// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! In-memory [`FlussBackend`] for the protocol tests (FIP-49 test plan).
//!
//! It needs no Fluss cluster and is compiled only under `cfg(test)`, so it can never be part of a
//! shippable path. Replacing the whole backend also replaces the connection pool, which is why the
//! pool has its own tests against an injected connector.

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::backend::{FlussBackend, unknown_cluster};
use crate::error::{GatewayError, GatewayResult, Resource};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// The injected behaviour of one fixture backend.
struct FakeState {
    databases: BTreeMap<String, Vec<String>>,
    /// Returned by every catalog call instead of the fixture content.
    failure: Option<GatewayError>,
    /// Delay before answering, for the deadline tests.
    latency: Duration,
}

/// A backend whose clusters and catalog are fixed and whose failures are injected by the test.
///
/// The catalog is shared by every configured cluster: the tests that need cluster isolation are the
/// pool's, not the protocol's.
pub struct FakeFlussBackend {
    clusters: Vec<ClusterId>,
    state: Mutex<FakeState>,
}

impl Default for FakeFlussBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeFlussBackend {
    /// One cluster named `default` with an empty catalog.
    pub fn new() -> Self {
        Self::with_catalog(&[])
    }

    /// One cluster named `default` over a catalog of databases and their tables, in any order.
    pub fn with_catalog(databases: &[(&str, &[&str])]) -> Self {
        Self {
            clusters: vec![cluster_id("default")],
            state: Mutex::new(FakeState {
                databases: databases
                    .iter()
                    .map(|(database, tables)| {
                        (
                            (*database).to_string(),
                            tables.iter().map(|table| (*table).to_string()).collect(),
                        )
                    })
                    .collect(),
                failure: None,
                latency: Duration::ZERO,
            }),
        }
    }

    /// Several clusters with an empty catalog, for the discovery tests.
    pub fn with_clusters(ids: &[&str]) -> Self {
        let mut clusters: Vec<ClusterId> = ids.iter().map(|id| cluster_id(id)).collect();
        clusters.sort();
        Self {
            clusters,
            ..Self::with_catalog(&[])
        }
    }

    /// Makes every catalog call fail, the way an unreachable or refusing cluster would.
    pub fn fail_with(&self, error: GatewayError) {
        self.state().failure = Some(error);
    }

    /// Delays every catalog call, so a test can drive a request into its deadline.
    pub fn set_latency(&self, latency: Duration) {
        self.state().latency = latency;
    }

    /// Runs one fixture answer under the request's budget, like the production backend does.
    async fn answer<T>(
        &self,
        ctx: &RequestContext,
        produce: impl FnOnce(&FakeState) -> GatewayResult<T>,
    ) -> GatewayResult<T> {
        if !self.has_cluster(ctx.cluster_id().as_str()) {
            return Err(unknown_cluster(ctx.cluster_id().as_str()));
        }
        let latency = self.state().latency;
        ctx.run(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            let state = self.state();
            match &state.failure {
                Some(failure) => Err(failure.clone()),
                None => produce(&state),
            }
        })
        .await
    }

    fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn cluster_id(id: &str) -> ClusterId {
    ClusterId::try_from(id).expect("valid fixture cluster ID")
}

#[async_trait]
impl FlussBackend for FakeFlussBackend {
    fn clusters(&self) -> Vec<ClusterId> {
        self.clusters.clone()
    }

    fn has_cluster(&self, id: &str) -> bool {
        self.clusters.iter().any(|cluster| cluster.as_str() == id)
    }

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>> {
        self.answer(ctx, |state| Ok(state.databases.keys().cloned().collect()))
            .await
    }

    async fn list_tables(
        &self,
        ctx: &RequestContext,
        database: &str,
    ) -> GatewayResult<Vec<String>> {
        self.answer(ctx, |state| {
            state.databases.get(database).cloned().ok_or_else(|| {
                GatewayError::not_found(format!("database `{database}` does not exist"))
                    .with_resource(Resource::Database)
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[tokio::test]
    async fn the_fixture_answers_its_catalog_until_a_failure_is_injected() {
        let backend = FakeFlussBackend::with_catalog(&[("sales", &["orders"]), ("ops", &[])]);
        let ctx = RequestContext::for_test("default", Duration::from_secs(5));

        assert_eq!(
            backend.list_databases(&ctx).await.unwrap(),
            ["ops", "sales"]
        );
        assert_eq!(
            backend.list_tables(&ctx, "sales").await.unwrap(),
            ["orders"]
        );
        assert_eq!(
            backend.list_tables(&ctx, "nope").await.unwrap_err().code(),
            "database_not_found"
        );

        // A cluster the fixture does not serve is the same 404 the production backend answers.
        let elsewhere = RequestContext::for_test("other", Duration::from_secs(5));
        assert_eq!(
            backend.list_databases(&elsewhere).await.unwrap_err().code(),
            "cluster_not_found"
        );

        backend.fail_with(GatewayError::unavailable("cluster is down"));
        assert_eq!(
            backend.list_databases(&ctx).await.unwrap_err().kind(),
            ErrorKind::Unavailable
        );
    }

    /// An injected delay is bounded by the request, not by the fixture.
    #[tokio::test]
    async fn a_slow_answer_runs_into_the_request_deadline() {
        let backend = FakeFlussBackend::new();
        backend.set_latency(Duration::from_secs(30));
        let ctx = RequestContext::for_test("default", Duration::from_millis(20));

        assert_eq!(
            backend.list_databases(&ctx).await.unwrap_err().kind(),
            ErrorKind::DeadlineExceeded
        );
    }

    #[test]
    fn several_fixture_clusters_are_discovered_in_order() {
        let backend = FakeFlussBackend::with_clusters(&["zeta", "alpha"]);
        assert_eq!(
            backend
                .clusters()
                .iter()
                .map(ClusterId::as_str)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
    }
}
