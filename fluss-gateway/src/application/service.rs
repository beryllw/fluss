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

//! Protocol-neutral facade over configured cluster backends: construction and shared plumbing only.
//!
//! This file holds the [`GatewayService`] struct, its constructors, its accessors, and the crate-private
//! helpers every domain method needs. The domain methods themselves live in separate inherent `impl` blocks in
//! sibling modules — [`crate::application::ddl`] for catalog reads and mutations,
//! [`crate::application::write`] for the records endpoint, [`crate::application::lookup`] for the two lookup
//! endpoints — so that independent workstreams extend the service without sharing one file.

use crate::application::RequestContext;
use crate::application::metadata_cache::TableMetadataCache;
use crate::backend::GatewayBackend;
use crate::backend::model::{TableDescription, TableRef};
use crate::backend::registry::ClusterRegistry;
use crate::error::{ErrorKind, GatewayError};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Fallback write delivery lifetime for services built without configuration, matching the `[write]` default.
const DEFAULT_MAX_WRITE_DELIVERY_TIME: Duration = Duration::from_secs(20);

/// Shared application facade used by REST now and by future protocol adapters.
///
/// The facade resolves the explicitly selected cluster and applies request cancellation and the
/// absolute deadline consistently. It holds no request-spanning state of its own: everything it owns is either
/// immutable configuration or the per-cluster caches held by the registry.
pub struct GatewayService {
    clusters: Arc<ClusterRegistry>,
    max_write_delivery_time: Duration,
}

impl GatewayService {
    /// Builds a service with the default write delivery lifetime.
    pub fn new(clusters: Arc<ClusterRegistry>) -> Self {
        Self {
            clusters,
            max_write_delivery_time: DEFAULT_MAX_WRITE_DELIVERY_TIME,
        }
    }

    /// Overrides the finite write delivery lifetime from validated gateway configuration.
    pub fn with_write_delivery_time(
        clusters: Arc<ClusterRegistry>,
        max_write_delivery_time: Duration,
    ) -> Self {
        Self {
            clusters,
            max_write_delivery_time,
        }
    }

    /// The immutable registry of configured clusters.
    pub fn clusters(&self) -> &Arc<ClusterRegistry> {
        &self.clusters
    }

    /// The table metadata cache isolated to one configured cluster.
    pub fn metadata_cache(
        &self,
        cluster: &str,
    ) -> Result<Arc<TableMetadataCache<TableDescription>>, GatewayError> {
        self.clusters.table_cache(cluster)
    }

    /// The connected backend of one configured cluster, without a request context.
    pub fn backend_for(&self, cluster: &str) -> Result<Arc<dyn GatewayBackend>, GatewayError> {
        self.clusters.backend(cluster)
    }

    /// The configured per-entry write delivery lifetime.
    pub fn max_write_delivery_time(&self) -> Duration {
        self.max_write_delivery_time
    }

    /// Resolves the backend of the request's cluster after checking that the request is still live.
    pub(crate) fn backend(
        &self,
        context: &RequestContext,
    ) -> Result<Arc<dyn GatewayBackend>, GatewayError> {
        context.ensure_active()?;
        self.clusters.backend(context.cluster_id().as_str())
    }

    /// The metadata cache of the request's cluster.
    pub(crate) fn cache(
        &self,
        context: &RequestContext,
    ) -> Result<Arc<TableMetadataCache<TableDescription>>, GatewayError> {
        self.clusters.table_cache(context.cluster_id().as_str())
    }

    /// Runs one backend operation under the request's cancellation signal and absolute deadline.
    pub(crate) async fn execute<T, F>(
        &self,
        context: &RequestContext,
        operation: F,
    ) -> Result<T, GatewayError>
    where
        F: Future<Output = Result<T, GatewayError>>,
    {
        context.ensure_active()?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => {
                Err(GatewayError::cancelled("request was cancelled"))
            }
            _ = tokio::time::sleep_until(deadline) => {
                Err(GatewayError::deadline_exceeded("request deadline exceeded"))
            }
            result = operation => result,
        }
    }
}

/// Attaches machine-readable resource context to the error kinds that name a resource.
pub(crate) fn resource_error(
    error: GatewayError,
    resource_kind: &'static str,
    resource_name: impl Into<String>,
) -> GatewayError {
    if error.details().is_some()
        || !matches!(
            error.kind(),
            ErrorKind::NotFound | ErrorKind::AlreadyExists | ErrorKind::FailedPrecondition
        )
    {
        return error;
    }
    error.with_resource(resource_kind, Some(resource_name.into()))
}

/// Reads table metadata through the per-cluster cache, loading it from the backend on a miss.
pub(crate) async fn load_table(
    cache: &TableMetadataCache<TableDescription>,
    backend: &Arc<dyn GatewayBackend>,
    table: &TableRef,
) -> Result<Arc<TableDescription>, GatewayError> {
    cache
        .get_or_load(table, || async {
            Ok((*backend.describe_table(table).await?).clone())
        })
        .await
}

/// Publishes a freshly read description into the cache and returns the cached instance.
pub(crate) async fn cache_table(
    cache: &TableMetadataCache<TableDescription>,
    table: &TableRef,
    description: Arc<TableDescription>,
) -> Result<Arc<TableDescription>, GatewayError> {
    cache
        .refresh(table, || async move { Ok((*description).clone()) })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{CancellationSignal, ClusterId};
    use std::time::Instant;

    fn service() -> GatewayService {
        GatewayService::new(Arc::new(ClusterRegistry::from_test_entries(vec![(
            "default".to_string(),
            None,
            None,
        )])))
    }

    /// The error kind of a failed resolution, for results whose success type is not `Debug`.
    fn kind_of<T>(result: Result<T, GatewayError>) -> ErrorKind {
        match result {
            Ok(_) => panic!("expected a failure"),
            Err(error) => error.kind(),
        }
    }

    fn context(cluster: &str, deadline: Instant) -> RequestContext {
        RequestContext::new(
            "request-1",
            "test",
            ClusterId::try_from(cluster).unwrap(),
            deadline,
            CancellationSignal::default(),
            crate::auth::Principal::new("tester"),
        )
    }

    #[test]
    fn accessors_expose_the_configured_clusters_and_write_lifetime() {
        let service = service();
        assert_eq!(service.clusters().snapshots().len(), 1);
        assert_eq!(
            service.max_write_delivery_time(),
            DEFAULT_MAX_WRITE_DELIVERY_TIME
        );
        assert!(service.metadata_cache("default").is_ok());
        assert_eq!(
            service
                .metadata_cache("missing")
                .err()
                .expect("an unconfigured cluster has no cache")
                .kind(),
            ErrorKind::NotFound
        );

        let configured = GatewayService::with_write_delivery_time(
            Arc::new(ClusterRegistry::from_test_entries(Vec::new())),
            Duration::from_secs(3),
        );
        assert_eq!(configured.max_write_delivery_time(), Duration::from_secs(3));
    }

    #[tokio::test]
    async fn backend_resolution_applies_the_request_lifecycle_first() {
        let service = service();

        assert_eq!(
            kind_of(service.backend(&context("default", Instant::now()))),
            ErrorKind::DeadlineExceeded
        );

        let cancellation = CancellationSignal::default();
        cancellation.cancel();
        let cancelled = RequestContext::new(
            "request-1",
            "test",
            ClusterId::try_from("default").unwrap(),
            Instant::now() + Duration::from_secs(5),
            cancellation,
            crate::auth::Principal::new("tester"),
        );
        assert_eq!(kind_of(service.backend(&cancelled)), ErrorKind::Cancelled);

        // A configured but disconnected cluster is unavailable, an unconfigured one is not found.
        let live = context("default", Instant::now() + Duration::from_secs(5));
        assert_eq!(kind_of(service.backend(&live)), ErrorKind::Unavailable);
        let unknown = context("missing", Instant::now() + Duration::from_secs(5));
        assert_eq!(kind_of(service.backend(&unknown)), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn execute_enforces_the_deadline_over_a_slow_operation() {
        let service = service();
        let context = context("default", Instant::now() + Duration::from_millis(20));
        let error = service
            .execute(&context, async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<(), GatewayError>(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
    }

    #[test]
    fn resource_context_is_added_only_to_resource_naming_kinds() {
        let named = resource_error(GatewayError::not_found("gone"), "table", "db.t");
        assert_eq!(
            named.details().and_then(|d| d.resource_name.clone()),
            Some("db.t".to_string())
        );
        let untouched = resource_error(GatewayError::internal("boom"), "table", "db.t");
        assert!(untouched.details().is_none());
    }
}
