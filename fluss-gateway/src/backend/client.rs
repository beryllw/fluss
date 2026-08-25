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

//! The production [`FlussBackend`] over `fluss-rs`.
//!
//! It owns one `ConnectionCache` per configured cluster, which is the whole of the gateway's connection
//! management: routing a request to its cluster and sharing its service-mode connection. None of that
//! is visible above this module.
//!
//! This module and [`crate::backend::connection`] own native client handles, connection lifecycle, and
//! transport error translation. Protocol adapters may still reuse stable `fluss-rs` domain types, such
//! as metadata descriptors, when they fit the API instead of defining duplicate models.

use crate::backend::FlussBackend;
use crate::backend::connection::{ConnectionCache, NativeConnector};
use crate::backend::context::RequestContext;
use crate::backend::errors::map_fluss_error;
use crate::backend::types::ClusterId;
use crate::backend::unknown_cluster;
use crate::config::GatewayConfig;
use crate::error::GatewayResult;
use async_trait::async_trait;
use fluss::client::FlussAdmin;
use fluss::error::Error as FlussClientError;
use futures_util::future::join_all;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub struct NativeFlussBackend {
    /// The cluster routing table. Built from configuration and never mutated, so it needs no lock and
    /// no request can add, remove, or reorder a cluster.
    caches: BTreeMap<ClusterId, ConnectionCache<NativeConnector>>,
}

impl NativeFlussBackend {
    /// Performs no I/O: the process starts while Fluss is down, and the first request of a cluster
    /// dials it.
    pub fn from_config(config: &GatewayConfig) -> Self {
        let caches = config
            .clusters
            .iter()
            .map(|(id, cluster)| {
                let id = ClusterId::try_from(id.as_str())
                    .expect("configuration validation accepted every cluster ID");
                let cache =
                    ConnectionCache::new(id.clone(), cluster, NativeConnector::new(cluster));
                (id, cache)
            })
            .collect();
        Self { caches }
    }

    /// Closes every connection of every cluster within `timeout`. Idempotent.
    ///
    /// Concurrent because the budget is the whole shutdown's, not each cluster's: closing sequentially
    /// would let one slow cluster consume the time the others need.
    pub(crate) async fn close(&self, timeout: Duration) -> GatewayResult<()> {
        let closes = join_all(self.caches.values().map(|cache| cache.close(timeout))).await;
        let mut first_failure = None;
        for (id, result) in self.caches.keys().zip(closes) {
            if let Err(error) = result {
                log::warn!("failed to close the connections of cluster `{id}`: {error}");
                first_failure = first_failure.or(Some(error));
            }
        }
        first_failure.map_or(Ok(()), Err)
    }

    /// Runs one idle scan for every configured cluster. Each cache logs its own best-effort close
    /// failures, so a slow or unavailable cluster cannot suppress cleanup of the others.
    pub(crate) async fn clean_expired_connections(&self) {
        join_all(self.caches.values().map(ConnectionCache::clean_expired)).await;
    }

    /// The only place `cluster_not_found` originates.
    fn cache_for(&self, ctx: &RequestContext) -> GatewayResult<&ConnectionCache<NativeConnector>> {
        self.caches
            .get(ctx.cluster_id())
            .ok_or_else(|| unknown_cluster(ctx.cluster_id().as_str()))
    }

    /// The single entry point of every admin call: route, run under the request budget, take the
    /// cluster's service connection, then classify the failure.
    ///
    /// A failure never evicts the connection. `fluss-rs` reports a broken transport per server and
    /// reconnects that server on the next use, so the logical client recovers on its own; discarding it
    /// would only throw away its cluster metadata and cached sub-clients.
    ///
    async fn admin_call<T, F, Fut>(
        &self,
        ctx: &RequestContext,
        what: &'static str,
        operation: F,
    ) -> GatewayResult<T>
    where
        F: FnOnce(Arc<FlussAdmin>) -> Fut,
        Fut: Future<Output = Result<T, FlussClientError>>,
    {
        let cache = self.cache_for(ctx)?;
        ctx.run(async {
            let connection = cache.connection(ctx).await?;
            let result = match connection.get_admin() {
                Ok(admin) => operation(admin).await,
                Err(error) => Err(error),
            };
            result.map_err(|native| map_fluss_error(what, native))
        })
        .await
    }
}

#[async_trait]
impl FlussBackend for NativeFlussBackend {
    fn clusters(&self) -> Vec<ClusterId> {
        self.caches.keys().cloned().collect()
    }

    fn has_cluster(&self, id: &str) -> bool {
        ClusterId::try_from(id).is_ok_and(|id| self.caches.contains_key(&id))
    }

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>> {
        self.admin_call(ctx, "list the databases", |admin| async move {
            admin.list_databases().await
        })
        .await
    }

    async fn list_tables(
        &self,
        ctx: &RequestContext,
        database: &str,
    ) -> GatewayResult<Vec<String>> {
        let database = database.to_string();
        self.admin_call(ctx, "list the tables", |admin| async move {
            admin.list_tables(&database).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, ConfigDuration};
    use crate::error::ErrorKind;

    fn backend(clusters: &[(&str, ClusterConfig)]) -> NativeFlussBackend {
        NativeFlussBackend::from_config(&GatewayConfig {
            clusters: clusters
                .iter()
                .map(|(id, config)| ((*id).to_string(), config.clone()))
                .collect(),
            ..GatewayConfig::default()
        })
    }

    fn service_cluster() -> ClusterConfig {
        ClusterConfig::default()
    }

    /// Every configured cluster is routable in lexical order, and a malformed or unconfigured ID is
    /// the same 404 — resolved from configuration alone, without any connection attempt.
    #[tokio::test]
    async fn routing_answers_only_from_configuration() {
        let backend = backend(&[("zeta", service_cluster()), ("alpha", service_cluster())]);

        assert_eq!(
            backend
                .clusters()
                .iter()
                .map(ClusterId::as_str)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert!(backend.has_cluster("alpha"));
        for unknown in ["beta", "Not A Cluster", ""] {
            assert!(!backend.has_cluster(unknown), "{unknown:?}");
        }

        let ctx = RequestContext::for_test("beta", Duration::from_secs(5));
        assert_eq!(
            failure(backend.list_databases(&ctx).await),
            (ErrorKind::NotFound, "cluster_not_found")
        );
    }

    /// The backend constructs without touching the network, so both ways a dial can fail surface on
    /// the first request rather than at startup. Shutdown then has nothing to close.
    #[tokio::test]
    async fn a_dial_failure_reaches_the_caller_classified() {
        let unreachable = ClusterConfig {
            // Port 1 has no listener in any test environment; the connect timeout bounds the attempt.
            bootstrap_servers: "127.0.0.1:1".to_string(),
            connect_timeout: ConfigDuration::from_millis(200),
            ..service_cluster()
        };
        let illegal = ClusterConfig {
            bootstrap_servers: "not-a-host-port".to_string(),
            ..service_cluster()
        };
        let backend = backend(&[("down", unreachable), ("bad_address", illegal)]);

        for (cluster, expected) in [
            ("down", ErrorKind::Unavailable),
            ("bad_address", ErrorKind::InvalidArgument),
        ] {
            let ctx = RequestContext::for_test(cluster, Duration::from_secs(5));
            assert_eq!(
                failure(backend.list_databases(&ctx).await).0,
                expected,
                "{cluster}"
            );
        }
        backend.close(Duration::from_secs(1)).await.unwrap();
    }

    fn failure<T>(result: GatewayResult<T>) -> (ErrorKind, &'static str) {
        let error = result.err().expect("the call fails");
        (error.kind(), error.code())
    }

    /// A server that accepts the connection and never answers must still end at the deadline.
    ///
    /// Nothing below the gateway ends this wait: `fluss-rs` has no per-RPC timeout, and the connect
    /// timeout is already satisfied by the accepted TCP connection. Only [`RequestContext::run`] does.
    /// The abandoned RPC stays registered on the native connection — see the note on
    /// `NativeConnector::dial`.
    #[tokio::test]
    async fn a_server_that_never_answers_ends_at_the_request_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a bound listener");
        let address = listener.local_addr().expect("a bound address");
        tokio::spawn(async move {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                // Held open and never answered.
                accepted.push(stream);
            }
        });

        let backend = backend(&[(
            "default",
            ClusterConfig {
                bootstrap_servers: address.to_string(),
                ..service_cluster()
            },
        )]);
        let ctx = RequestContext::for_test("default", Duration::from_millis(300));

        assert_eq!(
            failure(backend.list_databases(&ctx).await).0,
            ErrorKind::DeadlineExceeded
        );
    }
}
