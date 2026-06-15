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

//! P6 — FlussConnectionProvider.
//!
//! `resolve(cluster, principal) -> shared FlussConnection`. Phase 1 returns a
//! shared proxy-account connection for all principals (no doAs), but keeps the
//! `principal` argument so the call site does not change when per-user creds
//! land later, and so `principal` is forced to flow all the way down to
//! connection resolution. The same cluster's connection is reused across
//! sessions and requests (lazily constructed once, then cached) — never one per
//! session/request. Design: `design/infra.md` §P6.5.
//!
//! Testability note: the trait is generic over the connection handle type
//! (`Conn`). The production shared-proxy provider sets `Conn =
//! fluss::client::FlussConnection`, which needs a live cluster to construct;
//! the test fake sets `Conn` to its own cheap handle so connection-reuse and
//! error-mapping behavior can be exercised without a cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::cluster::{ClusterRegistry, ClusterConfig};
use crate::error::{GatewayError, GatewayResult};
use crate::types::{ClusterId, Principal};

/// Resolves a (cluster, principal) pair to a shared Fluss connection handle.
///
/// Phase 1 contract:
/// - the returned handle is shared/reused per cluster across all callers;
/// - `principal` is preserved but not consumed (shared proxy account, no doAs);
/// - any backend/credential failure is mapped to a [`GatewayError`] here, at the
///   backend→domain boundary — the raw fluss-rs error type never escapes.
#[async_trait]
pub trait FlussConnectionProvider: Send + Sync {
    /// The connection handle type. Production: `fluss::client::FlussConnection`.
    type Conn: Send + Sync;

    async fn resolve(
        &self,
        cluster: &ClusterId,
        principal: &Principal,
    ) -> GatewayResult<Arc<Self::Conn>>;
}

// ---------------------------------------------------------------------------
// Shared-proxy production provider (skeleton — real cluster access)
// ---------------------------------------------------------------------------

/// Phase 1 production provider: one shared proxy-account `FlussConnection` per
/// cluster, lazily constructed from [`ClusterRegistry`] config and cached for
/// reuse. `principal` is accepted and ignored (shared proxy account).
///
/// Real cluster access is not exercised by unit tests (no live cluster in CI);
/// the type only has to compile against the fluss-rs API. Connection-reuse and
/// error-mapping *behavior* is covered by [`tests`] via the fake provider.
pub struct SharedProxyConnectionProvider {
    registry: ClusterRegistry,
    // Lazily-built, reused-per-cluster connections. `Mutex` guards the cache map
    // only; connections themselves are `Arc`-shared out.
    cache: Mutex<HashMap<String, Arc<fluss::client::FlussConnection>>>,
}

impl SharedProxyConnectionProvider {
    pub fn new(registry: ClusterRegistry) -> Self {
        Self {
            registry,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Build a fluss-rs `Config` from gateway cluster config. Phase 1 only wires
    /// bootstrap servers; SASL / per-user credentials are out of scope.
    fn build_config(cfg: &ClusterConfig) -> fluss::config::Config {
        fluss::config::Config {
            bootstrap_servers: cfg.bootstrap_servers.clone(),
            ..fluss::config::Config::default()
        }
    }
}

#[async_trait]
impl FlussConnectionProvider for SharedProxyConnectionProvider {
    type Conn = fluss::client::FlussConnection;

    async fn resolve(
        &self,
        cluster: &ClusterId,
        _principal: &Principal,
    ) -> GatewayResult<Arc<Self::Conn>> {
        // Fast path: already-built shared connection for this cluster.
        if let Some(conn) = self
            .cache
            .lock()
            .expect("connection cache mutex poisoned")
            .get(&cluster.0)
            .cloned()
        {
            return Ok(conn);
        }

        let cfg = self.registry.config(cluster)?.clone();
        let config = Self::build_config(&cfg);

        // backend→domain error boundary: the raw fluss-rs error never escapes.
        let conn = fluss::client::FlussConnection::new(config)
            .await
            .map_err(|e| GatewayError::Backend(format!("connect to {}: {e}", cluster.0)))?;
        let conn = Arc::new(conn);

        // Re-check under the lock to avoid racing two builders; first writer wins
        // so the connection stays shared per cluster.
        let mut cache = self.cache.lock().expect("connection cache mutex poisoned");
        let entry = cache.entry(cluster.0.clone()).or_insert(conn);
        Ok(entry.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Cheap fake connection handle.
    struct FakeConn {
        cluster: String,
    }

    /// Fake provider: counts how many real connections it builds so tests can
    /// assert per-cluster reuse. `fail_for` forces the backend→domain mapping.
    struct FakeProvider {
        registry: ClusterRegistry,
        cache: Mutex<HashMap<String, Arc<FakeConn>>>,
        builds: AtomicUsize,
        fail_for: Option<String>,
    }

    impl FakeProvider {
        fn new() -> Self {
            Self {
                registry: ClusterRegistry::single_default(ClusterConfig {
                    bootstrap_servers: "fake:0".into(),
                }),
                cache: Mutex::new(HashMap::new()),
                builds: AtomicUsize::new(0),
                fail_for: None,
            }
        }

        fn failing(cluster: &str) -> Self {
            let mut p = Self::new();
            p.fail_for = Some(cluster.to_string());
            p
        }

        fn build_count(&self) -> usize {
            self.builds.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl FlussConnectionProvider for FakeProvider {
        type Conn = FakeConn;

        async fn resolve(
            &self,
            cluster: &ClusterId,
            _principal: &Principal,
        ) -> GatewayResult<Arc<Self::Conn>> {
            if let Some(c) = self.cache.lock().unwrap().get(&cluster.0).cloned() {
                return Ok(c);
            }
            // Validate the cluster exists (exercises the registry boundary).
            self.registry.config(cluster)?;
            if self.fail_for.as_deref() == Some(cluster.0.as_str()) {
                return Err(GatewayError::Backend(format!("connect to {}: boom", cluster.0)));
            }
            self.builds.fetch_add(1, Ordering::SeqCst);
            let conn = Arc::new(FakeConn {
                cluster: cluster.0.clone(),
            });
            let mut cache = self.cache.lock().unwrap();
            let entry = cache.entry(cluster.0.clone()).or_insert(conn);
            Ok(entry.clone())
        }
    }

    fn principal() -> Principal {
        Principal { name: "alice".into() }
    }

    #[tokio::test]
    async fn resolves_default_cluster() {
        let p = FakeProvider::new();
        let conn = p.resolve(&ClusterId("default".into()), &principal()).await.unwrap();
        assert_eq!(conn.cluster, "default");
    }

    #[tokio::test]
    async fn reuses_connection_per_cluster_across_principals() {
        let p = FakeProvider::new();
        let c1 = p.resolve(&ClusterId("default".into()), &principal()).await.unwrap();
        let c2 = p
            .resolve(&ClusterId("default".into()), &Principal { name: "bob".into() })
            .await
            .unwrap();
        // Same cluster -> same shared connection, built exactly once.
        assert!(Arc::ptr_eq(&c1, &c2));
        assert_eq!(p.build_count(), 1);
    }

    #[tokio::test]
    async fn unknown_cluster_is_invalid_argument() {
        let p = FakeProvider::new();
        let res = p.resolve(&ClusterId("missing".into()), &principal()).await;
        assert!(matches!(res, Err(GatewayError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn backend_failure_maps_to_domain_error() {
        let p = FakeProvider::failing("default");
        let res = p.resolve(&ClusterId("default".into()), &principal()).await;
        // Raw backend failure mapped to domain Backend, no leak of fluss-rs type.
        assert!(matches!(res, Err(GatewayError::Backend(_))));
    }
}
