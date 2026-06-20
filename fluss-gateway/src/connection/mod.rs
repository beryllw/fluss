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

//! FlussConnectionProvider.
//!
//! `resolve(cluster, principal) -> shared FlussConnection`. Returns a shared
//! proxy-account connection for all principals (no doAs), but keeps the
//! `principal` argument so it is forced to flow all the way down to connection
//! resolution. The same cluster's connection is reused across sessions and
//! requests (lazily constructed once, then cached) — never one per
//! session/request. Design: `design/infra.md`.
//!
//! Testability note: the trait is generic over the connection handle type
//! (`Conn`). The production shared-proxy provider sets `Conn =
//! fluss::client::FlussConnection`, which needs a live cluster to construct;
//! the test fake sets `Conn` to its own cheap handle so connection-reuse and
//! error-mapping behavior can be exercised without a cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;

use crate::cluster::{ClusterRegistry, ClusterConfig};
use crate::error::{GatewayError, GatewayResult};
use crate::types::{ClusterId, Principal};

/// Build a fluss-rs `Config` from gateway cluster config. Wires only bootstrap
/// servers; SASL / per-user credentials are out of scope. Shared by the
/// connection provider and the recovery [`ConnectionManager`] so a rebuilt
/// connection uses the exact same config as the original.
pub fn build_fluss_config(cfg: &ClusterConfig) -> fluss::config::Config {
    fluss::config::Config {
        bootstrap_servers: cfg.bootstrap_servers.clone(),
        ..fluss::config::Config::default()
    }
}

/// True if a backend error string indicates the shared Fluss connection is dead
/// (its RPC I/O task stopped / the connection is poisoned) — the signal to evict
/// and rebuild it. These markers originate in the fluss-rs RPC layer; the typed
/// error is already stringified by the time it reaches the gateway boundary
/// (SQL path buries it in a DataFusion error), so a substring match is the only
/// signal available on both paths.
pub fn is_connection_dead(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains("connection i/o task has stopped")
        || e.contains("connection closed before response")
        || e.contains("poisoned")
        || e.contains("is poisoned")
}

/// Hook invoked after a successful rebuild with the new connection, so consumers
/// that captured the connection by value (e.g. `FlussDatafusion`) can swap it in.
type SwapHook =
    Box<dyn Fn(&Arc<fluss::client::FlussConnection>) -> GatewayResult<()> + Send + Sync>;

/// Owns the process-wide shared `FlussConnection` and rebuilds it when it dies.
///
/// `current()` always returns the live connection; callers must fetch it per
/// operation (never cache it) so a swap is observed. `recover()` is **bounded**
/// and **single-flight**: at most [`Self::MAX_ATTEMPTS`] rebuilds with exponential
/// backoff, and within [`Self::COOLDOWN`] of a successful rebuild it is a no-op —
/// so a burst of failed queries triggers exactly one rebuild, never an infinite
/// loop. On success it swaps the new connection into every consumer (via the
/// `on_swap` hook + the `ArcSwap` the backend reads) and closes the old one.
pub struct ConnectionManager {
    config: fluss::config::Config,
    current: ArcSwap<fluss::client::FlussConnection>,
    on_swap: SwapHook,
    rebuild_lock: tokio::sync::Mutex<()>,
    last_rebuilt: Mutex<Option<Instant>>,
}

impl ConnectionManager {
    const MAX_ATTEMPTS: u32 = 3;
    const BASE_BACKOFF: Duration = Duration::from_secs(1);
    // Short window whose only job is to coalesce a thundering herd of queries that
    // all observe the SAME dead connection within a few ms (one rebuild serves
    // them all). Kept small so a genuinely new death seconds later can still
    // rebuild — important because some upstream paths (e.g. dropped full scans)
    // can re-kill the connection.
    const COOLDOWN: Duration = Duration::from_secs(2);

    pub fn new(
        initial: Arc<fluss::client::FlussConnection>,
        config: fluss::config::Config,
        on_swap: SwapHook,
    ) -> Self {
        Self {
            config,
            current: ArcSwap::from(initial),
            on_swap,
            rebuild_lock: tokio::sync::Mutex::new(()),
            last_rebuilt: Mutex::new(None),
        }
    }

    /// The live shared connection. Fetch per operation; do not cache.
    pub fn current(&self) -> Arc<fluss::client::FlussConnection> {
        self.current.load_full()
    }

    /// Bounded, single-flight rebuild of a dead connection. Returns `Ok` once a
    /// healthy connection is in place (rebuilt now, or rebuilt moments ago by a
    /// racing caller); `Err` if all bounded attempts failed.
    pub async fn recover(&self) -> GatewayResult<()> {
        // Single-flight: only one rebuild runs at a time.
        let _guard = self.rebuild_lock.lock().await;

        // Coalesce a burst of failures: if we just rebuilt, the current connection
        // is already fresh — do not thrash.
        if let Some(t) = *self.last_rebuilt.lock().unwrap() {
            if t.elapsed() < Self::COOLDOWN {
                return Ok(());
            }
        }

        let mut backoff = Self::BASE_BACKOFF;
        let mut last_err: Option<String> = None;
        for attempt in 1..=Self::MAX_ATTEMPTS {
            tracing::warn!(
                attempt,
                max = Self::MAX_ATTEMPTS,
                "rebuilding dead Fluss connection"
            );
            match fluss::client::FlussConnection::new(self.config.clone()).await {
                Ok(conn) => {
                    let new = Arc::new(conn);
                    // Point byvalue consumers (FlussDatafusion) at the new connection.
                    (self.on_swap)(&new)?;
                    let old = self.current.swap(new);
                    *self.last_rebuilt.lock().unwrap() = Some(Instant::now());
                    tracing::info!("Fluss connection rebuilt and swapped in");
                    // Close the dead connection in the background (best effort).
                    tokio::spawn(async move {
                        let _ = old.close(Duration::from_secs(5)).await;
                    });
                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    if attempt < Self::MAX_ATTEMPTS {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            }
        }
        Err(GatewayError::Backend(format!(
            "Fluss connection rebuild failed after {} attempts: {}",
            Self::MAX_ATTEMPTS,
            last_err.unwrap_or_default()
        )))
    }
}

/// Resolves a (cluster, principal) pair to a shared Fluss connection handle.
///
/// Contract:
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
// Shared-proxy production provider (real cluster access)
// ---------------------------------------------------------------------------

/// Production provider: one shared proxy-account `FlussConnection` per
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

    /// Build a fluss-rs `Config` from gateway cluster config (see
    /// [`build_fluss_config`]).
    fn build_config(cfg: &ClusterConfig) -> fluss::config::Config {
        build_fluss_config(cfg)
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

    #[test]
    fn detects_dead_connection_markers() {
        // The exact strings the fluss-rs RPC layer surfaces when a connection dies.
        for s in [
            "Fluss hitting unexpected rpc error connection error: ConnectionError(\"connection I/O task has stopped\")",
            "connection closed before response",
            "Connection is poisoned: ...",
            "planning failed: External error: ... connection I/O task has stopped",
        ] {
            assert!(is_connection_dead(s), "should detect: {s}");
        }
        // Ordinary errors must NOT trigger a rebuild.
        for s in [
            "table not found: fluss.db.t",
            "invalid argument: bad column type",
            "Column indices cannot be empty",
        ] {
            assert!(!is_connection_dead(s), "should NOT detect: {s}");
        }
    }
}
