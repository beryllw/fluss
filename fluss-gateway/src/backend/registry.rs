// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable registry of independently connected Fluss cluster runtimes.

use crate::auth::Principal;
use crate::backend::GatewayBackend;
use crate::backend::identity::{IdentityConnector, IdentityPool};
use crate::backend::metadata_cache::TableMetadataCache;
#[cfg(any(test, feature = "test-backend"))]
use crate::backend::model::ClusterHealthReport;
use crate::backend::model::TableDescription;
use crate::backend::resilient::{
    BackendHealth, BackendHealthSnapshot, ClusterSupervisor, ResilientBackend, SupervisorConfig,
};
use crate::backend::types::ClusterId;
use crate::config::GatewayConfig;
use crate::error::GatewayError;
use futures::future::join_all;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(test, feature = "test-backend"))]
type TestClusterEntry = (
    String,
    Option<Arc<dyn GatewayBackend>>,
    Option<ClusterHealthReport>,
);

/// Cached reachability exposed by cluster discovery and aggregate health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    Unknown,
    Available,
    Unavailable,
}

impl ClusterState {
    /// Stable lowercase wire value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Read-only cluster diagnostics assembled without a synchronous probe.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub id: ClusterId,
    pub state: ClusterState,
    pub health: BackendHealthSnapshot,
}

/// One cluster's isolated resilient backend slot and cached health.
pub struct ClusterRuntime {
    id: ClusterId,
    backend: Arc<ResilientBackend>,
    table_cache: Arc<TableMetadataCache<TableDescription>>,
    /// Per-user act-as connections; present exactly under `connection.identity-mode: user`.
    identity_pool: Option<Arc<IdentityPool>>,
}

impl ClusterRuntime {
    fn new(
        id: ClusterId,
        stale_after: Duration,
        cache_max_entries: usize,
        cache_ttl: Duration,
    ) -> Self {
        let health = Arc::new(BackendHealth::new(stale_after));
        let table_cache = Arc::new(
            TableMetadataCache::new(id.clone(), cache_max_entries, cache_ttl)
                .expect("validated metadata cache configuration"),
        );
        Self {
            id,
            backend: Arc::new(ResilientBackend::new(health)),
            table_cache,
            identity_pool: None,
        }
    }

    fn with_identity_pool(mut self, pool: Option<Arc<IdentityPool>>) -> Self {
        self.identity_pool = pool;
        self
    }

    fn snapshot(&self) -> ClusterSnapshot {
        let health = self.backend.health().snapshot();
        let state = if health.reachable {
            ClusterState::Available
        } else if health.reason == "starting" {
            ClusterState::Unknown
        } else {
            ClusterState::Unavailable
        };
        ClusterSnapshot {
            id: self.id.clone(),
            state,
            health,
        }
    }
}

/// Immutable map of configured cluster IDs to isolated runtimes.
pub struct ClusterRegistry {
    runtimes: BTreeMap<ClusterId, Arc<ClusterRuntime>>,
}

impl ClusterRegistry {
    /// Builds all runtime slots without requiring any cluster to be reachable.
    pub fn from_config(config: &GatewayConfig) -> Self {
        let runtimes = config
            .clusters
            .iter()
            .map(|(id, cluster)| {
                let runtime = ClusterRuntime::new(
                    id.clone(),
                    config.health.stale_after.get(),
                    config.metadata.cache_max_entries as usize,
                    config.metadata.cache_ttl.get(),
                )
                .with_identity_pool(identity_pool(id, cluster, config));
                (id.clone(), Arc::new(runtime))
            })
            .collect();
        Self { runtimes }
    }

    /// Resolves the backend serving `principal` on a configured cluster.
    ///
    /// Under the service identity mode every principal shares the supervised connection; under
    /// the user identity mode each principal gets its pooled act-as connection.
    pub async fn backend_for_principal(
        &self,
        id: &str,
        principal: &Principal,
    ) -> Result<Arc<dyn GatewayBackend>, GatewayError> {
        let cluster_id = ClusterId::try_from(id).map_err(|_| unknown_cluster(id))?;
        let runtime = self
            .runtimes
            .get(&cluster_id)
            .ok_or_else(|| unknown_cluster(id))?;
        match &runtime.identity_pool {
            Some(pool) => pool.acquire(&principal.name).await,
            None => self.backend(id),
        }
    }

    /// Returns the connected backend for a configured cluster.
    pub fn backend(&self, id: &str) -> Result<Arc<dyn GatewayBackend>, GatewayError> {
        let cluster_id = ClusterId::try_from(id).map_err(|_| unknown_cluster(id))?;
        let runtime = self
            .runtimes
            .get(&cluster_id)
            .ok_or_else(|| unknown_cluster(id))?;
        if !runtime.backend.is_connected() {
            return Err(
                GatewayError::unavailable(format!("cluster `{id}` is not connected"))
                    .with_resource("cluster", Some(id)),
            );
        }
        Ok(runtime.backend.clone())
    }

    /// Returns the table-metadata cache isolated to one configured cluster.
    pub fn table_cache(
        &self,
        id: &str,
    ) -> Result<Arc<TableMetadataCache<TableDescription>>, GatewayError> {
        let cluster_id = ClusterId::try_from(id).map_err(|_| unknown_cluster(id))?;
        self.runtimes
            .get(&cluster_id)
            .map(|runtime| runtime.table_cache.clone())
            .ok_or_else(|| unknown_cluster(id))
    }

    /// Returns cached snapshots in lexical cluster-ID order without probing.
    pub fn snapshots(&self) -> Vec<ClusterSnapshot> {
        self.runtimes
            .values()
            .map(|runtime| runtime.snapshot())
            .collect()
    }

    /// True when at least one configured cluster currently has a connected, fresh health result.
    pub fn any_available(&self) -> bool {
        self.runtimes
            .values()
            .any(|runtime| runtime.snapshot().state == ClusterState::Available)
    }

    /// Builds one independent reconnect and probe supervisor per configured cluster.
    ///
    /// The process lifecycle owns spawning and joining these subsystems.
    pub fn supervisors(&self, config: &GatewayConfig) -> Vec<ClusterSupervisor> {
        let supervisor_config = Arc::new(SupervisorConfig::from(config));
        self.runtimes
            .iter()
            .map(|(id, runtime)| {
                let cluster = config
                    .clusters
                    .get(id)
                    .expect("registry was built from this configuration")
                    .clone();
                ClusterSupervisor::new(
                    id.to_string(),
                    cluster,
                    supervisor_config.clone(),
                    runtime.backend.clone(),
                )
            })
            .collect()
    }

    /// Closes every connected backend concurrently.
    pub async fn close(&self, timeout: Duration) -> Result<(), GatewayError> {
        let results = join_all(self.runtimes.values().map(|runtime| {
            let cluster = runtime.id.to_string();
            let backend = runtime.backend.clone();
            async move {
                let result = GatewayBackend::close(backend.as_ref(), timeout).await;
                crate::observability::backend_close_result(&cluster, result.is_ok());
                crate::observability::backend_connected(&cluster, false);
                result
            }
        }))
        .await;
        results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map(|_| ())
    }

    /// Builds a connected single-cluster registry for protocol and integration tests.
    #[cfg(any(test, feature = "test-backend"))]
    pub fn single_for_test(
        id: impl Into<String>,
        backend: Arc<dyn GatewayBackend>,
        report: ClusterHealthReport,
    ) -> Self {
        Self::from_test_entries(vec![(id.into(), Some(backend), Some(report))])
    }

    /// Builds a connected single-cluster registry whose backend resolution runs in the user
    /// identity mode over the injected connector.
    #[cfg(any(test, feature = "test-backend"))]
    pub fn single_for_test_with_identity_pool(
        id: impl Into<String>,
        backend: Arc<dyn GatewayBackend>,
        report: ClusterHealthReport,
        connector: IdentityConnector,
        max_connections: usize,
        idle_timeout: Duration,
    ) -> Self {
        let id = id.into();
        let registry = Self::single_for_test(id.clone(), backend, report);
        let cluster_id = ClusterId::try_from(id).expect("test cluster ID must be valid");
        let runtime = registry.runtimes.get(&cluster_id).expect("runtime built");
        let pool = Arc::new(IdentityPool::new(
            cluster_id.as_str(),
            connector,
            max_connections,
            idle_timeout,
        ));
        let mut runtimes = BTreeMap::new();
        runtimes.insert(
            cluster_id.clone(),
            Arc::new(ClusterRuntime {
                id: runtime.id.clone(),
                backend: runtime.backend.clone(),
                table_cache: runtime.table_cache.clone(),
                identity_pool: Some(pool),
            }),
        );
        Self { runtimes }
    }

    /// Builds isolated connected or disconnected runtimes for multi-cluster tests.
    #[cfg(any(test, feature = "test-backend"))]
    pub fn from_test_entries(entries: Vec<TestClusterEntry>) -> Self {
        let runtimes = entries
            .into_iter()
            .map(|(id, backend, report)| {
                let id = ClusterId::try_from(id).expect("test cluster ID must be valid");
                let runtime = Arc::new(ClusterRuntime::new(
                    id.clone(),
                    Duration::from_secs(60),
                    1024,
                    Duration::from_secs(60),
                ));
                if let Some(backend) = backend {
                    runtime.backend.install(backend);
                }
                if let Some(report) = report {
                    runtime.backend.health().reachable(report);
                }
                (id, runtime)
            })
            .collect();
        Self { runtimes }
    }

    /// Replaces cached reachability for a configured test runtime.
    #[cfg(any(test, feature = "test-backend"))]
    pub fn set_unavailable_for_test(&self, id: &str, reason: &'static str) {
        if let Ok(id) = ClusterId::try_from(id)
            && let Some(runtime) = self.runtimes.get(&id)
        {
            runtime.backend.health().unreachable(reason);
        }
    }
}

fn unknown_cluster(id: &str) -> GatewayError {
    GatewayError::not_found(format!("unknown cluster `{id}`")).with_resource("cluster", Some(id))
}

/// Builds the per-user act-as pool of a user-mode cluster, or `None` under service mode.
///
/// The production connector dials a native backend whose SASL authorization id is the principal
/// name; configuration validation has already guaranteed the service credentials and a verifying
/// client authenticator.
fn identity_pool(
    id: &ClusterId,
    cluster: &crate::config::ClusterConfig,
    config: &GatewayConfig,
) -> Option<Arc<IdentityPool>> {
    if cluster.identity_mode != crate::config::IdentityMode::User {
        return None;
    }
    let cluster_config = cluster.clone();
    let lookup = config.lookup.clone();
    let connector: IdentityConnector = Arc::new(move |user: String| {
        let cluster_config = cluster_config.clone();
        let lookup = lookup.clone();
        Box::pin(async move {
            crate::backend::native::NativeGatewayBackend::connect_as(
                &cluster_config,
                &lookup,
                &user,
            )
            .await
            .map(|backend| Arc::new(backend) as Arc<dyn GatewayBackend>)
        })
    });
    Some(Arc::new(IdentityPool::new(
        id.as_str(),
        connector,
        cluster.effective_connection_max(),
        cluster.effective_connection_idle_timeout(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::model::ClusterStatus;
    use crate::backend::testing::TestBackend;

    fn green() -> ClusterHealthReport {
        ClusterHealthReport {
            status: ClusterStatus::Green,
            num_replicas: 1,
            in_sync_replicas: 1,
            num_leader_replicas: 1,
            active_leader_replicas: 1,
        }
    }

    #[tokio::test]
    async fn close_emits_one_bounded_result_per_configured_cluster() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let registry =
            ClusterRegistry::single_for_test("local", Arc::new(TestBackend::new()), green());

        registry.close(Duration::from_secs(1)).await.unwrap();

        let output = handle.render();
        let close = output
            .lines()
            .find(|line| line.starts_with("fluss_gateway_backend_closes_total"))
            .expect("backend close counter must be emitted");
        assert!(close.contains("cluster=\"local\""), "{close}");
        assert!(close.contains("result=\"success\""), "{close}");
        assert!(!close.contains("database="), "{close}");
        assert!(!close.contains("table="), "{close}");
    }

    #[tokio::test]
    async fn every_instance_derives_the_same_view_from_the_same_backend() {
        // Statelessness at the registry level: two independently built registries over one shared backend
        // expose identical cluster views, so no request can depend on which instance serves it.
        let backend: Arc<dyn GatewayBackend> = Arc::new(TestBackend::new());
        let entries = || {
            vec![
                ("alpha".to_string(), Some(backend.clone()), Some(green())),
                ("zeta".to_string(), None, None),
            ]
        };
        let first = ClusterRegistry::from_test_entries(entries());
        let second = ClusterRegistry::from_test_entries(entries());

        let ids = |registry: &ClusterRegistry| {
            registry
                .snapshots()
                .into_iter()
                .map(|snapshot| (snapshot.id.to_string(), snapshot.state))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&first), ids(&second));
        assert_eq!(
            ids(&first),
            vec![
                ("alpha".to_string(), ClusterState::Available),
                ("zeta".to_string(), ClusterState::Unknown),
            ]
        );
        assert!(first.any_available() && second.any_available());
        assert!(first.backend("alpha").is_ok() && second.backend("alpha").is_ok());
    }
}
