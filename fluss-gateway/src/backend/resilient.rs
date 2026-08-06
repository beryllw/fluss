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

//! Reconnecting backend slot and cached health state.
//!
//! The installed connection is a pooled resource, not request state: dropping and rebuilding it changes latency,
//! never correctness, which is what lets any instance serve any request.

use crate::backend::GatewayBackend;
use crate::backend::model::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, PartitionMutationRequest,
};
use crate::backend::model::{
    ClusterHealthReport, DatabaseDescription, LookupKey, LookupOutcome, PartitionDescription,
    PrefixLookupOutcome, PrefixLookupRequest, PreparedWriteRequest, TableDescription, TableRef,
    WriteResult,
};
use crate::backend::native::NativeGatewayBackend;
use crate::config::{ClusterConfig, GatewayConfig, HealthConfig, LookupConfig};
use crate::error::GatewayError;
use crate::observability;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// A read-only snapshot used by health handlers without issuing a backend RPC.
#[derive(Debug, Clone)]
pub struct BackendHealthSnapshot {
    pub reachable: bool,
    pub report: Option<ClusterHealthReport>,
    pub reason: &'static str,
}

/// Last probe result plus when it was taken, so [`BackendHealth::snapshot`] can age it out.
#[derive(Debug, Clone)]
struct HealthState {
    reachable: bool,
    report: Option<ClusterHealthReport>,
    reason: &'static str,
    checked_at: Option<Instant>,
}

/// Single-flight cached backend health shared by probes and HTTP handlers.
pub struct BackendHealth {
    state: RwLock<HealthState>,
    stale_after: Duration,
}

impl BackendHealth {
    /// Creates backend health state that becomes stale after the configured duration.
    pub fn new(stale_after: Duration) -> Self {
        Self {
            state: RwLock::new(HealthState {
                reachable: false,
                report: None,
                reason: "starting",
                checked_at: None,
            }),
            stale_after,
        }
    }

    /// Records a successful probe and its cluster health report.
    pub fn reachable(&self, report: ClusterHealthReport) {
        *self.state.write() = HealthState {
            reachable: true,
            report: Some(report),
            reason: "reachable",
            checked_at: Some(Instant::now()),
        };
    }

    /// Records that the backend is currently unreachable for a safe reason.
    pub fn unreachable(&self, reason: &'static str) {
        *self.state.write() = HealthState {
            reachable: false,
            report: None,
            reason,
            checked_at: Some(Instant::now()),
        };
    }

    /// Returns the current reachability snapshot, applying the staleness limit.
    pub fn snapshot(&self) -> BackendHealthSnapshot {
        let state = self.state.read();
        if state
            .checked_at
            .is_some_and(|checked_at| checked_at.elapsed() > self.stale_after)
        {
            return BackendHealthSnapshot {
                reachable: false,
                report: state.report,
                reason: "stale",
            };
        }
        BackendHealthSnapshot {
            reachable: state.reachable,
            report: state.report,
            reason: state.reason,
        }
    }
}

/// Delegates operations to the most recently connected backend.
pub struct ResilientBackend {
    current: RwLock<Option<Arc<dyn GatewayBackend>>>,
    health: Arc<BackendHealth>,
}

impl ResilientBackend {
    /// Starts with no backend installed, so every call fails as unavailable until the supervisor connects one.
    pub fn new(health: Arc<BackendHealth>) -> Self {
        Self {
            current: RwLock::new(None),
            health,
        }
    }

    /// The shared health state the supervisor updates and the readiness endpoint reads.
    pub fn health(&self) -> Arc<BackendHealth> {
        self.health.clone()
    }

    fn backend(&self) -> Result<Arc<dyn GatewayBackend>, GatewayError> {
        self.current
            .read()
            .clone()
            .ok_or_else(|| GatewayError::unavailable("Fluss backend is not connected"))
    }

    /// Returns whether a concrete backend is currently installed.
    pub fn is_connected(&self) -> bool {
        self.current.read().is_some()
    }

    /// Installs a successfully connected backend. Exposed within the crate for registry-backed tests.
    pub(crate) fn install(&self, backend: Arc<dyn GatewayBackend>) {
        *self.current.write() = Some(backend);
    }

    fn remove(&self) -> Option<Arc<dyn GatewayBackend>> {
        self.current.write().take()
    }
}

#[async_trait]
impl GatewayBackend for ResilientBackend {
    async fn list_databases(&self) -> Result<Vec<String>, GatewayError> {
        self.backend()?.list_databases().await
    }

    async fn describe_database(&self, database: &str) -> Result<DatabaseDescription, GatewayError> {
        self.backend()?.describe_database(database).await
    }

    async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<(), GatewayError> {
        self.backend()?.create_database(request).await
    }

    async fn drop_database(&self, database: &str) -> Result<(), GatewayError> {
        self.backend()?.drop_database(database).await
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, GatewayError> {
        self.backend()?.list_tables(database).await
    }

    async fn describe_table(
        &self,
        table: &TableRef,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        self.backend()?.describe_table(table).await
    }

    async fn create_table(&self, request: &CreateTableRequest) -> Result<(), GatewayError> {
        self.backend()?.create_table(request).await
    }

    async fn alter_table(&self, request: &AlterTableRequest) -> Result<(), GatewayError> {
        self.backend()?.alter_table(request).await
    }

    async fn drop_table(&self, table: &TableRef) -> Result<(), GatewayError> {
        self.backend()?.drop_table(table).await
    }

    async fn list_partitions(
        &self,
        table: &TableRef,
    ) -> Result<Vec<PartitionDescription>, GatewayError> {
        self.backend()?.list_partitions(table).await
    }

    async fn create_partition(
        &self,
        request: &PartitionMutationRequest,
    ) -> Result<(), GatewayError> {
        self.backend()?.create_partition(request).await
    }

    async fn drop_partition(&self, request: &PartitionMutationRequest) -> Result<(), GatewayError> {
        self.backend()?.drop_partition(request).await
    }

    async fn lookup(
        &self,
        table: &TableRef,
        keys: Vec<LookupKey>,
    ) -> Result<Vec<LookupOutcome>, GatewayError> {
        self.backend()?.lookup(table, keys).await
    }

    async fn prefix_lookup(
        &self,
        table: &TableRef,
        request: PrefixLookupRequest,
    ) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
        self.backend()?.prefix_lookup(table, request).await
    }

    async fn write(&self, request: PreparedWriteRequest) -> Result<WriteResult, GatewayError> {
        self.backend()?.write(request).await
    }

    async fn cluster_health(&self) -> Result<ClusterHealthReport, GatewayError> {
        self.backend()?.cluster_health().await
    }

    async fn close(&self, timeout: Duration) -> Result<(), GatewayError> {
        match self.remove() {
            Some(backend) => backend.close(timeout).await,
            None => Ok(()),
        }
    }
}

/// Exponential reconnect delay with bounded jitter, reset on every successful connect.
struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        let percent = 90 + (uuid::Uuid::new_v4().as_u128() % 21) as u32;
        delay.saturating_mul(percent) / 100
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }
}

/// The process-independent configuration one cluster supervisor needs.
///
/// Keeping this separate from [`GatewayConfig`] avoids retaining and cloning listener, shutdown,
/// metadata, write, and every other cluster's configuration into each supervisor.
#[derive(Debug, Clone)]
pub(crate) struct SupervisorConfig {
    health: HealthConfig,
    lookup: LookupConfig,
}

impl From<&GatewayConfig> for SupervisorConfig {
    fn from(config: &GatewayConfig) -> Self {
        Self {
            health: config.health.clone(),
            lookup: config.lookup.clone(),
        }
    }
}

/// One configured cluster's reconnect and health-probe subsystem.
pub struct ClusterSupervisor {
    cluster_id: String,
    cluster_config: ClusterConfig,
    config: Arc<SupervisorConfig>,
    backend: Arc<ResilientBackend>,
}

impl ClusterSupervisor {
    pub(crate) fn new(
        cluster_id: String,
        cluster_config: ClusterConfig,
        config: Arc<SupervisorConfig>,
        backend: Arc<ResilientBackend>,
    ) -> Self {
        Self {
            cluster_id,
            cluster_config,
            config,
            backend,
        }
    }

    /// Stable operator-facing task name used by the process supervisor.
    pub fn task_name(&self) -> String {
        format!("cluster `{}` supervisor", self.cluster_id)
    }

    /// Runs reconnect and single-flight health probes until process cancellation.
    pub async fn run(self, shutdown: CancellationToken) {
        let mut backoff = Backoff::new(
            self.config.health.reconnect_initial_backoff.get(),
            self.config.health.reconnect_max_backoff.get(),
        );

        loop {
            if shutdown.is_cancelled() {
                break;
            }
            let delay = if !self.backend.is_connected() {
                match connect_once(
                    &self.cluster_id,
                    &self.cluster_config,
                    &self.config,
                    &self.backend,
                )
                .await
                {
                    true => {
                        backoff.reset();
                        continue;
                    }
                    false => backoff.next_delay(),
                }
            } else if probe_once(&self.cluster_id, &self.config, &self.backend).await {
                // The probe dropped the backend, so retry on the reconnect backoff instead of
                // waiting a full probe interval before the first reconnect attempt.
                backoff.next_delay()
            } else {
                self.config.health.probe_interval.get()
            };
            if wait_or_stop(delay, &shutdown).await {
                break;
            }
        }
    }
}

/// Attempts one bounded connect, installing the backend on success. Returns whether it connected.
async fn connect_once(
    cluster_id: &str,
    cluster_config: &ClusterConfig,
    config: &SupervisorConfig,
    backend: &ResilientBackend,
) -> bool {
    observability::backend_reconnect_attempt(cluster_id);
    log::info!(
        "connecting cluster {cluster_id} to Fluss at {:?}",
        cluster_config.bootstrap_servers
    );
    let attempt = tokio::time::timeout(
        config.health.reconnect_attempt_timeout.get(),
        NativeGatewayBackend::connect(cluster_config, &config.lookup),
    )
    .await;
    let (result, reason) = match attempt {
        Ok(Ok(native)) => {
            backend.install(Arc::new(native) as Arc<dyn GatewayBackend>);
            observability::backend_reconnect_result(cluster_id, "success", true);
            return true;
        }
        Ok(Err(error)) => {
            log::warn!("failed to connect cluster {cluster_id} to Fluss: {error}");
            ("error", "backend_unreachable")
        }
        Err(_) => {
            log::warn!("Fluss connection attempt for cluster {cluster_id} timed out");
            ("timeout", "connect_timeout")
        }
    };
    backend.health.unreachable(reason);
    observability::backend_reconnect_result(cluster_id, result, false);
    false
}

/// Runs one bounded health probe, dropping the backend when it fails so the next pass reconnects.
///
/// Returns whether the backend is gone after the probe, so the caller takes its next delay from the
/// reconnect backoff instead of waiting a full probe interval before the first reconnect attempt.
async fn probe_once(
    cluster_id: &str,
    config: &SupervisorConfig,
    backend: &ResilientBackend,
) -> bool {
    let Ok(current) = backend.backend() else {
        return true;
    };
    let probe_timeout = config.health.probe_timeout.get();
    let (result, reason) = match tokio::time::timeout(probe_timeout, current.cluster_health()).await
    {
        Ok(Ok(report)) => {
            backend.health.reachable(report);
            observability::backend_probe_result(cluster_id, "success", true);
            return false;
        }
        Ok(Err(error)) => {
            log::warn!("Fluss health probe for cluster {cluster_id} failed: {error}");
            ("error", "backend_unreachable")
        }
        Err(_) => {
            log::warn!("Fluss health probe for cluster {cluster_id} timed out");
            ("timeout", "probe_timeout")
        }
    };
    backend.health.unreachable(reason);
    observability::backend_probe_result(cluster_id, result, false);
    if let Some(stale) = backend.remove() {
        let close = stale.close(probe_timeout).await;
        observability::backend_close_result(cluster_id, close.is_ok());
    }
    true
}

/// Waits for a delay unless shutdown is requested first.
async fn wait_or_stop(duration: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = shutdown.cancelled() => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::model::ClusterStatus;
    use crate::backend::testing::TestBackend;
    use crate::config::ConfigDuration;
    use crate::error::ErrorKind;

    fn unreachable_config() -> GatewayConfig {
        let mut config = GatewayConfig::default();
        config
            .clusters
            .values_mut()
            .next()
            .expect("default cluster")
            .bootstrap_servers = vec!["127.0.0.1:1".to_string()];
        config.health.reconnect_initial_backoff = ConfigDuration::from_secs(1);
        config.health.reconnect_max_backoff = ConfigDuration::from_secs(8);
        config.health.reconnect_attempt_timeout = ConfigDuration::from_secs(1);
        config.health.probe_interval = ConfigDuration::from_secs(5);
        config.health.probe_timeout = ConfigDuration::from_secs(1);
        config
    }

    fn spawn_default(
        config: GatewayConfig,
        backend: Arc<ResilientBackend>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let cluster = config
            .clusters
            .values()
            .next()
            .expect("default cluster")
            .clone();
        let supervisor = ClusterSupervisor::new(
            "default".to_string(),
            cluster,
            Arc::new(SupervisorConfig::from(&config)),
            backend,
        );
        tokio::spawn(supervisor.run(shutdown))
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        for _ in 0..2_000 {
            if ready() {
                return;
            }
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        panic!("the supervisor did not reach the expected state");
    }

    #[test]
    fn backoff_doubles_up_to_the_cap_and_resets() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(4));
        let delays: Vec<Duration> = (0..5).map(|_| backoff.next_delay()).collect();
        let bases = [1, 2, 4, 4, 4];
        for (delay, base) in delays.iter().zip(bases) {
            let base = Duration::from_secs(base);
            assert!(
                *delay >= base * 9 / 10 && *delay <= base * 11 / 10,
                "{delay:?}"
            );
        }
        backoff.reset();
        assert!(backoff.next_delay() <= Duration::from_millis(1100));
    }

    #[test]
    fn health_snapshot_reports_stale_after_the_configured_limit() {
        let health = BackendHealth::new(Duration::from_millis(5));
        assert_eq!(health.snapshot().reason, "starting");
        assert!(!health.snapshot().reachable);

        health.reachable(ClusterHealthReport {
            status: ClusterStatus::Green,
            num_replicas: 1,
            in_sync_replicas: 1,
            num_leader_replicas: 1,
            active_leader_replicas: 1,
        });
        let fresh = health.snapshot();
        assert!(fresh.reachable);
        assert_eq!(fresh.reason, "reachable");

        std::thread::sleep(Duration::from_millis(10));
        let stale = health.snapshot();
        assert!(!stale.reachable);
        assert_eq!(stale.reason, "stale");
        assert!(stale.report.is_some(), "the last report is still reported");

        health.unreachable("backend_unreachable");
        assert_eq!(health.snapshot().reason, "backend_unreachable");
    }

    #[tokio::test]
    async fn disconnected_backend_rejects_requests_as_unavailable() {
        let backend = ResilientBackend::new(Arc::new(BackendHealth::new(Duration::from_secs(1))));
        let error = backend.list_databases().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unavailable);

        backend.install(Arc::new(TestBackend::new()));
        assert!(!backend.list_databases().await.unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_retries_connecting_with_growing_backoff() {
        let config = unreachable_config();
        let health = Arc::new(BackendHealth::new(Duration::from_secs(60)));
        let backend = Arc::new(ResilientBackend::new(health.clone()));
        let shutdown = CancellationToken::new();
        let task = spawn_default(config, backend.clone(), shutdown.clone());

        wait_until(|| !health.snapshot().reachable && health.snapshot().reason != "starting").await;

        // Every retry fails against the closed port, and the slot stays empty across the growing backoff.
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(10)).await;
            tokio::task::yield_now().await;
            assert!(!backend.is_connected());
        }

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the supervisor stops on shutdown")
            .expect("the supervisor task does not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_probes_and_drops_a_failing_backend() {
        let config = unreachable_config();
        let health = Arc::new(BackendHealth::new(Duration::from_secs(60)));
        let backend = Arc::new(ResilientBackend::new(health.clone()));
        let installed = Arc::new(TestBackend::new());
        backend.install(installed.clone());
        let shutdown = CancellationToken::new();
        let task = spawn_default(config, backend.clone(), shutdown.clone());

        wait_until(|| health.snapshot().reachable).await;
        assert!(backend.is_connected());

        installed.set_available(false);
        wait_until(|| health.snapshot().reason == "backend_unreachable").await;
        assert!(
            !backend.is_connected(),
            "a failing backend is dropped so the next pass reconnects"
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the supervisor stops on shutdown")
            .expect("the supervisor task does not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_reconnects_on_the_backoff_after_a_failed_probe() {
        let mut config = unreachable_config();
        config.health.reconnect_initial_backoff = ConfigDuration::from_millis(250);
        let probe_interval = config.health.probe_interval.get();
        let health = Arc::new(BackendHealth::new(Duration::from_secs(60)));
        let backend = Arc::new(ResilientBackend::new(health.clone()));
        let installed = Arc::new(TestBackend::new());
        backend.install(installed.clone());
        let shutdown = CancellationToken::new();
        let task = spawn_default(config, backend.clone(), shutdown.clone());

        wait_until(|| health.snapshot().reachable).await;

        // A hanging probe fails by timeout, giving the drop a reason distinct from connect failures.
        installed.set_cluster_health_hanging(true);
        wait_until(|| health.snapshot().reason == "probe_timeout").await;
        assert!(!backend.is_connected());

        // The first reconnect attempt runs after the reconnect backoff (about 250ms plus the bounded
        // connect attempt), not after the full probe interval.
        let dropped_at = tokio::time::Instant::now();
        wait_until(|| health.snapshot().reason != "probe_timeout").await;
        let waited = dropped_at.elapsed();
        assert!(
            waited < probe_interval,
            "reconnect after a failed probe took {waited:?}, at least the probe interval {probe_interval:?}"
        );
        assert!(
            !backend.is_connected(),
            "the reconnect attempt against the closed port fails and leaves the slot empty"
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the supervisor stops on shutdown")
            .expect("the supervisor task does not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_stops_immediately_when_shutdown_is_already_requested() {
        let backend = Arc::new(ResilientBackend::new(Arc::new(BackendHealth::new(
            Duration::from_secs(60),
        ))));
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let task = spawn_default(unreachable_config(), backend, shutdown);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("no connect is attempted when shutdown is already set")
            .expect("the supervisor task does not panic");
    }
}
