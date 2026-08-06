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

//! Bounded, per-cluster table metadata cache with coalesced refreshes.

use crate::backend::model::TableRef;
use crate::backend::types::ClusterId;
use crate::error::GatewayError;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::time::Instant;

pub const DEFAULT_METADATA_CACHE_MAX_ENTRIES: usize = 1_024;
pub const DEFAULT_METADATA_CACHE_TTL: Duration = Duration::from_secs(60);

const CACHE_LOOKUP: &str = "lookup";
const CACHE_REFRESH: &str = "refresh";
const CACHE_INVALIDATE_TABLE: &str = "invalidate_table";
const CACHE_INVALIDATE_DATABASE: &str = "invalidate_database";
const CACHE_INVALIDATE_PARTITION: &str = "invalidate_partition";
const CACHE_CLEAR: &str = "clear";

struct CacheEntry<T> {
    value: Arc<T>,
    loaded_at: Instant,
}

type RefreshResult<T> = Result<Arc<T>, GatewayError>;
type RefreshSender<T> = watch::Sender<Option<RefreshResult<T>>>;

struct InFlightRefresh<T> {
    id: u64,
    sender: RefreshSender<T>,
}

struct CacheState<T> {
    entries: HashMap<TableRef, CacheEntry<T>>,
    least_to_most_recent: VecDeque<TableRef>,
    in_flight: HashMap<TableRef, InFlightRefresh<T>>,
    next_refresh_id: u64,
    invalidation_generation: u64,
}

impl<T> Default for CacheState<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            least_to_most_recent: VecDeque::new(),
            in_flight: HashMap::new(),
            next_refresh_id: 0,
            invalidation_generation: 0,
        }
    }
}

/// Cancellation guard for the caller elected to run a coalesced refresh.
///
/// Aborting that caller drops this guard. It synchronously wakes existing waiters with a transient error and removes
/// the abandoned single-flight slot asynchronously. A refresh ID prevents delayed cleanup from deleting a newer
/// leader for the same table.
struct RefreshLeaderGuard<T>
where
    T: Send + Sync + 'static,
{
    state: Arc<Mutex<CacheState<T>>>,
    table: TableRef,
    cluster: String,
    refresh_id: u64,
    sender: RefreshSender<T>,
    armed: bool,
}

impl<T> RefreshLeaderGuard<T>
where
    T: Send + Sync + 'static,
{
    fn new(
        state: Arc<Mutex<CacheState<T>>>,
        table: TableRef,
        cluster: String,
        refresh_id: u64,
        sender: RefreshSender<T>,
    ) -> Self {
        Self {
            state,
            table,
            cluster,
            refresh_id,
            sender,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<T> Drop for RefreshLeaderGuard<T>
where
    T: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _ = self.sender.send(Some(Err(GatewayError::unavailable(
            "metadata refresh was interrupted",
        ))));
        record_cache_operation(&self.cluster, CACHE_REFRESH, "interrupted");

        if let Ok(mut state) = self.state.try_lock() {
            remove_in_flight_if_current(&mut state, &self.table, self.refresh_id);
            return;
        }

        let state = Arc::clone(&self.state);
        let table = self.table.clone();
        let refresh_id = self.refresh_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut state = state.lock().await;
                remove_in_flight_if_current(&mut state, &table, refresh_id);
            });
        }
    }
}

/// One table cache owned by exactly one configured cluster runtime.
///
/// Missing tables and loader failures are never cached. A refresh for one table is shared by all
/// concurrent callers. Invalidations also prevent an older in-flight refresh from repopulating the
/// cache after a successful DDL operation.
pub struct TableMetadataCache<T> {
    cluster: ClusterId,
    max_entries: usize,
    ttl: Duration,
    state: Arc<Mutex<CacheState<T>>>,
}

impl<T> TableMetadataCache<T>
where
    T: Send + Sync + 'static,
{
    pub fn with_defaults(cluster: ClusterId) -> Self {
        Self {
            cluster,
            max_entries: DEFAULT_METADATA_CACHE_MAX_ENTRIES,
            ttl: DEFAULT_METADATA_CACHE_TTL,
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub fn new(
        cluster: ClusterId,
        max_entries: usize,
        ttl: Duration,
    ) -> Result<Self, GatewayError> {
        if max_entries == 0 {
            return Err(GatewayError::invalid_argument(
                "metadata.cache_max_entries must be positive",
            ));
        }
        if ttl.is_zero() {
            return Err(GatewayError::invalid_argument(
                "metadata.cache_ttl must be positive",
            ));
        }
        Ok(Self {
            cluster,
            max_entries,
            ttl,
            state: Arc::new(Mutex::new(CacheState::default())),
        })
    }

    pub fn cluster(&self) -> &ClusterId {
        &self.cluster
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Returns fresh metadata or coalesces one authoritative load for this table.
    pub async fn get_or_load<F, Fut>(&self, table: &TableRef, loader: F) -> RefreshResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, GatewayError>>,
    {
        self.load(table, false, loader).await
    }

    /// Evicts any cached value and coalesces one authoritative refresh.
    ///
    /// This is the hook used by write preflight after a schema mismatch against the cached table
    /// shape. The caller decides whether to repeat preflight, and must call it at most once per
    /// request.
    pub async fn refresh<F, Fut>(&self, table: &TableRef, loader: F) -> RefreshResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, GatewayError>>,
    {
        self.load(table, true, loader).await
    }

    async fn load<F, Fut>(&self, table: &TableRef, force: bool, loader: F) -> RefreshResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, GatewayError>>,
    {
        enum Action<T> {
            Wait(watch::Receiver<Option<RefreshResult<T>>>),
            Load {
                sender: RefreshSender<T>,
                generation: u64,
                refresh_id: u64,
            },
        }

        let action = {
            let mut state = self.state.lock().await;
            if force {
                remove_entry(&mut state, table);
                record_cache_operation(self.cluster.as_str(), CACHE_LOOKUP, "forced");
            } else {
                let fresh = state
                    .entries
                    .get(table)
                    .filter(|entry| entry.loaded_at.elapsed() < self.ttl)
                    .map(|entry| Arc::clone(&entry.value));
                if let Some(value) = fresh {
                    touch(&mut state.least_to_most_recent, table);
                    record_cache_operation(self.cluster.as_str(), CACHE_LOOKUP, "hit");
                    return Ok(value);
                }
                remove_entry(&mut state, table);
                record_cache_operation(self.cluster.as_str(), CACHE_LOOKUP, "miss");
            }

            let waiter = state.in_flight.get(table).and_then(|refresh| {
                refresh
                    .sender
                    .borrow()
                    .is_none()
                    .then(|| refresh.sender.subscribe())
            });
            if let Some(receiver) = waiter {
                Action::Wait(receiver)
            } else {
                // A cancelled leader has already notified its waiters. Do not let delayed asynchronous cleanup make
                // a later caller observe that completed slot instead of electing a replacement.
                state.in_flight.remove(table);
                let (sender, _receiver) = watch::channel(None);
                let refresh_id = state.next_refresh_id;
                state.next_refresh_id = state.next_refresh_id.wrapping_add(1);
                state.in_flight.insert(
                    table.clone(),
                    InFlightRefresh {
                        id: refresh_id,
                        sender: sender.clone(),
                    },
                );
                Action::Load {
                    sender,
                    generation: state.invalidation_generation,
                    refresh_id,
                }
            }
        };

        match action {
            Action::Wait(receiver) => {
                let result = wait_for_refresh(receiver).await;
                record_cache_operation(
                    self.cluster.as_str(),
                    CACHE_REFRESH,
                    if result.is_ok() {
                        "coalesced_success"
                    } else {
                        "coalesced_error"
                    },
                );
                result
            }
            Action::Load {
                sender,
                generation,
                refresh_id,
            } => {
                let mut leader = RefreshLeaderGuard::new(
                    Arc::clone(&self.state),
                    table.clone(),
                    self.cluster.to_string(),
                    refresh_id,
                    sender.clone(),
                );
                let loaded = loader().await.map(Arc::new);
                {
                    let mut state = self.state.lock().await;
                    remove_in_flight_if_current(&mut state, table, refresh_id);
                    // A DDL invalidation that raced this load wins and prevents stale repopulation.
                    if generation == state.invalidation_generation {
                        if let Ok(value) = &loaded {
                            insert_entry(
                                &mut state,
                                table.clone(),
                                Arc::clone(value),
                                self.max_entries,
                            );
                        }
                    }
                    record_cache_entries(self.cluster.as_str(), state.entries.len());
                }
                // Waiters receive exactly the leader's result. Failure is deliberately not retained.
                let _ = sender.send(Some(loaded.clone()));
                record_cache_operation(
                    self.cluster.as_str(),
                    CACHE_REFRESH,
                    if loaded.is_ok() { "success" } else { "error" },
                );
                leader.disarm();
                loaded
            }
        }
    }

    /// Invalidates one table after create, alter, or drop.
    pub async fn invalidate_table(&self, table: &TableRef) {
        self.invalidate_one(table, CACHE_INVALIDATE_TABLE).await;
    }

    /// Invalidates every cached table in a database after a successful database drop.
    pub async fn invalidate_database(&self, database: &str) {
        let mut state = self.state.lock().await;
        state.invalidation_generation = state.invalidation_generation.wrapping_add(1);
        let tables: Vec<TableRef> = state
            .entries
            .keys()
            .filter(|table| table.database == database)
            .cloned()
            .collect();
        for table in tables {
            remove_entry(&mut state, &table);
        }
        record_cache_operation(self.cluster.as_str(), CACHE_INVALIDATE_DATABASE, "success");
        record_cache_entries(self.cluster.as_str(), state.entries.len());
    }

    /// Partition creation or deletion invalidates the parent table metadata.
    pub async fn invalidate_partition(&self, table: &TableRef) {
        self.invalidate_one(table, CACHE_INVALIDATE_PARTITION).await;
    }

    /// Removes one table entry and fences racing in-flight refreshes, labeled per mutation kind.
    async fn invalidate_one(&self, table: &TableRef, operation: &'static str) {
        let mut state = self.state.lock().await;
        state.invalidation_generation = state.invalidation_generation.wrapping_add(1);
        remove_entry(&mut state, table);
        record_cache_operation(self.cluster.as_str(), operation, "success");
        record_cache_entries(self.cluster.as_str(), state.entries.len());
    }

    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.invalidation_generation = state.invalidation_generation.wrapping_add(1);
        state.entries.clear();
        state.least_to_most_recent.clear();
        record_cache_operation(self.cluster.as_str(), CACHE_CLEAR, "success");
        record_cache_entries(self.cluster.as_str(), 0);
    }

    pub async fn len(&self) -> usize {
        self.state.lock().await.entries.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

fn record_cache_operation(cluster: &str, operation: &'static str, result: &'static str) {
    crate::observability::metadata_cache_operation(cluster, operation, result);
}

fn record_cache_entries(cluster: &str, entries: usize) {
    crate::observability::metadata_cache_entries(cluster, entries);
}

fn remove_in_flight_if_current<T>(state: &mut CacheState<T>, table: &TableRef, refresh_id: u64) {
    if state
        .in_flight
        .get(table)
        .is_some_and(|refresh| refresh.id == refresh_id)
    {
        state.in_flight.remove(table);
    }
}

async fn wait_for_refresh<T>(
    mut receiver: watch::Receiver<Option<RefreshResult<T>>>,
) -> RefreshResult<T> {
    loop {
        if let Some(result) = receiver.borrow().clone() {
            return result;
        }
        receiver.changed().await.map_err(|_| {
            GatewayError::internal("metadata refresh ended without producing a result")
        })?;
    }
}

fn insert_entry<T>(state: &mut CacheState<T>, table: TableRef, value: Arc<T>, max_entries: usize) {
    remove_entry(state, &table);
    state.entries.insert(
        table.clone(),
        CacheEntry {
            value,
            loaded_at: Instant::now(),
        },
    );
    state.least_to_most_recent.push_back(table);
    while state.entries.len() > max_entries {
        if let Some(evicted) = state.least_to_most_recent.pop_front() {
            state.entries.remove(&evicted);
        }
    }
}

fn remove_entry<T>(state: &mut CacheState<T>, table: &TableRef) {
    state.entries.remove(table);
    if let Some(index) = state
        .least_to_most_recent
        .iter()
        .position(|candidate| candidate == table)
    {
        state.least_to_most_recent.remove(index);
    }
}

fn touch(recency: &mut VecDeque<TableRef>, table: &TableRef) {
    if let Some(index) = recency.iter().position(|candidate| candidate == table) {
        recency.remove(index);
    }
    recency.push_back(table.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Barrier, oneshot};

    fn cluster() -> ClusterId {
        ClusterId::try_from("local").unwrap()
    }

    fn metadata(version: i32) -> String {
        format!("v{version}")
    }

    #[tokio::test]
    async fn validates_limits() {
        assert!(TableMetadataCache::<String>::new(cluster(), 0, Duration::from_secs(1)).is_err());
        assert!(TableMetadataCache::<String>::new(cluster(), 1, Duration::ZERO).is_err());
        let cache = TableMetadataCache::<String>::with_defaults(cluster());
        assert_eq!(cache.max_entries(), DEFAULT_METADATA_CACHE_MAX_ENTRIES);
        assert_eq!(cache.ttl(), DEFAULT_METADATA_CACHE_TTL);
    }

    #[tokio::test]
    async fn coalesces_concurrent_loaders_and_shares_one_result() {
        let cache =
            Arc::new(TableMetadataCache::new(cluster(), 4, Duration::from_secs(60)).unwrap());
        let table = TableRef::new("db", "table");
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let table = table.clone();
            let calls = Arc::clone(&calls);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_load(&table, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok(metadata(7))
                    })
                    .await
                    .unwrap()
            }));
        }
        for task in tasks {
            let value = task.await.unwrap();
            assert_eq!(*value, "v7");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_expiry_refreshes_and_failures_are_not_cached() {
        let cache = TableMetadataCache::new(cluster(), 4, Duration::from_secs(60)).unwrap();
        let table = TableRef::new("db", "table");
        cache
            .get_or_load(&table, || async { Ok(metadata(1)) })
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(61)).await;
        let error = cache
            .get_or_load(&table, || async {
                Err(GatewayError::unavailable("metadata unavailable"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.message(), "metadata unavailable");
        assert!(cache.is_empty().await);

        let refreshed = cache
            .get_or_load(&table, || async { Ok(metadata(2)) })
            .await
            .unwrap();
        assert_eq!(*refreshed, "v2");
    }

    #[tokio::test]
    async fn uses_lru_eviction_and_all_invalidation_hooks() {
        let cache = TableMetadataCache::new(cluster(), 2, Duration::from_secs(60)).unwrap();
        let first = TableRef::new("db", "first");
        let second = TableRef::new("db", "second");
        let third = TableRef::new("other", "third");
        cache
            .get_or_load(&first, || async { Ok(metadata(1)) })
            .await
            .unwrap();
        cache
            .get_or_load(&second, || async { Ok(metadata(2)) })
            .await
            .unwrap();
        cache
            .get_or_load(&first, || async { unreachable!() })
            .await
            .unwrap();
        cache
            .get_or_load(&third, || async { Ok(metadata(3)) })
            .await
            .unwrap();

        let second_calls = AtomicUsize::new(0);
        cache
            .get_or_load(&second, || async {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(metadata(4))
            })
            .await
            .unwrap();
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);

        cache.invalidate_partition(&second).await;
        cache.invalidate_database("db").await;
        assert_eq!(cache.len().await, 1);
        cache.invalidate_table(&third).await;
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn invalidation_prevents_in_flight_refresh_from_repopulating_cache() {
        let cache =
            Arc::new(TableMetadataCache::new(cluster(), 2, Duration::from_secs(60)).unwrap());
        let table = TableRef::new("db", "table");
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let task = {
            let cache = Arc::clone(&cache);
            let table = table.clone();
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                cache
                    .get_or_load(&table, || async move {
                        started.wait().await;
                        release.wait().await;
                        Ok(metadata(1))
                    })
                    .await
                    .unwrap()
            })
        };
        started.wait().await;
        cache.invalidate_table(&table).await;
        release.wait().await;
        assert_eq!(*task.await.unwrap(), "v1");
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn aborted_leader_notifies_waiters_and_allows_recovery() {
        let cache =
            Arc::new(TableMetadataCache::new(cluster(), 2, Duration::from_secs(60)).unwrap());
        let table = TableRef::new("db", "table");
        let (started_tx, started_rx) = oneshot::channel();
        let leader = {
            let cache = Arc::clone(&cache);
            let table = table.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(&table, || async move {
                        let _ = started_tx.send(());
                        std::future::pending::<Result<String, GatewayError>>().await
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let waiter = {
            let cache = Arc::clone(&cache);
            let table = table.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(&table, || async {
                        panic!("a waiter must not run its loader")
                    })
                    .await
            })
        };
        wait_for_waiter(&cache, &table).await;

        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        let waiter_error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must be notified when its leader is aborted")
            .unwrap()
            .unwrap_err();
        assert_eq!(waiter_error.kind(), crate::error::ErrorKind::Unavailable);
        assert_eq!(waiter_error.message(), "metadata refresh was interrupted");
        wait_for_no_in_flight(&cache).await;

        let recovered = cache
            .get_or_load(&table, || async { Ok(metadata(2)) })
            .await
            .unwrap();
        assert_eq!(*recovered, "v2");
    }

    #[tokio::test]
    async fn aborted_leader_cleanup_is_safe_when_the_state_lock_is_contended() {
        let cache =
            Arc::new(TableMetadataCache::new(cluster(), 2, Duration::from_secs(60)).unwrap());
        let table = TableRef::new("db", "table");
        let (started_tx, started_rx) = oneshot::channel();
        let leader = {
            let cache = Arc::clone(&cache);
            let table = table.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(&table, || async move {
                        let _ = started_tx.send(());
                        std::future::pending::<Result<String, GatewayError>>().await
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let state_guard = cache.state.lock().await;
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        assert!(state_guard.in_flight.contains_key(&table));
        drop(state_guard);

        wait_for_no_in_flight(&cache).await;
        let recovered = cache
            .get_or_load(&table, || async { Ok(metadata(3)) })
            .await
            .unwrap();
        assert_eq!(*recovered, "v3");
    }

    #[tokio::test]
    async fn emits_cache_metrics_with_only_the_configured_cluster_resource_label() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let cache = TableMetadataCache::new(cluster(), 2, Duration::from_secs(60)).unwrap();
        let table = TableRef::new("db", "secret_table_name");

        cache
            .get_or_load(&table, || async { Ok(metadata(1)) })
            .await
            .unwrap();
        cache
            .get_or_load(&table, || async { unreachable!() })
            .await
            .unwrap();
        cache.invalidate_table(&table).await;

        let output = handle.render();
        assert_metric_labels(
            &output,
            "fluss_gateway_metadata_cache_operations_total",
            &["cluster", "operation", "result"],
        );
        assert_metric_labels(
            &output,
            "fluss_gateway_metadata_cache_entries",
            &["cluster"],
        );
        assert!(output.contains("cluster=\"local\""));
        assert!(!output.contains("secret_table_name"));
        assert!(!output.contains("database="));
        assert!(!output.contains("table="));
    }

    async fn wait_for_waiter(cache: &TableMetadataCache<String>, table: &TableRef) {
        for _ in 0..100 {
            let receiver_count = cache
                .state
                .lock()
                .await
                .in_flight
                .get(table)
                .map(|refresh| refresh.sender.receiver_count())
                .unwrap_or_default();
            if receiver_count > 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("waiter did not subscribe to the in-flight refresh");
    }

    async fn wait_for_no_in_flight(cache: &TableMetadataCache<String>) {
        for _ in 0..100 {
            if cache.state.lock().await.in_flight.is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("cancelled refresh slot was not removed");
    }

    fn assert_metric_labels(output: &str, metric: &str, expected: &[&str]) {
        let lines = output
            .lines()
            .filter(|line| line.starts_with(metric))
            .collect::<Vec<_>>();
        assert!(
            !lines.is_empty(),
            "metric {metric} was not emitted: {output}"
        );
        for line in lines {
            let labels = line
                .split_once('{')
                .and_then(|(_, suffix)| suffix.split_once('}'))
                .map(|(labels, _)| {
                    labels
                        .split(',')
                        .map(|label| label.split_once('=').unwrap().0)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            assert_eq!(labels, expected, "unexpected labels on {line}");
        }
    }
}
