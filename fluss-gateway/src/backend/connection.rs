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

//! The per-cluster connection pool a [`crate::backend::client::NativeFlussBackend`] owns.
//!
//! One pool per configured cluster, one entry per identity: lazy creation on the cold path, one dial
//! per entry, capacity, and idle reclamation.
//!
//! **This is not a checkout pool.** A `fluss-rs` connection multiplexes: one connection serves any
//! number of concurrent requests over a background reader, so there is no lease, no return, no maximum
//! lifetime, and no liveness query. Database-connection-pool intuition does not apply.
//!
//! Two things this pool deliberately does not do. It does not retry a dial: `fluss-rs` already walks
//! every bootstrap address and retries a single one with the same backoff a gateway retry would use.
//! And it does not evict a connection when a request fails on it: a broken transport is reported per
//! server and `RpcClient::get_connection` reconnects that one server, so discarding the logical client
//! would only throw away its cluster metadata and cached sub-clients.

use crate::backend::context::{Principal, RequestContext};
use crate::backend::errors::map_fluss_error;
use crate::backend::types::ClusterId;
use crate::config::{ClusterConfig, IdentityMode};
use crate::error::{GatewayError, GatewayResult, Resource};
use crate::observability;
use fluss::client::FlussConnection;
use fluss::config::Config as NativeClientConfig;
use fluss::error::Error as FlussClientError;
use futures_util::future::join_all;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

/// Budget for draining one connection that idle reclamation took out of the pool.
///
/// It is idle by definition, so the drain normally completes at once; this only stops a stuck sender
/// task from turning reclamation into a wait.
const RECLAIM_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Opens and releases the connections of one cluster.
///
/// The pool owns the algorithm — admission, capacity, dial coalescing, reclamation — and this trait
/// owns what a connection *is*, which is what lets the algorithm be tested without a cluster.
pub(crate) trait Connector: Send + Sync + 'static {
    type Conn: Send + Sync + 'static;

    fn dial(
        &self,
        identity: &Principal,
    ) -> impl Future<Output = Result<Self::Conn, FlussClientError>> + Send;

    /// Releases a connection, draining pending writes within `timeout`.
    fn close(
        &self,
        connection: Arc<Self::Conn>,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), FlussClientError>> + Send;
}

/// Dials the native Fluss client with the cluster's service credentials.
pub(crate) struct NativeConnector {
    config: NativeClientConfig,
}

impl NativeConnector {
    pub(crate) fn new(config: &ClusterConfig) -> Self {
        Self {
            config: config.native_client_config(),
        }
    }
}

impl Connector for NativeConnector {
    type Conn = FlussConnection;

    /// The identity is unused until `fluss-rs` can carry an act-user-id; configuration validation
    /// refuses `identity-mode: user` until then, so only the service identity reaches here.
    ///
    /// The connection also has no per-RPC timeout, because `fluss-rs` only exposes
    /// [`FlussConnection::new`]: `ServerConnection` deliberately stops cleaning up a request once it
    /// went out, so a request that runs out of budget abandons its future while the RPC stays
    /// registered, and a server that accepts connections but never answers leaks one entry per gateway
    /// timeout. Idle reclamation is what frees them until `fluss-rs` can bound the RPC itself — dropping
    /// the connection drops its whole in-flight map (the write path's counterpart of that gap is
    /// apache/fluss#3861).
    async fn dial(&self, _identity: &Principal) -> Result<FlussConnection, FlussClientError> {
        FlussConnection::new(self.config.clone()).await
    }

    async fn close(
        &self,
        connection: Arc<FlussConnection>,
        timeout: Duration,
    ) -> Result<(), FlussClientError> {
        connection.close(timeout).await
    }
}

/// Which entry a request lands on, and how many entries may live at once.
///
/// Not a trait: both identity modes run the same algorithm over the same storage, differing only in
/// the key they derive. It becomes a trait the day a mode differs in *behaviour* — a credential that
/// needs background refreshing, say.
enum Identity {
    /// Service mode: every request maps to one shared identity, so the pool holds at most one entry —
    /// a capacity of one follows from that rather than being a setting (FIP-49 ignores
    /// `connection.max` here).
    Service(Principal),
    /// User mode: one entry per caller, bounded by `connection.max`.
    User { capacity: usize },
}

impl Identity {
    fn from_config(config: &ClusterConfig) -> Self {
        match config.identity_mode {
            IdentityMode::Service => Self::Service(Principal::service()),
            IdentityMode::User => Self::User {
                capacity: config.connection_max().max(1),
            },
        }
    }

    /// The only place the identity mode is branched on.
    fn key(&self, ctx: &RequestContext) -> Principal {
        match self {
            Self::Service(service) => service.clone(),
            Self::User { .. } => ctx.principal().clone(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Service(_) => 1,
            Self::User { capacity } => *capacity,
        }
    }
}

/// One identity's slot, with the cheapest synchronisation each field allows.
struct Entry<C> {
    /// Set once by whoever wins the dial gate, having found it empty, and never replaced — so
    /// steady-state reads are lock free, and a connection leaves the pool only with its entry.
    current: OnceLock<Arc<C>>,
    /// The dial critical section, per entry, so different identities dial in parallel. It carries
    /// **only the last dial failure** — a dial outcome is the same for every waiter on this entry, so
    /// it can be shared; a read or write failure belongs to one request and never lands here.
    dial_gate: tokio::sync::Mutex<Option<(Instant, GatewayError)>>,
    last_used: Mutex<Instant>,
}

impl<C> Entry<C> {
    fn new() -> Self {
        Self {
            current: OnceLock::new(),
            dial_gate: tokio::sync::Mutex::new(None),
            last_used: Mutex::new(Instant::now()),
        }
    }

    fn connection(&self) -> Option<Arc<C>> {
        self.current.get().cloned()
    }

    fn has_connection(&self) -> bool {
        self.current.get().is_some()
    }

    fn install(&self, connection: Arc<C>) {
        let _ = self.current.set(connection);
    }

    fn touch(&self) {
        *self
            .last_used
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_used
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed()
    }
}

type Entries<C> = HashMap<Principal, Arc<Entry<C>>>;

/// The connections of one cluster, keyed by identity.
pub(crate) struct ClusterPool<K: Connector> {
    cluster: ClusterId,
    identity: Identity,
    /// Applies to both identity modes: a connection left idle this long has almost certainly been
    /// dropped by a NAT or load balancer already, and rebuilding on the next request is cheaper than
    /// discovering that through a failed one.
    idle_timeout: Duration,
    connector: K,
    entries: RwLock<Entries<K::Conn>>,
}

impl<K: Connector> ClusterPool<K> {
    pub(crate) fn new(cluster: ClusterId, config: &ClusterConfig, connector: K) -> Self {
        Self {
            cluster,
            identity: Identity::from_config(config),
            idle_timeout: config.connection_idle_timeout(),
            connector,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// The entry this request lands on.
    pub(crate) fn key(&self, ctx: &RequestContext) -> Principal {
        self.identity.key(ctx)
    }

    /// The connection of `key`, dialing it on the cold path.
    ///
    /// Concurrent first requests on one entry share a single dial and a single answer: the winner
    /// installs the connection, and everyone whose wait overlapped a failed dial gets that failure
    /// instead of piling further attempts onto an unreachable cluster.
    pub(crate) async fn connection(&self, key: &Principal) -> GatewayResult<Arc<K::Conn>> {
        // Taken before queueing, so "the dial that ran while I waited" is decidable.
        let arrived = Instant::now();
        let (admitted, expired) = {
            let mut expired = Vec::new();
            let admitted = self.admit(key, &mut expired);
            (admitted, expired)
        };
        // Drained even when admission was refused: the connections are already out of the pool, so
        // nothing else would ever close them.
        self.close_expired(expired).await;
        let entry = admitted?;
        if let Some(live) = entry.connection() {
            return Ok(live);
        }

        let mut gate = entry.dial_gate.lock().await;
        if let Some(live) = entry.connection() {
            return Ok(live);
        }
        if let Some((failed_at, error)) = gate.as_ref()
            && *failed_at >= arrived
        {
            return Err(error.clone());
        }

        match self.connector.dial(key).await {
            Ok(connection) => {
                let connection = Arc::new(connection);
                entry.install(connection.clone());
                *gate = None;
                observability::connection_created(self.cluster.as_str());
                observability::connections_active(self.cluster.as_str(), self.active());
                log::info!("connected to Fluss cluster `{}` as `{key}`", self.cluster);
                Ok(connection)
            }
            Err(native) => {
                let error = map_fluss_error("connect to Fluss", native);
                *gate = Some((Instant::now(), error.clone()));
                Err(error)
            }
        }
    }

    /// Releases entries nobody is using any more, draining each connection on the way out.
    ///
    /// A cluster that stops receiving requests never reaches [`Self::admit`]'s slow path again, so this
    /// is the only thing that lets its connection go.
    pub(crate) async fn reclaim_idle(&self) {
        let expired = self.retain_live(&mut self.write_entries());
        self.close_expired(expired).await;
    }

    /// Drains and releases the connections reclamation took out of the pool.
    ///
    /// Dropping them would not be enough: `WriterClient` has no `Drop`, so a dropped connection leaves
    /// its sender task detached with nothing able to reach it — batches short of their linger are never
    /// flushed, and the rows in them never get a verdict.
    ///
    /// Concurrent, because [`Self::admit`] drains on the request path: closing one after another would
    /// charge a single request the timeout of every expired connection.
    async fn close_expired(&self, expired: Vec<Arc<K::Conn>>) {
        if expired.is_empty() {
            return;
        }
        let results = join_all(
            expired
                .into_iter()
                .map(|connection| self.connector.close(connection, RECLAIM_CLOSE_TIMEOUT)),
        )
        .await;
        for result in results {
            if let Err(error) = result {
                log::warn!(
                    "failed to drain an idle Fluss connection of cluster `{}`: {error}",
                    self.cluster
                );
            }
            observability::connection_closed(self.cluster.as_str(), "idle");
        }
        observability::connections_active(self.cluster.as_str(), self.active());
    }

    /// Closes every connection within `timeout` and empties the pool. Idempotent.
    pub(crate) async fn close_all(&self, timeout: Duration) -> GatewayResult<()> {
        let connections: Vec<Arc<K::Conn>> = {
            let mut entries = self.write_entries();
            let taken = entries
                .values()
                .filter_map(|entry| entry.connection())
                .collect();
            entries.clear();
            taken
        };
        if connections.is_empty() {
            return Ok(());
        }

        let closes = connections.len();
        let results = join_all(
            connections
                .into_iter()
                .map(|connection| self.connector.close(connection, timeout)),
        )
        .await;
        for _ in 0..closes {
            observability::connection_closed(self.cluster.as_str(), "shutdown");
        }
        observability::connections_active(self.cluster.as_str(), 0);
        results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map(|_| ())
            .map_err(|error| map_fluss_error("close the Fluss connection", error))
    }

    /// Finds or admits the entry of one identity, collecting into `expired` the connections its
    /// reclamation pass took out of the pool for the caller to drain.
    ///
    /// Inserting the entry *is* the admission token, so concurrent first requests cannot structurally
    /// exceed the capacity. Reclamation runs before the capacity check, or a pool full of idle
    /// connections would starve a new identity.
    fn admit(
        &self,
        key: &Principal,
        expired: &mut Vec<Arc<K::Conn>>,
    ) -> GatewayResult<Arc<Entry<K::Conn>>> {
        if let Some(entry) = self.read_entries().get(key) {
            entry.touch();
            return Ok(entry.clone());
        }

        let mut entries = self.write_entries();
        if let Some(entry) = entries.get(key) {
            entry.touch();
            return Ok(entry.clone());
        }
        expired.extend(self.retain_live(&mut entries));
        if entries.len() >= self.identity.capacity() {
            return Err(GatewayError::resource_exhausted(format!(
                "cluster `{}` reached its limit of {} connections",
                self.cluster,
                self.identity.capacity()
            ))
            .with_resource(Resource::Cluster));
        }
        let entry = Arc::new(Entry::new());
        entries.insert(key.clone(), entry.clone());
        Ok(entry)
    }

    /// Drops the entries no caller holds any more and returns the connections among them, which the
    /// caller has to drain.
    ///
    /// Two reference counts have to be one, and they mean different things. `strong_count(entry) == 1`
    /// says nobody is inside [`Self::connection`], so removing the entry cannot orphan a connection
    /// someone is about to install. `strong_count(connection) == 1` says no request is *using* it: a
    /// request holds the connection, not the entry, for the whole operation, and `last_used` is stamped
    /// before that operation starts — so the entry looks unreferenced and idle while the work is still
    /// running. Both are evaluated under the write lock, and neither `Arc` escapes this module, so the
    /// test is exact rather than heuristic.
    fn retain_live(&self, entries: &mut Entries<K::Conn>) -> Vec<Arc<K::Conn>> {
        let mut expired = Vec::new();
        entries.retain(|key, entry| {
            if Arc::strong_count(entry) > 1 {
                return true;
            }
            // An entry without a connection is what a failed or cancelled dial leaves behind.
            let Some(connection) = entry.current.get() else {
                return false;
            };
            if Arc::strong_count(connection) > 1 || entry.idle_for() < self.idle_timeout {
                return true;
            }
            log::info!(
                "reclaimed the idle Fluss connection of cluster `{}` for `{key}`",
                self.cluster
            );
            expired.push(connection.clone());
            false
        });
        expired
    }

    fn active(&self) -> usize {
        self.read_entries()
            .values()
            .filter(|entry| entry.has_connection())
            .count()
    }

    fn read_entries(&self) -> RwLockReadGuard<'_, Entries<K::Conn>> {
        self.entries.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_entries(&self) -> RwLockWriteGuard<'_, Entries<K::Conn>> {
        self.entries.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::errors::tests::api_failure;
    use crate::config::ConfigDuration;
    use crate::error::ErrorKind;
    use fluss::error::FlussError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A stand-in connection: the pool never looks inside one, so identity by address is enough.
    #[derive(Debug)]
    struct FakeConnection;

    #[derive(Clone)]
    enum Outcome {
        Connect,
        Fail(FlussError),
        Hang,
    }

    /// A connector that records what it was asked to do and answers from a scripted outcome.
    struct Dialer {
        dials: Mutex<Vec<Principal>>,
        closes: AtomicUsize,
        outcome: Mutex<Outcome>,
        latency: Duration,
    }

    impl Dialer {
        fn new(outcome: Outcome, latency: Duration) -> Arc<Self> {
            Arc::new(Self {
                dials: Mutex::new(Vec::new()),
                closes: AtomicUsize::new(0),
                outcome: Mutex::new(outcome),
                latency,
            })
        }

        fn connecting() -> Arc<Self> {
            Self::new(Outcome::Connect, Duration::ZERO)
        }

        fn set(&self, outcome: Outcome) {
            *self.outcome.lock().unwrap() = outcome;
        }

        fn dialed(&self) -> Vec<String> {
            self.dials
                .lock()
                .unwrap()
                .iter()
                .map(|principal| principal.name().to_string())
                .collect()
        }

        fn closes(&self) -> usize {
            self.closes.load(Ordering::SeqCst)
        }
    }

    impl Connector for Arc<Dialer> {
        type Conn = FakeConnection;

        fn dial(
            &self,
            identity: &Principal,
        ) -> impl Future<Output = Result<FakeConnection, FlussClientError>> + Send {
            let dialer = self.clone();
            let identity = identity.clone();
            async move {
                dialer.dials.lock().unwrap().push(identity);
                let outcome = dialer.outcome.lock().unwrap().clone();
                if !dialer.latency.is_zero() {
                    tokio::time::sleep(dialer.latency).await;
                }
                match outcome {
                    Outcome::Connect => Ok(FakeConnection),
                    Outcome::Fail(error) => Err(api_failure(error)),
                    Outcome::Hang => std::future::pending().await,
                }
            }
        }

        fn close(
            &self,
            _connection: Arc<FakeConnection>,
            _timeout: Duration,
        ) -> impl Future<Output = Result<(), FlussClientError>> + Send {
            self.closes.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(()))
        }
    }

    fn pool(dialer: &Arc<Dialer>, config: ClusterConfig) -> ClusterPool<Arc<Dialer>> {
        ClusterPool::new(
            ClusterId::try_from("default").unwrap(),
            &config,
            dialer.clone(),
        )
    }

    fn service_pool(dialer: &Arc<Dialer>) -> ClusterPool<Arc<Dialer>> {
        pool(dialer, ClusterConfig::default())
    }

    fn user_pool(
        dialer: &Arc<Dialer>,
        max: u32,
        idle_timeout: Duration,
    ) -> ClusterPool<Arc<Dialer>> {
        pool(
            dialer,
            ClusterConfig {
                identity_mode: IdentityMode::User,
                connection_max: Some(max),
                connection_idle_timeout: Some(ConfigDuration::from_millis(
                    u64::try_from(idle_timeout.as_millis()).unwrap(),
                )),
                ..ClusterConfig::default()
            },
        )
    }

    /// A service pool whose entries expire immediately, so reclamation is observable without sleeping.
    fn expiring_pool(dialer: &Arc<Dialer>) -> ClusterPool<Arc<Dialer>> {
        pool(
            dialer,
            ClusterConfig {
                connection_idle_timeout: Some(ConfigDuration::from_millis(0)),
                ..ClusterConfig::default()
            },
        )
    }

    fn ctx(user: &str) -> RequestContext {
        RequestContext::for_test_as("default", user, Duration::from_secs(30))
    }

    /// A burst of first requests dials once, and every caller gets that same connection.
    #[tokio::test]
    async fn concurrent_first_requests_share_a_single_dial() {
        let dialer = Dialer::new(Outcome::Connect, Duration::from_millis(50));
        let pool = Arc::new(service_pool(&dialer));
        let key = pool.key(&ctx("anyone"));

        let connections = join_all((0..32).map(|_| {
            let pool = pool.clone();
            let key = key.clone();
            async move { pool.connection(&key).await.unwrap() }
        }))
        .await;

        assert_eq!(dialer.dialed().len(), 1);
        assert!(
            connections
                .windows(2)
                .all(|pair| Arc::ptr_eq(&pair[0], &pair[1])),
            "every caller must share the pooled connection"
        );
        assert_eq!(pool.active(), 1);
    }

    /// A dial failure is answered to everyone who waited for it, and leaves no entry behind to
    /// occupy capacity; once the cluster answers again the next request reconnects.
    #[tokio::test]
    async fn a_failed_dial_is_shared_and_leaves_no_entry() {
        let dialer = Dialer::new(
            Outcome::Fail(FlussError::NetworkException),
            Duration::from_millis(50),
        );
        let pool = Arc::new(service_pool(&dialer));
        let key = pool.key(&ctx("anyone"));

        let failures = join_all((0..8).map(|_| {
            let pool = pool.clone();
            let key = key.clone();
            async move { pool.connection(&key).await.unwrap_err() }
        }))
        .await;

        assert_eq!(dialer.dialed().len(), 1, "the waiters shared one dial");
        for failure in &failures {
            assert_eq!(failure.kind(), ErrorKind::Unavailable);
        }
        assert_eq!(pool.active(), 0);

        dialer.set(Outcome::Connect);
        assert!(pool.connection(&key).await.is_ok());
    }

    /// Service mode funnels every caller through one connection; user mode gives each its own.
    #[tokio::test]
    async fn the_identity_mode_decides_how_many_connections_exist() {
        let dialer = Dialer::connecting();
        let shared = service_pool(&dialer);
        let alice = shared.connection(&shared.key(&ctx("alice"))).await.unwrap();
        let bob = shared.connection(&shared.key(&ctx("bob"))).await.unwrap();
        assert!(Arc::ptr_eq(&alice, &bob));
        assert_eq!(dialer.dialed(), ["<service>"]);

        let dialer = Dialer::connecting();
        let per_user = user_pool(&dialer, 8, Duration::from_secs(3600));
        let alice = per_user
            .connection(&per_user.key(&ctx("alice")))
            .await
            .unwrap();
        let bob = per_user
            .connection(&per_user.key(&ctx("bob")))
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&alice, &bob));
        assert_eq!(dialer.dialed(), ["alice", "bob"]);
    }

    /// A full pool refuses a new identity while still serving the identities it already holds.
    #[tokio::test]
    async fn a_full_pool_refuses_a_new_identity() {
        let dialer = Dialer::connecting();
        let pool = user_pool(&dialer, 1, Duration::from_secs(3600));

        pool.connection(&pool.key(&ctx("alice"))).await.unwrap();
        let error = pool
            .connection(&pool.key(&ctx("bob")))
            .await
            .expect_err("the pool is full");
        assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
        assert!(pool.connection(&pool.key(&ctx("alice"))).await.is_ok());
        assert_eq!(
            dialer.dialed(),
            ["alice"],
            "the refused identity never dialed"
        );
    }

    /// Idle entries are reclaimed before the capacity check, so a pool of idle connections cannot
    /// starve a new identity, and the reclaimed connection is drained rather than dropped.
    #[tokio::test]
    async fn idle_entries_are_reclaimed_before_the_capacity_check() {
        let dialer = Dialer::connecting();
        // A zero idle timeout expires every entry immediately, so reclamation is observable without
        // sleeping.
        let pool = user_pool(&dialer, 1, Duration::ZERO);

        pool.connection(&pool.key(&ctx("alice"))).await.unwrap();
        // Admitted despite a capacity of one, because alice's idle entry is reclaimed first.
        pool.connection(&pool.key(&ctx("bob"))).await.unwrap();
        assert_eq!(dialer.dialed(), ["alice", "bob"]);
        assert_eq!(dialer.closes(), 1, "the reclaimed connection was drained");
        assert_eq!(pool.active(), 1);
    }

    /// The maintenance tick reclaims an idle connection with no request to trigger admission, in
    /// service mode too, and the next request rebuilds it.
    ///
    /// Reclaiming in service mode departs from FIP-49, which scopes `connection.idle-timeout` to user
    /// mode; the reason is on [`ClusterPool::idle_timeout`].
    #[tokio::test]
    async fn the_maintenance_tick_reclaims_the_shared_connection() {
        let dialer = Dialer::connecting();
        let pool = expiring_pool(&dialer);
        let key = pool.key(&ctx("anyone"));

        // Dropped, so the entry holds the only reference: what an operation that has returned leaves.
        drop(pool.connection(&key).await.unwrap());
        pool.reclaim_idle().await;
        assert_eq!(pool.active(), 0);
        assert_eq!(dialer.closes(), 1);

        pool.connection(&key).await.unwrap();
        assert_eq!(dialer.dialed().len(), 2, "the next request dialed again");
    }

    /// A connection a request is still using is never reclaimed, however idle its entry looks.
    ///
    /// The entry is unreferenced for the whole operation — the request holds the connection, not the
    /// entry — and `last_used` was stamped before the operation began, so the idle timeout can pass
    /// while the work runs. Closing it there would abort a write mid-flight.
    #[tokio::test]
    async fn a_connection_in_use_is_never_reclaimed() {
        let dialer = Dialer::connecting();
        let pool = expiring_pool(&dialer);
        let key = pool.key(&ctx("anyone"));

        let in_use = pool.connection(&key).await.unwrap();
        pool.reclaim_idle().await;
        assert_eq!(pool.active(), 1, "the operation is still holding it");
        assert_eq!(dialer.closes(), 0);

        drop(in_use);
        pool.reclaim_idle().await;
        assert_eq!(pool.active(), 0);
        assert_eq!(dialer.closes(), 1);
    }

    /// A cancelled dial releases the capacity it reserved: dropping an in-flight request must not
    /// leave an entry that refuses every later identity.
    #[tokio::test]
    async fn a_cancelled_dial_leaves_no_reserved_capacity() {
        let dialer = Dialer::new(Outcome::Hang, Duration::ZERO);
        let pool = user_pool(&dialer, 1, Duration::from_secs(3600));
        let key = pool.key(&ctx("alice"));

        let cancelled =
            tokio::time::timeout(Duration::from_millis(20), pool.connection(&key)).await;
        assert!(cancelled.is_err(), "the dial must still be pending");

        // The entry is gone, so a different identity reaches its own dial instead of a 429.
        dialer.set(Outcome::Connect);
        pool.connection(&pool.key(&ctx("bob"))).await.unwrap();
    }

    /// Shutdown drains every connection, empties the pool, and repeats cleanly.
    #[tokio::test]
    async fn close_drains_every_connection_and_repeats() {
        let dialer = Dialer::connecting();
        let pool = user_pool(&dialer, 8, Duration::from_secs(3600));
        for user in ["alice", "bob"] {
            pool.connection(&pool.key(&ctx(user))).await.unwrap();
        }

        pool.close_all(Duration::from_secs(1)).await.unwrap();
        assert_eq!(dialer.closes(), 2);
        assert_eq!(pool.active(), 0);

        pool.close_all(Duration::from_secs(1)).await.unwrap();
        assert_eq!(dialer.closes(), 2);
    }
}
