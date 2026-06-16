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

//! P2.1 / P2.5 / P2.6 / P2.9 — connection-scoped [`GatewaySession`].
//!
//! Holds the read-only connection identity, the mutable [`SessionVars`] (single
//! source of truth), the per-session [`OperationManager`], and the lazy /
//! dirty-rebuild `SessionContext` state machine. The *actual* construction of a
//! `SessionContext` (installing fluss-datafusion + the SQL environment) is P3;
//! P2 only models the lazy/dirty/generation lifecycle and the pointer-swap
//! semantics, behind an injected [`SessionContextBuilder`] seam so it is testable
//! without building a real context.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as SyncRwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::execution::context::SessionContext;
use tokio::sync::RwLock as AsyncRwLock;

use crate::error::GatewayResult;
use crate::session::operation::OperationManager;
use crate::session::vars::apply_session_mutation;
use crate::types::{
    ClientInfo, ClusterId, Principal, SessionId, SessionMutation, SessionMutationEffect,
    SessionSnapshot, SessionVars, SqlEnvironmentId,
};

/// P2/P3 seam: builds a `SessionContext` from the current [`SessionVars`].
///
/// In P2 the gateway does not assemble a real context (no catalog / SQL
/// environment). This trait is the injection point the P3 environment provider
/// implements; P2 tests supply a fake builder to exercise the dirty -> rebuild ->
/// generation lifecycle without touching DataFusion catalog wiring.
///
/// `build` is async because the real P3 assembly (`prepare_session_context`,
/// design §P3.1) installs catalogs / pg_catalog asynchronously; the rebuild path
/// (`context_for_query`) is already async, so the seam stays async end-to-end.
#[async_trait::async_trait]
pub trait SessionContextBuilder: Send + Sync {
    /// Build a fresh context for `vars`. The previous context (if any) is passed
    /// so an implementation may reuse shared heavy objects; P2 ignores it.
    async fn build(
        &self,
        vars: &SessionVars,
        previous: Option<&Arc<SessionContext>>,
    ) -> GatewayResult<Arc<SessionContext>>;
}

/// Convert a `SystemTime` to epoch millis for storage in an `AtomicU64`.
fn epoch_millis(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Connection-scoped session object (design §P2.1). Created per PostgreSQL
/// connection; the direct path never constructs one.
pub struct GatewaySession {
    pub id: SessionId,
    pub principal: Principal,
    pub cluster: ClusterId,
    pub sql_environment: Option<SqlEnvironmentId>,
    vars: Arc<SyncRwLock<SessionVars>>,
    /// Connection-time initial vars, kept immutably so `SessionMutation::ResetAll`
    /// (DISCARD ALL) can restore them verbatim instead of clearing to defaults.
    initial_vars: SessionVars,
    pub client_info: ClientInfo,
    operation_manager: OperationManager,
    /// Current per-session context; `None` until first describe/execute (lazy).
    sql_context: AsyncRwLock<Option<Arc<SessionContext>>>,
    /// Monotonic counter incremented on every successful (re)build.
    sql_context_generation: AtomicU64,
    /// Set when a mutation requires a rebuild before the next query.
    sql_context_dirty: AtomicBool,
    /// Set on close; blocks new describe/execute/alter (design §P2.6).
    closed: AtomicBool,
    pub created_at: SystemTime,
    /// Epoch-millis of last access; updated by the manager on session ops.
    last_access_at: AtomicU64,
}

impl GatewaySession {
    /// Construct a session in the lazy state: `sql_context = None`, generation 0,
    /// not dirty (design §P2.5).
    pub fn new(
        id: SessionId,
        principal: Principal,
        cluster: ClusterId,
        sql_environment: Option<SqlEnvironmentId>,
        initial_vars: SessionVars,
        client_info: ClientInfo,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            principal,
            cluster,
            sql_environment,
            vars: Arc::new(SyncRwLock::new(initial_vars.clone())),
            initial_vars,
            client_info,
            operation_manager: OperationManager::new(),
            sql_context: AsyncRwLock::new(None),
            sql_context_generation: AtomicU64::new(0),
            sql_context_dirty: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            created_at: now,
            last_access_at: AtomicU64::new(epoch_millis(now)),
        }
    }

    pub fn operation_manager(&self) -> &OperationManager {
        &self.operation_manager
    }

    pub fn vars(&self) -> Arc<SyncRwLock<SessionVars>> {
        Arc::clone(&self.vars)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn is_dirty(&self) -> bool {
        self.sql_context_dirty.load(Ordering::Acquire)
    }

    pub fn generation(&self) -> u64 {
        self.sql_context_generation.load(Ordering::Acquire)
    }

    pub fn last_access_millis(&self) -> u64 {
        self.last_access_at.load(Ordering::Acquire)
    }

    /// Refresh `last_access_at` to now (design §P2.11: open/get/alter/describe/
    /// execute).
    pub fn touch(&self) {
        self.last_access_at
            .store(epoch_millis(SystemTime::now()), Ordering::Release);
    }

    /// Read-only snapshot of mutable + immutable session state.
    pub fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            id: self.id.clone(),
            principal: self.principal.clone(),
            cluster: self.cluster.clone(),
            sql_environment: self.sql_environment.clone(),
            vars: self.vars.read().unwrap().clone(),
            client_info: self.client_info.clone(),
        }
    }

    /// §P2.3 / §P2.4 — apply a mutation: ① update vars, ② classify effect,
    /// ③ let the caller decide whether to live-apply or lazily rebuild. Only
    /// `RebuildContextBeforeNextQuery` sets the dirty flag here. Idempotent.
    pub fn apply_mutation(&self, mutation: &SessionMutation) -> SessionMutationEffect {
        let effect = {
            let mut vars = self.vars.write().unwrap();
            match mutation {
                // ResetAll restores the connection's initial vars (DISCARD ALL).
                // The bare-vars helper can only clear to defaults, so the session —
                // which owns the initial snapshot — applies the reset itself.
                SessionMutation::ResetAll => {
                    *vars = self.initial_vars.clone();
                    SessionMutationEffect::RebuildContextBeforeNextQuery
                }
                _ => apply_session_mutation(&mut vars, mutation),
            }
        };
        if effect == SessionMutationEffect::RebuildContextBeforeNextQuery {
            self.sql_context_dirty.store(true, Ordering::Release);
        }
        effect
    }

    /// Mark the SQL context dirty so the next query rebuilds it from the
    /// authoritative SessionVars snapshot.
    pub fn mark_context_dirty(&self) {
        self.sql_context_dirty.store(true, Ordering::Release);
    }

    /// Return the currently built SessionContext, if one exists.
    pub async fn current_context(&self) -> Option<Arc<SessionContext>> {
        self.sql_context.read().await.as_ref().map(Arc::clone)
    }

    #[cfg(test)]
    pub async fn replace_context_for_test(&self, ctx: Arc<SessionContext>) {
        *self.sql_context.write().await = Some(ctx);
        self.sql_context_dirty.store(false, Ordering::Release);
    }

    /// §P2.5 — obtain the context for the next query, building or rebuilding as
    /// needed:
    ///
    /// - `None` (lazy, first use) -> build, generation += 1.
    /// - dirty -> rebuild from latest vars, swap pointer, generation += 1, clear
    ///   dirty. The old `Arc` returned to any in-flight operation stays alive.
    /// - otherwise -> return the current context unchanged.
    pub async fn context_for_query(
        &self,
        builder: &dyn SessionContextBuilder,
    ) -> GatewayResult<Arc<SessionContext>> {
        let mut guard = self.sql_context.write().await;
        let needs_build = guard.is_none() || self.sql_context_dirty.load(Ordering::Acquire);
        if needs_build {
            let vars = self.vars.read().unwrap().clone();
            let new_ctx = builder.build(&vars, guard.as_ref()).await?;
            // Swap the pointer; the previous Arc is dropped from the session slot
            // but any running operation still holds its own clone.
            *guard = Some(Arc::clone(&new_ctx));
            self.sql_context_generation.fetch_add(1, Ordering::AcqRel);
            self.sql_context_dirty.store(false, Ordering::Release);
            Ok(new_ctx)
        } else {
            Ok(Arc::clone(guard.as_ref().expect("context present")))
        }
    }

    /// §P2.6 — close the session: mark closed (rejecting new describe/execute/
    /// alter) and request cancel on all active operations. Does not wait for
    /// operations to drain; the manager removes the session from its registry.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.operation_manager.cancel_all_active();
    }
}

/// §P2.9 — effective gateway deadline duration:
/// `min(statement_timeout, request_timeout_override)`. `None` for both means no
/// extra gateway deadline; one present means that value; both present means the
/// smaller.
pub fn effective_timeout(
    statement_timeout: Option<Duration>,
    request_override: Option<Duration>,
) -> Option<Duration> {
    match (statement_timeout, request_override) {
        (None, None) => None,
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(a.min(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ProtocolKind, SessionVarValue};
    use std::sync::atomic::AtomicUsize;

    fn client() -> ClientInfo {
        ClientInfo {
            protocol: ProtocolKind::Postgres,
            peer_addr: None,
        }
    }

    fn session(vars: SessionVars) -> GatewaySession {
        GatewaySession::new(
            SessionId("s1".into()),
            Principal { name: "alice".into() },
            ClusterId("default".into()),
            Some(SqlEnvironmentId("postgres".into())),
            vars,
            client(),
        )
    }

    /// Fake builder: counts builds and produces a fresh empty `SessionContext`.
    /// P2 does NOT assemble a real catalog/environment; this stands in for the P3
    /// provider so the dirty/rebuild lifecycle is testable.
    struct CountingBuilder {
        builds: AtomicUsize,
    }
    impl CountingBuilder {
        fn new() -> Self {
            Self {
                builds: AtomicUsize::new(0),
            }
        }
        fn count(&self) -> usize {
            self.builds.load(Ordering::Acquire)
        }
    }
    #[async_trait::async_trait]
    impl SessionContextBuilder for CountingBuilder {
        async fn build(
            &self,
            _vars: &SessionVars,
            _previous: Option<&Arc<SessionContext>>,
        ) -> GatewayResult<Arc<SessionContext>> {
            self.builds.fetch_add(1, Ordering::AcqRel);
            Ok(Arc::new(SessionContext::new()))
        }
    }

    // §P2.5 — lazy init: no context until first query, then generation 1.
    #[tokio::test]
    async fn lazy_build_on_first_query() {
        let s = session(SessionVars::default());
        let b = CountingBuilder::new();
        assert_eq!(s.generation(), 0);
        let _ctx = s.context_for_query(&b).await.unwrap();
        assert_eq!(b.count(), 1);
        assert_eq!(s.generation(), 1);
        // Second query without dirty: no rebuild.
        let _ctx2 = s.context_for_query(&b).await.unwrap();
        assert_eq!(b.count(), 1);
        assert_eq!(s.generation(), 1);
    }

    // §P2.4 / §P2.5 — RebuildContextBeforeNextQuery sets dirty; next query
    // rebuilds (generation++), dirty resets, old Arc stays alive.
    #[tokio::test]
    async fn rebuild_mutation_sets_dirty_and_rebuilds() {
        let s = session(SessionVars::default());
        let b = CountingBuilder::new();
        let first = s.context_for_query(&b).await.unwrap();
        assert_eq!(s.generation(), 1);

        let effect = s.apply_mutation(&SessionMutation::SetCurrentSchema(Some("public".into())));
        assert_eq!(effect, SessionMutationEffect::RebuildContextBeforeNextQuery);
        assert!(s.is_dirty());

        let second = s.context_for_query(&b).await.unwrap();
        assert_eq!(b.count(), 2);
        assert_eq!(s.generation(), 2);
        assert!(!s.is_dirty());
        // The old Arc handed to an in-flight op is still alive and distinct.
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(Arc::strong_count(&first) >= 1);
    }

    // §P2.4 — SessionOnly mutation does NOT set dirty.
    #[tokio::test]
    async fn session_only_mutation_does_not_set_dirty() {
        let s = session(SessionVars::default());
        let b = CountingBuilder::new();
        s.context_for_query(&b).await.unwrap();

        let effect = s.apply_mutation(&SessionMutation::SetStatementTimeout(Some(
            Duration::from_secs(3),
        )));
        assert_eq!(effect, SessionMutationEffect::SessionOnly);
        assert!(!s.is_dirty());

        // Display GUC also stays SessionOnly.
        let effect = s.apply_mutation(&SessionMutation::SetEnvironmentVar {
            key: "pg.application_name".into(),
            value: SessionVarValue::String("psql".into()),
        });
        assert_eq!(effect, SessionMutationEffect::SessionOnly);
        assert!(!s.is_dirty());

        // No rebuild triggered.
        s.context_for_query(&b).await.unwrap();
        assert_eq!(b.count(), 1);
    }

    #[tokio::test]
    async fn apply_mutation_is_idempotent_on_session() {
        let s = session(SessionVars::default());
        let m = SessionMutation::SetEnvironmentVar {
            key: "pg.search_path".into(),
            value: SessionVarValue::String("a,b".into()),
        };
        let e1 = s.apply_mutation(&m);
        let snap1 = s.snapshot().vars.environment.clone();
        let e2 = s.apply_mutation(&m);
        let snap2 = s.snapshot().vars.environment.clone();
        assert_eq!(e1, e2);
        assert_eq!(snap1, snap2);
    }

    // §P4.3 — ResetAll (DISCARD ALL) restores the connection's initial vars and
    // forces a rebuild before the next query.
    #[tokio::test]
    async fn reset_all_restores_initial_vars_and_rebuilds() {
        let initial = SessionVars {
            timezone: Some("Asia/Shanghai".into()),
            ..SessionVars::default()
        };
        let s = session(initial.clone());
        let b = CountingBuilder::new();
        s.context_for_query(&b).await.unwrap();

        // Drift the session away from its initial state.
        s.apply_mutation(&SessionMutation::SetTimezone(Some("UTC".into())));
        s.apply_mutation(&SessionMutation::SetEnvironmentVar {
            key: "pg.search_path".into(),
            value: SessionVarValue::String("custom".into()),
        });
        assert_eq!(s.snapshot().vars.timezone.as_deref(), Some("UTC"));

        let effect = s.apply_mutation(&SessionMutation::ResetAll);
        assert_eq!(effect, SessionMutationEffect::RebuildContextBeforeNextQuery);
        assert!(s.is_dirty());

        let vars = s.snapshot().vars;
        assert_eq!(vars.timezone.as_deref(), Some("Asia/Shanghai"));
        assert!(
            !vars.environment.contains_key("pg.search_path"),
            "all drifted vars reset to initial"
        );

        // Next query rebuilds with the restored vars.
        s.context_for_query(&b).await.unwrap();
        assert_eq!(b.count(), 2);
        assert!(!s.is_dirty());
    }

    // §P2.6 — close marks closed and cancels active ops.
    #[test]
    fn close_marks_closed() {
        let s = session(SessionVars::default());
        assert!(!s.is_closed());
        s.close();
        assert!(s.is_closed());
    }

    // §P2.9 — effective_timeout min logic, all branches.
    #[test]
    fn effective_timeout_branches() {
        assert_eq!(effective_timeout(None, None), None);
        assert_eq!(
            effective_timeout(Some(Duration::from_secs(5)), None),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            effective_timeout(None, Some(Duration::from_secs(7))),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            effective_timeout(Some(Duration::from_secs(5)), Some(Duration::from_secs(7))),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            effective_timeout(Some(Duration::from_secs(9)), Some(Duration::from_secs(2))),
            Some(Duration::from_secs(2))
        );
    }
}
