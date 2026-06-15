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

//! P2.11 — [`SessionManager`]: connection-level session governance.
//!
//! Responsible for: open/close, the session registry, a max-session cap, an idle
//! reaper, and basic lookup/snapshot reads. It deliberately does NOT do query
//! concurrency scheduling, REST direct-path throttling, backend pooling, or
//! protocol-local prepared-statement caching. The idle reaper only reclaims
//! sessions that have no active operations; `last_access_at` is refreshed on
//! open/get/alter/describe/execute.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{GatewayError, GatewayResult};
use crate::session::session::GatewaySession;
use crate::types::{OpenSessionRequest, SessionId};

/// Configuration for the session manager (design §P2.11).
#[derive(Debug, Clone)]
pub struct SessionManagerConfig {
    /// Maximum number of concurrently open sessions. Opening beyond this is
    /// rejected with `InvalidArgument` (session limit != query limit).
    pub max_sessions: usize,
    /// Idle threshold for the reaper. A session idle longer than this AND with no
    /// active operations is eligible for reclamation.
    pub idle_timeout: Duration,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1024,
            idle_timeout: Duration::from_secs(600),
        }
    }
}

/// Generates unique session ids. Phase 1 uses a simple monotonic counter; the
/// scheme is internal and not protocol-visible.
fn next_session_id(counter: &std::sync::atomic::AtomicU64) -> SessionId {
    let n = counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    SessionId(format!("session-{n}"))
}

/// Connection-level session registry and lifecycle owner.
pub struct SessionManager {
    config: SessionManagerConfig,
    sessions: RwLock<HashMap<SessionId, Arc<GatewaySession>>>,
    id_counter: std::sync::atomic::AtomicU64,
}

impl SessionManager {
    pub fn new(config: SessionManagerConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            id_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(SessionManagerConfig::default())
    }

    /// §P2.11 — open a session. Rejects when at capacity. Refreshes access time.
    pub fn open(&self, req: OpenSessionRequest) -> GatewayResult<Arc<GatewaySession>> {
        let mut sessions = self.sessions.write().unwrap();
        if sessions.len() >= self.config.max_sessions {
            return Err(GatewayError::InvalidArgument(format!(
                "session limit reached: {}",
                self.config.max_sessions
            )));
        }
        let id = next_session_id(&self.id_counter);
        let session = Arc::new(GatewaySession::new(
            id.clone(),
            req.principal,
            req.cluster,
            req.sql_environment,
            req.initial_vars,
            req.client_info,
        ));
        session.touch();
        sessions.insert(id, Arc::clone(&session));
        Ok(session)
    }

    /// Look up a live session, refreshing its access time. Closed/removed
    /// sessions are not found.
    pub fn get(&self, id: &SessionId) -> GatewayResult<Arc<GatewaySession>> {
        let session = self
            .sessions
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| GatewayError::SessionNotFound(id.0.clone()))?;
        session.touch();
        Ok(session)
    }

    /// §P2.6 — close: mark the session closed, cancel its active operations, and
    /// remove it from the registry. Subsequent requests on the id are not found.
    /// Does not synchronously wait for operations to drain.
    pub fn close(&self, id: &SessionId) -> GatewayResult<()> {
        let session = self
            .sessions
            .write()
            .unwrap()
            .remove(id)
            .ok_or_else(|| GatewayError::SessionNotFound(id.0.clone()))?;
        session.close();
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// §P2.11 — reclaim idle sessions: remove sessions whose last access is older
    /// than `idle_timeout` AND that have no active operations. Returns the ids
    /// reaped. Sessions with active operations are always retained.
    pub fn reap_idle(&self) -> Vec<SessionId> {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let idle_millis = self.config.idle_timeout.as_millis() as u64;

        let mut sessions = self.sessions.write().unwrap();
        let mut reaped = Vec::new();
        sessions.retain(|id, session| {
            let idle = now_millis.saturating_sub(session.last_access_millis()) >= idle_millis;
            let has_active = session.operation_manager().has_active();
            if idle && !has_active {
                session.close();
                reaped.push(id.clone());
                false
            } else {
                true
            }
        });
        reaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::operation::Operation;
    use crate::types::{
        ClientInfo, ClusterId, OperationId, Principal, ProtocolKind, SessionVars, SqlEnvironmentId,
    };

    fn req() -> OpenSessionRequest {
        OpenSessionRequest {
            principal: Principal { name: "alice".into() },
            cluster: ClusterId("default".into()),
            sql_environment: Some(SqlEnvironmentId("postgres".into())),
            initial_vars: SessionVars::default(),
            client_info: ClientInfo {
                protocol: ProtocolKind::Postgres,
                peer_addr: None,
            },
        }
    }

    // §P2.11 — opening beyond the cap is rejected.
    #[test]
    fn max_session_limit_is_enforced() {
        let mgr = SessionManager::new(SessionManagerConfig {
            max_sessions: 2,
            idle_timeout: Duration::from_secs(600),
        });
        assert!(mgr.open(req()).is_ok());
        assert!(mgr.open(req()).is_ok());
        assert!(matches!(
            mgr.open(req()),
            Err(GatewayError::InvalidArgument(_))
        ));
        assert_eq!(mgr.len(), 2);
    }

    // §P2.6 — after close, the session id is not found for new requests.
    #[test]
    fn close_removes_session_and_rejects_reuse() {
        let mgr = SessionManager::with_defaults();
        let s = mgr.open(req()).unwrap();
        let id = s.id.clone();
        assert!(mgr.get(&id).is_ok());
        mgr.close(&id).unwrap();
        assert!(matches!(
            mgr.get(&id),
            Err(GatewayError::SessionNotFound(_))
        ));
        assert!(s.is_closed());
        // Closing again is also not found.
        assert!(matches!(
            mgr.close(&id),
            Err(GatewayError::SessionNotFound(_))
        ));
    }

    // §P2.11 — idle reaper reclaims idle sessions but keeps ones with active ops.
    #[test]
    fn idle_reaper_keeps_sessions_with_active_operations() {
        let mgr = SessionManager::new(SessionManagerConfig {
            max_sessions: 16,
            // zero idle threshold: every session is immediately "idle".
            idle_timeout: Duration::from_secs(0),
        });

        let idle = mgr.open(req()).unwrap();
        let busy = mgr.open(req()).unwrap();

        // Give `busy` a running operation so it must be retained.
        let mut op = Operation::new(OperationId("op".into()), "SELECT 1");
        op.mark_running();
        busy.operation_manager().register(op);

        let reaped = mgr.reap_idle();
        assert_eq!(reaped, vec![idle.id.clone()]);
        assert!(mgr.get(&idle.id).is_err());
        assert!(mgr.get(&busy.id).is_ok());
    }

    // §P2.11 — recently accessed (non-idle) sessions are not reaped.
    #[test]
    fn idle_reaper_keeps_recently_accessed_sessions() {
        let mgr = SessionManager::new(SessionManagerConfig {
            max_sessions: 16,
            idle_timeout: Duration::from_secs(600),
        });
        let s = mgr.open(req()).unwrap();
        // Fresh open => last_access just now => not idle.
        let reaped = mgr.reap_idle();
        assert!(reaped.is_empty());
        assert!(mgr.get(&s.id).is_ok());
    }

    // §P2.11 — get refreshes last_access_at.
    #[test]
    fn get_updates_last_access() {
        let mgr = SessionManager::with_defaults();
        let s = mgr.open(req()).unwrap();
        let before = s.last_access_millis();
        // Force an older access time, then get() should bump it forward.
        std::thread::sleep(Duration::from_millis(5));
        let _ = mgr.get(&s.id).unwrap();
        assert!(s.last_access_millis() >= before);
    }
}
