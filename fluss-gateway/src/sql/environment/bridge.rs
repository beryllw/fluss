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

//! Bridge the [`SqlEnvironmentRegistry`] + provider onto the
//! [`SessionContextBuilder`] rebuild seam.
//!
//! `GatewaySession::context_for_query` builds/rebuilds through an injected
//! [`SessionContextBuilder`]. This bridge implements that seam by: ① picking the
//! provider for the session's `sql_environment` from the registry, ② creating a
//! clean `SessionContext` (assembly step 1), ③ running the provider's full
//! `prepare_session_context` (steps 2..5). Because step 5 re-applies the latest
//! `SessionVars` snapshot, dirty -> rebuild naturally restores state with no
//! mutation replay (design step 5).
//!
//! The bridge is constructed per session (it captures the session's
//! `sql_environment`) but holds no mutable per-session state; the registry and
//! providers are shared.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::error::{GatewayError, GatewayResult};
use crate::session::{GatewaySession, SessionContextBuilder};
use crate::sql::environment::registry::SqlEnvironmentRegistry;
use crate::types::{SessionVars, SqlEnvironmentId};

/// Adapts a shared registry + a session's environment id into a context builder.
pub struct EnvironmentContextBuilder {
    registry: Arc<SqlEnvironmentRegistry>,
    sql_environment: SqlEnvironmentId,
    session: Arc<GatewaySession>,
}

impl EnvironmentContextBuilder {
    /// Build a bridge for `session`. The session must have an `sql_environment`
    /// (the SQL path requires one; the direct path never builds a context).
    pub fn new(
        registry: Arc<SqlEnvironmentRegistry>,
        session: Arc<GatewaySession>,
    ) -> GatewayResult<Self> {
        let sql_environment = session.sql_environment.clone().ok_or_else(|| {
            GatewayError::InvalidArgument(
                "session has no SQL environment; cannot build a SessionContext".into(),
            )
        })?;
        Ok(Self {
            registry,
            sql_environment,
            session,
        })
    }
}

#[async_trait::async_trait]
impl SessionContextBuilder for EnvironmentContextBuilder {
    async fn build(
        &self,
        _vars: &SessionVars,
        _previous: Option<&Arc<SessionContext>>,
    ) -> GatewayResult<Arc<SessionContext>> {
        let provider = self.registry.get(&self.sql_environment)?;
        // Step 1: clean SessionContext (datafusion defaults + gateway config).
        // Enable DataFusion's `information_schema` so `information_schema.tables`
        // / `columns` / `schemata` resolve and reflect the registered Fluss
        // catalog (datafusion-pg-catalog provides pg_catalog, not information_schema).
        let config = SessionConfig::new().with_information_schema(true);
        let ctx = Arc::new(SessionContext::new_with_config(config));
        // Steps 2..5: the provider reads the session's authoritative SessionVars.
        provider
            .prepare_session_context(&self.session, &ctx)
            .await?;
        Ok(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::GatewaySession;
    use crate::sql::environment::provider::SqlEnvironmentProvider;
    use crate::types::{
        ClientInfo, ClusterId, Principal, ProtocolKind, SessionId, SessionMutation, SessionVars,
        SqlEnvironmentId,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fake provider recording how many times prepare ran and for which session.
    struct FakeProvider {
        prepared: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl SqlEnvironmentProvider for FakeProvider {
        async fn prepare_session_context(
            &self,
            session: &GatewaySession,
            ctx: &SessionContext,
        ) -> GatewayResult<()> {
            // Prove the bridge passed the right session and a usable ctx.
            assert_eq!(session.id, SessionId("s1".into()));
            let _ = ctx.catalog_names();
            self.prepared.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        async fn apply_session_mutation(
            &self,
            _session: &GatewaySession,
            _ctx: &SessionContext,
            _mutation: &SessionMutation,
        ) -> GatewayResult<()> {
            Ok(())
        }
    }

    fn session(env: Option<SqlEnvironmentId>) -> Arc<GatewaySession> {
        Arc::new(GatewaySession::new(
            SessionId("s1".into()),
            Principal { name: "alice".into() },
            ClusterId("default".into()),
            env,
            SessionVars::default(),
            ClientInfo {
                protocol: ProtocolKind::Postgres,
                peer_addr: None,
            },
        ))
    }

    #[tokio::test]
    async fn bridge_selects_provider_and_prepares() {
        let mut reg = SqlEnvironmentRegistry::new();
        let provider = Arc::new(FakeProvider {
            prepared: AtomicUsize::new(0),
        });
        reg.register(SqlEnvironmentId("postgres".into()), provider.clone());
        let reg = Arc::new(reg);

        let s = session(Some(SqlEnvironmentId("postgres".into())));
        let bridge = EnvironmentContextBuilder::new(Arc::clone(&reg), Arc::clone(&s)).unwrap();

        // Drive the builder seam directly.
        let _ctx = bridge.build(&SessionVars::default(), None).await.unwrap();
        assert_eq!(provider.prepared.load(Ordering::Acquire), 1);

        // And via the real session rebuild path: first query builds once.
        let _c = s.context_for_query(&bridge).await.unwrap();
        assert_eq!(provider.prepared.load(Ordering::Acquire), 2);
        assert_eq!(s.generation(), 1);
    }

    #[tokio::test]
    async fn bridge_errors_for_unknown_environment() {
        let reg = Arc::new(SqlEnvironmentRegistry::new());
        let s = session(Some(SqlEnvironmentId("mysql".into())));
        let bridge = EnvironmentContextBuilder::new(reg, s).unwrap();
        let err = bridge.build(&SessionVars::default(), None).await;
        assert!(matches!(err, Err(GatewayError::Unsupported(_))));
    }

    #[tokio::test]
    async fn bridge_requires_sql_environment() {
        let reg = Arc::new(SqlEnvironmentRegistry::new());
        let s = session(None);
        let err = EnvironmentContextBuilder::new(reg, s);
        assert!(matches!(err, Err(GatewayError::InvalidArgument(_))));
    }
}
