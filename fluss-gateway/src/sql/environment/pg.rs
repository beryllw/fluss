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

//! [`PgSqlEnvironmentProvider`]: the PostgreSQL SQL environment.
//!
//! Implements the fixed 5-step assembly order. The order is a
//! contract and is pinned by the tests in this module:
//!
//! ```text
//! 1. clean SessionContext (datafusion defaults + gateway common SessionConfig)
//! 2. install the real Fluss catalog        (FlussCatalogInstaller, step-2 seam)
//! 3. install datafusion-pg-catalog base     (real datafusion-pg-catalog crate)
//! 4. install the Fluss pg_catalog overlay   (PgCatalogOverlayInstaller, step-4 seam)
//! 5. apply the initial SessionVars snapshot (timezone / search_path / schema / app)
//! ```
//!
//! Steps 2 and 4 are injected as seams (see [`collaborators`]) so the assembly
//! order stays observable and unit-testable. Step 2 is backed by the real
//! `fluss-datafusion` catalog installer; step 4 (the Fluss-specific pg_catalog
//! overlay) is still a stub. Step 3 uses the real `datafusion-pg-catalog` crate.
//! The provider holds no per-session state; [`SessionVars`] is the single source
//! of truth.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use crate::error::{GatewayError, GatewayResult};
use crate::session::apply_session_mutation as apply_vars_mutation;
use crate::session::GatewaySession;
use crate::sql::environment::apply::apply_vars_snapshot;
use crate::sql::environment::collaborators::{
    FlussCatalogInstaller, PgCatalogOverlayInstaller, StubFlussCatalogInstaller,
    StubPgCatalogOverlayInstaller,
};
use crate::sql::environment::provider::SqlEnvironmentProvider;
use crate::types::{SessionMutation, SessionMutationEffect};

/// The catalog name the Fluss catalog is registered under (design step 2;
/// contract D1 `register_catalog(&ctx, "fluss", ...)`).
pub const FLUSS_CATALOG: &str = "fluss";

/// PostgreSQL SQL environment provider.
///
/// Holds the shared, cross-session collaborators (heavy objects live behind the
/// seams, not per session). No per-session state lives here.
pub struct PgSqlEnvironmentProvider {
    fluss_catalog: Arc<dyn FlussCatalogInstaller>,
    overlay: Arc<dyn PgCatalogOverlayInstaller>,
}

impl PgSqlEnvironmentProvider {
    /// Construct with explicit collaborators (used by tests and by production
    /// wiring that injects the `fluss-datafusion` installer and pg_catalog overlay).
    pub fn new(
        fluss_catalog: Arc<dyn FlussCatalogInstaller>,
        overlay: Arc<dyn PgCatalogOverlayInstaller>,
    ) -> Self {
        Self {
            fluss_catalog,
            overlay,
        }
    }

    /// Test default: stub Fluss catalog + no-op overlay, for exercising the
    /// assembly order without a live cluster. Step 3 still uses the real
    /// `datafusion-pg-catalog`. Production wires the real `fluss-datafusion`
    /// catalog installer via [`PgSqlEnvironmentProvider::new`].
    pub fn with_stubs() -> Self {
        Self::new(
            Arc::new(StubFlussCatalogInstaller),
            Arc::new(StubPgCatalogOverlayInstaller),
        )
    }

    /// Step 3 — install `datafusion-pg-catalog` base objects (pg_catalog schema +
    /// PG UDFs) under the Fluss catalog. Must run AFTER step 2: the crate requires
    /// the target catalog to already exist.
    fn install_pg_catalog_base(ctx: &SessionContext) -> GatewayResult<()> {
        use datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
        datafusion_pg_catalog::setup_pg_catalog(ctx, FLUSS_CATALOG, EmptyContextProvider)
            .map_err(|e| GatewayError::Internal(format!("pg_catalog base install failed: {e}")))
    }
}

#[async_trait::async_trait]
impl SqlEnvironmentProvider for PgSqlEnvironmentProvider {
    async fn prepare_session_context(
        &self,
        session: &GatewaySession,
        ctx: &SessionContext,
    ) -> GatewayResult<()> {
        // Step 1: the caller passes a clean SessionContext (datafusion defaults +
        // gateway common SessionConfig). The builder owns construction; assembly
        // starts here. Re-entrant: each call re-walks steps 2..5 from scratch.

        // Step 2: real Fluss catalog only (never pg_catalog here — that is the
        // gateway's job in steps 3/4, per contract D1).
        self.fluss_catalog
            .register_catalog(ctx, FLUSS_CATALOG)
            .await?;

        // Step 3: datafusion-pg-catalog base objects (real crate). After step 2.
        Self::install_pg_catalog_base(ctx)?;

        // Step 4: Fluss-specific pg_catalog overlay, layered on the base. After 3.
        self.overlay.install_overlay(ctx).await?;

        // Step 5: apply the current SessionVars snapshot (single source of truth),
        // so a rebuild restores live state with no mutation replay.
        let vars = session.vars().read().unwrap().clone();
        apply_vars_snapshot(ctx, &vars).await?;
        Ok(())
    }

    async fn apply_session_mutation(
        &self,
        session: &GatewaySession,
        ctx: &SessionContext,
        mutation: &SessionMutation,
    ) -> GatewayResult<()> {
        // Only ApplyToExistingContext mutations are acted on here; the rest are a
        // no-op (SessionOnly never reaches a provider; RebuildContextBeforeNextQuery
        // is handled by dirty + the next prepare_session_context). We re-classify
        // (without mutating vars — that already happened in the session layer) to
        // filter.
        if classify_effect(mutation) != SessionMutationEffect::ApplyToExistingContext {
            return Ok(());
        }
        // Idempotent: re-apply the relevant var from the authoritative snapshot.
        let vars = session.vars().read().unwrap().clone();
        apply_vars_snapshot(ctx, &vars).await
    }
}

/// Classify a mutation's effect WITHOUT mutating vars. Mirrors the
/// classification in `session::vars`; used only to filter which mutations the
/// provider acts on. This module does not re-implement the classification table —
/// it applies the same rules read-only here.
fn classify_effect(mutation: &SessionMutation) -> SessionMutationEffect {
    let mut vars = crate::types::SessionVars::default();
    apply_vars_mutation(&mut vars, mutation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::environment::collaborators::{
        FlussCatalogInstaller, PgCatalogOverlayInstaller,
    };
    use crate::types::{
        ClientInfo, ClusterId, Principal, ProtocolKind, SessionId, SessionVarValue, SessionVars,
        SqlEnvironmentId,
    };
    use std::sync::Mutex;
    use std::time::Duration;

    /// One observable assembly step, recorded in call order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Step {
        FlussCatalog,
        Overlay,
    }

    /// Shared recorder the fake collaborators append to, so the test can assert
    /// the exact 5-step order (steps 2 and 4 here; step 3 base + step 5 vars are
    /// observed directly on the resulting `SessionContext`).
    #[derive(Default)]
    struct Recorder {
        steps: Mutex<Vec<Step>>,
    }
    impl Recorder {
        fn push(&self, s: Step) {
            self.steps.lock().unwrap().push(s);
        }
        fn snapshot(&self) -> Vec<Step> {
            self.steps.lock().unwrap().clone()
        }
    }

    struct RecordingFluss(Arc<Recorder>);
    #[async_trait::async_trait]
    impl FlussCatalogInstaller for RecordingFluss {
        async fn register_catalog(
            &self,
            ctx: &SessionContext,
            catalog_name: &str,
        ) -> GatewayResult<()> {
            // Real Fluss catalog only; MUST NOT install pg_catalog here.
            assert_eq!(catalog_name, FLUSS_CATALOG);
            StubFlussCatalogInstaller
                .register_catalog(ctx, catalog_name)
                .await?;
            self.0.push(Step::FlussCatalog);
            Ok(())
        }
    }

    struct RecordingOverlay(Arc<Recorder>);
    #[async_trait::async_trait]
    impl PgCatalogOverlayInstaller for RecordingOverlay {
        async fn install_overlay(&self, _ctx: &SessionContext) -> GatewayResult<()> {
            self.0.push(Step::Overlay);
            Ok(())
        }
    }

    fn session(vars: SessionVars) -> GatewaySession {
        GatewaySession::new(
            SessionId("s1".into()),
            Principal { name: "alice".into() },
            ClusterId("default".into()),
            Some(SqlEnvironmentId("postgres".into())),
            vars,
            ClientInfo {
                protocol: ProtocolKind::Postgres,
                peer_addr: None,
            },
        )
    }

    fn recording_provider(rec: Arc<Recorder>) -> PgSqlEnvironmentProvider {
        PgSqlEnvironmentProvider::new(
            Arc::new(RecordingFluss(Arc::clone(&rec))),
            Arc::new(RecordingOverlay(rec)),
        )
    }

    /// Test 1: the 5 steps run in the contract order.
    /// fluss catalog (step 2) BEFORE pg_catalog base (step 3) BEFORE overlay
    /// (step 4); vars (step 5) last. The recorder pins 2 -> 4; base (3) is proven
    /// to be between them because it requires the fluss catalog to exist (it would
    /// error if run before step 2) and the overlay records after it returns. Vars
    /// (step 5) are proven applied by observing the context afterwards.
    #[tokio::test]
    async fn assembly_runs_in_fixed_contract_order() {
        let rec = Arc::new(Recorder::default());
        let provider = recording_provider(Arc::clone(&rec));
        let vars = SessionVars {
            timezone: Some("UTC".into()),
            ..Default::default()
        };
        let s = session(vars);
        let ctx = SessionContext::new();

        provider.prepare_session_context(&s, &ctx).await.unwrap();

        // Step 2 then step 4, in that relative order.
        assert_eq!(rec.snapshot(), vec![Step::FlussCatalog, Step::Overlay]);

        // Step 3 ran between them: pg_catalog schema is installed under the fluss
        // catalog (would be absent if step 3 were skipped or run before step 2).
        let fluss = ctx.catalog(FLUSS_CATALOG).expect("fluss catalog present");
        assert!(
            fluss.schema("pg_catalog").is_some(),
            "pg_catalog base must be installed under the fluss catalog"
        );

        // Step 5 ran last: timezone from vars is applied to the live ctx config.
        assert_eq!(
            ctx.state().config().options().execution.time_zone.as_deref(),
            Some("UTC")
        );
    }

    /// Step 3 strictly depends on step 2: installing the base before any catalog
    /// exists fails. This is what pins base AFTER the fluss catalog in the order.
    #[tokio::test]
    async fn base_requires_fluss_catalog_first() {
        let ctx = SessionContext::new();
        // No catalog named "fluss" registered yet -> base install must fail.
        let err = PgSqlEnvironmentProvider::install_pg_catalog_base(&ctx);
        assert!(err.is_err(), "base install must require the fluss catalog");
    }

    /// Test 2: re-entrant / full assembly. Calling prepare twice (fresh ctx each,
    /// as rebuild does) yields identical structure; calling twice on the SAME ctx
    /// does not error and leaves a consistent, fully-assembled context.
    #[tokio::test]
    async fn prepare_is_reentrant() {
        let rec = Arc::new(Recorder::default());
        let provider = recording_provider(Arc::clone(&rec));
        let s = session(SessionVars::default());

        let ctx1 = SessionContext::new();
        provider.prepare_session_context(&s, &ctx1).await.unwrap();
        let ctx2 = SessionContext::new();
        provider.prepare_session_context(&s, &ctx2).await.unwrap();

        // Both fresh contexts come out equivalently assembled.
        for ctx in [&ctx1, &ctx2] {
            let fluss = ctx.catalog(FLUSS_CATALOG).unwrap();
            assert!(fluss.schema("pg_catalog").is_some());
        }
        // Order recorded twice, identically.
        assert_eq!(
            rec.snapshot(),
            vec![
                Step::FlussCatalog,
                Step::Overlay,
                Step::FlussCatalog,
                Step::Overlay
            ]
        );

        // Re-running on the SAME context is also tolerated (idempotent-ish: no
        // error, no half state). pg_catalog still present.
        let ctx = SessionContext::new();
        provider.prepare_session_context(&s, &ctx).await.unwrap();
        provider.prepare_session_context(&s, &ctx).await.unwrap();
        let fluss = ctx.catalog(FLUSS_CATALOG).unwrap();
        assert!(fluss.schema("pg_catalog").is_some());
    }

    /// Test 3a: apply_session_mutation acts on ApplyToExistingContext (timezone).
    #[tokio::test]
    async fn apply_mutation_handles_apply_to_existing() {
        let provider = PgSqlEnvironmentProvider::with_stubs();
        let s = session(SessionVars::default());
        let ctx = SessionContext::new();
        provider.prepare_session_context(&s, &ctx).await.unwrap();

        // The session layer already updated vars; emulate that, then call the provider.
        s.vars().write().unwrap().timezone = Some("Asia/Shanghai".into());
        provider
            .apply_session_mutation(&s, &ctx, &SessionMutation::SetTimezone(Some("Asia/Shanghai".into())))
            .await
            .unwrap();
        assert_eq!(
            ctx.state().config().options().execution.time_zone.as_deref(),
            Some("Asia/Shanghai")
        );
    }

    /// Test 3b: SessionOnly / Rebuild mutations are no-ops in the provider (no
    /// context-assembly action). statement_timeout (SessionOnly) and
    /// current_schema (Rebuild) must not change the live context here.
    #[tokio::test]
    async fn apply_mutation_ignores_non_apply_to_existing() {
        let provider = PgSqlEnvironmentProvider::with_stubs();
        let s = session(SessionVars::default());
        let ctx = SessionContext::new();
        provider.prepare_session_context(&s, &ctx).await.unwrap();
        let tz_before = ctx.state().config().options().execution.time_zone.clone();

        // SessionOnly: statement_timeout.
        provider
            .apply_session_mutation(
                &s,
                &ctx,
                &SessionMutation::SetStatementTimeout(Some(Duration::from_secs(3))),
            )
            .await
            .unwrap();
        // Rebuild class: current_schema.
        provider
            .apply_session_mutation(
                &s,
                &ctx,
                &SessionMutation::SetCurrentSchema(Some("public".into())),
            )
            .await
            .unwrap();
        // SessionOnly env var.
        provider
            .apply_session_mutation(
                &s,
                &ctx,
                &SessionMutation::SetEnvironmentVar {
                    key: "pg.application_name".into(),
                    value: SessionVarValue::String("psql".into()),
                },
            )
            .await
            .unwrap();

        // Live context timezone unchanged by any of the above.
        assert_eq!(
            ctx.state().config().options().execution.time_zone,
            tz_before
        );
    }

    /// Test 3c: apply_session_mutation is idempotent.
    #[tokio::test]
    async fn apply_mutation_is_idempotent() {
        let provider = PgSqlEnvironmentProvider::with_stubs();
        let s = session(SessionVars::default());
        let ctx = SessionContext::new();
        provider.prepare_session_context(&s, &ctx).await.unwrap();

        s.vars().write().unwrap().timezone = Some("UTC".into());
        let m = SessionMutation::SetTimezone(Some("UTC".into()));
        provider.apply_session_mutation(&s, &ctx, &m).await.unwrap();
        let after_first = ctx.state().config().options().execution.time_zone.clone();
        provider.apply_session_mutation(&s, &ctx, &m).await.unwrap();
        let after_second = ctx.state().config().options().execution.time_zone.clone();
        assert_eq!(after_first, after_second);
        assert_eq!(after_second.as_deref(), Some("UTC"));
    }

    /// Clearing a live-applied timezone resets the SessionContext to DataFusion's
    /// default (`None`) rather than leaving the old value stuck.
    #[tokio::test]
    async fn clearing_timezone_resets_live_context() {
        let provider = PgSqlEnvironmentProvider::with_stubs();
        let s = session(SessionVars::default());
        let ctx = SessionContext::new();
        provider.prepare_session_context(&s, &ctx).await.unwrap();

        s.vars().write().unwrap().timezone = Some("UTC".into());
        provider
            .apply_session_mutation(&s, &ctx, &SessionMutation::SetTimezone(Some("UTC".into())))
            .await
            .unwrap();
        assert_eq!(
            ctx.state().config().options().execution.time_zone.as_deref(),
            Some("UTC")
        );

        s.vars().write().unwrap().timezone = None;
        provider
            .apply_session_mutation(&s, &ctx, &SessionMutation::SetTimezone(None))
            .await
            .unwrap();
        assert_eq!(ctx.state().config().options().execution.time_zone, None);
    }
}
