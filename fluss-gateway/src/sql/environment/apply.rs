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

//! P3.3 step 5 — apply a [`SessionVars`] snapshot onto a live `SessionContext`.
//!
//! Gateway-owned (not an external seam): this is the one place where session
//! vars become live context state, shared by `prepare_session_context` step 5
//! (full snapshot) and `apply_session_mutation` (incremental, idempotent). It is
//! written as a pure full-snapshot apply so re-running it is idempotent —
//! applying the same vars twice leaves the context in the same state, and a
//! rebuild restores state from the snapshot without replaying past mutations.
//!
//! Phase 1 applies the context-affecting vars that DataFusion's
//! `SessionContext` can carry today: timezone (execution time zone) and the
//! current catalog / schema (default name resolution). search_path /
//! application_name beyond that are tracked in vars and surfaced by the PG
//! adapter (P4); they are not DataFusion config knobs, so there is nothing to
//! install into the context here.

use datafusion::execution::context::SessionContext;

use crate::error::GatewayResult;
use crate::types::SessionVars;

/// Apply the full `vars` snapshot to `ctx`. Idempotent.
pub async fn apply_vars_snapshot(ctx: &SessionContext, vars: &SessionVars) -> GatewayResult<()> {
    if let Some(tz) = &vars.timezone {
        // Mutate the live execution time zone in place (idempotent).
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .time_zone = Some(tz.clone());
    }
    if let Some(catalog) = &vars.current_catalog {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .catalog
            .default_catalog = catalog.clone();
    }
    if let Some(schema) = &vars.current_schema {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .catalog
            .default_schema = schema.clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn applies_timezone_catalog_schema() {
        let ctx = SessionContext::new();
        let vars = SessionVars {
            timezone: Some("UTC".into()),
            current_catalog: Some("fluss".into()),
            current_schema: Some("public".into()),
            ..Default::default()
        };
        apply_vars_snapshot(&ctx, &vars).await.unwrap();
        let opts = ctx.state().config().options().clone();
        assert_eq!(opts.execution.time_zone.as_deref(), Some("UTC"));
        assert_eq!(opts.catalog.default_catalog, "fluss");
        assert_eq!(opts.catalog.default_schema, "public");
    }

    #[tokio::test]
    async fn empty_vars_is_noop() {
        let ctx = SessionContext::new();
        let before = ctx.state().config().options().execution.time_zone.clone();
        apply_vars_snapshot(&ctx, &SessionVars::default())
            .await
            .unwrap();
        assert_eq!(
            ctx.state().config().options().execution.time_zone,
            before
        );
    }

    #[tokio::test]
    async fn reapply_is_idempotent() {
        let ctx = SessionContext::new();
        let vars = SessionVars {
            timezone: Some("Asia/Shanghai".into()),
            ..Default::default()
        };
        apply_vars_snapshot(&ctx, &vars).await.unwrap();
        let once = ctx.state().config().options().execution.time_zone.clone();
        apply_vars_snapshot(&ctx, &vars).await.unwrap();
        let twice = ctx.state().config().options().execution.time_zone.clone();
        assert_eq!(once, twice);
        assert_eq!(twice.as_deref(), Some("Asia/Shanghai"));
    }
}
