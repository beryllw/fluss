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

//! P3 — stub seams for the not-yet-ready external assembly steps.
//!
//! `PgSqlEnvironmentProvider::prepare_session_context` runs a fixed 5-step order
//! (design §P3.3). Two of those steps reach into capabilities that are NOT built
//! in Phase 1 and must stay isolated behind a narrow injected seam so the order
//! is observable and unit-testable without those capabilities:
//!
//! - Step 2 (install real Fluss catalog) is owned by `fluss-datafusion`
//!   (`register_catalog`, contract D1). The gateway never absorbs that logic; it
//!   only calls it. The seam below mirrors that single call.
//! - Step 4 (Fluss-specific pg_catalog overlay) depends on P6 backend metadata,
//!   which is not landed yet.
//!
//! Step 3 (pg_catalog base objects) uses the real `datafusion-pg-catalog` crate
//! directly (it is a Phase 1 dependency) and so needs no seam here.
//!
//! Each trait corresponds to exactly one real assembly boundary; no extra
//! abstraction is introduced (CLAUDE.md: avoid empty future-oriented seams).
//! Swapping in the real `fluss-datafusion` / P6 metadata later means replacing
//! the implementations wired into `PgSqlEnvironmentProvider`, not these traits'
//! call sites.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use crate::error::GatewayResult;

/// Step 2 seam — install the real Fluss catalog into a `SessionContext`.
///
/// Mirrors the `fluss-datafusion` contract D1 call
/// `register_catalog(&ctx, "fluss", options)`: it registers ONLY the Fluss
/// catalog under `catalog_name` and MUST NOT touch `pg_catalog` (pg compatibility
/// is the gateway's responsibility in steps 3/4). Phase 1 ships a fake; the real
/// `FlussDatafusion` is swapped in later.
#[async_trait::async_trait]
pub trait FlussCatalogInstaller: Send + Sync {
    /// Register the Fluss catalog under `catalog_name` (e.g. `"fluss"`) on `ctx`.
    async fn register_catalog(
        &self,
        ctx: &SessionContext,
        catalog_name: &str,
    ) -> GatewayResult<()>;
}

/// Step 4 seam — install the Fluss-specific `pg_catalog` overlay.
///
/// Projects Fluss metadata (databases / tables) into PG system views layered on
/// top of the `datafusion-pg-catalog` base objects (step 3). This depends on P6
/// backend metadata, which is not landed in Phase 1, so it stays a seam. The real
/// implementation will read from the backend facade and overlay PG views.
#[async_trait::async_trait]
pub trait PgCatalogOverlayInstaller: Send + Sync {
    /// Install the Fluss overlay onto `ctx`. Runs strictly after the base objects
    /// so it can layer on top of them.
    async fn install_overlay(&self, ctx: &SessionContext) -> GatewayResult<()>;
}

// ---------------------------------------------------------------------------
// Real adapter — backs step 2 with the upstream `fluss-datafusion` crate.
// ---------------------------------------------------------------------------

/// Step 2 production adapter over `fluss_datafusion::FlussDatafusion`.
///
/// Holds the shared installer (one per `(cluster, proxy connection)`, contract
/// D1) and delegates each per-session call to `FlussDatafusion::register_catalog`,
/// installing ONLY the Fluss catalog. `FlussDatafusionError` is mapped into the
/// gateway domain taxonomy here, at the crate boundary (contract D3) — no
/// DataFusion/Fluss error type leaks past this point.
///
/// Construction needs a live `FlussConnection`, so the instance is built in P6
/// (connection provider) and injected into `PgSqlEnvironmentProvider`; tests use
/// [`StubFlussCatalogInstaller`] instead.
pub struct FlussDatafusionCatalogInstaller {
    inner: Arc<fluss_datafusion::FlussDatafusion>,
}

impl FlussDatafusionCatalogInstaller {
    /// Wrap a shared `FlussDatafusion` installer.
    pub fn new(inner: Arc<fluss_datafusion::FlussDatafusion>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl FlussCatalogInstaller for FlussDatafusionCatalogInstaller {
    async fn register_catalog(
        &self,
        ctx: &SessionContext,
        catalog_name: &str,
    ) -> GatewayResult<()> {
        self.inner
            .register_catalog(
                ctx,
                catalog_name,
                fluss_datafusion::RegisterCatalogOptions::default(),
            )
            .await
            .map_err(|e: fluss_datafusion::FlussDatafusionError| {
                crate::error::GatewayError::Backend(e.to_string())
            })
    }
}

// ---------------------------------------------------------------------------
// Phase 1 default implementations (no real Fluss / P6 metadata available).
// ---------------------------------------------------------------------------

/// Test/default Fluss catalog installer.
///
/// Registers a minimal empty schema under `catalog_name` to stand in for the real
/// catalog — just enough for steps 3/4 (which need the catalog to exist) and for
/// the order contract to hold. Used by unit/harness tests that exercise the
/// assembly order without a live Fluss cluster; production wires
/// [`FlussDatafusionCatalogInstaller`] instead.
#[derive(Debug, Default)]
pub struct StubFlussCatalogInstaller;

#[async_trait::async_trait]
impl FlussCatalogInstaller for StubFlussCatalogInstaller {
    async fn register_catalog(
        &self,
        ctx: &SessionContext,
        catalog_name: &str,
    ) -> GatewayResult<()> {
        use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
        let catalog = Arc::new(MemoryCatalogProvider::new());
        // A default schema so the catalog is non-empty and resolvable.
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .map_err(|e: datafusion::error::DataFusionError| {
                crate::error::GatewayError::Backend(e.to_string())
            })?;
        ctx.register_catalog(catalog_name, catalog);
        Ok(())
    }
}

/// Phase 1 default overlay installer: no-op until P6 backend metadata lands.
#[derive(Debug, Default)]
pub struct StubPgCatalogOverlayInstaller;

#[async_trait::async_trait]
impl PgCatalogOverlayInstaller for StubPgCatalogOverlayInstaller {
    async fn install_overlay(&self, _ctx: &SessionContext) -> GatewayResult<()> {
        // P6 metadata not yet available; the real overlay projects Fluss tables
        // into PG views here.
        Ok(())
    }
}
