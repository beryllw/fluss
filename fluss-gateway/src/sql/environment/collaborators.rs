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

//! Catalog/overlay assembly seams for the external assembly steps.
//!
//! `PgSqlEnvironmentProvider::prepare_session_context` runs a fixed 5-step order.
//! Two of those steps are isolated behind a narrow injected seam so the order is
//! observable and unit-testable independently of the backing implementations:
//!
//! - Step 2 (install real Fluss catalog) is owned by `fluss-datafusion`
//!   (`register_catalog`, contract D1). The gateway never absorbs that logic; it
//!   only calls it. The seam below mirrors that single call and is backed in
//!   production by [`FlussDatafusionCatalogInstaller`].
//! - Step 4 (Fluss-specific pg_catalog overlay) projects backend metadata into PG
//!   system views; it is still a stub ([`StubPgCatalogOverlayInstaller`]).
//!
//! Step 3 (pg_catalog base objects) uses the real `datafusion-pg-catalog` crate
//! directly and so needs no seam here.
//!
//! Each trait corresponds to exactly one real assembly boundary; no extra
//! abstraction is introduced (CLAUDE.md: avoid empty future-oriented seams).
//! Swapping the pg_catalog overlay for its real implementation means replacing
//! the implementation wired into `PgSqlEnvironmentProvider`, not these traits'
//! call sites.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use crate::error::GatewayResult;

/// Step 2 seam — install the real Fluss catalog into a `SessionContext`.
///
/// Mirrors the `fluss-datafusion` contract D1 call
/// `register_catalog(&ctx, "fluss", options)`: it registers ONLY the Fluss
/// catalog under `catalog_name` and MUST NOT touch `pg_catalog` (pg compatibility
/// is the gateway's responsibility in steps 3/4). Production uses
/// [`FlussDatafusionCatalogInstaller`]; tests use [`StubFlussCatalogInstaller`].
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
/// top of the `datafusion-pg-catalog` base objects (step 3). The overlay is still
/// a stub; the real implementation reads from the backend facade and overlays PG
/// views.
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
/// Construction needs a live `FlussConnection`, so the instance is built by the
/// connection provider and injected into `PgSqlEnvironmentProvider`; tests use
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
        // ① Install the live Fluss catalog (the real, read-only provider).
        self.inner
            .register_catalog(
                ctx,
                catalog_name,
                fluss_datafusion::RegisterCatalogOptions::default(),
            )
            .await
            .map_err(|e: fluss_datafusion::FlussDatafusionError| {
                crate::error::GatewayError::Backend(e.to_string())
            })?;

        // ② Re-register it wrapped so the gateway can add its own schemas
        // (`pg_catalog` in assembly step 3, overlay views in step 4) under the
        // SAME catalog name. The real `FlussCatalogProvider` rejects
        // `register_schema` ("Registering new schemas is not supported"), but the
        // assembly contract installs pg_catalog UNDER the fluss catalog; the wrapper
        // satisfies both: it delegates Fluss database/table resolution to the live
        // provider and keeps gateway-registered schemas in a small overlay map.
        let live = ctx.catalog(catalog_name).ok_or_else(|| {
            crate::error::GatewayError::Internal(format!(
                "fluss catalog {catalog_name} missing right after registration"
            ))
        })?;
        ctx.register_catalog(catalog_name, Arc::new(OverlayCatalogProvider::new(live)));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OverlayCatalogProvider — fluss catalog + gateway-registered schemas
// ---------------------------------------------------------------------------

/// Wraps the live Fluss [`CatalogProvider`] so the gateway can register extra
/// schemas (`pg_catalog`, overlay views) under the same catalog name.
///
/// The real `FlussCatalogProvider` is read-only and its default
/// `register_schema` returns "not supported"; `datafusion-pg-catalog`'s
/// `setup_pg_catalog` needs to `register_schema("pg_catalog", …)` under the
/// fluss catalog (assembly step 3). This wrapper resolves the conflict without
/// the gateway absorbing any catalog logic: Fluss databases/tables resolve
/// through the live provider; gateway-installed schemas live in a small in-memory
/// overlay that takes precedence on name collision.
#[derive(Debug)]
struct OverlayCatalogProvider {
    fluss: Arc<dyn datafusion::catalog::CatalogProvider>,
    overlay: std::sync::Mutex<
        std::collections::HashMap<String, Arc<dyn datafusion::catalog::SchemaProvider>>,
    >,
}

impl OverlayCatalogProvider {
    fn new(fluss: Arc<dyn datafusion::catalog::CatalogProvider>) -> Self {
        Self {
            fluss,
            overlay: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl datafusion::catalog::CatalogProvider for OverlayCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = self.fluss.schema_names();
        for k in self.overlay.lock().unwrap().keys() {
            if !names.contains(k) {
                names.push(k.clone());
            }
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        // Gateway-registered schemas win over Fluss databases on name collision.
        if let Some(s) = self.overlay.lock().unwrap().get(name) {
            return Some(s.clone());
        }
        self.fluss.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn datafusion::catalog::SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::catalog::SchemaProvider>>> {
        Ok(self.overlay.lock().unwrap().insert(name.to_string(), schema))
    }
}

// ---------------------------------------------------------------------------
// Stub implementations for tests and for the not-yet-real pg_catalog overlay.
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

/// Stub overlay installer: a no-op until the real overlay lands.
#[derive(Debug, Default)]
pub struct StubPgCatalogOverlayInstaller;

#[async_trait::async_trait]
impl PgCatalogOverlayInstaller for StubPgCatalogOverlayInstaller {
    async fn install_overlay(&self, _ctx: &SessionContext) -> GatewayResult<()> {
        // Stub: the real overlay projects Fluss tables into PG views here.
        Ok(())
    }
}
