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

//! P3 — SQL environment assembly layer.
//!
//! SQL environment differences (catalog wiring, pg_catalog base + overlay,
//! initial session vars) are installed through a [`SqlEnvironmentProvider`],
//! never hardcoded into `Instance`. A [`SqlEnvironmentRegistry`] selects the
//! provider per SQL frontend (PostgreSQL today). `PgSqlEnvironmentProvider`
//! implements the fixed 5-step assembly order. The provider owns only
//! *SessionContext content*; the wire belongs to the PG adapter (P4).
//! Design: `design/sql-path.md` §P3.1-§P3.4 and `design/datafusion-contract.md` D1.
//!
//! Submodules:
//! - `provider`      — the [`SqlEnvironmentProvider`] trait (§P3.1).
//! - `registry`      — [`SqlEnvironmentRegistry`] (§P3.2).
//! - `collaborators` — stub seams for the not-yet-ready external steps (2 & 4).
//! - `apply`         — step-5 session-vars snapshot applier (gateway-owned).
//! - `pg`            — [`PgSqlEnvironmentProvider`] + the 5-step order (§P3.3).
//! - `bridge`        — adapts registry+provider onto the P2 builder seam (§P3.4).

pub mod apply;
pub mod bridge;
pub mod collaborators;
pub mod pg;
pub mod provider;
pub mod registry;

pub use bridge::EnvironmentContextBuilder;
pub use collaborators::{
    FlussCatalogInstaller, PgCatalogOverlayInstaller, StubFlussCatalogInstaller,
    StubPgCatalogOverlayInstaller,
};
pub use pg::{PgSqlEnvironmentProvider, FLUSS_CATALOG};
pub use provider::SqlEnvironmentProvider;
pub use registry::SqlEnvironmentRegistry;

#[cfg(test)]
mod registry_tests {
    use super::*;
    use crate::error::GatewayError;
    use crate::types::SqlEnvironmentId;
    use std::sync::Arc;

    /// Test 4: registry register / lookup; unknown id gives a clear error.
    #[test]
    fn register_and_lookup() {
        let mut reg = SqlEnvironmentRegistry::new();
        assert!(reg.is_empty());
        let provider: Arc<dyn SqlEnvironmentProvider> = Arc::new(PgSqlEnvironmentProvider::with_stubs());
        reg.register(SqlEnvironmentId("postgres".into()), provider);
        assert_eq!(reg.len(), 1);
        assert!(reg.contains(&SqlEnvironmentId("postgres".into())));
        assert!(reg.get(&SqlEnvironmentId("postgres".into())).is_ok());
    }

    #[test]
    fn unknown_environment_errors_clearly() {
        let reg = SqlEnvironmentRegistry::new();
        match reg.get(&SqlEnvironmentId("mysql".into())) {
            Err(GatewayError::Unsupported(msg)) => assert!(msg.contains("mysql")),
            Err(other) => panic!("expected Unsupported error, got {other:?}"),
            Ok(_) => panic!("expected an error for an unregistered environment"),
        }
    }

    #[test]
    fn phase1_registers_only_postgres() {
        let mut reg = SqlEnvironmentRegistry::new();
        reg.register(
            SqlEnvironmentId("postgres".into()),
            Arc::new(PgSqlEnvironmentProvider::with_stubs()),
        );
        assert_eq!(reg.len(), 1);
        assert!(reg.contains(&SqlEnvironmentId("postgres".into())));
    }
}
