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

//! P3.2 — [`SqlEnvironmentRegistry`]: select a provider per SQL frontend.
//!
//! `Instance` holds one shared, read-only registry and looks up a provider by
//! [`SqlEnvironmentId`] (e.g. `"postgres"`). Providers are stored as
//! `Arc<dyn SqlEnvironmentProvider>` so a single provider — and the heavy shared
//! objects it owns (shared `FlussDatafusion`, pg_catalog templates) — is reused
//! across sessions. The registry exists so future Flight SQL / MySQL frontends
//! register a new provider WITHOUT changing `Instance`; Phase 1 registers only
//! `PgSqlEnvironmentProvider`. Design: `design/sql-path.md` §P3.2.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{GatewayError, GatewayResult};
use crate::sql::environment::provider::SqlEnvironmentProvider;
use crate::types::SqlEnvironmentId;

/// Shared, read-only map from [`SqlEnvironmentId`] to a provider.
#[derive(Default, Clone)]
pub struct SqlEnvironmentRegistry {
    providers: HashMap<SqlEnvironmentId, Arc<dyn SqlEnvironmentProvider>>,
}

impl SqlEnvironmentRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    /// Register `provider` under `id`. A later registration with the same id
    /// replaces the earlier one (last write wins).
    pub fn register(
        &mut self,
        id: SqlEnvironmentId,
        provider: Arc<dyn SqlEnvironmentProvider>,
    ) {
        self.providers.insert(id, provider);
    }

    /// Look up the provider for `id`, or a clear error if none is registered.
    pub fn get(&self, id: &SqlEnvironmentId) -> GatewayResult<Arc<dyn SqlEnvironmentProvider>> {
        self.providers.get(id).cloned().ok_or_else(|| {
            GatewayError::Unsupported(format!(
                "no SQL environment provider registered for '{}'",
                id.0
            ))
        })
    }

    pub fn contains(&self, id: &SqlEnvironmentId) -> bool {
        self.providers.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}
