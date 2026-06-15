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

//! P3.1 — [`SqlEnvironmentProvider`]: the single SQL-environment assembly entry.
//!
//! Installs the catalog / pg compatibility objects / initial session vars that a
//! SQL frontend (PostgreSQL today) needs onto a per-session `SessionContext`.
//! The provider holds NO per-session state: it reads the [`GatewaySession`]
//! (whose [`SessionVars`] is the single source of truth) and writes the result
//! into the caller-supplied `ctx`. Design: `design/sql-path.md` §P3.1.
//!
//! Responsibility boundary (design §P3.4): the provider owns *what is installed
//! into the `SessionContext`* (catalog / pg_catalog / overlay / vars). It never
//! touches the wire — startup handshake, query rewrite, Arrow→PG encoding, and
//! prepared-statement wire lifecycle all belong to the PostgreSQL adapter (P4),
//! not here. There are no pgwire concepts in this module.

use datafusion::execution::context::SessionContext;

use crate::error::GatewayResult;
use crate::session::GatewaySession;
use crate::types::SessionMutation;

/// Per-protocol SQL environment assembly seam (design §P3.1).
#[async_trait::async_trait]
pub trait SqlEnvironmentProvider: Send + Sync {
    /// Full assembly: build everything this SQL environment needs onto `ctx`.
    ///
    /// Called on first lazy init and on every rebuild. It is *re-entrant*: each
    /// call walks the whole fixed assembly order from a clean slate, so a rebuilt
    /// context fully reflects the latest [`SessionVars`] with no half-assembled
    /// residue and no need to replay historical mutations.
    async fn prepare_session_context(
        &self,
        session: &GatewaySession,
        ctx: &SessionContext,
    ) -> GatewayResult<()>;

    /// Incremental live apply for a single mutation.
    ///
    /// Only mutations P2 classified as `ApplyToExistingContext` (see
    /// `SessionMutationEffect`) are meaningful here; everything else is a no-op
    /// (`SessionOnly` never reaches a provider; `RebuildContextBeforeNextQuery`
    /// is handled by dirty + the next `prepare_session_context`). Must be
    /// idempotent: applying the same mutation twice leaves `ctx` unchanged the
    /// second time.
    async fn apply_session_mutation(
        &self,
        session: &GatewaySession,
        ctx: &SessionContext,
        mutation: &SessionMutation,
    ) -> GatewayResult<()>;
}
