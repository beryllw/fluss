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

//! GatewayInstance facade.
//!
//! The unified, protocol-agnostic facade over session / SQL / operation /
//! direct / metadata access. Protocol modules (PostgreSQL, REST, and future
//! Flight SQL / gRPC) depend on this trait, never the reverse. The core exposes
//! capabilities only — never internal service composition — so the protocol
//! layer can integration-test and inject fakes against a stable surface while
//! internal services are refactored freely.
//! Design: `DESIGN.md` (core layering) and `design/core-session.md`.

use async_trait::async_trait;

mod gateway_instance;
pub use gateway_instance::GatewayInstanceImpl;

use crate::error::GatewayResult;
use crate::types::{
    CancelResult, CreateTableRequest, DescribeSqlRequest, DirectReadRequest, DirectReadResult,
    DirectWriteRequest, DirectWriteResult, ExecuteSqlRequest, MetadataScope, OpenSessionRequest,
    OperationId, OperationStatusSnapshot, SessionId, SessionMutation, SessionSnapshot,
    SqlDescription, SqlExecution, TableInfo, TableRef,
};

/// The single core facade exposed to protocol modules.
#[async_trait]
pub trait GatewayInstance: Send + Sync {
    // --- Session ---
    async fn open_session(&self, req: OpenSessionRequest) -> GatewayResult<SessionSnapshot>;
    async fn close_session(&self, session_id: SessionId) -> GatewayResult<()>;
    async fn alter_session(
        &self,
        session_id: SessionId,
        mutation: SessionMutation,
    ) -> GatewayResult<SessionSnapshot>;
    async fn get_session(&self, session_id: SessionId) -> GatewayResult<SessionSnapshot>;

    // --- SQL ---
    async fn describe_sql(&self, req: DescribeSqlRequest) -> GatewayResult<SqlDescription>;
    async fn execute_sql(&self, req: ExecuteSqlRequest) -> GatewayResult<SqlExecution>;

    // --- Operation ---
    async fn cancel_operation(&self, op_id: OperationId) -> GatewayResult<CancelResult>;
    async fn get_operation_status(
        &self,
        op_id: OperationId,
    ) -> GatewayResult<OperationStatusSnapshot>;

    // --- Direct path ---
    async fn read_direct(&self, req: DirectReadRequest) -> GatewayResult<DirectReadResult>;
    async fn write_direct(&self, req: DirectWriteRequest) -> GatewayResult<DirectWriteResult>;

    // --- Metadata ---
    async fn list_databases(&self, scope: MetadataScope) -> GatewayResult<Vec<String>>;
    /// List table names within `database`. The `database` arg matches the facade
    /// shape: the backend `list_tables(db)` is database-scoped, and protocol frontends
    /// (REST `{db}` path segment, PG catalog views) always carry one.
    async fn list_tables(
        &self,
        scope: MetadataScope,
        database: String,
    ) -> GatewayResult<Vec<String>>;
    async fn get_table_info(
        &self,
        scope: MetadataScope,
        table: TableRef,
    ) -> GatewayResult<TableInfo>;

    // --- Table management / DDL ---
    async fn create_table(
        &self,
        scope: MetadataScope,
        request: CreateTableRequest,
    ) -> GatewayResult<()>;
    async fn drop_table(
        &self,
        scope: MetadataScope,
        table: TableRef,
        ignore_if_not_exists: bool,
    ) -> GatewayResult<()>;
}
