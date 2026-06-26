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

//! The rmcp `ServerHandler` and the four read-only tools.
//!
//! Each tool maps onto the protocol-agnostic [`GatewayInstance`] facade; rmcp
//! wire types never reach `instance/` or `backend/` (mirrors how `rest/otlp.rs`
//! keeps OTLP types at the boundary). The authenticated [`Principal`] is read
//! from the HTTP request `Parts` that the Streamable HTTP transport injects into
//! the request extensions — the auth middleware in [`super`] populates it.
//!
//! `query` is the only tool that touches the SQL path: it borrows it through an
//! ephemeral per-call session (MCP is request-scoped, like REST), drains the
//! Arrow result to bounded JSON, and always closes the session.

use std::sync::Arc;

use axum::http::request::Parts;
use futures::StreamExt;
use rmcp::handler::server::common::Extension;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ServerCapabilities, ServerInfo};
use rmcp::schemars::JsonSchema;
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler};
use serde::Deserialize;
use serde_json::{json, Value};

use super::tools::{batch_to_json, ensure_read_only, table_info_json};
use crate::error::GatewayError;
use crate::instance::GatewayInstance;
use crate::types::{
    ClientInfo, ClusterId, ExecuteSqlRequest, MetadataScope, OpenSessionRequest, Principal,
    ProtocolKind, SessionVars, SqlEnvironmentId, SqlExecution, SqlExecutionOptions, TableRef,
};

/// Default and hard caps for the `query` tool's row budget. A bounded result is
/// what an MCP tool returns (one response, not a stream); the cap also protects
/// the gateway from an agent's unbounded scan.
const DEFAULT_MAX_ROWS: usize = 1000;
const HARD_CAP_ROWS: usize = 10_000;

/// The SQL environment the `query` tool runs under — the same one the PostgreSQL
/// frontend registers (`main.rs`), so MCP reads see the same catalog/`pg_catalog`.
const SQL_ENVIRONMENT: &str = "postgres";

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListTablesArgs {
    /// Database whose tables to list.
    pub database: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DescribeTableArgs {
    /// Database the table belongs to.
    pub database: String,
    /// Table to describe.
    pub table: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryArgs {
    /// A single read-only SQL statement (SELECT / WITH / EXPLAIN / SHOW / DESCRIBE).
    pub sql: String,
    /// Maximum rows to return (default 1000, capped at 10000).
    #[serde(default)]
    pub max_rows: Option<usize>,
}

/// The MCP tool server. Holds the facade and the fixed (single) cluster; a fresh
/// instance is built per session by the transport's service factory.
pub struct McpHandler {
    instance: Arc<dyn GatewayInstance>,
    cluster: ClusterId,
    tool_router: ToolRouter<McpHandler>,
}

#[tool_router]
impl McpHandler {
    pub fn new(instance: Arc<dyn GatewayInstance>, cluster: ClusterId) -> Self {
        Self {
            instance,
            cluster,
            tool_router: Self::tool_router(),
        }
    }

    fn scope(&self, principal: Principal) -> MetadataScope {
        MetadataScope {
            principal,
            cluster: self.cluster.clone(),
        }
    }

    #[tool(description = "List the databases available in the Fluss cluster.")]
    async fn list_databases(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let principal = principal_from(&parts)?;
        let dbs = self
            .instance
            .list_databases(self.scope(principal))
            .await
            .map_err(to_mcp_err)?;
        Ok(CallToolResult::structured(json!({ "databases": dbs })))
    }

    #[tool(description = "List the tables in a database.")]
    async fn list_tables(
        &self,
        Parameters(args): Parameters<ListTablesArgs>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let principal = principal_from(&parts)?;
        let tables = self
            .instance
            .list_tables(self.scope(principal), args.database)
            .await
            .map_err(to_mcp_err)?;
        Ok(CallToolResult::structured(json!({ "tables": tables })))
    }

    #[tool(description = "Describe a table's columns (name, data type, nullability).")]
    async fn describe_table(
        &self,
        Parameters(args): Parameters<DescribeTableArgs>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let principal = principal_from(&parts)?;
        let table = TableRef {
            database: args.database,
            table: args.table,
        };
        let info = self
            .instance
            .get_table_info(self.scope(principal), table)
            .await
            .map_err(to_mcp_err)?;
        Ok(CallToolResult::structured(table_info_json(&info)))
    }

    #[tool(
        description = "Run a single read-only SQL query (SELECT/WITH/EXPLAIN/SHOW/DESCRIBE) and return up to `max_rows` rows as JSON."
    )]
    async fn query(
        &self,
        Parameters(args): Parameters<QueryArgs>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let principal = principal_from(&parts)?;
        ensure_read_only(&args.sql).map_err(to_mcp_err)?;
        let max = args
            .max_rows
            .unwrap_or(DEFAULT_MAX_ROWS)
            .clamp(1, HARD_CAP_ROWS);

        // Ephemeral session: MCP is request-scoped, so we borrow the SQL path for
        // exactly this call and always close it (success or error).
        let snap = self
            .instance
            .open_session(OpenSessionRequest {
                principal,
                cluster: self.cluster.clone(),
                sql_environment: Some(SqlEnvironmentId(SQL_ENVIRONMENT.into())),
                initial_vars: SessionVars::default(),
                client_info: ClientInfo {
                    protocol: ProtocolKind::Mcp,
                    peer_addr: None,
                },
            })
            .await
            .map_err(to_mcp_err)?;

        let session_id = snap.id.clone();
        let sql = args.sql;
        let result = run_query(self.instance.as_ref(), &session_id, &sql, max).await;
        let _ = self.instance.close_session(session_id).await;

        result
            .map(|value| {
                let mut result = CallToolResult::structured(value);
                result.content = vec![Content::text(sql)];
                result
            })
            .map_err(to_mcp_err)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Read-only access to Apache Fluss tables. Use `list_databases` and \
             `list_tables` to discover tables, `describe_table` to inspect a table's \
             columns, and `query` to run a single read-only SQL statement.",
        )
    }
}

/// Drain a read-only query into a bounded JSON envelope `{rows, row_count,
/// truncated}`. Caps accumulation at `max` rows so a large scan cannot blow up
/// memory; `batch_to_json` re-checks the cap as defense-in-depth.
async fn run_query(
    instance: &dyn GatewayInstance,
    session_id: &crate::types::SessionId,
    sql: &str,
    max: usize,
) -> Result<Value, GatewayError> {
    let exec = instance
        .execute_sql(ExecuteSqlRequest {
            session_id: session_id.clone(),
            statement: sql.to_owned(),
            params: None,
            options: SqlExecutionOptions::default(),
        })
        .await?;

    match exec {
        // PG is read-only; a command-shaped result carries no rows.
        SqlExecution::Command { .. } => {
            Ok(json!({ "rows": [], "row_count": 0, "truncated": false }))
        }
        SqlExecution::Query { mut stream, .. } => {
            let mut batches = Vec::new();
            let mut total = 0usize;
            let mut truncated = false;
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| GatewayError::Backend(e.to_string()))?;
                total += batch.num_rows();
                batches.push(batch);
                if total > max {
                    truncated = true;
                    break;
                }
            }
            let (rows, capped) = batch_to_json(&batches, max)?;
            Ok(json!({
                "row_count": rows.len(),
                "truncated": truncated || capped,
                "rows": rows,
            }))
        }
    }
}

/// Pull the authenticated principal the auth middleware stashed in the request
/// extensions. Absence is an internal error: the middleware rejects unauthenticated
/// requests with 401 before they ever reach a tool.
fn principal_from(parts: &Parts) -> Result<Principal, ErrorData> {
    parts
        .extensions
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| ErrorData::internal_error("missing authenticated principal", None))
}

/// Map a domain error to an MCP tool error, preserving the message. Bad
/// input / not-found map to invalid params; everything else is internal.
fn to_mcp_err(err: GatewayError) -> ErrorData {
    use GatewayError::*;
    match err {
        InvalidArgument(_)
        | DatabaseNotFound { .. }
        | TableNotFound { .. }
        | SessionNotFound(_)
        | OperationNotFound(_)
        | Unsupported(_) => ErrorData::invalid_params(err.to_string(), None),
        Unauthenticated(_) | Unauthorized(_) => {
            ErrorData::invalid_request(err.to_string(), None)
        }
        _ => ErrorData::internal_error(err.to_string(), None),
    }
}
