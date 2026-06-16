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

//! P1 — shared neutral domain types.
//!
//! Gateway-owned, protocol-neutral domain types: identity / routing newtypes,
//! session / SQL / operation / direct / metadata DTOs, and Arrow-native result
//! wrappers shared across the SQL and direct paths. These are intentionally
//! free of protocol types (pgwire / axum / HTTP / JSON) and free of any
//! fluss-datafusion / fluss-rs dependency. They are NOT the fluss-datafusion
//! `types/` (DataFusion type bridging); do not conflate the two.
//! Design: `design/core-session.md` §P1.2-§P1.6, §P2.2-§P2.4 and
//! `design/direct-path.md` §1-§3.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::ParamValues;
use datafusion::physical_plan::SendableRecordBatchStream;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// identity / routing (§P1.2)
// ---------------------------------------------------------------------------

/// Connection-scoped SQL session identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

/// Query-scoped operation identifier (only the SQL path exposes operations).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationId(pub String);

/// Per-request identifier used for tracing the direct path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(pub String);

/// Logical cluster identifier. The inner value is semantically a cluster name;
/// Phase 1 is always `"default"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterId(pub String);

/// Authenticated identity, preserved through the internal call chain even where
/// Fluss does not yet consume it (DESIGN.md §2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    pub name: String,
}

/// Identifier of an installed SQL environment provider (e.g. `"postgres"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlEnvironmentId(pub String);

// ---------------------------------------------------------------------------
// protocol context (§P1.2)
// ---------------------------------------------------------------------------

/// Which frontend protocol a session / request arrived through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolKind {
    Postgres,
    Rest,
}

/// Minimal connection-origin information captured at session open time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientInfo {
    pub protocol: ProtocolKind,
    /// Remote peer address, when known (e.g. "10.0.0.1:54321").
    pub peer_addr: Option<String>,
}

// ---------------------------------------------------------------------------
// session domain (§P2.2-§P2.4)
// ---------------------------------------------------------------------------

/// Information fixed at connection-establishment time.
///
/// `principal` / `cluster` / `sql_environment` are read-only for the session
/// lifetime; all mutable state lives in [`SessionVars`].
#[derive(Debug, Clone)]
pub struct OpenSessionRequest {
    pub principal: Principal,
    pub cluster: ClusterId,
    pub sql_environment: Option<SqlEnvironmentId>,
    pub initial_vars: SessionVars,
    pub client_info: ClientInfo,
}

/// A value for a namespaced session environment variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionVarValue {
    String(String),
    Bool(bool),
    Int(i64),
}

/// The single source of truth for mutable session state.
///
/// Only variables that are meaningful across protocols live as typed top-level
/// fields; protocol-local variables go into `environment` under a namespaced key
/// (e.g. `pg.search_path`).
#[derive(Debug, Clone, Default)]
pub struct SessionVars {
    pub statement_timeout: Option<Duration>,
    pub timezone: Option<String>,
    pub current_catalog: Option<String>,
    pub current_schema: Option<String>,
    pub environment: BTreeMap<String, SessionVarValue>,
}

/// A read-only snapshot of session state returned to protocol adapters.
#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub id: SessionId,
    pub principal: Principal,
    pub cluster: ClusterId,
    pub sql_environment: Option<SqlEnvironmentId>,
    pub vars: SessionVars,
    pub client_info: ClientInfo,
}

/// A single mutation applied to a live session. Processing order is fixed:
/// update [`SessionVars`] first, then compute and apply the runtime effect.
/// 详见 core-session.md §P2.4。
#[derive(Debug, Clone)]
pub enum SessionMutation {
    SetStatementTimeout(Option<Duration>),
    SetTimezone(Option<String>),
    SetCurrentCatalog(Option<String>),
    SetCurrentSchema(Option<String>),
    SetEnvironmentVar { key: String, value: SessionVarValue },
    UnsetEnvironmentVar { key: String },
    /// Reset all mutable session state to the values fixed at `open_session`
    /// (the connection's initial [`SessionVars`]) and force a context rebuild
    /// before the next query. Backs `DISCARD ALL` (sql-path.md §P4.3); its effect
    /// is always `RebuildContextBeforeNextQuery`.
    ResetAll,
}

/// How a [`SessionMutation`] affects the live `SessionContext`.
/// 详见 core-session.md §P2.4 / §P2.5。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMutationEffect {
    SessionOnly,
    ApplyToExistingContext,
    RebuildContextBeforeNextQuery,
}

// ---------------------------------------------------------------------------
// SQL domain (§P1.4-§P1.5)
// ---------------------------------------------------------------------------

/// Request to describe (plan / analyze without executing) a SQL statement.
#[derive(Debug, Clone)]
pub struct DescribeSqlRequest {
    pub session_id: SessionId,
    pub statement: String,
}

/// Result of describing a SQL statement: the result schema plus the inferred
/// parameter types, both Arrow-native. `param_types[i]` is the Arrow type of the
/// `$(i+1)` placeholder; the protocol boundary maps these to PG type OIDs for the
/// `ParameterDescription` reply (sql-path.md §P4.4). An empty vec means the
/// statement is non-parameterized (or the environment cannot infer parameters).
#[derive(Debug, Clone)]
pub struct SqlDescription {
    pub schema: SchemaRef,
    pub param_types: Vec<DataType>,
}

/// Per-execution overrides; merged with session vars at execution time.
#[derive(Debug, Clone, Default)]
pub struct SqlExecutionOptions {
    /// Optional per-request timeout override; combined with the session
    /// `statement_timeout` per core-session.md §P2.9.
    pub request_timeout: Option<Duration>,
}

/// Request to execute a SQL statement within a session.
///
/// `params` carries the bound positional parameter values for a parameterized
/// statement (the `$1..$N` placeholders), already decoded to DataFusion-native
/// [`ParamValues`] at the protocol boundary (PG wire text/binary -> `ScalarValue`,
/// sql-path.md §P4.4). It is `None` for a plain, non-parameterized statement
/// (e.g. the simple-query path), and the SQL service applies it to the logical
/// plan before execution.
#[derive(Debug, Clone)]
pub struct ExecuteSqlRequest {
    pub session_id: SessionId,
    pub statement: String,
    pub params: Option<ParamValues>,
    pub options: SqlExecutionOptions,
}

/// Arrow-native result of executing a SQL statement.
///
/// The `Command` branch is kept for shape stability even though Phase 1 PG is
/// read-only — preserving the shape is not the same as supporting SQL writes
/// (core-session.md §P1.5). Carries a stream, so this type is not `Clone`/`Debug`.
pub enum SqlExecution {
    Query {
        operation_id: OperationId,
        schema: SchemaRef,
        stream: SendableRecordBatchStream,
    },
    Command {
        operation_id: OperationId,
        affected_rows: u64,
    },
}

// ---------------------------------------------------------------------------
// operation domain (§P2.7, §P2.10)
// ---------------------------------------------------------------------------

/// Operation lifecycle state. `CancelRequested` is transitional; `Cancelled` /
/// `TimedOut` / `Failed` are mutually exclusive terminal states with no
/// regression. 详见 core-session.md §P2.7。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationState {
    Pending,
    Running,
    CancelRequested,
    Finished,
    Failed,
    Cancelled,
    TimedOut,
}

/// A read-only snapshot of an operation's status.
#[derive(Debug, Clone)]
pub struct OperationStatusSnapshot {
    pub id: OperationId,
    pub state: OperationState,
    pub statement_summary: String,
    pub error: Option<String>,
}

/// Outcome of a cancel request. Distinguishes the three cases required by
/// core-session.md §P2.10.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    /// No operation matched the supplied id.
    NotFound,
    /// The operation was already in a terminal state; nothing to cancel.
    AlreadyTerminal,
    /// The cancel request was accepted (cooperative / best-effort).
    Accepted,
}

// ---------------------------------------------------------------------------
// direct domain (direct-path.md §1-§3)
// ---------------------------------------------------------------------------

/// Request-scoped execution context for the direct path. Constructed per REST
/// request and dropped at request end; never enters the SessionManager and
/// never creates an Operation. 权威定义见 direct-path.md §1。
pub struct RequestExecutionContext {
    pub principal: Principal,
    pub cluster: ClusterId,
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
}

/// Direct read request shapes. 本期后置（Phase 1 不实现），形状先冻结。
/// 详见 direct-path.md §2。
pub enum DirectReadRequest {
    /// Full primary-key equality point lookup.
    Lookup {
        context: RequestExecutionContext,
        table: TableRef,
        keys: RecordBatch,
    },
    /// Multi-key batched point lookup.
    BatchLookup {
        context: RequestExecutionContext,
        table: TableRef,
        keys: RecordBatch,
    },
    /// Single-column string/binary primary-key prefix scan.
    PrefixScan {
        context: RequestExecutionContext,
        table: TableRef,
        prefix: RecordBatch,
        limit: usize,
    },
    /// Bounded log scan; LIMIT is required, offset ascending, default earliest.
    LogScan {
        context: RequestExecutionContext,
        table: TableRef,
        limit: usize,
    },
}

/// Arrow-native direct read result; encoding to JSON / Arrow IPC happens at the
/// REST boundary. Carries a stream, so this type is not `Clone`/`Debug`.
/// 本期后置。详见 direct-path.md §2 / §5。
pub struct DirectReadResult {
    pub schema: SchemaRef,
    pub stream: SendableRecordBatchStream,
}

/// Direct write request shapes. Body is already decoded to Arrow-native at the
/// boundary; schema is taken from the target table (no schema-on-write).
/// 详见 direct-path.md §3。
pub enum DirectWriteRequest {
    /// KV table upsert of a batch of rows.
    KvUpsert {
        context: RequestExecutionContext,
        table: TableRef,
        rows: RecordBatch,
    },
    /// KV table delete of a batch by primary key.
    KvDelete {
        context: RequestExecutionContext,
        table: TableRef,
        keys: RecordBatch,
    },
    /// Log table append of a batch of rows.
    LogAppend {
        context: RequestExecutionContext,
        table: TableRef,
        rows: RecordBatch,
    },
}

/// Domain write summary. Carries no HTTP status semantics; the REST boundary
/// maps this (and any error) to a response. 详见 direct-path.md §6。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWriteResult {
    /// Number of rows the gateway submitted to the backend.
    pub rows_written: u64,
}

// ---------------------------------------------------------------------------
// metadata domain (§P1.3, §P1.6)
// ---------------------------------------------------------------------------

/// Explicit access scope for metadata APIs; metadata is cluster-scoped and does
/// not implicitly read session state. 详见 core-session.md §P1.3。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataScope {
    pub principal: Principal,
    pub cluster: ClusterId,
}

/// A fully-qualified table reference within a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

/// Minimal table metadata summary. 详见 core-session.md §P1.6（后续阶段充实）。
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: TableRef,
    pub schema: SchemaRef,
}

// ---------------------------------------------------------------------------
// DDL domain (table management; see design/direct-path.md "表管理（DDL）API")
// ---------------------------------------------------------------------------

/// A neutral column data type for CREATE TABLE. Protocol-agnostic and free of
/// Fluss types: the backend maps each variant to `fluss::metadata::DataTypes`
/// (the single place Fluss type names are touched). Mirrors the REST `type`
/// vocabulary documented in `design/direct-path.md`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal { precision: u32, scale: u32 },
    Char { length: u32 },
    String,
    Binary { length: u32 },
    Bytes,
    Date,
    Time { precision: u32 },
    Timestamp { precision: u32 },
}

/// One column in a CREATE TABLE request. `nullable` defaults to true at the
/// protocol boundary; primary-key columns are forced non-null by Fluss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
}

/// Bucket distribution for a table. `bucket_keys` empty means "let Fluss decide"
/// (defaults to the primary key for KV tables).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableDistribution {
    pub bucket_keys: Vec<String>,
    pub bucket_count: Option<i32>,
}

/// A request to create a table. Cluster-scoped metadata mutation (paired with a
/// [`MetadataScope`] at the instance boundary), not a direct-path write.
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    pub table: TableRef,
    pub columns: Vec<ColumnSpec>,
    /// Empty => Log table; non-empty => KV (primary-key) table.
    pub primary_key: Vec<String>,
    pub distribution: Option<TableDistribution>,
    pub comment: Option<String>,
    /// Table properties (name/value), passed through to Fluss table options.
    pub properties: Vec<(String, String)>,
    /// When true, suppress the "already exists" error (CREATE TABLE IF NOT EXISTS).
    pub ignore_if_exists: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_ref_constructs_and_compares() {
        let a = TableRef {
            database: "db".into(),
            table: "t".into(),
        };
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(
            a,
            TableRef {
                database: "db".into(),
                table: "other".into(),
            }
        );
    }

    #[test]
    fn newtypes_construct_and_compare() {
        assert_eq!(SessionId("s1".into()), SessionId("s1".into()));
        assert_ne!(OperationId("o1".into()), OperationId("o2".into()));
        assert_eq!(ClusterId("default".into()), ClusterId("default".into()));
        let p = Principal {
            name: "alice".into(),
        };
        assert_eq!(p.clone(), p);
    }

    #[test]
    fn session_vars_default_is_empty() {
        let v = SessionVars::default();
        assert!(v.statement_timeout.is_none());
        assert!(v.environment.is_empty());
    }

    #[test]
    fn cancel_result_variants_distinct() {
        assert_ne!(CancelResult::NotFound, CancelResult::Accepted);
        assert_ne!(CancelResult::AlreadyTerminal, CancelResult::Accepted);
    }
}
