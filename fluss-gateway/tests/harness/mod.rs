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

//! T1-T4 — shared integration test harness.
//!
//! Provides a `FakeInstance` ([`GatewayInstance`]) and a spawned PostgreSQL
//! frontend so protocol behavior can be driven by a real wire client
//! (`tokio-postgres`) over loopback TCP with no Fluss cluster (P4 test
//! strategy). `FakeInstance` returns deterministic Arrow results for fixed
//! SELECTs, treats SET/SHOW/BEGIN as `Command`, and records cancel calls. The
//! REST frontend (P5) reuses the same `FakeInstance`, which additionally records
//! direct writes and opened-session counts.
//!
//! Each integration test binary includes this module, so items used by only one
//! binary look "dead" to the other; silence that here.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::metadata::ScalarAndMetadata;
use datafusion::common::ParamValues;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;

use fluss_gateway::auth::TrustAuthenticator;
use fluss_gateway::error::{GatewayError, GatewayResult};
use fluss_gateway::instance::GatewayInstance;
use fluss_gateway::server::postgres::PgServer;
use fluss_gateway::server::rest::RestServer;
use fluss_gateway::types::{
    CancelResult, CreateTableRequest, DescribeSqlRequest, DirectReadRequest, DirectReadResult,
    DirectWriteRequest, DirectWriteResult, ExecuteSqlRequest, MetadataScope, OpenSessionRequest,
    OperationId, OperationState, OperationStatusSnapshot, SessionId, SessionMutation,
    SessionSnapshot, SqlDescription, SqlExecution, TableInfo, TableRef,
};

/// One recorded direct write, so REST tests can assert the request reached the
/// instance with the right shape, table, principal, and row count — and that no
/// session was opened along the way (the direct path is stateless).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWrite {
    pub kind: WriteKind,
    pub table: TableRef,
    pub principal: String,
    pub cluster: String,
    pub rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    KvUpsert,
    KvDelete,
    LogAppend,
}

/// Deterministic in-memory [`GatewayInstance`] for protocol tests.
///
/// Holds per-session [`SessionVars`] so SET/SHOW round-trip, and records the
/// last cancelled operation so the cancel test can assert it was reached.
#[derive(Default)]
pub struct FakeInstance {
    sessions: Mutex<HashMap<String, SessionSnapshot>>,
    next_id: Mutex<u64>,
    pub cancelled: Mutex<Vec<String>>,
    /// Every direct write that reached the instance (REST path assertions).
    pub writes: Mutex<Vec<RecordedWrite>>,
    /// Tables that should resolve as not-found from metadata (drives the 404
    /// mapping test). Anything not listed resolves to the canned schema.
    pub missing_tables: Mutex<Vec<String>>,
    /// Number of sessions ever opened. Direct (REST) requests must NOT increment
    /// this — it backs the "direct path has no session" semantic test.
    pub sessions_opened: Mutex<u64>,
    /// Every CREATE TABLE that reached the instance (REST DDL assertions).
    pub created_tables: Mutex<Vec<CreateTableRequest>>,
    /// Tables that should resolve as already-existing from create (drives the 409
    /// mapping test).
    pub existing_tables: Mutex<Vec<String>>,
    /// Every dropped table name (REST DDL assertions).
    pub dropped_tables: Mutex<Vec<String>>,
}

impl FakeInstance {
    pub fn new() -> Self {
        Self::default()
    }

    fn next(&self, prefix: &str) -> String {
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        format!("{prefix}{n}")
    }

    /// The fixed result schema/batch returned for the canned SELECT.
    fn canned_result() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    /// Build a single-row `(id, name)` result from decoded bind parameters: `$1`
    /// becomes `id` (int) and `$2` (if present) becomes `name` (text). Lets a test
    /// assert that wire parameters were decoded to `ScalarValue` and reached here.
    fn param_echo_result(
        values: &[ScalarAndMetadata],
    ) -> GatewayResult<(SchemaRef, RecordBatch)> {
        let to_internal = |m: String| GatewayError::Internal(m);
        let id = match values.first().map(|v| &v.value) {
            Some(ScalarValue::Int32(Some(v))) => *v,
            Some(ScalarValue::Int64(Some(v))) => *v as i32,
            other => return Err(to_internal(format!("unexpected $1 param: {other:?}"))),
        };
        let name = match values.get(1).map(|v| &v.value) {
            Some(ScalarValue::Utf8(Some(s))) => Some(s.clone()),
            Some(ScalarValue::Utf8(None)) | None => None,
            other => return Err(to_internal(format!("unexpected $2 param: {other:?}"))),
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .map_err(|e| to_internal(e.to_string()))?;
        Ok((schema, batch))
    }
}

/// Count `$N` positional placeholders in a SQL string (cheap, test-only).
fn count_placeholders(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut n = 0usize;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                n = n * 10 + (bytes[j] - b'0') as usize;
                j += 1;
            }
            max = max.max(n);
            i = j;
        } else {
            i += 1;
        }
    }
    max
}

#[async_trait]
impl GatewayInstance for FakeInstance {
    async fn open_session(&self, req: OpenSessionRequest) -> GatewayResult<SessionSnapshot> {
        *self.sessions_opened.lock().unwrap() += 1;
        let id = SessionId(self.next("sess-"));
        let snap = SessionSnapshot {
            id: id.clone(),
            principal: req.principal,
            cluster: req.cluster,
            sql_environment: req.sql_environment,
            vars: req.initial_vars,
            client_info: req.client_info,
        };
        self.sessions
            .lock()
            .unwrap()
            .insert(id.0.clone(), snap.clone());
        Ok(snap)
    }

    async fn close_session(&self, session_id: SessionId) -> GatewayResult<()> {
        self.sessions.lock().unwrap().remove(&session_id.0);
        Ok(())
    }

    async fn alter_session(
        &self,
        session_id: SessionId,
        mutation: SessionMutation,
    ) -> GatewayResult<SessionSnapshot> {
        let mut sessions = self.sessions.lock().unwrap();
        let snap = sessions
            .get_mut(&session_id.0)
            .ok_or_else(|| GatewayError::SessionNotFound(session_id.0.clone()))?;
        match mutation {
            SessionMutation::SetTimezone(tz) => snap.vars.timezone = tz,
            SessionMutation::SetCurrentCatalog(c) => snap.vars.current_catalog = c,
            SessionMutation::SetCurrentSchema(s) => snap.vars.current_schema = s,
            SessionMutation::SetStatementTimeout(t) => snap.vars.statement_timeout = t,
            SessionMutation::SetEnvironmentVar { key, value } => {
                snap.vars.environment.insert(key, value);
            }
            SessionMutation::UnsetEnvironmentVar { key } => {
                snap.vars.environment.remove(&key);
            }
            // DISCARD ALL: the fake clears mutable vars to defaults (the real
            // session restores the connection's initial vars).
            SessionMutation::ResetAll => {
                snap.vars = Default::default();
            }
        }
        Ok(snap.clone())
    }

    async fn get_session(&self, session_id: SessionId) -> GatewayResult<SessionSnapshot> {
        self.sessions
            .lock()
            .unwrap()
            .get(&session_id.0)
            .cloned()
            .ok_or_else(|| GatewayError::SessionNotFound(session_id.0))
    }

    async fn describe_sql(&self, req: DescribeSqlRequest) -> GatewayResult<SqlDescription> {
        let (schema, _) = Self::canned_result();
        // Report one Int32 parameter type per `$N` placeholder so the PG
        // ParameterDescription carries real OIDs (mirrors the canned param echo
        // in `execute_sql`, which treats $1 as int and $2 as text).
        let n = count_placeholders(&req.statement);
        let param_types = (0..n)
            .map(|i| if i == 1 { DataType::Utf8 } else { DataType::Int32 })
            .collect();
        Ok(SqlDescription { schema, param_types })
    }

    async fn execute_sql(&self, req: ExecuteSqlRequest) -> GatewayResult<SqlExecution> {
        // If the statement was parameterized, echo the bound `$1`/`$2` values back
        // as the (id, name) row so a test can prove decoded params reached here.
        let (schema, batch) = match &req.params {
            Some(ParamValues::List(values)) if !values.is_empty() => {
                Self::param_echo_result(values)?
            }
            _ => Self::canned_result(),
        };
        let op = OperationId(self.next("op-"));
        let s = schema.clone();

        // A statement mentioning SLEEP yields one batch then hangs forever, so a
        // query is observably "running" while an out-of-band cancel arrives. Any
        // other SELECT returns the canned 2-row result immediately.
        let hang = req.statement.to_ascii_uppercase().contains("SLEEP");
        // unfold state: Some(batch) -> emit it; then None -> either end (fast) or
        // pend forever (slow), reproducing a long-running scan deterministically.
        let inner = futures::stream::unfold(Some(batch), move |state| async move {
            match state {
                Some(b) => Some((Ok(b), None)),
                None => {
                    if hang {
                        futures::future::pending::<()>().await;
                    }
                    None
                }
            }
        });
        let stream = RecordBatchStreamAdapter::new(schema.clone(), inner);
        Ok(SqlExecution::Query {
            operation_id: op,
            schema: s,
            stream: Box::pin(stream),
        })
    }

    async fn cancel_operation(&self, op_id: OperationId) -> GatewayResult<CancelResult> {
        self.cancelled.lock().unwrap().push(op_id.0);
        Ok(CancelResult::Accepted)
    }

    async fn get_operation_status(
        &self,
        op_id: OperationId,
    ) -> GatewayResult<OperationStatusSnapshot> {
        Ok(OperationStatusSnapshot {
            id: op_id,
            state: OperationState::Finished,
            statement_summary: String::new(),
            error: None,
        })
    }

    async fn read_direct(&self, _req: DirectReadRequest) -> GatewayResult<DirectReadResult> {
        Err(GatewayError::Unsupported("direct read deferred".into()))
    }

    async fn write_direct(&self, req: DirectWriteRequest) -> GatewayResult<DirectWriteResult> {
        let (kind, context, table, rows) = match req {
            DirectWriteRequest::KvUpsert {
                context,
                table,
                rows,
            } => (WriteKind::KvUpsert, context, table, rows),
            DirectWriteRequest::KvDelete {
                context,
                table,
                keys,
            } => (WriteKind::KvDelete, context, table, keys),
            DirectWriteRequest::LogAppend {
                context,
                table,
                rows,
            } => (WriteKind::LogAppend, context, table, rows),
        };
        let n = rows.num_rows();
        self.writes.lock().unwrap().push(RecordedWrite {
            kind,
            table,
            principal: context.principal.name.clone(),
            cluster: context.cluster.0.clone(),
            rows: n,
        });
        Ok(DirectWriteResult {
            rows_written: n as u64,
        })
    }

    async fn list_databases(&self, _scope: MetadataScope) -> GatewayResult<Vec<String>> {
        Ok(vec!["fluss".into()])
    }

    async fn list_tables(
        &self,
        _scope: MetadataScope,
        _database: String,
    ) -> GatewayResult<Vec<String>> {
        Ok(vec!["t".into()])
    }

    async fn get_table_info(
        &self,
        _scope: MetadataScope,
        table: TableRef,
    ) -> GatewayResult<TableInfo> {
        if self.missing_tables.lock().unwrap().contains(&table.table) {
            return Err(GatewayError::TableNotFound {
                database: table.database,
                table: table.table,
            });
        }
        let (schema, _) = Self::canned_result();
        Ok(TableInfo { name: table, schema })
    }

    async fn create_table(
        &self,
        _scope: MetadataScope,
        request: CreateTableRequest,
    ) -> GatewayResult<()> {
        // Conflict path: tables listed in `existing_tables` already exist.
        if self
            .existing_tables
            .lock()
            .unwrap()
            .contains(&request.table.table)
            && !request.ignore_if_exists
        {
            return Err(GatewayError::TableAlreadyExists {
                database: request.table.database.clone(),
                table: request.table.table.clone(),
            });
        }
        self.created_tables.lock().unwrap().push(request);
        Ok(())
    }

    async fn drop_table(
        &self,
        _scope: MetadataScope,
        table: TableRef,
        ignore_if_not_exists: bool,
    ) -> GatewayResult<()> {
        if self.missing_tables.lock().unwrap().contains(&table.table) && !ignore_if_not_exists {
            return Err(GatewayError::TableNotFound {
                database: table.database,
                table: table.table,
            });
        }
        self.dropped_tables.lock().unwrap().push(table.table);
        Ok(())
    }
}

/// A running PostgreSQL frontend bound to an ephemeral loopback port, backed by
/// a shared [`FakeInstance`].
pub struct PgTestServer {
    pub port: u16,
    pub instance: Arc<FakeInstance>,
}

impl PgTestServer {
    /// Bind and spawn the PG frontend on `127.0.0.1:0`, returning the resolved
    /// port and the shared fake instance (so tests can assert on `cancelled`).
    pub async fn start() -> PgTestServer {
        Self::start_with_authenticator(
            Arc::new(FakeInstance::new()),
            Arc::new(TrustAuthenticator::new()),
        )
        .await
    }

    pub async fn start_with_authenticator(
        instance: Arc<FakeInstance>,
        authenticator: Arc<dyn fluss_gateway::auth::Authenticator>,
    ) -> PgTestServer {
        let server = PgServer::new(instance.clone(), authenticator);
        let (listener, addr) = PgServer::bind("127.0.0.1:0").await.unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        PgTestServer {
            port: addr.port(),
            instance,
        }
    }

    /// A `host=... port=... user=... dbname=...` connection string for
    /// `tokio-postgres`.
    pub fn conn_string(&self) -> String {
        format!(
            "host=127.0.0.1 port={} user=alice password=ignored dbname=fluss",
            self.port
        )
    }
}

/// A running REST frontend bound to an ephemeral loopback port, backed by a
/// shared [`FakeInstance`] so tests can assert on recorded writes / sessions.
pub struct RestTestServer {
    pub port: u16,
    pub instance: Arc<FakeInstance>,
}

impl RestTestServer {
    pub async fn start() -> RestTestServer {
        Self::start_with(Arc::new(FakeInstance::new())).await
    }

    pub async fn start_with(instance: Arc<FakeInstance>) -> RestTestServer {
        Self::start_with_authenticator(instance, Arc::new(TrustAuthenticator::new())).await
    }

    pub async fn start_with_authenticator(
        instance: Arc<FakeInstance>,
        authenticator: Arc<dyn fluss_gateway::auth::Authenticator>,
    ) -> RestTestServer {
        let server = RestServer::new(instance.clone(), authenticator);
        let (listener, addr) = RestServer::bind("127.0.0.1:0").await.unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        RestTestServer {
            port: addr.port(),
            instance,
        }
    }

    /// Base URL for the frozen `default` cluster prefix.
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}/v1/clusters/default", self.port)
    }
}
