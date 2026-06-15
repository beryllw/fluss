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
//! SELECTs, treats SET/SHOW/BEGIN as `Command`, and records cancel calls.

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
use fluss_gateway::types::{
    CancelResult, DescribeSqlRequest, DirectReadRequest, DirectReadResult, DirectWriteRequest,
    DirectWriteResult, ExecuteSqlRequest, MetadataScope, OpenSessionRequest, OperationId,
    OperationState, OperationStatusSnapshot, SessionId, SessionMutation, SessionSnapshot,
    SqlDescription, SqlExecution, TableInfo, TableRef,
};

/// Deterministic in-memory [`GatewayInstance`] for protocol tests.
///
/// Holds per-session [`SessionVars`] so SET/SHOW round-trip, and records the
/// last cancelled operation so the cancel test can assert it was reached.
#[derive(Default)]
pub struct FakeInstance {
    sessions: Mutex<HashMap<String, SessionSnapshot>>,
    next_id: Mutex<u64>,
    pub cancelled: Mutex<Vec<String>>,
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

    async fn write_direct(&self, _req: DirectWriteRequest) -> GatewayResult<DirectWriteResult> {
        Err(GatewayError::Unsupported("not used in PG tests".into()))
    }

    async fn list_databases(&self, _scope: MetadataScope) -> GatewayResult<Vec<String>> {
        Ok(vec!["fluss".into()])
    }

    async fn list_tables(&self, _scope: MetadataScope) -> GatewayResult<Vec<String>> {
        Ok(vec![])
    }

    async fn get_table_info(
        &self,
        _scope: MetadataScope,
        table: TableRef,
    ) -> GatewayResult<TableInfo> {
        let (schema, _) = Self::canned_result();
        Ok(TableInfo { name: table, schema })
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
        let instance = Arc::new(FakeInstance::new());
        let server = PgServer::new(instance.clone(), Arc::new(TrustAuthenticator::new()));
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
