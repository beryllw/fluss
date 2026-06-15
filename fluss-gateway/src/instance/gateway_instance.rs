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

//! Concrete [`GatewayInstance`] implementation (回指 P1 facade / P2 session model).
//!
//! Composes the Phase 1 services behind the protocol-agnostic facade:
//! - session lifecycle (open/close/get/alter) -> [`SessionManager`] (P2);
//! - direct write + read-only metadata -> [`BackendFacade`] (P6.2/P6.3), the
//!   only path that actually reaches Fluss this task;
//! - operation cancel/status -> the owning session's `OperationManager` (P2.10),
//!   resolved through a small op->session index.
//!
//! SQL `describe_sql` / `execute_sql` delegate to the [`SqlGatewayService`]
//! (sql/gateway_service, design P3): per-session `SessionContext` build/rebuild +
//! DataFusion planning/execution. `execute_sql` also records the produced
//! operation in `op_index` so cancel/status route to the owning session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::BackendFacade;
use crate::error::{GatewayError, GatewayResult};
use crate::instance::GatewayInstance;
use crate::session::manager::SessionManager;
use crate::sql::environment::SqlEnvironmentRegistry;
use crate::sql::SqlGatewayService;
use crate::types::{
    CancelResult, DescribeSqlRequest, DirectReadRequest, DirectReadResult, DirectWriteRequest,
    DirectWriteResult, ExecuteSqlRequest, MetadataScope, OpenSessionRequest, OperationId,
    OperationStatusSnapshot, SessionId, SessionMutation, SessionSnapshot, SqlDescription,
    SqlExecution, TableInfo, TableRef,
};

/// Phase 1 concrete gateway facade.
///
/// Holds the session manager and the direct-path backend. It is constructed once
/// per gateway process and shared (`Arc<dyn GatewayInstance>`) across the
/// PostgreSQL and REST frontends — the facade is the single capability surface
/// both protocols depend on.
pub struct GatewayInstanceImpl {
    sessions: Arc<SessionManager>,
    backend: Arc<dyn BackendFacade>,
    /// SQL execution orchestration (P3): per-session context build/rebuild +
    /// DataFusion planning/execution. Shares the same `SessionManager` so the
    /// operations it registers live on the very sessions this facade owns.
    sql: SqlGatewayService,
    /// op -> owning session, so `cancel_operation` / `get_operation_status` can
    /// route to the right `OperationManager` without scanning every session.
    /// `execute_sql` populates this for each Query it produces.
    op_index: Mutex<HashMap<OperationId, SessionId>>,
}

impl GatewayInstanceImpl {
    /// Construct with the shared SQL environment registry (selects the per-session
    /// SQL environment provider, e.g. `"postgres"`).
    pub fn new(
        sessions: Arc<SessionManager>,
        backend: Arc<dyn BackendFacade>,
        sql_environments: Arc<SqlEnvironmentRegistry>,
    ) -> Self {
        let sql = SqlGatewayService::new(Arc::clone(&sessions), sql_environments);
        Self {
            sessions,
            backend,
            sql,
            op_index: Mutex::new(HashMap::new()),
        }
    }

    pub fn sessions(&self) -> &Arc<SessionManager> {
        &self.sessions
    }

    pub fn backend(&self) -> &Arc<dyn BackendFacade> {
        &self.backend
    }
}

#[async_trait]
impl GatewayInstance for GatewayInstanceImpl {
    // --- Session: delegate to SessionManager (P2) ---

    async fn open_session(&self, req: OpenSessionRequest) -> GatewayResult<SessionSnapshot> {
        let session = self.sessions.open(req)?;
        Ok(session.snapshot())
    }

    async fn close_session(&self, session_id: SessionId) -> GatewayResult<()> {
        self.sessions.close(&session_id)
    }

    async fn alter_session(
        &self,
        session_id: SessionId,
        mutation: SessionMutation,
    ) -> GatewayResult<SessionSnapshot> {
        let session = self.sessions.get(&session_id)?;
        // ① update vars, ② classify effect, ③ act on the live context (P2.4).
        // The dirty/rebuild bookkeeping lives inside the session; the next query
        // rebuilds lazily.
        session.apply_mutation(&mutation);
        Ok(session.snapshot())
    }

    async fn get_session(&self, session_id: SessionId) -> GatewayResult<SessionSnapshot> {
        let session = self.sessions.get(&session_id)?;
        Ok(session.snapshot())
    }

    // --- SQL: delegate to the SQL gateway service (P3) ---

    async fn describe_sql(&self, req: DescribeSqlRequest) -> GatewayResult<SqlDescription> {
        self.sql.describe_sql(req).await
    }

    async fn execute_sql(&self, req: ExecuteSqlRequest) -> GatewayResult<SqlExecution> {
        let session_id = req.session_id.clone();
        let exec = self.sql.execute_sql(req).await?;
        // Index the produced operation so cancel/status route to its owning session.
        let op_id = match &exec {
            SqlExecution::Query { operation_id, .. } => operation_id.clone(),
            SqlExecution::Command { operation_id, .. } => operation_id.clone(),
        };
        self.op_index.lock().unwrap().insert(op_id, session_id);
        Ok(exec)
    }

    // --- Operation: route to the owning session's OperationManager (P2.10) ---

    async fn cancel_operation(&self, op_id: OperationId) -> GatewayResult<CancelResult> {
        let session_id = self.op_index.lock().unwrap().get(&op_id).cloned();
        match session_id {
            None => Ok(CancelResult::NotFound),
            Some(sid) => {
                // The session may have been closed/reaped out from under the op.
                let session = self.sessions.get(&sid)?;
                Ok(session.operation_manager().cancel(&op_id))
            }
        }
    }

    async fn get_operation_status(
        &self,
        op_id: OperationId,
    ) -> GatewayResult<OperationStatusSnapshot> {
        let session_id = self
            .op_index
            .lock()
            .unwrap()
            .get(&op_id)
            .cloned()
            .ok_or_else(|| GatewayError::OperationNotFound(op_id.0.clone()))?;
        let session = self.sessions.get(&session_id)?;
        session
            .operation_manager()
            .status(&op_id)
            .ok_or_else(|| GatewayError::OperationNotFound(op_id.0.clone()))
    }

    // --- Direct path: delegate to the backend (P6.2). Reads deferred (P5/§7). ---

    async fn read_direct(&self, _req: DirectReadRequest) -> GatewayResult<DirectReadResult> {
        Err(GatewayError::Unsupported(
            "direct read (lookup / scan) is deferred past Phase 1".into(),
        ))
    }

    async fn write_direct(&self, req: DirectWriteRequest) -> GatewayResult<DirectWriteResult> {
        self.backend.write(req).await
    }

    // --- Metadata: delegate to the backend's read surface (P6.3) ---

    async fn list_databases(&self, scope: MetadataScope) -> GatewayResult<Vec<String>> {
        self.backend.list_databases(&scope).await
    }

    async fn list_tables(
        &self,
        scope: MetadataScope,
        database: String,
    ) -> GatewayResult<Vec<String>> {
        // P1 facade gap closed: delegate to the database-scoped backend surface and
        // project to bare table names (the REST view returns names within a `{db}`).
        let tables = self.backend.list_tables(&scope, &database).await?;
        Ok(tables.into_iter().map(|t| t.table).collect())
    }

    async fn get_table_info(
        &self,
        scope: MetadataScope,
        table: TableRef,
    ) -> GatewayResult<TableInfo> {
        self.backend.get_table_info(&scope, &table).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tokio_util::sync::CancellationToken;

    use crate::session::manager::SessionManagerConfig;
    use crate::types::{
        ClientInfo, ClusterId, Principal, ProtocolKind, RequestExecutionContext, RequestId,
        SessionVars, SqlEnvironmentId,
    };

    /// In-memory backend recording writes, so the instance's direct-path
    /// delegation can be asserted without a cluster.
    struct RecordingBackend {
        writes: StdMutex<Vec<(TableRef, u64)>>,
        databases: Vec<String>,
    }

    impl RecordingBackend {
        fn new() -> Self {
            Self {
                writes: StdMutex::new(Vec::new()),
                databases: vec!["db".to_string()],
            }
        }
    }

    #[async_trait]
    impl BackendFacade for RecordingBackend {
        async fn write(
            &self,
            request: DirectWriteRequest,
        ) -> GatewayResult<DirectWriteResult> {
            let (table, n) = match request {
                DirectWriteRequest::KvUpsert { table, rows, .. } => (table, rows.num_rows() as u64),
                DirectWriteRequest::LogAppend { table, rows, .. } => (table, rows.num_rows() as u64),
                DirectWriteRequest::KvDelete { table, keys, .. } => (table, keys.num_rows() as u64),
            };
            self.writes.lock().unwrap().push((table, n));
            Ok(DirectWriteResult { rows_written: n })
        }

        async fn list_databases(&self, _scope: &MetadataScope) -> GatewayResult<Vec<String>> {
            Ok(self.databases.clone())
        }

        async fn list_tables(
            &self,
            _scope: &MetadataScope,
            _database: &str,
        ) -> GatewayResult<Vec<TableRef>> {
            Ok(vec![])
        }

        async fn get_table_info(
            &self,
            _scope: &MetadataScope,
            table: &TableRef,
        ) -> GatewayResult<TableInfo> {
            if table.table == "ghost" {
                return Err(GatewayError::TableNotFound {
                    database: table.database.clone(),
                    table: table.table.clone(),
                });
            }
            Ok(TableInfo {
                name: table.clone(),
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            })
        }
    }

    fn instance() -> GatewayInstanceImpl {
        use crate::sql::environment::{PgSqlEnvironmentProvider, SqlEnvironmentRegistry};
        let mgr = Arc::new(SessionManager::new(SessionManagerConfig::default()));
        let mut reg = SqlEnvironmentRegistry::new();
        reg.register(
            SqlEnvironmentId("postgres".into()),
            Arc::new(PgSqlEnvironmentProvider::with_stubs()),
        );
        GatewayInstanceImpl::new(mgr, Arc::new(RecordingBackend::new()), Arc::new(reg))
    }

    fn open_req() -> OpenSessionRequest {
        OpenSessionRequest {
            principal: Principal { name: "alice".into() },
            cluster: ClusterId("default".into()),
            sql_environment: Some(SqlEnvironmentId("postgres".into())),
            initial_vars: SessionVars::default(),
            client_info: ClientInfo {
                protocol: ProtocolKind::Postgres,
                peer_addr: None,
            },
        }
    }

    fn scope() -> MetadataScope {
        MetadataScope {
            principal: Principal { name: "alice".into() },
            cluster: ClusterId("default".into()),
        }
    }

    fn ctx() -> RequestExecutionContext {
        RequestExecutionContext {
            principal: Principal { name: "alice".into() },
            cluster: ClusterId("default".into()),
            request_id: RequestId("r1".into()),
            deadline: None,
            cancel: CancellationToken::new(),
        }
    }

    fn rows(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>()))],
        )
        .unwrap()
    }

    // Session lifecycle round-trips through the SessionManager (P2).
    #[tokio::test]
    async fn open_get_alter_close_session() {
        let inst = instance();
        let snap = inst.open_session(open_req()).await.unwrap();
        let id = snap.id.clone();

        let got = inst.get_session(id.clone()).await.unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.principal.name, "alice");

        // alter applies the mutation and returns the updated snapshot.
        let altered = inst
            .alter_session(id.clone(), SessionMutation::SetCurrentSchema(Some("public".into())))
            .await
            .unwrap();
        assert_eq!(altered.vars.current_schema.as_deref(), Some("public"));

        inst.close_session(id.clone()).await.unwrap();
        assert!(matches!(
            inst.get_session(id).await,
            Err(GatewayError::SessionNotFound(_))
        ));
    }

    // write_direct delegates to the backend.
    #[tokio::test]
    async fn write_direct_delegates_to_backend() {
        let inst = instance();
        let res = inst
            .write_direct(DirectWriteRequest::KvUpsert {
                context: ctx(),
                table: TableRef { database: "db".into(), table: "t".into() },
                rows: rows(4),
            })
            .await
            .unwrap();
        assert_eq!(res.rows_written, 4);
    }

    // metadata delegates to the backend; a missing table maps to a domain error.
    #[tokio::test]
    async fn metadata_delegates_to_backend() {
        let inst = instance();
        let dbs = inst.list_databases(scope()).await.unwrap();
        assert_eq!(dbs, vec!["db".to_string()]);

        let info = inst
            .get_table_info(scope(), TableRef { database: "db".into(), table: "t".into() })
            .await
            .unwrap();
        assert_eq!(info.name.table, "t");

        let err = inst
            .get_table_info(scope(), TableRef { database: "db".into(), table: "ghost".into() })
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::TableNotFound { .. }));
    }

    // SQL path now executes through the SQL gateway service; the produced
    // operation is indexed so cancel/status route to its owning session (P3).
    #[tokio::test]
    async fn sql_execute_runs_and_indexes_operation() {
        use futures::TryStreamExt;

        let inst = instance();
        let snap = inst.open_session(open_req()).await.unwrap();

        let desc = inst
            .describe_sql(DescribeSqlRequest {
                session_id: snap.id.clone(),
                statement: "SELECT 1 AS one".into(),
            })
            .await
            .unwrap();
        assert_eq!(desc.schema.fields().len(), 1);

        let exec = inst
            .execute_sql(ExecuteSqlRequest {
                session_id: snap.id.clone(),
                statement: "SELECT 1 AS one".into(),
                params: None,
                options: Default::default(),
            })
            .await
            .unwrap();
        let op_id = match exec {
            SqlExecution::Query {
                operation_id,
                stream,
                ..
            } => {
                let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
                operation_id
            }
            _ => panic!("SELECT must be a Query"),
        };

        // Status routes through op_index to the owning session.
        let st = inst.get_operation_status(op_id).await.unwrap();
        assert_eq!(st.state, crate::types::OperationState::Finished);
    }

    // list_tables now delegates to the backend with the database arg.
    #[tokio::test]
    async fn list_tables_delegates_with_database() {
        let inst = instance();
        let tables = inst.list_tables(scope(), "db".into()).await.unwrap();
        assert!(tables.is_empty(), "RecordingBackend returns no tables");
    }

    // Unknown operation: cancel -> NotFound, status -> OperationNotFound.
    #[tokio::test]
    async fn unknown_operation_cancel_and_status() {
        let inst = instance();
        let c = inst.cancel_operation(OperationId("nope".into())).await.unwrap();
        assert_eq!(c, CancelResult::NotFound);
        let s = inst.get_operation_status(OperationId("nope".into())).await;
        assert!(matches!(s, Err(GatewayError::OperationNotFound(_))));
    }

    // read_direct is deferred past Phase 1.
    #[tokio::test]
    async fn read_direct_is_unsupported() {
        let inst = instance();
        let r = inst
            .read_direct(DirectReadRequest::LogScan {
                context: ctx(),
                table: TableRef { database: "db".into(), table: "t".into() },
                limit: 10,
            })
            .await;
        assert!(matches!(r, Err(GatewayError::Unsupported(_))));
    }
}
