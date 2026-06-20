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

//! SQL execution orchestration.
//!
//! Drives, for a `(session, statement)` pair: per-session `SessionContext`
//! build/rebuild through the session seam (`GatewaySession::context_for_query`)
//! wired to the [`EnvironmentContextBuilder`] (registry + `PgSqlEnvironmentProvider`),
//! then DataFusion planning/execution on that context, then mapping the result to
//! a [`SqlExecution`]:
//!
//! - a result-bearing plan (`SELECT`, `VALUES`, …) → [`SqlExecution::Query`]: a
//!   query-scoped [`Operation`] is registered on the session's `OperationManager`
//!   and the Arrow-native stream is wrapped so that draining it drives the
//!   operation state (`Running` → `Finished` / `Failed`) and observes cooperative
//!   cancel through the operation's [`CancellationToken`];
//! - a side-effecting / no-result plan (`SET`, `BEGIN`, DML/DDL) →
//!   [`SqlExecution::Command`] (the PG SQL path is read-only).
//!
//! `describe_sql` plans the statement WITHOUT executing it and returns the result
//! schema plus inferred positional parameter types.
//!
//! The service owns no per-session state; it is constructed once with the shared
//! [`SqlEnvironmentRegistry`] and reads the authoritative session state through
//! the `SessionManager`. Fluss is reached only through the environment provider's
//! catalog seam — there is no direct fluss-datafusion dependency in this module.
//! Design: `design/sql-path.md` (fixed assembly order) and `DESIGN.md`
//! (integration model); operation/stream/cancel semantics: `design/core-session.md`.

use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::ParamValues;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::{GatewayError, GatewayResult};
use crate::session::manager::SessionManager;
use crate::session::operation::{Operation, OperationManager};
use crate::session::GatewaySession;
use crate::sql::environment::bridge::EnvironmentContextBuilder;
use crate::sql::environment::registry::SqlEnvironmentRegistry;
use crate::types::{
    DescribeSqlRequest, ExecuteSqlRequest, OperationId, SessionMutation, SessionMutationEffect,
    SqlDescription, SqlExecution,
};

/// A freshly registered operation whose state the caller must drive: the id (to
/// surface on the `SqlExecution`) and the cancel token to thread into the stream.
struct RegisteredOp {
    id: OperationId,
    cancel: CancellationToken,
}

/// SQL execution orchestrator. Holds only shared, read-only
/// collaborators; per-session state lives in [`GatewaySession`].
pub struct SqlGatewayService {
    sessions: Arc<SessionManager>,
    registry: Arc<SqlEnvironmentRegistry>,
}

impl SqlGatewayService {
    pub fn new(sessions: Arc<SessionManager>, registry: Arc<SqlEnvironmentRegistry>) -> Self {
        Self { sessions, registry }
    }

    /// Apply one session mutation, including any live SessionContext update the
    /// selected SQL environment can perform in-place.
    pub async fn apply_session_mutation(
        &self,
        session: &Arc<GatewaySession>,
        mutation: &SessionMutation,
    ) -> GatewayResult<SessionMutationEffect> {
        let effect = session.apply_mutation(mutation);
        if effect != SessionMutationEffect::ApplyToExistingContext {
            return Ok(effect);
        }

        let Some(ctx) = session.current_context().await else {
            return Ok(effect);
        };
        let Some(sql_environment) = session.sql_environment.as_ref() else {
            return Ok(effect);
        };
        let provider = self.registry.get(sql_environment)?;
        if let Err(err) = provider.apply_session_mutation(session, &ctx, mutation).await {
            session.mark_context_dirty();
            return Err(err);
        }
        Ok(effect)
    }

    /// Build (or rebuild, if dirty) the session's `SessionContext` through the
    /// session seam wired to the environment provider. The returned `Arc` is the
    /// session's current context; an in-flight rebuild leaves any older context
    /// alive for operations still holding it.
    async fn context_for(
        &self,
        session: &Arc<GatewaySession>,
    ) -> GatewayResult<Arc<SessionContext>> {
        let builder = EnvironmentContextBuilder::new(Arc::clone(&self.registry), Arc::clone(session))?;
        session.context_for_query(&builder).await
    }

    /// Plan-only: build the logical plan for `req.statement` on the session's
    /// context and report the result schema + inferred positional parameter types.
    pub async fn describe_sql(&self, req: DescribeSqlRequest) -> GatewayResult<SqlDescription> {
        let session = self.sessions.get(&req.session_id)?;
        session.touch();
        let ctx = self.context_for(&session).await?;

        let plan = ctx
            .state()
            .create_logical_plan(&req.statement)
            .await
            .map_err(map_plan_err)?;

        let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        let param_types = ordered_param_types(&plan)?;
        Ok(SqlDescription { schema, param_types })
    }

    /// Plan + execute `req.statement` on the session's context, applying any bound
    /// parameters, and map the outcome to a [`SqlExecution`]. Result-bearing plans
    /// register an [`Operation`] and stream Arrow-native batches; side-effecting /
    /// no-result plans return [`SqlExecution::Command`].
    pub async fn execute_sql(&self, req: ExecuteSqlRequest) -> GatewayResult<SqlExecution> {
        let session = self.sessions.get(&req.session_id)?;
        session.touch();
        let ctx = self.context_for(&session).await?;

        let mut df = ctx.sql(&req.statement).await.map_err(map_plan_err)?;
        if let Some(params) = req.params {
            df = apply_params(df, params)?;
        }

        let plan = df.logical_plan().clone();
        let is_command = is_command_plan(&plan);

        // Register the operation BEFORE the first poll so an out-of-band cancel
        // arriving between registration and stream start is observed.
        let op = register_operation(session.operation_manager(), &req.statement);

        if is_command {
            // No result set: execute for side effects, then close the operation.
            // (PG is read-only at the adapter; this is the neutral shape.)
            let mgr = session.operation_manager();
            mgr.with_operation(&op.id, |o| o.mark_running());
            match df.collect().await {
                Ok(_) => {
                    mgr.with_operation(&op.id, |o| o.mark_finished());
                    Ok(SqlExecution::Command {
                        operation_id: op.id,
                        affected_rows: 0,
                    })
                }
                Err(e) => {
                    let msg = e.to_string();
                    mgr.with_operation(&op.id, |o| o.mark_failed(msg.clone()));
                    Err(map_exec_err(e))
                }
            }
        } else {
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let stream = match df.execute_stream().await {
                Ok(stream) => stream,
                Err(e) => {
                    let msg = e.to_string();
                    session.operation_manager().with_operation(&op.id, |o| {
                        o.mark_running();
                        o.mark_failed(msg);
                    });
                    return Err(map_exec_err(e));
                }
            };
            let tracked = tracked_stream(
                stream,
                schema.clone(),
                Arc::clone(&session),
                op.id.clone(),
                op.cancel,
            );
            Ok(SqlExecution::Query {
                operation_id: op.id,
                schema,
                stream: tracked,
            })
        }
    }
}

/// Register a fresh `Pending` operation and return its id + cancel token. The
/// operation's own token is used so `OperationManager::cancel` fires it.
fn register_operation(mgr: &OperationManager, statement: &str) -> RegisteredOp {
    let id = OperationId(format!("op-{}", uuid_like()));
    let op = Operation::new(id.clone(), summarize(statement));
    let cancel = mgr.register(op);
    RegisteredOp { id, cancel }
}

/// A small, dependency-free unique-ish token for operation ids. Operation ids are
/// internal routing keys (not protocol-visible), so a monotonic-plus-time token is
/// sufficient and avoids pulling in a uuid dependency.
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{t:x}-{n:x}")
}

/// Truncate a statement to a short, log-friendly operation summary.
fn summarize(statement: &str) -> String {
    const MAX: usize = 120;
    let trimmed = statement.trim();
    if trimmed.len() <= MAX {
        trimmed.to_string()
    } else {
        let mut s: String = trimmed.chars().take(MAX).collect();
        s.push('…');
        s
    }
}

/// Apply bound positional parameters to a planned `DataFrame`.
fn apply_params(
    df: datafusion::dataframe::DataFrame,
    params: ParamValues,
) -> GatewayResult<datafusion::dataframe::DataFrame> {
    df.with_param_values(params).map_err(|e| {
        GatewayError::InvalidArgument(format!("bind parameters rejected: {e}"))
    })
}

/// Side-effecting / no-result plan classification (→ `Command`). Everything else
/// is result-bearing (→ `Query`).
///
/// `SET` / `SHOW` / transaction control may land in `Statement`, or — because
/// DataFusion's `ctx.sql` handles `SET`/`RESET` eagerly — collapse to an
/// `EmptyRelation` with an empty schema. Both are treated as `Command`. A
/// result-bearing query that happens to be statically empty still carries its
/// projected output columns, so a non-empty schema keeps it on the `Query` path.
fn is_command_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Statement(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_) => true,
        LogicalPlan::EmptyRelation(empty) => empty.schema.fields().is_empty(),
        _ => false,
    }
}

/// Extract `$1..$N` parameter types in positional order. Placeholder ids are
/// `$<n>` strings; we sort numerically so `param_types[i]` is the `$(i+1)` type.
/// An unresolved placeholder type defaults to `Utf8` (the protocol boundary can
/// still send a usable OID); a non-`$<n>` placeholder is rejected.
fn ordered_param_types(plan: &LogicalPlan) -> GatewayResult<Vec<DataType>> {
    let map = plan
        .get_parameter_types()
        .map_err(|e| GatewayError::InvalidArgument(format!("parameter inference failed: {e}")))?;
    if map.is_empty() {
        return Ok(Vec::new());
    }
    let mut indexed: Vec<(usize, DataType)> = Vec::with_capacity(map.len());
    for (id, ty) in map {
        let n: usize = id
            .strip_prefix('$')
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                GatewayError::InvalidArgument(format!("unsupported placeholder '{id}'"))
            })?;
        indexed.push((n, ty.unwrap_or(DataType::Utf8)));
    }
    indexed.sort_by_key(|(n, _)| *n);
    Ok(indexed.into_iter().map(|(_, ty)| ty).collect())
}

/// Wrap an Arrow-native result stream so draining it drives the owning operation's
/// state and observes cooperative cancel:
///
/// - first batch polled → `Running`;
/// - normal EOF → `Finished`;
/// - execution error → `Failed` (and the error surfaces to the consumer);
/// - cancel token fired (cancel request / session close) → stop yielding batches
///   and mark `Cancelled`. The cancel is cooperative / best-effort: an already
///   in-flight `poll` may complete, but no further batches are produced.
///
/// The session `Arc` is held so the `OperationManager` outlives the stream even if
/// the session is reaped from the manager registry mid-drain.
fn tracked_stream(
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    session: Arc<GatewaySession>,
    op_id: OperationId,
    cancel: CancellationToken,
) -> SendableRecordBatchStream {
    // State carried across polls: the inner stream, whether we've marked Running,
    // and whether we've already reached a terminal transition.
    struct State {
        inner: SendableRecordBatchStream,
        session: Arc<GatewaySession>,
        op_id: OperationId,
        cancel: CancellationToken,
        started: bool,
        done: bool,
    }

    let state = State {
        inner,
        session,
        op_id,
        cancel,
        started: false,
        done: false,
    };

    let body = futures::stream::unfold(state, move |mut st| async move {
        if st.done {
            return None;
        }
        if !st.started {
            st.session
                .operation_manager()
                .with_operation(&st.op_id, |o| o.mark_running());
            st.started = true;
        }

        tokio::select! {
            biased;
            // Cooperative cancel: stop producing batches and mark cancelled.
            _ = st.cancel.cancelled() => {
                st.session
                    .operation_manager()
                    .with_operation(&st.op_id, |o| { o.request_cancel(); o.mark_cancelled(); });
                // Returning None ends the stream; no further poll occurs.
                None
            }
            next = st.inner.next() => {
                match next {
                    Some(Ok(batch)) => Some((Ok(batch), st)),
                    Some(Err(e)) => {
                        let msg = e.to_string();
                        st.session
                            .operation_manager()
                            .with_operation(&st.op_id, |o| o.mark_failed(msg));
                        st.done = true;
                        Some((Err(e), st))
                    }
                    None => {
                        st.session
                            .operation_manager()
                            .with_operation(&st.op_id, |o| o.mark_finished());
                        // Normal EOF: end the stream.
                        None
                    }
                }
            }
        }
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, body))
}

/// Map a DataFusion planning error (parse / analyze / catalog resolution) into a
/// domain error at this SQL-service boundary (contract D3). Planning failures are
/// caller-facing (bad SQL / unresolved relation), so they surface as
/// `InvalidArgument`; the protocol boundary maps that to its own error code.
fn map_plan_err(e: datafusion::error::DataFusionError) -> GatewayError {
    GatewayError::InvalidArgument(format!("planning failed: {e}"))
}

/// Map a DataFusion execution error into a domain error (contract D3).
fn map_exec_err(e: datafusion::error::DataFusionError) -> GatewayError {
    GatewayError::Backend(format!("query execution failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::any::Any;
    use std::sync::Arc as StdArc;

    use arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::catalog::Session as CatalogSession;
    use futures::TryStreamExt;

    use crate::session::manager::SessionManagerConfig;
    use crate::sql::environment::{PgSqlEnvironmentProvider, SqlEnvironmentRegistry};
    use crate::types::{
        ClientInfo, ClusterId, OperationState, Principal, ProtocolKind, SessionVars,
        SqlEnvironmentId, SqlExecutionOptions,
    };

    /// A service whose sessions use the stub Fluss catalog + real pg_catalog base,
    /// so `ctx.sql` can execute constant SELECTs with no live cluster.
    fn service() -> (SqlGatewayService, Arc<SessionManager>) {
        let sessions = Arc::new(SessionManager::new(SessionManagerConfig::default()));
        let mut reg = SqlEnvironmentRegistry::new();
        reg.register(
            SqlEnvironmentId("postgres".into()),
            Arc::new(PgSqlEnvironmentProvider::with_stubs()),
        );
        let svc = SqlGatewayService::new(Arc::clone(&sessions), Arc::new(reg));
        (svc, sessions)
    }

    fn open(sessions: &SessionManager) -> crate::types::SessionId {
        let s = sessions
            .open(crate::types::OpenSessionRequest {
                principal: Principal { name: "alice".into() },
                cluster: ClusterId("default".into()),
                sql_environment: Some(SqlEnvironmentId("postgres".into())),
                initial_vars: SessionVars::default(),
                client_info: ClientInfo {
                    protocol: ProtocolKind::Postgres,
                    peer_addr: None,
                },
            })
            .unwrap();
        s.id.clone()
    }

    async fn drain(exec: SqlExecution) -> (OperationId, Vec<RecordBatch>) {
        match exec {
            SqlExecution::Query {
                operation_id,
                stream,
                ..
            } => {
                let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
                (operation_id, batches)
            }
            SqlExecution::Command { operation_id, .. } => (operation_id, Vec::new()),
        }
    }

    #[derive(Debug)]
    struct FailingScanTable {
        schema: SchemaRef,
    }

    #[async_trait::async_trait]
    impl TableProvider for FailingScanTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            StdArc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn CatalogSession,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<StdArc<dyn ExecutionPlan>> {
            Err(datafusion::error::DataFusionError::Execution(
                "synthetic execute_stream setup failure".into(),
            ))
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
        }
    }

    // Full orchestration loop: build ctx -> plan -> Query stream -> collect rows ->
    // operation reaches Finished.
    #[tokio::test]
    async fn select_constant_executes_and_finishes() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let exec = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid.clone(),
                statement: "SELECT 1 AS one".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();

        let op_id = match &exec {
            SqlExecution::Query { operation_id, .. } => operation_id.clone(),
            _ => panic!("SELECT must be a Query"),
        };
        let (_, batches) = drain(exec).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);

        // The owning session's operation reached Finished after the stream drained.
        let session = sessions.get(&sid).unwrap();
        let status = session.operation_manager().status(&op_id).unwrap();
        assert_eq!(status.state, OperationState::Finished);
    }

    // The operation is registered (visible) before the stream is drained, and is
    // initially non-terminal (Pending/Running), proving cancel can be routed.
    #[tokio::test]
    async fn operation_registered_before_drain() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let exec = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid.clone(),
                statement: "SELECT 42".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();
        let op_id = match &exec {
            SqlExecution::Query { operation_id, .. } => operation_id.clone(),
            _ => panic!("expected Query"),
        };
        let session = sessions.get(&sid).unwrap();
        let st = session.operation_manager().status(&op_id).unwrap();
        assert!(matches!(
            st.state,
            OperationState::Pending | OperationState::Running
        ));
        let _ = drain(exec).await;
    }

    // Bound parameters are applied to the plan before execution.
    #[tokio::test]
    async fn bound_parameter_is_applied() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let exec = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid,
                statement: "SELECT $1 AS p".into(),
                params: Some(ParamValues::List(vec![ScalarValue::Int64(Some(7)).into()])),
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();
        let (_, batches) = drain(exec).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    // SET is a side-effecting statement -> Command, no result set.
    #[tokio::test]
    async fn set_statement_maps_to_command() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let exec = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid,
                statement: "SET datafusion.execution.batch_size = 16".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();
        assert!(matches!(exec, SqlExecution::Command { .. }));
    }

    // describe_sql plans without executing and returns the result schema.
    #[tokio::test]
    async fn describe_returns_schema() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let desc = svc
            .describe_sql(DescribeSqlRequest {
                session_id: sid,
                statement: "SELECT 1 AS one".into(),
            })
            .await
            .unwrap();
        assert_eq!(desc.schema.fields().len(), 1);
        assert_eq!(desc.schema.field(0).name(), "one");
        assert!(desc.param_types.is_empty());
    }

    // describe_sql infers a positional parameter type for `$1` where DataFusion
    // can resolve it (through arithmetic, here `$1 + 1` -> Int64).
    #[tokio::test]
    async fn describe_infers_parameter_type() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let desc = svc
            .describe_sql(DescribeSqlRequest {
                session_id: sid,
                statement: "SELECT $1 + 1 AS p".into(),
            })
            .await
            .unwrap();
        assert_eq!(desc.param_types.len(), 1);
        assert_eq!(desc.param_types[0], DataType::Int64);
    }

    // An unresolvable placeholder type defaults to Utf8 (the boundary still gets a
    // usable OID); param count is still reported.
    #[tokio::test]
    async fn describe_unresolved_param_defaults_to_utf8() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let desc = svc
            .describe_sql(DescribeSqlRequest {
                session_id: sid,
                statement: "SELECT $1 AS p".into(),
            })
            .await
            .unwrap();
        assert_eq!(desc.param_types, vec![DataType::Utf8]);
    }

    /// `SqlExecution` carries a stream and is not `Debug`, so collapse a result to
    /// its error for assertions without requiring `unwrap_err`.
    fn into_err(r: GatewayResult<SqlExecution>) -> GatewayError {
        match r {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        }
    }

    // Unknown session -> SessionNotFound (not a panic / silent build).
    #[tokio::test]
    async fn unknown_session_errors() {
        let (svc, _sessions) = service();
        let err = into_err(
            svc.execute_sql(ExecuteSqlRequest {
                session_id: crate::types::SessionId("ghost".into()),
                statement: "SELECT 1".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await,
        );
        assert!(matches!(err, GatewayError::SessionNotFound(_)));
    }

    // A planning error (syntactic / unresolved) maps to a domain InvalidArgument.
    #[tokio::test]
    async fn planning_error_maps_to_domain() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let err = into_err(
            svc.execute_sql(ExecuteSqlRequest {
                session_id: sid,
                statement: "SELECT * FROM no_such_table_xyz".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await,
        );
        assert!(matches!(err, GatewayError::InvalidArgument(_)));
    }

    // Cooperative cancel: a cancel request fired before draining stops the stream
    // and the operation reaches Cancelled (best-effort).
    #[tokio::test]
    async fn cancel_before_drain_stops_stream_and_cancels_op() {
        let (svc, sessions) = service();
        let sid = open(&sessions);
        let exec = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid.clone(),
                statement: "SELECT 1".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();
        let (op_id, stream) = match exec {
            SqlExecution::Query {
                operation_id,
                stream,
                ..
            } => (operation_id, stream),
            _ => panic!("expected Query"),
        };

        // Route a cancel through the OperationManager (as Instance.cancel_operation
        // would), THEN drain. The tracked stream must observe the token and stop.
        let session = sessions.get(&sid).unwrap();
        assert_eq!(
            session.operation_manager().cancel(&op_id),
            crate::types::CancelResult::Accepted
        );

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert!(batches.is_empty(), "cancelled stream yields no batches");
        let st = session.operation_manager().status(&op_id).unwrap();
        assert_eq!(st.state, OperationState::Cancelled);
    }

    // A rebuild between two queries (dirty flag) does not break execution; the
    // second query rebuilds the context and still runs.
    #[tokio::test]
    async fn rebuild_between_queries_still_executes() {
        let (svc, sessions) = service();
        let sid = open(&sessions);

        let e1 = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid.clone(),
                statement: "SELECT 1".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();
        let _ = drain(e1).await;
        let gen1 = sessions.get(&sid).unwrap().generation();

        // Force a rebuild before the next query.
        let session = sessions.get(&sid).unwrap();
        session.apply_mutation(&crate::types::SessionMutation::SetCurrentSchema(Some(
            "public".into(),
        )));
        assert!(session.is_dirty());

        let e2 = svc
            .execute_sql(ExecuteSqlRequest {
                session_id: sid.clone(),
                statement: "SELECT 2".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await
            .unwrap();
        let (_, batches) = drain(e2).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let gen2 = sessions.get(&sid).unwrap().generation();
        assert!(gen2 > gen1, "dirty forced a context rebuild");
    }

    // If query planning succeeds but execute_stream construction fails, the
    // registered operation must transition to Failed rather than staying Pending.
    #[tokio::test]
    async fn execute_stream_failure_marks_operation_failed() {
        let sessions = Arc::new(SessionManager::new(SessionManagerConfig::default()));
        let sid = open(&sessions);
        let session = sessions.get(&sid).unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let ctx = SessionContext::new();
        ctx.register_table(
            "failing_table",
            StdArc::new(FailingScanTable { schema: StdArc::clone(&schema) }),
        )
        .unwrap();
        session.replace_context_for_test(StdArc::new(ctx)).await;

        let reg = Arc::new(SqlEnvironmentRegistry::new());
        let svc = SqlGatewayService::new(Arc::clone(&sessions), reg);
        let err = into_err(
            svc.execute_sql(ExecuteSqlRequest {
                session_id: sid.clone(),
                statement: "SELECT id FROM failing_table".into(),
                params: None,
                options: SqlExecutionOptions::default(),
            })
            .await,
        );
        assert!(matches!(err, GatewayError::Backend(_)));

        let session = sessions.get(&sid).unwrap();
        let statuses = session.operation_manager().snapshots_for_test();
        assert_eq!(statuses.len(), 1, "one operation was registered");
        assert_eq!(statuses[0].state, OperationState::Failed);
        assert!(statuses[0]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("synthetic execute_stream setup failure"));
    }
}
