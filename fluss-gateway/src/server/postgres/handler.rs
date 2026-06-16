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

//! P4 — pgwire protocol state machine (`handler`).
//!
//! Implements the three pgwire handler traits (`StartupHandler`,
//! `SimpleQueryHandler`, `ExtendedQueryHandler`) plus `CancelHandler`, bridging
//! each wire request to a call on the [`GatewayInstance`] facade. All "线上长什
//! 么样" translation (encoding, error mapping, var <-> string, cancel keys) is
//! delegated to `adapter`; statement routing is delegated to `compat`. The
//! handler holds the per-connection session/operation state and the
//! prepared-statement wire lifecycle — none of which leaks into `Instance`'s
//! Operation model. Design: `design/sql-path.md` §P4.1/§P4.4/§P4.5/§P4.6.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::stream;
use futures::{Sink, SinkExt, StreamExt};
use pgwire::api::auth::{
    save_startup_parameters_to_metadata, ServerParameterProvider, StartupHandler,
};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo as PgClientInfo, ClientPortalStore, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::error::GatewayError;
use crate::instance::GatewayInstance;
use datafusion::common::ParamValues;

use crate::types::{
    DescribeSqlRequest, ExecuteSqlRequest, SessionId, SessionMutation, SessionVarValue,
    SqlExecution, SqlExecutionOptions,
};

use super::adapter::{self, CancelRegistry};
use super::compat::{self, StatementClass};

/// Per-connection state shared between the startup / query / extended handlers.
///
/// One [`PgConnection`] exists per TCP connection (per session). It owns the
/// session id assigned at startup and the backend `pid` used for out-of-band
/// cancel. The prepared-statement / portal wire lifecycle (§P4.5) is NOT
/// duplicated here: pgwire's per-connection `MemPortalStore` (on the client's
/// `DefaultClient`) owns named/anonymous statements and portals, removes them on
/// `Close`, and is dropped when the connection ends. None of that leaks into
/// `Instance`'s Operation model — each `Execute` maps to one `execute_sql`.
pub struct PgConnection {
    instance: Arc<dyn GatewayInstance>,
    authenticator: Arc<dyn crate::auth::Authenticator>,
    cancels: CancelRegistry,
    pid: i32,
    /// Filled once startup completes.
    session: Mutex<Option<SessionId>>,
}

impl PgConnection {
    pub fn new(
        instance: Arc<dyn GatewayInstance>,
        authenticator: Arc<dyn crate::auth::Authenticator>,
        cancels: CancelRegistry,
        pid: i32,
    ) -> Self {
        Self {
            instance,
            authenticator,
            cancels,
            pid,
            session: Mutex::new(None),
        }
    }

    pub fn session_id(&self) -> Option<SessionId> {
        self.session.lock().unwrap().clone()
    }

    fn set_session(&self, id: SessionId) {
        *self.session.lock().unwrap() = Some(id);
    }
}

// ---------------------------------------------------------------------------
// server parameters
// ---------------------------------------------------------------------------

/// Minimal server-parameter set advertised at startup. UTF-8 only (§P4.2).
#[derive(Debug, Clone, Default)]
pub struct PgServerParameters;

impl ServerParameterProvider for PgServerParameters {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: PgClientInfo,
    {
        let mut m = HashMap::new();
        m.insert("server_version".to_string(), "15.0 (fluss-gateway)".to_string());
        m.insert("server_encoding".to_string(), "UTF8".to_string());
        m.insert("client_encoding".to_string(), "UTF8".to_string());
        m.insert("DateStyle".to_string(), "ISO, MDY".to_string());
        m.insert("integer_datetimes".to_string(), "on".to_string());
        Some(m)
    }
}

// ---------------------------------------------------------------------------
// startup / auth (cleartext-then-trust) (§P4.2)
// ---------------------------------------------------------------------------

#[async_trait]
impl StartupHandler for PgConnection {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: PgClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                pgwire::api::auth::protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                // Assign a backend key now so the cancel handshake (BackendKeyData)
                // and our cancel registry agree on (pid, secret).
                let secret = rand_secret();
                client.set_pid_and_secret_key(self.pid, pgwire::messages::startup::SecretKey::I32(secret));
                client.set_state(pgwire::api::PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let username = client
                    .metadata()
                    .get(pgwire::api::METADATA_USER)
                    .cloned()
                    .unwrap_or_default();
                let cred = adapter::credential_from_pg_login(&username, Some(pwd.password));
                // cleartext-then-trust: the configured authenticator decides
                // (Phase 1 = TrustAuthenticator). The protocol layer never owns
                // the auth policy; it only adapts the wire handshake to a
                // neutral Credential and maps any AuthError to a domain error.
                let principal = self
                    .authenticator
                    .authenticate(cred)
                    .await
                    .map_err(GatewayError::from)
                    .map_err(to_pg_err)?;

                let params = client.metadata().clone();
                let peer = Some(client.socket_addr().to_string());
                let req = adapter::open_session_request_from_startup(principal, &params, peer)
                    .map_err(to_pg_err)?;
                let snap = self.instance.open_session(req).await.map_err(to_pg_err)?;

                let secret = client
                    .pid_and_secret_key()
                    .1
                    .as_i32()
                    .unwrap_or_default();
                self.cancels.register(self.pid, secret, snap.id.clone());
                self.set_session(snap.id);

                pgwire::api::auth::finish_authentication(client, &PgServerParameters).await?;
            }
            _ => {}
        }
        Ok(())
    }
}


// ---------------------------------------------------------------------------
// simple query (§P4.3 routing)
// ---------------------------------------------------------------------------

#[async_trait]
impl SimpleQueryHandler for PgConnection {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: PgClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // The simple-query protocol carries no bound parameters.
        let resp = self.route_statement(query, None, FieldFormat::Text).await?;
        Ok(vec![resp])
    }
}

// ---------------------------------------------------------------------------
// extended query (§P4.4/§P4.5)
// ---------------------------------------------------------------------------

#[async_trait]
impl ExtendedQueryHandler for PgConnection {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        // The statement is kept as the raw SQL string (NoopQueryParser); the PG
        // `ParameterDescription` / `RowDescription` are produced lazily by
        // `do_describe_statement` / `do_describe_portal` via `Instance.describe_sql`,
        // not by parsing here (§P4.4).
        Arc::new(NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: PgClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Statement carries the raw SQL (NoopQueryParser keeps it as a String).
        // The portal/statement themselves live in pgwire's MemPortalStore; each
        // Execute maps to exactly one execute_sql (§P4.5).
        let sql = portal.statement.statement.clone();
        let format = result_format(portal);
        // Decode the bound `$1..$N` parameters (PG wire text/binary -> ParamValues,
        // §P4.4) so a parameterized statement executes with its actual values.
        // Resolve the expected parameter types first so a client that left the
        // Parse OIDs blank (tokio-postgres / JDBC default) still decodes binary
        // values correctly against the gateway's inferred types.
        let inferred = if portal.parameter_len() > 0 {
            self.infer_param_types(&sql).await?
        } else {
            Vec::new()
        };
        let params = adapter::decode_params(portal, &inferred)?;
        self.route_statement(&sql, params, format).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: PgClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = target.statement.clone();
        let (fields, params) = self.describe_fields(&sql, FieldFormat::Text).await?;
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: PgClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = target.statement.statement.clone();
        let format = result_format(target);
        // A portal Describe only carries a RowDescription (no parameter list).
        let (fields, _params) = self.describe_fields(&sql, format).await?;
        Ok(DescribePortalResponse::new(fields))
    }
}

// ---------------------------------------------------------------------------
// routing + execution (shared by simple and extended paths)
// ---------------------------------------------------------------------------

impl PgConnection {
    fn require_session(&self) -> Result<SessionId, GatewayError> {
        self.session_id()
            .ok_or_else(|| GatewayError::SessionNotFound("no session for connection".into()))
    }

    /// Describe a statement's result columns and parameter types, mapping both to
    /// PG wire form: the `RowDescription` fields and the `ParameterDescription`
    /// type OIDs (§P4.4). SET/SHOW/transaction-control statements have no result
    /// columns and no parameters.
    async fn describe_fields(
        &self,
        sql: &str,
        format: FieldFormat,
    ) -> PgWireResult<(Vec<FieldInfo>, Vec<Type>)> {
        match compat::classify(sql) {
            StatementClass::Passthrough => {
                let session_id = self.require_session().map_err(to_pg_err)?;
                let desc = self
                    .instance
                    .describe_sql(DescribeSqlRequest {
                        session_id,
                        statement: compat::rewrite_introspection(sql),
                    })
                    .await
                    .map_err(to_pg_err)?;
                let fields = adapter::row_description(desc.schema.as_ref(), format)?;
                let params = adapter::param_types_to_pg(&desc.param_types)?;
                Ok((fields, params))
            }
            StatementClass::Show { .. } => Ok((
                vec![FieldInfo::new(
                    "value".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    format,
                )],
                vec![],
            )),
            _ => Ok((vec![], vec![])),
        }
    }

    /// Resolve the Arrow types of a statement's `$1..$N` placeholders via
    /// `Instance.describe_sql` (§P4.4). Used to drive bind-parameter decoding when
    /// the client did not declare parameter OIDs in `Parse`. Non-passthrough
    /// statements (SET/SHOW/transaction control) have no parameters.
    async fn infer_param_types(
        &self,
        sql: &str,
    ) -> PgWireResult<Vec<arrow::datatypes::DataType>> {
        if !matches!(compat::classify(sql), StatementClass::Passthrough) {
            return Ok(Vec::new());
        }
        let session_id = self.require_session().map_err(to_pg_err)?;
        let desc = self
            .instance
            .describe_sql(DescribeSqlRequest {
                session_id,
                statement: compat::rewrite_introspection(sql),
            })
            .await
            .map_err(to_pg_err)?;
        Ok(desc.param_types)
    }

    /// Route one statement to the right wire response per the compat table.
    /// `params` carries decoded bind values for the extended path (`None` for the
    /// parameterless simple-query path and for command statements).
    async fn route_statement(
        &self,
        sql: &str,
        params: Option<ParamValues>,
        result_format: FieldFormat,
    ) -> PgWireResult<Response> {
        match compat::classify(sql) {
            StatementClass::Write => Err(to_pg_err(adapter::write_rejected_error())),

            StatementClass::Begin
            | StatementClass::Commit
            | StatementClass::Rollback
            | StatementClass::Discard => {
                let class = compat::classify(sql);
                if matches!(class, StatementClass::Discard) {
                    // DISCARD ALL resets ALL session vars to the connection's
                    // initial values and forces a rebuild before the next query
                    // (§P4.3), not just search_path.
                    let session_id = self.require_session().map_err(to_pg_err)?;
                    self.instance
                        .alter_session(session_id, SessionMutation::ResetAll)
                        .await
                        .map_err(to_pg_err)?;
                }
                let tag = compat::transaction_command_tag(&class).unwrap_or("OK");
                Ok(Response::Execution(Tag::new(tag)))
            }

            StatementClass::Set { name, value } => {
                let session_id = self.require_session().map_err(to_pg_err)?;
                let mutation = set_to_mutation(&name, &value);
                self.instance
                    .alter_session(session_id, mutation)
                    .await
                    .map_err(to_pg_err)?;
                Ok(Response::Execution(Tag::new("SET")))
            }

            StatementClass::Show { name } => {
                let session_id = self.require_session().map_err(to_pg_err)?;
                let snap = self
                    .instance
                    .get_session(session_id)
                    .await
                    .map_err(to_pg_err)?;
                let value = adapter::show_var(&snap.vars, &name);
                let (fields, row) = adapter::single_text_row(&name, &value);
                let rows = stream::iter(vec![row]);
                Ok(Response::Query(QueryResponse::new(fields, rows)))
            }

            StatementClass::Passthrough => {
                let rewritten = compat::rewrite_introspection(sql);
                self.execute_select(&rewritten, params, result_format).await
            }
        }
    }

    /// Execute a passthrough SELECT and stream the Arrow result as PG DataRows.
    async fn execute_select(
        &self,
        sql: &str,
        params: Option<ParamValues>,
        result_format: FieldFormat,
    ) -> PgWireResult<Response> {
        let session_id = self.require_session().map_err(to_pg_err)?;
        let exec = self
            .instance
            .execute_sql(ExecuteSqlRequest {
                session_id,
                statement: sql.to_string(),
                params,
                options: SqlExecutionOptions::default(),
            })
            .await
            .map_err(to_pg_err)?;

        match exec {
            SqlExecution::Command { .. } => {
                // A command-shaped result reached the SELECT path; reply with a
                // neutral command tag rather than an empty row set.
                Ok(Response::Execution(Tag::new("OK")))
            }
            SqlExecution::Query {
                operation_id,
                schema,
                mut stream,
            } => {
                // Publish the running operation so an out-of-band CancelRequest
                // can target it; clear it once we have drained the result.
                self.cancels.set_running(self.pid, operation_id);

                let fields = Arc::new(
                    adapter::row_description(schema.as_ref(), result_format)?,
                );

                // Drain the Arrow stream into PG DataRows. Phase 1 materializes
                // here; cooperative cancel still applies because cancel acts on
                // the operation/backend stream (P2), and a cancelled stream
                // surfaces an error we map below.
                let mut rows: Vec<Result<_, PgWireError>> = Vec::new();
                while let Some(batch) = stream.next().await {
                    match batch {
                        Ok(b) => {
                            for r in adapter::encode_batch(fields.clone(), b) {
                                rows.push(r);
                            }
                        }
                        Err(e) => {
                            rows.push(Err(PgWireError::ApiError(Box::new(e))));
                            break;
                        }
                    }
                }
                self.cancels.clear_running(self.pid);

                let row_stream = stream::iter(rows);
                Ok(Response::Query(QueryResponse::new(fields, row_stream)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Map a `SET <name> = <value>` into a typed [`SessionMutation`]. Well-known
/// vars become typed top-level mutations; everything else is namespaced under
/// `pg.<name>` so it round-trips through `SHOW`.
fn set_to_mutation(name: &str, value: &str) -> SessionMutation {
    match name {
        "timezone" if value.eq_ignore_ascii_case("default") => SessionMutation::SetTimezone(None),
        "timezone" => SessionMutation::SetTimezone(Some(value.to_string())),
        _ => SessionMutation::SetEnvironmentVar {
            key: format!("pg.{name}"),
            value: SessionVarValue::String(value.to_string()),
        },
    }
}

/// Result column format requested by a portal (text unless the client asked for
/// binary on every column). Phase 1 treats the format uniformly across columns.
fn result_format(portal: &Portal<String>) -> FieldFormat {
    if portal.result_column_format.format_for(0) == FieldFormat::Binary {
        FieldFormat::Binary
    } else {
        FieldFormat::Text
    }
}

fn to_pg_err(err: GatewayError) -> PgWireError {
    // Domain error -> PG ErrorInfo (with SQLSTATE) -> wire UserError. This is the
    // only place the read-only PG path turns a domain error into a wire error.
    PgWireError::UserError(Box::new(adapter::error_to_pg(&err)))
}

/// An unguessable-per-connection secret for the PG backend key (§P4.6). The
/// cancel registry rejects mismatches, so this only needs to be hard to guess
/// across connections; `rand`'s thread RNG provides that without hand-rolling an
/// entropy source.
fn rand_secret() -> i32 {
    rand::random::<i32>()
}