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

//! P4 — `PgProtocolAdapter`: the wire <-> gateway boundary.
//!
//! Owns everything the design assigns to "线上长什么样" (`design/sql-path.md`
//! §P4.1/§P4.4): building [`OpenSessionRequest`] from startup parameters,
//! mapping startup vars into initial [`SessionVars`], encoding Arrow result
//! batches into PG `RowDescription` + `DataRow`, mapping a domain
//! [`GatewayError`] into a PG `ErrorInfo`, and the out-of-band cancel-key
//! registry. `Instance` itself never sees a pgwire type.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::ParamValues;
use pgwire::api::portal::Portal;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError};
use pgwire::messages::data::DataRow;

use crate::auth::credential_from_userpass;
use crate::error::GatewayError;
use crate::types::{
    ClientInfo, ClusterId, OpenSessionRequest, Principal, ProtocolKind, SessionId,
    SessionVarValue, SessionVars, SqlEnvironmentId,
};

/// The fixed SQL environment id the PostgreSQL frontend opens sessions with.
pub const PG_SQL_ENVIRONMENT: &str = "postgres";

/// Phase 1 PG path is single-cluster.
pub const DEFAULT_CLUSTER: &str = "default";

/// Catalog the `database` startup parameter is interpreted against (§P4.2).
pub const FLUSS_CATALOG: &str = "fluss";

// ---------------------------------------------------------------------------
// startup parameters -> OpenSessionRequest (§P4.2)
// ---------------------------------------------------------------------------

/// Build an [`OpenSessionRequest`] from PG startup parameters and the
/// authenticated principal.
///
/// Mapping (`design/sql-path.md` §P4.2):
/// - `sql_environment` is fixed to `"postgres"`, `cluster` is always `default`;
/// - `database` -> initial current catalog `fluss` + that name as schema;
/// - `application_name` / `TimeZone` / `search_path` -> initial [`SessionVars`];
/// - `client_encoding` must be UTF-8 (case/spelling-insensitive) or this errors.
///
/// `principal` is produced by the auth layer (cleartext-then-trust, P7) and is
/// preserved verbatim onto the request.
pub fn open_session_request_from_startup(
    principal: Principal,
    params: &HashMap<String, String>,
    peer_addr: Option<String>,
) -> Result<OpenSessionRequest, GatewayError> {
    if let Some(enc) = params.get("client_encoding") {
        if !is_utf8_encoding(enc) {
            return Err(GatewayError::Unsupported(format!(
                "client_encoding {enc:?} is not supported; Phase 1 PostgreSQL only supports UTF-8"
            )));
        }
    }

    // Phase 1: the PG `database` becomes the initial current schema under the
    // single `fluss` catalog.
    let current_schema = params
        .get("database")
        .filter(|d| !d.is_empty())
        .cloned();
    let timezone = params.get("TimeZone").filter(|t| !t.is_empty()).cloned();
    let mut vars = SessionVars {
        current_catalog: Some(FLUSS_CATALOG.to_string()),
        current_schema,
        timezone,
        ..SessionVars::default()
    };
    if let Some(app) = params.get("application_name").filter(|a| !a.is_empty()) {
        vars.environment.insert(
            "pg.application_name".to_string(),
            SessionVarValue::String(app.clone()),
        );
    }
    if let Some(sp) = params.get("search_path").filter(|s| !s.is_empty()) {
        vars.environment.insert(
            "pg.search_path".to_string(),
            SessionVarValue::String(sp.clone()),
        );
    }

    Ok(OpenSessionRequest {
        principal,
        cluster: ClusterId(DEFAULT_CLUSTER.to_string()),
        sql_environment: Some(SqlEnvironmentId(PG_SQL_ENVIRONMENT.to_string())),
        initial_vars: vars,
        client_info: ClientInfo {
            protocol: ProtocolKind::Postgres,
            peer_addr,
        },
    })
}

/// Map a cleartext-password handshake result into a neutral [`Credential`] and
/// keep the auth layer free of pgwire types. The trust authenticator ignores
/// the password; we still forward it so a future password store can verify it.
///
/// [`Credential`]: crate::auth::Credential
pub fn credential_from_pg_login(
    username: &str,
    password: Option<String>,
) -> crate::auth::Credential {
    credential_from_userpass(username, password)
}

fn is_utf8_encoding(enc: &str) -> bool {
    let norm: String = enc
        .chars()
        .filter(|c| !matches!(c, '-' | '_' | ' '))
        .collect::<String>()
        .to_ascii_uppercase();
    norm == "UTF8" || norm == "UNICODE"
}

// ---------------------------------------------------------------------------
// SET / SHOW semantics on SessionVars (§P4.3, the "落点" half lives in Instance;
// this is the var <-> string translation the adapter owns)
// ---------------------------------------------------------------------------

/// Read a session variable's current string value for a `SHOW <name>` reply.
///
/// Phase 1 answers the small set of vars the PG path tracks in [`SessionVars`];
/// unknown vars resolve to an empty string (matching PG's lenient behavior for
/// custom GUCs) rather than an error, so BI probes do not break.
pub fn show_var(vars: &SessionVars, name: &str) -> String {
    match name {
        "timezone" => vars.timezone.clone().unwrap_or_else(|| "UTC".to_string()),
        "search_path" => env_string(vars, "pg.search_path").unwrap_or_else(|| "public".to_string()),
        "application_name" => env_string(vars, "pg.application_name").unwrap_or_default(),
        "current_schema" | "current_schema()" => vars.current_schema.clone().unwrap_or_default(),
        "server_encoding" | "client_encoding" => "UTF8".to_string(),
        other => env_string(vars, &format!("pg.{other}")).unwrap_or_default(),
    }
}

fn env_string(vars: &SessionVars, key: &str) -> Option<String> {
    match vars.environment.get(key) {
        Some(SessionVarValue::String(s)) => Some(s.clone()),
        Some(SessionVarValue::Bool(b)) => Some(b.to_string()),
        Some(SessionVarValue::Int(i)) => Some(i.to_string()),
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Arrow -> PG encoding (§P4.4)
// ---------------------------------------------------------------------------

/// Derive PG `FieldInfo`s (the `RowDescription`) from an Arrow schema, honoring
/// the requested result column [`FieldFormat`] (text/binary). Reuses arrow-pg's
/// type-OID mapping rather than hand-rolling an OID table.
pub fn row_description(
    schema: &arrow::datatypes::Schema,
    format: FieldFormat,
) -> Result<Vec<FieldInfo>, PgWireError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let pg_type = arrow_pg::datatypes::field_into_pg_type(f)?;
            Ok(FieldInfo::new(
                f.name().clone(),
                None,
                None,
                pg_type,
                format,
            ))
        })
        .collect()
}

/// Encode a single Arrow [`RecordBatch`] into PG `DataRow`s against the given
/// field definitions. Reuses arrow-pg's `RowEncoder` for the actual value
/// serialization (text and binary).
pub fn encode_batch(
    fields: Arc<Vec<FieldInfo>>,
    batch: RecordBatch,
) -> Vec<Result<DataRow, PgWireError>> {
    arrow_pg::datatypes::encode_recordbatch(fields, normalize_for_pg(batch)).collect()
}

/// Normalize a result batch so arrow-pg's value encoder can handle every column.
///
/// arrow-pg maps `FixedSizeBinary(n)` (Fluss `BINARY(n)`) to the `bytea` OID in
/// the `RowDescription`, but its `encode_value` has no `FixedSizeBinary` arm and
/// falls through to an unsupported-type path that `format!`s the array and grows
/// an unbounded string (hangs / OOMs the connection). Variable-length `Binary`
/// IS handled, and the field is already advertised as `bytea`, so we convert any
/// `FixedSizeBinary` column to `Binary` here — same bytes, same wire type.
fn normalize_for_pg(batch: RecordBatch) -> RecordBatch {
    use arrow::array::{Array, BinaryBuilder, FixedSizeBinaryArray};
    use arrow::datatypes::{Field, Schema};

    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::FixedSizeBinary(_)))
    {
        return batch;
    }

    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if matches!(field.data_type(), DataType::FixedSizeBinary(_)) {
            // Downcast is infallible: we just matched the column's declared type.
            let fsb = column
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("FixedSizeBinary column");
            let mut builder = BinaryBuilder::with_capacity(fsb.len(), fsb.len() * fsb.value_length() as usize);
            for i in 0..fsb.len() {
                if fsb.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(fsb.value(i));
                }
            }
            fields.push(Arc::new(Field::new(
                field.name(),
                DataType::Binary,
                field.is_nullable(),
            )));
            columns.push(Arc::new(builder.finish()) as _);
        } else {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("rebuilt batch has matching schema/columns")
}

/// Build a single-column, single-row text result (used for `SHOW <var>`).
pub fn single_text_row(
    column: &str,
    value: &str,
) -> (Arc<Vec<FieldInfo>>, Result<DataRow, PgWireError>) {
    let fields = Arc::new(vec![FieldInfo::new(
        column.to_string(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    )]);
    let mut encoder = DataRowEncoder::new(fields.clone());
    let row = encoder.encode_field(&Some(value)).map(|_| encoder.take_row());
    (fields, row)
}

// ---------------------------------------------------------------------------
// PG -> Arrow/DataFusion: bind parameter decoding (§P4.4)
// ---------------------------------------------------------------------------

/// Decode a bound portal's positional parameters (PG wire text/binary bytes) into
/// DataFusion-native [`ParamValues`], reusing arrow-pg's per-OID
/// `text/binary -> ScalarValue` mapping. Returns `None` when the portal carries
/// no parameters so the simple, non-parameterized path stays allocation-free.
///
/// `inferred_types[i]` is the gateway's expected Arrow type for `$(i+1)` (from
/// `Instance.describe_sql`). It is the fallback used when the client did not
/// declare a parameter OID in `Parse` — which is the common case for
/// `tokio-postgres` / JDBC, who let the server's `ParameterDescription` drive the
/// binary encoding. Without it arrow-pg would mis-decode a binary INT4 as text.
pub fn decode_params<S>(
    portal: &Portal<S>,
    inferred_types: &[DataType],
) -> Result<Option<ParamValues>, PgWireError>
where
    S: Clone,
{
    if portal.parameter_len() == 0 {
        return Ok(None);
    }
    let inferred: Vec<Option<&DataType>> = inferred_types.iter().map(Some).collect();
    let params = arrow_pg::datatypes::df::deserialize_parameters(portal, &inferred)?;
    Ok(Some(params))
}

/// Map inferred Arrow parameter types into PG type OIDs for a statement's
/// `ParameterDescription`. Reuses arrow-pg's `into_pg_type`; an Arrow type with no
/// PG mapping surfaces a `PgWireError` rather than a silently-wrong OID.
pub fn param_types_to_pg(types: &[DataType]) -> Result<Vec<Type>, PgWireError> {
    types
        .iter()
        .map(arrow_pg::datatypes::into_pg_type)
        .collect()
}

// ---------------------------------------------------------------------------
// domain error -> PG error (§P4 boundary mapping)
// ---------------------------------------------------------------------------

/// Map a gateway domain [`GatewayError`] into a PG `ErrorInfo`, choosing the
/// SQLSTATE code at the protocol boundary (the domain error itself carries no
/// protocol code, per `error.rs`). Severity is always `ERROR`.
pub fn error_to_pg(err: &GatewayError) -> ErrorInfo {
    let (code, msg) = match err {
        GatewayError::InvalidArgument(m) => ("22023", m.clone()),
        GatewayError::Unauthenticated(m) => ("28000", m.clone()),
        GatewayError::Unauthorized(m) => ("42501", m.clone()),
        GatewayError::SessionNotFound(m) => ("08003", format!("session not found: {m}")),
        GatewayError::OperationNotFound(m) => ("34000", format!("operation not found: {m}")),
        GatewayError::DatabaseNotFound { database } => {
            ("3D000", format!("database not found: {database}"))
        }
        GatewayError::TableNotFound { database, table } => {
            ("42P01", format!("table not found: {database}.{table}"))
        }
        // 42P07 = duplicate_table.
        GatewayError::TableAlreadyExists { database, table } => {
            ("42P07", format!("table already exists: {database}.{table}"))
        }
        // 0A000 = feature_not_supported; message must point at the REST write path.
        GatewayError::Unsupported(m) => ("0A000", m.clone()),
        GatewayError::Timeout(m) => ("57014", format!("timed out: {m}")),
        GatewayError::Cancelled(m) => ("57014", format!("cancelled: {m}")),
        GatewayError::Backend(m) => ("58000", format!("backend error: {m}")),
        GatewayError::Internal(m) => ("XX000", format!("internal error: {m}")),
    };
    ErrorInfo::new("ERROR".to_string(), code.to_string(), msg)
}

/// The standard error returned when a write/DDL statement reaches the read-only
/// PG path (§P4.7). Centralized so the message is identical everywhere.
pub fn write_rejected_error() -> GatewayError {
    GatewayError::Unsupported(
        "Phase 1 PostgreSQL is read-only; use the REST API to write".to_string(),
    )
}

// ---------------------------------------------------------------------------
// out-of-band cancel-key registry (§P4.6)
// ---------------------------------------------------------------------------

/// An entry in the cancel registry: which session a backend key belongs to and,
/// if a query is currently running, the operation that a `CancelRequest` should
/// target.
#[derive(Debug, Clone)]
struct CancelEntry {
    secret: i32,
    /// Kept for traceability / future per-session cancel scoping; the cancel
    /// path itself only needs the running operation id.
    #[allow(dead_code)]
    session_id: SessionId,
    running_operation: Option<crate::types::OperationId>,
}

/// Shared `(PID, secret) -> session + running operation` map.
///
/// A `CancelRequest` arrives on a *separate* connection, so this registry must
/// be shared across all connection tasks. The connection task registers its
/// `(pid, secret)` at session open, publishes the running operation id while a
/// query executes, and clears it when the query finishes. On cancel we verify
/// the secret, then return the running operation id (if any) for the handler to
/// pass to `Instance.cancel_operation`.
#[derive(Debug, Clone, Default)]
pub struct CancelRegistry {
    inner: Arc<Mutex<HashMap<i32, CancelEntry>>>,
}

impl CancelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a backend key for a freshly opened session.
    pub fn register(&self, pid: i32, secret: i32, session_id: SessionId) {
        self.inner.lock().unwrap().insert(
            pid,
            CancelEntry {
                secret,
                session_id,
                running_operation: None,
            },
        );
    }

    /// Publish the operation a running query is executing under `pid`.
    pub fn set_running(&self, pid: i32, op: crate::types::OperationId) {
        if let Some(e) = self.inner.lock().unwrap().get_mut(&pid) {
            e.running_operation = Some(op);
        }
    }

    /// Clear the running operation once the query has finished.
    pub fn clear_running(&self, pid: i32) {
        if let Some(e) = self.inner.lock().unwrap().get_mut(&pid) {
            e.running_operation = None;
        }
    }

    /// Drop a session's key entirely (connection close / session close).
    pub fn remove(&self, pid: i32) {
        self.inner.lock().unwrap().remove(&pid);
    }

    /// Resolve a `CancelRequest` against the registry (§P4.6).
    pub fn resolve_cancel(&self, pid: i32, secret: i32) -> CancelResolution {
        match self.inner.lock().unwrap().get(&pid) {
            Some(e) if e.secret == secret => match &e.running_operation {
                Some(op) => CancelResolution::Cancel(op.clone()),
                None => CancelResolution::Ignore,
            },
            // Unknown pid or mismatched secret.
            _ => CancelResolution::Reject,
        }
    }
}

/// Outcome of resolving an out-of-band `CancelRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelResolution {
    /// Secret matched and a query is running: cancel this operation.
    Cancel(crate::types::OperationId),
    /// Secret matched but nothing is running: ignore the request.
    Ignore,
    /// Unknown pid or wrong secret: reject (the PG protocol sends no reply).
    Reject,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.to_string(),
        }
    }

    #[test]
    fn startup_maps_fixed_and_database_and_vars() {
        let mut params = HashMap::new();
        params.insert("database".to_string(), "salesdb".to_string());
        params.insert("application_name".to_string(), "psql".to_string());
        params.insert("TimeZone".to_string(), "Asia/Shanghai".to_string());
        params.insert("search_path".to_string(), "myschema".to_string());

        let req =
            open_session_request_from_startup(principal("alice"), &params, Some("1.2.3.4:5".into()))
                .unwrap();

        assert_eq!(req.principal.name, "alice");
        assert_eq!(req.cluster.0, "default");
        assert_eq!(req.sql_environment.unwrap().0, "postgres");
        assert_eq!(req.client_info.protocol, ProtocolKind::Postgres);
        assert_eq!(req.initial_vars.current_catalog.as_deref(), Some("fluss"));
        assert_eq!(req.initial_vars.current_schema.as_deref(), Some("salesdb"));
        assert_eq!(req.initial_vars.timezone.as_deref(), Some("Asia/Shanghai"));
        assert_eq!(
            req.initial_vars.environment.get("pg.application_name"),
            Some(&SessionVarValue::String("psql".into()))
        );
        assert_eq!(
            req.initial_vars.environment.get("pg.search_path"),
            Some(&SessionVarValue::String("myschema".into()))
        );
    }

    #[test]
    fn startup_accepts_utf8_spellings_and_rejects_others() {
        for ok in ["UTF8", "utf-8", "UNICODE", "Utf_8"] {
            let mut p = HashMap::new();
            p.insert("client_encoding".to_string(), ok.to_string());
            assert!(open_session_request_from_startup(principal("a"), &p, None).is_ok());
        }
        let mut bad = HashMap::new();
        bad.insert("client_encoding".to_string(), "LATIN1".to_string());
        let err = open_session_request_from_startup(principal("a"), &bad, None).unwrap_err();
        assert!(matches!(err, GatewayError::Unsupported(_)));
    }

    #[test]
    fn show_var_reads_session_vars() {
        let mut vars = SessionVars {
            timezone: Some("UTC".into()),
            ..SessionVars::default()
        };
        vars.environment.insert(
            "pg.application_name".into(),
            SessionVarValue::String("dbeaver".into()),
        );
        assert_eq!(show_var(&vars, "timezone"), "UTC");
        assert_eq!(show_var(&vars, "application_name"), "dbeaver");
        // Unknown var: empty string, not an error.
        assert_eq!(show_var(&vars, "some_custom_guc"), "");
        // Defaults for unset well-known vars.
        assert_eq!(show_var(&SessionVars::default(), "search_path"), "public");
    }

    #[test]
    fn error_mapping_picks_sqlstate_and_keeps_message() {
        let e = error_to_pg(&write_rejected_error());
        assert_eq!(e.code, "0A000");
        assert!(e.message.contains("REST"));

        let e = error_to_pg(&GatewayError::TableNotFound {
            database: "fluss".into(),
            table: "t".into(),
        });
        assert_eq!(e.code, "42P01");

        let e = error_to_pg(&GatewayError::Timeout("deadline".into()));
        assert_eq!(e.code, "57014");
    }

    #[test]
    fn param_types_map_arrow_to_pg_oids() {
        let pg = param_types_to_pg(&[DataType::Int32, DataType::Utf8, DataType::Boolean]).unwrap();
        assert_eq!(pg, vec![Type::INT4, Type::TEXT, Type::BOOL]);
        // Empty in -> empty out (non-parameterized statement).
        assert!(param_types_to_pg(&[]).unwrap().is_empty());
    }

    #[test]
    fn single_text_row_encodes() {
        let (fields, row) = single_text_row("search_path", "public");
        assert_eq!(fields.len(), 1);
        assert!(row.is_ok());
    }

    #[test]
    fn fixed_size_binary_is_normalized_to_binary_for_pg() {
        use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray};
        use arrow::datatypes::{Field, Schema};

        let fsb = FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2, 3, 4]].into_iter()).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("b", DataType::FixedSizeBinary(4), false)])),
            vec![Arc::new(fsb)],
        )
        .unwrap();

        let out = normalize_for_pg(batch);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Binary, "fsb -> binary");
        let col = out.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(col.value(0), &[1u8, 2, 3, 4], "bytes preserved");

        // And the full encode path must produce a row instead of hanging on the
        // unsupported-type fallback.
        let fields = Arc::new(row_description(&out.schema(), FieldFormat::Text).unwrap());
        let rows = encode_batch(fields, out);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_ok());
    }

    #[test]
    fn normalize_is_a_noop_without_fixed_size_binary() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema};

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let out = normalize_for_pg(batch);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn cancel_registry_secret_must_match_and_tracks_running_op() {
        let reg = CancelRegistry::new();
        reg.register(42, 999, SessionId("s1".into()));

        // No running op yet -> ignore.
        assert_eq!(reg.resolve_cancel(42, 999), CancelResolution::Ignore);

        // Wrong secret -> rejected.
        assert_eq!(reg.resolve_cancel(42, 1), CancelResolution::Reject);
        // Unknown pid -> rejected.
        assert_eq!(reg.resolve_cancel(7, 999), CancelResolution::Reject);

        // With a running op -> cancel it.
        reg.set_running(42, crate::types::OperationId("op1".into()));
        assert_eq!(
            reg.resolve_cancel(42, 999),
            CancelResolution::Cancel(crate::types::OperationId("op1".into()))
        );

        // After clear -> back to ignore.
        reg.clear_running(42);
        assert_eq!(reg.resolve_cancel(42, 999), CancelResolution::Ignore);

        // After remove -> rejected.
        reg.remove(42);
        assert_eq!(reg.resolve_cancel(42, 999), CancelResolution::Reject);
    }
}
