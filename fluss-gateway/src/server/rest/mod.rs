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

//! P5 — REST frontend (axum routes / handlers).
//!
//! The only write path in Phase 1, with at-least-once semantics; also serves
//! read-only metadata endpoints. The transport here is intentionally thin: it
//! parses `Authorization: Basic` into a neutral [`Principal`], builds a
//! request-scoped [`RequestExecutionContext`], decodes the write body via
//! [`crate::direct`], and calls the [`GatewayInstance`] facade. It NEVER creates
//! a `GatewaySession` or registers an Operation — the direct path is stateless
//! and `(principal, cluster)` is re-derived from every request.
//!
//! Multi-cluster evolution uses path prefixes (`/v1/clusters/{cluster}/...`),
//! not headers. Phase 1 only routes `cluster == "default"`; other clusters are
//! still parsed and threaded so the path shape is frozen from day one.
//!
//! at-least-once (direct-path.md §6): a 2xx means the backend acked. A timeout /
//! disconnect is NOT a cancel and NOT a rollback — the result is *unknown* and
//! the client must treat the write as "possibly applied". There is no
//! user-level cancel endpoint, by design.
//!
//! Design: `design/direct-path.md` §4 (path table), §6 (error map), §7 (no
//! session); auth seam in `design/infra.md` §P7.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::auth::{credential_from_userpass, Authenticator};
use crate::direct::{decode_write_body, WriteEncoding};
use crate::error::GatewayError;
use crate::instance::GatewayInstance;
use crate::types::{
    ClusterId, ColumnSpec, ColumnType, CreateTableRequest, DirectWriteRequest, MetadataScope,
    Principal, RequestExecutionContext, RequestId, TableDistribution, TableRef,
};

/// Shared, cheaply-cloneable REST wiring: the gateway facade and the auth seam.
/// One is built per server and cloned into axum state for every request. Holds
/// no per-connection or per-session state — the direct path is request-scoped.
#[derive(Clone)]
pub struct RestState {
    instance: Arc<dyn GatewayInstance>,
    authenticator: Arc<dyn Authenticator>,
}

/// The REST frontend: builds the axum router and owns bind/serve, mirroring the
/// `PgServer` split so tests can learn the ephemeral port before driving traffic.
#[derive(Clone)]
pub struct RestServer {
    state: RestState,
}

impl RestServer {
    pub fn new(
        instance: Arc<dyn GatewayInstance>,
        authenticator: Arc<dyn Authenticator>,
    ) -> Self {
        Self {
            state: RestState {
                instance,
                authenticator,
            },
        }
    }

    /// Build the axum [`Router`] with the frozen Phase 1 resource path table.
    /// Exposed (not just `serve`) so tests can mount it onto a `oneshot` tower
    /// service as well as a real loopback listener.
    pub fn router(&self) -> Router {
        Router::new()
            // --- write (implemented) ---
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables/{table}/records",
                post(handle_records),
            )
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables/{table}/records:delete",
                post(handle_records_delete),
            )
            // --- metadata (implemented, read-only) ---
            .route(
                "/v1/clusters/{cluster}/databases",
                get(handle_list_databases),
            )
            // List tables (GET) + create table (POST) on the collection resource.
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables",
                get(handle_list_tables).post(handle_create_table),
            )
            // Get table (GET) + drop table (DELETE) on the instance resource.
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables/{table}",
                get(handle_get_table).delete(handle_drop_table),
            )
            // --- direct read (path frozen, NOT implemented this phase) ---
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables/{table}/lookup",
                post(handle_read_not_implemented),
            )
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables/{table}/prefix-scan",
                post(handle_read_not_implemented),
            )
            .route(
                "/v1/clusters/{cluster}/databases/{db}/tables/{table}/log-scan",
                post(handle_read_not_implemented),
            )
            .with_state(self.state.clone())
    }

    /// Bind a TCP listener and return it with the resolved local address. Tests
    /// bind `127.0.0.1:0` to learn the ephemeral port.
    pub async fn bind(addr: &str) -> std::io::Result<(TcpListener, std::net::SocketAddr)> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        Ok((listener, local))
    }

    /// Serve the router on an already-bound listener until the process ends.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        axum::serve(listener, self.router().into_make_service()).await
    }
}

// ---------------------------------------------------------------------------
// auth: Authorization: Basic -> Principal (§P7, direct-path.md §1)
// ---------------------------------------------------------------------------

/// Parse a username out of an `Authorization: Basic <base64(user:pass)>` header.
/// The password is intentionally discarded (Phase 1 trust); the [`Authenticator`]
/// turns the username into a [`Principal`]. Extraction lives in the REST layer,
/// not in `auth/`, which must stay free of HTTP types. Returns the raw
/// `(username, password)` so the caller picks the credential variant.
fn parse_basic_auth(headers: &HeaderMap) -> Result<(String, Option<String>), GatewayError> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| GatewayError::Unauthenticated("missing Authorization header".into()))?;

    let b64 = value
        .strip_prefix("Basic ")
        .or_else(|| value.strip_prefix("basic "))
        .ok_or_else(|| GatewayError::Unauthenticated("expected Basic auth scheme".into()))?;

    let decoded = base64_decode(b64.trim())
        .map_err(|_| GatewayError::Unauthenticated("malformed Basic credentials".into()))?;
    let text = String::from_utf8(decoded)
        .map_err(|_| GatewayError::Unauthenticated("non-utf8 Basic credentials".into()))?;

    // `user:password`; password may itself contain ':' so split only once.
    let (user, pass) = match text.split_once(':') {
        Some((u, p)) => (u.to_string(), Some(p.to_string())),
        None => (text, None),
    };
    if user.is_empty() {
        return Err(GatewayError::Unauthenticated(
            "empty username in Basic credentials".into(),
        ));
    }
    Ok((user, pass))
}

/// Authenticate the request and build the request-scoped context. The principal
/// comes from auth; the cluster from the path; both are re-derived per request
/// and never inherited from a session (§7).
async fn make_context(
    state: &RestState,
    headers: &HeaderMap,
    cluster: &str,
) -> Result<RequestExecutionContext, GatewayError> {
    let (username, password) = parse_basic_auth(headers)?;
    let credential = credential_from_userpass(username, password);
    let principal: Principal = state
        .authenticator
        .authenticate(credential)
        .await
        .map_err(GatewayError::from)?;
    Ok(RequestExecutionContext {
        principal,
        cluster: ClusterId(cluster.to_string()),
        request_id: RequestId(new_request_id()),
        // Phase 1: deadline is left to a fronting timeout; the cancel token fires
        // on client disconnect via the runtime. A server-side request deadline is
        // a later refinement (direct-path.md §1).
        deadline: None,
        cancel: CancellationToken::new(),
    })
}

// ---------------------------------------------------------------------------
// handlers: write (§3, §4)
// ---------------------------------------------------------------------------

async fn handle_records(
    State(state): State<RestState>,
    Path((cluster, db, table)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    write_records(state, cluster, db, table, headers, body, WriteIntent::Records).await
}

async fn handle_records_delete(
    State(state): State<RestState>,
    Path((cluster, db, table)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    write_records(state, cluster, db, table, headers, body, WriteIntent::Delete).await
}

/// Whether the route is the upsert/append endpoint or the delete endpoint. The
/// concrete `DirectWriteRequest` variant (KvUpsert vs LogAppend) is resolved
/// against the table's kind by the backend in P6; here we only know "records"
/// vs "records:delete".
enum WriteIntent {
    Records,
    Delete,
}

async fn write_records(
    state: RestState,
    cluster: String,
    db: String,
    table: String,
    headers: HeaderMap,
    body: Bytes,
    intent: WriteIntent,
) -> Response {
    let result = async {
        let context = make_context(&state, &headers, &cluster).await?;
        let table_ref = TableRef {
            database: db,
            table,
        };

        let encoding = WriteEncoding::negotiate(content_type(&headers))?;

        // Schema is taken from the target table (no schema-on-write). Resolving it
        // up-front also yields a clean 404 for an unknown table before any decode.
        let scope = MetadataScope {
            principal: context.principal.clone(),
            cluster: context.cluster.clone(),
        };
        let info = state
            .instance
            .get_table_info(scope, table_ref.clone())
            .await?;

        let batch = decode_write_body(encoding, info.schema.clone(), &body)?;

        let req = match intent {
            // `records:delete` always carries primary keys to delete.
            WriteIntent::Delete => DirectWriteRequest::KvDelete {
                context,
                table: table_ref,
                keys: batch,
            },
            // `records` is upsert-or-append. Phase 1 maps it to KvUpsert; the
            // backend reinterprets it as LogAppend when the target is a Log table
            // (P6). The request shape covers all three; this transport does not
            // re-fetch the table kind just to pick the enum arm.
            WriteIntent::Records => DirectWriteRequest::KvUpsert {
                context,
                table: table_ref,
                rows: batch,
            },
        };

        let written = state.instance.write_direct(req).await?;
        Ok::<_, GatewayError>(written)
    }
    .await;

    match result {
        Ok(w) => (
            StatusCode::OK,
            Json(serde_json::json!({ "rows_written": w.rows_written })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

// ---------------------------------------------------------------------------
// handlers: metadata (§4)
// ---------------------------------------------------------------------------

async fn handle_list_databases(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let ctx = make_context(&state, &headers, &cluster).await?;
        let scope = MetadataScope {
            principal: ctx.principal,
            cluster: ctx.cluster,
        };
        state.instance.list_databases(scope).await
    }
    .await;
    match result {
        Ok(dbs) => (StatusCode::OK, Json(serde_json::json!({ "databases": dbs }))).into_response(),
        Err(e) => error_response(e),
    }
}

async fn handle_list_tables(
    State(state): State<RestState>,
    Path((cluster, db)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let ctx = make_context(&state, &headers, &cluster).await?;
        let scope = MetadataScope {
            principal: ctx.principal,
            cluster: ctx.cluster,
        };
        state.instance.list_tables(scope, db).await
    }
    .await;
    match result {
        Ok(tables) => {
            (StatusCode::OK, Json(serde_json::json!({ "tables": tables }))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn handle_get_table(
    State(state): State<RestState>,
    Path((cluster, db, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let ctx = make_context(&state, &headers, &cluster).await?;
        let scope = MetadataScope {
            principal: ctx.principal,
            cluster: ctx.cluster,
        };
        let table_ref = TableRef {
            database: db,
            table,
        };
        state.instance.get_table_info(scope, table_ref).await
    }
    .await;
    match result {
        Ok(info) => {
            let fields: Vec<_> = info
                .schema
                .fields()
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name(),
                        "data_type": f.data_type().to_string(),
                        "nullable": f.is_nullable(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "database": info.name.database,
                    "table": info.name.table,
                    "columns": fields,
                })),
            )
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

// ---------------------------------------------------------------------------
// handlers: table management / DDL (design/direct-path.md "表管理（DDL）API")
// ---------------------------------------------------------------------------

/// JSON body for `POST .../tables` (kafka-rest-style: name in body, configs as a
/// name/value array, `validate_only` dry-run). Wire-only; mapped to the neutral
/// [`CreateTableRequest`] domain type before reaching the instance.
#[derive(Debug, Deserialize)]
struct CreateTableBody {
    table_name: String,
    columns: Vec<ColumnBody>,
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    distribution: Option<DistributionBody>,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    configs: Vec<ConfigEntry>,
    #[serde(default)]
    validate_only: bool,
}

#[derive(Debug, Deserialize)]
struct ColumnBody {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default = "default_true")]
    nullable: bool,
}

#[derive(Debug, Deserialize)]
struct DistributionBody {
    #[serde(default)]
    bucket_keys: Vec<String>,
    #[serde(default)]
    bucket_count: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct ConfigEntry {
    name: String,
    value: String,
}

fn default_true() -> bool {
    true
}

/// Parse a column `type` string (case-insensitive) into a [`ColumnType`], honoring
/// parameterized forms `DECIMAL(p,s)`, `CHAR(n)`, `BINARY(n)`, `TIME(p)`,
/// `TIMESTAMP(p)`. Range validation is deferred to the backend mapping.
fn parse_column_type(raw: &str) -> Result<ColumnType, GatewayError> {
    let s = raw.trim();
    let bad = || GatewayError::InvalidArgument(format!("unsupported column type: {raw}"));
    let (base, args) = match s.split_once('(') {
        Some((b, rest)) => {
            let inner = rest.strip_suffix(')').ok_or_else(bad)?;
            let nums: Result<Vec<u32>, _> =
                inner.split(',').map(|p| p.trim().parse::<u32>()).collect();
            (b.trim(), nums.map_err(|_| bad())?)
        }
        None => (s, Vec::new()),
    };
    let up = base.to_ascii_uppercase();
    let ct = match up.as_str() {
        "BOOLEAN" | "BOOL" => ColumnType::Boolean,
        "TINYINT" => ColumnType::TinyInt,
        "SMALLINT" => ColumnType::SmallInt,
        "INT" | "INTEGER" => ColumnType::Int,
        "BIGINT" => ColumnType::BigInt,
        "FLOAT" | "REAL" => ColumnType::Float,
        "DOUBLE" => ColumnType::Double,
        "STRING" | "TEXT" | "VARCHAR" => ColumnType::String,
        "BYTES" => ColumnType::Bytes,
        "DATE" => ColumnType::Date,
        "DECIMAL" | "NUMERIC" => {
            let precision = *args.first().ok_or_else(bad)?;
            let scale = args.get(1).copied().unwrap_or(0);
            if args.len() > 2 {
                return Err(bad());
            }
            ColumnType::Decimal { precision, scale }
        }
        "CHAR" => ColumnType::Char {
            length: *args.first().ok_or_else(bad)?,
        },
        "BINARY" => ColumnType::Binary {
            length: *args.first().ok_or_else(bad)?,
        },
        "TIME" => ColumnType::Time {
            precision: args.first().copied().unwrap_or(0),
        },
        "TIMESTAMP" => ColumnType::Timestamp {
            precision: args.first().copied().unwrap_or(6),
        },
        _ => return Err(bad()),
    };
    Ok(ct)
}

/// Build the JSON metadata view of a table (same shape as `GET .../tables/{t}`).
fn table_info_json(info: &crate::types::TableInfo) -> serde_json::Value {
    let fields: Vec<_> = info
        .schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": f.data_type().to_string(),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    serde_json::json!({
        "database": info.name.database,
        "table": info.name.table,
        "columns": fields,
    })
}

async fn handle_create_table(
    State(state): State<RestState>,
    Path((cluster, db)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let ctx = make_context(&state, &headers, &cluster).await?;
        let scope = MetadataScope {
            principal: ctx.principal,
            cluster: ctx.cluster,
        };

        let spec: CreateTableBody = serde_json::from_slice(&body)
            .map_err(|e| GatewayError::InvalidArgument(format!("invalid create-table body: {e}")))?;
        if spec.table_name.trim().is_empty() {
            return Err(GatewayError::InvalidArgument("table_name is required".into()));
        }
        let columns = spec
            .columns
            .iter()
            .map(|c| {
                Ok(ColumnSpec {
                    name: c.name.clone(),
                    data_type: parse_column_type(&c.data_type)?,
                    nullable: c.nullable,
                })
            })
            .collect::<Result<Vec<_>, GatewayError>>()?;

        let table = TableRef {
            database: db.clone(),
            table: spec.table_name.clone(),
        };
        let req = CreateTableRequest {
            table: table.clone(),
            columns,
            primary_key: spec.primary_key,
            distribution: spec.distribution.map(|d| TableDistribution {
                bucket_keys: d.bucket_keys,
                bucket_count: d.bucket_count,
            }),
            comment: spec.comment,
            properties: spec
                .configs
                .into_iter()
                .map(|c| (c.name, c.value))
                .collect(),
            // CREATE returns 409 on conflict (kafka-rest semantics), so do not
            // silently ignore an existing table here.
            ignore_if_exists: false,
        };

        if spec.validate_only {
            // Dry-run: validate column types (already parsed above) without
            // creating. Reflect back the request shape; no Fluss call.
            return Ok((StatusCode::OK, validate_only_json(&req)));
        }

        state.instance.create_table(scope.clone(), req).await?;
        // Return the freshly-created table's metadata (201).
        let info = state.instance.get_table_info(scope, table).await?;
        Ok((StatusCode::CREATED, table_info_json(&info)))
    }
    .await;
    match result {
        Ok((status, body)) => (status, Json(body)).into_response(),
        Err(e) => error_response(e),
    }
}

/// JSON echoed back for a successful `validate_only` create (HTTP 200, not created).
fn validate_only_json(req: &CreateTableRequest) -> serde_json::Value {
    serde_json::json!({
        "validate_only": true,
        "database": req.table.database,
        "table": req.table.table,
        "column_count": req.columns.len(),
        "primary_key": req.primary_key,
    })
}

async fn handle_drop_table(
    State(state): State<RestState>,
    Path((cluster, db, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let ctx = make_context(&state, &headers, &cluster).await?;
        let scope = MetadataScope {
            principal: ctx.principal,
            cluster: ctx.cluster,
        };
        let table_ref = TableRef {
            database: db,
            table,
        };
        state.instance.drop_table(scope, table_ref, false).await
    }
    .await;
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_response(e),
    }
}

// ---------------------------------------------------------------------------
// handlers: deferred direct read (path frozen, §4 / Backlog §7)
// ---------------------------------------------------------------------------

/// Placeholder for the deferred direct-read endpoints: the route exists so the
/// path table is frozen, but Phase 1 returns 501 with a stable message.
async fn handle_read_not_implemented() -> Response {
    error_response(GatewayError::Unsupported(
        "direct read (lookup / prefix-scan / log-scan) is deferred past Phase 1".into(),
    ))
}

// ---------------------------------------------------------------------------
// error mapping: domain error -> HTTP status (direct-path.md §6)
// ---------------------------------------------------------------------------

/// The frozen domain→HTTP status map. Pure so it can be unit-tested without a
/// live server. `Cancelled` maps to 499 (client-closed-request, nginx ext.) —
/// it only arises when the client disconnected, so there is no client to read a
/// body, but the code is recorded for logs/metrics consistency.
pub fn status_for(err: &GatewayError) -> StatusCode {
    match err {
        GatewayError::InvalidArgument(_) => StatusCode::BAD_REQUEST, // 400
        GatewayError::Unauthenticated(_) => StatusCode::UNAUTHORIZED, // 401
        GatewayError::Unauthorized(_) => StatusCode::FORBIDDEN,      // 403
        GatewayError::SessionNotFound(_)
        | GatewayError::OperationNotFound(_)
        | GatewayError::DatabaseNotFound { .. }
        | GatewayError::TableNotFound { .. } => StatusCode::NOT_FOUND, // 404
        GatewayError::TableAlreadyExists { .. } => StatusCode::CONFLICT, // 409
        GatewayError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED, // 501
        GatewayError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,     // 504
        GatewayError::Cancelled(_) => StatusCode::from_u16(499).unwrap(),
        GatewayError::Backend(_) | GatewayError::Internal(_) => {
            StatusCode::INTERNAL_SERVER_ERROR // 5xx
        }
    }
}

/// A short machine-readable error code string for the JSON error envelope.
fn error_code(err: &GatewayError) -> &'static str {
    match err {
        GatewayError::InvalidArgument(_) => "invalid_argument",
        GatewayError::Unauthenticated(_) => "unauthenticated",
        GatewayError::Unauthorized(_) => "unauthorized",
        GatewayError::SessionNotFound(_) => "session_not_found",
        GatewayError::OperationNotFound(_) => "operation_not_found",
        GatewayError::DatabaseNotFound { .. } => "database_not_found",
        GatewayError::TableNotFound { .. } => "table_not_found",
        GatewayError::TableAlreadyExists { .. } => "table_already_exists",
        GatewayError::Unsupported(_) => "unsupported",
        GatewayError::Timeout(_) => "timeout",
        GatewayError::Cancelled(_) => "cancelled",
        GatewayError::Backend(_) => "backend",
        GatewayError::Internal(_) => "internal",
    }
}

fn error_response(err: GatewayError) -> Response {
    let status = status_for(&err);
    let body = serde_json::json!({
        "error": {
            "code": error_code(&err),
            "message": err.to_string(),
        }
    });
    (status, Json(body)).into_response()
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

fn content_type(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
}

fn new_request_id() -> String {
    use rand::Rng;
    let n: u64 = rand::thread_rng().gen();
    format!("req-{n:016x}")
}

/// Minimal RFC4648 standard base64 decoder (no padding requirement enforced).
/// Avoids adding a base64 crate just for Basic-auth parsing.
fn base64_decode(input: &str) -> Result<Vec<u8>, ()> {
    fn val(c: u8) -> Result<u8, ()> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(()),
        }
    }
    let bytes: Vec<u8> = input.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        let n = chunk.len();
        if n < 2 {
            return Err(());
        }
        for (i, &c) in chunk.iter().enumerate() {
            buf[i] = val(c)?;
        }
        out.push((buf[0] << 2) | (buf[1] >> 4));
        if n >= 3 {
            out.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if n == 4 {
            out.push((buf[2] << 6) | buf[3]);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_with_basic(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            value.parse().unwrap(),
        );
        h
    }

    #[test]
    fn base64_decode_roundtrip() {
        // "alice:secret" base64 == "YWxpY2U6c2VjcmV0"
        assert_eq!(base64_decode("YWxpY2U6c2VjcmV0").unwrap(), b"alice:secret");
        assert_eq!(base64_decode("YWxpY2U=").unwrap(), b"alice");
        assert!(base64_decode("not base64!!").is_err());
    }

    #[test]
    fn parse_basic_auth_extracts_username_and_password() {
        let h = header_with_basic("Basic YWxpY2U6c2VjcmV0"); // alice:secret
        let (u, p) = parse_basic_auth(&h).unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_basic_auth_password_may_contain_colon() {
        // base64("bob:a:b:c")
        let encoded = {
            let mut v = Vec::new();
            for chunk in b"bob:a:b:c".chunks(1) {
                v.extend_from_slice(chunk);
            }
            // build via the standard encoder shape using a tiny inline encoder
            base64_encode(&v)
        };
        let h = header_with_basic(&format!("Basic {encoded}"));
        let (u, p) = parse_basic_auth(&h).unwrap();
        assert_eq!(u, "bob");
        assert_eq!(p.as_deref(), Some("a:b:c"));
    }

    #[test]
    fn parse_basic_auth_missing_header_is_unauthenticated() {
        let h = HeaderMap::new();
        assert!(matches!(
            parse_basic_auth(&h),
            Err(GatewayError::Unauthenticated(_))
        ));
    }

    #[test]
    fn parse_basic_auth_wrong_scheme_is_unauthenticated() {
        let h = header_with_basic("Bearer sometoken");
        assert!(matches!(
            parse_basic_auth(&h),
            Err(GatewayError::Unauthenticated(_))
        ));
    }

    #[test]
    fn parse_basic_auth_empty_username_is_unauthenticated() {
        // base64(":pw")
        let encoded = base64_encode(b":pw");
        let h = header_with_basic(&format!("Basic {encoded}"));
        assert!(matches!(
            parse_basic_auth(&h),
            Err(GatewayError::Unauthenticated(_))
        ));
    }

    #[test]
    fn parse_column_type_handles_simple_and_parameterized() {
        assert_eq!(parse_column_type("int").unwrap(), ColumnType::Int);
        assert_eq!(parse_column_type("  STRING ").unwrap(), ColumnType::String);
        assert_eq!(parse_column_type("BigInt").unwrap(), ColumnType::BigInt);
        assert_eq!(
            parse_column_type("DECIMAL(10, 2)").unwrap(),
            ColumnType::Decimal { precision: 10, scale: 2 }
        );
        assert_eq!(
            parse_column_type("decimal(5)").unwrap(),
            ColumnType::Decimal { precision: 5, scale: 0 }
        );
        assert_eq!(
            parse_column_type("TIMESTAMP(3)").unwrap(),
            ColumnType::Timestamp { precision: 3 }
        );
        assert_eq!(
            parse_column_type("TIMESTAMP").unwrap(),
            ColumnType::Timestamp { precision: 6 }
        );
        assert_eq!(
            parse_column_type("CHAR(8)").unwrap(),
            ColumnType::Char { length: 8 }
        );
        assert!(matches!(
            parse_column_type("nope"),
            Err(GatewayError::InvalidArgument(_))
        ));
        assert!(matches!(
            parse_column_type("DECIMAL(x)"),
            Err(GatewayError::InvalidArgument(_))
        ));
    }

    #[test]
    fn status_map_covers_every_variant() {
        use GatewayError::*;
        assert_eq!(status_for(&InvalidArgument("x".into())), StatusCode::BAD_REQUEST);
        assert_eq!(status_for(&Unauthenticated("x".into())), StatusCode::UNAUTHORIZED);
        assert_eq!(status_for(&Unauthorized("x".into())), StatusCode::FORBIDDEN);
        assert_eq!(
            status_for(&TableNotFound {
                database: "d".into(),
                table: "t".into()
            }),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for(&DatabaseNotFound { database: "d".into() }),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for(&TableAlreadyExists {
                database: "d".into(),
                table: "t".into()
            }),
            StatusCode::CONFLICT
        );
        assert_eq!(status_for(&SessionNotFound("s".into())), StatusCode::NOT_FOUND);
        assert_eq!(status_for(&OperationNotFound("o".into())), StatusCode::NOT_FOUND);
        assert_eq!(status_for(&Unsupported("x".into())), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(status_for(&Timeout("x".into())), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(status_for(&Cancelled("x".into())).as_u16(), 499);
        assert_eq!(status_for(&Backend("x".into())), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(status_for(&Internal("x".into())), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Test-only base64 encoder, used to build fixtures for the decode/parse
    /// tests without adding a base64 dependency.
    fn base64_encode(input: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in input.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            out.push(ALPHABET[(b[0] >> 2) as usize] as char);
            out.push(ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
            if chunk.len() > 1 {
                out.push(ALPHABET[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(ALPHABET[(b[2] & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }
}
