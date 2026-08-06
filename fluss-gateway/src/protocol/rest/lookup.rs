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

//! The two lookup endpoints.
//!
//! `POST .../records/lookup` resolves a batch of primary keys. A key that matches no row is an outcome
//! (`found: false`) and never a 404 — the *table* is the resource, and it exists. At most `[lookup] max_keys`
//! keys and `[lookup] max_key_bytes` of key values are accepted, both as input validation with a 400.
//!
//! `POST .../records/prefix-lookup` resolves bounded prefix ranges. At most `[lookup] max_prefixes` prefixes are
//! accepted (400 beyond that), and each prefix is cut at `[lookup] max_rows_per_prefix` with a `truncated` flag
//! rather than an error, because the native prefix lookuper returns every matching row and takes no row bound. A
//! prefix that matches nothing returns an empty row list, not a miss.
//!
//! The two endpoints validate to different depths, deliberately:
//!
//! * Point lookup checks `exact_lookup_supported` up front, so a log table is refused with a clear 501 before
//!   anything is dispatched.
//! * Prefix lookup checks the same table-kind precondition — a log table is a 501, because the client's
//!   `new_lookup` refuses one before it ever looks at the requested columns — and then only what it needs in
//!   order to type the request: that the named columns exist and that each prefix supplies them exactly once.
//!   Whether the columns form a legal bucket-key prefix is decided by the Fluss client while it builds its
//!   lookuper, and its refusal is returned as a 400 carrying the client's own message.
//!   `TableCapabilities::prefix_lookup_supported` is an advisory hint derived from table metadata alone and is
//!   deliberately *not* used to refuse a request: three of the client's six rules depend on the requested
//!   columns, and refusing on the flag would reject prefix lookups that in fact work.
//!
//! Neither endpoint has any 429 or capacity path. The gateway does not rate limit; these caps are input
//! validation and nothing else.

use crate::auth::Principal;
use crate::backend::model::{
    LookupKey, LookupOutcome, LookupOutcomeKind, PrefixLookupOutcome, PrefixLookupRequest,
    PrefixOutcomeKind, TableDescription, TableKind, TableRef,
};
use crate::error::GatewayError;
use crate::observability;
use crate::protocol::rest::json::{parse_key_value, record_batch_to_json_rows};
use crate::protocol::rest::limits::ensure_json_acceptable;
use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{
    RequestDeadline, RequestId, RestState, application_context, ensure_no_query, error_response,
    json_response, metric_cluster, parse_json_body,
};
use axum::Extension;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashSet;
use std::fmt;
use std::time::Instant;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Lookup routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(lookup))
        .routes(routes!(prefix_lookup))
}

/// One key object as it appeared on the wire, kept as an entry list so duplicate columns stay visible.
#[derive(Debug)]
pub struct KeyObject {
    pub entries: Vec<(String, JsonValue)>,
}

impl<'de> Deserialize<'de> for KeyObject {
    /// Deserializes through a visitor so duplicate object fields remain observable.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyObjectVisitor;

        impl<'de> Visitor<'de> for KeyObjectVisitor {
            type Value = KeyObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object of key columns")
            }

            /// Preserves each map entry in input order, including duplicate names.
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<KeyObject, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry::<String, JsonValue>()? {
                    entries.push(entry);
                }
                Ok(KeyObject { entries })
            }
        }

        deserializer.deserialize_map(KeyObjectVisitor)
    }
}

/// Request body of the point-lookup endpoint. `keys` is always an array, even for a single key.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LookupRequestBody {
    /// One object per key, each supplying every logical primary-key column exactly once.
    #[schema(value_type = Vec<Object>, min_items = 1)]
    pub keys: Vec<KeyObject>,
    /// When true the first per-key failure fails the whole request instead of being reported per key.
    #[serde(default)]
    pub fail_fast: bool,
}

/// Request body of the prefix-lookup endpoint.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PrefixLookupRequestBody {
    /// The columns every prefix supplies, in order.
    ///
    /// They must contain all of the table's partition keys, in any position, and — once those are removed — the
    /// table's bucket keys in exactly their declared order. Naming the partition keys first is the readable
    /// convention rather than a requirement. The Fluss client is the authority and explains any refusal in its
    /// own words.
    pub prefix_columns: Vec<String>,
    /// One object per prefix, each supplying every column of `prefix_columns` exactly once.
    #[schema(value_type = Vec<Object>, min_items = 1)]
    pub prefixes: Vec<KeyObject>,
    /// When true the first per-prefix failure fails the whole request instead of being reported per prefix.
    #[serde(default)]
    pub fail_fast: bool,
}

/// Error detail of one failed key or prefix.
#[derive(Debug, Serialize, ToSchema)]
pub struct LookupErrorResponse {
    pub code: String,
    pub message: String,
}

/// Outcome of one key, aligned with the request by `input_index`.
#[derive(Debug, Serialize, ToSchema)]
pub struct LookupResultResponse {
    pub input_index: usize,
    pub found: bool,
    /// The row in the full table schema, null when the key matched nothing or its lookup failed.
    #[schema(value_type = Option<Object>)]
    pub row: Option<JsonMap<String, JsonValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LookupErrorResponse>,
}

/// Response body of the point-lookup endpoint.
#[derive(Debug, Serialize, ToSchema)]
pub struct LookupResponse {
    /// Schema the returned rows are shaped by.
    pub schema_id: i32,
    pub results: Vec<LookupResultResponse>,
}

/// Outcome of one prefix, aligned with the request by `input_index`.
///
/// There is no `found` flag: a prefix names a range, and an empty range is a normal answer.
#[derive(Debug, Serialize, ToSchema)]
pub struct PrefixLookupResultResponse {
    pub input_index: usize,
    /// Number of rows returned, which equals `rows.len()`.
    pub row_count: usize,
    /// True when the per-prefix row cap cut the result short, so more rows exist than were returned.
    pub truncated: bool,
    /// The matching rows in the full table schema, in the order the storage returned them.
    #[schema(value_type = Vec<Object>)]
    pub rows: Vec<JsonMap<String, JsonValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LookupErrorResponse>,
}

/// Response body of the prefix-lookup endpoint.
#[derive(Debug, Serialize, ToSchema)]
pub struct PrefixLookupResponse {
    /// Schema the returned rows are shaped by.
    pub schema_id: i32,
    /// The cap each result was truncated at, echoed so a caller can tell a cap change from a data change.
    pub max_rows_per_prefix: usize,
    pub results: Vec<PrefixLookupResultResponse>,
}

/// Looks up a batch of primary keys, one aligned outcome per input key.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records/lookup",
    operation_id = "lookupRecords",
    tag = "lookup",
    description = "Resolves a batch of primary keys against a primary-key table. A key that matches no row is \
                   reported as `found: false`, never as an error.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = LookupRequestBody, content_type = "application/json"),
    responses(
        (status = 200, description = "Positionally aligned lookup outcomes", body = LookupResponse),
        (status = 400, description = "Malformed request, invalid key, or a request-size cap exceeded",
         body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response not acceptable", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 501, description = "Lookup unsupported for this table", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
#[allow(clippy::too_many_arguments)] // Axum extractors, one per request-scoped concern.
pub(crate) async fn lookup(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let table_ref = TableRef::new(database, table);
    let parsed = ensure_no_query(&uri)
        .and_then(|()| ensure_json_acceptable(&headers))
        .and_then(|()| parse_json_body(&headers, &body));
    let result = run_lookup(
        &state,
        &request_id,
        deadline,
        &principal,
        &cluster,
        &table_ref,
        parsed,
    )
    .await;

    let cluster = metric_cluster(&state, &cluster);
    let response = match &result {
        Ok(response) => {
            record_key_outcomes(&cluster, response);
            json_response(response).unwrap_or_else(|error| error_response(&error, &request_id))
        }
        Err(error) => error_response(error, &request_id),
    };
    observability::lookup_request(&cluster, outcome_label(&response), started.elapsed());
    response
}

/// Looks up bounded prefix ranges, one aligned outcome per input prefix.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records/prefix-lookup",
    operation_id = "prefixLookupRecords",
    tag = "lookup",
    description = "Resolves bounded prefix ranges against a primary-key table. The prefix columns must be the \
                   table's partition keys followed by its bucket keys; the Fluss client decides and explains any \
                   refusal, which is returned as a 400. Each prefix is truncated at the configured per-prefix row \
                   cap and flagged with `truncated`; a prefix that matches nothing returns an empty row list.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = PrefixLookupRequestBody, content_type = "application/json"),
    responses(
        (status = 200, description = "Positionally aligned prefix outcomes", body = PrefixLookupResponse),
        (status = 400, description = "Malformed request, a prefix the table does not support, or a \
                                      request-size cap exceeded", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response not acceptable", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 501, description = "Lookup unsupported for this table", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
#[allow(clippy::too_many_arguments)] // Axum extractors, one per request-scoped concern.
pub(crate) async fn prefix_lookup(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let table_ref = TableRef::new(database, table);
    let parsed = ensure_no_query(&uri)
        .and_then(|()| ensure_json_acceptable(&headers))
        .and_then(|()| parse_json_body(&headers, &body));
    let result = run_prefix_lookup(
        &state,
        &request_id,
        deadline,
        &principal,
        &cluster,
        &table_ref,
        parsed,
    )
    .await;

    let cluster = metric_cluster(&state, &cluster);
    let response = match &result {
        Ok(response) => {
            record_prefix_outcomes(&cluster, response);
            json_response(response).unwrap_or_else(|error| error_response(&error, &request_id))
        }
        Err(error) => error_response(error, &request_id),
    };
    observability::prefix_lookup_request(&cluster, outcome_label(&response), started.elapsed());
    response
}

/// The metric label of a finished request. There is no capacity or rejection class: the gateway never sheds load.
fn outcome_label(response: &Response) -> &'static str {
    if response.status().is_success() {
        "success"
    } else {
        "error"
    }
}

/// Records how the individual keys of one batch resolved.
fn record_key_outcomes(cluster: &str, response: &LookupResponse) {
    let found = response
        .results
        .iter()
        .filter(|result| result.found)
        .count();
    let errors = response
        .results
        .iter()
        .filter(|result| result.error.is_some())
        .count();
    let missing = response.results.len() - found - errors;
    observability::lookup_keys(cluster, "found", found);
    observability::lookup_keys(cluster, "not_found", missing);
    observability::lookup_keys(cluster, "error", errors);
}

/// Records how the individual prefixes of one batch resolved, plus the rows and truncations they produced.
fn record_prefix_outcomes(cluster: &str, response: &PrefixLookupResponse) {
    let errors = response
        .results
        .iter()
        .filter(|result| result.error.is_some())
        .count();
    observability::prefix_lookup_prefixes(cluster, "ok", response.results.len() - errors);
    observability::prefix_lookup_prefixes(cluster, "error", errors);
    observability::prefix_lookup_rows(
        cluster,
        response
            .results
            .iter()
            .map(|result| result.row_count)
            .sum::<usize>(),
        response
            .results
            .iter()
            .filter(|result| result.truncated)
            .count(),
    );
}

/// Validates the point-lookup request, runs the batch, and shapes the response.
async fn run_lookup(
    state: &RestState,
    request_id: &RequestId,
    deadline: RequestDeadline,
    principal: &Principal,
    cluster: &str,
    table: &TableRef,
    request: Result<LookupRequestBody, GatewayError>,
) -> Result<LookupResponse, GatewayError> {
    let request = request?;
    let context = application_context(request_id, deadline, principal, cluster)?;
    let description = state.application.describe_table(&context, table).await?;
    check_exact_lookup_supported(table, &description)?;
    check_count(
        "keys",
        request.keys.len(),
        state.lookup_limits.max_keys,
        "[lookup] max_keys",
    )?;

    let key_columns = columns_of(&description, &description.primary_keys, "primary key")?;
    let keys = request
        .keys
        .iter()
        .enumerate()
        .map(|(index, key)| parse_key("key", index, key, &key_columns))
        .collect::<Result<Vec<_>, _>>()?;
    check_key_bytes(state, &keys)?;

    let outcomes = state.application.lookup(&context, table, keys).await?;
    let results = outcomes
        .into_iter()
        .map(|outcome| to_lookup_result(outcome, request.fail_fast))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LookupResponse {
        schema_id: description.schema_id,
        results,
    })
}

/// Validates the prefix-lookup request, runs the batch, and shapes the response.
async fn run_prefix_lookup(
    state: &RestState,
    request_id: &RequestId,
    deadline: RequestDeadline,
    principal: &Principal,
    cluster: &str,
    table: &TableRef,
    request: Result<PrefixLookupRequestBody, GatewayError>,
) -> Result<PrefixLookupResponse, GatewayError> {
    let request = request?;
    let context = application_context(request_id, deadline, principal, cluster)?;
    let description = state.application.describe_table(&context, table).await?;
    check_table_has_a_primary_key(table, &description)?;
    check_count(
        "prefixes",
        request.prefixes.len(),
        state.lookup_limits.max_prefixes,
        "[lookup] max_prefixes",
    )?;
    check_prefix_columns(&request.prefix_columns)?;

    // Only the typing question is answered here: which columns, and of which types. Whether these columns form a
    // legal prefix for this table is the client's call, made while its lookuper is built.
    let prefix_columns = columns_of(&description, &request.prefix_columns, "prefix")?;
    let prefixes = request
        .prefixes
        .iter()
        .enumerate()
        .map(|(index, prefix)| parse_key("prefix", index, prefix, &prefix_columns))
        .collect::<Result<Vec<_>, _>>()?;
    check_key_bytes(state, &prefixes)?;

    let max_rows_per_prefix = state.lookup_limits.max_rows_per_prefix;
    let outcomes = state
        .application
        .prefix_lookup(
            &context,
            table,
            PrefixLookupRequest {
                prefix_columns: request.prefix_columns.clone(),
                prefixes,
                max_rows_per_prefix,
            },
        )
        .await?;
    let results = outcomes
        .into_iter()
        .map(|outcome| to_prefix_result(outcome, request.fail_fast))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PrefixLookupResponse {
        schema_id: description.schema_id,
        max_rows_per_prefix,
        results,
    })
}

/// Rejects tables without exact-lookup support with a reason that names the actual obstacle.
fn check_exact_lookup_supported(
    table: &TableRef,
    description: &TableDescription,
) -> Result<(), GatewayError> {
    if description.capabilities.exact_lookup_supported {
        return Ok(());
    }
    let reason = match description.kind {
        TableKind::Log => format!(
            "table `{table}` is a log table without a primary key, and lookup requires a primary-key table"
        ),
        TableKind::PrimaryKey => {
            format!("exact primary-key lookup is not supported for table `{table}`")
        }
    };
    Err(GatewayError::unsupported(reason))
}

/// Refuses a prefix lookup against a log table before anything is dispatched.
///
/// This is **not** the `prefix_lookup_supported` gate, which this endpoint deliberately does not apply. The
/// capability flag is advisory because three of the client's six rules depend on the requested prefix columns.
/// Table *kind* is different: `FlussTable::new_lookup` refuses a table without a primary key unconditionally,
/// before `lookup_by(...)` is even reached, so the client's answer here is fixed by table metadata alone and
/// pre-empting it only improves the message. It is the same 501 the point-lookup path gives, for the same reason.
fn check_table_has_a_primary_key(
    table: &TableRef,
    description: &TableDescription,
) -> Result<(), GatewayError> {
    match description.kind {
        TableKind::Log => Err(GatewayError::unsupported(format!(
            "table `{table}` is a log table without a primary key, and lookup is only supported for \
             primary-key tables"
        ))),
        TableKind::PrimaryKey => Ok(()),
    }
}

/// Enforces the configured bound on the number of keys or prefixes in one request.
///
/// This is input validation, not rate limiting: it bounds one request's work, never the caller's request rate, so
/// it answers 400 rather than 413 or 429.
fn check_count(field: &str, count: usize, max: usize, setting: &str) -> Result<(), GatewayError> {
    if count == 0 {
        return Err(GatewayError::invalid_argument(format!(
            "`{field}` must contain at least one entry"
        )));
    }
    if count > max {
        return Err(GatewayError::invalid_argument(format!(
            "request has {count} {field} but the limit is {max} ({setting})"
        )));
    }
    Ok(())
}

/// Enforces the configured bound on the total estimated key bytes of one request.
fn check_key_bytes(state: &RestState, keys: &[LookupKey]) -> Result<(), GatewayError> {
    let total: u64 = keys.iter().map(LookupKey::size_estimate).sum();
    let max_key_bytes = state.lookup_limits.max_key_bytes;
    if total > max_key_bytes {
        return Err(GatewayError::invalid_argument(format!(
            "key values total about {total} bytes but the limit is {max_key_bytes} bytes \
             ([lookup] max_key_bytes)"
        )));
    }
    Ok(())
}

/// Rejects a prefix column list that cannot name anything, before the table is consulted.
fn check_prefix_columns(columns: &[String]) -> Result<(), GatewayError> {
    if columns.is_empty() {
        return Err(GatewayError::invalid_argument(
            "`prefix_columns` must name at least one column",
        ));
    }
    let mut seen = HashSet::with_capacity(columns.len());
    for column in columns {
        if !seen.insert(column.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "`prefix_columns` names column `{column}` more than once"
            )));
        }
    }
    Ok(())
}

/// Resolves the named columns to their Arrow types, in the order they were named.
fn columns_of(
    description: &TableDescription,
    names: &[String],
    role: &str,
) -> Result<Vec<(String, arrow::datatypes::DataType)>, GatewayError> {
    names
        .iter()
        .map(|name| {
            let field = description
                .arrow_schema
                .field_with_name(name)
                .map_err(|_| {
                    // A primary-key column that is absent from the schema is a gateway or server inconsistency; a
                    // caller-named prefix column that is absent is simply a bad request.
                    if role == "prefix" {
                        GatewayError::invalid_argument(format!(
                            "column `{name}` does not exist in table `{}`",
                            description.table
                        ))
                    } else {
                        GatewayError::internal(format!(
                            "{role} column `{name}` is missing from the table schema"
                        ))
                    }
                })?;
            Ok((name.clone(), field.data_type().clone()))
        })
        .collect()
}

/// Validates one key or prefix object and parses its values into declared column order.
///
/// Every declared column must appear exactly once and no other column may appear. Violations name the offending
/// column and the position of the offending entry.
fn parse_key(
    kind: &str,
    index: usize,
    key: &KeyObject,
    columns: &[(String, arrow::datatypes::DataType)],
) -> Result<LookupKey, GatewayError> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(key.entries.len());
    for (column, _) in &key.entries {
        if !seen.insert(column.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "{kind} {index}: column `{column}` appears more than once"
            )));
        }
        if !columns.iter().any(|(name, _)| name == column) {
            return Err(GatewayError::invalid_argument(format!(
                "{kind} {index}: column `{column}` is not one of the {kind} columns"
            )));
        }
    }
    let values = columns
        .iter()
        .map(|(name, data_type)| {
            let value = key
                .entries
                .iter()
                .find(|(column, _)| column == name)
                .map(|(_, value)| value)
                .ok_or_else(|| {
                    GatewayError::invalid_argument(format!(
                        "{kind} {index}: missing column `{name}`"
                    ))
                })?;
            parse_key_value(name, data_type, value).map_err(|error| {
                GatewayError::new(error.kind(), format!("{kind} {index}: {}", error.message()))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LookupKey::new(values))
}

/// Shapes one point-lookup outcome, failing the whole request on a per-key error when `fail_fast` is set.
fn to_lookup_result(
    outcome: LookupOutcome,
    fail_fast: bool,
) -> Result<LookupResultResponse, GatewayError> {
    let input_index = outcome.input_index;
    match outcome.kind {
        LookupOutcomeKind::Found(batch) => {
            let mut rows = record_batch_to_json_rows(&batch)?;
            let row = rows
                .pop()
                .ok_or_else(|| GatewayError::internal("a found outcome carried no row"))?;
            Ok(LookupResultResponse {
                input_index,
                found: true,
                row: Some(row),
                error: None,
            })
        }
        LookupOutcomeKind::NotFound => Ok(LookupResultResponse {
            input_index,
            found: false,
            row: None,
            error: None,
        }),
        LookupOutcomeKind::Error(error) => {
            if fail_fast {
                return Err(GatewayError::new(
                    error.kind(),
                    format!("lookup failed for key {input_index}: {}", error.message()),
                ));
            }
            Ok(LookupResultResponse {
                input_index,
                found: false,
                row: None,
                error: Some(LookupErrorResponse {
                    code: error.code().to_string(),
                    message: error.message().to_string(),
                }),
            })
        }
    }
}

/// Shapes one prefix outcome, failing the whole request on a per-prefix error when `fail_fast` is set.
fn to_prefix_result(
    outcome: PrefixLookupOutcome,
    fail_fast: bool,
) -> Result<PrefixLookupResultResponse, GatewayError> {
    let input_index = outcome.input_index;
    match outcome.kind {
        PrefixOutcomeKind::Rows { batch, truncated } => {
            let rows = record_batch_to_json_rows(&batch)?;
            Ok(PrefixLookupResultResponse {
                input_index,
                row_count: rows.len(),
                truncated,
                rows,
                error: None,
            })
        }
        PrefixOutcomeKind::Error(error) => {
            if fail_fast {
                return Err(GatewayError::new(
                    error.kind(),
                    format!(
                        "prefix lookup failed for prefix {input_index}: {}",
                        error.message()
                    ),
                ));
            }
            Ok(PrefixLookupResultResponse {
                input_index,
                row_count: 0,
                truncated: false,
                rows: Vec::new(),
                error: Some(LookupErrorResponse {
                    code: error.code().to_string(),
                    message: error.message().to_string(),
                }),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use axum::response::Response;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tower::ServiceExt;

    const USERS: &str = "/v1/clusters/default/databases/fluss/tables/users/records/lookup";
    const ORDERS_LOOKUP: &str = "/v1/clusters/default/databases/fluss/tables/orders/records/lookup";
    const EVENTS: &str = "/v1/clusters/default/databases/fluss/tables/events/records/lookup";
    const PREFIX_USERS: &str =
        "/v1/clusters/default/databases/fluss/tables/users/records/prefix-lookup";
    const PREFIX_ORDERS: &str =
        "/v1/clusters/default/databases/fluss/tables/orders/records/prefix-lookup";
    const PREFIX_EVENTS: &str =
        "/v1/clusters/default/databases/fluss/tables/events/records/prefix-lookup";
    const PREFIX_SESSIONS: &str =
        "/v1/clusters/default/databases/fluss/tables/sessions/records/prefix-lookup";

    /// A table the Fluss client would accept for prefix lookup on `["region", "user_id"]`.
    ///
    /// The fixture catalog has none: `users` and `orders` both have bucket keys equal to their physical primary
    /// key, which the client refuses in favour of a point lookup (rule 6). This one has physical primary key
    /// `["user_id", "item_id"]` and bucket keys `["user_id"]` — a *strict* prefix — so all six rules pass.
    const SESSIONS: &str = r#"{
        "table_name": "sessions",
        "columns": [
            {"name": "region", "data_type": {"type": "STRING", "nullable": false}},
            {"name": "user_id", "data_type": {"type": "BIGINT", "nullable": false}},
            {"name": "item_id", "data_type": {"type": "BIGINT", "nullable": false}},
            {"name": "note", "data_type": {"type": "STRING", "nullable": true}}
        ],
        "primary_key": {"columns": ["region", "user_id", "item_id"]},
        "partitioned_by": ["region"],
        "distribution": {"bucket_count": 3, "bucket_keys": ["user_id"]}
    }"#;

    fn app() -> Router {
        test_support::app(Arc::new(TestBackend::new()))
    }

    async fn post_to(app: &Router, path: &str, body: &str) -> Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn post(path: &str, body: Value) -> Response {
        post_to(&app(), path, &body.to_string()).await
    }

    async fn body_json(response: Response) -> Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    /// Builds a router whose fixture catalog also contains the prefix-lookupable `sessions` table.
    async fn app_with_sessions() -> Router {
        let app = app();
        let created = post_to(
            &app,
            "/v1/clusters/default/databases/fluss/tables",
            SESSIONS,
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED, "fixture table");
        app
    }

    #[tokio::test]
    async fn a_batch_answers_hits_and_misses_in_input_order() {
        let response = post(USERS, json!({"keys": [{"id": 7}, {"id": 404}, {"id": 8}]})).await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;

        assert_eq!(json["schema_id"], 1);
        assert_eq!(json["results"][0]["input_index"], 0);
        assert_eq!(json["results"][0]["found"], true);
        assert_eq!(json["results"][0]["row"]["id"], 7);
        assert_eq!(json["results"][1]["input_index"], 1);
        assert_eq!(json["results"][2]["found"], true);
        assert_eq!(json["results"][2]["row"]["id"], 8);
    }

    /// A key that matches nothing is data, not a missing resource: 200 with `found: false`, never 404.
    #[tokio::test]
    async fn a_miss_is_an_outcome_and_never_a_not_found() {
        let response = post(USERS, json!({"keys": [{"id": 404}]})).await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["results"][0]["found"], false);
        assert_eq!(json["results"][0]["row"], Value::Null);
        assert!(json["results"][0].get("error").is_none(), "{json}");
    }

    #[tokio::test]
    async fn a_per_key_failure_leaves_the_rest_of_the_batch_intact() {
        let response = post(USERS, json!({"keys": [{"id": 1}, {"id": 500}, {"id": 2}]})).await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;

        assert_eq!(json["results"][0]["found"], true);
        assert_eq!(json["results"][1]["found"], false);
        assert_eq!(json["results"][1]["error"]["code"], "unavailable");
        assert_eq!(json["results"][2]["found"], true);
    }

    #[tokio::test]
    async fn fail_fast_turns_a_per_key_failure_into_a_request_failure() {
        let response = post(
            USERS,
            json!({"keys": [{"id": 1}, {"id": 500}], "fail_fast": true}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "unavailable");
        assert!(
            json["error"]["message"].as_str().unwrap().contains("key 1"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn a_partitioned_table_is_looked_up_by_its_logical_primary_key() {
        let response = post(
            ORDERS_LOOKUP,
            json!({"keys": [{"id": 3, "region": "eu"}, {"region": "us", "id": 404}]}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["results"][0]["found"], true);
        assert_eq!(json["results"][0]["row"]["region"], "eu");
        assert_eq!(json["results"][1]["found"], false);
    }

    #[tokio::test]
    async fn more_keys_than_the_cap_is_a_bad_request_and_never_a_413_or_429() {
        // The fixture router caps a request at eight keys.
        let keys: Vec<Value> = (0..9).map(|id| json!({"id": id})).collect();
        let response = post(USERS, json!({"keys": keys})).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "invalid_argument");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("limit is 8"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn an_oversized_key_is_a_bad_request() {
        let big = "x".repeat(300);
        let response = post(ORDERS_LOOKUP, json!({"keys": [{"region": big, "id": 1}]})).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "invalid_argument");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("max_key_bytes"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn an_empty_batch_and_malformed_keys_are_rejected_by_column() {
        assert_eq!(
            post(USERS, json!({"keys": []})).await.status(),
            StatusCode::BAD_REQUEST
        );

        let missing = post(ORDERS_LOOKUP, json!({"keys": [{"region": "eu"}]})).await;
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(missing).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("`id`")
        );

        let extra = post(USERS, json!({"keys": [{"id": 1, "name": "Ada"}]})).await;
        assert_eq!(extra.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(extra).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("`name`")
        );

        let duplicated = post_to(&app(), USERS, r#"{"keys": [{"id": 1, "id": 2}]}"#).await;
        assert_eq!(duplicated.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(duplicated).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("appears more than once")
        );

        let mistyped = post(USERS, json!({"keys": [{"id": "not a number"}]})).await;
        assert_eq!(mistyped.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn lookup_on_a_log_table_is_unsupported_and_on_a_missing_table_is_not_found() {
        let log = post(EVENTS, json!({"keys": [{"ts": 1}]})).await;
        assert_eq!(log.status(), StatusCode::NOT_IMPLEMENTED);
        assert!(
            body_json(log).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("log table")
        );

        let missing = post(
            "/v1/clusters/default/databases/fluss/tables/nope/records/lookup",
            json!({"keys": [{"id": 1}]}),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn lookup_metrics_stay_bounded_and_never_report_capacity() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);

        let response = post(USERS, json!({"keys": [{"id": 404}, {"id": 1}]})).await;
        assert_eq!(response.status(), StatusCode::OK);

        let output = handle.render();
        assert!(output.contains("fluss_gateway_lookup_requests_total"));
        assert!(output.contains("fluss_gateway_lookup_keys_total"));
        assert!(output.contains("cluster=\"default\""));
        assert!(output.contains("result=\"found\""));
        assert!(output.contains("result=\"not_found\""));
        assert!(!output.contains("database="), "{output}");
        assert!(!output.contains("table="), "{output}");
        assert!(!output.contains("capacity"), "{output}");
    }

    // ---- prefix lookup ----

    #[tokio::test]
    async fn a_prefix_lookup_on_a_partitioned_table_returns_aligned_row_sets() {
        let app = app_with_sessions().await;
        let response = post_to(
            &app,
            PREFIX_SESSIONS,
            &json!({
                "prefix_columns": ["region", "user_id"],
                "prefixes": [
                    {"region": "eu", "user_id": 3},
                    {"region": "us", "user_id": 0}
                ]
            })
            .to_string(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["max_rows_per_prefix"], 100);
        assert_eq!(json["results"][0]["input_index"], 0);
        assert_eq!(json["results"][0]["row_count"], 3);
        assert_eq!(json["results"][0]["rows"].as_array().unwrap().len(), 3);
        assert_eq!(json["results"][0]["truncated"], false);
        assert_eq!(json["results"][0]["rows"][0]["region"], "eu");
        assert_eq!(json["results"][0]["rows"][0]["user_id"], "3");

        // An empty range is a normal answer, not a miss and not a 404.
        assert_eq!(json["results"][1]["row_count"], 0);
        assert_eq!(json["results"][1]["rows"], json!([]));
        assert_eq!(json["results"][1]["truncated"], false);
    }

    #[tokio::test]
    async fn a_prefix_beyond_the_row_cap_is_truncated_and_flagged() {
        let app = app_with_sessions().await;
        let response = post_to(
            &app,
            PREFIX_SESSIONS,
            &json!({
                "prefix_columns": ["region", "user_id"],
                "prefixes": [{"region": "eu", "user_id": 150}]
            })
            .to_string(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        // The fixture router caps a prefix at 100 rows; the fixture backend has 150 to give.
        assert_eq!(json["results"][0]["row_count"], 100);
        assert_eq!(json["results"][0]["truncated"], true);
        assert_eq!(json["max_rows_per_prefix"], 100);
    }

    #[tokio::test]
    async fn more_prefixes_than_the_cap_is_a_bad_request() {
        let app = app_with_sessions().await;
        // The fixture router caps a request at four prefixes.
        let prefixes: Vec<Value> = (0..5)
            .map(|id| json!({"region": "eu", "user_id": id}))
            .collect();
        let response = post_to(
            &app,
            PREFIX_SESSIONS,
            &json!({"prefix_columns": ["region", "user_id"], "prefixes": prefixes}).to_string(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "invalid_argument");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("max_prefixes"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn a_per_prefix_failure_leaves_the_rest_of_the_batch_intact() {
        let app = app_with_sessions().await;
        let response = post_to(
            &app,
            PREFIX_SESSIONS,
            &json!({
                "prefix_columns": ["region", "user_id"],
                "prefixes": [{"region": "eu", "user_id": 2}, {"region": "eu", "user_id": 500}]
            })
            .to_string(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["results"][0]["row_count"], 2);
        assert_eq!(json["results"][1]["error"]["code"], "unavailable");
        assert_eq!(json["results"][1]["row_count"], 0);
    }

    #[tokio::test]
    async fn a_prefix_column_the_table_does_not_have_is_a_bad_request() {
        let response = post(
            PREFIX_ORDERS,
            json!({"prefix_columns": ["nope"], "prefixes": [{"nope": 1}]}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("does not exist in table")
        );
    }

    #[tokio::test]
    async fn an_empty_or_repeated_prefix_column_list_is_a_bad_request() {
        assert_eq!(
            post(
                PREFIX_ORDERS,
                json!({"prefix_columns": [], "prefixes": [{"region": "eu"}]}),
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );

        let repeated = post(
            PREFIX_ORDERS,
            json!({"prefix_columns": ["region", "region"], "prefixes": [{"region": "eu"}]}),
        )
        .await;
        assert_eq!(repeated.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(repeated).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("more than once")
        );
    }

    /// Rules 2 to 6 of the client's `validate_prefix_lookup` reach the caller as a 400 carrying the client's own
    /// explanation — the gateway never paraphrases and never pre-empts the verdict.
    ///
    /// Rule 1 is the exception, and not because the gateway treats it differently: `FlussTable::new_lookup`
    /// refuses a table without a primary key *before* `lookup_by(...)` is reached, so the client never evaluates
    /// its own rule-1 branch on this path. Its real answer is an unsupported operation, which is a 501, and that
    /// is what is asserted below.
    ///
    /// Rules 4 and 6 are reachable from the standard fixture catalog; rules 2, 3 and 5 need tables the fixture
    /// does not define, so they are created here first. The refusals are produced by the fixture backend's
    /// client emulation (`backend::testing::lookup`), while the native path gets them from the client itself and
    /// maps them identically — see `backend::native_lookup::tests`.
    #[tokio::test]
    async fn every_client_prefix_rule_surfaces_with_the_client_message() {
        let app = app_with_sessions().await;
        let tables = "/v1/clusters/default/databases/fluss/tables";

        // Rule 2 needs a primary-key table with no bucket keys at all. Real Fluss defaults a table's bucket keys
        // to its physical primary key, so this exact shape only arises through the fixture catalog; a real
        // cluster would answer the same request under rule 6 instead. Both are a 400 in the client's own words.
        let no_buckets = r#"{
            "table_name": "no_buckets",
            "columns": [{"name": "id", "data_type": {"type": "INT", "nullable": false}}],
            "primary_key": {"columns": ["id"]}
        }"#;
        assert_eq!(
            post_to(&app, tables, no_buckets).await.status(),
            StatusCode::CREATED
        );

        // Rule 3 needs bucket keys that are not a leading subset of the physical primary keys.
        let unordered = r#"{
            "table_name": "unordered",
            "columns": [
                {"name": "a", "data_type": {"type": "INT", "nullable": false}},
                {"name": "b", "data_type": {"type": "INT", "nullable": false}}
            ],
            "primary_key": {"columns": ["a", "b"]},
            "distribution": {"bucket_count": 3, "bucket_keys": ["b"]}
        }"#;
        assert_eq!(
            post_to(&app, tables, unordered).await.status(),
            StatusCode::CREATED
        );

        let cases: Vec<(&str, &str, Value, StatusCode, &str)> = vec![
            (
                "1: log table, pre-empted by the client's own primary-key check",
                PREFIX_EVENTS,
                json!({"prefix_columns": ["ts"], "prefixes": [{"ts": 1}]}),
                StatusCode::NOT_IMPLEMENTED,
                "only supported for primary-key tables",
            ),
            (
                "2: no bucket keys",
                "/v1/clusters/default/databases/fluss/tables/no_buckets/records/prefix-lookup",
                json!({"prefix_columns": ["id"], "prefixes": [{"id": 1}]}),
                StatusCode::BAD_REQUEST,
                "because it has no bucket keys",
            ),
            (
                "3: bucket keys are not a prefix of the physical primary keys",
                "/v1/clusters/default/databases/fluss/tables/unordered/records/prefix-lookup",
                json!({"prefix_columns": ["a"], "prefixes": [{"a": 1}]}),
                StatusCode::BAD_REQUEST,
                "is not a prefix subset of the physical primary keys",
            ),
            (
                "4: lookup columns omit a partition field",
                PREFIX_ORDERS,
                json!({"prefix_columns": ["id"], "prefixes": [{"id": 1}]}),
                StatusCode::BAD_REQUEST,
                "must contain all partition fields",
            ),
            (
                "5: lookup columns are not the bucket keys in order",
                PREFIX_SESSIONS,
                json!({
                    "prefix_columns": ["region", "item_id"],
                    "prefixes": [{"region": "eu", "item_id": 1}]
                }),
                StatusCode::BAD_REQUEST,
                "must contain all bucket keys",
            ),
            (
                "6: lookup columns are the whole physical primary key",
                PREFIX_USERS,
                json!({"prefix_columns": ["id"], "prefixes": [{"id": 1}]}),
                StatusCode::BAD_REQUEST,
                "Please use primary key lookup",
            ),
        ];

        for (rule, path, body, status, expected) in cases {
            let response = post_to(&app, path, &body.to_string()).await;
            assert_eq!(response.status(), status, "rule {rule}");
            let json = body_json(response).await;
            let message = json["error"]["message"].as_str().unwrap_or_default();
            assert!(
                message.contains(expected),
                "rule {rule}: expected the client's own wording, got `{message}`"
            );
        }
    }

    /// A partitioned table whose bucket keys equal its physical primary key is refused by the *client*, not by
    /// the capability flag — the flag is advisory and reports this table as prefix-lookupable.
    #[tokio::test]
    async fn the_capability_flag_alone_never_decides_a_prefix_request() {
        let app = app_with_sessions().await;
        let described = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/clusters/default/databases/fluss/tables/orders")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(described.status(), StatusCode::OK);
        assert_eq!(
            body_json(described).await["capabilities"]["prefix_lookup_supported"],
            true,
            "the advisory hint says yes for this table"
        );

        // `sessions` is the mirror case: the request the client accepts succeeds.
        let accepted = post_to(
            &app,
            PREFIX_SESSIONS,
            &json!({
                "prefix_columns": ["region", "user_id"],
                "prefixes": [{"region": "eu", "user_id": 1}]
            })
            .to_string(),
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn prefix_lookup_metrics_record_rows_and_truncations() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);

        let app = app_with_sessions().await;
        let response = post_to(
            &app,
            PREFIX_SESSIONS,
            &json!({
                "prefix_columns": ["region", "user_id"],
                "prefixes": [{"region": "eu", "user_id": 150}]
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let output = handle.render();
        assert!(output.contains("fluss_gateway_prefix_lookup_requests_total"));
        assert!(output.contains("fluss_gateway_prefix_lookup_rows_total"));
        assert!(output.contains("fluss_gateway_prefix_lookup_truncations_total"));
        assert!(!output.contains("capacity"), "{output}");
    }

    // ---- shared request hygiene ----

    #[tokio::test]
    async fn both_endpoints_reject_stray_queries_unacceptable_types_and_bad_json() {
        for path in [USERS, PREFIX_ORDERS] {
            let query = app()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("{path}?foo=bar"))
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(query.status(), StatusCode::BAD_REQUEST, "{path}");

            let unacceptable = app()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::ACCEPT, "text/plain")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(unacceptable.status(), StatusCode::NOT_ACCEPTABLE, "{path}");

            let untyped = app()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                untyped.status(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{path}"
            );

            let malformed = post_to(&app(), path, "{not json").await;
            assert_eq!(malformed.status(), StatusCode::BAD_REQUEST, "{path}");

            let unknown_field = post_to(&app(), path, r#"{"keys": [], "nope": 1}"#).await;
            assert_eq!(unknown_field.status(), StatusCode::BAD_REQUEST, "{path}");
        }
    }

    /// Both lookup routes are part of the generated contract.
    ///
    /// The prefix-lookup assertion is the deliberate inverse of the prior branch's
    /// `prefix_lookup_is_not_exposed`: the endpoint that used to be absent is now part of the published API.
    #[tokio::test]
    async fn both_lookup_routes_are_published_in_the_openapi_document() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let document = body_json(response).await;

        let base = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records";
        assert_eq!(
            document["paths"][format!("{base}/lookup")]["post"]["operationId"],
            "lookupRecords"
        );
        assert_eq!(
            document["paths"][format!("{base}/prefix-lookup")]["post"]["operationId"],
            "prefixLookupRecords",
            "prefix lookup is exposed: {document}"
        );
        assert!(
            document["components"]["schemas"]["PrefixLookupResponse"].is_object(),
            "{document}"
        );
        // No endpoint declares a capacity response: the gateway does not rate limit.
        assert!(
            document["paths"][format!("{base}/lookup")]["post"]["responses"]
                .get("429")
                .is_none()
        );
        assert!(
            document["paths"][format!("{base}/prefix-lookup")]["post"]["responses"]
                .get("429")
                .is_none()
        );
    }
}
