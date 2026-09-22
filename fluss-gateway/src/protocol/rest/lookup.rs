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

//! Bounded batched primary-key and bucket-key-prefix lookups.

use crate::backend::{
    FlussBackend, LookupOutcome, LookupOutcomeKind, LookupRequest, PrefixLookupOutcome,
    PrefixLookupOutcomeKind, PrefixLookupRequest,
};
use crate::error::{ErrorEnvelope, GatewayError, GatewayResult};
use crate::protocol::rest::codec::{
    EncodedRow, RowDecodeError, RowShape, SchemaDecoder, record_batch_to_json_rows,
};
use crate::protocol::rest::{
    RestState, collect_body, ensure_json_acceptable, error_response, json_response, request_id,
    resolve_cluster, validate_json_content_type,
};
use axum::extract::{Path, Request, State};
use axum::response::Response;
use fluss::metadata::{DataType, RowType, TableInfo, TablePath};
use fluss::row::{Datum, GenericRow};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::HashSet;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Registers the primary-key lookup endpoint and its OpenAPI contract.
pub fn point_routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(lookup))
}

/// Registers the prefix lookup endpoint and its OpenAPI contract.
pub fn prefix_routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(prefix_lookup))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub(crate) struct LookupBody<T> {
    /// Complete logical primary keys, one JSON object per input.
    #[schema(value_type = Vec<Object>, min_items = 1)]
    pub keys: Vec<T>,
    /// Columns returned in each row, in caller order. Omitted means all columns.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub(crate) struct PrefixLookupBody<T> {
    /// Prefix objects containing partition columns followed by bucket-key columns.
    #[schema(value_type = Vec<Object>, min_items = 1)]
    pub prefixes: Vec<T>,
    /// Columns returned in each row, in caller order. Omitted means all columns.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Per-prefix row cap. Values above the server maximum are clamped.
    #[serde(default)]
    #[schema(minimum = 1)]
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LookupEntryResponse {
    #[schema(value_type = Object)]
    pub key: Box<RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<Object>>)]
    pub rows: Option<Vec<EncodedRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LookupResponse {
    pub schema_id: i32,
    pub results: Vec<LookupEntryResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PrefixLookupEntryResponse {
    #[schema(value_type = Object)]
    pub prefix: Box<RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<Object>>)]
    pub rows: Option<Vec<EncodedRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PrefixLookupResponse {
    pub schema_id: i32,
    #[schema(minimum = 1)]
    pub max_rows_per_prefix: u32,
    pub results: Vec<PrefixLookupEntryResponse>,
}

#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/lookup",
    operation_id = "lookupRecords",
    summary = "Looks up a batch of primary keys",
    tag = "records",
    description = "Returns one ordered outcome per input key. A missing key is a successful entry \
                   with `rows: []`; a failure isolated to one key is reported on that entry.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = LookupBody<serde_json::Value>, content_type = "application/json"),
    responses(
        (status = 200, description = "Positionally aligned primary-key lookup outcomes", body = LookupResponse),
        (status = 400, description = "Malformed key, invalid projection, unsupported table shape, or key-count/byte limit exceeded", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or table", body = ErrorEnvelope),
        (status = 406, description = "A JSON response is not acceptable", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured byte limit", body = ErrorEnvelope),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelope),
        (status = 429, description = "The point-lookup concurrency gate is exhausted", body = ErrorEnvelope),
        (status = 500, description = "Backend or backend-contract failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or table format", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope)
    )
)]
pub(crate) async fn lookup(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    run_lookup(&state, &cluster, database, table, request)
        .await
        .unwrap_or_else(|error| error_response(&error, &request_id))
}

#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/prefix-lookup",
    operation_id = "prefixLookupRecords",
    summary = "Looks up bounded bucket-key prefixes",
    tag = "records",
    description = "Derives the prefix columns from table metadata: partition columns followed by \
                   bucket-key columns. Every prefix has an explicit effective row cap and one ordered \
                   outcome; an empty range returns `rows: []`, and truncation is reported deterministically.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = PrefixLookupBody<serde_json::Value>, content_type = "application/json"),
    responses(
        (status = 200, description = "Positionally aligned bounded prefix lookup outcomes", body = PrefixLookupResponse),
        (status = 400, description = "Malformed prefix, invalid projection or limit, unsupported table shape, or prefix-count/key-byte limit exceeded", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or table", body = ErrorEnvelope),
        (status = 406, description = "A JSON response is not acceptable", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured byte limit", body = ErrorEnvelope),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelope),
        (status = 429, description = "The prefix-lookup concurrency gate is exhausted", body = ErrorEnvelope),
        (status = 500, description = "Backend or backend-contract failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or table format", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope)
    )
)]
pub(crate) async fn prefix_lookup(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    run_prefix_lookup(&state, &cluster, database, table, request)
        .await
        .unwrap_or_else(|error| error_response(&error, &request_id))
}

async fn run_lookup(
    state: &RestState,
    cluster: &str,
    database: String,
    table: String,
    request: Request,
) -> GatewayResult<Response> {
    validate_json_content_type(request.headers())?;
    ensure_json_acceptable(request.headers())?;
    let (backend, ctx) = resolve_cluster(state, &request, cluster)?;
    let bytes = collect_body(request).await?;
    let body: LookupBody<&RawValue> = parse_body("lookup", &bytes)?;
    check_count("keys", body.keys.len(), state.lookup_max_keys)?;

    let path = TablePath::new(database, table);
    let prepared = prepare_point_with_refresh(
        backend.as_ref(),
        &ctx,
        &path,
        &body,
        state.lookup_max_key_bytes,
    )
    .await?;
    let outcomes = backend.lookup(&ctx, prepared.request).await?;
    let results = shape_point_results(prepared.echoes, outcomes, prepared.projection.as_deref())?;
    json_response(&LookupResponse {
        schema_id: prepared.schema_id,
        results,
    })
}

async fn run_prefix_lookup(
    state: &RestState,
    cluster: &str,
    database: String,
    table: String,
    request: Request,
) -> GatewayResult<Response> {
    validate_json_content_type(request.headers())?;
    ensure_json_acceptable(request.headers())?;
    let (backend, ctx) = resolve_cluster(state, &request, cluster)?;
    let bytes = collect_body(request).await?;
    let body: PrefixLookupBody<&RawValue> = parse_body("prefix lookup", &bytes)?;
    check_count(
        "prefixes",
        body.prefixes.len(),
        state.prefix_lookup_max_prefixes,
    )?;
    let max_rows_per_prefix =
        effective_prefix_limit(body.limit, state.prefix_lookup_max_rows_per_prefix)?;

    let path = TablePath::new(database, table);
    let prepared = prepare_prefix_with_refresh(
        backend.as_ref(),
        &ctx,
        &path,
        &body,
        max_rows_per_prefix,
        state.lookup_max_key_bytes,
    )
    .await?;
    let outcomes = backend.prefix_lookup(&ctx, prepared.request).await?;
    let results = shape_prefix_results(
        prepared.echoes,
        outcomes,
        prepared.projection.as_deref(),
        max_rows_per_prefix as usize,
    )?;
    json_response(&PrefixLookupResponse {
        schema_id: prepared.schema_id,
        max_rows_per_prefix,
        results,
    })
}

fn parse_body<'a, T>(kind: &str, bytes: &'a [u8]) -> GatewayResult<T>
where
    T: Deserialize<'a>,
{
    serde_json::from_slice(bytes).map_err(|error| {
        GatewayError::invalid_argument(format!(
            "the request body is not a valid {kind} body: {error}"
        ))
    })
}

fn check_count(field: &str, count: usize, max: u32) -> GatewayResult<()> {
    if count == 0 {
        return Err(GatewayError::invalid_argument(format!(
            "`{field}` must contain at least one entry"
        )));
    }
    if count > max as usize {
        return Err(GatewayError::invalid_argument(format!(
            "request has {count} {field} but the configured limit is {max}"
        )));
    }
    Ok(())
}

fn check_key_bytes(keys: &[GenericRow<'_>], max: u64) -> GatewayResult<()> {
    let bytes = keys
        .iter()
        .flat_map(|key| &key.values)
        .try_fold(0_u64, |total, value| {
            total.checked_add(key_value_size(value)?).ok_or_else(|| {
                GatewayError::invalid_argument("lookup key bytes overflow the size counter")
            })
        })?;
    if bytes > max {
        return Err(GatewayError::invalid_argument(format!(
            "lookup key values use about {bytes} bytes but the configured limit is {max}"
        )));
    }
    Ok(())
}

fn key_value_size(value: &Datum<'_>) -> GatewayResult<u64> {
    Ok(match value {
        Datum::Null => 0,
        Datum::Bool(_) | Datum::Int8(_) => 1,
        Datum::Int16(_) => 2,
        Datum::Int32(_) | Datum::Float32(_) | Datum::Date(_) | Datum::Time(_) => 4,
        Datum::Int64(_) | Datum::Float64(_) => 8,
        Datum::String(value) => value.len() as u64,
        Datum::Blob(value) => value.len() as u64,
        Datum::Decimal(_) => 16,
        Datum::TimestampNtz(_) | Datum::TimestampLtz(_) => 12,
        Datum::Array(_) | Datum::Map(_) | Datum::Row(_) => {
            return Err(GatewayError::invalid_argument(
                "ARRAY, MAP, and ROW columns cannot be used as lookup keys",
            ));
        }
    })
}

fn effective_prefix_limit(limit: Option<u32>, server_max: u32) -> GatewayResult<u32> {
    match limit {
        Some(0) => Err(GatewayError::invalid_argument(
            "`limit` must be greater than zero",
        )),
        Some(limit) => Ok(limit.min(server_max)),
        None => Ok(server_max),
    }
}

struct PreparedPoint {
    request: LookupRequest,
    echoes: Vec<Box<RawValue>>,
    projection: Option<Vec<String>>,
    schema_id: i32,
}

struct PreparedPrefix {
    request: PrefixLookupRequest,
    echoes: Vec<Box<RawValue>>,
    projection: Option<Vec<String>>,
    schema_id: i32,
}

#[derive(Debug)]
struct PreflightError {
    error: GatewayError,
    refreshable: bool,
}

type DecodedKeys = (Vec<GenericRow<'static>>, Vec<Box<RawValue>>);

impl PreflightError {
    fn fixed(error: GatewayError) -> Self {
        Self {
            error,
            refreshable: false,
        }
    }

    fn schema(error: GatewayError) -> Self {
        Self {
            error,
            refreshable: true,
        }
    }
}

async fn prepare_point_with_refresh(
    backend: &dyn FlussBackend,
    ctx: &crate::backend::context::RequestContext,
    path: &TablePath,
    body: &LookupBody<&RawValue>,
    max_key_bytes: u64,
) -> GatewayResult<PreparedPoint> {
    let snapshot = backend.table_info(ctx, path).await?;
    let identity = (snapshot.table_id, snapshot.schema_id);
    match prepare_point(snapshot, body, max_key_bytes) {
        Ok(prepared) => Ok(prepared),
        Err(failure) if failure.refreshable => {
            let refreshed = backend.describe_table(ctx, path).await?;
            if (refreshed.table_id, refreshed.schema_id) == identity {
                return Err(failure.error);
            }
            prepare_point(refreshed, body, max_key_bytes).map_err(|failure| failure.error)
        }
        Err(failure) => Err(failure.error),
    }
}

async fn prepare_prefix_with_refresh(
    backend: &dyn FlussBackend,
    ctx: &crate::backend::context::RequestContext,
    path: &TablePath,
    body: &PrefixLookupBody<&RawValue>,
    max_rows_per_prefix: u32,
    max_key_bytes: u64,
) -> GatewayResult<PreparedPrefix> {
    let snapshot = backend.table_info(ctx, path).await?;
    let identity = (snapshot.table_id, snapshot.schema_id);
    match prepare_prefix(snapshot, body, max_rows_per_prefix, max_key_bytes) {
        Ok(prepared) => Ok(prepared),
        Err(failure) if failure.refreshable => {
            let refreshed = backend.describe_table(ctx, path).await?;
            if (refreshed.table_id, refreshed.schema_id) == identity {
                return Err(failure.error);
            }
            prepare_prefix(refreshed, body, max_rows_per_prefix, max_key_bytes)
                .map_err(|failure| failure.error)
        }
        Err(failure) => Err(failure.error),
    }
}

fn prepare_point(
    table: TableInfo,
    body: &LookupBody<&RawValue>,
    max_key_bytes: u64,
) -> Result<PreparedPoint, PreflightError> {
    if !table.has_primary_key() {
        return Err(PreflightError::schema(GatewayError::invalid_argument(
            format!(
                "table `{}` is a log table; primary-key lookup requires a primary-key table",
                table.table_path
            ),
        )));
    }
    let projection = validate_projection(&table, body.columns.as_deref())?;
    let key_columns = table.get_primary_keys().clone();
    let key_type = project_row_type(&table, &key_columns, "primary-key")?;
    validate_key_types(&key_type)?;
    let (keys, echoes) = decode_keys("key", &key_type, &body.keys)?;
    check_key_bytes(&keys, max_key_bytes).map_err(PreflightError::fixed)?;
    let schema_id = table.schema_id;
    let request = LookupRequest::new(table, keys).map_err(PreflightError::fixed)?;
    Ok(PreparedPoint {
        request,
        echoes,
        projection,
        schema_id,
    })
}

fn prepare_prefix(
    table: TableInfo,
    body: &PrefixLookupBody<&RawValue>,
    max_rows_per_prefix: u32,
    max_key_bytes: u64,
) -> Result<PreparedPrefix, PreflightError> {
    if !table.has_primary_key() {
        return Err(PreflightError::schema(GatewayError::invalid_argument(
            format!(
                "table `{}` is a log table; prefix lookup requires a primary-key table",
                table.table_path
            ),
        )));
    }
    let projection = validate_projection(&table, body.columns.as_deref())?;
    let prefix_columns = prefix_columns(&table)?;
    let prefix_type = project_row_type(&table, &prefix_columns, "prefix")?;
    validate_key_types(&prefix_type)?;
    let (prefixes, echoes) = decode_keys("prefix", &prefix_type, &body.prefixes)?;
    check_key_bytes(&prefixes, max_key_bytes).map_err(PreflightError::fixed)?;
    let schema_id = table.schema_id;
    let request = PrefixLookupRequest::new(
        table,
        prefix_columns,
        prefixes,
        max_rows_per_prefix as usize,
    )
    .map_err(PreflightError::fixed)?;
    Ok(PreparedPrefix {
        request,
        echoes,
        projection,
        schema_id,
    })
}

fn validate_key_types(row_type: &RowType) -> Result<(), PreflightError> {
    for field in row_type.fields() {
        if matches!(
            field.data_type(),
            DataType::Array(_) | DataType::Map(_) | DataType::Row(_)
        ) {
            return Err(PreflightError::fixed(GatewayError::invalid_argument(
                format!(
                    "column `{}` has type {}, which cannot be used as a lookup key",
                    field.name(),
                    field.data_type()
                ),
            )));
        }
    }
    Ok(())
}

fn prefix_columns(table: &TableInfo) -> Result<Vec<String>, PreflightError> {
    let physical_primary_keys = table.get_physical_primary_keys();
    let bucket_keys = table.get_bucket_keys();
    if bucket_keys.is_empty() {
        return Err(PreflightError::schema(GatewayError::invalid_argument(
            format!(
                "prefix lookup is not supported for table `{}` because it has no bucket keys",
                table.table_path
            ),
        )));
    }
    if !physical_primary_keys.starts_with(bucket_keys) {
        return Err(PreflightError::schema(GatewayError::invalid_argument(
            format!(
                "prefix lookup is not supported for table `{}` because its bucket keys are not a prefix of its physical primary key",
                table.table_path
            ),
        )));
    }
    if physical_primary_keys == bucket_keys {
        return Err(PreflightError::schema(GatewayError::invalid_argument(
            format!(
                "prefix lookup columns equal the full physical primary key of table `{}`; use primary-key lookup instead",
                table.table_path
            ),
        )));
    }
    let mut columns = table.get_partition_keys().to_vec();
    for bucket_key in bucket_keys {
        if !columns.contains(bucket_key) {
            columns.push(bucket_key.clone());
        }
    }
    Ok(columns)
}

fn validate_projection(
    table: &TableInfo,
    columns: Option<&[String]>,
) -> Result<Option<Vec<String>>, PreflightError> {
    let Some(columns) = columns else {
        return Ok(None);
    };
    if columns.is_empty() {
        return Err(PreflightError::fixed(GatewayError::invalid_argument(
            "`columns` must contain at least one column when present",
        )));
    }
    let known: HashSet<&str> = table
        .row_type()
        .fields()
        .iter()
        .map(|field| field.name())
        .collect();
    let mut seen = HashSet::with_capacity(columns.len());
    for column in columns {
        if !seen.insert(column.as_str()) {
            return Err(PreflightError::fixed(GatewayError::invalid_argument(
                format!("projection column `{column}` appears more than once"),
            )));
        }
        if !known.contains(column.as_str()) {
            return Err(PreflightError::schema(GatewayError::invalid_argument(
                format!(
                    "projection column `{column}` does not exist in table `{}`",
                    table.table_path
                ),
            )));
        }
    }
    Ok(Some(columns.to_vec()))
}

fn project_row_type(
    table: &TableInfo,
    columns: &[String],
    role: &str,
) -> Result<RowType, PreflightError> {
    table
        .row_type()
        .project_with_field_names(columns)
        .map_err(|error| {
            PreflightError::schema(GatewayError::internal(format!(
                "{role} columns disagree with table metadata: {error}"
            )))
        })
}

fn decode_keys(
    kind: &str,
    row_type: &RowType,
    raw_keys: &[&RawValue],
) -> Result<DecodedKeys, PreflightError> {
    let decoder = SchemaDecoder::new(row_type.clone()).map_err(PreflightError::fixed)?;
    let mut keys = Vec::with_capacity(raw_keys.len());
    let mut echoes = Vec::with_capacity(raw_keys.len());
    for (index, raw) in raw_keys.iter().enumerate() {
        let label = format!("{kind} {index}");
        keys.push(
            decoder
                .decode_row(&label, raw.get().as_bytes(), RowShape::Complete)
                .map_err(preflight_decode_error)?,
        );
        echoes.push((*raw).to_owned());
    }
    Ok((keys, echoes))
}

fn preflight_decode_error(error: RowDecodeError) -> PreflightError {
    let refreshable = error.is_schema_mismatch();
    PreflightError {
        error: error.into_gateway_error(),
        refreshable,
    }
}

fn shape_point_results(
    echoes: Vec<Box<RawValue>>,
    outcomes: Vec<LookupOutcome>,
    projection: Option<&[String]>,
) -> GatewayResult<Vec<LookupEntryResponse>> {
    validate_alignment("lookup key", echoes.len(), &outcomes, |outcome| {
        outcome.input_index
    })?;
    echoes
        .into_iter()
        .zip(outcomes)
        .map(|(key, outcome)| match outcome.kind {
            LookupOutcomeKind::Found(batch) => {
                if batch.num_rows() != 1 {
                    return Err(GatewayError::internal(format!(
                        "the backend returned {} rows for a primary-key hit",
                        batch.num_rows()
                    )));
                }
                Ok(LookupEntryResponse {
                    key,
                    rows: Some(project_rows(
                        record_batch_to_json_rows(&batch)?,
                        projection,
                    )?),
                    error_code: None,
                    message: None,
                })
            }
            LookupOutcomeKind::NotFound => Ok(LookupEntryResponse {
                key,
                rows: Some(Vec::new()),
                error_code: None,
                message: None,
            }),
            LookupOutcomeKind::Error(error) => Ok(LookupEntryResponse {
                key,
                rows: None,
                error_code: Some(error.code().to_string()),
                message: Some(error.message().to_string()),
            }),
        })
        .collect()
}

fn shape_prefix_results(
    echoes: Vec<Box<RawValue>>,
    outcomes: Vec<PrefixLookupOutcome>,
    projection: Option<&[String]>,
    max_rows_per_prefix: usize,
) -> GatewayResult<Vec<PrefixLookupEntryResponse>> {
    validate_alignment("prefix", echoes.len(), &outcomes, |outcome| {
        outcome.input_index
    })?;
    echoes
        .into_iter()
        .zip(outcomes)
        .map(|(prefix, outcome)| match outcome.kind {
            PrefixLookupOutcomeKind::Rows { batch, truncated } => {
                if batch.num_rows() > max_rows_per_prefix {
                    return Err(GatewayError::internal(format!(
                        "the backend returned {} prefix rows above the limit of {max_rows_per_prefix}",
                        batch.num_rows()
                    )));
                }
                Ok(PrefixLookupEntryResponse {
                    prefix,
                    truncated: Some(truncated),
                    rows: Some(project_rows(
                        record_batch_to_json_rows(&batch)?,
                        projection,
                    )?),
                    error_code: None,
                    message: None,
                })
            }
            PrefixLookupOutcomeKind::Error(error) => Ok(PrefixLookupEntryResponse {
                prefix,
                truncated: None,
                rows: None,
                error_code: Some(error.code().to_string()),
                message: Some(error.message().to_string()),
            }),
        })
        .collect()
}

fn validate_alignment<T>(
    name: &str,
    expected: usize,
    outcomes: &[T],
    input_index: impl Fn(&T) -> usize,
) -> GatewayResult<()> {
    if outcomes.len() != expected {
        return Err(GatewayError::internal(format!(
            "the backend returned {} outcomes for {expected} {name} inputs",
            outcomes.len()
        )));
    }
    for (expected_index, outcome) in outcomes.iter().enumerate() {
        let actual = input_index(outcome);
        if actual != expected_index {
            return Err(GatewayError::internal(format!(
                "the backend returned {name} outcome index {actual} at position {expected_index}"
            )));
        }
    }
    Ok(())
}

fn project_rows(
    rows: Vec<EncodedRow>,
    projection: Option<&[String]>,
) -> GatewayResult<Vec<EncodedRow>> {
    let Some(projection) = projection else {
        return Ok(rows);
    };
    rows.into_iter()
        .map(|row| row.project(projection))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::fake::{
        FakeFlussBackend, Operation, log_table_info, prefix_table_info, users_table_info,
    };
    use crate::protocol::rest::{RestOptions, test_support};
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::Value as JsonValue;
    use std::sync::Arc;
    use tower::ServiceExt;

    const POINT: &str = "/v1/clusters/default/databases/fluss/tables/users/lookup";
    const PREFIX: &str = "/v1/clusters/default/databases/fluss/tables/items/prefix-lookup";

    fn catalog() -> Arc<FakeFlussBackend> {
        Arc::new(
            FakeFlussBackend::new()
                .with_table(users_table_info(1))
                .with_table(log_table_info(1))
                .with_table(prefix_table_info(1)),
        )
    }

    fn app_with(backend: Arc<FakeFlussBackend>, options: RestOptions) -> axum::Router {
        let state = test_support::state_with_backend_and_options(backend, &options);
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &options)
    }

    fn batch(rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                ArrowDataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from_iter_values(0..rows as i32))],
        )
        .unwrap()
    }

    fn echo(value: &str) -> Box<RawValue> {
        serde_json::from_str(value).unwrap()
    }

    async fn post(
        app: &axum::Router,
        path: &str,
        body: &str,
    ) -> (StatusCode, JsonValue, axum::http::HeaderMap) {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap(), headers)
    }

    #[tokio::test]
    async fn point_hits_misses_failures_and_projection_stay_aligned() {
        let app = app_with(catalog(), test_support::test_options());
        let (status, body, _) = post(
            &app,
            POINT,
            r#"{"keys":[{"id":7},{"id":404},{"id":500}],"columns":["name","id"]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["key"], serde_json::json!({"id": 7}));
        assert_eq!(
            body["results"][0]["rows"],
            serde_json::json!([{"name": null, "id": 7}])
        );
        assert_eq!(body["results"][1]["rows"], serde_json::json!([]));
        assert_eq!(body["results"][2]["error_code"], "unavailable");
        assert!(body["results"][2].get("rows").is_none());
    }

    #[tokio::test]
    async fn prefix_results_are_clamped_bounded_and_independently_failed() {
        let mut options = test_support::test_options();
        options.prefix_lookup_max_rows_per_prefix = 2;
        let app = app_with(catalog(), options);
        let (status, body, _) = post(
            &app,
            PREFIX,
            r#"{"prefixes":[{"user_id":3},{"user_id":0},{"user_id":150},{"user_id":500}],"limit":20,
                "columns":["user_id","item_id"]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["max_rows_per_prefix"], 2);
        assert_eq!(body["results"][0]["rows"].as_array().unwrap().len(), 2);
        assert_eq!(body["results"][0]["truncated"], true);
        assert_eq!(body["results"][1]["rows"], serde_json::json!([]));
        assert_eq!(body["results"][1]["truncated"], false);
        assert_eq!(body["results"][2]["truncated"], true);
        assert_eq!(body["results"][3]["error_code"], "unavailable");
        assert!(body["results"][3].get("rows").is_none());
    }

    #[tokio::test]
    async fn invalid_shapes_and_admission_limits_fail_before_lookup() {
        let backend = catalog();
        backend.fail_once(
            Operation::Lookup,
            GatewayError::unavailable("point lookup reached the backend"),
        );
        backend.fail_once(
            Operation::PrefixLookup,
            GatewayError::unavailable("prefix lookup reached the backend"),
        );
        let mut options = test_support::test_options();
        options.lookup_max_keys = 1;
        options.prefix_lookup_max_prefixes = 1;
        let app = app_with(Arc::clone(&backend), options);
        for (path, body, message) in [
            (POINT, r#"{"keys":[]}"#, "at least one"),
            (POINT, r#"{"keys":[{"id":1},{"id":2}]}"#, "configured limit"),
            (
                POINT,
                r#"{"keys":[{"id":1,"unknown":"long value"}]}"#,
                "unknown column",
            ),
            (POINT, r#"{"keys":[1]}"#, "JSON object"),
            (POINT, r#"{"keys":[{}]}"#, "is required"),
            (POINT, r#"{"keys":[{"id":1}],"columns":[]}"#, "at least one"),
            (
                POINT,
                r#"{"keys":[{"id":1}],"columns":["unknown"]}"#,
                "does not exist",
            ),
            (
                "/v1/clusters/default/databases/fluss/tables/applog/lookup",
                r#"{"keys":[{"ts":"1"}]}"#,
                "primary-key table",
            ),
            (PREFIX, r#"{"prefixes":[]}"#, "at least one"),
            (
                PREFIX,
                r#"{"prefixes":[{"user_id":1},{"user_id":2}]}"#,
                "configured limit",
            ),
            (
                PREFIX,
                r#"{"prefixes":[{"user_id":1,"item_id":2}]}"#,
                "unknown column",
            ),
            (
                PREFIX,
                r#"{"prefixes":[{"user_id":1}],"limit":0}"#,
                "greater than zero",
            ),
            (
                "/v1/clusters/default/databases/fluss/tables/users/prefix-lookup",
                r#"{"prefixes":[{"id":1}]}"#,
                "use primary-key lookup",
            ),
        ] {
            let (status, response, _) = post(&app, path, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {response}");
            assert!(
                response["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains(message),
                "{body}: {response}"
            );
        }
        assert_eq!(
            post(&app, POINT, r#"{"keys":[{"id":1}]}"#).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            post(&app, PREFIX, r#"{"prefixes":[{"user_id":1}],"limit":1}"#)
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        let mut options = test_support::test_options();
        options.lookup_max_key_bytes = 3;
        let app = app_with(catalog(), options);
        let (status, response, _) = post(&app, POINT, r#"{"keys":[{"id":1}]}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("about 4 bytes"),
            "{response}"
        );
    }

    #[tokio::test]
    async fn point_and_prefix_requests_have_independent_non_queueing_gates() {
        let backend = catalog();
        backend.delay_lookups(Some(std::time::Duration::from_millis(100)), None);
        let mut options = test_support::test_options();
        options.lookup_max_concurrent_requests = 1;
        options.prefix_lookup_max_concurrent_requests = 1;
        let app = app_with(backend, options);

        let first = post(&app, POINT, r#"{"keys":[{"id":1}]}"#);
        tokio::pin!(first);
        assert!(futures_util::poll!(first.as_mut()).is_pending());

        let (point_status, point_body, point_headers) =
            post(&app, POINT, r#"{"keys":[{"id":2}]}"#).await;
        assert_eq!(point_status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(point_body["error"]["code"], "resource_exhausted");
        assert_eq!(point_headers["retry-after"], "1");

        let (prefix_status, _, _) =
            post(&app, PREFIX, r#"{"prefixes":[{"user_id":1}],"limit":1}"#).await;
        assert_eq!(prefix_status, StatusCode::OK);
        assert_eq!(first.await.0, StatusCode::OK);
    }

    #[tokio::test(start_paused = true)]
    async fn admission_permits_are_released_after_deadline_and_cancellation() {
        use std::time::Duration;

        for (path, body) in [
            (POINT, r#"{"keys":[{"id":1}]}"#),
            (PREFIX, r#"{"prefixes":[{"user_id":1}]}"#),
        ] {
            let backend = catalog();
            backend.delay_lookups(Some(Duration::from_secs(1)), Some(Duration::from_secs(1)));
            let mut options = test_support::test_options();
            options.request_timeout = Duration::from_millis(50);
            options.lookup_max_concurrent_requests = 1;
            options.prefix_lookup_max_concurrent_requests = 1;
            let app = app_with(Arc::clone(&backend), options);
            let mut first = Box::pin(post(&app, path, body));
            assert!(futures_util::poll!(first.as_mut()).is_pending());
            assert_eq!(
                post(&app, path, body).await.0,
                StatusCode::TOO_MANY_REQUESTS
            );
            let (status, response, headers) = first.await;
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
            assert_eq!(response["error"]["code"], "timeout");
            assert!(headers.contains_key("x-request-id"));

            // A disconnected caller drops its handler and the admission guard together.
            let mut disconnected = Box::pin(post(&app, path, body));
            assert!(futures_util::poll!(disconnected.as_mut()).is_pending());
            drop(disconnected);
            backend.delay_lookups(None, None);
            assert_eq!(post(&app, path, body).await.0, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn lookup_routes_enforce_body_media_and_query_contracts() {
        for (path, body) in [
            (POINT, r#"{"keys":[{"id":1}]}"#),
            (PREFIX, r#"{"prefixes":[{"user_id":1}]}"#),
        ] {
            let mut options = test_support::test_options();
            options.max_body_bytes = 64;
            let app = app_with(catalog(), options);
            // No Content-Length: the body collector must still enforce the byte cap.
            let oversized = format!("{body}{}", " ".repeat(64));
            assert_eq!(
                post(&app, path, &oversized).await.0,
                StatusCode::PAYLOAD_TOO_LARGE
            );
            assert_eq!(
                post(&app, &format!("{path}?limit=1"), body).await.0,
                StatusCode::BAD_REQUEST
            );
            for (content_type, accept, expected) in [
                (
                    "text/plain",
                    "application/json",
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                ),
                ("application/json", "text/plain", StatusCode::NOT_ACCEPTABLE),
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        HttpRequest::builder()
                            .method("POST")
                            .uri(path)
                            .header("content-type", content_type)
                            .header("accept", accept)
                            .body(Body::from(body))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected);
            }
        }
    }

    #[tokio::test]
    async fn key_validation_rejects_the_entire_batch_before_execution() {
        let backend = catalog();
        backend.fail_once(Operation::Lookup, GatewayError::unavailable("not executed"));
        backend.fail_once(
            Operation::PrefixLookup,
            GatewayError::unavailable("not executed"),
        );
        let app = app_with(backend, test_support::test_options());
        for (path, body) in [
            (POINT, r#"{"keys":[{"id":1},{"id":null}]}"#),
            (POINT, r#"{"keys":[{"id":1},{"id":2,"id":3}]}"#),
            (POINT, r#"{"keys":[{"id":1}],"columns":["id","id"]}"#),
            (PREFIX, r#"{"prefixes":[{"user_id":1},{"user_id":null}]}"#),
            (
                PREFIX,
                r#"{"prefixes":[{"user_id":1},{"user_id":2,"user_id":3}]}"#,
            ),
            (
                PREFIX,
                r#"{"prefixes":[{"user_id":1}],"columns":["note","note"]}"#,
            ),
            (PREFIX, r#"{"prefixes":[{"user_id":1}],"limit":-1}"#),
        ] {
            assert_eq!(
                post(&app, path, body).await.0,
                StatusCode::BAD_REQUEST,
                "{body}"
            );
        }
        assert_eq!(
            post(&app, POINT, r#"{"keys":[{"id":1}]}"#).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            post(&app, PREFIX, r#"{"prefixes":[{"user_id":1}]}"#)
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn key_byte_limit_counts_the_whole_batch_and_allows_the_boundary() {
        let mut options = test_support::test_options();
        options.lookup_max_key_bytes = 8;
        let app = app_with(catalog(), options);
        for (path, field, column) in [(POINT, "keys", "id"), (PREFIX, "prefixes", "user_id")] {
            let entry = format!(r#"{{"{column}":1}}"#);
            let accepted = format!(r#"{{"{field}":[{entry},{entry}]}}"#);
            let rejected = format!(r#"{{"{field}":[{entry},{entry},{entry}]}}"#);
            assert_eq!(post(&app, path, &accepted).await.0, StatusCode::OK);
            assert_eq!(post(&app, path, &rejected).await.0, StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn stale_table_capabilities_are_refreshed_before_lookup() {
        let backend = catalog();
        for table in [users_table_info(1), prefix_table_info(1)] {
            // A cached log table with the same path predates recreation as a KV table.
            let mut cached = log_table_info(0);
            cached.table_path = table.table_path;
            backend.cache_table(cached);
        }
        let app = app_with(backend, test_support::test_options());
        for (path, body) in [
            (POINT, r#"{"keys":[{"id":1}],"columns":["name"]}"#),
            (PREFIX, r#"{"prefixes":[{"user_id":1}],"columns":["note"]}"#),
        ] {
            let (status, response, _) = post(&app, path, body).await;
            assert_eq!(status, StatusCode::OK, "{response}");
            assert_eq!(response["schema_id"], 1);
            assert_eq!(response["results"][0]["rows"].as_array().unwrap().len(), 1);
        }
    }

    #[test]
    fn partition_columns_and_logical_key_order_are_derived_from_metadata() {
        use fluss::metadata::{DataTypes, Schema as FlussSchema, TableDescriptor};

        let schema = FlussSchema::builder()
            .column("user_id", DataTypes::int())
            .column("day", DataTypes::string())
            .column("item_id", DataTypes::int())
            .primary_key(["user_id", "day", "item_id"])
            .unwrap()
            .build()
            .unwrap();
        let table = |bucket: &str| {
            TableInfo::of(
                TablePath::new("fluss", "partitioned_items"),
                1,
                1,
                TableDescriptor::builder()
                    .schema(schema.clone())
                    .partitioned_by(vec!["day"])
                    .distributed_by(Some(2), vec![bucket.to_string()])
                    .build()
                    .unwrap(),
                0,
                0,
            )
        };
        let info = table("user_id");
        let body: LookupBody<&RawValue> =
            serde_json::from_str(r#"{"keys":[{"item_id":2,"day":"2026-09-21","user_id":1}]}"#)
                .unwrap();
        let prepared = prepare_point(info.clone(), &body, 1024).unwrap();
        let (_, keys) = prepared.request.into_parts();
        assert_eq!(
            keys[0].values,
            vec![Datum::Int32(1), Datum::from("2026-09-21"), Datum::Int32(2)]
        );

        let body: PrefixLookupBody<&RawValue> =
            serde_json::from_str(r#"{"prefixes":[{"user_id":1,"day":"2026-09-21"}]}"#).unwrap();
        let prepared = prepare_prefix(info.clone(), &body, 2, 1024).unwrap();
        let (_, columns, prefixes, limit) = prepared.request.into_parts();
        assert_eq!(columns, ["day", "user_id"]);
        assert_eq!(
            prefixes[0].values,
            vec![Datum::from("2026-09-21"), Datum::Int32(1)]
        );
        assert_eq!(limit, 2);

        let body: PrefixLookupBody<&RawValue> =
            serde_json::from_str(r#"{"prefixes":[{"user_id":1}]}"#).unwrap();
        assert!(prepare_prefix(info, &body, 2, 1024).is_err());
        assert!(
            prefix_columns(&table("item_id"))
                .unwrap_err()
                .error
                .message()
                .contains("not a prefix")
        );
    }

    #[test]
    fn misaligned_backend_outcomes_violate_the_contract() {
        let echoes = vec![echo("{}"), echo("{}")];
        let reordered = vec![
            LookupOutcome {
                input_index: 1,
                kind: LookupOutcomeKind::NotFound,
            },
            LookupOutcome {
                input_index: 0,
                kind: LookupOutcomeKind::NotFound,
            },
        ];
        assert!(
            shape_point_results(echoes, reordered, None)
                .unwrap_err()
                .message()
                .contains("at position 0")
        );

        let missing = vec![LookupOutcome {
            input_index: 0,
            kind: LookupOutcomeKind::NotFound,
        }];
        assert!(
            shape_point_results(vec![echo("{}"), echo("{}")], missing, None)
                .unwrap_err()
                .message()
                .contains("1 outcomes for 2")
        );
    }

    #[test]
    fn backend_row_shapes_and_limits_are_verified() {
        let point = vec![LookupOutcome {
            input_index: 0,
            kind: LookupOutcomeKind::Found(batch(0)),
        }];
        assert!(
            shape_point_results(vec![echo("{}")], point, None)
                .unwrap_err()
                .message()
                .contains("0 rows")
        );

        let prefix = vec![PrefixLookupOutcome {
            input_index: 0,
            kind: PrefixLookupOutcomeKind::Rows {
                batch: batch(2),
                truncated: false,
            },
        }];
        assert!(
            shape_prefix_results(vec![echo("{}")], prefix, None, 1)
                .unwrap_err()
                .message()
                .contains("above the limit")
        );

        let point = vec![LookupOutcome {
            input_index: 0,
            kind: LookupOutcomeKind::Found(batch(1)),
        }];
        assert!(
            shape_point_results(vec![echo("{}")], point, Some(&["missing".to_string()]))
                .unwrap_err()
                .message()
                .contains("missing projected column")
        );
    }

    #[test]
    fn echoed_decimal_numbers_keep_their_exact_lexeme() {
        let outcomes = vec![LookupOutcome {
            input_index: 0,
            kind: LookupOutcomeKind::NotFound,
        }];
        let response = shape_point_results(
            vec![echo(r#"{"amount":99999999999999999999999999999999999999}"#)],
            outcomes,
            None,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&response[0]).unwrap(),
            r#"{"key":{"amount":99999999999999999999999999999999999999},"rows":[]}"#
        );
    }
}
