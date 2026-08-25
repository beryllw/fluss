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

//! FIP-49 catalog mutation endpoints.

use crate::backend::FlussBackend;
use crate::backend::context::RequestContext;
use crate::error::{ErrorEnvelope, GatewayError, GatewayResult};
use crate::protocol::rest::datatype::ColumnDataType;
use crate::protocol::rest::metadata::{
    DatabaseResponse, PartitionResponse, TableResponse, resolve_cluster,
};
use crate::protocol::rest::{
    RestState, error_response, json_response, json_response_with_status, parse_json_body,
    request_id,
};
use axum::body::Bytes;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fluss::metadata::{
    AddColumn, AlterConfig, AlterConfigOpType, AlterTableChanges, ColumnPositionType, DataType,
    JsonSerde, PARTITION_SPEC_SEPARATOR, PartitionSpec, ResolvedPartitionSpec, Schema,
    TableDescriptor, TableInfo, TablePath,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// DDL routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(create_database))
        .routes(routes!(drop_database))
        .routes(routes!(create_table))
        .routes(routes!(alter_table))
        .routes(routes!(drop_table))
        .routes(routes!(create_partition))
        .routes(routes!(drop_partition))
}

/// Body of `POST /v1/clusters/{cluster}/databases`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateDatabaseBody {
    pub database: String,
}

/// One column; top-level nullability belongs here, not inside `data_type`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnBody {
    pub name: String,
    pub data_type: ColumnDataType,
    /// Defaults to true. A primary-key column must not be declared nullable.
    pub nullable: Option<bool>,
    pub comment: Option<String>,
}

/// The bucket distribution of a table definition.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DistributionBody {
    #[schema(minimum = 1)]
    pub bucket_count: i32,
    pub bucket_keys: Vec<String>,
}

/// Body of `POST /v1/clusters/{cluster}/databases/{database}/tables`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateTableBody {
    pub table_name: String,
    pub columns: Vec<ColumnBody>,
    /// Present makes it a primary-key table, absent a log table (FIP-49).
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub partitioned_by: Vec<String>,
    pub distribution: Option<DistributionBody>,
    #[serde(default)]
    pub configs: HashMap<String, String>,
    pub comment: Option<String>,
    /// Build and validate the definition locally without creating anything.
    #[serde(default)]
    pub validate_only: bool,
}

/// The result of a successful local `validate_only` dry run.
#[derive(Debug, Serialize, ToSchema)]
pub struct ValidateOnlyResponse {
    pub validate_only: bool,
    pub database: String,
    pub table: String,
    pub column_count: usize,
    pub primary_key: Vec<String>,
}

/// One supported table change.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableChangeBody {
    /// An added column must be nullable: existing rows have no value for it.
    AddColumn {
        name: String,
        data_type: ColumnDataType,
        comment: Option<String>,
    },
    SetConfig {
        key: String,
        value: String,
    },
    ResetConfig {
        key: String,
    },
}

/// Body of `PATCH /v1/clusters/{cluster}/databases/{database}/tables/{table}`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AlterTableBody {
    /// Schema and config changes must be submitted in separate requests.
    pub changes: Vec<TableChangeBody>,
}

/// Body of `POST .../tables/{table}/partitions`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePartitionBody {
    pub partition: HashMap<String, String>,
}

/// Creates one database.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases",
    operation_id = "createDatabase",
    tag = "ddl",
    params(("cluster" = String, Path, description = "Configured cluster ID")),
    request_body(content = CreateDatabaseBody, content_type = "application/json"),
    responses(
        (status = 201, description = "The created database", body = DatabaseResponse,
            headers(("Location" = String, description = "Created database URL"))),
        (status = 400, description = "Malformed body or invalid definition", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster", body = ErrorEnvelope),
        (status = 409, description = "The database already exists", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 415, description = "The body is not JSON", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn create_database(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    request: Request,
) -> Response {
    let (request_id, prepared) = split(&state, &cluster, request).await;
    let result = async {
        let (backend, ctx, headers, bytes) = prepared?;
        let body: CreateDatabaseBody = parse_json_body(&headers, &bytes)?;
        let name = body.database;
        native_name("database name", &name, true)?;
        backend.create_database(&ctx, &name).await?;
        let location = database_location(&cluster, &name);
        created_response(&DatabaseResponse { database: name }, &location)
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Drops one empty database.
#[utoipa::path(
    delete,
    path = "/v1/clusters/{cluster}/databases/{database}",
    operation_id = "dropDatabase",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
    ),
    responses(
        (status = 204, description = "The database was dropped"),
        (status = 400, description = "Unsupported query parameter", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or database", body = ErrorEnvelope),
        (status = 409, description = "The database still holds tables", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn drop_database(
    State(state): State<RestState>,
    Path((cluster, database)): Path<(String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = resolve_cluster(&state, &request, &cluster);
    let result = async {
        let (backend, ctx) = prepared?;
        backend.drop_database(&ctx, &database).await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Creates one table, or validates its definition without creating it.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables",
    operation_id = "createTable",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
    ),
    request_body(content = CreateTableBody, content_type = "application/json"),
    responses(
        (status = 200, description = "Dry run: fluss-rs accepted the definition and nothing was created", body = ValidateOnlyResponse),
        (status = 201, description = "The created table", body = TableResponse,
            headers(("Location" = String, description = "Created table URL"))),
        (status = 400, description = "Malformed body or invalid definition", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or database", body = ErrorEnvelope),
        (status = 409, description = "The table already exists", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 415, description = "The body is not JSON", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn create_table(
    State(state): State<RestState>,
    Path((cluster, database)): Path<(String, String)>,
    request: Request,
) -> Response {
    let (request_id, prepared) = split(&state, &cluster, request).await;
    let result = async {
        let (backend, ctx, headers, bytes) = prepared?;
        let body: CreateTableBody = parse_json_body(&headers, &bytes)?;
        let validate_only = body.validate_only;
        let table = TablePath::new(database, body.table_name.clone());
        let descriptor = table_descriptor(table.database(), body)?;
        if validate_only {
            // TODO: Forward dry runs when Fluss exposes server-side validation through fluss-rs.
            let schema = descriptor.schema();
            return json_response(&ValidateOnlyResponse {
                validate_only: true,
                database: table.database().to_string(),
                table: table.table().to_string(),
                column_count: schema.columns().len(),
                primary_key: schema
                    .primary_key()
                    .map(|key| key.column_names().to_vec())
                    .unwrap_or_default(),
            });
        }
        let created = backend.create_table(&ctx, &table, &descriptor).await?;
        created_response(
            &TableResponse::from(&created),
            &table_location(&cluster, &table),
        )
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Applies one group of schema or config changes to a table.
#[utoipa::path(
    patch,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
    operation_id = "alterTable",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("table" = String, Path, description = "Table name"),
    ),
    request_body(content = AlterTableBody, content_type = "application/json"),
    responses(
        (status = 200, description = "The altered table", body = TableResponse),
        (status = 400, description = "Malformed body or invalid change", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster, database, or table", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 415, description = "The body is not JSON", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn alter_table(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let (request_id, prepared) = split(&state, &cluster, request).await;
    let result = async {
        let (backend, ctx, headers, bytes) = prepared?;
        let body: AlterTableBody = parse_json_body(&headers, &bytes)?;
        let table = TablePath::new(database, table);
        let changes = table_changes(body)?;
        let altered = backend.alter_table(&ctx, &table, changes).await?;
        json_response(&TableResponse::from(&altered))
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Drops one table.
#[utoipa::path(
    delete,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
    operation_id = "dropTable",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 204, description = "The table was dropped"),
        (status = 400, description = "Unsupported query parameter", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster, database, or table", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn drop_table(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = resolve_cluster(&state, &request, &cluster);
    let result = async {
        let (backend, ctx) = prepared?;
        backend
            .drop_table(&ctx, &TablePath::new(database, table))
            .await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Creates one partition of a partitioned table.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions",
    operation_id = "createPartition",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("table" = String, Path, description = "Table name"),
    ),
    request_body(content = CreatePartitionBody, content_type = "application/json"),
    responses(
        (status = 201, description = "The created partition", body = PartitionResponse,
            headers(("Location" = String, description = "Created partition URL"))),
        (status = 400, description = "Malformed body, or a spec that does not match the partition keys", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster, database, or table", body = ErrorEnvelope),
        (status = 409, description = "The partition already exists", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 415, description = "The body is not JSON", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn create_partition(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let (request_id, prepared) = split(&state, &cluster, request).await;
    let result = async {
        let (backend, ctx, headers, bytes) = prepared?;
        let body: CreatePartitionBody = parse_json_body(&headers, &bytes)?;
        let table = TablePath::new(database, table);
        let current = backend.describe_table(&ctx, &table).await?;
        let (spec, name) = partition(&body.partition, &current)?;
        backend.create_partition(&ctx, &table, &spec).await?;
        created_response(
            &PartitionResponse {
                database: table.database().to_string(),
                table: table.table().to_string(),
                partition: body.partition,
            },
            &partition_location(&cluster, &table, &name),
        )
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Drops one partition by its Fluss name.
#[utoipa::path(
    delete,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions/{partition}",
    operation_id = "dropPartition",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("table" = String, Path, description = "Table name"),
        ("partition" = String, Path,
            description = "Fluss partition name; values follow partition-key order and are joined by `$`"),
    ),
    responses(
        (status = 204, description = "The partition was dropped"),
        (status = 400, description = "Unsupported query parameter, or a name that does not match the partition keys", body = ErrorEnvelope),
        (status = 403, description = "Fluss refused the operation", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster, database, table, or partition", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn drop_partition(
    State(state): State<RestState>,
    Path((cluster, database, table, partition)): Path<(String, String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = resolve_cluster(&state, &request, &cluster);
    let result = async {
        let (backend, ctx) = prepared?;
        let table = TablePath::new(database, table);
        let current = backend.describe_table(&ctx, &table).await?;
        let spec = partition_of_name(&partition, &current)?;
        backend.drop_partition(&ctx, &table, &spec).await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Builds a native descriptor, enforcing only REST-specific rules before the native builders run.
fn table_descriptor(database: &str, body: CreateTableBody) -> GatewayResult<TableDescriptor> {
    native_name("database name", database, true)?;
    native_name("table name", &body.table_name, true)?;

    if body.primary_key.as_ref().is_some_and(Vec::is_empty) {
        return Err(GatewayError::invalid_argument(
            "primary_key must contain at least one column when present",
        ));
    }
    let primary_key = body.primary_key.unwrap_or_default();
    for key in &primary_key {
        if body
            .columns
            .iter()
            .any(|column| column.name == *key && column.nullable.unwrap_or(true))
        {
            return Err(GatewayError::invalid_argument(format!(
                "the primary-key column `{key}` must not be nullable"
            )));
        }
    }

    let mut schema = Schema::builder();
    for column in body.columns {
        let data_type = column
            .data_type
            .0
            .with_root_nullable(column.nullable.unwrap_or(true));
        schema = schema.column(column.name, DataType::try_from(data_type)?);
        if let Some(comment) = column.comment {
            schema = schema.with_comment(comment);
        }
    }
    if !primary_key.is_empty() {
        schema = schema.primary_key(primary_key);
    }
    let schema = schema.build().map_err(|error| {
        GatewayError::invalid_argument(format!("invalid table schema: {error}"))
    })?;

    let mut descriptor = TableDescriptor::builder()
        .schema(schema)
        .properties(body.configs)
        .partitioned_by(body.partitioned_by);
    if let Some(distribution) = body.distribution {
        descriptor =
            descriptor.distributed_by(Some(distribution.bucket_count), distribution.bucket_keys);
    }
    if let Some(comment) = body.comment {
        descriptor = descriptor.comment(comment);
    }
    descriptor.build().map_err(|error| {
        GatewayError::invalid_argument(format!("invalid table definition: {error}"))
    })
}

/// Builds the native alter-table batch supported by the current Fluss API.
fn table_changes(body: AlterTableBody) -> GatewayResult<AlterTableChanges> {
    if body.changes.is_empty() {
        return Err(GatewayError::invalid_argument(
            "an alteration must contain at least one change",
        ));
    }

    let mut changes = AlterTableChanges::default();
    for change in body.changes {
        match change {
            TableChangeBody::AddColumn {
                name,
                data_type,
                comment,
            } => {
                let native = DataType::try_from(data_type.0.with_root_nullable(true))?;
                let json = native.serialize_json().map_err(encoding_failure)?;
                changes.add_columns.push(AddColumn {
                    column_name: name,
                    data_type_json: serde_json::to_vec(&json).map_err(encoding_failure)?,
                    comment,
                    position: ColumnPositionType::Last,
                });
            }
            TableChangeBody::SetConfig { key, value } => {
                changes.config_changes.push(AlterConfig::new(
                    key,
                    Some(value),
                    AlterConfigOpType::Set,
                ));
            }
            TableChangeBody::ResetConfig { key } => {
                changes
                    .config_changes
                    .push(AlterConfig::new(key, None, AlterConfigOpType::Delete));
            }
        }
    }
    // Fluss currently requires schema and config changes in separate requests.
    if !changes.add_columns.is_empty() && !changes.config_changes.is_empty() {
        return Err(GatewayError::invalid_argument(
            "schema and config changes cannot be mixed in one alteration",
        ));
    }
    Ok(changes)
}

/// Converts one exact REST partition map and returns its native name.
fn partition(
    entries: &HashMap<String, String>,
    current: &TableInfo,
) -> GatewayResult<(PartitionSpec, String)> {
    if current.partition_keys.is_empty() {
        return Err(GatewayError::invalid_argument(format!(
            "the table `{}` is not partitioned",
            current.table_path
        )));
    }
    if entries.len() != current.partition_keys.len() {
        return Err(GatewayError::invalid_argument(format!(
            "the partition spec must contain exactly {} entries, one per partition key",
            current.partition_keys.len()
        )));
    }

    let values = current
        .partition_keys
        .iter()
        .map(|key| {
            entries
                .get(key)
                .ok_or_else(|| {
                    GatewayError::invalid_argument(format!(
                        "the partition spec is missing the partition key `{key}`"
                    ))
                })
                .and_then(|value| {
                    native_name("partition value", value, true)?;
                    Ok(value.clone())
                })
        })
        .collect::<GatewayResult<Vec<_>>>()?;
    let resolved = ResolvedPartitionSpec::new(current.partition_keys.clone(), values)
        .map_err(invalid_partition)?;
    let name = resolved.get_partition_name();
    Ok((resolved.to_partition_spec(), name))
}

/// Converts a native partition name from the REST path back into a spec.
fn partition_of_name(name: &str, current: &TableInfo) -> GatewayResult<PartitionSpec> {
    if current.partition_keys.is_empty() {
        return Err(GatewayError::invalid_argument(format!(
            "the table `{}` is not partitioned",
            current.table_path
        )));
    }
    let values = name
        .split(PARTITION_SPEC_SEPARATOR)
        .map(|value| {
            native_name("partition value", value, false)?;
            Ok(value.to_string())
        })
        .collect::<GatewayResult<Vec<_>>>()?;
    ResolvedPartitionSpec::new(current.partition_keys.clone(), values)
        .map(|resolved| resolved.to_partition_spec())
        .map_err(invalid_partition)
}

fn native_name(kind: &str, value: &str, reject_internal_prefix: bool) -> GatewayResult<()> {
    if let Some(reason) = TablePath::detect_invalid_name(value).or_else(|| {
        reject_internal_prefix
            .then(|| TablePath::validate_prefix(value))
            .flatten()
    }) {
        return Err(GatewayError::invalid_argument(format!(
            "invalid {kind} `{value}`: {reason}"
        )));
    }
    Ok(())
}

fn invalid_partition(error: fluss::error::Error) -> GatewayError {
    GatewayError::invalid_argument(format!("invalid partition: {error}"))
}

fn encoding_failure(error: impl std::fmt::Display) -> GatewayError {
    log::error!("failed to encode a native column type: {error}");
    GatewayError::internal("the gateway failed to encode the new column type")
}

type SplitRequest = GatewayResult<(Arc<dyn FlussBackend>, RequestContext, HeaderMap, Bytes)>;

/// Resolves the request before buffering its body.
async fn split(
    state: &RestState,
    cluster: &str,
    request: Request,
) -> (crate::protocol::rest::RequestId, SplitRequest) {
    let request_id = request_id(&request);
    let prepared = resolve_cluster(state, &request, cluster);
    let (backend, ctx) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => return (request_id, Err(error)),
    };
    let headers = request.headers().clone();
    let body = Bytes::from_request(request, state).await.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            GatewayError::limit_exceeded("request body exceeds the configured limit")
        } else {
            GatewayError::invalid_argument(format!("unreadable request body: {error}"))
        }
    });
    (request_id, body.map(|body| (backend, ctx, headers, body)))
}

/// A 201 carrying the created resource and the `Location` that addresses it.
fn created_response<T: Serialize>(value: &T, location: &str) -> GatewayResult<Response> {
    let mut response = json_response_with_status(StatusCode::CREATED, value)?;
    let location = HeaderValue::from_str(location).map_err(|error| {
        GatewayError::internal(format!("failed to render the Location header: {error}"))
    })?;
    response.headers_mut().insert(header::LOCATION, location);
    Ok(response)
}

fn database_location(cluster: &str, database: &str) -> String {
    format!(
        "/v1/clusters/{}/databases/{}",
        encode_segment(cluster),
        encode_segment(database)
    )
}

fn table_location(cluster: &str, table: &TablePath) -> String {
    format!(
        "{}/tables/{}",
        database_location(cluster, table.database()),
        encode_segment(table.table())
    )
}

fn partition_location(cluster: &str, table: &TablePath, partition: &str) -> String {
    format!(
        "{}/partitions/{}",
        table_location(cluster, table),
        encode_segment(partition)
    )
}

/// Percent-encodes one path segment.
fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(*byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use crate::backend::FlussBackend;
    use crate::backend::fake::{FakeCall, FakeFlussBackend};
    use crate::error::{GatewayError, Resource};
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Method, Request as HttpRequest, StatusCode, header};
    use fluss::metadata::{AlterConfigOpType, TableInfo, TablePath};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn app(backend: Arc<FakeFlussBackend>) -> axum::Router {
        let state = test_support::state_with_backend(backend as Arc<dyn FlussBackend>);
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &test_support::test_options())
    }

    fn gateway() -> (Arc<FakeFlussBackend>, axum::Router) {
        let backend = Arc::new(FakeFlussBackend::new());
        (Arc::clone(&backend), app(backend))
    }

    async fn send(
        app: &axum::Router,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Option<String>, Value) {
        let mut builder = HttpRequest::builder().method(method).uri(path);
        let body = match body {
            Some(value) => {
                builder = builder.header(header::CONTENT_TYPE, "application/json");
                Body::from(serde_json::to_vec(&value).expect("the body serializes"))
            }
            None => Body::empty(),
        };
        let response = app
            .clone()
            .oneshot(builder.body(body).expect("a valid request"))
            .await
            .expect("the router answers");
        let status = response.status();
        let location = response
            .headers()
            .get(header::LOCATION)
            .map(|value| value.to_str().expect("an ASCII Location").to_string());
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        let parsed = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("a JSON body")
        };
        (status, location, parsed)
    }

    async fn post(
        app: &axum::Router,
        path: &str,
        body: Value,
    ) -> (StatusCode, Option<String>, Value) {
        send(app, Method::POST, path, Some(body)).await
    }

    async fn delete(app: &axum::Router, path: &str) -> (StatusCode, Value) {
        let (status, _, body) = send(app, Method::DELETE, path, None).await;
        (status, body)
    }

    fn partitioned_table() -> Value {
        json!({
            "table_name": "orders",
            "columns": [
                {"name": "id", "data_type": {"type": "BIGINT"}, "nullable": false},
                {"name": "dt", "data_type": {"type": "STRING"}, "nullable": false},
                {"name": "amount", "data_type": {"type": "DECIMAL", "precision": 18, "scale": 2}, "comment": "the order total"},
            ],
            "primary_key": ["id", "dt"],
            "partitioned_by": ["dt"],
            "distribution": {"bucket_count": 4, "bucket_keys": ["id"]},
            "configs": {"table.log.ttl": "7d"},
            "comment": "the orders table",
        })
    }

    fn define_partitioned_table(backend: &FakeFlussBackend) {
        let body = serde_json::from_value(partitioned_table()).expect("a table definition");
        let descriptor = super::table_descriptor("sales", body).expect("a descriptor");
        backend.define_table(TableInfo::of(
            TablePath::new("sales", "orders"),
            1,
            1,
            descriptor,
            0,
            0,
        ));
    }

    #[tokio::test]
    async fn ddl_requests_are_converted_and_forwarded() {
        let (backend, app) = gateway();

        let (status, location, body) = post(
            &app,
            "/v1/clusters/default/databases",
            json!({"database": "sales"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            location.as_deref(),
            Some("/v1/clusters/default/databases/sales")
        );
        assert_eq!(body, json!({"database": "sales"}));

        let (status, location, body) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables",
            partitioned_table(),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            location.as_deref(),
            Some("/v1/clusters/default/databases/sales/tables/orders")
        );
        assert_eq!(body["distribution"]["bucket_count"], 4);
        assert_eq!(
            body["columns"][2],
            json!({
                "name": "amount",
                "data_type": {"type": "DECIMAL", "precision": 18, "scale": 2},
                "nullable": true,
                "comment": "the order total",
            })
        );
        assert_eq!(body["primary_key"], json!(["id", "dt"]));

        define_partitioned_table(&backend);

        let (status, _, _) = send(
            &app,
            Method::PATCH,
            "/v1/clusters/default/databases/sales/tables/orders",
            Some(json!({
                "changes": [
                    {"kind": "add_column", "name": "note", "data_type": {"type": "STRING"}}
                ]
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _, _) = send(
            &app,
            Method::PATCH,
            "/v1/clusters/default/databases/sales/tables/orders",
            Some(json!({
                "changes": [
                    {"kind": "set_config", "key": "table.log.ttl", "value": "30d"},
                    {"kind": "reset_config", "key": "table.datalake.enabled"}
                ]
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, location, body) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables/orders/partitions",
            json!({"partition": {"dt": "2026-08-25"}}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            location.as_deref(),
            Some("/v1/clusters/default/databases/sales/tables/orders/partitions/2026-08-25")
        );
        assert_eq!(
            body,
            json!({
                "database": "sales",
                "table": "orders",
                "partition": {"dt": "2026-08-25"},
            })
        );

        for path in [
            "/v1/clusters/default/databases/sales/tables/orders/partitions/2026-08-25",
            "/v1/clusters/default/databases/sales/tables/orders",
            "/v1/clusters/default/databases/sales",
        ] {
            let (status, body) = delete(&app, path).await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{path}");
            assert_eq!(body, Value::Null, "{path}");
        }

        let calls = backend.calls();
        assert_eq!(calls.len(), 8);
        assert!(matches!(&calls[0], FakeCall::CreateDatabase(name) if name == "sales"));
        let FakeCall::CreateTable(table, descriptor) = &calls[1] else {
            panic!("expected create table, got {:?}", calls[1]);
        };
        assert_eq!(table, &TablePath::new("sales", "orders"));
        assert_eq!(descriptor.partition_keys(), ["dt"]);
        assert_eq!(descriptor.properties().get("table.log.ttl").unwrap(), "7d");

        let FakeCall::AlterTable(_, add_column) = &calls[2] else {
            panic!("expected add column, got {:?}", calls[2]);
        };
        assert_eq!(add_column.add_columns.len(), 1);
        assert_eq!(add_column.add_columns[0].column_name, "note");
        assert_eq!(
            serde_json::from_slice::<Value>(&add_column.add_columns[0].data_type_json).unwrap(),
            json!({"type": "STRING"})
        );

        let FakeCall::AlterTable(_, configs) = &calls[3] else {
            panic!("expected config changes, got {:?}", calls[3]);
        };
        assert_eq!(configs.config_changes.len(), 2);
        assert_eq!(configs.config_changes[0].op_type, AlterConfigOpType::Set);
        assert_eq!(configs.config_changes[1].op_type, AlterConfigOpType::Delete);

        for call in [&calls[4], &calls[5]] {
            let spec = match call {
                FakeCall::CreatePartition(table, spec) | FakeCall::DropPartition(table, spec) => {
                    assert_eq!(table, &TablePath::new("sales", "orders"));
                    spec
                }
                other => panic!("expected a partition call, got {other:?}"),
            };
            assert_eq!(spec.get_spec_map().get("dt").unwrap(), "2026-08-25");
        }
        assert!(
            matches!(&calls[6], FakeCall::DropTable(table) if table == &TablePath::new("sales", "orders"))
        );
        assert!(matches!(&calls[7], FakeCall::DropDatabase(name) if name == "sales"));
    }

    #[tokio::test]
    async fn a_dry_run_validates_without_creating() {
        let (backend, app) = gateway();
        backend.define_database("sales");

        let mut definition = partitioned_table();
        definition["validate_only"] = json!(true);
        let (status, location, body) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables",
            definition,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(location, None);
        assert_eq!(
            body,
            json!({
                "validate_only": true,
                "database": "sales",
                "table": "orders",
                "column_count": 3,
                "primary_key": ["id", "dt"],
            })
        );

        let (status, _, _) = send(
            &app,
            Method::GET,
            "/v1/clusters/default/databases/sales/tables/orders",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let mut invalid = partitioned_table();
        invalid["validate_only"] = json!(true);
        invalid["primary_key"] = json!(["absent"]);
        let (status, _, _) =
            post(&app, "/v1/clusters/default/databases/sales/tables", invalid).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let mut outside_primary_key = partitioned_table();
        outside_primary_key["validate_only"] = json!(true);
        outside_primary_key["distribution"] = json!({"bucket_count": 2, "bucket_keys": ["amount"]});
        let (status, _, body) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables",
            outside_primary_key,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let mut empty_primary_key = partitioned_table();
        empty_primary_key["validate_only"] = json!(true);
        empty_primary_key["primary_key"] = json!([]);
        let (status, _, body) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables",
            empty_primary_key,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    #[tokio::test]
    async fn a_body_that_is_not_exactly_the_contract_is_refused() {
        let (backend, app) = gateway();
        backend.define_database("sales");

        let cases = [
            json!({"table_name": "t", "columns": [{"name": "id", "data_type": {"type": "BIGINT"}, "nullable": false}], "primary_keys": ["id"]}),
            json!({"table_name": "t", "columns": [{"name": "id", "data_type": {"type": "BIGINT", "nullable": false}}]}),
            json!({"table_name": "t", "columns": [{"name": "id", "data_type": {"type": "NUMBER"}}]}),
            json!({"table_name": "t", "columns": [{"name": "id", "data_type": {"type": "BIGINT", "precision": 3}}]}),
            json!({"table_name": "t", "columns": [{"name": "id", "data_type": {"type": "BIGINT", "precision": null}}]}),
            json!({"table_name": "t", "columns": [{"name": "id", "data_type": {"type": "ARRAY", "element_type": {"type": "STRING", "nullable": null}}}]}),
            json!({"columns": []}),
        ];
        for body in cases {
            let (status, _, answered) = post(
                &app,
                "/v1/clusters/default/databases/sales/tables",
                body.clone(),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(answered["error"]["code"], "invalid_argument", "{body}");
        }

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases")
                    .body(Body::from("{}"))
                    .expect("a valid request"),
            )
            .await
            .expect("the router answers");
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn a_conflict_and_a_missing_resource_keep_their_own_status() {
        let (backend, app) = gateway();
        backend.fail_next(
            GatewayError::already_exists("database already exists")
                .with_resource(Resource::Database),
        );

        let (status, _, body) = post(
            &app,
            "/v1/clusters/default/databases",
            json!({"database": "sales"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "database_already_exists");

        for (path, resource) in [
            ("/v1/clusters/default/databases/absent", Resource::Database),
            (
                "/v1/clusters/default/databases/sales/tables/absent",
                Resource::Table,
            ),
        ] {
            backend.fail_next(GatewayError::not_found("missing").with_resource(resource));
            let (status, body) = delete(&app, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert!(
                body["error"]["code"]
                    .as_str()
                    .is_some_and(|code| code.ends_with("_not_found")),
                "{path}: {body}"
            );
        }

        backend.fail_next(
            GatewayError::failed_precondition("database is not empty")
                .with_resource(Resource::Database),
        );
        let (status, body) = delete(&app, "/v1/clusters/default/databases/sales").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "database_not_empty");
    }

    #[tokio::test]
    async fn a_partition_name_must_match_the_partition_keys() {
        let (backend, app) = gateway();
        define_partitioned_table(&backend);

        let (status, body) = delete(
            &app,
            "/v1/clusters/default/databases/sales/tables/orders/partitions/2026-08-25$eu",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");

        let (status, _, _) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables/orders/partitions",
            json!({"partition": {"region": "eu"}}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _, body) = post(
            &app,
            "/v1/clusters/default/databases/sales/tables/orders/partitions",
            json!({"partition": {"dt": "2026$08"}}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        backend.fail_next(
            GatewayError::not_found("partition does not exist").with_resource(Resource::Partition),
        );
        let (status, body) = delete(
            &app,
            "/v1/clusters/default/databases/sales/tables/orders/partitions/2026-08-25",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "partition_not_found");
    }

    #[tokio::test]
    async fn schema_and_config_changes_must_use_separate_requests() {
        let (backend, app) = gateway();

        let (status, _, body) = send(
            &app,
            Method::PATCH,
            "/v1/clusters/default/databases/sales/tables/orders",
            Some(json!({
                "changes": [
                    {"kind": "set_config", "key": "table.log.ttl", "value": "30d"},
                    {"kind": "add_column", "name": "note", "data_type": {"type": "STRING"}},
                ]
            })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(backend.calls().is_empty());
    }

    #[tokio::test]
    async fn a_mutation_is_checked_before_its_body() {
        let (_, app) = gateway();

        let (status, _, body) = post(
            &app,
            "/v1/clusters/other/databases",
            json!({"database": "sales"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "cluster_not_found");

        let (status, _, _) = post(
            &app,
            "/v1/clusters/default/databases?dry_run=true",
            json!({"database": "sales"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _, body) = post(
            &app,
            "/v1/clusters/default/databases",
            json!({"database": "x".repeat(2048)}),
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "limit_exceeded");
    }

    #[test]
    fn a_location_percent_encodes_its_segments() {
        assert_eq!(
            super::database_location("default", "sales db"),
            "/v1/clusters/default/databases/sales%20db"
        );
        assert_eq!(
            super::partition_location("default", &TablePath::new("sales", "orders"), "2026/08"),
            "/v1/clusters/default/databases/sales/tables/orders/partitions/2026%2F08"
        );
    }
}
