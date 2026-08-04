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

//! Catalog mutation endpoints: databases, tables, and partitions.
//!
//! Table definitions carry structured `data_type` objects from [`crate::protocol::rest::datatype`], never type
//! strings. `PATCH` on a table applies its ordered list of changes in one native request: the application layer
//! validates every change against current metadata before it dispatches anything, so a group containing one
//! invalid change leaves the table untouched.
//!
//! Every body is parsed with `deny_unknown_fields`, so a misspelt or dropped field is a `400` before any
//! mutation is attempted rather than a silently ignored instruction. Creations answer `201` with a `Location`
//! header naming the new resource; deletions answer `204` with an empty body.
//!
//! There is deliberately no `validate_only` dry-run flag: Fluss exposes no server-side validation API, and a
//! client-side simulation would promise a guarantee the gateway cannot keep (PLAN §4.8).

use crate::application::ddl::{
    AlterTableRequest, ColumnDefinition, CreateDatabaseRequest, CreateTableRequest,
    PartitionMutationRequest, PartitionSpecEntry, TableChange, TableDistributionDefinition,
};
use crate::application::{DataType, TableRef};
use crate::error::GatewayError;
use crate::observability;
use crate::protocol::rest::datatype::DataTypeResponse;
use crate::protocol::rest::limits::ensure_json_acceptable;
use crate::protocol::rest::metadata::{DatabaseResponse, PartitionResponse, TableResponse};
use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{
    RequestDeadline, RequestId, RestState, application_context, ensure_no_query, error_response,
    json_response, json_response_with_status, metric_cluster, parse_json_body,
};
use axum::Extension;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Instant;
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

/// Database creation body.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateDatabaseBody {
    pub name: String,
    pub comment: Option<String>,
    #[serde(default)]
    pub custom_properties: HashMap<String, String>,
}

/// One user-owned table column.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnDefinitionBody {
    pub name: String,
    pub data_type: DataTypeResponse,
    pub comment: Option<String>,
}

/// Logical primary-key columns, partition key columns included.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PrimaryKeyBody {
    pub columns: Vec<String>,
}

/// Table bucket distribution.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DistributionBody {
    pub bucket_count: i32,
    pub bucket_keys: Vec<String>,
}

/// Table creation body containing only user-owned metadata.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateTableBody {
    pub table_name: String,
    pub columns: Vec<ColumnDefinitionBody>,
    pub primary_key: Option<PrimaryKeyBody>,
    #[serde(default)]
    pub partitioned_by: Vec<String>,
    pub distribution: Option<DistributionBody>,
    #[serde(default)]
    pub configs: HashMap<String, String>,
    #[serde(default)]
    pub custom_properties: HashMap<String, String>,
    pub comment: Option<String>,
}

/// One supported table change. Added columns must be nullable, since existing rows have no value for them.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableChangeBody {
    AddColumn {
        name: String,
        data_type: DataTypeResponse,
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

/// Ordered table alteration body, applied as one atomic group.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AlterTableBody {
    #[schema(min_items = 1)]
    pub changes: Vec<TableChangeBody>,
}

/// One ordered partition specification entry.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PartitionSpecEntryBody {
    pub key: String,
    pub value: String,
}

/// Partition creation body. The spec must name every partition key of the table, in declaration order.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePartitionBody {
    pub spec: Vec<PartitionSpecEntryBody>,
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
        (status = 201, description = "Database created", body = DatabaseResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 409, description = "Database already exists", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn create_database(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path(cluster): Path<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        ensure_json_acceptable(&headers)?;
        let body: CreateDatabaseBody = parse_json_body(&headers, &body)?;
        let location = format!(
            "/v1/clusters/{}/databases/{}",
            encode_segment(&cluster),
            encode_segment(&body.name)
        );
        let context = application_context(&request_id, deadline, &cluster)?;
        let database = state
            .application
            .create_database(
                &context,
                CreateDatabaseRequest {
                    name: body.name,
                    comment: body.comment,
                    custom_properties: body.custom_properties,
                },
            )
            .await?;
        created_response(&DatabaseResponse::from(database), &location)
    }
    .await;
    finish(
        result,
        &state,
        &cluster,
        "create_database",
        started,
        &request_id,
    )
}

/// Drops one empty database.
#[utoipa::path(
    delete,
    path = "/v1/clusters/{cluster}/databases/{database}",
    operation_id = "dropDatabase",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name")
    ),
    responses(
        (status = 204, description = "Database dropped"),
        (status = 400, description = "Unexpected query parameters", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or database not found", body = ErrorEnvelopeSchema),
        (status = 409, description = "Database is not empty", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn drop_database(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path((cluster, database)): Path<(String, String)>,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        let context = application_context(&request_id, deadline, &cluster)?;
        state.application.drop_database(&context, &database).await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    finish(
        result,
        &state,
        &cluster,
        "drop_database",
        started,
        &request_id,
    )
}

/// Creates one table.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables",
    operation_id = "createTable",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name")
    ),
    request_body(content = CreateTableBody, content_type = "application/json"),
    responses(
        (status = 201, description = "Table created", body = TableResponse),
        (status = 400, description = "Invalid table definition", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or database not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 409, description = "Table already exists", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn create_table(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path((cluster, database)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        ensure_json_acceptable(&headers)?;
        let body: CreateTableBody = parse_json_body(&headers, &body)?;
        let table = TableRef::new(database, body.table_name.clone());
        let location = table_location(&cluster, &table);
        let request = create_table_request(table, body)?;
        let context = application_context(&request_id, deadline, &cluster)?;
        let created = state.application.create_table(&context, request).await?;
        created_response(&TableResponse::from(created.as_ref()), &location)
    }
    .await;
    finish(
        result,
        &state,
        &cluster,
        "create_table",
        started,
        &request_id,
    )
}

/// Applies one ordered group of table alterations atomically.
#[utoipa::path(
    patch,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
    operation_id = "alterTable",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = AlterTableBody, content_type = "application/json"),
    responses(
        (status = 200, description = "Canonical table metadata after alteration", body = TableResponse),
        (status = 400, description = "Invalid or unsupported alteration", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn alter_table(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        ensure_json_acceptable(&headers)?;
        let body: AlterTableBody = parse_json_body(&headers, &body)?;
        let request = alter_table_request(TableRef::new(database, table), body)?;
        let context = application_context(&request_id, deadline, &cluster)?;
        let altered = state.application.alter_table(&context, request).await?;
        json_response(&TableResponse::from(altered.as_ref()))
    }
    .await;
    finish(
        result,
        &state,
        &cluster,
        "alter_table",
        started,
        &request_id,
    )
}

/// Drops one table.
#[utoipa::path(
    delete,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
    operation_id = "dropTable",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    responses(
        (status = 204, description = "Table dropped"),
        (status = 400, description = "Unexpected query parameters", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn drop_table(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        let context = application_context(&request_id, deadline, &cluster)?;
        state
            .application
            .drop_table(&context, &TableRef::new(database, table))
            .await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    finish(result, &state, &cluster, "drop_table", started, &request_id)
}

/// Creates one exact partition.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions",
    operation_id = "createPartition",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = CreatePartitionBody, content_type = "application/json"),
    responses(
        (status = 201, description = "Partition created", body = PartitionResponse),
        (status = 400, description = "Invalid partition spec", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 409, description = "Partition already exists", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn create_partition(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        ensure_json_acceptable(&headers)?;
        let body: CreatePartitionBody = parse_json_body(&headers, &body)?;
        let table = TableRef::new(database, table);
        let context = application_context(&request_id, deadline, &cluster)?;
        let partition = state
            .application
            .create_partition(
                &context,
                PartitionMutationRequest {
                    table: table.clone(),
                    spec: body
                        .spec
                        .into_iter()
                        .map(|entry| PartitionSpecEntry {
                            key: entry.key,
                            value: entry.value,
                        })
                        .collect(),
                },
            )
            .await?;
        let location = format!(
            "{}/partitions/{}",
            table_location(&cluster, &table),
            encode_segment(&partition.partition_name)
        );
        created_response(&PartitionResponse::from(partition), &location)
    }
    .await;
    finish(
        result,
        &state,
        &cluster,
        "create_partition",
        started,
        &request_id,
    )
}

/// Drops one partition selected by its canonical partition name.
#[utoipa::path(
    delete,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions/{partition}",
    operation_id = "dropPartition",
    tag = "ddl",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name"),
        ("partition" = String, Path, description = "Canonical partition name, values joined by `$`")
    ),
    responses(
        (status = 204, description = "Partition dropped"),
        (status = 400, description = "Table is not partitioned", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster, table, or partition not found", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn drop_partition(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Path((cluster, database, table, partition)): Path<(String, String, String, String)>,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_no_query(&uri)?;
        let context = application_context(&request_id, deadline, &cluster)?;
        state
            .application
            .drop_partition(&context, &TableRef::new(database, table), &partition)
            .await?;
        Ok(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    finish(
        result,
        &state,
        &cluster,
        "drop_partition",
        started,
        &request_id,
    )
}

/// Maps a validated creation body onto the protocol-neutral request.
///
/// Type conversion is fallible: a structurally valid `data_type` object can still carry an impossible precision,
/// scale, or length, which is rejected here rather than at the native boundary.
fn create_table_request(
    table: TableRef,
    body: CreateTableBody,
) -> Result<CreateTableRequest, GatewayError> {
    if body
        .primary_key
        .as_ref()
        .is_some_and(|primary_key| primary_key.columns.is_empty())
    {
        return Err(GatewayError::invalid_argument(
            "primary_key.columns must not be empty when primary_key is present",
        ));
    }
    Ok(CreateTableRequest {
        table,
        columns: body
            .columns
            .into_iter()
            .map(|column| {
                Ok(ColumnDefinition {
                    name: column.name,
                    data_type: DataType::try_from(column.data_type)?,
                    comment: column.comment,
                })
            })
            .collect::<Result<Vec<_>, GatewayError>>()?,
        primary_key: body
            .primary_key
            .map(|primary_key| primary_key.columns)
            .unwrap_or_default(),
        partitioned_by: body.partitioned_by,
        distribution: body
            .distribution
            .map(|distribution| TableDistributionDefinition {
                bucket_count: distribution.bucket_count,
                bucket_keys: distribution.bucket_keys,
            }),
        configs: body.configs,
        custom_properties: body.custom_properties,
        comment: body.comment,
    })
}

/// Maps the ordered change list onto the protocol-neutral request, preserving request order.
fn alter_table_request(
    table: TableRef,
    body: AlterTableBody,
) -> Result<AlterTableRequest, GatewayError> {
    Ok(AlterTableRequest {
        table,
        changes: body
            .changes
            .into_iter()
            .map(|change| match change {
                TableChangeBody::AddColumn {
                    name,
                    data_type,
                    comment,
                } => Ok(TableChange::AddColumn(ColumnDefinition {
                    name,
                    data_type: DataType::try_from(data_type)?,
                    comment,
                })),
                TableChangeBody::SetConfig { key, value } => {
                    Ok(TableChange::SetConfig { key, value })
                }
                TableChangeBody::ResetConfig { key } => Ok(TableChange::ResetConfig { key }),
            })
            .collect::<Result<Vec<_>, GatewayError>>()?,
    })
}

/// Renders a 201 JSON response carrying the `Location` of the created resource.
fn created_response<T: serde::Serialize>(
    value: &T,
    location: &str,
) -> Result<Response, GatewayError> {
    let mut response = json_response_with_status(StatusCode::CREATED, value)?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(location)
            .map_err(|_| GatewayError::internal("failed to encode Location header"))?,
    );
    Ok(response)
}

/// Renders one mutation outcome and records its metric under a bounded cluster label.
fn finish(
    result: Result<Response, GatewayError>,
    state: &RestState,
    cluster: &str,
    operation: &'static str,
    started: Instant,
    request_id: &RequestId,
) -> Response {
    let outcome = if result.is_ok() { "success" } else { "error" };
    observability::ddl_operation(
        &metric_cluster(state, cluster),
        operation,
        outcome,
        started.elapsed(),
    );
    result.unwrap_or_else(|error| error_response(&error, request_id))
}

fn table_location(cluster: &str, table: &TableRef) -> String {
    format!(
        "/v1/clusters/{}/databases/{}/tables/{}",
        encode_segment(cluster),
        encode_segment(&table.database),
        encode_segment(&table.table)
    )
}

/// Percent-encodes one URI path segment, without pushing a protocol-only dependency into lower layers.
fn encode_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::GatewayService;
    use crate::backend::GatewayBackend;
    use crate::backend::registry::ClusterRegistry;
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// A table definition small enough to read but exercising a structured nested type.
    const SHIPMENTS: &str = r#"{
        "table_name": "shipments",
        "columns": [
            {"name": "region", "data_type": {"type": "STRING", "nullable": false}},
            {"name": "id", "data_type": {"type": "BIGINT", "nullable": false}},
            {"name": "payload", "data_type": {"type": "ROW", "nullable": true, "fields": [
                {"name": "tags", "field_type": {"type": "ARRAY", "nullable": true,
                 "element_type": {"type": "STRING", "nullable": true}}}
            ]}}
        ],
        "primary_key": {"columns": ["region", "id"]},
        "partitioned_by": ["region"],
        "distribution": {"bucket_count": 3, "bucket_keys": ["id"]},
        "configs": {"table.kv.format": "COMPACTED"},
        "custom_properties": {"source": "rest"},
        "comment": "shipments"
    }"#;

    fn request(method: Method, uri: &str, body: &str) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if !body.is_empty() {
            builder = builder
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "application/json");
        }
        builder.body(Body::from(body.to_string())).unwrap()
    }

    async fn send(app: &Router, method: Method, uri: &str, body: &str) -> Response {
        app.clone()
            .oneshot(request(method, uri, body))
            .await
            .unwrap()
    }

    async fn response_json(response: Response) -> serde_json::Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn location_path_segments_are_percent_encoded() {
        assert_eq!(encode_segment("sales/eu 1"), "sales%2Feu%201");
    }

    #[test]
    fn an_unknown_alteration_kind_is_refused_by_the_parser() {
        let error = serde_json::from_str::<AlterTableBody>(
            r#"{"changes":[{"kind":"drop_column","name":"old"}]}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }

    #[tokio::test]
    async fn creating_a_database_answers_201_with_its_location() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases",
            r#"{"name":"analytics","comment":"owned","custom_properties":{"owner":"data"}}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers()[header::LOCATION],
            "/v1/clusters/default/databases/analytics"
        );
        let body = response_json(response).await;
        assert_eq!(body["name"], "analytics");
        assert_eq!(body["custom_properties"]["owner"], "data");
    }

    #[tokio::test]
    async fn creating_an_existing_database_is_409_with_its_resource_kind() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases",
            r#"{"name":"fluss"}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], "ALREADY_EXISTS");
        assert_eq!(body["error"]["details"]["resource_kind"], "database");
        assert_eq!(body["error"]["details"]["resource_name"], "fluss");
    }

    #[tokio::test]
    async fn creating_an_existing_table_is_409_with_its_resource_kind() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables",
            r#"{"table_name":"users","columns":[{"name":"id","data_type":{"type":"INT","nullable":false}}],"primary_key":{"columns":["id"]}}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert_eq!(body["error"]["code"], "ALREADY_EXISTS");
        assert_eq!(body["error"]["details"]["resource_kind"], "table");
        assert_eq!(body["error"]["details"]["resource_name"], "fluss.users");
    }

    #[tokio::test]
    async fn creating_a_table_answers_201_with_its_location_and_structured_types() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables",
            SHIPMENTS,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers()[header::LOCATION],
            "/v1/clusters/default/databases/fluss/tables/shipments"
        );
        let body = response_json(response).await;
        assert_eq!(body["table_name"], "shipments");
        assert_eq!(body["kind"], "PRIMARY_KEY");
        assert_eq!(body["columns"][2]["data_type"]["type"], "ROW");
        assert_eq!(
            body["columns"][2]["data_type"]["fields"][0]["field_type"]["type"],
            "ARRAY"
        );
        assert_eq!(body["primary_key"]["columns"][0], "region");
        assert!(body["table_id"].is_string(), "64-bit-safe table id");
    }

    #[tokio::test]
    async fn a_table_without_a_primary_key_is_created_as_a_log_table() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables",
            r#"{"table_name":"audit","columns":[{"name":"payload","data_type":{"type":"STRING","nullable":true}}]}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response_json(response).await;
        assert_eq!(body["kind"], "LOG");
        assert_eq!(body["log_format"], "ARROW");
        assert!(body.get("primary_key").is_none());
    }

    #[tokio::test]
    async fn a_patch_applies_all_three_change_kinds_in_one_request() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::PATCH,
            "/v1/clusters/default/databases/fluss/tables/users",
            r#"{"changes":[
                {"kind":"add_column","name":"note","data_type":{"type":"STRING","nullable":true}},
                {"kind":"set_config","key":"table.log.ttl","value":"1d"},
                {"kind":"reset_config","key":"table.log.ttl"}
            ]}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["columns"][2]["name"], "note");
        assert!(
            body["configs"].get("table.log.ttl").is_none(),
            "the reset undoes the set within the same group: {body}"
        );
    }

    #[tokio::test]
    async fn an_invalid_change_leaves_the_whole_group_unapplied() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let before = response_json(
            send(
                &app,
                Method::GET,
                "/v1/clusters/default/databases/fluss/tables/users",
                "",
            )
            .await,
        )
        .await;

        // The first two changes are individually valid; the third is not. Validation runs over the whole group
        // before anything is dispatched, so none of them may take effect.
        let response = send(
            &app,
            Method::PATCH,
            "/v1/clusters/default/databases/fluss/tables/users",
            r#"{"changes":[
                {"kind":"add_column","name":"would_apply","data_type":{"type":"STRING","nullable":true}},
                {"kind":"set_config","key":"table.log.ttl","value":"1d"},
                {"kind":"add_column","name":"invalid","data_type":{"type":"STRING","nullable":false}}
            ]}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let after = response_json(
            send(
                &app,
                Method::GET,
                "/v1/clusters/default/databases/fluss/tables/users",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(after["columns"], before["columns"]);
        assert_eq!(after["configs"], before["configs"]);
        assert_eq!(after["schema_id"], before["schema_id"]);
    }

    #[tokio::test]
    async fn an_empty_change_list_is_rejected() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::PATCH,
            "/v1/clusters/default/databases/fluss/tables/users",
            r#"{"changes":[]}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_partition_lifecycle_creates_locates_and_drops() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables/orders/partitions",
            r#"{"spec":[{"key":"region","value":"apac"}]}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers()[header::LOCATION],
            "/v1/clusters/default/databases/fluss/tables/orders/partitions/apac"
        );
        let created = response_json(response).await;
        assert_eq!(created["partition_name"], "apac");
        // Like `table_id`, the partition id is a decimal string for clients without 64-bit JSON integers.
        assert!(created["partition_id"].is_string(), "{created}");

        let conflict = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables/orders/partitions",
            r#"{"spec":[{"key":"region","value":"apac"}]}"#,
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(conflict).await["error"]["details"]["resource_kind"],
            "partition"
        );

        let dropped = send(
            &app,
            Method::DELETE,
            "/v1/clusters/default/databases/fluss/tables/orders/partitions/apac",
            "",
        )
        .await;
        assert_eq!(dropped.status(), StatusCode::NO_CONTENT);

        let missing = send(
            &app,
            Method::DELETE,
            "/v1/clusters/default/databases/fluss/tables/orders/partitions/apac",
            "",
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(missing).await["error"]["details"]["resource_kind"],
            "partition"
        );
    }

    #[tokio::test]
    async fn a_partition_spec_must_match_the_declared_partition_keys() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        for body in [
            r#"{"spec":[{"key":"country","value":"eu"}]}"#,
            r#"{"spec":[]}"#,
        ] {
            let response = send(
                &app,
                Method::POST,
                "/v1/clusters/default/databases/fluss/tables/orders/partitions",
                body,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        }

        // An unpartitioned table has no valid spec at all.
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables/users/partitions",
            r#"{"spec":[{"key":"region","value":"eu"}]}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn dropping_a_non_empty_database_is_a_conflict_and_the_empty_one_succeeds() {
        let app = test_support::app(Arc::new(TestBackend::new()));

        let conflict = send(
            &app,
            Method::DELETE,
            "/v1/clusters/default/databases/fluss",
            "",
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert_eq!(
            response_json(conflict).await["error"]["code"],
            "FAILED_PRECONDITION"
        );

        for table in ["users", "orders", "events"] {
            let response = send(
                &app,
                Method::DELETE,
                &format!("/v1/clusters/default/databases/fluss/tables/{table}"),
                "",
            )
            .await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT, "{table}");
        }

        let dropped = send(
            &app,
            Method::DELETE,
            "/v1/clusters/default/databases/fluss",
            "",
        )
        .await;
        assert_eq!(dropped.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            dropped
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .len(),
            0,
            "204 carries no body"
        );

        let missing = send(
            &app,
            Method::DELETE,
            "/v1/clusters/default/databases/fluss",
            "",
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_fields_and_stray_queries_are_refused_before_any_mutation() {
        let backend = Arc::new(TestBackend::new());
        let app = test_support::app(backend.clone());

        // `if_not_exists` was never part of the contract; accepting it silently would change the semantics.
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases",
            r#"{"name":"bad","if_not_exists":true}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            !backend
                .list_databases()
                .await
                .unwrap()
                .contains(&"bad".to_string()),
            "a rejected body must not reach the catalog"
        );

        let stray = send(
            &app,
            Method::DELETE,
            "/v1/clusters/default/databases/fluss?if_exists=true",
            "",
        )
        .await;
        assert_eq!(stray.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(stray).await["error"]["code"],
            "INVALID_ARGUMENT"
        );
    }

    #[tokio::test]
    async fn a_body_without_a_json_content_type_is_refused() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases")
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::from(r#"{"name":"analytics"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn creating_under_a_missing_parent_is_404() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/missing/tables",
            r#"{"table_name":"t","columns":[{"name":"v","data_type":{"type":"INT","nullable":true}}]}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn an_impossible_data_type_is_rejected_before_dispatch() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/default/databases/fluss/tables",
            r#"{"table_name":"clock","columns":[{"name":"at","data_type":{"type":"DECIMAL","nullable":true,"precision":99,"scale":2}}]}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "INVALID_ARGUMENT"
        );
    }

    #[tokio::test]
    async fn mutations_are_isolated_to_the_selected_cluster() {
        let east = Arc::new(TestBackend::new());
        let west = Arc::new(TestBackend::new());
        let health = test_support::green_report();
        let clusters = Arc::new(ClusterRegistry::from_test_entries(vec![
            (
                "east".to_string(),
                Some(east.clone() as Arc<dyn GatewayBackend>),
                Some(health),
            ),
            (
                "west".to_string(),
                Some(west.clone() as Arc<dyn GatewayBackend>),
                Some(health),
            ),
        ]));
        let mut state = test_support::state_with_clusters(clusters.clone());
        state.application = Arc::new(GatewayService::new(clusters));
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());

        let response = send(
            &app,
            Method::POST,
            "/v1/clusters/east/databases",
            r#"{"name":"only_east"}"#,
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        assert!(
            east.list_databases()
                .await
                .unwrap()
                .contains(&"only_east".to_string())
        );
        assert!(
            !west
                .list_databases()
                .await
                .unwrap()
                .contains(&"only_east".to_string())
        );
    }
}
