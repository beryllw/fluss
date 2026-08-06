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

//! Catalog read endpoints.
//!
//! Collections are paginated with stateless keyset tokens: the gateway fetches the full sorted name list on every
//! page and returns the entries strictly greater than the token's last-seen name. The token itself is
//! self-contained (see [`crate::protocol::rest::pagination`]), so any instance serves any page.
//!
//! Page parameters and the continuation token are validated **before** the listing request is dispatched, so a
//! malformed or out-of-scope token is always a `400` and never depends on whether the parent resource exists.
//!
//! Path components arrive percent-decoded exactly once by the router and are matched as exact Fluss identifiers.

use crate::auth::Principal;
use crate::backend::model::TableRef;
use crate::backend::model::{
    DatabaseDescription, PartitionDescription, TableDescription, TableKind,
};
use crate::error::GatewayError;
use crate::observability;
use crate::protocol::rest::datatype::DataTypeResponse;
use crate::protocol::rest::limits::ensure_json_acceptable;
use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::pagination::{PageScope, decode_page_token, encode_page_token};
use crate::protocol::rest::{
    MetadataLimits, RequestDeadline, RequestId, RestState, application_context, ensure_no_query,
    error_response, json_response, metric_cluster, parse_query,
};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::{Extension, http::HeaderMap, http::Uri};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Metadata routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(list_databases))
        .routes(routes!(describe_database))
        .routes(routes!(list_tables))
        .routes(routes!(describe_table))
        .routes(routes!(list_partitions))
}

/// Keyset pagination parameters shared by every catalog collection endpoint.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct PageParams {
    /// Number of entries to return. Defaults to `[metadata] default_page_size` and is capped at
    /// `[metadata] max_page_size`.
    #[param(minimum = 1, maximum = 1000)]
    pub max_results: Option<usize>,
    /// Opaque continuation token returned by the same collection endpoint.
    pub page_token: Option<String>,
}

/// Response of `GET /v1/clusters/{cluster}/databases`.
#[derive(Debug, Serialize, ToSchema)]
pub struct DatabasesResponse {
    pub databases: Vec<String>,
    /// Absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}`.
#[derive(Debug, Serialize, ToSchema)]
pub struct DatabaseResponse {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub custom_properties: HashMap<String, String>,
    /// Milliseconds since the Unix epoch.
    pub created_time: i64,
    /// Milliseconds since the Unix epoch.
    pub modified_time: i64,
}

impl From<DatabaseDescription> for DatabaseResponse {
    /// Converts HTTP-independent database metadata into its stable wire shape.
    fn from(description: DatabaseDescription) -> Self {
        Self {
            name: description.name,
            comment: description.comment,
            custom_properties: description.custom_properties,
            created_time: description.created_time,
            modified_time: description.modified_time,
        }
    }
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables`.
#[derive(Debug, Serialize, ToSchema)]
pub struct TablesResponse {
    pub tables: Vec<String>,
    /// Absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// One column of a table schema.
#[derive(Debug, Serialize, ToSchema)]
pub struct ColumnResponse {
    pub name: String,
    pub data_type: DataTypeResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Bucket distribution of a table.
#[derive(Debug, Serialize, ToSchema)]
pub struct DistributionResponse {
    pub bucket_count: i32,
    pub bucket_keys: Vec<String>,
}

/// Ordered columns of a logical or physical primary key.
#[derive(Debug, Serialize, ToSchema)]
pub struct KeyColumnsResponse {
    pub columns: Vec<String>,
}

/// Capabilities the gateway derives from immutable table metadata.
///
/// The gateway is stateless and exposes no scan endpoints, so the only derived capabilities are the two lookup
/// shapes.
#[derive(Debug, Serialize, ToSchema)]
pub struct CapabilitiesResponse {
    /// Exact primary-key lookup is available for this table.
    pub exact_lookup_supported: bool,
    /// Bounded prefix lookup is available for this table.
    pub prefix_lookup_supported: bool,
}

/// Stable table kind reported by metadata endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub enum TableKindResponse {
    #[serde(rename = "PRIMARY_KEY")]
    PrimaryKey,
    #[serde(rename = "LOG")]
    Log,
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables/{table}`.
#[derive(Debug, Serialize, ToSchema)]
pub struct TableResponse {
    pub database: String,
    pub table_name: String,
    /// Canonical decimal string, safe for clients without 64-bit JSON integers.
    pub table_id: String,
    pub schema_id: i32,
    pub kind: TableKindResponse,
    pub columns: Vec<ColumnResponse>,
    /// Logical primary key in order, partition key columns included. Absent for log tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<KeyColumnsResponse>,
    /// Primary key without partition columns. Absent for log tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_primary_key: Option<KeyColumnsResponse>,
    pub partitioned_by: Vec<String>,
    pub distribution: DistributionResponse,
    /// Log storage format, absent for primary-key tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_format: Option<String>,
    /// Key-value storage format, absent for log tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kv_format: Option<String>,
    pub capabilities: CapabilitiesResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub configs: HashMap<String, String>,
    pub custom_properties: HashMap<String, String>,
    /// Milliseconds since the Unix epoch.
    pub created_time: i64,
    /// Milliseconds since the Unix epoch.
    pub modified_time: i64,
}

impl From<&TableDescription> for TableResponse {
    /// Converts HTTP-independent table metadata into its stable wire shape.
    fn from(description: &TableDescription) -> Self {
        Self {
            database: description.table.database.clone(),
            table_name: description.table.table.clone(),
            table_id: description.table_id.to_string(),
            schema_id: description.schema_id,
            kind: match description.kind {
                TableKind::PrimaryKey => TableKindResponse::PrimaryKey,
                TableKind::Log => TableKindResponse::Log,
            },
            columns: description
                .columns
                .iter()
                .map(|column| ColumnResponse {
                    name: column.name.clone(),
                    data_type: DataTypeResponse::from(&column.data_type),
                    comment: column.comment.clone(),
                })
                .collect(),
            primary_key: (!description.primary_keys.is_empty()).then(|| KeyColumnsResponse {
                columns: description.primary_keys.clone(),
            }),
            physical_primary_key: (!description.physical_primary_keys.is_empty()).then(|| {
                KeyColumnsResponse {
                    columns: description.physical_primary_keys.clone(),
                }
            }),
            partitioned_by: description.partition_keys.clone(),
            distribution: DistributionResponse {
                bucket_count: description.num_buckets,
                bucket_keys: description.bucket_keys.clone(),
            },
            log_format: description.log_format.clone(),
            kv_format: description.kv_format.clone(),
            capabilities: CapabilitiesResponse {
                exact_lookup_supported: description.capabilities.exact_lookup_supported,
                prefix_lookup_supported: description.capabilities.prefix_lookup_supported,
            },
            comment: description.comment.clone(),
            configs: description.properties.clone(),
            custom_properties: description.custom_properties.clone(),
            created_time: description.created_time,
            modified_time: description.modified_time,
        }
    }
}

/// One `(partition_key, value)` pair, kept as a pair so the key order of the table is preserved.
#[derive(Debug, Serialize, ToSchema)]
pub struct PartitionSpecEntryResponse {
    pub key: String,
    pub value: String,
}

/// One partition of a partitioned table.
#[derive(Debug, Serialize, ToSchema)]
pub struct PartitionResponse {
    /// Canonical decimal string, safe for clients without 64-bit JSON integers.
    pub partition_id: String,
    pub partition_name: String,
    pub spec: Vec<PartitionSpecEntryResponse>,
}

impl From<PartitionDescription> for PartitionResponse {
    /// Converts HTTP-independent partition metadata into its stable wire shape.
    fn from(description: PartitionDescription) -> Self {
        Self {
            partition_id: description.partition_id.to_string(),
            partition_name: description.partition_name,
            spec: description
                .spec
                .into_iter()
                .map(|(key, value)| PartitionSpecEntryResponse { key, value })
                .collect(),
        }
    }
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions`.
#[derive(Debug, Serialize, ToSchema)]
pub struct PartitionsResponse {
    pub partitions: Vec<PartitionResponse>,
    /// Absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// A validated pagination request: the page size plus the decoded keyset position.
///
/// Both are resolved before any listing request is dispatched, which is what makes an invalid `page_token` a
/// deterministic `400` rather than a status that depends on whether the parent resource exists.
struct Page {
    size: usize,
    /// Last entry of the previous page. The next page starts strictly after it.
    after: Option<String>,
}

impl Page {
    /// Validates the requested page size and decodes the continuation token against this collection's scope.
    fn prepare(
        params: PageParams,
        limits: &MetadataLimits,
        scope: &PageScope,
    ) -> Result<Self, GatewayError> {
        let size = params.max_results.unwrap_or(limits.default_page_size);
        if size == 0 || size > limits.max_page_size {
            return Err(GatewayError::invalid_argument(format!(
                "`max_results` must be between 1 and {}",
                limits.max_page_size
            )));
        }
        let after = params
            .page_token
            .as_deref()
            .map(|token| decode_page_token(token, scope))
            .transpose()?;
        Ok(Self { size, after })
    }

    /// Sorts one collection and returns the page strictly after the token position.
    ///
    /// Keyset rather than offset: the page boundary is a name, not an index, so concurrent DDL can neither
    /// duplicate an entry across pages nor hide a pre-existing one.
    fn apply<T, F>(
        &self,
        mut entries: Vec<T>,
        scope: &PageScope,
        key: F,
    ) -> Result<(Vec<T>, Option<String>), GatewayError>
    where
        F: Fn(&T) -> &str,
    {
        entries.sort_by(|left, right| key(left).cmp(key(right)));
        let start = match self.after.as_deref() {
            Some(after) => entries
                .iter()
                .position(|entry| key(entry) > after)
                .unwrap_or(entries.len()),
            None => 0,
        };
        let end = start.saturating_add(self.size).min(entries.len());
        let has_more = end < entries.len();
        let page: Vec<T> = entries.drain(start..end).collect();
        let next_page_token = if has_more {
            page.last()
                .map(|entry| encode_page_token(scope, key(entry)))
                .transpose()?
        } else {
            None
        };
        Ok((page, next_page_token))
    }
}

/// Renders one catalog read outcome and records its metric under a bounded cluster label.
fn respond<T: Serialize>(
    result: Result<T, GatewayError>,
    state: &RestState,
    cluster: &str,
    operation: &'static str,
    started: Instant,
    request_id: &RequestId,
) -> Response {
    let response = match result {
        Ok(body) => json_response(&body).unwrap_or_else(|error| error_response(&error, request_id)),
        Err(error) => error_response(&error, request_id),
    };
    let outcome = if response.status().is_success() {
        "success"
    } else {
        "error"
    };
    observability::catalog_operation(
        &metric_cluster(state, cluster),
        operation,
        outcome,
        started.elapsed(),
    );
    response
}

/// Lists database names of one cluster.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases",
    operation_id = "listDatabases",
    tag = "metadata",
    params(("cluster" = String, Path, description = "Configured cluster ID"), PageParams),
    responses(
        (status = 200, description = "Databases in lexical order", body = DatabasesResponse),
        (status = 400, description = "Invalid page parameters or token", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn list_databases(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path(cluster): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_json_acceptable(&headers)?;
        let scope = PageScope::Databases;
        let page = Page::prepare(parse_query(&uri)?, &state.metadata_limits, &scope)?;
        let context = application_context(&request_id, deadline, &principal, &cluster)?;
        let databases = super::catalog_ops::list_databases(&state.clusters, &context).await?;
        let (databases, next_page_token) = page.apply(databases, &scope, String::as_str)?;
        Ok(DatabasesResponse {
            databases,
            next_page_token,
        })
    }
    .await;
    respond(
        result,
        &state,
        &cluster,
        "list_databases",
        started,
        &request_id,
    )
}

/// Describes one database.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}",
    operation_id = "describeDatabase",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name")
    ),
    responses(
        (status = 200, description = "Database metadata", body = DatabaseResponse),
        (status = 400, description = "Unexpected query parameters", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or database not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn describe_database(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path((cluster, database)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_json_acceptable(&headers)?;
        ensure_no_query(&uri)?;
        let context = application_context(&request_id, deadline, &principal, &cluster)?;
        super::catalog_ops::describe_database(&state.clusters, &context, &database)
            .await
            .map(DatabaseResponse::from)
    }
    .await;
    respond(
        result,
        &state,
        &cluster,
        "describe_database",
        started,
        &request_id,
    )
}

/// Lists table names of one database.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables",
    operation_id = "listTables",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        PageParams
    ),
    responses(
        (status = 200, description = "Tables in lexical order", body = TablesResponse),
        (status = 400, description = "Invalid page parameters or token", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or database not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn list_tables(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path((cluster, database)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let result = async {
        ensure_json_acceptable(&headers)?;
        let scope = PageScope::tables(&database);
        let page = Page::prepare(parse_query(&uri)?, &state.metadata_limits, &scope)?;
        let context = application_context(&request_id, deadline, &principal, &cluster)?;
        let tables = super::catalog_ops::list_tables(&state.clusters, &context, &database).await?;
        let (tables, next_page_token) = page.apply(tables, &scope, String::as_str)?;
        Ok(TablesResponse {
            tables,
            next_page_token,
        })
    }
    .await;
    respond(
        result,
        &state,
        &cluster,
        "list_tables",
        started,
        &request_id,
    )
}

/// Describes one table: schema, keys, distribution, partitioning, and derived capabilities.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
    operation_id = "describeTable",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    responses(
        (status = 200, description = "Table metadata", body = TableResponse),
        (status = 400, description = "Unexpected query parameters", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn describe_table(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let table_ref = TableRef::new(database, table);
    let result = async {
        ensure_json_acceptable(&headers)?;
        ensure_no_query(&uri)?;
        let context = application_context(&request_id, deadline, &principal, &cluster)?;
        super::catalog_ops::describe_table(&state.clusters, &context, &table_ref)
            .await
            .map(|description| TableResponse::from(description.as_ref()))
    }
    .await;
    respond(
        result,
        &state,
        &cluster,
        "describe_table",
        started,
        &request_id,
    )
}

/// Lists partitions of a partitioned table.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions",
    operation_id = "listPartitions",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name"),
        PageParams
    ),
    responses(
        (status = 200, description = "Partitions in lexical name order", body = PartitionsResponse),
        (status = 400, description = "Invalid page token, or the table is not partitioned", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn list_partitions(
    State(state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Extension(deadline): Extension<RequestDeadline>,
    Extension(principal): Extension<Principal>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    let table_ref = TableRef::new(database, table);
    let result = async {
        ensure_json_acceptable(&headers)?;
        let scope = PageScope::partitions(&table_ref);
        let page = Page::prepare(parse_query(&uri)?, &state.metadata_limits, &scope)?;
        let context = application_context(&request_id, deadline, &principal, &cluster)?;
        let partitions =
            super::catalog_ops::list_partitions(&state.clusters, &context, &table_ref).await?;
        let (partitions, next_page_token) =
            page.apply(partitions, &scope, |partition| &partition.partition_name)?;
        Ok(PartitionsResponse {
            partitions: partitions
                .into_iter()
                .map(PartitionResponse::from)
                .collect(),
            next_page_token,
        })
    }
    .await;
    respond(
        result,
        &state,
        &cluster,
        "list_partitions",
        started,
        &request_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::GatewayBackend;
    use crate::backend::model::CreateDatabaseRequest;
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// Sends one metadata GET against a router over the supplied backend.
    async fn get_on(backend: Arc<TestBackend>, path: &str) -> Response {
        get_with_accept(backend, path, None).await
    }

    async fn get_with_accept(
        backend: Arc<TestBackend>,
        path: &str,
        accept: Option<&str>,
    ) -> Response {
        let app = test_support::app(backend);
        let mut request = Request::builder().uri(format!("/v1/clusters/default{path}"));
        if let Some(accept) = accept {
            request = request.header(header::ACCEPT, accept);
        }
        app.oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// Sends one metadata GET against the standard fixture catalog.
    async fn get(path: &str) -> Response {
        get_on(Arc::new(TestBackend::new()), path).await
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    async fn get_json(path: &str) -> serde_json::Value {
        body_json(get(path).await).await
    }

    /// A fixture backend carrying extra databases, so the database listing is worth paginating.
    async fn backend_with_databases(names: &[&str]) -> Arc<TestBackend> {
        let backend = Arc::new(TestBackend::new());
        for name in names {
            backend
                .create_database(&CreateDatabaseRequest {
                    name: (*name).to_string(),
                    comment: None,
                    custom_properties: HashMap::new(),
                })
                .await
                .expect("fixture database is creatable");
        }
        backend
    }

    #[tokio::test]
    async fn describe_database_returns_canonical_metadata() {
        let json = get_json("/databases/fluss").await;
        assert_eq!(json["name"], "fluss");
        assert_eq!(json["comment"], "fixture database");
        assert_eq!(json["created_time"], 1_700_000_000_000_i64);
        assert_eq!(json["custom_properties"], json!({}));
    }

    #[tokio::test]
    async fn describing_a_missing_database_is_404_with_its_resource_kind() {
        let response = get("/databases/missing").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "not_found");
        assert_eq!(json["error"]["details"]["resource_kind"], "database");
        assert_eq!(json["error"]["details"]["resource_name"], "missing");
    }

    #[tokio::test]
    async fn describing_a_missing_table_is_404_with_its_resource_kind() {
        let response = get("/databases/fluss/tables/missing").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "not_found");
        assert_eq!(json["error"]["details"]["resource_kind"], "table");
        assert_eq!(json["error"]["details"]["resource_name"], "fluss.missing");
    }

    #[tokio::test]
    async fn describe_pk_table_reports_schema_keys_and_lookup_capabilities() {
        let json = get_json("/databases/fluss/tables/users").await;
        assert_eq!(json["database"], "fluss");
        assert_eq!(json["table_name"], "users");
        assert_eq!(json["kind"], "PRIMARY_KEY");
        // 64-bit-unsafe clients get the table id as a decimal string, never a JSON number.
        assert_eq!(json["table_id"], "1");
        assert!(json["table_id"].is_string());
        assert_eq!(
            json["columns"],
            json!([
                {"name": "id", "data_type": {"type": "INT", "nullable": false}},
                {"name": "name", "data_type": {"type": "STRING", "nullable": true}},
            ])
        );
        assert_eq!(json["primary_key"], json!({"columns": ["id"]}));
        assert_eq!(
            json["distribution"],
            json!({"bucket_count": 3, "bucket_keys": ["id"]})
        );
        assert_eq!(
            json["capabilities"],
            json!({"exact_lookup_supported": true, "prefix_lookup_supported": true})
        );
        assert_eq!(json["kv_format"], "COMPACTED");
        assert!(json.get("log_format").is_none());
    }

    #[tokio::test]
    async fn describe_log_table_reports_no_keys_and_no_lookup_capabilities() {
        let json = get_json("/databases/fluss/tables/events").await;
        assert_eq!(json["kind"], "LOG");
        assert!(json.get("primary_key").is_none());
        assert!(json.get("physical_primary_key").is_none());
        assert_eq!(json["log_format"], "ARROW");
        assert_eq!(
            json["capabilities"],
            json!({"exact_lookup_supported": false, "prefix_lookup_supported": false})
        );
    }

    #[tokio::test]
    async fn describe_partitioned_table_separates_logical_and_physical_keys() {
        let json = get_json("/databases/fluss/tables/orders").await;
        assert_eq!(json["partitioned_by"], json!(["region"]));
        assert_eq!(json["primary_key"], json!({"columns": ["region", "id"]}));
        assert_eq!(json["physical_primary_key"], json!({"columns": ["id"]}));
    }

    #[tokio::test]
    async fn path_components_are_percent_decoded_exactly_once() {
        let message = get_json("/databases/my%20db").await["error"]["message"]
            .as_str()
            .expect("message")
            .to_string();
        assert!(message.contains("`my db`"), "decoded once: {message}");

        let message = get_json("/databases/my%2520db").await["error"]["message"]
            .as_str()
            .expect("message")
            .to_string();
        assert!(message.contains("`my%20db`"), "not twice: {message}");
    }

    #[tokio::test]
    async fn a_single_page_omits_the_continuation_token() {
        let json = get_json("/databases/fluss/tables").await;
        assert_eq!(json["tables"], json!(["events", "orders", "users"]));
        assert!(
            json.get("next_page_token").is_none(),
            "the last page carries no token: {json}"
        );
    }

    /// Walks a whole collection one entry at a time and proves keyset paging is exact.
    async fn walk(backend: Arc<TestBackend>, path: &str, field: &str) -> Vec<String> {
        let mut visited = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..32 {
            let query = match &token {
                Some(token) => format!("{path}?max_results=1&page_token={token}"),
                None => format!("{path}?max_results=1"),
            };
            let response = get_on(backend.clone(), &query).await;
            assert_eq!(response.status(), StatusCode::OK, "{query}");
            let json = body_json(response).await;
            for entry in json[field].as_array().expect("page array") {
                let name = match entry.as_str() {
                    Some(name) => name.to_string(),
                    None => entry["partition_name"].as_str().expect("name").to_string(),
                };
                visited.push(name);
            }
            match json.get("next_page_token") {
                Some(next) => token = Some(next.as_str().expect("token").to_string()),
                None => return visited,
            }
        }
        panic!("pagination did not terminate for {path}");
    }

    #[tokio::test]
    async fn paging_visits_every_database_exactly_once() {
        let backend = backend_with_databases(&["analytics", "ops", "staging"]).await;
        assert_eq!(
            walk(backend, "/databases", "databases").await,
            vec!["analytics", "fluss", "ops", "staging"]
        );
    }

    #[tokio::test]
    async fn paging_visits_every_table_exactly_once() {
        let backend = Arc::new(TestBackend::new());
        assert_eq!(
            walk(backend, "/databases/fluss/tables", "tables").await,
            vec!["events", "orders", "users"]
        );
    }

    #[tokio::test]
    async fn paging_visits_every_partition_exactly_once() {
        let backend = Arc::new(TestBackend::new());
        assert_eq!(
            walk(
                backend,
                "/databases/fluss/tables/orders/partitions",
                "partitions"
            )
            .await,
            vec!["eu", "us"]
        );
    }

    #[tokio::test]
    async fn a_page_boundary_is_a_name_not_an_index() {
        // Keyset paging: the token names the last entry served, so an insertion before it can neither duplicate
        // an already-returned entry nor push an unseen one past the window.
        let backend = backend_with_databases(&["ops", "staging"]).await;
        let first = body_json(get_on(backend.clone(), "/databases?max_results=2").await).await;
        assert_eq!(first["databases"], json!(["fluss", "ops"]));
        let token = first["next_page_token"]
            .as_str()
            .expect("token")
            .to_string();

        // Concurrent DDL inserts a name that sorts before the page boundary.
        backend
            .create_database(&CreateDatabaseRequest {
                name: "analytics".to_string(),
                comment: None,
                custom_properties: HashMap::new(),
            })
            .await
            .unwrap();

        let second = body_json(
            get_on(
                backend,
                &format!("/databases?page_token={token}&max_results=2"),
            )
            .await,
        )
        .await;
        assert_eq!(
            second["databases"],
            json!(["staging"]),
            "an offset would have re-served `ops` or skipped `staging`"
        );
    }

    #[tokio::test]
    async fn a_token_from_another_collection_is_rejected() {
        let first = get_json("/databases/fluss/tables?max_results=1").await;
        let token = first["next_page_token"]
            .as_str()
            .expect("token")
            .to_string();

        // The same token, replayed against every collection it does not belong to.
        for path in [
            format!("/databases?page_token={token}"),
            format!("/databases/other/tables?page_token={token}"),
            format!("/databases/fluss/tables/orders/partitions?page_token={token}"),
        ] {
            let response = get(&path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            let json = body_json(response).await;
            assert_eq!(json["error"]["code"], "invalid_argument", "{path}");
            assert!(
                json["error"]["message"]
                    .as_str()
                    .expect("message")
                    .contains("does not belong"),
                "{path}: {json}"
            );
        }
    }

    #[tokio::test]
    async fn a_malformed_token_is_rejected_before_the_parent_is_resolved() {
        // The parent database does not exist either; the token must still decide the status, so that a bad token
        // is a deterministic 400 rather than a 404 that depends on catalog contents.
        for path in [
            "/databases?page_token=not-base64!!",
            "/databases?page_token=bm90IGpzb24",
            "/databases/missing/tables?page_token=not-base64!!",
        ] {
            let response = get(path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(
                body_json(response).await["error"]["code"],
                "invalid_argument",
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn page_sizes_out_of_range_and_unknown_parameters_are_rejected() {
        for path in [
            "/databases?max_results=0",
            "/databases?max_results=1001",
            "/databases/fluss/tables?max_results=0",
            "/databases/fluss/tables/orders/partitions?max_results=1001",
            // `page_size` is not the wire name of the page bound; unknown query keys are refused outright.
            "/databases?page_size=2",
            "/databases?unknown=1",
        ] {
            let response = get(path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(
                body_json(response).await["error"]["code"],
                "invalid_argument",
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn describe_endpoints_reject_stray_query_parameters() {
        for path in [
            "/databases/fluss?unexpected=1",
            "/databases/fluss/tables/users?unexpected=1",
        ] {
            let response = get(path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        }
    }

    #[tokio::test]
    async fn listing_partitions_of_an_unpartitioned_table_is_invalid() {
        let response = get("/databases/fluss/tables/users/partitions").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_json(response).await["error"]["code"],
            "invalid_argument"
        );
    }

    #[tokio::test]
    async fn every_read_honours_the_accept_header() {
        for path in [
            "/databases",
            "/databases/fluss",
            "/databases/fluss/tables",
            "/databases/fluss/tables/users",
            "/databases/fluss/tables/orders/partitions",
        ] {
            let response = get_with_accept(
                Arc::new(TestBackend::new()),
                path,
                Some("application/json;q=0, */*;q=1"),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE, "{path}");
        }
    }

    #[tokio::test]
    async fn catalog_metrics_use_bounded_labels_and_hide_resource_names() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);

        assert_eq!(get("/databases/fluss").await.status(), StatusCode::OK);

        let output = handle.render();
        let line = output
            .lines()
            .find(|line| line.starts_with("fluss_gateway_catalog_operations_total"))
            .expect("a catalog operation metric is emitted");
        assert!(line.contains("cluster=\"default\""), "{line}");
        assert!(line.contains("operation=\"describe_database\""), "{line}");
        assert!(line.contains("result=\"success\""), "{line}");
        assert!(!line.contains("database="), "{line}");
        assert!(!line.contains("fluss\""), "{line}");
    }

    #[tokio::test]
    async fn repeated_and_interleaved_pages_are_identical() {
        // Statelessness: a page is a pure function of its token, so serving it twice — or out of order — cannot
        // differ. This is what lets any instance answer any page.
        let backend = backend_with_databases(&["ops", "staging"]).await;
        let first = body_json(get_on(backend.clone(), "/databases?max_results=2").await).await;
        let token = first["next_page_token"]
            .as_str()
            .expect("token")
            .to_string();
        let second =
            body_json(get_on(backend.clone(), &format!("/databases?page_token={token}")).await)
                .await;

        assert_eq!(
            body_json(get_on(backend.clone(), "/databases?max_results=2").await).await,
            first
        );
        assert_eq!(
            body_json(get_on(backend, &format!("/databases?page_token={token}")).await).await,
            second
        );
    }
}
