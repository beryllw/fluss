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
//! self-contained (see [`crate::application::pagination`]), so any instance serves any page.
//!
//! The handlers below are registered with their final routes and return `501 UNSUPPORTED` until the catalog read
//! path is implemented.

use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{RequestId, RestState, error_response};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::{Extension, http::Uri};
use serde::Deserialize;
use utoipa::IntoParams;
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
#[derive(Debug, Deserialize, IntoParams)]
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

/// Renders the placeholder response for a catalog read that is not implemented yet.
fn not_implemented(operation: &str, request_id: &RequestId) -> Response {
    error_response(
        &crate::error::GatewayError::unsupported(format!(
            "the gateway cannot {operation} yet: the catalog read path is not implemented"
        )),
        request_id,
    )
}

/// Lists database names of one cluster.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases",
    operation_id = "listDatabases",
    tag = "metadata",
    params(("cluster" = String, Path, description = "Configured cluster ID"), PageParams),
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn list_databases(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path(_cluster): Path<String>,
    _uri: Uri,
) -> Response {
    not_implemented("list databases", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn describe_database(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database)): Path<(String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("describe a database", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn list_tables(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database)): Path<(String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("list tables", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn describe_table(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("describe a table", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn list_partitions(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("list partitions", &request_id)
}
