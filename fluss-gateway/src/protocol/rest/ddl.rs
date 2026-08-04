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
//! strings. `PATCH` on a table applies its ordered list of changes in one native request.
//!
//! The handlers below are registered with their final routes and return `501 UNSUPPORTED` until the DDL path is
//! implemented.

use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{RequestId, RestState, error_response};
use axum::Extension;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
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

/// Renders the placeholder response for a mutation that is not implemented yet.
fn not_implemented(operation: &str, request_id: &RequestId) -> Response {
    error_response(
        &crate::error::GatewayError::unsupported(format!(
            "the gateway cannot {operation} yet: the DDL path is not implemented"
        )),
        request_id,
    )
}

/// Creates one database.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases",
    operation_id = "createDatabase",
    tag = "ddl",
    params(("cluster" = String, Path, description = "Configured cluster ID")),
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn create_database(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path(_cluster): Path<String>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    not_implemented("create a database", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn drop_database(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database)): Path<(String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("drop a database", &request_id)
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
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn create_table(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database)): Path<(String, String)>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    not_implemented("create a table", &request_id)
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
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn alter_table(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    not_implemented("alter a table", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn drop_table(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("drop a table", &request_id)
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
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn create_partition(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    not_implemented("create a partition", &request_id)
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
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn drop_partition(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table, _partition)): Path<(String, String, String, String)>,
    _uri: Uri,
) -> Response {
    not_implemented("drop a partition", &request_id)
}
