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
//! `POST .../records/lookup` resolves a batch of primary keys. Misses are outcomes (`found: false`), never
//! errors, and at most `[lookup] max_keys` keys are accepted per request.
//!
//! `POST .../records/prefix-lookup` resolves bounded prefix ranges. At most `[lookup] max_prefixes` prefixes are
//! accepted, the prefix columns must cover the table's bucket keys (rejected with a clear 400 otherwise), and
//! each prefix is truncated at `[lookup] max_rows_per_prefix` with a `truncated` flag rather than an error. A
//! prefix that matches nothing returns an empty row list, not a miss.
//!
//! Both handlers are registered with their final routes and return `501 UNSUPPORTED` until the lookup path is
//! implemented.

use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{RequestId, RestState, error_response};
use axum::Extension;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Lookup routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(lookup))
        .routes(routes!(prefix_lookup))
}

/// Renders the placeholder response for a lookup that is not implemented yet.
fn not_implemented(operation: &str, request_id: &RequestId) -> Response {
    error_response(
        &crate::error::GatewayError::unsupported(format!(
            "the gateway cannot {operation} yet: the lookup path is not implemented"
        )),
        request_id,
    )
}

/// Looks up a batch of primary keys, one outcome per key in input order.
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
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn lookup(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    not_implemented("look up records", &request_id)
}

/// Looks up bounded prefix ranges, one outcome per prefix in input order.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records/prefix-lookup",
    operation_id = "prefixLookupRecords",
    tag = "lookup",
    description = "Resolves bounded prefix ranges against a primary-key table. The prefix columns must cover the \
                   table's bucket keys. Each prefix is truncated at the configured per-prefix row cap and flagged \
                   with `truncated`; a prefix that matches nothing returns an empty row list.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn prefix_lookup(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    not_implemented("run prefix lookups", &request_id)
}
