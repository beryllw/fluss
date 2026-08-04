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

//! The batch write endpoint.
//!
//! Two failure classes are reported differently. A **validation** failure — unknown column, type mismatch,
//! missing primary key, an operation the table kind does not accept — rejects the whole batch with 400 before
//! anything is written, so the request is deterministic and safe to retry. A **delivery** failure after
//! submission is reported per entry inside a 200 response, because part of the batch may already be durable.
//!
//! The only request bounds are input validation: the 32 MiB body cap (413) and `[write] max_rows` (413). There
//! is no write concurrency permit and no 429.
//!
//! The handler below is registered with its final route and returns `501 UNSUPPORTED` until the write path is
//! implemented.

use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{RequestId, RestState, error_response};
use axum::Extension;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Records routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(write_records))
}

/// Writes ordered entries after complete schema-aware preflight.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records",
    operation_id = "writeRecords",
    tag = "records",
    description = "Writes a fully preflighted batch in input order. Delivery is at least once from the caller's \
                   perspective: the gateway never resubmits after submission, but client accumulator retries and \
                   caller retries can duplicate log appends. An entry whose completion is `unknown` may have been \
                   applied, and entries sharing one accumulator batch can share that verdict.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorEnvelopeSchema)
    )
)]
pub(crate) async fn write_records(
    State(_state): State<RestState>,
    Extension(request_id): Extension<RequestId>,
    Path((_cluster, _database, _table)): Path<(String, String, String)>,
    _uri: Uri,
    _headers: HeaderMap,
) -> Response {
    error_response(
        &crate::error::GatewayError::unsupported(
            "the gateway cannot write records yet: the write path is not implemented",
        ),
        &request_id,
    )
}
