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
//! **Granularity honesty.** A per-entry failure is reported at the precision the client provides, which is the
//! accumulator *batch*, not the row (PLAN §13). Entries that shared one batch can therefore share one verdict.
//! `completion: "rejected"` means the row is provably not written; `completion: "unknown"` means it may have
//! been committed and a blind retry can duplicate it.
//!
//! Request shape:
//!
//! ```json
//! {
//!   "partial_update_columns": ["id", "name"],
//!   "entries": [
//!     {"id": "e1", "upsert": {"id": 1, "name": "ada"}},
//!     {"id": "e2", "delete": {"id": 2}},
//!     {"id": "e3", "append": {"ts": "1700000000000", "message": "hi"}}
//!   ]
//! }
//! ```
//!
//! Each entry carries exactly one of `append`, `upsert`, or `delete`, whose value is the row object. `id` is an
//! opaque caller correlation value that every outcome echoes back; duplicates within one request are rejected.

use crate::application::{WriteEntry, WriteOperation, WriteRequest};
use crate::auth::Principal;
use crate::backend::model::{TableRef, WriteCompletion, WriteResult};
use crate::error::GatewayError;
use crate::observability;
use crate::protocol::rest::input::{WriteInputOperation, parse_write_input};
use crate::protocol::rest::limits::ensure_json_acceptable;
use crate::protocol::rest::openapi::ErrorEnvelopeSchema;
use crate::protocol::rest::{
    RequestDeadline, RequestId, RestState, application_context, ensure_no_query, error_response,
    json_response, metric_cluster, validate_json_content_type,
};
use axum::Extension;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use serde::Serialize;
use std::time::Instant;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Records routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(write_records))
}

/// Schema-only append entry. The row is a free-form object validated against the table schema at runtime.
#[derive(Debug, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AppendEntrySchema {
    /// Opaque caller correlation value, unique within the request.
    pub id: String,
    #[schema(value_type = Object)]
    pub append: serde_json::Value,
}

/// Schema-only upsert entry.
#[derive(Debug, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpsertEntrySchema {
    /// Opaque caller correlation value, unique within the request.
    pub id: String,
    #[schema(value_type = Object)]
    pub upsert: serde_json::Value,
}

/// Schema-only delete entry. The row needs only the primary-key columns.
#[derive(Debug, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteEntrySchema {
    /// Opaque caller correlation value, unique within the request.
    pub id: String,
    #[schema(value_type = Object)]
    pub delete: serde_json::Value,
}

/// One request entry. Exactly one row operation is required.
///
/// The document describes the shape only. The runtime parser is
/// [`crate::protocol::rest::input::parse_write_input`], which preserves number lexemes and duplicate row field
/// names so that exactness and malformed rows survive to schema-aware validation.
#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum WriteEntrySchema {
    Append(AppendEntrySchema),
    Upsert(UpsertEntrySchema),
    Delete(DeleteEntrySchema),
}

/// Schema-only write request body.
#[derive(Debug, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteRequestSchema {
    /// Columns an upsert batch targets, applied to the whole batch. Omit it to supply every column.
    ///
    /// Partial update is a property of the writer, not of a row, so the list is batch level and a batch that
    /// sets it cannot contain deletes. It must include every primary-key column, and every column it omits
    /// must be nullable.
    ///
    /// Every targeted column must carry a value in every entry of the batch, and that value must not be
    /// `null` even when the column is nullable — a targeted column is one the caller promised to supply. Use a
    /// full upsert, without this field, to write `null` into a column.
    pub partial_update_columns: Option<Vec<String>>,
    #[schema(min_items = 1)]
    pub entries: Vec<WriteEntrySchema>,
}

/// One entry that was acknowledged as written.
#[derive(Debug, Serialize, ToSchema)]
pub struct WriteSuccessResponse {
    pub id: String,
}

/// Whether a failed entry is proven not to have been applied, or may have been applied.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WriteCompletionResponse {
    /// The row never reached the server. Retrying cannot duplicate it.
    Rejected,
    /// The row may already be committed. Retrying can duplicate it.
    Unknown,
}

/// One entry that failed after submission.
#[derive(Debug, Serialize, ToSchema)]
pub struct WriteFailureResponse {
    pub id: String,
    /// A stable request-level error code, plus `storage_backpressure` — a KV write rejected by
    /// storage backpressure after the client retry budget was exhausted. It is retriable and
    /// occurs only at entry level, never as a whole-request HTTP status (FIP-49).
    pub error_code: String,
    pub message: String,
    pub completion: WriteCompletionResponse,
    pub retryable: bool,
}

/// Ordered, entry-correlated outcomes of one batch.
#[derive(Debug, Serialize, ToSchema)]
pub struct WriteResponse {
    pub row_count: usize,
    pub success_count: usize,
    pub error_count: usize,
    /// Successes in input order.
    pub successes: Vec<WriteSuccessResponse>,
    /// Failures in input order. A verdict can be shared by entries that landed in one accumulator batch.
    pub failures: Vec<WriteFailureResponse>,
}

/// Writes ordered entries after complete schema-aware preflight.
#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records",
    operation_id = "writeRecords",
    tag = "records",
    description = "Writes a fully preflighted batch in input order. A validation failure rejects the whole \
                   batch with 400 before anything is submitted. Delivery is at least once from the caller's \
                   perspective: the gateway never resubmits after submission, but client accumulator retries \
                   and caller retries can duplicate log appends. An entry whose completion is `unknown` may \
                   have been applied, and entries sharing one accumulator batch can share that verdict.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = WriteRequestSchema, content_type = "application/json"),
    responses(
        (status = 200, description = "Ordered entry outcomes. Completion can be unknown after submission", body = WriteResponse),
        (status = 400, description = "Malformed request, or preflight rejected the whole batch", body = ErrorEnvelopeSchema),
        (status = 404, description = "Cluster or table not found", body = ErrorEnvelopeSchema),
        (status = 406, description = "JSON response is not acceptable", body = ErrorEnvelopeSchema),
        (status = 409, description = "The table changed between preflight and submission", body = ErrorEnvelopeSchema),
        (status = 413, description = "Body or row limit exceeded", body = ErrorEnvelopeSchema),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelopeSchema),
        (status = 503, description = "Backend unavailable", body = ErrorEnvelopeSchema),
        (status = 504, description = "Deadline exceeded before submission", body = ErrorEnvelopeSchema)
    )
)]
#[allow(clippy::too_many_arguments)] // Axum extractors, one per request-scoped concern.
pub(crate) async fn write_records(
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
    let cluster_label = metric_cluster(&state, &cluster);
    let result = run_write(
        &state,
        &request_id,
        deadline,
        &principal,
        &cluster,
        &cluster_label,
        database,
        table,
        &uri,
        &headers,
        &body,
    )
    .await;
    match result {
        Ok(response) => {
            let outcome = if response.error_count == 0 {
                "success"
            } else if response.success_count == 0 {
                "failure"
            } else {
                "partial"
            };
            observability::write_request(&cluster_label, outcome, started.elapsed());
            record_outcome_rows(&cluster_label, &response);
            json_response(&response).unwrap_or_else(|error| error_response(&error, &request_id))
        }
        Err(error) => {
            observability::write_request(&cluster_label, "request_error", started.elapsed());
            error_response(&error, &request_id)
        }
    }
}

/// Validates the request envelope, dispatches the batch, and shapes the response.
///
/// The row cap is checked before the cluster is even resolved, so an oversized batch never reaches the
/// application layer. There is deliberately no concurrency permit here: backpressure comes from the Fluss
/// client's own writer buffer, not from the gateway rejecting callers.
#[allow(clippy::too_many_arguments)]
async fn run_write(
    state: &RestState,
    request_id: &RequestId,
    deadline: RequestDeadline,
    principal: &Principal,
    cluster: &str,
    cluster_label: &str,
    database: String,
    table: String,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<WriteResponse, GatewayError> {
    ensure_no_query(uri)?;
    ensure_json_acceptable(headers)?;
    validate_json_content_type(headers)?;
    let input = parse_write_input(body)?;
    if input.entries.len() > state.write_limits.max_rows {
        return Err(GatewayError::limit_exceeded(format!(
            "write request has {} rows but the limit is {}",
            input.entries.len(),
            state.write_limits.max_rows
        )));
    }
    observability::write_accepted(cluster_label, input.entries.len(), body.len() as u64);

    let context = application_context(request_id, deadline, principal, cluster)?;
    let request = WriteRequest {
        table: TableRef::new(database, table),
        partial_update_columns: input.partial_update_columns,
        entries: input
            .entries
            .into_iter()
            .map(|entry| WriteEntry {
                id: entry.id,
                operation: match entry.operation {
                    WriteInputOperation::Append(row) => WriteOperation::Append(row),
                    WriteInputOperation::Upsert(row) => WriteOperation::Upsert(row),
                    WriteInputOperation::Delete(row) => WriteOperation::Delete(row),
                },
            })
            .collect(),
    };

    let backend_started = Instant::now();
    let result = state.application.write(&context, request).await;
    observability::write_backend_duration(cluster_label, backend_started.elapsed());
    Ok(to_response(result?))
}

/// Splits the ordered backend verdicts into the success and failure lists of the response.
fn to_response(mut result: WriteResult) -> WriteResponse {
    result.entries.sort_by_key(|entry| entry.input_index);
    let row_count = result.entries.len();
    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for entry in result.entries {
        match entry.failure {
            None => successes.push(WriteSuccessResponse { id: entry.id }),
            Some(failure) => failures.push(WriteFailureResponse {
                id: entry.id,
                error_code: failure.error_code,
                message: failure.message,
                completion: match failure.completion {
                    WriteCompletion::Rejected => WriteCompletionResponse::Rejected,
                    WriteCompletion::Unknown => WriteCompletionResponse::Unknown,
                },
                retryable: failure.retryable,
            }),
        }
    }
    WriteResponse {
        row_count,
        success_count: successes.len(),
        error_count: failures.len(),
        successes,
        failures,
    }
}

fn record_outcome_rows(cluster: &str, response: &WriteResponse) {
    observability::write_outcome_rows(cluster, "success", response.success_count);
    let rejected = response
        .failures
        .iter()
        .filter(|failure| matches!(failure.completion, WriteCompletionResponse::Rejected))
        .count();
    observability::write_outcome_rows(cluster, "rejected", rejected);
    observability::write_outcome_rows(cluster, "unknown", response.error_count - rejected);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::TestBackend;
    use crate::protocol::rest::test_support;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn post(table: &str, body: String) -> Request<Body> {
        Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/v1/clusters/default/databases/fluss/tables/{table}/records"
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    fn users(body: &str) -> Request<Body> {
        post("users", body.to_string())
    }

    async fn json(response: Response) -> serde_json::Value {
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }

    async fn send(app: Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
        let response = app.oneshot(request).await.unwrap();
        (response.status(), json(response).await)
    }

    #[tokio::test]
    async fn a_mixed_primary_key_batch_answers_in_input_order() {
        let backend = Arc::new(TestBackend::new());
        let (status, body) = send(
            test_support::app(backend.clone()),
            users(
                r#"{"entries":[{"id":"u1","upsert":{"id":1,"name":"alice"}},{"id":"d2","delete":{"id":2}},{"id":"u3","upsert":{"id":3,"name":null}}]}"#,
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["row_count"], 3);
        assert_eq!(body["success_count"], 3);
        assert_eq!(body["error_count"], 0);
        assert_eq!(body["successes"][0]["id"], "u1");
        assert_eq!(body["successes"][1]["id"], "d2");
        assert_eq!(body["successes"][2]["id"], "u3");
        assert!(body["failures"].as_array().unwrap().is_empty());

        let writes = backend.recorded_writes();
        assert_eq!(
            writes
                .iter()
                .map(|write| (write.input_index, write.operation))
                .collect::<Vec<_>>(),
            vec![(0, "upsert"), (1, "delete"), (2, "upsert")]
        );
    }

    #[tokio::test]
    async fn a_log_table_accepts_appends() {
        let backend = Arc::new(TestBackend::new());
        let (status, body) = send(
            test_support::app(backend.clone()),
            post(
                "events",
                r#"{"entries":[{"id":"log","append":{"ts":"9007199254740993","message":"event"}}]}"#
                    .to_string(),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success_count"], 1);
        assert_eq!(backend.recorded_writes()[0].operation, "append");
    }

    /// A partial update may target a NOT NULL column, and must still refuse to omit one.
    ///
    /// The stock fixture has no primary-key table with a non-nullable non-key column, so the table is created
    /// through the DDL endpoint first. This is the shape the native client used to reject outright.
    #[tokio::test]
    async fn a_partial_update_targets_a_non_nullable_column_end_to_end() {
        let backend = Arc::new(TestBackend::new());
        let app = test_support::app(backend.clone());

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases/fluss/tables")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"table_name":"required","columns":[
                            {"name":"id","data_type":{"type":"INT","nullable":false}},
                            {"name":"label","data_type":{"type":"STRING","nullable":false}},
                            {"name":"note","data_type":{"type":"STRING","nullable":true}}],
                            "primary_key":{"columns":["id"]}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let (status, body) = send(
            app.clone(),
            post(
                "required",
                r#"{"partial_update_columns":["id","label"],"entries":[{"id":"p","upsert":{"id":1,"label":"set"}}]}"#
                    .to_string(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["success_count"], 1);

        let (status, body) = send(
            app,
            post(
                "required",
                r#"{"partial_update_columns":["id","note"],"entries":[{"id":"p","upsert":{"id":1,"note":"x"}}]}"#
                    .to_string(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("omitted column `label` must be nullable"),
            "{body}"
        );
        assert_eq!(
            backend.recorded_writes().len(),
            1,
            "only the legal write ran"
        );
    }

    #[tokio::test]
    async fn a_partial_update_batch_records_its_targeted_columns() {
        let backend = Arc::new(TestBackend::new());
        let (status, _) = send(
            test_support::app(backend.clone()),
            users(
                r#"{"partial_update_columns":["id","name"],"entries":[{"id":"patch","upsert":{"id":7,"name":"new"}}]}"#,
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            backend.recorded_writes()[0].partial_update_columns,
            Some(vec!["id".to_string(), "name".to_string()])
        );
    }

    /// Every validation-failure class is a 400 that submits nothing.
    #[tokio::test]
    async fn validation_failures_reject_the_whole_batch_before_anything_is_written() {
        for (label, table, body) in [
            (
                "unknown column",
                "users",
                r#"{"entries":[{"id":"a","upsert":{"id":1,"ghost":"x"}}]}"#,
            ),
            (
                "type mismatch",
                "users",
                r#"{"entries":[{"id":"a","upsert":{"id":"not a number","name":"x"}}]}"#,
            ),
            (
                "missing primary key",
                "users",
                r#"{"entries":[{"id":"a","delete":{"name":"x"}}]}"#,
            ),
            (
                "append on a primary-key table",
                "users",
                r#"{"entries":[{"id":"a","append":{"id":1,"name":"x"}}]}"#,
            ),
            (
                "upsert on a log table",
                "events",
                r#"{"entries":[{"id":"a","upsert":{"ts":1,"message":"x"}}]}"#,
            ),
            (
                "partial update omitting the primary key",
                "users",
                r#"{"partial_update_columns":["name"],"entries":[{"id":"a","upsert":{"name":"x"}}]}"#,
            ),
            (
                "partial update naming an unknown column",
                "users",
                r#"{"partial_update_columns":["id","ghost"],"entries":[{"id":"a","upsert":{"id":1}}]}"#,
            ),
            (
                "partial update nulling a targeted column",
                "users",
                r#"{"partial_update_columns":["id","name"],"entries":[{"id":"a","upsert":{"id":1,"name":null}}]}"#,
            ),
            (
                "partial update omitting a targeted column",
                "users",
                r#"{"partial_update_columns":["id","name"],"entries":[{"id":"a","upsert":{"id":1}}]}"#,
            ),
            (
                "partial update mixed with a delete",
                "users",
                r#"{"partial_update_columns":["id","name"],"entries":[{"id":"a","upsert":{"id":1,"name":"x"}},{"id":"b","delete":{"id":2}}]}"#,
            ),
            (
                "duplicate entry ids",
                "users",
                r#"{"entries":[{"id":"same","upsert":{"id":1,"name":"a"}},{"id":"same","delete":{"id":2}}]}"#,
            ),
            (
                "duplicate row column",
                "users",
                r#"{"entries":[{"id":"a","upsert":{"id":1,"id":2}}]}"#,
            ),
            ("empty batch", "users", r#"{"entries":[]}"#),
            (
                "two operations on one entry",
                "users",
                r#"{"entries":[{"id":"a","upsert":{"id":1},"delete":{"id":1}}]}"#,
            ),
            (
                "unknown envelope field",
                "users",
                r#"{"entries":[{"id":"a","upsert":{"id":1}}],"extra":1}"#,
            ),
        ] {
            let backend = Arc::new(TestBackend::new());
            let (status, json) = send(
                test_support::app(backend.clone()),
                post(table, body.to_string()),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {json}");
            assert_eq!(json["error"]["code"], "invalid_argument", "{label}");
            assert!(
                backend.recorded_writes().is_empty(),
                "{label}: preflight must submit nothing"
            );
        }
    }

    #[tokio::test]
    async fn an_injected_delivery_failure_is_a_200_with_per_entry_outcomes() {
        let backend = Arc::new(TestBackend::new());
        backend.inject_write_failure(vec![1], WriteCompletion::Unknown, "unavailable", true);
        let (status, body) = send(
            test_support::app(backend),
            users(
                r#"{"entries":[{"id":"first","upsert":{"id":1,"name":"a"}},{"id":"second","upsert":{"id":2,"name":"b"}},{"id":"third","delete":{"id":3}}]}"#,
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["row_count"], 3);
        assert_eq!(body["success_count"], 2);
        assert_eq!(body["error_count"], 1);
        assert_eq!(body["successes"][0]["id"], "first");
        assert_eq!(body["successes"][1]["id"], "third");
        assert_eq!(body["failures"][0]["id"], "second");
        assert_eq!(body["failures"][0]["completion"], "unknown");
        assert_eq!(body["failures"][0]["error_code"], "unavailable");
        assert_eq!(body["failures"][0]["retryable"], true);
    }

    /// The FIP-49 `storage_backpressure` condition surfaces only inside `failures[]` of a 200
    /// partial-success response — never as a whole-request HTTP status — as a retriable rejected
    /// entry the caller retries individually once pressure drains.
    #[tokio::test]
    async fn storage_backpressure_is_an_entry_level_retriable_code_never_a_request_status() {
        let backend = Arc::new(TestBackend::new());
        backend.inject_write_failure(
            vec![1],
            WriteCompletion::Rejected,
            "storage_backpressure",
            true,
        );
        let (status, body) = send(
            test_support::app(backend),
            users(
                r#"{"entries":[{"id":"first","upsert":{"id":1,"name":"a"}},{"id":"second","upsert":{"id":2,"name":"b"}}]}"#,
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success_count"], 1);
        assert_eq!(body["error_count"], 1);
        assert_eq!(body["failures"][0]["id"], "second");
        assert_eq!(body["failures"][0]["error_code"], "storage_backpressure");
        assert_eq!(body["failures"][0]["completion"], "rejected");
        assert_eq!(body["failures"][0]["retryable"], true);
        assert!(
            body.get("error").is_none(),
            "no request-level error envelope"
        );
    }

    #[tokio::test]
    async fn a_rejected_entry_is_distinguished_from_an_unknown_one() {
        let backend = Arc::new(TestBackend::new());
        backend.inject_write_failure(
            vec![0],
            WriteCompletion::Rejected,
            "resource_exhausted",
            true,
        );
        let (status, body) = send(
            test_support::app(backend),
            users(
                r#"{"entries":[{"id":"only","upsert":{"id":1,"name":"a"}},{"id":"other","upsert":{"id":2,"name":"b"}}]}"#,
            ),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["failures"][0]["id"], "only");
        assert_eq!(body["failures"][0]["completion"], "rejected");
        assert_eq!(body["successes"][0]["id"], "other");
    }

    #[tokio::test]
    async fn a_schema_change_between_preflight_and_submission_is_reported_as_a_conflict() {
        let backend = Arc::new(TestBackend::new());
        backend.evolve_schema_before_next_write();
        let (status, body) = send(
            test_support::app(backend.clone()),
            users(r#"{"entries":[{"id":"a","upsert":{"id":1,"name":"x"}}]}"#),
        )
        .await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "failed_precondition");
        assert_eq!(body["error"]["details"]["resource_kind"], "table");
        assert!(backend.recorded_writes().is_empty());
    }

    /// The only per-request bound is the row cap, and exceeding it is 413 — never 429.
    #[tokio::test]
    async fn too_many_rows_is_a_413_and_never_a_429() {
        let backend = Arc::new(TestBackend::new());
        let state = test_support::state_with_backend(backend.clone());
        let max_rows = state.write_limits.max_rows;
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());

        let entries = (0..=max_rows)
            .map(|index| format!(r#"{{"id":"e{index}","delete":{{"id":{index}}}}}"#))
            .collect::<Vec<_>>()
            .join(",");
        let (status, body) =
            send(app, post("users", format!(r#"{{"entries":[{entries}]}}"#))).await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "limit_exceeded");
        assert!(backend.recorded_writes().is_empty());
    }

    #[tokio::test]
    async fn an_unknown_table_is_a_404_naming_the_resource() {
        let backend = Arc::new(TestBackend::new());
        let (status, body) = send(
            test_support::app(backend),
            post(
                "ghost",
                r#"{"entries":[{"id":"a","upsert":{"id":1}}]}"#.to_string(),
            ),
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
        assert_eq!(body["error"]["details"]["resource_kind"], "table");
    }

    #[tokio::test]
    async fn stray_query_parameters_are_rejected_before_anything_is_written() {
        let backend = Arc::new(TestBackend::new());
        let response = test_support::app(backend.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases/fluss/tables/users/records?foo=bar")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"entries":[{"id":"u1","upsert":{"id":1,"name":"alice"}}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(response).await["error"]["code"], "invalid_argument");
        assert!(backend.recorded_writes().is_empty());
    }

    #[tokio::test]
    async fn content_negotiation_is_enforced() {
        let app = test_support::app(Arc::new(TestBackend::new()));

        let unsupported = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases/fluss/tables/users/records")
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::from(r#"{"entries":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsupported.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let unacceptable = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases/fluss/tables/users/records")
                    .header(header::CONTENT_TYPE, "application/vnd.fluss+json")
                    .header(header::ACCEPT, "text/plain")
                    .body(Body::from(r#"{"entries":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unacceptable.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn the_route_is_documented_with_its_request_body_and_no_429() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let document = json(response).await;
        let operation = &document["paths"]["/v1/clusters/{cluster}/databases/{database}/tables/{table}/records"]
            ["post"];

        assert_eq!(operation["operationId"], "writeRecords");
        assert!(
            operation["requestBody"]["content"]["application/json"].is_object(),
            "the Bytes extractor must not erase the documented request body: {operation}"
        );
        assert!(operation["responses"]["200"].is_object());
        assert!(
            operation["responses"]["429"].is_null(),
            "the gateway does not rate limit"
        );
        assert!(document["components"]["schemas"]["WriteResponse"].is_object());
    }
}
