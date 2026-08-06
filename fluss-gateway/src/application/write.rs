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

//! Protocol-neutral write models, all-or-nothing preflight, and the write half of [`GatewayService`].
//!
//! Two invariants are fixed by design and are not configurable per request. Preflight is
//! **all-or-nothing**: every entry is validated and decoded against the authoritative table schema before the
//! first row is handed to a native writer, so a validation failure rejects the whole batch with nothing
//! submitted. And every entry carries a **finite delivery deadline** derived from `[write] max_delivery_time`
//! and clamped to the request deadline minus [`WRITE_RESPONSE_BUDGET`], so a write can never outlive its HTTP
//! request.
//!
//! The split between the two failure classes is deliberate (PLAN §4.6). A validation failure — unknown column,
//! type mismatch, missing primary key, an operation the table kind does not accept, a malformed
//! `partial_update_columns` — is deterministic and retry-safe, so it is reported once for the whole request.
//! A delivery failure after submission is reported per entry, because part of the batch may already be durable.
//!
//! Preflight also distinguishes failures that a stale metadata cache could explain from failures in the data
//! itself. Only the former is treated as a schema mismatch, which buys exactly one forced metadata refresh and
//! one repeated preflight before the failure is returned to the caller.

use crate::application::input_decode::RowDecodeError;
use crate::application::service::resource_error;
use crate::application::{
    GatewayService, InputColumn, InputValue, RequestContext, SchemaDecoder, TableDescription,
    TableKind,
};
use crate::backend::model::{
    PreparedWriteEntry, PreparedWriteOperation, PreparedWriteRequest, TableRef, WriteResult,
};
use crate::config::WRITE_RESPONSE_BUDGET;
use crate::error::GatewayError;
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// One write request before schema-aware validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRequest {
    pub table: TableRef,
    /// Columns an upsert batch targets. `None` means every column is supplied.
    pub partial_update_columns: Option<Vec<String>>,
    pub entries: Vec<WriteEntry>,
}

/// One entry identified by an opaque caller correlation value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEntry {
    pub id: String,
    pub operation: WriteOperation,
}

/// Exactly one table mutation and its untyped protocol-neutral row object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOperation {
    Append(InputValue),
    Upsert(InputValue),
    Delete(InputValue),
}

impl WriteOperation {
    /// Stable operation name used in messages and deterministic recordings.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Append(_) => "append",
            Self::Upsert(_) => "upsert",
            Self::Delete(_) => "delete",
        }
    }

    /// The untyped row object carried by this operation.
    pub fn row(&self) -> &InputValue {
        match self {
            Self::Append(row) | Self::Upsert(row) | Self::Delete(row) => row,
        }
    }
}

/// One preflight failure plus a structural staleness signal consumed by [`GatewayService::write`].
///
/// The signal is true only when the failure refers to the *cached table shape* — an unknown or missing column,
/// a partial-update target that is not in the schema — so that one forced metadata refresh plus one repeated
/// preflight may resolve it. Plain data errors such as range, format, and nullability failures never set it,
/// because refreshing metadata cannot change the verdict. The wrapped [`GatewayError`] is the unchanged
/// client-visible error.
#[derive(Debug)]
pub(crate) struct PreflightError {
    schema_mismatch: bool,
    error: GatewayError,
}

impl PreflightError {
    fn schema_mismatch(error: GatewayError) -> Self {
        Self {
            schema_mismatch: true,
            error,
        }
    }

    /// True when refreshed table metadata may resolve the failure.
    pub(crate) fn is_schema_mismatch(&self) -> bool {
        self.schema_mismatch
    }

    /// Converts into the client-visible error at the service boundary.
    pub(crate) fn into_gateway_error(self) -> GatewayError {
        self.error
    }
}

impl From<GatewayError> for PreflightError {
    fn from(error: GatewayError) -> Self {
        Self {
            schema_mismatch: false,
            error,
        }
    }
}

impl From<RowDecodeError> for PreflightError {
    fn from(error: RowDecodeError) -> Self {
        Self {
            schema_mismatch: error.is_schema_mismatch(),
            error: error.into_gateway_error(),
        }
    }
}

/// Validates the complete request and decodes every row before the first native submission.
///
/// The request is borrowed so the common success path never deep-copies the input rows: only the entry
/// identifiers and the request header fields are copied into the prepared request.
pub(crate) fn preflight(
    cluster_id: &str,
    request: &WriteRequest,
    description: &TableDescription,
    request_deadline: Instant,
    max_delivery_time: Duration,
) -> Result<PreparedWriteRequest, PreflightError> {
    if request.entries.is_empty() {
        return Err(GatewayError::invalid_argument(
            "write request must contain at least one entry",
        )
        .into());
    }
    if description.table != request.table {
        return Err(GatewayError::internal(
            "write preflight received metadata for a different table",
        )
        .into());
    }
    let mut ids = HashSet::with_capacity(request.entries.len());
    for entry in &request.entries {
        if !ids.insert(entry.id.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate write entry ID `{}`",
                entry.id
            ))
            .into());
        }
    }

    validate_operations(
        description,
        &request.entries,
        request.partial_update_columns.as_deref(),
    )?;
    // The decoder always carries the *complete* table schema in declared order, even for a partial update: a
    // decoded row is positional and a native writer indexes it by position, so a decoder built from the
    // targeted column subset would mis-index every value. The subset is expressed through
    // `decode_sparse_row`, which still yields a full-arity row.
    let decoder = SchemaDecoder::new(input_columns(description))?;
    let sparse_columns =
        sparse_target_columns(description, request.partial_update_columns.as_deref())?;

    let mut prepared = Vec::with_capacity(request.entries.len());
    for (input_index, entry) in request.entries.iter().enumerate() {
        let row = match &entry.operation {
            WriteOperation::Append(value) => decoder.decode_row(&entry.id, value)?,
            WriteOperation::Upsert(value) => match sparse_columns.as_deref() {
                Some(targets) => decoder.decode_sparse_row(&entry.id, value, targets)?,
                None => decoder.decode_row(&entry.id, value)?,
            },
            WriteOperation::Delete(value) => {
                decoder.decode_sparse_row(&entry.id, value, &description.primary_keys)?
            }
        };
        let operation = match entry.operation {
            WriteOperation::Append(_) => PreparedWriteOperation::Append(row),
            WriteOperation::Upsert(_) => PreparedWriteOperation::Upsert(row),
            WriteOperation::Delete(_) => PreparedWriteOperation::Delete(row),
        };
        prepared.push(PreparedWriteEntry {
            input_index,
            id: entry.id.clone(),
            operation,
        });
    }

    Ok(PreparedWriteRequest {
        cluster_id: cluster_id.to_string(),
        table: request.table.clone(),
        expected_table_id: description.table_id,
        expected_schema_id: description.schema_id,
        partial_update_columns: request.partial_update_columns.clone(),
        delivery_deadline: delivery_deadline(request_deadline, max_delivery_time)?,
        entries: prepared,
    })
}

/// Derives the absolute, always-finite delivery deadline of every entry in one request.
///
/// It is the earlier of "now plus the configured `max_delivery_time`" and "the request deadline minus the fixed
/// response budget". Configuration validation already guarantees the first is no later than the second for a
/// request that starts with its full timeout, so the clamp only matters for a request that spent time in
/// preflight.
fn delivery_deadline(
    request_deadline: Instant,
    max_delivery_time: Duration,
) -> Result<Instant, PreflightError> {
    let now = Instant::now();
    let response_deadline = request_deadline
        .checked_sub(WRITE_RESPONSE_BUDGET)
        .filter(|deadline| *deadline > now)
        .ok_or_else(|| {
            PreflightError::from(GatewayError::deadline_exceeded(
                "request deadline leaves no write response budget",
            ))
        })?;
    Ok(now
        .checked_add(max_delivery_time)
        .unwrap_or(response_deadline)
        .min(response_deadline))
}

/// Rejects operations the table kind does not accept, and partial updates that cannot mean anything.
///
/// Partial update is a property of the *writer*, not of a row, so a batch that mixes a targeted column set with
/// deletes is rejected rather than silently applying the target list to the deletes as well.
fn validate_operations(
    description: &TableDescription,
    entries: &[WriteEntry],
    partial_columns: Option<&[String]>,
) -> Result<(), GatewayError> {
    match description.kind {
        TableKind::Log => {
            if let Some(entry) = entries
                .iter()
                .find(|entry| !matches!(entry.operation, WriteOperation::Append(_)))
            {
                return Err(GatewayError::invalid_argument(format!(
                    "log tables accept only append operations, but entry `{}` is a {}",
                    entry.id,
                    entry.operation.name()
                )));
            }
            if partial_columns.is_some() {
                return Err(GatewayError::invalid_argument(
                    "partial updates are not supported for log tables",
                ));
            }
        }
        TableKind::PrimaryKey => {
            if let Some(entry) = entries
                .iter()
                .find(|entry| matches!(entry.operation, WriteOperation::Append(_)))
            {
                return Err(GatewayError::invalid_argument(format!(
                    "primary-key tables accept only upsert and delete operations, but entry `{}` is an append",
                    entry.id
                )));
            }
            if partial_columns.is_some()
                && entries
                    .iter()
                    .any(|entry| matches!(entry.operation, WriteOperation::Delete(_)))
            {
                return Err(GatewayError::invalid_argument(
                    "partial-update requests cannot contain deletes",
                ));
            }
        }
    }
    Ok(())
}

/// Validates `partial_update_columns` against the table and returns the required-column set for decoding.
///
/// The nullability rule applies to **omitted** columns only. A targeted column may be non-nullable — the caller
/// supplies its value — which is exactly the case the native client used to reject.
fn sparse_target_columns(
    description: &TableDescription,
    partial_columns: Option<&[String]>,
) -> Result<Option<Vec<String>>, PreflightError> {
    if !description.auto_increment_columns.is_empty() && partial_columns.is_none() {
        return Err(PreflightError::schema_mismatch(
            GatewayError::invalid_argument(
                "this table has auto-increment columns, so partial_update_columns is required",
            ),
        ));
    }
    let Some(columns) = partial_columns else {
        return Ok(None);
    };
    if columns.is_empty() {
        return Err(
            GatewayError::invalid_argument("partial_update_columns must not be empty").into(),
        );
    }
    let known: HashSet<&str> = description
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut selected = HashSet::with_capacity(columns.len());
    for column in columns {
        if !known.contains(column.as_str()) {
            return Err(PreflightError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "partial-update column `{column}` is not in the table schema"
                )),
            ));
        }
        if !selected.insert(column.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate partial-update column `{column}`"
            ))
            .into());
        }
        if description.auto_increment_columns.contains(column) {
            return Err(PreflightError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "auto-increment column `{column}` cannot be targeted"
                )),
            ));
        }
    }
    for key in &description.primary_keys {
        if !selected.contains(key.as_str()) {
            return Err(PreflightError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "partial_update_columns must include primary-key column `{key}`"
                )),
            ));
        }
    }
    for column in &description.columns {
        let omitted = !selected.contains(column.name.as_str());
        let exempt = description.primary_keys.contains(&column.name)
            || description.auto_increment_columns.contains(&column.name);
        if omitted && !exempt && !column.data_type.nullable() {
            return Err(PreflightError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "omitted column `{}` must be nullable",
                    column.name
                )),
            ));
        }
    }
    Ok(Some(columns.to_vec()))
}

/// Projects the table description onto the decoder's column model, preserving declaration order.
fn input_columns(description: &TableDescription) -> Vec<InputColumn> {
    description
        .columns
        .iter()
        .map(|column| InputColumn::new(column.name.clone(), column.data_type.clone()))
        .collect()
}

/// The batch write path.
///
/// One of several inherent `impl GatewayService` blocks; see [`crate::application::service`].
impl GatewayService {
    /// Validates and decodes the complete request before submitting its first row.
    ///
    /// Unlike read operations, the native acknowledgement phase is deliberately **not** wrapped in the request
    /// deadline. Each row carries an earlier delivery deadline, and the backend returns completion-unknown
    /// entry outcomes after ownership rather than a request-level timeout — collapsing those into one 504
    /// would hide the fact that part of the batch may already be durable.
    pub async fn write(
        &self,
        context: &RequestContext,
        request: WriteRequest,
    ) -> Result<WriteResult, GatewayError> {
        context.ensure_active()?;
        let backend = self.backend(context).await?;
        let cache = self.cache(context)?;
        let table = request.table.clone();
        let table_name = table.to_string();

        let description = self
            .execute(
                context,
                crate::application::service::load_table(&cache, &backend, &table),
            )
            .await
            .map_err(|error| resource_error(error, "table", &table_name))?;

        let prepared = match preflight(
            context.cluster_id().as_str(),
            &request,
            &description,
            context.deadline(),
            self.max_write_delivery_time(),
        ) {
            Ok(prepared) => prepared,
            Err(error) if error.is_schema_mismatch() => {
                // The failure refers to the cached table shape, so one forced refresh and one repeated
                // preflight are worth it. At most one refresh happens per request.
                let refreshed = self
                    .execute(
                        context,
                        cache.refresh(&table, || async {
                            Ok((*backend.describe_table(&table).await?).clone())
                        }),
                    )
                    .await
                    .map_err(|error| resource_error(error, "table", &table_name))?;
                preflight(
                    context.cluster_id().as_str(),
                    &request,
                    &refreshed,
                    context.deadline(),
                    self.max_write_delivery_time(),
                )
                .map_err(|error| resource_error(error.into_gateway_error(), "table", &table_name))?
            }
            Err(error) => {
                return Err(resource_error(
                    error.into_gateway_error(),
                    "table",
                    &table_name,
                ));
            }
        };

        let result = backend
            .write(prepared)
            .await
            .map_err(|error| resource_error(error, "table", &table_name))?;

        // A per-entry verdict that names the schema or the target is evidence the cached description is stale,
        // so the next request re-reads it rather than repeating the same doomed batch.
        if result.entries.iter().any(|entry| {
            entry.failure.as_ref().is_some_and(|failure| {
                failure.error_code == "invalid_argument" || failure.error_code == "not_found"
            })
        }) {
            cache.invalidate_table(&table).await;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{CancellationSignal, ClusterId, DataType};
    use crate::backend::GatewayBackend;
    use crate::backend::model::{
        ClusterHealthReport, ClusterStatus, ColumnDescription, TableCapabilities, WriteCompletion,
        WriteEntryResult, WriteFailure,
    };
    use crate::backend::registry::ClusterRegistry;
    use crate::backend::testing::TestBackend;
    use crate::error::ErrorKind;
    use arrow::datatypes::{DataType as ArrowType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn object(entries: Vec<(&str, InputValue)>) -> InputValue {
        InputValue::Object(
            entries
                .into_iter()
                .map(|(name, value)| (name.to_string(), value))
                .collect(),
        )
    }

    fn number(value: &str) -> InputValue {
        InputValue::ExactNumber(value.to_string())
    }

    fn text(value: &str) -> InputValue {
        InputValue::String(value.to_string())
    }

    async fn users() -> Arc<TableDescription> {
        TestBackend::new()
            .describe_table(&TableRef::new("fluss", "users"))
            .await
            .unwrap()
    }

    async fn events() -> Arc<TableDescription> {
        TestBackend::new()
            .describe_table(&TableRef::new("fluss", "events"))
            .await
            .unwrap()
    }

    /// A primary-key table whose non-key column is NOT NULL.
    ///
    /// The fixture catalog has no such table, and it is the one shape that separates "the targeted column is
    /// non-nullable" (legal) from "the omitted column is non-nullable" (illegal).
    fn table_with_required_column() -> TableDescription {
        let columns = vec![
            ColumnDescription {
                name: "id".to_string(),
                data_type: DataType::Int { nullable: false },
                comment: None,
            },
            ColumnDescription {
                name: "label".to_string(),
                data_type: DataType::String { nullable: false },
                comment: None,
            },
            ColumnDescription {
                name: "note".to_string(),
                data_type: DataType::String { nullable: true },
                comment: None,
            },
        ];
        TableDescription {
            table: TableRef::new("fluss", "required"),
            table_id: 42,
            schema_id: 1,
            kind: TableKind::PrimaryKey,
            columns,
            primary_keys: vec!["id".to_string()],
            physical_primary_keys: vec!["id".to_string()],
            bucket_keys: vec!["id".to_string()],
            partition_keys: Vec::new(),
            auto_increment_columns: Vec::new(),
            num_buckets: 3,
            log_format: None,
            kv_format: Some("COMPACTED".to_string()),
            comment: None,
            properties: HashMap::new(),
            custom_properties: HashMap::new(),
            created_time: 0,
            modified_time: 0,
            capabilities: TableCapabilities {
                exact_lookup_supported: true,
                prefix_lookup_supported: true,
            },
            arrow_schema: Arc::new(Schema::new(vec![
                Field::new("id", ArrowType::Int32, false),
                Field::new("label", ArrowType::Utf8, false),
                Field::new("note", ArrowType::Utf8, true),
            ])),
        }
    }

    /// The same table with its nullable column declared auto-increment.
    ///
    /// Fluss generates the value, so the caller may never supply one. The fixture catalog always reports an
    /// empty auto-increment list, so this branch is reachable only from a hand-built description.
    fn table_with_auto_increment_column() -> TableDescription {
        TableDescription {
            auto_increment_columns: vec!["note".to_string()],
            ..table_with_required_column()
        }
    }

    fn run(
        request: &WriteRequest,
        description: &TableDescription,
    ) -> Result<PreparedWriteRequest, PreflightError> {
        preflight(
            "default",
            request,
            description,
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(20),
        )
    }

    fn upsert(id: &str, row: InputValue) -> WriteEntry {
        WriteEntry {
            id: id.to_string(),
            operation: WriteOperation::Upsert(row),
        }
    }

    fn delete(id: &str, row: InputValue) -> WriteEntry {
        WriteEntry {
            id: id.to_string(),
            operation: WriteOperation::Delete(row),
        }
    }

    fn append(id: &str, row: InputValue) -> WriteEntry {
        WriteEntry {
            id: id.to_string(),
            operation: WriteOperation::Append(row),
        }
    }

    #[test]
    fn operations_expose_a_stable_name_and_their_row() {
        let row = InputValue::Object(vec![("id".to_string(), InputValue::Null)]);
        for (operation, name) in [
            (WriteOperation::Append(row.clone()), "append"),
            (WriteOperation::Upsert(row.clone()), "upsert"),
            (WriteOperation::Delete(row.clone()), "delete"),
        ] {
            assert_eq!(operation.name(), name);
            assert_eq!(operation.row(), &row);
        }
    }

    #[tokio::test]
    async fn mixed_upsert_and_delete_are_prepared_in_input_order() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![
                upsert("u", object(vec![("id", number("1")), ("name", text("a"))])),
                delete("d", object(vec![("id", number("2"))])),
                upsert(
                    "u2",
                    object(vec![("id", number("3")), ("name", InputValue::Null)]),
                ),
            ],
        };

        let prepared = run(&request, &description).unwrap();
        assert_eq!(
            prepared
                .entries
                .iter()
                .map(|entry| (entry.input_index, entry.id.as_str(), entry.operation.name()))
                .collect::<Vec<_>>(),
            vec![(0, "u", "upsert"), (1, "d", "delete"), (2, "u2", "upsert")]
        );
        assert_eq!(prepared.expected_table_id, description.table_id);
        assert_eq!(prepared.expected_schema_id, description.schema_id);
        assert!(prepared.delivery_deadline > Instant::now());
    }

    #[tokio::test]
    async fn append_is_prepared_for_a_log_table() {
        let description = events().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![append(
                "e",
                object(vec![
                    ("ts", number("9007199254740993")),
                    ("message", text("hello")),
                ]),
            )],
        };

        let prepared = run(&request, &description).unwrap();
        assert_eq!(prepared.entries[0].operation.name(), "append");
        assert_eq!(prepared.entries[0].operation.row().field_count(), 2);
    }

    #[tokio::test]
    async fn duplicate_entry_ids_fail_before_anything_is_prepared() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![
                upsert(
                    "same",
                    object(vec![("id", number("1")), ("name", text("a"))]),
                ),
                delete("same", object(vec![("id", number("2"))])),
            ],
        };

        let error = run(&request, &description).unwrap_err();
        assert!(!error.is_schema_mismatch());
        assert_eq!(
            error.into_gateway_error().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[tokio::test]
    async fn an_empty_batch_is_rejected() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: Vec::new(),
        };
        assert_eq!(
            run(&request, &description)
                .unwrap_err()
                .into_gateway_error()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[tokio::test]
    async fn the_wrong_operation_for_the_table_kind_is_rejected_in_both_directions() {
        let pk = users().await;
        let bad_append = WriteRequest {
            table: pk.table.clone(),
            partial_update_columns: None,
            entries: vec![append(
                "a",
                object(vec![("id", number("1")), ("name", text("a"))]),
            )],
        };
        let error = run(&bad_append, &pk).unwrap_err().into_gateway_error();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(
            error.message().contains("only upsert and delete"),
            "{error:?}"
        );

        let log = events().await;
        let bad_upsert = WriteRequest {
            table: log.table.clone(),
            partial_update_columns: None,
            entries: vec![upsert(
                "u",
                object(vec![("ts", number("1")), ("message", text("m"))]),
            )],
        };
        let error = run(&bad_upsert, &log).unwrap_err().into_gateway_error();
        assert!(error.message().contains("only append"), "{error:?}");

        let partial_on_log = WriteRequest {
            table: log.table.clone(),
            partial_update_columns: Some(vec!["ts".to_string()]),
            entries: vec![append("a", object(vec![("ts", number("1"))]))],
        };
        assert!(
            run(&partial_on_log, &log)
                .unwrap_err()
                .into_gateway_error()
                .message()
                .contains("not supported for log tables")
        );
    }

    #[tokio::test]
    async fn partial_update_targets_a_non_nullable_column_and_rejects_omitting_one() {
        let description = table_with_required_column();

        // The targeted non-nullable column is supplied, so the write is legal (Phase-1 §6.3).
        let targeted = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: Some(vec!["id".to_string(), "label".to_string()]),
            entries: vec![upsert(
                "p",
                object(vec![("id", number("1")), ("label", text("required"))]),
            )],
        };
        let prepared = run(&targeted, &description).unwrap();
        assert_eq!(
            prepared.partial_update_columns.as_deref(),
            Some(["id".to_string(), "label".to_string()].as_slice())
        );
        // Sparse decoding still produces a full-arity row: the untouched `note` column is null.
        assert_eq!(prepared.entries[0].operation.row().field_count(), 3);

        // Omitting the same non-nullable column is what must fail.
        let omitted = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: Some(vec!["id".to_string(), "note".to_string()]),
            entries: vec![upsert(
                "p",
                object(vec![("id", number("1")), ("note", text("n"))]),
            )],
        };
        let error = run(&omitted, &description).unwrap_err();
        assert!(error.is_schema_mismatch());
        assert!(
            error
                .into_gateway_error()
                .message()
                .contains("omitted column `label` must be nullable")
        );
    }

    /// A targeted column must carry a non-null value: targeting it is a promise to supply it.
    ///
    /// Writing `null` into a nullable column is a full upsert, not a partial one.
    #[tokio::test]
    async fn a_targeted_column_can_be_neither_omitted_nor_explicitly_null() {
        let description = users().await;
        for row in [
            object(vec![("id", number("1"))]),
            object(vec![("id", number("1")), ("name", InputValue::Null)]),
        ] {
            let request = WriteRequest {
                table: description.table.clone(),
                partial_update_columns: Some(vec!["id".to_string(), "name".to_string()]),
                entries: vec![upsert("p", row)],
            };
            let error = run(&request, &description)
                .unwrap_err()
                .into_gateway_error();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(error.message().contains("`name` is required"), "{error:?}");
        }
    }

    #[tokio::test]
    async fn auto_increment_columns_require_a_column_list_and_cannot_be_targeted() {
        let description = table_with_auto_increment_column();
        let row = || object(vec![("id", number("1")), ("label", text("x"))]);

        // Without a target list every column is supplied, which would set the generated column.
        let full = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![upsert("a", row())],
        };
        let error = run(&full, &description).unwrap_err();
        assert!(error.is_schema_mismatch());
        assert!(
            error
                .into_gateway_error()
                .message()
                .contains("partial_update_columns is required")
        );

        // Naming the generated column explicitly is refused too.
        let targeted = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: Some(vec![
                "id".to_string(),
                "label".to_string(),
                "note".to_string(),
            ]),
            entries: vec![upsert("a", row())],
        };
        let error = run(&targeted, &description).unwrap_err();
        assert!(error.is_schema_mismatch());
        assert!(
            error
                .into_gateway_error()
                .message()
                .contains("auto-increment column `note` cannot be targeted")
        );

        // Targeting only the non-generated columns is the supported shape.
        let allowed = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: Some(vec!["id".to_string(), "label".to_string()]),
            entries: vec![upsert("a", row())],
        };
        assert_eq!(run(&allowed, &description).unwrap().entries.len(), 1);
    }

    #[tokio::test]
    async fn partial_update_column_lists_are_validated_before_any_row_is_decoded() {
        let description = users().await;
        let table = description.table.clone();
        let entry = || upsert("p", object(vec![("id", number("1")), ("name", text("a"))]));

        for (columns, mismatch, fragment) in [
            (vec![], false, "must not be empty"),
            (
                vec!["id".to_string(), "id".to_string()],
                false,
                "duplicate partial-update column",
            ),
            (
                vec!["id".to_string(), "nope".to_string()],
                true,
                "is not in the table schema",
            ),
            (
                vec!["name".to_string()],
                true,
                "must include primary-key column `id`",
            ),
        ] {
            let request = WriteRequest {
                table: table.clone(),
                partial_update_columns: Some(columns.clone()),
                entries: vec![entry()],
            };
            let error = run(&request, &description).unwrap_err();
            assert_eq!(error.is_schema_mismatch(), mismatch, "{columns:?}");
            let error = error.into_gateway_error();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{columns:?}");
            assert!(error.message().contains(fragment), "{columns:?}: {error:?}");
        }
    }

    #[tokio::test]
    async fn a_partial_update_batch_cannot_contain_deletes() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: Some(vec!["id".to_string(), "name".to_string()]),
            entries: vec![
                upsert("u", object(vec![("id", number("1")), ("name", text("a"))])),
                delete("d", object(vec![("id", number("2"))])),
            ],
        };
        assert!(
            run(&request, &description)
                .unwrap_err()
                .into_gateway_error()
                .message()
                .contains("cannot contain deletes")
        );
    }

    #[tokio::test]
    async fn a_delete_needs_only_the_primary_key() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![delete("d", object(vec![("id", number("2"))]))],
        };
        assert_eq!(run(&request, &description).unwrap().entries.len(), 1);

        let without_key = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![delete("d", object(vec![("name", text("a"))]))],
        };
        let error = run(&without_key, &description).unwrap_err();
        assert!(error.is_schema_mismatch());
        assert!(error.into_gateway_error().message().contains("required"));
    }

    #[tokio::test]
    async fn value_failures_are_not_schema_mismatches_but_shape_failures_are() {
        let description = users().await;
        let table = description.table.clone();

        let overflow = WriteRequest {
            table: table.clone(),
            partial_update_columns: None,
            entries: vec![upsert(
                "o",
                object(vec![("id", number("2147483648")), ("name", text("a"))]),
            )],
        };
        let error = run(&overflow, &description).unwrap_err();
        assert!(!error.is_schema_mismatch());
        assert_eq!(
            error.into_gateway_error().kind(),
            ErrorKind::InvalidArgument
        );

        let unknown_column = WriteRequest {
            table: table.clone(),
            partial_update_columns: None,
            entries: vec![upsert(
                "u",
                object(vec![("id", number("1")), ("extra", text("a"))]),
            )],
        };
        let error = run(&unknown_column, &description).unwrap_err();
        assert!(error.is_schema_mismatch());
        assert!(
            error
                .into_gateway_error()
                .message()
                .contains("unknown column")
        );

        let type_mismatch = WriteRequest {
            table,
            partial_update_columns: None,
            entries: vec![upsert(
                "t",
                object(vec![("id", text("not a number")), ("name", text("a"))]),
            )],
        };
        let error = run(&type_mismatch, &description).unwrap_err();
        assert!(!error.is_schema_mismatch());
        assert_eq!(
            error.into_gateway_error().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[tokio::test]
    async fn a_request_deadline_without_response_budget_is_refused() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![upsert("u", object(vec![("id", number("1"))]))],
        };
        let error = preflight(
            "default",
            &request,
            &description,
            Instant::now() + Duration::from_millis(1),
            Duration::from_secs(20),
        )
        .unwrap_err();
        assert_eq!(
            error.into_gateway_error().kind(),
            ErrorKind::DeadlineExceeded
        );
    }

    #[tokio::test]
    async fn the_delivery_deadline_is_clamped_to_the_request_response_budget() {
        let description = users().await;
        let request = WriteRequest {
            table: description.table.clone(),
            partial_update_columns: None,
            entries: vec![upsert(
                "u",
                object(vec![("id", number("1")), ("name", text("a"))]),
            )],
        };
        let request_deadline = Instant::now() + Duration::from_secs(2);
        let prepared = preflight(
            "default",
            &request,
            &description,
            request_deadline,
            Duration::from_secs(3600),
        )
        .unwrap();
        assert!(prepared.delivery_deadline <= request_deadline - WRITE_RESPONSE_BUDGET);
    }

    fn service_with(backend: Arc<TestBackend>) -> GatewayService {
        GatewayService::new(Arc::new(ClusterRegistry::single_for_test(
            "default",
            backend,
            ClusterHealthReport {
                status: ClusterStatus::Green,
                num_replicas: 6,
                in_sync_replicas: 6,
                num_leader_replicas: 3,
                active_leader_replicas: 3,
            },
        )))
    }

    fn context() -> RequestContext {
        RequestContext::new(
            "request-1",
            "test",
            ClusterId::try_from("default").unwrap(),
            Instant::now() + Duration::from_secs(30),
            CancellationSignal::default(),
            crate::auth::Principal::new("tester"),
        )
    }

    fn users_request(ids: &[&str]) -> WriteRequest {
        WriteRequest {
            table: TableRef::new("fluss", "users"),
            partial_update_columns: None,
            entries: ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    upsert(
                        id,
                        object(vec![("id", number(&index.to_string())), ("name", text(id))]),
                    )
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn a_preflight_failure_submits_zero_rows() {
        let backend = Arc::new(TestBackend::new());
        let service = service_with(backend.clone());
        let error = service
            .write(&context(), users_request(&["dup", "dup"]))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(backend.recorded_writes().is_empty());
    }

    #[tokio::test]
    async fn delivery_failures_are_reported_per_entry_in_input_order() {
        let backend = Arc::new(TestBackend::new());
        backend.inject_write_failure(vec![1], WriteCompletion::Unknown, "unavailable", true);
        let service = service_with(backend.clone());

        let result = service
            .write(&context(), users_request(&["first", "second", "third"]))
            .await
            .unwrap();

        assert_eq!(
            result.entries,
            vec![
                WriteEntryResult::success(0, "first".to_string()),
                WriteEntryResult::failure(
                    1,
                    "second".to_string(),
                    WriteFailure {
                        error_code: "unavailable".to_string(),
                        message: "injected test failure".to_string(),
                        completion: WriteCompletion::Unknown,
                        retryable: true,
                    }
                ),
                WriteEntryResult::success(2, "third".to_string()),
            ]
        );
        assert_eq!(
            backend
                .recorded_writes()
                .iter()
                .map(|write| write.input_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[tokio::test]
    async fn a_schema_change_between_preflight_and_submission_is_a_failed_precondition() {
        let backend = Arc::new(TestBackend::new());
        backend.evolve_schema_before_next_write();
        let service = service_with(backend.clone());

        let error = service
            .write(&context(), users_request(&["one"]))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::FailedPrecondition);
        assert!(backend.recorded_writes().is_empty());
    }

    #[tokio::test]
    async fn an_unknown_column_triggers_one_metadata_refresh_before_failing() {
        let backend = Arc::new(TestBackend::new());
        let service = service_with(backend.clone());
        let mut request = users_request(&["one"]);
        request.entries[0].operation =
            WriteOperation::Upsert(object(vec![("id", number("1")), ("ghost", text("x"))]));

        let error = service.write(&context(), request).await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert_eq!(
            error
                .details()
                .and_then(|details| details.resource_kind.clone()),
            None,
            "invalid-argument errors carry no resource details"
        );
        assert!(backend.recorded_writes().is_empty());
    }
}
