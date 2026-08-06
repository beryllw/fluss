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

//! Native execution of one preflighted write request.
//!
//! This is the only place the gateway touches `AppendWriter` and `UpsertWriter`.
//!
//! The contract it honours: a request-level `Err` is permitted only *before* the first row is accepted by the
//! client writer. Once submission begins, every entry gets an explicit success, rejected, or completion-unknown
//! verdict, and the per-entry `delivery_deadline` carried by the request bounds the whole submission.
//!
//! **Writer keying.** Partial update is configured on the *writer*, not on a row:
//! `TableUpsert::partial_update_with_column_names` is called before `create_writer()`. A writer is therefore
//! valid only for one `(table, targeted column set)` pair, which is exactly the granularity of one request —
//! a request names one table and carries one batch-level `partial_update_columns` list. The writer is built
//! inside this call rather than pooled across requests: a cached `UpsertWriter` pins an `Arc<TableInfo>` with a
//! fixed `schema_id`, so pooling would reintroduce precisely the staleness that the `table_id`/`schema_id`
//! re-check below exists to prevent.
//!
//! **Verdict honesty.** `WriteResultFuture` resolves per *accumulator batch*, not per row, so entries that
//! shared a batch can share a verdict. [`classify_rejected`] is used only where the client has provably not
//! taken ownership of the row, and [`classify_unknown`] everywhere after ownership, where the server may
//! already have committed the batch.

use crate::backend::model::{
    PreparedWriteEntry, PreparedWriteOperation, PreparedWriteRequest, WriteCompletion,
    WriteEntryResult, WriteFailure, WriteResult,
};
use crate::error::GatewayError;
use fluss::client::{AppendWriter, FlussConnection, UpsertWriter, WriteOptions, WriteResultFuture};
use fluss::error::{Error as FlussClientError, FlussError};
use fluss::metadata::TablePath;
use std::future::Future;
use std::sync::Arc;

/// The one writer a request needs, already configured for the request's operation kind and column targets.
enum NativeWriter {
    Append(AppendWriter),
    Upsert(UpsertWriter),
}

/// One row the client has taken ownership of, awaiting its acknowledgement.
struct PendingWrite {
    input_index: usize,
    id: String,
    future: WriteResultFuture,
}

/// Submits every entry of a preflighted request in input order and collects per-entry verdicts.
pub(crate) async fn execute(
    connection: &Arc<FlussConnection>,
    request: PreparedWriteRequest,
) -> Result<WriteResult, GatewayError> {
    validate_native_request(&request)?;
    let table_path = TablePath::new(request.table.database.clone(), request.table.table.clone());
    let table =
        await_before_submission(request.delivery_deadline, connection.get_table(&table_path))
            .await?
            .map_err(|error| super::native::map_fluss_error("open table for writing", error))?;
    let info = table.get_table_info();
    if info.table_id != request.expected_table_id || info.schema_id != request.expected_schema_id {
        return Err(GatewayError::failed_precondition(format!(
            "table `{}` changed during write preflight",
            request.table
        )));
    }

    let writer = match request.entries[0].operation {
        PreparedWriteOperation::Append(_) => NativeWriter::Append(
            table
                .new_append()
                .and_then(|append| append.create_writer())
                .map_err(|error| map_writer_setup_error(&request.table.to_string(), error))?,
        ),
        PreparedWriteOperation::Upsert(_) | PreparedWriteOperation::Delete(_) => {
            let upsert = table
                .new_upsert()
                .map_err(|error| map_writer_setup_error(&request.table.to_string(), error))?;
            let upsert = match request.partial_update_columns.as_ref() {
                Some(columns) => {
                    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
                    upsert
                        .partial_update_with_column_names(&names)
                        .map_err(|error| {
                            map_writer_setup_error(&request.table.to_string(), error)
                        })?
                }
                None => upsert,
            };
            NativeWriter::Upsert(
                upsert
                    .create_writer()
                    .map_err(|error| map_writer_setup_error(&request.table.to_string(), error))?,
            )
        }
    };

    let options = WriteOptions::new(request.delivery_deadline);
    let entry_count = request.entries.len();
    // Encoding and accumulator admission may park on the client's memory Condvar. Keep the whole ordered
    // submission loop in one blocking-pool job so a full accumulator cannot pin a Tokio worker or reorder
    // entries from this request. Once spawned, the job intentionally remains in the write ownership phase
    // until admission, its delivery deadline, or writer shutdown resolves it.
    let (mut ordered, pending) =
        run_blocking_enqueue(move || submit_entries(writer, request.entries, options, entry_count))
            .await?;

    for pending_write in pending {
        let outcome = match pending_write.future.await {
            Ok(()) => WriteEntryResult::success(pending_write.input_index, pending_write.id),
            Err(error) => WriteEntryResult::failure(
                pending_write.input_index,
                pending_write.id,
                classify_unknown(error),
            ),
        };
        ordered[pending_write.input_index] = Some(outcome);
    }

    Ok(WriteResult {
        entries: ordered
            .into_iter()
            .map(|entry| entry.expect("every preflighted entry receives a verdict"))
            .collect(),
    })
}

/// Bounds work that happens *before* any row is owned by the client, where a timeout is still a clean failure.
async fn await_before_submission<T, F>(
    deadline: std::time::Instant,
    work: F,
) -> Result<T, GatewayError>
where
    F: Future<Output = T>,
{
    tokio::time::timeout_at(deadline.into(), work)
        .await
        .map_err(|_| {
            GatewayError::deadline_exceeded("write delivery deadline exceeded before submission")
        })
}

/// Hands every row to the writer in input order, recording rejections that never reached the accumulator.
fn submit_entries(
    writer: NativeWriter,
    entries: Vec<PreparedWriteEntry>,
    options: WriteOptions,
    entry_count: usize,
) -> (Vec<Option<WriteEntryResult>>, Vec<PendingWrite>) {
    let mut ordered: Vec<Option<WriteEntryResult>> = vec![None; entry_count];
    let mut pending = Vec::with_capacity(entry_count);
    for entry in entries {
        let submitted = match (&writer, &entry.operation) {
            (NativeWriter::Append(writer), PreparedWriteOperation::Append(_)) => {
                writer.append_with_options(entry.operation.row().as_native(), options)
            }
            (NativeWriter::Upsert(writer), PreparedWriteOperation::Upsert(_)) => {
                writer.upsert_with_options(entry.operation.row().as_native(), options)
            }
            (NativeWriter::Upsert(writer), PreparedWriteOperation::Delete(_)) => {
                writer.delete_with_options(entry.operation.row().as_native(), options)
            }
            _ => unreachable!("operation compatibility was checked before writer construction"),
        };
        match submitted {
            Ok(future) => pending.push(PendingWrite {
                input_index: entry.input_index,
                id: entry.id,
                future,
            }),
            Err(error) => {
                // The client refused the row outright, so it is provably not written.
                ordered[entry.input_index] = Some(WriteEntryResult::failure(
                    entry.input_index,
                    entry.id,
                    classify_rejected(error),
                ));
            }
        }
    }
    (ordered, pending)
}

fn map_enqueue_join_error(error: tokio::task::JoinError) -> GatewayError {
    if error.is_cancelled() {
        GatewayError::unavailable("write enqueue task stopped during gateway shutdown")
    } else {
        log::error!("write enqueue task failed unexpectedly: {error}");
        GatewayError::internal("write enqueue task failed unexpectedly")
    }
}

/// Runs the ordered submission loop on the blocking pool, where parking on a full accumulator is safe.
async fn run_blocking_enqueue<T, F>(enqueue: F) -> Result<T, GatewayError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(enqueue)
        .await
        .map_err(map_enqueue_join_error)
}

/// Re-checks the invariants the application preflight is required to have established.
///
/// These are defensive: the application layer already rejects the wrong operation for a table kind, so a mixed
/// batch cannot reach here through the REST path. Checking anyway keeps the `unreachable!` in
/// [`submit_entries`] honest for any future caller of the backend trait.
fn validate_native_request(request: &PreparedWriteRequest) -> Result<(), GatewayError> {
    if request.entries.is_empty() {
        return Err(GatewayError::invalid_argument(
            "write request must contain at least one entry",
        ));
    }
    for (position, entry) in request.entries.iter().enumerate() {
        if entry.input_index != position {
            return Err(GatewayError::invalid_argument(
                "prepared writes must preserve contiguous input order",
            ));
        }
    }
    let append = matches!(
        request.entries[0].operation,
        PreparedWriteOperation::Append(_)
    );
    if request
        .entries
        .iter()
        .any(|entry| matches!(entry.operation, PreparedWriteOperation::Append(_)) != append)
    {
        return Err(GatewayError::invalid_argument(
            "append operations cannot be mixed with primary-key mutations",
        ));
    }
    Ok(())
}

/// Maps a writer-construction failure. Setup happens before ownership, so it is always a request-level error.
fn map_writer_setup_error(table: &str, error: FlussClientError) -> GatewayError {
    match error {
        FlussClientError::IllegalArgument { .. }
        | FlussClientError::RowConvertError { .. }
        | FlussClientError::ArrowError { .. }
        | FlussClientError::UnsupportedOperation { .. } => GatewayError::invalid_argument(format!(
            "write values are incompatible with table `{table}`"
        )),
        other => super::native::map_fluss_error("prepare table writer", other),
    }
}

/// Classifies a failure raised before the client took ownership of the row: it is provably not written.
fn classify_rejected(error: FlussClientError) -> WriteFailure {
    let (code, message, retryable) = classify_error(&error);
    WriteFailure {
        error_code: code.to_string(),
        message,
        completion: WriteCompletion::Rejected,
        retryable,
    }
}

/// Classifies a failure raised after the client took ownership: the batch may already be committed.
///
/// The specific message is deliberately dropped. After ownership the only honest statement is that the outcome
/// is unknown, and a message such as "timed out" invites a caller to assume the row was not written.
///
/// Storage backpressure is the one exception with a definitive outcome: the server rejected every delivery
/// attempt before applying it, so the entry is provably not written and safe to retry individually (FIP-49
/// `storage_backpressure`).
fn classify_unknown(error: FlussClientError) -> WriteFailure {
    if error.api_error() == Some(FlussError::StorageBackpressureException) {
        return classify_rejected(error);
    }
    let (code, _, retryable) = classify_error(&error);
    WriteFailure {
        error_code: code.to_string(),
        message: "write completion is unknown".to_string(),
        completion: WriteCompletion::Unknown,
        retryable,
    }
}

/// Maps a client error onto a stable gateway error code, a client-safe message, and retryability.
fn classify_error(error: &FlussClientError) -> (&'static str, String, bool) {
    if let Some(api_error) = error.api_error() {
        let (code, message) = match api_error {
            FlussError::SchemaNotExist
            | FlussError::InvalidTargetColumn
            | FlussError::InvalidTableException
            | FlussError::InvalidDatabaseException
            | FlussError::NonPrimaryKeyTableException => (
                "invalid_argument",
                "write is incompatible with the table schema",
            ),
            FlussError::TableNotExist
            | FlussError::DatabaseNotExist
            | FlussError::PartitionNotExists
            | FlussError::UnknownTableOrBucketException => {
                ("not_found", "the write target does not exist")
            }
            FlussError::RecordTooLargeException => {
                ("limit_exceeded", "the encoded record is too large")
            }
            FlussError::RequestTimeOut => ("timeout", "write delivery timed out"),
            FlussError::StorageBackpressureException => (
                "storage_backpressure",
                "the KV store rejected the write under backpressure; retry the failed entries once pressure drains",
            ),
            _ if api_error.is_retriable() => {
                ("unavailable", "the Fluss write service is unavailable")
            }
            _ => ("internal", "the write failed unexpectedly"),
        };
        return (code, message.to_string(), api_error.is_retriable());
    }

    match error {
        FlussClientError::BufferExhausted { .. } => (
            "resource_exhausted",
            "write buffer capacity is exhausted".to_string(),
            true,
        ),
        FlussClientError::IllegalArgument { .. }
        | FlussClientError::RowConvertError { .. }
        | FlussClientError::ArrowError { .. } => (
            "invalid_argument",
            "write values are incompatible with the table schema".to_string(),
            false,
        ),
        FlussClientError::WriterClosed { .. }
        | FlussClientError::RpcError { .. }
        | FlussClientError::WakeupError { .. } => (
            "unavailable",
            "the Fluss write service is unavailable".to_string(),
            true,
        ),
        _ => (
            "internal",
            "the write failed unexpectedly".to_string(),
            error.is_retriable(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::model::TableRef;
    use crate::backend::types::DataType;
    use crate::protocol::rest::input_decode::{InputColumn, SchemaDecoder};
    use crate::protocol::rest::input_value::InputValue;
    use parking_lot::{Condvar, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    fn delivery_timeout_error() -> FlussClientError {
        FlussClientError::FlussAPIError {
            api_error: FlussError::RequestTimeOut
                .to_api_error(Some("Write delivery deadline exceeded".to_string())),
        }
    }

    fn row(value: i32) -> crate::protocol::rest::input_decode::DecodedRow {
        let decoder = SchemaDecoder::new(vec![InputColumn::new(
            "id",
            DataType::Int { nullable: false },
        )])
        .unwrap();
        decoder
            .decode_row(
                "entry",
                &InputValue::Object(vec![(
                    "id".to_string(),
                    InputValue::ExactNumber(value.to_string()),
                )]),
            )
            .unwrap()
    }

    fn request(operations: Vec<PreparedWriteOperation>) -> PreparedWriteRequest {
        PreparedWriteRequest {
            cluster_id: "default".to_string(),
            table: TableRef::new("fluss", "users"),
            expected_table_id: 1,
            expected_schema_id: 1,
            partial_update_columns: None,
            delivery_deadline: Instant::now() + Duration::from_secs(5),
            entries: operations
                .into_iter()
                .enumerate()
                .map(|(input_index, operation)| PreparedWriteEntry {
                    input_index,
                    id: format!("e{input_index}"),
                    operation,
                })
                .collect(),
        }
    }

    #[test]
    fn native_validation_rejects_empty_reordered_and_mixed_requests() {
        assert_eq!(
            validate_native_request(&request(Vec::new()))
                .unwrap_err()
                .kind(),
            crate::error::ErrorKind::InvalidArgument
        );

        let mut reordered = request(vec![
            PreparedWriteOperation::Upsert(row(1)),
            PreparedWriteOperation::Upsert(row(2)),
        ]);
        reordered.entries[0].input_index = 1;
        reordered.entries[1].input_index = 0;
        assert!(
            validate_native_request(&reordered)
                .unwrap_err()
                .message()
                .contains("contiguous input order")
        );

        let mixed = request(vec![
            PreparedWriteOperation::Append(row(1)),
            PreparedWriteOperation::Upsert(row(2)),
        ]);
        assert!(
            validate_native_request(&mixed)
                .unwrap_err()
                .message()
                .contains("cannot be mixed")
        );

        // Upsert and delete share one writer, so mixing them is the supported case.
        validate_native_request(&request(vec![
            PreparedWriteOperation::Upsert(row(1)),
            PreparedWriteOperation::Delete(row(2)),
        ]))
        .unwrap();
    }

    #[tokio::test]
    async fn pre_submission_work_honors_delivery_deadline() {
        let deadline = Instant::now() + Duration::from_millis(10);
        let error = await_before_submission(deadline, std::future::pending::<()>())
            .await
            .unwrap_err();

        assert_eq!(error.kind(), crate::error::ErrorKind::DeadlineExceeded);
        assert_eq!(
            error.message(),
            "write delivery deadline exceeded before submission"
        );
    }

    #[test]
    fn delivery_timeout_is_unknown_only_after_accumulator_ownership() {
        let rejected = classify_rejected(delivery_timeout_error());
        let unknown = classify_unknown(delivery_timeout_error());

        assert_eq!(rejected.completion, WriteCompletion::Rejected);
        assert_eq!(rejected.error_code, "timeout");
        assert_eq!(rejected.message, "write delivery timed out");
        assert_eq!(unknown.completion, WriteCompletion::Unknown);
        assert_eq!(unknown.error_code, "timeout");
        assert_eq!(unknown.message, "write completion is unknown");
    }

    fn backpressure_error() -> FlussClientError {
        FlussClientError::FlussAPIError {
            api_error: FlussError::StorageBackpressureException
                .to_api_error(Some("KV storage under write pressure".to_string())),
        }
    }

    /// A storage-backpressure failure carries the FIP-49 entry-level code and is the one
    /// post-ownership error with a definitive outcome: every attempt was rejected by the server
    /// before being applied, so the entry is provably not written and safe to retry.
    #[test]
    fn storage_backpressure_is_a_retriable_rejected_entry_code() {
        let rejected = classify_rejected(backpressure_error());
        assert_eq!(rejected.error_code, "storage_backpressure");
        assert_eq!(rejected.completion, WriteCompletion::Rejected);
        assert!(rejected.retryable);

        let post_ownership = classify_unknown(backpressure_error());
        assert_eq!(post_ownership.error_code, "storage_backpressure");
        assert_eq!(post_ownership.completion, WriteCompletion::Rejected);
        assert!(post_ownership.retryable);
        assert_eq!(post_ownership.message, rejected.message);
    }

    #[test]
    fn post_enqueue_semantic_error_stays_completion_unknown() {
        let failure = classify_unknown(FlussClientError::invalid_table("stale schema"));

        assert_eq!(failure.error_code, "invalid_argument");
        assert_eq!(failure.completion, WriteCompletion::Unknown);
        assert!(!failure.retryable);
    }

    #[test]
    fn client_side_failures_map_to_stable_codes() {
        for (error, code, retryable) in [
            (
                FlussClientError::BufferExhausted {
                    message: "full".to_string(),
                },
                "resource_exhausted",
                true,
            ),
            (
                FlussClientError::IllegalArgument {
                    message: "bad".to_string(),
                },
                "invalid_argument",
                false,
            ),
            (
                FlussClientError::WriterClosed {
                    message: "closed".to_string(),
                },
                "unavailable",
                true,
            ),
            (
                FlussClientError::UnexpectedError {
                    message: "boom".to_string(),
                    source: None,
                },
                "internal",
                false,
            ),
        ] {
            let failure = classify_rejected(error);
            assert_eq!(failure.error_code, code);
            assert_eq!(failure.retryable, retryable, "{code}");
        }
    }

    #[test]
    fn writer_setup_failures_are_request_level_invalid_arguments() {
        let error = map_writer_setup_error(
            "fluss.users",
            FlussClientError::IllegalArgument {
                message: "omitted column is NOT NULL".to_string(),
            },
        );
        assert_eq!(error.kind(), crate::error::ErrorKind::InvalidArgument);
        assert!(error.message().contains("fluss.users"));
    }

    /// A one-permit stand-in for the client's memory accumulator, which parks on a `Condvar`.
    struct TinyBuffer {
        used: Mutex<bool>,
        available: Condvar,
        closed: AtomicBool,
    }

    impl TinyBuffer {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                used: Mutex::new(false),
                available: Condvar::new(),
                closed: AtomicBool::new(false),
            })
        }

        fn acquire_until(
            self: &Arc<Self>,
            deadline: Instant,
        ) -> Result<TinyPermit, FlussClientError> {
            let mut used = self.used.lock();
            while *used && !self.closed.load(Ordering::Acquire) {
                if self.available.wait_until(&mut used, deadline).timed_out() && *used {
                    return Err(delivery_timeout_error());
                }
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(FlussClientError::WriterClosed {
                    message: "tiny test buffer closed".to_string(),
                });
            }
            *used = true;
            Ok(TinyPermit {
                buffer: Arc::clone(self),
            })
        }

        fn close(&self) {
            self.closed.store(true, Ordering::Release);
            self.available.notify_all();
        }
    }

    struct TinyPermit {
        buffer: Arc<TinyBuffer>,
    }

    impl Drop for TinyPermit {
        fn drop(&mut self) {
            *self.buffer.used.lock() = false;
            self.buffer.available.notify_one();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_enqueue_buffer_does_not_block_tokio_progress() {
        let buffer = TinyBuffer::new();
        let held = run_blocking_enqueue({
            let buffer = Arc::clone(&buffer);
            move || buffer.acquire_until(Instant::now() + Duration::from_secs(1))
        })
        .await
        .unwrap()
        .unwrap();

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let waiting = tokio::spawn(run_blocking_enqueue({
            let buffer = Arc::clone(&buffer);
            move || {
                let _ = started_tx.send(());
                buffer.acquire_until(Instant::now() + Duration::from_secs(1))
            }
        }));
        started_rx.await.unwrap();

        tokio::time::timeout(Duration::from_millis(100), async {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        })
        .await
        .expect("a full writer buffer must not park the Tokio worker");

        drop(held);
        let released = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("released buffer must wake the queued enqueue")
            .unwrap()
            .unwrap()
            .unwrap();
        drop(released);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_enqueue_honors_delivery_deadline_and_shutdown() {
        let buffer = TinyBuffer::new();
        let held = run_blocking_enqueue({
            let buffer = Arc::clone(&buffer);
            move || buffer.acquire_until(Instant::now() + Duration::from_secs(1))
        })
        .await
        .unwrap()
        .unwrap();

        let timed_out = run_blocking_enqueue({
            let buffer = Arc::clone(&buffer);
            move || buffer.acquire_until(Instant::now() + Duration::from_millis(25))
        })
        .await
        .unwrap();
        let timed_out = match timed_out {
            Err(error) => error,
            Ok(_) => panic!("full buffer unexpectedly accepted an enqueue past its deadline"),
        };
        let failure = classify_rejected(timed_out);
        assert_eq!(failure.error_code, "timeout");
        assert_eq!(failure.completion, WriteCompletion::Rejected);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let waiting = tokio::spawn(run_blocking_enqueue({
            let buffer = Arc::clone(&buffer);
            move || {
                let _ = started_tx.send(());
                buffer.acquire_until(Instant::now() + Duration::from_secs(10))
            }
        }));
        started_rx.await.unwrap();
        buffer.close();
        let closed = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("closing the writer must wake blocked enqueue work")
            .unwrap()
            .unwrap();
        let closed = match closed {
            Err(error) => error,
            Ok(_) => panic!("closed buffer unexpectedly accepted an enqueue"),
        };
        let failure = classify_rejected(closed);
        assert_eq!(failure.error_code, "unavailable");
        assert_eq!(failure.completion, WriteCompletion::Rejected);
        drop(held);
    }
}
