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

//! Native execution of the two lookup operations.
//!
//! Both entry points are the only place the gateway touches `Lookuper` and `PrefixKeyLookuper`. The signatures
//! are fixed: [`crate::backend::native::NativeGatewayBackend`] delegates to them unchanged.
//!
//! Three native facts shape the implementation.
//!
//! `Lookuper::lookup` and `PrefixKeyLookuper::lookup` both take `&mut self`, and `create_lookuper()` resolves the
//! schema, the key encoders, and the bucketing function up front. A per-table lookuper pool therefore pays for
//! itself, and because a lookuper is exclusive for the duration of a call the pool is a *checkout* pool: an entry
//! is removed for the call and put back afterwards. It is a performance cache and never correctness state — see
//! [`pool`].
//!
//! `PrefixKeyLookuper::lookup` takes exactly one prefix per call and has no row bound, which is why
//! `max_rows_per_prefix` is applied here as truncation with a `truncated` flag rather than pushed to the server.
//!
//! `TablePrefixLookup::create_lookuper` runs the client's own `validate_prefix_lookup`, which enforces six rules
//! that table metadata alone cannot decide (they depend on the requested prefix columns). Construction therefore
//! happens inside this module's error-mapping boundary, and the client's `IllegalArgument` message is surfaced
//! verbatim as an invalid argument — see [`map_lookuper_error`].

use crate::backend::model::{
    KeyValue, LookupKey, LookupOutcome, LookupOutcomeKind, PrefixLookupOutcome,
    PrefixLookupRequest, PrefixOutcomeKind, TableRef,
};
use crate::backend::native::map_fluss_error;
use crate::error::GatewayError;
use arrow::array::RecordBatch;
use fluss::client::{FlussConnection, LookupResult};
use fluss::error::Error as FlussClientError;
use fluss::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
use futures::StreamExt;
use std::borrow::Cow;
use std::sync::Arc;

/// Looks up rows by primary key, returning one outcome per key in input order.
///
/// A key that matches nothing is a [`LookupOutcomeKind::NotFound`] outcome, and a key whose own lookup fails is an
/// [`LookupOutcomeKind::Error`] outcome; neither aborts the batch. Only a failure that prevents the batch from
/// running at all — resolving the table, building the lookuper, or encoding a key — returns `Err`.
pub(crate) async fn lookup(
    connection: &Arc<FlussConnection>,
    table: &TableRef,
    keys: Vec<LookupKey>,
    max_concurrent: usize,
) -> Result<Vec<LookupOutcome>, GatewayError> {
    let (key, native) = pool::prepare_point(connection, table).await?;
    let rows = native_rows(&keys)?;

    // The per-key futures are built eagerly rather than from a closure: a closure returning a future that borrows
    // its argument makes the whole batch future fail the `Send` proof that `#[async_trait]` needs.
    let mut calls = Vec::with_capacity(rows.len());
    for row in rows {
        let key = key.clone();
        let native = &native;
        calls.push(async move {
            // Concurrent keys of one batch each need their own lookuper, so an empty pool builds another rather
            // than failing. Pooling is what makes the *second* request cheap, never what makes this one possible.
            let mut lookuper = match pool::take_point(&key) {
                Some(lookuper) => lookuper,
                None => pool::build_point(native)?,
            };
            let result = lookuper.lookup(&row).await;
            pool::put_point(&key, lookuper);
            result
        });
    }
    let results: Vec<Result<LookupResult, FlussClientError>> = futures::stream::iter(calls)
        .buffered(max_concurrent.max(1))
        .collect()
        .await;

    Ok(results
        .into_iter()
        .enumerate()
        .map(|(input_index, result)| LookupOutcome {
            input_index,
            kind: point_outcome(result),
        })
        .collect())
}

/// Looks up rows by key prefix, returning one outcome per prefix in input order.
///
/// Lookuper construction is inside the mapped boundary on purpose: the client validates the prefix columns there,
/// and its refusal must reach the caller as an invalid argument carrying the client's own explanation.
pub(crate) async fn prefix_lookup(
    connection: &Arc<FlussConnection>,
    table: &TableRef,
    request: PrefixLookupRequest,
    max_concurrent: usize,
) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
    let (key, native) = pool::prepare_prefix(connection, table, &request.prefix_columns).await?;
    let rows = native_rows(&request.prefixes)?;
    let max_rows = request.max_rows_per_prefix;
    let columns = request.prefix_columns;

    let mut calls = Vec::with_capacity(rows.len());
    for row in rows {
        let key = key.clone();
        let native = &native;
        let columns = &columns;
        calls.push(async move {
            let mut lookuper = match pool::take_prefix(&key) {
                Some(lookuper) => lookuper,
                None => pool::build_prefix(native, columns)?,
            };
            let result = lookuper.lookup(&row).await;
            pool::put_prefix(&key, lookuper);
            result
        });
    }
    let results: Vec<Result<LookupResult, FlussClientError>> = futures::stream::iter(calls)
        .buffered(max_concurrent.max(1))
        .collect()
        .await;

    Ok(results
        .into_iter()
        .enumerate()
        .map(|(input_index, result)| PrefixLookupOutcome {
            input_index,
            kind: prefix_outcome(result, max_rows),
        })
        .collect())
}

/// Shapes one point-lookup result: at most one row, and a miss is a regular outcome.
fn point_outcome(result: Result<LookupResult, FlussClientError>) -> LookupOutcomeKind {
    match result {
        Ok(result) => match result.to_record_batch() {
            Ok(batch) if batch.num_rows() == 0 => LookupOutcomeKind::NotFound,
            Ok(batch) => LookupOutcomeKind::Found(batch),
            Err(error) => {
                LookupOutcomeKind::Error(map_fluss_error("decode the looked up row", error))
            }
        },
        Err(error) => LookupOutcomeKind::Error(map_fluss_error("look up a key", error)),
    }
}

/// Shapes one prefix result, applying the gateway-side row cap.
fn prefix_outcome(
    result: Result<LookupResult, FlussClientError>,
    max_rows: usize,
) -> PrefixOutcomeKind {
    match result {
        Ok(result) => match result.to_record_batch() {
            Ok(batch) => truncate(batch, max_rows),
            Err(error) => PrefixOutcomeKind::Error(map_fluss_error("decode a prefix row", error)),
        },
        Err(error) => PrefixOutcomeKind::Error(map_fluss_error("look up a key prefix", error)),
    }
}

/// Cuts a prefix result at the per-prefix row cap, flagging the outcome only when rows were actually dropped.
///
/// The native prefix lookuper returns every matching row, so the bound can only be applied here. A zero-row batch
/// is a normal answer and is never turned into a not-found variant.
fn truncate(batch: RecordBatch, max_rows: usize) -> PrefixOutcomeKind {
    if batch.num_rows() <= max_rows {
        return PrefixOutcomeKind::Rows {
            batch,
            truncated: false,
        };
    }
    PrefixOutcomeKind::Rows {
        batch: batch.slice(0, max_rows),
        truncated: true,
    }
}

/// Builds every native lookup row up front so an unencodable key fails the batch before any RPC is issued.
fn native_rows(keys: &[LookupKey]) -> Result<Vec<GenericRow<'static>>, GatewayError> {
    keys.iter().map(|key| to_native_row(&key.values)).collect()
}

/// Builds the native lookup row from already validated key values.
///
/// The values are in the order the caller declared — the logical primary key for a point lookup, the request's
/// `prefix_columns` for a prefix lookup — which is exactly the projected row type the client's key encoder
/// expects.
fn to_native_row(values: &[KeyValue]) -> Result<GenericRow<'static>, GatewayError> {
    let mut row = GenericRow::new(values.len());
    for (index, value) in values.iter().enumerate() {
        row.set_field(index, to_datum(value)?);
    }
    Ok(row)
}

/// Maps one protocol-neutral key value onto the native datum of the same logical type.
fn to_datum(value: &KeyValue) -> Result<Datum<'static>, GatewayError> {
    let datum = match value {
        KeyValue::Boolean(value) => Datum::Bool(*value),
        KeyValue::TinyInt(value) => Datum::Int8(*value),
        KeyValue::SmallInt(value) => Datum::Int16(*value),
        KeyValue::Int(value) => Datum::Int32(*value),
        KeyValue::BigInt(value) => Datum::Int64(*value),
        KeyValue::Float(value) => Datum::from(*value),
        KeyValue::Double(value) => Datum::from(*value),
        KeyValue::String(value) => Datum::String(Cow::Owned(value.clone())),
        KeyValue::Bytes(value) => Datum::Blob(Cow::Owned(value.clone())),
        KeyValue::Decimal {
            unscaled,
            precision,
            scale,
        } => {
            let native_scale = u32::try_from(*scale).map_err(|_| {
                GatewayError::invalid_argument(format!(
                    "a DECIMAL key value has scale {scale}, which the native type cannot represent"
                ))
            })?;
            let decimal = Decimal::from_unscaled_bytes(
                &unscaled.to_be_bytes(),
                u32::from(*precision),
                native_scale,
            )
            .map_err(|error| {
                GatewayError::invalid_argument(format!(
                    "a DECIMAL key value is out of range: {error}"
                ))
            })?;
            Datum::Decimal(decimal)
        }
        KeyValue::Date { days_since_epoch } => Datum::Date(Date::new(*days_since_epoch)),
        KeyValue::Time { millis_of_day } => Datum::Time(Time::new(*millis_of_day)),
        KeyValue::TimestampNtz {
            millis,
            nanos_of_milli,
        } => Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(*millis, sub_milli_nanos(*nanos_of_milli)?)
                .map_err(timestamp_error)?,
        ),
        KeyValue::TimestampLtz {
            epoch_millis,
            nanos_of_milli,
        } => Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(*epoch_millis, sub_milli_nanos(*nanos_of_milli)?)
                .map_err(timestamp_error)?,
        ),
    };
    Ok(datum)
}

/// Narrows the sub-millisecond remainder to the signed width the native timestamp constructors take.
fn sub_milli_nanos(nanos: u32) -> Result<i32, GatewayError> {
    i32::try_from(nanos).map_err(|_| {
        GatewayError::invalid_argument(format!(
            "a timestamp key value carries {nanos} nanoseconds within its millisecond, \
             which is out of range"
        ))
    })
}

fn timestamp_error(error: FlussClientError) -> GatewayError {
    GatewayError::invalid_argument(format!("a timestamp key value is out of range: {error}"))
}

/// Maps a failure from `create_lookuper()` onto the gateway taxonomy.
///
/// `Error::IllegalArgument` is the client's verdict on the requested lookup columns — one of the six rules in
/// `validate_prefix_lookup`, or a projection naming a column the table does not have. Its message is the only
/// thing that explains *which* rule was broken, so it is surfaced verbatim as an invalid argument. The shared
/// [`map_fluss_error`] deliberately replaces messages with a context sentence, which is right for infrastructure
/// failures and wrong for this one.
fn map_lookuper_error(error: FlussClientError) -> GatewayError {
    match error {
        FlussClientError::IllegalArgument { message } => GatewayError::invalid_argument(message),
        other => map_fluss_error("prepare the lookup", other),
    }
}

/// Per-table lookuper pool.
///
/// **This is a performance cache and nothing else.** Every entry may be dropped at any moment — between two
/// requests, or between two keys of one request — and the only consequence is that the next call rebuilds a
/// lookuper. No response is derived from what the pool happens to hold, which is what keeps the gateway stateless
/// in the sense of PLAN §3.
///
/// Three properties matter for correctness:
///
/// * **Connection identity.** An entry records a [`Weak`] to the connection its lookupers were built from and is
///   only reused for that same live `Arc`. A live `Weak` keeps the allocation alive, so a freed connection's
///   address cannot be recycled underneath us and an entry can never be handed to a different cluster.
/// * **Schema.** The key carries the table's schema ID read at preparation time. A lookuper captures the schema it
///   was created with, so a schema change yields a different key and the entries under the old one are evicted.
/// * **Exclusivity.** `lookup` takes `&mut self`, so an entry is *removed* while in use and put back afterwards.
///   No lock is ever held across an await.
mod pool {
    use super::map_lookuper_error;
    use crate::backend::model::TableRef;
    use crate::error::GatewayError;
    use fluss::client::{FlussConnection, FlussTable, Lookuper, PrefixKeyLookuper};
    use fluss::metadata::TablePath;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Weak};

    /// Maximum idle lookupers kept per key. Beyond it a returned lookuper is dropped instead of pooled.
    const MAX_IDLE_PER_KEY: usize = 32;

    /// Maximum distinct keys kept in one pool. Reaching it clears the pool, which costs rebuilds and nothing else.
    const MAX_KEYS: usize = 1024;

    /// Identifies lookupers that are interchangeable with one another.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub(super) struct PoolKey {
        /// Address of the connection allocation, only ever trusted together with the entry's [`Weak`].
        connection: usize,
        table: TableRef,
        schema_id: i32,
        /// Empty for a point lookup, the requested prefix columns otherwise.
        columns: Vec<String>,
    }

    /// Idle lookupers of one key plus the connection they belong to.
    struct Entry<L> {
        connection: Weak<FlussConnection>,
        idle: Vec<L>,
    }

    impl<L> Entry<L> {
        fn new(connection: &Arc<FlussConnection>) -> Self {
            Self {
                connection: Arc::downgrade(connection),
                idle: Vec::new(),
            }
        }
    }

    type Pool<L> = Mutex<HashMap<PoolKey, Entry<L>>>;

    static POINT: LazyLock<Pool<Lookuper>> = LazyLock::new(|| Mutex::new(HashMap::new()));
    static PREFIX: LazyLock<Pool<PrefixKeyLookuper>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Resolves the table for one request and guarantees at least one idle lookuper under its key.
    ///
    /// Nothing is built when the pool already holds one: for point lookup, building is pure cost, and paying it
    /// per request would defeat the pool. The resolved table is handed back because a batch whose keys run
    /// concurrently may still need to build more.
    pub(super) async fn prepare_point<'a>(
        connection: &'a Arc<FlussConnection>,
        table: &TableRef,
    ) -> Result<(PoolKey, FlussTable<'a>), GatewayError> {
        let (key, native) = resolve(connection, table, Vec::new()).await?;
        evict_foreign(&POINT, &key, connection);
        if !has_idle(&POINT, &key) {
            let lookuper = build_point(&native).map_err(map_lookuper_error)?;
            admit(&POINT, &key, connection, lookuper);
        }
        Ok((key, native))
    }

    /// Same for prefix lookup, except that one lookuper is always built.
    ///
    /// Here the build is load-bearing rather than wasteful: it is where the client validates the requested
    /// columns, so doing it up front turns a refusal into one request-level 400 instead of the same error
    /// repeated on every prefix. A pooled entry proves nothing about *this* request's columns — although in
    /// practice it does, since the columns are part of the pool key.
    pub(super) async fn prepare_prefix<'a>(
        connection: &'a Arc<FlussConnection>,
        table: &TableRef,
        columns: &[String],
    ) -> Result<(PoolKey, FlussTable<'a>), GatewayError> {
        let (key, native) = resolve(connection, table, columns.to_vec()).await?;
        evict_foreign(&PREFIX, &key, connection);
        let lookuper = build_prefix(&native, columns).map_err(map_lookuper_error)?;
        admit(&PREFIX, &key, connection, lookuper);
        Ok((key, native))
    }

    /// Builds one point lookuper for an already resolved table.
    pub(super) fn build_point(native: &FlussTable<'_>) -> Result<Lookuper, fluss::error::Error> {
        native.new_lookup()?.create_lookuper()
    }

    /// Builds one prefix lookuper, which is where the client validates the requested columns.
    pub(super) fn build_prefix(
        native: &FlussTable<'_>,
        columns: &[String],
    ) -> Result<PrefixKeyLookuper, fluss::error::Error> {
        native
            .new_lookup()?
            .lookup_by(columns.to_vec())
            .create_lookuper()
    }

    /// Reads the table once per request and builds the key that its lookupers are pooled under.
    async fn resolve<'a>(
        connection: &'a Arc<FlussConnection>,
        table: &TableRef,
        columns: Vec<String>,
    ) -> Result<(PoolKey, FlussTable<'a>), GatewayError> {
        let path = TablePath::new(table.database.clone(), table.table.clone());
        let native = connection
            .get_table(&path)
            .await
            .map_err(|error| super::map_fluss_error("resolve the table", error))?;
        let key = PoolKey {
            connection: Arc::as_ptr(connection) as usize,
            table: table.clone(),
            schema_id: native.get_table_info().get_schema_id(),
            columns,
        };
        Ok((key, native))
    }

    /// Discards everything that does not belong to this connection, and bounds the pool's size.
    ///
    /// Entries whose connection is gone go first, then any entry under this key that belongs to a *different*
    /// live connection. Address reuse cannot fool the comparison: the retained `Weak` keeps the old allocation
    /// alive, so a recycled address cannot masquerade as the connection an entry was built from.
    fn evict_foreign<L>(pool: &Pool<L>, key: &PoolKey, connection: &Arc<FlussConnection>) {
        let mut pool = pool.lock();
        pool.retain(|_, entry| entry.connection.strong_count() > 0);
        let stale = pool.get(key).is_some_and(|entry| {
            !entry
                .connection
                .upgrade()
                .is_some_and(|pooled| Arc::ptr_eq(&pooled, connection))
        });
        if stale {
            pool.remove(key);
        }
        if pool.len() >= MAX_KEYS {
            pool.clear();
        }
    }

    /// True when `key` already has a lookuper ready to be taken.
    fn has_idle<L>(pool: &Pool<L>, key: &PoolKey) -> bool {
        pool.lock()
            .get(key)
            .is_some_and(|entry| !entry.idle.is_empty())
    }

    /// Publishes one freshly built lookuper under `key`.
    fn admit<L>(pool: &Pool<L>, key: &PoolKey, connection: &Arc<FlussConnection>, lookuper: L) {
        let mut pool = pool.lock();
        let entry = pool
            .entry(key.clone())
            .or_insert_with(|| Entry::new(connection));
        if entry.idle.len() < MAX_IDLE_PER_KEY {
            entry.idle.push(lookuper);
        }
    }

    /// Takes one idle lookuper for exclusive use, or `None` when none is idle.
    fn take<L>(pool: &Pool<L>, key: &PoolKey) -> Option<L> {
        pool.lock().get_mut(key)?.idle.pop()
    }

    /// Returns a lookuper for reuse, dropping it when the key is already at its idle bound or has been evicted.
    fn put<L>(pool: &Pool<L>, key: &PoolKey, lookuper: L) {
        let mut pool = pool.lock();
        if let Some(entry) = pool.get_mut(key)
            && entry.idle.len() < MAX_IDLE_PER_KEY
        {
            entry.idle.push(lookuper);
        }
    }

    pub(super) fn take_point(key: &PoolKey) -> Option<Lookuper> {
        take(&POINT, key)
    }

    pub(super) fn put_point(key: &PoolKey, lookuper: Lookuper) {
        put(&POINT, key, lookuper);
    }

    pub(super) fn take_prefix(key: &PoolKey) -> Option<PrefixKeyLookuper> {
        take(&PREFIX, key)
    }

    pub(super) fn put_prefix(key: &PoolKey, lookuper: PrefixKeyLookuper) {
        put(&PREFIX, key, lookuper);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    /// The six refusals `validate_prefix_lookup` can produce, in the client's exact wording.
    ///
    /// Reproducing the messages is the point of the test: they are what the caller ends up reading, and the
    /// mapping must not paraphrase, truncate, or swallow any of them. Each entry corresponds to one rule of
    /// `fluss-rust/crates/fluss/src/client/table/lookup.rs::validate_prefix_lookup`.
    ///
    /// Rule 1 is included for completeness of the mapping property, but is not reachable through the builder:
    /// `FlussTable::new_lookup` rejects a table without a primary key first, and with `UnsupportedOperation`
    /// rather than `IllegalArgument` — the case covered by the next test.
    fn client_refusals() -> Vec<(&'static str, String)> {
        vec![
            (
                "1: log table (unreachable via the builder)",
                "Log table fluss.events doesn't support prefix lookup".to_string(),
            ),
            (
                "2: no bucket keys",
                "Can not perform prefix lookup on table 'fluss.users', because it has no bucket keys."
                    .to_string(),
            ),
            (
                "3: bucket keys are not a prefix of the physical primary keys",
                "Can not perform prefix lookup on table 'fluss.users', because the bucket keys [\"b\"] \
                 is not a prefix subset of the physical primary keys [\"a\", \"b\"] (excluded partition \
                 fields if present)."
                    .to_string(),
            ),
            (
                "4: lookup columns omit a partition field",
                "Can not perform prefix lookup on table 'fluss.orders', because the lookup columns \
                 [\"user_id\"] must contain all partition fields [\"region\"]."
                    .to_string(),
            ),
            (
                "5: lookup columns are not the bucket keys in order",
                "Can not perform prefix lookup on table 'fluss.orders', because the lookup columns \
                 [\"region\", \"item_id\"] must contain all bucket keys [\"user_id\"] in order."
                    .to_string(),
            ),
            (
                "6: lookup columns are the whole physical primary key",
                "Can not perform prefix lookup on table 'fluss.users', because the lookup columns \
                 [\"id\"] equals the physical primary keys [\"id\"]. Please use primary key lookup \
                 (Lookuper without lookup_by) instead."
                    .to_string(),
            ),
        ]
    }

    #[test]
    fn every_client_prefix_refusal_becomes_an_invalid_argument_with_its_own_message() {
        for (rule, message) in client_refusals() {
            let mapped = map_lookuper_error(FlussClientError::IllegalArgument {
                message: message.clone(),
            });
            assert_eq!(mapped.kind(), ErrorKind::InvalidArgument, "rule {rule}");
            assert_eq!(
                mapped.message(),
                message,
                "rule {rule} must reach the caller verbatim"
            );
            assert_eq!(mapped.kind().http_status(), 400, "rule {rule}");
        }
    }

    #[test]
    fn a_non_argument_failure_keeps_the_shared_infrastructure_mapping() {
        let mapped = map_lookuper_error(FlussClientError::UnsupportedOperation {
            message: "Lookup is only supported for primary key tables".to_string(),
        });
        assert_eq!(mapped.kind(), ErrorKind::Unsupported);
    }

    #[test]
    fn key_values_map_onto_their_native_datum_in_declared_order() {
        let row = to_native_row(&[
            KeyValue::Boolean(true),
            KeyValue::TinyInt(-1),
            KeyValue::SmallInt(2),
            KeyValue::Int(3),
            KeyValue::BigInt(4),
            KeyValue::Float(1.5),
            KeyValue::Double(2.5),
            KeyValue::String("eu".to_string()),
            KeyValue::Bytes(vec![1, 2, 3]),
            KeyValue::Decimal {
                unscaled: 12_345,
                precision: 5,
                scale: 2,
            },
            KeyValue::Date {
                days_since_epoch: 19_000,
            },
            KeyValue::Time {
                millis_of_day: 3_600_000,
            },
            KeyValue::TimestampNtz {
                millis: 1_700_000_000_000,
                nanos_of_milli: 500,
            },
            KeyValue::TimestampLtz {
                epoch_millis: 1_700_000_000_000,
                nanos_of_milli: 0,
            },
        ])
        .expect("every fixture value is representable");

        assert_eq!(row.values.len(), 14);
        assert_eq!(row.values[0], Datum::Bool(true));
        assert_eq!(row.values[3], Datum::Int32(3));
        assert_eq!(row.values[4], Datum::Int64(4));
        assert_eq!(row.values[7], Datum::String(Cow::Borrowed("eu")));
        assert_eq!(row.values[10], Datum::Date(Date::new(19_000)));
        assert_eq!(row.values[11], Datum::Time(Time::new(3_600_000)));
        assert!(matches!(row.values[12], Datum::TimestampNtz(_)));
        assert!(matches!(row.values[13], Datum::TimestampLtz(_)));
    }

    #[test]
    fn a_key_value_the_native_types_cannot_hold_is_an_invalid_argument() {
        let error = to_datum(&KeyValue::Decimal {
            unscaled: 1,
            precision: 5,
            scale: -1,
        })
        .expect_err("a negative scale has no native representation");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn truncation_cuts_at_the_cap_and_flags_only_when_rows_were_dropped() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .expect("a well formed fixture batch");

        match truncate(batch.clone(), 5) {
            PrefixOutcomeKind::Rows { batch, truncated } => {
                assert_eq!(batch.num_rows(), 5);
                assert!(!truncated, "a result exactly at the cap is not truncated");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match truncate(batch, 2) {
            PrefixOutcomeKind::Rows { batch, truncated } => {
                assert_eq!(batch.num_rows(), 2);
                assert!(truncated);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
}
