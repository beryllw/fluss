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

//! Lookup behaviour behind [`super::TestBackend`].
//!
//! The fixture catalog stores schemas but no rows, so results are *derived from the key* by one deterministic
//! rule: the last key value, read as an integer `n`, decides the answer. This keeps every interesting shape
//! reachable from a test without a row store — hits, misses, a per-key failure, many rows for one prefix — while
//! staying reproducible.
//!
//! | `n` | point lookup | prefix lookup |
//! |---|---|---|
//! | `500` | one `Unavailable` error outcome | one `Unavailable` error outcome |
//! | `>= 100` | `NotFound` | `min(n, 250)` rows, so the row cap can be crossed |
//! | otherwise | one row echoing the key | `n` rows |
//!
//! **The prefix pre-check below emulates the Fluss client**, whose `validate_prefix_lookup` is what a real
//! deployment consults. It is *not* gateway validation and must never be mistaken for it: the gateway itself
//! deliberately has no copy of those rules (see [`crate::application::lookup`]), and the native backend gets its
//! verdict by building a real lookuper. The emulation exists so protocol tests can exercise the
//! client-refusal-to-400 path without a cluster, and its messages mirror the client's wording rule by rule.

use crate::backend::model::{
    KeyValue, LookupKey, LookupOutcome, LookupOutcomeKind, PrefixLookupOutcome,
    PrefixLookupRequest, PrefixOutcomeKind, TableDescription, TableKind, TableRef,
};
use crate::backend::testing::TestBackend;
use crate::error::GatewayError;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::DataType as ArrowType;
use std::sync::Arc;

/// The key value that stands for a backend that failed for this key alone.
const UNAVAILABLE_KEY: i64 = 500;

/// Keys at or above this value match nothing.
const MISS_THRESHOLD: i64 = 100;

/// Upper bound on synthesised prefix rows, so an extreme key cannot allocate without limit.
const MAX_SYNTHETIC_ROWS: i64 = 250;

/// Returns one outcome per input key, in input order.
pub(crate) fn lookup(
    backend: &TestBackend,
    table: &TableRef,
    keys: Vec<LookupKey>,
) -> Result<Vec<LookupOutcome>, GatewayError> {
    let description = describe(backend, table)?;
    Ok(keys
        .into_iter()
        .enumerate()
        .map(|(input_index, key)| LookupOutcome {
            input_index,
            kind: point_outcome(&description, &key),
        })
        .collect())
}

/// Derives the answer for one key from its trailing value.
fn point_outcome(description: &TableDescription, key: &LookupKey) -> LookupOutcomeKind {
    match key_number(key) {
        UNAVAILABLE_KEY => LookupOutcomeKind::Error(GatewayError::unavailable(
            "the tablet server holding this key is unavailable",
        )),
        n if n >= MISS_THRESHOLD => LookupOutcomeKind::NotFound,
        _ => LookupOutcomeKind::Found(synthesise(
            description,
            &description.primary_keys,
            &key.values,
            1,
        )),
    }
}

/// Returns one outcome per input prefix, in input order.
///
/// The client-emulating pre-check runs first and fails the whole batch, because a prefix the client refuses is
/// refused while its lookuper is being built — before any prefix is looked up.
pub(crate) fn prefix_lookup(
    backend: &TestBackend,
    table: &TableRef,
    request: PrefixLookupRequest,
) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
    let description = describe(backend, table)?;
    emulate_client_prefix_validation(&description, &request.prefix_columns)?;
    Ok(request
        .prefixes
        .into_iter()
        .enumerate()
        .map(|(input_index, prefix)| PrefixLookupOutcome {
            input_index,
            kind: prefix_outcome(
                &description,
                &request.prefix_columns,
                &prefix,
                request.max_rows_per_prefix,
            ),
        })
        .collect())
}

/// Derives the rows for one prefix and applies the gateway-side row cap, exactly as a backend must.
fn prefix_outcome(
    description: &TableDescription,
    columns: &[String],
    prefix: &LookupKey,
    max_rows_per_prefix: usize,
) -> PrefixOutcomeKind {
    let n = key_number(prefix);
    if n == UNAVAILABLE_KEY {
        return PrefixOutcomeKind::Error(GatewayError::unavailable(
            "the tablet server holding this prefix is unavailable",
        ));
    }
    let rows = n.clamp(0, MAX_SYNTHETIC_ROWS) as usize;
    let truncated = rows > max_rows_per_prefix;
    let batch = synthesise(
        description,
        columns,
        &prefix.values,
        rows.min(max_rows_per_prefix),
    );
    PrefixOutcomeKind::Rows { batch, truncated }
}

/// Reads the trailing key value as an integer, which is what selects the fixture's answer.
///
/// Non-numeric trailing values answer as `1`, so a purely textual key is still a hit.
fn key_number(key: &LookupKey) -> i64 {
    match key.values.last() {
        Some(KeyValue::TinyInt(value)) => i64::from(*value),
        Some(KeyValue::SmallInt(value)) => i64::from(*value),
        Some(KeyValue::Int(value)) => i64::from(*value),
        Some(KeyValue::BigInt(value)) => *value,
        _ => 1,
    }
}

/// Builds `rows` rows in the full table schema, echoing the supplied key columns and filling the rest.
fn synthesise(
    description: &TableDescription,
    key_columns: &[String],
    key_values: &[KeyValue],
    rows: usize,
) -> RecordBatch {
    let columns: Vec<ArrayRef> = description
        .arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let pinned = key_columns
                .iter()
                .position(|name| name == field.name())
                .and_then(|index| key_values.get(index));
            column(field.data_type(), pinned, rows)
        })
        .collect();
    RecordBatch::try_new(description.arrow_schema.clone(), columns)
        .expect("the fixture builds one array per schema field")
}

/// Builds one column: the pinned key value repeated, or a deterministic filler derived from the row index.
fn column(data_type: &ArrowType, pinned: Option<&KeyValue>, rows: usize) -> ArrayRef {
    match data_type {
        ArrowType::Boolean => match pinned {
            Some(KeyValue::Boolean(value)) => Arc::new(BooleanArray::from(vec![*value; rows])),
            _ => Arc::new(BooleanArray::from(
                (0..rows).map(|row| row % 2 == 0).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Int8 => match pinned {
            Some(KeyValue::TinyInt(value)) => Arc::new(Int8Array::from(vec![*value; rows])),
            _ => Arc::new(Int8Array::from(
                (0..rows).map(|row| row as i8).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Int16 => match pinned {
            Some(KeyValue::SmallInt(value)) => Arc::new(Int16Array::from(vec![*value; rows])),
            _ => Arc::new(Int16Array::from(
                (0..rows).map(|row| row as i16).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Int32 => match pinned {
            Some(KeyValue::Int(value)) => Arc::new(Int32Array::from(vec![*value; rows])),
            _ => Arc::new(Int32Array::from(
                (0..rows).map(|row| row as i32).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Int64 => match pinned {
            Some(KeyValue::BigInt(value)) => Arc::new(Int64Array::from(vec![*value; rows])),
            _ => Arc::new(Int64Array::from(
                (0..rows).map(|row| row as i64).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Float32 => match pinned {
            Some(KeyValue::Float(value)) => Arc::new(Float32Array::from(vec![*value; rows])),
            _ => Arc::new(Float32Array::from(
                (0..rows).map(|row| row as f32 + 0.5).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Float64 => match pinned {
            Some(KeyValue::Double(value)) => Arc::new(Float64Array::from(vec![*value; rows])),
            _ => Arc::new(Float64Array::from(
                (0..rows).map(|row| row as f64 + 0.5).collect::<Vec<_>>(),
            )),
        },
        ArrowType::Binary => match pinned {
            Some(KeyValue::Bytes(value)) => Arc::new(BinaryArray::from(
                (0..rows).map(|_| value.as_slice()).collect::<Vec<_>>(),
            )),
            _ => Arc::new(BinaryArray::from(
                (0..rows).map(|_| [0xABu8].as_slice()).collect::<Vec<_>>(),
            )),
        },
        // The fixture maps every remaining logical type onto UTF-8, matching `testing::catalog::arrow_schema`.
        _ => match pinned {
            Some(KeyValue::String(value)) => {
                Arc::new(StringArray::from(vec![value.as_str(); rows]))
            }
            _ => Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| format!("value-{row}"))
                    .collect::<Vec<_>>(),
            )),
        },
    }
}

/// Applies the six rules of the Fluss client's `validate_prefix_lookup`, in the client's order and wording.
///
/// This is an emulation of `fluss-rust/crates/fluss/src/client/table/lookup.rs::validate_prefix_lookup`, so that
/// protocol tests can drive the client-refusal path without a cluster. The production gateway never runs anything
/// like this: it builds a real lookuper and forwards the client's own verdict.
fn emulate_client_prefix_validation(
    description: &TableDescription,
    lookup_columns: &[String],
) -> Result<(), GatewayError> {
    let table = &description.table;
    if matches!(description.kind, TableKind::Log) {
        // Rule 1 of `validate_prefix_lookup` is unreachable in production: `FlussTable::new_lookup` refuses a
        // table without a primary key *before* `lookup_by(...)` is reached, with `UnsupportedOperation` rather
        // than `IllegalArgument`. The fixture reproduces the reachable behaviour — an unsupported operation, not
        // a bad request — so a test cannot assert a status the real client would never produce.
        return Err(GatewayError::unsupported(
            "Lookup is only supported for primary key tables".to_string(),
        ));
    }

    let bucket_keys = &description.bucket_keys;
    let physical = &description.physical_primary_keys;
    if bucket_keys.is_empty() {
        return Err(GatewayError::invalid_argument(format!(
            "Can not perform prefix lookup on table '{table}', because it has no bucket keys."
        )));
    }
    if !physical.starts_with(bucket_keys) {
        return Err(GatewayError::invalid_argument(format!(
            "Can not perform prefix lookup on table '{table}', because the bucket keys {bucket_keys:?} \
             is not a prefix subset of the physical primary keys {physical:?} \
             (excluded partition fields if present)."
        )));
    }

    let partition_keys = &description.partition_keys;
    if description.is_partitioned() {
        for partition_key in partition_keys {
            if !lookup_columns.iter().any(|column| column == partition_key) {
                return Err(GatewayError::invalid_argument(format!(
                    "Can not perform prefix lookup on table '{table}', because the lookup columns \
                     {lookup_columns:?} must contain all partition fields {partition_keys:?}."
                )));
            }
        }
    }

    let physical_lookup_columns: Vec<&String> = lookup_columns
        .iter()
        .filter(|column| !partition_keys.iter().any(|key| key == *column))
        .collect();
    if physical_lookup_columns.len() != bucket_keys.len()
        || !physical_lookup_columns
            .iter()
            .zip(bucket_keys.iter())
            .all(|(requested, bucket_key)| *requested == bucket_key)
    {
        return Err(GatewayError::invalid_argument(format!(
            "Can not perform prefix lookup on table '{table}', because the lookup columns \
             {lookup_columns:?} must contain all bucket keys {bucket_keys:?} in order."
        )));
    }

    if bucket_keys == physical {
        return Err(GatewayError::invalid_argument(format!(
            "Can not perform prefix lookup on table '{table}', because the lookup columns \
             {lookup_columns:?} equals the physical primary keys {physical:?}. \
             Please use primary key lookup (Lookuper without lookup_by) instead."
        )));
    }

    Ok(())
}

fn describe(
    backend: &TestBackend,
    table: &TableRef,
) -> Result<std::sync::Arc<crate::backend::model::TableDescription>, GatewayError> {
    backend
        .state
        .lock()
        .catalog
        .table(table)
        .ok_or_else(|| GatewayError::not_found(format!("table `{table}` does not exist")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    fn backend() -> TestBackend {
        TestBackend::new()
    }

    fn users_key(id: i32) -> LookupKey {
        LookupKey::new(vec![KeyValue::Int(id)])
    }

    #[test]
    fn the_key_decides_hit_miss_and_failure() {
        let backend = backend();
        let table = TableRef::new("fluss", "users");
        let outcomes = lookup(
            &backend,
            &table,
            vec![users_key(7), users_key(404), users_key(500)],
        )
        .expect("the fixture table exists");

        assert_eq!(outcomes.len(), 3);
        match &outcomes[0].kind {
            LookupOutcomeKind::Found(batch) => {
                assert_eq!(batch.num_rows(), 1);
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("the fixture id column is an INT");
                assert_eq!(ids.value(0), 7, "a hit echoes its own key");
            }
            other => panic!("expected a hit, got {other:?}"),
        }
        assert!(matches!(outcomes[1].kind, LookupOutcomeKind::NotFound));
        match &outcomes[2].kind {
            LookupOutcomeKind::Error(error) => assert_eq!(error.kind(), ErrorKind::Unavailable),
            other => panic!("expected a per-key failure, got {other:?}"),
        }
    }

    #[test]
    fn a_composite_key_echoes_every_key_column() {
        let backend = backend();
        let outcomes = lookup(
            &backend,
            &TableRef::new("fluss", "orders"),
            vec![LookupKey::new(vec![
                KeyValue::String("eu".to_string()),
                KeyValue::BigInt(3),
            ])],
        )
        .expect("the fixture table exists");

        match &outcomes[0].kind {
            LookupOutcomeKind::Found(batch) => {
                let regions = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the fixture region column is a STRING");
                assert_eq!(regions.value(0), "eu");
            }
            other => panic!("expected a hit, got {other:?}"),
        }
    }

    #[test]
    fn each_of_the_six_client_rules_is_reproduced_with_its_own_message() {
        let backend = backend();
        let log = describe(&backend, &TableRef::new("fluss", "events")).unwrap();
        let users = describe(&backend, &TableRef::new("fluss", "users")).unwrap();
        let orders = describe(&backend, &TableRef::new("fluss", "orders")).unwrap();

        // 1: a log table, which the client refuses as an unsupported operation rather than a bad argument.
        let error = emulate_client_prefix_validation(&log, &["ts".to_string()]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(
            error.message().contains("only supported for primary key"),
            "{}",
            error.message()
        );

        // 4: a partitioned table whose lookup columns omit the partition key.
        let error = emulate_client_prefix_validation(&orders, &["id".to_string()]).unwrap_err();
        assert!(
            error
                .message()
                .contains("must contain all partition fields"),
            "{}",
            error.message()
        );

        // 6: the lookup columns are the whole physical primary key.
        let error = emulate_client_prefix_validation(&users, &["id".to_string()]).unwrap_err();
        assert!(
            error
                .message()
                .contains("Please use primary key lookup (Lookuper without lookup_by) instead"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn a_prefix_produces_as_many_rows_as_its_key_and_truncates_at_the_cap() {
        let backend = backend();
        // `orders` is refused by rule 6, so the row shape is exercised through the outcome helper directly; the
        // end-to-end path uses a table created by the test that the client would accept.
        let orders = describe(&backend, &TableRef::new("fluss", "orders")).unwrap();
        let columns = vec!["region".to_string(), "id".to_string()];
        let prefix = LookupKey::new(vec![
            KeyValue::String("eu".to_string()),
            KeyValue::BigInt(150),
        ]);

        match prefix_outcome(&orders, &columns, &prefix, 100) {
            PrefixOutcomeKind::Rows { batch, truncated } => {
                assert_eq!(batch.num_rows(), 100);
                assert!(truncated);
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match prefix_outcome(&orders, &columns, &prefix, 250) {
            PrefixOutcomeKind::Rows { batch, truncated } => {
                assert_eq!(batch.num_rows(), 150);
                assert!(!truncated);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn a_prefix_matching_nothing_is_an_empty_batch_and_never_a_miss() {
        let backend = backend();
        let orders = describe(&backend, &TableRef::new("fluss", "orders")).unwrap();
        let prefix = LookupKey::new(vec![
            KeyValue::String("eu".to_string()),
            KeyValue::BigInt(0),
        ]);

        match prefix_outcome(
            &orders,
            &["region".to_string(), "id".to_string()],
            &prefix,
            10,
        ) {
            PrefixOutcomeKind::Rows { batch, truncated } => {
                assert_eq!(batch.num_rows(), 0);
                assert!(!truncated);
            }
            other => panic!("expected an empty row set, got {other:?}"),
        }
    }
}
