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

//! Arrow `RecordBatch` -> Fluss `GenericRow` conversion for KV writes
//! (`KvUpsert` / `KvDelete`). Backs the real [`FlussBackendFacade`]; see
//! `design/direct-path.md` §P5 / `design/infra.md` §P6.2.
//!
//! Log appends do NOT go through this module — the Fluss `AppendWriter` takes a
//! `RecordBatch` directly (`append_arrow_batch`), so there is no row-by-row
//! conversion on the log path.
//!
//! Phase 1 supports the base column types `Int32` / `Int64` / `Utf8` /
//! `Boolean` / `Float64` (plus `Float32`). Any other Arrow type is rejected with
//! a clear [`GatewayError::InvalidArgument`] at this boundary rather than being
//! silently dropped — there is no schema-on-write coercion here. Nulls map to
//! `Datum::Null`.

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use fluss::row::{Datum, GenericRow};

use crate::error::{GatewayError, GatewayResult};

/// Convert every row of `batch` into an owned Fluss [`GenericRow`].
///
/// Column order is preserved (field `i` of the row is column `i` of the batch),
/// so the caller is responsible for having decoded the body against the target
/// table schema (no schema-on-write). Returns one `GenericRow` per batch row.
///
/// Returns `InvalidArgument` if any column has an unsupported Arrow type; the
/// error names the offending column and type so the caller gets actionable 400
/// feedback (direct-path.md §3/§6).
pub fn batch_to_generic_rows(batch: &RecordBatch) -> GatewayResult<Vec<GenericRow<'static>>> {
    let num_cols = batch.num_columns();
    let num_rows = batch.num_rows();

    // Pre-validate every column type once, up front, so an unsupported type fails
    // before we allocate any rows.
    let schema = batch.schema();
    for field in schema.fields().iter() {
        if !is_supported(field.data_type()) {
            return Err(GatewayError::InvalidArgument(format!(
                "unsupported column type for KV write: column `{}` has type {} \
                 (supported: Int32, Int64, Utf8, Boolean, Float64, Float32)",
                field.name(),
                field.data_type()
            )));
        }
    }

    let mut rows: Vec<GenericRow<'static>> = (0..num_rows)
        .map(|_| GenericRow::new(num_cols))
        .collect();

    for col_idx in 0..num_cols {
        let column = batch.column(col_idx);
        set_column(&mut rows, col_idx, column)?;
    }

    Ok(rows)
}

fn is_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Boolean
            | DataType::Float64
            | DataType::Float32
    )
}

/// Set one column across all rows, dispatching on the Arrow array type. Nulls
/// become `Datum::Null` (the default a fresh `GenericRow` already holds, but set
/// explicitly for clarity).
fn set_column(
    rows: &mut [GenericRow<'static>],
    col_idx: usize,
    column: &dyn Array,
) -> GatewayResult<()> {
    macro_rules! fill {
        ($arr_ty:ty, $to_datum:expr) => {{
            let arr = column
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| {
                    GatewayError::Internal(format!(
                        "column {col_idx} arrow downcast mismatch for {}",
                        column.data_type()
                    ))
                })?;
            for (row_idx, row) in rows.iter_mut().enumerate() {
                if arr.is_null(row_idx) {
                    row.set_field(col_idx, Datum::Null);
                } else {
                    row.set_field(col_idx, $to_datum(arr.value(row_idx)));
                }
            }
        }};
    }

    match column.data_type() {
        DataType::Int32 => fill!(Int32Array, |v: i32| v),
        DataType::Int64 => fill!(Int64Array, |v: i64| v),
        DataType::Boolean => fill!(BooleanArray, |v: bool| v),
        DataType::Float64 => fill!(Float64Array, |v: f64| v),
        DataType::Float32 => fill!(Float32Array, |v: f32| v),
        // Strings must be owned so the produced rows are `'static` and outlive the
        // borrowed batch (the writer flush happens after this returns).
        DataType::Utf8 => fill!(StringArray, |v: &str| v.to_string()),
        other => {
            return Err(GatewayError::InvalidArgument(format!(
                "unsupported column type for KV write: column {col_idx} has type {other}"
            )))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(fields: Vec<Field>, columns: Vec<Arc<dyn Array>>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn converts_supported_base_types() {
        let b = batch(
            vec![
                Field::new("i32", DataType::Int32, false),
                Field::new("i64", DataType::Int64, false),
                Field::new("s", DataType::Utf8, false),
                Field::new("b", DataType::Boolean, false),
                Field::new("f", DataType::Float64, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ],
        );
        let rows = batch_to_generic_rows(&b).unwrap();
        assert_eq!(rows.len(), 2);
        // Spot-check field encodings round-trip into the expected Datums.
        assert_eq!(rows[0].values[0], Datum::Int32(1));
        assert_eq!(rows[1].values[1], Datum::Int64(20));
        assert_eq!(rows[0].values[2], Datum::String("a".into()));
        assert_eq!(rows[1].values[3], Datum::Bool(false));
        assert_eq!(rows[0].values[4], Datum::Float64(1.5.into()));
    }

    #[test]
    fn nulls_become_datum_null() {
        let b = batch(
            vec![
                Field::new("i", DataType::Int32, true),
                Field::new("s", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![None, Some("x")])),
            ],
        );
        let rows = batch_to_generic_rows(&b).unwrap();
        assert_eq!(rows[1].values[0], Datum::Null);
        assert_eq!(rows[0].values[1], Datum::Null);
        assert_eq!(rows[1].values[1], Datum::String("x".into()));
    }

    #[test]
    fn unsupported_type_is_invalid_argument() {
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            )],
            vec![Arc::new(TimestampSecondArray::from(vec![1i64, 2]))],
        );
        let err = batch_to_generic_rows(&b).unwrap_err();
        assert!(matches!(err, GatewayError::InvalidArgument(_)));
        // The message must name the column so a 400 is actionable.
        assert!(err.to_string().contains("ts"));
    }

    #[test]
    fn empty_batch_yields_no_rows() {
        let b = batch(
            vec![Field::new("i", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        );
        let rows = batch_to_generic_rows(&b).unwrap();
        assert!(rows.is_empty());
    }
}
