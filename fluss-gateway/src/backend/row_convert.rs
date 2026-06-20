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
//! `design/direct-path.md` / `design/infra.md`.
//!
//! Log appends do NOT go through this module — the Fluss `AppendWriter` takes a
//! `RecordBatch` directly (`append_arrow_batch`), so there is no row-by-row
//! conversion on the log path.
//!
//! Covers every Fluss-writable column type: the integer family
//! (`Int8`/`Int16`/`Int32`/`Int64`), floats (`Float32`/`Float64`), `Boolean`,
//! strings (`Utf8`/`LargeUtf8`), binary (`Binary`/`LargeBinary`/`FixedSizeBinary`
//! → `Blob`), `Decimal128`, `Date32`, `Time32`/`Time64` (normalized to
//! milliseconds-since-midnight), and `Timestamp` (timezone-less → `TimestampNtz`,
//! timezoned → `TimestampLtz`, normalized to millis + nanos-of-millisecond). The
//! conversion mirrors the Fluss read path (`row::column_vector`). Any other Arrow
//! type is rejected with a clear [`GatewayError::InvalidArgument`] at this
//! boundary rather than silently dropped — there is no schema-on-write coercion.
//! Nulls map to `Datum::Null`.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use fluss::row::{Datum, Date, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};

use crate::error::{GatewayError, GatewayResult};

/// Convert every row of `batch` into an owned Fluss [`GenericRow`].
///
/// Column order is preserved (field `i` of the row is column `i` of the batch),
/// so the caller is responsible for having decoded the body against the target
/// table schema (no schema-on-write). Returns one `GenericRow` per batch row.
///
/// Returns `InvalidArgument` if any column has an unsupported Arrow type; the
/// error names the offending column and type so the caller gets actionable 400
/// feedback (direct-path.md).
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
                 (supported: integers, floats, boolean, string, binary, decimal, \
                 date, time, timestamp)",
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
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Boolean
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
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
        DataType::Int8 => fill!(Int8Array, |v: i8| v),
        DataType::Int16 => fill!(Int16Array, |v: i16| v),
        DataType::Int32 => fill!(Int32Array, |v: i32| v),
        DataType::Int64 => fill!(Int64Array, |v: i64| v),
        DataType::Boolean => fill!(BooleanArray, |v: bool| v),
        DataType::Float32 => fill!(Float32Array, |v: f32| v),
        DataType::Float64 => fill!(Float64Array, |v: f64| v),
        // Strings/blobs must be owned so the produced rows are `'static` and
        // outlive the borrowed batch (the writer flush happens after this returns).
        DataType::Utf8 => fill!(StringArray, |v: &str| v.to_string()),
        DataType::LargeUtf8 => fill!(LargeStringArray, |v: &str| v.to_string()),
        DataType::Binary => fill!(BinaryArray, |v: &[u8]| v.to_vec()),
        DataType::LargeBinary => fill!(LargeBinaryArray, |v: &[u8]| v.to_vec()),
        DataType::FixedSizeBinary(_) => fill!(FixedSizeBinaryArray, |v: &[u8]| v.to_vec()),
        DataType::Date32 => fill!(Date32Array, |v: i32| Date::new(v)),
        // Time is normalized to milliseconds since midnight (Fluss `Time` storage).
        DataType::Time32(TimeUnit::Second) => {
            fill!(Time32SecondArray, |v: i32| Time::new(v.saturating_mul(1000)))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            fill!(Time32MillisecondArray, |v: i32| Time::new(v))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            fill!(Time64MicrosecondArray, |v: i64| Time::new((v / 1_000) as i32))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            fill!(Time64NanosecondArray, |v: i64| Time::new(
                (v / 1_000_000) as i32
            ))
        }
        DataType::Decimal128(precision, scale) => {
            set_decimal_column(rows, col_idx, column, *precision as u32, *scale as u32)?
        }
        DataType::Timestamp(unit, tz) => {
            set_timestamp_column(rows, col_idx, column, *unit, tz.is_some())?
        }
        other => {
            return Err(GatewayError::InvalidArgument(format!(
                "unsupported column type for KV write: column {col_idx} has type {other}"
            )))
        }
    }
    Ok(())
}

/// Fill a `Decimal128(p, s)` column. The Arrow scale equals the target scale, so
/// no rescaling is needed; overflow/precision errors map to `InvalidArgument`.
fn set_decimal_column(
    rows: &mut [GenericRow<'static>],
    col_idx: usize,
    column: &dyn Array,
    precision: u32,
    scale: u32,
) -> GatewayResult<()> {
    let arr = column
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| GatewayError::Internal(format!("column {col_idx} decimal downcast")))?;
    for (row_idx, row) in rows.iter_mut().enumerate() {
        if arr.is_null(row_idx) {
            row.set_field(col_idx, Datum::Null);
        } else {
            let dec = Decimal::from_arrow_decimal128(arr.value(row_idx), scale as i64, precision, scale)
                .map_err(|e| {
                    GatewayError::InvalidArgument(format!(
                        "column {col_idx} decimal({precision},{scale}) conversion failed: {e}"
                    ))
                })?;
            row.set_field(col_idx, dec);
        }
    }
    Ok(())
}

/// Fill a `Timestamp` column, normalizing the Arrow unit to milliseconds plus a
/// nanos-of-millisecond remainder. A timezone-less column maps to `TimestampNtz`,
/// a timezoned column to `TimestampLtz`. `div_euclid`/`rem_euclid` keep the nanos
/// remainder in `[0, 999_999]` even for pre-epoch (negative) values.
fn set_timestamp_column(
    rows: &mut [GenericRow<'static>],
    col_idx: usize,
    column: &dyn Array,
    unit: TimeUnit,
    is_ltz: bool,
) -> GatewayResult<()> {
    let split = |v: i64| -> (i64, i32) {
        match unit {
            TimeUnit::Second => (v.saturating_mul(1_000), 0),
            TimeUnit::Millisecond => (v, 0),
            TimeUnit::Microsecond => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000) as i32),
            TimeUnit::Nanosecond => (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) as i32),
        }
    };
    let build = |millis: i64, nanos: i32| -> GatewayResult<Datum<'static>> {
        let oops = |e: String| {
            GatewayError::InvalidArgument(format!("column {col_idx} timestamp conversion failed: {e}"))
        };
        if is_ltz {
            let t = if nanos == 0 {
                TimestampLtz::new(millis)
            } else {
                TimestampLtz::from_millis_nanos(millis, nanos).map_err(|e| oops(e.to_string()))?
            };
            Ok(Datum::TimestampLtz(t))
        } else {
            let t = if nanos == 0 {
                TimestampNtz::new(millis)
            } else {
                TimestampNtz::from_millis_nanos(millis, nanos).map_err(|e| oops(e.to_string()))?
            };
            Ok(Datum::TimestampNtz(t))
        }
    };

    macro_rules! fill_ts {
        ($arr_ty:ty) => {{
            let arr = column
                .as_any()
                .downcast_ref::<$arr_ty>()
                .ok_or_else(|| GatewayError::Internal(format!("column {col_idx} timestamp downcast")))?;
            for (row_idx, row) in rows.iter_mut().enumerate() {
                if arr.is_null(row_idx) {
                    row.set_field(col_idx, Datum::Null);
                } else {
                    let (millis, nanos) = split(arr.value(row_idx));
                    row.set_field(col_idx, build(millis, nanos)?);
                }
            }
        }};
    }

    match unit {
        TimeUnit::Second => fill_ts!(TimestampSecondArray),
        TimeUnit::Millisecond => fill_ts!(TimestampMillisecondArray),
        TimeUnit::Microsecond => fill_ts!(TimestampMicrosecondArray),
        TimeUnit::Nanosecond => fill_ts!(TimestampNanosecondArray),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        Time32MillisecondArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

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
    fn converts_full_type_coverage() {
        let b = batch(
            vec![
                Field::new("i8", DataType::Int8, false),
                Field::new("i16", DataType::Int16, false),
                Field::new("dec", DataType::Decimal128(10, 2), false),
                Field::new("d", DataType::Date32, false),
                Field::new("t", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
                Field::new("bin", DataType::Binary, false),
                Field::new("fbin", DataType::FixedSizeBinary(2), false),
            ],
            vec![
                Arc::new(Int8Array::from(vec![7i8])),
                Arc::new(Int16Array::from(vec![300i16])),
                Arc::new(
                    Decimal128Array::from(vec![314i128])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![19_000])),
                Arc::new(Time32MillisecondArray::from(vec![3_600_000])),
                // 1_500_500 µs = 1500 ms + 500 µs (500_000 ns of the millisecond).
                Arc::new(TimestampMicrosecondArray::from(vec![1_500_500])),
                Arc::new(BinaryArray::from(vec![&[1u8, 2, 3][..]])),
                Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![9u8, 8]].into_iter()).unwrap()),
            ],
        );
        let rows = batch_to_generic_rows(&b).unwrap();
        assert_eq!(rows[0].values[0], Datum::Int8(7));
        assert_eq!(rows[0].values[1], Datum::Int16(300));
        assert_eq!(rows[0].values[2], Datum::Decimal(Decimal::from_arrow_decimal128(314, 2, 10, 2).unwrap()));
        assert_eq!(rows[0].values[3], Datum::Date(Date::new(19_000)));
        assert_eq!(rows[0].values[4], Datum::Time(Time::new(3_600_000)));
        assert_eq!(
            rows[0].values[5],
            Datum::TimestampNtz(TimestampNtz::from_millis_nanos(1_500, 500_000).unwrap())
        );
        assert_eq!(rows[0].values[6], Datum::Blob(vec![1u8, 2, 3].into()));
        assert_eq!(rows[0].values[7], Datum::Blob(vec![9u8, 8].into()));
    }

    #[test]
    fn timezoned_timestamp_maps_to_ltz() {
        let b = batch(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            )],
            vec![Arc::new(
                arrow::array::TimestampMillisecondArray::from(vec![1_700_000_000_000i64])
                    .with_timezone("UTC"),
            )],
        );
        let rows = batch_to_generic_rows(&b).unwrap();
        assert_eq!(
            rows[0].values[0],
            Datum::TimestampLtz(TimestampLtz::new(1_700_000_000_000))
        );
    }

    #[test]
    fn unsupported_type_is_invalid_argument() {
        // A nested List has no Fluss scalar mapping on the write path.
        let b = batch(
            vec![Field::new("n", DataType::Null, true)],
            vec![Arc::new(arrow::array::NullArray::new(2))],
        );
        let err = batch_to_generic_rows(&b).unwrap_err();
        assert!(matches!(err, GatewayError::InvalidArgument(_)));
        // The message must name the column so a 400 is actionable.
        assert!(err.to_string().contains('n'));
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
