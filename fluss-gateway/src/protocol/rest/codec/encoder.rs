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

//! FIP-49 JSON rendering for Arrow lookup results.

use crate::error::{GatewayError, GatewayResult};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use serde_json::{Map as JsonMap, Number, Value as JsonValue};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// One top-level lookup row whose field vector preserves schema or projection order.
#[derive(Debug)]
pub(crate) struct EncodedRow {
    fields: Vec<(String, JsonValue)>,
}

impl EncodedRow {
    pub(crate) fn project(self, projection: &[String]) -> GatewayResult<Self> {
        let mut fields = self.fields;
        let mut projected = Vec::with_capacity(projection.len());
        for column in projection {
            let index = fields
                .iter()
                .position(|(name, _)| name == column)
                .ok_or_else(|| {
                    GatewayError::internal(format!(
                        "lookup result is missing projected column `{column}`"
                    ))
                })?;
            projected.push(fields.remove(index));
        }
        Ok(Self { fields: projected })
    }
}

impl Serialize for EncodedRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for (name, value) in &self.fields {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

/// Renders every row in table schema order.
pub(crate) fn record_batch_to_json_rows(batch: &RecordBatch) -> GatewayResult<Vec<EncodedRow>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut fields = Vec::with_capacity(batch.num_columns());
        for (column, field) in batch.columns().iter().zip(schema.fields()) {
            fields.push((
                field.name().clone(),
                value_to_json(column.as_ref(), row_index)?,
            ));
        }
        rows.push(EncodedRow { fields });
    }
    Ok(rows)
}

fn value_to_json(array: &dyn Array, index: usize) -> GatewayResult<JsonValue> {
    if array.is_null(index) {
        return Ok(JsonValue::Null);
    }
    match array.data_type() {
        ArrowDataType::Boolean => Ok(downcast::<BooleanArray>(array)?.value(index).into()),
        ArrowDataType::Int8 => Ok(downcast::<Int8Array>(array)?.value(index).into()),
        ArrowDataType::Int16 => Ok(downcast::<Int16Array>(array)?.value(index).into()),
        ArrowDataType::Int32 => Ok(downcast::<Int32Array>(array)?.value(index).into()),
        ArrowDataType::Int64 => Ok(downcast::<Int64Array>(array)?
            .value(index)
            .to_string()
            .into()),
        ArrowDataType::Float32 => Ok(float_to_json(
            downcast::<Float32Array>(array)?.value(index) as f64
        )),
        ArrowDataType::Float64 => Ok(float_to_json(downcast::<Float64Array>(array)?.value(index))),
        ArrowDataType::Utf8 => Ok(downcast::<StringArray>(array)?.value(index).into()),
        ArrowDataType::Decimal128(_, _) => Ok(downcast::<Decimal128Array>(array)?
            .value_as_string(index)
            .into()),
        ArrowDataType::Binary => Ok(BASE64
            .encode(downcast::<BinaryArray>(array)?.value(index))
            .into()),
        ArrowDataType::FixedSizeBinary(_) => Ok(BASE64
            .encode(downcast::<FixedSizeBinaryArray>(array)?.value(index))
            .into()),
        ArrowDataType::Date32 => {
            Ok(format_date(i64::from(downcast::<Date32Array>(array)?.value(index))).into())
        }
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => time_to_json(array, index),
        ArrowDataType::Timestamp(_, _) => timestamp_to_json(array, index),
        ArrowDataType::List(_) => list_to_json(downcast::<ListArray>(array)?.value(index)),
        ArrowDataType::Struct(_) => struct_to_json(array, index),
        ArrowDataType::Map(_, _) => map_to_json(array, index),
        other => Err(GatewayError::internal(format!(
            "cannot render Arrow type {other} as JSON"
        ))),
    }
}

fn float_to_json(value: f64) -> JsonValue {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    Number::from_f64(value).map_or(JsonValue::Null, JsonValue::Number)
}

fn time_to_json(array: &dyn Array, index: usize) -> GatewayResult<JsonValue> {
    let (nanos_of_day, digits) = match array.data_type() {
        ArrowDataType::Time32(TimeUnit::Second) => (
            i64::from(downcast::<Time32SecondArray>(array)?.value(index)) * NANOS_PER_SECOND,
            0,
        ),
        ArrowDataType::Time32(TimeUnit::Millisecond) => (
            i64::from(downcast::<Time32MillisecondArray>(array)?.value(index)) * NANOS_PER_MILLI,
            3,
        ),
        ArrowDataType::Time64(TimeUnit::Microsecond) => (
            downcast::<Time64MicrosecondArray>(array)?.value(index) * 1_000,
            6,
        ),
        ArrowDataType::Time64(TimeUnit::Nanosecond) => {
            (downcast::<Time64NanosecondArray>(array)?.value(index), 9)
        }
        other => {
            return Err(GatewayError::internal(format!(
                "unsupported Arrow time type {other}"
            )));
        }
    };
    let seconds = nanos_of_day.div_euclid(NANOS_PER_SECOND);
    let fraction = nanos_of_day.rem_euclid(NANOS_PER_SECOND);
    Ok(format!(
        "{}{}",
        format_time_of_day(seconds),
        format_fraction(fraction, digits)
    )
    .into())
}

fn timestamp_to_json(array: &dyn Array, index: usize) -> GatewayResult<JsonValue> {
    let ArrowDataType::Timestamp(unit, zone) = array.data_type() else {
        return Err(GatewayError::internal("expected an Arrow timestamp"));
    };
    let (value, digits) = match unit {
        TimeUnit::Second => (downcast::<TimestampSecondArray>(array)?.value(index), 0),
        TimeUnit::Millisecond => (
            downcast::<TimestampMillisecondArray>(array)?.value(index),
            3,
        ),
        TimeUnit::Microsecond => (
            downcast::<TimestampMicrosecondArray>(array)?.value(index),
            6,
        ),
        TimeUnit::Nanosecond => (downcast::<TimestampNanosecondArray>(array)?.value(index), 9),
    };
    let (units_per_second, nanos_per_unit) = match unit {
        TimeUnit::Second => (1, NANOS_PER_SECOND),
        TimeUnit::Millisecond => (1_000, NANOS_PER_MILLI),
        TimeUnit::Microsecond => (1_000_000, 1_000),
        TimeUnit::Nanosecond => (NANOS_PER_SECOND, 1),
    };
    let seconds = value.div_euclid(units_per_second);
    let fraction = value
        .rem_euclid(units_per_second)
        .checked_mul(nanos_per_unit)
        .ok_or_else(|| GatewayError::internal("timestamp fraction is out of range"))?;
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    Ok(format!(
        "{}T{}{}{}",
        format_date(days),
        format_time_of_day(seconds_of_day),
        format_fraction(fraction, digits),
        if zone.is_some() { "Z" } else { "" }
    )
    .into())
}

fn list_to_json(values: arrow::array::ArrayRef) -> GatewayResult<JsonValue> {
    let mut rendered = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        rendered.push(value_to_json(values.as_ref(), index)?);
    }
    Ok(JsonValue::Array(rendered))
}

fn struct_to_json(array: &dyn Array, index: usize) -> GatewayResult<JsonValue> {
    let row = downcast::<StructArray>(array)?;
    let mut rendered = JsonMap::with_capacity(row.num_columns());
    for (column, field) in row.columns().iter().zip(row.fields()) {
        rendered.insert(field.name().clone(), value_to_json(column.as_ref(), index)?);
    }
    Ok(JsonValue::Object(rendered))
}

fn map_to_json(array: &dyn Array, index: usize) -> GatewayResult<JsonValue> {
    let entries = downcast::<MapArray>(array)?.value(index);
    let keys = entries.column(0);
    let values = entries.column(1);
    // Only string-keyed maps have a JSON-object representation. Preserve the type and
    // spelling of all other keys in the entry-array form accepted by the decoder.
    if keys.data_type() != &ArrowDataType::Utf8 {
        let mut rendered = Vec::with_capacity(entries.len());
        for position in 0..entries.len() {
            let key = value_to_json(keys.as_ref(), position)?;
            if key.is_null() {
                return Err(GatewayError::internal("a map key must not be null"));
            }
            rendered.push(serde_json::json!({
                "key": key,
                "value": value_to_json(values.as_ref(), position)?,
            }));
        }
        return Ok(JsonValue::Array(rendered));
    }
    let mut rendered = JsonMap::with_capacity(entries.len());
    for position in 0..entries.len() {
        let key = match value_to_json(keys.as_ref(), position)? {
            JsonValue::String(value) => value,
            JsonValue::Number(value) => value.to_string(),
            JsonValue::Bool(value) => value.to_string(),
            JsonValue::Null => {
                return Err(GatewayError::internal("a map key must not be null"));
            }
            JsonValue::Array(_) | JsonValue::Object(_) => {
                return Err(GatewayError::internal(
                    "a complex map key cannot be rendered as a JSON object key",
                ));
            }
        };
        rendered.insert(key, value_to_json(values.as_ref(), position)?);
    }
    Ok(JsonValue::Object(rendered))
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> GatewayResult<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        GatewayError::internal(format!(
            "Arrow array type disagrees with its schema: {}",
            array.data_type()
        ))
    })
}

/// Inverse of the proleptic Gregorian conversion used by the input decoder.
fn format_date(days_since_epoch: i64) -> String {
    let shifted = i128::from(days_since_epoch) + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i128::from(month <= 2);
    if year < 0 {
        format!("-{:04}-{month:02}-{day:02}", -year)
    } else {
        format!("{year:04}-{month:02}-{day:02}")
    }
}

fn format_time_of_day(seconds: i64) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3_600,
        seconds % 3_600 / 60,
        seconds % 60
    )
}

fn format_fraction(nanos: i64, digits: u32) -> String {
    if digits == 0 {
        String::new()
    } else {
        let divisor = 10_i64.pow(9 - digits);
        format!(".{:0width$}", nanos / divisor, width = digits as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int32Builder, Int64Array, ListBuilder, MapBuilder, RecordBatch, StringArray,
        StringBuilder, StructArray, Time32MillisecondArray, Time64NanosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn as_json(array: &dyn Array, index: usize) -> JsonValue {
        value_to_json(array, index).unwrap()
    }

    #[test]
    fn renders_lossless_bigints_and_dates() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("day", DataType::Date32, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![9_007_199_254_740_993_i64])),
                Arc::new(Date32Array::from(vec![20_484])),
            ],
        )
        .unwrap();
        let mut rows = record_batch_to_json_rows(&batch).unwrap();
        assert_eq!(
            serde_json::to_string(&rows[0]).unwrap(),
            r#"{"id":"9007199254740993","day":"2026-01-31"}"#
        );
        assert_eq!(
            serde_json::to_string(
                &rows
                    .remove(0)
                    .project(&["day".to_string(), "id".to_string()])
                    .unwrap()
            )
            .unwrap(),
            r#"{"day":"2026-01-31","id":"9007199254740993"}"#
        );
    }

    #[test]
    fn renders_decimal_float_and_binary_values() {
        let decimals = Decimal128Array::from(vec![Some(
            99_999_999_999_999_999_999_999_999_999_999_999_999_i128,
        )])
        .with_precision_and_scale(38, 2)
        .unwrap();
        assert_eq!(
            as_json(&decimals, 0),
            json!("999999999999999999999999999999999999.99")
        );

        let doubles = Float64Array::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5]);
        assert_eq!(as_json(&doubles, 0), json!("NaN"));
        assert_eq!(as_json(&doubles, 1), json!("Infinity"));
        assert_eq!(as_json(&doubles, 2), json!("-Infinity"));
        assert_eq!(as_json(&doubles, 3), json!(1.5));
        assert_eq!(
            as_json(&Float32Array::from(vec![f32::NAN]), 0),
            json!("NaN")
        );

        assert_eq!(
            as_json(&BinaryArray::from(vec![&[0_u8, 1, 254, 255][..]]), 0),
            json!("AAH+/w==")
        );
        let fixed = FixedSizeBinaryArray::try_from_iter(vec![vec![1_u8, 2]].into_iter()).unwrap();
        assert_eq!(as_json(&fixed, 0), json!("AQI="));
    }

    #[test]
    fn renders_temporal_values_at_declared_precision() {
        let millis = Time32MillisecondArray::from(vec![45_296_789]);
        assert_eq!(as_json(&millis, 0), json!("12:34:56.789"));
        let nanos = Time64NanosecondArray::from(vec![45_296_789_123_456]);
        assert_eq!(as_json(&nanos, 0), json!("12:34:56.789123456"));

        let epoch_millis = 1_769_862_896_789_i64;
        let ntz = TimestampMillisecondArray::from(vec![epoch_millis]);
        assert_eq!(as_json(&ntz, 0), json!("2026-01-31T12:34:56.789"));
        let ltz = TimestampMillisecondArray::from(vec![epoch_millis]).with_timezone("UTC");
        assert_eq!(as_json(&ltz, 0), json!("2026-01-31T12:34:56.789Z"));
        let before_epoch = TimestampNanosecondArray::from(vec![-1]).with_timezone("UTC");
        assert_eq!(
            as_json(&before_epoch, 0),
            json!("1969-12-31T23:59:59.999999999Z")
        );
    }

    #[test]
    fn renders_nested_list_row_and_map_values() {
        let mut list = ListBuilder::new(Int32Builder::new());
        list.values().append_value(1);
        list.values().append_null();
        list.values().append_value(3);
        list.append(true);
        assert_eq!(as_json(&list.finish(), 0), json!([1, null, 3]));

        let fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ]);
        let row = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec!["Ada"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef,
            ],
            None,
        );
        assert_eq!(
            as_json(&row, 0),
            json!({"name": "Ada", "id": "9223372036854775807"})
        );

        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("b");
        map.values().append_value(2);
        map.keys().append_value("a");
        map.values().append_value(1);
        map.append(true).unwrap();
        assert_eq!(as_json(&map.finish(), 0), json!({"a": 1, "b": 2}));
    }

    #[test]
    fn non_string_map_keys_use_typed_entries() {
        let mut map = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
        map.keys().append_value(42);
        map.values().append_value("answer");
        map.keys().append_value(-1);
        map.values().append_null();
        map.append(true).unwrap();
        assert_eq!(
            as_json(&map.finish(), 0),
            json!([{"key": 42, "value": "answer"}, {"key": -1, "value": null}])
        );
    }
}
