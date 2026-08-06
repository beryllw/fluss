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

//! Typed JSON value mapping, driven by the table's Arrow schema.
//!
//! One conversion serves the whole REST surface. Rows read from the backend arrive as Arrow record batches and are
//! rendered to JSON here. Lookup key values travel the other way and are parsed against the Arrow column type before
//! native encoding.
//!
//! Value mapping:
//!
//! - BOOLEAN and TINYINT through INT are JSON booleans and numbers
//! - BIGINT and DECIMAL are base-10 strings to avoid IEEE-754 loss
//! - FLOAT and DOUBLE are numbers, with non-finite values rendered as the strings `"NaN"`, `"Infinity"` and
//!   `"-Infinity"`
//! - CHAR and STRING are strings
//! - BINARY and BYTES are base64 strings
//! - DATE, TIME and both TIMESTAMP kinds are ISO-8601 strings, where TIMESTAMP stays zone free and TIMESTAMP_LTZ is UTC
//!   with a `Z`
//! - ARRAY and ROW recurse, MAP is an array of key and value entries
//! - NULL is JSON null

use crate::backend::model::KeyValue;
use crate::error::GatewayError;
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{Datelike, Duration, NaiveDate};
use serde_json::{Map as JsonMap, Number, Value as JsonValue};

/// Nanoseconds in one second.
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Nanoseconds in one millisecond.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Seconds in one day.
const SECONDS_PER_DAY: i64 = 86_400;

/// Renders every row of a record batch as a JSON object keyed by column name.
pub fn record_batch_to_json_rows(
    batch: &RecordBatch,
) -> Result<Vec<JsonMap<String, JsonValue>>, GatewayError> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut object = JsonMap::with_capacity(batch.num_columns());
        for (column, field) in batch.columns().iter().zip(schema.fields()) {
            object.insert(field.name().clone(), value_to_json(column.as_ref(), row)?);
        }
        rows.push(object);
    }
    Ok(rows)
}

/// Renders one Arrow array element as a JSON value.
///
/// The dispatch is one flat match over the array's data type with the real work in small per-type helpers.
pub fn value_to_json(array: &dyn Array, index: usize) -> Result<JsonValue, GatewayError> {
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
        ArrowDataType::LargeUtf8 => Ok(downcast::<LargeStringArray>(array)?.value(index).into()),
        ArrowDataType::Decimal128(_, _) => Ok(downcast::<Decimal128Array>(array)?
            .value_as_string(index)
            .into()),
        ArrowDataType::Binary => Ok(BASE64
            .encode(downcast::<BinaryArray>(array)?.value(index))
            .into()),
        ArrowDataType::LargeBinary => Ok(BASE64
            .encode(downcast::<LargeBinaryArray>(array)?.value(index))
            .into()),
        ArrowDataType::FixedSizeBinary(_) => Ok(BASE64
            .encode(downcast::<FixedSizeBinaryArray>(array)?.value(index))
            .into()),
        ArrowDataType::Date32 => {
            Ok(format_date(downcast::<Date32Array>(array)?.value(index) as i64).into())
        }
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => time_to_json(array, index),
        ArrowDataType::Timestamp(_, _) => timestamp_to_json(array, index),
        ArrowDataType::List(_) => list_to_json(array, index),
        ArrowDataType::Struct(_) => struct_to_json(array, index),
        ArrowDataType::Map(_, _) => map_to_json(array, index),
        other => Err(GatewayError::internal(format!(
            "cannot render Arrow type {other} as JSON"
        ))),
    }
}

/// Renders a float, mapping non-finite values to their string form.
fn float_to_json(value: f64) -> JsonValue {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    Number::from_f64(value).map_or(JsonValue::Null, JsonValue::Number)
}

/// Renders a TIME value as `HH:MM:SS` with the fraction width of its Arrow unit.
fn time_to_json(array: &dyn Array, index: usize) -> Result<JsonValue, GatewayError> {
    let (nanos_of_day, digits) = match array.data_type() {
        ArrowDataType::Time32(TimeUnit::Second) => (
            downcast::<Time32SecondArray>(array)?.value(index) as i64 * NANOS_PER_SECOND,
            0,
        ),
        ArrowDataType::Time32(TimeUnit::Millisecond) => (
            downcast::<Time32MillisecondArray>(array)?.value(index) as i64 * NANOS_PER_MILLI,
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
    let frac_nanos = nanos_of_day.rem_euclid(NANOS_PER_SECOND);
    Ok(format!(
        "{}{}",
        format_time_of_day(seconds),
        format_fraction(frac_nanos, digits)
    )
    .into())
}

/// Renders a timestamp as ISO-8601, zone free without a time zone in the Arrow type and UTC with a `Z` suffix
/// otherwise.
fn timestamp_to_json(array: &dyn Array, index: usize) -> Result<JsonValue, GatewayError> {
    let ArrowDataType::Timestamp(unit, zone) = array.data_type() else {
        return Err(GatewayError::internal("expected an Arrow timestamp type"));
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
    let total_seconds = value.div_euclid(units_per_second);
    let frac_nanos = value
        .rem_euclid(units_per_second)
        .checked_mul(nanos_per_unit)
        .ok_or_else(|| GatewayError::internal("timestamp fraction is out of range"))?;
    let days = total_seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = total_seconds.rem_euclid(SECONDS_PER_DAY);
    let suffix = if zone.is_some() { "Z" } else { "" };
    Ok(format!(
        "{}T{}{}{}",
        format_date(days),
        format_time_of_day(seconds_of_day),
        format_fraction(frac_nanos, digits),
        suffix
    )
    .into())
}

/// Renders a LIST element by recursing into its child array.
fn list_to_json(array: &dyn Array, index: usize) -> Result<JsonValue, GatewayError> {
    let list = downcast::<ListArray>(array)?;
    let element = list.value(index);
    let mut values = Vec::with_capacity(element.len());
    for position in 0..element.len() {
        values.push(value_to_json(element.as_ref(), position)?);
    }
    Ok(JsonValue::Array(values))
}

/// Renders a ROW element as a JSON object of its named fields.
fn struct_to_json(array: &dyn Array, index: usize) -> Result<JsonValue, GatewayError> {
    let row = downcast::<StructArray>(array)?;
    let mut object = JsonMap::with_capacity(row.num_columns());
    for (column, field) in row.columns().iter().zip(row.fields()) {
        object.insert(field.name().clone(), value_to_json(column.as_ref(), index)?);
    }
    Ok(JsonValue::Object(object))
}

/// Renders a MAP element as an array of `{"key", "value"}` entries so that non-string keys and entry order survive.
fn map_to_json(array: &dyn Array, index: usize) -> Result<JsonValue, GatewayError> {
    let map = downcast::<MapArray>(array)?;
    let entries = map.value(index);
    let keys = entries.column(0);
    let values = entries.column(1);
    let mut rendered = Vec::with_capacity(entries.len());
    for position in 0..entries.len() {
        let mut entry = JsonMap::with_capacity(2);
        entry.insert("key".to_string(), value_to_json(keys.as_ref(), position)?);
        entry.insert(
            "value".to_string(),
            value_to_json(values.as_ref(), position)?,
        );
        rendered.push(JsonValue::Object(entry));
    }
    Ok(JsonValue::Array(rendered))
}

/// Parses one JSON lookup key value against the Arrow column type.
///
/// Lossy coercions are rejected with an error naming the column and the expected type. BIGINT and DECIMAL accept both a
/// JSON string and an exact-integer JSON number.
pub fn parse_key_value(
    column: &str,
    data_type: &ArrowDataType,
    value: &JsonValue,
) -> Result<KeyValue, GatewayError> {
    if value.is_null() {
        return Err(GatewayError::invalid_argument(format!(
            "primary key column `{column}` must not be null"
        )));
    }
    match data_type {
        ArrowDataType::Boolean => parse_bool(column, value),
        ArrowDataType::Int8 => {
            parse_small_int(column, value, "TINYINT", i8::MIN as i64, i8::MAX as i64)
                .map(|v| KeyValue::TinyInt(v as i8))
        }
        ArrowDataType::Int16 => {
            parse_small_int(column, value, "SMALLINT", i16::MIN as i64, i16::MAX as i64)
                .map(|v| KeyValue::SmallInt(v as i16))
        }
        ArrowDataType::Int32 => {
            parse_small_int(column, value, "INT", i32::MIN as i64, i32::MAX as i64)
                .map(|v| KeyValue::Int(v as i32))
        }
        ArrowDataType::Int64 => parse_bigint(column, value).map(KeyValue::BigInt),
        ArrowDataType::Float32 => parse_float32(column, value),
        ArrowDataType::Float64 => parse_float64(column, value),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => parse_string(column, value),
        ArrowDataType::Decimal128(precision, scale) => {
            parse_decimal(column, value, *precision, *scale)
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => parse_binary(column, value, None),
        ArrowDataType::FixedSizeBinary(length) => parse_binary(column, value, Some(*length)),
        ArrowDataType::Date32 => parse_date(column, value),
        ArrowDataType::Time32(unit) | ArrowDataType::Time64(unit) => {
            parse_time(column, value, *unit)
        }
        ArrowDataType::Timestamp(unit, zone) => {
            parse_timestamp(column, value, *unit, zone.is_some())
        }
        other => Err(GatewayError::invalid_argument(format!(
            "column `{column}` of type {other} cannot be used as a lookup key"
        ))),
    }
}

/// Builds the standard type-mismatch error for one key column.
fn type_error(column: &str, expected: &str, value: &JsonValue) -> GatewayError {
    GatewayError::invalid_argument(format!(
        "column `{column}` expects {expected}, got {}",
        json_kind(value)
    ))
}

/// Names the JSON kind of a value for error messages, without echoing row data.
fn json_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "a boolean",
        JsonValue::Number(_) => "a number",
        JsonValue::String(_) => "a string",
        JsonValue::Array(_) => "an array",
        JsonValue::Object(_) => "an object",
    }
}

/// Parses a JSON boolean without accepting numeric or string coercions.
fn parse_bool(column: &str, value: &JsonValue) -> Result<KeyValue, GatewayError> {
    value
        .as_bool()
        .map(KeyValue::Boolean)
        .ok_or_else(|| type_error(column, "BOOLEAN (a JSON boolean)", value))
}

/// Parses an exact-integer JSON number, rejecting floats and values outside the given range.
fn parse_small_int(
    column: &str,
    value: &JsonValue,
    type_name: &str,
    min: i64,
    max: i64,
) -> Result<i64, GatewayError> {
    let expected = format!("{type_name} (an integer in [{min}, {max}])");
    let JsonValue::Number(number) = value else {
        return Err(type_error(column, &expected, value));
    };
    let parsed = number
        .as_i64()
        .ok_or_else(|| type_error(column, &expected, value))?;
    if parsed < min || parsed > max {
        return Err(GatewayError::invalid_argument(format!(
            "column `{column}` expects {expected}, value is out of range"
        )));
    }
    Ok(parsed)
}

/// Parses a BIGINT from an exact-integer number or a base-10 string.
fn parse_bigint(column: &str, value: &JsonValue) -> Result<i64, GatewayError> {
    let expected = "BIGINT (an exact integer number or a base-10 string)";
    match value {
        JsonValue::Number(number) => number
            .as_i64()
            .ok_or_else(|| type_error(column, expected, value)),
        JsonValue::String(text) => text
            .parse::<i64>()
            .map_err(|_| type_error(column, expected, value)),
        _ => Err(type_error(column, expected, value)),
    }
}

/// Parses a finite float from a number or one of the non-finite string spellings.
fn parse_float(column: &str, value: &JsonValue, type_name: &str) -> Result<f64, GatewayError> {
    let expected = format!("{type_name} (a number or \"NaN\", \"Infinity\", \"-Infinity\")");
    match value {
        JsonValue::Number(number) => number
            .as_f64()
            .ok_or_else(|| type_error(column, &expected, value)),
        JsonValue::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(type_error(column, &expected, value)),
        },
        _ => Err(type_error(column, &expected, value)),
    }
}

/// Parses a FLOAT value and rejects finite inputs that overflow `f32`.
fn parse_float32(column: &str, value: &JsonValue) -> Result<KeyValue, GatewayError> {
    let parsed = parse_float(column, value, "FLOAT")?;
    let narrowed = parsed as f32;
    if narrowed.is_infinite() && parsed.is_finite() {
        return Err(GatewayError::invalid_argument(format!(
            "column `{column}` expects FLOAT, value is out of 32-bit float range"
        )));
    }
    Ok(KeyValue::Float(narrowed))
}

/// Parses a DOUBLE value, including the supported non-finite spellings.
fn parse_float64(column: &str, value: &JsonValue) -> Result<KeyValue, GatewayError> {
    parse_float(column, value, "DOUBLE").map(KeyValue::Double)
}

/// Parses a JSON string without coercing other scalar kinds.
fn parse_string(column: &str, value: &JsonValue) -> Result<KeyValue, GatewayError> {
    value
        .as_str()
        .map(|text| KeyValue::String(text.to_string()))
        .ok_or_else(|| type_error(column, "STRING (a JSON string)", value))
}

/// Parses a base64 string, checking the exact length for BINARY(n).
fn parse_binary(
    column: &str,
    value: &JsonValue,
    fixed_length: Option<i32>,
) -> Result<KeyValue, GatewayError> {
    let expected = "BINARY (a base64 string)";
    let text = value
        .as_str()
        .ok_or_else(|| type_error(column, expected, value))?;
    let bytes = BASE64.decode(text).map_err(|_| {
        GatewayError::invalid_argument(format!(
            "column `{column}` expects {expected}, the string is not valid base64"
        ))
    })?;
    if let Some(length) = fixed_length {
        if bytes.len() != length as usize {
            return Err(GatewayError::invalid_argument(format!(
                "column `{column}` expects BINARY({length}), got {} bytes",
                bytes.len()
            )));
        }
    }
    Ok(KeyValue::Bytes(bytes))
}

/// Parses a DECIMAL from a base-10 string or an exact-integer number, rejecting values that do not fit the declared
/// precision and scale.
fn parse_decimal(
    column: &str,
    value: &JsonValue,
    precision: u8,
    scale: i8,
) -> Result<KeyValue, GatewayError> {
    let expected = format!("DECIMAL({precision}, {scale}) (a base-10 string or an exact integer)");
    let text = match value {
        JsonValue::String(text) => text.clone(),
        JsonValue::Number(number) => number
            .as_i64()
            .map(|v| v.to_string())
            .ok_or_else(|| type_error(column, &expected, value))?,
        _ => return Err(type_error(column, &expected, value)),
    };
    let unscaled = decimal_to_unscaled(&text, precision, scale).map_err(|reason| {
        GatewayError::invalid_argument(format!("column `{column}` expects {expected}: {reason}"))
    })?;
    Ok(KeyValue::Decimal {
        unscaled,
        precision,
        scale,
    })
}

/// Converts decimal text to the unscaled integer for the declared precision and scale, without rounding.
fn decimal_to_unscaled(text: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err("negative scales are not supported".to_string());
    }
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (unsigned, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err("not a decimal number".to_string());
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err("not a plain base-10 decimal (exponents are not accepted)".to_string());
    }
    let scale = scale as usize;
    let significant_frac = frac_part.trim_end_matches('0');
    if significant_frac.len() > scale {
        return Err(format!(
            "value has {} fractional digits but the scale is {scale}",
            significant_frac.len()
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(frac_part.get(..scale.min(frac_part.len())).unwrap_or(""));
    for _ in frac_part.len()..scale {
        digits.push('0');
    }
    let unscaled: i128 = digits
        .parse()
        .map_err(|_| "value does not fit a 128-bit decimal".to_string())?;
    let trimmed = digits.trim_start_matches('0');
    let significant = if trimmed.is_empty() { 1 } else { trimmed.len() };
    if significant > precision as usize {
        return Err(format!(
            "value needs {significant} digits of precision but the type allows {precision}"
        ));
    }
    Ok(if negative { -unscaled } else { unscaled })
}

/// Parses a DATE string of the form `YYYY-MM-DD`.
fn parse_date(column: &str, value: &JsonValue) -> Result<KeyValue, GatewayError> {
    let expected = "DATE (an ISO-8601 string like \"2026-01-31\")";
    let text = value
        .as_str()
        .ok_or_else(|| type_error(column, expected, value))?;
    let days = parse_date_text(text).ok_or_else(|| type_error(column, expected, value))?;
    Ok(KeyValue::Date {
        days_since_epoch: days as i32,
    })
}

/// Parses a TIME string, rejecting fractions finer than the declared precision. Values below one millisecond are
/// rejected because the native key encoding stores milliseconds of the day.
fn parse_time(column: &str, value: &JsonValue, unit: TimeUnit) -> Result<KeyValue, GatewayError> {
    let expected = "TIME (an ISO-8601 string like \"12:34:56.789\")";
    let text = value
        .as_str()
        .ok_or_else(|| type_error(column, expected, value))?;
    let (seconds_of_day, frac_nanos) =
        parse_time_text(text).ok_or_else(|| type_error(column, expected, value))?;
    check_fraction_granularity(column, frac_nanos, unit)?;
    if frac_nanos % NANOS_PER_MILLI != 0 {
        return Err(GatewayError::invalid_argument(format!(
            "column `{column}` cannot use sub-millisecond TIME key values, \
             the native key encoding stores milliseconds"
        )));
    }
    Ok(KeyValue::Time {
        millis_of_day: (seconds_of_day * 1_000 + frac_nanos / NANOS_PER_MILLI) as i32,
    })
}

/// Parses a TIMESTAMP or TIMESTAMP_LTZ string. A zone free type rejects any zone suffix and a zoned type requires `Z`
/// or a numeric offset.
fn parse_timestamp(
    column: &str,
    value: &JsonValue,
    unit: TimeUnit,
    with_zone: bool,
) -> Result<KeyValue, GatewayError> {
    let expected = if with_zone {
        "TIMESTAMP_LTZ (an ISO-8601 string with a zone, like \"2026-01-31T12:34:56.789Z\")"
    } else {
        "TIMESTAMP (a zone-free ISO-8601 string like \"2026-01-31T12:34:56.789\")"
    };
    let text = value
        .as_str()
        .ok_or_else(|| type_error(column, expected, value))?;
    let parsed = parse_timestamp_text(text).ok_or_else(|| type_error(column, expected, value))?;
    if with_zone && parsed.offset_seconds.is_none() {
        return Err(GatewayError::invalid_argument(format!(
            "column `{column}` expects {expected}, the value has no zone"
        )));
    }
    if !with_zone && parsed.offset_seconds.is_some() {
        return Err(GatewayError::invalid_argument(format!(
            "column `{column}` expects {expected}, the value carries a zone"
        )));
    }
    check_fraction_granularity(column, parsed.frac_nanos, unit)?;
    let local_millis = parsed
        .days
        .checked_mul(86_400_000)
        .and_then(|day_millis| {
            day_millis
                .checked_add(parsed.seconds_of_day * 1_000 + parsed.frac_nanos / NANOS_PER_MILLI)
        })
        .ok_or_else(|| type_error(column, expected, value))?;
    let nanos_of_milli = (parsed.frac_nanos % NANOS_PER_MILLI) as u32;
    if with_zone {
        let offset_millis = i64::from(parsed.offset_seconds.unwrap_or(0)) * 1_000;
        Ok(KeyValue::TimestampLtz {
            epoch_millis: local_millis - offset_millis,
            nanos_of_milli,
        })
    } else {
        Ok(KeyValue::TimestampNtz {
            millis: local_millis,
            nanos_of_milli,
        })
    }
}

/// Rejects fractional digits finer than the declared column precision.
fn check_fraction_granularity(
    column: &str,
    frac_nanos: i64,
    unit: TimeUnit,
) -> Result<(), GatewayError> {
    let granularity = match unit {
        TimeUnit::Second => NANOS_PER_SECOND,
        TimeUnit::Millisecond => NANOS_PER_MILLI,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    if frac_nanos % granularity != 0 {
        return Err(GatewayError::invalid_argument(format!(
            "column `{column}` has a coarser precision than the supplied fractional seconds"
        )));
    }
    Ok(())
}

/// A parsed ISO-8601 timestamp before zone normalization.
struct ParsedTimestamp {
    days: i64,
    seconds_of_day: i64,
    frac_nanos: i64,
    offset_seconds: Option<i32>,
}

/// Parses `YYYY-MM-DD` into days since the Unix epoch, or `None` when the text is not a valid calendar date.
fn parse_date_text(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year: i32 = digits_only(text.get(0..4)?)?.parse().ok()?;
    let month: u32 = digits_only(text.get(5..7)?)?.parse().ok()?;
    let day: u32 = digits_only(text.get(8..10)?)?.parse().ok()?;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    Some(date.signed_duration_since(unix_epoch_date()).num_days())
}

/// Parses `HH:MM`, `HH:MM:SS` or `HH:MM:SS.f{1..9}` into seconds of the day plus fractional nanoseconds.
fn parse_time_text(text: &str) -> Option<(i64, i64)> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let bytes = clock.as_bytes();
    let (hour, minute, second) = match bytes.len() {
        5 if bytes[2] == b':' => (clock.get(0..2)?, clock.get(3..5)?, "0"),
        8 if bytes[2] == b':' && bytes[5] == b':' => {
            (clock.get(0..2)?, clock.get(3..5)?, clock.get(6..8)?)
        }
        _ => return None,
    };
    let hour: i64 = digits_only(hour)?.parse().ok()?;
    let minute: i64 = digits_only(minute)?.parse().ok()?;
    let second: i64 = digits_only(second)?.parse().ok()?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let frac_nanos = match fraction {
        None => 0,
        Some(fraction) => {
            if fraction.is_empty() || fraction.len() > 9 {
                return None;
            }
            let digits: i64 = digits_only(fraction)?.parse().ok()?;
            digits * 10_i64.pow(9 - fraction.len() as u32)
        }
    };
    Some((hour * 3_600 + minute * 60 + second, frac_nanos))
}

/// Parses a full ISO-8601 timestamp with an optional `Z` or numeric offset suffix. The date and time may be separated
/// by `T` or a space.
fn parse_timestamp_text(text: &str) -> Option<ParsedTimestamp> {
    let (date_part, rest) = text.split_at_checked(10)?;
    let days = parse_date_text(date_part)?;
    let rest = rest.strip_prefix(['T', ' '])?;
    let (time_part, offset_seconds) = split_zone(rest)?;
    let (seconds_of_day, frac_nanos) = parse_time_text(time_part)?;
    Some(ParsedTimestamp {
        days,
        seconds_of_day,
        frac_nanos,
        offset_seconds,
    })
}

/// Splits a trailing zone designator off a time string. Returns the remaining time text and the offset in seconds when
/// a zone is present.
fn split_zone(text: &str) -> Option<(&str, Option<i32>)> {
    if let Some(time_part) = text.strip_suffix('Z') {
        return Some((time_part, Some(0)));
    }
    let Some(position) = text.rfind(['+', '-']) else {
        return Some((text, None));
    };
    let (time_part, zone) = text.split_at(position);
    let sign = if zone.starts_with('-') { -1 } else { 1 };
    let zone = &zone[1..];
    let (hours, minutes) = zone.split_once(':')?;
    if hours.len() != 2 || minutes.len() != 2 {
        return None;
    }
    let hours: i32 = digits_only(hours)?.parse().ok()?;
    let minutes: i32 = digits_only(minutes)?.parse().ok()?;
    if hours > 18 || minutes > 59 || (hours == 18 && minutes != 0) {
        return None;
    }
    Some((time_part, Some(sign * (hours * 3_600 + minutes * 60))))
}

/// Returns the input only when every character is an ASCII digit.
fn digits_only(text: &str) -> Option<&str> {
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        Some(text)
    } else {
        None
    }
}

/// The proleptic Gregorian calendar repeats every 400 years, which is exactly this many days.
const DAYS_PER_GREGORIAN_CYCLE: i64 = 146_097;

/// 1970-01-01, the origin every day count in this module is relative to.
fn unix_epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date")
}

/// Formats days since the Unix epoch as `YYYY-MM-DD`.
///
/// Timestamp columns can hold day counts far outside the range `NaiveDate` covers, so the count is first folded into a
/// single 400-year cycle and the whole cycles are added back onto the year.
pub(super) fn format_date(days: i64) -> String {
    let cycles = days.div_euclid(DAYS_PER_GREGORIAN_CYCLE);
    let offset = days.rem_euclid(DAYS_PER_GREGORIAN_CYCLE);
    let date = unix_epoch_date() + Duration::days(offset);
    let year = i64::from(date.year()) + cycles * 400;
    let (month, day) = (date.month(), date.day());
    if year < 0 {
        format!("-{:04}-{month:02}-{day:02}", -year)
    } else {
        format!("{year:04}-{month:02}-{day:02}")
    }
}

/// Formats seconds of the day as `HH:MM:SS`.
pub(super) fn format_time_of_day(seconds_of_day: i64) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        seconds_of_day / 3_600,
        seconds_of_day % 3_600 / 60,
        seconds_of_day % 60
    )
}

/// Formats fractional nanoseconds with a fixed digit count, or nothing for zero digits.
fn format_fraction(frac_nanos: i64, digits: u32) -> String {
    if digits == 0 {
        return String::new();
    }
    let scaled = frac_nanos / 10_i64.pow(9 - digits);
    format!(".{scaled:0width$}", width = digits as usize)
}

/// Downcasts a dynamic Arrow array to its concrete type.
fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, GatewayError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        GatewayError::internal(format!(
            "Arrow array does not match its declared type {}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Int32Builder, ListBuilder, MapBuilder, StringBuilder, StructArray,
    };
    use arrow::datatypes::{Field, Fields};
    use serde_json::json;
    use std::sync::Arc;

    /// Renders one non-null test array value, failing immediately on conversion errors.
    fn as_json(array: &dyn Array, index: usize) -> JsonValue {
        value_to_json(array, index).expect("conversion succeeds")
    }

    #[test]
    fn bigint_renders_as_string_at_the_limits() {
        let array = Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN), None]);
        assert_eq!(as_json(&array, 0), json!("9223372036854775807"));
        assert_eq!(as_json(&array, 1), json!("-9223372036854775808"));
        assert_eq!(as_json(&array, 2), JsonValue::Null);
    }

    #[test]
    fn small_ints_and_bool_render_as_json_scalars() {
        assert_eq!(as_json(&Int8Array::from(vec![-8]), 0), json!(-8));
        assert_eq!(as_json(&Int16Array::from(vec![300]), 0), json!(300));
        assert_eq!(as_json(&Int32Array::from(vec![7]), 0), json!(7));
        assert_eq!(as_json(&BooleanArray::from(vec![true]), 0), json!(true));
        assert_eq!(as_json(&StringArray::from(vec!["Ada"]), 0), json!("Ada"));
    }

    #[test]
    fn decimal_renders_as_string_at_precision_limit() {
        let array = Decimal128Array::from(vec![
            Some(99_999_999_999_999_999_999_999_999_999_999_999_999_i128),
            Some(-12_345_i128),
            None,
        ])
        .with_precision_and_scale(38, 2)
        .unwrap();
        assert_eq!(
            as_json(&array, 0),
            json!("999999999999999999999999999999999999.99")
        );
        assert_eq!(as_json(&array, 1), json!("-123.45"));
        assert_eq!(as_json(&array, 2), JsonValue::Null);
    }

    #[test]
    fn non_finite_floats_render_as_strings() {
        let array = Float64Array::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5]);
        assert_eq!(as_json(&array, 0), json!("NaN"));
        assert_eq!(as_json(&array, 1), json!("Infinity"));
        assert_eq!(as_json(&array, 2), json!("-Infinity"));
        assert_eq!(as_json(&array, 3), json!(1.5));
        let floats = Float32Array::from(vec![f32::NAN, 2.5]);
        assert_eq!(as_json(&floats, 0), json!("NaN"));
        assert_eq!(as_json(&floats, 1), json!(2.5));
    }

    #[test]
    fn binary_renders_as_base64() {
        let array = BinaryArray::from(vec![&[0u8, 1, 254, 255][..]]);
        assert_eq!(as_json(&array, 0), json!("AAH+/w=="));
        let fixed = FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2]].into_iter()).unwrap();
        assert_eq!(as_json(&fixed, 0), json!("AQI="));
    }

    #[test]
    fn date_renders_iso() {
        let array = Date32Array::from(vec![0, 20_484, -1]);
        assert_eq!(as_json(&array, 0), json!("1970-01-01"));
        assert_eq!(as_json(&array, 1), json!("2026-01-31"));
        assert_eq!(as_json(&array, 2), json!("1969-12-31"));
    }

    #[test]
    fn time_renders_each_precision() {
        let seconds = Time32SecondArray::from(vec![45_296]);
        assert_eq!(as_json(&seconds, 0), json!("12:34:56"));
        let millis = Time32MillisecondArray::from(vec![45_296_789]);
        assert_eq!(as_json(&millis, 0), json!("12:34:56.789"));
        let micros = Time64MicrosecondArray::from(vec![45_296_789_123]);
        assert_eq!(as_json(&micros, 0), json!("12:34:56.789123"));
        let nanos = Time64NanosecondArray::from(vec![45_296_789_123_456]);
        assert_eq!(as_json(&nanos, 0), json!("12:34:56.789123456"));
    }

    #[test]
    fn timestamps_render_each_precision_and_zone() {
        let epoch_millis = 1_769_862_896_789_i64;
        let ntz = TimestampMillisecondArray::from(vec![epoch_millis]);
        assert_eq!(as_json(&ntz, 0), json!("2026-01-31T12:34:56.789"));
        let ltz = TimestampMillisecondArray::from(vec![epoch_millis]).with_timezone("UTC");
        assert_eq!(as_json(&ltz, 0), json!("2026-01-31T12:34:56.789Z"));
        let seconds = TimestampSecondArray::from(vec![epoch_millis / 1_000]);
        assert_eq!(as_json(&seconds, 0), json!("2026-01-31T12:34:56"));
        let micros =
            TimestampMicrosecondArray::from(vec![epoch_millis * 1_000 + 123]).with_timezone("UTC");
        assert_eq!(as_json(&micros, 0), json!("2026-01-31T12:34:56.789123Z"));
        let nanos = TimestampNanosecondArray::from(vec![epoch_millis * 1_000_000 + 123_456]);
        assert_eq!(as_json(&nanos, 0), json!("2026-01-31T12:34:56.789123456"));
    }

    #[test]
    fn timestamps_before_the_epoch_render_correctly() {
        let array = TimestampMillisecondArray::from(vec![-1]).with_timezone("UTC");
        assert_eq!(as_json(&array, 0), json!("1969-12-31T23:59:59.999Z"));
    }

    #[test]
    fn timestamp_numeric_boundaries_never_overflow() {
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            assert!(timestamp_to_json(&TimestampSecondArray::from(vec![value]), 0).is_ok());
            assert!(timestamp_to_json(&TimestampMillisecondArray::from(vec![value]), 0).is_ok());
            assert!(timestamp_to_json(&TimestampMicrosecondArray::from(vec![value]), 0).is_ok());
            assert!(timestamp_to_json(&TimestampNanosecondArray::from(vec![value]), 0).is_ok());
        }
        assert_eq!(
            timestamp_to_json(&TimestampMicrosecondArray::from(vec![-1]), 0).unwrap(),
            json!("1969-12-31T23:59:59.999999")
        );
        assert_eq!(
            timestamp_to_json(&TimestampNanosecondArray::from(vec![-1]), 0).unwrap(),
            json!("1969-12-31T23:59:59.999999999")
        );
    }

    #[test]
    fn timezone_offset_boundary_is_strict() {
        assert_eq!(split_zone("12:00:00+18:00").unwrap().1, Some(18 * 3_600));
        assert_eq!(split_zone("12:00:00-18:00").unwrap().1, Some(-18 * 3_600));
        for invalid in [
            "12:00:00+18:01",
            "12:00:00-18:59",
            "12:00:00+19:00",
            "12:00:00+1:00",
        ] {
            assert!(split_zone(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn nested_list_struct_and_map_recurse() {
        let mut list = ListBuilder::new(Int32Builder::new());
        list.values().append_value(1);
        list.values().append_null();
        list.values().append_value(3);
        list.append(true);
        let list = list.finish();
        assert_eq!(as_json(&list, 0), json!([1, null, 3]));

        let strings: ArrayRef = Arc::new(StringArray::from(vec!["Ada"]));
        let numbers: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
        let fields = Fields::from(vec![
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("id", ArrowDataType::Int64, true),
        ]);
        let row = StructArray::new(fields, vec![strings, numbers], None);
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
        let map = map.finish();
        assert_eq!(
            as_json(&map, 0),
            json!([{"key": "b", "value": 2}, {"key": "a", "value": 1}])
        );
    }

    #[test]
    fn record_batch_rows_keyed_by_column_name() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Ada"), None])),
            ],
        )
        .unwrap();
        let rows = record_batch_to_json_rows(&batch).unwrap();
        assert_eq!(
            serde_json::to_value(&rows).unwrap(),
            json!([{"id": 1, "name": "Ada"}, {"id": 2, "name": null}])
        );
    }

    /// Parses one test value against a synthetic key column.
    fn parse(data_type: ArrowDataType, value: JsonValue) -> Result<KeyValue, GatewayError> {
        parse_key_value("k", &data_type, &value)
    }

    #[test]
    fn parse_small_int_rejects_floats_and_overflow() {
        assert_eq!(
            parse(ArrowDataType::Int32, json!(42)).unwrap(),
            KeyValue::Int(42)
        );
        let err = parse(ArrowDataType::Int32, json!(1.5)).unwrap_err();
        assert!(err.message().contains("`k`"), "{err}");
        assert!(err.message().contains("INT"), "{err}");
        assert!(parse(ArrowDataType::Int32, json!(i64::MAX)).is_err());
        assert!(parse(ArrowDataType::Int8, json!(128)).is_err());
        assert!(
            parse(ArrowDataType::Int32, json!("42")).is_err(),
            "no strings for INT"
        );
    }

    #[test]
    fn parse_bigint_accepts_number_and_string() {
        assert_eq!(
            parse(ArrowDataType::Int64, json!("9223372036854775807")).unwrap(),
            KeyValue::BigInt(i64::MAX)
        );
        assert_eq!(
            parse(ArrowDataType::Int64, json!(-9223372036854775808_i64)).unwrap(),
            KeyValue::BigInt(i64::MIN)
        );
        assert!(
            parse(ArrowDataType::Int64, json!(1.0)).is_err(),
            "floats rejected"
        );
        assert!(parse(ArrowDataType::Int64, json!("1.0")).is_err());
    }

    #[test]
    fn parse_decimal_scales_and_validates() {
        let parsed = parse(ArrowDataType::Decimal128(10, 2), json!("123.45")).unwrap();
        assert_eq!(
            parsed,
            KeyValue::Decimal {
                unscaled: 12_345,
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            parse(ArrowDataType::Decimal128(10, 2), json!("-1")).unwrap(),
            KeyValue::Decimal {
                unscaled: -100,
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            parse(ArrowDataType::Decimal128(10, 2), json!(7)).unwrap(),
            KeyValue::Decimal {
                unscaled: 700,
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            parse(ArrowDataType::Decimal128(10, 2), json!("1.450")).unwrap(),
            KeyValue::Decimal {
                unscaled: 145,
                precision: 10,
                scale: 2
            },
            "trailing zeros beyond the scale are lossless"
        );
        let max = "9".repeat(38);
        assert_eq!(
            parse(ArrowDataType::Decimal128(38, 0), json!(max)).unwrap(),
            KeyValue::Decimal {
                unscaled: 99_999_999_999_999_999_999_999_999_999_999_999_999,
                precision: 38,
                scale: 0
            }
        );
        assert!(
            parse(ArrowDataType::Decimal128(10, 2), json!("1.234")).is_err(),
            "lossy"
        );
        assert!(
            parse(ArrowDataType::Decimal128(4, 2), json!("123.45")).is_err(),
            "precision"
        );
        assert!(
            parse(ArrowDataType::Decimal128(10, 2), json!("1e3")).is_err(),
            "exponent"
        );
        assert!(
            parse(ArrowDataType::Decimal128(10, 2), json!(1.5)).is_err(),
            "float number"
        );
    }

    #[test]
    fn parse_float_accepts_non_finite_spellings() {
        assert_eq!(
            parse(ArrowDataType::Float64, json!(1.5)).unwrap(),
            KeyValue::Double(1.5)
        );
        let KeyValue::Double(nan) = parse(ArrowDataType::Float64, json!("NaN")).unwrap() else {
            panic!("expected a double");
        };
        assert!(nan.is_nan());
        assert_eq!(
            parse(ArrowDataType::Float32, json!("-Infinity")).unwrap(),
            KeyValue::Float(f32::NEG_INFINITY)
        );
        assert!(parse(ArrowDataType::Float64, json!("fast")).is_err());
        assert!(
            parse(ArrowDataType::Float32, json!(1e300)).is_err(),
            "out of f32 range"
        );
    }

    #[test]
    fn parse_binary_decodes_base64_and_checks_fixed_length() {
        assert_eq!(
            parse(ArrowDataType::Binary, json!("AAH+/w==")).unwrap(),
            KeyValue::Bytes(vec![0, 1, 254, 255])
        );
        assert_eq!(
            parse(ArrowDataType::FixedSizeBinary(2), json!("AQI=")).unwrap(),
            KeyValue::Bytes(vec![1, 2])
        );
        assert!(
            parse(ArrowDataType::FixedSizeBinary(3), json!("AQI=")).is_err(),
            "length"
        );
        assert!(parse(ArrowDataType::Binary, json!("not base64!")).is_err());
    }

    #[test]
    fn parse_date_validates_the_calendar() {
        assert_eq!(
            parse(ArrowDataType::Date32, json!("2026-01-31")).unwrap(),
            KeyValue::Date {
                days_since_epoch: 20_484
            }
        );
        assert_eq!(
            parse(ArrowDataType::Date32, json!("1969-12-31")).unwrap(),
            KeyValue::Date {
                days_since_epoch: -1
            }
        );
        assert!(parse(ArrowDataType::Date32, json!("2026-02-30")).is_err());
        assert!(parse(ArrowDataType::Date32, json!("2026-1-31")).is_err());
        assert!(
            parse(ArrowDataType::Date32, json!(20_484)).is_err(),
            "numbers rejected"
        );
    }

    #[test]
    fn parse_time_honors_precision_and_the_millisecond_floor() {
        assert_eq!(
            parse(
                ArrowDataType::Time32(TimeUnit::Millisecond),
                json!("12:34:56.789")
            )
            .unwrap(),
            KeyValue::Time {
                millis_of_day: 45_296_789
            }
        );
        assert_eq!(
            parse(ArrowDataType::Time32(TimeUnit::Second), json!("12:34")).unwrap(),
            KeyValue::Time {
                millis_of_day: 45_240_000
            }
        );
        assert!(
            parse(ArrowDataType::Time32(TimeUnit::Second), json!("12:34:56.5")).is_err(),
            "finer than the declared precision"
        );
        assert!(
            parse(
                ArrowDataType::Time64(TimeUnit::Microsecond),
                json!("12:34:56.789123")
            )
            .is_err(),
            "sub-millisecond time keys are not encodable"
        );
        assert!(
            parse(
                ArrowDataType::Time32(TimeUnit::Millisecond),
                json!("25:00:00")
            )
            .is_err()
        );
    }

    #[test]
    fn parse_timestamp_ntz_rejects_zones() {
        assert_eq!(
            parse(
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                json!("2026-01-31T12:34:56.789")
            )
            .unwrap(),
            KeyValue::TimestampNtz {
                millis: 1_769_862_896_789,
                nanos_of_milli: 0
            }
        );
        assert!(
            parse(
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                json!("2026-01-31T12:34:56.789Z")
            )
            .is_err()
        );
    }

    #[test]
    fn parse_timestamp_rejects_multibyte_text_without_panicking() {
        for text in ["123456789é1:11:11Z", "é", "2026-01-31é12:34:56"] {
            assert!(
                parse(
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                    json!(text)
                )
                .is_err(),
                "{text:?} must fail cleanly"
            );
        }
    }

    #[test]
    fn parse_timestamp_ltz_requires_and_applies_zones() {
        let utc = Some(Arc::from("UTC"));
        assert_eq!(
            parse(
                ArrowDataType::Timestamp(TimeUnit::Millisecond, utc.clone()),
                json!("2026-01-31T12:34:56.789Z")
            )
            .unwrap(),
            KeyValue::TimestampLtz {
                epoch_millis: 1_769_862_896_789,
                nanos_of_milli: 0
            }
        );
        assert_eq!(
            parse(
                ArrowDataType::Timestamp(TimeUnit::Millisecond, utc.clone()),
                json!("2026-01-31T14:34:56.789+02:00")
            )
            .unwrap(),
            KeyValue::TimestampLtz {
                epoch_millis: 1_769_862_896_789,
                nanos_of_milli: 0
            }
        );
        assert_eq!(
            parse(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, utc.clone()),
                json!("2026-01-31T12:34:56.789123456Z")
            )
            .unwrap(),
            KeyValue::TimestampLtz {
                epoch_millis: 1_769_862_896_789,
                nanos_of_milli: 123_456
            }
        );
        assert!(
            parse(
                ArrowDataType::Timestamp(TimeUnit::Millisecond, utc),
                json!("2026-01-31T12:34:56.789")
            )
            .is_err(),
            "a zone is required"
        );
    }

    #[test]
    fn parse_rejects_null_and_names_the_column() {
        let err =
            parse_key_value("customer_id", &ArrowDataType::Int64, &JsonValue::Null).unwrap_err();
        assert!(err.message().contains("`customer_id`"), "{err}");
        assert!(err.message().contains("null"), "{err}");
    }

    #[test]
    fn parse_rejects_nested_key_types() {
        let field = Arc::new(Field::new("item", ArrowDataType::Int32, true));
        let err = parse(ArrowDataType::List(field), json!([1])).unwrap_err();
        assert!(
            err.message().contains("cannot be used as a lookup key"),
            "{err}"
        );
    }

    #[test]
    fn civil_date_round_trip() {
        for days in [-719_468, -1, 0, 1, 20_484, 2_932_896] {
            assert_eq!(parse_date_text(&format_date(days)), Some(days), "{days}");
        }
    }
}

/// Round trips of the complete write-then-read path: JSON in, native row, Arrow, JSON out.
///
/// The two directions are written independently — the application layer decodes against the Fluss schema and
/// this module renders from the Arrow schema — so only an end-to-end round trip proves they agree.
#[cfg(test)]
mod round_trip_tests {
    use super::*;
    use crate::backend::types::{DataType as DomainType, RowField};
    use crate::protocol::rest::input::parse_input_value;
    use crate::protocol::rest::input_decode::{InputColumn, SchemaDecoder};
    use arrow::array::make_builder;
    use fluss::metadata::DataType as FlussDataType;
    use fluss::record::to_arrow_type;
    use serde_json::json;

    /// Decodes one JSON value against a column type and renders it back through Arrow.
    ///
    /// This is the exact path a written value takes on its way back out to a lookup response.
    fn round_trip(data_type: DomainType, json_text: &str) -> JsonValue {
        let input = parse_input_value(format!("{{\"v\": {json_text}}}").as_bytes())
            .expect("the fixture is valid JSON");
        let decoder = SchemaDecoder::new(vec![InputColumn::new("v", data_type.clone())])
            .expect("the fixture schema is valid");
        let row = decoder
            .decode_row("entry-1", &input)
            .expect("decoding succeeds");
        let native = FlussDataType::try_from(&data_type).expect("the native type converts");
        let arrow_type = to_arrow_type(&native).expect("the Arrow type converts");
        let mut builder = make_builder(&arrow_type, 1);
        row.as_native().values[0]
            .append_to(builder.as_mut(), &native, &arrow_type)
            .expect("the value appends to its Arrow builder");
        let array = builder.finish();
        assert_eq!(
            array.data_type(),
            &arrow_type,
            "the builder must keep the declared Arrow type"
        );
        value_to_json(array.as_ref(), 0).expect("rendering succeeds")
    }

    #[test]
    fn scalars_round_trip_unchanged() {
        assert_eq!(
            round_trip(DomainType::Boolean { nullable: false }, "true"),
            json!(true)
        );
        assert_eq!(
            round_trip(DomainType::TinyInt { nullable: false }, "-128"),
            json!(-128)
        );
        assert_eq!(
            round_trip(DomainType::SmallInt { nullable: false }, "32767"),
            json!(32_767)
        );
        assert_eq!(
            round_trip(DomainType::Int { nullable: false }, "-2147483648"),
            json!(i32::MIN)
        );
        assert_eq!(
            round_trip(DomainType::String { nullable: false }, "\"Ada\""),
            json!("Ada")
        );
        assert_eq!(
            round_trip(
                DomainType::Char {
                    nullable: false,
                    length: 3,
                },
                "\"Ada\""
            ),
            json!("Ada")
        );
        assert_eq!(
            round_trip(DomainType::Int { nullable: true }, "null"),
            JsonValue::Null
        );
    }

    #[test]
    fn bigint_round_trips_past_the_double_boundary() {
        // Both spellings must survive: the value is above 2^53, where a double would round it.
        for spelling in ["9007199254740993", "\"9007199254740993\""] {
            assert_eq!(
                round_trip(DomainType::BigInt { nullable: false }, spelling),
                json!("9007199254740993"),
                "{spelling}"
            );
        }
        assert_eq!(
            round_trip(
                DomainType::BigInt { nullable: false },
                "\"9223372036854775807\""
            ),
            json!("9223372036854775807")
        );
        assert_eq!(
            round_trip(
                DomainType::BigInt { nullable: false },
                "-9223372036854775808"
            ),
            json!("-9223372036854775808")
        );
    }

    #[test]
    fn decimals_round_trip_at_full_precision() {
        assert_eq!(
            round_trip(
                DomainType::Decimal {
                    nullable: false,
                    precision: 10,
                    scale: 2,
                },
                "\"123.45\""
            ),
            json!("123.45")
        );
        let max = "9".repeat(38);
        assert_eq!(
            round_trip(
                DomainType::Decimal {
                    nullable: false,
                    precision: 38,
                    scale: 0,
                },
                &format!("\"{max}\"")
            ),
            json!(max)
        );
        assert_eq!(
            round_trip(
                DomainType::Decimal {
                    nullable: false,
                    precision: 38,
                    scale: 18,
                },
                "9007199254740993.000000000000000001"
            ),
            json!("9007199254740993.000000000000000001"),
            "a number lexeme is decoded from its text, never through a double"
        );
    }

    #[test]
    fn floats_round_trip_including_the_non_finite_spellings() {
        assert_eq!(
            round_trip(DomainType::Double { nullable: false }, "1.5"),
            json!(1.5)
        );
        assert_eq!(
            round_trip(DomainType::Float { nullable: false }, "2.5"),
            json!(2.5)
        );
        for spelling in ["\"NaN\"", "\"Infinity\"", "\"-Infinity\""] {
            let expected: JsonValue = serde_json::from_str(spelling).unwrap();
            assert_eq!(
                round_trip(DomainType::Double { nullable: false }, spelling),
                expected,
                "{spelling}"
            );
            assert_eq!(
                round_trip(DomainType::Float { nullable: false }, spelling),
                expected,
                "{spelling}"
            );
        }
    }

    #[test]
    fn binary_round_trips_through_base64() {
        assert_eq!(
            round_trip(DomainType::Bytes { nullable: false }, "\"AAH+/w==\""),
            json!("AAH+/w==")
        );
        assert_eq!(
            round_trip(
                DomainType::Binary {
                    nullable: false,
                    length: 2,
                },
                "\"AQI=\""
            ),
            json!("AQI=")
        );
    }

    #[test]
    fn temporal_values_round_trip_at_their_declared_precision() {
        assert_eq!(
            round_trip(DomainType::Date { nullable: false }, "\"2026-01-31\""),
            json!("2026-01-31")
        );
        assert_eq!(
            round_trip(
                DomainType::Time {
                    nullable: false,
                    precision: 3,
                },
                "\"12:34:56.789\""
            ),
            json!("12:34:56.789")
        );
        assert_eq!(
            round_trip(
                DomainType::Timestamp {
                    nullable: false,
                    precision: 3,
                },
                "\"2026-01-31T12:34:56.789\""
            ),
            json!("2026-01-31T12:34:56.789"),
            "a zone-free column stays zone free"
        );
        assert_eq!(
            round_trip(
                DomainType::TimestampLtz {
                    nullable: false,
                    precision: 3,
                },
                "\"2026-01-31T14:34:56.789+02:00\""
            ),
            json!("2026-01-31T12:34:56.789Z"),
            "an offset is normalized to UTC and rendered with a Z"
        );
        assert_eq!(
            round_trip(
                DomainType::TimestampLtz {
                    nullable: false,
                    precision: 9,
                },
                "\"2026-01-31T12:34:56.789123456Z\""
            ),
            json!("2026-01-31T12:34:56.789123456Z")
        );
        assert_eq!(
            round_trip(
                DomainType::Timestamp {
                    nullable: false,
                    precision: 3,
                },
                "1769862896789"
            ),
            json!("2026-01-31T12:34:56.789"),
            "epoch milliseconds render as the same instant"
        );
    }

    #[test]
    fn containers_round_trip_with_nulls_inside() {
        assert_eq!(
            round_trip(
                DomainType::Array {
                    nullable: false,
                    element: Box::new(DomainType::Int { nullable: true }),
                },
                "[1, null, 3]"
            ),
            json!([1, null, 3])
        );
        assert_eq!(
            round_trip(
                DomainType::Array {
                    nullable: false,
                    element: Box::new(DomainType::BigInt { nullable: false }),
                },
                "[\"9007199254740993\"]"
            ),
            json!(["9007199254740993"])
        );
        assert_eq!(
            round_trip(
                DomainType::Map {
                    nullable: false,
                    key: Box::new(DomainType::String { nullable: false }),
                    value: Box::new(DomainType::Int { nullable: true }),
                },
                "[{\"key\": \"b\", \"value\": 2}, {\"key\": \"a\", \"value\": null}]"
            ),
            json!([{"key": "b", "value": 2}, {"key": "a", "value": null}]),
            "entry order survives the round trip"
        );
        let row_type = DomainType::Row {
            nullable: false,
            fields: vec![
                RowField {
                    name: "id".to_string(),
                    data_type: DomainType::BigInt { nullable: false },
                    description: None,
                    field_id: 1,
                },
                RowField {
                    name: "label".to_string(),
                    data_type: DomainType::String { nullable: true },
                    description: None,
                    field_id: 2,
                },
            ],
        };
        assert_eq!(
            round_trip(row_type, "{\"id\": 7, \"label\": null}"),
            json!({"id": "7", "label": null})
        );
    }
}
