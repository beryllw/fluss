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

//! Schema-aware conversion of protocol-neutral input values into native Fluss rows.
//!
//! Schema validation ([`validate_table_schema`], [`validate_data_type`]) serves DDL and the write path alike.
//! [`SchemaDecoder`] is the other half: it turns an [`InputValue`] into a native row, validating every value
//! against the column's declared [`DataType`] first. Nothing here is JSON aware; the protocol adapter has
//! already reduced the request to [`InputValue`], which retains number lexemes and ordered object entries so
//! that exactness and duplicate field names survive to this point.
//!
//! The accepted value mapping mirrors the rendering direction of the REST adapter:
//!
//! - BOOLEAN is a boolean, TINYINT through INT are exact integer numbers
//! - BIGINT is an exact integer number or a base-10 string; anything that would lose precision is rejected
//! - DECIMAL is a base-10 string or a number, always decoded from the literal text and never through `f64`
//! - FLOAT and DOUBLE are numbers, or the strings `"NaN"`, `"Infinity"` and `"-Infinity"`
//! - CHAR and STRING are strings. A CHAR(n) length is deliberately **not** enforced here, matching the native
//!   client, which does not check it either; BINARY(n) is length checked because its storage is fixed width
//! - BINARY and BYTES are base64 strings
//! - DATE and TIME are ISO-8601 strings, and both TIMESTAMP kinds accept an ISO-8601 string or epoch
//!   milliseconds. TIMESTAMP stays zone free and rejects any zone; TIMESTAMP_LTZ requires one and normalizes to
//!   UTC. Fractional seconds finer than the column's declared precision are rejected either way
//! - ARRAY and ROW recurse, MAP is an array of ordered `{key, value}` entries
//! - `null` is accepted only by a nullable type
//!
//! Every failure names the offending column, because a decode failure is a 400 raised before anything is
//! written: preflight is all-or-nothing, so the caller has to be able to find the value that stopped the batch.

use crate::backend::types::{DataType, RowField};
use crate::error::GatewayError;
use crate::protocol::rest::input_value::InputValue;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::NaiveDate;
use fluss::metadata::DataType as FlussDataType;
use fluss::row::{
    Date, Datum, Decimal, FlussArrayWriter, FlussMapWriter, GenericRow, Time, TimestampLtz,
    TimestampNtz,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;

const MAX_TYPE_NESTING: usize = 64;

/// Nanoseconds in one millisecond.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Milliseconds in one second.
const MILLIS_PER_SECOND: i64 = 1_000;

/// Milliseconds in one day.
const MILLIS_PER_DAY: i64 = 86_400_000;

/// One named column consumed by [`SchemaDecoder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputColumn {
    pub name: String,
    pub data_type: DataType,
}

impl InputColumn {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// One row decode failure plus whether refreshed table metadata could plausibly resolve it.
///
/// The staleness signal is set only by checks that compare the provided fields against the cached
/// table shape: unknown top-level columns, missing required columns, and required-column sets that
/// are inconsistent with the schema. Value-level failures such as range, format, and nullability
/// errors convert from [`GatewayError`] and never set it.
#[derive(Debug)]
pub struct RowDecodeError {
    schema_mismatch: bool,
    error: GatewayError,
}

impl RowDecodeError {
    /// Marks a failure that a forced metadata refresh plus one preflight retry may resolve.
    pub(crate) fn schema_mismatch(error: GatewayError) -> Self {
        Self {
            schema_mismatch: true,
            error,
        }
    }

    /// True when the failure refers to the cached table shape rather than the row data.
    pub fn is_schema_mismatch(&self) -> bool {
        self.schema_mismatch
    }

    /// The client-safe message of the underlying error.
    pub fn message(&self) -> &str {
        self.error.message()
    }

    /// Converts into the client-visible error without changing its envelope.
    pub fn into_gateway_error(self) -> GatewayError {
        self.error
    }
}

impl From<GatewayError> for RowDecodeError {
    fn from(error: GatewayError) -> Self {
        Self {
            schema_mismatch: false,
            error,
        }
    }
}

impl From<RowDecodeError> for GatewayError {
    fn from(error: RowDecodeError) -> Self {
        error.error
    }
}

/// One decoded row owned by the application boundary.
///
/// Protocol adapters cannot depend on the native representation. The native backend uses the
/// crate-private accessor only after complete application preflight has succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRow(GenericRow<'static>);

impl DecodedRow {
    /// Wraps a fully decoded native row. Only the decoder produces these.
    pub(crate) fn from_native(row: GenericRow<'static>) -> Self {
        Self(row)
    }

    pub fn field_count(&self) -> usize {
        self.0.values.len()
    }

    pub(crate) fn as_native(&self) -> &GenericRow<'static> {
        &self.0
    }
}

/// Columns validated by [`validate_table_schema`], consumed by [`SchemaDecoder`].
#[derive(Debug, Clone)]
pub struct ValidatedTableSchema {
    columns: Vec<InputColumn>,
    column_indexes: HashMap<String, usize>,
}

/// Validates schema shape plus primary-key and partition-key constraints enforced by Fluss.
pub fn validate_table_schema(
    columns: Vec<InputColumn>,
    primary_keys: Vec<String>,
    partition_keys: Vec<String>,
) -> Result<ValidatedTableSchema, GatewayError> {
    if columns.is_empty() {
        return Err(GatewayError::invalid_argument(
            "table schema must contain at least one column",
        ));
    }
    let mut column_indexes = HashMap::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        if column.name.is_empty() {
            return Err(GatewayError::invalid_argument(
                "column names must not be empty",
            ));
        }
        if column_indexes.insert(column.name.clone(), index).is_some() {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate column `{}`",
                column.name
            )));
        }
        validate_data_type(&column.data_type)?;
    }

    validate_key_names("primary key", &primary_keys, &column_indexes)?;
    validate_key_names("partition key", &partition_keys, &column_indexes)?;
    for key in &primary_keys {
        let data_type = &columns[column_indexes[key]].data_type;
        if data_type.nullable() {
            return Err(GatewayError::invalid_argument(format!(
                "primary-key column `{key}` must not be nullable"
            )));
        }
        if is_complex(data_type) {
            return Err(GatewayError::invalid_argument(format!(
                "primary-key column `{key}` has an unsupported complex type"
            )));
        }
    }
    for key in &partition_keys {
        let data_type = &columns[column_indexes[key]].data_type;
        if matches!(data_type, DataType::Decimal { .. }) || is_complex(data_type) {
            return Err(GatewayError::invalid_argument(format!(
                "partition-key column `{key}` has a type unsupported by Fluss"
            )));
        }
    }
    if !primary_keys.is_empty() {
        let primary_set: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();
        if let Some(key) = partition_keys
            .iter()
            .find(|key| !primary_set.contains(key.as_str()))
        {
            return Err(GatewayError::invalid_argument(format!(
                "partition-key column `{key}` must be part of the primary key"
            )));
        }
        if !partition_keys.is_empty() && partition_keys.len() == primary_keys.len() {
            return Err(GatewayError::invalid_argument(
                "a primary-key table must retain at least one non-partition primary-key column",
            ));
        }
    }

    Ok(ValidatedTableSchema {
        columns,
        column_indexes,
    })
}

/// Checks all recursive type invariants without relying on wire-specific deserialization.
pub fn validate_data_type(data_type: &DataType) -> Result<(), GatewayError> {
    validate_data_type_at_depth(data_type, 0)
}

fn validate_data_type_at_depth(data_type: &DataType, depth: usize) -> Result<(), GatewayError> {
    if depth > MAX_TYPE_NESTING {
        return Err(GatewayError::invalid_argument(format!(
            "data type nesting exceeds {MAX_TYPE_NESTING} levels"
        )));
    }
    match data_type {
        DataType::Char { length, .. } if *length == 0 => {
            return Err(GatewayError::invalid_argument(
                "character length must be at least one",
            ));
        }
        DataType::Binary { length, .. } if *length == 0 => {
            return Err(GatewayError::invalid_argument(
                "binary length must be at least one",
            ));
        }
        DataType::Array { element, .. } => {
            validate_data_type_at_depth(element, depth + 1)?;
        }
        DataType::Map { key, value, .. } => {
            if key.nullable() {
                return Err(GatewayError::invalid_argument(
                    "map key type must not be nullable",
                ));
            }
            validate_data_type_at_depth(key, depth + 1)?;
            validate_data_type_at_depth(value, depth + 1)?;
        }
        DataType::Row { fields, .. } => {
            let mut names = HashSet::with_capacity(fields.len());
            for field in fields {
                if field.name.is_empty() {
                    return Err(GatewayError::invalid_argument(
                        "row field names must not be empty",
                    ));
                }
                if !names.insert(field.name.as_str()) {
                    return Err(GatewayError::invalid_argument(format!(
                        "duplicate row field `{}`",
                        field.name
                    )));
                }
                validate_data_type_at_depth(&field.data_type, depth + 1)?;
            }
        }
        _ => {}
    }
    // The native constructors remain authoritative for decimal and temporal ranges.
    FlussDataType::try_from(data_type)?;
    Ok(())
}

fn validate_key_names(
    kind: &str,
    names: &[String],
    columns: &HashMap<String, usize>,
) -> Result<(), GatewayError> {
    let mut seen = HashSet::with_capacity(names.len());
    for name in names {
        if !seen.insert(name.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate {kind} column `{name}`"
            )));
        }
        if !columns.contains_key(name) {
            return Err(GatewayError::invalid_argument(format!(
                "{kind} column `{name}` does not exist"
            )));
        }
    }
    Ok(())
}

fn is_complex(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Array { .. } | DataType::Map { .. } | DataType::Row { .. }
    )
}

/// Reusable, schema-aware row decoder. It contains no REST or JSON types.
///
/// The columns must be the **complete** table schema in its declared order, never a subset such as the targeted
/// columns of a partial update. A decoded row is positional: it has one field per column of this decoder, in
/// this order, and a native writer indexes it by position. A partial update expresses its subset through
/// [`SchemaDecoder::decode_sparse_row`], which still produces a full-arity row with the untouched columns null.
#[derive(Debug, Clone)]
pub struct SchemaDecoder {
    columns: Vec<InputColumn>,
    /// Column name to position, used by sparse decoding to reject required columns that are not in the schema.
    column_indexes: HashMap<String, usize>,
    /// Native type per column, positionally aligned with `columns`.
    ///
    /// This is a performance-only cache of a pure conversion: nested container writers need the native child
    /// types, and converting per element would rebuild the same tree for every value of every row.
    native_types: Vec<FlussDataType>,
}

impl SchemaDecoder {
    pub fn new(columns: Vec<InputColumn>) -> Result<Self, GatewayError> {
        let validated = validate_table_schema(columns, Vec::new(), Vec::new())?;
        let native_types = validated
            .columns
            .iter()
            .map(|column| FlussDataType::try_from(&column.data_type))
            .collect::<Result<Vec<_>, GatewayError>>()?;
        Ok(Self {
            columns: validated.columns,
            column_indexes: validated.column_indexes,
            native_types,
        })
    }

    pub fn columns(&self) -> &[InputColumn] {
        &self.columns
    }

    /// Decodes a complete row in schema order. Missing nullable columns become null.
    ///
    /// Unknown and duplicate object fields are rejected before any native row is returned.
    pub fn decode_row(
        &self,
        entry_id: &str,
        value: &InputValue,
    ) -> Result<DecodedRow, RowDecodeError> {
        let entries = row_object(entry_id, value)?;
        self.check_column_names(entry_id, entries)?;
        let mut row = GenericRow::new(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let datum = match field(entries, &column.name) {
                Some(value) => self.decode_column(entry_id, index, value)?,
                None if column.data_type.nullable() => Datum::Null,
                None => {
                    return Err(RowDecodeError::schema_mismatch(
                        GatewayError::invalid_argument(format!(
                            "entry `{entry_id}`: column `{}` is required and was not provided",
                            column.name
                        )),
                    ));
                }
            };
            row.set_field(index, datum);
        }
        Ok(DecodedRow::from_native(row))
    }

    /// Decodes a sparse operation, such as delete, into full schema order.
    ///
    /// Required fields must be present and non-null. Missing non-required fields become null even
    /// when their table type is non-nullable because the operation does not submit those values.
    pub fn decode_sparse_row(
        &self,
        entry_id: &str,
        value: &InputValue,
        required_columns: &[String],
    ) -> Result<DecodedRow, RowDecodeError> {
        let entries = row_object(entry_id, value)?;
        self.check_column_names(entry_id, entries)?;
        for required in required_columns {
            if !self.column_indexes.contains_key(required) {
                return Err(RowDecodeError::schema_mismatch(
                    GatewayError::invalid_argument(format!(
                        "entry `{entry_id}`: required column `{required}` is not part of the table schema"
                    )),
                ));
            }
        }
        let mut row = GenericRow::new(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let required = required_columns.contains(&column.name);
            let datum = match field(entries, &column.name) {
                Some(InputValue::Null) if required => {
                    return Err(GatewayError::invalid_argument(format!(
                        "entry `{entry_id}`: column `{}` is required and must not be null",
                        column.name
                    ))
                    .into());
                }
                Some(value) => self.decode_column(entry_id, index, value)?,
                None if required => {
                    return Err(RowDecodeError::schema_mismatch(
                        GatewayError::invalid_argument(format!(
                            "entry `{entry_id}`: column `{}` is required and was not provided",
                            column.name
                        )),
                    ));
                }
                None => Datum::Null,
            };
            row.set_field(index, datum);
        }
        Ok(DecodedRow::from_native(row))
    }

    /// Decodes one provided value against the column at `index`.
    fn decode_column(
        &self,
        entry_id: &str,
        index: usize,
        value: &InputValue,
    ) -> Result<Datum<'static>, RowDecodeError> {
        let column = &self.columns[index];
        let path = ValuePath::column(entry_id, &column.name);
        let datum = decode_value(&path, &column.data_type, &self.native_types[index], value)?;
        Ok(datum)
    }

    /// Rejects unknown and duplicate top-level column names before any value is decoded.
    ///
    /// An unknown column is the one shape error a metadata refresh can plausibly resolve, because the cached
    /// schema may predate an added column. A duplicate name is a malformed request and never a staleness signal.
    fn check_column_names(
        &self,
        entry_id: &str,
        entries: &[(String, InputValue)],
    ) -> Result<(), RowDecodeError> {
        let mut seen = HashSet::with_capacity(entries.len());
        for (name, _) in entries {
            if !self.column_indexes.contains_key(name) {
                return Err(RowDecodeError::schema_mismatch(
                    GatewayError::invalid_argument(format!(
                        "entry `{entry_id}`: unknown column `{name}`"
                    )),
                ));
            }
            if !seen.insert(name.as_str()) {
                return Err(GatewayError::invalid_argument(format!(
                    "entry `{entry_id}`: duplicate column `{name}`"
                ))
                .into());
            }
        }
        Ok(())
    }
}

/// Returns the ordered entries of a row object, or an error naming the entry.
fn row_object<'v>(
    entry_id: &str,
    value: &'v InputValue,
) -> Result<&'v [(String, InputValue)], RowDecodeError> {
    value.object_entries().ok_or_else(|| {
        GatewayError::invalid_argument(format!(
            "entry `{entry_id}`: the row must be an object, got {}",
            input_kind(value)
        ))
        .into()
    })
}

/// Returns the first entry with the given name. Duplicates are rejected before this is called.
fn field<'v>(entries: &'v [(String, InputValue)], name: &str) -> Option<&'v InputValue> {
    entries
        .iter()
        .find_map(|(entry_name, value)| (entry_name == name).then_some(value))
}

/// Names the kind of an input value for error messages, without echoing row data.
fn input_kind(value: &InputValue) -> &'static str {
    match value {
        InputValue::Null => "null",
        InputValue::Boolean(_) => "a boolean",
        InputValue::ExactNumber(_) => "a number",
        InputValue::String(_) => "a string",
        InputValue::Array(_) => "an array",
        InputValue::Object(_) => "an object",
    }
}

/// The position of one value inside a row, used to name it in error messages.
///
/// The rendered path is the column name for a top-level value and a dotted, indexed path such as
/// `profile.tags[2].key` for anything nested inside a container.
struct ValuePath<'a> {
    entry_id: &'a str,
    path: String,
}

impl<'a> ValuePath<'a> {
    /// The path of one top-level column.
    fn column(entry_id: &'a str, column: &str) -> Self {
        Self {
            entry_id,
            path: column.to_string(),
        }
    }

    /// The path of the element at `index` of the current array.
    fn element(&self, index: usize) -> Self {
        self.nested(format!("{}[{index}]", self.path))
    }

    /// The path of the named field of the current row.
    fn nested_field(&self, name: &str) -> Self {
        self.nested(format!("{}.{name}", self.path))
    }

    /// The path of the key or value half of the map entry at `index`.
    fn map_part(&self, index: usize, part: &str) -> Self {
        self.nested(format!("{}[{index}].{part}", self.path))
    }

    fn nested(&self, path: String) -> Self {
        Self {
            entry_id: self.entry_id,
            path,
        }
    }

    /// Builds a validation error naming the entry and this path.
    fn error(&self, reason: impl fmt::Display) -> GatewayError {
        GatewayError::invalid_argument(format!(
            "entry `{}`: column `{}` {reason}",
            self.entry_id, self.path
        ))
    }

    /// Builds the standard type-mismatch error for this path.
    fn type_error(&self, expected: &str, value: &InputValue) -> GatewayError {
        self.error(format!("expects {expected}, got {}", input_kind(value)))
    }
}

/// Decodes one value against its declared type, recursing into containers.
///
/// `native` is the native mirror of `data_type`; container writers need it and it is never re-derived per
/// element. The dispatch is one flat match with the real work in small per-type helpers.
fn decode_value(
    path: &ValuePath<'_>,
    data_type: &DataType,
    native: &FlussDataType,
    value: &InputValue,
) -> Result<Datum<'static>, GatewayError> {
    if matches!(value, InputValue::Null) {
        return if data_type.nullable() {
            Ok(Datum::Null)
        } else {
            Err(path.error("must not be null"))
        };
    }
    match data_type {
        DataType::Boolean { .. } => match value {
            InputValue::Boolean(parsed) => Ok(Datum::Bool(*parsed)),
            _ => Err(path.type_error("BOOLEAN (a JSON boolean)", value)),
        },
        DataType::TinyInt { .. } => {
            decode_integer(path, value, "TINYINT", i8::MIN as i64, i8::MAX as i64)
                .map(|parsed| Datum::Int8(parsed as i8))
        }
        DataType::SmallInt { .. } => {
            decode_integer(path, value, "SMALLINT", i16::MIN as i64, i16::MAX as i64)
                .map(|parsed| Datum::Int16(parsed as i16))
        }
        DataType::Int { .. } => {
            decode_integer(path, value, "INT", i32::MIN as i64, i32::MAX as i64)
                .map(|parsed| Datum::Int32(parsed as i32))
        }
        DataType::BigInt { .. } => decode_bigint(path, value).map(Datum::Int64),
        DataType::Float { .. } => decode_float32(path, value),
        DataType::Double { .. } => decode_float(path, value, "DOUBLE").map(Datum::from),
        DataType::Char { .. } | DataType::String { .. } => match value {
            InputValue::String(text) => Ok(Datum::String(Cow::Owned(text.clone()))),
            _ => Err(path.type_error("STRING (a JSON string)", value)),
        },
        DataType::Decimal {
            precision, scale, ..
        } => decode_decimal(path, value, *precision, *scale),
        DataType::Bytes { .. } => decode_binary(path, value, None),
        DataType::Binary { length, .. } => decode_binary(path, value, Some(*length)),
        DataType::Date { .. } => decode_date(path, value),
        DataType::Time { precision, .. } => decode_time(path, value, *precision),
        DataType::Timestamp { precision, .. } => decode_timestamp(path, value, *precision, false),
        DataType::TimestampLtz { precision, .. } => decode_timestamp(path, value, *precision, true),
        DataType::Array { element, .. } => decode_array(path, element, native, value),
        DataType::Map { key, value: v, .. } => decode_map(path, key, v, native, value),
        DataType::Row { fields, .. } => decode_row_value(path, fields, native, value),
    }
}

/// Decodes an exact-integer number within the range of its type, rejecting fractions and exponents.
fn decode_integer(
    path: &ValuePath<'_>,
    value: &InputValue,
    type_name: &str,
    min: i64,
    max: i64,
) -> Result<i64, GatewayError> {
    let expected = format!("{type_name} (an integer in [{min}, {max}])");
    let InputValue::ExactNumber(lexeme) = value else {
        return Err(path.type_error(&expected, value));
    };
    let parsed = integer_lexeme(lexeme).ok_or_else(|| path.type_error(&expected, value))?;
    if parsed < min || parsed > max {
        return Err(path.error(format!("expects {expected}, value is out of range")));
    }
    Ok(parsed)
}

/// Decodes a BIGINT from an exact-integer number or a base-10 string.
///
/// The string spelling exists because BIGINT is rendered as a string, so a value that a JSON parser would round
/// through a double round-trips exactly.
fn decode_bigint(path: &ValuePath<'_>, value: &InputValue) -> Result<i64, GatewayError> {
    let expected = "BIGINT (an exact integer number or a base-10 string)";
    match value {
        InputValue::ExactNumber(lexeme) => {
            integer_lexeme(lexeme).ok_or_else(|| path.type_error(expected, value))
        }
        InputValue::String(text) => text
            .parse::<i64>()
            .map_err(|_| path.type_error(expected, value)),
        _ => Err(path.type_error(expected, value)),
    }
}

/// Parses a plain base-10 integer lexeme, rejecting fractions and exponents that could hide a lossy value.
fn integer_lexeme(lexeme: &str) -> Option<i64> {
    if lexeme.contains(['.', 'e', 'E']) {
        return None;
    }
    lexeme.parse::<i64>().ok()
}

/// Decodes a float from a number lexeme or one of the non-finite string spellings.
fn decode_float(
    path: &ValuePath<'_>,
    value: &InputValue,
    type_name: &str,
) -> Result<f64, GatewayError> {
    let expected = format!("{type_name} (a number or \"NaN\", \"Infinity\", \"-Infinity\")");
    match value {
        InputValue::ExactNumber(lexeme) => lexeme
            .parse::<f64>()
            .map_err(|_| path.type_error(&expected, value)),
        InputValue::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(path.type_error(&expected, value)),
        },
        _ => Err(path.type_error(&expected, value)),
    }
}

/// Decodes a FLOAT value and rejects finite inputs that overflow `f32`.
fn decode_float32(
    path: &ValuePath<'_>,
    value: &InputValue,
) -> Result<Datum<'static>, GatewayError> {
    let parsed = decode_float(path, value, "FLOAT")?;
    let narrowed = parsed as f32;
    if narrowed.is_infinite() && parsed.is_finite() {
        return Err(path.error("expects FLOAT, value is out of 32-bit float range"));
    }
    Ok(Datum::from(narrowed))
}

/// Decodes a DECIMAL from the literal text of a string or a number, never through binary floating point.
fn decode_decimal(
    path: &ValuePath<'_>,
    value: &InputValue,
    precision: u32,
    scale: u32,
) -> Result<Datum<'static>, GatewayError> {
    let expected = format!("DECIMAL({precision}, {scale}) (a base-10 string or a number)");
    let text = match value {
        InputValue::String(text) => text.as_str(),
        InputValue::ExactNumber(lexeme) => lexeme.as_str(),
        _ => return Err(path.type_error(&expected, value)),
    };
    let unscaled = decimal_to_unscaled(text, precision, scale)
        .map_err(|reason| path.error(format!("expects {expected}: {reason}")))?;
    let decimal = Decimal::from_unscaled_bytes(&unscaled.to_be_bytes(), precision, scale)
        .map_err(|error| path.error(format!("expects {expected}: {error}")))?;
    Ok(Datum::Decimal(decimal))
}

/// Converts decimal text to the unscaled integer for the declared precision and scale, without rounding.
fn decimal_to_unscaled(text: &str, precision: u32, scale: u32) -> Result<i128, String> {
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

/// Decodes a base64 string, checking the exact length for BINARY(n).
fn decode_binary(
    path: &ValuePath<'_>,
    value: &InputValue,
    fixed_length: Option<usize>,
) -> Result<Datum<'static>, GatewayError> {
    let expected = "BINARY (a base64 string)";
    let InputValue::String(text) = value else {
        return Err(path.type_error(expected, value));
    };
    let bytes = BASE64.decode(text).map_err(|error| {
        path.error(format!(
            "expects {expected}, the string is not valid base64: {error}"
        ))
    })?;
    if let Some(length) = fixed_length
        && bytes.len() != length
    {
        return Err(path.error(format!(
            "expects BINARY({length}), got {} bytes",
            bytes.len()
        )));
    }
    Ok(Datum::Blob(Cow::Owned(bytes)))
}

/// Decodes a DATE string of the form `YYYY-MM-DD`.
fn decode_date(path: &ValuePath<'_>, value: &InputValue) -> Result<Datum<'static>, GatewayError> {
    let expected = "DATE (an ISO-8601 string like \"2026-01-31\")";
    let InputValue::String(text) = value else {
        return Err(path.type_error(expected, value));
    };
    let days = parse_date_text(text).ok_or_else(|| path.type_error(expected, value))?;
    let days = i32::try_from(days)
        .map_err(|_| path.error(format!("expects {expected}, the date is out of range")))?;
    Ok(Datum::Date(Date::new(days)))
}

/// Decodes a TIME string, rejecting fractions finer than the declared precision.
///
/// Values below one millisecond are rejected because the native TIME representation stores milliseconds of the
/// day and would silently drop them.
fn decode_time(
    path: &ValuePath<'_>,
    value: &InputValue,
    precision: u32,
) -> Result<Datum<'static>, GatewayError> {
    let expected = "TIME (an ISO-8601 string like \"12:34:56.789\")";
    let InputValue::String(text) = value else {
        return Err(path.type_error(expected, value));
    };
    let (seconds_of_day, frac_nanos) =
        parse_time_text(text).ok_or_else(|| path.type_error(expected, value))?;
    check_fraction_granularity(path, frac_nanos, precision)?;
    if frac_nanos % NANOS_PER_MILLI != 0 {
        return Err(path.error(
            "cannot use sub-millisecond TIME values, the native representation stores milliseconds",
        ));
    }
    let millis_of_day = seconds_of_day * MILLIS_PER_SECOND + frac_nanos / NANOS_PER_MILLI;
    Ok(Datum::Time(Time::new(millis_of_day as i32)))
}

/// Decodes a TIMESTAMP or TIMESTAMP_LTZ from an ISO-8601 string or epoch milliseconds.
///
/// A zone-free column rejects any zone suffix and a zoned column requires `Z` or a numeric offset, which it
/// normalizes to UTC. Epoch milliseconds are subject to the same precision check as a string: a column that
/// declares fewer than three fractional digits rejects a value that carries them.
fn decode_timestamp(
    path: &ValuePath<'_>,
    value: &InputValue,
    precision: u32,
    with_zone: bool,
) -> Result<Datum<'static>, GatewayError> {
    let expected = if with_zone {
        "TIMESTAMP_LTZ (an ISO-8601 string with a zone like \"2026-01-31T12:34:56.789Z\", \
         or epoch milliseconds)"
    } else {
        "TIMESTAMP (a zone-free ISO-8601 string like \"2026-01-31T12:34:56.789\", \
         or epoch milliseconds)"
    };
    let (millis, nanos_of_milli) = match value {
        InputValue::ExactNumber(lexeme) => {
            let millis = integer_lexeme(lexeme).ok_or_else(|| path.type_error(expected, value))?;
            let frac_nanos = millis.rem_euclid(MILLIS_PER_SECOND) * NANOS_PER_MILLI;
            check_fraction_granularity(path, frac_nanos, precision)?;
            (millis, 0)
        }
        InputValue::String(text) => {
            let parsed =
                parse_timestamp_text(text).ok_or_else(|| path.type_error(expected, value))?;
            if with_zone && parsed.offset_seconds.is_none() {
                return Err(path.error(format!("expects {expected}, the value has no zone")));
            }
            if !with_zone && parsed.offset_seconds.is_some() {
                return Err(path.error(format!("expects {expected}, the value carries a zone")));
            }
            check_fraction_granularity(path, parsed.frac_nanos, precision)?;
            let local_millis = parsed
                .days
                .checked_mul(MILLIS_PER_DAY)
                .and_then(|day_millis| {
                    day_millis.checked_add(
                        parsed.seconds_of_day * MILLIS_PER_SECOND
                            + parsed.frac_nanos / NANOS_PER_MILLI,
                    )
                })
                .ok_or_else(|| {
                    path.error(format!("expects {expected}, the value is out of range"))
                })?;
            let offset_millis = i64::from(parsed.offset_seconds.unwrap_or(0)) * MILLIS_PER_SECOND;
            let millis = local_millis.checked_sub(offset_millis).ok_or_else(|| {
                path.error(format!("expects {expected}, the value is out of range"))
            })?;
            (millis, (parsed.frac_nanos % NANOS_PER_MILLI) as i32)
        }
        _ => return Err(path.type_error(expected, value)),
    };
    let datum = if with_zone {
        Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(millis, nanos_of_milli)
                .map_err(|error| path.error(format!("expects {expected}: {error}")))?,
        )
    } else {
        Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(millis, nanos_of_milli)
                .map_err(|error| path.error(format!("expects {expected}: {error}")))?,
        )
    };
    Ok(datum)
}

/// Rejects fractional seconds finer than the column's declared precision.
fn check_fraction_granularity(
    path: &ValuePath<'_>,
    frac_nanos: i64,
    precision: u32,
) -> Result<(), GatewayError> {
    let granularity = 10_i64.pow(9_u32.saturating_sub(precision.min(9)));
    if frac_nanos % granularity != 0 {
        return Err(path.error(format!(
            "declares precision {precision} but the value carries finer fractional seconds"
        )));
    }
    Ok(())
}

/// Decodes an ARRAY by recursing into every element and writing it to the native array writer.
fn decode_array(
    path: &ValuePath<'_>,
    element: &DataType,
    native: &FlussDataType,
    value: &InputValue,
) -> Result<Datum<'static>, GatewayError> {
    let InputValue::Array(values) = value else {
        return Err(path.type_error("ARRAY (a JSON array)", value));
    };
    let FlussDataType::Array(native_array) = native else {
        return Err(GatewayError::internal(
            "native type does not match the declared ARRAY type",
        ));
    };
    let native_element = native_array.get_element_type();
    let mut writer = FlussArrayWriter::new(values.len(), native_element);
    for (index, item) in values.iter().enumerate() {
        let element_path = path.element(index);
        let datum = decode_value(&element_path, element, native_element, item)?;
        write_element(&mut writer, index, datum, native_element)
            .map_err(|reason| element_path.error(reason))?;
    }
    let array = writer
        .complete()
        .map_err(|error| path.error(format!("could not be encoded as an ARRAY: {error}")))?;
    Ok(Datum::Array(array))
}

/// Decodes a MAP from an ordered array of `{key, value}` entries, matching the rendering direction.
fn decode_map(
    path: &ValuePath<'_>,
    key_type: &DataType,
    value_type: &DataType,
    native: &FlussDataType,
    value: &InputValue,
) -> Result<Datum<'static>, GatewayError> {
    let expected = "MAP (an array of {key, value} objects)";
    let InputValue::Array(entries) = value else {
        return Err(path.type_error(expected, value));
    };
    let FlussDataType::Map(native_map) = native else {
        return Err(GatewayError::internal(
            "native type does not match the declared MAP type",
        ));
    };
    let (native_key, native_value) = (native_map.key_type(), native_map.value_type());
    let mut writer = FlussMapWriter::new(entries.len(), native_key, native_value);
    for (index, entry) in entries.iter().enumerate() {
        let key_path = path.map_part(index, "key");
        let value_path = path.map_part(index, "value");
        let Some(fields) = entry.object_entries() else {
            return Err(path
                .element(index)
                .type_error("a map entry object with `key` and `value`", entry));
        };
        check_entry_fields(&path.element(index), fields, &["key", "value"])?;
        let key = fields
            .iter()
            .find_map(|(name, value)| (name == "key").then_some(value))
            .ok_or_else(|| key_path.error("is required in a map entry"))?;
        let entry_value = fields
            .iter()
            .find_map(|(name, value)| (name == "value").then_some(value))
            .ok_or_else(|| value_path.error("is required in a map entry"))?;
        let key = decode_value(&key_path, key_type, native_key, key)?;
        let entry_value = decode_value(&value_path, value_type, native_value, entry_value)?;
        writer
            .write_entry(key, entry_value)
            .map_err(|error| path.error(format!("could not be encoded as a MAP: {error}")))?;
    }
    let map = writer
        .complete()
        .map_err(|error| path.error(format!("could not be encoded as a MAP: {error}")))?;
    Ok(Datum::Map(map))
}

/// Decodes a nested ROW into a native row in declared field order.
fn decode_row_value(
    path: &ValuePath<'_>,
    fields: &[RowField],
    native: &FlussDataType,
    value: &InputValue,
) -> Result<Datum<'static>, GatewayError> {
    let Some(entries) = value.object_entries() else {
        return Err(path.type_error("ROW (a JSON object of its declared fields)", value));
    };
    let FlussDataType::Row(native_row) = native else {
        return Err(GatewayError::internal(
            "native type does not match the declared ROW type",
        ));
    };
    let names = fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    check_entry_fields(path, entries, &names)?;
    let mut row = GenericRow::new(fields.len());
    for (index, field_type) in fields.iter().enumerate() {
        let field_path = path.nested_field(&field_type.name);
        let native_field = native_row.fields()[index].data_type();
        let datum = match field(entries, &field_type.name) {
            Some(value) => decode_value(&field_path, &field_type.data_type, native_field, value)?,
            None if field_type.data_type.nullable() => Datum::Null,
            None => return Err(field_path.error("is required and was not provided")),
        };
        row.set_field(index, datum);
    }
    Ok(Datum::Row(Box::new(row)))
}

/// Rejects unknown and duplicate field names inside a nested object.
fn check_entry_fields(
    path: &ValuePath<'_>,
    entries: &[(String, InputValue)],
    known: &[&str],
) -> Result<(), GatewayError> {
    let mut seen = HashSet::with_capacity(entries.len());
    for (name, _) in entries {
        if !known.contains(&name.as_str()) {
            return Err(path.error(format!("has an unknown field `{name}`")));
        }
        if !seen.insert(name.as_str()) {
            return Err(path.error(format!("has a duplicate field `{name}`")));
        }
    }
    Ok(())
}

/// Writes one decoded element into a native array writer at `index`.
///
/// The datum was produced from `element_type`, so the variant and the type always agree; a mismatch can only be
/// a decoder bug and is reported rather than silently skipped.
fn write_element(
    writer: &mut FlussArrayWriter,
    index: usize,
    datum: Datum<'static>,
    element_type: &FlussDataType,
) -> Result<(), String> {
    match datum {
        Datum::Null => writer.set_null_at(index),
        Datum::Bool(value) => writer.write_boolean(index, value),
        Datum::Int8(value) => writer.write_byte(index, value),
        Datum::Int16(value) => writer.write_short(index, value),
        Datum::Int32(value) => writer.write_int(index, value),
        Datum::Int64(value) => writer.write_long(index, value),
        Datum::Float32(value) => writer.write_float(index, value.into_inner()),
        Datum::Float64(value) => writer.write_double(index, value.into_inner()),
        Datum::String(value) => writer.write_string(index, &value),
        Datum::Blob(value) => writer.write_binary_bytes(index, value.as_ref()),
        Datum::Decimal(value) => match element_type {
            FlussDataType::Decimal(declared) => {
                writer.write_decimal(index, &value, declared.precision());
            }
            _ => return Err("is a DECIMAL that does not match its declared element type".into()),
        },
        Datum::Date(value) => writer.write_date(index, value),
        Datum::Time(value) => writer.write_time(index, value),
        Datum::TimestampNtz(value) => match element_type {
            FlussDataType::Timestamp(declared) => {
                writer.write_timestamp_ntz(index, &value, declared.precision());
            }
            _ => return Err("is a TIMESTAMP that does not match its declared element type".into()),
        },
        Datum::TimestampLtz(value) => match element_type {
            FlussDataType::TimestampLTz(declared) => {
                writer.write_timestamp_ltz(index, &value, declared.precision());
            }
            _ => {
                return Err(
                    "is a TIMESTAMP_LTZ that does not match its declared element type".into(),
                );
            }
        },
        Datum::Array(value) => writer.write_array(index, &value),
        Datum::Map(value) => writer.write_map(index, &value),
        Datum::Row(value) => writer
            .write_row(index, value.as_ref())
            .map_err(|error| format!("could not be encoded as a nested ROW: {error}"))?,
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

/// Parses a full ISO-8601 timestamp with an optional `Z` or numeric offset suffix.
///
/// The date and time may be separated by `T` or a space.
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

/// Splits a trailing zone designator off a time string.
///
/// Returns the remaining time text and the offset in seconds when a zone is present.
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

/// 1970-01-01, the origin every day count in this module is relative to.
fn unix_epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, data_type: DataType) -> InputColumn {
        InputColumn::new(name, data_type)
    }

    #[test]
    fn accepts_a_partitioned_primary_key_schema() {
        validate_table_schema(
            vec![
                column("region", DataType::String { nullable: false }),
                column("id", DataType::BigInt { nullable: false }),
                column("name", DataType::String { nullable: true }),
            ],
            vec!["region".to_string(), "id".to_string()],
            vec!["region".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn rejects_structurally_invalid_schemas() {
        /// Asserts that one invalid schema is rejected with a message naming the reason.
        fn rejected(
            expected: &str,
            columns: Vec<InputColumn>,
            primary_keys: &[&str],
            partition_keys: &[&str],
        ) {
            let keys = |names: &[&str]| names.iter().map(|name| (*name).to_string()).collect();
            let error = validate_table_schema(columns, keys(primary_keys), keys(partition_keys))
                .expect_err(expected)
                .message()
                .to_string();
            assert!(error.contains(expected), "got: {error}");
        }

        rejected("at least one column", Vec::new(), &[], &[]);
        rejected(
            "duplicate column",
            vec![
                column("id", DataType::Int { nullable: false }),
                column("id", DataType::Int { nullable: false }),
            ],
            &[],
            &[],
        );
        rejected(
            "must not be nullable",
            vec![column("id", DataType::Int { nullable: true })],
            &["id"],
            &[],
        );
        rejected(
            "does not exist",
            vec![column("id", DataType::Int { nullable: false })],
            &["missing"],
            &[],
        );
        rejected(
            "must be part of the primary key",
            vec![
                column("id", DataType::Int { nullable: false }),
                column("region", DataType::String { nullable: false }),
            ],
            &["id"],
            &["region"],
        );
    }

    #[test]
    fn rejects_invalid_recursive_types() {
        assert!(
            validate_data_type(&DataType::Map {
                nullable: true,
                key: Box::new(DataType::String { nullable: true }),
                value: Box::new(DataType::Int { nullable: true }),
            })
            .is_err()
        );
        assert!(
            validate_data_type(&DataType::Char {
                nullable: true,
                length: 0,
            })
            .is_err()
        );
        validate_data_type(&DataType::Array {
            nullable: true,
            element: Box::new(DataType::String { nullable: true }),
        })
        .unwrap();
    }

    /// A number lexeme exactly as the wire parser retains it.
    fn number(lexeme: &str) -> InputValue {
        InputValue::ExactNumber(lexeme.to_string())
    }

    /// A JSON string value.
    fn text(value: &str) -> InputValue {
        InputValue::String(value.to_string())
    }

    /// A one-field row object.
    fn row_of(name: &str, value: InputValue) -> InputValue {
        InputValue::Object(vec![(name.to_string(), value)])
    }

    /// Decodes a single-column row and returns the decoded value of that column.
    fn decode_one(
        data_type: DataType,
        value: InputValue,
    ) -> Result<Datum<'static>, RowDecodeError> {
        let decoder = SchemaDecoder::new(vec![column("v", data_type)])?;
        let row = decoder.decode_row("entry-1", &row_of("v", value))?;
        Ok(row.as_native().values[0].clone())
    }

    /// Decodes a single-column row, expecting success.
    fn decoded(data_type: DataType, value: InputValue) -> Datum<'static> {
        decode_one(data_type, value).expect("decoding succeeds")
    }

    /// Decodes a single-column row, expecting the message to contain `expected`.
    fn rejected(data_type: DataType, value: InputValue, expected: &str) {
        let error = decode_one(data_type, value).expect_err(expected);
        assert!(
            error.message().contains(expected),
            "got: {}",
            error.message()
        );
        assert!(
            error.message().contains("column `v"),
            "the message must name the column, got: {}",
            error.message()
        );
    }

    #[test]
    fn decodes_booleans_and_small_integers() {
        assert_eq!(
            decoded(
                DataType::Boolean { nullable: false },
                InputValue::Boolean(true)
            ),
            Datum::Bool(true)
        );
        assert_eq!(
            decoded(DataType::TinyInt { nullable: false }, number("-128")),
            Datum::Int8(-128)
        );
        assert_eq!(
            decoded(DataType::SmallInt { nullable: false }, number("32767")),
            Datum::Int16(32_767)
        );
        assert_eq!(
            decoded(DataType::Int { nullable: false }, number("-2147483648")),
            Datum::Int32(i32::MIN)
        );
    }

    #[test]
    fn rejects_integers_that_are_out_of_range_or_not_exact() {
        rejected(
            DataType::TinyInt { nullable: false },
            number("128"),
            "out of range",
        );
        rejected(
            DataType::Int { nullable: false },
            number("2147483648"),
            "out of range",
        );
        rejected(
            DataType::Int { nullable: false },
            number("1.5"),
            "expects INT",
        );
        rejected(
            DataType::Int { nullable: false },
            number("1e3"),
            "expects INT",
        );
        rejected(
            DataType::Int { nullable: false },
            text("42"),
            "got a string",
        );
        rejected(
            DataType::Boolean { nullable: false },
            number("1"),
            "expects BOOLEAN",
        );
    }

    #[test]
    fn char_length_is_not_enforced() {
        // The native client documents that it does not check CHAR length either. Enforcing it here would reject
        // writes the client itself accepts, so the check deliberately stays out.
        assert_eq!(
            decoded(
                DataType::Char {
                    nullable: false,
                    length: 3,
                },
                text("much longer than three")
            ),
            Datum::String(Cow::Owned("much longer than three".to_string()))
        );
    }

    #[test]
    fn decodes_bigint_beyond_double_precision_from_number_and_string() {
        // 9007199254740993 is the first odd integer above 2^53 and cannot survive an f64 round trip.
        assert_eq!(
            decoded(
                DataType::BigInt { nullable: false },
                number("9007199254740993")
            ),
            Datum::Int64(9_007_199_254_740_993)
        );
        assert_eq!(
            decoded(
                DataType::BigInt { nullable: false },
                text("9223372036854775807")
            ),
            Datum::Int64(i64::MAX)
        );
        assert_eq!(
            decoded(
                DataType::BigInt { nullable: false },
                number("-9223372036854775808")
            ),
            Datum::Int64(i64::MIN)
        );
        rejected(
            DataType::BigInt { nullable: false },
            number("9223372036854775808"),
            "expects BIGINT",
        );
        rejected(
            DataType::BigInt { nullable: false },
            number("1.0"),
            "expects BIGINT",
        );
        rejected(
            DataType::BigInt { nullable: false },
            text("1.0"),
            "expects BIGINT",
        );
    }

    #[test]
    fn decodes_floats_including_the_non_finite_spellings() {
        assert_eq!(
            decoded(DataType::Double { nullable: false }, number("1.5")),
            Datum::from(1.5_f64)
        );
        assert_eq!(
            decoded(DataType::Float { nullable: false }, number("2.5")),
            Datum::from(2.5_f32)
        );
        assert_eq!(
            decoded(DataType::Double { nullable: false }, text("Infinity")),
            Datum::from(f64::INFINITY)
        );
        assert_eq!(
            decoded(DataType::Float { nullable: false }, text("-Infinity")),
            Datum::from(f32::NEG_INFINITY)
        );
        let Datum::Float64(nan) = decoded(DataType::Double { nullable: false }, text("NaN")) else {
            panic!("expected a double");
        };
        assert!(nan.into_inner().is_nan());
        rejected(
            DataType::Double { nullable: false },
            text("nan"),
            "expects DOUBLE",
        );
        rejected(
            DataType::Float { nullable: false },
            number("1e300"),
            "out of 32-bit float range",
        );
    }

    #[test]
    fn decodes_decimals_exactly_from_the_literal_text() {
        let ten_two = DataType::Decimal {
            nullable: false,
            precision: 10,
            scale: 2,
        };
        let decimal = |unscaled: i64| Decimal::from_unscaled_long(unscaled, 10, 2).unwrap();
        assert_eq!(
            decoded(ten_two.clone(), text("123.45")),
            Datum::Decimal(decimal(12_345))
        );
        assert_eq!(
            decoded(ten_two.clone(), number("123.45")),
            Datum::Decimal(decimal(12_345))
        );
        assert_eq!(
            decoded(ten_two.clone(), text("-1")),
            Datum::Decimal(decimal(-100))
        );
        assert_eq!(
            decoded(ten_two.clone(), text("1.450")),
            Datum::Decimal(decimal(145)),
            "trailing zeros beyond the scale are lossless"
        );
    }

    #[test]
    fn decodes_decimals_at_the_precision_boundary_without_a_float_detour() {
        let wide = DataType::Decimal {
            nullable: false,
            precision: 38,
            scale: 0,
        };
        let max = "9".repeat(38);
        let Datum::Decimal(decoded_value) = decoded(wide.clone(), text(&max)) else {
            panic!("expected a decimal");
        };
        assert_eq!(decoded_value.to_big_decimal().to_string(), max);

        let exact = DataType::Decimal {
            nullable: false,
            precision: 38,
            scale: 18,
        };
        let Datum::Decimal(decoded_value) =
            decoded(exact, number("9007199254740993.000000000000000001"))
        else {
            panic!("expected a decimal");
        };
        assert_eq!(
            decoded_value.to_big_decimal().to_string(),
            "9007199254740993.000000000000000001"
        );
    }

    #[test]
    fn rejects_decimals_that_would_round_or_overflow() {
        let ten_two = DataType::Decimal {
            nullable: false,
            precision: 10,
            scale: 2,
        };
        rejected(ten_two.clone(), text("1.234"), "fractional digits");
        rejected(ten_two.clone(), text("1e3"), "exponents are not accepted");
        rejected(ten_two.clone(), number("1e3"), "exponents are not accepted");
        rejected(
            ten_two.clone(),
            InputValue::Boolean(true),
            "expects DECIMAL",
        );
        rejected(
            DataType::Decimal {
                nullable: false,
                precision: 4,
                scale: 2,
            },
            text("123.45"),
            "digits of precision",
        );
    }

    #[test]
    fn decodes_binary_and_bytes_from_base64() {
        assert_eq!(
            decoded(DataType::Bytes { nullable: false }, text("AAH+/w==")),
            Datum::Blob(Cow::Owned(vec![0, 1, 254, 255]))
        );
        assert_eq!(
            decoded(
                DataType::Binary {
                    nullable: false,
                    length: 2,
                },
                text("AQI=")
            ),
            Datum::Blob(Cow::Owned(vec![1, 2]))
        );
        rejected(
            DataType::Bytes { nullable: false },
            text("not base64!"),
            "not valid base64",
        );
        rejected(
            DataType::Bytes { nullable: false },
            text("AQI"),
            "not valid base64",
        );
        rejected(
            DataType::Binary {
                nullable: false,
                length: 3,
            },
            text("AQI="),
            "expects BINARY(3), got 2 bytes",
        );
        rejected(
            DataType::Bytes { nullable: false },
            number("1"),
            "expects BINARY",
        );
    }

    #[test]
    fn decodes_dates_and_validates_the_calendar() {
        assert_eq!(
            decoded(DataType::Date { nullable: false }, text("2026-01-31")),
            Datum::Date(Date::new(20_484))
        );
        assert_eq!(
            decoded(DataType::Date { nullable: false }, text("1969-12-31")),
            Datum::Date(Date::new(-1))
        );
        rejected(
            DataType::Date { nullable: false },
            text("2026-02-30"),
            "expects DATE",
        );
        rejected(
            DataType::Date { nullable: false },
            text("2026-1-31"),
            "expects DATE",
        );
        rejected(
            DataType::Date { nullable: false },
            number("20484"),
            "got a number",
        );
    }

    #[test]
    fn decodes_times_and_honors_the_declared_precision() {
        assert_eq!(
            decoded(
                DataType::Time {
                    nullable: false,
                    precision: 3,
                },
                text("12:34:56.789")
            ),
            Datum::Time(Time::new(45_296_789))
        );
        assert_eq!(
            decoded(
                DataType::Time {
                    nullable: false,
                    precision: 0,
                },
                text("12:34")
            ),
            Datum::Time(Time::new(45_240_000))
        );
        rejected(
            DataType::Time {
                nullable: false,
                precision: 0,
            },
            text("12:34:56.5"),
            "finer fractional seconds",
        );
        rejected(
            DataType::Time {
                nullable: false,
                precision: 6,
            },
            text("12:34:56.789123"),
            "sub-millisecond TIME",
        );
        rejected(
            DataType::Time {
                nullable: false,
                precision: 3,
            },
            text("25:00:00"),
            "expects TIME",
        );
    }

    #[test]
    fn decodes_zone_free_timestamps_and_rejects_zones() {
        let ntz = DataType::Timestamp {
            nullable: false,
            precision: 3,
        };
        assert_eq!(
            decoded(ntz.clone(), text("2026-01-31T12:34:56.789")),
            Datum::TimestampNtz(TimestampNtz::new(1_769_862_896_789))
        );
        assert_eq!(
            decoded(ntz.clone(), text("2026-01-31 12:34:56.789")),
            Datum::TimestampNtz(TimestampNtz::new(1_769_862_896_789)),
            "a space separator is accepted"
        );
        rejected(
            ntz.clone(),
            text("2026-01-31T12:34:56.789Z"),
            "carries a zone",
        );
        rejected(ntz, text("2026-01-31T12:34:56.789+02:00"), "carries a zone");
    }

    #[test]
    fn decodes_zoned_timestamps_and_normalizes_them_to_utc() {
        let ltz = DataType::TimestampLtz {
            nullable: false,
            precision: 3,
        };
        assert_eq!(
            decoded(ltz.clone(), text("2026-01-31T12:34:56.789Z")),
            Datum::TimestampLtz(TimestampLtz::new(1_769_862_896_789))
        );
        assert_eq!(
            decoded(ltz.clone(), text("2026-01-31T14:34:56.789+02:00")),
            Datum::TimestampLtz(TimestampLtz::new(1_769_862_896_789)),
            "an offset is normalized to UTC"
        );
        assert_eq!(
            decoded(ltz.clone(), text("2026-01-31T10:34:56.789-02:00")),
            Datum::TimestampLtz(TimestampLtz::new(1_769_862_896_789))
        );
        rejected(ltz, text("2026-01-31T12:34:56.789"), "has no zone");
    }

    #[test]
    fn decodes_sub_millisecond_timestamps_into_the_nanosecond_remainder() {
        let nanos = DataType::TimestampLtz {
            nullable: false,
            precision: 9,
        };
        assert_eq!(
            decoded(nanos, text("2026-01-31T12:34:56.789123456Z")),
            Datum::TimestampLtz(
                TimestampLtz::from_millis_nanos(1_769_862_896_789, 123_456).unwrap()
            )
        );
        let micros = DataType::Timestamp {
            nullable: false,
            precision: 6,
        };
        assert_eq!(
            decoded(micros, text("1969-12-31T23:59:59.999999")),
            Datum::TimestampNtz(TimestampNtz::from_millis_nanos(-1, 999_000).unwrap()),
            "the fraction stays non-negative before the epoch"
        );
    }

    #[test]
    fn decodes_timestamps_from_epoch_millis_under_the_same_precision_rule() {
        assert_eq!(
            decoded(
                DataType::TimestampLtz {
                    nullable: false,
                    precision: 3,
                },
                number("1769862896789")
            ),
            Datum::TimestampLtz(TimestampLtz::new(1_769_862_896_789))
        );
        assert_eq!(
            decoded(
                DataType::Timestamp {
                    nullable: false,
                    precision: 0,
                },
                number("1769862896000")
            ),
            Datum::TimestampNtz(TimestampNtz::new(1_769_862_896_000))
        );
        rejected(
            DataType::Timestamp {
                nullable: false,
                precision: 0,
            },
            number("1500"),
            "finer fractional seconds",
        );
        rejected(
            DataType::TimestampLtz {
                nullable: false,
                precision: 2,
            },
            number("1001"),
            "finer fractional seconds",
        );
        rejected(
            DataType::Timestamp {
                nullable: false,
                precision: 3,
            },
            number("1.5"),
            "expects TIMESTAMP",
        );
    }

    #[test]
    fn decodes_nested_arrays_including_nulls() {
        let array_type = DataType::Array {
            nullable: false,
            element: Box::new(DataType::Int { nullable: true }),
        };
        let Datum::Array(array) = decoded(
            array_type,
            InputValue::Array(vec![number("1"), InputValue::Null, number("3")]),
        ) else {
            panic!("expected an array");
        };
        assert_eq!(array.size(), 3);
        assert_eq!(array.get_int(0).unwrap(), 1);
        assert!(array.is_null_at(1));
        assert_eq!(array.get_int(2).unwrap(), 3);
    }

    #[test]
    fn decodes_maps_as_ordered_key_value_entries() {
        let map_type = DataType::Map {
            nullable: false,
            key: Box::new(DataType::String { nullable: false }),
            value: Box::new(DataType::Int { nullable: true }),
        };
        let entry = |key: &str, value: InputValue| {
            InputValue::Object(vec![
                ("key".to_string(), text(key)),
                ("value".to_string(), value),
            ])
        };
        let Datum::Map(map) = decoded(
            map_type,
            InputValue::Array(vec![entry("b", number("2")), entry("a", InputValue::Null)]),
        ) else {
            panic!("expected a map");
        };
        assert_eq!(map.size(), 2);
        assert_eq!(map.key_array().get_string(0).unwrap(), "b");
        assert_eq!(map.value_array().get_int(0).unwrap(), 2);
        assert_eq!(
            map.key_array().get_string(1).unwrap(),
            "a",
            "entry order is preserved"
        );
        assert!(map.value_array().is_null_at(1));
    }

    #[test]
    fn rejects_malformed_map_entries_and_null_keys() {
        let map_type = || DataType::Map {
            nullable: false,
            key: Box::new(DataType::String { nullable: false }),
            value: Box::new(DataType::Int { nullable: true }),
        };
        rejected(
            map_type(),
            InputValue::Array(vec![text("a")]),
            "expects a map entry object",
        );
        rejected(
            map_type(),
            InputValue::Array(vec![InputValue::Object(vec![(
                "value".to_string(),
                number("1"),
            )])]),
            "is required in a map entry",
        );
        rejected(
            map_type(),
            InputValue::Array(vec![InputValue::Object(vec![
                ("key".to_string(), InputValue::Null),
                ("value".to_string(), number("1")),
            ])]),
            "must not be null",
        );
        rejected(
            map_type(),
            InputValue::Array(vec![InputValue::Object(vec![
                ("key".to_string(), text("a")),
                ("value".to_string(), number("1")),
                ("extra".to_string(), number("1")),
            ])]),
            "unknown field `extra`",
        );
        rejected(map_type(), text("a"), "expects MAP");
    }

    #[test]
    fn decodes_nested_rows_and_names_the_nested_path() {
        let row_type = DataType::Row {
            nullable: false,
            fields: vec![
                RowField {
                    name: "id".to_string(),
                    data_type: DataType::BigInt { nullable: false },
                    description: None,
                    field_id: -1,
                },
                RowField {
                    name: "label".to_string(),
                    data_type: DataType::String { nullable: true },
                    description: None,
                    field_id: -1,
                },
            ],
        };
        let Datum::Row(row) = decoded(
            row_type.clone(),
            InputValue::Object(vec![("id".to_string(), text("9007199254740993"))]),
        ) else {
            panic!("expected a row");
        };
        assert_eq!(
            row.values,
            vec![Datum::Int64(9_007_199_254_740_993), Datum::Null],
            "an omitted nullable field becomes null"
        );

        let error = decode_one(
            row_type.clone(),
            InputValue::Object(vec![("label".to_string(), text("x"))]),
        )
        .expect_err("the non-nullable field is required");
        assert!(
            error.message().contains("`v.id`"),
            "got: {}",
            error.message()
        );

        let error = decode_one(
            row_type,
            InputValue::Object(vec![("id".to_string(), InputValue::Boolean(true))]),
        )
        .expect_err("wrong field type");
        assert!(
            error.message().contains("`v.id`"),
            "got: {}",
            error.message()
        );
    }

    #[test]
    fn names_the_element_path_inside_containers() {
        let nested = DataType::Array {
            nullable: false,
            element: Box::new(DataType::Array {
                nullable: true,
                element: Box::new(DataType::Int { nullable: false }),
            }),
        };
        let error = decode_one(
            nested,
            InputValue::Array(vec![
                InputValue::Null,
                InputValue::Array(vec![number("1"), text("2")]),
            ]),
        )
        .expect_err("the inner element is not an integer");
        assert!(
            error.message().contains("`v[1][1]`"),
            "got: {}",
            error.message()
        );
    }

    #[test]
    fn nullability_is_enforced_per_node() {
        assert_eq!(
            decoded(DataType::Int { nullable: true }, InputValue::Null),
            Datum::Null
        );
        rejected(
            DataType::Int { nullable: false },
            InputValue::Null,
            "must not be null",
        );
        rejected(
            DataType::Array {
                nullable: false,
                element: Box::new(DataType::Int { nullable: false }),
            },
            InputValue::Array(vec![InputValue::Null]),
            "must not be null",
        );
    }

    #[test]
    fn decode_row_fills_missing_nullable_columns_and_rejects_missing_required_ones() {
        let decoder = SchemaDecoder::new(vec![
            column("id", DataType::Int { nullable: false }),
            column("name", DataType::String { nullable: true }),
        ])
        .unwrap();

        let row = decoder
            .decode_row("entry-1", &row_of("id", number("7")))
            .unwrap();
        assert_eq!(row.field_count(), 2);
        assert_eq!(row.as_native().values, vec![Datum::Int32(7), Datum::Null]);

        let error = decoder
            .decode_row("entry-1", &row_of("name", text("Ada")))
            .expect_err("id is required");
        assert!(error.is_schema_mismatch(), "a missing column may be stale");
        assert!(
            error.message().contains("`id` is required"),
            "got: {}",
            error.message()
        );
    }

    #[test]
    fn decode_row_rejects_unknown_and_duplicate_columns() {
        let decoder =
            SchemaDecoder::new(vec![column("id", DataType::Int { nullable: false })]).unwrap();

        let error = decoder
            .decode_row("entry-1", &row_of("nope", number("1")))
            .expect_err("unknown column");
        assert!(
            error.is_schema_mismatch(),
            "an unknown column may be resolved by a metadata refresh"
        );
        assert!(
            error.message().contains("unknown column `nope`"),
            "got: {}",
            error.message()
        );

        let error = decoder
            .decode_row(
                "entry-1",
                &InputValue::Object(vec![
                    ("id".to_string(), number("1")),
                    ("id".to_string(), number("2")),
                ]),
            )
            .expect_err("duplicate column");
        assert!(
            !error.is_schema_mismatch(),
            "a duplicate field is malformed input, never staleness"
        );
        assert!(
            error.message().contains("duplicate column `id`"),
            "got: {}",
            error.message()
        );

        let error = decoder
            .decode_row("entry-1", &text("not an object"))
            .expect_err("the row must be an object");
        assert!(
            error.message().contains("must be an object"),
            "got: {}",
            error.message()
        );
    }

    #[test]
    fn decode_sparse_row_requires_only_the_named_columns() {
        let decoder = SchemaDecoder::new(vec![
            column("id", DataType::Int { nullable: false }),
            column("region", DataType::String { nullable: false }),
            column("amount", DataType::BigInt { nullable: false }),
        ])
        .unwrap();
        let keys = vec!["id".to_string(), "region".to_string()];

        let row = decoder
            .decode_sparse_row(
                "entry-1",
                &InputValue::Object(vec![
                    ("id".to_string(), number("7")),
                    ("region".to_string(), text("eu")),
                ]),
                &keys,
            )
            .unwrap();
        assert_eq!(
            row.as_native().values,
            vec![
                Datum::Int32(7),
                Datum::String(Cow::Owned("eu".to_string())),
                Datum::Null
            ],
            "a non-required column is null even though its type is not nullable"
        );

        let error = decoder
            .decode_sparse_row("entry-1", &row_of("id", number("7")), &keys)
            .expect_err("region is required");
        assert!(error.is_schema_mismatch());
        assert!(
            error.message().contains("`region` is required"),
            "got: {}",
            error.message()
        );

        let error = decoder
            .decode_sparse_row(
                "entry-1",
                &InputValue::Object(vec![
                    ("id".to_string(), number("7")),
                    ("region".to_string(), InputValue::Null),
                ]),
                &keys,
            )
            .expect_err("an explicit null cannot satisfy a required column");
        assert!(!error.is_schema_mismatch());
        assert!(
            error.message().contains("must not be null"),
            "got: {}",
            error.message()
        );

        let error = decoder
            .decode_sparse_row(
                "entry-1",
                &row_of("id", number("7")),
                &["ghost".to_string()],
            )
            .expect_err("the required column is not in the schema");
        assert!(error.is_schema_mismatch());
        assert!(
            error.message().contains("not part of the table schema"),
            "got: {}",
            error.message()
        );
    }

    #[test]
    fn every_message_names_the_entry_it_came_from() {
        let decoder =
            SchemaDecoder::new(vec![column("id", DataType::Int { nullable: false })]).unwrap();
        let error = decoder
            .decode_row("order-42", &row_of("id", text("x")))
            .expect_err("wrong type");
        assert!(
            error.message().contains("entry `order-42`"),
            "got: {}",
            error.message()
        );
    }

    #[test]
    fn parses_the_zone_offset_boundary_strictly() {
        assert_eq!(split_zone("12:00:00+18:00").unwrap().1, Some(18 * 3_600));
        assert_eq!(split_zone("12:00:00-18:00").unwrap().1, Some(-18 * 3_600));
        assert_eq!(split_zone("12:00:00").unwrap().1, None);
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
    fn rejects_multibyte_temporal_text_without_panicking() {
        for invalid in ["123456789é1:11:11", "é", "2026-01-31é12:34:56"] {
            assert!(
                decode_one(
                    DataType::Timestamp {
                        nullable: false,
                        precision: 3,
                    },
                    text(invalid)
                )
                .is_err(),
                "{invalid:?} must fail cleanly"
            );
        }
    }
}
