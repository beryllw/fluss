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
//! Schema validation ([`validate_table_schema`], [`validate_data_type`]) is complete: DDL and the write path
//! both depend on it. The per-value decoding matrix behind [`SchemaDecoder::decode_row`] and
//! [`SchemaDecoder::decode_sparse_row`] is the write path's own concern and is not implemented yet; both report
//! an unsupported operation until it is.

use crate::application::{DataType, InputValue};
use crate::error::GatewayError;
use fluss::metadata::DataType as FlussDataType;
use fluss::row::GenericRow;
use std::collections::{HashMap, HashSet};

const MAX_TYPE_NESTING: usize = 64;

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
    #[allow(dead_code)] // Raised by the decoding matrix once the write path lands.
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
    #[allow(dead_code)] // Produced by the decoding matrix once the write path lands.
    pub(crate) fn from_native(row: GenericRow<'static>) -> Self {
        Self(row)
    }

    pub fn field_count(&self) -> usize {
        self.0.values.len()
    }

    #[allow(dead_code)] // Read by the native write backend once the write path lands.
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
#[derive(Debug, Clone)]
pub struct SchemaDecoder {
    columns: Vec<InputColumn>,
    /// Column name to position, used by sparse decoding to reject required columns that are not in the schema.
    #[allow(dead_code)]
    column_indexes: HashMap<String, usize>,
}

impl SchemaDecoder {
    pub fn new(columns: Vec<InputColumn>) -> Result<Self, GatewayError> {
        let validated = validate_table_schema(columns, Vec::new(), Vec::new())?;
        Ok(Self {
            columns: validated.columns,
            column_indexes: validated.column_indexes,
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
        _entry_id: &str,
        _value: &InputValue,
    ) -> Result<DecodedRow, RowDecodeError> {
        Err(GatewayError::unsupported("row decoding is not implemented yet").into())
    }

    /// Decodes a sparse operation, such as delete, into full schema order.
    ///
    /// Required fields must be present and non-null. Missing non-required fields become null even
    /// when their table type is non-nullable because the operation does not submit those values.
    pub fn decode_sparse_row(
        &self,
        _entry_id: &str,
        _value: &InputValue,
        _required_columns: &[String],
    ) -> Result<DecodedRow, RowDecodeError> {
        Err(GatewayError::unsupported("row decoding is not implemented yet").into())
    }
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

    #[test]
    fn row_decoding_reports_an_unsupported_operation_until_the_write_path_lands() {
        let decoder =
            SchemaDecoder::new(vec![column("id", DataType::Int { nullable: false })]).unwrap();
        assert_eq!(decoder.columns().len(), 1);
        for error in [
            decoder
                .decode_row("entry-1", &InputValue::Object(Vec::new()))
                .unwrap_err(),
            decoder
                .decode_sparse_row("entry-1", &InputValue::Object(Vec::new()), &[])
                .unwrap_err(),
        ] {
            assert!(!error.is_schema_mismatch());
            assert_eq!(
                error.into_gateway_error().kind(),
                crate::error::ErrorKind::Unsupported
            );
        }
    }
}
