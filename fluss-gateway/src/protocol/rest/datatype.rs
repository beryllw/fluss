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

//! The wire representation of a Fluss data type.
//!
//! This lives in its own module because two unrelated concerns depend on it: table metadata and DDL bodies read
//! and write `data_type` objects, and the JSON value codec dispatches on them. Keeping the DTO here means
//! neither has to own it.
//!
//! [`DataTypeResponse`] is a structured tagged object, never a type string: `{"type": "DECIMAL", "nullable":
//! false, "precision": 18, "scale": 2}`. Deserialization is strict — an unknown type name, a missing required
//! parameter, or a parameter that does not belong to the named type is rejected. Converting a wire type into the
//! domain [`DataType`] validates precision, scale, and length, reporting
//! [`crate::error::ErrorKind::InvalidArgument`] on failure.

use crate::backend::types::{DataType, RowField};
use crate::error::GatewayError;
use crate::protocol::rest::input_decode::validate_data_type;
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

/// Exact recursive Fluss type exposed by metadata and reused by table mutations.
#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
#[schema(no_recursion)]
pub enum DataTypeResponse {
    Boolean {
        nullable: bool,
    },
    #[serde(rename = "TINYINT")]
    TinyInt {
        nullable: bool,
    },
    #[serde(rename = "SMALLINT")]
    SmallInt {
        nullable: bool,
    },
    Int {
        nullable: bool,
    },
    #[serde(rename = "BIGINT")]
    BigInt {
        nullable: bool,
    },
    Float {
        nullable: bool,
    },
    Double {
        nullable: bool,
    },
    Char {
        nullable: bool,
        length: u32,
    },
    String {
        nullable: bool,
    },
    Decimal {
        nullable: bool,
        precision: u32,
        scale: u32,
    },
    Date {
        nullable: bool,
    },
    Time {
        nullable: bool,
        precision: u32,
    },
    Timestamp {
        nullable: bool,
        precision: u32,
    },
    TimestampLtz {
        nullable: bool,
        precision: u32,
    },
    Bytes {
        nullable: bool,
    },
    Binary {
        nullable: bool,
        length: usize,
    },
    Array {
        nullable: bool,
        element_type: Box<DataTypeResponse>,
    },
    Map {
        nullable: bool,
        key_type: Box<DataTypeResponse>,
        value_type: Box<DataTypeResponse>,
    },
    Row {
        nullable: bool,
        fields: Vec<RowFieldResponse>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DataTypeWire {
    #[serde(rename = "type")]
    type_name: String,
    nullable: bool,
    length: Option<u64>,
    precision: Option<u32>,
    scale: Option<u32>,
    element_type: Option<Box<DataTypeResponse>>,
    key_type: Option<Box<DataTypeResponse>>,
    value_type: Option<Box<DataTypeResponse>>,
    fields: Option<Vec<RowFieldResponse>>,
}

impl<'de> Deserialize<'de> for DataTypeResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DataTypeWire::deserialize(deserializer)?;
        let type_name = wire.type_name.to_ascii_uppercase();
        let no_parameters = || {
            if wire.length.is_some()
                || wire.precision.is_some()
                || wire.scale.is_some()
                || wire.element_type.is_some()
                || wire.key_type.is_some()
                || wire.value_type.is_some()
                || wire.fields.is_some()
            {
                Err(serde::de::Error::custom(format!(
                    "type `{type_name}` has unexpected parameters"
                )))
            } else {
                Ok(())
            }
        };
        let only_length = || {
            if wire.precision.is_some()
                || wire.scale.is_some()
                || wire.element_type.is_some()
                || wire.key_type.is_some()
                || wire.value_type.is_some()
                || wire.fields.is_some()
            {
                Err(serde::de::Error::custom(format!(
                    "type `{type_name}` has unexpected parameters"
                )))
            } else {
                wire.length.ok_or_else(|| {
                    serde::de::Error::custom(format!("type `{type_name}` requires `length`"))
                })
            }
        };
        let only_precision = || {
            if wire.length.is_some()
                || wire.scale.is_some()
                || wire.element_type.is_some()
                || wire.key_type.is_some()
                || wire.value_type.is_some()
                || wire.fields.is_some()
            {
                Err(serde::de::Error::custom(format!(
                    "type `{type_name}` has unexpected parameters"
                )))
            } else {
                wire.precision.ok_or_else(|| {
                    serde::de::Error::custom(format!("type `{type_name}` requires `precision`"))
                })
            }
        };

        let data_type = match type_name.as_str() {
            "BOOLEAN" => {
                no_parameters()?;
                Self::Boolean {
                    nullable: wire.nullable,
                }
            }
            "TINYINT" => {
                no_parameters()?;
                Self::TinyInt {
                    nullable: wire.nullable,
                }
            }
            "SMALLINT" => {
                no_parameters()?;
                Self::SmallInt {
                    nullable: wire.nullable,
                }
            }
            "INT" => {
                no_parameters()?;
                Self::Int {
                    nullable: wire.nullable,
                }
            }
            "BIGINT" => {
                no_parameters()?;
                Self::BigInt {
                    nullable: wire.nullable,
                }
            }
            "FLOAT" => {
                no_parameters()?;
                Self::Float {
                    nullable: wire.nullable,
                }
            }
            "DOUBLE" => {
                no_parameters()?;
                Self::Double {
                    nullable: wire.nullable,
                }
            }
            "CHAR" => Self::Char {
                nullable: wire.nullable,
                length: u32::try_from(only_length()?).map_err(serde::de::Error::custom)?,
            },
            "STRING" => {
                no_parameters()?;
                Self::String {
                    nullable: wire.nullable,
                }
            }
            "DECIMAL" => {
                if wire.length.is_some()
                    || wire.element_type.is_some()
                    || wire.key_type.is_some()
                    || wire.value_type.is_some()
                    || wire.fields.is_some()
                {
                    return Err(serde::de::Error::custom(
                        "type `DECIMAL` has unexpected parameters",
                    ));
                }
                Self::Decimal {
                    nullable: wire.nullable,
                    precision: wire.precision.ok_or_else(|| {
                        serde::de::Error::custom("type `DECIMAL` requires `precision`")
                    })?,
                    scale: wire.scale.ok_or_else(|| {
                        serde::de::Error::custom("type `DECIMAL` requires `scale`")
                    })?,
                }
            }
            "DATE" => {
                no_parameters()?;
                Self::Date {
                    nullable: wire.nullable,
                }
            }
            "TIME" => Self::Time {
                nullable: wire.nullable,
                precision: only_precision()?,
            },
            "TIMESTAMP" => Self::Timestamp {
                nullable: wire.nullable,
                precision: only_precision()?,
            },
            "TIMESTAMP_LTZ" => Self::TimestampLtz {
                nullable: wire.nullable,
                precision: only_precision()?,
            },
            "BYTES" => {
                no_parameters()?;
                Self::Bytes {
                    nullable: wire.nullable,
                }
            }
            "BINARY" => Self::Binary {
                nullable: wire.nullable,
                length: usize::try_from(only_length()?).map_err(serde::de::Error::custom)?,
            },
            "ARRAY" => {
                if wire.length.is_some()
                    || wire.precision.is_some()
                    || wire.scale.is_some()
                    || wire.key_type.is_some()
                    || wire.value_type.is_some()
                    || wire.fields.is_some()
                {
                    return Err(serde::de::Error::custom(
                        "type `ARRAY` has unexpected parameters",
                    ));
                }
                Self::Array {
                    nullable: wire.nullable,
                    element_type: wire.element_type.ok_or_else(|| {
                        serde::de::Error::custom("type `ARRAY` requires `element_type`")
                    })?,
                }
            }
            "MAP" => {
                if wire.length.is_some()
                    || wire.precision.is_some()
                    || wire.scale.is_some()
                    || wire.element_type.is_some()
                    || wire.fields.is_some()
                {
                    return Err(serde::de::Error::custom(
                        "type `MAP` has unexpected parameters",
                    ));
                }
                Self::Map {
                    nullable: wire.nullable,
                    key_type: wire.key_type.ok_or_else(|| {
                        serde::de::Error::custom("type `MAP` requires `key_type`")
                    })?,
                    value_type: wire.value_type.ok_or_else(|| {
                        serde::de::Error::custom("type `MAP` requires `value_type`")
                    })?,
                }
            }
            "ROW" => {
                if wire.length.is_some()
                    || wire.precision.is_some()
                    || wire.scale.is_some()
                    || wire.element_type.is_some()
                    || wire.key_type.is_some()
                    || wire.value_type.is_some()
                {
                    return Err(serde::de::Error::custom(
                        "type `ROW` has unexpected parameters",
                    ));
                }
                Self::Row {
                    nullable: wire.nullable,
                    fields: wire
                        .fields
                        .ok_or_else(|| serde::de::Error::custom("type `ROW` requires `fields`"))?,
                }
            }
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "unknown Fluss data type `{}`",
                    wire.type_name
                )));
            }
        };
        Ok(data_type)
    }
}

impl From<&DataType> for DataTypeResponse {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean { nullable } => Self::Boolean {
                nullable: *nullable,
            },
            DataType::TinyInt { nullable } => Self::TinyInt {
                nullable: *nullable,
            },
            DataType::SmallInt { nullable } => Self::SmallInt {
                nullable: *nullable,
            },
            DataType::Int { nullable } => Self::Int {
                nullable: *nullable,
            },
            DataType::BigInt { nullable } => Self::BigInt {
                nullable: *nullable,
            },
            DataType::Float { nullable } => Self::Float {
                nullable: *nullable,
            },
            DataType::Double { nullable } => Self::Double {
                nullable: *nullable,
            },
            DataType::Char { nullable, length } => Self::Char {
                nullable: *nullable,
                length: *length,
            },
            DataType::String { nullable } => Self::String {
                nullable: *nullable,
            },
            DataType::Decimal {
                nullable,
                precision,
                scale,
            } => Self::Decimal {
                nullable: *nullable,
                precision: *precision,
                scale: *scale,
            },
            DataType::Date { nullable } => Self::Date {
                nullable: *nullable,
            },
            DataType::Time {
                nullable,
                precision,
            } => Self::Time {
                nullable: *nullable,
                precision: *precision,
            },
            DataType::Timestamp {
                nullable,
                precision,
            } => Self::Timestamp {
                nullable: *nullable,
                precision: *precision,
            },
            DataType::TimestampLtz {
                nullable,
                precision,
            } => Self::TimestampLtz {
                nullable: *nullable,
                precision: *precision,
            },
            DataType::Bytes { nullable } => Self::Bytes {
                nullable: *nullable,
            },
            DataType::Binary { nullable, length } => Self::Binary {
                nullable: *nullable,
                length: *length,
            },
            DataType::Array { nullable, element } => Self::Array {
                nullable: *nullable,
                element_type: Box::new(Self::from(element.as_ref())),
            },
            DataType::Map {
                nullable,
                key,
                value,
            } => Self::Map {
                nullable: *nullable,
                key_type: Box::new(Self::from(key.as_ref())),
                value_type: Box::new(Self::from(value.as_ref())),
            },
            DataType::Row { nullable, fields } => Self::Row {
                nullable: *nullable,
                fields: fields.iter().map(RowFieldResponse::from).collect(),
            },
        }
    }
}

/// Structural conversion without validation. [`TryFrom`] validates the result once at the root.
fn to_domain(data_type: DataTypeResponse) -> DataType {
    {
        match data_type {
            DataTypeResponse::Boolean { nullable } => DataType::Boolean { nullable },
            DataTypeResponse::TinyInt { nullable } => DataType::TinyInt { nullable },
            DataTypeResponse::SmallInt { nullable } => DataType::SmallInt { nullable },
            DataTypeResponse::Int { nullable } => DataType::Int { nullable },
            DataTypeResponse::BigInt { nullable } => DataType::BigInt { nullable },
            DataTypeResponse::Float { nullable } => DataType::Float { nullable },
            DataTypeResponse::Double { nullable } => DataType::Double { nullable },
            DataTypeResponse::Char { nullable, length } => DataType::Char { nullable, length },
            DataTypeResponse::String { nullable } => DataType::String { nullable },
            DataTypeResponse::Decimal {
                nullable,
                precision,
                scale,
            } => DataType::Decimal {
                nullable,
                precision,
                scale,
            },
            DataTypeResponse::Date { nullable } => DataType::Date { nullable },
            DataTypeResponse::Time {
                nullable,
                precision,
            } => DataType::Time {
                nullable,
                precision,
            },
            DataTypeResponse::Timestamp {
                nullable,
                precision,
            } => DataType::Timestamp {
                nullable,
                precision,
            },
            DataTypeResponse::TimestampLtz {
                nullable,
                precision,
            } => DataType::TimestampLtz {
                nullable,
                precision,
            },
            DataTypeResponse::Bytes { nullable } => DataType::Bytes { nullable },
            DataTypeResponse::Binary { nullable, length } => DataType::Binary { nullable, length },
            DataTypeResponse::Array {
                nullable,
                element_type,
            } => DataType::Array {
                nullable,
                element: Box::new(to_domain(*element_type)),
            },
            DataTypeResponse::Map {
                nullable,
                key_type,
                value_type,
            } => DataType::Map {
                nullable,
                key: Box::new(to_domain(*key_type)),
                value: Box::new(to_domain(*value_type)),
            },
            DataTypeResponse::Row { nullable, fields } => DataType::Row {
                nullable,
                fields: fields.into_iter().map(row_field_to_domain).collect(),
            },
        }
    }
}

/// One field within a structured `ROW` type. Native field IDs are intentionally not public.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RowFieldResponse {
    pub name: String,
    #[schema(no_recursion)]
    pub field_type: DataTypeResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl From<&RowField> for RowFieldResponse {
    fn from(field: &RowField) -> Self {
        Self {
            name: field.name.clone(),
            field_type: DataTypeResponse::from(&field.data_type),
            description: field.description.clone(),
        }
    }
}

/// Structural conversion of one row field. Validation happens once at the root of the type tree.
fn row_field_to_domain(field: RowFieldResponse) -> RowField {
    RowField {
        name: field.name,
        data_type: to_domain(field.field_type),
        description: field.description,
        // A field created through the wire API carries the native client's unassigned sentinel.
        field_id: -1,
    }
}

impl TryFrom<DataTypeResponse> for DataType {
    type Error = GatewayError;

    /// Converts a wire type into the domain type, rejecting invalid precision, scale, and length.
    ///
    /// Validation runs once on the fully built tree, so a nested failure is reported with the same
    /// `invalid_argument` envelope as a top-level one.
    fn try_from(data_type: DataTypeResponse) -> Result<Self, Self::Error> {
        let domain = to_domain(data_type);
        validate_data_type(&domain)?;
        Ok(domain)
    }
}

impl TryFrom<RowFieldResponse> for RowField {
    type Error = GatewayError;

    fn try_from(field: RowFieldResponse) -> Result<Self, Self::Error> {
        let field = row_field_to_domain(field);
        validate_data_type(&field.data_type)?;
        Ok(field)
    }
}
