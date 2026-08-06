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

//! Protocol-neutral identifiers and recursive Fluss data types.

use crate::error::GatewayError;
use fluss::metadata::{
    ArrayType, BigIntType, BinaryType, BooleanType, BytesType, CharType, DataField,
    DataType as FlussDataType, DateType, DecimalType, DoubleType, FloatType, IntType, MapType,
    RowType, SmallIntType, StringType, TimeType, TimestampLTzType, TimestampType, TinyIntType,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Validated cluster identifier shared by configuration, application services, and protocols.
///
/// Cluster IDs match `[a-z][a-z0-9_]{0,62}`. This is also safe as an unquoted catalog name in a
/// future SQL adapter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ClusterId {
    type Error = GatewayError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = value.as_bytes();
        let valid = (1..=63).contains(&bytes.len())
            && bytes[0].is_ascii_lowercase()
            && bytes
                .iter()
                .skip(1)
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_');
        if !valid {
            return Err(GatewayError::invalid_argument(
                "cluster ID must match [a-z][a-z0-9_]{0,62}",
            ));
        }
        Ok(Self(value.to_string()))
    }
}

impl TryFrom<String> for ClusterId {
    type Error = GatewayError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for ClusterId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ClusterId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

/// A field nested within a [`DataType::Row`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowField {
    pub name: String,
    pub data_type: DataType,
    pub description: Option<String>,
    /// Stable nested field ID, or the native client's unassigned sentinel for a new field.
    pub field_id: i32,
}

/// Exact recursive Fluss schema type.
///
/// Nullability lives on every node, including nested elements, map keys and values, and rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean {
        nullable: bool,
    },
    TinyInt {
        nullable: bool,
    },
    SmallInt {
        nullable: bool,
    },
    Int {
        nullable: bool,
    },
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
        element: Box<DataType>,
    },
    Map {
        nullable: bool,
        key: Box<DataType>,
        value: Box<DataType>,
    },
    Row {
        nullable: bool,
        fields: Vec<RowField>,
    },
}

impl DataType {
    /// Returns nullability for this node without inspecting its children.
    pub fn nullable(&self) -> bool {
        match self {
            Self::Boolean { nullable }
            | Self::TinyInt { nullable }
            | Self::SmallInt { nullable }
            | Self::Int { nullable }
            | Self::BigInt { nullable }
            | Self::Float { nullable }
            | Self::Double { nullable }
            | Self::Char { nullable, .. }
            | Self::String { nullable }
            | Self::Decimal { nullable, .. }
            | Self::Date { nullable }
            | Self::Time { nullable, .. }
            | Self::Timestamp { nullable, .. }
            | Self::TimestampLtz { nullable, .. }
            | Self::Bytes { nullable }
            | Self::Binary { nullable, .. }
            | Self::Array { nullable, .. }
            | Self::Map { nullable, .. }
            | Self::Row { nullable, .. } => *nullable,
        }
    }
}

impl TryFrom<&FlussDataType> for DataType {
    type Error = GatewayError;

    fn try_from(value: &FlussDataType) -> Result<Self, Self::Error> {
        let nullable = value.is_nullable();
        let converted = match value {
            FlussDataType::Boolean(_) => Self::Boolean { nullable },
            FlussDataType::TinyInt(_) => Self::TinyInt { nullable },
            FlussDataType::SmallInt(_) => Self::SmallInt { nullable },
            FlussDataType::Int(_) => Self::Int { nullable },
            FlussDataType::BigInt(_) => Self::BigInt { nullable },
            FlussDataType::Float(_) => Self::Float { nullable },
            FlussDataType::Double(_) => Self::Double { nullable },
            FlussDataType::Char(value) => Self::Char {
                nullable,
                length: value.length(),
            },
            FlussDataType::String(_) => Self::String { nullable },
            FlussDataType::Decimal(value) => Self::Decimal {
                nullable,
                precision: value.precision(),
                scale: value.scale(),
            },
            FlussDataType::Date(_) => Self::Date { nullable },
            FlussDataType::Time(value) => Self::Time {
                nullable,
                precision: value.precision(),
            },
            FlussDataType::Timestamp(value) => Self::Timestamp {
                nullable,
                precision: value.precision(),
            },
            FlussDataType::TimestampLTz(value) => Self::TimestampLtz {
                nullable,
                precision: value.precision(),
            },
            FlussDataType::Bytes(_) => Self::Bytes { nullable },
            FlussDataType::Binary(value) => Self::Binary {
                nullable,
                length: value.length(),
            },
            FlussDataType::Array(value) => Self::Array {
                nullable,
                element: Box::new(Self::try_from(value.get_element_type())?),
            },
            FlussDataType::Map(value) => Self::Map {
                nullable,
                key: Box::new(Self::try_from(value.key_type())?),
                value: Box::new(Self::try_from(value.value_type())?),
            },
            FlussDataType::Row(value) => Self::Row {
                nullable,
                fields: value
                    .fields()
                    .iter()
                    .map(|field| {
                        Ok(RowField {
                            name: field.name.clone(),
                            data_type: Self::try_from(&field.data_type)?,
                            description: field.description.clone(),
                            field_id: field.field_id,
                        })
                    })
                    .collect::<Result<Vec<_>, GatewayError>>()?,
            },
        };
        Ok(converted)
    }
}

impl TryFrom<&DataType> for FlussDataType {
    type Error = GatewayError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        let converted = match value {
            DataType::Boolean { nullable } => Self::Boolean(BooleanType::with_nullable(*nullable)),
            DataType::TinyInt { nullable } => Self::TinyInt(TinyIntType::with_nullable(*nullable)),
            DataType::SmallInt { nullable } => {
                Self::SmallInt(SmallIntType::with_nullable(*nullable))
            }
            DataType::Int { nullable } => Self::Int(IntType::with_nullable(*nullable)),
            DataType::BigInt { nullable } => Self::BigInt(BigIntType::with_nullable(*nullable)),
            DataType::Float { nullable } => Self::Float(FloatType::with_nullable(*nullable)),
            DataType::Double { nullable } => Self::Double(DoubleType::with_nullable(*nullable)),
            DataType::Char { nullable, length } => {
                Self::Char(CharType::with_nullable(*length, *nullable))
            }
            DataType::String { nullable } => Self::String(StringType::with_nullable(*nullable)),
            DataType::Decimal {
                nullable,
                precision,
                scale,
            } => Self::Decimal(
                DecimalType::with_nullable(*nullable, *precision, *scale)
                    .map_err(invalid_native_type)?,
            ),
            DataType::Date { nullable } => Self::Date(DateType::with_nullable(*nullable)),
            DataType::Time {
                nullable,
                precision,
            } => Self::Time(
                TimeType::with_nullable(*nullable, *precision).map_err(invalid_native_type)?,
            ),
            DataType::Timestamp {
                nullable,
                precision,
            } => Self::Timestamp(
                TimestampType::with_nullable(*nullable, *precision).map_err(invalid_native_type)?,
            ),
            DataType::TimestampLtz {
                nullable,
                precision,
            } => Self::TimestampLTz(
                TimestampLTzType::with_nullable(*nullable, *precision)
                    .map_err(invalid_native_type)?,
            ),
            DataType::Bytes { nullable } => Self::Bytes(BytesType::with_nullable(*nullable)),
            DataType::Binary { nullable, length } => {
                if *length == 0 {
                    return Err(GatewayError::invalid_argument(
                        "binary length must be at least one",
                    ));
                }
                Self::Binary(BinaryType::with_nullable(*nullable, *length))
            }
            DataType::Array { nullable, element } => Self::Array(ArrayType::with_nullable(
                *nullable,
                Self::try_from(element.as_ref())?,
            )),
            DataType::Map {
                nullable,
                key,
                value,
            } => {
                if key.nullable() {
                    return Err(GatewayError::invalid_argument(
                        "map key type must not be nullable",
                    ));
                }
                Self::Map(MapType::with_nullable(
                    *nullable,
                    Self::try_from(key.as_ref())?,
                    Self::try_from(value.as_ref())?,
                ))
            }
            DataType::Row { nullable, fields } => Self::Row(RowType::with_nullable(
                *nullable,
                fields
                    .iter()
                    .map(|field| {
                        Ok(DataField::with_field_id(
                            field.name.clone(),
                            Self::try_from(&field.data_type)?,
                            field.description.clone(),
                            field.field_id,
                        ))
                    })
                    .collect::<Result<Vec<_>, GatewayError>>()?,
            )),
        };
        Ok(converted)
    }
}

fn invalid_native_type(error: fluss::error::Error) -> GatewayError {
    GatewayError::invalid_argument(format!("invalid data type: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct ClusterMap {
        clusters: BTreeMap<ClusterId, u8>,
    }

    #[test]
    fn cluster_id_validation_and_serde_are_strict() {
        let id = ClusterId::try_from("analytics_2").unwrap();
        assert_eq!(id.as_str(), "analytics_2");
        let clusters = ClusterMap {
            clusters: BTreeMap::from([(id.clone(), 2)]),
        };
        let encoded = toml::to_string(&clusters).unwrap();
        assert_eq!(toml::from_str::<ClusterMap>(&encoded).unwrap(), clusters);

        for invalid in ["", "Default", "2cluster", "with-hyphen", "white space"] {
            assert!(
                ClusterId::try_from(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
        assert!(ClusterId::try_from("a".repeat(64)).is_err());
    }

    #[test]
    fn recursively_round_trips_native_type_without_losing_nullability() {
        let native = FlussDataType::Row(RowType::with_nullable(
            false,
            vec![DataField::with_field_id(
                "payload",
                FlussDataType::Array(ArrayType::with_nullable(
                    false,
                    FlussDataType::Map(MapType::with_nullable(
                        true,
                        FlussDataType::String(StringType::with_nullable(false)),
                        FlussDataType::Decimal(DecimalType::with_nullable(false, 38, 18).unwrap()),
                    )),
                )),
                Some("nested value".to_string()),
                42,
            )],
        ));

        let domain = DataType::try_from(&native).unwrap();
        assert!(!domain.nullable());
        assert_eq!(FlussDataType::try_from(&domain).unwrap(), native);
    }

    #[test]
    fn rejects_nullable_map_key_in_domain_to_native_conversion() {
        let data_type = DataType::Map {
            nullable: true,
            key: Box::new(DataType::String { nullable: true }),
            value: Box::new(DataType::Int { nullable: true }),
        };

        let error = FlussDataType::try_from(&data_type).unwrap_err();
        assert_eq!(error.kind(), crate::error::ErrorKind::InvalidArgument);
    }
}
