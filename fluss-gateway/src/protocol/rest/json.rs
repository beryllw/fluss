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
//! One conversion serves the whole REST surface. Rows read from the backend arrive as Arrow record batches and
//! are rendered to JSON here. Key values travel the other way and are parsed against the Arrow column type
//! before native encoding.
//!
//! The mapping this module must implement:
//!
//! - BOOLEAN and TINYINT through INT are JSON booleans and numbers
//! - BIGINT and DECIMAL are base-10 strings to avoid IEEE-754 loss
//! - FLOAT and DOUBLE are numbers, with non-finite values rendered as the strings `"NaN"`, `"Infinity"` and
//!   `"-Infinity"`
//! - CHAR and STRING are strings
//! - BINARY and BYTES are base64 strings
//! - DATE, TIME and both TIMESTAMP kinds are ISO-8601 strings, where TIMESTAMP stays zone free and TIMESTAMP_LTZ
//!   is UTC with a `Z`
//! - ARRAY and ROW recurse, MAP is an ordered array of `{key, value}` entries
//! - NULL is JSON null
//!
//! None of it is implemented yet: every entry point reports an unsupported operation. The signatures are final.

use crate::backend::model::KeyValue;
use crate::error::GatewayError;
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType as ArrowDataType;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Renders every row of a record batch as a JSON object keyed by column name.
pub fn record_batch_to_json_rows(
    _batch: &RecordBatch,
) -> Result<Vec<JsonMap<String, JsonValue>>, GatewayError> {
    Err(GatewayError::unsupported(
        "JSON row rendering is not implemented yet",
    ))
}

/// Renders one Arrow array element as a JSON value.
pub fn value_to_json(_array: &dyn Array, _index: usize) -> Result<JsonValue, GatewayError> {
    Err(GatewayError::unsupported(
        "JSON value rendering is not implemented yet",
    ))
}

/// Parses one JSON key value against the Arrow column type.
///
/// Lossy coercions must be rejected with an error naming the column and the expected type. BIGINT and DECIMAL
/// accept both a JSON string and an exact-integer JSON number.
pub fn parse_key_value(
    _column: &str,
    _data_type: &ArrowDataType,
    _value: &JsonValue,
) -> Result<KeyValue, GatewayError> {
    Err(GatewayError::unsupported(
        "JSON key parsing is not implemented yet",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn every_entry_point_reports_an_unsupported_operation() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let array = Arc::new(Int32Array::from(vec![1]));
        let batch = RecordBatch::try_new(schema, vec![array.clone()]).unwrap();

        assert_eq!(
            record_batch_to_json_rows(&batch).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            value_to_json(array.as_ref(), 0).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            parse_key_value("id", &ArrowDataType::Int32, &JsonValue::from(1))
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
    }
}
