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

//! Syntax-only JSON decoding for row input.
//!
//! The normal `serde_json::Value` representation rounds some numbers and collapses duplicate
//! object fields. Writes instead retain number lexemes and ordered object entries until the
//! application layer validates them against the authoritative Fluss schema.

use crate::error::GatewayError;
use crate::protocol::rest::input_value::InputValue;
use std::collections::HashSet;

const MAX_NESTING_DEPTH: usize = 128;

/// Parses one complete JSON value without schema-dependent coercion.
pub fn parse_input_value(input: &[u8]) -> Result<InputValue, GatewayError> {
    let mut parser = Parser { input, offset: 0 };
    let value = parser.parse_value(0)?;
    parser.skip_whitespace();
    if parser.offset != input.len() {
        return Err(parser.error("trailing characters after the JSON value"));
    }
    Ok(value)
}

/// Syntax-level write operation whose row remains schema neutral.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteInputOperation {
    Append(InputValue),
    Upsert(InputValue),
    Delete(InputValue),
}

/// One parsed write entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteInputEntry {
    pub id: String,
    pub operation: WriteInputOperation,
}

/// Parsed write envelope passed to application preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteInputRequest {
    pub partial_update_columns: Option<Vec<String>>,
    pub entries: Vec<WriteInputEntry>,
}

/// Parses the stable REST write envelope while retaining each operation row as [`InputValue`].
pub fn parse_write_input(input: &[u8]) -> Result<WriteInputRequest, GatewayError> {
    let root = parse_input_value(input)?;
    let InputValue::Object(fields) = root else {
        return Err(GatewayError::invalid_argument(
            "write request must be a JSON object",
        ));
    };
    ensure_unique_known_fields(
        &fields,
        &["partial_update_columns", "entries"],
        "write request",
    )?;
    let partial_update_columns = match field(&fields, "partial_update_columns") {
        None => None,
        Some(InputValue::Array(values)) => Some(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| match value {
                    InputValue::String(value) => Ok(value.clone()),
                    _ => Err(GatewayError::invalid_argument(format!(
                        "`partial_update_columns[{index}]` must be a string"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => {
            return Err(GatewayError::invalid_argument(
                "`partial_update_columns` must be an array of strings",
            ));
        }
    };
    let Some(InputValue::Array(entries)) = field(&fields, "entries") else {
        return Err(GatewayError::invalid_argument(
            "`entries` is required and must be an array",
        ));
    };
    let entries = entries
        .iter()
        .enumerate()
        .map(parse_write_entry)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(WriteInputRequest {
        partial_update_columns,
        entries,
    })
}

fn parse_write_entry(
    (index, value): (usize, &InputValue),
) -> Result<WriteInputEntry, GatewayError> {
    let InputValue::Object(fields) = value else {
        return Err(GatewayError::invalid_argument(format!(
            "`entries[{index}]` must be an object"
        )));
    };
    ensure_unique_known_fields(fields, &["id", "append", "upsert", "delete"], "write entry")?;
    let id = match field(fields, "id") {
        Some(InputValue::String(id)) => id.clone(),
        Some(_) => {
            return Err(GatewayError::invalid_argument(format!(
                "`entries[{index}].id` must be a string"
            )));
        }
        None => {
            return Err(GatewayError::invalid_argument(format!(
                "`entries[{index}].id` is required"
            )));
        }
    };
    let operations = ["append", "upsert", "delete"]
        .into_iter()
        .filter_map(|name| field(fields, name).map(|row| (name, row)))
        .collect::<Vec<_>>();
    if operations.len() != 1 {
        return Err(GatewayError::invalid_argument(format!(
            "`entries[{index}]` must contain exactly one of `append`, `upsert`, or `delete`"
        )));
    }
    let (name, row) = operations[0];
    if !matches!(row, InputValue::Object(_)) {
        return Err(GatewayError::invalid_argument(format!(
            "`entries[{index}].{name}` must be an object"
        )));
    }
    let operation = match name {
        "append" => WriteInputOperation::Append(row.clone()),
        "upsert" => WriteInputOperation::Upsert(row.clone()),
        "delete" => WriteInputOperation::Delete(row.clone()),
        _ => unreachable!("operation names are fixed"),
    };
    Ok(WriteInputEntry { id, operation })
}

fn ensure_unique_known_fields(
    fields: &[(String, InputValue)],
    known: &[&str],
    context: &str,
) -> Result<(), GatewayError> {
    let mut seen = HashSet::new();
    for (name, _) in fields {
        if !known.contains(&name.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "unknown field `{name}` in {context}"
            )));
        }
        if !seen.insert(name) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate field `{name}` in {context}"
            )));
        }
    }
    Ok(())
}

fn field<'a>(fields: &'a [(String, InputValue)], name: &str) -> Option<&'a InputValue> {
    fields
        .iter()
        .find_map(|(field_name, value)| (field_name == name).then_some(value))
}

struct Parser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl Parser<'_> {
    fn parse_value(&mut self, depth: usize) -> Result<InputValue, GatewayError> {
        if depth > MAX_NESTING_DEPTH {
            return Err(self.error("JSON nesting exceeds 128 levels"));
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'n') => {
                self.literal(b"null")?;
                Ok(InputValue::Null)
            }
            Some(b't') => {
                self.literal(b"true")?;
                Ok(InputValue::Boolean(true))
            }
            Some(b'f') => {
                self.literal(b"false")?;
                Ok(InputValue::Boolean(false))
            }
            Some(b'"') => self.parse_string().map(InputValue::String),
            Some(b'[') => self.parse_array(depth + 1),
            Some(b'{') => self.parse_object(depth + 1),
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(InputValue::ExactNumber),
            Some(_) => Err(self.error("expected a JSON value")),
            None => Err(self.error("expected a JSON value")),
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<InputValue, GatewayError> {
        self.offset += 1;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.consume(b']') {
            return Ok(InputValue::Array(values));
        }
        loop {
            values.push(self.parse_value(depth)?);
            self.skip_whitespace();
            if self.consume(b']') {
                return Ok(InputValue::Array(values));
            }
            self.expect(b',', "expected `,` or `]` in JSON array")?;
        }
    }

    fn parse_object(&mut self, depth: usize) -> Result<InputValue, GatewayError> {
        self.offset += 1;
        self.skip_whitespace();
        let mut entries = Vec::new();
        if self.consume(b'}') {
            return Ok(InputValue::Object(entries));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.error("expected a string field name in JSON object"));
            }
            let name = self.parse_string()?;
            self.skip_whitespace();
            self.expect(b':', "expected `:` after JSON field name")?;
            let value = self.parse_value(depth)?;
            entries.push((name, value));
            self.skip_whitespace();
            if self.consume(b'}') {
                return Ok(InputValue::Object(entries));
            }
            self.expect(b',', "expected `,` or `}` in JSON object")?;
        }
    }

    fn parse_string(&mut self) -> Result<String, GatewayError> {
        let start = self.offset;
        self.offset += 1;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.offset += 1;
            if escaped {
                escaped = false;
                continue;
            }
            match byte {
                b'\\' => escaped = true,
                b'"' => {
                    return serde_json::from_slice(&self.input[start..self.offset])
                        .map_err(|error| self.error(format!("invalid JSON string: {error}")));
                }
                0x00..=0x1f => return Err(self.error("unescaped control byte in JSON string")),
                _ => {}
            }
        }
        Err(self.error("unterminated JSON string"))
    }

    fn parse_number(&mut self) -> Result<String, GatewayError> {
        let start = self.offset;
        self.consume(b'-');
        match self.peek() {
            Some(b'0') => {
                self.offset += 1;
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    return Err(self.error("leading zero in JSON number"));
                }
            }
            Some(b'1'..=b'9') => self.consume_digits(),
            _ => return Err(self.error("invalid JSON number")),
        }
        if self.consume(b'.') {
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("JSON number fraction requires a digit"));
            }
            self.consume_digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.offset += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.offset += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("JSON number exponent requires a digit"));
            }
            self.consume_digits();
        }
        String::from_utf8(self.input[start..self.offset].to_vec())
            .map_err(|_| self.error("JSON number is not UTF-8"))
    }

    fn consume_digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.offset += 1;
        }
    }

    fn literal(&mut self, expected: &[u8]) -> Result<(), GatewayError> {
        if self.input.get(self.offset..self.offset + expected.len()) == Some(expected) {
            self.offset += expected.len();
            Ok(())
        } else {
            Err(self.error("invalid JSON literal"))
        }
    }

    fn expect(&mut self, byte: u8, message: &str) -> Result<(), GatewayError> {
        self.skip_whitespace();
        if self.consume(byte) {
            Ok(())
        } else {
            Err(self.error(message))
        }
    }

    fn consume(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
    }

    fn error(&self, message: impl AsRef<str>) -> GatewayError {
        GatewayError::invalid_argument(format!(
            "invalid JSON at byte {}: {}",
            self.offset,
            message.as_ref()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_exact_numbers_and_ordered_duplicate_fields() {
        let value = parse_input_value(
            br#"{"z":9007199254740993.000000000000000001,"z":1.2300e+42,"nested":[null,true]}"#,
        )
        .unwrap();
        assert_eq!(
            value,
            InputValue::Object(vec![
                (
                    "z".to_string(),
                    InputValue::ExactNumber("9007199254740993.000000000000000001".to_string())
                ),
                (
                    "z".to_string(),
                    InputValue::ExactNumber("1.2300e+42".to_string())
                ),
                (
                    "nested".to_string(),
                    InputValue::Array(vec![InputValue::Null, InputValue::Boolean(true)])
                ),
            ])
        );
    }

    #[test]
    fn decodes_string_escapes_without_accepting_invalid_json() {
        assert_eq!(
            parse_input_value(br#""line\n\u03bb""#).unwrap(),
            InputValue::String("line\nλ".to_string())
        );
        for input in [
            b"01".as_slice(),
            b"1.".as_slice(),
            b"1e".as_slice(),
            br#"{"a":1,}"#.as_slice(),
            br#"[1 2]"#.as_slice(),
            b"true false".as_slice(),
        ] {
            assert!(parse_input_value(input).is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn rejects_excessive_nesting() {
        let input = format!("{}0{}", "[".repeat(130), "]".repeat(130));
        assert!(parse_input_value(input.as_bytes()).is_err());
    }

    #[test]
    fn parses_write_envelope_without_coercing_rows() {
        let request = parse_write_input(
            br#"{"partial_update_columns":["id","value"],"entries":[{"id":"a","upsert":{"id":9007199254740993,"value":null}},{"id":"b","delete":{"id":"2"}}]}"#,
        )
        .unwrap();
        assert_eq!(
            request.partial_update_columns,
            Some(vec!["id".to_string(), "value".to_string()])
        );
        assert_eq!(request.entries.len(), 2);
        assert_eq!(request.entries[0].id, "a");
        assert!(matches!(
            &request.entries[0].operation,
            WriteInputOperation::Upsert(InputValue::Object(fields))
                if fields[0].1 == InputValue::ExactNumber("9007199254740993".to_string())
        ));
    }

    #[test]
    fn rejects_ambiguous_or_unknown_write_fields() {
        for input in [
            br#"{"entries":[{"id":"a","append":{},"upsert":{}}]}"#.as_slice(),
            br#"{"entries":[{"id":"a","upsert":{},"extra":1}]}"#.as_slice(),
            br#"{"entries":[{"id":"a","id":"b","upsert":{}}]}"#.as_slice(),
            br#"{"entries":[{"id":"a","upsert":1}]}"#.as_slice(),
        ] {
            assert!(parse_write_input(input).is_err(), "accepted {input:?}");
        }
    }
}
