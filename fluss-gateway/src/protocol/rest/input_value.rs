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

//! Values accepted by protocol adapters before schema-aware validation.

/// One protocol-neutral input value.
///
/// Numbers retain their original text so large integers and decimals are never rounded through
/// binary floating point. Objects retain insertion order and duplicate names so schema-aware
/// validation can distinguish duplicates, missing fields, and explicit nulls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputValue {
    Null,
    Boolean(bool),
    ExactNumber(String),
    String(String),
    Array(Vec<InputValue>),
    Object(Vec<(String, InputValue)>),
}

impl InputValue {
    /// Returns an object field only when it occurs exactly once.
    ///
    /// The full ordered entries remain available to callers that need to report duplicate fields.
    pub fn unique_field(&self, name: &str) -> Option<&InputValue> {
        let InputValue::Object(entries) = self else {
            return None;
        };
        let mut matches = entries
            .iter()
            .filter_map(|(entry_name, value)| (entry_name == name).then_some(value));
        let value = matches.next()?;
        matches.next().is_none().then_some(value)
    }

    /// Returns the ordered object entries without discarding duplicate names.
    pub fn object_entries(&self) -> Option<&[(String, InputValue)]> {
        match self {
            Self::Object(entries) => Some(entries),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_exact_numbers_and_ordered_duplicate_fields() {
        let value = InputValue::Object(vec![
            (
                "amount".to_string(),
                InputValue::ExactNumber("9007199254740993.000000000000000001".to_string()),
            ),
            (
                "amount".to_string(),
                InputValue::ExactNumber("2".to_string()),
            ),
        ]);

        assert_eq!(
            value,
            InputValue::Object(vec![
                (
                    "amount".to_string(),
                    InputValue::ExactNumber("9007199254740993.000000000000000001".to_string()),
                ),
                (
                    "amount".to_string(),
                    InputValue::ExactNumber("2".to_string())
                ),
            ])
        );
        assert_eq!(value.unique_field("amount"), None);
    }

    #[test]
    fn distinguishes_missing_and_explicit_null() {
        let value = InputValue::Object(vec![("present".to_string(), InputValue::Null)]);

        assert_eq!(value.unique_field("present"), Some(&InputValue::Null));
        assert_eq!(value.unique_field("missing"), None);
    }
}
