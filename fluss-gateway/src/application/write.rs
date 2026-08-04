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

//! Protocol-neutral write models and the write half of [`GatewayService`].
//!
//! The models below are final. All-or-nothing preflight and native dispatch are not implemented yet, so
//! [`GatewayService::write`] reports an unsupported operation.
//!
//! Two invariants are fixed by design and are not configurable per request: preflight is all-or-nothing, so a
//! validation failure rejects the whole batch before any row is submitted; and every entry carries a finite
//! delivery deadline derived from `[write] max_delivery_time`, so a write can never outlive its HTTP request.

use crate::application::{GatewayService, InputValue, RequestContext};
use crate::backend::model::{TableRef, WriteResult};
use crate::error::GatewayError;

/// One write request before schema-aware validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRequest {
    pub table: TableRef,
    /// Columns an upsert batch targets. `None` means every column is supplied.
    pub partial_update_columns: Option<Vec<String>>,
    pub entries: Vec<WriteEntry>,
}

/// One entry identified by an opaque caller correlation value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEntry {
    pub id: String,
    pub operation: WriteOperation,
}

/// Exactly one table mutation and its untyped protocol-neutral row object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOperation {
    Append(InputValue),
    Upsert(InputValue),
    Delete(InputValue),
}

impl WriteOperation {
    /// Stable operation name used in messages and deterministic recordings.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Append(_) => "append",
            Self::Upsert(_) => "upsert",
            Self::Delete(_) => "delete",
        }
    }

    /// The untyped row object carried by this operation.
    pub fn row(&self) -> &InputValue {
        match self {
            Self::Append(row) | Self::Upsert(row) | Self::Delete(row) => row,
        }
    }
}

/// The batch write path.
///
/// One of several inherent `impl GatewayService` blocks; see [`crate::application::service`].
impl GatewayService {
    /// Validates and decodes the complete request before submitting its first row.
    ///
    /// Unlike read operations, the native acknowledgement phase is not wrapped in the request
    /// deadline. Each row carries an earlier delivery deadline, and the backend returns
    /// completion-unknown entry outcomes after ownership rather than a request-level timeout.
    pub async fn write(
        &self,
        _context: &RequestContext,
        _request: WriteRequest,
    ) -> Result<WriteResult, GatewayError> {
        Err(GatewayError::unsupported(
            "the write path is not implemented yet",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_expose_a_stable_name_and_their_row() {
        let row = InputValue::Object(vec![("id".to_string(), InputValue::Null)]);
        for (operation, name) in [
            (WriteOperation::Append(row.clone()), "append"),
            (WriteOperation::Upsert(row.clone()), "upsert"),
            (WriteOperation::Delete(row.clone()), "delete"),
        ] {
            assert_eq!(operation.name(), name);
            assert_eq!(operation.row(), &row);
        }
    }
}
