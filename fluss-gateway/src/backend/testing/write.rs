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

//! Recorded write execution behind [`super::TestBackend`].

use crate::backend::model::{
    PreparedWriteRequest, TableRef, WriteEntryResult, WriteFailure, WriteResult,
};
use crate::backend::testing::TestBackend;
use crate::error::GatewayError;

/// One entry the fixture backend accepted, in submission order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWrite {
    pub table: TableRef,
    pub input_index: usize,
    pub id: String,
    /// `append`, `upsert`, or `delete`.
    pub operation: &'static str,
    pub partial_update_columns: Option<Vec<String>>,
}

/// Applies one preflighted request, honouring an injected delivery failure and a staged schema change.
pub(crate) fn execute(
    backend: &TestBackend,
    request: PreparedWriteRequest,
) -> Result<WriteResult, GatewayError> {
    let mut state = backend.state.lock();

    if std::mem::take(&mut state.evolve_schema_before_next_write) {
        state.catalog.bump_schema_id(&request.table);
    }

    let current = state
        .catalog
        .table(&request.table)
        .ok_or_else(|| GatewayError::not_found("the requested table does not exist"))?;
    if current.table_id != request.expected_table_id
        || current.schema_id != request.expected_schema_id
    {
        return Err(GatewayError::failed_precondition(
            "the table schema changed between preflight and submission",
        ));
    }

    let injected = state.injected_write_failure.take();
    let mut entries = Vec::with_capacity(request.entries.len());
    for entry in &request.entries {
        state.writes.push(RecordedWrite {
            table: request.table.clone(),
            input_index: entry.input_index,
            id: entry.id.clone(),
            operation: entry.operation.name(),
            partial_update_columns: request.partial_update_columns.clone(),
        });
        let failure = injected.as_ref().and_then(|injected| {
            injected
                .input_indexes
                .contains(&entry.input_index)
                .then(|| WriteFailure {
                    error_code: injected.error_code.clone(),
                    message: "injected test failure".to_string(),
                    completion: injected.completion,
                    retryable: injected.retryable,
                })
        });
        entries.push(match failure {
            Some(failure) => {
                WriteEntryResult::failure(entry.input_index, entry.id.clone(), failure)
            }
            None => WriteEntryResult::success(entry.input_index, entry.id.clone()),
        });
    }
    Ok(WriteResult { entries })
}
