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

//! Lookup behaviour behind [`super::TestBackend`].
//!
//! The fixture holds no rows yet, so every key misses and every prefix matches an empty range. Both shapes are
//! still exercised end to end: the point lookup produces one `NotFound` outcome per key, and the prefix lookup
//! produces one zero-row batch per prefix — never a not-found variant, because an empty range is a normal
//! answer.

use crate::backend::model::{
    LookupKey, LookupOutcome, LookupOutcomeKind, PrefixLookupOutcome, PrefixLookupRequest,
    PrefixOutcomeKind, TableRef,
};
use crate::backend::testing::TestBackend;
use crate::error::GatewayError;
use arrow::array::RecordBatch;

/// Returns one outcome per input key, in input order.
pub(crate) fn lookup(
    backend: &TestBackend,
    table: &TableRef,
    keys: Vec<LookupKey>,
) -> Result<Vec<LookupOutcome>, GatewayError> {
    let _description = describe(backend, table)?;
    Ok(keys
        .into_iter()
        .enumerate()
        .map(|(input_index, _key)| LookupOutcome {
            input_index,
            kind: LookupOutcomeKind::NotFound,
        })
        .collect())
}

/// Returns one outcome per input prefix, in input order.
pub(crate) fn prefix_lookup(
    backend: &TestBackend,
    table: &TableRef,
    request: PrefixLookupRequest,
) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
    let description = describe(backend, table)?;
    let empty = RecordBatch::new_empty(description.arrow_schema.clone());
    Ok(request
        .prefixes
        .into_iter()
        .enumerate()
        .map(|(input_index, _prefix)| PrefixLookupOutcome {
            input_index,
            kind: PrefixOutcomeKind::Rows {
                batch: empty.clone(),
                truncated: false,
            },
        })
        .collect())
}

fn describe(
    backend: &TestBackend,
    table: &TableRef,
) -> Result<std::sync::Arc<crate::backend::model::TableDescription>, GatewayError> {
    backend
        .state
        .lock()
        .catalog
        .table(table)
        .ok_or_else(|| GatewayError::not_found(format!("table `{table}` does not exist")))
}
