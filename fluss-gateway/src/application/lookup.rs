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

//! The lookup half of [`GatewayService`]: batched primary-key lookup and bounded prefix lookup.
//!
//! Neither method is implemented yet; both report an unsupported operation. Their signatures are final.
//!
//! The two endpoints differ in how "nothing matched" is reported. A point lookup answers per key with an explicit
//! not-found outcome, because a primary key names at most one row. A prefix lookup answers per prefix with a
//! zero-row batch, because an empty range is a normal result rather than a missing resource.

use crate::application::{GatewayService, RequestContext};
use crate::backend::model::{
    LookupKey, LookupOutcome, PrefixLookupOutcome, PrefixLookupRequest, TableRef,
};
use crate::error::GatewayError;

/// The two lookup paths.
///
/// One of several inherent `impl GatewayService` blocks; see [`crate::application::service`].
impl GatewayService {
    /// Looks up rows by primary key, returning exactly one outcome per input key in input order.
    ///
    /// Keys carry values in logical primary-key order, partition key columns included. A miss is an outcome,
    /// never an error.
    pub async fn lookup(
        &self,
        _context: &RequestContext,
        _table: &TableRef,
        _keys: Vec<LookupKey>,
    ) -> Result<Vec<LookupOutcome>, GatewayError> {
        Err(GatewayError::unsupported(
            "primary-key lookup is not implemented yet",
        ))
    }

    /// Looks up rows by key prefix, returning exactly one outcome per input prefix in input order.
    ///
    /// The prefix columns must cover the table's bucket keys so each prefix routes to a single bucket. Results
    /// are truncated at `request.max_rows_per_prefix` and flagged when they are.
    pub async fn prefix_lookup(
        &self,
        _context: &RequestContext,
        _table: &TableRef,
        _request: PrefixLookupRequest,
    ) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
        Err(GatewayError::unsupported(
            "prefix lookup is not implemented yet",
        ))
    }
}
