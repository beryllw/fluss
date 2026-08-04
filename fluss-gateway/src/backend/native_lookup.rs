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

//! Native execution of the two lookup operations.
//!
//! Both entry points are the only place the gateway touches `Lookuper` and `PrefixKeyLookuper`. Neither is
//! implemented yet; both report an unsupported operation. The signatures are final:
//! [`crate::backend::native::NativeGatewayBackend`] delegates to them unchanged.
//!
//! Two native facts shape the contract. `Lookuper::lookup` and `PrefixKeyLookuper::lookup` both take `&mut self`,
//! so a per-table lookuper pool guarded by a mutex is required — a performance cache, never correctness state.
//! And `PrefixKeyLookuper::lookup` takes exactly one prefix per call with no row bound, which is why
//! `max_rows_per_prefix` is applied here as truncation with a `truncated` flag rather than pushed to the server.

use crate::backend::model::{
    LookupKey, LookupOutcome, PrefixLookupOutcome, PrefixLookupRequest, TableRef,
};
use crate::error::GatewayError;
use fluss::client::FlussConnection;
use std::sync::Arc;

/// Looks up rows by primary key, returning one outcome per key in input order.
pub(crate) async fn lookup(
    _connection: &Arc<FlussConnection>,
    _table: &TableRef,
    _keys: Vec<LookupKey>,
    _max_concurrent: usize,
) -> Result<Vec<LookupOutcome>, GatewayError> {
    Err(GatewayError::unsupported(
        "the gateway cannot look up rows yet: the native lookup backend is not implemented",
    ))
}

/// Looks up rows by key prefix, returning one outcome per prefix in input order.
pub(crate) async fn prefix_lookup(
    _connection: &Arc<FlussConnection>,
    _table: &TableRef,
    _request: PrefixLookupRequest,
    _max_concurrent: usize,
) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
    Err(GatewayError::unsupported(
        "the gateway cannot run prefix lookups yet: the native lookup backend is not implemented",
    ))
}
