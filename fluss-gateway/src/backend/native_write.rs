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

//! Native execution of one preflighted write request.
//!
//! This is the only place the gateway touches `AppendWriter` and `UpsertWriter`. It is not implemented yet and
//! reports an unsupported operation. The signature is final:
//! [`crate::backend::native::NativeGatewayBackend`] delegates to it unchanged.
//!
//! The contract it must honour: a request-level `Err` is permitted only before the first row is accepted by the
//! client writer. Once submission begins, every entry gets an explicit success, rejected, or completion-unknown
//! verdict, and the per-entry `delivery_deadline` carried by the request bounds the whole submission.

use crate::backend::model::{PreparedWriteRequest, WriteResult};
use crate::error::GatewayError;
use fluss::client::FlussConnection;
use std::sync::Arc;

/// Submits every entry of a preflighted request in input order and collects per-entry verdicts.
pub(crate) async fn execute(
    _connection: &Arc<FlussConnection>,
    _request: PreparedWriteRequest,
) -> Result<WriteResult, GatewayError> {
    Err(GatewayError::unsupported(
        "the gateway cannot write rows yet: the native write backend is not implemented",
    ))
}
