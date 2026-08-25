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

//! The gateway's HTTP-independent backend component.
//!
//! Protocol adapters reach Fluss only through [`FlussBackend`], which is their single dependency: the
//! cluster a request names, the identity it acts as, and the connection that serves it are all resolved
//! below this trait. That boundary is what lets the REST layer be tested without a cluster and lets a
//! second protocol be added without reimplementing any of it.
//!
//! A backend **owns its connections**. Shutdown is a method on the concrete type rather than on this
//! trait, so a protocol adapter cannot reach it.
//!
//! Every capability method is a complete request-response operation: the contract deliberately exposes
//! **no** way to open a stream, scanner, cursor, or any other handle that would outlive the call,
//! because the gateway keeps no request-spanning state. There is also no cluster-health method —
//! reachability is not a queryable state, it is the outcome of the next request.

pub mod client;
pub mod connection;
pub mod context;
pub mod errors;
pub mod types;

#[cfg(test)]
pub mod fake;

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::error::{GatewayError, GatewayResult, Resource};
use async_trait::async_trait;

/// The backend capabilities the protocol adapters depend on.
///
/// Implementations never return HTTP or JSON types: the adapter owns status mapping, and the backend
/// owns the translation from native failures into [`GatewayError`].
///
/// The remaining FIP-49 capabilities — describe table, partitions, DDL, write, lookup — are appended
/// here in the same shape, so carrying them changes neither the connection layer nor the protocol layer.
#[async_trait]
pub trait FlussBackend: Send + Sync + 'static {
    /// The clusters this gateway serves, in lexical ID order.
    ///
    /// A configuration echo, which is why it is synchronous, cannot fail, and takes no request.
    fn clusters(&self) -> Vec<ClusterId>;

    /// Whether `id` names a cluster this gateway serves. Allocation-free, for per-request validation.
    fn has_cluster(&self, id: &str) -> bool;

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>>;

    async fn list_tables(&self, ctx: &RequestContext, database: &str)
    -> GatewayResult<Vec<String>>;
}

/// The answer to a cluster this gateway does not serve.
///
/// A malformed ID and an unconfigured one are the same answer to a caller, so they share one error.
pub fn unknown_cluster(id: &str) -> GatewayError {
    GatewayError::not_found(format!("unknown cluster `{id}`")).with_resource(Resource::Cluster)
}
