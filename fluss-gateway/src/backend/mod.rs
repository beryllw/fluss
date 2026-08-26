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

//! HTTP-independent Fluss operations used by protocol adapters.

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
use fluss::metadata::{
    AlterTableChanges, PartitionInfo, PartitionSpec, TableDescriptor, TableInfo, TablePath,
};

/// The backend capabilities the protocol adapters depend on.
///
/// Native metadata crosses this boundary unchanged; protocol adapters own wire shapes.
#[async_trait]
pub trait FlussBackend: Send + Sync + 'static {
    /// The configured clusters, in lexical order.
    fn clusters(&self) -> Vec<ClusterId>;

    /// Whether `id` names a configured cluster.
    fn has_cluster(&self, id: &str) -> bool;

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>>;

    async fn create_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()>;

    /// Drops an empty database without cascading.
    async fn drop_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()>;

    async fn list_tables(&self, ctx: &RequestContext, database: &str)
    -> GatewayResult<Vec<String>>;

    async fn describe_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo>;

    async fn create_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        descriptor: &TableDescriptor,
    ) -> GatewayResult<()>;

    /// Applies the change group in one native request.
    async fn alter_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        changes: AlterTableChanges,
    ) -> GatewayResult<()>;

    async fn drop_table(&self, ctx: &RequestContext, table: &TablePath) -> GatewayResult<()>;

    async fn list_partitions(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<Vec<PartitionInfo>>;

    async fn create_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()>;

    async fn drop_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()>;
}

/// Returns the error for an unconfigured cluster.
pub fn unknown_cluster(id: &str) -> GatewayError {
    GatewayError::not_found(format!("unknown cluster `{id}`")).with_resource(Resource::Cluster)
}
