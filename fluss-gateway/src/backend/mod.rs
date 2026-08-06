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

//! The gateway's HTTP-independent backend contract.
//!
//! Protocol adapters call [`GatewayBackend`]. Only the native implementation ([`native::NativeGatewayBackend`])
//! touches `fluss-rs`. The trait covers catalog metadata, ordered batch primary-key lookup, bounded prefix
//! lookup, ordered writes, and cluster health.
//!
//! Every method is a complete request-response operation. The trait deliberately exposes **no** way to open a
//! stream, scanner, cursor, or any other handle that would outlive the call, because the gateway keeps no
//! request-spanning state.

pub mod context;
pub mod identity;
pub mod metadata_cache;
pub mod model;
pub mod native;
mod native_lookup;
mod native_write;
pub mod registry;
pub mod resilient;
pub mod types;

#[cfg(any(test, feature = "test-backend"))]
pub mod testing;

use crate::application::ddl::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, PartitionMutationRequest,
};
use crate::error::GatewayError;
use async_trait::async_trait;
use model::{
    ClusterHealthReport, DatabaseDescription, LookupKey, LookupOutcome, PartitionDescription,
    PrefixLookupOutcome, PrefixLookupRequest, PreparedWriteRequest, TableDescription, TableRef,
    WriteResult,
};
use std::sync::Arc;
use std::time::Duration;

/// Backend operations needed by the REST surface.
///
/// Implementations never return HTTP or JSON types. Adapters own status mapping.
#[async_trait]
pub trait GatewayBackend: Send + Sync + 'static {
    /// Lists all database names.
    async fn list_databases(&self) -> Result<Vec<String>, GatewayError>;

    /// Describes one database.
    async fn describe_database(&self, database: &str) -> Result<DatabaseDescription, GatewayError>;

    /// Creates one database and fails when it already exists.
    async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<(), GatewayError>;

    /// Drops one empty database and fails when it does not exist.
    async fn drop_database(&self, database: &str) -> Result<(), GatewayError>;

    /// Lists all table names in a database.
    async fn list_tables(&self, database: &str) -> Result<Vec<String>, GatewayError>;

    /// Describes one table: schema, keys, distribution, partitioning, table kind, and derived capabilities.
    async fn describe_table(&self, table: &TableRef)
    -> Result<Arc<TableDescription>, GatewayError>;

    /// Creates one table and fails when it already exists.
    async fn create_table(&self, request: &CreateTableRequest) -> Result<(), GatewayError>;

    /// Applies all table changes in one native request.
    async fn alter_table(&self, request: &AlterTableRequest) -> Result<(), GatewayError>;

    /// Drops one table and fails when it does not exist.
    async fn drop_table(&self, table: &TableRef) -> Result<(), GatewayError>;

    /// Lists partitions of a partitioned table.
    async fn list_partitions(
        &self,
        table: &TableRef,
    ) -> Result<Vec<PartitionDescription>, GatewayError>;

    /// Creates one exact partition and fails when it already exists.
    async fn create_partition(
        &self,
        request: &PartitionMutationRequest,
    ) -> Result<(), GatewayError>;

    /// Drops one exact partition and fails when it does not exist.
    async fn drop_partition(&self, request: &PartitionMutationRequest) -> Result<(), GatewayError>;

    /// Looks up rows by primary key, one outcome per input key.
    ///
    /// Keys carry values in logical primary-key order, partition key columns included, already validated against
    /// the table schema by the adapter. The REST layer enforces the `exact_lookup_supported` capability before
    /// calling, so implementations are not required to re-check it. The returned vector has exactly one entry per
    /// input key, ordered by `input_index`. A missing row is a [`model::LookupOutcomeKind::NotFound`] outcome and
    /// never an error. Per-key failures are recorded in their own outcome so the rest of the batch still completes.
    /// Only a failure that prevents the whole batch from running returns `Err`.
    async fn lookup(
        &self,
        table: &TableRef,
        keys: Vec<LookupKey>,
    ) -> Result<Vec<LookupOutcome>, GatewayError>;

    /// Looks up rows by key prefix, one outcome per input prefix.
    ///
    /// The adapter has already validated that `request.prefix_columns` covers the table's bucket keys and that
    /// the table reports `prefix_lookup_supported`. The returned vector has exactly one entry per input prefix,
    /// ordered by `input_index`. A prefix that matches nothing yields
    /// [`model::PrefixOutcomeKind::Rows`] with a zero-row batch, never a not-found variant. Implementations
    /// truncate each prefix at `request.max_rows_per_prefix` and set `truncated` when they do, because the
    /// native prefix lookuper returns every matching row. Per-prefix failures are recorded in their own outcome;
    /// only a failure that prevents the whole batch from running returns `Err`.
    async fn prefix_lookup(
        &self,
        table: &TableRef,
        request: PrefixLookupRequest,
    ) -> Result<Vec<PrefixLookupOutcome>, GatewayError>;

    /// Submits a fully preflighted sequence of row mutations in input order.
    ///
    /// A request-level error is permitted only before any row is accepted by the client writer.
    /// Once submission begins, every row receives an explicit success, rejected, or
    /// completion-unknown verdict in [`WriteResult`].
    async fn write(&self, request: PreparedWriteRequest) -> Result<WriteResult, GatewayError>;

    /// Reports Fluss cluster health. An `Err` means the backend could not be reached at all. Adapters render that as
    /// `UNKNOWN`.
    async fn cluster_health(&self) -> Result<ClusterHealthReport, GatewayError>;

    /// Closes backend-owned resources. Implementations must be idempotent.
    async fn close(&self, _timeout: Duration) -> Result<(), GatewayError> {
        Ok(())
    }
}
