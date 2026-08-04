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

//! Deterministic in-memory [`GatewayBackend`] for protocol and lifecycle tests.
//!
//! [`TestBackend`] needs no Fluss cluster. It is compiled only under `cfg(test)` or the `test-backend` feature
//! and is never part of a shippable runtime path.
//!
//! The single `impl GatewayBackend` block lives here and does nothing but delegate, so the fixture behaviour can
//! grow in [`catalog`], [`write`], and [`lookup`] without any workstream touching this file.

pub mod catalog;
pub mod lookup;
pub mod write;

use crate::application::ddl::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, PartitionMutationRequest,
};
use crate::backend::GatewayBackend;
use crate::backend::model::{
    ClusterHealthReport, ClusterStatus, DatabaseDescription, LookupKey, LookupOutcome,
    PartitionDescription, PrefixLookupOutcome, PrefixLookupRequest, PreparedWriteRequest,
    TableDescription, TableRef, WriteCompletion, WriteResult,
};
use crate::error::GatewayError;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

pub use write::RecordedWrite;

/// One injected delivery failure applied to the next write request.
#[derive(Debug, Clone)]
pub(crate) struct InjectedWriteFailure {
    pub(crate) input_indexes: Vec<usize>,
    pub(crate) completion: WriteCompletion,
    pub(crate) error_code: String,
    pub(crate) retryable: bool,
}

/// Mutable fixture state shared by every operation.
pub(crate) struct TestState {
    pub(crate) catalog: catalog::Catalog,
    pub(crate) available: bool,
    pub(crate) cluster_health_hanging: bool,
    pub(crate) writes: Vec<RecordedWrite>,
    pub(crate) injected_write_failure: Option<InjectedWriteFailure>,
    pub(crate) evolve_schema_before_next_write: bool,
    pub(crate) closed: bool,
}

/// Fixture backend with deterministic catalog contents and recorded mutations.
pub struct TestBackend {
    pub(crate) state: Mutex<TestState>,
}

impl Default for TestBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl TestBackend {
    /// Builds a backend preloaded with the standard fixture catalog.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TestState {
                catalog: catalog::Catalog::fixture(),
                available: true,
                cluster_health_hanging: false,
                writes: Vec::new(),
                injected_write_failure: None,
                evolve_schema_before_next_write: false,
                closed: false,
            }),
        }
    }

    /// Simulates a cluster that cannot be reached, so every operation fails as unavailable.
    pub fn set_available(&self, available: bool) {
        self.state.lock().available = available;
    }

    /// Simulates a health RPC that never answers, which the supervisor must time out.
    pub fn set_cluster_health_hanging(&self, hanging: bool) {
        self.state.lock().cluster_health_hanging = hanging;
    }

    /// Advances the schema ID of one table, simulating concurrent DDL.
    pub fn bump_schema_id(&self, table: &TableRef) {
        self.state.lock().catalog.bump_schema_id(table);
    }

    /// Advances the schema ID of the written table just before the next write is submitted.
    pub fn evolve_schema_before_next_write(&self) {
        self.state.lock().evolve_schema_before_next_write = true;
    }

    /// Every write the backend has accepted, in submission order.
    pub fn recorded_writes(&self) -> Vec<RecordedWrite> {
        self.state.lock().writes.clone()
    }

    /// Forgets previously recorded writes.
    pub fn clear_recorded_writes(&self) {
        self.state.lock().writes.clear();
    }

    /// Makes the listed entries of the next write request fail with the given completion class.
    pub fn inject_write_failure(
        &self,
        input_indexes: Vec<usize>,
        completion: WriteCompletion,
        error_code: &str,
        retryable: bool,
    ) {
        self.state.lock().injected_write_failure = Some(InjectedWriteFailure {
            input_indexes,
            completion,
            error_code: error_code.to_string(),
            retryable,
        });
    }

    /// True once [`GatewayBackend::close`] has run.
    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    /// Rejects work when the fixture is configured as unreachable.
    pub(crate) fn ensure_available(&self) -> Result<(), GatewayError> {
        if self.state.lock().available {
            Ok(())
        } else {
            Err(GatewayError::unavailable("test backend is unavailable"))
        }
    }
}

#[async_trait]
impl GatewayBackend for TestBackend {
    async fn list_databases(&self) -> Result<Vec<String>, GatewayError> {
        self.ensure_available()?;
        catalog::list_databases(self)
    }

    async fn describe_database(&self, database: &str) -> Result<DatabaseDescription, GatewayError> {
        self.ensure_available()?;
        catalog::describe_database(self, database)
    }

    async fn create_database(&self, request: &CreateDatabaseRequest) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::create_database(self, request)
    }

    async fn drop_database(&self, database: &str) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::drop_database(self, database)
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, GatewayError> {
        self.ensure_available()?;
        catalog::list_tables(self, database)
    }

    async fn describe_table(
        &self,
        table: &TableRef,
    ) -> Result<Arc<TableDescription>, GatewayError> {
        self.ensure_available()?;
        catalog::describe_table(self, table)
    }

    async fn create_table(&self, request: &CreateTableRequest) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::create_table(self, request)
    }

    async fn alter_table(&self, request: &AlterTableRequest) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::alter_table(self, request)
    }

    async fn drop_table(&self, table: &TableRef) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::drop_table(self, table)
    }

    async fn list_partitions(
        &self,
        table: &TableRef,
    ) -> Result<Vec<PartitionDescription>, GatewayError> {
        self.ensure_available()?;
        catalog::list_partitions(self, table)
    }

    async fn create_partition(
        &self,
        request: &PartitionMutationRequest,
    ) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::create_partition(self, request)
    }

    async fn drop_partition(&self, request: &PartitionMutationRequest) -> Result<(), GatewayError> {
        self.ensure_available()?;
        catalog::drop_partition(self, request)
    }

    async fn lookup(
        &self,
        table: &TableRef,
        keys: Vec<LookupKey>,
    ) -> Result<Vec<LookupOutcome>, GatewayError> {
        self.ensure_available()?;
        lookup::lookup(self, table, keys)
    }

    async fn prefix_lookup(
        &self,
        table: &TableRef,
        request: PrefixLookupRequest,
    ) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
        self.ensure_available()?;
        lookup::prefix_lookup(self, table, request)
    }

    async fn write(&self, request: PreparedWriteRequest) -> Result<WriteResult, GatewayError> {
        self.ensure_available()?;
        write::execute(self, request)
    }

    async fn cluster_health(&self) -> Result<ClusterHealthReport, GatewayError> {
        if self.state.lock().cluster_health_hanging {
            std::future::pending::<()>().await;
        }
        self.ensure_available()?;
        Ok(ClusterHealthReport {
            status: ClusterStatus::Green,
            num_replicas: 6,
            in_sync_replicas: 6,
            num_leader_replicas: 3,
            active_leader_replicas: 3,
        })
    }

    async fn close(&self, _timeout: Duration) -> Result<(), GatewayError> {
        self.state.lock().closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fixture_catalog_is_reachable_and_closable() {
        let backend = TestBackend::new();
        assert_eq!(backend.list_databases().await.unwrap(), vec!["fluss"]);
        assert_eq!(
            backend.cluster_health().await.unwrap().status,
            ClusterStatus::Green
        );

        backend.set_available(false);
        assert_eq!(
            backend.list_databases().await.unwrap_err().kind(),
            crate::error::ErrorKind::Unavailable
        );

        backend.set_available(true);
        GatewayBackend::close(&backend, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(backend.is_closed());
    }
}
