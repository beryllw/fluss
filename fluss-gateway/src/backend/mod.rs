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

//! P6 — BackendFacade + metadata read API.
//!
//! The direct-path backend: orchestrates direct read/write intents onto Fluss
//! and exposes read-only metadata (list_databases / list_tables / get_table_info
//! with a TTL cache). The SQL path does NOT go through here — it goes through
//! fluss-datafusion; the two paths only converge at the connection layer, never
//! at the backend layer. Design: `design/infra.md` §P6.2–P6.4, §P6.7.
//!
//! ## Two metadata caches (known drift; design/infra.md §P6.4)
//! This facade owns a metadata cache that serves the REST metadata endpoints and
//! the PG `pg_catalog` overlay. fluss-datafusion owns a *separate* internal
//! metadata cache that serves SQL planning; the gateway cannot and should not
//! reach into it (parallel-development contract). Both ultimately derive from the
//! same Fluss cluster, so they can briefly disagree. Phase 1 stance: do not share
//! one cache across the crate boundary, align TTLs on both sides, and treat
//! "SQL view vs REST view may momentarily drift" as a *known, accepted* risk —
//! no cross-cache invalidation broadcast. The cache TTL here is the gateway-side
//! knob that should be configured to match fluss-datafusion's.
//!
//! ## Error boundary (design/infra.md §P6.7)
//! This is the backend→domain mapping boundary: raw fluss-rs / fluss-datafusion
//! errors are mapped to [`GatewayError`] *at the entry of every method here*. No
//! raw backend error type escapes upward; the protocol layer later maps
//! domain→protocol. Implementations must not leak fluss-rs error types.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{GatewayError, GatewayResult};
use crate::types::{
    DirectReadRequest, DirectReadResult, DirectWriteRequest, DirectWriteResult, MetadataScope,
    TableInfo, TableRef,
};

/// Default metadata cache TTL. Should be aligned with fluss-datafusion's own
/// metadata cache TTL (design/infra.md §P6.4) to bound SQL/REST view drift.
pub const DEFAULT_METADATA_CACHE_TTL: Duration = Duration::from_secs(30);

/// The direct-path backend and the gateway's single internal metadata source.
///
/// Responsibilities (design/infra.md §P6.2–P6.3):
/// - orchestrate direct *writes* (`KvUpsert` / `KvDelete` / `LogAppend`) onto the
///   Fluss client, returning a domain [`DirectWriteResult`];
/// - expose read-only metadata (`list_databases` / `list_tables` /
///   `get_table_info`), produced Arrow-/domain-native and TTL-cached;
/// - `read` (lookup/scan) is deferred this phase (see [`BackendFacade::read`]).
///
/// Not its job: SQL planning/pushdown (fluss-datafusion), protocol encoding,
/// session/operation lifecycle.
///
/// `MetadataService` is intentionally *not* a separate type in MVP, but the three
/// metadata methods form a self-contained logical surface so it can be split out
/// later without changing callers (design/infra.md §P6.3).
#[async_trait]
pub trait BackendFacade: Send + Sync {
    // --- direct write orchestration (design/infra.md §P6.2) ---

    /// Orchestrate a direct write onto the backend. At-least-once semantics: a
    /// success means the backend acked; a mid-flight failure may have partially
    /// written and is not rolled back (direct-path.md §6). The request body is
    /// already decoded to Arrow-native at the REST boundary.
    async fn write(&self, request: DirectWriteRequest) -> GatewayResult<DirectWriteResult>;

    // --- metadata read surface (design/infra.md §P6.3) ---

    /// List database names visible in the cluster.
    async fn list_databases(&self, scope: &MetadataScope) -> GatewayResult<Vec<String>>;

    /// List tables in a database.
    async fn list_tables(
        &self,
        scope: &MetadataScope,
        database: &str,
    ) -> GatewayResult<Vec<TableRef>>;

    /// Fetch a single table's metadata (schema, name).
    async fn get_table_info(
        &self,
        scope: &MetadataScope,
        table: &TableRef,
    ) -> GatewayResult<TableInfo>;

    // --- direct read (DEFERRED this phase) ---

    /// Direct read (lookup / scan) is deferred in Phase 1 (TASKS §7 Backlog): the
    /// REST read endpoints are placeholders. The method stays on the trait so the
    /// shape is frozen and P5 can wire it without changing the trait; the default
    /// impl rejects it as `Unsupported`. Implementations must NOT implement it
    /// this phase.
    async fn read(&self, _request: DirectReadRequest) -> GatewayResult<DirectReadResult> {
        Err(GatewayError::Unsupported(
            "direct read is deferred in Phase 1".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Real Fluss-backed facade (skeleton)
// ---------------------------------------------------------------------------

/// Phase 1 production facade backed by a real shared `FlussConnection`.
///
/// Skeleton: direct *write* truly lands in P5 (direct-path / REST), so `write`
/// here is a documented `Unsupported` placeholder until then. Metadata reads
/// could be implemented against `connection.get_admin()`, but are also left as a
/// P5-time wiring point to avoid live-cluster coupling before the REST metadata
/// endpoints exist. The type exists now to pin the construction shape (it holds
/// the shared connection + the metadata cache TTL) and to prove the trait fits
/// the real client. Not unit-tested (no live cluster); behavior coverage is via
/// [`tests`]'s fake.
pub struct FlussBackendFacade {
    #[allow(dead_code)]
    connection: Arc<fluss::client::FlussConnection>,
    #[allow(dead_code)]
    metadata_cache_ttl: Duration,
}

impl FlussBackendFacade {
    pub fn new(connection: Arc<fluss::client::FlussConnection>) -> Self {
        Self {
            connection,
            metadata_cache_ttl: DEFAULT_METADATA_CACHE_TTL,
        }
    }

    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.metadata_cache_ttl = ttl;
        self
    }
}

#[async_trait]
impl BackendFacade for FlussBackendFacade {
    async fn write(&self, _request: DirectWriteRequest) -> GatewayResult<DirectWriteResult> {
        // direct write truly lands in P5 (direct-path.md §3/§6). Until then this
        // is an explicit Unsupported rather than a silent no-op.
        Err(GatewayError::Unsupported(
            "FlussBackendFacade.write lands in P5".to_string(),
        ))
    }

    async fn list_databases(&self, _scope: &MetadataScope) -> GatewayResult<Vec<String>> {
        // Wiring onto connection.get_admin() lands with the REST metadata
        // endpoints (P5). backend→domain mapping will happen right here.
        Err(GatewayError::Unsupported(
            "FlussBackendFacade.list_databases lands in P5".to_string(),
        ))
    }

    async fn list_tables(
        &self,
        _scope: &MetadataScope,
        _database: &str,
    ) -> GatewayResult<Vec<TableRef>> {
        Err(GatewayError::Unsupported(
            "FlussBackendFacade.list_tables lands in P5".to_string(),
        ))
    }

    async fn get_table_info(
        &self,
        _scope: &MetadataScope,
        _table: &TableRef,
    ) -> GatewayResult<TableInfo> {
        Err(GatewayError::Unsupported(
            "FlussBackendFacade.get_table_info lands in P5".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Instant;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::types::{ClusterId, Principal, RequestExecutionContext, RequestId};
    use tokio_util::sync::CancellationToken;

    /// One cached metadata entry with its insertion time, to exercise TTL.
    struct CacheEntry {
        tables: Vec<TableRef>,
        at: Instant,
    }

    /// In-memory fake facade: a catalog of database -> table schemas, a real TTL
    /// cache for `list_tables`, and a write log to assert write orchestration.
    /// Exercises trait behavior + the backend→domain error boundary with no
    /// cluster.
    struct FakeBackendFacade {
        catalog: BTreeMap<String, BTreeMap<String, arrow::datatypes::SchemaRef>>,
        ttl: Duration,
        list_tables_cache: Mutex<BTreeMap<String, CacheEntry>>,
        list_tables_backend_hits: Mutex<usize>,
        writes: Mutex<Vec<(TableRef, u64)>>,
    }

    impl FakeBackendFacade {
        fn new(ttl: Duration) -> Self {
            let mut catalog: BTreeMap<String, BTreeMap<String, arrow::datatypes::SchemaRef>> =
                BTreeMap::new();
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let mut db = BTreeMap::new();
            db.insert("t1".to_string(), schema.clone());
            db.insert("t2".to_string(), schema);
            catalog.insert("db".to_string(), db);
            Self {
                catalog,
                ttl,
                list_tables_cache: Mutex::new(BTreeMap::new()),
                list_tables_backend_hits: Mutex::new(0),
                writes: Mutex::new(Vec::new()),
            }
        }

        fn backend_hits(&self) -> usize {
            *self.list_tables_backend_hits.lock().unwrap()
        }
    }

    #[async_trait]
    impl BackendFacade for FakeBackendFacade {
        async fn write(&self, request: DirectWriteRequest) -> GatewayResult<DirectWriteResult> {
            let (table, rows) = match request {
                DirectWriteRequest::KvUpsert { table, rows, .. } => (table, rows),
                DirectWriteRequest::LogAppend { table, rows, .. } => (table, rows),
                DirectWriteRequest::KvDelete { table, keys, .. } => (table, keys),
            };
            // Validate the table exists; unknown table -> domain error (boundary).
            let db = self
                .catalog
                .get(&table.database)
                .ok_or_else(|| GatewayError::DatabaseNotFound {
                    database: table.database.clone(),
                })?;
            if !db.contains_key(&table.table) {
                return Err(GatewayError::TableNotFound {
                    database: table.database.clone(),
                    table: table.table.clone(),
                });
            }
            let n = rows.num_rows() as u64;
            self.writes.lock().unwrap().push((table, n));
            Ok(DirectWriteResult { rows_written: n })
        }

        async fn list_databases(&self, _scope: &MetadataScope) -> GatewayResult<Vec<String>> {
            Ok(self.catalog.keys().cloned().collect())
        }

        async fn list_tables(
            &self,
            _scope: &MetadataScope,
            database: &str,
        ) -> GatewayResult<Vec<TableRef>> {
            // TTL cache: serve fresh entries, otherwise hit the "backend".
            if let Some(e) = self.list_tables_cache.lock().unwrap().get(database) {
                if e.at.elapsed() < self.ttl {
                    return Ok(e.tables.clone());
                }
            }
            let db = self
                .catalog
                .get(database)
                .ok_or_else(|| GatewayError::DatabaseNotFound {
                    database: database.to_string(),
                })?;
            *self.list_tables_backend_hits.lock().unwrap() += 1;
            let tables: Vec<TableRef> = db
                .keys()
                .map(|t| TableRef {
                    database: database.to_string(),
                    table: t.clone(),
                })
                .collect();
            self.list_tables_cache.lock().unwrap().insert(
                database.to_string(),
                CacheEntry {
                    tables: tables.clone(),
                    at: Instant::now(),
                },
            );
            Ok(tables)
        }

        async fn get_table_info(
            &self,
            _scope: &MetadataScope,
            table: &TableRef,
        ) -> GatewayResult<TableInfo> {
            let db = self.catalog.get(&table.database).ok_or_else(|| {
                GatewayError::DatabaseNotFound {
                    database: table.database.clone(),
                }
            })?;
            let schema = db.get(&table.table).ok_or_else(|| GatewayError::TableNotFound {
                database: table.database.clone(),
                table: table.table.clone(),
            })?;
            Ok(TableInfo {
                name: table.clone(),
                schema: schema.clone(),
            })
        }
    }

    fn scope() -> MetadataScope {
        MetadataScope {
            principal: Principal { name: "alice".into() },
            cluster: ClusterId("default".into()),
        }
    }

    fn ctx() -> RequestExecutionContext {
        RequestExecutionContext {
            principal: Principal { name: "alice".into() },
            cluster: ClusterId("default".into()),
            request_id: RequestId("r1".into()),
            deadline: None,
            cancel: CancellationToken::new(),
        }
    }

    fn rows(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr = Int32Array::from((0..n as i32).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn tref() -> TableRef {
        TableRef {
            database: "db".into(),
            table: "t1".into(),
        }
    }

    #[tokio::test]
    async fn write_orchestrates_and_counts_rows() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let res = f
            .write(DirectWriteRequest::KvUpsert {
                context: ctx(),
                table: tref(),
                rows: rows(3),
            })
            .await
            .unwrap();
        assert_eq!(res.rows_written, 3);
    }

    #[tokio::test]
    async fn write_unknown_table_maps_to_domain_error() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let err = f
            .write(DirectWriteRequest::LogAppend {
                context: ctx(),
                table: TableRef {
                    database: "db".into(),
                    table: "nope".into(),
                },
                rows: rows(1),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::TableNotFound { .. }));
    }

    #[tokio::test]
    async fn write_unknown_database_maps_to_domain_error() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let err = f
            .write(DirectWriteRequest::KvDelete {
                context: ctx(),
                table: TableRef {
                    database: "ghost".into(),
                    table: "t1".into(),
                },
                keys: rows(1),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::DatabaseNotFound { .. }));
    }

    #[tokio::test]
    async fn list_databases_returns_catalog() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let dbs = f.list_databases(&scope()).await.unwrap();
        assert_eq!(dbs, vec!["db".to_string()]);
    }

    #[tokio::test]
    async fn list_tables_caches_within_ttl() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let a = f.list_tables(&scope(), "db").await.unwrap();
        let b = f.list_tables(&scope(), "db").await.unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a, b);
        // Second call served from cache: backend touched exactly once.
        assert_eq!(f.backend_hits(), 1);
    }

    #[tokio::test]
    async fn list_tables_refetches_after_ttl_expiry() {
        let f = FakeBackendFacade::new(Duration::from_millis(1));
        f.list_tables(&scope(), "db").await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        f.list_tables(&scope(), "db").await.unwrap();
        // TTL expired between calls: backend hit twice.
        assert_eq!(f.backend_hits(), 2);
    }

    #[tokio::test]
    async fn list_tables_unknown_db_errors() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let err = f.list_tables(&scope(), "ghost").await.unwrap_err();
        assert!(matches!(err, GatewayError::DatabaseNotFound { .. }));
    }

    #[tokio::test]
    async fn get_table_info_returns_schema() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let info = f.get_table_info(&scope(), &tref()).await.unwrap();
        assert_eq!(info.name, tref());
        assert_eq!(info.schema.fields().len(), 1);
    }

    #[tokio::test]
    async fn get_table_info_unknown_table_errors() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let err = f
            .get_table_info(
                &scope(),
                &TableRef {
                    database: "db".into(),
                    table: "missing".into(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::TableNotFound { .. }));
    }

    #[tokio::test]
    async fn read_is_deferred_unsupported() {
        let f = FakeBackendFacade::new(Duration::from_secs(60));
        let res = f
            .read(DirectReadRequest::LogScan {
                context: ctx(),
                table: tref(),
                limit: 10,
            })
            .await;
        assert!(matches!(res, Err(GatewayError::Unsupported(_))));
    }
}
