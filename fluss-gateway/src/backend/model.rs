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

//! HTTP-independent backend models.
//!
//! These types are the boundary between protocol adapters and the Fluss client. No `fluss-rs`, HTTP, or JSON type
//! appears here. REST request and response models map to these types before calling the backend.
//!
//! Every model is request-scoped. Nothing here represents a handle, session, or cursor that would outlive the
//! request that produced it.

use crate::application::{DataType, DecodedRow};
use crate::error::GatewayError;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::time::Instant;

/// Identifies a table by database and table name (exact Fluss identifiers, already percent-decoded by the adapter).
///
/// Ordering is `(database, table)` lexical, which is the order catalog listings and keyset pagination use.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

impl TableRef {
    /// Takes the identifiers exactly as given, so callers must percent-decode path segments first.
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

/// Result of `describe_database`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseDescription {
    pub name: String,
    pub comment: Option<String>,
    pub custom_properties: HashMap<String, String>,
    /// Milliseconds since epoch.
    pub created_time: i64,
    /// Milliseconds since epoch.
    pub modified_time: i64,
}

/// Whether a table is a primary-key (KV) table or a log table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    PrimaryKey,
    Log,
}

/// One column of a table schema.
///
/// Nullability belongs to the root [`DataType`] node and is not duplicated on the column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDescription {
    pub name: String,
    pub data_type: DataType,
    pub comment: Option<String>,
}

/// Capabilities derived from immutable table metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCapabilities {
    /// Exact primary-key lookup is available (PK table whose KV format/key encoder the native client can encode).
    pub exact_lookup_supported: bool,
    /// Bounded prefix lookup is plausible: a PK table with non-empty bucket keys that are a (not necessarily
    /// strict) prefix of the physical primary key, so a prefix covering the bucket keys routes to one bucket.
    ///
    /// This is an advisory hint mirroring part of the client's own `validate_prefix_lookup`, not a decision
    /// procedure. The client applies further rules that depend on the requested prefix columns rather than on
    /// table metadata alone — notably that the columns must equal the bucket keys in order once partition keys
    /// are removed, and that a prefix equal to the whole primary key is rejected in favour of a point lookup.
    /// Callers must therefore attempt lookuper construction and surface the client's `IllegalArgument` message
    /// as a 400; refusing a request on this flag alone would reject prefix lookups that in fact work (a
    /// partitioned table whose bucket keys equal its physical primary key is the motivating case).
    pub prefix_lookup_supported: bool,
}

/// Result of `describe_table`: schema, keys, distribution, partitioning, table kind, and derived capabilities.
#[derive(Debug, Clone, PartialEq)]
pub struct TableDescription {
    pub table: TableRef,
    pub table_id: i64,
    pub schema_id: i32,
    pub kind: TableKind,
    pub columns: Vec<ColumnDescription>,
    /// Logical primary key, including partition columns. Empty for log tables.
    pub primary_keys: Vec<String>,
    /// Physical primary key (logical PK minus partition columns).
    pub physical_primary_keys: Vec<String>,
    pub bucket_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    /// Columns whose values Fluss generates when they are omitted from a partial update.
    pub auto_increment_columns: Vec<String>,
    pub num_buckets: i32,
    /// `table.log.format` such as `ARROW` or `INDEXED`. `None` for PK tables.
    pub log_format: Option<String>,
    /// `table.kv.format` such as `COMPACTED` or `INDEXED`. `None` for log tables.
    pub kv_format: Option<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub custom_properties: HashMap<String, String>,
    /// Milliseconds since epoch.
    pub created_time: i64,
    /// Milliseconds since epoch.
    pub modified_time: i64,
    pub capabilities: TableCapabilities,
    /// Arrow schema of the full table row, produced by the client's single type mapper. Adapters use it to parse
    /// typed values and to render rows without building a second type mapping.
    pub arrow_schema: SchemaRef,
}

impl TableDescription {
    /// True when the table declares partition keys, which is what makes a `partition` argument required.
    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }
}

/// One fully decoded row mutation passed from the application layer to a backend.
///
/// The native row is kept behind the backend boundary. Protocol adapters only provide
/// [`crate::application::InputValue`] and never construct this type directly.
#[derive(Debug, Clone)]
pub enum PreparedWriteOperation {
    Append(DecodedRow),
    Upsert(DecodedRow),
    Delete(DecodedRow),
}

impl PreparedWriteOperation {
    /// The one-row batch carried by this operation.
    pub fn row(&self) -> &DecodedRow {
        match self {
            Self::Append(row) | Self::Upsert(row) | Self::Delete(row) => row,
        }
    }

    /// Stable operation name used by deterministic test recordings.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Append(_) => "append",
            Self::Upsert(_) => "upsert",
            Self::Delete(_) => "delete",
        }
    }
}

/// One entry whose complete request has passed application preflight.
#[derive(Debug, Clone)]
pub struct PreparedWriteEntry {
    pub input_index: usize,
    pub id: String,
    pub operation: PreparedWriteOperation,
}

/// A complete write handed to a backend after all-or-nothing preflight.
#[derive(Debug, Clone)]
pub struct PreparedWriteRequest {
    pub cluster_id: String,
    pub table: TableRef,
    pub expected_table_id: i64,
    pub expected_schema_id: i32,
    pub partial_update_columns: Option<Vec<String>>,
    pub delivery_deadline: Instant,
    pub entries: Vec<PreparedWriteEntry>,
}

/// Whether a failed write is proven not to have been applied or may have been applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteCompletion {
    Rejected,
    Unknown,
}

/// Public, protocol-neutral failure for one input entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteFailure {
    pub error_code: String,
    pub message: String,
    pub completion: WriteCompletion,
    pub retryable: bool,
}

/// Backend verdict for one entry, positionally correlated with the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEntryResult {
    pub input_index: usize,
    pub id: String,
    pub failure: Option<WriteFailure>,
}

impl WriteEntryResult {
    pub fn success(input_index: usize, id: String) -> Self {
        Self {
            input_index,
            id,
            failure: None,
        }
    }

    pub fn failure(input_index: usize, id: String, failure: WriteFailure) -> Self {
        Self {
            input_index,
            id,
            failure: Some(failure),
        }
    }
}

/// Ordered result of a submitted write request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResult {
    pub entries: Vec<WriteEntryResult>,
}

/// One partition of a partitioned table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDescription {
    pub partition_id: i64,
    /// Partition name in `value1$value2` form.
    pub partition_name: String,
    /// Ordered `(partition_key, value)` pairs.
    pub spec: Vec<(String, String)>,
}

/// One typed value of a key column, already validated against the table schema by the adapter.
///
/// This is the HTTP-independent shape between adapters and the native key encoder. Temporal values follow the
/// storage representation of the native client: dates as days since the Unix epoch, times as milliseconds of the
/// day, timestamps as epoch or wall-clock milliseconds plus nanoseconds within the millisecond.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    String(String),
    /// Raw bytes for BINARY and BYTES columns.
    Bytes(Vec<u8>),
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    Date {
        days_since_epoch: i32,
    },
    Time {
        millis_of_day: i32,
    },
    TimestampNtz {
        millis: i64,
        nanos_of_milli: u32,
    },
    TimestampLtz {
        epoch_millis: i64,
        nanos_of_milli: u32,
    },
}

impl KeyValue {
    /// Rough byte size used for the per-request key budget. Variable-length values count their payload, fixed-width
    /// values count their storage width.
    pub fn size_estimate(&self) -> u64 {
        match self {
            KeyValue::Boolean(_) | KeyValue::TinyInt(_) => 1,
            KeyValue::SmallInt(_) => 2,
            KeyValue::Int(_)
            | KeyValue::Float(_)
            | KeyValue::Date { .. }
            | KeyValue::Time { .. } => 4,
            KeyValue::BigInt(_) | KeyValue::Double(_) => 8,
            KeyValue::String(s) => s.len() as u64,
            KeyValue::Bytes(b) => b.len() as u64,
            KeyValue::Decimal { .. } => 16,
            KeyValue::TimestampNtz { .. } | KeyValue::TimestampLtz { .. } => 12,
        }
    }
}

/// One key with values in column order.
///
/// For a point lookup the values are the logical primary key, partition key columns included. For a prefix lookup
/// they are the values of the request's `prefix_columns`, in that order.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupKey {
    pub values: Vec<KeyValue>,
}

impl LookupKey {
    /// Values must already be in the declared column order. Nothing here checks that.
    pub fn new(values: Vec<KeyValue>) -> Self {
        Self { values }
    }

    /// Sum of the size estimates of all key values, used to bound the total key bytes of one lookup request.
    pub fn size_estimate(&self) -> u64 {
        self.values.iter().map(KeyValue::size_estimate).sum()
    }
}

/// Result of one key within a batch point lookup. Exactly one of the three cases applies per key.
#[derive(Debug, Clone, PartialEq)]
pub enum LookupOutcomeKind {
    /// The key matched. The batch holds exactly one row in the full table schema.
    Found(RecordBatch),
    /// The key did not match any row. This is a regular result, not an error.
    NotFound,
    /// Looking up this key failed. Other keys of the batch are unaffected.
    Error(GatewayError),
}

/// Positionally aligned result of one key in a batch point lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupOutcome {
    pub input_index: usize,
    pub kind: LookupOutcomeKind,
}

/// One batch of prefix lookups against a single table.
///
/// `prefix_columns` names the columns each prefix supplies, in order; the adapter validates that they form a
/// prefix covering the table's bucket keys before calling the backend. `max_rows_per_prefix` is applied by the
/// backend as gateway-side truncation: the native `PrefixKeyLookuper` takes one prefix per call and has no row
/// bound of its own, so the backend cuts the result and flags it instead.
#[derive(Debug, Clone, PartialEq)]
pub struct PrefixLookupRequest {
    pub prefix_columns: Vec<String>,
    pub prefixes: Vec<LookupKey>,
    pub max_rows_per_prefix: usize,
}

/// Result of one prefix within a batch prefix lookup.
///
/// Unlike a point lookup there is no not-found case: a prefix that matches nothing yields `Rows` with a zero-row
/// batch, because "no rows in this range" is a normal answer rather than a missing resource.
#[derive(Debug, Clone, PartialEq)]
pub enum PrefixOutcomeKind {
    /// The prefix was scanned. `truncated` is true when the per-prefix row cap cut the result short.
    Rows { batch: RecordBatch, truncated: bool },
    /// Scanning this prefix failed. Other prefixes of the batch are unaffected.
    Error(GatewayError),
}

/// Positionally aligned result of one prefix in a batch prefix lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct PrefixLookupOutcome {
    pub input_index: usize,
    pub kind: PrefixOutcomeKind,
}

/// Fluss cluster health, mirroring the server's traffic-light model plus an explicit `Unknown` for "could not
/// determine".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterStatus {
    Green,
    Yellow,
    Red,
    Unknown,
}

impl ClusterStatus {
    /// Stable uppercase spelling used in responses, for example `GREEN`.
    pub fn as_str(self) -> &'static str {
        match self {
            ClusterStatus::Green => "GREEN",
            ClusterStatus::Yellow => "YELLOW",
            ClusterStatus::Red => "RED",
            ClusterStatus::Unknown => "UNKNOWN",
        }
    }
}

/// Result of `cluster_health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterHealthReport {
    pub status: ClusterStatus,
    pub num_replicas: i32,
    pub in_sync_replicas: i32,
    pub num_leader_replicas: i32,
    pub active_leader_replicas: i32,
}
