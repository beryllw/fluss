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

//! Process logging and the complete gateway metric inventory.
//!
//! [`METRIC_DEFINITIONS`] is the cardinality contract: every gateway-owned metric family is declared here once,
//! with its kind, unit, description, and label set. Emission goes exclusively through the typed helpers in this
//! module so no call site can invent a family or a label that the inventory does not know about.
//!
//! Labels describe an operation or a bounded outcome. `cluster`, sourced from validated configuration, is the
//! only resource-name label; database, table, and partition names are never labels.

use log::{LevelFilter, Log, Metadata, Record};
use metrics::Unit;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Logger that writes one line per record to standard error, with no filtering beyond the global level.
struct StderrLogger;

impl Log for StderrLogger {
    /// Returns whether a record is within the configured global level.
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    /// Writes one enabled record to standard error.
    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            eprintln!("{} {} {}", record.level(), record.target(), record.args());
        }
    }

    /// Flushes buffered output, which is a no-op for direct standard-error writes.
    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Which Prometheus instrument a metric family uses.
#[derive(Clone, Copy)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// One declared metric family and its complete label set.
pub struct MetricDefinition {
    /// Fully qualified Prometheus family name.
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: Option<Unit>,
    pub description: &'static str,
    /// Every label key the family may carry. Values must come from a bounded vocabulary.
    pub labels: &'static [&'static str],
}

/// The complete inventory of gateway-owned metric families.
///
/// Adding an emission site means adding its family here first. Nothing in the gateway emits a family absent from
/// this table, and the tests below enforce the label-cardinality rules.
pub const METRIC_DEFINITIONS: &[MetricDefinition] = &[
    metric(
        "fluss_gateway_process_start_time_seconds",
        MetricKind::Gauge,
        Some(Unit::Seconds),
        "Gateway process start time since the Unix epoch.",
        &[],
    ),
    metric(
        "fluss_gateway_process_ready",
        MetricKind::Gauge,
        None,
        "Whether the gateway accepts requests.",
        &[],
    ),
    metric(
        "fluss_gateway_process_shutting_down",
        MetricKind::Gauge,
        None,
        "Whether graceful shutdown has begun.",
        &[],
    ),
    metric(
        "fluss_gateway_process_shutdown_total",
        MetricKind::Counter,
        None,
        "Gateway shutdown outcomes: success, task_error, or cleanup_error.",
        &["result"],
    ),
    metric(
        "fluss_gateway_process_shutdown_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "Gateway graceful-shutdown duration by success, task_error, or cleanup_error.",
        &["result"],
    ),
    metric(
        "fluss_gateway_http_requests_total",
        MetricKind::Counter,
        None,
        "Completed REST requests.",
        &["method", "route", "status"],
    ),
    metric(
        "fluss_gateway_http_request_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "REST request duration.",
        &["method", "route"],
    ),
    metric(
        "fluss_gateway_http_inflight_requests",
        MetricKind::Gauge,
        None,
        "REST requests currently executing.",
        &[],
    ),
    metric(
        "fluss_gateway_http_rejections_total",
        MetricKind::Counter,
        None,
        "REST requests rejected by an input-validation limit or the request deadline.",
        &["reason"],
    ),
    metric(
        "fluss_gateway_backend_connected",
        MetricKind::Gauge,
        None,
        "Whether a configured cluster currently has a connected backend.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_backend_reconnect_attempts_total",
        MetricKind::Counter,
        None,
        "Backend connection attempts.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_backend_reconnect_results_total",
        MetricKind::Counter,
        None,
        "Backend connection attempt outcomes.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_backend_probe_results_total",
        MetricKind::Counter,
        None,
        "Backend health probe outcomes.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_backend_closes_total",
        MetricKind::Counter,
        None,
        "Backend close outcomes during replacement or shutdown.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_catalog_operations_total",
        MetricKind::Counter,
        None,
        "Catalog read outcomes.",
        &["cluster", "operation", "result"],
    ),
    metric(
        "fluss_gateway_catalog_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "Catalog read duration.",
        &["cluster", "operation", "result"],
    ),
    metric(
        "fluss_gateway_metadata_cache_operations_total",
        MetricKind::Counter,
        None,
        "Metadata-cache hit, miss, refresh, and invalidation outcomes.",
        &["cluster", "operation", "result"],
    ),
    metric(
        "fluss_gateway_metadata_cache_entries",
        MetricKind::Gauge,
        None,
        "Metadata-cache entries retained for a cluster.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_lookup_requests_total",
        MetricKind::Counter,
        None,
        "Primary-key lookup request outcomes.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_lookup_keys_total",
        MetricKind::Counter,
        None,
        "Primary-key lookup key outcomes: found, not_found, or error.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_lookup_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "Primary-key lookup request duration.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_prefix_lookup_requests_total",
        MetricKind::Counter,
        None,
        "Prefix lookup request outcomes.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_prefix_lookup_prefixes_total",
        MetricKind::Counter,
        None,
        "Prefix lookup prefix outcomes: rows or error.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_prefix_lookup_rows_total",
        MetricKind::Counter,
        None,
        "Rows returned by prefix lookups after per-prefix truncation.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_prefix_lookup_truncations_total",
        MetricKind::Counter,
        None,
        "Prefixes whose rows were truncated at the configured per-prefix cap.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_prefix_lookup_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "Prefix lookup request duration.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_write_requests_total",
        MetricKind::Counter,
        None,
        "Write request outcomes.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_write_rows_total",
        MetricKind::Counter,
        None,
        "Write rows accepted for preflight.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_write_decoded_bytes_total",
        MetricKind::Counter,
        Some(Unit::Bytes),
        "Write request bytes presented to the decoder.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_write_outcome_rows_total",
        MetricKind::Counter,
        None,
        "Write row outcomes, including ambiguous completion.",
        &["cluster", "completion"],
    ),
    metric(
        "fluss_gateway_write_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "End-to-end write request duration.",
        &["cluster", "result"],
    ),
    metric(
        "fluss_gateway_write_backend_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "Native write execution duration.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_ddl_operations_total",
        MetricKind::Counter,
        None,
        "DDL operation outcomes.",
        &["cluster", "operation", "result"],
    ),
    metric(
        "fluss_gateway_ddl_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "DDL operation duration.",
        &["cluster", "operation", "result"],
    ),
];

const fn metric(
    name: &'static str,
    kind: MetricKind,
    unit: Option<Unit>,
    description: &'static str,
    labels: &'static [&'static str],
) -> MetricDefinition {
    MetricDefinition {
        name,
        kind,
        unit,
        description,
        labels,
    }
}

/// Initializes the process logger. Repeated calls refresh the global level.
pub fn init_logging() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .as_deref()
        .map(parse_level)
        .unwrap_or(LevelFilter::Info);
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(level);
}

/// Installs the process-wide Prometheus recorder before the Fluss client creates metric handles.
pub fn init_metrics(enabled: bool) -> Result<(), String> {
    if !enabled || METRICS_HANDLE.get().is_some() {
        return Ok(());
    }
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|error| format!("failed to install Prometheus recorder: {error}"))?;
    let _ = METRICS_HANDLE.set(handle);
    describe_metrics();
    Ok(())
}

/// Initializes process and configured-cluster series after the recorder is installed.
pub fn register_process_metrics<'a>(clusters: impl IntoIterator<Item = &'a str>) {
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    metrics::gauge!("fluss_gateway_process_start_time_seconds").set(started);
    metrics::gauge!("fluss_gateway_process_ready").set(0.0);
    metrics::gauge!("fluss_gateway_process_shutting_down").set(0.0);
    for cluster in clusters {
        metrics::gauge!("fluss_gateway_backend_connected", "cluster" => cluster.to_string())
            .set(0.0);
        metrics::gauge!("fluss_gateway_metadata_cache_entries", "cluster" => cluster.to_string())
            .set(0.0);
    }
}

/// Records that startup completed and request listeners accept work.
pub fn process_ready() {
    metrics::gauge!("fluss_gateway_process_ready").set(1.0);
}

/// Records the start of graceful shutdown before listeners stop accepting.
pub fn process_draining() {
    metrics::gauge!("fluss_gateway_process_ready").set(0.0);
    metrics::gauge!("fluss_gateway_process_shutting_down").set(1.0);
}

/// Records one terminal graceful-shutdown outcome and its bounded duration.
pub fn process_stopped(result: &'static str, duration: Duration) {
    metrics::counter!("fluss_gateway_process_shutdown_total", "result" => result).increment(1);
    metrics::histogram!("fluss_gateway_process_shutdown_duration_seconds", "result" => result)
        .record(duration.as_secs_f64());
}

/// Records one completed REST request against the matched route template, never the raw URI.
pub fn http_request(method: &str, route: &str, status: u16, duration: Duration) {
    metrics::counter!(
        "fluss_gateway_http_requests_total",
        "method" => method.to_string(),
        "route" => route.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_http_request_duration_seconds",
        "method" => method.to_string(),
        "route" => route.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Adjusts the in-flight request gauge by one in either direction.
pub fn http_inflight(delta: i8) {
    let gauge = metrics::gauge!("fluss_gateway_http_inflight_requests");
    if delta >= 0 {
        gauge.increment(f64::from(delta));
    } else {
        gauge.decrement(f64::from(-delta));
    }
}

/// Records one request rejected before reaching a handler, such as `body_size` or `timeout`.
pub fn http_rejection(reason: &'static str) {
    metrics::counter!("fluss_gateway_http_rejections_total", "reason" => reason).increment(1);
}

/// Records one backend connection attempt for a configured cluster.
pub fn backend_reconnect_attempt(cluster: &str) {
    metrics::counter!(
        "fluss_gateway_backend_reconnect_attempts_total",
        "cluster" => cluster.to_string()
    )
    .increment(1);
}

/// Records one backend connection attempt outcome and the resulting connectivity gauge.
pub fn backend_reconnect_result(cluster: &str, result: &'static str, connected: bool) {
    metrics::counter!(
        "fluss_gateway_backend_reconnect_results_total",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .increment(1);
    backend_connected(cluster, connected);
}

/// Records one health probe outcome and the resulting connectivity gauge.
pub fn backend_probe_result(cluster: &str, result: &'static str, connected: bool) {
    metrics::counter!(
        "fluss_gateway_backend_probe_results_total",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .increment(1);
    backend_connected(cluster, connected);
}

/// Records one backend close outcome during replacement or shutdown.
pub fn backend_close_result(cluster: &str, success: bool) {
    metrics::counter!(
        "fluss_gateway_backend_closes_total",
        "cluster" => cluster.to_string(),
        "result" => if success { "success" } else { "error" }
    )
    .increment(1);
}

/// Sets whether a configured cluster currently has a connected backend.
pub fn backend_connected(cluster: &str, connected: bool) {
    metrics::gauge!("fluss_gateway_backend_connected", "cluster" => cluster.to_string())
        .set(if connected { 1.0 } else { 0.0 });
}

/// Records one per-user act-as connection created by the user identity mode (FIP-49
/// `fluss_gateway_connections_created_total`).
pub fn identity_connection_created(cluster: &str) {
    metrics::counter!(
        "fluss_gateway_connections_created_total",
        "cluster" => cluster.to_string()
    )
    .increment(1);
}

/// Records one per-user act-as connection dropped from the pool (FIP-49
/// `fluss_gateway_connections_closed_total`), with why it left.
pub fn identity_connection_closed(cluster: &str, reason: &'static str) {
    metrics::counter!(
        "fluss_gateway_connections_closed_total",
        "cluster" => cluster.to_string(),
        "reason" => reason
    )
    .increment(1);
}

/// Sets the number of pooled per-user act-as connections (FIP-49
/// `fluss_gateway_connections_active`).
pub fn identity_connections_active(cluster: &str, active: usize) {
    metrics::gauge!(
        "fluss_gateway_connections_active",
        "cluster" => cluster.to_string()
    )
    .set(active as f64);
}

/// Records one catalog read outcome and its duration.
pub fn catalog_operation(
    cluster: &str,
    operation: &'static str,
    result: &'static str,
    duration: Duration,
) {
    metrics::counter!(
        "fluss_gateway_catalog_operations_total",
        "cluster" => cluster.to_string(),
        "operation" => operation,
        "result" => result
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_catalog_duration_seconds",
        "cluster" => cluster.to_string(),
        "operation" => operation,
        "result" => result
    )
    .record(duration.as_secs_f64());
}

/// Records one DDL mutation outcome and its duration.
pub fn ddl_operation(
    cluster: &str,
    operation: &'static str,
    result: &'static str,
    duration: Duration,
) {
    metrics::counter!(
        "fluss_gateway_ddl_operations_total",
        "cluster" => cluster.to_string(),
        "operation" => operation,
        "result" => result
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_ddl_duration_seconds",
        "cluster" => cluster.to_string(),
        "operation" => operation,
        "result" => result
    )
    .record(duration.as_secs_f64());
}

/// Records one metadata-cache operation, for example `("lookup", "hit")` or `("refresh", "error")`.
pub fn metadata_cache_operation(cluster: &str, operation: &'static str, result: &'static str) {
    metrics::counter!(
        "fluss_gateway_metadata_cache_operations_total",
        "cluster" => cluster.to_string(),
        "operation" => operation,
        "result" => result
    )
    .increment(1);
}

/// Publishes the current retained entry count of one cluster's metadata cache.
pub fn metadata_cache_entries(cluster: &str, entries: usize) {
    metrics::gauge!("fluss_gateway_metadata_cache_entries", "cluster" => cluster.to_string())
        .set(entries as f64);
}

/// Records one terminal primary-key lookup request result and its duration.
pub fn lookup_request(cluster: &str, result: &'static str, duration: Duration) {
    metrics::counter!(
        "fluss_gateway_lookup_requests_total",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_lookup_duration_seconds",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .record(duration.as_secs_f64());
}

/// Records per-key outcomes of one primary-key lookup batch.
pub fn lookup_keys(cluster: &str, result: &'static str, keys: usize) {
    if keys > 0 {
        metrics::counter!(
            "fluss_gateway_lookup_keys_total",
            "cluster" => cluster.to_string(),
            "result" => result
        )
        .increment(keys as u64);
    }
}

/// Records one terminal prefix-lookup request result and its duration.
pub fn prefix_lookup_request(cluster: &str, result: &'static str, duration: Duration) {
    metrics::counter!(
        "fluss_gateway_prefix_lookup_requests_total",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_prefix_lookup_duration_seconds",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .record(duration.as_secs_f64());
}

/// Records per-prefix outcomes of one prefix-lookup batch.
pub fn prefix_lookup_prefixes(cluster: &str, result: &'static str, prefixes: usize) {
    if prefixes > 0 {
        metrics::counter!(
            "fluss_gateway_prefix_lookup_prefixes_total",
            "cluster" => cluster.to_string(),
            "result" => result
        )
        .increment(prefixes as u64);
    }
}

/// Records rows returned by a prefix lookup and how many prefixes hit the per-prefix cap.
pub fn prefix_lookup_rows(cluster: &str, rows: usize, truncated_prefixes: usize) {
    if rows > 0 {
        metrics::counter!(
            "fluss_gateway_prefix_lookup_rows_total",
            "cluster" => cluster.to_string()
        )
        .increment(rows as u64);
    }
    if truncated_prefixes > 0 {
        metrics::counter!(
            "fluss_gateway_prefix_lookup_truncations_total",
            "cluster" => cluster.to_string()
        )
        .increment(truncated_prefixes as u64);
    }
}

/// Records one terminal write request result and its end-to-end duration.
pub fn write_request(cluster: &str, result: &'static str, duration: Duration) {
    metrics::counter!(
        "fluss_gateway_write_requests_total",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_write_duration_seconds",
        "cluster" => cluster.to_string(),
        "result" => result
    )
    .record(duration.as_secs_f64());
}

/// Records the rows and decoded bytes accepted by write preflight.
pub fn write_accepted(cluster: &str, rows: usize, decoded_bytes: u64) {
    if rows > 0 {
        metrics::counter!("fluss_gateway_write_rows_total", "cluster" => cluster.to_string())
            .increment(rows as u64);
    }
    if decoded_bytes > 0 {
        metrics::counter!(
            "fluss_gateway_write_decoded_bytes_total",
            "cluster" => cluster.to_string()
        )
        .increment(decoded_bytes);
    }
}

/// Records per-row delivery outcomes by completion class: `success`, `rejected`, or `unknown`.
pub fn write_outcome_rows(cluster: &str, completion: &'static str, rows: usize) {
    if rows > 0 {
        metrics::counter!(
            "fluss_gateway_write_outcome_rows_total",
            "cluster" => cluster.to_string(),
            "completion" => completion
        )
        .increment(rows as u64);
    }
}

/// Records how long native write delivery took, excluding preflight and response encoding.
pub fn write_backend_duration(cluster: &str, duration: Duration) {
    metrics::histogram!(
        "fluss_gateway_write_backend_duration_seconds",
        "cluster" => cluster.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Returns the installed recorder handle for the dedicated metrics listener.
pub fn metrics_handle() -> Option<PrometheusHandle> {
    METRICS_HANDLE.get().cloned()
}

fn describe_metrics() {
    for definition in METRIC_DEFINITIONS {
        match (definition.kind, definition.unit) {
            (MetricKind::Counter, Some(unit)) => {
                metrics::describe_counter!(definition.name, unit, definition.description)
            }
            (MetricKind::Counter, None) => {
                metrics::describe_counter!(definition.name, definition.description)
            }
            (MetricKind::Gauge, Some(unit)) => {
                metrics::describe_gauge!(definition.name, unit, definition.description)
            }
            (MetricKind::Gauge, None) => {
                metrics::describe_gauge!(definition.name, definition.description)
            }
            (MetricKind::Histogram, Some(unit)) => {
                metrics::describe_histogram!(definition.name, unit, definition.description)
            }
            (MetricKind::Histogram, None) => {
                metrics::describe_histogram!(definition.name, definition.description)
            }
        }
        debug_assert!(definition.labels.iter().all(|label| !label.is_empty()));
    }
}

/// Parses a supported global level name, defaulting unknown directives to `info`.
fn parse_level(value: &str) -> LevelFilter {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" => LevelFilter::Off,
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_global_levels() {
        assert_eq!(parse_level("off"), LevelFilter::Off);
        assert_eq!(parse_level("ERROR"), LevelFilter::Error);
        assert_eq!(parse_level("warn"), LevelFilter::Warn);
        assert_eq!(parse_level("info"), LevelFilter::Info);
        assert_eq!(parse_level("debug"), LevelFilter::Debug);
        assert_eq!(parse_level("trace"), LevelFilter::Trace);
        assert_eq!(parse_level("module=debug"), LevelFilter::Info);
    }

    #[test]
    fn metric_inventory_covers_every_required_subsystem() {
        for prefix in [
            "fluss_gateway_process_",
            "fluss_gateway_http_",
            "fluss_gateway_backend_",
            "fluss_gateway_catalog_",
            "fluss_gateway_lookup_",
            "fluss_gateway_prefix_lookup_",
            "fluss_gateway_write_",
            "fluss_gateway_ddl_",
        ] {
            assert!(
                METRIC_DEFINITIONS
                    .iter()
                    .any(|definition| definition.name.starts_with(prefix)),
                "missing metric family for {prefix}"
            );
        }
        assert!(
            METRIC_DEFINITIONS
                .iter()
                .any(|definition| { definition.name.starts_with("fluss_gateway_metadata_cache_") })
        );
    }

    #[test]
    fn inventory_declares_no_scan_or_cursor_family() {
        for definition in METRIC_DEFINITIONS {
            for forbidden in ["fluss_gateway_scan_", "fluss_gateway_cursor_"] {
                assert!(
                    !definition.name.starts_with(forbidden),
                    "stateless gateway must not declare {}",
                    definition.name
                );
            }
        }
    }

    #[test]
    fn metric_family_names_are_unique() {
        let mut names: Vec<&str> = METRIC_DEFINITIONS
            .iter()
            .map(|definition| definition.name)
            .collect();
        names.sort_unstable();
        let total = names.len();
        names.dedup();
        assert_eq!(names.len(), total, "duplicate metric family declared");
    }

    #[test]
    fn metric_labels_cannot_contain_unbounded_resource_names() {
        const FORBIDDEN: &[&str] = &[
            "database",
            "table",
            "partition",
            "cursor",
            "entry_id",
            "request_id",
            "raw_uri",
            "row",
        ];
        for definition in METRIC_DEFINITIONS {
            for label in definition.labels {
                assert!(
                    !FORBIDDEN.contains(label),
                    "metric {} has forbidden label {label}",
                    definition.name
                );
            }
            let resource_labels = definition
                .labels
                .iter()
                .filter(|label| matches!(**label, "cluster" | "database" | "table" | "partition"))
                .copied()
                .collect::<Vec<_>>();
            assert!(
                resource_labels.is_empty() || resource_labels == ["cluster"],
                "metric {} has invalid resource labels {resource_labels:?}",
                definition.name
            );
        }
    }
}
