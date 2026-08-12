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
        "fluss_gateway_rest_requests_total",
        MetricKind::Counter,
        None,
        "Completed REST requests. `operation` is the matched route template (FIP-49), `code` \
         the HTTP status, and `cluster` the bounded configured-cluster label (`none` for \
         cluster-free routes, `unknown` for unconfigured IDs).",
        &["cluster", "method", "operation", "code"],
    ),
    metric(
        "fluss_gateway_rest_request_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "REST request duration.",
        &["cluster", "method", "operation"],
    ),
    metric(
        "fluss_gateway_rest_inflight_requests",
        MetricKind::Gauge,
        None,
        "REST requests currently executing.",
        &[],
    ),
    metric(
        "fluss_gateway_rest_rejections_total",
        MetricKind::Counter,
        None,
        "REST requests rejected by an input-validation limit or the request deadline.",
        &["reason"],
    ),
    // FIP-49 process and Tokio runtime families, sampled periodically by the runtime sampler.
    // `process_cpu_seconds_total` is monotonic but published through the gauge instrument because
    // the `metrics` counter API is integral; the exposition value is the standard fractional total.
    metric(
        "process_cpu_seconds_total",
        MetricKind::Gauge,
        Some(Unit::Seconds),
        "Total user and system CPU time spent by the gateway process.",
        &[],
    ),
    metric(
        "process_resident_memory_bytes",
        MetricKind::Gauge,
        Some(Unit::Bytes),
        "Resident memory of the gateway process. Linux only; absent elsewhere.",
        &[],
    ),
    metric(
        "process_open_fds",
        MetricKind::Gauge,
        None,
        "Open file descriptors of the gateway process.",
        &[],
    ),
    metric(
        "tokio_alive_tasks",
        MetricKind::Gauge,
        None,
        "Tokio tasks spawned but not yet finished.",
        &[],
    ),
    metric(
        "tokio_global_queue_depth",
        MetricKind::Gauge,
        None,
        "Tasks waiting in the Tokio injection queue.",
        &[],
    ),
    // FIP-49 also lists `tokio_worker_busy_seconds_total`; it needs the `tokio_unstable` runtime
    // metrics and is added once the build enables them.
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

/// Initializes process series after the recorder is installed.
pub fn register_process_metrics() {
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    metrics::gauge!("fluss_gateway_process_start_time_seconds").set(started);
    metrics::gauge!("fluss_gateway_process_ready").set(0.0);
    metrics::gauge!("fluss_gateway_process_shutting_down").set(0.0);
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
///
/// `operation` and `code` are the FIP-49 label names: the operation is the matched route
/// template, the code the HTTP status. `cluster` is already bounded by the caller: a configured
/// cluster ID, `unknown` for a request that named an unconfigured one, or `none` for routes
/// without a cluster segment.
pub fn http_request(cluster: &str, method: &str, operation: &str, code: u16, duration: Duration) {
    metrics::counter!(
        "fluss_gateway_rest_requests_total",
        "cluster" => cluster.to_string(),
        "method" => method.to_string(),
        "operation" => operation.to_string(),
        "code" => code.to_string()
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_rest_request_duration_seconds",
        "cluster" => cluster.to_string(),
        "method" => method.to_string(),
        "operation" => operation.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Adjusts the in-flight request gauge by one in either direction.
pub fn http_inflight(delta: i8) {
    let gauge = metrics::gauge!("fluss_gateway_rest_inflight_requests");
    if delta >= 0 {
        gauge.increment(f64::from(delta));
    } else {
        gauge.decrement(f64::from(-delta));
    }
}

/// Records one request rejected before reaching a handler, such as `body_size` or `timeout`.
pub fn http_rejection(reason: &'static str) {
    metrics::counter!("fluss_gateway_rest_rejections_total", "reason" => reason).increment(1);
}

/// Returns the installed recorder handle for the dedicated metrics listener.
pub fn metrics_handle() -> Option<PrometheusHandle> {
    METRICS_HANDLE.get().cloned()
}

/// Samples the FIP-49 process and Tokio runtime gauges once.
///
/// Called periodically by the lifecycle's runtime sampler; each source that a platform cannot
/// provide is skipped rather than published as zero.
pub fn sample_runtime_metrics() {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let runtime = handle.metrics();
        metrics::gauge!("tokio_alive_tasks").set(runtime.num_alive_tasks() as f64);
        metrics::gauge!("tokio_global_queue_depth").set(runtime.global_queue_depth() as f64);
    }
    if let Some(cpu_seconds) = process_cpu_seconds() {
        metrics::gauge!("process_cpu_seconds_total").set(cpu_seconds);
    }
    if let Some(resident) = process_resident_memory_bytes() {
        metrics::gauge!("process_resident_memory_bytes").set(resident);
    }
    if let Some(fds) = process_open_fds() {
        metrics::gauge!("process_open_fds").set(fds);
    }
}

/// Total user plus system CPU seconds of this process, from `getrusage(2)`.
#[cfg(unix)]
fn process_cpu_seconds() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the buffer we own; a non-zero return leaves it unread.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `getrusage` returned 0, so the buffer is initialized.
    let usage = unsafe { usage.assume_init() };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    Some(seconds(usage.ru_utime) + seconds(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<f64> {
    None
}

/// Current resident set size in bytes, from `/proc/self/statm`. Linux only.
#[cfg(target_os = "linux")]
fn process_resident_memory_bytes() -> Option<f64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: `sysconf(_SC_PAGESIZE)` reads a process constant.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then_some(resident_pages * page_size as f64)
}

#[cfg(not(target_os = "linux"))]
fn process_resident_memory_bytes() -> Option<f64> {
    None
}

/// Number of open file descriptors, counted from the per-process descriptor directory.
#[cfg(unix)]
fn process_open_fds() -> Option<f64> {
    let directory = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    let entries = std::fs::read_dir(directory).ok()?;
    // The directory handle itself is one of the entries; excluding it keeps the count honest.
    Some(entries.count().saturating_sub(1) as f64)
}

#[cfg(not(unix))]
fn process_open_fds() -> Option<f64> {
    None
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
        for prefix in ["fluss_gateway_process_", "fluss_gateway_rest_"] {
            assert!(
                METRIC_DEFINITIONS
                    .iter()
                    .any(|definition| definition.name.starts_with(prefix)),
                "missing metric family for {prefix}"
            );
        }
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
