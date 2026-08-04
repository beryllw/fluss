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

//! Gateway configuration for the REST service.
//!
//! One TOML file plus complete env overrides plus targeted CLI overrides. Precedence: CLI > env > file > defaults.
//! Parsing is strict: unknown fields (file or env) are rejected, durations must be `<int><ms|s|m|h>`, byte sizes
//! are plain integers or `<int><B|KB|KiB|MB|MiB|GB|GiB>`, and both reject zero.
//!
//! # Schema shape
//!
//! The FIP-49 plan sketches a flat `[rest]`/`[limits]`/`[metrics]` sample with hyphenated keys. This module
//! implements the sectioned snake_case schema instead (`[server.rest]`, `[server.metrics]`, `[lookup]`,
//! `[write]`, …), which supersedes that sample by explicit user decision: it is the schema proven on the
//! prior-art branch, it maps one-to-one onto the `FLUSS_GATEWAY__*` override convention, and typed sections keep
//! `deny_unknown_fields` meaningful per subsystem.
//!
//! There is deliberately **no `[security]` section and no concurrency-permit key**. The gateway performs no
//! authentication, authorisation, TLS termination, or rate limiting; the only request bounds are the
//! input-validation caps in `[server.rest] max_body_bytes`, `[lookup]`, and `[write]`.
//!
//! Env override convention: `FLUSS_GATEWAY__<SECTION>__<KEY>`, with `__` separating path components. For example,
//! `FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS` overrides `[server.rest].bind_address`, while
//! `FLUSS_GATEWAY__CLUSTERS__ANALYTICS_EU__BOOTSTRAP_SERVERS` overrides
//! `[clusters.analytics_eu].bootstrap_servers`.

use crate::application::types::ClusterId;
use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use toml::Value;

/// Environment variable prefix for overrides.
pub const ENV_PREFIX: &str = "FLUSS_GATEWAY__";

/// A strictly parsed duration: `<integer><ms|s|m|h>` (e.g. `"60s"`, `"15m"`). No floats, no whitespace, no
/// compound values. Deserialization rejects zero because every configured duration is a deadline or an interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigDuration(Duration);

impl ConfigDuration {
    /// Builds a duration directly, bypassing the string syntax used by configuration sources.
    pub const fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }

    /// Builds a sub-second duration without going through the string syntax.
    pub const fn from_millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis))
    }

    /// Hands out the value for use with timers and deadlines.
    pub fn get(self) -> Duration {
        self.0
    }

    /// Parses the strict integer-plus-unit syntax and rejects a zero result.
    fn parse(s: &str) -> Result<Self, String> {
        let (digits, unit) = split_number_and_unit(s);
        if digits.is_empty() {
            return Err(format!(
                "invalid duration {s:?}: expected <integer><ms|s|m|h>"
            ));
        }
        let value: u64 = digits
            .parse()
            .map_err(|e| format!("invalid duration {s:?}: {e}"))?;
        let duration = match unit {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value.saturating_mul(60)),
            "h" => Duration::from_secs(value.saturating_mul(3600)),
            _ => {
                return Err(format!(
                    "invalid duration {s:?}: unit must be one of ms, s, m, h"
                ));
            }
        };
        if duration.is_zero() {
            return Err(format!("invalid duration {s:?}: must be greater than zero"));
        }
        Ok(Self(duration))
    }
}

impl<'de> Deserialize<'de> for ConfigDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// A strictly parsed byte size: a plain integer, or an integer with one of the suffixes `B`, `KB`, `KiB`, `MB`,
/// `MiB`, `GB`, `GiB` (e.g. `4194304` or `"4MiB"`). Deserialization rejects zero because every configured size is
/// a budget that must admit at least one byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(u64);

impl ByteSize {
    /// Builds a size directly, bypassing the syntax and non-zero rule applied to configuration sources.
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Hands out the value for use in size comparisons and buffer budgets.
    pub fn bytes(self) -> u64 {
        self.0
    }

    /// Parses an integer size with an optional supported suffix and rejects a zero result.
    fn parse(s: &str) -> Result<Self, String> {
        let (digits, unit) = split_number_and_unit(s);
        if digits.is_empty() {
            return Err(format!("invalid byte size {s:?}: expected <integer>[unit]"));
        }
        let value: u64 = digits
            .parse()
            .map_err(|e| format!("invalid byte size {s:?}: {e}"))?;
        let multiplier: u64 = match unit {
            "" | "B" => 1,
            "KB" => 1000,
            "KiB" => 1024,
            "MB" => 1_000_000,
            "MiB" => 1024 * 1024,
            "GB" => 1_000_000_000,
            "GiB" => 1024 * 1024 * 1024,
            _ => {
                return Err(format!(
                    "invalid byte size {s:?}: unit must be one of B, KB, KiB, MB, MiB, GB, GiB"
                ));
            }
        };
        let bytes = value
            .checked_mul(multiplier)
            .ok_or_else(|| format!("invalid byte size {s:?}: overflows u64"))?;
        Self::checked(bytes).ok_or_else(|| format!("invalid byte size {s:?}: must be non-zero"))
    }

    /// Returns the size unless it is zero.
    fn checked(bytes: u64) -> Option<Self> {
        (bytes != 0).then_some(Self(bytes))
    }
}

/// Splits a strictly formatted numeric value from its optional unit suffix.
fn split_number_and_unit(value: &str) -> (&str, &str) {
    let split = value
        .char_indices()
        .find(|(_, character)| !character.is_ascii_digit())
        .map_or(value.len(), |(index, _)| index);
    value.split_at(split)
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a positive integer or a string like \"4MiB\"")
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ByteSize, E> {
                let bytes = u64::try_from(v)
                    .map_err(|_| E::custom(format!("byte size must be non-negative, got {v}")))?;
                self.visit_u64(bytes)
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ByteSize, E> {
                ByteSize::checked(v).ok_or_else(|| E::custom("byte size must be non-zero"))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ByteSize::parse(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

/// Deserializes a bootstrap list from either a TOML array or one comma-separated string, which is the only shape an
/// environment variable or a CLI flag can carry.
fn deserialize_server_list<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ListOrCsv {
        List(Vec<String>),
        Csv(String),
    }
    Ok(match ListOrCsv::deserialize(deserializer)? {
        ListOrCsv::List(list) => list,
        ListOrCsv::Csv(csv) => csv
            .split(',')
            .map(|entry| entry.trim().to_string())
            .collect(),
    })
}

/// `[server]` table.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Optional operator-chosen identity used in logs and diagnostics only.
    ///
    /// Nothing in the gateway depends on it: the process is stateless, so no response, token, or handle is ever
    /// scoped to an instance. It is never required.
    pub instance_id: Option<String>,
    pub rest: RestServerConfig,
    pub metrics: MetricsServerConfig,
}

/// `[server.rest]`, the REST listener and its input-validation limits.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RestServerConfig {
    /// Loopback by default because the gateway has no transport security.
    pub bind_address: SocketAddr,
    /// Per-request server-side deadline. Exceeding it yields 504.
    pub request_timeout: ConfigDuration,
    /// Maximum accepted request body size. Exceeding it yields 413.
    pub max_body_bytes: ByteSize,
}

impl Default for RestServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".parse().expect("valid default"),
            request_timeout: ConfigDuration::from_secs(30),
            max_body_bytes: ByteSize::new(32 * 1024 * 1024),
        }
    }
}

/// `[server.metrics]`, the internal Prometheus listener.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsServerConfig {
    pub enabled: bool,
    pub bind_address: SocketAddr,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: "127.0.0.1:9095".parse().expect("valid default"),
        }
    }
}

/// One `[clusters.<id>]` table, which configures how to reach a Fluss cluster.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    #[serde(deserialize_with = "deserialize_server_list")]
    pub bootstrap_servers: Vec<String>,
    pub connect_timeout: ConfigDuration,
    pub request_timeout: ConfigDuration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["127.0.0.1:9123".to_string()],
            connect_timeout: ConfigDuration::from_secs(10),
            request_timeout: ConfigDuration::from_secs(10),
        }
    }
}

/// `[health]`, cached probing and reconnect timing.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HealthConfig {
    pub probe_interval: ConfigDuration,
    pub probe_timeout: ConfigDuration,
    pub stale_after: ConfigDuration,
    pub reconnect_initial_backoff: ConfigDuration,
    pub reconnect_max_backoff: ConfigDuration,
    pub reconnect_attempt_timeout: ConfigDuration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            probe_interval: ConfigDuration::from_secs(5),
            probe_timeout: ConfigDuration::from_secs(2),
            stale_after: ConfigDuration::from_secs(15),
            reconnect_initial_backoff: ConfigDuration::from_millis(250),
            reconnect_max_backoff: ConfigDuration::from_secs(30),
            reconnect_attempt_timeout: ConfigDuration::from_secs(15),
        }
    }
}

/// Hard ceiling on `metadata.max_page_size`. One catalog page is assembled in memory before it is written, so
/// the page size stays bounded no matter what the operator configures.
pub const METADATA_MAX_PAGE_SIZE_CEILING: u32 = 1000;

/// Time reserved after native delivery completes to encode and send a write response.
pub const WRITE_RESPONSE_BUDGET: Duration = Duration::from_millis(250);

/// `[metadata]`, the table metadata cache and REST catalog pagination limits.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetadataConfig {
    /// Page size applied when a catalog request does not ask for one.
    pub default_page_size: u32,
    /// Largest page size a catalog request may ask for. Cannot be raised above
    /// [`METADATA_MAX_PAGE_SIZE_CEILING`].
    pub max_page_size: u32,
    /// Per-cluster bound on cached table descriptions used for write preflight.
    pub cache_max_entries: u32,
    /// Mandatory freshness lifetime because external DDL has no local invalidation signal.
    pub cache_ttl: ConfigDuration,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            default_page_size: 100,
            max_page_size: 1000,
            cache_max_entries: 1024,
            cache_ttl: ConfigDuration::from_secs(60),
        }
    }
}

/// `[lookup]`, the input-validation caps and native client bounds of the two lookup endpoints.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LookupConfig {
    /// Maximum number of keys accepted in one point-lookup request. Exceeding it yields 413.
    pub max_keys: u32,
    /// Bound on the estimated total size of all key values in one request. Exceeding it yields 413.
    pub max_key_bytes: ByteSize,
    /// Maximum number of prefixes accepted in one prefix-lookup request. Exceeding it yields 413.
    pub max_prefixes: u32,
    /// Per-prefix row cap. The native prefix lookuper returns every matching row, so the gateway truncates and
    /// flags the outcome instead of pushing the bound down.
    pub max_rows_per_prefix: u32,
    /// Bound on queued native lookup work inside the Fluss client.
    pub queue_size: u32,
    /// Bound on native lookup retries inside the Fluss client.
    pub max_retries: u32,
    /// Maximum number of concurrently in-flight native lookups issued for one request.
    pub max_concurrent: u32,
}

impl Default for LookupConfig {
    fn default() -> Self {
        Self {
            max_keys: 128,
            max_key_bytes: ByteSize::new(1024 * 1024),
            max_prefixes: 16,
            max_rows_per_prefix: 1000,
            queue_size: 4096,
            max_retries: 3,
            max_concurrent: 32,
        }
    }
}

/// `[write]`, row limits and the finite native delivery lifetime.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WriteConfig {
    /// Maximum entries accepted in one write request.
    pub max_rows: u32,
    /// Maximum enqueue, batching, retry, and acknowledgement lifetime for one entry.
    pub max_delivery_time: ConfigDuration,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            max_delivery_time: ConfigDuration::from_secs(20),
        }
    }
}

/// `[shutdown]`, which configures the graceful-shutdown drain deadline.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ShutdownConfig {
    pub drain_timeout: ConfigDuration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout: ConfigDuration::from_secs(30),
        }
    }
}

/// Complete configuration for the gateway process.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayConfig {
    pub server: ServerConfig,
    /// Strictly named Fluss cluster connections. IDs are validated before the process starts.
    pub clusters: BTreeMap<ClusterId, ClusterConfig>,
    pub health: HealthConfig,
    pub metadata: MetadataConfig,
    pub lookup: LookupConfig,
    pub write: WriteConfig,
    pub shutdown: ShutdownConfig,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            clusters: default_clusters(),
            health: HealthConfig::default(),
            metadata: MetadataConfig::default(),
            lookup: LookupConfig::default(),
            write: WriteConfig::default(),
            shutdown: ShutdownConfig::default(),
        }
    }
}

fn default_clusters() -> BTreeMap<ClusterId, ClusterConfig> {
    BTreeMap::from([(
        ClusterId::try_from("default").expect("default cluster ID is valid"),
        ClusterConfig::default(),
    )])
}

impl GatewayConfig {
    /// Checks the invariants that span more than one field. Single-field syntax and non-zero rules are enforced
    /// while deserializing. Called by [`load`] and exposed for tests and programmatic construction.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = Vec::new();
        let meta = &self.metadata;
        let health = &self.health;
        let rest = &self.server.rest;
        let lookup = &self.lookup;
        let write = &self.write;

        let counts: [(&str, u128); 9] = [
            ("metadata.default_page_size", meta.default_page_size.into()),
            ("metadata.max_page_size", meta.max_page_size.into()),
            ("metadata.cache_max_entries", meta.cache_max_entries.into()),
            ("lookup.max_keys", lookup.max_keys.into()),
            ("lookup.max_prefixes", lookup.max_prefixes.into()),
            (
                "lookup.max_rows_per_prefix",
                lookup.max_rows_per_prefix.into(),
            ),
            ("lookup.queue_size", lookup.queue_size.into()),
            ("lookup.max_concurrent", lookup.max_concurrent.into()),
            ("write.max_rows", write.max_rows.into()),
        ];
        for (name, value) in counts {
            if value == 0 {
                problems.push(format!("{name} must be greater than zero"));
            }
        }

        let ns = |value: ConfigDuration| value.get().as_nanos();
        let page_size_ceiling = format!("the fixed ceiling of {METADATA_MAX_PAGE_SIZE_CEILING}");
        let ordered: [(&str, u128, &str, u128); 5] = [
            (
                "metadata.default_page_size",
                meta.default_page_size.into(),
                "metadata.max_page_size",
                meta.max_page_size.into(),
            ),
            (
                "metadata.max_page_size",
                meta.max_page_size.into(),
                page_size_ceiling.as_str(),
                METADATA_MAX_PAGE_SIZE_CEILING.into(),
            ),
            (
                "health.probe_timeout",
                ns(health.probe_timeout),
                "health.probe_interval",
                ns(health.probe_interval),
            ),
            (
                "health.probe_interval",
                ns(health.probe_interval),
                "health.stale_after",
                ns(health.stale_after),
            ),
            (
                "health.reconnect_initial_backoff",
                ns(health.reconnect_initial_backoff),
                "health.reconnect_max_backoff",
                ns(health.reconnect_max_backoff),
            ),
        ];
        for (lower, lower_value, upper, upper_value) in ordered {
            if lower_value > upper_value {
                problems.push(format!("{lower} must not exceed {upper}"));
            }
        }

        let delivery_with_budget = write
            .max_delivery_time
            .get()
            .checked_add(WRITE_RESPONSE_BUDGET);
        if delivery_with_budget.is_none_or(|required| required > rest.request_timeout.get()) {
            problems.push(format!(
                "write.max_delivery_time plus the fixed {}ms response budget must not exceed server.rest.request_timeout",
                WRITE_RESPONSE_BUDGET.as_millis()
            ));
        }

        self.validate_clusters(&mut problems);
        self.validate_identity(&mut problems);

        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// Rejects an empty cluster map, invalid IDs, and unusable per-cluster bootstrap lists.
    fn validate_clusters(&self, problems: &mut Vec<String>) {
        if self.clusters.is_empty() {
            problems.push("clusters must configure at least one cluster".to_string());
        }
        for (id, cluster) in &self.clusters {
            if cluster.bootstrap_servers.is_empty() {
                problems.push(format!("clusters.{id}.bootstrap_servers must not be empty"));
            }
            if cluster
                .bootstrap_servers
                .iter()
                .any(|server| server.trim().is_empty())
            {
                problems.push(format!(
                    "clusters.{id}.bootstrap_servers entries must not be blank"
                ));
            }
        }
    }

    /// Rejects an unusable instance identity or a port clash between the two listeners.
    ///
    /// A non-loopback listener does **not** require an instance ID. Nothing the gateway returns is scoped to an
    /// instance, so there is no identity to pin.
    fn validate_identity(&self, problems: &mut Vec<String>) {
        let server = &self.server;
        let rest_address = server.rest.bind_address;
        if let Some(instance_id) = server.instance_id.as_deref() {
            let valid = !instance_id.is_empty()
                && instance_id.len() <= 128
                && instance_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
            if !valid {
                problems.push(
                    "server.instance_id must be 1-128 ASCII letters, digits, dots, underscores, or hyphens"
                        .to_string(),
                );
            }
        }
        if server.metrics.enabled && server.metrics.bind_address == rest_address {
            problems.push(
                "server.metrics.bind_address must differ from server.rest.bind_address".to_string(),
            );
        }
    }

    /// Returns non-fatal configuration advisories that should be logged at startup.
    pub fn warnings(&self) -> Vec<String> {
        if !self.server.rest.bind_address.ip().is_loopback() {
            vec![format!(
                "server.rest.bind_address {} is not loopback. The REST listener has no authentication or TLS",
                self.server.rest.bind_address
            )]
        } else {
            Vec::new()
        }
    }
}

/// Targeted CLI overrides (highest precedence).
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    /// Overrides `server.rest.bind_address`.
    pub bind_address: Option<String>,
}

/// Configuration loading/validation failure.
#[derive(Debug)]
pub enum ConfigError {
    /// The config file could not be read.
    Io(String),
    /// The config file or an override value could not be parsed.
    Parse(String),
    /// A `FLUSS_GATEWAY__*` variable does not name a known section/key.
    UnknownEnvKey(String),
    /// One or more invariants failed validation.
    Invalid(Vec<String>),
}

impl fmt::Display for ConfigError {
    /// Renders a concise operator-facing configuration error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(msg) => write!(f, "cannot read configuration: {msg}"),
            ConfigError::Parse(msg) => write!(f, "invalid configuration: {msg}"),
            ConfigError::UnknownEnvKey(key) => {
                write!(f, "unknown configuration environment variable: {key}")
            }
            ConfigError::Invalid(problems) => {
                write!(f, "invalid configuration: {}", problems.join(", "))
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Translates one `FLUSS_GATEWAY__` suffix into a dotted configuration path. Only the section, which is the first
/// segment, may spell a nested table with an underscore, so `SERVER_REST__BIND_ADDRESS` addresses
/// `server.rest.bind_address` while the key keeps its underscores.
fn env_suffix_to_path(suffix: &str) -> String {
    let lowered = suffix.to_ascii_lowercase();
    if let Some(cluster_suffix) = lowered.strip_prefix("clusters__")
        && let Some((cluster, key)) = cluster_suffix.split_once("__")
    {
        return format!("clusters.{cluster}.{}", key.replace("__", "."));
    }
    match lowered.split_once("__") {
        Some((section, key)) => format!("{}.{}", section.replace('_', "."), key.replace("__", ".")),
        None => lowered,
    }
}

/// Reads one override value the way a TOML right-hand side would be read, so an operator can write an array, a
/// quoted string, a number, or a boolean. A bare value that is not valid TOML stays text, except that an unquoted
/// comma makes it a list, which is how a list-valued key is written outside a file.
fn coerce_override(raw: &str) -> Value {
    if let Ok(mut table) = format!("x = {raw}").parse::<toml::Table>()
        && let Some(value) = table.remove("x")
    {
        return value;
    }
    if raw.contains(',') {
        return Value::Array(
            raw.split(',')
                .map(|entry| Value::String(entry.trim().to_string()))
                .collect(),
        );
    }
    Value::String(raw.to_string())
}

/// Writes `value` at a dotted path, creating the tables along the way and replacing whatever sat there before.
fn insert_path(table: &mut toml::Table, path: &str, value: Value) {
    let mut current = table;
    let mut segments = path.split('.').peekable();
    while let Some(segment) = segments.next() {
        if segments.peek().is_none() {
            current.insert(segment.to_string(), value);
            return;
        }
        let entry = current
            .entry(segment.to_string())
            .or_insert_with(|| Value::Table(toml::Table::new()));
        if !entry.is_table() {
            *entry = Value::Table(toml::Table::new());
        }
        current = entry.as_table_mut().expect("table inserted above");
    }
}

/// Turns a deserialization failure into an error that names the override responsible for it, if one is. Each
/// override is replayed on its own against the defaults, so only the override that actually carries the offending
/// key is blamed and a bad key in the file is never attributed to an unrelated override.
fn attribute(message: String, overrides: &[(String, String, Value)]) -> ConfigError {
    for (path, origin, value) in overrides {
        let mut probe = toml::Table::new();
        insert_path(&mut probe, path, value.clone());
        let Err(error) = GatewayConfig::deserialize(Value::Table(probe)) else {
            continue;
        };
        let reason = error.to_string();
        if reason.contains("unknown field") && origin.starts_with(ENV_PREFIX) {
            return ConfigError::UnknownEnvKey(origin.clone());
        }
        return ConfigError::Parse(format!("{origin}: {reason}"));
    }
    ConfigError::Parse(message)
}

/// Loads configuration from all sources with precedence CLI > env > file > defaults.
///
/// `env` is passed explicitly (rather than read from the process environment) so loading is deterministic and
/// testable.
pub fn load(
    path: Option<&Path>,
    env: &BTreeMap<String, String>,
    cli: &CliOverrides,
) -> Result<GatewayConfig, ConfigError> {
    let mut table = toml::Table::new();
    if let Some(path) = path {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("{}: {e}", path.display())))?;
        table = contents
            .parse::<toml::Table>()
            .map_err(|e| ConfigError::Parse(e.to_string()))?;
    }

    // Each override is kept with the source that wrote it, so a failure names what the operator wrote.
    let mut overrides: Vec<(String, String, Value)> = Vec::new();
    for (key, raw) in env {
        let Some(suffix) = key.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        if suffix.is_empty() {
            return Err(ConfigError::UnknownEnvKey(key.clone()));
        }
        overrides.push((
            env_suffix_to_path(suffix),
            key.clone(),
            coerce_override(raw),
        ));
    }

    for (path, flag, value) in [(
        "server.rest.bind_address",
        "--bind-address",
        cli.bind_address.as_ref(),
    )] {
        if let Some(value) = value {
            overrides.push((
                path.to_string(),
                flag.to_string(),
                Value::String(value.clone()),
            ));
        }
    }

    for (path, _, value) in &overrides {
        insert_path(&mut table, path, value.clone());
    }

    let config = GatewayConfig::deserialize(Value::Table(table))
        .map_err(|error| attribute(error.to_string(), &overrides))?;

    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn no_env() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn write_temp_config(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(contents.as_bytes()).expect("write");
        file
    }

    fn load_file(contents: &str) -> Result<GatewayConfig, ConfigError> {
        let file = write_temp_config(contents);
        load(Some(file.path()), &no_env(), &CliOverrides::default())
    }

    fn cluster<'a>(config: &'a GatewayConfig, id: &str) -> &'a ClusterConfig {
        config
            .clusters
            .iter()
            .find_map(|(cluster_id, cluster)| (cluster_id.as_str() == id).then_some(cluster))
            .expect("configured cluster")
    }

    fn problems(error: ConfigError) -> Vec<String> {
        match error {
            ConfigError::Invalid(problems) => problems,
            other => panic!("expected Invalid, got: {other:?}"),
        }
    }

    #[test]
    fn defaults_when_no_sources() {
        let config = load(None, &no_env(), &CliOverrides::default()).unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["127.0.0.1:9123"]
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 32 * 1024 * 1024);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert_eq!(config.metadata.default_page_size, 100);
        assert_eq!(config.metadata.max_page_size, 1000);
        assert_eq!(config.write.max_rows, 10_000);
        assert_eq!(
            config.write.max_delivery_time.get(),
            Duration::from_secs(20)
        );
        assert_eq!(config.shutdown.drain_timeout.get(), Duration::from_secs(30));
        assert!(config.warnings().is_empty());
    }

    #[test]
    fn lookup_defaults_match_the_documented_input_caps() {
        let config = load(None, &no_env(), &CliOverrides::default()).unwrap();
        assert_eq!(config.lookup.max_keys, 128);
        assert_eq!(config.lookup.max_prefixes, 16);
        assert_eq!(config.lookup.max_rows_per_prefix, 1000);
        assert_eq!(config.lookup.max_key_bytes.bytes(), 1024 * 1024);
        assert_eq!(config.lookup.queue_size, 4096);
        assert_eq!(config.lookup.max_retries, 3);
        assert_eq!(config.lookup.max_concurrent, 32);
    }

    #[test]
    fn file_overrides_defaults() {
        let config = load_file(
            r#"
            [server.rest]
            bind_address = "127.0.0.1:18080"
            request_timeout = "5s"
            max_body_bytes = "2MiB"

            [clusters.default]
            bootstrap_servers = ["fluss-1:9123", "fluss-2:9123"]

            [metadata]
            default_page_size = 25

            [write]
            max_delivery_time = "4s"
            "#,
        )
        .unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:18080".parse().unwrap()
        );
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(5)
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 2 * 1024 * 1024);
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["fluss-1:9123", "fluss-2:9123"]
        );
        assert_eq!(config.metadata.default_page_size, 25);
        assert_eq!(config.write.max_delivery_time.get(), Duration::from_secs(4));
    }

    #[test]
    fn write_delivery_and_response_budget_must_fit_request_timeout() {
        let error = load_file(
            r#"
            [server.rest]
            request_timeout = "20s"

            [write]
            max_delivery_time = "20s"
            "#,
        )
        .unwrap_err();

        assert!(problems(error).iter().any(|problem| {
            problem.contains("write.max_delivery_time") && problem.contains("250ms")
        }));
    }

    #[test]
    fn env_overrides_file() {
        let file = write_temp_config(
            r#"
            [server.rest]
            bind_address = "127.0.0.1:18080"

            [server.metrics]
            enabled = true

            [clusters.default]
            bootstrap_servers = ["from-file:9123"]
            "#,
        );
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        env.insert(
            "FLUSS_GATEWAY__SERVER_METRICS__ENABLED".to_string(),
            "false".to_string(),
        );
        env.insert(
            "FLUSS_GATEWAY__CLUSTERS__DEFAULT__BOOTSTRAP_SERVERS".to_string(),
            "env-1:9123, env-2:9123".to_string(),
        );
        env.insert(
            "FLUSS_GATEWAY__LOOKUP__MAX_PREFIXES".to_string(),
            "4".to_string(),
        );
        env.insert("PATH".to_string(), "/usr/bin".to_string());

        let config = load(Some(file.path()), &env, &CliOverrides::default()).unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:28080".parse().unwrap()
        );
        assert!(!config.server.metrics.enabled);
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["env-1:9123", "env-2:9123"]
        );
        assert_eq!(config.lookup.max_prefixes, 4);
    }

    #[test]
    fn env_single_value_bootstrap_list() {
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__CLUSTERS__DEFAULT__BOOTSTRAP_SERVERS".to_string(),
            "solo:9123".to_string(),
        );
        let config = load(None, &env, &CliOverrides::default()).unwrap();
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["solo:9123"]
        );
    }

    #[test]
    fn multiple_clusters_and_exact_environment_paths_are_supported() {
        let file = write_temp_config(
            r#"
            [clusters.default]
            bootstrap_servers = ["default:9123"]

            [clusters.analytics_eu]
            bootstrap_servers = ["file:9123"]
            "#,
        );
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__CLUSTERS__ANALYTICS_EU__BOOTSTRAP_SERVERS".to_string(),
            "analytics-1:9123,analytics-2:9123".to_string(),
        );
        let config = load(Some(file.path()), &env, &CliOverrides::default()).unwrap();
        assert_eq!(config.clusters.len(), 2);
        assert_eq!(
            cluster(&config, "analytics_eu").bootstrap_servers,
            vec!["analytics-1:9123", "analytics-2:9123"]
        );
    }

    #[test]
    fn cluster_map_and_ids_are_strict() {
        let error = load_file("[clusters]\n").unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|problem| problem.contains("at least one cluster"))
        );

        for id in ["Default", "two-clusters", "_hidden"] {
            let error = load_file(&format!(
                "[clusters.{id}]\nbootstrap_servers = [\"127.0.0.1:9123\"]\n"
            ))
            .unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{id}: {error:?}");
        }
    }

    #[test]
    fn cli_overrides_env_and_file() {
        let file = write_temp_config("[server.rest]\nbind_address = \"127.0.0.1:18080\"\n");
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        let cli = CliOverrides {
            bind_address: Some("127.0.0.1:38080".to_string()),
        };
        let config = load(Some(file.path()), &env, &cli).unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:38080".parse().unwrap()
        );
    }

    #[test]
    fn missing_file_reported() {
        let error = load(
            Some(Path::new("/nonexistent/fluss-gateway.toml")),
            &no_env(),
            &CliOverrides::default(),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigError::Io(_)), "got: {error:?}");
    }

    #[test]
    fn unknown_file_field_rejected() {
        let error = load_file("[server.rest]\nbind_addres = \"127.0.0.1:8080\"\n").unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("bind_addres"), "got: {error}");
    }

    #[test]
    fn malformed_file_reports_line() {
        let error = load_file("[lookup]\nmax_keys = 1\nmax_keys = 2\n").unwrap_err();
        assert!(error.to_string().contains("line 3"), "got: {error}");
    }

    #[test]
    fn unknown_section_rejected() {
        let error = load_file("[query]\nmax_concurrent = 32\n").unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("query"), "got: {error}");
    }

    #[test]
    fn unknown_env_key_rejected() {
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRES".to_string(),
            "127.0.0.1:8080".to_string(),
        );
        let error = load(None, &env, &CliOverrides::default()).unwrap_err();
        let ConfigError::UnknownEnvKey(key) = &error else {
            panic!("expected UnknownEnvKey, got: {error:?}");
        };
        assert_eq!(key, "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRES");
    }

    #[test]
    fn unknown_env_section_rejected() {
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__QUERY__ENABLED".to_string(),
            "true".to_string(),
        );
        let error = load(None, &env, &CliOverrides::default()).unwrap_err();
        assert!(
            matches!(error, ConfigError::UnknownEnvKey(_)),
            "got: {error:?}"
        );
    }

    #[test]
    fn file_error_under_a_section_with_an_env_override_names_the_file() {
        let file = write_temp_config("[server]\ninstance_i = \"x\"\n");
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("instance_i"), "got: {error}");
    }

    #[test]
    fn env_string_values_keep_commas_outside_list_keys() {
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER__INSTANCE_ID".to_string(),
            "gateway-a".to_string(),
        );
        let config = load(None, &env, &CliOverrides::default()).unwrap();
        assert_eq!(config.server.instance_id.as_deref(), Some("gateway-a"));

        env.insert(
            "FLUSS_GATEWAY__SERVER__INSTANCE_ID".to_string(),
            "a,b".to_string(),
        );
        let error = load(None, &env, &CliOverrides::default()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("FLUSS_GATEWAY__SERVER__INSTANCE_ID")
                || error.to_string().contains("instance_id"),
            "got: {error}"
        );
    }

    #[test]
    fn invalid_env_value_names_the_variable() {
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__LOOKUP__MAX_KEYS".to_string(),
            "many".to_string(),
        );
        let error = load(None, &env, &CliOverrides::default()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("FLUSS_GATEWAY__LOOKUP__MAX_KEYS"),
            "got: {error}"
        );
    }

    #[test]
    fn invalid_cli_value_names_the_flag() {
        let cli = CliOverrides {
            bind_address: Some("not-an-address".to_string()),
        };
        let error = load(None, &no_env(), &cli).unwrap_err();
        assert!(error.to_string().contains("--bind-address"), "got: {error}");
    }

    #[test]
    fn invalid_duration_rejected() {
        for bad in ["60", "60 s", "6.5s", "s", "60d", "-1s"] {
            let error = load_file(&format!("[shutdown]\ndrain_timeout = \"{bad}\"\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{bad}: {error:?}");
            assert!(
                error.to_string().contains("drain_timeout"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn invalid_byte_size_rejected() {
        for bad in ["\"4Mb\"", "\"MiB\"", "-1", "\"1.5MiB\""] {
            let error = load_file(&format!("[lookup]\nmax_key_bytes = {bad}\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{bad}: {error:?}");
            assert!(
                error.to_string().contains("max_key_bytes"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn zero_durations_and_sizes_rejected_while_parsing() {
        for (key, contents) in [
            ("cache_ttl", "[metadata]\ncache_ttl = \"0s\"\n"),
            ("drain_timeout", "[shutdown]\ndrain_timeout = \"0ms\"\n"),
            ("max_body_bytes", "[server.rest]\nmax_body_bytes = 0\n"),
            ("max_key_bytes", "[lookup]\nmax_key_bytes = \"0MiB\"\n"),
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{key}: {error:?}");
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn zero_counts_rejected_by_validation() {
        let error = load_file(
            "[metadata]\ndefault_page_size = 0\nmax_page_size = 0\ncache_max_entries = 0\n\
             [lookup]\nmax_keys = 0\nmax_prefixes = 0\nmax_rows_per_prefix = 0\nqueue_size = 0\nmax_concurrent = 0\n\
             [write]\nmax_rows = 0\n",
        )
        .unwrap_err();
        let problems = problems(error);
        for key in [
            "metadata.default_page_size",
            "metadata.max_page_size",
            "metadata.cache_max_entries",
            "lookup.max_keys",
            "lookup.max_prefixes",
            "lookup.max_rows_per_prefix",
            "lookup.queue_size",
            "lookup.max_concurrent",
            "write.max_rows",
        ] {
            assert!(
                problems.iter().any(|p| p.contains(key)),
                "missing problem for {key}: {problems:?}"
            );
        }
    }

    #[test]
    fn removed_and_out_of_scope_configuration_keys_are_rejected() {
        for contents in [
            // Scan and cursor state, dropped with the stateless contract.
            "[scan]\nmax_open_global = 8\n",
            "[scan]\ncursor_ttl = \"1m\"\n",
            // Rate limiting, dropped by directive.
            "[server.rest]\nmax_concurrent_requests = 64\n",
            "[write]\nmax_concurrent_requests = 8\n",
            // Authentication and transport security, out of scope.
            "[security]\nauthentication = \"trust\"\n",
            "[server]\ntls_cert = \"/etc/tls.pem\"\n",
        ] {
            assert!(load_file(contents).is_err(), "accepted: {contents}");
        }
    }

    #[test]
    fn empty_bootstrap_rejected() {
        let error = load_file("[clusters.default]\nbootstrap_servers = []\n").unwrap_err();
        assert!(matches!(error, ConfigError::Invalid(_)), "got: {error:?}");
    }

    #[test]
    fn contradictory_page_sizes_rejected() {
        let error =
            load_file("[metadata]\ndefault_page_size = 100\nmax_page_size = 10\n").unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("metadata.default_page_size must not exceed"))
        );

        let error = load_file(&format!(
            "[metadata]\nmax_page_size = {}\n",
            METADATA_MAX_PAGE_SIZE_CEILING + 1
        ))
        .unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("the fixed ceiling of"))
        );
    }

    #[test]
    fn contradictory_health_timing_rejected() {
        let error = load_file(
            "[health]\nprobe_interval = \"5s\"\nprobe_timeout = \"10s\"\n\
             reconnect_initial_backoff = \"1m\"\nreconnect_max_backoff = \"10s\"\n",
        )
        .unwrap_err();
        let problems = problems(error);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("health.probe_timeout must not exceed")),
            "{problems:?}"
        );
        assert!(
            problems
                .iter()
                .any(|p| p.contains("health.reconnect_initial_backoff must not exceed")),
            "{problems:?}"
        );
    }

    #[test]
    fn metrics_address_must_differ_from_rest_address() {
        let error = load_file(
            "[server.rest]\nbind_address = \"127.0.0.1:9095\"\n[server.metrics]\nbind_address = \"127.0.0.1:9095\"\n",
        )
        .unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("server.metrics.bind_address must differ"))
        );
    }

    #[test]
    fn non_loopback_bind_is_accepted_without_an_instance_id_but_warns() {
        let config = load_file("[server.rest]\nbind_address = \"0.0.0.0:8080\"\n").unwrap();
        assert!(config.server.instance_id.is_none());
        assert_eq!(config.warnings().len(), 1);
        assert!(config.warnings()[0].contains("not loopback"));
        assert!(
            config.warnings()[0].contains("The REST listener has no authentication or TLS"),
            "{:?}",
            config.warnings()
        );
    }

    #[test]
    fn malformed_instance_id_rejected() {
        let error = load_file("[server]\ninstance_id = \"has space\"\n").unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("server.instance_id must be 1-128 ASCII"))
        );
    }

    #[test]
    fn duration_units() {
        assert_eq!(
            ConfigDuration::parse("250ms").unwrap().get(),
            Duration::from_millis(250)
        );
        assert_eq!(
            ConfigDuration::parse("15m").unwrap().get(),
            Duration::from_secs(900)
        );
        assert_eq!(
            ConfigDuration::parse("2h").unwrap().get(),
            Duration::from_secs(7200)
        );
        assert!(ConfigDuration::parse("0s").is_err());
    }

    #[test]
    fn byte_size_units() {
        assert_eq!(ByteSize::parse("512").unwrap().bytes(), 512);
        assert_eq!(ByteSize::parse("512B").unwrap().bytes(), 512);
        assert_eq!(ByteSize::parse("4KB").unwrap().bytes(), 4000);
        assert_eq!(ByteSize::parse("4KiB").unwrap().bytes(), 4096);
        assert_eq!(ByteSize::parse("1GiB").unwrap().bytes(), 1024 * 1024 * 1024);
        assert!(ByteSize::parse("4TB").is_err());
        assert!(ByteSize::parse("0").is_err());
    }

    #[test]
    fn env_suffix_paths() {
        assert_eq!(
            env_suffix_to_path("SERVER_REST__BIND_ADDRESS"),
            "server.rest.bind_address"
        );
        assert_eq!(
            env_suffix_to_path("SERVER__INSTANCE_ID"),
            "server.instance_id"
        );
        assert_eq!(env_suffix_to_path("LOOKUP__MAX_KEYS"), "lookup.max_keys");
        assert_eq!(
            env_suffix_to_path("METADATA__MAX_PAGE_SIZE"),
            "metadata.max_page_size"
        );
    }
}
