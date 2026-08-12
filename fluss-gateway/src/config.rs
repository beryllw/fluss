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
//! One `gateway.yaml` file plus complete env overrides plus targeted CLI overrides. Precedence:
//! CLI > env > file > defaults. Parsing is strict: unknown keys (file or env) are rejected, durations must be
//! `<int><ms|s|m|h>`, byte sizes are plain integers or `<int><B|KB|KiB|MB|MiB|GB|GiB>`, and both reject zero.
//!
//! # Schema shape
//!
//! The file is YAML whose top level is a mapping of **flat dotted keys**, exactly as documented by FIP-49
//! §Gateway Configuration and aligned with the Fluss `server.yaml` convention:
//!
//! ```yaml
//! gateway.rest.listen: 0.0.0.0:8080
//! gateway.rest.write.max-request-bytes: 32MiB
//! ```
//!
//! Keys named by the FIP keep their FIP spelling; internal keys the FIP does not cover (shutdown draining)
//! follow the same `gateway.<area>.<kebab-key>` style. Each flat key is
//! translated to a field of the typed sections below before deserialization, so `deny_unknown_fields` stays
//! meaningful per subsystem and an unrecognised flat key is rejected with the exact name the operator wrote.
//! This supersedes the earlier sectioned TOML schema by explicit user decision: the REST contract and the
//! configuration surface should quote one vocabulary, the FIP's.
//!
//! There is deliberately **no TLS section**: transport
//! security terminates at a fronting proxy.
//!
//! Env override convention (unchanged): `FLUSS_GATEWAY__<SECTION>__<KEY>`, with `__` separating path
//! components of the *internal* sections. For example, `FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS` overrides
//! the REST listener.

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
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        let (digits, unit) = split_number_and_unit(s);
        if digits.is_empty() {
            return Err(format!(
                "invalid duration {s:?}: expected <integer><ms|s|m|h>"
            ));
        }
        let value: u64 = digits
            .parse()
            .map_err(|e| format!("invalid duration {s:?}: {e}"))?;
        let overflow = || format!("invalid duration {s:?}: value is too large");
        let duration = match unit {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value.checked_mul(60).ok_or_else(overflow)?),
            "h" => Duration::from_secs(value.checked_mul(3600).ok_or_else(overflow)?),
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
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
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

/// The validated gateway configuration: everything the process needs before it binds a listener.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayConfig {
    pub server: ServerConfig,
    pub shutdown: ShutdownConfig,
}

impl GatewayConfig {
    /// Checks the invariants that span more than one field. Single-field syntax and non-zero rules are enforced
    /// while deserializing. Called by [`load`] and exposed for tests and programmatic construction.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = Vec::new();
        self.validate_identity(&mut problems);
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
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
        let mut warnings = Vec::new();
        if !self.server.rest.bind_address.ip().is_loopback() {
            warnings.push(format!(
                "server.rest.bind_address {} is not loopback. The REST listener accepts \
                 unauthenticated requests and has no TLS",
                self.server.rest.bind_address
            ));
        }
        warnings
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

/// The flat `gateway.*` file vocabulary, mapped to the dotted paths of the typed sections. FIP-named keys keep
/// their FIP spelling; the remaining internal keys follow the same `gateway.<area>.<kebab-key>` style.
///
/// `gateway.rest.write.request-timeout` maps to the shared REST deadline: the gateway runs every request,
/// not only writes, under that server-side budget.
const FLAT_FILE_KEYS: &[(&str, &str)] = &[
    ("gateway.instance-id", "server.instance_id"),
    ("gateway.rest.listen", "server.rest.bind_address"),
    (
        "gateway.rest.write.request-timeout",
        "server.rest.request_timeout",
    ),
    (
        "gateway.rest.write.max-request-bytes",
        "server.rest.max_body_bytes",
    ),
    ("gateway.metrics.enabled", "server.metrics.enabled"),
    (
        "gateway.metrics.exporter.prometheus.listen",
        "server.metrics.bind_address",
    ),
    ("gateway.shutdown.drain-timeout", "shutdown.drain_timeout"),
];

/// Resolves one flat file key against the vocabulary, or rejects it with the exact name the operator wrote.
fn resolve_flat_key(key: &str) -> Result<String, ConfigError> {
    if let Some((_, path)) = FLAT_FILE_KEYS.iter().find(|(flat, _)| *flat == key) {
        return Ok((*path).to_string());
    }
    Err(ConfigError::Parse(format!(
        "unknown configuration key: {key}"
    )))
}

/// Converts one YAML scalar or sequence into the internal TOML value model. Nested mappings are rejected
/// because the file contract is flat dotted keys.
fn yaml_to_toml(value: &serde_yaml::Value, key: &str) -> Result<Value, ConfigError> {
    match value {
        serde_yaml::Value::Bool(v) => Ok(Value::Boolean(*v)),
        serde_yaml::Value::Number(v) => {
            if let Some(int) = v.as_i64() {
                Ok(Value::Integer(int))
            } else if let Some(float) = v.as_f64() {
                Ok(Value::Float(float))
            } else {
                Err(ConfigError::Parse(format!("{key}: unsupported number")))
            }
        }
        serde_yaml::Value::String(v) => Ok(Value::String(v.clone())),
        serde_yaml::Value::Sequence(items) => Ok(Value::Array(
            items
                .iter()
                .map(|item| yaml_to_toml(item, key))
                .collect::<Result<_, _>>()?,
        )),
        serde_yaml::Value::Null => Err(ConfigError::Parse(format!("{key}: value is missing"))),
        serde_yaml::Value::Mapping(_) | serde_yaml::Value::Tagged(_) => Err(ConfigError::Parse(
            format!("{key}: nested values are not allowed, configuration keys are flat"),
        )),
    }
}

/// Parses the flat-key YAML file into the internal table model.
fn read_config_file(contents: &str) -> Result<toml::Table, ConfigError> {
    let document: serde_yaml::Value =
        serde_yaml::from_str(contents).map_err(|e| ConfigError::Parse(e.to_string()))?;
    let mut table = toml::Table::new();
    if document.is_null() {
        return Ok(table);
    }
    let mapping = document.as_mapping().ok_or_else(|| {
        ConfigError::Parse(
            "configuration must be a mapping of flat dotted keys (gateway.…: value)".to_string(),
        )
    })?;

    for (key, value) in mapping {
        let key = key
            .as_str()
            .ok_or_else(|| ConfigError::Parse("configuration keys must be strings".to_string()))?;
        let path = resolve_flat_key(key)?;
        insert_path(&mut table, &path, yaml_to_toml(value, key)?);
    }
    Ok(table)
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
        table = read_config_file(&contents)?;
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
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 32 * 1024 * 1024);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert_eq!(config.shutdown.drain_timeout.get(), Duration::from_secs(30));
        assert!(config.warnings().is_empty());
    }

    #[test]
    fn file_overrides_defaults() {
        let config = load_file(
            r#"
gateway.rest.listen: 127.0.0.1:18080
gateway.rest.write.request-timeout: 5s
gateway.rest.write.max-request-bytes: 2MiB
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
    }

    /// The configuration surface documented by FIP-49 §Gateway Configuration, restricted to the keys the
    /// gateway implements today, parses as one flat dotted-key YAML document.
    #[test]
    fn fip_yaml_example_parses_with_flat_dotted_keys() {
        let config = load_file(
            r#"
gateway.rest.listen: 0.0.0.0:8080
gateway.rest.write.max-request-bytes: 32MiB
gateway.rest.write.request-timeout: 30s
gateway.metrics.enabled: true
gateway.metrics.exporter.prometheus.listen: 0.0.0.0:9095
"#,
        )
        .unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "0.0.0.0:8080".parse().unwrap()
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 32 * 1024 * 1024);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert!(config.server.metrics.enabled);
        assert_eq!(
            config.server.metrics.bind_address,
            "0.0.0.0:9095".parse().unwrap()
        );
    }

    /// A key outside the documented vocabulary is rejected with the exact flat name the operator wrote,
    /// not a translated internal path.
    #[test]
    fn unknown_flat_key_is_rejected_with_its_original_name() {
        for contents in [
            "gateway.rest.listenn: 0.0.0.0:8080\n",
            "rest.listen: 0.0.0.0:8080\n",
            "gateway.rest.lookup.max-keyz: 5\n",
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
            let key = contents.split(':').next().unwrap();
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn env_overrides_file() {
        let file = write_temp_config(
            r#"
gateway.rest.listen: 127.0.0.1:18080
gateway.metrics.enabled: true
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
        env.insert("PATH".to_string(), "/usr/bin".to_string());

        let config = load(Some(file.path()), &env, &CliOverrides::default()).unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:28080".parse().unwrap()
        );
        assert!(!config.server.metrics.enabled);
    }

    #[test]
    fn cli_overrides_env_and_file() {
        let file = write_temp_config("gateway.rest.listen: 127.0.0.1:18080\n");
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
            Some(Path::new("/nonexistent/gateway.yaml")),
            &no_env(),
            &CliOverrides::default(),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigError::Io(_)), "got: {error:?}");
    }

    #[test]
    fn unknown_file_field_rejected() {
        let error = load_file("gateway.rest.listenn: 127.0.0.1:8080\n").unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(
            error.to_string().contains("gateway.rest.listenn"),
            "got: {error}"
        );
    }

    #[test]
    fn malformed_file_reports_position() {
        let error = load_file("gateway.rest.listen: [1\n").unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("line"), "got: {error}");
    }

    #[test]
    fn duplicate_flat_key_rejected() {
        let error =
            load_file("gateway.rest.listen: 127.0.0.1:8080\ngateway.rest.listen: 127.0.0.1:8081\n")
                .unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("duplicate"), "got: {error}");
    }

    #[test]
    fn unknown_section_rejected() {
        let error = load_file("gateway.query.max-concurrent: 32\n").unwrap_err();
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
        let file = write_temp_config("gateway.shutdown.drain-timeout: 0s\n");
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("drain_timeout"), "got: {error}");
        assert!(
            !error.to_string().contains("FLUSS_GATEWAY__"),
            "file problem misattributed to the env override: {error}"
        );
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
            "FLUSS_GATEWAY__SERVER_REST__MAX_BODY_BYTES".to_string(),
            "many".to_string(),
        );
        let error = load(None, &env, &CliOverrides::default()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("FLUSS_GATEWAY__SERVER_REST__MAX_BODY_BYTES"),
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
            let error =
                load_file(&format!("gateway.shutdown.drain-timeout: \"{bad}\"\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{bad}: {error:?}");
            assert!(
                error.to_string().contains("drain_timeout"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn overflowing_duration_is_rejected_rather_than_saturated() {
        // A syntactically valid but astronomically large duration must be refused at parse time,
        // not silently clamped, so it can never reach an `Instant + Duration` overflow at runtime.
        let error = ConfigDuration::parse("18446744073709551615h").unwrap_err();
        assert!(error.contains("too large"), "got: {error}");
    }

    #[test]
    fn invalid_byte_size_rejected() {
        for bad in ["\"4Mb\"", "\"MiB\"", "-1", "\"1.5MiB\""] {
            let error =
                load_file(&format!("gateway.rest.write.max-request-bytes: {bad}\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{bad}: {error:?}");
            assert!(
                error.to_string().contains("max_body_bytes"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn zero_durations_and_sizes_rejected_while_parsing() {
        for (key, contents) in [
            ("drain_timeout", "gateway.shutdown.drain-timeout: 0ms\n"),
            (
                "max_body_bytes",
                "gateway.rest.write.max-request-bytes: 0\n",
            ),
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{key}: {error:?}");
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn removed_and_out_of_scope_configuration_keys_are_rejected() {
        for contents in [
            // Scan and cursor state, dropped with the stateless contract.
            "gateway.scan.max-open-global: 8\n",
            "gateway.scan.cursor-ttl: 1m\n",
            // Transport security, out of scope (TLS terminates at a fronting proxy).
            "gateway.tls.cert: /etc/tls.pem\n",
        ] {
            assert!(load_file(contents).is_err(), "accepted: {contents}");
        }
    }

    #[test]
    fn metrics_address_must_differ_from_rest_address() {
        let error = load_file(
            "gateway.rest.listen: 127.0.0.1:9095\ngateway.metrics.exporter.prometheus.listen: 127.0.0.1:9095\n",
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
        let config = load_file("gateway.rest.listen: 0.0.0.0:8080\n").unwrap();
        assert!(config.server.instance_id.is_none());
        assert_eq!(config.warnings().len(), 1);
        assert!(config.warnings()[0].contains("not loopback"));
        // The warning calls out the unauthenticated exposure.
        assert!(
            config.warnings()[0].contains("accepts unauthenticated requests"),
            "{:?}",
            config.warnings()
        );
    }

    #[test]
    fn malformed_instance_id_rejected() {
        let error = load_file("gateway.instance-id: has space\n").unwrap_err();
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
        assert_eq!(
            env_suffix_to_path("SHUTDOWN__DRAIN_TIMEOUT"),
            "shutdown.drain_timeout"
        );
    }
}
