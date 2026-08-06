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
//! gateway.clusters: default
//! gateway.cluster.default.bootstrap.servers: 127.0.0.1:9123
//! gateway.rest.listen: 0.0.0.0:8080
//! gateway.rest.write.max-request-bytes: 32MiB
//! ```
//!
//! Keys named by the FIP keep their FIP spelling; internal keys the FIP does not cover (health probing,
//! metadata caching, shutdown draining) follow the same `gateway.<area>.<kebab-key>` style. Each flat key is
//! translated to a field of the typed sections below before deserialization, so `deny_unknown_fields` stays
//! meaningful per subsystem and an unrecognised flat key is rejected with the exact name the operator wrote.
//! This supersedes the earlier sectioned TOML schema by explicit user decision: the REST contract and the
//! configuration surface should quote one vocabulary, the FIP's.
//!
//! There is deliberately **no concurrency-permit key and no TLS section**. The gateway applies no rate
//! limiting (the only request bounds are the input-validation caps under `gateway.rest.*`), and transport
//! security terminates at a fronting proxy. Client-to-gateway authentication is configured under
//! `gateway.security.*` and performed by the pluggable [`crate::auth::Authenticator`].
//!
//! Env override convention (unchanged): `FLUSS_GATEWAY__<SECTION>__<KEY>`, with `__` separating path
//! components of the *internal* sections. For example, `FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS` overrides
//! the REST listener, while `FLUSS_GATEWAY__CLUSTERS__ANALYTICS_EU__BOOTSTRAP_SERVERS` overrides
//! `[clusters.analytics_eu].bootstrap_servers`.

use crate::application::types::ClusterId;
use crate::auth::{
    Authenticator, ConfigUserStoreAuthenticator, Secret, StoredSecret, TrustAuthenticator,
    parse_user_table,
};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
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

/// How the gateway identifies itself to one Fluss cluster
/// (`gateway.cluster.<id>.connection.identity-mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdentityMode {
    /// One shared connection authenticated as the service account itself; Fluss sees only the
    /// gateway account. The default, and the super-user non-propagating transition mode.
    #[default]
    Service,
    /// One connection per authenticated user, carrying the SASL authorization id, so Fluss
    /// authorizes as the impersonated end user (act-as).
    User,
}

/// One `[clusters.<id>]` table, which configures how to reach a Fluss cluster.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    #[serde(deserialize_with = "deserialize_server_list")]
    pub bootstrap_servers: Vec<String>,
    pub connect_timeout: ConfigDuration,
    pub request_timeout: ConfigDuration,
    /// The SASL service account the gateway authenticates to Fluss with
    /// (`gateway.cluster.<id>.connection.service.account`). Unset means a plaintext connection.
    pub service_account: Option<String>,
    /// The service account's password (`gateway.cluster.<id>.connection.service.secret`).
    /// Must be set exactly when `service_account` is.
    pub service_password: Option<Secret>,
    /// The identity the effective principal on Fluss is derived from
    /// (`gateway.cluster.<id>.connection.identity-mode`).
    pub identity_mode: IdentityMode,
    /// Cap on per-user act-as connections (`gateway.cluster.<id>.connection.max`), user mode
    /// only; exceeding it answers 429. Defaults to 512 when unset.
    pub connection_max: Option<u32>,
    /// Idle reclamation for per-user connections (`gateway.cluster.<id>.connection.idle-timeout`),
    /// user mode only. Defaults to 10 minutes when unset.
    pub connection_idle_timeout: Option<ConfigDuration>,
}

impl ClusterConfig {
    /// The effective per-user connection cap of the user identity mode.
    pub fn effective_connection_max(&self) -> usize {
        self.connection_max.unwrap_or(512) as usize
    }

    /// The effective per-user idle reclamation timeout of the user identity mode.
    pub fn effective_connection_idle_timeout(&self) -> Duration {
        self.connection_idle_timeout
            .map(ConfigDuration::get)
            .unwrap_or(Duration::from_secs(600))
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["127.0.0.1:9123".to_string()],
            connect_timeout: ConfigDuration::from_secs(10),
            request_timeout: ConfigDuration::from_secs(10),
            service_account: None,
            service_password: None,
            identity_mode: IdentityMode::Service,
            connection_max: None,
            connection_idle_timeout: None,
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

/// The client-to-gateway authentication mode (`gateway.security.authentication`).
///
/// The FIP also lists `trusted-header` and `oidc`; they arrive as further [`Authenticator`]
/// plugins and are rejected here until implemented, instead of silently degrading to trust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthenticationMode {
    /// The claimed username is taken at face value. The default, for local use.
    #[default]
    Trust,
    /// Credentials are verified against the `gateway.security.users` table. For production.
    Password,
}

/// `[security]`, the client-to-gateway authentication configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct SecurityConfig {
    pub authentication: AuthenticationMode,
    /// The FIP user table: comma-separated `name:secret` / `name:bcrypt:<hash>` entries, kept
    /// raw here and parsed by [`parse_user_table`] during validation and authenticator
    /// construction.
    pub users: Option<String>,
}

impl SecurityConfig {
    /// Parses the configured user table, attributing failures to `gateway.security.users`.
    fn parsed_users(&self) -> Result<HashMap<String, StoredSecret>, String> {
        parse_user_table(self.users.as_deref().unwrap_or(""))
            .map_err(|e| format!("gateway.security.users: {e}"))
    }

    /// Builds the one global [`Authenticator`] shared by every frontend.
    ///
    /// Call after [`GatewayConfig::validate`]; a malformed user table still fails here rather
    /// than panicking, so programmatic construction stays safe.
    pub fn build_authenticator(&self) -> Result<Arc<dyn Authenticator>, ConfigError> {
        match self.authentication {
            AuthenticationMode::Trust => Ok(Arc::new(TrustAuthenticator::new())),
            AuthenticationMode::Password => {
                let users = self
                    .parsed_users()
                    .map_err(|problem| ConfigError::Invalid(vec![problem]))?;
                Ok(Arc::new(ConfigUserStoreAuthenticator::new(users)))
            }
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
    pub security: SecurityConfig,
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
            security: SecurityConfig::default(),
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
        self.validate_security(&mut problems);

        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// Rejects a password mode whose user table is missing, empty, or malformed. Trust mode is
    /// never rejected here; a configured-but-ignored user table is only a warning.
    fn validate_security(&self, problems: &mut Vec<String>) {
        if self.security.authentication != AuthenticationMode::Password {
            return;
        }
        match self.security.parsed_users() {
            Ok(users) if users.is_empty() => problems.push(
                "gateway.security.users must configure at least one user when gateway.security.authentication is password"
                    .to_string(),
            ),
            Ok(_) => {}
            Err(problem) => problems.push(problem),
        }
    }

    /// Rejects an empty cluster map, invalid IDs, unusable per-cluster bootstrap lists, and
    /// half-configured service credentials.
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
            if cluster.service_account.is_some() != cluster.service_password.is_some() {
                problems.push(format!(
                    "gateway.cluster.{id}.connection.service.account and \
                     gateway.cluster.{id}.connection.service.secret must be set together"
                ));
            }
            if cluster.identity_mode == IdentityMode::User {
                // User mode impersonates through the super-user connection, so the service
                // credentials are mandatory …
                if cluster.service_account.is_none() {
                    problems.push(format!(
                        "gateway.cluster.{id}.connection.identity-mode user requires \
                         gateway.cluster.{id}.connection.service.account credentials"
                    ));
                }
                // … and the propagated identity must be a verified one: under trust
                // authentication any client can claim any username (including the anonymous
                // fallback), which must never become an act-as identity on Fluss.
                if self.security.authentication == AuthenticationMode::Trust {
                    problems.push(format!(
                        "gateway.cluster.{id}.connection.identity-mode user requires verified \
                         client identities; set gateway.security.authentication to password"
                    ));
                }
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
        let mut warnings = Vec::new();
        if !self.server.rest.bind_address.ip().is_loopback() {
            let exposure = match self.security.authentication {
                AuthenticationMode::Trust => {
                    "The REST listener accepts unauthenticated requests and has no TLS"
                }
                AuthenticationMode::Password => {
                    "The REST listener has no TLS, so credentials cross the network unencrypted unless a fronting proxy terminates TLS"
                }
            };
            warnings.push(format!(
                "server.rest.bind_address {} is not loopback. {exposure}",
                self.server.rest.bind_address
            ));
        }
        if self.security.authentication == AuthenticationMode::Trust
            && self.security.users.is_some()
        {
            warnings.push(
                "gateway.security.users is ignored because gateway.security.authentication is trust"
                    .to_string(),
            );
        }
        for (id, cluster) in &self.clusters {
            if cluster.identity_mode == IdentityMode::Service
                && (cluster.connection_max.is_some() || cluster.connection_idle_timeout.is_some())
            {
                warnings.push(format!(
                    "gateway.cluster.{id}.connection.max and connection.idle-timeout are ignored \
                     because connection.identity-mode is service"
                ));
            }
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

/// The flat `gateway.*` file vocabulary, mapped to the dotted paths of the typed sections. FIP-named keys keep
/// their FIP spelling; the remaining internal keys follow the same `gateway.<area>.<kebab-key>` style.
///
/// `gateway.rest.write.request-timeout` maps to the shared REST deadline: the gateway runs every request,
/// not only writes, under that server-side budget, and the write path additionally bounds native delivery
/// with `gateway.rest.write.max-delivery-time` below it.
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
    ("gateway.rest.write.max-rows", "write.max_rows"),
    (
        "gateway.rest.write.max-delivery-time",
        "write.max_delivery_time",
    ),
    ("gateway.rest.lookup.max-keys", "lookup.max_keys"),
    ("gateway.rest.lookup.max-key-bytes", "lookup.max_key_bytes"),
    ("gateway.rest.lookup.queue-size", "lookup.queue_size"),
    ("gateway.rest.lookup.max-retries", "lookup.max_retries"),
    (
        "gateway.rest.lookup.max-concurrent",
        "lookup.max_concurrent",
    ),
    (
        "gateway.rest.prefix-lookup.max-prefixes",
        "lookup.max_prefixes",
    ),
    (
        "gateway.rest.prefix-lookup.max-rows-per-prefix",
        "lookup.max_rows_per_prefix",
    ),
    ("gateway.metrics.enabled", "server.metrics.enabled"),
    (
        "gateway.metrics.exporter.prometheus.listen",
        "server.metrics.bind_address",
    ),
    ("gateway.health.probe-interval", "health.probe_interval"),
    ("gateway.health.probe-timeout", "health.probe_timeout"),
    ("gateway.health.stale-after", "health.stale_after"),
    (
        "gateway.health.reconnect-initial-backoff",
        "health.reconnect_initial_backoff",
    ),
    (
        "gateway.health.reconnect-max-backoff",
        "health.reconnect_max_backoff",
    ),
    (
        "gateway.health.reconnect-attempt-timeout",
        "health.reconnect_attempt_timeout",
    ),
    (
        "gateway.metadata.default-page-size",
        "metadata.default_page_size",
    ),
    ("gateway.metadata.max-page-size", "metadata.max_page_size"),
    (
        "gateway.metadata.cache-max-entries",
        "metadata.cache_max_entries",
    ),
    ("gateway.metadata.cache-ttl", "metadata.cache_ttl"),
    ("gateway.shutdown.drain-timeout", "shutdown.drain_timeout"),
    ("gateway.security.authentication", "security.authentication"),
    ("gateway.security.users", "security.users"),
];

/// The per-cluster key suffixes allowed under `gateway.cluster.<id>.`, mapped to [`ClusterConfig`] fields.
const FLAT_CLUSTER_KEYS: &[(&str, &str)] = &[
    ("bootstrap.servers", "bootstrap_servers"),
    ("connect-timeout", "connect_timeout"),
    ("request-timeout", "request_timeout"),
    ("connection.service.account", "service_account"),
    ("connection.service.secret", "service_password"),
    ("connection.identity-mode", "identity_mode"),
    ("connection.max", "connection_max"),
    ("connection.idle-timeout", "connection_idle_timeout"),
];

/// One recognised flat configuration key.
enum FlatKey {
    /// The `gateway.clusters` declaration listing every cluster ID the file may configure.
    ClusterDeclaration,
    /// Any other vocabulary key, resolved to its dotted struct path.
    Path(String),
}

/// Resolves one flat file key against the vocabulary, or rejects it with the exact name the operator wrote.
fn resolve_flat_key(key: &str) -> Result<FlatKey, ConfigError> {
    if key == "gateway.clusters" {
        return Ok(FlatKey::ClusterDeclaration);
    }
    if let Some((_, path)) = FLAT_FILE_KEYS.iter().find(|(flat, _)| *flat == key) {
        return Ok(FlatKey::Path((*path).to_string()));
    }
    if let Some(cluster_key) = key.strip_prefix("gateway.cluster.")
        && let Some((id, suffix)) = cluster_key.split_once('.')
    {
        if let Some((_, field)) = FLAT_CLUSTER_KEYS.iter().find(|(flat, _)| *flat == suffix) {
            return Ok(FlatKey::Path(format!("clusters.{id}.{field}")));
        }
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

/// Reads the cluster IDs of a `gateway.clusters` declaration, given as CSV or as a YAML list.
fn declared_cluster_ids(value: &serde_yaml::Value) -> Result<Vec<String>, ConfigError> {
    let entries: Vec<String> = match value {
        serde_yaml::Value::String(csv) => csv.split(',').map(|id| id.trim().to_string()).collect(),
        serde_yaml::Value::Sequence(items) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_string).ok_or_else(|| {
                    ConfigError::Parse("gateway.clusters: entries must be strings".to_string())
                })
            })
            .collect::<Result<_, _>>()?,
        _ => {
            return Err(ConfigError::Parse(
                "gateway.clusters: expected a comma-separated string or a list".to_string(),
            ));
        }
    };
    Ok(entries.into_iter().filter(|id| !id.is_empty()).collect())
}

/// Parses the flat-key YAML file into the internal table model and enforces the `gateway.clusters`
/// declaration: when present, it is authoritative, so a configured but undeclared cluster is rejected and a
/// declared but unconfigured cluster gets the connection defaults.
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

    let mut declared: Option<Vec<String>> = None;
    for (key, value) in mapping {
        let key = key
            .as_str()
            .ok_or_else(|| ConfigError::Parse("configuration keys must be strings".to_string()))?;
        match resolve_flat_key(key)? {
            FlatKey::ClusterDeclaration => declared = Some(declared_cluster_ids(value)?),
            FlatKey::Path(path) => insert_path(&mut table, &path, yaml_to_toml(value, key)?),
        }
    }

    if let Some(declared) = declared {
        // The declaration is authoritative even when empty: an empty list yields an empty cluster map,
        // which validation rejects, instead of silently falling back to the built-in default cluster.
        let clusters = table
            .entry("clusters".to_string())
            .or_insert_with(|| Value::Table(toml::Table::new()))
            .as_table_mut()
            .expect("clusters table inserted above");
        for id in &declared {
            clusters
                .entry(id.clone())
                .or_insert_with(|| Value::Table(toml::Table::new()));
        }
        if let Some(clusters) = table.get("clusters").and_then(Value::as_table) {
            for id in clusters.keys() {
                if !declared.iter().any(|declared_id| declared_id == id) {
                    return Err(ConfigError::Parse(format!(
                        "gateway.cluster.{id}.* is configured but {id} is not declared in gateway.clusters"
                    )));
                }
            }
        }
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
    use crate::auth::{ClientCredential, Secret};
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
gateway.rest.listen: 127.0.0.1:18080
gateway.rest.write.request-timeout: 5s
gateway.rest.write.max-request-bytes: 2MiB
gateway.rest.write.max-delivery-time: 4s
gateway.cluster.default.bootstrap.servers: ["fluss-1:9123", "fluss-2:9123"]
gateway.metadata.default-page-size: 25
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

    /// The configuration surface documented by FIP-49 §Gateway Configuration, restricted to the keys the
    /// gateway implements today, parses as one flat dotted-key YAML document.
    #[test]
    fn fip_yaml_example_parses_with_flat_dotted_keys() {
        let config = load_file(
            r#"
gateway.clusters: default
gateway.cluster.default.bootstrap.servers: 127.0.0.1:9123
gateway.rest.listen: 0.0.0.0:8080
gateway.rest.write.max-request-bytes: 32MiB
gateway.rest.write.request-timeout: 30s
gateway.rest.lookup.max-keys: 128
gateway.rest.prefix-lookup.max-prefixes: 16
gateway.rest.prefix-lookup.max-rows-per-prefix: 1000
gateway.metrics.enabled: true
gateway.metrics.exporter.prometheus.listen: 0.0.0.0:9095
"#,
        )
        .unwrap();
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["127.0.0.1:9123"]
        );
        assert_eq!(
            config.server.rest.bind_address,
            "0.0.0.0:8080".parse().unwrap()
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 32 * 1024 * 1024);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert_eq!(config.lookup.max_keys, 128);
        assert_eq!(config.lookup.max_prefixes, 16);
        assert_eq!(config.lookup.max_rows_per_prefix, 1000);
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

    /// `gateway.clusters` is the authoritative declaration: configuring an undeclared cluster is an error.
    #[test]
    fn undeclared_cluster_keys_are_rejected() {
        let error = load_file(
            "gateway.clusters: default\ngateway.cluster.analytics.bootstrap.servers: a:9123\n",
        )
        .unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("analytics"), "got: {error}");
    }

    /// A declared cluster without further keys is reachable with the built-in connection defaults.
    #[test]
    fn declared_but_unconfigured_cluster_gets_defaults() {
        let config = load_file("gateway.clusters: default, analytics\n").unwrap();
        assert_eq!(config.clusters.len(), 2);
        assert_eq!(
            cluster(&config, "analytics").bootstrap_servers,
            vec!["127.0.0.1:9123"]
        );
    }

    /// The FIP service-account keys configure the SASL identity the gateway connects with;
    /// the secret never appears in a Debug rendering of the configuration.
    #[test]
    fn cluster_service_account_parses_and_redacts_the_secret() {
        let config = load_file(
            "gateway.cluster.default.connection.service.account: gateway_svc\n\
             gateway.cluster.default.connection.service.secret: sup3r-s3cret\n",
        )
        .unwrap();
        let default_cluster = cluster(&config, "default");
        assert_eq!(
            default_cluster.service_account.as_deref(),
            Some("gateway_svc")
        );
        assert_eq!(
            default_cluster
                .service_password
                .as_ref()
                .map(|secret| secret.expose()),
            Some("sup3r-s3cret")
        );
        assert!(
            !format!("{default_cluster:?}").contains("sup3r-s3cret"),
            "secret leaked through Debug"
        );
        // Omitting both keys keeps today's plaintext connection.
        let config = load_file("gateway.clusters: default\n").unwrap();
        assert!(cluster(&config, "default").service_account.is_none());
        assert!(cluster(&config, "default").service_password.is_none());
    }

    /// The account and its secret only make sense together: one without the other is a
    /// misconfiguration, not a half-authenticated connection.
    #[test]
    fn cluster_service_credentials_must_be_paired() {
        for contents in [
            "gateway.cluster.default.connection.service.account: gateway_svc\n",
            "gateway.cluster.default.connection.service.secret: sup3r\n",
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(
                problems(error)
                    .iter()
                    .any(|p| p.contains("connection.service")),
                "accepted: {contents}"
            );
        }
    }

    /// A complete FIP user-mode declaration parses, with the documented defaults for the
    /// connection cap and idle reclamation when the keys are omitted.
    #[test]
    fn cluster_user_identity_mode_parses_with_documented_defaults() {
        let config = load_file(
            "gateway.security.authentication: password\ngateway.security.users: alice:pw\n\
             gateway.cluster.default.connection.identity-mode: user\n\
             gateway.cluster.default.connection.service.account: gateway_svc\n\
             gateway.cluster.default.connection.service.secret: sup3r\n",
        )
        .unwrap();
        let default_cluster = cluster(&config, "default");
        assert_eq!(default_cluster.identity_mode, IdentityMode::User);
        assert_eq!(default_cluster.effective_connection_max(), 512);
        assert_eq!(
            default_cluster.effective_connection_idle_timeout(),
            Duration::from_secs(600)
        );

        let config = load_file(
            "gateway.security.authentication: password\ngateway.security.users: alice:pw\n\
             gateway.cluster.default.connection.identity-mode: user\n\
             gateway.cluster.default.connection.service.account: gateway_svc\n\
             gateway.cluster.default.connection.service.secret: sup3r\n\
             gateway.cluster.default.connection.max: 16\n\
             gateway.cluster.default.connection.idle-timeout: 1m\n",
        )
        .unwrap();
        assert_eq!(cluster(&config, "default").effective_connection_max(), 16);
        assert_eq!(
            cluster(&config, "default").effective_connection_idle_timeout(),
            Duration::from_secs(60)
        );
    }

    /// User mode propagates identities to Fluss, so it demands super-user credentials and a
    /// verifying client authenticator: trust-claimed names must never become act-as identities.
    #[test]
    fn cluster_user_identity_mode_demands_credentials_and_verified_identities() {
        // Missing service credentials.
        let error = load_file(
            "gateway.security.authentication: password\ngateway.security.users: alice:pw\n\
             gateway.cluster.default.connection.identity-mode: user\n",
        )
        .unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("requires") && p.contains("service.account")),
        );

        // Trust authentication (the default) cannot feed act-as identities.
        let error = load_file(
            "gateway.cluster.default.connection.identity-mode: user\n\
             gateway.cluster.default.connection.service.account: gateway_svc\n\
             gateway.cluster.default.connection.service.secret: sup3r\n",
        )
        .unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("verified") && p.contains("password")),
        );
    }

    /// The connection pool keys have no effect under the default service mode and say so.
    #[test]
    fn cluster_pool_keys_warn_under_service_mode() {
        let config = load_file("gateway.cluster.default.connection.max: 16\n").unwrap();
        assert!(
            config
                .warnings()
                .iter()
                .any(|w| w.contains("connection.max") && w.contains("ignored")),
            "{:?}",
            config.warnings()
        );
    }

    /// The bootstrap list accepts both the FIP csv form and a YAML list.
    #[test]
    fn bootstrap_servers_accept_csv_and_yaml_list() {
        let config =
            load_file("gateway.cluster.default.bootstrap.servers: a:9123, b:9123\n").unwrap();
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["a:9123", "b:9123"]
        );

        let config =
            load_file("gateway.cluster.default.bootstrap.servers: [a:9123, b:9123]\n").unwrap();
        assert_eq!(
            cluster(&config, "default").bootstrap_servers,
            vec!["a:9123", "b:9123"]
        );
    }

    #[test]
    fn security_defaults_to_trust_without_users() {
        let config = load(None, &no_env(), &CliOverrides::default()).unwrap();
        assert_eq!(config.security.authentication, AuthenticationMode::Trust);
        assert!(config.security.users.is_none());
        assert!(config.warnings().is_empty());
    }

    #[tokio::test]
    async fn security_trust_mode_builds_a_trusting_authenticator() {
        let config = load_file("gateway.security.authentication: trust\n").unwrap();
        let authenticator = config.security.build_authenticator().unwrap();
        let principal = authenticator
            .authenticate(ClientCredential::Trust {
                username: "anyone".into(),
            })
            .await
            .unwrap();
        assert_eq!(principal.name, "anyone");
    }

    #[tokio::test]
    async fn security_password_mode_builds_a_verifying_store() {
        let hash = bcrypt::hash("s3cret", 4).unwrap();
        let config = load_file(&format!(
            "gateway.security.authentication: password\n\
             gateway.security.users: alice:pw,bob:bcrypt:{hash}\n"
        ))
        .unwrap();
        assert_eq!(config.security.authentication, AuthenticationMode::Password);

        let authenticator = config.security.build_authenticator().unwrap();
        let alice = authenticator
            .authenticate(ClientCredential::Password {
                username: "alice".into(),
                secret: Secret::new("pw"),
            })
            .await
            .unwrap();
        assert_eq!(alice.name, "alice");
        let bob = authenticator
            .authenticate(ClientCredential::Password {
                username: "bob".into(),
                secret: Secret::new("s3cret"),
            })
            .await
            .unwrap();
        assert_eq!(bob.name, "bob");
        // A verifying store never accepts a bare trust claim.
        assert!(
            authenticator
                .authenticate(ClientCredential::Trust {
                    username: "alice".into()
                })
                .await
                .is_err()
        );
    }

    #[test]
    fn security_password_mode_requires_a_non_empty_user_table() {
        for contents in [
            "gateway.security.authentication: password\n",
            "gateway.security.authentication: password\ngateway.security.users: \"\"\n",
            "gateway.security.authentication: password\ngateway.security.users: \",\"\n",
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(
                problems(error)
                    .iter()
                    .any(|p| p.contains("gateway.security.users")),
                "accepted: {contents}"
            );
        }
    }

    #[test]
    fn security_malformed_user_entry_fails_startup_naming_the_user() {
        let error = load_file(
            "gateway.security.authentication: password\n\
             gateway.security.users: alice:pw,bob:bcrypt:not-a-hash\n",
        )
        .unwrap_err();
        let problems = problems(error);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("gateway.security.users") && p.contains("bob")),
            "{problems:?}"
        );
    }

    #[test]
    fn security_trust_mode_with_users_warns_they_are_ignored() {
        let config = load_file("gateway.security.users: alice:pw\n").unwrap();
        assert!(
            config
                .warnings()
                .iter()
                .any(|w| w.contains("gateway.security.users") && w.contains("ignored")),
            "{:?}",
            config.warnings()
        );
    }

    #[test]
    fn security_unimplemented_authentication_modes_are_rejected() {
        // The FIP also lists trusted-header and oidc; they are future Authenticator plugins and
        // must fail loudly instead of silently degrading to trust.
        for mode in ["trusted-header", "oidc", "basic"] {
            let error =
                load_file(&format!("gateway.security.authentication: {mode}\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{mode}: {error:?}");
        }
    }

    #[test]
    fn write_delivery_and_response_budget_must_fit_request_timeout() {
        let error = load_file(
            "gateway.rest.write.request-timeout: 20s\ngateway.rest.write.max-delivery-time: 20s\n",
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
gateway.rest.listen: 127.0.0.1:18080
gateway.metrics.enabled: true
gateway.cluster.default.bootstrap.servers: from-file:9123
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
gateway.cluster.default.bootstrap.servers: default:9123
gateway.cluster.analytics_eu.bootstrap.servers: file:9123
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
        let error = load_file("gateway.clusters: \"\"\n").unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|problem| problem.contains("at least one cluster"))
        );

        for id in ["Default", "two-clusters", "_hidden"] {
            let error = load_file(&format!(
                "gateway.cluster.{id}.bootstrap.servers: 127.0.0.1:9123\n"
            ))
            .unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{id}: {error:?}");
        }
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
        let error = load_file("gateway.rest.lookup.max-keys: [1\n").unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("line"), "got: {error}");
    }

    #[test]
    fn duplicate_flat_key_rejected() {
        let error = load_file("gateway.rest.lookup.max-keys: 1\ngateway.rest.lookup.max-keys: 2\n")
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
        let file = write_temp_config("gateway.metadata.cache-ttl: 0s\n");
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("cache_ttl"), "got: {error}");
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
    fn invalid_byte_size_rejected() {
        for bad in ["\"4Mb\"", "\"MiB\"", "-1", "\"1.5MiB\""] {
            let error =
                load_file(&format!("gateway.rest.lookup.max-key-bytes: {bad}\n")).unwrap_err();
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
            ("cache_ttl", "gateway.metadata.cache-ttl: 0s\n"),
            ("drain_timeout", "gateway.shutdown.drain-timeout: 0ms\n"),
            (
                "max_body_bytes",
                "gateway.rest.write.max-request-bytes: 0\n",
            ),
            ("max_key_bytes", "gateway.rest.lookup.max-key-bytes: 0MiB\n"),
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{key}: {error:?}");
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn zero_counts_rejected_by_validation() {
        let error = load_file(
            "gateway.metadata.default-page-size: 0\ngateway.metadata.max-page-size: 0\n\
             gateway.metadata.cache-max-entries: 0\ngateway.rest.lookup.max-keys: 0\n\
             gateway.rest.prefix-lookup.max-prefixes: 0\ngateway.rest.prefix-lookup.max-rows-per-prefix: 0\n\
             gateway.rest.lookup.queue-size: 0\ngateway.rest.lookup.max-concurrent: 0\n\
             gateway.rest.write.max-rows: 0\n",
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
            "gateway.scan.max-open-global: 8\n",
            "gateway.scan.cursor-ttl: 1m\n",
            // Rate limiting, dropped by directive.
            "gateway.rest.write.max-concurrent-requests: 64\n",
            "gateway.rest.write.rate-limit.enabled: true\n",
            // Transport security, out of scope (TLS terminates at a fronting proxy).
            "gateway.tls.cert: /etc/tls.pem\n",
        ] {
            assert!(load_file(contents).is_err(), "accepted: {contents}");
        }
    }

    #[test]
    fn empty_bootstrap_rejected() {
        let error = load_file("gateway.cluster.default.bootstrap.servers: []\n").unwrap_err();
        assert!(matches!(error, ConfigError::Invalid(_)), "got: {error:?}");
    }

    #[test]
    fn contradictory_page_sizes_rejected() {
        let error = load_file(
            "gateway.metadata.default-page-size: 100\ngateway.metadata.max-page-size: 10\n",
        )
        .unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|p| p.contains("metadata.default_page_size must not exceed"))
        );

        let error = load_file(&format!(
            "gateway.metadata.max-page-size: {}\n",
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
            "gateway.health.probe-interval: 5s\ngateway.health.probe-timeout: 10s\n\
             gateway.health.reconnect-initial-backoff: 1m\ngateway.health.reconnect-max-backoff: 10s\n",
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
        // Trust mode: the warning calls out the unauthenticated exposure.
        assert!(
            config.warnings()[0].contains("accepts unauthenticated requests"),
            "{:?}",
            config.warnings()
        );

        // Password mode: authentication is enforced, so the warning is about missing TLS only.
        let config = load_file(
            "gateway.rest.listen: 0.0.0.0:8080\n\
             gateway.security.authentication: password\ngateway.security.users: alice:pw\n",
        )
        .unwrap();
        assert_eq!(config.warnings().len(), 1);
        assert!(
            !config.warnings()[0].contains("unauthenticated"),
            "{:?}",
            config.warnings()
        );
        assert!(
            config.warnings()[0].contains("no TLS"),
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
        assert_eq!(env_suffix_to_path("LOOKUP__MAX_KEYS"), "lookup.max_keys");
        assert_eq!(
            env_suffix_to_path("METADATA__MAX_PAGE_SIZE"),
            "metadata.max_page_size"
        );
    }
}
