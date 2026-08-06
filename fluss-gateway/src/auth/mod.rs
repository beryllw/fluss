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

//! Authentication: [`Authenticator`] / [`ClientCredential`] / [`Principal`].
//!
//! Protocol-agnostic client-to-gateway authentication, the pluggable first layer of the FIP-49
//! identity design. Protocol layers run their own wire handshake (e.g. HTTP `Authorization: Basic`),
//! translate the result into a neutral [`ClientCredential`], then call
//! [`Authenticator::authenticate`]; this module never sees HTTP types. The gateway configures
//! exactly one global `Authenticator`, shared by every frontend, and new mechanisms (token, OIDC,
//! mTLS) plug in as further implementations without touching the protocol layers.
//!
//! Contracts encoded here:
//! - **principal == username, 1:1**: a [`Principal`] is exactly the claimed username — no aliases,
//!   groups, roles, or mapping table. Its `attributes` carry optional mechanism metadata only.
//! - **authentication only, no authorization**: a successful `authenticate` grants access to the
//!   gateway; per-user authorization is Fluss-side (via act-as identity propagation).
//! - **no credential detail leaks to clients**: every rejection collapses to a single 401 message
//!   at the boundary; whether the user was unknown or the password wrong stays in the log.
//!
//! Ported from the gateway-v0.5.1 prior-art branch, adapted to the FIP contract: FIP naming
//! (`ClientCredential`, `Principal { name, attributes }`) and bcrypt password hashes
//! (`htpasswd -B` compatible) instead of the branch's sha256 scheme.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use async_trait::async_trait;
use subtle::ConstantTimeEq;

use crate::error::GatewayError;

/// The authenticated identity, exactly as defined by the FIP: the gateway principal name is the
/// user id propagated toward Fluss, and `attributes` carries optional mechanism metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub attributes: BTreeMap<String, String>,
}

impl Principal {
    /// A principal with no mechanism attributes, the common case for trust and password logins.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            attributes: BTreeMap::new(),
        }
    }
}

/// A secret value (e.g. a cleartext password) carried inside a [`ClientCredential`].
///
/// Wraps a `String` with a hand-written `Debug` that never prints the contents, so a secret cannot
/// leak through `{:?}` logging of a credential. Intentionally minimal: no `Display`, no
/// `Clone`-friendly accessors beyond what the password store needs.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the underlying secret. Callers (a password store) must not log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Deserializes from a plain string so configuration values (e.g. a cluster service-account
/// secret) can be carried as [`Secret`] and inherit its redacting `Debug`.
impl<'de> serde::Deserialize<'de> for Secret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Secret)
    }
}

/// A neutral credential handed to the [`Authenticator`] by a protocol layer.
///
/// Protocol adapters are responsible for turning wire-specific material (an HTTP
/// `Authorization: Basic` header, …) into one of these variants; the auth layer never inspects
/// wire formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCredential {
    /// A trusted identity claim with no secret to verify. Used by the trust path where the
    /// username is taken at face value.
    Trust { username: String },
    /// A username + secret to be verified by a credential store.
    Password { username: String, secret: Secret },
    // future: Token { .. }, Certificate { .. }, etc.
}

impl ClientCredential {
    /// The claimed username for either variant, if non-empty.
    ///
    /// An empty username is treated as *no identity* (returns `None`), so the trust path can
    /// reject anonymous requests.
    pub fn username(&self) -> Option<&str> {
        let name = match self {
            ClientCredential::Trust { username } => username,
            ClientCredential::Password { username, .. } => username,
        };
        if name.is_empty() { None } else { Some(name) }
    }
}

/// Neutral auth-layer error. Lives in `auth/` and carries no protocol codes; it is mapped to a
/// domain [`GatewayError`] at the boundary (see the `From` impl).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The caller presented no usable identity (e.g. an anonymous request).
    Unauthenticated(String),
    /// The presented credential was rejected (bad password / unknown user, deliberately blurred).
    InvalidCredential(String),
    /// A configured credential entry is malformed; raised while building a store, not per request.
    InvalidUserEntry(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::Unauthenticated(m) => write!(f, "unauthenticated: {m}"),
            AuthError::InvalidCredential(m) => write!(f, "invalid credential: {m}"),
            AuthError::InvalidUserEntry(m) => write!(f, "invalid user entry: {m}"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Boundary mapping: auth-layer error → gateway domain error. Every identity failure collapses to
/// one uniform 401 message so a client cannot distinguish an unknown user from a wrong password;
/// the specific reason stays in the auth layer for logging.
impl From<AuthError> for GatewayError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Unauthenticated(_) | AuthError::InvalidCredential(_) => {
                GatewayError::unauthenticated("authentication failed")
            }
            AuthError::InvalidUserEntry(m) => {
                GatewayError::internal(format!("authentication store misconfigured: {m}"))
            }
        }
    }
}

/// Protocol-agnostic authentication seam.
///
/// One global instance is shared by all frontends. Implementations see only the neutral
/// [`ClientCredential`] / [`Principal`] / [`AuthError`] models — never wire types — so swapping
/// [`TrustAuthenticator`] for [`ConfigUserStoreAuthenticator`] requires no change outside the
/// protocol handshake.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, credential: ClientCredential) -> Result<Principal, AuthError>;
}

/// Builds a neutral [`ClientCredential`] from a username and an optional password, without
/// depending on any protocol type. This is the contract protocol layers follow when adapting
/// their handshake result:
/// - `Some(password)` → [`ClientCredential::Password`] (a store *may* verify it);
/// - `None` → [`ClientCredential::Trust`] (no secret to verify).
pub fn credential_from_userpass(
    username: impl Into<String>,
    password: Option<impl Into<String>>,
) -> ClientCredential {
    let username = username.into();
    match password {
        Some(p) => ClientCredential::Password {
            username,
            secret: Secret::new(p),
        },
        None => ClientCredential::Trust { username },
    }
}

/// Default authenticator when no users are configured: accepts any credential and trusts the
/// claimed username, without verifying any secret. An empty / missing username is rejected with
/// [`AuthError::Unauthenticated`] so every request carries a non-empty identity.
#[derive(Debug, Default, Clone)]
pub struct TrustAuthenticator;

impl TrustAuthenticator {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Authenticator for TrustAuthenticator {
    async fn authenticate(&self, credential: ClientCredential) -> Result<Principal, AuthError> {
        match credential.username() {
            Some(name) => Ok(Principal::new(name)),
            None => Err(AuthError::Unauthenticated(
                "no username supplied".to_string(),
            )),
        }
    }
}

/// A stored secret from configuration: either a plaintext password, or a bcrypt hash prefixed
/// with `bcrypt:` — the hash format produced by `htpasswd -B`, per the FIP
/// (`gateway.security.users: alice:secret,bob:bcrypt:<hash>`).
#[derive(Clone, PartialEq, Eq)]
pub enum StoredSecret {
    Plain(Secret),
    Bcrypt(String),
}

impl fmt::Debug for StoredSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoredSecret::Plain(_) => f.write_str("StoredSecret::Plain(***)"),
            StoredSecret::Bcrypt(_) => f.write_str("StoredSecret::Bcrypt(***)"),
        }
    }
}

/// Parses one stored secret.
///
/// - `bcrypt:<hash>` → a bcrypt hash the user's cleartext password is verified against;
/// - anything else → a plaintext password (which may itself contain `:`).
pub fn parse_stored_secret(raw: &str) -> Result<StoredSecret, AuthError> {
    let Some(hash) = raw.strip_prefix("bcrypt:") else {
        return Ok(StoredSecret::Plain(Secret::new(raw)));
    };
    // Every bcrypt hash is `$2<minor>$<cost>$<salt+digest>`; catching a malformed hash here turns
    // a would-be always-failing login into a startup error that names the problem.
    if !hash.starts_with("$2") || hash.split('$').count() != 4 {
        return Err(AuthError::InvalidUserEntry(
            "malformed bcrypt hash: expected the `$2<minor>$<cost>$<salt+digest>` form produced by `htpasswd -B`"
                .to_string(),
        ));
    }
    Ok(StoredSecret::Bcrypt(hash.to_string()))
}

/// Parses the configured user table: comma-separated `name:secret` entries where the secret is a
/// plaintext password or `bcrypt:<hash>`. Empty entries (e.g. a trailing comma) are ignored;
/// a malformed or duplicate entry is rejected naming the offending user.
pub fn parse_user_table(raw: &str) -> Result<HashMap<String, StoredSecret>, AuthError> {
    let mut users = HashMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((username, secret)) = entry.split_once(':') else {
            return Err(AuthError::InvalidUserEntry(format!(
                "user entry {entry:?} must be `name:secret` or `name:bcrypt:<hash>`"
            )));
        };
        let username = username.trim();
        if username.is_empty() {
            return Err(AuthError::InvalidUserEntry(format!(
                "user entry {entry:?} has an empty username"
            )));
        }
        let stored = parse_stored_secret(secret)
            .map_err(|e| AuthError::InvalidUserEntry(format!("user {username:?}: {e}")))?;
        if users.insert(username.to_string(), stored).is_some() {
            return Err(AuthError::InvalidUserEntry(format!(
                "user {username:?} is declared more than once"
            )));
        }
    }
    Ok(users)
}

/// Configuration-backed credential store.
#[derive(Debug, Default, Clone)]
pub struct ConfigUserStoreAuthenticator {
    users: HashMap<String, StoredSecret>,
}

impl ConfigUserStoreAuthenticator {
    pub fn new(users: HashMap<String, StoredSecret>) -> Self {
        Self { users }
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }
}

/// Verifies a candidate password against a stored secret. Plaintext comparison is constant-time;
/// a malformed bcrypt hash verifies as a rejection rather than an error, so a login can never
/// bypass verification through a parse failure.
fn verify(stored: &StoredSecret, candidate: &Secret) -> bool {
    match stored {
        StoredSecret::Plain(expected) => expected
            .expose()
            .as_bytes()
            .ct_eq(candidate.expose().as_bytes())
            .into(),
        StoredSecret::Bcrypt(hash) => bcrypt::verify(candidate.expose(), hash).unwrap_or(false),
    }
}

#[async_trait]
impl Authenticator for ConfigUserStoreAuthenticator {
    async fn authenticate(&self, credential: ClientCredential) -> Result<Principal, AuthError> {
        match credential {
            ClientCredential::Password { username, secret } => match self.users.get(&username) {
                Some(expected) if verify(expected, &secret) => Ok(Principal::new(username)),
                // Deliberately blur unknown-user vs bad-password to avoid user enumeration.
                _ => Err(AuthError::InvalidCredential(
                    "username or password is incorrect".to_string(),
                )),
            },
            // A verifying store requires a secret to check; a bare trust claim is not acceptable.
            ClientCredential::Trust { .. } => Err(AuthError::Unauthenticated(
                "password credential required".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    /// A low-cost bcrypt hash so the test suite stays fast; production hashes come from
    /// `htpasswd -B` at its default cost.
    fn bcrypt_hash(password: &str) -> String {
        bcrypt::hash(password, 4).expect("bcrypt hash")
    }

    #[tokio::test]
    async fn trust_accepts_trust_credential_with_username() {
        let auth = TrustAuthenticator::new();
        let p = auth
            .authenticate(ClientCredential::Trust {
                username: "alice".into(),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "alice");
        assert!(p.attributes.is_empty());
    }

    #[tokio::test]
    async fn trust_accepts_password_credential_without_verifying_secret() {
        let auth = TrustAuthenticator::new();
        let p = auth
            .authenticate(ClientCredential::Password {
                username: "bob".into(),
                secret: Secret::new("whatever"),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "bob");
    }

    #[tokio::test]
    async fn trust_rejects_empty_username() {
        let auth = TrustAuthenticator::new();
        let err = auth
            .authenticate(ClientCredential::Trust {
                username: "".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::Unauthenticated(_)));
    }

    #[test]
    fn credential_username_treats_empty_as_none() {
        assert_eq!(
            ClientCredential::Trust {
                username: "x".into()
            }
            .username(),
            Some("x")
        );
        assert_eq!(
            ClientCredential::Trust {
                username: "".into()
            }
            .username(),
            None
        );
    }

    #[test]
    fn helper_maps_password_presence_to_variant() {
        let with = credential_from_userpass("u", Some("pw"));
        assert!(matches!(with, ClientCredential::Password { .. }));
        let without = credential_from_userpass("u", None::<String>);
        assert!(matches!(without, ClientCredential::Trust { .. }));
    }

    #[test]
    fn secret_debug_does_not_leak() {
        let s = Secret::new("hunter2");
        assert!(!format!("{s:?}").contains("hunter2"));
        assert_eq!(s.expose(), "hunter2");
        // And it must not leak through a credential's derived Debug either.
        let cred = ClientCredential::Password {
            username: "u".into(),
            secret: Secret::new("hunter2"),
        };
        assert!(!format!("{cred:?}").contains("hunter2"));
    }

    #[test]
    fn identity_failures_collapse_to_one_uniform_401() {
        let unknown = GatewayError::from(AuthError::InvalidCredential("user not found".into()));
        let bad_password = GatewayError::from(AuthError::InvalidCredential("bad password".into()));
        let anonymous = GatewayError::from(AuthError::Unauthenticated("no username".into()));

        for error in [&unknown, &bad_password, &anonymous] {
            assert_eq!(error.kind(), ErrorKind::Unauthenticated);
            assert_eq!(error.kind().http_status(), 401);
            assert!(!error.retryable());
        }
        // The client-visible message is uniform: no user-enumeration or reason detail.
        assert_eq!(unknown.message(), bad_password.message());
        assert_eq!(unknown.message(), anonymous.message());
        assert!(!unknown.message().contains("user not found"));
    }

    #[test]
    fn store_misconfiguration_maps_to_internal_not_401() {
        let error = GatewayError::from(AuthError::InvalidUserEntry("user \"x\": bad hash".into()));
        assert_eq!(error.kind(), ErrorKind::Internal);
    }

    #[test]
    fn parse_stored_secret_supports_plain_and_bcrypt_and_masks_debug() {
        let plain = parse_stored_secret("pw").unwrap();
        assert!(matches!(plain, StoredSecret::Plain(_)));

        // A plaintext password containing a colon is not mistaken for a scheme.
        let colon = parse_stored_secret("pw:with:colons").unwrap();
        assert!(matches!(colon, StoredSecret::Plain(_)));

        let hash = bcrypt_hash("secret456");
        let stored = parse_stored_secret(&format!("bcrypt:{hash}")).unwrap();
        assert!(matches!(stored, StoredSecret::Bcrypt(_)));
        assert!(!format!("{stored:?}").contains(&hash));

        let err = parse_stored_secret("bcrypt:not-a-hash").unwrap_err();
        assert!(matches!(err, AuthError::InvalidUserEntry(_)));
    }

    #[test]
    fn user_table_parses_the_fip_declaration_form() {
        let hash = bcrypt_hash("secret456");
        let users = parse_user_table(&format!("alice:secret, bob:bcrypt:{hash},")).unwrap();
        assert_eq!(users.len(), 2);
        assert!(matches!(users["alice"], StoredSecret::Plain(_)));
        assert!(matches!(users["bob"], StoredSecret::Bcrypt(_)));
    }

    #[test]
    fn user_table_rejects_malformed_and_duplicate_entries() {
        let missing_secret = parse_user_table("alice").unwrap_err();
        assert!(matches!(missing_secret, AuthError::InvalidUserEntry(_)));
        assert!(missing_secret.to_string().contains("alice"));

        let empty_username = parse_user_table(":secret").unwrap_err();
        assert!(matches!(empty_username, AuthError::InvalidUserEntry(_)));

        let bad_hash = parse_user_table("bob:bcrypt:nope").unwrap_err();
        assert!(bad_hash.to_string().contains("bob"), "{bad_hash}");

        let duplicate = parse_user_table("alice:a,alice:b").unwrap_err();
        assert!(duplicate.to_string().contains("alice"), "{duplicate}");
        assert!(duplicate.to_string().contains("more than once"));
    }

    #[tokio::test]
    async fn config_store_accepts_plaintext_and_bcrypt_and_rejects_bad_logins() {
        let auth = ConfigUserStoreAuthenticator::new(
            parse_user_table(&format!("carol:pw,bob:bcrypt:{}", bcrypt_hash("secret456"))).unwrap(),
        );
        assert_eq!(auth.user_count(), 2);

        // Happy path: matching plaintext password.
        let p = auth
            .authenticate(ClientCredential::Password {
                username: "carol".into(),
                secret: Secret::new("pw"),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "carol");

        // Happy path: configured bcrypt hash, client still sends cleartext.
        let p = auth
            .authenticate(ClientCredential::Password {
                username: "bob".into(),
                secret: Secret::new("secret456"),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "bob");

        // Wrong password and unknown user are intentionally blurred.
        assert!(matches!(
            auth.authenticate(ClientCredential::Password {
                username: "carol".into(),
                secret: Secret::new("nope"),
            })
            .await,
            Err(AuthError::InvalidCredential(_))
        ));
        assert!(matches!(
            auth.authenticate(ClientCredential::Password {
                username: "dave".into(),
                secret: Secret::new("pw"),
            })
            .await,
            Err(AuthError::InvalidCredential(_))
        ));

        // A trust claim has no secret to verify → rejected by a store.
        assert!(matches!(
            auth.authenticate(ClientCredential::Trust {
                username: "carol".into()
            })
            .await,
            Err(AuthError::Unauthenticated(_))
        ));
    }
}
