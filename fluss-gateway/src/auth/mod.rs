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

//! Auth: Authenticator / Credential / Principal.
//!
//! Protocol-agnostic authentication. Protocol layers run their own wire
//! handshake, translate the result into a neutral [`Credential`], then call
//! [`Authenticator::authenticate`]; this module never sees pgwire / HTTP types.
//! The gateway configures exactly one global `Authenticator`, shared by both the
//! PostgreSQL and REST frontends.
//!
//! Contracts encoded here:
//! - **principal == username, 1:1**: a [`Principal`] is exactly the claimed
//!   username — no aliases, groups, roles, or mapping table.
//! - **authentication only, no authorization**: a successful `authenticate`
//!   grants access; there is no permission model.
//! - the resulting `Principal` flows downstream unchanged (PG → session, REST →
//!   request context) and is preserved all the way to
//!   `FlussConnectionProvider::resolve(cluster, principal)`, which keeps
//!   but does not consume it (shared proxy account, no doAs).
//!
//! Design: `design/infra.md`.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::GatewayError;
// Reuse the canonical Principal; do not redefine it here.
pub use crate::types::Principal;

pub mod config;

/// A secret value (e.g. a cleartext password) carried inside a [`Credential`].
///
/// Wraps a `String` with a hand-written `Debug` that never prints the contents,
/// so a secret cannot leak through `{:?}` logging of a `Credential`. Intentionally
/// minimal: no new dependency, no `Display`, no `Clone`-friendly accessors beyond
/// what the password store needs.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying secret. Callers (a password store) must not log it.
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

/// A neutral credential handed to the [`Authenticator`] by a protocol layer.
///
/// Protocol adapters are responsible for turning wire-specific material (PG
/// cleartext-password exchange, HTTP `Authorization: Basic`, …) into one of these
/// variants; the auth layer never inspects wire formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// A trusted identity claim with no secret to verify. Used by the
    /// trust path where the username is taken at face value.
    Trust { username: String },
    /// A username + secret to be verified by a credential store.
    Password { username: String, secret: Secret },
    // future: Token { .. }, Certificate { .. }, etc.
}

impl Credential {
    /// The claimed username for either variant, if non-empty.
    ///
    /// An empty username is treated as *no identity* (returns `None`), so the
    /// trust path can reject anonymous connections.
    pub fn username(&self) -> Option<&str> {
        let name = match self {
            Credential::Trust { username } => username,
            Credential::Password { username, .. } => username,
        };
        if name.is_empty() {
            None
        } else {
            Some(name.as_str())
        }
    }
}

/// Neutral auth-layer error. Lives in `auth/` and carries no protocol codes; it
/// is mapped to a domain [`GatewayError`] at the boundary (see the `From` impl).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The caller presented no usable identity (e.g. anonymous connection).
    Unauthenticated(String),
    /// The presented credential was rejected (bad password / unknown user).
    InvalidCredential(String),
    /// The caller is known but not permitted. Not produced today (no
    /// authorization), but kept so the trait surface is stable when a permission
    /// model lands later.
    Unauthorized(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::Unauthenticated(m) => write!(f, "unauthenticated: {m}"),
            AuthError::InvalidCredential(m) => write!(f, "invalid credential: {m}"),
            AuthError::Unauthorized(m) => write!(f, "unauthorized: {m}"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Boundary mapping: auth-layer error → gateway domain error. Both
/// `Unauthenticated` and `InvalidCredential` collapse to
/// [`GatewayError::Unauthenticated`] (the caller failed to prove identity);
/// `Unauthorized` maps to [`GatewayError::Unauthorized`].
impl From<AuthError> for GatewayError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Unauthenticated(m) => GatewayError::Unauthenticated(m),
            AuthError::InvalidCredential(m) => GatewayError::Unauthenticated(m),
            AuthError::Unauthorized(m) => GatewayError::Unauthorized(m),
        }
    }
}

/// Protocol-agnostic authentication seam.
///
/// One global instance is shared by all frontends. Implementations see only the
/// neutral [`Credential`] / [`Principal`] / [`AuthError`] models — never wire
/// types — so swapping [`TrustAuthenticator`] for [`ConfigUserStoreAuthenticator`]
/// requires no change outside the protocol handshake.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthError>;
}

// ---------------------------------------------------------------------------
// neutral wire-to-credential helper
// ---------------------------------------------------------------------------

/// Build a neutral [`Credential`] from a username and an optional password,
/// without depending on any protocol type. This is the contract protocol layers
/// follow when adapting their handshake result:
/// - `Some(password)` → [`Credential::Password`] (a store *may* verify it);
/// - `None` → [`Credential::Trust`] (no secret to verify).
///
/// Actual extraction of `(username, password)` from PG cleartext exchange or an
/// HTTP `Authorization: Basic` header lives in the protocol layers, NOT
/// here — `auth/` must not depend on pgwire / HTTP types.
pub fn credential_from_userpass(
    username: impl Into<String>,
    password: Option<impl Into<String>>,
) -> Credential {
    let username = username.into();
    match password {
        Some(p) => Credential::Password {
            username,
            secret: Secret::new(p),
        },
        None => Credential::Trust { username },
    }
}

// ---------------------------------------------------------------------------
// TrustAuthenticator (default when no users are configured)
// ---------------------------------------------------------------------------

/// Default authenticator when no users are configured: accepts any credential and
/// trusts the claimed username, without verifying any secret. An empty / missing username is
/// rejected with [`AuthError::Unauthenticated`] so every connection / request
/// carries a non-empty identity.
#[derive(Debug, Default, Clone)]
pub struct TrustAuthenticator;

impl TrustAuthenticator {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Authenticator for TrustAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthError> {
        match credential.username() {
            Some(name) => Ok(Principal {
                name: name.to_string(),
            }),
            None => Err(AuthError::Unauthenticated(
                "no username supplied".to_string(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// ConfigUserStoreAuthenticator
// ---------------------------------------------------------------------------

/// A stored secret from config: either a plaintext password, or the hex form of
/// `sha256(<cleartext>)` prefixed with `sha256:`.
#[derive(Clone, PartialEq, Eq)]
pub enum StoredSecret {
    Plain(Secret),
    Sha256([u8; 32]),
}

impl fmt::Debug for StoredSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoredSecret::Plain(_) => f.write_str("StoredSecret::Plain(***)"),
            StoredSecret::Sha256(_) => f.write_str("StoredSecret::Sha256(***)"),
        }
    }
}

/// Parse one stored secret from config.
///
/// - `sha256:<64 hex chars>` -> a sha256 digest of the user's cleartext password
/// - anything else -> a plaintext password
pub fn parse_stored_secret(raw: &str) -> Result<StoredSecret, AuthError> {
    let Some((prefix, rest)) = raw.split_once(':') else {
        return Ok(StoredSecret::Plain(Secret::new(raw)));
    };
    if !prefix.eq_ignore_ascii_case("sha256") {
        return Ok(StoredSecret::Plain(Secret::new(raw)));
    }
    let bytes = hex::decode(rest.trim()).map_err(|e| {
        AuthError::InvalidCredential(format!("invalid sha256 hex in configured user secret: {e}"))
    })?;
    if bytes.len() != 32 {
        return Err(AuthError::InvalidCredential(format!(
            "invalid sha256 hex in configured user secret: expected 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&bytes);
    Ok(StoredSecret::Sha256(digest))
}

/// Config-backed credential store shared by PG + REST.
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

    /// Build from `username -> stored-secret-string` pairs loaded from config.
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, AuthError> {
        let mut users = HashMap::new();
        for (username, raw_secret) in pairs {
            users.insert(username, parse_stored_secret(&raw_secret)?);
        }
        Ok(Self { users })
    }
}

fn verify(stored: &StoredSecret, candidate: &Secret) -> bool {
    match stored {
        StoredSecret::Plain(expected) => expected
            .expose()
            .as_bytes()
            .ct_eq(candidate.expose().as_bytes())
            .into(),
        StoredSecret::Sha256(expected) => {
            let actual = Sha256::digest(candidate.expose().as_bytes());
            expected.ct_eq(actual.as_slice()).into()
        }
    }
}

#[async_trait]
impl Authenticator for ConfigUserStoreAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthError> {
        match credential {
            Credential::Password { username, secret } => match self.users.get(&username) {
                Some(expected) if verify(expected, &secret) => Ok(Principal { name: username }),
                // Deliberately blur unknown-user vs bad-password to avoid user enumeration.
                _ => Err(AuthError::InvalidCredential(
                    "username or password is incorrect".to_string(),
                )),
            },
            // A verifying store requires a secret to check; a bare trust claim is
            // not acceptable here.
            Credential::Trust { .. } => Err(AuthError::Unauthenticated(
                "password credential required".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trust_accepts_trust_credential_with_username() {
        let auth = TrustAuthenticator::new();
        let p = auth
            .authenticate(Credential::Trust {
                username: "alice".into(),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "alice");
    }

    #[tokio::test]
    async fn trust_accepts_password_credential_without_verifying_secret() {
        let auth = TrustAuthenticator::new();
        // Trust ignores the secret entirely; principal == claimed username.
        let p = auth
            .authenticate(Credential::Password {
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
            .authenticate(Credential::Trust { username: "".into() })
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::Unauthenticated(_)));
    }

    #[test]
    fn credential_username_treats_empty_as_none() {
        assert_eq!(
            Credential::Trust {
                username: "x".into()
            }
            .username(),
            Some("x")
        );
        assert_eq!(
            Credential::Trust { username: "".into() }.username(),
            None
        );
    }

    #[test]
    fn helper_maps_password_presence_to_variant() {
        // Some(password) -> Password; None -> Trust. No protocol types involved.
        let with = credential_from_userpass("u", Some("pw"));
        assert!(matches!(with, Credential::Password { .. }));
        let without = credential_from_userpass("u", None::<String>);
        assert!(matches!(without, Credential::Trust { .. }));
    }

    #[test]
    fn secret_debug_does_not_leak() {
        let s = Secret::new("hunter2");
        let rendered = format!("{s:?}");
        assert!(!rendered.contains("hunter2"));
        assert_eq!(s.expose(), "hunter2");
        // And it must not leak through a Credential's derived Debug either.
        let cred = Credential::Password {
            username: "u".into(),
            secret: Secret::new("hunter2"),
        };
        assert!(!format!("{cred:?}").contains("hunter2"));
    }

    #[test]
    fn auth_error_maps_to_domain_error() {
        // Both auth-side identity failures collapse to domain Unauthenticated.
        assert!(matches!(
            GatewayError::from(AuthError::Unauthenticated("x".into())),
            GatewayError::Unauthenticated(_)
        ));
        assert!(matches!(
            GatewayError::from(AuthError::InvalidCredential("x".into())),
            GatewayError::Unauthenticated(_)
        ));
        assert!(matches!(
            GatewayError::from(AuthError::Unauthorized("x".into())),
            GatewayError::Unauthorized(_)
        ));
    }

    #[test]
    fn parse_stored_secret_supports_plain_and_sha256_and_masks_debug() {
        let plain = parse_stored_secret("pw").unwrap();
        assert!(matches!(plain, StoredSecret::Plain(_)));

        let hash = hex::encode(Sha256::digest(b"secret456"));
        let stored = parse_stored_secret(&format!("sha256:{hash}")).unwrap();
        assert!(matches!(stored, StoredSecret::Sha256(_)));
        assert!(!format!("{stored:?}").contains(&hash));

        let err = parse_stored_secret("sha256:not-hex").unwrap_err();
        assert!(matches!(err, AuthError::InvalidCredential(_)));
    }

    #[tokio::test]
    async fn config_store_accepts_plaintext_and_sha256_and_rejects_bad_logins() {
        let auth = ConfigUserStoreAuthenticator::from_pairs([
            ("carol".to_string(), "pw".to_string()),
            (
                "bob".to_string(),
                format!("sha256:{}", hex::encode(Sha256::digest(b"secret456"))),
            ),
        ])
        .unwrap();

        // happy path: matching plaintext password
        let p = auth
            .authenticate(Credential::Password {
                username: "carol".into(),
                secret: Secret::new("pw"),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "carol");

        // happy path: configured sha256, client still sends cleartext
        let p = auth
            .authenticate(Credential::Password {
                username: "bob".into(),
                secret: Secret::new("secret456"),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "bob");

        // wrong password and unknown user are intentionally blurred.
        assert!(matches!(
            auth.authenticate(Credential::Password {
                username: "carol".into(),
                secret: Secret::new("nope"),
            })
            .await,
            Err(AuthError::InvalidCredential(_))
        ));
        assert!(matches!(
            auth.authenticate(Credential::Password {
                username: "dave".into(),
                secret: Secret::new("pw"),
            })
            .await,
            Err(AuthError::InvalidCredential(_))
        ));

        // A trust claim has no secret to verify -> rejected by a store.
        assert!(matches!(
            auth.authenticate(Credential::Trust {
                username: "carol".into()
            })
            .await,
            Err(AuthError::Unauthenticated(_))
        ));
    }

    /// Type-level proof that a `Principal` produced by auth flows unchanged into
    /// the connection-resolution signature `resolve(cluster, &Principal)`,
    /// i.e. the principal is not lost between authentication and connection
    /// resolution. We don't need a live provider — just that the principal binds
    /// to that parameter position. (Kept but not consumed.)
    #[tokio::test]
    async fn principal_threads_to_connection_resolution_signature() {
        async fn resolve_like(_cluster: &crate::types::ClusterId, _principal: &Principal) {}

        let auth = TrustAuthenticator::new();
        let principal = auth
            .authenticate(Credential::Trust {
                username: "alice".into(),
            })
            .await
            .unwrap();
        resolve_like(&crate::types::ClusterId("default".into()), &principal).await;
        assert_eq!(principal.name, "alice");
    }
}
