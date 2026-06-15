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

//! P7 — Auth: Authenticator / Credential / Principal.
//!
//! Protocol-agnostic authentication. Protocol layers run their own wire
//! handshake, translate the result into a neutral [`Credential`], then call
//! [`Authenticator::authenticate`]; this module never sees pgwire / HTTP types.
//! The gateway configures exactly one global `Authenticator`, shared by both the
//! PostgreSQL and REST frontends.
//!
//! Phase 1 contracts encoded here:
//! - **principal == username, 1:1**: a [`Principal`] is exactly the claimed
//!   username — no aliases, groups, roles, or mapping table.
//! - **authentication only, no authorization**: a successful `authenticate`
//!   grants access; there is no permission model in this phase.
//! - the resulting `Principal` flows downstream unchanged (PG → session, REST →
//!   request context) and is preserved all the way to
//!   `FlussConnectionProvider::resolve(cluster, principal)`, where Phase 1 keeps
//!   but does not consume it (shared proxy account, no doAs).
//!
//! Design: `design/infra.md` §P7.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;

use crate::error::GatewayError;
// Reuse the canonical Principal (types.rs §P1.2); do not redefine it here.
pub use crate::types::Principal;

/// A secret value (e.g. a cleartext password) carried inside a [`Credential`].
///
/// Wraps a `String` with a hand-written `Debug` that never prints the contents,
/// so a secret cannot leak through `{:?}` logging of a `Credential`. Intentionally
/// minimal: no new dependency, no `Display`, no `Clone`-friendly accessors beyond
/// what the (future) password store needs.
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
    /// A trusted identity claim with no secret to verify. Used by the Phase 1
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
    /// The caller is known but not permitted. Not produced in Phase 1 (no
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
// neutral wire-to-credential helper (§P7.1)
// ---------------------------------------------------------------------------

/// Build a neutral [`Credential`] from a username and an optional password,
/// without depending on any protocol type. This is the contract protocol layers
/// follow when adapting their handshake result:
/// - `Some(password)` → [`Credential::Password`] (a store *may* verify it);
/// - `None` → [`Credential::Trust`] (no secret to verify).
///
/// Actual extraction of `(username, password)` from PG cleartext exchange or an
/// HTTP `Authorization: Basic` header lives in the P4 / P5 protocol layers, NOT
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
// TrustAuthenticator (Phase 1 default) (§P7.2)
// ---------------------------------------------------------------------------

/// Phase 1 default authenticator: accepts any credential and trusts the claimed
/// username, without verifying any secret. An empty / missing username is
/// rejected with [`AuthError::Unauthenticated`] so every connection / request
/// carries a non-empty identity (keeping the principal chain non-empty per §P7.2).
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
// ConfigUserStoreAuthenticator (shape reserved only) (§P7.3)
// ---------------------------------------------------------------------------

/// Reserved shape for a config-backed credential store, kept to prove the
/// [`Authenticator`] trait is replaceable without touching the protocol layers.
///
/// Phase 1 only fixes the *shape*: a `username -> secret` table loaded from
/// config, with `authenticate` verifying a [`Credential::Password`]. The
/// cleartext-comparison body below is a minimal happy-path skeleton, NOT a
/// production credential store — hashing, timing-safe comparison, and a real
/// config loader are deferred to the phase that actually enables it.
#[derive(Debug, Default, Clone)]
pub struct ConfigUserStoreAuthenticator {
    users: HashMap<String, Secret>,
}

impl ConfigUserStoreAuthenticator {
    /// Build from an in-memory `username -> secret` table. A real implementation
    /// would load this from gateway config; that loader is out of Phase 1 scope.
    pub fn new(users: HashMap<String, Secret>) -> Self {
        Self { users }
    }
}

#[async_trait]
impl Authenticator for ConfigUserStoreAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthError> {
        match credential {
            Credential::Password { username, secret } => match self.users.get(&username) {
                Some(expected) if *expected == secret => Ok(Principal { name: username }),
                Some(_) => Err(AuthError::InvalidCredential(format!(
                    "bad password for user {username}"
                ))),
                None => Err(AuthError::InvalidCredential(format!(
                    "unknown user {username}"
                ))),
            },
            // A store requires a secret to verify; a bare trust claim is not
            // acceptable here.
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

    #[tokio::test]
    async fn config_store_skeleton_happy_and_reject_paths() {
        let mut users = HashMap::new();
        users.insert("carol".to_string(), Secret::new("pw"));
        let auth = ConfigUserStoreAuthenticator::new(users);

        // happy path: matching password
        let p = auth
            .authenticate(Credential::Password {
                username: "carol".into(),
                secret: Secret::new("pw"),
            })
            .await
            .unwrap();
        assert_eq!(p.name, "carol");

        // wrong password
        assert!(matches!(
            auth.authenticate(Credential::Password {
                username: "carol".into(),
                secret: Secret::new("nope"),
            })
            .await,
            Err(AuthError::InvalidCredential(_))
        ));

        // unknown user
        assert!(matches!(
            auth.authenticate(Credential::Password {
                username: "dave".into(),
                secret: Secret::new("pw"),
            })
            .await,
            Err(AuthError::InvalidCredential(_))
        ));

        // a trust claim has no secret to verify -> rejected by a store
        assert!(matches!(
            auth.authenticate(Credential::Trust {
                username: "carol".into()
            })
            .await,
            Err(AuthError::Unauthenticated(_))
        ));
    }

    /// Type-level proof that a `Principal` produced by auth flows unchanged into
    /// the connection-resolution signature `resolve(cluster, &Principal)` (P6 §5),
    /// i.e. the principal is not lost between authentication and connection
    /// resolution. We don't need a live provider — just that the principal binds
    /// to that parameter position. (Phase 1 keeps but does not consume it.)
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
