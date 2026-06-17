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

//! Auth config parsing: YAML file + `GATEWAY_USERS` env override.
//!
//! Kept pure (string in -> user pairs out) so it is easy to unit-test; file I/O
//! stays in `main.rs`.

use serde::Deserialize;

use crate::auth::AuthError;

#[derive(Debug, Deserialize)]
pub struct GatewayFileConfig {
    pub auth: Option<AuthFileConfig>,
}

#[derive(Debug, Deserialize)]
pub struct AuthFileConfig {
    #[serde(default)]
    pub users: Vec<UserEntry>,
}

#[derive(Debug, Deserialize)]
pub struct UserEntry {
    pub username: String,
    pub password: String,
}

pub fn parse_yaml(text: &str) -> Result<Vec<(String, String)>, AuthError> {
    let cfg: GatewayFileConfig = serde_yaml::from_str(text)
        .map_err(|e| AuthError::InvalidCredential(format!("invalid auth YAML: {e}")))?;
    Ok(cfg
        .auth
        .map(|a| a.users)
        .unwrap_or_default()
        .into_iter()
        .map(|u| (u.username, u.password))
        .collect())
}

/// Parse `GATEWAY_USERS="alice:pw,bob:sha256:abcd..."`.
///
/// Splits records on commas, then splits each record on the first `:` only, so a
/// secret may itself contain `:` (e.g. `sha256:<hex>`).
pub fn parse_users_env(text: &str) -> Vec<(String, String)> {
    text.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let (username, secret) = entry.split_once(':')?;
            let username = username.trim();
            if username.is_empty() {
                return None;
            }
            Some((username.to_string(), secret.trim().to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yaml_users() {
        let pairs = parse_yaml(
            r#"
auth:
  users:
    - username: alice
      password: secret123
    - username: bob
      password: "sha256:abc123"
"#,
        )
        .unwrap();
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "secret123".to_string()),
                ("bob".to_string(), "sha256:abc123".to_string()),
            ]
        );
    }

    #[test]
    fn parses_empty_yaml_to_no_users() {
        assert!(parse_yaml("{}").unwrap().is_empty());
    }

    #[test]
    fn parses_users_env_and_keeps_secret_colons() {
        let pairs = parse_users_env("alice:secret123,bob:sha256:abc:def, carol : pw ");
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "secret123".to_string()),
                ("bob".to_string(), "sha256:abc:def".to_string()),
                ("carol".to_string(), "pw".to_string()),
            ]
        );
    }
}
