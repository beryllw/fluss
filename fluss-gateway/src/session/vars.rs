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

//! P2.3 / P2.4 — apply a [`SessionMutation`] to [`SessionVars`] and classify its
//! runtime effect.
//!
//! Processing order is fixed (design §P2.4): ① update `SessionVars` first, then
//! ② compute the [`SessionMutationEffect`]. `SessionVars` is the single source of
//! truth; this function never mutates runtime context, only the vars. Applying
//! the same mutation twice is idempotent: the resulting vars and effect are
//! identical.

use crate::types::{SessionMutation, SessionMutationEffect, SessionVars};

/// Apply `mutation` to `vars` in place and return how it affects a live
/// `SessionContext`.
///
/// Idempotent: applying the same mutation again yields the same `vars` state and
/// the same [`SessionMutationEffect`]. Only `SessionVars` is touched here; the
/// caller is responsible for acting on the returned effect (design §P2.4).
pub fn apply_session_mutation(
    vars: &mut SessionVars,
    mutation: &SessionMutation,
) -> SessionMutationEffect {
    match mutation {
        // statement_timeout is consulted per-query; no live context impact.
        SessionMutation::SetStatementTimeout(value) => {
            vars.statement_timeout = *value;
            SessionMutationEffect::SessionOnly
        }
        // timezone can be pushed into an existing context without a rebuild.
        SessionMutation::SetTimezone(value) => {
            vars.timezone = value.clone();
            SessionMutationEffect::ApplyToExistingContext
        }
        // catalog / schema selection changes name resolution -> rebuild.
        SessionMutation::SetCurrentCatalog(value) => {
            vars.current_catalog = value.clone();
            SessionMutationEffect::RebuildContextBeforeNextQuery
        }
        SessionMutation::SetCurrentSchema(value) => {
            vars.current_schema = value.clone();
            SessionMutationEffect::RebuildContextBeforeNextQuery
        }
        SessionMutation::SetEnvironmentVar { key, value } => {
            vars.environment.insert(key.clone(), value.clone());
            classify_environment_key(key)
        }
        SessionMutation::UnsetEnvironmentVar { key } => {
            vars.environment.remove(key);
            classify_environment_key(key)
        }
    }
}

/// Classify the runtime effect of a namespaced environment variable (design
/// §P2.4). Only resolution-affecting keys force a rebuild; display-oriented keys
/// stay session-local.
fn classify_environment_key(key: &str) -> SessionMutationEffect {
    match key {
        // search_path affects unqualified name resolution -> rebuild.
        "pg.search_path" => SessionMutationEffect::RebuildContextBeforeNextQuery,
        // Display-oriented PG GUCs: tracked in vars, surfaced by the adapter, but
        // they do not change query planning, so no context work is needed.
        "pg.application_name" | "pg.datestyle" | "pg.bytea_output" => {
            SessionMutationEffect::SessionOnly
        }
        // Conservative default for not-yet-classified protocol vars: keep them in
        // vars only. A var that needs rebuild must be added explicitly above.
        _ => SessionMutationEffect::SessionOnly,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SessionVarValue;
    use std::time::Duration;

    // §P2.4 — SessionOnly: statement_timeout.
    #[test]
    fn statement_timeout_is_session_only() {
        let mut vars = SessionVars::default();
        let effect = apply_session_mutation(
            &mut vars,
            &SessionMutation::SetStatementTimeout(Some(Duration::from_secs(5))),
        );
        assert_eq!(effect, SessionMutationEffect::SessionOnly);
        assert_eq!(vars.statement_timeout, Some(Duration::from_secs(5)));
    }

    // §P2.4 — ApplyToExistingContext: timezone.
    #[test]
    fn timezone_applies_to_existing_context() {
        let mut vars = SessionVars::default();
        let effect = apply_session_mutation(
            &mut vars,
            &SessionMutation::SetTimezone(Some("UTC".into())),
        );
        assert_eq!(effect, SessionMutationEffect::ApplyToExistingContext);
        assert_eq!(vars.timezone.as_deref(), Some("UTC"));
    }

    // §P2.4 — RebuildContextBeforeNextQuery: current_catalog / current_schema.
    #[test]
    fn catalog_and_schema_force_rebuild() {
        let mut vars = SessionVars::default();
        assert_eq!(
            apply_session_mutation(
                &mut vars,
                &SessionMutation::SetCurrentCatalog(Some("fluss".into()))
            ),
            SessionMutationEffect::RebuildContextBeforeNextQuery
        );
        assert_eq!(vars.current_catalog.as_deref(), Some("fluss"));

        assert_eq!(
            apply_session_mutation(
                &mut vars,
                &SessionMutation::SetCurrentSchema(Some("public".into()))
            ),
            SessionMutationEffect::RebuildContextBeforeNextQuery
        );
        assert_eq!(vars.current_schema.as_deref(), Some("public"));
    }

    // §P2.4 — pg.search_path forces rebuild; display GUCs do not.
    #[test]
    fn environment_var_classification() {
        let mut vars = SessionVars::default();

        let rebuild = apply_session_mutation(
            &mut vars,
            &SessionMutation::SetEnvironmentVar {
                key: "pg.search_path".into(),
                value: SessionVarValue::String("a,b".into()),
            },
        );
        assert_eq!(rebuild, SessionMutationEffect::RebuildContextBeforeNextQuery);

        for key in ["pg.application_name", "pg.datestyle", "pg.bytea_output"] {
            let effect = apply_session_mutation(
                &mut vars,
                &SessionMutation::SetEnvironmentVar {
                    key: key.into(),
                    value: SessionVarValue::String("x".into()),
                },
            );
            assert_eq!(
                effect,
                SessionMutationEffect::SessionOnly,
                "display GUC {key} must be SessionOnly"
            );
        }
    }

    // Unsetting search_path also forces a rebuild (same classification).
    #[test]
    fn unset_search_path_forces_rebuild() {
        let mut vars = SessionVars::default();
        vars.environment.insert(
            "pg.search_path".into(),
            SessionVarValue::String("a".into()),
        );
        let effect = apply_session_mutation(
            &mut vars,
            &SessionMutation::UnsetEnvironmentVar {
                key: "pg.search_path".into(),
            },
        );
        assert_eq!(effect, SessionMutationEffect::RebuildContextBeforeNextQuery);
        assert!(!vars.environment.contains_key("pg.search_path"));
    }

    // §P2.4 — apply_session_mutation must be idempotent.
    #[test]
    fn applying_same_mutation_twice_is_idempotent() {
        let mut vars_a = SessionVars::default();
        let mutation = SessionMutation::SetEnvironmentVar {
            key: "pg.search_path".into(),
            value: SessionVarValue::String("a,b".into()),
        };
        let e1 = apply_session_mutation(&mut vars_a, &mutation);
        let snapshot = vars_a.environment.clone();
        let e2 = apply_session_mutation(&mut vars_a, &mutation);

        assert_eq!(e1, e2);
        assert_eq!(vars_a.environment, snapshot);

        // Idempotent for typed fields too.
        let mut vars_b = SessionVars::default();
        let tz = SessionMutation::SetTimezone(Some("UTC".into()));
        let f1 = apply_session_mutation(&mut vars_b, &tz);
        let tz_after = vars_b.timezone.clone();
        let f2 = apply_session_mutation(&mut vars_b, &tz);
        assert_eq!(f1, f2);
        assert_eq!(vars_b.timezone, tz_after);
    }
}
