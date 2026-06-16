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

//! P4 — PostgreSQL BI/IDE compatibility classification (`compat`).
//!
//! Pure, side-effect-free classification of an incoming SQL string into a
//! [`StatementClass`]. The wire/encoding work lives in `adapter`; this module
//! only decides *what* the statement is so the handler can route it.
//!
//! Design principle (`design/sql-path.md` §P4.3): **prefer answering from the
//! real `pg_catalog` over rewriting**. The interception list here is therefore
//! deliberately small and explicit — only statements that DataFusion / the real
//! catalog cannot answer (session-local `SET`/`SHOW`, autocommit transaction
//! no-ops, a couple of scalar probes) are intercepted. Everything else is
//! passthrough to `Instance.execute_sql`, and writes are rejected outright.

use std::sync::LazyLock;

use regex::Regex;

// ---------------------------------------------------------------------------
// PostgreSQL client introspection rewrite (psql \dt / \d / \l / \dn, BI tools)
// ---------------------------------------------------------------------------

/// `... COLLATE pg_catalog.default` / `... COLLATE "default"` — DataFusion's
/// planner rejects the `Collate` AST node. The default collation is a no-op for
/// our results, so we strip the whole `COLLATE <default>` span.
static COLLATE_DEFAULT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\s+COLLATE\s+(?:pg_catalog\.default|"default")"#).unwrap()
});

/// Explicit `OPERATOR(pg_catalog.<op>)` syntax (psql `\d` uses
/// `OPERATOR(pg_catalog.~)` for the regex match). DataFusion rejects the custom
/// operator node; it does accept the bare operator (`~`, `!~`, …), so we unwrap
/// `OPERATOR(pg_catalog.<op>)` to `<op>`.
static PG_CATALOG_OPERATOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)OPERATOR\s*\(\s*pg_catalog\.([^)\s]+)\s*\)").unwrap()
});

/// Schema-qualified `pg_catalog.<fn>(` function calls. DataFusion registers the
/// `datafusion-pg-catalog` UDFs under their bare name and cannot resolve a
/// schema-qualified function name (e.g. `pg_catalog.pg_table_is_visible(...)`),
/// so we de-qualify function calls. Table references like `pg_catalog.pg_class`
/// (no following `(`) are left intact — they resolve fine.
static PG_CATALOG_FN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\bpg_catalog\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\("#).unwrap()
});

/// Rewrite PostgreSQL client introspection SQL (psql backslash commands, IDE/BI
/// object browsers) into a form the DataFusion planner accepts, WITHOUT changing
/// result semantics. Applied only on the passthrough path; statements that need
/// no change are returned unchanged. The always-correct path for tools is direct
/// `information_schema` / `pg_catalog` SQL — this only smooths psql `\d*` & friends.
pub fn rewrite_introspection(sql: &str) -> String {
    let s = PG_CATALOG_OPERATOR.replace_all(sql, "${1}");
    let s = COLLATE_DEFAULT.replace_all(&s, "");
    let s = PG_CATALOG_FN.replace_all(&s, "${1}(");
    s.into_owned()
}

/// What kind of statement an incoming SQL string is, for routing purposes.
///
/// Classification is purely lexical (leading keyword + cheap parsing); it does
/// not validate the statement. The handler maps each class to a wire response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementClass {
    /// `SET <var> = <value>` — apply to `SessionVars`, reply with a `SET` tag.
    Set { name: String, value: String },
    /// `SHOW <var>` — read from `SessionVars`, reply with a single-row result.
    Show { name: String },
    /// `BEGIN` / `START TRANSACTION` — Phase 1 autocommit no-op.
    Begin,
    /// `COMMIT` / `END` — Phase 1 autocommit no-op.
    Commit,
    /// `ROLLBACK` / `ABORT` — Phase 1 autocommit no-op.
    Rollback,
    /// `DISCARD ALL` / `DISCARD ...` — session reset (vars + rebuild).
    Discard,
    /// A write / DDL statement — rejected with `Unsupported` (read-only phase).
    Write,
    /// Any other statement (normal `SELECT`, catalog probe) — passthrough to
    /// `Instance.execute_sql`.
    Passthrough,
}

/// The command tag a transaction-control no-op replies with on the wire.
pub fn transaction_command_tag(class: &StatementClass) -> Option<&'static str> {
    match class {
        StatementClass::Begin => Some("BEGIN"),
        StatementClass::Commit => Some("COMMIT"),
        StatementClass::Rollback => Some("ROLLBACK"),
        StatementClass::Discard => Some("DISCARD ALL"),
        _ => None,
    }
}

/// Normalize a statement to its leading uppercase keyword, stripping leading
/// whitespace and a trailing semicolon for matching.
fn first_keyword(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// Classify a single SQL statement. Multi-statement strings are classified by
/// their first statement only (the handler executes them as a unit).
pub fn classify(sql: &str) -> StatementClass {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let kw = first_keyword(sql);

    match kw.as_str() {
        "SET" => parse_set(trimmed),
        "SHOW" => parse_show(trimmed),
        "BEGIN" | "START" => StatementClass::Begin,
        "COMMIT" | "END" => StatementClass::Commit,
        "ROLLBACK" | "ABORT" => StatementClass::Rollback,
        "DISCARD" => StatementClass::Discard,
        // Writes & DDL are rejected up front (Phase 1 read-only, §P4.7).
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "TRUNCATE" | "COPY" | "CREATE" | "ALTER"
        | "DROP" | "GRANT" | "REVOKE" | "REINDEX" | "VACUUM" | "CALL" => StatementClass::Write,
        _ => StatementClass::Passthrough,
    }
}

/// Parse `SET <name> [TO|=] <value>` (also `SET SESSION/LOCAL <name> ...`).
/// Falls back to a no-op-able `Set` with an empty value when the value is
/// missing; the caller still answers with a `SET` tag to keep BI tools happy.
fn parse_set(sql: &str) -> StatementClass {
    let rest = sql["SET".len()..].trim();
    // Strip an optional SESSION / LOCAL scope qualifier.
    let rest = strip_prefix_ci(rest, "SESSION ")
        .or_else(|| strip_prefix_ci(rest, "LOCAL "))
        .unwrap_or(rest)
        .trim();

    // Split on the first '=' or ' TO ' (case-insensitive).
    let (name, value) = if let Some(idx) = rest.find('=') {
        (rest[..idx].trim(), rest[idx + 1..].trim())
    } else if let Some(idx) = find_ci(rest, " TO ") {
        (rest[..idx].trim(), rest[idx + 4..].trim())
    } else {
        (rest, "")
    };

    StatementClass::Set {
        name: name.to_ascii_lowercase(),
        value: unquote(value),
    }
}

/// Parse `SHOW <name>` (and `SHOW ALL`, kept as a `Show` with name `all`).
fn parse_show(sql: &str) -> StatementClass {
    let name = sql["SHOW".len()..].trim().trim_end_matches(';').trim();
    StatementClass::Show {
        name: name.to_ascii_lowercase(),
    }
}

/// Strip a leading prefix case-insensitively, returning the remainder.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Find the byte index of `needle` in `haystack`, case-insensitively (ASCII).
fn find_ci(haystack: &str, needle: &str) -> Option<usize> {
    let h = haystack.to_ascii_uppercase();
    let n = needle.to_ascii_uppercase();
    h.find(&n)
}

/// Remove a single layer of single/double quotes around a value.
fn unquote(v: &str) -> String {
    let v = v.trim();
    let bytes = v.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"'))
    {
        v[1..v.len() - 1].to_string()
    } else {
        v.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_select_as_passthrough() {
        assert_eq!(classify("SELECT 1"), StatementClass::Passthrough);
        assert_eq!(
            classify("  select * from pg_catalog.pg_class  "),
            StatementClass::Passthrough
        );
    }

    #[test]
    fn classifies_writes_as_write() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "update t set x=1",
            "DELETE FROM t",
            "CREATE TABLE t (a int)",
            "drop table t",
            "ALTER TABLE t ADD COLUMN c int",
            "TRUNCATE t",
        ] {
            assert_eq!(classify(sql), StatementClass::Write, "sql: {sql}");
        }
    }

    #[test]
    fn parses_set_with_equals_and_to() {
        assert_eq!(
            classify("SET search_path = 'public'"),
            StatementClass::Set {
                name: "search_path".into(),
                value: "public".into()
            }
        );
        assert_eq!(
            classify("set TimeZone TO 'UTC'"),
            StatementClass::Set {
                name: "timezone".into(),
                value: "UTC".into()
            }
        );
    }

    #[test]
    fn parses_set_with_session_local_scope() {
        assert_eq!(
            classify("SET SESSION application_name = 'psql'"),
            StatementClass::Set {
                name: "application_name".into(),
                value: "psql".into()
            }
        );
        assert_eq!(
            classify("SET LOCAL timezone = 'UTC'"),
            StatementClass::Set {
                name: "timezone".into(),
                value: "UTC".into()
            }
        );
    }

    #[test]
    fn parses_show() {
        assert_eq!(
            classify("SHOW search_path"),
            StatementClass::Show {
                name: "search_path".into()
            }
        );
        assert_eq!(
            classify("show TimeZone;"),
            StatementClass::Show {
                name: "timezone".into()
            }
        );
    }

    #[test]
    fn classifies_transaction_control() {
        assert_eq!(classify("BEGIN"), StatementClass::Begin);
        assert_eq!(classify("START TRANSACTION"), StatementClass::Begin);
        assert_eq!(classify("COMMIT"), StatementClass::Commit);
        assert_eq!(classify("END"), StatementClass::Commit);
        assert_eq!(classify("ROLLBACK"), StatementClass::Rollback);
        assert_eq!(classify("ABORT"), StatementClass::Rollback);
        assert_eq!(classify("DISCARD ALL"), StatementClass::Discard);
    }

    #[test]
    fn transaction_tags_match_class() {
        assert_eq!(transaction_command_tag(&StatementClass::Begin), Some("BEGIN"));
        assert_eq!(
            transaction_command_tag(&StatementClass::Commit),
            Some("COMMIT")
        );
        assert_eq!(
            transaction_command_tag(&StatementClass::Rollback),
            Some("ROLLBACK")
        );
        assert_eq!(transaction_command_tag(&StatementClass::Passthrough), None);
    }

    #[test]
    fn unquote_strips_one_layer() {
        assert_eq!(unquote("'abc'"), "abc");
        assert_eq!(unquote("\"abc\""), "abc");
        assert_eq!(unquote("abc"), "abc");
    }

    #[test]
    fn rewrite_dequalifies_pg_catalog_functions() {
        // psql \dt calls pg_catalog.pg_table_is_visible(c.oid); DataFusion only
        // resolves the bare UDF name.
        assert_eq!(
            rewrite_introspection("AND pg_catalog.pg_table_is_visible(c.oid)"),
            "AND pg_table_is_visible(c.oid)"
        );
        // a space before the paren is tolerated and collapsed.
        assert_eq!(
            rewrite_introspection("pg_catalog.pg_get_userbyid (c.relowner)"),
            "pg_get_userbyid(c.relowner)"
        );
    }

    #[test]
    fn rewrite_keeps_pg_catalog_table_refs() {
        // A schema-qualified TABLE (no following paren) must stay qualified.
        let sql = "SELECT * FROM pg_catalog.pg_class c";
        assert_eq!(rewrite_introspection(sql), sql);
    }

    #[test]
    fn rewrite_strips_collate_default() {
        assert_eq!(
            rewrite_introspection("WHERE x = 'a' COLLATE pg_catalog.default AND y = 1"),
            "WHERE x = 'a' AND y = 1"
        );
        assert_eq!(
            rewrite_introspection("ORDER BY x COLLATE \"default\""),
            "ORDER BY x"
        );
    }

    #[test]
    fn rewrite_unwraps_pg_catalog_operator() {
        // psql \d uses OPERATOR(pg_catalog.~) for regex match; unwrap to bare ~.
        assert_eq!(
            rewrite_introspection("WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$'"),
            "WHERE c.relname ~ '^(t)$'"
        );
        assert_eq!(
            rewrite_introspection("n.nspname OPERATOR(pg_catalog.!~) '^pg_'"),
            "n.nspname !~ '^pg_'"
        );
    }

    #[test]
    fn rewrite_is_noop_for_plain_sql() {
        let sql = "SELECT id, name FROM fluss.fluss.kv WHERE id = 1";
        assert_eq!(rewrite_introspection(sql), sql);
    }
}
