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

//! PostgreSQL BI/IDE compatibility classification (`compat`).
//!
//! Pure, side-effect-free classification of an incoming SQL string into a
//! [`StatementClass`]. The wire/encoding work lives in `adapter`; this module
//! only decides *what* the statement is so the handler can route it.
//!
//! Design principle (`design/sql-path.md`): **prefer answering from the
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

/// PostgreSQL-specific cast chains used by psql introspection queries. DataFusion
/// does not support `regtype`, and schema-qualified cast targets like
/// `::pg_catalog.text` also fail. For metadata display these expressions are only
/// cosmetic (typed-table display), so we degrade them to a plain `::text` cast.
static REGTYPE_TO_TEXT_CAST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)::\s*(?:pg_catalog\.)?regtype\s*::\s*(?:pg_catalog\.)?text\b").unwrap()
});

/// Bare schema-qualified `::pg_catalog.text` cast target.
static PG_CATALOG_TEXT_CAST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)::\s*pg_catalog\.text\b").unwrap()
});

/// PostgreSQL OID-alias type casts (`regclass`, `regproc`, `regtype`, …), which
/// DataFusion does not support. psql introspection uses them only to render an
/// object's name (e.g. `conrelid::regclass`); degrade them to `::text` (the oid
/// rendered as text). Harmless for our results — the rows that would show such a
/// name (foreign keys, indexes, …) are empty for Fluss tables anyway.
static PG_OID_ALIAS_CAST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)::\s*(?:pg_catalog\.)?reg(?:class|collation|config|dictionary|namespace|oper|operator|procedure|proc|role|type)\b",
    )
    .unwrap()
});

/// psql `\d`'s column query carries two correlated scalar subqueries that
/// DataFusion's analyzer rejects ("Correlated scalar subquery must be aggregated
/// to return at most one row"): the column default (from `pg_attrdef`) and the
/// non-default collation (from `pg_collation`). Both are always empty for Fluss
/// tables (no column defaults, no per-column collations), so we replace each with
/// `NULL` — faithful for our tables and lets `\d` plan. Matched against the raw
/// psql text (before function de-qualification), tolerant of whitespace/newlines.
static PG_ATTRDEF_SUBQUERY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)\(\s*SELECT\s+pg_catalog\.pg_get_expr\([^)]*\)\s+FROM\s+pg_catalog\.pg_attrdef\s+d\s+WHERE\s+.*?a\.atthasdef\s*\)",
    )
    .unwrap()
});

/// The non-default-collation correlated scalar subquery in psql `\d`.
static PG_COLLATION_SUBQUERY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)\(\s*SELECT\s+c\.collname\s+FROM\s+pg_catalog\.pg_collation\s+c,\s*pg_catalog\.pg_type\s+t\s+WHERE\s+.*?t\.typcollation\s*\)",
    )
    .unwrap()
});

/// psql `\d+`'s verbose attribute query renders per-column comments via
/// `col_description(attrelid, attnum)`. That function is not registered, and
/// Fluss columns carry no catalog comments, so it degrades to `NULL`.
static COL_DESCRIPTION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:pg_catalog\.)?col_description\s*\([^)]*\)").unwrap()
});

/// psql `\d`'s RLS-policy probe aggregates the policy's roles with the
/// PostgreSQL `array(SELECT ...)` constructor wrapped in `array_to_string(...)`.
/// DataFusion has no `array` constructor function ("Invalid function 'array'"),
/// and `pg_policy` is always empty for Fluss tables, so the whole role-name
/// expression is degraded to `NULL` — faithful (no policies) and lets `\d` plan.
static PG_POLICY_ROLES_ARRAY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)pg_catalog\.array_to_string\s*\(\s*array\s*\(\s*select\b.*?order\s+by\s+1\s*\)\s*,\s*'[^']*'\s*\)",
    )
    .unwrap()
});

/// psql `\d+`'s class-info query renders `reloptions` with
/// `array_to_string(c.reloptions || array(select ... unnest(tc.reloptions) ...), ', ')`.
/// The `array(SELECT ...)` constructor is unsupported, and Fluss tables carry no
/// reloptions, so the whole expression degrades to an empty string. (The rest of
/// that row is needed, so this is a sub-expression rewrite, not a probe skip.)
static RELOPTIONS_ARRAY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)pg_catalog\.array_to_string\s*\(\s*c\.reloptions\s*\|\|\s*array\(\s*select.*?\)\s*,\s*', '\s*\)",
    )
    .unwrap()
});

/// psql `\d`'s extended-statistics probe tests membership in `stxkind` (a
/// `char[]`) via `'d' = any(stxkind)`. DataFusion lowers `= ANY(<text>)` to
/// `array_has(Utf8, Utf8)`, which does not type-check. `pg_statistic_ext` is
/// always empty for Fluss tables, so each flag expression degrades to `false`.
static STXKIND_ANY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)'[a-z]'\s*=\s*any\s*\(\s*stxkind\s*\)").unwrap()
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
    // Some psql `\d` section probes are built entirely from PostgreSQL-only
    // constructs (ARRAY constructors / indexing, `string_agg` over
    // `generate_series`, `int2[]` casts) that DataFusion cannot plan, and they
    // target catalogs that are always empty for Fluss tables. Short-circuit the
    // whole statement to a zero-row result with the same column arity psql reads
    // positionally — faithful (no such objects) and trivially plannable.
    if let Some(canned) = empty_probe_replacement(sql) {
        return canned;
    }
    // Drop the correlated scalar subqueries first, before function de-qualifying,
    // so they match the raw `pg_catalog.*` text.
    // Each dropped column expression gets a distinct alias: psql reads these
    // columns positionally, but DataFusion rejects a projection with two
    // identically-named (`NULL`) columns, which `\d+` would otherwise produce.
    let s = PG_ATTRDEF_SUBQUERY.replace_all(sql, "NULL AS gw_attrdef");
    let s = PG_COLLATION_SUBQUERY.replace_all(&s, "NULL");
    let s = PG_POLICY_ROLES_ARRAY.replace_all(&s, "NULL");
    let s = RELOPTIONS_ARRAY.replace_all(&s, "''");
    let s = COL_DESCRIPTION.replace_all(&s, "NULL AS gw_coldesc");
    let s = STXKIND_ANY.replace_all(&s, "false");
    let s = PG_CATALOG_OPERATOR.replace_all(&s, "${1}");
    let s = REGTYPE_TO_TEXT_CAST.replace_all(&s, "::text");
    let s = PG_OID_ALIAS_CAST.replace_all(&s, "::text");
    let s = PG_CATALOG_TEXT_CAST.replace_all(&s, "::text");
    let s = COLLATE_DEFAULT.replace_all(&s, "");
    let s = PG_CATALOG_FN.replace_all(&s, "${1}(");
    s.into_owned()
}

/// Short-circuit a psql `\d` section probe that DataFusion cannot plan to an
/// always-empty result with the column arity psql expects positionally. Returns
/// `None` for statements that should go through the normal rewrite chain.
fn empty_probe_replacement(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    // Publications probe: a 3-way UNION over pg_publication using ARRAY indexing,
    // `string_agg` over `generate_series`, and `int2[]` casts. Three output
    // columns (pubname, qual, attrs); always empty for Fluss tables.
    if lower.contains("pg_catalog.pg_publication") {
        return Some(
            "SELECT NULL AS pubname, NULL AS pubqual, NULL AS pubattrs \
             FROM pg_catalog.pg_publication WHERE false"
                .to_string(),
        );
    }
    // NOT NULL constraint probe (`\d+`, PG18): joins on `c.conkey[1]` (array
    // indexing into a column DataFusion models as Utf8). Six output columns;
    // always empty for Fluss tables.
    if lower.contains("conkey[1]") {
        return Some(
            "SELECT NULL AS conname, NULL AS attname, NULL AS connoinherit, \
             NULL AS conislocal, NULL AS coninh, NULL AS convalidated \
             FROM pg_catalog.pg_constraint WHERE false"
                .to_string(),
        );
    }
    None
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
    /// `BEGIN` / `START TRANSACTION` — autocommit no-op.
    Begin,
    /// `COMMIT` / `END` — autocommit no-op.
    Commit,
    /// `ROLLBACK` / `ABORT` — autocommit no-op.
    Rollback,
    /// `DISCARD ALL` / `DISCARD ...` — session reset (vars + rebuild).
    Discard,
    /// A write / DDL statement — rejected with `Unsupported` (read-only path).
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
        // Writes & DDL are rejected up front (read-only path).
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
    fn rewrite_degrades_pg_specific_casts_for_introspection() {
        assert_eq!(
            rewrite_introspection("c.reloftype::pg_catalog.regtype::pg_catalog.text"),
            "c.reloftype::text"
        );
        assert_eq!(
            rewrite_introspection("c.reloftype::regtype::text"),
            "c.reloftype::text"
        );
        assert_eq!(
            rewrite_introspection("x::pg_catalog.text"),
            "x::text"
        );
    }

    #[test]
    fn rewrite_degrades_oid_alias_casts() {
        // psql \d's FK/index queries cast oids to regclass/regproc etc.
        assert_eq!(
            rewrite_introspection("conrelid::pg_catalog.regclass AS ontable"),
            "conrelid::text AS ontable"
        );
        assert_eq!(rewrite_introspection("x::regclass"), "x::text");
        assert_eq!(rewrite_introspection("p.proname::regproc"), "p.proname::text");
    }

    #[test]
    fn rewrite_drops_correlated_scalar_subqueries_in_d_column_query() {
        // The exact column-query shape psql \d sends (whitespace-collapsed here).
        let sql = "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
            (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
             FROM pg_catalog.pg_attrdef d \
             WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
            a.attnotnull, \
            (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
             WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
            a.attidentity FROM pg_catalog.pg_attribute a WHERE a.attrelid = '16386'";
        let out = rewrite_introspection(sql);
        assert!(!out.contains("pg_attrdef"), "default subquery must be dropped: {out}");
        assert!(!out.contains("pg_collation"), "collation subquery must be dropped: {out}");
        assert!(out.contains("a.attnotnull, NULL AS attcollation"), "collation -> NULL: {out}");
        assert!(out.contains("NULL AS gw_attrdef"), "default -> aliased NULL: {out}");
    }

    #[test]
    fn rewrite_drops_policy_roles_array_constructor() {
        // psql \d's RLS-policy probe aggregates roles with array(SELECT ...),
        // which DataFusion has no `array` function for.
        let sql = "CASE WHEN pol.polroles = '{0}' THEN NULL ELSE \
            pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles \
            where oid = any (pol.polroles) order by 1),',') END";
        let out = rewrite_introspection(sql);
        assert!(!out.contains("array("), "array constructor must be gone: {out}");
        assert!(!out.contains("array_to_string"), "array_to_string must be gone: {out}");
        assert!(out.contains("THEN NULL ELSE NULL END"), "roles -> NULL: {out}");
    }

    #[test]
    fn rewrite_degrades_stxkind_membership_to_false() {
        // 'd' = any(stxkind) lowers to array_has(Utf8, Utf8), which fails to plan.
        let sql = "  'd' = any(stxkind) AS ndist_enabled,\n  'f' = any(stxkind) AS deps_enabled";
        let out = rewrite_introspection(sql);
        assert!(!out.contains("stxkind"), "stxkind membership must be gone: {out}");
        assert_eq!(
            out,
            "  false AS ndist_enabled,\n  false AS deps_enabled"
        );
    }

    #[test]
    fn rewrite_blanks_reloptions_array_in_verbose_class_query() {
        // \d+'s reloptions display uses the array(SELECT ...) constructor.
        let sql = "c.relispartition, pg_catalog.array_to_string(c.reloptions || \
            array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ')\n, c.reltablespace";
        let out = rewrite_introspection(sql);
        assert!(!out.contains("array("), "array constructor must be gone: {out}");
        assert!(out.contains("c.relispartition, ''\n, c.reltablespace"), "reloptions -> '': {out}");
    }

    #[test]
    fn rewrite_degrades_col_description_to_aliased_null() {
        let sql = "a.attstorage, pg_catalog.col_description(a.attrelid, a.attnum)\nFROM pg_catalog.pg_attribute a";
        let out = rewrite_introspection(sql);
        assert!(!out.contains("col_description"), "col_description must be gone: {out}");
        assert!(out.contains("NULL AS gw_coldesc"), "col_description -> aliased NULL: {out}");
    }

    #[test]
    fn rewrite_short_circuits_publication_probe() {
        // The pg_publication probe is a 3-way UNION of PG-only constructs.
        let sql = "SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p \
            WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16386')";
        let out = rewrite_introspection(sql);
        assert!(out.contains("WHERE false"), "publication probe must be emptied: {out}");
        assert!(out.contains("AS pubname"), "must keep 3-col arity: {out}");
        assert!(out.contains("AS pubattrs"), "must keep 3-col arity: {out}");
    }

    #[test]
    fn rewrite_short_circuits_not_null_constraint_probe() {
        // The PG18 NOT NULL constraint probe indexes conkey[1] (array element).
        let sql = "SELECT c.conname, a.attname, c.connoinherit, c.conislocal, \
            c.coninhcount <> 0, c.convalidated FROM pg_catalog.pg_constraint c JOIN \
            pg_catalog.pg_attribute a ON (a.attrelid = c.conrelid AND a.attnum = c.conkey[1]) \
            WHERE c.contype = 'n' AND c.conrelid = '16386'::pg_catalog.regclass ORDER BY a.attnum";
        let out = rewrite_introspection(sql);
        assert!(out.contains("WHERE false"), "not-null probe must be emptied: {out}");
        assert!(!out.contains("conkey[1]"), "array indexing must be gone: {out}");
    }

    #[test]
    fn rewrite_is_noop_for_plain_sql() {
        let sql = "SELECT id, name FROM fluss.fluss.kv WHERE id = 1";
        assert_eq!(rewrite_introspection(sql), sql);
    }
}
