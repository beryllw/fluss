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

//! Pure, protocol-free helpers backing the MCP tools.
//!
//! These functions hold no rmcp/axum/HTTP types so they are unit-testable in
//! isolation: a read-only SQL guard, an Arrow `RecordBatch` -> JSON encoder with
//! a row cap, and the table-metadata JSON shape (mirroring the REST view).

use arrow::json::writer::{JsonArray, WriterBuilder};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::error::GatewayError;
use crate::types::TableInfo;

/// Reject anything that is not a single read-only statement.
///
/// Defense-in-depth + a crisp, early error for agents: the engine is already
/// read-only in Phase 1 (PG path is read-only; the direct read path is 501), so
/// this guard never *grants* access — it only stops an agent's accidental write/DDL
/// or multi-statement input before it reaches the SQL path. Authorization itself
/// is always Fluss's call.
pub fn ensure_read_only(sql: &str) -> Result<(), GatewayError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(GatewayError::InvalidArgument("empty SQL statement".into()));
    }

    // Reject multi-statement input: at most one non-empty `;`-separated segment.
    // (A single trailing `;` is allowed.)
    let non_empty_segments = trimmed
        .split(';')
        .filter(|seg| !seg.trim().is_empty())
        .count();
    if non_empty_segments > 1 {
        return Err(GatewayError::InvalidArgument(
            "multiple SQL statements are not allowed; submit a single read-only query".into(),
        ));
    }

    // The leading keyword decides read-only-ness. Only allow statements that
    // cannot mutate state.
    let first = trimmed
        .split(|c: char| c.is_whitespace() || c == '(')
        .find(|t| !t.is_empty())
        .unwrap_or("")
        .to_ascii_uppercase();

    const ALLOWED: &[&str] = &["SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "DESC"];
    if ALLOWED.contains(&first.as_str()) {
        Ok(())
    } else {
        Err(GatewayError::InvalidArgument(format!(
            "only read-only SELECT/WITH/EXPLAIN/SHOW/DESCRIBE statements are allowed (got `{first}`)"
        )))
    }
}

/// Encode up to `max_rows` of `batches` into a `Vec` of JSON objects.
///
/// Returns `(rows, truncated)` where `truncated` is true when more rows were
/// available than `max_rows`. Nulls are emitted explicitly so every row carries
/// the full column set (stable shape for agents). Uses Arrow's JSON array writer
/// (arrow `json` feature) rather than hand-encoding each Arrow type.
pub fn batch_to_json(
    batches: &[RecordBatch],
    max_rows: usize,
) -> Result<(Vec<Value>, bool), GatewayError> {
    let mut rows: Vec<Value> = Vec::new();
    let mut truncated = false;

    for batch in batches {
        if rows.len() >= max_rows {
            // A further non-empty batch exists beyond the cap.
            if batch.num_rows() > 0 {
                truncated = true;
            }
            break;
        }
        let remaining = max_rows - rows.len();
        let slice = if batch.num_rows() > remaining {
            truncated = true;
            batch.slice(0, remaining)
        } else {
            batch.clone()
        };
        if slice.num_rows() == 0 {
            continue;
        }

        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut buf);
            writer
                .write(&slice)
                .map_err(|e| GatewayError::Internal(format!("encode query result to JSON: {e}")))?;
            writer
                .finish()
                .map_err(|e| GatewayError::Internal(format!("finish JSON encoding: {e}")))?;
        }
        let mut batch_rows: Vec<Value> = serde_json::from_slice(&buf)
            .map_err(|e| GatewayError::Internal(format!("parse encoded JSON rows: {e}")))?;
        rows.append(&mut batch_rows);
    }

    Ok((rows, truncated))
}

/// The JSON metadata view of a table: `{database, table, columns:[{name,
/// data_type, nullable}]}`. Same shape the REST `GET .../tables/{t}` returns, so
/// the SQL/REST/MCP metadata views stay consistent (DESIGN.md §14).
pub fn table_info_json(info: &TableInfo) -> Value {
    let columns: Vec<Value> = info
        .schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": f.data_type().to_string(),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    serde_json::json!({
        "database": info.name.database,
        "table": info.name.table,
        "columns": columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::types::TableRef;

    #[test]
    fn ensure_read_only_accepts_read_statements() {
        for sql in [
            "SELECT 1",
            "select * from t",
            "  \n SELECT a FROM t WHERE a > 1 ",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "with x as (select 1) select * from x",
            "EXPLAIN SELECT * FROM t",
            "SHOW search_path",
            "DESCRIBE t",
            "DESC t",
            "SELECT 1;",      // single trailing semicolon ok
            "(SELECT 1)",     // leading paren before SELECT
        ] {
            assert!(ensure_read_only(sql).is_ok(), "should accept: {sql}");
        }
    }

    #[test]
    fn ensure_read_only_rejects_writes_ddl_txn_and_multi() {
        for sql in [
            "",
            "   ",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "MERGE INTO t ...",
            "CREATE TABLE t (a int)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN b int",
            "TRUNCATE t",
            "GRANT SELECT ON t TO u",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SET timezone = 'UTC'",
            "CALL proc()",
            "COPY t FROM '/x'",
            "SELECT 1; DROP TABLE t",  // multi-statement injection
            "SELECT 1; SELECT 2",
        ] {
            assert!(
                matches!(ensure_read_only(sql), Err(GatewayError::InvalidArgument(_))),
                "should reject: {sql:?}"
            );
        }
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn batch_to_json_encodes_rows_including_nulls() {
        let (rows, truncated) = batch_to_json(&[sample_batch()], 100).unwrap();
        assert!(!truncated);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], serde_json::json!({"id": 1, "name": "alice"}));
        // explicit_nulls => the null column is present as JSON null, not omitted.
        assert_eq!(rows[1], serde_json::json!({"id": 2, "name": null}));
    }

    #[test]
    fn batch_to_json_truncates_at_max_rows() {
        let (rows, truncated) = batch_to_json(&[sample_batch()], 1).unwrap();
        assert!(truncated);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], serde_json::json!({"id": 1, "name": "alice"}));
    }

    #[test]
    fn batch_to_json_empty_is_not_truncated() {
        let (rows, truncated) = batch_to_json(&[], 10).unwrap();
        assert!(!truncated);
        assert!(rows.is_empty());
    }

    #[test]
    fn table_info_json_has_expected_shape() {
        let info = TableInfo {
            name: TableRef {
                database: "fluss".into(),
                table: "t".into(),
            },
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ])),
        };
        let json = table_info_json(&info);
        assert_eq!(json["database"], "fluss");
        assert_eq!(json["table"], "t");
        assert_eq!(json["columns"][0]["name"], "id");
        assert_eq!(json["columns"][0]["nullable"], false);
        assert_eq!(json["columns"][1]["name"], "name");
        assert_eq!(json["columns"][1]["nullable"], true);
    }
}
