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

//! P5 — Direct path request models + body decoding.
//!
//! Direct write intents (`KvUpsert` / `KvDelete` / `LogAppend`) executed via the
//! [`GatewayInstance`](crate::instance::GatewayInstance) facade. Phase 1 writes
//! are at-least-once with only request-scoped timeout/cancel — no user-visible
//! Operation, never through the SQL execution chain, never via the
//! SessionManager. Direct reads (lookup/scan) are deferred past Phase 1.
//!
//! This module owns the *protocol-neutral* boundary logic for direct writes:
//! Content-Type negotiation and decoding a write body (JSON rows or Arrow IPC
//! stream) into one Arrow-native `RecordBatch`, plus the canonical kind/path
//! semantics. The HTTP transport (`server/rest`) layers axum on top of this.
//! Design: `design/direct-path.md` §1, §3, §6.

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use crate::error::{GatewayError, GatewayResult};

/// MIME type for the JSON-rows write encoding (curl/BI friendly, small batches).
pub const CONTENT_TYPE_JSON: &str = "application/json";
/// MIME type for the Arrow IPC stream write encoding (large-batch ingest).
pub const CONTENT_TYPE_ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// The two accepted write-body encodings, negotiated from the request
/// `Content-Type`. Both are decoded to one Arrow-native `RecordBatch` at the
/// boundary so the rest of the direct path never sees a wire encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEncoding {
    /// `application/json`: a JSON array of row objects (or newline-delimited
    /// objects), decoded against the target table schema.
    Json,
    /// `application/vnd.apache.arrow.stream`: a self-describing Arrow IPC stream.
    ArrowStream,
}

impl WriteEncoding {
    /// Negotiate the encoding from a raw `Content-Type` header value. Only the
    /// media type is considered; parameters (e.g. `; charset=utf-8`) are ignored.
    /// An absent or unrecognized type is an `InvalidArgument` so the boundary can
    /// answer 400 rather than guess (direct-path.md §3).
    pub fn negotiate(content_type: Option<&str>) -> GatewayResult<WriteEncoding> {
        let raw = content_type.ok_or_else(|| {
            GatewayError::InvalidArgument("missing Content-Type for write body".into())
        })?;
        let media = raw.split(';').next().unwrap_or("").trim();
        match media {
            CONTENT_TYPE_JSON => Ok(WriteEncoding::Json),
            CONTENT_TYPE_ARROW_STREAM => Ok(WriteEncoding::ArrowStream),
            other => Err(GatewayError::InvalidArgument(format!(
                "unsupported write Content-Type: {other}"
            ))),
        }
    }
}

/// Which direct write a request targets. Mirrors [`DirectWriteRequest`] without
/// carrying the decoded batch, so the transport can classify a route before it
/// has a schema/body. KV vs Log selection is the caller's responsibility (the
/// route path picks `records` vs `records:delete`; KV-vs-Log is resolved against
/// the table by the backend in P6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectWriteKind {
    /// Upsert rows into a KV table (`POST .../records` on a KV table).
    KvUpsert,
    /// Delete rows by primary key from a KV table (`POST .../records:delete`).
    KvDelete,
    /// Append rows to a Log table (`POST .../records` on a Log table).
    LogAppend,
}

/// Decode a write body into a single Arrow-native `RecordBatch`.
///
/// - [`WriteEncoding::Json`] decodes against `schema` (taken from the target
///   table; Phase 1 does no schema-on-write).
/// - [`WriteEncoding::ArrowStream`] is self-describing; its embedded schema is
///   returned as-is and the caller is responsible for any schema reconciliation
///   against the target table at the backend (P6).
///
/// All decode failures map to `InvalidArgument` (a malformed body is the
/// caller's fault), which the REST boundary answers as 400.
pub fn decode_write_body(
    encoding: WriteEncoding,
    schema: SchemaRef,
    body: &[u8],
) -> GatewayResult<RecordBatch> {
    match encoding {
        WriteEncoding::Json => decode_json(schema, body),
        WriteEncoding::ArrowStream => decode_arrow_stream(body),
    }
}

fn decode_json(schema: SchemaRef, body: &[u8]) -> GatewayResult<RecordBatch> {
    use arrow::json::ReaderBuilder;

    let bad = |m: String| GatewayError::InvalidArgument(format!("invalid JSON write body: {m}"));

    // The arrow JSON decoder consumes newline-delimited or bare-array-less object
    // streams; normalize a top-level JSON array (the curl-friendly shape) into the
    // object stream the decoder expects.
    let normalized = normalize_json_rows(body).map_err(bad)?;

    let mut decoder = ReaderBuilder::new(schema)
        .build_decoder()
        .map_err(|e| bad(e.to_string()))?;
    decoder.decode(&normalized).map_err(|e| bad(e.to_string()))?;
    decoder
        .flush()
        .map_err(|e| bad(e.to_string()))?
        .ok_or_else(|| bad("empty JSON body produced no rows".into()))
}

/// Accept either a top-level JSON array of objects (`[{...},{...}]`) or a stream
/// of objects (newline-delimited or concatenated), returning bytes the arrow
/// JSON decoder can consume directly (a sequence of object values).
fn normalize_json_rows(body: &[u8]) -> Result<Vec<u8>, String> {
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let trimmed = text.trim_start();
    if trimmed.starts_with('[') {
        // Parse the array and re-emit each element as a standalone object value.
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(trimmed).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in rows {
            let line = serde_json::to_vec(&row).map_err(|e| e.to_string())?;
            out.extend_from_slice(&line);
            out.push(b'\n');
        }
        Ok(out)
    } else {
        // Already an object stream; hand the raw bytes through.
        Ok(body.to_vec())
    }
}

fn decode_arrow_stream(body: &[u8]) -> GatewayResult<RecordBatch> {
    let bad =
        |m: String| GatewayError::InvalidArgument(format!("invalid Arrow IPC write body: {m}"));

    let reader = StreamReader::try_new(std::io::Cursor::new(body), None)
        .map_err(|e| bad(e.to_string()))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| bad(e.to_string()))?);
    }
    if batches.is_empty() {
        return Err(bad("empty Arrow stream produced no batches".into()));
    }
    // Concatenate so the direct path always sees a single batch (Phase 1 ingest
    // volumes are modest; a streaming write seam is deferred).
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| bad(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn negotiate_json_and_arrow_and_params() {
        assert_eq!(
            WriteEncoding::negotiate(Some("application/json")).unwrap(),
            WriteEncoding::Json
        );
        assert_eq!(
            WriteEncoding::negotiate(Some("application/json; charset=utf-8")).unwrap(),
            WriteEncoding::Json
        );
        assert_eq!(
            WriteEncoding::negotiate(Some("application/vnd.apache.arrow.stream")).unwrap(),
            WriteEncoding::ArrowStream
        );
    }

    #[test]
    fn negotiate_missing_or_unknown_is_invalid_argument() {
        assert!(matches!(
            WriteEncoding::negotiate(None),
            Err(GatewayError::InvalidArgument(_))
        ));
        assert!(matches!(
            WriteEncoding::negotiate(Some("text/csv")),
            Err(GatewayError::InvalidArgument(_))
        ));
    }

    #[test]
    fn decode_json_array_rows() {
        let body = br#"[{"id":1,"name":"alice"},{"id":2,"name":null}]"#;
        let batch = decode_write_body(WriteEncoding::Json, schema(), body).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(1));
    }

    #[test]
    fn decode_json_newline_delimited_rows() {
        let body = b"{\"id\":7,\"name\":\"x\"}\n{\"id\":8,\"name\":\"y\"}\n";
        let batch = decode_write_body(WriteEncoding::Json, schema(), body).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn decode_json_malformed_is_invalid_argument() {
        let body = b"{not json";
        assert!(matches!(
            decode_write_body(WriteEncoding::Json, schema(), body),
            Err(GatewayError::InvalidArgument(_))
        ));
    }

    #[test]
    fn decode_json_empty_is_invalid_argument() {
        let body = b"[]";
        assert!(matches!(
            decode_write_body(WriteEncoding::Json, schema(), body),
            Err(GatewayError::InvalidArgument(_))
        ));
    }

    #[test]
    fn decode_arrow_stream_roundtrip() {
        let sch = schema();
        let batch = RecordBatch::try_new(
            sch.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &sch).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        // Arrow stream is self-describing; pass a deliberately empty schema to
        // prove the embedded schema is used, not the argument.
        let empty = Arc::new(Schema::empty());
        let decoded = decode_write_body(WriteEncoding::ArrowStream, empty, &buf).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.schema().fields().len(), 2);
    }

    #[test]
    fn decode_arrow_stream_garbage_is_invalid_argument() {
        let body = b"not an arrow stream";
        let empty = Arc::new(Schema::empty());
        assert!(matches!(
            decode_write_body(WriteEncoding::ArrowStream, empty, body),
            Err(GatewayError::InvalidArgument(_))
        ));
    }
}
