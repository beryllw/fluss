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

//! REST direct-path integration tests.
//!
//! Drives the spawned REST frontend with a real HTTP client (`reqwest`) over
//! loopback TCP, against the `FakeInstance` in `harness`. Covers the P5
//! completion criteria: direct write (KvUpsert / KvDelete / LogAppend) over both
//! body encodings (JSON rows + Arrow IPC stream), the three read-only metadata
//! endpoints, the domain→HTTP error map (404 / 501 / 401), the unsupported read
//! endpoints returning 501, and the semantic guarantee that a direct request
//! creates no session. No Fluss cluster is required.

mod harness;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use fluss_gateway::auth::{ConfigUserStoreAuthenticator, StoredSecret};
use fluss_gateway::server::rest::OtlpConfig;
use fluss_gateway::types::TableRef;
use harness::{FakeInstance, RestTestServer, WriteKind};
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data as MetricData, AggregationTemporality, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span::SpanKind, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use sha2::Digest;

const JSON: &str = "application/json";
const ARROW: &str = "application/vnd.apache.arrow.stream";
const PROTOBUF: &str = "application/x-protobuf";

/// Basic auth header for `user` with an ignored password (trust mode).
fn basic(user: &str) -> String {
    basic_with_password(user, "ignored")
}

fn basic_with_password(user: &str, password: &str) -> String {
    let raw = format!("{user}:{password}");
    encode_base64(raw.as_bytes())
}

fn encode_base64(input: &[u8]) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        out.push(A[(b[0] >> 2) as usize] as char);
        out.push(A[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            A[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[(b[2] & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// JSON rows matching the canned `(id int, name text)` schema.
fn json_rows() -> &'static str {
    r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#
}

/// An Arrow IPC stream of 3 `(id, name)` rows.
fn arrow_rows() -> Vec<u8> {
    use std::sync::Arc as StdArc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;

    let schema = StdArc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            StdArc::new(Int32Array::from(vec![10, 20, 30])),
            StdArc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

fn telemetry_table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("signal", DataType::Utf8, false),
        Field::new("observed_time_unix_nano", DataType::Utf8, true),
        Field::new("time_unix_nano", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("metric_type", DataType::Utf8, true),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("aggregation_temporality", DataType::Int32, true),
        Field::new("is_monotonic", DataType::Boolean, true),
        Field::new("value_double", DataType::Float64, true),
        Field::new("value_int", DataType::Int64, true),
        Field::new("count", DataType::Utf8, true),
        Field::new("sum", DataType::Float64, true),
        Field::new("bucket_counts", DataType::Utf8, true),
        Field::new("explicit_bounds", DataType::Utf8, true),
        Field::new("start_time_unix_nano", DataType::Utf8, true),
        Field::new("end_time_unix_nano", DataType::Utf8, true),
        Field::new("status_code", DataType::Int32, true),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("events", DataType::Utf8, true),
        Field::new("links", DataType::Utf8, true),
    ]))
}

fn otlp_config() -> OtlpConfig {
    OtlpConfig {
        logs_table: TableRef {
            database: "obs".into(),
            table: "logs".into(),
        },
        metrics_table: TableRef {
            database: "obs".into(),
            table: "metrics".into(),
        },
        traces_table: TableRef {
            database: "obs".into(),
            table: "traces".into(),
        },
    }
}

async fn otlp_server() -> RestTestServer {
    let instance = Arc::new(FakeInstance::new());
    let schema = telemetry_table_schema();
    {
        let mut table_schemas = instance.table_schemas.lock().unwrap();
        table_schemas.insert("obs.logs".into(), schema.clone());
        table_schemas.insert("obs.metrics".into(), schema.clone());
        table_schemas.insert("obs.traces".into(), schema);
    }
    RestTestServer::start_with_authenticator_and_otlp(
        instance,
        Arc::new(fluss_gateway::auth::TrustAuthenticator::new()),
        Some(otlp_config()),
    )
    .await
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn minimal_logs_payload() -> Vec<u8> {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue("gateway".into()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "tests.logs".into(),
                    version: "1.0.0".into(),
                    attributes: vec![kv("scope.attr", Value::StringValue("value".into()))],
                    dropped_attributes_count: 0,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 10,
                    observed_time_unix_nano: 11,
                    severity_number: SeverityNumber::Info as i32,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("hello otlp logs".into())),
                    }),
                    attributes: vec![kv("log.attr", Value::IntValue(7))],
                    dropped_attributes_count: 0,
                    flags: 1,
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    event_name: "test.event".into(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn minimal_traces_payload() -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue("gateway".into()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "tests.traces".into(),
                    version: "1.0.0".into(),
                    attributes: vec![kv("scope.attr", Value::StringValue("value".into()))],
                    dropped_attributes_count: 0,
                }),
                spans: vec![Span {
                    trace_id: vec![0x11; 16],
                    span_id: vec![0x22; 8],
                    trace_state: "state".into(),
                    parent_span_id: vec![0x33; 8],
                    flags: 1,
                    name: "span-a".into(),
                    kind: SpanKind::Server as i32,
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 200,
                    attributes: vec![kv("span.attr", Value::BoolValue(true))],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: "ok".into(),
                        code: opentelemetry_proto::tonic::trace::v1::status::StatusCode::Ok as i32,
                    }),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

fn minimal_metrics_payload() -> Vec<u8> {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point;

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", Value::StringValue("gateway".into()))],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "tests.metrics".into(),
                    version: "1.0.0".into(),
                    attributes: vec![kv("scope.attr", Value::StringValue("value".into()))],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![
                    Metric {
                        name: "cpu.gauge".into(),
                        description: "gauge".into(),
                        unit: "%".into(),
                        metadata: vec![],
                        data: Some(MetricData::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![kv("host", Value::StringValue("a".into()))],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1000,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsDouble(1.5)),
                            }],
                        })),
                    },
                    Metric {
                        name: "requests.sum".into(),
                        description: "sum".into(),
                        unit: "1".into(),
                        metadata: vec![],
                        data: Some(MetricData::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![kv("route", Value::StringValue("/".into()))],
                                start_time_unix_nano: 10,
                                time_unix_nano: 2000,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsInt(9)),
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                            is_monotonic: true,
                        })),
                    },
                    Metric {
                        name: "latency.histogram".into(),
                        description: "hist".into(),
                        unit: "ms".into(),
                        metadata: vec![],
                        data: Some(MetricData::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![kv("route", Value::StringValue("/hist".into()))],
                                start_time_unix_nano: 20,
                                time_unix_nano: 3000,
                                count: 3,
                                sum: Some(42.0),
                                bucket_counts: vec![1, 2, 0],
                                explicit_bounds: vec![10.0, 20.0],
                                exemplars: vec![],
                                flags: 0,
                                min: Some(5.0),
                                max: Some(30.0),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        })),
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec()
}

#[tokio::test]
async fn otlp_logs_requires_auth() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/otlp/v1/logs", server.base_url()))
        .header("Content-Type", PROTOBUF)
        .body(minimal_logs_payload())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "unauthenticated");
    assert!(server.instance.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn otlp_metrics_wrong_content_type_maps_to_400() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/otlp/v1/metrics", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body("{}")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
}

#[tokio::test]
async fn otlp_traces_malformed_protobuf_maps_to_400() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/otlp/v1/traces", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", PROTOBUF)
        .body(vec![0xff, 0x01, 0x02])
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
}

#[tokio::test]
async fn otlp_logs_protobuf_succeeds_and_reaches_instance() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/otlp/v1/logs", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", PROTOBUF)
        .body(minimal_logs_payload())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        PROTOBUF
    );
    let bytes = resp.bytes().await.unwrap();
    let decoded = ExportLogsServiceResponse::decode(bytes.as_ref()).unwrap();
    assert!(decoded.partial_success.is_none());

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::LogAppend);
    assert_eq!(writes[0].table.database, "obs");
    assert_eq!(writes[0].table.table, "logs");
    assert_eq!(writes[0].principal, "alice");
    assert_eq!(writes[0].cluster, "default");
    assert_eq!(writes[0].rows, 1);
}

#[tokio::test]
async fn otlp_metrics_protobuf_succeeds_and_reaches_instance() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/otlp/v1/metrics", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("bob")))
        .header("Content-Type", PROTOBUF)
        .body(minimal_metrics_payload())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let decoded = ExportMetricsServiceResponse::decode(bytes.as_ref()).unwrap();
    assert!(decoded.partial_success.is_none());

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::LogAppend);
    assert_eq!(writes[0].table.database, "obs");
    assert_eq!(writes[0].table.table, "metrics");
    assert_eq!(writes[0].principal, "bob");
    assert_eq!(writes[0].rows, 3);
}

#[tokio::test]
async fn otlp_traces_protobuf_succeeds_and_reaches_instance() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/otlp/v1/traces", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("carol")))
        .header("Content-Type", PROTOBUF)
        .body(minimal_traces_payload())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    let decoded = ExportTraceServiceResponse::decode(bytes.as_ref()).unwrap();
    assert!(decoded.partial_success.is_none());

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::LogAppend);
    assert_eq!(writes[0].table.database, "obs");
    assert_eq!(writes[0].table.table, "traces");
    assert_eq!(writes[0].principal, "carol");
    assert_eq!(writes[0].rows, 1);
}

#[tokio::test]
async fn otlp_direct_path_opens_no_session() {
    let server = otlp_server().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    client
        .post(format!("{}/otlp/v1/logs", server.base_url()))
        .header("Authorization", &auth)
        .header("Content-Type", PROTOBUF)
        .body(minimal_logs_payload())
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/otlp/v1/metrics", server.base_url()))
        .header("Authorization", &auth)
        .header("Content-Type", PROTOBUF)
        .body(minimal_metrics_payload())
        .send()
        .await
        .unwrap();

    assert_eq!(*server.instance.sessions_opened.lock().unwrap(), 0);
}

#[tokio::test]
async fn kv_upsert_json_succeeds_and_reaches_instance() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 2);

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::KvUpsert);
    assert_eq!(writes[0].table.database, "db");
    assert_eq!(writes[0].table.table, "t");
    assert_eq!(writes[0].principal, "alice", "principal must come from Basic auth");
    assert_eq!(writes[0].cluster, "default", "cluster must come from path prefix");
    assert_eq!(writes[0].rows, 2);
}

#[tokio::test]
async fn log_append_arrow_stream_succeeds() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("bob")))
        .header("Content-Type", ARROW)
        .body(arrow_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows_written"], 3);

    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].rows, 3);
    assert_eq!(writes[0].principal, "bob");
}

#[tokio::test]
async fn kv_delete_routes_to_delete_kind() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/databases/db/tables/t/records:delete",
            server.base_url()
        ))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(r#"[{"id":1,"name":"x"}]"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let writes = server.instance.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].kind, WriteKind::KvDelete);
}

#[tokio::test]
async fn metadata_endpoints_return_deterministic_values() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    // list databases
    let resp = client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["databases"], serde_json::json!(["fluss"]));

    // list tables
    let resp = client
        .get(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["tables"], serde_json::json!(["t"]));

    // get table info
    let resp = client
        .get(format!("{}/databases/db/tables/t", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["database"], "db");
    assert_eq!(body["table"], "t");
    assert_eq!(body["columns"].as_array().unwrap().len(), 2);
    assert_eq!(body["columns"][0]["name"], "id");
}

#[tokio::test]
async fn unknown_table_maps_to_404() {
    let instance = Arc::new(FakeInstance::new());
    instance.missing_tables.lock().unwrap().push("ghost".into());
    let server = RestTestServer::start_with(instance).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/databases/db/tables/ghost/records",
            server.base_url()
        ))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "table_not_found");
}

#[tokio::test]
async fn missing_auth_maps_to_401() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "unauthenticated");
    // The write must never reach the instance when unauthenticated.
    assert!(server.instance.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn password_authenticator_accepts_correct_password_and_rejects_others() {
    let mut users = HashMap::new();
    users.insert("alice".to_string(), StoredSecret::Plain("secret123".into()));
    users.insert(
        "bob".to_string(),
        StoredSecret::Sha256(sha2::Sha256::digest(b"secret456").into()),
    );
    let auth = Arc::new(ConfigUserStoreAuthenticator::new(users));
    let server = RestTestServer::start_with_authenticator(Arc::new(FakeInstance::new()), auth).await;
    let client = reqwest::Client::new();

    let ok = client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", format!("Basic {}", basic_with_password("alice", "secret123")))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200, "correct plaintext password accepted");

    let ok = client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", format!("Basic {}", basic_with_password("bob", "secret456")))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200, "sha256-configured user sends cleartext password");

    for authz in [
        basic_with_password("alice", "wrong"),
        basic_with_password("nobody", "secret123"),
    ] {
        let resp = client
            .get(format!("{}/databases", server.base_url()))
            .header("Authorization", format!("Basic {}", authz))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "unauthenticated");
    }
}

#[tokio::test]
async fn bad_content_type_maps_to_400() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", "text/csv")
        .body("id,name\n1,a")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
}

#[tokio::test]
async fn deferred_read_endpoints_return_501() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    for path in ["lookup", "prefix-scan", "log-scan"] {
        let resp = client
            .post(format!(
                "{}/databases/db/tables/t/{path}",
                server.base_url()
            ))
            .header("Authorization", &auth)
            .header("Content-Type", JSON)
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 501, "{path} should be Not Implemented");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "unsupported");
    }
}

/// Semantic guarantee (direct-path.md §7): a direct REST request creates no
/// `GatewaySession`. We drive several writes + metadata reads and assert the
/// fake never had `open_session` called.
#[tokio::test]
async fn direct_path_opens_no_session() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();
    let auth = format!("Basic {}", basic("alice"));

    client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", &auth)
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();
    client
        .get(format!("{}/databases", server.base_url()))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    assert_eq!(
        *server.instance.sessions_opened.lock().unwrap(),
        0,
        "direct path must not open any GatewaySession"
    );
}

/// at-least-once (direct-path.md §6): a successful 2xx means backend ack. This
/// test pins the success contract; the unknown-on-timeout case is documented
/// behavior (no rollback, no cancel) and is asserted at the unit level via the
/// error map (Timeout -> 504) rather than racing a real timeout here.
#[tokio::test]
async fn successful_write_is_backend_ack() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables/t/records", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(json_rows())
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());
    // Exactly one ack recorded, with the rows we submitted (no phantom retries).
    assert_eq!(server.instance.writes.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// table management (DDL) — design/direct-path.md "表管理（DDL）API"
// ---------------------------------------------------------------------------

const CREATE_BODY: &str = r#"{
  "table_name": "gw_kv",
  "columns": [
    {"name": "id",   "type": "INT",    "nullable": false},
    {"name": "name", "type": "STRING"}
  ],
  "primary_key": ["id"],
  "distribution": {"bucket_keys": ["id"], "bucket_count": 1},
  "configs": [{"name": "k", "value": "v"}]
}"#;

#[tokio::test]
async fn create_table_succeeds_and_reaches_instance() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(CREATE_BODY)
        .send()
        .await
        .unwrap();

    // 201 Created, returning the new table's metadata (from the fake's get_table_info).
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["database"], "db");
    assert_eq!(body["table"], "gw_kv");

    let created = server.instance.created_tables.lock().unwrap();
    assert_eq!(created.len(), 1, "create reached the instance exactly once");
    assert_eq!(created[0].table.database, "db");
    assert_eq!(created[0].table.table, "gw_kv");
    assert_eq!(created[0].primary_key, vec!["id".to_string()]);
    assert_eq!(created[0].columns.len(), 2);
    assert!(!created[0].columns[0].nullable, "id parsed as NOT NULL");
    assert!(created[0].columns[1].nullable, "name defaults to nullable");
}

#[tokio::test]
async fn create_table_validate_only_does_not_create() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let vbody = r#"{"table_name":"gw_kv","columns":[{"name":"id","type":"INT"}],"validate_only":true}"#;
    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(vbody)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "validate_only returns 200, not 201");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["validate_only"], true);
    assert!(
        server.instance.created_tables.lock().unwrap().is_empty(),
        "validate_only must not create the table"
    );
}

#[tokio::test]
async fn create_table_duplicate_maps_to_409() {
    let instance = Arc::new(FakeInstance::new());
    instance.existing_tables.lock().unwrap().push("gw_kv".into());
    let server = RestTestServer::start_with(instance).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(CREATE_BODY)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 409);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "table_already_exists");
}

#[tokio::test]
async fn create_table_bad_type_maps_to_400() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .header("Content-Type", JSON)
        .body(r#"{"table_name":"t","columns":[{"name":"c","type":"NOTATYPE"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
}

#[tokio::test]
async fn create_table_requires_auth() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/databases/db/tables", server.base_url()))
        .header("Content-Type", JSON)
        .body(CREATE_BODY)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    assert!(server.instance.created_tables.lock().unwrap().is_empty());
}

#[tokio::test]
async fn drop_table_succeeds() {
    let server = RestTestServer::start().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/databases/db/tables/gw_kv", server.base_url()))
        .header("Authorization", format!("Basic {}", basic("alice")))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);
    let dropped = server.instance.dropped_tables.lock().unwrap();
    assert_eq!(dropped.as_slice(), ["gw_kv"]);
}
