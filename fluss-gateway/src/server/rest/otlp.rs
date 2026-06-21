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

//! OTLP-over-HTTP handlers mounted on the shared REST listener.
//!
//! The transport here stays thin: authenticate with the shared Basic-auth seam,
//! decode OTLP protobuf messages, flatten the payloads into Arrow-native rows
//! against the configured destination table schema, then submit them through the
//! existing direct write facade as `LogAppend`. This keeps OTLP wire types at the
//! HTTP boundary and preserves the protocol-agnostic core.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    UInt32Builder,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, HistogramDataPoint, Metric, NumberDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span;
use prost::Message;
use serde_json::{Map, Value};

use crate::error::GatewayError;
use crate::types::{DirectWriteRequest, MetadataScope, TableInfo, TableRef};

use super::{content_type, error_response, make_context, require_otlp, RestState};

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

pub async fn handle_logs(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let context = make_context(&state, &headers, &cluster).await?;
        let request = decode_protobuf::<ExportLogsServiceRequest>(&headers, &body, "logs")?;
        let table = require_otlp(&state)?.logs_table.clone();
        let info = table_info(&state, &context, table.clone()).await?;
        let rows = flatten_logs(&request, info.schema.clone())?;
        append_rows(&state, context, table, rows).await?;
        Ok::<_, GatewayError>(protobuf_response(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
    .await;

    match result {
        Ok(resp) => resp,
        Err(err) => error_response(err),
    }
}

pub async fn handle_metrics(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let context = make_context(&state, &headers, &cluster).await?;
        let request = decode_protobuf::<ExportMetricsServiceRequest>(&headers, &body, "metrics")?;
        let table = require_otlp(&state)?.metrics_table.clone();
        let info = table_info(&state, &context, table.clone()).await?;
        let rows = flatten_metrics(&request, info.schema.clone())?;
        append_rows(&state, context, table, rows).await?;
        Ok::<_, GatewayError>(protobuf_response(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
    .await;

    match result {
        Ok(resp) => resp,
        Err(err) => error_response(err),
    }
}

pub async fn handle_traces(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = async {
        let context = make_context(&state, &headers, &cluster).await?;
        let request = decode_protobuf::<ExportTraceServiceRequest>(&headers, &body, "traces")?;
        let table = require_otlp(&state)?.traces_table.clone();
        let info = table_info(&state, &context, table.clone()).await?;
        let rows = flatten_traces(&request, info.schema.clone())?;
        append_rows(&state, context, table, rows).await?;
        Ok::<_, GatewayError>(protobuf_response(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
    .await;

    match result {
        Ok(resp) => resp,
        Err(err) => error_response(err),
    }
}

fn decode_protobuf<M: Message + Default>(
    headers: &HeaderMap,
    body: &[u8],
    signal: &str,
) -> Result<M, GatewayError> {
    let content_type = content_type(headers).ok_or_else(|| {
        GatewayError::InvalidArgument(format!("missing Content-Type for OTLP {signal} body"))
    })?;
    let media = content_type.split(';').next().unwrap_or("").trim();
    if media != CONTENT_TYPE_PROTOBUF {
        return Err(GatewayError::InvalidArgument(format!(
            "unsupported OTLP Content-Type for {signal}: {media}"
        )));
    }
    M::decode(body).map_err(|e| {
        GatewayError::InvalidArgument(format!("invalid OTLP {signal} protobuf body: {e}"))
    })
}

/// Submit the flattened telemetry rows as a direct `LogAppend`. An empty export
/// (no resource/scope/records) is a valid OTLP no-op, so skip the backend round
/// trip rather than appending a zero-row batch.
async fn append_rows(
    state: &RestState,
    context: crate::types::RequestExecutionContext,
    table: TableRef,
    rows: RecordBatch,
) -> Result<(), GatewayError> {
    if rows.num_rows() == 0 {
        return Ok(());
    }
    state
        .instance
        .write_direct(DirectWriteRequest::LogAppend {
            context,
            table,
            rows,
        })
        .await?;
    Ok(())
}

async fn table_info(
    state: &RestState,
    context: &crate::types::RequestExecutionContext,
    table: TableRef,
) -> Result<TableInfo, GatewayError> {
    state
        .instance
        .get_table_info(
            MetadataScope {
                principal: context.principal.clone(),
                cluster: context.cluster.clone(),
            },
            table,
        )
        .await
}

fn protobuf_response<M: Message>(message: M) -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)],
        message.encode_to_vec(),
    )
        .into_response()
}

fn flatten_logs(
    request: &ExportLogsServiceRequest,
    schema: SchemaRef,
) -> Result<RecordBatch, GatewayError> {
    let mut rows = Vec::new();
    for resource_logs in &request.resource_logs {
        let resource_json = resource_json(resource_logs.resource.as_ref())?;
        for scope_logs in &resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            let scope_name = scope.map(|s| s.name.clone());
            let scope_version = scope.map(|s| s.version.clone());
            let scope_attrs = scope_json(scope)?;
            for log in &scope_logs.log_records {
                rows.push(TelemetryRow {
                    signal: "logs".into(),
                    observed_time_unix_nano: some_string(log.observed_time_unix_nano),
                    time_unix_nano: some_string(log.time_unix_nano),
                    trace_id: some_hex(&log.trace_id),
                    span_id: some_hex(&log.span_id),
                    parent_span_id: None,
                    trace_state: None,
                    name: some_string_ref(if log.event_name.is_empty() {
                        None
                    } else {
                        Some(log.event_name.as_str())
                    }),
                    kind: None,
                    severity_number: non_zero_i32(log.severity_number),
                    severity_text: some_string_ref(if log.severity_text.is_empty() {
                        None
                    } else {
                        Some(log.severity_text.as_str())
                    }),
                    body: any_value_json(log.body.as_ref())?,
                    metric_type: None,
                    metric_description: None,
                    metric_unit: None,
                    aggregation_temporality: None,
                    is_monotonic: None,
                    value_double: None,
                    value_int: None,
                    count: None,
                    sum: None,
                    bucket_counts: None,
                    explicit_bounds: None,
                    start_time_unix_nano: None,
                    end_time_unix_nano: None,
                    status_code: None,
                    status_message: None,
                    flags: non_zero_u32(log.flags),
                    resource_attributes: resource_json.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    scope_attributes: scope_attrs.clone(),
                    attributes: key_values_json(&log.attributes)?,
                    events: None,
                    links: None,
                });
            }
        }
    }
    build_batch(schema, rows)
}

fn flatten_traces(
    request: &ExportTraceServiceRequest,
    schema: SchemaRef,
) -> Result<RecordBatch, GatewayError> {
    let mut rows = Vec::new();
    for resource_spans in &request.resource_spans {
        let resource_json = resource_json(resource_spans.resource.as_ref())?;
        for scope_spans in &resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            let scope_name = scope.map(|s| s.name.clone());
            let scope_version = scope.map(|s| s.version.clone());
            let scope_attrs = scope_json(scope)?;
            for span in &scope_spans.spans {
                rows.push(TelemetryRow {
                    signal: "traces".into(),
                    observed_time_unix_nano: None,
                    time_unix_nano: None,
                    trace_id: some_hex(&span.trace_id),
                    span_id: some_hex(&span.span_id),
                    parent_span_id: some_hex(&span.parent_span_id),
                    trace_state: some_string_ref(if span.trace_state.is_empty() {
                        None
                    } else {
                        Some(span.trace_state.as_str())
                    }),
                    name: some_string_ref(if span.name.is_empty() {
                        None
                    } else {
                        Some(span.name.as_str())
                    }),
                    kind: non_zero_i32(span.kind)
                        .map(|kind| span_kind_name(kind).to_string()),
                    severity_number: None,
                    severity_text: None,
                    body: None,
                    metric_type: None,
                    metric_description: None,
                    metric_unit: None,
                    aggregation_temporality: None,
                    is_monotonic: None,
                    value_double: None,
                    value_int: None,
                    count: None,
                    sum: None,
                    bucket_counts: None,
                    explicit_bounds: None,
                    start_time_unix_nano: some_string(span.start_time_unix_nano),
                    end_time_unix_nano: some_string(span.end_time_unix_nano),
                    status_code: span.status.as_ref().map(|s| s.code),
                    status_message: span.status.as_ref().and_then(|s| {
                        if s.message.is_empty() {
                            None
                        } else {
                            Some(s.message.clone())
                        }
                    }),
                    flags: non_zero_u32(span.flags),
                    resource_attributes: resource_json.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    scope_attributes: scope_attrs.clone(),
                    attributes: key_values_json(&span.attributes)?,
                    events: span_events_json(&span.events)?,
                    links: span_links_json(&span.links)?,
                });
            }
        }
    }
    build_batch(schema, rows)
}

/// The resource/scope context shared by every row flattened from one
/// `ScopeMetrics` block; cloned per row at batch-build time.
#[derive(Clone, Default)]
struct ScopeContext {
    resource_attributes: Option<String>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attributes: Option<String>,
}

fn flatten_metrics(
    request: &ExportMetricsServiceRequest,
    schema: SchemaRef,
) -> Result<RecordBatch, GatewayError> {
    let mut rows = Vec::new();
    for resource_metrics in &request.resource_metrics {
        let resource_attributes = resource_json(resource_metrics.resource.as_ref())?;
        for scope_metrics in &resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.as_ref();
            let ctx = ScopeContext {
                resource_attributes: resource_attributes.clone(),
                scope_name: scope.map(|s| s.name.clone()),
                scope_version: scope.map(|s| s.version.clone()),
                scope_attributes: scope_json(scope)?,
            };
            for metric in &scope_metrics.metrics {
                let mut metric_rows = flatten_metric(metric, &ctx)?;
                rows.append(&mut metric_rows);
            }
        }
    }
    build_batch(schema, rows)
}

fn flatten_metric(metric: &Metric, ctx: &ScopeContext) -> Result<Vec<TelemetryRow>, GatewayError> {
    match metric.data.as_ref() {
        Some(metric::Data::Gauge(gauge)) => gauge
            .data_points
            .iter()
            .map(|point| telemetry_row_for_number_point(metric, "gauge", point, None, None, ctx))
            .collect(),
        Some(metric::Data::Sum(sum)) => sum
            .data_points
            .iter()
            .map(|point| {
                telemetry_row_for_number_point(
                    metric,
                    "sum",
                    point,
                    Some(sum.aggregation_temporality),
                    Some(sum.is_monotonic),
                    ctx,
                )
            })
            .collect(),
        Some(metric::Data::Histogram(histogram)) => histogram
            .data_points
            .iter()
            .map(|point| {
                telemetry_row_for_histogram(metric, point, histogram.aggregation_temporality, ctx)
            })
            .collect(),
        Some(metric::Data::ExponentialHistogram(_)) => Err(GatewayError::Unsupported(
            format!("OTLP metric type exponential histogram is not supported for `{}`", metric.name),
        )),
        Some(metric::Data::Summary(_)) => Err(GatewayError::Unsupported(format!(
            "OTLP metric type summary is not supported for `{}`",
            metric.name
        ))),
        None => Err(GatewayError::InvalidArgument(format!(
            "OTLP metric `{}` has no data",
            metric.name
        ))),
    }
}

fn telemetry_row_for_number_point(
    metric: &Metric,
    metric_type: &str,
    point: &NumberDataPoint,
    temporality: Option<i32>,
    is_monotonic: Option<bool>,
    ctx: &ScopeContext,
) -> Result<TelemetryRow, GatewayError> {
    let (value_double, value_int) = match point.value {
        Some(number_data_point::Value::AsDouble(v)) => (Some(v), None),
        Some(number_data_point::Value::AsInt(v)) => (None, Some(v)),
        None => {
            return Err(GatewayError::InvalidArgument(format!(
                "OTLP metric `{}` data point has no value",
                metric.name
            )))
        }
    };
    Ok(TelemetryRow {
        signal: "metrics".into(),
        observed_time_unix_nano: None,
        time_unix_nano: some_string(point.time_unix_nano),
        trace_id: None,
        span_id: None,
        parent_span_id: None,
        trace_state: None,
        name: some_string_ref(Some(metric.name.as_str())),
        kind: None,
        severity_number: None,
        severity_text: None,
        body: None,
        metric_type: Some(metric_type.to_string()),
        metric_description: some_string_ref(if metric.description.is_empty() {
            None
        } else {
            Some(metric.description.as_str())
        }),
        metric_unit: some_string_ref(if metric.unit.is_empty() {
            None
        } else {
            Some(metric.unit.as_str())
        }),
        aggregation_temporality: temporality,
        is_monotonic,
        value_double,
        value_int,
        count: None,
        sum: None,
        bucket_counts: None,
        explicit_bounds: None,
        start_time_unix_nano: some_string(point.start_time_unix_nano),
        end_time_unix_nano: None,
        status_code: None,
        status_message: None,
        flags: non_zero_u32(point.flags),
        resource_attributes: ctx.resource_attributes.clone(),
        scope_name: ctx.scope_name.clone(),
        scope_version: ctx.scope_version.clone(),
        scope_attributes: ctx.scope_attributes.clone(),
        attributes: key_values_json(&point.attributes)?,
        events: None,
        links: None,
    })
}

fn telemetry_row_for_histogram(
    metric: &Metric,
    point: &HistogramDataPoint,
    aggregation_temporality: i32,
    ctx: &ScopeContext,
) -> Result<TelemetryRow, GatewayError> {
    Ok(TelemetryRow {
        signal: "metrics".into(),
        observed_time_unix_nano: None,
        time_unix_nano: some_string(point.time_unix_nano),
        trace_id: None,
        span_id: None,
        parent_span_id: None,
        trace_state: None,
        name: some_string_ref(Some(metric.name.as_str())),
        kind: None,
        severity_number: None,
        severity_text: None,
        body: None,
        metric_type: Some("histogram".into()),
        metric_description: some_string_ref(if metric.description.is_empty() {
            None
        } else {
            Some(metric.description.as_str())
        }),
        metric_unit: some_string_ref(if metric.unit.is_empty() {
            None
        } else {
            Some(metric.unit.as_str())
        }),
        aggregation_temporality: Some(aggregation_temporality),
        is_monotonic: None,
        value_double: None,
        value_int: None,
        count: some_string(point.count),
        sum: point.sum,
        bucket_counts: Some(json_string(Value::Array(
            point
                .bucket_counts
                .iter()
                .map(|v| Value::String(v.to_string()))
                .collect(),
        ))?),
        explicit_bounds: Some(json_string(Value::Array(
            point
                .explicit_bounds
                .iter()
                .map(|v| json_number_from_f64(*v))
                .collect(),
        ))?),
        start_time_unix_nano: some_string(point.start_time_unix_nano),
        end_time_unix_nano: None,
        status_code: None,
        status_message: None,
        flags: non_zero_u32(point.flags),
        resource_attributes: ctx.resource_attributes.clone(),
        scope_name: ctx.scope_name.clone(),
        scope_version: ctx.scope_version.clone(),
        scope_attributes: ctx.scope_attributes.clone(),
        attributes: key_values_json(&point.attributes)?,
        events: None,
        links: None,
    })
}

#[derive(Default)]
struct TelemetryRow {
    signal: String,
    observed_time_unix_nano: Option<String>,
    time_unix_nano: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    parent_span_id: Option<String>,
    trace_state: Option<String>,
    name: Option<String>,
    kind: Option<String>,
    severity_number: Option<i32>,
    severity_text: Option<String>,
    body: Option<String>,
    metric_type: Option<String>,
    metric_description: Option<String>,
    metric_unit: Option<String>,
    aggregation_temporality: Option<i32>,
    is_monotonic: Option<bool>,
    value_double: Option<f64>,
    value_int: Option<i64>,
    count: Option<String>,
    sum: Option<f64>,
    bucket_counts: Option<String>,
    explicit_bounds: Option<String>,
    start_time_unix_nano: Option<String>,
    end_time_unix_nano: Option<String>,
    status_code: Option<i32>,
    status_message: Option<String>,
    flags: Option<u32>,
    resource_attributes: Option<String>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attributes: Option<String>,
    attributes: Option<String>,
    events: Option<String>,
    links: Option<String>,
}

fn build_batch(schema: SchemaRef, rows: Vec<TelemetryRow>) -> Result<RecordBatch, GatewayError> {
    let mut signal = StringBuilder::new();
    let mut observed_time_unix_nano = StringBuilder::new();
    let mut time_unix_nano = StringBuilder::new();
    let mut trace_id = StringBuilder::new();
    let mut span_id = StringBuilder::new();
    let mut parent_span_id = StringBuilder::new();
    let mut trace_state = StringBuilder::new();
    let mut name = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut severity_number = Int32Builder::new();
    let mut severity_text = StringBuilder::new();
    let mut body = StringBuilder::new();
    let mut metric_type = StringBuilder::new();
    let mut metric_description = StringBuilder::new();
    let mut metric_unit = StringBuilder::new();
    let mut aggregation_temporality = Int32Builder::new();
    let mut is_monotonic = BooleanBuilder::new();
    let mut value_double = Float64Builder::new();
    let mut value_int = Int64Builder::new();
    let mut count = StringBuilder::new();
    let mut sum = Float64Builder::new();
    let mut bucket_counts = StringBuilder::new();
    let mut explicit_bounds = StringBuilder::new();
    let mut start_time_unix_nano = StringBuilder::new();
    let mut end_time_unix_nano = StringBuilder::new();
    let mut status_code = Int32Builder::new();
    let mut status_message = StringBuilder::new();
    let mut flags = UInt32Builder::new();
    let mut resource_attributes = StringBuilder::new();
    let mut scope_name = StringBuilder::new();
    let mut scope_version = StringBuilder::new();
    let mut scope_attributes = StringBuilder::new();
    let mut attributes = StringBuilder::new();
    let mut events = StringBuilder::new();
    let mut links = StringBuilder::new();

    for row in rows {
        signal.append_value(row.signal);
        append_string(&mut observed_time_unix_nano, row.observed_time_unix_nano.as_deref());
        append_string(&mut time_unix_nano, row.time_unix_nano.as_deref());
        append_string(&mut trace_id, row.trace_id.as_deref());
        append_string(&mut span_id, row.span_id.as_deref());
        append_string(&mut parent_span_id, row.parent_span_id.as_deref());
        append_string(&mut trace_state, row.trace_state.as_deref());
        append_string(&mut name, row.name.as_deref());
        append_string(&mut kind, row.kind.as_deref());
        append_i32(&mut severity_number, row.severity_number);
        append_string(&mut severity_text, row.severity_text.as_deref());
        append_string(&mut body, row.body.as_deref());
        append_string(&mut metric_type, row.metric_type.as_deref());
        append_string(&mut metric_description, row.metric_description.as_deref());
        append_string(&mut metric_unit, row.metric_unit.as_deref());
        append_i32(&mut aggregation_temporality, row.aggregation_temporality);
        append_bool(&mut is_monotonic, row.is_monotonic);
        append_f64(&mut value_double, row.value_double);
        append_i64(&mut value_int, row.value_int);
        append_string(&mut count, row.count.as_deref());
        append_f64(&mut sum, row.sum);
        append_string(&mut bucket_counts, row.bucket_counts.as_deref());
        append_string(&mut explicit_bounds, row.explicit_bounds.as_deref());
        append_string(&mut start_time_unix_nano, row.start_time_unix_nano.as_deref());
        append_string(&mut end_time_unix_nano, row.end_time_unix_nano.as_deref());
        append_i32(&mut status_code, row.status_code);
        append_string(&mut status_message, row.status_message.as_deref());
        append_u32(&mut flags, row.flags);
        append_string(&mut resource_attributes, row.resource_attributes.as_deref());
        append_string(&mut scope_name, row.scope_name.as_deref());
        append_string(&mut scope_version, row.scope_version.as_deref());
        append_string(&mut scope_attributes, row.scope_attributes.as_deref());
        append_string(&mut attributes, row.attributes.as_deref());
        append_string(&mut events, row.events.as_deref());
        append_string(&mut links, row.links.as_deref());
    }

    let actual = vec![
        Arc::new(signal.finish()) as ArrayRef,
        Arc::new(observed_time_unix_nano.finish()),
        Arc::new(time_unix_nano.finish()),
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(trace_state.finish()),
        Arc::new(name.finish()),
        Arc::new(kind.finish()),
        Arc::new(severity_number.finish()),
        Arc::new(severity_text.finish()),
        Arc::new(body.finish()),
        Arc::new(metric_type.finish()),
        Arc::new(metric_description.finish()),
        Arc::new(metric_unit.finish()),
        Arc::new(aggregation_temporality.finish()),
        Arc::new(is_monotonic.finish()),
        Arc::new(value_double.finish()),
        Arc::new(value_int.finish()),
        Arc::new(count.finish()),
        Arc::new(sum.finish()),
        Arc::new(bucket_counts.finish()),
        Arc::new(explicit_bounds.finish()),
        Arc::new(start_time_unix_nano.finish()),
        Arc::new(end_time_unix_nano.finish()),
        Arc::new(status_code.finish()),
        Arc::new(status_message.finish()),
        Arc::new(flags.finish()),
        Arc::new(resource_attributes.finish()),
        Arc::new(scope_name.finish()),
        Arc::new(scope_version.finish()),
        Arc::new(scope_attributes.finish()),
        Arc::new(attributes.finish()),
        Arc::new(events.finish()),
        Arc::new(links.finish()),
    ];

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let idx = telemetry_column_index(field.name()).ok_or_else(|| {
            GatewayError::InvalidArgument(format!(
                "OTLP destination schema contains unsupported column `{}`",
                field.name()
            ))
        })?;
        let array = actual[idx].clone();
        if array.data_type() != field.data_type() {
            return Err(GatewayError::InvalidArgument(format!(
                "OTLP destination column `{}` has type {}, but the adapter produces {}",
                field.name(),
                field.data_type(),
                array.data_type()
            )));
        }
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| GatewayError::InvalidArgument(format!("invalid OTLP record batch: {e}")))
}

fn telemetry_column_index(name: &str) -> Option<usize> {
    Some(match name {
        "signal" => 0,
        "observed_time_unix_nano" => 1,
        "time_unix_nano" => 2,
        "trace_id" => 3,
        "span_id" => 4,
        "parent_span_id" => 5,
        "trace_state" => 6,
        "name" => 7,
        "kind" => 8,
        "severity_number" => 9,
        "severity_text" => 10,
        "body" => 11,
        "metric_type" => 12,
        "metric_description" => 13,
        "metric_unit" => 14,
        "aggregation_temporality" => 15,
        "is_monotonic" => 16,
        "value_double" => 17,
        "value_int" => 18,
        "count" => 19,
        "sum" => 20,
        "bucket_counts" => 21,
        "explicit_bounds" => 22,
        "start_time_unix_nano" => 23,
        "end_time_unix_nano" => 24,
        "status_code" => 25,
        "status_message" => 26,
        "flags" => 27,
        "resource_attributes" => 28,
        "scope_name" => 29,
        "scope_version" => 30,
        "scope_attributes" => 31,
        "attributes" => 32,
        "events" => 33,
        "links" => 34,
        _ => return None,
    })
}

fn resource_json(resource: Option<&Resource>) -> Result<Option<String>, GatewayError> {
    match resource {
        Some(resource) => key_values_json(&resource.attributes),
        None => Ok(None),
    }
}

fn scope_json(scope: Option<&InstrumentationScope>) -> Result<Option<String>, GatewayError> {
    match scope {
        Some(scope) => key_values_json(&scope.attributes),
        None => Ok(None),
    }
}

fn key_values_json(values: &[KeyValue]) -> Result<Option<String>, GatewayError> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut object = Map::new();
    for kv in values {
        object.insert(kv.key.clone(), any_value_to_json(kv.value.as_ref())?);
    }
    json_string(Value::Object(object)).map(Some)
}

fn any_value_json(value: Option<&AnyValue>) -> Result<Option<String>, GatewayError> {
    match value {
        Some(value) => json_string(any_value_to_json(Some(value))?).map(Some),
        None => Ok(None),
    }
}

fn any_value_to_json(value: Option<&AnyValue>) -> Result<Value, GatewayError> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    Ok(match value.value.as_ref() {
        Some(any_value::Value::StringValue(v)) => Value::String(v.clone()),
        Some(any_value::Value::BoolValue(v)) => Value::Bool(*v),
        Some(any_value::Value::IntValue(v)) => Value::String(v.to_string()),
        Some(any_value::Value::DoubleValue(v)) => json_number_from_f64(*v),
        Some(any_value::Value::ArrayValue(v)) => Value::Array(
            v.values
                .iter()
                .map(|item| any_value_to_json(Some(item)))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(any_value::Value::KvlistValue(v)) => {
            let mut object = Map::new();
            for kv in &v.values {
                object.insert(kv.key.clone(), any_value_to_json(kv.value.as_ref())?);
            }
            Value::Object(object)
        }
        Some(any_value::Value::BytesValue(v)) => Value::String(hex(v)),
        Some(any_value::Value::StringValueStrindex(v)) => Value::String(v.to_string()),
        None => Value::Null,
    })
}

fn span_events_json(events: &[span::Event]) -> Result<Option<String>, GatewayError> {
    if events.is_empty() {
        return Ok(None);
    }
    let values = events
        .iter()
        .map(|event| {
            let mut object = Map::new();
            object.insert(
                "time_unix_nano".into(),
                Value::String(event.time_unix_nano.to_string()),
            );
            object.insert("name".into(), Value::String(event.name.clone()));
            if let Some(attrs) = key_values_json(&event.attributes)? {
                object.insert(
                    "attributes".into(),
                    serde_json::from_str(&attrs).map_err(|e| {
                        GatewayError::Internal(format!("decode span event attrs json: {e}"))
                    })?,
                );
            }
            Ok(Value::Object(object))
        })
        .collect::<Result<Vec<_>, GatewayError>>()?;
    json_string(Value::Array(values)).map(Some)
}

fn span_links_json(links: &[span::Link]) -> Result<Option<String>, GatewayError> {
    if links.is_empty() {
        return Ok(None);
    }
    let values = links
        .iter()
        .map(|link| {
            let mut object = Map::new();
            object.insert("trace_id".into(), Value::String(hex(&link.trace_id)));
            object.insert("span_id".into(), Value::String(hex(&link.span_id)));
            if !link.trace_state.is_empty() {
                object.insert("trace_state".into(), Value::String(link.trace_state.clone()));
            }
            if let Some(attrs) = key_values_json(&link.attributes)? {
                object.insert(
                    "attributes".into(),
                    serde_json::from_str(&attrs).map_err(|e| {
                        GatewayError::Internal(format!("decode span link attrs json: {e}"))
                    })?,
                );
            }
            if link.flags != 0 {
                object.insert(
                    "flags".into(),
                    Value::Number(serde_json::Number::from(link.flags)),
                );
            }
            Ok(Value::Object(object))
        })
        .collect::<Result<Vec<_>, GatewayError>>()?;
    json_string(Value::Array(values)).map(Some)
}

fn append_string(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_i64(builder: &mut Int64Builder, value: Option<i64>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_u32(builder: &mut UInt32Builder, value: Option<u32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_bool(builder: &mut BooleanBuilder, value: Option<bool>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn some_string<T: ToString>(value: T) -> Option<String> {
    Some(value.to_string())
}

fn some_string_ref(value: Option<&str>) -> Option<String> {
    value.map(|v| v.to_string())
}

fn non_zero_i32(value: i32) -> Option<i32> {
    (value != 0).then_some(value)
}

fn non_zero_u32(value: u32) -> Option<u32> {
    (value != 0).then_some(value)
}

fn some_hex(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| hex(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn json_string(value: Value) -> Result<String, GatewayError> {
    serde_json::to_string(&value)
        .map_err(|e| GatewayError::Internal(format!("encode OTLP json value: {e}")))
}

fn json_number_from_f64(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn span_kind_name(kind: i32) -> &'static str {
    match kind {
        x if x == opentelemetry_proto::tonic::trace::v1::span::SpanKind::Internal as i32 => {
            "INTERNAL"
        }
        x if x == opentelemetry_proto::tonic::trace::v1::span::SpanKind::Server as i32 => {
            "SERVER"
        }
        x if x == opentelemetry_proto::tonic::trace::v1::span::SpanKind::Client as i32 => {
            "CLIENT"
        }
        x if x == opentelemetry_proto::tonic::trace::v1::span::SpanKind::Producer as i32 => {
            "PRODUCER"
        }
        x if x == opentelemetry_proto::tonic::trace::v1::span::SpanKind::Consumer as i32 => {
            "CONSUMER"
        }
        _ => "UNSPECIFIED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    #[test]
    fn key_values_json_supports_nested_values() {
        let attrs = vec![KeyValue {
            key: "nested".into(),
            value: Some(AnyValue {
                value: Some(Value::KvlistValue(opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![KeyValue {
                        key: "k".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("v".into())),
                        }),
                        key_strindex: 0,
                    }],
                })),
            }),
            key_strindex: 0,
        }];
        let json = key_values_json(&attrs).unwrap().unwrap();
        assert!(json.contains("nested"));
        assert!(json.contains("\"k\":\"v\""));
    }

    #[test]
    fn build_batch_rejects_unknown_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("unknown", DataType::Utf8, true)]));
        let err = build_batch(
            schema,
            vec![TelemetryRow {
                signal: "logs".into(),
                ..Default::default()
            }],
        )
        .unwrap_err();
        assert!(matches!(err, GatewayError::InvalidArgument(_)));
    }
}
