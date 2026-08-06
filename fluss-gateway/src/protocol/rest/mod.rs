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

//! REST adapter: router assembly and cross-cutting middleware.
//!
//! Data and metadata handlers dispatch through the orchestration modules (`catalog_ops`,
//! `write_ops`, `lookup_ops`), which resolve the caller's backend from the cluster registry. Each endpoint module builds its own [`OpenApiRouter`], and [`build_router`]
//! merges them, splits the result into an Axum router plus the generated OpenAPI document, and wraps the router
//! in the middleware defined here.
//!
//! The middleware applies two per-request input-validation bounds — a maximum body size (413) and the
//! per-request deadline (504) — and no request rate limiting. HTTP 429 appears for exactly one condition:
//! the per-user act-as connection pool of the user identity mode is at capacity
//! (`resource_exhausted`, always with a `Retry-After` header, per FIP-49).

pub mod auth;
pub(crate) mod catalog_ops;
pub mod clusters;
pub mod datatype;
pub mod ddl;
pub mod health;
pub mod input;
pub mod input_decode;
pub mod input_value;
pub mod json;
pub mod limits;
pub mod lookup;
pub(crate) mod lookup_ops;
pub mod metadata;
pub mod openapi;
pub mod pagination;
pub mod records;
pub(crate) mod write_ops;

use crate::auth::{Authenticator, Principal};
use crate::backend::context::{CancellationSignal, RequestContext};
use crate::backend::registry::ClusterRegistry;
use crate::backend::types::ClusterId;
use crate::config::{LookupConfig, MetadataConfig, RestServerConfig, WriteConfig};
use crate::error::{ErrorEnvelope, GatewayError};
use crate::lifecycle::Readiness;
use crate::observability;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio_util::sync::DropGuard;
use utoipa_axum::router::OpenApiRouter;

/// Shared state for REST handlers.
///
/// Everything here is either immutable configuration or a shared process service. Nothing is scoped to a
/// request, a session, or a client.
#[derive(Clone)]
pub struct RestState {
    /// Cluster registry resolving each request's backend; also serves discovery and health.
    pub clusters: Arc<ClusterRegistry>,
    /// The finite per-entry write delivery lifetime from `gateway.rest.write.max-delivery-time`.
    pub write_delivery_time: Duration,
    pub readiness: Arc<Readiness>,
    pub bind_address: SocketAddr,
    pub started_at: Instant,
    /// Input-validation caps of the two lookup endpoints, from `[lookup]`.
    pub lookup_limits: LookupLimits,
    /// Catalog pagination bounds from `[metadata]`.
    pub metadata_limits: MetadataLimits,
    /// Row limits of the records endpoint, from `[write]`.
    pub write_limits: WriteLimits,
    /// The OpenAPI document generated from the router this state was installed into.
    ///
    /// [`build_router`] fills it once, after the route modules are merged and split, so the served document is
    /// exactly the contract of the routes that are actually mounted.
    pub openapi: Arc<OnceLock<serde_json::Value>>,
    /// The one global client-to-gateway authenticator, from `gateway.security.*`.
    pub authenticator: Arc<dyn Authenticator>,
}

/// Returns a bounded metric label for a configured cluster, or one static value for rejected IDs.
///
/// Every endpoint that emits a per-cluster metric goes through this, which is what keeps a caller-supplied
/// cluster id out of the label set.
#[allow(dead_code)] // Consumed by the endpoint modules as they are implemented.
pub(crate) fn metric_cluster(state: &RestState, requested: &str) -> String {
    state
        .clusters
        .snapshots()
        .into_iter()
        .find(|snapshot| snapshot.id.as_str() == requested)
        .map(|snapshot| snapshot.id.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Write request limits supplied from `[write]` configuration.
#[derive(Debug, Clone, Copy)]
pub struct WriteLimits {
    /// Maximum entries in one request, excess yields 413.
    pub max_rows: usize,
}

impl From<&WriteConfig> for WriteLimits {
    fn from(config: &WriteConfig) -> Self {
        Self {
            max_rows: config.max_rows as usize,
        }
    }
}

/// Lookup request limits, extracted from `[lookup]`.
#[derive(Debug, Clone, Copy)]
pub struct LookupLimits {
    /// Maximum keys per point-lookup request, excess yields 413.
    pub max_keys: usize,
    /// Bound on the estimated total key bytes per request, excess yields 413.
    pub max_key_bytes: u64,
    /// Maximum prefixes per prefix-lookup request, excess yields 413.
    pub max_prefixes: usize,
    /// Per-prefix row cap applied as truncation, never as an error.
    pub max_rows_per_prefix: usize,
}

impl From<&LookupConfig> for LookupLimits {
    fn from(config: &LookupConfig) -> Self {
        Self {
            max_keys: config.max_keys as usize,
            max_key_bytes: config.max_key_bytes.bytes(),
            max_prefixes: config.max_prefixes as usize,
            max_rows_per_prefix: config.max_rows_per_prefix as usize,
        }
    }
}

/// Catalog collection pagination limits, extracted from `[metadata]`.
#[derive(Debug, Clone, Copy)]
pub struct MetadataLimits {
    pub default_page_size: usize,
    pub max_page_size: usize,
}

impl From<&MetadataConfig> for MetadataLimits {
    fn from(config: &MetadataConfig) -> Self {
        Self {
            default_page_size: config.default_page_size as usize,
            max_page_size: config.max_page_size as usize,
        }
    }
}

/// Per-request identifier, generated by the outermost middleware and echoed in the `x-request-id` response header and
/// error envelopes.
#[derive(Clone, Debug)]
pub struct RequestId(Arc<str>);

impl RequestId {
    /// The value echoed in the `x-request-id` header, or `unknown` when no middleware assigned one.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self(Arc::from("unknown"))
    }
}

/// Absolute request deadline assigned by the same middleware that enforces it.
#[derive(Clone, Copy, Debug)]
pub struct RequestDeadline(Instant);

impl RequestDeadline {
    pub fn instant(self) -> Instant {
        self.0
    }
}

/// Builds the protocol-neutral context shared by every REST application call.
#[allow(dead_code)] // Consumed by the endpoint modules as they are implemented.
pub(crate) fn application_context(
    request_id: &RequestId,
    deadline: RequestDeadline,
    principal: &Principal,
    cluster: &str,
) -> Result<RestRequestContext, GatewayError> {
    let cluster_id = ClusterId::try_from(cluster).map_err(|_| {
        GatewayError::not_found(format!("unknown cluster `{cluster}`"))
            .with_resource("cluster", Some(cluster))
    })?;
    let cancellation = CancellationSignal::default();
    let cancellation_guard = cancellation.drop_guard();
    Ok(RestRequestContext {
        context: RequestContext::new(
            request_id.as_str(),
            cluster_id,
            deadline.instant(),
            cancellation.clone(),
            principal.clone(),
        ),
        _cancellation_guard: cancellation_guard,
    })
}

/// REST-owned request context that cancels application work when its handler is dropped.
///
/// The inner context stays protocol neutral. Future adapters can provide their own lifecycle guard
/// without teaching the application layer about HTTP handler cancellation.
#[allow(dead_code)] // Consumed by the endpoint modules as they are implemented.
pub(crate) struct RestRequestContext {
    context: RequestContext,
    _cancellation_guard: DropGuard,
}

impl Deref for RestRequestContext {
    type Target = RequestContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

/// Middleware limits, extracted from `[server.rest]`.
#[derive(Debug, Clone)]
pub struct RestOptions {
    pub request_timeout: Duration,
    pub max_body_bytes: u64,
}

impl From<&RestServerConfig> for RestOptions {
    fn from(config: &RestServerConfig) -> Self {
        Self {
            request_timeout: config.request_timeout.get(),
            max_body_bytes: config.max_body_bytes.bytes(),
        }
    }
}

/// Marks a response whose body is already in its final shape so the error-normalising middleware leaves it alone.
#[derive(Clone, Copy)]
struct ShapedResponse;

/// Renders the error envelope with the status its kind maps to, marks the response as already shaped, and adds
/// `Retry-After` to the kinds that are worth retrying after a short pause.
pub fn error_response(error: &GatewayError, request_id: &RequestId) -> Response {
    let status = StatusCode::from_u16(error.kind().http_status())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response =
        json_response_with_status(status, &ErrorEnvelope::new(error, request_id.as_str()))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    response.extensions_mut().insert(ShapedResponse);
    if matches!(
        error.kind(),
        crate::error::ErrorKind::Unavailable | crate::error::ErrorKind::ResourceExhausted
    ) {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

/// Serializes `value` as a 200 JSON response. Fails only when serialization fails, which is reported as internal.
pub fn json_response<T: Serialize>(value: &T) -> Result<Response, GatewayError> {
    json_response_with_status(StatusCode::OK, value)
}

/// Serializes `value` as a JSON response with the given status. Serialization failures are reported as internal.
pub(crate) fn json_response_with_status<T: Serialize>(
    status: StatusCode,
    value: &T,
) -> Result<Response, GatewayError> {
    let body = serde_json::to_vec(value).map_err(|error| {
        GatewayError::internal(format!("failed to serialize JSON response: {error}"))
    })?;
    let mut response = (status, Body::from(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

/// Deserializes a JSON request body, requiring a JSON `Content-Type`.
pub fn parse_json_body<T: DeserializeOwned>(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<T, GatewayError> {
    validate_json_content_type(headers)?;
    serde_json::from_slice(body)
        .map_err(|error| GatewayError::invalid_argument(format!("invalid JSON body: {error}")))
}

pub(crate) fn validate_json_content_type(headers: &HeaderMap) -> Result<(), GatewayError> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Err(GatewayError::new(
            crate::error::ErrorKind::UnsupportedMediaType,
            "Content-Type must be application/json or application/*+json",
        ));
    };
    let media_type = value
        .to_str()
        .map_err(|_| GatewayError::invalid_argument("unreadable Content-Type header"))?
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let supported = media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"));
    if supported {
        Ok(())
    } else {
        Err(GatewayError::new(
            crate::error::ErrorKind::UnsupportedMediaType,
            "Content-Type must be application/json or application/*+json",
        ))
    }
}

/// Rejects any query string on endpoints that define no query parameters.
#[allow(dead_code)] // Consumed by the endpoint modules as they are implemented.
pub(crate) fn ensure_no_query(uri: &Uri) -> Result<(), GatewayError> {
    if uri.query().is_some() {
        return Err(GatewayError::invalid_argument(
            "this operation does not accept query parameters",
        ));
    }
    Ok(())
}

/// Deserializes the URI query string. Unknown or malformed parameters are rejected as invalid arguments.
pub fn parse_query<T: DeserializeOwned>(uri: &Uri) -> Result<T, GatewayError> {
    serde_urlencoded::from_str(uri.query().unwrap_or_default())
        .map_err(|error| GatewayError::invalid_argument(format!("invalid query: {error}")))
}

/// Marks a response as final so the error-normalising middleware does not rewrite its body. Use it for handler
/// responses that already carry their own envelope.
pub fn shaped(mut response: Response) -> Response {
    response.extensions_mut().insert(ShapedResponse);
    response
}

/// Assembles the full REST application: routes, generated OpenAPI document, fallback, and middleware.
///
/// Every route module is merged here on day one. The OpenAPI document is derived from the merged routers by
/// [`utoipa_axum::router::OpenApiRouter::split_for_parts`], so there is no hand-maintained path or schema
/// registry that could drift from the code.
pub fn build_router(state: RestState, options: &RestOptions) -> Router {
    let (data_router, data_api) = OpenApiRouter::new()
        .merge(metadata::routes())
        .merge(ddl::routes())
        .merge(lookup::routes())
        .merge(records::routes())
        .split_for_parts();
    let (secured_control_router, secured_control_api) = OpenApiRouter::new()
        .merge(clusters::routes())
        .merge(openapi::routes())
        .split_for_parts();
    let (open_router, open_api) = OpenApiRouter::new()
        .merge(health::routes())
        .split_for_parts();

    let mut api = data_api;
    api.merge(secured_control_api);
    api.merge(open_api);
    let _ = state.openapi.set(openapi::finalize(api));

    let data = data_router.with_state(state.clone());
    let data = apply_data_limits(data, options);
    let data = apply_acceptance_guard(data, state.readiness.clone());
    let secured_control =
        secured_control_router
            .with_state(state.clone())
            .layer(middleware::from_fn(assign_request_deadline(
                options.request_timeout,
            )));
    // Everything except the health probes requires authentication. The 404 fallback stays outside
    // the guard so an unknown path answers 404 rather than 401; the health router carries it.
    let secured = data
        .merge(secured_control)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_authentication,
        ));
    let open = open_router
        .fallback(unknown_route)
        .with_state(state)
        .layer(middleware::from_fn(assign_request_deadline(
            options.request_timeout,
        )));
    apply_common_middleware(open.merge(secured))
}

/// Rejects new data and metadata work after graceful draining starts while keeping health endpoints available.
fn apply_acceptance_guard(router: Router, readiness: Arc<Readiness>) -> Router {
    router.layer(middleware::from_fn(move |request: Request, next: Next| {
        let readiness = readiness.clone();
        async move {
            if let Err(error) = readiness.ensure_accepting() {
                let request_id = request
                    .extensions()
                    .get::<RequestId>()
                    .cloned()
                    .unwrap_or_default();
                return error_response(&error, &request_id);
            }
            next.run(request).await
        }
    }))
}

/// Applies the cross-cutting middleware stack to an already-routed app.
///
/// Exposed separately so tests can wrap purpose-built routers with the production middleware.
/// The body-limit layer is a streaming-body backstop. Requests with a declared length are rejected earlier with an
/// envelope.
///
/// Order (outermost first): request-id assignment and error normalisation, then the body size and deadline
/// limits, then access logging.
pub fn apply_middleware(router: Router, options: &RestOptions) -> Router {
    apply_common_middleware(apply_data_limits(router, options))
}

/// Records the absolute deadline of a request that does not pass through the data-limit layer.
fn assign_request_deadline(
    request_timeout: Duration,
) -> impl Fn(Request, Next) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> + Clone {
    move |mut request: Request, next: Next| {
        Box::pin(async move {
            request
                .extensions_mut()
                .insert(RequestDeadline(Instant::now() + request_timeout));
            next.run(request).await
        })
    }
}

fn apply_data_limits(router: Router, options: &RestOptions) -> Router {
    let request_timeout = options.request_timeout;
    let max_body_bytes = options.max_body_bytes;

    let limits = move |mut request: Request, next: Next| async move {
        let request_id = request
            .extensions()
            .get::<RequestId>()
            .cloned()
            .unwrap_or_default();
        request
            .extensions_mut()
            .insert(RequestDeadline(Instant::now() + request_timeout));

        let oversized = declared_content_length(&request).filter(|length| *length > max_body_bytes);
        if let Some(length) = oversized {
            observability::http_rejection("body_size");
            log::warn!(
                "request_id={} rejecting body of {} bytes above {} bytes",
                request_id.as_str(),
                length,
                max_body_bytes
            );
            return error_response(
                &GatewayError::limit_exceeded(format!(
                    "request body of {length} bytes exceeds the limit of {max_body_bytes} bytes"
                )),
                &request_id,
            );
        }

        observability::http_inflight(1);
        let result = tokio::time::timeout(request_timeout, next.run(request)).await;
        observability::http_inflight(-1);
        match result {
            Ok(response) => response,
            Err(_) => {
                observability::http_rejection("timeout");
                log::warn!(
                    "request_id={} deadline exceeded after {:?}",
                    request_id.as_str(),
                    request_timeout
                );
                error_response(
                    &GatewayError::deadline_exceeded("request deadline exceeded"),
                    &request_id,
                )
            }
        }
    };

    router
        .layer(DefaultBodyLimit::max(
            usize::try_from(max_body_bytes).unwrap_or(usize::MAX),
        ))
        .layer(middleware::from_fn(limits))
}

fn apply_common_middleware(router: Router) -> Router {
    router
        .layer(middleware::from_fn(request_log))
        .layer(middleware::from_fn(request_context))
}

async fn request_log(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("<unmatched>")
        .to_string();
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .cloned()
        .unwrap_or_default();
    let response = next.run(request).await;
    let elapsed = started.elapsed();
    let status = response.status();
    observability::http_request(method.as_str(), &route, status.as_u16(), elapsed);
    log::info!(
        "{}",
        format_request_log(&method, &route, &request_id, status, elapsed.as_millis())
    );
    response
}

fn format_request_log(
    method: &Method,
    route: &str,
    request_id: &RequestId,
    status: StatusCode,
    elapsed_ms: u128,
) -> String {
    format!(
        "method={method} route={route} request_id={} status={} elapsed_ms={elapsed_ms}",
        request_id.as_str(),
        status.as_u16()
    )
}

async fn request_context(mut request: Request, next: Next) -> Response {
    let request_id = RequestId(Arc::from(uuid::Uuid::new_v4().to_string()));
    request.extensions_mut().insert(request_id.clone());

    let response = next.run(request).await;
    let mut response = normalize_error(response, &request_id);

    if let Ok(value) = HeaderValue::from_str(request_id.as_str()) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

fn normalize_error(response: Response, request_id: &RequestId) -> Response {
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }
    if response.extensions().get::<ShapedResponse>().is_some() {
        return response;
    }

    let (status, code, message, retryable) = match status.as_u16() {
        400 | 422 => (400, "invalid_argument", "invalid request", false),
        404 => (404, "not_found", "resource not found", false),
        405 => (405, "method_not_allowed", "method not allowed", false),
        406 => (406, "not_acceptable", "unacceptable accept header", false),
        408 | 504 => (504, "timeout", "request deadline exceeded", true),
        413 => (413, "limit_exceeded", "request body too large", false),
        415 => (
            415,
            "unsupported_media_type",
            "unsupported media type",
            false,
        ),
        501 => (501, "unsupported", "unsupported operation", false),
        503 => (503, "unavailable", "service unavailable", true),
        other => (
            other,
            if other >= 500 {
                "internal"
            } else {
                "invalid_argument"
            },
            "request failed",
            false,
        ),
    };

    let envelope = ErrorEnvelope::from_parts(code, message, request_id.as_str(), retryable);
    json_response_with_status(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &envelope,
    )
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn unknown_route(method: Method, uri: Uri, request: Request) -> Response {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .cloned()
        .unwrap_or_default();
    error_response(
        &GatewayError::not_found(format!("no route for {method} {}", uri.path())),
        &request_id,
    )
}

fn declared_content_length(request: &Request) -> Option<u64> {
    request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

#[cfg(any(test, feature = "test-backend"))]
pub mod test_support {
    //! Router and state builders shared by in-crate protocol tests and the out-of-crate HTTP suites.

    use super::*;
    use crate::backend::model::{ClusterHealthReport, ClusterStatus};
    use crate::backend::testing::TestBackend;

    /// Middleware options with short, test-friendly bounds.
    pub fn test_options() -> RestOptions {
        RestOptions {
            request_timeout: Duration::from_secs(5),
            max_body_bytes: 1024,
        }
    }

    /// A green cluster health report for a reachable fixture cluster.
    pub fn green_report() -> ClusterHealthReport {
        ClusterHealthReport {
            status: ClusterStatus::Green,
            num_replicas: 6,
            in_sync_replicas: 6,
            num_leader_replicas: 3,
            active_leader_replicas: 3,
        }
    }

    /// Builds handler state over a single connected fixture cluster named `default`.
    pub fn state_with_backend(backend: Arc<TestBackend>) -> RestState {
        let clusters = Arc::new(ClusterRegistry::single_for_test(
            "default",
            backend,
            green_report(),
        ));
        state_with_clusters(clusters)
    }

    /// Builds handler state over an arbitrary cluster registry.
    pub fn state_with_clusters(clusters: Arc<ClusterRegistry>) -> RestState {
        let readiness = Arc::new(Readiness::new());
        readiness.set_serving();
        RestState {
            clusters,
            write_delivery_time: Duration::from_secs(20),
            readiness,
            bind_address: "127.0.0.1:8080".parse().expect("valid"),
            started_at: Instant::now(),
            lookup_limits: LookupLimits {
                max_keys: 8,
                max_key_bytes: 256,
                max_prefixes: 4,
                max_rows_per_prefix: 100,
            },
            metadata_limits: MetadataLimits {
                default_page_size: 100,
                max_page_size: 1000,
            },
            write_limits: WriteLimits { max_rows: 8 },
            openapi: Arc::new(OnceLock::new()),
            authenticator: Arc::new(crate::auth::TrustAuthenticator::new()),
        }
    }

    /// Builds the production router over a fixture backend.
    pub fn app(backend: Arc<TestBackend>) -> Router {
        build_router(state_with_backend(backend), &test_options())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::TestBackend;
    use axum::routing::get;
    use http_body_util::BodyExt;
    use serde::Deserialize;
    use tower::ServiceExt;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct BodyFixture {
        value: u32,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct QueryFixture {
        spec: String,
        bucket: i32,
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    #[test]
    fn dropping_rest_context_cancels_application_work() {
        let context = application_context(
            &RequestId::default(),
            RequestDeadline(Instant::now() + Duration::from_secs(1)),
            &Principal::new("tester"),
            "default",
        )
        .unwrap();
        let cancellation = context.cancellation().clone();

        drop(context);

        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn shared_json_parser_enforces_media_type_and_serde_strictness() {
        let body = Bytes::from_static(br#"{"value": 7}"#);
        assert!(parse_json_body::<BodyFixture>(&HeaderMap::new(), &body).is_err());

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(parse_json_body::<BodyFixture>(&headers, &body).is_err());

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.fluss+json; charset=utf-8"),
        );
        assert_eq!(
            parse_json_body::<BodyFixture>(&headers, &body).unwrap(),
            BodyFixture { value: 7 }
        );
        for bad in [
            br#"{"value": 7, "unknown": true}"#.as_slice(),
            br#"{"value": 7, "value": 8}"#.as_slice(),
            br#"{"value":"#.as_slice(),
        ] {
            assert!(
                parse_json_body::<BodyFixture>(&headers, &Bytes::copy_from_slice(bad)).is_err()
            );
        }
        assert!(parse_json_body::<BodyFixture>(&headers, &Bytes::new()).is_err());
    }

    #[test]
    fn shared_query_parser_decodes_and_rejects_duplicates() {
        let uri: Uri = "/?spec=hello%20world&bucket=7".parse().unwrap();
        assert_eq!(
            parse_query::<QueryFixture>(&uri).unwrap(),
            QueryFixture {
                spec: "hello world".to_string(),
                bucket: 7,
            }
        );
        let repeated: Uri = "/?spec=a&spec=b&bucket=7".parse().unwrap();
        assert!(parse_query::<QueryFixture>(&repeated).is_err());
        let unknown: Uri = "/?spec=a&bucket=7&extra=1".parse().unwrap();
        assert!(parse_query::<QueryFixture>(&unknown).is_err());
    }

    /// Under the user identity mode each authenticated Basic identity resolves its own act-as
    /// connection: the injected connector observes one dial per distinct principal, and repeated
    /// requests for a principal reuse the pooled backend.
    #[tokio::test]
    async fn user_identity_mode_dials_one_connection_per_principal() {
        use crate::backend::identity::IdentityConnector;
        use crate::backend::registry::ClusterRegistry;
        use base64::Engine;

        let dialed: Arc<parking_lot::Mutex<Vec<String>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));
        let connector: IdentityConnector = {
            let dialed = dialed.clone();
            Arc::new(move |user: String| {
                let dialed = dialed.clone();
                Box::pin(async move {
                    dialed.lock().push(user);
                    Ok(Arc::new(TestBackend::new()) as Arc<dyn crate::backend::GatewayBackend>)
                })
            })
        };
        let clusters = Arc::new(ClusterRegistry::single_for_test_with_identity_pool(
            "default",
            Arc::new(TestBackend::new()),
            test_support::green_report(),
            connector,
            8,
            Duration::from_secs(3600),
        ));
        let mut state = test_support::state_with_clusters(clusters);
        state.authenticator = Arc::new(crate::auth::ConfigUserStoreAuthenticator::new(
            crate::auth::parse_user_table("alice:pw,bob:pw").expect("user table"),
        ));
        let app = build_router(state, &test_support::test_options());

        for (user, times) in [("alice", 2), ("bob", 1)] {
            for _ in 0..times {
                let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:pw"));
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri("/v1/clusters/default/databases")
                            .header("authorization", format!("Basic {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK, "{user}");
            }
        }
        assert_eq!(
            *dialed.lock(),
            vec!["alice".to_string(), "bob".to_string()],
            "one dial per principal, reused across requests"
        );
    }

    #[tokio::test]
    async fn unknown_route_yields_404_envelope() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = app
            .oneshot(Request::builder().uri("/nope").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let header_id = response
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .expect("x-request-id header");

        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "not_found");
        assert_eq!(json["error"]["request_id"], header_id.as_str());
        assert_eq!(json["error"]["retryable"], false);
        assert!(
            json["error"]["message"].as_str().unwrap().contains("/nope"),
            "message names the missing route: {json}"
        );
    }

    #[tokio::test]
    async fn oversized_body_yields_413_envelope() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/clusters/default/databases/db/tables/t/records/lookup")
                    .header(header::CONTENT_LENGTH, "1048576")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "limit_exceeded");
        assert!(json["error"]["request_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn request_timeout_yields_504_envelope() {
        /// Runs longer than the configured test request deadline.
        async fn slow() -> &'static str {
            tokio::time::sleep(Duration::from_millis(250)).await;
            "ok"
        }
        let options = RestOptions {
            request_timeout: Duration::from_millis(50),
            max_body_bytes: 1024,
        };
        let app = apply_middleware(Router::new().route("/slow", get(slow)), &options);

        let response = app
            .oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "timeout");
        assert_eq!(json["error"]["retryable"], true);
    }

    #[tokio::test]
    async fn concurrent_requests_are_never_rejected_with_429() {
        /// Holds a request open long enough for several to overlap.
        async fn slow() -> &'static str {
            tokio::time::sleep(Duration::from_millis(50)).await;
            "ok"
        }
        let app = apply_middleware(
            Router::new().route("/slow", get(slow)),
            &test_support::test_options(),
        );
        let request = || Request::builder().uri("/slow").body(Body::empty()).unwrap();

        let responses =
            futures::future::join_all((0..16).map(|_| app.clone().oneshot(request()))).await;

        for response in responses {
            assert_eq!(response.unwrap().status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn success_responses_carry_request_id_header() {
        let app = test_support::app(Arc::new(TestBackend::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));
    }

    #[test]
    fn request_log_contains_protocol_context() {
        let message = format_request_log(
            &Method::POST,
            "/v1/clusters/{cluster}/databases",
            &RequestId(Arc::from("request-7")),
            StatusCode::CREATED,
            23,
        );
        assert_eq!(
            message,
            "method=POST route=/v1/clusters/{cluster}/databases request_id=request-7 status=201 elapsed_ms=23"
        );
    }
}
