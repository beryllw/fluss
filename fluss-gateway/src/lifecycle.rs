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

//! Process lifecycle for listeners and graceful shutdown.
//!
//! Listener binding and process readiness are independent from Fluss availability.
//!
//! Shutdown drains in-flight requests. Because the gateway holds no request-spanning
//! state, there is nothing to hand over, flush, or migrate: a terminated instance leaves no work that another
//! instance would have to pick up.

use crate::config::GatewayConfig;
use crate::error::GatewayError;
use crate::observability;
use crate::protocol::rest::{self, RestOptions, RestState};
use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::FutureExt;
use metrics_exporter_prometheus::PrometheusHandle;
use std::any::Any;
use std::future::Future;
use std::future::IntoFuture;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

type RunError = Box<dyn std::error::Error + Send + Sync>;

const MAX_SHUTDOWN_CLEANUP_RESERVE: Duration = Duration::from_secs(5);

/// Named terminal result from one process-owned asynchronous subsystem.
struct TaskExit {
    name: String,
    result: Result<(), String>,
}

/// The shared process acceptance predicate.
#[derive(Debug, Default)]
pub struct Readiness {
    serving: AtomicBool,
    shutting_down: AtomicBool,
}

impl Readiness {
    /// Starts neither serving nor shutting down, so new work is rejected until startup completes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the gateway ready to serve. Called once after the listeners are bound.
    pub fn set_serving(&self) {
        self.serving.store(true, Ordering::SeqCst);
    }

    /// Flips acceptance off so guarded routes answer 503 and callers stop sending traffic, before
    /// draining starts.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// True once startup finished, regardless of whether shutdown has begun.
    pub fn is_serving(&self) -> bool {
        self.serving.load(Ordering::SeqCst)
    }

    /// True once shutdown started. Never returns to false.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// The predicate that gates request acceptance: serving and not yet draining.
    pub fn is_accepting(&self) -> bool {
        self.is_serving() && !self.is_shutting_down()
    }

    /// Rejects new application work once startup has not completed or draining has begun.
    pub fn ensure_accepting(&self) -> Result<(), GatewayError> {
        if self.is_shutting_down() {
            return Err(GatewayError::unavailable("gateway is shutting down"));
        }
        if !self.is_serving() {
            return Err(GatewayError::unavailable("gateway is starting"));
        }
        Ok(())
    }
}

/// A gateway whose configured listeners are bound and serving.
pub struct RunningGateway {
    local_addr: std::net::SocketAddr,
    metrics_addr: Option<std::net::SocketAddr>,
    readiness: Arc<Readiness>,
    drain_timeout: Duration,
    shutdown: CancellationToken,
    tasks: JoinSet<TaskExit>,
}

impl RunningGateway {
    /// The bound REST address, resolved after binding so a configured port of 0 reads back as the real port.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    /// The bound metrics address, or `None` when the metrics listener is disabled.
    pub fn metrics_addr(&self) -> Option<std::net::SocketAddr> {
        self.metrics_addr
    }

    /// Begins graceful draining, the same transition SIGTERM triggers.
    pub fn begin_shutdown(&self) {
        self.readiness.begin_shutdown();
    }

    /// Stops accepting, drains in-flight requests within the configured drain timeout, then closes the
    /// background tasks. Consumes the gateway.
    pub async fn shutdown(self) -> Result<(), RunError> {
        self.finish(None).await
    }

    async fn finish(mut self, unexpected_exit: Option<String>) -> Result<(), RunError> {
        let shutdown_started = Instant::now();
        self.readiness.begin_shutdown();
        observability::process_draining();
        let (task_deadline, _deadline) = shutdown_deadlines(Instant::now(), self.drain_timeout);
        self.shutdown.cancel();
        let cleanup_error = drain_tasks(&mut self.tasks, task_deadline).await;

        if let Some(error) = unexpected_exit {
            observability::process_stopped("task_error", shutdown_started.elapsed());
            return Err(error.into());
        }
        if let Some(error) = cleanup_error {
            observability::process_stopped("cleanup_error", shutdown_started.elapsed());
            return Err(error.into());
        }
        observability::process_stopped("success", shutdown_started.elapsed());
        log::info!("fluss-gateway stopped");
        Ok(())
    }
}

/// Splits one process deadline into request draining and a bounded resource-cleanup tail.
fn shutdown_deadlines(started: Instant, timeout: Duration) -> (Instant, Instant) {
    // Configuration rejects overflowing durations, so this is defence in depth: an instant that
    // cannot represent `started + timeout` falls back to a one-hour drain rather than panicking.
    let deadline = started
        .checked_add(timeout)
        .unwrap_or_else(|| started + Duration::from_secs(3600));
    let minimum_reserve = Duration::from_millis(1).min(timeout);
    let cleanup_reserve = (timeout / 4)
        .max(minimum_reserve)
        .min(MAX_SHUTDOWN_CLEANUP_RESERVE);
    (deadline - cleanup_reserve, deadline)
}

/// Runs the gateway until a process shutdown signal or any process-owned task exits unexpectedly.
pub async fn run(config: GatewayConfig) -> Result<(), RunError> {
    let mut gateway = start(config).await?;
    let unexpected_exit = tokio::select! {
        biased;
        result = gateway.tasks.join_next() => {
            Some(unexpected_task_detail(result))
        }
        _ = shutdown_signal() => {
            log::info!("shutdown signal received");
            None
        }
    };
    gateway.finish(unexpected_exit).await
}

/// Binds listeners and starts serving without requiring Fluss to be available.
pub async fn start(config: GatewayConfig) -> Result<RunningGateway, RunError> {
    start_internal(config).await
}

/// Binds the listeners, installs the router, and spawns every process-owned task.
async fn start_internal(config: GatewayConfig) -> Result<RunningGateway, RunError> {
    for warning in config.warnings() {
        log::warn!("{warning}");
    }
    observability::init_metrics(config.server.metrics.enabled)?;
    observability::register_process_metrics();

    let listener = bind_listener(config.server.rest.bind_address, "REST").await?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read the bound REST listener address: {error}"))?;

    let metrics_listener = if config.server.metrics.enabled {
        Some(bind_listener(config.server.metrics.bind_address, "metrics").await?)
    } else {
        None
    };
    let metrics_addr = metrics_listener
        .as_ref()
        .map(tokio::net::TcpListener::local_addr)
        .transpose()
        .map_err(|error| format!("failed to read the bound metrics listener address: {error}"))?;

    let readiness = Arc::new(Readiness::new());
    let state = rest_state(&config, &readiness, local_addr);
    let router = rest::build_router(state, &RestOptions::from(&config.server.rest));
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();
    spawn_named(
        &mut tasks,
        "REST listener",
        serve(listener, router, shutdown.clone()),
    );
    if let Some(listener) = metrics_listener {
        let handle = observability::metrics_handle();
        spawn_named(
            &mut tasks,
            "metrics listener",
            serve(listener, metrics_router(handle), shutdown.clone()),
        );
        // Samples the FIP-49 process_* and tokio_* gauges alongside the exporter they feed.
        let sampler_shutdown = shutdown.clone();
        spawn_named(&mut tasks, "runtime metrics sampler", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    () = sampler_shutdown.cancelled() => return Ok(()),
                    _ = interval.tick() => observability::sample_runtime_metrics(),
                }
            }
        });
    }

    readiness.set_serving();
    observability::process_ready();
    log::info!("fluss-gateway REST listener serving at {local_addr}");
    if let Some(address) = metrics_addr {
        log::info!("fluss-gateway metrics listener serving at {address}");
    }
    Ok(RunningGateway {
        local_addr,
        metrics_addr,
        readiness,
        drain_timeout: config.shutdown.drain_timeout.get(),
        shutdown,
        tasks,
    })
}

/// Builds shared handler state from validated configuration and process services.
pub fn rest_state(
    _config: &GatewayConfig,
    readiness: &Arc<Readiness>,
    bind_address: std::net::SocketAddr,
) -> RestState {
    RestState {
        readiness: readiness.clone(),
        bind_address,
        started_at: Instant::now(),
        openapi: Arc::new(OnceLock::new()),
    }
}

/// Binds one configured HTTP listener and adds a contextual startup error.
async fn bind_listener(
    bind_address: std::net::SocketAddr,
    name: &str,
) -> Result<tokio::net::TcpListener, RunError> {
    tokio::net::TcpListener::bind(bind_address)
        .await
        .map_err(|error| {
            format!("failed to bind {name} listener on {bind_address}: {error}").into()
        })
}

/// Serves one Axum listener until process cancellation starts graceful drain.
async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: CancellationToken,
) -> Result<(), String> {
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        shutdown.cancelled().await;
    });
    server
        .into_future()
        .await
        .map_err(|error| error.to_string())
}

/// Builds the isolated Prometheus scrape router.
fn metrics_router(handle: Option<PrometheusHandle>) -> Router {
    Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { metrics_response(handle.as_ref()) }
        }),
    )
}

/// Renders the current Prometheus exposition without API middleware or labels from user input.
fn metrics_response(handle: Option<&PrometheusHandle>) -> Response {
    match handle {
        Some(handle) => {
            let mut response = (StatusCode::OK, handle.render()).into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            );
            response
        }
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// Registers one named process task and converts a panic into a normal named failure.
fn spawn_named<F, N>(tasks: &mut JoinSet<TaskExit>, name: N, future: F)
where
    F: Future<Output = Result<(), String>> + Send + 'static,
    N: Into<String>,
{
    let name = name.into();
    tasks.spawn(async move {
        let result = match AssertUnwindSafe(future).catch_unwind().await {
            Ok(result) => result,
            Err(payload) => Err(format!("task panicked: {}", panic_message(payload))),
        };
        TaskExit { name, result }
    });
}

/// Converts a process task ending before shutdown into an operator-facing failure.
fn unexpected_task_detail(result: Option<Result<TaskExit, tokio::task::JoinError>>) -> String {
    match result {
        Some(Ok(TaskExit {
            name,
            result: Ok(()),
        })) => format!("{name} exited unexpectedly"),
        Some(Ok(TaskExit {
            name,
            result: Err(error),
        })) => format!("{name} failed: {error}"),
        Some(Err(error)) => format!("gateway task failed: {error}"),
        None => "all gateway tasks exited unexpectedly".to_string(),
    }
}

/// Waits for every process task under one absolute deadline, then aborts and joins any stragglers.
async fn drain_tasks(tasks: &mut JoinSet<TaskExit>, deadline: Instant) -> Option<String> {
    let mut cleanup_error = None;
    loop {
        match tokio::time::timeout_at(deadline.into(), tasks.join_next()).await {
            Ok(Some(Ok(TaskExit { name, result }))) => match result {
                Ok(()) => log::info!("{name} stopped"),
                Err(error) => {
                    log::warn!("{name} failed while draining: {error}");
                    cleanup_error = Some(format!("{name} failed while draining"));
                }
            },
            Ok(Some(Err(error))) => {
                log::warn!("gateway task failed while draining: {error}");
                cleanup_error = Some("gateway task failed while draining".to_string());
            }
            Ok(None) => return cleanup_error,
            Err(_) => {
                let remaining = tasks.len();
                log::warn!("{remaining} gateway task(s) did not stop before the drain deadline");
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Some(format!(
                    "{remaining} gateway task(s) exceeded the process drain deadline"
                ));
            }
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

/// Resolves when SIGTERM on Unix or Ctrl-C is received.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct DropGuard(Arc<AtomicUsize>);

    impl Drop for DropGuard {
        /// Records that a task-owned guard was dropped.
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Verifies readiness transitions and idempotent shutdown state.
    #[test]
    fn readiness_predicate() {
        let readiness = Readiness::new();
        assert!(!readiness.is_accepting());
        readiness.set_serving();
        assert!(readiness.is_accepting());
        readiness.begin_shutdown();
        assert!(!readiness.is_accepting());
        assert_eq!(
            readiness.ensure_accepting().unwrap_err().message(),
            "gateway is shutting down"
        );
        readiness.begin_shutdown();
        assert!(readiness.is_shutting_down());
    }

    #[test]
    fn readiness_rejects_work_before_startup() {
        let readiness = Readiness::new();
        let error = readiness.ensure_accepting().unwrap_err();
        assert_eq!(error.kind(), crate::error::ErrorKind::Unavailable);
        assert_eq!(error.message(), "gateway is starting");

        readiness.set_serving();
        readiness.ensure_accepting().unwrap();
    }

    /// Timed-out tasks are aborted and joined before cleanup returns.
    #[tokio::test(start_paused = true)]
    async fn timed_out_tasks_are_aborted_and_joined_at_the_absolute_deadline() {
        let task_drops = Arc::new(AtomicUsize::new(0));
        let background_drops = task_drops.clone();
        let mut tasks = JoinSet::new();
        spawn_named(&mut tasks, "stuck task", async move {
            let _guard = DropGuard(background_drops);
            std::future::pending::<Result<(), String>>().await
        });
        tokio::task::yield_now().await;
        let started = tokio::time::Instant::now();
        let deadline = Instant::now() + Duration::from_secs(5);
        let error = drain_tasks(&mut tasks, deadline)
            .await
            .expect("a stuck task exceeds the deadline");

        let elapsed = tokio::time::Instant::now().duration_since(started);
        assert!(elapsed >= Duration::from_secs(5), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(5_010), "{elapsed:?}");
        assert!(error.contains("1 gateway task(s)"), "{error}");
        assert_eq!(task_drops.load(Ordering::SeqCst), 1);
        assert!(tasks.is_empty());
    }

    /// A stuck request cannot consume the tail reserved for resource cleanup.
    #[tokio::test]
    async fn stuck_task_leaves_time_for_resource_cleanup() {
        let mut tasks = JoinSet::new();
        spawn_named(&mut tasks, "stuck listener", async move {
            std::future::pending::<Result<(), String>>().await
        });
        tokio::task::yield_now().await;
        let started = Instant::now();
        let (task_deadline, deadline) = shutdown_deadlines(started, Duration::from_millis(200));

        let task_error = drain_tasks(&mut tasks, task_deadline).await;
        assert!(task_error.is_some());
        assert!(task_deadline < deadline);

        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(140), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(500), "{elapsed:?}");
    }

    #[tokio::test]
    async fn named_task_panic_is_reported_as_an_unexpected_process_failure() {
        let mut tasks = JoinSet::new();
        spawn_named(&mut tasks, "REST listener", async move {
            panic!("listener invariant failed");
            #[allow(unreachable_code)]
            Ok(())
        });

        let detail = unexpected_task_detail(tasks.join_next().await);
        assert!(detail.contains("REST listener"), "{detail}");
        assert!(detail.contains("listener invariant failed"), "{detail}");
    }

    #[tokio::test]
    async fn normal_task_exit_is_reported_as_an_unexpected_process_failure() {
        let mut tasks = JoinSet::new();
        spawn_named(&mut tasks, "REST listener", async move { Ok(()) });

        let detail = unexpected_task_detail(tasks.join_next().await);

        assert_eq!(detail, "REST listener exited unexpectedly");
    }

    #[tokio::test]
    async fn process_cancellation_drains_all_named_tasks_cleanly() {
        let shutdown = CancellationToken::new();
        let mut tasks = JoinSet::new();
        for name in ["REST listener", "metrics listener"] {
            let task_shutdown = shutdown.clone();
            spawn_named(&mut tasks, name, async move {
                task_shutdown.cancelled().await;
                Ok(())
            });
        }

        shutdown.cancel();
        let error = drain_tasks(&mut tasks, Instant::now() + Duration::from_secs(1)).await;

        assert!(error.is_none(), "{error:?}");
        assert!(tasks.is_empty());
    }
}
