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
use crate::error::{GatewayError, panic_message};
use crate::observability;
use crate::protocol::rest;
use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::FutureExt;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use metrics_exporter_prometheus::PrometheusHandle;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

type RunError = Box<dyn std::error::Error + Send + Sync>;

/// Named terminal result from one process-owned asynchronous subsystem.
struct TaskExit {
    name: String,
    result: Result<(), String>,
}

/// Monotonic process states; discriminants define their transition order.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
enum LifecycleState {
    Starting = 0,
    Serving = 1,
    Quiescing = 2,
    Draining = 3,
    Stopped = 4,
}

impl LifecycleState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Starting,
            1 => Self::Serving,
            2 => Self::Quiescing,
            3 => Self::Draining,
            4 => Self::Stopped,
            _ => unreachable!("invalid lifecycle state"),
        }
    }
}

/// The shared process acceptance state.
#[derive(Debug)]
pub struct Readiness {
    state: AtomicU8,
}

impl Default for Readiness {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(LifecycleState::Starting as u8),
        }
    }
}

impl Readiness {
    /// Starts in the non-accepting startup state.
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn set_serving(&self) {
        self.transition(LifecycleState::Starting, LifecycleState::Serving);
    }

    pub(crate) fn begin_quiescing(&self) {
        self.transition(LifecycleState::Serving, LifecycleState::Quiescing);
    }

    pub(crate) fn begin_draining(&self) {
        self.transition(LifecycleState::Quiescing, LifecycleState::Draining);
    }

    pub(crate) fn set_stopped(&self) {
        self.transition(LifecycleState::Draining, LifecycleState::Stopped);
    }

    /// True once startup completed, including while shutting down.
    pub fn has_started(&self) -> bool {
        self.state() >= LifecycleState::Serving
    }

    /// True once shutdown started.
    pub fn is_shutting_down(&self) -> bool {
        self.state() >= LifecycleState::Quiescing
    }

    /// True only while the gateway accepts application work.
    pub fn is_accepting(&self) -> bool {
        self.state() == LifecycleState::Serving
    }

    /// Rejects new application work before startup completes or after shutdown begins.
    pub fn ensure_accepting(&self) -> Result<(), GatewayError> {
        match self.state() {
            LifecycleState::Starting => Err(GatewayError::unavailable("gateway is starting")),
            LifecycleState::Serving => Ok(()),
            LifecycleState::Quiescing | LifecycleState::Draining | LifecycleState::Stopped => {
                Err(GatewayError::unavailable("gateway is shutting down"))
            }
        }
    }

    fn state(&self) -> LifecycleState {
        LifecycleState::from_u8(self.state.load(Ordering::SeqCst))
    }

    fn transition(&self, current: LifecycleState, next: LifecycleState) {
        assert_eq!(next as u8, current as u8 + 1);
        match self.state.compare_exchange(
            current as u8,
            next as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {}
            Err(actual) if actual >= next as u8 => {}
            Err(actual) => panic!(
                "invalid lifecycle transition from {:?} to {next:?}",
                LifecycleState::from_u8(actual)
            ),
        }
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

    /// Stops accepting application work without closing the listeners.
    pub fn begin_shutdown(&self) {
        self.readiness.begin_quiescing();
    }

    /// Stops accepting, drains in-flight requests within the configured drain timeout, then closes the
    /// background tasks. Consumes the gateway.
    pub async fn shutdown(self) -> Result<(), RunError> {
        self.finish(None).await
    }

    async fn finish(mut self, unexpected_exit: Option<String>) -> Result<(), RunError> {
        let shutdown_started = Instant::now();
        self.readiness.begin_quiescing();
        // Draining gets the whole configured budget: the gateway holds no request-spanning state, so there is
        // no cleanup step after the tasks stop that would need a reserved tail. The deadline comes from the
        // timer's own clock, so it cannot skew against it.
        let deadline = tokio::time::Instant::now() + self.drain_timeout;
        self.readiness.begin_draining();
        self.shutdown.cancel();
        let cleanup_error = drain_tasks(&mut self.tasks, deadline).await;
        self.readiness.set_stopped();

        // The metrics listener is one of the drained tasks, so nothing recorded from here on could ever be
        // scraped: the shutdown outcome is reported through this log line and the process exit code.
        let elapsed = shutdown_started.elapsed();
        if let Some(error) = unexpected_exit {
            log::error!("fluss-gateway stopped after {elapsed:?}: {error}");
            return Err(error.into());
        }
        if let Some(error) = cleanup_error {
            log::error!("fluss-gateway stopped after {elapsed:?}: {error}");
            return Err(error.into());
        }
        log::info!("fluss-gateway stopped after {elapsed:?}");
        Ok(())
    }
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

/// Validates the complete configuration, then binds listeners and starts serving without requiring Fluss to be
/// available.
pub async fn start(config: GatewayConfig) -> Result<RunningGateway, RunError> {
    config.validate()?;
    start_internal(config).await
}

/// Binds the listeners, installs the router, and spawns every process-owned task.
async fn start_internal(config: GatewayConfig) -> Result<RunningGateway, RunError> {
    for warning in config.warnings() {
        log::warn!("{warning}");
    }
    observability::init_metrics(config.server.metrics.enabled)?;

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
    let router = rest::build(&config.server.rest, &readiness, local_addr);
    let header_read_timeout = config.server.rest.header_read_timeout.get();
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();
    spawn_named(
        &mut tasks,
        "REST listener",
        serve(listener, router, header_read_timeout, shutdown.clone()),
    );
    if let Some(listener) = metrics_listener {
        let handle = observability::metrics_handle();
        spawn_named(
            &mut tasks,
            "metrics listener",
            serve(
                listener,
                metrics_router(handle),
                header_read_timeout,
                shutdown.clone(),
            ),
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
///
/// Follows axum's official `serve-with-hyper` example, because `axum::serve` does not expose the
/// hyper builder that the header read timeout lives on — the only defence against connections
/// that stall or dribble the request head, which the per-request deadline cannot see (it runs
/// only after a complete head). The plain http1 builder (not auto) starts that timer at
/// connection setup, before any byte arrives, and has no HTTP/2 preface sniffing to park on.
async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    header_read_timeout: Duration,
    shutdown: CancellationToken,
) -> Result<(), String> {
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    loop {
        let socket = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            socket = accept_with_retry(&listener) => socket,
        };
        // `axum::serve` sets TCP_NODELAY on accepted sockets; keep that behaviour.
        let _ = socket.set_nodelay(true);
        let mut builder = http1::Builder::new();
        // The header read timeout runs on hyper's background timer, so one must be installed first.
        builder
            .timer(TokioTimer::new())
            .header_read_timeout(header_read_timeout);
        let service = TowerToHyperService::new(router.clone());
        // The non-upgradeable connection is what `graceful.watch` accepts; the gateway has no
        // upgrade-based protocol.
        let connection = graceful.watch(builder.serve_connection(TokioIo::new(socket), service));
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                // A client speaking HTTP/2 is a misconfiguration on the other side, not per-
                // connection noise, and neither end can see it from a dropped connection alone.
                if error.is_parse_version_h2() {
                    log::warn!(
                        "rejected an HTTP/2 connection preface: this listener serves HTTP/1.1 \
                         only, so clients and ingresses must not be configured for h2c"
                    );
                } else {
                    log::debug!("connection ended with an error: {error}");
                }
            }
        });
    }
    // Idle connections close now; in-flight requests run to completion, bounded by the outer drain
    // budget that aborts this task.
    graceful.shutdown().await;
    Ok(())
}

/// Accepts one connection, absorbing the accept errors that must not end the listener.
///
/// The retry loop lives inside this future, as it does in `axum::serve`'s `Listener::accept`, so a
/// signal arriving during the backoff cancels the wait rather than having to outlast it; both
/// awaited operations are cancellation-safe.
async fn accept_with_retry(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    loop {
        match listener.accept().await {
            Ok((socket, _remote)) => return socket,
            Err(error) => handle_accept_error(error).await,
        }
    }
}

/// Copies `axum::serve`'s accept-loop resilience: per-connection hiccups are ignored, and a
/// resource error such as EMFILE (the process hit its open-file limit) logs and waits a second
/// for fds to be released instead of failing the whole listener.
async fn handle_accept_error(error: std::io::Error) {
    if matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    ) {
        return;
    }
    log::error!("accept error: {error}");
    tokio::time::sleep(Duration::from_secs(1)).await;
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
async fn drain_tasks(
    tasks: &mut JoinSet<TaskExit>,
    deadline: tokio::time::Instant,
) -> Option<String> {
    let mut cleanup_error = None;
    loop {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
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

    fn assert_readiness(
        readiness: &Readiness,
        state: LifecycleState,
        has_started: bool,
        is_accepting: bool,
        is_shutting_down: bool,
        rejection: Option<&str>,
    ) {
        assert_eq!(readiness.state(), state);
        assert_eq!(readiness.has_started(), has_started);
        assert_eq!(readiness.is_accepting(), is_accepting);
        assert_eq!(readiness.is_shutting_down(), is_shutting_down);
        match rejection {
            Some(message) => {
                let error = readiness.ensure_accepting().unwrap_err();
                assert_eq!(error.kind(), crate::error::ErrorKind::Unavailable);
                assert_eq!(error.message(), message);
            }
            None => readiness.ensure_accepting().unwrap(),
        }
    }

    #[test]
    fn readiness_lifecycle() {
        let readiness = Readiness::new();
        assert_readiness(
            &readiness,
            LifecycleState::Starting,
            false,
            false,
            false,
            Some("gateway is starting"),
        );

        readiness.set_serving();
        assert_readiness(&readiness, LifecycleState::Serving, true, true, false, None);

        readiness.begin_quiescing();
        assert_readiness(
            &readiness,
            LifecycleState::Quiescing,
            true,
            false,
            true,
            Some("gateway is shutting down"),
        );

        readiness.begin_draining();
        assert_readiness(
            &readiness,
            LifecycleState::Draining,
            true,
            false,
            true,
            Some("gateway is shutting down"),
        );

        readiness.set_stopped();
        assert_readiness(
            &readiness,
            LifecycleState::Stopped,
            true,
            false,
            true,
            Some("gateway is shutting down"),
        );
    }

    #[test]
    fn lifecycle_transitions_are_idempotent_and_never_move_backwards() {
        let readiness = Readiness::new();
        readiness.set_serving();
        readiness.set_serving();
        readiness.begin_quiescing();
        readiness.set_serving();
        readiness.begin_quiescing();
        readiness.begin_draining();
        readiness.begin_quiescing();
        readiness.set_stopped();
        readiness.begin_draining();

        assert_eq!(readiness.state(), LifecycleState::Stopped);
    }

    #[test]
    #[should_panic(expected = "invalid lifecycle transition")]
    fn lifecycle_transition_cannot_skip_a_state() {
        Readiness::new().begin_draining();
    }

    #[tokio::test]
    async fn start_rejects_programmatically_invalid_config_before_binding() {
        let mut config = GatewayConfig::default();
        config.server.rest.bind_address = "127.0.0.1:0".parse().expect("valid address");
        config.server.metrics.enabled = false;
        config.server.rest.request_timeout = crate::config::ConfigDuration::from_millis(0);

        let error = start(config).await.err().expect("invalid config");
        assert!(
            error
                .to_string()
                .contains("gateway.rest.write.request-timeout must be greater than zero"),
            "got: {error}"
        );
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
        let deadline = started + Duration::from_secs(5);
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
        let error = drain_tasks(
            &mut tasks,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await;

        assert!(error.is_none(), "{error:?}");
        assert!(tasks.is_empty());
    }
}
