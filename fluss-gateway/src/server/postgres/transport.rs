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

//! PostgreSQL TCP transport (`transport`).
//!
//! Owns the listener / accept loop / per-connection task and nothing about SQL
//!. Cleartext only; TLS is delegated to a fronting proxy, so
//! `process_socket` is always called with `None` for the TLS acceptor. Each accepted socket
//! gets its own [`PgConnection`] (no global shared session) plus a connection
//! id used as the backend PID for the out-of-band cancel handshake.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::{NoopHandler, PgWireServerHandlers};
use pgwire::messages::cancel::CancelRequest;
use tokio::net::{TcpListener, TcpStream};

use crate::auth::Authenticator;
use crate::instance::GatewayInstance;
use crate::types::OperationId;

use super::adapter::{CancelRegistry, CancelResolution};
use super::handler::PgConnection;

/// Shared, cheaply-cloneable wiring for the PostgreSQL frontend: the gateway
/// facade, the auth seam, and the out-of-band cancel registry. One of these is
/// built per server and handed to every connection task.
#[derive(Clone)]
pub struct PgServer {
    instance: Arc<dyn GatewayInstance>,
    authenticator: Arc<dyn Authenticator>,
    cancels: CancelRegistry,
    next_pid: Arc<AtomicI32>,
}

impl PgServer {
    pub fn new(
        instance: Arc<dyn GatewayInstance>,
        authenticator: Arc<dyn Authenticator>,
    ) -> Self {
        Self {
            instance,
            authenticator,
            cancels: CancelRegistry::new(),
            next_pid: Arc::new(AtomicI32::new(1)),
        }
    }

    /// Bind a TCP listener and return it along with the resolved local address.
    /// Splitting bind from serve lets tests learn the ephemeral port (bind to
    /// `127.0.0.1:0`) before driving traffic.
    pub async fn bind(addr: &str) -> std::io::Result<(TcpListener, std::net::SocketAddr)> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        Ok((listener, local))
    }

    /// Accept connections forever, spawning a per-connection task for each.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (socket, _peer) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                server.handle_connection(socket).await;
            });
        }
    }

    async fn handle_connection(&self, socket: TcpStream) {
        let _ = socket.set_nodelay(true);
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        let connection = Arc::new(PgConnection::new(
            self.instance.clone(),
            self.authenticator.clone(),
            self.cancels.clone(),
            pid,
        ));
        let handlers = PgHandlers {
            connection: connection.clone(),
            cancel: Arc::new(PgCancelHandler {
                instance: self.instance.clone(),
                cancels: self.cancels.clone(),
            }),
        };

        // Cleartext only (no TLS acceptor). Errors are per-connection
        // and non-fatal to the server.
        let _ = pgwire::tokio::process_socket(socket, None, handlers).await;

        // Connection done: drop its cancel-key entry and close its session so a
        // long-lived session does not leak when the client disconnects (close
        // semantics; a running operation is decoupled and not force-aborted here).
        self.cancels.remove(pid);
        if let Some(session_id) = connection.session_id() {
            let _ = self.instance.close_session(session_id).await;
        }
    }
}

/// Per-connection handler bundle handed to `process_socket`. The same
/// [`PgConnection`] backs all three SQL-facing handler roles (startup / simple /
/// extended); the cancel handler is separate because a `CancelRequest` arrives
/// on a *different* connection and must reach the shared registry.
struct PgHandlers {
    connection: Arc<PgConnection>,
    cancel: Arc<PgCancelHandler>,
}

impl PgWireServerHandlers for PgHandlers {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        self.connection.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        self.connection.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        self.connection.clone()
    }

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
    }

    fn error_handler(&self) -> Arc<impl pgwire::api::ErrorHandler> {
        Arc::new(NoopHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.cancel.clone()
    }
}

/// Maps a PG `CancelRequest` (PID + secret) to `Instance.cancel_operation`
///. Verifies the secret via the shared registry; ignores cancels with
/// no running operation; rejects (silently, per the PG cancel protocol which has
/// no reply) unknown pids / bad secrets.
struct PgCancelHandler {
    instance: Arc<dyn GatewayInstance>,
    cancels: CancelRegistry,
}

#[async_trait]
impl CancelHandler for PgCancelHandler {
    async fn on_cancel_request(&self, cancel_request: CancelRequest) {
        let pid = cancel_request.pid;
        let Some(secret) = cancel_request.secret_key.as_i32() else {
            return;
        };
        match self.cancels.resolve_cancel(pid, secret) {
            CancelResolution::Cancel(op) => {
                let _ = self.instance.cancel_operation(OperationId(op.0)).await;
            }
            // Secret matched but nothing running -> ignore.
            CancelResolution::Ignore => {}
            // Unknown pid / bad secret -> reject (no-op; the protocol sends no reply).
            CancelResolution::Reject => {}
        }
    }
}
