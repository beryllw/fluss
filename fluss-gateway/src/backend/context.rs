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

//! Request-scoped application metadata, deadlines, and cancellation.

use crate::auth::Principal;
use crate::backend::types::ClusterId;
use crate::error::GatewayError;
use std::time::Instant;
use tokio_util::sync::{CancellationToken, DropGuard};

/// Cloneable cooperative cancellation signal shared by an adapter and the application service.
#[derive(Debug, Clone, Default)]
pub struct CancellationSignal(CancellationToken);

impl CancellationSignal {
    /// Marks the signal cancelled. Calling this more than once has no additional effect.
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// Returns whether cancellation has already been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Waits until cancellation is requested.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }

    /// Creates an RAII guard that cancels this signal when an adapter request is dropped.
    pub(crate) fn drop_guard(&self) -> DropGuard {
        self.0.clone().drop_guard()
    }
}

/// Metadata and lifecycle controls shared by all operations in one external request.
#[derive(Debug, Clone)]
pub struct RequestContext {
    request_id: String,
    cluster_id: ClusterId,
    deadline: Instant,
    cancellation: CancellationSignal,
    principal: Principal,
}

impl RequestContext {
    /// Creates a request context with an absolute monotonic deadline and the authenticated caller.
    pub fn new(
        request_id: impl Into<String>,
        cluster_id: ClusterId,
        deadline: Instant,
        cancellation: CancellationSignal,
        principal: Principal,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            cluster_id,
            deadline,
            cancellation,
            principal,
        }
    }

    /// Runs one backend operation under this request's cancellation signal and absolute deadline.
    pub async fn run<T, F>(&self, operation: F) -> Result<T, GatewayError>
    where
        F: std::future::Future<Output = Result<T, GatewayError>>,
    {
        self.ensure_active()?;
        let deadline = tokio::time::Instant::from_std(self.deadline());
        tokio::select! {
            biased;
            _ = self.cancellation().cancelled() => {
                Err(GatewayError::cancelled("request was cancelled"))
            }
            _ = tokio::time::sleep_until(deadline) => {
                Err(GatewayError::deadline_exceeded("request deadline exceeded"))
            }
            result = operation => result,
        }
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// The authenticated caller this request acts for. Identity-aware backends (per-user act-as
    /// connections) key their connection choice on `principal().name`.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Rejects work that has already been cancelled or has no time remaining.
    pub fn ensure_active(&self) -> Result<(), GatewayError> {
        if self.cancellation.is_cancelled() {
            return Err(GatewayError::cancelled("request was cancelled"));
        }
        if Instant::now() >= self.deadline {
            return Err(GatewayError::deadline_exceeded("request deadline exceeded"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cluster_id() -> ClusterId {
        ClusterId::try_from("default").unwrap()
    }

    #[test]
    fn rejects_cancelled_request() {
        let cancellation = CancellationSignal::default();
        let context = RequestContext::new(
            "request-1",
            cluster_id(),
            Instant::now() + Duration::from_secs(1),
            cancellation.clone(),
            Principal::new("tester"),
        );
        cancellation.cancel();

        assert_eq!(
            context.ensure_active().unwrap_err().kind(),
            crate::error::ErrorKind::Cancelled
        );
    }

    #[test]
    fn rejects_expired_request() {
        let context = RequestContext::new(
            "request-1",
            cluster_id(),
            Instant::now(),
            CancellationSignal::default(),
            Principal::new("tester"),
        );

        assert_eq!(
            context.ensure_active().unwrap_err().kind(),
            crate::error::ErrorKind::DeadlineExceeded
        );
    }

    #[tokio::test]
    async fn run_enforces_the_deadline_over_a_slow_operation() {
        use std::time::{Duration, Instant};
        let context = RequestContext::new(
            "request-1",
            ClusterId::try_from("default").unwrap(),
            Instant::now() + Duration::from_millis(20),
            CancellationSignal::default(),
            crate::auth::Principal::new("tester"),
        );
        let error = context
            .run(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<(), GatewayError>(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), crate::error::ErrorKind::DeadlineExceeded);
    }
}
