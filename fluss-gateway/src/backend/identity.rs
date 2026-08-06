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

//! Per-user act-as connection pool of the user identity mode (FIP-49).
//!
//! Under `connection.identity-mode: user` every authenticated principal gets its own Fluss
//! connection: it authenticates with the super-user service account and carries the principal
//! name as the SASL authorization id, so Fluss authorizes as the impersonated end user. The pool
//! is keyed by principal name, capped by `connection.max` (exceeded → 429 `resource_exhausted`
//! with `Retry-After`), and idle entries are reclaimed lazily on access after
//! `connection.idle-timeout`.
//!
//! Connection construction is injected as an [`IdentityConnector`], so pool behaviour — reuse,
//! capacity, reclamation — is tested without a cluster, and the production connector simply
//! wraps the native backend.

use crate::backend::GatewayBackend;
use crate::error::GatewayError;
use crate::observability;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Connects one act-as backend for the given principal name.
pub type IdentityConnector = Arc<
    dyn Fn(
            String,
        )
            -> Pin<Box<dyn Future<Output = Result<Arc<dyn GatewayBackend>, GatewayError>> + Send>>
        + Send
        + Sync,
>;

/// One pooled per-user connection and when it last served a request.
struct IdentityEntry {
    backend: Arc<dyn GatewayBackend>,
    last_used: Instant,
}

/// The per-cluster pool of act-as connections.
pub struct IdentityPool {
    /// Bounded metric label of the owning cluster.
    cluster: String,
    connector: IdentityConnector,
    max_connections: usize,
    idle_timeout: Duration,
    entries: Mutex<HashMap<String, IdentityEntry>>,
}

impl IdentityPool {
    pub fn new(
        cluster: impl Into<String>,
        connector: IdentityConnector,
        max_connections: usize,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            cluster: cluster.into(),
            connector,
            max_connections: max_connections.max(1),
            idle_timeout,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the pooled connection acting as `user`, creating it on first use.
    ///
    /// Expired entries are reclaimed before the capacity check, so a full pool of idle
    /// connections never starves a new identity. When the pool is at capacity with live
    /// connections the caller gets 429 and retries after the hinted pause.
    pub async fn acquire(&self, user: &str) -> Result<Arc<dyn GatewayBackend>, GatewayError> {
        if let Some(backend) = self.checkout(user)? {
            return Ok(backend);
        }

        // Connect outside the lock; the accumulator handshake must not serialise the pool.
        let backend = (self.connector)(user.to_string()).await?;
        observability::identity_connection_created(&self.cluster);

        let mut entries = self.entries.lock();
        let backend = match entries.get_mut(user) {
            // A concurrent request for the same user won the race: keep the installed
            // connection so both callers share one accumulator, and drop ours.
            Some(existing) => {
                existing.last_used = Instant::now();
                observability::identity_connection_closed(&self.cluster, "duplicate");
                existing.backend.clone()
            }
            None => {
                entries.insert(
                    user.to_string(),
                    IdentityEntry {
                        backend: backend.clone(),
                        last_used: Instant::now(),
                    },
                );
                backend
            }
        };
        observability::identity_connections_active(&self.cluster, entries.len());
        Ok(backend)
    }

    /// Reaps expired entries, then returns the live entry for `user` or verifies capacity for a
    /// new one. `Ok(None)` means the caller may connect.
    fn checkout(&self, user: &str) -> Result<Option<Arc<dyn GatewayBackend>>, GatewayError> {
        let mut entries = self.entries.lock();

        let expired: Vec<String> = entries
            .iter()
            .filter(|(_, entry)| entry.last_used.elapsed() >= self.idle_timeout)
            .map(|(name, _)| name.clone())
            .collect();
        for name in expired {
            // Dropping the last Arc closes the native connection; per-user connections are
            // reclaimed here rather than through the lifecycle supervisor.
            entries.remove(&name);
            observability::identity_connection_closed(&self.cluster, "idle");
        }

        if let Some(entry) = entries.get_mut(user) {
            entry.last_used = Instant::now();
            return Ok(Some(entry.backend.clone()));
        }

        if entries.len() >= self.max_connections {
            observability::identity_connections_active(&self.cluster, entries.len());
            return Err(GatewayError::resource_exhausted(format!(
                "cluster `{}` is serving its maximum of {} per-user connections",
                self.cluster, self.max_connections
            ))
            .with_resource("cluster", Some(self.cluster.as_str())));
        }
        observability::identity_connections_active(&self.cluster, entries.len());
        Ok(None)
    }

    /// The number of pooled connections, for tests and diagnostics.
    /// Live connection count observation for the pool tests.
    #[cfg(test)]
    fn active(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::TestBackend;
    use crate::error::ErrorKind;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A connector that records which users it connected and how often.
    fn counting_connector(calls: Arc<Mutex<Vec<String>>>) -> IdentityConnector {
        Arc::new(move |user: String| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.lock().push(user);
                Ok(Arc::new(TestBackend::new()) as Arc<dyn GatewayBackend>)
            })
        })
    }

    fn failing_connector() -> IdentityConnector {
        Arc::new(|_user: String| {
            Box::pin(async { Err(GatewayError::unavailable("bootstrap unreachable")) })
        })
    }

    #[tokio::test]
    async fn the_same_principal_reuses_one_connection() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let pool = IdentityPool::new(
            "default",
            counting_connector(calls.clone()),
            8,
            Duration::from_secs(3600),
        );

        let first = pool.acquire("alice").await.unwrap();
        let second = pool.acquire("alice").await.unwrap();
        assert!(Arc::ptr_eq(&first, &second), "expected the pooled backend");
        assert_eq!(*calls.lock(), vec!["alice".to_string()]);
        assert_eq!(pool.active(), 1);
    }

    #[tokio::test]
    async fn different_principals_get_distinct_connections() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let pool = IdentityPool::new(
            "default",
            counting_connector(calls.clone()),
            8,
            Duration::from_secs(3600),
        );

        let alice = pool.acquire("alice").await.unwrap();
        let bob = pool.acquire("bob").await.unwrap();
        assert!(!Arc::ptr_eq(&alice, &bob));
        assert_eq!(*calls.lock(), vec!["alice".to_string(), "bob".to_string()]);
        assert_eq!(pool.active(), 2);
    }

    #[tokio::test]
    async fn a_full_pool_answers_resource_exhausted() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let pool = IdentityPool::new(
            "default",
            counting_connector(calls.clone()),
            1,
            Duration::from_secs(3600),
        );

        pool.acquire("alice").await.unwrap();
        let error = match pool.acquire("bob").await {
            Err(error) => error,
            Ok(_) => panic!("expected the full pool to reject a new identity"),
        };
        assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
        assert!(error.retryable());
        assert!(error.message().contains("per-user connections"));
        // The existing identity is still served from the full pool.
        assert!(pool.acquire("alice").await.is_ok());
        assert_eq!(*calls.lock(), vec!["alice".to_string()]);
    }

    #[tokio::test]
    async fn idle_entries_are_reclaimed_before_the_capacity_check() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        // A zero idle timeout expires every entry immediately: each acquire reclaims the
        // previous connection, so reclamation is observable without sleeping.
        let pool = IdentityPool::new(
            "default",
            counting_connector(calls.clone()),
            1,
            Duration::ZERO,
        );

        pool.acquire("alice").await.unwrap();
        // Despite max = 1, bob succeeds because alice's idle entry is reclaimed first.
        pool.acquire("bob").await.unwrap();
        assert_eq!(*calls.lock(), vec!["alice".to_string(), "bob".to_string()]);
        assert_eq!(pool.active(), 1);
    }

    #[tokio::test]
    async fn a_failed_connect_leaves_no_pool_entry() {
        let pool = IdentityPool::new("default", failing_connector(), 8, Duration::from_secs(3600));

        let error = match pool.acquire("alice").await {
            Err(error) => error,
            Ok(_) => panic!("expected the failing connector to surface its error"),
        };
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(pool.active(), 0);
    }

    #[tokio::test]
    async fn concurrent_acquires_for_one_principal_converge_on_one_entry() {
        static CONNECTS: AtomicUsize = AtomicUsize::new(0);
        let connector: IdentityConnector = Arc::new(|_user: String| {
            Box::pin(async {
                CONNECTS.fetch_add(1, Ordering::SeqCst);
                // Yield so both acquires pass the miss check before either installs.
                tokio::task::yield_now().await;
                Ok(Arc::new(TestBackend::new()) as Arc<dyn GatewayBackend>)
            })
        });
        let pool = Arc::new(IdentityPool::new(
            "default",
            connector,
            8,
            Duration::from_secs(3600),
        ));

        let (first, second) = tokio::join!(pool.acquire("alice"), pool.acquire("alice"));
        assert!(
            Arc::ptr_eq(&first.unwrap(), &second.unwrap()),
            "both callers must share one pooled backend"
        );
        assert_eq!(pool.active(), 1);
    }
}
