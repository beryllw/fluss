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

//! The lookup half of [`GatewayService`]: batched primary-key lookup and bounded prefix lookup.
//!
//! Both methods are thin: they resolve the request's cluster, run the backend call under the request's
//! cancellation signal and absolute deadline, and check the one invariant the whole REST contract rests on —
//! that the backend answered with exactly one outcome per input, in input order. Everything schema-shaped
//! (which columns a key carries, how a value parses) belongs to the adapter, and everything cluster-shaped
//! (whether a prefix is legal for this table) belongs to the Fluss client.
//!
//! In particular this layer does **not** re-implement the client's `validate_prefix_lookup`. The rules that
//! decide whether a prefix lookup is legal depend on the requested prefix columns, not on table metadata, so the
//! only correct oracle is the client itself: the backend builds the lookuper and the client's refusal travels
//! back as an invalid argument. A copy of those rules here would drift and would reject prefix lookups that work.
//!
//! The two endpoints differ in how "nothing matched" is reported. A point lookup answers per key with an explicit
//! not-found outcome, because a primary key names at most one row. A prefix lookup answers per prefix with a
//! zero-row batch, because an empty range is a normal result rather than a missing resource.

use crate::application::{GatewayService, RequestContext};
use crate::backend::model::{
    LookupKey, LookupOutcome, PrefixLookupOutcome, PrefixLookupRequest, TableRef,
};
use crate::error::GatewayError;

/// The two lookup paths.
///
/// One of several inherent `impl GatewayService` blocks; see [`crate::application::service`].
impl GatewayService {
    /// Looks up rows by primary key, returning exactly one outcome per input key in input order.
    ///
    /// Keys carry values in logical primary-key order, partition key columns included. A miss is an outcome,
    /// never an error.
    pub async fn lookup(
        &self,
        context: &RequestContext,
        table: &TableRef,
        keys: Vec<LookupKey>,
    ) -> Result<Vec<LookupOutcome>, GatewayError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let backend = self.backend(context)?;
        let expected = keys.len();
        let outcomes = self.execute(context, backend.lookup(table, keys)).await?;
        check_alignment(
            "lookup",
            expected,
            outcomes.iter().map(|outcome| outcome.input_index),
        )?;
        Ok(outcomes)
    }

    /// Looks up rows by key prefix, returning exactly one outcome per input prefix in input order.
    ///
    /// The prefix columns must cover the table's bucket keys so each prefix routes to a single bucket, which the
    /// Fluss client decides while building its lookuper; a refusal arrives here as an invalid argument carrying
    /// the client's own message. Results are truncated at `request.max_rows_per_prefix` and flagged when they are.
    pub async fn prefix_lookup(
        &self,
        context: &RequestContext,
        table: &TableRef,
        request: PrefixLookupRequest,
    ) -> Result<Vec<PrefixLookupOutcome>, GatewayError> {
        if request.prefixes.is_empty() {
            return Ok(Vec::new());
        }
        let backend = self.backend(context)?;
        let expected = request.prefixes.len();
        let outcomes = self
            .execute(context, backend.prefix_lookup(table, request))
            .await?;
        check_alignment(
            "prefix lookup",
            expected,
            outcomes.iter().map(|outcome| outcome.input_index),
        )?;
        Ok(outcomes)
    }
}

/// Verifies that a backend answered with one outcome per input, in input order.
///
/// Positional alignment is the entire correlation mechanism of both endpoints — there is no per-key echo of the
/// key in the response — so a backend that reorders or drops an outcome would silently attribute one key's row to
/// another key. That is a gateway bug rather than a caller error, hence the internal classification.
fn check_alignment(
    operation: &str,
    expected: usize,
    indexes: impl Iterator<Item = usize>,
) -> Result<(), GatewayError> {
    let mut seen = 0usize;
    for (position, input_index) in indexes.enumerate() {
        if input_index != position {
            return Err(GatewayError::internal(format!(
                "{operation} returned outcome {position} for input {input_index}; \
                 outcomes must be aligned with the request"
            )));
        }
        seen += 1;
    }
    if seen != expected {
        return Err(GatewayError::internal(format!(
            "{operation} returned {seen} outcomes for {expected} inputs"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{CancellationSignal, ClusterId};
    use crate::backend::model::{KeyValue, LookupOutcomeKind};
    use crate::backend::registry::ClusterRegistry;
    use crate::backend::testing::TestBackend;
    use crate::error::ErrorKind;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn service() -> (GatewayService, Arc<TestBackend>) {
        let backend = Arc::new(TestBackend::new());
        let clusters = Arc::new(ClusterRegistry::single_for_test(
            "default",
            backend.clone(),
            crate::backend::model::ClusterHealthReport {
                status: crate::backend::model::ClusterStatus::Green,
                num_replicas: 1,
                in_sync_replicas: 1,
                num_leader_replicas: 1,
                active_leader_replicas: 1,
            },
        ));
        (GatewayService::new(clusters), backend)
    }

    fn context() -> RequestContext {
        RequestContext::new(
            "request-1",
            "test",
            ClusterId::try_from("default").unwrap(),
            Instant::now() + Duration::from_secs(5),
            CancellationSignal::default(),
        )
    }

    fn key(id: i32) -> LookupKey {
        LookupKey::new(vec![KeyValue::Int(id)])
    }

    #[tokio::test]
    async fn a_batch_answers_one_outcome_per_key_in_input_order() {
        let (service, _backend) = service();
        let table = TableRef::new("fluss", "users");

        let outcomes = service
            .lookup(&context(), &table, vec![key(1), key(404), key(2)])
            .await
            .expect("the fixture table exists");

        assert_eq!(outcomes.len(), 3);
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.input_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(matches!(outcomes[0].kind, LookupOutcomeKind::Found(_)));
        assert!(matches!(outcomes[1].kind, LookupOutcomeKind::NotFound));
    }

    #[tokio::test]
    async fn an_empty_batch_never_reaches_the_backend() {
        let (service, _backend) = service();
        let table = TableRef::new("fluss", "missing");

        // A missing table would be a 404 from the backend; an empty batch answers without asking.
        assert!(
            service
                .lookup(&context(), &table, Vec::new())
                .await
                .expect("an empty batch is trivially satisfiable")
                .is_empty()
        );
        assert!(
            service
                .prefix_lookup(
                    &context(),
                    &table,
                    PrefixLookupRequest {
                        prefix_columns: vec!["region".to_string()],
                        prefixes: Vec::new(),
                        max_rows_per_prefix: 10,
                    },
                )
                .await
                .expect("an empty batch is trivially satisfiable")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_missing_table_is_reported_by_the_backend() {
        let (service, _backend) = service();
        let error = service
            .lookup(&context(), &TableRef::new("fluss", "missing"), vec![key(1)])
            .await
            .expect_err("the fixture has no such table");
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn an_unreachable_cluster_fails_the_whole_batch() {
        let (service, backend) = service();
        backend.set_available(false);
        let error = service
            .lookup(&context(), &TableRef::new("fluss", "users"), vec![key(1)])
            .await
            .expect_err("an unreachable cluster cannot answer");
        assert_eq!(error.kind(), ErrorKind::Unavailable);
    }

    #[test]
    fn misaligned_outcomes_are_an_internal_error_rather_than_a_silent_mismatch() {
        assert!(check_alignment("lookup", 2, [0, 1].into_iter()).is_ok());

        let reordered = check_alignment("lookup", 2, [1, 0].into_iter())
            .expect_err("a reordered batch breaks key correlation");
        assert_eq!(reordered.kind(), ErrorKind::Internal);

        let dropped = check_alignment("lookup", 3, [0, 1].into_iter())
            .expect_err("a dropped outcome breaks key correlation");
        assert_eq!(dropped.kind(), ErrorKind::Internal);
    }
}
