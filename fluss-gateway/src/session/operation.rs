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

//! P2.7 / P2.8 / P2.10 — query-scoped [`Operation`] state machine and the
//! per-session [`OperationManager`].
//!
//! `OperationState` (P1 `types.rs`) is the authoritative state enum. This module
//! adds the *transition rules*: only the edges enumerated in design §P2.7 are
//! legal, terminal states never regress, and `CancelRequested` is transitional.
//! The §P2.8 tracked-stream lifecycle points (first poll, EOF, deadline, cancel,
//! exec error) are expressed as named transition methods so the semantics are
//! testable now; the real stream wrapper lands in P4.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use tokio_util::sync::CancellationToken;

use crate::types::{CancelResult, OperationId, OperationState, OperationStatusSnapshot};

/// Whether a state is terminal (no outgoing transitions; design §P2.7).
fn is_terminal(state: OperationState) -> bool {
    matches!(
        state,
        OperationState::Finished
            | OperationState::Failed
            | OperationState::Cancelled
            | OperationState::TimedOut
    )
}

/// The legal transition edges from §P2.7:
///
/// ```text
/// Pending -> Running, CancelRequested
/// Running -> Finished, Failed, CancelRequested, TimedOut
/// CancelRequested -> Cancelled, TimedOut
/// ```
///
/// Terminal states (`Finished` / `Failed` / `Cancelled` / `TimedOut`) have no
/// outgoing edges and never regress.
fn transition_allowed(from: OperationState, to: OperationState) -> bool {
    use OperationState::*;
    matches!(
        (from, to),
        (Pending, Running)
            | (Pending, CancelRequested)
            | (Running, Finished)
            | (Running, Failed)
            | (Running, CancelRequested)
            | (Running, TimedOut)
            | (CancelRequested, Cancelled)
            | (CancelRequested, TimedOut)
    )
}

/// A single query-scoped operation. Only the SQL path creates these; the direct
/// path never does (design §P2.1).
#[derive(Debug)]
pub struct Operation {
    pub id: OperationId,
    pub statement_summary: String,
    state: OperationState,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub error: Option<String>,
    pub cancel_token: CancellationToken,
}

impl Operation {
    /// Create a fresh `Pending` operation with its own cancel token.
    pub fn new(id: OperationId, statement_summary: impl Into<String>) -> Self {
        Self {
            id,
            statement_summary: statement_summary.into(),
            state: OperationState::Pending,
            created_at: SystemTime::now(),
            started_at: None,
            finished_at: None,
            error: None,
            cancel_token: CancellationToken::new(),
        }
    }

    pub fn state(&self) -> OperationState {
        self.state
    }

    pub fn is_terminal(&self) -> bool {
        is_terminal(self.state)
    }

    /// Attempt an arbitrary state transition. Returns `true` if the edge is legal
    /// and was applied, `false` if rejected (illegal edge or terminal source).
    /// Idempotent timestamp bookkeeping is handled by the named helpers below.
    fn try_transition(&mut self, to: OperationState) -> bool {
        if !transition_allowed(self.state, to) {
            return false;
        }
        self.state = to;
        true
    }

    /// §P2.8 first poll: `Pending -> Running`.
    pub fn mark_running(&mut self) -> bool {
        if self.try_transition(OperationState::Running) {
            self.started_at.get_or_insert_with(SystemTime::now);
            true
        } else {
            false
        }
    }

    /// §P2.8 normal EOF: `Running -> Finished`.
    pub fn mark_finished(&mut self) -> bool {
        if self.try_transition(OperationState::Finished) {
            self.finished_at.get_or_insert_with(SystemTime::now);
            true
        } else {
            false
        }
    }

    /// §P2.8 non-cancel execution error: `Running -> Failed`. `Failed` is never
    /// used for cancel/timeout outcomes.
    pub fn mark_failed(&mut self, error: impl Into<String>) -> bool {
        if self.try_transition(OperationState::Failed) {
            self.finished_at.get_or_insert_with(SystemTime::now);
            self.error = Some(error.into());
            true
        } else {
            false
        }
    }

    /// §P2.10 cancel request: `Pending/Running -> CancelRequested`. Fires the
    /// cancel token regardless so cooperative consumers observe it. If already in
    /// a terminal state, no transition occurs.
    pub fn request_cancel(&mut self) -> bool {
        let moved = self.try_transition(OperationState::CancelRequested);
        if moved {
            self.cancel_token.cancel();
        }
        moved
    }

    /// §P2.8 cancel path exit: `CancelRequested -> Cancelled`.
    pub fn mark_cancelled(&mut self) -> bool {
        if self.try_transition(OperationState::Cancelled) {
            self.finished_at.get_or_insert_with(SystemTime::now);
            true
        } else {
            false
        }
    }

    /// §P2.8 deadline hit: `Running/CancelRequested -> TimedOut`.
    pub fn mark_timed_out(&mut self) -> bool {
        if self.try_transition(OperationState::TimedOut) {
            self.finished_at.get_or_insert_with(SystemTime::now);
            true
        } else {
            false
        }
    }

    pub fn status_snapshot(&self) -> OperationStatusSnapshot {
        OperationStatusSnapshot {
            id: self.id.clone(),
            state: self.state,
            statement_summary: self.statement_summary.clone(),
            error: self.error.clone(),
        }
    }
}

/// Per-session registry of operations (design §P2.10 / §P2.11). Tracks live and
/// terminal operations, answers status queries, and routes cancel requests.
#[derive(Debug, Default)]
pub struct OperationManager {
    operations: Mutex<HashMap<OperationId, Operation>>,
}

impl OperationManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a freshly created operation. Returns a clone of its cancel token
    /// so the caller can wire it into the execution stream.
    pub fn register(&self, op: Operation) -> CancellationToken {
        let token = op.cancel_token.clone();
        self.operations.lock().unwrap().insert(op.id.clone(), op);
        token
    }

    pub fn status(&self, id: &OperationId) -> Option<OperationStatusSnapshot> {
        self.operations
            .lock()
            .unwrap()
            .get(id)
            .map(Operation::status_snapshot)
    }

    /// §P2.10 cancel semantics: distinguish not-found / already-terminal /
    /// accepted. A `running` (or `pending`) operation moves to `CancelRequested`
    /// and its token fires; cancel is cooperative / best-effort.
    pub fn cancel(&self, id: &OperationId) -> CancelResult {
        let mut ops = self.operations.lock().unwrap();
        match ops.get_mut(id) {
            None => CancelResult::NotFound,
            Some(op) if op.is_terminal() => CancelResult::AlreadyTerminal,
            Some(op) => {
                // CancelRequested is already an in-flight cancel; treat as accepted.
                op.request_cancel();
                CancelResult::Accepted
            }
        }
    }

    /// §P2.6 — request cancel on every non-terminal operation (used by session
    /// close). Does not wait for them to exit.
    pub fn cancel_all_active(&self) {
        let mut ops = self.operations.lock().unwrap();
        for op in ops.values_mut() {
            if !op.is_terminal() {
                op.request_cancel();
            }
        }
    }

    /// True if any registered operation is still non-terminal (used by the idle
    /// reaper, §P2.11).
    pub fn has_active(&self) -> bool {
        self.operations
            .lock()
            .unwrap()
            .values()
            .any(|op| !op.is_terminal())
    }

    /// Run a closure against a registered operation under the lock, for driving
    /// tracked-stream transitions. Returns `None` if the id is unknown.
    pub fn with_operation<R>(
        &self,
        id: &OperationId,
        f: impl FnOnce(&mut Operation) -> R,
    ) -> Option<R> {
        self.operations.lock().unwrap().get_mut(id).map(f)
    }

    #[cfg(test)]
    pub fn snapshots_for_test(&self) -> Vec<OperationStatusSnapshot> {
        self.operations
            .lock()
            .unwrap()
            .values()
            .map(Operation::status_snapshot)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op() -> Operation {
        Operation::new(OperationId("op-1".into()), "SELECT 1")
    }

    // §P2.7 — legal happy-path transitions.
    #[test]
    fn legal_running_then_finished() {
        let mut o = op();
        assert_eq!(o.state(), OperationState::Pending);
        assert!(o.mark_running());
        assert_eq!(o.state(), OperationState::Running);
        assert!(o.started_at.is_some());
        assert!(o.mark_finished());
        assert_eq!(o.state(), OperationState::Finished);
        assert!(o.finished_at.is_some());
    }

    #[test]
    fn legal_running_then_failed_records_error() {
        let mut o = op();
        assert!(o.mark_running());
        assert!(o.mark_failed("boom"));
        assert_eq!(o.state(), OperationState::Failed);
        assert_eq!(o.error.as_deref(), Some("boom"));
    }

    #[test]
    fn legal_cancel_request_then_cancelled() {
        let mut o = op();
        assert!(o.mark_running());
        assert!(o.request_cancel());
        assert_eq!(o.state(), OperationState::CancelRequested);
        assert!(o.cancel_token.is_cancelled());
        assert!(o.mark_cancelled());
        assert_eq!(o.state(), OperationState::Cancelled);
    }

    #[test]
    fn legal_pending_cancel_request() {
        let mut o = op();
        // Pending -> CancelRequested is allowed (cancel before first poll).
        assert!(o.request_cancel());
        assert_eq!(o.state(), OperationState::CancelRequested);
        assert!(o.mark_cancelled());
        assert_eq!(o.state(), OperationState::Cancelled);
    }

    #[test]
    fn legal_running_timed_out() {
        let mut o = op();
        assert!(o.mark_running());
        assert!(o.mark_timed_out());
        assert_eq!(o.state(), OperationState::TimedOut);
    }

    #[test]
    fn legal_cancel_requested_timed_out() {
        let mut o = op();
        assert!(o.mark_running());
        assert!(o.request_cancel());
        // deadline can still win over a pending cancel.
        assert!(o.mark_timed_out());
        assert_eq!(o.state(), OperationState::TimedOut);
    }

    // §P2.7 — illegal transitions are rejected; terminal states never regress.
    #[test]
    fn finished_cannot_go_back_to_running() {
        let mut o = op();
        o.mark_running();
        o.mark_finished();
        assert!(!o.mark_running());
        assert!(!o.mark_failed("x"));
        assert!(!o.request_cancel());
        assert!(!o.mark_timed_out());
        assert_eq!(o.state(), OperationState::Finished);
    }

    #[test]
    fn pending_cannot_finish_directly() {
        let mut o = op();
        // Must go through Running first.
        assert!(!o.mark_finished());
        assert!(!o.mark_failed("x"));
        assert!(!o.mark_cancelled());
        assert_eq!(o.state(), OperationState::Pending);
    }

    #[test]
    fn terminal_states_are_mutually_exclusive_and_final() {
        let mut o = op();
        o.mark_running();
        o.mark_timed_out();
        // Already TimedOut: cannot become Cancelled or Failed.
        assert!(!o.mark_cancelled());
        assert!(!o.mark_failed("x"));
        assert_eq!(o.state(), OperationState::TimedOut);
    }

    // §P2.10 — CancelResult: NotFound / AlreadyTerminal / Accepted.
    #[test]
    fn cancel_unknown_is_not_found() {
        let mgr = OperationManager::new();
        assert_eq!(
            mgr.cancel(&OperationId("nope".into())),
            CancelResult::NotFound
        );
    }

    #[test]
    fn cancel_running_is_accepted_and_sets_cancel_requested() {
        let mgr = OperationManager::new();
        let id = OperationId("op-1".into());
        let mut o = Operation::new(id.clone(), "SELECT 1");
        o.mark_running();
        mgr.register(o);

        assert_eq!(mgr.cancel(&id), CancelResult::Accepted);
        assert_eq!(
            mgr.status(&id).unwrap().state,
            OperationState::CancelRequested
        );
    }

    #[test]
    fn cancel_terminal_is_already_terminal() {
        let mgr = OperationManager::new();
        let id = OperationId("op-1".into());
        let mut o = Operation::new(id.clone(), "SELECT 1");
        o.mark_running();
        o.mark_finished();
        mgr.register(o);

        assert_eq!(mgr.cancel(&id), CancelResult::AlreadyTerminal);
    }

    #[test]
    fn cancel_all_active_fires_only_non_terminal() {
        let mgr = OperationManager::new();
        let running = OperationId("r".into());
        let done = OperationId("d".into());

        let mut ro = Operation::new(running.clone(), "SELECT 1");
        ro.mark_running();
        mgr.register(ro);

        let mut dobj = Operation::new(done.clone(), "SELECT 2");
        dobj.mark_running();
        dobj.mark_finished();
        mgr.register(dobj);

        assert!(mgr.has_active());
        mgr.cancel_all_active();
        // The running op moves to CancelRequested (still non-terminal: cancel is
        // cooperative, it is not terminal until the stream actually winds down).
        assert_eq!(
            mgr.status(&running).unwrap().state,
            OperationState::CancelRequested
        );
        // The already-finished op is untouched.
        assert_eq!(mgr.status(&done).unwrap().state, OperationState::Finished);
    }
}
