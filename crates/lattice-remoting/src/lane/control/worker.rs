//! Ordered control application with independent per-stream retry scheduling.
//!
//! The socket loop only submits work and consumes results, leaving heartbeats and reads free to
//! progress while application work waits. A retrying reliable command blocks later commands in
//! its own stream; unrelated streams and ephemeral events can be attempted in the meantime.
//! Deferred frames are bounded. Once that bound is reached, scheduling waits for a retry rather
//! than continuing to drain inbound work into unbounded storage.
//!
//! Application calls are awaited serially by this worker. Stream isolation applies between retry
//! attempts, not to a single application call that has not returned. Retry age starts at the first
//! transient failure and persists across exponential delays; success releases the next deferred
//! command of that stream. Fatal failure is returned to the socket loop and ends the worker.

#![cfg_attr(not(test), deny(clippy::missing_docs_in_private_items))]

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::{
    spawn,
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, sleep_until},
};

use super::{apply_control_frame, apply_ephemeral_control_frame};
use crate::{
    association::Association,
    control::{
        ControlDispatch, ControlDispatchError, ControlFatalReason, ControlStreamId,
        decode_control_envelope,
    },
    lane::LaneError,
    wire::{Frame, FrameKind},
};

/// Owns the apply task so cancelling a lane also cancels its control consumer.
pub(in crate::lane) struct ControlWorker {
    /// Bounded submission channel used by the socket read loop.
    commands: mpsc::Sender<Frame>,
    /// Acknowledgements or fatal results returned for socket-loop handling.
    results: mpsc::Receiver<Result<Option<Frame>, LaneError>>,
    /// Apply task aborted when the containing lane runtime is destroyed.
    task: JoinHandle<()>,
}

impl ControlWorker {
    /// Starts the independent apply task with bounded input, output and deferred storage.
    pub(in crate::lane) fn spawn(
        association: Arc<Association>,
        dispatch: Arc<dyn ControlDispatch>,
        capacity: usize,
        retry_timeout: Duration,
    ) -> Self {
        let (commands, command_rx) = mpsc::channel(capacity);
        let (results, result_rx) = mpsc::channel(capacity);
        let task = spawn(run_control_worker(
            association,
            dispatch,
            command_rx,
            results,
            retry_timeout,
            capacity,
        ));
        Self {
            commands,
            results: result_rx,
            task,
        }
    }

    /// Offers work without blocking socket reads; a full/closed channel fails the lane.
    pub(in crate::lane) fn submit(&self, frame: Frame) -> Result<(), LaneError> {
        self.commands
            .try_send(frame)
            .map_err(|_| LaneError::ControlApplyBackpressure)
    }

    /// Receives an apply result; `None` reports worker termination to the socket loop.
    pub(in crate::lane) async fn recv(&mut self) -> Option<Result<Option<Frame>, LaneError>> {
        self.results.recv().await
    }
}

impl Drop for ControlWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Retry age and delay carried with the same logical command across attempts.
#[derive(Debug, Clone, Copy)]
struct ControlRetryState {
    /// Instant of the first retryable application failure.
    started: Instant,
    /// Delay used if this command fails transiently again.
    next_backoff: Duration,
}

/// One command selected for its next application attempt.
struct ControlWork {
    /// Original encoded operation, retained unchanged for reliable retry.
    frame: Frame,
    /// Existing retry age/delay, absent on the first attempt.
    retry_state: Option<ControlRetryState>,
}

/// A stream-blocking command waiting for its next eligible retry instant.
struct PendingControlRetry {
    /// Original operation to apply again.
    frame: Frame,
    /// Retry age preserved across attempts.
    state: ControlRetryState,
    /// Earliest time the scheduler should select this retry.
    retry_at: Instant,
}

/// Keeps later commands behind their stream's retry while other streams can progress.
struct ControlSchedule {
    /// Deferred commands whose preceding retry just completed, ready to continue in order.
    ready: VecDeque<ControlWork>,
    /// Later commands blocked behind a retry in their own stream.
    deferred: VecDeque<Frame>,
    /// At most one retrying command per blocked stream.
    pending: BTreeMap<ControlStreamId, PendingControlRetry>,
    /// Whether input ended; retained work is processed before normal termination.
    command_closed: bool,
    /// Bound on later commands held behind retries, independent of channel capacity.
    maximum_deferred: usize,
}

impl ControlSchedule {
    /// Creates an empty schedule with a finite deferred-command bound.
    fn new(maximum_deferred: usize) -> Self {
        Self {
            ready: VecDeque::new(),
            deferred: VecDeque::with_capacity(maximum_deferred),
            pending: BTreeMap::new(),
            command_closed: false,
            maximum_deferred,
        }
    }

    /// Selects released work, an unrelated incoming command, or the earliest retry.
    ///
    /// Frames from blocked streams enter deferred storage. Once storage is full, stop accepting
    /// additional commands until a retry can progress; the input channel then provides backpressure.
    async fn next(
        &mut self,
        commands: &mut mpsc::Receiver<Frame>,
    ) -> Result<Option<ControlWork>, LaneError> {
        loop {
            if let Some(work) = self.ready.pop_front() {
                return Ok(Some(work));
            }
            if self.pending.is_empty() {
                if self.command_closed {
                    return Ok(None);
                }
                match commands.recv().await {
                    Some(frame) => {
                        return Ok(Some(ControlWork {
                            frame,
                            retry_state: None,
                        }));
                    }
                    None => {
                        self.command_closed = true;
                        continue;
                    }
                }
            }
            let retry_at = self
                .pending
                .values()
                .map(|retry| retry.retry_at)
                .min()
                .expect("nonempty pending retry map");
            if self.deferred.len() == self.maximum_deferred || self.command_closed {
                sleep_until(retry_at).await;
                return Ok(Some(self.take_retry()));
            }
            tokio::select! {
                biased;
                inbound = commands.recv() => {
                    let Some(frame) = inbound else {
                        self.command_closed = true;
                        continue;
                    };
                    if frame.kind != FrameKind::CoordinatorEvent {
                        let stream_id = decode_control_envelope(&frame)?.stream_id;
                        if self.pending.contains_key(&stream_id) {
                            self.deferred.push_back(frame);
                            continue;
                        }
                    }
                    return Ok(Some(ControlWork { frame, retry_state: None }));
                }
                _ = sleep_until(retry_at) => return Ok(Some(self.take_retry())),
            }
        }
    }

    /// Removes the earliest pending retry while preserving its accumulated retry age.
    fn take_retry(&mut self) -> ControlWork {
        let stream_id = self
            .pending
            .iter()
            .min_by_key(|(_, retry)| retry.retry_at)
            .map(|(stream_id, _)| *stream_id)
            .expect("nonempty pending retry map");
        let retry = self
            .pending
            .remove(&stream_id)
            .expect("selected pending retry exists");
        ControlWork {
            frame: retry.frame,
            retry_state: Some(retry.state),
        }
    }

    /// Parks a transiently failed command, or fails the lane once its retry age reaches the limit.
    ///
    /// Delays start at 25 milliseconds and double up to one second. The original start instant
    /// is never renewed, so repeated transient failures cannot extend the command's retry age.
    fn retry(
        &mut self,
        work: ControlWork,
        stream_id: ControlStreamId,
        timeout: Duration,
        association: &Association,
    ) -> Result<(), LaneError> {
        let state = work.retry_state.unwrap_or(ControlRetryState {
            started: Instant::now(),
            next_backoff: Duration::from_millis(25),
        });
        if state.started.elapsed() >= timeout {
            association.record_control_retry_exhaustion();
            return Err(
                ControlDispatchError::Fatal(ControlFatalReason::RetryDeadlineExceeded).into(),
            );
        }
        association.record_control_apply_retry();
        self.pending.insert(
            stream_id,
            PendingControlRetry {
                frame: work.frame,
                state: ControlRetryState {
                    started: state.started,
                    next_backoff: state
                        .next_backoff
                        .saturating_mul(2)
                        .min(Duration::from_secs(1)),
                },
                retry_at: Instant::now() + state.next_backoff,
            },
        );
        Ok(())
    }

    /// Releases only the next blocked command from a stream whose preceding work completed.
    ///
    /// Releasing one at a time preserves order if that next command also needs to retry.
    fn release_deferred(&mut self, stream_id: ControlStreamId) {
        if let Some(index) = self.deferred.iter().position(|candidate| {
            decode_control_envelope(candidate).is_ok_and(|envelope| envelope.stream_id == stream_id)
        }) {
            let frame = self
                .deferred
                .remove(index)
                .expect("selected deferred control frame exists");
            self.ready.push_back(ControlWork {
                frame,
                retry_state: None,
            });
        }
    }
}

/// Applies selected work, retries reliable transient failures and returns terminal results.
///
/// Ephemeral events have one best-effort attempt. Reliable results carry acknowledgements back
/// to the socket loop; inability to return a result ends work rather than silently losing it.
async fn run_control_worker(
    association: Arc<Association>,
    dispatch: Arc<dyn ControlDispatch>,
    mut commands: mpsc::Receiver<Frame>,
    results: mpsc::Sender<Result<Option<Frame>, LaneError>>,
    retry_timeout: Duration,
    maximum_deferred: usize,
) {
    let mut schedule = ControlSchedule::new(maximum_deferred);
    loop {
        let work = match schedule.next(&mut commands).await {
            Ok(Some(work)) => work,
            Ok(None) => break,
            Err(error) => {
                let _ = results.send(Err(error)).await;
                break;
            }
        };
        if work.frame.kind == FrameKind::CoordinatorEvent {
            if let Err(error) =
                apply_ephemeral_control_frame(association.as_ref(), dispatch.as_ref(), work.frame)
                    .await
            {
                let _ = results.send(Err(error)).await;
                break;
            }
            continue;
        }
        let stream_id = match decode_control_envelope(&work.frame) {
            Ok(envelope) => envelope.stream_id,
            Err(error) => {
                let _ = results.send(Err(error.into())).await;
                break;
            }
        };
        let result =
            apply_control_frame(association.as_ref(), dispatch.as_ref(), work.frame.clone()).await;
        if matches!(
            result,
            Err(LaneError::ControlDispatch(
                ControlDispatchError::RetryLater(_)
            ))
        ) {
            if let Err(error) = schedule.retry(work, stream_id, retry_timeout, &association) {
                let _ = results.send(Err(error)).await;
                break;
            }
            continue;
        }
        let failed = result.is_err();
        if results.send(result).await.is_err() || failed {
            break;
        }
        schedule.release_deferred(stream_id);
    }
}
