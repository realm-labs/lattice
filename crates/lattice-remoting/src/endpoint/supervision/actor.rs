//! Lifecycle events are serialized here; handlers never wait for network I/O.
//!
//! A completed socket has already published detach before its event reaches this actor. The
//! actor restores its receiver and chooses between inbound waiting, outgoing sleep and retry.
//! Ordinary network failure preserves the receiver and generation. A task join failure cannot
//! recover the receiver, so continuing would leave a queue with no consumer; retire instead.
//!
//! Initial establishment expiry is armed once. A sticky activation flag distinguishes a peer
//! that never connected from an established peer temporarily reconnecting after that deadline.

use std::sync::{Arc, Weak};

use lattice_actor::{
    context::{ActorContext, HandlerContext},
    error::ActorStopError,
    state_machine::Stateless,
    traits::{Actor, Handler, StopReason},
};
use tokio::{runtime::Handle, time::sleep};

use crate::{
    association::{Association, AssociationReceivers, AssociationState, LaneKind},
    handshake::NodeIdentity,
    lane::LaneExit,
};

use super::{
    ConnectionEvent, EndpointError, RemotingEndpoint, endpoint,
    state::{LanePhase, LaneState, lane_index},
    tasks::IoTasks,
};

/// Sole lifecycle decision maker for one association generation and its lane resources.
pub(super) struct AssociationSupervisor {
    /// Shared endpoint services accessed transiently without making the actor its lifetime owner.
    pub(super) endpoint: Weak<RemotingEndpoint>,
    /// Admission snapshot and queues used directly by messaging and socket paths.
    pub(super) association: Arc<Association>,
    /// Full peer identity fixed at actor creation and rechecked for every incoming candidate.
    pub(super) peer: NodeIdentity,
    /// Original endpoint executor; socket work must not migrate to actor-runtime worker threads.
    pub(super) executor: Handle,
    /// Cancellation and destruction ledger shared with the endpoint lifetime owner.
    pub(super) tasks: Arc<IoTasks>,
    /// Receiver owners, phases and retry tokens, indexed by Control/Interactive/Bulk order.
    pub(super) lanes: Vec<LaneState>,
    /// Whether this endpoint alone is responsible for establishing the pair's outgoing sockets.
    pub(super) dialer: bool,
}

impl AssociationSupervisor {
    /// Transfers the complete receiver group into initially parked lane slots.
    ///
    /// The endpoint must still exist and the receiver order must match its lane enumeration.
    pub(super) fn new(
        owner: Weak<RemotingEndpoint>,
        association: Arc<Association>,
        peer: NodeIdentity,
        receivers: AssociationReceivers,
        executor: Handle,
        tasks: Arc<IoTasks>,
    ) -> Self {
        let endpoint = owner
            .upgrade()
            .expect("endpoint owns supervisor construction");
        let dialer = endpoint
            .associations
            .should_dial(&peer.address, peer.incarnation);
        let queues = [receivers.control, receivers.interactive]
            .into_iter()
            .chain(receivers.bulk);
        let lanes = endpoint
            .lanes()
            .zip(queues)
            .map(|(kind, receiver)| {
                LaneState::new(kind, receiver, endpoint.config.reconnect_backoff_min)
            })
            .collect();
        Self {
            endpoint: owner,
            association,
            peer,
            executor,
            tasks,
            lanes,
            dialer,
        }
    }

    /// Checks the externally published retirement fence before accepting lifecycle work.
    pub(super) fn closed(&self) -> bool {
        matches!(
            self.association.state(),
            AssociationState::Closing | AssociationState::Closed
        )
    }

    /// Installs one scoped, reliable completion waiter for a coalesced data-lane wake.
    ///
    /// Processing each wake rearms this waiter; Notify retains a concurrent wake in the gap.
    fn arm_wake(&self, ctx: &mut ActorContext<Self>, lane: LaneKind) -> Result<(), EndpointError> {
        let association = self.association.clone();
        ctx.pipe_to_self(
            async move { association.wait_for_lane_wake(lane).await },
            move |()| ConnectionEvent::Wake(lane),
        )
        .map_err(|_| EndpointError::SupervisorUnavailable)?;
        Ok(())
    }

    /// Applies a lifecycle input synchronously, launching off-turn work where necessary.
    ///
    /// Validate operation tokens before touching returned resources or publishing attachments.
    /// Incoming rejection is replied to its setup caller without retiring a healthy actor;
    /// unrecoverable management failures propagate to the handler's retirement path.
    fn handle_event(
        &mut self,
        ctx: &mut ActorContext<Self>,
        event: ConnectionEvent,
    ) -> Result<(), EndpointError> {
        if self.closed() {
            ctx.request_stop();
            return Ok(());
        }
        match event {
            #[cfg(test)]
            ConnectionEvent::Panic => panic!("injected connection supervisor failure"),
            #[cfg(test)]
            ConnectionEvent::HoldTurn { .. } => unreachable!("handled by the test turn gate"),
            ConnectionEvent::EnsureConnected => {
                if self.dialer {
                    for index in 0..self.lanes.len() {
                        if self.lanes[index].can_start() {
                            self.start_dial(ctx, index)?;
                        }
                    }
                }
            }
            ConnectionEvent::Inbound {
                connection,
                accepted,
            } => {
                if !accepted.is_closed() {
                    // A setup caller that already timed out must not leave an orphan candidate
                    // queued for adoption. Rejection drops the event's socket and permit.
                    let result = self.accept_inbound(ctx, *connection);
                    let _ = accepted.send(result);
                }
            }
            ConnectionEvent::DialCompleted {
                lane,
                attempt,
                result,
            } => {
                let index = lane_index(lane);
                if !self.lanes[index].is_current_dial(attempt) {
                    return Ok(());
                }
                let result = result.and_then(|outcome| {
                    self.lanes[index].permit = Some(outcome.permit);
                    outcome
                        .opened
                        .and_then(|opened| self.adopt(ctx, index, attempt, *opened))
                });
                if let Err(error) = result {
                    tracing::debug!(?lane, %error, "association dial failed");
                    self.dial_failed(ctx, index)?;
                }
            }
            ConnectionEvent::LaneStopped {
                lane,
                attempt,
                nonce,
                result,
            } => {
                let index = lane_index(lane);
                if !self.lanes[index].is_current_connection(attempt, nonce) {
                    return Ok(());
                }
                // A panicked/aborted task cannot return its receiver. Retire the generation
                // instead of presenting a connected association whose queue has no consumer.
                let (lease, result) = result?;
                let slot = &mut self.lanes[index];
                slot.receiver = Some(lease.receiver);
                slot.backoff = endpoint(&self.endpoint)?.config.reconnect_backoff_min;
                if matches!(result, Ok(LaneExit::QueueClosed | LaneExit::Shutdown)) {
                    self.association.begin_close();
                    ctx.request_stop();
                    return Ok(());
                }
                if !self.dialer {
                    slot.phase = LanePhase::WaitingInbound;
                } else if matches!(result, Ok(LaneExit::Idle)) && lane != LaneKind::Control {
                    if slot.wake_requested
                        || !slot
                            .receiver
                            .as_ref()
                            .expect("returned receiver")
                            .is_empty()
                    {
                        self.schedule_retry(ctx, index)?;
                    } else {
                        slot.phase = LanePhase::Sleeping;
                    }
                } else {
                    // Preserve a reconnecting lane's connection reservation. Sleeping lanes
                    // release theirs and acquire a fresh permit when woken.
                    slot.permit = Some(lease.permit);
                    self.schedule_retry(ctx, index)?;
                }
            }
            ConnectionEvent::Wake(lane) => {
                self.arm_wake(ctx, lane)?;
                let index = lane_index(lane);
                // The I/O path can detach before its LaneStopped event is delivered. Retain
                // this wake even while the actor's phase still says Running; the exit branch
                // consults it before parking an otherwise empty receiver in Sleeping.
                if !self.association.is_lane_attached(lane) {
                    self.lanes[index].wake_requested = true;
                }
                if self.dialer && self.lanes[index].phase == LanePhase::Sleeping {
                    self.schedule_retry(ctx, index)?;
                }
            }
            ConnectionEvent::RetryDue { lane, attempt } => {
                let index = lane_index(lane);
                if self.lanes[index].phase == LanePhase::Backoff(attempt) {
                    self.start_dial(ctx, index)?;
                }
            }
            ConnectionEvent::EstablishExpired => {
                if !self.association.has_activated() {
                    self.association.begin_close();
                    ctx.request_stop();
                }
            }
        }
        Ok(())
    }
}

impl Actor for AssociationSupervisor {
    type Error = EndpointError;
    type Behavior = Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let timeout = endpoint(&self.endpoint)?.config.establishing_timeout;
        ctx.pipe_to_self(sleep(timeout), |()| ConnectionEvent::EstablishExpired)
            .map_err(|_| EndpointError::SupervisorUnavailable)?;
        for slot in &self.lanes {
            if slot.kind != LaneKind::Control {
                self.arm_wake(ctx, slot.kind)?;
            }
        }
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.association.begin_close();
        self.tasks.abort_all();
        // The actor runtime has already joined cancelled off-turn completion work. This ledger
        // covers the remaining socket futures; only afterward can parked leases be released.
        self.tasks.wait_empty().await;
        for slot in &mut self.lanes {
            slot.phase = LanePhase::Closed;
            slot.receiver.take();
            slot.permit.take();
        }
        Ok(())
    }
}

impl Handler<ConnectionEvent> for AssociationSupervisor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        event: ConnectionEvent,
    ) -> Result<(), Self::Error> {
        #[cfg(test)]
        let event = match event {
            ConnectionEvent::HoldTurn { entered, release } => {
                let _ = entered.send(());
                let _ = release.await;
                return Ok(());
            }
            event => event,
        };
        let result = self.handle_event(ctx, event);
        if result.is_err() {
            self.association.begin_close();
            ctx.request_stop();
        }
        result
    }
}

impl Drop for AssociationSupervisor {
    fn drop(&mut self) {
        // Covers actor panic and cancellation of an externally owned actor runtime.
        self.association.begin_close();
        self.tasks.abort_all();
    }
}
