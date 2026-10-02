//! Actor decisions launch bounded work and adopt its results; no socket I/O runs in a turn.

use lattice_actor::context::ActorContext;
use tokio::time::{Instant, sleep};

use crate::association::{AssociationError, LaneAttachment, LaneKind};

use super::{
    ConnectionEvent, DialOutcome, EndpointError, InboundConnection,
    actor::AssociationSupervisor,
    endpoint,
    state::{LaneLease, LanePhase, lane_index},
};
use crate::endpoint::connection::OpenedLane;

impl AssociationSupervisor {
    pub(super) fn start_dial(
        &mut self,
        ctx: &mut ActorContext<Self>,
        index: usize,
    ) -> Result<(), EndpointError> {
        let owner = endpoint(&self.endpoint)?;
        owner.ensure_running()?;
        let slot = &mut self.lanes[index];
        slot.keep_permit_on_failure = slot.permit.is_some();
        if slot.permit.is_none() {
            match owner.connections.clone().try_acquire_owned() {
                Ok(permit) => slot.permit = Some(permit),
                Err(_) => {
                    slot.backoff = slot
                        .backoff
                        .saturating_mul(2)
                        .min(owner.config.reconnect_backoff_max);
                    return self.schedule_retry(ctx, index);
                }
            }
        }
        let attempt = slot.attempt();
        let lane = slot.kind;
        slot.phase = LanePhase::Dialing(attempt);
        let association = self.association.clone();
        let peer = self.peer.clone();
        let permit = slot
            .permit
            .take()
            .expect("dial reservation before socket work");
        let task = self.tasks.spawn(&self.executor, async move {
            let opened = owner
                .open_outbound_lane(&association, &peer, lane)
                .await
                .map(Box::new);
            DialOutcome { opened, permit }
        });
        ctx.pipe_to_self(task.join(), move |result| ConnectionEvent::DialCompleted {
            lane,
            attempt,
            result: result.map_err(EndpointError::Join),
        })
        .map_err(|_| EndpointError::SupervisorUnavailable)?;
        Ok(())
    }

    pub(super) fn schedule_retry(
        &mut self,
        ctx: &mut ActorContext<Self>,
        index: usize,
    ) -> Result<(), EndpointError> {
        let slot = &mut self.lanes[index];
        let attempt = slot.attempt();
        let lane = slot.kind;
        slot.phase = LanePhase::Backoff(attempt);
        // Unlike notify_after, pipe_to_self waits for mailbox capacity. A dropped retry tick
        // must never strand a lane with queued messages and no future connection attempt.
        ctx.pipe_to_self(sleep(slot.backoff), move |()| ConnectionEvent::RetryDue {
            lane,
            attempt,
        })
        .map_err(|_| EndpointError::SupervisorUnavailable)?;
        Ok(())
    }

    pub(super) fn dial_failed(
        &mut self,
        ctx: &mut ActorContext<Self>,
        index: usize,
    ) -> Result<(), EndpointError> {
        let maximum = endpoint(&self.endpoint)?.config.reconnect_backoff_max;
        let slot = &mut self.lanes[index];
        if !slot.keep_permit_on_failure {
            slot.permit.take();
        }
        slot.backoff = slot.backoff.saturating_mul(2).min(maximum);
        self.schedule_retry(ctx, index)
    }

    pub(super) fn accept_inbound(
        &mut self,
        ctx: &mut ActorContext<Self>,
        connection: InboundConnection,
    ) -> Result<(), EndpointError> {
        if Instant::now() >= connection.deadline {
            return Err(EndpointError::InboundSetupTimeout);
        }
        // The identity authenticated during setup must be the same identity that this actor
        // later publishes for Control. A data lane cannot substitute a different node name.
        if connection.handshake.source != self.peer
            || connection.handshake.association_id != self.association.id()
        {
            return Err(AssociationError::IdentityMismatch.into());
        }
        let index = lane_index(connection.handshake.lane);
        let slot = &mut self.lanes[index];
        if !slot.can_start() {
            return Err(EndpointError::LaneAlreadyRunning(slot.kind));
        }
        let attempt = slot.attempt();
        slot.permit = Some(connection.permit);
        let result = self.adopt(
            ctx,
            index,
            attempt,
            OpenedLane {
                stream: connection.stream,
                nonce: connection.handshake.connection_nonce,
                peer_catalogue: connection.peer_catalogue,
                authenticated: connection.authenticated,
            },
        );
        if result.is_err() {
            self.lanes[index].permit.take();
        }
        result
    }

    pub(super) fn adopt(
        &mut self,
        ctx: &mut ActorContext<Self>,
        index: usize,
        attempt: u64,
        opened: OpenedLane,
    ) -> Result<(), EndpointError> {
        let owner = endpoint(&self.endpoint)?;
        owner.ensure_running()?;
        if self.closed() {
            return Err(AssociationError::Closed.into());
        }
        let lane = self.lanes[index].kind;
        if lane == LaneKind::Control {
            self.association
                .install_peer_catalogue(opened.peer_catalogue)?;
        }
        let attachment = LaneAttachment {
            association_id: self.association.id(),
            key: self.association.key().clone(),
            lane,
            connection_nonce: opened.nonce,
        };
        // The actor already owns the receiver. Attachment, identity publication and starting
        // its consumer are one turn; any error rolls the attachment back before yielding.
        let registered = self
            .association
            .attach_and_replay(attachment)
            .and_then(|_| {
                if lane == LaneKind::Control && opened.authenticated {
                    #[cfg(any(feature = "tls", test))]
                    self.association
                        .record_authenticated_control_peer(opened.nonce, self.peer.clone())?;
                }
                Ok(())
            });
        if let Err(error) = registered {
            self.association.detach(lane, opened.nonce);
            return Err(error.into());
        }
        let slot = &mut self.lanes[index];
        let lease = LaneLease {
            receiver: slot.receiver.take().expect("one receiver per managed lane"),
            permit: slot
                .permit
                .take()
                .expect("connection reservation before adoption"),
        };
        slot.backoff = owner.config.reconnect_backoff_min;
        slot.wake_requested = false;
        slot.phase = LanePhase::Running {
            attempt,
            nonce: opened.nonce,
        };
        let association = self.association.clone();
        let nonce = opened.nonce;
        let task = self.tasks.spawn(&self.executor, async move {
            let mut lease = lease;
            let mut shutdown = owner.shutdown_tx.subscribe();
            let result = owner
                .run_lane_connection(
                    association,
                    lane,
                    nonce,
                    &mut lease.receiver,
                    opened.stream,
                    &mut shutdown,
                )
                .await;
            (lease, result)
        });
        if ctx
            .pipe_to_self(task.join(), move |result| ConnectionEvent::LaneStopped {
                lane,
                attempt,
                nonce,
                result: result.map_err(EndpointError::Join),
            })
            .is_err()
        {
            self.association.begin_close();
            ctx.request_stop();
            return Err(EndpointError::SupervisorUnavailable);
        }
        Ok(())
    }
}
