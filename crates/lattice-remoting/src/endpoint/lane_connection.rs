//! One socket task's boundary with retirement and injected disconnection.
//!
//! The normal lane run performs nonce-qualified detach on return. A selected retirement branch
//! drops that run future after association registration was invalidated. Injected disconnect
//! explicitly detaches the nonce and preserves the generation for normal retry handling.

use super::{RemotingEndpoint, diagnostics::wait_for_disconnect, stream::EndpointStream};
use crate::{
    association::{Association, LaneKind},
    lane::{BidirectionalLane, LaneError, LaneExit, LaneServices},
    wire::Frame,
};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, watch};
impl RemotingEndpoint {
    /// Runs a leased receiver/socket until lane exit, generation retirement or forced disconnect.
    ///
    /// Control and Interactive failure paths fail pending asks directly rather than waiting
    /// for the management actor's completion event.
    pub(super) async fn run_lane_connection(
        &self,
        association: Arc<Association>,
        lane: LaneKind,
        nonce: u128,
        receiver: &mut Receiver<Frame>,
        stream: EndpointStream,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<LaneExit, LaneError> {
        let association_id = association.id();
        let mut disconnect = self.disconnect_tx.subscribe();
        tokio::select! {
            biased;
            () = association.wait_closed() => {
                if lane.fails_pending_asks() {
                    self.messaging.fail_association(association_id);
                }
                Ok(LaneExit::RemoteClose)
            }
            result = BidirectionalLane::new(
                association.clone(),
                lane,
                nonce,
                LaneServices::new(
                    self.messaging.clone(),
                    self.dispatch.clone(),
                    self.control_dispatch.clone(),
                ),
                self.lane_config(),
            ).run(receiver, stream, shutdown) => result,
            () = wait_for_disconnect(&mut disconnect, association_id) => {
                association.detach(lane, nonce);
                if lane.fails_pending_asks() {
                    self.messaging.fail_association(association_id);
                }
                Ok(LaneExit::RemoteClose)
            }
        }
    }
}
