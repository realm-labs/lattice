use std::sync::Arc;

use lattice_core::failpoint::Failpoint;

use super::LaneError;
use crate::{
    association::{Association, LaneKind},
    control::{
        CommandId, ControlApply, ControlDispatch, ControlDispatchError, control_ack_frame,
        decode_control_envelope,
    },
    wire::{Frame, FrameKind},
};

pub(super) struct ControlWorkerGuard(pub(super) tokio::task::JoinHandle<()>);

impl Drop for ControlWorkerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn apply_control_frame(
    association: Arc<Association>,
    control_dispatch: Arc<dyn ControlDispatch>,
    frame: Frame,
) -> Result<Option<Frame>, LaneError> {
    match frame.kind {
        FrameKind::ControlEnvelope => {
            let envelope = decode_control_envelope(&frame)?;
            match association.preview_control(&envelope) {
                ControlApply::Apply(_) => {
                    let result = control_dispatch
                        .apply(
                            association.key().clone(),
                            envelope.command_id,
                            envelope.payload.clone(),
                        )
                        .await;
                    match result {
                        Ok(())
                        | Err(ControlDispatchError::InvalidCommand) => {}
                        Err(ControlDispatchError::Rejected(_)) => {
                            association.record_rejected_control_command();
                        }
                        Err(error) => return Err(error.into()),
                    }
                    lattice_core::failpoint::hit(Failpoint::ControlAfterRemoteApplyBeforeAck);
                    let ack = association.commit_control(envelope);
                    Ok(Some(control_ack_frame(ack)))
                }
                ControlApply::Duplicate(anticipated) => {
                    let ack = if association.current_control_ack().cumulative_sequence
                        < anticipated.cumulative_sequence
                    {
                        association.commit_control(envelope)
                    } else {
                        anticipated
                    };
                    Ok(Some(control_ack_frame(ack)))
                }
                ControlApply::Gap(gap) => {
                    control_dispatch
                        .reconcile(association.key().clone(), Some(gap))
                        .await?;
                    Ok(None)
                }
                ControlApply::ReconcileEpoch => {
                    control_dispatch
                        .reconcile(association.key().clone(), None)
                        .await?;
                    Ok(None)
                }
            }
        }
        FrameKind::CoordinatorEvent => {
            control_dispatch
                .apply_ephemeral(
                    association.key().clone(),
                    CommandId::generate(),
                    frame.into_payload(),
                )
                .await?;
            Ok(None)
        }
        _ => Err(LaneError::UnexpectedControlWork),
    }
}

pub(super) async fn apply_ephemeral_control_frame(
    association: Arc<Association>,
    control_dispatch: Arc<dyn ControlDispatch>,
    frame: Frame,
) -> Result<(), LaneError> {
    match apply_control_frame(association.clone(), control_dispatch, frame).await {
        Ok(_) => Ok(()),
        Err(error @ LaneError::ControlDispatch(ControlDispatchError::RetryLater(_)))
        | Err(error @ LaneError::ControlDispatch(ControlDispatchError::Rejected(_))) => {
            association.record_dropped_ephemeral_control();
            tracing::debug!(
                target: "lattice_remoting::control",
                association_id = association.id().get(),
                error = %error,
                "dropping ephemeral coordinator event"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn decode_lane_wake(frame: &Frame) -> Result<LaneKind, LaneError> {
    let [encoded] = frame.payload() else {
        return Err(LaneError::InvalidLaneWake);
    };
    if *encoded == 0 {
        return Ok(LaneKind::Interactive);
    }
    Ok(LaneKind::Bulk(encoded.saturating_sub(1)))
}
