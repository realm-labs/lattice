//! Control application, duplicate handling and acknowledgement generation.
//!
//! Reliable commands are previewed before application and committed only after an accepted
//! terminal result. Transient failures leave the watermark unchanged for retry. Invalid or
//! policy-rejected commands are acknowledged to prevent endless replay. Gaps and epoch changes
//! request reconciliation instead of applying an out-of-order operation.

use crate::failpoints;

use super::LaneError;
use crate::{
    association::{Association, LaneKind},
    control::{
        CommandId, ControlApply, ControlDispatch, ControlDispatchError, ReliableControlError,
        control_ack_frame, decode_control_envelope,
    },
    wire::{Frame, FrameKind},
};

pub(super) mod worker;

/// Applies one control operation or returns its protocol acknowledgement/reconciliation result.
///
/// The worker owns retries and ordering; this function performs one attempt. A duplicate is
/// acknowledged without dispatching its application effect again. Ephemeral events bypass
/// reliable sequence tracking and have no acknowledgement.
pub(super) async fn apply_control_frame(
    association: &Association,
    control_dispatch: &dyn ControlDispatch,
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
                            envelope.stream_id,
                            envelope.command_id,
                            envelope.payload.clone(),
                        )
                        .await;
                    match result {
                        Ok(()) | Err(ControlDispatchError::InvalidCommand) => {}
                        Err(ControlDispatchError::Rejected(_)) => {
                            association.record_rejected_control_command();
                        }
                        Err(error) => return Err(error.into()),
                    }
                    lattice_failpoint::hit(failpoints::CONTROL_AFTER_REMOTE_APPLY_BEFORE_ACK);
                    let ack = association.commit_control(envelope);
                    Ok(Some(control_ack_frame(ack)))
                }
                ControlApply::Duplicate(anticipated) => {
                    let ack = if association
                        .current_control_ack(envelope.stream_id)
                        .cumulative_sequence
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
                ControlApply::StreamLimit => Err(ReliableControlError::StreamLimit.into()),
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

/// Makes one best-effort attempt, dropping retryable or policy-rejected ephemeral events.
///
/// Other failures propagate to the worker. Ephemeral traffic does not enter its retry schedule.
pub(super) async fn apply_ephemeral_control_frame(
    association: &Association,
    control_dispatch: &dyn ControlDispatch,
    frame: Frame,
) -> Result<(), LaneError> {
    match apply_control_frame(association, control_dispatch, frame).await {
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

/// Decodes Interactive as byte zero and Bulk stripe `n` as byte `n + 1`.
///
/// The association validates the resulting stripe against its configured lane group.
pub(super) fn decode_lane_wake(frame: &Frame) -> Result<LaneKind, LaneError> {
    let [encoded] = frame.payload() else {
        return Err(LaneError::InvalidLaneWake);
    };
    if *encoded == 0 {
        return Ok(LaneKind::Interactive);
    }
    Ok(LaneKind::Bulk(encoded.saturating_sub(1)))
}
