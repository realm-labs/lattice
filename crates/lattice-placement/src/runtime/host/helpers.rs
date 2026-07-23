use broadcast::error::RecvError;
use lattice_remoting::control::ControlDispatchError;
use tokio::sync::broadcast;

use super::CoordinatorRuntimeError;
use crate::{coordinator::MemberEvent, runtime::membership::control_dispatch_error};

pub(super) async fn next_membership_event(
    events: &mut Option<broadcast::Receiver<MemberEvent>>,
) -> Result<MemberEvent, RecvError> {
    events
        .as_mut()
        .expect("membership event branch requires a receiver")
        .recv()
        .await
}

pub(super) fn dispatch_error(error: CoordinatorRuntimeError) -> ControlDispatchError {
    control_dispatch_error(&error)
}
