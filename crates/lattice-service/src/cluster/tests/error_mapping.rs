//! Mapping of actor dispatch failures onto the remote messaging error surface.

use crate::cluster::*;

#[test]
fn actor_panic_dispatch_maps_to_remote_actor_panicked() {
    assert_eq!(
        map_dispatch(DispatchError::Actor(ActorCallError::ActorPanicked)),
        RemoteMessageError::ActorPanicked
    );
}
