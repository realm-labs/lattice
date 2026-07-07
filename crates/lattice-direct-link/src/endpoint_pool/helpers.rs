use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::errors::LinkError;
use lattice_core::direct_link::ids::LinkId;
use lattice_core::direct_link::runtime::DirectLinkOpenRequest;
use lattice_core::direct_link::target::{DirectLinkEndpoint, LinkTarget};

pub(crate) fn endpoint_and_target(
    request: &DirectLinkOpenRequest,
) -> Result<(DirectLinkEndpoint, ActorRef), LinkError> {
    match &request.target {
        LinkTarget::Endpoint { endpoint, target } => Ok((endpoint.clone(), target.clone())),
        LinkTarget::Actor(_) => Err(LinkError::Protocol(
            "direct link ActorRef targets require placement endpoint resolution".to_string(),
        )),
    }
}

pub(crate) fn reject_reason_to_error(reason: crate::session::OpenLinkRejectReason) -> LinkError {
    match reason {
        crate::session::OpenLinkRejectReason::NotOwner => LinkError::NotOwner { redirect: None },
        crate::session::OpenLinkRejectReason::Fenced => LinkError::Fenced,
        crate::session::OpenLinkRejectReason::ActorUnavailable => LinkError::ActorUnavailable,
        crate::session::OpenLinkRejectReason::UnsupportedStream => LinkError::UnsupportedStream,
        crate::session::OpenLinkRejectReason::UnsupportedMessageType => {
            LinkError::UnsupportedMessageType
        }
        crate::session::OpenLinkRejectReason::Unauthorized => LinkError::Unauthorized,
        crate::session::OpenLinkRejectReason::Overloaded => LinkError::Overloaded,
        crate::session::OpenLinkRejectReason::ProtocolVersionMismatch => {
            LinkError::ProtocolVersionMismatch
        }
    }
}

pub(crate) fn stable_stripe_index(link_id: &LinkId, stripe_count: NonZeroUsize) -> usize {
    let mut hasher = DefaultHasher::new();
    link_id.hash(&mut hasher);
    (hasher.finish() as usize) % stripe_count.get()
}
