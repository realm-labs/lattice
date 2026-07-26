//! Admission control for traffic entering the actor world from outside the cluster mesh.
//!
//! Everything else that reaches a node is vouched for by something the node can check on its own:
//! an exact `ActorRef` carries the incarnation and `ActivationId` it is bound to, and a logical
//! reference is resolved against a placement claim with its own deadline. Traffic arriving from a
//! gateway, an HTTP handler, or any other process edge carries none of that, so the only thing
//! that says the node should still be taking it is the node's own membership in the cluster.
//!
//! That is why external ingress is the one admission scope a lost membership session closes.
//! Cluster-internal messaging keeps flowing through [`crate::builder::LatticeService::actor_system`]
//! and through peer remoting, while the edge sheds load until the node is a full member again.
//!
//! Edges that cannot route their traffic through [`ExternalIngress::tell`] or
//! [`ExternalIngress::ask`] — a `lattice-gateway` binding decoding into an application type, for
//! instance — should call [`ExternalIngress::admit`] before dispatching so the same decision is
//! applied at the same point.

use std::time::Duration;

use lattice_actor::{
    protocol::{SupportsAsk, SupportsTell},
    recipient::{ActorSystem, RecipientError},
    traits::{Message, Request},
};
use lattice_core::actor_ref::RecipientRef;
use lattice_remoting::messaging::error::{AskError, RemoteMessageError, TellError};
use thiserror::Error;

use crate::lifecycle::{AdmissionScope, NodeAdmissionGate};

/// Rejection produced when a node is not currently accepting external traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("this node is not currently admitting external traffic")]
pub struct ExternalAdmissionClosed;

impl From<ExternalAdmissionClosed> for RecipientError {
    fn from(_: ExternalAdmissionClosed) -> Self {
        RecipientError::Tell(TellError::Remote(RemoteMessageError::Unauthorized))
    }
}

/// The node's entry point for traffic originating outside the cluster.
///
/// Cloning is cheap and shares the node's admission state, so an edge can hold one per connection.
#[derive(Clone)]
pub struct ExternalIngress {
    admission: NodeAdmissionGate,
    actor_system: ActorSystem,
}

impl ExternalIngress {
    pub(crate) fn new(admission: NodeAdmissionGate, actor_system: ActorSystem) -> Self {
        Self {
            admission,
            actor_system,
        }
    }

    /// Whether this node currently accepts new external traffic.
    pub fn is_open(&self) -> bool {
        self.admission.is_open(AdmissionScope::External)
    }

    /// Checks external admission once, for edges that dispatch through their own code path.
    pub fn admit(&self) -> Result<(), ExternalAdmissionClosed> {
        if self.is_open() {
            Ok(())
        } else {
            Err(ExternalAdmissionClosed)
        }
    }

    /// Sends a one-way message on behalf of an external caller.
    ///
    /// External admission is checked in addition to, not instead of, the scope that governs the
    /// destination itself: a message that passes here is still subject to exact-activation or
    /// placement-claim admission on the way to its target.
    pub async fn tell<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.admit()?;
        self.actor_system.tell(target, message).await
    }

    /// Sends a request on behalf of an external caller and waits for its typed response.
    pub async fn ask<P, R>(
        &self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        if !self.is_open() {
            return Err(RecipientError::Ask(AskError::Protocol(
                RemoteMessageError::Unauthorized,
            )));
        }
        self.actor_system.ask(target, request, timeout).await
    }
}
