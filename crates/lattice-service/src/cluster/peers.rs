use std::sync::Arc;

use lattice_core::actor_ref::ClusterId;
use lattice_placement::{
    coordinator::{MemberChange, MemberEvent, MemberRecord},
    types::{MembershipVersion, NodeKey},
};
use lattice_remoting::{
    association::{Association, AssociationManager, AssociationState},
    endpoint::{EndpointError, RemotingEndpoint},
    handshake::NodeIdentity,
};
use thiserror::Error;

use super::members::{MemberDirectory, MemberDirectoryError};

pub struct PeerReconciler {
    cluster_id: ClusterId,
    endpoint: Arc<RemotingEndpoint>,
    associations: Arc<AssociationManager>,
    members: Arc<MemberDirectory>,
}

impl PeerReconciler {
    pub fn new(
        cluster_id: ClusterId,
        endpoint: Arc<RemotingEndpoint>,
        associations: Arc<AssociationManager>,
        members: Arc<MemberDirectory>,
    ) -> Self {
        Self {
            cluster_id,
            endpoint,
            associations,
            members,
        }
    }

    pub async fn connect(&self, node: &NodeKey) -> Result<Arc<Association>, PeerError> {
        let authoritative = self
            .members
            .lookup_up(node)
            .ok_or(PeerError::NotAuthoritativeUp)?;
        if let Some(association) = self.associations.get_exact(
            &self.cluster_id,
            &authoritative.node.address,
            authoritative.node.incarnation,
        ) && association.state() == AssociationState::Active
        {
            return Ok(association);
        }
        self.endpoint
            .connect_peer(NodeIdentity {
                cluster_id: self.cluster_id.clone(),
                node_id: authoritative.node.node_id,
                address: authoritative.node.address,
                incarnation: authoritative.node.incarnation,
            })
            .await
            .map_err(PeerError::Endpoint)
    }

    pub async fn install_snapshot(
        &self,
        version: MembershipVersion,
        members: Vec<MemberRecord>,
    ) -> Result<(), PeerError> {
        self.members
            .install_snapshot(version, members)
            .map_err(PeerError::Directory)
    }

    pub async fn apply(&self, event: MemberEvent) -> Result<(), PeerError> {
        if let MemberChange::Removed { node, .. } = &event.change
            && let Some(association) =
                self.associations
                    .get_exact(&self.cluster_id, &node.address, node.incarnation)
        {
            association.begin_close();
            let _ = self.endpoint.disconnect_association(association.id());
            association.finish_close();
            self.associations
                .remove(association.key(), association.id());
        }
        self.members.apply(event).map_err(PeerError::Directory)
    }
}

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("peer is not an exact authoritative Up member")]
    NotAuthoritativeUp,
    #[error("authoritative member directory rejected an event")]
    Directory(#[source] MemberDirectoryError),
    #[error("peer endpoint failed: {0}")]
    Endpoint(#[source] EndpointError),
}
