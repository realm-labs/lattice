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
            .map_err(PeerError::Directory)?;
        // A session that was down long enough to be caught up by a snapshot rather than by deltas
        // can have missed the removal of an incarnation entirely, so the snapshot the directory
        // accepted, rather than the one that was offered, is what the bindings are reconciled with.
        for member in self.members.snapshot().members {
            self.adopt_authoritative_incarnation(&member.node);
        }
        Ok(())
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
        let change = match &event.change {
            MemberChange::Removed { node, .. } => Ok(node.clone()),
            MemberChange::Upsert(record) => Err(record.node.clone()),
        };
        self.members.apply(event).map_err(PeerError::Directory)?;
        match change {
            Ok(retired) => {
                self.associations
                    .forget_remote_incarnation(&retired.address, retired.incarnation);
            }
            Err(node) => self.adopt_authoritative_incarnation(&node),
        }
        Ok(())
    }

    /// Binds a peer's address to the incarnation the authoritative directory now names.
    ///
    /// Remoting pins one incarnation per address so that an old or unreconciled one cannot take an
    /// address over from the incarnation the cluster believes in, and the pin is created by
    /// whichever incarnation this node first spoke to at that address. A node that restarts keeps
    /// its address and takes a new incarnation, so unless the pin follows the directory that has
    /// already accepted the successor, every association with the restarted node is refused as an
    /// old or unreconciled incarnation for as long as this process lives. Membership is the
    /// authority on which incarnation owns an address, so the pin follows membership.
    fn adopt_authoritative_incarnation(&self, node: &NodeKey) {
        if self
            .associations
            .remote_incarnation(&node.address)
            .is_none_or(|pinned| pinned == node.incarnation)
        {
            return;
        }
        self.associations
            .replace_remote_incarnation(node.address.clone(), node.incarnation);
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
