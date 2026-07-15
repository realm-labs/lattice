use std::sync::Arc;

use lattice_placement::coordinator::{MemberChange, MemberEvent, MemberRecord, MemberStatus};
use lattice_placement::types::NodeKey;
use lattice_placement::types::StateVersion;
use lattice_remoting::association::{Association, AssociationManager, AssociationState};
use lattice_remoting::endpoint::{EndpointError, RemotingEndpoint};
use lattice_remoting::handshake::NodeIdentity;
use thiserror::Error;

use super::members::MemberDirectory;

pub struct PeerReconciler {
    cluster_id: lattice_core::actor_ref::ClusterId,
    endpoint: Arc<RemotingEndpoint>,
    associations: Arc<AssociationManager>,
    members: Arc<MemberDirectory>,
}

impl PeerReconciler {
    pub fn new(
        cluster_id: lattice_core::actor_ref::ClusterId,
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
            .snapshot()
            .members
            .into_iter()
            .find(|record| record.node == *node && record.status == MemberStatus::Up)
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
        version: StateVersion,
        members: Vec<MemberRecord>,
    ) -> Result<(), PeerError> {
        self.members
            .install_snapshot(version, members.clone())
            .map_err(PeerError::Directory)?;
        self.connect_authoritative_members(&members).await;
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
        let connect = match &event.change {
            MemberChange::Upsert(record) if record.status == MemberStatus::Up => {
                Some(record.as_ref().clone())
            }
            _ => None,
        };
        self.members.apply(event).map_err(PeerError::Directory)?;
        if let Some(member) = connect {
            self.connect_authoritative_members(&[member]).await;
        }
        Ok(())
    }

    async fn connect_authoritative_members(&self, members: &[MemberRecord]) {
        for member in members {
            if member.status != MemberStatus::Up
                || !self
                    .associations
                    .should_dial(&member.node.address, member.node.incarnation)
            {
                continue;
            }
            if let Err(error) = self.connect(&member.node).await {
                tracing::warn!(
                    target: "lattice.cluster.peers",
                    %error,
                    node_id = %member.node.node_id,
                    "failed to reconcile an authoritative peer association"
                );
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("peer is not an exact authoritative Up member")]
    NotAuthoritativeUp,
    #[error("authoritative member directory rejected an event")]
    Directory(#[source] super::members::MemberDirectoryError),
    #[error("peer endpoint failed: {0}")]
    Endpoint(#[source] EndpointError),
}
