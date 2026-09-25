#[cfg(any(feature = "tls", test))]
use super::AssociationError;
use super::{Association, AssociationState, LaneKind, NodeIdentity};

impl Association {
    /// Returns the certificate-verified peer of the currently attached control
    /// connection. An address, handshake name, or authenticated data lane alone
    /// is not sufficient evidence for privileged control-plane administration.
    ///
    /// Detached or replaced lanes invalidate this observation. Call at request
    /// authorization time; do not cache it as a permanent administrative grant.
    pub fn authenticated_control_peer(&self) -> Option<NodeIdentity> {
        if matches!(
            self.state(),
            AssociationState::Closing | AssociationState::Closed
        ) {
            return None;
        }
        let inner = self.inner.lock().expect("association state poisoned");
        let (nonce, peer) = inner.authenticated_control_peer.as_ref()?;
        (inner.lanes.get(&LaneKind::Control) == Some(nonce)).then(|| peer.clone())
    }

    /// Only the TLS endpoint may record this, after certificate and handshake
    /// validation and successful ownership of this exact control connection.
    #[cfg(any(feature = "tls", test))]
    pub(crate) fn record_authenticated_control_peer(
        &self,
        nonce: u128,
        peer: NodeIdentity,
    ) -> Result<(), AssociationError> {
        let mut inner = self.inner.lock().expect("association state poisoned");
        if peer.cluster_id != self.key.cluster_id
            || peer.address != self.key.remote_address
            || peer.incarnation != self.key.remote_incarnation
            || inner.lanes.get(&LaneKind::Control) != Some(&nonce)
            || inner
                .authenticated_peer_identity
                .as_ref()
                .is_some_and(|previous| previous != &peer)
        {
            return Err(AssociationError::AuthenticatedPeerMismatch);
        }
        inner.authenticated_peer_identity = Some(peer.clone());
        inner.authenticated_control_peer = Some((nonce, peer));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use lattice_model::cluster::{ClusterId, NodeEndpoint, NodeIncarnation};

    use crate::{
        association::{Association, AssociationKey, LaneAttachment, LaneKind},
        config::RemotingConfig,
        handshake::NodeIdentity,
    };

    #[test]
    fn authentication_is_bound_to_the_control_connection_and_exact_identity() {
        let peer = NodeIdentity {
            cluster_id: ClusterId::new("auth-test").unwrap(),
            node_id: "admin".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 32102).unwrap(),
            incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let association = Association::new(
            AssociationKey {
                cluster_id: peer.cluster_id.clone(),
                local_incarnation: NodeIncarnation::new(1).unwrap(),
                remote_address: peer.address.clone(),
                remote_incarnation: peer.incarnation,
            },
            RemotingConfig::default(),
        )
        .unwrap();
        assert!(association.authenticated_control_peer().is_none());
        assert!(
            association
                .record_authenticated_control_peer(1, peer.clone())
                .is_err()
        );
        association
            .attach_and_replay(LaneAttachment {
                association_id: association.id(),
                key: association.key().clone(),
                lane: LaneKind::Control,
                connection_nonce: 1,
            })
            .unwrap();
        assert!(
            association.authenticated_control_peer().is_none(),
            "plain attachment is not authentication"
        );
        association
            .record_authenticated_control_peer(1, peer.clone())
            .unwrap();
        assert_eq!(association.authenticated_control_peer(), Some(peer.clone()));
        association.detach(LaneKind::Control, 1);
        assert!(association.authenticated_control_peer().is_none());
        // A peer controls its nonce. Reusing an old value must not restore TLS proof.
        association
            .attach_and_replay(LaneAttachment {
                association_id: association.id(),
                key: association.key().clone(),
                lane: LaneKind::Control,
                connection_nonce: 1,
            })
            .unwrap();
        assert!(association.authenticated_control_peer().is_none());
        association.detach(LaneKind::Control, 1);
        association
            .attach_and_replay(LaneAttachment {
                association_id: association.id(),
                key: association.key().clone(),
                lane: LaneKind::Control,
                connection_nonce: 2,
            })
            .unwrap();
        assert!(
            association.authenticated_control_peer().is_none(),
            "old TLS proof cannot bless a replacement lane"
        );
        assert!(
            association
                .record_authenticated_control_peer(1, peer.clone())
                .is_err()
        );
        let mut impostor = peer.clone();
        impostor.node_id = "another-admin".to_owned();
        assert!(
            association
                .record_authenticated_control_peer(2, impostor)
                .is_err()
        );
        association
            .record_authenticated_control_peer(2, peer.clone())
            .unwrap();
        assert_eq!(association.authenticated_control_peer(), Some(peer));
    }
}
