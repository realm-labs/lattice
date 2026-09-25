//! Remote management authorization. Transport authentication and administrative
//! permission are separate: being a candidate or discoverable node grants neither
//! shutdown permission nor permission to change the candidate set.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    candidates::{CandidateChange, CandidateChangeResult},
    storage::{StorageError, candidates::CandidateStore},
};
use lattice_model::cluster::{ClusterId, CoordinatorScope};
use lattice_remoting::{association::Association, handshake::NodeIdentity};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminAction {
    ManageCandidates(CoordinatorScope),
    ShutdownCluster,
}

/// The grants for one stable certificate node ID within one exact cluster.
/// Runtime incarnation changes do not require editing the allowlist; the current
/// incarnation is still authenticated and retained in the authorization result.
#[derive(Debug, Clone, Default)]
pub struct AdminPermissions {
    pub candidate_scopes: BTreeSet<CoordinatorScope>,
    pub shutdown_cluster: bool,
}

#[derive(Debug, Clone)]
pub struct AdministratorAllowlist {
    cluster: ClusterId,
    administrators: BTreeMap<String, AdminPermissions>,
}

impl AdministratorAllowlist {
    /// An empty allowlist denies every remote administrative operation.
    pub fn new(cluster: ClusterId, administrators: BTreeMap<String, AdminPermissions>) -> Self {
        Self {
            cluster,
            administrators,
        }
    }

    /// Authenticate from the live control connection, never a request's supplied
    /// name/address. Plaintext associations are always denied, including in tests
    /// and development deployments; offline reset uses a separate credential path.
    pub fn authorize(
        &self,
        association: &Association,
        action: AdminAction,
    ) -> Result<AdminAuthorization, AdminAuthorizationError> {
        let peer = association
            .authenticated_control_peer()
            .ok_or(AdminAuthorizationError::Unauthenticated)?;
        self.authorize_peer(peer, action)
    }

    fn authorize_peer(
        &self,
        peer: NodeIdentity,
        action: AdminAction,
    ) -> Result<AdminAuthorization, AdminAuthorizationError> {
        if peer.cluster_id != self.cluster {
            return Err(AdminAuthorizationError::Forbidden);
        }
        let permissions = self
            .administrators
            .get(&peer.node_id)
            .ok_or(AdminAuthorizationError::Forbidden)?;
        let allowed = match &action {
            AdminAction::ManageCandidates(scope) => permissions.candidate_scopes.contains(scope),
            AdminAction::ShutdownCluster => permissions.shutdown_cluster,
        };
        if !allowed {
            return Err(AdminAuthorizationError::Forbidden);
        }
        Ok(AdminAuthorization { peer, action })
    }
}

/// An in-process, request-scoped authorization result, intentionally not
/// deserializable or publicly constructible. It is not a portable credential and
/// does not replace the store's epoch, leader and candidate-generation checks.
#[derive(Debug)]
pub struct AdminAuthorization {
    peer: NodeIdentity,
    action: AdminAction,
}

impl AdminAuthorization {
    pub fn peer(&self) -> &NodeIdentity {
        &self.peer
    }
    pub fn action(&self) -> &AdminAction {
        &self.action
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AdminAuthorizationError {
    #[error("remote administration requires a live mTLS-authenticated control connection")]
    Unauthenticated,
    #[error("authenticated node is not authorized for this cluster administration action")]
    Forbidden,
}

#[cfg(test)]
mod tests {
    use super::{AdminAction, AdminAuthorizationError, AdminPermissions, AdministratorAllowlist};
    use lattice_model::cluster::{
        ActorGroupId, ClusterId, CoordinatorScope, NodeEndpoint, NodeIncarnation,
    };
    use lattice_remoting::{
        association::{Association, AssociationKey},
        config::RemotingConfig,
        handshake::NodeIdentity,
    };
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn permission_is_exactly_cluster_scope_and_action_bound() {
        let cluster = ClusterId::new("admin-test").unwrap();
        let group = CoordinatorScope::Group(ActorGroupId::new("gameplay").unwrap());
        let allowlist = AdministratorAllowlist::new(
            cluster.clone(),
            BTreeMap::from([(
                "operator".to_owned(),
                AdminPermissions {
                    candidate_scopes: BTreeSet::from([group.clone()]),
                    shutdown_cluster: false,
                },
            )]),
        );
        let peer = NodeIdentity {
            cluster_id: cluster,
            node_id: "operator".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 32103).unwrap(),
            incarnation: NodeIncarnation::new(2).unwrap(),
        };
        assert!(
            allowlist
                .authorize_peer(peer.clone(), AdminAction::ManageCandidates(group.clone()))
                .is_ok()
        );
        assert!(matches!(
            allowlist.authorize_peer(peer.clone(), AdminAction::ShutdownCluster),
            Err(AdminAuthorizationError::Forbidden)
        ));
        assert!(
            allowlist
                .authorize_peer(
                    peer.clone(),
                    AdminAction::ManageCandidates(CoordinatorScope::Cluster)
                )
                .is_err()
        );
        let mut other_cluster = peer.clone();
        other_cluster.cluster_id = ClusterId::new("foreign").unwrap();
        assert!(
            allowlist
                .authorize_peer(other_cluster, AdminAction::ManageCandidates(group.clone()))
                .is_err()
        );
        let plain = Association::new(
            AssociationKey {
                cluster_id: peer.cluster_id,
                remote_address: peer.address,
                remote_incarnation: peer.incarnation,
                local_incarnation: NodeIncarnation::new(1).unwrap(),
            },
            RemotingConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            allowlist.authorize(&plain, AdminAction::ManageCandidates(group)),
            Err(AdminAuthorizationError::Unauthenticated)
        ));
    }
}
/// Apply one explicitly authorized scope mutation. Store credentials are a
/// privileged deployment boundary; remote callers must obtain this non-forgeable
/// permission from the live mTLS association for every request.
pub async fn change_candidate<S: CandidateStore>(
    store: &S,
    authorization: AdminAuthorization,
    request: CandidateChange,
) -> Result<CandidateChangeResult, AdminOperationError> {
    if authorization.action() != &AdminAction::ManageCandidates(request.scope.clone()) {
        return Err(AdminAuthorizationError::Forbidden.into());
    }
    Ok(store.change_candidate(request).await?)
}

#[derive(Debug, Error)]
pub enum AdminOperationError {
    #[error(transparent)]
    Authorization(#[from] AdminAuthorizationError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}
