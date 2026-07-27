use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use lattice_actor::host::ProtocolHostRegistry;
use lattice_core::{
    actor_ref::{ClusterId, NodeIncarnation},
    release::ReleaseManifest,
};
use lattice_placement::{
    coordinator::{MemberChange, MemberEvent, MemberHello, MemberRecord, MemberStatus},
    types::{CoordinatorTerm, MembershipVersion, NodeKey, Revision},
};
use lattice_remoting::{
    association::AssociationManager, config::RemotingConfig, endpoint::RemotingEndpoint,
    handshake::NodeIdentity, messaging::outbound::OutboundMessaging,
};

use crate::{
    backend::ServiceInboundDispatch,
    cluster::{members::MemberDirectory, peers::PeerReconciler},
    lifecycle::NodeAdmissionGate,
    test_support::{network_test_guard, unused_address},
};

fn member(node: NodeKey, revision: u64) -> MemberRecord {
    let version = MembershipVersion::new(
        CoordinatorTerm::new(1).unwrap(),
        Revision::new(revision).unwrap(),
    );
    MemberRecord {
        node: node.clone(),
        hello: MemberHello {
            release: ReleaseManifest::development(1),
            rollout_participant: true,
            node,
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        },
        status: MemberStatus::Up,
        version,
        lease_id: 1,
    }
}

#[tokio::test]
async fn authoritative_replacement_moves_the_address_binding_to_the_new_incarnation() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("peer-reconciler-incarnation-test").unwrap();
    let local_address = unused_address().await;
    let local_incarnation = NodeIncarnation::new(1).unwrap();
    let remote_address = unused_address().await;
    let old_incarnation = NodeIncarnation::new(2).unwrap();
    let new_incarnation = NodeIncarnation::new(3).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local_address.clone(),
            local_incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    associations
        .get_or_create(cluster_id.clone(), remote_address.clone(), old_incarnation)
        .unwrap();
    let endpoint = Arc::new(
        RemotingEndpoint::builder(
            NodeIdentity {
                cluster_id: cluster_id.clone(),
                node_id: "local".to_owned(),
                address: local_address,
                incarnation: local_incarnation,
            },
            RemotingConfig::default(),
            associations.clone(),
            Arc::new(OutboundMessaging::new(8).unwrap()),
            Arc::new(ServiceInboundDispatch {
                hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
                logical: None,
                admission: NodeAdmissionGate::closed(),
            }),
        )
        .build()
        .unwrap(),
    );
    let members = Arc::new(MemberDirectory::new(8).unwrap());
    let old = member(
        NodeKey {
            node_id: "remote".to_owned(),
            address: remote_address.clone(),
            incarnation: old_incarnation,
        },
        1,
    );
    members
        .install_snapshot(old.version, vec![old.clone()])
        .unwrap();
    let reconciler = PeerReconciler::new(cluster_id, endpoint, associations.clone(), members);
    let replacement = member(
        NodeKey {
            node_id: old.node.node_id,
            address: remote_address.clone(),
            incarnation: new_incarnation,
        },
        2,
    );

    reconciler
        .apply(MemberEvent {
            version: replacement.version,
            change: MemberChange::Upsert(Box::new(replacement)),
        })
        .await
        .unwrap();

    assert_eq!(
        associations.remote_incarnation(&remote_address),
        Some(new_incarnation)
    );
}
