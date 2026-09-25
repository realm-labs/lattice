//! Exercise the live authenticated control lane, not a fabricated authorization token.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_coordination::{
    administration::{AdminPermissions, AdministratorAllowlist},
    candidates::CandidateChange,
    control::{
        DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter,
        control_stream_id, encode_control_command,
    },
    runtime::host::{CoordinatorHost, CoordinatorHostConfig},
    storage::{
        CoordinatorLeaseStore, InMemoryCoordinationStore,
        candidates::{CandidateStore, provision_candidate},
    },
    types::NodeKey,
};
use lattice_model::{
    cluster::{ActorGroupId, ClusterId, CoordinatorScope, NodeIncarnation},
    run::ControlOperationId,
};
use lattice_remoting::{
    association::AssociationKey,
    control::{CommandId, ControlDispatch, ControlDispatchError, ControlGap, ControlStreamId},
    handshake::NodeIdentity,
};
use tokio::sync::mpsc;

use super::{cluster_shutdown::tls, support::node_config};
use crate::{
    builder::LatticeService,
    test_support::{network_test_guard, unused_address},
};

struct ObservedDispatch {
    inner: Arc<PlacementControlRouter>,
    outcomes: mpsc::UnboundedSender<Result<(), ControlDispatchError>>,
}

#[async_trait]
impl ControlDispatch for ObservedDispatch {
    async fn apply(
        &self,
        association: AssociationKey,
        stream: ControlStreamId,
        command: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        let result = self
            .inner
            .apply(association, stream, command, payload)
            .await;
        self.outcomes.send(result.clone()).unwrap();
        result
    }

    async fn reconcile(
        &self,
        association: AssociationKey,
        gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        self.inner.reconcile(association, gap).await
    }
}

#[derive(Clone, Copy)]
enum Caller {
    Authorized,
    Unlisted,
    WrongScope,
    Plaintext,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn candidate_management_requires_live_mtls_and_exact_scope_permission() {
    let _network = network_test_guard().await;
    for caller in [
        Caller::Authorized,
        Caller::Unlisted,
        Caller::WrongScope,
        Caller::Plaintext,
    ] {
        verify_caller(caller).await;
    }
}

async fn verify_caller(caller: Caller) {
    let cluster = ClusterId::new("candidate-administration").unwrap();
    let server = NodeIdentity {
        cluster_id: cluster.clone(),
        node_id: "coordinator".to_owned(),
        address: unused_address().await,
        incarnation: NodeIncarnation::new(71).unwrap(),
    };
    let client = NodeIdentity {
        cluster_id: cluster.clone(),
        node_id: "operator".to_owned(),
        address: unused_address().await,
        incarnation: NodeIncarnation::new(72).unwrap(),
    };
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    let epoch = store.ensure_framework().await.unwrap();
    provision_candidate(
        store.as_ref(),
        CoordinatorScope::Cluster,
        server.node_id.clone(),
    )
    .await
    .unwrap();
    let permissions = if matches!(caller, Caller::Unlisted) {
        BTreeMap::new()
    } else {
        BTreeMap::from([(
            client.node_id.clone(),
            AdminPermissions {
                candidate_scopes: BTreeSet::from([CoordinatorScope::Cluster]),
                shutdown_cluster: false,
            },
        )])
    };
    let mut server_builder = LatticeService::builder(node_config(
        cluster.clone(),
        &server.node_id,
        server.address.clone(),
        server.incarnation,
    ))
    .unwrap();
    let mut client_builder = LatticeService::builder(node_config(
        cluster.clone(),
        &client.node_id,
        client.address.clone(),
        client.incarnation,
    ))
    .unwrap();
    if !matches!(caller, Caller::Plaintext) {
        let (server_security, client_security) = tls::pair(&server, &client);
        server_builder = server_builder.endpoint_security(server_security);
        client_builder = client_builder.endpoint_security(client_security);
    }
    let host = CoordinatorHost::elect(
        store.clone(),
        server_builder.association_manager(),
        NodeKey {
            node_id: server.node_id.clone(),
            address: server.address.clone(),
            incarnation: server.incarnation,
        },
        BTreeSet::new(),
        CoordinatorHostConfig {
            administrators: Some(AdministratorAllowlist::new(cluster, permissions)),
            renewal_interval: Duration::from_millis(50),
            election_interval: Duration::from_millis(50),
            maximum_candidate_jitter: Duration::ZERO,
            ..CoordinatorHostConfig::default()
        },
    )
    .await
    .unwrap();
    let (dispatch, controls) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let dispatch = Arc::new(dispatch);
    let (outcomes_tx, mut outcomes) = mpsc::unbounded_channel();
    let coordinator = server_builder
        .coordinator_host(dispatch.clone(), host, controls)
        .control_dispatch(Arc::new(ObservedDispatch {
            inner: dispatch,
            outcomes: outcomes_tx,
        }))
        .build()
        .unwrap();
    let (responses, mut responses_rx) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let response_task = tokio::spawn(async move {
        while let Some(event) = responses_rx.recv().await {
            event.complete(Ok(()));
        }
    });
    let operator = client_builder
        .control_dispatch(Arc::new(responses))
        .build()
        .unwrap();
    coordinator.start().await.unwrap();
    operator.start().await.unwrap();
    let association = operator.connect_peer(server).await.unwrap();
    let scope = if matches!(caller, Caller::WrongScope) {
        CoordinatorScope::Group(ActorGroupId::new("not-authorized").unwrap())
    } else {
        CoordinatorScope::Cluster
    };
    let before = store.candidate_set(&scope).await.unwrap();
    let mut request = CandidateChange {
        epoch,
        scope: scope.clone(),
        expected_revision: before.revision,
        node_id: "standby".to_owned(),
        enabled: true,
        operation: ControlOperationId::new("remote-enable").unwrap(),
    };
    for enable in [true, false] {
        request.enabled = enable;
        request.operation = ControlOperationId::new(if enable {
            "remote-enable"
        } else {
            "remote-revoke"
        })
        .unwrap();
        let payload = encode_control_command(
            &scope,
            &PlacementControlCommand::ChangeCandidate(request.clone()),
            DEFAULT_MAX_CONTROL_PAYLOAD,
        )
        .unwrap();
        association
            .admit_control_command_in(control_stream_id(&scope), payload)
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), outcomes.recv())
            .await
            .unwrap()
            .unwrap();
        if !matches!(caller, Caller::Authorized) {
            assert_eq!(result, Err(ControlDispatchError::InvalidCommand));
            assert_eq!(store.candidate_set(&scope).await.unwrap(), before);
            assert!(
                store
                    .candidate_authorization(&scope, "standby")
                    .await
                    .unwrap()
                    .is_none()
            );
            break;
        }
        result.unwrap();
        let after = store.candidate_set(&scope).await.unwrap();
        assert_eq!(after.revision, request.expected_revision + 1);
        assert_eq!(
            store
                .candidate_authorization(&scope, "standby")
                .await
                .unwrap()
                .is_some(),
            enable
        );
        request.expected_revision = after.revision;
    }
    operator.force_shutdown().await.unwrap();
    coordinator.force_shutdown().await.unwrap();
    response_task.abort();
}
