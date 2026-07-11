use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use lattice_core::id::{ActorId, RouteKey};
use lattice_core::instance::{InstanceCapacity, InstanceId, InstanceIncarnation};
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_placement::authority::{
    AdminPlacementReader, PlacementAuthority, SingletonClaimReader, TonicPlacementAuthority,
    TonicPlacementReader, TonicPlacementRoutingStore,
};
use lattice_placement::control::PlacementCoordinatorService;
use lattice_placement::control::proto;
use lattice_placement::control::proto::placement_coordinator_client::PlacementCoordinatorClient;
use lattice_placement::control::proto::placement_coordinator_server::PlacementCoordinatorServer;
use lattice_placement::coordination::actor::{ActivateActorRequest, PlacementCoordinator};
use lattice_placement::coordination::logic::NoopLogicControl;
use lattice_placement::coordination::singleton::ActivateSingletonRequest;
use lattice_placement::coordination::singleton::SingletonRouteResolver;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::{InstanceRecord, InstanceState};
use lattice_placement::routing::cache::RouteCacheConfig;
use lattice_placement::routing::placement::{ExplicitRouteResolver, PlacementRoutingStore};
use lattice_placement::routing::resolver::{ResolveRequest, RouteResolver};
use lattice_placement::storage::memory::InMemoryPlacementStore;
use lattice_placement::storage::{
    ActorPlacementKey, LeaseId, PlacementPrefix, PlacementRevision, PlacementStore, SingletonKey,
};
use lattice_rpc::security::ServiceIdentityConfig;
use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};

const TRUST_DOMAIN: &str = "lattice.test";
const TARGET_SERVICE: &str = "World";
const TARGET_INSTANCE: &str = "world-a";
const TARGET_INCARNATION: &str = "world-a-new-boot";
const TARGET_LEASE: LeaseId = LeaseId(101);
const REJECTION_DEADLINE: Duration = Duration::from_secs(1);
const TRANSPORT_REJECTION_CODES: &[Code] = &[
    Code::Cancelled,
    Code::DeadlineExceeded,
    Code::Unauthenticated,
    Code::Unavailable,
    Code::Unknown,
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_mtls_admission_fences_every_unverified_identity_before_mutation() {
    let pki = TestPki::generate();
    let store = ready_store().await;
    let server = TestAuthorityServer::start(&pki, store.clone()).await;

    assert_transport_rejected_without_mutation(
        connect_tls(server.address, &pki.ca_pem, None, "localhost").await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_tls(
            server.address,
            &pki.ca_pem,
            Some(&pki.untrusted_client),
            "localhost",
        )
        .await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_tls(
            server.address,
            &pki.untrusted_ca_pem,
            Some(&pki.valid_client),
            "localhost",
        )
        .await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_tls(
            server.address,
            &pki.ca_pem,
            Some(&pki.valid_client),
            "not-localhost.invalid",
        )
        .await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_plaintext(server.address).await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;

    let malformed = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.malformed_client),
        "localhost",
    )
    .await
    .expect("a trusted but malformed workload certificate completes TLS");
    assert_all_methods_rejected_without_mutation(malformed, &[Code::Unauthenticated], &store).await;

    let wrong_trust_domain = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.wrong_trust_domain_client),
        "localhost",
    )
    .await
    .expect("a client from the wrong trust domain is still signed by the transport CA");
    assert_all_methods_rejected_without_mutation(
        wrong_trust_domain,
        &[Code::PermissionDenied],
        &store,
    )
    .await;

    let wrong_identity = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.wrong_identity_client),
        "localhost",
    )
    .await
    .expect("a different trusted workload completes TLS");
    assert_all_methods_rejected_without_mutation(wrong_identity, &[Code::PermissionDenied], &store)
        .await;

    let stale_incarnation = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.stale_incarnation_client),
        "localhost",
    )
    .await
    .expect("a stale certificate for the reused instance still completes TLS");
    assert_all_methods_rejected_without_mutation(
        stale_incarnation,
        &[Code::PermissionDenied],
        &store,
    )
    .await;

    let cross_service = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.cross_service_client),
        "localhost",
    )
    .await
    .expect("a trusted workload from another service completes mutual TLS");
    let cross_service_reader = TonicPlacementReader::new(cross_service.clone());
    let cross_service_authority = TonicPlacementAuthority::new(cross_service.clone());
    let actor = cross_service_authority
        .activate_actor(actor_request())
        .await
        .expect("an authenticated workload may request cross-service actor activation");
    assert_eq!(actor.owner, InstanceId::new(TARGET_INSTANCE));
    let singleton = cross_service_authority
        .activate_singleton(singleton_request())
        .await
        .expect("an authenticated workload may request cross-service singleton activation");
    assert_eq!(singleton.owner, InstanceId::new(TARGET_INSTANCE));
    assert!(
        cross_service_reader
            .get_actor(&actor_key())
            .await
            .expect("a current authenticated peer may read cross-service actor placement")
            .is_some()
    );
    let snapshot = cross_service_reader
        .get_service_placement_snapshot(
            &ServiceKind::new(TARGET_SERVICE),
            &InstanceId::new(TARGET_INSTANCE),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .expect("a current authenticated peer may read a coherent cross-service snapshot");
    assert_eq!(snapshot.records.len(), 2);
    let admin_snapshot = cross_service_reader
        .service_admin_snapshot(
            &ServiceKind::new(TARGET_SERVICE),
            &InstanceId::new(TARGET_INSTANCE),
        )
        .await
        .expect("semantic admin snapshot uses bounded instance and placement reads");
    assert_eq!(admin_snapshot.instances.len(), 2);
    assert_eq!(admin_snapshot.actors.len(), 1);
    assert_eq!(admin_snapshot.singletons.len(), 1);
    assert!(matches!(
        cross_service_reader
            .list_service_instances(
                &ServiceKind::new(TARGET_SERVICE),
                NonZeroUsize::new(1).unwrap(),
            )
            .await,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::ResourceExhausted
        })
    ));
    assert_eq!(
        cross_service_reader
            .singleton_owner_lease_claims(
                &ServiceKind::new(TARGET_SERVICE),
                &InstanceId::new(TARGET_INSTANCE),
                &InstanceIncarnation::new(TARGET_INCARNATION),
            )
            .await
            .expect("semantic snapshot discovers current-boot singleton claims")
            .len(),
        1
    );
    assert!(
        cross_service_reader
            .singleton_owner_lease_claims(
                &ServiceKind::new(TARGET_SERVICE),
                &InstanceId::new(TARGET_INSTANCE),
                &InstanceIncarnation::new("world-a-old-boot"),
            )
            .await
            .expect("prior boot is filtered without a direct store scan")
            .is_empty()
    );
    assert_eq!(
        snapshot
            .clone()
            .into_ownership_view_snapshot()
            .records
            .len(),
        2
    );
    assert_eq!(
        snapshot.local_instance.unwrap().incarnation,
        InstanceIncarnation::new(TARGET_INCARNATION)
    );
    assert!(matches!(
        cross_service_reader
            .get_service_placement_snapshot(
                &ServiceKind::new(TARGET_SERVICE),
                &InstanceId::new(TARGET_INSTANCE),
                NonZeroUsize::new(1).unwrap(),
            )
            .await,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::ResourceExhausted
        })
    ));
    let mut remote_view = cross_service_reader
        .open_ownership_view(
            &ServiceKind::new(TARGET_SERVICE),
            &InstanceId::new(TARGET_INSTANCE),
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .expect("a current authenticated peer opens one no-gap ownership stream");
    let snapshot_revision = remote_view.snapshot.revision;
    cross_service_authority
        .activate_actor(ActivateActorRequest {
            service_kind: ServiceKind::new(TARGET_SERVICE),
            actor_kind: ActorKind::new("World"),
            actor_id: ActorId::U64(8),
        })
        .await
        .expect("a mutation after the remote snapshot is streamed");
    let batch = tokio::time::timeout(REJECTION_DEADLINE, remote_view.watch.next())
        .await
        .expect("remote ownership watch update is bounded")
        .expect("remote ownership watch remains valid");
    assert!(batch.revision > snapshot_revision);
    assert!(batch.events.iter().any(|event| matches!(
        event,
        lattice_placement::storage::OwnershipWatchEvent::ActorUpserted { record, .. }
            if record.actor_id == ActorId::U64(8)
    )));
    drop(remote_view);
    let routing_store = TonicPlacementRoutingStore::new(
        cross_service_reader.clone(),
        ServiceKind::new(TARGET_SERVICE),
        InstanceId::new(TARGET_INSTANCE),
        NonZeroUsize::new(8).unwrap(),
    )
    .unwrap();
    let resolver = ExplicitRouteResolver::new(
        ServiceKind::new(TARGET_SERVICE),
        routing_store.clone(),
        std::sync::Arc::new(cross_service_authority.clone()),
        RouteCacheConfig::default(),
    );
    let target = resolver
        .resolve(ResolveRequest {
            service_kind: ServiceKind::new(TARGET_SERVICE),
            actor_kind: ActorKind::new("World"),
            route_key: RouteKey::U64(8),
        })
        .await
        .expect("route resolution uses semantic point reads");
    assert_eq!(target.instance_id, InstanceId::new(TARGET_INSTANCE));
    let singleton_resolver = SingletonRouteResolver::new(
        routing_store.clone(),
        std::sync::Arc::new(cross_service_authority.clone()),
        RouteCacheConfig::default(),
    );
    let singleton_target = singleton_resolver
        .resolve(ResolveRequest {
            service_kind: ServiceKind::new(TARGET_SERVICE),
            actor_kind: ActorKind::new("SeasonManager"),
            route_key: RouteKey::Str("global".to_string()),
        })
        .await
        .expect("singleton resolution uses semantic point reads");
    assert_eq!(
        singleton_target.instance_id,
        InstanceId::new(TARGET_INSTANCE)
    );
    let mut routing_watch = routing_store
        .watch_routing(&ServiceKind::new(TARGET_SERVICE))
        .await
        .expect("route-cache watch uses the semantic ownership stream");
    cross_service_authority
        .activate_actor(ActivateActorRequest {
            service_kind: ServiceKind::new(TARGET_SERVICE),
            actor_kind: ActorKind::new("World"),
            actor_id: ActorId::U64(9),
        })
        .await
        .expect("new placement reaches semantic routing watch");
    assert!(matches!(
        tokio::time::timeout(REJECTION_DEADLINE, routing_watch.next())
            .await
            .expect("routing watch is bounded")
            .expect("routing watch remains live"),
        lattice_placement::storage::PlacementWatchEvent::ActorUpdated { record, .. }
            if record.actor_id == ActorId::U64(9)
    ));
    drop(routing_watch);
    assert!(
        cross_service_reader
            .get_singleton(&singleton_key())
            .await
            .expect("a current authenticated peer may read cross-service singleton placement")
            .is_some()
    );
    assert_rpc_rejected_without_mutation(cross_service, &[Code::PermissionDenied], &store).await;

    let exact_identity = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.valid_client),
        "localhost",
    )
    .await
    .expect("the trusted workload completes mutual TLS");
    let reader = TonicPlacementReader::new(exact_identity.clone());
    assert_eq!(
        reader
            .get_instance(&InstanceId::new(TARGET_INSTANCE))
            .await
            .expect("the current workload may read an instance")
            .expect("target instance exists")
            .incarnation,
        InstanceIncarnation::new(TARGET_INCARNATION)
    );
    assert!(
        reader
            .get_actor(&actor_key())
            .await
            .expect("the current workload may read an actor")
            .is_some()
    );
    assert!(
        reader
            .get_singleton(&singleton_key())
            .await
            .expect("the current workload may read a singleton")
            .is_some()
    );
    let report = TonicPlacementAuthority::new(exact_identity)
        .drain_instance(
            ServiceKind::new(TARGET_SERVICE),
            InstanceId::new(TARGET_INSTANCE),
            InstanceIncarnation::new(TARGET_INCARNATION),
            TARGET_LEASE,
        )
        .await
        .expect("the exact authenticated workload may drain itself");
    assert_eq!(report.drained_instance, InstanceId::new(TARGET_INSTANCE));
    assert_eq!(report.migrated_actors, 3);
    assert_eq!(report.migrated_virtual_shards, 0);
    assert_eq!(target_record(&store).await.state, InstanceState::Draining);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_mtls_registers_and_renews_only_the_current_boot_incarnation() {
    let pki = TestPki::generate();
    let store =
        InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/authority-mtls-liveness"));
    let server = TestAuthorityServer::start(&pki, store.clone()).await;
    let channel = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.valid_client),
        "localhost",
    )
    .await
    .expect("current workload completes mutual TLS");
    let reader = TonicPlacementReader::new(channel.clone());
    let authority = TonicPlacementAuthority::new(channel);
    let registered = authority
        .register_instance(InstanceRecord {
            service_kind: ServiceKind::new(TARGET_SERVICE),
            instance_id: InstanceId::new(TARGET_INSTANCE),
            incarnation: InstanceIncarnation::new(TARGET_INCARNATION),
            lease_id: LeaseId(0),
            advertised_endpoint: "http://127.0.0.1:50051".parse().unwrap(),
            control_endpoint: "http://127.0.0.1:50052".parse().unwrap(),
            version: "authority-mtls-test".to_string(),
            state: InstanceState::Starting,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        })
        .await
        .expect("current boot registers through semantic authority");
    assert_ne!(registered.lease_id, LeaseId(0));
    authority
        .transition_instance(
            registered.service_kind.clone(),
            registered.instance_id.clone(),
            registered.incarnation.clone(),
            registered.lease_id,
            InstanceState::Ready,
        )
        .await
        .expect("current boot transitions to Ready");
    authority
        .keepalive_instance(
            registered.service_kind.clone(),
            registered.instance_id.clone(),
            registered.incarnation.clone(),
            registered.lease_id,
        )
        .await
        .expect("current boot renews its authority-issued lease");
    assert_eq!(
        reader
            .get_instance(&registered.instance_id)
            .await
            .expect("current boot reads through the semantic proxy")
            .expect("registered instance remains present")
            .state,
        InstanceState::Ready
    );
    assert_eq!(
        store.instance_lease_keepalive_count(registered.lease_id),
        Some(1)
    );
    let singleton = authority
        .activate_singleton(singleton_request())
        .await
        .expect("current boot creates its singleton owner record");
    assert_eq!(
        store.instance_lease_keepalive_count(singleton.lease_id),
        Some(0)
    );
    assert!(matches!(
        authority
            .keepalive_singletons(
                registered.service_kind.clone(),
                registered.instance_id.clone(),
                registered.incarnation.clone(),
                vec![singleton.clone(), singleton.clone()],
            )
            .await,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::FailedPrecondition
        })
    ));
    assert_eq!(
        store.instance_lease_keepalive_count(singleton.lease_id),
        Some(0)
    );
    assert_eq!(
        authority
            .keepalive_singletons(
                registered.service_kind.clone(),
                registered.instance_id.clone(),
                registered.incarnation.clone(),
                vec![singleton.clone()],
            )
            .await
            .expect("current boot renews an exactly matching singleton claim"),
        1
    );
    assert_eq!(
        store.instance_lease_keepalive_count(singleton.lease_id),
        Some(1)
    );

    let stale_channel = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.stale_incarnation_client),
        "localhost",
    )
    .await
    .expect("stale workload completes mutual TLS");
    let stale = TonicPlacementAuthority::new(stale_channel);
    let revision_before = placement_revision(&store).await;
    let stale_registration = stale
        .register_instance(InstanceRecord {
            incarnation: InstanceIncarnation::new("world-a-old-boot"),
            lease_id: LeaseId(0),
            state: InstanceState::Starting,
            ..registered.clone()
        })
        .await;
    assert!(matches!(
        stale_registration,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::AlreadyExists
        })
    ));
    assert!(matches!(
        stale
            .keepalive_instance(
                ServiceKind::new(TARGET_SERVICE),
                InstanceId::new(TARGET_INSTANCE),
                InstanceIncarnation::new("world-a-old-boot"),
                registered.lease_id,
            )
            .await,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::PermissionDenied
        })
    ));
    assert!(matches!(
        stale
            .keepalive_singletons(
                ServiceKind::new(TARGET_SERVICE),
                InstanceId::new(TARGET_INSTANCE),
                InstanceIncarnation::new("world-a-old-boot"),
                vec![singleton.clone()],
            )
            .await,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::PermissionDenied
        })
    ));
    assert_eq!(
        store.instance_lease_keepalive_count(singleton.lease_id),
        Some(1)
    );
    assert_eq!(
        store.instance_lease_keepalive_count(registered.lease_id),
        Some(1)
    );
    assert!(matches!(
        stale
            .transition_instance(
                ServiceKind::new(TARGET_SERVICE),
                InstanceId::new(TARGET_INSTANCE),
                InstanceIncarnation::new("world-a-old-boot"),
                registered.lease_id,
                InstanceState::Stopping,
            )
            .await,
        Err(PlacementError::PlacementAuthorityRpc {
            code: Code::PermissionDenied
        })
    ));
    assert_eq!(placement_revision(&store).await, revision_before);
    let current = target_record(&store).await;
    assert_eq!(current.incarnation, registered.incarnation);
    assert_eq!(current.state, InstanceState::Ready);

    server.shutdown().await;
}

async fn assert_transport_rejected_without_mutation(
    result: Result<Channel, tonic::transport::Error>,
    allowed_codes: &[Code],
    store: &InMemoryPlacementStore,
) {
    let revision_before = placement_revision(store).await;
    if let Ok(channel) = result {
        let code = drain_status(channel).await.code();
        assert!(
            allowed_codes.contains(&code),
            "unexpected typed transport rejection code: {code:?}"
        );
        assert_eq!(placement_revision(store).await, revision_before);
        assert_target_ready(store).await;
        return;
    }
    assert_eq!(placement_revision(store).await, revision_before);
    assert_target_ready(store).await;
}

async fn assert_rpc_rejected_without_mutation(
    channel: Channel,
    allowed_codes: &[Code],
    store: &InMemoryPlacementStore,
) {
    let revision_before = placement_revision(store).await;
    let error = drain_error(channel).await;
    match error {
        PlacementError::PlacementAuthorityRpc { code } => assert!(
            allowed_codes.contains(&code),
            "unexpected typed authority rejection code: {code:?}"
        ),
        other => panic!("expected a typed authority RPC rejection, got {other:?}"),
    }
    assert_eq!(placement_revision(store).await, revision_before);
    assert_target_ready(store).await;
}

async fn assert_all_methods_rejected_without_mutation(
    channel: Channel,
    allowed_codes: &[Code],
    store: &InMemoryPlacementStore,
) {
    let revision_before = placement_revision(store).await;
    let reader = TonicPlacementReader::new(channel.clone());
    let authority = TonicPlacementAuthority::new(channel);
    for error in [
        bounded_error(
            authority.activate_actor(actor_request()),
            "unverified identity must not activate an actor",
        )
        .await,
        bounded_error(
            authority.activate_singleton(singleton_request()),
            "unverified identity must not activate a singleton",
        )
        .await,
        bounded_error(
            authority.drain_instance(
                ServiceKind::new(TARGET_SERVICE),
                InstanceId::new(TARGET_INSTANCE),
                InstanceIncarnation::new(TARGET_INCARNATION),
                TARGET_LEASE,
            ),
            "unverified identity must not drain an instance",
        )
        .await,
        bounded_error(
            reader.get_instance(&InstanceId::new(TARGET_INSTANCE)),
            "unverified identity must not read an instance",
        )
        .await,
        bounded_error(
            reader.get_actor(&actor_key()),
            "unverified identity must not read an actor",
        )
        .await,
        bounded_error(
            reader.get_singleton(&singleton_key()),
            "unverified identity must not read a singleton",
        )
        .await,
        bounded_error(
            reader.get_service_placement_snapshot(
                &ServiceKind::new(TARGET_SERVICE),
                &InstanceId::new(TARGET_INSTANCE),
                NonZeroUsize::new(8).unwrap(),
            ),
            "unverified identity must not read a placement snapshot",
        )
        .await,
        bounded_error(
            reader.list_service_instances(
                &ServiceKind::new(TARGET_SERVICE),
                NonZeroUsize::new(8).unwrap(),
            ),
            "unverified identity must not list service instances",
        )
        .await,
    ] {
        match error {
            PlacementError::PlacementAuthorityRpc { code } => assert!(
                allowed_codes.contains(&code),
                "unexpected typed authority rejection code: {code:?}"
            ),
            other => panic!("expected a typed authority RPC rejection, got {other:?}"),
        }
    }
    assert!(
        reader
            .open_ownership_view(
                &ServiceKind::new(TARGET_SERVICE),
                &InstanceId::new(TARGET_INSTANCE),
                NonZeroUsize::new(8).unwrap(),
            )
            .await
            .is_err(),
        "unverified identity must not open a placement watch"
    );
    assert_eq!(placement_revision(store).await, revision_before);
    assert!(store.get_actor(&actor_key()).await.unwrap().is_none());
    assert!(
        store
            .get_singleton(&singleton_key())
            .await
            .unwrap()
            .is_none()
    );
    assert_target_ready(store).await;
}

async fn drain_error(channel: Channel) -> PlacementError {
    bounded_error(
        TonicPlacementAuthority::new(channel).drain_instance(
            ServiceKind::new(TARGET_SERVICE),
            InstanceId::new(TARGET_INSTANCE),
            InstanceIncarnation::new(TARGET_INCARNATION),
            TARGET_LEASE,
        ),
        "an unverified or mismatched workload must not drain an instance",
    )
    .await
}

async fn drain_status(channel: Channel) -> tonic::Status {
    let mut client = PlacementCoordinatorClient::new(channel);
    match tokio::time::timeout(
        REJECTION_DEADLINE,
        client.drain_instance(proto::DrainInstanceRequest {
            service_kind: TARGET_SERVICE.to_string(),
            instance_id: TARGET_INSTANCE.to_string(),
            expected_lease_id: TARGET_LEASE.0,
            instance_incarnation: TARGET_INCARNATION.to_string(),
        }),
    )
    .await
    .expect("transport rejection must arrive before the test deadline")
    {
        Ok(_) => panic!("an unverified transport must not drain an instance"),
        Err(status) => status,
    }
}

async fn bounded_error<T>(
    future: impl std::future::Future<Output = Result<T, PlacementError>>,
    expectation: &'static str,
) -> PlacementError {
    match tokio::time::timeout(REJECTION_DEADLINE, future)
        .await
        .expect("authority rejection must arrive before the test deadline")
    {
        Ok(_) => panic!("{expectation}"),
        Err(error) => error,
    }
}

fn actor_request() -> ActivateActorRequest {
    ActivateActorRequest {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        actor_kind: ActorKind::new("World"),
        actor_id: ActorId::U64(7),
    }
}

fn actor_key() -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        actor_kind: ActorKind::new("World"),
        actor_id: ActorId::U64(7),
    }
}

fn singleton_request() -> ActivateSingletonRequest {
    ActivateSingletonRequest {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        singleton_kind: ActorKind::new("SeasonManager"),
        scope: "global".to_string(),
    }
}

fn singleton_key() -> SingletonKey {
    SingletonKey {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        singleton_kind: ActorKind::new("SeasonManager"),
        scope: "global".to_string(),
    }
}

async fn assert_target_ready(store: &InMemoryPlacementStore) {
    let record = target_record(store).await;
    assert_eq!(record.service_kind, ServiceKind::new(TARGET_SERVICE));
    assert_eq!(
        record.incarnation,
        InstanceIncarnation::new(TARGET_INCARNATION)
    );
    assert_eq!(record.lease_id, TARGET_LEASE);
    assert_eq!(record.state, InstanceState::Ready);
}

async fn placement_revision(store: &InMemoryPlacementStore) -> PlacementRevision {
    store
        .open_ownership_view(
            &ServiceKind::new(TARGET_SERVICE),
            &InstanceId::new(TARGET_INSTANCE),
            NonZeroUsize::new(16).unwrap(),
        )
        .await
        .expect("open coherent ownership snapshot")
        .snapshot
        .revision
}

async fn target_record(store: &InMemoryPlacementStore) -> InstanceRecord {
    store
        .get_instance(&InstanceId::new(TARGET_INSTANCE))
        .await
        .expect("read target instance")
        .expect("target instance remains present")
}

async fn ready_store() -> InMemoryPlacementStore {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/authority-mtls"));
    for (service_kind, instance_id, incarnation, lease_id) in [
        (
            TARGET_SERVICE,
            TARGET_INSTANCE,
            TARGET_INCARNATION,
            TARGET_LEASE,
        ),
        (TARGET_SERVICE, "world-b", "world-b-boot", LeaseId(102)),
        ("Player", "player-a", "player-a-boot", LeaseId(103)),
    ] {
        store
            .upsert_instance(InstanceRecord {
                service_kind: ServiceKind::new(service_kind),
                instance_id: InstanceId::new(instance_id),
                incarnation: InstanceIncarnation::new(incarnation),
                lease_id,
                advertised_endpoint: "http://127.0.0.1:50051".parse().unwrap(),
                control_endpoint: "http://127.0.0.1:50052".parse().unwrap(),
                version: "authority-mtls-test".to_string(),
                state: InstanceState::Ready,
                capacity: InstanceCapacity::default(),
                labels: BTreeMap::new(),
            })
            .await
            .expect("install ready instance");
    }
    store
}

async fn connect_tls(
    address: SocketAddr,
    server_ca_pem: &[u8],
    client_identity: Option<&PemIdentity>,
    domain_name: &str,
) -> Result<Channel, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(server_ca_pem))
        .domain_name(domain_name);
    if let Some(client_identity) = client_identity {
        tls = tls.identity(client_identity.tonic_identity());
    }
    Endpoint::from_shared(format!("https://{address}"))
        .expect("test endpoint URI")
        .tls_config(tls)
        .expect("test TLS configuration")
        .connect()
        .await
}

async fn connect_plaintext(address: SocketAddr) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(format!("http://{address}"))
        .expect("test endpoint URI")
        .connect()
        .await
}

struct TestAuthorityServer {
    address: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestAuthorityServer {
    async fn start(pki: &TestPki, store: InMemoryPlacementStore) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test authority");
        let address = listener.local_addr().expect("test authority address");
        let coordinator = PlacementCoordinator::new(store, NoopLogicControl);
        let service = PlacementCoordinatorService::authenticated(
            coordinator,
            ServiceIdentityConfig {
                trust_domain: TRUST_DOMAIN.to_string(),
            },
        )
        .expect("valid authority identity policy");
        let tls = ServerTlsConfig::new()
            .identity(pki.server.tonic_identity())
            .client_ca_root(Certificate::from_pem(pki.ca_pem.clone()));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .tls_config(tls)
                .expect("test server TLS configuration")
                .add_service(PlacementCoordinatorServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                }),
        );
        Self {
            address,
            shutdown,
            task,
        }
    }

    async fn shutdown(self) {
        self.shutdown.send(()).expect("stop test authority");
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("test authority stops before deadline")
            .expect("join test authority")
            .expect("test authority exits cleanly");
    }
}

#[derive(Clone)]
struct PemIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl PemIdentity {
    fn tonic_identity(&self) -> Identity {
        Identity::from_pem(self.cert_pem.clone(), self.key_pem.clone())
    }
}

struct TestPki {
    ca_pem: Vec<u8>,
    untrusted_ca_pem: Vec<u8>,
    server: PemIdentity,
    valid_client: PemIdentity,
    cross_service_client: PemIdentity,
    wrong_identity_client: PemIdentity,
    stale_incarnation_client: PemIdentity,
    wrong_trust_domain_client: PemIdentity,
    malformed_client: PemIdentity,
    untrusted_client: PemIdentity,
}

impl TestPki {
    fn generate() -> Self {
        let ca = test_ca("lattice authority test CA");
        let untrusted_ca = test_ca("untrusted authority test CA");
        Self {
            ca_pem: ca.pem().into_bytes(),
            untrusted_ca_pem: untrusted_ca.pem().into_bytes(),
            server: server_identity(&ca),
            valid_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/World/instance/world-a/incarnation/world-a-new-boot",
            ),
            cross_service_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/Player/instance/player-a/incarnation/player-a-boot",
            ),
            wrong_identity_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/World/instance/world-other/incarnation/world-other-boot",
            ),
            stale_incarnation_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/World/instance/world-a/incarnation/world-a-old-boot",
            ),
            wrong_trust_domain_client: workload_identity(
                &ca,
                "spiffe://other.test/svc/World/instance/world-a/incarnation/world-a-new-boot",
            ),
            malformed_client: workload_identity(&ca, "spiffe://lattice.test/svc/World/instance"),
            untrusted_client: workload_identity(
                &untrusted_ca,
                "spiffe://lattice.test/svc/World/instance/world-a/incarnation/world-a-new-boot",
            ),
        }
    }
}

fn test_ca(common_name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name = distinguished_name(common_name);
    CertifiedIssuer::self_signed(params, KeyPair::generate().expect("generate test CA key"))
        .expect("generate test CA")
}

fn server_identity(ca: &CertifiedIssuer<'_, KeyPair>) -> PemIdentity {
    let key = KeyPair::generate().expect("generate test server key");
    let mut params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("valid localhost server name");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.distinguished_name = distinguished_name("lattice authority test server");
    let cert = params
        .signed_by(&key, ca)
        .expect("sign test server certificate");
    PemIdentity {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
    }
}

fn workload_identity(ca: &CertifiedIssuer<'_, KeyPair>, spiffe_id: &str) -> PemIdentity {
    let key = KeyPair::generate().expect("generate test workload key");
    let mut params = CertificateParams::default();
    params.subject_alt_names = vec![SanType::URI(
        Ia5String::try_from(spiffe_id).expect("test SPIFFE ID is ASCII"),
    )];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.distinguished_name = distinguished_name("lattice authority test workload");
    let cert = params
        .signed_by(&key, ca)
        .expect("sign test workload certificate");
    PemIdentity {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}
