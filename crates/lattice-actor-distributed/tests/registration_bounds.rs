use std::{
    sync::{Arc, Barrier},
    time::Duration,
};

use lattice_actor_distributed::{ActorKey, actor_kind};
use lattice_actor_distributed::{
    actor_protocol,
    context::HandlerContext,
    directory::{ActivationDirectory, ActivationDirectoryError},
    error::ActorFailure,
    host::{ActorHost, HostRegistryError, ProtocolHostRegistry},
    protocol::ProstCodec,
    registry::{ActorAddressConfig, ActorRegistry, ActorRegistryConfig},
    runtime::{ActorRuntime, ActorSpawnOptions},
    traits::{Actor, Handler, Message, StopReason},
};
use lattice_model::{
    actor::{ActivationId, ActorAddress, ActorPath, ProtocolId},
    cluster::{ClusterId, NodeEndpoint, NodeIncarnation},
};

struct TestActor;

impl Actor for TestActor {
    type Error = ActorFailure;
    type Behavior = lattice_actor_distributed::state_machine::Stateless;
}

#[derive(Clone, PartialEq, prost::Message)]
struct Probe {}
impl Message for Probe {}

impl Handler<Probe> for TestActor {
    async fn handle(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        _: Probe,
    ) -> Result<(), ActorFailure> {
        Ok(())
    }
}

actor_protocol! {
    TestProtocol {
        protocol_id: 301;
        name: "review/registration/v1";
        tell 1 => Probe { schema_version: 1, codec: ProstCodec, }
    }
}

fn address(sequence: u64) -> ActorAddress {
    let incarnation = NodeIncarnation::new(301).unwrap();
    ActorAddress::new(
        ClusterId::new("review").unwrap(),
        NodeEndpoint::new("127.0.0.1", 19301).unwrap(),
        ActorPath::user([format!("actor-{sequence}")]).unwrap(),
        ActivationId::new(incarnation, sequence).unwrap(),
        ProtocolId::new(301).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn concurrent_directory_registration_never_exceeds_capacity() {
    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(TestActor, ActorSpawnOptions::default())
        .unwrap();
    for _ in 0..16 {
        let directory = ActivationDirectory::new(1).unwrap();
        let barrier = Barrier::new(16);
        let admitted = std::thread::scope(|scope| {
            let tasks = (1..=16)
                .map(|sequence| {
                    let directory = &directory;
                    let barrier = &barrier;
                    let handle = &handle;
                    scope.spawn(move || {
                        let reference = address(sequence);
                        barrier.wait();
                        match directory.register(&reference, handle) {
                            Ok(()) => Some(reference),
                            Err(ActivationDirectoryError::Capacity) => None,
                            Err(error) => panic!("unexpected registration error: {error}"),
                        }
                    })
                })
                .collect::<Vec<_>>();
            tasks
                .into_iter()
                .filter_map(|task| task.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(admitted.len(), 1);
        assert_eq!(directory.len(), 1);
        assert!(directory.remove(&admitted[0]));
        assert!(directory.is_empty());
        directory.register(&address(17), &handle).unwrap();
        directory.register(&address(17), &handle).unwrap();
        assert_eq!(
            directory.len(),
            1,
            "replacement must not consume extra capacity"
        );
    }
    let mut terminated = handle.subscribe_terminated();
    handle.stop(StopReason::Requested).unwrap();
    tokio::time::timeout(Duration::from_secs(2), terminated.recv())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn duplicate_host_registration_preserves_original_routing_and_drain() {
    let protocol = Arc::new(TestProtocol::bind::<TestActor>().unwrap());
    let config = ActorRegistryConfig {
        address: Some(ActorAddressConfig {
            cluster_id: ClusterId::new("review").unwrap(),
            node_address: NodeEndpoint::new("127.0.0.1", 19301).unwrap(),
            node_incarnation: NodeIncarnation::new(301).unwrap(),
        }),
        ..Default::default()
    };
    let original = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Original"),
        config.clone(),
        &protocol,
    ));
    let replacement = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Replacement"),
        config,
        &protocol,
    ));
    let actor_id = ActorKey::U64(1);
    let handle = original.start(actor_id.clone(), TestActor).await.unwrap();
    let reference = original.exact_address(&actor_id).unwrap();
    let mut hosts = ProtocolHostRegistry::new(2).unwrap();
    hosts
        .register(ActorHost::new(original.clone(), protocol.clone()))
        .unwrap();
    assert_eq!(
        hosts.register(ActorHost::new(replacement, protocol)),
        Err(HostRegistryError::DuplicateProtocol(301))
    );
    assert!(hosts.is_current(&(&reference).into()));
    let mut terminated = handle.subscribe_terminated();
    assert!(hosts.drain_all().await.is_empty());
    tokio::time::timeout(Duration::from_secs(2), terminated.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(original.get_running(&actor_id).is_none());
}
