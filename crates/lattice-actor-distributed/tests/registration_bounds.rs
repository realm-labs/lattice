use bytes::Bytes;
use std::{
    sync::{Arc, Barrier},
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};

use lattice_actor_distributed::ActorKey;
use lattice_actor_distributed::{
    actor_protocol,
    context::HandlerContext,
    directory::{ActivationDirectory, ActivationDirectoryError},
    environment::ActorEnvironment,
    error::ActorFailure,
    host::{ActorHost, HostRegistryError, ProtocolHostRegistry},
    protocol::ProstCodec,
    registry::{ActorAddressConfig, ActorDefinition, ActorRegistry, ActorRegistryConfig},
    runtime::{ActorRuntime, ActorSpawnOptions},
    state_machine::Stateless,
    traits::{Actor, Handler, Message, StopReason},
};
use lattice_model::{
    actor::{ActivationId, ActorAddress, ActorPath, ProtocolId},
    cluster::{ClusterId, NodeEndpoint, NodeIncarnation},
};

struct TestActor;

impl Actor for TestActor {
    type Error = ActorFailure;
    type Behavior = Stateless;
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
    timeout(Duration::from_secs(2), terminated.recv())
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
    let original = Arc::new(ActorRegistry::<OriginalDefinition, _>::new_bound(
        config.clone(),
        &protocol,
    ));
    let replacement = Arc::new(ActorRegistry::<OriginalDefinition, _>::new_bound(
        config, &protocol,
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
        Err(HostRegistryError::DuplicateDefinition {
            protocol_id: 301,
            actor_name: "Original"
        })
    );
    assert!(hosts.is_current(&(&reference).into()));
    let mut terminated = handle.subscribe_terminated();
    assert!(hosts.drain_all().await.is_empty());
    timeout(Duration::from_secs(2), terminated.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(original.get_running(&actor_id).is_none());
}

#[derive(Debug)]
struct OriginalDefinition;

impl ActorDefinition for OriginalDefinition {
    const NAME: &'static str = "Original";
    type Protocol = TestProtocol;
}

#[derive(Debug)]
struct ReplacementDefinition;

impl ActorDefinition for ReplacementDefinition {
    const NAME: &'static str = "Replacement";
    type Protocol = TestProtocol;
}

struct RoutedActor(Arc<Semaphore>);

impl Actor for RoutedActor {
    type Error = ActorFailure;
    type Behavior = Stateless;
}

impl Handler<Probe> for RoutedActor {
    async fn handle(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        _: Probe,
    ) -> Result<(), ActorFailure> {
        self.0.add_permits(1);
        Ok(())
    }
}

#[tokio::test]
async fn different_definitions_share_one_protocol_without_cross_routing() {
    let mut environment = ActorEnvironment::builder();
    environment
        .insert(ActivationDirectory::new(8).unwrap())
        .unwrap();
    let config = ActorRegistryConfig {
        environment: environment.build(),
        address: Some(ActorAddressConfig {
            cluster_id: ClusterId::new("shared-protocol").unwrap(),
            node_address: NodeEndpoint::new("127.0.0.1", 19301).unwrap(),
            node_incarnation: NodeIncarnation::new(302).unwrap(),
        }),
        ..Default::default()
    };
    let protocol = Arc::new(TestProtocol::bind::<RoutedActor>().unwrap());
    let first = Arc::new(ActorRegistry::<OriginalDefinition, _>::new_bound(
        config.clone(),
        &protocol,
    ));
    let second = Arc::new(ActorRegistry::<ReplacementDefinition, _>::new_bound(
        config, &protocol,
    ));
    let first_deliveries = Arc::new(Semaphore::new(0));
    let second_deliveries = Arc::new(Semaphore::new(0));
    let key = ActorKey::U64(42);
    first
        .start(key.clone(), RoutedActor(first_deliveries.clone()))
        .await
        .unwrap();
    second
        .start(key.clone(), RoutedActor(second_deliveries.clone()))
        .await
        .unwrap();
    let first_address = first.exact_address(&key).unwrap();
    let second_address = second.exact_address(&key).unwrap();
    assert_ne!(first_address.actor_path(), second_address.actor_path());
    assert!(first.get_exact(&second_address).is_none());
    assert!(second.get_exact(&first_address).is_none());
    let mut hosts = ProtocolHostRegistry::new(2).unwrap();
    hosts
        .register(ActorHost::new(first.clone(), protocol.clone()))
        .unwrap();
    hosts
        .register(ActorHost::new(second.clone(), protocol))
        .unwrap();
    assert!(hosts.is_current(&(&first_address).into()));
    assert!(hosts.is_current(&(&second_address).into()));
    let mut first_terminated = hosts
        .subscribe_terminated(&(&first_address).into())
        .unwrap();
    let mut second_terminated = hosts
        .subscribe_terminated(&(&second_address).into())
        .unwrap();
    hosts
        .tell_wait((&first_address).into(), 1, Bytes::new())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), first_deliveries.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(second_deliveries.available_permits(), 0);
    hosts
        .tell_wait((&second_address).into(), 1, Bytes::new())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), second_deliveries.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(first_deliveries.available_permits(), 0);
    assert!(hosts.drain_all().await.is_empty());
    timeout(Duration::from_secs(2), first_terminated.recv())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), second_terminated.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(first.get_running(&key).is_none());
    assert!(second.get_running(&key).is_none());
}
