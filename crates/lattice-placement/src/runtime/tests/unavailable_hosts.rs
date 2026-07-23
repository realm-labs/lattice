use super::*;

#[tokio::test]
async fn unavailable_shard_resolution_fails_fast_and_a_later_request_can_allocate() {
    let cluster_id = ClusterId::new("late-host-test").unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", 26220, 220);
    let (proxy, _) = node(&cluster_id, "proxy", 26221, 221);
    let (host, _) = node(&cluster_id, "host", 26222, 222);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator_node.address.clone(),
            coordinator_node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let proxy_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &proxy,
        60,
    );
    let host_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &host,
        70,
    );
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("late-host-entity").unwrap();
    let protocol_id = ProtocolId::new(57).unwrap();
    let entity_config = EntityConfig::new(
        domain(),
        entity_type.clone(),
        protocol_id,
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let singleton_kind = SingletonKind::new("late-host-singleton").unwrap();
    let singleton_config = SingletonConfig::new(domain(), singleton_kind.clone(), protocol_id);
    let committed = store
        .put_entity_config(
            &leader.leader_guard,
            PutEntityConfig {
                expected: None,
                config: entity_config.clone(),
            },
        )
        .await
        .unwrap();
    leader.version = committed.version;
    leader
        .entity_configs
        .insert(entity_type.clone(), entity_config.clone());
    let committed = store
        .put_singleton_config(
            &leader.leader_guard,
            PutSingletonConfig {
                expected: None,
                config: singleton_config.clone(),
            },
        )
        .await
        .unwrap();
    leader.version = committed.version;
    leader
        .singleton_configs
        .insert(singleton_kind.clone(), singleton_config.clone());
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint: ProtocolFingerprint::new([10; 32]),
    };
    register_up(
        &mut leader,
        test_hello(
            proxy.clone(),
            TestHelloSpec {
                capacity_units: 1,
                proxied_entity_types: [entity_type.clone()].into_iter().collect(),
                used_singletons: [singleton_kind.clone()].into_iter().collect(),
                protocols: vec![descriptor.clone()],
                ..TestHelloSpec::default()
            },
        ),
        proxy_key.clone(),
    )
    .await;
    let shard_id = ShardId::new(3);
    let resolve = |request_id| {
        PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
            association: proxy_key.clone(),
            command_id: CommandId::generate(),
            scope: CoordinatorScope::Placement(domain()),
            coordinator_term: None,
            command: PlacementControlCommand::ResolveShard {
                request_id,
                domain: domain(),
                entity_type: entity_type.clone(),
                shard_id,
            },
        }))
    };
    let resolve_singleton = |request_id| {
        PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
            association: proxy_key.clone(),
            command_id: CommandId::generate(),
            scope: CoordinatorScope::Placement(domain()),
            coordinator_term: None,
            command: PlacementControlCommand::ResolveSingleton {
                request_id,
                domain: domain(),
                kind: singleton_kind.clone(),
            },
        }))
    };

    leader.handle_control(resolve(1)).await.unwrap();
    leader.handle_control(resolve_singleton(11)).await.unwrap();
    assert!(leader.sessions.contains_key(&proxy.incarnation));
    let shard_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: entity_type.clone(),
        shard_id,
    };
    assert_eq!(store.get_slot(&shard_key).await.unwrap(), None);
    let singleton_key = PlacementSlotKey::Singleton {
        domain: domain(),
        kind: singleton_kind.clone(),
    };
    assert_eq!(store.get_slot(&singleton_key).await.unwrap(), None);
    let proxy_association = leader.associations.get(&proxy_key).unwrap();
    let failure = proxy_association
        .replay_control_frames()
        .into_iter()
        .filter_map(|frame| decode_control_envelope(&frame).ok())
        .filter_map(|envelope| {
            decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD).ok()
        })
        .filter_map(|scoped| match scoped.command {
            PlacementControlCommand::ResolutionFailed {
                request_id,
                slot,
                reason,
            } => Some((request_id, slot, reason)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        failure,
        [
            (
                1,
                shard_key.clone(),
                PlacementResolutionFailure::NoEligibleHost,
            ),
            (
                11,
                singleton_key.clone(),
                PlacementResolutionFailure::NoEligibleHost,
            ),
        ]
    );

    register_up(
        &mut leader,
        test_hello(
            host.clone(),
            TestHelloSpec {
                capacity_units: 10,
                hosted_entity_types: [entity_type.clone()].into_iter().collect(),
                singleton_eligibility: [singleton_kind.clone()].into_iter().collect(),
                protocols: vec![descriptor],
                entity_configs: vec![entity_config],
                singleton_configs: vec![singleton_config],
                ..TestHelloSpec::default()
            },
        ),
        host_key,
    )
    .await;
    leader.handle_control(resolve(2)).await.unwrap();
    leader.handle_control(resolve_singleton(12)).await.unwrap();

    let shard = store.get_slot(&shard_key).await.unwrap().unwrap();
    assert_eq!(shard.owner.as_ref(), Some(&host));
    assert_eq!(shard.state, PlacementSlotState::Allocating);
    let singleton = store.get_slot(&singleton_key).await.unwrap().unwrap();
    assert_eq!(singleton.owner.as_ref(), Some(&host));
    assert_eq!(singleton.state, PlacementSlotState::Allocating);
}
