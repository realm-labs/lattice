use lattice_actor::context::HandlerContext;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use lattice_actor::{
    error::{ActorError, ActorTellError},
    mailbox::MailboxConfig,
    registry::{ActorRegistry, ActorRegistryConfig},
    traits::{Actor, Handler, StopReason},
};
use lattice_core::{
    actor_kind,
    actor_ref::{
        EntityId, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId,
    },
    id::ActorId,
};
use lattice_placement::{
    allocation::{
        AllocationRequest, LoadSample, PlacedShard, PlacementNode, PlacementView, RebalanceLimits,
        RebalanceTrigger, ShardAllocationStrategy, WeightedLeastLoad,
    },
    handoff::{HandoffEvent, HandoffMachine},
    region::{BufferedMessageMode, EntityConfig, RegionConfig, ShardHome, ShardRegion},
    types::{
        AssignmentGeneration, CoordinatorTerm, MonotonicTime, NodeKey, PlacementSlotKey,
        PlacementSlotState, PlacementVersion, Revision, ShardId,
    },
};
use lattice_remoting::{
    association::AssociationId,
    control::{CommandId, ControlApply, ReliableControl},
};

#[derive(Debug, Clone)]
pub struct MatrixMeasurement {
    pub name: &'static str,
    pub operations: usize,
    pub elapsed: Duration,
}

impl MatrixMeasurement {
    pub fn throughput_per_second(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64()
    }
}

struct BenchActor;
#[derive(lattice_actor::Message)]
struct BenchTell;

impl Actor for BenchActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<BenchTell> for BenchActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        _message: BenchTell,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub async fn local_actor_admission(operations: usize) -> Result<MatrixMeasurement, Box<dyn Error>> {
    let registry = Arc::new(ActorRegistry::new(
        actor_kind!("BenchmarkLocal"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(1024),
            ..ActorRegistryConfig::default()
        },
    ));
    let handle = registry.start(ActorId::U64(1), BenchActor).await?;
    let started = Instant::now();
    for _ in 0..operations {
        let mut message = BenchTell;
        loop {
            match handle.try_tell(message) {
                Ok(()) => break,
                Err(ActorTellError::MailboxFull(returned)) => {
                    message = returned;
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(Box::new(error)),
            }
        }
    }
    let elapsed = started.elapsed();
    handle.stop(StopReason::Requested).await?;
    Ok(MatrixMeasurement {
        name: "local_actor_tell_admission",
        operations,
        elapsed,
    })
}

pub fn placement_matrix(
    operations: usize,
    payload_bytes: usize,
) -> Result<Vec<MatrixMeasurement>, Box<dyn Error>> {
    let operations = operations.max(1);
    let payload = Bytes::from(vec![0_u8; payload_bytes]);
    let entity_type = EntityType::new("benchmark-entity")?;
    let protocol = ProtocolId::new(0x6265_6e63_6800_0002)?;
    let config = EntityConfig::new(
        placement_domain(),
        entity_type.clone(),
        protocol,
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )?;
    let local = node("local", 1, 27101)?;
    let remote = node("remote", 2, 27102)?;
    let entity_id = EntityId::new(b"benchmark-key".to_vec())?;
    let home = |revision| ShardHome {
        owner: remote.clone(),
        generation: AssignmentGeneration::new(1).expect("constant generation"),
        revision: Revision::new(revision).expect("positive revision"),
        state: PlacementSlotState::Running,
    };

    let mut stable = ShardRegion::new(local.incarnation, config.clone(), RegionConfig::default())?;
    stable.apply_home(ShardId::new(0), home(1))?;
    let started = Instant::now();
    for message_id in 1..=operations {
        stable.route(
            entity_id.clone(),
            message_id as u64,
            BufferedMessageMode::Tell,
            payload.clone(),
            MonotonicTime::from_millis(1),
        )?;
    }
    let stable_shard = MatrixMeasurement {
        name: "stable_shard_route",
        operations,
        elapsed: started.elapsed(),
    };

    let mut unknown = ShardRegion::new(local.incarnation, config, RegionConfig::default())?;
    let started = Instant::now();
    for index in 0..operations {
        let route_revision = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_mul(2))
            .and_then(|value| value.checked_add(1))
            .ok_or("benchmark revision overflow")?;
        unknown.route(
            entity_id.clone(),
            (index + 1) as u64,
            BufferedMessageMode::Tell,
            payload.clone(),
            MonotonicTime::from_millis(route_revision),
        )?;
        unknown.apply_home(ShardId::new(0), home(route_revision))?;
        unknown.invalidate_for_handoff(ShardId::new(0), Revision::new(route_revision + 1)?)?;
    }
    let unknown_shard = MatrixMeasurement {
        name: "unknown_shard_lookup_buffer_install",
        operations,
        elapsed: started.elapsed(),
    };

    let (request, view) =
        allocation_fixture(entity_type.clone(), protocol, local.clone(), remote.clone())?;
    let strategy = WeightedLeastLoad::default();
    let started = Instant::now();
    for _ in 0..operations {
        strategy.allocate(&request, &view)?;
    }
    let allocation = MatrixMeasurement {
        name: "allocation_evaluation",
        operations,
        elapsed: started.elapsed(),
    };

    let limits = RebalanceLimits {
        moves_per_round: 4,
        concurrent_cluster: 4,
        concurrent_entity: 4,
        concurrent_source: 4,
        concurrent_target: 4,
    };
    let started = Instant::now();
    for _ in 0..operations {
        strategy.rebalance(
            &entity_type,
            protocol,
            RebalanceTrigger::Automatic,
            &view,
            limits,
        )?;
    }
    let rebalance = MatrixMeasurement {
        name: "rebalance_planning",
        operations,
        elapsed: started.elapsed(),
    };

    let started = Instant::now();
    for index in 0..operations {
        reduce_handoff(index as u128 + 1, &entity_type, &local, &remote)?;
    }
    let handoff = MatrixMeasurement {
        name: "handoff_reducer",
        operations,
        elapsed: started.elapsed(),
    };

    let started = Instant::now();
    for index in 0..operations {
        reduce_reconnect(index as u128 + 1)?;
    }
    let reconnect = MatrixMeasurement {
        name: "reliable_control_reconnect_replay",
        operations,
        elapsed: started.elapsed(),
    };

    Ok(vec![
        stable_shard,
        unknown_shard,
        allocation,
        rebalance,
        handoff,
        reconnect,
    ])
}

fn allocation_fixture(
    entity_type: EntityType,
    protocol: ProtocolId,
    source: NodeKey,
    target: NodeKey,
) -> Result<(AllocationRequest, PlacementView), Box<dyn Error>> {
    let placement_node = |key: NodeKey, weight| PlacementNode {
        key: key.clone(),
        ready: true,
        eligible_entity_types: BTreeSet::from([entity_type.clone()]),
        protocols: BTreeSet::from([protocol]),
        capacity_units: 10,
        joined_at: MonotonicTime::from_millis(0),
        load: Some(LoadSample {
            boot_incarnation: key.incarnation,
            sequence: 1,
            observed_at: MonotonicTime::from_millis(100_000),
            weight,
        }),
        reserved_weight: 0,
        draining: false,
    };
    Ok((
        AllocationRequest {
            domain: placement_domain(),
            entity_type: entity_type.clone(),
            shard_id: ShardId::new(1),
            required_protocol: protocol,
        },
        PlacementView {
            domain: placement_domain(),
            version: PlacementVersion::new(
                placement_domain(),
                CoordinatorTerm::new(1)?,
                Revision::new(1)?,
            ),
            now: MonotonicTime::from_millis(100_000),
            reconciled: true,
            degraded: false,
            nodes: vec![
                placement_node(source.clone(), 100),
                placement_node(target, 0),
            ],
            shards: vec![PlacedShard {
                domain: placement_domain(),
                entity_type,
                shard_id: ShardId::new(1),
                owner: source,
                generation: AssignmentGeneration::new(1)?,
                measured_weight: Some(20),
                assigned_at: MonotonicTime::from_millis(0),
                active_move: false,
            }],
            active_cluster_moves: 0,
            active_entity_moves: BTreeMap::new(),
            active_source_moves: BTreeMap::new(),
            active_target_moves: BTreeMap::new(),
            last_automatic_move_at: None,
        },
    ))
}

fn reduce_handoff(
    plan_id: u128,
    entity_type: &EntityType,
    source: &NodeKey,
    target: &NodeKey,
) -> Result<(), Box<dyn Error>> {
    let generation = AssignmentGeneration::new(1)?;
    let mut machine = HandoffMachine::begin(
        PlacementSlotKey::Shard {
            domain: placement_domain(),
            entity_type: entity_type.clone(),
            shard_id: ShardId::new(1),
        },
        plan_id,
        source.clone(),
        target.clone(),
        generation,
        PlacementVersion::new(
            placement_domain(),
            CoordinatorTerm::new(1)?,
            Revision::new(1)?,
        ),
        BTreeSet::new(),
    )?;
    machine.start();
    machine.transition(HandoffEvent::SourceDrained {
        source: source.clone(),
        generation,
    })?;
    machine.transition(HandoffEvent::TargetClaimInstalled {
        target: target.clone(),
        generation: generation.next()?,
    })?;
    machine.transition(HandoffEvent::TargetReady {
        target: target.clone(),
        generation: generation.next()?,
    })?;
    Ok(())
}

fn placement_domain() -> PlacementDomainId {
    PlacementDomainId::new("benchmark").expect("static placement domain is valid")
}

fn reduce_reconnect(command: u128) -> Result<(), Box<dyn Error>> {
    let epoch = AssociationId::new(1).ok_or("invalid benchmark association ID")?;
    let envelope = {
        let mut sender = ReliableControl::new(epoch, 4, 1024)?;
        sender.enqueue(
            CommandId::new(command).ok_or("invalid benchmark command ID")?,
            Bytes::from_static(b"state"),
        )?;
        sender
            .replay()
            .next()
            .cloned()
            .ok_or("replay outbox unexpectedly empty")?
    };
    let mut receiver = ReliableControl::new(epoch, 4, 1024)?;
    if !matches!(receiver.receive(envelope), ControlApply::Apply(_)) {
        return Err("replayed command was not applied".into());
    }
    Ok(())
}

fn node(node_id: &str, incarnation: u128, port: u16) -> Result<NodeKey, Box<dyn Error>> {
    Ok(NodeKey {
        node_id: node_id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port)?,
        incarnation: NodeIncarnation::new(incarnation)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn complete_benchmark_matrix_executes() {
        assert_eq!(placement_matrix(2, 16).unwrap().len(), 6);
        assert_eq!(local_actor_admission(2).await.unwrap().operations, 2);
    }
}
