//! Fixtures shared by the cluster router test modules.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use bytes::BytesMut;
use lattice_actor::{
    actor_protocol,
    context::HandlerContext,
    error::ActorError,
    protocol::{CodecDescriptor, DecodeError, EncodeError, WireCodec},
    registry::ActorCreateContext,
    reply::ReplyTo,
    traits::Responder,
};
use lattice_core::{
    actor_ref::{ClusterId, EntityType, NodeAddress, NodeIncarnation, SingletonKind},
    coordinator::CoordinatorScope,
};
use lattice_placement::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter},
    coordinator::{
        MemberHello, PlacementDomainHello, SnapshotLimits, SnapshotRecord, SnapshotVersion,
        build_snapshot,
    },
    session::{LogicCoordinatorConfig, LogicSessionError, PlacementDomainSession},
    types::{ClaimGrant, GrantSequence, PlacementSlot},
};
use lattice_remoting::{
    association::{AssociationKey, LaneAttachment, LaneKind},
    control::{CommandId, ControlDispatch},
};
use tokio::{sync::watch, task::JoinHandle};

use crate::cluster::*;

pub(super) const TEST_PROTOCOL_ID: u64 = 77;

pub(super) fn domain() -> PlacementDomainId {
    PlacementDomainId::new("service-test").unwrap()
}

#[derive(Clone, lattice_actor::Request)]
#[request(response = Value)]
pub(super) struct GetValue(pub(super) u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Value(pub(super) u64);

#[derive(Clone, Copy)]
struct GetCodec;

impl WireCodec<GetValue> for GetCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &GetValue, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<GetValue, DecodeError> {
        Ok(GetValue(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("GetValue requires eight bytes"),
        )?)))
    }
}

#[derive(Clone, Copy)]
struct ValueCodec;

impl WireCodec<Value> for ValueCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &Value, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Value, DecodeError> {
        Ok(Value(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("Value requires eight bytes"),
        )?)))
    }
}

pub(super) struct EntityActor {
    value: u64,
}

impl Actor for EntityActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<GetValue> for EntityActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: GetValue,
        reply_to: ReplyTo<Value>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(Value(self.value + request.0));
        Ok(())
    }
}

actor_protocol! {
    pub(super) EntityProtocol {
        protocol_id: TEST_PROTOCOL_ID;
        name: "cluster-router-test/v1";
        ask 1 => GetValue {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: GetCodec,
            response_codec: ValueCodec,
        }
    }
}

#[derive(Clone)]
pub(super) struct CountingLoader(pub(super) Arc<AtomicUsize>);

#[async_trait]
impl ActorLoader<EntityActor> for CountingLoader {
    async fn load(&self, _ctx: ActorCreateContext) -> Result<EntityActor, ActorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(EntityActor { value: 40 })
    }
}

pub(super) fn attach_coordinator(
    associations: &AssociationManager,
    cluster_id: &ClusterId,
    local_incarnation: NodeIncarnation,
    coordinator_address: NodeAddress,
    coordinator_incarnation: NodeIncarnation,
) -> AssociationKey {
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            coordinator_address.clone(),
            coordinator_incarnation,
        )
        .unwrap();
    let key = AssociationKey {
        cluster_id: cluster_id.clone(),
        local_incarnation,
        remote_address: coordinator_address,
        remote_incarnation: coordinator_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    key
}

pub(super) struct TestHello {
    pub(super) member: MemberHello,
    pub(super) domain: PlacementDomainHello,
}

pub(super) fn test_hello(
    node: NodeKey,
    hosted_entity_types: BTreeSet<EntityType>,
    singleton_eligibility: BTreeSet<SingletonKind>,
    used_singletons: BTreeSet<SingletonKind>,
) -> TestHello {
    TestHello {
        member: MemberHello {
            release: lattice_core::release::ReleaseManifest::development(1),
            rollout_participant: true,
            node: node.clone(),
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        },
        domain: PlacementDomainHello::builder(node, domain(), 1)
            .hosted_entity_types(hosted_entity_types)
            .singleton_eligibility(singleton_eligibility)
            .used_singletons(used_singletons)
            .build(),
    }
}

pub(super) async fn stage_logic_runtime(
    hello: TestHello,
    coordinator: AssociationKey,
    associations: Arc<AssociationManager>,
    slots: Vec<PlacementSlot>,
) -> (
    Arc<Mutex<LogicPlacementState>>,
    Arc<PlacementControlRouter>,
    watch::Sender<bool>,
    JoinHandle<Result<(), LogicSessionError>>,
) {
    let (control, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let control = Arc::new(control);
    let (logic, _effects) = PlacementDomainSession::new(
        hello.domain,
        coordinator.clone(),
        associations,
        LogicCoordinatorConfig::default(),
        64,
        1,
    )
    .unwrap();
    for slot in &slots {
        if slot.owner.as_ref() == Some(&hello.member.node) {
            logic
                .register_authority(slot.key.clone(), Duration::from_millis(10))
                .unwrap();
        }
    }
    let state = logic.state();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(logic.run(controls, shutdown_rx));
    let version = slots.iter().map(|slot| slot.version.clone()).max().unwrap();
    let records = slots
        .iter()
        .map(|slot| {
            let key = match &slot.key {
                PlacementSlotKey::Shard {
                    domain,
                    entity_type,
                    shard_id,
                } => format!(
                    "domain/{}/shard/{}/{}",
                    domain.as_str(),
                    entity_type.as_str(),
                    shard_id.get()
                ),
                PlacementSlotKey::Singleton { domain, kind } => {
                    format!("domain/{}/singleton/{}", domain.as_str(), kind.as_str())
                }
            };
            SnapshotRecord {
                key,
                value: serde_json::to_vec(slot).unwrap().into(),
            }
        })
        .collect();
    let limits = SnapshotLimits::default();
    let (begin, chunks, end) =
        build_snapshot(SnapshotVersion::Placement(version), records, &limits).unwrap();
    let mut commands = vec![PlacementControlCommand::SnapshotBegin(begin)];
    commands.extend(
        chunks
            .into_iter()
            .map(PlacementControlCommand::SnapshotChunk),
    );
    commands.push(PlacementControlCommand::SnapshotEnd(end));
    for slot in slots {
        if slot.owner.as_ref() == Some(&hello.member.node) {
            commands.push(PlacementControlCommand::ClaimGranted(ClaimGrant {
                domain: slot.key.domain().clone(),
                slot: slot.key,
                owner: hello.member.node.clone(),
                coordinator_term: slot.version.term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: GrantSequence::new(1).unwrap(),
                ttl: Duration::from_secs(5),
            }));
        }
    }
    for command in commands {
        control
            .apply(
                coordinator.clone(),
                CommandId::generate(),
                lattice_placement::control::encode_control_command_for_term(
                    &CoordinatorScope::Placement(domain()),
                    1,
                    &command,
                    DEFAULT_MAX_CONTROL_PAYLOAD,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    (state, control, shutdown, task)
}
