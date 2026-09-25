//! Fixtures shared by the cluster router test modules.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use bytes::BytesMut;
use lattice_actor_distributed::{
    actor_protocol,
    context::HandlerContext,
    error::ActorFailure,
    protocol::{CodecDescriptor, DecodeError, EncodeError, WireCodec},
    registry::ActorCreateContext,
    reply::ReplyTo,
    traits::Responder,
};
use lattice_coordination::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter},
    coordinator::{
        ActorGroupHello, MemberHello, SnapshotLimits, SnapshotRecord, SnapshotVersion,
        build_snapshot,
    },
    session::{GroupSession, GroupSessionConfig, GroupSessionError},
    types::{ClaimGrant, GrantSequence, PlacementSlot},
};
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ClusterId, EntityType, NodeEndpoint, NodeIncarnation, SingletonKind},
};
use lattice_remoting::{
    association::{AssociationKey, LaneAttachment, LaneKind},
    control::{CommandId, ControlDispatch},
};
use tokio::{sync::watch, task::JoinHandle};

use crate::cluster::*;

pub(super) const TEST_PROTOCOL_ID: u64 = 77;

pub(super) fn group() -> ActorGroupId {
    ActorGroupId::new("service-test").unwrap()
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
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<GetValue> for EntityActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: GetValue,
        reply_to: ReplyTo<Value>,
    ) -> Result<(), ActorFailure> {
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
    async fn load(&self, _ctx: ActorCreateContext) -> Result<EntityActor, ActorFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(EntityActor { value: 40 })
    }
}

#[derive(Clone)]
pub(super) struct TokenRecordingLoader {
    pub(super) loads: Arc<AtomicUsize>,
    pub(super) token: Arc<AtomicU64>,
}

#[async_trait]
impl ActorLoader<EntityActor> for TokenRecordingLoader {
    async fn load(&self, ctx: ActorCreateContext) -> Result<EntityActor, ActorFailure> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.token.store(
            ctx.fencing_token().map_or(0, |token| token.get()),
            Ordering::SeqCst,
        );
        Ok(EntityActor { value: 40 })
    }
}

pub(super) fn attach_coordinator(
    associations: &AssociationManager,
    cluster_id: &ClusterId,
    local_incarnation: NodeIncarnation,
    coordinator_address: NodeEndpoint,
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
    pub(super) group: ActorGroupHello,
}

pub(super) fn test_hello(
    node: NodeKey,
    hosted_entity_types: BTreeSet<EntityType>,
    singleton_eligibility: BTreeSet<SingletonKind>,
    used_singletons: BTreeSet<SingletonKind>,
) -> TestHello {
    TestHello {
        member: MemberHello {
            node: node.clone(),
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        },
        group: ActorGroupHello::builder(node, group(), 1)
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
    JoinHandle<Result<(), GroupSessionError>>,
) {
    let (control, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let control = Arc::new(control);
    let version = slots.iter().map(|slot| slot.version.clone()).max().unwrap();
    let coordinator_term = version.term.get();
    let (logic, _effects) = GroupSession::new(
        hello.group,
        coordinator.clone(),
        associations,
        GroupSessionConfig::default(),
        64,
        coordinator_term,
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
    let records = slots
        .iter()
        .map(|slot| {
            let key = match &slot.key {
                PlacementSlotKey::Shard {
                    group,
                    entity_type,
                    shard_id,
                } => format!(
                    "group/{}/shard/{}/{}",
                    group.as_str(),
                    entity_type.as_str(),
                    shard_id.get()
                ),
                PlacementSlotKey::Singleton { group, kind } => {
                    format!("group/{}/singleton/{}", group.as_str(), kind.as_str())
                }
            };
            SnapshotRecord {
                key,
                value: serde_json::to_vec(slot).unwrap().into(),
            }
        })
        .collect();
    let limits = SnapshotLimits::default();
    let scope = CoordinatorScope::Group(version.group.clone());
    let (begin, chunks, end) = build_snapshot(
        &scope,
        version.term.get(),
        DEFAULT_MAX_CONTROL_PAYLOAD,
        SnapshotVersion::Placement(version),
        records,
        &limits,
    )
    .unwrap();
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
                request_id: 0,
                group: slot.key.group().clone(),
                slot: slot.key,
                owner: hello.member.node.clone(),
                coordinator_term: slot.version.term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: GrantSequence::new(1).unwrap(),
                ttl: Duration::from_secs(5),
            }));
        }
    }
    for mut command in commands {
        if let PlacementControlCommand::ClaimGranted(grant) = &mut command {
            grant.request_id = state
                .lock()
                .unwrap()
                .pending_claim_request(&grant.slot)
                .expect("owner emitted renewal request");
        }
        control
            .apply(
                coordinator.clone(),
                lattice_coordination::control::control_stream_id(&CoordinatorScope::Group(group())),
                CommandId::generate(),
                lattice_coordination::control::encode_control_command_for_term(
                    &CoordinatorScope::Group(group()),
                    coordinator_term,
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
