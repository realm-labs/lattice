//! A driveable [`PlacementDomainLeader`] for fault-injection harnesses.
//!
//! The leader owns the durable store, the association manager and the member sessions behind
//! module-private state, so nothing outside `runtime` can put it through a real handoff. This
//! module is a sibling of the runtime split, so it reaches that state directly and republishes a
//! narrow set of drive-and-observe operations instead of widening the runtime API. It is compiled
//! only under the `test-harness` feature and is not part of the supported surface.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use lattice_core::{
    actor_ref::{
        ClusterId, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId,
    },
    coordinator::CoordinatorScope,
    release::ReleaseManifest,
};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager, LaneAttachment, LaneKind},
    config::RemotingConfig,
    control::decode_control_envelope,
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
    wire::{Frame, FrameKind},
};
use tokio::sync::mpsc;

use super::{
    CoordinatorRuntimeError, ManualRelocationRequest, PlacementDomainLeader,
    PlacementDomainLeaderConfig,
};
use crate::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, decode_control_command},
    coordinator::{
        COORDINATOR_PROTOCOL_GENERATION, LeaderRecord, MemberHello, MemberRecord, MemberStatus,
        MembershipLeaderGuard, PlacementDomainHello,
    },
    handoff::{HandoffEvent, HandoffPhase},
    plan::PlanStatus,
    region::EntityConfig,
    storage::{
        CoordinatorLeaseStore, InMemoryPlacementStore, MembershipStore, PlacementDomainStore,
        ScopedElectionStore, domain::CreateMember,
    },
    types::{
        ClaimGrant, CoordinatorTerm, MembershipVersion, NodeKey, PlacementSlot, PlacementSlotKey,
        PlacementSlotState, PlacementVersion, ShardId,
    },
};

const ENTITY_TYPE: &str = "harness-entity";
const SHARD_COUNT: u32 = 8;
const PROTOCOL_ID: u64 = 91;
const MEMBER_LEASE: Duration = Duration::from_secs(30);

struct HarnessHost {
    node: NodeKey,
    control: mpsc::Receiver<Frame>,
}

pub struct DomainHarness {
    leader: PlacementDomainLeader<InMemoryPlacementStore>,
    store: Arc<InMemoryPlacementStore>,
    associations: Arc<AssociationManager>,
    coordinator: NodeKey,
    domain: PlacementDomainId,
    entity_type: EntityType,
    hosts: Vec<HarnessHost>,
}

impl DomainHarness {
    pub async fn start(
        cluster: &str,
        domain: &str,
        port_base: u16,
        incarnation_base: u128,
    ) -> Result<Self, CoordinatorRuntimeError> {
        let cluster_id = ClusterId::new(cluster).map_err(rejected)?;
        let domain_id = PlacementDomainId::new(domain).map_err(rejected)?;
        let entity_type = EntityType::new(ENTITY_TYPE).map_err(rejected)?;
        let protocol_id = ProtocolId::new(PROTOCOL_ID).map_err(rejected)?;
        let coordinator = node("coordinator", port_base, incarnation_base)?;
        let nodes = [
            node("host-a", port_base + 1, incarnation_base + 1)?,
            node("host-b", port_base + 2, incarnation_base + 2)?,
        ];
        let associations = Arc::new(
            AssociationManager::new(
                coordinator.address.clone(),
                coordinator.incarnation,
                RemotingConfig::default(),
            )
            .map_err(rejected)?,
        );
        let store = Arc::new(InMemoryPlacementStore::new(64, 64).map_err(rejected)?);
        let mut leader = PlacementDomainLeader::elect(
            store.clone(),
            associations.clone(),
            coordinator.clone(),
            CoordinatorScope::Placement(domain_id.clone()),
            CoordinatorTerm::new(1).map_err(rejected)?,
            PlacementDomainLeaderConfig::default(),
        )
        .await?;
        let entity_config = EntityConfig::new(
            domain_id.clone(),
            entity_type.clone(),
            protocol_id,
            SHARD_COUNT,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .map_err(rejected)?;
        let descriptor = ProtocolDescriptor {
            protocol_id,
            fingerprint: ProtocolFingerprint::new([31; 32]),
        };
        let mut registered = Vec::new();
        for (index, node) in nodes.into_iter().enumerate() {
            let association = attach_session(
                &associations,
                &cluster_id,
                coordinator.incarnation,
                &node,
                incarnation_base * 100 + (index as u128 + 1) * 10,
            )?;
            let member = MemberHello {
                release: ReleaseManifest::development(1),
                rollout_participant: true,
                node: node.clone(),
                roles: BTreeSet::new(),
                failure_domains: BTreeMap::new(),
                protocols: vec![descriptor.clone()],
                remoting_capabilities: BTreeSet::new(),
            };
            let hello = PlacementDomainHello::builder(node.clone(), domain_id.clone(), 4)
                .hosted_entity_types([entity_type.clone()].into_iter().collect())
                .entity_configs(vec![entity_config.clone()])
                .build();
            register_up(&mut leader, &member, hello, &association).await?;
            registered.push((node, association));
        }
        let mut hosts = Vec::new();
        for (node, association) in registered {
            let lane = associations
                .get(&association)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            let mut control = lane
                .take_lane_receiver(LaneKind::Control)
                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
            while control.try_recv().is_ok() {}
            hosts.push(HarnessHost { node, control });
        }
        Ok(Self {
            leader,
            store,
            associations,
            coordinator,
            domain: domain_id,
            entity_type,
            hosts,
        })
    }

    pub fn shard_key(&self, shard: u32) -> PlacementSlotKey {
        PlacementSlotKey::Shard {
            domain: self.domain.clone(),
            entity_type: self.entity_type.clone(),
            shard_id: ShardId::new(shard),
        }
    }

    pub fn host(&self, index: usize) -> Option<&NodeKey> {
        self.hosts.get(index).map(|host| &host.node)
    }

    pub async fn allocate_shard(&mut self, shard: u32) -> Result<(), CoordinatorRuntimeError> {
        self.leader
            .ensure_shard_allocated(self.entity_type.clone(), ShardId::new(shard))
            .await?;
        Ok(())
    }

    pub async fn complete_ready(&mut self, shard: u32) -> Result<(), CoordinatorRuntimeError> {
        let key = self.shard_key(shard);
        let slot = self.require_slot(&key).await?;
        let owner = slot.owner.ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        self.leader
            .complete_initial_ready(&key, &owner, slot.assignment_generation)
            .await
    }

    pub async fn relocate(
        &mut self,
        operation_id: &str,
        shard: u32,
    ) -> Result<u128, CoordinatorRuntimeError> {
        let key = self.shard_key(shard);
        let slot = self.require_slot(&key).await?;
        let owner = slot.owner.ok_or(CoordinatorRuntimeError::UnknownSlot)?;
        let target = self
            .hosts
            .iter()
            .map(|host| &host.node)
            .find(|node| **node != owner)
            .ok_or(CoordinatorRuntimeError::IneligibleTarget)?
            .node_id
            .clone();
        self.leader
            .manual_relocate(ManualRelocationRequest {
                domain: self.domain.clone(),
                operation_id: operation_id.to_owned(),
                entity_type: self.entity_type.clone(),
                shard_id: ShardId::new(shard),
                expected_generation: slot.assignment_generation,
                target_node_id: target,
            })
            .await
    }

    pub async fn apply_barrier(&mut self, shard: u32) -> Result<(), CoordinatorRuntimeError> {
        let key = self.shard_key(shard);
        let handoff = self
            .leader
            .handoffs
            .get(&key)
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
        let version = handoff.barrier_version();
        let sessions = handoff
            .required_sessions()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for session in sessions {
            self.leader
                .transition_handoff(
                    key.clone(),
                    HandoffEvent::AppliedRevision {
                        session,
                        version: version.clone(),
                    },
                )
                .await?;
        }
        Ok(())
    }

    pub async fn source_drained(&mut self, shard: u32) -> Result<(), CoordinatorRuntimeError> {
        let key = self.shard_key(shard);
        let handoff = self
            .leader
            .handoffs
            .get(&key)
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
        let event = HandoffEvent::SourceDrained {
            source: handoff.source.clone(),
            generation: handoff.source_generation,
        };
        self.leader.transition_handoff(key, event).await
    }

    pub async fn target_ready(&mut self, shard: u32) -> Result<(), CoordinatorRuntimeError> {
        let key = self.shard_key(shard);
        let handoff = self
            .leader
            .handoffs
            .get(&key)
            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
        let event = HandoffEvent::TargetReady {
            target: handoff.target.clone(),
            generation: handoff.target_generation,
        };
        self.leader.transition_handoff(key, event).await
    }

    pub async fn set_automatic_paused(
        &mut self,
        operation_id: &str,
        paused: bool,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.leader
            .set_automatic_paused(operation_id.to_owned(), None, paused)
            .await
    }

    /// Re-runs election over the same durable store. The successor's initial reconciliation is the
    /// only path that adopts a claim left behind by a lower term, so it is the only place where the
    /// reconciliation commit boundary can be exercised.
    pub async fn reelect(&mut self, term: u64) -> Result<(), CoordinatorRuntimeError> {
        self.store.revoke_lease(self.leader.leader_lease_id).await?;
        let leader = PlacementDomainLeader::elect(
            self.store.clone(),
            self.associations.clone(),
            self.coordinator.clone(),
            CoordinatorScope::Placement(self.domain.clone()),
            CoordinatorTerm::new(term).map_err(rejected)?,
            PlacementDomainLeaderConfig::default(),
        )
        .await?;
        self.leader = leader;
        Ok(())
    }

    pub async fn slot(&self, shard: u32) -> Result<Option<PlacementSlot>, CoordinatorRuntimeError> {
        Ok(self.store.get_slot(&self.shard_key(shard)).await?)
    }

    pub async fn slot_state(
        &self,
        shard: u32,
    ) -> Result<Option<PlacementSlotState>, CoordinatorRuntimeError> {
        Ok(self.slot(shard).await?.map(|slot| slot.state))
    }

    pub async fn stored_claim(
        &self,
        shard: u32,
    ) -> Result<Option<ClaimGrant>, CoordinatorRuntimeError> {
        Ok(self
            .store
            .get_claim(&self.shard_key(shard))
            .await?
            .map(|claim| claim.grant))
    }

    pub async fn owner_index(&self, shard: u32) -> Result<Option<usize>, CoordinatorRuntimeError> {
        let owner = self.slot(shard).await?.and_then(|slot| slot.owner);
        Ok(owner.and_then(|owner| self.hosts.iter().position(|host| host.node == owner)))
    }

    pub fn tracks_claim(&self, shard: u32) -> bool {
        self.leader.claims.contains_key(&self.shard_key(shard))
    }

    pub fn handoff_phase(&self, shard: u32) -> Option<HandoffPhase> {
        self.leader
            .handoffs
            .get(&self.shard_key(shard))
            .map(|handoff| handoff.phase)
    }

    pub fn tracked_plans(&self) -> usize {
        self.leader.plans.len()
    }

    pub async fn stored_plans(&self) -> Result<usize, CoordinatorRuntimeError> {
        Ok(self.store.list_plans(&self.domain).await?.len())
    }

    pub async fn stored_plan_status(
        &self,
        plan_id: u128,
    ) -> Result<Option<PlanStatus>, CoordinatorRuntimeError> {
        Ok(self
            .store
            .get_plan(&self.domain, plan_id)
            .await?
            .map(|plan| plan.status))
    }

    pub fn automatic_paused(&self) -> bool {
        self.leader.automatic_globally_paused
    }

    pub async fn stored_automatic_paused(&self) -> Result<bool, CoordinatorRuntimeError> {
        Ok(self
            .store
            .get_automatic_settings(&self.domain)
            .await?
            .is_some_and(|settings| settings.globally_paused))
    }

    pub async fn stored_admin_operations(&self) -> Result<usize, CoordinatorRuntimeError> {
        Ok(self.store.list_admin_operations(&self.domain).await?.len())
    }

    pub fn version(&self) -> PlacementVersion {
        self.leader.version.clone()
    }

    pub fn unknown_outcomes(&self) -> u64 {
        self.leader.unknown_outcome_count
    }

    /// Collects everything the coordinator has actually put on a member's control lane since the
    /// last call, so a dropped command is observable as an absent command rather than as a counter.
    pub fn commands(&mut self, index: usize) -> Vec<PlacementControlCommand> {
        let Some(host) = self.hosts.get_mut(index) else {
            return Vec::new();
        };
        let mut commands = Vec::new();
        while let Ok(frame) = host.control.try_recv() {
            let payload = match frame.kind {
                FrameKind::CoordinatorEvent => frame.payload().to_vec(),
                FrameKind::ControlEnvelope => match decode_control_envelope(&frame) {
                    Ok(envelope) => envelope.payload.to_vec(),
                    Err(_) => continue,
                },
                _ => continue,
            };
            if let Ok(scoped) = decode_control_command(&payload, DEFAULT_MAX_CONTROL_PAYLOAD) {
                commands.push(scoped.command);
            }
        }
        commands
    }

    async fn require_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<PlacementSlot, CoordinatorRuntimeError> {
        self.store
            .get_slot(key)
            .await?
            .ok_or(CoordinatorRuntimeError::UnknownSlot)
    }
}

fn rejected<E>(_: E) -> CoordinatorRuntimeError {
    CoordinatorRuntimeError::InvalidConfig
}

fn node(id: &str, port: u16, incarnation: u128) -> Result<NodeKey, CoordinatorRuntimeError> {
    Ok(NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).map_err(rejected)?,
        incarnation: NodeIncarnation::new(incarnation).map_err(rejected)?,
    })
}

fn attach_session(
    associations: &AssociationManager,
    cluster_id: &ClusterId,
    local: NodeIncarnation,
    remote: &NodeKey,
    nonce_base: u128,
) -> Result<AssociationKey, CoordinatorRuntimeError> {
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote.address.clone(),
            remote.incarnation,
        )
        .map_err(rejected)?;
    let key = AssociationKey {
        cluster_id: cluster_id.clone(),
        local_incarnation: local,
        remote_address: remote.address.clone(),
        remote_incarnation: remote.incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, nonce_base),
        (LaneKind::Interactive, nonce_base + 1),
        (LaneKind::Bulk(0), nonce_base + 2),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key.clone(),
                lane,
                connection_nonce: nonce,
            })
            .map_err(rejected)?;
    }
    Ok(key)
}

async fn register_up(
    leader: &mut PlacementDomainLeader<InMemoryPlacementStore>,
    member: &MemberHello,
    hello: PlacementDomainHello,
    association: &AssociationKey,
) -> Result<(), CoordinatorRuntimeError> {
    let incarnation = member.node.incarnation;
    ensure_global_member(leader, member).await?;
    leader.register(hello, association.clone()).await?;
    leader
        .mark_member_up(incarnation, leader.membership_version, association)
        .await
}

async fn ensure_global_member(
    leader: &mut PlacementDomainLeader<InMemoryPlacementStore>,
    hello: &MemberHello,
) -> Result<(), CoordinatorRuntimeError> {
    if leader
        .store
        .get_member(&hello.node.node_id)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let scope = CoordinatorScope::Membership;
    let membership = match leader.store.get_leader(&scope).await? {
        Some(record) => record,
        None => {
            let lease = leader.store.grant_lease(MEMBER_LEASE).await?;
            let record = LeaderRecord {
                scope,
                node: leader.leader.node.clone(),
                protocol_generation: COORDINATOR_PROTOCOL_GENERATION,
                term: CoordinatorTerm::new(1).map_err(rejected)?,
            };
            if !leader.store.campaign_leader(&record, lease).await? {
                return Err(CoordinatorRuntimeError::NotLeader);
            }
            record
        }
    };
    let lease_id = leader.store.grant_lease(MEMBER_LEASE).await?;
    let revision = leader
        .store
        .get_membership_revision()
        .await?
        .next()
        .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)?;
    let member = MemberRecord {
        node: hello.node.clone(),
        hello: hello.clone(),
        status: MemberStatus::Up,
        version: MembershipVersion::new(membership.term, revision),
        lease_id,
    };
    let guard = MembershipLeaderGuard::new(membership).map_err(rejected)?;
    leader
        .store
        .create_member(&guard, CreateMember { member })
        .await?;
    Ok(())
}
