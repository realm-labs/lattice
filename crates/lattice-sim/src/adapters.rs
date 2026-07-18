use bytes::Bytes;
use lattice_core::actor_ref::{
    ActorRef, EntityId, NodeIncarnation, PlacementDomainId, ProtocolTag,
};
use lattice_placement::{
    authority::{AuthorityEffect, AuthorityError, AuthorityEvent, PlacementAuthority},
    coordinator::{
        CoordinatorDelta, PlacementDomainState, PlacementDomainStateError, SnapshotInstall,
    },
    handoff::{HandoffEffect, HandoffError, HandoffEvent, HandoffMachine},
    plan::{PlanError, RebalancePlan},
    region::{
        BufferedMessage, BufferedMessageMode, RegionError, RouteDecision, ShardHome, ShardRegion,
    },
    singleton::{SingletonError, SingletonManager},
    types::{AssignmentGeneration, MonotonicTime, ShardId},
};
use lattice_remoting::{
    association::AssociationId,
    control::{CommandId, ControlApply, ControlEnvelope, ReliableControl, ReliableControlError},
    messaging::target::ExactActorTarget,
    watch::{WatchCommand, WatchError, WatchId, WatchRegistry},
};
use lattice_service::lifecycle::{
    NodeLifecycle, NodeLifecycleState, ServiceLifecycleEffect, ServiceLifecycleError,
    ServiceLifecycleEvent,
};

pub struct ControlAdapter {
    reducer: ReliableControl,
}

impl ControlAdapter {
    pub fn new(
        epoch: AssociationId,
        maximum_frames: usize,
        maximum_bytes: usize,
    ) -> Result<Self, ReliableControlError> {
        Ok(Self {
            reducer: ReliableControl::new(epoch, maximum_frames, maximum_bytes)?,
        })
    }

    pub fn enqueue(
        &mut self,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<ControlEnvelope, ReliableControlError> {
        self.reducer.enqueue(command_id, payload)
    }

    pub fn receive(&mut self, envelope: ControlEnvelope) -> ControlApply {
        self.reducer.receive(envelope)
    }
}

pub struct SessionAdapter {
    reducer: PlacementDomainState,
}

impl SessionAdapter {
    pub fn new(domain: PlacementDomainId) -> Self {
        Self {
            reducer: PlacementDomainState::new(domain),
        }
    }

    pub fn install(&mut self, snapshot: SnapshotInstall) -> Result<(), PlacementDomainStateError> {
        self.reducer.install(snapshot)
    }

    pub fn apply(&mut self, delta: CoordinatorDelta) -> Result<(), PlacementDomainStateError> {
        self.reducer.apply(delta)
    }

    pub fn ready(&self) -> bool {
        self.reducer.ready()
    }
}

pub struct AuthorityAdapter {
    reducer: PlacementAuthority,
}

impl AuthorityAdapter {
    pub fn new(reducer: PlacementAuthority) -> Self {
        Self { reducer }
    }

    pub fn step(&mut self, event: AuthorityEvent) -> Result<Vec<AuthorityEffect>, AuthorityError> {
        self.reducer.transition(event)
    }

    pub fn admission_open(&self) -> bool {
        self.reducer.admission_open()
    }
}

pub struct RegionAdapter {
    reducer: ShardRegion,
}

impl RegionAdapter {
    pub fn new(reducer: ShardRegion) -> Self {
        Self { reducer }
    }

    pub fn route(
        &mut self,
        entity_id: EntityId,
        message_id: u64,
        mode: BufferedMessageMode,
        payload: Bytes,
        now: MonotonicTime,
    ) -> Result<RouteDecision, RegionError> {
        self.reducer
            .route(entity_id, message_id, mode, payload, now)
    }

    pub fn install_home(
        &mut self,
        shard_id: ShardId,
        home: ShardHome,
    ) -> Result<Vec<BufferedMessage>, RegionError> {
        self.reducer.apply_home(shard_id, home)
    }
}

pub struct HandoffAdapter {
    reducer: HandoffMachine,
}

impl HandoffAdapter {
    pub fn new(reducer: HandoffMachine) -> Self {
        Self { reducer }
    }

    pub fn step(&mut self, event: HandoffEvent) -> Result<Vec<HandoffEffect>, HandoffError> {
        self.reducer.transition(event)
    }

    pub fn reducer(&self) -> &HandoffMachine {
        &self.reducer
    }
}

pub struct PlanAdapter {
    reducer: RebalancePlan,
}

impl PlanAdapter {
    pub fn new(reducer: RebalancePlan) -> Self {
        Self { reducer }
    }

    pub fn begin(
        &mut self,
        shard_id: ShardId,
        generation: AssignmentGeneration,
        active_move: Option<u128>,
    ) -> Result<(), PlanError> {
        self.reducer.begin_move(shard_id, generation, active_move)
    }

    pub fn complete(&mut self, shard_id: ShardId) -> Result<(), PlanError> {
        self.reducer.complete_move(shard_id)
    }

    pub fn reducer(&self) -> &RebalancePlan {
        &self.reducer
    }
}

pub struct SingletonAdapter {
    reducer: SingletonManager,
}

impl SingletonAdapter {
    pub fn new(reducer: SingletonManager) -> Self {
        Self { reducer }
    }

    pub fn step(&mut self, event: AuthorityEvent) -> Result<Vec<AuthorityEffect>, SingletonError> {
        self.reducer.transition(event)
    }

    pub fn admission_open(&self) -> bool {
        self.reducer.accepts_messages()
    }
}

pub struct WatchAdapter {
    reducer: WatchRegistry,
}

#[derive(Default)]
pub struct NodeLifecycleAdapter {
    reducer: NodeLifecycle,
}

impl NodeLifecycleAdapter {
    pub fn step(
        &mut self,
        event: ServiceLifecycleEvent,
    ) -> Result<Vec<ServiceLifecycleEffect>, ServiceLifecycleError> {
        self.reducer.transition(event)
    }

    pub fn state(&self) -> NodeLifecycleState {
        self.reducer.state()
    }
}

impl WatchAdapter {
    pub fn new(reducer: WatchRegistry) -> Self {
        Self { reducer }
    }

    pub fn watch<A: ProtocolTag>(
        &mut self,
        association: AssociationId,
        target: &ActorRef<A>,
    ) -> Result<(WatchId, WatchCommand), WatchError> {
        self.reducer.watch(association, target)
    }

    pub fn node_down(&mut self, incarnation: NodeIncarnation) -> Vec<(WatchId, ExactActorTarget)> {
        self.reducer.node_down(incarnation)
    }
}

#[cfg(test)]
mod service_lifecycle_tests {
    use super::*;

    #[test]
    fn simulation_adapter_executes_production_service_reducer() {
        let mut adapter = NodeLifecycleAdapter::default();
        adapter.step(ServiceLifecycleEvent::RemotingReady).unwrap();
        let effects = adapter
            .step(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert_eq!(adapter.state(), NodeLifecycleState::Ready);
        assert_eq!(effects, vec![ServiceLifecycleEffect::OpenExternalAdmission]);
    }
}
