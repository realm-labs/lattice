use bytes::Bytes;
use lattice_core::actor_ref::{ActorRef, EntityId, NodeIncarnation};
use lattice_placement::{
    AuthorityEffect, AuthorityEvent, BufferedMessage, BufferedMessageMode, CoordinatorDelta,
    CoordinatorSession, HandoffEffect, HandoffEvent, HandoffMachine, MonotonicTime,
    PlacementAuthority, RebalancePlan, RouteDecision, ShardHome, ShardId, ShardRegion,
    SingletonManager,
};
use lattice_remoting::{
    AssociationId, CommandId, ControlApply, ControlEnvelope, ExactActorTarget, ReliableControl,
    WatchCommand, WatchId, WatchRegistry,
};
use lattice_service::{
    ServiceLifecycle, ServiceLifecycleEffect, ServiceLifecycleError, ServiceLifecycleEvent,
    ServiceLifecycleState,
};

pub struct ControlAdapter {
    reducer: ReliableControl,
}

impl ControlAdapter {
    pub fn new(
        epoch: AssociationId,
        maximum_frames: usize,
        maximum_bytes: usize,
    ) -> Result<Self, lattice_remoting::control::ReliableControlError> {
        Ok(Self {
            reducer: ReliableControl::new(epoch, maximum_frames, maximum_bytes)?,
        })
    }

    pub fn enqueue(
        &mut self,
        command_id: CommandId,
        payload: Bytes,
    ) -> Result<ControlEnvelope, lattice_remoting::control::ReliableControlError> {
        self.reducer.enqueue(command_id, payload)
    }

    pub fn receive(&mut self, envelope: ControlEnvelope) -> ControlApply {
        self.reducer.receive(envelope)
    }
}

#[derive(Default)]
pub struct SessionAdapter {
    reducer: CoordinatorSession,
}

impl SessionAdapter {
    pub fn install(
        &mut self,
        snapshot: lattice_placement::coordinator::SnapshotInstall,
    ) -> Result<(), lattice_placement::coordinator::CoordinatorError> {
        self.reducer.install(snapshot)
    }

    pub fn apply(
        &mut self,
        delta: CoordinatorDelta,
    ) -> Result<(), lattice_placement::coordinator::CoordinatorError> {
        self.reducer.apply_delta(delta)
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

    pub fn step(
        &mut self,
        event: AuthorityEvent,
    ) -> Result<Vec<AuthorityEffect>, lattice_placement::authority::AuthorityError> {
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
    ) -> Result<RouteDecision, lattice_placement::RegionError> {
        self.reducer
            .route(entity_id, message_id, mode, payload, now)
    }

    pub fn install_home(
        &mut self,
        shard_id: ShardId,
        home: ShardHome,
    ) -> Result<Vec<BufferedMessage>, lattice_placement::RegionError> {
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

    pub fn step(
        &mut self,
        event: HandoffEvent,
    ) -> Result<Vec<HandoffEffect>, lattice_placement::HandoffError> {
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
        generation: lattice_placement::AssignmentGeneration,
        active_move: Option<u128>,
    ) -> Result<(), lattice_placement::PlanError> {
        self.reducer.begin_move(shard_id, generation, active_move)
    }

    pub fn complete(&mut self, shard_id: ShardId) -> Result<(), lattice_placement::PlanError> {
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

    pub fn step(
        &mut self,
        event: AuthorityEvent,
    ) -> Result<Vec<AuthorityEffect>, lattice_placement::SingletonError> {
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
pub struct ServiceLifecycleAdapter {
    reducer: ServiceLifecycle,
}

impl ServiceLifecycleAdapter {
    pub fn step(
        &mut self,
        event: ServiceLifecycleEvent,
    ) -> Result<Vec<ServiceLifecycleEffect>, ServiceLifecycleError> {
        self.reducer.transition(event)
    }

    pub fn state(&self) -> ServiceLifecycleState {
        self.reducer.state()
    }
}

impl WatchAdapter {
    pub fn new(reducer: WatchRegistry) -> Self {
        Self { reducer }
    }

    pub fn watch<A>(
        &mut self,
        association: AssociationId,
        target: &ActorRef<A>,
    ) -> Result<(WatchId, WatchCommand), lattice_remoting::WatchError> {
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
        let mut adapter = ServiceLifecycleAdapter::default();
        adapter.step(ServiceLifecycleEvent::RemotingReady).unwrap();
        let effects = adapter
            .step(ServiceLifecycleEvent::SnapshotInstalled)
            .unwrap();
        assert_eq!(adapter.state(), ServiceLifecycleState::Ready);
        assert_eq!(effects, vec![ServiceLifecycleEffect::OpenExternalAdmission]);
    }
}
