use std::collections::BTreeSet;

use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use lattice_placement::{
    AssignmentGeneration, HandoffEffect, HandoffEvent, HandoffMachine, HandoffPhase, NodeKey,
    PlacementSlotKey, Revision, ShardId,
};
use lattice_remoting::{
    AssociationId, CommandId, ControlApply, ControlEnvelope, ExactActorTarget, ReliableControl,
    WatchRegistry, WatchStatus,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::clock::{SimClock, SimScheduler};
use crate::fault::{FailAction, Failpoint, FaultInjector};
use crate::trace::{TraceEvent, TraceJournal};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub seed: u64,
    pub maximum_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScenarioEvent {
    ApplyBarrier(NodeIncarnation),
    FenceBarrier(NodeIncarnation),
    SourceInvalid,
    TargetClaimInstalled,
    TargetReady,
    DeliverControl(ControlEnvelope),
    DuplicateControl(ControlEnvelope),
    NodeDown(NodeIncarnation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioState {
    pub source_incarnation: u128,
    pub target_incarnation: u128,
    pub assignment_generation: u64,
    pub phase: String,
    pub claim_owner_incarnation: Option<u128>,
    pub running: bool,
    pub terminal_watches: usize,
    pub applied_control_commands: usize,
}

pub struct Scenario {
    pub config: ScenarioConfig,
    pub clock: SimClock,
    pub trace: TraceJournal,
    pub faults: FaultInjector,
    state: ScenarioState,
    scheduler: SimScheduler<ScenarioEvent>,
    handoff: HandoffMachine,
    control: ReliableControl,
    watches: WatchRegistry,
    watch_id: lattice_remoting::WatchId,
}

impl Scenario {
    pub fn standard(config: ScenarioConfig) -> Result<Self, ScenarioError> {
        if config.maximum_events == 0 {
            return Err(ScenarioError::InvalidConfig);
        }
        let source = node("source", 1, 28001);
        let target = node("target", 2, 28002);
        let barrier = [source.incarnation, target.incarnation]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let handoff = HandoffMachine::begin(
            PlacementSlotKey::Shard {
                entity_type: lattice_core::actor_ref::EntityType::new("sim-entity").unwrap(),
                shard_id: ShardId::new(1),
            },
            1,
            source.clone(),
            target.clone(),
            AssignmentGeneration::new(1).unwrap(),
            Revision::new(2).unwrap(),
            barrier,
        )
        .map_err(ScenarioError::Handoff)?;
        let actor = actor_ref(&source);
        let mut watches = WatchRegistry::new(16, 16).map_err(ScenarioError::Watch)?;
        let (watch_id, _) = watches
            .watch(AssociationId::new(1).unwrap(), &actor)
            .map_err(ScenarioError::Watch)?;
        let watched_target = ExactActorTarget::from(&actor);
        watches.receive_ack(watch_id, &watched_target);
        let configuration = serde_json::to_value(&config).map_err(|_| ScenarioError::Codec)?;
        let trace = TraceJournal::new(
            "standard-handoff",
            config.seed,
            configuration,
            config.maximum_events,
        )
        .ok_or(ScenarioError::InvalidConfig)?;
        Ok(Self {
            scheduler: SimScheduler::new(config.seed),
            config,
            clock: SimClock::new(),
            trace,
            faults: FaultInjector::default(),
            state: ScenarioState {
                source_incarnation: 1,
                target_incarnation: 2,
                assignment_generation: 1,
                phase: "invalidating".to_owned(),
                claim_owner_incarnation: Some(1),
                running: false,
                terminal_watches: 0,
                applied_control_commands: 0,
            },
            handoff,
            control: ReliableControl::new(AssociationId::new(1).unwrap(), 32, 4096)
                .map_err(ScenarioError::Control)?,
            watches,
            watch_id,
        })
    }

    pub fn schedule(&mut self, at_millis: u64, event: ScenarioEvent) {
        self.scheduler.schedule(at_millis, event);
    }

    pub fn enqueue_control(&mut self, value: u128) -> Result<ControlEnvelope, ScenarioError> {
        self.control
            .enqueue(
                CommandId::new(value).ok_or(ScenarioError::InvalidConfig)?,
                bytes::Bytes::from_static(b"command"),
            )
            .map_err(ScenarioError::Control)
    }

    pub fn schedule_standard_workload(&mut self) -> Result<(), ScenarioError> {
        let control = self.enqueue_control(1)?;
        self.schedule(
            1,
            ScenarioEvent::ApplyBarrier(NodeIncarnation::new(1).unwrap()),
        );
        self.schedule(
            1,
            ScenarioEvent::ApplyBarrier(NodeIncarnation::new(2).unwrap()),
        );
        self.schedule(2, ScenarioEvent::SourceInvalid);
        self.schedule(3, ScenarioEvent::TargetClaimInstalled);
        self.schedule(4, ScenarioEvent::TargetReady);
        self.schedule(5, ScenarioEvent::DeliverControl(control.clone()));
        self.schedule(5, ScenarioEvent::DuplicateControl(control));
        self.schedule(6, ScenarioEvent::NodeDown(NodeIncarnation::new(1).unwrap()));
        self.schedule(7, ScenarioEvent::NodeDown(NodeIncarnation::new(1).unwrap()));
        Ok(())
    }

    pub fn run(&mut self) -> Result<&ScenarioState, ScenarioError> {
        while let Some((at, event)) = self.scheduler.pop_next() {
            self.clock.advance_to(at);
            self.step(event)?;
            self.check_invariants().map_err(ScenarioError::Invariant)?;
        }
        Ok(&self.state)
    }

    pub fn state(&self) -> &ScenarioState {
        &self.state
    }

    pub fn step(&mut self, event: ScenarioEvent) -> Result<(), ScenarioError> {
        let previous = self.state.phase.clone();
        let kind = format!("{event:?}");
        match event {
            ScenarioEvent::ApplyBarrier(session) => {
                let effects = self
                    .handoff
                    .transition(HandoffEvent::AppliedRevision {
                        session,
                        revision: Revision::new(2).unwrap(),
                    })
                    .map_err(ScenarioError::Handoff)?;
                self.apply_handoff_effects(effects)?;
            }
            ScenarioEvent::FenceBarrier(session) => {
                let effects = self
                    .handoff
                    .transition(HandoffEvent::FenceSession(session))
                    .map_err(ScenarioError::Handoff)?;
                self.apply_handoff_effects(effects)?;
            }
            ScenarioEvent::SourceInvalid => {
                if self
                    .faults
                    .hit(Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke)
                    == FailAction::Crash
                {
                    return Ok(());
                }
                self.state.claim_owner_incarnation = None;
                let effects = self
                    .handoff
                    .transition(HandoffEvent::SourceAuthorityInvalid {
                        source: node("source", 1, 28001),
                        generation: AssignmentGeneration::new(1).unwrap(),
                    })
                    .map_err(ScenarioError::Handoff)?;
                self.apply_handoff_effects(effects)?;
            }
            ScenarioEvent::TargetClaimInstalled => {
                self.state.assignment_generation = 2;
                self.state.claim_owner_incarnation = Some(2);
                self.handoff
                    .transition(HandoffEvent::TargetClaimInstalled {
                        target: node("target", 2, 28002),
                        generation: AssignmentGeneration::new(2).unwrap(),
                    })
                    .map_err(ScenarioError::Handoff)?;
            }
            ScenarioEvent::TargetReady => {
                let effects = self
                    .handoff
                    .transition(HandoffEvent::TargetReady {
                        target: node("target", 2, 28002),
                        generation: AssignmentGeneration::new(2).unwrap(),
                    })
                    .map_err(ScenarioError::Handoff)?;
                self.apply_handoff_effects(effects)?;
            }
            ScenarioEvent::DeliverControl(envelope) | ScenarioEvent::DuplicateControl(envelope) => {
                if let ControlApply::Apply(_) = self.control.receive(envelope) {
                    self.state.applied_control_commands += 1;
                }
            }
            ScenarioEvent::NodeDown(incarnation) => {
                self.state.terminal_watches += self.watches.node_down(incarnation).len();
            }
        }
        self.state.phase = phase_name(self.handoff.phase).to_owned();
        let pushed = self.trace.push(TraceEvent {
            index: 0,
            causal_parents: self
                .trace
                .events
                .last()
                .map(|event| vec![event.index])
                .unwrap_or_default(),
            time_millis: self.clock.now_millis(),
            node: "coordinator".to_owned(),
            kind,
            previous,
            next: self.state.phase.clone(),
            operation_id: Some(self.handoff.plan_id.to_string()),
        });
        if !pushed {
            return Err(ScenarioError::TraceCapacity);
        }
        Ok(())
    }

    fn apply_handoff_effects(&mut self, effects: Vec<HandoffEffect>) -> Result<(), ScenarioError> {
        for effect in effects {
            match effect {
                HandoffEffect::DrainSource => {}
                HandoffEffect::ReplaceAuthority => {}
                HandoffEffect::PublishActive => self.state.running = true,
                HandoffEffect::StopFailed => return Err(ScenarioError::UnexpectedStopFailure),
            }
        }
        Ok(())
    }

    pub fn check_invariants(&self) -> Result<(), InvariantViolation> {
        if self.state.running
            && (self.state.claim_owner_incarnation != Some(self.state.target_incarnation)
                || self.state.assignment_generation != 2
                || self.handoff.phase != HandoffPhase::Completed)
        {
            return Err(InvariantViolation::RunningWithoutTargetClaim);
        }
        if self.state.claim_owner_incarnation == Some(self.state.source_incarnation)
            && self.state.assignment_generation > 1
        {
            return Err(InvariantViolation::StaleOwnerRegainedAdmission);
        }
        if self.state.terminal_watches > 1
            || (self.state.terminal_watches == 1
                && self.watches.status(self.watch_id) != WatchStatus::Terminated)
        {
            return Err(InvariantViolation::DuplicateWatchTerminal);
        }
        Ok(())
    }
}

fn phase_name(phase: HandoffPhase) -> &'static str {
    match phase {
        HandoffPhase::Invalidating => "invalidating",
        HandoffPhase::Draining => "draining",
        HandoffPhase::ReplacingAuthority => "replacing-authority",
        HandoffPhase::Starting => "starting",
        HandoffPhase::Completed => "completed",
    }
}

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn actor_ref(node: &NodeKey) -> ActorRef<()> {
    ActorRef::new(
        ClusterId::new("sim-cluster").unwrap(),
        node.address.clone(),
        node.incarnation,
        ActorPath::user(["user", "simulated"]).unwrap(),
        ActivationId::new(node.incarnation, 1).unwrap(),
        ProtocolId::new(1).unwrap(),
    )
    .unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvariantViolation {
    #[error("running authority has no exact target generation claim")]
    RunningWithoutTargetClaim,
    #[error("stale owner regained admission")]
    StaleOwnerRegainedAdmission,
    #[error("watch terminal delivery was duplicated or not retained")]
    DuplicateWatchTerminal,
}

#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error("scenario configuration is invalid")]
    InvalidConfig,
    #[error("scenario trace capacity is exhausted")]
    TraceCapacity,
    #[error("scenario serialization failed")]
    Codec,
    #[error("scenario observed an unexpected voluntary stop failure")]
    UnexpectedStopFailure,
    #[error(transparent)]
    Handoff(#[from] lattice_placement::HandoffError),
    #[error(transparent)]
    Control(#[from] lattice_remoting::control::ReliableControlError),
    #[error(transparent)]
    Watch(#[from] lattice_remoting::WatchError),
    #[error(transparent)]
    Invariant(#[from] InvariantViolation),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::{
        Explorable, Failpoint, FaultMatrix, FaultTarget, ProcessState, SimEtcd, SimNetwork,
        SimProcess, StateExplorer,
    };

    fn run(seed: u64) -> Scenario {
        let mut scenario = Scenario::standard(ScenarioConfig {
            seed,
            maximum_events: 64,
        })
        .unwrap();
        scenario.schedule_standard_workload().unwrap();
        scenario.run().unwrap();
        scenario
    }

    #[test]
    fn same_seed_replays_identical_production_reducer_trace() {
        let first = run(44);
        let second = run(44);
        assert_eq!(first.state(), second.state());
        assert_eq!(first.trace, second.trace);
        assert!(first.state().running);
        assert_eq!(first.state().applied_control_commands, 1);
        assert_eq!(first.state().terminal_watches, 1);
    }

    #[test]
    fn trace_shrinking_preserves_one_command_reproduction() {
        let scenario = run(9);
        let shrunk = scenario.trace.shrink(|events| {
            events
                .iter()
                .any(|event| event.kind.contains("TargetReady"))
        });
        assert_eq!(shrunk.events.len(), 1);
        assert!(shrunk.events[0].kind.contains("TargetReady"));
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TinyOwnership {
        generation: u8,
        owner: Option<u8>,
        ready: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    enum TinyEvent {
        Fence,
        Claim,
        Ready,
    }

    impl Explorable for TinyOwnership {
        type Event = TinyEvent;
        type Error = ();

        fn enabled(&self) -> Vec<Self::Event> {
            if self.owner == Some(1) {
                vec![TinyEvent::Fence]
            } else if self.owner.is_none() {
                vec![TinyEvent::Claim]
            } else if !self.ready {
                vec![TinyEvent::Ready]
            } else {
                Vec::new()
            }
        }

        fn step(&self, event: &Self::Event) -> Result<Self, Self::Error> {
            let mut next = self.clone();
            match event {
                TinyEvent::Fence if self.owner == Some(1) => next.owner = None,
                TinyEvent::Claim if self.owner.is_none() => {
                    next.owner = Some(2);
                    next.generation = 2;
                }
                TinyEvent::Ready if self.owner == Some(2) => next.ready = true,
                _ => return Err(()),
            }
            Ok(next)
        }

        fn invariant(&self) -> Result<(), String> {
            if self.ready && (self.owner != Some(2) || self.generation != 2) {
                Err("ready owner lacks exact generation claim".to_owned())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn bounded_state_explorer_checks_every_transition() {
        let report = StateExplorer {
            maximum_states: 16,
            maximum_depth: 8,
        }
        .explore(TinyOwnership {
            generation: 1,
            owner: Some(1),
            ready: false,
        })
        .unwrap();
        assert_eq!(report.visited_states, 4);
        assert_eq!(report.maximum_depth_reached, 3);
    }

    #[test]
    fn simulated_etcd_cas_leases_watch_and_compaction_are_revisioned() {
        let mut etcd = SimEtcd::new(8).unwrap();
        let lease = etcd.grant_lease(0, 10).unwrap();
        let revision = etcd
            .compare_and_put(
                "claim".to_owned(),
                None,
                bytes::Bytes::from_static(b"one"),
                Some(lease),
            )
            .unwrap();
        assert_eq!(revision, 1);
        assert!(
            etcd.compare_and_put("claim".to_owned(), None, bytes::Bytes::new(), None)
                .is_err()
        );
        assert_eq!(etcd.expire_leases(10).unwrap(), vec!["claim"]);
        etcd.compact(2);
        assert!(matches!(
            etcd.watch_from(1).as_slice(),
            [crate::SimWatchEvent::Compacted { compacted: 2, .. }]
        ));
    }

    #[test]
    fn required_failpoint_matrix_is_machine_checked() {
        let mut matrix = FaultMatrix::required_default();
        for point in Failpoint::ALL {
            let targets: &[FaultTarget] = match point {
                Failpoint::AssociationAfterHandshakeBeforeCatalogue
                | Failpoint::ControlAfterOutboxBeforeSocketWrite
                | Failpoint::ControlAfterRemoteApplyBeforeAck
                | Failpoint::WatchAfterInstallBeforeAck
                | Failpoint::WatchAfterTerminatedBeforeAck => &[
                    FaultTarget::Network,
                    FaultTarget::Queue,
                    FaultTarget::Target,
                ],
                Failpoint::ShutdownAfterFenceBeforeTaskJoin => {
                    &[FaultTarget::Source, FaultTarget::Coordinator]
                }
                _ => &[
                    FaultTarget::Coordinator,
                    FaultTarget::Source,
                    FaultTarget::Target,
                    FaultTarget::Store,
                    FaultTarget::Network,
                ],
            };
            for target in targets {
                let observed = Arc::new(Mutex::new(Vec::new()));
                let captured = observed.clone();
                let _guard = lattice_core::failpoint::install_hook(move |observed_point| {
                    captured.lock().unwrap().push(observed_point);
                });
                lattice_core::failpoint::hit(point);
                assert_eq!(observed.lock().unwrap().as_slice(), &[point]);
                exercise_fault_adapter(*target);
                matrix.record(point, *target);
            }
        }
        assert_eq!(matrix.missing().count(), 0);
    }

    fn exercise_fault_adapter(target: FaultTarget) {
        match target {
            FaultTarget::Coordinator | FaultTarget::Source | FaultTarget::Target => {
                let mut process = SimProcess {
                    node_id: format!("{target:?}"),
                    address: NodeAddress::new("127.0.0.1", 29000).unwrap(),
                    incarnation: NodeIncarnation::new(1).unwrap(),
                    state: ProcessState::Running,
                };
                process.crash();
                assert_eq!(process.state, ProcessState::Crashed);
                process.restart(NodeIncarnation::new(2).unwrap());
                assert_eq!(process.incarnation.get(), 2);
            }
            FaultTarget::Store => {
                let mut store = SimEtcd::new(1).unwrap();
                store
                    .compare_and_put(
                        "one".to_owned(),
                        None,
                        bytes::Bytes::from_static(b"one"),
                        None,
                    )
                    .unwrap();
                assert!(
                    store
                        .compare_and_put(
                            "two".to_owned(),
                            None,
                            bytes::Bytes::from_static(b"two"),
                            None,
                        )
                        .is_err()
                );
            }
            FaultTarget::Network => {
                let mut network = SimNetwork::new(1).unwrap();
                network.partition("source", "target");
                let frame = network
                    .send("source", "target", bytes::Bytes::from_static(b"frame"))
                    .unwrap();
                assert!(network.deliver(frame).is_none());
            }
            FaultTarget::Queue => {
                let mut network = SimNetwork::new(1).unwrap();
                network
                    .send("source", "target", bytes::Bytes::from_static(b"one"))
                    .unwrap();
                assert!(
                    network
                        .send("source", "target", bytes::Bytes::from_static(b"two"))
                        .is_none()
                );
            }
        }
    }
}
