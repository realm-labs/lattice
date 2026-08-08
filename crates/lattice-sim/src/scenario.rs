//! The standard handoff scenario.
//!
//! [`Scenario`] owns every simulated resource — the clock, the scheduler, the production handoff
//! reducer, both control endpoints, the network and the watch registry — and drives them from a
//! single deterministic event loop. The type definitions and that loop stay here; the work each
//! event performs is grouped into sibling modules by responsibility, so the published paths are
//! independent of that grouping.

use std::collections::BTreeSet;

use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, EntityType, NodeAddress, NodeIncarnation,
    PlacementDomainId, ProtocolId,
};
use lattice_placement::{
    handoff::{HandoffError, HandoffMachine, HandoffPhase},
    types::{
        AssignmentGeneration, CoordinatorTerm, NodeKey, PlacementSlotKey, PlacementVersion,
        Revision, ShardId,
    },
};
use lattice_remoting::{
    association::AssociationId,
    control::{ReliableControl, ReliableControlError},
    messaging::target::ExactActorTarget,
    watch::{WatchError, WatchId, WatchRegistry, WatchStatus},
    wire::{FrameCodec, WireError},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    clock::{SimClock, SimRandom, SimScheduler},
    fault::{FaultEvidence, SharedFaultInjector},
    network::SimNetwork,
    process::{ProcessState, SimProcess},
    trace::{TraceEvent, TraceJournal},
};

mod control;
mod handoff;
mod injection;
#[cfg(test)]
mod tests;
mod watch;
mod workload;

const DELIVERY_STREAM: u64 = 0xBF58_476D_1CE4_E5B9;
const COORDINATOR: &str = "coordinator";
const MEMBER: &str = "member";
const MAXIMUM_FRAMES: usize = 8;
const MAXIMUM_ATTEMPTS: u8 = 6;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub seed: u64,
    pub maximum_events: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HandoffStep {
    ApplyBarrier(NodeIncarnation),
    FenceBarrier(NodeIncarnation),
    SourceInvalid,
    TargetClaimInstalled,
    TargetReady,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScenarioEvent {
    Handoff(HandoffStep, u8),
    PublishActive(u8),
    SendControl(u128),
    ReplayControl,
    DeliverFrame(u64),
    PartitionControl,
    HealControl,
    InstallWatch,
    TargetTerminated,
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
    pub rejected_transitions: usize,
    pub duplicate_control_commands: usize,
    pub lost_control_frames: usize,
    pub injected_faults: usize,
    pub unhonoured_injections: usize,
    pub watch_acknowledged: bool,
}

pub struct Scenario {
    pub config: ScenarioConfig,
    pub clock: SimClock,
    pub trace: TraceJournal,
    pub faults: SharedFaultInjector,
    state: ScenarioState,
    scheduler: SimScheduler<ScenarioEvent>,
    delivery: SimRandom,
    handoff: HandoffMachine,
    coordinator: ReliableControl,
    member: ReliableControl,
    network: SimNetwork,
    codec: FrameCodec,
    coordinator_process: SimProcess,
    source_process: SimProcess,
    target_process: SimProcess,
    watches: WatchRegistry,
    watch_id: WatchId,
    watch_target: ExactActorTarget,
    commands: BTreeSet<u128>,
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
                domain: placement_domain(),
                entity_type: EntityType::new("sim-entity").unwrap(),
                shard_id: ShardId::new(1),
            },
            1,
            source.clone(),
            target.clone(),
            AssignmentGeneration::new(1).unwrap(),
            PlacementVersion::new(
                placement_domain(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(2).unwrap(),
            ),
            barrier,
        )
        .map_err(ScenarioError::Handoff)?;
        let actor = actor_ref(&source);
        let mut watches = WatchRegistry::new(16, 16).map_err(ScenarioError::Watch)?;
        let (registered_watch, _) = watches
            .watch(AssociationId::new(1).unwrap(), &actor)
            .map_err(ScenarioError::Watch)?;
        let watch_id = registered_watch.id();
        let configuration = serde_json::to_value(&config).map_err(|_| ScenarioError::Codec)?;
        let trace = TraceJournal::new(
            "standard-handoff",
            config.seed,
            configuration,
            config.maximum_events,
        )
        .ok_or(ScenarioError::InvalidConfig)?;
        let epoch = AssociationId::new(1).unwrap();
        Ok(Self {
            scheduler: SimScheduler::new(config.seed),
            delivery: SimRandom::new(config.seed ^ DELIVERY_STREAM),
            clock: SimClock::new(),
            trace,
            faults: SharedFaultInjector::default(),
            state: ScenarioState {
                source_incarnation: 1,
                target_incarnation: 2,
                assignment_generation: 1,
                phase: "invalidating".to_owned(),
                claim_owner_incarnation: Some(1),
                running: false,
                terminal_watches: 0,
                applied_control_commands: 0,
                rejected_transitions: 0,
                duplicate_control_commands: 0,
                lost_control_frames: 0,
                injected_faults: 0,
                unhonoured_injections: 0,
                watch_acknowledged: false,
            },
            handoff,
            coordinator: ReliableControl::new(epoch, MAXIMUM_FRAMES, 4096)
                .map_err(ScenarioError::Control)?,
            member: ReliableControl::new(epoch, MAXIMUM_FRAMES, 4096)
                .map_err(ScenarioError::Control)?,
            network: SimNetwork::new(MAXIMUM_FRAMES).ok_or(ScenarioError::InvalidConfig)?,
            codec: FrameCodec::new(4096).map_err(ScenarioError::Wire)?,
            coordinator_process: process(COORDINATOR, 3, 28003),
            source_process: process("source", 1, 28001),
            target_process: process("target", 2, 28002),
            watch_target: ExactActorTarget::from(&actor),
            watches,
            watch_id,
            commands: BTreeSet::new(),
            config,
        })
    }

    pub fn schedule(&mut self, at_millis: u64, event: ScenarioEvent) {
        self.scheduler.schedule(at_millis, event);
    }

    pub fn run(&mut self) -> Result<&ScenarioState, ScenarioError> {
        let _injector = self.faults.install();
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
            ScenarioEvent::Handoff(step, attempts) => self.apply_handoff_step(step, attempts)?,
            ScenarioEvent::PublishActive(attempts) => self.publish_active(attempts),
            ScenarioEvent::SendControl(command) => self.send_control(command)?,
            ScenarioEvent::ReplayControl => self.replay_control(),
            ScenarioEvent::DeliverFrame(frame) => self.deliver_frame(frame)?,
            ScenarioEvent::PartitionControl => {
                self.network.partition(COORDINATOR, MEMBER);
                self.network.partition(MEMBER, COORDINATOR);
            }
            ScenarioEvent::HealControl => {
                self.network.heal(COORDINATOR, MEMBER);
                self.network.heal(MEMBER, COORDINATOR);
            }
            ScenarioEvent::InstallWatch => self.install_watch()?,
            ScenarioEvent::TargetTerminated => self.terminate_watch_target(),
            ScenarioEvent::NodeDown(session) => {
                for termination in self.watches.node_down(session) {
                    if termination.watch_id == self.watch_id {
                        self.state.terminal_watches += 1;
                    }
                }
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
            node: COORDINATOR.to_owned(),
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

    pub fn evidence(&self) -> Vec<FaultEvidence> {
        self.faults.evidence()
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
        if self.state.running && self.target_process.state != ProcessState::Running {
            return Err(InvariantViolation::ActivePublishedForUnavailableTarget);
        }
        if self.state.terminal_watches > 1
            || (self.state.terminal_watches == 1
                && self.watches.status(self.watch_id) != WatchStatus::Terminated)
        {
            return Err(InvariantViolation::DuplicateWatchTerminal);
        }
        if self.state.applied_control_commands > self.commands.len() {
            return Err(InvariantViolation::ReplayedControlCommandApplied);
        }
        if self.state.unhonoured_injections > 0 {
            return Err(InvariantViolation::InjectedDecisionWasIgnored);
        }
        Ok(())
    }
}

fn placement_domain() -> PlacementDomainId {
    PlacementDomainId::new("simulation").unwrap()
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

fn incarnation(value: u128) -> NodeIncarnation {
    NodeIncarnation::new(value).expect("simulation incarnations are positive")
}

fn node(id: &str, value: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: incarnation(value),
    }
}

fn process(id: &str, value: u128, port: u16) -> SimProcess {
    SimProcess {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: incarnation(value),
        state: ProcessState::Running,
    }
}

fn actor_ref(node: &NodeKey) -> ActorRef {
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
    #[error("an Active shard was published for an unavailable target process")]
    ActivePublishedForUnavailableTarget,
    #[error("watch terminal delivery was duplicated or not retained")]
    DuplicateWatchTerminal,
    #[error("a replayed control command was applied twice")]
    ReplayedControlCommandApplied,
    #[error("a production call site ignored an injected failpoint decision")]
    InjectedDecisionWasIgnored,
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
    #[error("a rejected handoff transition still advanced the reducer phase")]
    RejectedTransitionAdvancedPhase,
    #[error(transparent)]
    Handoff(#[from] HandoffError),
    #[error(transparent)]
    Control(#[from] ReliableControlError),
    #[error(transparent)]
    Watch(#[from] WatchError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Invariant(#[from] InvariantViolation),
}
