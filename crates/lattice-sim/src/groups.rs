use std::collections::BTreeMap;

use bytes::Bytes;
use lattice_coordination::coordinator::{
    ActorGroupState, ActorGroupStateError, CoordinatorDelta, SnapshotInstall, SnapshotRecord,
    SnapshotVersion,
};
use lattice_coordination::types::{CoordinatorTerm, PlacementVersion, Revision};
use lattice_model::cluster::ActorGroupId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::clock::{SimClock, SimRandom, SimScheduler};
use crate::trace::{TraceEvent, TraceJournal};

const WORKLOAD_STREAM: u64 = 0x1405_7B7E_F767_814F;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SimGroup {
    Alpha,
    Beta,
}

impl SimGroup {
    const ALL: [Self; 2] = [Self::Alpha, Self::Beta];

    fn id(self) -> ActorGroupId {
        ActorGroupId::new(match self {
            Self::Alpha => "simulation-alpha",
            Self::Beta => "simulation-beta",
        })
        .expect("static simulation group must be valid")
    }

    fn label(self) -> &'static str {
        match self {
            Self::Alpha => "alpha",
            Self::Beta => "beta",
        }
    }
}

fn other_group(group: SimGroup) -> SimGroup {
    match group {
        SimGroup::Alpha => SimGroup::Beta,
        SimGroup::Beta => SimGroup::Alpha,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiGroupScenarioConfig {
    pub seed: u64,
    pub maximum_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiGroupEvent {
    ApplyDelta(SimGroup),
    LoseLeader(SimGroup),
    Campaign { group: SimGroup, host: String },
    InstallSnapshot(SimGroup),
    RejectCrossDomainDelta { target: SimGroup, source: SimGroup },
    AdvanceHandoff(SimGroup),
    MembershipLost,
    MembershipRecovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupScenarioView {
    pub leader: Option<String>,
    pub leader_term: u64,
    pub snapshot_term: u64,
    pub revision: u64,
    pub session_ready: bool,
    pub control_available: bool,
    pub handoff_generation: u64,
    pub records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiGroupScenarioState {
    pub membership_up: bool,
    pub membership_term: u64,
    pub groups: BTreeMap<SimGroup, GroupScenarioView>,
    pub cross_group_rejections: usize,
}

struct DomainPlane {
    reducer: ActorGroupState,
}

pub struct MultiGroupScenario {
    pub config: MultiGroupScenarioConfig,
    pub clock: SimClock,
    pub trace: TraceJournal,
    scheduler: SimScheduler<MultiGroupEvent>,
    planes: BTreeMap<SimGroup, DomainPlane>,
    state: MultiGroupScenarioState,
}

impl MultiGroupScenario {
    pub fn standard(config: MultiGroupScenarioConfig) -> Result<Self, MultiGroupScenarioError> {
        if config.maximum_events == 0 {
            return Err(MultiGroupScenarioError::InvalidConfig);
        }
        let mut planes = BTreeMap::new();
        let mut groups = BTreeMap::new();
        for (group, host) in [(SimGroup::Alpha, "host-a"), (SimGroup::Beta, "host-b")] {
            let mut reducer = ActorGroupState::new(group.id());
            reducer.install(snapshot(group, 1, 1, host))?;
            groups.insert(
                group,
                GroupScenarioView {
                    leader: Some(host.to_owned()),
                    leader_term: 1,
                    snapshot_term: 1,
                    revision: 1,
                    session_ready: true,
                    control_available: true,
                    handoff_generation: 1,
                    records: reducer.records().count(),
                },
            );
            planes.insert(group, DomainPlane { reducer });
        }
        let configuration =
            serde_json::to_value(&config).map_err(|_| MultiGroupScenarioError::Serialization)?;
        let trace = TraceJournal::new(
            "multi-group-isolation",
            config.seed,
            configuration,
            config.maximum_events,
        )
        .ok_or(MultiGroupScenarioError::InvalidConfig)?;
        Ok(Self {
            scheduler: SimScheduler::new(config.seed),
            config,
            clock: SimClock::new(),
            trace,
            planes,
            state: MultiGroupScenarioState {
                membership_up: true,
                membership_term: 1,
                groups,
                cross_group_rejections: 0,
            },
        })
    }

    pub fn schedule(&mut self, at_millis: u64, event: MultiGroupEvent) {
        self.scheduler.schedule(at_millis, event);
    }

    pub fn schedule_acceptance(&mut self) {
        let mut random = SimRandom::new(self.config.seed ^ WORKLOAD_STREAM);
        let mut last = 0;
        for group in SimGroup::ALL {
            let mut at = 1 + u64::try_from(random.below(3)).unwrap_or(0);
            for _ in 0..=random.below(2) {
                self.schedule(at, MultiGroupEvent::ApplyDelta(group));
                at += 1;
            }
            if random.chance(2, 3) {
                self.schedule(at, MultiGroupEvent::LoseLeader(group));
                at += 1;
                self.schedule(
                    at,
                    MultiGroupEvent::Campaign {
                        group,
                        host: format!("host-{}-successor", group.label()),
                    },
                );
                at += 1;
                self.schedule(at, MultiGroupEvent::InstallSnapshot(group));
                at += 1;
                for _ in 0..random.below(2) {
                    self.schedule(at, MultiGroupEvent::ApplyDelta(group));
                    at += 1;
                }
            }
            last = last.max(at);
        }
        let target = SimGroup::ALL[random.below(SimGroup::ALL.len())];
        let source = SimGroup::ALL[(random.below(SimGroup::ALL.len()) + 1) % 2];
        self.schedule(
            last,
            MultiGroupEvent::RejectCrossDomainDelta {
                target,
                source: if source == target {
                    other_group(target)
                } else {
                    source
                },
            },
        );
        self.schedule(last + 1, MultiGroupEvent::MembershipLost);
        let mut handoffs = SimGroup::ALL;
        random.shuffle(&mut handoffs);
        for (index, group) in handoffs.into_iter().enumerate() {
            let at = last + 2 + u64::try_from(index * random.below(2)).unwrap_or(0);
            self.schedule(at, MultiGroupEvent::AdvanceHandoff(group));
        }
        self.schedule(last + 4, MultiGroupEvent::MembershipRecovered);
    }

    pub fn run(&mut self) -> Result<&MultiGroupScenarioState, MultiGroupScenarioError> {
        while let Some((at, event)) = self.scheduler.pop_next() {
            self.clock.advance_to(at);
            self.step(event)?;
            self.check_invariants()?;
        }
        Ok(&self.state)
    }

    pub fn state(&self) -> &MultiGroupScenarioState {
        &self.state
    }

    pub fn step(&mut self, event: MultiGroupEvent) -> Result<(), MultiGroupScenarioError> {
        let before = self.state.clone();
        let previous =
            serde_json::to_string(&before).map_err(|_| MultiGroupScenarioError::Serialization)?;
        match &event {
            MultiGroupEvent::ApplyDelta(group) => self.apply_delta(*group, "progress")?,
            MultiGroupEvent::LoseLeader(group) => {
                let view = self.view_mut(*group);
                view.leader = None;
                view.control_available = false;
            }
            MultiGroupEvent::Campaign { group, host } => {
                if !self.state.membership_up {
                    return Err(MultiGroupScenarioError::MembershipUnavailable);
                }
                let view = self.view(*group);
                if view.leader.is_some() {
                    return Err(MultiGroupScenarioError::LeaderAlreadyPresent);
                }
                let next_term = view.leader_term.saturating_add(1);
                let next_revision = view.revision.saturating_add(1);
                let result = self.plane_mut(*group).reducer.apply(CoordinatorDelta {
                    version: placement_version(*group, next_term, next_revision),
                    records: Vec::new(),
                });
                if result != Err(ActorGroupStateError::SnapshotRequired) {
                    return Err(MultiGroupScenarioError::MutationBeforeSnapshot);
                }
                let view = self.view_mut(*group);
                view.leader = Some(host.clone());
                view.leader_term = next_term;
                view.session_ready = false;
                view.control_available = false;
            }
            MultiGroupEvent::InstallSnapshot(group) => {
                let view = self.view(*group).clone();
                let host = view
                    .leader
                    .as_deref()
                    .ok_or(MultiGroupScenarioError::LeaderMissing)?;
                let revision = view.revision.saturating_add(1);
                self.plane_mut(*group).reducer.install(snapshot(
                    *group,
                    view.leader_term,
                    revision,
                    host,
                ))?;
                self.refresh(*group);
                self.view_mut(*group).control_available = true;
            }
            MultiGroupEvent::RejectCrossDomainDelta { target, source } => {
                let revision = self.view(*target).revision.saturating_add(1);
                let source_term = self.view(*source).snapshot_term;
                let result = self.plane_mut(*target).reducer.apply(CoordinatorDelta {
                    version: placement_version(*source, source_term, revision),
                    records: vec![record("cross-group", 1_u64)],
                });
                if result != Err(ActorGroupStateError::GroupMismatch) {
                    return Err(MultiGroupScenarioError::CrossDomainMutationAccepted);
                }
                self.state.cross_group_rejections =
                    self.state.cross_group_rejections.saturating_add(1);
            }
            MultiGroupEvent::AdvanceHandoff(group) => {
                let generation = self.view(*group).handoff_generation.saturating_add(1);
                self.apply_delta(*group, "handoff")?;
                self.view_mut(*group).handoff_generation = generation;
            }
            MultiGroupEvent::MembershipLost => self.state.membership_up = false,
            MultiGroupEvent::MembershipRecovered => {
                self.state.membership_up = true;
                self.state.membership_term = self.state.membership_term.saturating_add(1);
            }
        }
        self.assert_untouched_group(&event, &before)?;
        let next = serde_json::to_string(&self.state)
            .map_err(|_| MultiGroupScenarioError::Serialization)?;
        if !self.trace.push(TraceEvent {
            index: 0,
            causal_parents: self
                .trace
                .events
                .last()
                .map(|event| vec![event.index])
                .unwrap_or_default(),
            time_millis: self.clock.now_millis(),
            node: "coordinator-hosts".to_owned(),
            kind: format!("{event:?}"),
            previous,
            next,
            operation_id: None,
        }) {
            return Err(MultiGroupScenarioError::TraceCapacity);
        }
        Ok(())
    }

    pub fn check_invariants(&self) -> Result<(), MultiGroupScenarioError> {
        for group in SimGroup::ALL {
            let view = self.view(group);
            let reducer = &self
                .planes
                .get(&group)
                .expect("all simulation groups have reducers")
                .reducer;
            let version = reducer
                .version()
                .ok_or(MultiGroupScenarioError::SnapshotMissing)?;
            if version.group != group.id()
                || version.term.get() != view.snapshot_term
                || version.revision.get() != view.revision
                || reducer.ready() != view.session_ready
            {
                return Err(MultiGroupScenarioError::ReducerViewMismatch);
            }
            if view.control_available
                && (view.leader.is_none()
                    || !view.session_ready
                    || view.leader_term != view.snapshot_term)
            {
                return Err(MultiGroupScenarioError::AuthorityWithoutSnapshot);
            }
            if view.handoff_generation == 0 || view.handoff_generation > 2 {
                return Err(MultiGroupScenarioError::InvalidHandoffGeneration);
            }
        }
        Ok(())
    }

    fn apply_delta(
        &mut self,
        group: SimGroup,
        record_key: &str,
    ) -> Result<(), MultiGroupScenarioError> {
        let view = self.view(group).clone();
        if !view.control_available {
            return Err(MultiGroupScenarioError::GroupUnavailable);
        }
        let revision = view.revision.saturating_add(1);
        self.plane_mut(group).reducer.apply(CoordinatorDelta {
            version: placement_version(group, view.snapshot_term, revision),
            records: vec![record(record_key, revision)],
        })?;
        self.refresh(group);
        Ok(())
    }

    fn refresh(&mut self, group: SimGroup) {
        let plane = self
            .planes
            .get(&group)
            .expect("all simulation groups have reducers");
        let version = plane
            .reducer
            .version()
            .expect("installed simulation group has a version");
        let snapshot_term = version.term.get();
        let revision = version.revision.get();
        let session_ready = plane.reducer.ready();
        let records = plane.reducer.records().count();
        let view = self.view_mut(group);
        view.snapshot_term = snapshot_term;
        view.revision = revision;
        view.session_ready = session_ready;
        view.records = records;
    }

    fn assert_untouched_group(
        &self,
        event: &MultiGroupEvent,
        before: &MultiGroupScenarioState,
    ) -> Result<(), MultiGroupScenarioError> {
        let touched = match event {
            MultiGroupEvent::ApplyDelta(group)
            | MultiGroupEvent::LoseLeader(group)
            | MultiGroupEvent::InstallSnapshot(group)
            | MultiGroupEvent::AdvanceHandoff(group)
            | MultiGroupEvent::Campaign { group, .. } => Some(*group),
            MultiGroupEvent::RejectCrossDomainDelta { .. }
            | MultiGroupEvent::MembershipLost
            | MultiGroupEvent::MembershipRecovered => None,
        };
        for group in SimGroup::ALL {
            if touched != Some(group) && self.state.groups.get(&group) != before.groups.get(&group)
            {
                return Err(MultiGroupScenarioError::CrossDomainMutationAccepted);
            }
        }
        Ok(())
    }

    fn plane_mut(&mut self, group: SimGroup) -> &mut DomainPlane {
        self.planes
            .get_mut(&group)
            .expect("all simulation groups have reducers")
    }

    fn view(&self, group: SimGroup) -> &GroupScenarioView {
        self.state
            .groups
            .get(&group)
            .expect("all simulation groups have views")
    }

    fn view_mut(&mut self, group: SimGroup) -> &mut GroupScenarioView {
        self.state
            .groups
            .get_mut(&group)
            .expect("all simulation groups have views")
    }
}

fn placement_version(group: SimGroup, term: u64, revision: u64) -> PlacementVersion {
    PlacementVersion::new(
        group.id(),
        CoordinatorTerm::new(term).expect("simulation term is positive"),
        Revision::new(revision).expect("simulation revision is positive"),
    )
}

fn snapshot(group: SimGroup, term: u64, revision: u64, host: &str) -> SnapshotInstall {
    SnapshotInstall {
        version: SnapshotVersion::Placement(placement_version(group, term, revision)),
        records: vec![SnapshotRecord {
            key: "leader".to_owned(),
            value: Bytes::copy_from_slice(host.as_bytes()),
        }],
    }
}

fn record(key: &str, value: u64) -> SnapshotRecord {
    SnapshotRecord {
        key: key.to_owned(),
        value: Bytes::copy_from_slice(&value.to_be_bytes()),
    }
}

#[derive(Debug, Error)]
pub enum MultiGroupScenarioError {
    #[error("multi-group scenario configuration is invalid")]
    InvalidConfig,
    #[error("multi-group trace capacity is exhausted")]
    TraceCapacity,
    #[error("multi-group scenario serialization failed")]
    Serialization,
    #[error("membership is unavailable for a new campaign")]
    MembershipUnavailable,
    #[error("group already has a leader")]
    LeaderAlreadyPresent,
    #[error("group has no elected leader")]
    LeaderMissing,
    #[error("group control is unavailable")]
    GroupUnavailable,
    #[error("new-term placement mutation was accepted before a snapshot")]
    MutationBeforeSnapshot,
    #[error("a cross-group placement delta was accepted")]
    CrossDomainMutationAccepted,
    #[error("group reducer has no installed snapshot")]
    SnapshotMissing,
    #[error("group reducer state diverged from the simulation view")]
    ReducerViewMismatch,
    #[error("group authority became available without its exact-term snapshot")]
    AuthorityWithoutSnapshot,
    #[error("group handoff generation is outside its bounded scenario range")]
    InvalidHandoffGeneration,
    #[error(transparent)]
    PlacementState(#[from] ActorGroupStateError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::explorer::{Explorable, StateExplorer};

    fn run(seed: u64) -> MultiGroupScenario {
        let mut scenario = MultiGroupScenario::standard(MultiGroupScenarioConfig {
            seed,
            maximum_events: 64,
        })
        .unwrap();
        scenario.schedule_acceptance();
        scenario.run().unwrap();
        scenario
    }

    #[test]
    fn multi_group_trace_replays_independent_elections_and_handoffs() {
        let first = run(71);
        let second = run(71);
        assert_eq!(first.state(), second.state());
        assert_eq!(first.trace, second.trace);
        assert_eq!(first.state().cross_group_rejections, 1);
        for group in SimGroup::ALL {
            let view = &first.state().groups[&group];
            assert!(view.leader.is_some());
            assert_eq!(view.leader_term, view.snapshot_term);
            assert_eq!(view.handoff_generation, 2);
        }
    }

    #[test]
    fn seeded_multi_group_workloads_explore_independent_election_orders() {
        let mut elected = 0;
        let signatures = (1..=32)
            .map(|seed| {
                let scenario = run(seed);
                elected += scenario
                    .state()
                    .groups
                    .values()
                    .filter(|view| view.leader_term > 1)
                    .count();
                scenario
                    .trace
                    .events
                    .iter()
                    .map(|event| format!("{}@{}", event.kind, event.time_millis))
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(signatures.len() >= 24, "only {} traces", signatures.len());
        assert!(elected > 0);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct DomainView {
        leader_term: u64,
        installed_term: u64,
        revision: u64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct MultiGroupExploration {
        groups: [DomainView; 2],
        cross_group_rejections: u8,
        gate_rejections: u8,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum ReducerEvent {
        Progress(usize),
        Elect(usize),
        InstallSnapshot(usize),
        CrossGroupDelta(usize),
        StaleDelta(usize),
    }

    impl MultiGroupExploration {
        fn materialize(&self, index: usize) -> ActorGroupState {
            let group = SimGroup::ALL[index];
            let view = self.groups[index];
            let mut reducer = ActorGroupState::new(group.id());
            reducer
                .install(snapshot(group, view.installed_term, view.revision, "host"))
                .expect("materialized group snapshot is installable");
            reducer
        }
    }

    impl Explorable for MultiGroupExploration {
        type Event = ReducerEvent;
        type Error = ();

        fn enabled(&self) -> Vec<Self::Event> {
            let mut events = Vec::new();
            for (index, view) in self.groups.iter().enumerate() {
                if view.revision < 4 {
                    events.push(ReducerEvent::Progress(index));
                    events.push(ReducerEvent::StaleDelta(index));
                }
                if view.leader_term < 2 {
                    events.push(ReducerEvent::Elect(index));
                }
                if view.installed_term < view.leader_term && view.revision < 4 {
                    events.push(ReducerEvent::InstallSnapshot(index));
                }
                if self.cross_group_rejections < 2 {
                    events.push(ReducerEvent::CrossGroupDelta(index));
                }
            }
            events
        }

        fn step(&self, event: &Self::Event) -> Result<Self, Self::Error> {
            let mut next = *self;
            let index = match *event {
                ReducerEvent::Progress(index)
                | ReducerEvent::Elect(index)
                | ReducerEvent::InstallSnapshot(index)
                | ReducerEvent::CrossGroupDelta(index)
                | ReducerEvent::StaleDelta(index) => index,
            };
            let group = SimGroup::ALL[index];
            let view = self.groups[index];
            let other = self.materialize(1 - index);
            let mut reducer = self.materialize(index);
            match *event {
                ReducerEvent::Elect(_) => next.groups[index].leader_term += 1,
                ReducerEvent::Progress(_) => {
                    let result = reducer.apply(CoordinatorDelta {
                        version: placement_version(group, view.leader_term, view.revision + 1),
                        records: vec![record("progress", view.revision + 1)],
                    });
                    if view.leader_term == view.installed_term {
                        result.map_err(|_| ())?;
                        if !reducer.ready() {
                            return Err(());
                        }
                        next.groups[index].revision += 1;
                    } else {
                        if result != Err(ActorGroupStateError::SnapshotRequired) || reducer.ready()
                        {
                            return Err(());
                        }
                        next.gate_rejections = next.gate_rejections.saturating_add(1);
                    }
                }
                ReducerEvent::StaleDelta(_) => {
                    if reducer.apply(CoordinatorDelta {
                        version: placement_version(group, view.installed_term, view.revision),
                        records: Vec::new(),
                    }) != Err(ActorGroupStateError::RevisionGap)
                        || reducer.ready()
                    {
                        return Err(());
                    }
                    next.gate_rejections = next.gate_rejections.saturating_add(1);
                }
                ReducerEvent::InstallSnapshot(_) => {
                    reducer
                        .install(snapshot(
                            group,
                            view.leader_term,
                            view.revision + 1,
                            "successor",
                        ))
                        .map_err(|_| ())?;
                    next.groups[index].installed_term = view.leader_term;
                    next.groups[index].revision += 1;
                }
                ReducerEvent::CrossGroupDelta(_) => {
                    if reducer.apply(CoordinatorDelta {
                        version: placement_version(
                            other_group(group),
                            view.installed_term,
                            view.revision + 1,
                        ),
                        records: vec![record("cross-group", 1)],
                    }) != Err(ActorGroupStateError::GroupMismatch)
                        || !reducer.ready()
                    {
                        return Err(());
                    }
                    next.cross_group_rejections = next.cross_group_rejections.saturating_add(1);
                }
            }
            let installed = reducer.version().ok_or(())?;
            if installed.term.get() != next.groups[index].installed_term
                || installed.revision.get() != next.groups[index].revision
                || installed.group != group.id()
            {
                return Err(());
            }
            if other.version() != self.materialize(1 - index).version() {
                return Err(());
            }
            Ok(next)
        }

        fn invariant(&self) -> Result<(), String> {
            for view in self.groups {
                if view.installed_term > view.leader_term {
                    return Err("a group installed a snapshot beyond its elected term".to_owned());
                }
                if view.revision == 0 || view.revision > 4 {
                    return Err("group revision escaped its bounded range".to_owned());
                }
            }
            if self.cross_group_rejections > 4 {
                return Err("cross-group rejections escaped their bound".to_owned());
            }
            Ok(())
        }
    }

    #[test]
    fn multi_group_bounded_state_explorer_checks_every_production_reducer_transition() {
        let initial = DomainView {
            leader_term: 1,
            installed_term: 1,
            revision: 1,
        };
        let report = StateExplorer {
            maximum_states: 50_000,
            maximum_depth: 10,
        }
        .explore(MultiGroupExploration {
            groups: [initial, initial],
            cross_group_rejections: 0,
            gate_rejections: 0,
        })
        .unwrap();
        assert!(report.visited_states > 100, "{report:?}");
        assert!(report.explored_transitions > 1_000, "{report:?}");
        assert_eq!(report.maximum_depth_reached, 10);
    }
}
