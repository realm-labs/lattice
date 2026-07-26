use std::collections::BTreeMap;

use bytes::Bytes;
use lattice_core::actor_ref::PlacementDomainId;
use lattice_placement::coordinator::{
    CoordinatorDelta, PlacementDomainState, PlacementDomainStateError, SnapshotInstall,
    SnapshotRecord, SnapshotVersion,
};
use lattice_placement::types::{CoordinatorTerm, PlacementVersion, Revision};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::clock::{SimClock, SimRandom, SimScheduler};
use crate::trace::{TraceEvent, TraceJournal};

const WORKLOAD_STREAM: u64 = 0x1405_7B7E_F767_814F;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SimDomain {
    Alpha,
    Beta,
}

impl SimDomain {
    const ALL: [Self; 2] = [Self::Alpha, Self::Beta];

    fn id(self) -> PlacementDomainId {
        PlacementDomainId::new(match self {
            Self::Alpha => "simulation-alpha",
            Self::Beta => "simulation-beta",
        })
        .expect("static simulation domain must be valid")
    }

    fn label(self) -> &'static str {
        match self {
            Self::Alpha => "alpha",
            Self::Beta => "beta",
        }
    }
}

fn other_domain(domain: SimDomain) -> SimDomain {
    match domain {
        SimDomain::Alpha => SimDomain::Beta,
        SimDomain::Beta => SimDomain::Alpha,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiDomainScenarioConfig {
    pub seed: u64,
    pub maximum_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiDomainEvent {
    ApplyDelta(SimDomain),
    LoseLeader(SimDomain),
    Campaign {
        domain: SimDomain,
        host: String,
    },
    InstallSnapshot(SimDomain),
    RejectCrossDomainDelta {
        target: SimDomain,
        source: SimDomain,
    },
    AdvanceHandoff(SimDomain),
    MembershipLost,
    MembershipRecovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainScenarioView {
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
pub struct MultiDomainScenarioState {
    pub membership_up: bool,
    pub membership_term: u64,
    pub domains: BTreeMap<SimDomain, DomainScenarioView>,
    pub cross_domain_rejections: usize,
}

struct DomainPlane {
    reducer: PlacementDomainState,
}

pub struct MultiDomainScenario {
    pub config: MultiDomainScenarioConfig,
    pub clock: SimClock,
    pub trace: TraceJournal,
    scheduler: SimScheduler<MultiDomainEvent>,
    planes: BTreeMap<SimDomain, DomainPlane>,
    state: MultiDomainScenarioState,
}

impl MultiDomainScenario {
    pub fn standard(config: MultiDomainScenarioConfig) -> Result<Self, MultiDomainScenarioError> {
        if config.maximum_events == 0 {
            return Err(MultiDomainScenarioError::InvalidConfig);
        }
        let mut planes = BTreeMap::new();
        let mut domains = BTreeMap::new();
        for (domain, host) in [(SimDomain::Alpha, "host-a"), (SimDomain::Beta, "host-b")] {
            let mut reducer = PlacementDomainState::new(domain.id());
            reducer.install(snapshot(domain, 1, 1, host))?;
            domains.insert(
                domain,
                DomainScenarioView {
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
            planes.insert(domain, DomainPlane { reducer });
        }
        let configuration =
            serde_json::to_value(&config).map_err(|_| MultiDomainScenarioError::Serialization)?;
        let trace = TraceJournal::new(
            "multi-domain-isolation",
            config.seed,
            configuration,
            config.maximum_events,
        )
        .ok_or(MultiDomainScenarioError::InvalidConfig)?;
        Ok(Self {
            scheduler: SimScheduler::new(config.seed),
            config,
            clock: SimClock::new(),
            trace,
            planes,
            state: MultiDomainScenarioState {
                membership_up: true,
                membership_term: 1,
                domains,
                cross_domain_rejections: 0,
            },
        })
    }

    pub fn schedule(&mut self, at_millis: u64, event: MultiDomainEvent) {
        self.scheduler.schedule(at_millis, event);
    }

    pub fn schedule_acceptance(&mut self) {
        let mut random = SimRandom::new(self.config.seed ^ WORKLOAD_STREAM);
        let mut last = 0;
        for domain in SimDomain::ALL {
            let mut at = 1 + u64::try_from(random.below(3)).unwrap_or(0);
            for _ in 0..=random.below(2) {
                self.schedule(at, MultiDomainEvent::ApplyDelta(domain));
                at += 1;
            }
            if random.chance(2, 3) {
                self.schedule(at, MultiDomainEvent::LoseLeader(domain));
                at += 1;
                self.schedule(
                    at,
                    MultiDomainEvent::Campaign {
                        domain,
                        host: format!("host-{}-successor", domain.label()),
                    },
                );
                at += 1;
                self.schedule(at, MultiDomainEvent::InstallSnapshot(domain));
                at += 1;
                for _ in 0..random.below(2) {
                    self.schedule(at, MultiDomainEvent::ApplyDelta(domain));
                    at += 1;
                }
            }
            last = last.max(at);
        }
        let target = SimDomain::ALL[random.below(SimDomain::ALL.len())];
        let source = SimDomain::ALL[(random.below(SimDomain::ALL.len()) + 1) % 2];
        self.schedule(
            last,
            MultiDomainEvent::RejectCrossDomainDelta {
                target,
                source: if source == target {
                    other_domain(target)
                } else {
                    source
                },
            },
        );
        self.schedule(last + 1, MultiDomainEvent::MembershipLost);
        let mut handoffs = SimDomain::ALL;
        random.shuffle(&mut handoffs);
        for (index, domain) in handoffs.into_iter().enumerate() {
            let at = last + 2 + u64::try_from(index * random.below(2)).unwrap_or(0);
            self.schedule(at, MultiDomainEvent::AdvanceHandoff(domain));
        }
        self.schedule(last + 4, MultiDomainEvent::MembershipRecovered);
    }

    pub fn run(&mut self) -> Result<&MultiDomainScenarioState, MultiDomainScenarioError> {
        while let Some((at, event)) = self.scheduler.pop_next() {
            self.clock.advance_to(at);
            self.step(event)?;
            self.check_invariants()?;
        }
        Ok(&self.state)
    }

    pub fn state(&self) -> &MultiDomainScenarioState {
        &self.state
    }

    pub fn step(&mut self, event: MultiDomainEvent) -> Result<(), MultiDomainScenarioError> {
        let before = self.state.clone();
        let previous =
            serde_json::to_string(&before).map_err(|_| MultiDomainScenarioError::Serialization)?;
        match &event {
            MultiDomainEvent::ApplyDelta(domain) => self.apply_delta(*domain, "progress")?,
            MultiDomainEvent::LoseLeader(domain) => {
                let view = self.view_mut(*domain);
                view.leader = None;
                view.control_available = false;
            }
            MultiDomainEvent::Campaign { domain, host } => {
                if !self.state.membership_up {
                    return Err(MultiDomainScenarioError::MembershipUnavailable);
                }
                let view = self.view(*domain);
                if view.leader.is_some() {
                    return Err(MultiDomainScenarioError::LeaderAlreadyPresent);
                }
                let next_term = view.leader_term.saturating_add(1);
                let next_revision = view.revision.saturating_add(1);
                let result = self.plane_mut(*domain).reducer.apply(CoordinatorDelta {
                    version: placement_version(*domain, next_term, next_revision),
                    records: Vec::new(),
                });
                if result != Err(PlacementDomainStateError::SnapshotRequired) {
                    return Err(MultiDomainScenarioError::MutationBeforeSnapshot);
                }
                let view = self.view_mut(*domain);
                view.leader = Some(host.clone());
                view.leader_term = next_term;
                view.session_ready = false;
                view.control_available = false;
            }
            MultiDomainEvent::InstallSnapshot(domain) => {
                let view = self.view(*domain).clone();
                let host = view
                    .leader
                    .as_deref()
                    .ok_or(MultiDomainScenarioError::LeaderMissing)?;
                let revision = view.revision.saturating_add(1);
                self.plane_mut(*domain).reducer.install(snapshot(
                    *domain,
                    view.leader_term,
                    revision,
                    host,
                ))?;
                self.refresh(*domain);
                self.view_mut(*domain).control_available = true;
            }
            MultiDomainEvent::RejectCrossDomainDelta { target, source } => {
                let revision = self.view(*target).revision.saturating_add(1);
                let source_term = self.view(*source).snapshot_term;
                let result = self.plane_mut(*target).reducer.apply(CoordinatorDelta {
                    version: placement_version(*source, source_term, revision),
                    records: vec![record("cross-domain", 1_u64)],
                });
                if result != Err(PlacementDomainStateError::DomainMismatch) {
                    return Err(MultiDomainScenarioError::CrossDomainMutationAccepted);
                }
                self.state.cross_domain_rejections =
                    self.state.cross_domain_rejections.saturating_add(1);
            }
            MultiDomainEvent::AdvanceHandoff(domain) => {
                let generation = self.view(*domain).handoff_generation.saturating_add(1);
                self.apply_delta(*domain, "handoff")?;
                self.view_mut(*domain).handoff_generation = generation;
            }
            MultiDomainEvent::MembershipLost => self.state.membership_up = false,
            MultiDomainEvent::MembershipRecovered => {
                self.state.membership_up = true;
                self.state.membership_term = self.state.membership_term.saturating_add(1);
            }
        }
        self.assert_untouched_domain(&event, &before)?;
        let next = serde_json::to_string(&self.state)
            .map_err(|_| MultiDomainScenarioError::Serialization)?;
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
            return Err(MultiDomainScenarioError::TraceCapacity);
        }
        Ok(())
    }

    pub fn check_invariants(&self) -> Result<(), MultiDomainScenarioError> {
        for domain in SimDomain::ALL {
            let view = self.view(domain);
            let reducer = &self
                .planes
                .get(&domain)
                .expect("all simulation domains have reducers")
                .reducer;
            let version = reducer
                .version()
                .ok_or(MultiDomainScenarioError::SnapshotMissing)?;
            if version.domain != domain.id()
                || version.term.get() != view.snapshot_term
                || version.revision.get() != view.revision
                || reducer.ready() != view.session_ready
            {
                return Err(MultiDomainScenarioError::ReducerViewMismatch);
            }
            if view.control_available
                && (view.leader.is_none()
                    || !view.session_ready
                    || view.leader_term != view.snapshot_term)
            {
                return Err(MultiDomainScenarioError::AuthorityWithoutSnapshot);
            }
            if view.handoff_generation == 0 || view.handoff_generation > 2 {
                return Err(MultiDomainScenarioError::InvalidHandoffGeneration);
            }
        }
        Ok(())
    }

    fn apply_delta(
        &mut self,
        domain: SimDomain,
        record_key: &str,
    ) -> Result<(), MultiDomainScenarioError> {
        let view = self.view(domain).clone();
        if !view.control_available {
            return Err(MultiDomainScenarioError::DomainUnavailable);
        }
        let revision = view.revision.saturating_add(1);
        self.plane_mut(domain).reducer.apply(CoordinatorDelta {
            version: placement_version(domain, view.snapshot_term, revision),
            records: vec![record(record_key, revision)],
        })?;
        self.refresh(domain);
        Ok(())
    }

    fn refresh(&mut self, domain: SimDomain) {
        let plane = self
            .planes
            .get(&domain)
            .expect("all simulation domains have reducers");
        let version = plane
            .reducer
            .version()
            .expect("installed simulation domain has a version");
        let snapshot_term = version.term.get();
        let revision = version.revision.get();
        let session_ready = plane.reducer.ready();
        let records = plane.reducer.records().count();
        let view = self.view_mut(domain);
        view.snapshot_term = snapshot_term;
        view.revision = revision;
        view.session_ready = session_ready;
        view.records = records;
    }

    fn assert_untouched_domain(
        &self,
        event: &MultiDomainEvent,
        before: &MultiDomainScenarioState,
    ) -> Result<(), MultiDomainScenarioError> {
        let touched = match event {
            MultiDomainEvent::ApplyDelta(domain)
            | MultiDomainEvent::LoseLeader(domain)
            | MultiDomainEvent::InstallSnapshot(domain)
            | MultiDomainEvent::AdvanceHandoff(domain)
            | MultiDomainEvent::Campaign { domain, .. } => Some(*domain),
            MultiDomainEvent::RejectCrossDomainDelta { .. }
            | MultiDomainEvent::MembershipLost
            | MultiDomainEvent::MembershipRecovered => None,
        };
        for domain in SimDomain::ALL {
            if touched != Some(domain)
                && self.state.domains.get(&domain) != before.domains.get(&domain)
            {
                return Err(MultiDomainScenarioError::CrossDomainMutationAccepted);
            }
        }
        Ok(())
    }

    fn plane_mut(&mut self, domain: SimDomain) -> &mut DomainPlane {
        self.planes
            .get_mut(&domain)
            .expect("all simulation domains have reducers")
    }

    fn view(&self, domain: SimDomain) -> &DomainScenarioView {
        self.state
            .domains
            .get(&domain)
            .expect("all simulation domains have views")
    }

    fn view_mut(&mut self, domain: SimDomain) -> &mut DomainScenarioView {
        self.state
            .domains
            .get_mut(&domain)
            .expect("all simulation domains have views")
    }
}

fn placement_version(domain: SimDomain, term: u64, revision: u64) -> PlacementVersion {
    PlacementVersion::new(
        domain.id(),
        CoordinatorTerm::new(term).expect("simulation term is positive"),
        Revision::new(revision).expect("simulation revision is positive"),
    )
}

fn snapshot(domain: SimDomain, term: u64, revision: u64, host: &str) -> SnapshotInstall {
    SnapshotInstall {
        version: SnapshotVersion::Placement(placement_version(domain, term, revision)),
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
pub enum MultiDomainScenarioError {
    #[error("multi-domain scenario configuration is invalid")]
    InvalidConfig,
    #[error("multi-domain trace capacity is exhausted")]
    TraceCapacity,
    #[error("multi-domain scenario serialization failed")]
    Serialization,
    #[error("membership is unavailable for a new campaign")]
    MembershipUnavailable,
    #[error("domain already has a leader")]
    LeaderAlreadyPresent,
    #[error("domain has no elected leader")]
    LeaderMissing,
    #[error("domain control is unavailable")]
    DomainUnavailable,
    #[error("new-term placement mutation was accepted before a snapshot")]
    MutationBeforeSnapshot,
    #[error("a cross-domain placement delta was accepted")]
    CrossDomainMutationAccepted,
    #[error("domain reducer has no installed snapshot")]
    SnapshotMissing,
    #[error("domain reducer state diverged from the simulation view")]
    ReducerViewMismatch,
    #[error("domain authority became available without its exact-term snapshot")]
    AuthorityWithoutSnapshot,
    #[error("domain handoff generation is outside its bounded scenario range")]
    InvalidHandoffGeneration,
    #[error(transparent)]
    PlacementState(#[from] PlacementDomainStateError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::explorer::{Explorable, StateExplorer};

    fn run(seed: u64) -> MultiDomainScenario {
        let mut scenario = MultiDomainScenario::standard(MultiDomainScenarioConfig {
            seed,
            maximum_events: 64,
        })
        .unwrap();
        scenario.schedule_acceptance();
        scenario.run().unwrap();
        scenario
    }

    #[test]
    fn multi_domain_trace_replays_independent_elections_and_handoffs() {
        let first = run(71);
        let second = run(71);
        assert_eq!(first.state(), second.state());
        assert_eq!(first.trace, second.trace);
        assert_eq!(first.state().cross_domain_rejections, 1);
        for domain in SimDomain::ALL {
            let view = &first.state().domains[&domain];
            assert!(view.leader.is_some());
            assert_eq!(view.leader_term, view.snapshot_term);
            assert_eq!(view.handoff_generation, 2);
        }
    }

    #[test]
    fn seeded_multi_domain_workloads_explore_independent_election_orders() {
        let mut elected = 0;
        let signatures = (1..=32)
            .map(|seed| {
                let scenario = run(seed);
                elected += scenario
                    .state()
                    .domains
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
    struct MultiDomainExploration {
        domains: [DomainView; 2],
        cross_domain_rejections: u8,
        gate_rejections: u8,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum ReducerEvent {
        Progress(usize),
        Elect(usize),
        InstallSnapshot(usize),
        CrossDomainDelta(usize),
        StaleDelta(usize),
    }

    impl MultiDomainExploration {
        fn materialize(&self, index: usize) -> PlacementDomainState {
            let domain = SimDomain::ALL[index];
            let view = self.domains[index];
            let mut reducer = PlacementDomainState::new(domain.id());
            reducer
                .install(snapshot(domain, view.installed_term, view.revision, "host"))
                .expect("materialized domain snapshot is installable");
            reducer
        }
    }

    impl Explorable for MultiDomainExploration {
        type Event = ReducerEvent;
        type Error = ();

        fn enabled(&self) -> Vec<Self::Event> {
            let mut events = Vec::new();
            for (index, view) in self.domains.iter().enumerate() {
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
                if self.cross_domain_rejections < 2 {
                    events.push(ReducerEvent::CrossDomainDelta(index));
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
                | ReducerEvent::CrossDomainDelta(index)
                | ReducerEvent::StaleDelta(index) => index,
            };
            let domain = SimDomain::ALL[index];
            let view = self.domains[index];
            let other = self.materialize(1 - index);
            let mut reducer = self.materialize(index);
            match *event {
                ReducerEvent::Elect(_) => next.domains[index].leader_term += 1,
                ReducerEvent::Progress(_) => {
                    let result = reducer.apply(CoordinatorDelta {
                        version: placement_version(domain, view.leader_term, view.revision + 1),
                        records: vec![record("progress", view.revision + 1)],
                    });
                    if view.leader_term == view.installed_term {
                        result.map_err(|_| ())?;
                        if !reducer.ready() {
                            return Err(());
                        }
                        next.domains[index].revision += 1;
                    } else {
                        if result != Err(PlacementDomainStateError::SnapshotRequired)
                            || reducer.ready()
                        {
                            return Err(());
                        }
                        next.gate_rejections = next.gate_rejections.saturating_add(1);
                    }
                }
                ReducerEvent::StaleDelta(_) => {
                    if reducer.apply(CoordinatorDelta {
                        version: placement_version(domain, view.installed_term, view.revision),
                        records: Vec::new(),
                    }) != Err(PlacementDomainStateError::RevisionGap)
                        || reducer.ready()
                    {
                        return Err(());
                    }
                    next.gate_rejections = next.gate_rejections.saturating_add(1);
                }
                ReducerEvent::InstallSnapshot(_) => {
                    reducer
                        .install(snapshot(
                            domain,
                            view.leader_term,
                            view.revision + 1,
                            "successor",
                        ))
                        .map_err(|_| ())?;
                    next.domains[index].installed_term = view.leader_term;
                    next.domains[index].revision += 1;
                }
                ReducerEvent::CrossDomainDelta(_) => {
                    if reducer.apply(CoordinatorDelta {
                        version: placement_version(
                            other_domain(domain),
                            view.installed_term,
                            view.revision + 1,
                        ),
                        records: vec![record("cross-domain", 1)],
                    }) != Err(PlacementDomainStateError::DomainMismatch)
                        || !reducer.ready()
                    {
                        return Err(());
                    }
                    next.cross_domain_rejections = next.cross_domain_rejections.saturating_add(1);
                }
            }
            let installed = reducer.version().ok_or(())?;
            if installed.term.get() != next.domains[index].installed_term
                || installed.revision.get() != next.domains[index].revision
                || installed.domain != domain.id()
            {
                return Err(());
            }
            if other.version() != self.materialize(1 - index).version() {
                return Err(());
            }
            Ok(next)
        }

        fn invariant(&self) -> Result<(), String> {
            for view in self.domains {
                if view.installed_term > view.leader_term {
                    return Err("a domain installed a snapshot beyond its elected term".to_owned());
                }
                if view.revision == 0 || view.revision > 4 {
                    return Err("domain revision escaped its bounded range".to_owned());
                }
            }
            if self.cross_domain_rejections > 4 {
                return Err("cross-domain rejections escaped their bound".to_owned());
            }
            Ok(())
        }
    }

    #[test]
    fn multi_domain_bounded_state_explorer_checks_every_production_reducer_transition() {
        let initial = DomainView {
            leader_term: 1,
            installed_term: 1,
            revision: 1,
        };
        let report = StateExplorer {
            maximum_states: 50_000,
            maximum_depth: 10,
        }
        .explore(MultiDomainExploration {
            domains: [initial, initial],
            cross_domain_rejections: 0,
            gate_rejections: 0,
        })
        .unwrap();
        assert!(report.visited_states > 100, "{report:?}");
        assert!(report.explored_transitions > 1_000, "{report:?}");
        assert_eq!(report.maximum_depth_reached, 10);
    }
}
