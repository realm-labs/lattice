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

use crate::clock::{SimClock, SimScheduler};
use crate::trace::{TraceEvent, TraceJournal};

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
        self.schedule(1, MultiDomainEvent::ApplyDelta(SimDomain::Alpha));
        self.schedule(1, MultiDomainEvent::ApplyDelta(SimDomain::Beta));
        self.schedule(2, MultiDomainEvent::LoseLeader(SimDomain::Alpha));
        self.schedule(3, MultiDomainEvent::ApplyDelta(SimDomain::Beta));
        self.schedule(
            4,
            MultiDomainEvent::Campaign {
                domain: SimDomain::Alpha,
                host: "host-c".to_owned(),
            },
        );
        self.schedule(5, MultiDomainEvent::InstallSnapshot(SimDomain::Alpha));
        self.schedule(
            6,
            MultiDomainEvent::RejectCrossDomainDelta {
                target: SimDomain::Alpha,
                source: SimDomain::Beta,
            },
        );
        self.schedule(7, MultiDomainEvent::MembershipLost);
        self.schedule(8, MultiDomainEvent::ApplyDelta(SimDomain::Beta));
        self.schedule(9, MultiDomainEvent::MembershipRecovered);
        self.schedule(10, MultiDomainEvent::AdvanceHandoff(SimDomain::Alpha));
        self.schedule(10, MultiDomainEvent::AdvanceHandoff(SimDomain::Beta));
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
        let alpha = &first.state().domains[&SimDomain::Alpha];
        let beta = &first.state().domains[&SimDomain::Beta];
        assert_eq!(alpha.leader.as_deref(), Some("host-c"));
        assert_eq!(alpha.leader_term, 2);
        assert_eq!(beta.leader.as_deref(), Some("host-b"));
        assert_eq!(beta.leader_term, 1);
        assert_eq!(alpha.handoff_generation, 2);
        assert_eq!(beta.handoff_generation, 2);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct DomainModel {
        phase: u8,
        leader: u8,
        leader_term: u8,
        snapshot_term: u8,
        revision: u8,
        handoff_generation: u8,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct MultiDomainModel {
        membership_phase: u8,
        domains: [DomainModel; 2],
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum ModelEvent {
        Lose(usize),
        Elect(usize),
        InstallSnapshot(usize),
        Progress(usize),
        Handoff(usize),
        LoseMembership,
        RecoverMembership,
    }

    impl Explorable for MultiDomainModel {
        type Event = ModelEvent;
        type Error = ();

        fn enabled(&self) -> Vec<Self::Event> {
            let mut events = Vec::new();
            for (index, domain) in self.domains.iter().enumerate() {
                match domain.phase {
                    0 => events.push(ModelEvent::Lose(index)),
                    1 if self.membership_phase != 1 => events.push(ModelEvent::Elect(index)),
                    2 => events.push(ModelEvent::InstallSnapshot(index)),
                    _ => {}
                }
                if matches!(domain.phase, 0 | 3) && domain.revision < 4 {
                    events.push(ModelEvent::Progress(index));
                }
                if matches!(domain.phase, 0 | 3) && domain.handoff_generation < 2 {
                    events.push(ModelEvent::Handoff(index));
                }
            }
            match self.membership_phase {
                0 => events.push(ModelEvent::LoseMembership),
                1 => events.push(ModelEvent::RecoverMembership),
                _ => {}
            }
            events
        }

        fn step(&self, event: &Self::Event) -> Result<Self, Self::Error> {
            let mut next = self.clone();
            match *event {
                ModelEvent::Lose(index) => {
                    next.domains[index].phase = 1;
                    next.domains[index].leader = 0;
                }
                ModelEvent::Elect(index) => {
                    let domain = &mut next.domains[index];
                    domain.phase = 2;
                    domain.leader = 3 + index as u8;
                    domain.leader_term += 1;
                }
                ModelEvent::InstallSnapshot(index) => {
                    let domain = &mut next.domains[index];
                    domain.phase = 3;
                    domain.snapshot_term = domain.leader_term;
                    domain.revision += 1;
                }
                ModelEvent::Progress(index) => next.domains[index].revision += 1,
                ModelEvent::Handoff(index) => next.domains[index].handoff_generation += 1,
                ModelEvent::LoseMembership => next.membership_phase = 1,
                ModelEvent::RecoverMembership => next.membership_phase = 2,
            }
            if let Some(index) = match *event {
                ModelEvent::Lose(index)
                | ModelEvent::Elect(index)
                | ModelEvent::InstallSnapshot(index)
                | ModelEvent::Progress(index)
                | ModelEvent::Handoff(index) => Some(index),
                ModelEvent::LoseMembership | ModelEvent::RecoverMembership => None,
            } {
                let other = 1 - index;
                if next.domains[other] != self.domains[other] {
                    return Err(());
                }
            }
            Ok(next)
        }

        fn invariant(&self) -> Result<(), String> {
            for domain in self.domains {
                if matches!(domain.phase, 0 | 3)
                    && (domain.leader == 0 || domain.leader_term != domain.snapshot_term)
                {
                    return Err("live domain lacks its exact-term leader snapshot".to_owned());
                }
                if domain.phase == 2 && domain.leader_term == domain.snapshot_term {
                    return Err("successor became ready without a new-term snapshot".to_owned());
                }
                if domain.handoff_generation == 0 || domain.handoff_generation > 2 {
                    return Err("handoff generation escaped its domain bound".to_owned());
                }
            }
            Ok(())
        }
    }

    #[test]
    fn multi_domain_bounded_state_explorer_checks_every_transition() {
        let initial_domain = DomainModel {
            phase: 0,
            leader: 1,
            leader_term: 1,
            snapshot_term: 1,
            revision: 1,
            handoff_generation: 1,
        };
        let report = StateExplorer {
            maximum_states: 50_000,
            maximum_depth: 10,
        }
        .explore(MultiDomainModel {
            membership_phase: 0,
            domains: [initial_domain, initial_domain],
        })
        .unwrap();
        assert!(report.visited_states > 1_000);
        assert_eq!(report.maximum_depth_reached, 10);
    }
}
