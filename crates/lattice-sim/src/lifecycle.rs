use std::collections::{BTreeMap, BTreeSet};

use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};
use lattice_placement::coordinator::{
    MemberChange, MemberEvent, MemberRecord, MemberRemovalReason, MemberStatus, NodeHello,
};
use lattice_placement::types::{CoordinatorTerm, NodeKey, Revision, StateVersion};
use lattice_service::cluster::members::{MemberDirectory, MemberDirectoryError};
use lattice_service::lifecycle::{
    ServiceLifecycle, ServiceLifecycleEffect, ServiceLifecycleError, ServiceLifecycleEvent,
    ServiceLifecycleState,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::clock::{SimClock, SimScheduler};
use crate::trace::{TraceEvent, TraceJournal};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleScenarioConfig {
    pub seed: u64,
    pub maximum_events: usize,
}

#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    RemotingReady,
    DiscoveryCandidate(NodeKey),
    Member(MemberEvent),
    CoordinatorPartition,
    DiscoveryOutage,
    Reconciled,
    BeginDrain,
    DrainComplete,
    ShutdownComplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleScenarioState {
    pub lifecycle: String,
    pub admission_open: bool,
    pub accepted_member_events: usize,
    pub rejected_stale_events: usize,
    pub discovered_candidates: usize,
}

pub struct LifecycleScenario {
    pub trace: TraceJournal,
    pub state: LifecycleScenarioState,
    clock: SimClock,
    scheduler: SimScheduler<LifecycleEvent>,
    lifecycle: ServiceLifecycle,
    members: MemberDirectory,
}

impl LifecycleScenario {
    pub fn standard(config: LifecycleScenarioConfig) -> Result<Self, LifecycleScenarioError> {
        if config.maximum_events == 0 {
            return Err(LifecycleScenarioError::InvalidConfig);
        }
        let trace = TraceJournal::new(
            "cluster-member-lifecycle",
            config.seed,
            serde_json::to_value(&config).map_err(|_| LifecycleScenarioError::Codec)?,
            config.maximum_events,
        )
        .ok_or(LifecycleScenarioError::InvalidConfig)?;
        Ok(Self {
            trace,
            state: LifecycleScenarioState {
                lifecycle: "Booting".to_owned(),
                admission_open: false,
                accepted_member_events: 0,
                rejected_stale_events: 0,
                discovered_candidates: 0,
            },
            clock: SimClock::new(),
            scheduler: SimScheduler::new(config.seed),
            lifecycle: ServiceLifecycle::default(),
            members: MemberDirectory::new(32)?,
        })
    }

    pub fn schedule_acceptance(&mut self) {
        let local = member("member", 11, 29301, 1);
        let peer_old = member("peer", 21, 29302, 2);
        let peer_new = member("peer", 22, 29302, 4);
        self.schedule(0, LifecycleEvent::RemotingReady);
        self.schedule(1, LifecycleEvent::DiscoveryCandidate(peer_old.node.clone()));
        self.schedule(2, LifecycleEvent::Member(upsert(local.clone())));
        self.schedule(3, LifecycleEvent::Member(upsert(peer_old.clone())));
        self.schedule(4, LifecycleEvent::CoordinatorPartition);
        self.schedule(5, LifecycleEvent::DiscoveryOutage);
        self.schedule(6, LifecycleEvent::Member(removed(&peer_old, 3)));
        self.schedule(7, LifecycleEvent::Member(upsert(peer_new)));
        self.schedule(8, LifecycleEvent::Member(upsert(peer_old)));
        self.schedule(9, LifecycleEvent::Reconciled);
        self.schedule(10, LifecycleEvent::BeginDrain);
        self.schedule(11, LifecycleEvent::Member(removed(&local, 5)));
        self.schedule(12, LifecycleEvent::DrainComplete);
        self.schedule(13, LifecycleEvent::ShutdownComplete);
    }

    pub fn run(&mut self) -> Result<&LifecycleScenarioState, LifecycleScenarioError> {
        while let Some((at, event)) = self.scheduler.pop_next() {
            self.clock.advance_to(at);
            self.step(event)?;
            self.check_invariants()?;
        }
        Ok(&self.state)
    }

    fn schedule(&mut self, at: u64, event: LifecycleEvent) {
        self.scheduler.schedule(at, event);
    }

    fn step(&mut self, event: LifecycleEvent) -> Result<(), LifecycleScenarioError> {
        let previous = self.state.lifecycle.clone();
        let kind = format!("{event:?}");
        match event {
            LifecycleEvent::RemotingReady => {
                self.apply_lifecycle(ServiceLifecycleEvent::RemotingReady)?;
            }
            LifecycleEvent::DiscoveryCandidate(_) => {
                self.state.discovered_candidates += 1;
            }
            LifecycleEvent::Member(event) => match self.members.apply(event) {
                Ok(()) => {
                    self.state.accepted_member_events += 1;
                    if self.lifecycle.state() == ServiceLifecycleState::Joining {
                        self.apply_lifecycle(ServiceLifecycleEvent::SnapshotInstalled)?;
                    }
                }
                Err(MemberDirectoryError::StaleRevision) => {
                    self.state.rejected_stale_events += 1;
                }
                Err(error) => return Err(error.into()),
            },
            LifecycleEvent::CoordinatorPartition => {
                self.apply_lifecycle(ServiceLifecycleEvent::CoordinatorLost)?;
            }
            LifecycleEvent::DiscoveryOutage => {}
            LifecycleEvent::Reconciled => {
                self.apply_lifecycle(ServiceLifecycleEvent::Reconciled)?;
            }
            LifecycleEvent::BeginDrain => {
                self.apply_lifecycle(ServiceLifecycleEvent::BeginDrain)?;
            }
            LifecycleEvent::DrainComplete => {
                self.apply_lifecycle(ServiceLifecycleEvent::DrainComplete)?;
            }
            LifecycleEvent::ShutdownComplete => {
                self.apply_lifecycle(ServiceLifecycleEvent::ShutdownComplete)?;
            }
        }
        self.state.lifecycle = format!("{:?}", self.lifecycle.state());
        if !self.trace.push(TraceEvent {
            index: 0,
            causal_parents: self
                .trace
                .events
                .last()
                .map(|event| vec![event.index])
                .unwrap_or_default(),
            time_millis: self.clock.now_millis(),
            node: "member".to_owned(),
            kind,
            previous,
            next: self.state.lifecycle.clone(),
            operation_id: None,
        }) {
            return Err(LifecycleScenarioError::TraceCapacity);
        }
        Ok(())
    }

    fn apply_lifecycle(
        &mut self,
        event: ServiceLifecycleEvent,
    ) -> Result<(), LifecycleScenarioError> {
        for effect in self.lifecycle.transition(event)? {
            match effect {
                ServiceLifecycleEffect::OpenExternalAdmission => {
                    self.state.admission_open = true;
                }
                ServiceLifecycleEffect::CloseExternalAdmission
                | ServiceLifecycleEffect::FenceClaimsAndStopRuntime => {
                    self.state.admission_open = false;
                }
                ServiceLifecycleEffect::ReleaseRuntimeIdentity => {
                    self.state.admission_open = false;
                    if let Some(version) = self.members.snapshot().version {
                        self.members.install_snapshot(version, Vec::new())?;
                    }
                }
                ServiceLifecycleEffect::BeginPlacementDrain => {}
            }
        }
        Ok(())
    }

    fn check_invariants(&self) -> Result<(), LifecycleInvariantViolation> {
        let snapshot = self.members.snapshot();
        let mut node_ids = BTreeMap::new();
        for record in &snapshot.members {
            if record.status == MemberStatus::Up
                && node_ids
                    .insert(record.node.node_id.clone(), record.node.incarnation)
                    .is_some()
            {
                return Err(LifecycleInvariantViolation::DuplicateUpNodeId);
            }
        }
        if self.state.admission_open
            && !matches!(
                self.lifecycle.state(),
                ServiceLifecycleState::Ready | ServiceLifecycleState::Draining
            )
        {
            return Err(LifecycleInvariantViolation::AdmissionWithoutReady);
        }
        if self.state.discovered_candidates > 0
            && self.state.accepted_member_events == 0
            && !snapshot.members.is_empty()
        {
            return Err(LifecycleInvariantViolation::DiscoveryMutatedMembership);
        }
        if self.lifecycle.state() == ServiceLifecycleState::Terminated
            && (!snapshot.members.is_empty() || self.state.admission_open)
        {
            return Err(LifecycleInvariantViolation::TerminatedRetainedAuthority);
        }
        Ok(())
    }
}

fn member(node_id: &str, incarnation: u128, port: u16, revision: u64) -> MemberRecord {
    let node = NodeKey {
        node_id: node_id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    };
    MemberRecord {
        node: node.clone(),
        hello: NodeHello {
            node,
            roles: BTreeSet::new(),
            capacity_units: 1,
            hosted_entity_types: BTreeSet::new(),
            proxied_entity_types: BTreeSet::new(),
            singleton_eligibility: BTreeSet::new(),
            used_singletons: BTreeSet::new(),
            protocols: Vec::new(),
            entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
        },
        status: MemberStatus::Up,
        version: StateVersion::new(
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(revision).unwrap(),
        ),
        lease_id: i64::try_from(incarnation).unwrap(),
    }
}

fn upsert(record: MemberRecord) -> MemberEvent {
    MemberEvent {
        version: record.version,
        change: MemberChange::Upsert(Box::new(record)),
    }
}

fn removed(record: &MemberRecord, revision: u64) -> MemberEvent {
    MemberEvent {
        version: StateVersion::new(
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(revision).unwrap(),
        ),
        change: MemberChange::Removed {
            node: record.node.clone(),
            reason: MemberRemovalReason::FailureDetected,
        },
    }
}

#[derive(Debug, Error)]
pub enum LifecycleInvariantViolation {
    #[error("more than one Up incarnation exists for a node ID")]
    DuplicateUpNodeId,
    #[error("external admission is open outside Ready or Draining")]
    AdmissionWithoutReady,
    #[error("a discovery candidate mutated authoritative membership")]
    DiscoveryMutatedMembership,
    #[error("a terminated service retained membership authority")]
    TerminatedRetainedAuthority,
}

#[derive(Debug, Error)]
pub enum LifecycleScenarioError {
    #[error("lifecycle scenario configuration is invalid")]
    InvalidConfig,
    #[error("lifecycle scenario serialization failed")]
    Codec,
    #[error("lifecycle trace capacity is exhausted")]
    TraceCapacity,
    #[error(transparent)]
    Lifecycle(#[from] ServiceLifecycleError),
    #[error(transparent)]
    MemberDirectory(#[from] MemberDirectoryError),
    #[error(transparent)]
    Invariant(#[from] LifecycleInvariantViolation),
}

#[cfg(test)]
mod tests {
    use super::{LifecycleScenario, LifecycleScenarioConfig};

    fn run(seed: u64) -> LifecycleScenario {
        let mut scenario = LifecycleScenario::standard(LifecycleScenarioConfig {
            seed,
            maximum_events: 64,
        })
        .unwrap();
        scenario.schedule_acceptance();
        scenario.run().unwrap();
        scenario
    }

    #[test]
    fn reorder_duplicate_partition_lease_expiry_and_leave_preserve_lifecycle() {
        let scenario = run(20260713);
        assert_eq!(scenario.state.lifecycle, "Terminated");
        assert!(!scenario.state.admission_open);
        // Strict StateVersion sequencing rejects every reordered gap, not
        // only the formerly record-local stale revision.
        assert_eq!(scenario.state.rejected_stale_events, 6);
    }

    #[test]
    fn lifecycle_trace_is_seed_replayable() {
        assert_eq!(run(77).trace, run(77).trace);
    }
}
