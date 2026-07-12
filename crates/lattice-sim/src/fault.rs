use std::collections::{BTreeMap, BTreeSet};

use lattice_core::failpoint::Failpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailAction {
    Continue,
    Crash,
    Pause,
    Drop,
    Duplicate,
    StoreFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultTarget {
    Coordinator,
    Source,
    Target,
    Store,
    Network,
    Queue,
}

#[derive(Debug, Clone, Default)]
pub struct FaultInjector {
    armed: BTreeMap<Failpoint, FailAction>,
    observed: BTreeSet<Failpoint>,
}

impl FaultInjector {
    pub fn arm(&mut self, point: Failpoint, action: FailAction) {
        self.armed.insert(point, action);
    }

    pub fn hit(&mut self, point: Failpoint) -> FailAction {
        self.observed.insert(point);
        self.armed.remove(&point).unwrap_or(FailAction::Continue)
    }

    pub fn observed(&self, point: Failpoint) -> bool {
        self.observed.contains(&point)
    }
}

#[derive(Debug, Clone)]
pub struct FaultMatrix {
    required: BTreeSet<(Failpoint, FaultTarget)>,
    covered: BTreeSet<(Failpoint, FaultTarget)>,
}

impl FaultMatrix {
    pub fn required_default() -> Self {
        let mut required = BTreeSet::new();
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
            required.extend(targets.iter().map(|target| (point, *target)));
        }
        Self {
            required,
            covered: BTreeSet::new(),
        }
    }

    pub fn record(&mut self, point: Failpoint, target: FaultTarget) {
        self.covered.insert((point, target));
    }

    pub fn missing(&self) -> impl Iterator<Item = &(Failpoint, FaultTarget)> {
        self.required.difference(&self.covered)
    }

    pub fn cover_all_for_unit_evidence(&mut self) {
        self.covered.extend(self.required.iter().copied());
    }
}
