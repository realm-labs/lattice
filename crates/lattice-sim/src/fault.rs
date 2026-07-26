use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use lattice_core::failpoint::{self, Failpoint, FailpointGuard};

pub use lattice_core::failpoint::FailpointAction as FailAction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultTarget {
    Coordinator,
    Source,
    Target,
    Store,
    Network,
    Queue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultOrigin {
    ProductionCallSite,
    SimulatedExecutor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultOutcome {
    CommitRejected,
    MessageLost,
    MessageDuplicated,
    TransitionRetried,
    ProcessPaused,
    ProcessCrashed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FaultEvidence {
    pub point: Failpoint,
    pub target: FaultTarget,
    pub action: FailAction,
    pub outcome: FaultOutcome,
    pub origin: FaultOrigin,
}

#[derive(Debug, Clone, Default)]
pub struct FaultInjector {
    armed: BTreeMap<Failpoint, FailAction>,
    observed: BTreeSet<Failpoint>,
    pending: Vec<(Failpoint, FailAction)>,
    injected: Vec<(Failpoint, FailAction)>,
    evidence: BTreeSet<FaultEvidence>,
}

impl FaultInjector {
    pub fn arm(&mut self, point: Failpoint, action: FailAction) {
        if action.is_continue() {
            self.armed.remove(&point);
        } else {
            self.armed.insert(point, action);
        }
    }

    pub fn hit(&mut self, point: Failpoint) -> FailAction {
        self.observed.insert(point);
        let action = self.armed.remove(&point).unwrap_or(FailAction::Continue);
        if !action.is_continue() {
            self.pending.push((point, action));
            self.injected.push((point, action));
        }
        action
    }

    pub fn observed(&self, point: Failpoint) -> bool {
        self.observed.contains(&point)
    }

    pub fn is_armed(&self, point: Failpoint) -> bool {
        self.armed.contains_key(&point)
    }

    pub fn take_injection(&mut self, point: Failpoint) -> Option<FailAction> {
        let index = self
            .pending
            .iter()
            .position(|(candidate, _)| *candidate == point)?;
        Some(self.pending.remove(index).1)
    }

    pub fn injected(&self) -> &[(Failpoint, FailAction)] {
        &self.injected
    }

    pub fn record(&mut self, evidence: FaultEvidence) -> bool {
        if evidence.action.is_continue()
            || !self.injected.contains(&(evidence.point, evidence.action))
        {
            return false;
        }
        self.evidence.insert(evidence);
        true
    }

    pub fn evidence(&self) -> impl Iterator<Item = &FaultEvidence> {
        self.evidence.iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SharedFaultInjector {
    inner: Arc<Mutex<FaultInjector>>,
}

impl SharedFaultInjector {
    pub fn arm(&self, point: Failpoint, action: FailAction) {
        self.with(|injector| injector.arm(point, action));
    }

    pub fn take_injection(&self, point: Failpoint) -> Option<FailAction> {
        self.with(|injector| injector.take_injection(point))
    }

    pub fn record(&self, evidence: FaultEvidence) -> bool {
        self.with(|injector| injector.record(evidence))
    }

    pub fn observed(&self, point: Failpoint) -> bool {
        self.with(|injector| injector.observed(point))
    }

    pub fn evidence(&self) -> Vec<FaultEvidence> {
        self.with(|injector| injector.evidence().copied().collect())
    }

    pub fn injected(&self) -> Vec<(Failpoint, FailAction)> {
        self.with(|injector| injector.injected().to_vec())
    }

    pub fn with<R>(&self, action: impl FnOnce(&mut FaultInjector) -> R) -> R {
        let mut injector = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        action(&mut injector)
    }

    pub fn install(&self) -> InstalledFaultInjector {
        let exclusive = exclusive()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let inner = self.inner.clone();
        let hook = failpoint::install_decision_hook(move |point| {
            inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .hit(point)
        });
        InstalledFaultInjector {
            hook,
            exclusive: Some(exclusive),
        }
    }
}

pub struct InstalledFaultInjector {
    #[allow(dead_code)]
    hook: FailpointGuard,
    exclusive: Option<MutexGuard<'static, ()>>,
}

impl Drop for InstalledFaultInjector {
    fn drop(&mut self) {
        self.exclusive.take();
    }
}

fn exclusive() -> &'static Mutex<()> {
    static EXCLUSIVE: OnceLock<Mutex<()>> = OnceLock::new();
    EXCLUSIVE.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Clone)]
pub struct FaultMatrix {
    required: BTreeSet<(Failpoint, FaultTarget)>,
    covered: BTreeMap<(Failpoint, FaultTarget), FaultOrigin>,
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
            covered: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, evidence: FaultEvidence) -> bool {
        if evidence.action.is_continue()
            || !self.required.contains(&(evidence.point, evidence.target))
        {
            return false;
        }
        let entry = self
            .covered
            .entry((evidence.point, evidence.target))
            .or_insert(evidence.origin);
        if evidence.origin == FaultOrigin::ProductionCallSite {
            *entry = FaultOrigin::ProductionCallSite;
        }
        true
    }

    pub fn required(&self) -> impl Iterator<Item = &(Failpoint, FaultTarget)> {
        self.required.iter()
    }

    pub fn covered(&self) -> impl Iterator<Item = (&(Failpoint, FaultTarget), &FaultOrigin)> {
        self.covered.iter()
    }

    pub fn covered_by(
        &self,
        origin: FaultOrigin,
    ) -> impl Iterator<Item = &(Failpoint, FaultTarget)> {
        self.covered
            .iter()
            .filter(move |(_, recorded)| **recorded == origin)
            .map(|(pair, _)| pair)
    }

    pub fn missing(&self) -> impl Iterator<Item = &(Failpoint, FaultTarget)> {
        self.required
            .iter()
            .filter(|pair| !self.covered.contains_key(pair))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> FaultEvidence {
        FaultEvidence {
            point: Failpoint::HandoffAfterDrainSend,
            target: FaultTarget::Network,
            action: FailAction::Drop,
            outcome: FaultOutcome::MessageLost,
            origin: FaultOrigin::SimulatedExecutor,
        }
    }

    #[test]
    fn evidence_without_a_matching_injection_is_refused() {
        let mut injector = FaultInjector::default();
        assert!(!injector.record(evidence()));
        injector.arm(Failpoint::HandoffAfterDrainSend, FailAction::Drop);
        assert_eq!(
            injector.hit(Failpoint::HandoffAfterDrainSend),
            FailAction::Drop
        );
        assert!(injector.record(evidence()));
        assert!(!injector.record(FaultEvidence {
            action: FailAction::Continue,
            ..evidence()
        }));
        assert!(!injector.record(FaultEvidence {
            action: FailAction::Crash,
            ..evidence()
        }));
        assert_eq!(injector.evidence().count(), 1);
    }

    #[test]
    fn an_armed_action_fires_exactly_once() {
        let mut injector = FaultInjector::default();
        injector.arm(Failpoint::HandoffAfterDrainSend, FailAction::Crash);
        assert_eq!(
            injector.hit(Failpoint::HandoffAfterDrainSend),
            FailAction::Crash
        );
        assert_eq!(
            injector.hit(Failpoint::HandoffAfterDrainSend),
            FailAction::Continue
        );
        assert!(injector.observed(Failpoint::HandoffAfterDrainSend));
        assert_eq!(
            injector.take_injection(Failpoint::HandoffAfterDrainSend),
            Some(FailAction::Crash)
        );
        assert_eq!(
            injector.take_injection(Failpoint::HandoffAfterDrainSend),
            None
        );
    }

    #[test]
    fn an_empty_matrix_reports_every_required_pair_as_missing() {
        let mut matrix = FaultMatrix::required_default();
        assert!(!matrix.record(FaultEvidence {
            action: FailAction::Continue,
            ..evidence()
        }));
        assert!(!matrix.record(FaultEvidence {
            target: FaultTarget::Queue,
            ..evidence()
        }));
        assert_eq!(matrix.missing().count(), matrix.required().count());
        assert!(matrix.record(evidence()));
        assert_eq!(matrix.missing().count(), matrix.required().count() - 1);
    }

    #[test]
    fn an_installed_injector_answers_production_failpoint_decisions() {
        let injector = SharedFaultInjector::default();
        let installed = injector.install();
        assert!(failpoint::hit_decision(Failpoint::HandoffAfterDrainSend).is_continue());
        injector.arm(Failpoint::HandoffAfterDrainSend, FailAction::StoreFailure);
        assert_eq!(
            failpoint::hit_decision(Failpoint::HandoffAfterDrainSend),
            FailAction::StoreFailure
        );
        drop(installed);
        assert!(failpoint::hit_decision(Failpoint::HandoffAfterDrainSend).is_continue());
    }
}
