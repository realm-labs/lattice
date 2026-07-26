//! Seed-driven workload generation.
//!
//! Every scheduling decision and every armed failpoint is derived from the scenario seed through a
//! dedicated random stream, so a seed alone reproduces both the event interleaving and the fault
//! set it runs against.

use lattice_core::failpoint::Failpoint;

use super::{HandoffStep, MAXIMUM_ATTEMPTS, Scenario, ScenarioError, ScenarioEvent, incarnation};
use crate::{clock::SimRandom, fault::FailAction};

const WORKLOAD_STREAM: u64 = 0x9E37_79B9_7F4A_7C15;

impl Scenario {
    pub fn schedule_standard_workload(&mut self) -> Result<(), ScenarioError> {
        let mut random = SimRandom::new(self.config.seed ^ WORKLOAD_STREAM);
        self.arm_seeded_faults(&mut random);
        let mut at = 1;
        let mut next = |random: &mut SimRandom| {
            at += u64::try_from(random.below(2)).unwrap_or(0);
            at
        };
        self.schedule(next(&mut random), ScenarioEvent::InstallWatch);
        let mut barrier = [barrier_step(&mut random, 1), barrier_step(&mut random, 2)];
        random.shuffle(&mut barrier);
        for step in barrier {
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::Handoff(step, MAXIMUM_ATTEMPTS));
        }
        for step in [
            HandoffStep::SourceInvalid,
            HandoffStep::TargetClaimInstalled,
            HandoffStep::TargetReady,
        ] {
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::Handoff(step, MAXIMUM_ATTEMPTS));
        }
        for command in 1..=u128::try_from(1 + random.below(2)).unwrap_or(1) {
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::SendControl(command));
        }
        if random.chance(1, 2) {
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::PartitionControl);
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::HealControl);
        }
        let at = next(&mut random);
        self.schedule(at, ScenarioEvent::ReplayControl);
        for noise in noise_steps(&mut random) {
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::Handoff(noise, 0));
        }
        if random.chance(1, 2) {
            let at = next(&mut random);
            self.schedule(at, ScenarioEvent::TargetTerminated);
        }
        let at = next(&mut random);
        self.schedule(at, ScenarioEvent::NodeDown(incarnation(1)));
        let at = next(&mut random);
        self.schedule(at, ScenarioEvent::NodeDown(incarnation(1)));
        Ok(())
    }

    fn arm_seeded_faults(&mut self, random: &mut SimRandom) {
        for (point, actions) in armable_boundaries() {
            if random.chance(1, 3) {
                let action = actions[random.below(actions.len())];
                self.faults.arm(*point, action);
            }
        }
    }
}

fn armable_boundaries() -> &'static [(Failpoint, &'static [FailAction])] {
    &[
        (
            Failpoint::HandoffAfterPartialBarrier,
            &[
                FailAction::Drop,
                FailAction::Crash,
                FailAction::StoreFailure,
            ],
        ),
        (
            Failpoint::HandoffAfterShardDrainedBeforeClaimRevoke,
            &[
                FailAction::Crash,
                FailAction::StoreFailure,
                FailAction::Pause,
            ],
        ),
        (
            Failpoint::HandoffAfterNewClaimBeforeGrantSend,
            &[
                FailAction::Drop,
                FailAction::Crash,
                FailAction::StoreFailure,
            ],
        ),
        (
            Failpoint::HandoffAfterGrantBeforeShardReady,
            &[FailAction::Pause, FailAction::Drop, FailAction::Crash],
        ),
        (
            Failpoint::HandoffAfterActivePersistBeforeDelta,
            &[FailAction::StoreFailure, FailAction::Crash],
        ),
        (
            Failpoint::ControlAfterOutboxBeforeSocketWrite,
            &[FailAction::Drop, FailAction::Duplicate],
        ),
        (
            Failpoint::ControlAfterRemoteApplyBeforeAck,
            &[FailAction::Drop, FailAction::Crash],
        ),
        (Failpoint::WatchAfterInstallBeforeAck, &[FailAction::Drop]),
        (
            Failpoint::WatchAfterTerminatedBeforeAck,
            &[FailAction::Drop],
        ),
    ]
}

fn barrier_step(random: &mut SimRandom, session: u128) -> HandoffStep {
    if random.chance(1, 4) {
        HandoffStep::FenceBarrier(incarnation(session))
    } else {
        HandoffStep::ApplyBarrier(incarnation(session))
    }
}

fn noise_steps(random: &mut SimRandom) -> Vec<HandoffStep> {
    let candidates = [
        HandoffStep::ApplyBarrier(incarnation(9)),
        HandoffStep::TargetReady,
        HandoffStep::SourceInvalid,
        HandoffStep::TargetClaimInstalled,
    ];
    let mut selected = Vec::new();
    for candidate in candidates {
        if random.chance(1, 2) {
            selected.push(candidate);
        }
    }
    selected
}
