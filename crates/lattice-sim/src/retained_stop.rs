use lattice_actor::traits::ActorLifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedStopEvent {
    BeginVoluntaryDrain,
    RetryPersistenceFails,
    ExternalAuthorityLost,
    MembershipLost,
    RetryPersistenceSucceeds,
    ForceDiscard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedStopState {
    pub cell: ActorLifecycleState,
    pub instance_id: u64,
    pub in_memory_value: u64,
    pub authoritative: bool,
    pub business_admitted: bool,
    pub replacement_allowed: bool,
    pub drain_blocked: bool,
    pub membership_up: bool,
    pub terminal_notifications: u8,
    pub forced_data_loss_events: u8,
}

impl Default for RetainedStopState {
    fn default() -> Self {
        Self {
            cell: ActorLifecycleState::Running,
            instance_id: 1,
            in_memory_value: 42,
            authoritative: true,
            business_admitted: true,
            replacement_allowed: false,
            drain_blocked: false,
            membership_up: true,
            terminal_notifications: 0,
            forced_data_loss_events: 0,
        }
    }
}

impl RetainedStopState {
    pub fn apply(&mut self, event: RetainedStopEvent) {
        match event {
            RetainedStopEvent::BeginVoluntaryDrain if self.cell == ActorLifecycleState::Running => {
                self.cell = ActorLifecycleState::StopFailed;
                self.business_admitted = false;
                self.drain_blocked = true;
            }
            RetainedStopEvent::RetryPersistenceFails
                if matches!(
                    self.cell,
                    ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
                ) => {}
            RetainedStopEvent::ExternalAuthorityLost
                if self.cell != ActorLifecycleState::Stopped =>
            {
                self.authoritative = false;
                self.business_admitted = false;
                self.replacement_allowed = true;
                self.cell = ActorLifecycleState::Quarantined;
                self.drain_blocked = false;
            }
            RetainedStopEvent::MembershipLost => {
                self.membership_up = false;
                self.business_admitted = false;
            }
            RetainedStopEvent::RetryPersistenceSucceeds
                if matches!(
                    self.cell,
                    ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
                ) =>
            {
                self.cell = ActorLifecycleState::Stopped;
                self.drain_blocked = false;
                self.terminal_notifications = 1;
            }
            RetainedStopEvent::ForceDiscard
                if matches!(
                    self.cell,
                    ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
                ) =>
            {
                self.cell = ActorLifecycleState::Stopped;
                self.drain_blocked = false;
                self.terminal_notifications = 1;
                self.forced_data_loss_events = 1;
            }
            RetainedStopEvent::BeginVoluntaryDrain
            | RetainedStopEvent::RetryPersistenceFails
            | RetainedStopEvent::ExternalAuthorityLost
            | RetainedStopEvent::RetryPersistenceSucceeds
            | RetainedStopEvent::ForceDiscard => {}
        }
    }

    pub fn check_invariants(&self) -> Result<(), &'static str> {
        if matches!(
            self.cell,
            ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
        ) && self.business_admitted
        {
            return Err("retained actor admitted business traffic");
        }
        if self.cell == ActorLifecycleState::StopFailed
            && (!self.authoritative || self.replacement_allowed || !self.drain_blocked)
        {
            return Err("voluntary StopFailed did not retain its authority reservation");
        }
        if self.cell == ActorLifecycleState::Quarantined
            && (self.authoritative || !self.replacement_allowed)
        {
            return Err("quarantined actor retained authority or delayed replacement");
        }
        if self.cell != ActorLifecycleState::Stopped && self.terminal_notifications != 0 {
            return Err("nonterminal retained actor emitted termination");
        }
        if self.terminal_notifications > 1 || self.forced_data_loss_events > 1 {
            return Err("terminal or forced-data-loss evidence was duplicated");
        }
        Ok(())
    }
}

pub fn replay_retained_stop(
    events: impl IntoIterator<Item = RetainedStopEvent>,
) -> Result<Vec<RetainedStopState>, &'static str> {
    let mut state = RetainedStopState::default();
    let mut trace = vec![state.clone()];
    for event in events {
        state.apply(event);
        state.check_invariants()?;
        trace.push(state.clone());
    }
    Ok(trace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_failure_claim_loss_and_retry_replay_is_deterministic() {
        let events = [
            RetainedStopEvent::BeginVoluntaryDrain,
            RetainedStopEvent::RetryPersistenceFails,
            RetainedStopEvent::MembershipLost,
            RetainedStopEvent::ExternalAuthorityLost,
            RetainedStopEvent::RetryPersistenceFails,
            RetainedStopEvent::RetryPersistenceSucceeds,
            RetainedStopEvent::RetryPersistenceSucceeds,
        ];
        let first = replay_retained_stop(events).unwrap();
        let second = replay_retained_stop(events).unwrap();

        assert_eq!(first, second);
        assert_eq!(first[2].cell, ActorLifecycleState::StopFailed);
        assert_eq!(first[2].instance_id, 1);
        assert_eq!(first[2].in_memory_value, 42);
        assert_eq!(first[2].terminal_notifications, 0);
        assert_eq!(first[4].cell, ActorLifecycleState::Quarantined);
        assert!(first[4].replacement_allowed);
        assert_eq!(first.last().unwrap().terminal_notifications, 1);
        assert_eq!(first.last().unwrap().forced_data_loss_events, 0);
    }

    #[test]
    fn duplicate_force_discard_emits_one_data_loss_and_terminal_event() {
        let trace = replay_retained_stop([
            RetainedStopEvent::BeginVoluntaryDrain,
            RetainedStopEvent::ForceDiscard,
            RetainedStopEvent::ForceDiscard,
        ])
        .unwrap();
        let final_state = trace.last().unwrap();
        assert_eq!(final_state.cell, ActorLifecycleState::Stopped);
        assert_eq!(final_state.terminal_notifications, 1);
        assert_eq!(final_state.forced_data_loss_events, 1);
    }

    #[test]
    fn bounded_quarantine_rejects_overflow_without_dropping_existing_state() {
        let capacity = 1;
        let mut retained = [RetainedStopState::default(), RetainedStopState::default()];
        retained[0].apply(RetainedStopEvent::BeginVoluntaryDrain);
        retained[0].apply(RetainedStopEvent::ExternalAuthorityLost);
        assert_eq!(
            retained
                .iter()
                .filter(|state| state.cell == ActorLifecycleState::Quarantined)
                .count(),
            capacity
        );

        retained[1].apply(RetainedStopEvent::BeginVoluntaryDrain);
        let overflow = retained
            .iter()
            .filter(|state| state.cell == ActorLifecycleState::Quarantined)
            .count()
            >= capacity;
        assert!(overflow, "capacity exhaustion must be an explicit result");
        assert_eq!(retained[0].in_memory_value, 42);
        assert_eq!(retained[1].cell, ActorLifecycleState::StopFailed);
        assert_eq!(retained[1].in_memory_value, 42);
    }
}
