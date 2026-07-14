use std::time::Duration;

use thiserror::Error;

use crate::types::{
    ClaimGrant, GrantSequence, MonotonicTime, NodeKey, PlacementSlot, PlacementSlotState,
    PlacementTypeError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityEvent {
    ReconcileSlot(PlacementSlot),
    InstallGrant {
        grant: ClaimGrant,
        now: MonotonicTime,
    },
    Tick {
        now: MonotonicTime,
    },
    BeginDrain,
    StopSucceeded,
    StopFailed,
    ExternalClaimLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityEffect {
    FenceAdmission,
    OpenAdmission,
    StartSlot,
    DrainSlot,
    StopSlot,
    PublishReady,
    PublishDrained,
    PublishStopFailed,
    StateLossPossible,
}

#[derive(Debug, Clone)]
struct InstalledGrant {
    sequence: GrantSequence,
    deadline: MonotonicTime,
}

#[derive(Debug, Clone)]
pub struct PlacementAuthority {
    local_node: NodeKey,
    safety_margin: Duration,
    slot: Option<PlacementSlot>,
    grant: Option<InstalledGrant>,
    admission_open: bool,
}

impl PlacementAuthority {
    pub fn new(local_node: NodeKey, safety_margin: Duration) -> Result<Self, AuthorityError> {
        local_node.validate().map_err(AuthorityError::InvalidType)?;
        if safety_margin.is_zero() {
            return Err(AuthorityError::ZeroSafetyMargin);
        }
        Ok(Self {
            local_node,
            safety_margin,
            slot: None,
            grant: None,
            admission_open: false,
        })
    }

    pub fn transition(
        &mut self,
        event: AuthorityEvent,
    ) -> Result<Vec<AuthorityEffect>, AuthorityError> {
        let before = self.clone();
        let result = self.apply(event);
        if result.is_err() {
            *self = before;
        }
        result
    }

    pub fn slot(&self) -> Option<&PlacementSlot> {
        self.slot.as_ref()
    }

    pub fn admission_open(&self) -> bool {
        self.admission_open
    }

    fn apply(&mut self, event: AuthorityEvent) -> Result<Vec<AuthorityEffect>, AuthorityError> {
        match event {
            AuthorityEvent::ReconcileSlot(slot) => {
                slot.validate().map_err(AuthorityError::InvalidType)?;
                let same_authority = self.slot.as_ref().is_some_and(|current| {
                    current.key == slot.key
                        && current.owner == slot.owner
                        && current.assignment_generation == slot.assignment_generation
                        && current.version.term == slot.version.term
                });
                self.slot = Some(slot);
                if !same_authority {
                    self.grant = None;
                    return Ok(self.fence_effects());
                }
                Ok(Vec::new())
            }
            AuthorityEvent::InstallGrant { grant, now } => self.install_grant(grant, now),
            AuthorityEvent::Tick { now } => {
                if self
                    .grant
                    .as_ref()
                    .is_some_and(|grant| now >= grant.deadline)
                {
                    self.grant = None;
                    let mut effects = self.fence_effects();
                    effects.push(AuthorityEffect::StopSlot);
                    Ok(effects)
                } else {
                    Ok(Vec::new())
                }
            }
            AuthorityEvent::BeginDrain => {
                self.require_current_authority()?;
                if !matches!(
                    self.slot.as_ref().map(|slot| slot.state),
                    Some(PlacementSlotState::BeginHandoff | PlacementSlotState::Stopping)
                ) {
                    return Err(AuthorityError::IllegalTransition);
                }
                self.admission_open = false;
                Ok(vec![
                    AuthorityEffect::FenceAdmission,
                    AuthorityEffect::DrainSlot,
                ])
            }
            AuthorityEvent::StopSucceeded => {
                if self.admission_open {
                    return Err(AuthorityError::IllegalTransition);
                }
                Ok(vec![AuthorityEffect::PublishDrained])
            }
            AuthorityEvent::StopFailed => {
                let slot = self.slot.as_mut().ok_or(AuthorityError::NoSlot)?;
                slot.state = PlacementSlotState::StopFailed;
                Ok(vec![AuthorityEffect::PublishStopFailed])
            }
            AuthorityEvent::ExternalClaimLost => {
                self.grant = None;
                let mut effects = self.fence_effects();
                effects.push(AuthorityEffect::StopSlot);
                if self
                    .slot
                    .as_ref()
                    .is_some_and(|slot| slot.state == PlacementSlotState::StopFailed)
                {
                    effects.push(AuthorityEffect::StateLossPossible);
                }
                Ok(effects)
            }
        }
    }

    fn install_grant(
        &mut self,
        grant: ClaimGrant,
        now: MonotonicTime,
    ) -> Result<Vec<AuthorityEffect>, AuthorityError> {
        grant
            .validate(self.safety_margin)
            .map_err(AuthorityError::InvalidType)?;
        let slot = self.slot.as_ref().ok_or(AuthorityError::NoSlot)?;
        if grant.slot != slot.key
            || grant.owner != self.local_node
            || slot.owner.as_ref() != Some(&self.local_node)
            || grant.assignment_generation != slot.assignment_generation
            || grant.coordinator_term != slot.version.term
        {
            return Err(AuthorityError::StaleGrant);
        }
        if self
            .grant
            .as_ref()
            .is_some_and(|current| grant.grant_sequence < current.sequence)
        {
            return Err(AuthorityError::StaleGrant);
        }
        let usable_ttl = grant
            .ttl
            .checked_sub(self.safety_margin)
            .ok_or(AuthorityError::InvalidClaimDeadline)?;
        let deadline = now
            .checked_add(usable_ttl)
            .ok_or(AuthorityError::InvalidClaimDeadline)?;
        let first = self.grant.is_none();
        self.grant = Some(InstalledGrant {
            sequence: grant.grant_sequence,
            deadline,
        });
        let can_serve = matches!(
            slot.state,
            PlacementSlotState::Allocating | PlacementSlotState::Running
        );
        if can_serve && !self.admission_open {
            self.admission_open = true;
            let mut effects = vec![AuthorityEffect::OpenAdmission];
            if first {
                effects.insert(0, AuthorityEffect::StartSlot);
                effects.push(AuthorityEffect::PublishReady);
            }
            Ok(effects)
        } else {
            Ok(Vec::new())
        }
    }

    fn require_current_authority(&self) -> Result<(), AuthorityError> {
        if self.grant.is_none() || !self.admission_open {
            return Err(AuthorityError::NotAuthorized);
        }
        Ok(())
    }

    fn fence_effects(&mut self) -> Vec<AuthorityEffect> {
        if self.admission_open {
            self.admission_open = false;
            vec![AuthorityEffect::FenceAdmission]
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthorityError {
    #[error("placement value is invalid")]
    InvalidType(#[source] PlacementTypeError),
    #[error("claim safety margin must be nonzero")]
    ZeroSafetyMargin,
    #[error("placement slot has not been reconciled")]
    NoSlot,
    #[error("claim grant is stale or does not match the exact slot owner")]
    StaleGrant,
    #[error("claim deadline cannot be represented")]
    InvalidClaimDeadline,
    #[error("slot does not currently have serving authority")]
    NotAuthorized,
    #[error("placement authority transition is illegal in the current state")]
    IllegalTransition,
}
