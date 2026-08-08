use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Monotonically increasing application release identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReleaseId(u64);

impl ReleaseId {
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exact compatibility contract for a code-only rolling upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseCompatibility {
    pub transport_generation: u64,
    pub control_generation: u64,
    pub storage_generation: u64,
    pub actor_protocol_fingerprint: [u8; 32],
    pub actor_state_fingerprint: [u8; 32],
    pub placement_fingerprint: [u8; 32],
    pub service_abi_fingerprint: [u8; 32],
}

impl ReleaseCompatibility {
    pub fn validate(&self) -> Result<(), ReleaseError> {
        if self.transport_generation == 0
            || self.control_generation == 0
            || self.storage_generation == 0
            || [
                self.actor_protocol_fingerprint,
                self.actor_state_fingerprint,
                self.placement_fingerprint,
                self.service_abi_fingerprint,
            ]
            .contains(&[0; 32])
        {
            return Err(ReleaseError::InvalidManifest);
        }
        Ok(())
    }

    /// Stable digest suitable for logs, status APIs and deployment tooling.
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut canonical = b"lattice-code-only-release-v1".to_vec();
        canonical.extend_from_slice(&self.transport_generation.to_be_bytes());
        canonical.extend_from_slice(&self.control_generation.to_be_bytes());
        canonical.extend_from_slice(&self.storage_generation.to_be_bytes());
        canonical.extend_from_slice(&self.actor_protocol_fingerprint);
        canonical.extend_from_slice(&self.actor_state_fingerprint);
        canonical.extend_from_slice(&self.placement_fingerprint);
        canonical.extend_from_slice(&self.service_abi_fingerprint);
        *blake3::hash(&canonical).as_bytes()
    }

    /// Convenience contract for examples and local tests.
    ///
    /// Production applications should provide independently generated
    /// fingerprints for every compatibility dimension.
    pub const fn development() -> Self {
        Self {
            transport_generation: 1,
            control_generation: 9,
            storage_generation: 6,
            actor_protocol_fingerprint: [1; 32],
            actor_state_fingerprint: [2; 32],
            placement_fingerprint: [3; 32],
            service_abi_fingerprint: [4; 32],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseManifest {
    pub release_id: ReleaseId,
    pub compatibility: ReleaseCompatibility,
}

impl ReleaseManifest {
    pub fn new(
        release_id: ReleaseId,
        compatibility: ReleaseCompatibility,
    ) -> Result<Self, ReleaseError> {
        compatibility.validate()?;
        Ok(Self {
            release_id,
            compatibility,
        })
    }

    pub fn development(release: u64) -> Self {
        Self::new(
            ReleaseId::new(release).expect("development release must be nonzero"),
            ReleaseCompatibility::development(),
        )
        .expect("development compatibility is valid")
    }

    pub fn validate(&self) -> Result<(), ReleaseError> {
        if self.release_id.get() == 0 {
            return Err(ReleaseError::InvalidManifest);
        }
        self.compatibility.validate()
    }

    pub fn validate_framework_generations(
        &self,
        transport: u64,
        control: u64,
        storage: u64,
    ) -> Result<(), ReleaseError> {
        self.validate()?;
        if self.compatibility.transport_generation != transport
            || self.compatibility.control_generation != control
            || self.compatibility.storage_generation != storage
        {
            return Err(ReleaseError::FrameworkGenerationMismatch);
        }
        Ok(())
    }

    pub fn compatibility_fingerprint(&self) -> [u8; 32] {
        self.compatibility.fingerprint()
    }
}

/// Release composition derived from live, lease-backed cluster members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ClusterReleaseState {
    Empty,
    Stable {
        release: ReleaseManifest,
    },
    Rolling {
        from: ReleaseManifest,
        to: ReleaseManifest,
    },
}

impl ClusterReleaseState {
    pub fn from_manifests<'a>(
        manifests: impl IntoIterator<Item = &'a ReleaseManifest>,
    ) -> Result<Self, ReleaseError> {
        let mut releases = BTreeMap::<ReleaseId, ReleaseManifest>::new();
        for manifest in manifests {
            manifest.validate()?;
            if let Some(existing) = releases.get(&manifest.release_id) {
                if existing != manifest {
                    return Err(ReleaseError::ReleaseIdConflict {
                        release_id: manifest.release_id,
                    });
                }
            } else {
                releases.insert(manifest.release_id, manifest.clone());
            }
        }
        match releases.len() {
            0 => Ok(Self::Empty),
            1 => Ok(Self::Stable {
                release: releases.into_values().next().expect("one release exists"),
            }),
            2 => {
                let mut values = releases.into_values();
                let from = values.next().expect("old release exists");
                let to = values.next().expect("new release exists");
                require_code_only_compatibility(&from, &to)?;
                Ok(Self::Rolling { from, to })
            }
            _ => Err(ReleaseError::TooManyActiveReleases),
        }
    }

    pub fn admit(&self, incoming: &ReleaseManifest) -> Result<Self, ReleaseError> {
        incoming.validate()?;
        match self {
            Self::Empty => Ok(Self::Stable {
                release: incoming.clone(),
            }),
            Self::Stable { release } if release == incoming => Ok(self.clone()),
            Self::Stable { release } if release.release_id == incoming.release_id => {
                Err(ReleaseError::ReleaseIdConflict {
                    release_id: incoming.release_id,
                })
            }
            Self::Stable { release } => {
                require_code_only_compatibility(release, incoming)?;
                if incoming.release_id < release.release_id {
                    return Err(ReleaseError::ReleaseRegression {
                        stable: release.release_id,
                        incoming: incoming.release_id,
                    });
                }
                Ok(Self::Rolling {
                    from: release.clone(),
                    to: incoming.clone(),
                })
            }
            Self::Rolling { from, to } if incoming == from || incoming == to => Ok(self.clone()),
            Self::Rolling { from, to }
                if incoming.release_id == from.release_id
                    || incoming.release_id == to.release_id =>
            {
                Err(ReleaseError::ReleaseIdConflict {
                    release_id: incoming.release_id,
                })
            }
            Self::Rolling { .. } => Err(ReleaseError::TooManyActiveReleases),
        }
    }

    pub fn target_release(&self) -> Option<ReleaseId> {
        match self {
            Self::Empty => None,
            Self::Stable { release } => Some(release.release_id),
            Self::Rolling { to, .. } => Some(to.release_id),
        }
    }
}

fn require_code_only_compatibility(
    current: &ReleaseManifest,
    incoming: &ReleaseManifest,
) -> Result<(), ReleaseError> {
    if current.compatibility != incoming.compatibility {
        return Err(ReleaseError::FullRestartRequired {
            current: current.release_id,
            incoming: incoming.release_id,
        });
    }
    Ok(())
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReleaseError {
    #[error("release manifest generations must be nonzero")]
    InvalidManifest,
    #[error("release manifest does not match the running lattice framework generations")]
    FrameworkGenerationMismatch,
    #[error("release {release_id:?} is associated with multiple manifests")]
    ReleaseIdConflict { release_id: ReleaseId },
    #[error(
        "release {incoming:?} is incompatible with {current:?}; a full deployment restart is required"
    )]
    FullRestartRequired {
        current: ReleaseId,
        incoming: ReleaseId,
    },
    #[error("a code-only rolling upgrade supports at most two active releases")]
    TooManyActiveReleases,
    #[error("incoming release {incoming:?} is older than stable release {stable:?}")]
    ReleaseRegression {
        stable: ReleaseId,
        incoming: ReleaseId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_exactly_compatible_n_plus_one_and_rejects_a_third_release() {
        let n = ReleaseManifest::development(10);
        let next = ReleaseManifest::development(11);
        let state = ClusterReleaseState::from_manifests([&n])
            .unwrap()
            .admit(&next)
            .unwrap();
        assert_eq!(state.target_release(), Some(next.release_id));
        assert_eq!(
            state.admit(&ReleaseManifest::development(12)),
            Err(ReleaseError::TooManyActiveReleases)
        );
    }

    #[test]
    fn protocol_or_state_changes_require_a_full_restart() {
        let n = ReleaseManifest::development(10);
        let mut incompatible = ReleaseManifest::development(11);
        incompatible.compatibility.actor_state_fingerprint = [7; 32];
        assert!(matches!(
            ClusterReleaseState::from_manifests([&n])
                .unwrap()
                .admit(&incompatible),
            Err(ReleaseError::FullRestartRequired { .. })
        ));
    }

    #[test]
    fn rejects_release_id_reuse_and_implicit_reverse_rollout() {
        let n = ReleaseManifest::development(10);
        let mut conflicting = n.clone();
        conflicting.compatibility.service_abi_fingerprint = [9; 32];
        assert!(matches!(
            ClusterReleaseState::from_manifests([&n])
                .unwrap()
                .admit(&conflicting),
            Err(ReleaseError::ReleaseIdConflict { .. })
        ));
        assert!(matches!(
            ClusterReleaseState::from_manifests([&n])
                .unwrap()
                .admit(&ReleaseManifest::development(9)),
            Err(ReleaseError::ReleaseRegression { .. })
        ));
    }

    #[test]
    fn deserialized_zero_release_id_is_rejected() {
        let manifest = ReleaseManifest::development(1);
        let mut value = serde_json::to_value(manifest).unwrap();
        value["release_id"] = serde_json::json!(0);
        let invalid: ReleaseManifest = serde_json::from_value(value).unwrap();
        assert_eq!(invalid.validate(), Err(ReleaseError::InvalidManifest));
    }
}
