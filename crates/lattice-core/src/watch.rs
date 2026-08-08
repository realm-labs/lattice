use uuid::Uuid;

/// Globally unique identity for one DeathWatch registration.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct WatchId {
    watcher_boot: u128,
    sequence: u64,
}

impl WatchId {
    pub const fn new(watcher_boot: u128, sequence: u64) -> Option<Self> {
        if watcher_boot == 0 || sequence == 0 {
            None
        } else {
            Some(Self {
                watcher_boot,
                sequence,
            })
        }
    }

    /// Creates a standalone watch identity for an Actor-local registration.
    pub fn random() -> Self {
        Self {
            watcher_boot: Uuid::new_v4().as_u128(),
            sequence: 1,
        }
    }

    pub const fn watcher_boot(self) -> u128 {
        self.watcher_boot
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TerminatedReason {
    Stopped,
    Panicked,
    Passivated,
    Migrated,
    Fenced,
    NodeDown,
    ActivationChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchStatus {
    Pending,
    Active,
    Terminated,
    Unknown,
}
