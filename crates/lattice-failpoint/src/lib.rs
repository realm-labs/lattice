//! Lightweight failpoint mechanism shared by production crates and test harnesses.
//!
//! This crate deliberately does not own a catalogue. Each subsystem declares
//! its failpoint IDs next to the code whose boundary they describe.

#[cfg(feature = "enabled")]
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FailpointId(&'static str);

impl FailpointId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn name(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FailpointAction {
    #[default]
    Continue,
    Crash,
    Pause,
    Drop,
    Duplicate,
    StoreFailure,
}

impl FailpointAction {
    pub const fn is_continue(self) -> bool {
        matches!(self, Self::Continue)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Crash => "crash",
            Self::Pause => "pause",
            Self::Drop => "drop",
            Self::Duplicate => "duplicate",
            Self::StoreFailure => "store-failure",
        }
    }
}

#[cfg(feature = "enabled")]
type Hook = Arc<dyn Fn(FailpointId) -> FailpointAction + Send + Sync>;

#[cfg(feature = "enabled")]
fn hook() -> &'static RwLock<Option<Hook>> {
    static HOOK: OnceLock<RwLock<Option<Hook>>> = OnceLock::new();
    HOOK.get_or_init(|| RwLock::new(None))
}

pub fn hit(point: impl Into<FailpointId>) {
    let _ = hit_decision(point);
}

pub fn hit_decision(point: impl Into<FailpointId>) -> FailpointAction {
    let point = point.into();
    #[cfg(feature = "enabled")]
    if let Some(hook) = hook().read().expect("failpoint hook poisoned").clone() {
        return hook(point);
    }
    #[cfg(not(feature = "enabled"))]
    let _ = point;
    FailpointAction::Continue
}

#[cfg(feature = "enabled")]
pub struct FailpointGuard {
    previous: Option<Hook>,
}

#[cfg(feature = "enabled")]
impl Drop for FailpointGuard {
    fn drop(&mut self) {
        *hook().write().expect("failpoint hook poisoned") = self.previous.take();
    }
}

#[cfg(feature = "enabled")]
pub fn install_hook(hook_fn: impl Fn(FailpointId) + Send + Sync + 'static) -> FailpointGuard {
    install_decision_hook(move |point| {
        hook_fn(point);
        FailpointAction::Continue
    })
}

#[cfg(feature = "enabled")]
pub fn install_decision_hook(
    hook_fn: impl Fn(FailpointId) -> FailpointAction + Send + Sync + 'static,
) -> FailpointGuard {
    let mut active = hook().write().expect("failpoint hook poisoned");
    let previous = active.replace(Arc::new(hook_fn));
    FailpointGuard { previous }
}

#[cfg(test)]
mod tests {
    use super::{FailpointAction, FailpointId};

    #[test]
    fn identifiers_and_actions_are_stable_values() {
        let point = FailpointId::new("after_commit");

        assert_eq!(point.name(), "after_commit");
        assert!(FailpointAction::Continue.is_continue());
        assert_eq!(FailpointAction::StoreFailure.name(), "store-failure");
    }
}
