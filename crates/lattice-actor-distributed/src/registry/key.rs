//! Application-defined keys used by the distributed activation registry.

use std::{borrow::Cow, fmt};

use serde::{Deserialize, Serialize};

/// Application-shaped key for one registry activation.
///
/// A key is local registry vocabulary. It is encoded into a validated
/// [`ActorPath`](lattice_model::actor::ActorPath) only when an activation is published.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ActorKey {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

/// Stable application name for one registered Actor implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActorKind(Cow<'static, str>);

impl ActorKind {
    pub const fn from_static(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }

    pub fn new(value: impl Into<String>) -> Self {
        Self(Cow::Owned(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ActorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[macro_export]
macro_rules! actor_kind {
    ($name:literal) => {
        $crate::registry::ActorKind::from_static($name)
    };
}

#[cfg(test)]
mod tests {
    use super::ActorKind;

    const WORLD: ActorKind = crate::actor_kind!("World");

    #[test]
    fn actor_kind_macro_is_const() {
        assert_eq!(WORLD.as_str(), "World");
    }
}
