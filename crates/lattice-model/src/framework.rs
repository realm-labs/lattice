//! Framework identity, distinct from application releases and runtime safety counters.

use serde::{Deserialize, Serialize};

/// Fails compilation when linked framework crates come from different release versions.
/// This is an internal integration guard, not an application version setting.
#[doc(hidden)]
pub const fn assert_release(version: &str) {
    let expected = env!("CARGO_PKG_VERSION").as_bytes();
    let actual = version.as_bytes();
    assert!(
        actual.len() == expected.len(),
        "mixed Lattice crate versions"
    );
    let mut index = 0;
    while index < expected.len() {
        assert!(
            actual[index] == expected[index],
            "mixed Lattice crate versions"
        );
        index += 1;
    }
}

/// Exact framework package version, supplied automatically rather than configured by an application.
///
/// Published packages and development builds both use the Cargo package version, without a source
/// digest. Developers must keep same-version builds consistent across nodes and bump the package
/// version for incompatible protocol or storage changes. Equality is exact; semantic-version
/// compatibility is not negotiated. This gate does not detect different sources with the same version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LatticeVersion(String);

impl LatticeVersion {
    pub const CURRENT: &'static str = env!("CARGO_PKG_VERSION");

    pub fn current() -> Self {
        Self(Self::CURRENT.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_current(&self) -> bool {
        self.as_str() == Self::CURRENT
    }
}

#[cfg(test)]
mod tests {
    use super::LatticeVersion;

    #[test]
    fn identity_is_automatic_and_exact() {
        let current = LatticeVersion::current();
        assert!(current.is_current());
        assert_eq!(current.as_str(), env!("CARGO_PKG_VERSION"));
        let encoded = serde_json::to_vec(&current).unwrap();
        assert_eq!(
            serde_json::from_slice::<LatticeVersion>(&encoded).unwrap(),
            current
        );
        let other: LatticeVersion = serde_json::from_str("\"different-framework\"").unwrap();
        assert!(!other.is_current());
    }
}
