use std::{collections::BTreeMap, fmt};

use http::Uri;
use lattice_config::{error::ConfigError, source::ConfigSource};
use serde::{Deserialize, Serialize};

use crate::uri_serde;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies one process boot for a reusable configured [`InstanceId`].
///
/// This value is public fencing identity rather than a secret. Production
/// workload certificates bind it into their SPIFFE URI SAN.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstanceIncarnation(String);

impl InstanceIncarnation {
    pub const MAX_BYTES: usize = 128;

    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_canonical(&self) -> bool {
        !self.0.is_empty()
            && self.0.len() <= Self::MAX_BYTES
            && self.0 != "."
            && self.0 != ".."
            && self
                .0
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    }
}

impl fmt::Display for InstanceIncarnation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceConfig {
    pub instance_id: InstanceId,
    #[serde(with = "uri_serde")]
    pub advertised_endpoint: Uri,
    #[serde(with = "uri_serde")]
    pub control_endpoint: Uri,
    pub version: String,
    #[serde(default)]
    pub capacity: InstanceCapacity,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl InstanceConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        ConfigSource::env("LATTICE").load()?.section("instance")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCapacity {
    #[serde(default)]
    pub max_actors: Option<u64>,
    #[serde(default)]
    pub max_connections: Option<u64>,
}
