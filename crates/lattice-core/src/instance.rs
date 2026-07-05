use std::collections::BTreeMap;
use std::fmt;

use http::Uri;
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
    pub fn from_env() -> Result<Self, lattice_config::ConfigError> {
        lattice_config::ConfigSource::env("LATTICE")
            .load()?
            .section("instance")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCapacity {
    #[serde(default)]
    pub max_actors: Option<u64>,
    #[serde(default)]
    pub max_connections: Option<u64>,
}
