use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdWorkerIdStoreConfig {
    pub endpoints: Vec<String>,
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
}

impl Default for EtcdWorkerIdStoreConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            key_prefix: default_key_prefix(),
        }
    }
}

impl EtcdWorkerIdStoreConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.endpoints.is_empty() || self.endpoints.iter().any(|endpoint| endpoint.is_empty()) {
            return Err("Etcd worker ID endpoints must be nonempty");
        }
        validate_key_prefix(&self.key_prefix)
    }
}

impl std::fmt::Debug for EtcdWorkerIdStoreConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EtcdWorkerIdStoreConfig")
            .field("endpoint_count", &self.endpoints.len())
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

pub(crate) fn validate_key_prefix(prefix: &str) -> Result<(), &'static str> {
    if prefix.is_empty()
        || !prefix.starts_with('/')
        || prefix == "/"
        || prefix.ends_with('/')
        || prefix.contains("//")
        || prefix.contains(['\\', '\0'])
        || prefix.chars().any(char::is_control)
    {
        return Err("Etcd worker ID key prefix must be a canonical absolute path");
    }
    Ok(())
}

fn default_key_prefix() -> String {
    "/lattice/worker-ids".to_string()
}

#[cfg(test)]
mod tests {
    use super::EtcdWorkerIdStoreConfig;

    #[test]
    fn default_uses_the_documented_key_prefix() {
        let config = EtcdWorkerIdStoreConfig::default();
        assert!(config.endpoints.is_empty());
        assert_eq!(config.key_prefix, "/lattice/worker-ids");
    }
}
