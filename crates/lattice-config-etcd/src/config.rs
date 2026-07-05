use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdConfigStoreConfig {
    pub key_prefix: String,
    pub endpoints: Vec<String>,
}
