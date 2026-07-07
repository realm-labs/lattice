use lattice_config::store::ConfigStoreError;

pub(crate) fn encode_value(value: &serde_json::Value) -> Result<Vec<u8>, ConfigStoreError> {
    serde_json::to_vec(value).map_err(codec_error)
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<serde_json::Value, ConfigStoreError> {
    serde_json::from_slice(bytes).map_err(codec_error)
}

pub(crate) fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "/lattice/config".to_string()
    } else {
        format!("/{trimmed}")
    }
}

pub(crate) fn etcd_error(error: etcd_client::Error) -> ConfigStoreError {
    ConfigStoreError::Backend {
        message: error.to_string(),
    }
}

pub(crate) fn codec_error(error: impl std::fmt::Display) -> ConfigStoreError {
    ConfigStoreError::Codec {
        message: error.to_string(),
    }
}
