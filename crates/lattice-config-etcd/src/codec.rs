use std::fmt::Display;

use lattice_config::store::ConfigStoreError;

pub(crate) fn encode_value(value: &serde_json::Value) -> Result<Vec<u8>, ConfigStoreError> {
    serde_json::to_vec(value).map_err(codec_error)
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<serde_json::Value, ConfigStoreError> {
    serde_json::from_slice(bytes).map_err(codec_error)
}

pub(crate) fn normalize_prefix(prefix: &str) -> Result<String, ConfigStoreError> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Err(backend_error(
            "etcd config store key prefix must not be empty",
        ));
    }
    Ok(format!("/{trimmed}"))
}

pub(crate) fn etcd_error(error: etcd_client::Error) -> ConfigStoreError {
    backend_error(error)
}

pub(crate) fn backend_error(message: impl Display) -> ConfigStoreError {
    ConfigStoreError::Backend {
        message: message.to_string(),
    }
}

pub(crate) fn codec_error(error: impl Display) -> ConfigStoreError {
    ConfigStoreError::Codec {
        message: error.to_string(),
    }
}
