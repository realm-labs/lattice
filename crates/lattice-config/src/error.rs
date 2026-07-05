use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unknown config format for path {path}")]
    UnknownFormat { path: PathBuf },
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML config: {source}")]
    ParseToml { source: toml::de::Error },
    #[error("failed to parse YAML config: {source}")]
    ParseYaml { source: serde_yaml::Error },
    #[error("failed to parse JSON config: {source}")]
    ParseJson { source: serde_json::Error },
    #[error("failed to convert config value: {0}")]
    SerializeConfig(serde_json::Error),
    #[error("bootstrap config root must be an object")]
    RootMustBeObject,
    #[error("missing config section {path}")]
    MissingSection { path: String },
    #[error("failed to decode config section {path}: {source}")]
    DecodeSection {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("environment config separator cannot be empty")]
    EmptyEnvSeparator,
}
