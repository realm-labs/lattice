use std::env;
use std::fs;
use std::path::PathBuf;

use crate::bootstrap::BootstrapConfig;
use crate::{ConfigError, ConfigFormat};

#[derive(Debug, Clone)]
pub enum ConfigSource {
    File {
        path: PathBuf,
        format: Option<ConfigFormat>,
    },
    Env {
        prefix: String,
        separator: String,
    },
    Inline {
        content: String,
        format: ConfigFormat,
    },
    Composite(Vec<ConfigSource>),
}

impl ConfigSource {
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::File {
            path: path.into(),
            format: None,
        }
    }

    pub fn file_with_format(path: impl Into<PathBuf>, format: ConfigFormat) -> Self {
        Self::File {
            path: path.into(),
            format: Some(format),
        }
    }

    pub fn env(prefix: impl Into<String>) -> Self {
        Self::Env {
            prefix: prefix.into(),
            separator: "__".to_string(),
        }
    }

    pub fn env_with_separator(prefix: impl Into<String>, separator: impl Into<String>) -> Self {
        Self::Env {
            prefix: prefix.into(),
            separator: separator.into(),
        }
    }

    pub fn inline(content: impl Into<String>, format: ConfigFormat) -> Self {
        Self::Inline {
            content: content.into(),
            format,
        }
    }

    pub fn composite(sources: impl IntoIterator<Item = ConfigSource>) -> Self {
        Self::Composite(sources.into_iter().collect())
    }

    pub fn load(&self) -> Result<BootstrapConfig, ConfigError> {
        match self {
            Self::File { path, format } => {
                let content = fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
                    path: path.clone(),
                    source,
                })?;
                let format = match format {
                    Some(format) => *format,
                    None => ConfigFormat::from_path(path)?,
                };
                BootstrapConfig::parse(&content, format)
            }
            Self::Env { prefix, separator } => {
                BootstrapConfig::from_env_iter(prefix, separator, env::vars_os())
            }
            Self::Inline { content, format } => BootstrapConfig::parse(content, *format),
            Self::Composite(sources) => {
                let mut merged = BootstrapConfig::default();
                for source in sources {
                    merged.merge(source.load()?);
                }
                Ok(merged)
            }
        }
    }
}
