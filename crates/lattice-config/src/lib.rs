use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use thiserror::Error;

mod store;

pub use store::{ConfigStore, ConfigStoreError, ConfigWatch, LocalConfigStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    Toml,
    Yaml,
    Json,
}

impl ConfigFormat {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        match path
            .as_ref()
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("toml") => Ok(Self::Toml),
            Some("yaml" | "yml") => Ok(Self::Yaml),
            Some("json") => Ok(Self::Json),
            _ => Err(ConfigError::UnknownFormat {
                path: path.as_ref().to_path_buf(),
            }),
        }
    }
}

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

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BootstrapConfig {
    root: Map<String, Value>,
}

impl BootstrapConfig {
    pub fn parse(content: &str, format: ConfigFormat) -> Result<Self, ConfigError> {
        let value =
            match format {
                ConfigFormat::Toml => {
                    let value: toml::Value = toml::from_str(content)
                        .map_err(|source| ConfigError::ParseToml { source })?;
                    serde_json::to_value(value).map_err(ConfigError::SerializeConfig)?
                }
                ConfigFormat::Yaml => serde_yaml::from_str(content)
                    .map_err(|source| ConfigError::ParseYaml { source })?,
                ConfigFormat::Json => serde_json::from_str(content)
                    .map_err(|source| ConfigError::ParseJson { source })?,
            };

        Self::from_value(value)
    }

    pub fn section<T>(&self, path: &str) -> Result<T, ConfigError>
    where
        T: DeserializeOwned,
    {
        let value = self
            .get_path(path)
            .ok_or_else(|| ConfigError::MissingSection {
                path: path.to_string(),
            })?;
        serde_json::from_value(value.clone()).map_err(|source| ConfigError::DecodeSection {
            path: path.to_string(),
            source,
        })
    }

    pub fn get_path(&self, path: &str) -> Option<&Value> {
        let mut segments = path.split('.').filter(|segment| !segment.is_empty());
        let first = segments.next()?;
        let mut current = self.root.get(first)?;

        for segment in segments {
            current = current.get(segment)?;
        }

        Some(current)
    }

    pub fn merge(&mut self, other: BootstrapConfig) {
        merge_object(&mut self.root, other.root);
    }

    fn from_value(value: Value) -> Result<Self, ConfigError> {
        match value {
            Value::Object(root) => Ok(Self { root }),
            _ => Err(ConfigError::RootMustBeObject),
        }
    }

    fn from_env_iter<I, K, V>(prefix: &str, separator: &str, values: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<OsString>,
        V: Into<OsString>,
    {
        if separator.is_empty() {
            return Err(ConfigError::EmptyEnvSeparator);
        }

        let normalized_prefix = prefix.to_ascii_uppercase();
        let mut root = Map::new();

        for (key, value) in values {
            let Some(key) = key.into().into_string().ok() else {
                continue;
            };
            let Some(value) = value.into().into_string().ok() else {
                continue;
            };

            let Some(stripped) = key.strip_prefix(&normalized_prefix) else {
                continue;
            };
            let Some(path) = stripped.strip_prefix(separator) else {
                continue;
            };

            let segments = path
                .split(separator)
                .filter(|segment| !segment.is_empty())
                .map(normalize_env_segment)
                .collect::<Vec<_>>();

            if segments.is_empty() {
                continue;
            }

            insert_path(&mut root, &segments, parse_env_value(&value));
        }

        Ok(Self { root })
    }
}

fn merge_object(target: &mut Map<String, Value>, source: Map<String, Value>) {
    for (key, source_value) in source {
        match (target.get_mut(&key), source_value) {
            (Some(Value::Object(target_child)), Value::Object(source_child)) => {
                merge_object(target_child, source_child);
            }
            (_, source_value) => {
                target.insert(key, source_value);
            }
        }
    }
}

fn insert_path(root: &mut Map<String, Value>, segments: &[String], value: Value) {
    let Some((leaf, parents)) = segments.split_last() else {
        return;
    };

    let mut current = root;
    for segment in parents {
        current = current
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("env config path collision with scalar value");
    }
    current.insert(leaf.clone(), value);
}

fn parse_env_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
}

fn normalize_env_segment(value: &str) -> String {
    value.to_ascii_lowercase()
}

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

#[cfg(test)]
mod store_tests {
    use async_trait::async_trait;
    use serde_json::json;
    use tokio::sync::watch;

    use super::*;

    #[tokio::test]
    async fn local_config_store_supports_watch_reload() {
        let store = LocalConfigStore::default();
        let mut watch = store.watch("world.tick_ms").await.unwrap();

        store
            .put("world.tick_ms".to_string(), json!(50))
            .await
            .unwrap();
        let value = watch.changed().await.unwrap();

        assert_eq!(value, Some(json!(50)));
        assert_eq!(store.get("world.tick_ms").await.unwrap(), Some(json!(50)));
    }

    #[tokio::test]
    async fn custom_store_can_build_config_watch_from_channel() {
        #[derive(Clone)]
        struct CustomStore {
            tx: watch::Sender<Option<serde_json::Value>>,
        }

        #[async_trait]
        impl ConfigStore for CustomStore {
            async fn get(&self, _key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
                Ok(self.tx.borrow().clone())
            }

            async fn put(
                &self,
                _key: String,
                value: serde_json::Value,
            ) -> Result<(), ConfigStoreError> {
                self.tx.send_replace(Some(value));
                Ok(())
            }

            async fn watch(&self, _key: &str) -> Result<ConfigWatch, ConfigStoreError> {
                Ok(ConfigWatch::from_receiver(self.tx.subscribe()))
            }
        }

        let (tx, mut watch) = ConfigWatch::channel(Some(json!(10)));
        let store = CustomStore { tx };

        store
            .put("world.tick_ms".to_string(), json!(20))
            .await
            .unwrap();

        assert_eq!(watch.changed().await.unwrap(), Some(json!(20)));
        assert_eq!(store.get("world.tick_ms").await.unwrap(), Some(json!(20)));
    }

    #[tokio::test]
    async fn unsupported_writes_are_explicit() {
        #[derive(Clone)]
        struct ReadOnlyStore;

        #[async_trait]
        impl ConfigStore for ReadOnlyStore {
            async fn get(&self, _key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
                Ok(None)
            }

            async fn put(
                &self,
                _key: String,
                _value: serde_json::Value,
            ) -> Result<(), ConfigStoreError> {
                Err(ConfigStoreError::UnsupportedOperation {
                    operation: "put",
                    backend: "readonly",
                })
            }

            async fn watch(&self, _key: &str) -> Result<ConfigWatch, ConfigStoreError> {
                Ok(ConfigWatch::channel(None).1)
            }
        }

        let error = ReadOnlyStore
            .put("feature.foo".to_string(), json!(true))
            .await;

        assert!(matches!(
            error,
            Err(ConfigStoreError::UnsupportedOperation {
                operation: "put",
                backend: "readonly"
            })
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct ServiceConfig {
        name: String,
        workers: u16,
        enabled: bool,
    }

    #[test]
    fn parses_toml_yaml_and_json_sections() {
        let toml = BootstrapConfig::parse(
            r#"
            [service]
            name = "world"
            workers = 4
            enabled = true
            "#,
            ConfigFormat::Toml,
        )
        .unwrap();
        let yaml = BootstrapConfig::parse(
            r#"
            service:
              name: world
              workers: 4
              enabled: true
            "#,
            ConfigFormat::Yaml,
        )
        .unwrap();
        let json = BootstrapConfig::parse(
            r#"{ "service": { "name": "world", "workers": 4, "enabled": true } }"#,
            ConfigFormat::Json,
        )
        .unwrap();

        let expected = ServiceConfig {
            name: "world".to_string(),
            workers: 4,
            enabled: true,
        };
        assert_eq!(toml.section::<ServiceConfig>("service").unwrap(), expected);
        assert_eq!(yaml.section::<ServiceConfig>("service").unwrap(), expected);
        assert_eq!(json.section::<ServiceConfig>("service").unwrap(), expected);
    }

    #[test]
    fn composite_sources_merge_later_values_over_earlier_values() {
        let base = ConfigSource::inline(
            r#"
            [service]
            name = "world"
            workers = 4
            enabled = false
            "#,
            ConfigFormat::Toml,
        );
        let override_source = ConfigSource::inline(
            r#"
            service:
              workers: 8
              enabled: true
            "#,
            ConfigFormat::Yaml,
        );

        let config = ConfigSource::composite([base, override_source])
            .load()
            .unwrap();

        assert_eq!(
            config.section::<ServiceConfig>("service").unwrap(),
            ServiceConfig {
                name: "world".to_string(),
                workers: 8,
                enabled: true,
            }
        );
    }

    #[test]
    fn env_source_builds_nested_config_and_parses_json_scalars() {
        let config = BootstrapConfig::from_env_iter(
            "LATTICE",
            "__",
            BTreeMap::from([
                ("LATTICE__SERVICE__NAME", "world"),
                ("LATTICE__SERVICE__WORKERS", "12"),
                ("LATTICE__SERVICE__ENABLED", "true"),
                ("OTHER__SERVICE__ENABLED", "false"),
            ]),
        )
        .unwrap();

        assert_eq!(
            config.section::<ServiceConfig>("service").unwrap(),
            ServiceConfig {
                name: "world".to_string(),
                workers: 12,
                enabled: true,
            }
        );
    }

    #[test]
    fn file_source_detects_format_from_extension() {
        let temp = tempfile::NamedTempFile::with_suffix(".json").unwrap();
        fs::write(
            temp.path(),
            r#"{ "service": { "name": "world", "workers": 2, "enabled": true } }"#,
        )
        .unwrap();

        let config = ConfigSource::file(temp.path()).load().unwrap();

        assert_eq!(
            config.section::<ServiceConfig>("service").unwrap().workers,
            2
        );
    }
}
