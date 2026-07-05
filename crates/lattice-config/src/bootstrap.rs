use std::ffi::OsString;

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::{ConfigError, ConfigFormat};

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

    pub(crate) fn from_env_iter<I, K, V>(
        prefix: &str,
        separator: &str,
        values: I,
    ) -> Result<Self, ConfigError>
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
