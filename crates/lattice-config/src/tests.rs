use std::fs;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::watch;

use crate::bootstrap::BootstrapConfig;
use crate::error::ConfigError;
use crate::format::ConfigFormat;
use crate::source::ConfigSource;
use crate::store::{ConfigStore, ConfigStoreError, ConfigWatch, LocalConfigStore};

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
fn env_source_reports_scalar_object_path_collisions() {
    let error = BootstrapConfig::from_env_iter(
        "LATTICE",
        "__",
        BTreeMap::from([
            ("LATTICE__SERVICE", "world"),
            ("LATTICE__SERVICE__WORKERS", "12"),
        ]),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ConfigError::EnvPathCollision { path } if path == "service"
    ));

    let error = BootstrapConfig::from_env_iter(
        "LATTICE",
        "__",
        vec![
            ("LATTICE__SERVICE__WORKERS", "12"),
            ("LATTICE__SERVICE", "world"),
        ],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ConfigError::EnvPathCollision { path } if path == "service"
    ));
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
