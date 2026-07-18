use std::{fmt::Display, pin::Pin, time::Duration};

use futures_util::Stream;
use lattice_config::store::ConfigStore;
use lattice_core::{actor_ref::NodeAddress, coordinator::CoordinatorScope};
use serde::Deserialize;

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
    DiscoverySource, DiscoveryTarget,
};

const CONFIG_STORE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct ConfigStoreDiscovery<S> {
    scope: CoordinatorScope,
    store: S,
    key: String,
    reconnect_delay: Duration,
}

impl<S> ConfigStoreDiscovery<S>
where
    S: ConfigStore,
{
    pub fn new(
        scope: CoordinatorScope,
        store: S,
        key: impl Into<String>,
    ) -> Result<Self, DiscoveryError> {
        Self::with_reconnect_delay(scope, store, key, Duration::from_millis(250))
    }

    pub fn with_reconnect_delay(
        scope: CoordinatorScope,
        store: S,
        key: impl Into<String>,
        reconnect_delay: Duration,
    ) -> Result<Self, DiscoveryError> {
        let key = key.into();
        if key.is_empty() || reconnect_delay.is_zero() {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "ConfigStore key and reconnect delay must be nonzero".to_string(),
            });
        }
        Ok(Self {
            scope,
            store,
            key,
            reconnect_delay,
        })
    }
}

impl<S> CoordinatorDiscovery for ConfigStoreDiscovery<S>
where
    S: ConfigStore,
{
    fn scope(&self) -> &CoordinatorScope {
        &self.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        let scope = self.scope.clone();
        Box::pin(async_stream::stream! {
            let mut output_generation = 0_u64;
            let mut document_generation = 0_u64;
            let mut last_value = None;
            let mut emitted_initial = false;

            loop {
                let fetched = match self.store.get(&self.key).await {
                    Ok(value) => value,
                    Err(error) => {
                        if !emitted_initial {
                            output_generation += 1;
                            emitted_initial = true;
                            yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation: output_generation, targets: Vec::new() });
                        }
                        yield Err(provider_error(error));
                        tokio::time::sleep(self.reconnect_delay).await;
                        continue;
                    }
                };

                if !emitted_initial || fetched != last_value {
                    match parse_update(&self.key, fetched.clone(), document_generation, last_value.is_some()) {
                        Ok(Some((next_document_generation, targets))) => {
                            document_generation = next_document_generation;
                            output_generation += 1;
                            emitted_initial = true;
                            last_value = fetched.clone();
                            yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation: output_generation, targets });
                        }
                        Ok(None) if !emitted_initial => {
                            output_generation += 1;
                            emitted_initial = true;
                            yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation: output_generation, targets: Vec::new() });
                        }
                        Ok(None) => {}
                        Err(error) => {
                            if !emitted_initial {
                                output_generation += 1;
                                emitted_initial = true;
                                yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation: output_generation, targets: Vec::new() });
                            }
                            yield Err(error);
                        }
                    }
                }

                let mut watch = match self.store.watch(&self.key).await {
                    Ok(watch) => watch,
                    Err(error) => {
                        yield Err(provider_error(error));
                        tokio::time::sleep(self.reconnect_delay).await;
                        continue;
                    }
                };

                let current = watch.current();
                if current != fetched && current != last_value {
                    match parse_update(&self.key, current.clone(), document_generation, last_value.is_some()) {
                        Ok(Some((next_document_generation, targets))) => {
                            document_generation = next_document_generation;
                            output_generation += 1;
                            last_value = current;
                            yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation: output_generation, targets });
                        }
                        Ok(None) => {}
                        Err(error) => yield Err(error),
                    }
                }

                loop {
                    match watch.changed().await {
                        Ok(value) if value == last_value => {}
                        Ok(value) => match parse_update(&self.key, value.clone(), document_generation, last_value.is_some()) {
                            Ok(Some((next_document_generation, targets))) => {
                                document_generation = next_document_generation;
                                output_generation += 1;
                                last_value = value;
                                yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation: output_generation, targets });
                            }
                            Ok(None) => {}
                            Err(error) => yield Err(error),
                        },
                        Err(error) => {
                            yield Err(provider_error(error));
                            break;
                        }
                    }
                }
                tokio::time::sleep(self.reconnect_delay).await;
            }
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointDocument {
    schema_version: u32,
    generation: u64,
    endpoints: Vec<EndpointEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointEntry {
    host: String,
    port: u16,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    priority: u16,
}

fn parse_update(
    key: &str,
    value: Option<serde_json::Value>,
    previous_generation: u64,
    has_previous: bool,
) -> Result<Option<(u64, Vec<DiscoveryTarget>)>, DiscoveryError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let document: EndpointDocument =
        serde_json::from_value(value).map_err(|error| DiscoveryError::Provider {
            provider: "config_store",
            message: format!("malformed endpoint document: {error}"),
        })?;
    if document.schema_version != CONFIG_STORE_SCHEMA_VERSION {
        return Err(DiscoveryError::Provider {
            provider: "config_store",
            message: format!("unsupported schema version {}", document.schema_version),
        });
    }
    if document.generation == 0 || document.generation <= previous_generation {
        return Err(DiscoveryError::Provider {
            provider: "config_store",
            message: format!(
                "document generation {} does not follow {}",
                document.generation, previous_generation
            ),
        });
    }
    if has_previous && document.endpoints.is_empty() {
        return Err(DiscoveryError::Provider {
            provider: "config_store",
            message: "empty update retained the last valid snapshot".to_string(),
        });
    }

    let mut targets = Vec::with_capacity(document.endpoints.len());
    for endpoint in document.endpoints {
        let address = NodeAddress::new(endpoint.host, endpoint.port).map_err(|error| {
            DiscoveryError::Provider {
                provider: "config_store",
                message: format!("invalid endpoint: {error}"),
            }
        })?;
        if endpoint.node_id.as_ref().is_some_and(String::is_empty) {
            return Err(DiscoveryError::Provider {
                provider: "config_store",
                message: format!("endpoint {address} has an empty node ID"),
            });
        }
        targets.push(DiscoveryTarget {
            address,
            expected_node_id: endpoint.node_id,
            source: DiscoverySource::single(DiscoveryOrigin::ConfigStore {
                key: key.to_string(),
            }),
            priority: endpoint.priority,
        });
    }
    targets.sort_by(|left, right| left.address.cmp(&right.address));
    if targets
        .windows(2)
        .any(|pair| pair[0].address == pair[1].address)
    {
        return Err(DiscoveryError::Provider {
            provider: "config_store",
            message: "endpoint document contains duplicate addresses".to_string(),
        });
    }
    Ok(Some((document.generation, targets)))
}

fn provider_error(error: impl Display) -> DiscoveryError {
    DiscoveryError::Provider {
        provider: "config_store",
        message: error.to_string(),
    }
}
