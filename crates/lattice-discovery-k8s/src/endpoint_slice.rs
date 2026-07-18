use std::{
    collections::BTreeMap,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures_util::{Stream, StreamExt};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::{Client, ResourceExt, api::Api, config::KubeConfigOptions, runtime::watcher};
use lattice_core::{actor_ref::NodeAddress, coordinator::CoordinatorScope};
use lattice_discovery::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
    DiscoverySource, DiscoveryTarget,
};

const SERVICE_LABEL: &str = "kubernetes.io/service-name";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KubernetesCredentials {
    InCluster,
    Kubeconfig { context: Option<String> },
}

#[derive(Debug, Clone)]
pub struct KubernetesEndpointSliceConfig {
    pub scope: CoordinatorScope,
    pub namespace: String,
    pub service: String,
    pub label_selector: Option<String>,
    pub port_name: String,
    pub priority: u16,
    pub credentials: KubernetesCredentials,
}

impl KubernetesEndpointSliceConfig {
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if self.namespace.is_empty() || self.service.is_empty() || self.port_name.is_empty() {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "Kubernetes namespace, service and port name must not be empty"
                    .to_string(),
            });
        }
        if self
            .label_selector
            .as_ref()
            .is_some_and(|selector| selector.trim().is_empty())
        {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "Kubernetes label selector must not be blank".to_string(),
            });
        }
        Ok(())
    }

    fn selector(&self) -> String {
        match &self.label_selector {
            Some(extra) => format!("{SERVICE_LABEL}={},{}", self.service, extra),
            None => format!("{SERVICE_LABEL}={}", self.service),
        }
    }
}

#[derive(Clone)]
pub struct KubernetesEndpointSliceDiscovery {
    config: KubernetesEndpointSliceConfig,
    source: Arc<dyn EndpointSliceSource>,
}

impl Debug for KubernetesEndpointSliceDiscovery {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("KubernetesEndpointSliceDiscovery")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl KubernetesEndpointSliceDiscovery {
    pub async fn connect(config: KubernetesEndpointSliceConfig) -> Result<Self, DiscoveryError> {
        config.validate()?;
        let client_config = match &config.credentials {
            KubernetesCredentials::InCluster => {
                kube::Config::incluster().map_err(kube_config_error)?
            }
            KubernetesCredentials::Kubeconfig { context } => {
                kube::Config::from_kubeconfig(&KubeConfigOptions {
                    context: context.clone(),
                    ..KubeConfigOptions::default()
                })
                .await
                .map_err(kube_config_error)?
            }
        };
        let client = Client::try_from(client_config).map_err(kube_config_error)?;
        let source = KubeEndpointSliceSource {
            client,
            namespace: config.namespace.clone(),
            selector: config.selector(),
        };
        Ok(Self {
            config,
            source: Arc::new(source),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_source(
        config: KubernetesEndpointSliceConfig,
        source: Arc<dyn EndpointSliceSource>,
    ) -> Result<Self, DiscoveryError> {
        config.validate()?;
        Ok(Self { config, source })
    }
}

impl CoordinatorDiscovery for KubernetesEndpointSliceDiscovery {
    fn scope(&self) -> &CoordinatorScope {
        &self.config.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        let scope = self.config.scope.clone();
        Box::pin(async_stream::stream! {
            let mut state = EndpointSliceState::default();
            let mut generation = 0_u64;
            let mut emitted_initial = false;
            loop {
                let mut events = self.source.events();
                while let Some(event) = events.next().await {
                    match event {
                        Err(message) => {
                            if !emitted_initial {
                                generation += 1;
                                emitted_initial = true;
                                yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation, targets: Vec::new() });
                            }
                            yield Err(DiscoveryError::Provider {
                                provider: "kubernetes_endpoint_slice",
                                message,
                            });
                        }
                        Ok(event) => match state.apply(event) {
                            Err(error) => yield Err(error),
                            Ok(false) => {}
                            Ok(true) => match state.targets(&self.config) {
                                Ok(targets) if !targets.is_empty() || !emitted_initial => {
                                    generation += 1;
                                    emitted_initial = true;
                                    yield Ok(CoordinatorDirectorySnapshot { scope: scope.clone(), generation, targets });
                                }
                                Ok(_) => {
                                    yield Err(DiscoveryError::Provider {
                                        provider: "kubernetes_endpoint_slice",
                                        message: "empty update retained the last valid snapshot".to_string(),
                                    });
                                }
                                Err(error) => yield Err(error),
                            },
                        },
                    }
                }
                yield Err(DiscoveryError::Provider {
                    provider: "kubernetes_endpoint_slice",
                    message: "watch stream ended; reconnecting".to_string(),
                });
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SliceEvent {
    Apply(EndpointSlice),
    Delete(EndpointSlice),
    Init,
    InitApply(EndpointSlice),
    InitDone,
}

pub(crate) trait EndpointSliceSource: Send + Sync {
    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<SliceEvent, String>> + Send + '_>>;
}

struct KubeEndpointSliceSource {
    client: Client,
    namespace: String,
    selector: String,
}

impl EndpointSliceSource for KubeEndpointSliceSource {
    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<SliceEvent, String>> + Send + '_>> {
        let api = Api::<EndpointSlice>::namespaced(self.client.clone(), &self.namespace);
        let config = watcher::Config::default().labels(&self.selector);
        Box::pin(watcher::watcher(api, config).map(|event| {
            event
                .map(|event| match event {
                    watcher::Event::Apply(slice) => SliceEvent::Apply(slice),
                    watcher::Event::Delete(slice) => SliceEvent::Delete(slice),
                    watcher::Event::Init => SliceEvent::Init,
                    watcher::Event::InitApply(slice) => SliceEvent::InitApply(slice),
                    watcher::Event::InitDone => SliceEvent::InitDone,
                })
                .map_err(|error| error.to_string())
        }))
    }
}

#[derive(Debug, Default)]
struct EndpointSliceState {
    live: BTreeMap<String, EndpointSlice>,
    staging: Option<BTreeMap<String, EndpointSlice>>,
}

impl EndpointSliceState {
    fn apply(&mut self, event: SliceEvent) -> Result<bool, DiscoveryError> {
        match event {
            SliceEvent::Init => {
                self.staging = Some(BTreeMap::new());
                Ok(false)
            }
            SliceEvent::InitApply(slice) => {
                let name = slice_name(&slice)?;
                let Some(staging) = self.staging.as_mut() else {
                    return Err(DiscoveryError::Provider {
                        provider: "kubernetes_endpoint_slice",
                        message: "InitApply arrived outside a relist".to_string(),
                    });
                };
                staging.insert(name, slice);
                Ok(false)
            }
            SliceEvent::InitDone => {
                let Some(staging) = self.staging.take() else {
                    return Err(DiscoveryError::Provider {
                        provider: "kubernetes_endpoint_slice",
                        message: "InitDone arrived without Init".to_string(),
                    });
                };
                self.live = staging;
                Ok(true)
            }
            SliceEvent::Apply(slice) => {
                self.live.insert(slice_name(&slice)?, slice);
                Ok(true)
            }
            SliceEvent::Delete(slice) => {
                self.live.remove(&slice_name(&slice)?);
                Ok(true)
            }
        }
    }

    fn targets(
        &self,
        config: &KubernetesEndpointSliceConfig,
    ) -> Result<Vec<DiscoveryTarget>, DiscoveryError> {
        let mut targets = BTreeMap::<NodeAddress, DiscoveryTarget>::new();
        for slice in self.live.values() {
            if slice
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(SERVICE_LABEL))
                != Some(&config.service)
            {
                continue;
            }
            if !matches!(slice.address_type.as_str(), "IPv4" | "IPv6" | "FQDN") {
                continue;
            }
            let Some(port) = selected_port(slice, &config.port_name) else {
                continue;
            };
            for endpoint in &slice.endpoints {
                let ready = endpoint.conditions.as_ref().and_then(|value| value.ready);
                let terminating = endpoint
                    .conditions
                    .as_ref()
                    .and_then(|value| value.terminating);
                if ready == Some(false) || terminating == Some(true) {
                    continue;
                }
                for host in &endpoint.addresses {
                    let address = NodeAddress::new(host.clone(), port).map_err(|error| {
                        DiscoveryError::Provider {
                            provider: "kubernetes_endpoint_slice",
                            message: format!("invalid EndpointSlice address: {error}"),
                        }
                    })?;
                    let source =
                        DiscoverySource::single(DiscoveryOrigin::KubernetesEndpointSlice {
                            namespace: config.namespace.clone(),
                            service: config.service.clone(),
                        });
                    targets.entry(address.clone()).or_insert(DiscoveryTarget {
                        address,
                        expected_node_id: None,
                        source,
                        priority: config.priority,
                    });
                }
            }
        }
        Ok(targets.into_values().collect())
    }
}

fn slice_name(slice: &EndpointSlice) -> Result<String, DiscoveryError> {
    let name = slice.name_any();
    if name.is_empty() {
        return Err(DiscoveryError::Provider {
            provider: "kubernetes_endpoint_slice",
            message: "EndpointSlice has no metadata.name".to_string(),
        });
    }
    Ok(name)
}

fn selected_port(slice: &EndpointSlice, port_name: &str) -> Option<u16> {
    slice.ports.as_ref()?.iter().find_map(|port| {
        if port.name.as_deref() != Some(port_name)
            || port.protocol.as_deref().unwrap_or("TCP") != "TCP"
        {
            return None;
        }
        u16::try_from(port.port?).ok().filter(|port| *port != 0)
    })
}

fn kube_config_error(error: impl Display) -> DiscoveryError {
    DiscoveryError::InvalidConfiguration {
        message: format!("cannot initialize Kubernetes client: {error}"),
    }
}
