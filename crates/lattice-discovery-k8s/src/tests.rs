use std::{collections::BTreeMap, net::Ipv4Addr, pin::Pin, sync::Arc};

use futures_util::{Stream, StreamExt};
use k8s_openapi::{
    api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use lattice_core::coordinator::CoordinatorScope;
use lattice_discovery::provider::{CoordinatorDiscovery, DiscoveryError};

use crate::endpoint_slice::{
    EndpointSliceSource, KubernetesCredentials, KubernetesEndpointSliceConfig,
    KubernetesEndpointSliceDiscovery, SliceEvent,
};

#[tokio::test]
async fn endpoint_slice_filters_conditions_and_applies_add_update_delete() {
    let mut first = endpoint_slice("a", "cluster", "10.0.0.1");
    first
        .endpoints
        .push(endpoint("10.0.0.9", Some(false), None));
    first.endpoints.push(endpoint("10.0.0.8", None, Some(true)));
    let second = endpoint_slice("b", "cluster", "2001:db8::2");
    let updated = endpoint_slice("a", "cluster", "node-a.cluster.local");
    let source = FakeEndpointSliceSource::new(vec![
        Ok(SliceEvent::Init),
        Ok(SliceEvent::InitApply(first)),
        Ok(SliceEvent::InitApply(second.clone())),
        Ok(SliceEvent::InitDone),
        Ok(SliceEvent::Apply(updated)),
        Ok(SliceEvent::Delete(second)),
    ]);
    let discovery =
        KubernetesEndpointSliceDiscovery::with_source(config(), Arc::new(source)).unwrap();
    let snapshots = discovery
        .snapshots()
        .filter_map(|item| async { item.ok() })
        .take(3)
        .collect::<Vec<_>>()
        .await;

    assert_eq!(snapshots[0].targets.len(), 2);
    assert!(
        snapshots[0]
            .targets
            .iter()
            .any(|target| target.address.host() == "2001:db8::2")
    );
    assert_eq!(snapshots[1].targets.len(), 2);
    assert!(
        snapshots[1]
            .targets
            .iter()
            .any(|target| target.address.host() == "node-a.cluster.local")
    );
    assert_eq!(snapshots[2].targets.len(), 1);
}

#[tokio::test]
async fn expired_watch_error_retains_snapshot_then_relist_replaces_it() {
    let source = FakeEndpointSliceSource::new(vec![
        Ok(SliceEvent::Init),
        Ok(SliceEvent::InitApply(endpoint_slice(
            "old", "cluster", "10.0.0.1",
        ))),
        Ok(SliceEvent::InitDone),
        Err("410 Gone: resource version expired".to_string()),
        Ok(SliceEvent::Init),
        Ok(SliceEvent::InitApply(endpoint_slice(
            "new", "cluster", "10.0.0.2",
        ))),
        Ok(SliceEvent::InitDone),
    ]);
    let discovery =
        KubernetesEndpointSliceDiscovery::with_source(config(), Arc::new(source)).unwrap();
    let mut snapshots = discovery.snapshots();

    let initial = snapshots.next().await.unwrap().unwrap();
    assert_eq!(initial.targets[0].address.host(), "10.0.0.1");
    assert!(matches!(
        snapshots.next().await.unwrap(),
        Err(DiscoveryError::Provider { .. })
    ));
    let relisted = snapshots.next().await.unwrap().unwrap();
    assert_eq!(relisted.targets[0].address.host(), "10.0.0.2");
}

#[derive(Clone)]
struct FakeEndpointSliceSource {
    events: Vec<Result<SliceEvent, String>>,
}

impl FakeEndpointSliceSource {
    fn new(events: Vec<Result<SliceEvent, String>>) -> Self {
        Self { events }
    }
}

impl EndpointSliceSource for FakeEndpointSliceSource {
    fn events(&self) -> Pin<Box<dyn Stream<Item = Result<SliceEvent, String>> + Send + '_>> {
        Box::pin(futures_util::stream::iter(self.events.clone()))
    }
}

fn config() -> KubernetesEndpointSliceConfig {
    KubernetesEndpointSliceConfig {
        scope: CoordinatorScope::Membership,
        namespace: "lattice".to_string(),
        service: "cluster".to_string(),
        label_selector: Some("app=lattice".to_string()),
        port_name: "remoting".to_string(),
        priority: 40,
        credentials: KubernetesCredentials::InCluster,
    }
}

fn endpoint_slice(name: &str, service: &str, address: &str) -> EndpointSlice {
    EndpointSlice {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(BTreeMap::from([(
                "kubernetes.io/service-name".to_string(),
                service.to_string(),
            )])),
            ..ObjectMeta::default()
        },
        address_type: if address.contains(':') {
            "IPv6".to_string()
        } else if address.parse::<Ipv4Addr>().is_ok() {
            "IPv4".to_string()
        } else {
            "FQDN".to_string()
        },
        endpoints: vec![endpoint(address, Some(true), Some(false))],
        ports: Some(vec![EndpointPort {
            name: Some("remoting".to_string()),
            port: Some(7447),
            protocol: Some("TCP".to_string()),
            ..EndpointPort::default()
        }]),
    }
}

fn endpoint(address: &str, ready: Option<bool>, terminating: Option<bool>) -> Endpoint {
    Endpoint {
        addresses: vec![address.to_string()],
        conditions: Some(EndpointConditions {
            ready,
            terminating,
            ..EndpointConditions::default()
        }),
        ..Endpoint::default()
    }
}
