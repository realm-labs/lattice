use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use lattice_core::instance::InstanceId;
use lattice_rpc::types::RouteTarget;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointPoolKey {
    pub instance_id: InstanceId,
    pub advertised_endpoint: String,
}

impl EndpointPoolKey {
    pub fn from_target(target: &RouteTarget) -> Self {
        Self {
            instance_id: target.instance_id.clone(),
            advertised_endpoint: target.advertised_endpoint.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointLease {
    pub key: EndpointPoolKey,
    pub connection_id: u64,
}

#[derive(Debug, Default, Clone)]
pub struct EndpointPool {
    connections: Arc<DashMap<EndpointPoolKey, EndpointLease>>,
    next_connection_id: Arc<AtomicU64>,
}

impl EndpointPool {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            next_connection_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn get_or_connect(&self, target: &RouteTarget) -> EndpointLease {
        let key = EndpointPoolKey::from_target(target);
        match self.connections.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry
                .insert(EndpointLease {
                    key,
                    connection_id: self.next_connection_id.fetch_add(1, Ordering::SeqCst),
                })
                .clone(),
        }
    }
}
