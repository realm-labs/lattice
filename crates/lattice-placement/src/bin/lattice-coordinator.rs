use std::env;
use std::net::SocketAddr;

use lattice_placement::control::TonicLogicControl;
use lattice_placement::control::{PlacementCoordinatorServer, PlacementCoordinatorService};
use lattice_placement::coordinator::PlacementCoordinator;
use lattice_placement::etcd::{EtcdPlacementStore, EtcdPlacementStoreConfig};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr =
        env_value("LATTICE_COORDINATOR_ADDR", "127.0.0.1:50080").parse::<SocketAddr>()?;
    let key_prefix = env_value("LATTICE_CLUSTER_PREFIX", "/lattice/default");
    let endpoints = env_value("LATTICE_ETCD_ENDPOINTS", "http://127.0.0.1:2379")
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let store = EtcdPlacementStore::from_options(EtcdPlacementStoreConfig {
        key_prefix,
        endpoints,
        instance_lease_ttl_secs: env_i64("LATTICE_INSTANCE_LEASE_TTL_SECS", 30),
        activation_lock_ttl_secs: env_i64("LATTICE_ACTIVATION_LOCK_TTL_SECS", 30),
    })
    .await?;
    let coordinator = PlacementCoordinator::new(store, TonicLogicControl);

    Server::builder()
        .add_service(PlacementCoordinatorServer::new(
            PlacementCoordinatorService::new(coordinator),
        ))
        .serve_with_shutdown(listen_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn env_value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
