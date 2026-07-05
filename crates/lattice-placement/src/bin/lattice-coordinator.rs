use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use lattice_core::InstanceId;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::control::{PlacementCoordinatorServer, PlacementCoordinatorService};
use lattice_placement::coordinator::PlacementCoordinator;
use lattice_placement::etcd::{EtcdPlacementStore, EtcdPlacementStoreConfig};
use lattice_placement::{CoordinatorLeadership, PlacementStore};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr =
        env_value("LATTICE_COORDINATOR_ADDR", "127.0.0.1:50080").parse::<SocketAddr>()?;
    let key_prefix = env_value("LATTICE_CLUSTER_PREFIX", "/lattice/default");
    let candidate_id = InstanceId::new(env_value(
        "LATTICE_COORDINATOR_ID",
        &listen_addr.to_string(),
    ));
    let endpoints = env_value("LATTICE_ETCD_ENDPOINTS", "http://127.0.0.1:2379")
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let store = EtcdPlacementStore::from_options(EtcdPlacementStoreConfig {
        key_prefix: key_prefix.clone(),
        endpoints,
        instance_lease_ttl_secs: env_i64("LATTICE_INSTANCE_LEASE_TTL_SECS", 30),
        activation_lock_ttl_secs: env_i64("LATTICE_ACTIVATION_LOCK_TTL_SECS", 30),
    })
    .await?;
    let leadership = store
        .campaign_coordinator_leader(candidate_id.clone())
        .await?
        .ok_or_else(|| format!("coordinator leader already exists for prefix {key_prefix}"))?;
    let keepalive_store = store.clone();
    let keepalive_leadership = leadership.clone();
    let coordinator = PlacementCoordinator::new(store.clone(), TonicLogicControl);
    let reconciler = coordinator.start_all_service_lease_expiry_reconciler(Duration::from_secs(
        env_u64("LATTICE_LEASE_RECONCILE_INTERVAL_SECS", 5),
    ));
    let keepalive = keepalive_loop(keepalive_store, keepalive_leadership);

    let server = Server::builder()
        .add_service(PlacementCoordinatorServer::new(
            PlacementCoordinatorService::new(coordinator),
        ))
        .serve_with_shutdown(listen_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        });
    tokio::select! {
        result = server => result?,
        result = keepalive => result?,
    }
    reconciler.cancel();
    store.resign_coordinator_leader(&leadership).await?;
    Ok(())
}

async fn keepalive_loop<S>(
    store: S,
    leadership: CoordinatorLeadership,
) -> Result<(), lattice_placement::PlacementError>
where
    S: PlacementStore,
{
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        store.keepalive_coordinator_leader(&leadership).await?;
    }
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

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
