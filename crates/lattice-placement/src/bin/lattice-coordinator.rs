use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use lattice_core::instance::InstanceId;
use lattice_placement::control::PlacementCoordinatorService;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::control::proto::placement_coordinator_server::PlacementCoordinatorServer;
use lattice_placement::coordination::actor::PlacementCoordinator;
use lattice_placement::error::PlacementError;
use lattice_placement::storage::etcd::{
    EtcdConnectionOptions, EtcdPasswordAuthentication, EtcdPlacementStore, EtcdPlacementStoreConfig,
};
use lattice_placement::storage::{CoordinatorLeadership, PlacementStore};
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
    let store = EtcdPlacementStore::connect_with_connection_options(
        EtcdPlacementStoreConfig {
            key_prefix: key_prefix.clone(),
            endpoints,
            instance_lease_ttl_secs: env_i64("LATTICE_INSTANCE_LEASE_TTL_SECS", 30),
            activation_lock_ttl_secs: env_i64("LATTICE_ACTIVATION_LOCK_TTL_SECS", 30),
        },
        etcd_connection_options_from_env()?,
    )
    .await?;
    let leadership = campaign_until_leader(
        store.clone(),
        candidate_id.clone(),
        Duration::from_secs(env_u64("LATTICE_COORDINATOR_CAMPAIGN_RETRY_SECS", 5)),
    )
    .await?;
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

async fn campaign_until_leader<S>(
    store: S,
    candidate_id: InstanceId,
    retry_interval: Duration,
) -> Result<CoordinatorLeadership, PlacementError>
where
    S: PlacementStore,
{
    loop {
        if let Some(leadership) = store
            .campaign_coordinator_leader(candidate_id.clone())
            .await?
        {
            return Ok(leadership);
        }
        tokio::time::sleep(retry_interval).await;
    }
}

async fn keepalive_loop<S>(
    store: S,
    leadership: CoordinatorLeadership,
) -> Result<(), PlacementError>
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

fn etcd_connection_options_from_env() -> Result<EtcdConnectionOptions, PlacementError> {
    etcd_connection_options(
        optional_env("LATTICE_ETCD_USERNAME")?,
        optional_env("LATTICE_ETCD_PASSWORD_FILE")?,
        optional_env("LATTICE_ETCD_CA_FILE")?,
        optional_env("LATTICE_ETCD_TOKEN_REFRESH_INTERVAL_SECS")?,
        optional_env("LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_ETCD")?,
    )
}

fn etcd_connection_options(
    username: Option<String>,
    password_file: Option<String>,
    ca_file: Option<String>,
    token_refresh_interval_secs: Option<String>,
    dangerously_allow_unauthenticated: Option<String>,
) -> Result<EtcdConnectionOptions, PlacementError> {
    let mut options = match (
        username,
        password_file,
        dangerously_allow_unauthenticated.as_deref(),
    ) {
        (None, None, Some("true")) => Ok(EtcdConnectionOptions::dangerously_unauthenticated()),
        (Some(username), Some(password_file), None) => Ok(EtcdConnectionOptions::password_file(
            EtcdPasswordAuthentication::new(username, password_file),
        )),
        _ => Err(PlacementError::InvalidEtcdAuthentication),
    }?;
    if !options.is_authenticated() && (ca_file.is_some() || token_refresh_interval_secs.is_some()) {
        return Err(PlacementError::InvalidEtcdAuthentication);
    }
    if let Some(ca_file) = ca_file {
        options = options.with_ca_file(ca_file);
    }
    if let Some(interval) = token_refresh_interval_secs {
        let seconds = interval
            .parse::<u64>()
            .map_err(|_| PlacementError::InvalidEtcdAuthentication)?;
        options = options.with_token_refresh_interval(Duration::from_secs(seconds));
    }
    Ok(options)
}

fn optional_env(name: &str) -> Result<Option<String>, PlacementError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(PlacementError::InvalidEtcdAuthentication),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattice_placement::storage::PlacementPrefix;
    use lattice_placement::storage::memory::InMemoryPlacementStore;

    #[tokio::test]
    async fn campaign_until_leader_waits_and_recampaigns_as_standby() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/coordinator-bin"));
        let first = store
            .campaign_coordinator_leader(InstanceId::new("coordinator-a"))
            .await
            .unwrap()
            .unwrap();
        let release_store = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            release_store
                .resign_coordinator_leader(&first)
                .await
                .unwrap();
        });

        let leadership = tokio::time::timeout(
            Duration::from_secs(1),
            campaign_until_leader(
                store,
                InstanceId::new("coordinator-b"),
                Duration::from_millis(1),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(leadership.candidate_id, InstanceId::new("coordinator-b"));
    }

    #[test]
    fn coordinator_etcd_credentials_require_both_environment_values() {
        let options = |username, password_file, dangerous| {
            etcd_connection_options(username, password_file, None, None, dangerous)
        };
        assert_eq!(
            options(None, None, None).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert!(
            !options(None, None, Some("true".to_string()))
                .unwrap()
                .is_authenticated()
        );
        assert!(
            options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                None,
            )
            .unwrap()
            .is_authenticated()
        );
        assert_eq!(
            options(Some("authority".to_string()), None, None).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            options(None, Some("/run/secrets/etcd-password".to_string()), None,).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            options(None, None, Some("false".to_string())).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                Some("true".to_string()),
            )
            .unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert!(
            etcd_connection_options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                Some("/run/secrets/etcd-ca.pem".to_string()),
                Some("15".to_string()),
                None,
            )
            .unwrap()
            .is_authenticated()
        );
        assert_eq!(
            etcd_connection_options(
                None,
                None,
                Some("/run/secrets/etcd-ca.pem".to_string()),
                None,
                Some("true".to_string()),
            )
            .unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            etcd_connection_options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                None,
                Some("not-a-duration".to_string()),
                None,
            )
            .unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
    }
}
