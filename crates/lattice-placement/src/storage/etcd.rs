use std::fmt;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use http::Uri;
use lattice_core::id::ActorId;
use lattice_core::instance::{InstanceId, InstanceIncarnation};
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::service_context::ConfiguredComponent;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::{InstanceRecord, InstanceState};
#[cfg(test)]
use crate::storage::etcd::client::InMemoryEtcdClient;
use crate::storage::etcd::client::{
    ActivationLockTtl, EtcdEpochCommitRequest, EtcdEpochReservationRequest, EtcdKv,
    EtcdLegacyEpochPutRequest, EtcdOwnershipFloorProof, EtcdOwnershipRanges,
    EtcdOwnershipRecordRange, EtcdOwnershipWatchEvent, EtcdOwnershipWatchUpdate, EtcdValueGuard,
    InstanceLeaseTtl, RealEtcdClient,
};
use crate::storage::etcd::codec::{
    EtcdValue, activation_lock_key, activation_lock_namespace_prefix,
    actor_epoch_floor_service_prefix, actor_key, actor_namespace_prefix, actor_service_prefix,
    coordinator_leader_key, default_instance_lease_ttl_secs, epoch_floor_key, instance_key,
    instance_namespace_prefix, instance_service_prefix, logic_prefix,
    singleton_epoch_floor_service_prefix, singleton_key, singleton_lock_key,
    singleton_lock_namespace_prefix, singleton_namespace_prefix, singleton_service_prefix,
    virtual_shard_epoch_floor_service_prefix, vshard_actor_prefix, vshard_key,
    vshard_service_prefix,
};
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, EpochFloorRecord, LeaseId,
    OwnershipEpochFloorProof, OwnershipProofContext, OwnershipProofError, OwnershipRecordBinding,
    OwnershipView, OwnershipViewError, OwnershipViewRecord, OwnershipViewSnapshot, OwnershipWatch,
    OwnershipWatchBatch, OwnershipWatchError, OwnershipWatchEvent, OwnershipWatchMessage,
    OwnershipWatchUpdate, PlacementEpochGuard, PlacementEpochKey, PlacementEpochReservation,
    PlacementPrefix, PlacementStore, PlacementVersion, PlacementWatch, PlacementWatchEvent,
    SingletonKey, SingletonPlacementRecord, VirtualShardPlacementKey, VirtualShardPlacementRecord,
    next_reserved_epoch, validate_epoch_floor_lineage, validate_legacy_epoch,
};

pub mod client;
pub(crate) mod codec;

#[derive(Debug, Clone)]
pub struct EtcdPlacementStore<C> {
    prefix: PlacementPrefix,
    client: C,
}

impl<C> EtcdPlacementStore<C> {
    pub(crate) fn new(prefix: PlacementPrefix, client: C) -> Self {
        Self { prefix, client }
    }
}

const MAX_ETCD_USERNAME_BYTES: usize = 256;
const MAX_ETCD_PASSWORD_BYTES: usize = 1_024;
const MAX_ETCD_CA_BYTES: usize = 1_048_576;
const ETCD_CREDENTIAL_FILE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const MIN_ETCD_TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const MAX_ETCD_TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(240);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdPlacementStoreConfig {
    pub key_prefix: String,
    pub endpoints: Vec<String>,
    #[serde(default = "default_instance_lease_ttl_secs")]
    pub instance_lease_ttl_secs: i64,
    pub activation_lock_ttl_secs: i64,
}

impl fmt::Debug for EtcdPlacementStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdPlacementStoreConfig")
            .field("key_prefix", &self.key_prefix)
            .field("endpoint_count", &self.endpoints.len())
            .field("instance_lease_ttl_secs", &self.instance_lease_ttl_secs)
            .field("activation_lock_ttl_secs", &self.activation_lock_ttl_secs)
            .finish()
    }
}

/// Password authentication for an etcd connection.
///
/// Password bytes are loaded from a bounded absolute-path file at connection
/// time, so they never enter the general bootstrap configuration tree.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdPasswordAuthentication {
    username: String,
    password_file: PathBuf,
}

impl fmt::Debug for EtcdPasswordAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdPasswordAuthentication")
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

impl EtcdPasswordAuthentication {
    pub fn new(username: impl Into<String>, password_file: impl Into<PathBuf>) -> Self {
        Self {
            username: username.into(),
            password_file: password_file.into(),
        }
    }

    async fn into_loaded_credentials(
        self,
        endpoints: &[String],
        allow_plaintext_loopback: bool,
    ) -> Result<LoadedEtcdCredentials, PlacementError> {
        validate_authenticated_endpoints(endpoints, allow_plaintext_loopback)?;
        if self.username.trim().is_empty()
            || self.username.len() > MAX_ETCD_USERNAME_BYTES
            || self.password_file.as_os_str().is_empty()
            || !self.password_file.is_absolute()
        {
            return Err(PlacementError::InvalidEtcdAuthentication);
        }
        let password = read_etcd_password(self.password_file).await?;
        Ok(LoadedEtcdCredentials {
            username: self.username,
            password,
            use_tls_roots: endpoints_use_https(endpoints)?,
            token_refresh_interval: DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL,
            ca_certificate: None,
        })
    }
}

struct LoadedEtcdCredentials {
    username: String,
    password: String,
    use_tls_roots: bool,
    token_refresh_interval: Duration,
    ca_certificate: Option<Vec<u8>>,
}

/// Connection-only settings kept separate from the source-compatible store
/// layout. The deliberately dangerous plaintext switch exists for isolated
/// loopback integration tests and cannot authorize a non-loopback endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct EtcdConnectionOptions {
    authentication: Option<EtcdPasswordAuthentication>,
    dangerously_allow_plaintext_loopback_authentication: bool,
    token_refresh_interval: Duration,
    ca_file: Option<PathBuf>,
}

impl fmt::Debug for EtcdConnectionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdConnectionOptions")
            .field("authentication_configured", &self.authentication.is_some())
            .field(
                "plaintext_loopback_authentication",
                &self.dangerously_allow_plaintext_loopback_authentication,
            )
            .field("token_refresh_interval", &self.token_refresh_interval)
            .field("custom_ca_configured", &self.ca_file.is_some())
            .finish()
    }
}

impl EtcdConnectionOptions {
    pub fn dangerously_unauthenticated() -> Self {
        Self {
            authentication: None,
            dangerously_allow_plaintext_loopback_authentication: false,
            token_refresh_interval: DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL,
            ca_file: None,
        }
    }

    pub fn password_file(authentication: EtcdPasswordAuthentication) -> Self {
        Self {
            authentication: Some(authentication),
            dangerously_allow_plaintext_loopback_authentication: false,
            token_refresh_interval: DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL,
            ca_file: None,
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.authentication.is_some()
    }

    /// Permits password authentication over plaintext only when every etcd
    /// endpoint is an explicit loopback HTTP URL.
    pub fn dangerously_allow_plaintext_loopback_authentication(mut self) -> Self {
        self.dangerously_allow_plaintext_loopback_authentication = true;
        self
    }

    pub fn with_token_refresh_interval(mut self, interval: Duration) -> Self {
        self.token_refresh_interval = interval;
        self
    }

    pub fn with_ca_file(mut self, ca_file: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(ca_file.into());
        self
    }

    async fn into_loaded_credentials(
        self,
        endpoints: &[String],
    ) -> Result<Option<LoadedEtcdCredentials>, PlacementError> {
        validate_no_endpoint_userinfo(endpoints)?;
        if self.authentication.is_none() {
            if self.dangerously_allow_plaintext_loopback_authentication
                || self.token_refresh_interval != DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL
                || self.ca_file.is_some()
            {
                return Err(PlacementError::InvalidEtcdAuthentication);
            }
            validate_dangerously_unauthenticated_endpoints(endpoints)?;
            return Ok(None);
        }
        if self.token_refresh_interval < MIN_ETCD_TOKEN_REFRESH_INTERVAL
            || self.token_refresh_interval > MAX_ETCD_TOKEN_REFRESH_INTERVAL
        {
            return Err(PlacementError::InvalidEtcdAuthentication);
        }
        let ca_certificate = match self.ca_file {
            Some(path) => {
                if !endpoints_use_https(endpoints)? || !path.is_absolute() {
                    return Err(PlacementError::InvalidEtcdAuthentication);
                }
                Some(read_etcd_ca(path).await?)
            }
            None => None,
        };
        match self.authentication {
            Some(authentication) => authentication
                .into_loaded_credentials(
                    endpoints,
                    self.dangerously_allow_plaintext_loopback_authentication,
                )
                .await
                .map(|mut credentials| {
                    credentials.token_refresh_interval = self.token_refresh_interval;
                    credentials.ca_certificate = ca_certificate;
                    Some(credentials)
                }),
            None => unreachable!("unauthenticated options returned before credential loading"),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EtcdConnectionSection {
    authentication: EtcdPasswordAuthentication,
    #[serde(default = "default_etcd_token_refresh_interval_secs")]
    token_refresh_interval_secs: u64,
    #[serde(default)]
    ca_file: Option<PathBuf>,
}

impl From<EtcdConnectionSection> for EtcdConnectionOptions {
    fn from(section: EtcdConnectionSection) -> Self {
        Self {
            authentication: Some(section.authentication),
            dangerously_allow_plaintext_loopback_authentication: false,
            token_refresh_interval: Duration::from_secs(section.token_refresh_interval_secs),
            ca_file: section.ca_file,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EtcdPlacementStoreSection {
    key_prefix: String,
    endpoints: Vec<String>,
    #[serde(default = "default_instance_lease_ttl_secs")]
    instance_lease_ttl_secs: i64,
    activation_lock_ttl_secs: i64,
    connection: EtcdConnectionSection,
}

impl EtcdPlacementStoreSection {
    fn into_parts(self) -> (EtcdPlacementStoreConfig, EtcdConnectionOptions) {
        (
            EtcdPlacementStoreConfig {
                key_prefix: self.key_prefix,
                endpoints: self.endpoints,
                instance_lease_ttl_secs: self.instance_lease_ttl_secs,
                activation_lock_ttl_secs: self.activation_lock_ttl_secs,
            },
            self.connection.into(),
        )
    }
}

impl EtcdPlacementStore<RealEtcdClient> {
    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section(
            "placement_store",
            |section: EtcdPlacementStoreSection| async move {
                let (config, connection) = section.into_parts();
                Self::connect_with_connection_options(config, connection).await
            },
        )
    }

    pub async fn dangerously_connect_unauthenticated(
        config: EtcdPlacementStoreConfig,
    ) -> Result<Self, PlacementError> {
        Self::connect_with_connection_options(
            config,
            EtcdConnectionOptions::dangerously_unauthenticated(),
        )
        .await
    }

    pub async fn connect_with_connection_options(
        config: EtcdPlacementStoreConfig,
        connection: EtcdConnectionOptions,
    ) -> Result<Self, PlacementError> {
        let credentials = connection
            .into_loaded_credentials(&config.endpoints)
            .await?;
        let instance_lease_ttl = InstanceLeaseTtl::new(config.instance_lease_ttl_secs);
        let activation_lock_ttl = ActivationLockTtl::new(config.activation_lock_ttl_secs);
        let client = match credentials {
            Some(credentials) => {
                RealEtcdClient::connect_authenticated(
                    config.endpoints,
                    instance_lease_ttl,
                    activation_lock_ttl,
                    credentials,
                )
                .await?
            }
            None => {
                RealEtcdClient::connect(config.endpoints, instance_lease_ttl, activation_lock_ttl)
                    .await?
            }
        };
        Ok(Self::new(PlacementPrefix::new(config.key_prefix), client))
    }
}

fn validate_no_endpoint_userinfo(endpoints: &[String]) -> Result<(), PlacementError> {
    if endpoints.iter().any(|endpoint| {
        endpoint
            .split_once("://")
            .map(|(_, remainder)| {
                remainder
                    .split('/')
                    .next()
                    .unwrap_or_default()
                    .contains('@')
            })
            .unwrap_or(false)
    }) {
        return Err(PlacementError::EtcdEndpointUserinfoUnsupported);
    }
    Ok(())
}

fn validate_authenticated_endpoints(
    endpoints: &[String],
    allow_plaintext_loopback: bool,
) -> Result<(), PlacementError> {
    validate_no_endpoint_userinfo(endpoints)?;
    if endpoints.is_empty() {
        return Err(PlacementError::InvalidEtcdEndpoint);
    }
    let https = endpoints_use_https(endpoints)?;
    if https {
        return Ok(());
    }
    if !allow_plaintext_loopback {
        return Err(PlacementError::InsecureEtcdAuthenticationTransport);
    }
    for endpoint in endpoints {
        let uri = endpoint
            .parse::<Uri>()
            .map_err(|_| PlacementError::InvalidEtcdEndpoint)?;
        if uri.scheme_str() != Some("http") || !is_loopback_uri(&uri) {
            return Err(PlacementError::InsecureEtcdAuthenticationTransport);
        }
    }
    Ok(())
}

fn validate_dangerously_unauthenticated_endpoints(
    endpoints: &[String],
) -> Result<(), PlacementError> {
    if endpoints.is_empty() {
        return Err(PlacementError::InvalidEtcdEndpoint);
    }
    for endpoint in endpoints {
        let uri = endpoint
            .parse::<Uri>()
            .map_err(|_| PlacementError::InvalidEtcdEndpoint)?;
        if uri.scheme_str() != Some("http") || !is_loopback_uri(&uri) {
            return Err(PlacementError::InsecureEtcdUnauthenticatedTransport);
        }
    }
    Ok(())
}

fn endpoints_use_https(endpoints: &[String]) -> Result<bool, PlacementError> {
    let mut expected_https = None;
    for endpoint in endpoints {
        let uri = endpoint
            .parse::<Uri>()
            .map_err(|_| PlacementError::InvalidEtcdEndpoint)?;
        let scheme = uri
            .scheme_str()
            .ok_or(PlacementError::InvalidEtcdEndpoint)?;
        let is_https = match scheme {
            "http" => false,
            "https" => true,
            _ => return Err(PlacementError::InvalidEtcdEndpoint),
        };
        match expected_https {
            Some(expected) if expected != is_https => {
                return Err(PlacementError::InsecureEtcdAuthenticationTransport);
            }
            None => expected_https = Some(is_https),
            _ => {}
        }
    }
    Ok(expected_https == Some(true))
}

fn is_loopback_uri(uri: &Uri) -> bool {
    let Some(host) = uri.host() else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

async fn read_etcd_password(path: PathBuf) -> Result<String, PlacementError> {
    let result = tokio::time::timeout(ETCD_CREDENTIAL_FILE_TIMEOUT, async move {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(etcd_password_file_error)?;
        let metadata = file.metadata().await.map_err(etcd_password_file_error)?;
        if !metadata.is_file() {
            return Err(PlacementError::InvalidEtcdAuthentication);
        }
        let max_file_bytes = MAX_ETCD_PASSWORD_BYTES + 2;
        let mut bytes = Vec::with_capacity(max_file_bytes.min(256));
        file.take((max_file_bytes + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(etcd_password_file_error)?;
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
        }
        if bytes.is_empty() || bytes.len() > MAX_ETCD_PASSWORD_BYTES || bytes.contains(&b'\0') {
            return Err(PlacementError::InvalidEtcdAuthentication);
        }
        String::from_utf8(bytes).map_err(|_| PlacementError::InvalidEtcdAuthentication)
    })
    .await;
    result.unwrap_or(Err(PlacementError::EtcdPasswordFile {
        kind: std::io::ErrorKind::TimedOut,
    }))
}

async fn read_etcd_ca(path: PathBuf) -> Result<Vec<u8>, PlacementError> {
    let result = tokio::time::timeout(ETCD_CREDENTIAL_FILE_TIMEOUT, async move {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(etcd_tls_ca_file_error)?;
        let metadata = file.metadata().await.map_err(etcd_tls_ca_file_error)?;
        if !metadata.is_file() {
            return Err(PlacementError::InvalidEtcdAuthentication);
        }
        let mut bytes = Vec::with_capacity(MAX_ETCD_CA_BYTES.min(4_096));
        file.take((MAX_ETCD_CA_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(etcd_tls_ca_file_error)?;
        if bytes.is_empty() || bytes.len() > MAX_ETCD_CA_BYTES {
            return Err(PlacementError::InvalidEtcdAuthentication);
        }
        Ok(bytes)
    })
    .await;
    result.unwrap_or(Err(PlacementError::EtcdTlsCaFile {
        kind: std::io::ErrorKind::TimedOut,
    }))
}

fn etcd_password_file_error(error: std::io::Error) -> PlacementError {
    PlacementError::EtcdPasswordFile { kind: error.kind() }
}

fn etcd_tls_ca_file_error(error: std::io::Error) -> PlacementError {
    PlacementError::EtcdTlsCaFile { kind: error.kind() }
}

const fn default_etcd_token_refresh_interval_secs() -> u64 {
    DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL.as_secs()
}

#[cfg(test)]
impl EtcdPlacementStore<InMemoryEtcdClient> {
    pub(crate) fn in_memory_from_config(config: EtcdPlacementStoreConfig) -> Self {
        Self::new(
            PlacementPrefix::new(config.key_prefix),
            InMemoryEtcdClient::new(),
        )
    }
}

// The raw transport bound is intentionally private: callers receive the typed
// PlacementStore API but cannot import the arbitrary-key put/delete surface.
#[allow(private_bounds)]
impl<C> EtcdPlacementStore<C>
where
    C: EtcdKv,
{
    async fn get_epoch_floor(
        &self,
        key: &PlacementEpochKey,
    ) -> Result<Option<(PlacementVersion, EpochFloorRecord)>, PlacementError> {
        validate_placement_epoch_key(key)?;
        let storage_key = epoch_floor_key(&self.prefix, key);
        let Some((token, value)) = self.client.get(&storage_key).await? else {
            return Ok(None);
        };
        validate_etcd_value_key(&self.prefix, &storage_key, &value)
            .map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::EpochFloor(record) => Ok(Some((token, *record))),
            _ => Err(unexpected_etcd_value("epoch floor", &storage_key)),
        }
    }

    fn floor_compare(
        floor: &Option<(PlacementVersion, EpochFloorRecord)>,
    ) -> Option<(PlacementVersion, EtcdValue)> {
        floor
            .as_ref()
            .map(|(token, record)| (*token, EtcdValue::EpochFloor(Box::new(record.clone()))))
    }

    fn floor_value(key: PlacementEpochKey, epoch: lattice_core::actor_ref::Epoch) -> EtcdValue {
        EtcdValue::EpochFloor(Box::new(EpochFloorRecord { key, epoch }))
    }
}

#[async_trait]
#[allow(private_bounds)]
impl<C> PlacementStore for EtcdPlacementStore<C>
where
    C: EtcdKv,
{
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        self.client.grant_instance_lease().await
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        self.client.keepalive_instance_lease(lease_id).await
    }

    async fn campaign_coordinator_leader(
        &self,
        candidate_id: InstanceId,
    ) -> Result<Option<CoordinatorLeadership>, PlacementError> {
        validate_instance_id(&candidate_id)?;
        let lease_id = self.client.grant_instance_lease().await?;
        let leadership = CoordinatorLeadership {
            candidate_id,
            lease_id,
        };
        match self
            .client
            .compare_and_put(
                coordinator_leader_key(&self.prefix),
                None,
                EtcdValue::CoordinatorLeader(Box::new(leadership.clone())),
            )
            .await
        {
            Ok(_) => Ok(Some(leadership)),
            Err(PlacementError::CompareAndPutFailed) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn keepalive_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError> {
        validate_instance_id(&leadership.candidate_id)?;
        let Some((_, EtcdValue::CoordinatorLeader(current))) = self
            .client
            .get(&coordinator_leader_key(&self.prefix))
            .await?
        else {
            return Err(PlacementError::CoordinatorLeadershipLost);
        };
        validate_instance_id(&current.candidate_id)?;
        if current.as_ref() != leadership {
            return Err(PlacementError::CoordinatorLeadershipLost);
        }
        self.client
            .keepalive_instance_lease(leadership.lease_id)
            .await
    }

    async fn resign_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError> {
        validate_instance_id(&leadership.candidate_id)?;
        let Some((_, EtcdValue::CoordinatorLeader(current))) = self
            .client
            .get(&coordinator_leader_key(&self.prefix))
            .await?
        else {
            return Ok(());
        };
        validate_instance_id(&current.candidate_id)?;
        if current.as_ref() == leadership {
            self.client
                .delete(&coordinator_leader_key(&self.prefix))
                .await?;
        }
        Ok(())
    }

    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        validate_instance_record(&record)?;
        self.client
            .put(
                instance_key(&self.prefix, &record.service_kind, &record.instance_id),
                EtcdValue::Instance(Box::new(record)),
            )
            .await
    }

    async fn compare_and_set_instance_state(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        expected_incarnation: &InstanceIncarnation,
        expected_lease_id: LeaseId,
        state: InstanceState,
    ) -> Result<InstanceRecord, PlacementError> {
        validate_service_kind(service_kind)?;
        validate_instance_id(instance_id)?;
        let key = instance_key(&self.prefix, service_kind, instance_id);
        let Some((version, value)) = self.client.get(&key).await? else {
            return Err(PlacementError::InstanceNotFound {
                instance_id: instance_id.clone(),
            });
        };
        validate_etcd_value_key(&self.prefix, &key, &value)
            .map_err(placement_key_validation_error)?;
        let EtcdValue::Instance(record) = value else {
            return Err(PlacementError::InstanceNotFound {
                instance_id: instance_id.clone(),
            });
        };
        let mut record = *record;
        if &record.incarnation != expected_incarnation {
            return Err(PlacementError::InstanceIncarnationMismatch {
                instance_id: instance_id.clone(),
                expected: expected_incarnation.clone(),
                actual: record.incarnation,
            });
        }
        if record.lease_id != expected_lease_id {
            return Err(PlacementError::InstanceLeaseMismatch {
                instance_id: instance_id.clone(),
                expected: expected_lease_id,
                actual: record.lease_id,
            });
        }
        record.state = state;
        self.client
            .compare_and_put(
                key,
                Some(version),
                EtcdValue::Instance(Box::new(record.clone())),
            )
            .await?;
        Ok(record)
    }

    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        validate_instance_id(instance_id)?;
        let prefix = instance_namespace_prefix(&self.prefix);
        for (key, _version, value) in self.client.list_prefix(&prefix).await? {
            validate_etcd_value_key(&self.prefix, &key, &value)
                .map_err(placement_key_validation_error)?;
            if let EtcdValue::Instance(record) = value
                && &record.instance_id == instance_id
            {
                return Ok(Some(*record));
            }
        }
        Ok(None)
    }

    async fn get_service_instance(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        validate_service_kind(service_kind)?;
        validate_instance_id(instance_id)?;
        let key = instance_key(&self.prefix, service_kind, instance_id);
        let Some((_version, value)) = self.client.get(&key).await? else {
            return Ok(None);
        };
        validate_etcd_value_key(&self.prefix, &key, &value)
            .map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::Instance(record) => Ok(Some(*record)),
            _ => Err(unexpected_etcd_value("instance", &key)),
        }
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        validate_service_kind(service_kind)?;
        let prefix = instance_service_prefix(&self.prefix, service_kind);
        collect_instances(&self.prefix, self.client.list_prefix(&prefix).await?)
    }

    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError> {
        let prefix = instance_namespace_prefix(&self.prefix);
        collect_instances(&self.prefix, self.client.list_prefix(&prefix).await?)
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        validate_actor_key(key)?;
        let storage_key = actor_key(&self.prefix, key);
        let Some((version, value)) = self.client.get(&storage_key).await? else {
            return Ok(None);
        };
        validate_etcd_value_key(&self.prefix, &storage_key, &value)
            .map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::Actor(record) => Ok(Some((version, *record))),
            _ => Ok(None),
        }
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        let prefix = actor_namespace_prefix(&self.prefix);
        collect_actors(&self.prefix, self.client.list_prefix(&prefix).await?)
    }

    async fn reserve_actor_epoch(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        activation_lock: Option<LeaseId>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        validate_actor_key(&key)?;
        let current = self.get_actor(&key).await?;
        if current.as_ref().map(|(token, _)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let epoch_key = PlacementEpochKey::Actor(key.clone());
        let floor = self.get_epoch_floor(&epoch_key).await?;
        validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, record)| (*token, record.epoch)),
        )?;
        let epoch = next_reserved_epoch(
            current.as_ref().map(|(_, record)| record.epoch),
            floor.as_ref().map(|(_, record)| record.epoch),
        )?;
        let guard = activation_lock.map(|lease_id| EtcdValueGuard {
            key: activation_lock_key(&self.prefix, &key),
            value: EtcdValue::ActivationLock(lease_id),
        });
        let floor_token = self
            .client
            .reserve_epoch(EtcdEpochReservationRequest {
                record_key: actor_key(&self.prefix, &key),
                expected_record: expected,
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                expected_floor: Self::floor_compare(&floor),
                floor_value: Self::floor_value(epoch_key.clone(), epoch),
                guard,
            })
            .await?;
        Ok(PlacementEpochReservation::new(
            epoch_key,
            epoch,
            expected,
            floor_token,
            activation_lock.map(PlacementEpochGuard::Actor),
        ))
    }

    async fn commit_actor_epoch(
        &self,
        reservation: PlacementEpochReservation,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        validate_actor_record(&value)?;
        let PlacementEpochKey::Actor(key) = reservation.key() else {
            return Err(PlacementError::EpochReservationMismatch);
        };
        validate_actor_key(key)?;
        if key.service_kind != value.service_kind
            || key.actor_kind != value.actor_kind
            || key.actor_id != value.actor_id
            || reservation.epoch() != value.epoch
        {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let guard = match reservation.guard() {
            Some(PlacementEpochGuard::Actor(lease_id)) => Some(EtcdValueGuard {
                key: activation_lock_key(&self.prefix, key),
                value: EtcdValue::ActivationLock(lease_id),
            }),
            None => None,
            Some(PlacementEpochGuard::Singleton(_)) => {
                return Err(PlacementError::EpochReservationMismatch);
            }
        };
        let epoch_key = reservation.key().clone();
        self.client
            .commit_epoch(EtcdEpochCommitRequest {
                record_key: actor_key(&self.prefix, key),
                expected_record: reservation.expected_record(),
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                floor_token: reservation.floor_token(),
                floor_value: Self::floor_value(epoch_key, reservation.epoch()),
                record_value: EtcdValue::Actor(Box::new(value)),
                guard,
            })
            .await
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        validate_actor_key_record(&key, &value)?;
        let current = self.get_actor(&key).await?;
        if current.as_ref().map(|(token, _)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let epoch_key = PlacementEpochKey::Actor(key.clone());
        let floor = self.get_epoch_floor(&epoch_key).await?;
        validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, record)| (*token, record.epoch)),
        )?;
        let authority_changed = current.as_ref().is_some_and(|(_, record)| {
            record.owner != value.owner || record.lease_id != value.lease_id
        });
        let reactivating = current.as_ref().is_some_and(|(_, record)| {
            record.state == crate::storage::PlacementState::Stopped
                && value.state != crate::storage::PlacementState::Stopped
        });
        validate_legacy_epoch(
            current.as_ref().map(|(_, record)| record.epoch),
            floor.as_ref().map(|(_, record)| record.epoch),
            value.epoch,
            authority_changed,
            reactivating,
        )?;
        self.client
            .compare_and_put_epoch(EtcdLegacyEpochPutRequest {
                record_key: actor_key(&self.prefix, &key),
                expected_record: expected,
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                expected_floor: Self::floor_compare(&floor),
                floor_value: Self::floor_value(epoch_key, value.epoch),
                record_value: EtcdValue::Actor(Box::new(value)),
            })
            .await
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        validate_virtual_shard_key(key)?;
        let storage_key = vshard_key(&self.prefix, key);
        let Some((version, value)) = self.client.get(&storage_key).await? else {
            return Ok(None);
        };
        validate_etcd_value_key(&self.prefix, &storage_key, &value)
            .map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::VirtualShard(record) => Ok(Some((version, *record))),
            _ => Ok(None),
        }
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        validate_service_kind(service_kind)?;
        validate_actor_kind(actor_kind)?;
        let prefix = vshard_actor_prefix(&self.prefix, service_kind, actor_kind);
        collect_virtual_shards(&self.prefix, self.client.list_prefix(&prefix).await?)
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        validate_service_kind(service_kind)?;
        let prefix = vshard_service_prefix(&self.prefix, service_kind);
        collect_virtual_shards(&self.prefix, self.client.list_prefix(&prefix).await?)
    }

    async fn reserve_virtual_shard_epoch(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        validate_virtual_shard_key(&key)?;
        let current = self.get_virtual_shard(&key).await?;
        if current.as_ref().map(|(token, _)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let epoch_key = PlacementEpochKey::VirtualShard(key.clone());
        let floor = self.get_epoch_floor(&epoch_key).await?;
        validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, record)| (*token, record.epoch)),
        )?;
        let epoch = next_reserved_epoch(
            current.as_ref().map(|(_, record)| record.epoch),
            floor.as_ref().map(|(_, record)| record.epoch),
        )?;
        let floor_token = self
            .client
            .reserve_epoch(EtcdEpochReservationRequest {
                record_key: vshard_key(&self.prefix, &key),
                expected_record: expected,
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                expected_floor: Self::floor_compare(&floor),
                floor_value: Self::floor_value(epoch_key.clone(), epoch),
                guard: None,
            })
            .await?;
        Ok(PlacementEpochReservation::new(
            epoch_key,
            epoch,
            expected,
            floor_token,
            None,
        ))
    }

    async fn commit_virtual_shard_epoch(
        &self,
        reservation: PlacementEpochReservation,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        validate_virtual_shard_record(&value)?;
        let PlacementEpochKey::VirtualShard(key) = reservation.key() else {
            return Err(PlacementError::EpochReservationMismatch);
        };
        validate_virtual_shard_key(key)?;
        if key.service_kind != value.service_kind
            || key.actor_kind != value.actor_kind
            || key.shard_id != value.shard_id
            || reservation.epoch() != value.epoch
            || reservation.guard().is_some()
        {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let epoch_key = reservation.key().clone();
        self.client
            .commit_epoch(EtcdEpochCommitRequest {
                record_key: vshard_key(&self.prefix, key),
                expected_record: reservation.expected_record(),
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                floor_token: reservation.floor_token(),
                floor_value: Self::floor_value(epoch_key, reservation.epoch()),
                record_value: EtcdValue::VirtualShard(Box::new(value)),
                guard: None,
            })
            .await
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        validate_virtual_shard_key_record(&key, &value)?;
        let current = self.get_virtual_shard(&key).await?;
        if current.as_ref().map(|(token, _)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let epoch_key = PlacementEpochKey::VirtualShard(key.clone());
        let floor = self.get_epoch_floor(&epoch_key).await?;
        validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, record)| (*token, record.epoch)),
        )?;
        let authority_changed = current
            .as_ref()
            .is_some_and(|(_, record)| record.owner != value.owner);
        validate_legacy_epoch(
            current.as_ref().map(|(_, record)| record.epoch),
            floor.as_ref().map(|(_, record)| record.epoch),
            value.epoch,
            authority_changed,
            false,
        )?;
        self.client
            .compare_and_put_epoch(EtcdLegacyEpochPutRequest {
                record_key: vshard_key(&self.prefix, &key),
                expected_record: expected,
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                expected_floor: Self::floor_compare(&floor),
                floor_value: Self::floor_value(epoch_key, value.epoch),
                record_value: EtcdValue::VirtualShard(Box::new(value)),
            })
            .await
    }

    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        validate_singleton_key(key)?;
        let storage_key = singleton_key(&self.prefix, key);
        let Some((version, value)) = self.client.get(&storage_key).await? else {
            return Ok(None);
        };
        validate_etcd_value_key(&self.prefix, &storage_key, &value)
            .map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::Singleton(record) => Ok(Some((version, *record))),
            _ => Ok(None),
        }
    }

    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        let prefix = singleton_namespace_prefix(&self.prefix);
        collect_singletons(&self.prefix, self.client.list_prefix(&prefix).await?)
    }

    async fn reserve_singleton_epoch(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        singleton_lock: Option<LeaseId>,
    ) -> Result<PlacementEpochReservation, PlacementError> {
        validate_singleton_key(&key)?;
        let current = self.get_singleton(&key).await?;
        if current.as_ref().map(|(token, _)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let epoch_key = PlacementEpochKey::Singleton(key.clone());
        let floor = self.get_epoch_floor(&epoch_key).await?;
        validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, record)| (*token, record.epoch)),
        )?;
        let epoch = next_reserved_epoch(
            current.as_ref().map(|(_, record)| record.epoch),
            floor.as_ref().map(|(_, record)| record.epoch),
        )?;
        let guard = singleton_lock.map(|lease_id| EtcdValueGuard {
            key: singleton_lock_key(&self.prefix, &key),
            value: EtcdValue::SingletonLock(lease_id),
        });
        let floor_token = self
            .client
            .reserve_epoch(EtcdEpochReservationRequest {
                record_key: singleton_key(&self.prefix, &key),
                expected_record: expected,
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                expected_floor: Self::floor_compare(&floor),
                floor_value: Self::floor_value(epoch_key.clone(), epoch),
                guard,
            })
            .await?;
        Ok(PlacementEpochReservation::new(
            epoch_key,
            epoch,
            expected,
            floor_token,
            singleton_lock.map(PlacementEpochGuard::Singleton),
        ))
    }

    async fn commit_singleton_epoch(
        &self,
        reservation: PlacementEpochReservation,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        validate_singleton_record(&value)?;
        let PlacementEpochKey::Singleton(key) = reservation.key() else {
            return Err(PlacementError::EpochReservationMismatch);
        };
        validate_singleton_key(key)?;
        if key.service_kind != value.service_kind
            || key.singleton_kind != value.singleton_kind
            || key.scope != value.scope
            || reservation.epoch() != value.epoch
        {
            return Err(PlacementError::EpochReservationMismatch);
        }
        let guard = match reservation.guard() {
            Some(PlacementEpochGuard::Singleton(lease_id)) => Some(EtcdValueGuard {
                key: singleton_lock_key(&self.prefix, key),
                value: EtcdValue::SingletonLock(lease_id),
            }),
            None => None,
            Some(PlacementEpochGuard::Actor(_)) => {
                return Err(PlacementError::EpochReservationMismatch);
            }
        };
        let epoch_key = reservation.key().clone();
        self.client
            .commit_epoch(EtcdEpochCommitRequest {
                record_key: singleton_key(&self.prefix, key),
                expected_record: reservation.expected_record(),
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                floor_token: reservation.floor_token(),
                floor_value: Self::floor_value(epoch_key, reservation.epoch()),
                record_value: EtcdValue::Singleton(Box::new(value)),
                guard,
            })
            .await
    }

    async fn compare_and_put_singleton(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        validate_singleton_key_record(&key, &value)?;
        let current = self.get_singleton(&key).await?;
        if current.as_ref().map(|(token, _)| *token) != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let epoch_key = PlacementEpochKey::Singleton(key.clone());
        let floor = self.get_epoch_floor(&epoch_key).await?;
        validate_epoch_floor_lineage(
            current
                .as_ref()
                .map(|(token, record)| (*token, record.epoch)),
            floor.as_ref().map(|(token, record)| (*token, record.epoch)),
        )?;
        let authority_changed = current.as_ref().is_some_and(|(_, record)| {
            record.owner != value.owner || record.lease_id != value.lease_id
        });
        let reactivating = current.as_ref().is_some_and(|(_, record)| {
            record.state == crate::storage::PlacementState::Stopped
                && value.state != crate::storage::PlacementState::Stopped
        });
        validate_legacy_epoch(
            current.as_ref().map(|(_, record)| record.epoch),
            floor.as_ref().map(|(_, record)| record.epoch),
            value.epoch,
            authority_changed,
            reactivating,
        )?;
        self.client
            .compare_and_put_epoch(EtcdLegacyEpochPutRequest {
                record_key: singleton_key(&self.prefix, &key),
                expected_record: expected,
                floor_key: epoch_floor_key(&self.prefix, &epoch_key),
                expected_floor: Self::floor_compare(&floor),
                floor_value: Self::floor_value(epoch_key, value.epoch),
                record_value: EtcdValue::Singleton(Box::new(value)),
            })
            .await
    }

    async fn acquire_singleton_lock(&self, key: SingletonKey) -> Result<LeaseId, PlacementError> {
        validate_singleton_key(&key)?;
        let lease_id = self.client.next_lease_id().await?;
        match self
            .client
            .compare_and_put(
                singleton_lock_key(&self.prefix, &key),
                None,
                EtcdValue::SingletonLock(lease_id),
            )
            .await
        {
            Ok(_) => Ok(lease_id),
            Err(PlacementError::CompareAndPutFailed) => Err(PlacementError::SingletonLockHeld),
            Err(error) => Err(error),
        }
    }

    async fn validate_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        validate_singleton_key(key)?;
        match self
            .client
            .get(&singleton_lock_key(&self.prefix, key))
            .await?
        {
            Some((_, EtcdValue::SingletonLock(current))) if current == lease_id => Ok(()),
            _ => Err(PlacementError::SingletonLockLost),
        }
    }

    async fn release_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        validate_singleton_key(key)?;
        self.client
            .compare_and_delete(
                singleton_lock_key(&self.prefix, key),
                EtcdValue::SingletonLock(lease_id),
            )
            .await
            .map_err(|error| match error {
                PlacementError::CompareAndPutFailed => PlacementError::SingletonLockLost,
                error => error,
            })
    }

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        validate_actor_key(&key)?;
        let lease_id = self.client.next_lease_id().await?;
        match self
            .client
            .compare_and_put(
                activation_lock_key(&self.prefix, &key),
                None,
                EtcdValue::ActivationLock(lease_id),
            )
            .await
        {
            Ok(_) => Ok(lease_id),
            Err(PlacementError::CompareAndPutFailed) => Err(PlacementError::ActivationLockHeld),
            Err(error) => Err(error),
        }
    }

    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        validate_actor_key(key)?;
        match self
            .client
            .get(&activation_lock_key(&self.prefix, key))
            .await?
        {
            Some((_, EtcdValue::ActivationLock(current))) if current == lease_id => Ok(()),
            _ => Err(PlacementError::ActivationLockLost),
        }
    }

    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        validate_actor_key(key)?;
        self.client
            .compare_and_delete(
                activation_lock_key(&self.prefix, key),
                EtcdValue::ActivationLock(lease_id),
            )
            .await
            .map_err(|error| match error {
                PlacementError::CompareAndPutFailed => PlacementError::ActivationLockLost,
                error => error,
            })
    }

    async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: NonZeroUsize,
    ) -> Result<OwnershipView, OwnershipViewError> {
        validate_service_kind(service_kind).map_err(ownership_identity_error)?;
        validate_instance_id(instance_id).map_err(ownership_identity_error)?;
        let mut raw = self
            .client
            .open_ownership_view(
                EtcdOwnershipRanges {
                    local_instance_key: instance_key(&self.prefix, service_kind, instance_id),
                    record_ranges: vec![
                        EtcdOwnershipRecordRange {
                            record_prefix: actor_service_prefix(&self.prefix, service_kind),
                            floor_prefix: actor_epoch_floor_service_prefix(
                                &self.prefix,
                                service_kind,
                            ),
                        },
                        EtcdOwnershipRecordRange {
                            record_prefix: vshard_service_prefix(&self.prefix, service_kind),
                            floor_prefix: virtual_shard_epoch_floor_service_prefix(
                                &self.prefix,
                                service_kind,
                            ),
                        },
                        EtcdOwnershipRecordRange {
                            record_prefix: singleton_service_prefix(&self.prefix, service_kind),
                            floor_prefix: singleton_epoch_floor_service_prefix(
                                &self.prefix,
                                service_kind,
                            ),
                        },
                    ],
                    watch_prefix: logic_prefix(&self.prefix),
                },
                max_entries,
            )
            .await?;

        let mut local_instance = None;
        let mut records = Vec::new();
        let raw_snapshot_revision = raw.snapshot.revision;
        for entry in raw.snapshot.entries {
            validate_etcd_value_key(&self.prefix, &entry.key, &entry.value).map_err(|error| {
                OwnershipViewError::Protocol {
                    message: error.to_string(),
                }
            })?;
            match entry.value {
                EtcdValue::Instance(record) => {
                    if entry.floor.is_some() {
                        return Err(OwnershipViewError::Protocol {
                            message: format!(
                                "etcd ownership snapshot attached an epoch-floor proof to instance {}",
                                entry.key
                            ),
                        });
                    }
                    if record.service_kind != *service_kind || record.instance_id != *instance_id {
                        return Err(OwnershipViewError::Protocol {
                            message: format!(
                                "etcd ownership snapshot returned unexpected instance {} for service {}",
                                record.instance_id, record.service_kind
                            ),
                        });
                    }
                    local_instance = Some(*record);
                }
                EtcdValue::Actor(record) => {
                    if record.service_kind != *service_kind {
                        return Err(snapshot_service_mismatch(
                            service_kind,
                            &record.service_kind,
                        ));
                    }
                    let record = *record;
                    let proof = map_etcd_snapshot_proof(
                        &self.prefix,
                        raw_snapshot_revision,
                        entry.revision,
                        OwnershipRecordBinding::Actor(record.clone()),
                        entry.floor,
                    )?;
                    records.push(OwnershipViewRecord::Actor {
                        revision: entry.revision,
                        record,
                        proof,
                    });
                }
                EtcdValue::VirtualShard(record) => {
                    if record.service_kind != *service_kind {
                        return Err(snapshot_service_mismatch(
                            service_kind,
                            &record.service_kind,
                        ));
                    }
                    let record = *record;
                    let proof = map_etcd_snapshot_proof(
                        &self.prefix,
                        raw_snapshot_revision,
                        entry.revision,
                        OwnershipRecordBinding::VirtualShard(record.clone()),
                        entry.floor,
                    )?;
                    records.push(OwnershipViewRecord::VirtualShard {
                        revision: entry.revision,
                        record,
                        proof,
                    });
                }
                EtcdValue::Singleton(record) => {
                    if record.service_kind != *service_kind {
                        return Err(snapshot_service_mismatch(
                            service_kind,
                            &record.service_kind,
                        ));
                    }
                    let record = *record;
                    let proof = map_etcd_snapshot_proof(
                        &self.prefix,
                        raw_snapshot_revision,
                        entry.revision,
                        OwnershipRecordBinding::Singleton(record.clone()),
                        entry.floor,
                    )?;
                    records.push(OwnershipViewRecord::Singleton {
                        revision: entry.revision,
                        record,
                        proof,
                    });
                }
                EtcdValue::CoordinatorLeader(_)
                | EtcdValue::ActivationLock(_)
                | EtcdValue::SingletonLock(_)
                | EtcdValue::EpochFloor(_) => {
                    return Err(OwnershipViewError::Protocol {
                        message: format!(
                            "etcd ownership snapshot returned non-ownership key {}",
                            entry.key
                        ),
                    });
                }
            }
        }
        if records.len() > max_entries.get() {
            return Err(OwnershipViewError::CapacityExceeded {
                max_entries: max_entries.get(),
            });
        }

        let snapshot = OwnershipViewSnapshot {
            revision: raw_snapshot_revision,
            local_instance,
            records,
        };
        let prefix = self.prefix.clone();
        let expected_service = service_kind.clone();
        let expected_instance = instance_id.clone();
        let snapshot_revision = snapshot.revision;
        let max_watch_events = client::ownership_watch_event_limit(max_entries).ok_or(
            OwnershipViewError::CapacityExceeded {
                max_entries: max_entries.get(),
            },
        )?;
        let (tx, rx) = broadcast::channel(128);
        let bridge_task = tokio::spawn(async move {
            let mut high_water = snapshot_revision;
            loop {
                match raw.watch.next_update().await {
                    Ok(EtcdOwnershipWatchUpdate::Progress { revision }) => {
                        if let Err(error) = advance_etcd_watch_progress(revision, &mut high_water) {
                            let _ = tx.send(OwnershipWatchMessage::Failed(error));
                            break;
                        }
                        if tx
                            .send(OwnershipWatchMessage::Update(
                                OwnershipWatchUpdate::Progress { revision },
                            ))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(EtcdOwnershipWatchUpdate::Batch(batch)) => {
                        if batch.events.len() > max_watch_events {
                            let _ = tx.send(OwnershipWatchMessage::Failed(
                                OwnershipWatchError::BatchCapacityExceeded {
                                    max_events: max_watch_events,
                                },
                            ));
                            break;
                        }
                        if let Err(error) =
                            advance_etcd_watch_batch(batch.revision, &mut high_water)
                        {
                            let _ = tx.send(OwnershipWatchMessage::Failed(error));
                            break;
                        }
                        match map_etcd_watch_batch(
                            &prefix,
                            &expected_service,
                            &expected_instance,
                            batch.revision,
                            batch.events,
                        ) {
                            Ok(Some(batch)) => {
                                if tx
                                    .send(OwnershipWatchMessage::Update(
                                        OwnershipWatchUpdate::Batch(batch),
                                    ))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Ok(None) => {
                                if tx
                                    .send(OwnershipWatchMessage::Update(
                                        OwnershipWatchUpdate::Progress {
                                            revision: batch.revision,
                                        },
                                    ))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = tx.send(OwnershipWatchMessage::Failed(error));
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(OwnershipWatchMessage::Failed(error));
                        break;
                    }
                }
            }
        });
        Ok(OwnershipView {
            snapshot,
            watch: OwnershipWatch::new_cancellable(rx, bridge_task.abort_handle()),
        })
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        let logic_prefix = logic_prefix(&prefix);
        let mut etcd_watch = self.client.watch_prefix(&logic_prefix).await?;
        let (tx, rx) = broadcast::channel(128);
        tokio::spawn(async move {
            while let Ok(event) = etcd_watch.next().await {
                if let Some(value) = event.value.as_ref()
                    && validate_etcd_value_key(&prefix, &event.key, value).is_err()
                {
                    // The legacy watch cannot carry a structured terminal error.
                    // Closing is still fail-closed: malformed placement identity
                    // must never enter routing or coordinator caches.
                    break;
                }
                match event.value {
                    Some(EtcdValue::Instance(record)) => {
                        let _ = tx.send(PlacementWatchEvent::InstanceUpdated { record: *record });
                    }
                    Some(EtcdValue::Actor(record)) => {
                        let record = *record;
                        let key = ActorPlacementKey {
                            service_kind: record.service_kind.clone(),
                            actor_kind: record.actor_kind.clone(),
                            actor_id: record.actor_id.clone(),
                        };
                        let _ = tx.send(PlacementWatchEvent::ActorUpdated {
                            key,
                            version: event.version,
                            record,
                        });
                    }
                    Some(EtcdValue::VirtualShard(record)) => {
                        let record = *record;
                        let key = VirtualShardPlacementKey {
                            service_kind: record.service_kind.clone(),
                            actor_kind: record.actor_kind.clone(),
                            shard_id: record.shard_id,
                        };
                        let _ = tx.send(PlacementWatchEvent::VirtualShardUpdated {
                            key,
                            version: event.version,
                            record,
                        });
                    }
                    Some(EtcdValue::Singleton(record)) => {
                        let record = *record;
                        let key = SingletonKey {
                            service_kind: record.service_kind.clone(),
                            singleton_kind: record.singleton_kind.clone(),
                            scope: record.scope.clone(),
                        };
                        let _ = tx.send(PlacementWatchEvent::SingletonUpdated {
                            key,
                            version: event.version,
                            record,
                        });
                    }
                    _ => {}
                }
            }
        });
        Ok(PlacementWatch::new(rx))
    }

    fn prefix(&self) -> &PlacementPrefix {
        &self.prefix
    }
}

type EtcdListEntry = (String, PlacementVersion, EtcdValue);

fn collect_instances(
    prefix: &PlacementPrefix,
    entries: Vec<EtcdListEntry>,
) -> Result<Vec<InstanceRecord>, PlacementError> {
    let mut records = Vec::with_capacity(entries.len());
    for (key, _version, value) in entries {
        validate_etcd_value_key(prefix, &key, &value).map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::Instance(record) => records.push(*record),
            _ => return Err(unexpected_etcd_value("instance", &key)),
        }
    }
    Ok(records)
}

fn collect_actors(
    prefix: &PlacementPrefix,
    entries: Vec<EtcdListEntry>,
) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
    let mut records = Vec::with_capacity(entries.len());
    for (key, version, value) in entries {
        validate_etcd_value_key(prefix, &key, &value).map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::Actor(record) => records.push((version, *record)),
            _ => return Err(unexpected_etcd_value("actor", &key)),
        }
    }
    Ok(records)
}

fn collect_virtual_shards(
    prefix: &PlacementPrefix,
    entries: Vec<EtcdListEntry>,
) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
    let mut records = Vec::with_capacity(entries.len());
    for (key, version, value) in entries {
        validate_etcd_value_key(prefix, &key, &value).map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::VirtualShard(record) => records.push((version, *record)),
            _ => return Err(unexpected_etcd_value("virtual shard", &key)),
        }
    }
    Ok(records)
}

fn collect_singletons(
    prefix: &PlacementPrefix,
    entries: Vec<EtcdListEntry>,
) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
    let mut records = Vec::with_capacity(entries.len());
    for (key, version, value) in entries {
        validate_etcd_value_key(prefix, &key, &value).map_err(placement_key_validation_error)?;
        match value {
            EtcdValue::Singleton(record) => records.push((version, *record)),
            _ => return Err(unexpected_etcd_value("singleton", &key)),
        }
    }
    Ok(records)
}

fn validate_actor_key_record(
    key: &ActorPlacementKey,
    record: &ActorPlacementRecord,
) -> Result<(), PlacementError> {
    validate_actor_key(key)?;
    validate_actor_record(record)?;
    if key.service_kind != record.service_kind
        || key.actor_kind != record.actor_kind
        || key.actor_id != record.actor_id
    {
        return Err(key_record_mismatch("actor"));
    }
    Ok(())
}

fn validate_virtual_shard_key_record(
    key: &VirtualShardPlacementKey,
    record: &VirtualShardPlacementRecord,
) -> Result<(), PlacementError> {
    validate_virtual_shard_key(key)?;
    validate_virtual_shard_record(record)?;
    if key.service_kind != record.service_kind
        || key.actor_kind != record.actor_kind
        || key.shard_id != record.shard_id
    {
        return Err(key_record_mismatch("virtual shard"));
    }
    Ok(())
}

fn validate_singleton_key_record(
    key: &SingletonKey,
    record: &SingletonPlacementRecord,
) -> Result<(), PlacementError> {
    validate_singleton_key(key)?;
    validate_singleton_record(record)?;
    if key.service_kind != record.service_kind
        || key.singleton_kind != record.singleton_kind
        || key.scope != record.scope
    {
        return Err(key_record_mismatch("singleton"));
    }
    Ok(())
}

fn validate_service_kind(service_kind: &ServiceKind) -> Result<(), PlacementError> {
    validate_raw_path_identity("service kind", service_kind.as_str())
}

fn validate_actor_kind(actor_kind: &ActorKind) -> Result<(), PlacementError> {
    validate_raw_path_identity("actor kind", actor_kind.as_str())
}

fn validate_instance_id(instance_id: &InstanceId) -> Result<(), PlacementError> {
    validate_raw_path_identity("instance ID", instance_id.as_str())
}

fn validate_actor_id(actor_id: &ActorId) -> Result<(), PlacementError> {
    if let ActorId::Str(value) = actor_id {
        validate_raw_path_identity("string actor ID", value)?;
    }
    Ok(())
}

fn validate_raw_path_identity(identity: &str, value: &str) -> Result<(), PlacementError> {
    // Keep every non-delimiter identity byte-for-byte compatible with existing
    // keys. Escaping only new values would be unsafe during rolling upgrades:
    // an encoded `A/B` could collide with a legacy literal identity such as
    // `A%2FB`. Rejecting `/` makes the raw key grammar unambiguous instead.
    if value.contains('/') {
        return Err(PlacementError::PlacementCodec {
            message: format!("etcd {identity} must not contain the '/' path delimiter"),
        });
    }
    Ok(())
}

fn validate_actor_key(key: &ActorPlacementKey) -> Result<(), PlacementError> {
    validate_service_kind(&key.service_kind)?;
    validate_actor_kind(&key.actor_kind)?;
    validate_actor_id(&key.actor_id)
}

fn validate_virtual_shard_key(key: &VirtualShardPlacementKey) -> Result<(), PlacementError> {
    validate_service_kind(&key.service_kind)?;
    validate_actor_kind(&key.actor_kind)
}

fn validate_singleton_key(key: &SingletonKey) -> Result<(), PlacementError> {
    validate_service_kind(&key.service_kind)?;
    validate_actor_kind(&key.singleton_kind)
}

fn validate_placement_epoch_key(key: &PlacementEpochKey) -> Result<(), PlacementError> {
    match key {
        PlacementEpochKey::Actor(key) => validate_actor_key(key),
        PlacementEpochKey::VirtualShard(key) => validate_virtual_shard_key(key),
        PlacementEpochKey::Singleton(key) => validate_singleton_key(key),
    }
}

fn validate_instance_record(record: &InstanceRecord) -> Result<(), PlacementError> {
    validate_service_kind(&record.service_kind)?;
    validate_instance_id(&record.instance_id)?;
    if !record.incarnation.is_canonical() {
        return Err(PlacementError::PlacementCodec {
            message: "instance incarnation must be one canonical bounded path segment".to_string(),
        });
    }
    Ok(())
}

fn validate_actor_record(record: &ActorPlacementRecord) -> Result<(), PlacementError> {
    validate_service_kind(&record.service_kind)?;
    validate_actor_kind(&record.actor_kind)?;
    validate_actor_id(&record.actor_id)?;
    validate_instance_id(&record.owner)
}

fn validate_virtual_shard_record(
    record: &VirtualShardPlacementRecord,
) -> Result<(), PlacementError> {
    validate_service_kind(&record.service_kind)?;
    validate_actor_kind(&record.actor_kind)?;
    validate_instance_id(&record.owner)
}

fn validate_singleton_record(record: &SingletonPlacementRecord) -> Result<(), PlacementError> {
    validate_service_kind(&record.service_kind)?;
    validate_actor_kind(&record.singleton_kind)?;
    validate_instance_id(&record.owner)
}

fn validate_etcd_value_identity(value: &EtcdValue) -> Result<(), PlacementError> {
    match value {
        EtcdValue::Instance(record) => validate_instance_record(record),
        EtcdValue::Actor(record) => validate_actor_record(record),
        EtcdValue::VirtualShard(record) => validate_virtual_shard_record(record),
        EtcdValue::Singleton(record) => validate_singleton_record(record),
        EtcdValue::EpochFloor(record) => validate_placement_epoch_key(&record.key),
        EtcdValue::CoordinatorLeader(leadership) => validate_instance_id(&leadership.candidate_id),
        EtcdValue::ActivationLock(_) | EtcdValue::SingletonLock(_) => Ok(()),
    }
}

fn key_record_mismatch(record_kind: &str) -> PlacementError {
    PlacementError::PlacementCodec {
        message: format!("{record_kind} placement key does not match its record identity"),
    }
}

fn placement_key_validation_error(error: OwnershipWatchError) -> PlacementError {
    match error {
        OwnershipWatchError::Protocol { message } => PlacementError::PlacementCodec { message },
        error => PlacementError::PlacementCodec {
            message: error.to_string(),
        },
    }
}

fn ownership_identity_error(error: PlacementError) -> OwnershipViewError {
    OwnershipViewError::Protocol {
        message: error.to_string(),
    }
}

fn unexpected_etcd_value(expected: &str, key: &str) -> PlacementError {
    PlacementError::PlacementCodec {
        message: format!("etcd {expected} range returned an unexpected value at {key}"),
    }
}

fn snapshot_service_mismatch(expected: &ServiceKind, actual: &ServiceKind) -> OwnershipViewError {
    OwnershipViewError::Protocol {
        message: format!("etcd ownership snapshot expected service {expected}, got {actual}"),
    }
}

fn validate_etcd_value_key(
    prefix: &PlacementPrefix,
    actual_key: &str,
    value: &EtcdValue,
) -> Result<(), OwnershipWatchError> {
    validate_etcd_value_identity(value).map_err(|error| OwnershipWatchError::Protocol {
        message: error.to_string(),
    })?;
    let expected_key = match value {
        EtcdValue::Instance(record) => {
            instance_key(prefix, &record.service_kind, &record.instance_id)
        }
        EtcdValue::Actor(record) => actor_key(
            prefix,
            &ActorPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                actor_id: record.actor_id.clone(),
            },
        ),
        EtcdValue::VirtualShard(record) => vshard_key(
            prefix,
            &VirtualShardPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                shard_id: record.shard_id,
            },
        ),
        EtcdValue::Singleton(record) => singleton_key(
            prefix,
            &SingletonKey {
                service_kind: record.service_kind.clone(),
                singleton_kind: record.singleton_kind.clone(),
                scope: record.scope.clone(),
            },
        ),
        EtcdValue::EpochFloor(record) => epoch_floor_key(prefix, &record.key),
        EtcdValue::CoordinatorLeader(_) => coordinator_leader_key(prefix),
        EtcdValue::ActivationLock(_) => {
            let namespace = activation_lock_namespace_prefix(prefix);
            if is_canonical_activation_lock_key(actual_key, &namespace) {
                return Ok(());
            }
            return Err(OwnershipWatchError::Protocol {
                message: format!("etcd activation lock has a non-canonical key: {actual_key}"),
            });
        }
        EtcdValue::SingletonLock(_) => {
            let namespace = singleton_lock_namespace_prefix(prefix);
            if is_canonical_singleton_lock_key(actual_key, &namespace) {
                return Ok(());
            }
            return Err(OwnershipWatchError::Protocol {
                message: format!("etcd singleton lock has a non-canonical key: {actual_key}"),
            });
        }
    };
    if actual_key != expected_key {
        return Err(OwnershipWatchError::Protocol {
            message: format!(
                "etcd ownership value key mismatch: expected {expected_key}, got {actual_key}"
            ),
        });
    }
    Ok(())
}

fn is_canonical_activation_lock_key(actual_key: &str, namespace: &str) -> bool {
    let Some([_service_kind, _actor_kind, actor_id]) = split_lock_segments(actual_key, namespace)
    else {
        return false;
    };
    is_canonical_actor_id_segment(actor_id)
}

fn is_canonical_singleton_lock_key(actual_key: &str, namespace: &str) -> bool {
    let Some([_service_kind, _singleton_kind, scope]) = split_lock_segments(actual_key, namespace)
    else {
        return false;
    };
    is_lower_hex(scope)
}

fn split_lock_segments<'a>(actual_key: &'a str, namespace: &str) -> Option<[&'a str; 3]> {
    let mut segments = actual_key.strip_prefix(namespace)?.split('/');
    let result = [segments.next()?, segments.next()?, segments.next()?];
    segments.next().is_none().then_some(result)
}

fn is_canonical_actor_id_segment(segment: &str) -> bool {
    if segment.strip_prefix("str:").is_some() {
        return true;
    }
    if let Some(value) = segment.strip_prefix("u64:") {
        return value
            .parse::<u64>()
            .is_ok_and(|parsed| parsed.to_string() == value);
    }
    if let Some(value) = segment.strip_prefix("i64:") {
        return value
            .parse::<i64>()
            .is_ok_and(|parsed| parsed.to_string() == value);
    }
    segment.strip_prefix("bytes:").is_some_and(is_lower_hex)
}

fn is_lower_hex(value: &str) -> bool {
    value.len().is_multiple_of(2)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn map_etcd_snapshot_proof(
    prefix: &PlacementPrefix,
    observed_revision: crate::storage::PlacementRevision,
    record_revision: crate::storage::PlacementRevision,
    binding: OwnershipRecordBinding,
    raw: Option<EtcdOwnershipFloorProof>,
) -> Result<OwnershipEpochFloorProof, OwnershipViewError> {
    map_etcd_epoch_floor_proof(
        prefix,
        OwnershipProofContext::Snapshot,
        observed_revision,
        PlacementVersion::from_modification_revision(record_revision.0),
        binding,
        raw,
    )
    .map_err(|error| OwnershipViewError::Proof { error })
}

fn map_etcd_watch_proof(
    prefix: &PlacementPrefix,
    context: OwnershipProofContext,
    observed_revision: crate::storage::PlacementRevision,
    record_version: PlacementVersion,
    binding: OwnershipRecordBinding,
    raw: Option<EtcdOwnershipFloorProof>,
) -> Result<OwnershipEpochFloorProof, OwnershipWatchError> {
    map_etcd_epoch_floor_proof(
        prefix,
        context,
        observed_revision,
        record_version,
        binding,
        raw,
    )
    .map_err(|error| OwnershipWatchError::Proof { error })
}

fn map_etcd_epoch_floor_proof(
    prefix: &PlacementPrefix,
    context: OwnershipProofContext,
    observed_revision: crate::storage::PlacementRevision,
    record_version: PlacementVersion,
    binding: OwnershipRecordBinding,
    raw: Option<EtcdOwnershipFloorProof>,
) -> Result<OwnershipEpochFloorProof, OwnershipProofError> {
    let key = binding.epoch_key();
    let raw = raw.ok_or_else(|| OwnershipProofError::MissingFloor {
        key: key.clone(),
        observed_revision,
    })?;
    if raw.observed_revision != observed_revision {
        return Err(OwnershipProofError::ObservationRevisionMismatch {
            key,
            observed_revision,
            record: record_version,
            floor: raw.version,
        });
    }
    let expected_floor_key = epoch_floor_key(prefix, &key);
    if raw.key != expected_floor_key {
        return Err(OwnershipProofError::MalformedFloor {
            key,
            message: format!(
                "backend returned floor key {}, expected {expected_floor_key}",
                raw.key
            ),
        });
    }
    validate_etcd_value_key(prefix, &raw.key, &raw.value).map_err(|error| {
        OwnershipProofError::MalformedFloor {
            key: key.clone(),
            message: error.to_string(),
        }
    })?;
    let EtcdValue::EpochFloor(floor) = raw.value else {
        return Err(OwnershipProofError::MalformedFloor {
            key,
            message: "backend returned a non-floor value".to_string(),
        });
    };
    OwnershipEpochFloorProof::new(
        context,
        observed_revision,
        record_version,
        binding,
        raw.version,
        *floor,
        None,
    )
}

fn map_etcd_watch_batch(
    prefix: &PlacementPrefix,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    revision: crate::storage::PlacementRevision,
    raw_events: Vec<EtcdOwnershipWatchEvent>,
) -> Result<Option<OwnershipWatchBatch>, OwnershipWatchError> {
    let context = EtcdWatchMappingContext {
        prefix,
        expected_service,
        expected_instance,
        observed_revision: revision,
    };
    let mut events = Vec::new();
    for event in raw_events {
        let mapped = match event {
            EtcdOwnershipWatchEvent::Upserted {
                key,
                version,
                value,
                floor,
            } => map_etcd_watch_value(&context, &key, version, value, floor, false)?,
            EtcdOwnershipWatchEvent::Deleted {
                key,
                previous_version,
                previous_value,
                floor,
            } => map_etcd_watch_value(
                &context,
                &key,
                previous_version,
                previous_value,
                floor,
                true,
            )?,
        };
        if let Some(event) = mapped {
            events.push(event);
        }
    }
    if events.is_empty() {
        Ok(None)
    } else {
        Ok(Some(OwnershipWatchBatch { revision, events }))
    }
}

struct EtcdWatchMappingContext<'a> {
    prefix: &'a PlacementPrefix,
    expected_service: &'a ServiceKind,
    expected_instance: &'a InstanceId,
    observed_revision: crate::storage::PlacementRevision,
}

fn advance_etcd_watch_batch(
    revision: crate::storage::PlacementRevision,
    high_water: &mut crate::storage::PlacementRevision,
) -> Result<(), OwnershipWatchError> {
    if revision <= *high_water {
        return Err(OwnershipWatchError::Protocol {
            message: format!(
                "etcd ownership batch revision {revision:?} did not advance beyond {high_water:?}"
            ),
        });
    }
    *high_water = revision;
    Ok(())
}

fn advance_etcd_watch_progress(
    revision: crate::storage::PlacementRevision,
    high_water: &mut crate::storage::PlacementRevision,
) -> Result<(), OwnershipWatchError> {
    if revision < *high_water {
        return Err(OwnershipWatchError::Protocol {
            message: format!(
                "etcd ownership progress revision {revision:?} regressed behind {high_water:?}"
            ),
        });
    }
    *high_water = revision;
    Ok(())
}

fn map_etcd_watch_value(
    context: &EtcdWatchMappingContext<'_>,
    key: &str,
    record_version: PlacementVersion,
    value: EtcdValue,
    floor: Option<EtcdOwnershipFloorProof>,
    deleted: bool,
) -> Result<Option<OwnershipWatchEvent>, OwnershipWatchError> {
    validate_etcd_value_key(context.prefix, key, &value)?;
    let event = match value {
        EtcdValue::Instance(record) => {
            if floor.is_some() {
                return Err(OwnershipWatchError::Protocol {
                    message: format!(
                        "etcd ownership watch attached an epoch-floor proof to instance {key}"
                    ),
                });
            }
            if record.service_kind != *context.expected_service
                || record.instance_id != *context.expected_instance
            {
                return Ok(None);
            }
            if deleted {
                OwnershipWatchEvent::InstanceDeleted { record: *record }
            } else {
                OwnershipWatchEvent::InstanceUpserted { record: *record }
            }
        }
        EtcdValue::Actor(record) => {
            if record.service_kind != *context.expected_service {
                return Ok(None);
            }
            let key = ActorPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                actor_id: record.actor_id.clone(),
            };
            let record = *record;
            let proof = map_etcd_watch_proof(
                context.prefix,
                if deleted {
                    OwnershipProofContext::Delete
                } else {
                    OwnershipProofContext::Upsert
                },
                context.observed_revision,
                record_version,
                OwnershipRecordBinding::Actor(record.clone()),
                floor,
            )?;
            if deleted {
                OwnershipWatchEvent::ActorDeleted {
                    key,
                    previous_record: record,
                    proof,
                }
            } else {
                OwnershipWatchEvent::ActorUpserted { key, record, proof }
            }
        }
        EtcdValue::VirtualShard(record) => {
            if record.service_kind != *context.expected_service {
                return Ok(None);
            }
            let key = VirtualShardPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                shard_id: record.shard_id,
            };
            let record = *record;
            let proof = map_etcd_watch_proof(
                context.prefix,
                if deleted {
                    OwnershipProofContext::Delete
                } else {
                    OwnershipProofContext::Upsert
                },
                context.observed_revision,
                record_version,
                OwnershipRecordBinding::VirtualShard(record.clone()),
                floor,
            )?;
            if deleted {
                OwnershipWatchEvent::VirtualShardDeleted {
                    key,
                    previous_record: record,
                    proof,
                }
            } else {
                OwnershipWatchEvent::VirtualShardUpserted { key, record, proof }
            }
        }
        EtcdValue::Singleton(record) => {
            if record.service_kind != *context.expected_service {
                return Ok(None);
            }
            let key = SingletonKey {
                service_kind: record.service_kind.clone(),
                singleton_kind: record.singleton_kind.clone(),
                scope: record.scope.clone(),
            };
            let record = *record;
            let proof = map_etcd_watch_proof(
                context.prefix,
                if deleted {
                    OwnershipProofContext::Delete
                } else {
                    OwnershipProofContext::Upsert
                },
                context.observed_revision,
                record_version,
                OwnershipRecordBinding::Singleton(record.clone()),
                floor,
            )?;
            if deleted {
                OwnershipWatchEvent::SingletonDeleted {
                    key,
                    previous_record: record,
                    proof,
                }
            } else {
                OwnershipWatchEvent::SingletonUpserted { key, record, proof }
            }
        }
        EtcdValue::CoordinatorLeader(_)
        | EtcdValue::ActivationLock(_)
        | EtcdValue::SingletonLock(_)
        | EtcdValue::EpochFloor(_) => {
            if floor.is_some() {
                return Err(OwnershipWatchError::Protocol {
                    message: format!(
                        "etcd ownership watch attached an epoch-floor proof to non-placement {key}"
                    ),
                });
            }
            return Ok(None);
        }
    };
    Ok(Some(event))
}

#[cfg(test)]
mod tests;
