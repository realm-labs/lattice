use std::{collections::BTreeSet, fmt, time::Duration};

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp,
};
use lattice_id::worker::{
    WorkerId, WorkerIdAcquisition, WorkerIdLease, WorkerIdLeaseStore, WorkerIdLeaseToken,
    WorkerIdOwner, WorkerIdRange, WorkerIdStoreError,
};
use serde::{Deserialize, Serialize};

use crate::config::{EtcdWorkerIdStoreConfig, validate_key_prefix};

const SLOT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone)]
pub struct EtcdWorkerIdLeaseStore {
    client: Client,
    key_prefix: String,
}

impl fmt::Debug for EtcdWorkerIdLeaseStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdWorkerIdLeaseStore")
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

impl EtcdWorkerIdLeaseStore {
    pub async fn connect(config: EtcdWorkerIdStoreConfig) -> Result<Self, WorkerIdStoreError> {
        Self::connect_with_options(config, None).await
    }

    pub async fn connect_with_options(
        config: EtcdWorkerIdStoreConfig,
        options: Option<ConnectOptions>,
    ) -> Result<Self, WorkerIdStoreError> {
        config
            .validate()
            .map_err(|message| WorkerIdStoreError::InvalidConfiguration {
                message: message.to_string(),
            })?;
        let client = Client::connect(config.endpoints, options)
            .await
            .map_err(|error| backend_error("connect", error))?;
        Ok(Self {
            client,
            key_prefix: config.key_prefix,
        })
    }

    pub fn from_client(
        client: Client,
        key_prefix: impl Into<String>,
    ) -> Result<Self, WorkerIdStoreError> {
        let key_prefix = key_prefix.into();
        validate_key_prefix(&key_prefix).map_err(|message| {
            WorkerIdStoreError::InvalidConfiguration {
                message: message.to_string(),
            }
        })?;
        Ok(Self { client, key_prefix })
    }

    async fn occupied_ids(
        &self,
        owner: &WorkerIdOwner,
    ) -> Result<BTreeSet<WorkerId>, WorkerIdStoreError> {
        let prefix = self.slot_prefix(owner);
        let mut client = self.client.clone();
        let response = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|error| backend_error("list slots", error))?;
        let mut occupied = BTreeSet::new();
        for record in response.kvs() {
            let key = std::str::from_utf8(record.key()).map_err(|_| codec_error("slot key"))?;
            let suffix = key
                .strip_prefix(&prefix)
                .ok_or_else(|| codec_error("slot prefix"))?;
            let id = suffix
                .parse::<u64>()
                .map(WorkerId::new)
                .map_err(|_| codec_error("worker ID slot"))?;
            occupied.insert(id);
        }
        Ok(occupied)
    }

    async fn try_acquire_slot(
        &self,
        owner: &WorkerIdOwner,
        id: WorkerId,
        lease_id: i64,
        valid_for: Duration,
    ) -> Result<Option<WorkerIdAcquisition>, WorkerIdStoreError> {
        let slot_key = self.slot_key(owner, id);
        let history_key = self.history_key(owner, id);
        let fence = uuid::Uuid::new_v4().to_string();
        let token = WorkerIdLeaseToken::new(format!("{lease_id}:{fence}"))
            .map_err(|_| codec_error("generated fencing token"))?;
        let record = SlotRecord {
            schema_version: SLOT_SCHEMA_VERSION,
            worker_id: id.get(),
            owner: owner.clone(),
            fence,
            lease_id,
        };
        let encoded = serde_json::to_vec(&record).map_err(|_| codec_error("slot record"))?;
        let slot_put = TxnOp::put(
            slot_key.clone(),
            encoded.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        );

        let mut client = self.client.clone();
        let first = client
            .txn(
                Txn::new()
                    .when([
                        Compare::version(slot_key.clone(), CompareOp::Equal, 0),
                        Compare::version(history_key.clone(), CompareOp::Equal, 0),
                    ])
                    .and_then([TxnOp::put(history_key.clone(), "1", None), slot_put.clone()]),
            )
            .await
            .map_err(|error| backend_error("claim unused slot", error))?;
        let first_use = first.succeeded();
        let acquired = if first_use {
            true
        } else {
            client
                .txn(
                    Txn::new()
                        .when([
                            Compare::version(slot_key, CompareOp::Equal, 0),
                            Compare::version(history_key, CompareOp::Greater, 0),
                        ])
                        .and_then([slot_put]),
                )
                .await
                .map_err(|error| backend_error("claim reused slot", error))?
                .succeeded()
        };
        if !acquired {
            return Ok(None);
        }
        let lease = WorkerIdLease::new(id, owner.clone(), token, valid_for)
            .map_err(|_| codec_error("Etcd lease"))?;
        Ok(Some(if first_use {
            WorkerIdAcquisition::FirstUse(lease)
        } else {
            WorkerIdAcquisition::Reused(lease)
        }))
    }

    async fn matching_record(
        &self,
        lease: &WorkerIdLease,
        lease_id: i64,
    ) -> Result<Option<Vec<u8>>, WorkerIdStoreError> {
        let key = self.slot_key(lease.owner(), lease.id());
        let mut client = self.client.clone();
        let response = client
            .get(key, None)
            .await
            .map_err(|error| backend_error("read slot", error))?;
        let Some(value) = response.kvs().first() else {
            return Ok(None);
        };
        if value.lease() != lease_id {
            return Ok(None);
        }
        let record: SlotRecord =
            serde_json::from_slice(value.value()).map_err(|_| codec_error("stored slot record"))?;
        if !record.matches(lease, lease_id)? {
            return Ok(None);
        }
        Ok(Some(value.value().to_vec()))
    }

    fn slot_prefix(&self, owner: &WorkerIdOwner) -> String {
        format!("{}/{}/slots/", self.key_prefix, owner.cluster_id())
    }

    fn slot_key(&self, owner: &WorkerIdOwner, id: WorkerId) -> String {
        format!("{}{id}", self.slot_prefix(owner))
    }

    fn history_key(&self, owner: &WorkerIdOwner, id: WorkerId) -> String {
        format!("{}/{}/history/{id}", self.key_prefix, owner.cluster_id())
    }
}

#[async_trait]
impl WorkerIdLeaseStore for EtcdWorkerIdLeaseStore {
    async fn acquire(
        &self,
        owner: &WorkerIdOwner,
        range: WorkerIdRange,
        ttl: Duration,
    ) -> Result<WorkerIdAcquisition, WorkerIdStoreError> {
        let ttl_seconds = ttl_seconds(ttl)?;
        let mut client = self.client.clone();
        let granted = client
            .lease_grant(ttl_seconds, None)
            .await
            .map_err(|error| backend_error("grant lease", error))?;
        let lease_id = granted.id();
        let valid_for = match granted_validity(granted.ttl()) {
            Some(valid_for) => valid_for,
            None => {
                let _ = client.lease_revoke(lease_id).await;
                return Err(WorkerIdStoreError::Backend {
                    message: format!("Etcd granted a lease with TTL {}", granted.ttl()),
                });
            }
        };
        let occupied = match self.occupied_ids(owner).await {
            Ok(occupied) => occupied,
            Err(error) => {
                let _ = client.lease_revoke(lease_id).await;
                return Err(error);
            }
        };
        for id in range.ids().filter(|id| !occupied.contains(id)) {
            match self.try_acquire_slot(owner, id, lease_id, valid_for).await {
                Ok(Some(acquisition)) => return Ok(acquisition),
                Ok(None) => {}
                Err(error) => {
                    let _ = client.lease_revoke(lease_id).await;
                    return Err(error);
                }
            }
        }
        let _ = client.lease_revoke(lease_id).await;
        Err(WorkerIdStoreError::unavailable(owner, range))
    }

    /// Keeps the Etcd lease alive. Etcd always resets a keepalive to the TTL it
    /// granted, so `_ttl` cannot change the window; the returned lease reports
    /// the TTL Etcd actually applied.
    async fn renew(
        &self,
        lease: &WorkerIdLease,
        _ttl: Duration,
    ) -> Result<Option<WorkerIdLease>, WorkerIdStoreError> {
        let lease_id = lease_id(lease.token())?;
        if self.matching_record(lease, lease_id).await?.is_none() {
            return Ok(None);
        }
        let mut client = self.client.clone();
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lease_id)
            .await
            .map_err(|error| backend_error("open lease keepalive", error))?;
        keeper
            .keep_alive()
            .await
            .map_err(|error| backend_error("send lease keepalive", error))?;
        let response = stream
            .message()
            .await
            .map_err(|error| backend_error("receive lease keepalive", error))?;
        let Some(valid_for) = keepalive_validity(response.map(|response| response.ttl()))? else {
            return Ok(None);
        };
        if self.matching_record(lease, lease_id).await?.is_none() {
            return Ok(None);
        }
        WorkerIdLease::new(
            lease.id(),
            lease.owner().clone(),
            lease.token().clone(),
            valid_for,
        )
        .map(Some)
        .map_err(|_| codec_error("renewed lease"))
    }

    async fn release(&self, lease: &WorkerIdLease) -> Result<bool, WorkerIdStoreError> {
        let lease_id = lease_id(lease.token())?;
        let Some(encoded) = self.matching_record(lease, lease_id).await? else {
            return Ok(false);
        };
        let key = self.slot_key(lease.owner(), lease.id());
        let mut client = self.client.clone();
        let deleted = client
            .txn(
                Txn::new()
                    .when([Compare::value(key.clone(), CompareOp::Equal, encoded)])
                    .and_then([TxnOp::delete(key, Some(DeleteOptions::new()))]),
            )
            .await
            .map_err(|error| backend_error("release slot", error))?
            .succeeded();
        if !deleted {
            return Ok(false);
        }
        let revoked = client
            .lease_revoke(lease_id)
            .await
            .map(|_| ())
            .map_err(|error| backend_error("revoke released lease", error));
        release_outcome(revoked, lease)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlotRecord {
    schema_version: u32,
    worker_id: u64,
    owner: WorkerIdOwner,
    fence: String,
    lease_id: i64,
}

impl SlotRecord {
    fn matches(&self, lease: &WorkerIdLease, lease_id: i64) -> Result<bool, WorkerIdStoreError> {
        if self.schema_version != SLOT_SCHEMA_VERSION {
            return Err(codec_error("slot schema version"));
        }
        let expected = format!("{lease_id}:{}", self.fence);
        Ok(self.worker_id == lease.id().get()
            && &self.owner == lease.owner()
            && self.lease_id == lease_id
            && lease.token().expose() == expected)
    }
}

fn lease_id(token: &WorkerIdLeaseToken) -> Result<i64, WorkerIdStoreError> {
    let (lease_id, fence) = token
        .expose()
        .split_once(':')
        .ok_or_else(|| codec_error("lease token"))?;
    if fence.is_empty() {
        return Err(codec_error("lease token fence"));
    }
    lease_id
        .parse::<i64>()
        .ok()
        .filter(|lease_id| *lease_id > 0)
        .ok_or_else(|| codec_error("lease token ID"))
}

fn granted_validity(granted_ttl: i64) -> Option<Duration> {
    u64::try_from(granted_ttl)
        .ok()
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

fn keepalive_validity(granted_ttl: Option<i64>) -> Result<Option<Duration>, WorkerIdStoreError> {
    match granted_ttl {
        None => Err(WorkerIdStoreError::Backend {
            message: "Etcd closed the lease keepalive stream before reporting a TTL".to_string(),
        }),
        Some(granted_ttl) => Ok(granted_validity(granted_ttl)),
    }
}

fn release_outcome(
    revoked: Result<(), WorkerIdStoreError>,
    lease: &WorkerIdLease,
) -> Result<bool, WorkerIdStoreError> {
    if let Err(error) = revoked {
        tracing::warn!(
            error = %error,
            worker_id = %lease.id(),
            "released the Etcd worker ID slot but could not revoke its lease; the lease expires on its own"
        );
    }
    Ok(true)
}

fn ttl_seconds(ttl: Duration) -> Result<i64, WorkerIdStoreError> {
    if ttl.is_zero() {
        return Err(WorkerIdStoreError::InvalidConfiguration {
            message: "Etcd lease TTL must be nonzero".to_string(),
        });
    }
    let seconds = ttl
        .as_secs()
        .saturating_add(u64::from(ttl.subsec_nanos() != 0));
    i64::try_from(seconds).map_err(|_| WorkerIdStoreError::InvalidConfiguration {
        message: "Etcd lease TTL is too large".to_string(),
    })
}

fn backend_error(operation: &'static str, error: etcd_client::Error) -> WorkerIdStoreError {
    WorkerIdStoreError::Backend {
        message: format!("Etcd {operation} failed: {error}"),
    }
}

fn codec_error(context: &'static str) -> WorkerIdStoreError {
    WorkerIdStoreError::Codec {
        message: format!("invalid {context}"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lattice_core::actor_ref::{ClusterId, NodeIncarnation};
    use lattice_id::worker::{
        WorkerId, WorkerIdLease, WorkerIdLeaseToken, WorkerIdOwner, WorkerIdStoreError,
    };

    use super::{granted_validity, keepalive_validity, release_outcome, ttl_seconds};

    fn lease() -> WorkerIdLease {
        let owner = WorkerIdOwner::for_node(
            ClusterId::new("etcd-store-test").unwrap(),
            "node-a",
            NodeIncarnation::new(1).unwrap(),
        )
        .unwrap();
        WorkerIdLease::new(
            WorkerId::new(3),
            owner,
            WorkerIdLeaseToken::new("7:fence").unwrap(),
            Duration::from_secs(5),
        )
        .unwrap()
    }

    #[test]
    fn lease_ttl_rounds_up_to_etcd_seconds() {
        assert_eq!(ttl_seconds(Duration::from_millis(1)).unwrap(), 1);
        assert_eq!(ttl_seconds(Duration::from_secs(5)).unwrap(), 5);
        assert_eq!(ttl_seconds(Duration::from_millis(5_001)).unwrap(), 6);
    }

    #[test]
    fn the_granted_ttl_bounds_the_validity_window() {
        assert_eq!(granted_validity(7), Some(Duration::from_secs(7)));
        assert_eq!(granted_validity(0), None);
        assert_eq!(granted_validity(-1), None);
    }

    #[test]
    fn a_closed_keepalive_stream_is_retryable_and_a_zero_ttl_is_lost() {
        assert!(matches!(
            keepalive_validity(None),
            Err(WorkerIdStoreError::Backend { .. })
        ));
        assert_eq!(keepalive_validity(Some(0)).unwrap(), None);
        assert_eq!(
            keepalive_validity(Some(5)).unwrap(),
            Some(Duration::from_secs(5))
        );
    }

    #[test]
    fn a_deleted_slot_is_released_even_when_the_lease_revoke_fails() {
        assert!(
            release_outcome(
                Err(WorkerIdStoreError::Backend {
                    message: "revoke failed".to_string(),
                }),
                &lease(),
            )
            .unwrap()
        );
    }
}
