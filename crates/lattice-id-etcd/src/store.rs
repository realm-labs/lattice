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
        let lease_id = client
            .lease_grant(ttl_seconds, None)
            .await
            .map_err(|error| backend_error("grant lease", error))?
            .id();
        let valid_for = Duration::from_secs(ttl_seconds as u64);
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
        let Some(response) = stream
            .message()
            .await
            .map_err(|error| backend_error("receive lease keepalive", error))?
        else {
            return Ok(None);
        };
        if response.ttl() <= 0 || self.matching_record(lease, lease_id).await?.is_none() {
            return Ok(None);
        }
        WorkerIdLease::new(
            lease.id(),
            lease.owner().clone(),
            lease.token().clone(),
            Duration::from_secs(response.ttl() as u64),
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
        client
            .lease_revoke(lease_id)
            .await
            .map_err(|error| backend_error("revoke released lease", error))?;
        Ok(true)
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

    use super::ttl_seconds;

    #[test]
    fn lease_ttl_rounds_up_to_etcd_seconds() {
        assert_eq!(ttl_seconds(Duration::from_millis(1)).unwrap(), 1);
        assert_eq!(ttl_seconds(Duration::from_secs(5)).unwrap(), 5);
        assert_eq!(ttl_seconds(Duration::from_millis(5_001)).unwrap(), 6);
    }
}
