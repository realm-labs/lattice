use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, ConnectOptions, GetOptions, PutOptions, Txn, TxnOp};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{CoordinatorStore, PlacementStore, StorageError};
use crate::coordinator::{LeaderRecord, MemberRecord};
use crate::plan::RebalancePlan;
use crate::types::{ClaimGrant, PlacementSlot, PlacementSlotKey, Revision};

pub const STORAGE_SCHEMA_GENERATION: u64 = 3;

#[derive(Debug, Clone)]
pub struct EtcdPlacementConfig {
    pub endpoints: Vec<String>,
    pub cluster_prefix: String,
    pub maximum_list_records: usize,
    pub connect_options: Option<ConnectOptions>,
}

impl EtcdPlacementConfig {
    pub fn validate(&self) -> Result<(), StorageError> {
        validate_prefix(&self.cluster_prefix)?;
        if self.endpoints.is_empty()
            || self.endpoints.len() > 16
            || self.maximum_list_records == 0
            || self
                .endpoints
                .iter()
                .any(|endpoint| endpoint.is_empty() || endpoint.len() > 2048)
        {
            return Err(StorageError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct EtcdPlacementStore {
    client: Client,
    prefix: String,
    maximum_list_records: usize,
}

impl EtcdPlacementStore {
    pub async fn connect(config: EtcdPlacementConfig) -> Result<Self, StorageError> {
        config.validate()?;
        let client = Client::connect(&config.endpoints, config.connect_options)
            .await
            .map_err(map_etcd)?;
        Ok(Self {
            client,
            prefix: config.cluster_prefix,
            maximum_list_records: config.maximum_list_records,
        })
    }

    pub fn from_client(
        client: Client,
        cluster_prefix: impl Into<String>,
        maximum_list_records: usize,
    ) -> Result<Self, StorageError> {
        let prefix = cluster_prefix.into();
        validate_prefix(&prefix)?;
        if maximum_list_records == 0 {
            return Err(StorageError::ZeroLimit);
        }
        Ok(Self {
            client,
            prefix,
            maximum_list_records,
        })
    }

    pub async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        let key = self.key("schema_generation");
        let expected = STORAGE_SCHEMA_GENERATION.to_string().into_bytes();
        let mut client = self.client.clone();
        let response = client.get(key.clone(), None).await.map_err(map_etcd)?;
        if let Some(record) = response.kvs().first() {
            return if record.value() == expected {
                Ok(())
            } else {
                Err(StorageError::SchemaGenerationMismatch)
            };
        }
        let response = client
            .txn(
                Txn::new()
                    .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(key.clone(), expected.clone(), None)]),
            )
            .await
            .map_err(map_etcd)?;
        if response.succeeded() {
            return Ok(());
        }
        let response = client.get(key, None).await.map_err(map_etcd)?;
        if response
            .kvs()
            .first()
            .is_some_and(|record| record.value() == expected)
        {
            Ok(())
        } else {
            Err(StorageError::SchemaGenerationMismatch)
        }
    }

    pub async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        let seconds = i64::try_from(ttl.as_secs()).map_err(|_| StorageError::InvalidConfig)?;
        if seconds == 0 {
            return Err(StorageError::InvalidConfig);
        }
        let mut client = self.client.clone();
        client
            .lease_grant(seconds, None)
            .await
            .map(|response| response.id())
            .map_err(map_etcd)
    }

    pub async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        if lease_id == 0 {
            return Err(StorageError::InvalidConfig);
        }
        let mut client = self.client.clone();
        let (mut keeper, mut stream) = client.lease_keep_alive(lease_id).await.map_err(map_etcd)?;
        keeper.keep_alive().await.map_err(map_etcd)?;
        stream
            .message()
            .await
            .map_err(map_etcd)?
            .filter(|response| response.ttl() > 0)
            .ok_or(StorageError::Unavailable)
            .map(|_| ())
    }

    pub async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        let mut client = self.client.clone();
        client
            .lease_revoke(lease_id)
            .await
            .map(|_| ())
            .map_err(map_etcd)
    }

    pub async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        leader
            .node
            .validate()
            .map_err(|_| StorageError::InvalidRecord)?;
        let leader_key = self.key("coordinator/leader");
        let term_key = self.key("coordinator/term");
        let current_term = self.read_raw(&term_key).await?;
        let term = current_term
            .as_ref()
            .map(|(bytes, _)| {
                std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or(StorageError::Codec)
            })
            .transpose()?
            .unwrap_or(0);
        let expected = term.checked_add(1).ok_or(StorageError::Capacity)?;
        if leader.term.get() != expected {
            return Err(StorageError::CompareFailed);
        }
        let term_compare = match current_term.as_ref() {
            Some((_, revision)) => {
                Compare::mod_revision(term_key.clone(), CompareOp::Equal, *revision)
            }
            None => Compare::version(term_key.clone(), CompareOp::Equal, 0),
        };
        let mut client = self.client.clone();
        client
            .txn(
                Txn::new()
                    .when([
                        Compare::version(leader_key.clone(), CompareOp::Equal, 0),
                        term_compare,
                    ])
                    .and_then([
                        TxnOp::put(term_key, expected.to_string(), None),
                        TxnOp::put(
                            leader_key,
                            encode(leader)?,
                            Some(PutOptions::new().with_lease(lease_id)),
                        ),
                    ]),
            )
            .await
            .map(|response| response.succeeded())
            .map_err(map_etcd)
    }

    pub async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError> {
        self.get_json("coordinator/leader").await
    }

    pub async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        self.get_json(&format!("members/{node_id}")).await
    }

    pub async fn create_member(&self, member: &MemberRecord) -> Result<(), StorageError> {
        validate_member_record(member)?;
        let suffix = format!("members/{}", member.node.node_id);
        let key = self.key(&suffix);
        let current = self.read_raw(&key).await?;
        if let Some((bytes, _)) = &current {
            let current: MemberRecord = decode(bytes)?;
            if current.node.incarnation != member.node.incarnation {
                return Err(StorageError::IncarnationConflict);
            }
            return Err(StorageError::CompareFailed);
        }
        self.put_leased_cas(&key, member, member.lease_id, None)
            .await
    }

    pub async fn compare_and_put_member(
        &self,
        expected: &MemberRecord,
        member: &MemberRecord,
    ) -> Result<(), StorageError> {
        validate_member_record(member)?;
        if expected.node.node_id != member.node.node_id
            || expected.node.incarnation != member.node.incarnation
        {
            return Err(StorageError::CompareFailed);
        }
        let key = self.key(&format!("members/{}", member.node.node_id));
        let Some((bytes, mod_revision)) = self.read_raw(&key).await? else {
            return Err(StorageError::CompareFailed);
        };
        if decode::<MemberRecord>(&bytes)? != *expected {
            return Err(StorageError::CompareFailed);
        }
        self.put_leased_cas(&key, member, member.lease_id, Some(mod_revision))
            .await
    }

    pub async fn compare_and_delete_member(
        &self,
        expected: &MemberRecord,
    ) -> Result<(), StorageError> {
        let key = self.key(&format!("members/{}", expected.node.node_id));
        let Some((bytes, mod_revision)) = self.read_raw(&key).await? else {
            return Err(StorageError::CompareFailed);
        };
        if decode::<MemberRecord>(&bytes)? != *expected {
            return Err(StorageError::CompareFailed);
        }
        let mut client = self.client.clone();
        let response = client
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        mod_revision,
                    )])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await
            .map_err(map_etcd)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }

    pub async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        self.list_json("members/").await
    }

    pub async fn put_claim(&self, grant: &ClaimGrant, lease_id: i64) -> Result<(), StorageError> {
        let key = self.claim_key(&grant.slot);
        let current = self.read_raw(&key).await?;
        if let Some((bytes, _)) = &current {
            let claim: ClaimGrant = decode(bytes)?;
            if claim.assignment_generation > grant.assignment_generation
                || (claim.assignment_generation == grant.assignment_generation
                    && (claim.owner != grant.owner
                        || claim.coordinator_term > grant.coordinator_term
                        || claim.grant_sequence > grant.grant_sequence))
            {
                return Err(StorageError::CompareFailed);
            }
        }
        self.put_leased_cas(&key, grant, lease_id, current.map(|(_, revision)| revision))
            .await
    }

    pub async fn get_claim(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<ClaimGrant>, StorageError> {
        self.get_json_key(&self.claim_key(key)).await
    }

    pub async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError> {
        let mut shards = self.list_json("shards/").await?;
        shards.extend(self.list_json("singletons/").await?);
        if shards.len() > self.maximum_list_records {
            return Err(StorageError::Capacity);
        }
        Ok(shards)
    }

    pub async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError> {
        let envelopes: Vec<StoredPlan> = self.list_json("rebalances/").await?;
        Ok(envelopes.into_iter().map(|stored| stored.plan).collect())
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix)
    }

    fn slot_key(&self, key: &PlacementSlotKey) -> String {
        match key {
            PlacementSlotKey::Shard {
                entity_type,
                shard_id,
            } => self.key(&format!(
                "shards/{}/{}",
                entity_type.as_str(),
                shard_id.get()
            )),
            PlacementSlotKey::Singleton(kind) => self.key(&format!("singletons/{}", kind.as_str())),
        }
    }

    fn claim_key(&self, key: &PlacementSlotKey) -> String {
        match key {
            PlacementSlotKey::Shard {
                entity_type,
                shard_id,
            } => self.key(&format!(
                "shard_claims/{}/{}",
                entity_type.as_str(),
                shard_id.get()
            )),
            PlacementSlotKey::Singleton(kind) => {
                self.key(&format!("singleton_claims/{}", kind.as_str()))
            }
        }
    }

    async fn get_json<T: DeserializeOwned>(&self, suffix: &str) -> Result<Option<T>, StorageError> {
        self.get_json_key(&self.key(suffix)).await
    }

    async fn get_json_key<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        self.read_raw(key)
            .await?
            .map(|(bytes, _)| decode(&bytes))
            .transpose()
    }

    async fn list_json<T: DeserializeOwned>(&self, suffix: &str) -> Result<Vec<T>, StorageError> {
        let prefix = self.key(suffix);
        let limit = i64::try_from(self.maximum_list_records.saturating_add(1))
            .map_err(|_| StorageError::Capacity)?;
        let mut client = self.client.clone();
        let response = client
            .get(
                prefix,
                Some(GetOptions::new().with_prefix().with_limit(limit)),
            )
            .await
            .map_err(map_etcd)?;
        if response.kvs().len() > self.maximum_list_records || response.more() {
            return Err(StorageError::Capacity);
        }
        response
            .kvs()
            .iter()
            .map(|record| decode(record.value()))
            .collect()
    }

    async fn read_raw(&self, key: &str) -> Result<Option<(Vec<u8>, i64)>, StorageError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(map_etcd)?;
        Ok(response
            .kvs()
            .first()
            .map(|record| (record.value().to_vec(), record.mod_revision())))
    }

    async fn put_leased_cas<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        lease_id: i64,
        expected_mod_revision: Option<i64>,
    ) -> Result<(), StorageError> {
        let compare = match expected_mod_revision {
            Some(revision) => Compare::mod_revision(key, CompareOp::Equal, revision),
            None => Compare::version(key, CompareOp::Equal, 0),
        };
        let mut client = self.client.clone();
        let response = client
            .txn(Txn::new().when([compare]).and_then([TxnOp::put(
                key,
                encode(value)?,
                Some(PutOptions::new().with_lease(lease_id)),
            )]))
            .await
            .map_err(map_etcd)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }

    async fn put_cas<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        expected_mod_revision: Option<i64>,
    ) -> Result<(), StorageError> {
        let compare = match expected_mod_revision {
            Some(revision) => Compare::mod_revision(key, CompareOp::Equal, revision),
            None => Compare::version(key, CompareOp::Equal, 0),
        };
        let mut client = self.client.clone();
        let response = client
            .txn(
                Txn::new()
                    .when([compare])
                    .and_then([TxnOp::put(key, encode(value)?, None)]),
            )
            .await
            .map_err(map_etcd)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }
}

#[async_trait]
impl PlacementStore for EtcdPlacementStore {
    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        self.get_json_key(&self.slot_key(key)).await
    }

    async fn compare_and_put_slot(
        &self,
        expected_revision: Option<Revision>,
        slot: PlacementSlot,
    ) -> Result<(), StorageError> {
        slot.validate().map_err(|_| StorageError::InvalidRecord)?;
        let key = self.slot_key(&slot.key);
        let current = self.read_raw(&key).await?;
        let current_slot = current
            .as_ref()
            .map(|(bytes, _)| decode::<PlacementSlot>(bytes))
            .transpose()?;
        if current_slot.as_ref().map(|record| record.revision) != expected_revision {
            return Err(StorageError::CompareFailed);
        }
        self.put_cas(&key, &slot, current.map(|(_, revision)| revision))
            .await
    }

    async fn get_plan(&self, plan_id: u128) -> Result<Option<RebalancePlan>, StorageError> {
        self.get_json::<StoredPlan>(&format!("rebalances/{plan_id:032x}"))
            .await
            .map(|stored| stored.map(|value| value.plan))
    }

    async fn compare_and_put_plan(
        &self,
        expected_revision: Option<Revision>,
        plan: RebalancePlan,
        revision: Revision,
    ) -> Result<(), StorageError> {
        let key = self.key(&format!("rebalances/{:032x}", plan.plan_id));
        let current = self.read_raw(&key).await?;
        let current_plan = current
            .as_ref()
            .map(|(bytes, _)| decode::<StoredPlan>(bytes))
            .transpose()?;
        if current_plan.as_ref().map(|record| record.revision) != expected_revision {
            return Err(StorageError::CompareFailed);
        }
        self.put_cas(
            &key,
            &StoredPlan { revision, plan },
            current.map(|(_, mod_revision)| mod_revision),
        )
        .await
    }

    async fn delete_plan(
        &self,
        plan_id: u128,
        expected_revision: Revision,
    ) -> Result<(), StorageError> {
        let key = self.key(&format!("rebalances/{plan_id:032x}"));
        let Some((bytes, mod_revision)) = self.read_raw(&key).await? else {
            return Err(StorageError::CompareFailed);
        };
        if decode::<StoredPlan>(&bytes)?.revision != expected_revision {
            return Err(StorageError::CompareFailed);
        }
        let mut client = self.client.clone();
        let response = client
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        mod_revision,
                    )])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await
            .map_err(map_etcd)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }
}

#[async_trait]
impl CoordinatorStore for EtcdPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        EtcdPlacementStore::ensure_schema_generation(self).await
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        EtcdPlacementStore::grant_lease(self, ttl).await
    }

    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        EtcdPlacementStore::keep_lease_alive(self, lease_id).await
    }

    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        EtcdPlacementStore::revoke_lease(self, lease_id).await
    }

    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        EtcdPlacementStore::campaign_leader(self, leader, lease_id).await
    }

    async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError> {
        EtcdPlacementStore::get_leader(self).await
    }

    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        EtcdPlacementStore::get_member(self, node_id).await
    }

    async fn create_member(&self, member: &MemberRecord) -> Result<(), StorageError> {
        EtcdPlacementStore::create_member(self, member).await
    }

    async fn compare_and_put_member(
        &self,
        expected: &MemberRecord,
        member: &MemberRecord,
    ) -> Result<(), StorageError> {
        EtcdPlacementStore::compare_and_put_member(self, expected, member).await
    }

    async fn compare_and_delete_member(&self, expected: &MemberRecord) -> Result<(), StorageError> {
        EtcdPlacementStore::compare_and_delete_member(self, expected).await
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        EtcdPlacementStore::list_members(self).await
    }

    async fn put_claim(&self, grant: &ClaimGrant, lease_id: i64) -> Result<(), StorageError> {
        EtcdPlacementStore::put_claim(self, grant, lease_id).await
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<ClaimGrant>, StorageError> {
        EtcdPlacementStore::get_claim(self, key).await
    }

    async fn delete_claim(&self, expected: &ClaimGrant) -> Result<(), StorageError> {
        let key = self.claim_key(&expected.slot);
        let Some((bytes, mod_revision)) = self.read_raw(&key).await? else {
            return Err(StorageError::CompareFailed);
        };
        if decode::<ClaimGrant>(&bytes)? != *expected {
            return Err(StorageError::CompareFailed);
        }
        let mut client = self.client.clone();
        let response = client
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        mod_revision,
                    )])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await
            .map_err(map_etcd)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }

    async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError> {
        EtcdPlacementStore::list_slots(self).await
    }

    async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError> {
        EtcdPlacementStore::list_plans(self).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPlan {
    revision: Revision,
    plan: RebalancePlan,
}

fn validate_member_record(member: &MemberRecord) -> Result<(), StorageError> {
    if member.node != member.hello.node || member.lease_id == 0 || member.node.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), StorageError> {
    if !prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix.len() > 512
        || prefix.contains("//")
        || prefix.split('/').any(|segment| segment == "..")
        || prefix.chars().any(char::is_control)
    {
        return Err(StorageError::InvalidConfig);
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|_| StorageError::Codec)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    serde_json::from_slice(bytes).map_err(|_| StorageError::Codec)
}

fn map_etcd(_error: etcd_client::Error) -> StorageError {
    StorageError::Unavailable
}
