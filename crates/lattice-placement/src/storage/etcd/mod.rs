use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use serde::{Serialize, de::DeserializeOwned};

use super::domain::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CompleteMove,
    CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan, DurableStorageLimits,
    FenceAuthority, FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit,
    PlanCommit, RecordAdminOperation, RemoveMember, RemoveMemberWithOperation, ReserveHandoff,
    ReserveMove, SlotCommit, TransitionSlot, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};
use super::{CoordinatorStore, PlacementStore, StorageError};
use crate::coordinator::{LeaderGuard, LeaderRecord, MemberRecord};
use crate::plan::RebalancePlan;
use crate::types::{PlacementSlot, PlacementSlotKey, Revision};

pub mod migration;
mod transactions;

pub const STORAGE_SCHEMA_GENERATION: u64 = 4;

#[derive(Debug, Clone)]
pub struct EtcdPlacementConfig {
    pub endpoints: Vec<String>,
    pub cluster_prefix: String,
    pub list_page_size: usize,
    pub limits: DurableStorageLimits,
    pub connect_options: Option<ConnectOptions>,
}

impl EtcdPlacementConfig {
    pub fn validate(&self) -> Result<(), StorageError> {
        validate_prefix(&self.cluster_prefix)?;
        if self.endpoints.is_empty()
            || self.endpoints.len() > 16
            || self.list_page_size == 0
            || !self.limits.validate()
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
    pub(super) client: Client,
    pub(super) prefix: String,
    list_page_size: usize,
    pub(super) limits: DurableStorageLimits,
}

impl EtcdPlacementStore {
    pub async fn connect(config: EtcdPlacementConfig) -> Result<Self, StorageError> {
        config.validate()?;
        let client = Client::connect(&config.endpoints, config.connect_options)
            .await
            .map_err(map_etcd_read)?;
        Ok(Self {
            client,
            prefix: config.cluster_prefix,
            list_page_size: config.list_page_size,
            limits: config.limits,
        })
    }

    pub fn from_client(
        client: Client,
        cluster_prefix: impl Into<String>,
        list_page_size: usize,
        limits: DurableStorageLimits,
    ) -> Result<Self, StorageError> {
        let prefix = cluster_prefix.into();
        validate_prefix(&prefix)?;
        if list_page_size == 0 || !limits.validate() {
            return Err(StorageError::ZeroLimit);
        }
        Ok(Self {
            client,
            prefix,
            list_page_size,
            limits,
        })
    }

    async fn ensure_schema_generation_inner(&self) -> Result<(), StorageError> {
        let schema_key = self.key("schema_generation");
        let limits_key = self.key("schema/limits");
        let revision_key = self.key("coordinator/state_revision");
        let expected = STORAGE_SCHEMA_GENERATION.to_string().into_bytes();
        let expected_limits = encode(&self.limits)?;
        let counter_keys = [
            self.key("counters/slots"),
            self.key("counters/plans"),
            self.key("counters/members"),
            self.key("counters/admin_operations"),
        ];
        let schema = self.read_raw(&schema_key).await?;
        if let Some((bytes, _, _)) = &schema
            && bytes != &expected
        {
            return Err(StorageError::SchemaGenerationMismatch);
        }
        if schema.is_none() {
            let mut client = self.client.clone();
            let response = client
                .get(
                    self.key(""),
                    Some(GetOptions::new().with_prefix().with_limit(1)),
                )
                .await
                .map_err(map_etcd_read)?;
            if !response.kvs().is_empty() {
                return Err(StorageError::SchemaGenerationMismatch);
            }
        } else {
            if self
                .read_raw(&limits_key)
                .await?
                .is_none_or(|(value, _, _)| value != expected_limits)
            {
                return Err(StorageError::SchemaGenerationMismatch);
            }
            for key in &counter_keys {
                if self.read_raw(key).await?.is_none() {
                    return Err(StorageError::SchemaGenerationMismatch);
                }
            }
            if self.read_raw(&revision_key).await?.is_none() {
                return Err(StorageError::SchemaGenerationMismatch);
            }
        }
        let mut compares = Vec::new();
        let mut puts = Vec::new();
        if schema.is_none() {
            compares.push(Compare::version(schema_key.clone(), CompareOp::Equal, 0));
            puts.push(TxnOp::put(schema_key.clone(), expected.clone(), None));
            compares.push(Compare::version(limits_key.clone(), CompareOp::Equal, 0));
            puts.push(TxnOp::put(
                limits_key.clone(),
                expected_limits.clone(),
                None,
            ));
            for key in &counter_keys {
                compares.push(Compare::version(key.clone(), CompareOp::Equal, 0));
                puts.push(TxnOp::put(key.clone(), "0", None));
            }
            compares.push(Compare::version(revision_key.clone(), CompareOp::Equal, 0));
            puts.push(TxnOp::put(revision_key, "1", None));
        }
        if compares.is_empty() {
            return Ok(());
        }
        let mut client = self.client.clone();
        let response = client
            .txn(Txn::new().when(compares).and_then(puts))
            .await
            .map_err(map_etcd_txn)?;
        if response.succeeded() {
            return Ok(());
        }
        let schema = self.read_raw(&schema_key).await?;
        let mut counters_present = true;
        for key in &counter_keys {
            counters_present &= self.read_raw(key).await?.is_some();
        }
        if schema
            .as_ref()
            .is_some_and(|(bytes, _, _)| bytes == &expected)
            && self
                .read_raw(&self.key("coordinator/state_revision"))
                .await?
                .is_some()
            && self
                .read_raw(&limits_key)
                .await?
                .is_some_and(|(value, _, _)| value == expected_limits)
            && counters_present
        {
            Ok(())
        } else {
            Err(StorageError::SchemaGenerationMismatch)
        }
    }

    async fn grant_lease_inner(&self, ttl: Duration) -> Result<i64, StorageError> {
        let seconds = i64::try_from(ttl.as_secs()).map_err(|_| StorageError::InvalidConfig)?;
        if seconds == 0 {
            return Err(StorageError::InvalidConfig);
        }
        let mut client = self.client.clone();
        client
            .lease_grant(seconds, None)
            .await
            .map(|response| response.id())
            .map_err(map_etcd_read)
    }

    async fn keep_lease_alive_inner(&self, lease_id: i64) -> Result<(), StorageError> {
        if lease_id <= 0 {
            return Err(StorageError::InvalidConfig);
        }
        let mut client = self.client.clone();
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lease_id)
            .await
            .map_err(map_etcd_read)?;
        keeper.keep_alive().await.map_err(map_etcd_read)?;
        stream
            .message()
            .await
            .map_err(map_etcd_read)?
            .filter(|response| response.ttl() > 0)
            .ok_or(StorageError::Unavailable)
            .map(|_| ())
    }

    async fn revoke_lease_inner(&self, lease_id: i64) -> Result<(), StorageError> {
        let mut client = self.client.clone();
        client
            .lease_revoke(lease_id)
            .await
            .map(|_| ())
            .map_err(map_etcd_read)
    }

    async fn campaign_leader_inner(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        leader
            .node
            .validate()
            .map_err(|_| StorageError::InvalidRecord)?;
        if lease_id <= 0 {
            return Err(StorageError::InvalidRecord);
        }
        let leader_key = self.key("coordinator/leader");
        let term_key = self.key("coordinator/term");
        let current_term = self.read_raw(&term_key).await?;
        let term = current_term
            .as_ref()
            .map(|(bytes, _, _)| parse_revision_value(bytes))
            .transpose()?
            .map(Revision::get)
            .unwrap_or(0);
        let expected = term.checked_add(1).ok_or(StorageError::CounterExhausted)?;
        if leader.term.get() != expected {
            return Err(StorageError::CompareFailed);
        }
        let term_compare = match current_term {
            Some((_, revision, _)) => {
                Compare::mod_revision(term_key.clone(), CompareOp::Equal, revision)
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
                            Some(etcd_client::PutOptions::new().with_lease(lease_id)),
                        ),
                    ]),
            )
            .await
            .map(|response| response.succeeded())
            .map_err(map_etcd_txn)
    }

    pub(super) fn key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix)
    }

    pub(super) fn slot_key(&self, key: &PlacementSlotKey) -> String {
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

    pub(super) fn claim_key(&self, key: &PlacementSlotKey) -> String {
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

    pub(super) fn plan_key(&self, plan_id: u128) -> String {
        self.key(&format!("rebalances/{plan_id:032x}"))
    }

    pub(super) fn operation_key(&self, operation_id: &str) -> String {
        let encoded = operation_id
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.key(&format!("operations/{encoded}"))
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
            .map(|(bytes, _, _)| decode(&bytes))
            .transpose()
    }

    async fn list_raw_bounded(
        &self,
        suffix: &str,
        total_limit: usize,
    ) -> Result<Vec<(Vec<u8>, i64)>, StorageError> {
        let prefix = self.key(suffix).into_bytes();
        let end = prefix_range_end(prefix.clone())?;
        let page_limit = i64::try_from(self.list_page_size).map_err(|_| StorageError::Capacity)?;
        let mut start = prefix;
        let mut records = Vec::new();
        loop {
            let mut client = self.client.clone();
            let response = client
                .get(
                    start.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(end.clone())
                            .with_limit(page_limit)
                            .with_sort(SortTarget::Key, SortOrder::Ascend),
                    ),
                )
                .await
                .map_err(map_etcd_read)?;
            records.extend(
                response
                    .kvs()
                    .iter()
                    .map(|record| (record.value().to_vec(), record.lease())),
            );
            if records.len() > total_limit {
                return Err(StorageError::Capacity);
            }
            if !response.more() {
                break;
            }
            let Some(last) = response.kvs().last() else {
                return Err(StorageError::Codec);
            };
            start = last.key().to_vec();
            start.push(0);
        }
        Ok(records)
    }

    async fn list_json<T: DeserializeOwned>(
        &self,
        suffix: &str,
        total_limit: usize,
    ) -> Result<Vec<T>, StorageError> {
        self.list_raw_bounded(suffix, total_limit)
            .await?
            .into_iter()
            .map(|(value, _)| decode(&value))
            .collect()
    }

    async fn list_claims_suffix(&self, suffix: &str) -> Result<Vec<LeasedClaim>, StorageError> {
        self.list_raw_bounded(suffix, self.limits.maximum_slots)
            .await?
            .into_iter()
            .map(|(value, lease_id)| {
                Ok(LeasedClaim {
                    grant: decode(&value)?,
                    lease_id,
                })
            })
            .collect()
    }

    pub(super) async fn read_raw(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, i64, i64)>, StorageError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(map_etcd_read)?;
        Ok(response.kvs().first().map(|record| {
            (
                record.value().to_vec(),
                record.mod_revision(),
                record.lease(),
            )
        }))
    }
}

#[async_trait]
impl PlacementStore for EtcdPlacementStore {
    fn durable_limits(&self) -> DurableStorageLimits {
        self.limits
    }

    async fn get_state_revision(&self) -> Result<Revision, StorageError> {
        let Some((bytes, _, _)) = self
            .read_raw(&self.key("coordinator/state_revision"))
            .await?
        else {
            return Ok(Revision::new(1).expect("one is a valid state revision"));
        };
        parse_revision_value(&bytes)
    }

    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        self.get_json_key(&self.slot_key(key)).await
    }

    async fn get_plan(&self, plan_id: u128) -> Result<Option<RebalancePlan>, StorageError> {
        self.get_json_key(&self.plan_key(plan_id)).await
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        self.read_raw(&self.claim_key(key))
            .await?
            .map(|(bytes, _, lease_id)| {
                Ok(LeasedClaim {
                    grant: decode(&bytes)?,
                    lease_id,
                })
            })
            .transpose()
    }

    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        self.get_json(&format!("members/{node_id}")).await
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        self.list_json("members/", self.limits.maximum_members)
            .await
    }

    async fn list_slots(&self) -> Result<Vec<PlacementSlot>, StorageError> {
        let mut slots = self.list_json("shards/", self.limits.maximum_slots).await?;
        slots.extend(
            self.list_json("singletons/", self.limits.maximum_slots)
                .await?,
        );
        if slots.len() > self.limits.maximum_slots {
            return Err(StorageError::Capacity);
        }
        Ok(slots)
    }

    async fn list_plans(&self) -> Result<Vec<RebalancePlan>, StorageError> {
        self.list_json("rebalances/", self.limits.maximum_plans)
            .await
    }

    async fn list_claims(&self) -> Result<Vec<LeasedClaim>, StorageError> {
        let mut claims = self.list_claims_suffix("shard_claims/").await?;
        claims.extend(self.list_claims_suffix("singleton_claims/").await?);
        if claims.len() > self.limits.maximum_slots {
            return Err(StorageError::Capacity);
        }
        Ok(claims)
    }

    async fn get_automatic_settings(
        &self,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        self.get_json("settings/automatic_balance").await
    }

    async fn get_admin_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        self.get_json_key(&self.operation_key(operation_id)).await
    }

    async fn list_admin_operations(&self) -> Result<Vec<AdminOperationRecord>, StorageError> {
        self.list_json("operations/", self.limits.maximum_admin_operations)
            .await
    }
}

#[async_trait]
impl CoordinatorStore for EtcdPlacementStore {
    async fn ensure_schema_generation(&self) -> Result<(), StorageError> {
        self.ensure_schema_generation_inner().await
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<i64, StorageError> {
        self.grant_lease_inner(ttl).await
    }

    async fn keep_lease_alive(&self, lease_id: i64) -> Result<(), StorageError> {
        self.keep_lease_alive_inner(lease_id).await
    }

    async fn revoke_lease(&self, lease_id: i64) -> Result<(), StorageError> {
        self.revoke_lease_inner(lease_id).await
    }

    async fn lease_time_to_live(&self, lease_id: i64) -> Result<Option<Duration>, StorageError> {
        let mut client = self.client.clone();
        let response = client
            .lease_time_to_live(lease_id, None)
            .await
            .map_err(map_etcd_read)?;
        if response.ttl() <= 0 {
            Ok(None)
        } else {
            Ok(Some(Duration::from_secs(
                u64::try_from(response.ttl()).map_err(|_| StorageError::InvalidRecord)?,
            )))
        }
    }

    async fn campaign_leader(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        self.campaign_leader_inner(leader, lease_id).await
    }

    async fn get_leader(&self) -> Result<Option<LeaderRecord>, StorageError> {
        self.get_json("coordinator/leader").await
    }

    async fn create_member(
        &self,
        guard: &LeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::create_member(self, guard, request).await
    }

    async fn update_member(
        &self,
        guard: &LeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::update_member(self, guard, request).await
    }

    async fn remove_member(
        &self,
        guard: &LeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::remove_member(self, guard, request).await
    }

    async fn create_plan(
        &self,
        guard: &LeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::create_plan(self, guard, request).await
    }

    async fn update_plan(
        &self,
        guard: &LeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::update_plan(self, guard, request).await
    }

    async fn delete_plan(
        &self,
        guard: &LeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::delete_plan(self, guard, request).await
    }

    async fn transition_slot(
        &self,
        guard: &LeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        transactions::transition_slot(self, guard, request).await
    }

    async fn allocate_initial(
        &self,
        guard: &LeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::allocate_initial(self, guard, request).await
    }

    async fn activate_authority(
        &self,
        guard: &LeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::activate_authority(self, guard, request).await
    }

    async fn reserve_move(
        &self,
        guard: &LeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        transactions::reserve_move(self, guard, request).await
    }

    async fn reserve_handoff(
        &self,
        guard: &LeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        transactions::reserve_handoff(self, guard, request).await
    }

    async fn fence_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::fence_authority(self, guard, request).await
    }

    async fn fence_missing_authority(
        &self,
        guard: &LeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::fence_missing_authority(self, guard, request).await
    }

    async fn install_authority(
        &self,
        guard: &LeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::install_authority(self, guard, request).await
    }

    async fn adopt_authority(
        &self,
        guard: &LeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::adopt_authority(self, guard, request).await
    }

    async fn complete_move(
        &self,
        guard: &LeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        transactions::complete_move(self, guard, request).await
    }

    async fn commit_automatic_settings(
        &self,
        guard: &LeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        transactions::commit_automatic_settings(self, guard, request).await
    }

    async fn create_plan_with_operation(
        &self,
        guard: &LeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        transactions::create_plan_with_operation(self, guard, request).await
    }

    async fn update_plan_with_operation(
        &self,
        guard: &LeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        transactions::update_plan_with_operation(self, guard, request).await
    }

    async fn remove_member_with_operation(
        &self,
        guard: &LeaderGuard,
        request: RemoveMemberWithOperation,
    ) -> Result<MemberCommit, StorageError> {
        transactions::remove_member_with_operation(self, guard, request).await
    }

    async fn record_admin_operation(
        &self,
        guard: &LeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        transactions::record_admin_operation(self, guard, request).await
    }

    async fn compact_admin_operations(
        &self,
        guard: &LeaderGuard,
        request: CompactAdminOperations,
    ) -> Result<(), StorageError> {
        transactions::compact_admin_operations(self, guard, request).await
    }
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

fn prefix_range_end(mut prefix: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    let Some(last) = prefix.last_mut() else {
        return Err(StorageError::InvalidConfig);
    };
    *last = last.checked_add(1).ok_or(StorageError::InvalidConfig)?;
    Ok(prefix)
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(value).map_err(|_| StorageError::Codec)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    serde_json::from_slice(bytes).map_err(|_| StorageError::Codec)
}

pub(super) fn parse_revision_value(bytes: &[u8]) -> Result<Revision, StorageError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|value| Revision::new(value).ok())
        .ok_or(StorageError::Codec)
}

pub(super) fn map_etcd_read(error: etcd_client::Error) -> StorageError {
    match error {
        etcd_client::Error::InvalidArgs(_)
        | etcd_client::Error::InvalidUri(_)
        | etcd_client::Error::InvalidMetadataValue(_) => StorageError::BackendArgument,
        etcd_client::Error::GRpcStatus(status) => match status.code() as i32 {
            3 => StorageError::BackendArgument,
            4 => StorageError::Deadline,
            7 | 16 => StorageError::Authentication,
            _ => StorageError::Unavailable,
        },
        _ => StorageError::Unavailable,
    }
}

pub(super) fn map_etcd_txn(error: etcd_client::Error) -> StorageError {
    match error {
        etcd_client::Error::InvalidArgs(_)
        | etcd_client::Error::InvalidUri(_)
        | etcd_client::Error::InvalidMetadataValue(_) => StorageError::BackendArgument,
        etcd_client::Error::GRpcStatus(status) => match status.code() as i32 {
            3 => StorageError::BackendArgument,
            4 => StorageError::OutcomeUnknown,
            7 | 16 => StorageError::Authentication,
            _ => StorageError::OutcomeUnknown,
        },
        _ => StorageError::OutcomeUnknown,
    }
}
