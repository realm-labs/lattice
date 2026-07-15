use std::time::Duration;

use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use lattice_core::actor_ref::PlacementDomainId;
use lattice_core::coordinator::CoordinatorScope;
use serde::{Serialize, de::DeserializeOwned};

use super::StorageError;
use super::domain::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CompleteMove,
    CreateDomainMember, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
    DomainMemberCommit, DurableStorageLimits, EntityConfigCommit, FenceAuthority,
    FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
    PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveDomainMember, RemoveMember,
    ReserveHandoff, ReserveMove, SingletonConfigCommit, SlotCommit, TransitionSlot,
    UpdateDomainMember, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};
use crate::coordinator::{
    DomainMemberRecord, LeaderRecord, MemberRecord, MembershipLeaderGuard, PlacementLeaderGuard,
};
use crate::plan::RebalancePlan;
use crate::types::{PlacementSlot, PlacementSlotKey, Revision};

pub mod migration;
mod traits;
mod transactions;

pub const STORAGE_SCHEMA_GENERATION: u64 = 5;

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
        let revision_key = self.key("membership/state_revision");
        let expected = STORAGE_SCHEMA_GENERATION.to_string().into_bytes();
        let expected_limits = encode(&self.limits)?;
        let counter_keys = [self.key("membership/counters/members")];
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
                .read_raw(&self.key("membership/state_revision"))
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
        leader.validate().map_err(|_| StorageError::InvalidRecord)?;
        if lease_id <= 0 {
            return Err(StorageError::InvalidRecord);
        }
        let leader_key = self.scope_key(&leader.scope, "leader");
        let term_key = self.scope_key(&leader.scope, "term");
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

    pub(super) fn scope_key(&self, scope: &CoordinatorScope, suffix: &str) -> String {
        match scope {
            CoordinatorScope::Membership => self.key(&format!("membership/{suffix}")),
            CoordinatorScope::Placement(domain) => {
                self.key(&format!("domains/{}/{suffix}", domain.as_str()))
            }
        }
    }

    pub(super) fn slot_key(&self, key: &PlacementSlotKey) -> String {
        match key {
            PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } => self.key(&format!(
                "domains/{}/shards/{}/{}",
                domain.as_str(),
                entity_type.as_str(),
                shard_id.get()
            )),
            PlacementSlotKey::Singleton { domain, kind } => self.key(&format!(
                "domains/{}/singletons/{}",
                domain.as_str(),
                kind.as_str()
            )),
        }
    }

    pub(super) fn claim_key(&self, key: &PlacementSlotKey) -> String {
        match key {
            PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } => self.key(&format!(
                "domains/{}/shard_claims/{}/{}",
                domain.as_str(),
                entity_type.as_str(),
                shard_id.get()
            )),
            PlacementSlotKey::Singleton { domain, kind } => self.key(&format!(
                "domains/{}/singleton_claims/{}",
                domain.as_str(),
                kind.as_str()
            )),
        }
    }

    pub(super) fn plan_key(&self, domain: &PlacementDomainId, plan_id: u128) -> String {
        self.key(&format!(
            "domains/{}/rebalances/{plan_id:032x}",
            domain.as_str()
        ))
    }

    pub(super) fn domain_member_key(&self, domain: &PlacementDomainId, node_id: &str) -> String {
        self.key(&format!("domains/{}/members/{node_id}", domain.as_str()))
    }

    pub(super) fn entity_config_key(
        &self,
        domain: &PlacementDomainId,
        entity_type: &lattice_core::actor_ref::EntityType,
    ) -> String {
        self.key(&format!(
            "domains/{}/entity_types/{}",
            domain.as_str(),
            entity_type.as_str()
        ))
    }

    pub(super) fn singleton_config_key(
        &self,
        domain: &PlacementDomainId,
        kind: &lattice_core::actor_ref::SingletonKind,
    ) -> String {
        self.key(&format!(
            "domains/{}/singleton_types/{}",
            domain.as_str(),
            kind.as_str()
        ))
    }

    pub(super) fn operation_key(&self, domain: &PlacementDomainId, operation_id: &str) -> String {
        let encoded = operation_id
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.key(&format!("domains/{}/admin/{encoded}", domain.as_str()))
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

impl EtcdPlacementStore {
    fn durable_limits_inner(&self) -> DurableStorageLimits {
        self.limits
    }

    async fn get_membership_revision_inner(&self) -> Result<Revision, StorageError> {
        let Some((bytes, _, _)) = self
            .read_raw(&self.key("membership/state_revision"))
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

    async fn get_plan(
        &self,
        domain: &PlacementDomainId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        self.get_json_key(&self.plan_key(domain, plan_id)).await
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
        self.get_json(&format!("membership/members/{node_id}"))
            .await
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        self.list_json("membership/members/", self.limits.maximum_members)
            .await
    }

    async fn get_domain_member(
        &self,
        domain: &PlacementDomainId,
        node_id: &str,
    ) -> Result<Option<DomainMemberRecord>, StorageError> {
        self.get_json_key(&self.domain_member_key(domain, node_id))
            .await
    }

    async fn list_domain_members(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<DomainMemberRecord>, StorageError> {
        self.list_json(
            &format!("domains/{}/members/", domain.as_str()),
            self.limits.maximum_members,
        )
        .await
    }

    async fn list_slots(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<PlacementSlot>, StorageError> {
        let mut slots = self
            .list_json(
                &format!("domains/{}/shards/", domain.as_str()),
                self.limits.maximum_slots,
            )
            .await?;
        slots.extend(
            self.list_json(
                &format!("domains/{}/singletons/", domain.as_str()),
                self.limits.maximum_slots,
            )
            .await?,
        );
        if slots.len() > self.limits.maximum_slots {
            return Err(StorageError::Capacity);
        }
        Ok(slots)
    }

    async fn list_plans(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<RebalancePlan>, StorageError> {
        self.list_json(
            &format!("domains/{}/rebalances/", domain.as_str()),
            self.limits.maximum_plans,
        )
        .await
    }

    async fn list_claims(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<LeasedClaim>, StorageError> {
        let mut claims = self
            .list_claims_suffix(&format!("domains/{}/shard_claims/", domain.as_str()))
            .await?;
        claims.extend(
            self.list_claims_suffix(&format!("domains/{}/singleton_claims/", domain.as_str()))
                .await?,
        );
        if claims.len() > self.limits.maximum_slots {
            return Err(StorageError::Capacity);
        }
        Ok(claims)
    }

    async fn get_automatic_settings(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        self.get_json(&format!(
            "domains/{}/settings/automatic_balance",
            domain.as_str()
        ))
        .await
    }

    async fn get_admin_operation(
        &self,
        domain: &PlacementDomainId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        self.get_json_key(&self.operation_key(domain, operation_id))
            .await
    }

    async fn list_admin_operations(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError> {
        self.list_json(
            &format!("domains/{}/admin/", domain.as_str()),
            self.limits.maximum_admin_operations,
        )
        .await
    }
}

impl EtcdPlacementStore {
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

    async fn get_leader_inner(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Option<LeaderRecord>, StorageError> {
        self.get_json_key(&self.scope_key(scope, "leader")).await
    }

    async fn get_leader_term_inner(&self, scope: &CoordinatorScope) -> Result<u64, StorageError> {
        self.read_raw(&self.scope_key(scope, "term"))
            .await?
            .map(|(bytes, _, _)| parse_revision_value(&bytes).map(Revision::get))
            .transpose()
            .map(|term| term.unwrap_or(0))
    }

    async fn create_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::create_member(self, guard, request).await
    }

    async fn update_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::update_member(self, guard, request).await
    }

    async fn remove_member(
        &self,
        guard: &MembershipLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::remove_member(self, guard, request).await
    }

    async fn create_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        transactions::create_domain_member(self, guard, request).await
    }

    async fn update_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdateDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        transactions::update_domain_member(self, guard, request).await
    }

    async fn remove_domain_member(
        &self,
        guard: &PlacementLeaderGuard,
        request: RemoveDomainMember,
    ) -> Result<DomainMemberCommit, StorageError> {
        transactions::remove_domain_member(self, guard, request).await
    }

    async fn put_entity_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError> {
        transactions::put_entity_config(self, guard, request).await
    }

    async fn put_singleton_config(
        &self,
        guard: &PlacementLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError> {
        transactions::put_singleton_config(self, guard, request).await
    }

    async fn create_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::create_plan(self, guard, request).await
    }

    async fn update_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::update_plan(self, guard, request).await
    }

    async fn delete_plan(
        &self,
        guard: &PlacementLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::delete_plan(self, guard, request).await
    }

    async fn transition_slot(
        &self,
        guard: &PlacementLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        transactions::transition_slot(self, guard, request).await
    }

    async fn allocate_initial(
        &self,
        guard: &PlacementLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::allocate_initial(self, guard, request).await
    }

    async fn activate_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::activate_authority(self, guard, request).await
    }

    async fn reserve_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        transactions::reserve_move(self, guard, request).await
    }

    async fn reserve_handoff(
        &self,
        guard: &PlacementLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        transactions::reserve_handoff(self, guard, request).await
    }

    async fn fence_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::fence_authority(self, guard, request).await
    }

    async fn fence_missing_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::fence_missing_authority(self, guard, request).await
    }

    async fn install_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::install_authority(self, guard, request).await
    }

    async fn adopt_authority(
        &self,
        guard: &PlacementLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::adopt_authority(self, guard, request).await
    }

    async fn complete_move(
        &self,
        guard: &PlacementLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        transactions::complete_move(self, guard, request).await
    }

    async fn commit_automatic_settings(
        &self,
        guard: &PlacementLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        transactions::commit_automatic_settings(self, guard, request).await
    }

    async fn create_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        transactions::create_plan_with_operation(self, guard, request).await
    }

    async fn update_plan_with_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        transactions::update_plan_with_operation(self, guard, request).await
    }

    async fn record_admin_operation(
        &self,
        guard: &PlacementLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        transactions::record_admin_operation(self, guard, request).await
    }

    async fn compact_admin_operations(
        &self,
        guard: &PlacementLeaderGuard,
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
