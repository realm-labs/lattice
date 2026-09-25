use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};

use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use lattice_model::run::RunEpoch;
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, EntityType, SingletonKind},
};
use serde::{Serialize, de::DeserializeOwned};

use super::{
    ClusterLifecycleStore, StorageError,
    records::{
        ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
        AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations, CompleteMove,
        CreateGroupMember, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan,
        DurableStorageLimits, EntityConfigCommit, FenceAuthority, FenceMissingAuthority,
        GroupMemberCommit, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
        PutEntityConfig, PutSingletonConfig, RecordAdminOperation, RemoveExpiredMember,
        RemoveGroupMember, RemoveMember, ReserveHandoff, ReserveMove, SingletonConfigCommit,
        SlotCommit, TransitionSlot, UpdateGroupMember, UpdateMember, UpdatePlan,
        UpdatePlanWithOperation,
    },
};
use crate::{
    coordinator::{
        ClusterLeaderGuard, GroupLeaderGuard, GroupMemberRecord, LeaderRecord, MemberRecord,
    },
    plan::RebalancePlan,
    types::{PlacementSlot, PlacementSlotKey, Revision},
};

mod barrier;
mod barrier_gc;
mod candidates;
mod lifecycle;
mod maintenance;
mod page;
mod plan_records;
mod shutdown;
mod traits;
mod transactions;
mod transfer_capacity;

pub const MAX_STORE_PAGE_RECORDS: usize = 256;

pub const DEFAULT_ETCD_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct EtcdCoordinationConfig {
    pub endpoints: Vec<String>,
    pub cluster_prefix: String,
    pub list_page_size: usize,
    pub limits: DurableStorageLimits,
    pub connect_options: Option<ConnectOptions>,
}

impl EtcdCoordinationConfig {
    pub fn validate(&self) -> Result<(), StorageError> {
        validate_prefix(&self.cluster_prefix)?;
        if self.endpoints.is_empty()
            || self.endpoints.len() > 16
            || self.list_page_size == 0
            || self.list_page_size > MAX_STORE_PAGE_RECORDS
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
pub struct EtcdCoordinationStore {
    pub(super) client: Client,
    pub(super) prefix: String,
    list_page_size: usize,
    pub(super) limits: DurableStorageLimits,
    operation_timeout: Duration,
    epoch: Arc<OnceLock<RunEpoch>>,
}

impl EtcdCoordinationStore {
    pub async fn connect(config: EtcdCoordinationConfig) -> Result<Self, StorageError> {
        config.validate()?;
        let client = run_read_deadline(
            DEFAULT_ETCD_OPERATION_TIMEOUT,
            Client::connect(&config.endpoints, config.connect_options),
        )
        .await?;
        Ok(Self {
            client,
            prefix: config.cluster_prefix,
            list_page_size: config.list_page_size,
            limits: config.limits,
            operation_timeout: DEFAULT_ETCD_OPERATION_TIMEOUT,
            epoch: Arc::new(OnceLock::new()),
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
        if list_page_size == 0 || list_page_size > MAX_STORE_PAGE_RECORDS || !limits.validate() {
            return Err(StorageError::ZeroLimit);
        }
        Ok(Self {
            client,
            prefix,
            list_page_size,
            limits,
            operation_timeout: DEFAULT_ETCD_OPERATION_TIMEOUT,
            epoch: Arc::new(OnceLock::new()),
        })
    }

    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, StorageError> {
        if timeout.is_zero() {
            return Err(StorageError::InvalidConfig);
        }
        self.operation_timeout = timeout;
        Ok(self)
    }

    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub(super) async fn read_deadline<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        F: Future<Output = Result<T, etcd_client::Error>>,
    {
        run_read_deadline(self.operation_timeout, operation).await
    }

    pub(super) async fn write_deadline<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        F: Future<Output = Result<T, etcd_client::Error>>,
    {
        run_write_deadline(self.operation_timeout, operation).await
    }

    async fn grant_lease_inner(&self, ttl: Duration) -> Result<i64, StorageError> {
        let seconds = i64::try_from(ttl.as_secs()).map_err(|_| StorageError::InvalidConfig)?;
        if seconds == 0 {
            return Err(StorageError::InvalidConfig);
        }
        let mut client = self.client.clone();
        self.write_deadline(client.lease_grant(seconds, None))
            .await
            .map(|response| response.id())
    }

    async fn keep_lease_alive_inner(&self, lease_id: i64) -> Result<(), StorageError> {
        if lease_id <= 0 {
            return Err(StorageError::InvalidConfig);
        }
        tokio::time::timeout(self.operation_timeout, async {
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
        })
        .await
        .map_err(|_| StorageError::Deadline)?
    }

    async fn revoke_lease_inner(&self, lease_id: i64) -> Result<(), StorageError> {
        let mut client = self.client.clone();
        self.write_deadline(client.lease_revoke(lease_id))
            .await
            .map(|_| ())
    }

    async fn campaign_leader_inner(
        &self,
        leader: &LeaderRecord,
        lease_id: i64,
    ) -> Result<bool, StorageError> {
        leader.validate().map_err(|_| StorageError::InvalidRecord)?;
        if self.bound_epoch()? != leader.epoch {
            return Err(StorageError::RunMismatch);
        }
        if lease_id <= 0 {
            return Err(StorageError::InvalidRecord);
        }
        let lifecycle = self.lifecycle().await?;
        if lifecycle.epoch != leader.epoch {
            return Err(StorageError::RunMismatch);
        }
        if !lifecycle.permits_election() {
            return Err(StorageError::RunNotRunning);
        }
        let registration = leader.candidate_registration();
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
        self.write_deadline(
            client.txn(
                Txn::new()
                    .when([
                        Compare::value(
                            self.candidate_authorization_key(&leader.scope, &leader.node.node_id),
                            CompareOp::Equal,
                            encode(&registration.authorization)?,
                        ),
                        Compare::value(
                            self.candidate_registration_key(&leader.scope, &leader.node.node_id),
                            CompareOp::Equal,
                            encode(&registration)?,
                        ),
                        Compare::version(leader_key.clone(), CompareOp::Equal, 0),
                        term_compare,
                        Compare::value(
                            self.key("meta/lifecycle"),
                            CompareOp::Equal,
                            encode(&lifecycle)?,
                        ),
                    ])
                    .and_then([
                        TxnOp::put(term_key, expected.to_string(), None),
                        TxnOp::put(
                            leader_key,
                            encode(leader)?,
                            Some(etcd_client::PutOptions::new().with_lease(lease_id)),
                        ),
                    ]),
            ),
        )
        .await
        .map(|response| response.succeeded())
    }

    pub(super) fn root_key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix)
    }

    pub(super) fn run_key(&self, epoch: RunEpoch, suffix: &str) -> String {
        self.root_key(&format!("runs/{}/{suffix}", epoch.get()))
    }

    pub(super) fn bound_epoch(&self) -> Result<RunEpoch, StorageError> {
        self.epoch
            .get()
            .copied()
            .ok_or(StorageError::RunNotInitialized)
    }

    pub(super) fn bind_epoch(&self, epoch: RunEpoch) -> Result<RunEpoch, StorageError> {
        let bound = *self.epoch.get_or_init(|| epoch);
        if bound != epoch {
            return Err(StorageError::RunMismatch);
        }
        Ok(bound)
    }

    // Public store operations validate initialization before reaching key construction.
    // The binding is immutable: an old store can never silently follow a new run.
    pub(super) fn key(&self, suffix: &str) -> String {
        if suffix.starts_with("meta/") || suffix.starts_with("definitions/") {
            return self.root_key(suffix);
        }
        self.run_key(
            self.bound_epoch()
                .expect("store operation validated run initialization"),
            suffix,
        )
    }

    pub(super) fn scope_key(&self, scope: &CoordinatorScope, suffix: &str) -> String {
        if matches!(
            suffix,
            "counters/entity_configs" | "counters/singleton_configs"
        ) {
            if let CoordinatorScope::Group(group) = scope {
                return self.root_key(&format!("definitions/groups/{}/{suffix}", group.as_str()));
            }
        }
        if suffix == "term" {
            return match scope {
                CoordinatorScope::Cluster => self.root_key("meta/terms/cluster"),
                CoordinatorScope::Group(group) => {
                    self.root_key(&format!("meta/terms/groups/{}", group.as_str()))
                }
            };
        }
        match scope {
            CoordinatorScope::Cluster => self.key(&format!("membership/{suffix}")),
            CoordinatorScope::Group(group) => {
                self.key(&format!("groups/{}/{suffix}", group.as_str()))
            }
        }
    }

    pub(super) fn slot_key(&self, key: &PlacementSlotKey) -> String {
        match key {
            PlacementSlotKey::Shard {
                group,
                entity_type,
                shard_id,
            } => self.key(&format!(
                "groups/{}/shards/{}/{}",
                group.as_str(),
                entity_type.as_str(),
                shard_id.get()
            )),
            PlacementSlotKey::Singleton { group, kind } => self.key(&format!(
                "groups/{}/singletons/{}",
                group.as_str(),
                kind.as_str()
            )),
        }
    }

    pub(super) fn claim_key(&self, key: &PlacementSlotKey) -> String {
        match key {
            PlacementSlotKey::Shard {
                group,
                entity_type,
                shard_id,
            } => self.key(&format!(
                "groups/{}/shard_claims/{}/{}",
                group.as_str(),
                entity_type.as_str(),
                shard_id.get()
            )),
            PlacementSlotKey::Singleton { group, kind } => self.key(&format!(
                "groups/{}/singleton_claims/{}",
                group.as_str(),
                kind.as_str()
            )),
        }
    }

    pub(super) fn plan_key(&self, group: &ActorGroupId, plan_id: u128) -> String {
        self.key(&format!(
            "groups/{}/rebalances/{plan_id:032x}",
            group.as_str()
        ))
    }

    pub(super) fn group_member_key(&self, group: &ActorGroupId, node_id: &str) -> String {
        self.key(&format!("groups/{}/members/{node_id}", group.as_str()))
    }

    pub(super) fn entity_config_key(
        &self,
        group: &ActorGroupId,
        entity_type: &EntityType,
    ) -> String {
        self.key(&format!(
            "definitions/groups/{}/entity_types/{}",
            group.as_str(),
            entity_type.as_str()
        ))
    }

    pub(super) fn singleton_config_key(
        &self,
        group: &ActorGroupId,
        kind: &SingletonKind,
    ) -> String {
        self.key(&format!(
            "definitions/groups/{}/singleton_types/{}",
            group.as_str(),
            kind.as_str()
        ))
    }

    pub(super) fn operation_key(&self, group: &ActorGroupId, operation_id: &str) -> String {
        let encoded = operation_id
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.key(&format!("groups/{}/admin/{encoded}", group.as_str()))
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
            let response = self
                .read_deadline(
                    client.get(
                        start.clone(),
                        Some(
                            GetOptions::new()
                                .with_range(end.clone())
                                .with_limit(page_limit)
                                .with_sort(SortTarget::Key, SortOrder::Ascend),
                        ),
                    ),
                )
                .await?;
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
        let response = self.read_deadline(client.get(key, None)).await?;
        Ok(response.kvs().first().map(|record| {
            (
                record.value().to_vec(),
                record.mod_revision(),
                record.lease(),
            )
        }))
    }
}

impl EtcdCoordinationStore {
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
        match self.get_json_key(&self.slot_key(key)).await? {
            Some(slot) => self.hydrate_slot_barrier(slot).await.map(Some),
            None => Ok(None),
        }
    }

    async fn get_plan(
        &self,
        group: &ActorGroupId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        Ok(self
            .read_plan_record(&self.plan_key(group, plan_id))
            .await?
            .map(|(plan, _)| plan))
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

    async fn get_group_member(
        &self,
        group: &ActorGroupId,
        node_id: &str,
    ) -> Result<Option<GroupMemberRecord>, StorageError> {
        self.get_json_key(&self.group_member_key(group, node_id))
            .await
    }

    async fn list_group_members(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<GroupMemberRecord>, StorageError> {
        self.list_json(
            &format!("groups/{}/members/", group.as_str()),
            self.limits.maximum_members,
        )
        .await
    }

    async fn list_slots(&self, group: &ActorGroupId) -> Result<Vec<PlacementSlot>, StorageError> {
        let mut slots = self
            .list_json(
                &format!("groups/{}/shards/", group.as_str()),
                self.limits.maximum_slots,
            )
            .await?;
        slots.extend(
            self.list_json(
                &format!("groups/{}/singletons/", group.as_str()),
                self.limits.maximum_slots,
            )
            .await?,
        );
        if slots.len() > self.limits.maximum_slots {
            return Err(StorageError::Capacity);
        }
        let mut hydrated = Vec::with_capacity(slots.len());
        for slot in slots {
            hydrated.push(self.hydrate_slot_barrier(slot).await?);
        }
        Ok(hydrated)
    }

    async fn list_plans(&self, group: &ActorGroupId) -> Result<Vec<RebalancePlan>, StorageError> {
        let mut cursor = None;
        let mut result = Vec::new();
        loop {
            let page = self
                .list_plans_page_inner(group, cursor.as_ref(), self.list_page_size)
                .await?;
            result.extend(page.records);
            if result.len() > self.limits.maximum_plans {
                return Err(StorageError::Capacity);
            }
            let Some(next) = page.next_cursor else {
                return Ok(result);
            };
            cursor = Some(next);
        }
    }

    async fn list_claims(&self, group: &ActorGroupId) -> Result<Vec<LeasedClaim>, StorageError> {
        let mut claims = self
            .list_claims_suffix(&format!("groups/{}/shard_claims/", group.as_str()))
            .await?;
        claims.extend(
            self.list_claims_suffix(&format!("groups/{}/singleton_claims/", group.as_str()))
                .await?,
        );
        if claims.len() > self.limits.maximum_slots {
            return Err(StorageError::Capacity);
        }
        Ok(claims)
    }

    async fn get_automatic_settings(
        &self,
        group: &ActorGroupId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        self.get_json(&format!(
            "definitions/groups/{}/settings/automatic_balance",
            group.as_str()
        ))
        .await
    }

    async fn get_admin_operation(
        &self,
        group: &ActorGroupId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        self.get_json_key(&self.operation_key(group, operation_id))
            .await
    }

    async fn list_admin_operations(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<AdminOperationRecord>, StorageError> {
        self.list_json(
            &format!("groups/{}/admin/", group.as_str()),
            self.limits.maximum_admin_operations,
        )
        .await
    }
}

impl EtcdCoordinationStore {
    async fn ensure_framework(&self) -> Result<RunEpoch, StorageError> {
        self.ensure_framework_inner().await
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
        let response = self
            .read_deadline(client.lease_time_to_live(lease_id, None))
            .await?;
        if response.ttl() <= 0 {
            Ok(None)
        } else {
            Ok(Some(
                Duration::from_secs(
                    u64::try_from(response.ttl()).map_err(|_| StorageError::InvalidRecord)?,
                )
                .saturating_sub(Duration::from_secs(1)),
            ))
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
        guard: &ClusterLeaderGuard,
        request: CreateMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::create_member(self, guard, request).await
    }

    async fn update_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: UpdateMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::update_member(self, guard, request).await
    }

    async fn remove_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::remove_member(self, guard, request).await
    }

    async fn remove_expired_member(
        &self,
        guard: &ClusterLeaderGuard,
        request: RemoveExpiredMember,
    ) -> Result<MemberCommit, StorageError> {
        transactions::remove_expired_member(self, guard, request).await
    }

    async fn create_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: CreateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        transactions::create_group_member(self, guard, request).await
    }

    async fn update_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdateGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        transactions::update_group_member(self, guard, request).await
    }

    async fn remove_group_member(
        &self,
        guard: &GroupLeaderGuard,
        request: RemoveGroupMember,
    ) -> Result<GroupMemberCommit, StorageError> {
        transactions::remove_group_member(self, guard, request).await
    }

    async fn put_entity_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutEntityConfig,
    ) -> Result<EntityConfigCommit, StorageError> {
        transactions::put_entity_config(self, guard, request).await
    }

    async fn put_singleton_config(
        &self,
        guard: &GroupLeaderGuard,
        request: PutSingletonConfig,
    ) -> Result<SingletonConfigCommit, StorageError> {
        transactions::put_singleton_config(self, guard, request).await
    }

    async fn create_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::create_plan(self, guard, request).await
    }

    async fn update_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::update_plan(self, guard, request).await
    }

    async fn delete_plan(
        &self,
        guard: &GroupLeaderGuard,
        request: DeletePlan,
    ) -> Result<PlanCommit, StorageError> {
        transactions::delete_plan(self, guard, request).await
    }

    async fn transition_slot(
        &self,
        guard: &GroupLeaderGuard,
        request: TransitionSlot,
    ) -> Result<SlotCommit, StorageError> {
        transactions::transition_slot(self, guard, request).await
    }

    async fn allocate_initial(
        &self,
        guard: &GroupLeaderGuard,
        request: AllocateInitial,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::allocate_initial(self, guard, request).await
    }

    async fn activate_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: ActivateAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::activate_authority(self, guard, request).await
    }

    async fn reserve_move(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveMove,
    ) -> Result<MoveCommit, StorageError> {
        transactions::reserve_move(self, guard, request).await
    }

    async fn reserve_handoff(
        &self,
        guard: &GroupLeaderGuard,
        request: ReserveHandoff,
    ) -> Result<SlotCommit, StorageError> {
        transactions::reserve_handoff(self, guard, request).await
    }

    async fn fence_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::fence_authority(self, guard, request).await
    }

    async fn fence_missing_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        transactions::fence_missing_authority(self, guard, request).await
    }

    async fn install_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: InstallAuthority,
    ) -> Result<AuthorityCommit, StorageError> {
        transactions::install_authority(self, guard, request).await
    }

    async fn adopt_authority(
        &self,
        guard: &GroupLeaderGuard,
        request: AdoptAuthority,
    ) -> Result<LeasedClaim, StorageError> {
        transactions::adopt_authority(self, guard, request).await
    }

    async fn complete_move(
        &self,
        guard: &GroupLeaderGuard,
        request: CompleteMove,
    ) -> Result<MoveCommit, StorageError> {
        transactions::complete_move(self, guard, request).await
    }

    async fn commit_automatic_settings(
        &self,
        guard: &GroupLeaderGuard,
        request: CommitAutomaticSettings,
    ) -> Result<AutomaticBalanceSettings, StorageError> {
        transactions::commit_automatic_settings(self, guard, request).await
    }

    async fn create_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: CreatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        transactions::create_plan_with_operation(self, guard, request).await
    }

    async fn update_plan_with_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: UpdatePlanWithOperation,
    ) -> Result<PlanCommit, StorageError> {
        transactions::update_plan_with_operation(self, guard, request).await
    }

    async fn record_admin_operation(
        &self,
        guard: &GroupLeaderGuard,
        request: RecordAdminOperation,
    ) -> Result<AdminOperationRecord, StorageError> {
        transactions::record_admin_operation(self, guard, request).await
    }

    async fn compact_admin_operations(
        &self,
        guard: &GroupLeaderGuard,
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
    super::records::encode_record(value)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    if bytes.len() > super::records::MAX_DURABLE_VALUE_BYTES {
        return Err(StorageError::RecordTooLarge {
            actual: bytes.len(),
            maximum: super::records::MAX_DURABLE_VALUE_BYTES,
        });
    }
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
            11 if status.message().contains("compacted") => StorageError::SnapshotCompacted,
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

async fn run_read_deadline<T, F>(timeout: Duration, operation: F) -> Result<T, StorageError>
where
    F: Future<Output = Result<T, etcd_client::Error>>,
{
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| StorageError::Deadline)?
        .map_err(map_etcd_read)
}

async fn run_write_deadline<T, F>(timeout: Duration, operation: F) -> Result<T, StorageError>
where
    F: Future<Output = Result<T, etcd_client::Error>>,
{
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| StorageError::OutcomeUnknown)?
        .map_err(map_etcd_txn)
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[tokio::test]
    async fn stalled_reads_and_writes_preserve_distinct_deadline_semantics() {
        let read = run_read_deadline(
            Duration::from_millis(5),
            std::future::pending::<Result<(), etcd_client::Error>>(),
        )
        .await;
        assert_eq!(read, Err(StorageError::Deadline));

        let write = run_write_deadline(
            Duration::from_millis(5),
            std::future::pending::<Result<(), etcd_client::Error>>(),
        )
        .await;
        assert_eq!(write, Err(StorageError::OutcomeUnknown));
    }
}
