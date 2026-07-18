use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
use lattice_core::coordinator::CoordinatorScope;
use serde::de::DeserializeOwned;

use super::{EtcdPlacementStore, decode, encode, map_etcd_txn, parse_revision_value};
use crate::{
    coordinator::{
        DomainMemberRecord, DomainMemberStatus, ExactLeaderGuard, MemberRecord, MemberStatus,
        MembershipLeaderGuard, PlacementLeaderGuard, SessionLimits,
    },
    plan::{MoveProgress, RebalancePlan},
    storage::{
        PlacementDomainStore, StorageError,
        domain::{
            ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial,
            AuthorityCommit, AutomaticBalanceSettings, ClaimPredicate, CommitAutomaticSettings,
            CompactAdminOperations, CompleteMove, CreateDomainMember, CreateMember, CreatePlan,
            CreatePlanWithOperation, DeletePlan, DomainMemberCommit, EntityConfigCommit,
            FenceAuthority, FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit,
            MoveCommit, PlanCommit, PutEntityConfig, PutSingletonConfig, RecordAdminOperation,
            RemoveDomainMember, RemoveMember, ReserveHandoff, ReserveMove, SingletonConfigCommit,
            SlotCommit, TransitionSlot, UpdateDomainMember, UpdateMember, UpdatePlan,
            UpdatePlanWithOperation,
        },
    },
    types::{
        ClaimGrant, NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion,
        Revision,
    },
};

struct StateCounter {
    compare: Compare,
    put: TxnOp,
}

struct CardinalityCounter {
    compare: Compare,
    put: TxnOp,
}

async fn cardinality_counter(
    store: &EtcdPlacementStore,
    scope: &CoordinatorScope,
    name: &str,
    delta: i64,
    maximum: usize,
) -> Result<CardinalityCounter, StorageError> {
    let key = store.scope_key(scope, &format!("counters/{name}"));
    let current_record = store.read_raw(&key).await?;
    let current = current_record
        .as_ref()
        .map(|(bytes, _, _)| {
            std::str::from_utf8(bytes)
                .map_err(|_| StorageError::Codec)?
                .parse::<i64>()
                .map_err(|_| StorageError::Codec)
        })
        .transpose()?
        .unwrap_or(0);
    let next = current
        .checked_add(delta)
        .ok_or(StorageError::CounterExhausted)?;
    if next < 0 || usize::try_from(next).map_err(|_| StorageError::Capacity)? > maximum {
        return Err(StorageError::Capacity);
    }
    Ok(CardinalityCounter {
        compare: current_record.map_or_else(
            || Compare::version(key.clone(), CompareOp::Equal, 0),
            |(_, mod_revision, _)| {
                Compare::mod_revision(key.clone(), CompareOp::Equal, mod_revision)
            },
        ),
        put: TxnOp::put(key, next.to_string(), None),
    })
}

async fn state_counter(
    store: &EtcdPlacementStore,
    scope: &CoordinatorScope,
    proposed: Revision,
) -> Result<StateCounter, StorageError> {
    let key = store.scope_key(scope, "state_revision");
    let current_record = store.read_raw(&key).await?;
    let current = current_record
        .as_ref()
        .map(|(bytes, _, _)| parse_revision_value(bytes))
        .transpose()?
        .unwrap_or_else(|| Revision::new(1).expect("one is a valid state revision"));
    let next = current.next().map_err(|_| StorageError::CounterExhausted)?;
    if proposed != next {
        return Err(StorageError::CompareFailed);
    }
    Ok(StateCounter {
        compare: current_record.map_or_else(
            || Compare::version(key.clone(), CompareOp::Equal, 0),
            |(_, mod_revision, _)| {
                Compare::mod_revision(key.clone(), CompareOp::Equal, mod_revision)
            },
        ),
        put: TxnOp::put(key, proposed.get().to_string(), None),
    })
}

fn guard_compares(
    store: &EtcdPlacementStore,
    guard: &impl ExactLeaderGuard,
) -> Result<[Compare; 2], StorageError> {
    Ok([
        Compare::value(
            store.scope_key(guard.scope(), "leader"),
            CompareOp::Equal,
            encode(guard.record())?,
        ),
        Compare::value(
            store.scope_key(guard.scope(), "term"),
            CompareOp::Equal,
            guard.term().get().to_string(),
        ),
    ])
}

async fn diagnose_false(
    store: &EtcdPlacementStore,
    guard: &impl ExactLeaderGuard,
) -> Result<(), StorageError> {
    let leader = store
        .read_raw(&store.scope_key(guard.scope(), "leader"))
        .await?;
    let term = store
        .read_raw(&store.scope_key(guard.scope(), "term"))
        .await?;
    let leader_matches = leader
        .as_ref()
        .and_then(|(bytes, _, _)| decode(bytes).ok())
        .as_ref()
        == Some(guard.record());
    let term_matches = term
        .as_ref()
        .and_then(|(bytes, _, _)| std::str::from_utf8(bytes).ok())
        .and_then(|value| value.parse::<u64>().ok())
        == Some(guard.term().get());
    if leader_matches && term_matches {
        Err(StorageError::CompareFailed)
    } else {
        Err(StorageError::LeadershipLost)
    }
}

async fn ensure_guard_live(
    store: &EtcdPlacementStore,
    guard: &impl ExactLeaderGuard,
) -> Result<(), StorageError> {
    match diagnose_false(store, guard).await {
        Err(StorageError::CompareFailed) => Ok(()),
        Err(error) => Err(error),
        Ok(()) => unreachable!("leader diagnosis always returns a classification"),
    }
}

async fn commit(
    store: &EtcdPlacementStore,
    guard: &impl ExactLeaderGuard,
    mut compares: Vec<Compare>,
    operations: Vec<TxnOp>,
) -> Result<(), StorageError> {
    compares.extend(guard_compares(store, guard)?);
    let mut client = store.client.clone();
    let response = client
        .txn(Txn::new().when(compares).and_then(operations))
        .await
        .map_err(map_etcd_txn)?;
    if response.succeeded() {
        Ok(())
    } else {
        diagnose_false(store, guard).await
    }
}

fn validate_member(member: &MemberRecord) -> Result<(), StorageError> {
    if member.node != member.hello.node || member.lease_id <= 0 || member.node.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_domain_member(
    guard: &PlacementLeaderGuard,
    member: &DomainMemberRecord,
) -> Result<(), StorageError> {
    let CoordinatorScope::Placement(domain) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    member
        .validate(&SessionLimits::default())
        .map_err(|_| StorageError::InvalidRecord)?;
    if &member.version.domain != domain || &member.hello.domain != domain {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_operation(
    guard: &PlacementLeaderGuard,
    operation: &AdminOperationRecord,
) -> Result<(), StorageError> {
    let CoordinatorScope::Placement(domain) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if operation.operation_id.is_empty()
        || operation.operation_id.len() > 256
        || operation.fingerprint.is_empty()
        || operation.fingerprint.len() > 1024
        || operation.version.term != guard.term()
        || &operation.version.domain != domain
        || operation.expires_unix_millis <= operation.created_unix_millis
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_slot(
    guard: &PlacementLeaderGuard,
    expected: Option<&PlacementSlot>,
    slot: &PlacementSlot,
) -> Result<(), StorageError> {
    let CoordinatorScope::Placement(domain) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    slot.validate().map_err(|_| StorageError::InvalidRecord)?;
    if slot.version.term != guard.term()
        || &slot.version.domain != domain
        || slot.key.domain() != domain
        || expected.is_some_and(|expected| expected.key != slot.key)
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_plan_update(
    expected: &RebalancePlan,
    plan: &RebalancePlan,
) -> Result<(), StorageError> {
    if expected.plan_id != plan.plan_id
        || plan.record_revision
            != expected
                .record_revision
                .next()
                .map_err(|_| StorageError::CounterExhausted)?
    {
        return Err(StorageError::CompareFailed);
    }
    Ok(())
}

fn validate_plan_domain(
    guard: &PlacementLeaderGuard,
    plan: &RebalancePlan,
) -> Result<(), StorageError> {
    let CoordinatorScope::Placement(domain) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &plan.domain != domain || plan.coordinator_term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_claim(claim: &LeasedClaim, slot: &PlacementSlot) -> Result<(), StorageError> {
    if claim.lease_id <= 0 || claim.grant.ttl.is_zero() || !claim.matches_slot(slot) {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

async fn exact_record<T>(
    store: &EtcdPlacementStore,
    key: &str,
    expected: &T,
) -> Result<i64, StorageError>
where
    T: DeserializeOwned + PartialEq,
{
    let Some((bytes, mod_revision, _)) = store.read_raw(key).await? else {
        return Err(StorageError::CompareFailed);
    };
    if decode::<T>(&bytes)? != *expected {
        return Err(StorageError::CompareFailed);
    }
    Ok(mod_revision)
}

async fn exact_claim(
    store: &EtcdPlacementStore,
    key: &str,
    expected: &ClaimGrant,
) -> Result<i64, StorageError> {
    exact_record(store, key, expected).await
}

async fn assignment_compares(
    store: &EtcdPlacementStore,
    global_member: &MemberRecord,
    domain_member: &DomainMemberRecord,
    owner: &NodeKey,
) -> Result<[Compare; 2], StorageError> {
    if global_member.status != MemberStatus::Up
        || domain_member.status != DomainMemberStatus::Up
        || &global_member.node != owner
        || &domain_member.node != owner
    {
        return Err(StorageError::InvalidRecord);
    }
    let global_key = store.key(&format!("membership/members/{}", owner.node_id));
    let domain_key = store.domain_member_key(&domain_member.version.domain, &owner.node_id);
    let global_revision = exact_record(store, &global_key, global_member).await?;
    let domain_revision = exact_record(store, &domain_key, domain_member).await?;
    Ok([
        Compare::mod_revision(global_key, CompareOp::Equal, global_revision),
        Compare::mod_revision(domain_key, CompareOp::Equal, domain_revision),
    ])
}

pub(super) async fn create_member(
    store: &EtcdPlacementStore,
    guard: &MembershipLeaderGuard,
    request: CreateMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Membership {
        return Err(StorageError::InvalidRecord);
    }
    ensure_guard_live(store, guard).await?;
    validate_member(&request.member)?;
    let state = state_counter(store, guard.scope(), request.member.version.revision).await?;
    let count = cardinality_counter(
        store,
        guard.scope(),
        "members",
        1,
        store.limits.maximum_members,
    )
    .await?;
    let key = store.key(&format!(
        "membership/members/{}",
        request.member.node.node_id
    ));
    commit(
        store,
        guard,
        vec![
            Compare::version(key.clone(), CompareOp::Equal, 0),
            state.compare,
            count.compare,
        ],
        vec![
            TxnOp::put(
                key,
                encode(&request.member)?,
                Some(PutOptions::new().with_lease(request.member.lease_id)),
            ),
            state.put,
            count.put,
        ],
    )
    .await?;
    Ok(MemberCommit {
        revision: request.member.version.revision,
        member: request.member,
    })
}

pub(super) async fn update_member(
    store: &EtcdPlacementStore,
    guard: &MembershipLeaderGuard,
    request: UpdateMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Membership {
        return Err(StorageError::InvalidRecord);
    }
    ensure_guard_live(store, guard).await?;
    validate_member(&request.member)?;
    if request.expected.node.node_id != request.member.node.node_id
        || request.expected.node.incarnation != request.member.node.incarnation
    {
        return Err(StorageError::CompareFailed);
    }
    let state = state_counter(store, guard.scope(), request.member.version.revision).await?;
    let key = store.key(&format!(
        "membership/members/{}",
        request.member.node.node_id
    ));
    let revision = exact_record(store, &key, &request.expected).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(key.clone(), CompareOp::Equal, revision),
            state.compare,
        ],
        vec![
            TxnOp::put(
                key,
                encode(&request.member)?,
                Some(PutOptions::new().with_lease(request.member.lease_id)),
            ),
            state.put,
        ],
    )
    .await?;
    Ok(MemberCommit {
        revision: request.member.version.revision,
        member: request.member,
    })
}

pub(super) async fn remove_member(
    store: &EtcdPlacementStore,
    guard: &MembershipLeaderGuard,
    request: RemoveMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Membership {
        return Err(StorageError::InvalidRecord);
    }
    ensure_guard_live(store, guard).await?;
    let current = store.get_membership_revision_inner().await?;
    let next = current.next().map_err(|_| StorageError::CounterExhausted)?;
    let state = state_counter(store, guard.scope(), next).await?;
    let count = cardinality_counter(
        store,
        guard.scope(),
        "members",
        -1,
        store.limits.maximum_members,
    )
    .await?;
    let key = store.key(&format!(
        "membership/members/{}",
        request.expected.node.node_id
    ));
    let revision = exact_record(store, &key, &request.expected).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(key.clone(), CompareOp::Equal, revision),
            state.compare,
            count.compare,
        ],
        vec![TxnOp::delete(key, None), state.put, count.put],
    )
    .await?;
    Ok(MemberCommit {
        member: request.expected,
        revision: next,
    })
}

pub(super) async fn create_domain_member(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: CreateDomainMember,
) -> Result<DomainMemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_domain_member(guard, &request.member)?;
    if request.member.version.term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    if request.expected_global_member.status != MemberStatus::Up
        || request.expected_global_member.node != request.member.node
    {
        return Err(StorageError::InvalidRecord);
    }
    let global_key = store.key(&format!(
        "membership/members/{}",
        request.expected_global_member.node.node_id
    ));
    let global_revision = exact_record(store, &global_key, &request.expected_global_member).await?;
    let member_key =
        store.domain_member_key(&request.member.version.domain, &request.member.node.node_id);
    let state = state_counter(store, guard.scope(), request.member.version.revision).await?;
    let count = cardinality_counter(
        store,
        guard.scope(),
        "members",
        1,
        store.limits.maximum_members,
    )
    .await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(global_key, CompareOp::Equal, global_revision),
            Compare::version(member_key.clone(), CompareOp::Equal, 0),
            state.compare,
            count.compare,
        ],
        vec![
            TxnOp::put(member_key, encode(&request.member)?, None),
            state.put,
            count.put,
        ],
    )
    .await?;
    Ok(DomainMemberCommit {
        member: request.member,
    })
}

pub(super) async fn update_domain_member(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: UpdateDomainMember,
) -> Result<DomainMemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_domain_member(guard, &request.expected)?;
    validate_domain_member(guard, &request.member)?;
    if request.member.version.term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    if request.expected_global_member.status != MemberStatus::Up
        || request.expected_global_member.node != request.member.node
        || request.expected.node != request.member.node
    {
        return Err(StorageError::InvalidRecord);
    }
    let global_key = store.key(&format!(
        "membership/members/{}",
        request.expected_global_member.node.node_id
    ));
    let global_revision = exact_record(store, &global_key, &request.expected_global_member).await?;
    let member_key =
        store.domain_member_key(&request.member.version.domain, &request.member.node.node_id);
    let member_revision = exact_record(store, &member_key, &request.expected).await?;
    let state = state_counter(store, guard.scope(), request.member.version.revision).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(global_key, CompareOp::Equal, global_revision),
            Compare::mod_revision(member_key.clone(), CompareOp::Equal, member_revision),
            state.compare,
        ],
        vec![
            TxnOp::put(member_key, encode(&request.member)?, None),
            state.put,
        ],
    )
    .await?;
    Ok(DomainMemberCommit {
        member: request.member,
    })
}

pub(super) async fn remove_domain_member(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: RemoveDomainMember,
) -> Result<DomainMemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_domain_member(guard, &request.expected)?;
    let member_key = store.domain_member_key(
        &request.expected.version.domain,
        &request.expected.node.node_id,
    );
    let member_revision = exact_record(store, &member_key, &request.expected).await?;
    let proposed = store
        .get_placement_revision(&request.expected.version.domain)
        .await?
        .next()
        .map_err(|_| StorageError::CounterExhausted)?;
    let state = state_counter(store, guard.scope(), proposed).await?;
    let count = cardinality_counter(
        store,
        guard.scope(),
        "members",
        -1,
        store.limits.maximum_members,
    )
    .await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(member_key.clone(), CompareOp::Equal, member_revision),
            state.compare,
            count.compare,
        ],
        vec![TxnOp::delete(member_key, None), state.put, count.put],
    )
    .await?;
    Ok(DomainMemberCommit {
        member: request.expected,
    })
}

pub(super) async fn put_entity_config(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: PutEntityConfig,
) -> Result<EntityConfigCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    let CoordinatorScope::Placement(domain) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &request.config.domain != domain || request.config.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    let key = store.entity_config_key(domain, &request.config.entity_type);
    let mut compares = Vec::new();
    let mut operations = Vec::new();
    if let Some(expected) = &request.expected {
        compares.push(Compare::mod_revision(
            key.clone(),
            CompareOp::Equal,
            exact_record(store, &key, expected).await?,
        ));
    } else {
        compares.push(Compare::version(key.clone(), CompareOp::Equal, 0));
        let count = cardinality_counter(
            store,
            guard.scope(),
            "entity_configs",
            1,
            store.limits.maximum_entity_configs,
        )
        .await?;
        compares.push(count.compare);
        operations.push(count.put);
    }
    let revision = store
        .get_placement_revision(domain)
        .await?
        .next()
        .map_err(|_| StorageError::CounterExhausted)?;
    let state = state_counter(store, guard.scope(), revision).await?;
    compares.push(state.compare);
    operations.push(TxnOp::put(key, encode(&request.config)?, None));
    operations.push(state.put);
    commit(store, guard, compares, operations).await?;
    Ok(EntityConfigCommit {
        config: request.config,
        version: PlacementVersion::new(domain.clone(), guard.term(), revision),
    })
}

pub(super) async fn put_singleton_config(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: PutSingletonConfig,
) -> Result<SingletonConfigCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    let CoordinatorScope::Placement(domain) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &request.config.domain != domain || !request.config.validate() {
        return Err(StorageError::InvalidRecord);
    }
    let key = store.singleton_config_key(domain, &request.config.kind);
    let mut compares = Vec::new();
    let mut operations = Vec::new();
    if let Some(expected) = &request.expected {
        compares.push(Compare::mod_revision(
            key.clone(),
            CompareOp::Equal,
            exact_record(store, &key, expected).await?,
        ));
    } else {
        compares.push(Compare::version(key.clone(), CompareOp::Equal, 0));
        let count = cardinality_counter(
            store,
            guard.scope(),
            "singleton_configs",
            1,
            store.limits.maximum_singleton_configs,
        )
        .await?;
        compares.push(count.compare);
        operations.push(count.put);
    }
    let revision = store
        .get_placement_revision(domain)
        .await?
        .next()
        .map_err(|_| StorageError::CounterExhausted)?;
    let state = state_counter(store, guard.scope(), revision).await?;
    compares.push(state.compare);
    operations.push(TxnOp::put(key, encode(&request.config)?, None));
    operations.push(state.put);
    commit(store, guard, compares, operations).await?;
    Ok(SingletonConfigCommit {
        config: request.config,
        version: PlacementVersion::new(domain.clone(), guard.term(), revision),
    })
}

include!("transactions_placement.rs");
