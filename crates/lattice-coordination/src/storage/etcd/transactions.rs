use crate::admin_operation::matches_committed;
use etcd_client::{Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use lattice_model::{
    cluster::CoordinatorScope,
    run::{ClusterLifecycle, RunPhase},
};
use serde::{Serialize, de::DeserializeOwned};

use super::plan_records::{plan_deletes, plan_puts};
use super::{EtcdCoordinationStore, decode, encode, parse_revision_value};
use crate::{
    coordinator::{
        ClusterLeaderGuard, ExactLeaderGuard, GroupLeaderGuard, GroupMemberRecord,
        GroupMemberStatus, MemberRecord, MemberStatus, SessionLimits,
    },
    plan::{MAXIMUM_PLAN_MOVES, MoveProgress, RebalancePlan},
    storage::{
        ActorGroupStore, ClusterLifecycleStore, StorageError,
        plan_records::validate_plan_payload,
        records::{
            ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial,
            AuthorityCommit, AutomaticBalanceSettings, ClaimPredicate, CommitAutomaticSettings,
            CompactAdminOperations, CompleteMove, CreateGroupMember, CreateMember, CreatePlan,
            CreatePlanWithOperation, DeletePlan, EntityConfigCommit, FenceAuthority,
            FenceMissingAuthority, GroupMemberCommit, InstallAuthority, LeasedClaim,
            MAX_ADMIN_GC_BATCH, MemberCommit, MoveCommit, PlanCommit, PutEntityConfig,
            PutSingletonConfig, RecordAdminOperation, RemoveExpiredMember, RemoveGroupMember,
            RemoveMember, ReserveHandoff, ReserveMove, SingletonConfigCommit, SlotCommit,
            TransitionSlot, UpdateGroupMember, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
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
    store: &EtcdCoordinationStore,
    scope: &CoordinatorScope,
    name: &str,
    delta: i64,
    maximum: usize,
) -> Result<CardinalityCounter, StorageError> {
    let key = store.scope_key(scope, &format!("counters/{name}"));
    let current_record = store.read_raw(&key).await?;
    let stored_count = current_record
        .as_ref()
        .map(|(bytes, _, _)| {
            std::str::from_utf8(bytes)
                .map_err(|_| StorageError::Codec)?
                .parse::<i64>()
                .map_err(|_| StorageError::Codec)
        })
        .transpose()?
        .unwrap_or(0);
    // Leased membership keys can disappear without a counter transaction. The
    // counter's mod-revision still serializes writers, but occupancy comes from
    // live keys. Expiry between this read and commit only reduces occupancy.
    let current = if name == "members" {
        let mut client = store.client.clone();
        store
            .read_deadline(client.get(
                store.scope_key(scope, "members/"),
                Some(GetOptions::new().with_prefix().with_count_only()),
            ))
            .await?
            .count()
    } else {
        stored_count
    };
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
    store: &EtcdCoordinationStore,
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

pub(super) fn leader_compares(
    store: &EtcdCoordinationStore,
    guard: &impl ExactLeaderGuard,
) -> Result<[Compare; 4], StorageError> {
    if store.bound_epoch()? != guard.record().epoch {
        return Err(StorageError::RunMismatch);
    }
    let registration = guard.record().candidate_registration();
    Ok([
        Compare::value(
            store.candidate_authorization_key(guard.scope(), &guard.record().node.node_id),
            CompareOp::Equal,
            encode(&registration.authorization)?,
        ),
        Compare::value(
            store.candidate_registration_key(guard.scope(), &guard.record().node.node_id),
            CompareOp::Equal,
            encode(&registration)?,
        ),
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
    store: &EtcdCoordinationStore,
    guard: &impl ExactLeaderGuard,
) -> Result<(), StorageError> {
    let lifecycle = store.lifecycle().await?;
    if lifecycle.epoch != guard.record().epoch {
        return Err(StorageError::RunMismatch);
    }
    if !lifecycle.is_running() {
        return Err(StorageError::RunNotRunning);
    }
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
        let expected_registration = guard.record().candidate_registration();
        let current_authorization = store
            .read_raw(
                &store.candidate_authorization_key(guard.scope(), &guard.record().node.node_id),
            )
            .await?;
        let current_registration = store
            .read_raw(
                &store.candidate_registration_key(guard.scope(), &guard.record().node.node_id),
            )
            .await?;
        if current_authorization.as_ref().map(|(value, _, _)| value)
            != Some(&encode(&expected_registration.authorization)?)
            || current_registration.as_ref().map(|(value, _, _)| value)
                != Some(&encode(&expected_registration)?)
        {
            return Err(StorageError::CandidateNotEligible);
        }
        Err(StorageError::CompareFailed)
    } else {
        Err(StorageError::LeadershipLost)
    }
}

async fn ensure_guard_live(
    store: &EtcdCoordinationStore,
    guard: &impl ExactLeaderGuard,
) -> Result<(), StorageError> {
    match diagnose_false(store, guard).await {
        Err(StorageError::CompareFailed) => Ok(()),
        Err(error) => Err(error),
        Ok(()) => unreachable!("leader diagnosis always returns a classification"),
    }
}

pub(super) async fn commit(
    store: &EtcdCoordinationStore,
    guard: &impl ExactLeaderGuard,
    mut compares: Vec<Compare>,
    operations: Vec<TxnOp>,
) -> Result<(), StorageError> {
    compares.extend(leader_compares(store, guard)?);
    compares.push(Compare::value(
        store.key("meta/lifecycle"),
        CompareOp::Equal,
        encode(&ClusterLifecycle {
            epoch: guard.record().epoch,
            phase: RunPhase::Running,
        })?,
    ));
    let mut client = store.client.clone();
    let response = store
        .write_deadline(client.txn(Txn::new().when(compares).and_then(operations)))
        .await?;
    if response.succeeded() {
        Ok(())
    } else {
        diagnose_false(store, guard).await
    }
}

fn validate_member(member: &MemberRecord) -> Result<(), StorageError> {
    if member.lease_id <= 0 || member.node.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_group_member(
    guard: &GroupLeaderGuard,
    member: &GroupMemberRecord,
) -> Result<(), StorageError> {
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    member
        .validate(&SessionLimits::default())
        .map_err(|_| StorageError::InvalidRecord)?;
    if &member.version.group != group {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_operation(
    guard: &GroupLeaderGuard,
    operation: &AdminOperationRecord,
) -> Result<(), StorageError> {
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if operation.operation_id.is_empty()
        || operation.operation_id.len() > 256
        || operation.fingerprint.is_empty()
        || operation.fingerprint.len() > 1024
        || operation.version.term != guard.term()
        || &operation.version.group != group
        || operation.expires_unix_millis <= operation.created_unix_millis
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_slot(
    guard: &GroupLeaderGuard,
    expected: Option<&PlacementSlot>,
    slot: &PlacementSlot,
) -> Result<(), StorageError> {
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    slot.validate().map_err(|_| StorageError::InvalidRecord)?;
    if slot.version.term != guard.term()
        || &slot.version.group != group
        || slot.key.group() != group
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
        || expected.moves.len() != plan.moves.len()
        || expected.entity_type != plan.entity_type
        || expected
            .moves
            .iter()
            .zip(&plan.moves)
            .any(|(before, after)| {
                before.shard_id != after.shard_id
                    || before.source != after.source
                    || before.target != after.target
                    || before.expected_generation != after.expected_generation
                    || before.estimated_weight != after.estimated_weight
            })
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

fn validate_plan_group(guard: &GroupLeaderGuard, plan: &RebalancePlan) -> Result<(), StorageError> {
    validate_plan_payload(plan)?;
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &plan.group != group
        || plan.coordinator_term != guard.term()
        || plan.moves.is_empty()
        || plan.moves.len() > MAXIMUM_PLAN_MOVES
    {
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

async fn exact_plan(
    store: &EtcdCoordinationStore,
    key: &str,
    expected: &RebalancePlan,
) -> Result<i64, StorageError> {
    match (store.read_plan_record(key).await?, expected.durable()) {
        (None, None) => Ok(0),
        (Some((current, revision)), Some(expected))
            if serde_json::to_vec(&current).map_err(|_| StorageError::Codec)?
                == serde_json::to_vec(&expected).map_err(|_| StorageError::Codec)? =>
        {
            Ok(revision)
        }
        _ => Err(StorageError::CompareFailed),
    }
}

async fn exact_record<T>(
    store: &EtcdCoordinationStore,
    key: &str,
    expected: &T,
) -> Result<i64, StorageError>
where
    T: DeserializeOwned + Serialize + PartialEq,
{
    let Some((bytes, mod_revision, _)) = store.read_raw(key).await? else {
        return Err(StorageError::CompareFailed);
    };
    if encode(&decode::<T>(&bytes)?)? != encode(expected)? {
        return Err(StorageError::CompareFailed);
    }
    Ok(mod_revision)
}

async fn exact_claim(
    store: &EtcdCoordinationStore,
    key: &str,
    expected: &ClaimGrant,
) -> Result<i64, StorageError> {
    exact_record(store, key, expected).await
}

pub(super) async fn assignment_compares(
    store: &EtcdCoordinationStore,
    global_member: &MemberRecord,
    group_member: &GroupMemberRecord,
    owner: &NodeKey,
) -> Result<[Compare; 2], StorageError> {
    if global_member.status != MemberStatus::Up
        || group_member.status != GroupMemberStatus::Up
        || &global_member.node != owner
        || &group_member.node != owner
    {
        return Err(StorageError::InvalidRecord);
    }
    let global_key = store.key(&format!("membership/members/{}", owner.node_id));
    let group_key = store.group_member_key(&group_member.version.group, &owner.node_id);
    let global_revision = exact_record(store, &global_key, global_member).await?;
    let group_revision = exact_record(store, &group_key, group_member).await?;
    Ok([
        Compare::mod_revision(global_key, CompareOp::Equal, global_revision),
        Compare::mod_revision(group_key, CompareOp::Equal, group_revision),
    ])
}

pub(super) async fn create_member(
    store: &EtcdCoordinationStore,
    guard: &ClusterLeaderGuard,
    request: CreateMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Cluster {
        return Err(StorageError::InvalidRecord);
    }
    ensure_guard_live(store, guard).await?;
    validate_member(&request.member)?;
    // Unresolved departed incarnations are safety obligations, not an unbounded
    // historical log. Reaching the bound requires positive stop proof or reset.
    let obligation_key = store.key(&format!(
        "shutdown/obligations/{}",
        crate::shutdown::participant_key(&request.member.node)
    ));
    if store.read_raw(&obligation_key).await?.is_none() {
        let mut client = store.client.clone();
        let obligations = store
            .read_deadline(
                client.get(
                    store.key("shutdown/obligations/"),
                    Some(
                        etcd_client::GetOptions::new()
                            .with_prefix()
                            .with_count_only(),
                    ),
                ),
            )
            .await?;
        if obligations.count() >= store.limits.maximum_members as i64 {
            return Err(StorageError::Capacity);
        }
    }
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
                store.key(&format!(
                    "shutdown/obligations/{}",
                    crate::shutdown::participant_key(&request.member.node)
                )),
                encode(&request.member.node)?,
                None,
            ),
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
    store: &EtcdCoordinationStore,
    guard: &ClusterLeaderGuard,
    request: UpdateMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Cluster {
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
    store: &EtcdCoordinationStore,
    guard: &ClusterLeaderGuard,
    request: RemoveMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Cluster {
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

pub(super) async fn remove_expired_member(
    store: &EtcdCoordinationStore,
    guard: &ClusterLeaderGuard,
    request: RemoveExpiredMember,
) -> Result<MemberCommit, StorageError> {
    if guard.scope() != &CoordinatorScope::Cluster {
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
        0,
        store.limits.maximum_members,
    )
    .await?;
    let key = store.key(&format!(
        "membership/members/{}",
        request.expected.node.node_id
    ));
    commit(
        store,
        guard,
        vec![
            Compare::version(key, CompareOp::Equal, 0),
            state.compare,
            count.compare,
        ],
        vec![state.put, count.put],
    )
    .await?;
    Ok(MemberCommit {
        member: request.expected,
        revision: next,
    })
}

pub(super) async fn create_group_member(
    store: &EtcdCoordinationStore,
    guard: &GroupLeaderGuard,
    request: CreateGroupMember,
) -> Result<GroupMemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_group_member(guard, &request.member)?;
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
        store.group_member_key(&request.member.version.group, &request.member.node.node_id);
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
            TxnOp::put(
                member_key,
                encode(&request.member)?,
                Some(PutOptions::new().with_lease(request.expected_global_member.lease_id)),
            ),
            state.put,
            count.put,
        ],
    )
    .await?;
    Ok(GroupMemberCommit {
        member: request.member,
    })
}

pub(super) async fn update_group_member(
    store: &EtcdCoordinationStore,
    guard: &GroupLeaderGuard,
    request: UpdateGroupMember,
) -> Result<GroupMemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_group_member(guard, &request.expected)?;
    validate_group_member(guard, &request.member)?;
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
        store.group_member_key(&request.member.version.group, &request.member.node.node_id);
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
            TxnOp::put(
                member_key,
                encode(&request.member)?,
                Some(PutOptions::new().with_lease(request.expected_global_member.lease_id)),
            ),
            state.put,
        ],
    )
    .await?;
    Ok(GroupMemberCommit {
        member: request.member,
    })
}

pub(super) async fn remove_group_member(
    store: &EtcdCoordinationStore,
    guard: &GroupLeaderGuard,
    request: RemoveGroupMember,
) -> Result<GroupMemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_group_member(guard, &request.expected)?;
    let member_key = store.group_member_key(
        &request.expected.version.group,
        &request.expected.node.node_id,
    );
    let member_revision = exact_record(store, &member_key, &request.expected).await?;
    let proposed = store
        .get_placement_revision(&request.expected.version.group)
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
    Ok(GroupMemberCommit {
        member: request.expected,
    })
}

pub(super) async fn put_entity_config(
    store: &EtcdCoordinationStore,
    guard: &GroupLeaderGuard,
    request: PutEntityConfig,
) -> Result<EntityConfigCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &request.config.group != group || request.config.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    let key = store.entity_config_key(group, &request.config.entity_type);
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
        .get_placement_revision(group)
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
        version: PlacementVersion::new(group.clone(), guard.term(), revision),
    })
}

pub(super) async fn put_singleton_config(
    store: &EtcdCoordinationStore,
    guard: &GroupLeaderGuard,
    request: PutSingletonConfig,
) -> Result<SingletonConfigCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &request.config.group != group || !request.config.validate() {
        return Err(StorageError::InvalidRecord);
    }
    let key = store.singleton_config_key(group, &request.config.kind);
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
        .get_placement_revision(group)
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
        version: PlacementVersion::new(group.clone(), guard.term(), revision),
    })
}

include!("transactions_placement.rs");
