use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};

use super::super::domain::{
    ActivateAuthority, AdminOperationRecord, AdoptAuthority, AllocateInitial, AuthorityCommit,
    AutomaticBalanceSettings, ClaimPredicate, CommitAutomaticSettings, CompactAdminOperations,
    CompleteMove, CreateMember, CreatePlan, CreatePlanWithOperation, DeletePlan, FenceAuthority,
    FenceMissingAuthority, InstallAuthority, LeasedClaim, MemberCommit, MoveCommit, PlanCommit,
    RecordAdminOperation, RemoveMember, RemoveMemberWithOperation, ReserveHandoff, ReserveMove,
    SlotCommit, TransitionSlot, UpdateMember, UpdatePlan, UpdatePlanWithOperation,
};
use super::super::{PlacementStore, StorageError};
use super::{EtcdPlacementStore, decode, encode, map_etcd_txn, parse_revision_value};
use crate::coordinator::{LeaderGuard, MemberRecord};
use crate::plan::{MoveProgress, RebalancePlan};
use crate::types::{ClaimGrant, PlacementSlot, PlacementSlotState, Revision};

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
    name: &str,
    delta: i64,
    maximum: usize,
) -> Result<CardinalityCounter, StorageError> {
    let key = store.key(&format!("counters/{name}"));
    let Some((bytes, mod_revision, _)) = store.read_raw(&key).await? else {
        return Err(StorageError::SchemaGenerationMismatch);
    };
    let current = std::str::from_utf8(&bytes)
        .map_err(|_| StorageError::Codec)?
        .parse::<i64>()
        .map_err(|_| StorageError::Codec)?;
    let next = current
        .checked_add(delta)
        .ok_or(StorageError::CounterExhausted)?;
    if next < 0 || usize::try_from(next).map_err(|_| StorageError::Capacity)? > maximum {
        return Err(StorageError::Capacity);
    }
    Ok(CardinalityCounter {
        compare: Compare::mod_revision(key.clone(), CompareOp::Equal, mod_revision),
        put: TxnOp::put(key, next.to_string(), None),
    })
}

async fn state_counter(
    store: &EtcdPlacementStore,
    proposed: Revision,
) -> Result<StateCounter, StorageError> {
    let key = store.key("coordinator/state_revision");
    let Some((bytes, mod_revision, _)) = store.read_raw(&key).await? else {
        return Err(StorageError::SchemaGenerationMismatch);
    };
    let current = parse_revision_value(&bytes)?;
    let next = current.next().map_err(|_| StorageError::CounterExhausted)?;
    if proposed != next {
        return Err(StorageError::CompareFailed);
    }
    Ok(StateCounter {
        compare: Compare::mod_revision(key.clone(), CompareOp::Equal, mod_revision),
        put: TxnOp::put(key, proposed.get().to_string(), None),
    })
}

fn guard_compares(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
) -> Result<[Compare; 2], StorageError> {
    Ok([
        Compare::value(
            store.key("coordinator/leader"),
            CompareOp::Equal,
            encode(guard.record())?,
        ),
        Compare::value(
            store.key("coordinator/term"),
            CompareOp::Equal,
            guard.term().get().to_string(),
        ),
    ])
}

async fn diagnose_false(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
) -> Result<(), StorageError> {
    let leader = store.read_raw(&store.key("coordinator/leader")).await?;
    let term = store.read_raw(&store.key("coordinator/term")).await?;
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
    guard: &LeaderGuard,
) -> Result<(), StorageError> {
    match diagnose_false(store, guard).await {
        Err(StorageError::CompareFailed) => Ok(()),
        Err(error) => Err(error),
        Ok(()) => unreachable!("leader diagnosis always returns a classification"),
    }
}

async fn commit(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
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

fn validate_operation(
    guard: &LeaderGuard,
    operation: &AdminOperationRecord,
) -> Result<(), StorageError> {
    if operation.operation_id.is_empty()
        || operation.operation_id.len() > 256
        || operation.fingerprint.is_empty()
        || operation.fingerprint.len() > 1024
        || operation.version.term != guard.term()
        || operation.expires_unix_millis <= operation.created_unix_millis
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_slot(
    guard: &LeaderGuard,
    expected: Option<&PlacementSlot>,
    slot: &PlacementSlot,
) -> Result<(), StorageError> {
    slot.validate().map_err(|_| StorageError::InvalidRecord)?;
    if slot.version.term != guard.term()
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
    T: serde::de::DeserializeOwned + PartialEq,
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

pub(super) async fn create_member(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: CreateMember,
) -> Result<MemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_member(&request.member)?;
    let state = state_counter(store, request.member.version.revision).await?;
    let count = cardinality_counter(store, "members", 1, store.limits.maximum_members).await?;
    let key = store.key(&format!("members/{}", request.member.node.node_id));
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
    guard: &LeaderGuard,
    request: UpdateMember,
) -> Result<MemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_member(&request.member)?;
    if request.expected.node.node_id != request.member.node.node_id
        || request.expected.node.incarnation != request.member.node.incarnation
    {
        return Err(StorageError::CompareFailed);
    }
    let state = state_counter(store, request.member.version.revision).await?;
    let key = store.key(&format!("members/{}", request.member.node.node_id));
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
    guard: &LeaderGuard,
    request: RemoveMember,
) -> Result<MemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    let current = store.get_state_revision().await?;
    let next = current.next().map_err(|_| StorageError::CounterExhausted)?;
    let state = state_counter(store, next).await?;
    let count = cardinality_counter(store, "members", -1, store.limits.maximum_members).await?;
    let key = store.key(&format!("members/{}", request.expected.node.node_id));
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

pub(super) async fn create_plan(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: CreatePlan,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    if request.plan.coordinator_term != guard.term() || request.plan.record_revision.get() != 1 {
        return Err(StorageError::InvalidRecord);
    }
    let key = store.plan_key(request.plan.plan_id);
    let count = cardinality_counter(store, "plans", 1, store.limits.maximum_plans).await?;
    commit(
        store,
        guard,
        vec![
            Compare::version(key.clone(), CompareOp::Equal, 0),
            count.compare,
        ],
        vec![TxnOp::put(key, encode(&request.plan)?, None), count.put],
    )
    .await?;
    Ok(PlanCommit { plan: request.plan })
}

pub(super) async fn update_plan(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: UpdatePlan,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_plan_update(&request.expected, &request.plan)?;
    let key = store.plan_key(request.plan.plan_id);
    let revision = exact_record(store, &key, &request.expected).await?;
    commit(
        store,
        guard,
        vec![Compare::mod_revision(
            key.clone(),
            CompareOp::Equal,
            revision,
        )],
        vec![TxnOp::put(key, encode(&request.plan)?, None)],
    )
    .await?;
    Ok(PlanCommit { plan: request.plan })
}

pub(super) async fn delete_plan(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: DeletePlan,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    let key = store.plan_key(request.expected.plan_id);
    let revision = exact_record(store, &key, &request.expected).await?;
    let count = cardinality_counter(store, "plans", -1, store.limits.maximum_plans).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(key.clone(), CompareOp::Equal, revision),
            count.compare,
        ],
        vec![TxnOp::delete(key, None), count.put],
    )
    .await?;
    Ok(PlanCommit {
        plan: request.expected,
    })
}

pub(super) async fn transition_slot(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: TransitionSlot,
) -> Result<SlotCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected), &request.slot)?;
    if request.expected.owner != request.slot.owner
        || request.expected.assignment_generation != request.slot.assignment_generation
        || request.expected.active_move != request.slot.active_move
        || matches!(
            request.slot.state,
            PlacementSlotState::Allocating | PlacementSlotState::Running
        )
        || !matches!(
            (request.expected.state, request.slot.state),
            (
                PlacementSlotState::BeginHandoff,
                PlacementSlotState::Stopping
            ) | (PlacementSlotState::Stopping, PlacementSlotState::StopFailed)
                | (PlacementSlotState::StopFailed, PlacementSlotState::Stopping)
        )
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let key = store.slot_key(&request.slot.key);
    let revision = exact_record(store, &key, &request.expected).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(key.clone(), CompareOp::Equal, revision),
            state.compare,
        ],
        vec![TxnOp::put(key, encode(&request.slot)?, None), state.put],
    )
    .await?;
    Ok(SlotCommit { slot: request.slot })
}

pub(super) async fn allocate_initial(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: AllocateInitial,
) -> Result<AuthorityCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, None, &request.slot)?;
    validate_claim(&request.claim, &request.slot)?;
    if request.slot.state != PlacementSlotState::Allocating || request.slot.active_move.is_some() {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let count = cardinality_counter(store, "slots", 1, store.limits.maximum_slots).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    commit(
        store,
        guard,
        vec![
            Compare::version(slot_key.clone(), CompareOp::Equal, 0),
            Compare::version(claim_key.clone(), CompareOp::Equal, 0),
            state.compare,
            count.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            TxnOp::put(
                claim_key,
                encode(&request.claim.grant)?,
                Some(PutOptions::new().with_lease(request.claim.lease_id)),
            ),
            state.put,
            count.put,
        ],
    )
    .await?;
    Ok(AuthorityCommit {
        slot: request.slot,
        claim: request.claim,
    })
}

pub(super) async fn activate_authority(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: ActivateAuthority,
) -> Result<SlotCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    if request.expected_slot.state != PlacementSlotState::Allocating
        || request.slot.state != PlacementSlotState::Running
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || request.expected_claim.slot != request.slot.key
        || request.slot.owner.as_ref() != Some(&request.expected_claim.owner)
        || request.slot.assignment_generation != request.expected_claim.assignment_generation
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    let claim_revision = exact_claim(store, &claim_key, &request.expected_claim).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::mod_revision(claim_key, CompareOp::Equal, claim_revision),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            state.put,
        ],
    )
    .await?;
    Ok(SlotCommit { slot: request.slot })
}

pub(super) async fn reserve_move(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: ReserveMove,
) -> Result<MoveCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_plan_update(&request.expected_plan, &request.plan)?;
    if request.expected_slot.state != PlacementSlotState::Running
        || request.expected_slot.active_move.is_some()
        || request.slot.state != PlacementSlotState::BeginHandoff
        || request.slot.active_move != Some(request.plan.plan_id)
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || !request.plan.moves.iter().any(|movement| {
            movement.progress == MoveProgress::Handoff
                && request.slot.target.as_ref() == Some(&movement.target)
        })
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let plan_key = store.plan_key(request.plan.plan_id);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    let plan_revision = exact_record(store, &plan_key, &request.expected_plan).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::mod_revision(plan_key.clone(), CompareOp::Equal, plan_revision),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            TxnOp::put(plan_key, encode(&request.plan)?, None),
            state.put,
        ],
    )
    .await?;
    Ok(MoveCommit {
        slot: request.slot,
        plan: request.plan,
    })
}

pub(super) async fn reserve_handoff(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: ReserveHandoff,
) -> Result<SlotCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    if !matches!(
        request.slot.key,
        crate::types::PlacementSlotKey::Singleton(_)
    ) || request.expected_slot.state != PlacementSlotState::Running
        || request.expected_slot.active_move.is_some()
        || request.slot.state != PlacementSlotState::BeginHandoff
        || request.slot.active_move.is_none()
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            state.put,
        ],
    )
    .await?;
    Ok(SlotCommit { slot: request.slot })
}

pub(super) async fn fence_authority(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: FenceAuthority,
) -> Result<SlotCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    if !matches!(
        request.expected_slot.state,
        PlacementSlotState::Stopping | PlacementSlotState::StopFailed
    ) || request.slot.state != PlacementSlotState::Fenced
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || request.expected_slot.active_move != request.slot.active_move
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    let (claim_compare, claim_op) = match &request.expected_claim {
        ClaimPredicate::Present(expected) => {
            if expected.slot != request.slot.key {
                return Err(StorageError::InvalidTransition);
            }
            let revision = exact_claim(store, &claim_key, expected).await?;
            (
                Compare::mod_revision(claim_key.clone(), CompareOp::Equal, revision),
                Some(TxnOp::delete(claim_key, None)),
            )
        }
        ClaimPredicate::Absent => (Compare::version(claim_key, CompareOp::Equal, 0), None),
    };
    let mut operations = vec![TxnOp::put(slot_key.clone(), encode(&request.slot)?, None)];
    if let Some(operation) = claim_op {
        operations.push(operation);
    }
    operations.push(state.put);
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key, CompareOp::Equal, slot_revision),
            claim_compare,
            state.compare,
        ],
        operations,
    )
    .await?;
    Ok(SlotCommit { slot: request.slot })
}

pub(super) async fn fence_missing_authority(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: FenceMissingAuthority,
) -> Result<SlotCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    if !matches!(
        request.expected_slot.state,
        PlacementSlotState::Allocating | PlacementSlotState::Running
    ) || request.slot.state != PlacementSlotState::Fenced
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || request.expected_slot.active_move != request.slot.active_move
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::version(claim_key, CompareOp::Equal, 0),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            state.put,
        ],
    )
    .await?;
    Ok(SlotCommit { slot: request.slot })
}

pub(super) async fn install_authority(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: InstallAuthority,
) -> Result<AuthorityCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_claim(&request.claim, &request.slot)?;
    if request.expected_slot.state != PlacementSlotState::Fenced
        || request.slot.state != PlacementSlotState::Allocating
        || request.expected_slot.active_move != request.slot.active_move
        || request.slot.assignment_generation
            != request
                .expected_slot
                .assignment_generation
                .next()
                .map_err(|_| StorageError::CounterExhausted)?
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::version(claim_key.clone(), CompareOp::Equal, 0),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            TxnOp::put(
                claim_key,
                encode(&request.claim.grant)?,
                Some(PutOptions::new().with_lease(request.claim.lease_id)),
            ),
            state.put,
        ],
    )
    .await?;
    Ok(AuthorityCommit {
        slot: request.slot,
        claim: request.claim,
    })
}

pub(super) async fn adopt_authority(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: AdoptAuthority,
) -> Result<AuthorityCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_claim(&request.claim, &request.slot)?;
    if request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || request.expected_slot.state != request.slot.state
        || request.expected_claim.owner != request.claim.grant.owner
        || request.expected_claim.assignment_generation != request.claim.grant.assignment_generation
        || request.expected_claim.coordinator_term >= request.claim.grant.coordinator_term
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    let claim_revision = exact_claim(store, &claim_key, &request.expected_claim).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::mod_revision(claim_key.clone(), CompareOp::Equal, claim_revision),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            TxnOp::put(
                claim_key,
                encode(&request.claim.grant)?,
                Some(PutOptions::new().with_lease(request.claim.lease_id)),
            ),
            state.put,
        ],
    )
    .await?;
    Ok(AuthorityCommit {
        slot: request.slot,
        claim: request.claim,
    })
}

pub(super) async fn complete_move(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: CompleteMove,
) -> Result<MoveCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_plan_update(&request.expected_plan, &request.plan)?;
    if request.expected_slot.state != PlacementSlotState::Allocating
        || request.slot.state != PlacementSlotState::Running
        || request.expected_slot.active_move != Some(request.plan.plan_id)
        || request.slot.active_move.is_some()
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || request.slot.owner.as_ref() != Some(&request.expected_claim.owner)
        || request
            .plan
            .moves
            .iter()
            .filter(|movement| movement.progress == MoveProgress::Completed)
            .count()
            <= request
                .expected_plan
                .moves
                .iter()
                .filter(|movement| movement.progress == MoveProgress::Completed)
                .count()
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let plan_key = store.plan_key(request.plan.plan_id);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    let claim_revision = exact_claim(store, &claim_key, &request.expected_claim).await?;
    let plan_revision = exact_record(store, &plan_key, &request.expected_plan).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::mod_revision(claim_key, CompareOp::Equal, claim_revision),
            Compare::mod_revision(plan_key.clone(), CompareOp::Equal, plan_revision),
            state.compare,
        ],
        vec![
            TxnOp::put(slot_key, encode(&request.slot)?, None),
            TxnOp::put(plan_key, encode(&request.plan)?, None),
            state.put,
        ],
    )
    .await?;
    Ok(MoveCommit {
        slot: request.slot,
        plan: request.plan,
    })
}

pub(super) async fn commit_automatic_settings(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: CommitAutomaticSettings,
) -> Result<AutomaticBalanceSettings, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    if request.settings.version.term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    let settings_key = store.key("settings/automatic_balance");
    let operation_key = store.operation_key(&request.operation.operation_id);
    let operation_count = cardinality_counter(
        store,
        "admin_operations",
        1,
        store.limits.maximum_admin_operations,
    )
    .await?;
    let settings_compare = match &request.expected {
        Some(expected) => Compare::mod_revision(
            settings_key.clone(),
            CompareOp::Equal,
            exact_record(store, &settings_key, expected).await?,
        ),
        None => Compare::version(settings_key.clone(), CompareOp::Equal, 0),
    };
    commit(
        store,
        guard,
        vec![
            settings_compare,
            Compare::version(operation_key.clone(), CompareOp::Equal, 0),
            operation_count.compare,
        ],
        vec![
            TxnOp::put(settings_key, encode(&request.settings)?, None),
            TxnOp::put(operation_key, encode(&request.operation)?, None),
            operation_count.put,
        ],
    )
    .await?;
    Ok(request.settings)
}

pub(super) async fn create_plan_with_operation(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: CreatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    if request.plan.coordinator_term != guard.term() || request.plan.record_revision.get() != 1 {
        return Err(StorageError::InvalidRecord);
    }
    let plan_key = store.plan_key(request.plan.plan_id);
    let operation_key = store.operation_key(&request.operation.operation_id);
    let plan_count = cardinality_counter(store, "plans", 1, store.limits.maximum_plans).await?;
    let operation_count = cardinality_counter(
        store,
        "admin_operations",
        1,
        store.limits.maximum_admin_operations,
    )
    .await?;
    commit(
        store,
        guard,
        vec![
            Compare::version(plan_key.clone(), CompareOp::Equal, 0),
            Compare::version(operation_key.clone(), CompareOp::Equal, 0),
            plan_count.compare,
            operation_count.compare,
        ],
        vec![
            TxnOp::put(plan_key, encode(&request.plan)?, None),
            TxnOp::put(operation_key, encode(&request.operation)?, None),
            plan_count.put,
            operation_count.put,
        ],
    )
    .await?;
    Ok(PlanCommit { plan: request.plan })
}

pub(super) async fn update_plan_with_operation(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: UpdatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    validate_plan_update(&request.expected_plan, &request.plan)?;
    let plan_key = store.plan_key(request.plan.plan_id);
    let operation_key = store.operation_key(&request.operation.operation_id);
    let plan_revision = exact_record(store, &plan_key, &request.expected_plan).await?;
    let operation_count = cardinality_counter(
        store,
        "admin_operations",
        1,
        store.limits.maximum_admin_operations,
    )
    .await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(plan_key.clone(), CompareOp::Equal, plan_revision),
            Compare::version(operation_key.clone(), CompareOp::Equal, 0),
            operation_count.compare,
        ],
        vec![
            TxnOp::put(plan_key, encode(&request.plan)?, None),
            TxnOp::put(operation_key, encode(&request.operation)?, None),
            operation_count.put,
        ],
    )
    .await?;
    Ok(PlanCommit { plan: request.plan })
}

pub(super) async fn remove_member_with_operation(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: RemoveMemberWithOperation,
) -> Result<MemberCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    let state = state_counter(store, request.operation.version.revision).await?;
    let member_key = store.key(&format!("members/{}", request.expected_member.node.node_id));
    let operation_key = store.operation_key(&request.operation.operation_id);
    let member_revision = exact_record(store, &member_key, &request.expected_member).await?;
    let member_count =
        cardinality_counter(store, "members", -1, store.limits.maximum_members).await?;
    let operation_count = cardinality_counter(
        store,
        "admin_operations",
        1,
        store.limits.maximum_admin_operations,
    )
    .await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(member_key.clone(), CompareOp::Equal, member_revision),
            Compare::version(operation_key.clone(), CompareOp::Equal, 0),
            state.compare,
            member_count.compare,
            operation_count.compare,
        ],
        vec![
            TxnOp::delete(member_key, None),
            TxnOp::put(operation_key, encode(&request.operation)?, None),
            state.put,
            member_count.put,
            operation_count.put,
        ],
    )
    .await?;
    Ok(MemberCommit {
        member: request.expected_member,
        revision: request.operation.version.revision,
    })
}

pub(super) async fn record_admin_operation(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: RecordAdminOperation,
) -> Result<AdminOperationRecord, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    let key = store.operation_key(&request.operation.operation_id);
    let operation_count = cardinality_counter(
        store,
        "admin_operations",
        1,
        store.limits.maximum_admin_operations,
    )
    .await?;
    commit(
        store,
        guard,
        vec![
            Compare::version(key.clone(), CompareOp::Equal, 0),
            operation_count.compare,
        ],
        vec![
            TxnOp::put(key, encode(&request.operation)?, None),
            operation_count.put,
        ],
    )
    .await?;
    Ok(request.operation)
}

pub(super) async fn compact_admin_operations(
    store: &EtcdPlacementStore,
    guard: &LeaderGuard,
    request: CompactAdminOperations,
) -> Result<(), StorageError> {
    ensure_guard_live(store, guard).await?;
    if request.expected.is_empty() {
        return Ok(());
    }
    let delta = -i64::try_from(request.expected.len()).map_err(|_| StorageError::Capacity)?;
    let count = cardinality_counter(
        store,
        "admin_operations",
        delta,
        store.limits.maximum_admin_operations,
    )
    .await?;
    let mut compares = Vec::with_capacity(request.expected.len() + 1);
    let mut operations = Vec::with_capacity(request.expected.len());
    for record in request.expected {
        let key = store.operation_key(&record.operation_id);
        let revision = exact_record(store, &key, &record).await?;
        compares.push(Compare::mod_revision(
            key.clone(),
            CompareOp::Equal,
            revision,
        ));
        operations.push(TxnOp::delete(key, None));
    }
    compares.push(count.compare);
    operations.push(count.put);
    commit(store, guard, compares, operations).await
}
