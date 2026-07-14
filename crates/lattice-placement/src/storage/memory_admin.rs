use super::domain::{
    AdminOperationRecord, AutomaticBalanceSettings, CommitAutomaticSettings,
    CompactAdminOperations, CreatePlanWithOperation, MemberCommit, PlanCommit,
    RecordAdminOperation, RemoveMemberWithOperation, UpdatePlanWithOperation,
};
use super::{
    InMemoryPlacementStore, LeaderGuard, StorageError, validate_admin_operation, validate_guard,
    validate_next_revision, validate_plan_update,
};

pub(super) fn commit_automatic_settings(
    store: &InMemoryPlacementStore,
    guard: &LeaderGuard,
    request: CommitAutomaticSettings,
) -> Result<AutomaticBalanceSettings, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    if state.automatic_settings != request.expected
        || state
            .admin_operations
            .contains_key(&request.operation.operation_id)
        || request.settings.version.term != guard.term()
    {
        return Err(StorageError::CompareFailed);
    }
    if state.admin_operations.len() == store.maximum_admin_operations {
        return Err(StorageError::Capacity);
    }
    state.automatic_settings = Some(request.settings.clone());
    state
        .admin_operations
        .insert(request.operation.operation_id.clone(), request.operation);
    Ok(request.settings)
}

pub(super) fn create_plan_with_operation(
    store: &InMemoryPlacementStore,
    guard: &LeaderGuard,
    request: CreatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    if request.plan.coordinator_term != guard.term()
        || request.plan.record_revision.get() != 1
        || state.plans.contains_key(&request.plan.plan_id)
        || state
            .admin_operations
            .contains_key(&request.operation.operation_id)
    {
        return Err(StorageError::CompareFailed);
    }
    if state.plans.len() == store.maximum_plans
        || state.admin_operations.len() == store.maximum_admin_operations
    {
        return Err(StorageError::Capacity);
    }
    state
        .plans
        .insert(request.plan.plan_id, request.plan.clone());
    state
        .admin_operations
        .insert(request.operation.operation_id.clone(), request.operation);
    Ok(PlanCommit { plan: request.plan })
}

pub(super) fn update_plan_with_operation(
    store: &InMemoryPlacementStore,
    guard: &LeaderGuard,
    request: UpdatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    validate_plan_update(&request.expected_plan, &request.plan)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    if state.plans.get(&request.expected_plan.plan_id) != Some(&request.expected_plan)
        || state
            .admin_operations
            .contains_key(&request.operation.operation_id)
    {
        return Err(StorageError::CompareFailed);
    }
    if state.admin_operations.len() == store.maximum_admin_operations {
        return Err(StorageError::Capacity);
    }
    state
        .plans
        .insert(request.plan.plan_id, request.plan.clone());
    state
        .admin_operations
        .insert(request.operation.operation_id.clone(), request.operation);
    Ok(PlanCommit { plan: request.plan })
}

pub(super) fn remove_member_with_operation(
    store: &InMemoryPlacementStore,
    guard: &LeaderGuard,
    request: RemoveMemberWithOperation,
) -> Result<MemberCommit, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    validate_next_revision(&state, request.operation.version.revision)?;
    if state.members.get(&request.expected_member.node.node_id) != Some(&request.expected_member)
        || state
            .admin_operations
            .contains_key(&request.operation.operation_id)
    {
        return Err(StorageError::CompareFailed);
    }
    if state.admin_operations.len() == store.maximum_admin_operations {
        return Err(StorageError::Capacity);
    }
    state.state_revision = Some(request.operation.version.revision);
    state.members.remove(&request.expected_member.node.node_id);
    state.admin_operations.insert(
        request.operation.operation_id.clone(),
        request.operation.clone(),
    );
    Ok(MemberCommit {
        member: request.expected_member,
        revision: request.operation.version.revision,
    })
}

pub(super) fn record_admin_operation(
    store: &InMemoryPlacementStore,
    guard: &LeaderGuard,
    request: RecordAdminOperation,
) -> Result<AdminOperationRecord, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    if state
        .admin_operations
        .contains_key(&request.operation.operation_id)
    {
        return Err(StorageError::CompareFailed);
    }
    if state.admin_operations.len() == store.maximum_admin_operations {
        return Err(StorageError::Capacity);
    }
    state.admin_operations.insert(
        request.operation.operation_id.clone(),
        request.operation.clone(),
    );
    Ok(request.operation)
}

pub(super) fn compact_admin_operations(
    store: &InMemoryPlacementStore,
    guard: &LeaderGuard,
    request: CompactAdminOperations,
) -> Result<(), StorageError> {
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    if request
        .expected
        .iter()
        .any(|record| state.admin_operations.get(&record.operation_id) != Some(record))
    {
        return Err(StorageError::CompareFailed);
    }
    for record in request.expected {
        state.admin_operations.remove(&record.operation_id);
    }
    Ok(())
}
