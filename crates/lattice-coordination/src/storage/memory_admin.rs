use super::records::{
    AdminOperationRecord, AutomaticBalanceSettings, CommitAutomaticSettings,
    CompactAdminOperations, CreatePlanWithOperation, PlanCommit, RecordAdminOperation,
    UpdatePlanWithOperation,
};
use super::{
    ExactLeaderGuard, GroupLeaderGuard, InMemoryCoordinationStore, StorageError, set_revision,
    validate_admin_operation, validate_guard, validate_next_revision, validate_plan_group,
    validate_plan_update,
};

pub(super) fn commit_automatic_settings(
    store: &InMemoryCoordinationStore,
    guard: &GroupLeaderGuard,
    request: CommitAutomaticSettings,
) -> Result<AutomaticBalanceSettings, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    if request.settings.version.group != request.operation.version.group
        || request.settings.version.term != guard.term()
        || request
            .expected
            .as_ref()
            .is_some_and(|expected| expected.version.group != request.settings.version.group)
    {
        return Err(StorageError::InvalidRecord);
    }
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    let group = request.operation.version.group.clone();
    if state.automatic_settings.get(&group) != request.expected.as_ref()
        || state
            .admin_operations
            .contains_key(&(group.clone(), request.operation.operation_id.clone()))
        || request.settings.version.term != guard.term()
    {
        return Err(StorageError::CompareFailed);
    }
    if state
        .admin_operations
        .keys()
        .filter(|(candidate, _)| candidate == &group)
        .count()
        == store.maximum_admin_operations
    {
        return Err(StorageError::Capacity);
    }
    state
        .automatic_settings
        .insert(group.clone(), request.settings.clone());
    state.admin_operations.insert(
        (group, request.operation.operation_id.clone()),
        request.operation,
    );
    Ok(request.settings)
}

pub(super) fn create_plan_with_operation(
    store: &InMemoryCoordinationStore,
    guard: &GroupLeaderGuard,
    request: CreatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    validate_plan_group(guard, &request.plan)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    let group = request.plan.group.clone();
    if request.plan.coordinator_term != guard.term()
        || request.plan.record_revision.get() != 1
        || state
            .plans
            .contains_key(&(group.clone(), request.plan.plan_id))
        || state
            .admin_operations
            .contains_key(&(group.clone(), request.operation.operation_id.clone()))
    {
        return Err(StorageError::CompareFailed);
    }
    if state
        .plans
        .keys()
        .filter(|(candidate, _)| candidate == &group)
        .count()
        == store.maximum_plans
        || state
            .admin_operations
            .keys()
            .filter(|(candidate, _)| candidate == &group)
            .count()
            == store.maximum_admin_operations
    {
        return Err(StorageError::Capacity);
    }
    state
        .plans
        .insert((group.clone(), request.plan.plan_id), request.plan.clone());
    state.admin_operations.insert(
        (group, request.operation.operation_id.clone()),
        request.operation,
    );
    Ok(PlanCommit { plan: request.plan })
}

pub(super) fn update_plan_with_operation(
    store: &InMemoryCoordinationStore,
    guard: &GroupLeaderGuard,
    request: UpdatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    validate_plan_group(guard, &request.expected_plan)?;
    validate_plan_group(guard, &request.plan)?;
    validate_plan_update(&request.expected_plan, &request.plan)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    let group = request.plan.group.clone();
    if state
        .plans
        .get(&(group.clone(), request.expected_plan.plan_id))
        != Some(&request.expected_plan)
        || state
            .admin_operations
            .contains_key(&(group.clone(), request.operation.operation_id.clone()))
    {
        return Err(StorageError::CompareFailed);
    }
    if state
        .admin_operations
        .keys()
        .filter(|(candidate, _)| candidate == &group)
        .count()
        == store.maximum_admin_operations
    {
        return Err(StorageError::Capacity);
    }
    state
        .plans
        .insert((group.clone(), request.plan.plan_id), request.plan.clone());
    state.admin_operations.insert(
        (group, request.operation.operation_id.clone()),
        request.operation,
    );
    Ok(PlanCommit { plan: request.plan })
}

pub(super) fn record_admin_operation(
    store: &InMemoryCoordinationStore,
    guard: &GroupLeaderGuard,
    request: RecordAdminOperation,
) -> Result<AdminOperationRecord, StorageError> {
    validate_admin_operation(guard, &request.operation)?;
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    validate_next_revision(&state, guard.scope(), request.operation.version.revision)?;
    let group = request.operation.version.group.clone();
    if state
        .admin_operations
        .contains_key(&(group.clone(), request.operation.operation_id.clone()))
    {
        return Err(StorageError::CompareFailed);
    }
    if state
        .admin_operations
        .keys()
        .filter(|(candidate, _)| candidate == &group)
        .count()
        == store.maximum_admin_operations
    {
        return Err(StorageError::Capacity);
    }
    state.admin_operations.insert(
        (group, request.operation.operation_id.clone()),
        request.operation.clone(),
    );
    set_revision(
        &mut state,
        guard.scope(),
        request.operation.version.revision,
    );
    Ok(request.operation)
}

pub(super) fn compact_admin_operations(
    store: &InMemoryCoordinationStore,
    guard: &GroupLeaderGuard,
    request: CompactAdminOperations,
) -> Result<(), StorageError> {
    for record in &request.expected {
        validate_admin_operation(guard, record)?;
    }
    let mut state = store.inner.lock().expect("placement memory store poisoned");
    validate_guard(&state, guard)?;
    if request.expected.iter().any(|record| {
        state
            .admin_operations
            .get(&(record.version.group.clone(), record.operation_id.clone()))
            != Some(record)
    }) {
        return Err(StorageError::CompareFailed);
    }
    for record in request.expected {
        state
            .admin_operations
            .remove(&(record.version.group, record.operation_id));
    }
    Ok(())
}
