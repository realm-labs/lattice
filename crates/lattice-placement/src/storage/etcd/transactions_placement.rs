pub(super) async fn create_plan(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: CreatePlan,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_plan_domain(guard, &request.plan)?;
    if request.plan.coordinator_term != guard.term() || request.plan.record_revision.get() != 1 {
        return Err(StorageError::InvalidRecord);
    }
    let key = store.plan_key(&request.plan.domain, request.plan.plan_id);
    let count =
        cardinality_counter(store, guard.scope(), "plans", 1, store.limits.maximum_plans).await?;
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
    guard: &PlacementLeaderGuard,
    request: UpdatePlan,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_plan_domain(guard, &request.expected)?;
    validate_plan_domain(guard, &request.plan)?;
    validate_plan_update(&request.expected, &request.plan)?;
    let key = store.plan_key(&request.plan.domain, request.plan.plan_id);
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
    guard: &PlacementLeaderGuard,
    request: DeletePlan,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_plan_domain(guard, &request.expected)?;
    let key = store.plan_key(&request.expected.domain, request.expected.plan_id);
    let revision = exact_record(store, &key, &request.expected).await?;
    let count = cardinality_counter(
        store,
        guard.scope(),
        "plans",
        -1,
        store.limits.maximum_plans,
    )
    .await?;
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
    guard: &PlacementLeaderGuard,
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
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
    guard: &PlacementLeaderGuard,
    request: AllocateInitial,
) -> Result<AuthorityCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, None, &request.slot)?;
    validate_claim(&request.claim, &request.slot)?;
    let assignment = assignment_compares(
        store,
        &request.expected_global_member,
        &request.expected_domain_member,
        request
            .slot
            .owner
            .as_ref()
            .ok_or(StorageError::InvalidRecord)?,
    )
    .await?;
    if request.slot.state != PlacementSlotState::Allocating || request.slot.active_move.is_some() {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
    let count =
        cardinality_counter(store, guard.scope(), "slots", 1, store.limits.maximum_slots).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    commit(
        store,
        guard,
        vec![
            Compare::version(slot_key.clone(), CompareOp::Equal, 0),
            Compare::version(claim_key.clone(), CompareOp::Equal, 0),
            assignment[0].clone(),
            assignment[1].clone(),
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
    guard: &PlacementLeaderGuard,
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
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
    guard: &PlacementLeaderGuard,
    request: ReserveMove,
) -> Result<MoveCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_plan_domain(guard, &request.expected_plan)?;
    validate_plan_domain(guard, &request.plan)?;
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let plan_key = store.plan_key(&request.plan.domain, request.plan.plan_id);
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
    guard: &PlacementLeaderGuard,
    request: ReserveHandoff,
) -> Result<SlotCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    if !matches!(
        request.slot.key,
        crate::types::PlacementSlotKey::Singleton { .. }
    ) || request.expected_slot.state != PlacementSlotState::Running
        || request.expected_slot.active_move.is_some()
        || request.slot.state != PlacementSlotState::BeginHandoff
        || request.slot.active_move.is_none()
        || request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
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
    guard: &PlacementLeaderGuard,
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
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
    guard: &PlacementLeaderGuard,
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
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
    guard: &PlacementLeaderGuard,
    request: InstallAuthority,
) -> Result<AuthorityCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_claim(&request.claim, &request.slot)?;
    let assignment = assignment_compares(
        store,
        &request.expected_global_member,
        &request.expected_domain_member,
        request
            .slot
            .owner
            .as_ref()
            .ok_or(StorageError::InvalidRecord)?,
    )
    .await?;
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let slot_revision = exact_record(store, &slot_key, &request.expected_slot).await?;
    commit(
        store,
        guard,
        vec![
            Compare::mod_revision(slot_key.clone(), CompareOp::Equal, slot_revision),
            Compare::version(claim_key.clone(), CompareOp::Equal, 0),
            assignment[0].clone(),
            assignment[1].clone(),
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
    guard: &PlacementLeaderGuard,
    request: AdoptAuthority,
) -> Result<AuthorityCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_claim(&request.claim, &request.slot)?;
    let assignment = assignment_compares(
        store,
        &request.expected_global_member,
        &request.expected_domain_member,
        request
            .slot
            .owner
            .as_ref()
            .ok_or(StorageError::InvalidRecord)?,
    )
    .await?;
    if request.expected_slot.owner != request.slot.owner
        || request.expected_slot.assignment_generation != request.slot.assignment_generation
        || request.expected_slot.state != request.slot.state
        || request.expected_claim.owner != request.claim.grant.owner
        || request.expected_claim.assignment_generation != request.claim.grant.assignment_generation
        || request.expected_claim.coordinator_term >= request.claim.grant.coordinator_term
    {
        return Err(StorageError::InvalidTransition);
    }
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
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
            assignment[0].clone(),
            assignment[1].clone(),
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
    guard: &PlacementLeaderGuard,
    request: CompleteMove,
) -> Result<MoveCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_slot(guard, Some(&request.expected_slot), &request.slot)?;
    validate_plan_domain(guard, &request.expected_plan)?;
    validate_plan_domain(guard, &request.plan)?;
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
    let state = state_counter(store, guard.scope(), request.slot.version.revision).await?;
    let slot_key = store.slot_key(&request.slot.key);
    let claim_key = store.claim_key(&request.slot.key);
    let plan_key = store.plan_key(&request.plan.domain, request.plan.plan_id);
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
    guard: &PlacementLeaderGuard,
    request: CommitAutomaticSettings,
) -> Result<AutomaticBalanceSettings, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    if request.settings.version.term != guard.term()
        || request.settings.version.domain != request.operation.version.domain
        || request
            .expected
            .as_ref()
            .is_some_and(|expected| expected.version.domain != request.settings.version.domain)
    {
        return Err(StorageError::InvalidRecord);
    }
    let settings_key = store.key(&format!(
        "domains/{}/settings/automatic_balance",
        request.operation.version.domain.as_str()
    ));
    let operation_key = store.operation_key(
        &request.operation.version.domain,
        &request.operation.operation_id,
    );
    let operation_count = cardinality_counter(
        store,
        guard.scope(),
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
    guard: &PlacementLeaderGuard,
    request: CreatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    validate_plan_domain(guard, &request.plan)?;
    if request.plan.coordinator_term != guard.term() || request.plan.record_revision.get() != 1 {
        return Err(StorageError::InvalidRecord);
    }
    let plan_key = store.plan_key(&request.plan.domain, request.plan.plan_id);
    let operation_key = store.operation_key(
        &request.operation.version.domain,
        &request.operation.operation_id,
    );
    let plan_count =
        cardinality_counter(store, guard.scope(), "plans", 1, store.limits.maximum_plans).await?;
    let operation_count = cardinality_counter(
        store,
        guard.scope(),
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
    guard: &PlacementLeaderGuard,
    request: UpdatePlanWithOperation,
) -> Result<PlanCommit, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    validate_plan_domain(guard, &request.expected_plan)?;
    validate_plan_domain(guard, &request.plan)?;
    validate_plan_update(&request.expected_plan, &request.plan)?;
    let plan_key = store.plan_key(&request.plan.domain, request.plan.plan_id);
    let operation_key = store.operation_key(
        &request.operation.version.domain,
        &request.operation.operation_id,
    );
    let plan_revision = exact_record(store, &plan_key, &request.expected_plan).await?;
    let operation_count = cardinality_counter(
        store,
        guard.scope(),
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

pub(super) async fn record_admin_operation(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: RecordAdminOperation,
) -> Result<AdminOperationRecord, StorageError> {
    ensure_guard_live(store, guard).await?;
    validate_operation(guard, &request.operation)?;
    let key = store.operation_key(
        &request.operation.version.domain,
        &request.operation.operation_id,
    );
    let operation_count = cardinality_counter(
        store,
        guard.scope(),
        "admin_operations",
        1,
        store.limits.maximum_admin_operations,
    )
    .await?;
    let state = state_counter(store, guard.scope(), request.operation.version.revision).await?;
    commit(
        store,
        guard,
        vec![
            Compare::version(key.clone(), CompareOp::Equal, 0),
            operation_count.compare,
            state.compare,
        ],
        vec![
            TxnOp::put(key, encode(&request.operation)?, None),
            operation_count.put,
            state.put,
        ],
    )
    .await?;
    Ok(request.operation)
}

pub(super) async fn compact_admin_operations(
    store: &EtcdPlacementStore,
    guard: &PlacementLeaderGuard,
    request: CompactAdminOperations,
) -> Result<(), StorageError> {
    ensure_guard_live(store, guard).await?;
    for record in &request.expected {
        validate_operation(guard, record)?;
    }
    if request.expected.is_empty() {
        return Ok(());
    }
    let delta = -i64::try_from(request.expected.len()).map_err(|_| StorageError::Capacity)?;
    let count = cardinality_counter(
        store,
        guard.scope(),
        "admin_operations",
        delta,
        store.limits.maximum_admin_operations,
    )
    .await?;
    let mut compares = Vec::with_capacity(request.expected.len() + 1);
    let mut operations = Vec::with_capacity(request.expected.len());
    for record in request.expected {
        let key = store.operation_key(&record.version.domain, &record.operation_id);
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
