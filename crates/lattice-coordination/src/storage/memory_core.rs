use crate::types::{ClaimGrant, NodeKey};

impl InMemoryCoordinationStore {
    pub fn new(maximum_slots: usize, maximum_plans: usize) -> Result<Self, StorageError> {
        if maximum_slots == 0 || maximum_plans == 0 {
            return Err(StorageError::ZeroLimit);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(MemoryState::default())),
            counters: Arc::new(StoreReadCounters::default()),
            maximum_slots,
            maximum_plans,
            maximum_members: maximum_slots,
            maximum_admin_operations: maximum_plans,
        })
    }

    /// Reports the reads this store served. Clones share one set of counters, so a leader that
    /// holds the store behind an `Arc` is measured through any handle to it.
    pub fn read_counts(&self) -> StoreReadCounts {
        self.counters.snapshot()
    }

    pub fn reset_read_counts(&self) {
        self.counters.reset();
    }

    #[cfg(test)]
    pub(crate) fn insert_slot_record(&self, slot: PlacementSlot) {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        state
            .placement_revisions
            .insert(slot.version.group.clone(), slot.version.revision);
        state.slots.insert(slot.key.clone(), slot);
    }

    #[cfg(test)]
    pub(crate) fn remove_slot_record(&self, key: &PlacementSlotKey) {
        self.inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .remove(key);
    }

    async fn fence_missing_authority_inner(
        &self,
        guard: &GroupLeaderGuard,
        request: FenceMissingAuthority,
    ) -> Result<SlotCommit, StorageError> {
        let mut state = self.inner.lock().expect("placement memory store poisoned");
        validate_guard(&state, guard)?;
        validate_slot_common(guard, &state, Some(&request.expected_slot), &request.slot)?;
        if state.claims.contains_key(&request.slot.key)
            || !matches!(
                request.expected_slot.state,
                PlacementSlotState::Allocating | PlacementSlotState::Running
            )
            || request.slot.state != PlacementSlotState::Fenced
            || request.expected_slot.owner != request.slot.owner
            || request.expected_slot.assignment_generation != request.slot.assignment_generation
            || request.expected_slot.active_move != request.slot.active_move
        {
            return Err(StorageError::InvalidTransition);
        }
        set_revision(&mut state, guard.scope(), request.slot.version.revision);
        state
            .slots
            .insert(request.slot.key.clone(), request.slot.clone());
        Ok(SlotCommit { slot: request.slot })
    }
}

fn initial_revision() -> Revision {
    Revision::new(1).expect("one is a valid state revision")
}

fn validate_guard(state: &MemoryState, guard: &impl ExactLeaderGuard) -> Result<(), StorageError> {
    let Some((lease_id, leader)) = state.leaders.get(guard.scope()) else {
        return Err(StorageError::LeadershipLost);
    };
    if leader != guard.record()
        || state.leader_terms.get(guard.scope()).copied().unwrap_or(0) != guard.term().get()
        || !state.leases.contains_key(lease_id)
    {
        return Err(StorageError::LeadershipLost);
    }
    Ok(())
}

fn validate_member_record(member: &MemberRecord) -> Result<(), StorageError> {
    if member.node != member.hello.node || member.lease_id <= 0 || member.node.validate().is_err() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_group_member_record(
    guard: &GroupLeaderGuard,
    member: &GroupMemberRecord,
) -> Result<(), StorageError> {
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    member
        .validate(&SessionLimits::default())
        .map_err(|_| StorageError::InvalidRecord)?;
    if &member.hello.group != group || &member.version.group != group {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_assignment_members(
    state: &MemoryState,
    global_member: &MemberRecord,
    group_member: &GroupMemberRecord,
    owner: &NodeKey,
) -> Result<(), StorageError> {
    if global_member.status != MemberStatus::Up
        || group_member.status != GroupMemberStatus::Up
        || &global_member.node != owner
        || &group_member.node != owner
        || state.members.get(&owner.node_id) != Some(global_member)
        || state
            .group_members
            .get(&(group_member.version.group.clone(), owner.node_id.clone()))
            != Some(group_member)
    {
        return Err(StorageError::CompareFailed);
    }
    Ok(())
}

fn validate_admin_operation(
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

fn current_revision(state: &MemoryState, scope: &CoordinatorScope) -> Revision {
    match scope {
        CoordinatorScope::Cluster => state.membership_revision,
        CoordinatorScope::Group(group) => state.placement_revisions.get(group).copied(),
    }
    .unwrap_or_else(initial_revision)
}

fn set_revision(state: &mut MemoryState, scope: &CoordinatorScope, revision: Revision) {
    match scope {
        CoordinatorScope::Cluster => state.membership_revision = Some(revision),
        CoordinatorScope::Group(group) => {
            state.placement_revisions.insert(group.clone(), revision);
        }
    }
}

fn validate_next_revision(
    state: &MemoryState,
    scope: &CoordinatorScope,
    revision: Revision,
) -> Result<(), StorageError> {
    let expected = current_revision(state, scope)
        .next()
        .map_err(|_| StorageError::CounterExhausted)?;
    if revision != expected {
        return Err(StorageError::CompareFailed);
    }
    Ok(())
}

fn validate_plan_update(
    expected: &RebalancePlan,
    plan: &RebalancePlan,
) -> Result<(), StorageError> {
    if expected.plan_id != plan.plan_id
        || expected.group != plan.group
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
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if &plan.group != group || plan.coordinator_term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn validate_claim_lease(state: &MemoryState, claim: &LeasedClaim) -> Result<(), StorageError> {
    if claim.lease_id <= 0
        || !state.leases.contains_key(&claim.lease_id)
        || claim.grant.ttl.is_zero()
    {
        return Err(StorageError::InvalidRecord);
    }
    Ok(())
}

fn claim_matches(state: &MemoryState, expected: &ClaimGrant) -> bool {
    state
        .claims
        .get(&expected.slot)
        .is_some_and(|current| current.grant == *expected)
}

fn validate_slot_common(
    guard: &GroupLeaderGuard,
    state: &MemoryState,
    expected: Option<&PlacementSlot>,
    slot: &PlacementSlot,
) -> Result<(), StorageError> {
    slot.validate().map_err(|_| StorageError::InvalidRecord)?;
    if slot.version.term != guard.term() {
        return Err(StorageError::InvalidRecord);
    }
    if let Some(expected) = expected
        && (expected.key != slot.key || state.slots.get(&slot.key) != Some(expected))
    {
        return Err(StorageError::CompareFailed);
    }
    let CoordinatorScope::Group(group) = guard.scope() else {
        return Err(StorageError::InvalidRecord);
    };
    if slot.key.group() != group || &slot.version.group != group {
        return Err(StorageError::InvalidRecord);
    }
    validate_next_revision(state, guard.scope(), slot.version.revision)
}

impl InMemoryCoordinationStore {
    fn durable_limits_inner(&self) -> DurableStorageLimits {
        DurableStorageLimits {
            maximum_slots: self.maximum_slots,
            maximum_plans: self.maximum_plans,
            maximum_members: self.maximum_members,
            maximum_admin_operations: self.maximum_admin_operations,
            maximum_entity_configs: self.maximum_members,
            maximum_singleton_configs: self.maximum_members,
        }
    }

    async fn get_membership_revision_inner(&self) -> Result<Revision, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .membership_revision
            .unwrap_or_else(initial_revision))
    }

    async fn get_slot(
        &self,
        key: &PlacementSlotKey,
    ) -> Result<Option<PlacementSlot>, StorageError> {
        self.counters.record(|counts| {
            counts.get_slot += 1;
            counts.slot_records += 1;
        });
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .get(key)
            .cloned())
    }

    async fn get_plan(
        &self,
        group: &ActorGroupId,
        plan_id: u128,
    ) -> Result<Option<RebalancePlan>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .plans
            .get(&(group.clone(), plan_id))
            .cloned())
    }

    async fn get_claim(&self, key: &PlacementSlotKey) -> Result<Option<LeasedClaim>, StorageError> {
        self.counters.record(|counts| counts.get_claim += 1);
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .claims
            .get(key)
            .cloned())
    }

    async fn get_member(&self, node_id: &str) -> Result<Option<MemberRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .members
            .get(node_id)
            .cloned())
    }

    async fn list_members(&self) -> Result<Vec<MemberRecord>, StorageError> {
        self.counters.record(|counts| counts.list_members += 1);
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .members
            .values()
            .cloned()
            .collect())
    }

    async fn get_group_member(
        &self,
        group: &ActorGroupId,
        node_id: &str,
    ) -> Result<Option<GroupMemberRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .group_members
            .get(&(group.clone(), node_id.to_owned()))
            .cloned())
    }

    async fn list_group_members(
        &self,
        group: &ActorGroupId,
    ) -> Result<Vec<GroupMemberRecord>, StorageError> {
        self.counters
            .record(|counts| counts.list_group_members += 1);
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .group_members
            .iter()
            .filter(|((candidate, _), _)| candidate == group)
            .map(|(_, member)| member.clone())
            .collect())
    }

    async fn list_slots(&self, group: &ActorGroupId) -> Result<Vec<PlacementSlot>, StorageError> {
        let slots = self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .slots
            .values()
            .filter(|slot| slot.key.group() == group)
            .cloned()
            .collect::<Vec<_>>();
        self.counters.record(|counts| {
            counts.list_slots += 1;
            counts.slot_records += u64::try_from(slots.len()).unwrap_or(u64::MAX);
        });
        Ok(slots)
    }

    async fn list_plans(&self, group: &ActorGroupId) -> Result<Vec<RebalancePlan>, StorageError> {
        self.counters.record(|counts| counts.list_plans += 1);
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .plans
            .values()
            .filter(|plan| &plan.group == group)
            .cloned()
            .collect())
    }

    async fn list_claims(&self, group: &ActorGroupId) -> Result<Vec<LeasedClaim>, StorageError> {
        self.counters.record(|counts| counts.list_claims += 1);
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .claims
            .values()
            .filter(|claim| &claim.grant.group == group)
            .cloned()
            .collect())
    }

    async fn get_automatic_settings(
        &self,
        group: &ActorGroupId,
    ) -> Result<Option<AutomaticBalanceSettings>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .automatic_settings
            .get(group)
            .cloned())
    }

    async fn get_admin_operation(
        &self,
        group: &ActorGroupId,
        operation_id: &str,
    ) -> Result<Option<AdminOperationRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .admin_operations
            .get(&(group.clone(), operation_id.to_owned()))
            .cloned())
    }

    async fn list_admin_operations(&self) -> Result<Vec<AdminOperationRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .expect("placement memory store poisoned")
            .admin_operations
            .values()
            .cloned()
            .collect())
    }
}
