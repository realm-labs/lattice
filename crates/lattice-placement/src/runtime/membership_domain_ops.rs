impl<S> PlacementDomainLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    pub(super) async fn begin_member_drain(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
        operation_id: String,
        expected_incarnation: lattice_core::actor_ref::NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        if operation_id.is_empty()
            || operation_id.len() > 256
            || expected_incarnation != incarnation
        {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if session.draining {
            if session
                .drain_operation
                .as_ref()
                .is_some_and(|current| current != &operation_id)
            {
                return Err(CoordinatorRuntimeError::IdempotencyConflict);
            }
            let session = self
                .sessions
                .get_mut(&incarnation)
                .ok_or(CoordinatorRuntimeError::UnknownSession)?;
            session.drain_operation = Some(operation_id);
            return self.maybe_send_drain_ready(incarnation).await;
        }
        if session.record.status != MemberStatus::Up {
            return Err(CoordinatorRuntimeError::StaleMember);
        }
        let global_member = session.record.clone();
        let expected = session
            .domain_record
            .clone()
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        let source = session.hello.node.clone();
        let entity_types = session
            .hello
            .hosted_entity_types
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let version = self.next_version()?;
        let mut member = expected.clone();
        member.status = DomainMemberStatus::Leaving;
        member.version = version.clone();
        let member = self
            .store
            .update_domain_member(
                &self.leader_guard,
                UpdateDomainMember {
                    expected_global_member: global_member,
                    expected,
                    member,
                },
            )
            .await?
            .member;
        let session = self
            .sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        session.domain_record = Some(member.clone());
        session.draining = true;
        session.drain_operation = Some(operation_id);
        session.drain_ready = false;
        self.version = version;
        for entity_type in entity_types {
            let _ = self
                .evaluate_rebalance(
                    entity_type,
                    RebalanceTrigger::Drain {
                        node: source.clone(),
                    },
                )
                .await?;
        }
        let singletons = self
            .store
            .list_slots(&self.version.domain)
            .await?
            .into_iter()
            .filter(|slot| {
                slot.owner.as_ref() == Some(&source)
                    && matches!(slot.key, PlacementSlotKey::Singleton { .. })
            })
            .collect::<Vec<_>>();
        for slot in singletons {
            match self.begin_singleton_recovery(slot).await {
                Ok(()) | Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                Err(error) => return Err(error),
            }
        }
        self.maybe_send_drain_ready(incarnation).await
    }

    pub(super) async fn maybe_send_drain_ready(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if !session.draining || session.drain_ready {
            return Ok(());
        }
        let node = session.record.node.clone();
        if self
            .store
            .list_slots(&self.version.domain)
            .await?
            .iter()
            .any(|slot| slot.owner.as_ref() == Some(&node))
        {
            return Ok(());
        }
        let operation_id = session
            .drain_operation
            .clone()
            .ok_or(CoordinatorRuntimeError::DrainNotReady)?;
        let association_key = session.association.clone();
        let association = self
            .associations
            .get(&association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        send_control(
            &association,
            &self.version.domain,
            PlacementControlCommand::DrainReady {
                operation_id,
                expected_incarnation: incarnation,
            },
            &self.config,
        )?;
        self.sessions
            .get_mut(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?
            .drain_ready = true;
        Ok(())
    }

    pub(super) async fn complete_member_drain(
        &mut self,
        incarnation: lattice_core::actor_ref::NodeIncarnation,
        operation_id: &str,
        expected_incarnation: lattice_core::actor_ref::NodeIncarnation,
    ) -> Result<(), CoordinatorRuntimeError> {
        let session = self
            .sessions
            .get(&incarnation)
            .ok_or(CoordinatorRuntimeError::UnknownSession)?;
        if expected_incarnation != incarnation
            || !session.draining
            || !session.drain_ready
            || session.drain_operation.as_deref() != Some(operation_id)
        {
            return Err(CoordinatorRuntimeError::DrainNotReady);
        }
        let member = session.record.clone();
        self.remove_member(member, MemberRemovalReason::GracefulLeave)
            .await
    }

    pub(super) async fn remove_member(
        &mut self,
        member: MemberRecord,
        _reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        if let Some(domain_member) = self
            .store
            .get_domain_member(&self.version.domain, &member.node.node_id)
            .await?
        {
            self.store
                .remove_domain_member(
                    &self.leader_guard,
                    RemoveDomainMember {
                        expected: domain_member,
                    },
                )
                .await?;
            self.version = PlacementVersion::new(
                self.version.domain.clone(),
                self.version.term,
                self.store
                    .get_placement_revision(&self.version.domain)
                    .await?,
            );
        }
        self.finish_member_removal(member).await
    }

    pub(super) async fn finish_member_removal(
        &mut self,
        member: MemberRecord,
    ) -> Result<(), CoordinatorRuntimeError> {
        self.finish_node_removal(member.node).await
    }

    async fn finish_node_removal(&mut self, node: NodeKey) -> Result<(), CoordinatorRuntimeError> {
        let incarnation = node.incarnation;
        self.sessions.remove(&incarnation);
        let expired_claims = self
            .claims
            .iter()
            .filter_map(|(key, claim)| {
                (claim.grant.owner.incarnation == incarnation).then_some((
                    key.clone(),
                    claim.lease_id,
                    claim.grant.clone(),
                ))
            })
            .collect::<Vec<_>>();
        for (key, claim_lease, _grant) in expired_claims {
            self.store.revoke_lease(claim_lease).await?;
            self.claims.remove(&key);
        }
        let barriers = self
            .handoffs
            .iter()
            .filter_map(|(key, handoff)| {
                (handoff.phase == HandoffPhase::Invalidating
                    && handoff.required_sessions().contains(&incarnation))
                .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in barriers {
            self.transition_handoff(key, HandoffEvent::FenceSession(incarnation))
                .await?;
        }
        let owned_slots = self
            .store
            .list_slots(&self.version.domain)
            .await?
            .into_iter()
            .filter(|slot| slot.owner.as_ref() == Some(&node))
            .collect::<Vec<_>>();
        let entity_types = owned_slots
            .iter()
            .filter_map(|slot| match slot.key {
                PlacementSlotKey::Shard {
                    ref entity_type, ..
                } => Some(entity_type.clone()),
                PlacementSlotKey::Singleton { .. } => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        for entity_type in entity_types {
            let _ = self
                .evaluate_rebalance(
                    entity_type,
                    RebalanceTrigger::Recovery {
                        owner: node.clone(),
                    },
                )
                .await;
        }
        for slot in owned_slots {
            if matches!(slot.key, PlacementSlotKey::Singleton { .. }) {
                match self.begin_singleton_recovery(slot).await {
                    Ok(()) | Err(CoordinatorRuntimeError::IneligibleTarget) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        let remaining_slots = self.store.list_slots(&self.version.domain).await?;
        for slot in remaining_slots
            .into_iter()
            .filter(|slot| {
                slot.owner.as_ref() == Some(&node)
                    && slot.state == PlacementSlotState::Running
                    && slot.active_move.is_none()
            })
        {
            if self.store.get_claim(&slot.key).await?.is_none() {
                self.fence_missing_claim(slot).await?;
            }
        }
        Ok(())
    }

    async fn remove_global_member_participation(
        &mut self,
        node: NodeKey,
        _reason: MemberRemovalReason,
    ) -> Result<(), CoordinatorRuntimeError> {
        let Some(domain_member) = self
            .store
            .get_domain_member(&self.version.domain, &node.node_id)
            .await?
            .filter(|member| member.node == node)
        else {
            return Ok(());
        };
        self.store
            .remove_domain_member(
                &self.leader_guard,
                RemoveDomainMember {
                    expected: domain_member,
                },
            )
            .await?;
        self.version = PlacementVersion::new(
            self.version.domain.clone(),
            self.version.term,
            self.store
                .get_placement_revision(&self.version.domain)
                .await?,
        );
        self.finish_node_removal(node).await
    }

    pub(super) async fn resume_handoffs_for(
        &mut self,
        node: &NodeKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let candidates = self
            .handoffs
            .iter()
            .filter_map(|(key, handoff)| {
                ((handoff.phase == HandoffPhase::Draining
                    && (&handoff.source == node
                        || (&handoff.target == node
                            && matches!(key, PlacementSlotKey::Singleton { .. })
                            && !self.sessions.contains_key(&handoff.source.incarnation))))
                    || (handoff.phase == HandoffPhase::ReplacingAuthority
                        && &handoff.target == node))
                    .then_some((key.clone(), handoff.phase))
            })
            .collect::<Vec<_>>();
        for (key, phase) in candidates {
            match phase {
                HandoffPhase::Draining => {
                    let slot = self
                        .store
                        .get_slot(&key)
                        .await?
                        .ok_or(CoordinatorRuntimeError::UnknownSlot)?;
                    if slot.state == PlacementSlotState::Stopping {
                        let handoff = self
                            .handoffs
                            .get(&key)
                            .cloned()
                            .ok_or(CoordinatorRuntimeError::UnknownHandoff)?;
                        if &handoff.source == node {
                            let session = self
                                .sessions
                                .get(&node.incarnation)
                                .ok_or(CoordinatorRuntimeError::UnknownSession)?;
                            let association = self
                                .associations
                                .get(&session.association)
                                .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
                            send_control(
                                &association,
                                &self.version.domain,
                                PlacementControlCommand::DrainSlot {
                                    slot: key,
                                    generation: handoff.source_generation,
                                    version: slot.version,
                                },
                                &self.config,
                            )?;
                        } else if self.store.get_claim(&key).await?.is_none() {
                            self.transition_handoff(
                                key,
                                HandoffEvent::SourceAuthorityInvalid {
                                    source: handoff.source,
                                    generation: handoff.source_generation,
                                },
                            )
                            .await?;
                        }
                    }
                }
                HandoffPhase::ReplacingAuthority => self.replace_authority(&key).await?,
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) async fn send_snapshot(
        &mut self,
        hello: PlacementDomainHello,
        association_key: lattice_remoting::association::AssociationKey,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut placement_records = Vec::new();
        for member in self.store.list_domain_members(&self.version.domain).await? {
            placement_records.push(SnapshotRecord {
                key: format!(
                    "domain/{}/member/{}",
                    self.version.domain.as_str(),
                    member.node.node_id
                ),
                value: Bytes::from(
                    serde_json::to_vec(&member).map_err(|_| CoordinatorRuntimeError::Codec)?,
                ),
            });
        }
        for slot in self.store.list_slots(&self.version.domain).await? {
            if slot.key.domain() != &self.version.domain {
                continue;
            }
            let include = match &slot.key {
                PlacementSlotKey::Shard { entity_type, .. } => hello.subscribes_to(entity_type),
                PlacementSlotKey::Singleton { kind, .. } => {
                    hello.singleton_eligibility.contains(kind)
                        || hello.used_singletons.contains(kind)
                }
            };
            if include {
                placement_records.push(SnapshotRecord {
                    key: slot_record_key(&slot.key),
                    value: Bytes::from(
                        serde_json::to_vec(&slot).map_err(|_| CoordinatorRuntimeError::Codec)?,
                    ),
                });
            }
        }
        let placement = build_snapshot(
            SnapshotVersion::Placement(self.version.clone()),
            placement_records,
            &self.config.snapshot_limits,
        )
        .map_err(CoordinatorRuntimeError::Coordinator)?;
        let association = self
            .associations
            .get(&association_key)
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        for (begin, chunks, end) in [placement] {
            send_control(
                &association,
                &self.version.domain,
                PlacementControlCommand::SnapshotBegin(begin),
                &self.config,
            )?;
            for chunk in chunks {
                send_control(
                    &association,
                    &self.version.domain,
                    PlacementControlCommand::SnapshotChunk(chunk),
                    &self.config,
                )?;
            }
            send_control(
                &association,
                &self.version.domain,
                PlacementControlCommand::SnapshotEnd(end),
                &self.config,
            )?;
        }
        Ok(())
    }

    pub(super) async fn reconcile_claims_for(
        &mut self,
        hello: &PlacementDomainHello,
    ) -> Result<(), CoordinatorRuntimeError> {
        let association = self
            .sessions
            .get(&hello.node.incarnation)
            .and_then(|session| self.associations.get(&session.association))
            .ok_or(CoordinatorRuntimeError::AssociationUnavailable)?;
        for slot in self.store.list_slots(&self.version.domain).await? {
            if slot.owner.as_ref() != Some(&hello.node)
                || !matches!(
                    slot.state,
                    crate::types::PlacementSlotState::Allocating
                        | crate::types::PlacementSlotState::Running
                )
            {
                continue;
            }
            let Some(previous) = self.store.get_claim(&slot.key).await? else {
                continue;
            };
            if previous.grant.owner != hello.node
                || previous.grant.assignment_generation != slot.assignment_generation
            {
                return Err(CoordinatorRuntimeError::ClaimNotProven);
            }
            let committed = if previous.grant.coordinator_term == self.leader.term {
                crate::storage::domain::AuthorityCommit {
                    slot: slot.clone(),
                    claim: previous,
                }
            } else {
                let lease_id = self.store.grant_lease(self.config.claim_ttl).await?;
                let mut adopted_slot = slot.clone();
                adopted_slot.version = self.next_version()?;
                let grant = ClaimGrant {
                    domain: slot.key.domain().clone(),
                    slot: slot.key.clone(),
                    owner: hello.node.clone(),
                    coordinator_term: self.leader.term,
                    assignment_generation: slot.assignment_generation,
                    grant_sequence: previous
                        .grant
                        .grant_sequence
                        .next()
                        .map_err(|_| CoordinatorRuntimeError::ClaimSequence)?,
                    ttl: self.config.claim_ttl,
                };
                let (expected_global_member, expected_domain_member) =
                    self.assignment_members(&hello.node).await?;
                let result = self
                    .store
                    .adopt_authority(
                        &self.leader_guard,
                        crate::storage::domain::AdoptAuthority {
                            expected_global_member,
                            expected_domain_member,
                            expected_slot: slot.clone(),
                            expected_claim: previous.grant.clone(),
                            slot: adopted_slot,
                            claim: LeasedClaim { grant, lease_id },
                        },
                    )
                    .await;
                match result {
                    Ok(committed) => {
                        let _ = self.store.revoke_lease(previous.lease_id).await;
                        self.version = committed.slot.version.clone();
                        self.publish_slot_delta(&committed.slot).await?;
                        committed
                    }
                    Err(error) => {
                        let _ = self.store.revoke_lease(lease_id).await;
                        return Err(error.into());
                    }
                }
            };
            let lease_id = committed.claim.lease_id;
            let grant = committed.claim.grant;
            self.claims.insert(
                slot.key.clone(),
                ClaimLease {
                    lease_id,
                    grant: grant.clone(),
                },
            );
            send_control(
                &association,
                &self.version.domain,
                PlacementControlCommand::ClaimGranted(grant),
                &self.config,
            )?;
        }
        Ok(())
    }
}
