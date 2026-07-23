use std::{collections::BTreeMap, sync::Arc, time::Duration};

use lattice_core::{coordinator::CoordinatorScope, failpoint::Failpoint};
use tokio::{
    sync::{broadcast, watch},
    time::MissedTickBehavior,
};

use super::CoordinatorRuntimeError;
use crate::{
    coordinator::{
        COORDINATOR_PROTOCOL_GENERATION, LeaderRecord, MemberChange, MemberEvent, MemberHello,
        MemberRecord, MemberRemovalReason, MemberStatus, MembershipLeaderGuard, SessionLimits,
    },
    storage::{
        CoordinatorLeaseStore, MembershipStore, ScopedElectionStore,
        domain::{CreateMember, RemoveExpiredMember, RemoveMember, UpdateMember},
    },
    types::{CoordinatorTerm, MembershipVersion, NodeKey},
};

#[derive(Debug, Clone)]
pub struct MembershipLeaderConfig {
    pub leader_lease_ttl: Duration,
    pub member_lease_ttl: Duration,
    pub renewal_interval: Duration,
    pub maximum_events: usize,
    pub session_limits: SessionLimits,
}

impl Default for MembershipLeaderConfig {
    fn default() -> Self {
        Self {
            leader_lease_ttl: Duration::from_secs(10),
            member_lease_ttl: Duration::from_secs(15),
            renewal_interval: Duration::from_secs(5),
            maximum_events: 256,
            session_limits: SessionLimits::default(),
        }
    }
}

impl MembershipLeaderConfig {
    fn validate(&self) -> Result<(), CoordinatorRuntimeError> {
        if self.leader_lease_ttl.is_zero()
            || self.member_lease_ttl.is_zero()
            || self.renewal_interval.is_zero()
            || self.renewal_interval >= self.leader_lease_ttl
            || self.renewal_interval >= self.member_lease_ttl
            || self.maximum_events == 0
        {
            return Err(CoordinatorRuntimeError::InvalidConfig);
        }
        Ok(())
    }
}

/// The sole runtime writer for cluster-wide membership state.
///
/// Placement-domain leaders consume the exact records produced here; they do
/// not receive this guard and therefore cannot mutate global membership.
pub struct MembershipLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore,
{
    store: Arc<S>,
    leader: LeaderRecord,
    guard: MembershipLeaderGuard,
    leader_lease_id: i64,
    config: MembershipLeaderConfig,
    version: MembershipVersion,
    known_members: BTreeMap<String, MemberRecord>,
    events: broadcast::Sender<MemberEvent>,
}

impl<S> MembershipLeader<S>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore,
{
    pub async fn elect(
        store: Arc<S>,
        node: NodeKey,
        term: CoordinatorTerm,
        config: MembershipLeaderConfig,
    ) -> Result<Self, CoordinatorRuntimeError> {
        config.validate()?;
        store.ensure_schema_generation().await?;
        let leader_lease_id = store.grant_lease(config.leader_lease_ttl).await?;
        let leader = LeaderRecord {
            scope: CoordinatorScope::Membership,
            node,
            protocol_generation: COORDINATOR_PROTOCOL_GENERATION,
            term,
        };
        if !store.campaign_leader(&leader, leader_lease_id).await? {
            let _ = store.revoke_lease(leader_lease_id).await;
            return Err(CoordinatorRuntimeError::NotLeader);
        }
        let revision = store.get_membership_revision().await?;
        let known_members = store
            .list_members()
            .await?
            .into_iter()
            .map(|member| (member.node.node_id.clone(), member))
            .collect();
        let (events, _) = broadcast::channel(config.maximum_events);
        Ok(Self {
            store,
            guard: MembershipLeaderGuard::new(leader.clone())
                .map_err(CoordinatorRuntimeError::Coordinator)?,
            leader,
            leader_lease_id,
            config,
            version: MembershipVersion::new(term, revision),
            known_members,
            events,
        })
    }

    pub fn leader(&self) -> &LeaderRecord {
        &self.leader
    }

    pub fn version(&self) -> MembershipVersion {
        self.version
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MemberEvent> {
        self.events.subscribe()
    }

    pub async fn renew_leadership(&self) -> Result<(), CoordinatorRuntimeError> {
        self.store.keep_lease_alive(self.leader_lease_id).await?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), CoordinatorRuntimeError> {
        self.store.revoke_lease(self.leader_lease_id).await?;
        Ok(())
    }

    pub async fn join(
        &mut self,
        hello: MemberHello,
    ) -> Result<MemberRecord, CoordinatorRuntimeError> {
        hello
            .validate(&self.config.session_limits)
            .map_err(CoordinatorRuntimeError::Coordinator)?;
        if let Some(current) = self.store.get_member(&hello.node.node_id).await? {
            if current.node == hello.node && current.hello == hello {
                self.store.keep_lease_alive(current.lease_id).await?;
                if current.version.term == self.version.term {
                    self.known_members
                        .insert(current.node.node_id.clone(), current.clone());
                    return Ok(current);
                }
                let mut member = current.clone();
                member.version = self.next_version()?;
                lattice_core::failpoint::hit(Failpoint::MemberBeforeGuardedCommit);
                let committed = self
                    .store
                    .update_member(
                        &self.guard,
                        UpdateMember {
                            expected: current,
                            member,
                        },
                    )
                    .await?
                    .member;
                self.version = committed.version;
                self.known_members
                    .insert(committed.node.node_id.clone(), committed.clone());
                self.publish(MemberChange::Upsert(Box::new(committed.clone())));
                return Ok(committed);
            }
            return Err(CoordinatorRuntimeError::IncarnationPending {
                predecessor: current.node.incarnation,
                remaining_ttl: self.store.lease_time_to_live(current.lease_id).await?,
            });
        }
        if let Some(expired) = self.known_members.get(&hello.node.node_id).cloned() {
            let committed = self
                .store
                .remove_expired_member(
                    &self.guard,
                    RemoveExpiredMember {
                        expected: expired.clone(),
                    },
                )
                .await?;
            self.version = MembershipVersion::new(self.version.term, committed.revision);
            self.known_members.remove(&expired.node.node_id);
            self.publish(MemberChange::Removed {
                node: expired.node,
                reason: MemberRemovalReason::FailureDetected,
            });
        }
        let lease_id = self.store.grant_lease(self.config.member_lease_ttl).await?;
        let member = MemberRecord {
            node: hello.node.clone(),
            hello,
            status: MemberStatus::Joining,
            version: self.next_version()?,
            lease_id,
        };
        lattice_core::failpoint::hit(Failpoint::MemberBeforeGuardedCommit);
        let committed = match self
            .store
            .create_member(
                &self.guard,
                CreateMember {
                    member: member.clone(),
                },
            )
            .await
        {
            Ok(committed) => committed,
            Err(error) => {
                let _ = self.store.revoke_lease(lease_id).await;
                return Err(error.into());
            }
        };
        self.version = committed.member.version;
        self.known_members.insert(
            committed.member.node.node_id.clone(),
            committed.member.clone(),
        );
        self.publish(MemberChange::Upsert(Box::new(committed.member.clone())));
        Ok(committed.member)
    }

    pub async fn mark_up(
        &mut self,
        node: &NodeKey,
    ) -> Result<MemberRecord, CoordinatorRuntimeError> {
        self.transition(node, MemberStatus::Joining, MemberStatus::Up)
            .await
    }

    pub async fn begin_leave(
        &mut self,
        node: &NodeKey,
    ) -> Result<MemberRecord, CoordinatorRuntimeError> {
        self.transition(node, MemberStatus::Up, MemberStatus::Leaving)
            .await
    }

    pub async fn heartbeat(&self, node: &NodeKey) -> Result<(), CoordinatorRuntimeError> {
        let member = self
            .store
            .get_member(&node.node_id)
            .await?
            .filter(|member| &member.node == node)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        self.store.keep_lease_alive(member.lease_id).await?;
        Ok(())
    }

    pub async fn remove(
        &mut self,
        node: &NodeKey,
        reason: MemberRemovalReason,
    ) -> Result<MemberRecord, CoordinatorRuntimeError> {
        let member = self
            .store
            .get_member(&node.node_id)
            .await?
            .filter(|member| &member.node == node)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        lattice_core::failpoint::hit(Failpoint::MemberBeforeGuardedCommit);
        let committed = self
            .store
            .remove_member(
                &self.guard,
                RemoveMember {
                    expected: member.clone(),
                },
            )
            .await?;
        self.version = MembershipVersion::new(self.version.term, committed.revision);
        self.store.revoke_lease(member.lease_id).await?;
        self.known_members.remove(&member.node.node_id);
        self.publish(MemberChange::Removed {
            node: member.node.clone(),
            reason,
        });
        Ok(member)
    }

    /// Converts lease-driven key deletion into a revisioned membership event.
    ///
    /// Etcd removes a member key automatically when its lease expires. Without this guarded
    /// reconciliation commit, the durable membership revision and remote member directories
    /// would never observe that deletion.
    pub async fn reconcile_expired_members(&mut self) -> Result<(), CoordinatorRuntimeError> {
        let current = self
            .store
            .list_members()
            .await?
            .into_iter()
            .map(|member| (member.node.node_id.clone(), member))
            .collect::<BTreeMap<_, _>>();
        let expired = self
            .known_members
            .iter()
            .filter(|(node_id, _)| !current.contains_key(*node_id))
            .map(|(_, member)| member.clone())
            .collect::<Vec<_>>();
        self.known_members.extend(current);
        for member in expired {
            let committed = self
                .store
                .remove_expired_member(
                    &self.guard,
                    RemoveExpiredMember {
                        expected: member.clone(),
                    },
                )
                .await?;
            self.version = MembershipVersion::new(self.version.term, committed.revision);
            self.known_members.remove(&member.node.node_id);
            self.publish(MemberChange::Removed {
                node: member.node,
                reason: MemberRemovalReason::FailureDetected,
            });
        }
        Ok(())
    }

    pub async fn run(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), CoordinatorRuntimeError> {
        let mut renewal = tokio::time::interval(self.config.renewal_interval);
        renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.store.revoke_lease(self.leader_lease_id).await?;
                        return Ok(());
                    }
                }
                _ = renewal.tick() => self.store.keep_lease_alive(self.leader_lease_id).await?,
            }
        }
    }

    async fn transition(
        &mut self,
        node: &NodeKey,
        expected_status: MemberStatus,
        status: MemberStatus,
    ) -> Result<MemberRecord, CoordinatorRuntimeError> {
        let expected = self
            .store
            .get_member(&node.node_id)
            .await?
            .filter(|member| &member.node == node && member.status == expected_status)
            .ok_or(CoordinatorRuntimeError::StaleMember)?;
        let mut member = expected.clone();
        member.status = status;
        member.version = self.next_version()?;
        lattice_core::failpoint::hit(Failpoint::MemberBeforeGuardedCommit);
        let committed = self
            .store
            .update_member(&self.guard, UpdateMember { expected, member })
            .await?;
        self.version = committed.member.version;
        self.known_members.insert(
            committed.member.node.node_id.clone(),
            committed.member.clone(),
        );
        self.publish(MemberChange::Upsert(Box::new(committed.member.clone())));
        Ok(committed.member)
    }

    fn next_version(&self) -> Result<MembershipVersion, CoordinatorRuntimeError> {
        self.version
            .next_revision()
            .map_err(|_| CoordinatorRuntimeError::RevisionExhausted)
    }

    fn publish(&self, change: MemberChange) {
        let _ = self.events.send(MemberEvent {
            version: self.version,
            change,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};

    use super::{MembershipLeader, MembershipLeaderConfig};
    use crate::{
        coordinator::{MemberChange, MemberHello, MemberRemovalReason},
        storage::{CoordinatorLeaseStore, InMemoryPlacementStore, MembershipStore},
        types::{CoordinatorTerm, NodeKey},
    };

    fn node(incarnation: u128) -> NodeKey {
        NodeKey {
            node_id: "logic-a".to_owned(),
            address: NodeAddress::new("127.0.0.1", 29001).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        }
    }

    fn hello(node: NodeKey) -> MemberHello {
        MemberHello {
            node,
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        }
    }

    #[tokio::test]
    async fn expired_member_key_becomes_a_revisioned_removal_event() {
        let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
        let mut leader = MembershipLeader::elect(
            store.clone(),
            NodeKey {
                node_id: "coordinator".to_owned(),
                address: NodeAddress::new("127.0.0.1", 29000).unwrap(),
                incarnation: NodeIncarnation::new(1).unwrap(),
            },
            CoordinatorTerm::new(1).unwrap(),
            MembershipLeaderConfig::default(),
        )
        .await
        .unwrap();
        let mut events = leader.subscribe();
        let member = leader.join(hello(node(2))).await.unwrap();
        leader.mark_up(&member.node).await.unwrap();
        let _ = events.recv().await.unwrap();
        let _ = events.recv().await.unwrap();

        store.revoke_lease(member.lease_id).await.unwrap();
        assert!(store.get_member("logic-a").await.unwrap().is_none());
        leader.reconcile_expired_members().await.unwrap();

        let event = events.recv().await.unwrap();
        assert_eq!(event.version, leader.version());
        assert!(matches!(
            event.change,
            MemberChange::Removed {
                node,
                reason: MemberRemovalReason::FailureDetected,
            } if node == member.node
        ));
        assert_eq!(
            store.get_membership_revision().await.unwrap(),
            event.version.revision
        );

        let replacement = leader.join(hello(node(3))).await.unwrap();
        assert_eq!(replacement.node.incarnation, node(3).incarnation);
        assert!(replacement.version > event.version);
    }

    #[tokio::test]
    async fn immediate_rejoin_commits_expired_predecessor_before_replacement() {
        let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
        let mut leader = MembershipLeader::elect(
            store.clone(),
            NodeKey {
                node_id: "coordinator".to_owned(),
                address: NodeAddress::new("127.0.0.1", 29000).unwrap(),
                incarnation: NodeIncarnation::new(1).unwrap(),
            },
            CoordinatorTerm::new(1).unwrap(),
            MembershipLeaderConfig::default(),
        )
        .await
        .unwrap();
        let mut events = leader.subscribe();
        let predecessor = leader.join(hello(node(2))).await.unwrap();
        leader.mark_up(&predecessor.node).await.unwrap();
        let _ = events.recv().await.unwrap();
        let _ = events.recv().await.unwrap();

        store.revoke_lease(predecessor.lease_id).await.unwrap();
        let replacement = leader.join(hello(node(3))).await.unwrap();

        let removed = events.recv().await.unwrap();
        assert!(matches!(
            removed.change,
            MemberChange::Removed {
                node,
                reason: MemberRemovalReason::FailureDetected,
            } if node == predecessor.node
        ));
        let inserted = events.recv().await.unwrap();
        assert!(matches!(
            inserted.change,
            MemberChange::Upsert(member) if member.node == replacement.node
        ));
        assert!(removed.version < inserted.version);
        assert_eq!(inserted.version, replacement.version);
        assert_eq!(
            store.get_membership_revision().await.unwrap(),
            replacement.version.revision
        );
    }
}
