use std::collections::BTreeMap;
use std::sync::Mutex;

use lattice_placement::coordinator::{MemberChange, MemberEvent, MemberRecord};
use lattice_placement::types::{NodeKey, StateVersion};
use thiserror::Error;
use tokio::sync::broadcast;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberSnapshot {
    pub version: Option<StateVersion>,
    pub members: Vec<MemberRecord>,
}

#[derive(Debug)]
struct MemberDirectoryState {
    version: Option<StateVersion>,
    members: BTreeMap<(String, lattice_core::actor_ref::NodeIncarnation), MemberRecord>,
}

#[derive(Debug)]
pub struct MemberDirectory {
    state: Mutex<MemberDirectoryState>,
    events: broadcast::Sender<MemberEvent>,
}

impl MemberDirectory {
    pub fn new(event_capacity: usize) -> Result<Self, MemberDirectoryError> {
        if event_capacity == 0 {
            return Err(MemberDirectoryError::ZeroCapacity);
        }
        let (events, _) = broadcast::channel(event_capacity);
        Ok(Self {
            state: Mutex::new(MemberDirectoryState {
                version: None,
                members: BTreeMap::new(),
            }),
            events,
        })
    }

    pub fn snapshot(&self) -> MemberSnapshot {
        let state = self.state.lock().expect("member directory poisoned");
        MemberSnapshot {
            version: state.version,
            members: state.members.values().cloned().collect(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MemberEvent> {
        self.events.subscribe()
    }

    pub fn install_snapshot(
        &self,
        version: StateVersion,
        records: Vec<MemberRecord>,
    ) -> Result<(), MemberDirectoryError> {
        let mut members = BTreeMap::new();
        for record in records {
            if record.version > version || record.node != record.hello.node {
                return Err(MemberDirectoryError::InvalidRecord);
            }
            let key = member_key(&record.node);
            if members.insert(key, record).is_some() {
                return Err(MemberDirectoryError::DuplicateMember);
            }
        }
        let mut state = self.state.lock().expect("member directory poisoned");
        if state
            .version
            .is_some_and(|current| version.term < current.term)
        {
            return Err(MemberDirectoryError::StaleRevision);
        }
        state.members = members;
        state.version = Some(version);
        Ok(())
    }

    pub fn apply(&self, event: MemberEvent) -> Result<(), MemberDirectoryError> {
        let mut state = self.state.lock().expect("member directory poisoned");
        if state
            .version
            .is_none_or(|version| !version.accepts_delta_after(event.version))
        {
            return Err(MemberDirectoryError::StaleRevision);
        }
        match &event.change {
            MemberChange::Upsert(record) => {
                if record.version != event.version || record.node != record.hello.node {
                    return Err(MemberDirectoryError::InvalidRecord);
                }
                state
                    .members
                    .insert(member_key(&record.node), *record.clone());
            }
            MemberChange::Removed { node, .. } => {
                state.members.remove(&member_key(node));
            }
        }
        state.version = Some(event.version);
        drop(state);
        let _ = self.events.send(event);
        Ok(())
    }
}

fn member_key(node: &NodeKey) -> (String, lattice_core::actor_ref::NodeIncarnation) {
    (node.node_id.clone(), node.incarnation)
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MemberDirectoryError {
    #[error("member event capacity must be nonzero")]
    ZeroCapacity,
    #[error("member directory revision is stale or duplicated")]
    StaleRevision,
    #[error("member record is inconsistent with its event or snapshot")]
    InvalidRecord,
    #[error("member snapshot contains a duplicate node incarnation")]
    DuplicateMember,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};
    use lattice_placement::coordinator::{
        MemberChange, MemberEvent, MemberRecord, MemberStatus, NodeHello,
    };
    use lattice_placement::types::{CoordinatorTerm, NodeKey, Revision, StateVersion};

    use super::{MemberDirectory, MemberDirectoryError};

    fn member(revision: u64, incarnation: u64) -> MemberRecord {
        let node = NodeKey {
            node_id: "node-a".to_string(),
            address: NodeAddress::new("127.0.0.1", 7447).unwrap(),
            incarnation: NodeIncarnation::new(u128::from(incarnation)).unwrap(),
        };
        MemberRecord {
            node: node.clone(),
            hello: NodeHello {
                node,
                roles: BTreeSet::new(),
                capacity_units: 1,
                hosted_entity_types: BTreeSet::new(),
                proxied_entity_types: BTreeSet::new(),
                singleton_eligibility: BTreeSet::new(),
                used_singletons: BTreeSet::new(),
                protocols: Vec::new(),
                entity_configs: Vec::new(),
                singleton_configs: Vec::new(),
            },
            status: MemberStatus::Up,
            version: StateVersion::new(
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(revision).unwrap(),
            ),
            lease_id: 1,
        }
    }

    #[test]
    fn exact_incarnations_do_not_retarget_each_other() {
        let directory = MemberDirectory::new(4).unwrap();
        let first = member(1, 1);
        directory
            .install_snapshot(first.version, vec![first.clone()])
            .unwrap();
        let replacement = member(2, 2);
        directory
            .apply(MemberEvent {
                version: replacement.version,
                change: MemberChange::Upsert(Box::new(replacement)),
            })
            .unwrap();
        assert_eq!(directory.snapshot().members.len(), 2);
        assert_eq!(
            directory
                .apply(MemberEvent {
                    version: StateVersion::new(
                        CoordinatorTerm::new(1).unwrap(),
                        Revision::new(2).unwrap(),
                    ),
                    change: MemberChange::Removed {
                        node: first.node,
                        reason: lattice_placement::coordinator::MemberRemovalReason::ForceRemoved,
                    },
                })
                .unwrap_err(),
            MemberDirectoryError::StaleRevision
        );
    }
}
