use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use lattice_core::actor_ref::NodeIncarnation;
use lattice_placement::{
    coordinator::{MemberChange, MemberEvent, MemberRecord, MemberStatus},
    types::{MembershipVersion, NodeKey},
};
use thiserror::Error;
use tokio::sync::broadcast;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberSnapshot {
    pub version: Option<MembershipVersion>,
    pub members: Vec<MemberRecord>,
}

#[derive(Debug)]
struct MemberDirectoryState {
    version: Option<MembershipVersion>,
    members: BTreeMap<(String, NodeIncarnation), MemberRecord>,
    fenced_incarnations: BTreeSet<NodeIncarnation>,
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
                fenced_incarnations: BTreeSet::new(),
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

    pub(crate) fn snapshot_and_subscribe(
        &self,
    ) -> (MemberSnapshot, broadcast::Receiver<MemberEvent>) {
        let state = self.state.lock().expect("member directory poisoned");
        let events = self.events.subscribe();
        let snapshot = MemberSnapshot {
            version: state.version,
            members: state.members.values().cloned().collect(),
        };
        (snapshot, events)
    }

    pub(crate) fn lookup_up(&self, node: &NodeKey) -> Option<MemberRecord> {
        self.state
            .lock()
            .expect("member directory poisoned")
            .members
            .get(&member_key(node))
            .filter(|record| record.node == *node && record.status == MemberStatus::Up)
            .cloned()
    }

    pub fn install_snapshot(
        &self,
        version: MembershipVersion,
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
        members.retain(|(_, incarnation), _| !state.fenced_incarnations.contains(incarnation));
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
                if !state.fenced_incarnations.contains(&record.node.incarnation) {
                    state
                        .members
                        .insert(member_key(&record.node), *record.clone());
                }
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

    /// Permanently excludes an incarnation from this local directory.
    ///
    /// A graceful leave has already committed the authoritative removal before this is called.
    /// The local membership runtime can still have an older snapshot in flight, so the fence and
    /// removal must be atomic with respect to snapshot installation.
    pub(crate) fn fence_incarnation(&self, incarnation: NodeIncarnation) {
        let mut state = self.state.lock().expect("member directory poisoned");
        state.fenced_incarnations.insert(incarnation);
        state
            .members
            .retain(|(_, member_incarnation), _| *member_incarnation != incarnation);
    }
}

fn member_key(node: &NodeKey) -> (String, NodeIncarnation) {
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
    use std::collections::{BTreeMap, BTreeSet};

    use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};
    use lattice_placement::{
        coordinator::{
            MemberChange, MemberEvent, MemberHello, MemberRecord, MemberRemovalReason, MemberStatus,
        },
        types::{CoordinatorTerm, MembershipVersion, NodeKey, Revision},
    };

    use super::{MemberDirectory, MemberDirectoryError};

    fn member(revision: u64, incarnation: u64) -> MemberRecord {
        let node = NodeKey {
            node_id: "node-a".to_string(),
            address: NodeAddress::new("127.0.0.1", 7447).unwrap(),
            incarnation: NodeIncarnation::new(u128::from(incarnation)).unwrap(),
        };
        MemberRecord {
            node: node.clone(),
            hello: MemberHello {
                node,
                roles: BTreeSet::new(),
                failure_domains: BTreeMap::new(),
                protocols: Vec::new(),
                remoting_capabilities: BTreeSet::new(),
            },
            status: MemberStatus::Up,
            version: MembershipVersion::new(
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
                    version: MembershipVersion::new(
                        CoordinatorTerm::new(1).unwrap(),
                        Revision::new(2).unwrap(),
                    ),
                    change: MemberChange::Removed {
                        node: first.node,
                        reason: MemberRemovalReason::ForceRemoved,
                    },
                })
                .unwrap_err(),
            MemberDirectoryError::StaleRevision
        );
    }

    #[test]
    fn fenced_incarnation_cannot_be_reintroduced_by_a_late_snapshot() {
        let directory = MemberDirectory::new(4).unwrap();
        let local = member(1, 1);
        directory
            .install_snapshot(local.version, vec![local.clone()])
            .unwrap();

        directory.fence_incarnation(local.node.incarnation);
        directory
            .install_snapshot(local.version, vec![local.clone()])
            .unwrap();

        assert!(directory.snapshot().members.is_empty());
    }
}
