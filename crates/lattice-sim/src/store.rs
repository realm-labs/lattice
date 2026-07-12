use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimWatchEvent {
    Put {
        revision: u64,
        key: String,
        value: Bytes,
    },
    Delete {
        revision: u64,
        key: String,
    },
    Compacted {
        requested: u64,
        compacted: u64,
    },
}

#[derive(Debug, Clone)]
struct Entry {
    value: Bytes,
    revision: u64,
    lease: Option<i64>,
}

#[derive(Debug, Clone)]
struct Lease {
    deadline_millis: u64,
    keys: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct SimEtcd {
    revision: u64,
    compacted_revision: u64,
    next_lease: i64,
    entries: BTreeMap<String, Entry>,
    leases: BTreeMap<i64, Lease>,
    history: Vec<SimWatchEvent>,
    maximum_entries: usize,
}

impl SimEtcd {
    pub fn new(maximum_entries: usize) -> Result<Self, SimEtcdError> {
        if maximum_entries == 0 {
            return Err(SimEtcdError::ZeroLimit);
        }
        Ok(Self {
            revision: 0,
            compacted_revision: 0,
            next_lease: 1,
            entries: BTreeMap::new(),
            leases: BTreeMap::new(),
            history: Vec::new(),
            maximum_entries,
        })
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn get(&self, key: &str) -> Option<(&Bytes, u64)> {
        self.entries
            .get(key)
            .map(|entry| (&entry.value, entry.revision))
    }

    pub fn compare_and_put(
        &mut self,
        key: String,
        expected_revision: Option<u64>,
        value: Bytes,
        lease: Option<i64>,
    ) -> Result<u64, SimEtcdError> {
        if self.entries.get(&key).map(|entry| entry.revision) != expected_revision {
            return Err(SimEtcdError::CompareFailed);
        }
        if self.entries.len() == self.maximum_entries && !self.entries.contains_key(&key) {
            return Err(SimEtcdError::Capacity);
        }
        if lease.is_some_and(|id| !self.leases.contains_key(&id)) {
            return Err(SimEtcdError::UnknownLease);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(SimEtcdError::Exhausted)?;
        if let Some(previous) = self.entries.get(&key).and_then(|entry| entry.lease)
            && let Some(previous) = self.leases.get_mut(&previous)
        {
            previous.keys.remove(&key);
        }
        self.entries.insert(
            key.clone(),
            Entry {
                value: value.clone(),
                revision: self.revision,
                lease,
            },
        );
        if let Some(lease) = lease {
            self.leases
                .get_mut(&lease)
                .expect("validated simulated lease")
                .keys
                .insert(key.clone());
        }
        self.history.push(SimWatchEvent::Put {
            revision: self.revision,
            key,
            value,
        });
        Ok(self.revision)
    }

    pub fn compare_and_delete(
        &mut self,
        key: &str,
        expected_revision: u64,
    ) -> Result<u64, SimEtcdError> {
        if self.entries.get(key).map(|entry| entry.revision) != Some(expected_revision) {
            return Err(SimEtcdError::CompareFailed);
        }
        let entry = self.entries.remove(key).expect("validated simulated entry");
        if let Some(lease) = entry.lease.and_then(|id| self.leases.get_mut(&id)) {
            lease.keys.remove(key);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(SimEtcdError::Exhausted)?;
        self.history.push(SimWatchEvent::Delete {
            revision: self.revision,
            key: key.to_owned(),
        });
        Ok(self.revision)
    }

    pub fn grant_lease(&mut self, now_millis: u64, ttl_millis: u64) -> Result<i64, SimEtcdError> {
        if ttl_millis == 0 {
            return Err(SimEtcdError::InvalidTtl);
        }
        let id = self.next_lease;
        self.next_lease = self
            .next_lease
            .checked_add(1)
            .ok_or(SimEtcdError::Exhausted)?;
        self.leases.insert(
            id,
            Lease {
                deadline_millis: now_millis.saturating_add(ttl_millis),
                keys: BTreeSet::new(),
            },
        );
        Ok(id)
    }

    pub fn keep_alive(
        &mut self,
        id: i64,
        now_millis: u64,
        ttl_millis: u64,
    ) -> Result<(), SimEtcdError> {
        let lease = self.leases.get_mut(&id).ok_or(SimEtcdError::UnknownLease)?;
        lease.deadline_millis = now_millis.saturating_add(ttl_millis);
        Ok(())
    }

    pub fn expire_leases(&mut self, now_millis: u64) -> Result<Vec<String>, SimEtcdError> {
        let expired = self
            .leases
            .iter()
            .filter_map(|(id, lease)| (lease.deadline_millis <= now_millis).then_some(*id))
            .collect::<Vec<_>>();
        let mut deleted = Vec::new();
        for id in expired {
            let lease = self.leases.remove(&id).expect("located expired lease");
            for key in lease.keys {
                if self.entries.remove(&key).is_some() {
                    self.revision = self
                        .revision
                        .checked_add(1)
                        .ok_or(SimEtcdError::Exhausted)?;
                    self.history.push(SimWatchEvent::Delete {
                        revision: self.revision,
                        key: key.clone(),
                    });
                    deleted.push(key);
                }
            }
        }
        Ok(deleted)
    }

    pub fn compact(&mut self, revision: u64) {
        self.compacted_revision = self.compacted_revision.max(revision.min(self.revision));
        self.history
            .retain(|event| event_revision(event) > self.compacted_revision);
    }

    pub fn watch_from(&self, revision: u64) -> Vec<SimWatchEvent> {
        if revision <= self.compacted_revision {
            return vec![SimWatchEvent::Compacted {
                requested: revision,
                compacted: self.compacted_revision,
            }];
        }
        self.history
            .iter()
            .filter(|event| event_revision(event) >= revision)
            .cloned()
            .collect()
    }
}

fn event_revision(event: &SimWatchEvent) -> u64 {
    match event {
        SimWatchEvent::Put { revision, .. } | SimWatchEvent::Delete { revision, .. } => *revision,
        SimWatchEvent::Compacted { compacted, .. } => *compacted,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SimEtcdError {
    #[error("simulated etcd limit is zero")]
    ZeroLimit,
    #[error("simulated etcd capacity is exhausted")]
    Capacity,
    #[error("simulated etcd comparison failed")]
    CompareFailed,
    #[error("simulated etcd lease does not exist")]
    UnknownLease,
    #[error("simulated etcd lease TTL is invalid")]
    InvalidTtl,
    #[error("simulated etcd counter is exhausted")]
    Exhausted,
}
