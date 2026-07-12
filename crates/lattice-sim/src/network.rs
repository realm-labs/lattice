use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NetworkFrame {
    pub id: u64,
    pub source: String,
    pub target: String,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct SimNetwork {
    maximum_frames: usize,
    next_id: u64,
    queued: VecDeque<NetworkFrame>,
    partitions: BTreeSet<(String, String)>,
    dropped: BTreeMap<u64, NetworkFrame>,
}

impl SimNetwork {
    pub fn new(maximum_frames: usize) -> Option<Self> {
        (maximum_frames > 0).then_some(Self {
            maximum_frames,
            next_id: 1,
            queued: VecDeque::new(),
            partitions: BTreeSet::new(),
            dropped: BTreeMap::new(),
        })
    }

    pub fn send(&mut self, source: &str, target: &str, payload: Bytes) -> Option<u64> {
        if self.queued.len() == self.maximum_frames {
            return None;
        }
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.queued.push_back(NetworkFrame {
            id,
            source: source.to_owned(),
            target: target.to_owned(),
            payload,
        });
        Some(id)
    }

    pub fn deliver(&mut self, id: u64) -> Option<NetworkFrame> {
        let index = self.queued.iter().position(|frame| frame.id == id)?;
        let frame = self.queued.remove(index)?;
        if self
            .partitions
            .contains(&(frame.source.clone(), frame.target.clone()))
        {
            self.dropped.insert(id, frame);
            None
        } else {
            Some(frame)
        }
    }

    pub fn drop_frame(&mut self, id: u64) -> bool {
        let Some(index) = self.queued.iter().position(|frame| frame.id == id) else {
            return false;
        };
        let frame = self.queued.remove(index).expect("located network frame");
        self.dropped.insert(id, frame);
        true
    }

    pub fn duplicate(&mut self, id: u64) -> Option<u64> {
        let frame = self.queued.iter().find(|frame| frame.id == id)?.clone();
        self.send(&frame.source, &frame.target, frame.payload)
    }

    pub fn partition(&mut self, source: &str, target: &str) {
        self.partitions
            .insert((source.to_owned(), target.to_owned()));
    }

    pub fn heal(&mut self, source: &str, target: &str) {
        self.partitions
            .remove(&(source.to_owned(), target.to_owned()));
    }

    pub fn queued(&self) -> impl Iterator<Item = &NetworkFrame> {
        self.queued.iter()
    }
}
