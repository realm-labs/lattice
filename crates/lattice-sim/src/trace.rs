use std::{
    io::{Error, Result as IoResult},
    path::Path,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEvent {
    pub index: u64,
    pub causal_parents: Vec<u64>,
    pub time_millis: u64,
    pub node: String,
    pub kind: String,
    pub previous: String,
    pub next: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceJournal {
    pub scenario: String,
    pub seed: u64,
    pub configuration: serde_json::Value,
    pub events: Vec<TraceEvent>,
    maximum_events: usize,
}

impl TraceJournal {
    pub fn new(
        scenario: impl Into<String>,
        seed: u64,
        configuration: serde_json::Value,
        maximum_events: usize,
    ) -> Option<Self> {
        (maximum_events > 0).then_some(Self {
            scenario: scenario.into(),
            seed,
            configuration,
            events: Vec::new(),
            maximum_events,
        })
    }

    pub fn push(&mut self, mut event: TraceEvent) -> bool {
        if self.events.len() == self.maximum_events {
            return false;
        }
        event.index = self.events.len() as u64;
        self.events.push(event);
        true
    }

    pub fn write_json(&self, path: &Path) -> IoResult<()> {
        let encoded = serde_json::to_vec_pretty(self).map_err(Error::other)?;
        std::fs::write(path, encoded)
    }

    pub fn read_json(path: &Path) -> IoResult<Self> {
        let encoded = std::fs::read(path)?;
        serde_json::from_slice(&encoded).map_err(Error::other)
    }

    pub fn shrink<F>(&self, still_fails: F) -> Self
    where
        F: Fn(&[TraceEvent]) -> bool,
    {
        let mut events = self.events.clone();
        let mut chunk = events.len() / 2;
        while chunk > 0 {
            let mut start = 0;
            let mut reduced = false;
            while start + chunk <= events.len() {
                let mut candidate = events.clone();
                candidate.drain(start..start + chunk);
                if still_fails(&candidate) {
                    events = candidate;
                    reduced = true;
                    break;
                }
                start += chunk;
            }
            if !reduced {
                chunk /= 2;
            }
        }
        for (index, event) in events.iter_mut().enumerate() {
            event.index = index as u64;
        }
        let mut minimized = self.clone();
        minimized.events = events;
        minimized
    }
}
