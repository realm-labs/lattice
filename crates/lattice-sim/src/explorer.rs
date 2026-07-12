use std::collections::{BTreeSet, VecDeque};

pub trait Explorable: Clone + Ord {
    type Event: Clone + Ord;
    type Error;

    fn enabled(&self) -> Vec<Self::Event>;
    fn step(&self, event: &Self::Event) -> Result<Self, Self::Error>;
    fn invariant(&self) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplorationReport {
    pub visited_states: usize,
    pub explored_transitions: usize,
    pub maximum_depth_reached: usize,
}

pub struct StateExplorer {
    pub maximum_states: usize,
    pub maximum_depth: usize,
}

impl StateExplorer {
    pub fn explore<S: Explorable>(&self, initial: S) -> Result<ExplorationReport, String> {
        if self.maximum_states == 0 || self.maximum_depth == 0 {
            return Err("state exploration bounds must be nonzero".to_owned());
        }
        let mut seen = BTreeSet::from([initial.clone()]);
        let mut pending = VecDeque::from([(initial, 0_usize)]);
        let mut transitions = 0;
        let mut maximum_depth_reached = 0;
        while let Some((state, depth)) = pending.pop_front() {
            state.invariant()?;
            maximum_depth_reached = maximum_depth_reached.max(depth);
            if depth == self.maximum_depth {
                continue;
            }
            for event in state.enabled() {
                let next = state
                    .step(&event)
                    .map_err(|_| "state reducer rejected an enabled event".to_owned())?;
                next.invariant()?;
                transitions += 1;
                if seen.insert(next.clone()) {
                    if seen.len() > self.maximum_states {
                        return Err("state exploration capacity exceeded".to_owned());
                    }
                    pending.push_back((next, depth + 1));
                }
            }
        }
        Ok(ExplorationReport {
            visited_states: seen.len(),
            explored_transitions: transitions,
            maximum_depth_reached,
        })
    }
}
