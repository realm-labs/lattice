use lattice_core::actor_ref::{NodeAddress, NodeIncarnation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProcessState {
    Running,
    Paused,
    Crashed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimProcess {
    pub node_id: String,
    pub address: NodeAddress,
    pub incarnation: NodeIncarnation,
    pub state: ProcessState,
}

impl SimProcess {
    pub fn pause(&mut self) {
        if self.state == ProcessState::Running {
            self.state = ProcessState::Paused;
        }
    }

    pub fn resume(&mut self) {
        if self.state == ProcessState::Paused {
            self.state = ProcessState::Running;
        }
    }

    pub fn crash(&mut self) {
        self.state = ProcessState::Crashed;
    }

    pub fn restart(&mut self, incarnation: NodeIncarnation) {
        self.incarnation = incarnation;
        self.state = ProcessState::Running;
    }
}
