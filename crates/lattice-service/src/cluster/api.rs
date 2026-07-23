use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use lattice_core::actor_ref::ClusterId;
use lattice_placement::{
    coordinator::{MemberEvent, MemberRecord},
    types::{MembershipVersion, NodeKey},
};
use thiserror::Error;
use tokio::sync::{broadcast, watch};

use super::members::{MemberDirectory, MemberSnapshot};
use crate::lifecycle::{
    CoordinatorScopeState, NodeLifecycleState, PlacementDomainState, ServiceHealthSnapshot,
};

/// A user-facing handle to the local node's view of its cluster.
///
/// Cloning this handle is cheap. The state is a locally observed view and is updated by the
/// membership and lifecycle runtimes.
#[derive(Clone)]
pub struct Cluster {
    cluster_id: ClusterId,
    local_node: NodeKey,
    health: Arc<Mutex<ServiceHealthSnapshot>>,
    health_events: watch::Sender<ServiceHealthSnapshot>,
    members: Arc<MemberDirectory>,
}

impl Cluster {
    pub(crate) fn new(
        cluster_id: ClusterId,
        local_node: NodeKey,
        health: Arc<Mutex<ServiceHealthSnapshot>>,
        health_events: watch::Sender<ServiceHealthSnapshot>,
        members: Arc<MemberDirectory>,
    ) -> Self {
        Self {
            cluster_id,
            local_node,
            health,
            health_events,
            members,
        }
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn local_node(&self) -> &NodeKey {
        &self.local_node
    }

    /// Returns the latest locally observed cluster state.
    pub fn state(&self) -> ClusterState {
        let health = self.health.lock().expect("service health poisoned").clone();
        let members = self.members.snapshot();
        ClusterState::new(
            self.cluster_id.clone(),
            self.local_node.clone(),
            health,
            members,
        )
    }

    /// Subscribes to cluster changes.
    ///
    /// The first event is always [`ClusterEvent::CurrentState`]. Later events describe health or
    /// membership changes. If the consumer falls behind membership events, the stream emits a new
    /// `CurrentState` snapshot so the consumer can reconcile without reconstructing missed deltas.
    pub fn subscribe(&self) -> ClusterEvents {
        let health = self.health_events.subscribe();
        let (members, member_events) = self.members.snapshot_and_subscribe();
        let current = ClusterState::new(
            self.cluster_id.clone(),
            self.local_node.clone(),
            self.health.lock().expect("service health poisoned").clone(),
            members,
        );
        ClusterEvents {
            cluster: self.clone(),
            health,
            members: member_events,
            initial: Some(current),
            health_open: true,
            members_open: true,
        }
    }

    /// Waits until a locally observed cluster state satisfies `predicate`.
    pub async fn wait_for<F>(
        &self,
        timeout: Duration,
        predicate: F,
    ) -> Result<ClusterState, ClusterWaitError>
    where
        F: Fn(&ClusterState) -> bool,
    {
        tokio::time::timeout(timeout, async {
            let mut events = self.subscribe();
            loop {
                let state = self.state();
                if predicate(&state) {
                    return Ok(state);
                }
                if events.recv().await.is_none() {
                    return Err(ClusterWaitError::Closed);
                }
            }
        })
        .await
        .map_err(|_| ClusterWaitError::Timeout)?
    }

    /// Waits until the node and all of its configured placement domains are ready.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<ClusterState, ClusterWaitError> {
        tokio::time::timeout(timeout, async {
            let mut events = self.subscribe();
            loop {
                let state = self.state();
                if state.is_ready() {
                    return Ok(state);
                }
                if matches!(
                    state.health.node,
                    NodeLifecycleState::Stopping | NodeLifecycleState::Terminated
                ) {
                    return Err(ClusterWaitError::Terminated);
                }
                if events.recv().await.is_none() {
                    return Err(ClusterWaitError::Closed);
                }
            }
        })
        .await
        .map_err(|_| ClusterWaitError::Timeout)?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub cluster_id: ClusterId,
    pub local_node: NodeKey,
    pub health: ServiceHealthSnapshot,
    pub membership_version: Option<MembershipVersion>,
    pub members: Vec<MemberRecord>,
}

impl ClusterState {
    fn new(
        cluster_id: ClusterId,
        local_node: NodeKey,
        health: ServiceHealthSnapshot,
        members: MemberSnapshot,
    ) -> Self {
        Self {
            cluster_id,
            local_node,
            health,
            membership_version: members.version,
            members: members.members,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.health.node == NodeLifecycleState::Ready
            && self
                .health
                .domains
                .values()
                .all(|state| *state == PlacementDomainState::Ready)
            && self
                .health
                .coordinator_scopes
                .values()
                .all(|state| *state != CoordinatorScopeState::Failed)
    }

    pub fn self_member(&self) -> Option<&MemberRecord> {
        self.members
            .iter()
            .find(|member| member.node == self.local_node)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterEvent {
    CurrentState(ClusterState),
    HealthChanged(ServiceHealthSnapshot),
    MemberChanged(MemberEvent),
}

/// A cluster event subscription returned by [`Cluster::subscribe`].
pub struct ClusterEvents {
    cluster: Cluster,
    health: watch::Receiver<ServiceHealthSnapshot>,
    members: broadcast::Receiver<MemberEvent>,
    initial: Option<ClusterState>,
    health_open: bool,
    members_open: bool,
}

impl ClusterEvents {
    pub async fn recv(&mut self) -> Option<ClusterEvent> {
        if let Some(initial) = self.initial.take() {
            return Some(ClusterEvent::CurrentState(initial));
        }
        loop {
            if !self.health_open && !self.members_open {
                return None;
            }
            tokio::select! {
                changed = self.health.changed(), if self.health_open => {
                    match changed {
                        Ok(()) => {
                            return Some(ClusterEvent::HealthChanged(
                                self.health.borrow_and_update().clone(),
                            ));
                        }
                        Err(_) => self.health_open = false,
                    }
                }
                event = self.members.recv(), if self.members_open => {
                    match event {
                        Ok(event) => return Some(ClusterEvent::MemberChanged(event)),
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            return Some(ClusterEvent::CurrentState(self.cluster.state()));
                        }
                        Err(broadcast::error::RecvError::Closed) => self.members_open = false,
                    }
                }
            }
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ClusterWaitError {
    #[error("cluster state wait exceeded its deadline")]
    Timeout,
    #[error("cluster event stream closed")]
    Closed,
    #[error("node terminated before becoming ready")]
    Terminated,
}
