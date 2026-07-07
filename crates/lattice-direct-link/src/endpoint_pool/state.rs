use std::collections::{BTreeMap, HashMap};

use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::ids::LinkId;
use lattice_core::direct_link::messages::{LinkClosed, LinkDirectionClosed};
use lattice_core::direct_link::options::{LinkCloseReason, LinkDirection};
use lattice_core::direct_link::runtime::DirectLinkOpenRequest;
use tokio::sync::mpsc;

use crate::endpoint_pool::connection::ConnectionCommand;
use crate::endpoint_pool::{
    DirectLinkConnectionId, DirectLinkConnectionStripe, DirectLinkEndpointKey,
};
use crate::session::OpenLinkAck;

#[derive(Debug, Default)]
pub(crate) struct PoolState {
    pub(crate) endpoints: HashMap<DirectLinkEndpointKey, EndpointState>,
    pub(crate) links: BTreeMap<LinkId, PooledLinkState>,
    pub(crate) closed_links: BTreeMap<LinkId, LinkCloseReason>,
}

impl PoolState {
    pub(crate) fn find_writer(
        &self,
        connection_id: DirectLinkConnectionId,
    ) -> Option<mpsc::Sender<ConnectionCommand>> {
        self.endpoints
            .values()
            .flat_map(|endpoint| endpoint.stripes.iter())
            .filter_map(Option::as_ref)
            .find(|stripe| stripe.stripe.connection_id == connection_id)
            .map(|stripe| stripe.writer.clone())
    }

    pub(crate) fn find_stripe_mut(
        &mut self,
        endpoint_key: &DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
    ) -> Option<&mut PooledStripeState> {
        self.endpoints
            .get_mut(endpoint_key)?
            .stripes
            .iter_mut()
            .filter_map(Option::as_mut)
            .find(|stripe| stripe.stripe.connection_id == connection_id)
    }
}

#[derive(Debug)]
pub(crate) struct EndpointState {
    pub(crate) stripes: Vec<Option<PooledStripeState>>,
}

impl EndpointState {
    pub(crate) fn new(stripe_count: usize) -> Self {
        Self {
            stripes: vec![None; stripe_count],
        }
    }

    pub(crate) fn active_links(&self) -> usize {
        self.stripes
            .iter()
            .filter_map(Option::as_ref)
            .map(|stripe| stripe.active_links)
            .sum()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PooledStripeState {
    pub(crate) stripe: DirectLinkConnectionStripe,
    pub(crate) writer: mpsc::Sender<ConnectionCommand>,
    pub(crate) active_links: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct PooledLinkState {
    pub(crate) endpoint_key: DirectLinkEndpointKey,
    pub(crate) connection_id: DirectLinkConnectionId,
    pub(crate) source: ActorRef,
    pub(crate) directions: BTreeMap<LinkDirection, PooledDirectionState>,
}

#[derive(Debug, Clone)]
pub(crate) struct PooledDirectionState {
    pub(crate) stream_name: String,
    pub(crate) closed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PooledLinkClosure {
    pub(crate) source: ActorRef,
    pub(crate) direction_closed: Vec<LinkDirectionClosed>,
    pub(crate) link_closed: Option<LinkClosed>,
}

impl PooledLinkState {
    pub(crate) fn from_open_ack(
        endpoint_key: DirectLinkEndpointKey,
        connection_id: DirectLinkConnectionId,
        request: &DirectLinkOpenRequest,
        ack: &OpenLinkAck,
    ) -> Self {
        let mut directions = BTreeMap::from([(
            LinkDirection::SourceToTarget,
            PooledDirectionState {
                stream_name: ack.source_to_target.stream_name.clone(),
                closed: false,
            },
        )]);
        if let Some(direction) = &ack.target_to_source {
            directions.insert(
                LinkDirection::TargetToSource,
                PooledDirectionState {
                    stream_name: direction.stream_name.clone(),
                    closed: false,
                },
            );
        }
        Self {
            endpoint_key,
            connection_id,
            source: request.source.clone(),
            directions,
        }
    }

    pub(crate) fn close_direction_event(
        &mut self,
        link_id: &LinkId,
        direction: LinkDirection,
        reason: LinkCloseReason,
    ) -> Option<PooledLinkClosure> {
        let direction_state = self.directions.get_mut(&direction)?;
        if direction_state.closed {
            return None;
        }
        direction_state.closed = true;
        let direction_closed = source_observes_direction(direction).then(|| LinkDirectionClosed {
            link_id: link_id.clone(),
            direction,
            stream: direction_state.stream_name.clone(),
            reason: reason.clone(),
            last_sequence_seen: None,
        });
        let link_closed = self
            .is_fully_closed()
            .then(|| self.link_closed(link_id, reason));
        Some(PooledLinkClosure {
            source: self.source.clone(),
            direction_closed: direction_closed.into_iter().collect(),
            link_closed,
        })
    }

    pub(crate) fn close_all_event(
        &self,
        link_id: &LinkId,
        reason: LinkCloseReason,
    ) -> PooledLinkClosure {
        let direction_closed = self
            .directions
            .iter()
            .filter(|(_, state)| !state.closed)
            .filter(|(direction, _)| source_observes_direction(**direction))
            .map(|(direction, state)| LinkDirectionClosed {
                link_id: link_id.clone(),
                direction: *direction,
                stream: state.stream_name.clone(),
                reason: reason.clone(),
                last_sequence_seen: None,
            })
            .collect();
        PooledLinkClosure {
            source: self.source.clone(),
            direction_closed,
            link_closed: Some(self.link_closed(link_id, reason)),
        }
    }

    pub(crate) fn link_closed(&self, link_id: &LinkId, reason: LinkCloseReason) -> LinkClosed {
        LinkClosed {
            link_id: link_id.clone(),
            reason,
            closed_directions: self.directions.keys().copied().collect(),
            last_sequence_seen: None,
        }
    }

    pub(crate) fn is_fully_closed(&self) -> bool {
        self.directions.values().all(|direction| direction.closed)
    }
}

fn source_observes_direction(direction: LinkDirection) -> bool {
    direction == LinkDirection::TargetToSource
}
