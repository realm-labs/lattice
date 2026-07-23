use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Notify, futures::Notified};

use super::{Association, AssociationError, BulkAdmission, LaneKind};

#[derive(Debug)]
pub(super) struct OutboundByteBudget {
    used: AtomicUsize,
    available: Notify,
}

impl OutboundByteBudget {
    pub(super) fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
            available: Notify::new(),
        }
    }

    fn try_reserve(&self, bytes: usize, maximum: usize) -> bool {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let next = current.checked_add(bytes)?;
                (next <= maximum).then_some(next)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        let _ = self
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
        self.available.notify_waiters();
    }

    fn notified(&self) -> Notified<'_> {
        self.available.notified()
    }
}

impl Association {
    pub(crate) fn try_reserve_prepared_bulk(
        &self,
        stripe: usize,
        bytes: usize,
    ) -> Result<BulkAdmission<'_>, AssociationError> {
        if stripe >= self.bulk.len() {
            return Err(AssociationError::InvalidBulkStripe(
                u8::try_from(stripe).unwrap_or(u8::MAX),
            ));
        }
        self.prepare_data_lane(LaneKind::Bulk(stripe as u8))?;
        let permit = self.bulk[stripe].try_reserve().map_err(|_| {
            self.metrics.record_queue_rejection();
            AssociationError::QueueFull
        })?;
        self.reserve_bytes(bytes)?;
        Ok(BulkAdmission {
            association: self,
            permit: Some(permit),
            reserved_bytes: bytes,
        })
    }

    pub(crate) async fn reserve_prepared_bulk(
        &self,
        stripe: usize,
        bytes: usize,
    ) -> Result<BulkAdmission<'_>, AssociationError> {
        match self.try_reserve_prepared_bulk(stripe, bytes) {
            Ok(admission) => return Ok(admission),
            Err(
                AssociationError::QueueFull
                | AssociationError::ByteBudgetExceeded
                | AssociationError::NodeByteBudgetExceeded,
            ) => {}
            Err(error) => return Err(error),
        }
        if stripe >= self.bulk.len() {
            return Err(AssociationError::InvalidBulkStripe(
                u8::try_from(stripe).unwrap_or(u8::MAX),
            ));
        }
        if bytes > self.config.max_outbound_bytes_per_association {
            self.metrics.record_association_byte_budget_rejection();
            return Err(AssociationError::ByteBudgetExceeded);
        }
        if bytes > self.config.max_outbound_bytes_per_node {
            self.metrics.record_node_byte_budget_rejection();
            return Err(AssociationError::NodeByteBudgetExceeded);
        }
        self.prepare_data_lane(LaneKind::Bulk(stripe as u8))?;
        let sender = &self.bulk[stripe];
        let permit = loop {
            let state_changed = self.admission_changed.notified();
            tokio::pin!(state_changed);
            state_changed.as_mut().enable();
            self.ensure_active()?;
            let permit = tokio::select! {
                permit = sender.reserve() => {
                    permit.map_err(|_| AssociationError::Closed)?
                }
                () = state_changed.as_mut() => {
                    self.ensure_active()?;
                    continue;
                }
            };
            self.ensure_active()?;
            break permit;
        };
        self.reserve_bytes_when_available(bytes).await?;
        Ok(BulkAdmission {
            association: self,
            permit: Some(permit),
            reserved_bytes: bytes,
        })
    }

    pub fn release_queued_bytes(&self, bytes: usize) {
        self.queued_bytes.release(bytes);
        self.node_queued_bytes.release(bytes);
    }

    pub(super) fn reserve_bytes(&self, bytes: usize) -> Result<(), AssociationError> {
        self.try_reserve_bytes(bytes)
            .inspect_err(|error| match error {
                AssociationError::ByteBudgetExceeded => {
                    self.metrics.record_association_byte_budget_rejection();
                }
                AssociationError::NodeByteBudgetExceeded => {
                    self.metrics.record_node_byte_budget_rejection();
                }
                _ => {}
            })
    }

    fn try_reserve_bytes(&self, bytes: usize) -> Result<(), AssociationError> {
        if !self
            .queued_bytes
            .try_reserve(bytes, self.config.max_outbound_bytes_per_association)
        {
            return Err(AssociationError::ByteBudgetExceeded);
        }
        if !self
            .node_queued_bytes
            .try_reserve(bytes, self.config.max_outbound_bytes_per_node)
        {
            self.queued_bytes.release(bytes);
            return Err(AssociationError::NodeByteBudgetExceeded);
        }
        Ok(())
    }

    async fn reserve_bytes_when_available(&self, bytes: usize) -> Result<(), AssociationError> {
        loop {
            let association_available = self.queued_bytes.notified();
            let node_available = self.node_queued_bytes.notified();
            let state_changed = self.admission_changed.notified();
            tokio::pin!(association_available, node_available, state_changed);
            association_available.as_mut().enable();
            node_available.as_mut().enable();
            state_changed.as_mut().enable();
            self.ensure_active()?;
            match self.try_reserve_bytes(bytes) {
                Ok(()) => return Ok(()),
                Err(AssociationError::ByteBudgetExceeded) => {
                    tokio::select! {
                        () = association_available.as_mut() => {}
                        () = state_changed.as_mut() => self.ensure_active()?,
                    }
                }
                Err(AssociationError::NodeByteBudgetExceeded) => {
                    tokio::select! {
                        () = node_available.as_mut() => {}
                        () = state_changed.as_mut() => self.ensure_active()?,
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};

    use super::*;
    use crate::{
        association::{AssociationKey, LaneAttachment},
        config::RemotingConfig,
        wire::{Frame, FrameKind},
    };

    fn key() -> AssociationKey {
        AssociationKey {
            cluster_id: ClusterId::new("test").unwrap(),
            local_incarnation: NodeIncarnation::new(1).unwrap(),
            remote_address: NodeAddress::new("remote", 25520).unwrap(),
            remote_incarnation: NodeIncarnation::new(2).unwrap(),
        }
    }

    fn active_association(association_bytes: usize, node_bytes: usize) -> Arc<Association> {
        let association = Arc::new(
            Association::new(
                key(),
                RemotingConfig {
                    max_outbound_bytes_per_association: association_bytes,
                    max_outbound_bytes_per_node: node_bytes,
                    ..RemotingConfig::default()
                },
            )
            .unwrap(),
        );
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        association
    }

    async fn wait_for_budget_rejection(association: &Association) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while association.metrics().association_byte_budget_rejections == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn async_bulk_reservation_parks_until_byte_budget_is_released() {
        let association = active_association(8, 8);
        association
            .try_admit_interactive(Frame::new(
                FrameKind::Backpressure,
                Bytes::from_static(b"12345678"),
            ))
            .unwrap();
        let mut bulk = association.take_lane_receiver(LaneKind::Bulk(0)).unwrap();
        let waiting_association = association.clone();
        let waiting = tokio::spawn(async move {
            let admission = waiting_association
                .reserve_prepared_bulk(0, 8)
                .await
                .unwrap();
            admission.send(Frame::new(FrameKind::Tell, Bytes::from_static(b"abcdefgh")));
        });

        wait_for_budget_rejection(&association).await;
        assert!(!waiting.is_finished());
        association.release_queued_bytes(8);

        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bulk.recv().await.unwrap().payload_len(), 8);
        assert_eq!(association.metrics().association_byte_budget_rejections, 1);
    }

    #[tokio::test]
    async fn async_bulk_reservation_rejects_an_impossible_frame_without_waiting() {
        let association = active_association(8, 16);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            association.reserve_prepared_bulk(0, 9),
        )
        .await
        .unwrap();

        assert!(matches!(result, Err(AssociationError::ByteBudgetExceeded)));
    }

    #[tokio::test]
    async fn closing_association_wakes_a_byte_budget_waiter() {
        let association = active_association(8, 8);
        association
            .try_admit_interactive(Frame::new(
                FrameKind::Backpressure,
                Bytes::from_static(b"12345678"),
            ))
            .unwrap();
        let waiting_association = association.clone();
        let waiting = tokio::spawn(async move {
            waiting_association
                .reserve_prepared_bulk(0, 8)
                .await
                .map(drop)
        });
        wait_for_budget_rejection(&association).await;

        association.begin_close();

        let result = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(AssociationError::NotActive)));
    }
}
