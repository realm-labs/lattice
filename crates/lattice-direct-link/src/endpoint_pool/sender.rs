use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use lattice_core::direct_link::errors::LinkSendError;
use lattice_core::direct_link::ids::{LinkId, LinkSequence};
use lattice_core::direct_link::options::{LinkCloseReason, LinkDirection};
use lattice_core::direct_link::runtime::{DirectLinkSender, OutboundDirectLinkMessage};

use crate::endpoint_pool::connection::ConnectionCommand;
use crate::endpoint_pool::{
    DirectLinkConnectionId, DirectLinkEndpointKey, PooledDirectLinkEndpointPoolInner,
};
use crate::protocol::DirectLinkFrame;
use crate::transport::DirectLinkTransport;

pub(crate) struct PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    pub(crate) inner: Arc<PooledDirectLinkEndpointPoolInner<T>>,
    pub(crate) endpoint_key: DirectLinkEndpointKey,
    pub(crate) connection_id: DirectLinkConnectionId,
    pub(crate) link_id: LinkId,
    pub(crate) direction: LinkDirection,
    pub(crate) next_sequence: StdMutex<u64>,
    pub(crate) closed: AtomicBool,
}

impl<T> fmt::Debug for PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledDirectLinkSender")
            .field("endpoint_key", &self.endpoint_key)
            .field("connection_id", &self.connection_id)
            .field("link_id", &self.link_id)
            .field("direction", &self.direction)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> DirectLinkSender for PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkSendError::Closed {
                reason: LinkCloseReason::Done,
            });
        }
        let frame = self.next_message_frame(message)?;
        self.inner
            .write_frame(self.connection_id, frame)
            .await
            .map_err(|error| LinkSendError::Protocol(error.to_string()))
    }

    fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkSendError::Closed {
                reason: LinkCloseReason::Done,
            });
        }
        let writer = self
            .inner
            .state
            .try_lock()
            .ok()
            .and_then(|state| state.find_writer(self.connection_id))
            .ok_or_else(|| {
                self.inner.metrics.record_pool_queue_backpressure();
                LinkSendError::BackpressureFull
            })?;
        let mut next_sequence = self
            .next_sequence
            .lock()
            .expect("direct link pooled sender sequence poisoned");
        let frame = self.message_to_frame(message, LinkSequence(*next_sequence))?;
        match writer.try_send(ConnectionCommand::Write {
            frame,
            completion: None,
        }) {
            Ok(()) => {
                *next_sequence += 1;
                Ok(())
            }
            Err(_) => {
                self.inner.metrics.record_pool_queue_backpressure();
                Err(LinkSendError::BackpressureFull)
            }
        }
    }

    async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let frame =
            DirectLinkFrame::close_direction(self.link_id.clone(), self.direction, _reason.clone());
        let _ = self.inner.write_frame(self.connection_id, frame).await;
        self.inner
            .close_logical_direction(&self.link_id, self.direction, _reason)
            .await;
        Ok(())
    }
}

impl<T> PooledDirectLinkSender<T>
where
    T: DirectLinkTransport,
{
    fn next_message_frame(
        &self,
        message: OutboundDirectLinkMessage,
    ) -> Result<DirectLinkFrame, LinkSendError> {
        let mut next_sequence = self
            .next_sequence
            .lock()
            .expect("direct link pooled sender sequence poisoned");
        let frame = self.message_to_frame(message, LinkSequence(*next_sequence))?;
        *next_sequence += 1;
        Ok(frame)
    }

    fn message_to_frame(
        &self,
        message: OutboundDirectLinkMessage,
        sequence: LinkSequence,
    ) -> Result<DirectLinkFrame, LinkSendError> {
        if message.direction != self.direction {
            return Err(LinkSendError::Protocol(
                "direct link sender used with the wrong direction".to_string(),
            ));
        }
        if message.link_id != self.link_id {
            return Err(LinkSendError::Protocol(format!(
                "direct link sender for {} cannot send frame for {}",
                self.link_id, message.link_id
            )));
        }
        Ok(DirectLinkFrame::directed_message_with_header(
            message.link_id,
            message.direction,
            sequence,
            message.message_id,
            message.metadata,
            message.payload,
        ))
    }
}
