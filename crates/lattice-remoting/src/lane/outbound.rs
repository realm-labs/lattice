use std::{future::Future, sync::Arc, time::Duration};

use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use lattice_failpoint::hit as hit_failpoint;
use tokio::{io::AsyncWrite, sync::mpsc, time::timeout};

use super::{BidirectionalLaneConfig, LaneError};
use crate::{
    association::{Association, LaneKind},
    failpoints,
    messaging::{
        error::RemoteMessageError,
        outbound::{OutboundMessaging, PreparedOutboundFrame},
        target::CorrelationId,
    },
    transport::FramedWriter,
    wire::{Frame, FrameKind, WireError},
};

/// Reuses batch storage and keeps byte reservations alive until writes finish or are cancelled.
pub(super) struct LaneWriter<W> {
    writer: FramedWriter<W>,
    association: Arc<Association>,
    messaging: Arc<OutboundMessaging>,
    lane: LaneKind,
    write_timeout: Option<Duration>,
    maximum_batch_frames: usize,
    candidates: Vec<Frame>,
    batch: Vec<Frame>,
    correlations: Vec<Option<CorrelationId>>,
}

impl<W: AsyncWrite + Send + Unpin> LaneWriter<W> {
    pub(super) fn new(
        writer: FramedWriter<W>,
        association: Arc<Association>,
        lane: LaneKind,
        messaging: Arc<OutboundMessaging>,
        config: BidirectionalLaneConfig,
    ) -> Self {
        let maximum_batch_frames = config.maximum_ready_write_batch_frames;
        Self {
            writer,
            association,
            messaging,
            lane,
            write_timeout: (lane == LaneKind::Control)
                .then(|| config.heartbeat_interval * config.heartbeat_miss_limit),
            maximum_batch_frames,
            candidates: Vec::with_capacity(maximum_batch_frames),
            batch: Vec::with_capacity(maximum_batch_frames),
            correlations: Vec::with_capacity(maximum_batch_frames),
        }
    }

    pub(super) async fn write_frame(&mut self, frame: &Frame) -> Result<(), LaneError> {
        write_within(self.write_timeout, self.writer.write_frame(frame)).await?;
        Ok(())
    }

    pub(super) async fn flush(&mut self) -> Result<(), LaneError> {
        write_within(self.write_timeout, self.writer.flush()).await
    }

    pub(super) async fn write_replies<F>(
        &mut self,
        first: Frame,
        asks: &mut FuturesUnordered<F>,
    ) -> Result<(), LaneError>
    where
        F: Future<Output = Result<Frame, RemoteMessageError>>,
    {
        self.batch.clear();
        self.batch.push(first);
        while self.batch.len() < self.maximum_batch_frames {
            let Some(completed) = asks.next().now_or_never().flatten() else {
                break;
            };
            self.batch.push(completed?);
        }
        if self.batch.len() == 1 {
            write_within(self.write_timeout, self.writer.write_frame(&self.batch[0])).await?;
        } else {
            write_within(
                self.write_timeout,
                self.writer.write_frames_with_commit(&self.batch, |_| {}),
            )
            .await?;
        }
        Ok(())
    }

    /// Returns false when every dequeued frame was cancelled before socket admission.
    pub(super) async fn write_queued(
        &mut self,
        first: Frame,
        receiver: &mut mpsc::Receiver<Frame>,
    ) -> Result<bool, LaneError> {
        self.collect_batch(first, receiver);
        self.prepare_batch();
        if self.batch.is_empty() {
            return Ok(false);
        }
        if self
            .batch
            .iter()
            .any(|frame| frame.kind == FrameKind::ControlEnvelope)
        {
            hit_failpoint(failpoints::CONTROL_AFTER_OUTBOX_BEFORE_SOCKET_WRITE);
        }
        let frame_count = self.batch.len();
        let result = if frame_count == 1 && !matches!(self.lane, LaneKind::Bulk(_)) {
            let correlation = self.correlations[0];
            write_within(
                self.write_timeout,
                self.writer
                    .write_frame_with_commit_outcome(&self.batch[0], || {
                        if let Some(correlation) = correlation {
                            self.messaging.mark_socket_write_started(correlation);
                        }
                    }),
            )
            .await
        } else {
            write_within(
                self.write_timeout,
                self.writer.write_frames_with_commit(&self.batch, |index| {
                    if let Some(correlation) = self.correlations[index] {
                        self.messaging.mark_socket_write_started(correlation);
                    }
                }),
            )
            .await
        };
        // Frames own their byte reservations through writes and cancellation.
        // Drop completed batches now so parked producers can resume while idle.
        self.batch.clear();
        let outcome = result?;
        self.association
            .record_outbound_write(frame_count, outcome.socket_writes);
        Ok(true)
    }

    fn collect_batch(&mut self, first: Frame, receiver: &mut mpsc::Receiver<Frame>) {
        self.candidates.clear();
        self.candidates.push(first);
        let limit = if self.lane == LaneKind::Control {
            1
        } else {
            self.maximum_batch_frames
        };
        while self.candidates.len() < limit {
            let Ok(frame) = receiver.try_recv() else {
                break;
            };
            self.candidates.push(frame);
        }
    }

    fn prepare_batch(&mut self) {
        self.batch.clear();
        self.correlations.clear();
        for mut frame in self.candidates.drain(..) {
            if let LaneKind::Bulk(index) = self.lane {
                frame.expand_stale_compact_target(
                    self.association.bulk_lane_epoch(usize::from(index)),
                );
            }
            let Some(prepared) = self.messaging.prepare_outbound_for_socket_write(&mut frame)
            else {
                continue;
            };
            self.correlations.push(match prepared {
                PreparedOutboundFrame::Other => None,
                PreparedOutboundFrame::Ask(correlation) => Some(correlation),
            });
            self.batch.push(frame);
        }
    }
}

async fn write_within<T, F>(limit: Option<Duration>, write: F) -> Result<T, LaneError>
where
    F: Future<Output = Result<T, WireError>>,
{
    match limit {
        Some(limit) => timeout(limit, write)
            .await
            .map_err(|_| LaneError::WriteTimeout)?
            .map_err(LaneError::from),
        None => write.await.map_err(LaneError::from),
    }
}
