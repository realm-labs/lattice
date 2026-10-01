use std::time::Instant;

use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    io::split,
    sync::{mpsc, watch},
    time::{Instant as TokioInstant, MissedTickBehavior, interval, sleep},
};

use super::{
    BidirectionalLane, LaneError, LaneExit,
    ask::dispatch_inbound_ask,
    control::worker::ControlWorker,
    inbound::{InboundAction, InboundLane},
    outbound::LaneWriter,
};
use crate::{
    association::LaneKind,
    messaging::{target_cache::ExactTargetCache, target_dictionary::ExactTargetDictionary},
    transport::{FramedReader, FramedWriter, RemotingIo},
    wire::{Frame, FrameCodec, FrameKind},
};

pub(super) async fn run_bidirectional_lane_inner<S>(
    runtime: &BidirectionalLane,
    receiver: &mut mpsc::Receiver<Frame>,
    stream: S,
    shutdown: &mut watch::Receiver<bool>,
    target_cache: &mut ExactTargetCache,
    target_dictionary: &mut ExactTargetDictionary,
) -> Result<LaneExit, LaneError>
where
    S: RemotingIo,
{
    let association = runtime.association.as_ref();
    let lane = runtime.lane;
    let messaging = runtime.services.messaging.as_ref();
    let dispatch = runtime.services.dispatch.clone();
    let config = runtime.config.validate()?;
    if *shutdown.borrow() {
        return Ok(LaneExit::Shutdown);
    }
    let codec = FrameCodec::new(config.maximum_frame_size)?;
    let (read, write) = split(stream);
    let mut reader =
        FramedReader::new_with_read_ahead(read, codec.clone(), config.socket_read_ahead_bytes);
    let writer = FramedWriter::new_with_tuning(
        write,
        codec,
        config.maximum_ready_write_batch_frames,
        config.maximum_coalesced_write_batch_bytes,
    );
    let mut writer = LaneWriter::new(
        writer,
        runtime.association.clone(),
        lane,
        runtime.services.messaging.clone(),
        config,
    );
    let mut inbound = InboundLane::new(
        association,
        lane,
        &runtime.services,
        config,
        target_cache,
        target_dictionary,
    );
    let mut asks = FuturesUnordered::new();
    let mut heartbeat = interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_received = Instant::now();
    let mut control_worker = (lane == LaneKind::Control).then(|| {
        ControlWorker::spawn(
            runtime.association.clone(),
            runtime.services.control_dispatch.clone(),
            config.maximum_pending_control_applies,
            config.control_apply_retry_timeout,
        )
    });
    let idle = sleep(config.idle_data_connection_timeout);
    tokio::pin!(idle);

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    if lane == LaneKind::Control {
                        let _ = writer.write_frame(&Frame::new(FrameKind::Close, Bytes::new())).await;
                    }
                    writer.flush().await?;
                    return Ok(LaneExit::Shutdown);
                }
            }
            completed = async {
                control_worker.as_mut()
                    .expect("control result branch requires a worker")
                    .recv().await
            }, if control_worker.is_some() => {
                let Some(completed) = completed else {
                    return Err(LaneError::ControlWorkerClosed);
                };
                if let Some(frame) = completed? {
                    writer.write_frame(&frame).await?;
                }
            }
            _ = heartbeat.tick(), if lane == LaneKind::Control => {
                if Instant::now().duration_since(last_received)
                    >= config.heartbeat_interval * config.heartbeat_miss_limit
                {
                    return Err(LaneError::HeartbeatTimeout);
                }
                writer.write_frame(&Frame::new(FrameKind::Heartbeat, Bytes::new())).await?;
            }
            completed = asks.next(), if !asks.is_empty() => {
                let Some(completed) = completed else {
                    continue;
                };
                writer.write_replies(completed?, &mut asks).await?;
                idle.as_mut().reset(TokioInstant::now() + config.idle_data_connection_timeout);
            }
            outbound = receiver.recv() => {
                let Some(frame) = outbound else {
                    return Ok(LaneExit::QueueClosed);
                };
                if !writer.write_queued(frame, receiver).await? {
                    continue;
                }
                idle.as_mut().reset(TokioInstant::now() + config.idle_data_connection_timeout);
            }
            received = reader.read_frame() => {
                let mut next_frame = Some(received?);
                association.record_peer_activity();
                let mut processed_frames = 0;
                while let Some(frame) = next_frame {
                    last_received = Instant::now();
                    idle.as_mut().reset(TokioInstant::now() + config.idle_data_connection_timeout);
                    let pending_asks = asks.len();
                    let action = inbound.dispatch(frame, pending_asks).await?;
                    match action {
                        InboundAction::Continue => {}
                        InboundAction::EnqueueAsk(work) => {
                            asks.push(dispatch_inbound_ask(dispatch.clone(), work));
                        }
                        InboundAction::Write(frame) => writer.write_frame(&frame).await?,
                        InboundAction::ApplyControl(frame) => control_worker.as_ref()
                            .expect("control lane requires an apply worker")
                            .submit(frame)?,
                        InboundAction::Close => return Ok(LaneExit::RemoteClose),
                    }
                    inbound.report_cache_metrics();
                    processed_frames += 1;
                    next_frame = if processed_frames < config.maximum_ready_read_batch_frames {
                        reader.try_read_frame()?
                    } else {
                        None
                    };
                }
            }
            () = &mut idle, if lane != LaneKind::Control => {
                if lane == LaneKind::Interactive
                    && (!asks.is_empty() || messaging.has_pending_for_association(association.id()))
                {
                    idle.as_mut().reset(TokioInstant::now() + config.idle_data_connection_timeout);
                    continue;
                }
                writer.flush().await?;
                return Ok(LaneExit::Idle);
            }
        }
    }
}
