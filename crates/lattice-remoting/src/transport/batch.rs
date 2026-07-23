use std::io::{Error, ErrorKind, IoSlice};

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::{BatchWriteOutcome, HARD_MAX_WRITE_BATCH_FRAMES};
use crate::wire::{Frame, FrameCodec, WireError};

pub(super) fn should_coalesce_write_batch(frames: &[Frame], maximum_bytes: usize) -> bool {
    frames.len() > 1
        && frames.iter().any(|frame| frame.payload_segment_count() > 1)
        && frames
            .iter()
            .try_fold(0_usize, |total, frame| {
                total.checked_add(crate::wire::WIRE_HEADER_LEN + frame.payload_len())
            })
            .is_some_and(|total| total <= maximum_bytes)
}

pub(super) fn invalid_write_batch_size(actual: usize, maximum: usize) -> WireError {
    WireError::Io(Error::new(
        ErrorKind::InvalidInput,
        format!("write batch contains {actual} frames, maximum is {maximum}"),
    ))
}

pub(super) async fn write_coalesced_frames<W, F>(
    writer: &mut W,
    codec: &FrameCodec,
    scratch: &mut BytesMut,
    frames: &[Frame],
    mut on_first_frame_write: F,
) -> Result<BatchWriteOutcome, WireError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(usize),
{
    if frames.len() > HARD_MAX_WRITE_BATCH_FRAMES {
        return Err(invalid_write_batch_size(
            frames.len(),
            HARD_MAX_WRITE_BATCH_FRAMES,
        ));
    }
    scratch.clear();
    let required = frames
        .iter()
        .map(|frame| crate::wire::WIRE_HEADER_LEN + frame.payload_len())
        .sum();
    scratch.reserve(required);
    let mut frame_offsets = [0_usize; HARD_MAX_WRITE_BATCH_FRAMES];
    for (index, frame) in frames.iter().enumerate() {
        frame_offsets[index] = scratch.len();
        scratch.extend_from_slice(&codec.header(frame)?);
        for segment in 0..frame.payload_segment_count() {
            scratch.extend_from_slice(frame.payload_segment(segment));
        }
    }
    debug_assert_eq!(scratch.len(), required);

    let mut written = 0;
    let mut next_commit = 0;
    let mut socket_writes = 0;
    while written < scratch.len() {
        let count = writer.write(&scratch[written..]).await?;
        socket_writes += 1;
        if count == 0 {
            return Err(WireError::Io(Error::new(
                ErrorKind::WriteZero,
                "remoting socket wrote zero bytes",
            )));
        }
        let next_written = written + count;
        while next_commit < frames.len() && frame_offsets[next_commit] < next_written {
            on_first_frame_write(next_commit);
            next_commit += 1;
        }
        written = next_written;
    }
    Ok(BatchWriteOutcome {
        bytes: required,
        socket_writes,
    })
}

pub(super) fn append_frame_buffers<'a>(
    buffers: &mut [IoSlice<'a>],
    frame: &'a Frame,
    header: &'a [u8; crate::wire::WIRE_HEADER_LEN],
    first_part: usize,
    first_part_written: usize,
) -> usize {
    let part_count = 1 + frame.payload_segment_count();
    let mut count = 0;
    for part_index in first_part..part_count {
        let part = frame_part(frame, header, part_index);
        let written = if part_index == first_part {
            first_part_written
        } else {
            0
        };
        if written < part.len() {
            buffers[count] = IoSlice::new(&part[written..]);
            count += 1;
        }
    }
    count
}

pub(super) fn advance_frame_parts(
    frame: &Frame,
    header: &[u8; crate::wire::WIRE_HEADER_LEN],
    part_index: &mut usize,
    part_written: &mut usize,
    maximum: usize,
) -> usize {
    let part_count = 1 + frame.payload_segment_count();
    let mut remaining = maximum;
    while remaining > 0 && *part_index < part_count {
        let part = frame_part(frame, header, *part_index);
        let part_remaining = part.len() - *part_written;
        let consumed = remaining.min(part_remaining);
        *part_written += consumed;
        remaining -= consumed;
        if *part_written == part.len() {
            *part_index += 1;
            *part_written = 0;
            skip_empty_frame_parts(frame, header, part_index, part_written);
        }
    }
    maximum - remaining
}

pub(super) fn skip_empty_frame_parts(
    frame: &Frame,
    header: &[u8; crate::wire::WIRE_HEADER_LEN],
    part_index: &mut usize,
    part_written: &mut usize,
) {
    let part_count = 1 + frame.payload_segment_count();
    while *part_index < part_count && frame_part(frame, header, *part_index).is_empty() {
        *part_index += 1;
        *part_written = 0;
    }
}

fn frame_part<'a>(
    frame: &'a Frame,
    header: &'a [u8; crate::wire::WIRE_HEADER_LEN],
    part_index: usize,
) -> &'a [u8] {
    if part_index == 0 {
        header
    } else {
        frame.payload_segment(part_index - 1)
    }
}
