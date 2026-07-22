use std::io::IoSlice;

use crate::wire::Frame;

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
