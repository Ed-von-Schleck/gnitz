//! The frame control header: a fixed-width, little-endian header followed by the
//! frame's blob. Both `gnitz-server` and `gnitz-core` (client) build and parse it
//! through this one codec, so the two cannot drift.

use std::ops::Range;

use crate::{read_u32_le, read_u64_le, WalError, WireFlags, WireStatus};

// Layout (all little-endian):
//   [0,4)   STATUS     u32   `WireStatus`
//   [4,8)   BLOB_LEN   u32
//   [8,16)  FLAGS      u64   `WireFlags::pack`
//   [16,24) TARGET_ID  u64
//   [24,32) ARG0       u64
//   [32,40) ARG1       u64
//   [40, 40+BLOB_LEN)  blob; under a non-`Ok` STATUS, the UTF-8 error text
const OFF_STATUS: usize = 0;
const OFF_BLOB_LEN: usize = 4;
const OFF_FLAGS: usize = 8;
const OFF_TARGET_ID: usize = 16;
const OFF_ARG0: usize = 24;
const OFF_ARG1: usize = 32;

pub const CTRL_HEADER_SIZE: usize = 40;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlHeader {
    pub status: WireStatus,
    pub target_id: u64,
    pub flags: WireFlags,
    /// Per-verb arguments; each `ClientVerb`, `WireStatus` and `SalMessageKind`
    /// variant documents what it reads here.
    pub arg0: u64,
    pub arg1: u64,
}

pub struct DecodedControl {
    pub hdr: ControlHeader,
    pub blob: Vec<u8>,
    /// Header plus blob.
    pub block_size: usize,
}

pub const fn ctrl_block_size(blob_len: usize) -> usize {
    CTRL_HEADER_SIZE + blob_len
}

#[inline]
pub fn encode_ctrl_block(out: &mut [u8], hdr: &ControlHeader, blob: &[u8]) -> usize {
    let (head, tail) = out
        .split_first_chunk_mut::<CTRL_HEADER_SIZE>()
        .expect("the caller sized `out` with ctrl_block_size");
    // A local array stored whole: const offsets carry no bounds checks, and an
    // empty blob reaches no copy.
    let mut h = [0u8; CTRL_HEADER_SIZE];
    h[OFF_STATUS..OFF_STATUS + 4].copy_from_slice(&hdr.status.as_wire().to_le_bytes());
    h[OFF_BLOB_LEN..OFF_BLOB_LEN + 4].copy_from_slice(&(blob.len() as u32).to_le_bytes());
    h[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&hdr.flags.pack().to_le_bytes());
    h[OFF_TARGET_ID..OFF_TARGET_ID + 8].copy_from_slice(&hdr.target_id.to_le_bytes());
    h[OFF_ARG0..OFF_ARG0 + 8].copy_from_slice(&hdr.arg0.to_le_bytes());
    h[OFF_ARG1..OFF_ARG1 + 8].copy_from_slice(&hdr.arg1.to_le_bytes());
    *head = h;
    if !blob.is_empty() {
        tail[..blob.len()].copy_from_slice(blob);
    }
    CTRL_HEADER_SIZE + blob.len()
}

pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    let (h, rest) = data
        .split_first_chunk::<CTRL_HEADER_SIZE>()
        .ok_or("control header truncated")?;
    let blob_len = read_u32_le(h, OFF_BLOB_LEN) as usize;
    let blob = rest.get(..blob_len).ok_or("control blob runs past the frame")?;
    let status = WireStatus::from_wire(read_u32_le(h, OFF_STATUS)).ok_or("control header names no status")?;
    Ok(DecodedControl {
        hdr: ControlHeader {
            status,
            flags: WireFlags::unpack(read_u64_le(h, OFF_FLAGS))?,
            target_id: read_u64_le(h, OFF_TARGET_ID),
            arg0: read_u64_le(h, OFF_ARG0),
            arg1: read_u64_le(h, OFF_ARG1),
        },
        blob: blob.to_vec(),
        block_size: CTRL_HEADER_SIZE + blob_len,
    })
}

/// Where the blocks after `ctrl` sit in `frame`: the schema block, then the data
/// block, each present exactly when its flag is set.
pub struct FrameBlocks {
    pub schema: Option<Range<usize>>,
    pub data: Option<Range<usize>>,
}

/// Locate the blocks after the control header `ctrl` was peeked from.
pub fn frame_blocks(frame: &[u8], ctrl: &DecodedControl) -> Result<FrameBlocks, WalError> {
    let mut off = ctrl.block_size;
    let mut next = |present: bool| -> Result<Option<Range<usize>>, WalError> {
        if !present {
            return Ok(None);
        }
        let len = crate::wal::block_slice_at(frame, off)?.len();
        let r = off..off + len;
        off += len;
        Ok(Some(r))
    };
    let schema = next(ctrl.hdr.flags.has_schema)?;
    Ok(FrameBlocks {
        schema,
        data: next(ctrl.hdr.flags.has_data)?,
    })
}

#[cfg(test)]
#[path = "tests/control.rs"]
mod tests;
