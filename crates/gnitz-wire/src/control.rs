//! The frame control header: a fixed-width, little-endian header followed by the
//! frame's blob. Both `gnitz-server` and `gnitz-core` (client) build and parse it
//! through this one codec, so the two cannot drift.

use std::ops::Range;

use crate::flags::{FLAG_HAS_DATA, FLAG_HAS_SCHEMA};
use crate::{read_u32_le, read_u64_le, write_u32_le, write_u64_le, ClientVerb, WireFault, WireFlags, WireStatus};

// Layout (all little-endian):
//   [0,4)   STATUS     u32   `WireStatus`
//   [4,8)   BLOB_LEN   u32
//   [8,16)  FLAGS      u64   `WireFlags::pack`
//   [16,24) TARGET_ID  u64
//   [24,32) ARG0       u64
//   [32,40) ARG1       u64
//   [40, 40+BLOB_LEN)  blob; under a non-`Ok` STATUS, the UTF-8 error text
//   then a `u32`-length-prefixed schema record (FLAGS bit 32) and a self-sizing
//   data block (bit 33), each present iff its bit is set
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
    /// The bytes after the last declared section — blob, schema record, data
    /// block — to the end of the frame: a multi-item frame's items.
    pub body: Range<usize>,
    /// Where the frame's schema block sits in the peeked buffer.
    pub schema: Option<Range<usize>>,
    /// Where the frame's data block sits in the peeked buffer.
    pub data: Option<Range<usize>>,
}

impl DecodedControl {
    /// The verb a client frame names, refusing a section that verb does not
    /// carry.
    pub fn client_verb(&self) -> Result<ClientVerb, &'static str> {
        let verb = self.hdr.flags.verb;
        if self.data.is_some() && verb != ClientVerb::Push {
            return Err("frame carries a data block on a verb other than PUSH");
        }
        let items = matches!(
            verb,
            ClientVerb::DdlTxn | ClientVerb::PushTxn | ClientVerb::ScanMulti | ClientVerb::DeltaPoll
        );
        if items && (!self.blob.is_empty() || self.schema.is_some() || self.hdr.target_id != 0) {
            return Err("a multi-item frame carries nothing but its items");
        }
        if !items && !self.body.is_empty() {
            return Err("frame carries bytes past its last section");
        }
        Ok(verb)
    }

    /// The error a non-`Ok` frame carries: its status and its blob as text.
    pub fn fault(&self) -> Option<WireFault> {
        (self.hdr.status != WireStatus::Ok).then(|| WireFault {
            status: self.hdr.status,
            text: String::from_utf8_lossy(&self.blob).into_owned(),
        })
    }
}

/// Bytes of the control header, its blob and an optional length-prefixed schema
/// record.
pub const fn frame_head_size(blob_len: usize, schema_len: Option<usize>) -> usize {
    CTRL_HEADER_SIZE
        + blob_len
        + match schema_len {
            Some(n) => 4 + n,
            None => 0,
        }
}

/// Write the control header, `blob` and the length-prefixed `schema_block` into
/// the front of `out`, returning the offset a data block follows at.
#[inline]
pub fn encode_frame_head(
    out: &mut [u8],
    hdr: &ControlHeader,
    blob: &[u8],
    schema_block: Option<&[u8]>,
    has_data: bool,
) -> usize {
    let (head, tail) = out
        .split_first_chunk_mut::<CTRL_HEADER_SIZE>()
        .expect("the caller sized `out` with frame_head_size");
    debug_assert!(
        hdr.status == WireStatus::Ok || !blob.is_empty(),
        "a fault frame names its cause"
    );
    let flags = hdr.flags.pack()
        | if schema_block.is_some() { FLAG_HAS_SCHEMA } else { 0 }
        | if has_data { FLAG_HAS_DATA } else { 0 };
    write_u32_le(head, OFF_STATUS, hdr.status.as_wire());
    write_u32_le(head, OFF_BLOB_LEN, blob.len() as u32);
    write_u64_le(head, OFF_FLAGS, flags);
    write_u64_le(head, OFF_TARGET_ID, hdr.target_id);
    write_u64_le(head, OFF_ARG0, hdr.arg0);
    write_u64_le(head, OFF_ARG1, hdr.arg1);
    tail[..blob.len()].copy_from_slice(blob);
    let mut pos = CTRL_HEADER_SIZE + blob.len();
    if let Some(sb) = schema_block {
        write_u32_le(out, pos, sb.len() as u32);
        pos += 4;
        out[pos..pos + sb.len()].copy_from_slice(sb);
        pos += sb.len();
    }
    pos
}

/// Decode the control header at the front of the whole frame `data`, and locate
/// the blocks behind it.
pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    let (h, rest) = data
        .split_first_chunk::<CTRL_HEADER_SIZE>()
        .ok_or("control header truncated")?;
    let blob_len = read_u32_le(h, OFF_BLOB_LEN) as usize;
    let blob = rest.get(..blob_len).ok_or("control blob runs past the frame")?;
    let status = WireStatus::from_wire(read_u32_le(h, OFF_STATUS)).ok_or("control header names no status")?;
    let word = read_u64_le(h, OFF_FLAGS);
    let flags = WireFlags::unpack(word)?;
    let mut off = CTRL_HEADER_SIZE + blob_len;
    let schema = if word & FLAG_HAS_SCHEMA != 0 {
        let r = crate::codec::bytes32_extent(data, off).ok_or("schema record runs past the frame")?;
        off = r.end;
        Some(r)
    } else {
        None
    };
    let data_block = if word & FLAG_HAS_DATA != 0 {
        let start = off;
        off += crate::wal::block_slice_at(data, off)?.len();
        Some(start..off)
    } else {
        None
    };
    Ok(DecodedControl {
        hdr: ControlHeader {
            status,
            flags,
            target_id: read_u64_le(h, OFF_TARGET_ID),
            arg0: read_u64_le(h, OFF_ARG0),
            arg1: read_u64_le(h, OFF_ARG1),
        },
        blob: blob.to_vec(),
        body: off..data.len(),
        schema,
        data: data_block,
    })
}

#[cfg(test)]
#[path = "tests/control.rs"]
mod tests;
