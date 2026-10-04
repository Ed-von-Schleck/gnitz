//! The frame control header: a fixed-width, little-endian header followed by the
//! frame's blob. Both `gnitz-server` and `gnitz-core` (client) build and parse it
//! through this one codec, so the two cannot drift.

use std::ops::Range;

use crate::codec::Reader;
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
    /// Per-verb arguments; each `ClientVerb` and `WireStatus` variant documents
    /// what it reads here.
    pub arg0: u64,
    pub arg1: u64,
}

/// The relation a request names: its id, and the token of the RESOLVE answer
/// the request was built from — `0` for a request built from none, which is what
/// a bare id converts to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Target {
    pub tid: u64,
    pub token: u64,
}

impl From<u64> for Target {
    fn from(tid: u64) -> Self {
        Target { tid, token: 0 }
    }
}

impl ControlHeader {
    /// A `verb` request naming `target`, whose token rides `arg1`.
    pub fn naming(verb: ClientVerb, target: Target, arg0: u64) -> Self {
        ControlHeader {
            flags: WireFlags { verb, ..Default::default() },
            target_id: target.tid,
            arg0,
            arg1: target.token,
            ..Default::default()
        }
    }

    /// The relation a [`Self::naming`] header names.
    pub fn target(&self) -> Target {
        Target { tid: self.target_id, token: self.arg1 }
    }
}

#[derive(Debug)]
pub struct DecodedControl {
    pub hdr: ControlHeader,
    /// Where the frame's blob sits in the peeked buffer.
    pub blob: Range<usize>,
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
        let items = crate::txn_frame::item_shape(verb).is_some();
        if items && (!self.blob.is_empty() || self.schema.is_some() || self.hdr.target_id != 0) {
            return Err("a multi-item frame carries nothing but its items");
        }
        if !items && !self.body.is_empty() {
            return Err("frame carries bytes past its last section");
        }
        Ok(verb)
    }

    /// The error a non-`Ok` frame carries: its status and its blob, read out of
    /// the peeked `frame`, as text.
    pub fn fault(&self, frame: &[u8]) -> Option<WireFault> {
        (self.hdr.status != WireStatus::Ok).then(|| WireFault {
            status: self.hdr.status,
            text: String::from_utf8_lossy(&frame[self.blob.clone()]).into_owned(),
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

/// Bytes of one frame with these sections.
pub(crate) fn frame_size(blob: &[u8], schema: Option<&[u8]>, data: Option<&[&[u8]]>) -> usize {
    frame_head_size(blob.len(), schema.map(<[u8]>::len)) + data.map_or(0, crate::wal::block_size)
}

/// Append one frame — header, blob, optional schema record, optional data block
/// over a canonical region list — to `out`.
pub fn append_frame(
    out: &mut Vec<u8>,
    hdr: &ControlHeader,
    blob: &[u8],
    schema: Option<&[u8]>,
    data: Option<&[&[u8]]>,
) {
    out.reserve(frame_size(blob, schema, data));
    let at = out.len();
    out.resize(at + frame_head_size(blob.len(), schema.map(<[u8]>::len)), 0);
    encode_frame_head(&mut out[at..], hdr, blob, schema, data.is_some());
    if let Some(regions) = data {
        // A client claims no dead heap bytes.
        crate::wal::append_block(regions, 0, out);
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
pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, String> {
    let mut r = Reader::new(data);
    let h: &[u8; CTRL_HEADER_SIZE] = r
        .take(CTRL_HEADER_SIZE)
        .map_err(|_| "control header truncated")?
        .try_into()
        .unwrap();
    let blob_len = read_u32_le(h, OFF_BLOB_LEN) as usize;
    r.take(blob_len).map_err(|_| "control blob runs past the frame")?;
    let blob = CTRL_HEADER_SIZE..r.pos();
    let status = WireStatus::from_wire(read_u32_le(h, OFF_STATUS)).ok_or("control header names no status")?;
    let word = read_u64_le(h, OFF_FLAGS);
    let flags = WireFlags::unpack(word)?;
    let schema = if word & FLAG_HAS_SCHEMA != 0 {
        let s = r.bytes32().map_err(|e| format!("schema record: {e}"))?;
        Some(r.pos() - s.len()..r.pos())
    } else {
        None
    };
    let data_block = if word & FLAG_HAS_DATA != 0 {
        let at = r.pos();
        r.take(crate::wal::block_slice(&data[at..])?.len())?;
        Some(at..r.pos())
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
        blob,
        body: r.pos()..data.len(),
        schema,
        data: data_block,
    })
}

#[cfg(test)]
#[path = "tests/control.rs"]
mod tests;
