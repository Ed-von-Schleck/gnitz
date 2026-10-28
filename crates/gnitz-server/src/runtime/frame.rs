//! The frame encoder: a [`WireMsg`] sized and written into a w2m slot, a SAL
//! group's payload or a client reply.

use gnitz_wire::{ControlHeader, WireFault, WireFlags, WireStatus};
use gnitz_zset::repr::WireRows;

// ---------------------------------------------------------------------------
// WireMsg
// ---------------------------------------------------------------------------

/// One frame: its control header fields, blob, schema record and data block.
/// Build it once, then [`size`](WireMsg::size) it and encode it: both read the
/// same value, so the byte count a caller reserves and the bytes the encoder
/// writes cannot disagree.
///
/// Every field defaults to zero/absent (`status` defaults to `WireStatus::Ok`), so a
/// caller names only what it sends:
///
/// ```ignore
/// let msg = WireMsg { status: WireStatus::Error, blob: text, ..Default::default() };
/// writer.send_msg(request_id, &msg);
/// ```
#[derive(Clone, Copy, Default)]
pub struct WireMsg<'a> {
    pub target_id: u64,
    pub flags: WireFlags,
    pub arg0: u64,
    pub arg1: u64,
    pub status: WireStatus,
    /// The frame's data block; an empty delta ships none.
    pub data: Option<WireRows<'a>>,
    /// The frame's schema record, written behind its length prefix; `None` writes
    /// none and leaves the schema bit clear.
    pub schema_block: Option<&'a [u8]>,
    pub blob: &'a [u8],
}

impl<'a> WireMsg<'a> {
    /// The control-only frame of `fault`: its status, and its text as the blob.
    pub(crate) fn fault(fault: &'a WireFault) -> Self {
        WireMsg {
            status: fault.status,
            blob: fault.text.as_bytes(),
            ..Default::default()
        }
    }

    /// One frame of a reply train for `target_id`, carrying `data`.
    pub(crate) fn train_frame(target_id: u64, last: bool, data: Option<WireRows<'a>>) -> Self {
        WireMsg {
            target_id,
            flags: WireFlags::train_frame(last),
            data,
            ..Default::default()
        }
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        gnitz_wire::frame_head_size(self.blob.len(), self.schema_block.map(<[u8]>::len))
            + self.data.map_or(0, |d| d.byte_size())
    }

    /// Refuse a frame past [`gnitz_wire::MAX_FRAME_PAYLOAD`], with the fault the
    /// reply is answered with instead.
    pub(crate) fn check_size(&self) -> Result<(), WireFault> {
        let sz = self.size();
        if sz > gnitz_wire::MAX_FRAME_PAYLOAD {
            return Err(format!(
                "a reply of {sz} bytes exceeds the maximum frame payload {}",
                gnitz_wire::MAX_FRAME_PAYLOAD
            )
            .into());
        }
        Ok(())
    }

    /// The control header this frame carries.
    pub(crate) fn header(&self) -> ControlHeader {
        ControlHeader {
            status: self.status,
            target_id: self.target_id,
            flags: self.flags,
            arg0: self.arg0,
            arg1: self.arg1,
        }
    }

    /// Encode into `out`, which must be exactly [`size`](WireMsg::size) bytes.
    pub fn encode(&self, out: &mut [u8]) {
        let mut pos =
            gnitz_wire::encode_frame_head(out, &self.header(), self.blob, self.schema_block, self.data.is_some());

        if let Some(d) = self.data {
            pos += d.encode(&mut out[pos..]);
        }

        assert_eq!(pos, out.len(), "WireMsg::size and encode disagree");
    }

    /// Append `self`'s framed bytes — its length prefix, then its payload — to
    /// `out`, so a corked reply never passes through a buffer of its own.
    pub(crate) fn encode_framed(&self, out: &mut Vec<u8>) {
        const PFX: usize = gnitz_wire::FRAME_LEN_PREFIX_BYTES;
        let sz = self.size();
        let base = out.len();
        let total = base + PFX + sz;
        out.reserve(PFX + sz);
        // SAFETY: `encode` writes every byte of the payload (it asserts so) — a
        // block's header and regions pack end to end — and the length prefix is
        // written immediately below.
        #[allow(clippy::uninit_vec)]
        unsafe {
            out.set_len(total);
        }
        out[base..base + PFX].copy_from_slice(&gnitz_wire::frame_len_prefix(sz));
        self.encode(&mut out[base + PFX..total]);
    }
}
