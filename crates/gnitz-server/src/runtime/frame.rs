//! The frame encoder: a [`WireMsg`] sized and written into a w2m slot or a
//! client reply.

use gnitz_wire::{ControlHeader, WireFault};
use gnitz_zset::repr::{Batch, WireRows};

// ---------------------------------------------------------------------------
// WireMsg
// ---------------------------------------------------------------------------

/// One frame: its control header, blob and data block.
/// Build it once, then [`size`](WireMsg::size) it and encode it: both read the
/// same value, so the byte count a caller reserves and the bytes the encoder
/// writes cannot disagree.
///
/// Every field defaults to zero/absent (the header's `status` to
/// `WireStatus::Ok`), so a caller names only what it sends:
///
/// ```ignore
/// let msg = WireMsg { hdr: ControlHeader::ack(target_id, lsn), ..Default::default() };
/// writer.try_send_msg(request_id, false, &msg);
/// ```
#[derive(Clone, Copy, Default)]
pub struct WireMsg<'a> {
    pub hdr: ControlHeader,
    /// The frame's data block; an empty delta ships none.
    pub data: Option<WireRows<'a>>,
    pub blob: &'a [u8],
}

/// The longest text a fault frame carries. A refusal's text may echo what its
/// request named, and a request may be a whole frame of that.
const MAX_FAULT_TEXT: usize = 16 << 10;

impl<'a> WireMsg<'a> {
    /// The control-only frame of `fault` refusing the request naming
    /// `target_id`: its status, and as the blob its text up to the last
    /// character boundary within [`MAX_FAULT_TEXT`] bytes.
    pub(crate) fn fault(target_id: u64, fault: &'a WireFault) -> Self {
        let text = &fault.text;
        WireMsg {
            hdr: ControlHeader::fault(target_id, fault.status),
            blob: &text.as_bytes()[..text.floor_char_boundary(MAX_FAULT_TEXT)],
            ..Default::default()
        }
    }

    /// One frame of a reply train for `target_id`, carrying `data`.
    pub(crate) fn train_frame(target_id: u64, last: bool, data: Option<WireRows<'a>>) -> Self {
        WireMsg {
            hdr: ControlHeader::train(target_id, last),
            data,
            ..Default::default()
        }
    }

    /// The next frame of `target_id`'s train over `batch`, its rows starting at
    /// row `from` and the frame within `budget` bytes, and the row the one after
    /// it starts at, which the caller commits once the frame is sent. The frame
    /// reaching the end of `batch` is the train's last, flagged `scan_last`, iff
    /// `ends`: no row follows `batch`. `Err` is a row too wide for any frame.
    pub(crate) fn next_train_frame(
        target_id: u64,
        batch: &'a Batch,
        from: usize,
        budget: usize,
        ends: bool,
    ) -> Result<(Self, usize), WireFault> {
        let head = Self::train_frame(target_id, false, None).size();
        let rows = batch.wire_rows_within(from, budget.saturating_sub(head));
        let next = from + rows.map_or(0, |r| r.rows());
        let msg = Self::train_frame(target_id, ends && next == batch.len(), rows);
        msg.check_size()?;
        Ok((msg, next))
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        gnitz_wire::CTRL_HEADER_SIZE + self.blob.len() + self.data.map_or(0, |d| d.byte_size())
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

    /// Encode into `out`, which must be exactly [`size`](WireMsg::size) bytes.
    pub fn encode(&self, out: &mut [u8]) {
        let mut pos = gnitz_wire::encode_frame_head(out, &self.hdr, self.blob, self.data.is_some());

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
        // SAFETY: `encode` writes every payload byte. The control header's fields
        // tile it, the blob follows it, and a data block is its header, then its
        // regions and heap end to end, each row of which its writer fills whole
        // (gnitz-zset's `DirectWriter`). The length prefix is written immediately
        // below.
        #[allow(clippy::uninit_vec)]
        unsafe {
            out.set_len(total);
        }
        out[base..base + PFX].copy_from_slice(&gnitz_wire::frame_len_prefix(sz));
        self.encode(&mut out[base + PFX..total]);
    }
}

#[cfg(test)]
#[path = "tests/frame.rs"]
mod tests;
