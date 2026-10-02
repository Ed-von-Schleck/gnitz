//! Wire protocol: IPC message codec, encode/decode.

use gnitz_wire::control::DecodedControl;
use gnitz_wire::{WireFault, WireFlags, WireStatus};
use gnitz_zset::repr::{Batch, WireRows};
use gnitz_zset::schema::SchemaDescriptor;

/// The error text for a reply past [`gnitz_wire::MAX_FRAME_PAYLOAD`].
pub(crate) fn oversized_frame_message(sz: usize) -> String {
    format!(
        "reply wire_size={sz} exceeds the maximum frame payload {}; \
         one row wider than the cap cannot be returned at all",
        gnitz_wire::MAX_FRAME_PAYLOAD
    )
}

// ---------------------------------------------------------------------------
// WireMsg
// ---------------------------------------------------------------------------

/// One IPC/WAL wire message. Build it once, then [`size`](WireMsg::size) it and
/// encode it: both read the same value, so the byte count a caller reserves and
/// the bytes the encoder writes cannot disagree.
///
/// Every field defaults to zero/absent (`status` defaults to `WireStatus::Ok`), so a
/// caller names only what it sends:
///
/// ```ignore
/// let msg = ipc::WireMsg { status: WireStatus::Error, blob: text, ..Default::default() };
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
    /// These bytes *are* the frame's schema record, and their length sizes the
    /// prefix that announces it; `None` emits none and leaves the frame's schema
    /// bit clear.
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

    /// One frame of a reply train for `target_id`, but for its rows.
    pub(crate) fn train_frame(target_id: u64, last: bool) -> Self {
        WireMsg {
            target_id,
            flags: WireFlags::train_frame(last),
            ..Default::default()
        }
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        gnitz_wire::control::frame_head_size(self.blob.len(), self.schema_block.map(<[u8]>::len))
            + self.data.map_or(0, |d| d.byte_size())
    }

    /// Encode into `out`, which must be exactly [`size`](WireMsg::size) bytes.
    pub fn encode(&self, out: &mut [u8]) {
        let mut pos = gnitz_wire::control::encode_frame_head(
            out,
            &gnitz_wire::control::ControlHeader {
                status: self.status,
                target_id: self.target_id,
                flags: self.flags,
                arg0: self.arg0,
                arg1: self.arg1,
            },
            self.blob,
            self.schema_block,
            self.data.is_some(),
        );

        if let Some(d) = self.data {
            pos += d.encode(&mut out[pos..]);
        }

        assert_eq!(pos, out.len(), "WireMsg::size and encode disagree");
    }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// The rows of a client frame's data block under `schema`; `None` for a frame
/// without one.
pub fn decode_client_rows(
    data: &[u8],
    control: &DecodedControl,
    schema: &SchemaDescriptor,
) -> Result<Option<Batch>, &'static str> {
    control
        .data
        .clone()
        .map(|r| Batch::decode_foreign_wal_block(&data[r], schema))
        .transpose()
}

#[cfg(test)]
#[path = "tests/wire.rs"]
mod tests;
