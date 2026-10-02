//! Wire protocol: IPC message codec, encode/decode.

use std::rc::Rc;

use gnitz_wire::control::{peek_control_block, DecodedControl};
use gnitz_wire::{WireFlags, WireStatus};
use gnitz_zset::repr::{Batch, WireRows};
use gnitz_zset::schema::{decode_schema_block, encode_schema_block, SchemaDescriptor};

/// The error text for a reply past [`gnitz_wire::MAX_FRAME_PAYLOAD`].
pub(crate) fn oversized_frame_message(sz: usize) -> String {
    format!(
        "reply wire_size={sz} exceeds the maximum frame payload {}; \
         one row wider than the cap cannot be returned at all",
        gnitz_wire::MAX_FRAME_PAYLOAD
    )
}

/// A relation's wire identity: the target id and the encoded block describing
/// its schema.
pub(crate) struct WireSchema {
    /// The relation id, as the catalog keys it. Narrowing to the block's and the
    /// frame's widths happens here and nowhere else.
    tid: u64,
    block: Rc<[u8]>,
}

impl WireSchema {
    /// A one-off **anonymous** block, encoded here from `descriptor` and cached
    /// nowhere: the only option for a schema no catalog entry describes.
    pub(crate) fn encoded(tid: u64, descriptor: &SchemaDescriptor) -> Self {
        WireSchema {
            tid,
            block: Rc::from(encode_schema_block(descriptor)),
        }
    }

    /// `tid` and its catalog entry's *named* block.
    pub(crate) fn from_catalog(cat: &crate::catalog::CatalogEngine, tid: u64) -> Self {
        WireSchema {
            tid,
            block: cat
                .schema_record(tid)
                .expect("a wire target is registered under the catalog lock"),
        }
    }

    pub(crate) fn tid(&self) -> u64 {
        self.tid
    }

    /// The encoded schema record a frame of this relation's rows carries.
    pub(crate) fn block(&self) -> &[u8] {
        &self.block
    }
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
    /// bit clear. Usually from a [`WireSchema`], which pairs them with the
    /// descriptor they encode.
    pub schema_block: Option<&'a [u8]>,
    pub blob: &'a [u8],
}

impl<'a> WireMsg<'a> {
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

/// A SAL slot's control block, and the rows of its data block under the slot's
/// own schema record; `known` answers what that record decodes to when the
/// caller already knows.
pub fn decode_sal_frame(
    bytes: &[u8],
    known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
) -> Result<(DecodedControl, Option<Batch>), String> {
    let control = peek_control_block(bytes)?;
    let Some(r) = control.data.clone() else {
        return Ok((control, None));
    };
    let record = &bytes[control.schema.clone().ok_or("a data block without a schema block")?];
    let schema = match known(control.hdr.target_id, record) {
        Some(s) => s,
        None => decode_schema_block(record)?,
    };
    let batch = Batch::decode_from_wal_block(&bytes[r], &schema)?;
    batch.debug_verify_null_bits();
    Ok((control, Some(batch)))
}

#[cfg(test)]
#[path = "tests/wire.rs"]
mod tests;
