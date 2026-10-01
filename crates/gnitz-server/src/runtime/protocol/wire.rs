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

    /// `rest` addressed to this relation: its target id and schema block, with
    /// the caller's own header fields kept.
    pub(crate) fn frame<'a>(&'a self, rest: WireMsg<'a>) -> WireMsg<'a> {
        WireMsg {
            target_id: self.tid,
            schema_block: Some(&self.block),
            ..rest
        }
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

/// Full decoded wire message.
pub struct DecodedWire {
    pub control: DecodedControl,
    /// The frame's blob, copied out of the buffer `control` indexes: a decoded
    /// request can outlive that buffer.
    pub blob: Vec<u8>,
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<Batch>,
}

/// A `known` for a frame whose record is always decoded.
pub fn unknown(_: &[u8]) -> Result<Option<SchemaDescriptor>, String> {
    Ok(None)
}

/// Decode a client frame. `recordless` lays out a frame that carries no record; `known` answers
/// what a record decodes to when that is already known (`Ok(None)`: decode it),
/// or refuses the frame.
///
/// A data block with no rows is refused: an empty delta ships no block (as
/// [`WireMsg`] encodes one), so a present block always carries a row, and no
/// handler opens a zone or bumps a commit LSN for nothing.
pub fn decode_client_frame(
    data: &[u8],
    control: DecodedControl,
    recordless: Option<&SchemaDescriptor>,
    known: impl FnOnce(&[u8]) -> Result<Option<SchemaDescriptor>, String>,
) -> Result<DecodedWire, String> {
    decode_frame(data, control, recordless, known, |b, s| {
        let batch = Batch::decode_foreign_wal_block(b, s)?;
        if batch.is_empty() {
            return Err("a data block with no rows");
        }
        Ok(batch)
    })
}

/// Decode one SAL slot; `known` is asked with the slot's target id.
pub fn decode_sal_slot(
    data: &[u8],
    known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
) -> Result<DecodedWire, String> {
    let control = peek_control_block(data)?;
    let tid = control.hdr.target_id;
    let decoded = decode_frame(
        data,
        control,
        None,
        |record: &[u8]| Ok(known(tid, record)),
        Batch::decode_from_wal_block,
    )?;
    if let Some(b) = &decoded.data_batch {
        b.debug_verify_null_bits();
    }
    Ok(decoded)
}

/// A frame decoded into its `DecodedWire`, every field built in the slot it
/// stays in rather than assembled from a returned tuple.
fn decode_frame(
    data: &[u8],
    control: DecodedControl,
    recordless: Option<&SchemaDescriptor>,
    known: impl FnOnce(&[u8]) -> Result<Option<SchemaDescriptor>, String>,
    decode: impl FnOnce(&[u8], &SchemaDescriptor) -> Result<Batch, &'static str>,
) -> Result<DecodedWire, String> {
    let schema = match &control.schema {
        Some(r) => {
            let record = &data[r.clone()];
            Some(match known(record)? {
                Some(s) => s,
                None => decode_schema_block(record)?,
            })
        }
        None => recordless.copied(),
    };
    let mut out = DecodedWire {
        blob: data[control.blob.clone()].to_vec(),
        control,
        schema,
        data_batch: None,
    };
    let Some(r) = out.control.data.clone() else {
        return Ok(out);
    };

    let (schema, batch) = (&out.schema, &mut out.data_batch);
    let schema = schema.as_ref().ok_or("a data block without a schema block")?;
    *batch = Some(decode(&data[r], schema).map_err(str::to_string)?);
    Ok(out)
}

#[cfg(test)]
#[path = "tests/wire.rs"]
mod tests;
