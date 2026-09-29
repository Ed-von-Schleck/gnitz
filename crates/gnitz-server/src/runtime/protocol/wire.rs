//! Wire protocol: IPC message codec, encode/decode.

use std::rc::Rc;

use gnitz_store::schema::{decode_schema_block, encode_schema_block, SchemaDescriptor};
use gnitz_store::storage::{Batch, Layout, WireChunk};
use gnitz_wire::control::{peek_control_block, DecodedControl};
use gnitz_wire::{WireFlags, WireStatus};

/// The error text for a reply past [`gnitz_wire::MAX_FRAME_PAYLOAD`].
pub(crate) fn oversized_frame_message(sz: usize) -> String {
    format!(
        "reply wire_size={sz} exceeds the maximum frame payload {}; \
         one row wider than the cap cannot be returned at all",
        gnitz_wire::MAX_FRAME_PAYLOAD
    )
}

/// A relation's wire identity: the target id, the schema, and the encoded block
/// describing that schema — one value, so the descriptor a slot is routed and
/// sized by and the block it is framed with cannot disagree.
pub(crate) struct WireSchema {
    /// The relation id, as the catalog keys it. Narrowing to the block's and the
    /// frame's widths happens here and nowhere else.
    tid: u64,
    descriptor: SchemaDescriptor,
    block: Rc<[u8]>,
}

impl WireSchema {
    /// A one-off **anonymous** block, encoded here from `descriptor` and cached
    /// nowhere: the only option for a schema no catalog entry describes.
    pub(crate) fn encoded(tid: u64, descriptor: SchemaDescriptor) -> Self {
        WireSchema {
            tid,
            block: Rc::from(encode_schema_block(&descriptor)),
            descriptor,
        }
    }

    /// `tid`'s registry descriptor and its catalog entry's *named* block.
    pub(crate) fn from_catalog(cat: &crate::catalog::CatalogEngine, tid: u64) -> Self {
        let descriptor = cat
            .registry
            .relation(tid)
            .expect("a wire target is registered under the catalog lock")
            .schema();
        WireSchema {
            tid,
            descriptor,
            block: cat
                .schema_record(tid)
                .expect("a wire target is registered under the catalog lock")
                .bytes,
        }
    }

    pub(crate) fn descriptor(&self) -> &SchemaDescriptor {
        &self.descriptor
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

/// The data payload of one wire message: which rows of a batch it carries, and
/// how they are gathered — the only axis on which the encode shapes differ.
#[derive(Clone, Copy, Default)]
pub enum WireData<'a> {
    #[default]
    None,
    Whole(&'a Batch),
    /// Rows `[start, start + rows)` framed directly off `batch`, with no heap —
    /// so no payload cell of those rows may reference one.
    Range {
        batch: &'a Batch,
        start: usize,
        rows: usize,
    },
    /// The rows `indices` selects, in that order, encoded straight into the
    /// destination — no per-worker sub-`Batch` in between. `batch` has no heap,
    /// so no cell of those rows references one.
    Scattered {
        batch: &'a Batch,
        indices: &'a [u32],
    },
}

impl<'a> WireData<'a> {
    /// The payload for one frame of a train over `batch`, from `start`.
    pub(crate) fn of_chunk(batch: &'a Batch, start: usize, chunk: &'a WireChunk) -> Self {
        match chunk {
            WireChunk::Range { rows } => WireData::Range { batch, start, rows: *rows },
            WireChunk::Owned(b) => WireData::Whole(b),
        }
    }

    /// Rows this payload carries; `0` means the slot or frame is dataless.
    pub(crate) fn row_count(&self) -> usize {
        match *self {
            WireData::None => 0,
            WireData::Whole(b) => b.len(),
            WireData::Range { rows, .. } => rows,
            WireData::Scattered { indices, .. } => indices.len(),
        }
    }

    /// The batch this frame's layout claim is read off — for a `Range`, the
    /// source, whose claim a contiguous subrange keeps.
    fn batch(&self) -> Option<&'a Batch> {
        match *self {
            WireData::None => None,
            WireData::Whole(b) | WireData::Range { batch: b, .. } | WireData::Scattered { batch: b, .. } => Some(b),
        }
    }

    /// Bytes [`Self::encode`] writes.
    pub(crate) fn wire_byte_size(&self) -> usize {
        match *self {
            WireData::None => 0,
            WireData::Whole(b) => b.wire_byte_size(),
            WireData::Range { batch, rows, .. } => batch.wire_byte_size_range(rows),
            WireData::Scattered { batch, indices } => batch.wire_byte_size_range(indices.len()),
        }
    }

    /// The rows as one WAL block at the front of `out`; returns bytes written.
    pub(crate) fn encode(&self, out: &mut [u8]) -> usize {
        match *self {
            WireData::None => 0,
            WireData::Whole(b) => b.encode_to_wire(out),
            WireData::Range { batch, start, rows } => batch.encode_range_to_wire(start, rows, out),
            WireData::Scattered { batch, indices } => batch
                .encode_scattered_to_wire(indices, out)
                .expect("a heap-free scatter fits the bytes its size reserved"),
        }
    }
}

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
    pub data: WireData<'a>,
    /// These bytes *are* the frame's schema record, and their length sizes the
    /// prefix that announces it; `None` emits none and leaves the frame's schema
    /// bit clear. Usually from a [`WireSchema`], which pairs them with the
    /// descriptor they encode.
    pub schema_block: Option<&'a [u8]>,
    pub blob: &'a [u8],
}

impl<'a> WireMsg<'a> {
    fn has_data(&self) -> bool {
        self.data.row_count() > 0
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        let mut total = gnitz_wire::control::frame_head_size(self.blob.len(), self.schema_block.map(<[u8]>::len));
        if self.has_data() {
            total += self.data.wire_byte_size();
        }
        total
    }

    /// Encode into `out`, which must be exactly [`size`](WireMsg::size) bytes.
    pub fn encode(&self, out: &mut [u8]) {
        let has_data = self.has_data();

        let wire_flags = WireFlags {
            // Maps `b.layout()` with no re-verify: a non-`Raw` tag was certified
            // (debug-verified) at its producer, so the shipped claim is
            // verified-by-construction.
            batch_consolidated: has_data && self.data.batch().is_some_and(|b| b.layout() == Layout::Consolidated),
            ..self.flags
        };

        let mut pos = gnitz_wire::control::encode_frame_head(
            out,
            &gnitz_wire::control::ControlHeader {
                status: self.status,
                target_id: self.target_id,
                flags: wire_flags,
                arg0: self.arg0,
                arg1: self.arg1,
            },
            self.blob,
            self.schema_block,
            has_data,
        );

        if has_data {
            pos += self.data.encode(&mut out[pos..]);
        }

        assert_eq!(pos, out.len(), "WireMsg::size and encode disagree");
    }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Wire schema of a unique pre-flight reply frame: the leading `n_promoted`
/// columns of `idx_schema`, all PK, so a row's PK region is one indexed-key span.
pub(crate) fn unique_preflight_wire_schema(idx_schema: &SchemaDescriptor, n_promoted: usize) -> SchemaDescriptor {
    let cols = &idx_schema.columns[..n_promoted];
    let pks: Vec<u32> = (0..n_promoted as u32).collect();
    SchemaDescriptor::new(cols, &pks)
}

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

/// Decode a client frame. The batch stays `Raw`: a client's layout claim is not
/// trusted. `recordless` lays out a frame that carries no record; `known` answers
/// what a record decodes to when that is already known (`Ok(None)`: decode it),
/// or refuses the frame.
pub fn decode_client_frame(
    data: &[u8],
    control: DecodedControl,
    recordless: Option<&SchemaDescriptor>,
    known: impl FnOnce(&[u8]) -> Result<Option<SchemaDescriptor>, String>,
) -> Result<DecodedWire, String> {
    decode_frame(data, control, recordless, known, |b, s| {
        Batch::decode_foreign_wal_block(b, s)
    })
}

/// Decode one SAL slot; `known` is asked with the slot's target id.
pub fn decode_sal_slot(
    data: &[u8],
    known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
) -> Result<DecodedWire, String> {
    let control = peek_control_block(data)?;
    let tid = control.hdr.target_id;
    let mut decoded = decode_frame(
        data,
        control,
        None,
        |record: &[u8]| Ok(known(tid, record)),
        Batch::decode_from_wal_block,
    )?;
    certify_engine_frame(&mut decoded);
    Ok(decoded)
}

/// Install an engine-authored frame's layout claim: its `batch_consolidated`
/// bit is real, and skipping the re-fold is the point of sending it, so the batch
/// is raised off `Raw`. `certify_layout`
/// debug-verifies what it installs, which is why the client path
/// (`decode_client_frame`) never comes through here — a lying client frame
/// must be answered with an error, not a debug-build abort.
fn certify_engine_frame(decoded: &mut DecodedWire) {
    let layout = if decoded.control.hdr.flags.batch_consolidated {
        Layout::Consolidated
    } else {
        Layout::Raw
    };
    if let Some(b) = decoded.data_batch.as_mut() {
        b.certify_layout(layout);
    }
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
