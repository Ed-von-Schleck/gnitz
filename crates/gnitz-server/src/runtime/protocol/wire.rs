//! Wire protocol: IPC message codec, encode/decode.

use std::rc::Rc;

use gnitz_store::schema::{decode_schema_block, SchemaDescriptor};
use gnitz_store::storage::{Batch, Layout, MemBatch, WireChunk, MAX_BATCH_REGIONS};
use gnitz_wire::control::{frame_blocks, peek_control_block, DecodedControl};
use gnitz_wire::{WireFlags, WireStatus};

/// The operative bound on **every** reply the server emits, forwarded or not,
/// and the limit the HELLO ACK advertises (`Peer::send_hello_ack`). A worker
/// frame reaches a client verbatim, so this is the only readable size there; for
/// a reply the master consumes instead — `HasPk`, `Gather`, the
/// unique pre-flight — it turns a would-be `try_reserve` abort into an error at
/// the producer. It bounds the one non-reply a worker emits too, its exchange
/// partition (`publish_exchange`), so nothing the engine sends is unbounded.
/// The server's *ingress* limit is the wire constant itself.
pub(crate) const FRAME_CAP: usize = gnitz_wire::MAX_FRAME_PAYLOAD_SERVER;

/// The one text for a reply that cannot be framed within [`FRAME_CAP`].
pub(crate) fn oversized_frame_message(sz: usize) -> String {
    format!(
        "reply wire_size={sz} exceeds the maximum frame payload {FRAME_CAP}; \
         one row wider than the cap cannot be returned at all"
    )
}

/// A relation's wire identity: the target id, the schema, and the encoded block
/// describing that schema — one value, because the scatter writer needs all
/// three and a slot sized from one shape but filled from another is corruption.
///
/// Every constructor *derives* the block; none accepts one.
pub(crate) struct WireSchema {
    /// The relation id, as the catalog keys it. Narrowing to the block's and the
    /// frame's widths happens here and nowhere else.
    tid: i64,
    descriptor: SchemaDescriptor,
    block: Rc<Vec<u8>>,
}

impl WireSchema {
    /// A one-off **anonymous** block, encoded here and cached nowhere: smaller in
    /// every worker's SAL slot than [`Self::from_catalog`]'s cached *named* one,
    /// and the only option for a schema no catalog entry describes.
    pub(crate) fn encoded(tid: i64, descriptor: SchemaDescriptor) -> Self {
        WireSchema {
            tid,
            block: Rc::new(crate::catalog::encode_schema_block(&descriptor, tid as u32)),
            descriptor,
        }
    }

    /// `tid`'s catalog entry: the cached *named* block, built from `descriptor`
    /// by the one call below and reused until DDL invalidates it.
    pub(crate) fn from_catalog(
        cat: &mut crate::catalog::CatalogEngine,
        tid: i64,
        descriptor: SchemaDescriptor,
    ) -> Self {
        let entry = cat.schema_wire_entry(tid, &descriptor);
        WireSchema { tid, descriptor, block: entry.block }
    }

    pub(crate) fn descriptor(&self) -> &SchemaDescriptor {
        &self.descriptor
    }

    /// `rest` addressed to this relation: its target id and schema block, with
    /// the caller's own header fields kept.
    pub(crate) fn frame<'a>(&'a self, rest: WireMsg<'a>) -> WireMsg<'a> {
        WireMsg {
            target_id: self.tid as u64,
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
    /// destination — no per-worker sub-`Batch` in between. Valid only for a
    /// schema with no German-string column.
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

    fn wire_byte_size(&self) -> usize {
        match *self {
            WireData::None => 0,
            WireData::Whole(b) => b.wire_byte_size(),
            WireData::Range { batch, rows, .. } => batch.wire_byte_size_range(rows),
            WireData::Scattered { batch, indices } => batch.wire_byte_size_range(indices.len()),
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
    /// These bytes *are* the frame's schema block, and their length sizes it;
    /// `None` emits none and leaves `has_schema` clear. Usually from a
    /// [`WireSchema`], which pairs them with the descriptor they encode.
    pub schema_block: Option<&'a [u8]>,
    pub blob: &'a [u8],
}

impl<'a> WireMsg<'a> {
    fn has_data(&self) -> bool {
        self.data.row_count() > 0
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        let mut total = gnitz_wire::control::ctrl_block_size(self.blob.len());
        if let Some(block) = self.schema_block {
            total += block.len();
        }
        if self.has_data() {
            total += self.data.wire_byte_size();
        }
        total
    }

    /// Encode into the front of `out`, returning bytes written; panics if `out` is
    /// shorter than [`size`](WireMsg::size).
    pub fn encode(&self, out: &mut [u8]) -> usize {
        let has_data = self.has_data();

        let wire_flags = WireFlags {
            has_schema: self.schema_block.is_some(),
            has_data,
            // Maps `b.layout()` with no re-verify: a non-`Raw` tag was certified
            // (debug-verified) at its producer, so the shipped claim is
            // verified-by-construction.
            batch_consolidated: has_data && self.data.batch().is_some_and(|b| b.layout() == Layout::Consolidated),
            ..self.flags
        };

        let mut pos = gnitz_wire::control::encode_ctrl_block(
            out,
            &gnitz_wire::control::ControlHeader {
                status: self.status,
                target_id: self.target_id,
                flags: wire_flags,
                arg0: self.arg0,
                arg1: self.arg1,
            },
            self.blob,
        );

        if let Some(block) = self.schema_block {
            let end = pos + block.len();
            out[pos..end].copy_from_slice(block);
            pos = end;
        }

        if has_data {
            let tid = self.target_id as u32;
            pos += match self.data {
                WireData::None => unreachable!("has_data implies a batch"),
                WireData::Whole(b) => b.encode_to_wire(tid, out, pos, false),
                WireData::Range { batch, start, rows } => batch.encode_range_to_wire(start, rows, tid, out, pos),
                WireData::Scattered { batch, indices } => batch.encode_scattered_to_wire(indices, tid, out, pos),
            };
        }

        pos
    }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Validate that a peer-supplied schema descriptor matches the expected one.
/// Applied at every trust boundary where rows are decoded against a descriptor
/// the sender chose (client INSERT frames, worker reply trains) — batch append
/// helpers do not validate shape, so an unguarded mismatch turns into
/// misinterpreted bytes handed onward.
pub(crate) fn validate_schema_match(wire: &SchemaDescriptor, expected: &SchemaDescriptor) -> Result<(), String> {
    if wire == expected {
        return Ok(());
    }
    // `==` is the verdict; the scan only *names* the first differing column and
    // restates no field list — `SchemaColumn: PartialEq` covers every field, and
    // `Debug` renders exactly the fields `PartialEq` compares. Every error path
    // that surfaces this string does so to a human, and a 65-column schema is not
    // diffable by eye.
    let at = (wire.num_columns() == expected.num_columns())
        .then(|| (0..wire.num_columns()).find(|&i| wire.columns[i] != expected.columns[i]))
        .flatten()
        .map_or(String::new(), |i| format!(" at column {i}"));
    Err(format!("Schema mismatch{at}: expected {expected:?}, got {wire:?}"))
}

/// Wire schema of every unique pre-flight reply frame: the leading `n_promoted`
/// columns of `idx_schema`, all marked PK. Its `pk_stride` is exactly
/// `idx_key_size`, so the OPK leading-key span fills that PK region verbatim —
/// built per-index because no single fixed-width column can represent a
/// composite (e.g. 24-byte) span. The one definition shared by the worker's
/// encoder (`send_unique_preflight_keys`) and the master's merge decoder, so the
/// frame layout agrees by construction.
///
/// `idx_schema` must come from `make_index_schema`, whose columns are all
/// non-nullable — which is what satisfies the constructor's non-nullable-PK
/// assertion.
pub(crate) fn unique_preflight_wire_schema(idx_schema: &SchemaDescriptor, n_promoted: usize) -> SchemaDescriptor {
    let cols = &idx_schema.columns[..n_promoted];
    let pks: Vec<u32> = (0..n_promoted as u32).collect();
    SchemaDescriptor::new(cols, &pks)
}

/// Full decoded wire message.
pub struct DecodedWire {
    pub control: DecodedControl,
    /// The frame's own schema block, decoded; `None` when the frame carried none.
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<Batch>,
}

/// Decode a client frame. The batch stays `Raw`: a client's layout claim is not trusted.
pub fn decode_client_frame(
    data: &[u8],
    control: DecodedControl,
    hint: Option<&SchemaDescriptor>,
) -> Result<DecodedWire, &'static str> {
    let (schema, data_batch) = decode_frame(data, &control, hint, Batch::decode_foreign_wal_block)?;
    Ok(DecodedWire { control, schema, data_batch })
}

/// Decode one SAL slot.
pub fn decode_sal_slot(data: &[u8]) -> Result<DecodedWire, &'static str> {
    let control = peek_control_block(data)?;
    let (schema, data_batch) = decode_frame(data, &control, None, |b, s| Batch::decode_from_wal_block(b, s, false))?;
    let mut decoded = DecodedWire { control, schema, data_batch };
    certify_engine_frame(&mut decoded);
    Ok(decoded)
}

/// Decode one W2M ring frame into an owned `DecodedWire`. The append relocates
/// the blob heap: an exchange frame can carry a whole unfiltered heap, and this
/// is where it is compacted.
pub fn decode_wire_ipc(data: &[u8]) -> Result<DecodedWire, &'static str> {
    let control = peek_control_block(data)?;
    let (schema, data_batch) = decode_frame(data, &control, None, |block, schema| {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = gnitz_store::storage::decode_mem_batch_from_wal_block(block, schema, false, &mut offsets)?;
        let mut owned = Batch::with_capacity(schema, mb.len());
        owned.append_mem_batch(&mb);
        Ok(owned)
    })?;
    let mut decoded = DecodedWire { control, schema, data_batch };
    certify_engine_frame(&mut decoded);
    Ok(decoded)
}

/// One frame of a worker train, borrowed from `data`. A frame carrying its own
/// schema block must carry `expected`.
pub(crate) fn decode_train_frame<'a>(
    data: &'a [u8],
    control: &DecodedControl,
    expected: &SchemaDescriptor,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<Option<MemBatch<'a>>, String> {
    let (block_schema, batch) = decode_frame(data, control, Some(expected), move |block, schema| {
        gnitz_store::storage::decode_mem_batch_from_wal_block(block, schema, false, offsets)
    })?;
    if let Some(s) = &block_schema {
        validate_schema_match(s, expected)?;
    }
    Ok(batch)
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

/// A frame's own schema, and its data decoded through `decode` against that
/// schema or else `hint`.
fn decode_frame<'a, B>(
    data: &'a [u8],
    control: &DecodedControl,
    hint: Option<&SchemaDescriptor>,
    decode: impl FnOnce(&'a [u8], &SchemaDescriptor) -> Result<B, &'static str>,
) -> Result<(Option<SchemaDescriptor>, Option<B>), &'static str> {
    let blocks = frame_blocks(data, control)?;
    let block_schema = match blocks.schema {
        Some(r) => Some(decode_schema_block(&data[r])?),
        None => None,
    };
    let Some(r) = blocks.data else {
        return Ok((block_schema, None));
    };
    let schema = block_schema
        .as_ref()
        .or(hint)
        .ok_or("a data block without a schema block")?;
    let batch = decode(&data[r], schema)?;
    Ok((block_schema, Some(batch)))
}

#[cfg(test)]
#[path = "tests/wire.rs"]
mod tests;
