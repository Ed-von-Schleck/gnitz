//! Wire protocol: IPC message codec, encode/decode.

use std::rc::Rc;

use gnitz_store::schema::{decode_schema_block, SchemaDescriptor};
use gnitz_store::storage::{Batch, Layout, MemBatch, MAX_BATCH_REGIONS};
use gnitz_wire::control::{peek_control_block_ipc, DecodedControl};
use gnitz_wire::{FLAG_HAS_DATA, FLAG_HAS_SCHEMA};

/// The operative bound on **every** reply the server emits, forwarded or not,
/// and the limit the HELLO ACK advertises (`Peer::send_hello_ack`). A worker
/// frame reaches a client verbatim, so this is the only readable size there; for
/// a reply the master consumes instead — `HasPk`, `Gather`, `SeekByIndex`, the
/// unique pre-flight — it turns a would-be `try_reserve` abort into an error at
/// the producer. It bounds the one non-reply a worker emits too, its exchange
/// partition (`publish_exchange`), so nothing the engine sends is unbounded.
/// The server's *ingress* limit is the wire constant itself.
pub(crate) const FRAME_CAP: usize = gnitz_wire::MAX_FRAME_PAYLOAD_SERVER;

/// Ceiling on a concatenation of client-bound frames — the scan heads a fan-out
/// coalesces, and the replies `Peer` holds corked — bounding the copy paid to
/// save their per-frame `OP_SEND`/`OP_TIMEOUT`/`OP_ASYNC_CANCEL` triples.
///
/// `fanout_coalesced_egress_bench` (reactor tests) sweeps it: the win grows with
/// worker count and shrinks with total size, breaking even around 128 KiB at two
/// workers and still large there at eight.
pub(crate) const COALESCE_MAX_BYTES: usize = 32 * 1024;

/// The one text for a reply that cannot be framed, shared by the two producers
/// that check [`FRAME_CAP`].
pub(crate) fn oversized_frame_message(sz: usize) -> String {
    format!(
        "reply wire_size={sz} exceeds the maximum frame payload {FRAME_CAP}; \
         one row wider than the cap cannot be returned at all"
    )
}

/// Set on the last (or only) scan chunk from a worker. `pub` in `gnitz_wire` so
/// the bit is guarded against every other wire flag; narrowed to this crate
/// here, because nothing outside the engine sets or reads it. It is forwarded to
/// clients along with the rest of the frame's flags, and ignored there.
pub(crate) use gnitz_wire::FLAG_SCAN_LAST;

// ---------------------------------------------------------------------------
// Chunked distributed-backfill coordination: the two overloads of `seek_col_idx`
// ---------------------------------------------------------------------------
//
// A distributed CREATE-VIEW backfill runs one exchange round per source chunk,
// and every worker must run the SAME number of rounds (short partitions pad),
// so termination and SAL reclamation are decided collectively by the master and
// stamped back on each relay. Both legs overload the otherwise-unused
// `seek_col_idx`, whose `0` reads as "no backfill coordination" — which is what
// a steady-state exchange already sends.

/// Up-leg (worker→master, on `FLAG_EXCHANGE`): the per-chunk PAD bit. Set when
/// this worker's `drain_chunk` returned `None` — its partition is exhausted and
/// the chunk it is participating in is an empty pad. The master ANDs this bit
/// across all workers for a round; an all-pad round is the final round.
pub const BACKFILL_PAD_BIT: u64 = 1;

/// Down-leg (master→worker, on `ExchangeRelay`): the collective decision the
/// master stamps onto a round's relay after ANDing the round's pad bits and
/// checking SAL space. `CONTINUE` keeps the loop going; `STOP` ends every
/// worker's loop on the same (all-pad) round; `CHECKPOINT` is a continue that
/// also tells the worker to advance its SAL read epoch + reset its read cursor
/// inline (the master reclaims the SAL write side at the next round barrier).
pub const BACKFILL_DECISION_CONTINUE: u64 = 0;
pub const BACKFILL_DECISION_STOP: u64 = 1;
pub const BACKFILL_DECISION_CHECKPOINT: u64 = 2;

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
#[derive(Clone, Copy)]
pub enum WireData<'a> {
    Whole(Option<&'a Batch>),
    /// The rows `indices` selects, in that order, encoded straight into the
    /// destination — no per-worker sub-`Batch` in between. Valid only for a
    /// schema with no German-string column.
    ///
    /// The descriptor rides here because both halves read region strides off
    /// it — `wire_block_size` sizes the block, `encode_scattered_to_wire` carves
    /// it — and a frame's schema block is bytes, not strides.
    Scattered {
        batch: &'a Batch,
        indices: &'a [u32],
        schema: &'a SchemaDescriptor,
    },
}

impl Default for WireData<'_> {
    fn default() -> Self {
        WireData::Whole(None)
    }
}

impl<'a> WireData<'a> {
    /// Rows this payload carries; `0` means the slot or frame is dataless.
    pub(crate) fn row_count(&self) -> usize {
        match *self {
            WireData::Whole(b) => b.map(|b| b.len()).unwrap_or(0),
            WireData::Scattered { indices, .. } => indices.len(),
        }
    }

    fn layout_batch(&self) -> Option<&'a Batch> {
        match *self {
            WireData::Whole(b) => b,
            WireData::Scattered { batch, .. } => Some(batch),
        }
    }

    fn wire_byte_size(&self) -> usize {
        match *self {
            WireData::Whole(b) => b.map(|b| b.wire_byte_size()).unwrap_or(0),
            WireData::Scattered { indices, schema, .. } => {
                gnitz_store::storage::wire_block_size(schema, indices.len(), 0)
            }
        }
    }
}

/// One IPC/WAL wire message. Build it once, then [`size`](WireMsg::size) it and
/// encode it: both read the same value, so the byte count a caller reserves and
/// the bytes the encoder writes cannot disagree.
///
/// Every field defaults to zero/absent (`status` default 0 is `STATUS_OK`), so a
/// caller names only what it sends:
///
/// ```ignore
/// let msg = ipc::WireMsg { request_id, status: STATUS_ERROR, error_msg: msg, ..Default::default() };
/// writer.send_msg(request_id, &msg);
/// ```
#[derive(Clone, Copy, Default)]
pub struct WireMsg<'a> {
    pub target_id: u64,
    pub client_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    pub seek_col_idx: u64,
    pub request_id: u64,
    pub status: u32,
    pub error_msg: &'a [u8],
    pub data: WireData<'a>,
    /// These bytes *are* the frame's schema block, and their length sizes it;
    /// `None` emits none and leaves `FLAG_HAS_SCHEMA` clear. Usually from a
    /// [`WireSchema`], which pairs them with the descriptor they encode.
    pub schema_block: Option<&'a [u8]>,
    pub seek_pk_extra: &'a [u8],
}

impl<'a> WireMsg<'a> {
    fn has_data(&self) -> bool {
        self.data.row_count() > 0
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        let mut total = gnitz_wire::control::ctrl_block_size(self.error_msg.len(), self.seek_pk_extra.len());
        if let Some(block) = self.schema_block {
            total += block.len();
        }
        if self.has_data() {
            total += self.data.wire_byte_size();
        }
        total
    }

    /// Encode into `out[offset..]` with WAL block checksums, for the durable and
    /// cross-process paths (WAL, SAL). Returns bytes written; panics if `out` is
    /// too small for [`size`](WireMsg::size).
    pub fn encode(&self, out: &mut [u8], offset: usize) -> usize {
        self.encode_impl(out, offset, true)
    }

    /// Encode without checksums, for the frames whose reader verifies none: the
    /// W2M ring (`decode_wire_ipc`) and client egress (`peek_control_block_ipc`).
    pub fn encode_ipc(&self, out: &mut [u8], offset: usize) -> usize {
        self.encode_impl(out, offset, false)
    }

    fn encode_impl(&self, out: &mut [u8], offset: usize, checksum: bool) -> usize {
        let has_data = self.has_data();

        let mut wire_flags = self.flags;
        if self.schema_block.is_some() {
            wire_flags |= FLAG_HAS_SCHEMA;
        }
        if has_data {
            wire_flags |= FLAG_HAS_DATA;
            // Maps `b.layout()` with no re-verify: a non-`Raw` tag was certified
            // (debug-verified) at its producer, so the shipped claim is
            // verified-by-construction.
            wire_flags |= self.data.layout_batch().unwrap().layout().to_wire_flags();
        }

        let written = gnitz_wire::control::encode_ctrl_block(
            out,
            offset,
            &gnitz_wire::control::ControlHeader {
                status: self.status,
                target_id: self.target_id,
                client_id: self.client_id,
                flags: wire_flags,
                seek_pk: self.seek_pk,
                seek_col_idx: self.seek_col_idx,
                request_id: self.request_id,
            },
            self.error_msg,
            self.seek_pk_extra,
            checksum,
        );
        let mut pos = offset + written;

        if let Some(block) = self.schema_block {
            let end = pos + block.len();
            out[pos..end].copy_from_slice(block);
            pos = end;
        }

        if has_data {
            pos += match self.data {
                WireData::Whole(b) => b.unwrap().encode_to_wire(self.target_id as u32, out, pos, checksum),
                WireData::Scattered { batch, indices, schema } => {
                    batch.encode_scattered_to_wire(indices, schema, self.target_id as u32, out, pos, checksum)
                }
            };
        }

        pos - offset
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

/// Full decoded wire message. `B` is the data batch's form: an owned [`Batch`]
/// — the default — or a [`MemBatch`] borrowing the frame bytes, from the
/// zero-copy ring decoder.
pub struct DecodedWire<B = Batch> {
    pub control: DecodedControl,
    /// The frame's own schema block, decoded — or, for a continuation frame
    /// that carries data with no block, the hint its data was decoded against.
    /// A data batch is therefore never present without the descriptor it was
    /// read under.
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<B>,
}

/// Checksum-verified control-block parse, for a frame that crossed a trust or
/// durability boundary. It bounds every read by the block's own size field, so
/// the routing fields and the decode that follows read one directory — a
/// malicious client cannot forge one that points the auth check at one offset
/// and the decoder at another.
pub(crate) use gnitz_wire::control::peek_control_block;

/// Client-boundary decode with a pre-parsed control block (the `handle_message`
/// single-parse path). Full checksum verification on all three blocks — the
/// control block's by the [`peek_control_block`] that produced `control`.
///
/// The batch comes back `Raw`: unlike [`decode_wire`] this never installs the
/// frame's `FLAG_BATCH_CONSOLIDATED` claim, which a client must not be trusted
/// to make.
pub fn decode_wire_with_ctrl(
    data: &[u8],
    control: DecodedControl,
    hint: Option<&SchemaDescriptor>,
) -> Result<DecodedWire, &'static str> {
    decode_frame(data, control, hint, true, |block, schema| {
        Batch::decode_from_wal_block(block, schema, true).map(|(b, _)| b)
    })
}

/// Decode a full checksum-verified wire message from raw bytes: the SAL, the
/// boot replay and the worker's own SAL consumption all read frames this way.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
    let control = peek_control_block(data)?;
    let mut decoded = decode_wire_with_ctrl(data, control, None)?;
    certify_engine_frame(&mut decoded);
    Ok(decoded)
}

/// Decode one W2M ring frame into an owned `DecodedWire`. No checksum
/// verification — the ring is a trusted intra-process mapping, unlike the SAL —
/// and a control-only frame decodes fine (`data_batch: None`).
///
/// Built through the zero-copy decode, so `append_mem_batch` **relocates** the
/// blob heap where `Batch::decode_from_wal_block` would copy it verbatim. That
/// is the compaction point for an exchange frame's full unfiltered heap, and a
/// cost (one cell rewrite per string cell, against one bulk `memcpy`) for a
/// frame with no dead heap. One policy for the ring, not the cheaper of the two
/// per frame.
pub fn decode_wire_ipc(data: &[u8]) -> Result<DecodedWire, &'static str> {
    let control = peek_control_block_ipc(data)?;
    let mut decoded = decode_frame(data, control, None, false, |block, schema| {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = gnitz_store::storage::decode_mem_batch_from_wal_block(block, schema, &mut offsets)?;
        let mut owned = Batch::with_capacity(schema, mb.len());
        owned.append_mem_batch(&mb);
        Ok(owned)
    })?;
    certify_engine_frame(&mut decoded);
    Ok(decoded)
}

/// Decode a W2M ring frame without copying data: the data block comes back as
/// a [`MemBatch`] borrowing `data`, so the caller keeps `data` live (holds the
/// `W2mSlot`) while reading it, and lends the region-offset array the view
/// borrows (see [`MemBatch::offsets`]).
///
/// Takes a pre-parsed `control` block (from `peek_control_block_ipc`) so the
/// train readers can inspect the header before choosing a decode path without
/// a second parse. `hint` is what a continuation frame decodes against.
pub(crate) fn decode_wire_ipc_zero_copy_with_ctrl<'a>(
    data: &'a [u8],
    control: DecodedControl,
    hint: Option<&SchemaDescriptor>,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<DecodedWire<MemBatch<'a>>, &'static str> {
    decode_frame(data, control, hint, false, move |block, schema| {
        gnitz_store::storage::decode_mem_batch_from_wal_block(block, schema, offsets)
    })
}

/// Install an engine-authored frame's layout claim: its `FLAG_BATCH_CONSOLIDATED`
/// bit is real, and skipping the re-fold is the point of sending it, so the batch
/// is raised off `Raw`. `certify_layout`
/// debug-verifies what it installs, which is why the client path
/// (`decode_wire_with_ctrl`) never comes through here — a lying client frame
/// must be answered with an error, not a debug-build abort.
fn certify_engine_frame(decoded: &mut DecodedWire) {
    let flags = decoded.control.flags;
    if let Some(b) = decoded.data_batch.as_mut() {
        b.certify_layout(Layout::from_wire_flags(flags));
    }
}

/// The one frame walk behind every decoder: locate the schema and data blocks
/// after the control block, resolve the descriptor the data is read against,
/// and decode the data block through `decode` — which is the only thing the
/// owned and the zero-copy decoders do differently.
///
/// A frame that carries a schema block decodes against that block. A
/// continuation frame — data with no block of its own — decodes against `hint`,
/// and without one there is nothing to do but reject it.
fn decode_frame<'a, B>(
    data: &'a [u8],
    control: DecodedControl,
    hint: Option<&SchemaDescriptor>,
    verify: bool,
    decode: impl FnOnce(&'a [u8], &SchemaDescriptor) -> Result<B, &'static str>,
) -> Result<DecodedWire<B>, &'static str> {
    let has_schema = control.flags & FLAG_HAS_SCHEMA != 0;
    let has_data = control.flags & FLAG_HAS_DATA != 0;
    let mut off = control.block_size;

    let block_schema = if has_schema {
        let sblock = gnitz_wire::wal::block_slice_at(data, off)?;
        off += sblock.len();
        Some(decode_schema_block(sblock, verify)?)
    } else {
        None
    };
    if !has_data {
        return Ok(DecodedWire {
            control,
            schema: block_schema,
            data_batch: None,
        });
    }
    // `or_else`, not `or`: `Option::or` evaluates its argument, so the 360-byte
    // hint copy would run per decoded frame and be discarded by every frame that
    // carried its own schema block.
    let schema = block_schema
        .or_else(|| hint.copied())
        .ok_or("FLAG_HAS_DATA without FLAG_HAS_SCHEMA")?;
    let dblock = gnitz_wire::wal::block_slice_at(data, off)?;
    let data_batch = decode(dblock, &schema)?;
    Ok(DecodedWire {
        control,
        schema: Some(schema),
        data_batch: Some(data_batch),
    })
}

#[cfg(test)]
#[path = "tests/wire.rs"]
mod tests;
