//! Wire protocol: IPC message codec, encode/decode.

use std::rc::Rc;

use gnitz_engine::schema::{decode_schema_block, SchemaDescriptor};
use gnitz_engine::storage::{Batch, Layout, MemBatch, MAX_BATCH_REGIONS};
use gnitz_wire::control::{peek_control_block, peek_control_block_ipc, DecodedControl};
use gnitz_wire::{wire_flags_get_schema_version, FLAG_HAS_DATA, FLAG_HAS_SCHEMA};

/// The most one reply frame may carry on its way to a client. A worker frame is
/// forwarded verbatim, and the client's ceiling is `min(server, client)` over the
/// limit the HELLO ACK advertises — which is `MAX_FRAME_PAYLOAD_SERVER`. So it
/// bounds the chunk split point, the single-frame paths that cannot chunk, and
/// the master's merge of per-worker replies alike. The W2M ring's own bound,
/// `MAX_W2M_MSG`, is deliberately the larger of the two — it is the right limit
/// for a train the master consumes rather than forwards.
pub(crate) const FRAME_CAP: usize = gnitz_wire::MAX_FRAME_PAYLOAD_SERVER;
const _: () = assert!(FRAME_CAP < super::w2m::MAX_W2M_MSG as usize);

/// Set on the last (or only) scan chunk from a worker. `pub` in `gnitz_wire` so
/// the bit is guarded against every other wire flag; narrowed to this crate
/// here, because it is stripped before a frame reaches a client.
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
    /// A one-off anonymous block, encoded here and cached nowhere — for a
    /// projected or synthetic schema no catalog entry describes.
    pub(crate) fn encoded(tid: i64, descriptor: SchemaDescriptor) -> Self {
        WireSchema {
            tid,
            block: Rc::new(gnitz_engine::catalog::encode_schema_block(&descriptor, tid as u32)),
            descriptor,
        }
    }

    /// `tid`'s catalog entry: the cached *named* block, built from `descriptor`
    /// by the one call below and reused until DDL invalidates it.
    pub(crate) fn from_catalog(
        cat: &mut gnitz_engine::catalog::CatalogEngine,
        tid: i64,
        descriptor: SchemaDescriptor,
    ) -> Self {
        let entry = cat.schema_wire_entry(tid, &descriptor);
        WireSchema {
            tid,
            descriptor,
            block: entry.block,
        }
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
    Range {
        batch: &'a Batch,
        start_row: usize,
        count: usize,
    },
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
            WireData::Whole(b) => b.map(|b| b.count).unwrap_or(0),
            WireData::Range { count, .. } => count,
            WireData::Scattered { indices, .. } => indices.len(),
        }
    }

    fn layout_batch(&self) -> Option<&'a Batch> {
        match *self {
            WireData::Whole(b) => b,
            WireData::Range { batch, .. } | WireData::Scattered { batch, .. } => Some(batch),
        }
    }

    fn wire_byte_size(&self) -> usize {
        match *self {
            WireData::Whole(b) => b.map(|b| b.wire_byte_size()).unwrap_or(0),
            WireData::Range { batch, count, .. } => batch.wire_byte_size_range(count),
            WireData::Scattered { indices, schema, .. } => {
                gnitz_engine::storage::wire_block_size(schema, indices.len(), 0)
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

    /// Encode into a fresh `Vec` sized by [`size`](WireMsg::size).
    #[cfg(test)]
    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.size()];
        self.encode(&mut buf, 0);
        buf
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
                WireData::Range {
                    batch,
                    start_row,
                    count,
                } => batch.encode_range_to_wire(start_row, count, self.target_id as u32, out, pos, checksum),
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
    // `Debug` renders exactly the fields `PartialEq` compares. Four error paths
    // surface this string, and a 65-column schema is not diffable by eye.
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
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<Batch>,
}

/// Zero-copy decoded wire message: data borrows directly from the source buffer.
pub struct DecodedWireZeroCopy<'a> {
    pub control: DecodedControl,
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<MemBatch<'a>>,
}

/// A schema descriptor paired with the server-side schema version.
/// Passed to decode functions so continuation frames (no schema block)
/// can be decoded against a cached schema and the version can be verified
/// against what the sender embedded in `wire_flags`.
pub struct SchemaWithVersion<'a> {
    pub descriptor: &'a SchemaDescriptor,
    pub version: u16,
}

/// Parse a frame's control block once, bounds-limited to the block's own
/// `block_size` slice (exactly the slice the full decode would parse, so the
/// routing/auth fields and the decode see one directory — a malicious client
/// cannot forge a directory that points the auth check at one offset and the
/// decoder at another).
pub fn peek_frame_control(data: &[u8]) -> Result<DecodedControl, &'static str> {
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    peek_control_block(ctrl)
}

/// Client-boundary decode with a pre-parsed control block (the `handle_message`
/// single-parse path). Full checksum verification on all three blocks — the
/// control block's by the `peek_frame_control` that produced `control`.
///
/// The batch comes back `Raw`: unlike [`decode_wire`] this never installs the
/// frame's `FLAG_BATCH_SORTED` / `FLAG_BATCH_CONSOLIDATED` claim, which a client
/// must not be trusted to make.
pub fn decode_wire_with_ctrl(
    data: &[u8],
    control: DecodedControl,
    schema_hint: Option<SchemaWithVersion<'_>>,
) -> Result<DecodedWire, &'static str> {
    let ctrl_size = control.block_size;
    decode_wire_body(data, ctrl_size, control, schema_hint, true)
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
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    let control = peek_control_block_ipc(ctrl)?;
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let zc = decode_wire_ipc_zero_copy_with_ctrl(data, control, None, &mut offsets)?;
    let flags = zc.control.flags;
    let schema = zc.schema;
    let data_batch = match zc.data_batch {
        Some(mb) => {
            let sch = schema.as_ref().ok_or("FLAG_HAS_DATA set but no schema")?;
            let mut owned = Batch::with_capacity(*sch, mb.count);
            owned.append_mem_batch(&mb);
            // The wire flags are ground truth. `append_mem_batch` leaves `owned`
            // `Raw`; raise it to the frame's claim, debug-verifying the data.
            owned.certify_layout(Layout::from_wire_flags(flags), sch);
            Some(owned)
        }
        None => None,
    };
    Ok(DecodedWire {
        control: zc.control,
        schema,
        data_batch,
    })
}

/// Decode a full checksum-verified wire message from raw bytes: the SAL, the
/// boot replay and the worker's own SAL consumption all read frames this way.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    let control = peek_control_block(ctrl)?;
    let mut decoded = decode_wire_body(data, ctrl.len(), control, None, true)?;
    // An engine-authored frame (SAL consumption, W2M, boot replay): its layout
    // claim is real and skipping the re-sort is the point of sending it, so
    // raise the batch off `Raw`. `certify_layout` debug-verifies what it
    // installs, which is why the client path (`decode_wire_with_ctrl`) does not
    // come through here — a lying client frame must be answered with an error,
    // not a debug-build abort.
    let flags = decoded.control.flags;
    if let (Some(b), Some(schema)) = (decoded.data_batch.as_mut(), decoded.schema.as_ref()) {
        b.certify_layout(Layout::from_wire_flags(flags), schema);
    }
    Ok(decoded)
}

/// Resolve a frame's schema and locate its data block — the prologue both
/// decoders run. Returns the schema the data block must be read against, and
/// the data block itself when the frame carries one.
///
/// With a `hint`, any block in the frame must equal it, and the hint's
/// descriptor is what comes back — it carries the placement the block does not.
fn split_wire_blocks<'a>(
    data: &'a [u8],
    ctrl_size: usize,
    flags: u64,
    hint: Option<SchemaWithVersion<'_>>,
    verify: bool,
) -> Result<(Option<SchemaDescriptor>, Option<&'a [u8]>), &'static str> {
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    // A continuation frame carries data with no block of its own; without a hint
    // to decode it against there is nothing to do but reject it. The hint's
    // version must match what the sender stamped into `flags`, or the cached
    // descriptor no longer describes these rows.
    if has_data && !has_schema {
        let h = hint.as_ref().ok_or("FLAG_HAS_DATA without FLAG_HAS_SCHEMA")?;
        if wire_flags_get_schema_version(flags) != h.version {
            return Err("schema version mismatch on continuation frame");
        }
        wire_schema = Some(*h.descriptor);
    }

    if has_schema {
        let sblock = gnitz_wire::wal::block_slice_at(data, off)?;
        let parsed = decode_schema_block(sblock, verify)?;
        wire_schema = Some(match hint {
            Some(ref h) => {
                if parsed != *h.descriptor {
                    return Err("schema mismatch: client schema differs from server schema");
                }
                *h.descriptor
            }
            None => parsed,
        });
        off += sblock.len();
    }

    let dblock = if has_data {
        Some(gnitz_wire::wal::block_slice_at(data, off)?)
    } else {
        None
    };
    Ok((wire_schema, dblock))
}

/// Leaves the decoded batch `Raw`. Whether the frame's layout claim may be
/// installed on top depends on who sent it, so that is the caller's call.
fn decode_wire_body(
    data: &[u8],
    ctrl_size: usize,
    control: DecodedControl,
    schema_hint: Option<SchemaWithVersion<'_>>,
    verify_checksum: bool,
) -> Result<DecodedWire, &'static str> {
    let (schema, dblock) = split_wire_blocks(data, ctrl_size, control.flags, schema_hint, verify_checksum)?;
    let data_batch = match dblock {
        Some(dblock) => {
            let eff_schema = schema.as_ref().ok_or("no schema for data block")?;
            Some(Batch::decode_from_wal_block(dblock, eff_schema, verify_checksum)?.0)
        }
        None => None,
    };

    Ok(DecodedWire {
        control,
        schema,
        data_batch,
    })
}

/// Decode a W2M IPC message without copying data: schema is parsed from the
/// wire bytes directly and the data block is returned as a `MemBatch<'a>`
/// that borrows slices from `data`.  The caller must keep `data` live (i.e.
/// hold the `W2mSlot`) until it is done reading from the `MemBatch`.
///
/// Takes a pre-parsed `control` block (from `peek_control_block`) so the
/// caller can inspect flags before choosing a decode path without
/// triggering a redundant parse, and the region-offset array the returned
/// `MemBatch` borrows (see [`MemBatch::offsets`]).
pub(crate) fn decode_wire_ipc_zero_copy_with_ctrl<'a>(
    data: &'a [u8],
    control: DecodedControl,
    schema_hint: Option<SchemaWithVersion<'_>>,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<DecodedWireZeroCopy<'a>, &'static str> {
    let (schema, dblock) = split_wire_blocks(data, control.block_size, control.flags, schema_hint, false)?;
    let data_batch = match dblock {
        Some(dblock) => {
            let eff_schema = schema.as_ref().ok_or("no schema for data block")?;
            Some(gnitz_engine::storage::decode_mem_batch_from_wal_block(
                dblock, eff_schema, offsets,
            )?)
        }
        None => None,
    };

    Ok(DecodedWireZeroCopy {
        control,
        schema,
        data_batch,
    })
}
