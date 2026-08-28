//! Wire protocol: IPC message codec, schema conversion, encode/decode.

use std::rc::Rc;

use gnitz_engine::catalog::ColumnDef;
use gnitz_engine::schema::SchemaDescriptor;
use gnitz_engine::storage::{Batch, MemBatch, MAX_BATCH_REGIONS};
use gnitz_wire::schema_block::SchemaBlockCol;

// ---------------------------------------------------------------------------
// Constants re-exported from gnitz_wire
// ---------------------------------------------------------------------------

/// The most one reply frame may carry on its way to a client. A worker frame is
/// forwarded verbatim, and the client's ceiling is `min(server, client)` over the
/// limit the HELLO ACK advertises — which is `MAX_FRAME_PAYLOAD_SERVER`. So it
/// bounds the chunk split point, the single-frame paths that cannot chunk, and
/// the master's merge of per-worker replies alike. `MAX_W2M_MSG` (4×) still
/// bounds the ring itself, and is the right limit for a train the master
/// consumes rather than forwards.
pub(crate) const FRAME_CAP: usize = gnitz_wire::MAX_FRAME_PAYLOAD_SERVER;

/// W2M-internal flag set on the last (or only) scan chunk from a worker.
/// Not part of the public wire protocol; stripped before reaching clients.
/// The master uses this to detect end-of-train without removing FLAG_CONTINUATION
/// from the TCP frame (FLAG_CONTINUATION must stay set on all worker scan frames
/// so the client's loop termination — "stop on no FLAG_CONTINUATION" — still works).
pub(crate) use gnitz_wire::FLAG_SCAN_LAST;
pub use gnitz_wire::{
    wire_flags_get_conflict_mode, wire_flags_get_schema_version, wire_flags_set_schema_version, WireConflictMode,
    FLAG_BATCH_CONSOLIDATED, FLAG_BATCH_SORTED, FLAG_CONTINUATION, FLAG_EXCHANGE, FLAG_HAS_DATA, FLAG_HAS_SCHEMA,
    STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};

/// Map a batch's layout claim to its wire flag bits. Encode normalizes
/// `Consolidated ⇒ both bits`; `layout_from_wire_flags` inverts losslessly. The
/// pair folds every bit↔enum conversion into one place so the encode and decode
/// sides can never drift.
pub(crate) fn layout_to_wire_flags(layout: gnitz_engine::storage::Layout) -> u64 {
    use gnitz_engine::storage::Layout;
    match layout {
        Layout::Raw => 0,
        Layout::Sorted => FLAG_BATCH_SORTED,
        Layout::Consolidated => FLAG_BATCH_SORTED | FLAG_BATCH_CONSOLIDATED,
    }
}

/// Recover a batch layout claim from wire flag bits. The constructor already
/// defaults `Raw`, so this is the value fed to `certify_layout` at the decode
/// boundary (which debug-verifies the data against the claim).
pub(crate) fn layout_from_wire_flags(flags: u64) -> gnitz_engine::storage::Layout {
    use gnitz_engine::storage::Layout;
    if flags & FLAG_BATCH_CONSOLIDATED != 0 {
        Layout::Consolidated
    } else if flags & FLAG_BATCH_SORTED != 0 {
        Layout::Sorted
    } else {
        Layout::Raw
    }
}

// ---------------------------------------------------------------------------
// The meta-schema block: this side's adapter to the shared codec
// ---------------------------------------------------------------------------
//
// The block's layout, its region shape, and every rule about what makes one
// admissible live in `gnitz_wire::schema_block` — the one implementation the
// client runs too. What stays here is only the translation between a
// `SchemaDescriptor` (+ the catalog `ColumnDef`s that name it) and the codec's
// neutral per-column facts.

/// One [`SchemaBlockCol`] per column: the physical shape from `schema`, and the
/// per-column catalog facts (name, `META_FLAG_HIDDEN`, `META_FLAG_SERIAL`) from
/// `defs`.
///
/// `defs` is all-or-nothing, not per-column: a relation either has one COL_TAB
/// row per physical column — so `defs[ci]` describes `schema.columns[ci]` — or
/// it has none, and every name comes out empty with every catalog flag clear.
fn schema_block_cols<'a>(schema: &SchemaDescriptor, defs: Option<&'a [ColumnDef]>) -> Vec<SchemaBlockCol<'a>> {
    let ncols = schema.num_columns();
    debug_assert!(defs.is_none_or(|d| d.len() == ncols));
    (0..ncols)
        .map(|ci| {
            let col = &schema.columns[ci];
            let def = defs.map(|d| &d[ci]);
            SchemaBlockCol {
                type_code: col.type_code,
                // For compound PKs the position within `pk_indices()` is what
                // determines decode order — column order ≠ PK order in general
                // (e.g. `PRIMARY KEY (b, a)`). Carrying the position lets the
                // decoder rebuild `pk_indices` exactly as the user wrote them.
                flags: gnitz_wire::pack_col_meta_flags(
                    col.nullable != 0,
                    def.is_some_and(|d| d.is_hidden),
                    def.is_some_and(|d| d.is_serial),
                    schema
                        .pk_indices()
                        .iter()
                        .position(|&p| p as usize == ci)
                        .map(|p| p as u8),
                ),
                name: def.map_or(&b""[..], |d| d.name.as_bytes()),
            }
        })
        .collect()
}

/// Encode a schema descriptor into a standalone WAL wire block carrying only
/// the physical column shape — no names, no catalog flags. This is what SAL
/// entries and one-off reply blocks ship: nothing engine-side reads a name, and
/// the client decodes such a block against a schema it already holds.
///
/// The returned bytes are a self-contained schema block identical to what
/// [`WireMsg::encode`] would embed. Callers cache this per table and pass it as
/// [`WireMsg::prebuilt_schema_block`] to skip rebuilding it on every SEEK/SCAN
/// response.
pub fn build_schema_wire_block(schema: &SchemaDescriptor, target_tid: u32) -> Vec<u8> {
    gnitz_wire::schema_block::encode(target_tid, &schema_block_cols(schema, None))
}

/// [`build_schema_wire_block`] plus the per-column catalog facts the descriptor
/// does not carry — name, `is_hidden`, `is_serial`. The block a *client* decodes
/// into a `Schema`, so it is the one that must be named.
pub(crate) fn build_named_schema_wire_block(schema: &SchemaDescriptor, defs: &[ColumnDef], target_tid: u32) -> Vec<u8> {
    gnitz_wire::schema_block::encode(target_tid, &schema_block_cols(schema, Some(defs)))
}

/// Get-or-build the cached schema wire block for `tid`, returning the full
/// cache entry (block, version, wire_safe, stride). On a miss the block is
/// built from the catalog's column defs and stored; it is invalidated
/// alongside them whenever DDL modifies the table. The caller supplies the
/// resolved `schema` — the call sites differ only in what they do when the
/// table has no schema (panic / minimal fallback / one-off block), which stays
/// with them.
pub(crate) fn get_or_build_schema_wire_block(
    cat: &mut gnitz_engine::catalog::CatalogEngine,
    tid: i64,
    schema: &SchemaDescriptor,
) -> gnitz_engine::catalog::SchemaWireEntry {
    if let Some(cached) = cat.get_cached_schema_wire_block(tid) {
        return cached;
    }
    // `defs` is empty exactly when `tid` names no relation: the caller then
    // passes `SchemaDescriptor::minimal_u64` as a placeholder, and a placeholder
    // has no names or flags to carry. A relation that does exist has one COL_TAB
    // row per physical column, which is what makes `defs[ci]` describe
    // `schema.columns[ci]` — callers with a *projected* schema build a one-off
    // anonymous block instead of coming here.
    let defs = cat.read_column_defs(tid);
    let block = Rc::new(if defs.is_empty() {
        build_schema_wire_block(schema, tid as u32)
    } else {
        build_named_schema_wire_block(schema, &defs, tid as u32)
    });
    let (wire_safe, wire_row_fixed_stride) = gnitz_engine::storage::compute_wire_props(schema);
    let entry = gnitz_engine::catalog::SchemaWireEntry {
        block,
        version: cat.get_schema_version(tid),
        wire_safe,
        wire_row_fixed_stride,
    };
    cat.set_schema_wire_block(tid, entry.clone());
    entry
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Encode only the ctrl WAL block (no schema, no data) into `out[offset..]`.
/// Caller pre-computes `wire_flags` (including `FLAG_HAS_SCHEMA`,
/// `FLAG_HAS_DATA`, sorted/consolidated bits, etc.) so this helper can be
/// called after the data block is already written. Returns bytes written.
///
/// Thin wrapper over the shared `gnitz_wire::control::encode_ctrl_block`
/// codec (template-and-patch fast path, German-string blob fallback), which
/// leaves the checksum field 0 — stamped here for checksummed frames.
#[inline]
pub(crate) fn encode_ctrl_block_direct(
    out: &mut [u8],
    offset: usize,
    hdr: &gnitz_wire::control::ControlHeader,
    error_msg: &[u8],
    seek_pk_extra: &[u8],
    checksum: bool,
) -> usize {
    let n = gnitz_wire::control::encode_ctrl_block(out, offset, hdr, error_msg, seek_pk_extra);
    if checksum {
        gnitz_wire::wal::stamp_checksum(&mut out[offset..offset + n], n);
    }
    n
}

/// Encoded size of the schema wire block for `schema`, or the prebuilt block's
/// length when one is supplied. [`WireMsg::size`] is its one caller, so every
/// shape of message sizes its schema block the same way.
///
/// The no-prebuilt arm sizes an *anonymous* block: a [`WireMsg`] encodes its
/// block with no defs, so every name is empty and nothing spills into the blob
/// heap. A named block always arrives prebuilt.
fn schema_block_wire_size(schema: Option<&SchemaDescriptor>, prebuilt_schema_block: Option<&[u8]>) -> usize {
    if let Some(prebuilt) = prebuilt_schema_block {
        return prebuilt.len();
    }
    gnitz_wire::schema_block::anonymous_encoded_len(schema.unwrap().num_columns())
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
    /// `schema_wire_safe` schema.
    ///
    /// The schema rides in the variant rather than being read from
    /// [`WireMsg::schema`] because both halves of this shape read region strides
    /// from it: `wire_block_size` sizes the block and `encode_scattered_to_wire`
    /// carves it. Sizing off the batch's strides while emitting off the schema's
    /// would put the slot's byte count and its bytes on two sources — and a slot
    /// is sized to reserve space inside the SAL mmap, so a disagreement writes
    /// past it.
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
/// writer.send_encoded(msg.size(), request_id as u32, |buf| { msg.encode_ipc(buf, 0); });
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
    pub schema: Option<&'a SchemaDescriptor>,
    pub data: WireData<'a>,
    /// When `Some`, these bytes *are* the schema block: they are copied in
    /// verbatim instead of encoding `schema`, and their length sizes the block.
    /// Skips the `Batch` allocation `schema` would cost on the hot SEEK/SCAN path.
    pub prebuilt_schema_block: Option<&'a [u8]>,
    pub seek_pk_extra: &'a [u8],
}

impl<'a> WireMsg<'a> {
    fn has_data(&self) -> bool {
        self.data.row_count() > 0
    }

    fn has_schema(&self) -> bool {
        (self.schema.is_some() || self.prebuilt_schema_block.is_some()) && (self.has_data() || self.status == STATUS_OK)
    }

    /// Total encoded size, without allocating.
    pub fn size(&self) -> usize {
        let mut total = gnitz_wire::control::ctrl_block_size(self.error_msg.len(), self.seek_pk_extra.len());
        if self.has_schema() {
            total += schema_block_wire_size(self.schema, self.prebuilt_schema_block);
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

    /// Encode without checksums, for trusted intra-process IPC (the W2M ring,
    /// client egress) where the shared mapping already guarantees integrity.
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
        let has_schema = self.has_schema();

        let mut wire_flags = self.flags;
        if has_schema {
            wire_flags |= FLAG_HAS_SCHEMA;
        }
        if has_data {
            wire_flags |= FLAG_HAS_DATA;
            // Maps `b.layout()` with no re-verify: a non-`Raw` tag was certified
            // (debug-verified) at its producer, so the shipped claim is
            // verified-by-construction.
            wire_flags |= layout_to_wire_flags(self.data.layout_batch().unwrap().layout());
        }

        let written = encode_ctrl_block_direct(
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

        if has_schema {
            if let Some(prebuilt) = self.prebuilt_schema_block {
                let end = pos + prebuilt.len();
                out[pos..end].copy_from_slice(prebuilt);
                pos = end;
            } else {
                let cols = schema_block_cols(self.schema.unwrap(), None);
                pos += gnitz_wire::schema_block::encode_into(out, pos, self.target_id as u32, &cols, checksum);
            }
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

pub use gnitz_engine::schema::decode_schema_block;

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

/// Decoded control fields + directory-driven control-block decoder — the
/// shared codec both ends run.
pub use gnitz_wire::control::{peek_control_block, peek_control_block_ipc, DecodedControl};

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

/// Parse the control block of a CLIENT frame once, bounds-limited to the
/// block's own `block_size` slice (exactly the slice the full decode would
/// parse, so the routing/auth fields and the decode see one directory — a
/// malicious client cannot forge a directory that points the auth check at
/// one offset and the decoder at another).
pub fn peek_client_control(data: &[u8]) -> Result<DecodedControl, &'static str> {
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    peek_control_block(ctrl)
}

/// Client-boundary decode with a pre-parsed control block (the `handle_message`
/// single-parse path). Full checksum verification on all three blocks — the
/// control block's by the `peek_client_control` that produced `control`.
///
/// The batch comes back `Raw`: unlike `decode_wire_impl` this never installs the
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

/// Decode a full IPC wire message from raw bytes.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, true)
}

/// The three transaction-shaped request frames — `PUSH_TXN`, `DDL_TXN` and
/// `SCAN_MULTI` — decode through the shared `gnitz_wire::txn_frame` codec, the
/// same one the client encodes them with, so neither end can walk a layout the
/// other does not write. Re-exported (not wrapped) so the handlers name them
/// through `ipc::` as before.
///
/// Shape rules beyond the layout — an empty bundle, a duplicate or illegal tid,
/// a count past the per-frame cap — stay with the handlers, so a well-formed but
/// unacceptable frame is rejected there with a specific message.
pub use gnitz_wire::txn_frame::{decode_ddl_txn, decode_push_txn, decode_scan_multi};

/// Like `decode_wire` but skips WAL block checksum verification.  Use for
/// trusted intra-process IPC (W2M ring).
pub fn decode_wire_ipc(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, false)
}

fn decode_wire_impl(data: &[u8], verify_checksum: bool) -> Result<DecodedWire, &'static str> {
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    let control = if verify_checksum {
        peek_control_block(ctrl)?
    } else {
        peek_control_block_ipc(ctrl)?
    };
    let mut decoded = decode_wire_body(data, ctrl.len(), control, None, verify_checksum)?;
    // An engine-authored frame (SAL consumption, W2M, boot replay): its layout
    // claim is real and skipping the re-sort is the point of sending it, so
    // raise the batch off `Raw`. `certify_layout` debug-verifies what it
    // installs, which is why the client path (`decode_wire_with_ctrl`) does not
    // come through here — a lying client frame must be answered with an error,
    // not a debug-build abort.
    let flags = decoded.control.flags;
    if let (Some(b), Some(schema)) = (decoded.data_batch.as_mut(), decoded.schema.as_ref()) {
        b.certify_layout(layout_from_wire_flags(flags), schema);
    }
    Ok(decoded)
}

/// Resolve the schema for a continuation frame (`has_data && !has_schema`).
/// Verifies the server version embedded in `flags` against the hint version.
fn resolve_continuation_schema(
    hint: &Option<SchemaWithVersion<'_>>,
    flags: u64,
) -> Result<SchemaDescriptor, &'static str> {
    match hint.as_ref() {
        None => Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA"),
        Some(h) => {
            let server_version = wire_flags_get_schema_version(flags);
            if server_version != h.version {
                return Err("schema version mismatch on continuation frame");
            }
            Ok(*h.descriptor)
        }
    }
}

/// Resolve a frame's schema and locate its data block — the prologue both
/// decoders run. Returns the schema the data block must be read against, and
/// the data block itself when the frame carries one.
///
/// `verify` threads to exactly one call, `decode_schema_block`; it is the trust
/// level this file already carries as `encode`/`encode_ipc` and
/// `peek_control_block`/`peek_control_block_ipc`.
///
/// When the caller supplies a `hint`, a schema block in the frame is validated
/// against it and the **hint's** descriptor is what comes back. The
/// substitution is not cosmetic: the returned descriptor is what the data
/// block's region sizes are checked against. It is a no-op only because the two
/// are equal by construction — both ends build the pre-flight frame schema from
/// `unique_preflight_wire_schema`. What it adds over that check is a type or
/// nullability difference at equal width; an unequal stride already fails in
/// `decode_mem_batch_inner`.
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
    // to decode it against there is nothing to do but reject it.
    if has_data && !has_schema {
        wire_schema = Some(resolve_continuation_schema(&hint, flags)?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_engine::schema::{SchemaColumn, SchemaDescriptor};
    use gnitz_engine::storage::Layout;
    use gnitz_engine_testkit::{arb_type_code, named_col_defs};
    use gnitz_wire::is_pk_eligible;
    use gnitz_wire::type_code;
    use gnitz_wire::MAX_PK_COLUMNS;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;

    /// Client `encode_ddl_txn` → server `decode_ddl_txn` → `Batch::decode_from_wal_block`
    /// against each family's own schema, for 1-, 2-, 3-, and 5-family bundles.
    /// This is the one place a cross-crate sys-schema drift or a silent misframe
    /// can hide, so it encodes with the *client* schemas and decodes with the
    /// *server* schemas — the two crates hand-keep those identical.
    #[test]
    fn ddl_txn_roundtrip_client_to_server() {
        use gnitz_core::protocol::types::{BatchAppender, ZSetBatch};
        use gnitz_core::types::sys_schema;
        use gnitz_wire::{
            COL_TAB, IDX_TAB, TABLE_TAB, VIEW_TAB, {CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB},
        };

        // Build a small COL_TAB batch for `oid` with `n` U64 columns.
        let col_batch = |oid: u64, kind: u64, n: usize| -> ZSetBatch {
            let s = sys_schema(COL_TAB);
            let mut b = ZSetBatch::new(s);
            {
                let mut a = BatchAppender::new(&mut b, s);
                for i in 0..n {
                    a.add_row(gnitz_wire::pack_col_id(oid, i as u64).unwrap() as u128, 1)
                        .u64_val(oid)
                        .u64_val(kind)
                        .u64_val(i as u64)
                        .str_val(&format!("c{i}"))
                        .u64_val(4) // type_code U64
                        .u64_val(0) // is_nullable
                        .u64_val(0) // fk_table_id
                        .u64_val(0) // fk_col_idx
                        .u64_val(0) // is_serial
                        .u64_val(0); // is_hidden
                }
            }
            b
        };
        let table_batch = |tid: u64, weight: i64| -> ZSetBatch {
            let s = sys_schema(TABLE_TAB);
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row(tid as u128, weight)
                .u64_val(3) // schema_id
                .str_val("t")
                .u64_val(0)
                .u64_val(0);
            b
        };
        let idx_batch = |idx_id: u64, owner: u64| -> ZSetBatch {
            let s = sys_schema(IDX_TAB);
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row(idx_id as u128, 1)
                .u64_val(owner)
                .u64_val(gnitz_wire::pack_pk_cols(&[1]))
                .str_val("idx_t_b")
                .u64_val(1);
            b
        };

        // Verify a bundle roundtrips: family count, order (tid), per-row weight,
        // and — for single-PK families — the PK. `check_pk` skips the compound-PK
        // circuit families whose engine `get_pk` returns the packed narrow key
        // rather than the client's low/high u128 layout.
        let verify = |families: &[(u64, ZSetBatch)], check_pk: &[bool]| {
            let payload = gnitz_core::protocol::encode_ddl_txn(0xABCD, families);
            let decoded = decode_ddl_txn(&payload).expect("decode_ddl_txn");
            assert_eq!(decoded.len(), families.len(), "family count");
            for (fi, ((exp_tid, exp_batch), (got_tid, slice))) in families.iter().zip(&decoded).enumerate() {
                assert_eq!(*got_tid, *exp_tid as u32, "family {fi} tid/order");
                let schema = gnitz_engine::catalog::SysFamily::from_id(*got_tid as i64)
                    .expect("bundle family id must be a system family")
                    .schema();
                let (batch, _) = Batch::decode_from_wal_block(slice, &schema, false).expect("decode family batch");
                assert_eq!(batch.count, exp_batch.len(), "row count tid {got_tid}");
                for i in 0..batch.count {
                    assert_eq!(
                        batch.get_weight(i),
                        exp_batch.weights[i],
                        "weight row {i} tid {got_tid}"
                    );
                    if check_pk[fi] {
                        assert_eq!(batch.get_pk(i), exp_batch.pks.get(i), "pk row {i} tid {got_tid}");
                    }
                }
            }
        };

        // 1-family: DROP TABLE (one TABLE_TAB -1).
        verify(&[(TABLE_TAB, table_batch(16, -1))], &[true]);

        // 2-family: CREATE TABLE (COL_TAB + TABLE_TAB).
        verify(
            &[(COL_TAB, col_batch(17, 0, 2)), (TABLE_TAB, table_batch(17, 1))],
            &[true, true],
        );

        // 3-family: CREATE TABLE + inline UNIQUE index (COL_TAB + TABLE_TAB + IDX_TAB).
        verify(
            &[
                (COL_TAB, col_batch(18, 0, 2)),
                (TABLE_TAB, table_batch(18, 1)),
                (IDX_TAB, idx_batch(100, 18)),
            ],
            &[true, true, true],
        );

        // 5-family: CREATE VIEW (COL + 3 circuit + VIEW). The compound-PK
        // families are built with the client's exact low/high u128 packing.
        let vid: u64 = 20;
        let src: u64 = 16;
        // Compound-PK families: `pk = view_id (low) | sub (high)`, matching the
        // client's low/high packing. `check_pk` is false for these, so the exact
        // sub values are arbitrary — pick non-trivial ones (clippy `identity_op`).
        let nodes = {
            let s = sys_schema(CIRCUIT_NODES_TAB);
            let mut b = ZSetBatch::new(s);
            {
                let mut a = BatchAppender::new(&mut b, s);
                a.add_row((vid as u128) | (1u128 << 64), 1)
                    .u64_val(1)
                    .u64_val(0)
                    .u64_val(src)
                    .null();
                a.add_row((vid as u128) | (2u128 << 64), 1)
                    .u64_val(2)
                    .u64_val(1)
                    .null()
                    .null();
            }
            b
        };
        let edges = {
            let s = sys_schema(CIRCUIT_EDGES_TAB);
            let mut b = ZSetBatch::new(s);
            let sub = (2u128 << 8) | 1u128; // (dst_node, dst_port)
            BatchAppender::new(&mut b, s)
                .add_row((vid as u128) | (sub << 64), 1)
                .u64_val(2)
                .u64_val(1)
                .u64_val(1);
            b
        };
        let node_cols = {
            let s = sys_schema(CIRCUIT_NODE_COLUMNS_TAB);
            let mut b = ZSetBatch::new(s);
            let sub = (1u128 << 24) | (2u128 << 16) | 3u128; // (node_id, kind, position)
            BatchAppender::new(&mut b, s)
                .add_row((vid as u128) | (sub << 64), 1)
                .u64_val(1)
                .u64_val(2)
                .u64_val(3)
                .u64_val(4)
                .u64_val(5);
            b
        };
        let view = {
            let s = sys_schema(VIEW_TAB);
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row(vid as u128, 1)
                .u64_val(3)
                .str_val("v")
                .str_val("")
                .u64_val(0) // pk_col_idx
                .u64_val(0) // capacity_bytes
                .u64_val(0); // delta_bytes
            b
        };
        verify(
            &[
                (COL_TAB, col_batch(vid, 1, 1)),
                (CIRCUIT_NODES_TAB, nodes),
                (CIRCUIT_EDGES_TAB, edges),
                (CIRCUIT_NODE_COLUMNS_TAB, node_cols),
                (VIEW_TAB, view),
            ],
            // Skip pk check on the compound-PK circuit families.
            &[true, false, false, false, true],
        );
    }

    /// `max_pk` bounds the generated PK arity: the engine codec supports up to
    /// `MAX_PK_COLUMNS` (5, the secondary-index schema width), but the persisted
    /// client codec caps at `PK_LIST_MAX_COLS` (4) — tests that decode through the
    /// client (`batch_to_schema` → `Schema::validate_pk_cols`) must stay within it.
    fn arb_schema(max_pk: usize) -> impl Strategy<Value = SchemaDescriptor> {
        // n_cols ≥ 1, so `1..=n_cols.min(max_pk)` is never empty.
        (1usize..=8)
            .prop_flat_map(move |n_cols| {
                (
                    Just(n_cols),
                    vec(arb_type_code(), n_cols), // column types
                    vec(any::<bool>(), n_cols),   // nullability
                    vec(any::<u32>(), n_cols),    // permutation weights
                    1usize..=n_cols.min(max_pk),  // PK arity
                )
            })
            .prop_map(|(n_cols, types, nullables, weights, k)| {
                // PK index set = first `k` columns ordered by their weight.
                // The order is the "declared" PK order the encoder must preserve.
                let mut idx: Vec<u32> = (0..n_cols as u32).collect();
                idx.sort_by_key(|&i| weights[i as usize]);
                let pk_indices: Vec<u32> = idx[..k].to_vec();

                let cols: Vec<SchemaColumn> = (0..n_cols)
                    .map(|i| {
                        let is_pk = pk_indices.contains(&(i as u32));
                        // PK columns must be PK-eligible and non-nullable; remap
                        // ineligible draws to U64 so `new()` accepts the schema.
                        let tc = if is_pk && !is_pk_eligible(types[i]) {
                            type_code::U64
                        } else {
                            types[i]
                        };
                        let nullable = if is_pk { 0 } else { nullables[i] as u8 };
                        SchemaColumn::new(tc, nullable)
                    })
                    .collect();

                SchemaDescriptor::new(&cols, &pk_indices)
            })
    }

    fn assert_descriptor_eq(a: &SchemaDescriptor, b: &SchemaDescriptor) -> Result<(), TestCaseError> {
        prop_assert_eq!(
            a.pk_indices(),
            b.pk_indices(),
            "pk_indices (declared order) changed on round-trip"
        );
        prop_assert_eq!(a.num_columns(), b.num_columns(), "column count changed on round-trip");
        for i in 0..a.num_columns() {
            prop_assert_eq!(
                a.columns[i].type_code,
                b.columns[i].type_code,
                "type_code at col {} changed",
                i
            );
            prop_assert_eq!(
                a.columns[i].nullable,
                b.columns[i].nullable,
                "nullable at col {} changed",
                i
            );
        }
        Ok(())
    }

    /// SchemaDescriptor → owned client Schema, with synthetic `c{i}` names.
    fn descriptor_to_client_schema(sd: &SchemaDescriptor) -> gnitz_core::protocol::types::Schema {
        use gnitz_core::protocol::types::{ColumnDef, Schema, TypeCode};
        let columns = (0..sd.num_columns())
            .map(|i| {
                let col = &sd.columns[i];
                // arb_schema only emits valid codes, so unwrap is total.
                ColumnDef::new(
                    format!("c{i}"),
                    TypeCode::try_from_u8(col.type_code).unwrap(),
                    col.nullable != 0,
                )
            })
            .collect();
        let pk_cols = sd.pk_indices().iter().map(|&i| i as usize).collect();
        Schema { columns, pk_cols }
    }

    proptest! {
        /// Engine encoder → engine decoder.
        #[test]
        fn schema_roundtrip_engine_codec(original in arb_schema(MAX_PK_COLUMNS)) {
            let original = &original;
            let names: Vec<String> = (0..original.num_columns()).map(|i| format!("c{i}")).collect();
            let wire = build_named_schema_wire_block(original, &named_col_defs(&names), 0);
            let decoded = decode_schema_block(&wire, true)
                .expect("decode must succeed for any valid schema");
            assert_descriptor_eq(original, &decoded)?;
        }

        /// Client encoder → client decoder.
        #[test]
        fn schema_roundtrip_client_codec(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
            use gnitz_core::protocol::codec::{encode_schema_block, schema_from_block};

            let client = descriptor_to_client_schema(&original);
            let wire = encode_schema_block(&client, 0);
            prop_assert_eq!(schema_from_block(&wire).unwrap(), client);
        }

        /// Engine encoder → client decoder.
        #[test]
        fn schema_cross_codec_engine_to_client(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
            use gnitz_core::protocol::codec::schema_from_block;

            let original = &original;
            let names: Vec<String> = (0..original.num_columns()).map(|i| format!("c{i}")).collect();
            let wire = build_named_schema_wire_block(original, &named_col_defs(&names), 0);
            let client_schema = schema_from_block(&wire)
                .expect("client failed to decode engine-encoded schema block");

            prop_assert_eq!(client_schema, descriptor_to_client_schema(original));
        }

        /// The two crates' adapters must emit the **same bytes** for the same
        /// schema — the property that makes either side's block decodable by the
        /// other for reasons stronger than "both round-trip".
        #[test]
        fn schema_block_bytes_agree_across_the_two_adapters(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
            use gnitz_core::protocol::codec::encode_schema_block;

            let original = &original;
            let names: Vec<String> = (0..original.num_columns()).map(|i| format!("c{i}")).collect();
            let engine = build_named_schema_wire_block(original, &named_col_defs(&names), 7);
            // `descriptor_to_client_schema` names column `i` `c{i}` — the same
            // names `named_col_defs` gives the engine side above.
            let client = encode_schema_block(&descriptor_to_client_schema(original), 7);
            prop_assert_eq!(engine, client);
        }

        /// Client encoder → engine decoder.
        #[test]
        fn schema_cross_codec_client_to_engine(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
            use gnitz_core::protocol::codec::encode_schema_block;

            let original = &original;
            let wire = encode_schema_block(&descriptor_to_client_schema(original), 0);
            let decoded = decode_schema_block(&wire, true)
                .expect("engine failed to decode client-encoded schema block");
            assert_descriptor_eq(original, &decoded)?;
        }
    }

    /// A wire schema declaring a float PK must be rejected with an error, not
    /// abort the engine. The decoder gates float PKs before they reach
    /// `SchemaDescriptor::new` (whose assert would otherwise abort the process).
    #[test]
    fn decode_schema_block_rejects_float_pk() {
        // `new` itself now rejects float PKs, so the wire block is built from a
        // valid U64 PK and the type_code region (region 3) is patched to F64 —
        // mirroring how the nullable-PK test patches the flags region.
        let cols = [SchemaColumn::new(type_code::U64, 0)];
        let sd = SchemaDescriptor::new(&cols, &[0]);
        let mut wire = build_named_schema_wire_block(&sd, &named_col_defs(&["c0"]), 0);
        let (tc_off, _) = gnitz_wire::wal::dir_entry(&wire, 3);
        wire[tc_off..tc_off + 8].copy_from_slice(&(type_code::F64 as u64).to_le_bytes());
        // verify_checksum=false: the type_code region is inside the checksummed body.
        match decode_schema_block(&wire, false) {
            Err("PK column type not PK-eligible") => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("float PK must be rejected"),
        }
    }

    /// A nullable PK in a wire schema must be rejected with an error rather
    /// than abort the engine inside `SchemaDescriptor::new`. Built by flipping
    /// the NULLABLE flag on a valid non-nullable PK (the engine builder won't
    /// produce a nullable PK directly).
    #[test]
    fn decode_schema_block_rejects_nullable_pk() {
        let cols = [SchemaColumn::new(type_code::U64, 0)];
        let sd = SchemaDescriptor::new(&cols, &[0]);
        let mut wire = build_named_schema_wire_block(&sd, &named_col_defs(&["c0"]), 0);
        // Region 4 is the flags column; OR in NULLABLE on the PK (col 0).
        let (fl_off, _) = gnitz_wire::wal::dir_entry(&wire, 4);
        let f = gnitz_wire::read_u64_le(&wire, fl_off) | gnitz_wire::META_FLAG_NULLABLE;
        wire[fl_off..fl_off + 8].copy_from_slice(&f.to_le_bytes());
        // verify_checksum=false: the flags region is inside the checksummed body.
        match decode_schema_block(&wire, false) {
            Err("PK column must be non-nullable") => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("nullable PK must be rejected"),
        }
    }

    /// A schema header claiming more regions than the buffer can hold must be
    /// rejected before any `dir_entry` indexes past the end.
    #[test]
    fn decode_schema_block_rejects_directory_overflow() {
        let cols = [SchemaColumn::new(type_code::U64, 0)];
        let sd = SchemaDescriptor::new(&cols, &[0]);
        let mut wire = build_named_schema_wire_block(&sd, &named_col_defs(&["c0"]), 0);
        // num_regions lives in the header (outside the checksummed body), so a
        // huge value still passes the checksum and trips the directory guard.
        wire[gnitz_wire::WAL_OFF_NUM_REGIONS..gnitz_wire::WAL_OFF_NUM_REGIONS + 4]
            .copy_from_slice(&100_000u32.to_le_bytes());
        match decode_schema_block(&wire, true) {
            Err("schema block directory overflows buffer") => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("directory overflow must be rejected"),
        }
    }

    /// The shared decoder applies the header's FLAG_BATCH_SORTED /
    /// FLAG_BATCH_CONSOLIDATED bits onto the decoded batch — the precondition the
    /// client trust strip neutralizes. Encode a frame whose data batch is flagged
    /// sorted+consolidated (the encoder mirrors the batch's own fields into the
    /// header), decode it, and confirm both flags arrive set; then apply the
    /// `handle_message` strip and confirm both clear.
    #[test]
    fn decode_applies_batch_flags_then_strip_clears_them() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let mut batch = Batch::with_capacity(schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &42i64.to_le_bytes());
        batch.count += 1;
        batch.certify_layout(Layout::Consolidated, &schema);

        let wire = WireMsg {
            target_id: 7,
            schema: Some(&schema),
            data: WireData::Whole(Some(&batch)),
            ..Default::default()
        }
        .encode_to_vec();

        let mut decoded = decode_wire(&wire).expect("decode");
        {
            let b = decoded.data_batch.as_ref().expect("data batch present");
            assert!(b.is_sorted(), "decoder applies FLAG_BATCH_SORTED");
            assert!(b.is_consolidated(), "decoder applies FLAG_BATCH_CONSOLIDATED");
        }

        // The trust-boundary strip, identical to handle_message.
        let b = decoded.data_batch.as_mut().unwrap();
        b.downgrade();
        assert!(
            !b.is_sorted() && !b.is_consolidated(),
            "strip clears both engine-internal flags"
        );
    }
    fn two_col_schema(col1_nullable: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, col1_nullable),
            ],
            &[0],
        )
    }

    #[test]
    fn validate_schema_match_ok() {
        let sd = two_col_schema(0);
        assert!(validate_schema_match(&sd, &sd).is_ok());
    }

    /// Each mismatch family is rejected, and each names itself distinctly.
    ///
    /// `wire == expected` is the verdict; the message only names the first
    /// differing column, and the executor and reply-train decoder surface it.
    /// Four `is_err()` assertions would still pass if the message collapsed to
    /// one constant string — pairwise distinctness is what tests it, without
    /// pinning the prose.
    #[test]
    fn validate_schema_match_names_each_mismatch_distinctly() {
        let col = |tc, n| SchemaColumn::new(tc, n);
        let expected = two_col_schema(0);
        let cases = [
            (
                "count",
                SchemaDescriptor::new(&[col(type_code::U64, 0)], &[0]),
                expected,
            ),
            (
                "pk",
                SchemaDescriptor::new(&[col(type_code::U64, 0), col(type_code::I64, 0)], &[1]),
                expected,
            ),
            (
                "type",
                SchemaDescriptor::new(&[col(type_code::U64, 0), col(type_code::F64, 0)], &[0]),
                expected,
            ),
            ("nullable", two_col_schema(0), two_col_schema(1)),
        ];
        let msgs: Vec<String> = cases
            .iter()
            .map(|(what, wire, exp)| validate_schema_match(wire, exp).expect_err(what))
            .collect();
        for i in 0..msgs.len() {
            for j in (i + 1)..msgs.len() {
                assert_ne!(
                    msgs[i], msgs[j],
                    "{} vs {} report the same message",
                    cases[i].0, cases[j].0
                );
            }
        }
    }
}
