//! Wire protocol: IPC message codec, schema conversion, encode/decode.

use std::rc::Rc;

use crate::catalog::ColumnDef;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, MemBatch, MAX_WIRE_REGIONS};
use gnitz_wire::encode_german_string;

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
    FLAG_RESOLVE, STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH, STATUS_TXN_CONFLICT,
};

/// Map a batch's layout claim to its wire flag bits. Encode normalizes
/// `Consolidated ⇒ both bits`; `layout_from_wire_flags` inverts losslessly. The
/// pair folds every bit↔enum conversion into one place so the encode and decode
/// sides can never drift.
pub(crate) fn layout_to_wire_flags(layout: crate::storage::Layout) -> u64 {
    use crate::storage::Layout;
    match layout {
        Layout::Raw => 0,
        Layout::Sorted => FLAG_BATCH_SORTED,
        Layout::Consolidated => FLAG_BATCH_SORTED | FLAG_BATCH_CONSOLIDATED,
    }
}

/// Recover a batch layout claim from wire flag bits. The constructor already
/// defaults `Raw`, so this is the value fed to `certify_layout` at the decode
/// boundary (which debug-verifies the data against the claim).
pub(crate) fn layout_from_wire_flags(flags: u64) -> crate::storage::Layout {
    use crate::storage::Layout;
    if flags & FLAG_BATCH_CONSOLIDATED != 0 {
        Layout::Consolidated
    } else if flags & FLAG_BATCH_SORTED != 0 {
        Layout::Sorted
    } else {
        Layout::Raw
    }
}

use gnitz_wire::WAL_OFF_TID;

// ---------------------------------------------------------------------------
// Internal schema descriptors for the wire control and schema blocks
// ---------------------------------------------------------------------------

/// The schema block's columns, derived from the shared `gnitz-wire` definition
/// the client's `meta_schema()` also builds from — the block travels between the
/// two, so its shape is stated once, there, not once per side.
const META_SCHEMA_COLUMNS: [SchemaColumn; gnitz_wire::META_SCHEMA_COLS.len()] = {
    let mut out = [SchemaColumn::EMPTY; gnitz_wire::META_SCHEMA_COLS.len()];
    let mut i = 0;
    while i < out.len() {
        let c = &gnitz_wire::META_SCHEMA_COLS[i];
        out[i] = SchemaColumn::new(c.type_code as u8, c.nullable as u8);
        i += 1;
    }
    out
};

pub(crate) const META_SCHEMA_DESC: SchemaDescriptor =
    SchemaDescriptor::new(&META_SCHEMA_COLUMNS, gnitz_wire::META_SCHEMA_PK);

// ---------------------------------------------------------------------------
// Schema ↔ batch conversion
// ---------------------------------------------------------------------------

/// Encode a schema descriptor into a standalone WAL wire block carrying only
/// the physical column shape — no names, no catalog flags. This is what SAL
/// entries and one-off reply blocks ship: nothing engine-side reads a name, and
/// the client decodes such a block against a schema it already holds.
///
/// The returned bytes are a self-contained schema block identical to what
/// `encode_wire_into` would embed. Callers cache this per table and pass it
/// as `prebuilt_schema_block` to `wire_size` / `encode_wire_into` to skip the
/// `Batch` allocation on every SEEK/SCAN response.
pub fn build_schema_wire_block(schema: &SchemaDescriptor, target_tid: u32) -> Vec<u8> {
    encode_schema_block(&schema_to_batch(schema, None), target_tid)
}

/// [`build_schema_wire_block`] plus the per-column catalog facts the descriptor
/// does not carry — name, `is_hidden`, `is_serial`. The block a *client* decodes
/// into a `Schema`, so it is the one that must be named.
pub(crate) fn build_named_schema_wire_block(schema: &SchemaDescriptor, defs: &[ColumnDef], target_tid: u32) -> Vec<u8> {
    encode_schema_block(&schema_to_batch(schema, Some(defs)), target_tid)
}

fn encode_schema_block(schema_batch: &Batch, target_tid: u32) -> Vec<u8> {
    let mut block = vec![0u8; schema_batch.wire_byte_size()];
    schema_batch.encode_to_wire(target_tid, &mut block, 0, true);
    block
}

/// Get-or-build the cached schema wire block for `tid`, returning the full
/// cache entry (block, version, wire_safe, stride). On a miss the block is
/// built from the catalog's column defs and stored; it is invalidated
/// alongside them whenever DDL modifies the table. The caller supplies the
/// resolved `schema` — the call sites differ only in what they do when the
/// table has no schema (panic / minimal fallback / one-off block), which stays
/// with them.
pub(crate) fn get_or_build_schema_wire_block(
    cat: &mut crate::catalog::CatalogEngine,
    tid: i64,
    schema: &SchemaDescriptor,
) -> crate::catalog::SchemaWireEntry {
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
    let (wire_safe, wire_row_fixed_stride) = crate::storage::compute_wire_props(schema);
    let entry = crate::catalog::SchemaWireEntry {
        block,
        version: cat.get_schema_version(tid),
        wire_safe,
        wire_row_fixed_stride,
    };
    cat.set_schema_wire_block(tid, entry.clone());
    entry
}

/// One row per column: the physical shape from `schema`, and the per-column
/// catalog facts (name, `META_FLAG_HIDDEN`, `META_FLAG_SERIAL`) from `defs`.
///
/// `defs` is all-or-nothing, not per-column: a relation either has one COL_TAB
/// row per physical column — so `defs[ci]` describes `schema.columns[ci]` — or
/// it has none, and every name comes out empty with every catalog flag clear.
pub(crate) fn schema_to_batch(schema: &SchemaDescriptor, defs: Option<&[ColumnDef]>) -> Batch {
    let ncols = schema.num_columns();
    debug_assert!(defs.is_none_or(|d| d.len() == ncols));
    let mut batch = Batch::with_capacity(META_SCHEMA_DESC, ncols);

    for ci in 0..ncols {
        let col = &schema.columns[ci];
        // For compound PKs the position within `schema.pk_indices()` is
        // what determines decode order — column order ≠ PK order in
        // general (e.g. `PRIMARY KEY (b, a)`). Carrying the position lets
        // the decoder rebuild `pk_indices` exactly as the user wrote them.
        let pk_pos = schema
            .pk_indices()
            .iter()
            .position(|&p| p as usize == ci)
            .map(|p| p as u8);
        let def = defs.map(|d| &d[ci]);
        let flags = gnitz_wire::pack_col_meta_flags(
            col.nullable != 0,
            def.is_some_and(|d| d.is_hidden),
            def.is_some_and(|d| d.is_serial),
            pk_pos,
        );
        let name_st = encode_german_string(def.map_or(&b""[..], |d| d.name.as_bytes()), &mut batch.blob);

        batch.extend_pk(ci as u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(
            gnitz_wire::METASCHEMA_PAY_TYPE_CODE,
            &(col.type_code as u64).to_le_bytes(),
        );
        batch.extend_col(gnitz_wire::METASCHEMA_PAY_FLAGS, &flags.to_le_bytes());
        batch.extend_col(gnitz_wire::METASCHEMA_PAY_NAME, &name_st);
        batch.count += 1;
    }
    batch
}

#[cfg(test)]
pub(crate) fn batch_to_schema(batch: &Batch) -> Result<(SchemaDescriptor, Vec<Vec<u8>>), &'static str> {
    if batch.count == 0 {
        return Err("empty schema batch");
    }
    if batch.count > crate::schema::MAX_COLUMNS {
        return Err("schema exceeds column limit");
    }
    let mut cols = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
    let mut names = Vec::with_capacity(batch.count);
    let mut pk_pairs: [(u8, u32); crate::schema::MAX_PK_COLUMNS] = [(0, 0); crate::schema::MAX_PK_COLUMNS];
    let mut pk_count: usize = 0;
    for (i, col) in cols.iter_mut().enumerate().take(batch.count) {
        let off8 = i * 8;
        let type_code_val = gnitz_wire::read_u64_le(batch.col_data(0), off8) as u8;
        let flags_val = gnitz_wire::read_u64_le(batch.col_data(1), off8);
        let off16 = i * 16;
        let mut st = [0u8; 16];
        st.copy_from_slice(&batch.col_data(2)[off16..off16 + 16]);
        names.push(gnitz_wire::try_decode_german_string(&st, &batch.blob).unwrap());
        let is_nullable = gnitz_wire::col_meta_nullable(flags_val);
        *col = SchemaColumn::new(type_code_val, if is_nullable { 1 } else { 0 });
        if let Some(pos) = gnitz_wire::col_meta_pk_pos(flags_val) {
            if pk_count >= crate::schema::MAX_PK_COLUMNS {
                return Err("too many PK columns");
            }
            pk_pairs[pk_count] = (pos, i as u32);
            pk_count += 1;
        }
    }
    if pk_count == 0 {
        return Err("no PK column");
    }
    pk_pairs[..pk_count].sort_by_key(|(p, _)| *p);
    let mut pk_indices: [u32; crate::schema::MAX_PK_COLUMNS] = [0; crate::schema::MAX_PK_COLUMNS];
    for (k, (_, ci)) in pk_pairs[..pk_count].iter().enumerate() {
        pk_indices[k] = *ci;
    }
    let sd = SchemaDescriptor::new(&cols[..batch.count], &pk_indices[..pk_count]);
    Ok((sd, names))
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

pub(crate) const CTRL_BLOCK_SIZE_NO_BLOB: usize = gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;

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
/// length when one is supplied. Shared by `wire_size` and `wire_size_range` so
/// the two size paths cannot drift.
///
/// The no-prebuilt arm allows no blob bytes: a [`WireMsg`] encodes its block
/// through [`schema_to_batch`] with no defs, so every name is empty and spills nothing.
/// A named block always arrives prebuilt.
fn schema_block_wire_size(schema: Option<&SchemaDescriptor>, prebuilt_schema_block: Option<&[u8]>) -> usize {
    if let Some(prebuilt) = prebuilt_schema_block {
        return prebuilt.len();
    }
    crate::storage::wire_block_size(&META_SCHEMA_DESC, schema.unwrap().num_columns(), 0)
}

// ---------------------------------------------------------------------------
// WireMsg
// ---------------------------------------------------------------------------

/// The data payload of one wire message: a whole (optional) batch, or a row
/// range of one — the only axis on which the two encode shapes differ.
#[derive(Clone, Copy)]
pub enum WireData<'a> {
    Whole(Option<&'a Batch>),
    Range {
        batch: &'a Batch,
        start_row: usize,
        count: usize,
    },
}

impl Default for WireData<'_> {
    fn default() -> Self {
        WireData::Whole(None)
    }
}

impl<'a> WireData<'a> {
    fn row_count(&self) -> usize {
        match *self {
            WireData::Whole(b) => b.map(|b| b.count).unwrap_or(0),
            WireData::Range { count, .. } => count,
        }
    }

    fn layout_batch(&self) -> Option<&'a Batch> {
        match *self {
            WireData::Whole(b) => b,
            WireData::Range { batch, .. } => Some(batch),
        }
    }

    fn wire_byte_size(&self) -> usize {
        match *self {
            WireData::Whole(b) => b.map(|b| b.wire_byte_size()).unwrap_or(0),
            WireData::Range { batch, count, .. } => batch.wire_byte_size_range(count),
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
                let schema_batch = schema_to_batch(self.schema.unwrap(), None);
                pos += schema_batch.encode_to_wire(self.target_id as u32, out, pos, checksum);
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
            };
        }

        pos - offset
    }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

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

// META_SCHEMA_DESC payload regions, after the fixed pk (= col_idx) / weight /
// null_bmp trio. Only the two U64 columns are read here; the name is carried but
// unused. The payload slots come from the shared definition the encoder uses.
const REG_TYPE_CODE: usize = gnitz_wire::REG_PAYLOAD_START + gnitz_wire::METASCHEMA_PAY_TYPE_CODE;
const REG_FLAGS: usize = gnitz_wire::REG_PAYLOAD_START + gnitz_wire::METASCHEMA_PAY_FLAGS;

pub(crate) fn decode_schema_block(data: &[u8], verify_checksum: bool) -> Result<SchemaDescriptor, &'static str> {
    // Parse directly from WAL block bytes; no Batch allocation needed.
    //
    // Format validity — header size, version, `total_size` in-bounds, body
    // checksum, region count ≤ cap, and every region's extent within the block —
    // is the shared framer's job, the same front door the batch decoders use.
    // This decoder adds only the schema-conformance checks on top; the parsed
    // directory lands in `offs`/`sizes` with every extent already bounded.
    let mut offs = [0u64; MAX_WIRE_REGIONS];
    let mut sizes = [0u32; MAX_WIRE_REGIONS];
    let header = gnitz_wire::wal::validate_and_parse(data, &mut offs, &mut sizes, verify_checksum).map_err(|e| {
        match e {
            gnitz_wire::WalError::InvalidVersion => "schema block wrong version",
            gnitz_wire::WalError::ChecksumMismatch => "schema block checksum mismatch",
            // Region count exceeds the directory cap — a forged over-long header.
            gnitz_wire::WalError::InvalidShard => "schema block directory overflows buffer",
            // Truncated / BufferTooSmall: shorter than its header, directory, or
            // a declared region requires.
            _ => "schema block truncated",
        }
    })?;

    let count = header.entry_count as usize;
    let num_regions = header.num_regions as usize;

    if count == 0 {
        return Err("empty schema block");
    }
    if count > crate::schema::MAX_COLUMNS {
        return Err("schema exceeds column limit");
    }
    if num_regions <= REG_FLAGS {
        return Err("schema block region count mismatch");
    }

    // Region 0 is the PK (col_idx). Validate that col_idx values are
    // exactly [0, 1, ..., count-1] — every malformed-schema test relies
    // on this ordering, and downstream consumers index columns by the
    // physical row position, so an out-of-order/gap/duplicate col_idx
    // would silently re-route columns to the wrong type. The col_idx is
    // an unsigned U64 PK column stored OPK (big-endian) at rest, so decode
    // it big-endian to recover the native index. `validate_and_parse` already
    // bounded each region's extent to the block, so only the schema-level
    // "exactly `count` columns" check remains. Exact, not a lower bound: a forged
    // smaller COUNT passes the col_idx monotonicity check on the truncated prefix
    // `[0, 1, …]` and yields a descriptor that silently dropped columns.
    let (pk_off, pk_sz) = (offs[gnitz_wire::REG_PK] as usize, sizes[gnitz_wire::REG_PK] as usize);
    if pk_sz != count * 8 {
        return Err("schema col_idx region OOB");
    }
    let pk_data = &data[pk_off..pk_off + count * 8];
    for i in 0..count {
        let v = u64::from_be_bytes(pk_data[i * 8..(i + 1) * 8].try_into().unwrap());
        if v != i as u64 {
            return Err("schema col_idx not in monotonic order");
        }
    }

    let (tc_off, tc_sz) = (offs[REG_TYPE_CODE] as usize, sizes[REG_TYPE_CODE] as usize);
    let (fl_off, fl_sz) = (offs[REG_FLAGS] as usize, sizes[REG_FLAGS] as usize);

    if tc_sz != count * 8 {
        return Err("schema type_code region OOB");
    }
    if fl_sz != count * 8 {
        return Err("schema flags region OOB");
    }

    let type_data = &data[tc_off..tc_off + count * 8];
    let flags_data = &data[fl_off..fl_off + count * 8];

    let mut cols = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
    // Each entry pairs the PK column's logical index with its 0-indexed
    // position in the PK tuple (carried in the column's flags word).
    // Sorted by position before building the SchemaDescriptor.
    let mut pk_pairs: [(u8, u32); crate::schema::MAX_PK_COLUMNS] = [(0, 0); crate::schema::MAX_PK_COLUMNS];
    let mut pk_count: usize = 0;

    for (i, col) in cols[..count].iter_mut().enumerate() {
        let off8 = i * 8;
        let tc = gnitz_wire::read_u64_le(type_data, off8) as u8;
        let fl = gnitz_wire::read_u64_le(flags_data, off8);
        // Reject unknown type codes here so a crafted wire schema cannot
        // smuggle in a type_code the downstream cursors can't decode.
        if !gnitz_wire::is_valid_type_code(tc) {
            return Err("schema: invalid type code");
        }
        let is_nullable = gnitz_wire::col_meta_nullable(fl);
        *col = SchemaColumn::new(tc, if is_nullable { 1 } else { 0 });
        if let Some(pos) = gnitz_wire::col_meta_pk_pos(fl) {
            // Reject malformed PK columns here rather than letting them reach
            // `SchemaDescriptor::new`, whose `assert!`s would abort the engine
            // process on a nullable/STRING/BLOB PK and which silently accepts
            // float PKs that `is_pk_eligible` (and the client decoder) reject.
            if is_nullable {
                return Err("PK column must be non-nullable");
            }
            if !gnitz_wire::is_pk_eligible(tc) {
                return Err("PK column type not PK-eligible");
            }
            if pk_count >= crate::schema::MAX_PK_COLUMNS {
                return Err("too many PK columns");
            }
            pk_pairs[pk_count] = (pos, i as u32);
            pk_count += 1;
        }
    }
    if pk_count == 0 {
        return Err("no PK column");
    }
    // Sort by position; single-PK schemas all carry position 0 so this
    // is a no-op for the common path.
    pk_pairs[..pk_count].sort_by_key(|(p, _)| *p);
    let mut pk_indices: [u32; crate::schema::MAX_PK_COLUMNS] = [0; crate::schema::MAX_PK_COLUMNS];
    for (k, (_, ci)) in pk_pairs[..pk_count].iter().enumerate() {
        pk_indices[k] = *ci;
    }
    Ok(SchemaDescriptor::new(&cols[..count], &pk_indices[..pk_count]))
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
    decode_wire_impl(data, None, true)
}

/// Walk a transaction frame's shared prologue — control block, then the `u32`
/// family count — returning `(count, offset of the first family, capacity hint)`.
/// The hint bounds `count` by what the remaining bytes can physically hold
/// (`min_family_bytes` is the least a single family can encode to), so a hostile
/// count cannot force a giant pre-allocation on an ingress-capped frame.
fn txn_frame_prologue(data: &[u8], min_family_bytes: usize) -> Result<(usize, usize, usize), &'static str> {
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    peek_control_block(ctrl)?;
    let off = ctrl.len();
    if off + 4 > data.len() {
        return Err("TXN family count truncated");
    }
    let count = gnitz_wire::read_u32_le(data, off) as usize;
    let off = off + 4;
    let max_families = data.len().saturating_sub(off) / min_family_bytes + 1;
    Ok((count, off, count.min(max_families)))
}

/// Decode a `FLAG_DDL_TXN` frame into its per-family `(table_id, wal-block
/// slice)` list, in send order. Walks the concatenated family blocks by header
/// alone — `table_id` at `WAL_OFF_TID`, total size at `WAL_OFF_SIZE` — so no
/// schema is needed here; the caller resolves each family's schema from the
/// catalog and calls `Batch::decode_from_wal_block` on its slice. The frame is:
/// control block, then `u32` family count, then `count` data blocks. The control
/// block is validated (version, region count) but not returned — the caller
/// already has the routing header from `handle_message`'s peek.
pub fn decode_ddl_txn(data: &[u8]) -> Result<Vec<(i64, &[u8])>, &'static str> {
    let (count, mut off, cap) = txn_frame_prologue(data, gnitz_wire::WAL_HEADER_SIZE)?;
    let mut families = Vec::with_capacity(cap);
    for _ in 0..count {
        let block = gnitz_wire::wal::block_slice_at(data, off)?;
        let tid = gnitz_wire::read_u32_le(block, WAL_OFF_TID) as i64;
        families.push((tid, block));
        off += block.len();
    }
    Ok(families)
}

/// One decoded `FLAG_PUSH_TXN` family: the target `tid` (read from the data
/// block's `WAL_OFF_TID`), the conflict `mode` byte, and the borrowed schema and
/// data WAL-block slices — same lifetime discipline as `decode_ddl_txn`'s
/// `(i64, &[u8])`. The master validates the schema block against its catalog and
/// decodes the data block via `Batch::decode_from_wal_block`.
pub struct TxnFamilyWire<'a> {
    pub tid: i64,
    pub mode: u8,
    pub schema_block: &'a [u8],
    pub wal_block: &'a [u8],
}

/// The two lists a `FLAG_PUSH_TXN` frame decodes to: its per-family write bundle
/// and its OCC preconditions (each `(tid, basis_lsn)`). Named to keep
/// `decode_push_txn`'s signature legible (and clear of `clippy::type_complexity`).
pub type DecodedPushTxn<'a> = (Vec<TxnFamilyWire<'a>>, Vec<(i64, u64)>);

/// Decode a `FLAG_PUSH_TXN` frame into its per-family list plus its OCC
/// precondition list, in send order — the user-table analogue of
/// `decode_ddl_txn`. The frame is: control block, `u32` family count, then per
/// family a `u8` conflict mode, a self-sized meta-schema WAL block, and a
/// self-sized data WAL block; then — appended after the last family — a `u32`
/// precondition count and that many `[u64 tid][u64 basis_lsn]` pairs (all LE).
/// The precondition section is always present (a zero count still encodes its
/// 4-byte length). Truncation at any field rejects the whole frame. The section
/// is appended *after* the families (not before `family_count`) so the shared
/// `txn_frame_prologue` and `decode_ddl_txn` are untouched.
pub fn decode_push_txn(data: &[u8]) -> Result<DecodedPushTxn<'_>, &'static str> {
    // A family is at least a mode byte plus two WAL headers (schema + data).
    let (count, mut off, cap) = txn_frame_prologue(data, 1 + 2 * gnitz_wire::WAL_HEADER_SIZE)?;
    let mut families = Vec::with_capacity(cap);
    for _ in 0..count {
        if off + 1 > data.len() {
            return Err("PUSH_TXN family mode truncated");
        }
        let mode = data[off];
        off += 1;
        let schema_block = gnitz_wire::wal::block_slice_at(data, off)?;
        off += schema_block.len();
        let wal_block = gnitz_wire::wal::block_slice_at(data, off)?;
        let tid = gnitz_wire::read_u32_le(wal_block, WAL_OFF_TID) as i64;
        off += wal_block.len();
        families.push(TxnFamilyWire {
            tid,
            mode,
            schema_block,
            wal_block,
        });
    }
    // Precondition section: `u32` count, then count × `[u64 tid][u64 basis]`.
    if off + 4 > data.len() {
        return Err("PUSH_TXN precondition count truncated");
    }
    let pre_count = gnitz_wire::read_u32_le(data, off) as usize;
    off += 4;
    // Bound the count by the bytes physically remaining (16 per precondition) so
    // a hostile count cannot force a giant pre-allocation, and the reads below
    // stay in bounds.
    if pre_count > data.len().saturating_sub(off) / 16 {
        return Err("PUSH_TXN precondition section truncated");
    }
    let mut preconditions = Vec::with_capacity(pre_count);
    for _ in 0..pre_count {
        let tid = gnitz_wire::read_u64_le(data, off) as i64;
        let basis = gnitz_wire::read_u64_le(data, off + 8);
        off += 16;
        preconditions.push((tid, basis));
    }
    Ok((families, preconditions))
}

/// Decode a `FLAG_SCAN_MULTI` frame into its per-relation `(tid,
/// client_schema_version)` list, in request order. The frame is: control block,
/// then a `u32` relation count, then per relation a `u64` tid and a `u16` cached
/// schema version (all LE). The control block is validated (version, region
/// count) but not returned — `handle_message`'s peek already holds the routing
/// header. Truncation at any field rejects the whole frame; the count/duplicate/
/// tid-legality shape rules are the handler's (`handle_scan_multi`), so a
/// well-formed but empty or over-cap list decodes cleanly and is rejected there
/// with a specific message.
///
/// Shares the control-block + `u32` count
/// prologue with `decode_ddl_txn`/`decode_push_txn` via `txn_frame_prologue`; the
/// explicit 10-byte-record bound below then rejects a hostile count before
/// allocating (the per-record reads would otherwise panic on a short slice).
pub fn decode_scan_multi(data: &[u8]) -> Result<Vec<(u64, u16)>, &'static str> {
    // A relation record is exactly 10 bytes (u64 tid + u16 version).
    const RECORD_BYTES: usize = 10;
    let (count, mut off, _cap) = txn_frame_prologue(data, RECORD_BYTES)?;
    if count > data.len().saturating_sub(off) / RECORD_BYTES {
        return Err("SCAN_MULTI relation section truncated");
    }
    let mut relations = Vec::with_capacity(count);
    for _ in 0..count {
        let tid = gnitz_wire::read_u64_le(data, off);
        let version = gnitz_wire::read_u16_le(data, off + 8);
        off += RECORD_BYTES;
        relations.push((tid, version));
    }
    Ok(relations)
}

/// Like `decode_wire` but skips WAL block checksum verification.  Use for
/// trusted intra-process IPC (W2M ring).
pub fn decode_wire_ipc(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, None, false)
}

fn decode_wire_impl(
    data: &[u8],
    schema_hint: Option<SchemaWithVersion<'_>>,
    verify_checksum: bool,
) -> Result<DecodedWire, &'static str> {
    let ctrl = gnitz_wire::wal::block_slice_at(data, 0)?;
    let control = if verify_checksum {
        peek_control_block(ctrl)?
    } else {
        peek_control_block_ipc(ctrl)?
    };
    decode_wire_body(data, ctrl.len(), control, schema_hint, verify_checksum)
}

/// Like `decode_wire_ipc` but supplies a versioned schema hint for W2M
/// continuation frames that carry `FLAG_HAS_DATA` without `FLAG_HAS_SCHEMA`.
/// Callers must supply the `server_version` that matches what the sender
/// embedded in `wire_flags` bits 24-39; a version mismatch is a hard error.
///
/// Currently only used by tests; the production warmup path now decodes
/// continuation frames via `decode_wire_ipc_zero_copy_with_ctrl` to avoid
/// the owned `Batch` allocation.
#[cfg(test)]
pub(crate) fn decode_wire_ipc_with_schema<'a>(
    data: &[u8],
    hint: SchemaWithVersion<'a>,
) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, Some(hint), false)
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

fn decode_wire_body(
    data: &[u8],
    ctrl_size: usize,
    control: DecodedControl,
    schema_hint: Option<SchemaWithVersion<'_>>,
    verify_checksum: bool,
) -> Result<DecodedWire, &'static str> {
    let flags = control.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    if has_data && !has_schema {
        // External client traffic must include the schema block unless the
        // caller supplies an explicit catalog hint (warm-cache PUSH path).
        if verify_checksum && schema_hint.is_none() {
            return Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA");
        }
        wire_schema = Some(resolve_continuation_schema(&schema_hint, flags)?);
    }

    if has_schema {
        let sblock = gnitz_wire::wal::block_slice_at(data, off)?;
        let parsed = decode_schema_block(sblock, verify_checksum)?;
        // Integrity cross-check against hint even when versions match.
        if let Some(ref hint) = schema_hint {
            if crate::schema::validate_schema_match(&parsed, hint.descriptor).is_err() {
                return Err("schema mismatch: client schema differs from server schema");
            }
            wire_schema = Some(*hint.descriptor);
        } else {
            wire_schema = Some(parsed);
        }
        off += sblock.len();
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        let dblock = gnitz_wire::wal::block_slice_at(data, off)?;
        let (mut batch, _) = Batch::decode_from_wal_block(dblock, eff_schema, verify_checksum)?;
        // The constructor defaults `Raw`; raise to the wire's claim, debug-verifying
        // the decoded data against it (the backstop against a lying frame).
        batch.certify_layout(layout_from_wire_flags(flags), eff_schema);
        Some(batch)
    } else {
        None
    };

    Ok(DecodedWire {
        control,
        schema: wire_schema,
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
/// triggering a redundant parse.
pub(crate) fn decode_wire_ipc_zero_copy_with_ctrl<'a>(
    data: &'a [u8],
    control: DecodedControl,
    schema_hint: Option<SchemaWithVersion<'_>>,
) -> Result<DecodedWireZeroCopy<'a>, &'static str> {
    let ctrl_size = control.block_size;
    let flags = control.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    if has_data && !has_schema {
        wire_schema = Some(resolve_continuation_schema(&schema_hint, flags)?);
    }

    if has_schema {
        let sblock = gnitz_wire::wal::block_slice_at(data, off)?;
        wire_schema = Some(decode_schema_block(sblock, false)?);
        off += sblock.len();
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        let dblock = gnitz_wire::wal::block_slice_at(data, off)?;
        let mb = crate::storage::decode_mem_batch_from_wal_block(dblock, eff_schema)?;
        Some(mb)
    } else {
        None
    };

    Ok(DecodedWireZeroCopy {
        control,
        schema: wire_schema,
        data_batch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MAX_PK_COLUMNS;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Layout;
    use crate::test_support::{arb_type_code, named_col_defs};
    use gnitz_wire::is_pk_eligible;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;

    /// Client `encode_ddl_txn` → server `decode_ddl_txn` → `Batch::decode_from_wal_block`
    /// against the server `sys_tab_schema`, for 1-, 2-, 3-, and 5-family bundles.
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
                assert_eq!(*got_tid, *exp_tid as i64, "family {fi} tid/order");
                let schema = crate::catalog::sys_tab_schema(*got_tid);
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
                    .bytes_null();
                a.add_row((vid as u128) | (2u128 << 64), 1)
                    .u64_val(2)
                    .u64_val(1)
                    .u64_null()
                    .bytes_null();
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
                .u64_val(0);
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
            use gnitz_core::protocol::codec::{schema_to_batch, batch_to_schema};
            use gnitz_core::protocol::types::meta_schema;
            use gnitz_core::protocol::wal_block::{
                decode_wal_block_verified, encode_wal_block,
            };

            let original = &original;
            let client = descriptor_to_client_schema(original);
            let ms = meta_schema();
            let batch = schema_to_batch(&client);
            let encoded = encode_wal_block(ms, 0, &batch);
            let (decoded_batch, _) =
                decode_wal_block_verified(&encoded, ms).unwrap();
            let reconstructed = batch_to_schema(&decoded_batch).unwrap();
            prop_assert_eq!(client, reconstructed);
        }

        /// Engine encoder → client decoder.
        #[test]
        fn schema_cross_codec_engine_to_client(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
            use gnitz_core::protocol::codec::batch_to_schema;
            use gnitz_core::protocol::types::meta_schema;
            use gnitz_core::protocol::wal_block::decode_wal_block_verified;

            let original = &original;
            let names: Vec<String> = (0..original.num_columns()).map(|i| format!("c{i}")).collect();
            let wire = build_named_schema_wire_block(original, &named_col_defs(&names), 0);

            let ms = meta_schema();
            let (decoded_batch, _) =
                decode_wal_block_verified(&wire, ms)
                    .expect("client failed to decode engine-encoded WAL block");
            let client_schema = batch_to_schema(&decoded_batch)
                .expect("client failed to parse meta batch");

            prop_assert_eq!(client_schema, descriptor_to_client_schema(original));
        }

        /// Client encoder → engine decoder.
        #[test]
        fn schema_cross_codec_client_to_engine(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
            use gnitz_core::protocol::codec::schema_to_batch;
            use gnitz_core::protocol::types::meta_schema;
            use gnitz_core::protocol::wal_block::encode_wal_block;

            let original = &original;
            let client = descriptor_to_client_schema(original);
            let ms = meta_schema();
            let batch = schema_to_batch(&client);
            let wire = encode_wal_block(ms, 0, &batch);

            let decoded = decode_schema_block(&wire, true)
                .expect("engine failed to decode client-encoded WAL block");
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
        b.set_layout_unchecked(Layout::Raw);
        assert!(
            !b.is_sorted() && !b.is_consolidated(),
            "strip clears both engine-internal flags"
        );
    }
}
