//! Wire protocol: IPC message codec, schema conversion, encode/decode.

use std::rc::Rc;

use crate::foundation::codec;
use crate::schema::{encode_german_string, type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{wal_block_size, Batch, MemBatch, MAX_WIRE_REGIONS};

// ---------------------------------------------------------------------------
// Constants re-exported from gnitz_wire
// ---------------------------------------------------------------------------

/// W2M-internal flag set on the last (or only) scan chunk from a worker.
/// Not part of the public wire protocol; stripped before reaching clients.
/// The master uses this to detect end-of-train without removing FLAG_CONTINUATION
/// from the TCP frame (FLAG_CONTINUATION must stay set on all worker scan frames
/// so the client's loop termination — "stop on no FLAG_CONTINUATION" — still works).
pub(crate) use gnitz_wire::FLAG_SCAN_LAST;
pub use gnitz_wire::{
    wire_flags_get_conflict_mode, wire_flags_get_index_version, wire_flags_get_schema_version,
    wire_flags_set_index_version, wire_flags_set_schema_version, WireConflictMode, FLAG_BATCH_CONSOLIDATED,
    FLAG_BATCH_SORTED, FLAG_CONTINUATION, FLAG_EXCHANGE, FLAG_GET_INDICES, FLAG_HAS_DATA, FLAG_HAS_SCHEMA,
    META_FLAG_IS_PK, META_FLAG_NULLABLE, STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH,
    STATUS_TXN_CONFLICT,
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

// WAL block header field offsets (matches storage/lsm/wal.rs; duplicated here to
// avoid cross-module coupling between runtime and storage internals).
pub(crate) use gnitz_wire::WAL_OFF_SIZE;
use gnitz_wire::{WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_NUM_REGIONS, WAL_OFF_TID, WAL_OFF_VERSION};

// ---------------------------------------------------------------------------
// Internal schema descriptors for the wire control and schema blocks
// ---------------------------------------------------------------------------

const U64_COL: SchemaColumn = SchemaColumn::new(type_code::U64, 0);
const STR_COL: SchemaColumn = SchemaColumn::new(type_code::STRING, 0);

pub(crate) const META_SCHEMA_DESC: SchemaDescriptor =
    SchemaDescriptor::new(&[U64_COL, U64_COL, U64_COL, STR_COL], &[0]);

const _: () = assert!(
    META_SCHEMA_DESC.num_columns() == 4,
    "META_SCHEMA layout changed; update schema_to_batch and decode_schema_block",
);

// ---------------------------------------------------------------------------
// WAL block sizing (block framing arithmetic is single-homed in `wal::block_size`)
// ---------------------------------------------------------------------------

fn schema_wal_block_size(schema: &SchemaDescriptor, row_count: usize, blob_size: usize) -> usize {
    let pk_stride = schema.pk_stride() as usize;
    let num_payload = schema.num_payload_cols();
    // V4 wire format: 3 fixed regions (pk pk_stride*B, weight 8B, null_bmp 8B) + payload + blob
    let num_regions = 3 + num_payload + 1;
    let mut sizes = [0u32; MAX_WIRE_REGIONS];
    sizes[0] = (pk_stride * row_count) as u32; // pk: pk_stride bytes per row
    sizes[1] = (8 * row_count) as u32; // weight
    sizes[2] = (8 * row_count) as u32; // null_bmp
    for (pi, _ci, col) in schema.payload_columns() {
        sizes[3 + pi] = (col.size() as usize * row_count) as u32;
    }
    sizes[3 + num_payload] = blob_size as u32;
    wal_block_size(&sizes[..num_regions])
}

// ---------------------------------------------------------------------------
// Schema ↔ batch conversion
// ---------------------------------------------------------------------------

/// Encode a schema descriptor + column names into a standalone WAL wire block.
///
/// The returned bytes are a self-contained schema block identical to what
/// `encode_wire_into` would embed. Callers cache this per table and pass it
/// as `prebuilt_schema_block` to `wire_size` / `encode_wire_into` to skip the
/// `Batch` allocation on every SEEK/SCAN response.
pub fn build_schema_wire_block(
    schema: &SchemaDescriptor,
    col_names: &[&[u8]],
    hidden_mask: u128,
    target_tid: u32,
) -> Vec<u8> {
    let schema_batch = schema_to_batch(schema, col_names, hidden_mask);
    let sz = schema_batch.wire_byte_size();
    let mut block = vec![0u8; sz];
    schema_batch.encode_to_wire(target_tid, &mut block, 0, true);
    block
}

/// Convert a slice of owned column-name bytes to a stack-allocated `[&[u8]; MAX_COLUMNS]`,
/// capped at MAX_COLUMNS. Returns the filled array and the fill count.
pub(crate) fn col_names_as_refs(names: &[Vec<u8>]) -> ([&[u8]; crate::schema::MAX_COLUMNS], usize) {
    let mut refs = [&[][..]; crate::schema::MAX_COLUMNS];
    let n = names.len().min(crate::schema::MAX_COLUMNS);
    for (i, name) in names.iter().take(n).enumerate() {
        refs[i] = name;
    }
    (refs, n)
}

/// Get-or-build the cached schema wire block for `tid`, returning the full
/// cache entry (block, version, wire_safe, stride). On a miss the block is
/// built from the catalog's column names + hidden mask and stored; it is
/// invalidated alongside col_names whenever DDL modifies the table. The
/// caller supplies the resolved `schema` — the call sites differ only in
/// what they do when the table has no schema (panic / minimal fallback /
/// one-off block), which stays with them.
pub(crate) fn get_or_build_schema_wire_block(
    cat: &mut crate::catalog::CatalogEngine,
    tid: i64,
    schema: &SchemaDescriptor,
) -> crate::catalog::CachedSchemaWire {
    if let Some(cached) = cat.get_cached_schema_wire_block(tid) {
        return cached;
    }
    let col_names = cat.get_col_names_bytes(tid);
    let hidden = cat.get_col_hidden_mask(tid);
    let (name_refs, n) = col_names_as_refs(&col_names);
    let block = Rc::new(build_schema_wire_block(schema, &name_refs[..n], hidden, tid as u32));
    let (wire_safe, wire_row_fixed_stride) = crate::runtime::sal::compute_wire_props(schema);
    cat.set_schema_wire_block(tid, block.clone(), wire_safe, wire_row_fixed_stride);
    crate::catalog::CachedSchemaWire {
        block,
        version: cat.get_schema_version(tid),
        wire_safe,
        wire_row_fixed_stride,
    }
}

/// `hidden_mask`: bit N set ⇔ column N is a hidden key slot (COL_TAB
/// `is_hidden`), echoed as `META_FLAG_HIDDEN` so clients can suppress the
/// column in presentation. Engine-internal blocks (SAL entries, nameless
/// one-offs) pass 0 — nothing engine-side reads the flag.
pub(crate) fn schema_to_batch(schema: &SchemaDescriptor, col_names: &[&[u8]], hidden_mask: u128) -> Batch {
    let ncols = schema.num_columns();
    let meta = META_SCHEMA_DESC;
    let mut batch = Batch::with_schema(meta, ncols);

    for ci in 0..ncols {
        let col = &schema.columns[ci];
        let mut flags: u64 = 0;
        if col.nullable != 0 {
            flags |= META_FLAG_NULLABLE;
        }
        if hidden_mask & (1 << ci) != 0 {
            flags |= gnitz_wire::META_FLAG_HIDDEN;
        }
        // For compound PKs the position within `schema.pk_indices()` is
        // what determines decode order — column order ≠ PK order in
        // general (e.g. `PRIMARY KEY (b, a)`). Encode the position so
        // the decoder rebuilds `pk_indices` exactly as the user wrote
        // them. Single-PK schemas write position 0, matching the
        // pre-compound wire form (flag bit 1 set, upper bits zero).
        if let Some(pos) = schema.pk_indices().iter().position(|&p| p as usize == ci) {
            flags |= META_FLAG_IS_PK;
            flags |= (pos as u64) << gnitz_wire::META_FLAG_PK_POS_SHIFT;
        }

        let type_code_val = col.type_code as u64;
        let name = if ci < col_names.len() { col_names[ci] } else { b"" };
        let name_st = encode_german_string(name, &mut batch.blob);

        batch.extend_pk(ci as u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &type_code_val.to_le_bytes());
        batch.extend_col(1, &flags.to_le_bytes());
        batch.extend_col(2, &name_st);
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
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut names = Vec::with_capacity(batch.count);
    let mut pk_pairs: [(u8, u32); gnitz_wire::MAX_PK_COLUMNS] = [(0, 0); gnitz_wire::MAX_PK_COLUMNS];
    let mut pk_count: usize = 0;
    for (i, col) in cols.iter_mut().enumerate().take(batch.count) {
        let off8 = i * 8;
        let type_code_val = codec::read_u64_le(batch.col_data(0), off8) as u8;
        let flags_val = codec::read_u64_le(batch.col_data(1), off8);
        let off16 = i * 16;
        let mut st = [0u8; 16];
        st.copy_from_slice(&batch.col_data(2)[off16..off16 + 16]);
        names.push(crate::schema::try_decode_german_string(&st, &batch.blob).unwrap());
        let is_nullable = (flags_val & META_FLAG_NULLABLE) != 0;
        let is_pk = (flags_val & META_FLAG_IS_PK) != 0;
        *col = SchemaColumn::new(type_code_val, if is_nullable { 1 } else { 0 });
        if is_pk {
            if pk_count >= gnitz_wire::MAX_PK_COLUMNS {
                return Err("too many PK columns");
            }
            let pos = ((flags_val & gnitz_wire::META_FLAG_PK_POS_MASK) >> gnitz_wire::META_FLAG_PK_POS_SHIFT) as u8;
            pk_pairs[pk_count] = (pos, i as u32);
            pk_count += 1;
        }
    }
    if pk_count == 0 {
        return Err("no PK column");
    }
    pk_pairs[..pk_count].sort_by_key(|(p, _)| *p);
    let mut pk_indices: [u32; gnitz_wire::MAX_PK_COLUMNS] = [0; gnitz_wire::MAX_PK_COLUMNS];
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
#[allow(clippy::too_many_arguments)]
#[inline]
pub(crate) fn encode_ctrl_block_direct(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    wire_flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    seek_pk_extra: &[u8],
    checksum: bool,
) -> usize {
    let n = gnitz_wire::control::encode_ctrl_block(
        out,
        offset,
        target_id,
        client_id,
        wire_flags,
        seek_pk,
        seek_col_idx,
        request_id,
        status,
        error_msg,
        seek_pk_extra,
    );
    if checksum {
        let cs = crate::foundation::xxh::checksum(&out[offset + gnitz_wire::WAL_HEADER_SIZE..offset + n]);
        out[offset + WAL_OFF_CHECKSUM..offset + WAL_OFF_CHECKSUM + 8].copy_from_slice(&cs.to_le_bytes());
    }
    n
}

/// Blob bytes a German string of length `len` spills into the shared blob
/// region: 0 when it fits the 12-byte inline form, its full length otherwise.
#[inline]
fn german_spill_len(len: usize) -> usize {
    if len > gnitz_wire::SHORT_STRING_THRESHOLD {
        len
    } else {
        0
    }
}

/// Encoded size of the schema wire block for `schema` + `col_names`, or the
/// prebuilt block's length when one is supplied. Shared by `wire_size` and
/// `wire_size_range` so the two size paths cannot drift.
fn schema_block_wire_size(
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    prebuilt_schema_block: Option<&[u8]>,
) -> usize {
    if let Some(prebuilt) = prebuilt_schema_block {
        return prebuilt.len();
    }
    let s = schema.unwrap();
    let ncols = s.num_columns();
    let schema_blob: usize = col_names
        .unwrap_or(&[])
        .iter()
        .take(ncols)
        .map(|n| german_spill_len(n.len()))
        .sum();
    schema_wal_block_size(&META_SCHEMA_DESC, ncols, schema_blob)
}

#[inline]
fn should_include_schema(
    schema: Option<&SchemaDescriptor>,
    prebuilt: Option<&[u8]>,
    has_data: bool,
    status: u32,
) -> bool {
    (schema.is_some() || prebuilt.is_some()) && (has_data || status == STATUS_OK)
}

/// Compute total encoded wire size without allocating.
///
/// `prebuilt_schema_block`: when `Some`, use its length as the schema block
/// size instead of computing it from `schema` / `col_names`. Pass the result
/// of [`build_schema_wire_block`] here to avoid the `schema_wal_block_size`
/// calculation on hot paths.
pub fn wire_size(
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
    prebuilt_schema_block: Option<&[u8]>,
    seek_pk_extra: &[u8],
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = should_include_schema(schema, prebuilt_schema_block, has_data, status);

    let mut total = gnitz_wire::control::ctrl_block_size(error_msg.len(), seek_pk_extra.len());

    if has_schema {
        total += schema_block_wire_size(schema, col_names, prebuilt_schema_block);
    }

    if has_data {
        total += data_batch.unwrap().wire_byte_size();
    }
    total
}

/// Encode a full IPC wire message. Returns heap-allocated Vec<u8>.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_wire(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> Vec<u8> {
    let sz = wire_size(status, error_msg, schema, col_names, data_batch, None, &[]);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf,
        0,
        target_id,
        client_id,
        flags,
        seek_pk,
        seek_col_idx,
        request_id,
        status,
        error_msg,
        schema,
        col_names,
        data_batch,
        None,
        &[],
    );
    buf
}

/// Encode a full IPC wire message into a caller-provided buffer.
/// Returns bytes written. Panics if the buffer is too small.
///
/// `prebuilt_schema_block`: when `Some`, the bytes are copied directly into the
/// schema block slot rather than calling `schema_to_batch` + `encode_to_wire`,
/// eliminating the `Batch` heap allocation on the hot SEEK/SCAN path. The
/// caller must have computed the buffer size using `wire_size` with the same
/// prebuilt slice.
#[allow(clippy::too_many_arguments)]
pub fn encode_wire_into(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
    prebuilt_schema_block: Option<&[u8]>,
    seek_pk_extra: &[u8],
) -> usize {
    encode_wire_into_impl(
        out,
        offset,
        target_id,
        client_id,
        flags,
        seek_pk,
        seek_col_idx,
        request_id,
        status,
        error_msg,
        schema,
        col_names,
        data_batch,
        prebuilt_schema_block,
        seek_pk_extra,
        true,
    )
}

/// Like `encode_wire_into` but skips WAL block checksums.  Use for trusted
/// intra-process IPC (W2M ring) where integrity is guaranteed by the OS
/// shared-memory mapping.
#[allow(clippy::too_many_arguments)]
pub fn encode_wire_into_ipc(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
    prebuilt_schema_block: Option<&[u8]>,
    seek_pk_extra: &[u8],
) -> usize {
    encode_wire_into_impl(
        out,
        offset,
        target_id,
        client_id,
        flags,
        seek_pk,
        seek_col_idx,
        request_id,
        status,
        error_msg,
        schema,
        col_names,
        data_batch,
        prebuilt_schema_block,
        seek_pk_extra,
        false,
    )
}

/// Compute total encoded wire size for `count` rows of `data_batch`.
///
/// Pass `schema`/`prebuilt_schema_block` for the first chunk of a multi-chunk
/// SCAN; pass `None` for both on continuation chunks (no schema block).
/// Only valid for wire-safe schemas (no German-string (STRING or BLOB) columns).
pub fn wire_size_range(
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: &Batch,
    count: usize,
    prebuilt_schema_block: Option<&[u8]>,
) -> usize {
    let has_data = count > 0;
    let has_schema = should_include_schema(schema, prebuilt_schema_block, has_data, status);

    let mut total = gnitz_wire::control::ctrl_block_size(error_msg.len(), 0);

    if has_schema {
        total += schema_block_wire_size(schema, col_names, prebuilt_schema_block);
    }

    if has_data {
        total += data_batch.wire_byte_size_range(count);
    }
    total
}

/// Encode a wire message that carries only rows `[start_row, start_row+count)`
/// from `data_batch`. No checksums (IPC fast path). For the first chunk pass
/// `schema`/`prebuilt_schema_block`; pass `None` for both on continuations.
/// The `sorted`/`consolidated` flags are propagated from `data_batch`.
#[allow(clippy::too_many_arguments)]
pub fn encode_wire_into_range(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    flags: u64,
    request_id: u64,
    status: u32,
    schema: Option<&SchemaDescriptor>,
    data_batch: &Batch,
    start_row: usize,
    count: usize,
    prebuilt_schema_block: Option<&[u8]>,
) -> usize {
    let has_data = count > 0;
    let has_schema = should_include_schema(schema, prebuilt_schema_block, has_data, status);

    let mut wire_flags = flags;
    if has_schema {
        wire_flags |= FLAG_HAS_SCHEMA;
    }
    if has_data {
        wire_flags |= FLAG_HAS_DATA;
        // Maps `b.layout()` with no re-verify: a non-`Raw` tag was certified
        // (debug-verified) at its producer, so the shipped claim is
        // verified-by-construction.
        wire_flags |= layout_to_wire_flags(data_batch.layout());
    }

    let written = encode_ctrl_block_direct(
        out,
        offset,
        target_id,
        client_id,
        wire_flags,
        0u128,
        0,
        request_id,
        status,
        &[],
        &[],
        false,
    );
    let mut pos = offset + written;

    if has_schema {
        if let Some(prebuilt) = prebuilt_schema_block {
            let end = pos + prebuilt.len();
            out[pos..end].copy_from_slice(prebuilt);
            pos = end;
        } else if let Some(s) = schema {
            let schema_batch = schema_to_batch(s, &[], 0);
            let w = schema_batch.encode_to_wire(target_id as u32, out, pos, false);
            pos += w;
        }
    }

    if has_data {
        let w = data_batch.encode_range_to_wire(start_row, count, target_id as u32, out, pos, false);
        pos += w;
    }

    pos - offset
}

#[allow(clippy::too_many_arguments)]
fn encode_wire_into_impl(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
    prebuilt_schema_block: Option<&[u8]>,
    seek_pk_extra: &[u8],
    checksum: bool,
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = should_include_schema(schema, prebuilt_schema_block, has_data, status);

    let mut wire_flags = flags;
    if has_schema {
        wire_flags |= FLAG_HAS_SCHEMA;
    }
    if has_data {
        wire_flags |= FLAG_HAS_DATA;
        // See `encode_data_response_block`: maps `b.layout()`, verified-by-construction.
        wire_flags |= layout_to_wire_flags(data_batch.unwrap().layout());
    }

    let written = encode_ctrl_block_direct(
        out,
        offset,
        target_id,
        client_id,
        wire_flags,
        seek_pk,
        seek_col_idx,
        request_id,
        status,
        error_msg,
        seek_pk_extra,
        checksum,
    );
    let mut pos = offset + written;

    if has_schema {
        if let Some(prebuilt) = prebuilt_schema_block {
            // Fast path: prebuilt bytes already hold the encoded schema block.
            // Copy directly — skips Batch allocation and encode_to_wire overhead.
            let end = pos + prebuilt.len();
            out[pos..end].copy_from_slice(prebuilt);
            pos = end;
        } else {
            let eff_schema = schema.unwrap();
            let names = col_names.unwrap_or(&[]);
            let schema_batch = schema_to_batch(eff_schema, names, 0);
            let written = schema_batch.encode_to_wire(target_id as u32, out, pos, checksum);
            pos += written;
        }
    }

    if has_data {
        let written = data_batch.unwrap().encode_to_wire(target_id as u32, out, pos, checksum);
        pos += written;
    }

    pos - offset
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Decoded control fields + directory-driven control-block decoder — the
/// shared codec both ends run.
pub use gnitz_wire::control::{peek_control_block, DecodedControl};

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

/// Read the (data_offset, data_size) directory entry for region `r` from a
/// WAL block.  Panics on slice errors — callers must validate `data.len()`
/// covers the full directory before calling.
#[inline(always)]
fn wal_dir_entry(data: &[u8], r: usize) -> (usize, usize) {
    const HEADER: usize = gnitz_wire::WAL_HEADER_SIZE;
    const DIR_ENTRY: usize = 8;
    let base = HEADER + r * DIR_ENTRY;
    let off = codec::read_u32_le(data, base) as usize;
    let sz = codec::read_u32_le(data, base + 4) as usize;
    (off, sz)
}

pub(crate) fn decode_schema_block(data: &[u8], verify_checksum: bool) -> Result<SchemaDescriptor, &'static str> {
    // Parse directly from WAL block bytes; no Batch allocation needed.
    // META_SCHEMA_DESC layout: pk(reg 0), weight(1), null_bmp(2),
    //   type_code/U64(3), flags/U64(4), name/STR(5), blob(6).
    const HEADER: usize = gnitz_wire::WAL_HEADER_SIZE;

    if data.len() < HEADER {
        return Err("schema block too small");
    }

    let version = codec::read_u32_le(data, WAL_OFF_VERSION);
    if version != gnitz_wire::WAL_FORMAT_VERSION {
        return Err("schema block wrong version");
    }
    let total_size = codec::read_u32_le(data, WAL_OFF_SIZE) as usize;
    if total_size > data.len() {
        return Err("schema block truncated");
    }

    if verify_checksum && total_size > HEADER {
        let expected = codec::read_u64_le(data, WAL_OFF_CHECKSUM);
        let actual = crate::foundation::xxh::checksum(&data[HEADER..total_size]);
        if actual != expected {
            return Err("schema block checksum mismatch");
        }
    }

    let count = codec::read_u32_le(data, WAL_OFF_COUNT) as usize;
    let num_regions = codec::read_u32_le(data, WAL_OFF_NUM_REGIONS) as usize;

    if count == 0 {
        return Err("empty schema block");
    }
    if count > crate::schema::MAX_COLUMNS {
        return Err("schema exceeds column limit");
    }
    if num_regions < 5 {
        return Err("schema block region count mismatch");
    }

    // The directory table is `num_regions` × 8 bytes immediately after the
    // header; every `wal_dir_entry` call below indexes into it. Reject any
    // frame too short to hold it before reading a single entry.
    if HEADER.saturating_add(num_regions.saturating_mul(8)) > data.len() {
        return Err("schema block directory overflows buffer");
    }

    // Region 0 is the PK (col_idx). Validate that col_idx values are
    // exactly [0, 1, ..., count-1] — every malformed-schema test relies
    // on this ordering, and downstream consumers index columns by the
    // physical row position, so an out-of-order/gap/duplicate col_idx
    // would silently re-route columns to the wrong type. The col_idx is
    // an unsigned U64 PK column stored OPK (big-endian) at rest, so decode
    // it big-endian to recover the native index.
    let (pk_off, pk_sz) = wal_dir_entry(data, 0);
    if pk_sz < count * 8 || pk_off.saturating_add(pk_sz) > data.len() {
        return Err("schema col_idx region OOB");
    }
    let pk_data = &data[pk_off..pk_off + count * 8];
    for i in 0..count {
        let v = u64::from_be_bytes(pk_data[i * 8..(i + 1) * 8].try_into().unwrap());
        if v != i as u64 {
            return Err("schema col_idx not in monotonic order");
        }
    }

    let (tc_off, tc_sz) = wal_dir_entry(data, 3);
    let (fl_off, fl_sz) = wal_dir_entry(data, 4);

    if tc_sz < count * 8 || tc_off.saturating_add(tc_sz) > data.len() {
        return Err("schema type_code region OOB");
    }
    if fl_sz < count * 8 || fl_off.saturating_add(fl_sz) > data.len() {
        return Err("schema flags region OOB");
    }

    let type_data = &data[tc_off..tc_off + count * 8];
    let flags_data = &data[fl_off..fl_off + count * 8];

    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    // Each entry pairs the PK column's logical index with its 0-indexed
    // position in the PK tuple (carried in the column's flags word).
    // Sorted by position before building the SchemaDescriptor.
    let mut pk_pairs: [(u8, u32); gnitz_wire::MAX_PK_COLUMNS] = [(0, 0); gnitz_wire::MAX_PK_COLUMNS];
    let mut pk_count: usize = 0;

    for (i, col) in cols[..count].iter_mut().enumerate() {
        let off8 = i * 8;
        let tc = codec::read_u64_le(type_data, off8) as u8;
        let fl = codec::read_u64_le(flags_data, off8);
        // Reject unknown type codes here so a crafted wire schema cannot
        // smuggle in a type_code the downstream cursors can't decode.
        if gnitz_wire::TypeCode::try_from_u8(tc).is_none() {
            return Err("schema: invalid type code");
        }
        let is_nullable = (fl & META_FLAG_NULLABLE) != 0;
        let is_pk = (fl & META_FLAG_IS_PK) != 0;
        *col = SchemaColumn::new(tc, if is_nullable { 1 } else { 0 });
        if is_pk {
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
            if pk_count >= gnitz_wire::MAX_PK_COLUMNS {
                return Err("too many PK columns");
            }
            let pos = ((fl & gnitz_wire::META_FLAG_PK_POS_MASK) >> gnitz_wire::META_FLAG_PK_POS_SHIFT) as u8;
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
    let mut pk_indices: [u32; gnitz_wire::MAX_PK_COLUMNS] = [0; gnitz_wire::MAX_PK_COLUMNS];
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
    let ctrl = wal_block_slice_at(data, 0)?;
    peek_control_block(ctrl)
}

/// Client-boundary decode with a pre-parsed control block (the `handle_message`
/// single-parse path). Full checksum verification on the schema/data blocks —
/// the control block itself is never checksummed by design; its integrity is
/// covered by the frame length + directory bounds checks in the peek.
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

/// Decode a `FLAG_DDL_TXN` frame into its per-family `(table_id, wal-block
/// slice)` list, in send order. Walks the concatenated family blocks by header
/// alone — `table_id` at `WAL_OFF_TID`, total size at `WAL_OFF_SIZE` — so no
/// schema is needed here; the caller resolves each family's schema from the
/// catalog and calls `Batch::decode_from_wal_block` on its slice. The frame is:
/// control block, then `u32` family count, then `count` data blocks. The control
/// block is validated (version, region count) but not returned — the caller
/// already has the routing header from `handle_message`'s peek.
/// Read the length-prefixed WAL block at `off` in `data`: confirm the header is
/// present, read the block's total size (`WAL_OFF_SIZE`), confirm the block fits,
/// and return its slice. Every framed decoder walks concatenated blocks this way
/// — the schema block, the data block, each DDL_TXN family — so the bounds check
/// lives in one place; callers advance `off` by the returned slice's `len()`. The
/// header-present check precedes the size read, so the 4-byte field access is
/// always in range.
fn wal_block_slice_at(data: &[u8], off: usize) -> Result<&[u8], &'static str> {
    if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
        return Err("WAL block header truncated");
    }
    let size = codec::read_u32_le(data, off + WAL_OFF_SIZE) as usize;
    if size < gnitz_wire::WAL_HEADER_SIZE || off + size > data.len() {
        return Err("WAL block truncated");
    }
    Ok(&data[off..off + size])
}

/// Walk a transaction frame's shared prologue — control block, then the `u32`
/// family count — returning `(count, offset of the first family, capacity hint)`.
/// The hint bounds `count` by what the remaining bytes can physically hold
/// (`min_family_bytes` is the least a single family can encode to), so a hostile
/// count cannot force a giant pre-allocation on an ingress-capped frame.
fn txn_frame_prologue(data: &[u8], min_family_bytes: usize) -> Result<(usize, usize, usize), &'static str> {
    let ctrl = wal_block_slice_at(data, 0)?;
    peek_control_block(ctrl)?;
    let off = ctrl.len();
    if off + 4 > data.len() {
        return Err("TXN family count truncated");
    }
    let count = codec::read_u32_le(data, off) as usize;
    let off = off + 4;
    let max_families = data.len().saturating_sub(off) / min_family_bytes + 1;
    Ok((count, off, count.min(max_families)))
}

pub fn decode_ddl_txn(data: &[u8]) -> Result<Vec<(i64, &[u8])>, &'static str> {
    let (count, mut off, cap) = txn_frame_prologue(data, gnitz_wire::WAL_HEADER_SIZE)?;
    let mut families = Vec::with_capacity(cap);
    for _ in 0..count {
        let block = wal_block_slice_at(data, off)?;
        let tid = codec::read_u32_le(block, WAL_OFF_TID) as i64;
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
        let schema_block = wal_block_slice_at(data, off)?;
        off += schema_block.len();
        let wal_block = wal_block_slice_at(data, off)?;
        let tid = codec::read_u32_le(wal_block, WAL_OFF_TID) as i64;
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
    let pre_count = codec::read_u32_le(data, off) as usize;
    off += 4;
    // Bound the count by the bytes physically remaining (16 per precondition) so
    // a hostile count cannot force a giant pre-allocation, and the reads below
    // stay in bounds.
    if pre_count > data.len().saturating_sub(off) / 16 {
        return Err("PUSH_TXN precondition section truncated");
    }
    let mut preconditions = Vec::with_capacity(pre_count);
    for _ in 0..pre_count {
        let tid = codec::read_u64_le(data, off) as i64;
        let basis = codec::read_u64_le(data, off + 8);
        off += 16;
        preconditions.push((tid, basis));
    }
    Ok((families, preconditions))
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
    let ctrl = wal_block_slice_at(data, 0)?;
    let control = peek_control_block(ctrl)?;
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
        let sblock = wal_block_slice_at(data, off)?;
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
        let dblock = wal_block_slice_at(data, off)?;
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
        let sblock = wal_block_slice_at(data, off)?;
        wire_schema = Some(decode_schema_block(sblock, false)?);
        off += sblock.len();
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        let dblock = wal_block_slice_at(data, off)?;
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
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Layout;
    use crate::test_support::arb_type_code;
    use gnitz_wire::{is_pk_eligible, MAX_PK_COLUMNS};
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;

    /// Client `encode_ddl_txn` → server `decode_ddl_txn` → `Batch::decode_from_wal_block`
    /// against the server `sys_tab_schema`, for 1-, 2-, 3-, and 6-family bundles.
    /// This is the one place a cross-crate sys-schema drift or a silent misframe
    /// can hide, so it encodes with the *client* schemas and decodes with the
    /// *server* schemas — the two crates hand-keep those identical.
    #[test]
    fn ddl_txn_roundtrip_client_to_server() {
        use gnitz_core::protocol::types::{BatchAppender, Schema, ZSetBatch};
        use gnitz_core::types::{
            circuit_edges_schema, circuit_node_columns_schema, circuit_nodes_schema, col_tab_schema, dep_tab_schema,
            idx_tab_schema, table_tab_schema, view_tab_schema,
        };
        use gnitz_wire::{
            COL_TAB, DEP_TAB, IDX_TAB, TABLE_TAB, VIEW_TAB,
            {CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB},
        };

        // Build a small COL_TAB batch for `oid` with `n` U64 columns.
        let col_batch = |oid: u64, kind: u64, n: usize| -> ZSetBatch {
            let s = col_tab_schema();
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
            let s = table_tab_schema();
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row(tid as u128, weight)
                .u64_val(3) // schema_id
                .str_val("t")
                .str_val("")
                .u64_val(0)
                .u64_val(0)
                .u64_val(0);
            b
        };
        let idx_batch = |idx_id: u64, owner: u64| -> ZSetBatch {
            let s = idx_tab_schema();
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row(idx_id as u128, 1)
                .u64_val(owner)
                .u64_val(0)
                .u64_val(gnitz_wire::pack_pk_cols(&[1]))
                .str_val("idx_t_b")
                .u64_val(1)
                .str_val("");
            b
        };

        // Verify a bundle roundtrips: family count, order (tid), per-row weight,
        // and — for single-PK families — the PK. `check_pk` skips the compound-PK
        // circuit/dep families whose engine `get_pk` returns the packed narrow key
        // rather than the client's low/high u128 layout.
        let verify = |families: &[(u64, &'static Schema, ZSetBatch)], check_pk: &[bool]| {
            let payload = gnitz_core::protocol::encode_ddl_txn(0xABCD, families);
            let decoded = decode_ddl_txn(&payload).expect("decode_ddl_txn");
            assert_eq!(decoded.len(), families.len(), "family count");
            for (fi, ((exp_tid, _s, exp_batch), (got_tid, slice))) in families.iter().zip(&decoded).enumerate() {
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
        verify(&[(TABLE_TAB, table_tab_schema(), table_batch(16, -1))], &[true]);

        // 2-family: CREATE TABLE (COL_TAB + TABLE_TAB).
        verify(
            &[
                (COL_TAB, col_tab_schema(), col_batch(17, 0, 2)),
                (TABLE_TAB, table_tab_schema(), table_batch(17, 1)),
            ],
            &[true, true],
        );

        // 3-family: CREATE TABLE + inline UNIQUE index (COL_TAB + TABLE_TAB + IDX_TAB).
        verify(
            &[
                (COL_TAB, col_tab_schema(), col_batch(18, 0, 2)),
                (TABLE_TAB, table_tab_schema(), table_batch(18, 1)),
                (IDX_TAB, idx_tab_schema(), idx_batch(100, 18)),
            ],
            &[true, true, true],
        );

        // 6-family: CREATE VIEW (COL + DEP + 3 circuit + VIEW). The compound-PK
        // families are built with the client's exact low/high u128 packing.
        let vid: u64 = 20;
        let src: u64 = 16;
        let dep = {
            let s = dep_tab_schema();
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row((vid as u128) | ((src as u128) << 64), 1)
                .u64_val(0);
            b
        };
        // Compound-PK families: `pk = view_id (low) | sub (high)`, matching the
        // client's low/high packing. `check_pk` is false for these, so the exact
        // sub values are arbitrary — pick non-trivial ones (clippy `identity_op`).
        let nodes = {
            let s = circuit_nodes_schema();
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
            let s = circuit_edges_schema();
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
            let s = circuit_node_columns_schema();
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
            let s = view_tab_schema();
            let mut b = ZSetBatch::new(s);
            BatchAppender::new(&mut b, s)
                .add_row(vid as u128, 1)
                .u64_val(3)
                .str_val("v")
                .str_val("")
                .str_val("")
                .u64_val(0)
                .u64_val(0);
            b
        };
        verify(
            &[
                (COL_TAB, col_tab_schema(), col_batch(vid, 1, 1)),
                (DEP_TAB, dep_tab_schema(), dep),
                (CIRCUIT_NODES_TAB, circuit_nodes_schema(), nodes),
                (CIRCUIT_EDGES_TAB, circuit_edges_schema(), edges),
                (CIRCUIT_NODE_COLUMNS_TAB, circuit_node_columns_schema(), node_cols),
                (VIEW_TAB, view_tab_schema(), view),
            ],
            // Skip pk check on the compound-PK DEP/circuit families.
            &[true, false, false, false, false, true],
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
            let names: Vec<Vec<u8>> = (0..original.num_columns())
                .map(|i| format!("c{i}").into_bytes())
                .collect();
            let (refs, n) = col_names_as_refs(&names);
            let wire = build_schema_wire_block(original, &refs[..n], 0, 0);
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
            let names: Vec<Vec<u8>> = (0..original.num_columns())
                .map(|i| format!("c{i}").into_bytes())
                .collect();
            let (refs, n) = col_names_as_refs(&names);
            let wire = build_schema_wire_block(original, &refs[..n], 0, 0);

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
        let mut wire = build_schema_wire_block(&sd, &[b"c0".as_slice()], 0, 0);
        let (tc_off, _) = wal_dir_entry(&wire, 3);
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
        let mut wire = build_schema_wire_block(&sd, &[b"c0".as_slice()], 0, 0);
        // Region 4 is the flags column; OR in NULLABLE on the PK (col 0).
        let (fl_off, _) = wal_dir_entry(&wire, 4);
        let f = codec::read_u64_le(&wire, fl_off) | META_FLAG_NULLABLE;
        wire[fl_off..fl_off + 8].copy_from_slice(&f.to_le_bytes());
        // verify_checksum=false: the flags region is inside the checksummed body.
        match decode_schema_block(&wire, false) {
            Err("PK column must be non-nullable") => {}
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("nullable PK must be rejected"),
        }
    }

    /// A schema header claiming more regions than the buffer can hold must be
    /// rejected before any `wal_dir_entry` indexes past the end.
    #[test]
    fn decode_schema_block_rejects_directory_overflow() {
        let cols = [SchemaColumn::new(type_code::U64, 0)];
        let sd = SchemaDescriptor::new(&cols, &[0]);
        let mut wire = build_schema_wire_block(&sd, &[b"c0".as_slice()], 0, 0);
        // num_regions lives in the header (outside the checksummed body), so a
        // huge value still passes the checksum and trips the directory guard.
        wire[WAL_OFF_NUM_REGIONS..WAL_OFF_NUM_REGIONS + 4].copy_from_slice(&100_000u32.to_le_bytes());
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
        let mut batch = Batch::with_schema(schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &42i64.to_le_bytes());
        batch.count += 1;
        batch.certify_layout(Layout::Consolidated, &schema);

        let col_names = [b"pk".as_slice(), b"v".as_slice()];
        let wire = encode_wire(
            7,
            0,
            0,
            0,
            0,
            0,
            STATUS_OK,
            b"",
            Some(&schema),
            Some(&col_names),
            Some(&batch),
        );

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

    /// Byte-level cross-crate consistency check: the engine wrapper
    /// (`encode_ctrl_block_direct`, checksum stamped via `foundation::xxh`)
    /// and the client wrapper
    /// (`gnitz_core::protocol::message::encode_control_block`, checksum
    /// stamped via `xxhash-rust`) must produce identical bytes for the same
    /// logical message — pinning that the two wrappers over the shared
    /// `gnitz_wire::control` codec (and their two XXH3 implementations)
    /// cannot drift.
    #[test]
    fn test_ctrl_block_client_server_byte_equality() {
        use gnitz_core::protocol::header::Header;
        use gnitz_core::protocol::message::encode_control_block;

        #[allow(clippy::type_complexity)]
        let cases: &[(u32, u64, u64, u64, u128, u64, u64, &str)] = &[
            // status, target_id, client_id, flags, seek_pk, seek_col_idx, request_id, error_msg
            (0, 0, 0, 0, 0, 0, 0, ""),
            (
                1,
                0xDEAD_BEEF_1234_5678,
                0xCAFE_BABE_0000_0001,
                0xFFFF,
                42u128 | (99u128 << 64),
                7,
                0xCAFE_BABE_DEAD_F00D,
                "",
            ),
            (2, 1, 2, 3, u128::MAX, u64::MAX, u64::MAX, "something went wrong"),
            (
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                "a longer error message that exceeds the German string inline threshold of twelve bytes",
            ),
        ];

        for &(status, target_id, client_id, flags, seek_pk, seek_col_idx, request_id, error_msg) in cases {
            let header = Header {
                status,
                target_id,
                client_id,
                flags,
                seek_pk,
                seek_col_idx,
                request_id,
            };
            let client_bytes = encode_control_block(&header, error_msg, &[]);

            let mut engine_buf = vec![0u8; client_bytes.len() + 64];
            let written = encode_ctrl_block_direct(
                &mut engine_buf,
                0,
                target_id,
                client_id,
                flags,
                seek_pk,
                seek_col_idx,
                request_id,
                status,
                error_msg.as_bytes(),
                b"",
                true, // checksum, matching encode_wal_block's unconditional checksum
            );

            assert_eq!(
                &client_bytes[..],
                &engine_buf[..written],
                "ctrl block byte mismatch for error_msg={error_msg:?}",
            );
        }
    }
}
