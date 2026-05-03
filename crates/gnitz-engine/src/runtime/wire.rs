//! Wire protocol: IPC message codec, schema conversion, encode/decode.

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, type_size, encode_german_string, decode_german_string};
use crate::storage::{Batch, MemBatch};
use crate::util::align8;

// ---------------------------------------------------------------------------
// Constants re-exported from gnitz_wire
// ---------------------------------------------------------------------------

pub use gnitz_wire::{
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_EXCHANGE, FLAG_CONTINUATION, IPC_CONTROL_TID,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK,
    FLAG_CONFLICT_MODE_PRESENT as FLAG_CONFLICT_MODE_PRESENT_U64,
    WireConflictMode,
};

/// Engine-side u32 alias for the cross-crate conflict-mode marker bit.
pub const FLAG_CONFLICT_MODE_PRESENT: u32 = FLAG_CONFLICT_MODE_PRESENT_U64 as u32;

#[inline]
pub fn decode_conflict_mode(flags: u64, seek_col_idx: u64) -> WireConflictMode {
    if flags & FLAG_CONFLICT_MODE_PRESENT_U64 != 0 {
        WireConflictMode::from_u8((seek_col_idx & 0xFF) as u8)
    } else {
        WireConflictMode::Update
    }
}

pub const FLAG_BATCH_SORTED: u64 = 1 << 50;
pub const FLAG_BATCH_CONSOLIDATED: u64 = 1 << 51;

// WAL block header field offsets (matches storage/wal.rs; duplicated here to
// avoid cross-module coupling between runtime and storage internals).
const WAL_OFF_COUNT:       usize = 12;
pub(crate) const WAL_OFF_SIZE: usize = 16;
const WAL_OFF_VERSION:     usize = 20;
const WAL_OFF_CHECKSUM:    usize = 24;
const WAL_OFF_NUM_REGIONS: usize = 32;

// ---------------------------------------------------------------------------
// Internal schema descriptors for the wire control and schema blocks
// ---------------------------------------------------------------------------

const ZERO_COL: SchemaColumn = SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 };
const U64_COL: SchemaColumn = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
const U128_COL: SchemaColumn = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
const STR_COL: SchemaColumn = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
const STR_COL_NULL: SchemaColumn = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0 };

pub(crate) const META_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor { num_columns: 4, pk_index: 0, columns: [ZERO_COL; crate::schema::MAX_COLUMNS] };
    sd.columns[0] = U64_COL;
    sd.columns[1] = U64_COL;
    sd.columns[2] = U64_COL;
    sd.columns[3] = STR_COL;
    sd
};

const _: () = assert!(
    META_SCHEMA_DESC.num_columns == 4,
    "META_SCHEMA layout changed; update schema_to_batch and decode_schema_block",
);

// col 0: msg_idx (PK, U64); col 1: status; col 2: client_id; col 3: target_id;
// col 4: flags; col 5: seek_pk (U128); col 6: seek_col_idx; col 7: request_id;
// col 8: error_msg (String, nullable)
pub(crate) const CONTROL_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor {
        num_columns: gnitz_wire::control::NUM_COLUMNS as u32,
        pk_index: 0,
        columns: [ZERO_COL; crate::schema::MAX_COLUMNS],
    };
    sd.columns[0] = U64_COL; sd.columns[1] = U64_COL; sd.columns[2] = U64_COL;
    sd.columns[3] = U64_COL; sd.columns[4] = U64_COL; sd.columns[5] = U128_COL;
    sd.columns[6] = U64_COL; sd.columns[7] = U64_COL; sd.columns[8] = STR_COL_NULL;
    sd
};

// ---------------------------------------------------------------------------
// Private WAL-sizing helpers (replaces wal::block_size / schema_wal_block_size)
// ---------------------------------------------------------------------------

fn wal_block_size(num_regions: usize, region_sizes: &[u32]) -> usize {
    let mut pos = gnitz_wire::WAL_HEADER_SIZE + num_regions * 8;
    for &sz in region_sizes.iter().take(num_regions) {
        pos = align8(pos);
        pos += sz as usize;
    }
    pos
}

fn schema_wal_block_size(schema: &SchemaDescriptor, row_count: usize, blob_size: usize) -> usize {
    let pk_idx = schema.pk_index as usize;
    let pk_stride = schema.columns[pk_idx].size as usize;
    let num_payload = schema.num_columns as usize - 1;
    // V4 wire format: 3 fixed regions (pk pk_stride*B, weight 8B, null_bmp 8B) + payload + blob
    let num_regions = 3 + num_payload + 1;
    let mut sizes = [0u32; 128];
    sizes[0] = (pk_stride * row_count) as u32; // pk: pk_stride bytes per row
    sizes[1] = (8 * row_count) as u32; // weight
    sizes[2] = (8 * row_count) as u32; // null_bmp
    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_idx { continue; }
        sizes[3 + pi] = (schema.columns[ci].size as usize * row_count) as u32;
        pi += 1;
    }
    sizes[3 + pi] = blob_size as u32;
    wal_block_size(num_regions, &sizes[..num_regions])
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
    target_tid: u32,
) -> Vec<u8> {
    let schema_batch = schema_to_batch(schema, col_names);
    let sz = schema_batch.wire_byte_size(target_tid);
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

pub(crate) fn schema_to_batch(schema: &SchemaDescriptor, col_names: &[&[u8]]) -> Batch {
    let ncols = schema.num_columns as usize;
    let meta = META_SCHEMA_DESC;
    let mut batch = Batch::with_schema(meta, ncols);

    for ci in 0..ncols {
        let col = &schema.columns[ci];
        let mut flags: u64 = 0;
        if col.nullable != 0 { flags |= META_FLAG_NULLABLE; }
        if ci == schema.pk_index as usize { flags |= META_FLAG_IS_PK; }

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
    if batch.count == 0 { return Err("empty schema batch"); }
    if batch.count > crate::schema::MAX_COLUMNS { return Err("schema exceeds column limit"); }
    let mut sd = SchemaDescriptor {
        num_columns: batch.count as u32,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; crate::schema::MAX_COLUMNS],
    };
    let mut names = Vec::with_capacity(batch.count);
    let mut pk_found = false;
    for i in 0..batch.count {
        let off8 = i * 8;
        let type_code_val = u64::from_le_bytes(
            batch.col_data(0)[off8..off8 + 8].try_into().unwrap()
        ) as u8;
        let flags_val = u64::from_le_bytes(
            batch.col_data(1)[off8..off8 + 8].try_into().unwrap()
        );
        let off16 = i * 16;
        let mut st = [0u8; 16];
        st.copy_from_slice(&batch.col_data(2)[off16..off16 + 16]);
        names.push(decode_german_string(&st, &batch.blob));
        let is_nullable = (flags_val & META_FLAG_NULLABLE) != 0;
        let is_pk = (flags_val & META_FLAG_IS_PK) != 0;
        let col_size = type_size(type_code_val);
        sd.columns[i] = SchemaColumn {
            type_code: type_code_val,
            size: col_size,
            nullable: if is_nullable { 1 } else { 0 },
            _pad: 0,
        };
        if is_pk {
            if pk_found { return Err("multiple PK columns"); }
            sd.pk_index = i as u32;
            pk_found = true;
        }
    }
    if !pk_found { return Err("no PK column"); }
    Ok((sd, names))
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Encode only the ctrl WAL block (no schema, no data) into `out[offset..]`
/// with checksums skipped. Caller pre-computes `wire_flags` (including
/// `FLAG_HAS_SCHEMA`, `FLAG_HAS_DATA`, sorted/consolidated bits, etc.) so
/// this helper can be called after the data block is already written.
/// Returns bytes written.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_ctrl_block_ipc(
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
    checksum: bool,
) -> usize {
    use gnitz_wire::control as ctrl;
    let cs = CONTROL_SCHEMA_DESC;
    let mut b = Batch::with_schema(cs, 1);
    let has_error = !error_msg.is_empty();
    let null_word: u64 = if has_error { 0 } else { ctrl::NULL_BIT_ERROR_MSG };
    b.extend_pk(0u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&null_word.to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_STATUS,       &(status as u64).to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_CLIENT_ID,    &client_id.to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_TARGET_ID,    &target_id.to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_FLAGS,        &wire_flags.to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_SEEK_PK,      &seek_pk.to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_SEEK_COL_IDX, &seek_col_idx.to_le_bytes());
    b.extend_col(ctrl::PAYLOAD_REQUEST_ID,   &request_id.to_le_bytes());
    let error_st = if has_error {
        encode_german_string(error_msg, &mut b.blob)
    } else {
        [0u8; 16]
    };
    b.extend_col(ctrl::PAYLOAD_ERROR_MSG, &error_st);
    b.count = 1;
    b.encode_to_wire(IPC_CONTROL_TID, out, offset, checksum)
}

// ---------------------------------------------------------------------------
// Direct ctrl-block encoder (no Batch allocation on the no-error fast path)
// ---------------------------------------------------------------------------
//
// Walks `CONTROL_SCHEMA_DESC` once to derive each region's offset within the
// ctrl WAL block. Mirrors `wal::encode`'s phase-1 directory walk: directory
// immediately follows the WAL header, each region is `align8`-padded before
// its data. The const-time assertion enforces that every region size is a
// multiple of 8 (so the implicit `align8` is a no-op); a future schema change
// that introduces an unaligned column fails at compile time.
const fn ctrl_region_offset(target_region: usize) -> usize {
    use gnitz_wire::control::NUM_REGIONS;
    let schema = &CONTROL_SCHEMA_DESC;
    let pk_idx = schema.pk_index as usize;

    let mut sizes = [0usize; NUM_REGIONS];
    sizes[0] = schema.columns[pk_idx].size as usize;          // pk
    sizes[1] = 8;                                             // weight
    sizes[2] = 8;                                             // null_bmp
    let mut pi = 0usize;
    let mut ci = 0usize;
    while ci < schema.num_columns as usize {
        if ci == pk_idx { ci += 1; continue; }
        sizes[3 + pi] = schema.columns[ci].size as usize;
        pi += 1;
        ci += 1;
    }
    sizes[NUM_REGIONS - 1] = 0;                               // blob (no-blob path)

    let mut pos = gnitz_wire::WAL_HEADER_SIZE + NUM_REGIONS * 8;
    let mut r = 0;
    while r < target_region {
        assert!(sizes[r] % 8 == 0,
            "ctrl_region_offset assumes every region size is 8-aligned");
        pos += sizes[r];
        r += 1;
    }
    pos
}

pub(crate) const CTRL_BLOCK_SIZE_NO_BLOB: usize =
    ctrl_region_offset(gnitz_wire::control::NUM_REGIONS);

const OFF_STATUS:       usize = ctrl_region_offset(gnitz_wire::control::REGION_STATUS);
const OFF_CLIENT_ID:    usize = ctrl_region_offset(gnitz_wire::control::REGION_CLIENT_ID);
const OFF_TARGET_ID:    usize = ctrl_region_offset(gnitz_wire::control::REGION_TARGET_ID);
const OFF_FLAGS:        usize = ctrl_region_offset(gnitz_wire::control::REGION_FLAGS);
const OFF_SEEK_PK:      usize = ctrl_region_offset(gnitz_wire::control::REGION_SEEK_PK);
const OFF_SEEK_COL_IDX: usize = ctrl_region_offset(gnitz_wire::control::REGION_SEEK_COL_IDX);
const OFF_REQUEST_ID:   usize = ctrl_region_offset(gnitz_wire::control::REGION_REQUEST_ID);

static CTRL_BLOCK_TEMPLATE: std::sync::LazyLock<[u8; CTRL_BLOCK_SIZE_NO_BLOB]> =
    std::sync::LazyLock::new(|| {
        let mut arr = [0u8; CTRL_BLOCK_SIZE_NO_BLOB];
        let n = encode_ctrl_block_ipc(&mut arr, 0, 0, 0, 0, 0, 0, 0, 0, b"", false);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB,
            "ctrl block template size mismatch");
        arr
    });

/// Direct ctrl-block encoder. On the no-error fast path
/// (`error_msg.is_empty()`), copies the pre-encoded template and patches the 7
/// variable fields, avoiding the `Batch` pool allocation and `encode_to_wire`
/// overhead. Falls back to `encode_ctrl_block_ipc` when an error message is
/// present (the blob region is variable-size, so the template approach does
/// not apply).
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
    checksum: bool,
) -> usize {
    if !error_msg.is_empty() {
        return encode_ctrl_block_ipc(
            out, offset, target_id, client_id, wire_flags,
            seek_pk, seek_col_idx, request_id, status, error_msg, checksum,
        );
    }
    let buf = &mut out[offset..offset + CTRL_BLOCK_SIZE_NO_BLOB];
    buf.copy_from_slice(&*CTRL_BLOCK_TEMPLATE);
    buf[OFF_STATUS..OFF_STATUS + 8].copy_from_slice(&(status as u64).to_le_bytes());
    buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 8].copy_from_slice(&client_id.to_le_bytes());
    buf[OFF_TARGET_ID..OFF_TARGET_ID + 8].copy_from_slice(&target_id.to_le_bytes());
    buf[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&wire_flags.to_le_bytes());
    buf[OFF_SEEK_PK..OFF_SEEK_PK + 16].copy_from_slice(&seek_pk.to_le_bytes());
    buf[OFF_SEEK_COL_IDX..OFF_SEEK_COL_IDX + 8].copy_from_slice(&seek_col_idx.to_le_bytes());
    buf[OFF_REQUEST_ID..OFF_REQUEST_ID + 8].copy_from_slice(&request_id.to_le_bytes());
    if checksum {
        let cs = crate::xxh::checksum(&buf[gnitz_wire::WAL_HEADER_SIZE..]);
        buf[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8].copy_from_slice(&cs.to_le_bytes());
    }
    CTRL_BLOCK_SIZE_NO_BLOB
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
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = has_data
        || (schema.is_some() && status == STATUS_OK)
        || (prebuilt_schema_block.is_some() && status == STATUS_OK);

    let ctrl_blob = if error_msg.len() > gnitz_wire::SHORT_STRING_THRESHOLD {
        error_msg.len()
    } else { 0 };
    let mut total = if ctrl_blob == 0 {
        CTRL_BLOCK_SIZE_NO_BLOB
    } else {
        schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, ctrl_blob)
    };

    if has_schema {
        if let Some(prebuilt) = prebuilt_schema_block {
            total += prebuilt.len();
        } else {
            let s = schema.unwrap_or_else(|| data_batch.unwrap().schema.as_ref().unwrap());
            let names = col_names.unwrap_or(&[]);
            let ncols = s.num_columns as usize;
            let schema_blob: usize = names.iter().take(ncols)
                .map(|n| if n.len() > gnitz_wire::SHORT_STRING_THRESHOLD { n.len() } else { 0 })
                .sum();
            total += schema_wal_block_size(&META_SCHEMA_DESC, ncols, schema_blob);
        }
    }

    if has_data {
        total += data_batch.unwrap().wire_byte_size(0);
    }
    total
}

/// Encode a full IPC wire message. Returns heap-allocated Vec<u8>.
#[cfg(test)]
pub fn encode_wire(
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
    let sz = wire_size(status, error_msg, schema, col_names, data_batch, None);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf, 0, target_id, client_id, flags,
        seek_pk, seek_col_idx, request_id,
        status, error_msg, schema, col_names, data_batch, None,
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
) -> usize {
    encode_wire_into_impl(
        out, offset, target_id, client_id, flags, seek_pk, seek_col_idx,
        request_id, status, error_msg, schema, col_names, data_batch,
        prebuilt_schema_block, true,
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
) -> usize {
    encode_wire_into_impl(
        out, offset, target_id, client_id, flags, seek_pk, seek_col_idx,
        request_id, status, error_msg, schema, col_names, data_batch,
        prebuilt_schema_block, false,
    )
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
    checksum: bool,
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = has_data
        || (schema.is_some() && status == STATUS_OK)
        || (prebuilt_schema_block.is_some() && status == STATUS_OK);

    let mut wire_flags = flags;
    if has_schema { wire_flags |= FLAG_HAS_SCHEMA; }
    if has_data {
        wire_flags |= FLAG_HAS_DATA;
        let b = data_batch.unwrap();
        if b.sorted { wire_flags |= FLAG_BATCH_SORTED; }
        if b.consolidated { wire_flags |= FLAG_BATCH_CONSOLIDATED; }
    }

    let written = encode_ctrl_block_direct(out, offset, target_id, client_id, wire_flags,
                                           seek_pk, seek_col_idx, request_id, status, error_msg, checksum);
    let mut pos = offset + written;

    if has_schema {
        if let Some(prebuilt) = prebuilt_schema_block {
            // Fast path: prebuilt bytes already hold the encoded schema block.
            // Copy directly — skips Batch allocation and encode_to_wire overhead.
            let end = pos + prebuilt.len();
            out[pos..end].copy_from_slice(prebuilt);
            pos = end;
        } else {
            let eff_schema = if let Some(s) = schema { s } else {
                data_batch.unwrap().schema.as_ref().unwrap()
            };
            let names = col_names.unwrap_or(&[]);
            let schema_batch = schema_to_batch(eff_schema, names);
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

/// Decoded control fields from a wire message.
pub struct DecodedControl {
    pub status: u32,
    pub client_id: u64,
    pub target_id: u64,
    pub flags: u64,
    pub seek_pk: u128,
    pub seek_col_idx: u64,
    pub request_id: u64,
    pub error_msg: Vec<u8>,
    /// Total byte length of this control WAL block (read from the WAL size
    /// field at offset WAL_OFF_SIZE). Callers that need to advance past the
    /// ctrl block to find the schema/data blocks use this directly instead of
    /// re-reading the WAL header.
    pub block_size: usize,
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

/// Read the (data_offset, data_size) directory entry for region `r` from a
/// WAL block.  Panics on slice errors — callers must validate `data.len()`
/// covers the full directory before calling.
#[inline(always)]
fn wal_dir_entry(data: &[u8], r: usize) -> (usize, usize) {
    const HEADER: usize = gnitz_wire::WAL_HEADER_SIZE;
    const DIR_ENTRY: usize = 8;
    let base = HEADER + r * DIR_ENTRY;
    let off = u32::from_le_bytes(data[base    ..base + 4].try_into().unwrap()) as usize;
    let sz  = u32::from_le_bytes(data[base + 4..base + 8].try_into().unwrap()) as usize;
    (off, sz)
}

/// Decode all control fields directly from the WAL block's directory without
/// allocating a `Batch`. Each directory entry stores (data_offset: u32,
/// data_size: u32) at `HEADER_SIZE + region * 8`. For a 1-row control block
/// every u64 region is exactly 8 bytes, so we can index the fields directly.
pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    use gnitz_wire::control as ctrl;

    let dir_end = gnitz_wire::WAL_HEADER_SIZE + ctrl::NUM_REGIONS * 8;
    if data.len() < dir_end {
        return Err("control block too small");
    }

    // Validate WAL version and region count without computing the checksum.
    let version = u32::from_le_bytes(data[WAL_OFF_VERSION..WAL_OFF_VERSION + 4].try_into().unwrap());
    if version != gnitz_wire::WAL_FORMAT_VERSION {
        return Err("control block wrong version");
    }
    let num_regions = u32::from_le_bytes(data[WAL_OFF_NUM_REGIONS..WAL_OFF_NUM_REGIONS + 4].try_into().unwrap());
    if num_regions as usize != ctrl::NUM_REGIONS {
        return Err("control block wrong region count");
    }

    // Read a u64 from a fixed-width u64 region (exactly 8 bytes for 1 row).
    let read_u64 = |r: usize| -> Result<u64, &'static str> {
        let (off, sz) = wal_dir_entry(data, r);
        if sz < 8 || off + 8 > data.len() {
            return Err("control block region out of bounds");
        }
        Ok(u64::from_le_bytes(data[off..off + 8].try_into().unwrap()))
    };

    // Read a u128 from a fixed-width u128 region (exactly 16 bytes for 1 row).
    let read_u128 = |r: usize| -> Result<u128, &'static str> {
        let (off, sz) = wal_dir_entry(data, r);
        if sz < 16 || off + 16 > data.len() {
            return Err("control block u128 region out of bounds");
        }
        Ok(u128::from_le_bytes(data[off..off + 16].try_into().unwrap()))
    };

    let null_bmp     = read_u64(ctrl::REGION_NULL_BMP)?;
    let status       = read_u64(ctrl::REGION_STATUS)? as u32;
    let client_id    = read_u64(ctrl::REGION_CLIENT_ID)?;
    let target_id    = read_u64(ctrl::REGION_TARGET_ID)?;
    let flags        = read_u64(ctrl::REGION_FLAGS)?;
    let seek_pk      = read_u128(ctrl::REGION_SEEK_PK)?;
    let seek_col_idx = read_u64(ctrl::REGION_SEEK_COL_IDX)?;
    let request_id   = read_u64(ctrl::REGION_REQUEST_ID)?;

    let error_is_null = (null_bmp & ctrl::NULL_BIT_ERROR_MSG) != 0;
    let error_msg = if error_is_null {
        Vec::new()
    } else {
        let (err_off, err_sz) = wal_dir_entry(data, ctrl::REGION_ERROR_MSG);
        if err_sz < 16 || err_off + 16 > data.len() {
            return Err("error_msg region out of bounds");
        }
        let mut st = [0u8; 16];
        st.copy_from_slice(&data[err_off..err_off + 16]);
        let (blob_off, blob_sz) = wal_dir_entry(data, ctrl::REGION_BLOB);
        let blob = if blob_sz > 0 && blob_off + blob_sz <= data.len() {
            &data[blob_off..blob_off + blob_sz]
        } else {
            &[]
        };
        decode_german_string(&st, blob)
    };

    let block_size = u32::from_le_bytes(data[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].try_into().unwrap()) as usize;
    Ok(DecodedControl {
        status, client_id, target_id, flags,
        seek_pk, seek_col_idx, request_id, error_msg,
        block_size,
    })
}

fn schemas_layout_equal(a: &SchemaDescriptor, b: &SchemaDescriptor) -> bool {
    if a.num_columns != b.num_columns || a.pk_index != b.pk_index { return false; }
    for i in 0..a.num_columns as usize {
        if a.columns[i].type_code != b.columns[i].type_code
            || a.columns[i].nullable != b.columns[i].nullable
        {
            return false;
        }
    }
    true
}

fn decode_schema_block(data: &[u8], verify_checksum: bool) -> Result<SchemaDescriptor, &'static str> {
    // Parse directly from WAL block bytes; no Batch allocation needed.
    // META_SCHEMA_DESC layout: pk(reg 0), weight(1), null_bmp(2),
    //   type_code/U64(3), flags/U64(4), name/STR(5), blob(6).
    const HEADER: usize = gnitz_wire::WAL_HEADER_SIZE;

    if data.len() < HEADER { return Err("schema block too small"); }

    let version = u32::from_le_bytes(data[WAL_OFF_VERSION..WAL_OFF_VERSION + 4].try_into().unwrap());
    if version != gnitz_wire::WAL_FORMAT_VERSION {
        return Err("schema block wrong version");
    }
    let total_size = u32::from_le_bytes(data[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].try_into().unwrap()) as usize;
    if total_size > data.len() { return Err("schema block truncated"); }

    if verify_checksum && total_size > HEADER {
        let expected = u64::from_le_bytes(data[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8].try_into().unwrap());
        let actual = crate::xxh::checksum(&data[HEADER..total_size]);
        if actual != expected { return Err("schema block checksum mismatch"); }
    }

    let count = u32::from_le_bytes(data[WAL_OFF_COUNT..WAL_OFF_COUNT + 4].try_into().unwrap()) as usize;
    let num_regions = u32::from_le_bytes(data[WAL_OFF_NUM_REGIONS..WAL_OFF_NUM_REGIONS + 4].try_into().unwrap()) as usize;

    if count == 0 { return Err("empty schema block"); }
    if count > crate::schema::MAX_COLUMNS { return Err("schema exceeds column limit"); }
    if num_regions < 5 { return Err("schema block region count mismatch"); }

    let (tc_off, tc_sz) = wal_dir_entry(data, 3);
    let (fl_off, fl_sz) = wal_dir_entry(data, 4);

    if tc_sz < count * 8 || tc_off + tc_sz > data.len() { return Err("schema type_code region OOB"); }
    if fl_sz < count * 8 || fl_off + fl_sz > data.len() { return Err("schema flags region OOB"); }

    let type_data  = &data[tc_off..tc_off + count * 8];
    let flags_data = &data[fl_off..fl_off + count * 8];

    let mut sd = SchemaDescriptor {
        num_columns: count as u32,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; crate::schema::MAX_COLUMNS],
    };
    let mut pk_found = false;

    for i in 0..count {
        let off8 = i * 8;
        let tc = u64::from_le_bytes(type_data[off8..off8+8].try_into().unwrap()) as u8;
        let fl = u64::from_le_bytes(flags_data[off8..off8+8].try_into().unwrap());
        let is_nullable = (fl & META_FLAG_NULLABLE) != 0;
        let is_pk       = (fl & META_FLAG_IS_PK)       != 0;
        sd.columns[i] = SchemaColumn {
            type_code: tc,
            size: type_size(tc),
            nullable: if is_nullable { 1 } else { 0 },
            _pad: 0,
        };
        if is_pk {
            if pk_found { return Err("multiple PK columns"); }
            sd.pk_index = i as u32;
            pk_found = true;
        }
    }
    if !pk_found { return Err("no PK column"); }
    Ok(sd)
}

/// Peek at just the `target_id` from a wire message's control block.
pub fn peek_target_id(data: &[u8]) -> Result<i64, &'static str> {
    if data.len() < gnitz_wire::WAL_HEADER_SIZE {
        return Err("IPC payload too small");
    }
    let ctrl_size = u32::from_le_bytes(
        data[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].try_into().map_err(|_| "bad control size")?
    ) as usize;
    if ctrl_size > data.len() {
        return Err("control block truncated");
    }
    let ctrl = peek_control_block(&data[..ctrl_size])?;
    Ok(ctrl.target_id as i64)
}

/// Decode a wire message using a caller-provided schema.
pub fn decode_wire_with_schema(data: &[u8], schema: &SchemaDescriptor) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, Some(schema), true)
}

/// Decode a full IPC wire message from raw bytes.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, None, true)
}

/// Like `decode_wire` but skips WAL block checksum verification.  Use for
/// trusted intra-process IPC (W2M ring).
pub fn decode_wire_ipc(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, None, false)
}

fn decode_wire_impl(
    data: &[u8],
    schema_hint: Option<&SchemaDescriptor>,
    verify_checksum: bool,
) -> Result<DecodedWire, &'static str> {
    if data.len() < gnitz_wire::WAL_HEADER_SIZE {
        return Err("IPC payload too small");
    }
    let ctrl_size = u32::from_le_bytes(
        data[WAL_OFF_SIZE..WAL_OFF_SIZE + 4].try_into().map_err(|_| "bad control size")?
    ) as usize;
    if ctrl_size > data.len() {
        return Err("control block truncated");
    }
    let control = peek_control_block(&data[..ctrl_size])?;
    decode_wire_body(data, ctrl_size, control, schema_hint, verify_checksum)
}

/// Like `decode_wire_ipc` but reuses a pre-parsed control block, avoiding a
/// redundant parse of the control WAL block.
pub(crate) fn decode_wire_ipc_with_ctrl(
    data: &[u8],
    ctrl_size: usize,
    control: DecodedControl,
) -> Result<DecodedWire, &'static str> {
    decode_wire_body(data, ctrl_size, control, None, false)
}

fn decode_wire_body(
    data: &[u8],
    ctrl_size: usize,
    control: DecodedControl,
    schema_hint: Option<&SchemaDescriptor>,
    verify_checksum: bool,
) -> Result<DecodedWire, &'static str> {
    let flags = control.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data   = (flags & FLAG_HAS_DATA)   != 0;

    if has_data && !has_schema {
        return Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA");
    }

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    if has_schema {
        if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
            return Err("schema block truncated");
        }
        let schema_size = u32::from_le_bytes(
            data[off + WAL_OFF_SIZE..off + WAL_OFF_SIZE + 4].try_into().map_err(|_| "bad schema size")?
        ) as usize;
        if off + schema_size > data.len() {
            return Err("schema block truncated");
        }
        let parsed = decode_schema_block(&data[off..off + schema_size], verify_checksum)?;
        if let Some(hint) = schema_hint {
            if !schemas_layout_equal(&parsed, hint) {
                return Err("schema mismatch: client schema differs from server schema");
            }
            wire_schema = Some(*hint);
        } else {
            wire_schema = Some(parsed);
        }
        off += schema_size;
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
            return Err("data block truncated");
        }
        let data_size = u32::from_le_bytes(
            data[off + WAL_OFF_SIZE..off + WAL_OFF_SIZE + 4].try_into().map_err(|_| "bad data size")?
        ) as usize;
        if off + data_size > data.len() {
            return Err("data block truncated");
        }
        let (mut batch, _) = Batch::decode_from_wal_block(&data[off..off + data_size], eff_schema, verify_checksum)?;
        if (flags & FLAG_BATCH_SORTED) != 0 { batch.mark_sorted(); }
        if (flags & FLAG_BATCH_CONSOLIDATED) != 0 { batch.mark_consolidated(); }
        Some(batch)
    } else {
        None
    };

    Ok(DecodedWire { control, schema: wire_schema, data_batch })
}

/// Decode a W2M IPC message without copying data: schema is parsed from the
/// wire bytes directly and the data block is returned as a `MemBatch<'a>`
/// that borrows slices from `data`.  The caller must keep `data` live (i.e.
/// hold the `W2mSlot`) until it is done reading from the `MemBatch`.
///
/// Takes a pre-parsed `control` block (from `peek_control_block`) and its byte
/// size so the caller can inspect flags before choosing a decode path without
/// triggering a redundant parse.
pub(crate) fn decode_wire_ipc_zero_copy_with_ctrl<'a>(
    data: &'a [u8],
    ctrl_size: usize,
    control: DecodedControl,
) -> Result<DecodedWireZeroCopy<'a>, &'static str> {
    let flags = control.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data   = (flags & FLAG_HAS_DATA)   != 0;

    if has_data && !has_schema {
        return Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA");
    }

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    if has_schema {
        if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
            return Err("schema block truncated");
        }
        let schema_size = u32::from_le_bytes(
            data[off + WAL_OFF_SIZE..off + WAL_OFF_SIZE + 4].try_into().map_err(|_| "bad schema size")?
        ) as usize;
        if off + schema_size > data.len() {
            return Err("schema block truncated");
        }
        wire_schema = Some(decode_schema_block(&data[off..off + schema_size], false)?);
        off += schema_size;
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        if off + gnitz_wire::WAL_HEADER_SIZE > data.len() {
            return Err("data block truncated");
        }
        let data_size = u32::from_le_bytes(
            data[off + WAL_OFF_SIZE..off + WAL_OFF_SIZE + 4].try_into().map_err(|_| "bad data size")?
        ) as usize;
        if off + data_size > data.len() {
            return Err("data block truncated");
        }
        let mb = crate::storage::decode_mem_batch_from_wal_block(
            &data[off..off + data_size], eff_schema,
        )?;
        Some(mb)
    } else {
        None
    };

    Ok(DecodedWireZeroCopy { control, schema: wire_schema, data_batch })
}
