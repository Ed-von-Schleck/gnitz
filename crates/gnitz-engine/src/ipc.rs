//! IPC transport layer: atomics, SAL (shared append-only log), W2M (worker→master),
//! and wire protocol encode/decode.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::ipc_sys;
use crate::sys;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, type_size, encode_german_string, decode_german_string};
use crate::storage::{Batch, wal};
use crate::util::{align8, read_u32_raw, read_u64_raw, write_u32_raw, write_u64_raw};
use crate::w2m_ring::{
    self, W2mRingHeader, FLAG_MASTER_PARKED, FLAG_WRITER_PARKED, TryReserve,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAX_WORKERS: usize = 64;
/// Group header: 32 fixed + MAX_WORKERS*4 offsets + MAX_WORKERS*4 sizes + 32 pad
pub(crate) const GROUP_HEADER_SIZE: usize = 576;
pub const SAL_MMAP_SIZE: usize = 1 << 30;

/// Base page size on Linux x86_64. The SAL is mmap'd from a regular
/// filesystem, so hugepage backing is not in play here. Hardcoding
/// avoids a sysconf(_SC_PAGESIZE) call on every prefault.
const PAGE_SIZE: u64 = 4096;

pub use gnitz_wire::{
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, IPC_CONTROL_TID,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK,
    FLAG_CONFLICT_MODE_PRESENT as FLAG_CONFLICT_MODE_PRESENT_U64,
    WireConflictMode,
};
/// Engine-side u32 alias for the cross-crate conflict-mode marker bit.
/// The SAL group header stores flags as u32.
pub const FLAG_CONFLICT_MODE_PRESENT: u32 = FLAG_CONFLICT_MODE_PRESENT_U64 as u32;

/// Decode a `WireConflictMode` from a wire or SAL header. Callers with
/// a `u32` SAL header flags field should widen it at the call site
/// (`flags as u64`) so we only need one decode path. Missing flag bit
/// defaults to `Update` so SAL entries written without a mode retain
/// silent-upsert semantics.
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

// SAL group header flags (u32 — engine-only flags after FLAG_SEEK_BY_INDEX)
gnitz_wire::cast_consts! { pub u32;
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH,
    FLAG_HAS_PK, FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_EXECUTE_SQL,
}
pub const FLAG_EXCHANGE_RELAY: u32      = 512;
pub const FLAG_PRELOADED_EXCHANGE: u32  = 1024;
pub const FLAG_BACKFILL: u32            = 2048;
pub const FLAG_TICK: u32                = 4096;
pub const FLAG_CHECKPOINT: u32          = 8192;
pub const FLAG_FLUSH: u32               = 16384;

// ---------------------------------------------------------------------------
// Wire protocol: schema ↔ batch conversion
// ---------------------------------------------------------------------------

const ZERO_COL: SchemaColumn = SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 };
const U64_COL: SchemaColumn = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
const STR_COL: SchemaColumn = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
const STR_COL_NULL: SchemaColumn = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 1, _pad: 0 };

/// META_SCHEMA: col_idx(PK), type_code, flags, name.
const META_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor { num_columns: 4, pk_index: 0, columns: [ZERO_COL; 64] };
    sd.columns[0] = U64_COL;
    sd.columns[1] = U64_COL;
    sd.columns[2] = U64_COL;
    sd.columns[3] = STR_COL;
    sd
};

// `schema_to_batch` / `decode_schema_block` hardcode the 4-column META_SCHEMA
// layout. If anyone ever bumps `META_SCHEMA_DESC.num_columns`, this
// compile-time check fails and forces the converter functions to be updated.
const _: () = assert!(
    META_SCHEMA_DESC.num_columns == 4,
    "META_SCHEMA layout changed; update schema_to_batch and decode_schema_block",
);

/// CONTROL_SCHEMA: 9 U64 cols + 1 nullable String. The exact column layout,
/// payload indices, null-bit positions, and region count are defined in
/// `gnitz_wire::control` so the server (this crate) and the client
/// (`gnitz-protocol`) cannot drift.
const CONTROL_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor {
        num_columns: gnitz_wire::control::NUM_COLUMNS as u32,
        pk_index: 0,
        columns: [ZERO_COL; 64],
    };
    sd.columns[0] = U64_COL; sd.columns[1] = U64_COL; sd.columns[2] = U64_COL;
    sd.columns[3] = U64_COL; sd.columns[4] = U64_COL; sd.columns[5] = U64_COL;
    sd.columns[6] = U64_COL; sd.columns[7] = U64_COL; sd.columns[8] = U64_COL;
    sd.columns[9] = STR_COL_NULL;
    sd
};

/// Convert a SchemaDescriptor + column names into a META_SCHEMA Batch.
fn schema_to_batch(schema: &SchemaDescriptor, col_names: &[&[u8]]) -> Batch {
    let ncols = schema.num_columns as usize;
    let meta = META_SCHEMA_DESC;
    let mut batch = Batch::with_schema(meta, ncols);

    for ci in 0..ncols {
        let col = &schema.columns[ci];
        let mut flags: u64 = 0;
        if col.nullable != 0 {
            flags |= META_FLAG_NULLABLE;
        }
        if ci == schema.pk_index as usize {
            flags |= META_FLAG_IS_PK;
        }

        let type_code_val = col.type_code as u64;
        let name = if ci < col_names.len() { col_names[ci] } else { b"" };
        let name_st = encode_german_string(name, &mut batch.blob);

        // Append row: pk = (ci, 0), weight = 1, null_word = 0
        batch.extend_pk_lo(&(ci as u64).to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        // payload col 0 (type_code): u64
        batch.extend_col(0, &type_code_val.to_le_bytes());
        // payload col 1 (flags): u64
        batch.extend_col(1, &flags.to_le_bytes());
        // payload col 2 (name): 16-byte German string
        batch.extend_col(2, &name_st);
        batch.count += 1;
    }
    batch
}

/// Convert a META_SCHEMA Batch back into a SchemaDescriptor + column names.
/// Only used by tests (production decode uses `decode_schema_block` instead).
#[cfg(test)]
fn batch_to_schema(batch: &Batch) -> Result<(SchemaDescriptor, Vec<Vec<u8>>), &'static str> {
    if batch.count == 0 {
        return Err("empty schema batch");
    }
    if batch.count > 64 {
        return Err("schema exceeds 64-column limit");
    }
    let mut sd = SchemaDescriptor {
        num_columns: batch.count as u32,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
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
        let name = decode_german_string(&st, &batch.blob);
        names.push(name);

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
            if pk_found {
                return Err("multiple PK columns");
            }
            sd.pk_index = i as u32;
            pk_found = true;
        }
    }
    if !pk_found {
        return Err("no PK column");
    }
    Ok((sd, names))
}

// ---------------------------------------------------------------------------
// Wire protocol: encode
// ---------------------------------------------------------------------------

/// Encode a full IPC wire message.
///
/// Returns a heap-allocated `Vec<u8>` containing the encoded wire message.
pub fn encode_wire(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk_lo: u64,
    seek_pk_hi: u64,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> Vec<u8> {
    let sz = wire_size(status, error_msg, schema, col_names, data_batch);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf, 0, target_id, client_id, flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx, request_id,
        status, error_msg, schema, col_names, data_batch,
    );
    buf
}

/// Max regions per batch: 4 fixed + up to 63 payload + 1 blob = 68.
const MAX_REGIONS_TOTAL: usize = 69;

/// Compute the WAL block size for a given batch without allocating.
fn batch_wal_block_size(batch: &Batch) -> usize {
    let nr = batch.num_regions_total();
    let mut sizes = [0u32; MAX_REGIONS_TOTAL];
    for i in 0..nr {
        sizes[i] = batch.region_size(i) as u32;
    }
    wal::block_size(nr, &sizes[..nr])
}

/// Compute WAL block size from schema metadata without constructing a batch.
/// Replicates the region layout: 4 fixed (pk_lo/pk_hi/weight/null_bmp, stride 8)
/// + (num_columns - 1) payload columns (PK excluded) + 1 blob.
fn schema_wal_block_size(schema: &SchemaDescriptor, row_count: usize, blob_size: usize) -> usize {
    let pk_idx = schema.pk_index as usize;
    let num_payload = schema.num_columns as usize - 1;
    let num_regions = 4 + num_payload + 1;
    let mut sizes = [0u32; MAX_REGIONS_TOTAL];
    for i in 0..4 { sizes[i] = (8 * row_count) as u32; }
    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_idx { continue; }
        sizes[4 + pi] = (schema.columns[ci].size as usize * row_count) as u32;
        pi += 1;
    }
    sizes[4 + pi] = blob_size as u32;
    wal::block_size(num_regions, &sizes[..num_regions])
}

/// Compute the total encoded wire size without allocating.
///
/// Same parameters as `encode_wire`. Returns the exact byte count that
/// `encode_wire()` would produce.
pub fn wire_size(
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = has_data || (schema.is_some() && status == STATUS_OK);

    // Control block: 1 row of CONTROL_SCHEMA_DESC
    let ctrl_blob = if error_msg.len() > gnitz_wire::SHORT_STRING_THRESHOLD {
        error_msg.len()
    } else { 0 };
    let mut total = schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, ctrl_blob);

    if has_schema {
        let s = schema.unwrap_or_else(|| data_batch.unwrap().schema.as_ref().unwrap());
        let names = col_names.unwrap_or(&[]);
        let ncols = s.num_columns as usize;
        let schema_blob: usize = names.iter().take(ncols)
            .map(|n| if n.len() > gnitz_wire::SHORT_STRING_THRESHOLD { n.len() } else { 0 })
            .sum();
        total += schema_wal_block_size(&META_SCHEMA_DESC, ncols, schema_blob);
    }

    if has_data {
        total += batch_wal_block_size(data_batch.unwrap());
    }
    total
}

/// Encode a WAL block for a batch into `out[offset..]`. Returns new offset.
fn encode_batch_to_wal_into(
    out: &mut [u8], offset: usize, batch: &Batch, table_id: u32,
) -> usize {
    let nr = batch.num_regions_total();
    let mut ptrs = [std::ptr::null::<u8>(); MAX_REGIONS_TOTAL];
    let mut sizes = [0u32; MAX_REGIONS_TOTAL];
    for i in 0..nr {
        ptrs[i] = batch.region_ptr(i);
        sizes[i] = batch.region_size(i) as u32;
    }
    let blob_size = batch.blob.len() as u64;
    wal::encode(
        out, offset, 0, table_id, batch.count as u32,
        &ptrs[..nr], &sizes[..nr], blob_size,
    ).expect("WAL encode_into failed: buffer too small")
}

/// Encode a full IPC wire message into a caller-provided buffer.
///
/// Same logic as `encode_wire()` but writes into `out` starting at `offset`.
/// Returns bytes written. Panics if the buffer is too small (caller must
/// pre-size via `wire_size()`).
pub fn encode_wire_into(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk_lo: u64,
    seek_pk_hi: u64,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&Batch>,
) -> usize {
    let has_data = data_batch.map(|b| b.count > 0).unwrap_or(false);
    let has_schema = has_data || (schema.is_some() && status == STATUS_OK);

    let mut wire_flags = flags;
    if has_schema { wire_flags |= FLAG_HAS_SCHEMA; }
    if has_data {
        wire_flags |= FLAG_HAS_DATA;
        let b = data_batch.unwrap();
        if b.sorted { wire_flags |= FLAG_BATCH_SORTED; }
        if b.consolidated { wire_flags |= FLAG_BATCH_CONSOLIDATED; }
    }

    // Control block
    let ctrl_batch = {
        use gnitz_wire::control as ctrl;
        let cs = CONTROL_SCHEMA_DESC;
        let mut b = Batch::with_schema(cs, 1);
        let has_error = !error_msg.is_empty();
        let null_word: u64 = if has_error { 0 } else { ctrl::NULL_BIT_ERROR_MSG };
        b.extend_pk_lo(&0u64.to_le_bytes());
        b.extend_pk_hi(&0u64.to_le_bytes());
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_STATUS,       &(status as u64).to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_CLIENT_ID,    &client_id.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_TARGET_ID,    &target_id.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_FLAGS,        &wire_flags.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_SEEK_PK_LO,   &seek_pk_lo.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_SEEK_PK_HI,   &seek_pk_hi.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_SEEK_COL_IDX, &seek_col_idx.to_le_bytes());
        b.extend_col(ctrl::PAYLOAD_REQUEST_ID,   &request_id.to_le_bytes());
        let error_st = if has_error {
            encode_german_string(error_msg, &mut b.blob)
        } else {
            [0u8; 16]
        };
        b.extend_col(ctrl::PAYLOAD_ERROR_MSG, &error_st);
        b.count = 1;
        b
    };
    let mut pos = encode_batch_to_wal_into(out, offset, &ctrl_batch, IPC_CONTROL_TID);

    if has_schema {
        let eff_schema = if let Some(s) = schema {
            s
        } else {
            data_batch.unwrap().schema.as_ref().unwrap()
        };
        let names = col_names.unwrap_or(&[]);
        let schema_batch = schema_to_batch(eff_schema, names);
        pos = encode_batch_to_wal_into(out, pos, &schema_batch, target_id as u32);
    }

    if has_data {
        pos = encode_batch_to_wal_into(out, pos, data_batch.unwrap(), target_id as u32);
    }

    pos - offset
}

// ---------------------------------------------------------------------------
// Wire protocol: decode
// ---------------------------------------------------------------------------

/// Decoded control fields from a wire message.
pub struct DecodedControl {
    pub status: u32,
    pub client_id: u64,
    pub target_id: u64,
    pub flags: u64,
    pub seek_pk_lo: u64,
    pub seek_pk_hi: u64,
    pub seek_col_idx: u64,
    pub request_id: u64,
    pub error_msg: Vec<u8>,
}

/// Full decoded wire message.
pub struct DecodedWire {
    pub control: DecodedControl,
    pub schema: Option<SchemaDescriptor>,
    pub data_batch: Option<Batch>,
    /// Backing buffer for `data_batch`. `Batch::from_regions` builds the
    /// batch out of pointers into the source bytes; in W2M this source
    /// is a heap copy of the ring slot (the slot itself can be reused
    /// by the producer the moment the read cursor advances). Keeping
    /// the buffer here ensures the pointers remain valid for the
    /// lifetime of `DecodedWire`. `None` when `data_batch` is `None`
    /// (no allocation needed) or when the source bytes are already
    /// owned by a longer-lived allocation (e.g. the SAL mmap region).
    pub batch_backing: Option<Vec<u8>>,
}

/// Decode a WAL block from raw bytes into an Batch.
fn decode_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Batch, &'static str> {
    let npc = schema.num_columns as usize - 1; // payload cols
    let expected_regions = 4 + npc + 1; // pk_lo, pk_hi, weight, nulls, payload cols, blob

    let mut lsn = 0u64;
    let mut tid = 0u32;
    let mut count = 0u32;
    let mut num_regions = 0u32;
    let mut blob_size = 0u64;
    let mut offsets = [0u64; 128];
    let mut sizes = [0u32; 128];

    if wal::validate_and_parse(
        data, &mut lsn, &mut tid, &mut count, &mut num_regions,
        &mut blob_size, &mut offsets, &mut sizes, 128,
    ).is_err() {
        return Err("WAL block validation failed");
    }
    if (num_regions as usize) != expected_regions {
        return Err("WAL block region count mismatch");
    }

    // Build region pointers into the data buffer.
    // Stack arrays (MAX_REGIONS_TOTAL) avoid two Vec allocs per call.
    let nr = num_regions as usize;
    let mut ptrs = [std::ptr::null::<u8>(); MAX_REGIONS_TOTAL];
    let mut region_sizes = [0u32; MAX_REGIONS_TOTAL];
    for i in 0..nr {
        let off = offsets[i] as usize;
        let sz = sizes[i] as usize;
        region_sizes[i] = sizes[i];
        if sz > 0 && off + sz <= data.len() {
            ptrs[i] = unsafe { data.as_ptr().add(off) };
        }
    }

    let mut batch = unsafe {
        Batch::from_regions(&ptrs[..nr], &region_sizes[..nr], count as usize, npc)
    };
    batch.set_schema(*schema);
    Ok(batch)
}

/// Decode the control WAL block directly into [`DecodedControl`] without
/// constructing a [`Batch`]. Region indices and null-bit positions come from
/// `gnitz_wire::control`; see that module for the schema layout.
fn decode_control_block(data: &[u8]) -> Result<DecodedControl, &'static str> {
    use gnitz_wire::control as ctrl;
    const EXPECTED_REGIONS: usize = ctrl::NUM_REGIONS;
    const _: () = assert!(
        {
            let npc = CONTROL_SCHEMA_DESC.num_columns as usize - 1;
            4 + npc + 1 == EXPECTED_REGIONS
        },
        "EXPECTED_REGIONS out of sync with CONTROL_SCHEMA_DESC",
    );

    let mut lsn = 0u64;
    let mut tid = 0u32;
    let mut count = 0u32;
    let mut num_regions = 0u32;
    let mut blob_size = 0u64;
    let mut offsets = [0u64; EXPECTED_REGIONS];
    let mut sizes   = [0u32; EXPECTED_REGIONS];

    if wal::validate_and_parse(
        data, &mut lsn, &mut tid, &mut count, &mut num_regions,
        &mut blob_size, &mut offsets, &mut sizes, EXPECTED_REGIONS as u32,
    ).is_err() {
        return Err("control block invalid");
    }
    if count != 1 {
        return Err("control block must have 1 row");
    }
    if num_regions as usize != EXPECTED_REGIONS {
        return Err("control block region count mismatch");
    }

    let read8 = |r: usize| -> Result<u64, &'static str> {
        let off = offsets[r] as usize;
        if off + 8 > data.len() {
            return Err("control field truncated");
        }
        Ok(crate::util::read_u64_le(data, off))
    };

    let null_bmp     = read8(ctrl::REGION_NULL_BMP)?;
    let status       = read8(ctrl::REGION_STATUS)? as u32;
    let client_id    = read8(ctrl::REGION_CLIENT_ID)?;
    let target_id    = read8(ctrl::REGION_TARGET_ID)?;
    let flags        = read8(ctrl::REGION_FLAGS)?;
    let seek_pk_lo   = read8(ctrl::REGION_SEEK_PK_LO)?;
    let seek_pk_hi   = read8(ctrl::REGION_SEEK_PK_HI)?;
    let seek_col_idx = read8(ctrl::REGION_SEEK_COL_IDX)?;
    let request_id   = read8(ctrl::REGION_REQUEST_ID)?;

    let error_is_null = (null_bmp & ctrl::NULL_BIT_ERROR_MSG) != 0;
    let error_msg = if error_is_null {
        Vec::new()
    } else {
        let off = offsets[ctrl::REGION_ERROR_MSG] as usize;
        if off + 16 > data.len() {
            return Err("error_msg region truncated");
        }
        let mut st = [0u8; 16];
        st.copy_from_slice(&data[off..off + 16]);
        let blob_off = offsets[ctrl::REGION_BLOB] as usize;
        let blob_sz  = sizes[ctrl::REGION_BLOB] as usize;
        let blob = if blob_sz > 0 && blob_off + blob_sz <= data.len() {
            &data[blob_off..blob_off + blob_sz]
        } else {
            &[]
        };
        decode_german_string(&st, blob)
    };

    Ok(DecodedControl {
        status, client_id, target_id, flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx, request_id, error_msg,
    })
}

/// Decode the META_SCHEMA WAL block directly into a [`SchemaDescriptor`]
/// without constructing a [`Batch`] or decoding column names.
///
/// META_SCHEMA_DESC layout (4 columns, pk_index=0, N rows = N table columns):
///   [0]=pk_lo  [1]=pk_hi  [2]=weight  [3]=null_bmp
///   [4]=type_code (u64)  [5]=flags (u64)  [6]=name (STRING, skipped)  [7]=blob (skipped)
fn decode_schema_block(data: &[u8]) -> Result<SchemaDescriptor, &'static str> {
    // 4 fixed + 3 payload + 1 blob = 8. Keep in sync with META_SCHEMA_DESC.
    const EXPECTED_REGIONS: usize = 8;
    const _: () = assert!(
        {
            let npc = META_SCHEMA_DESC.num_columns as usize - 1;
            4 + npc + 1 == EXPECTED_REGIONS
        },
        "EXPECTED_REGIONS out of sync with META_SCHEMA_DESC",
    );

    let mut lsn = 0u64;
    let mut tid = 0u32;
    let mut count = 0u32;
    let mut num_regions = 0u32;
    let mut blob_size = 0u64;
    let mut offsets = [0u64; EXPECTED_REGIONS];
    let mut sizes   = [0u32; EXPECTED_REGIONS];

    if wal::validate_and_parse(
        data, &mut lsn, &mut tid, &mut count, &mut num_regions,
        &mut blob_size, &mut offsets, &mut sizes, EXPECTED_REGIONS as u32,
    ).is_err() {
        return Err("schema block invalid");
    }
    if count == 0 {
        return Err("empty schema block");
    }
    if count > 64 {
        return Err("schema exceeds 64-column limit");
    }
    if num_regions as usize != EXPECTED_REGIONS {
        return Err("schema block region count mismatch");
    }

    // Region [4] = type_code (u64 per row), Region [5] = flags (u64 per row)
    let type_off = offsets[4] as usize;
    let flags_off = offsets[5] as usize;
    let n = count as usize;

    if type_off + n * 8 > data.len() {
        return Err("type_code region truncated");
    }
    if flags_off + n * 8 > data.len() {
        return Err("flags region truncated");
    }

    let mut sd = SchemaDescriptor {
        num_columns: count as u32,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    let mut pk_found = false;

    for i in 0..n {
        let off8 = i * 8;
        let tc = u64::from_le_bytes(
            data[type_off + off8..type_off + off8 + 8].try_into().unwrap()
        ) as u8;
        let fl = u64::from_le_bytes(
            data[flags_off + off8..flags_off + off8 + 8].try_into().unwrap()
        );

        let is_nullable = (fl & META_FLAG_NULLABLE) != 0;
        let is_pk = (fl & META_FLAG_IS_PK) != 0;

        sd.columns[i] = SchemaColumn {
            type_code: tc,
            size: type_size(tc),
            nullable: if is_nullable { 1 } else { 0 },
            _pad: 0,
        };

        if is_pk {
            if pk_found {
                return Err("multiple PK columns");
            }
            sd.pk_index = i as u32;
            pk_found = true;
        }
    }
    if !pk_found {
        return Err("no PK column");
    }

    Ok(sd)
}

/// Peek at just the `target_id` from a wire message's control block.
///
/// Stack-only, 0 allocations. Returns `Err` only on corrupt/truncated input.
pub fn peek_target_id(data: &[u8]) -> Result<i64, &'static str> {
    if data.len() < wal::HEADER_SIZE {
        return Err("IPC payload too small");
    }
    let ctrl_size = u32::from_le_bytes(
        data[16..20].try_into().map_err(|_| "bad control size")?
    ) as usize;
    if ctrl_size > data.len() {
        return Err("control block truncated");
    }
    let ctrl = decode_control_block(&data[..ctrl_size])?;
    Ok(ctrl.target_id as i64)
}

/// Decode a wire message using a caller-provided schema, skipping schema
/// block decode entirely.
///
/// The schema block bytes are still present on the wire (protocol requires
/// FLAG_HAS_SCHEMA when FLAG_HAS_DATA is set), but this function only reads
/// the block SIZE to advance the offset — it does not parse the contents.
pub fn decode_wire_with_schema(data: &[u8], schema: &SchemaDescriptor) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, Some(schema))
}

/// Decode a full IPC wire message from raw bytes.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
    decode_wire_impl(data, None)
}

/// Shared decode implementation.
///
/// When `schema_hint` is `Some`, the schema block is skipped (only its SIZE
/// is read to advance the offset) and the provided descriptor is used to
/// decode the data block. When `None`, the schema block is parsed via
/// `decode_schema_block`.
fn decode_wire_impl(
    data: &[u8],
    schema_hint: Option<&SchemaDescriptor>,
) -> Result<DecodedWire, &'static str> {
    if data.len() < wal::HEADER_SIZE {
        return Err("IPC payload too small");
    }

    // Block 0: control
    let ctrl_size = u32::from_le_bytes(
        data[16..20].try_into().map_err(|_| "bad control size")?
    ) as usize;
    if ctrl_size > data.len() {
        return Err("control block truncated");
    }

    let control = decode_control_block(&data[..ctrl_size])?;

    let flags = control.flags;
    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    if has_data && !has_schema {
        return Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA");
    }

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;

    if has_schema {
        if off + wal::HEADER_SIZE > data.len() {
            return Err("schema block truncated");
        }
        let schema_size = u32::from_le_bytes(
            data[off + 16..off + 20].try_into().map_err(|_| "bad schema size")?
        ) as usize;
        if off + schema_size > data.len() {
            return Err("schema block truncated");
        }

        if let Some(hint) = schema_hint {
            wire_schema = Some(*hint);
        } else {
            wire_schema = Some(decode_schema_block(&data[off..off + schema_size])?);
        }
        off += schema_size;
    }

    let data_batch = if has_data {
        let eff_schema = wire_schema.as_ref().ok_or("no schema for data block")?;
        if off + wal::HEADER_SIZE > data.len() {
            return Err("data block truncated");
        }
        let data_size = u32::from_le_bytes(
            data[off + 16..off + 20].try_into().map_err(|_| "bad data size")?
        ) as usize;
        if off + data_size > data.len() {
            return Err("data block truncated");
        }

        let mut batch = decode_wal_block(&data[off..off + data_size], eff_schema)?;
        if (flags & FLAG_BATCH_SORTED) != 0 {
            batch.sorted = true;
        }
        if (flags & FLAG_BATCH_CONSOLIDATED) != 0 {
            batch.consolidated = true;
        }
        Some(batch)
    } else {
        None
    };

    Ok(DecodedWire { control, schema: wire_schema, data_batch, batch_backing: None })
}

// ---------------------------------------------------------------------------
// Atomics (acquire/release for cross-process shared memory)
// ---------------------------------------------------------------------------

/// Atomic load with Acquire ordering from a raw pointer.
///
/// # Safety
/// `ptr` must point to a naturally-aligned u64 in shared memory.
pub(crate) unsafe fn atomic_load_u64(ptr: *const u8) -> u64 {
    let atomic = &*(ptr as *const AtomicU64);
    atomic.load(Ordering::Acquire)
}

/// Atomic store with Release ordering to a raw pointer.
///
/// # Safety
/// `ptr` must point to a naturally-aligned u64 in shared memory.
pub(crate) unsafe fn atomic_store_u64(ptr: *mut u8, val: u64) {
    let atomic = &*(ptr as *const AtomicU64);
    atomic.store(val, Ordering::Release);
}

/// Write a zero sentinel at `offset` in the SAL mmap so workers stop reading.
/// No-op if the sentinel would fall outside the mapped region.
#[inline]
unsafe fn sal_write_sentinel(sal_ptr: *mut u8, offset: usize, mmap_size: usize) {
    if offset + 8 <= mmap_size {
        atomic_store_u64(sal_ptr.add(offset), 0);
    }
}

// ---------------------------------------------------------------------------
// SAL write (master→workers)
// ---------------------------------------------------------------------------

/// Result from SAL write (used by tests via `sal_write_group`).
#[cfg(test)]
#[repr(C)]
pub(crate) struct SalWriteResult {
    /// 0 on success, -1 on error (SAL full).
    pub status: i32,
    /// New write cursor position.
    pub new_cursor: u64,
}

/// Handle returned by `sal_begin_group`. Header and per-worker directory
/// are already written; caller fills per-worker data, then calls `commit()`.
pub(crate) struct SalGroup {
    sal_ptr: *mut u8,
    hdr_off: usize,
    base: usize,
    total: usize,
    payload_size: usize,
    mmap_size: usize,
}

impl SalGroup {
    /// Raw pointer to the start of worker data at `offset` bytes from header.
    #[inline]
    pub(crate) unsafe fn data_ptr(&self, offset: usize) -> *mut u8 {
        self.sal_ptr.add(self.hdr_off + offset)
    }

    /// Commit: zero sentinel at next position, then atomic release of size prefix.
    pub(crate) unsafe fn commit(self) -> u64 {
        sal_write_sentinel(self.sal_ptr, self.base + self.total, self.mmap_size);
        atomic_store_u64(self.sal_ptr.add(self.base), self.payload_size as u64);
        (self.base + self.total) as u64
    }
}

/// Reserve SAL space, write group header + per-worker directory.
/// Returns `None` if the group doesn't fit or `num_workers > MAX_WORKERS`.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
pub(crate) unsafe fn sal_begin_group(
    sal_ptr: *mut u8,
    write_cursor: usize,
    mmap_size: usize,
    num_workers: usize,
    target_id: u32,
    lsn: u64,
    flags: u32,
    epoch: u32,
    worker_sizes: &[u32],
) -> Option<SalGroup> {
    if num_workers > MAX_WORKERS { return None; }

    let mut payload_size = GROUP_HEADER_SIZE;
    for &sz in &worker_sizes[..num_workers] {
        if sz > 0 { payload_size += align8(sz as usize); }
    }
    let total = 8 + align8(payload_size);
    if write_cursor + total > mmap_size { return None; }

    let base = write_cursor;
    let hdr_off = base + 8;

    // Group header
    write_u64_raw(sal_ptr, hdr_off, payload_size as u64);
    write_u64_raw(sal_ptr, hdr_off + 8, lsn);
    write_u32_raw(sal_ptr, hdr_off + 16, num_workers as u32);
    write_u32_raw(sal_ptr, hdr_off + 20, flags);
    write_u32_raw(sal_ptr, hdr_off + 24, target_id);
    write_u32_raw(sal_ptr, hdr_off + 28, epoch);

    // Per-worker offset/size directory
    let mut data_offset = GROUP_HEADER_SIZE;
    for w in 0..num_workers {
        let sz = worker_sizes[w] as usize;
        if sz > 0 {
            write_u32_raw(sal_ptr, hdr_off + 32 + w * 4, data_offset as u32);
            write_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, worker_sizes[w]);
            data_offset += align8(sz);
        } else {
            write_u32_raw(sal_ptr, hdr_off + 32 + w * 4, 0);
            write_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, 0);
        }
    }

    Some(SalGroup { sal_ptr, hdr_off, base, total, payload_size, mmap_size })
}

/// Write a message group into the SAL for N workers (test helper).
///
/// `worker_ptrs[i]` / `worker_sizes[i]` point to pre-encoded wire buffers.
/// A null pointer or size=0 means "no data for worker i".
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `write_cursor + total`
/// bytes. `worker_ptrs`/`worker_sizes` must be valid arrays of `num_workers`.
#[cfg(test)]
pub(crate) unsafe fn sal_write_group(
    sal_ptr: *mut u8,
    write_cursor: u64,
    num_workers: u32,
    target_id: u32,
    lsn: u64,
    flags: u32,
    epoch: u32,
    mmap_size: u64,
    worker_ptrs: *const *const u8,
    worker_sizes: *const u32,
) -> SalWriteResult {
    let nw = num_workers as usize;
    let mut sizes = [0u32; MAX_WORKERS];
    for w in 0..nw { sizes[w] = *worker_sizes.add(w); }

    let group = match sal_begin_group(
        sal_ptr, write_cursor as usize, mmap_size as usize,
        nw, target_id, lsn, flags, epoch, &sizes[..nw],
    ) {
        Some(g) => g,
        None => return SalWriteResult { status: -1, new_cursor: write_cursor },
    };

    let mut off = GROUP_HEADER_SIZE;
    for w in 0..nw {
        let sz = sizes[w] as usize;
        if sz > 0 {
            let p = *worker_ptrs.add(w);
            if !p.is_null() {
                std::ptr::copy_nonoverlapping(p, group.data_ptr(off), sz);
            }
            off += align8(sz);
        }
    }

    SalWriteResult { status: 0, new_cursor: group.commit() }
}

// ---------------------------------------------------------------------------
// SAL read (worker reads its data from a group)
// ---------------------------------------------------------------------------

/// Result from reading a SAL group header for a specific worker.
#[repr(C)]
pub(crate) struct SalReadResult {
    /// 0 = has data, 1 = no data for this worker, -1 = no message yet
    pub status: i32,
    /// Bytes to advance read_cursor (8 + align8(payload_size))
    pub advance: u64,
    /// LSN from group header
    pub lsn: u64,
    /// Flags from group header
    pub flags: u32,
    /// Target ID from group header
    pub target_id: u32,
    /// Epoch from group header
    pub epoch: u32,
    pub _pad: u32,
    /// Pointer to this worker's data within the SAL (non-owning)
    pub data_ptr: *const u8,
    /// Size of this worker's data
    pub data_size: u32,
    pub _pad2: u32,
}

/// Read a SAL group header and extract this worker's data pointer/size.
///
/// Returns status=-1 if no message at cursor (payload_size==0).
/// Returns status=1 if message exists but no data for this worker.
/// Returns status=0 with data_ptr/data_size set if data exists.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer. `read_cursor` must be within bounds.
pub(crate) unsafe fn sal_read_group_header(
    sal_ptr: *const u8,
    read_cursor: u64,
    worker_id: u32,
) -> SalReadResult {
    let rc = read_cursor as usize;
    let wid = worker_id as usize;

    // 8-byte size prefix (atomic acquire load)
    let payload_size = atomic_load_u64(sal_ptr.add(rc)) as usize;
    if payload_size == 0 {
        return SalReadResult {
            status: -1, advance: 0, lsn: 0, flags: 0,
            target_id: 0, epoch: 0, _pad: 0,
            data_ptr: std::ptr::null(), data_size: 0, _pad2: 0,
        };
    }

    let hdr_off = rc + 8;
    let lsn = read_u64_raw(sal_ptr, hdr_off + 8);
    let flags = read_u32_raw(sal_ptr, hdr_off + 20);
    let target_id = read_u32_raw(sal_ptr, hdr_off + 24);
    let epoch = read_u32_raw(sal_ptr, hdr_off + 28);
    let advance = (8 + align8(payload_size)) as u64;

    let my_offset = read_u32_raw(sal_ptr, hdr_off + 32 + wid * 4) as usize;
    let my_size = read_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + wid * 4) as usize;

    if my_size > 0 && my_offset > 0 {
        // Assert the slice stays within the mapped region; clamp to 0 on
        // corruption so decode_wire returns Err rather than crashing.
        debug_assert!(
            my_offset + my_size <= payload_size,
            "SAL group header corrupt: my_offset={} my_size={} payload_size={}",
            my_offset, my_size, payload_size
        );
        let data_size = if my_offset + my_size <= payload_size { my_size as u32 } else { 0 };
        SalReadResult {
            status: 0,
            advance,
            lsn,
            flags,
            target_id,
            epoch,
            _pad: 0,
            data_ptr: sal_ptr.add(hdr_off + my_offset),
            data_size,
            _pad2: 0,
        }
    } else {
        SalReadResult {
            status: 1,
            advance,
            lsn,
            flags,
            target_id,
            epoch,
            _pad: 0,
            data_ptr: std::ptr::null(),
            data_size: 0,
            _pad2: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Channel types: SalWriter, SalReader, W2mWriter, W2mReceiver
// ---------------------------------------------------------------------------

/// Master's write side of the SAL.
pub struct SalWriter {
    ptr: *mut u8,
    fd: i32,
    mmap_size: u64,
    write_cursor: u64,
    lsn: u64,
    epoch: u32,
    checkpoint_threshold: u64,
    m2w_efds: Vec<i32>,
    num_workers: usize,
    last_prefaulted: u64,
}

unsafe impl Send for SalWriter {}

impl SalWriter {
    pub fn new(ptr: *mut u8, fd: i32, mmap_size: u64, m2w_efds: Vec<i32>) -> Self {
        let num_workers = m2w_efds.len();
        let checkpoint_threshold = std::env::var("GNITZ_CHECKPOINT_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or((mmap_size * 3) >> 2);
        SalWriter {
            ptr,
            fd,
            mmap_size,
            write_cursor: 0,
            lsn: 0,
            epoch: 0,
            checkpoint_threshold,
            m2w_efds,
            num_workers,
            last_prefaulted: 0,
        }
    }

    /// Pre-fault SAL mmap pages 2 MB ahead of the write cursor via MADV_WILLNEED.
    ///
    /// `madvise` requires the address to be page-aligned. `last_prefaulted`
    /// and `write_cursor` can be arbitrary byte offsets between calls, so
    /// we round the start down and the end up to page boundaries. Without
    /// this alignment Linux silently returns EINVAL and the advice is lost.
    fn prefault_ahead(&mut self) {
        const PREFAULT_AHEAD: u64 = 2 * 1024 * 1024;
        let target = (self.write_cursor + PREFAULT_AHEAD).min(self.mmap_size);
        if target <= self.last_prefaulted { return; }
        let raw_start = self.last_prefaulted.max(self.write_cursor);
        let start = raw_start & !(PAGE_SIZE - 1);
        let end_raw = (target + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let end = end_raw.min(self.mmap_size);
        if end > start {
            sys::madvise_willneed(
                unsafe { self.ptr.add(start as usize) },
                (end - start) as usize,
            );
            self.last_prefaulted = target;
        }
    }

    /// Encode per-worker wire data directly into SAL mmap. Does NOT sync/signal.
    pub fn write_group_direct(
        &mut self,
        target_id: u32,
        flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
        assert_eq!(req_ids.len(), nw,
            "write_group_direct: req_ids.len()={} != num_workers={}",
            req_ids.len(), nw);
        let lsn = self.lsn;
        self.lsn += 1;

        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            if unicast_worker >= 0 && w != unicast_worker as usize { continue; }
            let data_batch = worker_batches.get(w).and_then(|opt| opt.as_ref());
            worker_sizes[w] = wire_size(
                STATUS_OK, b"", Some(schema), col_names_opt, data_batch.copied(),
            ) as u32;
        }

        let group = unsafe {
            sal_begin_group(
                self.ptr, self.write_cursor as usize, self.mmap_size as usize,
                nw, target_id, lsn, flags, self.epoch, &worker_sizes[..nw],
            )
        }.ok_or_else(|| format!(
            "SAL write_group_direct failed (cursor={})", self.write_cursor
        ))?;

        let mut off = GROUP_HEADER_SIZE;
        for w in 0..nw {
            let wsz = worker_sizes[w] as usize;
            if wsz > 0 {
                let data_batch = worker_batches.get(w).and_then(|opt| opt.as_ref());
                let slot = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(off), wsz) };
                let written = encode_wire_into(
                    slot, 0, target_id as u64, 0, flags as u64,
                    seek_pk_lo, seek_pk_hi, seek_col_idx, req_ids[w],
                    STATUS_OK, b"", Some(schema), col_names_opt, data_batch.copied(),
                );
                debug_assert_eq!(written, wsz);
                off += align8(wsz);
            }
        }

        self.write_cursor = unsafe { group.commit() };
        Ok(())
    }

    /// Encode once into worker 0's slot, memcpy to workers 1..N-1.
    /// Does NOT sync/signal.
    pub fn write_broadcast_direct(
        &mut self,
        target_id: u32,
        flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
        request_id: u64,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
        let lsn = self.lsn;
        self.lsn += 1;

        let wsz = wire_size(
            STATUS_OK, b"", Some(schema), col_names_opt, batch,
        ) as u32;
        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw { worker_sizes[w] = wsz; }

        let group = unsafe {
            sal_begin_group(
                self.ptr, self.write_cursor as usize, self.mmap_size as usize,
                nw, target_id, lsn, flags, self.epoch, &worker_sizes[..nw],
            )
        }.ok_or_else(|| format!(
            "SAL write_broadcast_direct failed (cursor={})", self.write_cursor
        ))?;

        if wsz > 0 {
            let wsz = wsz as usize;
            let slot0 = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(GROUP_HEADER_SIZE), wsz) };
            let written = encode_wire_into(
                slot0, 0, target_id as u64, 0, flags as u64,
                seek_pk_lo, seek_pk_hi, seek_col_idx, request_id,
                STATUS_OK, b"", Some(schema), col_names_opt, batch,
            );
            debug_assert_eq!(written, wsz);
            let mut off = GROUP_HEADER_SIZE + align8(wsz);
            for _ in 1..nw {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        group.data_ptr(GROUP_HEADER_SIZE),
                        group.data_ptr(off),
                        wsz,
                    );
                }
                off += align8(wsz);
            }
        }

        self.write_cursor = unsafe { group.commit() };
        Ok(())
    }

    /// fdatasync + eventfd_signal all workers. Returns the fdatasync return
    /// code: on failure (< 0), workers are NOT signaled so they never see
    /// non-durable data. On success, returns 0.
    pub fn sync_and_signal_all(&self) -> i32 {
        let rc = sys::fdatasync(self.fd);
        if rc < 0 {
            return rc;
        }
        for w in 0..self.num_workers {
            ipc_sys::eventfd_signal(self.m2w_efds[w]);
        }
        0
    }

    /// fdatasync + eventfd_signal one worker. Returns the fdatasync return
    /// code: on failure (< 0), the worker is NOT signaled.
    pub fn sync_and_signal_one(&self, worker: usize) -> i32 {
        let rc = sys::fdatasync(self.fd);
        if rc < 0 {
            return rc;
        }
        ipc_sys::eventfd_signal(self.m2w_efds[worker]);
        0
    }

    /// eventfd_signal all workers (no fdatasync).
    pub fn signal_all(&self) {
        for w in 0..self.num_workers {
            ipc_sys::eventfd_signal(self.m2w_efds[w]);
        }
    }

    /// eventfd_signal one worker (no fdatasync).
    pub fn signal_one(&self, worker: usize) {
        ipc_sys::eventfd_signal(self.m2w_efds[worker]);
    }

    /// fdatasync only (no signal). Returns the fdatasync return code.
    pub fn sync(&self) -> i32 {
        sys::fdatasync(self.fd)
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.write_cursor >= self.checkpoint_threshold
    }

    /// Reset cursor to 0, increment epoch, atomic-store 0 at SAL base.
    pub fn checkpoint_reset(&mut self) {
        self.epoch += 1;
        self.write_cursor = 0;
        self.last_prefaulted = 0;
        unsafe { atomic_store_u64(self.ptr, 0); }
    }

    pub fn reset(&mut self, cursor: u64, epoch: u32) {
        self.write_cursor = cursor;
        self.epoch = epoch;
    }

    pub fn cursor(&self) -> u64 { self.write_cursor }
    pub fn epoch(&self) -> u32 { self.epoch }
    pub fn mmap_size(&self) -> u64 { self.mmap_size }
    pub fn num_workers(&self) -> usize { self.num_workers }
    pub fn sal_fd(&self) -> i32 { self.fd }

}

/// A decoded SAL message for a specific worker.
pub struct SalMessage<'a> {
    pub lsn: u64,
    pub flags: u32,
    pub target_id: u32,
    pub epoch: u32,
    /// None = no data for this worker in this group.
    pub wire_data: Option<&'a [u8]>,
}

/// Worker + bootstrap read side of the SAL.
pub struct SalReader {
    ptr: *const u8,
    worker_id: u32,
    m2w_efd: i32,
}

unsafe impl Send for SalReader {}

impl SalReader {
    pub fn new(ptr: *const u8, worker_id: u32, _mmap_size: usize, m2w_efd: i32) -> Self {
        SalReader { ptr, worker_id, m2w_efd }
    }

    /// Read next group at `cursor`. Returns None if no message (payload_size==0).
    /// Does NOT check epoch — caller decides policy.
    /// On success returns (message, new_cursor).
    ///
    /// # Safety note
    /// The returned `SalMessage` contains a slice into the SAL mmap region.
    /// The mmap is process-lifetime, so `'static` is safe as long as the SAL
    /// is not unmapped while messages are in use (which never happens in
    /// normal operation).
    pub fn try_read(&self, cursor: u64) -> Option<(SalMessage<'static>, u64)> {
        let result = unsafe { sal_read_group_header(self.ptr, cursor, self.worker_id) };
        if result.advance == 0 {
            return None;
        }
        let new_cursor = cursor + result.advance;
        let wire_data = if result.status == 0 && result.data_size > 0 && !result.data_ptr.is_null() {
            Some(unsafe {
                std::slice::from_raw_parts(result.data_ptr, result.data_size as usize)
            })
        } else {
            None
        };
        Some((
            SalMessage {
                lsn: result.lsn,
                flags: result.flags,
                target_id: result.target_id,
                epoch: result.epoch,
                wire_data,
            },
            new_cursor,
        ))
    }

    /// Wait for master signal, with timeout.
    pub fn wait(&self, timeout_ms: i32) -> i32 {
        ipc_sys::eventfd_wait(self.m2w_efd, timeout_ms)
    }
}

/// Worker's write side of a single W2M ring. Each worker owns exactly
/// one `W2mWriter`; the SPSC tail-chasing publish protocol (stamp size
/// prefix → copy payload → Release-store the write cursor) is correct
/// only with a single producer. Multiple concurrent writers would
/// interleave payload copies and corrupt the ring. The type is `!Sync`
/// (via `*mut u8`), so the type system already prevents `&W2mWriter`
/// from crossing threads.
///
/// On a full ring, `send_encoded` parks on `writer_seq` via a raw v1
/// `FUTEX_WAIT` (shared, no `FUTEX_PRIVATE_FLAG` — W2M is MAP_SHARED
/// across fork). The master's consume path wakes us after advancing
/// `read_cursor` when it observes `FLAG_WRITER_PARKED`.
pub struct W2mWriter {
    region_ptr: *mut u8,
}

unsafe impl Send for W2mWriter {}

impl W2mWriter {
    pub fn new(region_ptr: *mut u8, region_size: u64) -> Self {
        // One-shot sanity check: the caller's `region_size` must match
        // the ring header's own `capacity`. Runs once at construction
        // instead of per-send.
        let hdr = unsafe { W2mRingHeader::from_raw(region_ptr as *const u8) };
        assert_eq!(
            hdr.capacity(), region_size,
            "W2mWriter region_size must match ring header capacity",
        );
        W2mWriter { region_ptr }
    }

    /// Encode wire data into the W2M ring. Blocks on `writer_seq` if
    /// the ring is full until the master advances `read_cursor`, then
    /// retries. On successful publish, bumps `reader_seq` and issues a
    /// `FUTEX_WAKE` if `FLAG_MASTER_PARKED` is set.
    ///
    /// The loop is bounded only by backpressure — a dead master would
    /// park the writer forever, but the `worker_watcher` task in the
    /// executor traps master death and tears the worker down first
    /// (see `executor::worker_watcher`).
    ///
    /// # Panics
    /// Panics if `sz > MAX_W2M_MSG`. Without this guard the ring would
    /// return `TryReserve::Full` for every attempt and `send_encoded`
    /// would park indefinitely on `writer_seq` — a silent deadlock for
    /// a caller bug. An oversized message is always a programming error
    /// (the wire protocol caps payloads well below 256 MiB), so failing
    /// loudly at the source is the right behavior.
    pub fn send_encoded(&self, sz: usize, encode_fn: impl FnOnce(&mut [u8])) {
        assert!(
            (sz as u64) <= w2m_ring::MAX_W2M_MSG,
            "W2mWriter::send_encoded: sz={} exceeds MAX_W2M_MSG={}",
            sz, w2m_ring::MAX_W2M_MSG,
        );
        let hdr = unsafe { W2mRingHeader::from_raw(self.region_ptr as *const u8) };

        // Loop until a reservation is granted, then encode + commit.
        // Encoding happens exactly once, after the slot is secured.
        let reservation = loop {
            let r = unsafe { w2m_ring::try_reserve(hdr, self.region_ptr, sz) };
            match r {
                TryReserve::Ok(r) => break r,
                TryReserve::Full => {
                    // Park. Protocol: sample writer_seq, publish
                    // WRITER_PARKED (AcqRel — ensures the flag is
                    // visible before we look at the cursor again),
                    // re-check whether the ring now has room (covers
                    // the race where the consumer drained between
                    // our try_reserve and fetch_or), and only then
                    // block in FUTEX_WAIT. The wake path (reader's
                    // writer_seq bump) synchronizes with the
                    // Acquire-loaded expected value.
                    let expected = hdr.writer_seq().load(Ordering::Acquire);
                    hdr.waiter_flags()
                        .fetch_or(FLAG_WRITER_PARKED, Ordering::AcqRel);
                    let room_now = unsafe { w2m_ring::has_room(hdr, sz) };
                    if !room_now {
                        let _ = ipc_sys::futex_wait_u32(
                            hdr.writer_seq() as *const AtomicU32,
                            expected,
                            -1,
                        );
                    }
                    hdr.waiter_flags()
                        .fetch_and(!FLAG_WRITER_PARKED, Ordering::AcqRel);
                }
            }
        };

        // Encode into the reserved slot (at most once) and commit.
        unsafe {
            if reservation.slot_len > 0 {
                let slice = std::slice::from_raw_parts_mut(
                    reservation.slot_ptr, reservation.slot_len,
                );
                encode_fn(slice);
            }
            w2m_ring::commit(hdr, &reservation);
        }

        // Bump reader_seq + wake master if parked.
        hdr.reader_seq().fetch_add(1, Ordering::Release);
        if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_MASTER_PARKED != 0 {
            let _ = ipc_sys::futex_wake_u32(
                hdr.reader_seq() as *const AtomicU32, 1,
            );
        }
    }
}

/// Master's read side of W2M. Thin adapter over `w2m_ring::try_consume`.
pub struct W2mReceiver {
    region_ptrs: Vec<*mut u8>,
}

unsafe impl Send for W2mReceiver {}

impl W2mReceiver {
    pub fn new(region_ptrs: Vec<*mut u8>) -> Self {
        W2mReceiver { region_ptrs }
    }

    /// Pointer to worker `w`'s ring region.
    #[inline]
    pub fn region_ptr(&self, worker: usize) -> *mut u8 {
        self.region_ptrs[worker]
    }

    /// Access worker `w`'s ring header.
    ///
    /// # Safety
    /// `worker` must be < `num_workers`.
    #[inline]
    pub unsafe fn header(&self, worker: usize) -> &'static W2mRingHeader {
        W2mRingHeader::from_raw(self.region_ptrs[worker] as *const u8)
    }

    /// Closure-based consume. The slot bytes are exposed to `f` while
    /// the read cursor still names them; the cursor only advances after
    /// `f` returns. This is the structural form of the SPSC consume
    /// protocol — there is no way for a caller to advance the cursor
    /// before they are done with the slot.
    ///
    /// `T` cannot borrow from the slot (the slot's lifetime is local to
    /// this function and `T` must outlive it), so callers that need
    /// data after the cursor advances must copy bytes out inside `f`.
    pub fn try_read_with<F, T>(&self, worker: usize, f: F) -> Option<T>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let hdr = unsafe { self.header(worker) };
        let cursor = hdr.read_cursor().load(Ordering::Acquire);
        let (ptr, sz, new_rc) = unsafe {
            w2m_ring::try_consume(hdr, self.region_ptrs[worker] as *const u8, cursor)?
        };

        let result = {
            let slot = unsafe { std::slice::from_raw_parts(ptr, sz as usize) };
            f(slot)
            // `slot` is dropped here, before the cursor advances. The
            // borrow checker prevents `T` from carrying any reference
            // into `slot`.
        };

        hdr.read_cursor().store(new_rc, Ordering::Release);
        hdr.writer_seq().fetch_add(1, Ordering::Release);
        if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_WRITER_PARKED != 0 {
            let _ = ipc_sys::futex_wake_u32(
                hdr.writer_seq() as *const AtomicU32, 1,
            );
        }

        Some(result)
    }

    /// Decode one reply from worker `w`.
    ///
    /// Control-only replies (the common case: tick ACKs, push ACKs,
    /// checkpoint ACKs, exchange view signals) decode in place against
    /// the ring slot — no heap copy. Replies that carry a `data_batch`
    /// fall back to a heap copy because `Batch` holds raw pointers into
    /// the source bytes and the slot is released to the producer as
    /// soon as this function returns.
    pub fn try_read(&self, worker: usize) -> Option<DecodedWire> {
        enum Outcome {
            NoData(DecodedWire),
            HasData(Vec<u8>),
            BadWire,
        }
        let outcome = self.try_read_with(worker, |slot| match decode_wire(slot) {
            Err(_) => Outcome::BadWire,
            Ok(d) if d.data_batch.is_none() => Outcome::NoData(d),
            Ok(_) => Outcome::HasData(slot.to_vec()),
        })?;
        match outcome {
            Outcome::NoData(d) => Some(d),
            Outcome::HasData(owned) => {
                let mut d = decode_wire(&owned).ok()?;
                d.batch_backing = Some(owned);
                Some(d)
            }
            Outcome::BadWire => None,
        }
    }

    /// Block until worker `w` publishes, or `timeout_ms` elapses. Used
    /// by bootstrap paths that cannot go through the reactor's
    /// FUTEX_WAITV multiplex.
    ///
    /// Parking ordering (matches `Reactor::refresh_futex_waitv_vals` so
    /// the two W2M wait paths are shape-equivalent):
    ///
    /// 1. `fetch_or(FLAG_MASTER_PARKED)` — publish park intent FIRST so
    ///    any writer that observes the flag will wake us.
    /// 2. Snapshot `expected = reader_seq`.
    /// 3. Unread-data short-circuit: if `write_cursor != read_cursor`
    ///    there's already a pending publish — skip the futex wait.
    /// 4. Otherwise `FUTEX_WAIT(reader_seq, expected, timeout_ms)`.
    pub fn wait_for(&self, worker: usize, timeout_ms: i32) -> i32 {
        let hdr = unsafe { self.header(worker) };
        hdr.waiter_flags().fetch_or(FLAG_MASTER_PARKED, Ordering::AcqRel);
        let expected = hdr.reader_seq().load(Ordering::Acquire);
        let wc = hdr.write_cursor().load(Ordering::Acquire);
        let rc_cur = hdr.read_cursor().load(Ordering::Acquire);
        let rc = if wc != rc_cur {
            0 // data already pending; no wait needed
        } else {
            ipc_sys::futex_wait_u32(
                hdr.reader_seq() as *const AtomicU32,
                expected,
                timeout_ms,
            )
        };
        hdr.waiter_flags().fetch_and(!FLAG_MASTER_PARKED, Ordering::AcqRel);
        rc
    }

    pub fn num_workers(&self) -> usize { self.region_ptrs.len() }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: allocate an anonymous mmap region.
    unsafe fn alloc_mmap(size: usize) -> *mut u8 {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1,
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        // Zero it
        std::ptr::write_bytes(ptr as *mut u8, 0, size);
        ptr as *mut u8
    }

    unsafe fn free_mmap(ptr: *mut u8, size: usize) {
        libc::munmap(ptr as *mut libc::c_void, size);
    }

    fn make_test_data(val: u8, len: usize) -> Vec<u8> {
        vec![val; len]
    }

    #[test]
    fn test_sal_round_trip() {
        unsafe {
            let size = 1 << 20; // 1MB
            let ptr = alloc_mmap(size);

            let nw = 4u32;
            let bufs: Vec<Vec<u8>> = vec![
                make_test_data(0xAA, 100),
                vec![],  // empty
                make_test_data(0xBB, 200),
                make_test_data(0xCC, 50),
            ];

            let mut ptrs: Vec<*const u8> = Vec::new();
            let mut sizes: Vec<u32> = Vec::new();
            for b in &bufs {
                if b.is_empty() {
                    ptrs.push(std::ptr::null());
                    sizes.push(0);
                } else {
                    ptrs.push(b.as_ptr());
                    sizes.push(b.len() as u32);
                }
            }

            let res = sal_write_group(
                ptr, 0, nw, 42, 100, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res.status, 0);
            assert!(res.new_cursor > 0);

            // Read back each worker's data
            for w in 0..4u32 {
                let rr = sal_read_group_header(ptr, 0, w);
                assert_eq!(rr.lsn, 100);
                assert_eq!(rr.target_id, 42);
                assert_eq!(rr.epoch, 1);
                assert_eq!(rr.advance, res.new_cursor);

                if bufs[w as usize].is_empty() {
                    assert_eq!(rr.status, 1); // no data
                } else {
                    assert_eq!(rr.status, 0); // has data
                    let data = std::slice::from_raw_parts(
                        rr.data_ptr, rr.data_size as usize
                    );
                    assert_eq!(data, bufs[w as usize].as_slice());
                }
            }

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_sal_unicast_isolation() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let nw = 4u32;

            // Only worker 2 gets data
            let buf = make_test_data(0xDD, 128);
            let mut ptrs = vec![std::ptr::null(); 4];
            let mut sizes = vec![0u32; 4];
            ptrs[2] = buf.as_ptr();
            sizes[2] = buf.len() as u32;

            let res = sal_write_group(
                ptr, 0, nw, 10, 1, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res.status, 0);

            // Workers 0,1,3 see no data
            for w in [0u32, 1, 3] {
                let rr = sal_read_group_header(ptr, 0, w);
                assert_eq!(rr.status, 1);
                assert!(rr.advance > 0);
            }
            // Worker 2 sees data
            let rr = sal_read_group_header(ptr, 0, 2);
            assert_eq!(rr.status, 0);
            let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
            assert_eq!(data, buf.as_slice());

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_sal_multiple_groups() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let nw = 2u32;

            let mut cursor = 0u64;
            for g in 0..3u64 {
                let buf = make_test_data((g + 1) as u8, 64);
                let ptrs = [buf.as_ptr(), std::ptr::null()];
                let sizes = [buf.len() as u32, 0u32];

                let res = sal_write_group(
                    ptr, cursor, nw, g as u32, g * 10, 0, 1, size as u64,
                    ptrs.as_ptr(), sizes.as_ptr(),
                );
                assert_eq!(res.status, 0);
                cursor = res.new_cursor;
            }

            // Read all three groups back
            let mut rc = 0u64;
            for g in 0..3u64 {
                let rr = sal_read_group_header(ptr, rc, 0);
                assert_eq!(rr.status, 0);
                assert_eq!(rr.lsn, g * 10);
                assert_eq!(rr.target_id, g as u32);
                let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
                assert_eq!(data, vec![(g + 1) as u8; 64].as_slice());
                rc += rr.advance;
            }
            // No more messages
            let rr = sal_read_group_header(ptr, rc, 0);
            assert_eq!(rr.status, -1);

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_sal_epoch_write_read() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);

            let buf = make_test_data(0x11, 32);
            let ptrs = [buf.as_ptr()];
            let sizes = [buf.len() as u32];

            let res = sal_write_group(
                ptr, 0, 1, 0, 0, 0, 42, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res.status, 0);

            let rr = sal_read_group_header(ptr, 0, 0);
            assert_eq!(rr.epoch, 42);

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_sal_full_error() {
        unsafe {
            // Tiny region that can't fit a message group
            let size = 256;
            let ptr = alloc_mmap(size);

            let buf = make_test_data(0xFF, 100);
            let ptrs = [buf.as_ptr()];
            let sizes = [buf.len() as u32];

            let res = sal_write_group(
                ptr, 0, 1, 0, 0, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res.status, -1);

            free_mmap(ptr, size);
        }
    }

    // (W2M-specific tests — round-trip, wrap, corrupt-prefix rejection,
    // zero-copy — now live in `crate::w2m_ring::tests`.)

    #[test]
    fn test_sal_cross_process() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let efd = crate::ipc_sys::eventfd_create();
            assert!(efd >= 0);

            let pid = libc::fork();
            if pid == 0 {
                // Child (master): write a SAL group, signal parent
                let buf = make_test_data(0x77, 128);
                let ptrs = [buf.as_ptr()];
                let sizes = [buf.len() as u32];
                sal_write_group(
                    ptr, 0, 1, 99, 555, 0, 1, size as u64,
                    ptrs.as_ptr(), sizes.as_ptr(),
                );
                crate::ipc_sys::eventfd_signal(efd);
                libc::_exit(0);
            }

            // Parent (worker): wait for signal, read SAL
            let r = crate::ipc_sys::eventfd_wait(efd, 5000);
            assert!(r > 0, "eventfd timed out");

            let rr = sal_read_group_header(ptr, 0, 0);
            assert_eq!(rr.status, 0);
            assert_eq!(rr.lsn, 555);
            assert_eq!(rr.target_id, 99);
            let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
            assert_eq!(data, vec![0x77u8; 128].as_slice());

            let mut status = 0i32;
            libc::waitpid(pid, &mut status, 0);
            free_mmap(ptr, size);
            libc::close(efd);
        }
    }

    #[test]
    fn test_sal_checkpoint_reset() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);

            // Write with epoch=1
            let buf1 = make_test_data(0x11, 32);
            let ptrs = [buf1.as_ptr()];
            let sizes = [buf1.len() as u32];
            let res = sal_write_group(
                ptr, 0, 1, 0, 0, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res.status, 0);

            // "Checkpoint reset": zero the region, write with epoch=2
            std::ptr::write_bytes(ptr, 0, size);
            let buf2 = make_test_data(0x22, 32);
            let ptrs2 = [buf2.as_ptr()];
            let sizes2 = [buf2.len() as u32];
            let res2 = sal_write_group(
                ptr, 0, 1, 0, 0, 0, 2, size as u64,
                ptrs2.as_ptr(), sizes2.as_ptr(),
            );
            assert_eq!(res2.status, 0);

            // Reader sees epoch=2
            let rr = sal_read_group_header(ptr, 0, 0);
            assert_eq!(rr.epoch, 2);
            let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
            assert_eq!(data, vec![0x22u8; 32].as_slice());

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_sal_epoch_fence() {
        // Verify that epoch mismatch can be detected by the reader
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);

            let buf = make_test_data(0x33, 32);
            let ptrs = [buf.as_ptr()];
            let sizes = [buf.len() as u32];

            // Write two groups with different epochs
            let res1 = sal_write_group(
                ptr, 0, 1, 0, 0, 0, 5, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            let res2 = sal_write_group(
                ptr, res1.new_cursor, 1, 0, 0, 0, 6, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res2.status, 0);

            // Reader can distinguish epochs
            let rr1 = sal_read_group_header(ptr, 0, 0);
            assert_eq!(rr1.epoch, 5);
            let rr2 = sal_read_group_header(ptr, rr1.advance, 0);
            assert_eq!(rr2.epoch, 6);

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_sal_cross_process_checkpoint() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let efd = crate::ipc_sys::eventfd_create();
            let efd2 = crate::ipc_sys::eventfd_create();
            assert!(efd >= 0 && efd2 >= 0);

            let pid = libc::fork();
            if pid == 0 {
                // Child: phase 1 — write epoch=1, signal
                let buf = make_test_data(0xAA, 64);
                let ptrs = [buf.as_ptr()];
                let sizes = [buf.len() as u32];
                sal_write_group(
                    ptr, 0, 1, 0, 10, 0, 1, size as u64,
                    ptrs.as_ptr(), sizes.as_ptr(),
                );
                crate::ipc_sys::eventfd_signal(efd);

                // Wait for parent to read phase 1
                crate::ipc_sys::eventfd_wait(efd2, 5000);

                // Phase 2: reset and write epoch=2
                std::ptr::write_bytes(ptr, 0, size);
                let buf2 = make_test_data(0xBB, 64);
                let ptrs2 = [buf2.as_ptr()];
                let sizes2 = [buf2.len() as u32];
                sal_write_group(
                    ptr, 0, 1, 0, 20, 0, 2, size as u64,
                    ptrs2.as_ptr(), sizes2.as_ptr(),
                );
                crate::ipc_sys::eventfd_signal(efd);
                libc::_exit(0);
            }

            // Parent: read phase 1
            crate::ipc_sys::eventfd_wait(efd, 5000);
            let rr1 = sal_read_group_header(ptr, 0, 0);
            assert_eq!(rr1.epoch, 1);
            assert_eq!(rr1.lsn, 10);
            crate::ipc_sys::eventfd_signal(efd2);

            // Read phase 2
            crate::ipc_sys::eventfd_wait(efd, 5000);
            let rr2 = sal_read_group_header(ptr, 0, 0);
            assert_eq!(rr2.epoch, 2);
            assert_eq!(rr2.lsn, 20);

            let mut status = 0i32;
            libc::waitpid(pid, &mut status, 0);
            free_mmap(ptr, size);
            libc::close(efd);
            libc::close(efd2);
        }
    }

    // -----------------------------------------------------------------------
    // Wire protocol tests
    // -----------------------------------------------------------------------

    /// Helper: build a simple 2-column schema (U64 pk + U64 payload).
    fn simple_schema() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        sd.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        sd.columns[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        sd
    }

    /// Helper: build a 3-column schema (U64 pk + U64 val + String name).
    fn string_schema() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        sd.columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        sd.columns[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        sd.columns[2] = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
        sd
    }

    /// Helper: build a simple 1-row batch.
    fn make_simple_batch(pk: u64, val: u64) -> Batch {
        let sd = simple_schema();
        let mut b = Batch::with_schema(sd, 1);
        b.extend_pk_lo(&pk.to_le_bytes());
        b.extend_pk_hi(&0u64.to_le_bytes());
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count = 1;
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_encode_decode_roundtrip_no_data() {
        let wire = encode_wire(
            42, 7, 0x100, 10, 20, 3, 0,
            STATUS_OK, b"",
            None, None, None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.target_id, 42);
        assert_eq!(decoded.control.client_id, 7);
        assert_eq!(decoded.control.flags & 0xFFFF, 0x100);
        assert_eq!(decoded.control.seek_pk_lo, 10);
        assert_eq!(decoded.control.seek_pk_hi, 20);
        assert_eq!(decoded.control.seek_col_idx, 3);
        assert_eq!(decoded.control.request_id, 0);
        assert_eq!(decoded.control.status, STATUS_OK);
        assert!(decoded.control.error_msg.is_empty());
        assert!(decoded.schema.is_none());
        assert!(decoded.data_batch.is_none());
    }

    #[test]
    fn test_encode_decode_roundtrip_with_schema() {
        let sd = simple_schema();
        let names: Vec<&[u8]> = vec![b"id", b"value"];
        let wire = encode_wire(
            1, 0, 0, 0, 0, 0, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert!(decoded.schema.is_some());
        let s = decoded.schema.unwrap();
        assert_eq!(s.num_columns, 2);
        assert_eq!(s.pk_index, 0);
        assert_eq!(s.columns[0].type_code, type_code::U64);
        assert_eq!(s.columns[1].type_code, type_code::U64);
        assert!(decoded.data_batch.is_none());
    }

    #[test]
    fn test_encode_decode_roundtrip_with_data() {
        let sd = simple_schema();
        let batch = make_simple_batch(100, 999);
        let names: Vec<&[u8]> = vec![b"id", b"val"];
        let wire = encode_wire(
            5, 0, 0, 0, 0, 0, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );
        let decoded = decode_wire(&wire).unwrap();
        assert!(decoded.schema.is_some());
        assert!(decoded.data_batch.is_some());
        let db = decoded.data_batch.as_ref().unwrap();
        assert_eq!(db.count, 1);
        // Check pk_lo
        let pk = u64::from_le_bytes(db.pk_lo_data()[0..8].try_into().unwrap());
        assert_eq!(pk, 100);
        // Check payload col 0 (val)
        let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(val, 999);
        // Check sorted/consolidated flags roundtrip
        assert!(db.sorted);
        assert!(db.consolidated);
    }

    #[test]
    fn test_encode_decode_error_msg() {
        let wire = encode_wire(
            0, 0, 0, 0, 0, 0, 0,
            STATUS_ERROR, b"something went wrong",
            None, None, None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.status, STATUS_ERROR);
        assert_eq!(decoded.control.error_msg, b"something went wrong");
    }

    /// request_id round-trips through encode→decode for a non-zero value,
    /// covering the new payload column 7 introduced for reactor reply routing.
    #[test]
    fn test_encode_decode_request_id_nonzero() {
        let req_id: u64 = 0xDEAD_BEEF_CAFE_F00D;
        let wire = encode_wire(
            42, 7, 0, 0, 0, 0, req_id,
            STATUS_OK, b"",
            None, None, None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.request_id, req_id);
    }

    /// `u64::MAX` is the reserved broadcast-reply request_id; verify it
    /// round-trips intact (no sign-extension or truncation).
    #[test]
    fn test_encode_decode_request_id_max() {
        let wire = encode_wire(
            1, 2, 3, 4, 5, 6, u64::MAX,
            STATUS_OK, b"",
            None, None, None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.request_id, u64::MAX);
    }

    /// request_id must survive the error-message path: the new null bit
    /// shifted from position 7 to 8, so the error decode must read the right
    /// bit and the request_id field must still be populated.
    #[test]
    fn test_encode_decode_request_id_with_error() {
        let req_id: u64 = 0x1122_3344_5566_7788;
        let wire = encode_wire(
            10, 20, 30, 40, 50, 60, req_id,
            STATUS_ERROR, b"boom",
            None, None, None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.status, STATUS_ERROR);
        assert_eq!(decoded.control.request_id, req_id);
        assert_eq!(decoded.control.error_msg, b"boom");
    }

    /// `wire_size` must include the new request_id payload column. The
    /// roundtrip equality is a stronger check than a literal byte count
    /// since it catches both under-sized and over-sized buffers.
    #[test]
    fn test_wire_size_includes_request_id() {
        let sz = wire_size(STATUS_OK, b"", None, None, None);
        let wire = encode_wire(
            0, 0, 0, 0, 0, 0, 0xAAAA_BBBB_CCCC_DDDD,
            STATUS_OK, b"",
            None, None, None,
        );
        assert_eq!(sz, wire.len());
        let sz2 = wire_size(STATUS_ERROR, b"err", None, None, None);
        let wire2 = encode_wire(
            0, 0, 0, 0, 0, 0, u64::MAX,
            STATUS_ERROR, b"err",
            None, None, None,
        );
        assert_eq!(sz2, wire2.len());
    }

    #[test]
    fn test_schema_roundtrip_with_names() {
        let sd = string_schema();
        let names: Vec<&[u8]> = vec![b"pk_col", b"int_col", b"name_col"];
        let batch = schema_to_batch(&sd, &names);
        let (sd2, names2) = batch_to_schema(&batch).unwrap();
        assert_eq!(sd2.num_columns, 3);
        assert_eq!(sd2.pk_index, 0);
        assert_eq!(sd2.columns[0].type_code, type_code::U64);
        assert_eq!(sd2.columns[1].type_code, type_code::U64);
        assert_eq!(sd2.columns[2].type_code, type_code::STRING);
        assert_eq!(sd2.columns[2].size, 16);
        assert_eq!(names2[0], b"pk_col");
        assert_eq!(names2[1], b"int_col");
        assert_eq!(names2[2], b"name_col");
    }

    #[test]
    fn test_flag_has_data_requires_schema() {
        // Encode with data, then truncate after control block to remove schema+data.
        // Flags still has FLAG_HAS_SCHEMA set, so decode should fail with truncation.
        let sd = simple_schema();
        let batch = make_simple_batch(1, 2);
        let names: Vec<&[u8]> = vec![b"a", b"b"];
        let mut wire = encode_wire(
            0, 0, 0, 0, 0, 0, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );
        // The ctrl block flags has FLAG_HAS_DATA | FLAG_HAS_SCHEMA.
        // Truncate after ctrl block to remove schema+data, but keep the flags.
        let ctrl_size = u32::from_le_bytes(wire[16..20].try_into().unwrap()) as usize;
        wire.truncate(ctrl_size);
        // Now decode should fail with "schema block truncated" since FLAG_HAS_SCHEMA is set
        // but there's no schema block.
        let result = decode_wire(&wire);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_roundtrip_long_names() {
        // Test with column names longer than SHORT_STRING_THRESHOLD (12 bytes)
        let sd = simple_schema();
        let long_name = b"this_is_a_very_long_column_name_exceeding_twelve_bytes";
        let names: Vec<&[u8]> = vec![b"pk", long_name];
        let batch = schema_to_batch(&sd, &names);
        let (sd2, names2) = batch_to_schema(&batch).unwrap();
        assert_eq!(sd2.num_columns, 2);
        assert_eq!(names2[0], b"pk");
        assert_eq!(names2[1], long_name);
    }

    #[test]
    fn test_encode_decode_string_column() {
        let sd = string_schema();
        let mut batch = Batch::with_schema(sd, 2);

        // Row 1: short string
        batch.extend_pk_lo(&1u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &42u64.to_le_bytes()); // int col
        let st1 = encode_german_string(b"hello", &mut batch.blob);
        batch.extend_col(1, &st1);
        batch.count += 1;

        // Row 2: long string
        batch.extend_pk_lo(&2u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &99u64.to_le_bytes());
        let long_str = b"this is a long string that exceeds twelve bytes";
        let st2 = encode_german_string(long_str, &mut batch.blob);
        batch.extend_col(1, &st2);
        batch.count += 1;

        let names: Vec<&[u8]> = vec![b"id", b"val", b"name"];
        let wire = encode_wire(
            10, 0, 0, 0, 0, 0, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );
        let decoded = decode_wire(&wire).unwrap();
        let db = decoded.data_batch.as_ref().unwrap();
        assert_eq!(db.count, 2);

        // Verify short string roundtrip
        let mut s1 = [0u8; 16];
        s1.copy_from_slice(&db.col_data(1)[0..16]);
        let str1 = decode_german_string(&s1, &db.blob);
        assert_eq!(str1, b"hello");

        // Verify long string roundtrip
        let mut s2 = [0u8; 16];
        s2.copy_from_slice(&db.col_data(1)[16..32]);
        let str2 = decode_german_string(&s2, &db.blob);
        assert_eq!(str2, long_str);
    }

    #[test]
    fn wire_size_matches_encode() {
        // No data
        let sz = wire_size(STATUS_OK, b"", None, None, None);
        let wire = encode_wire(42, 7, 0x100, 10, 20, 3, 0, STATUS_OK, b"", None, None, None);
        assert_eq!(sz, wire.len(), "wire_size mismatch (no data)");

        // With schema only
        let sd = simple_schema();
        let names: Vec<&[u8]> = vec![b"id", b"val"];
        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), None);
        let wire = encode_wire(1, 0, 0, 0, 0, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), None);
        assert_eq!(sz, wire.len(), "wire_size mismatch (schema only)");

        // With data
        let batch = make_simple_batch(100, 999);
        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        let wire = encode_wire(5, 0, 0, 0, 0, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        assert_eq!(sz, wire.len(), "wire_size mismatch (with data)");

        // With error message
        let sz = wire_size(STATUS_ERROR, b"something went wrong", None, None, None);
        let wire = encode_wire(0, 0, 0, 0, 0, 0, 0, STATUS_ERROR, b"something went wrong", None, None, None);
        assert_eq!(sz, wire.len(), "wire_size mismatch (error msg)");
    }

    #[test]
    fn encode_wire_into_roundtrip() {
        let sd = simple_schema();
        let batch = make_simple_batch(100, 999);
        let names: Vec<&[u8]> = vec![b"id", b"val"];

        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        let mut buf = vec![0u8; sz];
        let written = encode_wire_into(
            &mut buf, 0,
            5, 0, 0, 0, 0, 0, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );
        assert_eq!(written, sz);

        let decoded = decode_wire(&buf).unwrap();
        assert_eq!(decoded.control.target_id, 5);
        assert!(decoded.schema.is_some());
        let db = decoded.data_batch.as_ref().unwrap();
        assert_eq!(db.count, 1);
        let pk = u64::from_le_bytes(db.pk_lo_data()[0..8].try_into().unwrap());
        assert_eq!(pk, 100);
        let val = u64::from_le_bytes(db.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(val, 999);
    }

    #[test]
    fn encode_wire_into_matches_encode_wire() {
        let sd = simple_schema();
        let batch = make_simple_batch(42, 123);
        let names: Vec<&[u8]> = vec![b"id", b"val"];

        let wire = encode_wire(
            10, 3, 0x200, 5, 6, 7, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );

        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        let mut buf = vec![0u8; sz];
        let written = encode_wire_into(
            &mut buf, 0,
            10, 3, 0x200, 5, 6, 7, 0,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );
        assert_eq!(written, wire.len());
        assert_eq!(buf, wire, "encode_wire_into should produce identical bytes");
    }

    /// Truncating a wire buffer below the new region-14 extent must surface
    /// as a decode error rather than an out-of-bounds read. Stage 0 widens
    /// the control schema, so we explicitly exercise the new minimum size.
    #[test]
    fn test_decode_truncated_control_block_returns_err() {
        let wire = encode_wire(
            1, 2, 3, 4, 5, 6, 7,
            STATUS_OK, b"",
            None, None, None,
        );
        // Drop the last 32 bytes — well past where region 13 (blob) starts.
        let truncated = &wire[..wire.len().saturating_sub(32)];
        let result = decode_wire(truncated);
        assert!(result.is_err(),
            "decode should reject truncated control block, got Ok");
    }

    /// Concurrent W2M writer + reader on a small ring with high publish
    /// pressure. Catches two failure modes at once:
    ///
    /// 1. Wrap-overwrite: virtual cursors must prevent the writer from
    ///    lapping unread data. A regression here surfaces as out-of-order
    ///    or missing `req_id`s.
    /// 2. Decode-after-release: `try_read` must copy the slot bytes
    ///    before advancing `read_cursor`. A regression here surfaces as
    ///    wrong `req_id`s (a later message's bytes overwriting the slot
    ///    we just claimed but haven't decoded yet).
    ///
    /// 8 KiB cap forces the writer to wrap many times for 2000 msgs of
    /// ~280 B each. Worker thread is the producer, main thread spins on
    /// `try_read` after each publish, asserting strict ordering.
    #[test]
    fn test_w2m_concurrent_publish_consume_ordered() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        const CAP: usize = 8 * 1024; // 8 KiB → many wraps
        const N: u64 = 2_000;

        let region = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                CAP,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            ) as *mut u8
        };
        assert!(!region.is_null());
        unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

        // SAFETY: the writer thread takes exclusive ownership of `region`
        // for publishes; the reader (this thread) only reads via
        // W2mReceiver. The mmap is process-lifetime.
        let region_addr = region as usize;
        let done = Arc::new(AtomicBool::new(false));
        let done_w = Arc::clone(&done);
        let writer_thread = std::thread::spawn(move || {
            let writer = W2mWriter::new(region_addr as *mut u8, CAP as u64);
            let sz = wire_size(STATUS_OK, b"", None, None, None);
            for req_id in 1..=N {
                writer.send_encoded(sz, |buf| {
                    encode_wire_into(
                        buf, 0, 0, 0, 0,
                        0, 0, 0, req_id, STATUS_OK, b"", None, None, None,
                    );
                });
            }
            done_w.store(true, Ordering::Release);
        });

        let receiver = W2mReceiver::new(vec![region]);
        let mut next_expected: u64 = 1;
        let started = std::time::Instant::now();
        while next_expected <= N {
            if let Some(decoded) = receiver.try_read(0) {
                assert_eq!(
                    decoded.control.request_id, next_expected,
                    "msg ordering broke at req_id={}: writer-cross-reader \
                     or decode-after-release race",
                    next_expected,
                );
                next_expected += 1;
            } else {
                if done.load(Ordering::Acquire) && next_expected <= N {
                    if receiver.try_read(0).is_none() {
                        panic!(
                            "writer done but only {}/{} msgs received",
                            next_expected - 1, N,
                        );
                    }
                }
                std::thread::yield_now();
            }
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "test timed out at req_id={}", next_expected,
            );
        }

        writer_thread.join().expect("writer thread");
        unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
    }

    /// Larger-message variant of the concurrent stress: ~4 KiB messages
    /// in a 64 KiB cap (only ~15 fit at a time → constant
    /// backpressure + frequent SKIP wraps with realistic-size payloads).
    #[test]
    fn test_w2m_concurrent_large_messages_ordered() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        const CAP: usize = 64 * 1024;
        const N: u64 = 500;
        // Fabricate a payload that pushes the wire size near 4 KiB by
        // attaching a long error_msg.
        let pad: Vec<u8> = vec![b'x'; 4000];

        let region = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                CAP,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            ) as *mut u8
        };
        assert!(!region.is_null());
        unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

        let region_addr = region as usize;
        let done = Arc::new(AtomicBool::new(false));
        let done_w = Arc::clone(&done);
        let pad_w = pad.clone();
        let writer_thread = std::thread::spawn(move || {
            let writer = W2mWriter::new(region_addr as *mut u8, CAP as u64);
            let sz = wire_size(STATUS_OK, &pad_w, None, None, None);
            for req_id in 1..=N {
                writer.send_encoded(sz, |buf| {
                    encode_wire_into(
                        buf, 0, 0, 0, 0,
                        0, 0, 0, req_id, STATUS_OK, &pad_w, None, None, None,
                    );
                });
            }
            done_w.store(true, Ordering::Release);
        });

        let receiver = W2mReceiver::new(vec![region]);
        let mut next_expected: u64 = 1;
        let started = std::time::Instant::now();
        while next_expected <= N {
            if let Some(decoded) = receiver.try_read(0) {
                assert_eq!(
                    decoded.control.request_id, next_expected,
                    "large-msg ordering broke at req_id={}", next_expected,
                );
                assert_eq!(
                    decoded.control.error_msg, pad,
                    "payload corrupted at req_id={}", next_expected,
                );
                next_expected += 1;
            } else {
                std::thread::yield_now();
            }
            let _ = done.load(Ordering::Acquire);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(30),
                "test timed out at req_id={}", next_expected,
            );
        }

        writer_thread.join().expect("writer thread");
        unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
    }

    /// Regression: `send_encoded` must panic on `sz > MAX_W2M_MSG`
    /// rather than silently park forever on a ring that can never
    /// fit the message. The panic is a *programmer-bug* guard;
    /// `w2m_ring::try_reserve` returns `Full` for oversized reservations
    /// and the writer's park-and-retry loop would otherwise spin
    /// indefinitely.
    #[test]
    fn test_w2m_writer_rejects_oversized() {
        const CAP: usize = 64 * 1024;
        let region = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                CAP,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            ) as *mut u8
        };
        assert!(!region.is_null());
        unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

        let region_addr = region as usize;
        let (tx, rx) = std::sync::mpsc::channel::<bool>();
        std::thread::spawn(move || {
            let writer = W2mWriter::new(region_addr as *mut u8, CAP as u64);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.send_encoded(
                    (w2m_ring::MAX_W2M_MSG + 1) as usize,
                    |_| {},
                );
            }));
            let _ = tx.send(result.is_err());
        });

        match rx.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(true) => { /* panicked as expected */ }
            Ok(false) => panic!(
                "send_encoded returned normally on oversized sz — expected panic"
            ),
            Err(_) => panic!(
                "send_encoded deadlocked on oversized sz — regression: \
                 must panic instead of spinning on Full forever"
            ),
        }

        unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
    }

    /// Control-only W2M replies (the hot path: tick/push/checkpoint
    /// ACKs) must decode zero-copy against the ring slot —
    /// `batch_backing` must be `None` because no interior pointers
    /// reference the slot, and allocating a backing buffer per ACK
    /// would burn one alloc + one memcpy on every steady-state reply.
    #[test]
    fn test_w2m_control_only_reply_has_no_backing() {
        const CAP: usize = 64 * 1024;
        let region = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                CAP,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            ) as *mut u8
        };
        assert!(!region.is_null());
        unsafe { w2m_ring::init_region_for_tests(region, CAP as u64); }

        let writer = W2mWriter::new(region, CAP as u64);
        let sz = wire_size(STATUS_OK, b"", None, None, None);
        writer.send_encoded(sz, |buf| {
            encode_wire_into(
                buf, 0, 0, 0, 0,
                0, 0, 0, 42, STATUS_OK, b"", None, None, None,
            );
        });

        let receiver = W2mReceiver::new(vec![region]);
        let decoded = receiver.try_read(0).expect("ACK must decode");
        assert_eq!(decoded.control.request_id, 42);
        assert!(decoded.data_batch.is_none(),
            "control-only ACK must have no data_batch");
        assert!(decoded.batch_backing.is_none(),
            "control-only ACK must skip the heap-copy path");

        unsafe { libc::munmap(region as *mut libc::c_void, CAP); }
    }

}
