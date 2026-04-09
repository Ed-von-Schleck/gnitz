//! IPC transport layer: atomics, SAL (shared append-only log), W2M (worker→master),
//! and wire protocol encode/decode.
//!
//! Replaces gnitz/server/ipc.py transport + wire protocol logic.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::ipc_sys;
use crate::sys;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, type_size, encode_german_string, decode_german_string};
use crate::storage::{OwnedBatch, wal};
use crate::util::align8;

// ---------------------------------------------------------------------------
// Constants (must match ipc.py)
// ---------------------------------------------------------------------------

pub const MAX_WORKERS: usize = 64;
/// Group header: 32 fixed + MAX_WORKERS*4 offsets + MAX_WORKERS*4 sizes + 32 pad
pub(crate) const GROUP_HEADER_SIZE: usize = 576;
pub const SAL_MMAP_SIZE: usize = 1 << 30;
pub const W2M_REGION_SIZE: usize = 1 << 30;
pub const W2M_HEADER_SIZE: usize = 128;

pub use gnitz_wire::{
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, IPC_CONTROL_TID,
    STATUS_OK, STATUS_ERROR, META_FLAG_NULLABLE, META_FLAG_IS_PK,
};
pub const FLAG_BATCH_SORTED: u64 = 1 << 50;
pub const FLAG_BATCH_CONSOLIDATED: u64 = 1 << 51;

// SAL group header flags (u32 — engine-only flags after FLAG_SEEK_BY_INDEX)
gnitz_wire::cast_consts! { pub u32;
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH,
    FLAG_HAS_PK, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
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

/// CONTROL_SCHEMA: 8 U64 cols + 1 nullable String.
const CONTROL_SCHEMA_DESC: SchemaDescriptor = {
    let mut sd = SchemaDescriptor { num_columns: 9, pk_index: 0, columns: [ZERO_COL; 64] };
    sd.columns[0] = U64_COL; sd.columns[1] = U64_COL; sd.columns[2] = U64_COL;
    sd.columns[3] = U64_COL; sd.columns[4] = U64_COL; sd.columns[5] = U64_COL;
    sd.columns[6] = U64_COL; sd.columns[7] = U64_COL; sd.columns[8] = STR_COL_NULL;
    sd
};

/// Convert a SchemaDescriptor + column names into a META_SCHEMA OwnedBatch.
fn schema_to_batch(schema: &SchemaDescriptor, col_names: &[&[u8]]) -> OwnedBatch {
    let ncols = schema.num_columns as usize;
    let meta = META_SCHEMA_DESC;
    let mut batch = OwnedBatch::with_schema(meta, ncols);

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

/// Convert a META_SCHEMA OwnedBatch back into a SchemaDescriptor + column names.
fn batch_to_schema(batch: &OwnedBatch) -> Result<(SchemaDescriptor, Vec<Vec<u8>>), &'static str> {
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
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&OwnedBatch>,
) -> Vec<u8> {
    let sz = wire_size(status, error_msg, schema, col_names, data_batch);
    let mut buf = vec![0u8; sz];
    encode_wire_into(
        &mut buf, 0, target_id, client_id, flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx,
        status, error_msg, schema, col_names, data_batch,
    );
    buf
}

/// Max regions per batch: 4 fixed + up to 63 payload + 1 blob = 68.
const MAX_REGIONS_TOTAL: usize = 69;

/// Compute the WAL block size for a given batch without allocating.
fn batch_wal_block_size(batch: &OwnedBatch) -> usize {
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
    data_batch: Option<&OwnedBatch>,
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
    out: &mut [u8], offset: usize, batch: &OwnedBatch, table_id: u32,
) -> usize {
    let nr = batch.num_regions_total();
    let mut ptrs = [std::ptr::null::<u8>(); MAX_REGIONS_TOTAL];
    let mut sizes = [0u32; MAX_REGIONS_TOTAL];
    for i in 0..nr {
        ptrs[i] = batch.region_ptr(i);
        sizes[i] = batch.region_size(i) as u32;
    }
    let blob_size = batch.blob.len() as u64;
    let result = wal::encode(
        out, offset, 0, table_id, batch.count as u32,
        &ptrs[..nr], &sizes[..nr], blob_size,
    );
    assert!(result >= 0, "WAL encode_into failed");
    result as usize
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
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    col_names: Option<&[&[u8]]>,
    data_batch: Option<&OwnedBatch>,
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
        let cs = CONTROL_SCHEMA_DESC;
        let mut b = OwnedBatch::with_schema(cs, 1);
        let has_error = !error_msg.is_empty();
        let null_word: u64 = if has_error { 0 } else { 1u64 << 7 };
        b.extend_pk_lo(&0u64.to_le_bytes());
        b.extend_pk_hi(&0u64.to_le_bytes());
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(0, &(status as u64).to_le_bytes());
        b.extend_col(1, &client_id.to_le_bytes());
        b.extend_col(2, &target_id.to_le_bytes());
        b.extend_col(3, &wire_flags.to_le_bytes());
        b.extend_col(4, &seek_pk_lo.to_le_bytes());
        b.extend_col(5, &seek_pk_hi.to_le_bytes());
        b.extend_col(6, &seek_col_idx.to_le_bytes());
        let error_st = if has_error {
            encode_german_string(error_msg, &mut b.blob)
        } else {
            [0u8; 16]
        };
        b.extend_col(7, &error_st);
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
    pub error_msg: Vec<u8>,
}

/// Full decoded wire message.
pub struct DecodedWire {
    pub control: DecodedControl,
    pub schema: Option<SchemaDescriptor>,
    pub col_names: Vec<Vec<u8>>,
    pub data_batch: Option<Box<OwnedBatch>>,
}

/// Decode a WAL block from raw bytes into an OwnedBatch.
fn decode_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<OwnedBatch, &'static str> {
    let npc = schema.num_columns as usize - 1; // payload cols
    let expected_regions = 4 + npc + 1; // pk_lo, pk_hi, weight, nulls, payload cols, blob

    let mut lsn = 0u64;
    let mut tid = 0u32;
    let mut count = 0u32;
    let mut num_regions = 0u32;
    let mut blob_size = 0u64;
    let mut offsets = [0u64; 128];
    let mut sizes = [0u32; 128];

    let rc = wal::validate_and_parse(
        data, &mut lsn, &mut tid, &mut count, &mut num_regions,
        &mut blob_size, &mut offsets, &mut sizes, 128,
    );
    if rc != wal::WAL_OK {
        return Err("WAL block validation failed");
    }
    if (num_regions as usize) != expected_regions {
        return Err("WAL block region count mismatch");
    }

    // Build region pointers into the data buffer
    let nr = num_regions as usize;
    let mut ptrs = Vec::with_capacity(nr);
    let mut region_sizes = Vec::with_capacity(nr);
    for i in 0..nr {
        let off = offsets[i] as usize;
        let sz = sizes[i] as usize;
        if sz > 0 && off + sz <= data.len() {
            ptrs.push(unsafe { data.as_ptr().add(off) });
        } else {
            ptrs.push(std::ptr::null());
        }
        region_sizes.push(sizes[i]);
    }

    let mut batch = unsafe {
        OwnedBatch::from_regions(&ptrs, &region_sizes, count as usize, npc)
    };
    batch.schema = Some(*schema);
    Ok(batch)
}

/// Decode a full IPC wire message from raw bytes.
pub fn decode_wire(data: &[u8]) -> Result<DecodedWire, &'static str> {
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

    let cs = CONTROL_SCHEMA_DESC;
    let ctrl_batch = decode_wal_block(&data[..ctrl_size], &cs)?;

    // Extract control fields
    let status = u64::from_le_bytes(ctrl_batch.col_data(0)[0..8].try_into().unwrap()) as u32;
    let client_id = u64::from_le_bytes(ctrl_batch.col_data(1)[0..8].try_into().unwrap());
    let target_id = u64::from_le_bytes(ctrl_batch.col_data(2)[0..8].try_into().unwrap());
    let flags = u64::from_le_bytes(ctrl_batch.col_data(3)[0..8].try_into().unwrap());
    let seek_pk_lo = u64::from_le_bytes(ctrl_batch.col_data(4)[0..8].try_into().unwrap());
    let seek_pk_hi = u64::from_le_bytes(ctrl_batch.col_data(5)[0..8].try_into().unwrap());
    let seek_col_idx = u64::from_le_bytes(ctrl_batch.col_data(6)[0..8].try_into().unwrap());

    // error_msg: payload col 7, null bit 7
    let null_word = u64::from_le_bytes(ctrl_batch.null_bmp_data()[0..8].try_into().unwrap());
    let error_is_null = (null_word & (1u64 << 7)) != 0;
    let error_msg = if error_is_null {
        Vec::new()
    } else {
        let mut st = [0u8; 16];
        st.copy_from_slice(&ctrl_batch.col_data(7)[0..16]);
        decode_german_string(&st, &ctrl_batch.blob)
    };

    let control = DecodedControl {
        status, client_id, target_id, flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx, error_msg,
    };

    let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
    let has_data = (flags & FLAG_HAS_DATA) != 0;

    if has_data && !has_schema {
        return Err("FLAG_HAS_DATA without FLAG_HAS_SCHEMA");
    }

    let mut off = ctrl_size;
    let mut wire_schema: Option<SchemaDescriptor> = None;
    let mut col_names: Vec<Vec<u8>> = Vec::new();

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

        let meta = META_SCHEMA_DESC;
        let schema_batch = decode_wal_block(&data[off..off + schema_size], &meta)?;
        let (sd, names) = batch_to_schema(&schema_batch)?;
        wire_schema = Some(sd);
        col_names = names;
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
        Some(Box::new(batch))
    } else {
        None
    };

    Ok(DecodedWire { control, schema: wire_schema, col_names, data_batch })
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
// Raw read/write helpers for mmap regions
// ---------------------------------------------------------------------------

#[inline]
unsafe fn write_u64_raw(base: *mut u8, offset: usize, val: u64) {
    let p = base.add(offset) as *mut u64;
    p.write_unaligned(val);
}

#[inline]
unsafe fn read_u64_raw(base: *const u8, offset: usize) -> u64 {
    let p = base.add(offset) as *const u64;
    p.read_unaligned()
}

#[inline]
unsafe fn write_u32_raw(base: *mut u8, offset: usize, val: u32) {
    let p = base.add(offset) as *mut u32;
    p.write_unaligned(val);
}

#[inline]
unsafe fn read_u32_raw(base: *const u8, offset: usize) -> u32 {
    let p = base.add(offset) as *const u32;
    p.read_unaligned()
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
// W2M write (worker→master)
// ---------------------------------------------------------------------------

/// Write a pre-encoded wire buffer into a W2M region (test helper).
///
/// Returns the new write cursor, or -1 if the region is full.
///
/// # Safety
/// `region_ptr` must be a valid mmap pointer of at least `region_size` bytes.
#[cfg(test)]
pub(crate) unsafe fn w2m_write(
    region_ptr: *mut u8,
    data_ptr: *const u8,
    data_size: u32,
    region_size: u64,
) -> i64 {
    let wc = atomic_load_u64(region_ptr) as usize;
    let sz = data_size as usize;
    let total = 8 + align8(sz);

    if wc + total > region_size as usize {
        return -1;
    }

    // 8-byte size prefix
    write_u64_raw(region_ptr, wc, sz as u64);

    if sz > 0 && !data_ptr.is_null() {
        std::ptr::copy_nonoverlapping(
            data_ptr,
            region_ptr.add(wc + 8),
            sz,
        );
    }

    let new_wc = wc + total;
    atomic_store_u64(region_ptr, new_wc as u64);
    new_wc as i64
}

/// Read the next message from a W2M region.
///
/// Returns (data_ptr, data_size, new_read_cursor). data_ptr is non-owning.
/// Returns data_size=0 if no message (should not happen in normal flow).
///
/// # Safety
/// `region_ptr` must be a valid mmap pointer. `read_cursor` must be valid.
pub(crate) unsafe fn w2m_read(
    region_ptr: *const u8,
    read_cursor: u64,
) -> (u64, u32, *const u8) {
    let rc = read_cursor as usize;
    let size = read_u64_raw(region_ptr, rc) as usize;
    if size == 0 {
        return (read_cursor, 0, std::ptr::null());
    }
    let data_ptr = region_ptr.add(rc + 8);
    let new_rc = rc + 8 + align8(size);
    (new_rc as u64, size as u32, data_ptr)
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
    fn prefault_ahead(&mut self) {
        const PREFAULT_AHEAD: u64 = 2 * 1024 * 1024;
        let target = (self.write_cursor + PREFAULT_AHEAD).min(self.mmap_size);
        if target <= self.last_prefaulted { return; }
        let start = self.last_prefaulted.max(self.write_cursor);
        let len = (target - start) as usize;
        if len > 0 {
            sys::madvise_willneed(unsafe { self.ptr.add(start as usize) }, len);
            self.last_prefaulted = target;
        }
    }

    /// Encode per-worker wire data directly into SAL mmap. Does NOT sync/signal.
    pub fn write_group_direct(
        &mut self,
        target_id: u32,
        flags: u32,
        worker_batches: &[Option<&OwnedBatch>],
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
        unicast_worker: i32,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
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
                    seek_pk_lo, seek_pk_hi, seek_col_idx,
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
        batch: Option<&OwnedBatch>,
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
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
                seek_pk_lo, seek_pk_hi, seek_col_idx,
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

    /// fdatasync + eventfd_signal all workers.
    pub fn sync_and_signal_all(&self) {
        sys::fdatasync(self.fd);
        for w in 0..self.num_workers {
            ipc_sys::eventfd_signal(self.m2w_efds[w]);
        }
    }

    /// fdatasync + eventfd_signal one worker.
    pub fn sync_and_signal_one(&self, worker: usize) {
        sys::fdatasync(self.fd);
        ipc_sys::eventfd_signal(self.m2w_efds[worker]);
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

    /// fdatasync only (no signal).
    pub fn sync(&self) {
        sys::fdatasync(self.fd);
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

// ---------------------------------------------------------------------------
// Async fdatasync via io_uring
// ---------------------------------------------------------------------------

/// Submits fdatasync asynchronously via a dedicated io_uring ring.
/// Separate from the transport ring to avoid coupling engine ↔ transport.
pub struct AsyncFsync {
    ring: io_uring::IoUring,
    fd: i32,
    in_flight: bool,
}

impl AsyncFsync {
    pub fn new(fd: i32) -> Result<Self, String> {
        let ring = io_uring::IoUring::new(4)
            .map_err(|e| format!("AsyncFsync io_uring init: {}", e))?;
        Ok(AsyncFsync { ring, fd, in_flight: false })
    }

    /// Submit an async fdatasync. No-op if one is already in flight.
    pub fn submit(&mut self) {
        if self.in_flight { return; }
        let entry = io_uring::opcode::Fsync::new(io_uring::types::Fd(self.fd))
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build()
            .user_data(1);
        unsafe {
            self.ring.submission().push(&entry).ok();
        }
        let _ = self.ring.submit();
        self.in_flight = true;
    }

    /// Blocking wait: returns the fsync result code.
    pub fn wait_complete(&mut self) -> i32 {
        if !self.in_flight { return 0; }
        // Check if already completed before issuing a blocking syscall.
        if let Some(cqe) = self.ring.completion().next() {
            self.in_flight = false;
            return cqe.result();
        }
        let _ = self.ring.submit_and_wait(1);
        if let Some(cqe) = self.ring.completion().next() {
            self.in_flight = false;
            cqe.result()
        } else {
            self.in_flight = false;
            -1
        }
    }
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

/// Worker's write side of W2M.
pub struct W2mWriter {
    region_ptr: *mut u8,
    region_size: u64,
    efd: i32,
}

unsafe impl Send for W2mWriter {}

impl W2mWriter {
    pub fn new(region_ptr: *mut u8, region_size: u64, efd: i32) -> Self {
        W2mWriter { region_ptr, region_size, efd }
    }

    /// Encode wire data directly into W2M mmap region + signal master.
    pub fn send_encoded(&self, sz: usize, encode_fn: impl FnOnce(&mut [u8])) {
        unsafe {
            let wc = atomic_load_u64(self.region_ptr) as usize;
            let total = 8 + align8(sz);
            if wc + total > self.region_size as usize { return; }
            write_u64_raw(self.region_ptr, wc, sz as u64);
            if sz > 0 {
                let slot = std::slice::from_raw_parts_mut(
                    self.region_ptr.add(wc + 8), sz,
                );
                encode_fn(slot);
            }
            atomic_store_u64(self.region_ptr, (wc + total) as u64);
        }
        ipc_sys::eventfd_signal(self.efd);
    }
}

/// Master's read side of W2M.
pub struct W2mReceiver {
    region_ptrs: Vec<*mut u8>,
    efds: Vec<i32>,
    num_workers: usize,
}

unsafe impl Send for W2mReceiver {}

impl W2mReceiver {
    pub fn new(region_ptrs: Vec<*mut u8>, efds: Vec<i32>) -> Self {
        let num_workers = region_ptrs.len();
        W2mReceiver { region_ptrs, efds, num_workers }
    }

    /// Poll for any worker signal (wraps eventfd_wait_any).
    pub fn poll(&self, timeout_ms: i32) -> i32 {
        ipc_sys::eventfd_wait_any(&self.efds, timeout_ms)
    }

    /// Wait for a specific worker signal (wraps eventfd_wait).
    pub fn wait_one(&self, worker: usize, timeout_ms: i32) -> i32 {
        ipc_sys::eventfd_wait(self.efds[worker], timeout_ms)
    }

    /// Read write-cursor of worker (atomic load).
    pub fn write_cursor(&self, worker: usize) -> u64 {
        unsafe { atomic_load_u64(self.region_ptrs[worker]) }
    }

    /// Try to read next decoded message from worker at read_cursor.
    /// Returns None if no new data. Returns (DecodedWire, new_cursor).
    pub fn try_read(&self, worker: usize, read_cursor: u64) -> Option<(DecodedWire, u64)> {
        let wc = self.write_cursor(worker);
        if read_cursor >= wc {
            return None;
        }
        let (new_rc, data_size, data_ptr) = unsafe {
            w2m_read(self.region_ptrs[worker], read_cursor)
        };
        if data_size == 0 {
            return None;
        }
        let data = unsafe { std::slice::from_raw_parts(data_ptr, data_size as usize) };
        match decode_wire(data) {
            Ok(decoded) => Some((decoded, new_rc)),
            Err(_) => None,
        }
    }

    /// Reset all W2M write cursors to W2M_HEADER_SIZE.
    pub fn reset_all(&self) {
        for w in 0..self.num_workers {
            unsafe {
                atomic_store_u64(self.region_ptrs[w], W2M_HEADER_SIZE as u64);
            }
        }
    }

    /// Reset one worker's W2M write cursor.
    pub fn reset_one(&self, worker: usize) {
        unsafe {
            atomic_store_u64(self.region_ptrs[worker], W2M_HEADER_SIZE as u64);
        }
    }

    pub fn num_workers(&self) -> usize { self.num_workers }
    pub fn efds(&self) -> &[i32] { &self.efds }
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

    #[test]
    fn test_w2m_round_trip() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);

            // Init write cursor to W2M_HEADER_SIZE
            atomic_store_u64(ptr, W2M_HEADER_SIZE as u64);

            let buf = make_test_data(0xEE, 256);
            let new_wc = w2m_write(ptr, buf.as_ptr(), buf.len() as u32, size as u64);
            assert!(new_wc > 0);

            // Read back
            let (new_rc, data_size, data_ptr) = w2m_read(ptr, W2M_HEADER_SIZE as u64);
            assert_eq!(data_size as usize, 256);
            let data = std::slice::from_raw_parts(data_ptr, data_size as usize);
            assert_eq!(data, buf.as_slice());
            assert_eq!(new_rc, new_wc as u64);

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_w2m_multiple_messages() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            atomic_store_u64(ptr, W2M_HEADER_SIZE as u64);

            for i in 0..3u8 {
                let buf = make_test_data(i + 1, 100);
                let r = w2m_write(ptr, buf.as_ptr(), buf.len() as u32, size as u64);
                assert!(r > 0);
            }

            let mut rc = W2M_HEADER_SIZE as u64;
            for i in 0..3u8 {
                let (new_rc, sz, data_ptr) = w2m_read(ptr, rc);
                assert_eq!(sz, 100);
                let data = std::slice::from_raw_parts(data_ptr, sz as usize);
                assert_eq!(data[0], i + 1);
                rc = new_rc;
            }

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_w2m_cursor_reset() {
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            atomic_store_u64(ptr, W2M_HEADER_SIZE as u64);

            // Write something
            let buf = make_test_data(0x42, 64);
            w2m_write(ptr, buf.as_ptr(), buf.len() as u32, size as u64);

            // Reset cursor
            atomic_store_u64(ptr, W2M_HEADER_SIZE as u64);
            let wc = atomic_load_u64(ptr);
            assert_eq!(wc, W2M_HEADER_SIZE as u64);

            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_w2m_full_error() {
        unsafe {
            // Tiny region
            let size = W2M_HEADER_SIZE + 16; // barely any space
            let ptr = alloc_mmap(size);
            atomic_store_u64(ptr, W2M_HEADER_SIZE as u64);

            let buf = make_test_data(0xFF, 100);
            let r = w2m_write(ptr, buf.as_ptr(), buf.len() as u32, size as u64);
            assert_eq!(r, -1);

            free_mmap(ptr, size);
        }
    }

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
    fn make_simple_batch(pk: u64, val: u64) -> OwnedBatch {
        let sd = simple_schema();
        let mut b = OwnedBatch::with_schema(sd, 1);
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
            42, 7, 0x100, 10, 20, 3,
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
            1, 0, 0, 0, 0, 0,
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
        assert_eq!(decoded.col_names.len(), 2);
        assert_eq!(decoded.col_names[0], b"id");
        assert_eq!(decoded.col_names[1], b"value");
        assert!(decoded.data_batch.is_none());
    }

    #[test]
    fn test_encode_decode_roundtrip_with_data() {
        let sd = simple_schema();
        let batch = make_simple_batch(100, 999);
        let names: Vec<&[u8]> = vec![b"id", b"val"];
        let wire = encode_wire(
            5, 0, 0, 0, 0, 0,
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
            0, 0, 0, 0, 0, 0,
            STATUS_ERROR, b"something went wrong",
            None, None, None,
        );
        let decoded = decode_wire(&wire).unwrap();
        assert_eq!(decoded.control.status, STATUS_ERROR);
        assert_eq!(decoded.control.error_msg, b"something went wrong");
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
            0, 0, 0, 0, 0, 0,
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
        let mut batch = OwnedBatch::with_schema(sd, 2);

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
            10, 0, 0, 0, 0, 0,
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
        let wire = encode_wire(42, 7, 0x100, 10, 20, 3, STATUS_OK, b"", None, None, None);
        assert_eq!(sz, wire.len(), "wire_size mismatch (no data)");

        // With schema only
        let sd = simple_schema();
        let names: Vec<&[u8]> = vec![b"id", b"val"];
        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), None);
        let wire = encode_wire(1, 0, 0, 0, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), None);
        assert_eq!(sz, wire.len(), "wire_size mismatch (schema only)");

        // With data
        let batch = make_simple_batch(100, 999);
        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        let wire = encode_wire(5, 0, 0, 0, 0, 0, STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        assert_eq!(sz, wire.len(), "wire_size mismatch (with data)");

        // With error message
        let sz = wire_size(STATUS_ERROR, b"something went wrong", None, None, None);
        let wire = encode_wire(0, 0, 0, 0, 0, 0, STATUS_ERROR, b"something went wrong", None, None, None);
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
            5, 0, 0, 0, 0, 0,
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
            10, 3, 0x200, 5, 6, 7,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );

        let sz = wire_size(STATUS_OK, b"", Some(&sd), Some(&names), Some(&batch));
        let mut buf = vec![0u8; sz];
        let written = encode_wire_into(
            &mut buf, 0,
            10, 3, 0x200, 5, 6, 7,
            STATUS_OK, b"",
            Some(&sd), Some(&names), Some(&batch),
        );
        assert_eq!(written, wire.len());
        assert_eq!(buf, wire, "encode_wire_into should produce identical bytes");
    }
}
