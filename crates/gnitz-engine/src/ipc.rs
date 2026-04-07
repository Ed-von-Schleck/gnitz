//! IPC transport layer: atomics, SAL (shared append-only log), W2M (worker→master),
//! and wire protocol encode/decode.
//!
//! Replaces gnitz/server/ipc.py transport + wire protocol logic.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, type_size, encode_german_string, decode_german_string};
use crate::storage::{OwnedBatch, wal};
use crate::util::align8;

// ---------------------------------------------------------------------------
// Constants (must match ipc.py)
// ---------------------------------------------------------------------------

pub const MAX_WORKERS: usize = 64;
/// Group header: 32 fixed + MAX_WORKERS*4 offsets + MAX_WORKERS*4 sizes + 32 pad
pub const GROUP_HEADER_SIZE: usize = 576;
pub const SAL_MMAP_SIZE: usize = 1 << 30;
pub const W2M_REGION_SIZE: usize = 1 << 30;
pub const W2M_HEADER_SIZE: usize = 128;

// Wire protocol flags (bits in the control block flags field)
pub const FLAG_HAS_SCHEMA: u64 = 1 << 48;
pub const FLAG_HAS_DATA: u64 = 1 << 49;
pub const FLAG_BATCH_SORTED: u64 = 1 << 50;
pub const FLAG_BATCH_CONSOLIDATED: u64 = 1 << 51;

/// Control block TID — max u32, never a real table ID.
pub const IPC_CONTROL_TID: u32 = 0xFFFFFFFF;

/// Status codes
pub const STATUS_OK: u32 = 0;
pub const STATUS_ERROR: u32 = 1;

/// Meta-schema flags
pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK: u64 = 2;

// SAL group header flags (u32) — must match ipc.py
pub const FLAG_SHUTDOWN: u32            = 4;
pub const FLAG_DDL_SYNC: u32            = 8;
pub const FLAG_EXCHANGE: u32            = 16;
pub const FLAG_PUSH: u32                = 32;
pub const FLAG_HAS_PK: u32             = 64;
pub const FLAG_SEEK: u32                = 128;
pub const FLAG_SEEK_BY_INDEX: u32       = 256;
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
        batch.pk_lo.extend_from_slice(&(ci as u64).to_le_bytes());
        batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch.weight.extend_from_slice(&1i64.to_le_bytes());
        batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        // payload col 0 (type_code): u64
        batch.col_data[0].extend_from_slice(&type_code_val.to_le_bytes());
        // payload col 1 (flags): u64
        batch.col_data[1].extend_from_slice(&flags.to_le_bytes());
        // payload col 2 (name): 16-byte German string
        batch.col_data[2].extend_from_slice(&name_st);
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
            batch.col_data[0][off8..off8 + 8].try_into().unwrap()
        ) as u8;
        let flags_val = u64::from_le_bytes(
            batch.col_data[1][off8..off8 + 8].try_into().unwrap()
        );

        let off16 = i * 16;
        let mut st = [0u8; 16];
        st.copy_from_slice(&batch.col_data[2][off16..off16 + 16]);
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

/// Encode a control batch into WAL block bytes.
fn encode_control_block(
    target_id: u64,
    client_id: u64,
    flags: u64,
    seek_pk_lo: u64,
    seek_pk_hi: u64,
    seek_col_idx: u64,
    status: u32,
    error_msg: &[u8],
) -> Vec<u8> {
    let cs = CONTROL_SCHEMA_DESC;
    let mut batch = OwnedBatch::with_schema(cs, 1);

    let has_error = !error_msg.is_empty();
    let null_word: u64 = if has_error { 0 } else { 1u64 << 7 }; // payload col 7 = error_msg

    batch.pk_lo.extend_from_slice(&0u64.to_le_bytes()); // msg_idx PK = 0
    batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
    batch.weight.extend_from_slice(&1i64.to_le_bytes());
    batch.null_bmp.extend_from_slice(&null_word.to_le_bytes());
    batch.col_data[0].extend_from_slice(&(status as u64).to_le_bytes());
    batch.col_data[1].extend_from_slice(&client_id.to_le_bytes());
    batch.col_data[2].extend_from_slice(&target_id.to_le_bytes());
    batch.col_data[3].extend_from_slice(&flags.to_le_bytes());
    batch.col_data[4].extend_from_slice(&seek_pk_lo.to_le_bytes());
    batch.col_data[5].extend_from_slice(&seek_pk_hi.to_le_bytes());
    batch.col_data[6].extend_from_slice(&seek_col_idx.to_le_bytes());

    let error_st = if has_error {
        encode_german_string(error_msg, &mut batch.blob)
    } else {
        [0u8; 16] // null
    };
    batch.col_data[7].extend_from_slice(&error_st);
    batch.count = 1;

    encode_batch_to_wal(&batch, 0, IPC_CONTROL_TID)
}

/// Encode an OwnedBatch into a WAL block using the gnitz-engine WAL encoder.
fn encode_batch_to_wal(batch: &OwnedBatch, lsn: u64, table_id: u32) -> Vec<u8> {
    let nr = batch.num_regions();
    let mut ptrs = Vec::with_capacity(nr);
    let mut sizes = Vec::with_capacity(nr);
    for i in 0..nr {
        ptrs.push(batch.region_ptr(i));
        sizes.push(batch.region_size(i) as u32);
    }
    let blob_size = batch.blob.len() as u64;
    let total = wal::block_size(nr, &sizes);
    let mut buf = vec![0u8; total];
    let result = wal::encode(
        &mut buf, 0, lsn, table_id, batch.count as u32,
        &ptrs, &sizes, blob_size,
    );
    assert!(result >= 0, "WAL encode failed");
    buf.truncate(result as usize);
    buf
}

/// Encode a full IPC wire message.
///
/// Returns a heap-allocated buffer (ptr + len). Caller must free via `gnitz_ipc_wire_free`.
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

    let ctrl = encode_control_block(
        target_id, client_id, wire_flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx,
        status, error_msg,
    );

    let mut out = ctrl;

    if has_schema {
        let eff_schema = if let Some(s) = schema {
            s
        } else {
            data_batch.unwrap().schema.as_ref().unwrap()
        };
        let names = col_names.unwrap_or(&[]);
        let schema_batch = schema_to_batch(eff_schema, names);
        let schema_block = encode_batch_to_wal(&schema_batch, 0, target_id as u32);
        out.extend_from_slice(&schema_block);
    }

    if has_data {
        let b = data_batch.unwrap();
        let data_block = encode_batch_to_wal(b, 0, target_id as u32);
        out.extend_from_slice(&data_block);
    }

    out
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
    let mut offsets = [0u32; 128];
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
    let status = u64::from_le_bytes(ctrl_batch.col_data[0][0..8].try_into().unwrap()) as u32;
    let client_id = u64::from_le_bytes(ctrl_batch.col_data[1][0..8].try_into().unwrap());
    let target_id = u64::from_le_bytes(ctrl_batch.col_data[2][0..8].try_into().unwrap());
    let flags = u64::from_le_bytes(ctrl_batch.col_data[3][0..8].try_into().unwrap());
    let seek_pk_lo = u64::from_le_bytes(ctrl_batch.col_data[4][0..8].try_into().unwrap());
    let seek_pk_hi = u64::from_le_bytes(ctrl_batch.col_data[5][0..8].try_into().unwrap());
    let seek_col_idx = u64::from_le_bytes(ctrl_batch.col_data[6][0..8].try_into().unwrap());

    // error_msg: payload col 7, null bit 7
    let null_word = u64::from_le_bytes(ctrl_batch.null_bmp[0..8].try_into().unwrap());
    let error_is_null = (null_word & (1u64 << 7)) != 0;
    let error_msg = if error_is_null {
        Vec::new()
    } else {
        let mut st = [0u8; 16];
        st.copy_from_slice(&ctrl_batch.col_data[7][0..16]);
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
pub unsafe fn atomic_load_u64(ptr: *const u8) -> u64 {
    let atomic = &*(ptr as *const AtomicU64);
    atomic.load(Ordering::Acquire)
}

/// Atomic store with Release ordering to a raw pointer.
///
/// # Safety
/// `ptr` must point to a naturally-aligned u64 in shared memory.
pub unsafe fn atomic_store_u64(ptr: *mut u8, val: u64) {
    let atomic = &*(ptr as *const AtomicU64);
    atomic.store(val, Ordering::Release);
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

/// Result from SAL write.
#[repr(C)]
pub struct SalWriteResult {
    /// 0 on success, -1 on error (SAL full).
    pub status: i32,
    /// New write cursor position.
    pub new_cursor: u64,
}

/// Write a message group into the SAL for N workers.
///
/// `worker_ptrs[i]` / `worker_sizes[i]` point to pre-encoded wire buffers
/// (from `gnitz_ipc_encode_wire`). A null pointer or size=0 means "no data
/// for worker i".
///
/// The size prefix is written LAST via atomic release store — workers use it
/// as the "data ready" sentinel.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `write_cursor + total`
/// bytes. `worker_ptrs`/`worker_sizes` must be valid arrays of `num_workers`.
pub unsafe fn sal_write_group(
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
    if nw > MAX_WORKERS {
        return SalWriteResult { status: -1, new_cursor: write_cursor };
    }
    let wc = write_cursor as usize;

    // Compute total size: 8-byte prefix + GROUP_HEADER_SIZE + per-worker data
    let mut payload_size = GROUP_HEADER_SIZE;
    for w in 0..nw {
        let sz = *worker_sizes.add(w) as usize;
        if sz > 0 {
            payload_size += align8(sz);
        }
    }

    let total = 8 + align8(payload_size);
    if wc + total > mmap_size as usize {
        return SalWriteResult { status: -1, new_cursor: write_cursor };
    }

    let base = wc;
    let hdr_off = base + 8;

    // Group header
    write_u64_raw(sal_ptr, hdr_off + 0, payload_size as u64);
    write_u64_raw(sal_ptr, hdr_off + 8, lsn);
    write_u32_raw(sal_ptr, hdr_off + 16, num_workers);
    write_u32_raw(sal_ptr, hdr_off + 20, flags);
    write_u32_raw(sal_ptr, hdr_off + 24, target_id);
    write_u32_raw(sal_ptr, hdr_off + 28, epoch);

    // Worker offsets, sizes, and data copy
    let mut data_offset = GROUP_HEADER_SIZE;
    for w in 0..nw {
        let wsz = *worker_sizes.add(w) as usize;
        let wptr = *worker_ptrs.add(w);
        if wsz > 0 && !wptr.is_null() {
            write_u32_raw(sal_ptr, hdr_off + 32 + w * 4, data_offset as u32);
            write_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, wsz as u32);
            std::ptr::copy_nonoverlapping(
                wptr,
                sal_ptr.add(hdr_off + data_offset),
                wsz,
            );
            data_offset += align8(wsz);
        } else {
            write_u32_raw(sal_ptr, hdr_off + 32 + w * 4, 0);
            write_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, 0);
        }
    }

    // Write size prefix LAST via atomic release store
    atomic_store_u64(sal_ptr.add(base), payload_size as u64);

    SalWriteResult {
        status: 0,
        new_cursor: (wc + total) as u64,
    }
}

// ---------------------------------------------------------------------------
// SAL read (worker reads its data from a group)
// ---------------------------------------------------------------------------

/// Result from reading a SAL group header for a specific worker.
#[repr(C)]
pub struct SalReadResult {
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
pub unsafe fn sal_read_group_header(
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

/// Write a pre-encoded wire buffer into a W2M region.
///
/// Returns the new write cursor, or -1 if the region is full.
///
/// # Safety
/// `region_ptr` must be a valid mmap pointer of at least `region_size` bytes.
pub unsafe fn w2m_write(
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
pub unsafe fn w2m_read(
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
// FFI exports: wire protocol
// ---------------------------------------------------------------------------

/// Encode a wire message. Returns heap-allocated buffer.
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
        b.pk_lo.extend_from_slice(&pk.to_le_bytes());
        b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        b.weight.extend_from_slice(&1i64.to_le_bytes());
        b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        b.col_data[0].extend_from_slice(&val.to_le_bytes());
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
        let pk = u64::from_le_bytes(db.pk_lo[0..8].try_into().unwrap());
        assert_eq!(pk, 100);
        // Check payload col 0 (val)
        let val = u64::from_le_bytes(db.col_data[0][0..8].try_into().unwrap());
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
        batch.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch.weight.extend_from_slice(&1i64.to_le_bytes());
        batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        batch.col_data[0].extend_from_slice(&42u64.to_le_bytes()); // int col
        let st1 = encode_german_string(b"hello", &mut batch.blob);
        batch.col_data[1].extend_from_slice(&st1);

        // Row 2: long string
        batch.pk_lo.extend_from_slice(&2u64.to_le_bytes());
        batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch.weight.extend_from_slice(&1i64.to_le_bytes());
        batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        batch.col_data[0].extend_from_slice(&99u64.to_le_bytes());
        let long_str = b"this is a long string that exceeds twelve bytes";
        let st2 = encode_german_string(long_str, &mut batch.blob);
        batch.col_data[1].extend_from_slice(&st2);
        batch.count = 2;

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
        s1.copy_from_slice(&db.col_data[1][0..16]);
        let str1 = decode_german_string(&s1, &db.blob);
        assert_eq!(str1, b"hello");

        // Verify long string roundtrip
        let mut s2 = [0u8; 16];
        s2.copy_from_slice(&db.col_data[1][16..32]);
        let str2 = decode_german_string(&s2, &db.blob);
        assert_eq!(str2, long_str);
    }
}
