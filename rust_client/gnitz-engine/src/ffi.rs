use std::ffi::CStr;
use std::panic;
use std::ptr;
use std::slice;

use libc::{c_int, c_void};
use xorf::Xor8;

use crate::{bloom::BloomFilter, xor8};

// ---------------------------------------------------------------------------
// XOR8
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_xor8_build(
    pk_lo: *const u64,
    pk_hi: *const u64,
    count: u32,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if pk_lo.is_null() || pk_hi.is_null() || count == 0 {
            return ptr::null_mut();
        }
        let n = count as usize;
        let lo = unsafe { slice::from_raw_parts(pk_lo, n) };
        let hi = unsafe { slice::from_raw_parts(pk_hi, n) };
        match xor8::build(lo, hi) {
            Some(filter) => Box::into_raw(Box::new(filter)) as *mut c_void,
            None => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_xor8_may_contain(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
) -> c_int {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return 0;
        }
        let filter = unsafe { &*(handle as *const Xor8) };
        if xor8::may_contain(filter, key_lo, key_hi) {
            1
        } else {
            0
        }
    });
    result.unwrap_or(0)
}

/// Serialize an Xor8 filter.
/// If out is NULL, returns the required buffer size.
/// If out is non-NULL, writes at most cap bytes and returns bytes written, or -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_xor8_serialize(
    handle: *const c_void,
    out: *mut u8,
    cap: i64,
) -> i64 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return -1i64;
        }
        let filter = unsafe { &*(handle as *const Xor8) };
        if out.is_null() {
            // Query size mode
            return xor8::serialized_size(filter) as i64;
        }
        let bytes = xor8::serialize(filter);
        let cap = cap as usize;
        if bytes.len() > cap {
            return -1i64;
        }
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len());
        }
        bytes.len() as i64
    });
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_xor8_deserialize(buf: *const u8, len: i64) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if buf.is_null() || len <= 0 {
            return ptr::null_mut();
        }
        let data = unsafe { slice::from_raw_parts(buf, len as usize) };
        match xor8::deserialize(data) {
            Some(filter) => Box::into_raw(Box::new(filter)) as *mut c_void,
            None => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_xor8_free(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(handle as *mut Xor8) };
    });
}

// ---------------------------------------------------------------------------
// Bloom
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_bloom_create(expected_n: u32) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        let bf = BloomFilter::new(expected_n);
        Box::into_raw(Box::new(bf)) as *mut c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_bloom_add(handle: *mut c_void, key_lo: u64, key_hi: u64) {
    let _ = panic::catch_unwind(|| {
        if handle.is_null() {
            return;
        }
        let bf = unsafe { &mut *(handle as *mut BloomFilter) };
        bf.add(key_lo, key_hi);
    });
}

#[no_mangle]
pub extern "C" fn gnitz_bloom_may_contain(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
) -> c_int {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return 0;
        }
        let bf = unsafe { &*(handle as *const BloomFilter) };
        if bf.may_contain(key_lo, key_hi) {
            1
        } else {
            0
        }
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_bloom_reset(handle: *mut c_void) {
    let _ = panic::catch_unwind(|| {
        if handle.is_null() {
            return;
        }
        let bf = unsafe { &mut *(handle as *mut BloomFilter) };
        bf.reset();
    });
}

#[no_mangle]
pub extern "C" fn gnitz_bloom_free(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(handle as *mut BloomFilter) };
    });
}

// ---------------------------------------------------------------------------
// XXH3 checksum
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_xxh3_checksum(data: *const u8, len: i64) -> u64 {
    let result = panic::catch_unwind(|| {
        if data.is_null() || len <= 0 {
            return 0u64;
        }
        let buf = unsafe { slice::from_raw_parts(data, len as usize) };
        crate::xxh::checksum(buf)
    });
    result.unwrap_or(0)
}

/// Hash a 128-bit key (lo, hi) with seeds to a 64-bit hash.
/// XORs inputs with seeds before hashing.
#[no_mangle]
pub extern "C" fn gnitz_xxh3_hash_u128(
    lo: u64,
    hi: u64,
    seed_lo: u64,
    seed_hi: u64,
) -> u64 {
    let result = panic::catch_unwind(|| {
        crate::xxh::hash_u128_seeded(lo, hi, seed_lo, seed_hi)
    });
    result.unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Shard reader
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_shard_open(
    path: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
    validate_checksums: i32,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if path.is_null() || schema_desc.is_null() {
            return ptr::null_mut();
        }
        let cpath = unsafe { CStr::from_ptr(path) };
        let schema = unsafe { &*schema_desc };
        match crate::shard_reader::MappedShard::open(cpath, schema, validate_checksums != 0) {
            Ok(shard) => Box::into_raw(Box::new(shard)) as *mut c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

/// Create a buffer-backed shard from raw column pointers.
/// Returns an opaque handle usable with all gnitz_shard_* functions, or null on error.
/// The caller must keep the buffer memory alive until gnitz_shard_close is called.
#[no_mangle]
pub extern "C" fn gnitz_shard_open_from_buffers(
    pk_lo: *const u8,
    pk_hi: *const u8,
    weight: *const u8,
    null_bm: *const u8,
    col_ptrs: *const *const u8,
    col_sizes: *const u64,
    num_payload_cols: u32,
    blob_ptr: *const u8,
    blob_len: u64,
    count: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() {
            return ptr::null_mut();
        }
        let schema = unsafe { &*schema_desc };
        let n_rows = count as usize;
        let n_cols = num_payload_cols as usize;

        if n_cols != (schema.num_columns as usize).saturating_sub(1) {
            return ptr::null_mut();
        }

        if n_rows > 0 && (pk_lo.is_null() || pk_hi.is_null() || weight.is_null() || null_bm.is_null()) {
            return ptr::null_mut();
        }

        let (payload_ptrs, payload_sizes) = if n_cols > 0 {
            if col_ptrs.is_null() || col_sizes.is_null() {
                return ptr::null_mut();
            }
            let ptrs = unsafe { slice::from_raw_parts(col_ptrs, n_cols) };
            let raw_sizes = unsafe { slice::from_raw_parts(col_sizes, n_cols) };
            let sizes: Vec<usize> = raw_sizes.iter().map(|&s| s as usize).collect();
            (ptrs.to_vec(), sizes)
        } else {
            (vec![], vec![])
        };

        let shard = crate::shard_reader::MappedShard::from_buffers(
            pk_lo, pk_hi, weight, null_bm,
            &payload_ptrs, &payload_sizes,
            blob_ptr, blob_len as usize,
            n_rows, schema,
        );
        Box::into_raw(Box::new(shard)) as *mut c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_shard_close(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(handle as *mut crate::shard_reader::MappedShard) };
    });
}

#[no_mangle]
pub extern "C" fn gnitz_shard_row_count(handle: *const c_void) -> i32 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.count as i32
}

#[no_mangle]
pub extern "C" fn gnitz_shard_has_xor8(handle: *const c_void) -> i32 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    if shard.has_xor8() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_shard_xor8_may_contain(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() { return 1; }
        let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
        if shard.xor8_may_contain(key_lo, key_hi) { 1 } else { 0 }
    });
    result.unwrap_or(1)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_find_row(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() { return -1; }
        let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
        shard.find_row_index(key_lo, key_hi)
    });
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_lower_bound(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() { return 0; }
        let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
        shard.find_lower_bound(key_lo, key_hi) as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_get_pk_lo(handle: *const c_void, row: i32) -> u64 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.get_pk_lo(row as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_get_pk_hi(handle: *const c_void, row: i32) -> u64 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.get_pk_hi(row as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_get_weight(handle: *const c_void, row: i32) -> i64 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.get_weight(row as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_get_null_word(handle: *const c_void, row: i32) -> u64 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.get_null_word(row as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_col_ptr(
    handle: *const c_void,
    row: i32,
    col_idx: i32,
    col_size: i32,
) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.col_ptr_by_logical(row as usize, col_idx as usize, col_size as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_shard_blob_ptr(handle: *const c_void) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.blob_ptr()
}

#[no_mangle]
pub extern "C" fn gnitz_shard_blob_len(handle: *const c_void) -> i64 {
    if handle.is_null() { return 0; }
    let shard = unsafe { &*(handle as *const crate::shard_reader::MappedShard) };
    shard.blob_len() as i64
}

// ---------------------------------------------------------------------------
// WAL encode/decode
// ---------------------------------------------------------------------------

/// Encode a WAL block from region data into out_buf at out_offset.
/// Returns new offset, or -1 on error (buffer too small).
#[no_mangle]
pub extern "C" fn gnitz_wal_encode(
    out_buf: *mut u8,
    out_offset: i64,
    out_capacity: i64,
    lsn: u64,
    table_id: u32,
    entry_count: u32,
    region_ptrs: *const *const u8,
    region_sizes: *const u32,
    num_regions: u32,
    blob_size: u64,
) -> i64 {
    let result = panic::catch_unwind(|| {
        if out_buf.is_null() || out_capacity <= 0 {
            return -1i64;
        }
        let n = num_regions as usize;
        let buf = unsafe { slice::from_raw_parts_mut(out_buf, out_capacity as usize) };
        let ptrs = if n > 0 && !region_ptrs.is_null() {
            unsafe { slice::from_raw_parts(region_ptrs, n) }
        } else {
            &[]
        };
        let sizes = if n > 0 && !region_sizes.is_null() {
            unsafe { slice::from_raw_parts(region_sizes, n) }
        } else {
            &[]
        };
        crate::wal::encode(
            buf, out_offset as usize, lsn, table_id, entry_count,
            ptrs, sizes, blob_size,
        )
    });
    result.unwrap_or(-1)
}

/// Validate a WAL block and extract header + directory.
/// Returns 0=ok, -1=bad version, -2=bad checksum, -3=truncated.
#[no_mangle]
pub extern "C" fn gnitz_wal_validate_and_parse(
    block: *const u8,
    block_len: i64,
    out_lsn: *mut u64,
    out_tid: *mut u32,
    out_count: *mut u32,
    out_num_regions: *mut u32,
    out_blob_size: *mut u64,
    out_region_offsets: *mut u32,
    out_region_sizes: *mut u32,
    max_regions: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if block.is_null() || block_len <= 0
            || out_lsn.is_null() || out_tid.is_null() || out_count.is_null()
            || out_num_regions.is_null() || out_blob_size.is_null()
        {
            return crate::wal::WAL_ERR_TRUNCATED;
        }
        let data = unsafe { slice::from_raw_parts(block, block_len as usize) };
        let lsn = unsafe { &mut *out_lsn };
        let tid = unsafe { &mut *out_tid };
        let count = unsafe { &mut *out_count };
        let num_regions = unsafe { &mut *out_num_regions };
        let blob_size = unsafe { &mut *out_blob_size };
        let max = max_regions as usize;
        let offsets = if max > 0 && !out_region_offsets.is_null() {
            unsafe { slice::from_raw_parts_mut(out_region_offsets, max) }
        } else {
            &mut []
        };
        let sizes = if max > 0 && !out_region_sizes.is_null() {
            unsafe { slice::from_raw_parts_mut(out_region_sizes, max) }
        } else {
            &mut []
        };
        crate::wal::validate_and_parse(
            data, lsn, tid, count, num_regions, blob_size,
            offsets, sizes, max_regions,
        )
    });
    result.unwrap_or(crate::wal::WAL_ERR_TRUNCATED)
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// Serialize manifest entries to buffer.
/// Returns bytes written, or -1 if buffer too small.
#[no_mangle]
pub extern "C" fn gnitz_manifest_serialize(
    out_buf: *mut u8,
    out_capacity: i64,
    entries: *const u8,
    count: u32,
    global_max_lsn: u64,
) -> i64 {
    let result = panic::catch_unwind(|| {
        if out_buf.is_null() || out_capacity <= 0 {
            return -1i64;
        }
        let buf = unsafe { slice::from_raw_parts_mut(out_buf, out_capacity as usize) };
        let n = count as usize;
        let entry_slice = if n > 0 && !entries.is_null() {
            unsafe {
                slice::from_raw_parts(
                    entries as *const crate::manifest::ManifestEntryRaw,
                    n,
                )
            }
        } else {
            &[]
        };
        crate::manifest::serialize(buf, entry_slice, global_max_lsn)
    });
    result.unwrap_or(-1)
}

/// Parse manifest buffer. Returns entry count, or negative on error.
#[no_mangle]
pub extern "C" fn gnitz_manifest_parse(
    buf: *const u8,
    buf_len: i64,
    out_entries: *mut u8,
    max_entries: u32,
    out_global_max_lsn: *mut u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if buf.is_null() || buf_len <= 0 || out_global_max_lsn.is_null() {
            return crate::manifest::MANIFEST_ERR_TRUNCATED;
        }
        let data = unsafe { slice::from_raw_parts(buf, buf_len as usize) };
        let lsn = unsafe { &mut *out_global_max_lsn };
        let max = max_entries as usize;
        let entries = if max > 0 && !out_entries.is_null() {
            unsafe {
                slice::from_raw_parts_mut(
                    out_entries as *mut crate::manifest::ManifestEntryRaw,
                    max,
                )
            }
        } else {
            &mut []
        };
        crate::manifest::parse(data, entries, max_entries, lsn)
    });
    result.unwrap_or(crate::manifest::MANIFEST_ERR_TRUNCATED)
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// N-way merge compaction: merge input shards into one output shard.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_compact_shards(
    input_files: *const *const libc::c_char,
    num_inputs: u32,
    output_file: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
    table_id: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if input_files.is_null() || output_file.is_null() || schema_desc.is_null() {
            return -1;
        }
        let n = num_inputs as usize;
        let schema = unsafe { &*schema_desc };
        let out = unsafe { CStr::from_ptr(output_file) };

        let file_ptrs = unsafe { slice::from_raw_parts(input_files, n) };
        let inputs: Vec<&CStr> = file_ptrs
            .iter()
            .map(|&p| unsafe { CStr::from_ptr(p) })
            .collect();

        crate::compact::compact_shards(&inputs, out, schema, table_id)
    });
    result.unwrap_or(-99)
}

/// Guarded N-way merge: merge input shards, routing rows to guard-bounded outputs.
/// Returns number of non-empty guard outputs on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_merge_and_route(
    input_files: *const *const libc::c_char,
    num_inputs: u32,
    output_dir: *const libc::c_char,
    guard_keys: *const u64,  // flat array: [lo0, hi0, lo1, hi1, ...]
    num_guards: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    lsn_tag: u64,
    out_results: *mut crate::compact::GuardResult,
    max_results: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if input_files.is_null() || output_dir.is_null() || schema_desc.is_null() {
            return -1;
        }
        let n_inputs = num_inputs as usize;
        let n_guards = num_guards as usize;
        let schema = unsafe { &*schema_desc };
        let out_dir = unsafe { CStr::from_ptr(output_dir) };

        let file_ptrs = unsafe { slice::from_raw_parts(input_files, n_inputs) };
        let inputs: Vec<&CStr> = file_ptrs
            .iter()
            .map(|&p| unsafe { CStr::from_ptr(p) })
            .collect();

        // Parse guard keys from flat array
        let gk_flat = if n_guards > 0 && !guard_keys.is_null() {
            unsafe { slice::from_raw_parts(guard_keys, n_guards * 2) }
        } else {
            &[]
        };
        let guards: Vec<(u64, u64)> = (0..n_guards)
            .map(|i| (gk_flat[i * 2], gk_flat[i * 2 + 1]))
            .collect();

        let results = if max_results > 0 && !out_results.is_null() {
            unsafe { slice::from_raw_parts_mut(out_results, max_results as usize) }
        } else {
            &mut []
        };

        crate::compact::merge_and_route(
            &inputs, out_dir, &guards, schema,
            table_id, level_num, lsn_tag, results,
        )
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Shard file write
// ---------------------------------------------------------------------------

/// Write a shard file atomically from pre-built column region buffers.
/// Returns 0 on success, negative error code on failure.
#[no_mangle]
pub extern "C" fn gnitz_write_shard(
    dirfd: i32,
    filename: *const libc::c_char,
    table_id: u32,
    row_count: u32,
    region_ptrs: *const *const u8,
    region_sizes: *const u64,
    num_regions: u32,
    pk_lo_ptr: *const u64,
    pk_hi_ptr: *const u64,
    durable: i32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if filename.is_null() {
            return crate::shard_file::ERR_BAD_ARGS;
        }
        let cfilename = unsafe { CStr::from_ptr(filename) };
        let n_regions = num_regions as usize;
        let n_rows = row_count as usize;

        // Reconstruct region slices
        let mut regions: Vec<&[u8]> = Vec::with_capacity(n_regions);
        if n_regions > 0 {
            if region_ptrs.is_null() || region_sizes.is_null() {
                return crate::shard_file::ERR_BAD_ARGS;
            }
            let ptrs = unsafe { slice::from_raw_parts(region_ptrs, n_regions) };
            let sizes = unsafe { slice::from_raw_parts(region_sizes, n_regions) };
            for i in 0..n_regions {
                let sz = sizes[i] as usize;
                if sz > 0 && ptrs[i].is_null() {
                    return crate::shard_file::ERR_BAD_ARGS;
                }
                let data = if sz > 0 {
                    unsafe { slice::from_raw_parts(ptrs[i], sz) }
                } else {
                    &[]
                };
                regions.push(data);
            }
        }

        // Reconstruct PK slices
        let pk_lo = if n_rows > 0 {
            if pk_lo_ptr.is_null() {
                return crate::shard_file::ERR_BAD_ARGS;
            }
            unsafe { slice::from_raw_parts(pk_lo_ptr, n_rows) }
        } else {
            &[]
        };
        let pk_hi = if n_rows > 0 {
            if pk_hi_ptr.is_null() {
                return crate::shard_file::ERR_BAD_ARGS;
            }
            unsafe { slice::from_raw_parts(pk_hi_ptr, n_rows) }
        } else {
            &[]
        };

        match crate::shard_file::write_shard(
            dirfd, cfilename, table_id, n_rows,
            &regions, pk_lo, pk_hi, durable != 0,
        ) {
            Ok(()) => 0,
            Err(e) => e,
        }
    });
    result.unwrap_or(crate::shard_file::ERR_BAD_ARGS)
}

// ---------------------------------------------------------------------------
// Batch descriptor (for passing ArenaZSetBatch buffers across FFI)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct GnitzBatchDesc {
    pub pk_lo: *const u8,
    pub pk_hi: *const u8,
    pub weight: *const u8,
    pub null_bm: *const u8,
    pub col_ptrs: *const *const u8,
    pub col_sizes: *const u64,
    pub num_payload_cols: u32,
    pub blob_ptr: *const u8,
    pub blob_len: u64,
    pub count: u32,
}

// ---------------------------------------------------------------------------
// Merged batch result (Rust-allocated output from merge)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct GnitzMergedBatch {
    pub count: u32,
    pub pk_lo: *mut u8,
    pub pk_lo_len: u64,
    pub pk_hi: *mut u8,
    pub pk_hi_len: u64,
    pub weight: *mut u8,
    pub weight_len: u64,
    pub null_bm: *mut u8,
    pub null_bm_len: u64,
    pub num_cols: u32,
    pub col_ptrs: [*mut u8; 64],
    pub col_lens: [u64; 64],
    pub blob: *mut u8,
    pub blob_len: u64,
}

impl GnitzMergedBatch {
    fn zeroed() -> Self {
        GnitzMergedBatch {
            count: 0,
            pk_lo: ptr::null_mut(), pk_lo_len: 0,
            pk_hi: ptr::null_mut(), pk_hi_len: 0,
            weight: ptr::null_mut(), weight_len: 0,
            null_bm: ptr::null_mut(), null_bm_len: 0,
            num_cols: 0,
            col_ptrs: [ptr::null_mut(); 64],
            col_lens: [0u64; 64],
            blob: ptr::null_mut(), blob_len: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: build ShardRef vec from FFI inputs
// ---------------------------------------------------------------------------

fn build_shard_refs(
    borrowed_handles: *const *const c_void,
    num_borrowed: u32,
    batch_descs: *const GnitzBatchDesc,
    num_batches: u32,
    schema: &crate::compact::SchemaDescriptor,
) -> Option<Vec<crate::cursor::ShardRef>> {
    let n_borrowed = num_borrowed as usize;
    let n_batches = num_batches as usize;
    let mut refs = Vec::with_capacity(n_borrowed + n_batches);

    if n_borrowed > 0 {
        if borrowed_handles.is_null() { return None; }
        let handles = unsafe { slice::from_raw_parts(borrowed_handles, n_borrowed) };
        for &h in handles {
            if h.is_null() { return None; }
            let shard = unsafe { &*(h as *const crate::shard_reader::MappedShard) };
            refs.push(crate::cursor::ShardRef::Borrowed(shard as *const _));
        }
    }

    if n_batches > 0 {
        if batch_descs.is_null() { return None; }
        let descs = unsafe { slice::from_raw_parts(batch_descs, n_batches) };
        for desc in descs {
            let n_cols = desc.num_payload_cols as usize;
            let (col_ptrs, col_sizes) = if n_cols > 0 && !desc.col_ptrs.is_null() && !desc.col_sizes.is_null() {
                let ptrs = unsafe { slice::from_raw_parts(desc.col_ptrs, n_cols) };
                let raw_sizes = unsafe { slice::from_raw_parts(desc.col_sizes, n_cols) };
                let sizes: Vec<usize> = raw_sizes.iter().map(|&s| s as usize).collect();
                (ptrs.to_vec(), sizes)
            } else {
                (vec![ptr::null(); n_cols], vec![0usize; n_cols])
            };

            let shard = crate::shard_reader::MappedShard::from_buffers(
                desc.pk_lo, desc.pk_hi, desc.weight, desc.null_bm,
                &col_ptrs, &col_sizes,
                desc.blob_ptr, desc.blob_len as usize,
                desc.count as usize, schema,
            );
            refs.push(crate::cursor::ShardRef::Owned(shard));
        }
    }

    Some(refs)
}

// ---------------------------------------------------------------------------
// Cursor FFI
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_cursor_create(
    borrowed_handles: *const *const c_void,
    num_borrowed: u32,
    batch_descs: *const GnitzBatchDesc,
    num_batches: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() { return ptr::null_mut(); }
        let schema = unsafe { &*schema_desc };

        let refs = match build_shard_refs(borrowed_handles, num_borrowed, batch_descs, num_batches, schema) {
            Some(r) => r,
            None => return ptr::null_mut(),
        };

        let cursor = crate::cursor::UnifiedCursor::new(refs, schema.clone());
        Box::into_raw(Box::new(cursor)) as *mut c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_seek(handle: *mut c_void, key_lo: u64, key_hi: u64) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let cursor = unsafe { &mut *(handle as *mut crate::cursor::UnifiedCursor) };
        cursor.seek(key_lo, key_hi);
    }));
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_advance(handle: *mut c_void) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let cursor = unsafe { &mut *(handle as *mut crate::cursor::UnifiedCursor) };
        cursor.advance();
        if cursor.is_valid() { 1 } else { 0 }
    }));
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_is_valid(handle: *const c_void) -> i32 {
    if handle.is_null() { return 0; }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    if cursor.is_valid() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_key_lo(handle: *const c_void) -> u64 {
    if handle.is_null() { return 0; }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.key_lo()
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_key_hi(handle: *const c_void) -> u64 {
    if handle.is_null() { return 0; }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.key_hi()
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_weight(handle: *const c_void) -> i64 {
    if handle.is_null() { return 0; }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.weight()
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_null_word(handle: *const c_void) -> u64 {
    if handle.is_null() { return 0; }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.null_word()
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_col_ptr(
    handle: *const c_void,
    col_idx: u32,
    col_size: u32,
) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.col_ptr(col_idx as usize, col_size as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_blob_ptr(handle: *const c_void) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.blob_ptr()
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_blob_len(handle: *const c_void) -> i64 {
    if handle.is_null() { return 0; }
    let cursor = unsafe { &*(handle as *const crate::cursor::UnifiedCursor) };
    cursor.blob_len() as i64
}

#[no_mangle]
pub extern "C" fn gnitz_cursor_close(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(handle as *mut crate::cursor::UnifiedCursor) };
    });
}

// ---------------------------------------------------------------------------
// Bulk merge FFI
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_merge_to_batch(
    batch_descs: *const GnitzBatchDesc,
    num_batches: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out: *mut GnitzMergedBatch,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() || out.is_null() { return -1; }
        let schema = unsafe { &*schema_desc };

        let refs = match build_shard_refs(ptr::null(), 0, batch_descs, num_batches, schema) {
            Some(r) => r,
            None => return -1,
        };

        let writer = crate::cursor::merge_to_writer(&refs, schema);
        let pk_index = schema.pk_index as usize;

        let out_ref = unsafe { &mut *out };
        *out_ref = GnitzMergedBatch::zeroed();
        out_ref.count = writer.count as u32;

        // Transfer ownership of Vec buffers to the output struct.
        // The caller must call gnitz_merged_batch_free to deallocate.
        let mut w = writer;
        let pk_lo = std::mem::take(&mut w.pk_lo);
        out_ref.pk_lo_len = pk_lo.len() as u64;
        out_ref.pk_lo = Box::into_raw(pk_lo.into_boxed_slice()) as *mut u8;

        let pk_hi = std::mem::take(&mut w.pk_hi);
        out_ref.pk_hi_len = pk_hi.len() as u64;
        out_ref.pk_hi = Box::into_raw(pk_hi.into_boxed_slice()) as *mut u8;

        let weight = std::mem::take(&mut w.weight);
        out_ref.weight_len = weight.len() as u64;
        out_ref.weight = Box::into_raw(weight.into_boxed_slice()) as *mut u8;

        let null_bm = std::mem::take(&mut w.null_bitmap);
        out_ref.null_bm_len = null_bm.len() as u64;
        out_ref.null_bm = Box::into_raw(null_bm.into_boxed_slice()) as *mut u8;

        let mut ci = 0u32;
        for col_idx in 0..schema.num_columns as usize {
            if col_idx == pk_index { continue; }
            let buf = std::mem::take(&mut w.col_bufs[col_idx]);
            out_ref.col_lens[ci as usize] = buf.len() as u64;
            out_ref.col_ptrs[ci as usize] = Box::into_raw(buf.into_boxed_slice()) as *mut u8;
            ci += 1;
        }
        out_ref.num_cols = ci;

        let blob = std::mem::take(&mut w.blob_heap);
        out_ref.blob_len = blob.len() as u64;
        out_ref.blob = Box::into_raw(blob.into_boxed_slice()) as *mut u8;

        0
    });
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_merged_batch_free(batch: *mut GnitzMergedBatch) {
    if batch.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let b = unsafe { &mut *batch };
        unsafe {
            if !b.pk_lo.is_null() { let _ = Box::from_raw(slice::from_raw_parts_mut(b.pk_lo, b.pk_lo_len as usize)); }
            if !b.pk_hi.is_null() { let _ = Box::from_raw(slice::from_raw_parts_mut(b.pk_hi, b.pk_hi_len as usize)); }
            if !b.weight.is_null() { let _ = Box::from_raw(slice::from_raw_parts_mut(b.weight, b.weight_len as usize)); }
            if !b.null_bm.is_null() { let _ = Box::from_raw(slice::from_raw_parts_mut(b.null_bm, b.null_bm_len as usize)); }
            for i in 0..b.num_cols as usize {
                if !b.col_ptrs[i].is_null() {
                    let _ = Box::from_raw(slice::from_raw_parts_mut(b.col_ptrs[i], b.col_lens[i] as usize));
                }
            }
            if !b.blob.is_null() { let _ = Box::from_raw(slice::from_raw_parts_mut(b.blob, b.blob_len as usize)); }
        }
    });
}

// ---------------------------------------------------------------------------
// Table FFI
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_table_create_ephemeral(
    directory: *const libc::c_char,
    name: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
    table_id: u32,
    memtable_arena: u64,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if directory.is_null() || name.is_null() || schema_desc.is_null() { return ptr::null_mut(); }
        let dir = unsafe { CStr::from_ptr(directory) }.to_str().unwrap_or("");
        let n = unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("");
        let schema = unsafe { &*schema_desc };
        match crate::table::RustTable::create_ephemeral(dir, n, schema.clone(), table_id, memtable_arena as usize) {
            Ok(t) => Box::into_raw(Box::new(t)) as *mut c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_create_persistent(
    directory: *const libc::c_char,
    name: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
    table_id: u32,
    memtable_arena: u64,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if directory.is_null() || name.is_null() || schema_desc.is_null() { return ptr::null_mut(); }
        let dir = unsafe { CStr::from_ptr(directory) }.to_str().unwrap_or("");
        let n = unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("");
        let schema = unsafe { &*schema_desc };
        match crate::table::RustTable::create_persistent(dir, n, schema.clone(), table_id, memtable_arena as usize) {
            Ok(t) => Box::into_raw(Box::new(t)) as *mut c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_close(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(handle as *mut crate::table::RustTable) };
    });
}

#[no_mangle]
pub extern "C" fn gnitz_table_ingest_batch(
    handle: *mut c_void,
    desc: *const GnitzBatchDesc,
    lsn: u64,
) -> i32 {
    if handle.is_null() || desc.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
        let d = unsafe { &*desc };
        let n_cols = d.num_payload_cols as usize;
        let (col_ptrs, col_sizes) = if n_cols > 0 && !d.col_ptrs.is_null() && !d.col_sizes.is_null() {
            (unsafe { slice::from_raw_parts(d.col_ptrs, n_cols) }.to_vec(),
             unsafe { slice::from_raw_parts(d.col_sizes, n_cols) }.iter().map(|&s| s as usize).collect::<Vec<_>>())
        } else { (vec![], vec![]) };
        let shard = crate::shard_reader::MappedShard::from_buffers(
            d.pk_lo, d.pk_hi, d.weight, d.null_bm,
            &col_ptrs, &col_sizes,
            d.blob_ptr, d.blob_len as usize,
            d.count as usize, &table.schema,
        );
        table.ingest_batch(&shard, lsn)
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_table_ingest_batch_memonly(
    handle: *mut c_void,
    desc: *const GnitzBatchDesc,
) -> i32 {
    if handle.is_null() || desc.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
        let d = unsafe { &*desc };
        let n_cols = d.num_payload_cols as usize;
        let (col_ptrs, col_sizes) = if n_cols > 0 && !d.col_ptrs.is_null() && !d.col_sizes.is_null() {
            (unsafe { slice::from_raw_parts(d.col_ptrs, n_cols) }.to_vec(),
             unsafe { slice::from_raw_parts(d.col_sizes, n_cols) }.iter().map(|&s| s as usize).collect::<Vec<_>>())
        } else { (vec![], vec![]) };
        let shard = crate::shard_reader::MappedShard::from_buffers(
            d.pk_lo, d.pk_hi, d.weight, d.null_bm,
            &col_ptrs, &col_sizes,
            d.blob_ptr, d.blob_len as usize,
            d.count as usize, &table.schema,
        );
        table.ingest_batch_memonly(&shard)
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_table_ingest_one(
    handle: *mut c_void,
    key_lo: u64, key_hi: u64, weight: i64,
    null_word: u64,
    col_ptrs: *const *const u8,
    num_payload_cols: u32,
    source_blob: *const u8, source_blob_len: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
        let n = num_payload_cols as usize;
        let ptrs = if n > 0 && !col_ptrs.is_null() {
            unsafe { slice::from_raw_parts(col_ptrs, n) }
        } else { &[] };
        table.ingest_one(key_lo, key_hi, weight, null_word, ptrs, source_blob, source_blob_len as usize)
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_table_flush(handle: *mut c_void) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
        match table.flush() {
            Ok(true) => 1,
            Ok(false) => 0,
            Err(e) => e,
        }
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_table_create_cursor(handle: *mut c_void) -> *mut c_void {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
        let cursor = table.create_cursor();
        Box::into_raw(Box::new(cursor)) as *mut c_void
    }));
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_has_pk(handle: *const c_void, key_lo: u64, key_hi: u64) -> i32 {
    if handle.is_null() { return 0; }
    let table = unsafe { &*(handle as *const crate::table::RustTable) };
    if table.has_pk(key_lo, key_hi) { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_table_get_weight(
    handle: *const c_void,
    key_lo: u64, key_hi: u64,
    null_word: u64,
    col_ptrs: *const *const u8,
    col_sizes: *const u64,
    num_payload_cols: u32,
    blob_ptr: *const u8, blob_len: u64,
) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &*(handle as *const crate::table::RustTable) };
        let n = num_payload_cols as usize;
        let (ptrs, sizes) = if n > 0 && !col_ptrs.is_null() && !col_sizes.is_null() {
            (unsafe { slice::from_raw_parts(col_ptrs, n) }.to_vec(),
             unsafe { slice::from_raw_parts(col_sizes, n) }.iter().map(|&s| s as usize).collect::<Vec<_>>())
        } else { (vec![], vec![]) };
        table.get_weight_for_row(key_lo, key_hi, &ptrs, &sizes, null_word, blob_ptr, blob_len as usize)
    }));
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_retract_pk(
    handle: *const c_void,
    key_lo: u64, key_hi: u64,
    out_shard: *mut *const c_void,
    out_row: *mut i32,
    out_weight: *mut i64,
) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &*(handle as *const crate::table::RustTable) };
        let (total_w, found) = table.retract_pk_find(key_lo, key_hi);
        if let Some((shard, row)) = found {
            if !out_shard.is_null() {
                unsafe { *out_shard = Box::into_raw(Box::new(shard)) as *const c_void; }
            }
            if !out_row.is_null() { unsafe { *out_row = row as i32; } }
            if !out_weight.is_null() { unsafe { *out_weight = total_w; } }
            1
        } else {
            if !out_weight.is_null() { unsafe { *out_weight = total_w; } }
            0
        }
    }));
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_create_child(
    handle: *const c_void,
    name: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut c_void {
    if handle.is_null() || name.is_null() || schema_desc.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let table = unsafe { &*(handle as *const crate::table::RustTable) };
        let n = unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("");
        let schema = unsafe { &*schema_desc };
        match table.create_child(n, schema.clone()) {
            Ok(t) => Box::into_raw(Box::new(t)) as *mut c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_should_flush(handle: *const c_void) -> i32 {
    if handle.is_null() { return 0; }
    let table = unsafe { &*(handle as *const crate::table::RustTable) };
    if table.should_flush() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_table_is_empty(handle: *const c_void) -> i32 {
    if handle.is_null() { return 1; }
    let table = unsafe { &*(handle as *const crate::table::RustTable) };
    if table.is_empty() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_table_compact_if_needed(handle: *mut c_void) -> i32 {
    if handle.is_null() { return -1; }
    let _ = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
        table.compact_if_needed();
    }));
    0
}

#[no_mangle]
pub extern "C" fn gnitz_table_current_lsn(handle: *const c_void) -> u64 {
    if handle.is_null() { return 0; }
    let table = unsafe { &*(handle as *const crate::table::RustTable) };
    table.current_lsn
}

#[no_mangle]
pub extern "C" fn gnitz_table_set_lsn(handle: *mut c_void, lsn: u64) {
    if handle.is_null() { return; }
    let table = unsafe { &mut *(handle as *mut crate::table::RustTable) };
    table.current_lsn = lsn;
}

// ---------------------------------------------------------------------------
// MemTable FFI
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_memtable_create(
    schema_desc: *const crate::compact::SchemaDescriptor,
    max_bytes: u64,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() { return ptr::null_mut(); }
        let schema = unsafe { &*schema_desc };
        let mt = crate::memtable::RustMemTable::new(schema.clone(), max_bytes as usize);
        Box::into_raw(Box::new(mt)) as *mut c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_free(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(handle as *mut crate::memtable::RustMemTable) };
    });
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_upsert_batch(
    handle: *mut c_void,
    desc: *const GnitzBatchDesc,
) -> i32 {
    if handle.is_null() || desc.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::RustMemTable) };
        let d = unsafe { &*desc };
        let schema = mt.schema_ref();
        let n_cols = d.num_payload_cols as usize;
        let (col_ptrs, col_sizes) = if n_cols > 0 && !d.col_ptrs.is_null() && !d.col_sizes.is_null() {
            let ptrs = unsafe { slice::from_raw_parts(d.col_ptrs, n_cols) };
            let raw_sizes = unsafe { slice::from_raw_parts(d.col_sizes, n_cols) };
            let sizes: Vec<usize> = raw_sizes.iter().map(|&s| s as usize).collect();
            (ptrs.to_vec(), sizes)
        } else {
            (vec![ptr::null(); n_cols], vec![0usize; n_cols])
        };
        let shard = crate::shard_reader::MappedShard::from_buffers(
            d.pk_lo, d.pk_hi, d.weight, d.null_bm,
            &col_ptrs, &col_sizes,
            d.blob_ptr, d.blob_len as usize,
            d.count as usize, schema,
        );
        mt.upsert_batch(&shard)
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_append_row(
    handle: *mut c_void,
    key_lo: u64, key_hi: u64, weight: i64,
    null_word: u64,
    col_ptrs: *const *const u8,
    num_payload_cols: u32,
    source_blob: *const u8,
    source_blob_len: u64,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::RustMemTable) };
        let n = num_payload_cols as usize;
        let ptrs = if n > 0 && !col_ptrs.is_null() {
            unsafe { slice::from_raw_parts(col_ptrs, n) }
        } else {
            &[]
        };
        mt.append_row(
            key_lo, key_hi, weight, null_word,
            ptrs, source_blob, source_blob_len as usize,
        )
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_flush(
    handle: *mut c_void,
    dirfd: i32,
    filename: *const libc::c_char,
    table_id: u32,
    durable: i32,
) -> i32 {
    if handle.is_null() || filename.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::RustMemTable) };
        let cname = unsafe { CStr::from_ptr(filename) };
        match mt.flush(dirfd, cname, table_id, durable != 0) {
            Ok(true) => 1,
            Ok(false) => 0,
            Err(e) => e,
        }
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_get_snapshot(
    handle: *mut c_void,
    out: *mut GnitzMergedBatch,
) -> i32 {
    if handle.is_null() || out.is_null() { return -1; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::RustMemTable) };
        let mut snapshot = mt.get_snapshot();
        let schema = mt.schema_ref().clone();
        let pk_index = schema.pk_index as usize;

        let out_ref = unsafe { &mut *out };
        *out_ref = GnitzMergedBatch::zeroed();
        out_ref.count = snapshot.count as u32;

        fn vec_to_ptr(v: Vec<u8>, out_ptr: &mut *mut u8, out_len: &mut u64) {
            *out_len = v.len() as u64;
            *out_ptr = Box::into_raw(v.into_boxed_slice()) as *mut u8;
        }

        vec_to_ptr(std::mem::take(&mut snapshot.pk_lo), &mut out_ref.pk_lo, &mut out_ref.pk_lo_len);
        vec_to_ptr(std::mem::take(&mut snapshot.pk_hi), &mut out_ref.pk_hi, &mut out_ref.pk_hi_len);
        vec_to_ptr(std::mem::take(&mut snapshot.weight), &mut out_ref.weight, &mut out_ref.weight_len);
        vec_to_ptr(std::mem::take(&mut snapshot.null_bm), &mut out_ref.null_bm, &mut out_ref.null_bm_len);

        let mut ci = 0u32;
        for col_idx in 0..schema.num_columns as usize {
            if col_idx == pk_index { continue; }
            let buf = std::mem::take(&mut snapshot.cols[col_idx]);
            out_ref.col_lens[ci as usize] = buf.len() as u64;
            out_ref.col_ptrs[ci as usize] = Box::into_raw(buf.into_boxed_slice()) as *mut u8;
            ci += 1;
        }
        out_ref.num_cols = ci;

        vec_to_ptr(std::mem::take(&mut snapshot.blob), &mut out_ref.blob, &mut out_ref.blob_len);
        0
    }));
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_lookup_pk(
    handle: *mut c_void,
    key_lo: u64, key_hi: u64,
    out_weight: *mut i64,
    out_shard: *mut *const c_void,
    out_row: *mut i32,
) -> i32 {
    if handle.is_null() || out_weight.is_null() { return 0; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::RustMemTable) };
        let (weight, found) = mt.lookup_pk(key_lo, key_hi);
        unsafe { *out_weight = weight; }
        if let Some((shard, row)) = found {
            // Store the shard as a boxed handle so caller can read via gnitz_shard_* FFI
            if !out_shard.is_null() {
                unsafe { *out_shard = Box::into_raw(Box::new(shard)) as *const c_void; }
            }
            if !out_row.is_null() {
                unsafe { *out_row = row as i32; }
            }
            1
        } else {
            if !out_shard.is_null() { unsafe { *out_shard = ptr::null(); } }
            if !out_row.is_null() { unsafe { *out_row = -1; } }
            0
        }
    }));
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_find_weight(
    handle: *mut c_void,
    key_lo: u64, key_hi: u64,
    null_word: u64,
    col_ptrs: *const *const u8,
    col_sizes: *const u64,
    num_payload_cols: u32,
    blob_ptr: *const u8, blob_len: u64,
) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::RustMemTable) };
        let n = num_payload_cols as usize;
        let (ptrs, sizes) = if n > 0 && !col_ptrs.is_null() && !col_sizes.is_null() {
            let p = unsafe { slice::from_raw_parts(col_ptrs, n) };
            let s: Vec<usize> = unsafe { slice::from_raw_parts(col_sizes, n) }
                .iter().map(|&v| v as usize).collect();
            (p.to_vec(), s)
        } else {
            (vec![], vec![])
        };
        mt.find_weight_for_row(
            key_lo, key_hi, &ptrs, &sizes, null_word,
            blob_ptr, blob_len as usize,
        )
    }));
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_may_contain(handle: *const c_void, key_lo: u64, key_hi: u64) -> i32 {
    if handle.is_null() { return 0; }
    let mt = unsafe { &*(handle as *const crate::memtable::RustMemTable) };
    if mt.may_contain_pk(key_lo, key_hi) { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_should_flush(handle: *const c_void) -> i32 {
    if handle.is_null() { return 0; }
    let mt = unsafe { &*(handle as *const crate::memtable::RustMemTable) };
    if mt.should_flush() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_is_empty(handle: *const c_void) -> i32 {
    if handle.is_null() { return 1; }
    let mt = unsafe { &*(handle as *const crate::memtable::RustMemTable) };
    if mt.is_empty() { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_total_rows(handle: *const c_void) -> u32 {
    if handle.is_null() { return 0; }
    let mt = unsafe { &*(handle as *const crate::memtable::RustMemTable) };
    mt.total_rows() as u32
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_reset(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::RustMemTable) };
        mt.reset();
    }));
}

// ---------------------------------------------------------------------------
// FFI tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor8_ffi_roundtrip() {
        let lo: Vec<u64> = (0..100).collect();
        let hi: Vec<u64> = vec![0; 100];

        let handle = gnitz_xor8_build(lo.as_ptr(), hi.as_ptr(), 100);
        assert!(!handle.is_null());

        // Query
        for i in 0..100u64 {
            assert_eq!(gnitz_xor8_may_contain(handle, i, 0), 1);
        }

        // Serialize
        let size = gnitz_xor8_serialize(handle, ptr::null_mut(), 0);
        assert!(size > 0);
        let mut buf = vec![0u8; size as usize];
        let written = gnitz_xor8_serialize(handle, buf.as_mut_ptr(), size);
        assert_eq!(written, size);

        // Deserialize
        let handle2 = gnitz_xor8_deserialize(buf.as_ptr(), written);
        assert!(!handle2.is_null());
        for i in 0..100u64 {
            assert_eq!(gnitz_xor8_may_contain(handle2, i, 0), 1);
        }

        gnitz_xor8_free(handle);
        gnitz_xor8_free(handle2);
    }

    #[test]
    fn bloom_ffi_roundtrip() {
        let handle = gnitz_bloom_create(100);
        assert!(!handle.is_null());

        for i in 0..100u64 {
            gnitz_bloom_add(handle, i, 0);
        }
        for i in 0..100u64 {
            assert_eq!(gnitz_bloom_may_contain(handle, i, 0), 1);
        }

        gnitz_bloom_reset(handle);
        // After reset, should not contain anything
        let mut found = 0;
        for i in 0..100u64 {
            found += gnitz_bloom_may_contain(handle, i, 0);
        }
        assert_eq!(found, 0);

        gnitz_bloom_free(handle);
    }

    #[test]
    fn null_handle_safety() {
        assert_eq!(gnitz_xor8_may_contain(ptr::null(), 1, 2), 0);
        assert_eq!(gnitz_xor8_serialize(ptr::null(), ptr::null_mut(), 0), -1);
        assert!(gnitz_xor8_deserialize(ptr::null(), 0).is_null());
        gnitz_xor8_free(ptr::null_mut());

        assert_eq!(gnitz_bloom_may_contain(ptr::null(), 1, 2), 0);
        gnitz_bloom_add(ptr::null_mut(), 1, 2); // should not crash
        gnitz_bloom_reset(ptr::null_mut());
        gnitz_bloom_free(ptr::null_mut());
    }

    #[test]
    fn xxh3_hash_u128_ffi() {
        let h1 = gnitz_xxh3_hash_u128(42, 99, 0, 0);
        let h2 = gnitz_xxh3_hash_u128(42, 99, 0, 0);
        assert_eq!(h1, h2);
        // Matches internal hash_u128
        assert_eq!(h1, crate::xxh::hash_u128(42, 99));
        // Non-zero seeds differ
        assert_ne!(gnitz_xxh3_hash_u128(42, 99, 1, 0), h1);
    }

    #[test]
    fn xor8_build_empty() {
        let handle = gnitz_xor8_build(ptr::null(), ptr::null(), 0);
        assert!(handle.is_null());
    }

    // -----------------------------------------------------------------------
    // WAL FFI null safety
    // -----------------------------------------------------------------------

    #[test]
    fn wal_encode_null_safety() {
        assert_eq!(gnitz_wal_encode(ptr::null_mut(), 0, 0, 0, 0, 0, ptr::null(), ptr::null(), 0, 0), -1);
    }

    #[test]
    fn wal_parse_null_safety() {
        // All output pointers null
        assert_eq!(
            gnitz_wal_validate_and_parse(
                ptr::null(), 0,
                ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                ptr::null_mut(), ptr::null_mut(),
                ptr::null_mut(), ptr::null_mut(), 0,
            ),
            crate::wal::WAL_ERR_TRUNCATED,
        );

        // Block valid but output pointers null
        let mut buf = vec![0u8; 256];
        // Write a minimal valid WAL block
        let ptrs: Vec<*const u8> = vec![];
        let sizes: Vec<u32> = vec![];
        let new_off = crate::wal::encode(&mut buf, 0, 1, 1, 0, &ptrs, &sizes, 0);
        assert!(new_off > 0);

        assert_eq!(
            gnitz_wal_validate_and_parse(
                buf.as_ptr(), new_off,
                ptr::null_mut(), ptr::null_mut(), ptr::null_mut(),
                ptr::null_mut(), ptr::null_mut(),
                ptr::null_mut(), ptr::null_mut(), 0,
            ),
            crate::wal::WAL_ERR_TRUNCATED,
        );
    }

    #[test]
    fn wal_ffi_roundtrip() {
        let r0 = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let ptrs = vec![r0.as_ptr()];
        let sizes = vec![8u32];
        let mut buf = vec![0u8; 4096];

        let new_off = gnitz_wal_encode(
            buf.as_mut_ptr(), 0, 4096,
            42, 7, 1,
            ptrs.as_ptr() as *const *const u8, sizes.as_ptr(), 1, 0,
        );
        assert!(new_off > 0);

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u32; 4];
        let mut rsizes = [0u32; 4];

        let rc = gnitz_wal_validate_and_parse(
            buf.as_ptr(), new_off,
            &mut lsn, &mut tid, &mut count,
            &mut num_regions, &mut blob_size,
            offsets.as_mut_ptr(), rsizes.as_mut_ptr(), 4,
        );
        assert_eq!(rc, 0);
        assert_eq!(lsn, 42);
        assert_eq!(tid, 7);
        assert_eq!(count, 1);
        assert_eq!(num_regions, 1);
    }

    // -----------------------------------------------------------------------
    // Manifest FFI null safety
    // -----------------------------------------------------------------------

    #[test]
    fn manifest_serialize_null_safety() {
        assert_eq!(gnitz_manifest_serialize(ptr::null_mut(), 0, ptr::null(), 0, 0), -1);
    }

    #[test]
    fn manifest_parse_null_safety() {
        assert_eq!(
            gnitz_manifest_parse(ptr::null(), 0, ptr::null_mut(), 0, ptr::null_mut()),
            crate::manifest::MANIFEST_ERR_TRUNCATED,
        );
    }

    #[test]
    fn manifest_ffi_roundtrip() {
        let mut entry = crate::manifest::ManifestEntryRaw::zeroed();
        entry.table_id = 42;
        entry.pk_min_lo = 1;
        entry.min_lsn = 10;
        entry.max_lsn = 20;
        entry.filename[..6].copy_from_slice(b"t.db\x00\x00");

        let entries = [entry];
        let mut buf = vec![0u8; 512];
        let written = gnitz_manifest_serialize(
            buf.as_mut_ptr(), 512,
            entries.as_ptr() as *const u8, 1, 99,
        );
        assert!(written > 0);

        let mut out = [crate::manifest::ManifestEntryRaw::zeroed()];
        let mut lsn = 0u64;
        let count = gnitz_manifest_parse(
            buf.as_ptr(), written,
            out.as_mut_ptr() as *mut u8, 1, &mut lsn,
        );
        assert_eq!(count, 1);
        assert_eq!(lsn, 99);
        assert_eq!(out[0].table_id, 42);
    }

    // -----------------------------------------------------------------------
    // Compact FFI null safety
    // -----------------------------------------------------------------------

    #[test]
    fn compact_null_safety() {
        assert_eq!(gnitz_compact_shards(ptr::null(), 0, ptr::null(), ptr::null(), 0), -1);
    }

    #[test]
    fn merge_and_route_null_safety() {
        assert_eq!(
            gnitz_merge_and_route(
                ptr::null(), 0, ptr::null(),
                ptr::null(), 0, ptr::null(),
                0, 0, 0, ptr::null_mut(), 0,
            ),
            -1,
        );
    }

    // -----------------------------------------------------------------------
    // Buffer-backed shard FFI
    // -----------------------------------------------------------------------

    #[test]
    fn from_buffers_ffi_null_schema() {
        let handle = gnitz_shard_open_from_buffers(
            ptr::null(), ptr::null(), ptr::null(), ptr::null(),
            ptr::null(), ptr::null(), 0,
            ptr::null(), 0, 0, ptr::null(),
        );
        assert!(handle.is_null());
    }

    #[test]
    fn from_buffers_ffi_roundtrip() {
        let pk_lo: Vec<u64> = vec![5, 10, 15];
        let pk_hi: Vec<u64> = vec![0, 0, 0];
        let weights: Vec<i64> = vec![1, 2, 3];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let col1: Vec<i64> = vec![50, 100, 150];

        let schema = crate::compact::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: {
                let mut cols = [crate::compact::SchemaColumn {
                    type_code: 8, size: 8, nullable: 0, _pad: 0,
                }; 64];
                cols[1] = crate::compact::SchemaColumn {
                    type_code: 9, size: 8, nullable: 0, _pad: 0,
                };
                cols
            },
        };

        let col_ptrs_arr: Vec<*const u8> = vec![col1.as_ptr() as *const u8];
        let col_sizes_arr: Vec<u64> = vec![24]; // 3 * 8

        let handle = gnitz_shard_open_from_buffers(
            pk_lo.as_ptr() as *const u8,
            pk_hi.as_ptr() as *const u8,
            weights.as_ptr() as *const u8,
            nulls.as_ptr() as *const u8,
            col_ptrs_arr.as_ptr(),
            col_sizes_arr.as_ptr(),
            1, // 1 payload column
            ptr::null(), 0,
            3,
            &schema as *const crate::compact::SchemaDescriptor,
        );
        assert!(!handle.is_null());

        assert_eq!(gnitz_shard_row_count(handle), 3);
        assert_eq!(gnitz_shard_get_pk_lo(handle, 0), 5);
        assert_eq!(gnitz_shard_get_pk_lo(handle, 2), 15);
        assert_eq!(gnitz_shard_get_weight(handle, 1), 2);
        assert_eq!(gnitz_shard_has_xor8(handle), 0);
        assert_eq!(gnitz_shard_xor8_may_contain(handle, 5, 0), 1); // conservative

        assert_eq!(gnitz_shard_find_row(handle, 10, 0), 1);
        assert_eq!(gnitz_shard_find_row(handle, 7, 0), -1);
        assert_eq!(gnitz_shard_lower_bound(handle, 12, 0), 2);

        let col_ptr = gnitz_shard_col_ptr(handle, 1, 1, 8);
        assert!(!col_ptr.is_null());
        assert_eq!(unsafe { *(col_ptr as *const i64) }, 100);

        gnitz_shard_close(handle);
    }

    // -----------------------------------------------------------------------
    // Write shard FFI
    // -----------------------------------------------------------------------

    #[test]
    fn write_shard_null_safety() {
        assert_eq!(
            gnitz_write_shard(
                libc::AT_FDCWD, ptr::null(), 0, 0,
                ptr::null(), ptr::null(), 0,
                ptr::null(), ptr::null(), 0,
            ),
            crate::shard_file::ERR_BAD_ARGS,
        );
    }

    #[test]
    fn write_shard_ffi_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ffi_rt.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let pk_lo: Vec<u64> = vec![5, 10];
        let pk_hi: Vec<u64> = vec![0, 0];
        let weights: Vec<i64> = vec![1, 1];
        let nulls: Vec<u64> = vec![0, 0];
        let col1: Vec<i64> = vec![50, 100];

        let region_ptrs: Vec<*const u8> = vec![
            pk_lo.as_ptr() as *const u8,
            pk_hi.as_ptr() as *const u8,
            weights.as_ptr() as *const u8,
            nulls.as_ptr() as *const u8,
            col1.as_ptr() as *const u8,
            ptr::null(), // empty blob
        ];
        let region_sizes: Vec<u64> = vec![16, 16, 16, 16, 16, 0];

        let rc = gnitz_write_shard(
            libc::AT_FDCWD,
            cpath.as_ptr(),
            99, 2,
            region_ptrs.as_ptr(), region_sizes.as_ptr(), 6,
            pk_lo.as_ptr(), pk_hi.as_ptr(),
            0,
        );
        assert_eq!(rc, 0);

        // Read back via shard reader FFI
        let schema = crate::compact::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: {
                let mut cols = [crate::compact::SchemaColumn {
                    type_code: 8, size: 8, nullable: 0, _pad: 0,
                }; 64];
                cols[1] = crate::compact::SchemaColumn {
                    type_code: 9, size: 8, nullable: 0, _pad: 0,
                };
                cols
            },
        };
        let handle = gnitz_shard_open(
            cpath.as_ptr(),
            &schema as *const crate::compact::SchemaDescriptor,
            1,
        );
        assert!(!handle.is_null());
        assert_eq!(gnitz_shard_row_count(handle), 2);
        assert_eq!(gnitz_shard_get_pk_lo(handle, 0), 5);
        assert_eq!(gnitz_shard_get_pk_lo(handle, 1), 10);
        assert_eq!(gnitz_shard_has_xor8(handle), 1);
        assert_eq!(gnitz_shard_xor8_may_contain(handle, 5, 0), 1);
        gnitz_shard_close(handle);
    }
}
