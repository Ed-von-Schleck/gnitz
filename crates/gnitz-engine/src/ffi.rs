use std::ffi::CStr;
use std::panic;
use std::ptr;
use std::slice;

use libc::{c_int, c_void};
use xorf::Xor8;

use crate::{bloom::BloomFilter, xor8};

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Initialize the Rust logging subsystem.
/// level: 0=QUIET, 1=NORMAL, 2=DEBUG. tag: process tag bytes (e.g. "M", "W0").
#[no_mangle]
pub extern "C" fn gnitz_log_init(level: u32, tag_ptr: *const u8, tag_len: u32) {
    let tag = if tag_ptr.is_null() || tag_len == 0 {
        &[] as &[u8]
    } else {
        unsafe { slice::from_raw_parts(tag_ptr, tag_len.min(3) as usize) }
    };
    crate::log::init(level, tag);
}

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
// Manifest file I/O
// ---------------------------------------------------------------------------

/// Read a manifest file from disk. Returns entry count (>= 0) on success,
/// negative on error: -1 = bad magic, -2 = truncated, -3 = I/O error.
#[no_mangle]
pub extern "C" fn gnitz_manifest_read_file(
    path: *const libc::c_char,
    out_entries: *mut u8,
    max_entries: u32,
    out_global_max_lsn: *mut u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if path.is_null() || out_global_max_lsn.is_null() {
            return -1;
        }
        let cpath = unsafe { CStr::from_ptr(path) };
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
        crate::manifest::read_file(cpath, entries, max_entries, lsn)
    });
    result.unwrap_or(crate::manifest::MANIFEST_ERR_IO)
}

/// Write manifest entries atomically (serialize + .tmp + fdatasync + rename).
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_manifest_write_file(
    path: *const libc::c_char,
    entries_buf: *const u8,
    count: u32,
    global_max_lsn: u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if path.is_null() {
            return -1;
        }
        let cpath = unsafe { CStr::from_ptr(path) };
        let n = count as usize;
        let entries = if n > 0 && !entries_buf.is_null() {
            unsafe {
                slice::from_raw_parts(
                    entries_buf as *const crate::manifest::ManifestEntryRaw,
                    n,
                )
            }
        } else {
            &[]
        };
        crate::manifest::write_file(cpath, entries, global_max_lsn)
    });
    result.unwrap_or(crate::manifest::MANIFEST_ERR_IO)
}

// ---------------------------------------------------------------------------
// WAL writer lifecycle
// ---------------------------------------------------------------------------

/// Open a WAL file for writing (O_WRONLY | O_CREAT | O_APPEND) with exclusive flock.
/// Returns handle on success, null on error. Error code written to *out_error:
///   0 = success, -2 = lock contention, -3 = I/O error.
#[no_mangle]
pub extern "C" fn gnitz_wal_writer_open(
    path: *const libc::c_char,
    out_error: *mut i32,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if path.is_null() || out_error.is_null() {
            return ptr::null_mut();
        }
        let cpath = unsafe { CStr::from_ptr(path) };
        match crate::wal::WalWriter::open(cpath) {
            Ok(writer) => {
                unsafe { *out_error = 0; }
                Box::into_raw(Box::new(writer)) as *mut c_void
            }
            Err(code) => {
                unsafe { *out_error = code; }
                ptr::null_mut()
            }
        }
    });
    result.unwrap_or(ptr::null_mut())
}

/// Append an encoded WAL block (encode + write + fdatasync).
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_wal_writer_append(
    handle: *mut c_void,
    lsn: u64,
    table_id: u32,
    count: u32,
    region_ptrs: *const *const u8,
    region_sizes: *const u32,
    num_regions: u32,
    blob_size: u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() || region_ptrs.is_null() || region_sizes.is_null() {
            return -1;
        }
        let writer = unsafe { &mut *(handle as *mut crate::wal::WalWriter) };
        let n = num_regions as usize;
        let ptrs = unsafe { slice::from_raw_parts(region_ptrs, n) };
        let sizes = unsafe { slice::from_raw_parts(region_sizes, n) };
        writer.append_batch(lsn, table_id, count, ptrs, sizes, blob_size)
    });
    result.unwrap_or(-99)
}

/// Truncate the WAL file to zero length.
/// Returns 0 on success, -3 on I/O error.
#[no_mangle]
pub extern "C" fn gnitz_wal_writer_truncate(handle: *mut c_void) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() { return -1; }
        let writer = unsafe { &mut *(handle as *mut crate::wal::WalWriter) };
        writer.truncate()
    });
    result.unwrap_or(-99)
}

/// Close and free a WAL writer handle (unlocks, closes fd).
#[no_mangle]
pub extern "C" fn gnitz_wal_writer_close(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        unsafe { drop(Box::from_raw(handle as *mut crate::wal::WalWriter)); }
    });
}

// ---------------------------------------------------------------------------
// WAL reader lifecycle
// ---------------------------------------------------------------------------

/// Open a WAL file for reading (mmap, read-only).
/// Returns handle on success, null on error.
/// *out_base_ptr receives the mmap base pointer (for Buffer.from_existing_data).
/// *out_file_size receives the file size in bytes.
#[no_mangle]
pub extern "C" fn gnitz_wal_reader_open(
    path: *const libc::c_char,
    out_base_ptr: *mut *const u8,
    out_file_size: *mut i64,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if path.is_null() || out_base_ptr.is_null() || out_file_size.is_null() {
            return ptr::null_mut();
        }
        let cpath = unsafe { CStr::from_ptr(path) };
        match crate::wal::WalReader::open(cpath) {
            Ok(reader) => {
                unsafe {
                    *out_base_ptr = reader.base_ptr();
                    *out_file_size = reader.file_size() as i64;
                }
                Box::into_raw(Box::new(reader)) as *mut c_void
            }
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

/// Read the next WAL block from the reader.
/// Returns 0=success, 1=EOF, -4=corrupt.
/// Region offsets are absolute (relative to mmap base), so RPython can use
/// rffi.ptradd(base_ptr, out_offsets[i]) directly.
#[no_mangle]
pub extern "C" fn gnitz_wal_reader_next(
    handle: *mut c_void,
    out_lsn: *mut u64,
    out_tid: *mut u32,
    out_count: *mut u32,
    out_num_regions: *mut u32,
    out_blob_size: *mut u64,
    out_offsets: *mut u32,
    out_sizes: *mut u32,
    max_regions: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return -1;
        }
        let reader = unsafe { &mut *(handle as *mut crate::wal::WalReader) };
        let n = max_regions as usize;
        let offsets = if out_offsets.is_null() { &mut [] } else {
            unsafe { slice::from_raw_parts_mut(out_offsets, n) }
        };
        let sizes = if out_sizes.is_null() { &mut [] } else {
            unsafe { slice::from_raw_parts_mut(out_sizes, n) }
        };
        reader.read_next_block(
            unsafe { &mut *out_lsn },
            unsafe { &mut *out_tid },
            unsafe { &mut *out_count },
            unsafe { &mut *out_num_regions },
            unsafe { &mut *out_blob_size },
            offsets, sizes, max_regions,
        )
    });
    result.unwrap_or(-99)
}

/// Close and free a WAL reader handle (munmap, close fd).
#[no_mangle]
pub extern "C" fn gnitz_wal_reader_close(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        unsafe { drop(Box::from_raw(handle as *mut crate::wal::WalReader)); }
    });
}

// ---------------------------------------------------------------------------
// Shard file writing
// ---------------------------------------------------------------------------

/// Write a shard file atomically from pre-built region buffers.
///
/// dirfd: directory fd for openat/renameat. Use AT_FDCWD (-100) for absolute paths.
/// basename: null-terminated filename (relative to dirfd).
/// table_id: table identifier stored in shard header.
/// row_count: number of rows in the shard.
/// region_ptrs: array of pointers to region data.
/// region_sizes: array of sizes (bytes) for each region.
/// num_regions: length of region_ptrs and region_sizes arrays.
/// durable: if nonzero, fdatasync before rename.
///
/// Returns 0 on success, negative on failure:
///   -1: null/invalid arguments
///   -3: I/O error (open/write/sync/rename)
#[no_mangle]
pub extern "C" fn gnitz_write_shard(
    dirfd: c_int,
    basename: *const libc::c_char,
    table_id: u32,
    row_count: u32,
    region_ptrs: *const *const u8,
    region_sizes: *const u32,
    num_regions: u32,
    durable: c_int,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if basename.is_null() || region_ptrs.is_null() || region_sizes.is_null() {
            return -1;
        }
        let n = num_regions as usize;
        let ptrs = unsafe { slice::from_raw_parts(region_ptrs, n) };
        let sizes = unsafe { slice::from_raw_parts(region_sizes, n) };

        let mut regions: Vec<(*const u8, usize)> = Vec::with_capacity(n);
        for i in 0..n {
            regions.push((ptrs[i], sizes[i] as usize));
        }

        let image = crate::shard_file::build_shard_image(table_id, row_count, &regions);
        let basename_cstr = unsafe { CStr::from_ptr(basename) };
        crate::shard_file::write_shard_at(dirfd, basename_cstr, &image, durable != 0)
    });
    result.unwrap_or(-99)
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


// ---------------------------------------------------------------------------
// DBSP operator FFI
// ---------------------------------------------------------------------------

/// Distinct operator: returns output batch + consolidated delta.
/// Both out handles must be freed by the caller.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_op_distinct(
    delta_handle: *const libc::c_void,
    cursor_handle: *mut libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
    out_consolidated: *mut *mut libc::c_void,
) -> i32 {
    if delta_handle.is_null() || cursor_handle.is_null() || schema_desc.is_null()
        || out_result.is_null() || out_consolidated.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let delta = unsafe { &*(delta_handle as *const crate::memtable::OwnedBatch) };
        let ch = unsafe { &mut *(cursor_handle as *mut crate::read_cursor::CursorHandle) };
        let schema = unsafe { &*schema_desc };
        let (output, consolidated) = crate::ops::op_distinct(delta, &mut ch.cursor, schema);
        unsafe {
            *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void;
            *out_consolidated = Box::into_raw(Box::new(consolidated)) as *mut libc::c_void;
        }
        0
    });
    result.unwrap_or(-99)
}

/// Anti-join delta-trace: returns output batch.
#[no_mangle]
pub extern "C" fn gnitz_op_anti_join_dt(
    delta_handle: *const libc::c_void,
    cursor_handle: *mut libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if delta_handle.is_null() || cursor_handle.is_null() || schema_desc.is_null()
        || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let delta = unsafe { &*(delta_handle as *const crate::memtable::OwnedBatch) };
        let ch = unsafe { &mut *(cursor_handle as *mut crate::read_cursor::CursorHandle) };
        let schema = unsafe { &*schema_desc };
        let output = crate::ops::op_anti_join_delta_trace(delta, &mut ch.cursor, schema);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Semi-join delta-trace: returns output batch.
#[no_mangle]
pub extern "C" fn gnitz_op_semi_join_dt(
    delta_handle: *const libc::c_void,
    cursor_handle: *mut libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if delta_handle.is_null() || cursor_handle.is_null() || schema_desc.is_null()
        || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let delta = unsafe { &*(delta_handle as *const crate::memtable::OwnedBatch) };
        let ch = unsafe { &mut *(cursor_handle as *mut crate::read_cursor::CursorHandle) };
        let schema = unsafe { &*schema_desc };
        let output = crate::ops::op_semi_join_delta_trace(delta, &mut ch.cursor, schema);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Inner join delta-trace: returns output batch with composite schema.
#[no_mangle]
pub extern "C" fn gnitz_op_join_dt(
    delta_handle: *const libc::c_void,
    cursor_handle: *mut libc::c_void,
    left_schema: *const crate::compact::SchemaDescriptor,
    right_schema: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if delta_handle.is_null() || cursor_handle.is_null()
        || left_schema.is_null() || right_schema.is_null() || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let delta = unsafe { &*(delta_handle as *const crate::memtable::OwnedBatch) };
        let ch = unsafe { &mut *(cursor_handle as *mut crate::read_cursor::CursorHandle) };
        let ls = unsafe { &*left_schema };
        let rs = unsafe { &*right_schema };
        let output = crate::ops::op_join_delta_trace(delta, &mut ch.cursor, ls, rs);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Left outer join delta-trace: returns output batch with composite schema.
#[no_mangle]
pub extern "C" fn gnitz_op_join_dt_outer(
    delta_handle: *const libc::c_void,
    cursor_handle: *mut libc::c_void,
    left_schema: *const crate::compact::SchemaDescriptor,
    right_schema: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if delta_handle.is_null() || cursor_handle.is_null()
        || left_schema.is_null() || right_schema.is_null() || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let delta = unsafe { &*(delta_handle as *const crate::memtable::OwnedBatch) };
        let ch = unsafe { &mut *(cursor_handle as *mut crate::read_cursor::CursorHandle) };
        let ls = unsafe { &*left_schema };
        let rs = unsafe { &*right_schema };
        let output = crate::ops::op_join_delta_trace_outer(delta, &mut ch.cursor, ls, rs);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Anti-join delta-delta: returns output batch.
#[no_mangle]
pub extern "C" fn gnitz_op_anti_join_dd(
    batch_a: *const libc::c_void,
    batch_b: *const libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_a.is_null() || batch_b.is_null() || schema_desc.is_null()
        || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let a = unsafe { &*(batch_a as *const crate::memtable::OwnedBatch) };
        let b = unsafe { &*(batch_b as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_desc };
        let output = crate::ops::op_anti_join_delta_delta(a, b, schema);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Semi-join delta-delta: returns output batch.
#[no_mangle]
pub extern "C" fn gnitz_op_semi_join_dd(
    batch_a: *const libc::c_void,
    batch_b: *const libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_a.is_null() || batch_b.is_null() || schema_desc.is_null()
        || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let a = unsafe { &*(batch_a as *const crate::memtable::OwnedBatch) };
        let b = unsafe { &*(batch_b as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_desc };
        let output = crate::ops::op_semi_join_delta_delta(a, b, schema);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Inner join delta-delta: returns output batch with composite schema.
#[no_mangle]
pub extern "C" fn gnitz_op_join_dd(
    batch_a: *const libc::c_void,
    batch_b: *const libc::c_void,
    left_schema: *const crate::compact::SchemaDescriptor,
    right_schema: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_a.is_null() || batch_b.is_null()
        || left_schema.is_null() || right_schema.is_null() || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let a = unsafe { &*(batch_a as *const crate::memtable::OwnedBatch) };
        let b = unsafe { &*(batch_b as *const crate::memtable::OwnedBatch) };
        let ls = unsafe { &*left_schema };
        let rs = unsafe { &*right_schema };
        let output = crate::ops::op_join_delta_delta(a, b, ls, rs);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Reduce operators
// ---------------------------------------------------------------------------

/// Reduce operator: incremental GROUP BY + aggregation.
/// Returns (raw_output, optional finalized_output).
#[no_mangle]
pub extern "C" fn gnitz_op_reduce(
    delta_handle: *const libc::c_void,
    trace_in_cursor: *mut libc::c_void,
    trace_out_cursor: *mut libc::c_void,
    input_schema: *const crate::compact::SchemaDescriptor,
    output_schema: *const crate::compact::SchemaDescriptor,
    group_by_cols: *const u32,
    num_group_by_cols: u32,
    agg_descs: *const crate::ops::AggDescriptor,
    num_aggs: u32,
    avi_cursor: *mut libc::c_void,
    avi_for_max: i32,
    avi_agg_col_type_code: u8,
    avi_group_by_cols: *const u32,
    avi_num_group_by_cols: u32,
    avi_input_schema: *const crate::compact::SchemaDescriptor,
    gi_cursor: *mut libc::c_void,
    gi_col_idx: u32,
    gi_col_type_code: u8,
    finalize_prog: *const libc::c_void,
    finalize_out_schema: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
    out_finalized: *mut *mut libc::c_void,
) -> i32 {
    if delta_handle.is_null() || trace_out_cursor.is_null()
        || input_schema.is_null() || output_schema.is_null()
        || group_by_cols.is_null() || agg_descs.is_null()
        || out_result.is_null() || out_finalized.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let delta = unsafe { &*(delta_handle as *const crate::memtable::OwnedBatch) };
        let to_ch = unsafe { &mut *(trace_out_cursor as *mut crate::read_cursor::CursorHandle) };
        let in_schema = unsafe { &*input_schema };
        let out_schema = unsafe { &*output_schema };
        let gcols = unsafe { slice::from_raw_parts(group_by_cols, num_group_by_cols as usize) };
        let aggs = unsafe { slice::from_raw_parts(agg_descs, num_aggs as usize) };

        let mut ti_cursor_opt: Option<&mut crate::read_cursor::ReadCursor> = if trace_in_cursor.is_null() {
            None
        } else {
            let ch = unsafe { &mut *(trace_in_cursor as *mut crate::read_cursor::CursorHandle) };
            Some(&mut ch.cursor)
        };

        let mut avi_cursor_opt: Option<&mut crate::read_cursor::ReadCursor> = if avi_cursor.is_null() {
            None
        } else {
            let ch = unsafe { &mut *(avi_cursor as *mut crate::read_cursor::CursorHandle) };
            Some(&mut ch.cursor)
        };

        let avi_gcols = if avi_group_by_cols.is_null() || avi_num_group_by_cols == 0 {
            &[] as &[u32]
        } else {
            unsafe { slice::from_raw_parts(avi_group_by_cols, avi_num_group_by_cols as usize) }
        };

        let avi_in_schema_opt: Option<&crate::compact::SchemaDescriptor> = if avi_input_schema.is_null() {
            None
        } else {
            Some(unsafe { &*avi_input_schema })
        };

        let mut gi_cursor_opt: Option<&mut crate::read_cursor::ReadCursor> = if gi_cursor.is_null() {
            None
        } else {
            let ch = unsafe { &mut *(gi_cursor as *mut crate::read_cursor::CursorHandle) };
            Some(&mut ch.cursor)
        };

        let fin_prog_opt: Option<&crate::expr::ExprProgram> = if finalize_prog.is_null() {
            None
        } else {
            Some(unsafe { &*(finalize_prog as *const crate::expr::ExprProgram) })
        };

        let fin_schema_opt: Option<&crate::compact::SchemaDescriptor> = if finalize_out_schema.is_null() {
            None
        } else {
            Some(unsafe { &*finalize_out_schema })
        };

        let (raw_out, fin_out) = crate::ops::op_reduce(
            delta,
            ti_cursor_opt.as_deref_mut(),
            &mut to_ch.cursor,
            in_schema,
            out_schema,
            gcols,
            aggs,
            avi_cursor_opt.as_deref_mut(),
            avi_for_max != 0,
            avi_agg_col_type_code,
            avi_gcols,
            avi_in_schema_opt,
            gi_cursor_opt.as_deref_mut(),
            gi_col_idx,
            gi_col_type_code,
            fin_prog_opt,
            fin_schema_opt,
        );

        unsafe {
            *out_result = Box::into_raw(Box::new(raw_out)) as *mut libc::c_void;
            match fin_out {
                Some(batch) => *out_finalized = Box::into_raw(Box::new(batch)) as *mut libc::c_void,
                None => *out_finalized = ptr::null_mut(),
            }
        }
        0
    });
    result.unwrap_or(-99)
}

/// Gather-reduce: merge partial aggregate deltas from workers.
#[no_mangle]
pub extern "C" fn gnitz_op_gather_reduce(
    partial_handle: *const libc::c_void,
    trace_out_cursor: *mut libc::c_void,
    partial_schema: *const crate::compact::SchemaDescriptor,
    agg_descs: *const crate::ops::AggDescriptor,
    num_aggs: u32,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if partial_handle.is_null() || trace_out_cursor.is_null()
        || partial_schema.is_null() || agg_descs.is_null() || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let partial = unsafe { &*(partial_handle as *const crate::memtable::OwnedBatch) };
        let to_ch = unsafe { &mut *(trace_out_cursor as *mut crate::read_cursor::CursorHandle) };
        let schema = unsafe { &*partial_schema };
        let aggs = unsafe { slice::from_raw_parts(agg_descs, num_aggs as usize) };

        let output = crate::ops::op_gather_reduce(partial, &mut to_ch.cursor, schema, aggs);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Scan trace
// ---------------------------------------------------------------------------

/// Scan rows from a cursor into a new OwnedBatch.
#[no_mangle]
pub extern "C" fn gnitz_op_scan_trace(
    cursor_handle: *mut libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    chunk_limit: i32,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if cursor_handle.is_null() || schema_desc.is_null() || out_result.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let ch = unsafe { &mut *(cursor_handle as *mut crate::read_cursor::CursorHandle) };
        let schema = unsafe { &*schema_desc };
        let output = crate::ops::op_scan_trace(&mut ch.cursor, schema, chunk_limit);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Integrate with indexes
// ---------------------------------------------------------------------------

/// Integrate a batch into a table with optional GI/AVI index population.
#[no_mangle]
pub extern "C" fn gnitz_op_integrate_with_indexes(
    batch_handle: *const libc::c_void,
    target_table: *mut libc::c_void,
    input_schema: *const crate::compact::SchemaDescriptor,
    // GI params (all null/0 if no GI)
    gi_table: *mut libc::c_void,
    gi_col_idx: u32,
    gi_col_type_code: u8,
    // AVI params (all null/0 if no AVI)
    avi_table: *mut libc::c_void,
    avi_for_max: i32,
    avi_agg_col_type_code: u8,
    avi_group_by_cols: *const u32,
    avi_num_group_by_cols: u32,
    avi_input_schema: *const crate::compact::SchemaDescriptor,
    avi_agg_col_idx: u32,
) -> i32 {
    if batch_handle.is_null() || input_schema.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch = unsafe { &*(batch_handle as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*input_schema };

        let target = if target_table.is_null() {
            None
        } else {
            Some(unsafe { &mut *(target_table as *mut crate::table::Table) })
        };

        let gi = if gi_table.is_null() {
            None
        } else {
            Some(crate::ops::GiDesc {
                table: gi_table as *mut crate::table::Table,
                col_idx: gi_col_idx,
                col_type_code: gi_col_type_code,
            })
        };

        let avi = if avi_table.is_null() {
            None
        } else {
            let gcols = if avi_group_by_cols.is_null() || avi_num_group_by_cols == 0 {
                Vec::new()
            } else {
                unsafe { slice::from_raw_parts(avi_group_by_cols, avi_num_group_by_cols as usize) }.to_vec()
            };
            let avi_schema = if avi_input_schema.is_null() {
                *schema
            } else {
                unsafe { *avi_input_schema }
            };
            Some(crate::ops::AviDesc {
                table: avi_table as *mut crate::table::Table,
                for_max: avi_for_max != 0,
                agg_col_type_code: avi_agg_col_type_code,
                group_by_cols: gcols,
                input_schema: avi_schema,
                agg_col_idx: avi_agg_col_idx,
            })
        };

        crate::ops::op_integrate_with_indexes(batch, target, schema, gi.as_ref(), avi.as_ref())
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// VM: program lifecycle and execution
// ---------------------------------------------------------------------------

/// Persistent VM state: owns the Program + RegisterFile.
/// Now defined in vm.rs as crate::vm::VmHandle.
type VmHandle = crate::vm::VmHandle;

/// Execute one epoch of the VM program.
///
/// Takes ownership of input_batch (frees it).
/// On success, writes result batch to *out_result (or null if no output).
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_vm_execute_epoch(
    handle: *mut libc::c_void,
    input_batch: *mut libc::c_void,
    input_reg_idx: u16,
    output_reg_idx: u16,
    cursor_handles: *const *mut libc::c_void,
    num_cursors: u32,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if handle.is_null() || input_batch.is_null() || out_result.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let vm = unsafe { &mut *(handle as *mut VmHandle) };
        let batch = unsafe { *Box::from_raw(input_batch as *mut crate::memtable::OwnedBatch) };
        let cursors = if cursor_handles.is_null() || num_cursors == 0 {
            &[] as &[*mut libc::c_void]
        } else {
            unsafe { slice::from_raw_parts(cursor_handles, num_cursors as usize) }
        };

        // Refresh owned-table cursors BEFORE execute_epoch.
        // bind_cursors (inside execute_epoch) preserves already-set cursors.
        if !vm.owned_trace_regs.is_empty() {
            vm.refresh_owned_cursors();
        }

        match crate::vm::execute_epoch(&vm.program, &mut vm.regfile, batch, input_reg_idx, output_reg_idx, cursors) {
            Ok(Some(output)) => {
                unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
                0
            }
            Ok(None) => {
                unsafe { *out_result = ptr::null_mut(); }
                0
            }
            Err(rc) => rc,
        }
    });
    result.unwrap_or(-99)
}

/// Free a VM program handle.
#[no_mangle]
pub extern "C" fn gnitz_vm_program_free(handle: *mut libc::c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle as *mut VmHandle)); }
    }
}

// ---------------------------------------------------------------------------
// ProgramBuilder FFI — construct programs via incremental add_*() calls
// ---------------------------------------------------------------------------

macro_rules! builder_mut {
    ($handle:expr) => {{
        if $handle.is_null() { return; }
        unsafe { &mut *($handle as *mut crate::vm::ProgramBuilder) }
    }};
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_new(num_registers: u16) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        let builder = crate::vm::ProgramBuilder::new(num_registers);
        Box::into_raw(Box::new(builder)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_free(handle: *mut libc::c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle as *mut crate::vm::ProgramBuilder)); }
    }
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_halt(handle: *mut libc::c_void) {
    let builder = builder_mut!(handle);
    builder.add_halt();
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_clear_deltas(handle: *mut libc::c_void) {
    let builder = builder_mut!(handle);
    builder.add_clear_deltas();
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_delay(
    handle: *mut libc::c_void, src: u16, dst: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_delay(src, dst);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_scan_trace(
    handle: *mut libc::c_void, trace_reg: u16, out_reg: u16, chunk_limit: i32,
) {
    let builder = builder_mut!(handle);
    builder.add_scan_trace(trace_reg, out_reg, chunk_limit);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_seek_trace(
    handle: *mut libc::c_void, trace_reg: u16, key_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_seek_trace(trace_reg, key_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_filter(
    handle: *mut libc::c_void, in_reg: u16, out_reg: u16, func: *const libc::c_void,
) {
    let builder = builder_mut!(handle);
    builder.add_filter(in_reg, out_reg, func as *const crate::scalar_func::ScalarFuncKind);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_map(
    handle: *mut libc::c_void,
    in_reg: u16, out_reg: u16,
    func: *const libc::c_void,
    out_schema: *const crate::compact::SchemaDescriptor,
    reindex_col: i32,
) {
    if handle.is_null() || out_schema.is_null() { return; }
    let builder = unsafe { &mut *(handle as *mut crate::vm::ProgramBuilder) };
    let schema = unsafe { *out_schema };
    builder.add_map(
        in_reg, out_reg,
        func as *const crate::scalar_func::ScalarFuncKind,
        schema, reindex_col,
    );
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_negate(
    handle: *mut libc::c_void, in_reg: u16, out_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_negate(in_reg, out_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_union(
    handle: *mut libc::c_void, in_a: u16, in_b: u16, has_b: i32, out_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_union(in_a, in_b, has_b != 0, out_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_distinct(
    handle: *mut libc::c_void,
    in_reg: u16, hist_reg: u16, out_reg: u16,
    hist_table: *mut libc::c_void,
) {
    let builder = builder_mut!(handle);
    builder.add_distinct(in_reg, hist_reg, out_reg, hist_table as *mut crate::table::Table);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_join_dt(
    handle: *mut libc::c_void,
    delta_reg: u16, trace_reg: u16, out_reg: u16,
    right_schema: *const crate::compact::SchemaDescriptor,
) {
    if handle.is_null() || right_schema.is_null() { return; }
    let builder = unsafe { &mut *(handle as *mut crate::vm::ProgramBuilder) };
    builder.add_join_dt(delta_reg, trace_reg, out_reg, unsafe { *right_schema });
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_join_dd(
    handle: *mut libc::c_void,
    a_reg: u16, b_reg: u16, out_reg: u16,
    right_schema: *const crate::compact::SchemaDescriptor,
) {
    if handle.is_null() || right_schema.is_null() { return; }
    let builder = unsafe { &mut *(handle as *mut crate::vm::ProgramBuilder) };
    builder.add_join_dd(a_reg, b_reg, out_reg, unsafe { *right_schema });
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_join_dt_outer(
    handle: *mut libc::c_void,
    delta_reg: u16, trace_reg: u16, out_reg: u16,
    right_schema: *const crate::compact::SchemaDescriptor,
) {
    if handle.is_null() || right_schema.is_null() { return; }
    let builder = unsafe { &mut *(handle as *mut crate::vm::ProgramBuilder) };
    builder.add_join_dt_outer(delta_reg, trace_reg, out_reg, unsafe { *right_schema });
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_anti_join_dt(
    handle: *mut libc::c_void,
    delta_reg: u16, trace_reg: u16, out_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_anti_join_dt(delta_reg, trace_reg, out_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_anti_join_dd(
    handle: *mut libc::c_void,
    a_reg: u16, b_reg: u16, out_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_anti_join_dd(a_reg, b_reg, out_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_semi_join_dt(
    handle: *mut libc::c_void,
    delta_reg: u16, trace_reg: u16, out_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_semi_join_dt(delta_reg, trace_reg, out_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_semi_join_dd(
    handle: *mut libc::c_void,
    a_reg: u16, b_reg: u16, out_reg: u16,
) {
    let builder = builder_mut!(handle);
    builder.add_semi_join_dd(a_reg, b_reg, out_reg);
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_integrate(
    handle: *mut libc::c_void,
    in_reg: u16,
    target_table: *mut libc::c_void,
    // GI params
    gi_table: *mut libc::c_void,
    gi_col_idx: u32,
    gi_col_type_code: u8,
    // AVI params
    avi_table: *mut libc::c_void,
    avi_for_max: i32,
    avi_agg_col_type_code: u8,
    avi_group_cols: *const u32,
    avi_num_group_cols: u32,
    avi_input_schema: *const crate::compact::SchemaDescriptor,
    avi_agg_col_idx: u32,
) {
    let builder = builder_mut!(handle);
    let avi_gcols = if !avi_group_cols.is_null() && avi_num_group_cols > 0 {
        unsafe { slice::from_raw_parts(avi_group_cols, avi_num_group_cols as usize) }
    } else {
        &[]
    };
    builder.add_integrate(
        in_reg,
        target_table as *mut crate::table::Table,
        gi_table as *mut crate::table::Table,
        gi_col_idx,
        gi_col_type_code,
        avi_table as *mut crate::table::Table,
        avi_for_max != 0,
        avi_agg_col_type_code,
        avi_gcols,
        avi_input_schema,
        avi_agg_col_idx,
    );
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_reduce(
    handle: *mut libc::c_void,
    in_reg: u16,
    trace_in_reg: i16,
    trace_out_reg: u16,
    out_reg: u16,
    fin_out_reg: i16,
    agg_descs: *const crate::ops::AggDescriptor,
    num_agg_descs: u32,
    group_cols: *const u32,
    num_group_cols: u32,
    output_schema: *const crate::compact::SchemaDescriptor,
    // AVI params
    avi_table: *mut libc::c_void,
    avi_for_max: i32,
    avi_agg_col_type_code: u8,
    avi_group_cols: *const u32,
    avi_num_group_cols: u32,
    avi_input_schema: *const crate::compact::SchemaDescriptor,
    avi_agg_col_idx: u32,
    // GI params
    gi_table: *mut libc::c_void,
    gi_col_idx: u32,
    gi_col_type_code: u8,
    // Finalize
    finalize_prog: *const libc::c_void,
    finalize_schema: *const crate::compact::SchemaDescriptor,
) {
    if handle.is_null() || output_schema.is_null() { return; }
    let builder = unsafe { &mut *(handle as *mut crate::vm::ProgramBuilder) };
    let aggs = if !agg_descs.is_null() && num_agg_descs > 0 {
        unsafe { slice::from_raw_parts(agg_descs, num_agg_descs as usize) }
    } else {
        &[]
    };
    let gcols = if !group_cols.is_null() && num_group_cols > 0 {
        unsafe { slice::from_raw_parts(group_cols, num_group_cols as usize) }
    } else {
        &[]
    };
    let avi_gcols = if !avi_group_cols.is_null() && avi_num_group_cols > 0 {
        unsafe { slice::from_raw_parts(avi_group_cols, avi_num_group_cols as usize) }
    } else {
        &[]
    };
    builder.add_reduce(
        in_reg, trace_in_reg, trace_out_reg, out_reg, fin_out_reg,
        aggs, gcols,
        unsafe { *output_schema },
        avi_table as *mut crate::table::Table,
        avi_for_max != 0,
        avi_agg_col_type_code,
        avi_gcols,
        avi_input_schema,
        avi_agg_col_idx,
        gi_table as *mut crate::table::Table,
        gi_col_idx,
        gi_col_type_code,
        finalize_prog as *const crate::expr::ExprProgram,
        finalize_schema,
    );
}

#[no_mangle]
pub extern "C" fn gnitz_program_builder_add_gather_reduce(
    handle: *mut libc::c_void,
    in_reg: u16,
    trace_out_reg: u16,
    out_reg: u16,
    agg_descs: *const crate::ops::AggDescriptor,
    num_agg_descs: u32,
) {
    let builder = builder_mut!(handle);
    let aggs = if !agg_descs.is_null() && num_agg_descs > 0 {
        unsafe { slice::from_raw_parts(agg_descs, num_agg_descs as usize) }
    } else {
        &[]
    };
    builder.add_gather_reduce(in_reg, trace_out_reg, out_reg, aggs);
}

/// Build the program from the builder. Consumes the builder.
/// Returns opaque VmHandle or null on error.
#[no_mangle]
pub extern "C" fn gnitz_program_builder_build(
    handle: *mut libc::c_void,
    reg_schemas: *const crate::compact::SchemaDescriptor,
    reg_kinds: *const u8,
    num_registers: u32,
) -> *mut libc::c_void {
    if handle.is_null() || reg_schemas.is_null() || reg_kinds.is_null() {
        return ptr::null_mut();
    }
    let result = panic::catch_unwind(|| {
        let builder = unsafe { *Box::from_raw(handle as *mut crate::vm::ProgramBuilder) };
        let schemas = unsafe { slice::from_raw_parts(reg_schemas, num_registers as usize) };
        let kinds = unsafe { slice::from_raw_parts(reg_kinds, num_registers as usize) };
        let vm_handle = builder.build(schemas, kinds);
        Box::into_raw(vm_handle) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

// ---------------------------------------------------------------------------
// Expression programs and scalar functions
// ---------------------------------------------------------------------------

/// Create an ExprProgram from a flat i64 code array + string constants.
/// Returns opaque handle or null on error.
#[no_mangle]
pub extern "C" fn gnitz_expr_program_create(
    code: *const i64,
    code_len: u32,
    num_regs: u32,
    result_reg: u32,
    const_string_data: *const u8,
    const_string_offsets: *const u32,
    const_string_lengths: *const u32,
    num_const_strings: u32,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if code.is_null() || code_len == 0 {
            return ptr::null_mut();
        }
        let code_slice = unsafe { slice::from_raw_parts(code, code_len as usize) };
        let code_vec = code_slice.to_vec();

        let mut const_strings = Vec::new();
        if num_const_strings > 0 && !const_string_data.is_null()
            && !const_string_offsets.is_null() && !const_string_lengths.is_null()
        {
            let offsets = unsafe { slice::from_raw_parts(const_string_offsets, num_const_strings as usize) };
            let lengths = unsafe { slice::from_raw_parts(const_string_lengths, num_const_strings as usize) };
            let total_len: usize = offsets.last().map_or(0, |&o| o as usize)
                + lengths.last().map_or(0, |&l| l as usize);
            let data = unsafe { slice::from_raw_parts(const_string_data, total_len) };
            for i in 0..num_const_strings as usize {
                let off = offsets[i] as usize;
                let len = lengths[i] as usize;
                const_strings.push(data[off..off + len].to_vec());
            }
        }

        let prog = crate::expr::ExprProgram::new(code_vec, num_regs, result_reg, const_strings);
        Box::into_raw(Box::new(prog)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_expr_program_free(handle: *mut libc::c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle as *mut crate::expr::ExprProgram)); }
    }
}

/// Create a ScalarFuncKind::ExprPredicate from an ExprProgram handle.
/// Takes ownership of the program (caller must not free it separately).
#[no_mangle]
pub extern "C" fn gnitz_scalar_func_create_expr_predicate(
    program_handle: *mut libc::c_void,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if program_handle.is_null() {
            return ptr::null_mut();
        }
        let prog = unsafe { *Box::from_raw(program_handle as *mut crate::expr::ExprProgram) };
        let func = crate::scalar_func::ScalarFuncKind::ExprPredicate(prog);
        Box::into_raw(Box::new(func)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

/// Create a ScalarFuncKind::ExprMap from an ExprProgram handle.
/// Takes ownership of the program.
#[no_mangle]
pub extern "C" fn gnitz_scalar_func_create_expr_map(
    program_handle: *mut libc::c_void,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if program_handle.is_null() {
            return ptr::null_mut();
        }
        let prog = unsafe { *Box::from_raw(program_handle as *mut crate::expr::ExprProgram) };
        let analysis = crate::scalar_func::MapAnalysis::from_program(&prog);
        let func = crate::scalar_func::ScalarFuncKind::ExprMap { program: prog, analysis };
        Box::into_raw(Box::new(func)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

/// Create a ScalarFuncKind::UniversalPredicate.
#[no_mangle]
pub extern "C" fn gnitz_scalar_func_create_universal_predicate(
    col_idx: u32,
    op: u8,
    val_bits: u64,
    is_float: i32,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        let func = crate::scalar_func::ScalarFuncKind::UniversalPredicate {
            col_idx,
            op,
            val_bits,
            is_float: is_float != 0,
        };
        Box::into_raw(Box::new(func)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

/// Create a ScalarFuncKind::UniversalProjection.
#[no_mangle]
pub extern "C" fn gnitz_scalar_func_create_universal_projection(
    src_indices: *const u32,
    src_types: *const u8,
    count: u32,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if src_indices.is_null() || src_types.is_null() || count == 0 {
            return ptr::null_mut();
        }
        let indices = unsafe { slice::from_raw_parts(src_indices, count as usize) }.to_vec();
        let types = unsafe { slice::from_raw_parts(src_types, count as usize) }.to_vec();
        let func = crate::scalar_func::ScalarFuncKind::UniversalProjection {
            src_indices: indices,
            src_types: types,
        };
        Box::into_raw(Box::new(func)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_scalar_func_free(handle: *mut libc::c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle as *mut crate::scalar_func::ScalarFuncKind)); }
    }
}

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: returns output batch with rows matching predicate.
#[no_mangle]
pub extern "C" fn gnitz_op_filter(
    batch_handle: *const libc::c_void,
    func_handle: *const libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_handle.is_null() || schema_desc.is_null() || out_result.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch = unsafe { &*(batch_handle as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_desc };
        let func = if func_handle.is_null() {
            &crate::scalar_func::ScalarFuncKind::Null
        } else {
            unsafe { &*(func_handle as *const crate::scalar_func::ScalarFuncKind) }
        };
        let output = crate::ops::op_filter(batch, func, schema);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Map: returns transformed output batch.
#[no_mangle]
pub extern "C" fn gnitz_op_map(
    batch_handle: *const libc::c_void,
    func_handle: *const libc::c_void,
    in_schema: *const crate::compact::SchemaDescriptor,
    out_schema: *const crate::compact::SchemaDescriptor,
    reindex_col: i32,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_handle.is_null() || in_schema.is_null() || out_schema.is_null()
        || out_result.is_null()
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch = unsafe { &*(batch_handle as *const crate::memtable::OwnedBatch) };
        let is = unsafe { &*in_schema };
        let os = unsafe { &*out_schema };
        let func = if func_handle.is_null() {
            &crate::scalar_func::ScalarFuncKind::Null
        } else {
            unsafe { &*(func_handle as *const crate::scalar_func::ScalarFuncKind) }
        };
        let output = crate::ops::op_map(batch, func, is, os, reindex_col);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Negate: returns batch with all weights negated.
#[no_mangle]
pub extern "C" fn gnitz_op_negate(
    batch_handle: *const libc::c_void,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_handle.is_null() || out_result.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch = unsafe { &*(batch_handle as *const crate::memtable::OwnedBatch) };
        let output = crate::ops::op_negate(batch);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

/// Union: returns merged output batch.
#[no_mangle]
pub extern "C" fn gnitz_op_union(
    batch_a_handle: *const libc::c_void,
    batch_b_handle: *const libc::c_void,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_result: *mut *mut libc::c_void,
) -> i32 {
    if batch_a_handle.is_null() || schema_desc.is_null() || out_result.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch_a = unsafe { &*(batch_a_handle as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_desc };
        let batch_b = if batch_b_handle.is_null() {
            None
        } else {
            Some(unsafe { &*(batch_b_handle as *const crate::memtable::OwnedBatch) })
        };
        let output = crate::ops::op_union(batch_a, batch_b, schema);
        unsafe { *out_result = Box::into_raw(Box::new(output)) as *mut libc::c_void; }
        0
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Read cursor (opaque N-way merge cursor)
// ---------------------------------------------------------------------------

/// Create a read cursor from batch regions + shard handles.
/// Batch regions follow the standard layout (pk_lo, pk_hi, weight, null, cols..., blob).
/// Shard handles are opaque void* from gnitz_shard_open (NOT owned by cursor).
/// Returns opaque handle or null on error.
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_create(
    batch_region_ptrs: *const *const u8,
    batch_region_sizes: *const u32,
    batch_row_counts: *const u32,
    num_batches: u32,
    regions_per_batch: u32,
    shard_handles: *const *const libc::c_void,
    num_shards: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() {
            return ptr::null_mut();
        }
        let schema = unsafe { *schema_desc };
        let nb = num_batches as usize;
        let ns = num_shards as usize;
        let rpb = regions_per_batch as usize;
        let num_payload_cols = schema.num_columns as usize - 1;

        // Parse batch sources
        let mut batches: Vec<crate::merge::MemBatch> = Vec::new();
        if nb > 0 && !batch_region_ptrs.is_null() && !batch_region_sizes.is_null()
            && !batch_row_counts.is_null()
        {
            let in_ptrs = unsafe { slice::from_raw_parts(batch_region_ptrs, nb * rpb) };
            let in_sizes = unsafe { slice::from_raw_parts(batch_region_sizes, nb * rpb) };
            let counts = unsafe { slice::from_raw_parts(batch_row_counts, nb) };
            batches = unsafe {
                crate::merge::parse_batches_from_regions(
                    in_ptrs, in_sizes, counts, nb, rpb, num_payload_cols,
                )
            };
        }

        // Parse shard sources
        let mut shard_ptrs: Vec<*const crate::shard_reader::MappedShard> = Vec::new();
        if ns > 0 && !shard_handles.is_null() {
            let handles = unsafe { slice::from_raw_parts(shard_handles, ns) };
            for &h in handles {
                if !h.is_null() {
                    shard_ptrs.push(h as *const crate::shard_reader::MappedShard);
                }
            }
        }

        let cursor = unsafe {
            crate::read_cursor::create_read_cursor(&batches, &shard_ptrs, schema)
        };
        let handle = crate::read_cursor::CursorHandle::from_cursor(cursor);
        Box::into_raw(Box::new(handle)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

unsafe fn write_cursor_state(
    cursor: &crate::read_cursor::ReadCursor,
    out_valid: *mut i32,
    out_key_lo: *mut u64,
    out_key_hi: *mut u64,
    out_weight: *mut i64,
    out_null_word: *mut u64,
) {
    if !out_valid.is_null() { *out_valid = cursor.valid as i32; }
    if !out_key_lo.is_null() { *out_key_lo = cursor.current_key_lo; }
    if !out_key_hi.is_null() { *out_key_hi = cursor.current_key_hi; }
    if !out_weight.is_null() { *out_weight = cursor.current_weight; }
    if !out_null_word.is_null() { *out_null_word = cursor.current_null_word; }
}

/// Seek cursor to first row >= (key_lo, key_hi).
/// Populates out_ params with current row state.
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_seek(
    handle: *mut libc::c_void,
    key_lo: u64,
    key_hi: u64,
    out_valid: *mut i32,
    out_key_lo: *mut u64,
    out_key_hi: *mut u64,
    out_weight: *mut i64,
    out_null_word: *mut u64,
) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let ch = unsafe { &mut *(handle as *mut crate::read_cursor::CursorHandle) };
        ch.cursor.seek(key_lo, key_hi);
        unsafe {
            write_cursor_state(&ch.cursor, out_valid, out_key_lo, out_key_hi, out_weight, out_null_word);
        }
    });
}

/// Advance to next non-ghost row. Populates out_ params.
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_next(
    handle: *mut libc::c_void,
    out_valid: *mut i32,
    out_key_lo: *mut u64,
    out_key_hi: *mut u64,
    out_weight: *mut i64,
    out_null_word: *mut u64,
) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let ch = unsafe { &mut *(handle as *mut crate::read_cursor::CursorHandle) };
        ch.cursor.advance();
        unsafe {
            write_cursor_state(&ch.cursor, out_valid, out_key_lo, out_key_hi, out_weight, out_null_word);
        }
    });
}

/// Get column data pointer for current row (schema-indexed).
/// Returns NULL if cursor exhausted or col_idx out of range.
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_col_ptr(
    handle: *const libc::c_void,
    col_idx: i32,
    col_size: i32,
) -> *const u8 {
    if handle.is_null() {
        return ptr::null();
    }
    let result = panic::catch_unwind(|| {
        let ch = unsafe { &*(handle as *const crate::read_cursor::CursorHandle) };
        ch.cursor.col_ptr(col_idx as usize, col_size as usize)
    });
    result.unwrap_or(ptr::null())
}

/// Get blob arena base pointer for current row's source.
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_blob_ptr(
    handle: *const libc::c_void,
) -> *const u8 {
    if handle.is_null() {
        return ptr::null();
    }
    let result = panic::catch_unwind(|| {
        let ch = unsafe { &*(handle as *const crate::read_cursor::CursorHandle) };
        ch.cursor.blob_ptr()
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn gnitz_read_cursor_blob_len(
    handle: *const libc::c_void,
) -> u64 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let ch = unsafe { &*(handle as *const crate::read_cursor::CursorHandle) };
        ch.cursor.blob_len() as u64
    });
    result.unwrap_or(0)
}

/// Close cursor and free Rust-side state.
/// Does NOT close/free shard handles (RPython owns those).
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_close(handle: *mut libc::c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        unsafe {
            let _ = Box::from_raw(handle as *mut crate::read_cursor::CursorHandle);
        }
    });
}

// ---------------------------------------------------------------------------
// MemTable (opaque handle)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_memtable_create(
    schema_desc: *const crate::compact::SchemaDescriptor,
    max_bytes: u64,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() {
            return ptr::null_mut();
        }
        let schema = unsafe { *schema_desc };
        let mt = crate::memtable::MemTable::new(schema, max_bytes as usize);
        Box::into_raw(Box::new(mt)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_close(handle: *mut libc::c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        unsafe {
            let _ = Box::from_raw(handle as *mut crate::memtable::MemTable);
        }
    });
}

/// Upsert a pre-sorted batch into the MemTable.  Copies data from the
/// provided region pointers.  Returns 0 on success, ERR_CAPACITY (-2) if
/// the MemTable is over capacity.
#[no_mangle]
pub extern "C" fn gnitz_memtable_upsert_batch(
    handle: *mut libc::c_void,
    in_ptrs: *const *const u8,
    in_sizes: *const u32,
    row_count: u32,
    regions_per_batch: u32,
) -> i32 {
    if handle.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::MemTable) };
        let rpb = regions_per_batch as usize;
        let num_payload_cols = mt.schema().num_columns as usize - 1;

        let ptrs = unsafe { slice::from_raw_parts(in_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(in_sizes, rpb) };

        let batch = unsafe {
            crate::memtable::OwnedBatch::from_regions(
                ptrs, sizes, row_count as usize, num_payload_cols,
            )
        };

        match mt.upsert_sorted_batch(batch) {
            Ok(()) => 0,
            Err(code) => code,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_bloom_add(
    handle: *mut libc::c_void,
    key_lo: u64,
    key_hi: u64,
) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::MemTable) };
        mt.bloom_add(key_lo, key_hi);
    });
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_may_contain_pk(
    handle: *const libc::c_void,
    key_lo: u64,
    key_hi: u64,
) -> i32 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.may_contain_pk(key_lo, key_hi) as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_should_flush(handle: *const libc::c_void) -> i32 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.should_flush() as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_is_empty(handle: *const libc::c_void) -> i32 {
    if handle.is_null() {
        return 1;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.is_empty() as i32
    });
    result.unwrap_or(1)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_total_row_count(handle: *const libc::c_void) -> i32 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.total_row_count() as i32
    });
    result.unwrap_or(0)
}

/// Get a consolidated snapshot.  Returns an opaque MemTableSnapshot handle.
/// Caller must free with gnitz_memtable_snapshot_free.
#[no_mangle]
pub extern "C" fn gnitz_memtable_get_snapshot(
    handle: *mut libc::c_void,
) -> *mut libc::c_void {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::MemTable) };
        let arc = mt.get_snapshot();
        let snap = crate::memtable::MemTableSnapshot { inner: arc };
        Box::into_raw(Box::new(snap)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_snapshot_count(
    snap_handle: *const libc::c_void,
) -> u32 {
    if snap_handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let snap = unsafe { &*(snap_handle as *const crate::memtable::MemTableSnapshot) };
        snap.inner.count as u32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_snapshot_free(snap_handle: *mut libc::c_void) {
    if snap_handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        unsafe {
            let _ = Box::from_raw(snap_handle as *mut crate::memtable::MemTableSnapshot);
        }
    });
}

/// Look up a PK in the MemTable.  Returns net weight (i64).
/// Sets `*out_found = 1` if any row was found.
/// After a successful find, use gnitz_memtable_found_* functions to access
/// the row data.
#[no_mangle]
pub extern "C" fn gnitz_memtable_lookup_pk(
    handle: *mut libc::c_void,
    key_lo: u64,
    key_hi: u64,
    out_found: *mut i32,
) -> i64 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::MemTable) };
        let (weight, found) = mt.lookup_pk(key_lo, key_hi);
        unsafe {
            if !out_found.is_null() { *out_found = found as i32; }
        }
        weight
    });
    result.unwrap_or(0)
}

/// Get null word for the last-found row (after gnitz_memtable_lookup_pk).
#[no_mangle]
pub extern "C" fn gnitz_memtable_found_null_word(
    handle: *const libc::c_void,
) -> u64 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.found_null_word()
    });
    result.unwrap_or(0)
}

/// Get column data pointer for the last-found row.
#[no_mangle]
pub extern "C" fn gnitz_memtable_found_col_ptr(
    handle: *const libc::c_void,
    payload_col: i32,
    col_size: i32,
) -> *const u8 {
    if handle.is_null() {
        return ptr::null();
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.found_col_ptr(payload_col as usize, col_size as usize)
    });
    result.unwrap_or(ptr::null())
}

/// Get blob arena pointer for the last-found row's source run.
#[no_mangle]
pub extern "C" fn gnitz_memtable_found_blob_ptr(
    handle: *const libc::c_void,
) -> *const u8 {
    if handle.is_null() {
        return ptr::null();
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        mt.found_blob_ptr()
    });
    result.unwrap_or(ptr::null())
}

/// Find net weight for rows matching PK + full payload.
/// The reference row is passed as a 1-row batch in region format.
#[no_mangle]
pub extern "C" fn gnitz_memtable_find_weight_for_row(
    handle: *const libc::c_void,
    key_lo: u64,
    key_hi: u64,
    ref_ptrs: *const *const u8,
    ref_sizes: *const u32,
    ref_count: u32,
    regions_per_batch: u32,
) -> i64 {
    if handle.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &*(handle as *const crate::memtable::MemTable) };
        let rpb = regions_per_batch as usize;
        let num_payload_cols = mt.schema().num_columns as usize - 1;

        let ptrs = unsafe { slice::from_raw_parts(ref_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(ref_sizes, rpb) };

        let ref_batch = unsafe {
            crate::merge::parse_single_batch_from_regions(
                ptrs, sizes, ref_count as usize, num_payload_cols,
            )
        };

        mt.find_weight_for_row(key_lo, key_hi, &ref_batch, 0)
    });
    result.unwrap_or(0)
}

/// Consolidate all runs and write to a shard file.
/// Returns 0 on success, -1 if empty (no file written), or negative error code.
#[no_mangle]
pub extern "C" fn gnitz_memtable_flush(
    handle: *mut libc::c_void,
    dirfd: libc::c_int,
    basename: *const libc::c_char,
    table_id: u32,
    durable: i32,
) -> i32 {
    if handle.is_null() || basename.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::MemTable) };
        let name = unsafe { CStr::from_ptr(basename) };
        mt.flush(dirfd, name, table_id, durable != 0)
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_memtable_reset(handle: *mut libc::c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let mt = unsafe { &mut *(handle as *mut crate::memtable::MemTable) };
        mt.reset();
    });
}

/// Create a read cursor from Rust-owned snapshot handles + shard handles.
/// Snapshot handles are opaque MemTableSnapshot pointers from
/// gnitz_memtable_get_snapshot.
/// The cursor clones their Arc references internally.
/// Caller must still free snapshots after closing the cursor.
#[no_mangle]
pub extern "C" fn gnitz_read_cursor_create_from_snapshots(
    snap_handles: *const *mut libc::c_void,
    num_snapshots: u32,
    shard_handles: *const *const libc::c_void,
    num_shards: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() {
            return ptr::null_mut();
        }
        let schema = unsafe { *schema_desc };
        let ns = num_snapshots as usize;
        let nsh = num_shards as usize;

        // Extract Arc<OwnedBatch> from snapshot handles
        let mut snapshot_arcs: Vec<std::sync::Arc<crate::memtable::OwnedBatch>> = Vec::new();
        if ns > 0 && !snap_handles.is_null() {
            let handles = unsafe { slice::from_raw_parts(snap_handles, ns) };
            for &h in handles {
                if !h.is_null() {
                    let snap = unsafe {
                        &*(h as *const crate::memtable::MemTableSnapshot)
                    };
                    snapshot_arcs.push(std::sync::Arc::clone(&snap.inner));
                }
            }
        }

        // Parse shard handles
        let mut shard_ptrs: Vec<*const crate::shard_reader::MappedShard> = Vec::new();
        if nsh > 0 && !shard_handles.is_null() {
            let handles = unsafe { slice::from_raw_parts(shard_handles, nsh) };
            for &h in handles {
                if !h.is_null() {
                    shard_ptrs.push(h as *const crate::shard_reader::MappedShard);
                }
            }
        }

        let cursor_handle = unsafe {
            crate::read_cursor::create_cursor_from_snapshots(
                &snapshot_arcs, &shard_ptrs, schema,
            )
        };

        Box::into_raw(Box::new(cursor_handle)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

// ---------------------------------------------------------------------------
// Batch (OwnedBatch as first-class FFI handle)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_batch_create(
    schema_desc: *const crate::compact::SchemaDescriptor,
    initial_capacity: u32,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() { return ptr::null_mut(); }
        let schema = unsafe { *schema_desc };
        let b = crate::memtable::OwnedBatch::with_schema(schema, initial_capacity as usize);
        Box::into_raw(Box::new(b)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_free(handle: *mut libc::c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        unsafe { let _ = Box::from_raw(handle as *mut crate::memtable::OwnedBatch); }
    });
}

#[no_mangle]
pub extern "C" fn gnitz_batch_length(handle: *const libc::c_void) -> u32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.count as u32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_is_sorted(handle: *const libc::c_void) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.sorted as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_is_consolidated(handle: *const libc::c_void) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.consolidated as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_set_sorted(handle: *mut libc::c_void, val: i32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        b.sorted = val != 0;
    });
}

#[no_mangle]
pub extern "C" fn gnitz_batch_set_consolidated(handle: *mut libc::c_void, val: i32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        b.consolidated = val != 0;
        if b.consolidated {
            b.sorted = true;
        }
    });
}

#[no_mangle]
pub extern "C" fn gnitz_batch_get_pk_lo(handle: *const libc::c_void, row: u32) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.get_pk_lo(row as usize)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_get_pk_hi(handle: *const libc::c_void, row: u32) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.get_pk_hi(row as usize)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_get_weight(handle: *const libc::c_void, row: u32) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.get_weight(row as usize)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_get_null_word(handle: *const libc::c_void, row: u32) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.get_null_word(row as usize)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_col_ptr(
    handle: *const libc::c_void, row: u32, payload_col: u32, col_size: u32,
) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.get_col_ptr(row as usize, payload_col as usize, col_size as usize).as_ptr()
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_blob_ptr(handle: *const libc::c_void) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.blob.as_ptr()
    });
    result.unwrap_or(ptr::null())
}

/// Append a single row from flat column data.
/// col_ptrs[i] → payload column i data, col_sizes[i] → byte size.
/// blob_src/blob_len → source blob arena for string relocation.
#[no_mangle]
pub extern "C" fn gnitz_batch_append_row(
    handle: *mut libc::c_void,
    pk_lo: u64, pk_hi: u64, weight: i64, null_word: u64,
    col_ptrs: *const *const u8,
    col_sizes: *const u32,
    num_cols: u32,
    blob_src: *const u8,
    blob_len: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let ptrs = unsafe { slice::from_raw_parts(col_ptrs, num_cols as usize) };
        let sizes = unsafe { slice::from_raw_parts(col_sizes, num_cols as usize) };
        let blob = if blob_src.is_null() || blob_len == 0 {
            &[] as &[u8]
        } else {
            unsafe { slice::from_raw_parts(blob_src, blob_len as usize) }
        };
        unsafe { b.append_row(pk_lo, pk_hi, weight, null_word, ptrs, sizes, blob); }
        0
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_append_row_simple(
    handle: *mut libc::c_void,
    pk_lo: u64, pk_hi: u64, weight: i64, null_word: u64,
    lo_values: *const i64,
    hi_values: *const u64,
    str_ptrs: *const *const i8,
    str_lens: *const u32,
    n_payload: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let n = n_payload as usize;
        let lo = unsafe { slice::from_raw_parts(lo_values, n) };
        let hi = unsafe { slice::from_raw_parts(hi_values, n) };
        // Cast *const *const i8 → *const *const u8 (same ABI, different signedness)
        let sp = unsafe { slice::from_raw_parts(str_ptrs as *const *const u8, n) };
        let sl = unsafe { slice::from_raw_parts(str_lens, n) };
        unsafe { b.append_row_simple(pk_lo, pk_hi, weight, null_word, lo, hi, sp, sl); }
        0
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_append_batch(
    handle: *mut libc::c_void, src: *const libc::c_void,
    start: u32, end: u32,
) -> i32 {
    if handle.is_null() || src.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dst = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let src = unsafe { &*(src as *const crate::memtable::OwnedBatch) };
        dst.append_batch(src, start as usize, end as usize);
        0
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_append_batch_negated(
    handle: *mut libc::c_void, src: *const libc::c_void,
    start: u32, end: u32,
) -> i32 {
    if handle.is_null() || src.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dst = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let src = unsafe { &*(src as *const crate::memtable::OwnedBatch) };
        dst.append_batch_negated(src, start as usize, end as usize);
        0
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_to_sorted(
    handle: *const libc::c_void, schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if handle.is_null() || schema_desc.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let src = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        if src.sorted || src.count <= 1 {
            return handle as *mut libc::c_void; // return same handle
        }
        let schema = unsafe { *schema_desc };
        let mb = src.as_mem_batch();
        let sorted = crate::memtable::write_to_owned_batch(&schema, src.count, src.blob.len().max(1), |w| {
            crate::merge::sort_only(&mb, &schema, w);
        });
        let mut sorted = sorted;
        sorted.sorted = true;
        sorted.schema = Some(schema);
        Box::into_raw(Box::new(sorted)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_to_consolidated(
    handle: *const libc::c_void, schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if handle.is_null() || schema_desc.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let src = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        if src.consolidated || src.count == 0 {
            return handle as *mut libc::c_void;
        }
        let schema = unsafe { *schema_desc };
        let mb = src.as_mem_batch();
        let consolidated = crate::memtable::write_to_owned_batch(&schema, src.count, src.blob.len().max(1), |w| {
            crate::merge::sort_and_consolidate(&mb, &schema, w);
        });
        let mut consolidated = consolidated;
        consolidated.sorted = true;
        consolidated.consolidated = true;
        consolidated.schema = Some(schema);
        // If output count == input count and was already sorted, return original
        if consolidated.count == src.count && src.sorted {
            return handle as *mut libc::c_void;
        }
        Box::into_raw(Box::new(consolidated)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_scatter_copy(
    src: *const libc::c_void,
    indices: *const u32, num_indices: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if src.is_null() || indices.is_null() || schema_desc.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let src_batch = unsafe { &*(src as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { *schema_desc };
        let idx = unsafe { slice::from_raw_parts(indices, num_indices as usize) };
        let result = crate::memtable::OwnedBatch::from_indexed_rows(
            &src_batch.as_mem_batch(), idx, &schema,
        );
        Box::into_raw(Box::new(result)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_scatter_copy_weighted(
    src: *const libc::c_void,
    indices: *const u32,
    weights: *const i64,
    num_indices: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if src.is_null() || indices.is_null() || weights.is_null() || schema_desc.is_null() {
        return ptr::null_mut();
    }
    let result = panic::catch_unwind(|| {
        let src_batch = unsafe { &*(src as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { *schema_desc };
        let ni = num_indices as usize;
        let idx = unsafe { slice::from_raw_parts(indices, ni) };
        let w = unsafe { slice::from_raw_parts(weights, ni) };
        let mb = src_batch.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let result = crate::memtable::write_to_owned_batch(&schema, ni, blob_cap, |writer| {
            crate::merge::scatter_copy(&mb, idx, w, writer);
        });
        Box::into_raw(Box::new(result)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

/// Append a single row from another batch, with a given weight.
#[no_mangle]
pub extern "C" fn gnitz_batch_append_row_from_batch(
    handle: *mut libc::c_void,
    src: *const libc::c_void,
    row: u32,
    weight: i64,
) -> i32 {
    if handle.is_null() || src.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dst = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let src_batch = unsafe { &*(src as *const crate::memtable::OwnedBatch) };
        let r = row as usize;
        if r >= src_batch.count { return -1; }
        let null_word = src_batch.get_null_word(r);
        dst.pk_lo.extend_from_slice(&src_batch.get_pk_lo(r).to_le_bytes());
        dst.pk_hi.extend_from_slice(&src_batch.get_pk_hi(r).to_le_bytes());
        dst.weight.extend_from_slice(&weight.to_le_bytes());
        dst.null_bmp.extend_from_slice(&null_word.to_le_bytes());
        let schema = dst.schema.unwrap_or_else(|| src_batch.schema.unwrap());
        let pk_index = schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col = &src_batch.col_data[pi];
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            let off = r * cs;
            if is_null {
                let new_len = dst.col_data[pi].len() + cs;
                dst.col_data[pi].resize(new_len, 0);
            } else if col_desc.type_code == crate::compact::type_code::STRING {
                crate::ops::write_string_from_raw(
                    &mut dst.col_data[pi], &mut dst.blob,
                    &col[off..off + cs],
                    if src_batch.blob.is_empty() { std::ptr::null() } else { src_batch.blob.as_ptr() },
                );
            } else {
                dst.col_data[pi].extend_from_slice(&col[off..off + cs]);
            }
            pi += 1;
        }
        dst.count += 1;
        dst.sorted = false;
        dst.consolidated = false;
        0
    });
    result.unwrap_or(-99)
}

/// Copy the current row of a ReadCursor into an OwnedBatch with a given weight.
/// Returns 0 on success, -1 if the cursor is exhausted or handles are null.
#[no_mangle]
pub extern "C" fn gnitz_batch_append_row_from_cursor(
    handle: *mut libc::c_void,
    cursor_handle: *const libc::c_void,
    weight: i64,
) -> i32 {
    if handle.is_null() || cursor_handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dst = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let ch = unsafe { &*(cursor_handle as *const crate::read_cursor::CursorHandle) };
        if !ch.cursor.valid { return -1; }
        let pk_lo = ch.cursor.current_key_lo;
        let pk_hi = ch.cursor.current_key_hi;
        let null_word = ch.cursor.current_null_word;
        dst.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        dst.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        dst.weight.extend_from_slice(&weight.to_le_bytes());
        dst.null_bmp.extend_from_slice(&null_word.to_le_bytes());
        let schema = dst.schema.unwrap();
        let pk_index = schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                let new_len = dst.col_data[pi].len() + cs;
                dst.col_data[pi].resize(new_len, 0);
            } else if col_desc.type_code == crate::compact::type_code::STRING {
                // col_ptr uses logical index ci
                let src = ch.cursor.col_ptr(ci, cs);
                if src.is_null() { return -1; }
                let src_slice = unsafe { slice::from_raw_parts(src, cs) };
                crate::ops::write_string_from_raw(
                    &mut dst.col_data[pi], &mut dst.blob,
                    src_slice, ch.cursor.blob_ptr(),
                );
            } else {
                let src = ch.cursor.col_ptr(ci, cs);
                if src.is_null() { return -1; }
                let src_slice = unsafe { slice::from_raw_parts(src, cs) };
                dst.col_data[pi].extend_from_slice(src_slice);
            }
            pi += 1;
        }
        dst.count += 1;
        dst.sorted = false;
        dst.consolidated = false;
        0
    });
    result.unwrap_or(-99)
}

/// Copy the found row of a Table into an OwnedBatch.
/// The caller provides the PK since it was the argument to retract_pk.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_batch_append_row_from_table_found(
    handle: *mut libc::c_void,
    table_handle: *const libc::c_void,
    pk_lo: u64, pk_hi: u64, weight: i64,
) -> i32 {
    if handle.is_null() || table_handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dst = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let table = unsafe { &*(table_handle as *const crate::table::Table) };
        let null_word = table.found_null_word();
        dst.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        dst.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        dst.weight.extend_from_slice(&weight.to_le_bytes());
        dst.null_bmp.extend_from_slice(&null_word.to_le_bytes());
        let schema = dst.schema.unwrap();
        let pk_index = schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                let new_len = dst.col_data[pi].len() + cs;
                dst.col_data[pi].resize(new_len, 0);
            } else if col_desc.type_code == crate::compact::type_code::STRING {
                // found_col_ptr takes payload index pi
                let src = table.found_col_ptr(pi, cs);
                if src.is_null() { return -1; }
                let src_slice = unsafe { slice::from_raw_parts(src, cs) };
                crate::ops::write_string_from_raw(
                    &mut dst.col_data[pi], &mut dst.blob,
                    src_slice, table.found_blob_ptr(),
                );
            } else {
                let src = table.found_col_ptr(pi, cs);
                if src.is_null() { return -1; }
                let src_slice = unsafe { slice::from_raw_parts(src, cs) };
                dst.col_data[pi].extend_from_slice(src_slice);
            }
            pi += 1;
        }
        dst.count += 1;
        dst.sorted = false;
        dst.consolidated = false;
        0
    });
    result.unwrap_or(-99)
}

/// Copy the found row of a PartitionedTable into an OwnedBatch.
/// Same as table_found but for PartitionedTable handles.
#[no_mangle]
pub extern "C" fn gnitz_batch_append_row_from_ptable_found(
    handle: *mut libc::c_void,
    ptable_handle: *const libc::c_void,
    pk_lo: u64, pk_hi: u64, weight: i64,
) -> i32 {
    if handle.is_null() || ptable_handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dst = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let pt = unsafe { &*(ptable_handle as *const crate::partitioned_table::PartitionedTable) };
        let null_word = pt.found_null_word();
        dst.pk_lo.extend_from_slice(&pk_lo.to_le_bytes());
        dst.pk_hi.extend_from_slice(&pk_hi.to_le_bytes());
        dst.weight.extend_from_slice(&weight.to_le_bytes());
        dst.null_bmp.extend_from_slice(&null_word.to_le_bytes());
        let schema = dst.schema.unwrap();
        let pk_index = schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                let new_len = dst.col_data[pi].len() + cs;
                dst.col_data[pi].resize(new_len, 0);
            } else if col_desc.type_code == crate::compact::type_code::STRING {
                let src = pt.found_col_ptr(pi, cs);
                if src.is_null() { return -1; }
                let src_slice = unsafe { slice::from_raw_parts(src, cs) };
                crate::ops::write_string_from_raw(
                    &mut dst.col_data[pi], &mut dst.blob,
                    src_slice, pt.found_blob_ptr(),
                );
            } else {
                let src = pt.found_col_ptr(pi, cs);
                if src.is_null() { return -1; }
                let src_slice = unsafe { slice::from_raw_parts(src, cs) };
                dst.col_data[pi].extend_from_slice(src_slice);
            }
            pi += 1;
        }
        dst.count += 1;
        dst.sorted = false;
        dst.consolidated = false;
        0
    });
    result.unwrap_or(-99)
}

/// Promote a source column value to an index key (lo, hi).
///
/// Matches the Python `promote_to_index_key` semantics:
/// - I8/I16/I32: sign-extend to i64, reinterpret as u64
/// - Everything else (U8/U16/U32/U64/I64/F32/F64): zero-extend bytes to u64
/// - U128: read as (lo, hi) pair
fn promote_to_index_key(
    col_data: &[u8],
    offset: usize,
    col_size: usize,
    type_code: u8,
) -> (u64, u64) {
    use crate::compact::type_code as tc;
    match type_code {
        tc::U128 => {
            let lo = u64::from_le_bytes(col_data[offset..offset + 8].try_into().unwrap());
            let hi = u64::from_le_bytes(col_data[offset + 8..offset + 16].try_into().unwrap());
            (lo, hi)
        }
        tc::I8 | tc::I16 | tc::I32 => {
            (crate::compact::read_signed(&col_data[offset..], col_size) as u64, 0)
        }
        _ => {
            let mut bytes = [0u8; 8];
            let copy_len = col_size.min(8);
            bytes[..copy_len].copy_from_slice(&col_data[offset..offset + copy_len]);
            (u64::from_le_bytes(bytes), 0)
        }
    }
}

/// Batch-level columnar index projection.
///
/// Takes an entire source batch and produces an index batch in a single call,
/// replacing the Python row loop in `ingest_projection`.
///
/// For each source row with non-zero weight and non-null source column:
/// - Promotes the source column value to an index key
/// - Maps the source PK → index payload
///
/// Returns a new OwnedBatch (caller owns), or null on error.
#[no_mangle]
pub extern "C" fn gnitz_batch_project_index(
    src_handle: *const libc::c_void,
    source_col_idx: u32,
    index_schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if src_handle.is_null() || index_schema_desc.is_null() {
        return ptr::null_mut();
    }
    let result = panic::catch_unwind(|| {
        let src = unsafe { &*(src_handle as *const crate::memtable::OwnedBatch) };
        let idx_schema = unsafe { *index_schema_desc };
        let src_schema = match src.schema {
            Some(s) => s,
            None => return ptr::null_mut(),
        };

        let src_pk_index = src_schema.pk_index as usize;
        let source_col = source_col_idx as usize;
        let is_pk_col = source_col == src_pk_index;

        // Payload index in col_data (columns minus PK)
        let src_payload_idx = if is_pk_col {
            usize::MAX
        } else if source_col < src_pk_index {
            source_col
        } else {
            source_col - 1
        };

        let src_col_type = src_schema.columns[source_col].type_code;
        let src_col_size = src_schema.columns[source_col].size as usize;

        // Output payload column size
        let out_payload_col_schema_idx = if idx_schema.pk_index == 0 { 1usize } else { 0usize };
        let out_payload_size = idx_schema.columns[out_payload_col_schema_idx].size as usize;

        let mut out = crate::memtable::OwnedBatch::with_schema(idx_schema, src.count.max(1));

        for row in 0..src.count {
            let weight = src.get_weight(row);
            if weight == 0 { continue; }

            // Null check (PK is never null)
            if !is_pk_col {
                let null_word = src.get_null_word(row);
                if null_word & (1u64 << src_payload_idx) != 0 { continue; }
            }

            // Promote source column to index key
            let (key_lo, key_hi) = if is_pk_col {
                (src.get_pk_lo(row), src.get_pk_hi(row))
            } else {
                let col = &src.col_data[src_payload_idx];
                let offset = row * src_col_size;
                promote_to_index_key(col, offset, src_col_size, src_col_type)
            };

            // Source PK → index payload
            let src_pk_lo = src.get_pk_lo(row);
            let src_pk_hi = src.get_pk_hi(row);

            out.pk_lo.extend_from_slice(&key_lo.to_le_bytes());
            out.pk_hi.extend_from_slice(&key_hi.to_le_bytes());
            out.weight.extend_from_slice(&weight.to_le_bytes());
            out.null_bmp.extend_from_slice(&0u64.to_le_bytes());

            if out_payload_size == 16 {
                out.col_data[0].extend_from_slice(&src_pk_lo.to_le_bytes());
                out.col_data[0].extend_from_slice(&src_pk_hi.to_le_bytes());
            } else {
                out.col_data[0].extend_from_slice(&src_pk_lo.to_le_bytes());
            }

            out.count += 1;
        }

        if out.count > 0 {
            out.sorted = false;
            out.consolidated = false;
        }

        Box::into_raw(Box::new(out)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_clone(handle: *const libc::c_void) -> *mut libc::c_void {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let src = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        let cloned = crate::memtable::OwnedBatch {
            pk_lo: src.pk_lo.clone(),
            pk_hi: src.pk_hi.clone(),
            weight: src.weight.clone(),
            null_bmp: src.null_bmp.clone(),
            col_data: src.col_data.clone(),
            blob: src.blob.clone(),
            count: src.count,
            sorted: src.sorted,
            consolidated: src.consolidated,
            schema: src.schema,
        };
        Box::into_raw(Box::new(cloned)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_clear(handle: *mut libc::c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        b.clear();
    });
}

#[no_mangle]
pub extern "C" fn gnitz_batch_region_ptr(handle: *const libc::c_void, idx: u32) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.region_ptr(idx as usize)
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn gnitz_batch_region_size(handle: *const libc::c_void, idx: u32) -> u32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.region_size(idx as usize) as u32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_batch_num_regions(handle: *const libc::c_void) -> u32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.num_regions() as u32
    });
    result.unwrap_or(0)
}

/// Construct a batch from FFI region pointers (used by IPC decode / from_buffers).
#[no_mangle]
pub extern "C" fn gnitz_batch_from_regions(
    schema_desc: *const crate::compact::SchemaDescriptor,
    ptrs: *const *const u8,
    sizes: *const u32,
    count: u32,
    regions_per_batch: u32,
) -> *mut libc::c_void {
    if schema_desc.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let schema = unsafe { *schema_desc };
        let npc = schema.num_columns as usize - 1;
        let rpb = regions_per_batch as usize;
        let p = unsafe { slice::from_raw_parts(ptrs, rpb) };
        let s = unsafe { slice::from_raw_parts(sizes, rpb) };
        let mut batch = unsafe {
            crate::memtable::OwnedBatch::from_regions(p, s, count as usize, npc)
        };
        batch.schema = Some(schema);
        Box::into_raw(Box::new(batch)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

// ---------------------------------------------------------------------------
// PartitionedTable (hash-routed N-way table handle)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_ptable_create(
    dir: *const libc::c_char,
    name: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
    table_id: u32,
    num_partitions: u32,
    durable: i32,
    part_start: u32,
    part_end: u32,
    arena_size: u64,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if dir.is_null() || name.is_null() || schema_desc.is_null() {
            return ptr::null_mut();
        }
        let dir_s = unsafe { CStr::from_ptr(dir) }.to_str().unwrap_or("");
        let name_s = unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("");
        let schema = unsafe { *schema_desc };
        match crate::partitioned_table::PartitionedTable::new(
            dir_s, name_s, schema, table_id, num_partitions,
            durable != 0, part_start, part_end, arena_size,
        ) {
            Ok(pt) => Box::into_raw(Box::new(pt)) as *mut libc::c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_close(handle: *mut libc::c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        unsafe { let _ = Box::from_raw(handle as *mut crate::partitioned_table::PartitionedTable); }
    });
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_ingest_batch(
    handle: *mut libc::c_void,
    in_ptrs: *const *const u8, in_sizes: *const u32,
    row_count: u32, regions_per_batch: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        let rpb = regions_per_batch as usize;
        let npc = pt.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(in_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(in_sizes, rpb) };
        match pt.ingest_batch_from_regions(ptrs, sizes, row_count, npc) {
            Ok(()) => 0, Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_ingest_batch_memonly(
    handle: *mut libc::c_void,
    in_ptrs: *const *const u8, in_sizes: *const u32,
    row_count: u32, regions_per_batch: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        let rpb = regions_per_batch as usize;
        let npc = pt.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(in_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(in_sizes, rpb) };
        match pt.ingest_batch_memonly_from_regions(ptrs, sizes, row_count, npc) {
            Ok(()) => 0, Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_create_cursor(handle: *mut libc::c_void) -> *mut libc::c_void {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        match pt.create_cursor() {
            Ok(ch) => Box::into_raw(Box::new(ch)) as *mut libc::c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_has_pk(
    handle: *mut libc::c_void, key_lo: u64, key_hi: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        pt.has_pk(key_lo, key_hi) as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_retract_pk(
    handle: *mut libc::c_void, key_lo: u64, key_hi: u64, out_found: *mut i32,
) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        let (weight, found) = pt.retract_pk(key_lo, key_hi);
        unsafe { if !out_found.is_null() { *out_found = found as i32; } }
        weight
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_get_weight(
    handle: *mut libc::c_void, key_lo: u64, key_hi: u64,
    ref_ptrs: *const *const u8, ref_sizes: *const u32,
    ref_count: u32, regions_per_batch: u32,
) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        let rpb = regions_per_batch as usize;
        let npc = pt.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(ref_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(ref_sizes, rpb) };
        let ref_batch = unsafe {
            crate::merge::parse_single_batch_from_regions(ptrs, sizes, ref_count as usize, npc)
        };
        pt.get_weight_for_row(key_lo, key_hi, &ref_batch, 0)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_flush(handle: *mut libc::c_void) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        match pt.flush() { Ok(_) => 0, Err(e) => e }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_compact_if_needed(handle: *mut libc::c_void) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        match pt.compact_if_needed() { Ok(()) => 0, Err(e) => e }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_set_has_wal(handle: *mut libc::c_void, flag: i32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        pt.set_has_wal(flag != 0);
    });
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_current_lsn(handle: *const libc::c_void) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &*(handle as *const crate::partitioned_table::PartitionedTable) };
        pt.current_lsn()
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_close_partitions_outside(
    handle: *mut libc::c_void, start: u32, end: u32,
) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        pt.close_partitions_outside(start, end);
    });
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_close_all_partitions(handle: *mut libc::c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        pt.close_all_partitions();
    });
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_found_null_word(handle: *const libc::c_void) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &*(handle as *const crate::partitioned_table::PartitionedTable) };
        pt.found_null_word()
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_found_col_ptr(
    handle: *const libc::c_void, payload_col: i32, col_size: i32,
) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &*(handle as *const crate::partitioned_table::PartitionedTable) };
        pt.found_col_ptr(payload_col as usize, col_size as usize)
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_found_blob_ptr(handle: *const libc::c_void) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &*(handle as *const crate::partitioned_table::PartitionedTable) };
        pt.found_blob_ptr()
    });
    result.unwrap_or(ptr::null())
}

/// Get the child base directory for a PartitionedTable (partition 0's directory).
#[no_mangle]
pub extern "C" fn gnitz_ptable_get_child_dir(
    handle: *const libc::c_void,
    buf: *mut libc::c_char,
    buf_len: u32,
) -> i32 {
    if handle.is_null() || buf.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &*(handle as *const crate::partitioned_table::PartitionedTable) };
        let dir = pt.child_base_dir();
        let bytes = dir.as_bytes();
        let n = bytes.len().min(buf_len as usize - 1);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, n);
        }
        n as i32
    });
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_create_child(
    handle: *const libc::c_void,
    child_name: *const libc::c_char,
    child_schema: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if handle.is_null() || child_name.is_null() || child_schema.is_null() {
        return ptr::null_mut();
    }
    let result = panic::catch_unwind(|| {
        let pt = unsafe { &*(handle as *const crate::partitioned_table::PartitionedTable) };
        let name_s = unsafe { CStr::from_ptr(child_name) }.to_str().unwrap_or("");
        let schema = unsafe { *child_schema };
        match pt.create_child(name_s, schema) {
            Ok(child) => Box::into_raw(Box::new(child)) as *mut libc::c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_ptable_bloom_add(
    handle: *mut libc::c_void, key_lo: u64, key_hi: u64,
) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let pt = unsafe { &mut *(handle as *mut crate::partitioned_table::PartitionedTable) };
        pt.bloom_add(key_lo, key_hi);
    });
}

// ---------------------------------------------------------------------------
// Table (unified opaque handle: MemTable + ShardIndex + optional WAL)
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn gnitz_table_create(
    dir: *const libc::c_char,
    name: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
    table_id: u32,
    arena_size: u64,
    durable: i32,
) -> *mut libc::c_void {
    let result = panic::catch_unwind(|| {
        if dir.is_null() || name.is_null() || schema_desc.is_null() {
            return ptr::null_mut();
        }
        let dir_s = unsafe { CStr::from_ptr(dir) }.to_str().unwrap_or("");
        let name_s = unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("");
        let schema = unsafe { *schema_desc };
        match crate::table::Table::new(dir_s, name_s, schema, table_id, arena_size, durable != 0) {
            Ok(t) => Box::into_raw(Box::new(t)) as *mut libc::c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_close(handle: *mut libc::c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        unsafe { let _ = Box::from_raw(handle as *mut crate::table::Table); }
    });
}

#[no_mangle]
pub extern "C" fn gnitz_table_ingest_batch(
    handle: *mut libc::c_void,
    in_ptrs: *const *const u8,
    in_sizes: *const u32,
    row_count: u32,
    regions_per_batch: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        let rpb = regions_per_batch as usize;
        let npc = t.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(in_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(in_sizes, rpb) };
        match t.ingest_batch_from_regions(ptrs, sizes, row_count, npc) {
            Ok(()) => 0,
            Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_table_ingest_batch_memonly(
    handle: *mut libc::c_void,
    in_ptrs: *const *const u8,
    in_sizes: *const u32,
    row_count: u32,
    regions_per_batch: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        let rpb = regions_per_batch as usize;
        let npc = t.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(in_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(in_sizes, rpb) };
        match t.ingest_batch_memonly_from_regions(ptrs, sizes, row_count, npc) {
            Ok(()) => 0,
            Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_table_create_cursor(
    handle: *mut libc::c_void,
) -> *mut libc::c_void {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        match t.create_cursor() {
            Ok(ch) => Box::into_raw(Box::new(ch)) as *mut libc::c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_has_pk(
    handle: *mut libc::c_void,
    key_lo: u64, key_hi: u64,
) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        t.has_pk(key_lo, key_hi) as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_retract_pk(
    handle: *mut libc::c_void,
    key_lo: u64, key_hi: u64,
    out_found: *mut i32,
) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        let (weight, found) = t.retract_pk(key_lo, key_hi);
        unsafe { if !out_found.is_null() { *out_found = found as i32; } }
        weight
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_get_weight(
    handle: *mut libc::c_void,
    key_lo: u64, key_hi: u64,
    ref_ptrs: *const *const u8,
    ref_sizes: *const u32,
    ref_count: u32,
    regions_per_batch: u32,
) -> i64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        let rpb = regions_per_batch as usize;
        let npc = t.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(ref_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(ref_sizes, rpb) };
        let ref_batch = unsafe {
            crate::merge::parse_single_batch_from_regions(
                ptrs, sizes, ref_count as usize, npc,
            )
        };
        t.get_weight_for_row(key_lo, key_hi, &ref_batch, 0)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_flush(handle: *mut libc::c_void) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        match t.flush() {
            Ok(true) => 0,
            Ok(false) => -1, // empty, nothing written
            Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_table_compact_if_needed(handle: *mut libc::c_void) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        match t.compact_if_needed() {
            Ok(()) => 0,
            Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_table_set_has_wal(handle: *mut libc::c_void, flag: i32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        t.set_has_wal(flag != 0);
    });
}

#[no_mangle]
pub extern "C" fn gnitz_table_current_lsn(handle: *const libc::c_void) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        t.current_lsn
    });
    result.unwrap_or(0)
}

// Table: accumulator support

#[no_mangle]
pub extern "C" fn gnitz_table_bloom_add(
    handle: *mut libc::c_void, key_lo: u64, key_hi: u64,
) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        t.bloom_add(key_lo, key_hi);
    });
}

#[no_mangle]
pub extern "C" fn gnitz_table_memtable_upsert_batch(
    handle: *mut libc::c_void,
    in_ptrs: *const *const u8,
    in_sizes: *const u32,
    row_count: u32,
    regions_per_batch: u32,
) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        let rpb = regions_per_batch as usize;
        let npc = t.schema().num_columns as usize - 1;
        let ptrs = unsafe { slice::from_raw_parts(in_ptrs, rpb) };
        let sizes = unsafe { slice::from_raw_parts(in_sizes, rpb) };
        let batch = unsafe {
            crate::memtable::OwnedBatch::from_regions(ptrs, sizes, row_count as usize, npc)
        };
        match t.memtable_upsert_sorted_batch(batch) {
            Ok(()) => 0,
            Err(e) => e,
        }
    });
    result.unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn gnitz_table_memtable_should_flush(handle: *const libc::c_void) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        t.memtable_should_flush() as i32
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_memtable_is_empty(handle: *const libc::c_void) -> i32 {
    if handle.is_null() { return 1; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        t.memtable_is_empty() as i32
    });
    result.unwrap_or(1)
}

// Table: found-row accessors

#[no_mangle]
pub extern "C" fn gnitz_table_found_null_word(handle: *const libc::c_void) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        t.found_null_word()
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn gnitz_table_found_col_ptr(
    handle: *const libc::c_void, payload_col: i32, col_size: i32,
) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        t.found_col_ptr(payload_col as usize, col_size as usize)
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn gnitz_table_found_blob_ptr(handle: *const libc::c_void) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        t.found_blob_ptr()
    });
    result.unwrap_or(ptr::null())
}

// Table: PartitionedTable support

#[no_mangle]
pub extern "C" fn gnitz_table_get_snapshot(handle: *mut libc::c_void) -> *mut libc::c_void {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &mut *(handle as *mut crate::table::Table) };
        let arc = t.get_snapshot();
        let snap = crate::memtable::MemTableSnapshot { inner: arc };
        Box::into_raw(Box::new(snap)) as *mut libc::c_void
    });
    result.unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn gnitz_table_all_shard_ptrs(
    handle: *const libc::c_void,
    out_ptrs: *mut *const libc::c_void,
    max_ptrs: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        let ptrs = t.all_shard_ptrs();
        let n = ptrs.len().min(max_ptrs as usize);
        let out = unsafe { slice::from_raw_parts_mut(out_ptrs, n) };
        for i in 0..n {
            out[i] = ptrs[i] as *const libc::c_void;
        }
        n as i32
    });
    result.unwrap_or(0)
}

// Table: child table creation

#[no_mangle]
pub extern "C" fn gnitz_table_create_child(
    handle: *const libc::c_void,
    child_name: *const libc::c_char,
    child_schema: *const crate::compact::SchemaDescriptor,
) -> *mut libc::c_void {
    if handle.is_null() || child_name.is_null() || child_schema.is_null() {
        return ptr::null_mut();
    }
    let result = panic::catch_unwind(|| {
        let t = unsafe { &*(handle as *const crate::table::Table) };
        let name_s = unsafe { CStr::from_ptr(child_name) }.to_str().unwrap_or("");
        let schema = unsafe { *child_schema };
        match t.create_child(name_s, schema) {
            Ok(child) => Box::into_raw(Box::new(child)) as *mut libc::c_void,
            Err(_) => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

// ---------------------------------------------------------------------------
// Shard Index (opaque handle for FLSM lifecycle, compaction, manifest I/O)
// ---------------------------------------------------------------------------

/// Create a new ShardIndex handle.
/// Caller owns the handle; free with gnitz_shard_index_close.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_create(
    table_id: u32,
    output_dir: *const libc::c_char,
    schema_desc: *const crate::compact::SchemaDescriptor,
) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        if output_dir.is_null() || schema_desc.is_null() {
            return ptr::null_mut();
        }
        let dir = unsafe { CStr::from_ptr(output_dir) }.to_str().unwrap_or("");
        let schema = unsafe { *schema_desc };
        let idx = crate::shard_index::ShardIndex::new(table_id, dir, schema);
        Box::into_raw(Box::new(idx)) as *mut c_void
    });
    result.unwrap_or(ptr::null_mut())
}

/// Close and free a ShardIndex handle.
/// Drops all owned MappedShards (unmaps them).
#[no_mangle]
pub extern "C" fn gnitz_shard_index_close(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        unsafe { drop(Box::from_raw(handle as *mut crate::shard_index::ShardIndex)); }
    });
}

/// Load manifest entries into the index.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_load_manifest(
    handle: *mut c_void,
    manifest_path: *const libc::c_char,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() || manifest_path.is_null() {
            return -1;
        }
        let idx = unsafe { &mut *(handle as *mut crate::shard_index::ShardIndex) };
        let path = unsafe { CStr::from_ptr(manifest_path) }.to_str().unwrap_or("");
        match idx.load_manifest(path) {
            Ok(()) => 0,
            Err(code) => code,
        }
    });
    result.unwrap_or(-99)
}

/// Add a shard file to L0.
/// Returns 1 if needs_compaction, 0 if not, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_add_shard(
    handle: *mut c_void,
    shard_path: *const libc::c_char,
    min_lsn: u64,
    max_lsn: u64,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() || shard_path.is_null() {
            return -1;
        }
        let idx = unsafe { &mut *(handle as *mut crate::shard_index::ShardIndex) };
        let path = unsafe { CStr::from_ptr(shard_path) }.to_str().unwrap_or("");
        match idx.add_shard(path, min_lsn, max_lsn) {
            Ok(()) => if idx.needs_compaction { 1 } else { 0 },
            Err(code) => code,
        }
    });
    result.unwrap_or(-99)
}

/// Run compaction (L0 → L1, horizontal, vertical).
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_compact(handle: *mut c_void) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return -1;
        }
        let idx = unsafe { &mut *(handle as *mut crate::shard_index::ShardIndex) };
        match idx.run_compact() {
            Ok(()) => 0,
            Err(code) => code,
        }
    });
    result.unwrap_or(-99)
}

/// Publish current index state as a manifest file.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_publish_manifest(
    handle: *mut c_void,
    manifest_path: *const libc::c_char,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() || manifest_path.is_null() {
            return -1;
        }
        let idx = unsafe { &*(handle as *const crate::shard_index::ShardIndex) };
        let path = unsafe { CStr::from_ptr(manifest_path) }.to_str().unwrap_or("");
        match idx.publish_manifest(path) {
            Ok(()) => 0,
            Err(code) => code,
        }
    });
    result.unwrap_or(-99)
}

/// Try to delete pending shard files.
/// Returns count of successfully deleted files, or negative on panic.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_try_cleanup(handle: *mut c_void) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return 0;
        }
        let idx = unsafe { &mut *(handle as *mut crate::shard_index::ShardIndex) };
        idx.try_cleanup() as i32
    });
    result.unwrap_or(0)
}

/// Check if compaction is needed.
/// Returns 1 if yes, 0 if no.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_needs_compaction(handle: *const c_void) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return 0;
        }
        let idx = unsafe { &*(handle as *const crate::shard_index::ShardIndex) };
        if idx.needs_compaction { 1 } else { 0 }
    });
    result.unwrap_or(0)
}

/// Return the maximum LSN across all shards.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_max_lsn(handle: *const c_void) -> u64 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() {
            return 0;
        }
        let idx = unsafe { &*(handle as *const crate::shard_index::ShardIndex) };
        idx.max_lsn()
    });
    result.unwrap_or(0)
}

/// Fill out_ptrs with pointers to all MappedShard objects.
/// Returns count of shards written, capped at max_ptrs.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_all_shard_ptrs(
    handle: *const c_void,
    out_ptrs: *mut *const c_void,
    max_ptrs: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() || out_ptrs.is_null() {
            return 0;
        }
        let idx = unsafe { &*(handle as *const crate::shard_index::ShardIndex) };
        let ptrs = idx.all_shard_ptrs();
        let n = ptrs.len().min(max_ptrs as usize);
        let out = unsafe { slice::from_raw_parts_mut(out_ptrs, n) };
        for i in 0..n {
            out[i] = ptrs[i] as *const c_void;
        }
        n as i32
    });
    result.unwrap_or(0)
}

/// Point lookup: find all shards containing key (key_lo, key_hi).
/// Fills out_shard_ptrs and out_row_indices arrays.
/// Returns count of matches, capped at max_results, or negative on error.
#[no_mangle]
pub extern "C" fn gnitz_shard_index_find_pk(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
    out_shard_ptrs: *mut *const c_void,
    out_row_indices: *mut i32,
    max_results: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if handle.is_null() || out_shard_ptrs.is_null() || out_row_indices.is_null() {
            return 0;
        }
        let idx = unsafe { &*(handle as *const crate::shard_index::ShardIndex) };
        let max = max_results as usize;
        let ptrs = unsafe { slice::from_raw_parts_mut(out_shard_ptrs, max) };
        let indices = unsafe { slice::from_raw_parts_mut(out_row_indices, max) };
        let mut n = 0usize;
        idx.find_pk(key_lo, key_hi, &mut |shard_ptr, row_idx| {
            if n < max {
                ptrs[n] = shard_ptr as *const c_void;
                indices[n] = row_idx as i32;
                n += 1;
            }
        });
        n as i32
    });
    result.unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Exchange repartitioning FFI
// ---------------------------------------------------------------------------

/// Repartition a single batch across N workers by hashing on col_indices.
/// out_handles must be caller-allocated with num_workers slots, pre-filled
/// with NULL. Non-empty sub-batch handles are stored in out_handles[w].
/// Empty workers leave their slot as NULL.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_repartition_batch(
    src_batch: *const libc::c_void,
    col_indices: *const u32,
    num_col_indices: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
    num_workers: u32,
    out_handles: *mut *mut libc::c_void,
) -> i32 {
    if src_batch.is_null() || schema_desc.is_null() || out_handles.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch = unsafe { &*(src_batch as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_desc };
        let nw = num_workers as usize;
        let col_idx: &[u32] = if col_indices.is_null() || num_col_indices == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(col_indices, num_col_indices as usize) }
        };
        let sub_batches = crate::ops::op_repartition_batch(batch, col_idx, schema, nw);
        for (w, sb) in sub_batches.into_iter().enumerate() {
            if sb.count > 0 {
                unsafe {
                    *out_handles.add(w) = Box::into_raw(Box::new(sb)) as *mut libc::c_void;
                }
            }
        }
        0i32
    });
    result.unwrap_or(-99)
}

/// Scatter N source batches across num_workers, choosing the merged
/// (consolidated) or fallback path based on source flags.
/// source_handles[i] may be NULL (absent worker). out_handles must be
/// caller-allocated with num_workers slots, pre-filled with NULL.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_relay_scatter(
    source_handles: *const *mut libc::c_void,
    num_sources: u32,
    col_indices: *const u32,
    num_col_indices: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
    num_workers: u32,
    out_handles: *mut *mut libc::c_void,
) -> i32 {
    if schema_desc.is_null() || out_handles.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let schema = unsafe { &*schema_desc };
        let nw = num_workers as usize;
        let ns = num_sources as usize;
        let col_idx: &[u32] = if col_indices.is_null() || num_col_indices == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(col_indices, num_col_indices as usize) }
        };
        let mut sources: Vec<Option<&crate::memtable::OwnedBatch>> = Vec::with_capacity(ns);
        for i in 0..ns {
            let h = if source_handles.is_null() {
                ptr::null_mut()
            } else {
                unsafe { *source_handles.add(i) }
            };
            if h.is_null() {
                sources.push(None);
            } else {
                sources.push(Some(unsafe { &*(h as *const crate::memtable::OwnedBatch) }));
            }
        }
        let sub_batches = crate::ops::op_relay_scatter(&sources, col_idx, schema, nw);
        for (w, sb) in sub_batches.into_iter().enumerate() {
            if sb.count > 0 {
                unsafe {
                    *out_handles.add(w) = Box::into_raw(Box::new(sb)) as *mut libc::c_void;
                }
            }
        }
        0i32
    });
    result.unwrap_or(-99)
}

/// Scatter one batch by M column specs simultaneously.
/// col_specs_flat: concatenation of all spec arrays.
/// spec_lengths[i]: length of spec i.
/// out_handles: caller-allocated [num_specs * num_workers], pre-filled NULL.
/// Result stored at out_handles[si * num_workers + w].
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_multi_scatter(
    src_batch: *const libc::c_void,
    col_specs_flat: *const u32,
    spec_lengths: *const u32,
    num_specs: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
    num_workers: u32,
    out_handles: *mut *mut libc::c_void,
) -> i32 {
    if src_batch.is_null() || schema_desc.is_null() || out_handles.is_null()
        || (num_specs > 0 && spec_lengths.is_null())
    {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        let batch = unsafe { &*(src_batch as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_desc };
        let nw = num_workers as usize;
        let ns = num_specs as usize;

        let mut col_specs: Vec<&[u32]> = Vec::with_capacity(ns);
        let mut offset = 0usize;
        for si in 0..ns {
            let len = unsafe { *spec_lengths.add(si) } as usize;
            let spec: &[u32] = if len == 0 || col_specs_flat.is_null() {
                &[]
            } else {
                unsafe { slice::from_raw_parts(col_specs_flat.add(offset), len) }
            };
            col_specs.push(spec);
            offset += len;
        }

        let results = crate::ops::op_multi_scatter(batch, &col_specs, schema, nw);
        for (si, worker_batches) in results.into_iter().enumerate() {
            for (w, sb) in worker_batches.into_iter().enumerate() {
                if sb.count > 0 {
                    unsafe {
                        *out_handles.add(si * nw + w) =
                            Box::into_raw(Box::new(sb)) as *mut libc::c_void;
                    }
                }
            }
        }
        0i32
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Partition routing FFI
// ---------------------------------------------------------------------------

/// Map a 128-bit PK to a partition index in [0, 255].
#[no_mangle]
pub extern "C" fn gnitz_partition_for_key(pk_lo: u64, pk_hi: u64) -> u32 {
    crate::partitioned_table::partition_for_key(pk_lo, pk_hi) as u32
}

/// Map a 128-bit PK to its worker index given `num_workers`.
#[no_mangle]
pub extern "C" fn gnitz_worker_for_pk(pk_lo: u64, pk_hi: u64, num_workers: u32) -> u32 {
    let partition = crate::partitioned_table::partition_for_key(pk_lo, pk_hi);
    crate::ops::worker_for_partition_pub(partition, num_workers as usize) as u32
}

/// Create a new PartitionRouter. Returns an opaque heap pointer.
#[no_mangle]
pub extern "C" fn gnitz_partition_router_create() -> *mut libc::c_void {
    Box::into_raw(Box::new(crate::ops::PartitionRouter::new())) as *mut libc::c_void
}

/// Free a PartitionRouter created by `gnitz_partition_router_create`.
#[no_mangle]
pub extern "C" fn gnitz_partition_router_free(handle: *mut libc::c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle as *mut crate::ops::PartitionRouter)) };
    }
}

/// Query the routing cache. Returns worker index, or -1 on cache miss.
#[no_mangle]
pub extern "C" fn gnitz_partition_router_worker_for_index_key(
    handle: *const libc::c_void,
    tid: u32,
    col_idx: u32,
    key_lo: u64,
) -> i32 {
    if handle.is_null() {
        return -1;
    }
    let router = unsafe { &*(handle as *const crate::ops::PartitionRouter) };
    router.worker_for_index_key(tid, col_idx, key_lo)
}

/// Populate or retract routing entries from a sub-batch routed to `worker`.
/// `schema_ptr` must point to the packed SchemaDescriptor for the batch.
#[no_mangle]
pub extern "C" fn gnitz_partition_router_record_routing(
    handle: *mut libc::c_void,
    batch_handle: *const libc::c_void,
    schema_ptr: *const crate::compact::SchemaDescriptor,
    tid: u32,
    col_idx: u32,
    worker: u32,
) {
    if handle.is_null() || batch_handle.is_null() || schema_ptr.is_null() {
        return;
    }
    let result = std::panic::catch_unwind(|| {
        let router = unsafe { &mut *(handle as *mut crate::ops::PartitionRouter) };
        let batch = unsafe { &*(batch_handle as *const crate::memtable::OwnedBatch) };
        let schema = unsafe { &*schema_ptr };
        router.record_routing(batch, schema, tid, col_idx, worker);
    });
    let _ = result;
}

// ---------------------------------------------------------------------------
// Circuit compiler FFI
// ---------------------------------------------------------------------------

/// Compile a view circuit entirely in Rust.
///
/// Takes system table handles + external registry and returns a CompileResult
/// containing VmHandle(s) + metadata.
///
/// Returns 0 on success (result populated), negative on error.
#[no_mangle]
pub extern "C" fn gnitz_compile_view(
    view_id: u64,
    // 6 system table handles (NULL if absent)
    sys_nodes: *mut libc::c_void,
    sys_edges: *mut libc::c_void,
    sys_sources: *mut libc::c_void,
    sys_params: *mut libc::c_void,
    sys_gcols: *mut libc::c_void,
    sys_dep: *mut libc::c_void,
    // 6 system table schemas (packed SchemaDescriptor pointers)
    sys_nodes_schema: *const crate::compact::SchemaDescriptor,
    sys_edges_schema: *const crate::compact::SchemaDescriptor,
    sys_sources_schema: *const crate::compact::SchemaDescriptor,
    sys_params_schema: *const crate::compact::SchemaDescriptor,
    sys_gcols_schema: *const crate::compact::SchemaDescriptor,
    sys_dep_schema: *const crate::compact::SchemaDescriptor,
    // View family directory, table_id, and schema
    view_dir: *const libc::c_char,
    view_table_id: u32,
    view_schema: *const crate::compact::SchemaDescriptor,
    // External table registry: parallel arrays
    reg_tids: *const i64,
    reg_handles: *const *mut libc::c_void,
    reg_schemas: *const crate::compact::SchemaDescriptor,
    reg_count: u32,
    // Output: flat i64 buffer of COMPILE_RESULT_SLOTS entries
    out_result: *mut i64,
) -> i32 {
    if out_result.is_null() || view_schema.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(|| {
        // Build external tables array
        let ext_tables: Vec<crate::compiler::ExternalTable> = if reg_count > 0 && !reg_tids.is_null() && !reg_handles.is_null() && !reg_schemas.is_null() {
            (0..reg_count as usize).map(|i| unsafe {
                crate::compiler::ExternalTable {
                    table_id: *reg_tids.add(i),
                    handle: *reg_handles.add(i) as *mut crate::table::Table,
                    schema: *reg_schemas.add(i),
                }
            }).collect()
        } else {
            Vec::new()
        };

        let view_dir_str = if view_dir.is_null() {
            String::new()
        } else {
            unsafe { std::ffi::CStr::from_ptr(view_dir) }.to_string_lossy().into_owned()
        };

        let compile_result = unsafe {
            crate::compiler::compile_view(
                view_id,
                sys_nodes as *mut crate::table::Table,
                sys_edges as *mut crate::table::Table,
                sys_sources as *mut crate::table::Table,
                sys_params as *mut crate::table::Table,
                sys_gcols as *mut crate::table::Table,
                sys_dep as *mut crate::table::Table,
                &*sys_nodes_schema,
                &*sys_edges_schema,
                &*sys_sources_schema,
                &*sys_params_schema,
                &*sys_gcols_schema,
                &*sys_dep_schema,
                &view_dir_str,
                view_table_id,
                &*view_schema,
                &ext_tables,
            )
        };

        match compile_result {
            Ok(output) => {
                let cr = output.to_result_buffer();
                let out_slice = unsafe {
                    slice::from_raw_parts_mut(out_result, crate::compiler::COMPILE_RESULT_SLOTS)
                };
                out_slice.copy_from_slice(&cr.buf);
                0
            }
            Err(code) => {
                let cr = crate::compiler::CompileResult::error(code);
                let out_slice = unsafe {
                    slice::from_raw_parts_mut(out_result, crate::compiler::COMPILE_RESULT_SLOTS)
                };
                out_slice.copy_from_slice(&cr.buf);
                code
            }
        }
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// DagEngine FFI
// ---------------------------------------------------------------------------

use crate::dag::{DagEngine, StoreHandle, SysTableRefs};

/// Create a new DagEngine. Returns an opaque handle.
#[no_mangle]
pub extern "C" fn gnitz_dag_create() -> *mut c_void {
    let result = panic::catch_unwind(|| {
        let dag = Box::new(DagEngine::new());
        Box::into_raw(dag) as *mut c_void
    });
    result.unwrap_or(ptr::null_mut())
}

/// Destroy a DagEngine.
#[no_mangle]
pub extern "C" fn gnitz_dag_destroy(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let mut dag = unsafe { Box::from_raw(handle as *mut DagEngine) };
        dag.close();
    });
}

/// Set the 6 system table handles + schemas on a DagEngine.
#[no_mangle]
pub extern "C" fn gnitz_dag_set_sys_tables(
    handle: *mut c_void,
    h_nodes: *mut c_void,
    h_edges: *mut c_void,
    h_sources: *mut c_void,
    h_params: *mut c_void,
    h_gcols: *mut c_void,
    h_dep: *mut c_void,
    s_nodes: *const crate::compact::SchemaDescriptor,
    s_edges: *const crate::compact::SchemaDescriptor,
    s_sources: *const crate::compact::SchemaDescriptor,
    s_params: *const crate::compact::SchemaDescriptor,
    s_gcols: *const crate::compact::SchemaDescriptor,
    s_dep: *const crate::compact::SchemaDescriptor,
) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.set_sys_tables(SysTableRefs {
            nodes: h_nodes as *mut crate::table::Table,
            edges: h_edges as *mut crate::table::Table,
            sources: h_sources as *mut crate::table::Table,
            params: h_params as *mut crate::table::Table,
            group_cols: h_gcols as *mut crate::table::Table,
            dep_tab: h_dep as *mut crate::table::Table,
            nodes_schema: if s_nodes.is_null() { crate::compiler::empty_schema() } else { unsafe { *s_nodes } },
            edges_schema: if s_edges.is_null() { crate::compiler::empty_schema() } else { unsafe { *s_edges } },
            sources_schema: if s_sources.is_null() { crate::compiler::empty_schema() } else { unsafe { *s_sources } },
            params_schema: if s_params.is_null() { crate::compiler::empty_schema() } else { unsafe { *s_params } },
            group_cols_schema: if s_gcols.is_null() { crate::compiler::empty_schema() } else { unsafe { *s_gcols } },
            dep_tab_schema: if s_dep.is_null() { crate::compiler::empty_schema() } else { unsafe { *s_dep } },
        });
    });
}

/// Register a table with the DagEngine.
/// is_partitioned: 0 = Single (EphemeralTable), 1 = Partitioned.
#[no_mangle]
pub extern "C" fn gnitz_dag_register_table(
    handle: *mut c_void,
    table_id: i64,
    store_handle: *mut c_void,
    schema: *const crate::compact::SchemaDescriptor,
    depth: i32,
    unique_pk: c_int,
    is_partitioned: c_int,
    dir_ptr: *const libc::c_char,
) {
    if handle.is_null() || schema.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let sd = unsafe { *schema };
        let sh = if is_partitioned != 0 {
            StoreHandle::Partitioned(store_handle as *mut crate::partitioned_table::PartitionedTable)
        } else {
            StoreHandle::Single(store_handle as *mut crate::table::Table)
        };
        let dir = if dir_ptr.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(dir_ptr) }.to_string_lossy().into_owned()
        };
        dag.register_table(table_id, sh, sd, depth, unique_pk != 0, dir);
    });
}

/// Unregister a table from the DagEngine.
#[no_mangle]
pub extern "C" fn gnitz_dag_unregister_table(handle: *mut c_void, table_id: i64) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.unregister_table(table_id);
    });
}

/// Set the depth of a table in the DagEngine.
#[no_mangle]
pub extern "C" fn gnitz_dag_set_depth(handle: *mut c_void, table_id: i64, depth: i32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.set_depth(table_id, depth);
    });
}

/// Add an index circuit to a table.
#[no_mangle]
pub extern "C" fn gnitz_dag_add_index_circuit(
    handle: *mut c_void,
    table_id: i64,
    col_idx: u32,
    idx_handle: *mut c_void,
    idx_schema: *const crate::compact::SchemaDescriptor,
    is_unique: c_int,
) {
    if handle.is_null() || idx_schema.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let sd = unsafe { *idx_schema };
        dag.add_index_circuit(
            table_id, col_idx,
            idx_handle as *mut crate::table::Table,
            sd, is_unique != 0,
        );
    });
}

/// Remove an index circuit from a table.
#[no_mangle]
pub extern "C" fn gnitz_dag_remove_index_circuit(
    handle: *mut c_void,
    table_id: i64,
    col_idx: u32,
) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.remove_index_circuit(table_id, col_idx);
    });
}

/// Invalidate a single view's cached plan.
#[no_mangle]
pub extern "C" fn gnitz_dag_invalidate(handle: *mut c_void, view_id: i64) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.invalidate(view_id);
    });
}

/// Invalidate all cached plans.
#[no_mangle]
pub extern "C" fn gnitz_dag_invalidate_all(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.invalidate_all();
    });
}

/// Invalidate the dependency map.
#[no_mangle]
pub extern "C" fn gnitz_dag_invalidate_dep_map(handle: *mut c_void) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.invalidate_dep_map();
    });
}

/// Run the full single-worker DAG evaluation.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_dag_evaluate(
    handle: *mut c_void,
    source_id: i64,
    delta_handle: *mut c_void,
) -> i32 {
    if handle.is_null() || delta_handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let delta = unsafe { Box::from_raw(delta_handle as *mut crate::memtable::OwnedBatch) };
        dag.evaluate_dag(source_id, *delta)
    });
    result.unwrap_or(-99)
}

/// Execute a single epoch for one view (multi-worker path).
/// Returns a new batch handle (caller owns) or null.
#[no_mangle]
pub extern "C" fn gnitz_dag_execute_epoch(
    handle: *mut c_void,
    view_id: i64,
    input_handle: *mut c_void,
    source_id: i64,
) -> *mut c_void {
    if handle.is_null() || input_handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let input = unsafe { Box::from_raw(input_handle as *mut crate::memtable::OwnedBatch) };
        match dag.execute_epoch(view_id, *input, source_id) {
            Some(batch) => Box::into_raw(Box::new(batch)) as *mut c_void,
            None => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

/// Ingest a batch into a table via DagEngine (user tables only).
#[no_mangle]
pub extern "C" fn gnitz_dag_ingest(
    handle: *mut c_void,
    table_id: i64,
    batch_handle: *mut c_void,
) -> i32 {
    if handle.is_null() || batch_handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let batch = unsafe { Box::from_raw(batch_handle as *mut crate::memtable::OwnedBatch) };
        dag.ingest_to_family(table_id, *batch)
    });
    result.unwrap_or(-99)
}

/// Flush a table's WAL via DagEngine.
#[no_mangle]
pub extern "C" fn gnitz_dag_flush(handle: *mut c_void, table_id: i64) -> i32 {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        dag.flush(table_id)
    });
    result.unwrap_or(-99)
}

/// Get the dependency map: writes (source_tid, view_id) pairs to out_pairs.
/// Returns count of pairs written (each pair = 2 i64s).
#[no_mangle]
pub extern "C" fn gnitz_dag_get_dep_map(
    handle: *mut c_void,
    out_pairs: *mut i64,
    max_pairs: u32,
) -> i32 {
    if handle.is_null() || out_pairs.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let dm = dag.get_dep_map();
        let mut count: u32 = 0;
        for (&source_tid, view_ids) in dm {
            for &vid in view_ids {
                if count >= max_pairs { return count as i32; }
                unsafe {
                    *out_pairs.add(count as usize * 2) = source_tid;
                    *out_pairs.add(count as usize * 2 + 1) = vid;
                }
                count += 1;
            }
        }
        count as i32
    });
    result.unwrap_or(0)
}

/// Get shard columns for a view.
#[no_mangle]
pub extern "C" fn gnitz_dag_get_shard_cols(
    handle: *mut c_void,
    view_id: i64,
    out_cols: *mut i32,
    max: u32,
) -> i32 {
    if handle.is_null() || out_cols.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let cols = dag.get_shard_cols(view_id);
        let n = cols.len().min(max as usize);
        for i in 0..n {
            unsafe { *out_cols.add(i) = cols[i]; }
        }
        n as i32
    });
    result.unwrap_or(0)
}

/// Get exchange info for a view.
/// out_cols: shard column indices; out_trivial/out_copart: flags.
#[no_mangle]
pub extern "C" fn gnitz_dag_get_exchange_info(
    handle: *mut c_void,
    view_id: i64,
    out_cols: *mut i32,
    max: u32,
    out_trivial: *mut c_int,
    out_copart: *mut c_int,
) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let info = dag.get_exchange_info(view_id);
        let n = info.shard_cols.len().min(max as usize);
        if !out_cols.is_null() {
            for i in 0..n {
                unsafe { *out_cols.add(i) = info.shard_cols[i]; }
            }
        }
        if !out_trivial.is_null() {
            unsafe { *out_trivial = if info.is_trivial { 1 } else { 0 }; }
        }
        if !out_copart.is_null() {
            unsafe { *out_copart = if info.is_co_partitioned { 1 } else { 0 }; }
        }
        n as i32
    });
    result.unwrap_or(0)
}

/// Get source IDs for a view.
#[no_mangle]
pub extern "C" fn gnitz_dag_get_source_ids(
    handle: *mut c_void,
    view_id: i64,
    out_ids: *mut i64,
    max: u32,
) -> i32 {
    if handle.is_null() || out_ids.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let ids = dag.get_source_ids(view_id);
        let n = ids.len().min(max as usize);
        for i in 0..n {
            unsafe { *out_ids.add(i) = ids[i]; }
        }
        n as i32
    });
    result.unwrap_or(0)
}

/// Get preloadable views for a source table.
/// out_vids: view IDs, out_cols: first shard column per view.
#[no_mangle]
pub extern "C" fn gnitz_dag_get_preloadable_views(
    handle: *mut c_void,
    src_tid: i64,
    out_vids: *mut i64,
    out_cols: *mut i32,
    max: u32,
) -> i32 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let views = dag.get_preloadable_views(src_tid);
        let n = views.len().min(max as usize);
        for i in 0..n {
            let (vid, ref cols) = views[i];
            if !out_vids.is_null() { unsafe { *out_vids.add(i) = vid; } }
            if !out_cols.is_null() && !cols.is_empty() {
                unsafe { *out_cols.add(i) = cols[0]; }
            }
        }
        n as i32
    });
    result.unwrap_or(0)
}

/// Get join shard columns for a specific (view_id, source_id) pair.
#[no_mangle]
pub extern "C" fn gnitz_dag_get_join_shard_cols(
    handle: *mut c_void,
    view_id: i64,
    source_id: i64,
    out_cols: *mut i32,
    max: u32,
) -> i32 {
    if handle.is_null() || out_cols.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let cols = dag.get_join_shard_cols(view_id, source_id);
        let n = cols.len().min(max as usize);
        for i in 0..n {
            unsafe { *out_cols.add(i) = cols[i]; }
        }
        n as i32
    });
    result.unwrap_or(0)
}

/// Check if a view needs exchange.
#[no_mangle]
pub extern "C" fn gnitz_dag_view_needs_exchange(handle: *mut c_void, view_id: i64) -> c_int {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        if dag.view_needs_exchange(view_id) { 1 } else { 0 }
    });
    result.unwrap_or(0)
}

/// Get the exchange schema for a view. Writes to out_schema if non-null.
/// Returns 1 if the view has an exchange schema, 0 otherwise.
#[no_mangle]
pub extern "C" fn gnitz_dag_get_exchange_schema(
    handle: *mut c_void,
    view_id: i64,
    out_schema: *mut crate::compact::SchemaDescriptor,
) -> c_int {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        match dag.get_exchange_schema(view_id) {
            Some(schema) => {
                if !out_schema.is_null() {
                    unsafe { *out_schema = schema; }
                }
                1
            }
            None => 0,
        }
    });
    result.unwrap_or(0)
}

/// Ensure a view's plan is compiled. Returns 1 on success, 0 on failure.
#[no_mangle]
pub extern "C" fn gnitz_dag_ensure_compiled(handle: *mut c_void, view_id: i64) -> c_int {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        if dag.ensure_compiled(view_id) { 1 } else { 0 }
    });
    result.unwrap_or(0)
}

/// Execute just the post-plan of a view (after exchange IPC).
#[no_mangle]
pub extern "C" fn gnitz_dag_execute_post_epoch(
    handle: *mut c_void,
    view_id: i64,
    input_handle: *mut c_void,
) -> *mut c_void {
    if handle.is_null() || input_handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        let input = unsafe { Box::from_raw(input_handle as *mut crate::memtable::OwnedBatch) };
        match dag.execute_post_epoch(view_id, *input) {
            Some(batch) => Box::into_raw(Box::new(batch)) as *mut c_void,
            None => ptr::null_mut(),
        }
    });
    result.unwrap_or(ptr::null_mut())
}

/// Check if a source is co-partitioned for a view's join.
#[no_mangle]
pub extern "C" fn gnitz_dag_plan_source_co_partitioned(
    handle: *mut c_void,
    view_id: i64,
    source_id: i64,
) -> c_int {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &mut *(handle as *mut DagEngine) };
        if dag.plan_source_co_partitioned(view_id, source_id) { 1 } else { 0 }
    });
    result.unwrap_or(0)
}

/// Validate a circuit graph structure before persistence.
/// nodes, edges, sources are parallel arrays.
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_dag_validate_graph(
    handle: *mut c_void,
    nodes_ptr: *const i32,    // [nid, opcode] pairs
    nodes_count: u32,
    edges_ptr: *const i32,    // [eid, src, dst, port] quads
    edges_count: u32,
    sources_ptr: *const i64,  // [nid_i64, table_id] pairs
    sources_count: u32,
) -> c_int {
    if handle.is_null() { return -1; }
    let result = panic::catch_unwind(|| {
        let dag = unsafe { &*(handle as *const DagEngine) };

        let nodes: Vec<(i32, i32)> = if nodes_count > 0 && !nodes_ptr.is_null() {
            let s = unsafe { slice::from_raw_parts(nodes_ptr, nodes_count as usize * 2) };
            (0..nodes_count as usize).map(|i| (s[i*2], s[i*2+1])).collect()
        } else {
            Vec::new()
        };

        let edges: Vec<(i32, i32, i32, i32)> = if edges_count > 0 && !edges_ptr.is_null() {
            let s = unsafe { slice::from_raw_parts(edges_ptr, edges_count as usize * 4) };
            (0..edges_count as usize).map(|i| (s[i*4], s[i*4+1], s[i*4+2], s[i*4+3])).collect()
        } else {
            Vec::new()
        };

        let sources: Vec<(i32, i64)> = if sources_count > 0 && !sources_ptr.is_null() {
            let s = unsafe { slice::from_raw_parts(sources_ptr, sources_count as usize * 2) };
            (0..sources_count as usize).map(|i| (s[i*2] as i32, s[i*2+1])).collect()
        } else {
            Vec::new()
        };

        match dag.validate_graph_structure(&nodes, &edges, &sources) {
            Ok(()) => 0,
            Err(_msg) => -1,
        }
    });
    result.unwrap_or(-99)
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
    // Manifest file I/O null safety
    // -----------------------------------------------------------------------

    #[test]
    fn manifest_read_file_null_safety() {
        assert_eq!(gnitz_manifest_read_file(ptr::null(), ptr::null_mut(), 0, ptr::null_mut()), -1);
    }

    #[test]
    fn manifest_write_file_null_safety() {
        assert_eq!(gnitz_manifest_write_file(ptr::null(), ptr::null(), 0, 0), -1);
    }

    // -----------------------------------------------------------------------
    // WAL lifecycle FFI null safety
    // -----------------------------------------------------------------------

    #[test]
    fn wal_writer_close_null_safety() {
        gnitz_wal_writer_close(ptr::null_mut());
    }

    #[test]
    fn wal_reader_close_null_safety() {
        gnitz_wal_reader_close(ptr::null_mut());
    }

    #[test]
    fn wal_writer_append_null_handle() {
        assert_eq!(gnitz_wal_writer_append(ptr::null_mut(), 0, 0, 0, ptr::null(), ptr::null(), 0, 0), -1);
    }

    #[test]
    fn wal_writer_truncate_null_handle() {
        assert_eq!(gnitz_wal_writer_truncate(ptr::null_mut()), -1);
    }

    #[test]
    fn wal_reader_next_null_handle() {
        assert_eq!(gnitz_wal_reader_next(ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), 0), -1);
    }

    // -----------------------------------------------------------------------
    // Shard file FFI null safety
    // -----------------------------------------------------------------------

    #[test]
    fn write_shard_null_safety() {
        assert_eq!(gnitz_write_shard(-100, ptr::null(), 0, 0, ptr::null(), ptr::null(), 0, 0), -1);
    }

    // -----------------------------------------------------------------------
    // Compact FFI null safety
    // -----------------------------------------------------------------------

    #[test]
    fn compact_null_safety() {
        assert_eq!(gnitz_compact_shards(ptr::null(), 0, ptr::null(), ptr::null(), 0), -1);
    }

    // -----------------------------------------------------------------------
    // append_row_from_cursor
    // -----------------------------------------------------------------------

    #[test]
    fn test_append_row_from_cursor() {
        use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};
        use crate::memtable::OwnedBatch;

        // Schema: U64(pk=0), I64, STRING
        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        columns[2] = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
        let schema = SchemaDescriptor { num_columns: 3, pk_index: 0, columns };

        // Build source batch with 2 rows
        // Payload cols: pi=0 → I64, pi=1 → STRING
        let mut src = OwnedBatch::with_schema(schema, 4);
        let s0 = b"short";
        unsafe {
            src.append_row_simple(5, 0, 1, 0,
                &[100i64, 0], &[0u64, 0],
                &[ptr::null(), s0.as_ptr()], &[0, 5]);
        }
        let s1 = b"a long string for blob";
        unsafe {
            src.append_row_simple(10, 0, 1, 0,
                &[200i64, 0], &[0u64, 0],
                &[ptr::null(), s1.as_ptr()], &[0, s1.len() as u32]);
        }

        // Rows are already in PK order; create cursor directly
        src.sorted = true;
        let mb = src.as_mem_batch();
        let cursor = unsafe {
            crate::read_cursor::create_read_cursor(&[mb], &[], schema)
        };
        let mut ch = crate::read_cursor::CursorHandle::from_cursor(cursor);

        let mut dst = OwnedBatch::with_schema(schema, 4);
        let dst_ptr = &mut dst as *mut OwnedBatch as *mut libc::c_void;
        let ch_ptr = &mut ch as *mut crate::read_cursor::CursorHandle as *mut libc::c_void;

        // Copy row 0 (pk=5)
        assert_eq!(gnitz_batch_append_row_from_cursor(dst_ptr, ch_ptr, 42), 0);
        ch.cursor.advance();
        assert!(ch.cursor.valid);
        // Copy row 1 (pk=10)
        assert_eq!(gnitz_batch_append_row_from_cursor(dst_ptr, ch_ptr, 99), 0);

        assert_eq!(dst.count, 2);
        assert_eq!(dst.get_pk_lo(0), 5);
        assert_eq!(dst.get_pk_lo(1), 10);
        assert_eq!(i64::from_le_bytes(dst.weight[0..8].try_into().unwrap()), 42);
        assert_eq!(i64::from_le_bytes(dst.weight[8..16].try_into().unwrap()), 99);

        // Verify I64 payload
        let val0 = i64::from_le_bytes(dst.col_data[0][0..8].try_into().unwrap());
        let val1 = i64::from_le_bytes(dst.col_data[0][8..16].try_into().unwrap());
        assert_eq!(val0, 100);
        assert_eq!(val1, 200);

        // Verify STRING payload
        let str0 = crate::ipc::decode_german_string(
            &dst.col_data[1][0..16].try_into().unwrap(), &dst.blob,
        );
        assert_eq!(str0, b"short");
        let str1 = crate::ipc::decode_german_string(
            &dst.col_data[1][16..32].try_into().unwrap(), &dst.blob,
        );
        assert_eq!(str1, b"a long string for blob");
    }

    #[test]
    fn test_append_row_from_cursor_null_columns() {
        use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};
        use crate::memtable::OwnedBatch;

        // Schema: U64(pk=0), I64(nullable)
        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 1, _pad: 0 };
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

        let mut src = OwnedBatch::with_schema(schema, 4);
        // Row 0: pk=1, null_word=1 (payload col 0 is null)
        unsafe {
            src.append_row_simple(1, 0, 1, 1, &[0i64], &[0u64], &[ptr::null()], &[0]);
        }
        // Row 1: pk=2, val=42
        unsafe {
            src.append_row_simple(2, 0, 1, 0, &[42i64], &[0u64], &[ptr::null()], &[0]);
        }

        src.sorted = true;
        let mb = src.as_mem_batch();
        let cursor = unsafe {
            crate::read_cursor::create_read_cursor(&[mb], &[], schema)
        };
        let mut ch = crate::read_cursor::CursorHandle::from_cursor(cursor);

        let mut dst = OwnedBatch::with_schema(schema, 4);
        let dst_ptr = &mut dst as *mut OwnedBatch as *mut libc::c_void;
        let ch_ptr = &mut ch as *mut crate::read_cursor::CursorHandle as *mut libc::c_void;

        assert_eq!(gnitz_batch_append_row_from_cursor(dst_ptr, ch_ptr, 1), 0);
        ch.cursor.advance();
        assert_eq!(gnitz_batch_append_row_from_cursor(dst_ptr, ch_ptr, 1), 0);

        assert_eq!(dst.count, 2);
        // Row 0: null bit set, zeroed data
        assert_eq!(dst.get_null_word(0) & 1, 1);
        assert!(dst.col_data[0][0..8].iter().all(|&b| b == 0));
        // Row 1: not null, value=42
        assert_eq!(dst.get_null_word(1) & 1, 0);
        assert_eq!(i64::from_le_bytes(dst.col_data[0][8..16].try_into().unwrap()), 42);
    }

    #[test]
    fn test_append_row_from_table_found() {
        use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};
        use crate::memtable::OwnedBatch;

        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 0, _pad: 0 };
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("ffi_table_found_test");

        let mut table = crate::table::Table::new(
            tdir.to_str().unwrap(), "test", schema, 600, 1 << 20, false,
        ).unwrap();

        // Ingest rows
        let n = 2usize;
        let pk_lo: Vec<u8> = [10u64, 20u64].iter().flat_map(|v| v.to_le_bytes()).collect();
        let pk_hi = vec![0u8; n * 8];
        let weight: Vec<u8> = [1i64, 1i64].iter().flat_map(|v| v.to_le_bytes()).collect();
        let null_bmp = vec![0u8; n * 8];
        let col0: Vec<u8> = [100i64, 200i64].iter().flat_map(|v| v.to_le_bytes()).collect();
        let blob = Vec::new();
        let ptrs: Vec<*const u8> = vec![
            pk_lo.as_ptr(), pk_hi.as_ptr(), weight.as_ptr(),
            null_bmp.as_ptr(), col0.as_ptr(), blob.as_ptr(),
        ];
        let sizes: Vec<u32> = vec![
            (n * 8) as u32, (n * 8) as u32, (n * 8) as u32,
            (n * 8) as u32, (n * 8) as u32, 0,
        ];
        table.ingest_batch_from_regions(&ptrs, &sizes, n as u32, 1).unwrap();

        // retract_pk populates found state
        let (w, found) = table.retract_pk(10, 0);
        assert!(found);
        assert_eq!(w, 1);

        let mut dst = OwnedBatch::with_schema(schema, 4);
        let dst_ptr = &mut dst as *mut OwnedBatch as *mut libc::c_void;
        let t_ptr = &table as *const crate::table::Table as *const libc::c_void;

        let rc = gnitz_batch_append_row_from_table_found(
            dst_ptr, t_ptr as *mut libc::c_void, 10, 0, -1,
        );
        assert_eq!(rc, 0);
        assert_eq!(dst.count, 1);
        assert_eq!(dst.get_pk_lo(0), 10);
        assert_eq!(i64::from_le_bytes(dst.weight[0..8].try_into().unwrap()), -1);
        assert_eq!(i64::from_le_bytes(dst.col_data[0][0..8].try_into().unwrap()), 100);

        table.close();
    }

    #[test]
    fn test_append_row_from_cursor_null_handle() {
        assert_eq!(gnitz_batch_append_row_from_cursor(ptr::null_mut(), ptr::null(), 1), -1);
    }

    #[test]
    fn test_append_row_from_table_found_null_handle() {
        assert_eq!(gnitz_batch_append_row_from_table_found(ptr::null_mut(), ptr::null_mut(), 0, 0, 1), -1);
    }

    #[test]
    fn test_append_row_from_ptable_found_null_handle() {
        assert_eq!(gnitz_batch_append_row_from_ptable_found(ptr::null_mut(), ptr::null_mut(), 0, 0, 1), -1);
    }

    // -----------------------------------------------------------------------
    // project_index
    // -----------------------------------------------------------------------

    /// Helper: build a source batch with schema [U64(pk=0), I32(col1), U64(col2)]
    /// and populate with test rows.
    fn make_source_batch_for_project() -> crate::memtable::OwnedBatch {
        use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};

        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: type_code::I32, size: 4, nullable: 1, _pad: 0 };
        columns[2] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        let schema = SchemaDescriptor { num_columns: 3, pk_index: 0, columns };

        let mut src = crate::memtable::OwnedBatch::with_schema(schema, 8);

        // Row 0: pk=100, col1=-5 (I32), col2=999, weight=1
        src.pk_lo.extend_from_slice(&100u64.to_le_bytes());
        src.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        src.weight.extend_from_slice(&1i64.to_le_bytes());
        src.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        src.col_data[0].extend_from_slice(&(-5i32).to_le_bytes());
        src.col_data[1].extend_from_slice(&999u64.to_le_bytes());
        src.count += 1;

        // Row 1: pk=200, col1=42 (I32), col2=888, weight=0 (should be skipped)
        src.pk_lo.extend_from_slice(&200u64.to_le_bytes());
        src.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        src.weight.extend_from_slice(&0i64.to_le_bytes());
        src.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        src.col_data[0].extend_from_slice(&42i32.to_le_bytes());
        src.col_data[1].extend_from_slice(&888u64.to_le_bytes());
        src.count += 1;

        // Row 2: pk=300, col1=NULL, col2=777, weight=1 (null → skipped)
        src.pk_lo.extend_from_slice(&300u64.to_le_bytes());
        src.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        src.weight.extend_from_slice(&1i64.to_le_bytes());
        // null bit for payload col 0 (col1 is schema col 1, payload col 0)
        src.null_bmp.extend_from_slice(&1u64.to_le_bytes());
        src.col_data[0].extend_from_slice(&0i32.to_le_bytes());
        src.col_data[1].extend_from_slice(&777u64.to_le_bytes());
        src.count += 1;

        // Row 3: pk=400, col1=10 (I32), col2=666, weight=-1
        src.pk_lo.extend_from_slice(&400u64.to_le_bytes());
        src.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        src.weight.extend_from_slice(&(-1i64).to_le_bytes());
        src.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        src.col_data[0].extend_from_slice(&10i32.to_le_bytes());
        src.col_data[1].extend_from_slice(&666u64.to_le_bytes());
        src.count += 1;

        src.sorted = false;
        src.consolidated = false;
        src
    }

    fn make_u64_index_schema() -> crate::compact::SchemaDescriptor {
        use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};
        let mut cols = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        cols[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        cols[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns: cols }
    }

    #[test]
    fn test_project_index_i32_to_u64() {
        let src = make_source_batch_for_project();
        let src_ptr = &src as *const crate::memtable::OwnedBatch as *const libc::c_void;
        let idx_schema = make_u64_index_schema();

        let result = gnitz_batch_project_index(src_ptr, 1, &idx_schema);
        assert!(!result.is_null());

        let out = unsafe { &*(result as *const crate::memtable::OwnedBatch) };
        // Row 1 (weight=0) and Row 2 (null) should be skipped → 2 output rows
        assert_eq!(out.count, 2);

        // Row 0: I32(-5) → sign-extend to i64 → u64 = 0xFFFFFFFFFFFFFFFF - 4
        let key0 = out.get_pk_lo(0);
        assert_eq!(key0, -5i64 as u64);
        assert_eq!(out.get_pk_hi(0), 0);
        assert_eq!(out.get_weight(0), 1);
        // Payload = source PK = 100
        let payload0 = u64::from_le_bytes(out.col_data[0][0..8].try_into().unwrap());
        assert_eq!(payload0, 100);

        // Row 3: I32(10) → sign-extend to i64 → u64 = 10
        let key1 = out.get_pk_lo(1);
        assert_eq!(key1, 10);
        assert_eq!(out.get_weight(1), -1);
        let payload1 = u64::from_le_bytes(out.col_data[0][8..16].try_into().unwrap());
        assert_eq!(payload1, 400);

        // Clean up
        unsafe { drop(Box::from_raw(result as *mut crate::memtable::OwnedBatch)); }
    }

    #[test]
    fn test_project_index_u128_source_pk() {
        use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};

        // Source schema: U128(pk=0), U64(col1)
        let mut src_cols = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        src_cols[0] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        src_cols[1] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        let src_schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns: src_cols };

        let mut src = crate::memtable::OwnedBatch::with_schema(src_schema, 4);
        // Row: pk=(10,20), col1=42, weight=1
        src.pk_lo.extend_from_slice(&10u64.to_le_bytes());
        src.pk_hi.extend_from_slice(&20u64.to_le_bytes());
        src.weight.extend_from_slice(&1i64.to_le_bytes());
        src.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        src.col_data[0].extend_from_slice(&42u64.to_le_bytes());
        src.count += 1;

        let src_ptr = &src as *const crate::memtable::OwnedBatch as *const libc::c_void;

        // Index schema: U64(pk=0), U128(payload) — source PK is U128
        let mut idx_cols = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        idx_cols[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        idx_cols[1] = SchemaColumn { type_code: type_code::U128, size: 16, nullable: 0, _pad: 0 };
        let idx_schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns: idx_cols };

        let result = gnitz_batch_project_index(src_ptr, 1, &idx_schema);
        assert!(!result.is_null());

        let out = unsafe { &*(result as *const crate::memtable::OwnedBatch) };
        assert_eq!(out.count, 1);
        // Index key = promoted U64(42) → key_lo=42, key_hi=0
        assert_eq!(out.get_pk_lo(0), 42);
        assert_eq!(out.get_pk_hi(0), 0);
        // Payload = source PK as U128: lo=10, hi=20
        let plo = u64::from_le_bytes(out.col_data[0][0..8].try_into().unwrap());
        let phi = u64::from_le_bytes(out.col_data[0][8..16].try_into().unwrap());
        assert_eq!(plo, 10);
        assert_eq!(phi, 20);

        unsafe { drop(Box::from_raw(result as *mut crate::memtable::OwnedBatch)); }
    }

    #[test]
    fn test_project_index_empty_batch() {
        let src_schema = make_u64_index_schema();
        let src = crate::memtable::OwnedBatch::with_schema(src_schema, 1);
        let src_ptr = &src as *const crate::memtable::OwnedBatch as *const libc::c_void;
        let idx_schema = make_u64_index_schema();

        let result = gnitz_batch_project_index(src_ptr, 1, &idx_schema);
        assert!(!result.is_null());
        let out = unsafe { &*(result as *const crate::memtable::OwnedBatch) };
        assert_eq!(out.count, 0);
        assert!(out.sorted);
        assert!(out.consolidated);

        unsafe { drop(Box::from_raw(result as *mut crate::memtable::OwnedBatch)); }
    }

    #[test]
    fn test_project_index_null_handles() {
        let idx_schema = make_u64_index_schema();

        assert!(gnitz_batch_project_index(ptr::null(), 0, &idx_schema).is_null());
        assert!(gnitz_batch_project_index(ptr::null(), 0, ptr::null()).is_null());
    }
}
