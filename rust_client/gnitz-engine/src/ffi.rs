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

/// Packed instruction format (little-endian, per instruction):
///   [u8  opcode]
///   [u16 slot0] [u16 slot1] [u16 slot2] [u16 slot3] [u16 slot4] [u16 slot5]
///   [u32 inline_data_len]
///   [u8... inline_data]
///
/// Opcode encoding matches gnitz/core/opcodes.py:
///   0=HALT, 1=FILTER, 2=MAP, 3=NEGATE, 4=UNION, 5=JOIN_DT, 6=JOIN_DD,
///   7=INTEGRATE, 8=DELAY, 9=REDUCE, 10=DISTINCT, 11=SCAN_TRACE,
///   12=SEEK_TRACE, 15=CLEAR_DELTAS, 16=ANTI_JOIN_DT, 17=ANTI_JOIN_DD,
///   18=SEMI_JOIN_DT, 19=SEMI_JOIN_DD, 22=JOIN_DT_OUTER, 24=GATHER_REDUCE
///
/// Slot assignments per opcode (0xFFFF = unused):
///   HALT:           no slots
///   CLEAR_DELTAS:   no slots
///   DELAY:          s0=src, s1=dst
///   SCAN_TRACE:     s0=trace_reg, s1=out_reg; inline: [i32 chunk_limit]
///   SEEK_TRACE:     s0=trace_reg, s1=key_reg
///   FILTER:         s0=in_reg, s1=out_reg, s2=func_idx
///   MAP:            s0=in_reg, s1=out_reg, s2=func_idx, s3=out_schema_idx; inline: [i32 reindex_col]
///   NEGATE:         s0=in_reg, s1=out_reg
///   UNION:          s0=in_a, s1=in_b, s2=out_reg; inline: [u8 has_b]
///   DISTINCT:       s0=in_reg, s1=hist_reg, s2=out_reg
///   JOIN_DT:        s0=delta_reg, s1=trace_reg, s2=out_reg, s3=right_schema_idx
///   JOIN_DD:        s0=a_reg, s1=b_reg, s2=out_reg, s3=right_schema_idx
///   JOIN_DT_OUTER:  s0=delta_reg, s1=trace_reg, s2=out_reg, s3=right_schema_idx
///   ANTI_JOIN_DT:   s0=delta_reg, s1=trace_reg, s2=out_reg
///   ANTI_JOIN_DD:   s0=a_reg, s1=b_reg, s2=out_reg
///   SEMI_JOIN_DT:   s0=delta_reg, s1=trace_reg, s2=out_reg
///   SEMI_JOIN_DD:   s0=a_reg, s1=b_reg, s2=out_reg
///   INTEGRATE:      s0=in_reg; inline: [i32 table_idx, u8 has_gi, u8 has_avi,
///                     if has_gi: u16 gi_table_idx, u32 gi_col_idx, u8 gi_col_type_code,
///                     if has_avi: u16 avi_table_idx, u8 avi_for_max, u8 avi_agg_col_type_code,
///                       u32 avi_group_cols_offset, u16 avi_group_cols_count,
///                       u16 avi_input_schema_idx, u32 avi_agg_col_idx]
///   REDUCE:         s0=in_reg, s1=trace_out_reg, s2=out_reg, s3=output_schema_idx;
///                   inline: [i16 trace_in_reg, i16 fin_out_reg,
///                     u32 agg_descs_offset, u16 agg_descs_count,
///                     u32 group_cols_offset, u16 group_cols_count,
///                     i16 avi_trace_reg, u8 avi_for_max, u8 avi_agg_col_type_code,
///                     u32 avi_group_cols_offset, u16 avi_group_cols_count,
///                     i16 avi_input_schema_idx, u32 avi_agg_col_idx,
///                     i16 gi_trace_reg, u32 gi_col_idx, u8 gi_col_type_code,
///                     i16 finalize_func_idx, i16 finalize_schema_idx]
///   GATHER_REDUCE:  s0=in_reg, s1=trace_out_reg, s2=out_reg;
///                   inline: [u32 agg_descs_offset, u16 agg_descs_count]

fn read_u8(data: &[u8], pos: &mut usize) -> u8 {
    let v = data[*pos];
    *pos += 1;
    v
}
fn read_u16(data: &[u8], pos: &mut usize) -> u16 {
    let v = u16::from_le_bytes(data[*pos..*pos+2].try_into().unwrap());
    *pos += 2;
    v
}
fn read_i16(data: &[u8], pos: &mut usize) -> i16 {
    let v = i16::from_le_bytes(data[*pos..*pos+2].try_into().unwrap());
    *pos += 2;
    v
}
fn read_u32(data: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes(data[*pos..*pos+4].try_into().unwrap());
    *pos += 4;
    v
}
fn read_i32(data: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_le_bytes(data[*pos..*pos+4].try_into().unwrap());
    *pos += 4;
    v
}

fn deserialize_instructions(data: &[u8]) -> Vec<crate::vm::Instr> {
    let mut instrs = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let opcode = read_u8(data, &mut pos);
        let s0 = read_u16(data, &mut pos);
        let s1 = read_u16(data, &mut pos);
        let s2 = read_u16(data, &mut pos);
        let s3 = read_u16(data, &mut pos);
        let _s4 = read_u16(data, &mut pos);
        let _s5 = read_u16(data, &mut pos);
        let inline_len = read_u32(data, &mut pos) as usize;
        let inline_start = pos;
        let mut ipos = pos; // position within inline data

        let instr = match opcode {
            0 => crate::vm::Instr::Halt,
            15 => crate::vm::Instr::ClearDeltas,
            8 => crate::vm::Instr::Delay { src: s0, dst: s1 },
            11 => { // SCAN_TRACE
                let chunk_limit = read_i32(data, &mut ipos);
                crate::vm::Instr::ScanTrace { trace_reg: s0, out_reg: s1, chunk_limit }
            }
            12 => crate::vm::Instr::SeekTrace { trace_reg: s0, key_reg: s1 },
            1 => crate::vm::Instr::Filter { in_reg: s0, out_reg: s1, func_idx: s2 },
            2 => { // MAP
                let reindex_col = read_i32(data, &mut ipos);
                crate::vm::Instr::Map { in_reg: s0, out_reg: s1, func_idx: s2, out_schema_idx: s3, reindex_col }
            }
            3 => crate::vm::Instr::Negate { in_reg: s0, out_reg: s1 },
            4 => { // UNION
                let has_b = if inline_len > 0 { read_u8(data, &mut ipos) != 0 } else { s1 != 0xFFFF };
                crate::vm::Instr::Union { in_a: s0, in_b: s1, has_b, out_reg: s2 }
            }
            10 => {
                let hist_table_idx = if inline_len > 0 { read_i16(data, &mut ipos) } else { -1 };
                crate::vm::Instr::Distinct { in_reg: s0, hist_reg: s1, out_reg: s2, hist_table_idx }
            }
            5 => crate::vm::Instr::JoinDT { delta_reg: s0, trace_reg: s1, out_reg: s2, right_schema_idx: s3 },
            6 => crate::vm::Instr::JoinDD { a_reg: s0, b_reg: s1, out_reg: s2, right_schema_idx: s3 },
            22 => crate::vm::Instr::JoinDTOuter { delta_reg: s0, trace_reg: s1, out_reg: s2, right_schema_idx: s3 },
            16 => crate::vm::Instr::AntiJoinDT { delta_reg: s0, trace_reg: s1, out_reg: s2 },
            17 => crate::vm::Instr::AntiJoinDD { a_reg: s0, b_reg: s1, out_reg: s2 },
            18 => crate::vm::Instr::SemiJoinDT { delta_reg: s0, trace_reg: s1, out_reg: s2 },
            19 => crate::vm::Instr::SemiJoinDD { a_reg: s0, b_reg: s1, out_reg: s2 },
            7 => { // INTEGRATE
                let table_idx = read_i32(data, &mut ipos);
                let has_gi = read_u8(data, &mut ipos) != 0;
                let has_avi = read_u8(data, &mut ipos) != 0;
                let gi = if has_gi {
                    let gi_table_idx = read_u16(data, &mut ipos);
                    let gi_col_idx = read_u32(data, &mut ipos);
                    let gi_col_type_code = read_u8(data, &mut ipos);
                    Some(crate::vm::IntegrateGi { table_idx: gi_table_idx, col_idx: gi_col_idx, col_type_code: gi_col_type_code })
                } else { None };
                let avi = if has_avi {
                    let avi_table_idx = read_u16(data, &mut ipos);
                    let avi_for_max = read_u8(data, &mut ipos) != 0;
                    let avi_agg_col_type_code = read_u8(data, &mut ipos);
                    let avi_group_cols_offset = read_u32(data, &mut ipos);
                    let avi_group_cols_count = read_u16(data, &mut ipos);
                    let avi_input_schema_idx = read_u16(data, &mut ipos);
                    let avi_agg_col_idx = read_u32(data, &mut ipos);
                    Some(crate::vm::IntegrateAvi {
                        table_idx: avi_table_idx, for_max: avi_for_max,
                        agg_col_type_code: avi_agg_col_type_code,
                        group_cols_offset: avi_group_cols_offset,
                        group_cols_count: avi_group_cols_count,
                        input_schema_idx: avi_input_schema_idx,
                        agg_col_idx: avi_agg_col_idx,
                    })
                } else { None };
                crate::vm::Instr::Integrate { in_reg: s0, table_idx, gi, avi }
            }
            9 => { // REDUCE
                let trace_in_reg = read_i16(data, &mut ipos);
                let fin_out_reg = read_i16(data, &mut ipos);
                let agg_descs_offset = read_u32(data, &mut ipos);
                let agg_descs_count = read_u16(data, &mut ipos);
                let group_cols_offset = read_u32(data, &mut ipos);
                let group_cols_count = read_u16(data, &mut ipos);
                let avi_table_idx = read_i16(data, &mut ipos);
                let avi_for_max = read_u8(data, &mut ipos) != 0;
                let avi_agg_col_type_code = read_u8(data, &mut ipos);
                let avi_group_cols_offset = read_u32(data, &mut ipos);
                let avi_group_cols_count = read_u16(data, &mut ipos);
                let avi_input_schema_idx = read_i16(data, &mut ipos);
                let avi_agg_col_idx = read_u32(data, &mut ipos);
                let gi_table_idx = read_i16(data, &mut ipos);
                let gi_col_idx = read_u32(data, &mut ipos);
                let gi_col_type_code = read_u8(data, &mut ipos);
                let finalize_func_idx = read_i16(data, &mut ipos);
                let finalize_schema_idx = read_i16(data, &mut ipos);
                crate::vm::Instr::Reduce {
                    in_reg: s0, trace_in_reg, trace_out_reg: s1, out_reg: s2, fin_out_reg,
                    agg_descs_offset, agg_descs_count,
                    group_cols_offset, group_cols_count,
                    output_schema_idx: s3,
                    avi_table_idx, avi_for_max, avi_agg_col_type_code,
                    avi_group_cols_offset, avi_group_cols_count,
                    avi_input_schema_idx, avi_agg_col_idx,
                    gi_table_idx, gi_col_idx, gi_col_type_code,
                    finalize_func_idx, finalize_schema_idx,
                }
            }
            24 => { // GATHER_REDUCE
                let agg_descs_offset = read_u32(data, &mut ipos);
                let agg_descs_count = read_u16(data, &mut ipos);
                crate::vm::Instr::GatherReduce {
                    in_reg: s0, trace_out_reg: s1, out_reg: s2,
                    agg_descs_offset, agg_descs_count,
                }
            }
            _ => crate::vm::Instr::Halt, // unknown opcode → halt
        };

        pos = inline_start + inline_len;
        instrs.push(instr);
    }
    instrs
}

/// Persistent VM state: owns the Program + RegisterFile.
struct VmHandle {
    program: crate::vm::Program,
    regfile: crate::vm::RegisterFile,
}

/// Create a VM program from packed instruction data + resource handles.
///
/// Returns opaque handle or null on error.
#[no_mangle]
pub extern "C" fn gnitz_vm_program_create(
    // Packed instruction data
    instr_data: *const u8,
    instr_data_len: u32,
    // Register metadata: parallel arrays
    reg_schemas: *const crate::compact::SchemaDescriptor,
    reg_kinds: *const u8,     // 0=Delta, 1=Trace
    num_registers: u32,
    // Handle arrays indexed by instruction slots
    func_handles: *mut *mut libc::c_void,
    num_funcs: u32,
    table_handles: *mut *mut libc::c_void,
    num_tables: u32,
    expr_handles: *mut *mut libc::c_void,
    num_exprs: u32,
    // Auxiliary data
    agg_descs_buf: *const crate::ops::AggDescriptor,
    num_agg_descs: u32,
    group_cols_buf: *const u32,
    num_group_cols: u32,
    // Extra schemas
    extra_schemas: *const crate::compact::SchemaDescriptor,
    num_extra_schemas: u32,
) -> *mut libc::c_void {
    gnitz_debug!("vm_program_create: instr_len={} num_regs={} num_funcs={} num_tables={} num_exprs={}",
        instr_data_len, num_registers, num_funcs, num_tables, num_exprs);
    let result = panic::catch_unwind(|| {
        if instr_data.is_null() || instr_data_len == 0
            || reg_schemas.is_null() || reg_kinds.is_null() || num_registers == 0
        {
            gnitz_debug!("vm_program_create: null args, returning null");
            return ptr::null_mut();
        }

        let data = unsafe { slice::from_raw_parts(instr_data, instr_data_len as usize) };
        let instructions = deserialize_instructions(data);
        gnitz_debug!("vm_program_create: deserialized {} instructions", instructions.len());

        // Build register metadata
        let nr = num_registers as usize;
        let schemas_slice = unsafe { slice::from_raw_parts(reg_schemas, nr) };
        let kinds_slice = unsafe { slice::from_raw_parts(reg_kinds, nr) };

        let mut reg_meta = Vec::with_capacity(nr);
        for i in 0..nr {
            reg_meta.push(crate::vm::RegisterMeta {
                schema: schemas_slice[i],
                kind: if kinds_slice[i] == 0 { crate::vm::RegisterKind::Delta } else { crate::vm::RegisterKind::Trace },
            });
        }

        // Collect resource handles
        let funcs: Vec<*const crate::scalar_func::ScalarFuncKind> = if func_handles.is_null() || num_funcs == 0 {
            Vec::new()
        } else {
            let fh = unsafe { slice::from_raw_parts(func_handles, num_funcs as usize) };
            fh.iter().map(|&h| h as *const crate::scalar_func::ScalarFuncKind).collect()
        };

        let tables: Vec<*mut crate::table::Table> = if table_handles.is_null() || num_tables == 0 {
            Vec::new()
        } else {
            let th = unsafe { slice::from_raw_parts(table_handles, num_tables as usize) };
            th.iter().map(|&h| h as *mut crate::table::Table).collect()
        };

        let expr_progs: Vec<*const crate::expr::ExprProgram> = if expr_handles.is_null() || num_exprs == 0 {
            Vec::new()
        } else {
            let eh = unsafe { slice::from_raw_parts(expr_handles, num_exprs as usize) };
            eh.iter().map(|&h| h as *const crate::expr::ExprProgram).collect()
        };

        let agg_descs: Vec<crate::ops::AggDescriptor> = if agg_descs_buf.is_null() || num_agg_descs == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(agg_descs_buf, num_agg_descs as usize) }.to_vec()
        };

        let group_cols: Vec<u32> = if group_cols_buf.is_null() || num_group_cols == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(group_cols_buf, num_group_cols as usize) }.to_vec()
        };

        let schemas: Vec<crate::compact::SchemaDescriptor> = if extra_schemas.is_null() || num_extra_schemas == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(extra_schemas, num_extra_schemas as usize) }.to_vec()
        };

        let program = crate::vm::Program {
            instructions,
            reg_meta: Vec::new(), // stored separately in regfile
            funcs,
            tables,
            schemas,
            agg_descs,
            group_cols,
            expr_progs,
        };

        let regfile = crate::vm::RegisterFile::new(&reg_meta);

        gnitz_debug!("vm_program_create: built program, creating regfile with {} regs", reg_meta.len());
        let handle = VmHandle { program, regfile };
        gnitz_debug!("vm_program_create: success");
        Box::into_raw(Box::new(handle)) as *mut libc::c_void
    });
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            let msg = if let Some(s) = e.downcast_ref::<String>() {
                s.as_str()
            } else if let Some(s) = e.downcast_ref::<&str>() {
                s
            } else {
                "<unknown panic>"
            };
            gnitz_debug!("vm_program_create: PANIC: {}", msg);
            ptr::null_mut()
        }
    }
}

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

/// Extend system column buffers by n rows (pk_lo, pk_hi, weight, null_bmp)
/// and increment the count. The caller fills the new space via region_ptr + memcpy.
#[no_mangle]
pub extern "C" fn gnitz_batch_alloc_system(handle: *mut libc::c_void, n: u32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let extra = n as usize * 8;
        b.pk_lo.resize(b.pk_lo.len() + extra, 0);
        b.pk_hi.resize(b.pk_hi.len() + extra, 0);
        b.weight.resize(b.weight.len() + extra, 0);
        b.null_bmp.resize(b.null_bmp.len() + extra, 0);
        b.count += n as usize;
    });
}

/// Extend a payload column buffer by n_bytes, returning a pointer to the start of the new region.
#[no_mangle]
pub extern "C" fn gnitz_batch_col_extend(
    handle: *mut libc::c_void, payload_col: u32, n_bytes: u32,
) -> *mut u8 {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let pi = payload_col as usize;
        if pi >= b.col_data.len() { return ptr::null_mut(); }
        let start = b.col_data[pi].len();
        b.col_data[pi].resize(start + n_bytes as usize, 0);
        unsafe { b.col_data[pi].as_mut_ptr().add(start) }
    });
    result.unwrap_or(ptr::null_mut())
}

/// Extend the blob arena by n_bytes, returning the offset of the newly allocated region.
#[no_mangle]
pub extern "C" fn gnitz_batch_blob_extend(handle: *mut libc::c_void, n_bytes: u32) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        let offset = b.blob.len();
        b.blob.resize(offset + n_bytes as usize, 0);
        offset as u64
    });
    result.unwrap_or(0)
}

/// Return the current length of the blob arena.
#[no_mangle]
pub extern "C" fn gnitz_batch_blob_len(handle: *const libc::c_void) -> u64 {
    if handle.is_null() { return 0; }
    let result = panic::catch_unwind(|| {
        let b = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        b.blob.len() as u64
    });
    result.unwrap_or(0)
}

/// Set the row count (used by RPython after bulk appends via alloc_system + col_extend).
#[no_mangle]
pub extern "C" fn gnitz_batch_set_count(handle: *mut libc::c_void, count: u32) {
    if handle.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let b = unsafe { &mut *(handle as *mut crate::memtable::OwnedBatch) };
        b.count = count as usize;
    });
}

#[no_mangle]
pub extern "C" fn gnitz_batch_clone(handle: *const libc::c_void) -> *mut libc::c_void {
    if handle.is_null() { return ptr::null_mut(); }
    let result = panic::catch_unwind(|| {
        let src = unsafe { &*(handle as *const crate::memtable::OwnedBatch) };
        let mut cloned = crate::memtable::OwnedBatch {
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

}
