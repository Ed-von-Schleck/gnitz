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
// Batch merge (memtable consolidation)
// ---------------------------------------------------------------------------

/// N-way merge + consolidation of in-memory batches.
///
/// Each batch is described by `regions_per_batch` consecutive regions in the
/// `in_region_ptrs`/`in_region_sizes` arrays (layout: pk_lo, pk_hi, weight,
/// null_bmp, payload_col_0..N-1, blob). `in_row_counts[i]` gives the row count
/// of batch `i`.
///
/// The caller pre-allocates output buffers (same region layout, one set) and
/// passes them via `out_region_ptrs`. On success, `out_region_sizes` is filled
/// with actual bytes written and `out_row_count` with the result row count.
///
/// Returns 0 on success, negative on error.
#[no_mangle]
pub extern "C" fn gnitz_merge_batches(
    in_region_ptrs: *const *const u8,
    in_region_sizes: *const u32,
    in_row_counts: *const u32,
    num_batches: u32,
    regions_per_batch: u32,
    schema_desc: *const u8,
    out_region_ptrs: *const *mut u8,
    out_region_sizes: *mut u32,
    out_row_count: *mut u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if in_region_ptrs.is_null()
            || in_region_sizes.is_null()
            || in_row_counts.is_null()
            || schema_desc.is_null()
            || out_region_ptrs.is_null()
            || out_region_sizes.is_null()
            || out_row_count.is_null()
        {
            return -1;
        }
        if num_batches == 0 || regions_per_batch < 5 {
            unsafe { *out_row_count = 0; }
            return 0;
        }

        let nb = num_batches as usize;
        let rpb = regions_per_batch as usize;
        let num_payload_cols = rpb - 5; // 4 fixed + blob
        let total_regions = nb * rpb;

        let schema = unsafe { &*(schema_desc as *const crate::compact::SchemaDescriptor) };
        let in_ptrs = unsafe { slice::from_raw_parts(in_region_ptrs, total_regions) };
        let in_sizes = unsafe { slice::from_raw_parts(in_region_sizes, total_regions) };
        let in_counts = unsafe { slice::from_raw_parts(in_row_counts, nb) };
        let out_ptrs = unsafe { slice::from_raw_parts(out_region_ptrs, rpb) };
        let out_sizes = unsafe { slice::from_raw_parts_mut(out_region_sizes, rpb) };

        // Compute totals for writer allocation
        let mut total_rows: usize = 0;
        let mut total_blob: usize = 0;
        for bi in 0..nb {
            total_rows += in_counts[bi] as usize;
            let blob_ri = bi * rpb + 4 + num_payload_cols;
            total_blob += in_sizes[blob_ri] as usize;
        }
        if total_blob == 0 {
            total_blob = 1;
        }

        let batches = unsafe {
            crate::merge::parse_batches_from_regions(
                in_ptrs, in_sizes, in_counts, nb, rpb, num_payload_cols,
            )
        };

        let mut writer = unsafe {
            crate::merge::create_writer_from_regions(
                out_ptrs, rpb, schema, total_rows, total_blob,
            )
        };

        crate::merge::merge_batches(&batches, schema, &mut writer);

        let count = writer.row_count();
        crate::merge::fill_output_sizes(&writer, schema, out_sizes);
        unsafe { *out_row_count = count as u32; }
        0
    });
    result.unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort and consolidate a single batch. Rows with the same (PK, payload)
/// have their weights summed; zero-weight rows are dropped.
/// RPython pre-allocates output buffers; Rust writes into them.
///
/// Returns 0 on success, -1 on invalid args, -99 on Rust panic.
#[no_mangle]
pub extern "C" fn gnitz_consolidate_batch(
    in_region_ptrs: *const *const u8,
    in_region_sizes: *const u32,
    row_count: u32,
    regions_per_batch: u32,
    schema_desc: *const crate::compact::SchemaDescriptor,
    out_region_ptrs: *const *mut u8,
    out_region_sizes: *mut u32,
    out_row_count: *mut u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        if schema_desc.is_null() || out_region_sizes.is_null() || out_row_count.is_null() {
            return -1;
        }

        let count = row_count as usize;
        if count == 0 {
            unsafe { *out_row_count = 0; }
            return 0;
        }

        if in_region_ptrs.is_null() || in_region_sizes.is_null() || out_region_ptrs.is_null() {
            return -1;
        }

        let rpb = regions_per_batch as usize;
        let schema = unsafe { &*schema_desc };
        let in_ptrs = unsafe { slice::from_raw_parts(in_region_ptrs, rpb) };
        let in_sizes = unsafe { slice::from_raw_parts(in_region_sizes, rpb) };
        let out_ptrs = unsafe { slice::from_raw_parts(out_region_ptrs, rpb) };
        let out_szs = unsafe { slice::from_raw_parts_mut(out_region_sizes, rpb) };

        let num_payload_cols = schema.num_columns as usize - 1;

        let batch = unsafe {
            crate::merge::parse_single_batch_from_regions(
                in_ptrs, in_sizes, count, num_payload_cols,
            )
        };

        let total_blob = batch.blob.len();
        let blob_cap = if total_blob > 0 { total_blob } else { 1 };

        let mut writer = unsafe {
            crate::merge::create_writer_from_regions(
                out_ptrs, rpb, schema, count, blob_cap,
            )
        };

        crate::merge::sort_and_consolidate(&batch, schema, &mut writer);

        crate::merge::fill_output_sizes(&writer, schema, out_szs);
        unsafe { *out_row_count = writer.row_count() as u32; }

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
        Box::into_raw(Box::new(cursor)) as *mut libc::c_void
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
        let cursor = unsafe { &mut *(handle as *mut crate::read_cursor::ReadCursor) };
        cursor.seek(key_lo, key_hi);
        unsafe {
            write_cursor_state(cursor, out_valid, out_key_lo, out_key_hi, out_weight, out_null_word);
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
        let cursor = unsafe { &mut *(handle as *mut crate::read_cursor::ReadCursor) };
        cursor.advance();
        unsafe {
            write_cursor_state(cursor, out_valid, out_key_lo, out_key_hi, out_weight, out_null_word);
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
        let cursor = unsafe { &*(handle as *const crate::read_cursor::ReadCursor) };
        cursor.col_ptr(col_idx as usize, col_size as usize)
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
        let cursor = unsafe { &*(handle as *const crate::read_cursor::ReadCursor) };
        cursor.blob_ptr()
    });
    result.unwrap_or(ptr::null())
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
            let _ = Box::from_raw(handle as *mut crate::read_cursor::ReadCursor);
        }
    });
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
}
