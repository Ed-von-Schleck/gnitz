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
        if block.is_null() || block_len <= 0 {
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
    fn xor8_build_empty() {
        let handle = gnitz_xor8_build(ptr::null(), ptr::null(), 0);
        assert!(handle.is_null());
    }
}
