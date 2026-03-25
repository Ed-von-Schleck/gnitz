use std::ffi::CStr;
use std::panic;
use std::ptr;
use std::slice;

use libc::{c_void};

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
// Retract PK row — returns column pointers directly (no shard handle leak)
// ---------------------------------------------------------------------------

/// Find a PK and return per-column pointers for the found row.
/// The returned `out_cleanup` handle MUST be freed via `gnitz_retract_row_free`
/// after the caller is done reading column data.
#[no_mangle]
pub extern "C" fn gnitz_table_retract_pk_row(
    handle: *const c_void,
    key_lo: u64,
    key_hi: u64,
    out_col_ptrs: *mut *const u8,
    n_payload: u32,
    out_null_word: *mut u64,
    out_blob_ptr: *mut *const u8,
    out_blob_len: *mut u64,
    out_weight: *mut i64,
    out_cleanup: *mut *mut c_void,
) -> i32 {
    if handle.is_null() || out_col_ptrs.is_null() || out_cleanup.is_null() {
        return 0;
    }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = unsafe { &*(handle as *const crate::table::RustTable) };
        let (total_w, found) = table.retract_pk_find(key_lo, key_hi);
        if let Some((shard, row)) = found {
            let np = n_payload as usize;
            let pk_idx = table.schema.pk_index as usize;

            // Fill per-payload-column pointers for this row
            for pi in 0..np.min(shard.col_ptrs.len()) {
                let logical_ci = if pi < pk_idx { pi } else { pi + 1 };
                let elem_size = table.schema.columns[logical_ci].size as usize;
                unsafe {
                    *out_col_ptrs.add(pi) = shard.col_ptrs[pi].add(row * elem_size);
                }
            }
            if !out_null_word.is_null() {
                unsafe { *out_null_word = shard.get_null_word(row); }
            }
            if !out_blob_ptr.is_null() {
                unsafe { *out_blob_ptr = shard.blob_ptr(); }
            }
            if !out_blob_len.is_null() {
                unsafe { *out_blob_len = shard.blob_len() as u64; }
            }
            if !out_weight.is_null() {
                unsafe { *out_weight = total_w; }
            }
            // Keep shard alive — caller frees via gnitz_retract_row_free
            unsafe { *out_cleanup = Box::into_raw(Box::new(shard)) as *mut c_void; }
            1
        } else {
            if !out_weight.is_null() {
                unsafe { *out_weight = total_w; }
            }
            0
        }
    }));
    result.unwrap_or(0)
}

/// Free the cleanup handle returned by `gnitz_table_retract_pk_row`.
#[no_mangle]
pub extern "C" fn gnitz_retract_row_free(cleanup: *mut c_void) {
    if cleanup.is_null() { return; }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(cleanup as *mut crate::shard_reader::MappedShard) };
    });
}

// ---------------------------------------------------------------------------
// FFI tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_handle_safety() {
        // XXH3 null safety
        assert_eq!(gnitz_xxh3_checksum(ptr::null(), 0), 0);
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
    fn retract_pk_row_ffi() {
        let dir = tempfile::tempdir().unwrap();
        let schema = crate::compact::SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: {
                let mut cols = [crate::compact::SchemaColumn {
                    type_code: 8, size: 8, nullable: 0, _pad: 0,
                }; 64];
                // col 1: signed i64 (type_code 9, size 8)
                cols[1] = crate::compact::SchemaColumn {
                    type_code: 9, size: 8, nullable: 0, _pad: 0,
                };
                cols
            },
        };

        let cdir = std::ffi::CString::new(dir.path().to_str().unwrap()).unwrap();
        let cname = std::ffi::CString::new("retract_test").unwrap();
        let handle = gnitz_table_create_ephemeral(
            cdir.as_ptr(),
            cname.as_ptr(),
            &schema as *const _,
            1, 1 << 20,
        );
        assert!(!handle.is_null());

        // Ingest a row: PK=42, value=999
        let val: i64 = 999;
        let col_ptr = &val as *const i64 as *const u8;
        let col_ptrs_arr: [*const u8; 1] = [col_ptr];
        let rc = gnitz_table_ingest_one(
            handle, 42, 0, 1, 0,
            col_ptrs_arr.as_ptr(), 1,
            ptr::null(), 0,
        );
        assert_eq!(rc, 0);

        // Now retract via the new row API
        let mut out_col_ptrs: [*const u8; 1] = [ptr::null()];
        let mut null_word: u64 = 0;
        let mut blob_ptr: *const u8 = ptr::null();
        let mut blob_len: u64 = 0;
        let mut weight: i64 = 0;
        let mut cleanup: *mut c_void = ptr::null_mut();

        let found = gnitz_table_retract_pk_row(
            handle, 42, 0,
            out_col_ptrs.as_mut_ptr(),
            1, // n_payload
            &mut null_word,
            &mut blob_ptr,
            &mut blob_len,
            &mut weight,
            &mut cleanup,
        );
        assert_eq!(found, 1);
        assert_eq!(weight, 1);
        assert_eq!(null_word, 0);
        assert!(!out_col_ptrs[0].is_null());

        // Read the column value
        let val = unsafe { *(out_col_ptrs[0] as *const i64) };
        assert_eq!(val, 999);

        // Cleanup
        gnitz_retract_row_free(cleanup);

        // Not found case
        let found2 = gnitz_table_retract_pk_row(
            handle, 999, 0,
            out_col_ptrs.as_mut_ptr(),
            1,
            &mut null_word,
            &mut blob_ptr,
            &mut blob_len,
            &mut weight,
            &mut cleanup,
        );
        assert_eq!(found2, 0);

        gnitz_table_close(handle);
    }
}
