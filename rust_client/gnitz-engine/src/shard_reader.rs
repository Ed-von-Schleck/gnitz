//! Columnar shard reader — file-backed (mmap) or buffer-backed (raw pointers).
//!
//! `MappedShard::open` creates a file-backed shard from an on-disk shard file.
//! `MappedShard::from_buffers` creates a buffer-backed shard from raw column
//! pointers (e.g. ArenaZSetBatch buffers passed across FFI). Both variants
//! expose the same API; callers (compact.rs, FFI) don't need to distinguish.

use std::ffi::CStr;
use std::ptr;

use xorf::Xor8;

use crate::layout::*;
use crate::util::{read_u64_le, read_u64_at, read_i64_at};
use crate::xxh;
use crate::xor8;

pub struct MappedShard {
    /// Non-null for file-backed shards (Drop calls munmap). Null for buffer-backed.
    mmap_ptr: *mut u8,
    mmap_len: usize,
    pub(crate) count: usize,
    pub(crate) pk_lo_ptr: *const u8,
    pub(crate) pk_hi_ptr: *const u8,
    pub(crate) weight_ptr: *const u8,
    pub(crate) null_ptr: *const u8,
    pub(crate) col_ptrs: Vec<*const u8>,
    pub(crate) col_sizes: Vec<usize>,
    pub(crate) blob_ptr_: *const u8,
    blob_len: usize,
    col_to_payload: Vec<usize>,
    xor8_filter: Option<Xor8>,
}

// MappedShard stores raw pointers that are valid for its lifetime
// (mmap'd region or caller-owned buffers). Send is needed because
// compact.rs stores Vec<MappedShard> and the struct is not auto-Send
// due to raw pointers.
unsafe impl Send for MappedShard {}

impl MappedShard {
    pub fn open(
        path: &CStr,
        schema: &crate::compact::SchemaDescriptor,
        validate_checksums: bool,
    ) -> Result<Self, i32> {
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(-1);
        }
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } < 0 {
            unsafe { libc::close(fd); }
            return Err(-1);
        }
        let file_size = st.st_size as usize;
        if file_size < HEADER_SIZE {
            unsafe { libc::close(fd); }
            return Err(-2);
        }

        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                file_size,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        unsafe { libc::close(fd); }
        if mmap_ptr == libc::MAP_FAILED {
            return Err(-1);
        }
        let mmap_ptr = mmap_ptr as *mut u8;
        let data = unsafe { std::slice::from_raw_parts(mmap_ptr, file_size) };

        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
            return Err(-2);
        }
        if read_u64_le(data, OFF_VERSION) != SHARD_VERSION {
            unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
            return Err(-2);
        }

        let count = read_u64_le(data, OFF_ROW_COUNT) as usize;
        let dir_off = read_u64_le(data, OFF_DIR_OFFSET) as usize;

        let num_cols = schema.num_columns as usize;
        let pk_index = schema.pk_index as usize;
        let num_non_pk = num_cols - 1;
        let num_regions = 4 + num_non_pk + 1;

        let mut regions: Vec<(usize, usize, u64)> = Vec::with_capacity(num_regions);
        for i in 0..num_regions {
            let entry_off = dir_off + i * DIR_ENTRY_SIZE;
            if entry_off + DIR_ENTRY_SIZE > file_size {
                unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
                return Err(-2);
            }
            let r_off = read_u64_le(data, entry_off) as usize;
            let r_sz = read_u64_le(data, entry_off + 8) as usize;
            let r_cs = read_u64_le(data, entry_off + 16);
            if r_off.saturating_add(r_sz) > file_size {
                unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
                return Err(-2);
            }
            regions.push((r_off, r_sz, r_cs));
        }

        if validate_checksums {
            for i in 0..num_regions {
                let (r_off, r_sz, r_cs) = regions[i];
                if r_sz > 0 && xxh::checksum(&data[r_off..r_off + r_sz]) != r_cs {
                    unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
                    return Err(-3);
                }
            }
        }

        let mut col_to_payload = Vec::with_capacity(num_cols);
        let mut col_ptrs = Vec::with_capacity(num_non_pk);
        let mut col_sizes = Vec::with_capacity(num_non_pk);
        let mut reg_idx = 4;
        for ci in 0..num_cols {
            if ci == pk_index {
                col_to_payload.push(usize::MAX);
            } else {
                col_to_payload.push(col_ptrs.len());
                col_ptrs.push(unsafe { mmap_ptr.add(regions[reg_idx].0) as *const u8 });
                col_sizes.push(regions[reg_idx].1);
                reg_idx += 1;
            }
        }
        let blob_off = regions[reg_idx].0;
        let blob_len = regions[reg_idx].1;

        let xor8_off = read_u64_le(data, OFF_XOR8_OFFSET) as usize;
        let xor8_sz = read_u64_le(data, OFF_XOR8_SIZE) as usize;
        let xor8_filter = if xor8_off > 0 && xor8_sz >= 16 && xor8_off + xor8_sz <= file_size {
            xor8::deserialize(&data[xor8_off..xor8_off + xor8_sz])
        } else {
            None
        };

        Ok(MappedShard {
            mmap_ptr,
            mmap_len: file_size,
            count,
            pk_lo_ptr: unsafe { mmap_ptr.add(regions[0].0) as *const u8 },
            pk_hi_ptr: unsafe { mmap_ptr.add(regions[1].0) as *const u8 },
            weight_ptr: unsafe { mmap_ptr.add(regions[2].0) as *const u8 },
            null_ptr: unsafe { mmap_ptr.add(regions[3].0) as *const u8 },
            col_ptrs,
            col_sizes,
            blob_ptr_: unsafe { mmap_ptr.add(blob_off) as *const u8 },
            blob_len,
            col_to_payload,
            xor8_filter,
        })
    }

    /// Create a buffer-backed shard from raw column pointers.
    ///
    /// The caller owns the buffer memory and must keep it alive for the
    /// lifetime of this `MappedShard`. Drop is a no-op (no munmap).
    pub fn from_buffers(
        pk_lo: *const u8,
        pk_hi: *const u8,
        weight: *const u8,
        null_bm: *const u8,
        payload_col_ptrs: &[*const u8],
        payload_col_sizes: &[usize],
        blob_ptr: *const u8,
        blob_len: usize,
        count: usize,
        schema: &crate::compact::SchemaDescriptor,
    ) -> Self {
        let pk_index = schema.pk_index as usize;
        let num_cols = schema.num_columns as usize;
        let mut col_to_payload = Vec::with_capacity(num_cols);
        let mut col_ptrs = Vec::with_capacity(payload_col_ptrs.len());
        let mut col_sizes = Vec::with_capacity(payload_col_sizes.len());
        let mut pi = 0usize;
        for ci in 0..num_cols {
            if ci == pk_index {
                col_to_payload.push(usize::MAX);
            } else {
                col_to_payload.push(pi);
                col_ptrs.push(payload_col_ptrs[pi]);
                col_sizes.push(payload_col_sizes[pi]);
                pi += 1;
            }
        }

        MappedShard {
            mmap_ptr: ptr::null_mut(),
            mmap_len: 0,
            count,
            pk_lo_ptr: pk_lo,
            pk_hi_ptr: pk_hi,
            weight_ptr: weight,
            null_ptr: null_bm,
            col_ptrs,
            col_sizes,
            blob_ptr_: blob_ptr,
            blob_len,
            col_to_payload,
            xor8_filter: None,
        }
    }

    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        let lo = unsafe { read_u64_at(self.pk_lo_ptr, row * 8) };
        let hi = unsafe { read_u64_at(self.pk_hi_ptr, row * 8) };
        ((hi as u128) << 64) | (lo as u128)
    }

    #[inline]
    pub fn get_pk_lo(&self, row: usize) -> u64 {
        unsafe { read_u64_at(self.pk_lo_ptr, row * 8) }
    }

    #[inline]
    pub fn get_pk_hi(&self, row: usize) -> u64 {
        unsafe { read_u64_at(self.pk_hi_ptr, row * 8) }
    }

    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        unsafe { read_i64_at(self.weight_ptr, row * 8) }
    }

    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        unsafe { read_u64_at(self.null_ptr, row * 8) }
    }

    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        let base = self.col_ptrs[payload_col_idx];
        let start = row * col_size;
        debug_assert!(start + col_size <= self.col_sizes[payload_col_idx]);
        unsafe { std::slice::from_raw_parts(base.add(start), col_size) }
    }

    /// Raw pointer to a column value by *logical* column index.
    /// Returns null for out-of-range.
    #[inline]
    pub fn col_ptr_by_logical(&self, row: usize, col_idx: usize, col_size: usize) -> *const u8 {
        if col_idx >= self.col_to_payload.len() {
            return ptr::null();
        }
        let payload_idx = self.col_to_payload[col_idx];
        if payload_idx == usize::MAX {
            if row >= self.count {
                return ptr::null();
            }
            return unsafe { self.pk_lo_ptr.add(row * 8) };
        }
        if payload_idx >= self.col_ptrs.len() {
            return ptr::null();
        }
        let off = row * col_size;
        if off + col_size > self.col_sizes[payload_idx] {
            return ptr::null();
        }
        unsafe { self.col_ptrs[payload_idx].add(off) }
    }

    #[inline]
    pub fn blob_slice(&self) -> &[u8] {
        if self.blob_len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.blob_ptr_, self.blob_len) }
    }

    #[inline]
    pub fn blob_ptr(&self) -> *const u8 {
        self.blob_ptr_
    }

    #[inline]
    pub fn blob_len(&self) -> usize {
        self.blob_len
    }

    pub fn has_xor8(&self) -> bool {
        self.xor8_filter.is_some()
    }

    pub fn xor8_may_contain(&self, key_lo: u64, key_hi: u64) -> bool {
        match &self.xor8_filter {
            Some(filter) => xor8::may_contain(filter, key_lo, key_hi),
            None => true,
        }
    }

    pub fn find_lower_bound(&self, key_lo: u64, key_hi: u64) -> usize {
        if self.count == 0 {
            return 0;
        }
        let mut lo = 0usize;
        let mut hi = (self.count - 1) as isize;
        let mut result = self.count;

        while (lo as isize) <= hi {
            let mid = lo + ((hi as usize - lo) >> 1);
            let mid_lo = unsafe { read_u64_at(self.pk_lo_ptr, mid * 8) };
            let mid_hi = unsafe { read_u64_at(self.pk_hi_ptr, mid * 8) };
            if pk_lt(mid_lo, mid_hi, key_lo, key_hi) {
                lo = mid + 1;
            } else {
                result = mid;
                hi = mid as isize - 1;
            }
        }
        result
    }

    pub fn find_row_index(&self, key_lo: u64, key_hi: u64) -> i32 {
        let idx = self.find_lower_bound(key_lo, key_hi);
        if idx < self.count {
            let found_lo = unsafe { read_u64_at(self.pk_lo_ptr, idx * 8) };
            let found_hi = unsafe { read_u64_at(self.pk_hi_ptr, idx * 8) };
            if found_lo == key_lo && found_hi == key_hi {
                return idx as i32;
            }
        }
        -1
    }
}

#[inline]
fn pk_lt(a_lo: u64, a_hi: u64, b_lo: u64, b_hi: u64) -> bool {
    (a_hi, a_lo) < (b_hi, b_lo)
}

impl Drop for MappedShard {
    fn drop(&mut self) {
        if !self.mmap_ptr.is_null() {
            unsafe {
                libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
            }
            self.mmap_ptr = ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor};
    use crate::util::write_u64_le;

    fn build_test_shard(dir: &std::path::Path, rows: &[(u64, i64)]) -> String {
        let path = dir.join("test.db");
        let num_cols = 2usize;
        let num_non_pk = num_cols - 1;
        let num_regions = 4 + num_non_pk + 1;
        let count = rows.len();
        let dir_size = num_regions * DIR_ENTRY_SIZE;
        let dir_offset = HEADER_SIZE;
        let alignment = 64;

        let mut pk_lo = Vec::new();
        let mut pk_hi = Vec::new();
        let mut weights = Vec::new();
        let mut null_bm = Vec::new();
        let mut col1_data = Vec::new();

        for &(pk, val) in rows {
            pk_lo.extend_from_slice(&pk.to_le_bytes());
            pk_hi.extend_from_slice(&0u64.to_le_bytes());
            weights.extend_from_slice(&1i64.to_le_bytes());
            null_bm.extend_from_slice(&0u64.to_le_bytes());
            col1_data.extend_from_slice(&val.to_le_bytes());
        }
        let blob: Vec<u8> = Vec::new();

        let regions_data: Vec<&[u8]> = vec![&pk_lo, &pk_hi, &weights, &null_bm, &col1_data, &blob];

        fn align(v: usize, a: usize) -> usize { (v + a - 1) & !(a - 1) }
        let mut pos = align(dir_offset + dir_size, alignment);
        let mut offsets = Vec::new();
        for r in &regions_data {
            offsets.push(pos);
            pos = align(pos + r.len(), alignment);
        }
        let total = pos;

        let mut image = vec![0u8; total];
        write_u64_le(&mut image, OFF_MAGIC, SHARD_MAGIC);
        write_u64_le(&mut image, OFF_VERSION, SHARD_VERSION);
        write_u64_le(&mut image, OFF_ROW_COUNT, count as u64);
        write_u64_le(&mut image, OFF_DIR_OFFSET, dir_offset as u64);

        for i in 0..num_regions {
            let r_off = offsets[i];
            let r_sz = regions_data[i].len();
            image[r_off..r_off + r_sz].copy_from_slice(regions_data[i]);
            let cs = if r_sz > 0 { crate::xxh::checksum(&image[r_off..r_off + r_sz]) } else { 0 };
            let d = dir_offset + i * DIR_ENTRY_SIZE;
            write_u64_le(&mut image, d, r_off as u64);
            write_u64_le(&mut image, d + 8, r_sz as u64);
            write_u64_le(&mut image, d + 16, cs);
        }

        std::fs::write(&path, &image).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn test_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        columns[0] = SchemaColumn { type_code: 8, size: 8, nullable: 0, _pad: 0 };
        columns[1] = SchemaColumn { type_code: 9, size: 8, nullable: 0, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    #[test]
    fn open_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert_eq!(shard.count, 10);
        assert_eq!(shard.get_pk_lo(0), 1);
        assert_eq!(shard.get_pk_lo(9), 10);
        assert_eq!(shard.get_pk_hi(0), 0);
        assert_eq!(shard.get_weight(0), 1);
    }

    #[test]
    fn binary_search() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=100).map(|i| (i * 2, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert_eq!(shard.find_row_index(10, 0), 4);
        assert_eq!(shard.find_row_index(200, 0), 99);
        assert_eq!(shard.find_row_index(3, 0), -1);
        assert_eq!(shard.find_row_index(0, 0), -1);
        assert_eq!(shard.find_row_index(201, 0), -1);
        assert_eq!(shard.find_lower_bound(3, 0), 1);
        assert_eq!(shard.find_lower_bound(1, 0), 0);
        assert_eq!(shard.find_lower_bound(201, 0), 100);
    }

    #[test]
    fn col_ptr_by_logical() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 42i64), (2, 84)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        let ptr = shard.col_ptr_by_logical(0, 0, 8);
        assert!(!ptr.is_null());
        assert_eq!(unsafe { *(ptr as *const u64) }, 1);

        let ptr = shard.col_ptr_by_logical(0, 1, 8);
        assert!(!ptr.is_null());
        assert_eq!(unsafe { *(ptr as *const i64) }, 42);
    }

    #[test]
    fn checksum_validation() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 10i64)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        assert!(MappedShard::open(&cpath, &schema, true).is_ok());

        let path_str = cpath.to_str().unwrap();
        let mut data = std::fs::read(path_str).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        let pk_lo_off = read_u64_le(&data, dir_off) as usize;
        data[pk_lo_off] ^= 0xFF;
        std::fs::write(path_str, &data).unwrap();

        assert_eq!(MappedShard::open(&cpath, &schema, true).err(), Some(-3));
    }

    #[test]
    fn empty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert_eq!(shard.count, 0);
        assert_eq!(shard.find_row_index(1, 0), -1);
        assert_eq!(shard.find_lower_bound(1, 0), 0);
    }

    // -----------------------------------------------------------------------
    // from_buffers tests
    // -----------------------------------------------------------------------

    #[test]
    fn from_buffers_roundtrip() {
        let schema = test_schema();
        let pk_lo: Vec<u64> = vec![10, 20, 30];
        let pk_hi: Vec<u64> = vec![0, 0, 0];
        let weights: Vec<i64> = vec![1, 2, 3];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let col1: Vec<i64> = vec![100, 200, 300];

        let shard = MappedShard::from_buffers(
            pk_lo.as_ptr() as *const u8,
            pk_hi.as_ptr() as *const u8,
            weights.as_ptr() as *const u8,
            nulls.as_ptr() as *const u8,
            &[col1.as_ptr() as *const u8],
            &[col1.len() * 8],
            ptr::null(),
            0,
            3,
            &schema,
        );

        assert_eq!(shard.count, 3);
        assert_eq!(shard.get_pk_lo(0), 10);
        assert_eq!(shard.get_pk_lo(2), 30);
        assert_eq!(shard.get_pk_hi(0), 0);
        assert_eq!(shard.get_weight(1), 2);
        assert_eq!(shard.get_null_word(0), 0);
        assert!(!shard.has_xor8());
        assert!(shard.xor8_may_contain(10, 0)); // conservative true

        let col_data = shard.get_col_ptr(1, 0, 8);
        assert_eq!(i64::from_le_bytes(col_data.try_into().unwrap()), 200);

        let ptr = shard.col_ptr_by_logical(2, 1, 8);
        assert!(!ptr.is_null());
        assert_eq!(unsafe { *(ptr as *const i64) }, 300);
    }

    #[test]
    fn from_buffers_binary_search() {
        let schema = test_schema();
        let pk_lo: Vec<u64> = vec![2, 4, 6, 8, 10];
        let pk_hi: Vec<u64> = vec![0; 5];
        let weights: Vec<i64> = vec![1; 5];
        let nulls: Vec<u64> = vec![0; 5];
        let col1: Vec<i64> = vec![20, 40, 60, 80, 100];

        let shard = MappedShard::from_buffers(
            pk_lo.as_ptr() as *const u8,
            pk_hi.as_ptr() as *const u8,
            weights.as_ptr() as *const u8,
            nulls.as_ptr() as *const u8,
            &[col1.as_ptr() as *const u8],
            &[col1.len() * 8],
            ptr::null(),
            0,
            5,
            &schema,
        );

        assert_eq!(shard.find_row_index(6, 0), 2);
        assert_eq!(shard.find_row_index(3, 0), -1);
        assert_eq!(shard.find_lower_bound(5, 0), 2);
        assert_eq!(shard.find_lower_bound(1, 0), 0);
        assert_eq!(shard.find_lower_bound(11, 0), 5);
    }

    #[test]
    fn from_buffers_empty() {
        let schema = test_schema();
        // Schema has 1 payload column — pass a null pointer with size 0.
        let shard = MappedShard::from_buffers(
            ptr::null(), ptr::null(), ptr::null(), ptr::null(),
            &[ptr::null()], &[0],
            ptr::null(), 0,
            0,
            &schema,
        );
        assert_eq!(shard.count, 0);
        assert_eq!(shard.find_row_index(1, 0), -1);
        assert_eq!(shard.find_lower_bound(1, 0), 0);
    }

    #[test]
    fn from_buffers_drop_safety() {
        let schema = test_schema();
        let pk_lo: Vec<u64> = vec![1];
        let pk_hi: Vec<u64> = vec![0];
        let w: Vec<i64> = vec![1];
        let n: Vec<u64> = vec![0];
        let c: Vec<i64> = vec![42];

        {
            let _shard = MappedShard::from_buffers(
                pk_lo.as_ptr() as *const u8,
                pk_hi.as_ptr() as *const u8,
                w.as_ptr() as *const u8,
                n.as_ptr() as *const u8,
                &[c.as_ptr() as *const u8],
                &[8],
                ptr::null(), 0,
                1,
                &schema,
            );
        } // drop — should not crash (no munmap)
    }
}
