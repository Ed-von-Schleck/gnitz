//! Memory-mapped columnar shard reader.
//!
//! Shared between the compaction path (compact.rs) and query-time reads
//! (exposed via FFI for RPython `TableShardView`).

use std::ffi::CStr;
use std::ptr;

use xorf::Xor8;

use crate::layout::*;
use crate::util::{read_u64_le, read_i64_le};
use crate::xxh;
use crate::xor8;

// ---------------------------------------------------------------------------
// MappedShard
// ---------------------------------------------------------------------------

pub struct MappedShard {
    pub(crate) mmap_ptr: *mut u8,
    pub(crate) mmap_len: usize,
    pub(crate) count: usize,
    pub(crate) pk_lo_off: usize,
    pub(crate) pk_hi_off: usize,
    pub(crate) weight_off: usize,
    pub(crate) null_off: usize,
    /// Non-PK column regions: (offset, size) indexed by payload position.
    pub(crate) col_regions: Vec<(usize, usize)>,
    pub(crate) blob_off: usize,
    pub(crate) blob_len: usize,
    /// Column index mapping: logical col_idx -> payload col index.
    /// PK column maps to usize::MAX (sentinel).
    col_to_payload: Vec<usize>,
    /// XOR8 membership filter (loaded from embedded header data).
    xor8_filter: Option<Xor8>,
}

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

        // Validate header
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

        // Parse region directory
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

        // Validate checksums for all regions if requested
        if validate_checksums {
            for i in 0..num_regions {
                let (r_off, r_sz, r_cs) = regions[i];
                if r_sz > 0 && xxh::checksum(&data[r_off..r_off + r_sz]) != r_cs {
                    unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
                    return Err(-3);
                }
            }
        }

        // Build column-to-payload mapping
        let mut col_to_payload = Vec::with_capacity(num_cols);
        let mut col_regions = Vec::with_capacity(num_non_pk);
        let mut reg_idx = 4;
        for ci in 0..num_cols {
            if ci == pk_index {
                col_to_payload.push(usize::MAX);
            } else {
                col_to_payload.push(col_regions.len());
                col_regions.push((regions[reg_idx].0, regions[reg_idx].1));
                reg_idx += 1;
            }
        }
        let blob_off = regions[reg_idx].0;
        let blob_len = regions[reg_idx].1;

        // Load embedded XOR8 filter
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
            pk_lo_off: regions[0].0,
            pk_hi_off: regions[1].0,
            weight_off: regions[2].0,
            null_off: regions[3].0,
            col_regions,
            blob_off,
            blob_len,
            col_to_payload,
            xor8_filter,
        })
    }

    /// Returns a borrowed slice over the mmap'd data.
    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.mmap_ptr, self.mmap_len) }
    }

    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        let d = self.data();
        let lo = read_u64_le(d, self.pk_lo_off + row * 8);
        let hi = read_u64_le(d, self.pk_hi_off + row * 8);
        ((hi as u128) << 64) | (lo as u128)
    }

    #[inline]
    pub fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(self.data(), self.pk_lo_off + row * 8)
    }

    #[inline]
    pub fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(self.data(), self.pk_hi_off + row * 8)
    }

    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(self.data(), self.weight_off + row * 8)
    }

    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data(), self.null_off + row * 8)
    }

    /// Get a slice for a column value at the given row, indexed by payload position.
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        let (off, region_len) = self.col_regions[payload_col_idx];
        let start = off + row * col_size;
        let end = start + col_size;
        debug_assert!(end <= off + region_len, "get_col_ptr out of bounds");
        &self.data()[start..end]
    }

    /// Get a raw pointer to a column value, indexed by *logical* column index.
    /// Returns null for PK column or out-of-range.
    #[inline]
    pub fn col_ptr_by_logical(&self, row: usize, col_idx: usize, col_size: usize) -> *const u8 {
        if col_idx >= self.col_to_payload.len() {
            return ptr::null();
        }
        let payload_idx = self.col_to_payload[col_idx];
        if payload_idx == usize::MAX {
            // PK column — return pointer into pk_lo region
            let off = self.pk_lo_off + row * 8;
            if off + 8 > self.mmap_len {
                return ptr::null();
            }
            return unsafe { self.mmap_ptr.add(off) };
        }
        if payload_idx >= self.col_regions.len() {
            return ptr::null();
        }
        let (r_off, r_len) = self.col_regions[payload_idx];
        let off = r_off + row * col_size;
        if off + col_size > r_off + r_len {
            return ptr::null();
        }
        unsafe { self.mmap_ptr.add(off) }
    }

    #[inline]
    pub fn blob_slice(&self) -> &[u8] {
        &self.data()[self.blob_off..self.blob_off + self.blob_len]
    }

    #[inline]
    pub fn blob_ptr(&self) -> *const u8 {
        unsafe { self.mmap_ptr.add(self.blob_off) }
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
            None => true, // No filter — conservatively return true
        }
    }

    /// Binary search for the first row where PK >= (key_lo, key_hi).
    /// Returns `count` if no such row exists.
    pub fn find_lower_bound(&self, key_lo: u64, key_hi: u64) -> usize {
        let d = self.data();
        let mut lo = 0usize;
        let mut hi = self.count.wrapping_sub(1) as isize; // -1 if count==0
        let mut result = self.count;

        while (lo as isize) <= hi {
            let mid = lo + ((hi as usize - lo) >> 1);
            let mid_lo = read_u64_le(d, self.pk_lo_off + mid * 8);
            let mid_hi = read_u64_le(d, self.pk_hi_off + mid * 8);
            if pk_lt(mid_lo, mid_hi, key_lo, key_hi) {
                lo = mid + 1;
            } else {
                result = mid;
                hi = mid as isize - 1;
            }
        }
        result
    }

    /// Binary search for an exact PK match. Returns row index or -1.
    pub fn find_row_index(&self, key_lo: u64, key_hi: u64) -> i32 {
        let idx = self.find_lower_bound(key_lo, key_hi);
        if idx < self.count {
            let d = self.data();
            let found_lo = read_u64_le(d, self.pk_lo_off + idx * 8);
            let found_hi = read_u64_le(d, self.pk_hi_off + idx * 8);
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

impl crate::columnar::ColumnarSource for MappedShard {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        self.get_null_word(row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        self.get_col_ptr(row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        self.blob_slice()
    }
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

    /// Build a minimal shard file with N rows, 2 columns (PK=col0 u64, col1 i64).
    fn build_test_shard(dir: &std::path::Path, rows: &[(u64, i64)]) -> String {
        let path = dir.join("test.db");
        let num_cols = 2usize;
        let num_non_pk = num_cols - 1;
        let num_regions = 4 + num_non_pk + 1; // pk_lo, pk_hi, weight, null, col1, blob
        let count = rows.len();
        let dir_size = num_regions * DIR_ENTRY_SIZE;
        let dir_offset = HEADER_SIZE;
        let alignment = 64;

        // Build region data
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

        // Compute offsets
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
        columns[0] = SchemaColumn { type_code: 8, size: 8, nullable: 0, _pad: 0 }; // U64 PK
        columns[1] = SchemaColumn { type_code: 9, size: 8, nullable: 0, _pad: 0 }; // I64
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

        // Read PKs
        assert_eq!(shard.get_pk_lo(0), 1);
        assert_eq!(shard.get_pk_lo(9), 10);
        assert_eq!(shard.get_pk_hi(0), 0);

        // Read weights
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

        // Exact match
        assert_eq!(shard.find_row_index(10, 0), 4); // PK=10 is at index 4 (PKs: 2,4,6,8,10)
        assert_eq!(shard.find_row_index(200, 0), 99); // last row

        // Not found
        assert_eq!(shard.find_row_index(3, 0), -1); // odd, not present
        assert_eq!(shard.find_row_index(0, 0), -1); // below range
        assert_eq!(shard.find_row_index(201, 0), -1); // above range

        // Lower bound
        assert_eq!(shard.find_lower_bound(3, 0), 1); // first row >= 3 is PK=4 at idx 1
        assert_eq!(shard.find_lower_bound(1, 0), 0); // first row >= 1 is PK=2 at idx 0
        assert_eq!(shard.find_lower_bound(201, 0), 100); // returns count
    }

    #[test]
    fn col_ptr_by_logical() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 42i64), (2, 84)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        // col 0 is PK → returns pk_lo pointer
        let ptr = shard.col_ptr_by_logical(0, 0, 8);
        assert!(!ptr.is_null());
        let val = unsafe { *(ptr as *const u64) };
        assert_eq!(val, 1);

        // col 1 is payload → returns payload region pointer
        let ptr = shard.col_ptr_by_logical(0, 1, 8);
        assert!(!ptr.is_null());
        let val = unsafe { *(ptr as *const i64) };
        assert_eq!(val, 42);
    }

    #[test]
    fn checksum_validation() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 10i64)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        // Should succeed with validation enabled
        let shard = MappedShard::open(&cpath, &schema, true);
        assert!(shard.is_ok());

        // Corrupt a PK byte and verify checksum fails
        let path_str = cpath.to_str().unwrap();
        let mut data = std::fs::read(path_str).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        let pk_lo_off = read_u64_le(&data, dir_off) as usize;
        data[pk_lo_off] ^= 0xFF;
        std::fs::write(path_str, &data).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true);
        assert_eq!(shard.err(), Some(-3));
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
}
