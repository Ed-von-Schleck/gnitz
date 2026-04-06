//! Memory-mapped columnar shard reader.
//!
//! Used by compaction (`compact.rs`) and query-time reads (`read_cursor.rs`).
//! Supports shard format v3 (24-byte dir entries, all raw) and v4 (32-byte
//! dir entries with per-region encoding byte).

use std::ffi::CStr;
use std::ptr;

use xorf::Xor8;

use crate::ipc_sys;
use crate::layout::*;
use crate::util::{read_u64_le, read_i64_le};
use crate::xxh;
use crate::xor8;

/// RAII guard for mmap'd memory.  Calls `munmap` on drop unless disarmed.
struct MmapGuard {
    ptr: *mut u8,
    len: usize,
}

impl MmapGuard {
    fn disarm(mut self) -> *mut u8 {
        let p = self.ptr;
        self.ptr = ptr::null_mut();
        p
    }
}

impl Drop for MmapGuard {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, self.len);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RegionView — self-describing region accessor
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) enum RegionView {
    Raw { offset: usize, size: usize },
    /// All elements identical. `value` holds the element bytes (for fast
    /// accessor reads via `from_le_bytes`). `offset` points into the mmap
    /// so that `col_ptr_by_logical` can return a naturally-aligned pointer.
    Constant { value: [u8; 16], offset: usize },
    TwoValue { value_a: i64, value_b: i64, bitvec_off: usize },
}

pub struct MappedShard {
    pub(crate) mmap_ptr: *mut u8,
    pub(crate) mmap_len: usize,
    pub(crate) count: usize,
    pub(crate) pk_lo: RegionView,
    pub(crate) pk_hi: RegionView,
    pub(crate) weight: RegionView,
    pub(crate) null_bmp: RegionView,
    /// Non-PK column regions indexed by payload position.
    pub(crate) col_regions: Vec<RegionView>,
    pub(crate) blob_off: usize,
    pub(crate) blob_len: usize,
    /// Column index mapping: logical col_idx -> payload col index.
    /// PK column maps to usize::MAX (sentinel).
    col_to_payload: Vec<usize>,
    /// XOR8 membership filter (loaded from embedded header data).
    xor8_filter: Option<Xor8>,
    /// True if weight region is Raw (may contain zero-weight ghosts).
    /// False for Constant/TwoValue (non-zero weights guaranteed).
    pub(crate) has_ghosts: bool,
}

impl MappedShard {
    pub fn open(
        path: &CStr,
        schema: &crate::schema::SchemaDescriptor,
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
        ipc_sys::madvise_hugepage(mmap_ptr as *mut u8, file_size);
        ipc_sys::madvise_sequential(mmap_ptr as *mut u8, file_size);

        let guard = MmapGuard {
            ptr: mmap_ptr as *mut u8,
            len: file_size,
        };
        let data = unsafe { std::slice::from_raw_parts(guard.ptr, file_size) };

        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            return Err(-2);
        }
        let version = read_u64_le(data, OFF_VERSION);
        if version != SHARD_VERSION && version != SHARD_VERSION_V3 {
            return Err(-2);
        }
        let dir_entry_size = if version >= 4 { DIR_ENTRY_SIZE } else { DIR_ENTRY_SIZE_V3 };

        let count = read_u64_le(data, OFF_ROW_COUNT) as usize;
        let dir_off = read_u64_le(data, OFF_DIR_OFFSET) as usize;

        let num_cols = schema.num_columns as usize;
        let pk_index = schema.pk_index as usize;
        let num_non_pk = num_cols - 1;
        let num_regions = 4 + num_non_pk + 1;

        // Parse directory entries
        struct DirEntry {
            offset: usize,
            size: usize,
            checksum: u64,
            encoding: u8,
        }

        let mut entries: Vec<DirEntry> = Vec::with_capacity(num_regions);
        for i in 0..num_regions {
            let entry_off = dir_off + i * dir_entry_size;
            if entry_off + dir_entry_size > file_size {
                return Err(-2);
            }
            let r_off = read_u64_le(data, entry_off) as usize;
            let r_sz = read_u64_le(data, entry_off + 8) as usize;
            let r_cs = read_u64_le(data, entry_off + 16);

            let encoding = if version >= 4 {
                let enc = data[entry_off + 24];
                // Validate reserved bytes [25..32] are all zero
                for b in &data[entry_off + 25..entry_off + 32] {
                    if *b != 0 {
                        return Err(-2);
                    }
                }
                // Validate known encoding
                if enc != ENCODING_RAW && enc != ENCODING_CONSTANT && enc != ENCODING_TWO_VALUE {
                    return Err(-2);
                }
                enc
            } else {
                ENCODING_RAW
            };

            if r_off.saturating_add(r_sz) > file_size {
                return Err(-2);
            }
            entries.push(DirEntry { offset: r_off, size: r_sz, checksum: r_cs, encoding });
        }

        // Validate checksums
        if validate_checksums {
            for e in &entries {
                if e.size > 0 && xxh::checksum(&data[e.offset..e.offset + e.size]) != e.checksum {
                    return Err(-3);
                }
            }
        }

        // Build RegionViews
        fn build_region_view(data: &[u8], e: &DirEntry) -> RegionView {
            match e.encoding {
                ENCODING_CONSTANT => {
                    let mut value = [0u8; 16];
                    if e.size > 0 {
                        let copy_len = e.size.min(16);
                        value[..copy_len].copy_from_slice(&data[e.offset..e.offset + copy_len]);
                    }
                    RegionView::Constant { value, offset: e.offset }
                }
                ENCODING_TWO_VALUE => {
                    let value_a = read_i64_le(data, e.offset);
                    let value_b = read_i64_le(data, e.offset + 8);
                    let bitvec_off = e.offset + 16;
                    RegionView::TwoValue { value_a, value_b, bitvec_off }
                }
                _ => {
                    RegionView::Raw { offset: e.offset, size: e.size }
                }
            }
        }

        let pk_lo = build_region_view(data, &entries[0]);
        let pk_hi = build_region_view(data, &entries[1]);
        let weight = build_region_view(data, &entries[2]);
        let null_bmp = build_region_view(data, &entries[3]);

        // has_ghosts: true if any row could have weight == 0.
        let has_ghosts = match &weight {
            RegionView::Raw { .. } => true,
            RegionView::Constant { value, .. } => {
                i64::from_le_bytes(value[..8].try_into().unwrap()) == 0
            }
            RegionView::TwoValue { value_a, value_b, .. } => {
                *value_a == 0 || *value_b == 0
            }
        };

        let mut col_to_payload = Vec::with_capacity(num_cols);
        let mut col_regions = Vec::with_capacity(num_non_pk);
        let mut reg_idx = 4;
        for ci in 0..num_cols {
            if ci == pk_index {
                col_to_payload.push(usize::MAX);
            } else {
                col_to_payload.push(col_regions.len());
                col_regions.push(build_region_view(data, &entries[reg_idx]));
                reg_idx += 1;
            }
        }
        let blob_off = entries[reg_idx].offset;
        let blob_len = entries[reg_idx].size;

        let xor8_off = read_u64_le(data, OFF_XOR8_OFFSET) as usize;
        let xor8_sz = read_u64_le(data, OFF_XOR8_SIZE) as usize;
        let xor8_filter = if xor8_off > 0 && xor8_sz >= 16 && xor8_off + xor8_sz <= file_size {
            xor8::deserialize(&data[xor8_off..xor8_off + xor8_sz])
        } else {
            None
        };

        let mmap_ptr = guard.disarm();
        Ok(MappedShard {
            mmap_ptr,
            mmap_len: file_size,
            count,
            pk_lo,
            pk_hi,
            weight,
            null_bmp,
            col_regions,
            blob_off,
            blob_len,
            col_to_payload,
            xor8_filter,
            has_ghosts,
        })
    }

    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.mmap_ptr, self.mmap_len) }
    }

    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        crate::util::make_pk(self.get_pk_lo(row), self.get_pk_hi(row))
    }

    #[inline]
    pub fn get_pk_lo(&self, row: usize) -> u64 {
        match &self.pk_lo {
            RegionView::Raw { offset, .. } => read_u64_le(self.data(), offset + row * 8),
            RegionView::Constant { value, .. } => u64::from_le_bytes(value[..8].try_into().unwrap()),
            RegionView::TwoValue { .. } => unreachable!(),
        }
    }

    #[inline]
    pub fn get_pk_hi(&self, row: usize) -> u64 {
        match &self.pk_hi {
            RegionView::Raw { offset, .. } => read_u64_le(self.data(), offset + row * 8),
            RegionView::Constant { value, .. } => u64::from_le_bytes(value[..8].try_into().unwrap()),
            RegionView::TwoValue { .. } => unreachable!(),
        }
    }

    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        match &self.weight {
            RegionView::Raw { offset, .. } => read_i64_le(self.data(), offset + row * 8),
            RegionView::Constant { value, .. } => i64::from_le_bytes(value[..8].try_into().unwrap()),
            RegionView::TwoValue { value_a, value_b, bitvec_off } => {
                let byte = self.data()[bitvec_off + row / 8];
                if (byte >> (row % 8)) & 1 == 0 { *value_a } else { *value_b }
            }
        }
    }

    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        match &self.null_bmp {
            RegionView::Raw { offset, .. } => read_u64_le(self.data(), offset + row * 8),
            RegionView::Constant { value, .. } => u64::from_le_bytes(value[..8].try_into().unwrap()),
            RegionView::TwoValue { .. } => unreachable!(),
        }
    }

    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        match &self.col_regions[payload_col_idx] {
            RegionView::Raw { offset, size } => {
                let start = offset + row * col_size;
                debug_assert!(start + col_size <= offset + size, "get_col_ptr out of bounds");
                &self.data()[start..start + col_size]
            }
            RegionView::Constant { value, .. } => &value[..col_size],
            RegionView::TwoValue { .. } => unreachable!(),
        }
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
            // PK column
            match &self.pk_lo {
                RegionView::Raw { offset, .. } => {
                    let off = offset + row * 8;
                    if off + 8 > self.mmap_len {
                        return ptr::null();
                    }
                    return unsafe { self.mmap_ptr.add(off) };
                }
                RegionView::Constant { offset, .. } => {
                    return unsafe { self.mmap_ptr.add(*offset) };
                }
                RegionView::TwoValue { .. } => unreachable!(),
            }
        }
        if payload_idx >= self.col_regions.len() {
            return ptr::null();
        }
        match &self.col_regions[payload_idx] {
            RegionView::Raw { offset, size } => {
                let off = offset + row * col_size;
                if off + col_size > offset + size {
                    return ptr::null();
                }
                unsafe { self.mmap_ptr.add(off) }
            }
            RegionView::Constant { offset, .. } => {
                unsafe { self.mmap_ptr.add(*offset) }
            }
            RegionView::TwoValue { .. } => unreachable!(),
        }
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
            None => true,
        }
    }

    /// Binary search for the first row where PK >= (key_lo, key_hi).
    /// Returns `count` if no such row exists.
    pub fn find_lower_bound(&self, key_lo: u64, key_hi: u64) -> usize {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_lo = self.get_pk_lo(mid);
            let mid_hi = self.get_pk_hi(mid);
            if pk_lt(mid_lo, mid_hi, key_lo, key_hi) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Binary search for an exact PK match. Returns row index or -1.
    pub fn find_row_index(&self, key_lo: u64, key_hi: u64) -> i32 {
        let idx = self.find_lower_bound(key_lo, key_hi);
        if idx < self.count {
            let found_lo = self.get_pk_lo(idx);
            let found_hi = self.get_pk_hi(idx);
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
    use crate::schema::{SchemaColumn, SchemaDescriptor};
    use crate::util::write_u64_le;

    /// Build a minimal v3 shard file (24-byte dir entries) for backward-compat testing.
    fn build_test_shard_v3(dir: &std::path::Path, rows: &[(u64, i64)]) -> String {
        let path = dir.join("test.db");
        let num_cols = 2usize;
        let num_non_pk = num_cols - 1;
        let num_regions = 4 + num_non_pk + 1;
        let count = rows.len();
        let dir_entry_size = DIR_ENTRY_SIZE_V3;
        let dir_size = num_regions * dir_entry_size;
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
        write_u64_le(&mut image, OFF_VERSION, SHARD_VERSION_V3);
        write_u64_le(&mut image, OFF_ROW_COUNT, count as u64);
        write_u64_le(&mut image, OFF_DIR_OFFSET, dir_offset as u64);

        for i in 0..num_regions {
            let r_off = offsets[i];
            let r_sz = regions_data[i].len();
            image[r_off..r_off + r_sz].copy_from_slice(regions_data[i]);
            let cs = if r_sz > 0 { crate::xxh::checksum(&image[r_off..r_off + r_sz]) } else { 0 };
            let d = dir_offset + i * dir_entry_size;
            write_u64_le(&mut image, d, r_off as u64);
            write_u64_le(&mut image, d + 8, r_sz as u64);
            write_u64_le(&mut image, d + 16, cs);
        }

        std::fs::write(&path, &image).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Build a v4 shard via build_shard_image (uses encoding detection).
    fn build_test_shard(dir: &std::path::Path, rows: &[(u64, i64)]) -> String {
        let path = dir.join("test.db");
        let count = rows.len() as u32;

        let mut pk_lo: Vec<u64> = Vec::new();
        let mut pk_hi: Vec<u64> = Vec::new();
        let mut weights: Vec<i64> = Vec::new();
        let mut null_bm: Vec<u64> = Vec::new();
        let mut col1_data: Vec<i64> = Vec::new();

        for &(pk, val) in rows {
            pk_lo.push(pk);
            pk_hi.push(0);
            weights.push(1);
            null_bm.push(0);
            col1_data.push(val);
        }
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_lo.as_ptr() as *const u8, pk_lo.len() * 8),
            (pk_hi.as_ptr() as *const u8, pk_hi.len() * 8),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (col1_data.as_ptr() as *const u8, col1_data.len() * 8),
            (blob.as_ptr(), blob.len()),
        ];

        let image = crate::shard_file::build_shard_image(0, count, &regions);
        std::fs::write(&path, &image).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Build a v4 shard with custom weights.
    fn build_test_shard_weights(
        dir: &std::path::Path,
        name: &str,
        pks: &[u64],
        wts: &[i64],
        vals: &[i64],
    ) -> String {
        let path = dir.join(name);
        let count = pks.len() as u32;
        let pk_hi: Vec<u64> = vec![0; pks.len()];
        let null_bm: Vec<u64> = vec![0; pks.len()];
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pks.as_ptr() as *const u8, pks.len() * 8),
            (pk_hi.as_ptr() as *const u8, pk_hi.len() * 8),
            (wts.as_ptr() as *const u8, wts.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), blob.len()),
        ];

        let image = crate::shard_file::build_shard_image(0, count, &regions);
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
        crate::util::raise_fd_limit_for_tests();
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
        assert!(!shard.has_ghosts);
    }

    #[test]
    fn binary_search() {
        crate::util::raise_fd_limit_for_tests();
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
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 42i64), (2, 84)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        let ptr = shard.col_ptr_by_logical(0, 0, 8);
        assert!(!ptr.is_null());
        let val = unsafe { *(ptr as *const u64) };
        assert_eq!(val, 1);

        let ptr = shard.col_ptr_by_logical(0, 1, 8);
        assert!(!ptr.is_null());
        let val = unsafe { *(ptr as *const i64) };
        assert_eq!(val, 42);
    }

    #[test]
    fn checksum_validation() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 10i64)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true);
        assert!(shard.is_ok());

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
        crate::util::raise_fd_limit_for_tests();
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

    // --- v4 encoding tests ---

    #[test]
    fn constant_weight_roundtrip() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 100;
        let pks: Vec<u64> = (1..=n).collect();
        let wts: Vec<i64> = vec![1; n as usize];
        let vals: Vec<i64> = (1..=n).map(|i| i as i64 * 10).collect();
        let path = build_test_shard_weights(dir.path(), "const_w.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path.clone()).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, n as usize);
        assert!(!shard.has_ghosts);
        for i in 0..n as usize {
            assert_eq!(shard.get_weight(i), 1);
        }

        // Verify v4 shard is smaller than a naive raw shard would be
        let file_len = std::fs::metadata(&path).unwrap().len() as usize;
        // Raw weight region alone would be n*8 = 800 bytes
        // Constant region is just 8 bytes
        assert!(file_len < HEADER_SIZE + n as usize * 8 * 6);
    }

    #[test]
    fn two_value_weight_roundtrip() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 64usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let vals: Vec<i64> = (0..n).map(|i| i as i64 * 10).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_w.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, n);
        // TwoValue weight -> has_ghosts = false (neither value is 0)
        assert!(!shard.has_ghosts);
        for i in 0..n {
            let expected = if i % 2 == 0 { 1i64 } else { -1i64 };
            assert_eq!(shard.get_weight(i), expected, "row {i}");
        }
    }

    #[test]
    fn three_value_weight_raw() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let pks: Vec<u64> = vec![1, 2, 3];
        let wts: Vec<i64> = vec![1, -1, 2];
        let vals: Vec<i64> = vec![10, 20, 30];
        let path = build_test_shard_weights(dir.path(), "raw_w.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert!(shard.has_ghosts); // Raw encoding
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_weight(1), -1);
        assert_eq!(shard.get_weight(2), 2);
    }

    #[test]
    fn constant_pk_hi_roundtrip() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 128;
        let rows: Vec<(u64, i64)> = (1..=n).map(|i| (i, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        for i in 0..n as usize {
            assert_eq!(shard.get_pk_hi(i), 0);
        }
        // Binary search still works
        assert_eq!(shard.find_row_index(64, 0), 63);
        assert_eq!(shard.find_row_index(1, 0), 0);
        assert_eq!(shard.find_row_index(128, 0), 127);
        assert_eq!(shard.find_lower_bound(65, 0), 64);
    }

    #[test]
    fn constant_null_bmp() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        for i in 0..10 {
            assert_eq!(shard.get_null_word(i), 0);
        }
    }

    #[test]
    fn constant_payload_column() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 10;
        let pks: Vec<u64> = (1..=n).collect();
        let wts: Vec<i64> = vec![1; n as usize];
        let vals: Vec<i64> = vec![42; n as usize]; // all same
        let path = build_test_shard_weights(dir.path(), "const_col.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        for i in 0..n as usize {
            let data = shard.get_col_ptr(i, 0, 8);
            let v = i64::from_le_bytes(data.try_into().unwrap());
            assert_eq!(v, 42);
            // col_ptr_by_logical for constant payload column
            let ptr = shard.col_ptr_by_logical(i, 1, 8);
            assert!(!ptr.is_null());
            let v2 = unsafe { *(ptr as *const i64) };
            assert_eq!(v2, 42);
        }
    }

    #[test]
    fn v3_backward_compat() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=5).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard_v3(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 5);
        assert!(shard.has_ghosts); // v3 -> all Raw
        for i in 0..5 {
            assert_eq!(shard.get_pk_lo(i), (i + 1) as u64);
            assert_eq!(shard.get_pk_hi(i), 0);
            assert_eq!(shard.get_weight(i), 1);
            assert_eq!(shard.get_null_word(i), 0);
        }
        assert_eq!(shard.find_row_index(3, 0), 2);
    }

    #[test]
    fn unknown_encoding_rejected() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(1, 10)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();

        // Corrupt: set encoding byte of first region to 0x10
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        data[dir_off + 24] = 0x10; // unknown encoding
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(MappedShard::open(&cpath, &schema, false).err(), Some(-2));
    }

    #[test]
    fn nonzero_reserved_rejected() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(1, 10)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();

        // Corrupt: set a reserved byte to non-zero
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        data[dir_off + 25] = 0xFF; // reserved byte
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(MappedShard::open(&cpath, &schema, false).err(), Some(-2));
    }

    #[test]
    fn single_row_shard() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(42, 999)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 1);
        assert!(!shard.has_ghosts);
        assert_eq!(shard.get_pk_lo(0), 42);
        assert_eq!(shard.get_pk_hi(0), 0);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_null_word(0), 0);
        assert_eq!(shard.find_row_index(42, 0), 0);
        assert_eq!(shard.find_row_index(1, 0), -1);
    }
}
