//! Memory-mapped columnar shard reader.
//!
//! Used by compaction (`compact.rs`) and query-time reads (`read_cursor.rs`).
//! Supports shard format v3 (24-byte dir entries, all raw) and v4 (32-byte
//! dir entries with per-region encoding byte).

use std::ffi::CStr;
use std::ptr;

use xorf::Xor8;

use crate::sys;
use crate::layout::*;
use crate::util::{read_u64_le, read_i64_le};
use crate::xxh;
use super::error::StorageError;
use super::xor8;

/// RAII handle for an mmap'd file region.
///
/// Owning the mapping in a single `Drop`-bearing struct means the unmap path
/// lives in exactly one place — neither `MappedShard::open`'s error returns
/// nor `MappedShard::Drop` need to repeat the `munmap` call.
struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    /// Open `path` read-only, mmap the whole file, and apply huge-page +
    /// sequential madvise hints.  Returns `Truncated` for a file shorter than
    /// `HEADER_SIZE` (the minimum a shard header occupies).
    fn open(path: &CStr) -> Result<Self, StorageError> {
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(StorageError::Io);
        }
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } < 0 {
            unsafe { libc::close(fd); }
            return Err(StorageError::Io);
        }
        let len = st.st_size as usize;
        if len < HEADER_SIZE {
            unsafe { libc::close(fd); }
            return Err(StorageError::Truncated);
        }
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        unsafe { libc::close(fd); }
        if raw == libc::MAP_FAILED {
            return Err(StorageError::Io);
        }
        let ptr = raw as *mut u8;
        sys::madvise_hugepage(ptr, len);
        sys::madvise_sequential(ptr, len);
        Ok(Mmap { ptr, len })
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
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
    /// Owning RAII handle for the mmap.  Dropped last, so `as_slice()` /
    /// raw pointers derived from it remain valid for the entire lifetime
    /// of the `MappedShard`.
    mmap: Mmap,
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
    ) -> Result<Self, StorageError> {
        // `Mmap`'s Drop unmaps the file on any early `?` return below — no
        // manual cleanup needed in the validation path.
        let mmap = Mmap::open(path)?;
        let file_size = mmap.len;
        let data = mmap.as_slice();

        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            return Err(StorageError::InvalidMagic);
        }
        let version = read_u64_le(data, OFF_VERSION);
        if version != SHARD_VERSION && version != SHARD_VERSION_V3 {
            return Err(StorageError::InvalidVersion);
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
                return Err(StorageError::InvalidShard);
            }
            let r_off = read_u64_le(data, entry_off) as usize;
            let r_sz = read_u64_le(data, entry_off + 8) as usize;
            let r_cs = read_u64_le(data, entry_off + 16);

            let encoding = if version >= 4 {
                let enc = data[entry_off + 24];
                // Validate reserved bytes [25..32] are all zero
                for b in &data[entry_off + 25..entry_off + 32] {
                    if *b != 0 {
                        return Err(StorageError::InvalidShard);
                    }
                }
                // Validate known encoding
                if enc != ENCODING_RAW && enc != ENCODING_CONSTANT && enc != ENCODING_TWO_VALUE {
                    return Err(StorageError::InvalidShard);
                }
                enc
            } else {
                ENCODING_RAW
            };

            if r_off.saturating_add(r_sz) > file_size {
                return Err(StorageError::InvalidShard);
            }
            entries.push(DirEntry { offset: r_off, size: r_sz, checksum: r_cs, encoding });
        }

        // Validate checksums
        if validate_checksums {
            for e in &entries {
                if e.size > 0 && xxh::checksum(&data[e.offset..e.offset + e.size]) != e.checksum {
                    return Err(StorageError::ChecksumMismatch);
                }
            }
        }

        // Build RegionViews
        let build_region_view = |e: &DirEntry| -> Result<RegionView, StorageError> {
            match e.encoding {
                ENCODING_CONSTANT => {
                    let mut value = [0u8; 16];
                    if e.size > 0 {
                        let copy_len = e.size.min(16);
                        value[..copy_len].copy_from_slice(&data[e.offset..e.offset + copy_len]);
                    }
                    Ok(RegionView::Constant { value, offset: e.offset })
                }
                ENCODING_TWO_VALUE => {
                    let expected_bitvec = count.div_ceil(8);
                    if e.size < 16 + expected_bitvec {
                        return Err(StorageError::InvalidShard);
                    }
                    let value_a = read_i64_le(data, e.offset);
                    let value_b = read_i64_le(data, e.offset + 8);
                    let bitvec_off = e.offset + 16;
                    Ok(RegionView::TwoValue { value_a, value_b, bitvec_off })
                }
                _ => {
                    Ok(RegionView::Raw { offset: e.offset, size: e.size })
                }
            }
        };

        let pk_lo = build_region_view(&entries[0])?;
        let pk_hi = build_region_view(&entries[1])?;
        let weight = build_region_view(&entries[2])?;
        let null_bmp = build_region_view(&entries[3])?;

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
                col_regions.push(build_region_view(&entries[reg_idx])?);
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

        Ok(MappedShard {
            mmap,
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
        self.mmap.as_slice()
    }

    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        crate::util::make_pk(self.get_pk_lo(row), self.get_pk_hi(row))
    }

    #[inline]
    fn get_pk_lo(&self, row: usize) -> u64 {
        match &self.pk_lo {
            RegionView::Raw { offset, .. } => read_u64_le(self.data(), offset + row * 8),
            RegionView::Constant { value, .. } => u64::from_le_bytes(value[..8].try_into().unwrap()),
            RegionView::TwoValue { .. } => unreachable!(),
        }
    }

    #[inline]
    fn get_pk_hi(&self, row: usize) -> u64 {
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
                assert!(
                    start + col_size <= offset + size,
                    "get_col_ptr out of bounds: row={} payload_col={} col_size={} region_end={}",
                    row, payload_col_idx, col_size, offset + size,
                );
                &self.data()[start..start + col_size]
            }
            RegionView::Constant { value, .. } => &value[..col_size],
            RegionView::TwoValue { .. } => unreachable!(),
        }
    }

    /// Get a raw pointer to a column value, indexed by *logical* column index.
    /// For the PK column, returns a pointer to the pk_lo region (always 8 bytes).
    /// Callers requiring the full u128 PK must use get_pk_lo / get_pk_hi directly.
    /// Returns null for out-of-range column indices.
    #[inline]
    pub fn col_ptr_by_logical(&self, row: usize, col_idx: usize, col_size: usize) -> *const u8 {
        if col_idx >= self.col_to_payload.len() {
            return ptr::null();
        }
        let payload_idx = self.col_to_payload[col_idx];
        let base = self.mmap.ptr;
        if payload_idx == usize::MAX {
            // PK column
            match &self.pk_lo {
                RegionView::Raw { offset, .. } => {
                    let off = offset + row * 8;
                    if off + 8 > self.mmap.len {
                        return ptr::null();
                    }
                    return unsafe { base.add(off) };
                }
                RegionView::Constant { offset, .. } => {
                    return unsafe { base.add(*offset) };
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
                unsafe { base.add(off) }
            }
            RegionView::Constant { offset, .. } => {
                unsafe { base.add(*offset) }
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
        unsafe { self.mmap.ptr.add(self.blob_off) }
    }

    pub fn has_xor8(&self) -> bool {
        self.xor8_filter.is_some()
    }

    pub fn xor8_may_contain(&self, key: u128) -> bool {
        match &self.xor8_filter {
            Some(filter) => xor8::may_contain(filter, key),
            None => true,
        }
    }

    /// Binary search for the first row where PK >= key.
    /// Returns `count` if no such row exists.
    pub fn find_lower_bound(&self, key: u128) -> usize {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.get_pk(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Binary search for an exact PK match. Returns the row index, or `None`
    /// if `key` is not present.
    pub fn find_row_index(&self, key: u128) -> Option<usize> {
        let idx = self.find_lower_bound(key);
        if idx < self.count && self.get_pk(idx) == key {
            Some(idx)
        } else {
            None
        }
    }

    /// Bulk-copy a contiguous slice of rows into an Batch.
    /// Bypasses per-row cursor overhead entirely — one memcpy per column.
    #[allow(clippy::uninit_vec)]
    pub(crate) fn slice_to_owned_batch(
        &self,
        start: usize,
        row_count: usize,
        schema: &crate::schema::SchemaDescriptor,
    ) -> super::batch::Batch {
        use super::batch::{compute_offsets, strides_from_schema, Batch};

        if row_count == 0 {
            return Batch::empty_with_schema(schema);
        }

        let shard = self.data();

        // Compute the final columnar layout before allocating anything.
        let (strides, num_regions_u8) = strides_from_schema(schema);
        let nr = num_regions_u8 as usize; // 4 + npc
        let (offsets, total_size) = compute_offsets(&strides, nr, row_count);

        // One allocation for all fixed-stride columnar data.
        let mut data = super::batch_pool::acquire_buf();
        data.clear();
        data.reserve(total_size);
        unsafe { data.set_len(total_size) };

        // Write each region directly into its final slice — no intermediate buffers.
        let expand_into = |region: &RegionView, stride: usize, dst: &mut [u8]| {
            match region {
                RegionView::Raw { offset, .. } => {
                    let begin = offset + start * stride;
                    dst.copy_from_slice(&shard[begin..begin + row_count * stride]);
                }
                RegionView::Constant { value, .. } => {
                    for chunk in dst.chunks_exact_mut(stride) {
                        chunk.copy_from_slice(&value[..stride]);
                    }
                }
                RegionView::TwoValue { value_a, value_b, bitvec_off } => {
                    let a_bytes = value_a.to_le_bytes();
                    let b_bytes = value_b.to_le_bytes();
                    for i in 0..row_count {
                        let row = start + i;
                        let bit = (shard[bitvec_off + row / 8] >> (row % 8)) & 1;
                        let src = if bit == 0 { &a_bytes[..stride] } else { &b_bytes[..stride] };
                        dst[i * stride..(i + 1) * stride].copy_from_slice(src);
                    }
                }
            }
        };

        let sz8 = row_count * 8;
        expand_into(&self.pk_lo,    8, &mut data[offsets[0] as usize..][..sz8]);
        expand_into(&self.pk_hi,    8, &mut data[offsets[1] as usize..][..sz8]);
        expand_into(&self.weight,   8, &mut data[offsets[2] as usize..][..sz8]);
        expand_into(&self.null_bmp, 8, &mut data[offsets[3] as usize..][..sz8]);

        for (pi, _ci, col) in schema.payload_columns() {
            let stride = col.size as usize;
            let off = offsets[4 + pi] as usize;
            let sz = row_count * stride;
            expand_into(&self.col_regions[pi], stride, &mut data[off..][..sz]);
        }

        // Blob: one allocation, copy entire blob region (string offsets stay valid).
        let blob = if self.blob_len > 0 {
            let src = &shard[self.blob_off..self.blob_off + self.blob_len];
            let mut buf = super::batch_pool::acquire_buf();
            buf.clear();
            buf.reserve(self.blob_len);
            unsafe { buf.set_len(self.blob_len) };
            buf.copy_from_slice(src);
            buf
        } else {
            Vec::new()
        };

        let mut batch = unsafe {
            Batch::from_prebuilt(data, blob, strides, offsets, num_regions_u8, row_count)
        };
        batch.sorted = true;
        batch.consolidated = true;
        batch.set_schema(*schema);
        batch
    }

    /// Bulk-copy all rows into an Batch.
    pub(crate) fn to_owned_batch(
        &self,
        schema: &crate::schema::SchemaDescriptor,
    ) -> super::batch::Batch {
        self.slice_to_owned_batch(0, self.count, schema)
    }
}

impl super::columnar::ColumnarSource for MappedShard {
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

// MappedShard does not implement Drop — the owned `mmap: Mmap` field handles
// `munmap` automatically when the shard is dropped.

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

        let image = super::super::shard_file::build_shard_image(0, count, &regions);
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

        let image = super::super::shard_file::build_shard_image(0, count, &regions);
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
        assert_eq!(shard.get_pk(0), 1);
        assert_eq!(shard.get_pk(9), 10);
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

        assert_eq!(shard.find_row_index(10), Some(4));
        assert_eq!(shard.find_row_index(200), Some(99));

        assert_eq!(shard.find_row_index(3), None);
        assert_eq!(shard.find_row_index(0), None);
        assert_eq!(shard.find_row_index(201), None);

        assert_eq!(shard.find_lower_bound(3), 1);
        assert_eq!(shard.find_lower_bound(1), 0);
        assert_eq!(shard.find_lower_bound(201), 100);
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
        assert_eq!(shard.err(), Some(StorageError::ChecksumMismatch));
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
        assert_eq!(shard.find_row_index(1), None);
        assert_eq!(shard.find_lower_bound(1), 0);
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
            assert_eq!(shard.get_pk(i), (i + 1) as u128);
        }
        // Binary search still works
        assert_eq!(shard.find_row_index(64), Some(63));
        assert_eq!(shard.find_row_index(1), Some(0));
        assert_eq!(shard.find_row_index(128), Some(127));
        assert_eq!(shard.find_lower_bound(65), 64);
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
            assert_eq!(shard.get_pk(i), (i + 1) as u128);
            assert_eq!(shard.get_weight(i), 1);
            assert_eq!(shard.get_null_word(i), 0);
        }
        assert_eq!(shard.find_row_index(3), Some(2));
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
        assert_eq!(MappedShard::open(&cpath, &schema, false).err(), Some(StorageError::InvalidShard));
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
        assert_eq!(MappedShard::open(&cpath, &schema, false).err(), Some(StorageError::InvalidShard));
    }

    #[test]
    fn two_value_truncated_bitvec_rejected() {
        // Build a shard with TwoValue weight encoding, then corrupt it so the
        // region size is shorter than the required bitvec (< 16 + ceil(n/8)).
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 64usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let vals: Vec<i64> = (0..n).map(|i| i as i64).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_trunc.db", &pks, &wts, &vals);
        let schema = test_schema();

        // The weight region must be TwoValue.  Shrink its size field in the
        // directory entry so bitvec_len < ceil(n/8) = 8 bytes.  Weight
        // region is entry index 2 in the directory.
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        let weight_entry_off = dir_off + 2 * DIR_ENTRY_SIZE;
        let orig_size = read_u64_le(&data, weight_entry_off + 8);
        // Only write the two values (16 bytes), drop the bitvec.
        let truncated_size = 16u64;
        assert!(orig_size > truncated_size, "weight region must be larger than 16 bytes for this test");
        // Patch the size field.  Checksum validation is disabled below so the
        // stale checksum doesn't mask the InvalidShard we're expecting.
        crate::util::write_u64_le(&mut data, weight_entry_off + 8, truncated_size);
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        // Must be rejected — the bitvec is too short for 64 rows.
        assert_eq!(
            MappedShard::open(&cpath, &schema, false).err(),
            Some(StorageError::InvalidShard),
            "truncated TwoValue bitvec must be rejected at open time"
        );
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
        assert_eq!(shard.get_pk(0), 42);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_null_word(0), 0);
        assert_eq!(shard.find_row_index(42), Some(0));
        assert_eq!(shard.find_row_index(1), None);
    }

    #[test]
    fn to_owned_batch_roundtrip() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.to_owned_batch(&schema);

        assert_eq!(batch.count, 10);
        assert!(batch.sorted);
        assert!(batch.consolidated);
        for i in 0..10 {
            assert_eq!(batch.get_pk(i), (i + 1) as u128);
            let w = crate::util::read_i64_le(batch.weight_data(), i * 8);
            assert_eq!(w, 1);
            let v = crate::util::read_i64_le(batch.col_data(0), i * 8);
            assert_eq!(v, (i as i64 + 1) * 100);
        }
    }

    #[test]
    fn to_owned_batch_constant_regions() {
        // All weights = 1 (Constant), all pk_hi = 0 (Constant), all null = 0 (Constant)
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 20;
        let pks: Vec<u64> = (1..=n).collect();
        let wts: Vec<i64> = vec![1; n as usize];
        let vals: Vec<i64> = vec![42; n as usize]; // constant payload
        let path = build_test_shard_weights(dir.path(), "const_batch.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.to_owned_batch(&schema);

        assert_eq!(batch.count, n as usize);
        for i in 0..n as usize {
            assert_eq!(batch.get_pk(i), (i + 1) as u128);
            assert_eq!(crate::util::read_i64_le(batch.weight_data(), i * 8), 1);
            assert_eq!(crate::util::read_i64_le(batch.col_data(0), i * 8), 42);
        }
    }

    #[test]
    fn to_owned_batch_two_value_weight() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 16usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let vals: Vec<i64> = (0..n).map(|i| i as i64 * 10).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_batch.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.to_owned_batch(&schema);

        assert_eq!(batch.count, n);
        for i in 0..n {
            let expected_w = if i % 2 == 0 { 1i64 } else { -1i64 };
            assert_eq!(crate::util::read_i64_le(batch.weight_data(), i * 8), expected_w, "row {i}");
            assert_eq!(crate::util::read_i64_le(batch.col_data(0), i * 8), i as i64 * 10);
        }
    }

    #[test]
    fn slice_to_owned_batch_with_offset() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        // Slice rows 3..7 (0-indexed: PKs 4,5,6,7)
        let batch = shard.slice_to_owned_batch(3, 4, &schema);
        assert_eq!(batch.count, 4);
        assert_eq!(batch.get_pk(0), 4);
        assert_eq!(batch.get_pk(3), 7);
        assert_eq!(crate::util::read_i64_le(batch.col_data(0), 0), 400);
        assert_eq!(crate::util::read_i64_le(batch.col_data(0), 3 * 8), 700);
    }

    #[test]
    fn slice_to_owned_batch_empty() {
        crate::util::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=5).map(|i| (i, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.slice_to_owned_batch(0, 0, &schema);
        assert_eq!(batch.count, 0);
    }
}
