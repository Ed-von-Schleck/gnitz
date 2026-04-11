//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::collections::HashMap;
use std::ffi::CStr;

use super::columnar::ColumnarSource;
use super::merge::{self, MemBatch};
use super::shard_file;
use crate::schema::SchemaDescriptor;
use crate::util::{read_i64_le, read_u64_le};

/// Minimum allocation size to request transparent hugepage backing.
/// Below this, hugepage promotion is impossible (no full 2MB PMD region).
const HUGEPAGE_THRESHOLD: usize = 2 * 1024 * 1024;

/// Maximum regions: 4 fixed (pk_lo, pk_hi, weight, null_bmp) + up to 63
/// payload columns.  Use 68 for alignment.
pub(super) const MAX_BATCH_REGIONS: usize = 68;

/// Allocate a zeroed buffer and request hugepage backing for large allocations.
///
/// For sizes >= HUGEPAGE_THRESHOLD, calls `calloc` (via `vec!`) which for large
/// allocations uses an internal `mmap`, returning demand-zero pages not yet faulted
/// in. The subsequent `madvise(MADV_HUGEPAGE)` causes the kernel to allocate 2MB
/// zero pages on first access instead of 4KB pages.
///
/// For sizes < HUGEPAGE_THRESHOLD, behaves identically to `vec![0u8; size]`.
#[inline]
fn alloc_large_zeroed(size: usize) -> Vec<u8> {
    let v = vec![0u8; size];
    if size >= HUGEPAGE_THRESHOLD {
        crate::sys::madvise_hugepage(v.as_ptr() as *mut u8, size);
    }
    v
}

/// Compute byte offsets for each region given strides and row capacity.
pub(super) fn compute_offsets(strides: &[u8; MAX_BATCH_REGIONS], num_regions: usize, capacity: usize) -> ([u32; MAX_BATCH_REGIONS], usize) {
    let mut offsets = [0u32; MAX_BATCH_REGIONS];
    let mut off = 0usize;
    for i in 0..num_regions {
        offsets[i] = off as u32;
        off += capacity * strides[i] as usize;
    }
    (offsets, off)
}

/// Copy a 16-byte German String struct from source into a destination slice,
/// relocating long-string blob data into `dst_blob`.
pub(super) fn relocate_string_cell(
    src_struct: &[u8],
    src_blob: &[u8],
    dst: &mut [u8],
    dst_blob: &mut Vec<u8>,
) {
    let (mut dest, is_long) = crate::schema::prep_german_string_copy(src_struct);
    if is_long {
        let length = u32::from_le_bytes(src_struct[0..4].try_into().unwrap()) as usize;
        let old_off = u64::from_le_bytes(src_struct[8..16].try_into().unwrap()) as usize;
        if old_off.saturating_add(length) > src_blob.len() {
            dest[0..4].copy_from_slice(&0u32.to_le_bytes());
        } else {
            let new_off = dst_blob.len();
            dst_blob.extend_from_slice(&src_blob[old_off..old_off + length]);
            dest[8..16].copy_from_slice(&(new_off as u64).to_le_bytes());
        }
    }
    dst[..16].copy_from_slice(&dest);
}

/// Build a strides array from a SchemaDescriptor.
pub(super) fn strides_from_schema(schema: &SchemaDescriptor) -> ([u8; MAX_BATCH_REGIONS], u8) {
    let pk_index = schema.pk_index as usize;
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    strides[0] = 8; strides[1] = 8; strides[2] = 8; strides[3] = 8;
    let mut pi = 0;
    for ci in 0..schema.num_columns as usize {
        if ci == pk_index { continue; }
        strides[4 + pi] = schema.columns[ci].size;
        pi += 1;
    }
    let nr = (4 + pi) as u8;
    (strides, nr)
}

/// Owned columnar batch.  All fixed-stride column data lives in a single
/// contiguous `data` buffer.  Blob data is separate (variable-length).
///
/// Layout of `data`: `[pk_lo | pk_hi | weight | null_bmp | col_0 | ... | col_{N-1}]`
/// Each region has `capacity * stride` bytes allocated; `count * stride` bytes
/// contain data.
///
/// **2 heap allocations** (data + blob) instead of N+7.
pub struct Batch {
    data: Vec<u8>,
    pub blob: Vec<u8>,
    offsets: [u32; MAX_BATCH_REGIONS],
    strides: [u8; MAX_BATCH_REGIONS],
    num_regions: u8,
    capacity: u32,
    pub count: usize,
    pub sorted: bool,
    pub consolidated: bool,
    pub schema: Option<SchemaDescriptor>,
}

impl Batch {
    // ── Constructors ────────────────────────────────────────────────────

    /// Create an empty batch with the given number of payload columns.
    pub fn empty(num_payload_cols: usize) -> Self {
        let nr = 4 + num_payload_cols;
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        strides[0] = 8; strides[1] = 8; strides[2] = 8; strides[3] = 8;
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0u32; MAX_BATCH_REGIONS],
            strides,
            num_regions: nr as u8,
            capacity: 0,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: None,
        }
    }

    /// Create an empty batch with schema, pre-allocated for `initial_capacity` rows.
    pub fn with_schema(schema: SchemaDescriptor, initial_capacity: usize) -> Self {
        let cap = initial_capacity.max(1);
        let (strides, nr) = strides_from_schema(&schema);
        let (offsets, total_size) = compute_offsets(&strides, nr as usize, cap);

        let data = if let Some(mut buf) = super::batch_pool::acquire_buf() {
            buf.resize(total_size, 0);
            buf
        } else {
            vec![0u8; total_size]
        };

        Batch {
            data,
            blob: Vec::with_capacity(64),
            offsets,
            strides,
            num_regions: nr,
            capacity: cap as u32,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: Some(schema),
        }
    }

    /// Construct by copying data in from FFI region pointers.
    ///
    /// Region layout: pk_lo, pk_hi, weight, null_bmp,
    /// payload_col_0 .. payload_col_{N-1}, blob.
    ///
    /// # Safety
    /// `ptrs[i]` must point to at least `sizes[i]` readable bytes.
    pub unsafe fn from_regions(
        ptrs: &[*const u8],
        sizes: &[u32],
        count: usize,
        num_payload_cols: usize,
    ) -> Self {
        if count == 0 {
            return Self::empty(num_payload_cols);
        }

        let nr = 4 + num_payload_cols;
        // Derive strides from sizes / count.
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        for i in 0..nr {
            strides[i] = (sizes[i] as usize / count) as u8;
        }
        let (offsets, total_size) = compute_offsets(&strides, nr, count);

        // SAFETY: the loop below writes every byte of every non-null region with
        // stride>0. compute_offsets packs regions back-to-back, so total_size is
        // fully covered. Bytes belonging to a stride-0 or null-ptr region are
        // zero bytes that no accessor reads (count*stride == 0 for those slots).
        let mut data = if let Some(mut buf) = super::batch_pool::acquire_buf() {
            buf.clear();
            buf.reserve(total_size);
            unsafe { buf.set_len(total_size); }
            buf
        } else {
            let mut v = Vec::with_capacity(total_size);
            unsafe { v.set_len(total_size); }
            v
        };

        // Copy each region into the contiguous buffer.
        for i in 0..nr {
            let sz = sizes[i] as usize;
            if sz > 0 && !ptrs[i].is_null() {
                let off = offsets[i] as usize;
                std::ptr::copy_nonoverlapping(ptrs[i], data.as_mut_ptr().add(off), sz);
            }
        }

        // Blob is separate — try pool first.
        let blob_idx = 4 + num_payload_cols;
        let blob_sz = sizes[blob_idx] as usize;
        let blob = if blob_sz > 0 && !ptrs[blob_idx].is_null() {
            // SAFETY: copy_nonoverlapping below writes all blob_sz bytes.
            let mut v = if let Some(mut buf) = super::batch_pool::acquire_buf() {
                buf.clear();
                buf.reserve(blob_sz);
                unsafe { buf.set_len(blob_sz); }
                buf
            } else {
                let mut v = Vec::with_capacity(blob_sz);
                unsafe { v.set_len(blob_sz); }
                v
            };
            std::ptr::copy_nonoverlapping(ptrs[blob_idx], v.as_mut_ptr(), blob_sz);
            v
        } else {
            Vec::new()
        };

        Batch {
            data,
            blob,
            offsets,
            strides,
            num_regions: nr as u8,
            capacity: count as u32,
            count,
            sorted: false,
            consolidated: false,
            schema: None,
        }
    }

    // ── Read accessors ──────────────────────────────────────────────────

    #[inline]
    pub fn pk_lo_data(&self) -> &[u8] {
        let off = self.offsets[0] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn pk_hi_data(&self) -> &[u8] {
        let off = self.offsets[1] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn weight_data(&self) -> &[u8] {
        let off = self.offsets[2] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn null_bmp_data(&self) -> &[u8] {
        let off = self.offsets[3] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn col_data(&self, pi: usize) -> &[u8] {
        let r = 4 + pi;
        let off = self.offsets[r] as usize;
        &self.data[off..off + self.count * self.strides[r] as usize]
    }
    #[inline]
    pub fn blob_data(&self) -> &[u8] { &self.blob }
    #[inline]
    pub fn num_payload_cols(&self) -> usize {
        self.num_regions as usize - 4
    }

    // ── Mutable slice accessors ─────────────────────────────────────────

    #[inline]
    pub fn pk_lo_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[0] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn pk_hi_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[1] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn weight_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[2] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn null_bmp_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[3] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn col_data_mut(&mut self, pi: usize) -> &mut [u8] {
        let r = 4 + pi;
        let off = self.offsets[r] as usize;
        let end = off + self.count * self.strides[r] as usize;
        &mut self.data[off..end]
    }

    // ── Row accessors ───────────────────────────────────────────────────

    #[inline]
    fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[0] as usize..], row * 8)
    }
    #[inline]
    fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[1] as usize..], row * 8)
    }
    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        crate::util::make_pk(self.get_pk_lo(row), self.get_pk_hi(row))
    }
    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(&self.data[self.offsets[2] as usize..], row * 8)
    }
    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[3] as usize..], row * 8)
    }
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = self.offsets[4 + payload_col] as usize + row * col_size;
        &self.data[off..off + col_size]
    }

    // ── Extend methods (building batches row-by-row) ────────────────────

    /// Initialize strides from schema if they're unset (batch created via empty()).
    fn init_strides_from_schema(&mut self) {
        if self.strides[4] != 0 || self.num_payload_cols() == 0 { return; }
        if let Some(schema) = self.schema {
            let (s, nr) = strides_from_schema(&schema);
            self.strides = s;
            self.num_regions = nr;
        }
    }

    /// Copy strides from another batch if ours are unset.
    fn init_strides_from_batch(&mut self, src: &Batch) {
        if self.strides[4] != 0 || self.num_payload_cols() == 0 { return; }
        let nr = src.num_regions as usize;
        self.strides[..nr].copy_from_slice(&src.strides[..nr]);
        self.num_regions = src.num_regions;
    }

    /// Ensure the data buffer has room for at least one more row.
    pub(crate) fn ensure_row_capacity(&mut self) {
        self.reserve_rows(1);
    }

    /// Ensure the data buffer has room for at least `n` more rows beyond `count`.
    pub(crate) fn reserve_rows(&mut self, n: usize) {
        self.init_strides_from_schema();
        if self.count + n <= self.capacity as usize { return; }
        let nr = self.num_regions as usize;
        let new_cap = (self.capacity as usize * 2).max(8).max(self.count + n);
        let (new_offsets, new_total) = compute_offsets(&self.strides, nr, new_cap);

        // Hugepage-friendly fast path: if the new buffer crosses the 2MB
        // threshold and the current buffer doesn't already have a >=2MB
        // allocation behind it, do an out-of-place grow into a fresh
        // alloc_large_zeroed buffer. This (a) routes the allocation through
        // madvise(MADV_HUGEPAGE), and (b) lets us copy regions directly into
        // their NEW offsets, skipping the in-place shift loop.
        if new_total >= HUGEPAGE_THRESHOLD && self.data.capacity() < HUGEPAGE_THRESHOLD {
            let mut new_data = alloc_large_zeroed(new_total);
            for i in 0..nr {
                let len = self.count * self.strides[i] as usize;
                if len == 0 { continue; }
                let old_off = self.offsets[i] as usize;
                let new_off = new_offsets[i] as usize;
                new_data[new_off..new_off + len]
                    .copy_from_slice(&self.data[old_off..old_off + len]);
            }
            // Old buffer is dropped here; its Drop returns it to batch_pool.
            self.data = new_data;
            self.offsets = new_offsets;
            self.capacity = new_cap as u32;
            return;
        }

        // In-place grow. Reserve capacity without zero-init; slack is never read
        // (every accessor on Batch is bounded by `count`, not `capacity`).
        // SAFETY: bytes in [old_len..new_total) are uninit until the shift loop
        // and future appends populate the live regions. No accessor reads slack.
        let additional = new_total.saturating_sub(self.data.len());
        self.data.reserve(additional);
        unsafe { self.data.set_len(new_total); }

        // Move columns backwards to avoid overlap (new offsets >= old offsets).
        for i in (0..nr).rev() {
            let old_start = self.offsets[i] as usize;
            let new_start = new_offsets[i] as usize;
            let data_len = self.count * self.strides[i] as usize;
            if old_start != new_start && data_len > 0 {
                self.data.copy_within(old_start..old_start + data_len, new_start);
            }
        }
        self.offsets = new_offsets;
        self.capacity = new_cap as u32;
    }

    /// Write data into region `r` at the current row position.
    /// Auto-grows capacity if needed.  Auto-infers stride from the first write
    /// (for batches created via `empty()` where strides are initially 0).
    #[inline]
    fn extend_region(&mut self, r: usize, src: &[u8]) {
        if self.strides[r] == 0 && !src.is_empty() {
            self.strides[r] = src.len() as u8;
            self.capacity = 0; // force realloc to include this region
        }
        if self.count >= self.capacity as usize {
            self.ensure_row_capacity();
        }
        let off = self.offsets[r] as usize + self.count * self.strides[r] as usize;
        self.data[off..off + src.len()].copy_from_slice(src);
    }

    #[inline]
    pub fn extend_pk_lo(&mut self, d: &[u8]) { self.extend_region(0, d); }
    #[inline]
    pub fn extend_pk_hi(&mut self, d: &[u8]) { self.extend_region(1, d); }
    #[inline]
    pub fn extend_weight(&mut self, d: &[u8]) { self.extend_region(2, d); }
    #[inline]
    pub fn extend_null_bmp(&mut self, d: &[u8]) { self.extend_region(3, d); }
    #[inline]
    pub fn extend_col(&mut self, pi: usize, d: &[u8]) { self.extend_region(4 + pi, d); }

    /// Fill `nbytes` of zeros at the current row position in a payload column.
    #[inline]
    pub fn fill_col_zero(&mut self, pi: usize, nbytes: usize) {
        let r = 4 + pi;
        if self.strides[r] == 0 && nbytes > 0 {
            self.strides[r] = nbytes as u8;
            self.capacity = 0;
        }
        if self.count >= self.capacity as usize {
            self.ensure_row_capacity();
        }
        let off = self.offsets[r] as usize + self.count * self.strides[r] as usize;
        self.data[off..off + nbytes].fill(0);
    }

    /// Bulk-copy a range of rows from `src_region_data` into region `r`.
    fn bulk_copy_region(&mut self, r: usize, src_region_data: &[u8], start: usize, end: usize) {
        let stride = self.strides[r] as usize;
        let n = end - start;
        let dst_off = self.offsets[r] as usize + self.count * stride;
        let src_off = start * stride;
        self.data[dst_off..dst_off + n * stride]
            .copy_from_slice(&src_region_data[src_off..src_off + n * stride]);
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Create a borrowed `MemBatch` view over this batch's data.
    pub fn as_mem_batch(&self) -> MemBatch<'_> {
        let npc = self.num_payload_cols();
        let mut col_slices = Vec::with_capacity(npc);
        for pi in 0..npc {
            col_slices.push(self.col_data(pi));
        }
        MemBatch {
            pk_lo: self.pk_lo_data(),
            pk_hi: self.pk_hi_data(),
            weight: self.weight_data(),
            null_bmp: self.null_bmp_data(),
            col_data: col_slices,
            blob: &self.blob,
            count: self.count,
        }
    }

    /// Total bytes occupied by all buffers.
    pub fn total_bytes(&self) -> usize {
        let nr = self.num_regions as usize;
        let mut total = self.blob.len();
        for i in 0..nr {
            total += self.count * self.strides[i] as usize;
        }
        total
    }

    /// Scatter-copy selected rows from a MemBatch into a new Batch.
    pub fn from_indexed_rows(
        batch: &MemBatch,
        indices: &[u32],
        schema: &SchemaDescriptor,
    ) -> Self {
        if indices.is_empty() {
            return Self::empty(schema.num_columns as usize - 1);
        }
        let blob_cap = batch.blob.len().max(1);
        write_to_batch(schema, indices.len(), blob_cap, |writer| {
            merge::scatter_copy(batch, indices, &[], writer);
        })
    }

    /// Produce region pointer/size arrays for `Table::ingest_batch_from_regions`.
    pub fn to_region_ptrs(&self) -> (Vec<*const u8>, Vec<u32>) {
        let npc = self.num_payload_cols();
        let cap = 4 + npc + 1;
        let mut ptrs = Vec::with_capacity(cap);
        let mut sizes = Vec::with_capacity(cap);
        for i in 0..4 + npc {
            let off = self.offsets[i] as usize;
            let len = self.count * self.strides[i] as usize;
            ptrs.push(unsafe { self.data.as_ptr().add(off) });
            sizes.push(len as u32);
        }
        ptrs.push(self.blob.as_ptr());
        sizes.push(self.blob.len() as u32);
        (ptrs, sizes)
    }

    /// Clone all buffers into a new independent Batch.
    /// 2 allocations (data + blob) instead of N+7.
    pub fn clone_batch(&self) -> Self {
        // Only clone the actually-used portion of data (count-based, not capacity-based).
        let nr = self.num_regions as usize;
        let (packed_offsets, packed_size) = compute_offsets(&self.strides, nr, self.count);
        let mut new_data = Vec::with_capacity(packed_size);
        // SAFETY: we'll fill all `packed_size` bytes immediately below.
        unsafe { new_data.set_len(packed_size); }
        for i in 0..nr {
            let stride = self.strides[i] as usize;
            let len = self.count * stride;
            let src_off = self.offsets[i] as usize;
            let dst_off = packed_offsets[i] as usize;
            new_data[dst_off..dst_off + len].copy_from_slice(&self.data[src_off..src_off + len]);
        }
        Batch {
            data: new_data,
            blob: self.blob.clone(),
            offsets: packed_offsets,
            strides: self.strides,
            num_regions: self.num_regions,
            capacity: self.count as u32,
            count: self.count,
            sorted: self.sorted,
            consolidated: self.consolidated,
            schema: self.schema,
        }
    }

    /// Decompose into owned buffers (for pool recycling).
    /// Takes the buffers out, leaving zero-capacity vecs that Drop will ignore.
    pub(crate) fn into_buffers(mut self) -> (Vec<u8>, Vec<u8>) {
        let data = std::mem::take(&mut self.data);
        let blob = std::mem::take(&mut self.blob);
        (data, blob)
    }

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

    /// Append a single row from flat column data.
    ///
    /// # Safety
    /// `col_ptrs[i]` must be valid for `col_sizes[i]` bytes.
    pub unsafe fn append_row(
        &mut self,
        pk: u128,
        weight: i64,
        null_word: u64,
        col_ptrs: &[*const u8],
        col_sizes: &[u32],
        blob_src: &[u8],
    ) {
        self.ensure_row_capacity();
        let (pk_lo, pk_hi) = crate::util::split_pk(pk);
        self.extend_pk_lo(&pk_lo.to_le_bytes());
        self.extend_pk_hi(&pk_hi.to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        let schema = self.schema;
        let pk_index = schema.map_or(usize::MAX, |s| s.pk_index as usize);
        let mut pi = 0;

        for (ci_raw, (ptr, &sz)) in col_ptrs.iter().zip(col_sizes.iter()).enumerate() {
            let ci = if pk_index == usize::MAX { ci_raw }
                     else if ci_raw < pk_index { ci_raw }
                     else { ci_raw + 1 };

            let is_string = schema.map_or(false, |s| {
                ci < s.num_columns as usize
                    && s.columns[ci].type_code == crate::schema::type_code::STRING
            });
            let is_null = (null_word >> pi) & 1 != 0;
            let col_size = sz as usize;

            if is_null {
                self.fill_col_zero(pi, col_size);
            } else if is_string && col_size == 16 {
                let src = std::slice::from_raw_parts(*ptr, 16);
                let length = u32::from_le_bytes(src[0..4].try_into().unwrap()) as usize;
                let mut dest = [0u8; 16];
                dest[0..8].copy_from_slice(&src[0..8]);
                if length <= crate::schema::SHORT_STRING_THRESHOLD {
                    let sfx = if length > 4 { length - 4 } else { 0 };
                    if sfx > 0 { dest[8..8 + sfx].copy_from_slice(&src[8..8 + sfx]); }
                } else {
                    let old_off = u64::from_le_bytes(src[8..16].try_into().unwrap()) as usize;
                    if old_off.saturating_add(length) <= blob_src.len() {
                        let new_off = self.blob.len();
                        self.blob.extend_from_slice(&blob_src[old_off..old_off + length]);
                        dest[8..16].copy_from_slice(&(new_off as u64).to_le_bytes());
                    } else {
                        // Malformed wire data: emit an empty string rather
                        // than silently substituting unrelated bytes. Matches
                        // relocate_string_cell. dest[8..16] is already zero
                        // from the initializer above.
                        dest[0..4].copy_from_slice(&0u32.to_le_bytes());
                    }
                }
                self.extend_col(pi, &dest);
            } else {
                let src = std::slice::from_raw_parts(*ptr, col_size);
                self.extend_col(pi, src);
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Append a row from RowBuilder-style value arrays.
    ///
    /// # Safety
    /// For STRING columns, `str_ptrs[i]` must be valid for `str_lens[i]` bytes.
    pub unsafe fn append_row_simple(
        &mut self,
        pk: u128, weight: i64, null_word: u64,
        lo_values: &[i64],
        hi_values: &[u64],
        str_ptrs: &[*const u8],
        str_lens: &[u32],
    ) {
        self.ensure_row_capacity();
        let (pk_lo, pk_hi) = crate::util::split_pk(pk);
        self.extend_pk_lo(&pk_lo.to_le_bytes());
        self.extend_pk_hi(&pk_hi.to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        let schema = self.schema.expect("append_row_simple requires schema");
        let pk_index = schema.pk_index as usize;

        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col = &schema.columns[ci];
            let col_size = col.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;

            if is_null {
                self.fill_col_zero(pi, col_size);
            } else {
                match col.type_code {
                    crate::schema::type_code::STRING => {
                        let ptr = str_ptrs[pi];
                        let slen = str_lens[pi] as usize;
                        let bytes: &[u8] = if ptr.is_null() || slen == 0 {
                            &[]
                        } else {
                            std::slice::from_raw_parts(ptr, slen)
                        };
                        let gs = crate::schema::encode_german_string(bytes, &mut self.blob);
                        self.extend_col(pi, &gs);
                    }
                    crate::schema::type_code::F64 => {
                        self.extend_col(pi, &lo_values[pi].to_le_bytes());
                    }
                    crate::schema::type_code::F32 => {
                        let f64_val = f64::from_bits(lo_values[pi] as u64);
                        let f32_val = f64_val as f32;
                        self.extend_col(pi, &f32_val.to_le_bytes());
                    }
                    crate::schema::type_code::U128 => {
                        self.extend_col(pi, &(lo_values[pi] as u64).to_le_bytes());
                        // U128 second half goes into same cell at +8
                        let r = 4 + pi;
                        let off = self.offsets[r] as usize + self.count * self.strides[r] as usize + 8;
                        self.data[off..off + 8].copy_from_slice(&hi_values[pi].to_le_bytes());
                    }
                    _ => {
                        let bytes = lo_values[pi].to_le_bytes();
                        self.extend_col(pi, &bytes[..col_size]);
                    }
                }
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Bulk-copy rows [start, end) from another Batch (same schema).
    pub fn append_batch(&mut self, src: &Batch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.init_strides_from_batch(src);
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, false);
    }

    /// Bulk-copy rows with negated weights.
    pub fn append_batch_negated(&mut self, src: &Batch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.init_strides_from_batch(src);
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, true);
    }

    fn append_rows_inner(&mut self, src: &Batch, start: usize, end: usize, negate: bool) {
        let n = end - start;
        self.reserve_rows(n);

        // Fixed columns: bulk copy from src's data buffer.
        self.bulk_copy_region(0, &src.data[src.offsets[0] as usize..], start, end);
        self.bulk_copy_region(1, &src.data[src.offsets[1] as usize..], start, end);
        if negate {
            let w_off = self.offsets[2] as usize;
            for i in start..end {
                let dst = w_off + (self.count + i - start) * 8;
                self.data[dst..dst + 8].copy_from_slice(&(-src.get_weight(i)).to_le_bytes());
            }
        } else {
            self.bulk_copy_region(2, &src.data[src.offsets[2] as usize..], start, end);
        }
        self.bulk_copy_region(3, &src.data[src.offsets[3] as usize..], start, end);

        // Payload columns
        let has_schema = self.schema.is_some();
        let schema_copy = self.schema;
        let pk_index = schema_copy.map_or(usize::MAX, |s| s.pk_index as usize);
        let npc = self.num_payload_cols();

        let mut pi = 0;
        let num_cols = schema_copy.map_or(npc, |s| s.num_columns as usize);
        for ci in 0..num_cols {
            if ci == pk_index { continue; }
            if pi >= npc { break; }
            let is_string = has_schema
                && schema_copy.unwrap().columns[ci].type_code == crate::schema::type_code::STRING;
            let cs = self.strides[4 + pi] as usize;

            if is_string && cs == 16 {
                for row in start..end {
                    let src_off = src.offsets[4 + pi] as usize + row * 16;
                    let src_struct = &src.data[src_off..src_off + 16];
                    let dst_off = self.offsets[4 + pi] as usize + (self.count + row - start) * 16;
                    relocate_string_cell(src_struct, &src.blob, &mut self.data[dst_off..dst_off + 16], &mut self.blob);
                }
            } else if cs > 0 {
                self.bulk_copy_region(4 + pi, &src.data[src.offsets[4 + pi] as usize..], start, end);
            }
            pi += 1;
        }

        self.count += n;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Copy the found row from a PartitionedTable into this batch.
    pub fn append_row_from_ptable_found(
        &mut self,
        ptable: &super::partitioned_table::PartitionedTable,
        pk: u128,
        weight: i64,
    ) {
        self.ensure_row_capacity();
        let schema = self.schema.expect("append_row_from_ptable_found requires schema");
        let pk_index = schema.pk_index as usize;
        let null_word = ptable.found_null_word();

        let (pk_lo, pk_hi) = crate::util::split_pk(pk);
        self.extend_pk_lo(&pk_lo.to_le_bytes());
        self.extend_pk_hi(&pk_hi.to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                self.fill_col_zero(pi, cs);
            } else if col_desc.type_code == crate::schema::type_code::STRING {
                let src = ptable.found_col_ptr(pi, cs);
                assert!(!src.is_null());
                let src_slice = unsafe { std::slice::from_raw_parts(src, cs) };
                // Inline write_string_from_raw to use field splitting.
                let (mut dest, is_long) = crate::schema::prep_german_string_copy(src_slice);
                if is_long {
                    let length = u32::from_le_bytes(src_slice[0..4].try_into().unwrap()) as usize;
                    let blob_ptr = ptable.found_blob_ptr();
                    assert!(!blob_ptr.is_null());
                    let old_offset = u64::from_le_bytes(src_slice[8..16].try_into().unwrap()) as usize;
                    let src_data = unsafe { std::slice::from_raw_parts(blob_ptr.add(old_offset), length) };
                    let new_offset = self.blob.len();
                    self.blob.extend_from_slice(src_data);
                    dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
                }
                self.extend_col(pi, &dest);
            } else {
                let src = ptable.found_col_ptr(pi, cs);
                assert!(!src.is_null());
                let src_slice = unsafe { std::slice::from_raw_parts(src, cs) };
                self.extend_col(pi, src_slice);
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Scatter-copy selected rows from a MemBatch directly into this batch.
    pub fn append_indexed_rows(
        &mut self,
        src: &merge::MemBatch,
        indices: &[u32],
        schema: &crate::schema::SchemaDescriptor,
    ) {
        if indices.is_empty() { return; }
        let pki = schema.pk_index as usize;
        let mut col_meta: Vec<(usize, bool)> = Vec::with_capacity(schema.num_columns as usize - 1);
        for ci in 0..schema.num_columns as usize {
            if ci == pki { continue; }
            let col = &schema.columns[ci];
            let is_string = col.type_code == crate::schema::type_code::STRING && col.size == 16;
            col_meta.push((col.size as usize, is_string));
        }

        self.reserve_rows(indices.len());
        for &idx in indices {
            let row = idx as usize;
            let pk = src.get_pk(row);
            let (lo, hi) = crate::util::split_pk(pk);
            self.extend_pk_lo(&lo.to_le_bytes());
            self.extend_pk_hi(&hi.to_le_bytes());
            self.extend_weight(&src.get_weight(row).to_le_bytes());
            let null_word = src.get_null_word(row);
            self.extend_null_bmp(&null_word.to_le_bytes());

            for (pi, &(cs, is_string)) in col_meta.iter().enumerate() {
                let is_null = (null_word >> pi) & 1 != 0;
                if is_null {
                    self.fill_col_zero(pi, cs);
                } else if is_string {
                    let src_struct = src.get_col_ptr(row, pi, 16);
                    let dst_off = self.offsets[4 + pi] as usize + self.count * 16;
                    relocate_string_cell(src_struct, src.blob, &mut self.data[dst_off..dst_off + 16], &mut self.blob);
                } else if cs > 0 {
                    self.extend_col(pi, src.get_col_ptr(row, pi, cs));
                }
            }
            self.count += 1;
        }
        self.sorted = false;
        self.consolidated = false;
    }

    /// Reset to empty without freeing buffer allocations.
    pub fn clear(&mut self) {
        self.count = 0;
        self.blob.clear();
        self.sorted = true;
        self.consolidated = true;
        // data buffer stays allocated — capacity and offsets remain valid.
    }

    /// Number of regions in the standard layout (including blob).
    pub fn num_regions_total(&self) -> usize {
        self.num_regions as usize + 1
    }

    /// Get region pointer by index.
    pub fn region_ptr(&self, idx: usize) -> *const u8 {
        let npc = self.num_payload_cols();
        if idx < 4 + npc {
            unsafe { self.data.as_ptr().add(self.offsets[idx] as usize) }
        } else {
            self.blob.as_ptr()
        }
    }

    /// Get region size by index.
    pub fn region_size(&self, idx: usize) -> usize {
        let npc = self.num_payload_cols();
        if idx < 4 + npc {
            self.count * self.strides[idx] as usize
        } else {
            self.blob.len()
        }
    }

    pub fn regions(&self) -> Vec<(*const u8, usize)> {
        let npc = self.num_payload_cols();
        let mut r = Vec::with_capacity(4 + npc + 1);
        for i in 0..4 + npc {
            let off = self.offsets[i] as usize;
            let len = self.count * self.strides[i] as usize;
            r.push((unsafe { self.data.as_ptr().add(off) }, len));
        }
        r.push((self.blob.as_ptr(), self.blob.len()));
        r
    }

    /// Append a single row from any ColumnarSource with blob deduplication.
    pub fn append_row_from_source<S: ColumnarSource>(
        &mut self,
        key: u128,
        weight: i64,
        source: &S,
        row: usize,
        blob_cache: &mut HashMap<(u64, usize), usize>,
    ) {
        if weight == 0 { return; }
        self.ensure_row_capacity();
        let schema = self.schema.expect("append_row_from_source requires schema");
        let pk_index = schema.pk_index as usize;

        self.extend_pk_lo(&(key as u64).to_le_bytes());
        self.extend_pk_hi(&((key >> 64) as u64).to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        let null_word = source.get_null_word(row);
        self.extend_null_bmp(&null_word.to_le_bytes());

        let src_blob = source.blob_slice();
        let mut pi: usize = 0;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col = &schema.columns[ci];
            let cs = col.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;

            if is_null {
                self.fill_col_zero(pi, cs);
            } else if col.type_code == crate::schema::type_code::STRING {
                let src_struct = source.get_col_ptr(row, pi, cs);
                let (mut dest, is_long) = crate::schema::prep_german_string_copy(src_struct);
                if is_long {
                    let length = u32::from_le_bytes(src_struct[0..4].try_into().unwrap()) as usize;
                    let old_offset = u64::from_le_bytes(src_struct[8..16].try_into().unwrap()) as usize;
                    if old_offset.saturating_add(length) > src_blob.len() {
                        dest[0..4].copy_from_slice(&0u32.to_le_bytes());
                    } else {
                        let src_data = &src_blob[old_offset..old_offset + length];
                        let new_offset = self.blob.len();
                        let off = match crate::schema::blob_cache_lookup(
                            src_data, blob_cache, &self.blob, new_offset,
                        ) {
                            Some(cached) => cached,
                            None => {
                                self.blob.extend_from_slice(src_data);
                                new_offset
                            }
                        };
                        dest[8..16].copy_from_slice(&(off as u64).to_le_bytes());
                    }
                }
                self.extend_col(pi, &dest);
            } else {
                self.extend_col(pi, source.get_col_ptr(row, pi, cs));
            }
            pi += 1;
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    #[cfg(test)]
    pub(crate) fn data_capacity(&self) -> usize { self.data.capacity() }

    /// Write this batch as a shard file directly to disk.
    pub fn write_as_shard(&self, path: &CStr, table_id: u32) -> i32 {
        let regions = self.regions();
        shard_file::write_shard_streaming(libc::AT_FDCWD, path, table_id, self.count as u32, &regions, true)
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        super::batch_pool::recycle_buf(std::mem::take(&mut self.data));
        super::batch_pool::recycle_buf(std::mem::take(&mut self.blob));
    }
}

impl Clone for Batch {
    fn clone(&self) -> Self { self.clone_batch() }
}

impl ColumnarSource for Batch {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        Batch::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        Batch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] { &self.blob }
}

/// Allocate a single contiguous arena, run a merge/copy operation via
/// DirectWriter, and return the arena as a Batch — zero copy-out.
///
/// In steady state the arena is recycled from the thread-local buffer pool,
/// so this path allocates nothing.
pub fn write_to_batch(
    schema: &SchemaDescriptor,
    max_rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut merge::DirectWriter),
) -> Batch {
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;

    // Arena layout: [pk_lo | pk_hi | weight | null | col_0 | ... | col_{N-1}]
    // Sized for max_rows; blob is separate.
    let (offsets, arena_size) = compute_offsets(&strides, nr, max_rows);

    let mut data = if let Some(mut buf) = super::batch_pool::acquire_buf() {
        buf.resize(arena_size, 0);
        buf
    } else {
        alloc_large_zeroed(arena_size)
    };
    let mut blob = if let Some(mut buf) = super::batch_pool::acquire_buf() {
        buf.resize(max_blob, 0);
        buf
    } else {
        alloc_large_zeroed(max_blob)
    };

    let actual_rows;
    let actual_blob;
    {
        // Carve non-overlapping mutable slices via split_at_mut.
        let fixed = max_rows * 8;
        let (pk_lo, rest) = data.split_at_mut(fixed);
        let (pk_hi, rest) = rest.split_at_mut(fixed);
        let (weight, rest) = rest.split_at_mut(fixed);
        let (null_bmp, mut rest) = rest.split_at_mut(fixed);

        let mut col_slices: Vec<&mut [u8]> = Vec::with_capacity(nr - 4);
        for i in 4..nr {
            let col_sz = max_rows * strides[i] as usize;
            let (col, new_rest) = rest.split_at_mut(col_sz);
            col_slices.push(col);
            rest = new_rest;
        }

        let mut writer = merge::DirectWriter::new(
            pk_lo, pk_hi, weight, null_bmp,
            col_slices, &mut blob, *schema,
        );
        write_fn(&mut writer);
        actual_rows = writer.row_count();
        actual_blob = writer.blob_written();
    }

    // The arena IS the batch's data buffer — no copy-out needed.
    // Just record the actual row count; offsets stay max_rows-based
    // (extra capacity is harmless and enables future appends).
    blob.truncate(actual_blob);

    Batch {
        data,
        blob,
        offsets,
        strides,
        num_regions: nr as u8,
        capacity: max_rows as u32,
        count: actual_rows,
        sorted: false,
        consolidated: false,
        schema: None,
    }
}
