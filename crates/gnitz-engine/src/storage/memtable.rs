//! MemTable: manages sorted runs, Bloom filter, consolidation cache,
//! PK lookups, and shard flush.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::Arc;

use super::bloom::BloomFilter;
use super::columnar::{self, ColumnarSource};
use crate::schema::SchemaDescriptor;
use super::merge::{self, MemBatch};
use super::shard_file;
use crate::util::{read_i64_le, read_u64_le};

/// Error code returned when the MemTable exceeds its capacity.
pub const ERR_CAPACITY: i32 = -2;

/// Maximum sorted runs before inline consolidation merges them into one.
const INLINE_CONSOLIDATE_THRESHOLD: usize = 16;

/// Minimum allocation size to request transparent hugepage backing.
/// Below this, hugepage promotion is impossible (no full 2MB PMD region).
const HUGEPAGE_THRESHOLD: usize = 2 * 1024 * 1024;

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

/// Maximum regions: 4 fixed (pk_lo, pk_hi, weight, null_bmp) + up to 63
/// payload columns.  Use 68 for alignment.
const MAX_BATCH_REGIONS: usize = 68;

/// Compute byte offsets for each region given strides and row capacity.
fn compute_offsets(strides: &[u8; MAX_BATCH_REGIONS], num_regions: usize, capacity: usize) -> ([u32; MAX_BATCH_REGIONS], usize) {
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
fn relocate_string_cell(
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
fn strides_from_schema(schema: &SchemaDescriptor) -> ([u8; MAX_BATCH_REGIONS], u8) {
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
pub struct OwnedBatch {
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

impl OwnedBatch {
    // ── Constructors ────────────────────────────────────────────────────

    /// Create an empty batch with the given number of payload columns.
    pub fn empty(num_payload_cols: usize) -> Self {
        let nr = 4 + num_payload_cols;
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        strides[0] = 8; strides[1] = 8; strides[2] = 8; strides[3] = 8;
        OwnedBatch {
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

        OwnedBatch {
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

        OwnedBatch {
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
    fn init_strides_from_batch(&mut self, src: &OwnedBatch) {
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
        // (every accessor on OwnedBatch is bounded by `count`, not `capacity`).
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

    /// Scatter-copy selected rows from a MemBatch into a new OwnedBatch.
    pub fn from_indexed_rows(
        batch: &MemBatch,
        indices: &[u32],
        schema: &SchemaDescriptor,
    ) -> Self {
        if indices.is_empty() {
            return Self::empty(schema.num_columns as usize - 1);
        }
        let blob_cap = batch.blob.len().max(1);
        write_to_owned_batch(schema, indices.len(), blob_cap, |writer| {
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

    /// Clone all buffers into a new independent OwnedBatch.
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
        OwnedBatch {
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

    /// Bulk-copy rows [start, end) from another OwnedBatch (same schema).
    pub fn append_batch(&mut self, src: &OwnedBatch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.init_strides_from_batch(src);
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, false);
    }

    /// Bulk-copy rows with negated weights.
    pub fn append_batch_negated(&mut self, src: &OwnedBatch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.init_strides_from_batch(src);
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, true);
    }

    fn append_rows_inner(&mut self, src: &OwnedBatch, start: usize, end: usize, negate: bool) {
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

    /// Write this batch as a shard file directly to disk.
    pub fn write_as_shard(&self, path: &CStr, table_id: u32) -> i32 {
        let regions = self.regions();
        shard_file::write_shard_streaming(libc::AT_FDCWD, path, table_id, self.count as u32, &regions, true)
    }
}

impl Drop for OwnedBatch {
    fn drop(&mut self) {
        super::batch_pool::recycle_buf(std::mem::take(&mut self.data));
        super::batch_pool::recycle_buf(std::mem::take(&mut self.blob));
    }
}

impl Clone for OwnedBatch {
    fn clone(&self) -> Self { self.clone_batch() }
}

impl ColumnarSource for OwnedBatch {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        OwnedBatch::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        OwnedBatch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] { &self.blob }
}

/// Allocate a single contiguous arena, run a merge/copy operation via
/// DirectWriter, and return the arena as an OwnedBatch — zero copy-out.
///
/// In steady state the arena is recycled from the thread-local buffer pool,
/// so this path allocates nothing.
pub fn write_to_owned_batch(
    schema: &SchemaDescriptor,
    max_rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut merge::DirectWriter),
) -> OwnedBatch {
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

    OwnedBatch {
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

/// Merge N sorted MemBatch views into a single consolidated OwnedBatch.
fn consolidate_batches(
    batches: &[MemBatch],
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let num_payload_cols = schema.num_columns as usize - 1;
    if batches.is_empty() {
        return OwnedBatch::empty(num_payload_cols);
    }

    let total_rows: usize = batches.iter().map(|b| b.count).sum();
    let total_blob: usize = batches.iter().map(|b| b.blob.len()).sum();
    if total_rows == 0 {
        return OwnedBatch::empty(num_payload_cols);
    }

    let result = write_to_owned_batch(schema, total_rows, total_blob, |writer| {
        if batches.len() == 1 {
            merge::sort_and_consolidate(&batches[0], schema, writer);
        } else {
            merge::merge_batches(batches, schema, writer);
        }
    });
    result
}


pub struct MemTable {
    runs: Vec<OwnedBatch>,
    bloom: BloomFilter,
    schema: SchemaDescriptor,
    max_bytes: usize,
    total_row_count: usize,
    runs_bytes: usize,
    cached_consolidated: Option<Arc<OwnedBatch>>,
    // Last lookup result (set by lookup_pk, read by found_* accessors)
    found_run: usize,
    found_row: usize,
    has_found: bool,
}

impl MemTable {
    pub fn new(schema: SchemaDescriptor, max_bytes: usize) -> Self {
        let capacity = (max_bytes / 40).max(16); // rough estimate
        MemTable {
            runs: Vec::with_capacity(8),
            bloom: BloomFilter::new(capacity as u32),
            schema,
            max_bytes,
            total_row_count: 0,
            runs_bytes: 0,
            cached_consolidated: None,
            found_run: 0,
            found_row: 0,
            has_found: false,
        }
    }

    /// Append a pre-sorted batch as a new run.
    pub fn upsert_sorted_batch(&mut self, batch: OwnedBatch) -> Result<(), i32> {
        if batch.count == 0 {
            return Ok(());
        }
        self.check_capacity()?;

        // Add all PKs to bloom
        for i in 0..batch.count {
            self.bloom.add(batch.get_pk(i));
        }

        self.total_row_count += batch.count;
        self.runs_bytes += batch.total_bytes();
        self.runs.push(batch);
        self.invalidate_cache();
        self.maybe_inline_consolidate();
        Ok(())
    }

    /// Add a single PK to the bloom filter (for accumulator rows).
    pub fn bloom_add(&mut self, key: u128) {
        self.bloom.add(key);
    }

    pub fn may_contain_pk(&self, key: u128) -> bool {
        self.bloom.may_contain(key)
    }

    pub fn should_flush(&self) -> bool {
        self.runs_bytes > self.max_bytes * 3 / 4
    }

    pub fn is_empty(&self) -> bool {
        self.total_row_count == 0
    }

    #[cfg(test)]
    pub fn total_row_count(&self) -> usize {
        self.total_row_count
    }

    /// Get a consolidated snapshot.  Caches the merged result of all runs.
    /// Returns an `Arc<OwnedBatch>` — cheap to clone for multiple consumers.
    pub fn get_snapshot(&mut self) -> Arc<OwnedBatch> {
        let num_payload_cols = self.schema.num_columns as usize - 1;

        if self.cached_consolidated.is_none() && !self.runs.is_empty() {
            let batches: Vec<MemBatch> =
                self.runs.iter().map(|r| r.as_mem_batch()).collect();
            let consolidated = consolidate_batches(&batches, &self.schema);
            self.cached_consolidated = Some(Arc::new(consolidated));
        }

        match &self.cached_consolidated {
            Some(arc) => Arc::clone(arc),
            None => Arc::new(OwnedBatch::empty(num_payload_cols)),
        }
    }

    /// Look up a PK across all sorted runs.
    ///
    /// Returns `(net_weight, Some((run_idx, row_idx)))` if found, else
    /// `(0, None)`.
    pub fn lookup_pk(&mut self, key: u128) -> (i64, bool) {
        let mut total_w: i64 = 0;
        self.has_found = false;

        for (ri, run) in self.runs.iter().enumerate() {
            let mut lo = run.find_lower_bound(key);
            while lo < run.count && run.get_pk(lo) == key {
                total_w += run.get_weight(lo);
                if !self.has_found {
                    self.found_run = ri;
                    self.found_row = lo;
                    self.has_found = true;
                }
                lo += 1;
            }
        }

        (total_w, self.has_found)
    }

    fn found_entry(&self) -> Option<(&OwnedBatch, usize)> {
        if self.has_found && self.found_run < self.runs.len() {
            Some((&self.runs[self.found_run], self.found_row))
        } else {
            None
        }
    }

    pub fn found_col_ptr(&self, payload_col: usize, col_size: usize) -> *const u8 {
        match self.found_entry() {
            Some((run, row)) => run.get_col_ptr(row, payload_col, col_size).as_ptr(),
            None => std::ptr::null(),
        }
    }

    pub fn found_null_word(&self) -> u64 {
        match self.found_entry() {
            Some((run, row)) => run.get_null_word(row),
            None => 0,
        }
    }

    pub fn found_blob_ptr(&self) -> *const u8 {
        match self.found_entry() {
            Some((run, _)) => run.blob.as_ptr(),
            None => std::ptr::null(),
        }
    }

    /// Find the net weight for rows matching both PK and full payload.
    pub fn find_weight_for_row<S: super::columnar::ColumnarSource>(
        &self,
        key: u128,
        ref_source: &S,
        ref_row: usize,
    ) -> i64 {
        let mut total_w: i64 = 0;

        for run in &self.runs {
            let mut lo = run.find_lower_bound(key);
            while lo < run.count && run.get_pk(lo) == key {
                let ord = columnar::compare_rows(
                    &self.schema, run, lo, ref_source, ref_row,
                );
                if ord == Ordering::Equal {
                    total_w += run.get_weight(lo);
                }
                lo += 1;
            }
        }

        total_w
    }

    /// Find the first memtable row whose (PK, payload) has positive net weight
    /// across all runs.  Sets `found_run`/`found_row`/`has_found` on success.
    /// Returns true if such a row was found, false otherwise.
    ///
    /// Used by `Table::retract_pk` to locate the live row for an UPDATE+DELETE
    /// sequence where the old payload has been cancelled but the new payload
    /// is still positive.
    pub fn find_positive_payload_row(&mut self, key: u128) -> bool {
        // Pass 1: collect all PK-matching (run_idx, row_idx) pairs.
        // The iterator borrow on self.runs is fully dropped before pass 2,
        // avoiding any aliasing issue when find_weight_for_row also borrows self.runs.
        let mut candidates: Vec<(usize, usize)> = Vec::new();
        for (ri, run) in self.runs.iter().enumerate() {
            let mut lo = run.find_lower_bound(key);
            while lo < run.count && run.get_pk(lo) == key {
                candidates.push((ri, lo));
                lo += 1;
            }
        }

        // Pass 2: for each candidate, compute net (PK, payload) weight.
        // The first candidate with positive net weight is the live row.
        for &(ri, row_idx) in &candidates {
            let mb = self.runs[ri].as_mem_batch();
            let net_w = self.find_weight_for_row(key, &mb, row_idx);
            if net_w > 0 {
                self.found_run = ri;
                self.found_row = row_idx;
                self.has_found = true;
                return true;
            }
        }

        self.has_found = false;
        false
    }

    /// Consolidate all runs and write to a shard file.
    ///
    /// Returns 0 on success, -1 if empty (no file written), or a negative
    /// error code from `write_shard_at`.
    pub fn flush(
        &mut self,
        dirfd: libc::c_int,
        basename: &CStr,
        table_id: u32,
        durable: bool,
    ) -> i32 {
        let snapshot = self.get_snapshot();
        if snapshot.count == 0 {
            return -1; // nothing to write
        }

        let regions = snapshot.regions();
        let image = shard_file::build_shard_image(
            table_id,
            snapshot.count as u32,
            &regions,
        );
        shard_file::write_shard_at(dirfd, basename, &image, durable)
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Clear all runs, bloom filter, and cache.  Ready for reuse.
    pub fn reset(&mut self) {
        self.runs.clear();
        self.runs_bytes = 0;
        self.total_row_count = 0;
        self.cached_consolidated = None;
        self.has_found = false;
        self.bloom.reset();
    }


    fn invalidate_cache(&mut self) {
        self.cached_consolidated = None;
    }

    /// If the run count has reached the threshold, merge all runs into a
    /// single consolidated run.  Bounds the cost of `get_snapshot()` and
    /// `lookup_pk()`, and eliminates weight-cancelled rows early.
    fn maybe_inline_consolidate(&mut self) {
        if self.runs.len() < INLINE_CONSOLIDATE_THRESHOLD {
            return;
        }
        let batches: Vec<MemBatch> =
            self.runs.iter().map(|r| r.as_mem_batch()).collect();
        let merged = consolidate_batches(&batches, &self.schema);
        self.runs.clear();
        self.runs_bytes = merged.total_bytes();
        self.total_row_count = merged.count;
        if merged.count > 0 {
            self.runs.push(merged);
        }
        self.has_found = false;
        self.invalidate_cache();
    }

    fn check_capacity(&self) -> Result<(), i32> {
        if self.runs_bytes > self.max_bytes {
            return Err(ERR_CAPACITY);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

    fn make_u64_i64_schema() -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        // Col 0: PK (U64)
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        // Col 1: payload (I64)
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    /// Build a sorted OwnedBatch from (pk, weight, payload) triples.
    /// Assumes triples are already sorted by pk.
    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }

        b.sorted = true;
        b.consolidated = false;
        b
    }

    #[test]
    fn owned_batch_roundtrip() {
        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        assert_eq!(batch.count, 2);
        assert_eq!(batch.get_pk(0), 10);
        assert_eq!(batch.get_pk(1), 20);

        let mb = batch.as_mem_batch();
        assert_eq!(mb.count, 2);
        assert_eq!(mb.get_pk(0), 10);
        assert_eq!(mb.get_weight(1), 1);
    }

    #[test]
    fn memtable_upsert_and_snapshot() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        assert!(mt.is_empty());

        // Upsert two runs
        let b1 = make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 1, 200), (30, -1, 300)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();
        assert_eq!(mt.total_row_count(), 4);

        // Snapshot should consolidate: PK 30 has +1 -1 = 0 (ghost)
        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 2); // PK 10 and 20 survive
        assert_eq!(snap.get_pk(0), 10);
        assert_eq!(snap.get_pk(1), 20);
    }

    #[test]
    fn memtable_snapshot_caching() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let s1 = mt.get_snapshot();
        let s2 = mt.get_snapshot();
        // Should be the same Arc (cached)
        assert!(Arc::ptr_eq(&s1, &s2));

        // After new upsert, cache is invalidated
        let b2 = make_batch(&schema, &[(20, 1, 200)]);
        mt.upsert_sorted_batch(b2).unwrap();
        let s3 = mt.get_snapshot();
        assert!(!Arc::ptr_eq(&s1, &s3));
        assert_eq!(s3.count, 2);
    }

    #[test]
    fn memtable_lookup_pk() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let b2 = make_batch(&schema, &[(20, 2, 200)]); // PK 20 appears in two runs
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        let (w, found) = mt.lookup_pk(20);
        assert_eq!(w, 3); // 1 + 2
        assert!(found);

        let (w, found) = mt.lookup_pk(99);
        assert_eq!(w, 0);
        assert!(!found);
    }

    #[test]
    fn memtable_bloom() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        assert!(mt.may_contain_pk(10));
        assert!(mt.may_contain_pk(20));
        // 99 might be a false positive, but definitely not in data
    }

    #[test]
    fn memtable_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();
        assert!(!mt.is_empty());

        mt.reset();
        assert!(mt.is_empty());
        assert_eq!(mt.total_row_count(), 0);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn snapshot_survives_reset() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 1);

        mt.reset();
        // Snapshot still valid (Arc keeps data alive)
        assert_eq!(snap.count, 1);
        assert_eq!(snap.get_pk(0), 10);
    }

    #[test]
    fn memtable_capacity_error() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 64); // tiny capacity

        // First upsert fills past max_bytes (4 rows × 40 bytes = 160 > 64)
        let b1 = make_batch(
            &schema,
            &[(10, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)],
        );
        mt.upsert_sorted_batch(b1).unwrap(); // OK — check fires before adding

        // Second upsert fails: runs_bytes (160) > max_bytes (64)
        let b2 = make_batch(&schema, &[(50, 1, 500)]);
        let rc = mt.upsert_sorted_batch(b2);
        assert_eq!(rc, Err(ERR_CAPACITY));
    }

    #[test]
    fn memtable_find_weight_for_row() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // PK 10 with payload 100
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        // PK 10 with payload 200 (different row identity)
        let b2 = make_batch(&schema, &[(10, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // Search for PK 10, payload 100 — should find weight 1
        let ref_batch = make_batch(&schema, &[(10, 1, 100)]);
        let ref_mb = ref_batch.as_mem_batch();
        let w = mt.find_weight_for_row(10, &ref_mb, 0);
        assert_eq!(w, 1);

        // Search for PK 10, payload 200 — should find weight 1
        let ref_batch2 = make_batch(&schema, &[(10, 1, 200)]);
        let ref_mb2 = ref_batch2.as_mem_batch();
        let w2 = mt.find_weight_for_row(10, &ref_mb2, 0);
        assert_eq!(w2, 1);

        // Search for PK 10, payload 999 — should find weight 0
        let ref_batch3 = make_batch(&schema, &[(10, 1, 999)]);
        let ref_mb3 = ref_batch3.as_mem_batch();
        let w3 = mt.find_weight_for_row(10, &ref_mb3, 0);
        assert_eq!(w3, 0);
    }

    #[test]
    fn memtable_flush_to_shard() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let b1 = make_batch(&schema, &[(10, 1, 100), (20, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            libc::open(
                dir.path().as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDONLY | libc::O_DIRECTORY,
            )
        };
        assert!(dirfd >= 0);

        let name = std::ffi::CString::new("test_shard.db").unwrap();
        let rc = mt.flush(dirfd, &name, 42, false);
        assert_eq!(rc, 0);

        // Verify shard file exists
        let shard_path = dir.path().join("test_shard.db");
        assert!(shard_path.exists());
        assert!(std::fs::metadata(&shard_path).unwrap().len() > 0);

        unsafe { libc::close(dirfd); }
    }

    #[test]
    fn memtable_flush_empty_returns_neg1() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);
        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            libc::open(
                dir.path().as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDONLY | libc::O_DIRECTORY,
            )
        };
        assert!(dirfd >= 0);

        let name = std::ffi::CString::new("empty.db").unwrap();
        let rc = mt.flush(dirfd, &name, 42, false);
        assert_eq!(rc, -1);
        unsafe { libc::close(dirfd); }
    }

    #[test]
    fn batch_append_batch() {
        let schema = make_u64_i64_schema();
        let src = make_batch(&schema, &[(10, 1, 100), (20, 1, 200), (30, 1, 300)]);
        let mut dst = OwnedBatch::with_schema(schema, 8);

        // Append all rows
        dst.append_batch(&src, 0, 3);
        assert_eq!(dst.count, 3);
        assert_eq!(dst.get_pk(0), 10);
        assert_eq!(dst.get_pk(2), 30);
        assert_eq!(dst.get_weight(1), 1);

        // Append subset
        dst.clear();
        dst.append_batch(&src, 1, 2);
        assert_eq!(dst.count, 1);
        assert_eq!(dst.get_pk(0), 20);
    }

    #[test]
    fn batch_append_batch_negated() {
        let schema = make_u64_i64_schema();
        let src = make_batch(&schema, &[(10, 1, 100), (20, 2, 200)]);
        let mut dst = OwnedBatch::with_schema(schema, 8);

        dst.append_batch_negated(&src, 0, 2);
        assert_eq!(dst.count, 2);
        assert_eq!(dst.get_weight(0), -1);
        assert_eq!(dst.get_weight(1), -2);
        assert_eq!(dst.get_pk(0), 10);
    }

    #[test]
    fn batch_append_batch_from_empty_exceeds_initial_capacity() {
        // Regression: bulk append into an empty() batch must not spin when n > initial capacity.
        let schema = make_u64_i64_schema();
        let rows: Vec<(u64, i64, i64)> = (1u64..=200).map(|i| (i, 1i64, (i * 10) as i64)).collect();
        let src = make_batch(&schema, &rows);

        let ncols = schema.num_columns as usize - 1;
        let mut dst = OwnedBatch::empty(ncols);
        dst.schema = Some(schema);

        dst.append_batch(&src, 0, src.count);

        assert_eq!(dst.count, 200);
        for i in 0..200usize {
            assert_eq!(dst.get_pk(i), (i + 1) as u128);
        }
    }

    #[test]
    fn batch_region_access() {
        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(10, 1, 100)]);

        // Schema has 2 columns: PK (U64) + payload (I64)
        // Regions: pk_lo(0), pk_hi(1), weight(2), null(3), col0(4), blob(5)
        assert_eq!(batch.num_regions_total(), 6);
        assert_eq!(batch.region_size(0), 8); // pk_lo: 1 row * 8 bytes
        assert_eq!(batch.region_size(4), 8); // col0: 1 row * 8 bytes
        assert!(!batch.region_ptr(0).is_null());
    }

    #[test]
    fn batch_clear() {
        let schema = make_u64_i64_schema();
        let mut batch = make_batch(&schema, &[(10, 1, 100)]);
        assert_eq!(batch.count, 1);

        batch.clear();
        assert_eq!(batch.count, 0);
        assert!(batch.sorted);
        assert!(batch.consolidated);
    }

    /// Schema matching reduce output: U128 PK (pk_index=0) + I64 group_val + I64 agg_val
    fn make_reduce_schema() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn::new(0, 0); 64],
        };
        sd.columns[0] = SchemaColumn::new(type_code::U128, 0); // U128 PK
        sd.columns[1] = SchemaColumn::new(type_code::I64, 0);  // I64 group_val
        sd.columns[2] = SchemaColumn::new(type_code::I64, 0);  // I64 agg_val
        sd
    }

    /// Build a 3-column OwnedBatch from (pk_lo, pk_hi, weight, group_val, agg_val) tuples.
    fn make_reduce_batch(rows: &[(u64, u64, i64, i64, i64)]) -> OwnedBatch {
        let schema = make_reduce_schema();
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(schema, n.max(1));

        for &(plo, phi, w, gv, av) in rows {
            b.extend_pk_lo(&plo.to_le_bytes());
            b.extend_pk_hi(&phi.to_le_bytes());
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &gv.to_le_bytes());
            b.extend_col(1, &av.to_le_bytes());
            b.count += 1;
        }

        b.sorted = true;
        b.consolidated = false;
        b
    }

    /// Reproduce the reduce output pattern: insertion + retraction across ticks.
    /// After consolidation, only the LAST tick's aggregate should survive.
    #[test]
    fn test_reduce_output_consolidation_3col() {
        let schema = make_reduce_schema();

        // Tick 1: insert (PK=0, group=0, sum=5000, w=+1)
        let run1 = make_reduce_batch(&[(0, 0, 1, 0, 5000)]);
        // Tick 2: retract old, insert new
        let run2 = make_reduce_batch(&[
            (0, 0, -1, 0, 5000),   // retract sum=5000
            (0, 0,  1, 0, 10000),  // insert sum=10000
        ]);
        // Tick 3: retract old, insert new
        let run3 = make_reduce_batch(&[
            (0, 0, -1, 0, 10000),  // retract sum=10000
            (0, 0,  1, 0, 15000),  // insert sum=15000
        ]);

        let batches: Vec<super::merge::MemBatch> = vec![
            run1.as_mem_batch(),
            run2.as_mem_batch(),
            run3.as_mem_batch(),
        ];

        let consolidated = consolidate_batches(&batches, &schema);

        // After consolidation: only (PK=0, group=0, sum=15000, w=+1) should remain
        assert_eq!(consolidated.count, 1,
            "expected 1 row after consolidation, got {}", consolidated.count);
        assert_eq!(consolidated.get_pk(0), 0);
        assert_eq!(consolidated.get_weight(0), 1);
        // Check agg_val (payload column 1) = 15000
        let agg_bytes = consolidated.get_col_ptr(0, 1, 8);
        let agg_val = i64::from_le_bytes(agg_bytes.try_into().unwrap());
        assert_eq!(agg_val, 15000, "expected agg_val=15000, got {}", agg_val);
    }

    // ── append_row_simple tests ──────────────────────────────────────────

    fn make_schema_cols(cols: &[(u8, u8)], pk_index: u32) -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        for (i, &(tc, nullable)) in cols.iter().enumerate() {
            columns[i] = SchemaColumn::new(tc, nullable);
        }
        SchemaDescriptor { num_columns: cols.len() as u32, pk_index, columns }
    }

    fn decode_str(batch: &OwnedBatch, row: usize, payload_col: usize) -> Vec<u8> {
        let raw = batch.get_col_ptr(row, payload_col, 16);
        let st: [u8; 16] = raw.try_into().unwrap();
        crate::schema::decode_german_string(&st, &batch.blob)
    }

    #[test]
    fn test_append_row_simple_nullable_string() {
        // Schema: U64(pk=0), STRING(nullable)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),
            (type_code::STRING, 1),
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 4);

        // Row 0: non-null string "not null"
        let s = b"not null";
        let lo = [0i64]; // not used for STRING
        let hi = [0u64];
        let ptrs = [s.as_ptr()];
        let lens = [s.len() as u32];
        unsafe {
            batch.append_row_simple(1, 1, 0, &lo, &hi, &ptrs, &lens);
        }

        // Row 1: null string (null_word bit 0 set)
        let null_ptr: *const u8 = std::ptr::null();
        let ptrs2 = [null_ptr];
        let lens2 = [0u32];
        unsafe {
            batch.append_row_simple(2, 1, 1, &[0i64], &[0u64], &ptrs2, &lens2);
        }

        assert_eq!(batch.count, 2);
        // Row 0: string decodes correctly
        assert_eq!(decode_str(&batch, 0, 0), b"not null");
        // Row 0: not null
        assert_eq!(batch.get_null_word(0) & 1, 0);
        // Row 1: null bit set
        assert_eq!(batch.get_null_word(1) & 1, 1);
        // Row 1: col data is zeroed
        let raw = batch.get_col_ptr(1, 0, 16);
        assert!(raw.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_append_row_simple_multi_string() {
        // Schema: U64(pk=0), STRING(name), STRING(desc)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),
            (type_code::STRING, 0),
            (type_code::STRING, 0),
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 4);

        let cases: &[(&[u8], &[u8])] = &[
            (b"Alice", b"short"),
            (b"Bob has a long name!", b"Also quite a long description"),
            (b"", b"nonempty"),
            (b"mix", b"another long one for blob storage"),
        ];

        for (pk, (name, desc)) in cases.iter().enumerate() {
            let lo = [0i64, 0i64];
            let hi = [0u64, 0u64];
            let ptrs = [name.as_ptr(), desc.as_ptr()];
            let lens = [name.len() as u32, desc.len() as u32];
            unsafe {
                batch.append_row_simple(pk as u128, 1, 0, &lo, &hi, &ptrs, &lens);
            }
        }

        assert_eq!(batch.count, 4);
        for (i, (name, desc)) in cases.iter().enumerate() {
            assert_eq!(decode_str(&batch, i, 0), *name, "row {i} name mismatch");
            assert_eq!(decode_str(&batch, i, 1), *desc, "row {i} desc mismatch");
        }
    }

    #[test]
    fn test_append_row_simple_all_types() {
        // Schema: U64(pk=0), then one of each remaining type
        // Payload cols (pi): U8(0), I8(1), U16(2), I16(3), U32(4), I32(5),
        //                     F32(6), U64(7), I64(8), F64(9), STRING(10), U128(11)
        let schema = make_schema_cols(&[
            (type_code::U64, 0),    // pk
            (type_code::U8, 0),     // pi 0
            (type_code::I8, 0),     // pi 1
            (type_code::U16, 0),    // pi 2
            (type_code::I16, 0),    // pi 3
            (type_code::U32, 0),    // pi 4
            (type_code::I32, 0),    // pi 5
            (type_code::F32, 0),    // pi 6
            (type_code::U64, 0),    // pi 7
            (type_code::I64, 0),    // pi 8
            (type_code::F64, 0),    // pi 9
            (type_code::STRING, 0), // pi 10
            (type_code::U128, 0),   // pi 11
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 1);

        let n = 12;
        let mut lo = vec![0i64; n];
        let mut hi = vec![0u64; n];
        let mut ptrs = vec![std::ptr::null::<u8>(); n];
        let mut lens = vec![0u32; n];

        lo[0] = 42;        // U8: 42
        lo[1] = -7;        // I8: -7
        lo[2] = 1000;      // U16: 1000
        lo[3] = -500;      // I16: -500
        lo[4] = 70000;     // U32: 70000
        lo[5] = -12345;    // I32: -12345
        // F32: 3.14 → store as f64 bit pattern (float2longlong convention)
        lo[6] = f64::to_bits(3.14f64) as i64;
        lo[7] = 0x1234_5678_9ABC_DEF0u64 as i64;  // U64
        lo[8] = -99999;    // I64
        // F64: 2.718281828 → store as bit pattern
        lo[9] = f64::to_bits(2.718281828f64) as i64;
        // STRING: "hello world!"
        let s = b"hello world!";
        ptrs[10] = s.as_ptr();
        lens[10] = s.len() as u32;
        // U128: lo=0xDEADBEEF, hi=0xCAFEBABE
        lo[11] = 0xDEADBEEFu64 as i64;
        hi[11] = 0xCAFEBABE;

        unsafe {
            batch.append_row_simple(100, 1, 0, &lo, &hi, &ptrs, &lens);
        }

        assert_eq!(batch.count, 1);

        // U8
        assert_eq!(batch.get_col_ptr(0, 0, 1), &[42]);
        // I8
        assert_eq!(batch.get_col_ptr(0, 1, 1), &[(-7i8) as u8]);
        // U16
        assert_eq!(batch.get_col_ptr(0, 2, 2), &1000u16.to_le_bytes());
        // I16
        assert_eq!(batch.get_col_ptr(0, 3, 2), &(-500i16).to_le_bytes());
        // U32
        assert_eq!(batch.get_col_ptr(0, 4, 4), &70000u32.to_le_bytes());
        // I32
        assert_eq!(batch.get_col_ptr(0, 5, 4), &(-12345i32).to_le_bytes());
        // F32: 3.14f64 as f32
        let f32_bytes = batch.get_col_ptr(0, 6, 4);
        let f32_val = f32::from_le_bytes(f32_bytes.try_into().unwrap());
        assert!((f32_val - 3.14f32).abs() < 1e-5, "f32: {f32_val}");
        // U64
        assert_eq!(batch.get_col_ptr(0, 7, 8), &0x1234_5678_9ABC_DEF0u64.to_le_bytes());
        // I64
        assert_eq!(batch.get_col_ptr(0, 8, 8), &(-99999i64).to_le_bytes());
        // F64
        let f64_bytes = batch.get_col_ptr(0, 9, 8);
        let f64_val = f64::from_le_bytes(f64_bytes.try_into().unwrap());
        assert!((f64_val - 2.718281828).abs() < 1e-9, "f64: {f64_val}");
        // STRING
        assert_eq!(decode_str(&batch, 0, 10), b"hello world!");
        // U128
        let u128_bytes = batch.get_col_ptr(0, 11, 16);
        let u128_lo = u64::from_le_bytes(u128_bytes[0..8].try_into().unwrap());
        let u128_hi = u64::from_le_bytes(u128_bytes[8..16].try_into().unwrap());
        assert_eq!(u128_lo, 0xDEADBEEF);
        assert_eq!(u128_hi, 0xCAFEBABE);
    }

    /// Verify that `append_row` substitutes an empty string when the
    /// declared length would read past the end of `blob_src`. This matches
    /// `relocate_string_cell` and prevents silent corruption from emitting
    /// unrelated bytes from the start of the blob.
    #[test]
    fn test_append_row_blob_length_header() {
        let schema = make_schema_cols(&[
            (type_code::U64, 0),    // PK
            (type_code::STRING, 0), // STRING payload
        ], 0);
        let mut batch = OwnedBatch::with_schema(schema, 1);

        // Build a German String struct with length=20 but only 5 blob bytes available.
        // This triggers the malformed-wire-data fallback branch in append_row.
        let mut gs_struct = [0u8; 16];
        gs_struct[0..4].copy_from_slice(&20u32.to_le_bytes()); // declared length = 20
        gs_struct[4..8].copy_from_slice(&0u32.to_le_bytes());  // prefix bytes = 0
        gs_struct[8..16].copy_from_slice(&0u64.to_le_bytes()); // blob offset = 0

        let blob_src = b"hello"; // only 5 bytes — out of bounds for length=20

        let col_ptrs = [gs_struct.as_ptr()];
        let col_sizes = [16u32];
        unsafe {
            batch.append_row(1, 1, 0, &col_ptrs, &col_sizes, blob_src);
        }

        // The stored German String header must have length=0 (empty-string
        // fallback), not the declared length=20 nor a truncated length=5.
        // Emitting an empty string is the only safe response to malformed
        // wire data; anything else silently corrupts the row.
        let stored_gs = batch.get_col_ptr(0, 0, 16);
        let stored_len = u32::from_le_bytes(stored_gs[0..4].try_into().unwrap());
        assert_eq!(stored_len, 0, "malformed long-string should fall back to empty");

        // No bytes copied into the blob arena.
        assert!(batch.blob.is_empty());
    }

    /// Verify that `find_positive_payload_row` locates the row with positive
    /// net weight, skipping rows whose (PK, payload) is cancelled out.
    #[test]
    fn test_find_positive_payload_row() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Run 0: INSERT (PK=10, val=100, weight=+1)
        let b1 = make_batch(&schema, &[(10, 1, 100)]);
        // Run 1: UPDATE delta — retract val=100, insert val=200
        let b2 = make_batch(&schema, &[(10, -1, 100), (10, 1, 200)]);
        mt.upsert_sorted_batch(b1).unwrap();
        mt.upsert_sorted_batch(b2).unwrap();

        // Net weights: val=100 → 1-1=0, val=200 → 1 (positive)
        let found = mt.find_positive_payload_row(10);
        assert!(found, "should find a live row");
        assert!(mt.has_found);

        // The found row should have payload = 200
        let col_ptr = mt.found_col_ptr(0, 8);
        assert!(!col_ptr.is_null());
        let val = i64::from_le_bytes(
            unsafe { std::slice::from_raw_parts(col_ptr, 8) }.try_into().unwrap(),
        );
        assert_eq!(val, 200, "found row should be the live val=200 row, not the retracted val=100");

        // PK with no rows at all → not found
        let found2 = mt.find_positive_payload_row(99);
        assert!(!found2);
        assert!(!mt.has_found);
    }

    #[test]
    fn test_inline_consolidate_basic() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // Push 15 batches — no consolidation yet
        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert_eq!(mt.runs.len(), 15);

        // 16th batch triggers consolidation
        let b16 = make_batch(&schema, &[(16, 1, 1600)]);
        mt.upsert_sorted_batch(b16).unwrap();
        assert_eq!(mt.runs.len(), 1);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 16);
        assert_eq!(snap.get_pk(0), 1);
        assert_eq!(snap.get_pk(15), 16);
    }

    #[test]
    fn test_inline_consolidate_ghost_elimination() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // 14 distinct insertions
        for i in 0..14u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // Retract PK 1
        let retract = make_batch(&schema, &[(1, -1, 100)]);
        mt.upsert_sorted_batch(retract).unwrap();
        // 16th batch — triggers consolidation
        let b16 = make_batch(&schema, &[(15, 1, 1500)]);
        mt.upsert_sorted_batch(b16).unwrap();

        assert_eq!(mt.runs.len(), 1);
        let snap = mt.get_snapshot();
        // PK 1 cancelled out: 14 - 1 + 1 new = 14 survive
        assert_eq!(snap.count, 14);
        // PK 1 should be gone, first row is PK 2
        assert_eq!(snap.get_pk(0), 2);
    }

    #[test]
    fn test_inline_consolidate_all_cancelled() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // 8 insertions + 8 matching retractions = 16 batches
        for i in 0..8u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        for i in 0..8u64 {
            let b = make_batch(&schema, &[(i + 1, -1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }

        // All cancelled: runs should be empty
        assert_eq!(mt.runs.len(), 0);
        assert!(mt.is_empty());
        assert_eq!(mt.total_row_count(), 0);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn test_inline_consolidate_below_threshold() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // 15 < 16, no consolidation
        assert_eq!(mt.runs.len(), 15);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 15);
    }

    #[test]
    fn test_inline_consolidate_repeated() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        // First 16 → consolidation fires → 1 run
        for i in 0..16u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert_eq!(mt.runs.len(), 1);

        // 14 more → 1 + 14 = 15 runs (no consolidation yet)
        for i in 16..30u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert_eq!(mt.runs.len(), 15);

        // One more → 16 runs → consolidation fires again → 1 run
        let b31 = make_batch(&schema, &[(31, 1, 3100)]);
        mt.upsert_sorted_batch(b31).unwrap();
        assert_eq!(mt.runs.len(), 1);

        let snap = mt.get_snapshot();
        assert_eq!(snap.count, 31);
    }

    #[test]
    fn test_inline_consolidate_should_flush() {
        let schema = make_u64_i64_schema();
        // Each 1-row batch ≈ 40 bytes.  max_bytes = 700 → threshold = 525.
        // After 14 insertions: runs_bytes ≈ 560 > 525 → should_flush true.
        // After 8 retractions (total 16 batches): consolidation fires,
        // net 6 rows ≈ 240 < 525 → should_flush false.
        let mut mt = MemTable::new(schema, 700);

        // 8 insertions
        for i in 0..8u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }

        // 6 more insertions → 14 runs, runs_bytes ≈ 560 > 525
        for i in 8..14u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        assert!(mt.should_flush(), "gross bytes should exceed threshold");

        // Retract PKs 1-2 → 16 batches total, triggers consolidation
        // Net: 12 rows survive (PKs 3-14)
        let r1 = make_batch(&schema, &[(1, -1, 100)]);
        let r2 = make_batch(&schema, &[(2, -1, 200)]);
        mt.upsert_sorted_batch(r1).unwrap();
        mt.upsert_sorted_batch(r2).unwrap();

        // After consolidation: net 12 rows ≈ 480 < 525
        assert!(!mt.should_flush(), "net state should be under threshold");
    }

    #[test]
    fn test_inline_consolidate_has_found_cleared() {
        let schema = make_u64_i64_schema();
        let mut mt = MemTable::new(schema, 1 << 20);

        for i in 0..15u64 {
            let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
            mt.upsert_sorted_batch(b).unwrap();
        }
        // lookup_pk sets has_found
        let (w, found) = mt.lookup_pk(5);
        assert_eq!(w, 1);
        assert!(found);
        assert!(mt.has_found);

        // 16th batch triggers consolidation → has_found cleared
        let b16 = make_batch(&schema, &[(16, 1, 1600)]);
        mt.upsert_sorted_batch(b16).unwrap();
        assert!(!mt.has_found);
    }

    /// Bug 5: relocate_string_cell must not panic when blob offset is past end.
    #[test]
    fn test_relocate_string_cell_out_of_bounds() {
        // Build a 16-byte German string struct for a long string (length > 12)
        // with an out-of-bounds blob offset.
        let length: u32 = 20;
        let prefix = [b'A'; 4];
        let bad_offset: u64 = 9999; // way past any blob
        let mut src_struct = [0u8; 16];
        src_struct[0..4].copy_from_slice(&length.to_le_bytes());
        src_struct[4..8].copy_from_slice(&prefix);
        src_struct[8..16].copy_from_slice(&bad_offset.to_le_bytes());

        let src_blob: &[u8] = &[0u8; 10]; // only 10 bytes, offset 9999 is OOB
        let mut dst = [0u8; 16];
        let mut dst_blob = Vec::new();

        // Must not panic
        super::relocate_string_cell(&src_struct, src_blob, &mut dst, &mut dst_blob);

        // The corrupted string should have length set to 0
        let out_len = u32::from_le_bytes(dst[0..4].try_into().unwrap());
        assert_eq!(out_len, 0, "corrupted string should be zero-length");
        assert!(dst_blob.is_empty(), "no blob data should have been copied");
    }

    /// Bug 5: append_row_from_source must not panic when blob offset is invalid.
    #[test]
    fn test_append_row_from_source_corrupted_blob() {
        use crate::schema::{SchemaColumn, type_code};
        use std::collections::HashMap;

        // Schema: col0 = PK (U64), col1 = STRING
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

        // Build a source OwnedBatch with a STRING column containing a bad blob offset.
        let mut src = OwnedBatch::with_schema(schema, 1);

        // German string struct: length=20, prefix="ABCD", offset=9999 (OOB)
        let length: u32 = 20;
        let prefix = [b'A', b'B', b'C', b'D'];
        let bad_offset: u64 = 9999;
        let mut str_struct = [0u8; 16];
        str_struct[0..4].copy_from_slice(&length.to_le_bytes());
        str_struct[4..8].copy_from_slice(&prefix);
        str_struct[8..16].copy_from_slice(&bad_offset.to_le_bytes());

        src.ensure_row_capacity();
        src.extend_pk_lo(&42u64.to_le_bytes());
        src.extend_pk_hi(&0u64.to_le_bytes());
        src.extend_weight(&1i64.to_le_bytes());
        src.extend_null_bmp(&0u64.to_le_bytes());
        src.extend_col(0, &str_struct);
        src.blob = vec![0u8; 10]; // only 10 bytes
        src.count = 1;

        // Build destination batch and call append_row_from_source
        let mut dst = OwnedBatch::with_schema(schema, 1);
        let mut blob_cache: HashMap<(u64, usize), usize> = HashMap::new();

        // Must not panic
        dst.append_row_from_source(42u128, 1, &src, 0, &mut blob_cache);

        assert_eq!(dst.count, 1);
        // The corrupted string should have length 0
        let out_len = u32::from_le_bytes(dst.col_data(0)[0..4].try_into().unwrap());
        assert_eq!(out_len, 0, "corrupted blob reference should produce zero-length string");
    }

    #[test]
    fn drop_recycles_buffers() {
        use crate::storage::batch_pool::{acquire_buf, recycle_buf};
        // Drain pool first.
        while acquire_buf().is_some() {}

        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let data_cap = batch.data.capacity();
        assert!(data_cap > 0);
        drop(batch);

        // Pool should now have at least one warm buffer (data buf).
        // May also have blob buf (if non-zero cap). Drain and check.
        let mut found = false;
        let mut drained = Vec::new();
        while let Some(buf) = acquire_buf() {
            if buf.capacity() >= data_cap { found = true; }
            drained.push(buf);
        }
        assert!(found, "pool should contain the recycled data buffer");
        for buf in drained { recycle_buf(buf); }
    }

    #[test]
    fn clone_drops_independently() {
        use crate::storage::batch_pool::acquire_buf;
        while acquire_buf().is_some() {}

        let schema = make_u64_i64_schema();
        let batch = make_batch(&schema, &[(1, 1, 10)]);
        let cloned = batch.clone();
        drop(batch);
        drop(cloned);

        // Both data + blob buffers from two batches should be in pool.
        // Original had data + blob, clone has data + blob.
        // Some may be zero-cap (empty blob), so just verify at least 2 buffers.
        let mut count = 0;
        while acquire_buf().is_some() { count += 1; }
        assert!(count >= 2, "expected at least 2 recycled buffers, got {}", count);
    }

    #[test]
    fn empty_batch_drop_is_noop() {
        use crate::storage::batch_pool::acquire_buf;
        while acquire_buf().is_some() {}

        let batch = OwnedBatch::empty(2);
        assert_eq!(batch.data.capacity(), 0);
        drop(batch);

        // Zero-capacity buffers should NOT be pooled.
        assert!(acquire_buf().is_none(), "empty batch should not pollute pool");
    }
}
