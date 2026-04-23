//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::atomic::{AtomicU64, Ordering};

use super::columnar::ColumnarSource;
use super::merge::{self, MemBatch};
use super::shard_file;
use crate::schema::SchemaDescriptor;
use crate::util::{read_i64_le, read_u64_le};

static BLOB_ID_CTR: AtomicU64 = AtomicU64::new(1);
#[inline(always)]
fn next_blob_id() -> u64 { BLOB_ID_CTR.fetch_add(1, Ordering::Relaxed) }

/// Minimum allocation size to request transparent hugepage backing.
/// Below this, hugepage promotion is impossible (no full 2MB PMD region).
const HUGEPAGE_THRESHOLD: usize = 2 * 1024 * 1024;

/// Maximum regions tracked in the `offsets`/`strides` arrays:
/// 4 fixed (pk_lo, pk_hi, weight, null_bmp) + up to 63 payload columns
/// (schema max is 64 total columns, 1 is the PK).  The blob is not in this
/// array; it lives in `self.blob` and is accounted for separately.
/// 4 + 63 = 67, rounded up to 68 to keep the array size as a multiple of 4.
pub(super) const MAX_BATCH_REGIONS: usize = 68;

// ── Region indices into `offsets` / `strides` ───────────────────────────────
//
// The first four regions are fixed (one 8-byte word per row each); payload
// columns start at index 4 and continue for `num_payload_cols()` slots.  Use
// these constants instead of bare numeric literals — naming the slots makes
// the layout self-documenting and turns an off-by-one in this file into a
// compile-time grep target.
const REG_PK_LO: usize = 0;
const REG_PK_HI: usize = 1;
const REG_WEIGHT: usize = 2;
const REG_NULL_BMP: usize = 3;
const REG_PAYLOAD_START: usize = 4;
/// Stride (in bytes) of each of the four fixed regions.
const FIXED_REGION_STRIDE: u8 = 8;

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
    let dest = crate::schema::relocate_german_string_vec(src_struct, src_blob, dst_blob, None);
    dst[..16].copy_from_slice(&dest);
}

/// Append payload strides from `schema` into `strides` starting at `start`.
/// Returns the next free index (i.e. `start + num_payload_cols`).
fn fill_payload_strides(
    schema: &SchemaDescriptor,
    strides: &mut [u8; MAX_BATCH_REGIONS],
    start: usize,
) -> usize {
    let pk = schema.pk_index as usize;
    let mut idx = start;
    for ci in 0..schema.num_columns as usize {
        if ci == pk { continue; }
        strides[idx] = schema.columns[ci].size;
        idx += 1;
    }
    idx
}

/// Build a strides array from a SchemaDescriptor.
pub(super) fn strides_from_schema(schema: &SchemaDescriptor) -> ([u8; MAX_BATCH_REGIONS], u8) {
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    strides[REG_PK_LO] = FIXED_REGION_STRIDE;
    strides[REG_PK_HI] = FIXED_REGION_STRIDE;
    strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
    strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
    let nr = fill_payload_strides(schema, &mut strides, REG_PAYLOAD_START);
    (strides, nr as u8)
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
    pub(crate) sorted: bool,
    pub(crate) consolidated: bool,
    pub schema: Option<SchemaDescriptor>,
    /// Identity token for blob-sharing: two batches with equal `blob_id` have
    /// identical blob content, making verbatim 16-byte German String struct
    /// copies safe.  Set by `share_blob_from` and checked by
    /// `append_batch_no_blob_reloc`.
    pub(crate) blob_id: u64,
}

impl Batch {
    // ── Constructors ────────────────────────────────────────────────────

    /// Create an empty, zero-allocation placeholder batch with only the four
    /// fixed regions (pk_lo/pk_hi/weight/null_bmp) described.  Payload strides
    /// remain zero, so the batch is NOT safe to populate via `extend_*` or
    /// `append_batch` — use `empty_with_schema` or `with_schema` for that.
    ///
    /// Intended for zero-row return values and swap-placeholder slots.
    pub fn empty(num_payload_cols: usize) -> Self {
        let nr = REG_PAYLOAD_START + num_payload_cols;
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        strides[REG_PK_LO] = FIXED_REGION_STRIDE;
        strides[REG_PK_HI] = FIXED_REGION_STRIDE;
        strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
        strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
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
            blob_id: next_blob_id(),
        }
    }

    /// Zero-allocation empty batch with strides pre-filled from `schema`.
    ///
    /// Use this — not `empty(npc)` — when the caller intends to populate the
    /// batch via `extend_*`, `append_batch`, or similar.  Strides and
    /// `schema` are set up front so no one-shot realloc fires on the first
    /// column write.
    pub fn empty_with_schema(schema: &SchemaDescriptor) -> Self {
        let (strides, nr) = strides_from_schema(schema);
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0u32; MAX_BATCH_REGIONS],
            strides,
            num_regions: nr,
            capacity: 0,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: Some(*schema),
            blob_id: next_blob_id(),
        }
    }

    /// Zero-allocation empty batch whose payload columns are `left_schema`'s
    /// non-PK columns followed by `right_schema`'s non-PK columns.  Used for
    /// join outputs ([left_PK, left_payload..., right_payload...]), which
    /// have no single `SchemaDescriptor` in the engine.
    pub fn empty_joined(
        left_schema: &SchemaDescriptor,
        right_schema: &SchemaDescriptor,
    ) -> Self {
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        strides[REG_PK_LO] = FIXED_REGION_STRIDE;
        strides[REG_PK_HI] = FIXED_REGION_STRIDE;
        strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
        strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
        let mid = fill_payload_strides(left_schema, &mut strides, REG_PAYLOAD_START);
        let out_idx = fill_payload_strides(right_schema, &mut strides, mid);
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0u32; MAX_BATCH_REGIONS],
            strides,
            num_regions: out_idx as u8,
            capacity: 0,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: None,
            blob_id: next_blob_id(),
        }
    }

    /// Create an empty batch with schema, pre-allocated for `initial_capacity` rows.
    pub fn with_schema(schema: SchemaDescriptor, initial_capacity: usize) -> Self {
        let cap = initial_capacity.max(1);
        let (strides, nr) = strides_from_schema(&schema);
        let (offsets, total_size) = compute_offsets(&strides, nr as usize, cap);

        // Invariant: `data` is fully zero-filled on return.  Some callers
        // (null-extend, scalar-func EMIT_NULL) leave payload columns unwritten
        // and depend on that.  Undersized pool buffers are recycled rather
        // than grown in place: Vec::reserve on a too-small buffer copies the
        // old contents forward before zeroing, which is slower than a fresh
        // zeroed allocation.
        let data = if total_size >= HUGEPAGE_THRESHOLD {
            let mut v = vec![0u8; total_size];
            crate::sys::madvise_hugepage(v.as_mut_ptr(), total_size);
            v
        } else {
            match super::batch_pool::acquire_buf() {
                Some(mut buf) if buf.capacity() >= total_size => {
                    buf.resize(total_size, 0);
                    buf
                }
                other => {
                    if let Some(buf) = other { super::batch_pool::recycle_buf(buf); }
                    vec![0u8; total_size]
                }
            }
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
            blob_id: next_blob_id(),
        }
    }

    /// Construct by copying data in from region pointers.
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

        let nr = REG_PAYLOAD_START + num_payload_cols;
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
        let blob_idx = REG_PAYLOAD_START + num_payload_cols;
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
            blob_id: next_blob_id(),
        }
    }

    /// Construct a `Batch` from fully pre-built, correctly-laid-out buffers.
    ///
    /// `data` must be at least `count * strides[i]` bytes starting at
    /// `offsets[i]` for every `i < num_regions`, as produced by
    /// `compute_offsets`.  Used by `slice_to_owned_batch` to avoid an
    /// intermediate copy.
    ///
    /// # Safety
    /// Caller must guarantee the layout invariant described above.
    pub(super) unsafe fn from_prebuilt(
        data: Vec<u8>,
        blob: Vec<u8>,
        strides: [u8; MAX_BATCH_REGIONS],
        offsets: [u32; MAX_BATCH_REGIONS],
        num_regions: u8,
        count: usize,
    ) -> Self {
        Batch {
            data,
            blob,
            offsets,
            strides,
            num_regions,
            capacity: count as u32,
            count,
            sorted: false,
            consolidated: false,
            schema: None,
            blob_id: next_blob_id(),
        }
    }

    // ── Schema installation ─────────────────────────────────────────────

    /// Install a schema on this batch after verifying its column count
    /// matches the batch's physical payload regions. Every code path that
    /// wants to mutate `batch.schema` from outside the constructors MUST go
    /// through this helper: it turns a latent "batch shape != declared
    /// shape" bug into a localized panic at the first assignment, instead
    /// of a cryptic OOB slice panic 5 call-frames later (e.g. in
    /// scalar_func::copy_column).
    #[inline]
    pub fn set_schema(&mut self, s: SchemaDescriptor) {
        debug_assert_eq!(
            self.num_payload_cols() + 1, s.num_columns as usize,
            "Batch::set_schema: batch has {} payload cols, schema declares {}",
            self.num_payload_cols(), s.num_columns as usize - 1
        );
        self.schema = Some(s);
    }

    // ── Read accessors ──────────────────────────────────────────────────

    #[inline]
    pub fn pk_lo_data(&self) -> &[u8] {
        let off = self.offsets[REG_PK_LO] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn pk_hi_data(&self) -> &[u8] {
        let off = self.offsets[REG_PK_HI] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn weight_data(&self) -> &[u8] {
        let off = self.offsets[REG_WEIGHT] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn null_bmp_data(&self) -> &[u8] {
        let off = self.offsets[REG_NULL_BMP] as usize;
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn col_data(&self, pi: usize) -> &[u8] {
        let r = REG_PAYLOAD_START + pi;
        let off = self.offsets[r] as usize;
        &self.data[off..off + self.count * self.strides[r] as usize]
    }
    #[inline]
    pub fn blob_data(&self) -> &[u8] { &self.blob }
    #[inline]
    pub fn num_payload_cols(&self) -> usize {
        self.num_regions as usize - REG_PAYLOAD_START
    }

    // ── Mutable slice accessors ─────────────────────────────────────────

    #[inline]
    pub fn pk_lo_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_PK_LO] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn pk_hi_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_PK_HI] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn weight_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_WEIGHT] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn null_bmp_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_NULL_BMP] as usize;
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn col_data_mut(&mut self, pi: usize) -> &mut [u8] {
        let r = REG_PAYLOAD_START + pi;
        let off = self.offsets[r] as usize;
        let end = off + self.count * self.strides[r] as usize;
        &mut self.data[off..end]
    }

    // ── Row accessors ───────────────────────────────────────────────────

    #[inline]
    fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[REG_PK_LO] as usize..], row * 8)
    }
    #[inline]
    fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[REG_PK_HI] as usize..], row * 8)
    }
    #[inline]
    pub fn get_pk(&self, row: usize) -> u128 {
        crate::util::make_pk(self.get_pk_lo(row), self.get_pk_hi(row))
    }
    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(&self.data[self.offsets[REG_WEIGHT] as usize..], row * 8)
    }
    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[REG_NULL_BMP] as usize..], row * 8)
    }
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = self.offsets[REG_PAYLOAD_START + payload_col] as usize + row * col_size;
        &self.data[off..off + col_size]
    }

    // ── Extend methods (building batches row-by-row) ────────────────────

    /// Ensure the data buffer has room for at least one more row.
    pub(crate) fn ensure_row_capacity(&mut self) {
        self.reserve_rows(1);
    }

    /// Ensure the data buffer has room for at least `n` more rows beyond `count`.
    pub(crate) fn reserve_rows(&mut self, n: usize) {
        if self.count + n <= self.capacity as usize { return; }
        let nr = self.num_regions as usize;
        let new_cap = (self.capacity as usize * 2).max(8).max(self.count + n);
        let (new_offsets, new_total) = compute_offsets(&self.strides, nr, new_cap);

        if new_total > self.data.capacity() {
            // Out-of-place grow.  Vec::reserve on a too-small buffer triggers
            // realloc, which copies ALL old bytes to a new allocation (copy #1),
            // then copy_within would shift regions to new offsets (copy #2).
            // Bypass that by scatter-copying directly into a fresh buffer.
            let mut new_data = if new_total >= HUGEPAGE_THRESHOLD {
                // Zeroing is not needed: the scatter-copy loop below fills
                // every live byte, and all accessors are bounded by `count`.
                // We still want THP backing for large buffers.
                let mut v = Vec::with_capacity(new_total);
                unsafe { v.set_len(new_total); }
                crate::sys::madvise_hugepage(v.as_ptr() as *mut u8, new_total);
                v
            } else {
                match super::batch_pool::acquire_buf() {
                    Some(mut buf) if buf.capacity() >= new_total => {
                        unsafe { buf.set_len(new_total); }
                        buf
                    }
                    other => {
                        // Return undersized buffer to pool if we got one.
                        if let Some(buf) = other {
                            super::batch_pool::recycle_buf(buf);
                        }
                        let mut v = Vec::with_capacity(new_total);
                        unsafe { v.set_len(new_total); }
                        v
                    }
                }
            };

            for i in 0..nr {
                let len = self.count * self.strides[i] as usize;
                if len == 0 { continue; }
                let old_off = self.offsets[i] as usize;
                let new_off = new_offsets[i] as usize;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr().add(old_off),
                        new_data.as_mut_ptr().add(new_off),
                        len,
                    );
                }
            }

            let old_data = std::mem::replace(&mut self.data, new_data);
            super::batch_pool::recycle_buf(old_data);
        } else {
            // Vec already has sufficient capacity (e.g. cleared batch being
            // refilled).  copy_within shifts regions in one pass.
            unsafe {
                self.data.set_len(new_total);
                for i in (0..nr).rev() {
                    let old_start = self.offsets[i] as usize;
                    let new_start = new_offsets[i] as usize;
                    let data_len = self.count * self.strides[i] as usize;
                    if old_start != new_start && data_len > 0 {
                        std::ptr::copy(
                            self.data.as_ptr().add(old_start),
                            self.data.as_mut_ptr().add(new_start),
                            data_len,
                        );
                    }
                }
            }
        }
        self.offsets = new_offsets;
        self.capacity = new_cap as u32;
    }

    /// Write data into region `r` at the current row position.
    /// Auto-grows capacity if needed.  Strides must be set at construction —
    /// use `empty_with_schema`, `empty_joined`, or `with_schema`.
    #[inline]
    fn extend_region(&mut self, r: usize, src: &[u8]) {
        if self.count >= self.capacity as usize {
            self.ensure_row_capacity();
        }
        let off = self.offsets[r] as usize + self.count * self.strides[r] as usize;
        self.data[off..off + src.len()].copy_from_slice(src);
    }

    #[inline]
    pub fn extend_pk_lo(&mut self, d: &[u8]) { self.extend_region(REG_PK_LO, d); }
    #[inline]
    pub fn extend_pk_hi(&mut self, d: &[u8]) { self.extend_region(REG_PK_HI, d); }
    #[inline]
    pub fn extend_weight(&mut self, d: &[u8]) { self.extend_region(REG_WEIGHT, d); }
    #[inline]
    pub fn extend_null_bmp(&mut self, d: &[u8]) { self.extend_region(REG_NULL_BMP, d); }
    #[inline]
    pub fn extend_col(&mut self, pi: usize, d: &[u8]) { self.extend_region(REG_PAYLOAD_START + pi, d); }

    /// Fill `nbytes` of zeros at the current row position in a payload column.
    #[inline]
    pub fn fill_col_zero(&mut self, pi: usize, nbytes: usize) {
        let r = REG_PAYLOAD_START + pi;
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

    /// Bulk-copy rows `[start, end)` from a `MemBatch` into `self`, writing
    /// `weight_override` into the weight column instead of the source weights.
    ///
    /// One `copy_from_slice` per column region replaces the per-row extend loop
    /// used by `copy_cursor_row_with_weight`. The caller must ensure the source
    /// has no out-of-line string blobs (i.e. no STRING-typed payload columns with
    /// long values that require blob-offset relocation).
    ///
    /// Preconditions:
    /// - `start <= end <= src.count`
    /// - `self` has the same schema (column count and strides) as `src`
    pub(crate) fn append_mem_batch_range(
        &mut self,
        src: &MemBatch<'_>,
        start: usize,
        end: usize,
        weight_override: i64,
    ) {
        let n = end - start;
        if n == 0 { return; }
        self.reserve_rows(n);
        self.bulk_copy_region(REG_PK_LO, src.pk_lo, start, end);
        self.bulk_copy_region(REG_PK_HI, src.pk_hi, start, end);
        // Fill weight region with the constant override value.
        {
            let dst_off = self.offsets[REG_WEIGHT] as usize + self.count * 8;
            let w_bytes = weight_override.to_le_bytes();
            let dest = &mut self.data[dst_off..dst_off + n * 8];
            for chunk in dest.chunks_exact_mut(8) {
                chunk.copy_from_slice(&w_bytes);
            }
        }
        self.bulk_copy_region(REG_NULL_BMP, src.null_bmp, start, end);
        for pi in 0..src.col_data.len() {
            self.bulk_copy_region(REG_PAYLOAD_START + pi, src.col_data[pi], start, end);
        }
        self.count += n;
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

    /// Returns `None` for unsorted batches; `count <= 1` is always sorted.
    pub(super) fn as_sorted_mem_batch(&self) -> Option<merge::SortedMemBatch<'_>> {
        if self.sorted || self.count <= 1 {
            Some(merge::SortedMemBatch::new_unchecked(self.as_mem_batch()))
        } else {
            None
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
        // 4 fixed regions + npc payload regions + 1 blob region.
        let cap = REG_PAYLOAD_START + npc + 1;
        let mut ptrs = Vec::with_capacity(cap);
        let mut sizes = Vec::with_capacity(cap);
        for i in 0..REG_PAYLOAD_START + npc {
            let off = self.offsets[i] as usize;
            let len = self.count * self.strides[i] as usize;
            ptrs.push(self.data[off..].as_ptr());
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
        let mut new_data = super::batch_pool::acquire_buf().unwrap_or_default();
        new_data.clear();
        new_data.reserve(packed_size);
        // SAFETY: copy_nonoverlapping fills all `packed_size` bytes below;
        // src and dst are non-overlapping (different allocations).
        unsafe {
            new_data.set_len(packed_size);
            for i in 0..nr {
                let stride = self.strides[i] as usize;
                let len = self.count * stride;
                let src_off = self.offsets[i] as usize;
                let dst_off = packed_offsets[i] as usize;
                if len > 0 {
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr().add(src_off),
                        new_data.as_mut_ptr().add(dst_off),
                        len,
                    );
                }
            }
        }
        let mut new_blob = super::batch_pool::acquire_buf().unwrap_or_default();
        new_blob.clear();
        new_blob.extend_from_slice(&self.blob);
        Batch {
            data: new_data,
            blob: new_blob,
            offsets: packed_offsets,
            strides: self.strides,
            num_regions: self.num_regions,
            capacity: self.count as u32,
            count: self.count,
            sorted: self.sorted,
            consolidated: self.consolidated,
            schema: self.schema,
            blob_id: self.blob_id,
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
                let dest = crate::schema::relocate_german_string_vec(src, blob_src, &mut self.blob, None);
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

        for (pi, _ci, col) in schema.payload_columns() {
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
                        let mut bytes = [0u8; 16];
                        bytes[..8].copy_from_slice(&(lo_values[pi] as u64).to_le_bytes());
                        bytes[8..].copy_from_slice(&hi_values[pi].to_le_bytes());
                        self.extend_col(pi, &bytes);
                    }
                    _ => {
                        let bytes = lo_values[pi].to_le_bytes();
                        self.extend_col(pi, &bytes[..col_size]);
                    }
                }
            }
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Bulk-copy rows [start, end) from another Batch (same schema).
    ///
    /// `self` must have strides pre-set (see `empty_with_schema` / `with_schema`).
    pub fn append_batch(&mut self, src: &Batch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, false);
    }

    /// Bulk-copy rows with negated weights.
    pub fn append_batch_negated(&mut self, src: &Batch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end { return; }
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, true);
    }

    /// Bulk-copy rows [start, end) verbatim — no string blob relocation.
    ///
    /// # Precondition
    /// `self.blob` must already equal `src.blob` (e.g. assigned with
    /// `self.blob = src.blob.clone()` before the first call).  The 16-byte
    /// German String structs are copied as-is; their heap offsets remain valid
    /// because both batches share the same blob content.
    pub fn append_batch_no_blob_reloc(&mut self, src: &Batch, start: usize, end: usize) {
        let end = end.min(src.count);
        if start >= end { return; }
        // Guard for the precondition: both batches must share the same blob identity,
        // established by calling `share_blob_from` (or `clone_batch`) before this.
        // Matching IDs guarantees identical blob content; same-length-different-content
        // blobs would produce dangling heap offsets in the copied German String structs.
        debug_assert_eq!(
            self.blob_id, src.blob_id,
            "append_batch_no_blob_reloc: blobs must be identical; \
             call share_blob_from(&src) before appending (self.blob_id={}, src.blob_id={})",
            self.blob_id, src.blob_id,
        );
        let n = end - start;
        self.reserve_rows(n);
        // All four system regions
        self.bulk_copy_region(REG_PK_LO, &src.data[src.offsets[REG_PK_LO] as usize..], start, end);
        self.bulk_copy_region(REG_PK_HI, &src.data[src.offsets[REG_PK_HI] as usize..], start, end);
        self.bulk_copy_region(REG_WEIGHT, &src.data[src.offsets[REG_WEIGHT] as usize..], start, end);
        self.bulk_copy_region(REG_NULL_BMP, &src.data[src.offsets[REG_NULL_BMP] as usize..], start, end);
        // Payload columns — string structs copied verbatim (blob already shared)
        let npc = self.num_payload_cols();
        for pi in 0..npc {
            if self.strides[REG_PAYLOAD_START + pi] > 0 {
                self.bulk_copy_region(REG_PAYLOAD_START + pi, &src.data[src.offsets[REG_PAYLOAD_START + pi] as usize..], start, end);
            }
        }
        self.count += n;
        self.sorted = false;
        self.consolidated = false;
    }

    fn append_rows_inner(&mut self, src: &Batch, start: usize, end: usize, negate: bool) {
        let n = end - start;
        self.reserve_rows(n);

        // Fixed columns: bulk copy from src's data buffer.
        self.bulk_copy_region(REG_PK_LO, &src.data[src.offsets[REG_PK_LO] as usize..], start, end);
        self.bulk_copy_region(REG_PK_HI, &src.data[src.offsets[REG_PK_HI] as usize..], start, end);
        if negate {
            let w_off = self.offsets[REG_WEIGHT] as usize;
            for i in start..end {
                let dst = w_off + (self.count + i - start) * 8;
                self.data[dst..dst + 8].copy_from_slice(&(-src.get_weight(i)).to_le_bytes());
            }
        } else {
            self.bulk_copy_region(REG_WEIGHT, &src.data[src.offsets[REG_WEIGHT] as usize..], start, end);
        }
        self.bulk_copy_region(REG_NULL_BMP, &src.data[src.offsets[REG_NULL_BMP] as usize..], start, end);

        // Payload columns.  When the schema is installed, we need to know
        // which payload positions hold STRING values so the blob can be
        // relocated; without a schema, treat all payload columns as opaque
        // bytes (no blob relocation is possible without type info anyway).
        //
        // `is_string_at[pi]` is true iff payload column `pi` is a STRING.
        let npc = self.num_payload_cols();
        let mut is_string_at = [false; MAX_BATCH_REGIONS];
        if let Some(s) = self.schema {
            for (pi, _ci, col) in s.payload_columns() {
                if pi >= npc { break; }
                is_string_at[pi] = col.type_code == crate::schema::type_code::STRING;
            }
        }

        for pi in 0..npc {
            let cs = self.strides[REG_PAYLOAD_START + pi] as usize;
            if is_string_at[pi] && cs == 16 {
                for row in start..end {
                    let src_off = src.offsets[REG_PAYLOAD_START + pi] as usize + row * 16;
                    let src_struct = &src.data[src_off..src_off + 16];
                    let dst_off = self.offsets[REG_PAYLOAD_START + pi] as usize + (self.count + row - start) * 16;
                    relocate_string_cell(src_struct, &src.blob, &mut self.data[dst_off..dst_off + 16], &mut self.blob);
                }
            } else if cs > 0 {
                self.bulk_copy_region(REG_PAYLOAD_START + pi, &src.data[src.offsets[REG_PAYLOAD_START + pi] as usize..], start, end);
            }
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
        let null_word = ptable.found_null_word();

        let (pk_lo, pk_hi) = crate::util::split_pk(pk);
        self.extend_pk_lo(&pk_lo.to_le_bytes());
        self.extend_pk_hi(&pk_hi.to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        for (pi, _ci, col_desc) in schema.payload_columns() {
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
        let col_meta: Vec<(usize, bool)> = schema
            .payload_columns()
            .map(|(_pi, _ci, col)| {
                let is_string = col.type_code == crate::schema::type_code::STRING && col.size == 16;
                (col.size as usize, is_string)
            })
            .collect();

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
                    let dst_off = self.offsets[REG_PAYLOAD_START + pi] as usize + self.count * 16;
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
        self.blob_id = next_blob_id();
        // data buffer stays allocated — capacity and offsets remain valid.
    }

    /// Copy blob content from `src` and record that this batch shares `src`'s
    /// blob identity.  Must be called before `append_batch_no_blob_reloc`.
    pub fn share_blob_from(&mut self, src: &Batch) {
        self.blob = src.blob.clone();
        self.blob_id = src.blob_id;
    }

    /// Number of regions in the standard layout (including blob).
    pub fn num_regions_total(&self) -> usize {
        self.num_regions as usize + 1
    }

    /// Get region pointer by index.
    pub fn region_ptr(&self, idx: usize) -> *const u8 {
        let blob_idx = REG_PAYLOAD_START + self.num_payload_cols();
        if idx < blob_idx {
            self.data[self.offsets[idx] as usize..].as_ptr()
        } else if idx == blob_idx {
            self.blob.as_ptr()
        } else {
            panic!("region_ptr: index {idx} out of range (num_regions_total = {})", blob_idx + 1);
        }
    }

    /// Get region size by index.
    pub fn region_size(&self, idx: usize) -> usize {
        let blob_idx = REG_PAYLOAD_START + self.num_payload_cols();
        if idx < blob_idx {
            self.count * self.strides[idx] as usize
        } else {
            self.blob.len()
        }
    }

    pub fn regions(&self) -> Vec<(*const u8, usize)> {
        let npc = self.num_payload_cols();
        let mut r = Vec::with_capacity(REG_PAYLOAD_START + npc + 1);
        for i in 0..REG_PAYLOAD_START + npc {
            let off = self.offsets[i] as usize;
            let len = self.count * self.strides[i] as usize;
            r.push((self.data[off..].as_ptr(), len));
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

        self.extend_pk_lo(&(key as u64).to_le_bytes());
        self.extend_pk_hi(&((key >> 64) as u64).to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        let null_word = source.get_null_word(row);
        self.extend_null_bmp(&null_word.to_le_bytes());

        let src_blob = source.blob_slice();
        for (pi, _ci, col) in schema.payload_columns() {
            let cs = col.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;

            if is_null {
                self.fill_col_zero(pi, cs);
            } else if col.type_code == crate::schema::type_code::STRING {
                let src_struct = source.get_col_ptr(row, pi, cs);
                let dest = crate::schema::relocate_german_string_vec(
                    src_struct, src_blob, &mut self.blob, Some(blob_cache),
                );
                self.extend_col(pi, &dest);
            } else {
                self.extend_col(pi, source.get_col_ptr(row, pi, cs));
            }
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Consume this batch, consolidating it if needed.
    ///
    /// Fast path: if already consolidated or empty, wraps `self` in a
    /// `ConsolidatedBatch` without any allocation (free move).
    /// Slow path: sorts and weight-folds into a fresh batch, then drops `self`.
    pub fn into_consolidated(self, schema: &SchemaDescriptor) -> ConsolidatedBatch {
        if self.consolidated || self.count == 0 {
            return ConsolidatedBatch(self);
        }
        let already_sorted = self.sorted;
        let mb = self.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut result = write_to_batch(schema, self.count, blob_cap, |writer| {
            if already_sorted {
                merge::fold_sorted(&mb, schema, writer);
            } else {
                merge::sort_and_consolidate(&mb, schema, writer);
            }
        });
        result.sorted = true;
        result.consolidated = true;
        result.set_schema(*schema);
        ConsolidatedBatch(result)
    }

    /// Consolidate a borrowed batch if needed. Returns `None` when the batch is already
    /// consolidated or empty (caller borrows the original). Returns `Some(ConsolidatedBatch)`
    /// when a new consolidated batch was allocated (caller borrows that instead).
    ///
    /// Idiomatic usage:
    /// ```ignore
    /// let cs = Batch::consolidate_if_needed(delta, schema);
    /// let c: &Batch = cs.as_deref().unwrap_or(delta);
    /// ```
    pub fn consolidate_if_needed(batch: &Batch, schema: &SchemaDescriptor) -> Option<ConsolidatedBatch> {
        if batch.consolidated || batch.count == 0 {
            return None;
        }
        let mb = batch.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut result = write_to_batch(schema, batch.count, blob_cap, |writer| {
            merge::sort_and_consolidate(&mb, schema, writer);
        });
        result.sorted = true;
        result.consolidated = true;
        result.set_schema(*schema);
        Some(ConsolidatedBatch::new_unchecked(result))
    }

    #[cfg(test)]
    pub(crate) fn data_capacity(&self) -> usize { self.data.capacity() }

    /// Write this batch as a shard file directly to disk.
    pub fn write_as_shard(&self, path: &CStr, table_id: u32) -> Result<(), super::error::StorageError> {
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

/// Owned `Batch` certified to be consolidated (sorted, no duplicate PKs, weights folded).
///
/// Obtain via `Batch::into_consolidated` (checked) or `new_unchecked` (caller asserts).
#[repr(transparent)]
pub struct ConsolidatedBatch(Batch);

impl ConsolidatedBatch {
    pub(crate) fn new_unchecked(batch: Batch) -> Self {
        debug_assert!(batch.sorted || batch.count == 0, "ConsolidatedBatch must be sorted");
        debug_assert!(batch.consolidated || batch.count == 0, "ConsolidatedBatch must be consolidated");
        ConsolidatedBatch(batch)
    }
    pub fn into_inner(self) -> Batch { self.0 }
    /// Reinterpret `&Batch` as `&ConsolidatedBatch` when `batch.consolidated` is set.
    /// Empty batches are always considered consolidated.
    // SAFETY: ConsolidatedBatch is #[repr(transparent)] over Batch.
    pub(crate) fn from_batch_ref(batch: &Batch) -> Option<&Self> {
        (batch.consolidated || batch.count == 0)
            .then(|| unsafe { &*(batch as *const Batch as *const ConsolidatedBatch) })
    }
}

impl std::ops::Deref for ConsolidatedBatch {
    type Target = Batch;
    fn deref(&self) -> &Batch { &self.0 }
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
        blob_id: next_blob_id(),
        capacity: max_rows as u32,
        count: actual_rows,
        sorted: false,
        consolidated: false,
        schema: None,
    }
}
