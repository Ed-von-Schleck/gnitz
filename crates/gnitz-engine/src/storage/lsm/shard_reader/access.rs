//! Per-row accessors for [`MappedShard`] — the region reads, the XOR8 probe and
//! the OPK binary searches — plus the bulk `*_owned_batch` materializers.

use std::ptr;

use super::super::batch::{
    acquire_arena, Fill, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT,
};
use super::super::merge::{relocate_german_string_vec, BlobCacheGuard, ColPtr, UnifiedSource};
use super::super::xor8;
use super::{MappedShard, PackedRegion, PayloadRegion, RegionView, WeightRegion};
use crate::schema::key::PkBuf;
use crate::schema::{SchemaDescriptor, MAX_COLUMNS};
use gnitz_wire::{read_i64_le, read_u64_le};

impl RegionView {
    /// This region as a [`ColPtr`]. A constant region already carries
    /// `stride == 0`, so `base.add(i * stride) == base` reads the same element
    /// for every row with no per-row branch.
    #[inline]
    fn to_col_ptr(self, data_ptr: *const u8) -> ColPtr {
        ColPtr {
            base: unsafe { data_ptr.add(self.offset) },
            stride: self.stride,
        }
    }
}

impl MappedShard {
    /// `inline(always)` rather than the plain hint: every accessor below funnels
    /// through this, and the plain hint is a no-op at `opt-level=0` — the debug
    /// binary the E2E suite runs would pay a frame per shard cell read.
    #[inline(always)]
    pub(crate) fn data(&self) -> &[u8] {
        self.mmap.as_slice()
    }

    /// Materialize a [`PackedRegion`] (FoR payload) to its full
    /// `count × elem_width` little-endian raw image, decoding once per shard
    /// open and caching it in the region's `OnceCell`. The returned slice serves
    /// every payload accessor at the same `row * elem_width` offset the Raw arm
    /// uses; its content address is stable for the shard's lifetime.
    fn packed_bytes<'a>(&'a self, region: &'a PackedRegion) -> &'a [u8] {
        region
            .decoded
            .get_or_init(|| {
                super::super::shard_file::decode_for_region(
                    &self.data()[region.offset..region.offset + region.size],
                    self.count,
                    region.elem_width,
                )
            })
            .as_bytes()
    }

    // Production reads PK regions as raw OPK bytes (`get_pk_bytes`); only tests
    // need the native value back.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn get_pk(&self, row: usize) -> u128 {
        let width = self.pk_stride as usize;
        gnitz_wire::widen_pk_be(self.get_pk_bytes(row), width)
    }

    #[inline]
    pub fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let width = self.pk_stride as usize;
        &self.data()[self.pk.row_off(row)..][..width]
    }

    /// `(pk_min, pk_max)` OPK bounds for this shard, in `self.pk_stride`-wide
    /// `PkBuf`s. Derived from `count`, never serialized. An empty shard
    /// (`count == 0`) has no row to read `get_pk_bytes` from, so it returns
    /// zero-key bounds; an empty shard is never probed, so these bounds are
    /// only a defensive backstop.
    pub fn pk_bounds(&self) -> (PkBuf, PkBuf) {
        if self.count > 0 {
            (
                PkBuf::from_bytes(self.get_pk_bytes(0)),
                PkBuf::from_bytes(self.get_pk_bytes(self.count - 1)),
            )
        } else {
            let e = PkBuf::zeroed(self.pk_stride as usize);
            (e, e)
        }
    }

    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        match &self.weight {
            WeightRegion::Direct(v) => read_i64_le(self.data(), v.row_off(row)),
            WeightRegion::TwoValue {
                value_a,
                value_b,
                bitvec_off,
            } => {
                let byte = self.data()[bitvec_off + row / 8];
                if (byte >> (row % 8)) & 1 == 0 {
                    *value_a
                } else {
                    *value_b
                }
            }
        }
    }

    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data(), self.null_bmp.row_off(row))
    }

    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        match &self.col_regions[payload_col_idx] {
            PayloadRegion::Direct(v) => &self.data()[v.row_off(row)..][..col_size],
            // `col_size == elem_width`; the decoded image serves the same
            // `row * col_size` slice a direct region would.
            PayloadRegion::Packed(p) => &self.packed_bytes(p)[row * col_size..][..col_size],
        }
    }

    #[inline]
    pub fn blob_slice(&self) -> &[u8] {
        &self.data()[self.blob_off..self.blob_off + self.blob_len]
    }

    /// Test-only: distinguishes "no filter" from "filter admits it", which
    /// [`xor8_may_contain`](Self::xor8_may_contain) deliberately cannot.
    #[cfg(test)]
    pub(crate) fn has_xor8(&self) -> bool {
        self.xor8_filter.is_some()
    }

    /// A shard carrying no filter admits every key.
    pub fn xor8_may_contain(&self, probe_key: u64) -> bool {
        match &self.xor8_filter {
            Some(filter) => xor8::may_contain(filter, probe_key),
            None => true,
        }
    }

    /// Test-only u128 oracle that cross-checks `find_lower_bound_bytes` (the
    /// production path): binary search for the first row where PK >= key.
    /// Returns `count` if no such row exists.
    #[cfg(test)]
    pub(crate) fn find_lower_bound(&self, key: u128) -> usize {
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

    /// The PK region as a [`ColPtr`] view — the addressing source for the OPK
    /// seeks below (via [`ColPtr::row`]) and `to_unified`'s PK column. The base
    /// aliases `self`; keep `self` alive while the view is read (the seek
    /// closures run synchronously within the call).
    #[inline]
    fn pk_col_ptr(&self) -> ColPtr {
        self.pk.to_col_ptr(self.data().as_ptr())
    }

    /// First row whose OPK bytes are `>= key`. A raw `memcmp` binary search —
    /// correct at every PK width with no schema dependency. `key` must be
    /// exactly `pk_stride` OPK bytes.
    pub fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        let stride = self.pk_stride as usize;
        let cp = self.pk_col_ptr();
        super::super::columnar::lower_bound_opk(self.count, key, stride, |i| unsafe { cp.row(i, stride) })
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. Byte-identical
    /// body to `Batch::advance_to` — same `count`/`pk_col_ptr` seek contract.
    /// `key` must be exactly `pk_stride` OPK bytes.
    pub fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        let stride = self.pk_stride as usize;
        let cp = self.pk_col_ptr();
        super::super::columnar::gallop_opk(self.count, key, hint, stride, |i| unsafe { cp.row(i, stride) })
    }

    /// Test-only u128 oracle (exact-match point lookup) cross-checking the
    /// production byte path. Returns the row index, or `None` if absent.
    #[cfg(test)]
    pub(crate) fn find_row_index(&self, key: u128) -> Option<usize> {
        let idx = self.find_lower_bound(key);
        if idx < self.count && self.get_pk(idx) == key {
            Some(idx)
        } else {
            None
        }
    }

    /// Bulk-copy a contiguous slice of rows into an Batch.
    /// Bypasses per-row cursor overhead entirely — one memcpy per column.
    ///
    /// Picks the blob arm via [`Batch::should_relocate_blob`]: a narrow slice off a
    /// wide heap relocates only its own rows' strings, anything else copies the
    /// whole region.
    #[inline]
    pub(crate) fn slice_to_owned_batch(
        &self,
        start: usize,
        row_count: usize,
        schema: &crate::schema::SchemaDescriptor,
    ) -> super::super::batch::Batch {
        let relocate = super::super::batch::Batch::should_relocate_blob(row_count, self.count, self.blob_len);
        self.slice_to_owned_batch_with(start, row_count, schema, relocate)
    }

    /// [`slice_to_owned_batch`](Self::slice_to_owned_batch) with the blob arm
    /// passed in rather than derived, so `slice_blob_relocate_bench` can time both
    /// on one slice.
    #[allow(clippy::uninit_vec)]
    pub(super) fn slice_to_owned_batch_with(
        &self,
        start: usize,
        row_count: usize,
        schema: &crate::schema::SchemaDescriptor,
        relocate: bool,
    ) -> super::super::batch::Batch {
        use super::super::batch::{compute_offsets, strides_from_schema, Batch, Layout};

        if row_count == 0 {
            return Batch::empty_with_schema(schema);
        }

        let shard = self.data();

        // Compute the final columnar layout before allocating anything.
        let (strides, num_regions_u8) = strides_from_schema(schema);
        let nr = num_regions_u8 as usize; // 4 + npc
        let (offsets, total_size) = compute_offsets(&strides, nr, row_count);

        // One allocation for all fixed-stride columnar data. Materializing a
        // whole shard is among the largest single allocations the engine makes,
        // so it goes through the shared arena path for its hugepage bypass (and
        // its undersized-buffer eviction and debug poison).
        let mut data = acquire_arena(total_size, Fill::Uninit);

        // Write each region directly into its final slice — no intermediate
        // buffers. The Raw/Constant fill is shared by both expanders; only the
        // weight region can carry the TwoValue bitvec form.
        let copy_raw = |begin: usize, width: usize, dst: &mut [u8]| {
            dst.copy_from_slice(&shard[begin..begin + row_count * width]);
        };
        let fill_const = |value: &[u8], dst: &mut [u8]| {
            for chunk in dst.chunks_exact_mut(value.len()) {
                chunk.copy_from_slice(value);
            }
        };
        // A constant region carries `stride == 0`, so it fills `dst` from its one
        // element; anything else is a contiguous copy.
        let expand_view = |v: &RegionView, width: usize, dst: &mut [u8]| {
            if v.stride == 0 {
                fill_const(&shard[v.offset..v.offset + width], dst)
            } else {
                copy_raw(v.row_off(start), width, dst)
            }
        };
        let expand_payload = |region: &PayloadRegion, stride: usize, dst: &mut [u8]| match region {
            PayloadRegion::Direct(v) => expand_view(v, stride, dst),
            // Payload strides equal `elem_width`, so the decoded image is
            // `count · stride` long.
            PayloadRegion::Packed(p) => {
                let bytes = self.packed_bytes(p);
                dst.copy_from_slice(&bytes[start * stride..(start + row_count) * stride]);
            }
        };
        let expand_weight = |region: &WeightRegion, dst: &mut [u8]| match region {
            WeightRegion::Direct(v) => expand_view(v, FIXED_REGION_BYTES, dst),
            WeightRegion::TwoValue {
                value_a,
                value_b,
                bitvec_off,
            } => {
                let a_bytes = value_a.to_le_bytes();
                let b_bytes = value_b.to_le_bytes();
                for i in 0..row_count {
                    let row = start + i;
                    let bit = (shard[bitvec_off + row / 8] >> (row % 8)) & 1;
                    let src = if bit == 0 { &a_bytes[..] } else { &b_bytes[..] };
                    dst[i * 8..(i + 1) * 8].copy_from_slice(src);
                }
            }
        };

        let pk_stride = self.pk_stride as usize;
        let sz8 = row_count * 8;
        expand_view(
            &self.pk,
            pk_stride,
            &mut data[offsets[REG_PK]..][..row_count * pk_stride],
        );
        expand_weight(&self.weight, &mut data[offsets[REG_WEIGHT]..][..sz8]);
        expand_view(
            &self.null_bmp,
            FIXED_REGION_BYTES,
            &mut data[offsets[REG_NULL_BMP]..][..sz8],
        );

        for (pi, col) in schema.payload_columns() {
            // A relocated string column is written cell-by-cell below; filling it
            // here would only be overwritten.
            if relocate && gnitz_wire::is_german_string(col.type_code) {
                continue;
            }
            let stride = col.size() as usize;
            let off = offsets[REG_PAYLOAD_START + pi];
            let sz = row_count * stride;
            expand_payload(&self.col_regions[pi], stride, &mut data[off..][..sz]);
        }

        // Blob. Relocating carries only the sliced rows' strings; the
        // whole-region copy keeps every German-string offset valid without
        // touching a cell.
        let blob = if relocate {
            debug_assert!(start + row_count <= self.count, "slice out of range");
            // The slice's share of the heap — an estimate, so `div_ceil` rather than
            // a truncating quotient that would reserve nothing for a shard holding
            // fewer heap bytes than rows.
            let mut out = acquire_arena(self.blob_len.div_ceil(self.count) * row_count, Fill::Reserve);
            // Cap the dedup-map hint: a hint above `BLOB_CACHE_RECYCLE_CAP`'s
            // capacity would make the map too large to return to the pool, so a
            // chunked drain would malloc and free one per chunk. Entry count is
            // bounded by *distinct* spans, which is below `row_count` anyway.
            let mut guard = BlobCacheGuard::acquire(schema, row_count.min(4096));
            let src_blob = self.blob_slice();
            for (pi, col) in schema.payload_columns() {
                if !gnitz_wire::is_german_string(col.type_code) {
                    continue;
                }
                let off = offsets[REG_PAYLOAD_START + pi];
                let dst = &mut data[off..off + row_count * 16];
                for (i, cell) in dst.chunks_exact_mut(16).enumerate() {
                    // Read through `get_col_ptr` so the Raw/Constant/Packed region
                    // forms are handled the same way the bulk fill handles them.
                    let src = self.get_col_ptr(start + i, pi, 16);
                    cell.copy_from_slice(&relocate_german_string_vec(src, src_blob, &mut out, guard.get_mut()));
                }
            }
            out
        } else if self.blob_len > 0 {
            let src = &shard[self.blob_off..self.blob_off + self.blob_len];
            let mut buf = acquire_arena(self.blob_len, Fill::Uninit);
            buf.copy_from_slice(src);
            buf
        } else {
            Vec::new()
        };

        let mut batch = unsafe { Batch::from_prebuilt(data, blob, strides, offsets, num_regions_u8, row_count) };
        batch.set_schema(*schema);
        // Shards are written consolidated; a contiguous slice stays (PK, payload)-
        // sorted and ghost-free (shards are ghost-free by construction). Certify it.
        batch.certify_layout(Layout::Consolidated, schema);
        batch
    }

    /// Derive a `UnifiedSource` view over this shard: each `ScalarRegion` becomes
    /// a `(base, stride)` `ColPtr` into the shard's mmap, with `Constant` regions
    /// mapped to `stride == 0` so `base.add(ri * stride) == base` reads the same
    /// bytes for every row. Pure pointer arithmetic — no allocation, no scan. The
    /// returned `ColPtr`s alias the mapped memory, so the caller must keep `self`
    /// alive for as long as the view is read.
    ///
    /// The shard-side counterpart of `repr::merge::mem_batch_to_unified`; shared
    /// by the read-cursor drain (shard-vs-`MemBatch` polymorphism) and shard
    /// compaction.
    pub(crate) fn to_unified(&self, schema: &SchemaDescriptor) -> UnifiedSource {
        let data_ptr = self.data().as_ptr();

        let pk = self.pk.to_col_ptr(data_ptr);
        let null_bmp = self.null_bmp.to_col_ptr(data_ptr);

        let mut cols = [ColPtr {
            base: ptr::null(),
            stride: 0,
        }; MAX_COLUMNS - 1];
        for (pi, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            cols[pi] = match &self.col_regions[pi] {
                PayloadRegion::Direct(v) => v.to_col_ptr(data_ptr),
                // Decoded image (`cs == elem_width`), stable for the shard's
                // lifetime; the caller already keeps `self` alive for the view.
                PayloadRegion::Packed(p) => ColPtr {
                    base: self.packed_bytes(p).as_ptr(),
                    stride: cs,
                },
            };
        }

        let blob = self.blob_slice();
        UnifiedSource {
            pk,
            null_bmp,
            cols,
            blob_ptr: blob.as_ptr(),
            blob_len: blob.len(),
        }
    }
}

/// `MappedShard` is a [`RowSource`] but deliberately **not** a `BatchView`: a
/// shard column may be a `ScalarRegion::Constant`, which has a cell address but
/// no `rows * col_size` region to hand out.
impl gnitz_expr::RowSource for MappedShard {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        MappedShard::get_pk_bytes(self, row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.get_null_word(row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        self.get_col_ptr(row, payload_col, col_size)
    }
    #[inline(always)]
    fn blob(&self) -> &[u8] {
        self.blob_slice()
    }
}

impl super::super::columnar::ColumnarSource for MappedShard {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        MappedShard::get_weight(self, row)
    }
}
