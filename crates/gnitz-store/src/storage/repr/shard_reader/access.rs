//! Per-row accessors for [`MappedShard`] — the region reads, the PK-filter probe and
//! the OPK binary searches — plus the bulk `*_owned_batch` materializers.

use super::super::batch::{FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::super::batch_pool::{acquire_arena, Fill};
use super::super::layout::two_value_bit;
use super::super::merge::{
    prorated_blob_cap, relocate_german_string_vec, should_relocate_blob, BlobCacheGuard, ColPtr, UnifiedSource,
};
use super::{MappedShard, PackedRegion, PayloadRegion, RegionView, WeightRegion, ZERO_CELL};
use crate::schema::key::PkBuf;
use crate::schema::SchemaDescriptor;
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
    pub(crate) fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let width = self.pk_stride as usize;
        &self.data()[self.pk.row_off(row)..][..width]
    }

    /// `(pk_min, pk_max)` OPK bounds for this shard, in `self.pk_stride`-wide
    /// `PkBuf`s. Derived from `count`, never serialized. An empty shard
    /// (`count == 0`) has no row to read `get_pk_bytes` from, so it returns
    /// zero-key bounds; an empty shard is never probed, so these bounds are
    /// only a defensive backstop.
    pub(crate) fn pk_bounds(&self) -> (PkBuf, PkBuf) {
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
    pub(crate) fn get_weight(&self, row: usize) -> i64 {
        match &self.weight {
            WeightRegion::Direct(v) => read_i64_le(self.data(), v.row_off(row)),
            WeightRegion::TwoValue { value_a, value_b, bitvec_off } => {
                if two_value_bit(&self.data()[*bitvec_off..], row) {
                    *value_b
                } else {
                    *value_a
                }
            }
        }
    }

    /// The row's null word under the *reader's* schema: the file's raw word with
    /// every payload column the file predates forced to NULL (`null_pad_mask`,
    /// `0` for a full-width shard).
    #[inline]
    pub(crate) fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data(), self.null_bmp.row_off(row)) | self.null_pad_mask
    }

    #[inline]
    pub(crate) fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        match &self.col_regions[payload_col_idx] {
            PayloadRegion::Direct(v) => &self.data()[v.row_off(row)..][..col_size],
            // `col_size == elem_width`; the decoded image serves the same
            // `row * col_size` slice a direct region would.
            PayloadRegion::Packed(p) => &self.packed_bytes(p)[row * col_size..][..col_size],
            PayloadRegion::Absent => &ZERO_CELL[..col_size],
        }
    }

    #[inline]
    pub(crate) fn blob_slice(&self) -> &[u8] {
        &self.data()[self.blob_off..self.blob_off + self.blob_len]
    }

    /// Whether this shard carries a PK filter at all — distinguishes "no filter"
    /// from "filter admits it", which
    /// [`shard_filter_may_contain`](Self::shard_filter_may_contain) deliberately
    /// cannot. An assertion's question, not a read path's.
    pub(crate) fn has_shard_filter(&self) -> bool {
        self.shard_filter.is_some()
    }

    /// A shard carrying no filter admits every key.
    pub(crate) fn shard_filter_may_contain(&self, probe_key: u64) -> bool {
        match &self.shard_filter {
            Some(filter) => filter.may_contain(self.data(), probe_key),
            None => true,
        }
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
    pub(crate) fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        let stride = self.pk_stride as usize;
        let cp = self.pk_col_ptr();
        unsafe { super::super::columnar::seek_lower_bound(self.count, stride, cp, key) }
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. `key` must be
    /// exactly `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        let stride = self.pk_stride as usize;
        let cp = self.pk_col_ptr();
        unsafe { super::super::columnar::seek_advance_to(self.count, stride, cp, key, hint) }
    }

    /// Bulk-copy a contiguous slice of rows into an Batch.
    /// Bypasses per-row cursor overhead entirely — one memcpy per column.
    ///
    /// Picks the blob arm via [`should_relocate_blob`]: a narrow slice off a
    /// wide heap relocates only its own rows' strings, anything else copies the
    /// whole region.
    #[inline]
    pub(crate) fn slice_to_owned_batch(
        &self,
        start: usize,
        row_count: usize,
        schema: &crate::schema::SchemaDescriptor,
    ) -> super::super::batch::Batch {
        let relocate = should_relocate_blob(self.blob_len, self.count, row_count);
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

        // A skeleton shard has no payload bytes to materialize: this path
        // force-NULLs every absent column, dereferences every German-string cell,
        // and certifies the result against `schema`'s NOT NULL bits. A bounded
        // view's store must be read row-at-a-time through the cursor instead, so
        // its skeleton keys can be hydrated rather than handed out as NULL rows.
        debug_assert!(
            !self.skeleton,
            "slice_to_owned_batch_with on a skeleton shard: a bounded view's store must be read row-at-a-time",
        );

        if row_count == 0 {
            return Batch::empty_with_schema(schema);
        }

        let shard = self.data();

        // Compute the final columnar layout before allocating anything.
        let (strides, nr) = strides_from_schema(schema);
        let nr = nr as usize; // 4 + npc
        let (offsets, total_size) = compute_offsets(&strides, nr, row_count);

        // One allocation for all fixed-stride columnar data. Materializing a
        // whole shard is among the largest single allocations the engine makes,
        // so it goes through the shared arena path for its pool bypass (and its
        // undersized-buffer eviction and debug poison).
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
            // No bytes to expand, but the destination arena is uninitialised, so
            // the cells must be written rather than skipped. A NULL cell is zero.
            PayloadRegion::Absent => dst.fill(0),
        };
        let expand_weight = |region: &WeightRegion, dst: &mut [u8]| match region {
            WeightRegion::Direct(v) => expand_view(v, FIXED_REGION_BYTES, dst),
            WeightRegion::TwoValue { value_a, value_b, bitvec_off } => {
                let a_bytes = value_a.to_le_bytes();
                let b_bytes = value_b.to_le_bytes();
                let bitvec = &shard[*bitvec_off..];
                for i in 0..row_count {
                    let src = if two_value_bit(bitvec, start + i) {
                        &b_bytes[..]
                    } else {
                        &a_bytes[..]
                    };
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
        let null_dst = &mut data[offsets[REG_NULL_BMP]..][..sz8];
        expand_view(&self.null_bmp, FIXED_REGION_BYTES, null_dst);
        // Force every payload column this file predates to NULL, the same way
        // `get_null_word` does for the per-row path. No-op for a full-width shard.
        if self.null_pad_mask != 0 {
            for word in null_dst.chunks_exact_mut(8) {
                let padded = read_u64_le(word, 0) | self.null_pad_mask;
                word.copy_from_slice(&padded.to_le_bytes());
            }
        }

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
            let cap = prorated_blob_cap(self.blob_len, self.count, row_count);
            let mut out = acquire_arena(cap, Fill::Reserve);
            let mut guard = BlobCacheGuard::acquire(schema, row_count);
            let src_blob = self.blob_slice();
            for (pi, col) in schema.payload_columns() {
                if !gnitz_wire::is_german_string(col.type_code) {
                    continue;
                }
                let off = offsets[REG_PAYLOAD_START + pi];
                let dst = &mut data[off..off + row_count * 16];
                for (i, cell) in dst.chunks_exact_mut(16).enumerate() {
                    // Read through `get_col_ptr` so every `PayloadRegion` form —
                    // `Direct` (per-row or constant-stride), `Packed`, `Absent` —
                    // is handled the same way the bulk fill handles them.
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

        let mut batch = unsafe { Batch::from_prebuilt(data, blob, strides, offsets, row_count, *schema) };
        // Shards are written consolidated; a contiguous slice stays (PK, payload)-
        // sorted and ghost-free (shards are ghost-free by construction). Certify it.
        batch.certify_layout(Layout::Consolidated, schema);
        batch
    }

    /// Derive a `UnifiedSource` view over this shard: each fixed-width region
    /// becomes a `(base, stride)` `ColPtr` into the shard's mmap, carrying the
    /// [`RegionView`](super::RegionView)'s own stride — so a constant region's
    /// `stride == 0` makes `base.add(ri * stride) == base` read the same bytes
    /// for every row. Pure pointer arithmetic — no allocation, no scan. The
    /// returned `ColPtr`s alias the mapped memory, so the caller must keep `self`
    /// alive for as long as the view is read.
    ///
    /// The shard-side counterpart of `repr::merge::mem_batch_to_unified` (which
    /// documents the `cols` table); shared by the read-cursor drain
    /// (shard-vs-`MemBatch` polymorphism) and shard compaction.
    pub(crate) fn to_unified(&self, schema: &SchemaDescriptor, cols: &mut Vec<ColPtr>) -> UnifiedSource {
        let data_ptr = self.data().as_ptr();

        let pk = self.pk.to_col_ptr(data_ptr);
        let null_bmp = self.null_bmp.to_col_ptr(data_ptr);

        let cols_off = cols.len();
        for (pi, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            cols.push(match &self.col_regions[pi] {
                PayloadRegion::Direct(v) => v.to_col_ptr(data_ptr),
                // Decoded image (`cs == elem_width`), stable for the shard's
                // lifetime; the caller already keeps `self` alive for the view.
                PayloadRegion::Packed(p) => ColPtr {
                    base: self.packed_bytes(p).as_ptr(),
                    stride: cs,
                },
                // Every row reads the same `'static` zero cell, the shape a
                // `stride == 0` constant region already uses, so the gather has
                // no per-row branch. The scatter gathers every schema payload
                // column regardless of the null bit, so the pointer must be real.
                PayloadRegion::Absent => ColPtr { base: ZERO_CELL.as_ptr(), stride: 0 },
            });
        }

        let blob = self.blob_slice();
        UnifiedSource {
            pk,
            null_bmp,
            null_pad_mask: self.null_pad_mask,
            cols_off,
            blob_ptr: blob.as_ptr(),
            blob_len: blob.len(),
        }
    }
}

/// `MappedShard` is a [`RowSource`] but deliberately **not** a `BatchView`: a
/// shard column may be a constant region (a `RegionView` whose `stride` is 0) or
/// [`PayloadRegion::Absent`](super::PayloadRegion::Absent), which reads a shared
/// 16-byte cell — each has a cell address but no `rows * col_size` region to
/// hand out.
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
    #[inline(always)]
    fn row_count(&self) -> usize {
        self.count
    }
}

impl super::super::columnar::ColumnarSource for MappedShard {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        MappedShard::get_weight(self, row)
    }
    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        MappedShard::is_skeleton(self)
    }
}
