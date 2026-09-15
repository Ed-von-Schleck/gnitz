//! Per-row accessors for [`MappedShard`] — the region reads, the PK-filter probe and
//! the OPK binary searches — plus the bulk `*_owned_batch` materializers.

use super::super::batch::{write_to_batch, Batch, Layout, FIXED_REGION_BYTES};
use super::super::columnar::ColumnarSource;
use super::super::layout::{for_image_len, two_value_bit};
use super::super::merge::{prorated_blob_cap, should_relocate_blob, ColPtr, UnifiedSource};
use super::{MappedShard, PackedRegion, PayloadRegion, WeightRegion};
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;
use gnitz_wire::{read_i64_le, read_u64_le};

impl MappedShard {
    /// `inline(always)` rather than the plain hint: the blob, the TwoValue
    /// weight, the filter probe and the FoR image read through this, and the
    /// plain hint is a no-op at `opt-level=0` — the debug binary the E2E suite
    /// runs would pay a frame per such read.
    #[inline(always)]
    pub(crate) fn data(&self) -> &[u8] {
        self.mmap.as_slice()
    }

    /// Materialize a [`PackedRegion`] (FoR payload) to its full
    /// `count × elem_width` little-endian raw image, decoding once per shard
    /// open and caching it in the region's `OnceCell`; its content address is
    /// stable for the shard's lifetime.
    fn packed_bytes<'a>(&'a self, region: &'a PackedRegion) -> &'a [u8] {
        region.decoded.get_or_init(|| {
            let mut out = vec![0u8; self.count * region.elem_width].into_boxed_slice();
            super::decode_for_region(
                &self.data()[region.offset..][..for_image_len(self.count, region.bw)],
                region.bw,
                region.elem_width,
                &mut out,
            );
            out
        })
    }

    /// Payload column `pi` as a [`ColPtr`] whose stride is the region's own
    /// element width (0 for a constant or predated column).
    #[inline(always)]
    fn payload_col(&self, pi: usize) -> ColPtr {
        match &self.col_regions[pi] {
            PayloadRegion::Mapped(cp) => *cp,
            PayloadRegion::Packed(p) => ColPtr {
                base: self.packed_bytes(p).as_ptr(),
                stride: p.elem_width,
            },
        }
    }

    // Production reads PK regions as raw OPK bytes (`get_pk_bytes`); only tests
    // need the native value back.
    #[cfg(test)]
    pub(crate) fn get_pk(&self, row: usize) -> u128 {
        gnitz_wire::widen_pk_be(RowSource::get_pk_bytes(self, row))
    }

    /// Whether this shard carries a PK filter at all — distinguishes "no filter"
    /// from "filter admits it", which
    /// [`shard_filter_may_contain`](Self::shard_filter_may_contain) deliberately
    /// cannot. An assertion's question, not a read path's.
    #[cfg(test)]
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

    /// First row whose OPK bytes are `>= key`. A raw `memcmp` binary search —
    /// correct at every PK width with no schema dependency. `key` must be
    /// exactly `pk_stride` OPK bytes.
    pub(crate) fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        unsafe { super::super::columnar::seek_lower_bound(self.count, self.pk_stride, self.pk, key) }
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. `key` must be
    /// exactly `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        unsafe { super::super::columnar::seek_advance_to(self.count, self.pk_stride, self.pk, key, hint) }
    }

    /// Bulk-copy a contiguous slice of rows into an Batch.
    /// Bypasses per-row cursor overhead entirely — one memcpy per column.
    ///
    /// Picks the blob arm via [`should_relocate_blob`]: a narrow slice off a
    /// wide heap relocates only its own rows' strings, anything else copies the
    /// whole region.
    #[inline]
    pub(crate) fn slice_to_owned_batch(&self, start: usize, row_count: usize, schema: &SchemaDescriptor) -> Batch {
        let relocate = should_relocate_blob(self.blob_len, self.count, row_count);
        self.slice_to_owned_batch_with(start, row_count, schema, relocate)
    }

    /// [`slice_to_owned_batch`](Self::slice_to_owned_batch) with the blob arm
    /// passed in rather than derived, so both arms can be forced on one slice
    /// (`slice_blob_relocate_bench` and the padded-string test).
    pub(super) fn slice_to_owned_batch_with(
        &self,
        start: usize,
        row_count: usize,
        schema: &SchemaDescriptor,
        relocate: bool,
    ) -> Batch {
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
        assert!(start + row_count <= self.count, "slice out of range");

        let pk_stride = self.pk_stride;
        // A constant region (stride 0) is its one element repeated: write it once,
        // then double the written prefix, so any element width costs O(log rows)
        // copies. Any other region is contiguous.
        let copy_rows = |cp: ColPtr, width: usize, dst: &mut [u8]| {
            if cp.stride == 0 {
                dst[..width].copy_from_slice(unsafe { cp.row(0, width) });
                let mut filled = width;
                while filled < dst.len() {
                    let n = filled.min(dst.len() - filled);
                    dst.copy_within(..n, filled);
                    filled += n;
                }
            } else {
                dst.copy_from_slice(unsafe { std::slice::from_raw_parts(cp.row_ptr(start), dst.len()) });
            }
        };
        let blob_cap = if relocate {
            prorated_blob_cap(self.blob_len, self.count, row_count)
        } else {
            self.blob_len
        };
        let mut batch = write_to_batch(schema, row_count, blob_cap, |w| {
            copy_rows(self.pk, pk_stride, w.pk);
            match &self.weight {
                WeightRegion::Mapped(cp) => copy_rows(*cp, FIXED_REGION_BYTES, w.weight),
                WeightRegion::TwoValue { value_a, value_b, bitvec_off } => {
                    let bitvec = &self.data()[*bitvec_off..];
                    for (i, cell) in w.weight.as_chunks_mut::<8>().0.iter_mut().enumerate() {
                        let v = if two_value_bit(bitvec, start + i) {
                            value_b
                        } else {
                            value_a
                        };
                        *cell = v.to_le_bytes();
                    }
                }
            }
            copy_rows(self.null_bmp, FIXED_REGION_BYTES, w.null_bmp);
            // Force every payload column this file predates to NULL, the same way
            // `get_null_word` does for the per-row path. No-op for a full-width shard.
            if self.null_pad_mask != 0 {
                for word in w.null_bmp.as_chunks_mut::<8>().0 {
                    *word = (u64::from_le_bytes(*word) | self.null_pad_mask).to_le_bytes();
                }
            }
            for (pi, col) in schema.payload_columns() {
                let cp = self.payload_col(pi);
                if relocate && gnitz_wire::is_german_string(col.type_code) {
                    for i in 0..row_count {
                        w.write_string_cell(pi, unsafe { cp.row(start + i, 16) }, self.blob(), i);
                    }
                } else {
                    copy_rows(cp, col.size() as usize, w.col_bufs[pi]);
                }
            }
            if !relocate {
                w.copy_blob_verbatim(self.blob());
            }
            w.count = row_count;
        });
        // Shards are written consolidated; a contiguous slice stays (PK, payload)-
        // sorted and ghost-free.
        batch.certify_layout(Layout::Consolidated);
        batch
    }

    /// Derive a `UnifiedSource` view over this shard: its region `ColPtr`s, with
    /// one payload `ColPtr` per reader-schema column appended to `cols`. A
    /// packed column is decoded here if no read has decoded it yet.
    pub(crate) fn to_unified(&self, cols: &mut Vec<ColPtr>) -> UnifiedSource<'_> {
        let cols_off = cols.len();
        cols.extend((0..self.col_regions.len()).map(|pi| self.payload_col(pi)));
        UnifiedSource {
            pk: self.pk,
            null_bmp: self.null_bmp,
            null_pad_mask: self.null_pad_mask,
            cols_off,
            blob: self.blob(),
        }
    }
}

impl RowSource for MappedShard {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        debug_assert!(row < self.count);
        unsafe { self.pk.row(row, self.pk_stride) }
    }

    /// The row's null word under the *reader's* schema: the file's raw word with
    /// every payload column the file predates forced to NULL (`null_pad_mask`,
    /// `0` for a full-width shard).
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        debug_assert!(row < self.count);
        read_u64_le(unsafe { self.null_bmp.row(row, 8) }, 0) | self.null_pad_mask
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        debug_assert!(row < self.count);
        unsafe { self.payload_col(payload_col).row(row, col_size) }
    }

    #[inline(always)]
    fn blob(&self) -> &[u8] {
        &self.data()[self.blob_off..][..self.blob_len]
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.count
    }
}

impl ColumnarSource for MappedShard {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        debug_assert!(row < self.count);
        match &self.weight {
            WeightRegion::Mapped(cp) => read_i64_le(unsafe { cp.row(row, 8) }, 0),
            WeightRegion::TwoValue { value_a, value_b, bitvec_off } => {
                if two_value_bit(&self.data()[*bitvec_off..], row) {
                    *value_b
                } else {
                    *value_a
                }
            }
        }
    }

    /// Whether this file is a bounded view's skeleton shard.
    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        self.skeleton
    }
}
