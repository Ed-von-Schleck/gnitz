//! Hot per-row accessors for [`MappedShard`]: the self-describing
//! [`ScalarRegion`] / [`WeightRegion`] reads (`get_pk_bytes`, `get_weight`, `get_null_word`,
//! `get_col_ptr`, …), the XOR8 probe, the OPK binary-search helpers, and the
//! bulk `*_owned_batch` materializers. All `#[inline]`; the comparators read
//! every stride/offset straight off the mapped regions.

use std::ptr;

use super::super::batch::FIXED_REGION_BYTES;
use super::super::merge::{ColPtr, UnifiedSource};
use super::super::xor8;
use super::{MappedShard, PackedRegion, PayloadRegion, ScalarRegion, WeightRegion};
use crate::foundation::codec::{read_i64_le, read_u64_le};
use crate::schema::key::PkBuf;
use crate::schema::{SchemaDescriptor, MAX_COLUMNS};

impl ScalarRegion {
    /// This region as a uniform `(base, stride)` [`ColPtr`]: `Raw` points into
    /// `data_ptr` at its mmap offset with the given `stride`; `Constant` points
    /// at its inline value with `stride: 0`, so `base.add(i*stride) == base`
    /// reads the same bytes for every row with no per-row branch. The one
    /// Raw/Constant→`ColPtr` conversion behind `pk_col_ptr` and `to_unified`.
    #[inline]
    fn to_col_ptr(&self, data_ptr: *const u8, stride: usize) -> ColPtr {
        match self {
            ScalarRegion::Raw { offset, .. } => ColPtr {
                base: unsafe { data_ptr.add(*offset) },
                stride,
            },
            ScalarRegion::Constant { value, .. } => ColPtr {
                base: value.as_ptr(),
                stride: 0,
            },
        }
    }
}

impl MappedShard {
    #[inline]
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

    // The value accessor: production reads PK regions as raw OPK bytes
    // (`get_pk_bytes`); only tests recover the native value via `get_pk`.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn get_pk(&self, row: usize) -> u128 {
        let stride = self.pk_stride as usize;
        let data = self.data();
        match &self.pk {
            ScalarRegion::Raw { offset, .. } => {
                let src = &data[offset + row * stride..offset + row * stride + stride];
                gnitz_wire::widen_pk_be(src, stride)
            }
            // The Constant region stores the OPK bytes left-aligned at
            // `value[..stride]`. `widen_pk_be` right-aligns the active bytes,
            // recovering the native unsigned value (sign-flipped for signed).
            ScalarRegion::Constant { value, .. } => gnitz_wire::widen_pk_be(&value[..stride], stride),
        }
    }

    #[inline]
    pub fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let stride = self.pk_stride as usize;
        let data = self.data();
        match &self.pk {
            ScalarRegion::Raw { offset, .. } => &data[offset + row * stride..offset + row * stride + stride],
            // value is [u8; 16]; stride <= 16 is guaranteed by the constructor.
            // When ScalarRegion::Constant is widened to hold larger values,
            // this arm must be updated alongside it.
            ScalarRegion::Constant { value, .. } => &value[..stride],
        }
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
            let e = PkBuf::empty(self.pk_stride);
            (e, e)
        }
    }

    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        match &self.weight {
            WeightRegion::Raw { offset, .. } => read_i64_le(self.data(), offset + row * 8),
            WeightRegion::Constant { value } => i64::from_le_bytes(value[..8].try_into().unwrap()),
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
        match &self.null_bmp {
            ScalarRegion::Raw { offset, .. } => read_u64_le(self.data(), offset + row * 8),
            ScalarRegion::Constant { value, .. } => u64::from_le_bytes(value[..8].try_into().unwrap()),
        }
    }

    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        match &self.col_regions[payload_col_idx] {
            PayloadRegion::Scalar(ScalarRegion::Raw { offset, size }) => {
                let start = offset + row * col_size;
                // Region size >= count * stride is validated once at open; the
                // safe slice below still bounds-checks against the mmap.
                debug_assert!(
                    start + col_size <= offset + size,
                    "get_col_ptr out of bounds: row={} payload_col={} col_size={} region_end={}",
                    row,
                    payload_col_idx,
                    col_size,
                    offset + size,
                );
                &self.data()[start..start + col_size]
            }
            PayloadRegion::Scalar(ScalarRegion::Constant { value, .. }) => &value[..col_size],
            // `col_size == elem_width`; the decoded image serves the same
            // `row * col_size` slice the Raw arm would.
            PayloadRegion::Packed(p) => {
                let start = row * col_size;
                &self.packed_bytes(p)[start..start + col_size]
            }
        }
    }

    /// Get a raw pointer to a column value, indexed by *logical* column index.
    /// For the PK column, returns a pointer into the pk region (16 bytes).
    /// Returns null for out-of-range column indices.
    #[inline]
    pub fn col_ptr_by_logical(&self, row: usize, col_idx: usize, col_size: usize) -> *const u8 {
        if col_idx >= self.col_to_payload.len() {
            return ptr::null();
        }
        let payload_idx = self.col_to_payload[col_idx];
        let base = self.mmap.as_ptr();
        if payload_idx == usize::MAX {
            // PK column (pk_stride bytes)
            match &self.pk {
                ScalarRegion::Raw { offset, .. } => {
                    let stride = self.pk_stride as usize;
                    let off = offset + row * stride;
                    if off + stride > self.mmap.len() {
                        return ptr::null();
                    }
                    return unsafe { base.add(off) };
                }
                ScalarRegion::Constant { offset, .. } => {
                    return unsafe { base.add(*offset) };
                }
            }
        }
        if payload_idx >= self.col_regions.len() {
            return ptr::null();
        }
        match &self.col_regions[payload_idx] {
            PayloadRegion::Scalar(ScalarRegion::Raw { offset, size }) => {
                let off = offset + row * col_size;
                if off + col_size > offset + size {
                    return ptr::null();
                }
                unsafe { base.add(off) }
            }
            PayloadRegion::Scalar(ScalarRegion::Constant { offset, .. }) => unsafe { base.add(*offset) },
            // Point into the decoded image (col_size == elem_width). The address
            // is stable for the shard's lifetime, matching the mmap-pointer
            // contract of the Raw/Constant arms.
            PayloadRegion::Packed(p) => {
                let bytes = self.packed_bytes(p);
                let off = row * col_size;
                if off + col_size > bytes.len() {
                    return ptr::null();
                }
                unsafe { bytes.as_ptr().add(off) }
            }
        }
    }

    #[inline]
    pub fn blob_slice(&self) -> &[u8] {
        &self.data()[self.blob_off..self.blob_off + self.blob_len]
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

    /// The PK region as a uniform [`ColPtr`] view — the single addressing source
    /// for the OPK seeks below (via [`ColPtr::row`]) and `to_unified`'s PK
    /// column. The `Constant` arm's `stride: 0` lets the seek closures read every
    /// probe through one branchless accessor, hoisting the Raw/Constant match out
    /// of the search loop. The base aliases `self`; keep `self` alive while the
    /// view is read (the seek closures run synchronously within the call).
    #[inline]
    fn pk_col_ptr(&self) -> ColPtr {
        self.pk.to_col_ptr(self.data().as_ptr(), self.pk_stride as usize)
    }

    /// First row whose OPK bytes are `>= key`. After the OPK-at-rest flip this
    /// is a raw `memcmp` binary search — correct at every PK width with no
    /// schema dependency. `key` must be exactly `pk_stride` OPK bytes.
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
    #[allow(clippy::uninit_vec)]
    pub(crate) fn slice_to_owned_batch(
        &self,
        start: usize,
        row_count: usize,
        schema: &crate::schema::SchemaDescriptor,
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

        // One allocation for all fixed-stride columnar data.
        let mut data = super::super::batch_pool::acquire_buf();
        data.clear();
        data.reserve(total_size);
        unsafe { data.set_len(total_size) };

        // Write each region directly into its final slice — no intermediate
        // buffers. The Raw/Constant fill is shared by both expanders; only the
        // weight region can carry the TwoValue bitvec form.
        let copy_raw = |offset: usize, stride: usize, dst: &mut [u8]| {
            let begin = offset + start * stride;
            dst.copy_from_slice(&shard[begin..begin + row_count * stride]);
        };
        let fill_const = |value: &[u8; 16], stride: usize, dst: &mut [u8]| {
            for chunk in dst.chunks_exact_mut(stride) {
                chunk.copy_from_slice(&value[..stride]);
            }
        };
        let expand_scalar = |region: &ScalarRegion, stride: usize, dst: &mut [u8]| match region {
            ScalarRegion::Raw { offset, .. } => copy_raw(*offset, stride, dst),
            ScalarRegion::Constant { value, .. } => fill_const(value, stride, dst),
        };
        let expand_payload = |region: &PayloadRegion, stride: usize, dst: &mut [u8]| match region {
            PayloadRegion::Scalar(s) => expand_scalar(s, stride, dst),
            // Payload strides equal `elem_width`, so the decoded image is
            // `count · stride` long.
            PayloadRegion::Packed(p) => {
                let bytes = self.packed_bytes(p);
                dst.copy_from_slice(&bytes[start * stride..(start + row_count) * stride]);
            }
        };
        let expand_weight = |region: &WeightRegion, dst: &mut [u8]| match region {
            WeightRegion::Raw { offset, .. } => copy_raw(*offset, 8, dst),
            WeightRegion::Constant { value } => fill_const(value, 8, dst),
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
        expand_scalar(&self.pk, pk_stride, &mut data[offsets[0]..][..row_count * pk_stride]);
        expand_weight(&self.weight, &mut data[offsets[1]..][..sz8]);
        expand_scalar(&self.null_bmp, 8, &mut data[offsets[2]..][..sz8]);

        for (pi, _ci, col) in schema.payload_columns() {
            let stride = col.size() as usize;
            let off = offsets[3 + pi];
            let sz = row_count * stride;
            expand_payload(&self.col_regions[pi], stride, &mut data[off..][..sz]);
        }

        // Blob: one allocation, copy entire blob region (string offsets stay valid).
        let blob = if self.blob_len > 0 {
            let src = &shard[self.blob_off..self.blob_off + self.blob_len];
            let mut buf = super::super::batch_pool::acquire_buf();
            buf.clear();
            buf.reserve(self.blob_len);
            unsafe { buf.set_len(self.blob_len) };
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

    /// Bulk-copy all rows into an Batch.
    pub(crate) fn to_owned_batch(&self, schema: &crate::schema::SchemaDescriptor) -> super::super::batch::Batch {
        self.slice_to_owned_batch(0, self.count, schema)
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

        let pk = self.pk.to_col_ptr(data_ptr, self.pk_stride as usize);
        let null_bmp = self.null_bmp.to_col_ptr(data_ptr, FIXED_REGION_BYTES);

        let mut cols = [ColPtr {
            base: ptr::null(),
            stride: 0,
        }; MAX_COLUMNS - 1];
        for (pi, _ci, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            cols[pi] = match &self.col_regions[pi] {
                PayloadRegion::Scalar(s) => s.to_col_ptr(data_ptr, cs),
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

impl super::super::columnar::ColumnarSource for MappedShard {
    #[inline]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        MappedShard::get_pk_bytes(self, row)
    }
    #[inline]
    fn get_weight(&self, row: usize) -> i64 {
        MappedShard::get_weight(self, row)
    }
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
