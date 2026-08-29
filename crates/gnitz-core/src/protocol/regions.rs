//! A client `ZSetBatch` in the §6 region shape — the one form everything
//! downstream reads: `gnitz_wire::wal::encode` frames these regions into a WAL
//! block, and the shared `gnitz-expr` evaluator runs its kernels over them.
//!
//! One builder serves both. The encode path and the evaluator want the same
//! list — PK, weight, null bitmap, one region per payload slot in slot order,
//! blob heap last — so the §6 rule for a `ZSetBatch` is stated here once.
//!
//! Every [`gnitz_expr::RowSource`]/[`gnitz_expr::BatchView`] method below is
//! `#[inline(always)]`; see [`gnitz_expr::BatchView`] for why the plain hint is
//! not enough.

use super::types::{null_word_get, ColData, PkColumn, Schema, ZSetBatch};
use gnitz_wire::{as_le_bytes, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK};

// ── The region builder ───────────────────────────────────────────────────────

/// The buffers a `ZSetBatch` does **not** already hold in §6 region form: the
/// OPK PK region (a [`PkColumn`] holds native LE values or on-wire LE bytes,
/// never OPK) and the 16-byte German-string cells plus the blob arena they
/// point into. A `Fixed` column *is* a region and is borrowed in place.
///
/// Hoist one above a loop over batches: these buffers keep their capacity
/// across views. The region *list* itself is rebuilt per view.
#[derive(Default)]
pub struct ViewBuffers {
    pk_region: Vec<u8>,
    /// Indexed by payload slot: `rows * 16` German cells for a String/Blob
    /// column, empty for every other slot.
    str_cols: Vec<Vec<u8>>,
    blob: Vec<u8>,
}

impl ViewBuffers {
    /// Refill from `batch` and lend a view over it. The only way to obtain a
    /// [`ZSetBatchView`]: buffers and batch are paired in one call, so a view
    /// over stale buffers cannot be written, and the batch cannot be mutated
    /// while a view is live. The view also holds the buffers, so a second
    /// concurrent view over the same `ViewBuffers` is a compile error — a caller
    /// that needs two live views owns two `ViewBuffers`.
    pub fn view<'a>(&'a mut self, batch: &'a ZSetBatch, schema: &Schema) -> ZSetBatchView<'a> {
        let pk_stride = schema.pk_stride();
        let regions = self.regions(batch, schema);
        // The three regions the evaluator reads per row are hoisted out of the
        // list; `col_data` / `null_bmp` are read once per morsel and stay indexed.
        let (pk, blob) = (regions[REG_PK], regions[regions.len() - 1]);
        ZSetBatchView {
            regions,
            pk,
            blob,
            batch,
            pk_stride,
            rows: batch.len(),
        }
    }

    /// `batch` as the §6 canonical region list: PK, weight, null bitmap, one
    /// region per payload slot in slot order, blob heap last — exactly what
    /// [`gnitz_wire::wal::encode`] frames.
    ///
    /// Every region is indexed absolutely — by a kernel (`col_data(pi, sz)[row *
    /// sz ..]`, `get_pk_bytes`) or by the framer — so a column whose variant or
    /// length contradicts the schema is an out-of-bounds read one crate away
    /// from its cause. `check_columns` is the same rule `ZSetBatch::validate`
    /// applies on the push path; a view can be built for a batch that never went
    /// through it.
    pub(crate) fn regions<'a>(&'a mut self, batch: &'a ZSetBatch, schema: &Schema) -> Vec<&'a [u8]> {
        let (rows, pk_stride, npc) = (batch.len(), schema.pk_stride(), schema.num_payload_cols());
        batch.check_columns(schema).expect("ZSetBatch columns match schema");

        build_pk_region_into(&mut self.pk_region, &batch.pks, pk_stride, schema);
        self.blob.clear();
        self.str_cols.resize_with(npc, Vec::new);
        // String and Blob are separate arms because their cell iterators have
        // different concrete types, so one arm could not produce both.
        for (pi, ci, _) in schema.payload_columns() {
            match &batch.columns[ci] {
                ColData::Strings(v) => encode_german_col_into(
                    &mut self.str_cols[pi],
                    &batch.nulls,
                    pi,
                    v.iter().map(|o| o.as_deref().map(str::as_bytes)),
                    &mut self.blob,
                ),
                ColData::Bytes(v) => encode_german_col_into(
                    &mut self.str_cols[pi],
                    &batch.nulls,
                    pi,
                    v.iter().map(|o| o.as_deref()),
                    &mut self.blob,
                ),
                ColData::Fixed(_) => {}
            }
        }

        // Buffers are final; the shared reborrow lends them for the caller's
        // lifetime. The element type is declared so both arms coerce to `&[u8]`.
        let me = &*self;
        let mut regions: Vec<&[u8]> = Vec::with_capacity(gnitz_wire::wal::num_regions(npc));
        regions.push(&me.pk_region);
        regions.push(as_le_bytes(&batch.weights));
        regions.push(as_le_bytes(&batch.nulls));
        assert_eq!(me.pk_region.len(), rows * pk_stride, "pk region length");
        for (pi, ci, _) in schema.payload_columns() {
            regions.push(match &batch.columns[ci] {
                ColData::Fixed(v) => v,
                // The 16-byte German cells this builder just wrote, one per row.
                ColData::Strings(_) | ColData::Bytes(_) => &me.str_cols[pi],
            });
        }
        regions.push(&me.blob); // the blob arena is always the last region
        regions
    }
}

/// Build the PK region as **order-preserving big-endian** (OPK) bytes. The
/// in-memory [`PkColumn`] holds native LE values; this is the single client-side
/// encode point (the server stores the region verbatim and `decode_wal_block`
/// does the inverse). `schema` supplies per-column type codes for signed
/// sign-flipping. The framer places the region at its aligned offset, so this
/// writes just the tightly-packed region bytes.
fn build_pk_region_into(dst: &mut Vec<u8>, pks: &PkColumn, pk_stride: usize, schema: &Schema) {
    debug_assert_eq!(pks.stride as usize, pk_stride, "PK column stride != schema stride");
    let row_count = pks.buf.len().checked_div(pk_stride).unwrap_or(0);
    resize_zeroed(dst, pks.buf.len());
    // Collect (col_size, type_code) once; avoids schema re-iteration per row. A
    // scalar PK is the one-column case of the same walk: unsigned → plain
    // big-endian, signed (including a lone 16-byte I128 join key) → big-endian
    // with the leading sign bit flipped.
    let col_info: Vec<(usize, u8)> = schema.pk_col_codes().collect();
    for row in 0..row_count {
        let (lo, hi) = (row * pk_stride, (row + 1) * pk_stride);
        gnitz_wire::encode_pk_tuple(col_info.iter().copied(), &pks.buf[lo..hi], &mut dst[lo..hi]);
    }
}

/// `dst` as exactly `n` zeroed bytes, reusing its capacity. Not `Vec::resize`:
/// that is a per-element write loop, which LLVM turns into a memset only from
/// `-O1` up — at `opt-level=0`, the build the whole E2E suite runs, it costs
/// ~43 instructions per byte on every client push.
fn resize_zeroed(dst: &mut Vec<u8>, n: usize) {
    dst.clear();
    dst.reserve(n);
    // SAFETY: `reserve` guarantees `n` bytes of capacity, and they are zeroed
    // before `set_len` publishes them, so no uninitialized byte is observable.
    unsafe {
        std::ptr::write_bytes(dst.as_mut_ptr(), 0, n);
        dst.set_len(n);
    }
}

/// Encode a STRING/BLOB column region: one 16-byte German-string struct per row
/// (a zeroed struct for a null or `None` cell), spilling long values into
/// `blob`. Shared by the STRING and BLOB arms — both are byte-oriented German
/// strings, differing only in the source cell type.
fn encode_german_col_into<'a>(
    dst: &mut Vec<u8>,
    nulls: &[u64],
    payload_idx: usize,
    cells: impl ExactSizeIterator<Item = Option<&'a [u8]>>,
    blob: &mut Vec<u8>,
) {
    dst.clear();
    dst.reserve(cells.len() * 16);
    for (row, val) in cells.enumerate() {
        let is_null = null_word_get(nulls[row], payload_idx);
        if let (false, Some(b)) = (is_null, val) {
            dst.extend_from_slice(&gnitz_wire::encode_german_string(b, blob));
        } else {
            dst.extend_from_slice(&[0u8; 16]);
        }
    }
}

// ── The view ─────────────────────────────────────────────────────────────────

/// A [`ZSetBatch`] presented as §6 regions. Every accessor is one index — the
/// slot-to-buffer question was answered once, when the list was built.
///
/// `pk` and `blob` are the same two regions the list already holds, hoisted out
/// of it because they are read per row.
/// `gnitz_expr::assert_batchview_consistent` pins the region and per-row
/// readings against each other.
///
/// The view also carries the batch it was built over, so a caller that needs
/// both — registers through the evaluator, a `ColData` cell directly — passes
/// one value instead of a `(&view, &batch)` pair that would type-check even when
/// mismatched.
pub struct ZSetBatchView<'a> {
    regions: Vec<&'a [u8]>,
    pk: &'a [u8],
    blob: &'a [u8],
    batch: &'a ZSetBatch,
    pk_stride: usize,
    /// Cached rather than re-derived: `ZSetBatch::len` divides the PK buffer
    /// length by a runtime stride, and `row_count` is read once per morsel and
    /// once per `eval_row` — which the DML row loop drives with `m = 1` per row.
    rows: usize,
}

impl<'a> ZSetBatchView<'a> {
    /// The batch this view presents.
    pub fn batch(&self) -> &'a ZSetBatch {
        self.batch
    }
}

impl gnitz_expr::RowSource for ZSetBatchView<'_> {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let s = self.pk_stride;
        &self.pk[row * s..row * s + s]
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.batch.nulls[row]
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, pi: usize, sz: usize) -> &[u8] {
        &self.regions[REG_PAYLOAD_START + pi][row * sz..row * sz + sz]
    }

    #[inline(always)]
    fn blob(&self) -> &[u8] {
        self.blob
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.rows
    }
}

impl gnitz_expr::BatchView for ZSetBatchView<'_> {
    /// The slot was resolved when the list was built, so `col_size` only has to
    /// agree — it comes from the program's resolve-time schema, the region from
    /// the view's, and this is the one place the two meet.
    #[inline(always)]
    fn col_data(&self, pi: usize, col_size: usize) -> &[u8] {
        let region = self.regions[REG_PAYLOAD_START + pi];
        debug_assert_eq!(
            region.len(),
            self.rows * col_size,
            "col_data({pi}, {col_size}) width mismatch",
        );
        region
    }

    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        self.regions[REG_NULL_BMP]
    }

    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (self.pk, self.pk_stride)
    }
}

#[cfg(test)]
#[path = "tests/regions.rs"]
mod tests;
