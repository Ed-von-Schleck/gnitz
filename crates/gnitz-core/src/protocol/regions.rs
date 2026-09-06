//! A client `ZSetBatch` in the §6 region shape — the one form everything
//! downstream reads: `gnitz_wire::wal::encode` frames these regions into a WAL
//! block, and the shared `gnitz-expr` evaluator runs its kernels over them.
//!
//! A batch already holds every region in place, so this is a list of borrows:
//! PK, weight, null bitmap, one region per payload slot in slot order, blob heap
//! last.
//!
//! Every [`gnitz_expr::RowSource`]/[`gnitz_expr::BatchView`] method below is
//! `#[inline(always)]`; see [`gnitz_expr::BatchView`] for why the plain hint is
//! not enough.

use super::types::{Schema, ZSetBatch};
use gnitz_wire::{as_le_bytes, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK};

/// `batch` as the §6 canonical region list — exactly what
/// [`gnitz_wire::wal::encode`] frames.
///
/// Every region is indexed absolutely — by a kernel or by the framer — so a
/// column whose length contradicts the schema would read out of bounds one crate
/// away from its cause. `check_columns` is the same rule `ZSetBatch::validate`
/// applies on the push path, restated here because a list can be built for a
/// batch that never went through it.
pub(crate) fn regions<'a>(batch: &'a ZSetBatch, schema: &Schema) -> Vec<&'a [u8]> {
    let npc = schema.num_payload_cols();
    batch.check_columns(schema).expect("ZSetBatch columns match schema");
    let mut regions: Vec<&[u8]> = Vec::with_capacity(gnitz_wire::wal::num_regions(npc));
    regions.push(batch.pks.region());
    regions.push(as_le_bytes(&batch.weights));
    regions.push(as_le_bytes(&batch.nulls));
    regions.extend(schema.payload_columns().map(|(_, ci, _)| batch.columns[ci].as_slice()));
    regions.push(&batch.blob); // the blob arena is always the last region
    regions
}

// ── The view ─────────────────────────────────────────────────────────────────

/// A [`ZSetBatch`] presented as §6 regions. Every accessor is one index — the
/// slot-to-region question was answered once, when the list was built.
/// `gnitz_expr::assert_batchview_consistent` pins the region and per-row
/// readings against each other.
///
/// The view also carries the batch it was built over, so a caller that needs
/// both — registers through the evaluator, a raw cell directly — passes one
/// value instead of a `(&view, &batch)` pair that would type-check even when
/// mismatched.
pub struct ZSetBatchView<'a> {
    regions: Vec<&'a [u8]>,
    /// The two regions the evaluator reads per row, hoisted out of the list.
    pk: &'a [u8],
    blob: &'a [u8],
    batch: &'a ZSetBatch,
    pk_stride: usize,
}

impl<'a> ZSetBatchView<'a> {
    pub fn new(batch: &'a ZSetBatch, schema: &Schema) -> Self {
        let regions = regions(batch, schema);
        let (pk, blob) = (regions[REG_PK], regions[regions.len() - 1]);
        ZSetBatchView {
            regions,
            pk,
            blob,
            batch,
            pk_stride: schema.pk_stride(),
        }
    }

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
        self.batch.len()
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
            self.batch.len() * col_size,
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
