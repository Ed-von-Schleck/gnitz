//! Exchange worker routing: [`ScatterPlan`] and the per-row routing-key
//! kernels.

use crate::ops::group_key::GroupKey;
use crate::ops::reindex::{locate_key_col, FoldCols, ReindexPacker};
use crate::schema::ColumnTable;
use crate::schema::Slot;
use crate::schema::{worker_for_key, worker_for_pk_bytes};
use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

/// Keep only the live rows `slot` owns: those whose PK `worker_for_pk_bytes`
/// routes to it, the hash the equality scatter routes a join key by. A broadcast
/// delta filtered here integrates into a trace partitioned like a scattered one.
pub fn op_worker_filter(batch: &Batch, slot: Slot) -> Batch {
    ScatterPlan::whole_pk(batch.schema()).share(batch, slot)
}

/// A scatter key resolved against one schema: each row goes to the owner of the
/// PK its consumer gives it.
pub struct ScatterPlan(Key);

enum Key {
    /// The output PK a reduce over the columns stamps — also what an unpromoted
    /// PK-prefix or single-column join key packs to.
    Group(GroupKey),
    /// The `_join_pk` the reindex Map packs.
    Packed(ReindexPacker),
}

impl ScatterPlan {
    /// Rows grouped by `cols`, keyed by the output PK a reduce over them stamps.
    pub fn group(schema: &SchemaDescriptor, cols: &[u32]) -> Result<Self, String> {
        Ok(ScatterPlan(Key::Group(GroupKey::new(schema, cols)?)))
    }

    /// An equi-join key, keyed by the `_join_pk` the reindex Map packs from `slots`.
    pub fn join(schema: &SchemaDescriptor, slots: &[gnitz_wire::ReindexSlot]) -> Result<Self, String> {
        let pk = schema.pk_cols();
        let key = match slots {
            // Slots pack in slot order, so only the PK's leading columns in PK
            // order, unpromoted, pack the PK's own bytes.
            _ if slots.len() <= pk.len() && slots.iter().zip(pk).all(|(&(c, t), &p)| c == p && t.is_none()) => {
                GroupKey::PkPrefix(
                    slots
                        .iter()
                        .map(|&(c, _)| schema.columns[c as usize].size() as usize)
                        .sum(),
                )
            }
            &[(c, None)]
                if schema
                    .column(c as usize)
                    .is_some_and(|col| col.type_code.is_pk_eligible()) =>
            {
                GroupKey::Image(locate_key_col(schema, c, "reindex key")?)
            }
            _ => return Ok(ScatterPlan(Key::Packed(ReindexPacker::new(schema, slots)?))),
        };
        Ok(ScatterPlan(Key::Group(key)))
    }

    /// The whole PK — what [`op_worker_filter`] keeps a broadcast delta's share by.
    fn whole_pk(schema: &SchemaDescriptor) -> Self {
        ScatterPlan(Key::Group(GroupKey::PkPrefix(schema.pk_stride())))
    }

    /// True iff the exchange this plan describes would move nothing: it hashes
    /// exactly the bytes `worker_for_pk` placed `schema`'s rows by.
    pub fn routes_to_native_owner(&self, schema: &SchemaDescriptor) -> bool {
        schema.placement().is_key_routed()
            && matches!(self.0, Key::Group(GroupKey::PkPrefix(n)) if n == schema.dist_stride())
    }

    /// Route every live row of `mb` — a weight-0 row is not a Z-set element —
    /// into `slots`, one ascending row list per worker.
    pub(super) fn route(&self, mb: &MemBatch, slots: &mut [Vec<u32>]) {
        let nw = slots.len();
        self.route_into(mb, nw, &mut EveryWorker(slots));
    }

    /// The live rows of `batch` that `slot` owns: its share of a batch every
    /// worker holds whole, and what a round under this plan would hand it.
    pub fn share(&self, batch: &Batch, slot: Slot) -> Batch {
        let nw = slot.of as usize;
        let mut share = Share {
            rank: slot.rank as usize,
            rows: Vec::with_capacity(batch.count / nw + 1),
        };
        self.route_into(&batch.as_mem_batch(), nw, &mut share);
        batch.ascending_subset(&share.rows)
    }

    fn route_into(&self, mb: &MemBatch, nw: usize, sink: &mut impl RowSink) {
        match self.0 {
            Key::Group(GroupKey::PkPrefix(n)) => route_rows(mb, sink, PrefixW { n, nw }),
            Key::Group(GroupKey::Image(loc @ ColumnLocator::Payload { .. })) => {
                route_rows(mb, sink, ImageW::<true> { loc, nw })
            }
            Key::Group(GroupKey::Image(loc)) => route_rows(mb, sink, ImageW::<false> { loc, nw }),
            Key::Packed(ref packer) => route_rows_packed(mb, sink, packer, nw),
            Key::Group(GroupKey::Fold(ref fold)) => route_rows(mb, sink, FoldW { fold, nw }),
        }
    }
}

/// Where a routed row goes.
trait RowSink {
    fn put(&mut self, worker: usize, row: u32);
}

/// One row list per worker.
struct EveryWorker<'a>(&'a mut [Vec<u32>]);

impl RowSink for EveryWorker<'_> {
    #[inline(always)]
    fn put(&mut self, worker: usize, row: u32) {
        self.0[worker].push(row);
    }
}

/// `rank`'s rows alone.
struct Share {
    rank: usize,
    rows: Vec<u32>,
}

impl RowSink for Share {
    #[inline(always)]
    fn put(&mut self, worker: usize, row: u32) {
        if worker == self.rank {
            self.rows.push(row);
        }
    }
}

/// One kind's per-row route, forced inline into [`route_rows`] (a closure
/// cannot be).
trait RowWorker {
    fn worker(&self, mb: &MemBatch, row: usize) -> usize;
}

struct PrefixW {
    n: usize,
    nw: usize,
}

impl RowWorker for PrefixW {
    #[inline(always)]
    fn worker(&self, mb: &MemBatch, row: usize) -> usize {
        worker_for_pk_bytes(&mb.get_pk_bytes(row)[..self.n], self.nw)
    }
}

/// `PAYLOAD` fixes the locator's variant for the walk, so `opk_image`'s region
/// branch folds away.
struct ImageW<const PAYLOAD: bool> {
    loc: ColumnLocator,
    nw: usize,
}

impl<const PAYLOAD: bool> RowWorker for ImageW<PAYLOAD> {
    #[inline(always)]
    fn worker(&self, mb: &MemBatch, row: usize) -> usize {
        assert!(matches!(self.loc, ColumnLocator::Payload { .. }) == PAYLOAD);
        worker_for_key(self.loc.opk_image(mb, row), self.nw)
    }
}

struct FoldW<'a> {
    fold: &'a FoldCols,
    nw: usize,
}

impl RowWorker for FoldW<'_> {
    #[inline(always)]
    fn worker(&self, mb: &MemBatch, row: usize) -> usize {
        worker_for_key(self.fold.key_row(mb, row, mb.get_null_word(row)), self.nw)
    }
}

#[inline(never)]
fn route_rows<W: RowWorker>(mb: &MemBatch, sink: &mut impl RowSink, w: W) {
    for row in 0..mb.count {
        if mb.get_weight(row) != 0 {
            sink.put(w.worker(mb, row), row as u32);
        }
    }
}

/// [`route_rows`] for a packed join key, packed a chunk of rows at a time.
#[inline(never)]
fn route_rows_packed(mb: &MemBatch, sink: &mut impl RowSink, packer: &ReindexPacker, nw: usize) {
    packer.for_each_key(mb, packer.out_stride, |row, key| {
        if mb.get_weight(row) != 0 {
            sink.put(worker_for_pk_bytes(key, nw), row as u32);
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/router.rs"]
mod tests;
