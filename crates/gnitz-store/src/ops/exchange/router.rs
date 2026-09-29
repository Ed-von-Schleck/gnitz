//! Exchange worker routing: `ScatterSpec`, `ScatterKey`, and the per-row
//! routing-key helpers.

use crate::schema::key::{locate_key_col, FoldCols, ReindexPacker};
use crate::schema::ColumnTable;
use crate::schema::Slot;
use crate::schema::{worker_for_key, worker_for_pk_bytes};
use crate::schema::{ColumnLocator, OpBuildErr, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

use super::super::group_key::GroupKey;

/// Keep only the live rows `slot` owns: those whose PK `worker_for_pk_bytes`
/// routes to it, the hash the equality scatter routes a join key by. A broadcast
/// delta filtered here integrates into a trace partitioned like a scattered one.
pub fn op_worker_filter(batch: &Batch, slot: Slot) -> Batch {
    ScatterKey::PkPrefix(batch.schema().pk_stride()).share(batch, slot)
}

/// What a scatter routes by. Either way a row goes to the owner of the PK its
/// consumer gives it.
#[derive(Clone, Copy, Debug)]
pub enum ScatterSpec<'a> {
    /// Rows grouped by these columns: the owner of the output PK a reduce over
    /// them keys the row's group by.
    GroupKey(&'a [u32]),
    /// An equi-join key: the owner of the `_join_pk` the reindex Map packs from
    /// these `(source column, promotion target)` slots.
    JoinKey(&'a [gnitz_wire::ReindexSlot]),
}

impl ScatterSpec<'_> {
    /// Refused exactly where the scatter would refuse it.
    pub fn check(self, schema: &SchemaDescriptor) -> Result<(), OpBuildErr> {
        ScatterKey::new(self, schema).map(drop)
    }

    /// True iff the exchange this spec describes would move nothing: it hashes
    /// exactly the bytes `worker_for_pk` placed the rows by.
    pub fn routes_to_native_owner(self, schema: &SchemaDescriptor) -> bool {
        schema.placement().is_key_routed()
            && matches!(ScatterKey::new(self, schema), Ok(ScatterKey::PkPrefix(n)) if n == schema.dist_stride())
    }
}

/// A [`ScatterSpec`] resolved against one schema: what each row is hashed by.
pub(super) enum ScatterKey {
    /// The row's leading `n` PK bytes.
    PkPrefix(usize),
    /// One column's OPK image — what a single-column key packs to.
    Image(ColumnLocator),
    /// The `_join_pk` the reindex Map packs.
    Packed(ReindexPacker),
    /// The NULL-distinct XXH3 fold of the group columns.
    Fold(FoldCols),
}

impl From<GroupKey> for ScatterKey {
    fn from(key: GroupKey) -> Self {
        match key {
            GroupKey::PkPrefix(n) => ScatterKey::PkPrefix(n),
            GroupKey::Image(loc) => ScatterKey::Image(loc),
            GroupKey::Fold(fold) => ScatterKey::Fold(fold),
        }
    }
}

impl ScatterKey {
    /// Refused when `schema` cannot route by `spec`: a column it has not got, or
    /// one the group key or a reindex key refuses.
    pub(super) fn new(spec: ScatterSpec<'_>, schema: &SchemaDescriptor) -> Result<Self, OpBuildErr> {
        let pk = schema.pk_cols();
        Ok(match spec {
            ScatterSpec::GroupKey(cols) => GroupKey::new(schema, cols)?.into(),
            // Slots pack in slot order, so only the PK's leading columns in PK
            // order, unpromoted, pack the PK's own bytes.
            ScatterSpec::JoinKey(slots)
                if slots.len() <= pk.len() && slots.iter().zip(pk).all(|(&(c, t), &p)| c == p && t.is_none()) =>
            {
                ScatterKey::PkPrefix(
                    slots
                        .iter()
                        .map(|&(c, _)| schema.columns[c as usize].size() as usize)
                        .sum(),
                )
            }
            ScatterSpec::JoinKey(&[(c, None)])
                if schema
                    .column(c as usize)
                    .is_some_and(|col| col.type_code.is_pk_eligible()) =>
            {
                ScatterKey::Image(locate_key_col(schema, c, "reindex key")?)
            }
            ScatterSpec::JoinKey(slots) => ScatterKey::Packed(ReindexPacker::new(schema, slots)?),
        })
    }

    /// Route every live row of `mb` — a weight-0 row is not a Z-set element —
    /// into `slots`, one ascending row list per worker.
    pub(super) fn route(&self, mb: &MemBatch, slots: &mut [Vec<u32>]) {
        let nw = slots.len();
        self.route_into(mb, nw, &mut EveryWorker(slots));
    }

    /// The live rows of `batch` that `slot` owns: its share of a batch every
    /// worker holds whole.
    pub(super) fn share(&self, batch: &Batch, slot: Slot) -> Batch {
        let nw = slot.of as usize;
        let mut share = Share {
            rank: slot.rank as usize,
            rows: Vec::with_capacity(batch.count / nw + 1),
        };
        self.route_into(&batch.as_mem_batch(), nw, &mut share);
        batch.ascending_subset(&share.rows)
    }

    fn route_into(&self, mb: &MemBatch, nw: usize, sink: &mut impl RowSink) {
        match *self {
            ScatterKey::PkPrefix(n) => route_rows(mb, sink, PrefixW { n, nw }),
            ScatterKey::Image(loc @ ColumnLocator::Payload { .. }) => route_rows(mb, sink, ImageW::<true> { loc, nw }),
            ScatterKey::Image(loc) => route_rows(mb, sink, ImageW::<false> { loc, nw }),
            ScatterKey::Packed(ref packer) => route_rows_packed(mb, sink, packer, nw),
            ScatterKey::Fold(ref fold) => route_rows(mb, sink, FoldW { fold, nw }),
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
