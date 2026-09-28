//! Exchange worker routing: `ScatterSpec`, `ScatterKey`, and the per-row
//! routing-key helpers.

use crate::schema::key::{locate_key_col, FoldCols, ReindexPacker};
use crate::schema::ColumnTable;
use crate::schema::Slot;
use crate::schema::{worker_for_key, worker_for_pk_bytes};
use crate::schema::{ColumnLocator, OpBuildErr, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

use super::super::group_key::GroupKey;

/// Keep only the rows `slot` owns: those whose PK `worker_for_pk_bytes` routes
/// to it, the hash the equality scatter routes a join key by. A broadcast delta
/// filtered here integrates into a trace partitioned like a scattered one.
pub fn op_worker_filter(batch: &Batch, slot: Slot) -> Batch {
    let n = batch.count;
    if n == 0 {
        return Batch::clone(batch);
    }
    let nw = slot.of as usize;
    let wid = slot.rank as usize;
    let mb = batch.as_mem_batch();
    let mut indices: Vec<u32> = Vec::with_capacity(n / nw + 1);
    for i in 0..n {
        if worker_for_pk_bytes(mb.get_pk_bytes(i), nw) == wid {
            indices.push(i as u32);
        }
    }
    batch.ascending_subset(&indices)
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
        match *self {
            ScatterKey::PkPrefix(n) => route_rows(mb, slots, PrefixW { n, nw }),
            ScatterKey::Image(loc @ ColumnLocator::Payload { .. }) => route_rows(mb, slots, ImageW::<true> { loc, nw }),
            ScatterKey::Image(loc) => route_rows(mb, slots, ImageW::<false> { loc, nw }),
            ScatterKey::Packed(ref packer) => route_rows_packed(mb, slots, packer),
            ScatterKey::Fold(ref fold) => route_rows(mb, slots, FoldW { fold, nw }),
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
fn route_rows<W: RowWorker>(mb: &MemBatch, slots: &mut [Vec<u32>], w: W) {
    for row in 0..mb.count {
        if mb.get_weight(row) != 0 {
            slots[w.worker(mb, row)].push(row as u32);
        }
    }
}

/// [`route_rows`] for a packed join key, packed a chunk of rows at a time.
#[inline(never)]
fn route_rows_packed(mb: &MemBatch, slots: &mut [Vec<u32>], packer: &ReindexPacker) {
    let nw = slots.len();
    packer.for_each_key(mb, packer.out_stride, |row, key| {
        if mb.get_weight(row) != 0 {
            slots[worker_for_pk_bytes(key, nw)].push(row as u32);
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/router.rs"]
mod tests;
