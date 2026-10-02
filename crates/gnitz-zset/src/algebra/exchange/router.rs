//! Exchange worker routing: [`ScatterPlan`] and the per-row routing-key
//! kernels.

use crate::algebra::group_key::GroupKey;
use crate::algebra::reindex::{FoldCols, ReindexPacker};
use crate::repr::{Batch, MemBatch};
use crate::schema::Slot;
use crate::schema::{worker_for_key, worker_for_pk_bytes};
use crate::schema::{ColumnLocator, Placement, SchemaDescriptor};

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
    /// The output PK a reduce over the columns stamps — also what a join key
    /// packing a PK prefix or one column's own image packs to.
    Group(GroupKey),
    /// The `_join_pk` the reindex Map packs.
    Packed(ReindexPacker),
    /// No key: every worker is sent every live row, as list 0.
    Broadcast,
}

impl ScatterPlan {
    /// Rows grouped by `cols`, keyed by the output PK a reduce over them stamps.
    pub fn group(schema: &SchemaDescriptor, cols: &[u32]) -> Result<Self, String> {
        Ok(ScatterPlan(Key::Group(GroupKey::new(schema, cols)?)))
    }

    /// An equi-join key, keyed by the `_join_pk` the reindex Map packs from `slots`.
    pub fn join(schema: &SchemaDescriptor, slots: &[gnitz_wire::ReindexSlot]) -> Result<Self, String> {
        let packer = ReindexPacker::new(schema, slots)?;
        let key = match packer.pk_prefix_len() {
            Some(n) => Some(GroupKey::PkPrefix(n)),
            None => match packer.identity_columns().as_deref() {
                Some(&[loc]) => Some(GroupKey::Image(loc)),
                _ => None,
            },
        };
        Ok(ScatterPlan(key.map_or(Key::Packed(packer), Key::Group)))
    }

    /// The route a relation's own rows are placed by. Panics on `Placement::Local`,
    /// whose rows have no key owner.
    pub fn native(placement: Placement) -> Self {
        match placement {
            Placement::Replicated => Self::broadcast(),
            Placement::Keyed { dist_stride } => ScatterPlan(Key::Group(GroupKey::PkPrefix(dist_stride as usize))),
            Placement::Local => panic!("ScatterPlan::native: a Local relation's rows have no key owner"),
        }
    }

    /// Every live row to every worker.
    pub fn broadcast() -> Self {
        ScatterPlan(Key::Broadcast)
    }

    /// The whole PK — what [`op_worker_filter`] keeps a broadcast delta's share by.
    fn whole_pk(schema: &SchemaDescriptor) -> Self {
        ScatterPlan(Key::Group(GroupKey::PkPrefix(schema.pk_stride())))
    }

    /// True when this plan hashes exactly the bytes `placement` places rows by,
    /// so an exchange by it over rows so placed moves nothing.
    pub fn routes_to_native_owner(&self, placement: Placement) -> bool {
        matches!((&self.0, placement),
            (Key::Group(GroupKey::PkPrefix(n)), Placement::Keyed { dist_stride }) if *n == dist_stride as usize)
    }

    /// Reset `out` to one ascending list per worker of the live rows of `batch`
    /// that worker owns — a weight-0 row is not a Z-set element. A broadcast is
    /// the one list every worker is sent.
    pub fn route<'a>(&self, batch: &Batch, out: &'a mut Vec<Vec<u32>>, num_workers: usize) -> &'a [Vec<u32>] {
        let lists = match self.0 {
            Key::Broadcast => 1,
            _ => num_workers,
        };
        // Reset to `lists` empty lists, keeping their allocations.
        if out.len() < lists {
            out.resize_with(lists, Vec::new);
        }
        let slots = &mut out[..lists];
        slots.iter_mut().for_each(Vec::clear);
        self.route_into(&batch.as_mem_batch(), num_workers, &mut EveryWorker(slots));
        slots
    }

    /// The live rows of `batch` that `slot` owns: its share of a batch every
    /// worker holds whole, and what a round under this plan would hand it.
    pub fn share(&self, batch: &Batch, slot: Slot) -> Batch {
        let nw = slot.of as usize;
        let mut share = Share {
            rank: match self.0 {
                Key::Broadcast => 0,
                _ => slot.rank as usize,
            },
            rows: Vec::with_capacity(batch.count / nw + 1),
        };
        self.route_into(&batch.as_mem_batch(), nw, &mut share);
        batch.ascending_subset(&share.rows)
    }

    fn route_into(&self, mb: &MemBatch, nw: usize, sink: &mut impl RowSink) {
        match self.0 {
            // One worker owns every key, so no key is read or hashed.
            _ if nw == 1 => route_rows(mb, sink, ListZero),
            Key::Group(GroupKey::PkPrefix(n)) => route_rows(mb, sink, PrefixW { n, nw }),
            Key::Group(GroupKey::Image(loc @ ColumnLocator::Payload { .. })) => {
                route_rows(mb, sink, ImageW::<true> { loc, nw })
            }
            Key::Group(GroupKey::Image(loc)) => route_rows(mb, sink, ImageW::<false> { loc, nw }),
            Key::Packed(ref packer) => route_rows_packed(mb, sink, packer, nw),
            Key::Group(GroupKey::Fold(ref fold)) => route_rows(mb, sink, FoldW { fold, nw }),
            Key::Broadcast => route_rows(mb, sink, ListZero),
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

struct ListZero;

impl RowWorker for ListZero {
    #[inline(always)]
    fn worker(&self, _: &MemBatch, _: usize) -> usize {
        0
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
    for (row, weight) in mb.weight().as_chunks::<8>().0.iter().enumerate() {
        if *weight != [0; 8] {
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
