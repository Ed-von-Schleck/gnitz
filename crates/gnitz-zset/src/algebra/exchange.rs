//! The scatter plan of a worker exchange: [`ScatterPlan`] and the per-row
//! routing-key kernels.

use super::route::{ground_owner, worker_for_key, worker_for_pk_bytes, Placement, Slot};
use crate::algebra::group_key::{cell_image, for_cell_width, GroupKey, KeyCells};
use crate::algebra::reindex::{FoldCols, ReindexPacker};
use crate::repr::{Batch, MemBatch};
use crate::schema::SchemaDescriptor;
use gnitz_wire::zip_cells;

/// A scatter key resolved against one schema: each row goes to the owner of the
/// PK its consumer gives it. Without a key every worker is sent every
/// nonzero-weight row, as list 0.
pub struct ScatterPlan(Option<GroupKey>);

impl ScatterPlan {
    /// Rows grouped by `cols`, keyed by the output PK a reduce over them stamps.
    pub fn group(schema: &SchemaDescriptor, cols: &[u32]) -> Result<Self, String> {
        Ok(ScatterPlan(Some(GroupKey::new(schema, cols)?)))
    }

    /// An equi-join key, keyed by the `_join_pk` the reindex Map packs from `slots`.
    pub fn join(schema: &SchemaDescriptor, slots: &[gnitz_wire::ReindexSlot]) -> Result<Self, String> {
        Ok(ScatterPlan(Some(GroupKey::packed(ReindexPacker::new(schema, slots)?))))
    }

    /// The route a relation's own rows are placed by. Panics on `Placement::Local`,
    /// whose rows have no key owner.
    pub fn native(placement: Placement) -> Self {
        match placement {
            Placement::Replicated => Self::broadcast(),
            Placement::Keyed { dist_stride } => ScatterPlan(Some(GroupKey::PkRange { at: 0, n: dist_stride as usize })),
            Placement::Local => panic!("ScatterPlan::native: a Local relation's rows have no key owner"),
        }
    }

    /// Every nonzero-weight row to every worker.
    pub fn broadcast() -> Self {
        ScatterPlan(None)
    }

    /// Every worker is sent every row.
    pub fn is_broadcast(&self) -> bool {
        self.0.is_none()
    }

    /// True when this plan hashes exactly the bytes `placement` places rows by,
    /// so an exchange by it over rows so placed moves nothing.
    pub fn routes_to_native_owner(&self, placement: Placement) -> bool {
        matches!((&self.0, placement),
            (Some(GroupKey::PkRange { at: 0, n }), Placement::Keyed { dist_stride }) if *n == dist_stride as usize)
    }

    /// Reset `out` to one ascending list per worker of the nonzero-weight rows of
    /// `batch` that worker owns — a weight-0 row is not a Z-set element. A
    /// broadcast is the one list every worker is sent.
    pub fn route<'a>(&self, batch: &Batch, out: &'a mut Vec<Vec<u32>>, num_workers: usize) -> &'a [Vec<u32>] {
        let lists = match self.0 {
            None => 1,
            Some(_) => num_workers,
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

    /// The nonzero-weight rows of `batch` that `slot` owns: its share of a batch
    /// every worker holds whole, and what a round under this plan would hand it.
    pub fn share(&self, batch: &Batch, slot: Slot) -> Batch {
        let nw = slot.of as usize;
        let mut share = Share {
            rank: match self.0 {
                None => 0,
                Some(_) => slot.rank as usize,
            },
            rows: Vec::with_capacity(batch.count / nw + 1),
        };
        self.route_into(&batch.as_mem_batch(), nw, &mut share);
        batch.ascending_subset(&share.rows)
    }

    fn route_into(&self, mb: &MemBatch, nw: usize, sink: &mut impl RowSink) {
        match self.0 {
            // One worker owns every key, so no key is read or hashed.
            _ if nw == 1 => route_rows(mb, sink, Owner(0)),
            Some(ref key) => match key.cells(mb) {
                Some(cells) => for_cell_width!(cells.width, |W| match cells.opk {
                    true => route_cells::<W, true>(&cells, mb.weight(), sink, nw),
                    false => route_cells::<W, false>(&cells, mb.weight(), sink, nw),
                }),
                None => match *key {
                    GroupKey::PkRange { at, n } => route_rows(mb, sink, PkRangeW { at, n, nw }),
                    GroupKey::Packed(ref packer) => route_rows_packed(mb, sink, packer, nw),
                    // The one group of no columns has one owner, and no key to hash.
                    GroupKey::Fold(ref fold) if fold.is_empty() => route_rows(mb, sink, Owner(ground_owner(nw))),
                    GroupKey::Fold(ref fold) => route_rows(mb, sink, FoldW { fold, nw }),
                    GroupKey::Image(_) => unreachable!("an image key is one cell"),
                },
            },
            None => route_rows(mb, sink, Owner(0)),
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

/// A PK range [`GroupKey::cells`] does not answer for.
struct PkRangeW {
    at: usize,
    n: usize,
    nw: usize,
}

impl RowWorker for PkRangeW {
    #[inline(always)]
    fn worker(&self, mb: &MemBatch, row: usize) -> usize {
        worker_for_pk_bytes(mb.get_pk_range(row, self.at, self.n), self.nw)
    }
}

/// Every row to one worker.
struct Owner(usize);

impl RowWorker for Owner {
    #[inline(always)]
    fn worker(&self, _: &MemBatch, _: usize) -> usize {
        self.0
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

/// [`route_rows`] for a key that is one `W`-byte cell per row: each nonzero-weight row to the
/// owner of its cell's image.
#[inline(never)]
fn route_cells<const W: usize, const OPK: bool>(cells: &KeyCells, weights: &[u8], sink: &mut impl RowSink, nw: usize) {
    let rows = weights.as_chunks::<8>().0.iter().enumerate();
    zip_cells::<W, _>(cells.region, cells.stride, cells.off, rows, |cell, (row, weight)| {
        if *weight != [0; 8] {
            sink.put(worker_for_key(cell_image::<W, OPK>(cell, cells.signed), nw), row as u32);
        }
    });
}

/// [`route_rows`] for a packed key, packed a chunk of rows at a time.
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
#[path = "tests/exchange.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/exchange.rs"]
mod bench;
