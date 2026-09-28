//! Exchange worker routing: `ScatterSpec`, `ScatterKey`, and the per-row
//! routing-key helpers.

use crate::schema::key::{locate_key_col, FoldCols, ReindexPacker};
use crate::schema::Slot;
use crate::schema::{worker_for_key, worker_for_pk_bytes, MAX_PK_BYTES};
use crate::schema::{ColumnLocator, OpBuildErr, SchemaDescriptor};
use crate::storage::{run_merge, Batch, MemBatch};

use super::super::group_key::GroupKeyCols;

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

impl ScatterKey {
    /// Refused when `schema` cannot route by `spec`: a column it has not got, or
    /// one the group key or a reindex key refuses.
    pub(super) fn new(spec: ScatterSpec<'_>, schema: &SchemaDescriptor) -> Result<Self, OpBuildErr> {
        let pk = schema.pk_indices();
        Ok(match spec {
            ScatterSpec::GroupKey(cols) if cols == pk => ScatterKey::PkPrefix(schema.pk_stride()),
            ScatterSpec::GroupKey(cols) => match GroupKeyCols::new(schema, cols)? {
                GroupKeyCols {
                    canonical: Some(ColumnLocator::Pk { byte_off: 0, size, .. }),
                    ..
                } => ScatterKey::PkPrefix(size as usize),
                GroupKeyCols { canonical: Some(loc), .. } => ScatterKey::Image(loc),
                GroupKeyCols { cols: fold, .. } => ScatterKey::Fold(fold),
            },
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

    /// Route `mem` into `slots`, one per worker: with `merge`, each
    /// (PK, payload) group once at its net weight; else every row of nonzero
    /// weight — a weight-0 row is not a Z-set element.
    pub(super) fn route(
        &self,
        mem: &[MemBatch],
        schema: &SchemaDescriptor,
        merge: bool,
        slots: &mut [Vec<(u32, u32, i64)>],
    ) {
        let nw = slots.len();
        match *self {
            ScatterKey::PkPrefix(n) => walk(mem, schema, merge, slots, PrefixW { n, nw }),
            ScatterKey::Image(loc @ ColumnLocator::Payload { .. }) => {
                walk(mem, schema, merge, slots, ImageW::<true> { loc, nw })
            }
            ScatterKey::Image(loc) => walk(mem, schema, merge, slots, ImageW::<false> { loc, nw }),
            ScatterKey::Packed(ref packer) if merge => {
                walk(mem, schema, merge, slots, PackedW { packer, n: packer.out_stride, nw })
            }
            ScatterKey::Packed(ref packer) => {
                for (si, mb) in mem.iter().enumerate() {
                    route_source_packed(mb, si as u32, slots, packer);
                }
            }
            ScatterKey::Fold(ref fold) => walk(mem, schema, merge, slots, FoldW { fold, nw }),
        }
    }
}

type Scratch = [u8; MAX_PK_BYTES];

/// One kind's per-row route, forced inline into its walk (a closure cannot be).
/// The walk owns the scratch: a worker holding it would reload every field per
/// row, its address escaping into `pack_into`.
trait RowWorker {
    fn worker(&self, scratch: &mut Scratch, mb: &MemBatch, row: usize) -> usize;
}

struct PrefixW {
    n: usize,
    nw: usize,
}

impl RowWorker for PrefixW {
    #[inline(always)]
    fn worker(&self, _: &mut Scratch, mb: &MemBatch, row: usize) -> usize {
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
    fn worker(&self, _: &mut Scratch, mb: &MemBatch, row: usize) -> usize {
        assert!(matches!(self.loc, ColumnLocator::Payload { .. }) == PAYLOAD);
        worker_for_key(self.loc.opk_image(mb, row), self.nw)
    }
}

struct PackedW<'a> {
    packer: &'a ReindexPacker,
    n: usize,
    nw: usize,
}

impl RowWorker for PackedW<'_> {
    #[inline(always)]
    fn worker(&self, scratch: &mut Scratch, mb: &MemBatch, row: usize) -> usize {
        let key = &mut scratch[..self.n];
        self.packer.pack_into(key, mb, row);
        worker_for_pk_bytes(key, self.nw)
    }
}

struct FoldW<'a> {
    fold: &'a FoldCols,
    nw: usize,
}

impl RowWorker for FoldW<'_> {
    #[inline(always)]
    fn worker(&self, _: &mut Scratch, mb: &MemBatch, row: usize) -> usize {
        worker_for_key(self.fold.key_row(mb, row, mb.get_null_word(row)), self.nw)
    }
}

#[inline(always)]
fn walk(
    mem: &[MemBatch],
    schema: &SchemaDescriptor,
    merge: bool,
    slots: &mut [Vec<(u32, u32, i64)>],
    w: impl RowWorker,
) {
    if merge {
        route_merge(mem, schema, slots, &w)
    } else {
        for (si, mb) in mem.iter().enumerate() {
            route_source(mb, si as u32, slots, &w);
        }
    }
}

/// One exemplar per (PK, payload) group, byte-equal to its members wherever a
/// key can read, so it routes for all of them.
#[inline(never)]
fn route_merge<W: RowWorker>(mem: &[MemBatch], schema: &SchemaDescriptor, slots: &mut [Vec<(u32, u32, i64)>], w: &W) {
    let mut scratch = [0u8; MAX_PK_BYTES];
    run_merge(mem, schema, |si, row, wt| {
        slots[merge_worker(w, &mut scratch, &mem[si], row)].push((si as u32, row as u32, wt))
    });
}

/// Out of line: inlined into `run_merge`'s emit callback the route is slower on
/// every kind.
#[inline(never)]
fn merge_worker<W: RowWorker>(w: &W, scratch: &mut Scratch, mb: &MemBatch, row: usize) -> usize {
    w.worker(scratch, mb, row)
}

#[inline(never)]
fn route_source<W: RowWorker>(mb: &MemBatch, si: u32, slots: &mut [Vec<(u32, u32, i64)>], w: &W) {
    let mut scratch = [0u8; MAX_PK_BYTES];
    for row in 0..mb.count {
        let wt = mb.get_weight(row);
        if wt != 0 {
            slots[w.worker(&mut scratch, mb, row)].push((si, row as u32, wt));
        }
    }
}

/// [`route_source`] for a packed join key, packed a chunk of rows at a time.
#[inline(never)]
fn route_source_packed(mb: &MemBatch, si: u32, slots: &mut [Vec<(u32, u32, i64)>], packer: &ReindexPacker) {
    let nw = slots.len();
    packer.for_each_key(mb, packer.out_stride, |row, key| {
        let wt = mb.get_weight(row);
        if wt != 0 {
            slots[worker_for_pk_bytes(key, nw)].push((si, row as u32, wt));
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/router.rs"]
mod tests;
