//! The scatter plan of a worker exchange: [`ScatterPlan`] and the per-row
//! routing-key kernels.

use super::group_key::{GroupKey, KeyCells, KeyForm, cell_image, for_key_cells};
use super::key_pack::{IDENTITY_CHUNK, KeyPacker, filled};
use super::route::{Placement, Slot, ground_owner, worker_for_key, worker_for_wide_pk};
use crate::repr::{Batch, MemBatch};
use crate::schema::SchemaDescriptor;
use gnitz_wire::{MAX_PK_BYTES, zip_cells};
use std::mem::MaybeUninit;

/// A scatter key resolved against one schema: each row goes to the owner of the
/// PK its consumer gives it. Without a key every worker is sent every
/// nonzero-weight row, as list 0.
pub struct ScatterPlan(Option<Route>);

/// The PK a row is routed by.
enum Route {
    Key(GroupKey),
    /// A join key packed wider than 16 bytes, routed by its packed bytes' hash.
    WidePacked(KeyPacker),
}

impl ScatterPlan {
    /// Rows grouped by `cols`, keyed by the output PK a reduce over them stamps.
    pub fn group(schema: &SchemaDescriptor, cols: &[u32]) -> Result<Self, String> {
        Ok(ScatterPlan(Some(Route::Key(GroupKey::new(schema, cols)?))))
    }

    /// An equi-join key, keyed by the `_join_pk` a reindex `Map` packs from `slots`.
    pub fn join(schema: &SchemaDescriptor, slots: &[gnitz_wire::ReindexSlot]) -> Result<Self, String> {
        let route = match GroupKey::packed(KeyPacker::new(schema, slots)?) {
            Ok(key) => Route::Key(key),
            Err(wide) => Route::WidePacked(wide),
        };
        Ok(ScatterPlan(Some(route)))
    }

    /// The route a relation's own rows are placed by. Panics on `Placement::Local`,
    /// whose rows have no key owner.
    pub fn native(placement: Placement) -> Self {
        match placement {
            Placement::Replicated => Self::broadcast(),
            Placement::Keyed { dist_stride } => {
                ScatterPlan(Some(Route::Key(GroupKey::PkRange { at: 0, n: dist_stride as usize })))
            }
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
            (Some(Route::Key(GroupKey::PkRange { at: 0, n })), Placement::Keyed { dist_stride }) if *n == dist_stride as usize)
    }

    /// Reset `out` to one ascending list per worker of the nonzero-weight rows of
    /// `batch` that worker owns — a weight-0 row is not a Z-set element. A
    /// broadcast is the one list every worker is sent.
    pub fn route<'a>(&self, batch: &MemBatch<'_>, out: &'a mut Vec<Vec<u32>>, num_workers: usize) -> &'a [Vec<u32>] {
        let lists = if self.is_broadcast() { 1 } else { num_workers };
        // Reset to `lists` empty lists, keeping their allocations.
        if out.len() < lists {
            out.resize_with(lists, Vec::new);
        }
        let slots = &mut out[..lists];
        slots.iter_mut().for_each(Vec::clear);
        self.route_into(batch, num_workers, |worker, row| slots[worker].push(row));
        slots
    }

    /// The nonzero-weight rows of `batch` that `slot` owns: its share of a batch
    /// every worker holds whole, and what a round under this plan would hand it.
    pub fn share(&self, batch: &Batch, slot: Slot) -> Batch {
        let nw = slot.of as usize;
        let rank = if self.is_broadcast() { 0 } else { slot.rank as usize };
        let mut rows = Vec::with_capacity(batch.len() / nw + 1);
        self.route_into(&batch.as_mem_batch(), nw, |worker, row| {
            if worker == rank {
                rows.push(row);
            }
        });
        batch.ascending_subset(&rows)
    }

    /// Hand `put` each nonzero-weight row of `mb` with the list it goes to: its
    /// owner's, or a broadcast's one list 0.
    fn route_into(&self, mb: &MemBatch, nw: usize, put: impl FnMut(usize, u32)) {
        match self.0 {
            Some(Route::WidePacked(ref p)) if nw > 1 => route_rows_packed(mb, put, p, nw),
            Some(Route::Key(ref key)) if nw > 1 => match key.form(mb) {
                KeyForm::Cells(cells) => {
                    for_key_cells!(cells, |W, OPK| route_cells::<W, OPK>(&cells, mb.weight(), put, nw))
                }
                KeyForm::WidePk { at, n } => {
                    // Captured by value: read through the borrowed view, each
                    // row's PK range costs the loop more instructions.
                    let rows = *mb;
                    route_rows(mb, put, move |row| {
                        worker_for_wide_pk(rows.get_pk_range(row, at, n), nw)
                    })
                }
                KeyForm::Chunked => route_identities(mb, put, key, nw),
                // The one group of no columns has one owner, and no key to hash.
                KeyForm::Global => {
                    let owner = ground_owner(nw);
                    route_rows(mb, put, |_| owner)
                }
            },
            // One worker owns every key, and a broadcast is the one list, so no
            // key is read or hashed.
            _ => route_rows(mb, put, |_| 0),
        }
    }
}

/// Each nonzero-weight row to the worker `worker` names for it.
#[inline(never)]
fn route_rows(mb: &MemBatch, mut put: impl FnMut(usize, u32), worker: impl Fn(usize) -> usize) {
    for (row, weight) in mb.weight().as_chunks::<8>().0.iter().enumerate() {
        if *weight != [0; 8] {
            put(worker(row), row as u32);
        }
    }
}

/// [`route_rows`] for a key that is one `W`-byte cell per row: each nonzero-weight row to the
/// owner of its cell's image.
#[inline(never)]
fn route_cells<const W: usize, const OPK: bool>(
    cells: &KeyCells,
    weights: &[u8],
    mut put: impl FnMut(usize, u32),
    nw: usize,
) {
    let rows = weights.as_chunks::<8>().0.iter().zip(0u32..);
    zip_cells::<W, _>(cells.region, cells.stride, cells.off, rows, |cell, (weight, row)| {
        if *weight != [0; 8] {
            put(worker_for_key(cell_image::<W, OPK>(cell, cells.signed), nw), row);
        }
    });
}

/// [`route_rows`] for a packed key of at most 16 bytes or a fold: each nonzero-weight row
/// to the owner of its [`GroupKey::identities`], taken a chunk of rows at a time.
#[inline(never)]
fn route_identities(mb: &MemBatch, mut put: impl FnMut(usize, u32), key: &GroupKey, nw: usize) {
    let (rows, weights) = (mb.len(), mb.weight().as_chunks::<8>().0);
    let mut buf = [MaybeUninit::<u128>::uninit(); IDENTITY_CHUNK];
    let ids = filled(&mut buf[..rows.min(IDENTITY_CHUNK)], 0);
    for start in (0..rows).step_by(IDENTITY_CHUNK) {
        let ids = &mut ids[..(rows - start).min(IDENTITY_CHUNK)];
        key.identities(mb, start, ids);
        for ((id, weight), row) in ids.iter().zip(&weights[start..]).zip(start as u32..) {
            if *weight != [0; 8] {
                put(worker_for_key(*id, nw), row);
            }
        }
    }
}

/// [`route_rows`] for a packed key wider than 16 bytes, packed a chunk of rows at a time
/// into a stack buffer. The buffer is zeroed only as far as the first chunk's keys reach,
/// since no later chunk is longer; [`KeyPacker::pack_rows`] writes every byte of each key
/// it packs, so a chunk reads no key left by the one before.
#[inline(never)]
fn route_rows_packed(mb: &MemBatch, mut put: impl FnMut(usize, u32), packer: &KeyPacker, nw: usize) {
    let (rows, width, weights) = (mb.len(), packer.out_stride(), mb.weight().as_chunks::<8>().0);
    let mut buf = [MaybeUninit::<u8>::uninit(); IDENTITY_CHUNK * MAX_PK_BYTES];
    let keys = filled(&mut buf[..rows.min(IDENTITY_CHUNK) * width], 0);
    for start in (0..rows).step_by(IDENTITY_CHUNK) {
        let n = (rows - start).min(IDENTITY_CHUNK);
        packer.pack_rows(keys, width, mb, &[(start, start + n)]);
        let rows = keys
            .chunks_exact(width)
            .zip(&weights[start..start + n])
            .zip(start as u32..);
        for ((key, weight), row) in rows {
            if *weight != [0; 8] {
                put(worker_for_wide_pk(key, nw), row);
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/exchange.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/exchange.rs"]
mod bench;
