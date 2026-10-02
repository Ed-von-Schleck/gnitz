//! Write-path fan-out: turns a pushed batch into a SAL group's per-worker
//! payload, routed by the native `ScatterPlan` of the relation's placement.

use std::cell::RefCell;

use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::repr::Batch;

use crate::runtime::sal::GroupData;
use gnitz_zset::schema::Placement;

// Reuse the row lists across calls.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

/// Route `batch`'s live rows to the workers `placement` names, and hand `f` the
/// group data that carries them.
pub(crate) fn with_routed<R>(
    batch: &Batch,
    placement: Placement,
    num_workers: usize,
    f: impl FnOnce(GroupData<'_>) -> R,
) -> R {
    SCATTER_INDICES.with(|pool| {
        let mut pool = pool.borrow_mut();
        let rows = ScatterPlan::native(placement).route(batch, &mut pool, num_workers);
        let subs: Vec<Batch>;
        let data = match rows {
            [all] if all.len() == batch.len() => GroupData::Same(batch.wire_whole()),
            [_, _, ..] if batch.heap_referencing_slots() == 0 => GroupData::Scattered { batch, rows },
            _ => {
                subs = rows.iter().map(|r| batch.ascending_subset(r)).collect();
                GroupData::batches(&subs)
            }
        };
        f(data)
    })
}

#[cfg(test)]
#[path = "tests/scatter.rs"]
mod tests;
