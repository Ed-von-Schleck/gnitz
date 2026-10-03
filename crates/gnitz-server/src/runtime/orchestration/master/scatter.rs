//! Write-path fan-out: turns a pushed batch into a SAL group's per-worker
//! payload, routed by the native `ScatterPlan` of the relation's placement.

use std::cell::RefCell;

use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::repr::{Batch, WireRows};

use crate::runtime::sal::{GroupData, MAX_WORKERS};
use gnitz_zset::schema::Placement;

// Reuse the row lists across calls.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

/// Route `batch`'s nonzero-weight rows to the workers `placement` names, and hand `f` the
/// group data that carries them, framed straight from `batch`.
pub(crate) fn with_routed<R>(
    batch: &Batch,
    placement: Placement,
    num_workers: usize,
    f: impl FnOnce(GroupData<'_>) -> R,
) -> R {
    let plan = ScatterPlan::native(placement);
    // One payload for every worker is the batch itself unless it holds a
    // ghost, and then no row needs routing.
    let shared = num_workers == 1 || matches!(placement, Placement::Replicated);
    if shared && !batch.has_ghost() {
        return f(GroupData::Same(batch.wire_whole()));
    }
    // One row has one owner, named by the placement's point query: no list is built.
    if batch.len() == 1 && !batch.has_ghost() {
        if let Some(owner) = placement.owner(batch.as_mem_batch().get_pk_bytes(0), num_workers) {
            let mut each = [None; MAX_WORKERS];
            each[owner] = batch.wire_whole();
            return f(GroupData::Each(&each[..num_workers]));
        }
    }
    SCATTER_INDICES.with(|pool| {
        let mut pool = pool.borrow_mut();
        match plan.route(batch, &mut pool, num_workers) {
            [live] => f(GroupData::Same(listed(batch, live))),
            lists => {
                let mut each = [None; MAX_WORKERS];
                for (rows, list) in each.iter_mut().zip(lists) {
                    *rows = listed(batch, list);
                }
                f(GroupData::Each(&each[..lists.len()]))
            }
        }
    })
}

/// The rows of `batch` one of `route`'s lists names. A list of every row,
/// ascending as `route` builds it, is the batch itself.
fn listed<'a>(batch: &'a Batch, list: &'a [u32]) -> Option<WireRows<'a>> {
    match list.len() == batch.len() {
        true => batch.wire_whole(),
        false => batch.wire_listed(list),
    }
}

#[cfg(test)]
#[path = "tests/scatter.rs"]
mod tests;
