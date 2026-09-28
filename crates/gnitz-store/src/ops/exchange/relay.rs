//! Exchange relay: [`op_relay_scatter`].

use std::cell::RefCell;

use crate::schema::{OpBuildErr, SchemaDescriptor};
use crate::storage::{Batch, Layout, MemBatch, UnifiedSet};

use super::router::{ScatterKey, ScatterSpec};

thread_local! {
    static WORKER_ROWS: RefCell<Vec<Vec<(u32, u32, i64)>>> = const { RefCell::new(Vec::new()) };
}

fn worker_rows_to_batches(
    schema: &SchemaDescriptor,
    mem: &[MemBatch],
    worker_rows: &[Vec<(u32, u32, i64)>],
) -> Vec<Batch> {
    let set = UnifiedSet::whole(mem, schema);
    let total_rows: usize = worker_rows.iter().map(|v| v.len()).sum();
    worker_rows
        .iter()
        .map(|rows| set.materialize(rows, total_rows))
        .collect()
}

/// Scatter `sources` into one batch per worker, routed by `spec`: Z-set `+`
/// across the sources, restricted to each worker's routing slice. A slice
/// claims `Consolidated` iff every source did.
///
/// Refused when `spec` does not route against `schema` — see `ScatterKey::new`,
/// which owns the reason.
pub fn op_relay_scatter(
    sources: &[&Batch],
    spec: ScatterSpec<'_>,
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Result<Vec<Batch>, OpBuildErr> {
    gnitz_debug!("op_relay_scatter: sources={} spec={:?}", sources.len(), spec);
    let consolidated = sources.iter().all(|b| b.is_consolidated());
    #[cfg(debug_assertions)]
    if consolidated {
        for b in sources {
            b.debug_verify_consolidated(schema);
        }
    }
    let key = ScatterKey::new(spec, schema)?;
    let mem: Vec<MemBatch> = sources.iter().map(|b| b.as_mem_batch()).collect();

    Ok(WORKER_ROWS.with(|pool| {
        let mut pool = pool.borrow_mut();
        let slots = crate::storage::reset_slots(&mut pool, num_workers);
        key.route(&mem, schema, consolidated && mem.len() >= 2, slots);

        let mut out = worker_rows_to_batches(schema, &mem, slots);
        if consolidated {
            // A slice is a subsequence of the sources, so consolidated too.
            for b in out.iter_mut().filter(|b| b.count > 0) {
                b.certify_layout(Layout::Consolidated);
            }
        }
        out
    }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/relay.rs"]
mod tests;
