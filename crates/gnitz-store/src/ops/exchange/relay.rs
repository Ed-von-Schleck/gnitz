//! Exchange relay: [`op_relay_scatter`].

use std::cell::RefCell;

use crate::schema::{OpBuildErr, SchemaDescriptor};
use crate::storage::{prorated_blob_cap, run_merge, Batch, Layout, MemBatch, UnifiedSet};

use super::router::{ScatterKey, ScatterSpec};

thread_local! {
    static WORKER_ROWS: RefCell<Vec<Vec<(u32, u32, i64)>>> = const { RefCell::new(Vec::new()) };
}

fn worker_rows_to_batches(
    schema: &SchemaDescriptor,
    mem: &[MemBatch],
    worker_rows: &[Vec<(u32, u32, i64)>],
) -> Vec<Batch> {
    let set = UnifiedSet::of(mem, schema);
    let total_blob: usize = mem.iter().map(|mb| mb.blob.len()).sum();
    let total_rows: usize = worker_rows.iter().map(|v| v.len()).sum();
    worker_rows
        .iter()
        .map(|rows| set.materialize(schema, rows, prorated_blob_cap(total_blob, total_rows, rows.len())))
        .collect()
}

/// Scatter `sources` into one batch per worker, routed by `spec`: Z-set `+`
/// across the sources, restricted to each worker's routing slice. A slice
/// claims `Consolidated` iff every source did.
///
/// Refused when `spec` does not route against `schema` — see [`ScatterKey::new`],
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
    let mut key = ScatterKey::new(spec, schema, num_workers)?;
    let mem: Vec<MemBatch> = sources.iter().map(|b| b.as_mem_batch()).collect();

    Ok(WORKER_ROWS.with(|pool| {
        let mut pool = pool.borrow_mut();
        let slots = crate::storage::reset_slots(&mut pool, num_workers);

        if consolidated && mem.len() >= 2 {
            // One exemplar per (PK, payload) group, and its members are
            // byte-equal wherever a key can read, so it routes for all of them.
            run_merge(&mem, schema, |si, row, w| {
                slots[key.worker(&mem[si], row)].push((si as u32, row as u32, w));
            });
        } else {
            for (si, mb) in mem.iter().enumerate() {
                key.route_into(mb, si as u32, slots);
            }
        }

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
