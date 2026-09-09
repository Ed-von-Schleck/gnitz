//! Exchange relay/scatter: `op_repartition_batches_mode`,
//! `op_relay_scatter_consolidated_mode`, `op_relay_broadcast`.

use std::cell::RefCell;

use crate::schema::{OpBuildErr, SchemaDescriptor};
use crate::storage::{
    mem_batch_to_unified, prorated_blob_cap, run_merge, scatter_unified_sources, write_to_batch, Batch, Layout,
    MemBatch,
};

use super::router::{ScatterKey, ScatterSpec};

// Thread-local pool: reuse the per-worker (source, row, weight) scratch across calls.
thread_local! {
    static WORKER_ROWS: RefCell<Vec<Vec<(u32, u32, i64)>>> = const { RefCell::new(Vec::new()) };
}

/// One `MemBatch` per source slot, an empty-batch view standing in for an absent
/// or rowless one: that keeps `si` a direct index into every per-source table
/// below, and no emitted row can name a zero-row source.
fn mem_batch_slots<'a>(sources: &[Option<&'a Batch>], empty: &'a Batch) -> Vec<MemBatch<'a>> {
    sources
        .iter()
        .map(|opt| match opt {
            Some(s) if s.count > 0 => s.as_mem_batch(),
            _ => empty.as_mem_batch(),
        })
        .collect()
}

fn mem_batch_blob_cap(mem_batches: &[MemBatch]) -> usize {
    mem_batches.iter().map(|mb| mb.blob.len()).sum::<usize>().max(1)
}

/// Materialize per-worker `(source, row, weight)` lists into per-worker batches:
/// blob capacity prorated by each worker's row share of `total_blob`, rows
/// copied via the shared column-first scatter. Outputs are left `Raw` — each caller
/// owns its layout gate. The two production gates differ in both condition and
/// operation (`is_pk_routing && single source → inherit_layout(src)` vs
/// `single_source → certify_layout(Consolidated)`); merging them would falsely
/// certify Consolidated and silently corrupt non-linear reduce weights.
fn worker_rows_to_batches(
    schema: &SchemaDescriptor,
    mem_batches: &[MemBatch],
    worker_rows: &[Vec<(u32, u32, i64)>],
    total_blob: usize,
) -> Vec<Batch> {
    // One view per source slot, built once and shared by every worker's scatter.
    let mut cols = Vec::with_capacity(mem_batches.len() * schema.num_payload_cols());
    let unified: Vec<_> = mem_batches
        .iter()
        .map(|mb| mem_batch_to_unified(mb, schema, &mut cols))
        .collect();
    let total_rows: usize = worker_rows.iter().map(|v| v.len()).sum();
    worker_rows
        .iter()
        .map(|rows| {
            if rows.is_empty() {
                return Batch::empty_with_schema(schema);
            }
            let blob_cap = prorated_blob_cap(total_blob, total_rows, rows.len());
            write_to_batch(schema, rows.len(), blob_cap, |writer| {
                scatter_unified_sources(&unified, &cols, rows, writer);
            })
        })
        .collect()
}

/// Refused when `spec` does not route against `schema` — see [`ScatterKey::new`],
/// which owns the reason.
pub fn op_repartition_batches(
    sources: &[Option<&Batch>],
    spec: ScatterSpec<'_>,
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Result<Vec<Batch>, OpBuildErr> {
    gnitz_debug!(
        "op_repartition_batches: sources={} spec={:?}",
        sources.iter().filter(|s| matches!(s, Some(sb) if sb.count > 0)).count(),
        spec,
    );
    let empty = Batch::empty_with_schema(schema);
    let mem_batches = mem_batch_slots(sources, &empty);

    let total_blob = mem_batch_blob_cap(&mem_batches);
    let mut scatter_key = ScatterKey::new(spec, schema, num_workers)?;

    Ok(WORKER_ROWS.with(|pool| {
        let mut worker_rows = pool.borrow_mut();
        super::reset_slots(&mut worker_rows, num_workers);

        let is_pk_routing = scatter_key.is_pk_routed();
        for (si, mb) in mem_batches.iter().enumerate() {
            for i in 0..mb.count {
                worker_rows[scatter_key.worker(mb, i)].push((si as u32, i as u32, mb.get_weight(i)));
            }
        }

        let mut out = worker_rows_to_batches(schema, &mem_batches, &worker_rows[..num_workers], total_blob);

        // PK-routed, single-source repartition preserves source order and
        // distinctness per worker (a PK group never splits across workers, so a
        // worker's sub-batch is an in-order subset of one sorted source).
        // Multi-source scatter is per-source-concatenated — not globally sorted —
        // so it propagates nothing. Non-PK routing is excluded because a PK group
        // can then split across workers, costing per-worker distinctness.
        if is_pk_routing {
            let mut contributing = sources.iter().filter_map(|s| *s).filter(|b| b.count > 0);
            if let (Some(src), None) = (contributing.next(), contributing.next()) {
                for b in out.iter_mut() {
                    if b.count > 0 {
                        // PK-routed single source: each per-worker sub-batch is an
                        // in-order subset of one source, so it carries the source's
                        // layout faithfully.
                        b.inherit_layout(src);
                    }
                }
            }
        }
        out
    }))
}

/// Refused when `spec` does not route against `schema` — see [`ScatterKey::new`],
/// which owns the reason.
pub fn op_relay_scatter_consolidated(
    sources: &[Option<&Batch>],
    spec: ScatterSpec<'_>,
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Result<Vec<Batch>, OpBuildErr> {
    // The dispatch gate selected these on `is_consolidated()`; debug-verify each
    // source's data here before either walk fast-paths on it.
    #[cfg(debug_assertions)]
    for s in sources.iter().flatten() {
        s.debug_verify_consolidated(schema);
    }
    let empty = Batch::empty_with_schema(schema);
    let mem_batches = mem_batch_slots(sources, &empty);
    let mut live = mem_batches
        .iter()
        .enumerate()
        .filter(|(_, mb)| mb.count > 0)
        .map(|(si, _)| si);
    let (first, second) = (live.next(), live.next());
    gnitz_debug!(
        "op_relay_scatter_consolidated: sources={}",
        mem_batches.iter().filter(|mb| mb.count > 0).count(),
    );
    // Built before the empty short-circuit, so an unroutable key is refused
    // whether or not this round carries rows.
    let mut scatter_key = ScatterKey::new(spec, schema, num_workers)?;
    let Some(first) = first else {
        return Ok((0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect());
    };
    let total_blob: usize = mem_batch_blob_cap(&mem_batches);

    Ok(WORKER_ROWS.with(|pool| {
        let mut worker_rows = pool.borrow_mut();
        super::reset_slots(&mut worker_rows, num_workers);

        match second {
            // One contributing source: already ordered and folded, so a
            // tournament would compare per row to fold nothing. Reached by every
            // replicated relation (`prepare_relay`) and every single-worker run.
            None => {
                let mb = &mem_batches[first];
                for row in 0..mb.count {
                    worker_rows[scatter_key.worker(mb, row)].push((first as u32, row as u32, mb.get_weight(row)));
                }
            }
            // Two or more: the shared N-way merge, which folds across sources —
            // one key can carry an insert in one and a retraction in another.
            // `worker` is pure in `(batch, row)` and a (PK, payload) group's rows
            // are identical, so the exemplar routes where every member would.
            Some(_) => run_merge(&mem_batches, schema, |si, row, w| {
                worker_rows[scatter_key.worker(&mem_batches[si], row)].push((si as u32, row as u32, w));
            }),
        }

        let mut out = worker_rows_to_batches(schema, &mem_batches, &worker_rows[..num_workers], total_blob);
        // Both walks emit in (PK, payload) order at net weights with no ghost, and
        // a subsequence of that is still both — so every slice is consolidated,
        // whatever the routing key was.
        for b in out.iter_mut() {
            if b.count > 0 {
                b.certify_layout(Layout::Consolidated);
            }
        }
        out
    }))
}

/// Broadcast relay: the FULL delta, delivered to every worker. The per-worker
/// source slices are disjoint (each is one worker's base-table-PK-partitioned
/// slice), so their concatenation is the full delta with no duplication. One
/// batch is built; the SAL emit references it once per worker slot
/// (`RelayDest::Broadcast`), so no per-worker clone is materialized. The
/// range and cross probes need the whole delta on every worker — a match can
/// live on any worker's trace — which the equality scatter (one destination per
/// row) cannot deliver. Sibling of `op_repartition_batches` /
/// `op_relay_scatter_consolidated`, but without a `ScatterSpec` (broadcast
/// routes nothing).
pub fn op_relay_broadcast(sources: &[Option<&Batch>], schema: &SchemaDescriptor) -> Batch {
    let total: usize = sources.iter().flatten().map(|b| b.count).sum();
    if total == 0 {
        return Batch::empty_with_schema(schema);
    }
    // Concatenate the disjoint slices into the full delta once (append_batch
    // relocates each source's blob, so independent source blobs stay valid).
    let mut full = Batch::with_capacity(schema, total);
    for src in sources.iter().flatten() {
        if src.count > 0 {
            full.append_batch(src, 0, src.count);
        }
    }
    full
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/relay.rs"]
mod tests;
