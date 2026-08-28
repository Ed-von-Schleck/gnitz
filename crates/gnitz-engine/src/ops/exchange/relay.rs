//! Exchange relay/scatter: `op_repartition_batches_mode`,
//! `op_relay_scatter_consolidated_mode`, `op_relay_broadcast`.

use std::cell::RefCell;
use std::cmp::Ordering;

use crate::schema::key::{compare_pk_ordering, pack_pk_be};
use crate::schema::SchemaDescriptor;
use crate::storage::{
    mem_batch_to_unified, prorated_blob_cap, scatter_unified_sources, write_to_batch, Batch, Layout, MemBatch,
};
use gnitz_wire::MAX_WORKERS;

use super::router::{RouteMode, ScatterKey};

// Thread-local pool: reuse the per-worker (source, row, weight) scratch across calls.
thread_local! {
    static WORKER_ROWS: RefCell<Vec<Vec<(u32, u32, i64)>>> = const { RefCell::new(Vec::new()) };
}

fn mem_batch_blob_cap(mem_batches: &[Option<MemBatch>]) -> usize {
    mem_batches
        .iter()
        .filter_map(|o| o.as_ref())
        .map(|mb| mb.blob.len())
        .sum::<usize>()
        .max(1)
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
    mem_batches: &[Option<MemBatch>],
    worker_rows: &[Vec<(u32, u32, i64)>],
    total_blob: usize,
) -> Vec<Batch> {
    // One view per source slot, built once and shared by every worker's scatter.
    // An absent slot views one live empty batch rather than being skipped: that
    // keeps `si` a direct index into `unified`, and no emitted row names it
    // anyway (the walks above only visit `Some` sources).
    let empty = Batch::empty_with_schema(schema);
    let empty_mb = empty.as_mem_batch();
    let mut cols = Vec::new();
    let unified: Vec<_> = mem_batches
        .iter()
        .map(|o| mem_batch_to_unified(o.as_ref().unwrap_or(&empty_mb), schema, &mut cols))
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

pub fn op_repartition_batches_mode(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    mode: RouteMode,
) -> Vec<Batch> {
    gnitz_debug!(
        "op_repartition_batches_mode: sources={} mode={:?}",
        sources.iter().filter(|s| matches!(s, Some(sb) if sb.count > 0)).count(),
        mode,
    );
    let mem_batches: Vec<Option<MemBatch>> = sources
        .iter()
        .map(|opt| match opt {
            Some(s) if s.count > 0 => Some(s.as_mem_batch()),
            _ => None,
        })
        .collect();

    let total_blob = mem_batch_blob_cap(&mem_batches);

    WORKER_ROWS.with(|pool| {
        let mut worker_rows = pool.borrow_mut();
        super::reset_slots(&mut worker_rows, num_workers);

        // One `ScatterKey` per scatter, built out of the row loop: native PK
        // bytes when the key is exactly the PK (so the owner matches the one the
        // write path routes to), a packed `_join_pk` for a `JoinPromote` key, the
        // group fold for a `GroupKey` key.
        let mut scatter_key = ScatterKey::new(mode, col_indices, target_tcs, schema, num_workers);
        let is_pk_routing = scatter_key.is_pk_routed();
        for (si, mb_opt) in mem_batches.iter().enumerate() {
            let mb = match mb_opt {
                Some(m) => m,
                None => continue,
            };
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
    })
}

/// Unified K-way merge-walk skeleton — one body, one comparator, every PK width.
/// `order_cache[si]` holds each active source's order-preserving `pack_pk_be`
/// (the leading-≤16 OPK bytes packed big-endian: the whole key for `pk_stride ≤ 16`,
/// the order-preserving prefix for `> 16`; `pack_pk_be` never panics, unlike
/// `get_pk`, which debug_asserts `stride ≤ 16`). The winner each step is the source
/// whose cached key is smallest — the register compare is the common fast path, and
/// a key tie defers to the canonical `compare_pk_ordering` on the live OPK bytes,
/// whose own `len > 16` guard no-ops a `≤ 16` tie (the cached key is then the whole
/// PK, so equal keys are byte-equal) and reserves the byte tiebreak for a `> 16`
/// prefix collision — then on ascending source index (deterministic under
/// `swap_remove`'s scramble of `active_sources`). `route` partitions each emitted
/// row through `worker_for_pk_bytes` (or the group/packer path) — byte-correct at
/// any width. No width fork, no `get_pk`, no `pk_cache`.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn relay_walk_inner<'a, Route>(
    mem_batches: &[Option<MemBatch<'a>>],
    worker_rows: &mut [Vec<(u32, u32, i64)>],
    order_cache: &mut [u128; MAX_WORKERS],
    mut cursors: [u32; MAX_WORKERS],
    mut active_sources: [u8; MAX_WORKERS],
    mut num_active: usize,
    mut route: Route,
) where
    Route: FnMut(&MemBatch<'a>, usize) -> usize,
{
    while num_active > 0 {
        if num_active == 1 {
            // Bulk-drain the sole remaining source without PK comparisons.
            let si = active_sources[0] as usize;
            let mb = mem_batches[si].as_ref().unwrap();
            for row in cursors[si] as usize..mb.count {
                worker_rows[route(mb, row)].push((si as u32, row as u32, mb.get_weight(row)));
            }
            return;
        }

        // Winner = smallest cached `pack_pk_be`, a cache tie settled by the
        // comparator below, then by ascending source index.
        let mut best_pos = 0usize;
        let mut best_si = active_sources[0];
        let mut best_key = order_cache[best_si as usize];
        #[allow(clippy::needless_range_loop)]
        for pos in 1..num_active {
            let si = active_sources[pos];
            let key = order_cache[si as usize];
            let ord = match key.cmp(&best_key) {
                // Cached-key tie: settle on the live OPK bytes through the canonical
                // comparator. Its `len > 16` guard no-ops a `≤ 16` tie (the cached
                // key is the whole PK, so equal keys are byte-equal) and runs the
                // memcmp only on a `> 16` prefix collision.
                Ordering::Equal => compare_pk_ordering(
                    mem_batches[si as usize]
                        .as_ref()
                        .unwrap()
                        .get_pk_bytes(cursors[si as usize] as usize),
                    mem_batches[best_si as usize]
                        .as_ref()
                        .unwrap()
                        .get_pk_bytes(cursors[best_si as usize] as usize),
                ),
                other => other,
            };
            if ord == Ordering::Less || (ord == Ordering::Equal && si < best_si) {
                best_pos = pos;
                best_si = si;
                best_key = key;
            }
        }

        let best_si = best_si as usize;
        let row = cursors[best_si] as usize;
        cursors[best_si] += 1;
        let mb = mem_batches[best_si].as_ref().unwrap();
        worker_rows[route(mb, row)].push((best_si as u32, row as u32, mb.get_weight(row)));

        let new_cur = cursors[best_si] as usize;
        if new_cur == mb.count {
            // Swap-remove the exhausted source in O(1).
            num_active -= 1;
            active_sources[best_pos] = active_sources[num_active];
        } else {
            order_cache[best_si] = pack_pk_be(mb.get_pk_bytes(new_cur));
        }
    }
}

/// Scatter pre-consolidated sources to workers in PK order using a K-cursor merge walk.
/// Fills `worker_rows[0..num_workers]` with PK-ordered (si, row) index pairs.
/// No post-sort needed. Caller holds `WORKER_ROWS` borrow_mut.
fn relay_scatter_merge_walk(
    mem_batches: &[Option<MemBatch<'_>>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    worker_rows: &mut Vec<Vec<(u32, u32, i64)>>,
    mode: RouteMode,
) {
    assert!(
        mem_batches.len() <= MAX_WORKERS,
        "one source per worker: {} sources exceeds MAX_WORKERS",
        mem_batches.len()
    );
    let cursors = [0u32; MAX_WORKERS];
    let mut order_cache = [0u128; MAX_WORKERS];
    let mut active_sources = [0u8; MAX_WORKERS];
    let mut num_active: usize = 0;

    super::reset_slots(worker_rows, num_workers);

    // One order-preserving winner key per active source, valid at every PK width:
    // `pack_pk_be` packs the leading ≤16 OPK bytes big-endian — the whole key for
    // stride ≤ 16, the order-preserving prefix for > 16. Never panics (unlike
    // `get_pk`, which debug_asserts stride ≤ 16).
    for (si, mb_opt) in mem_batches.iter().enumerate() {
        if let Some(mb) = mb_opt {
            if mb.count > 0 {
                order_cache[si] = pack_pk_be(mb.get_pk_bytes(0));
                active_sources[num_active] = si as u8;
                num_active += 1;
            }
        }
    }

    // One `ScatterKey` picked once (never per row): native PK bytes when the key
    // is exactly the PK (byte-identical to the old narrow `worker_for_key(get_pk)`
    // route — both reduce to `mix(widen_pk_be(bytes))`), a packed `_join_pk` for
    // JoinPromote, the group fold for GroupKey. The `&mut scatter_key` capture
    // (the packer's inline scratch) is why `relay_walk_inner` takes `FnMut`.
    let mut scatter_key = ScatterKey::new(mode, col_indices, target_tcs, schema, num_workers);
    relay_walk_inner(
        mem_batches,
        worker_rows,
        &mut order_cache,
        cursors,
        active_sources,
        num_active,
        |mb: &MemBatch, row: usize| scatter_key.worker(mb, row),
    );
}

pub fn op_relay_scatter_consolidated_mode(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    mode: RouteMode,
) -> Vec<Batch> {
    // The dispatch gate selected these on `is_consolidated()`; debug-verify each
    // source's data here before the merge-walk fast-paths on it.
    #[cfg(debug_assertions)]
    for s in sources.iter().flatten() {
        s.debug_verify_consolidated(schema);
    }
    let mem_batches: Vec<Option<MemBatch>> = sources
        .iter()
        .map(|opt| match opt {
            Some(s) if s.count > 0 => Some(s.as_mem_batch()),
            _ => None,
        })
        .collect();
    gnitz_debug!(
        "op_relay_scatter_consolidated: sources={}",
        mem_batches.iter().filter(|o| o.is_some()).count(),
    );
    if mem_batches.iter().all(|o| o.is_none()) {
        return (0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect();
    }
    let total_blob: usize = mem_batch_blob_cap(&mem_batches);

    // The merge-walk yields globally sorted output across all sources. With
    // exactly one contributing source no cross-source duplicate PK is possible,
    // so the per-worker output is also consolidated; with ≥2 sources the same PK
    // can appear in two sources (e.g. an insert in one, a retraction in another),
    // so consolidated stays false.
    let single_source = mem_batches.iter().filter(|o| o.is_some()).count() == 1;

    WORKER_ROWS.with(|pool| {
        let mut worker_rows = pool.borrow_mut();
        relay_scatter_merge_walk(
            &mem_batches,
            col_indices,
            target_tcs,
            schema,
            num_workers,
            &mut worker_rows,
            mode,
        );

        let mut out = worker_rows_to_batches(schema, &mem_batches, &worker_rows[..num_workers], total_blob);
        // One contributing source ⇒ each worker's PK-routed slice is an
        // in-order, distinct subset of a consolidated source: certify it.
        // Multiple sources merge by PK only (not (PK, payload)), so the
        // output is not even `Sorted` — leave it `Raw`; the consumer re-folds.
        if single_source {
            for b in out.iter_mut() {
                if b.count > 0 {
                    b.certify_layout(Layout::Consolidated, schema);
                }
            }
        }
        out
    })
}

/// Broadcast relay: the FULL delta, delivered to every worker. The per-worker
/// source slices are disjoint (each is one worker's base-table-PK-partitioned
/// slice), so their concatenation is the full delta with no duplication. One
/// batch is built; the SAL emit references it once per worker slot
/// (`RelayDest::Broadcast`), so no per-worker clone is materialized. The
/// range-join probe needs the whole delta on every worker — a range match can
/// live on any worker's trace — which the equality scatter (one destination per
/// row) cannot deliver. Sibling of `op_repartition_batches_mode` /
/// `op_relay_scatter_consolidated_mode`, but without `col_indices` / `RouteMode`
/// (broadcast routes nothing).
pub fn op_relay_broadcast(sources: &[Option<&Batch>], schema: &SchemaDescriptor) -> Batch {
    let total: usize = sources.iter().flatten().map(|b| b.count).sum();
    if total == 0 {
        return Batch::empty_with_schema(schema);
    }
    // Concatenate the disjoint slices into the full delta once (append_batch
    // relocates each source's blob, so independent source blobs stay valid).
    let mut full = Batch::with_capacity(*schema, total);
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
