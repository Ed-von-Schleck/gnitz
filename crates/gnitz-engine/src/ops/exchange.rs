//! Exchange repartitioning: PartitionRouter, op_repartition_batch, op_relay_scatter_consolidated, op_relay_scatter, op_multi_scatter.

use std::cell::RefCell;

use rustc_hash::FxHashMap;

use std::cmp::Ordering;

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ConsolidatedBatch, MemBatch, compare_pk_bytes, pk_sort_key, partition_for_key, partition_for_pk_bytes, write_to_batch, scatter_multi_source};

use super::util::extract_group_key;

// ---------------------------------------------------------------------------
// Exchange repartitioning
// ---------------------------------------------------------------------------

// Thread-local pool: reuse Vec<Vec<u32>> index scratch across calls so
// steady-state repartition/scatter ops allocate nothing for routing tables.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
    static WORKER_ROWS: RefCell<Vec<Vec<(u8, u32)>>> = const { RefCell::new(Vec::new()) };
}

fn mem_batch_blob_cap(mem_batches: &[Option<MemBatch>]) -> usize {
    mem_batches.iter()
        .filter_map(|o| o.as_ref())
        .map(|mb| mb.blob.len())
        .sum::<usize>()
        .max(1)
}

/// Extract the routing key for `col_idx` from row `row` of `mb`.
/// STRING columns are hashed to u64 (stored as u128); all others use the full value.
fn extract_col_key(mb: &MemBatch<'_>, row: usize, col_idx: usize, schema: &SchemaDescriptor) -> u128 {
    let loc = schema.locate(col_idx);
    // NULL values shard together. Return 0 without touching the slot: a NULL
    // string/blob column's German-string length field is uninitialized, and a
    // garbage length > SHORT_STRING_THRESHOLD would drive a wild heap read. PK
    // columns are never null, so this only fires for a payload column.
    if loc.is_null(mb, row) {
        return 0u128;
    }
    // German strings have no order-preserving routing image; route by a content
    // hash (a raw byte image would alias distinct strings sharing a prefix). PK
    // columns are never STRING/BLOB, so `is_german_string` already implies a
    // payload column. A 64-bit hash widened to u128 is deliberate: this key feeds
    // only the index-routing cache, whose string seeks fall back to broadcast
    // (`index_route_key` returns None for STRING/BLOB), so it need not match the
    // 128-bit join-scatter image `german_string_promote_key` produces.
    if gnitz_wire::is_german_string(loc.type_code()) {
        let content = crate::schema::german_string_content(loc.bytes(mb, row), mb.blob);
        return if content.is_empty() { 0u128 } else { crate::xxh::checksum(content) as u128 };
    }
    // PK column → widen its OPK bytes; integer / U128 / UUID payload → OPK-encode
    // then widen. Both agree with `partition_for_pk_bytes` on the PK side
    // (including the sign-flip for signed columns). Matched by `index_route_key`,
    // which rebuilds this same key from a native seek value.
    loc.route_key(mb, row)
}

/// Build a 256-entry partition→worker lookup table, hoisting the division out
/// of the per-row loop. `partition_for_key` always returns values in 0..=255.
#[inline]
fn build_w_map(num_workers: usize) -> [usize; 256] {
    let mut map = [0usize; 256];
    for (p, item) in map.iter_mut().enumerate() {
        *item = worker_for_partition(p, num_workers);
    }
    map
}

#[inline]
pub fn worker_for_partition(partition: usize, num_workers: usize) -> usize {
    let chunk = 256 / num_workers;
    (partition / chunk).min(num_workers - 1)
}

/// Keep only the rows this worker owns, by packed-PK partition — the trace-side
/// counterpart of the **pure** range-join broadcast input relay. A pure range
/// join (n_eq == 0) has no eq prefix to scatter by, so it broadcasts; every worker
/// receives the full delta and, before it integrates into the trace, this drops
/// the rows whose `partition_for_pk_bytes` partition is not assigned to
/// `worker_id`. (A band join scatters by the eq prefix instead — its trace is
/// already eq-prefix-partitioned and carries no `PartitionFilter`.) It is the SAME
/// hash the equality scatter (`RouteMode::JoinPromote`)
/// applies to the SAME packed PK bytes, so the integrated trace is partitioned
/// identically to a scattered equi-join trace — no trace replicates, no match
/// duplicates. Worker identity is a compile-time constant baked into the emitted
/// instruction; `num_workers <= 1` (single process) keeps every row.
pub(crate) fn op_partition_filter(
    batch: &Batch,
    schema: &SchemaDescriptor,
    worker_id: u32,
    num_workers: u32,
) -> Batch {
    let n = batch.count;
    if num_workers <= 1 || n == 0 {
        // Single process owns every partition; degenerate to identity (preserving
        // the source's sorted/consolidated flags via clone_batch).
        return batch.clone_batch();
    }
    let nw = num_workers as usize;
    let wid = worker_id as usize;
    let mb = batch.as_mem_batch();

    // Keep just this worker's partitions, routed by the same `worker_for_partition`
    // the equality scatter uses — no 256-entry table needed for a single worker.
    let mut indices: Vec<u32> = Vec::with_capacity(n / nw + 1);
    for i in 0..n {
        if worker_for_partition(partition_for_pk_bytes(mb.get_pk_bytes(i)), nw) == wid {
            indices.push(i as u32);
        }
    }
    let mut out = Batch::from_indexed_rows(&mb, &indices, schema);
    // Filtering keeps the ascending row order and (PK, payload) distinctness of
    // the input, so the consolidated/sorted flags carry through unchanged.
    out.sorted = batch.sorted;
    out.consolidated = batch.consolidated;
    out
}

// ---------------------------------------------------------------------------
// Partition routing cache
// ---------------------------------------------------------------------------

/// Master-side routing cache: maps (table_id, col_idx, key) → worker.
///
/// Populated by `record_routing` after repartitioning unique-indexed columns.
/// Lets the master unicast index seeks instead of broadcasting on cache hit.
pub struct PartitionRouter {
    index_routing: FxHashMap<(u32, u32, u128), u32>,
}

impl PartitionRouter {
    pub fn new() -> Self {
        PartitionRouter {
            index_routing: FxHashMap::default(),
        }
    }

    /// Returns the worker for a given index key, or -1 on cache miss.
    pub fn worker_for_index_key(&self, tid: u32, col_idx: u32, key: u128) -> i32 {
        match self.index_routing.get(&(tid, col_idx, key)) {
            Some(&w) => w as i32,
            None => -1,
        }
    }

    /// Like `record_routing` but iterates over `indices` into `source` rather
    /// than every row of a pre-partitioned sub-batch. Used by the scatter-to-wire
    /// path which never materialises per-worker Batch objects.
    pub fn record_routing_from_source(
        &mut self,
        source: &Batch,
        indices: &[u32],
        schema: &SchemaDescriptor,
        tid: u32,
        col_idx: u32,
        worker: u32,
    ) {
        let mb = source.as_mem_batch();
        for &idx in indices {
            let row = idx as usize;
            let weight = source.get_weight(row);
            let key = extract_col_key(&mb, row, col_idx as usize, schema);
            let map_key = (tid, col_idx, key);
            if weight < 0 {
                self.index_routing.remove(&map_key);
            } else {
                self.index_routing.insert(map_key, worker);
            }
        }
    }

    /// Scan every row in `batch` and record or retract its index key → worker mapping.
    /// Rows with negative weight retract; non-negative weight records.
    /// Test-only: the production path populates the cache through the sibling
    /// `record_routing_from_source` (`write_commit_group → record_index_routing`),
    /// which `fan_out_seek_by_index_async` reads to unicast a unique-index seek.
    #[cfg(test)]
    pub fn record_routing(
        &mut self,
        batch: &Batch,
        schema: &SchemaDescriptor,
        tid: u32,
        col_idx: u32,
        worker: u32,
    ) {
        let mb = batch.as_mem_batch();
        for row in 0..batch.count {
            let weight = batch.get_weight(row);
            let key = extract_col_key(&mb, row, col_idx as usize, schema);
            let map_key = (tid, col_idx, key);
            if weight < 0 {
                self.index_routing.remove(&map_key);
            } else {
                self.index_routing.insert(map_key, worker);
            }
        }
    }
}

/// Scatter routing key for `cols` at `row`. Equal to `extract_group_key` for
/// co-locating equal keys, EXCEPT a NULL in a single non-PK integer column
/// routes by its raw (zero) bytes via `payload_route_key` rather than the
/// null-distinct hash `extract_group_key` uses.
///
/// This must match `PkPromoter::promote_into`, which ignores the null bitmap and
/// OPK-encodes the column bytes into the synthetic `_join_pk`. A LEFT-join
/// NULL-key bypass row reindexes to that same `_join_pk`; routing it by the
/// null-distinct hash would scatter it to a worker that does NOT own its
/// `_join_pk` partition, so the view scan (which reads each `_join_pk` from its
/// partition owner) would never see it. `extract_group_key` itself must keep
/// NULL distinct because it also generates GROUP BY output PKs (op_reduce),
/// where a NULL group and a 0 group must not collide on one PK; scatter routing
/// has no such requirement — local grouping (`compare_by_group_cols`) still
/// separates co-located groups.
fn route_partition_key(
    mb: &MemBatch,
    row: usize,
    cols: &[u32],
    schema: &SchemaDescriptor,
) -> u128 {
    if cols.len() == 1 {
        let c_idx = cols[0] as usize;
        let col = &schema.columns[c_idx];
        let tc = col.type_code;
        if !schema.is_pk_col(c_idx) {
            let loc = schema.locate(c_idx);
            if crate::schema::is_routable_int(tc) {
                // OPK-encode+widen the native value (sign-flip for signed), so it
                // agrees with the `_join_pk` `PkPromoter::promote_into` writes.
                return loc.route_key(mb, row);
            }
            // A string join key reindexes to the same content hash; route by it
            // so the row co-locates with its `_join_pk` partition. A NULL string
            // is a zeroed struct → empty content → 0, matching promote_into.
            if gnitz_wire::is_german_string(tc) {
                return crate::ops::linear::german_string_promote_key(loc.bytes(mb, row), mb.blob);
            }
        }
    }
    extract_group_key(mb, row, schema, cols)
}

/// Which routing key a non-PK scatter uses. A JOIN scatter must route by the
/// key the downstream reindex (`promote_into`) writes as `_join_pk`, so a row
/// lands on the worker that owns its `_join_pk` partition. A GROUP BY / set-op
/// scatter must route by `extract_group_key`, which op_reduce also uses for the
/// group's output PK — the two must agree or the result is mis-gathered. The two
/// keys diverge for nullable and string columns, so the scatter caller picks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RouteMode {
    GroupKey,
    JoinPromote,
}

fn hash_row_for_partition(
    mb: &MemBatch,
    row: usize,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    mode: RouteMode,
) -> usize {
    // Reached only on the non-PK-routing path (every caller guards with
    // `is_pk_routing = col_indices == pk_indices` and routes PK-keyed rows via
    // `partition_for_pk_bytes`), so `col_indices != pk_indices` here.
    // Both key functions handle a single PK column internally.
    let key = match mode {
        RouteMode::JoinPromote => route_partition_key(mb, row, col_indices, schema),
        RouteMode::GroupKey => extract_group_key(mb, row, schema, col_indices),
    };
    partition_for_key(key)
}

/// Whether any key slot carries a cross-width promotion target (`tc != 0`). A
/// promoted key packs at the wider `T`, so it must route through the
/// `ReindexPacker` and must NOT take the native-PK fast-path. The one home of
/// this predicate so `compound_join_packer` and the two scatter gates cannot
/// drift apart.
#[inline]
fn key_is_promoted(target_tcs: &[u8]) -> bool {
    target_tcs.iter().any(|&tc| tc != 0)
}

/// The packer that routes a `JoinPromote` scatter by the SAME packed OPK bytes
/// the reindex Map writes as `_join_pk` — so the delta scatter and the reindexed
/// trace co-partition byte-for-byte. Fires for a compound (len > 1) key OR any
/// active cross-width promotion (a single promoted key must also pack at `T`, not
/// route at its narrow source width). Returns `None` for a non-promoted single
/// key and every GroupKey scatter, which keep the `hash_row_for_partition` path
/// (GROUP BY / set-op rows are never reindexed to a packed PK). Centralised so the
/// two scatter implementations (`op_repartition_batches_mode` and
/// `relay_scatter_merge_walk`) gate on the exact same predicate and cannot drift.
fn compound_join_packer(
    mode: RouteMode,
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
) -> Option<crate::ops::linear::ReindexPacker> {
    (mode == RouteMode::JoinPromote && (col_indices.len() > 1 || key_is_promoted(target_tcs)))
        .then(|| crate::ops::linear::ReindexPacker::new(schema, col_indices, target_tcs))
}

fn fill_worker_indices(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
    out: &mut Vec<Vec<u32>>,
) {
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);
    if out.len() < num_workers {
        out.resize_with(num_workers, Vec::new);
    }
    out[..num_workers].iter_mut().for_each(Vec::clear);
    // Strict sequence equality: route by PK bytes only when `col_indices` is
    // exactly `pk_indices()` in order. Set equality (`group_cols_eq_pk`) would
    // accept a permuted compound PK and disagree with `partition_for_pk_bytes`,
    // which always hashes the OPK bytes in schema order — silently dropping
    // join rows. `partition_for_pk_bytes` is the universal PK router at every
    // width (it calls `widen_pk_be`), so narrow PKs take it too.
    let is_pk_routing = col_indices == schema.pk_indices();
    if is_pk_routing {
        // Write-side table-key scatter: `partition_for_pk` hashes the table's
        // distribution prefix, exactly as `PartitionedTable` ingest/probe do, so a
        // row's DML scatter lands on the worker that owns its partition. The
        // join-relay scatters route by the *join* key over a derived schema and
        // call `partition_for_pk_bytes` directly (see `op_repartition_batches_mode`).
        for i in 0..batch.count {
            let partition = schema.partition_for_pk(mb.get_pk_bytes(i));
            out[w_map[partition]].push(i as u32);
        }
    } else {
        for i in 0..batch.count {
            // PK-routing for index seeks; the non-PK fallback uses the group key.
            let partition = hash_row_for_partition(&mb, i, col_indices, schema, RouteMode::GroupKey);
            out[w_map[partition]].push(i as u32);
        }
    }
}

/// Compute per-worker row indices into the TLS pool and call `f` with a
/// borrowed view, avoiding the clone done by `compute_worker_indices`.
///
/// `f` must not call `compute_worker_indices` or `with_worker_indices` —
/// the `SCATTER_INDICES` `RefCell` is already mutably borrowed.
pub fn with_worker_indices<F, R>(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
    f: F,
) -> R
where
    F: FnOnce(&[Vec<u32>]) -> R,
{
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        fill_worker_indices(batch, col_indices, schema, num_workers, &mut worker_indices);
        f(&worker_indices[..num_workers])
    })
}

/// Compute per-worker row-index lists for `batch` without building sub-batches.
/// Returns `worker_indices[w]` = row indices from `batch` destined for worker `w`.
/// Use `with_worker_indices` instead when the caller can borrow the routing table
/// for the duration of the scatter.
#[cfg(test)]
pub fn compute_worker_indices(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<u32>> {
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        fill_worker_indices(batch, col_indices, schema, num_workers, &mut worker_indices);
        worker_indices[..num_workers].to_vec()
    })
}

#[cfg(test)]
pub fn op_repartition_batch(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    gnitz_debug!("op_repartition_batch: count={} num_workers={}", batch.count, num_workers);
    let worker_indices = compute_worker_indices(batch, col_indices, schema, num_workers);
    let mb = batch.as_mem_batch();
    // Single source by definition; mirror the production single-source rule.
    // `compute_worker_indices` does not return the routing decision, so recompute
    // `is_pk_routing` here (a cheap slice compare).
    let is_pk_routing = col_indices == schema.pk_indices();
    let mut out: Vec<Batch> = worker_indices.into_iter()
        .map(|indices| {
            if !indices.is_empty() {
                Batch::from_indexed_rows(&mb, &indices, schema)
            } else {
                Batch::empty_with_schema(schema)
            }
        })
        .collect();
    if is_pk_routing {
        for b in out.iter_mut() {
            if b.count > 0 {
                b.sorted = batch.sorted;
                b.consolidated = batch.consolidated;
            }
        }
    }
    out
}

pub(crate) fn op_repartition_batches_mode(
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
    assert!(sources.len() <= 256, "source index must fit in u8 (got {})", sources.len());
    let w_map = build_w_map(num_workers);

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
        if worker_rows.len() < num_workers {
            worker_rows.resize_with(num_workers, Vec::new);
        }
        for w in 0..num_workers { worker_rows[w].clear(); }

        // Same hoist as `fill_worker_indices`: wide-PK single-column routing
        // and compound-PK routing both go through `partition_for_pk_bytes`
        // so the partition matches `PartitionedTable::local_index`. Mixing
        // the hash-combine path with PK-existence checks would route rows
        // to the wrong worker.
        // A promoted join key must not take the native-PK fast-path: its source PK
        // bytes are at the narrow width while the trace `_join_pk` is `T`-wide, so
        // the two would land on different workers. Route it through the packer.
        let is_pk_routing = col_indices == schema.pk_indices() && !key_is_promoted(target_tcs);
        // Compound/promoted JoinPromote routes by the packed `_join_pk` (see
        // `compound_join_packer`); the `pack_buf` is hoisted out of the row loop
        // because `pack_into` fully overwrites `buf[..out_stride]` each row.
        let join_packer = compound_join_packer(mode, col_indices, target_tcs, schema);
        let mut pack_buf = [0u8; gnitz_wire::MAX_PK_BYTES];
        for (si, mb_opt) in mem_batches.iter().enumerate() {
            let mb = match mb_opt { Some(m) => m, None => continue };
            if is_pk_routing {
                for i in 0..mb.count {
                    let partition = partition_for_pk_bytes(mb.get_pk_bytes(i));
                    worker_rows[w_map[partition]].push((si as u8, i as u32));
                }
            } else if let Some(p) = &join_packer {
                for i in 0..mb.count {
                    p.pack_into(&mut pack_buf, mb, i);
                    let partition = partition_for_pk_bytes(&pack_buf[..p.out_stride]);
                    worker_rows[w_map[partition]].push((si as u8, i as u32));
                }
            } else {
                for i in 0..mb.count {
                    let partition = hash_row_for_partition(mb, i, col_indices, schema, mode);
                    worker_rows[w_map[partition]].push((si as u8, i as u32));
                }
            }
        }

        let total_rows: usize = worker_rows[..num_workers].iter().map(|v| v.len()).sum();
        let mut out: Vec<Batch> = (0..num_workers)
            .map(|w| {
                if worker_rows[w].is_empty() {
                    return Batch::empty_with_schema(schema);
                }
                let blob_cap = (total_blob * worker_rows[w].len() / total_rows).max(1);
                write_to_batch(schema, worker_rows[w].len(), blob_cap, |writer| {
                    scatter_multi_source(&mem_batches, &worker_rows[w], writer);
                })
            })
            .collect();

        // PK-routed, single-source repartition preserves source order and
        // distinctness per worker (a PK group never splits across workers, so a
        // worker's sub-batch is an in-order subset of one sorted source).
        // Multi-source scatter is per-source-concatenated — not globally sorted
        // — so it propagates nothing, and non-PK routing destroys PK order.
        if is_pk_routing {
            let mut contributing = sources.iter().filter_map(|s| *s).filter(|b| b.count > 0);
            if let (Some(src), None) = (contributing.next(), contributing.next()) {
                for b in out.iter_mut() {
                    if b.count > 0 {
                        b.sorted = src.sorted;
                        b.consolidated = src.consolidated;
                    }
                }
            }
        }
        out
    })
}

/// Unified merge-walk skeleton. One body owns the determinism-critical logic
/// (bulk-drain, source-index tiebreak, swap_remove). `DO_REFILL` const-gates
/// the narrow-only cache refill so the wide path never calls `get_pk`
/// (which would panic for `pk_stride > 16`). The `pk_val` argument to `route`
/// must also be gated: function arguments are eagerly evaluated, so reading
/// `mb.get_pk(row)` unconditionally would panic on wide-PK batches even
/// though the wide route closure ignores the value.
///
/// Two parallel caches with disjoint roles: `pk_cache` holds the raw routing
/// value (`get_pk`, feeds `partition_for_key` — order-irrelevant, must stay
/// consistent with every partitioner); `order_cache` holds the order-preserving
/// `pk_sort_key`, read only by `select_min` for the winner pick. Decoupling
/// them lets the winner pick use a raw `u128` compare without disturbing
/// routing.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn relay_walk_inner<'a, const DO_REFILL: bool, Sel, Route>(
    mem_batches: &[Option<MemBatch<'a>>],
    w_map: &[usize; 256],
    worker_rows: &mut [Vec<(u8, u32)>],
    pk_cache: &mut [u128; 256],
    order_cache: &mut [u128; 256],
    _schema: &SchemaDescriptor,
    mut cursors: [u32; 256],
    mut active_sources: [u8; 256],
    mut num_active: usize,
    select_min: Sel,
    route: Route,
) where
    Sel: Fn(&[u128; 256], &[u32; 256], &[u8; 256], usize) -> usize + Copy,
    Route: Fn(&MemBatch<'a>, usize, u128) -> usize + Copy,
{
    while num_active > 0 {
        if num_active == 1 {
            // Bulk-drain the sole remaining source without PK comparisons.
            let si = active_sources[0] as usize;
            let mb = mem_batches[si].as_ref().unwrap();
            for row in cursors[si] as usize..mb.count {
                let pk_val = if DO_REFILL { mb.get_pk(row) } else { 0u128 };
                worker_rows[w_map[route(mb, row, pk_val)]].push((si as u8, row as u32));
            }
            return;
        }

        let best_pos = select_min(&*order_cache, &cursors, &active_sources, num_active);
        let best_si = active_sources[best_pos] as usize;
        let row = cursors[best_si] as usize;
        cursors[best_si] += 1;
        let mb = mem_batches[best_si].as_ref().unwrap();
        // K-way path: pass the already-cached PK to `route` so narrow PK
        // routing reuses the loaded value instead of re-reading from `mb`.
        let pk_val = if DO_REFILL { pk_cache[best_si] } else { 0u128 };
        worker_rows[w_map[route(mb, row, pk_val)]].push((best_si as u8, row as u32));

        let new_cur = cursors[best_si] as usize;
        if new_cur == mb.count {
            // Swap-remove the exhausted source in O(1).
            num_active -= 1;
            active_sources[best_pos] = active_sources[num_active];
        } else if DO_REFILL {
            pk_cache[best_si] = mb.get_pk(new_cur);
            order_cache[best_si] = pk_sort_key(mb.get_pk_bytes(new_cur));
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
    worker_rows: &mut Vec<Vec<(u8, u32)>>,
    mode: RouteMode,
) {
    assert!(mem_batches.len() <= 256, "source index must fit in u8 (got {})", mem_batches.len());
    let w_map = build_w_map(num_workers);
    let cursors = [0u32; 256];
    let mut pk_cache = [0u128; 256];
    // Parallel order-preserving key cache for the winner pick (narrow only);
    // pk_cache keeps the raw routing value. +4 KiB stack, only active slots read.
    let mut order_cache = [0u128; 256];
    let mut active_sources = [0u8; 256];
    let mut num_active: usize = 0;

    if worker_rows.len() < num_workers {
        worker_rows.resize_with(num_workers, Vec::new);
    }
    worker_rows[..num_workers].iter_mut().for_each(Vec::clear);

    // pk_cache (raw routing value) and order_cache (order-preserving winner key)
    // are filled for narrow regions only; wide PKs (pk_stride > 16) cannot pack
    // into u128 and would panic in get_pk — the wide path reads PK bytes direct.
    let is_wide = schema.pk_is_wide();
    for (si, mb_opt) in mem_batches.iter().enumerate() {
        if let Some(mb) = mb_opt {
            if mb.count > 0 {
                if !is_wide {
                    pk_cache[si] = mb.get_pk(0);
                    order_cache[si] = pk_sort_key(mb.get_pk_bytes(0));
                }
                active_sources[num_active] = si as u8;
                num_active += 1;
            }
        }
    }

    // Strict sequence equality (see `fill_worker_indices`): set equality would
    // route a permuted compound PK to a different worker than the schema-order
    // `partition_for_pk_bytes` on the other join side. For narrow PKs,
    // `partition_for_key(get_pk value)` equals `partition_for_pk_bytes(OPK)`
    // (both reduce to `partition_for_key(widen_pk_be)`), so the value-cache
    // route path stays consistent.
    // A promoted join key must not take the native-PK fast-path (its source PK is
    // at the narrow width, the trace `_join_pk` is `T`-wide); route it through the
    // packer so the two sides co-partition.
    let is_pk_routing = col_indices == schema.pk_indices() && !key_is_promoted(target_tcs);

    // Compound/promoted JoinPromote routes by the packed `_join_pk` (see
    // `compound_join_packer`) — the consolidated join path, exactly where
    // co-partition must hold. The packer is captured by shared ref so the
    // `route_group` closure stays `Copy` (`&T` is `Copy`), satisfying the
    // `Route: Fn + Copy` bound on `relay_walk_inner`; the per-row scratch buffer
    // therefore lives INSIDE the closure (a captured `&mut buf` would make it
    // non-`Copy`).
    let join_packer = compound_join_packer(mode, col_indices, target_tcs, schema);

    // Route closures — 3-arg form. pk_val is the cached PK in the K-way
    // path and `get_pk(row)` in the bulk-drain (both narrow only).
    let route_pk_narrow = |_mb: &MemBatch, _row: usize, pk_val: u128| partition_for_key(pk_val);
    let route_pk_wide = |mb: &MemBatch, row: usize, _pk_val: u128| {
        partition_for_pk_bytes(mb.get_pk_bytes(row))
    };
    let route_group = |mb: &MemBatch, row: usize, _pk_val: u128| {
        if let Some(p) = &join_packer {
            let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
            p.pack_into(&mut buf, mb, row);
            partition_for_pk_bytes(&buf[..p.out_stride])
        } else {
            hash_row_for_partition(mb, row, col_indices, schema, mode)
        }
    };

    if !is_wide {
        // Narrow winner-comparator: a raw `u128` compare on the order-preserving
        // key (unsigned/signed/compound alike) — no pk_fast/slow_cmp branch.
        let select_narrow = move |order_cache: &[u128; 256],
                                  _cursors: &[u32; 256],
                                  active: &[u8; 256],
                                  n: usize|
              -> usize {
            let mut best_pos = 0;
            let mut best_key = order_cache[active[0] as usize];
            let mut best_si_u8 = active[0];
            #[allow(clippy::needless_range_loop)]
            for pos in 1..n {
                let si = active[pos];
                let key = order_cache[si as usize];
                // Source-index tiebreak: swap_remove scrambles active_sources,
                // so equal-key winners would otherwise be
                // eviction-history-dependent.
                if key < best_key || (key == best_key && si < best_si_u8) {
                    best_key = key;
                    best_pos = pos;
                    best_si_u8 = si;
                }
            }
            best_pos
        };

        if is_pk_routing {
            relay_walk_inner::<true, _, _>(
                mem_batches, &w_map, worker_rows, &mut pk_cache, &mut order_cache, schema,
                cursors, active_sources, num_active, select_narrow, route_pk_narrow,
            );
        } else {
            relay_walk_inner::<true, _, _>(
                mem_batches, &w_map, worker_rows, &mut pk_cache, &mut order_cache, schema,
                cursors, active_sources, num_active, select_narrow, route_group,
            );
        }
    } else {
        // Wide winner-comparator: read raw PK bytes per source on demand and
        // delegate to the column-aware compare_pk_bytes. The winner's bytes
        // are cached across the inner scan to avoid re-unwrap/re-slice on
        // every iteration.
        let select_wide = |_pk_cache: &[u128; 256],
                           cursors: &[u32; 256],
                           active: &[u8; 256],
                           n: usize|
              -> usize {
            let mut best_pos = 0;
            let mut best_si_u8 = active[0];
            let mut best_pk_bytes = mem_batches[best_si_u8 as usize]
                .as_ref()
                .unwrap()
                .get_pk_bytes(cursors[best_si_u8 as usize] as usize);
            #[allow(clippy::needless_range_loop)]
            for pos in 1..n {
                let si = active[pos];
                let si_idx = si as usize;
                let pk = mem_batches[si_idx]
                    .as_ref()
                    .unwrap()
                    .get_pk_bytes(cursors[si_idx] as usize);
                let ord = compare_pk_bytes(pk, best_pk_bytes);
                if ord == Ordering::Less || (ord == Ordering::Equal && si < best_si_u8) {
                    best_pos = pos;
                    best_si_u8 = si;
                    best_pk_bytes = pk;
                }
            }
            best_pos
        };

        if is_pk_routing {
            relay_walk_inner::<false, _, _>(
                mem_batches, &w_map, worker_rows, &mut pk_cache, &mut order_cache, schema,
                cursors, active_sources, num_active, select_wide, route_pk_wide,
            );
        } else {
            relay_walk_inner::<false, _, _>(
                mem_batches, &w_map, worker_rows, &mut pk_cache, &mut order_cache, schema,
                cursors, active_sources, num_active, select_wide, route_group,
            );
        }
    }
}

/// Scatter pre-consolidated batches across workers using a merge-walk.
/// All sources must satisfy the consolidated invariant (sorted, no duplicate PKs).
/// Output batches are sorted but not consolidated (duplicate PKs can appear across sources).
#[cfg(test)]
pub fn op_relay_scatter_consolidated(
    sources: &[Option<&ConsolidatedBatch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    op_relay_scatter_consolidated_mode(sources, col_indices, &[], schema, num_workers, RouteMode::GroupKey)
}

pub(crate) fn op_relay_scatter_consolidated_mode(
    sources: &[Option<&ConsolidatedBatch>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    mode: RouteMode,
) -> Vec<Batch> {
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
        relay_scatter_merge_walk(&mem_batches, col_indices, target_tcs, schema, num_workers, &mut worker_rows, mode);

        let total_rows: usize = worker_rows[..num_workers].iter().map(|v| v.len()).sum();
        (0..num_workers)
            .map(|w| {
                if worker_rows[w].is_empty() {
                    return Batch::empty_with_schema(schema);
                }
                let blob_cap = (total_blob * worker_rows[w].len() / total_rows).max(1);
                let mut b = write_to_batch(schema, worker_rows[w].len(), blob_cap, |writer| {
                    scatter_multi_source(&mem_batches, &worker_rows[w], writer);
                });
                b.sorted = true;
                if single_source {
                    b.consolidated = true;
                }
                b
            })
            .collect()
    })
}

/// Broadcast relay: deliver the FULL delta to every worker. The per-worker
/// source slices are disjoint (each is one worker's base-table-PK-partitioned
/// slice), so their concatenation is the full delta with no duplication; that
/// concatenation is then cloned to each of `num_workers` destinations. The
/// range-join probe needs the whole delta on every worker — a range match can
/// live on any worker's trace — which the equality scatter (one destination per
/// row) cannot deliver. Sibling of `op_repartition_batches_mode` /
/// `op_relay_scatter_consolidated_mode`, but without `col_indices` / `RouteMode`
/// (broadcast routes nothing). All destination batches are byte-identical.
pub(crate) fn op_relay_broadcast(
    sources: &[Option<&Batch>],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    let total: usize = sources.iter().flatten().map(|b| b.count).sum();
    if total == 0 {
        return (0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect();
    }
    // Concatenate the disjoint slices into the full delta once (append_batch
    // relocates each source's blob, so independent source blobs stay valid).
    let mut full = Batch::with_schema(*schema, total);
    for src in sources.iter().flatten() {
        if src.count > 0 {
            full.append_batch(src, 0, src.count);
        }
    }
    // Clone to every worker; the last destination takes ownership of `full`.
    let mut out: Vec<Batch> = (1..num_workers).map(|_| full.clone_batch()).collect();
    out.push(full);
    out
}

/// Scatter non-consolidated batches across workers by hashing each row.
#[cfg(test)]
pub fn op_relay_scatter(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    op_repartition_batches_mode(sources, col_indices, &[], schema, num_workers, RouteMode::GroupKey)
}

/// Scatter a single batch across workers using multiple independent column
/// specifications in one pass.  Returns one `Vec<Batch>` per spec.
///
/// Only used in tests; retained as `#[cfg(test)]` so the helper doesn't
/// bloat the production binary. Routes non-PK specs by `RouteMode::GroupKey`
/// only — it does NOT build a `ReindexPacker`, so it does not co-partition a
/// join-promoted compound key. A hand-assembled compound-join co-partition test
/// must drive `op_repartition_batches_mode` / `op_relay_scatter_consolidated_mode`
/// with `RouteMode::JoinPromote`, not this helper.
#[cfg(test)]
pub fn op_multi_scatter(
    batch: &Batch,
    col_specs: &[&[u32]],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<Batch>> {
    let n = batch.count;
    let n_specs = col_specs.len();
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);
    let needed = n_specs * num_workers;

    SCATTER_INDICES.with(|pool| {
        let mut flat_indices = pool.borrow_mut();
        if flat_indices.len() < needed {
            flat_indices.resize_with(needed, Vec::new);
        }
        for slot in flat_indices[..needed].iter_mut() { slot.clear(); }

        for si in 0..n_specs {
            let spec = col_specs[si];
            let is_pk_routing = spec == schema.pk_indices();
            if is_pk_routing {
                // Distribution-prefix route, exactly as `fill_worker_indices`.
                for i in 0..n {
                    let partition = schema.partition_for_pk(mb.get_pk_bytes(i));
                    flat_indices[si * num_workers + w_map[partition]].push(i as u32);
                }
            } else {
                for i in 0..n {
                    let partition = hash_row_for_partition(&mb, i, spec, schema, RouteMode::GroupKey);
                    flat_indices[si * num_workers + w_map[partition]].push(i as u32);
                }
            }
        }

        let mut results: Vec<Vec<Batch>> = (0..n_specs)
            .map(|_| (0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect())
            .collect();

        for si in 0..n_specs {
            for w in 0..num_workers {
                let indices = &flat_indices[si * num_workers + w];
                if !indices.is_empty() {
                    results[si][w] = Batch::from_indexed_rows(&mb, indices, schema);
                }
            }
        }

        // Flag propagation: PK-spec sub-batches inherit sorted/consolidated from source.
        for si in 0..n_specs {
            let spec = col_specs[si];
            if spec == schema.pk_indices() {
                if crate::storage::ConsolidatedBatch::from_batch_ref(batch).is_some() {
                    for batch in results[si].iter_mut().take(num_workers) {
                        if batch.count > 0 {
                            batch.sorted = true;
                            batch.consolidated = true;
                        }
                    }
                } else if batch.sorted {
                    for batch in results[si].iter_mut().take(num_workers) {
                        if batch.count > 0 {
                            batch.sorted = true;
                        }
                    }
                }
            }
        }

        results
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};

    #[test]
    #[should_panic(expected = "source index must fit in u8")]
    fn test_op_repartition_batches_rejects_over_256_sources() {
        // The source index is stored in a u8; > 256 sources would truncate it
        // silently in release builds. The guard must be a hard assert.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let sources: Vec<Option<&Batch>> = vec![None; 257];
        let _ = op_repartition_batches_mode(&sources, &[0], &[], &schema, 4, RouteMode::GroupKey);
    }

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u128_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u64_string() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        )
    }

    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_batch_str(schema: &SchemaDescriptor, rows: &[(u64, i64, &str)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, s) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());

            let bytes = s.as_bytes();
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                let copy_len = bytes.len().min(12);
                gs[4..4 + copy_len].copy_from_slice(&bytes[..copy_len]);
            } else {
                gs[4..8].copy_from_slice(&bytes[..4]);
                let offset = b.blob.len() as u64;
                gs[8..16].copy_from_slice(&offset.to_le_bytes());
                b.blob.extend_from_slice(bytes);
            }
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn total_rows(batches: &[Batch]) -> usize {
        batches.iter().map(|b| b.count).sum()
    }

    /// A BLOB payload column shares STRING's 16-byte German-string header
    /// (col_size = 16). `extract_col_key` must hash it like a STRING rather
    /// than falling into the generic 8-byte-buffer branch (which would panic).
    #[test]
    fn extract_col_key_handles_blob_column() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::BLOB, 0),
            ],
            &[0],
        );
        // make_batch_str builds the same German-string encoding for any
        // 16-byte-struct column; long value (> threshold) exercises the blob spill.
        let cb = make_batch_str(&schema, &[
            (1, 1, "short"),
            (2, 1, "a long blob value well beyond the inline threshold"),
            (3, 1, ""),
        ]);
        let b = cb.into_inner();
        let mb = b.as_mem_batch();
        // Must not panic and must distinguish the two non-empty values.
        let k_short = extract_col_key(&mb, 0, 1, &schema);
        let k_long  = extract_col_key(&mb, 1, 1, &schema);
        let k_empty = extract_col_key(&mb, 2, 1, &schema);
        assert_ne!(k_short, 0);
        assert_ne!(k_long, 0);
        assert_eq!(k_empty, 0, "empty blob hashes to 0");
        assert_ne!(k_short, k_long);
    }

    fn make_wide_schema() -> SchemaDescriptor {
        // 3×U64 compound PK → pk_stride = 24 (wide).
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        )
    }

    fn wide_pk(c0: u64, c1: u64, c2: u64) -> [u8; 24] {
        let mut pk = [0u8; 24];
        pk[..8].copy_from_slice(&c0.to_le_bytes());
        pk[8..16].copy_from_slice(&c1.to_le_bytes());
        pk[16..24].copy_from_slice(&c2.to_le_bytes());
        pk
    }

    /// Build a pre-consolidated wide-PK batch. `rows` must already be
    /// sorted ascending by (c0,c1,c2) with unique PKs.
    fn make_wide_batch(schema: &SchemaDescriptor, rows: &[([u8; 24], i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk_bytes(&pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_relay_scatter_wide_pk_order_and_routing() {
        let schema = make_wide_schema();
        let num_workers = 4;
        // 3 sources, interleaved, with a low-16 collision: (1,1,*) appears
        // in all three with differing c2 — prefix-equal, distinguished only
        // by compare_pk_bytes' third column walk.
        let b0 = make_wide_batch(&schema, &[
            (wide_pk(0, 0, 0), 1, 10),
            (wide_pk(1, 1, 0), 1, 11),
            (wide_pk(5, 9, 2), 1, 12),
        ]);
        let b1 = make_wide_batch(&schema, &[
            (wide_pk(1, 1, 1), 1, 20),
            (wide_pk(2, 2, 2), 1, 21),
            (wide_pk(9, 9, 9), 1, 22),
        ]);
        let b2 = make_wide_batch(&schema, &[
            (wide_pk(1, 1, 2), 1, 30),
            (wide_pk(3, 3, 3), 1, 31),
            (wide_pk(7, 0, 0), 1, 32),
        ]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1), Some(&b2)];
        let result = op_relay_scatter_consolidated(&sources, schema.pk_indices(), &schema, num_workers);

        assert_eq!(total_rows(&result), 9);
        for (w, sb) in result.iter().enumerate() {
            // (a) Non-decreasing under the column-aware comparator.
            for r in 1..sb.count {
                let prev = sb.get_pk_bytes(r - 1);
                let cur = sb.get_pk_bytes(r);
                assert_ne!(
                    compare_pk_bytes(prev, cur),
                    std::cmp::Ordering::Greater,
                    "worker {w} row {r} out of order",
                );
            }
            // (b) Every row routed to partition_for_pk_bytes's worker.
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "wide PK routed to wrong worker");
            }
        }
    }

    /// Compound (U64, U64) PK schema: pk_stride = 16 (narrow), pk_count = 2.
    /// Forces the narrow-slow closure set: a packed-u128 `<` would put the
    /// trailing column in the high bits and reverse column priority.
    fn make_narrow_compound_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// Pack (c0, c1) into a u128 the way `MemBatch::get_pk` would read a
    /// 16-byte PK region: low 8 bytes = c0, high 8 bytes = c1.
    fn mk_compound_pk(c0: u64, c1: u64) -> u128 {
        ((c1 as u128) << 64) | (c0 as u128)
    }

    fn make_narrow_compound_batch(
        schema: &SchemaDescriptor,
        rows: &[(u128, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            // `mk_compound_pk` packs c0 in the low 8 bytes and c1 in the high 8.
            // The compound PK at rest is OPK = col0_BE ++ col1_BE, so encode the
            // two native column values through extend_pk_opk rather than writing
            // the raw u128 (which would byte-reverse the column order).
            let c0 = pk as u64 as u128;
            let c1 = (pk >> 64) as u64 as u128;
            b.extend_pk_opk(schema, &[c0, c1]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_relay_scatter_narrow_compound_pk_order_and_routing() {
        // (U64, U64) compound PK: low-8-byte collisions force the column-aware
        // comparator. (1, 10) packed = (10<<64)|1, (2, 0) packed = 2; raw u128
        // `<` would order (2, 0) before (1, 10), but lexicographic order says
        // (1, 10) < (2, 0). Likewise (1, 5) < (1, 10) < (1, 15) lexicographically.
        let schema = make_narrow_compound_schema();
        assert!(!schema.pk_is_wide(), "pk_stride must be 16 (narrow)");
        assert!(schema.pk_indices().len() > 1, "compound PK must take slow path");
        let num_workers = 4;
        let b0 = make_narrow_compound_batch(&schema, &[
            (mk_compound_pk(1, 10), 1, 11),
            (mk_compound_pk(2, 0),  1, 12),
            (mk_compound_pk(5, 9),  1, 13),
        ]);
        let b1 = make_narrow_compound_batch(&schema, &[
            (mk_compound_pk(1, 5),  1, 21),
            (mk_compound_pk(1, 15), 1, 22),
            (mk_compound_pk(3, 3),  1, 23),
        ]);
        let b2 = make_narrow_compound_batch(&schema, &[
            (mk_compound_pk(1, 7),  1, 31),
            (mk_compound_pk(2, 2),  1, 32),
            (mk_compound_pk(7, 0),  1, 33),
        ]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1), Some(&b2)];
        // col_indices = [0, 1] is the full PK set: exercises the compound-PK-set
        // is_pk_routing path (route_pk_narrow via partition_for_key on cached u128).
        let result = op_relay_scatter_consolidated(&sources, &[0u32, 1u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 9);
        for (w, sb) in result.iter().enumerate() {
            // (a) Non-decreasing under the column-aware comparator.
            for r in 1..sb.count {
                let prev = sb.get_pk_bytes(r - 1);
                let cur = sb.get_pk_bytes(r);
                assert_ne!(
                    compare_pk_bytes(prev, cur),
                    Ordering::Greater,
                    "worker {w} row {r} out of order (compound PK)",
                );
            }
            // (b) Routing matches partition_for_pk_bytes (the canonical PK route
            // for narrow PKs via fill_worker_indices' is_compound_pk path).
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "compound PK routed to wrong worker");
            }
        }
    }

    fn make_schema_i64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i64_batch(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            // Signed PK at rest is OPK (big-endian, sign-bit flipped), so
            // memcmp == signed order. Encode through extend_pk_opk, not the
            // raw right-aligned `extend_pk` (which would store native bytes).
            b.extend_pk_opk(schema, &[(pk as u64) as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_relay_scatter_i64_pk_signed_ordering() {
        // Native I64 PK with negative values. Each output batch must be
        // sorted under canonical signed order — raw u128 ordering would put
        // -1 (0xFFFF...) after +1 and break the `sorted = true` invariant set
        // by op_relay_scatter_consolidated.
        let schema = make_schema_i64_i64();
        let num_workers = 4;
        // Each source sorted ascending under signed I64 order.
        let b0 = make_i64_batch(&schema, &[
            (-100, 1, 10),
            (-1,   1, 11),
            (5,    1, 12),
        ]);
        let b1 = make_i64_batch(&schema, &[
            (-50,  1, 20),
            (0,    1, 21),
            (100,  1, 22),
        ]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 6);
        for (w, sb) in result.iter().enumerate() {
            assert!(sb.sorted, "worker {w} output not marked sorted");
            for r in 1..sb.count {
                let prev = sb.get_pk(r - 1);
                let cur  = sb.get_pk(r);
                assert_ne!(
                    compare_pk_bytes(sb.get_pk_bytes(r - 1), sb.get_pk_bytes(r)),
                    Ordering::Greater,
                    "worker {w} row {r}: signed pks out of order (prev={prev:#x} cur={cur:#x})",
                );
            }
        }
    }

    #[test]
    fn test_partition_filter_partitions_rows_by_owner() {
        let schema = make_schema_u64_i64();
        let num_workers = 4u32;
        let rows: Vec<(u64, i64, i64)> = (0..40u64).map(|i| (i * 7 + 1, 1, i as i64)).collect();
        let batch = make_batch(&schema, &rows).into_inner();

        // Each row kept by exactly the worker that owns its PK partition; the
        // union across workers is the whole batch with no duplication.
        let mut total_kept = 0usize;
        for wid in 0..num_workers {
            let out = op_partition_filter(&batch, &schema, wid, num_workers);
            total_kept += out.count;
            for r in 0..out.count {
                let pk = out.get_pk_bytes(r);
                let owner = worker_for_partition(partition_for_pk_bytes(pk), num_workers as usize);
                assert_eq!(owner as u32, wid, "row routed to wrong worker");
            }
        }
        assert_eq!(total_kept, batch.count, "partition filter dropped or duplicated rows");
    }

    #[test]
    fn test_partition_filter_single_worker_keeps_all() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).into_inner();
        let out = op_partition_filter(&batch, &schema, 0, 1);
        assert_eq!(out.count, 3, "(0, 1) must keep every row");
    }

    #[test]
    fn test_partition_filter_empty_in_empty_out() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[]).into_inner();
        let out = op_partition_filter(&batch, &schema, 1, 4);
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_relay_scatter_wide_pk_single_source_bulk_drain() {
        let schema = make_wide_schema();
        let num_workers = 4;
        // Exactly one active source → the num_active == 1 bulk-drain path.
        let b0 = make_wide_batch(&schema, &[
            (wide_pk(1, 2, 3), 1, 1),
            (wide_pk(4, 5, 6), 1, 2),
            (wide_pk(7, 8, 9), 1, 3),
            (wide_pk(10, 11, 12), 1, 4),
        ]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0)];
        let result = op_relay_scatter_consolidated(&sources, schema.pk_indices(), &schema, num_workers);

        assert_eq!(total_rows(&result), 4);
        for (w, sb) in result.iter().enumerate() {
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "bulk-drain wide PK routed to wrong worker");
            }
        }
    }

    #[test]
    fn test_repartition_batch_pk_routing() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let pk_vals: &[u64] = &[1, 7, 42, 100, 255, 1024, 65537, 999983];

        let mut b = Batch::with_schema(schema, pk_vals.len());

        for &pk in pk_vals {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), pk_vals.len());

        for &pk in pk_vals {
            let expected = worker_for_partition(
                partition_for_key(pk as u128),
                num_workers,
            );
            let found = (0..sub_batches[expected].count)
                .any(|r| (sub_batches[expected].get_pk(r) as u64) == pk);
            assert!(found, "pk={pk} not found in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_u128_pk() {
        let schema = make_schema_u128_i64();
        let num_workers = 4;
        let pks: &[u128] = &[
            1u128 << 64,
            (0xCAFE_BABEu128 << 64) | 0xDEAD_BEEF,
            u128::MAX,
            (7u128 << 64) | 42,
        ];

        let n = pks.len();
        let mut b = Batch::with_schema(schema, n);

        for &pk in pks {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), n);

        for &pk in pks {
            let expected = worker_for_partition(
                partition_for_key(pk),
                num_workers,
            );
            let found = (0..sub_batches[expected].count).any(|r| {
                sub_batches[expected].get_pk(r) == pk
            });
            assert!(found, "pk={pk} not in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_group_col() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let same_val: i64 = 42;

        let mut b = Batch::with_schema(schema, 4);

        for pk in [1u64, 2, 3, 4] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &same_val.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), 4);
        let non_empty = sub_batches.iter().filter(|sb| sb.count > 0).count();
        assert_eq!(non_empty, 1, "all rows with same group key must go to one worker");
    }

    #[test]
    fn test_repartition_batch_string_col() {
        let schema = make_schema_u64_string();
        let num_workers = 4;

        // Short string "hello" (≤ 12 bytes): two rows must go to same worker
        let b = make_batch_str(&schema, &[(1, 1, "hello"), (2, 1, "hello"), (3, 1, "world")]);
        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), 3);

        let mut worker_of_1 = None;
        for (w, batch) in sub_batches.iter().enumerate().take(num_workers) {
            for r in 0..batch.count {
                if (batch.get_pk(r) as u64) == 1 {
                    worker_of_1 = Some(w);
                }
            }
        }
        let w1 = worker_of_1.expect("pk=1 must be in some worker");
        let pk2_same = (0..sub_batches[w1].count).any(|r| (sub_batches[w1].get_pk(r) as u64) == 2);
        assert!(pk2_same, "same short string 'hello' must route to same worker");

        // Long string (> 12 bytes): two rows must go to same worker
        let long_str = "this is a longer string for heap";
        let b2 = make_batch_str(&schema, &[(10, 1, long_str), (11, 1, long_str)]);
        let sub2 = op_repartition_batch(&b2, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub2), 2);

        let mut worker_of_10 = None;
        for (w, batch) in sub2.iter().enumerate().take(num_workers) {
            for r in 0..batch.count {
                if (batch.get_pk(r) as u64) == 10 {
                    worker_of_10 = Some(w);
                }
            }
        }
        let w10 = worker_of_10.expect("pk=10 must be in some worker");
        let pk11_same = (0..sub2[w10].count).any(|r| (sub2[w10].get_pk(r) as u64) == 11);
        assert!(pk11_same, "same long string must route to same worker");
    }

    #[test]
    fn test_repartition_batch_pk_routing_propagates_flags() {
        let schema = make_schema_u64_i64(); // PK = col 0 (U64), payload = col 1 (I64)
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        assert!(b.sorted && b.consolidated);

        // PK routing (col 0 == pk_indices()): single source ⇒ flags propagate.
        let pk_routed = op_repartition_batch(&b, &[0u32], &schema, 4);
        for sb in pk_routed.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "PK-routed sub-batch must inherit sorted");
            assert!(sb.consolidated, "PK-routed sub-batch must inherit consolidated");
        }

        // Non-PK routing (col 1): hash distribution destroys PK order ⇒ no flags.
        let hash_routed = op_repartition_batch(&b, &[1u32], &schema, 4);
        for sb in hash_routed.iter().filter(|s| s.count > 0) {
            assert!(!sb.sorted, "non-PK-routed sub-batch must not claim sorted");
            assert!(!sb.consolidated, "non-PK-routed sub-batch must not claim consolidated");
        }
    }

    #[test]
    fn test_repartition_batches_pk_routing_single_vs_multi_source() {
        let schema = make_schema_u64_i64();
        // Single contributing source, PK routing: propagate both flags.
        let b0 = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let single: Vec<Option<&Batch>> = vec![Some(&b0)];
        let out = op_repartition_batches_mode(&single, &[0u32], &[], &schema, 4, RouteMode::GroupKey);
        for sb in out.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "single-source PK-routed must inherit sorted");
            assert!(sb.consolidated, "single-source PK-routed must inherit consolidated");
        }

        // Two contributing sources, PK routing: per-source-concatenated output is
        // not globally sorted, so nothing propagates.
        let b1 = make_batch(&schema, &[(4, 1, 40), (5, 1, 50), (6, 1, 60)]);
        let multi: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
        let out = op_repartition_batches_mode(&multi, &[0u32], &[], &schema, 4, RouteMode::GroupKey);
        for sb in out.iter().filter(|s| s.count > 0) {
            assert!(!sb.sorted, "multi-source scatter must not claim sorted");
            assert!(!sb.consolidated, "multi-source scatter must not claim consolidated");
        }
    }

    #[test]
    fn test_repartition_batch_sorted_not_consolidated_pk_routing() {
        let schema = make_schema_u64_i64();
        let mut b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).into_inner();
        // Sorted but not consolidated source: only `sorted` may propagate.
        b.consolidated = false;
        assert!(b.sorted && !b.consolidated);

        let out = op_repartition_batch(&b, &[0u32], &schema, 4);
        for sb in out.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "PK-routed sub-batch inherits sorted");
            assert!(!sb.consolidated, "must not invent consolidated");
        }
    }

    #[test]
    fn test_relay_scatter_consolidated_path() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b0 = make_batch(&schema, &[(1, 1, 10), (5, 1, 50), (9, 1, 90)]);
        let b1 = make_batch(&schema, &[(2, 1, 20), (6, 1, 60), (10, 1, 100)]);

        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 6);
        for sb in &result {
            if sb.count > 0 {
                assert!(!sb.consolidated, "merged path uses PK-only comparison, must not claim consolidated");
                assert!(sb.sorted, "merged path output must be sorted");
            }
        }

        // Single contributing source: no cross-source duplicate PK possible, so
        // the output is consolidated as well as sorted.
        let single: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0)];
        let result = op_relay_scatter_consolidated(&single, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&result), 3);
        for sb in result.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "single-source merge output must be sorted");
            assert!(sb.consolidated, "single-source merge output must be consolidated");
        }
    }

    #[test]
    fn test_relay_scatter_fallback_path() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let b1 = make_batch(&schema, &[(2, 1, 20)]);

        let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 2);
        for sb in &result {
            if sb.count > 0 {
                assert!(!sb.consolidated, "non-consolidated path output must not be consolidated");
            }
        }
    }

    #[test]
    fn test_multi_scatter_pk_flag_propagation() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        assert!(b.sorted && b.consolidated);

        let specs: Vec<&[u32]> = vec![&[0u32], &[1u32]];
        let result = op_multi_scatter(&b, &specs, &schema, num_workers);

        // PK spec (specs[0]): sub-batches must inherit sorted + consolidated
        for sb in &result[0] {
            if sb.count > 0 {
                assert!(sb.sorted, "PK spec sub-batch must inherit sorted");
                assert!(sb.consolidated, "PK spec sub-batch must inherit consolidated");
            }
        }
        // Non-PK spec (specs[1]): must NOT inherit
        for sb in &result[1] {
            if sb.count > 0 {
                assert!(!sb.sorted, "non-PK spec sub-batch must not inherit sorted");
                assert!(!sb.consolidated, "non-PK spec sub-batch must not inherit consolidated");
            }
        }
    }

    #[test]
    fn test_repartition_row_count() {
        let schema = make_schema_u64_i64();
        let rows: Vec<(u64, i64, i64)> = (1u64..=100).map(|i| (i, 1, i as i64 * 10)).collect();
        let b = make_batch(&schema, &rows);

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, 4);
        assert_eq!(total_rows(&sub_batches), 100, "total rows must equal input count");
    }

    #[test]
    fn test_repartition_routing_contract() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let vals: Vec<i64> = (0..64i64).map(|i| i * 997 + 1).collect();

        let mut b = Batch::with_schema(schema, vals.len());

        for (i, &v) in vals.iter().enumerate() {
            b.extend_pk((i + 1) as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), vals.len());

        for &v in &vals {
            // Routing by a payload column uses the canonical route key (signed
            // columns are sign-flipped via payload_route_key) so a payload FK
            // routes identically to the same value stored as a PK column.
            let route_key = crate::schema::payload_route_key(&v.to_le_bytes(), 0, 8, type_code::I64);
            let expected_partition = partition_for_key(route_key);
            let expected_worker = worker_for_partition(expected_partition, num_workers);
            let found = (0..sub_batches[expected_worker].count).any(|r| {
                i64::from_le_bytes(
                    sub_batches[expected_worker].col_data(0)[r * 8..r * 8 + 8]
                        .try_into()
                        .unwrap(),
                ) == v
            });
            assert!(found, "val={v} should be in worker {expected_worker}");
        }
    }

    #[test]
    fn partition_router_basic() {
        let schema = make_schema_u64_i64();
        let mut router = PartitionRouter::new();

        let b = make_batch(&schema, &[(42, 1, 0)]);
        router.record_routing(&b, &schema, 1, 0, 3);
        assert_eq!(router.worker_for_index_key(1, 0, 42), 3);
        assert_eq!(router.worker_for_index_key(1, 0, 99), -1);

        // Retract: negative weight removes the entry
        let b2 = make_batch(&schema, &[(42, -1, 0)]);
        router.record_routing(&b2, &schema, 1, 0, 3);
        assert_eq!(router.worker_for_index_key(1, 0, 42), -1);
    }

    #[test]
    fn test_partition_router_string_col() {
        let schema = make_schema_u64_string();
        let mut router = PartitionRouter::new();

        // Short string
        let b = make_batch_str(&schema, &[(1, 1, "hello")]);
        router.record_routing(&b, &schema, 1, 1, 2);
        // Must not panic — the old code would overflow copying 16 bytes into 8-byte buffer.
        // The key_lo is a hash, so we just check it was stored and is retrievable.
        // record_routing uses col_idx=1 (STRING column), so look up the hashed key.
        let mb = b.as_mem_batch();
        let struct_bytes = mb.get_col_ptr(0, 0, 16); // payload_idx(1, 0) = 0
        let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
        let hashed_key = crate::xxh::checksum(&struct_bytes[4..4 + length]);
        assert_eq!(router.worker_for_index_key(1, 1, hashed_key as u128), 2);

        // Long string (> 12 bytes)
        let b2 = make_batch_str(&schema, &[(2, 1, "a_long_string_value")]);
        router.record_routing(&b2, &schema, 1, 1, 3);
        // Must not panic
    }

    #[test]
    fn test_repartition_merged_duplicate_rows() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        // Two sources with identical (PK=1, val=10) rows — merged output must NOT
        // claim consolidated because it uses PK-only comparison.
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let b1 = make_batch(&schema, &[(1, 1, 10)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 2, "both rows must appear in output");
        for sb in &result {
            if sb.count > 0 {
                assert!(!sb.consolidated, "merged path must not claim consolidated");
            }
        }
    }

    #[test]
    fn test_distinct_update_same_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: (PK=1, val=100, w=+1) — a row inserted in a previous tick.
        let trace_batch = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut cursor_handle = CursorHandle::from_owned(&[trace_batch], schema);

        // Delta: UPDATE PK=1 sets val=100 → 200.
        // _enforce_unique_pk emits (PK=1, val=100, w=-1) and (PK=1, val=200, w=+1).
        // Both rows have the same PK but different payloads; sorted by payload ascending.
        let delta = make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]);

        let (out, _consolidated) =
            crate::ops::op_distinct(delta.into_inner(), cursor_handle.cursor_mut(), &schema);

        assert_eq!(
            out.count,
            2,
            "expected 2 output rows after same-PK update, got {}",
            out.count
        );

        assert_eq!((out.get_pk(0) as u64), 1);
        let val0 = i64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(val0, 100);
        assert_eq!(out.get_weight(0), -1);

        assert_eq!((out.get_pk(1) as u64), 1);
        let val1 = i64::from_le_bytes(out.col_data(0)[8..16].try_into().unwrap());
        assert_eq!(val1, 200);
        assert_eq!(out.get_weight(1), 1);
    }

    #[test]
    fn test_partition_routing_invariance_narrow_pk() {
        use crate::xxh::hash_u128;
        let num_workers = 4usize;
        let pks: Vec<u64> = vec![
            1,
            42,
            100,
            1000,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX / 2,
        ];
        for &pk in &pks {
            let pk_u128 = pk as u128;
            let partition = (hash_u128(pk_u128) % num_workers as u64) as usize;
            assert!(
                partition < num_workers,
                "partition {partition} out of range for pk={pk}"
            );
        }
    }

    #[test]
    fn test_relay_scatter_consolidated_output_order() {
        let schema = make_schema_u64_i64();
        // Source 0: PKs 1, 3, 5 (odd); source 1: PKs 2, 4, 6 (even).
        // After merge-walk each worker's batch must be PK-sorted.
        let b0 = make_batch(&schema, &[(1, 1, 0), (3, 1, 0), (5, 1, 0)]);
        let b1 = make_batch(&schema, &[(2, 1, 0), (4, 1, 0), (6, 1, 0)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, 4);
        for sb in &result {
            for r in 1..sb.count {
                assert!(
                    sb.get_pk(r) >= sb.get_pk(r - 1),
                    "output must be PK-sorted (non-decreasing) within each worker"
                );
            }
        }
    }

    #[test]
    fn test_relay_scatter_consolidated_tie_breaking() {
        // Both sources emit PK=1. Source 0's row must appear before source 1's row
        // in every worker's output (ascending si tie-break).
        let schema = make_schema_u64_i64();
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let b1 = make_batch(&schema, &[(1, 1, 20)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, 4);
        for sb in &result {
            if sb.count >= 2 {
                assert_eq!(sb.get_pk(0), sb.get_pk(1));
                let val0 = i64::from_le_bytes(sb.col_data(0)[0..8].try_into().unwrap());
                let val1 = i64::from_le_bytes(sb.col_data(0)[8..16].try_into().unwrap());
                assert_eq!(val0, 10, "source 0 row must come first");
                assert_eq!(val1, 20, "source 1 row must come second");
            }
        }
    }

    #[test]
    fn test_op_repartition_batches_compound_pk_routes_by_bytes() {
        // op_repartition_batches_mode must route compound-PK rows by raw PK
        // bytes (partition_for_pk_bytes), matching PartitionedTable::
        // local_index. The pre-fix code only branched on (single_pk &&
        // wide), falling through to hash_row_for_partition for narrow
        // compound PKs — which would route to a different worker than
        // the data lives on.
        let schema = make_narrow_compound_schema();
        let num_workers = 4;
        let b0 = make_narrow_compound_batch(&schema, &[
            (mk_compound_pk(1, 10), 1, 11),
            (mk_compound_pk(2, 0),  1, 12),
            (mk_compound_pk(5, 9),  1, 13),
        ]);
        let b1 = make_narrow_compound_batch(&schema, &[
            (mk_compound_pk(1, 5),  1, 21),
            (mk_compound_pk(3, 3),  1, 23),
            (mk_compound_pk(7, 0),  1, 33),
        ]);
        let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
        // col_indices = [0, 1] = the full PK set → compound-PK routing.
        let sub_batches = op_repartition_batches_mode(&sources, &[0u32, 1u32], &[], &schema, num_workers, RouteMode::GroupKey);
        assert_eq!(total_rows(&sub_batches), 6);
        for (w, sb) in sub_batches.iter().enumerate() {
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "compound-PK row routed to wrong worker");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Compound JoinPromote scatter co-partition (both production functions)
    // -----------------------------------------------------------------------

    /// Schema with a compound, non-PK join key spanning a signed I64 and a U128
    /// column (packed stride 24 > 16). The PK is a separate U64 so the join key
    /// is NOT the PK and routing goes through the `route_group` / packer arm.
    fn make_join_key_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),  // col0: PK
                SchemaColumn::new(type_code::I64, 0),  // col1: signed join key part
                SchemaColumn::new(type_code::U128, 0), // col2: wide join key part
            ],
            &[0],
        )
    }

    fn make_join_key_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u128)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, c1, c2) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c1.to_le_bytes()); // I64 payload (pi 0)
            b.extend_col(1, &c2.to_le_bytes()); // U128 payload (pi 1)
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_compound_join_promote_scatter_copartitions_both_functions() {
        // A compound JoinPromote scatter must route every row by the SAME packed
        // OPK bytes the reindex Map writes as `_join_pk` — i.e. by
        // `partition_for_pk_bytes(ReindexPacker::pack(cols, row))`, NOT by
        // `extract_group_key`. Exercised through BOTH production scatter
        // functions: the non-consolidated row loop in `op_repartition_batches_mode`
        // and the consolidated merge-walk's `route_group` in
        // `op_relay_scatter_consolidated_mode`. Covers signed-negative and
        // >16-byte composite keys, including a duplicate join key that must
        // co-locate (the no-dropped-rows / matching-keys-together guarantee).
        let schema = make_join_key_schema();
        let cols = [1u32, 2u32]; // (I64, U128) join key — non-PK, stride 24.
        let num_workers = 4;
        // PK ascending (consolidated invariant). Rows 0 and 3 share the join key
        // (-5, 100) → must land on the same worker.
        let rows: &[(u64, i64, u128)] = &[
            (1, -5,        100),
            (2,  7,        100),
            (3, -5,        100),
            (4,  i64::MIN, 0),
            (5,  3,        0xdead_beef_cafe_0001),
            (6, -1,        1),
        ];
        let cb = make_join_key_batch(&schema, rows);

        let packer = crate::ops::linear::ReindexPacker::new(&schema, &cols, &[]);
        let expected_worker = |sb: &Batch, r: usize| -> usize {
            let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
            packer.pack_into(&mut buf[..packer.out_stride], &sb.as_mem_batch(), r);
            worker_for_partition(partition_for_pk_bytes(&buf[..packer.out_stride]), num_workers)
        };

        // Helper: assert every output row routed to the worker its packed key
        // dictates, no rows dropped, and the shared-key rows co-located.
        let check = |result: &[Batch], label: &str| {
            assert_eq!(total_rows(result), rows.len(), "{label}: no dropped rows");
            let mut worker_of_pk = std::collections::HashMap::new();
            for (w, sb) in result.iter().enumerate() {
                for r in 0..sb.count {
                    assert_eq!(expected_worker(sb, r), w, "{label}: row routed to wrong worker");
                    worker_of_pk.insert(sb.get_pk(r) as u64, w);
                }
            }
            // Rows with pk=1 and pk=3 share the join key (-5, 100): same worker.
            assert_eq!(
                worker_of_pk.get(&1), worker_of_pk.get(&3),
                "{label}: matching join keys must co-locate",
            );
        };

        // (a) Non-consolidated path.
        let repart = op_repartition_batches_mode(
            &[Some(&cb)], &cols, &[], &schema, num_workers, RouteMode::JoinPromote);
        check(&repart, "op_repartition_batches_mode");

        // (b) Consolidated merge-walk path (route_group).
        let consol = op_relay_scatter_consolidated_mode(
            &[Some(&cb)], &cols, &[], &schema, num_workers, RouteMode::JoinPromote);
        check(&consol, "op_relay_scatter_consolidated_mode");
    }

    /// A SINGLE promoted join key (arity 1, carried `T != 0`) must also route
    /// through the `ReindexPacker` — the packer fires on promotion, not just on a
    /// compound key. Covers both a payload key (`Narrow` I32 → I64, including a
    /// negative value so sign-extension is exercised) and a key that is the source
    /// PK (`Pk` arm), where the native-PK fast-path must be GATED OFF so the row
    /// routes by its `T`-wide `_join_pk`, not its narrow PK bytes.
    #[test]
    fn test_single_key_promote_scatter_copartitions() {
        let num_workers = 4;

        // ---- (1) Payload key: [U64 PK, I32 payload], reindex col1 → I64. ----
        let schema = SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(type_code::I32, 0)],
            &[0],
        );
        // PK ascending; rows pk=1 and pk=4 share key -5 → must co-locate.
        let rows: &[(u64, i32)] = &[(1, -5), (2, 7), (3, i32::MIN), (4, -5), (5, 0), (6, -1)];
        let mut b = Batch::with_schema(schema, rows.len());
        for &(pk, key) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &key.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true; b.consolidated = true;
        let cb = ConsolidatedBatch::new_unchecked(b);
        let cols = [1u32];
        let targets = [type_code::I64];

        let packer = crate::ops::linear::ReindexPacker::new(&schema, &cols, &targets);
        let expected_worker = |sb: &Batch, r: usize| -> usize {
            let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
            packer.pack_into(&mut buf[..packer.out_stride], &sb.as_mem_batch(), r);
            worker_for_partition(partition_for_pk_bytes(&buf[..packer.out_stride]), num_workers)
        };
        let check = |result: &[Batch], label: &str| {
            assert_eq!(total_rows(result), rows.len(), "{label}: no dropped rows");
            let mut worker_of_pk = std::collections::HashMap::new();
            for (w, sb) in result.iter().enumerate() {
                for r in 0..sb.count {
                    assert_eq!(expected_worker(sb, r), w,
                        "{label}: promoted single key routed to wrong worker");
                    worker_of_pk.insert(sb.get_pk(r) as u64, w);
                }
            }
            assert_eq!(worker_of_pk.get(&1), worker_of_pk.get(&4),
                "{label}: equal promoted keys must co-locate");
        };
        let repart = op_repartition_batches_mode(
            &[Some(&cb)], &cols, &targets, &schema, num_workers, RouteMode::JoinPromote);
        check(&repart, "payload-key op_repartition_batches_mode");
        let consol = op_relay_scatter_consolidated_mode(
            &[Some(&cb)], &cols, &targets, &schema, num_workers, RouteMode::JoinPromote);
        check(&consol, "payload-key op_relay_scatter_consolidated_mode");

        // ---- (2) PK key: [I32 PK, U64 payload], reindex col0 → I64. The native
        // PK fast-path must be gated off so routing uses the packed I64 key. ----
        let pk_schema = SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::I32, 0), SchemaColumn::new(type_code::U64, 0)],
            &[0],
        );
        let pk_rows: &[(i32, u64)] = &[(-5, 9), (-1, 9), (3, 9), (i32::MIN, 9)];
        let mut pb = Batch::with_schema(pk_schema, pk_rows.len());
        for &(pk, v) in pk_rows {
            let mut opk = [0u8; 4];
            gnitz_wire::encode_pk_column(&pk.to_le_bytes(), type_code::I32, &mut opk);
            pb.extend_pk_bytes(&opk);
            pb.extend_weight(&1i64.to_le_bytes());
            pb.extend_null_bmp(&0u64.to_le_bytes());
            pb.extend_col(0, &v.to_le_bytes());
            pb.count += 1;
        }
        pb.sorted = true; pb.consolidated = true;
        let pk_cb = ConsolidatedBatch::new_unchecked(pb);
        let pk_cols = [0u32];
        let pk_targets = [type_code::I64];
        let pk_packer = crate::ops::linear::ReindexPacker::new(&pk_schema, &pk_cols, &pk_targets);
        let pk_repart = op_repartition_batches_mode(
            &[Some(&pk_cb)], &pk_cols, &pk_targets, &pk_schema, num_workers, RouteMode::JoinPromote);
        assert_eq!(total_rows(&pk_repart), pk_rows.len(), "PK-key: no dropped rows");
        for (w, sb) in pk_repart.iter().enumerate() {
            for r in 0..sb.count {
                let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
                pk_packer.pack_into(&mut buf[..pk_packer.out_stride], &sb.as_mem_batch(), r);
                assert_eq!(
                    worker_for_partition(partition_for_pk_bytes(&buf[..pk_packer.out_stride]), num_workers),
                    w, "PK-key fast-path must be gated: routed by native PK not packed T");
            }
        }
    }
}
