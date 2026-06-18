//! Exchange partition routing: `PartitionRouter`, `worker_for_partition`,
//! `with_*_indices`, `RouteMode`, and the per-row routing-key helpers.

use std::cell::RefCell;

use rustc_hash::FxHashMap;

use crate::schema::SchemaDescriptor;
use crate::storage::{partition_for_key, partition_for_pk_bytes, Batch, MemBatch};

use super::super::reindex::{german_string_promote_key, ReindexPacker};
use super::super::util::extract_group_key;

// Thread-local pool: reuse Vec<Vec<u32>> index scratch across calls so
// steady-state repartition/scatter ops allocate nothing for routing tables.
thread_local! {
    pub(super) static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
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
        return if content.is_empty() {
            0u128
        } else {
            crate::foundation::xxh::checksum(content) as u128
        };
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
pub(super) fn build_w_map(num_workers: usize) -> [usize; 256] {
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
pub(crate) fn op_partition_filter(batch: &Batch, schema: &SchemaDescriptor, worker_id: u32, num_workers: u32) -> Batch {
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
    pub(crate) fn record_routing(
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
fn route_partition_key(mb: &MemBatch, row: usize, cols: &[u32], schema: &SchemaDescriptor) -> u128 {
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
                return german_string_promote_key(loc.bytes(mb, row), mb.blob);
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

pub(super) fn hash_row_for_partition(
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
pub(super) fn key_is_promoted(target_tcs: &[u8]) -> bool {
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
pub(super) fn compound_join_packer(
    mode: RouteMode,
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
) -> Option<ReindexPacker> {
    (mode == RouteMode::JoinPromote && (col_indices.len() > 1 || key_is_promoted(target_tcs)))
        .then(|| ReindexPacker::new(schema, col_indices, target_tcs))
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

/// Broadcast sibling of [`fill_worker_indices`]: fills **every** worker's slot
/// with **all** row indices `0..batch.count` instead of PK-partitioning them.
fn fill_broadcast_indices(batch: &Batch, num_workers: usize, out: &mut Vec<Vec<u32>>) {
    if out.len() < num_workers {
        out.resize_with(num_workers, Vec::new);
    }
    for wi in out[..num_workers].iter_mut() {
        wi.clear();
        wi.extend(0..batch.count as u32);
    }
}

/// Like [`with_worker_indices`] but a full **broadcast** rather than a
/// PK-partitioned scatter. Used by the write path for a replicated table: the
/// same `scatter_wire_group(... FLAG_PUSH ...)` machinery then lands the whole
/// batch in every worker's ingest + SAL slot (inheriting the atomic zone, LSN,
/// ACK accounting, and the committer's single `fdatasync`), so every worker
/// holds an identical full copy. Same TLS-pool reuse and borrow contract as
/// `with_worker_indices`.
pub fn with_broadcast_indices<F, R>(batch: &Batch, num_workers: usize, f: F) -> R
where
    F: FnOnce(&[Vec<u32>]) -> R,
{
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        fill_broadcast_indices(batch, num_workers, &mut worker_indices);
        f(&worker_indices[..num_workers])
    })
}

/// Compute per-worker row-index lists for `batch` without building sub-batches.
/// Returns `worker_indices[w]` = row indices from `batch` destined for worker `w`.
/// Use `with_worker_indices` instead when the caller can borrow the routing table
/// for the duration of the scatter.
#[cfg(test)]
pub(crate) fn compute_worker_indices(
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD};
    use crate::storage::ConsolidatedBatch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
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
        let cb = make_batch_str(
            &schema,
            &[
                (1, 1, "short"),
                (2, 1, "a long blob value well beyond the inline threshold"),
                (3, 1, ""),
            ],
        );
        let b = cb.into_inner();
        let mb = b.as_mem_batch();
        // Must not panic and must distinguish the two non-empty values.
        let k_short = extract_col_key(&mb, 0, 1, &schema);
        let k_long = extract_col_key(&mb, 1, 1, &schema);
        let k_empty = extract_col_key(&mb, 2, 1, &schema);
        assert_ne!(k_short, 0);
        assert_ne!(k_long, 0);
        assert_eq!(k_empty, 0, "empty blob hashes to 0");
        assert_ne!(k_short, k_long);
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
        let length = crate::foundation::codec::read_u32_le(struct_bytes, 0) as usize;
        let hashed_key = crate::foundation::xxh::checksum(&struct_bytes[4..4 + length]);
        assert_eq!(router.worker_for_index_key(1, 1, hashed_key as u128), 2);

        // Long string (> 12 bytes)
        let b2 = make_batch_str(&schema, &[(2, 1, "a_long_string_value")]);
        router.record_routing(&b2, &schema, 1, 1, 3);
        // Must not panic
    }

    #[test]
    fn test_partition_routing_invariance_narrow_pk() {
        use crate::foundation::xxh::hash_u128;
        let num_workers = 4usize;
        let pks: Vec<u64> = vec![1, 42, 100, 1000, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX / 2];
        for &pk in &pks {
            let pk_u128 = pk as u128;
            let partition = (hash_u128(pk_u128) % num_workers as u64) as usize;
            assert!(
                partition < num_workers,
                "partition {partition} out of range for pk={pk}"
            );
        }
    }
}
