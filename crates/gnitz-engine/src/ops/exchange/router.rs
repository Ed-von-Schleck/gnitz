//! Exchange worker routing: `RouteMode`, `ScatterKey`, `scatter_is_pk_routed`,
//! and the per-row routing-key helpers.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, MemBatch};
use gnitz_wire::{worker_for_key, worker_for_pk_bytes};

use super::super::reindex::ReindexPacker;
use super::super::util::GroupKeyCols;

/// Keep only the rows this worker owns, by packed-PK hash — the trace-side
/// counterpart of the **pure** range-join broadcast input relay. A pure range
/// join (n_eq == 0) has no eq prefix to scatter by, so it broadcasts; every worker
/// receives the full delta and, before it integrates into the trace, this drops
/// the rows whose `worker_for_pk_bytes` owner is not
/// `worker_id`. (A band join scatters by the eq prefix instead — its trace is
/// already eq-prefix-partitioned and carries no `WorkerFilter`.) It is the SAME
/// hash the equality scatter (`RouteMode::JoinPromote`)
/// applies to the SAME packed PK bytes, so the integrated trace is partitioned
/// identically to a scattered equi-join trace — no trace replicates, no match
/// duplicates. Worker identity is a compile-time constant baked into the emitted
/// instruction; `num_workers <= 1` (single process) keeps every row.
pub(crate) fn op_worker_filter(batch: &Batch, schema: &SchemaDescriptor, worker_id: u32, num_workers: u32) -> Batch {
    let n = batch.count;
    if num_workers <= 1 || n == 0 {
        // Single process owns every row; degenerate to identity (preserving
        // the source's sorted/consolidated flags via clone_batch).
        return batch.clone_batch();
    }
    let nw = num_workers as usize;
    let wid = worker_id as usize;
    let mb = batch.as_mem_batch();

    // Keep just this worker's rows, routed by the same key→worker function the
    // equality scatter uses.
    let mut indices: Vec<u32> = Vec::with_capacity(n / nw + 1);
    for i in 0..n {
        if worker_for_pk_bytes(mb.get_pk_bytes(i), nw) == wid {
            indices.push(i as u32);
        }
    }
    batch.ascending_subset(&indices, schema)
}

/// Which routing key a non-PK scatter uses; picks between `ScatterKind::Packed`
/// and `ScatterKind::Fold` (whose docs carry the two contracts). The two keys
/// diverge for nullable and string columns, so the scatter caller picks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RouteMode {
    GroupKey,
    JoinPromote,
}

/// Per-scatter row router, built once (out of the row loop — a packer's
/// per-column schema classification is hoisted here) and applied per row. The
/// variant is picked from circuit metadata, not per-query data:
///
/// - `PkBytes`: the key IS the schema's PK list with no promotion
///   (`scatter_is_pk_routed`) — route by the row's native OPK bytes.
/// - `Packed`: a `JoinPromote` scatter packs the SAME OPK bytes the downstream
///   reindex Map stamps as the `_join_pk`, so the delta scatter and the
///   reindexed trace co-partition byte-for-byte. It is null-blind and
///   value-preserving by design — a LEFT-join NULL-key bypass row reads its
///   canonically-zeroed key slot and routes to the `_join_pk 0` owner, the
///   same place the reindex Map stamps it. (Float columns, the only type whose
///   OPK image would diverge from the routing hash, cannot be join keys — they
///   are rejected at plan time — so packing every `JoinPromote` key is exact.)
///   `buf` is the pack scratch; `pack_into` fully overwrites the `out_stride`
///   prefix it reads, so no inter-row clear is needed.
/// - `Fold`: a `GroupKey` (GROUP BY / set-op) scatter routes by the
///   null-distinct group fold — the baked [`GroupKeyCols`], byte-identical to
///   the group-key fold, which `op_reduce` also uses for the group's output
///   PK — the two must agree or the result is mis-gathered. The fold keeps
///   NULL distinct because a NULL group and a 0 group must not collide on one
///   output PK; scatter routing has no such requirement, but local grouping
///   (`compare_by_group_cols`) still separates co-located groups.
// One `ScatterKey` is built per scatter (a stack local) and read per row, so
// the `ReindexPacker` and its scratch stay inline — boxing would add a heap
// alloc and a per-row pointer chase for no benefit.
#[allow(clippy::large_enum_variant)]
pub(super) enum ScatterKind {
    PkBytes,
    Packed {
        packer: ReindexPacker,
        buf: [u8; crate::schema::MAX_PK_BYTES],
    },
    Fold {
        keys: GroupKeyCols,
    },
}

/// A [`ScatterKind`] bound to the worker count it routes into. The count is
/// carried here rather than passed per row so a scatter cannot route two rows
/// against different cluster shapes.
pub(super) struct ScatterKey {
    kind: ScatterKind,
    num_workers: usize,
}

impl ScatterKey {
    #[inline]
    pub(super) fn new(
        mode: RouteMode,
        cols: &[u32],
        tcs: &[u8],
        schema: &SchemaDescriptor,
        num_workers: usize,
    ) -> Self {
        let kind = if scatter_is_pk_routed(cols, tcs, schema) {
            ScatterKind::PkBytes
        } else {
            match mode {
                // `.expect`, not `?`: the scatter path has no compile-time
                // guard, so an invalid key must fail loudly rather than route
                // rows to the wrong worker.
                RouteMode::JoinPromote => ScatterKind::Packed {
                    packer: ReindexPacker::new(schema, cols, tcs)
                        .expect("ScatterKey: reindex key columns invalid for this schema"),
                    buf: [0u8; crate::schema::MAX_PK_BYTES],
                },
                RouteMode::GroupKey => ScatterKind::Fold {
                    keys: GroupKeyCols::new(schema, cols),
                },
            }
        };
        ScatterKey { kind, num_workers }
    }

    /// Whether this scatter routes by native PK bytes — the callers' gate for
    /// layout propagation (only a PK-routed sub-batch is an in-order subset of
    /// its source).
    #[inline]
    pub(super) fn is_pk_routed(&self) -> bool {
        matches!(self.kind, ScatterKind::PkBytes)
    }

    /// Route one row to its owning worker.
    #[inline]
    pub(super) fn worker(&mut self, mb: &MemBatch, row: usize) -> usize {
        let nw = self.num_workers;
        match &mut self.kind {
            ScatterKind::PkBytes => worker_for_pk_bytes(mb.get_pk_bytes(row), nw),
            ScatterKind::Packed { packer, buf } => {
                packer.pack_into(&mut buf[..packer.out_stride], mb, row);
                worker_for_pk_bytes(&buf[..packer.out_stride], nw)
            }
            ScatterKind::Fold { keys } => worker_for_key(keys.key_row(mb, row), nw),
        }
    }
}

/// One home for the relay-scatter "route by native PK bytes" gate: strict
/// sequence equality with the schema's PK list (set equality would route a
/// permuted compound PK differently from `worker_for_pk_bytes`, which
/// hashes OPK bytes in schema order) AND no cross-width promotion — a promoted
/// key (`tc != 0`) packs at the wider `T` via `ScatterKind::Packed`, so its
/// narrow source PK bytes must not route natively. Consulted once, in
/// `ScatterKey::new`, so the relay scatter paths cannot drift.
///
/// Deliberately NOT used by the write-path fan-out, which routes by the table's
/// distribution prefix (`schema.worker_for_pk`) — a different hash domain
/// with no promotion concept.
#[inline]
pub(super) fn scatter_is_pk_routed(col_indices: &[u32], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    col_indices == schema.pk_indices() && target_tcs.iter().all(|&tc| tc == 0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::util::GroupKeyCols;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::{make_batch, make_schema_u64_i64};

    #[test]
    fn test_worker_filter_keeps_only_this_workers_rows() {
        let schema = make_schema_u64_i64();
        let num_workers = 4u32;
        let rows: Vec<(u64, i64, i64)> = (0..40u64).map(|i| (i * 7 + 1, 1, i as i64)).collect();
        let batch = make_batch(&schema, &rows);

        // Each row kept by exactly the worker that owns its PK; the
        // union across workers is the whole batch with no duplication.
        let mut total_kept = 0usize;
        for wid in 0..num_workers {
            let out = op_worker_filter(&batch, &schema, wid, num_workers);
            total_kept += out.count;
            for r in 0..out.count {
                let pk = out.get_pk_bytes(r);
                let owner = worker_for_pk_bytes(pk, num_workers as usize);
                assert_eq!(owner as u32, wid, "row routed to wrong worker");
            }
        }
        assert_eq!(total_kept, batch.count, "worker filter dropped or duplicated rows");
    }

    #[test]
    fn test_worker_filter_single_worker_keeps_all() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let out = op_worker_filter(&batch, &schema, 0, 1);
        assert_eq!(out.count, 3, "(0, 1) must keep every row");
    }

    #[test]
    fn test_worker_filter_empty_in_empty_out() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[]);
        let out = op_worker_filter(&batch, &schema, 1, 4);
        assert_eq!(out.count, 0);
    }

    /// The `ScatterKey` collapse replaced the single-column `route_partition_key`
    /// (routable-int → `route_key`, string → `german_string_promote_key`) and the
    /// `compound_join_packer` path with one packed-`ReindexPacker` route.
    /// For every reachable JoinPromote key shape, the packed owner must equal
    /// what the pre-collapse routing produced — so no row moves workers. The
    /// NULL arms are null-blind by design (they read the canonically-zeroed key
    /// slot), exactly as the deleted `route_partition_key` was.
    #[test]
    fn test_scatter_key_packed_matches_legacy_routing() {
        use crate::storage::MemBatch;

        // Run the JoinPromote `ScatterKey` over `row` and return its worker.
        const NW: usize = 4;
        fn packed(schema: &SchemaDescriptor, cols: &[u32], tcs: &[u8], mb: &MemBatch, row: usize) -> usize {
            let mut sk = ScatterKey::new(RouteMode::JoinPromote, cols, tcs, schema, NW);
            assert!(!sk.is_pk_routed(), "test key shapes must take the packed route");
            sk.worker(mb, row)
        }

        // (1) non-null I64 payload; (2) NULL I64 payload (nullable col).
        {
            let schema = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::I64, 1),
                ],
                &[0],
            );
            let mut b = Batch::with_capacity(schema, 2);
            b.extend_pk(1u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &(-7i64).to_le_bytes());
            b.count += 1;
            b.extend_pk(2u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&1u64.to_le_bytes()); // col1 NULL, slot zeroed
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
            let mb = b.as_mem_batch();
            for row in 0..2 {
                // Legacy: routable-int → loc.route_key → worker_for_key (null-blind).
                let legacy = worker_for_key(schema.locate(1).route_key(&mb, row), NW);
                assert_eq!(packed(&schema, &[1], &[], &mb, row), legacy, "I64 row {row}");
            }
        }

        // (3) STRING payload; (4) NULL STRING payload.
        {
            let schema = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::STRING, 1),
                ],
                &[0],
            );
            let mut b = Batch::with_capacity(schema, 2);
            b.extend_pk(1u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let gs = gnitz_wire::encode_german_string(b"abc", &mut b.blob);
            b.extend_col(0, &gs);
            b.count += 1;
            b.extend_pk(2u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&1u64.to_le_bytes()); // col1 NULL, zeroed struct
            b.extend_col(0, &[0u8; 16]);
            b.count += 1;
            let mb = b.as_mem_batch();
            for row in 0..2 {
                // Legacy: string → german_string_promote_key → worker_for_key.
                let legacy = worker_for_key(
                    crate::ops::reindex::german_string_promote_key(mb.get_col_ptr(row, 0, 16), mb.blob),
                    NW,
                );
                assert_eq!(packed(&schema, &[1], &[], &mb, row), legacy, "STRING row {row}");
            }
        }

        // (5) U128 payload key: `is_pk_eligible` includes U128, so the
        // wide arm of `loc.route_key` fed `worker_for_key`.
        {
            let schema = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::U128, 0),
                ],
                &[0],
            );
            let mut b = Batch::with_capacity(schema, 1);
            b.extend_pk(1u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &(u128::MAX - 7).to_le_bytes());
            b.count += 1;
            let mb = b.as_mem_batch();
            let legacy = worker_for_key(schema.locate(1).route_key(&mb, 0), NW);
            assert_eq!(packed(&schema, &[1], &[], &mb, 0), legacy, "U128 payload");
        }

        // (6) single sub-column of a compound PK — the one legacy JoinPromote
        // shape that fell through `route_partition_key`'s non-PK guard to the
        // group fold `extract_group_key` (its Pk arm is `pk_route_key`).
        {
            let schema = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::I32, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0, 1],
            );
            let mut b = Batch::with_capacity(schema, 1);
            let mut pk = [0u8; 8];
            gnitz_wire::encode_pk_column(&7u32.to_le_bytes(), type_code::U32, &mut pk[0..4]);
            gnitz_wire::encode_pk_column(&(-9i32).to_le_bytes(), type_code::I32, &mut pk[4..8]);
            b.extend_pk_bytes(&pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &11i64.to_le_bytes());
            b.count += 1;
            let mb = b.as_mem_batch();
            for col in [0u32, 1u32] {
                let legacy = worker_for_key(GroupKeyCols::new(&schema, &[col]).key_row(&mb, 0), NW);
                assert_eq!(
                    packed(&schema, &[col], &[], &mb, 0),
                    legacy,
                    "compound-PK sub-col {col}"
                );
            }
        }
    }
}
