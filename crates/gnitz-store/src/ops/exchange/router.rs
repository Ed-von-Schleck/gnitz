//! Exchange worker routing: `RouteMode`, `ScatterKey`,
//! and the per-row routing-key helpers.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, MemBatch};
use gnitz_wire::{worker_for_key, worker_for_pk_bytes};

use super::super::group_key::GroupKeyCols;
use crate::schema::key::ReindexPacker;

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
pub fn op_worker_filter(batch: &Batch, schema: &SchemaDescriptor, worker_id: u32, num_workers: u32) -> Batch {
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
/// - `PkBytes`: the key IS the schema's PK list with no promotion (see
///   [`ScatterKey::new`]) — route by the row's native OPK bytes. Deliberately
///   not the write-path fan-out's rule, which routes by the distribution prefix
///   — a different hash domain with no promotion concept.
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
    pub(crate) fn new(
        mode: RouteMode,
        cols: &[u32],
        tcs: &[u8],
        schema: &SchemaDescriptor,
        num_workers: usize,
    ) -> Self {
        // Sequence equality, not set equality: `worker_for_pk_bytes` hashes OPK
        // bytes in schema order, so a permuted compound PK routes differently.
        // And `tc == 0` throughout: a promoted key packs at the wider `T`, so its
        // narrow source PK bytes must not route natively.
        let kind = if cols == schema.pk_indices() && tcs.iter().all(|&tc| tc == 0) {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/router.rs"]
mod tests;
