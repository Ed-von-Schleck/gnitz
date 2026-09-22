//! Exchange worker routing: `ScatterSpec`, `ScatterKey`, and the per-row
//! routing-key helpers.

use crate::schema::{worker_for_key, worker_for_pk_bytes};
use crate::schema::{OpBuildErr, Placement, SchemaDescriptor};
use crate::storage::{Batch, MemBatch, Slot};

use super::super::group_key::{single_col_canonical_group_key, GroupKeyCols};
use crate::schema::key::ReindexPacker;

/// Keep only the rows `slot` owns: those whose PK `worker_for_pk_bytes` routes
/// to it, the hash the equality scatter routes a join key by. A broadcast delta
/// filtered here integrates into a trace partitioned like a scattered one.
pub fn op_worker_filter(batch: &Batch, slot: Slot) -> Batch {
    let n = batch.count;
    if n == 0 {
        return batch.clone_batch();
    }
    let nw = slot.of as usize;
    let wid = slot.rank as usize;
    let mb = batch.as_mem_batch();
    let mut indices: Vec<u32> = Vec::with_capacity(n / nw + 1);
    for i in 0..n {
        if worker_for_pk_bytes(mb.get_pk_bytes(i), nw) == wid {
            indices.push(i as u32);
        }
    }
    batch.ascending_subset(&indices)
}

/// Which routing key a scatter uses, and the columns it reads. The two keys
/// diverge for nullable and string columns, so the circuit states which it means
/// rather than the scatter guessing — see `ScatterKind::Packed` / `::Fold` for
/// the two contracts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ScatterSpec<'a> {
    /// GROUP BY / set-op: the null-distinct group fold over these columns.
    GroupKey(&'a [u32]),
    /// Equi-join: the packed `_join_pk`, one slot per `(source column, promoted
    /// key type)` — `None` derives the slot type from the source column.
    JoinKey(&'a [gnitz_wire::ReindexSlot]),
}

impl ScatterSpec<'_> {
    /// Refused exactly where `ScatterKey::new` would refuse it.
    pub fn check(self, schema: &SchemaDescriptor) -> Result<(), OpBuildErr> {
        ScatterKey::new(self, schema, 1).map(drop)
    }

    /// True iff the exchange this spec describes would move nothing: its columns
    /// are exactly `schema`'s distribution prefix, and its `ScatterKind` hashes
    /// them to the bytes `worker_for_pk` already placed the rows by.
    pub fn routes_to_native_owner(self, schema: &SchemaDescriptor) -> bool {
        let Placement::Keyed { prefix_len } = schema.placement() else {
            return false;
        };
        let pk = schema.pk_indices();
        let (cols, native_hash): (Vec<u32>, bool) = match self {
            // `PkBytes` over the whole PK, or `Fold` over the one column's
            // `opk_image`. A wider fold is Xxh3 over material no PK hash sees.
            ScatterSpec::GroupKey(cols) => (
                cols.to_vec(),
                cols == pk || single_col_canonical_group_key(schema, cols),
            ),
            // `PkBytes` or `Packed`, both re-emitting the columns' own OPK bytes
            // — unless a slot promotes, which packs wider than its source column.
            ScatterSpec::JoinKey(slots) => (
                slots.iter().map(|&(c, _)| c).collect(),
                slots.iter().all(|&(_, t)| t.is_none()),
            ),
        };
        native_hash && gnitz_wire::validate_dist_prefix(pk, &cols).is_ok_and(|n| n == prefix_len as usize)
    }
}

/// Per-scatter row router, built once (out of the row loop — a packer's
/// per-column schema classification is hoisted here) and applied per row. The
/// variant is picked from circuit metadata, not per-query data:
///
/// - `PkBytes`: the key IS the schema's PK list with no promotion (see
///   [`ScatterKey::new`]) — route by the row's native OPK bytes. Deliberately
///   not the write-path fan-out's rule, which routes by the distribution prefix
///   — a different hash domain with no promotion concept.
/// - `Packed`: a `JoinKey` scatter packs the SAME OPK bytes the downstream
///   reindex Map stamps as the `_join_pk`, so the delta scatter and the
///   reindexed trace co-partition byte-for-byte. It is null-blind and
///   value-preserving by design — a LEFT-join NULL-key bypass row reads its
///   canonically-zeroed key slot and routes to the `_join_pk 0` owner, the
///   same place the reindex Map stamps it. (Float columns, the only type whose
///   OPK image would diverge from the routing hash, cannot be join keys — they
///   are rejected at plan time — so packing every `JoinKey` is exact.)
/// - `Fold`: a `GroupKey` (GROUP BY / set-op) scatter routes by the
///   null-distinct group fold — the baked [`GroupKeyCols`], byte-identical to
///   the group-key fold, which `op_reduce` also uses for the group's output
///   PK — the two must agree or the result is mis-gathered. The fold keeps
///   NULL distinct because a NULL group and a 0 group must not collide on one
///   output PK.
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
    /// Refused when `schema` cannot route by `spec`: a column it has not got, or
    /// one the group key or a reindex key refuses.
    #[inline]
    pub(super) fn new(
        spec: ScatterSpec<'_>,
        schema: &SchemaDescriptor,
        num_workers: usize,
    ) -> Result<Self, OpBuildErr> {
        // Sequence equality, not set equality: `worker_for_pk_bytes` hashes OPK
        // bytes in schema order, so a permuted compound PK routes differently.
        // And no carried target throughout: a promoted key packs at the wider
        // `T`, so its narrow source PK bytes must not route natively.
        let pk = schema.pk_indices();
        let kind = match spec {
            ScatterSpec::GroupKey(cols) if cols == pk => ScatterKind::PkBytes,
            ScatterSpec::GroupKey(cols) => ScatterKind::Fold { keys: GroupKeyCols::new(schema, cols)? },
            ScatterSpec::JoinKey(slots)
                if slots.len() == pk.len() && slots.iter().zip(pk).all(|(&(c, tc), &p)| c == p && tc.is_none()) =>
            {
                ScatterKind::PkBytes
            }
            ScatterSpec::JoinKey(slots) => ScatterKind::Packed {
                packer: ReindexPacker::new(schema, slots)?,
                buf: [0u8; crate::schema::MAX_PK_BYTES],
            },
        };
        Ok(ScatterKey { kind, num_workers })
    }

    /// Route one row to its owning worker.
    #[inline]
    pub(super) fn worker(&mut self, mb: &MemBatch, row: usize) -> usize {
        let nw = self.num_workers;
        match &mut self.kind {
            ScatterKind::PkBytes => worker_for_pk_bytes(mb.get_pk_bytes(row), nw),
            ScatterKind::Packed { packer, buf } => worker_for_pk_bytes(packer.pack_prefix(buf, mb, row), nw),
            ScatterKind::Fold { keys } => worker_for_key(keys.key_row(mb, row), nw),
        }
    }

    /// Route every row of one source into `slots`, dropping the weight-0 rows
    /// an unconsolidated source may carry — not Z-set elements.
    pub(super) fn route_into(&mut self, mb: &MemBatch, si: u32, slots: &mut [Vec<(u32, u32, i64)>]) {
        let nw = self.num_workers;
        match &mut self.kind {
            ScatterKind::PkBytes => route_rows(mb, si, slots, |mb, row| worker_for_pk_bytes(mb.get_pk_bytes(row), nw)),
            ScatterKind::Packed { packer, buf } => route_rows(mb, si, slots, |mb, row| {
                worker_for_pk_bytes(packer.pack_prefix(buf, mb, row), nw)
            }),
            ScatterKind::Fold { keys } => {
                route_rows(mb, si, slots, |mb, row| worker_for_key(keys.key_row(mb, row), nw))
            }
        }
    }
}

/// Generic in `worker`, and `#[inline(always)]`, so each [`ScatterKind`] arm
/// monomorphizes its key derivation into the row loop.
#[inline(always)]
fn route_rows(
    mb: &MemBatch,
    si: u32,
    slots: &mut [Vec<(u32, u32, i64)>],
    mut worker: impl FnMut(&MemBatch, usize) -> usize,
) {
    for row in 0..mb.count {
        let w = mb.get_weight(row);
        if w == 0 {
            continue;
        }
        slots[worker(mb, row)].push((si, row as u32, w));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/router.rs"]
mod tests;
