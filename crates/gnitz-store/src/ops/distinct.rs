//! DBSP distinct operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, Layout, ReadCursor};

use super::cogroup::cogroup_left;

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

/// Shared body for the two weight-clamp operators. Per consolidated (PK, payload)
/// emits `clamp(w_old + Δw, lo, hi) − clamp(w_old, lo, hi)`; the `(lo, hi)` preset
/// selects the operator:
///
/// * `(-1, 1)` → `distinct` (set membership — `clamp(w, -1, 1) == signum(w)`),
/// * `(0, i64::MAX)` → `positive_part` (bag multiplicity, negative part only).
///
/// Both are the DBSP incremental form of a per-element weight clamp lifted to its
/// delta. Returns `(output_batch, consolidated_delta)`; the consolidated delta is
/// returned so the caller can feed it to `ingest_batch`.
pub fn op_weight_clamp(
    delta: Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
    lo: i64,
    hi: i64,
) -> (Batch, Batch) {
    // 1. Consolidate delta
    let consolidated = delta.into_consolidated(schema);
    let n = consolidated.count;
    if n == 0 {
        return (Batch::empty_with_schema(schema), consolidated);
    }

    // 2. Per delta element, the clamped change against its trace weight. Every
    //    delta group is visited: an element transitions whether or not the trace
    //    holds its PK.
    // Grown on first emit, not up front: a tick where nothing transitions —
    // the common shape once a set op has settled — then allocates nothing.
    let mut emit_indices: Vec<u32> = Vec::new();
    let mut emit_weights: Vec<i64> = Vec::new();

    let consolidated_mb = consolidated.as_mem_batch();
    cogroup_left(&consolidated, cursor, |_, range, m| {
        m.for_each_mem_row_weight(&consolidated_mb, range, |i, w_old| {
            let w_new = w_old.wrapping_add(consolidated_mb.get_weight(i));
            let out_w = w_new.clamp(lo, hi) - w_old.clamp(lo, hi);
            if out_w != 0 {
                if emit_indices.is_empty() {
                    emit_indices.reserve(n - i);
                    emit_weights.reserve(n - i);
                }
                emit_indices.push(i as u32);
                emit_weights.push(out_w);
            }
        });
    });

    // 3. Scatter-copy emitting rows, column-first, then blit the *clamp's* net
    //    weights over the finished region — one sequential `n·8` write against a
    //    per-(row, column) dispatch loop through the row-at-a-time writer.
    let mut output = Batch::from_indexed_rows(&consolidated_mb, &emit_indices, schema);
    output.overwrite_weights(&emit_weights);
    // Emitting rows are scattered in consolidated-delta order (ascending indices),
    // one per transitioning element ⇒ (PK, payload)-sorted and ghost-free.
    output.certify_layout(Layout::Consolidated, schema);

    (output, consolidated)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/distinct.rs"]
mod tests;
