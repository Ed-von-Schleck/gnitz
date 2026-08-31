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

    // 2. Co-group the consolidated delta against the integral trace on PK, then
    //    run a (PK, payload) sub-merge inside each group: for each delta element
    //    fold the byte-equal trace row's weight and compute the clamped change
    //    clamp(w_old + w_delta) − clamp(w_old). `cogroup_left` visits every
    //    delta group (every element transitions or not); the inner payload merge
    //    walks the delta sub-range and the trace PK group in lockstep, both
    //    being (PK, payload)-sorted, so the per-element trace probe is the
    //    monotone forward walk the old per-row `seek_bytes` open-coded.
    // Grown on first emit, not up front: a tick where nothing transitions —
    // the common shape once a set op has settled — then allocates nothing.
    let mut emit_indices: Vec<u32> = Vec::new();
    let mut emit_weights: Vec<i64> = Vec::new();

    let consolidated_mb = consolidated.as_mem_batch();
    // The payload comparator, dispatched once for the whole scan. This is the
    // inner loop of every set operation and every equi/band outer-join
    // null-fill, so a per-comparison dispatch here would run the generic
    // per-column body even where the schema selects the branch-free one.
    let row_cmp = cursor.payload_cmp_vs_mem();

    cogroup_left(&consolidated, cursor, |key, range, m| {
        for i in range {
            let w_old: i64 = loop {
                if !m.valid || !m.current_pk_eq(key) {
                    break 0;
                }
                match row_cmp(m, schema, &consolidated_mb, i) {
                    std::cmp::Ordering::Less => {
                        m.advance();
                    }
                    std::cmp::Ordering::Equal => {
                        let w = m.current_weight;
                        m.advance();
                        break w;
                    }
                    std::cmp::Ordering::Greater => break 0,
                }
            };

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
        }
    });

    // 3. Scatter-copy emitting rows, column-first, then blit the *clamp's* net
    //    weights over the finished region — one sequential `n·8` write against a
    //    per-(row, column) dispatch loop through the row-at-a-time writer.
    let mut output = Batch::from_indexed_rows(&consolidated_mb, &emit_indices, schema);
    for (dst, w) in output.weight_data_mut().chunks_exact_mut(8).zip(&emit_weights) {
        dst.copy_from_slice(&w.to_le_bytes());
    }
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
