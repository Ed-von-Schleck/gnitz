//! Output row emitters: raw reduce rows and the global-aggregate ground row.

use crate::storage::{Batch, MemBatch};
use gnitz_wire::WideKind;

use super::agg::{Accumulator, AggValue};
use super::plan::ReducePlan;

/// Emit one aggregate column: the value truncated to the column width, or the
/// empty-render when no row contributed — so `COUNT(col)` over an all-NULL group
/// renders `0`, not NULL.
#[inline]
fn emit_agg_col(output: &mut Batch, acc: &Accumulator, out_pi: usize, null_word: &mut u64) {
    let cs = acc.out_size();
    let Some(value) = acc.value() else {
        // Zero bytes either way. The zero-identity family (COUNT family /
        // SumZero) leaves the null bit clear so it renders a concrete `0`;
        // SUM/MIN/MAX flag NULL.
        if !acc.empty_renders_zero() {
            gnitz_wire::null_word_set(null_word, out_pi, true);
        }
        output.fill_col_zero(out_pi, cs);
        return;
    };
    match value {
        AggValue::Bits(bits) => output.extend_col(out_pi, &bits.to_le_bytes()[..cs]),
        AggValue::Wide(WideKind::Bytes, v) => output.extend_col_blob(out_pi, v),
        AggValue::Wide(WideKind::Fixed(_), v) => {
            debug_assert_eq!(v.len(), cs, "a 16-byte extreme fills its output column");
            output.extend_col(out_pi, v);
        }
    }
}

/// Emit the trailing aggregate columns, starting at payload index `pi_base`
/// (= the plan's group-exemplar count).
#[inline]
fn emit_agg_cols(output: &mut Batch, accs: &[Accumulator], pi_base: usize, null_word: &mut u64) {
    for (k, acc) in accs.iter().enumerate() {
        emit_agg_col(output, acc, pi_base + k, null_word);
    }
}

/// Emit one reduce output row — the +1 new-value row. Retractions are not built
/// here; they are byte-copied from the stored row via `copy_current_row_into`.
/// The output schema is `[key…, group exemplars…, aggregates…]`, so the two
/// payload loops are positional against the plan's `exemplar_locs()`.
pub(super) fn emit_reduce_row(
    output: &mut Batch,
    // The source row the group-exemplar columns copy from.
    (input_mb, exemplar_row): (&MemBatch, usize),
    out_pk_bytes: &[u8],
    accs: &[Accumulator],
    plan: &ReducePlan,
) {
    // The caller materialised the group's output PK bytes once (verbatim source
    // PK for natural-PK grouping, the synthetic group key otherwise); copy them.
    output.begin_row(out_pk_bytes, 1);
    let mut null_word: u64 = 0;

    for (out_pi, loc) in plan.exemplar_locs().iter().enumerate() {
        output.append_cell_from(out_pi, loc, input_mb, exemplar_row, &mut null_word);
    }
    emit_agg_cols(output, accs, plan.exemplar_locs().len(), &mut null_word);

    output.commit_row(null_word);
}

/// Emit a global (ungrouped) aggregate's one row at PK `out_pk_bytes` (= `V₀`) from `accs`.
/// Untouched accumulators render the ground row (`COUNT(*)=0`, `SUM/MIN/MAX/AVG=NULL`).
/// Emitted at weight +1.
pub(super) fn emit_global_ground(raw_output: &mut Batch, out_pk_bytes: &[u8], accs: &[Accumulator]) {
    raw_output.begin_row(out_pk_bytes, 1);
    let mut null_word: u64 = 0;
    emit_agg_cols(raw_output, accs, 0, &mut null_word);
    raw_output.commit_row(null_word);
}
