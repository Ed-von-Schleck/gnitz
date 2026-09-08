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

/// Emit the synthetic **ground row** of a global (ungrouped) aggregate at PK
/// `out_pk_bytes` (= `V₀`) — `COUNT(*)=0`, `SUM/MIN/MAX/AVG=NULL`, the one row SQL
/// scalar-aggregate semantics require over an empty or fully-retracted source.
/// Rendered from the plan's own untouched accumulator template, so it cannot drift
/// from a computed row. Emitted at weight +1; the caller nets it to one row (the
/// `has_old` retraction in `n>0`, the `!trace_out_has_V0` guard in `n==0`).
pub(super) fn emit_global_ground(raw_output: &mut Batch, out_pk_bytes: &[u8], plan: &ReducePlan) {
    // A global-aggregate output schema is `[_group_pk, aggs…]`: no exemplar column,
    // so there is no source row to read — which is what lets the empty-delta seed,
    // which has no input batch at all, emit the aggregate columns directly here
    // rather than through `emit_reduce_row`.
    debug_assert!(
        plan.exemplar_locs().is_empty(),
        "global_ground output schema must have zero group-exemplar columns",
    );

    // The plan's template is exactly the empty-group state — every accumulator
    // untouched, never stepped — and `emit_agg_col` renders each by
    // `empty_renders_zero`, so the ground row shares the one render path with a
    // normal row.
    raw_output.begin_row(out_pk_bytes, 1);
    let mut null_word: u64 = 0;
    emit_agg_cols(raw_output, &plan.acc_template, 0, &mut null_word);
    raw_output.commit_row(null_word);
}
