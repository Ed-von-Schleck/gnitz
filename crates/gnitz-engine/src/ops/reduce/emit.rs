//! Output row emitters: raw reduce rows and the global-aggregate ground row.

use crate::schema::ColumnLocator;
use crate::storage::{Batch, MemBatch};

use super::agg::Accumulator;
use super::plan::ReducePlan;

/// Emit one aggregate column: its value bits truncated to the column width when
/// the accumulator holds a value, else its empty-render. An untouched accumulator
/// renders a concrete `0` (null bit clear) for the zero-identity family — COUNT /
/// COUNT_NON_NULL / SumZero (`empty_renders_zero`) — and NULL for SUM/MIN/MAX. So
/// `COUNT(col)` over an all-NULL group renders `0`, not NULL. Used by
/// `emit_reduce_row`.
#[inline]
fn emit_agg_col(output: &mut Batch, acc: &Accumulator, out_pi: usize, cs: usize, null_word: &mut u64) {
    if acc.is_untouched() {
        // Never stepped: zero bytes either way. The zero-identity family (COUNT
        // family / SumZero) leaves the null bit clear so it renders a concrete
        // `0`; SUM/MIN/MAX flag NULL.
        if !acc.empty_renders_zero() {
            gnitz_wire::null_word_set(null_word, out_pi, true);
        }
        output.fill_col_zero(out_pi, cs);
    } else {
        output.extend_col(out_pi, &acc.get_value_bits().to_le_bytes()[..cs]);
    }
}

/// Write a row's PK region and its `+1` weight. Paired with [`finish_row`],
/// which pushes the null word the payload emitters accumulated and commits the
/// row; every payload column between the two writes its own region at its own
/// payload index, so their order is free.
#[inline]
fn begin_row(output: &mut Batch, out_pk_bytes: &[u8]) {
    output.extend_pk_bytes(out_pk_bytes);
    output.extend_weight(&1i64.to_le_bytes());
}

#[inline]
fn finish_row(output: &mut Batch, null_word: u64) {
    output.extend_null_bmp(&null_word.to_le_bytes());
    output.count += 1;
}

/// Emit the trailing aggregate columns, starting at payload index `pi_base`
/// (= the plan's group-exemplar count).
#[inline]
fn emit_agg_cols(output: &mut Batch, accs: &[Accumulator], plan: &ReducePlan, pi_base: usize, null_word: &mut u64) {
    // The declaration/emission agreement is pinned once per plan by
    // `ReducePlan::new`, not per row.
    for (k, acc) in accs.iter().enumerate() {
        emit_agg_col(output, acc, pi_base + k, plan.agg_col_widths[k], null_word);
    }
}

/// Emit one reduce output row — the +1 new-value row. Retractions are not built
/// here; they are byte-copied from the stored row via `copy_current_row_into`.
/// The output schema is `[key…, group exemplars…, aggregates…]`, so the two
/// payload loops are positional against the plan's `exemplar_locs`.
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
    begin_row(output, out_pk_bytes);
    let mut null_word: u64 = 0;

    for (out_pi, loc) in plan.exemplar_locs.iter().enumerate() {
        match *loc {
            ColumnLocator::Pk { .. } => {
                // PK lives in the OPK region; decode the addressed column back to
                // native LE before copying into the payload region (a raw copy
                // keeps the flipped sign bit / big-endian order for signed and
                // wide columns).
                let mut scratch = [0u8; 16];
                output.extend_col(out_pi, loc.native_le_bytes(input_mb, exemplar_row, &mut scratch));
            }
            ColumnLocator::Payload { slot, size, type_code } => {
                let cs = size as usize;
                let is_null = gnitz_wire::null_word_get(input_mb.get_null_word(exemplar_row), slot as usize);
                if is_null {
                    gnitz_wire::null_word_set(&mut null_word, out_pi, true);
                }
                let cell = (!is_null).then(|| input_mb.get_col_ptr(exemplar_row, slot as usize, cs));
                output.append_payload_cell(out_pi, type_code, cs, cell, input_mb.blob, None);
            }
        }
    }
    emit_agg_cols(output, accs, plan, plan.exemplar_locs.len(), &mut null_word);

    finish_row(output, null_word);
}

/// Emit the synthetic **ground row** of a global (ungrouped) aggregate at PK
/// `out_pk_bytes` (= `V₀`): COUNT-family columns render `0`, SUM/MIN/MAX render
/// NULL. This is the one row SQL scalar-aggregate semantics require over an empty
/// or fully-retracted source (`COUNT(*)=0`, `SUM/MIN/MAX/AVG=NULL`); the
/// post-reduce MAP turns a derived `(SUM=NULL, COUNT_NON_NULL=0)` into
/// `AVG`/nullable-`SUM` = NULL with no special case.
///
/// Built from a **fresh** accumulator set in the empty-group state — never the
/// reduce loop's computed `accs` — so the ground row's layout has a single home,
/// shared by both emission sites (the `n>0` cardinality-zero branch and the
/// `n==0` seed), and cannot drift from a computed row. Emitted at weight +1; the
/// caller nets it to one row (the `has_old` retraction in `n>0`, the
/// `!trace_out_has_V0` guard in `n==0`).
pub(super) fn emit_global_ground(raw_output: &mut Batch, out_pk_bytes: &[u8], plan: &ReducePlan) {
    // A global-aggregate output schema is `[_group_pk, aggs…]`: group-less, so
    // `exemplar_locs` is empty and the whole payload is aggregates. That is why
    // this path emits the aggregate columns *directly* rather than through
    // `emit_reduce_row` — with no exemplar column there is no source row to
    // supply, so the empty-delta seed (which has no input batch at all) is
    // structurally unable to read one.
    debug_assert!(
        plan.exemplar_locs.is_empty(),
        "global_ground output schema must have zero group-exemplar columns",
    );

    // Fresh accumulators in the empty-group state — every one untouched
    // (`has_value` false). `emit_agg_col` renders each by `empty_renders_zero`: the
    // COUNT family and SumZero ground to a concrete `0` (null bit clear), SUM/MIN/MAX
    // to NULL. No COUNT seed is needed — an untouched Count / CountNonNull already
    // renders `0` (byte-identical to a `seed_from_raw_bits(0)` value), so the ground
    // row shares the one render path with a normal row.
    let accs: Vec<Accumulator> = plan
        .agg_descs
        .iter()
        .zip(&plan.agg_locs)
        .map(|(d, &loc)| Accumulator::new(d, loc))
        .collect();

    begin_row(raw_output, out_pk_bytes);
    let mut null_word: u64 = 0;
    emit_agg_cols(raw_output, &accs, plan, 0, &mut null_word);
    finish_row(raw_output, null_word);
}
