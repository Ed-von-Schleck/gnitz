//! Output row emitters: raw reduce rows and the global-aggregate ground row.

use crate::schema::ColumnLocator;
use crate::storage::{Batch, MemBatch};

use super::agg::Accumulator;
use super::plan::{OutColRole, ReducePlan};

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
            *null_word |= 1u64 << out_pi;
        }
        output.fill_col_zero(out_pi, cs);
    } else {
        output.extend_col(out_pi, &acc.get_value_bits().to_le_bytes()[..cs]);
    }
}

/// Emit one reduce output row — the +1 new-value row. Retractions are not built
/// here; they are byte-copied from the stored row via `copy_current_row_into`.
/// Each payload column's role (aggregate vs. group exemplar, with a
/// pre-resolved input locator) comes from the plan's role table.
pub(super) fn emit_reduce_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    out_pk_bytes: &[u8],
    accs: &[Accumulator],
    plan: &ReducePlan,
) {
    // The caller materialised the group's output PK bytes once (verbatim source
    // PK for natural-PK grouping, the synthetic group key otherwise); copy them.
    output.extend_pk_bytes(out_pk_bytes);
    output.extend_weight(&1i64.to_le_bytes());

    // Build null word and payload columns
    let mut null_word: u64 = 0;

    for (out_pi, role) in plan.out_roles.iter().enumerate() {
        match *role {
            OutColRole::Agg { k } => {
                let cs = plan.agg_col_widths[k as usize];
                emit_agg_col(output, &accs[k as usize], out_pi, cs, &mut null_word);
            }
            OutColRole::Exemplar(loc) => match loc {
                ColumnLocator::Pk { .. } => {
                    // PK lives in the OPK region; decode the addressed column
                    // back to native LE before copying into the payload region
                    // (a raw copy keeps the flipped sign bit / big-endian order
                    // for signed and wide columns).
                    let mut scratch = [0u8; 16];
                    output.extend_col(out_pi, loc.native_le_bytes(input_mb, exemplar_row, &mut scratch));
                }
                ColumnLocator::Payload { slot, size, type_code } => {
                    if loc.is_null(input_mb, exemplar_row) {
                        null_word |= 1u64 << out_pi;
                        output.fill_col_zero(out_pi, size as usize);
                    } else if gnitz_wire::is_german_string(type_code) {
                        let src = input_mb.get_col_ptr(exemplar_row, slot as usize, 16);
                        let cell =
                            crate::schema::relocate_german_string_vec(src, input_mb.blob, &mut output.blob, None);
                        output.extend_col(out_pi, &cell);
                    } else {
                        output.extend_col(out_pi, loc.bytes(input_mb, exemplar_row));
                    }
                }
            },
        }
    }

    output.extend_null_bmp(&null_word.to_le_bytes());
    output.count += 1;
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
    // A global-aggregate output schema is `[_group_pk, aggs…]` — no group-exemplar
    // columns — so every payload column in `emit_reduce_row` takes the `Agg` role
    // and reads only the accumulator + `out_pk_bytes`; the lone role that
    // dereferences `input_mb[exemplar_row]` is unreachable. Pin it: this is what
    // lets the empty-delta seed pass an empty input batch with no out-of-bounds
    // read.
    debug_assert!(
        plan.output_schema.num_payload_cols() == plan.agg_descs.len(),
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

    // No source row exists (the seed runs over an empty delta), and none is read:
    // a throwaway empty input batch satisfies `emit_reduce_row`'s signature, and
    // the unreachable exemplar role never dereferences it.
    let empty_input = Batch::empty_with_schema(&plan.input_schema);
    let empty_mb = empty_input.as_mem_batch();
    emit_reduce_row(raw_output, &empty_mb, 0, out_pk_bytes, &accs, plan);
}
