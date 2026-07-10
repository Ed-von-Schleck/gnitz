//! Output row emitters: raw reduce rows and the global-aggregate ground row.

use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

use super::super::util::write_string_from_batch;
use super::agg::{Accumulator, AggDescriptor};

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
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_reduce_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    out_pk_bytes: &[u8],
    accs: &[Accumulator],
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    use_natural_pk: bool,
    num_aggs: usize,
) {
    let num_out_cols = output_schema.num_columns();

    // The caller materialised the group's output PK bytes once (verbatim source
    // PK for natural-PK grouping, the synthetic group key otherwise); copy them.
    output.extend_pk_bytes(out_pk_bytes);
    output.extend_weight(&1i64.to_le_bytes());

    // Build null word and payload columns
    let mut null_word: u64 = 0;

    for (out_pi, ci, col) in output_schema.payload_columns() {
        let cs = col.size() as usize;

        // Determine if this is an agg column or a group exemplar column
        let agg_base = num_out_cols - num_aggs;
        if ci >= agg_base {
            emit_agg_col(output, &accs[ci - agg_base], out_pi, cs, &mut null_word);
        } else if use_natural_pk {
            // use_natural_pk: no group exemplar columns in output (PK IS the group)
            // This shouldn't happen — with use_natural_pk, non-agg non-PK cols don't exist
            output.fill_col_zero(out_pi, cs);
        } else {
            // Group exemplar column: ci=1..N maps to group_by_cols[ci-1]
            let grp_idx = ci - 1; // skip PK at 0
            if grp_idx < group_by_cols.len() {
                let src_ci = group_by_cols[grp_idx] as usize;
                let loc = input_schema.locate(src_ci);
                match loc {
                    ColumnLocator::Pk { .. } => {
                        // PK lives in the OPK region; decode the addressed column
                        // back to native LE before copying into the payload region
                        // (a raw copy keeps the flipped sign bit / big-endian order
                        // for signed and wide columns).
                        let mut scratch = [0u8; 16];
                        output.extend_col(out_pi, loc.native_le_bytes(input_mb, exemplar_row, &mut scratch));
                    }
                    ColumnLocator::Payload { slot, .. } => {
                        if loc.is_null(input_mb, exemplar_row) {
                            null_word |= 1u64 << out_pi;
                            output.fill_col_zero(out_pi, cs);
                        } else if gnitz_wire::is_german_string(col.type_code) {
                            write_string_from_batch(output, out_pi, input_mb, exemplar_row, slot as usize);
                        } else {
                            output.extend_col(out_pi, loc.bytes(input_mb, exemplar_row));
                        }
                    }
                }
            } else {
                output.fill_col_zero(out_pi, cs);
            }
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
pub(super) fn emit_global_ground(
    raw_output: &mut Batch,
    out_pk_bytes: &[u8],
    agg_descs: &[AggDescriptor],
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) {
    let num_aggs = agg_descs.len();

    // A global-aggregate output schema is `[_group_pk, aggs…]` — no group-exemplar
    // columns — so every payload column in `emit_reduce_row` takes the
    // `ci >= agg_base` branch and reads only the accumulator + `out_pk_bytes`; the
    // lone branch that dereferences `input_mb[exemplar_row]` is unreachable. Pin
    // it: this is what lets the empty-delta seed pass an empty input batch with no
    // out-of-bounds read.
    debug_assert!(
        output_schema.num_payload_cols() == num_aggs,
        "global_ground output schema must have zero group-exemplar columns",
    );

    // Fresh accumulators in the empty-group state — every one untouched
    // (`has_value` false). `emit_agg_col` renders each by `empty_renders_zero`: the
    // COUNT family and SumZero ground to a concrete `0` (null bit clear), SUM/MIN/MAX
    // to NULL. No COUNT seed is needed — an untouched Count / CountNonNull already
    // renders `0` (byte-identical to a `seed_from_raw_bits(0)` value), so the ground
    // row shares the one render path with a normal row.
    let accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, input_schema)).collect();

    // No source row exists (the seed runs over an empty delta), and none is read:
    // a throwaway empty input batch satisfies `emit_reduce_row`'s signature, and
    // the unreachable exemplar branch never dereferences it. `use_natural_pk` is
    // irrelevant for the same reason (no exemplar column is emitted).
    let empty_input = Batch::empty_with_schema(input_schema);
    let empty_mb = empty_input.as_mem_batch();
    emit_reduce_row(
        raw_output,
        &empty_mb,
        0,
        out_pk_bytes,
        &accs,
        input_schema,
        output_schema,
        group_by_cols,
        /* use_natural_pk = */ false,
        num_aggs,
    );
}
