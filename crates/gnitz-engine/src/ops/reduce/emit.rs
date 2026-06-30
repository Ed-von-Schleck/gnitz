//! Output row emitters: raw reduce rows, finalized rows, the global-aggregate ground row.

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
                        let le = gnitz_wire::decode_pk_column_owned(loc.bytes(input_mb, exemplar_row), col.type_code);
                        output.extend_col(out_pi, &le[..cs]);
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
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_global_ground(
    raw_output: &mut Batch,
    out_pk_bytes: &[u8],
    agg_descs: &[AggDescriptor],
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    finalize_prog: Option<&crate::expr::ResolvedProgram>,
    finalize_out_schema: Option<&SchemaDescriptor>,
    fin_output: Option<&mut Batch>,
    fin_ctx: Option<&mut crate::expr::FinalizeContext>,
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

    if let (Some(prog), Some(fin_schema), Some(fin_out), Some(ctx)) =
        (finalize_prog, finalize_out_schema, fin_output, fin_ctx)
    {
        emit_finalized_row(
            fin_out,
            raw_output,
            raw_output.count - 1,
            1,
            prog,
            output_schema,
            fin_schema,
            ctx,
        );
    }
}

/// Emit a finalized row by evaluating the finalize ExprProgram on the raw output.
///
/// Handles COPY_COL (copy column from raw→finalized), EMIT (computed value),
/// and EMIT_NULL (null column) instructions. Per-row state (scratch register
/// file, classification, EMIT→register map, no_nulls) lives in `ctx`, which
/// the caller builds once before the group loop.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_finalized_row(
    fin_output: &mut Batch,
    raw_output: &Batch,
    raw_row: usize,
    weight: i64,
    prog: &crate::expr::ResolvedProgram,
    raw_schema: &SchemaDescriptor,
    fin_schema: &SchemaDescriptor,
    ctx: &mut crate::expr::FinalizeContext,
) {
    use crate::expr::{ColSrc, OutputColKind};

    let raw_mb = raw_output.as_mem_batch();
    let null_word = raw_mb.get_null_word(raw_row);

    ctx.eval_row(prog, &raw_mb, raw_row);
    let out_cols = ctx.out_cols();

    // The raw reduce-output PK is already materialised and the finalize output
    // carries it verbatim (same PK columns/stride), so copy its OPK bytes directly.
    fin_output.extend_pk_bytes(raw_mb.get_pk_bytes(raw_row));
    fin_output.extend_weight(&weight.to_le_bytes());

    let mut fin_null_mask: u64 = 0;

    for (fpi, _ci, col) in fin_schema.payload_columns() {
        let cs = col.size() as usize;

        if fpi < out_cols.len() {
            let kind = out_cols[fpi];
            // The finalize program emits in dense destination order, so the
            // bytecode-order `out_cols[fpi]` is the instruction producing payload
            // column `fpi`. `out_payload` is therefore redundant here (we index by
            // `fpi`); this assert pins that load-bearing invariant once.
            debug_assert_eq!(
                kind.out_payload(),
                fpi,
                "finalize out_cols not in dense destination order"
            );
            match kind {
                OutputColKind::CopyCol { src, .. } => match src {
                    ColSrc::Pk { off } => {
                        // PK source: decode the addressed OPK column back to native
                        // LE (raw copy is wrong for signed / big-endian columns).
                        // The fin column type equals the source PK column type for a
                        // verbatim copy. A single PK column is decoded, never the
                        // whole compound region.
                        let opk = raw_mb.get_pk_bytes(raw_row);
                        let off = off as usize;
                        let le = gnitz_wire::decode_pk_column_owned(&opk[off..off + cs], col.type_code);
                        fin_output.extend_col(fpi, &le[..cs]);
                    }
                    ColSrc::Payload(pi) => {
                        let src_pi = pi as usize;
                        let src_ci = raw_schema.payload_col_idx(src_pi);
                        if (null_word >> src_pi) & 1 != 0 {
                            fin_null_mask |= 1u64 << fpi;
                            fin_output.fill_col_zero(fpi, cs);
                        } else if gnitz_wire::is_german_string(raw_schema.columns[src_ci].type_code) {
                            write_string_from_batch(fin_output, fpi, &raw_mb, raw_row, src_pi);
                        } else {
                            let src = raw_mb.get_col_ptr(raw_row, src_pi, cs);
                            fin_output.extend_col(fpi, src);
                        }
                    }
                },
                OutputColKind::Emit { reg, .. } => {
                    let (val, is_null) = ctx.read_emit(reg);
                    if is_null {
                        fin_null_mask |= 1u64 << fpi;
                        fin_output.fill_col_zero(fpi, cs);
                    } else {
                        fin_output.extend_col(fpi, &val.to_le_bytes()[..cs]);
                    }
                }
                OutputColKind::EmitNull { .. } => {
                    fin_null_mask |= 1u64 << fpi;
                    fin_output.fill_col_zero(fpi, cs);
                }
            }
        } else {
            fin_output.fill_col_zero(fpi, cs);
        }
    }

    fin_output.extend_null_bmp(&fin_null_mask.to_le_bytes());
    fin_output.count += 1;
}
