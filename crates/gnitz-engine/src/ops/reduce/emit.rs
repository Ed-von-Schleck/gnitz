//! Output row emitters: raw reduce rows, finalized rows, gather-reduce rows.

use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

use super::super::util::write_string_from_batch;
use super::agg::Accumulator;

/// Emit one aggregate column: NULL (and zero-filled) when the accumulator holds
/// no value, else its value bits truncated to the column width. Shared by
/// `emit_reduce_row` and `emit_gather_row` so the two emitters cannot drift.
#[inline]
fn emit_agg_col(output: &mut Batch, acc: &Accumulator, out_pi: usize, cs: usize, null_word: &mut u64) {
    if acc.is_zero() {
        *null_word |= 1u64 << out_pi;
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

/// Emit one gather-reduce output row — the +1 new-global row. Retractions are
/// byte-copied from the stored row via `copy_current_row_into`, not built here.
pub(super) fn emit_gather_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    accs: &[Accumulator],
    schema: &SchemaDescriptor,
    num_aggs: usize,
) {
    let num_cols = schema.num_columns();
    let agg_base = num_cols - num_aggs;

    // The gather partial's PK region IS the materialised group key — copy the
    // exemplar row's OPK bytes verbatim (correct at every arity and width).
    output.extend_pk_bytes(input_mb.get_pk_bytes(exemplar_row));
    output.extend_weight(&1i64.to_le_bytes());

    let mut null_word: u64 = 0;
    let in_null = input_mb.get_null_word(exemplar_row);

    for (out_pi, ci, col) in schema.payload_columns() {
        let cs = col.size() as usize;

        if ci >= agg_base {
            emit_agg_col(output, &accs[ci - agg_base], out_pi, cs, &mut null_word);
        } else {
            // Group exemplar column: copy from input
            let src_pi = out_pi; // same position
            if (in_null >> src_pi) & 1 != 0 {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else if gnitz_wire::is_german_string(col.type_code) {
                write_string_from_batch(output, out_pi, input_mb, exemplar_row, src_pi);
            } else {
                let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                output.extend_col(out_pi, src);
            }
        }
    }

    output.extend_null_bmp(&null_word.to_le_bytes());
    output.count += 1;
}
