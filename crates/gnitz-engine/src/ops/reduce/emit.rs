//! Output row emitters: raw reduce rows, finalized rows, gather-reduce rows.

use crate::schema::{SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{Batch, MemBatch};

use super::super::util::write_string_from_batch;
use super::agg::Accumulator;

/// Write the row's PK to `output`. For arity-1 PKs the synthetic u128
/// `group_key` is sufficient; compound PKs must copy the source row's
/// PK bytes verbatim because the u128 cannot carry a >16-byte PK region
/// nor reorder columns to match the output's pk_indices layout.
#[inline]
fn emit_pk(
    output: &mut Batch,
    output_schema: &SchemaDescriptor,
    src_pk_bytes: &[u8],
    group_key: u128,
) {
    if output_schema.pk_indices().len() > 1 {
        output.extend_pk_bytes(src_pk_bytes);
    } else {
        output.extend_pk(group_key);
    }
}

/// Emit one reduce output row.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_reduce_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key: u128,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    use_natural_pk: bool,
    num_aggs: usize,
) {
    let num_out_cols = output_schema.num_columns();

    emit_pk(output, output_schema, input_mb.get_pk_bytes(exemplar_row), group_key);
    output.extend_weight(&weight.to_le_bytes());

    // Build null word and payload columns
    let mut null_word: u64 = 0;

    for (out_pi, ci, col) in output_schema.payload_columns() {
        let cs = col.size() as usize;

        // Determine if this is an agg column or a group exemplar column
        let agg_base = num_out_cols - num_aggs;
        if ci >= agg_base {
            // Aggregate column
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            // Null if accumulator is zero (no value) and not using old val
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else {
                output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);
            }
        } else if use_natural_pk {
            // use_natural_pk: no group exemplar columns in output (PK IS the group)
            // This shouldn't happen — with use_natural_pk, non-agg non-PK cols don't exist
            output.fill_col_zero(out_pi, cs);
        } else {
            // Group exemplar column: ci=1..N maps to group_by_cols[ci-1]
            let grp_idx = ci - 1; // skip PK at 0
            if grp_idx < group_by_cols.len() {
                let src_ci = group_by_cols[grp_idx] as usize;
                if input_schema.is_pk_col(src_ci) {
                    // PK is never null and lives outside the payload region.
                    let pk = input_mb.get_pk(exemplar_row);
                    output.extend_col(out_pi, &pk.to_le_bytes()[..cs]);
                } else {
                    let src_pi = input_schema.payload_idx(src_ci);
                    // Check null from input
                    let in_null = input_mb.get_null_word(exemplar_row);
                    if (in_null >> src_pi) & 1 != 0 {
                        null_word |= 1u64 << out_pi;
                        output.fill_col_zero(out_pi, cs);
                    } else if col.type_code == TypeCode::String as u8 {
                        write_string_from_batch(
                            output, out_pi,
                            input_mb, exemplar_row, src_pi,
                        );
                    } else {
                        let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                        output.extend_col(out_pi, src);
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
    group_key: u128,
    weight: i64,
    prog: &crate::expr::ExprProgram,
    raw_schema: &SchemaDescriptor,
    fin_schema: &SchemaDescriptor,
    ctx: &mut crate::expr::FinalizeContext,
) {
    use crate::expr::OutputColKind;

    let raw_mb = raw_output.as_mem_batch();
    let null_word = raw_mb.get_null_word(raw_row);

    ctx.eval_row(prog, &raw_mb, raw_row);
    let out_cols = ctx.out_cols();

    emit_pk(fin_output, fin_schema, raw_mb.get_pk_bytes(raw_row), group_key);
    fin_output.extend_weight(&weight.to_le_bytes());

    let mut fin_null_mask: u64 = 0;

    for (fpi, _ci, col) in fin_schema.payload_columns() {
        let cs = col.size() as usize;

        if fpi < out_cols.len() {
            match out_cols[fpi] {
                OutputColKind::CopyCol(src_pi_byte) => {
                    // After resolve_column_indices, COPY_COL.a1 carries the
                    // resolved payload byte: SENTINEL for the PK column,
                    // otherwise the dense payload index.
                    if src_pi_byte == PAYLOAD_MAPPING_PK_SENTINEL as usize {
                        // Source is the PK column — slice from the full 16-byte
                        // little-endian PK so 8- and 16-byte column sizes both work.
                        let pk = raw_mb.get_pk(raw_row);
                        fin_output.extend_col(fpi, &pk.to_le_bytes()[..cs]);
                    } else {
                        let src_pi = src_pi_byte;
                        let src_ci = raw_schema.payload_col_idx(src_pi);
                        if (null_word >> src_pi) & 1 != 0 {
                            fin_null_mask |= 1u64 << fpi;
                            fin_output.fill_col_zero(fpi, cs);
                        } else if raw_schema.columns[src_ci].type_code == TypeCode::String as u8 {
                            write_string_from_batch(
                                fin_output, fpi,
                                &raw_mb, raw_row, src_pi,
                            );
                        } else {
                            let src = raw_mb.get_col_ptr(raw_row, src_pi, cs);
                            fin_output.extend_col(fpi, src);
                        }
                    }
                }
                OutputColKind::Emit(eidx) => {
                    let (val, is_null) = ctx.read_emit(eidx);
                    if is_null {
                        fin_null_mask |= 1u64 << fpi;
                        fin_output.fill_col_zero(fpi, cs);
                    } else {
                        fin_output.extend_col(fpi, &val.to_le_bytes()[..cs]);
                    }
                }
                OutputColKind::EmitNull => {
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

/// Emit one gather-reduce output row.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_gather_row(
    output: &mut Batch,
    input_mb: &MemBatch,
    exemplar_row: usize,
    group_key: u128,
    weight: i64,
    old_vals: &[u64],
    use_old_val: bool,
    accs: &[Accumulator],
    schema: &SchemaDescriptor,
    num_aggs: usize,
) {
    let num_cols = schema.num_columns();
    let agg_base = num_cols - num_aggs;

    emit_pk(output, schema, input_mb.get_pk_bytes(exemplar_row), group_key);
    output.extend_weight(&weight.to_le_bytes());

    let mut null_word: u64 = 0;
    let in_null = input_mb.get_null_word(exemplar_row);

    for (out_pi, ci, col) in schema.payload_columns() {
        let cs = col.size() as usize;

        if ci >= agg_base {
            let agg_idx = ci - agg_base;
            let bits = if use_old_val {
                old_vals[agg_idx]
            } else {
                accs[agg_idx].get_value_bits()
            };
            if !use_old_val && accs[agg_idx].is_zero() {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else {
                output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);
            }
        } else {
            // Group exemplar column: copy from input
            let src_pi = out_pi; // same position
            if (in_null >> src_pi) & 1 != 0 {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else if col.type_code == TypeCode::String as u8 {
                write_string_from_batch(
                    output, out_pi,
                    input_mb, exemplar_row, src_pi,
                );
            } else {
                let src = input_mb.get_col_ptr(exemplar_row, src_pi, cs);
                output.extend_col(out_pi, src);
            }
        }
    }

    output.extend_null_bmp(&null_word.to_le_bytes());
    output.count += 1;
}
