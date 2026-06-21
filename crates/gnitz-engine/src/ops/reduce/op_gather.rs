//! Gather-reduce: merge partial aggregate deltas from workers.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor};

use super::agg::{fold_old_aggs, readback_agg_bits, Accumulator, AggDescriptor};
use super::emit::emit_gather_row;
use super::sort::sort_owned;

/// Gather-reduce: merge partial aggregate deltas from workers.
#[allow(clippy::needless_range_loop)]
pub fn op_gather_reduce(
    partial_batch: &Batch,
    trace_out_cursor: &mut ReadCursor,
    partial_schema: &SchemaDescriptor,
    agg_descs: &[AggDescriptor],
) -> Batch {
    let num_aggs = agg_descs.len();
    let num_out_cols = partial_schema.num_columns();

    // Sort without consolidation
    let sorted = sort_owned(partial_batch, partial_schema);
    let n = sorted.count;
    if n == 0 {
        return Batch::empty_with_schema(partial_schema);
    }

    let smb = sorted.as_mem_batch();

    let mut output = Batch::with_schema(*partial_schema, n);
    let mut accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, partial_schema)).collect();
    // Output width of each trailing agg column (loop-invariant). MIN/MAX now
    // emit at the source type's width, so a narrow column is < 8 bytes; the read
    // sites below use this both as the per-row offset stride and the slice
    // length, then `readback_agg_bits` rebuilds the 8-byte accumulator
    // (width-gated, so a float MIN/MAX widened to F64 is read verbatim).
    let agg_col_widths: Vec<usize> = (0..num_aggs)
        .map(|k| partial_schema.columns[num_out_cols - num_aggs + k].size() as usize)
        .collect();
    // First aggregate column's payload slot and logical index: the aggregates
    // are the trailing payload columns, so the null-bitmap bit / get_col_ptr
    // index (`pbase`) and the logical column index (`cbase`) are correct at any
    // PK arity.
    let pbase = partial_schema.num_payload_cols() - num_aggs;
    let cbase = num_out_cols - num_aggs;

    let mut idx = 0usize;
    while idx < n {
        let exemplar_row = idx;
        // Borrows of `smb` are all immutable here — `get_pk_bytes` returns a
        // slice into `smb.data` and coexists freely with `get_pk_bytes(idx)`
        // and `get_col_ptr(idx, ..)` inside the inner loop.
        let group_pk_bytes = smb.get_pk_bytes(idx);

        for acc in accs.iter_mut() {
            acc.reset();
        }

        // Accumulate all partial deltas for this group; a NULL partial
        // contributes nothing (combining its zero bytes would corrupt MIN/MAX —
        // e.g. global MIN(5, 0) = 0 — and saturate has_value for SUM).
        while idx < n && smb.get_pk_bytes(idx) == group_pk_bytes {
            let w = smb.get_weight(idx);
            let in_null_word = smb.get_null_word(idx);
            for k in 0..num_aggs {
                let pi = pbase + k;
                if (in_null_word >> pi) & 1 != 0 {
                    continue;
                }
                let bits = readback_agg_bits(smb.get_col_ptr(idx, pi, agg_col_widths[k]), agg_descs[k].col_type_code);
                if w > 0 {
                    accs[k].combine(bits);
                } else if w < 0 && accs[k].is_linear() {
                    accs[k].merge_accumulated(bits, -1);
                }
            }
            idx += 1;
        }

        // `get_pk` panics for wide PKs (stride > 16); leave the u128 0 there. It
        // is consumed only by the narrow seek/equality (via `seek_bytes`/
        // `current_pk_eq`) and by `emit_gather_row`, which re-derives compound
        // PKs from the exemplar row's bytes and ignores it.
        let group_pk: u128 = if partial_schema.pk_is_wide() {
            0
        } else {
            smb.get_pk(exemplar_row)
        };

        // Read old global from trace_out, keyed by the group's OPK bytes. The
        // partial batch is sorted in output-PK order, so the per-group probe is
        // monotone → galloping `advance_to` seeded at the live position.
        trace_out_cursor.advance_to(group_pk_bytes);
        let has_old = trace_out_cursor.valid && trace_out_cursor.current_pk_eq(group_pk_bytes);

        if has_old {
            // δ_out's −Agg(history) term IS the stored global row: copy trace_out's
            // current row at weight -1 (byte-identical PK/payload/null bits, no
            // reconstruction). The copy takes &self, so the fold below still reads it.
            trace_out_cursor.copy_current_row_into(&mut output, -1);

            // Fold old global into the accumulators (new = old + delta).
            fold_old_aggs(&mut accs, trace_out_cursor, agg_descs, &agg_col_widths, cbase, pbase);
        }

        // Emit new global if non-zero
        let any_nonzero = accs.iter().any(|a| !a.is_zero());
        if any_nonzero {
            emit_gather_row(
                &mut output,
                &smb,
                exemplar_row,
                group_pk,
                &accs,
                partial_schema,
                num_aggs,
            );
        }
    }

    output.sorted = true;
    gnitz_debug!("op_gather_reduce: in={} out={}", n, output.count);
    output
}
