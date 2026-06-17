//! Gather-reduce: merge partial aggregate deltas from workers.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor};

use super::agg::{Accumulator, AggDescriptor, readback_agg_bits};
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

    // Derive group_indices layout
    let num_group_cols = num_out_cols - 1 - num_aggs; // -1 for PK
    let _use_natural_pk_gather = num_group_cols == 0;

    let mut output = Batch::with_schema(*partial_schema, n);
    let mut accs: Vec<Accumulator> = agg_descs.iter().map(|d| Accumulator::new(d, partial_schema)).collect();
    let mut old_vals: Vec<u64> = vec![0u64; num_aggs];
    // Output width of each trailing agg column (loop-invariant). MIN/MAX now
    // emit at the source type's width, so a narrow column is < 8 bytes; the read
    // sites below use this both as the per-row offset stride and the slice
    // length, then `readback_agg_bits` rebuilds the 8-byte accumulator
    // (width-gated, so a float MIN/MAX widened to F64 is read verbatim).
    let agg_col_widths: Vec<usize> = (0..num_aggs)
        .map(|k| partial_schema.columns[num_out_cols - num_aggs + k].size() as usize)
        .collect();

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

        // Accumulate all partial deltas for this group
        while idx < n && smb.get_pk_bytes(idx) == group_pk_bytes
        {
            let w = smb.get_weight(idx);
            for k in 0..num_aggs {
                let pi = num_out_cols - num_aggs + k - 1; // -1 for PK (pk_index=0 always in output schema)
                let bits = readback_agg_bits(
                    smb.get_col_ptr(idx, pi, agg_col_widths[k]),
                    agg_descs[k].col_type_code,
                );
                if w > 0 {
                    accs[k].combine(bits);
                } else if w < 0 && accs[k].is_linear() {
                    accs[k].merge_accumulated(bits, -1);
                }
            }
            idx += 1;
        }

        // `get_pk` panics for wide PKs (stride > 16); leave the u128 0 there. It
        // is consumed only by the narrow seek/equality (via `seek_group`/
        // `current_pk_eq`) and by `emit_gather_row`, which re-derives compound
        // PKs from the exemplar row's bytes and ignores it.
        let group_pk: u128 = if partial_schema.pk_is_wide() { 0 } else { smb.get_pk(exemplar_row) };

        // Read old global from trace_out, keyed by the group's OPK bytes. The
        // partial batch is sorted in output-PK order, so the per-group probe is
        // monotone → galloping `advance_to` seeded at the live position.
        trace_out_cursor.advance_to(group_pk_bytes);
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_pk_eq(group_pk_bytes);

        if has_old {
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let cw = agg_col_widths[k];
                let ptr = trace_out_cursor.col_ptr(agg_col_idx, cw);
                if !ptr.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, cw) };
                    old_vals[k] = readback_agg_bits(bytes, agg_descs[k].col_type_code);
                } else {
                    old_vals[k] = 0;
                }
            }

            // Emit retraction
            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_pk, -1,
                &old_vals, true,
                &accs, partial_schema, num_aggs,
            );

            // Fold old global into accumulators
            for k in 0..num_aggs {
                accs[k].merge_accumulated(old_vals[k], 1);
            }

        }

        // Emit new global if non-zero
        let any_nonzero = accs.iter().any(|a| !a.is_zero());
        if any_nonzero {
            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_pk, 1,
                &old_vals, false,
                &accs, partial_schema, num_aggs,
            );
        }
    }

    output.sorted = true;
    gnitz_debug!("op_gather_reduce: in={} out={}", n, output.count);
    output
}
