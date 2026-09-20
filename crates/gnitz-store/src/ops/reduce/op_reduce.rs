//! Incremental REDUCE operator: δ_out = Agg(history + δ_in) − Agg(history).

use std::cmp::Ordering;

use crate::schema::payload_order::compare_rows;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, Layout, ReadCursor, RowMark};

use super::emit::emit_reduce_row;
use super::plan::ReducePlan;

/// A group with more delta rows than this skips the pre-step and probes the
/// AVI: past it, stepping every row costs more than one seek.
const SKIP_TRACK_CAP: usize = 128;

/// Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history), emitted
/// consolidated.
pub fn op_reduce(
    delta: &Batch,
    trace_out_cursor: &mut ReadCursor,
    // Over the value index `plan.avi` describes, this epoch's entries included.
    // `Some` whenever `plan.avi` is and the delta is non-empty.
    history: Option<&mut ReadCursor>,
    plan: &ReducePlan,
) -> Batch {
    let shape = &plan.shape;
    let output_schema = &shape.output_schema;

    let cs = match plan.consolidates_input() {
        true => Batch::consolidate_if_needed(delta, &plan.input_schema),
        false => None,
    };
    let working: &Batch = cs.as_ref().unwrap_or(delta);

    if working.count == 0 {
        // An empty source delivers only empty deltas, so the ground row is minted
        // here: by the one worker owning V₀, and only while no V₀ row is stored.
        if plan.seeds_ground {
            let v0 = shape.key.ground_pk();
            if !trace_out_cursor.seek_pk_group_ascending(v0.bytes()) {
                let mut out = Batch::with_capacity(output_schema, 1);
                emit_reduce_row(&mut out, None, v0.bytes(), &shape.acc_template, shape);
                return out;
            }
        }
        return Batch::empty_with_schema(output_schema);
    }

    let mb = working.as_mem_batch();
    let runs = shape.key.runs(working);

    let mut out = Batch::with_capacity(output_schema, 2 * runs.len());
    let mut accs = shape.acc_template.clone();
    let mut avi = plan.avi.as_ref().map(|bake| {
        let cursor = history.expect("a value-indexed reduce is handed a cursor over the index its plan describes");
        (bake, cursor)
    });
    let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
    for run in runs.iter() {
        let first = runs.row(run.start);
        let out_pk = shape.key.out_pk(&mb, first);
        let out_pk_bytes: &[u8] = out_pk.bytes();

        for acc in accs.iter_mut() {
            acc.reset();
        }
        // An extreme recedes only on a retraction, so an all-insert group's
        // extremes are its rows' and its stored row's: no probe.
        let prestep = avi.is_some() && run.len() <= SKIP_TRACK_CAP;
        let mut saw_negative = false;
        for pos in run.clone() {
            let row = runs.row(pos);
            let w = mb.get_weight(row);
            saw_negative |= w <= 0;
            let extremes = prestep && !saw_negative;
            for acc in accs.iter_mut() {
                if acc.is_linear() || extremes {
                    acc.step_from_batch(&mb, row, w);
                }
            }
        }
        let probe = !prestep || saw_negative;

        let mark = out.mark();
        if trace_out_cursor.seek_pk_group_ascending(out_pk_bytes) {
            // −Agg(history) is the stored row, copied byte-identical at −1.
            trace_out_cursor.copy_current_row_into(&mut out, -1);
            let (stored_row, stored_idx) = trace_out_cursor.current_row_source();
            for acc in accs.iter_mut().filter(|a| a.is_linear() || !probe) {
                acc.fold_stored(stored_row, stored_idx);
            }
        }
        if probe {
            if let Some((bake, cursor)) = &mut avi {
                bake.pack_group(&mut gk, &mb, first);
                for (j, k) in bake.acc_indices().enumerate() {
                    bake.seed_extreme(cursor, &mut gk, j, &mut accs[k]);
                }
            }
        }

        debug_assert!(
            accs[plan.cardinality].count_value() >= 0,
            "reduce input must be bag-positive: negative group cardinality",
        );
        if accs[plan.cardinality].count_value() > 0 {
            emit_reduce_row(&mut out, Some((&mb, first)), out_pk_bytes, &accs, shape);
        } else if plan.seeds_ground {
            // An emptied global aggregate still publishes one row. The empty-key
            // scatter sends every row of a ground reduce to V₀'s owner.
            emit_reduce_row(&mut out, None, out_pk_bytes, &shape.acc_template, shape);
        }
        consolidate_group(&mut out, output_schema, mark);
    }

    gnitz_debug!(
        "op_reduce: in={} groups={} out={}",
        working.count,
        runs.len(),
        out.count
    );

    // A consolidated batch reaches a view store by move, allocation included.
    if out.count < runs.len() {
        out = out.clone_batch();
    }
    out.certify_layout(Layout::Consolidated);
    out
}

/// Consolidate the rows one group wrote since `mark`: at most a retraction and
/// a new row, under one PK.
fn consolidate_group(out: &mut Batch, schema: &SchemaDescriptor, mark: RowMark) {
    if out.rows_since(mark) != 2 {
        return;
    }
    let row0 = out.count - 2;
    let omb = out.as_mem_batch();
    match compare_rows(schema, &omb, row0, &omb, row0 + 1) {
        Ordering::Equal => out.truncate_to(mark),
        Ordering::Greater => out.swap_rows(row0, row0 + 1),
        Ordering::Less => {}
    }
}
