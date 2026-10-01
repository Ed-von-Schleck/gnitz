//! Incremental REDUCE operator: δ_out = Agg(history + δ_in) − Agg(history).

use std::cmp::Ordering;

use crate::repr::{Batch, MemBatch, ReadCursor, RowMark};
use crate::schema::payload_order::compare_rows;
use crate::schema::SchemaDescriptor;

use super::avi::AviBake;
use super::plan::ReducePlan;
use crate::algebra::emit_reduce_row;
use crate::algebra::ground_pk;
use crate::algebra::{Accumulator, GroupOrdinals, GroupedState};

/// A group with more delta rows than this skips the pre-step and probes the
/// value index: one seek in place of stepping every row.
const PRESTEP_CAP: usize = 128;

/// Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history), emitted
/// consolidated.
///
/// The delta is folded a column at a time, each aggregate over its own column:
/// over row ranges where the delta already stands in group order and its groups
/// are long enough to pay for a range each, else over one group ordinal per row.
pub fn op_reduce(
    delta: &Batch,
    trace_out_cursor: &mut ReadCursor,
    // Over the value index `plan.avi` describes, this epoch's entries included.
    history: Option<&mut ReadCursor>,
    plan: &ReducePlan,
) -> Batch {
    let shape = &plan.shape;
    let output_schema = &shape.output_schema;

    // The VM folds this register before any reader.
    debug_assert!(plan.is_exact_linear() || delta.is_consolidated());

    if delta.count == 0 {
        // An empty source delivers only empty deltas, so the ground row is minted
        // here, by a worker `seeds_ground` names, and only while no V₀ row is stored.
        if plan.seeds_ground {
            let v0 = ground_pk();
            if !trace_out_cursor.seek_pk_group_ascending(v0.bytes()) {
                let mut out = Batch::with_capacity(output_schema, 1);
                emit_reduce_row(&mut out, None, v0.bytes(), &shape.acc_template);
                return out;
            }
        }
        return Batch::empty_with_schema(output_schema);
    }

    let mb = delta.as_mem_batch();
    let weights = mb.weight().as_chunks::<8>().0;
    let retracts = |w: &[u8; 8]| i64::from_le_bytes(*w) <= 0;
    let avi = plan.avi.as_ref().map(|bake| {
        let cursor = history.expect("a value-indexed reduce is handed a cursor over the index its plan describes");
        (bake, cursor)
    });
    let indexed = avi.is_some();
    let mut accs = shape.acc_template.clone();

    let groups = match shape.key.runs(delta) {
        Some(runs) if runs.len() == 1 || 2 * runs.len() <= delta.count => {
            let mut groups = GroupEmit::new(plan, &mb, trace_out_cursor, avi, runs.len());
            for run in runs.iter() {
                accs.iter_mut().for_each(Accumulator::reset);
                // An extreme recedes only on a retraction, so an all-insert group's
                // extremes are its rows' and its stored row's: no probe.
                let prestep = indexed && run.len() <= PRESTEP_CAP && !weights[run.clone()].iter().any(retracts);
                Accumulator::fold_rows(&mut accs, &mb, run.clone(), prestep);
                groups.emit(run.start, &mut accs, indexed && !prestep);
            }
            return groups.finish();
        }
        Some(runs) => GroupOrdinals::of_runs(&runs),
        None => shape.key.numbered(delta),
    };

    let mut states: Vec<GroupedState> = shape.acc_template.iter().map(Accumulator::grouped).collect();
    let whole = [(0, delta.count)];
    for (acc, state) in shape.acc_template.iter().zip(&mut states) {
        state.resize(groups.len(), acc);
        acc.fold_grouped(&mb, &whole, &groups.ord, state);
    }
    // Per group, whether its extremes come off the index: too many rows to have
    // pre-stepped, or a retraction among them.
    let mut probes = vec![false; if indexed { groups.len() } else { 0 }];
    if indexed {
        let mut rows = vec![0u32; groups.len()];
        for (&g, w) in groups.ord.iter().zip(weights) {
            let g = g as usize;
            rows[g] += 1;
            probes[g] |= retracts(w) || rows[g] as usize > PRESTEP_CAP;
        }
    }

    let mut emit = GroupEmit::new(plan, &mb, trace_out_cursor, avi, groups.len());
    for &g in &groups.by_pk {
        let g = g as usize;
        for (acc, state) in accs.iter_mut().zip(&mut states) {
            state.take(g, acc);
        }
        emit.emit(groups.first[g] as usize, &mut accs, indexed && probes[g]);
    }
    emit.finish()
}

/// The output of one `op_reduce` call under construction: each group's folded
/// delta is merged with its history and written as a retraction and a new row.
struct GroupEmit<'a, 'c> {
    plan: &'a ReducePlan,
    delta: &'a MemBatch<'a>,
    trace_out: &'c mut ReadCursor,
    avi: Option<(&'a AviBake, &'c mut ReadCursor)>,
    groups: usize,
    out: Batch,
}

impl<'a, 'c> GroupEmit<'a, 'c> {
    fn new(
        plan: &'a ReducePlan,
        delta: &'a MemBatch<'a>,
        trace_out: &'c mut ReadCursor,
        avi: Option<(&'a AviBake, &'c mut ReadCursor)>,
        groups: usize,
    ) -> Self {
        // A group emits at most its retraction and its new row.
        let out = Batch::with_capacity(&plan.shape.output_schema, 2 * groups);
        GroupEmit { plan, delta, trace_out, avi, groups, out }
    }

    /// Emit the group of delta row `first`, whose delta `accs` hold. Under
    /// `probe` its extremes are read off the value index, whatever `accs` hold
    /// for them. Groups arrive in ascending output-PK order.
    #[inline(always)]
    fn emit(&mut self, first: usize, accs: &mut [Accumulator], probe: bool) {
        let shape = &self.plan.shape;
        let out_pk = shape.key.out_pk(self.delta, first);
        let out_pk_bytes: &[u8] = out_pk.bytes();

        let mark = self.out.mark();
        if self.trace_out.seek_pk_group_ascending(out_pk_bytes) {
            // −Agg(history) is the stored row, copied byte-identical at −1.
            self.trace_out.copy_current_row_into(&mut self.out, -1);
            let (stored_row, stored_idx) = self.trace_out.current_row_source();
            for acc in accs.iter_mut().filter(|a| a.is_linear() || !probe) {
                acc.fold_stored(stored_row, stored_idx);
            }
        }
        if probe {
            if let Some((bake, cursor)) = &mut self.avi {
                bake.seed_extremes(cursor, self.delta, first, accs);
            }
        }

        let cardinality = accs[self.plan.cardinality].count_value();
        debug_assert!(
            cardinality >= 0,
            "reduce input must be bag-positive: negative group cardinality"
        );
        if cardinality > 0 {
            let group = Some((self.delta, first, shape.key.carried()));
            emit_reduce_row(&mut self.out, group, out_pk_bytes, accs);
        } else if self.plan.seeds_ground {
            // An emptied global aggregate still publishes one row.
            emit_reduce_row(&mut self.out, None, out_pk_bytes, &shape.acc_template);
        }
        consolidate_group(&mut self.out, &shape.output_schema, mark);
    }

    fn finish(mut self) -> Batch {
        gnitz_debug!(
            "op_reduce: in={} groups={} out={}",
            self.delta.count,
            self.groups,
            self.out.count
        );
        self.out.certify_consolidated();
        self.out
    }
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
