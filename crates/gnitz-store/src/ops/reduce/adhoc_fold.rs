//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No DBSP circuit, no operator-trace tables, no
//! exchange: each surviving scan chunk folds into per-group accumulators in
//! bounded RAM, then one partial reduce-output batch is emitted after full
//! accumulation.
//!
//! Semantics parity with a CREATE VIEW of the same statement is **structural**,
//! not reimplemented: the accumulation kernel (`Accumulator::step_from_batch`),
//! the group key (`GroupKeyCols`, whose 128-bit key is a group's identity here
//! as in a view), and the emission path (`emit_reduce_row` driven by a
//! `ReduceShape`) are the exact shared code a view's reduce runs, so partial agg
//! columns are byte-identical to a view's reduce output.
//!
//! Exactness over the committed base: the scan cursor delivers consolidated,
//! positive-net-weight rows (ghosts excluded, base tables DML-forced
//! non-negative), so with no history there is no retraction arithmetic — the
//! accumulator over such rows *is* the aggregate. The fold has no `should_emit`
//! gate: a grouped fold emits one partial per present group (over positive
//! weights a present group always has net cardinality > 0, matching the view),
//! and a global fold its one row, over no input included. It carries no COUNT(*)
//! cardinality companion — that is a `should_emit` signal the stateless fold does
//! not need.

use rustc_hash::FxHashMap;

use gnitz_wire::AggReadSpec;

use super::agg::Accumulator;

use super::emit::emit_reduce_row;
use super::plan::ReduceShape;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, StoreError};

/// The request-scoped fold state.
pub(crate) struct AdhocFold {
    shape: ReduceShape,
    /// Grouped folds only: one representative source row per group, in
    /// group-discovery order; the row index IS the group ordinal (and
    /// `rep_rows.count` the group count).
    /// `emit_reduce_row` reads the group columns from it, and the group key is
    /// re-derived from it at `finish` (a pure function of the group columns).
    rep_rows: Batch,
    /// Flat accumulator matrix: group `ord` owns
    /// `accs[ord * n_aggs .. (ord + 1) * n_aggs]`.
    accs: Vec<Accumulator>,
    /// Group key → group ordinal.
    by_key: FxHashMap<u128, u32>,
    /// Same-group memo: the previous row's `(key, ordinal)`. Consecutive rows
    /// of one group — per cluster, when the scan is ordered by the group
    /// column — resolve without a map probe.
    last: Option<(u128, u32)>,
    group_cap: usize,
}

impl AdhocFold {
    /// The fold of `agg` over `src_schema`, refused for a spec the schema cannot
    /// serve.
    pub(crate) fn new(src_schema: &SchemaDescriptor, agg: &AggReadSpec, group_cap: usize) -> Result<Self, StoreError> {
        let shape = ReduceShape::for_fold(src_schema, &agg.group_cols, &agg.aggs)
            .map_err(|e| StoreError::rejected(format!("scan_spec fold: {e}")))?;
        Ok(AdhocFold {
            accs: if shape.is_global() {
                shape.acc_template.clone()
            } else {
                Vec::new()
            },
            shape,
            rep_rows: Batch::empty_with_schema(src_schema),
            by_key: FxHashMap::default(),
            last: None,
            group_cap,
        })
    }

    /// The partial reduce-output layout [`Self::finish`] emits.
    pub(crate) fn output_schema(&self) -> &SchemaDescriptor {
        &self.shape.output_schema
    }

    /// Fold every `[start, end)` row range of one source chunk into the group
    /// state — the caller passes the filter's surviving row ranges directly, so
    /// no survivor batch is materialized. `Err` on exceeding the per-worker group
    /// cap.
    ///
    /// The whole list rather than one range: a fragmented survivor list would
    /// otherwise repay the per-call setup — the state destructure, the keyer and
    /// the batch view — once per range instead of once per chunk.
    pub(crate) fn fold_ranges(&mut self, chunk: &Batch, ranges: &[(usize, usize)]) -> Result<(), StoreError> {
        let Self {
            shape,
            rep_rows,
            accs,
            by_key,
            last,
            group_cap,
        } = self;
        let n_aggs = shape.acc_template.len();
        let mb = chunk.as_mem_batch();
        if shape.is_global() {
            // Global aggregate: its one group's accumulators exist from `new` — no
            // per-row key hash, memo, or map probe.
            for row in ranges.iter().flat_map(|&(s, e)| s..e) {
                let w = mb.get_weight(row);
                debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
                if w <= 0 {
                    continue;
                }
                for acc in &mut accs[..n_aggs] {
                    acc.step_from_batch(&mb, row, w);
                }
            }
            return Ok(());
        }
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            let w = mb.get_weight(row);
            // The scan cursor delivers consolidated, positive-net-weight rows
            // (ghosts excluded); the kernel's extreme arm asserts the same.
            debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
            if w <= 0 {
                continue;
            }
            let key = shape.key.group.key_row(&mb, row);
            let ord = match *last {
                Some((k, ord)) if k == key => ord,
                _ => {
                    let ord = match by_key.entry(key) {
                        std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            // A new group. The cap is per-worker (a worker sees
                            // a subset of the global groups), so it never fires
                            // when the global group count ≤ cap; firing is a
                            // stated resource-exhaustion abort, not silent
                            // degradation.
                            let ord = rep_rows.count;
                            if ord >= *group_cap {
                                return Err(StoreError::rejected(format!(
                                    "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                                     CREATE VIEW to maintain this aggregation incrementally"
                                )));
                            }
                            // Append onto the flat matrix, never a fresh `Vec` per
                            // group: the tail grows amortized.
                            rep_rows.append_batch(chunk, row, row + 1);
                            accs.extend_from_slice(&shape.acc_template);
                            *e.insert(ord as u32)
                        }
                    };
                    *last = Some((key, ord));
                    ord
                }
            };
            let ord = ord as usize;
            for acc in &mut accs[ord * n_aggs..(ord + 1) * n_aggs] {
                acc.step_from_batch(&mb, row, w);
            }
        }
        Ok(())
    }

    /// Emit one partial reduce-output row per present group (weight +1), in the
    /// synthetic-fold reply layout and group-discovery order — or a global fold's
    /// one row, which a worker that saw no rows emits as the ground row.
    pub(crate) fn finish(self) -> Batch {
        let shape = &self.shape;
        let n_aggs = shape.acc_template.len();
        // The exact output row count is the group count — reserve once.
        let mut output = Batch::with_capacity(&shape.output_schema, self.rep_rows.count.max(1));
        if shape.is_global() {
            emit_reduce_row(&mut output, None, shape.key.ground_pk().bytes(), &self.accs, shape);
            return output;
        }
        let rep_mb = self.rep_rows.as_mem_batch();
        for ord in 0..self.rep_rows.count {
            // Synthetic `_group_pk`, off the retained representative row — a pure
            // function of its group columns.
            let pk = shape.key.out_pk(&rep_mb, ord);
            emit_reduce_row(
                &mut output,
                Some((&rep_mb, ord)),
                pk.bytes(),
                &self.accs[ord * n_aggs..(ord + 1) * n_aggs],
                shape,
            );
        }
        output
    }
}

#[cfg(test)]
#[path = "tests/adhoc_fold.rs"]
mod tests;
