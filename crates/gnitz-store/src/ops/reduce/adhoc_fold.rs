//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No circuit, no operator traces, no exchange: each
//! surviving scan chunk folds into per-group accumulators in bounded RAM, then
//! one partial reduce-output batch is emitted.
//!
//! It runs the same accumulators, group key and `emit_reduce_row` as a view's
//! reduce, so its partial aggregate columns are byte-identical to a view's
//! reduce output.
//!
//! The scan cursor delivers consolidated, positive-weight rows, so there is no
//! retraction arithmetic and every present group has a positive cardinality:
//! a grouped fold emits one partial per present group, a global fold its one
//! row even over no input, and neither needs a COUNT(*) emission gate.

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
    /// Grouped folds only: row `ord` is group `ord`'s exemplar source row.
    rep_rows: Batch,
    /// Group `ord` owns `accs[ord * n_aggs .. (ord + 1) * n_aggs]`; a global
    /// fold's one group is ordinal 0.
    accs: Vec<Accumulator>,
    /// Group key → group ordinal.
    by_key: FxHashMap<u128, u32>,
    /// The previous row's `(key, ordinal)`, so a run of one group skips the map.
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

    /// Fold the surviving `[start, end)` row ranges of one source chunk into the
    /// group state. `Err` past the per-worker group cap.
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
        let global = shape.is_global();
        let mb = chunk.as_mem_batch();
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            let w = mb.get_weight(row);
            debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
            let ord = if global {
                0
            } else {
                let key = shape.key.group.key_row(&mb, row);
                match *last {
                    Some((k, ord)) if k == key => ord,
                    _ => {
                        let ord = match by_key.entry(key) {
                            std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                            std::collections::hash_map::Entry::Vacant(e) => {
                                let ord = rep_rows.count;
                                if ord >= *group_cap {
                                    return Err(StoreError::rejected(format!(
                                        "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                                         CREATE VIEW to maintain this aggregation incrementally"
                                    )));
                                }
                                rep_rows.append_batch(chunk, row, row + 1);
                                accs.extend_from_slice(&shape.acc_template);
                                *e.insert(ord as u32)
                            }
                        };
                        *last = Some((key, ord));
                        ord
                    }
                }
            } as usize;
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
