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

use super::agg::{Accumulator, BulkStep};

use super::emit::emit_reduce_row;
use super::plan::{build_reduce_output_schema, ReduceShape};
use crate::schema::{SchemaDescriptor, SchemaFacts};
use crate::storage::{Batch, StoreError};

/// The request-scoped fold state.
pub(crate) struct AdhocFold {
    shape: ReduceShape,
    /// Row `ord` is group `ord`'s key and group columns; a global fold's one
    /// group is row 0 from [`Self::new`].
    groups: Batch,
    /// Group `ord` owns `accs[ord * n_aggs .. (ord + 1) * n_aggs]`.
    accs: Vec<Accumulator>,
    /// A global fold's [`BulkStep`] per accumulator, resolved in [`Self::new`];
    /// empty for a grouped fold.
    bulk: Vec<BulkStep>,
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
        let refuse = |e| StoreError::rejected(format!("scan_spec fold: {e}"));
        let shape = ReduceShape::for_fold(src_schema, &agg.group_cols, &agg.aggs).map_err(refuse)?;
        let group_schema =
            build_reduce_output_schema(src_schema, &agg.group_cols, &[], shape.key.kind).map_err(refuse)?;
        let mut groups = Batch::empty_with_schema(&group_schema);
        let (mut accs, mut bulk) = (Vec::new(), Vec::new());
        if shape.is_global() {
            // A global fold's one group exists over no input: every worker emits it.
            emit_reduce_row(&mut groups, None, shape.key.ground_pk().bytes(), &[]);
            accs.extend_from_slice(&shape.acc_template);
            bulk.extend(accs.iter().map(Accumulator::bulk_step));
        }
        Ok(AdhocFold {
            accs,
            bulk,
            groups,
            shape,
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
            groups,
            accs,
            bulk,
            by_key,
            last,
            group_cap,
            ..
        } = self;
        let mb = chunk.as_mem_batch();
        if shape.is_global() {
            for (acc, step) in accs.iter_mut().zip(bulk.iter()) {
                for &(s, e) in ranges {
                    step(acc, &mb, s..e);
                }
            }
            return Ok(());
        }
        let n_aggs = shape.acc_template.len();
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            let w = mb.get_weight(row);
            debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
            let key = shape.key.group.key_row(&mb, row);
            let ord = match *last {
                Some((k, ord)) if k == key => ord,
                _ => {
                    let ord = match by_key.entry(key) {
                        std::collections::hash_map::Entry::Occupied(e) => *e.get(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            let ord = groups.count;
                            if ord >= *group_cap {
                                return Err(StoreError::rejected(format!(
                                    "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                                     CREATE VIEW to maintain this aggregation incrementally"
                                )));
                            }
                            emit_reduce_row(
                                groups,
                                Some((&mb, row, shape.key.exemplar_locs())),
                                shape.key.narrow_pk(key).bytes(),
                                &[],
                            );
                            accs.extend_from_slice(&shape.acc_template);
                            *e.insert(ord as u32)
                        }
                    };
                    *last = Some((key, ord));
                    ord
                }
            } as usize;
            for acc in &mut accs[ord * n_aggs..(ord + 1) * n_aggs] {
                acc.step_from_batch(&mb, row, w);
            }
        }
        Ok(())
    }

    /// Emit one partial reduce-output row per present group (weight +1), in the
    /// synthetic-fold reply layout and group-discovery order.
    pub(crate) fn finish(self) -> Batch {
        let n_aggs = self.shape.acc_template.len();
        let gs = self.groups.schema();
        let carried = gs.payload_locators();
        let mut output = Batch::with_capacity(&self.shape.output_schema, self.groups.count);
        let groups_mb = self.groups.as_mem_batch();
        for ord in 0..self.groups.count {
            emit_reduce_row(
                &mut output,
                Some((&groups_mb, ord, &carried)),
                groups_mb.get_pk_bytes(ord),
                &self.accs[ord * n_aggs..(ord + 1) * n_aggs],
            );
        }
        output
    }
}

#[cfg(test)]
#[path = "tests/adhoc_fold.rs"]
mod tests;
