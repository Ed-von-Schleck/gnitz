//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No circuit, no operator traces, no exchange: each
//! surviving scan chunk folds into per-group accumulators in bounded RAM, then
//! one partial reduce-output batch is emitted.
//!
//! It runs the same `GroupOutKey`, accumulators and `emit_reduce_row` as a
//! view's reduce, so its output is that reduce's output over the same group set.
//!
//! The scan cursor delivers consolidated, positive-weight rows, so there is no
//! retraction arithmetic and every present group has a positive cardinality:
//! a grouped fold emits one partial per present group, a global fold its one
//! row even over no input, and neither needs a COUNT(*) emission gate.

use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;

use gnitz_wire::AggReadSpec;

use super::super::group_key::{ground_pk, GroupOutKey, IdentityLoop};
use super::agg::Accumulator;
use super::emit::emit_reduce_row;
use super::plan::ReduceShape;
use crate::schema::{SchemaDescriptor, SchemaFacts};
use crate::storage::{Batch, MemBatch};

/// The request-scoped fold state.
pub(crate) struct AdhocFold {
    shape: ReduceShape,
    /// Row `ord` is group `ord`'s key and group columns; a global fold's one
    /// group is row 0 from [`Self::new`].
    groups: Batch,
    /// Group `ord` owns `accs[ord * n_aggs .. (ord + 1) * n_aggs]`.
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
    pub(crate) fn new(src_schema: &SchemaDescriptor, agg: &AggReadSpec, group_cap: usize) -> Result<Self, String> {
        let refuse = |e| format!("scan_spec fold: {e}");
        let (key, prefix) =
            GroupOutKey::new(src_schema, &agg.group_cols, agg.group_cols.iter().copied()).map_err(refuse)?;
        let mut groups = Batch::empty_with_schema(&prefix.finish());
        let shape = ReduceShape::new(src_schema, key, prefix, &agg.aggs).map_err(refuse)?;
        let mut accs = Vec::new();
        if shape.key.is_global() {
            // A global fold's one group exists over no input: every worker emits it.
            groups.push_key_row(ground_pk().bytes(), 1);
            accs.extend_from_slice(&shape.acc_template);
        }
        Ok(AdhocFold {
            accs,
            groups,
            shape,
            by_key: FxHashMap::default(),
            last: None,
            group_cap,
        })
    }

    /// The layout [`Self::finish`] emits.
    pub(crate) fn output_schema(&self) -> &SchemaDescriptor {
        &self.shape.output_schema
    }

    /// Fold the surviving `[start, end)` row ranges of one source chunk into the
    /// group state. `Err` past the per-worker group cap.
    pub(crate) fn fold_ranges(&mut self, chunk: &Batch, ranges: &[(usize, usize)]) -> Result<(), String> {
        let Self {
            shape,
            groups,
            accs,
            by_key,
            last,
            group_cap,
        } = self;
        let mb = chunk.as_mem_batch();
        if shape.key.is_global() {
            for &(s, e) in ranges {
                Accumulator::fold_rows(accs, &mb, s..e);
            }
            return Ok(());
        }
        let rows = FoldRows {
            shape,
            groups,
            accs,
            by_key,
            last,
            group_cap: *group_cap,
            mb: &mb,
            ranges,
        };
        shape.key.with_identity(&mb, rows)
    }

    /// One partial row per present group (weight +1), in group-discovery order.
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

/// One chunk's surviving rows, folded into the group state.
struct FoldRows<'a> {
    shape: &'a ReduceShape,
    groups: &'a mut Batch,
    accs: &'a mut Vec<Accumulator>,
    by_key: &'a mut FxHashMap<u128, u32>,
    last: &'a mut Option<(u128, u32)>,
    group_cap: usize,
    mb: &'a MemBatch<'a>,
    ranges: &'a [(usize, usize)],
}

impl IdentityLoop for FoldRows<'_> {
    type Out = Result<(), String>;

    fn run(self, identity: impl Fn(usize) -> u128) -> Self::Out {
        let FoldRows {
            shape,
            groups,
            accs,
            by_key,
            last,
            group_cap,
            mb,
            ranges,
        } = self;
        let n_aggs = shape.acc_template.len();
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            let w = mb.get_weight(row);
            debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
            let key = identity(row);
            let ord = match *last {
                Some((k, ord)) if k == key => ord,
                _ => {
                    let ord = match by_key.entry(key) {
                        Entry::Occupied(e) => *e.get(),
                        Entry::Vacant(e) => {
                            let ord = groups.count;
                            if ord >= group_cap {
                                return Err(format!(
                                    "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                                     CREATE VIEW to maintain this aggregation incrementally"
                                ));
                            }
                            emit_reduce_row(
                                groups,
                                Some((mb, row, shape.key.carried())),
                                shape.key.out_pk(mb, row).bytes(),
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
                acc.step_from_batch(mb, row, w);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/adhoc_fold.rs"]
mod tests;
