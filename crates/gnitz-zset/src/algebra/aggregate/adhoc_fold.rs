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

use gnitz_wire::AggReadSpec;

use super::agg::{Accumulator, GroupedState};
use super::emit::emit_reduce_row;
use super::shape::ReduceShape;
use crate::algebra::group_key::{ground_pk, GroupNumbers, GroupOutKey, IdentityLoop};
use crate::repr::{Batch, MemBatch};
use crate::schema::{SchemaDescriptor, SchemaFacts};

/// The request-scoped fold state.
pub(crate) struct AdhocFold {
    shape: ReduceShape,
    /// Row `ord` is group `ord`'s key and group columns; a global fold's one
    /// group is row 0 from [`Self::new`].
    groups: Batch,
    /// A global fold's one group. A grouped fold holds [`Self::states`] instead.
    global: Vec<Accumulator>,
    /// Per aggregate, its running value for every group of `groups`.
    states: Vec<GroupedState>,
    /// Group key → group ordinal.
    numbers: GroupNumbers,
    /// The group ordinal of each surviving row of the chunk being folded.
    ord: Vec<u32>,
    group_cap: usize,
}

impl AdhocFold {
    /// The fold of `agg` over `src_schema`, refused for a spec the schema cannot
    /// serve.
    pub(crate) fn new(src_schema: &SchemaDescriptor, agg: &AggReadSpec, group_cap: usize) -> Result<Self, String> {
        let refuse = |e| format!("scan_spec fold: {e}");
        let (key, prefix) =
            GroupOutKey::new(src_schema, &agg.group_cols, agg.group_cols.iter().copied()).map_err(refuse)?;
        let mut groups = Batch::empty_with_schema(&prefix.finish().map_err(refuse)?);
        let shape = ReduceShape::new(src_schema, key, prefix, &agg.aggs).map_err(refuse)?;
        let mut global = Vec::new();
        if shape.key.is_global() {
            // A global fold's one group exists over no input: every worker emits it.
            groups.push_key_row(ground_pk().bytes(), 1);
            global.extend_from_slice(&shape.acc_template);
        }
        Ok(AdhocFold {
            states: shape.acc_template.iter().map(Accumulator::grouped).collect(),
            global,
            groups,
            shape,
            numbers: GroupNumbers::default(),
            ord: Vec::new(),
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
            global,
            states,
            numbers,
            ord,
            group_cap,
        } = self;
        let mb = chunk.as_mem_batch();
        if shape.key.is_global() {
            for &(s, e) in ranges {
                Accumulator::fold_rows(global, &mb, s..e, true);
            }
            return Ok(());
        }
        // Each row's group first, then each aggregate over its own column.
        ord.clear();
        let assign = AssignGroups {
            shape,
            groups,
            numbers,
            ord,
            group_cap: *group_cap,
            mb: &mb,
            ranges,
        };
        shape.key.with_identity(&mb, assign)?;
        for (acc, state) in shape.acc_template.iter().zip(states) {
            state.resize(groups.count, acc);
            acc.fold_grouped(&mb, ranges, ord, state);
        }
        Ok(())
    }

    /// One partial row per present group (weight +1), in group-discovery order.
    pub(crate) fn finish(mut self) -> Batch {
        let gs = self.groups.schema();
        let carried = gs.payload_locators();
        let mut output = Batch::with_capacity(&self.shape.output_schema, self.groups.count);
        let groups_mb = self.groups.as_mem_batch();
        let grouped = !self.shape.key.is_global();
        let mut accs = match grouped {
            true => self.shape.acc_template.clone(),
            false => self.global,
        };
        for ord in 0..self.groups.count {
            if grouped {
                for (acc, state) in accs.iter_mut().zip(&mut self.states) {
                    state.take(ord, acc);
                }
            }
            emit_reduce_row(
                &mut output,
                Some((&groups_mb, ord, &carried)),
                groups_mb.get_pk_bytes(ord),
                &accs,
            );
        }
        output
    }
}

/// One chunk's surviving rows, each assigned its group's ordinal; a group met
/// for the first time is appended to `groups`.
struct AssignGroups<'a> {
    shape: &'a ReduceShape,
    groups: &'a mut Batch,
    numbers: &'a mut GroupNumbers,
    ord: &'a mut Vec<u32>,
    group_cap: usize,
    mb: &'a MemBatch<'a>,
    ranges: &'a [(usize, usize)],
}

impl IdentityLoop for AssignGroups<'_> {
    type Out = Result<(), String>;

    fn run(self, identity: impl Fn(usize) -> u128) -> Self::Out {
        let AssignGroups {
            shape,
            groups,
            numbers,
            ord,
            group_cap,
            mb,
            ranges,
        } = self;
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            debug_assert!(
                mb.get_weight(row) > 0,
                "adhoc fold: scan cursor must deliver positive weights"
            );
            let g = numbers.ordinal(identity(row), || {
                let g = groups.count;
                if g >= group_cap {
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
                Ok(g as u32)
            })?;
            ord.push(g);
        }
        Ok(())
    }
}
