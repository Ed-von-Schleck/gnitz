//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No circuit, no operator traces, no exchange: each
//! surviving scan chunk folds into per-group aggregate values in bounded RAM, then
//! one partial reduce-output batch is emitted.
//!
//! It runs the same `GroupOutKey`, aggregates and `AggValues::emit_row` as a
//! view's reduce, so its output is that reduce's output over the same group set.
//!
//! The scan cursor delivers consolidated, positive-weight rows, so there is no
//! retraction arithmetic and every present group has a positive cardinality:
//! a grouped fold emits one partial per present group, a global fold its one
//! row even over no input, and neither needs a COUNT(*) emission gate.

use gnitz_wire::GroupAggs;

use super::agg::{AggValues, RangeGroups};
use super::shape::ReduceShape;
use crate::algebra::group_key::{ground_pk, GroupNumbers, GroupOutKey, IdentityLoop};
use crate::repr::{Batch, MemBatch};
use crate::schema::SchemaDescriptor;

/// The request-scoped fold state.
pub(crate) struct AdhocFold {
    shape: ReduceShape,
    /// Row `ord` is group `ord`'s key and group columns; a global fold's one
    /// group is row 0 from [`Self::new`].
    groups: Batch,
    /// Each aggregate's running value for every group of `groups`.
    vals: AggValues,
    /// Group key → group ordinal.
    numbers: GroupNumbers,
    /// The group ordinal of each surviving row of the chunk being folded.
    ord: Vec<u32>,
    group_cap: usize,
}

impl AdhocFold {
    /// The fold of `agg` over `src_schema`, refused for a spec the schema cannot
    /// serve.
    pub(crate) fn new(src_schema: &SchemaDescriptor, agg: &GroupAggs, group_cap: usize) -> Result<Self, String> {
        let (shape, leading) =
            ReduceShape::new(src_schema, &agg.group_cols, &agg.aggs).map_err(|e| format!("fold: {e}"))?;
        let mut groups = Batch::empty_with_schema(&leading);
        if shape.key.is_global() {
            // A global fold's one group exists over no input: every worker emits it.
            groups.push_key_row(ground_pk().bytes(), 1);
        }
        Ok(AdhocFold {
            vals: AggValues::new(&shape.aggs, groups.len()),
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
        let mb = chunk.as_mem_batch();
        if self.shape.key.is_global() {
            for k in 0..self.shape.aggs.len() {
                self.vals.fold(k, &mb, ranges, RangeGroups::FIRST);
            }
            return Ok(());
        }
        // Each row's group first, then each aggregate over its own column.
        self.ord.clear();
        let assign = AssignGroups {
            key: &self.shape.key,
            groups: &mut self.groups,
            numbers: &mut self.numbers,
            ord: &mut self.ord,
            group_cap: self.group_cap,
            mb: &mb,
            ranges,
        };
        self.shape.key.with_identity(&mb, ranges, assign)?;
        self.vals.resize(self.groups.len());
        for k in 0..self.shape.aggs.len() {
            self.vals.fold(k, &mb, ranges, &self.ord[..]);
        }
        Ok(())
    }

    /// One partial row per present group (weight +1), in group-discovery order.
    pub(crate) fn finish(self) -> Batch {
        let carried = self.groups.schema().payload_locators();
        let mut output = Batch::with_capacity(&self.shape.output_schema, self.groups.len());
        let groups_mb = self.groups.as_mem_batch();
        for g in 0..self.groups.len() {
            let group = Some((&groups_mb, g, &carried[..]));
            self.vals.emit_row(&mut output, group, groups_mb.get_pk_bytes(g), g);
        }
        output
    }
}

/// One chunk's surviving rows, each assigned its group's ordinal; a group met
/// for the first time is appended to `groups`.
struct AssignGroups<'a> {
    key: &'a GroupOutKey,
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
            key,
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
                let g = groups.len();
                if g >= group_cap {
                    return Err(format!(
                        "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                         CREATE VIEW to maintain this aggregation incrementally"
                    ));
                }
                groups.begin_row(key.out_pk(mb, row).bytes(), 1);
                groups.append_cells_from(key.carried(), mb, row);
                groups.commit_row();
                Ok(g as u32)
            })?;
            ord.push(g);
        }
        Ok(())
    }
}
