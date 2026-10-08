//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No circuit, no operator traces, no exchange: each
//! surviving scan chunk folds into per-group aggregate values in bounded RAM, then
//! one partial reduce-output batch is emitted.
//!
//! It runs the same `GroupOutKey`, aggregates and `emit_reduce_row` as a
//! view's reduce, so its output is that reduce's output over the same group set.
//!
//! The scan cursor delivers consolidated, positive-weight rows, so there is no
//! retraction arithmetic and every present group has a positive cardinality:
//! a grouped fold emits one partial per present group, a global fold its one
//! row even over no input, and neither needs a COUNT(*) emission gate.

use gnitz_wire::AggReadSpec;

use super::agg::{AggValues, RangeGroups};
use super::emit::emit_reduce_row;
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
    pub(crate) fn new(src_schema: &SchemaDescriptor, agg: &AggReadSpec, group_cap: usize) -> Result<Self, String> {
        let refuse = |e| format!("scan_spec fold: {e}");
        let (key, prefix) =
            GroupOutKey::new(src_schema, &agg.group_cols, agg.group_cols.iter().copied()).map_err(refuse)?;
        let mut groups = Batch::empty_with_schema(&prefix.finish().map_err(refuse)?);
        let shape = ReduceShape::new(src_schema, key, prefix, &agg.aggs).map_err(refuse)?;
        if shape.key.is_global() {
            // A global fold's one group exists over no input: every worker emits it.
            groups.push_key_row(ground_pk().bytes(), 1);
        }
        Ok(AdhocFold {
            vals: AggValues::new(&shape.aggs, groups.count),
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
            vals,
            numbers,
            ord,
            group_cap,
        } = self;
        let mb = chunk.as_mem_batch();
        if shape.key.is_global() {
            for (k, agg) in shape.aggs.iter().enumerate() {
                vals.fold(k, agg, &mb, ranges, RangeGroups::FIRST);
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
        vals.resize(groups.count);
        for (k, agg) in shape.aggs.iter().enumerate() {
            vals.fold(k, agg, &mb, ranges, &ord[..]);
        }
        Ok(())
    }

    /// One partial row per present group (weight +1), in group-discovery order.
    pub(crate) fn finish(self) -> Batch {
        let gs = self.groups.schema();
        let carried = gs.payload_locators();
        let mut output = Batch::with_capacity(&self.shape.output_schema, self.groups.count);
        let groups_mb = self.groups.as_mem_batch();
        for g in 0..self.groups.count {
            let group = Some((&groups_mb, g, &carried[..]));
            emit_reduce_row(
                &mut output,
                group,
                groups_mb.get_pk_bytes(g),
                &self.shape.aggs,
                &self.vals,
                g,
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
                groups.begin_row(shape.key.out_pk(mb, row).bytes(), 1);
                groups.append_cells_from(0, shape.key.carried(), mb, row);
                groups.commit_row();
                Ok(g as u32)
            })?;
            ord.push(g);
        }
        Ok(())
    }
}
