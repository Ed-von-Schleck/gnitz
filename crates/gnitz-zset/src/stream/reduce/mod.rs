//! Incremental reduce: δ_out = Agg(history + δ_in) − Agg(history). `ReducePlan`
//! holds what `op_reduce` needs at emit time: the `ReduceShape` the ad-hoc fold
//! shares, and the value index its MIN/MAX read their history from.

mod value_index;

#[cfg(test)]
#[path = "benches/reduce.rs"]
mod bench;

#[cfg(test)]
#[path = "tests/reduce.rs"]
mod tests;

use std::cmp::Ordering;

use crate::algebra::{ground_pk, Agg, AggValues, GroupOrdinals, RangeGroups, ReduceShape};
use crate::repr::{Batch, MemBatch, RowMark};
use crate::schema::payload_order::{Generic, PayloadOrder};
use crate::schema::SchemaDescriptor;
use crate::stream::OpenAt;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;
use value_index::ValueIndex;

/// The baked per-instruction circuit reduce plan.
pub struct ReducePlan {
    shape: ReduceShape,
    /// The row a global aggregate publishes over an empty input; `None` for a
    /// reduce that owes none.
    ground: Option<Batch>,
    /// Position of the COUNT(*) holding a group's net row count.
    count_star: usize,
    /// The value index the non-linear aggregates read their history from.
    value_index: Option<ValueIndex>,
}

impl ReducePlan {
    /// `Err` for a shape `ReduceShape` refuses and a reduce without a COUNT(*).
    pub fn from_wire(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
        seeds_ground: bool,
    ) -> Result<Self, String> {
        Self::new(input, group_cols, aggs, count_star(aggs)?, seeds_ground)
    }

    /// One worker's share of a global reduce over `aggs`, or `None` for a float
    /// sum, whose value depends on the order a split would change.
    pub fn partial(input: &SchemaDescriptor, aggs: &[AggDescriptor]) -> Result<Option<Self>, String> {
        // No ground row: a worker with no rows contributes no partial.
        let plan = Self::from_wire(input, &[], aggs, false)?;
        let splits = |agg: &Agg| agg.is_exact_linear() || !agg.is_linear();
        Ok(plan.shape.aggs.iter().all(splits).then_some(plan))
    }

    /// The global reduce over relayed [`Self::partial`] outputs `[V₀ | aggregates…]`,
    /// each aggregate folding its own column by its [`AggFunc::merge_op`].
    pub fn combine(partials: &SchemaDescriptor, aggs: &[AggDescriptor], seeds_ground: bool) -> Result<Self, String> {
        let merged: Vec<AggDescriptor> = aggs
            .iter()
            .zip(1..)
            .map(|(d, col_idx)| AggDescriptor { col_idx, agg_op: d.agg_op.merge_op() })
            .collect();
        // A COUNT merges as a SUM, so its position is read off `aggs`.
        let plan = Self::new(partials, &[], &merged, count_star(aggs)?, seeds_ground)?;
        debug_assert_eq!(plan.shape.output_schema, *partials);
        Ok(plan)
    }

    fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
        count_star: usize,
        seeds_ground: bool,
    ) -> Result<Self, String> {
        debug_assert!(
            !seeds_ground || group_cols.is_empty(),
            "the ground row carries no group columns"
        );
        let (shape, _) = ReduceShape::new(input, group_cols, aggs)?;
        let value_index = ValueIndex::new(input, group_cols, &shape.aggs)?;
        let ground = seeds_ground.then(|| {
            let mut v0 = Batch::with_capacity(&shape.output_schema, 1);
            let values = AggValues::new(&shape.aggs, 1);
            values.emit_row(&mut v0, None, ground_pk().bytes(), 0);
            v0
        });
        Ok(ReducePlan { shape, ground, count_star, value_index })
    }

    /// The row a global aggregate publishes over an empty input; `None` for a
    /// reduce that owes none.
    pub fn ground_row(&self) -> Option<&Batch> {
        self.ground.as_ref()
    }

    /// The layout `op_reduce` emits under this plan.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.shape.output_schema
    }

    /// The layout of the value index a MIN/MAX of this reduce is maintained
    /// through; `None` for a reduce without one.
    pub fn index_schema(&self) -> Option<&SchemaDescriptor> {
        self.value_index.as_ref().map(|index| &index.schema)
    }

    /// The index entries `delta` contributes, each at its row's weight,
    /// unsorted; `None` for a reduce without a value index.
    pub fn index_entries(&self, delta: &Batch) -> Option<Batch> {
        self.value_index.as_ref().map(|index| index.entries(delta))
    }

    /// True iff every aggregate is a count or an integer sum, whose value is the
    /// same whatever the row order and the partition.
    pub fn is_exact_linear(&self) -> bool {
        self.shape.aggs.iter().all(Agg::is_exact_linear)
    }
}

/// The position of the COUNT(*) holding a group's net row count: only it tells
/// an emptied group from a live one.
fn count_star(aggs: &[AggDescriptor]) -> Result<usize, String> {
    aggs.iter()
        .position(|d| d.agg_op == AggFunc::Count)
        .ok_or_else(|| "reduce: a circuit reduce needs a COUNT(*)".to_string())
}

/// In a delta standing in group order, a run of more rows than this reads its
/// extremes off the value index instead of stepping its rows: one seek in place
/// of stepping every row.
const RUN_STEP_CAP: usize = 128;

/// One epoch of the reduce, its output consolidated.
///
/// The delta is folded a column at a time, each aggregate over its own column:
/// over row ranges where the delta already stands in group order and its groups
/// are long enough to pay for a range each, else over one group ordinal per row.
///
/// `out_trace` opens the output's history, at the groups the delta touches.
/// `index` opens the value index `plan.value_index` describes, this epoch's
/// entries included, at the groups whose extremes are read off it; a delta with
/// none leaves it unopened.
pub fn op_reduce(delta: &Batch, out_trace: OpenAt<'_>, index: Option<OpenAt<'_>>, plan: &ReducePlan) -> Batch {
    let shape = &plan.shape;

    debug_assert!(plan.is_exact_linear() || delta.is_consolidated());

    if delta.is_empty() {
        return Batch::empty_with_schema(&shape.output_schema);
    }

    let mb = delta.as_mem_batch();
    let weights = mb.weight().as_chunks::<8>().0;
    let retracts = |w: &[u8; 8]| i64::from_le_bytes(*w) <= 0;
    let indexed = plan.value_index.is_some();
    let aggs = &shape.aggs;

    let groups = match shape.key.runs(delta) {
        Some(ranges) if ranges.len() == 1 || 2 * ranges.len() <= delta.len() => {
            // An extreme recedes only on a retraction, so an all-insert group's
            // extremes are its rows' and its stored row's: no probe.
            let probes: Vec<bool> = ranges
                .iter()
                .map(|&(s, e)| indexed && (e - s > RUN_STEP_CAP || weights[s..e].iter().any(retracts)))
                .collect();
            // A probed run's extremes come off the index: an extreme folds the
            // runs with each of those emptied, so it steps none of its rows.
            let unprobed: Vec<(usize, usize)>;
            let stepped: &[(usize, usize)] = match probes.contains(&true) {
                true => {
                    let keep = |(&(s, e), &probe): (&(usize, usize), &bool)| (s, if probe { s } else { e });
                    unprobed = ranges.iter().zip(&probes).map(keep).collect();
                    &unprobed
                }
                false => &ranges,
            };
            let mut vals = AggValues::new(aggs, ranges.len());
            for (k, agg) in aggs.iter().enumerate() {
                let folded = if agg.is_linear() { &ranges[..] } else { stepped };
                vals.fold(k, &mb, folded, RangeGroups::EACH);
            }
            let order = ranges.iter().enumerate().map(|(g, &(first, _))| (first, g));
            return emit_groups(plan, &mb, out_trace, index, vals, &probes, order);
        }
        Some(runs) => GroupOrdinals::of_runs(&runs),
        None => shape.key.numbered(delta),
    };

    let mut vals = AggValues::new(aggs, groups.len());
    for k in 0..aggs.len() {
        vals.fold(k, &mb, &[(0, delta.len())], &groups.ord[..]);
    }
    // Per group, whether its extremes come off the index. Every row is stepped
    // whatever its group then reads, so only a retraction sends one there.
    let mut probes = vec![false; groups.len()];
    if indexed {
        for (&g, w) in groups.ord.iter().zip(weights) {
            probes[g as usize] |= retracts(w);
        }
    }
    let order = groups
        .by_pk
        .iter()
        .map(|&g| (groups.first[g as usize] as usize, g as usize));
    emit_groups(plan, &mb, out_trace, index, vals, &probes, order)
}

/// Emit each group of `order` merged with its history, as a retraction and a
/// new row. `order` yields a delta row of each group and the group's index in
/// `vals`, in ascending output-PK order. A group `g` with `probes[g]` reads its
/// extremes off the value index, whatever `vals` holds for them.
fn emit_groups(
    plan: &ReducePlan,
    delta: &MemBatch<'_>,
    out_trace: OpenAt<'_>,
    index: Option<OpenAt<'_>>,
    mut vals: AggValues,
    probes: &[bool],
    order: impl DoubleEndedIterator<Item = (usize, usize)> + ExactSizeIterator + Clone,
) -> Batch {
    let shape = &plan.shape;
    let key = &shape.key;
    let row = |end: Option<(usize, usize)>| end.expect("a delta with a row has a group").0;
    let (lo, hi) = (row(order.clone().next()), row(order.clone().next_back()));
    let mut out_trace = out_trace(key.out_pk(delta, lo).bytes(), key.out_pk(delta, hi).bytes());
    let mut value_index = plan.value_index.as_ref().and_then(|value_index| {
        let probed = order.clone().filter(|&(_, g)| probes[g]).map(|(row, _)| row);
        let (lo, hi) = value_index.group_span(delta, probed)?;
        let open = index.expect("a value-indexed reduce is handed its index");
        Some((value_index, open(lo.pk_bytes(), hi.pk_bytes())))
    });
    let groups = order.len();
    // A group emits at most its retraction and its new row.
    let mut out = Batch::with_capacity(&shape.output_schema, 2 * groups);

    for (first, g) in order {
        let out_pk = key.out_pk(delta, first);
        let out_pk_bytes: &[u8] = out_pk.bytes();
        let probed = value_index.as_mut().filter(|_| probes[g]);

        let mark = out.mark();
        if out_trace.seek_pk_group_ascending(out_pk_bytes) {
            // −Agg(history) is the stored row, copied byte-identical at −1.
            out_trace.copy_current_row_into(&mut out, -1);
            let (stored_row, stored_idx) = out_trace.current_row_source();
            vals.fold_stored(g, stored_row, stored_idx, probed.is_none());
        }
        if let Some((value_index, cursor)) = probed {
            value_index.seed_extremes(cursor, delta, first, &mut vals, g);
        }

        let cardinality = vals.count_value(plan.count_star, g);
        debug_assert!(
            cardinality >= 0,
            "reduce input must be bag-positive: negative group cardinality"
        );
        if cardinality > 0 {
            let group = Some((delta, first, key.carried()));
            vals.emit_row(&mut out, group, out_pk_bytes, g);
        } else if let Some(v0) = &plan.ground {
            // An emptied global aggregate still publishes one row.
            out.append_batch(v0);
        }
        consolidate_group(&mut out, mark);
    }

    gnitz_debug!("op_reduce: in={} groups={groups} out={}", delta.count, out.len());
    out.certify_consolidated();
    out
}

/// Consolidate the rows one group wrote since `mark`: at most a retraction and
/// a new row, under one PK.
fn consolidate_group(out: &mut Batch, mark: RowMark) {
    if out.rows_since(mark) != 2 {
        return;
    }
    let row0 = out.len() - 2;
    let omb = out.as_mem_batch();
    match Generic.compare(out.schema(), &omb, row0, &omb, row0 + 1) {
        Ordering::Equal => out.truncate_to(mark),
        Ordering::Greater => out.swap_rows(row0, row0 + 1),
        Ordering::Less => {}
    }
}
