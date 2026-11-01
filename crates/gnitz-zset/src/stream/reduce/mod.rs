//! Incremental reduce: δ_out = Agg(history + δ_in) − Agg(history). `ReducePlan`
//! holds what `op_reduce` needs at emit time: the `ReduceShape` the ad-hoc fold
//! shares, the value index its MIN/MAX read their history from, and the buffers
//! one epoch folds its delta in, kept for the next.

mod value_index;

#[cfg(test)]
#[path = "benches/reduce.rs"]
mod bench;

#[cfg(test)]
#[path = "tests/reduce.rs"]
mod tests;

use std::cmp::Ordering;

use crate::algebra::{ground_pk, vec_bytes, Agg, AggValues, GroupScratch, RangeGroups, ReduceShape};
use crate::repr::{Batch, CursorRow, GroupProbe, MemBatch, ReadCursor};
use crate::schema::SchemaDescriptor;
use crate::stream::OpenAt;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;
use gnitz_wire::RowSource;
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
    /// Boxed, so taking it out for an epoch moves a pointer; `None` until the
    /// first epoch, and after one that grew it past [`SCRATCH_KEEP_BYTES`].
    scratch: Option<Box<ReduceScratch>>,
}

/// The buffers an epoch of [`op_reduce`] fills, kept across epochs so a small
/// delta allocates none of them again. Every epoch overwrites each one it reads.
struct ReduceScratch {
    grouping: GroupScratch,
    /// The delta's groups as row runs, where it stands in group order.
    runs: Vec<(usize, usize)>,
    /// `runs` with each probed run emptied.
    unprobed: Vec<(usize, usize)>,
    /// What the fold leaves [`emit_groups`].
    emit: EmitScratch,
}

/// The buffers [`emit_groups`] reads and fills.
struct EmitScratch {
    vals: AggValues,
    /// Per group, whether its extremes come off the value index; empty for a
    /// reduce without one.
    probes: Vec<bool>,
    /// The stored rows of the groups that have one, in output order.
    stored: Vec<CursorRow>,
    /// Per stored row, its group.
    stored_groups: Vec<u32>,
    /// The output rows, as rows of the stored and fresh rows they copy.
    out_rows: Vec<u32>,
    /// Per new row copied off a stored row, its output row and its group.
    derived: Vec<(u32, u32)>,
}

impl ReduceScratch {
    /// Empty buffers for `shape`'s aggregates; none is allocated yet.
    fn new(shape: &ReduceShape) -> Self {
        ReduceScratch {
            grouping: GroupScratch::default(),
            runs: Vec::new(),
            unprobed: Vec::new(),
            emit: EmitScratch {
                vals: AggValues::new(&shape.aggs, 0),
                probes: Vec::new(),
                stored: Vec::new(),
                stored_groups: Vec::new(),
                out_rows: Vec::new(),
                derived: Vec::new(),
            },
        }
    }

    /// The bytes this holds allocated.
    fn held_bytes(&self) -> usize {
        let e = &self.emit;
        self.grouping.held_bytes()
            + vec_bytes(&self.runs)
            + vec_bytes(&self.unprobed)
            + e.vals.held_bytes()
            + vec_bytes(&e.probes)
            + vec_bytes(&e.stored)
            + vec_bytes(&e.stored_groups)
            + vec_bytes(&e.out_rows)
            + vec_bytes(&e.derived)
    }
}

/// The most bytes of [`ReduceScratch`] a plan keeps after an epoch; an epoch
/// that grew it past this, such as a backfill, drops it.
const SCRATCH_KEEP_BYTES: usize = 1 << 16;

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
            values.emit_row(&mut v0, None, &ground_pk(), 0);
            v0
        });
        Ok(ReducePlan {
            shape,
            ground,
            count_star,
            value_index,
            scratch: None,
        })
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
pub fn op_reduce(delta: &Batch, out_trace: OpenAt<'_>, index: Option<OpenAt<'_>>, plan: &mut ReducePlan) -> Batch {
    debug_assert!(plan.is_exact_linear() || delta.is_consolidated());

    if delta.is_empty() {
        return Batch::empty_with_schema(&plan.shape.output_schema);
    }

    // Taken out for the epoch, so the plan stays shared while the buffers fill.
    let mut scratch = plan
        .scratch
        .take()
        .unwrap_or_else(|| Box::new(ReduceScratch::new(&plan.shape)));
    let out = reduce_epoch(delta, out_trace, index, plan, &mut scratch);
    if scratch.held_bytes() <= SCRATCH_KEEP_BYTES {
        plan.scratch = Some(scratch);
    }
    out
}

/// [`op_reduce`] over a non-empty `delta`, folding it in `s`.
fn reduce_epoch(
    delta: &Batch,
    out_trace: OpenAt<'_>,
    index: Option<OpenAt<'_>>,
    plan: &ReducePlan,
    s: &mut ReduceScratch,
) -> Batch {
    let shape = &plan.shape;
    let mb = delta.as_mem_batch();
    let weights = mb.weight().as_chunks::<8>().0;
    let retracts = |w: &[u8; 8]| i64::from_le_bytes(*w) <= 0;
    let indexed = plan.value_index.is_some();
    let aggs = &shape.aggs;
    let ReduceScratch { grouping, runs, unprobed, emit } = s;
    let EmitScratch { vals, probes, .. } = emit;

    let in_runs = shape.key.runs_into(delta, runs);
    if in_runs && (runs.len() == 1 || 2 * runs.len() <= delta.len()) {
        let ranges = &runs[..];
        // An extreme recedes only on a retraction, so an all-insert group's
        // extremes are its rows' and its stored row's: no probe.
        probes.clear();
        if indexed {
            let probed = |&(s, e): &(usize, usize)| e - s > RUN_STEP_CAP || weights[s..e].iter().any(retracts);
            probes.extend(ranges.iter().map(probed));
        }
        // A probed run's extremes come off the index: an extreme folds the
        // runs with each of those emptied, so it steps none of its rows.
        let stepped: &[(usize, usize)] = match probes.contains(&true) {
            true => {
                let keep = |(&(s, e), &probe): (&(usize, usize), &bool)| (s, if probe { s } else { e });
                unprobed.clear();
                unprobed.extend(ranges.iter().zip(probes.iter()).map(keep));
                &unprobed[..]
            }
            false => ranges,
        };
        vals.reset(ranges.len());
        for (k, agg) in aggs.iter().enumerate() {
            let folded = if agg.is_linear() { ranges } else { stepped };
            vals.fold(k, &mb, folded, RangeGroups::Each);
        }
        let order = ranges.iter().enumerate().map(|(g, &(first, _))| (first, g, None));
        return emit_groups(plan, &mb, out_trace, index, emit, order);
    }
    match in_runs {
        true => grouping.groups.fill_runs(runs),
        false => shape.key.numbered_into(delta, grouping),
    }
    let groups = &grouping.groups;

    vals.reset(groups.len());
    for k in 0..aggs.len() {
        vals.fold(k, &mb, &[(0, delta.len())], &groups.ord[..]);
    }
    // Per group, whether its extremes come off the index. Every row is stepped
    // whatever its group then reads, so only a retraction sends one there.
    probes.clear();
    if indexed {
        probes.resize(groups.len(), false);
        let probed = &mut probes[..];
        for (&g, w) in groups.ord.iter().zip(weights) {
            probed[g as usize] |= retracts(w);
        }
    }
    let order = groups.by_pk.iter().map(|&g| {
        (
            groups.first[g as usize] as usize,
            g as usize,
            groups.ids.get(g as usize).copied(),
        )
    });
    emit_groups(plan, &mb, out_trace, index, emit, order)
}

/// Emit each group of `order` merged with its history, as a retraction and a
/// new row. `order` yields a delta row of each group, the group's index in
/// `s.vals` and the identity its numbering kept for it, if any, in ascending
/// output-PK order. A group `g` with `s.probes[g]` reads its extremes off the
/// value index, whatever `s.vals` holds for them; `s.probes` is empty for a
/// reduce without a value index.
///
/// The retraction is the group's stored row, and its new row equals it in
/// every column but the aggregates, the trailing ones: the two order, and
/// cancel, by the aggregates alone.
fn emit_groups(
    plan: &ReducePlan,
    delta: &MemBatch<'_>,
    out_trace: OpenAt<'_>,
    index: Option<OpenAt<'_>>,
    s: &mut EmitScratch,
    order: impl DoubleEndedIterator<Item = Group> + ExactSizeIterator + Clone,
) -> Batch {
    debug_assert!(
        plan.ground.is_none() || order.len() == 1,
        "a global aggregate has one group"
    );
    let key = &plan.shape.key;
    let row = |end: Option<Group>| end.expect("a delta with a row has a group").0;
    let (lo, hi) = (row(order.clone().next()), row(order.clone().next_back()));
    let mut out_trace = out_trace(key.out_pk(delta, lo).bytes(), key.out_pk(delta, hi).bytes());
    let EmitScratch {
        vals,
        probes,
        stored,
        stored_groups,
        out_rows,
        derived,
    } = s;
    let mut value_index = plan.value_index.as_ref().and_then(|value_index| {
        let probed = order.clone().filter(|&(_, g, _)| probes[g]).map(|(row, _, _)| row);
        let (lo, hi) = value_index.group_span(delta, probed)?;
        let open = index.expect("a value-indexed reduce is handed its index");
        Some((value_index, open(lo.pk_bytes(), hi.pk_bytes())))
    });

    // A trace with no row at the delta's groups holds no group's history.
    let traced = out_trace.is_valid();
    let probe = out_trace.group_probe();
    let mut groups = GroupEmit {
        plan,
        delta,
        probe,
        value_index: &mut value_index,
        vals,
        probes,
    };
    let mut out = match order.len() <= ROW_EPOCH_GROUPS || !traced {
        true => groups.emit_by_row(order, traced),
        false => groups.emit_gathered(order, stored, stored_groups, out_rows, derived),
    };
    out.certify_consolidated();
    out
}

/// Group `g`'s new row against its stored row, `Less` where it has none;
/// `None` where it has no new row. A group has one while it has rows, and the
/// one group of a global aggregate always: emptied, it publishes its values
/// over no rows, to which this resets `vals`.
#[inline(always)]
fn new_row(
    plan: &ReducePlan,
    vals: &mut AggValues,
    g: usize,
    stored: Option<(&impl RowSource, usize)>,
) -> Option<Ordering> {
    let cardinality = vals.count_value(plan.count_star, g);
    debug_assert!(
        cardinality >= 0,
        "reduce input must be bag-positive: negative group cardinality"
    );
    if cardinality == 0 {
        plan.ground.as_ref()?;
        vals.clear();
    }
    Some(stored.map_or(Ordering::Less, |(rows, row)| vals.cmp_stored(g, rows, row)))
}

/// A group of the delta: a row of it, its index in the values and the identity
/// its numbering kept for it, if any.
type Group = (usize, usize, Option<u128>);

/// The most groups an epoch emits a row at a time: up to this, a column
/// kernel's fixed cost is more than its rows' copies cost one by one.
const ROW_EPOCH_GROUPS: usize = 4;

/// What [`emit_groups`] reads each group's history through.
struct GroupEmit<'a, 'b> {
    plan: &'a ReducePlan,
    delta: &'a MemBatch<'b>,
    probe: GroupProbe<'a>,
    value_index: &'a mut Option<(&'a ValueIndex, ReadCursor)>,
    vals: &'a mut AggValues,
    probes: &'a [bool],
}

impl GroupEmit<'_, '_> {
    /// Group `g`, of delta row `first`, seeded from the value index if it reads
    /// its extremes off it: after its stored row is folded, so they are the
    /// index's alone.
    fn seed(&mut self, first: usize, g: usize) {
        if let Some((value_index, cursor)) = self.value_index.as_mut().filter(|_| self.probes[g]) {
            value_index.seed_extremes(cursor, self.delta, first, self.vals, g);
        }
    }

    /// Append the new row of group `g` under `out_pk`, its group columns read
    /// off delta row `first`.
    #[inline(always)]
    fn emit_fresh(&self, out: &mut Batch, out_pk: &[u8], first: usize, g: usize) {
        let group = Some((self.delta, first, self.plan.shape.key.carried()));
        self.vals.emit_row(out, group, out_pk, g);
    }

    /// The groups one at a time: each group's stored row, found where
    /// `traced`, folded and copied off the trace, its new row written off its
    /// delta row.
    fn emit_by_row(&mut self, order: impl ExactSizeIterator<Item = Group>, traced: bool) -> Batch {
        // A group emits at most its retraction and its new row.
        let mut out = Batch::with_capacity(&self.plan.shape.output_schema, 2 * order.len());
        for (first, g, id) in order {
            let out_pk = self.plan.shape.key.out_pk_with(self.delta, first, id);
            let at = traced.then(|| self.probe.find(out_pk.bytes())).flatten();
            if let Some(at) = at {
                let (rows, row) = self.probe.row_at(at);
                self.vals.fold_stored(g, rows, row);
            }
            self.seed(first, g);
            let new = new_row(self.plan, self.vals, g, at.map(|at| self.probe.row_at(at)));
            if new == Some(Ordering::Equal) {
                continue;
            }
            if new == Some(Ordering::Less) {
                self.emit_fresh(&mut out, out_pk.bytes(), first, g);
            }
            if let Some(at) = at {
                self.probe.copy_row_into(at, &mut out, -1);
            }
            if new == Some(Ordering::Greater) {
                self.emit_fresh(&mut out, out_pk.bytes(), first, g);
            }
        }
        out
    }

    /// The groups a column at a time: the stored rows gathered off the trace
    /// in one pass and folded, then each copied out as the retraction and, its
    /// aggregates rewritten, as the new row.
    fn emit_gathered(
        &mut self,
        order: impl ExactSizeIterator<Item = Group> + Clone,
        stored: &mut Vec<CursorRow>,
        stored_groups: &mut Vec<u32>,
        out_rows: &mut Vec<u32>,
        derived: &mut Vec<(u32, u32)>,
    ) -> Batch {
        let key = &self.plan.shape.key;
        stored.clear();
        stored_groups.clear();
        for (first, g, id) in order.clone() {
            if let Some(at) = self.probe.find(key.out_pk_with(self.delta, first, id).bytes()) {
                stored.push(at);
                stored_groups.push(g as u32);
            }
        }
        if stored.is_empty() {
            // Every group is fresh.
            return self.emit_by_row(order, false);
        }

        // The stored rows, then the new rows of the groups with none.
        let mut rows = Batch::with_capacity(&self.plan.shape.output_schema, order.len());
        self.probe.gather_into(stored, &mut rows);
        self.vals.fold_stored_rows(&rows.as_mem_batch(), stored_groups);

        out_rows.clear();
        derived.clear();
        let mut next = 0;
        for (first, g, id) in order {
            // The stored row of the group, if it has one.
            let h = (stored_groups.get(next) == Some(&(g as u32))).then(|| {
                next += 1;
                next as u32 - 1
            });
            self.seed(first, g);
            let mb = rows.as_mem_batch();
            let new = new_row(self.plan, self.vals, g, h.map(|h| (&mb, h as usize)));
            let Some(h) = h else {
                // A fresh group's new row is written whole behind the stored rows.
                if new.is_some() {
                    out_rows.push(rows.len() as u32);
                    let out_pk = key.out_pk_with(self.delta, first, id);
                    self.emit_fresh(&mut rows, out_pk.bytes(), first, g);
                }
                continue;
            };
            match new {
                None => out_rows.push(h),
                Some(Ordering::Equal) => {}
                // The new row is a copy of the stored row, its aggregates
                // rewritten, on its side of the retraction.
                Some(ord) => {
                    derived.push((out_rows.len() as u32 + u32::from(ord.is_gt()), g as u32));
                    out_rows.extend([h, h]);
                }
            }
        }

        let retracted = stored.len() as u32;
        let vals = &*self.vals;
        rows.indexed_rows_then(out_rows, |w| {
            // Every row copies one at +1: a stored row's copy is its
            // retraction, but where it is a new row.
            let weights = w.weight_mut().as_chunks_mut::<8>().0;
            for (cell, &h) in weights.iter_mut().zip(out_rows.iter()) {
                if h < retracted {
                    *cell = (-1i64).to_le_bytes();
                }
            }
            for &(row, _) in derived.iter() {
                weights[row as usize] = 1i64.to_le_bytes();
            }
            vals.emit_at(w, derived.iter().map(|&(row, g)| (row as usize, g as usize)));
        })
    }
}
