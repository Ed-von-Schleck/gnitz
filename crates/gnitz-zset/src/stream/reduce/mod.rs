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

use crate::algebra::{
    ground_pk, vec_bytes, Agg, AggValues, GroupOrdinals, GroupScratch, OutPk, RangeGroups, ReduceShape,
};
use crate::repr::{Batch, CursorRow, DirectWriter, GroupProbe, MemBatch, ReadCursor};
use crate::schema::SchemaDescriptor;
use crate::stream::OpenAt;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;
use gnitz_wire::RowSource;
use value_index::ValueIndex;

/// The baked per-instruction circuit reduce plan.
pub struct ReducePlan {
    shape: ReduceShape,
    /// Whether this global aggregate publishes a row over an empty input.
    seeds_ground: bool,
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
    /// `runs` with each capped run emptied.
    uncapped: Vec<(usize, usize)>,
    /// `runs` with each probed run emptied.
    unprobed: Vec<(usize, usize)>,
    /// What the fold leaves [`emit_groups`].
    emit: EmitScratch,
}

/// Which of a group's extremes come off the value index.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Probe {
    /// None: an extreme recedes only on a retraction, so an all-insert group's
    /// extremes are its rows' and its stored row's.
    No,
    /// Each one a retraction may have receded past: [`AggValues::recedes`].
    Retracts,
    /// Every one: its rows stepped none, or its retractions were not folded.
    Every,
}

/// The buffers [`emit_groups`] reads and fills.
struct EmitScratch {
    vals: AggValues,
    /// Per probed group, the least image each scalar extreme retracts that no
    /// insert of the group reaches: [`AggValues::fold_retracted`].
    retracted: AggValues,
    /// Per group, which of its extremes come off the value index. Empty for a
    /// reduce without one.
    probes: Vec<Probe>,
    /// The stored rows of the groups that have one, in output order.
    stored: Vec<CursorRow>,
    /// Per stored row, its group.
    stored_groups: Vec<u32>,
    /// The output rows, as rows of the stored and fresh rows they copy.
    out_rows: Vec<u32>,
    /// The groups with a new row and no stored row, in output order.
    fresh: Vec<u32>,
    /// Per new row copied off a stored row, its output row and its group.
    derived: Vec<(u32, u32)>,
}

impl ReduceScratch {
    /// Empty buffers for `shape`'s aggregates; none is allocated yet.
    fn new(shape: &ReduceShape) -> Self {
        ReduceScratch {
            grouping: GroupScratch::default(),
            runs: Vec::new(),
            uncapped: Vec::new(),
            unprobed: Vec::new(),
            emit: EmitScratch {
                vals: AggValues::new(&shape.aggs, 0),
                retracted: AggValues::new(&shape.aggs, 0),
                probes: Vec::new(),
                stored: Vec::new(),
                stored_groups: Vec::new(),
                out_rows: Vec::new(),
                fresh: Vec::new(),
                derived: Vec::new(),
            },
        }
    }

    /// The bytes this holds allocated.
    fn held_bytes(&self) -> usize {
        let e = &self.emit;
        self.grouping.held_bytes()
            + vec_bytes(&self.runs)
            + vec_bytes(&self.uncapped)
            + vec_bytes(&self.unprobed)
            + e.vals.held_bytes()
            + e.retracted.held_bytes()
            + vec_bytes(&e.probes)
            + vec_bytes(&e.stored)
            + vec_bytes(&e.stored_groups)
            + vec_bytes(&e.out_rows)
            + vec_bytes(&e.fresh)
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
        Ok(ReducePlan {
            shape,
            seeds_ground,
            count_star,
            value_index,
            scratch: None,
        })
    }

    /// The row a global aggregate publishes over an empty input; `None` for a
    /// reduce that owes none.
    pub fn ground_row(&self) -> Option<Batch> {
        self.seeds_ground.then(|| {
            let mut v0 = Batch::with_capacity(&self.shape.output_schema, 1);
            AggValues::new(&self.shape.aggs, 1).emit_row(&mut v0, None, &ground_pk(), 0);
            v0
        })
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

/// In the ordinal arm, the most delta rows per probed group at which the
/// retractions are folded to spare the seeks of extremes they leave in place:
/// past it, the pass costs more than the seeks it would spare.
const RETRACTED_FOLD_ROWS: usize = 64;

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
    let ReduceScratch { grouping, runs, uncapped, unprobed, emit } = s;
    let EmitScratch { vals, retracted, probes, .. } = emit;

    let in_runs = shape.key.runs_into(delta, runs);
    if in_runs && (runs.len() == 1 || 2 * runs.len() <= delta.len()) {
        let ranges = &runs[..];
        probes.clear();
        if indexed {
            // Each run's weights scanned whole and branch-free: a run of
            // inserts reads every one either way.
            probes.extend(ranges.iter().map(|&(s, e)| {
                if e - s > RUN_STEP_CAP {
                    Probe::Every
                } else if weights[s..e].iter().fold(false, |any, w| any | retracts(w)) {
                    Probe::Retracts
                } else {
                    Probe::No
                }
            }));
        }
        // A capped run steps none of its rows for its extremes, and a
        // retracting one none for its wide ones, which fold no retraction.
        let (uncapped, unprobed) = match probes.iter().any(|&p| p != Probe::No) {
            true => (
                emptied(ranges, uncapped, |g| probes[g] == Probe::Every),
                emptied(ranges, unprobed, |g| probes[g] != Probe::No),
            ),
            false => (ranges, ranges),
        };
        vals.reset(ranges.len());
        for (k, agg) in aggs.iter().enumerate() {
            let folded = match agg.is_linear() {
                true => ranges,
                false if agg.is_wide() => unprobed,
                false => uncapped,
            };
            vals.fold(k, &mb, folded, RangeGroups::Each);
        }
        if probes.contains(&Probe::Retracts) {
            retracted.fold_retracted(&mb, uncapped, RangeGroups::Each, vals);
        }
        grouping.groups.fill_heads(ranges);
    } else {
        match in_runs {
            true => grouping.groups.fill_runs(runs),
            false => shape.key.numbered_into(delta, grouping),
        }
        let groups = &grouping.groups;

        probes.clear();
        if indexed {
            probes.resize(groups.len(), Probe::No);
            for (&g, w) in groups.ord.iter().zip(weights) {
                if retracts(w) {
                    probes[g as usize] = Probe::Retracts;
                }
            }
        }
        let retracting = probes.iter().filter(|&&p| p == Probe::Retracts).count();
        // A retracting group's wide extremes come off the index, so its rows
        // step none of them; a scalar one costs less to step than to skip.
        let all = &[(0, delta.len())];
        vals.reset(groups.len());
        for (k, agg) in aggs.iter().enumerate() {
            match retracting > 0 && agg.is_wide() {
                true => vals.fold_wide(k, &mb, all, &groups.ord[..], |g| probes[g] != Probe::No),
                false => vals.fold(k, &mb, all, &groups.ord[..]),
            }
        }
        if retracting * RETRACTED_FOLD_ROWS >= delta.len() {
            retracted.fold_retracted(&mb, all, &groups.ord[..], vals);
        } else if retracting > 0 {
            probes
                .iter_mut()
                .filter(|p| **p == Probe::Retracts)
                .for_each(|p| *p = Probe::Every);
        }
    }
    emit_groups(plan, &mb, out_trace, index, emit, &grouping.groups)
}

/// `ranges` with each run `g` that `skip(g)` emptied, in `out`.
fn emptied<'a>(
    ranges: &[(usize, usize)],
    out: &'a mut Vec<(usize, usize)>,
    skip: impl Fn(usize) -> bool,
) -> &'a [(usize, usize)] {
    out.clear();
    out.extend(
        ranges
            .iter()
            .enumerate()
            .map(|(g, &(s, e))| (s, if skip(g) { s } else { e })),
    );
    out
}

/// Emit each group of `groups` merged with its history, as a retraction and a
/// new row, in ascending output-PK order. A group reads the extremes its
/// `s.probes` names off the value index, whatever `s.vals` holds for them.
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
    groups: &GroupOrdinals,
) -> Batch {
    debug_assert!(
        !plan.seeds_ground || groups.len() == 1,
        "a global aggregate has one group"
    );
    let heads = GroupHeads { plan, delta, groups };
    let end = |g: Option<&u32>| heads.out_pk(*g.expect("a delta with a row has a group"));
    let mut out_trace = out_trace(end(groups.by_pk.first()).bytes(), end(groups.by_pk.last()).bytes());
    let mut index_cursor = None;

    // A trace with no row at the delta's groups holds no group's history.
    let traced = out_trace.is_valid();
    let probe = out_trace.group_probe();
    let mut emit = GroupEmit {
        heads,
        probe,
        index,
        index_cursor: &mut index_cursor,
        s,
    };
    let mut out = match (groups.len() <= ROW_EPOCH_GROUPS, traced) {
        (true, _) => emit.emit_by_row(traced),
        (false, false) => emit.emit_fresh_columns(),
        (false, true) => emit.emit_gathered(),
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
        if !plan.seeds_ground {
            return None;
        }
        vals.clear();
    }
    Some(stored.map_or(Ordering::Less, |(rows, row)| vals.cmp_stored(g, rows, row)))
}

/// The most groups an epoch emits a row at a time, and past which a column at a
/// time: up to this, a column kernel's fixed cost is more than its rows' copies
/// cost one by one.
const ROW_EPOCH_GROUPS: usize = 4;

/// Where each group of an epoch's delta stands in it.
#[derive(Clone, Copy)]
struct GroupHeads<'a, 'b> {
    plan: &'a ReducePlan,
    delta: &'a MemBatch<'b>,
    groups: &'a GroupOrdinals,
}

impl<'a> GroupHeads<'a, '_> {
    /// Group `g`'s first delta row.
    #[inline(always)]
    fn first(&self, g: u32) -> usize {
        self.groups.first[g as usize] as usize
    }

    /// Group `g`'s output PK.
    #[inline(always)]
    fn out_pk(&self, g: u32) -> OutPk<'a> {
        let id = self.groups.ids.get(g as usize).copied();
        self.plan.shape.key.out_pk_with(self.delta, self.first(g), id)
    }

    /// The new rows of groups `fresh`, none with a stored row, onto `w` a
    /// column at a time: each at +1, its group columns gathered off its first
    /// delta row, its aggregates off `vals`.
    #[inline(always)]
    fn write_fresh(&self, vals: &AggValues, w: &mut DirectWriter<'_>, fresh: &[u32]) {
        let (keys, weights, nulls) = w.fixed_mut();
        for (key, &g) in keys
            .chunks_exact_mut(self.plan.shape.output_schema.pk_stride())
            .zip(fresh)
        {
            key.copy_from_slice(self.out_pk(g).bytes());
        }
        weights.as_chunks_mut::<8>().0.fill(1i64.to_le_bytes());
        nulls.fill(0);
        let firsts = fresh.iter().map(|&g| self.first(g));
        w.gather_cells_from(self.plan.shape.key.carried(), self.delta, firsts);
        vals.emit_at(w, fresh.iter().enumerate().map(|(row, &g)| (row, g as usize)));
    }
}

/// What [`emit_groups`] reads each group's history through.
struct GroupEmit<'a, 'b, 'c> {
    heads: GroupHeads<'a, 'b>,
    probe: GroupProbe<'a>,
    /// What opens the value index.
    index: Option<OpenAt<'c>>,
    /// The value index opened at the probed groups, once a group seeks in it.
    /// Borrowed: a cursor is too large to move into this per epoch.
    index_cursor: &'a mut Option<ReadCursor>,
    s: &'a mut EmitScratch,
}

impl GroupEmit<'_, '_, '_> {
    /// Group `g` seeded from the value index at the extremes its probe names:
    /// after its stored row is folded, so the probe sees the stored extreme and
    /// a seeded one is the index's alone. An emptied group has no extremes to
    /// seed.
    #[inline(always)]
    fn seed(&mut self, g: u32) {
        let probe = self.s.probes.get(g as usize).copied().unwrap_or(Probe::No);
        if probe != Probe::No && self.s.vals.count_value(self.heads.plan.count_star, g as usize) > 0 {
            self.seed_probed(g, probe);
        }
    }

    /// [`Self::seed`] of a probed group, the index opened at the first that
    /// seeks in it.
    #[inline(never)]
    fn seed_probed(&mut self, g: u32, probe: Probe) {
        let GroupEmit { heads, index, index_cursor, s, .. } = self;
        let EmitScratch { vals, retracted, probes, .. } = &mut **s;
        let value_index = heads
            .plan
            .value_index
            .as_ref()
            .expect("a reduce that probes has a value index");
        let open = index.as_mut().expect("a value-indexed reduce is handed its index");
        let gi = g as usize;
        let open = || {
            let probed = heads.groups.by_pk.iter().filter(|&&g| probes[g as usize] != Probe::No);
            let (lo, hi) = value_index
                .group_span(heads.delta, probed.map(|&g| heads.first(g)))
                .expect("a probed group spans its own key");
            open(lo.pk_bytes(), hi.pk_bytes())
        };
        let retracted = (probe == Probe::Retracts).then_some(&*retracted);
        value_index.seed_extremes((index_cursor, open), heads.delta, heads.first(g), vals, gi, retracted);
    }

    /// Append the new row of group `g` under `out_pk`, its group columns read
    /// off its first delta row.
    #[inline(always)]
    fn emit_fresh(&self, out: &mut Batch, out_pk: &[u8], g: u32) {
        let heads = &self.heads;
        let group = Some((heads.delta, heads.first(g), heads.plan.shape.key.carried()));
        self.s.vals.emit_row(out, group, out_pk, g as usize);
    }

    /// The groups one at a time: each group's stored row, found where
    /// `traced`, folded and copied off the trace, its new row written off its
    /// delta row.
    fn emit_by_row(&mut self, traced: bool) -> Batch {
        let heads = self.heads;
        // A group emits at most its retraction and its new row; untraced, its
        // new row alone.
        let rows = heads.groups.len() << usize::from(traced);
        let mut out = Batch::with_capacity(&heads.plan.shape.output_schema, rows);
        for &g in &heads.groups.by_pk {
            let out_pk = heads.out_pk(g);
            let at = traced.then(|| self.probe.find(out_pk.bytes())).flatten();
            if let Some(at) = at {
                let (rows, row) = self.probe.row_at(at);
                self.s.vals.fold_stored(g as usize, rows, row);
            }
            self.seed(g);
            let stored = at.map(|at| self.probe.row_at(at));
            let new = new_row(heads.plan, &mut self.s.vals, g as usize, stored);
            if new == Some(Ordering::Equal) {
                continue;
            }
            if new == Some(Ordering::Less) {
                self.emit_fresh(&mut out, out_pk.bytes(), g);
            }
            if let Some(at) = at {
                self.probe.copy_row_into(at, &mut out, -1);
            }
            if new == Some(Ordering::Greater) {
                self.emit_fresh(&mut out, out_pk.bytes(), g);
            }
        }
        out
    }

    /// The groups, none with a stored row, a column at a time: each group's new
    /// row, its group columns gathered off its first delta row.
    fn emit_fresh_columns(&mut self) -> Batch {
        let heads = self.heads;
        self.s.fresh.clear();
        for &g in &heads.groups.by_pk {
            self.seed(g);
            if new_row(heads.plan, &mut self.s.vals, g as usize, None::<(&MemBatch, usize)>).is_some() {
                self.s.fresh.push(g);
            }
        }
        let EmitScratch { vals, fresh, .. } = &*self.s;
        let mut out = Batch::with_capacity(&heads.plan.shape.output_schema, fresh.len());
        out.write_rows(fresh.len(), |w| heads.write_fresh(vals, w, fresh));
        out
    }

    /// The groups a column at a time: the stored rows gathered off the trace
    /// in one pass and folded, then each copied out as the retraction and, its
    /// aggregates rewritten, as the new row.
    fn emit_gathered(&mut self) -> Batch {
        let heads = self.heads;
        self.s.stored.clear();
        self.s.stored_groups.clear();
        for &g in &heads.groups.by_pk {
            if let Some(at) = self.probe.find(heads.out_pk(g).bytes()) {
                self.s.stored.push(at);
                self.s.stored_groups.push(g);
            }
        }
        if self.s.stored.is_empty() {
            return self.emit_fresh_columns();
        }

        // The stored rows, then the new rows of the groups with none.
        let mut rows = Batch::with_capacity(&heads.plan.shape.output_schema, heads.groups.len());
        self.probe.gather_into(&self.s.stored, &mut rows);
        // Folded at +1, which a COUNT reads; from here each is its retraction.
        self.s
            .vals
            .fold_stored_rows(&rows.as_mem_batch(), &self.s.stored_groups);
        rows.map_weights(|_| -1);

        self.s.out_rows.clear();
        self.s.fresh.clear();
        self.s.derived.clear();
        let mut next = 0;
        for &g in &heads.groups.by_pk {
            // The stored row of the group, if it has one.
            let h = (self.s.stored_groups.get(next) == Some(&g)).then(|| {
                next += 1;
                next as u32 - 1
            });
            self.seed(g);
            let mb = rows.as_mem_batch();
            let new = new_row(heads.plan, &mut self.s.vals, g as usize, h.map(|h| (&mb, h as usize)));
            let Some(h) = h else {
                // A fresh group's new row is written behind the stored rows.
                if new.is_some() {
                    self.s.out_rows.push((self.s.stored.len() + self.s.fresh.len()) as u32);
                    self.s.fresh.push(g);
                }
                continue;
            };
            match new {
                None => self.s.out_rows.push(h),
                Some(Ordering::Equal) => {}
                // The new row is a copy of the stored row, its aggregates
                // rewritten, on its side of the retraction.
                Some(ord) => {
                    let row = self.s.out_rows.len() as u32 + u32::from(ord.is_gt());
                    self.s.derived.push((row, g));
                    self.s.out_rows.extend([h, h]);
                }
            }
        }
        let EmitScratch { vals, out_rows, fresh, derived, .. } = &*self.s;
        rows.write_rows(fresh.len(), |w| heads.write_fresh(vals, w, fresh));
        rows.indexed_rows_then(out_rows, |w| {
            // Every row copies at its own weight: a stored row at −1, a fresh
            // row at +1. A new row copied off a stored row is set to +1.
            let weights = w.weight_mut().as_chunks_mut::<8>().0;
            for &(row, _) in derived {
                weights[row as usize] = 1i64.to_le_bytes();
            }
            vals.emit_at(w, derived.iter().map(|&(row, g)| (row as usize, g as usize)));
        })
    }
}
