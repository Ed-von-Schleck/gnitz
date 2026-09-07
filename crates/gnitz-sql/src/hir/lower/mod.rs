//! HIR → circuit lowering. Walks the tree once and emits one circuit per
//! combine-class node (plus the linear nodes around it), cutting nested
//! combine-class subtrees to hidden segments.
//!
//! [`fold`] is the one shell that emits something else: the same `Reduce`,
//! lowered to an ad-hoc read's fold sink instead of to a circuit. Nothing else
//! here is circuit-specific, which is what lets the two share the reduce rule
//! below.
//!
//! This module owns the rules the whole lowering obeys, one home each, so the
//! per-node shells (`join`/`reduce`/`setop`/`fold`) carry only their own emission:
//!
//! * the **segment-cut** rule — [`resolve_input`] / [`cut_segment`], memoized by
//!   `Rc::as_ptr` through one compilation-wide [`CutMemo`];
//! * the **source-collision** rule — [`resolve_collisions`], comparing *resolved*
//!   tids and wrapping a repeat in a pass-through segment;
//! * the **reduce-derivation** rule — [`resolve_reduce_specs`] and
//!   [`reduce_out_layout`], the spec decomposition and output layout both reduce
//!   shells read, so neither can address an aggregate column the other did not;
//! * the **keep** rule — [`join_sides`], which decides what a join materializes
//!   into its traces, and hands both join shells one [`JoinSide`] per side;
//! * the **addressing** rule — [`Frame`], the `(layout, coldefs)` pair every
//!   filter and projection resolves against, and [`project_tail`], the one
//!   projection tail every combine emit ends with;
//! * the **exchange-topology** backstop, asserted once per emitted circuit inside
//!   `ViewChain::add_segment` and at the final emit.
//!
//! The linear path resolves the HIR to its physical inputs (source, scan bound,
//! WHERE conjuncts, physicalized projection) and delegates to
//! `linear::emit_linear` — one home for the emission strategy; only the
//! scan-bound extraction and the `HirExpr → BoundExpr` resolution are HIR work.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(crate) mod exists;
pub(crate) mod fold;
pub(crate) mod join;
pub(crate) mod joincore;
mod linear;
pub(crate) mod prims;
pub(crate) mod reduce;
pub(crate) mod setop;

use super::chain::{EmitPieces, ViewChain};
use super::physical;
use super::{slot_of, split_filter, ColId, HirAgg, HirExpr, HirRef, ProjEntry, RelExpr};
use super::{JoinClass, JoinShape, JoinType};
use crate::access::ranked_index_bounds;
use crate::agg::{push_agg_specs, AggSpec};
use crate::codec::project_schema::{payload_map, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::{ColumnDef, RelDescriptor, Schema};
use gnitz_wire::IndexBound;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

/// What a HIR `Reduce` becomes physically, before either lowering picks a sink:
/// where its group columns and aggregate arguments sit in the reduce input, and
/// the spec decomposition over them.
///
/// One home because the three are interlocked. `push_agg_specs` decides how many
/// specs an aggregate materialises — an AVG emits two — `agg_starts` records
/// where each aggregate's block begins, and every reduce-output column position
/// downstream is `agg_col_offset + agg_starts[i]` (with a companion at `+ 1`).
/// Two copies of this loop that disagreed would not fail: they would silently
/// address the wrong aggregate column.
pub(crate) struct ReduceSpecs {
    /// Each group column's slot in the reduce input, parallel to `group_cols`.
    pub(crate) group_positions: Vec<usize>,
    /// The physical specs, pre-companion — `ensure_cardinality_count` is the
    /// circuit lowering's own addition and is not applied here.
    pub(crate) specs: Vec<AggSpec>,
    /// `specs` index at which aggregate `i` begins.
    pub(crate) agg_starts: Vec<usize>,
}

/// Resolve a `Reduce`'s group columns and aggregates against its input's layout
/// and schema — the derivation `lower::reduce` and `lower::fold` share.
pub(crate) fn resolve_reduce_specs(
    group_cols: &[ColId],
    aggs: &[HirAgg],
    layout: &[ColId],
    schema: &Schema,
) -> Result<ReduceSpecs, GnitzSqlError> {
    let group_positions: Vec<usize> = group_cols
        .iter()
        .map(|g| slot_of(layout, *g))
        .collect::<Result<_, _>>()?;
    let mut specs: Vec<AggSpec> = Vec::new();
    let mut agg_starts: Vec<usize> = Vec::with_capacity(aggs.len());
    for a in aggs {
        agg_starts.push(specs.len());
        let arg_pos = a.arg.map(|id| slot_of(layout, id)).transpose()?;
        push_agg_specs(a.func, arg_pos, &schema.columns, &mut specs)?;
    }
    Ok(ReduceSpecs { group_positions, specs, agg_starts })
}

/// The reduce output's `ColId` at each of its `width` physical slots:
/// `group_slots[j]` holds group column `j`, and aggregate `i` its raw value at
/// `agg_col_offset + agg_starts[i]` with its `COUNT_NON_NULL` companion, when it
/// has one, immediately after.
///
/// Slots with no logical identity — a synthetic key, the cardinality COUNT —
/// stay [`ColId::NONE`], which nothing can reference. `group_slots` is the
/// caller's because the key region differs by sink (the reduce output layout over
/// the circuit's chosen out-key; the fold's is always SyntheticFold).
pub(crate) fn reduce_out_layout(
    width: usize,
    group_cols: &[ColId],
    group_slots: &[usize],
    aggs: &[HirAgg],
    agg_starts: &[usize],
    agg_col_offset: usize,
) -> Vec<ColId> {
    let mut layout = vec![ColId::NONE; width];
    for (j, &gid) in group_cols.iter().enumerate() {
        layout[group_slots[j]] = gid;
    }
    for (i, a) in aggs.iter().enumerate() {
        let slot = agg_col_offset + agg_starts[i];
        layout[slot] = a.out.id;
        if let Some(c) = &a.companion {
            layout[slot + 1] = c.id;
        }
    }
    layout
}

/// The downstream demand on a combine's output: the final projection and the
/// post-node WHERE (outer joins only; empty for INNER — the rewrite folded the
/// WHERE into the residual).
#[derive(Clone, Copy)]
pub(crate) struct Demand<'a> {
    pub(crate) items: &'a [ProjEntry],
    pub(crate) where_preds: &'a [HirExpr],
}

impl Demand<'_> {
    /// Every `ColId` the demand reads, into `out`.
    pub(crate) fn refs(&self, out: &mut HashSet<ColId>) {
        collect_live_cols(self.items.iter().map(|i| &i.expr).chain(self.where_preds), out);
    }
}

/// A resolved combine input: its delta source, registered schema, and the `ColId`
/// layout (physical column order) against which key / group / projection
/// references resolve. A base/segment `Get` maps directly; a cut combine subtree
/// resolves to its hidden segment via [`cut_segment`]. (Generalizes the join
/// builder's original `JoinInput`.)
#[derive(Clone)]
pub(crate) struct SegInput {
    pub tid: u64,
    pub schema: Arc<Schema>,
    pub layout: Vec<ColId>,
    /// The catalog descriptor `tid` resolved to, or `None` when `tid` is a
    /// chain-minted segment, which has no catalog rows until the chain commits.
    /// The lowering reads every catalog fact off this: the scan-bound index list
    /// and the reduce's replication flag.
    pub desc: Option<Arc<RelDescriptor>>,
}

/// The `Rc::as_ptr` cut memo, threaded through the whole lowering: a subtree
/// reached from two places is cut to **one** hidden segment, and every reference
/// resolves to it. It makes [`cut_segment`] idempotent per subtree identity, and
/// is the only reason the HIR→HIR rewrites preserve `Rc` identity (a rebuild that
/// dropped it would split a shared node into two segments). Per-shell memos would
/// defeat both.
///
/// Shared nodes reach the lowering through `RelExpr::Alias` — a CTE named twice,
/// and the window desugar's reads of its own `W` — and this memo is what cuts
/// each such subtree once, however many aliases read it. Where two aliases then
/// meet as inputs of one combine, [`resolve_collisions`] gives the later one a
/// pass-through wrapper, since a circuit's delta inputs must carry distinct
/// source ids.
pub(crate) type CutMemo = HashMap<*const RelExpr, SegInput>;

/// Insert every `HirRef::Col` id referenced anywhere in `exprs` into `live` — the
/// one home for "which columns does this expression set demand", used to prune a
/// cut input's segment to exactly the columns its parent reads.
pub(crate) fn collect_live_cols<'a>(exprs: impl IntoIterator<Item = &'a HirExpr>, live: &mut HashSet<ColId>) {
    for e in exprs {
        e.for_each_ref(&mut |r| {
            if let HirRef::Col(id) = r {
                live.insert(*id);
            }
        });
    }
}

/// A `ColId` layout behind a key region: `npk` identity-free slots (the synthetic
/// `_join_pk` / `_pair_pk` / `_set_pk` region — hidden, so nothing can reference
/// them) followed by `payload`. Every combine emit's output has this shape, and so
/// does every layout a residual, WHERE or output projection resolves against.
pub(crate) fn key_region_layout(npk: usize, payload: impl IntoIterator<Item = ColId>) -> Vec<ColId> {
    let mut layout = vec![ColId::NONE; npk];
    layout.extend(payload);
    layout
}

/// The `(ColId layout, ColumnDef list)` one node is addressed through. A
/// reference resolves to a position in `layout` and is then typed against `cols`
/// at that position, so the two must describe the same node; passing them as one
/// value is what keeps them from being paired by hand.
pub(crate) struct Frame {
    layout: Vec<ColId>,
    cols: Vec<ColumnDef>,
    npk: usize,
}

impl Frame {
    /// `pk_cols` as the leading key region — identity-free slots nothing can
    /// reference — then one iterator of `(id, def)` driving both halves.
    pub(crate) fn keyed(pk_cols: Vec<ColumnDef>, payload: impl IntoIterator<Item = (ColId, ColumnDef)>) -> Frame {
        let npk = pk_cols.len();
        let mut layout = vec![ColId::NONE; npk];
        let mut cols = pk_cols;
        for (id, def) in payload {
            layout.push(id);
            cols.push(def);
        }
        Frame { layout, cols, npk }
    }

    /// A relation's own layout and registered schema, which already travel
    /// together.
    pub(crate) fn of(layout: &[ColId], schema: &Schema) -> Frame {
        debug_assert_eq!(layout.len(), schema.columns.len(), "a frame's two halves are parallel");
        Frame {
            layout: layout.to_vec(),
            cols: schema.columns.clone(),
            npk: schema.pk_cols.len(),
        }
    }

    /// The frame as a `Schema`, key region as the PK — what an expression
    /// compiles its column references against.
    fn schema(&self) -> Schema {
        Schema {
            columns: self.cols.clone(),
            pk_cols: (0..self.npk as u32).collect(),
        }
    }
}

/// A combine emit's projection tail: the cheapest node producing
/// `[key region][projected payload]`, and the output column defs. Every combine
/// shell ends here, so any of them may project a computed expression without a
/// hidden segment to materialize it in.
pub(crate) fn project_tail(
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    items: &[ProjEntry],
    frame: &Frame,
) -> Result<(gnitz_core::NodeId, Vec<ColumnDef>), GnitzSqlError> {
    let npk = frame.npk;
    let mut out_cols = frame.cols[..npk].to_vec();
    let mut proj: Vec<ProjItem> = Vec::with_capacity(items.len());
    for item in items {
        proj.push(ProjItem::from_bound(physical::resolve_refs(&item.expr, &frame.layout)?));
        out_cols.push(item.out.def.clone());
    }
    // All pass-through: a copy list, or nothing when it already names the payload
    // in order.
    let slots: Option<Vec<usize>> = proj
        .iter()
        .map(|i| match i {
            ProjItem::PassThrough { src_col } => Some(*src_col),
            ProjItem::Computed { .. } => None,
        })
        .collect();
    let out = match slots {
        Some(slots)
            if slots.len() == frame.cols.len() - npk && slots.iter().enumerate().all(|(i, &p)| p == i + npk) =>
        {
            node
        }
        Some(slots) => cb.map(node, &slots),
        None => cb.map_expr(node, payload_map(&proj, &out_cols[npk..], &frame.schema())?),
    };
    Ok((out, out_cols))
}

/// A `SegInput` reading a `Get` directly — its delta source is the relation
/// itself and its layout the Get's minted `ColId`s. `None` for any non-`Get` node
/// (which must instead be cut to a hidden segment). The one home for the
/// `Get → SegInput` mapping every combine input and linear source shares.
pub(crate) fn seginput_of_get(rel: &RelExpr) -> Option<SegInput> {
    let RelExpr::Get { tid, schema, cols, desc } = rel else {
        return None;
    };
    Some(SegInput {
        tid: *tid,
        schema: Arc::clone(schema),
        layout: cols.iter().map(|c| c.id).collect(),
        desc: desc.clone(),
    })
}

/// Lower a bound + classified `RelExpr` tree to circuit pieces.
pub(crate) fn lower(chain: &mut ViewChain, rel: Rc<RelExpr>, bounded: bool) -> Result<EmitPieces, GnitzSqlError> {
    if bounded {
        reject_ineligible_capacity_body(&rel)?;
    }
    let mut memo = CutMemo::new();
    let (pieces, _layout) = lower_body(chain, &mut memo, &rel)?;
    Ok(pieces)
}

/// Reject a body a capacity-bounded view cannot have. Eligibility is a **positive
/// list** of exactly the two shapes per-key hydration can replay: a
/// filter/projection over one relation, and a plain inner equi-join. Anything else
/// falls through to a rejection, so a new body shape does not silently inherit
/// eligibility.
///
/// One classifier over the bound tree, asked once before lowering, rather than a
/// rule restated in each `lower_body` arm: a per-arm rule is one a new arm can
/// forget, and it reads as a whitelist while behaving as a blacklist. The arm
/// structure here mirrors `lower_body`'s deliberately — same order, same fusion
/// question first — so the shape named in the error is the shape that would have
/// been lowered. `build_query_segments` supplies the other half of the rule (the
/// body must not have cut into hidden segments), which no single-tree walk can
/// see.
fn reject_ineligible_capacity_body(rel: &RelExpr) -> Result<(), GnitzSqlError> {
    let shape = match rel {
        RelExpr::Project { input, items } => {
            let (_, source) = split_filter(input);
            // An alias reads what it aliases; the shape is the target's.
            let mut source = source;
            while let RelExpr::Alias { input, .. } = source.as_ref() {
                source = input;
            }
            match source.as_ref() {
                // Only a `Get` source can carry one: over a combine the projection
                // either cuts (bounding a projection that sits on an unbounded copy
                // of the same rows) or fuses into the join tail (leaving per-key
                // hydration to replay a skeleton row through a compute map).
                _ if projection_is_computed(items) && !matches!(source.as_ref(), RelExpr::Get { .. }) => {
                    "a computed projection over a combine"
                }
                // Eligible: filter/projection over one relation.
                RelExpr::Get { .. } => return Ok(()),
                RelExpr::Join { kind: JoinType::Inner, on, .. } => match on.class()?.shape() {
                    // A range/band join's null-fill threshold pipeline is not
                    // replayable per key; a cross join has no key at all, so a
                    // skeleton row names no trace group to replay.
                    JoinShape::Range => "a range or band join",
                    JoinShape::Cross => "a cross join",
                    // Eligible: plain inner equi-join.
                    JoinShape::Equi => return Ok(()),
                },
                RelExpr::Join {
                    kind: JoinType::Left | JoinType::Right | JoinType::Full,
                    ..
                } => "an outer join",
                RelExpr::Join {
                    kind: JoinType::Semi | JoinType::Anti, ..
                } => "EXISTS / NOT EXISTS / IN",
                RelExpr::Join { kind: JoinType::Mark, .. } => "a mark join (IN / ANY over a subquery)",
                RelExpr::Reduce { .. } => "GROUP BY / an aggregate",
                _ => "a derived table / DISTINCT / set-operation subquery",
            }
        }
        RelExpr::Distinct { .. } => "a root DISTINCT",
        RelExpr::SetOp { .. } => "a root set operation",
        _ => "this body",
    };
    Err(GnitzSqlError::Unsupported(format!(
        "CREATE VIEW WITH (capacity …): {shape} is not supported; \
         only a filter/projection over one relation and an inner equi-join are"
    )))
}

/// Lower a complete view body — linear or combine-class — returning the circuit
/// pieces plus the output `ColId` layout ([`ColId::NONE`] for each hidden
/// synthetic-key slot, the real output id for each visible column). The layout is
/// what a cut segment exposes to its parent, so every body shape is cuttable by
/// construction.
fn lower_body(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &Rc<RelExpr>,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Project { input, items } => {
            let (fpreds, source) = split_filter(input);
            match source.as_ref() {
                // The fusion decision, asked once for every source kind rather than
                // re-answered per arm: a shell that cannot materialize a computed
                // projection has its source cut to a hidden segment, and the WHERE +
                // projection are lowered as a linear body over it (the
                // scalar-decorrelation finalize shape).
                _ if projection_is_computed(items) && !fuses_computed_projection(source) => {
                    lower_computed_over_combine(chain, memo, items, fpreds, source)
                }
                RelExpr::Get { .. } => {
                    let src = seginput_of_get(source).expect("Get arm resolves to a SegInput");
                    lower_linear(&src, fpreds, items)
                }
                RelExpr::Join {
                    kind: JoinType::Semi | JoinType::Anti, ..
                } => exists::lower_semi_anti_view(chain, memo, items, fpreds, source),
                RelExpr::Join { kind: JoinType::Mark, .. } => {
                    exists::lower_mark_view(chain, memo, items, fpreds, source)
                }
                RelExpr::Join { .. } => join::lower_join_view(chain, memo, items, fpreds, source),
                RelExpr::Reduce { .. } => reduce::lower_reduce(chain, memo, items, fpreds, source),
                // A derived table (a projection, DISTINCT, or set operation) is not a
                // delta source the linear path can read in place, so it cuts even for
                // a bare-column projection; an alias resolves to whatever it reads.
                RelExpr::Project { .. } | RelExpr::Distinct { .. } | RelExpr::SetOp { .. } | RelExpr::Alias { .. } => {
                    lower_computed_over_combine(chain, memo, items, fpreds, source)
                }
                _ => Err(unsupported_body()),
            }
        }
        RelExpr::Distinct { input } => setop::lower_distinct(chain, memo, input),
        RelExpr::SetOp { .. } => setop::lower_setop(chain, memo, rel),
        _ => Err(unsupported_body()),
    }
}

/// Whether any projection item is a computed expression (not a bare column
/// reference) — typically a decorrelated scalar finalize.
fn projection_is_computed(items: &[ProjEntry]) -> bool {
    items
        .iter()
        .any(|it| !matches!(&it.expr, BExpr::ColRef(HirRef::Col(_))))
}

/// Whether the shell for this source materializes a computed projection itself.
/// A `Get` (the linear expr-map), a `Reduce` (its finalize map) and every join
/// (through [`project_tail`]) do. A derived table is not a delta source at all and
/// cuts instead. One predicate, so a new combine shell cannot silently inherit the
/// wrong answer.
fn fuses_computed_projection(source: &RelExpr) -> bool {
    matches!(
        source,
        RelExpr::Get { .. } | RelExpr::Reduce { .. } | RelExpr::Join { .. }
    )
}

/// Cut a source to a hidden segment (pruned to the columns the WHERE + projection
/// read) and lower the projection — with the WHERE applied — as a linear body over
/// a synthetic `Get` on that segment. Reached by a derived table, and by any source
/// [`fuses_computed_projection`] answers `false` for.
///
/// The general rule it applies: an operator addresses its columns by position, so
/// an expression it cannot take becomes a column of a `Project` wrapped around
/// it, above or below.
fn lower_computed_over_combine(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    where_preds: &[HirExpr],
    source: &Rc<RelExpr>,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let mut live: HashSet<ColId> = HashSet::new();
    Demand { items, where_preds }.refs(&mut live);
    let seg = resolve_input(chain, memo, source, &live)?;
    lower_linear(&seg, where_preds, items)
}

/// A body shape the driver has no arm for. Every shape bind can produce is
/// covered, so this is an internal invariant break, not a user-facing limit —
/// phrase it as one rather than as an engine restriction.
fn unsupported_body() -> GnitzSqlError {
    GnitzSqlError::Internal("HIR lowering has no body arm for this node".into())
}

/// Resolve one input of a combine node to a `SegInput` — **the** segment-cut
/// rule, in one place rather than re-derived as a structural match per lowering
/// shell. A bare `Get` is read in place (the delta source is the relation
/// itself), and an alias is whatever it reads under its own ids; every other
/// shape — a nested combine, or a linear spine whose projection must be
/// materialized first — is cut to a hidden segment.
pub(crate) fn resolve_input(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    match resolve_in_place(chain, memo, input)? {
        Some(seg) => Ok(seg),
        None => cut_segment(chain, memo, input, live),
    }
}

/// The half of [`resolve_input`] that materializes nothing new: a `Get` read in
/// place, or an alias resolved to its input's source (cut once with every column
/// live, since every alias of it reads that one segment) under the alias's own
/// ids. `None` for a shape that must be cut — the one test of "is this read in
/// place", which the reduce and set-op shells ask directly because their inline
/// arms do their own WHERE and scan-bound work.
pub(crate) fn resolve_in_place(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
) -> Result<Option<SegInput>, GnitzSqlError> {
    if let Some(seg) = seginput_of_get(input) {
        return Ok(Some(seg));
    }
    let RelExpr::Alias { input: inner, cols } = input.as_ref() else {
        return Ok(None);
    };
    let inner_cols = inner.cols();
    let all: HashSet<ColId> = inner_cols.iter().map(|c| c.id).collect();
    let seg = resolve_input(chain, memo, inner, &all)?;
    let layout = seg
        .layout
        .iter()
        .map(|id| {
            inner_cols
                .iter()
                .position(|c| c.id == *id)
                .map_or(ColId::NONE, |p| cols[p].id)
        })
        .collect();
    Ok(Some(SegInput { layout, ..seg }))
}

/// Cut a subtree to a hidden segment and return a `SegInput` over it. A subtree
/// that is not already a complete body (a bare `Join`, a `Filter`, a `Get`) is
/// wrapped in an identity `Project` over its **live** cols — the chain-liveness
/// prune, so the registered segment schema carries only what the parent demands.
/// Memoized by `Rc::as_ptr`: one segment per shared subtree.
pub(crate) fn cut_segment(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    subtree: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    let key = Rc::as_ptr(subtree);
    if let Some(cached) = memo.get(&key) {
        return Ok(cached.clone());
    }
    let body = as_body(subtree, live);
    let seg = chain.add_segment(|chain| lower_body(chain, memo, &body))?;
    memo.insert(key, seg.clone());
    Ok(seg)
}

/// A subtree as a complete, lowerable body: `Project`/`Distinct`/`SetOp` already
/// are one; anything else is wrapped in an identity `Project` over its live cols.
///
/// A cut `Project` narrows its own items to the live set — the one pruning gap the
/// lowering-time demand did not already close (its parent's demand reaches the
/// identity-`Project` wrap below and the shell-built child-demand, but never a cut
/// `Project` segment's own entries). `Distinct`/`SetOp` stay full-column: their
/// content-hash identity spans every projected column (`π(distinct(X)) ≠
/// distinct(π(X))`), so narrowing would change the dedup/match classes.
fn as_body(subtree: &Rc<RelExpr>, live: &HashSet<ColId>) -> Rc<RelExpr> {
    match subtree.as_ref() {
        RelExpr::Project { input, items } => {
            let narrowed: Vec<ProjEntry> = items.iter().filter(|it| live.contains(&it.out.id)).cloned().collect();
            if narrowed.len() == items.len() {
                Rc::clone(subtree)
            } else {
                RelExpr::project(Rc::clone(input), narrowed)
            }
        }
        RelExpr::Distinct { .. } | RelExpr::SetOp { .. } => Rc::clone(subtree),
        // The HIR spelling of `SELECT <live cols> FROM <subtree>`.
        _ => {
            let items = RelExpr::passthrough_items(subtree.cols().into_iter().filter(|c| live.contains(&c.id)));
            RelExpr::project(Rc::clone(subtree), items)
        }
    }
}

/// Resolve the source-collision rule over a combine's already-resolved inputs:
/// a circuit's delta inputs must carry distinct source ids, so a repeated `tid`
/// wraps the later side in an identity pass-through segment. One home for the
/// self-join wrapper (a table twice, or a shared segment through two aliases)
/// and the same-relation INTERSECT/EXCEPT wrapper — it compares **resolved**
/// tids, so a side that already became its own segment is correctly seen as
/// distinct and never wrapped redundantly.
///
/// `exempt` skips the rule for UNION / UNION ALL: those are linear merges the dag
/// explicitly drives by cloning one epoch's delta to both sides, and `a UNION a`
/// compiles unwrapped today — wrapping would regress a working physical plan.
pub(crate) fn resolve_collisions(
    chain: &mut ViewChain,
    inputs: &mut [SegInput],
    live: &[HashSet<ColId>],
    exempt: bool,
) -> Result<(), GnitzSqlError> {
    if exempt {
        return Ok(());
    }
    let mut seen: HashSet<u64> = HashSet::new();
    for (input, live) in inputs.iter_mut().zip(live) {
        if seen.insert(input.tid) {
            continue;
        }
        *input = wrap_passthrough_segment(chain, input, live)?;
        seen.insert(input.tid);
    }
    Ok(())
}

/// Wrap a resolved source in an identity pass-through segment and return a
/// `SegInput` over it — the collision-resolution device shared by the self-join
/// wrapper and the same-relation set-op/DISTINCT wrapper. Being a second
/// materialized relation, it carries only what `live` demands of *this* side,
/// exactly as a cut segment does, and it is lowered through `lower_linear` so the
/// PK-front convention comes from `physicalize_projection`.
fn wrap_passthrough_segment(
    chain: &mut ViewChain,
    src: &SegInput,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    // `place_pk_front` re-prepends an unprojected source PK (staying hidden), so
    // the wrapper keeps a row identity whatever the demand is.
    let items = RelExpr::passthrough_items(
        src.schema
            .columns
            .iter()
            .zip(&src.layout)
            .filter(|(def, id)| !def.is_hidden && live.contains(id))
            .map(|(def, id)| super::HirCol::new(*id, def.clone())),
    );
    chain.add_segment(|_chain| lower_linear(src, &[], &items))
}

/// Lower a linear body over a resolved source `SegInput` (a base/segment `Get`, or
/// a combine subtree already cut to a hidden segment): resolve the WHERE against
/// the source layout, extract the scan bound, physicalize the projection, and hand
/// the physical inputs to `linear::emit_linear`.
fn lower_linear(
    src: &SegInput,
    filter_preds: &[HirExpr],
    proj_items: &[ProjEntry],
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let preds = physical::resolve_preds(filter_preds, &src.layout)?;
    let bound = extract_scan_bound(&preds, src);
    let proj = physical::physicalize_projection(proj_items, &src.layout, &src.schema)?;
    let pieces = linear::emit_linear(src, bound, &preds, &proj)?;
    Ok((pieces, proj.layout))
}

/// Resolve `preds` against `layout`, compile their AND, and emit the filter —
/// passing `node` through untouched when nothing is left to test. Shared by the
/// emits that resolve and compile in one step: HAVING, the join and EXISTS
/// residuals, and a set-op segment's WHERE. A primary-position WHERE does not
/// come through here — `linear` and `reduce` resolve it earlier, because the scan
/// bound reads the same conjuncts, and compile it themselves.
pub(crate) fn emit_filter<'a>(
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    preds: impl IntoIterator<Item = &'a HirExpr>,
    frame: &Frame,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    let preds = physical::resolve_preds(preds, &frame.layout)?;
    match crate::expr_lower::compile_filter_program(&preds, &frame.cols)? {
        Some(prog) => Ok(cb.filter(node, prog)),
        None => Ok(node),
    }
}

/// Scan-bound extraction — a primary-position lowering decision (source and the
/// resolved WHERE both in hand). Only a catalog source bounds. Shared by every
/// primary-position `Get` lowering (`lower_linear` here, `reduce::lower_reduce`'s
/// inline-source arm).
pub(crate) fn extract_scan_bound(preds: &[crate::ir::BoundExpr], src: &SegInput) -> Option<IndexBound> {
    // The head candidate outright: this path compiles no residual — the `Filter`
    // is emitted verbatim either way — so there is nothing a later candidate
    // could express that the best-ranked one cannot.
    ranked_index_bounds(preds, &src.schema, &src.desc.as_ref()?.indexes)
        .into_iter()
        .next()
        .map(|c| IndexBound { idx_cols: c.idx_cols, desc: c.desc })
}

/// One side of a join as [`join_sides`] left it: the resolved input, its
/// reindex-payload keep list in emission order, those columns' defs, and where
/// the pinned source PK sits inside the kept payload (empty when unpinned).
pub(crate) struct JoinSide {
    pub(crate) seg: SegInput,
    pub(crate) keep: Vec<u32>,
    pub(crate) coldefs: Vec<ColumnDef>,
    pub(crate) pk_span: std::ops::Range<usize>,
}

impl JoinSide {
    /// The kept payload width.
    pub(crate) fn n(&self) -> usize {
        self.keep.len()
    }

    /// The pinned source PK's arity — `0` when this shape packs no PK out of the
    /// payload.
    pub(crate) fn pa(&self) -> usize {
        self.pk_span.len()
    }

    /// The kept payload's `ColId`s, in keep order.
    pub(crate) fn ids(&self) -> impl Iterator<Item = ColId> + '_ {
        self.keep.iter().map(|&i| self.seg.layout[i as usize])
    }
}

/// Both sides of a join step under the keep rule: which source columns survive
/// into the join's traces, and so onto disk. The five contributors are marked in
/// order below; a wildcard projection is already expanded into `ProjEntry` column
/// refs by bind, so Rule 1 covers `SELECT *`.
pub(crate) fn join_sides(down: Demand<'_>, class: &JoinClass, kind: JoinType, inputs: [SegInput; 2]) -> [JoinSide; 2] {
    let [left_in, right_in] = inputs;
    let left_n = left_in.layout.len();
    let mut keep = vec![false; left_n + right_in.layout.len()];
    let mark = |keep: &mut Vec<bool>, id: ColId| {
        if let Some(p) = left_in.layout.iter().position(|c| *c == id) {
            keep[p] = true;
        } else if let Some(p) = right_in.layout.iter().position(|c| *c == id) {
            keep[left_n + p] = true;
        }
        // An id in neither layout is the mark column, which the mark branch
        // substitutes by its `0/1` constant before resolving anything.
    };
    // Rules 1 + 2: projection, residual ON + top-level WHERE.
    let mut referenced: HashSet<ColId> = HashSet::new();
    down.refs(&mut referenced);
    collect_live_cols(&class.residual, &mut referenced);
    for id in referenced {
        mark(&mut keep, id);
    }
    // Rule 3: a side with a ν keeps its nullable join-key components. `map_reindex`
    // collapses a NULL key to synthetic PK 0, so pruning the key would make a
    // NULL-keyed row and a real `k = 0` row byte-identical and the NULL row's
    // null-fill would cancel against the real row's matched multiplicity. Only the
    // equi shapes need it — the band and pure-range ν key on the source PK, where
    // NULL and 0 cannot collide — so elsewhere it merely over-keeps.
    for (is_left, base, seg) in [(true, 0, &left_in), (false, left_n, &right_in)] {
        if !kind.has_nu(is_left) {
            continue;
        }
        for p in &class.eq {
            let id = if is_left { p.left } else { p.right };
            if let Some(pos) = seg.layout.iter().position(|c| *c == id) {
                if seg.schema.columns[pos].is_nullable {
                    keep[base + pos] = true;
                }
            }
        }
    }
    // Rule 4: a shape that packs its output key out of the payload keeps that
    // side's `pk_cols`, pinned to the front of the keep list by `one_side`. A
    // range-correlated EXISTS/IN reaches no null-fill tail, so only its outer PK.
    let packs_source_pk = matches!(class.shape(), JoinShape::Range | JoinShape::Cross);
    let outer_only = matches!(kind, JoinType::Semi | JoinType::Anti | JoinType::Mark);
    let pins = [packs_source_pk, packs_source_pk && !outer_only];
    for (pin, (base, seg)) in pins.into_iter().zip([(0, &left_in), (left_n, &right_in)]) {
        if pin {
            for &c in &seg.schema.pk_cols {
                keep[base + c as usize] = true;
            }
        }
    }
    // Rule 5, the fallback: a side with a ν needs an identity to subtract on. A
    // side without one keeps nothing — column 0 would only split one trace element
    // per key into one per row, and by ordinal it can pin a whole German string.
    for (is_left, base, width) in [(true, 0, left_n), (false, left_n, keep.len() - left_n)] {
        if width > 0 && kind.has_nu(is_left) && !keep[base..base + width].iter().any(|&b| b) {
            keep[base] = true;
        }
    }
    // A join reading nothing from either side still emits rows.
    if !keep.iter().any(|&b| b) && left_n > 0 {
        keep[0] = true;
    }
    let (kl, kr) = keep.split_at(left_n);
    [one_side(left_in, kl, pins[0]), one_side(right_in, kr, pins[1])]
}

/// One side's keep list in emission order: its pinned PK columns first, then every
/// other kept column in source order. A keep list need not ascend, and the PK at
/// the front is what makes every pair-PK slot list a range.
fn one_side(seg: SegInput, keep: &[bool], pin_pk: bool) -> JoinSide {
    let pinned: Vec<u32> = if pin_pk { seg.schema.pk_cols.clone() } else { Vec::new() };
    let mut cols = pinned.clone();
    cols.extend((0..keep.len() as u32).filter(|i| keep[*i as usize] && !pinned.contains(i)));
    let coldefs = cols.iter().map(|&i| seg.schema.columns[i as usize].clone()).collect();
    JoinSide {
        pk_span: 0..pinned.len(),
        seg,
        keep: cols,
        coldefs,
    }
}
