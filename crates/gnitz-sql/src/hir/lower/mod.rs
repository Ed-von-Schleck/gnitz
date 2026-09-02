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
//! * the **exchange-topology** backstop, asserted once per emitted circuit inside
//!   `ViewChain::add_segment` and at the final emit.
//!
//! The linear path resolves the HIR to its physical inputs (source, scan bound,
//! folded predicate, physicalized projection) and delegates to
//! `linear::emit_linear` — one home for the emission strategy; only the
//! scan-bound extraction and the `HirExpr → BoundExpr` resolution are HIR work.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(crate) mod exists;
pub(crate) mod fold;
pub(crate) mod join;
mod linear;
pub(crate) mod prims;
pub(crate) mod reduce;
pub(crate) mod setop;

use super::chain::{EmitPieces, ViewChain};
use super::physical;
use super::JoinType;
use super::{slot_of, ColId, HirAgg, HirExpr, HirRef, ProjEntry, RelExpr};
use crate::access::ranked_index_bounds;
use crate::agg::{push_agg_specs, AggSpec};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::{ColumnDef, RelDescriptor, Schema};
use gnitz_wire::ScanBound;
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
    Ok(ReduceSpecs {
        group_positions,
        specs,
        agg_starts,
    })
}

/// The reduce output's `ColId` at each of its `width` physical slots:
/// `group_slots[j]` holds group column `j`, and aggregate `i` its raw value at
/// `agg_col_offset + agg_starts[i]` with its `COUNT_NON_NULL` companion, when it
/// has one, immediately after.
///
/// Slots with no logical identity — a synthetic key, the cardinality COUNT —
/// stay [`ColId::NONE`], which nothing can reference. `group_slots` is the
/// caller's because the key region differs by sink (`group_col_reduce_pos` over
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
/// No producer shares a node *today* — bind builds a tree, and decorrelation
/// dedups its cloned subquery `rel`s before placing them. Two references to one
/// CTE are **not** an exception: each mints a fresh `Get` on the same tid, which
/// [`resolve_collisions`] reconciles by tid, not this memo by identity. The memo
/// keeps single-cutting a property of the lowering rather than an obligation on
/// every producer.
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

/// The output `ColId` layout of a combine emit: `npk` identity-free key slots (the
/// synthetic `_join_pk` / `_pair_pk` / `_set_pk` region — hidden, so nothing can
/// reference them) followed by one projected id per payload column, in item order.
/// Every combine shell's output has this shape, so this is its one home.
pub(crate) fn key_region_layout(npk: usize, items: &[ProjEntry]) -> Vec<ColId> {
    let mut layout = vec![ColId::NONE; npk];
    layout.extend(items.iter().map(|i| i.out.id));
    layout
}

/// Resolve a combine emit's output projection: each item's slot against `layout`,
/// and the output column defs as the leading key region (`pk_cols`) followed by the
/// item defs. Every combine shell's tail begins here, so the
/// `[key region][projected payload]` output shape has one home.
pub(crate) fn resolve_projection(
    items: &[ProjEntry],
    layout: &[ColId],
    pk_cols: Vec<ColumnDef>,
) -> Result<(Vec<usize>, Vec<ColumnDef>), GnitzSqlError> {
    let mut slots = Vec::with_capacity(items.len());
    let mut cols = pk_cols;
    for item in items {
        slots.push(super::slot_of_expr(&item.expr, layout)?);
        cols.push(item.out.def.clone());
    }
    Ok((slots, cols))
}

/// Apply a resolved projection to `node`, eliding the `Map` when it is already the
/// identity over the `payload_n` payload columns sitting at `offset` (the leading
/// key region the engine carries verbatim). The shared skip-the-map contract of
/// every combine emit tail.
pub(crate) fn apply_projection(
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    slots: &[usize],
    payload_n: usize,
    offset: usize,
) -> gnitz_core::NodeId {
    if slots.len() == payload_n && slots.iter().enumerate().all(|(i, &p)| p == i + offset) {
        node
    } else {
        cb.map(node, slots)
    }
}

/// A `SegInput` reading a `Get` directly — its delta source is the relation
/// itself and its layout the Get's minted `ColId`s. `None` for any non-`Get` node
/// (which must instead be cut to a hidden segment). The one home for the
/// `Get → SegInput` mapping every combine input and linear source shares.
pub(crate) fn seginput_of_get(rel: &RelExpr) -> Option<SegInput> {
    let RelExpr::Get {
        tid,
        schema,
        cols,
        desc,
    } = rel
    else {
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
            match source.as_ref() {
                // The cut materializes the whole pre-WHERE combine output at full
                // width in a hidden, unbounded segment, so a capacity here would
                // bound a small projection sitting on an unbounded copy of the
                // same rows.
                _ if projection_is_computed(items) && !fuses_computed_projection(source) => {
                    "a computed projection over a combine"
                }
                // Eligible: filter/projection over one relation.
                RelExpr::Get { .. } => return Ok(()),
                RelExpr::Join {
                    kind: JoinType::Inner,
                    on,
                    ..
                } => match on.class()?.range.is_some() {
                    // A range/band join's null-fill threshold pipeline is not
                    // replayable per key.
                    true => "a range or band join",
                    // Eligible: plain inner equi-join.
                    false => return Ok(()),
                },
                RelExpr::Join {
                    kind: JoinType::Left | JoinType::Right | JoinType::Full,
                    ..
                } => "an outer join",
                RelExpr::Join {
                    kind: JoinType::Semi | JoinType::Anti,
                    ..
                } => "EXISTS / NOT EXISTS / IN",
                RelExpr::Join {
                    kind: JoinType::Mark, ..
                } => "a mark join (IN / ANY over a subquery)",
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
                    kind: JoinType::Semi | JoinType::Anti,
                    ..
                } => exists::lower_semi_anti_view(chain, memo, items, fpreds, source),
                RelExpr::Join {
                    kind: JoinType::Mark, ..
                } => exists::lower_mark_view(chain, memo, items, fpreds, source),
                RelExpr::Join { .. } => join::lower_join_view(chain, memo, items, fpreds, source),
                RelExpr::Reduce { .. } => reduce::lower_reduce(chain, memo, items, fpreds, source),
                // A derived table (a projection, DISTINCT, or set operation) is not a
                // delta source the linear path can read in place, so it cuts even for
                // a bare-column projection.
                RelExpr::Project { .. } | RelExpr::Distinct { .. } | RelExpr::SetOp { .. } => {
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
/// A `Get` (the linear expr-map), a `Reduce` (its finalize map), and a `Mark` join
/// (its per-branch map, which already substitutes the mark constant) all emit an
/// expr-map and so fuse. Every other join emits a bare column projection, and a
/// derived table is not a delta source at all — both cut instead. One predicate, so
/// a new combine shell cannot silently inherit the wrong answer.
fn fuses_computed_projection(source: &RelExpr) -> bool {
    match source {
        RelExpr::Get { .. } | RelExpr::Reduce { .. } => true,
        RelExpr::Join { kind, .. } => *kind == JoinType::Mark,
        _ => false,
    }
}

/// Cut a `Join` source to a hidden segment (pruned to the columns the WHERE +
/// projection read) and lower the computed projection — with the WHERE applied —
/// as a linear body over a synthetic `Get` on that segment. This is the
/// scalar-decorrelation finalize: the finalize composite and the outer WHERE both
/// run over the materialized join output.
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
    collect_live_cols(items.iter().map(|i| &i.expr).chain(where_preds), &mut live);
    let seg = cut_segment(chain, memo, source, &live)?;
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
/// itself); every other shape — a nested combine, or a linear spine whose
/// projection must be materialized first — is cut to a hidden segment.
pub(crate) fn resolve_input(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    match seginput_of_get(input) {
        Some(seg) => Ok(seg),
        None => cut_segment(chain, memo, input, live),
    }
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
    let (seg_vid, seg_schema, layout) = chain.add_segment(|chain| lower_body(chain, memo, &body))?;
    let seg = SegInput {
        tid: seg_vid,
        schema: seg_schema,
        layout,
        desc: None,
    };
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
        _ => identity_project(subtree, |c| live.contains(&c.id)),
    }
}

/// Resolve the source-collision rule over a combine's already-resolved inputs:
/// a circuit's delta inputs must carry distinct source ids, so a repeated `tid`
/// wraps the later side in an identity pass-through segment. One home for the
/// self-join wrapper and the same-relation INTERSECT/EXCEPT wrapper — it compares
/// **resolved** tids, so a side that already became its own segment is correctly
/// seen as distinct and never wrapped redundantly.
///
/// `exempt` skips the rule for UNION / UNION ALL: those are linear merges the dag
/// explicitly drives by cloning one epoch's delta to both sides, and `a UNION a`
/// compiles unwrapped today — wrapping would regress a working physical plan.
pub(crate) fn resolve_collisions(
    chain: &mut ViewChain,
    inputs: &mut [SegInput],
    sources: &[&Rc<RelExpr>],
    exempt: bool,
) -> Result<(), GnitzSqlError> {
    if exempt {
        return Ok(());
    }
    let mut seen: HashSet<u64> = HashSet::new();
    for (i, input) in inputs.iter_mut().enumerate() {
        if seen.insert(input.tid) {
            continue;
        }
        // Only a bare `Get` can collide — a cut segment carries a fresh vid.
        let get = base_get(sources[i]);
        if !matches!(get.as_ref(), RelExpr::Get { .. }) {
            return Err(GnitzSqlError::Internal(
                "HIR source collision on a non-Get input".into(),
            ));
        }
        *input = wrap_passthrough_segment(chain, get)?;
        seen.insert(input.tid);
    }
    Ok(())
}

/// The `Get` at the base of a linear spine, else the node itself.
fn base_get(rel: &Rc<RelExpr>) -> &Rc<RelExpr> {
    match rel.as_ref() {
        RelExpr::Filter { input, .. } | RelExpr::Project { input, .. } => base_get(input),
        _ => rel,
    }
}

/// An identity `Project` over the columns of `subtree` that satisfy `keep` — the
/// HIR spelling of `SELECT <cols> FROM <subtree>`, shared by the segment-cut wrap
/// (live cols) and the collision pass-through wrap (visible cols).
fn identity_project(subtree: &Rc<RelExpr>, keep: impl Fn(&super::HirCol) -> bool) -> Rc<RelExpr> {
    let items = RelExpr::passthrough_items(subtree.cols().into_iter().filter(|c| keep(c)));
    RelExpr::project(Rc::clone(subtree), items)
}

/// Wrap a `Get` in an identity pass-through segment and return a `SegInput` over
/// it — the collision-resolution device shared by the self-join wrapper and the
/// same-relation set-op/DISTINCT wrapper.
///
/// Built as a HIR identity `Project` over the `Get`'s **visible** columns and
/// lowered through `lower_linear`, so the wrapper is an ordinary linear body: the
/// PK-front convention and the resulting `ColId` layout both come from
/// `physicalize_projection`, the one home, instead of being hand-rolled here
/// against a separately-synthesized `SELECT *`.
fn wrap_passthrough_segment(chain: &mut ViewChain, get: &Rc<RelExpr>) -> Result<SegInput, GnitzSqlError> {
    // Visible columns only — `place_pk_front` re-prepends an unprojected source
    // PK (staying hidden), exactly as a `SELECT *` view body would.
    let body = identity_project(get, |c| !c.def.is_hidden);
    let RelExpr::Project { items, .. } = body.as_ref() else {
        unreachable!("identity_project builds a Project");
    };
    let src = seginput_of_get(get).expect("pass-through wrapper receives a Get");
    let (wrap_vid, wrap_schema, layout) = chain.add_segment(|_chain| lower_linear(&src, &[], items))?;
    Ok(SegInput {
        tid: wrap_vid,
        schema: wrap_schema,
        layout,
        desc: None,
    })
}

/// Split an optional `Filter` off a node, returning its conjuncts (empty when
/// absent) and the source below it. Hands back the source as the `Rc` every caller
/// holds anyway — a cut/exists path needs to clone it, and a `&RelExpr` deref-coerces
/// for the rest — so this is the one home for the peel.
pub(crate) fn split_filter(input: &Rc<RelExpr>) -> (&[HirExpr], &Rc<RelExpr>) {
    match input.as_ref() {
        RelExpr::Filter { input, preds } => (preds, input),
        _ => (&[], input),
    }
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
    let folded = physical::fold_preds(filter_preds, &src.layout)?;
    let bound = extract_scan_bound(&folded, src);
    let proj = physical::physicalize_projection(proj_items, &src.layout, &src.schema)?;
    let pieces = linear::emit_linear(src, bound, folded, &proj)?;
    Ok((pieces, proj.layout))
}

/// Resolve `preds` against `layout`, AND-fold them, compile the program, and emit
/// the filter — passing `node` through untouched when there is nothing to filter
/// or the predicate folds to a constant true. Shared by the emits that fold and
/// compile in one step: HAVING, the join and EXISTS residuals, and a set-op
/// segment's WHERE. A primary-position WHERE does not come through here —
/// `linear` and `reduce` fold it earlier, because the scan bound reads that same
/// folded predicate, and compile it themselves.
pub(crate) fn emit_filter<'a>(
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    preds: impl IntoIterator<Item = &'a HirExpr>,
    layout: &[ColId],
    cols: &[ColumnDef],
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    match physical::fold_preds(preds, layout)? {
        Some(folded) => match crate::expr_lower::compile_filter_program(&folded, cols)? {
            Some(prog) => Ok(cb.filter(node, Some(prog))),
            None => Ok(node),
        },
        None => Ok(node),
    }
}

/// Scan-bound extraction — a primary-position lowering decision (source and the
/// resolved WHERE both in hand). Only a catalog source with a folded WHERE bounds.
/// Shared by every primary-position `Get` lowering (`lower_linear` here,
/// `reduce::lower_reduce`'s inline-source arm).
pub(crate) fn extract_scan_bound(folded: &Option<crate::ir::BoundExpr>, src: &SegInput) -> Option<ScanBound> {
    let (Some(f), Some(desc)) = (folded, src.desc.as_ref()) else {
        return None;
    };
    // The head candidate outright: this path compiles no residual — the `Filter`
    // is emitted verbatim either way — so there is nothing a later candidate
    // could express that the best-ranked one cannot.
    ranked_index_bounds(f, &src.schema, &desc.indexes)
        .into_iter()
        .next()
        .map(|c| ScanBound {
            idx_cols: c.idx_cols,
            desc: c.desc,
        })
}
