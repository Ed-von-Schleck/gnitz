//! HIR → circuit lowering. Walks the tree once and emits one circuit per
//! combine-class node (plus the linear nodes around it), cutting nested
//! combine-class subtrees to hidden segments.
//!
//! This module owns the three rules the whole lowering obeys, one home each, so
//! the per-node shells (`join`/`reduce`/`setop`) carry only their own emission:
//!
//! * the **segment-cut** rule — [`resolve_input`] / [`cut_segment`], memoized by
//!   `Rc::as_ptr` through one compilation-wide [`CutMemo`];
//! * the **source-collision** rule — [`resolve_collisions`], comparing *resolved*
//!   tids and wrapping a repeat in a pass-through segment;
//! * the **exchange-topology** backstop, asserted once per emitted circuit inside
//!   `ViewChain::add_segment` and at the final emit.
//!
//! The linear path resolves the HIR to its physical inputs (source, scan bound,
//! folded predicate, physicalized projection) and delegates to
//! `linear::emit_linear` — one home for the emission strategy; only the
//! scan-bound extraction and the `HirExpr → BoundExpr` resolution are HIR work.

pub(crate) mod exists;
pub(crate) mod join;
mod linear;
pub(crate) mod prims;
pub(crate) mod reduce;
pub(crate) mod setop;

use super::chain::{EmitPieces, ViewChain};
use super::physical;
use super::JoinType;
use super::{ColId, HirExpr, HirRef, ProjEntry, RelExpr};
use crate::access::best_index_bound;
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::{ColumnDef, GnitzClient, Schema};
use gnitz_wire::ScanBound;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// A resolved combine input: its delta source, registered schema, and the `ColId`
/// layout (physical column order) against which key / group / projection
/// references resolve. A base/segment `Get` maps directly; a cut combine subtree
/// resolves to its hidden segment via [`cut_segment`]. (Generalizes the join
/// builder's original `JoinInput`.)
#[derive(Clone)]
pub(crate) struct SegInput {
    pub tid: u64,
    pub schema: Rc<Schema>,
    pub layout: Vec<ColId>,
    /// Whether `tid` names a catalog relation rather than a chain-minted segment.
    /// Every catalog probe (scan-bound index lookup, replication) is gated on it:
    /// a segment's vid is not in `TABLE_TAB` yet, so probing it is a full catalog
    /// scan that can only answer by fall-through.
    pub from_catalog: bool,
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
        from_catalog,
    } = rel
    else {
        return None;
    };
    Some(SegInput {
        tid: *tid,
        schema: Rc::clone(schema),
        layout: cols.iter().map(|c| c.id).collect(),
        from_catalog: *from_catalog,
    })
}

/// Lower a bound + classified `RelExpr` tree to circuit pieces for the
/// pre-allocated `view_id`.
pub(crate) fn lower(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    rel: Rc<RelExpr>,
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let mut memo = CutMemo::new();
    let (pieces, _layout) = lower_body(client, chain, &mut memo, &rel, view_id)?;
    Ok(pieces)
}

/// Lower a complete view body — linear or combine-class — returning the circuit
/// pieces plus the output `ColId` layout ([`ColId::NONE`] for each hidden
/// synthetic-key slot, the real output id for each visible column). The layout is
/// what a cut segment exposes to its parent, so every body shape is cuttable by
/// construction.
fn lower_body(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &Rc<RelExpr>,
    view_id: u64,
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
                    lower_computed_over_combine(client, chain, memo, items, fpreds, source, view_id)
                }
                RelExpr::Get { .. } => {
                    let src = seginput_of_get(source).expect("Get arm resolves to a SegInput");
                    lower_linear(client, &src, fpreds, items, view_id)
                }
                RelExpr::Join {
                    kind: JoinType::Semi | JoinType::Anti,
                    ..
                } => exists::lower_semi_anti_view(client, chain, memo, items, fpreds, source, view_id),
                RelExpr::Join {
                    kind: JoinType::Mark, ..
                } => exists::lower_mark_view(client, chain, memo, items, fpreds, source, view_id),
                RelExpr::Join { .. } => join::lower_join_view(client, chain, memo, items, fpreds, source, view_id),
                RelExpr::Reduce { .. } => reduce::lower_reduce(client, chain, memo, items, fpreds, source, view_id),
                // A derived table (a projection, DISTINCT, or set operation) is not a
                // delta source the linear path can read in place, so it cuts even for
                // a bare-column projection.
                RelExpr::Project { .. } | RelExpr::Distinct { .. } | RelExpr::SetOp { .. } => {
                    lower_computed_over_combine(client, chain, memo, items, fpreds, source, view_id)
                }
                _ => Err(unsupported_body()),
            }
        }
        RelExpr::Distinct { input } => setop::lower_distinct(client, chain, memo, input, view_id),
        RelExpr::SetOp { .. } => setop::lower_setop(client, chain, memo, rel, view_id),
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
fn lower_computed_over_combine(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    items: &[ProjEntry],
    where_preds: &[HirExpr],
    source: &Rc<RelExpr>,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let mut live: HashSet<ColId> = HashSet::new();
    collect_live_cols(items.iter().map(|i| &i.expr).chain(where_preds), &mut live);
    let seg = cut_segment(client, chain, memo, source, &live)?;
    lower_linear(client, &seg, where_preds, items, view_id)
}

/// A body shape the driver has no arm for. Every shape bind can produce is
/// covered, so this is an internal invariant break, not a user-facing limit —
/// phrase it as one rather than as an engine restriction.
fn unsupported_body() -> GnitzSqlError {
    GnitzSqlError::Plan("internal: HIR lowering has no body arm for this node".into())
}

/// Resolve one input of a combine node to a `SegInput` — **the** segment-cut
/// rule, in one place rather than re-derived as a structural match per lowering
/// shell. A bare `Get` is read in place (the delta source is the relation
/// itself); every other shape — a nested combine, or a linear spine whose
/// projection must be materialized first — is cut to a hidden segment.
pub(crate) fn resolve_input(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    match seginput_of_get(input) {
        Some(seg) => Ok(seg),
        None => cut_segment(client, chain, memo, input, live),
    }
}

/// Cut a subtree to a hidden segment and return a `SegInput` over it. A subtree
/// that is not already a complete body (a bare `Join`, a `Filter`, a `Get`) is
/// wrapped in an identity `Project` over its **live** cols — the chain-liveness
/// prune, so the registered segment schema carries only what the parent demands.
/// Memoized by `Rc::as_ptr`: one segment per shared subtree.
pub(crate) fn cut_segment(
    client: &mut GnitzClient,
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
    let (seg_vid, seg_schema, layout) =
        chain.add_segment(client, |client, chain, vid| lower_body(client, chain, memo, &body, vid))?;
    let seg = SegInput {
        tid: seg_vid,
        schema: seg_schema,
        layout,
        from_catalog: false,
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
    client: &mut GnitzClient,
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
            return Err(GnitzSqlError::Plan(
                "internal: HIR source collision on a non-Get input".into(),
            ));
        }
        *input = wrap_passthrough_segment(client, chain, get)?;
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
    let items: Vec<ProjEntry> = subtree
        .cols()
        .into_iter()
        .filter(|c| keep(c))
        .map(|c| ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(c.id)),
            out: c,
        })
        .collect();
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
fn wrap_passthrough_segment(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    get: &Rc<RelExpr>,
) -> Result<SegInput, GnitzSqlError> {
    // Visible columns only — `place_pk_front` re-prepends an unprojected source
    // PK (staying hidden), exactly as a `SELECT *` view body would.
    let body = identity_project(get, |c| !c.def.is_hidden);
    let RelExpr::Project { items, .. } = body.as_ref() else {
        unreachable!("identity_project builds a Project");
    };
    let src = seginput_of_get(get).expect("pass-through wrapper receives a Get");
    let (wrap_vid, wrap_schema, layout) = chain.add_segment(client, |client, _chain, vid| {
        lower_linear(client, &src, &[], items, vid)
    })?;
    Ok(SegInput {
        tid: wrap_vid,
        schema: wrap_schema,
        layout,
        from_catalog: false,
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
pub(crate) fn lower_linear(
    client: &mut GnitzClient,
    src: &SegInput,
    filter_preds: &[HirExpr],
    proj_items: &[ProjEntry],
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let folded = physical::fold_preds(filter_preds, &src.layout)?;
    let bound = extract_scan_bound(client, &folded, src.from_catalog, src.tid, &src.schema)?;
    let proj = physical::physicalize_projection(proj_items, &src.layout, &src.schema)?;
    let pieces = linear::emit_linear(view_id, src, bound, folded, &proj)?;
    Ok((pieces, proj.layout))
}

/// Resolve `preds` against `layout`, AND-fold them, compile the program, and emit
/// the filter — passing `node` through untouched when there is nothing to filter
/// or the predicate folds to a constant true. The one home of that idiom, which
/// every WHERE / HAVING / residual emit in the lowering shares (the HIR analogue
/// of `predicates::and_fold_compile`).
pub(crate) fn emit_filter<'a>(
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    preds: impl IntoIterator<Item = &'a HirExpr>,
    layout: &[ColId],
    cols: &[ColumnDef],
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    match physical::fold_preds(preds, layout)? {
        Some(folded) => match crate::lower::compile_filter_program(&folded, cols)? {
            Some(prog) => Ok(cb.filter(node, Some(prog))),
            None => Ok(node),
        },
        None => Ok(node),
    }
}

/// Scan-bound extraction — a primary-position lowering decision (client, tid,
/// schema, and the resolved WHERE all in hand). Only a catalog source with a
/// folded WHERE bounds. Shared by every primary-position `Get` lowering
/// (`lower_linear` here, `reduce::lower_reduce`'s inline-source arm).
pub(crate) fn extract_scan_bound(
    client: &mut GnitzClient,
    folded: &Option<crate::ir::BoundExpr>,
    from_catalog: bool,
    tid: u64,
    schema: &Schema,
) -> Result<Option<ScanBound>, GnitzSqlError> {
    match (folded, from_catalog) {
        (Some(f), true) => Ok(
            best_index_bound(f, schema, || client.table_indexes(tid))?.map(|c| ScanBound {
                idx_cols: c.idx_cols,
                desc: c.desc,
            }),
        ),
        _ => Ok(None),
    }
}
