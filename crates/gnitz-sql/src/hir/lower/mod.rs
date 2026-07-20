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
//! The linear path resolves the HIR to a `plan/lp.rs::Rel` and delegates to
//! `simple::emit_linear` — one home for the emission strategy and the
//! backfill-seeding rule; only the scan-bound extraction and the
//! `HirExpr → BoundExpr` resolution are HIR work.

pub(crate) mod join;
pub(crate) mod reduce;
pub(crate) mod setop;

use super::physical;
use super::{ColId, ColIdGen, HirExpr, HirRef, ProjEntry, RelExpr};
use crate::access::best_index_bound;
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use crate::plan::lp::Rel;
use crate::plan::view::{simple, EmitPieces, ViewChain};
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

/// The `Rc::as_ptr` cut memo, threaded through the whole lowering (alongside
/// `ids`) so a subtree referenced from two places — a shared CTE, a sub-join
/// reachable from two parents — is cut to **one** hidden segment and every
/// reference resolves to it. Per-shell memos would defeat that, which is the
/// only reason the rewrites preserve `Rc` identity at all.
pub(crate) type CutMemo = HashMap<*const RelExpr, SegInput>;

/// Lower a bound + classified `RelExpr` tree to circuit pieces for the
/// pre-allocated `view_id`. `ids` continues bind's `ColId` counter (for a cut
/// segment's hidden-key placeholder ids).
pub(crate) fn lower(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    rel: Rc<RelExpr>,
    mut ids: ColIdGen,
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let mut memo = CutMemo::new();
    let (pieces, _layout) = lower_body(client, chain, &mut ids, &mut memo, &rel, view_id)?;
    Ok(pieces)
}

/// Lower a complete view body — linear or combine-class — returning the circuit
/// pieces plus the output `ColId` layout (a placeholder id for each hidden
/// synthetic-key slot, the real output id for each visible column). The layout is
/// what a cut segment exposes to its parent, so every body shape is cuttable by
/// construction.
fn lower_body(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    rel: &Rc<RelExpr>,
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Project { input, items } => {
            let (fpreds, source) = split_filter(input);
            match source {
                RelExpr::Get { .. } => lower_linear(client, chain, ids, source, fpreds, items, view_id),
                RelExpr::Join { .. } => join::lower_join_view(client, chain, ids, memo, items, fpreds, source, view_id),
                RelExpr::Reduce { .. } => {
                    reduce::lower_reduce(client, chain, ids, memo, items, fpreds, source, view_id)
                }
                other => Err(unsupported_body(other)),
            }
        }
        RelExpr::Distinct { input } => setop::lower_distinct(client, chain, ids, memo, input, view_id),
        RelExpr::SetOp { .. } => setop::lower_setop(client, chain, ids, memo, rel, view_id),
        other => Err(unsupported_body(other)),
    }
}

/// A body shape the driver has no arm for. Every shape bind can produce is
/// covered, so this is an internal invariant break, not a user-facing limit —
/// phrase it as one rather than as an engine restriction.
fn unsupported_body(rel: &RelExpr) -> GnitzSqlError {
    let kind = match rel {
        RelExpr::Get { .. } => "Get",
        RelExpr::Filter { .. } => "Filter",
        RelExpr::Project { .. } => "Project",
        RelExpr::Join { .. } => "Join",
        RelExpr::Reduce { .. } => "Reduce",
        RelExpr::Distinct { .. } => "Distinct",
        RelExpr::SetOp { .. } => "SetOp",
    };
    GnitzSqlError::Plan(format!("internal: HIR lowering has no body arm for {kind}"))
}

/// Resolve one input of a combine node to a `SegInput` — **the** segment-cut
/// rule, in one place rather than re-derived as a structural match per lowering
/// shell. A bare `Get` is read in place (the delta source is the relation
/// itself); every other shape — a nested combine, or a linear spine whose
/// projection must be materialized first — is cut to a hidden segment.
pub(crate) fn resolve_input(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    match input.as_ref() {
        RelExpr::Get {
            tid,
            schema,
            cols,
            from_catalog,
        } => Ok(SegInput {
            tid: *tid,
            schema: Rc::clone(schema),
            layout: cols.iter().map(|c| c.id).collect(),
            from_catalog: *from_catalog,
        }),
        _ => cut_segment(client, chain, ids, memo, input, live),
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
    ids: &mut ColIdGen,
    memo: &mut CutMemo,
    subtree: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    let key = Rc::as_ptr(subtree);
    if let Some(cached) = memo.get(&key) {
        return Ok(cached.clone());
    }
    let body = as_body(subtree, live);
    let mut captured: Option<Vec<ColId>> = None;
    let (seg_vid, seg_schema) = chain.add_segment(client, |client, chain, vid| {
        let (pieces, layout) = lower_body(client, chain, ids, memo, &body, vid)?;
        captured = Some(layout);
        Ok(pieces)
    })?;
    let seg = SegInput {
        tid: seg_vid,
        schema: seg_schema,
        layout: captured.expect("cut segment layout captured"),
        from_catalog: false,
    };
    memo.insert(key, seg.clone());
    Ok(seg)
}

/// A subtree as a complete, lowerable body: `Project`/`Distinct`/`SetOp` already
/// are one; anything else is wrapped in an identity `Project` over its live cols.
fn as_body(subtree: &Rc<RelExpr>, live: &HashSet<ColId>) -> Rc<RelExpr> {
    match subtree.as_ref() {
        RelExpr::Project { .. } | RelExpr::Distinct { .. } | RelExpr::SetOp { .. } => Rc::clone(subtree),
        _ => {
            let items: Vec<ProjEntry> = subtree
                .cols()
                .into_iter()
                .filter(|c| live.contains(&c.id))
                .map(|c| ProjEntry {
                    expr: BExpr::ColRef(HirRef::Col(c.id)),
                    out: c,
                })
                .collect();
            RelExpr::project(Rc::clone(subtree), items)
        }
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
    ids: &mut ColIdGen,
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
        if !matches!(get, RelExpr::Get { .. }) {
            return Err(GnitzSqlError::Plan(
                "internal: HIR source collision on a non-Get input".into(),
            ));
        }
        *input = wrap_passthrough_segment(client, chain, ids, get)?;
        seen.insert(input.tid);
    }
    Ok(())
}

/// The `Get` at the base of a linear spine, else the node itself.
fn base_get(rel: &Rc<RelExpr>) -> &RelExpr {
    match rel.as_ref() {
        RelExpr::Filter { input, .. } | RelExpr::Project { input, .. } => base_get(input),
        other => other,
    }
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
pub(crate) fn wrap_passthrough_segment(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    get: &RelExpr,
) -> Result<SegInput, GnitzSqlError> {
    // Visible columns only — `place_pk_front` re-prepends an unprojected source
    // PK (staying hidden), exactly as a `SELECT *` view body would.
    let items: Vec<ProjEntry> = get
        .cols()
        .into_iter()
        .filter(|c| !c.def.is_hidden)
        .map(|c| ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(c.id)),
            out: c,
        })
        .collect();
    let mut captured: Option<Vec<ColId>> = None;
    let (wrap_vid, wrap_schema) = chain.add_segment(client, |client, chain, vid| {
        let (pieces, layout) = lower_linear(client, chain, ids, get, &[], &items, vid)?;
        captured = Some(layout);
        Ok(pieces)
    })?;
    Ok(SegInput {
        tid: wrap_vid,
        schema: wrap_schema,
        layout: captured.expect("pass-through wrapper layout captured"),
        from_catalog: false,
    })
}

/// Split an optional `Filter` off a node, returning its conjuncts (empty when
/// absent) and the source below it.
pub(crate) fn split_filter(input: &RelExpr) -> (&[HirExpr], &RelExpr) {
    match input {
        RelExpr::Filter { input, preds } => (preds, input.as_ref()),
        other => (&[], other),
    }
}

/// Lower `Project(Filter?(Get))`: resolve the WHERE against the Get layout,
/// extract the scan bound, physicalize the projection, and hand the resulting
/// `Rel` to `simple::emit_linear`. Also reused by `setop` to materialize a
/// computed set-op / DISTINCT side into a hidden linear segment.
pub(crate) fn lower_linear(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    ids: &mut ColIdGen,
    get: &RelExpr,
    filter_preds: &[HirExpr],
    proj_items: &[ProjEntry],
    view_id: u64,
) -> Result<(EmitPieces, Vec<ColId>), GnitzSqlError> {
    let RelExpr::Get {
        tid,
        schema,
        cols,
        from_catalog,
    } = get
    else {
        unreachable!("lower_linear receives a Get");
    };
    let layout: Vec<ColId> = cols.iter().map(|c| c.id).collect();
    let folded = physical::fold_preds(filter_preds, &layout)?;
    let bound = extract_scan_bound(client, &folded, *from_catalog, *tid, schema)?;
    let proj = physical::physicalize_projection(ids, proj_items, &layout, schema)?;
    let mut rel = Rel::Source {
        tid: *tid,
        schema: Rc::clone(schema),
        bound,
    };
    if let Some(pred) = folded {
        rel = Rel::Filter {
            input: Box::new(rel),
            pred,
        };
    }
    let rel = Rel::Project {
        input: Box::new(rel),
        items: proj.items,
        out_cols: proj.out_cols,
        pk_arity: proj.pk_arity,
    };
    Ok((simple::emit_linear(chain, view_id, rel)?, proj.layout))
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
