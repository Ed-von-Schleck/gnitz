//! HIR → circuit lowering. Walks the tree once and emits one circuit per
//! combine-class node (plus the linear nodes around it), cutting nested
//! combine-class subtrees to hidden segments.
//!
//! The linear path resolves the HIR to a `plan/lp.rs::Rel` and delegates to
//! `simple::emit_linear` — one home for the emission strategy and the
//! backfill-seeding rule; only the scan-bound extraction and the
//! `HirExpr → BoundExpr` resolution are HIR work.

pub(crate) mod join;

use super::physical;
use super::{ColId, ColIdGen, HirExpr, ProjEntry, RelExpr};
use crate::access::best_index_bound;
use crate::error::GnitzSqlError;
use crate::plan::lp::Rel;
use crate::plan::view::{simple, EmitPieces, ViewChain};
use gnitz_core::GnitzClient;
use gnitz_wire::ScanBound;
use std::rc::Rc;

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
    // A lowered view is rooted at a Project (bind appends one unconditionally).
    let RelExpr::Project { input, items } = rel.as_ref() else {
        return Err(GnitzSqlError::Plan("HIR lower: expected a Project root".into()));
    };
    let (filter_preds, source) = split_filter(input);
    match source {
        RelExpr::Get { .. } => lower_linear(client, chain, source, filter_preds, items, view_id),
        RelExpr::Join { .. } => join::lower_join_view(client, chain, &mut ids, items, filter_preds, source, view_id),
        _ => Err(GnitzSqlError::Unsupported("HIR lower: unsupported source shape".into())),
    }
}

/// Split an optional `Filter` off the Project's input, returning its conjuncts
/// (empty when absent) and the source below it.
fn split_filter(input: &RelExpr) -> (&[HirExpr], &RelExpr) {
    match input {
        RelExpr::Filter { input, preds } => (preds, input.as_ref()),
        other => (&[], other),
    }
}

/// Lower `Project(Filter?(Get))`: resolve the WHERE against the Get layout,
/// extract the scan bound, physicalize the projection, and hand the resulting
/// `Rel` to `simple::emit_linear`.
fn lower_linear(
    client: &mut GnitzClient,
    chain: &mut ViewChain,
    get: &RelExpr,
    filter_preds: &[HirExpr],
    proj_items: &[ProjEntry],
    view_id: u64,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Get {
        tid,
        schema,
        cols,
        from_catalog,
    } = get
    else {
        unreachable!("lower_linear receives a Get");
    };
    // The Get layout mirrors the base schema (fresh ids in schema order); the
    // Filter above it is pass-through, so the same layout serves both.
    let layout: Vec<ColId> = cols.iter().map(|c| c.id).collect();

    // Resolve the WHERE conjuncts against the Get layout and AND-fold into one
    // predicate — semantically identical to binding the whole WHERE at once.
    let folded = physical::fold_preds(filter_preds, &layout)?;

    // Scan bound — a primary-position lowering decision (client, tid, schema, and
    // the resolved WHERE all in hand). Only a catalog source with a WHERE bounds.
    let bound: Option<ScanBound> = match (&folded, *from_catalog) {
        (Some(f), true) => best_index_bound(f, schema, || client.table_indexes(*tid))?.map(|c| ScanBound {
            idx_cols: c.idx_cols,
            desc: c.desc,
        }),
        _ => None,
    };

    let (items, out_cols, pk_arity) = physical::physicalize_projection(proj_items, &layout, schema)?;

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
        items,
        out_cols,
        pk_arity,
    };
    simple::emit_linear(chain, view_id, rel)
}
