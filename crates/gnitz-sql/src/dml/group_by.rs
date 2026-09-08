//! The ad-hoc read path's aggregate / DISTINCT fold shape (consumed by
//! `dml::select`).
//!
//! A grouped or `SELECT DISTINCT` body is **not bound here.** It goes through
//! the one front end — `hir::bind_and_lower_fold`, the same binder and the same
//! lowering the equivalent `CREATE VIEW` body uses — and this module only turns
//! the layout that comes back into the executable [`FoldShape`]: compile the
//! HAVING and finalize expressions, build the output schema, and apply the two
//! limits that are the ad-hoc *sink's* own rather than the query's (the partial
//! reply's column width, and a duplicate output name).

use crate::error::GnitzSqlError;
use crate::exec::agg_finish::{build_agg_out_schema, FinalizeItem, FoldShape};
use crate::exec::order::{order_exprs, wire_order, OrderKey};
use crate::expr_lower::{compile_conjuncts_evaluator, compile_scalar_evaluator};
use crate::hir::bind_and_lower_fold;
use crate::ir::BoundExpr;
use crate::validate::reject_duplicate_projection_names;
use gnitz_core::{ColumnDef, Schema};
use sqlparser::ast::Select;
use std::sync::Arc;

/// The physical layout and reply schemas a GROUP BY / global aggregate / HAVING /
/// DISTINCT read folds under, plus its ORDER BY resolved over that layout — a
/// pure function of the AST and the source schema. A shape the fold cannot
/// express is a feature-named `Unsupported`; a binder's own propagates.
pub(crate) fn build_fold_shape(
    select: &Select,
    schema: &Arc<Schema>,
    alias: &str,
    keys: &[OrderKey<'_>],
) -> Result<(FoldShape, Vec<gnitz_wire::OrderKey>), GnitzSqlError> {
    // The partial reply's width gate ran in the lowering, which is where the
    // schema it bounds is built.
    let (pieces, order_cols) = bind_and_lower_fold(select, schema, alias, &order_exprs(keys))?;

    // HAVING and the finalize items are both expressions over the raw reduce
    // output, compiled against the partial reply schema with the same
    // `BoundExpr → Evaluator` pipeline a grouped view's post-reduce FILTER and
    // MAP use. Compiling here (rather than in the client finish) keeps every
    // rejection pre-dispatch, and it is the same rejection either path gives: a
    // predicate that fails to compile here fails as a view too — including a
    // wide literal, which `OpcodeBackend::lower` rejects with the message that
    // names it.
    let having = compile_conjuncts_evaluator(&pieces.having, &pieces.partial_schema)?;

    let mut finalize = Vec::with_capacity(pieces.finalize.len());
    let mut out_cols = Vec::with_capacity(pieces.finalize.len());
    for (expr, def) in &pieces.finalize {
        finalize.push(finalize_item(expr, def, &pieces.partial_schema)?);
        out_cols.push(def.clone());
    }
    // Over what THIS sink outputs. A view compile gates `lower_reduce`'s output,
    // which also carries the group columns — a different column set, so the two
    // gates legitimately disagree: `SELECT COUNT(*) AS kind FROM t GROUP BY kind`
    // is `[kind, kind]` as a view and refused, `[kind]` here and accepted.
    let ctx = if select.distinct.is_some() {
        "SELECT DISTINCT"
    } else {
        "aggregate SELECT"
    };
    reject_duplicate_projection_names(&select.projection, out_cols.iter(), ctx)?;

    let (out_schema, base) = build_agg_out_schema(out_cols)?;
    let order = wire_order(keys, &out_schema, &order_cols, base)?;

    Ok((
        FoldShape {
            reduce_schema: pieces.reduce_schema,
            group_positions: pieces.group_positions,
            agg_specs: pieces.agg_specs,
            pre: pieces.pre,
            out_schema,
            partial_schema: Arc::new(pieces.partial_schema),
            having,
            finalize,
        },
        order,
    ))
}

/// Classify one finalize item. A bare reference to a reduce-output column of the
/// output's own type is a byte move; everything else — an aggregate's finalize
/// composite (AVG's divide, a nullable SUM's null gate), or any expression over
/// group columns and aggregates — is compiled to one evaluated register.
///
/// The type equality is what makes the pass-through arm a *byte* move: it copies
/// `wire_stride(out type)` bytes out of the source column, so a mismatch there
/// would truncate or over-read. Nothing produces one today (a raw column's type
/// is the output's), and an item that ever did takes the evaluator instead of
/// mis-copying.
fn finalize_item(expr: &BoundExpr, def: &ColumnDef, partial: &Schema) -> Result<FinalizeItem, GnitzSqlError> {
    if let BoundExpr::ColRef(ci) = expr {
        if partial.columns[*ci].type_code == def.type_code {
            return Ok(FinalizeItem::PassThrough { partial_ci: *ci });
        }
    }
    Ok(FinalizeItem::Computed {
        ev: Box::new(compile_scalar_evaluator(expr, partial)?),
    })
}
