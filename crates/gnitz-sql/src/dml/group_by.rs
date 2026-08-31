//! The ad-hoc read path's aggregate / DISTINCT fold shape (consumed by
//! `dml::select`).
//!
//! A grouped body is **not bound here.** It goes through the one grouped front
//! end — `hir::bind_and_lower_fold`, the same binder and the same reduce
//! lowering a grouped `CREATE VIEW` body uses — and this module only turns the
//! layout that comes back into the executable [`FoldShape`]: compile the HAVING
//! and finalize expressions, build the output schema, and apply the two limits
//! that are the ad-hoc *sink's* own rather than the query's (the partial reply's
//! column width, and a duplicate output name).
//!
//! `SELECT DISTINCT` is the degenerate case and keeps its own resolver
//! ([`resolve_set_projection`]): it has no GROUP BY, no aggregates and no
//! HAVING to bind, so routing it through the grouped binder would buy nothing —
//! it is a projection plus a group set, which is exactly what that resolver
//! returns.

use crate::agg::fold_partial_schema;
use crate::ast_util::{aliased_def, expand_wildcard_item, is_bare_wildcard_projection, scalar_projection_item};
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::{build_agg_out_schema, FinalizeItem, FoldShape};
use crate::expr_lower::{compile_conjuncts_evaluator, compile_finalize_evaluator};
use crate::hir::bind_and_lower_fold;
use crate::ir::BoundExpr;
use crate::validate::reject_float_keys;
use gnitz_core::{ColumnDef, Schema};
use sqlparser::ast::{Select, SelectItem};
use std::sync::Arc;

/// The physical layout and reply schemas a GROUP BY / global aggregate / HAVING /
/// DISTINCT read folds under — a pure function of the AST and the source schema.
/// A shape the fold cannot express (a partial reply wider than the column limit,
/// a HAVING or finalize the shared expression compiler rejects) is a
/// feature-named `Unsupported`; a binder's own `Unsupported`/`Bind` propagates.
pub(crate) fn build_fold_shape(select: &Select, schema: &Arc<Schema>) -> Result<FoldShape, GnitzSqlError> {
    if select.distinct.is_some() {
        return distinct_fold_shape(select, schema);
    }
    // The partial reply's width gate ran in the lowering, which is where the
    // schema it bounds is built.
    let pieces = bind_and_lower_fold(select, schema)?;

    // HAVING and the finalize items are both expressions over the raw reduce
    // output, compiled against the partial reply schema with the same
    // `BoundExpr → Evaluator` pipeline a grouped view's post-reduce FILTER and
    // MAP use. Compiling here (rather than in the client finish) keeps every
    // rejection pre-dispatch, and it is the same rejection either path gives: a
    // predicate that fails to compile here fails as a view too — including a
    // wide literal, which `OpcodeBackend::lower` rejects with the message that
    // names it.
    let having_refs: Vec<&BoundExpr> = pieces.having.iter().collect();
    let having = compile_conjuncts_evaluator(&having_refs, &pieces.partial_schema)?;

    let mut finalize = Vec::with_capacity(pieces.finalize.len());
    let mut out_cols = Vec::with_capacity(pieces.finalize.len());
    for (expr, def) in &pieces.finalize {
        finalize.push(finalize_item(expr, def, &pieces.partial_schema)?);
        out_cols.push(def.clone());
    }

    Ok(FoldShape {
        reduce_schema: pieces.reduce_schema,
        group_positions: pieces.group_positions,
        agg_specs: pieces.agg_specs,
        pre_map: pieces.pre_map,
        pre_payload: pieces.pre_payload,
        out_schema: build_agg_out_schema(&out_cols)?,
        partial_schema: pieces.partial_schema,
        having,
        finalize,
    })
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
    let ev = compile_finalize_evaluator(expr, partial)?;
    let is_str = ev.result_is_str();
    Ok(FinalizeItem::Computed {
        ev: Box::new(ev),
        is_str,
    })
}

/// `SELECT DISTINCT c1, …` — a grouped fold with zero aggregates: every
/// projected column is a group column, in SELECT order, so every finalize item
/// is a pass-through of the partial layout's own payload slot (`_group_pk`
/// occupies slot 0, the group columns follow in order).
fn distinct_fold_shape(select: &Select, schema: &Arc<Schema>) -> Result<FoldShape, GnitzSqlError> {
    let (group_positions, out_cols) = resolve_set_projection(&select.projection, schema, "SELECT DISTINCT")?;
    let partial_schema = fold_partial_schema(schema, &group_positions, &[])?;
    Ok(FoldShape {
        finalize: (0..group_positions.len())
            .map(|j| FinalizeItem::PassThrough { partial_ci: 1 + j })
            .collect(),
        // No pre-map: DISTINCT groups source columns directly.
        reduce_schema: Arc::clone(schema),
        group_positions,
        agg_specs: Vec::new(),
        pre_map: Vec::new(),
        pre_payload: Vec::new(),
        partial_schema,
        out_schema: build_agg_out_schema(&out_cols)?,
        having: None,
    })
}

/// Resolve a `SELECT DISTINCT` projection to source column indices plus output
/// column definitions. Supports `SELECT *`, bare column references, and aliased
/// column references; rejects computed expressions (which have no meaningful set
/// identity) with a clean error rather than silently dropping them.
pub(crate) fn resolve_set_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
    context: &str,
) -> Result<(Vec<usize>, Vec<ColumnDef>), GnitzSqlError> {
    // Wildcard expands to *visible* columns only: an upstream synthetic key
    // (`_join_pk`, `_set_pk`, …) must not participate in DISTINCT row identity —
    // hashing it into the dedup key would keep otherwise-identical rows distinct.
    // Only a *bare* `*` takes this fast path; a `* EXCEPT/EXCLUDE/RENAME` (or a
    // rejected `* REPLACE/ILIKE`) falls into the single Wildcard arm below.
    if is_bare_wildcard_projection(projection) {
        let (indices, cols): (Vec<usize>, Vec<ColumnDef>) =
            source_schema.visible_columns().map(|(i, c)| (i, c.clone())).unzip();
        reject_float_keys(source_schema, &indices)?;
        return Ok((indices, cols));
    }
    let mut indices: Vec<usize> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                // Visible columns only (as the bare-`*` fast path); `EXCEPT`/
                // `EXCLUDE`/`RENAME` rewrite by name, `REPLACE`/`ILIKE` reject.
                for (i, out) in expand_wildcard_item(item, &source_schema.columns, context)? {
                    indices.push(i);
                    out_cols.push(out);
                }
            }
            _ => {
                let (expr, alias) = scalar_projection_item(item, context)?;
                let BoundExpr::ColRef(ci) = bind_single_table(expr, source_schema)? else {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "{context}: computed expressions are not supported"
                    )));
                };
                indices.push(ci);
                out_cols.push(aliased_def(&source_schema.columns[ci], alias));
            }
        }
    }
    // Single chokepoint: every projected column lands in `indices`, so one pass
    // here rejects a float row-identity key regardless of which SELECT-item arm
    // produced it (a new arm is covered automatically).
    reject_float_keys(source_schema, &indices)?;
    Ok((indices, out_cols))
}
