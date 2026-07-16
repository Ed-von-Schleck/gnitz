//! Projection resolution: SELECT items → output column layout.
//!
//! `resolve_proj_col` resolves one SELECT item (column lookup + alias rename +
//! computed-expr typing) and `place_pk_front` pins the source PK to the leading
//! output slots; `build_projection` composes them into the single-table view's
//! output layout. `build_projection`'s sole caller is the simple CREATE VIEW
//! builder (`plan::view::simple`) — the join and SET projections have their own
//! leading-column contracts and build their layouts separately.

use crate::ast_util::{wildcard_name_is_visible, WildcardRewrite};
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::lower::compile_bound_expr;
use gnitz_core::{ColumnDef, ExprBuilder, ExprProgram, Schema};
use sqlparser::ast::{Expr, SelectItem};

/// One output column of a projection: a verbatim source column
/// (`PassThrough`) or a value derived by an expression (`Computed`).
pub(crate) enum ProjItem {
    PassThrough { src_col: usize },
    Computed { bound_expr: BoundExpr },
}

/// Resolve one *non-wildcard* SELECT item against `source_schema` into a
/// `(ProjItem, output ColumnDef)` pair; `idx` names an anonymous computed
/// column. A bare, qualified (`t.col`), parenthesized, or aliased column
/// reference binds to a `PassThrough` — an alias only renames the output
/// column, so an aliased or qualified PK column is still found by the
/// PK-placement scan and a qualified non-PK column is not needlessly
/// recomputed. Any other expression becomes a `Computed` column typed by
/// `infer_type` and named by its alias (or `_expr{idx}`). `Wildcard` is
/// expanded by the caller and rejected here.
fn resolve_proj_col(
    item: &SelectItem,
    idx: usize,
    source_schema: &Schema,
) -> Result<(ProjItem, ColumnDef), GnitzSqlError> {
    resolve_proj_col_with(item, idx, source_schema, |e| bind_single_table(e, source_schema))
}

/// [`resolve_proj_col`] with a caller-supplied expression binder — the mark
/// builder binds through [`crate::bind::bind_single_table_mark`] so a projected
/// subquery resolves to its branch constant, while the pass-through/computed
/// split, naming, and typing conventions stay defined here once.
pub(crate) fn resolve_proj_col_with(
    item: &SelectItem,
    idx: usize,
    source_schema: &Schema,
    bind: impl FnOnce(&Expr) -> Result<BoundExpr, GnitzSqlError>,
) -> Result<(ProjItem, ColumnDef), GnitzSqlError> {
    let (expr, alias) = match item {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "unsupported SELECT item in CREATE VIEW projection".to_string(),
            ))
        }
    };
    let bound = bind(expr)?;
    // A (possibly aliased / qualified / parenthesized) bare column reference is a
    // pass-through; an alias only renames the output column. Anything else is a
    // computed column, typed by `infer_type` and named by its alias or `_expr{idx}`.
    if let BoundExpr::ColRef(ci) = bound {
        let mut col = source_schema.columns[ci].clone();
        if let Some(name) = alias {
            col.name = name;
        }
        Ok((ProjItem::PassThrough { src_col: ci }, col))
    } else {
        let out_type = bound.infer_type(source_schema);
        Ok((
            ProjItem::Computed { bound_expr: bound },
            ColumnDef::new(alias.unwrap_or_else(|| format!("_expr{idx}")), out_type, true),
        ))
    }
}

/// Compile the *payload* slice of a projection (`items` must exclude the
/// leading PK slots, which the engine carries verbatim) into one expr-map
/// program: a COPY_COL per pass-through, a compiled expression + EMIT per
/// computed slot. `payload_idx` is the dense output payload position. EMIT
/// writes the raw register bits via append_int — correct for float. The
/// program's `result_reg` is unused (EMIT/COPY_COL write directly).
pub(crate) fn compile_projection_map(items: &[ProjItem], schema: &Schema) -> Result<ExprProgram, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    for (payload_idx, item) in items.iter().enumerate() {
        let payload_idx = payload_idx as u32;
        match item {
            ProjItem::PassThrough { src_col } => {
                let tc = schema.columns[*src_col].type_code as u32;
                eb.copy_col(tc, *src_col as u32, payload_idx);
            }
            ProjItem::Computed { bound_expr } => {
                let reg = compile_bound_expr(bound_expr, schema, &mut eb)?;
                eb.emit_col(reg, payload_idx);
            }
        }
    }
    Ok(eb.build(0))
}

/// Pin the full source PK to output slots `0..k` in `pk_indices()` order,
/// matching the engine's `build_map_output_schema` (which copies every PK
/// column to the front via `copy_pk_columns_into`). A PK column already at its
/// target slot stays; one appearing later is removed+inserted (shifting the
/// spanned non-PK columns right by one, preserving their relative order — a
/// swap would not); one absent from the projection (omitted, or referenced
/// only through a computed expression) is auto-prepended so the view carries
/// the full source PK verbatim. One loop serves every PK arity — `k == 1`
/// reduces to a single move-to-front.
fn place_pk_front(items: &mut Vec<ProjItem>, out_cols: &mut Vec<ColumnDef>, source_schema: &Schema) {
    for (target, &pk) in source_schema.pk_indices().iter().enumerate() {
        // First occurrence is the canonical physical-PK slot; any later
        // duplicate (SELECT pk, pk AS x) stays in the payload region and is
        // materialized by the expr-map COPY_COL path.
        let cur = items
            .iter()
            .position(|i| matches!(i, ProjItem::PassThrough { src_col } if *src_col == pk));
        match cur {
            Some(pos) if pos == target => { /* already in place */ }
            Some(pos) => {
                let it = items.remove(pos);
                let col = out_cols.remove(pos);
                items.insert(target, it);
                out_cols.insert(target, col);
            }
            None => {
                // Auto-prepended: the source PK column the user did not project.
                // It must ride the view (the physical key), but the user never
                // named it, so it is hidden — `SELECT b FROM t` no longer leaks
                // the PK `a`. (A source PK that is itself already hidden, e.g. a
                // synthetic `_join_pk`, stays hidden — `.hidden()` is idempotent.)
                items.insert(target, ProjItem::PassThrough { src_col: pk });
                out_cols.insert(target, source_schema.columns[pk].clone().hidden());
            }
        }
    }
}

/// Build the projected `(items, out_cols)` for a single-table view: resolve
/// each SELECT item (expanding `SELECT *` to every source column), then pin the
/// source PK to the leading slots.
pub(crate) fn build_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
) -> Result<(Vec<ProjItem>, Vec<ColumnDef>), GnitzSqlError> {
    let mut items: Vec<ProjItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();

    for (idx, item) in projection.iter().enumerate() {
        if matches!(item, SelectItem::Wildcard(_)) {
            // No early return: `SELECT *` expands here and then flows through
            // place_pk_front below, so the source PK is pinned to slots 0..k
            // even when it is not the table's leading column. A PK already at
            // the front degenerates to the verbatim identity order.
            //
            // Hidden key slots are excluded — a `SELECT *` over a view whose key
            // is synthetic (`_join_pk`, …) must not re-admit that column into the
            // new view's payload. A hidden *source PK* is not lost: place_pk_front
            // re-prepends it below (staying hidden), so the derived view still
            // carries the full source PK verbatim.
            //
            // `EXCEPT`/`EXCLUDE`/`RENAME` rewrite the output column list per
            // column; `REPLACE`/`ILIKE` are rejected by `for_item`. A dropped
            // source PK is re-prepended (hidden) by place_pk_front, exactly like
            // `SELECT <non-pk cols>`.
            let rw = WildcardRewrite::for_item(
                item,
                |n| wildcard_name_is_visible(&source_schema.columns, n),
                "CREATE VIEW",
            )?;
            for (i, col) in source_schema.visible_columns() {
                let Some(out) = rw.rewrite_column(col) else { continue };
                items.push(ProjItem::PassThrough { src_col: i });
                out_cols.push(out);
            }
        } else {
            let (it, col) = resolve_proj_col(item, idx, source_schema)?;
            items.push(it);
            out_cols.push(col);
        }
    }

    place_pk_front(&mut items, &mut out_cols, source_schema);
    Ok((items, out_cols))
}
