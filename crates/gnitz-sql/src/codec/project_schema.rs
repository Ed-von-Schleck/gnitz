//! Projection resolution: SELECT items → output column layout.
//!
//! `resolve_proj_col` resolves one SELECT item (column lookup + alias rename +
//! computed-expr typing) and `place_pk_front` pins the source PK to the leading
//! output slots. The CREATE VIEW linear projection composes them in
//! `hir::physical::physicalize_projection`; `build_read_projection` is the ad-hoc
//! read path's variant (PK hidden-prepended, user column order preserved).

use crate::ast_util::expand_wildcard_item;
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::lower::compile_bound_expr;
use gnitz_core::{ColumnDef, ExprBuilder, ExprProgram, Schema};
use sqlparser::ast::SelectItem;

/// One output column of a projection: a verbatim source column
/// (`PassThrough`) or a value derived by an expression (`Computed`).
pub(crate) enum ProjItem {
    PassThrough { src_col: usize },
    Computed { bound_expr: BoundExpr },
}

impl ProjItem {
    /// Classify a *resolved* expression into an emission item: a bare column ref
    /// is a zero-cost `PassThrough`, everything else a `Computed` program. The one
    /// home for the split every physicalized projection makes.
    pub(crate) fn from_bound(bound: BoundExpr) -> ProjItem {
        match bound {
            BoundExpr::ColRef(src_col) => ProjItem::PassThrough { src_col },
            bound_expr => ProjItem::Computed { bound_expr },
        }
    }
}

/// Resolve one *non-wildcard* SELECT item against `source_schema` into a
/// `(ProjItem, output ColumnDef)` pair; `idx` names an anonymous computed
/// column. A bare, qualified (`t.col`), parenthesized, or aliased column
/// reference binds to a `PassThrough` — an alias only renames the output
/// column, so an aliased or qualified PK column is still found by the
/// PK-placement scan and a qualified non-PK column is not needlessly
/// recomputed. Any other expression becomes a `Computed` column built by
/// [`ColumnDef::computed`]. `Wildcard` is expanded by the caller and rejected
/// here.
fn resolve_proj_col(
    item: &SelectItem,
    idx: usize,
    source_schema: &Schema,
) -> Result<(ProjItem, ColumnDef), GnitzSqlError> {
    let (expr, alias) = match item {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "unsupported SELECT item in projection".to_string(),
            ))
        }
    };
    let bound = bind_single_table(expr, source_schema)?;
    // A (possibly aliased / qualified / parenthesized) bare column reference is a
    // pass-through; an alias only renames the output column. Anything else is a
    // computed column, built by `ColumnDef::computed` from its `infer_type`.
    if let BoundExpr::ColRef(ci) = bound {
        let mut col = source_schema.columns[ci].clone();
        if let Some(name) = alias {
            col.name = name;
        }
        Ok((ProjItem::PassThrough { src_col: ci }, col))
    } else {
        let nominal = bound.infer_type(&source_schema.columns);
        Ok((
            ProjItem::Computed { bound_expr: bound },
            ColumnDef::computed(alias, idx, nominal),
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
            ProjItem::PassThrough { src_col } => eb.copy_col(*src_col as u32, payload_idx),
            ProjItem::Computed { bound_expr } => {
                let reg = compile_bound_expr(bound_expr, &schema.columns, &mut eb)?;
                eb.emit_col(reg, payload_idx);
            }
        }
    }
    Ok(eb.build(0))
}

/// Pin the full source PK to output slots `0..k` in `pk_indices()` order,
/// matching the engine's `build_map_output_schema` (which copies every PK
/// column to the front via `DerivedSchema::push_pk_of`). A PK column already at its
/// target slot stays; one appearing later is removed+inserted (shifting the
/// spanned non-PK columns right by one, preserving their relative order — a
/// swap would not); one absent from the projection (omitted, or referenced
/// only through a computed expression) is auto-prepended so the view carries
/// the full source PK verbatim. One loop serves every PK arity — `k == 1`
/// reduces to a single move-to-front.
///
/// Returns the permutation applied, as the pre-call index of each output slot
/// (`None` for an auto-prepended PK column, which had no pre-call slot). Callers
/// carrying a vector parallel to `out_cols` — the HIR's `ColId` layout — reorder
/// it through this instead of re-deriving the convention.
pub(crate) fn place_pk_front(
    items: &mut Vec<ProjItem>,
    out_cols: &mut Vec<ColumnDef>,
    source_schema: &Schema,
) -> Vec<Option<usize>> {
    let mut perm: Vec<Option<usize>> = (0..items.len()).map(Some).collect();
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
                let tag = perm.remove(pos);
                items.insert(target, it);
                out_cols.insert(target, col);
                perm.insert(target, tag);
            }
            None => {
                // Auto-prepended: the source PK column the user did not project.
                // It must ride the view (the physical key), but the user never
                // named it, so it is hidden — `SELECT b FROM t` no longer leaks
                // the PK `a`. (A source PK that is itself already hidden, e.g. a
                // synthetic `_join_pk`, stays hidden — `.hidden()` is idempotent.)
                items.insert(target, ProjItem::PassThrough { src_col: pk });
                out_cols.insert(target, source_schema.columns[pk].clone().hidden());
                perm.insert(target, None);
            }
        }
    }
    perm
}

/// Build the projected `(items, out_cols)` for the **ad-hoc read path**
/// (`plan_read_spec`): the full source PK is **always hidden-prepended** to slots
/// `0..k`, and every SELECT item — a projected PK column included — is a payload
/// slot in SELECT order (materialized by a `COPY_COL` / computed `EMIT`). Unlike
/// the CREATE VIEW linear path's [`place_pk_front`] (which *promotes* a projected
/// PK to the visible front), this preserves the user's column order (`SELECT a,
/// id` stays `[a, id]`) and never rejects a PK-dropping projection — the physical
/// key rides hidden.
pub(crate) fn build_read_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
) -> Result<(Vec<ProjItem>, Vec<ColumnDef>), GnitzSqlError> {
    let mut items: Vec<ProjItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();

    for (idx, item) in projection.iter().enumerate() {
        if matches!(item, SelectItem::Wildcard(_)) {
            // Hidden key slots are excluded — a `SELECT *` over a view whose key is
            // synthetic (`_join_pk`, …) must not re-admit that column into the
            // result. A hidden *source PK* is not lost: it is re-prepended (staying
            // hidden) below. `EXCEPT`/`EXCLUDE`/`RENAME` rewrite the output column
            // list per column; `REPLACE`/`ILIKE` are rejected by `for_item`.
            for (i, out) in expand_wildcard_item(item, &source_schema.columns, "SELECT")? {
                items.push(ProjItem::PassThrough { src_col: i });
                out_cols.push(out);
            }
        } else {
            let (it, col) = resolve_proj_col(item, idx, source_schema)?;
            items.push(it);
            out_cols.push(col);
        }
    }

    // The full source PK, hidden-prepended to slots `0..k`.
    for (target, &pk) in source_schema.pk_indices().iter().enumerate() {
        items.insert(target, ProjItem::PassThrough { src_col: pk });
        out_cols.insert(target, source_schema.columns[pk].clone().hidden());
    }
    Ok((items, out_cols))
}

/// The `(reply_schema, projection blob)` a `ReadSpec` rows sink replies under —
/// the one definition of that shape. `items`/`out_cols` come from
/// [`build_read_projection`] (possibly extended with hidden ORDER BY columns):
/// slots `0..k` are the source PK, so the reply's PK columns are `0..k` and the
/// compiled program fills the payload slots `items[k..]`.
///
/// A caller wanting keys only passes an empty SELECT list — the reply then
/// carries the PK region and nothing else.
pub(crate) fn read_reply_shape(
    items: &[ProjItem],
    out_cols: Vec<ColumnDef>,
    source_schema: &Schema,
) -> Result<(Schema, Vec<u8>), GnitzSqlError> {
    let k = source_schema.pk_indices().len();
    let reply_schema = Schema::from_parts(out_cols, (0..k).collect())
        .map_err(|e| GnitzSqlError::Unsupported(format!("read-spec reply schema is invalid: {e}")))?;
    Ok((
        reply_schema,
        compile_projection_map(&items[k..], source_schema)?.encode(),
    ))
}
