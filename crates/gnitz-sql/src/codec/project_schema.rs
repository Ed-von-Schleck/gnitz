//! Projection resolution: SELECT items → output column layout.
//!
//! `resolve_proj_col` resolves one SELECT item (column lookup + alias rename +
//! computed-expr typing) and `place_pk_front` pins the source PK to the leading
//! output slots. The CREATE VIEW linear projection composes them in
//! `hir::physical::physicalize_projection`; `build_read_projection` is the ad-hoc
//! read path's variant (PK hidden-prepended, user column order preserved).

use crate::ast_util::{aliased_def, expand_wildcard_item, scalar_projection_item};
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_bound_expr;
use crate::ir::BoundExpr;
use crate::validate::reject_duplicate_projection_names;
use gnitz_core::{ColumnDef, FixedInt, Schema};
use gnitz_expr::{ExprBuilder, LogicalInstr, LogicalProgram, Sink};
use gnitz_wire::ComputeMap;
use sqlparser::ast::SelectItem;

/// One output column of a projection: a verbatim source column
/// (`PassThrough`) or a value derived by an expression (`Computed`).
///
/// Equality is "these two slots emit the same value" — `BoundExpr` compares
/// after names resolve, so `t.a + b` and `a + b` are one item.
#[derive(PartialEq)]
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

    /// The source column a pass-through copies, `None` for a computed item.
    pub(crate) fn passthrough_src(&self) -> Option<usize> {
        match self {
            ProjItem::PassThrough { src_col } => Some(*src_col),
            ProjItem::Computed { .. } => None,
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
/// [`crate::validate::computed_column`]. `Wildcard` is expanded by the caller and rejected
/// here.
fn resolve_proj_col(
    item: &SelectItem,
    idx: usize,
    source_schema: &Schema,
    rel_alias: &str,
) -> Result<(ProjItem, ColumnDef), GnitzSqlError> {
    let (expr, alias) = scalar_projection_item(item, "projection")?;
    let bound = bind_single_table(expr, source_schema, rel_alias)?;
    // The output `ColumnDef` follows the same split: a pass-through keeps the
    // source column's declaration under the written alias — which only renames
    // it — and a computed column is declared from its `infer_ty`.
    Ok(match ProjItem::from_bound(bound) {
        ProjItem::PassThrough { src_col } => (
            ProjItem::PassThrough { src_col },
            aliased_def(&source_schema.columns[src_col], alias),
        ),
        ProjItem::Computed { bound_expr } => {
            let nominal = bound_expr.infer_ty(&source_schema.columns);
            (
                ProjItem::Computed { bound_expr },
                crate::validate::computed_column(alias, idx, nominal),
            )
        }
    })
}

/// One projection payload as a [`ComputeMap`]: the compiled program plus the
/// `(type_code, nullable)` declaration of the slots it writes. The two halves
/// must describe the *same* payload slice — a program writing slot `i` under a
/// declaration for a different column emits the wrong width, silently — so they
/// are built together here and travel as one value, rather than being paired by
/// hand at each emit.
pub(crate) fn payload_map(
    items: &[ProjItem],
    cols: &[ColumnDef],
    schema: &Schema,
) -> Result<ComputeMap, GnitzSqlError> {
    Ok(ComputeMap {
        program: compile_projection_map(items, cols, schema)?.to_blob_bytes(),
        out_cols: cols.iter().map(|c| (c.type_code as u8, c.is_nullable)).collect(),
    })
}

/// Compile the *payload* slice of a projection (`items` must exclude the
/// leading PK slots, which the engine carries verbatim) into one expr-map
/// program: one sink per output payload slot, in slot order. A register sink is
/// class-agnostic here — the engine splits it by the source register's class,
/// storing the raw 8-byte image for a scalar and a German-string cell for a
/// string.
fn compile_projection_map(
    items: &[ProjItem],
    out_cols: &[ColumnDef],
    schema: &Schema,
) -> Result<LogicalProgram, GnitzSqlError> {
    debug_assert_eq!(items.len(), out_cols.len(), "one declared column per projection item");
    let mut eb = ExprBuilder::new();
    for (item, col) in items.iter().zip(out_cols) {
        match item {
            ProjItem::PassThrough { src_col } => eb.sink(Sink::Col(*src_col as u32)),
            ProjItem::Computed { bound_expr } => {
                let mut reg = compile_bound_expr(bound_expr, &schema.columns, &mut eb)?;
                // A DATE slot is narrower than the 8-byte register it is emitted
                // from, so the value is range-checked into those low bytes
                // first. The width comes from the declaration this program is
                // paired with, never re-inferred: the two must not drift.
                if let Some(fi) = FixedInt::from_type_code(col.type_code).filter(|fi| fi.width() < 8) {
                    reg = eb.emit(LogicalInstr::IntCast { a: reg, fi });
                }
                eb.sink(Sink::Reg(reg));
            }
        }
    }
    Ok(eb.build(None)?)
}

/// Pin the full source PK to output slots `0..k` in PK-list order,
/// matching the engine's `project_schema` (which copies every PK
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
    for (target, &pk) in source_schema.pk_cols.iter().enumerate() {
        let pk = pk as usize;
        // First occurrence is the canonical physical-PK slot; any later
        // duplicate (SELECT pk, pk AS x) stays in the payload region and is
        // materialized by the expr-map column-copy path.
        let cur = items.iter().position(|i| i.passthrough_src() == Some(pk));
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
                // named it, so it is hidden — `SELECT b FROM t` does not leak the
                // PK `a`. (A source PK that is itself already hidden, e.g. a
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
/// (the rows sink): the full source PK is **always hidden-prepended** to slots
/// `0..k`, and every SELECT item — a projected PK column included — is a payload
/// slot in SELECT order (materialized by a column sink / a computed register
/// sink). Unlike
/// the CREATE VIEW linear path's [`place_pk_front`] (which *promotes* a projected
/// PK to the visible front), this preserves the user's column order (`SELECT a,
/// id` stays `[a, id]`) and never rejects a PK-dropping projection — the physical
/// key rides hidden.
pub(crate) fn build_read_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
    rel_alias: &str,
) -> Result<(Vec<ProjItem>, Vec<ColumnDef>), GnitzSqlError> {
    let mut items: Vec<ProjItem> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();

    for (idx, item) in projection.iter().enumerate() {
        if let SelectItem::Wildcard(o) = item {
            // The expansion drops hidden key slots (`_join_pk`, …); a hidden
            // *source PK* is not lost, being re-prepended (still hidden) below.
            for (i, out) in expand_wildcard_item(o, &source_schema.columns, "SELECT")? {
                items.push(ProjItem::PassThrough { src_col: i });
                out_cols.push(out);
            }
        } else {
            let (it, col) = resolve_proj_col(item, idx, source_schema, rel_alias)?;
            items.push(it);
            out_cols.push(col);
        }
    }

    // Before the hidden PK prepend below, which may reuse a name the projection
    // already carries.
    reject_duplicate_projection_names(projection, out_cols.iter(), "SELECT projection")?;

    // The full source PK, hidden-prepended to slots `0..k`.
    for (target, &pk) in source_schema.pk_cols.iter().enumerate() {
        items.insert(target, ProjItem::PassThrough { src_col: pk as usize });
        out_cols.insert(target, source_schema.columns[pk as usize].clone().hidden());
    }
    Ok((items, out_cols))
}

/// The `(schema, map)` a leading-key projection over `source_schema` produces:
/// the source PK, then what the map fills. `what` names the shape if the schema
/// is invalid.
pub(crate) fn read_reply_shape(
    items: &[ProjItem],
    out_cols: Vec<ColumnDef>,
    source_schema: &Schema,
    what: &str,
) -> Result<(Schema, ComputeMap), GnitzSqlError> {
    let k = source_schema.pk_cols.len();
    let map = payload_map(&items[k..], &out_cols[k..], source_schema)?;
    let reply_schema = Schema::from_parts(out_cols, (0..k as u32).collect())
        .map_err(|e| GnitzSqlError::Unsupported(format!("{what}: {e}")))?;
    Ok((reply_schema, map))
}

#[cfg(test)]
#[path = "tests/project_schema.rs"]
mod tests;
