//! Physical projections: [`ProjItem`]s over a source schema → output layout and
//! the compiled payload program.
//!
//! `place_pk_front` pins the source PK to the leading output slots of a CREATE VIEW
//! linear projection; [`payload_map`] compiles a payload slice into a
//! [`ComputeMap`]; [`reply_program`] / [`read_reply_shape`] build the reply schema
//! and program of a leading-key projection.

use crate::error::GnitzSqlError;
use crate::expr_lower::compile_bound_expr;
use crate::ir::BoundExpr;
use gnitz_core::{ColumnDef, FixedInt, Schema};
use gnitz_expr::{ExprBuilder, LogicalInstr, LogicalProgram, Sink};
use gnitz_wire::ComputeMap;

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

    /// The source column a pass-through copies, `None` for a computed item.
    pub(crate) fn passthrough_src(&self) -> Option<usize> {
        match self {
            ProjItem::PassThrough { src_col } => Some(*src_col),
            ProjItem::Computed { .. } => None,
        }
    }
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
    Ok(compute_map(compile_projection_map(items, cols, schema)?, cols))
}

/// A compiled payload program paired with the `(type_code, nullable)` declaration of the slots
/// it writes — the two halves must describe the same slice.
fn compute_map(program: LogicalProgram, cols: &[ColumnDef]) -> ComputeMap {
    ComputeMap {
        program: program.to_blob_bytes(),
        out_cols: cols.iter().map(|c| (c.type_code as u8, c.is_nullable)).collect(),
    }
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

/// The `(schema, payload program)` a leading-key projection over `source_schema` produces: the
/// source PK, then what the program fills. `what` names the shape if the schema is invalid.
pub(crate) fn reply_program(
    items: &[ProjItem],
    out_cols: Vec<ColumnDef>,
    source_schema: &Schema,
    what: &str,
) -> Result<(Schema, LogicalProgram), GnitzSqlError> {
    let k = source_schema.pk_cols.len();
    let program = compile_projection_map(&items[k..], &out_cols[k..], source_schema)?;
    let schema = Schema::from_parts(out_cols, (0..k as u32).collect())
        .map_err(|e| GnitzSqlError::Unsupported(format!("{what}: {e}")))?;
    Ok((schema, program))
}

/// [`reply_program`], encoded for the wire.
pub(crate) fn read_reply_shape(
    items: &[ProjItem],
    out_cols: Vec<ColumnDef>,
    source_schema: &Schema,
    what: &str,
) -> Result<(Schema, ComputeMap), GnitzSqlError> {
    let (schema, program) = reply_program(items, out_cols, source_schema, what)?;
    let map = compute_map(program, &schema.columns[source_schema.pk_cols.len()..]);
    Ok((schema, map))
}

#[cfg(test)]
#[path = "tests/project_schema.rs"]
mod tests;
