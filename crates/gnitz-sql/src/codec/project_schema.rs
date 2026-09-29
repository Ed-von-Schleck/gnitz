//! Physical projections: [`ProjItem`]s over a source schema → output layout and
//! the compiled payload program.
//!
//! [`payload_program`] compiles a leading-key projection's payload;
//! [`reply_program`] builds the reply schema and program of one over a source's
//! hidden key; [`leading_schema`] admits a leading-key schema.

use crate::error::GnitzSqlError;
use crate::expr_lower::compile_bound_expr;
use crate::ir::BoundExpr;
use gnitz_core::Schema;
use gnitz_expr::{ExprBuilder, LogicalInstr, LogicalProgram, Sink};
use gnitz_wire::ComputeMap;
use gnitz_wire::{ColumnDef, FixedInt};

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

/// `columns` behind a leading key region of `npk`, admitted as a schema the engine
/// can hold.
pub(crate) fn leading_schema(columns: Vec<ColumnDef>, npk: usize) -> Result<Schema, GnitzSqlError> {
    Schema::from_parts(columns, (0..npk as u32).collect())
        .map_err(|e| GnitzSqlError::Rejected(format!("output schema: {e}")))
}

/// The payload program of a physicalized projection, whose first `k` items copy
/// `input`'s PK.
pub(crate) fn payload_program(
    items: &[ProjItem],
    out: &Schema,
    input: &Schema,
) -> Result<LogicalProgram, GnitzSqlError> {
    let k = input.pk_cols.len();
    debug_assert_eq!(out.pk_cols.len(), k, "a projection keeps its input's key");
    debug_assert!(
        (0..k).all(|i| items[i].passthrough_src() == Some(input.pk_cols[i] as usize)),
        "a physicalized projection copies its input's PK in front"
    );
    compile_projection_map(&items[k..], &out.columns[k..], input)
}

/// A compiled payload program paired with the `(type_code, nullable)` declaration of `out`'s
/// payload slots, which it writes.
pub(crate) fn compute_map(program: LogicalProgram, out: &Schema) -> ComputeMap {
    ComputeMap {
        program: program.to_blob_bytes(),
        out_cols: out.columns[out.pk_cols.len()..]
            .iter()
            .map(|c| (c.ty.tc, c.is_nullable))
            .collect(),
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
    let sinks = items
        .iter()
        .zip(out_cols)
        .map(|(item, col)| -> Result<Sink, GnitzSqlError> {
            match item {
                ProjItem::PassThrough { src_col } => Ok(Sink::Col(*src_col as u32)),
                ProjItem::Computed { bound_expr } => {
                    let mut reg = compile_bound_expr(bound_expr, &schema.columns, &mut eb)?;
                    // A slot narrower than the register takes a value range-checked
                    // into the slot's declared type.
                    if let Some(fi) = FixedInt::from_type_code(col.ty.tc).filter(|fi| fi.width() < 8) {
                        reg = eb.emit(LogicalInstr::IntCast { a: reg, fi });
                    }
                    Ok(Sink::Reg(reg))
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(eb.build(sinks)?)
}

/// The `(schema, payload program)` a projection over `source` produces: `source`'s PK,
/// hidden, then the payload columns the program fills.
pub(crate) fn reply_program(
    payload_items: &[ProjItem],
    payload_cols: Vec<ColumnDef>,
    source: &Schema,
) -> Result<(Schema, LogicalProgram), GnitzSqlError> {
    let program = compile_projection_map(payload_items, &payload_cols, source)?;
    let columns = source.hidden_key_columns().chain(payload_cols).collect();
    Ok((leading_schema(columns, source.pk_cols.len())?, program))
}
