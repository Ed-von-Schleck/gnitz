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
use gnitz_expr::{ExprBuilder, LogicalProgram, Sink};
use gnitz_wire::{ColumnDef, ComputeMap};

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
    let pk: Vec<u32> = (0..npk as u32).collect();
    Schema::from_parts(columns, &pk).map_err(|e| GnitzSqlError::Rejected(format!("output schema: {e}")))
}

/// The payload program of a physicalized projection, whose first `k` items copy
/// `input`'s PK.
pub(crate) fn payload_program(
    items: &[ProjItem],
    out: &Schema,
    input: &Schema,
) -> Result<LogicalProgram, GnitzSqlError> {
    let k = input.pk_cols().len();
    debug_assert_eq!(out.pk_cols().len(), k, "a projection keeps its input's key");
    debug_assert!(
        (0..k).all(|i| items[i].passthrough_src() == Some(input.pk_cols()[i] as usize)),
        "a physicalized projection copies its input's PK in front"
    );
    projection_program(&items[k..], &out.columns()[k..], input)
}

/// A compiled payload program paired with the `(type_code, nullable)` declaration of `out`'s
/// payload slots, which it writes.
pub(crate) fn compute_map(program: LogicalProgram, out: &Schema) -> ComputeMap {
    ComputeMap {
        program: program.to_blob_bytes(),
        out_cols: out
            .payload_columns()
            .map(|(_, _, c)| (c.ty.tc, c.is_nullable))
            .collect(),
    }
}

/// Compile the *payload* slice of a projection (`items` must exclude the
/// leading PK slots, which the engine carries verbatim) into one expr-map
/// program: one sink per output payload slot, in slot order. A computed item's
/// sink is the one [`ExprBuilder::slot_sink`] builds for its declared column type.
fn projection_program(
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
                    let reg = compile_bound_expr(bound_expr, schema.columns(), &mut eb)?;
                    Ok(eb.slot_sink(reg, col.ty.tc))
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(eb.build(sinks)?)
}

/// The `(schema, payload program)` of a projection of `payload` over `source`:
/// `source`'s PK, hidden, then one payload column per expression, which the
/// program fills.
pub(crate) fn reply_program(
    payload: impl IntoIterator<Item = (BoundExpr, ColumnDef)>,
    source: &Schema,
) -> Result<(Schema, LogicalProgram), GnitzSqlError> {
    let (items, cols): (Vec<ProjItem>, Vec<ColumnDef>) = payload
        .into_iter()
        .map(|(e, def)| (ProjItem::from_bound(e), def))
        .unzip();
    let program = projection_program(&items, &cols, source)?;
    let columns = source.hidden_key_columns().chain(cols).collect();
    Ok((leading_schema(columns, source.pk_cols().len())?, program))
}
