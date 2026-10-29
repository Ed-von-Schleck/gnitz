//! Physical projections: resolved [`BoundExpr`] slots over a source schema →
//! output layout and the compiled payload program. A bare `ColRef` slot copies
//! its source column; any other expression is compiled.
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

/// `columns` behind a leading key region of `npk`, admitted as a schema the engine
/// can hold.
pub(crate) fn leading_schema(columns: Vec<ColumnDef>, npk: usize) -> Result<Schema, GnitzSqlError> {
    let pk: Vec<u32> = (0..npk as u32).collect();
    Schema::from_parts(columns, &pk).map_err(|e| GnitzSqlError::Rejected(format!("output schema: {e}")))
}

/// The payload program of a physicalized projection, whose first `k` items copy
/// `input`'s PK.
pub(crate) fn payload_program(
    items: &[BoundExpr],
    out: &Schema,
    input: &Schema,
) -> Result<LogicalProgram, GnitzSqlError> {
    let k = input.pk_cols().len();
    debug_assert_eq!(out.pk_cols().len(), k, "a projection keeps its input's key");
    debug_assert!(
        (0..k).all(|i| items[i].as_col() == Some(input.pk_cols()[i] as usize)),
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
    items: &[BoundExpr],
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
                BoundExpr::ColRef(c) => Ok(Sink::Col(*c as u32)),
                e => {
                    let reg = compile_bound_expr(e, schema.columns(), &mut eb)?;
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
    let (items, cols): (Vec<BoundExpr>, Vec<ColumnDef>) = payload.into_iter().unzip();
    let program = projection_program(&items, &cols, source)?;
    let columns = source.hidden_key_columns().chain(cols).collect();
    Ok((leading_schema(columns, source.pk_cols().len())?, program))
}
