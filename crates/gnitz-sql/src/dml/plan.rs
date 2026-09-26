//! WHERE → access path for every direct-read verb ([`access_path`], over the
//! candidates [`crate::access`] recognizes), and the rows reply a projection produces
//! ([`rows_reply`]).

use std::sync::Arc;

use crate::access::{candidates, residual, Candidate};
use crate::codec::project_schema::{reply_program, ProjItem};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::ir::BoundExpr;
use crate::tail::{order_exprs, parse_order_by, wire_keys};
use gnitz_core::{ColumnDef, RelDescriptor, RelIndex, Schema};
use gnitz_expr::LogicalProgram;
use gnitz_wire::{OrderKey, ReadBound};
use sqlparser::ast::{Expr, OrderBy, SelectItem};

// ---------------------------------------------------------------------------
// The access-path ladder
// ---------------------------------------------------------------------------

/// A single-table WHERE (or its absence) as a pushed-down bound and the compiled
/// predicate of the conjuncts it does not apply. `alias` is what a qualifier must name.
pub(super) fn access_path(
    schema: &Schema,
    alias: &str,
    selection: Option<&Expr>,
    indexes: &[RelIndex],
) -> Result<(ReadBound, Vec<u8>), GnitzSqlError> {
    let conjuncts = match selection {
        Some(we) => crate::bind::bind_conjuncts(we, &crate::bind::SingleTable { schema, alias })?,
        None => Vec::new(),
    };
    bound_and_predicate(schema, &conjuncts, indexes)
}

/// Bind-once WHERE → the access path that serves it: the first of [`candidates`]
/// whose residual compiles, else an unbounded scan under the whole WHERE.
/// `indexes` is the relation's declared secondary-index list.
fn bound_and_predicate(
    schema: &Schema,
    conjuncts: &[BoundExpr],
    indexes: &[RelIndex],
) -> Result<(ReadBound, Vec<u8>), GnitzSqlError> {
    let mut blocked = None;
    for Candidate { bound, consumed } in candidates(conjuncts, schema, indexes) {
        let residual = residual(conjuncts, &consumed);
        match compile_wire_conjuncts(residual.iter().copied(), &schema.columns) {
            Ok(predicate) => return Ok((bound, predicate)),
            // A conjunct the VM cannot carry; a later candidate may consume it.
            Err(e @ GnitzSqlError::Unsupported(_)) => {
                blocked.get_or_insert(e);
            }
            Err(e) => return Err(e),
        }
    }
    // A candidate's residual is part of the whole WHERE, which then fails too.
    if let Some(e) = blocked {
        return Err(e);
    }
    Ok((ReadBound::None, compile_wire_conjuncts(conjuncts, &schema.columns)?))
}

// ---------------------------------------------------------------------------
// The rows reply
// ---------------------------------------------------------------------------

/// The rows reply a projection over a relation produces.
pub(crate) struct RowsReply {
    pub(crate) schema: Arc<Schema>,
    /// The program filling the reply from a source row; `None` when the reply is the
    /// relation's own layout.
    pub(crate) program: Option<LogicalProgram>,
    /// The ORDER BY keys over the reply.
    pub(crate) order: Vec<OrderKey>,
}

/// The rows reply `projection` produces over `desc`, ordered by `order_by`.
pub(crate) fn rows_reply(
    projection: &[SelectItem],
    order_by: Option<&OrderBy>,
    desc: &Arc<RelDescriptor>,
    alias: &str,
) -> Result<RowsReply, GnitzSqlError> {
    let keys = parse_order_by(order_by)?;
    let crate::hir::AdhocRows { items, cols: mut out_cols, placed } =
        crate::hir::bind_adhoc_rows(projection, desc, alias, &order_exprs(&keys))?;
    let schema = &desc.schema;
    let mut order = wire_keys(&keys, &out_cols, placed)?;
    // Past the PK prefix `bind_adhoc_rows` places for ORDER BY; the reply carries its own.
    let k = schema.pk_count();
    if reproduces(schema, &items[k..], &out_cols[k..]) {
        for key in &mut order {
            key.col = items[key.col as usize]
                .passthrough_src()
                .expect("a reproducing projection only copies") as u16;
        }
        return Ok(RowsReply {
            schema: Arc::clone(schema),
            program: None,
            order,
        });
    }
    let (reply, program) = reply_program(
        &items[k..],
        out_cols.split_off(k),
        schema,
        "read-spec reply schema is invalid",
    )?;
    Ok(RowsReply {
        schema: Arc::new(reply),
        program: Some(program),
        order,
    })
}

/// Whether a reply payload is the relation's visible columns, each copied in place,
/// visible, under its own name.
fn reproduces(schema: &Schema, items: &[ProjItem], out_cols: &[ColumnDef]) -> bool {
    let reply = items
        .iter()
        .zip(out_cols)
        .map(|(item, col)| (item.passthrough_src(), col.is_hidden, col.name.as_str()));
    let relation = schema
        .visible_columns()
        .map(|(i, col)| (Some(i), false, col.name.as_str()));
    !schema.has_hidden_payload() && reply.eq(relation)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
