//! WHERE → access path for every direct-read verb ([`bound_and_predicate`], over the
//! candidates [`crate::access`] recognizes), and the rows reply a projection produces
//! ([`rows_reply`]).

use std::sync::Arc;

use crate::access::{candidates, residual, Candidate};
use crate::codec::project_schema::{reply_program, ProjItem};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::ir::BoundExpr;
use crate::tail::{order_exprs, parse_order_by, wire_keys};
use gnitz_core::{ColumnDef, IndexMeta, Schema};
use gnitz_expr::LogicalProgram;
use gnitz_wire::{OrderKey, ReadBound};
use sqlparser::ast::{OrderBy, SelectItem};

// ---------------------------------------------------------------------------
// The access-path ladder
// ---------------------------------------------------------------------------

/// A WHERE as a pushed-down [`ReadBound`] and the compiled server-side predicate of
/// the conjuncts it does not apply.
pub(crate) struct AccessPlan {
    bound: ReadBound,
    predicate: Vec<u8>,
}

impl AccessPlan {
    /// The bound and the compiled predicate.
    pub(crate) fn into_parts(self) -> (ReadBound, Vec<u8>) {
        (self.bound, self.predicate)
    }
}

/// Bind a single-table `WHERE` (or its absence) for [`bound_and_predicate`], as
/// its conjunct list. `alias` is the relation's effective alias, which a written
/// qualifier must name.
pub(crate) fn bind_where(
    schema: &Schema,
    alias: &str,
    where_expr: Option<&sqlparser::ast::Expr>,
) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    match where_expr {
        Some(we) => crate::bind::bind_conjuncts(we, &crate::bind::SingleTable { schema, alias }),
        None => Ok(Vec::new()),
    }
}

/// Bind-once WHERE → the access plan that serves it: the first of [`candidates`]
/// whose residual compiles, else an unbounded scan under the whole WHERE.
/// `indexes` is the relation's declared secondary-index list.
pub(crate) fn bound_and_predicate(
    schema: &Schema,
    conjuncts: &[BoundExpr],
    indexes: &[IndexMeta],
) -> Result<AccessPlan, GnitzSqlError> {
    let mut blocked = None;
    for Candidate { bound, consumed } in candidates(conjuncts, schema, indexes) {
        let residual = residual(conjuncts, &consumed);
        match compile_wire_conjuncts(residual.iter().copied(), &schema.columns) {
            Ok(predicate) => return Ok(AccessPlan { bound, predicate }),
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
    Ok(AccessPlan {
        bound: ReadBound::None,
        predicate: compile_wire_conjuncts(conjuncts, &schema.columns)?,
    })
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

/// The rows reply `projection` produces over `schema`, ordered by `order_by`.
pub(crate) fn rows_reply(
    projection: &[SelectItem],
    order_by: Option<&OrderBy>,
    schema: &Arc<Schema>,
    alias: &str,
) -> Result<RowsReply, GnitzSqlError> {
    let keys = parse_order_by(order_by)?;
    let crate::hir::AdhocRows { items, cols: out_cols, placed } =
        crate::hir::bind_adhoc_rows(projection, schema, alias, &order_exprs(&keys))?;
    let mut order = wire_keys(&keys, &out_cols, placed)?;
    if reproduces(schema, &items, &out_cols) {
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
    let (reply, program) = reply_program(&items, out_cols, schema, "read-spec reply schema is invalid")?;
    Ok(RowsReply {
        schema: Arc::new(reply),
        program: Some(program),
        order,
    })
}

/// Whether the reply past its hidden PK prefix is the relation's visible columns, each
/// copied in place, visible, under its own name.
fn reproduces(schema: &Schema, items: &[ProjItem], out_cols: &[ColumnDef]) -> bool {
    let k = schema.pk_cols.len();
    let reply = items[k..]
        .iter()
        .zip(&out_cols[k..])
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
