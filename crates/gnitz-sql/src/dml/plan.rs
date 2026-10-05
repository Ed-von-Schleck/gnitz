//! WHERE → access path for every direct-read verb ([`access_path`], over the
//! candidates [`crate::access`] recognizes), and the rows reply a projection produces
//! ([`rows_reply`]).

use std::sync::Arc;

use crate::access::{candidates, residual, Candidate};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::hir::AdhocRows;
use crate::ir::BoundExpr;
use crate::project::{leading_schema, payload_program, ProjItem};
use crate::tail::wire_keys;
use gnitz_core::{RelDescriptor, Schema};
use gnitz_expr::{LogicalProgram, SchemaFacts};
use gnitz_wire::{ColumnDef, RelIndex};
use gnitz_wire::{OrderKey, ReadBound};
use sqlparser::ast::Expr;

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
pub(super) fn bound_and_predicate(
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
            Err(e @ GnitzSqlError::Rejected(_)) => {
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
    /// Whether `order` ascends a leading run of the relation's PK columns, so
    /// rows in store order are already in it.
    pub(crate) pk_ordered: bool,
}

/// The rows reply `rows` produces over `desc`, ordered by `keys`.
pub(crate) fn rows_reply(
    rows: AdhocRows,
    keys: &[crate::tail::OrderKey<'_>],
    desc: &Arc<RelDescriptor>,
) -> Result<RowsReply, GnitzSqlError> {
    let AdhocRows { items, cols: out_cols, placed } = rows;
    let schema = &desc.schema;
    let mut order = wire_keys(keys, &out_cols, placed)?;
    let pk_ordered = leads_with_pk(&order, &items, &schema.pk_cols);
    let k = schema.pk_cols.len();
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
            pk_ordered,
        });
    }
    let out = leading_schema(out_cols, k)?;
    Ok(RowsReply {
        program: Some(payload_program(&items, &out, schema)?),
        schema: Arc::new(out),
        order,
        pk_ordered,
    })
}

/// Whether `order`, over `items`, ascends a leading run of `pk_cols`, each key a
/// verbatim copy of its column. The stored key's byte order is the typed order of
/// those columns.
fn leads_with_pk(order: &[OrderKey], items: &[ProjItem], pk_cols: &[u32]) -> bool {
    order.len() <= pk_cols.len()
        && order
            .iter()
            .zip(pk_cols)
            .all(|(key, &pk)| !key.desc && items[key.col as usize].passthrough_src() == Some(pk as usize))
}

/// Whether a worker reading under `bound` yields its rows in ascending PK order:
/// every walk but a secondary index's.
pub(super) fn walks_in_pk_order(bound: &ReadBound, pk_cols: &[u32]) -> bool {
    match bound {
        ReadBound::None | ReadBound::PkSet(_) => true,
        ReadBound::Range(r) => r.walks_pk(pk_cols),
    }
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
    !has_hidden_payload(schema) && reply.eq(relation)
}

/// True iff any **non-PK** column is hidden. A view's synthetic hidden keys are
/// PK columns, so they never count.
fn has_hidden_payload(schema: &Schema) -> bool {
    schema
        .columns
        .iter()
        .enumerate()
        .any(|(i, c)| c.is_hidden && !schema.is_pk_col(i))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
