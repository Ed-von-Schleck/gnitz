//! WHERE → access path for every direct-read verb ([`access_path`], over the
//! candidates [`crate::access`] recognizes), and the rows reply a projection produces
//! ([`rows_reply`]).

use std::sync::Arc;

use crate::access::{candidates, residual, Candidate};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::hir::AdhocRows;
use crate::ir::BoundExpr;
use crate::project::{projection_program, ProjItem};
use crate::tail::wire_keys;
use gnitz_core::{RelDescriptor, Schema};
use gnitz_expr::LogicalProgram;
use gnitz_wire::RelIndex;
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
    /// The reply's regions under the SELECT list's numbering: the key columns no
    /// item names, hidden, then the items — a key column at the first item that
    /// copies it, every other item a payload column.
    pub(crate) schema: Arc<Schema>,
    /// The program filling the reply's payload from a source row; `None` when the
    /// reply's regions are the relation's own.
    pub(crate) program: Option<LogicalProgram>,
    /// The ORDER BY keys over `schema`.
    pub(crate) order: Vec<OrderKey>,
    /// `order` as the worker's sink numbers its input: the program's output, key
    /// region first, or the relation where there is no program.
    pub(crate) sink_order: Vec<OrderKey>,
    /// Whether `order` ascends a leading run of the relation's PK columns, so
    /// rows in store order are already in it.
    pub(crate) pk_ordered: bool,
    /// `(relation column, reply column)` for each relation column an item copies
    /// verbatim, at the first item that does.
    pub(crate) copied: Vec<(u32, u32)>,
}

/// The rows reply `rows` produces over `desc`, ordered by `keys`.
pub(crate) fn rows_reply(
    rows: AdhocRows,
    keys: &[crate::tail::OrderKey<'_>],
    desc: &Arc<RelDescriptor>,
) -> Result<RowsReply, GnitzSqlError> {
    let AdhocRows { items, cols, placed } = rows;
    let schema = &desc.schema;
    let k = schema.pk_cols.len();
    // Keyed by item.
    let item_order = wire_keys(keys, &cols, placed)?;
    let pk_ordered = leads_with_pk(&item_order, &items, &schema.pk_cols);

    // The first visible item copying a key column is that column of the key
    // region, which the reply carries once.
    let mut named: Vec<Option<usize>> = vec![None; k];
    for (i, (item, col)) in items.iter().zip(&cols).enumerate().skip(k) {
        let key = schema
            .pk_cols
            .iter()
            .position(|&pk| item.passthrough_src() == Some(pk as usize));
        if let Some(j) = key.filter(|&j| !col.is_hidden && named[j].is_none()) {
            named[j] = Some(i);
        }
    }

    // Each item's column in the reply, and as the program's output numbers it.
    let (mut reply_at, mut sink_at) = (vec![0u16; items.len()], vec![0u16; items.len()]);
    let (mut columns, mut pk_cols) = (Vec::with_capacity(items.len()), vec![0u32; k]);
    let (mut payload, mut payload_cols) = (Vec::new(), Vec::new());
    let mut sources = Vec::with_capacity(items.len());
    for (i, (item, col)) in items.into_iter().zip(cols).enumerate() {
        sources.push(item.passthrough_src());
        let key = match i < k {
            true => named[i].is_none().then_some(i),
            false => named.iter().position(|&n| n == Some(i)),
        };
        match (key, i < k) {
            // A key column an item names stands at that item.
            (None, true) => continue,
            (Some(j), _) => {
                pk_cols[j] = columns.len() as u32;
                (reply_at[j], sink_at[j]) = (columns.len() as u16, j as u16);
                sink_at[i] = j as u16;
            }
            (None, false) => {
                sink_at[i] = (k + payload.len()) as u16;
                payload.push(item);
                payload_cols.push(col.clone());
            }
        }
        reply_at[i] = columns.len() as u16;
        columns.push(col);
    }

    // The relation's payload columns, each copied in place at its own type: the
    // reply's regions are the relation's.
    let copied = payload
        .iter()
        .zip(&payload_cols)
        .map(|(item, col)| (item.passthrough_src(), &col.ty));
    let unmapped = copied.eq(schema.payload_columns().map(|(_, ci, col)| (Some(ci), &col.ty)));
    let renumbered = |at: &dyn Fn(usize) -> u16| -> Vec<OrderKey> {
        item_order
            .iter()
            .map(|key| OrderKey { col: at(key.col as usize), ..*key })
            .collect()
    };
    let (program, sink_order) = match unmapped {
        true => {
            let source = |i: usize| sources[i].expect("a key or an unmapped payload column is copied") as u16;
            (None, renumbered(&source))
        }
        false => (
            Some(projection_program(&payload, &payload_cols, schema)?),
            renumbered(&|i| sink_at[i]),
        ),
    };
    let mut copied: Vec<(u32, u32)> = Vec::new();
    for (i, src) in sources.iter().enumerate() {
        if let Some(src) = src.map(|s| s as u32).filter(|s| copied.iter().all(|(c, _)| c != s)) {
            copied.push((src, reply_at[i] as u32));
        }
    }
    let reply =
        Schema::from_parts(columns, pk_cols).map_err(|e| GnitzSqlError::Rejected(format!("output schema: {e}")))?;
    Ok(RowsReply {
        schema: Arc::new(reply),
        program,
        order: renumbered(&|i| reply_at[i]),
        sink_order,
        pk_ordered,
        copied,
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
