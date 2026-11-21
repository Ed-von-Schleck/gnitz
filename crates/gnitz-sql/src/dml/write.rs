//! Every write verb as one of two primitives: a blind push of the VALUES rows
//! (a plain INSERT), or a read-modify-write — a read of the target whose rows
//! are rewritten and pushed as an upsert (UPDATE, DELETE, INSERT … ON CONFLICT).
//! The read is a WHERE's or the VALUES keys'; the rewrite is fixed by whether
//! there is a SET list:
//!
//! |              | no SET list                    | SET list                                  |
//! |--------------|--------------------------------|-------------------------------------------|
//! | WHERE        | DELETE: retract the held keys  | UPDATE: the held rows rewritten           |
//! | VALUES' keys | DO NOTHING: the unheld rows    | DO UPDATE: the unheld rows + held rewritten |
//!
//! Planning (`plan_insert`, `plan_update`, `plan_delete`) reads only the
//! catalog, so a refused statement issues no request and consumes no SERIAL id;
//! [`execute_write`] runs what was planned.

use super::insert::{plan_insert, resolve_conflicts, serial_col, serial_keys};
use super::mutate::{plan_delete, plan_update};
use super::set::{SetCol, apply_set};
use crate::SqlResult;
use crate::catalog::Catalog;
use crate::error::GnitzSqlError;
use crate::exec::client_map::ClientMap;
use gnitz_core::{GnitzClient, RelDescriptor, ScanReply, ZSetBatch};
use gnitz_wire::{ReadBound, RelClass, WireConflictMode};
use sqlparser::ast::Statement;
use std::sync::Arc;

/// A write, planned: everything but the SERIAL keys, which only the server can
/// hand out.
pub(crate) struct WritePlan {
    pub(super) target: Arc<RelDescriptor>,
    pub(super) write: Write,
}

pub(super) enum Write {
    /// A plain INSERT: the VALUES rows at weight +1, and the RETURNING reply's
    /// map over them. Into a SERIAL table the key region is empty until
    /// [`execute_write`] stamps it.
    Insert {
        rows: ZSetBatch,
        returning: Option<Box<ClientMap>>,
    },
    /// A read-modify-write: the rows `read` holds, rewritten by the SET list or,
    /// without one, retracted (a WHERE) or left alone (ON CONFLICT DO NOTHING).
    Rmw { read: RmwRead, set: Option<Vec<SetCol>> },
}

pub(super) enum RmwRead {
    /// UPDATE or DELETE.
    Where { bound: ReadBound, predicate: Vec<u8> },
    /// INSERT … ON CONFLICT: the VALUES rows, one per key, each pushed where its
    /// key is not held.
    Values(ZSetBatch),
}

/// Plan an INSERT, UPDATE or DELETE.
pub(crate) fn plan_write(stmt: &Statement, cat: &dyn Catalog) -> Result<WritePlan, GnitzSqlError> {
    match stmt {
        Statement::Insert(insert) => plan_insert(insert, cat),
        Statement::Update(update) => plan_update(update, cat),
        Statement::Delete(del) => plan_delete(del, cat),
        _ => unreachable!("dispatch routes only INSERT, UPDATE and DELETE here"),
    }
}

/// Run one planned write, tagged with its verb.
pub(crate) async fn execute_write(client: &mut GnitzClient, plan: WritePlan) -> Result<SqlResult, GnitzSqlError> {
    let WritePlan { target, mut write } = plan;
    let schema = &target.schema;
    if serial_col(&target).is_some()
        && let Write::Insert { rows, .. } | Write::Rmw { read: RmwRead::Values(rows), .. } = &mut write
    {
        // One reservation for the whole statement.
        let n = rows.len();
        let keys = serial_keys(client.reserve_serial_ids(&target, n as u64).await?, n, schema)?;
        rows.rewrite(|w| w.pk_mut().copy_from_slice(&keys));
    }
    let (tag, count) = match write {
        Write::Insert { rows, returning } => {
            // The engine refuses `Error` on a stream (its PK is not unique) and
            // leaves `Update` unread: the push appends, so the same row twice is
            // one element at weight 2.
            let mode = if target.class == RelClass::Stream {
                WireConflictMode::Update
            } else {
                WireConflictMode::Error
            };
            // Split on RETURNING: the batch is given away when nothing will
            // project it, saving a deep clone inside a transaction.
            let Some(mut map) = returning else {
                let count = rows.len();
                client.push(&*target, rows, mode).await?;
                return Ok(SqlResult::RowsAffected { tag: "INSERT", count });
            };
            // Unshared again once the push is sent; a transaction holding it costs a copy.
            let rows = Arc::new(rows);
            client.push(&*target, Arc::clone(&rows), mode).await?;
            let batch = map.apply(Arc::unwrap_or_clone(rows));
            return Ok(SqlResult::Rows(ScanReply { batch, lsn: None }));
        }
        Write::Rmw { read, mut set } => {
            let (tag, bound, predicate, values) = match read {
                RmwRead::Where { bound, predicate } => {
                    (if set.is_some() { "UPDATE" } else { "DELETE" }, bound, predicate, None)
                }
                RmwRead::Values(rows) => ("INSERT", ReadBound::PkSet(rows.pk_keys()), Vec::new(), Some(rows)),
            };
            let count = client
                .read_modify_write(&target, bound, predicate, set.is_none(), |held| {
                    match (values, set.as_deref_mut()) {
                        (Some(rows), set) => resolve_conflicts(rows, held, set),
                        (None, Some(set)) => apply_set(set, held, None),
                        (None, None) => Ok(ZSetBatch::key_rows(schema, held.view().pk(), -1)),
                    }
                })
                .await?;
            (tag, count)
        }
    };
    Ok(SqlResult::RowsAffected { tag, count })
}
