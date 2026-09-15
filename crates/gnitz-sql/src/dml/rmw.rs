//! The read-modify-write commit driver shared by UPDATE, DELETE, and INSERT ...
//! ON CONFLICT — every autocommit statement that reads a table before writing it,
//! where a naive push is a silent lost-update race.
//!
//! Each such statement resolves its target rows client-side, computes new rows
//! client-side, then writes. This driver wraps that read+build so that:
//!
//! - **Autocommit** ships a one-family, one-precondition `PUSH_TXN` frame
//!   (`GnitzClient::commit_rmw`) asserting the table has not been written since
//!   the basis. On a `TxnConflict` it adopts the server's fresh basis and re-runs
//!   the build from scratch (re-reading fresh state — re-shipping the stale batch
//!   would commit the very update it lost), bounded at [`RMW_MAX_ATTEMPTS`].
//! - **Inside a transaction** it buffers the write and records the table in the
//!   transaction's read-set, so COMMIT ships that table's OCC precondition; no
//!   per-statement retry (the buffered reads are stale by definition).

use crate::error::GnitzSqlError;
use gnitz_core::{ClientError, GnitzClient, Schema, ZSetBatch};

/// Max autocommit RMW attempts before the conflict is surfaced to the caller for
/// its own (application-level) retry. Forward progress each attempt comes from
/// the adopted fresh basis + the master's table-lock serialization, not backoff.
const RMW_MAX_ATTEMPTS: usize = 4;

/// Commit an autocommit RMW with bounded OCC retry, or buffer it into the open
/// transaction. `build` re-reads the target and produces the batch to write under
/// the table's catalog `schema`; it is re-run on every retry (a conflict means the
/// read was stale). Returns the written batch's row count. `tid` / `table_name`
/// identify the single table the statement writes (DML is single-table);
/// `table_name` names the conflict if the bound exhausts.
pub(crate) fn commit_rmw_or_buffer<F>(
    client: &mut GnitzClient,
    table_name: &str,
    tid: u64,
    schema: &Schema,
    mut build: F,
) -> Result<usize, GnitzSqlError>
where
    F: FnMut(&mut GnitzClient) -> Result<ZSetBatch, GnitzSqlError>,
{
    // An empty build buffers and records nothing, so the read-set stays a subset
    // of the family tids.
    if client.txn_active() {
        let batch = build(client)?;
        let count = batch.len();
        if count > 0 {
            client.txn_push_rmw(tid, schema, batch);
        }
        return Ok(count);
    }

    // Autocommit: bounded OCC retry, adopting the server's fresh basis on each
    // conflict (without it a warm connection would recompute the identical basis
    // and deterministically re-conflict).
    let mut basis = client.last_seen_lsn();
    for _ in 0..RMW_MAX_ATTEMPTS {
        let batch = build(client)?;
        // The engine rejects an empty family batch.
        if batch.is_empty() {
            return Ok(0);
        }
        match client.commit_rmw(tid, schema, &batch, basis) {
            Ok(_lsn) => return Ok(batch.len()),
            Err(ClientError::TxnConflict { fresh_basis }) => basis = fresh_basis,
            Err(e) => return Err(GnitzSqlError::Exec(e)),
        }
    }
    // Exhausted the internal bound under sustained contention: surface a named
    // conflict so the application can retry the whole statement.
    Err(GnitzSqlError::Conflict { table: Some(table_name.to_string()) })
}
