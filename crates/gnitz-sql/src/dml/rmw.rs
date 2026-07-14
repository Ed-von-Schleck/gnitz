//! The read-modify-write commit driver shared by UPDATE, DELETE, and INSERT ...
//! ON CONFLICT — every autocommit statement that reads a table before writing it,
//! where a naive push is a silent lost-update race.
//!
//! Each such statement resolves its target rows client-side, computes new rows
//! client-side, then writes. This driver wraps that read+build so that:
//!
//! - **Autocommit** ships a one-family, one-precondition `FLAG_PUSH_TXN` frame
//!   (`GnitzClient::commit_rmw`) asserting the table has not been written since
//!   the basis. On a `TxnConflict` it adopts the server's fresh basis and re-runs
//!   the build from scratch (re-reading fresh state — re-shipping the stale batch
//!   would commit the very update it lost), bounded at [`RMW_MAX_ATTEMPTS`].
//! - **Inside a transaction** it buffers the write and records the table in the
//!   transaction's read-set, so COMMIT ships that table's OCC precondition; no
//!   per-statement retry (the buffered reads are stale by definition).

use crate::error::GnitzSqlError;
use gnitz_core::{ClientError, GnitzClient, Schema, WireConflictMode, ZSetBatch};

/// The write an RMW statement produces after reading: the schema to push against
/// (the wire reply's schema when it overrode the catalog's, else the catalog's),
/// the batch, and the conflict mode.
pub(crate) struct RmwWrite {
    pub schema: Schema,
    pub batch: ZSetBatch,
    pub mode: WireConflictMode,
}

/// One build attempt's result: the SQL-reported affected-row `count`, and the
/// `write` to commit — `None` for a zero-row mutation, which ships nothing (the
/// engine rejects an empty family batch, so it is short-circuited client-side).
pub(crate) struct RmwBuild {
    pub count: usize,
    pub write: Option<RmwWrite>,
}

/// Max autocommit RMW attempts before the conflict is surfaced to the caller for
/// its own (application-level) retry. Forward progress each attempt comes from
/// the adopted fresh basis + the master's table-lock serialization, not backoff.
const RMW_MAX_ATTEMPTS: usize = 4;

/// Commit an autocommit RMW with bounded OCC retry, or buffer it into the open
/// transaction. `build` re-reads the target and produces the write; it is re-run
/// on every retry (a conflict means the read was stale). Returns the affected-row
/// count. `tid` / `table_name` identify the single table the statement writes
/// (DML is single-table); `table_name` names the conflict if the bound exhausts.
pub(crate) fn commit_rmw_or_buffer<F>(
    client: &mut GnitzClient,
    table_name: &str,
    tid: u64,
    mut build: F,
) -> Result<usize, GnitzSqlError>
where
    F: FnMut(&mut GnitzClient) -> Result<RmwBuild, GnitzSqlError>,
{
    // Transaction: build once, buffer the write, record `tid` in the read-set. A
    // zero-row build buffers nothing and records nothing, so the read-set stays a
    // subset of the buffered-write (family) tids.
    if client.txn_active() {
        let RmwBuild { count, write } = build(client)?;
        if let Some(w) = write {
            client.txn_push_rmw(tid, &w.schema, &w.batch, w.mode);
        }
        return Ok(count);
    }

    // Autocommit: bounded OCC retry, adopting the server's fresh basis on each
    // conflict (without it a warm connection would recompute the identical basis
    // and deterministically re-conflict).
    let mut basis = client.last_seen_lsn();
    for _ in 0..RMW_MAX_ATTEMPTS {
        let RmwBuild { count, write } = build(client)?;
        let Some(w) = write else {
            return Ok(count);
        };
        match client.commit_rmw(tid, &w.schema, &w.batch, w.mode, basis) {
            Ok(_lsn) => return Ok(count),
            Err(ClientError::TxnConflict { fresh_basis }) => basis = fresh_basis,
            Err(e) => return Err(GnitzSqlError::Exec(e)),
        }
    }
    // Exhausted the internal bound under sustained contention: surface a named
    // conflict so the application can retry the whole statement.
    Err(GnitzSqlError::Conflict {
        table: Some(table_name.to_string()),
    })
}
