//! The read-modify-write driver shared by UPDATE, DELETE, and INSERT ... ON
//! CONFLICT — every statement that reads a table before writing it, where a naive
//! push is a silent lost-update race.
//!
//! Each such statement reads its target rows ([`TargetRead`]), builds the rows to
//! write client-side, and writes them conditioned on the read's watermark
//! ([`commit_rmw`]):
//!
//! - **Autocommit** ships the write at once. A conflict means a write landed after
//!   the read, so the driver re-reads and rebuilds — re-shipping the stale batch
//!   would commit the very update it lost — bounded at [`RMW_MAX_ATTEMPTS`], which
//!   surfaces sustained contention to the caller.
//! - **Inside a transaction** a non-empty write is buffered, and COMMIT checks its
//!   condition; an empty one buffers nothing. The read sees the transaction's own
//!   buffered writes over the committed rows.

use std::sync::Arc;

use crate::codec::project_schema::key_reply;
use crate::error::GnitzSqlError;
use gnitz_core::{
    ClientError, GnitzClient, RelDescriptor, ScanReply, Schema, TxnReads, WireFault, WireStatus, ZSetBatch,
};
use gnitz_expr::RowFilter;
use gnitz_wire::{ReadBound, ReadSink, ReadSpec};

/// Max autocommit RMW attempts before the conflict is surfaced to the caller for
/// its own (application-level) retry. Each attempt re-reads, so each makes
/// progress from a fresh read; there is no backoff.
const RMW_MAX_ATTEMPTS: usize = 4;

/// A read of the target's rows a bound and predicate match, in the transaction's
/// effective state: committed rows the transaction has not written, plus its own
/// live rows that match. `keys` narrows the reply to the PK.
pub(super) struct TargetRead {
    tid: u64,
    schema: Arc<Schema>,
    spec: ReadSpec,
    reply: Arc<Schema>,
    keys: bool,
}

impl TargetRead {
    pub(super) fn new(
        target: &RelDescriptor,
        bound: ReadBound,
        predicate: Vec<u8>,
        keys: bool,
    ) -> Result<Self, GnitzSqlError> {
        let (reply, sink) = if keys {
            let (reply, map) = key_reply(&target.schema)?;
            (Arc::new(reply), ReadSink { map: Some(map), ..ReadSink::all_rows() })
        } else {
            (Arc::clone(&target.schema), ReadSink::all_rows())
        };
        Ok(TargetRead {
            tid: target.tid,
            schema: Arc::clone(&target.schema),
            spec: ReadSpec { bound, predicate, sink },
            reply,
            keys,
        })
    }

    /// The matched rows under the reply schema, and the read's watermark.
    fn resolve(&self, client: &mut GnitzClient) -> Result<(ZSetBatch, u64), GnitzSqlError> {
        // A DML target is a base table, which is never mirrored.
        let ScanReply { batch, lsn, .. } = client.scan_spec(self.tid, &self.spec, &self.reply)?;
        let lsn = lsn.expect("a server read carries its watermark");
        let rows = match client.txn_reads(self.tid, &self.schema)? {
            None => batch,
            Some(txn) => self.merge(batch, &txn)?,
        };
        Ok((rows, lsn))
    }

    /// `committed` less every PK the transaction wrote, plus the transaction's live
    /// rows (last op per PK, weight > 0) that the bound and predicate keep. A
    /// `PkSet` bound restricts the buffered side to its keys, as the server does
    /// the committed side.
    fn merge(&self, committed: ZSetBatch, txn: &TxnReads<'_>) -> Result<ZSetBatch, GnitzSqlError> {
        let keep: Vec<(usize, i64)> = (0..committed.len())
            .filter(|&i| txn.last_op(committed.pks.get_bytes(i)).is_none())
            .map(|i| (i, committed.weights[i]))
            .collect();
        // `gather` compacts the string arena the dropped rows carried.
        let mut out = committed.gather(&keep);

        let mut live = ZSetBatch::new(&self.schema);
        let take = |(batch, row): (&ZSetBatch, usize)| {
            if batch.weights[row] > 0 {
                live.copy_row_at(batch, row, batch.weights[row]);
            }
        };
        match &self.spec.bound {
            ReadBound::PkSet(keys) => keys.iter().filter_map(|k| txn.last_op(k)).for_each(take),
            _ => txn.last_ops().for_each(take),
        }
        if live.is_empty() {
            return Ok(out);
        }

        let mut ranges = Vec::new();
        RowFilter::for_read(&self.spec.predicate, &self.spec.bound, &*self.schema)?.ranges(&live, &mut ranges);
        for r in ranges.into_iter().flat_map(|(s, e)| s..e) {
            if self.keys {
                // The key reply has no payload: a key, its weight and a null word.
                out.pks.push_from(&live.pks, r);
                out.weights.push(live.weights[r]);
                out.nulls.push(0);
            } else {
                out.copy_row_at(&live, r, live.weights[r]);
            }
        }
        Ok(out)
    }
}

/// Commit an RMW statement with bounded OCC retry, or buffer it into the open
/// transaction. Each attempt re-reads `read` and hands the rows to `build`; the
/// write is conditioned on that read's watermark. Returns the written row count.
pub(super) fn commit_rmw(
    client: &mut GnitzClient,
    read: &TargetRead,
    mut build: impl FnMut(ZSetBatch) -> Result<ZSetBatch, GnitzSqlError>,
) -> Result<usize, GnitzSqlError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let (rows, basis) = read.resolve(client)?;
        let batch = build(rows)?;
        let count = batch.len();
        match client.push_rmw(read.tid, &read.schema, batch, basis) {
            Err(ClientError::Refused(WireFault { status: WireStatus::TxnConflict, .. }))
                if attempt < RMW_MAX_ATTEMPTS => {}
            r => return Ok(r.map(|()| count)?),
        }
    }
}

#[cfg(test)]
#[path = "tests/rmw.rs"]
mod tests;
