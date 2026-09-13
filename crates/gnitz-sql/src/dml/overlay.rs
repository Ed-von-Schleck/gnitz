//! The transaction read-your-own-writes overlay.
//!
//! An open transaction's writes are buffered, not sent, so a DML statement must
//! resolve its rows against the **effective** state: the transaction's own
//! buffered ops layered over the committed store. `TxnBuffer` indexes each
//! buffered row by PK as it arrives (`last_op`/`last_ops`), so the fold here is
//! a read of that index — the last op per PK, `Present`/`Deleted` by weight sign
//! (mirroring the engine's own per-table fold).
//!
//! [`effective_rows`] returns **owned** rows so the caller can then borrow the
//! client mutably (gather, push) with no buffer borrow outstanding. The map-based
//! [`buffered_net`] instead borrows each buffered row **in place** (no copy);
//! [`present_rows`] materializes them exactly once, all before the caller's next
//! `&mut client`.
//!
//! - [`effective_rows`] — a PK column: buffered ops, then one gather for the rest
//!   (INSERT's ON CONFLICT paths).
//! - [`buffered_net`] — the net map, over a known key set or over every PK the
//!   transaction touched (UPDATE/DELETE's key-pinned and unpinned bounds).
//! - [`present_rows`] — that map's live rows, as a batch.
//!
//! The map is **empty in autocommit**, where the overlay costs nothing:
//! `HashMap::new()` does not allocate, and its callers short-circuit on an empty
//! map before touching a row.

use std::sync::Arc;

use crate::dml::plan::{fetch_bound, AccessPlan};
use crate::error::GnitzSqlError;
use crate::exec::batch::RowGather;
use gnitz_core::{GnitzClient, PkBuf, PkColumn, Schema, ZSetBatch};
use gnitz_wire::{PkKeys, ReadBound, ReadSink};
use std::collections::{HashMap, HashSet};

/// A PK's net effect within the transaction so far. `Present` borrows the
/// buffered row in place (its batch + row index); `Deleted` is a tombstone.
pub(super) enum Buffered<'a> {
    Present(&'a ZSetBatch, usize),
    Deleted,
}

/// PK → net effect, borrowing the buffer's rows. Empty in autocommit.
pub(super) type Net<'a> = HashMap<PkBuf, Buffered<'a>>;

/// One incoming row's verdict against the effective state.
pub(crate) enum Conflict {
    /// An earlier row of the same batch already claimed this PK.
    Repeat,
    /// Nothing holds this PK: absent from the store, or buffered as deleted.
    Fresh,
    /// Row `usize` of the returned batch holds it.
    Existing(usize),
}

/// One [`Conflict`] per row of `pks`, plus the rows that exist as one owned batch.
/// The transaction's last buffered op on a key wins; every key it leaves undecided
/// goes into a single `PkSet` gather, one round trip for the whole column.
pub(crate) fn effective_rows(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    pks: &PkColumn,
) -> Result<(ZSetBatch, Vec<Conflict>), GnitzSqlError> {
    let gather = RowGather::new(schema);
    let mut out = ZSetBatch::with_capacity(schema, pks.len());
    let mut verdicts: Vec<Conflict> = Vec::with_capacity(pks.len());
    // Flat OPK bytes, one key per `pk_stride`.
    let mut undecided: Vec<u8> = Vec::new();
    // The buffer borrow is confined to this block, so it ends before the fetch.
    {
        let buf = client.txn_reads(tid);
        let mut seen: HashSet<&[u8]> = HashSet::with_capacity(pks.len());
        for i in 0..pks.len() {
            let key = pks.get_bytes(i);
            if !seen.insert(key) {
                verdicts.push(Conflict::Repeat);
                continue;
            }
            match buf.as_ref().and_then(|b| b.last_op(key)).map(|(b, r)| op_of(b, r)) {
                Some(Buffered::Present(batch, row)) => {
                    verdicts.push(Conflict::Existing(out.len()));
                    gather.copy(batch, row, &mut out);
                }
                // A buffered delete is absent whatever the store holds.
                Some(Buffered::Deleted) => verdicts.push(Conflict::Fresh),
                None => {
                    undecided.extend_from_slice(key);
                    verdicts.push(Conflict::Fresh);
                }
            }
        }
    }
    if !undecided.is_empty() {
        let committed = fetch_committed(client, tid, schema, &undecided)?;
        let base = out.len();
        for i in 0..committed.len() {
            gather.copy(&committed, i, &mut out);
        }
        let found: HashMap<&[u8], usize> = (0..committed.len())
            .map(|i| (committed.pks.get_bytes(i), base + i))
            .collect();
        // Only an undecided key was fetched, so a buffered delete stays `Fresh`
        // and a `Repeat` keeps its verdict whatever the store answered.
        for (i, verdict) in verdicts.iter_mut().enumerate() {
            if let (Conflict::Fresh, Some(&row)) = (&*verdict, found.get(pks.get_bytes(i))) {
                *verdict = Conflict::Existing(row);
            }
        }
    }
    Ok((out, verdicts))
}

/// The committed rows for `keys`, flat OPK bytes at `schema`'s stride, read back
/// under the caller's `schema` rather than a server-echoed one.
fn fetch_committed(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Arc<Schema>,
    keys: &[u8],
) -> Result<ZSetBatch, GnitzSqlError> {
    let stride = schema.pk_stride();
    let keys = PkKeys::from_keys(stride, keys.chunks_exact(stride));
    // No WHERE behind it, so the gather is the whole selection.
    let plan = AccessPlan::new(ReadBound::PkSet(keys), &[], Vec::new(), schema)?;
    fetch_bound(client, tid, &plan.access, &ReadSink::all_rows(), schema)
}

/// The transaction's net effect on `tid`: restricted to `keys` when the access
/// bound names an exact key set — its residual does not constrain the PK, so an
/// unrelated buffered row must not enter the candidates — else every PK the
/// transaction touched.
///
/// **Empty in autocommit**, and in any transaction that has not written `tid`.
pub(crate) fn buffered_net<'a>(client: &'a mut GnitzClient, tid: u64, keys: Option<&[PkBuf]>) -> Net<'a> {
    let Some(buf) = client.txn_reads(tid) else {
        return Net::new();
    };
    match keys {
        Some(keys) => keys
            .iter()
            .filter_map(|pk| buf.last_op(pk.pk_bytes()).map(|(b, r)| (*pk, op_of(b, r))))
            .collect(),
        None => buf.last_ops().map(|(pk, b, r)| (pk, op_of(b, r))).collect(),
    }
}

/// The transaction's own live rows: every `Present` op in `net`, materialized
/// under `schema` as one batch. These are the rows no server-side walk ever saw,
/// so they are the only ones a DML verb re-filters client-side.
///
/// Copies under `schema` — a buffered batch is layout-identical to it, because
/// DDL is barred inside a transaction, so both carry the same full physical
/// schema (a DROP COLUMN'd base table has a hidden slot, but it is zero-filled
/// NOT NULL on both sides — identical bytes).
pub(crate) fn present_rows(net: &Net, schema: &Schema) -> ZSetBatch {
    let mut out = ZSetBatch::with_capacity(schema, net.len());
    let gather = RowGather::new(schema);
    for op in net.values() {
        if let Buffered::Present(batch, row) = op {
            gather.copy(batch, *row, &mut out);
        }
    }
    out
}

/// One buffered row's net effect, borrowing it in place: the weight sign decides.
fn op_of(batch: &ZSetBatch, row: usize) -> Buffered<'_> {
    if batch.weights[row] < 0 {
        Buffered::Deleted
    } else {
        Buffered::Present(batch, row)
    }
}

#[cfg(test)]
#[path = "tests/overlay.rs"]
mod tests;
