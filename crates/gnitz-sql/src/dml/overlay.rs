//! The transaction read-your-own-writes overlay.
//!
//! An open transaction's writes are buffered, not sent, so a DML statement must
//! resolve its rows against the **effective** state: the transaction's own
//! buffered ops layered over the committed store. `TxnBuffer` indexes each
//! buffered row by PK as it arrives (`last_op`/`last_ops`), so the fold here is
//! a read of that index — the last op per PK, `Present`/`Deleted` by weight sign
//! (mirroring the engine's `fold_family`).
//!
//! [`effective_rows`] returns **owned** rows so the caller can then borrow the
//! client mutably (gather, push) with no buffer borrow outstanding. The map-based
//! [`buffered_net`] instead borrows each buffered row **in place** (no copy);
//! [`present_rows`] materializes them exactly once, all before the caller's next
//! `&mut client`.
//!
//! - [`effective_rows`] — a key set: buffered ops, then one gather for the rest
//!   (INSERT's ON CONFLICT paths).
//! - [`buffered_net`] — the net map, over a known key set or over every PK the
//!   transaction touched (UPDATE/DELETE's key-pinned and unpinned bounds).
//! - [`present_rows`] — that map's live rows, as a batch.
//!
//! The map is **empty in autocommit**, where the overlay costs nothing:
//! `HashMap::new()` does not allocate, and its callers short-circuit on an empty
//! map before touching a row.

use crate::dml::plan::{fetch_bound, AccessPlan};
use crate::error::GnitzSqlError;
use crate::exec::batch::RowGather;
use gnitz_core::{GnitzClient, PkTuple, Schema, ZSetBatch};
use gnitz_wire::{ReadBound, ReadSink};
use std::collections::{HashMap, HashSet};

/// A PK's net effect within the transaction so far. `Present` borrows the
/// buffered row in place (its batch + row index); `Deleted` is a tombstone.
pub(super) enum Buffered<'a> {
    Present(&'a ZSetBatch, usize),
    Deleted,
}

/// PK → net effect, borrowing the buffer's rows. Empty in autocommit.
pub(super) type Net<'a> = HashMap<PkTuple, Buffered<'a>>;

/// The rows `keys` currently resolve to, as one owned batch plus a `PK → row`
/// index; a key that resolves to nothing is absent from the index. Per key the
/// rule is the transaction's last buffered op wins (`Present` → that row,
/// `Deleted` → absent), else the committed store decides.
///
/// Resolved for the whole set at once, which is what keeps a multi-row
/// `ON CONFLICT` at one round trip rather than one per row: every key the buffer
/// does not decide goes into a single `PkSet` gather. Duplicate keys resolve to
/// the same row, so the caller need not pre-deduplicate.
pub(crate) fn effective_rows(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    keys: &[PkTuple],
) -> Result<(ZSetBatch, HashMap<PkTuple, usize>), GnitzSqlError> {
    let gather = RowGather::new(schema);
    let mut out = ZSetBatch::with_capacity(schema, keys.len());
    let mut index: HashMap<PkTuple, usize> = HashMap::with_capacity(keys.len());
    let mut undecided: Vec<PkTuple> = Vec::new();
    // The buffer borrow is confined to this block, so it ends before the fetch.
    {
        let net = buffered_net(client, tid, Some(keys));
        let mut seen: HashSet<PkTuple> = HashSet::with_capacity(keys.len());
        for pk in keys {
            if !seen.insert(*pk) {
                continue;
            }
            match net.get(pk) {
                Some(Buffered::Present(batch, row)) => {
                    index.insert(*pk, out.len());
                    gather.copy(batch, *row, &mut out);
                }
                // A buffered delete is absent whatever the store holds.
                Some(Buffered::Deleted) => {}
                None => undecided.push(*pk),
            }
        }
    }
    if !undecided.is_empty() {
        let mut committed = fetch_committed(client, tid, schema, &undecided)?;
        for i in 0..committed.len() {
            index.insert(committed.pks.get_tuple(i), out.len());
            gather.take(&mut committed, i, &mut out);
        }
    }
    Ok((out, index))
}

/// The committed rows for `keys`. A single-column PK has a `PkSet` wire form, so
/// the whole set is one gather (chunked by `fetch_bound` past the per-gather
/// cap). A compound PK has none, so it falls back to a seek per key — the shape
/// `ON CONFLICT` reaches only through a bare `DO NOTHING`, since a conflict
/// target is already rejected on a compound-PK table.
fn fetch_committed(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    keys: &[PkTuple],
) -> Result<ZSetBatch, GnitzSqlError> {
    if schema.pk_count() == 1 {
        // No WHERE behind it, so nothing is residual and no predicate ships: the
        // gather is the whole selection. ON CONFLICT needs the committed rows for
        // a key set it already holds, so it takes this bound directly rather than
        // re-deriving it from a synthetic `pk IN (…)`.
        let keys = keys.iter().map(|k| k.split_wire().0).collect();
        let plan = AccessPlan::new(ReadBound::PkSet(keys), None, Vec::new(), schema)?;
        return fetch_bound(client, tid, &plan.access, &ReadSink::all_rows(), schema);
    }
    let gather = RowGather::new(schema);
    let mut out = ZSetBatch::with_capacity(schema, keys.len());
    for pk in keys {
        if let Some(mut b) = client.seek(tid, pk)?.1.filter(|b| !b.pks.is_empty()) {
            for i in 0..b.len() {
                gather.take(&mut b, i, &mut out);
            }
        }
    }
    Ok(out)
}

/// The transaction's net effect on `tid`: restricted to `keys` when the access
/// bound names an exact key set — its residual does not constrain the PK, so an
/// unrelated buffered row must not enter the candidates — else every PK the
/// transaction touched.
///
/// **Empty in autocommit**, and in any transaction that has not written `tid`.
pub(crate) fn buffered_net<'a>(client: &'a GnitzClient, tid: u64, keys: Option<&[PkTuple]>) -> Net<'a> {
    let Some(buf) = client.txn_buffer() else {
        return Net::new();
    };
    match keys {
        Some(keys) => keys
            .iter()
            .filter_map(|pk| buf.last_op(tid, pk).map(|(b, r)| (*pk, op_of(b, r))))
            .collect(),
        None => buf.last_ops(tid).map(|(pk, b, r)| (pk, op_of(b, r))).collect(),
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
