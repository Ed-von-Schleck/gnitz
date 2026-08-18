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
use gnitz_wire::ReadSink;
use std::collections::{HashMap, HashSet};

/// A PK's net effect within the transaction so far. `Present` borrows the
/// buffered row in place (its batch + row index); `Deleted` is a tombstone.
pub(crate) enum Buffered<'a> {
    Present(&'a ZSetBatch, usize),
    Deleted,
}

/// PK → net effect, borrowing the buffer's rows. Empty in autocommit.
pub(crate) type Net<'a> = HashMap<PkTuple, Buffered<'a>>;

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
        let plan = AccessPlan::for_pk_set(keys.iter().map(|k| k.split_wire().0).collect());
        return fetch_bound(client, tid, &plan, &ReadSink::all_rows(), schema);
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
mod tests {
    use super::*;
    use crate::test_support::two_col;
    use gnitz_core::{ColData, PkColumn, TxnBuffer, TypeCode, WireConflictMode};

    /// (pk U64 PK, val I64) rows as (pk, val, weight).
    fn batch(schema: &Schema, rows: &[(u128, i64, i64)]) -> ZSetBatch {
        let mut b = ZSetBatch::new(schema);
        for &(pk, val, w) in rows {
            b.pks.push_u128(pk);
            b.weights.push(w);
            b.nulls.push(0);
            if let ColData::Fixed(buf) = &mut b.columns[1] {
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }
        b
    }

    /// The full net map, straight off a buffer (the client-free half of
    /// `buffered_all`).
    fn net_of(buf: &TxnBuffer, tid: u64) -> Net<'_> {
        buf.last_ops(tid).map(|(pk, b, row)| (pk, op_of(b, row))).collect()
    }

    fn val_of(net: &Net, pk: u128) -> Option<i64> {
        match net.get(&PkTuple::from_u128(8, pk)) {
            Some(Buffered::Present(batch, row)) => match &batch.columns[1] {
                ColData::Fixed(buf) => {
                    let o = row * 8;
                    Some(i64::from_le_bytes(buf[o..o + 8].try_into().unwrap()))
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn is_deleted(net: &Net, pk: u128) -> bool {
        matches!(net.get(&PkTuple::from_u128(8, pk)), Some(Buffered::Deleted))
    }

    fn rows_of(b: &ZSetBatch) -> Vec<(u128, i64)> {
        let mut out: Vec<(u128, i64)> = (0..b.len())
            .map(|i| {
                let pk = b.pks.get(i);
                let val = b.columns[1]
                    .cell(i, 8)
                    .map_or(0, |c| i64::from_le_bytes(c.try_into().unwrap()));
                (pk, val)
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn net_folds_last_op_per_pk() {
        let schema = two_col(TypeCode::I64);
        let tid = 7;
        let mut buf = TxnBuffer::default();
        // Insert pk=1 (v=10), pk=2 (v=20) [Error family].
        buf.push_with_mode(
            tid,
            &schema,
            &batch(&schema, &[(1, 10, 1), (2, 20, 1)]),
            WireConflictMode::Error,
        );
        // Update pk=1 → v=11 [Update family].
        buf.push(tid, &schema, &batch(&schema, &[(1, 11, 1)]));
        // Delete pk=2 [coalesces with the Update family above].
        buf.delete(tid, &schema, PkColumn::from_u128s(8, [2]));

        let net = net_of(&buf, tid);
        assert_eq!(net.len(), 2);
        assert_eq!(val_of(&net, 1), Some(11), "pk=1 last op is the update");
        assert!(is_deleted(&net, 2), "pk=2 deleted");
    }

    #[test]
    fn net_delete_then_reinsert_is_present() {
        let schema = two_col(TypeCode::I64);
        let tid = 7;
        let mut buf = TxnBuffer::default();
        buf.push_with_mode(tid, &schema, &batch(&schema, &[(5, 50, 1)]), WireConflictMode::Error);
        buf.delete(tid, &schema, PkColumn::from_u128s(8, [5]));
        buf.push_with_mode(tid, &schema, &batch(&schema, &[(5, 99, 1)]), WireConflictMode::Error);
        let net = net_of(&buf, tid);
        assert_eq!(
            val_of(&net, 5),
            Some(99),
            "delete-then-reinsert yields the reinserted payload"
        );
    }

    #[test]
    fn net_scopes_by_tid_and_skips_zero_weight() {
        let schema = two_col(TypeCode::I64);
        let mut buf = TxnBuffer::default();
        buf.push_with_mode(1, &schema, &batch(&schema, &[(1, 10, 1)]), WireConflictMode::Error);
        buf.push_with_mode(2, &schema, &batch(&schema, &[(2, 20, 1)]), WireConflictMode::Error);
        buf.push(1, &schema, &batch(&schema, &[(3, 30, 0)])); // w=0 contributes nothing
        let net1 = net_of(&buf, 1);
        assert_eq!(net1.len(), 1);
        assert_eq!(val_of(&net1, 1), Some(10));
        assert!(!net1.contains_key(&PkTuple::from_u128(8, 3)), "w=0 row skipped");
        assert!(!net1.contains_key(&PkTuple::from_u128(8, 2)), "other tid excluded");
    }

    #[test]
    fn present_rows_of_an_empty_net_is_empty() {
        let schema = two_col(TypeCode::I64);
        assert!(rows_of(&present_rows(&Net::new(), &schema)).is_empty());
    }

    /// The live half of the net: an override and a transaction-born row are both
    /// `Present`; a tombstone contributes nothing.
    #[test]
    fn present_rows_keeps_overrides_and_born_rows_and_drops_tombstones() {
        let schema = two_col(TypeCode::I64);
        let tid = 7;
        let mut buf = TxnBuffer::default();
        buf.push(tid, &schema, &batch(&schema, &[(1, 99, 1)])); // override committed 1
        buf.delete(tid, &schema, PkColumn::from_u128s(8, [2])); // delete committed 2
        buf.push(tid, &schema, &batch(&schema, &[(5, 50, 1)])); // transaction-born

        let present = present_rows(&net_of(&buf, tid), &schema);
        assert_eq!(rows_of(&present), vec![(1, 99), (5, 50)]);
    }

    #[test]
    fn net_restricted_to_keys_excludes_untouched_and_unlisted() {
        let schema = two_col(TypeCode::I64);
        let tid = 7;
        let mut buf = TxnBuffer::default();
        buf.push(tid, &schema, &batch(&schema, &[(1, 11, 1), (2, 22, 1)]));

        // A key-pinned bound restricts the net to the keys it names: restricting
        // to [1] leaves pk=2's buffered row out of the candidates entirely.
        let full = net_of(&buf, tid);
        let only_1: Net = [PkTuple::from_u128(8, 1)]
            .iter()
            .filter_map(|pk| buf.last_op(tid, pk).map(|(b, r)| (*pk, op_of(b, r))))
            .collect();
        assert_eq!(full.len(), 2);
        assert_eq!(only_1.len(), 1);
        assert_eq!(rows_of(&present_rows(&only_1, &schema)), vec![(1, 11)]);
    }
}
