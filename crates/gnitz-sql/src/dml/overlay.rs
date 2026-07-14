//! The transaction read-your-own-writes overlay.
//!
//! An open transaction's writes are buffered, not sent, so a DML statement must
//! resolve its rows against the **effective** state: the transaction's own
//! buffered ops layered over the committed store. `TxnBuffer` indexes each
//! buffered row by PK as it arrives (`last_op`/`last_ops`), so the fold here is
//! a read of that index — the last op per PK, `Present`/`Deleted` by weight sign
//! (mirroring the engine's `fold_family`).
//!
//! [`effective_row`] returns an **owned** row so the caller can then borrow the
//! client mutably (seek, push) with no buffer borrow outstanding. The map-based
//! [`buffered_keys`]/[`buffered_all`] instead borrow each buffered row **in
//! place** (no copy); [`overlay_batch`] materializes them into the effective
//! batch exactly once, all before the caller's next `&mut client`.
//!
//! - [`effective_row`] — one PK: buffered op, else committed seek (INSERT's ON
//!   CONFLICT paths).
//! - [`buffered_keys`] / [`buffered_all`] — the net map for a known key set, or
//!   for every PK the transaction touched (UPDATE/DELETE's seek and scan paths).
//! - [`overlay_batch`] — apply such a map to a committed batch.
//!
//! Every map is **empty in autocommit**, where the overlay is the identity and
//! costs nothing: `HashMap::new()` does not allocate, and `overlay_batch` hands
//! the committed batch straight back.

use crate::error::GnitzSqlError;
use crate::exec::batch::copy_batch_row;
use gnitz_core::{GnitzClient, PkTuple, Schema, ZSetBatch};
use std::collections::HashMap;

/// A PK's net effect within the transaction so far. `Present` borrows the
/// buffered row in place (its batch + row index); `Deleted` is a tombstone.
pub(crate) enum Buffered<'a> {
    Present(&'a ZSetBatch, usize),
    Deleted,
}

/// PK → net effect, borrowing the buffer's rows. Empty in autocommit.
pub(crate) type Net<'a> = HashMap<PkTuple, Buffered<'a>>;

/// The row `pk` currently resolves to: the transaction's last buffered op wins
/// (`Present` → that row, `Deleted` → absent), else the committed store decides.
/// Owned, so the buffer borrow is released before the seek.
pub(crate) fn effective_row(
    client: &mut GnitzClient,
    tid: u64,
    schema: &Schema,
    pk: &PkTuple,
) -> Result<Option<ZSetBatch>, GnitzSqlError> {
    // The buffer borrow is confined to this block, so it ends before the seek.
    if let Some((batch, row)) = client.txn_buffer().and_then(|buf| buf.last_op(tid, pk)) {
        // Weight sign is the net effect: negative = buffered delete (absent),
        // else the live buffered row, copied out owned.
        return Ok((batch.weights[row] >= 0).then(|| {
            let mut one = ZSetBatch::new(schema);
            copy_batch_row(batch, row, &mut one, schema);
            one
        }));
    }
    // The transaction has not touched this PK — the committed store decides.
    Ok(client.seek(tid, pk)?.1.filter(|b| !b.pks.is_empty()))
}

/// The net map restricted to `keys` — the seek access paths, whose residual does
/// not constrain the PK, so only the seeked keys may enter the effective rows.
pub(crate) fn buffered_keys<'a>(client: &'a GnitzClient, tid: u64, keys: &[PkTuple]) -> Net<'a> {
    let Some(buf) = client.txn_buffer() else {
        return Net::new();
    };
    keys.iter()
        .filter_map(|pk| buf.last_op(tid, pk).map(|(b, r)| (*pk, op_of(b, r))))
        .collect()
}

/// The net map over every PK the transaction touched in `tid` — the scan access
/// paths, where any buffered row may satisfy the predicate.
pub(crate) fn buffered_all<'a>(client: &'a GnitzClient, tid: u64) -> Net<'a> {
    let Some(buf) = client.txn_buffer() else {
        return Net::new();
    };
    buf.last_ops(tid).map(|(pk, b, r)| (pk, op_of(b, r))).collect()
}

/// Layer `net` over a committed batch: the committed rows the transaction did
/// NOT touch, plus its `Present` rows. All copies under `actual` — buffered
/// batches are layout-identical to the reply (same base table; DDL is barred in
/// a transaction and base-table columns are never hidden).
///
/// An empty `net` (autocommit, or a table the transaction has not touched) is
/// the identity: the committed batch IS the effective state, returned uncopied.
pub(crate) fn overlay_batch(committed: Option<ZSetBatch>, net: &Net, actual: &Schema) -> ZSetBatch {
    if net.is_empty() {
        return committed.unwrap_or_else(|| ZSetBatch::new(actual));
    }
    let stride = actual.pk_stride() as u8;
    let mut eff = ZSetBatch::new(actual);
    if let Some(b) = &committed {
        for i in 0..b.len() {
            if !net.contains_key(&b.pks.get_tuple(i, stride)) {
                copy_batch_row(b, i, &mut eff, actual);
            }
        }
    }
    for op in net.values() {
        if let Buffered::Present(batch, row) = op {
            copy_batch_row(batch, *row, &mut eff, actual);
        }
    }
    eff
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

    fn rows_of(schema: &Schema, b: &ZSetBatch) -> Vec<(u128, i64)> {
        let stride = schema.pk_stride() as u8;
        let mut out: Vec<(u128, i64)> = (0..b.len())
            .map(|i| {
                let pk = b.pks.get_tuple(i, stride).split_wire().0;
                let val = match &b.columns[1] {
                    ColData::Fixed(buf) => i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap()),
                    _ => 0,
                };
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
        buf.delete(tid, &schema, PkColumn::U64s(vec![2]));

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
        buf.delete(tid, &schema, PkColumn::U64s(vec![5]));
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
    fn overlay_empty_net_returns_committed() {
        let schema = two_col(TypeCode::I64);
        let committed = batch(&schema, &[(1, 10, 1), (2, 20, 1)]);
        let eff = overlay_batch(Some(committed), &Net::new(), &schema);
        assert_eq!(rows_of(&schema, &eff), vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn overlay_overrides_deletes_and_adds_born_rows() {
        let schema = two_col(TypeCode::I64);
        let committed = batch(&schema, &[(1, 10, 1), (2, 20, 1)]);
        let tid = 7;
        let mut buf = TxnBuffer::default();
        buf.push(tid, &schema, &batch(&schema, &[(1, 99, 1)])); // override committed 1
        buf.delete(tid, &schema, PkColumn::U64s(vec![2])); // delete committed 2
        buf.push(tid, &schema, &batch(&schema, &[(5, 50, 1)])); // transaction-born

        let eff = overlay_batch(Some(committed), &net_of(&buf, tid), &schema);
        assert_eq!(rows_of(&schema, &eff), vec![(1, 99), (5, 50)]);
    }

    #[test]
    fn net_restricted_to_keys_excludes_untouched_and_unlisted() {
        let schema = two_col(TypeCode::I64);
        let tid = 7;
        let mut buf = TxnBuffer::default();
        buf.push(tid, &schema, &batch(&schema, &[(1, 11, 1), (2, 22, 1)]));

        // The seek paths only overlay the keys they seeked: restricting to [1]
        // leaves pk=2's buffered row out of the effective batch.
        let full = net_of(&buf, tid);
        let only_1: Net = [PkTuple::from_u128(8, 1)]
            .iter()
            .filter_map(|pk| buf.last_op(tid, pk).map(|(b, r)| (*pk, op_of(b, r))))
            .collect();
        assert_eq!(full.len(), 2);
        assert_eq!(only_1.len(), 1);
        let eff = overlay_batch(None, &only_1, &schema);
        assert_eq!(rows_of(&schema, &eff), vec![(1, 11)]);
    }
}
