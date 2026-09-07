use super::*;
use crate::test_support::two_col;
use gnitz_core::{PkColumn, TxnBuffer, TypeCode, WireConflictMode};

/// (pk U64 PK, val I64) rows as (pk, val, weight).
fn batch(schema: &Schema, rows: &[(u128, i64, i64)]) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    for &(pk, val, w) in rows {
        b.pks.push_u128(schema, pk);
        b.weights.push(w);
        b.nulls.push(0);
        {
            let buf = &mut b.columns[1];
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    b
}

/// A one-PK retraction batch — what a buffered DELETE is.
fn del(schema: &Schema, pk: u128) -> ZSetBatch {
    gnitz_core::retraction_batch(schema, PkColumn::from_natives(schema, [pk]))
}

/// The full net map, straight off a buffer (the client-free half of
/// `buffered_all`).
fn net_of(buf: &mut TxnBuffer, tid: u64) -> Net<'_> {
    buf.reads(tid)
        .last_ops()
        .map(|(pk, b, row)| (pk, op_of(b, row)))
        .collect()
}

fn val_of(net: &Net, pk: u128) -> Option<i64> {
    match net.get(&key(pk)) {
        Some(Buffered::Present(batch, row)) => {
            let o = row * 8;
            Some(i64::from_le_bytes(batch.columns[1][o..o + 8].try_into().unwrap()))
        }
        _ => None,
    }
}

fn is_deleted(net: &Net, pk: u128) -> bool {
    matches!(net.get(&key(pk)), Some(Buffered::Deleted))
}

/// The identity key for `pk` under [`two_col`]'s single U64 PK.
fn key(pk: u128) -> PkBuf {
    gnitz_core::opk_key_packed(&two_col(TypeCode::I64), pk)
}

fn rows_of(b: &ZSetBatch) -> Vec<(u128, i64)> {
    let mut out: Vec<(u128, i64)> = (0..b.len())
        .map(|i| {
            let pk = b.pks.get(&two_col(TypeCode::I64), i);
            let val = b.cell(1, i, 8).map_or(0, |c| i64::from_le_bytes(c.try_into().unwrap()));
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
    buf.push(
        tid,
        &schema,
        batch(&schema, &[(1, 10, 1), (2, 20, 1)]),
        WireConflictMode::Error,
    );
    // Update pk=1 → v=11 [Update family].
    buf.push(tid, &schema, batch(&schema, &[(1, 11, 1)]), WireConflictMode::Update);
    // Delete pk=2 [coalesces with the Update family above].
    buf.push(tid, &schema, del(&schema, 2), WireConflictMode::Update);

    let net = net_of(&mut buf, tid);
    assert_eq!(net.len(), 2);
    assert_eq!(val_of(&net, 1), Some(11), "pk=1 last op is the update");
    assert!(is_deleted(&net, 2), "pk=2 deleted");
}

#[test]
fn net_delete_then_reinsert_is_present() {
    let schema = two_col(TypeCode::I64);
    let tid = 7;
    let mut buf = TxnBuffer::default();
    buf.push(tid, &schema, batch(&schema, &[(5, 50, 1)]), WireConflictMode::Error);
    buf.push(tid, &schema, del(&schema, 5), WireConflictMode::Update);
    buf.push(tid, &schema, batch(&schema, &[(5, 99, 1)]), WireConflictMode::Error);
    let net = net_of(&mut buf, tid);
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
    buf.push(1, &schema, batch(&schema, &[(1, 10, 1)]), WireConflictMode::Error);
    buf.push(2, &schema, batch(&schema, &[(2, 20, 1)]), WireConflictMode::Error);
    // w=0 contributes nothing
    buf.push(1, &schema, batch(&schema, &[(3, 30, 0)]), WireConflictMode::Update);
    let net1 = net_of(&mut buf, 1);
    assert_eq!(net1.len(), 1);
    assert_eq!(val_of(&net1, 1), Some(10));
    assert!(!net1.contains_key(&key(3)), "w=0 row skipped");
    assert!(!net1.contains_key(&key(2)), "other tid excluded");
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
    buf.push(tid, &schema, batch(&schema, &[(1, 99, 1)]), WireConflictMode::Update); // override committed 1
    buf.push(tid, &schema, del(&schema, 2), WireConflictMode::Update); // delete committed 2
    buf.push(tid, &schema, batch(&schema, &[(5, 50, 1)]), WireConflictMode::Update); // transaction-born

    let present = present_rows(&net_of(&mut buf, tid), &schema);
    assert_eq!(rows_of(&present), vec![(1, 99), (5, 50)]);
}

#[test]
fn net_restricted_to_keys_excludes_untouched_and_unlisted() {
    let schema = two_col(TypeCode::I64);
    let tid = 7;
    let mut buf = TxnBuffer::default();
    buf.push(
        tid,
        &schema,
        batch(&schema, &[(1, 11, 1), (2, 22, 1)]),
        WireConflictMode::Update,
    );

    // A key-pinned bound restricts the net to the keys it names: restricting
    // to [1] leaves pk=2's buffered row out of the candidates entirely.
    let full_len = net_of(&mut buf, tid).len();
    let reads = buf.reads(tid);
    let only_1: Net = [key(1)]
        .iter()
        .filter_map(|pk| reads.last_op(pk.pk_bytes()).map(|(b, r)| (*pk, op_of(b, r))))
        .collect();
    assert_eq!(full_len, 2);
    assert_eq!(only_1.len(), 1);
    assert_eq!(rows_of(&present_rows(&only_1, &schema)), vec![(1, 11)]);
}
