use super::*;
use crate::test_support::{interrupt_self_until, kv_rows, kv_schema, reply_ctrl, session_pair};
use gnitz_wire::TypeCode;
use std::sync::atomic::{AtomicBool, Ordering};
use WireConflictMode::{Error, Update};

fn del(pk: u64) -> ZSetBatch {
    let s = kv_schema(TypeCode::I64);
    retraction_batch(&s, PkColumn::from_natives(&s, [pk as u128]))
}

/// `(pk, v, weight)` of a `kv_schema(I64)` batch, sorted.
fn contents(b: &ZSetBatch) -> Vec<(u64, i64, i64)> {
    let mut out: Vec<_> = (0..b.len())
        .map(|i| (b.pks.get(i) as u64, payload_u64(b, i, 0) as i64, b.weights[i]))
        .collect();
    out.sort();
    out
}

/// `buf`'s overlay of `committed`, a read of `tid` under `bound` and
/// `predicate`, as [`contents`].
fn overlay_of(
    buf: &mut TxnBuffer,
    tid: u64,
    bound: ReadBound,
    predicate: Vec<u8>,
    committed: ZSetBatch,
) -> Vec<(u64, i64, i64)> {
    let spec = ReadSpec {
        bound,
        predicate,
        sink: ReadSink::all_rows(),
    };
    contents(
        &buf.overlay(tid, &kv_schema(TypeCode::I64), &spec, false, committed)
            .unwrap(),
    )
}

/// [`overlay_of`] every row of an empty committed read.
fn overlaid(buf: &mut TxnBuffer, tid: u64) -> Vec<(u64, i64, i64)> {
    overlay_of(buf, tid, ReadBound::None, Vec::new(), kv_rows(&[]))
}

/// One mixed sequence pins the whole buffer: same-mode pushes extend the tid's
/// current run and keep its oldest basis (a blind batch weakens nothing), a mode
/// change opens the next family in call order, an empty batch opens none, tids
/// are independent, and every PK indexes to its latest row. "delete k;
/// insert_error k" is the Update-then-Error pair.
#[test]
fn pushes_coalesce_per_tid_into_maximal_same_mode_runs() {
    let s = kv_schema(TypeCode::I64);
    let mut buf = TxnBuffer::default();
    for (tid, batch, mode, basis) in [
        (16, kv_rows(&[(1, 11, 1)]), Update, BLIND), // family 0, row 0
        (16, kv_rows(&[]), Update, 5),               // empty: opens no family, indexes nothing
        (16, kv_rows(&[(2, 21, 1)]), Update, 7),     // extends family 0, row 1, and lowers its basis
        (16, kv_rows(&[(4, 41, 1)]), Update, BLIND), // extends family 0, row 2, and raises nothing
        (17, kv_rows(&[(1, 12, 1)]), Update, BLIND), // family 1 — a different tid
        (17, kv_rows(&[]), Update, 5),               // empty: lowers nothing
        (16, kv_rows(&[(3, 31, 1)]), Error, BLIND),  // family 2: the mode changed
        (16, del(1), Update, 9),                     // family 3: back to Update, in call order
        (16, kv_rows(&[(1, 13, 1)]), Error, BLIND),  // family 4: the Error re-insert
    ] {
        buf.push(tid, &s, batch, mode, basis).unwrap();
    }

    let shape: Vec<_> = buf
        .families
        .iter()
        .map(|f| (f.tid, f.mode, f.basis, f.batch.weights.clone()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (16, Update, 7, vec![1, 1, 1]),
            (17, Update, BLIND, vec![1]),
            (16, Error, BLIND, vec![1]),
            (16, Update, 9, vec![-1]),
            (16, Error, BLIND, vec![1]),
        ]
    );

    assert_eq!(
        overlaid(&mut buf, 16),
        [(1, 13, 1), (2, 21, 1), (3, 31, 1), (4, 41, 1)],
        "the last of pk 1's three ops wins"
    );
    assert_eq!(overlaid(&mut buf, 17), [(1, 12, 1)], "tids are independent");
}

/// A buffered row joins the read at weight 1, as the committed rows beside it
/// stand, whatever weight it was pushed at.
#[test]
fn the_overlay_adds_a_buffered_row_at_weight_one() {
    let s = kv_schema(TypeCode::I64);
    let mut buf = TxnBuffer::default();
    buf.push(7, &s, kv_rows(&[(1, 11, 2)]), Update, 3).unwrap();
    let got = overlay_of(&mut buf, 7, ReadBound::None, Vec::new(), kv_rows(&[(2, 20, 1)]));
    assert_eq!(got, [(1, 11, 1), (2, 20, 1)]);
}

/// The overlay replaces every committed row the transaction wrote with its last
/// op, and holds the buffered side to the read's bound and predicate. Each
/// committed read is what the server would have answered under that spec.
#[test]
fn the_overlay_replaces_committed_rows_with_the_last_buffered_op() {
    // `v > 15`, over the source schema's column indices.
    let mut b = gnitz_expr::ExprBuilder::new();
    let c = b.emit(gnitz_expr::LogicalInstr::LoadCol { col: 1 });
    let k = b.emit(gnitz_expr::LogicalInstr::LoadConst { val: 15, unsigned: false });
    let cond = b.emit(gnitz_expr::LogicalInstr::Cmp { op: gnitz_expr::CmpOp::Gt, a: c, b: k });
    let over_15 = b.build(vec![gnitz_expr::Sink::Reg(cond)]).unwrap().to_blob_bytes();
    let s = kv_schema(TypeCode::I64);
    let committed = [(1, 10, 1), (2, 20, 1), (5, 50, 1)];

    for (what, ops, bound, predicate, committed, want) in [
        (
            "the last op on a key wins",
            vec![(kv_rows(&[(1, 11, 1)]), Update), (kv_rows(&[(1, 12, 1)]), Update)],
            ReadBound::None,
            Vec::new(),
            &committed[..],
            &[(1, 12, 1), (2, 20, 1), (5, 50, 1)][..],
        ),
        (
            "a delete then re-insert is present",
            vec![(del(5), Update), (kv_rows(&[(5, 99, 1)]), Error)],
            ReadBound::None,
            Vec::new(),
            &committed[..],
            &[(1, 10, 1), (2, 20, 1), (5, 99, 1)][..],
        ),
        (
            "a tombstone drops the committed row",
            vec![(del(2), Update)],
            ReadBound::None,
            Vec::new(),
            &committed[..],
            &[(1, 10, 1), (5, 50, 1)][..],
        ),
        (
            "a PK set restricts the buffered side to its keys",
            vec![(kv_rows(&[(1, 11, 1), (2, 22, 1)]), Update)],
            ReadBound::PkSet(PkColumn::from_natives(&s, [1]).keys()),
            Vec::new(),
            &[(1, 10, 1)][..],
            &[(1, 11, 1)][..],
        ),
        (
            "a committed row rewritten to fail the predicate is gone",
            vec![(kv_rows(&[(2, 5, 1), (1, 30, 1)]), Update)],
            ReadBound::None,
            over_15.clone(),
            &[(2, 20, 1)][..],
            &[(1, 30, 1)][..],
        ),
    ] {
        let mut buf = TxnBuffer::default();
        for (batch, mode) in ops {
            buf.push(7, &s, batch, mode, 3).unwrap();
        }
        let got = overlay_of(&mut buf, 7, bound, predicate, kv_rows(committed));
        assert_eq!(got, want, "{what}");
    }
}

/// The overlay is the committed read itself when there is nothing to overlay: a
/// relation the transaction never wrote, and one whose only ops are inert weight-0
/// rows.
#[test]
fn overlay_is_the_committed_read_without_a_live_op() {
    let s = kv_schema(TypeCode::I64);
    let spec = ReadSpec::all_rows(ReadBound::None);
    let committed = kv_rows(&[(1, 10, 1), (2, 20, 1)]);
    let mut buf = TxnBuffer::default();
    let out = buf.overlay(16, &s, &spec, false, committed.clone()).unwrap();
    assert_eq!(out, committed, "an untouched relation");

    buf.push(16, &s, kv_rows(&[(1, 10, 0)]), Update, BLIND).unwrap();
    let out = buf.overlay(16, &s, &spec, false, committed.clone()).unwrap();
    assert_eq!(out, committed, "only a weight-0 op");
}

/// The read-your-own-writes index is built on read: a blind transaction builds
/// none of it, a read indexes only the relation it reads, and a write after a
/// read waits for the next read.
#[test]
fn the_read_index_is_built_on_read() {
    let s = kv_schema(TypeCode::I64);
    let indexed = |buf: &TxnBuffer| buf.families.iter().map(|f| f.indexed).sum::<usize>();
    let mut buf = TxnBuffer::default();
    buf.push(17, &s, kv_rows(&[(9, 90, 1)]), Error, BLIND).unwrap();
    for pk in 1..=4 {
        buf.push(16, &s, kv_rows(&[(pk, 10, 1)]), Error, BLIND).unwrap();
    }
    assert_eq!(indexed(&buf), 0, "a blind transaction indexes nothing");

    assert_eq!(overlaid(&mut buf, 16).len(), 4);
    assert_eq!(indexed(&buf), 4, "another relation is not indexed by this read");

    buf.push(16, &s, kv_rows(&[(5, 10, 1)]), Error, BLIND).unwrap();
    assert_eq!(indexed(&buf), 4, "the write itself indexes nothing");
    assert_eq!(overlaid(&mut buf, 16).len(), 5);
    assert_eq!(indexed(&buf), 5);
}

/// Every family of a tid shares one layout: a batch in another is refused and
/// leaves the buffer as it was, and a read under another layout is refused too.
#[test]
fn a_tid_refuses_a_second_layout() {
    let s = kv_schema(TypeCode::I64);
    let mut wide = kv_schema(TypeCode::I64);
    wide.columns.push(ColumnDef::new("w", TypeCode::I64, false));
    let wide_row = || {
        let mut b = ZSetBatch::new(&wide);
        BatchAppender::new(&mut b).add_row(2, 1).i64_val(20).i64_val(30);
        b
    };
    let mut buf = TxnBuffer::default();
    buf.push(16, &s, kv_rows(&[(1, 10, 1)]), Update, BLIND).unwrap();

    assert!(buf.push(16, &wide, wide_row(), Error, BLIND).is_err());
    let shape: Vec<_> = buf.families.iter().map(|f| (f.tid, f.batch.len())).collect();
    assert_eq!(shape, [(16, 1)], "the refused batch left no trace");
    let spec = ReadSpec::all_rows(ReadBound::None);
    assert!(buf.overlay(16, &wide, &spec, false, ZSetBatch::new(&wide)).is_err());

    // Another tid is independent.
    buf.push(17, &wide, wide_row(), Error, BLIND).unwrap();
}

/// A batch built under another schema is refused at the push, a tid's first
/// included, and the transaction stays open.
#[test]
fn a_transaction_refuses_a_batch_of_another_schema_and_stays_open() {
    let (s, _peer) = session_pair();
    let mut c = GnitzClient::from_session(s);
    c.txn_begin().unwrap();
    let signed = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let batch = kv_rows(&[(1, 10, 1)]);
    for push in [
        c.push(16, &signed, &batch, Update),
        c.push_owned(16, &signed, batch.clone(), Update),
    ] {
        let err = push.unwrap_err().to_string();
        assert!(err.contains("mismatched key column types"), "{err}");
    }
    assert!(c.txn_active());
    assert!(c.txn.as_ref().unwrap().families.is_empty());
    c.push(16, &kv_schema(TypeCode::I64), &batch, Update).unwrap();
    assert_eq!(c.txn.as_ref().unwrap().families.len(), 1);
}

/// Foreign-key slots that are neither absent nor one per column are refused
/// before anything is sent — the peer is gone, so a request would fail as a
/// transport error instead.
#[test]
fn create_table_refuses_an_fk_it_cannot_write() {
    let (s, peer) = session_pair();
    drop(peer);
    let mut c = GnitzClient::from_session(s);
    let schema = kv_schema(TypeCode::I64);
    let fk = Some(FkTarget::Table(FkRef { table_id: 16, col: 0 }));
    for fks in [vec![fk], vec![None, fk, None]] {
        let err = c
            .create_table("public", "t", &schema, &fks, TableProps::default(), &[])
            .unwrap_err()
            .to_string();
        let want = format!("{} foreign-key slots for 2 columns", fks.len());
        assert!(err.contains(&want), "{err}");
    }
}

/// A keys reply carries no payload, so a buffered row joins it as its key alone.
#[test]
fn a_keys_reply_appends_keys_only() {
    let s = kv_schema(TypeCode::I64);
    let mut buf = TxnBuffer::default();
    buf.push(7, &s, kv_rows(&[(3, 30, 1)]), Update, 3).unwrap();
    let (reply, sink) = key_reply(&s);
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink,
    };
    let mut committed = ZSetBatch::new(&reply);
    BatchAppender::new(&mut committed).add_row(1, 1);
    let out = buf.overlay(7, &s, &spec, true, committed).unwrap();

    let mut want = ZSetBatch::new(&reply);
    let mut app = BatchAppender::new(&mut want);
    app.add_row(1, 1);
    app.add_row(3, 1);
    assert_eq!(out, want);
}

// ---------------------------------------------------------------------------
// The blocking driver
// ---------------------------------------------------------------------------

/// An aborted park leaves its slot pending — the frame is already on the
/// wire, so the next call drains that reply before its own.
#[test]
fn an_aborted_park_leaves_its_slot_pending_and_the_next_call_drains_it_first() {
    let (s, peer) = session_pair();
    let mut c = GnitzClient::from_session(s);
    let mut fired = false;
    c.set_park_hook(Some(Box::new(move || {
        if std::mem::replace(&mut fired, true) {
            Ok(())
        } else {
            Err("interrupted".into())
        }
    })));
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));

    // The peer never answers, so the scan parks; the signal makes the park
    // return `EINTR`, which is the one place the hook runs.
    let (sa, spec) = (Arc::new(kv_schema(TypeCode::I64)), ReadSpec::all_rows(ReadBound::None));
    let r = c.scan_spec(1, &spec, &sa);
    assert!(matches!(r, Err(ClientError::Interrupted(ref e)) if e.to_string() == "interrupted"));
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    assert_eq!(c.session.interest(), Interest::READ, "the abandoned slot stays pending");

    // The peer answers both requests; the second call must get the second.
    peer.recv();
    peer.send(&reply_ctrl(1, 100));
    let h = std::thread::spawn(move || {
        peer.recv();
        peer.send(&reply_ctrl(1, 200));
        peer
    });
    assert_eq!(c.scan_spec(1, &spec, &sa).unwrap().lsn, Some(200));
    let _peer = h.join().unwrap();
    assert_eq!(c.session.interest(), Interest::NONE);
}
