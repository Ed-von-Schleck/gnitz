use super::*;
use crate::block_on;
use crate::test_support::{interrupt_self_until, kv_rows, kv_schema, reply_ctrl, session_pair};
use gnitz_wire::TypeCode;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use WireConflictMode::{Error, Update};

fn del(pk: u64) -> ZSetBatch {
    let s = Arc::new(kv_schema(TypeCode::I64));
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
    let s = Arc::new(kv_schema(TypeCode::I64));
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
        .map(|f| (f.target.tid, f.mode, f.basis, f.batch.weights.clone()))
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
    let s = Arc::new(kv_schema(TypeCode::I64));
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
    let s = Arc::new(kv_schema(TypeCode::I64));
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
    let s = Arc::new(kv_schema(TypeCode::I64));
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
    let s = Arc::new(kv_schema(TypeCode::I64));
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
    let s = Arc::new(kv_schema(TypeCode::I64));
    let mut wide = kv_schema(TypeCode::I64);
    wide.columns.push(ColumnDef::new("w", TypeCode::I64, false));
    let wide = Arc::new(wide);
    let wide_row = || {
        let mut b = ZSetBatch::new(&wide);
        BatchAppender::new(&mut b).add_row(2, 1).i64_val(20).i64_val(30);
        b
    };
    let mut buf = TxnBuffer::default();
    buf.push(16, &s, kv_rows(&[(1, 10, 1)]), Update, BLIND).unwrap();

    assert!(buf.push(16, &wide, wide_row(), Error, BLIND).is_err());
    let shape: Vec<_> = buf.families.iter().map(|f| (f.target.tid, f.batch.len())).collect();
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
    let signed = Arc::new(Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    });
    let batch = kv_rows(&[(1, 10, 1)]);
    for push in [
        block_on(c.push(16, &signed, &batch, Update)),
        block_on(c.push(16, &signed, batch.clone(), Update)),
    ] {
        let err = push.unwrap_err().to_string();
        assert!(err.contains("mismatched key column types"), "{err}");
    }
    assert!(c.txn_active());
    assert!(c.txn.as_ref().unwrap().families.is_empty());
    block_on(c.push(16, &Arc::new(kv_schema(TypeCode::I64)), &batch, Update)).unwrap();
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
        let err = block_on(c.create_table("public", "t", &schema, &fks, TableProps::default(), &[]))
            .unwrap_err()
            .to_string();
        let want = format!("{} foreign-key slots for 2 columns", fks.len());
        assert!(err.contains(&want), "{err}");
    }
}

/// A keys reply carries no payload, so a buffered row joins it as its key alone.
#[test]
fn a_keys_reply_appends_keys_only() {
    let s = Arc::new(kv_schema(TypeCode::I64));
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
    c.host = Box::new(BlockingHost::with_hook(Box::new(move || {
        if std::mem::replace(&mut fired, true) {
            Ok(())
        } else {
            Err("interrupted".into())
        }
    })));
    c.host.attach(c.session.as_fd()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));

    // The peer never answers, so the scan parks; the signal makes the park
    // return `EINTR`, which is the one place the hook runs.
    let (sa, spec) = (Arc::new(kv_schema(TypeCode::I64)), ReadSpec::all_rows(ReadBound::None));
    let r = block_on(c.scan_spec(1, &spec, &sa));
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
    assert_eq!(block_on(c.scan_spec(1, &spec, &sa)).unwrap().lsn, Some(200));
    let _peer = h.join().unwrap();
    assert_eq!(c.session.interest(), Interest::NONE);
}

// ── Waiting ─────────────────────────────────────────────────────────────────

/// A peer that answers every request with a control frame naming target 0 and
/// carrying the request's ordinal, so a reply's value names the request it
/// answered.
fn numbering_peer(peer: crate::test_support::Peer) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for k in 0.. {
            let mut len = [0u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES];
            if std::io::Read::read_exact(&mut &peer.0, &mut len).is_err() {
                return;
            }
            let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
            std::io::Read::read_exact(&mut &peer.0, &mut payload).unwrap();
            peer.send(&reply_ctrl(0, k));
        }
    })
}

/// A host that pends the way a reactor does: every other ask finds the socket
/// ready, and a job runs on a thread of its own — unless it is dropped.
#[derive(Default)]
struct PendingHost {
    asked: bool,
    drops_jobs: bool,
}

impl Host for PendingHost {
    fn attach(&mut self, _: BorrowedFd<'_>) -> std::io::Result<()> {
        Ok(())
    }

    fn poll_io(
        &mut self,
        _: Interest,
        _: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> Interest,
    ) -> Poll<Result<(), ClientError>> {
        self.asked = !self.asked;
        if self.asked {
            return Poll::Pending;
        }
        io(Interest::BOTH);
        Poll::Ready(Ok(()))
    }

    fn spawn(&mut self, job: Job) {
        if !self.drops_jobs {
            std::thread::spawn(job);
        }
    }
}

/// Poll `fut` to completion, parking on `fd` whenever it pends, and count the
/// polls it took.
fn drive<F: Future>(fd: RawFd, fut: F) -> (F::Output, usize) {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    let patience = std::time::Duration::from_millis(20);
    for polls in 1.. {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return (out, polls);
        }
        let _ = poll_fd(fd, libc::POLLIN, Some(std::time::Instant::now() + patience));
    }
    unreachable!()
}

/// Requests detached one after another are all on their way before any reply
/// is waited for, and waiting for the last answers every one before it.
#[test]
fn detached_requests_are_answered_by_one_wait() {
    let (s, peer) = session_pair();
    let peer = numbering_peer(peer);
    let mut c = GnitzClient::from_session(s);
    let ack = |c: &mut GnitzClient| c.ack(Request::AllocIds(1)).detach();
    let (mut first, mut second, last) = (ack(&mut c), ack(&mut c), ack(&mut c));
    assert_eq!(c.requests_sent(), 3);
    assert!(first.try_take().is_none(), "nothing has stepped the session");

    assert_eq!(block_on(c.wait(last)).unwrap(), 2);
    assert_eq!(first.try_take().unwrap().unwrap(), 0);
    assert_eq!(second.try_take().unwrap().unwrap(), 1);

    // A verb awaited whole is the same round trip, undetached.
    assert_eq!(block_on(c.alloc_id()).unwrap(), 3);
    drop(c);
    peer.join().unwrap();
}

/// A reply still owed when its session goes resolves `Closed`, on a replaced
/// connection's too: nothing waits on a session that is gone.
#[test]
fn a_session_that_goes_resolves_what_it_owed() {
    let (s, _peer) = session_pair();
    let mut c = GnitzClient::from_session(s);
    let mut owed = c.ack(Request::AllocIds(1)).detach();
    drop(c);
    assert!(matches!(owed.try_take(), Some(Err(ClientError::Closed))));

    let (s, _peer) = session_pair();
    let mut c = GnitzClient::from_session(s);
    let (replacement, _peer2) = session_pair();
    let stale = c.ack(Request::AllocIds(1)).detach();
    c.session = replacement;
    assert!(matches!(block_on(c.wait(stale)), Err(ClientError::Closed)));
}

/// `serve` under a host that really pends: a verb of three round trips, then
/// two detached requests left outstanding when the queue empties. Each runs in
/// its turn, and the loop's own stepping delivers the detached replies.
#[test]
fn serve_runs_calls_in_order_and_steps_for_detached_replies() {
    let (s, peer) = session_pair();
    let fd = s.as_raw_fd();
    let peer = numbering_peer(peer);
    let c = GnitzClient::over(s, Box::new(PendingHost::default())).unwrap();

    let whole = Arc::new(Mutex::new(None));
    let detached: Arc<Mutex<Vec<Sent<u64>>>> = Arc::default();
    let mut calls: VecDeque<Op> = VecDeque::new();
    let out = Arc::clone(&whole);
    calls.push_back(Box::new(move |c| {
        Box::pin(async move {
            let mut ids = Vec::new();
            for _ in 0..3 {
                ids.push(c.alloc_id().await.unwrap());
            }
            *out.lock().unwrap() = Some(ids);
        })
    }));
    for _ in 0..2 {
        let detached = Arc::clone(&detached);
        calls.push_back(Box::new(move |c| {
            Box::pin(async move {
                let sent = c.ack(Request::AllocIds(1)).detach();
                detached.lock().unwrap().push(sent);
            })
        }));
    }
    let (owed, mut answered) = (Arc::clone(&detached), Vec::new());
    let next = |_: &mut Context<'_>| match calls.pop_front() {
        Some(call) => Poll::Ready(Some(call)),
        // Nothing left to run, and two replies outstanding: only the loop's
        // own stepping can deliver them.
        None => {
            let mut owed = owed.lock().unwrap();
            while let Some(reply) = owed.first_mut().and_then(Sent::try_take) {
                answered.push(reply.unwrap());
                owed.remove(0);
            }
            match owed.is_empty() {
                true => Poll::Ready(None),
                false => Poll::Pending,
            }
        }
    };
    let (c, polls) = drive(fd, serve(c, next));
    assert_eq!(whole.lock().unwrap().take().unwrap(), [0, 1, 2]);
    assert_eq!(answered, [3, 4]);
    assert_eq!(c.requests_sent(), 5);
    assert!(polls > 1, "the host pended");
    drop(c);
    peer.join().unwrap();
}

/// A job runs where the host puts it, and its value, its panic or its loss
/// comes back to the call that offloaded it.
#[test]
fn an_offloaded_job_is_the_hosts_to_run() {
    let here = std::thread::current().id();
    let ran_on = || std::thread::current().id();
    let (s, _peer) = session_pair();
    let fd = s.as_raw_fd();
    let mut c = GnitzClient::over(s, Box::new(PendingHost::default())).unwrap();
    assert_ne!(drive(fd, c.offload(ran_on)).0.unwrap(), here);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drive(fd, c.offload(|| panic!("the job's own failure")))
    }));
    assert!(panicked.is_err(), "a job's panic resumes in its caller");

    // A blocking host runs it in place.
    let (s, _peer) = session_pair();
    assert_eq!(block_on(GnitzClient::from_session(s).offload(ran_on)).unwrap(), here);

    // A host that drops the job ends the call rather than leaving it waiting.
    let (s, _peer) = session_pair();
    let host = PendingHost { drops_jobs: true, ..Default::default() };
    let mut c = GnitzClient::over(s, Box::new(host)).unwrap();
    assert!(matches!(drive(fd, c.offload(|| 7)).0, Err(ClientError::Closed)));
}

/// `block_on` is for a host that never pends; over any other it would spin.
#[test]
#[should_panic(expected = "belongs to an event loop")]
fn block_on_refuses_a_host_that_pends() {
    let (s, _peer) = session_pair();
    let mut c = GnitzClient::over(s, Box::new(PendingHost::default())).unwrap();
    let _ = block_on(c.alloc_id());
}
