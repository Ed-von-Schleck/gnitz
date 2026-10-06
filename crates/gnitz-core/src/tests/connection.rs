//! The spine under bytes only a test can arrange: a scripted peer on the far
//! end of a socketpair feeds reply frames one `step` at a time.

use super::*;
use crate::protocol::transport::poll_fd;
use crate::test_support::{framed, kv_rows, kv_schema, reply_ctrl, reply_status, session_pair as pair};
use crate::BatchAppender;
use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TypeCode, WireFlags};

/// Submit a read of every row of `tid`, decoded under `schema`.
fn submit_scan(s: &mut Session, tid: u64, schema: &Arc<Schema>) -> Result<Sent<ScanReply>, ClientError> {
    s.submit_scan(tid.into(), &ReadSpec::all_rows(ReadBound::None), schema)
}

/// Step and park until `sent` is answered.
fn await_reply<T>(s: &mut Session, mut sent: Sent<T>) -> Result<T, ClientError> {
    let mut ready = Interest::WRITE;
    loop {
        s.step(ready);
        if let Some(reply) = sent.try_take() {
            return reply;
        }
        ready = Interest::from_revents(poll_fd(s.as_raw_fd(), s.interest().poll_events(), None).unwrap());
    }
}

/// A request ACKed at target 0.
const COMMIT: Request<'static> = Request::PushTxn { families: &[] };

fn schema_a() -> Arc<Schema> {
    Arc::new(kv_schema(TypeCode::I64))
}

fn schema_b() -> Arc<Schema> {
    Arc::new(Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("s", TypeCode::String, false),
            ColumnDef::new("f", TypeCode::F64, false),
        ],
        pk_cols: vec![0],
    })
}

/// `kv_rows` at weight 1, each `v` ten times its pk.
fn batch_a(pks: &[u64]) -> ZSetBatch {
    kv_rows(&pks.iter().map(|&pk| (pk, pk as i64 * 10, 1)).collect::<Vec<_>>())
}

fn batch_b(pks: &[u64]) -> ZSetBatch {
    let schema = schema_b();
    let mut b = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut b);
    for &pk in pks {
        app.add_row(pk as u128, 1)
            .str_val(&format!("s{pk}"))
            .f64_val(pk as f64 * 0.5);
    }
    b
}

fn reply_header(tid: u64, lsn: u64, cont: bool) -> ControlHeader {
    ControlHeader {
        target_id: tid,
        flags: WireFlags { continuation: cont, ..Default::default() },
        arg0: lsn,
        ..Default::default()
    }
}

/// A reply frame carrying `batch` and `lsn` in `arg0`, and no schema block;
/// `cont` sets `continuation`.
fn reply_rows(tid: u64, batch: &ZSetBatch, lsn: u64, cont: bool) -> Vec<u8> {
    encode_frame(reply_header(tid, lsn, cont), &[], None, Some(batch))
}

#[test]
fn train_split_across_continuation_frames_completes_once() {
    let (mut s, peer) = pair();
    let schema = schema_a();
    let mut sent = submit_scan(&mut s, 7, &schema).unwrap();
    assert_eq!(s.interest(), Interest::BOTH);
    s.step(Interest::WRITE);
    assert!(sent.try_take().is_none());
    assert_eq!(s.interest(), Interest::READ);
    peer.recv();

    // Three frames, one per step: nothing completes until the terminal.
    peer.send(&reply_rows(7, &batch_a(&[1, 2]), 0, true));
    s.step(Interest::READ);
    assert!(sent.try_take().is_none());
    peer.send(&reply_rows(7, &batch_a(&[3]), 0, true));
    s.step(Interest::READ);
    assert!(sent.try_take().is_none());
    peer.send(&reply_rows(7, &batch_a(&[4, 5]), 0, false));
    s.step(Interest::READ);
    let r = sent.try_take().unwrap().unwrap();
    assert_eq!(r.batch, batch_a(&[1, 2, 3, 4, 5]));
    assert!(Arc::ptr_eq(&r.schema, &schema), "the schema the request named");
    assert_eq!(s.interest(), Interest::NONE);
}

#[test]
fn scan_multi_decodes_each_train_under_its_own_relation() {
    let (mut s, peer) = pair();
    let (sa, sb) = (schema_a(), schema_b());
    // Each train must decode under the schema paired with its relation — a
    // two-frame train for relation 2 included.
    let mut sent = s
        .submit_scan_multi(vec![(1, Arc::clone(&sa)), (2, Arc::clone(&sb))])
        .unwrap();
    s.step(Interest::WRITE);
    let req = peer.recv();
    let ctrl = peek_control_block(&req).unwrap();
    let items = gnitz_wire::txn_frame::decode_scan_multi(&req[ctrl.body]).unwrap();
    let layouts: Vec<(u64, u64)> = items.iter().map(|i| (i.tid, i.reply_layout)).collect();
    assert_eq!(
        layouts,
        [(1, sa.layout_digest()), (2, sb.layout_digest())],
        "each item names its reply layout"
    );
    peer.send(&reply_rows(1, &batch_a(&[10, 11]), 0, false));
    s.step(Interest::READ);
    assert!(sent.try_take().is_none(), "one of two trains");
    peer.send(&reply_rows(2, &batch_b(&[20]), 0, true));
    peer.send(&reply_rows(2, &batch_b(&[21]), 0, false));
    let replies = await_reply(&mut s, sent).unwrap();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0].batch, batch_a(&[10, 11]));
    assert_eq!(replies[1].batch, batch_b(&[20, 21]));
    // Each reply carries the schema its relation was paired with.
    assert!(Arc::ptr_eq(&replies[0].schema, &sa));
    assert!(Arc::ptr_eq(&replies[1].schema, &sb));
    assert_eq!(s.interest(), Interest::NONE);
}

/// A schema block on a read's reply fails the slot and ends the session.
#[test]
fn a_schema_block_on_a_read_reply_fails_the_slot_and_ends_the_session() {
    let (mut s, peer) = pair();
    let sa = schema_a();
    let sent = submit_scan(&mut s, 7, &sa).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&encode_frame(
        reply_header(7, 0, false),
        &[],
        Some(&sa.to_block()),
        Some(&batch_a(&[5])),
    ));
    let e = await_reply(&mut s, sent).expect_err("a schema block on a read is a decode error");
    assert!(
        matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
        "{e:?}"
    );
    assert!(s.is_closed());
}

/// A status frame completes the slot at the head as that status's refusal —
/// whether it replaces a later train of a multi-read or answers a commit — and
/// the connection keeps serving.
#[test]
fn a_status_frame_fails_its_slot_alone() {
    let sa = schema_a();
    /// `sent`'s request refused `status` after `trains_before` of its trains.
    fn refused<T: std::fmt::Debug>(
        what: &str,
        submit: impl FnOnce(&mut Session) -> Result<Sent<T>, ClientError>,
        trains_before: u64,
        status: WireStatus,
    ) {
        let sa = schema_a();
        let (mut s, peer) = pair();
        let sent = submit(&mut s).unwrap();
        s.step(Interest::WRITE);
        peer.recv();
        for tid in 1..=trains_before {
            peer.send(&reply_rows(tid, &batch_a(&[tid]), 0, false));
        }
        peer.send(&reply_status(0, status, "refused"));
        let r = await_reply(&mut s, sent);
        assert!(
            matches!(&r, Err(ClientError::Refused(f)) if f.status == status && f.text == "refused"),
            "{what}: {r:?}"
        );
        assert!(!s.is_closed(), "{what}");
        assert_eq!(s.interest(), Interest::NONE, "{what}: nothing left pending");

        let sent = submit_scan(&mut s, 3, &sa).unwrap();
        s.step(Interest::WRITE);
        peer.recv();
        peer.send(&reply_rows(3, &batch_a(&[5]), 42, false));
        let r = await_reply(&mut s, sent).unwrap();
        assert_eq!(r.lsn, Some(42), "{what}: the next request completes normally");
    }
    let three = (1..=3).map(|tid| (tid, Arc::clone(&sa))).collect();
    refused(
        "the second of three trains",
        |s| s.submit_scan_multi(three),
        1,
        WireStatus::Error,
    );
    refused("a commit", |s| s.submit(COMMIT), 0, WireStatus::TxnConflict);
}

/// A request `submit` refuses opens no slot and queues nothing: the reply queue
/// stays aligned with what the server was sent.
#[test]
fn a_refused_submit_leaves_the_session_as_it_was() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    let ddl = [(12345, ZSetBatch::new(&sa))];
    let wrong_layout = batch_b(&[1]);
    // One cell alone past the server's ingress cap, refused before it is sent.
    let sb = schema_b();
    let mut oversize = ZSetBatch::new(&sb);
    BatchAppender::new(&mut oversize)
        .add_row(1, 1)
        .str_val(&"x".repeat(gnitz_wire::MAX_FRAME_PAYLOAD))
        .f64_val(0.0);
    let untouched = |s: &Session, what: &str| {
        assert_eq!(s.requests_sent(), 0, "{what}");
        assert_eq!(s.interest(), Interest::NONE, "{what}");
        assert!(!s.is_closed(), "{what}");
    };
    assert!(s.submit_scan_multi(Vec::new()).is_err());
    untouched(&s, "an empty multi-read");
    for (what, req) in [
        ("a DDL family that is no system table", Request::DdlTxn(&ddl)),
        (
            "a push whose batch is not in its schema's layout",
            Request::Push {
                target: 4.into(),
                schema: &sa,
                batch: &wrong_layout,
                mode: WireConflictMode::Update,
            },
        ),
        (
            "a push whose frame is past the server's ingress cap",
            Request::Push {
                target: 4.into(),
                schema: &sb,
                batch: &oversize,
                mode: WireConflictMode::Update,
            },
        ),
    ] {
        assert!(s.submit(req).is_err(), "{what}");
        untouched(&s, what);
    }
}

#[test]
fn one_read_serves_two_slots_and_leaves_nothing_buffered() {
    let (mut s, peer) = pair();
    let sa = schema_a();
    let mut sent = [1, 2].map(|tid| submit_scan(&mut s, tid, &sa).unwrap());
    s.step(Interest::WRITE);
    peer.recv();
    peer.recv();
    let empty = ZSetBatch::new(&sa);
    let mut both = framed(&reply_rows(1, &empty, 11, false));
    both.extend(framed(&reply_rows(2, &empty, 22, false)));
    peer.send_bytes(&both);
    s.step(Interest::READ);
    for (sent, lsn) in sent.iter_mut().zip([11, 22]) {
        let r = sent.try_take().unwrap().unwrap();
        assert_eq!(r.lsn, Some(lsn), "in request order");
    }
    assert_eq!(s.interest(), Interest::NONE);
}

/// A reply that decoded ahead of the frame that broke the framing reaches
/// its slot; the slot the bad frame was for fails, and so does every later
/// call, without touching the socket.
#[test]
fn replies_completed_before_a_fatal_frame_are_delivered() {
    let (mut s, peer) = pair();
    let (sa, rows) = (schema_a(), batch_a(&[1]));
    let mut a = s.submit(COMMIT).unwrap();
    let push = Request::Push {
        target: 5.into(),
        schema: &sa,
        batch: &rows,
        mode: WireConflictMode::Update,
    };
    let mut b = s.submit(push).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    peer.recv();
    // The push to 5 is answered by a frame naming 6.
    let mut both = framed(&reply_ctrl(0, 11));
    both.extend(framed(&reply_ctrl(6, 1)));
    peer.send_bytes(&both);
    s.step(Interest::READ);
    let (a, b) = (a.try_take().unwrap(), b.try_take().unwrap());
    assert!(matches!(a, Ok(11)), "{a:?}");
    assert!(
        matches!(&b, Err(ClientError::ConnectionLost(ProtocolError::DecodeError(_)))),
        "{b:?}"
    );
    assert!(s.is_closed());
    assert_eq!(s.interest(), Interest::NONE);
    assert!(matches!(
        submit_scan(&mut s, 1, &sa),
        Err(ClientError::ConnectionLost(_))
    ));
}

/// A peer that answered and then went is still readable after the write
/// that finds it gone: the answer is delivered, the unanswered slot fails.
#[test]
fn a_reply_readable_behind_a_failed_flush_is_delivered() {
    let (mut s, peer) = pair();
    let mut a = s.submit(COMMIT).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&reply_ctrl(0, 11));
    drop(peer);
    let mut b = submit_scan(&mut s, 5, &schema_a()).unwrap();
    s.step(Interest::WRITE);
    let (a, b) = (a.try_take().unwrap(), b.try_take().unwrap());
    assert!(matches!(a, Ok(11)), "{a:?}");
    assert!(
        matches!(&b, Err(ClientError::ConnectionLost(ProtocolError::IoError(_)))),
        "{b:?}"
    );
}

#[test]
fn a_peer_that_closes_surfaces_an_io_error_rather_than_a_park() {
    let (mut s, peer) = pair();
    let mut sent = submit_scan(&mut s, 5, &schema_a()).unwrap();
    s.step(Interest::WRITE);
    // The request is on the wire and unread, so dropping the peer draws a
    // reset. A hangup folds into read readiness, so the waiting driver wakes.
    drop(peer);
    let rev = poll_fd(
        s.as_raw_fd(),
        Interest::READ.poll_events(),
        Some(Instant::now() + std::time::Duration::from_secs(5)),
    )
    .expect("poll");
    assert!(Interest::from_revents(rev).read, "a hangup wakes a read park");
    s.step(Interest::READ);
    let lost = sent.try_take().unwrap();
    assert!(
        matches!(&lost, Err(ClientError::ConnectionLost(ProtocolError::IoError(_)))),
        "{lost:?}"
    );
}

#[test]
fn close_abandons_every_pending_slot_and_refuses_further_work() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    let mut owed = [1, 2].map(|tid| submit_scan(&mut s, tid, &sa).unwrap());
    assert_eq!(s.interest(), Interest::BOTH);
    s.close();
    for sent in &mut owed {
        assert!(matches!(sent.try_take(), Some(Err(ClientError::Closed))));
    }
    assert_eq!(s.interest(), Interest::NONE);
    assert!(matches!(submit_scan(&mut s, 3, &sa), Err(ClientError::Closed)));
}

/// The in-flight count bounds no memory on its own, so the byte cap is what
/// stops a driver that submits without ever flushing.
#[test]
fn queued_bytes_tracks_the_write_cursor_and_caps_submission() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    // About 1 MiB encoded, so the cap is some sixty pushes away.
    let b = batch_a(&(0..32_768).collect::<Vec<_>>());
    let push = |s: &mut Session| {
        let push = Request::Push {
            target: 4.into(),
            schema: &sa,
            batch: &b,
            mode: WireConflictMode::Update,
        };
        s.submit(push).map(drop)
    };
    // An encoded push is a full copy of its batch, so the counter grows with
    // what was pushed, not with how many times.
    push(&mut s).unwrap();
    let one = s.queued_bytes();
    assert!(one > b.len() * 32, "the frame carries the batch");
    push(&mut s).unwrap();
    assert_eq!(s.queued_bytes(), 2 * one, "bytes, not submits");

    // The cap is checked before queueing, so the push that crosses it goes
    // through and it is the *next* submit that is refused.
    while !s.at_capacity() {
        push(&mut s).unwrap();
    }
    let queued = s.queued_bytes();
    assert!(
        queued - one < MAX_QUEUED_BYTES,
        "the last push was admitted under the cap"
    );
    let sent = s.requests_sent();
    assert!(matches!(push(&mut s), Err(ClientError::Refused(_))));
    assert_eq!(
        (s.queued_bytes(), s.requests_sent()),
        (queued, sent),
        "the refusal queued nothing"
    );
    assert!(s.interest().write, "nothing was flushed, so it is all still queued");

    // Writing gives the budget back — the counter tracks the write cursor,
    // not just the pushes.
    s.step(Interest::WRITE);
    assert!(s.queued_bytes() < queued, "a flush releases what it wrote");
}

#[test]
fn in_flight_cap_raises_rather_than_hanging() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    for _ in 0..MAX_IN_FLIGHT {
        assert!(!s.at_capacity());
        submit_scan(&mut s, 1, &sa).unwrap();
    }
    assert!(s.at_capacity());
    assert!(matches!(submit_scan(&mut s, 1, &sa), Err(ClientError::Refused(_))));
}

/// A delta poll that fails whole — refused at target 0, or cut off by the
/// connection — still ends every view it had yet to answer, once each.
#[test]
fn a_poll_that_fails_whole_ends_each_unanswered_view() {
    let item = |view_id| txn_frame::DeltaPollItem {
        view_id,
        after_tick: 4,
        reply_layout: schema_a().layout_digest(),
        spec: &[],
    };
    for refused in [true, false] {
        let (mut s, peer) = pair();
        s.submit_delta_poll(&[item(7), item(8), item(9)], Duration::ZERO);
        s.step(Interest::WRITE);
        peer.recv();
        // View 7 is answered; the failure finds 8 and 9 open.
        let mut wire = framed(&encode_frame(
            ControlHeader {
                target_id: 7,
                arg0: 9,
                arg1: 1,
                ..Default::default()
            },
            &[],
            None,
            None,
        ));
        if refused {
            wire.extend(framed(&reply_status(0, WireStatus::Error, "refused")));
        }
        peer.send_bytes(&wire);
        if !refused {
            drop(peer);
        }
        while !s.interest().is_empty() {
            s.step(Interest::READ);
        }
        assert_eq!(s.is_closed(), !refused);
        let ends: Vec<PollEnd> = std::mem::take(&mut s.polls.queue)
            .into_iter()
            .map(|polled| match polled {
                Polled::End(end) => end,
                Polled::Block(_) => panic!("no view sent a block"),
            })
            .collect();
        assert_eq!(ends.len(), 3, "one end per view: {ends:?}");
        assert!(ends[0].is_ok(), "{ends:?}");
        for end in &ends[1..] {
            match end {
                Err(ClientError::Refused(f)) => assert!(refused && f.text == "refused"),
                Err(ClientError::ConnectionLost(_)) => assert!(!refused),
                other => panic!("{other:?}"),
            }
        }
    }
}

/// A poll whose reader is gone hands nothing on: its trains are read off the
/// socket and dropped, and the next poll's results are the queue's alone.
#[test]
fn an_abandoned_poll_queues_nothing() {
    let item = |view_id| txn_frame::DeltaPollItem {
        view_id,
        after_tick: 4,
        reply_layout: schema_a().layout_digest(),
        spec: &[],
    };
    let terminal = |tid| ControlHeader {
        target_id: tid,
        arg0: 9,
        arg1: 1,
        ..Default::default()
    };
    let (mut s, peer) = pair();
    s.submit_delta_poll(&[item(7)], Duration::ZERO);
    s.abandon_poll();
    s.submit_delta_poll(&[item(8)], Duration::ZERO);
    s.step(Interest::WRITE);
    let mut wire = Vec::new();
    for tid in [7, 8] {
        wire.extend(framed(&reply_rows(tid, &batch_a(&[tid]), 0, true)));
        wire.extend(framed(&encode_frame(terminal(tid), &[], None, None)));
    }
    peer.send_bytes(&wire);
    while !s.interest().is_empty() {
        s.step(Interest::from_revents(
            poll_fd(s.as_raw_fd(), s.interest().poll_events(), None).unwrap(),
        ));
    }
    let Some(Polled::Block(block)) = s.next_polled() else {
        panic!("the live poll's block comes first");
    };
    let mut rows = ZSetBatch::new(&schema_a());
    decode_wal_block_into(&mut rows, block.block(), &schema_a()).unwrap();
    assert_eq!(rows, batch_a(&[8]));
    assert!(matches!(s.next_polled(), Some(Polled::End(Ok(_)))));
    assert!(s.polled_out());
}

/// A RESOLVE reply that does not decode ends the session, whichever of its
/// sections is the malformed one.
#[test]
fn a_resolve_reply_that_does_not_decode_ends_the_session() {
    let schema = schema_a().to_block();
    let named = ControlHeader { target_id: 7, ..Default::default() };
    for (what, frame) in [
        ("no schema block", encode_frame(named, &[], None, None)),
        ("a truncated descriptor", encode_frame(named, &[], Some(&schema), None)),
        (
            "a truncated schema block",
            encode_frame(named, &[], Some(&schema[..2]), None),
        ),
    ] {
        let (mut s, peer) = pair();
        let sent = s.submit_resolve("s.t").unwrap();
        s.step(Interest::WRITE);
        peer.recv();
        peer.send(&frame);
        let e = await_reply(&mut s, sent).expect_err(what);
        assert!(
            matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
            "{what}: {e:?}"
        );
        assert!(s.is_closed(), "{what}");
    }
}
