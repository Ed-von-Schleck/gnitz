//! The spine under bytes only a test can arrange: a scripted peer on the far
//! end of a socketpair feeds reply frames one `step` at a time.

use super::*;
use crate::protocol::transport::poll_fd;
use crate::test_support::{framed, kv_rows, kv_schema, pushed_marker, reply_ctrl, reply_status, session_pair as pair};
use crate::BatchAppender;
use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TypeCode, WireFlags, WireStatus};

/// Submit a read of every row of `tid`, decoded under `schema`.
fn submit_scan(s: &mut Session, tid: u64, schema: &Arc<Schema>) -> Sent<ScanReply> {
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
    Arc::new(
        Schema::from_parts(
            vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
                ColumnDef::new("f", TypeCode::F64, false),
            ],
            &[0],
        )
        .unwrap(),
    )
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
    let mut sent = submit_scan(&mut s, 7, &schema);
    assert_eq!(s.interest(), Interest { read: true, write: true });
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
    let (all, keys) = (
        ReadSpec::all_rows(ReadBound::None),
        ReadSpec::all_rows(ReadBound::PkSet(gnitz_wire::PkKeys::from_sorted(
            8,
            7u64.to_be_bytes().to_vec(),
        ))),
    );
    let mut sent = s.submit_scan_multi(&[(Target::from(1), &all, &sa), (Target { tid: 2, token: 9 }, &keys, &sb)]);
    s.step(Interest::WRITE);
    let req = peer.recv();
    let ctrl = peek_control_block(&req).unwrap();
    let items = gnitz_wire::txn_frame::decode_scan_multi(&req[ctrl.body]).unwrap();
    let named: Vec<(Target, u64, Vec<u8>)> = items
        .iter()
        .map(|i| (i.target, i.reply_layout, i.spec.to_vec()))
        .collect();
    assert_eq!(
        named,
        [
            (Target::from(1), sa.layout().layout_digest(), all.encode()),
            (Target { tid: 2, token: 9 }, sb.layout().layout_digest(), keys.encode()),
        ],
        "each item names its target and token, its reply layout and its spec"
    );
    // Relation 2's train is two frames.
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
    let sent = submit_scan(&mut s, 7, &sa);
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
        submit: impl FnOnce(&mut Session) -> Sent<T>,
        trains_before: u64,
        status: WireStatus,
    ) {
        let sa = schema_a();
        let (mut s, peer) = pair();
        let sent = submit(&mut s);
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

        let sent = submit_scan(&mut s, 3, &sa);
        s.step(Interest::WRITE);
        peer.recv();
        peer.send(&reply_rows(3, &batch_a(&[5]), 42, false));
        let r = await_reply(&mut s, sent).unwrap();
        assert_eq!(r.lsn, Some(42), "{what}: the next request completes normally");
    }
    let all = ReadSpec::all_rows(ReadBound::None);
    refused(
        "the second of three trains",
        |s| s.submit_scan_multi(&[1, 2, 3].map(|tid| (Target::from(tid), &all, &sa))),
        1,
        WireStatus::Error,
    );
    refused("a commit", |s| s.submit(COMMIT), 0, WireStatus::TxnConflict);
}

/// A request `submit` refuses is answered with the refusal, opens no slot and
/// queues nothing: the reply queue stays aligned with what the server was sent.
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
    assert!(matches!(s.submit_scan_multi(&[]).try_take(), Some(Err(_))));
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
        assert!(matches!(s.submit(req).try_take(), Some(Err(_))), "{what}");
        untouched(&s, what);
    }
}

#[test]
fn one_read_serves_two_slots_and_leaves_nothing_buffered() {
    let (mut s, peer) = pair();
    let sa = schema_a();
    let mut sent = [1, 2].map(|tid| submit_scan(&mut s, tid, &sa));
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
    let mut a = s.submit(COMMIT);
    let push = Request::Push {
        target: 5.into(),
        schema: &sa,
        batch: &rows,
        mode: WireConflictMode::Update,
    };
    let mut b = s.submit(push);
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
        submit_scan(&mut s, 1, &sa).try_take(),
        Some(Err(ClientError::ConnectionLost(_)))
    ));
}

/// A peer that answered and then went is still readable after the write
/// that finds it gone: the answer is delivered, the unanswered slot fails.
#[test]
fn a_reply_readable_behind_a_failed_flush_is_delivered() {
    let (mut s, peer) = pair();
    let mut a = s.submit(COMMIT);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&reply_ctrl(0, 11));
    drop(peer);
    let mut b = submit_scan(&mut s, 5, &schema_a());
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
    let mut sent = submit_scan(&mut s, 5, &schema_a());
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
fn end_abandons_every_pending_slot_and_refuses_further_work() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    let mut owed = [1, 2].map(|tid| submit_scan(&mut s, tid, &sa));
    assert_eq!(s.interest(), Interest { read: true, write: true });
    s.end(ClientError::Closed);
    for sent in &mut owed {
        assert!(matches!(sent.try_take(), Some(Err(ClientError::Closed))));
    }
    assert_eq!(s.interest(), Interest::NONE);
    assert!(matches!(
        submit_scan(&mut s, 3, &sa).try_take(),
        Some(Err(ClientError::Closed))
    ));
}

/// The in-flight count bounds no memory on its own, so the byte cap is what
/// stops a driver that submits without ever flushing.
#[test]
fn queued_bytes_tracks_the_write_cursor_and_caps_submission() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    // Large enough that a few dozen of them reach the cap.
    let b = batch_a(&(0..32_768).collect::<Vec<_>>());
    let push = |s: &mut Session| {
        let push = Request::Push {
            target: 4.into(),
            schema: &sa,
            batch: &b,
            mode: WireConflictMode::Update,
        };
        s.submit(push).try_take()
    };
    // An encoded push is a full copy of its batch, so the counter grows with
    // what was pushed, not with how many times.
    assert!(push(&mut s).is_none());
    let one = s.queued_bytes();
    assert!(one > b.len() * 32, "the frame carries the batch");
    assert!(push(&mut s).is_none());
    assert_eq!(s.queued_bytes(), 2 * one, "bytes, not submits");

    // The cap is checked before queueing, so the push that crosses it goes
    // through and it is the *next* submit that is refused.
    while !s.at_capacity() {
        assert!(push(&mut s).is_none());
    }
    let queued = s.queued_bytes();
    assert!(
        queued - one < MAX_QUEUED_BYTES,
        "the last push was admitted under the cap"
    );
    let sent = s.requests_sent();
    assert!(matches!(push(&mut s), Some(Err(ClientError::Refused(_)))));
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
        assert!(submit_scan(&mut s, 1, &sa).try_take().is_none());
    }
    assert!(s.at_capacity());
    assert!(matches!(
        submit_scan(&mut s, 1, &sa).try_take(),
        Some(Err(ClientError::Refused(_)))
    ));
}

/// A DELTA_POLL item: `view` after `(tag 1, tick 4)`, replied as `schema_a`.
fn delta_item(view: u64) -> txn_frame::DeltaPollItem<'static> {
    txn_frame::DeltaPollItem {
        view: view.into(),
        from: DeltaCursor::from_pair(1, 4),
        reply_layout: schema_a().layout().layout_digest(),
        spec: &[],
    }
}

/// `view`'s terminal of a delta poll, carrying cursor `(tag 1, tick)`.
fn delta_terminal(view: u64, tick: u64) -> Vec<u8> {
    let hdr = ControlHeader {
        target_id: view,
        arg0: tick,
        arg1: 1,
        ..Default::default()
    };
    encode_frame(hdr, &[], None, None)
}

/// A delta poll that fails whole — refused at target 0, or cut off by the
/// connection — still ends every view it had yet to answer, once each.
#[test]
fn a_poll_that_fails_whole_ends_each_unanswered_view() {
    for refused in [true, false] {
        let (mut s, peer) = pair();
        s.submit_delta_poll(1, &[delta_item(7), delta_item(8), delta_item(9)]);
        s.step(Interest::WRITE);
        peer.recv();
        // View 7 is answered; the failure finds 8 and 9 open.
        let mut wire = framed(&delta_terminal(7, 9));
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
    let (mut s, peer) = pair();
    s.submit_delta_poll(1, &[delta_item(7)]);
    s.abandon_poll();
    s.submit_delta_poll(2, &[delta_item(8)]);
    s.step(Interest::WRITE);
    let mut wire = Vec::new();
    for tid in [7, 8] {
        wire.extend(framed(&reply_rows(tid, &batch_a(&[tid]), 0, true)));
        wire.extend(framed(&delta_terminal(tid, 9)));
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

/// A read of view 7's delta feed after `(tag 1, tick 4)`.
fn submit_delta_read(s: &mut Session, schema: &Arc<Schema>) -> Sent<(ScanReply, DeltaCursor)> {
    s.submit_delta_read(7.into(), DeltaCursor::from_pair(1, 4), schema, &[])
}

#[test]
fn a_delta_read_yields_its_trains_rows_and_the_terminals_cursor() {
    let (mut s, peer) = pair();
    let schema = schema_a();
    let sent = submit_delta_read(&mut s, &schema);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&reply_rows(7, &batch_a(&[1, 2]), 0, true));
    peer.send(&reply_rows(7, &batch_a(&[3]), 0, true));
    peer.send(&delta_terminal(7, 9));
    let (reply, cursor) = await_reply(&mut s, sent).unwrap();
    assert_eq!(reply.batch, batch_a(&[1, 2, 3]));
    assert_eq!(reply.lsn, None);
    assert!(Arc::ptr_eq(&reply.schema, &schema));
    assert_eq!(cursor.pair(), (1, 9));
    assert_eq!(s.interest(), Interest::NONE);
}

#[test]
fn a_fault_naming_a_delta_reads_view_refuses_it_alone() {
    let (mut s, peer) = pair();
    let sent = submit_delta_read(&mut s, &schema_a());
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&reply_status(7, WireStatus::Error, "refused"));
    let r = await_reply(&mut s, sent);
    assert!(
        matches!(&r, Err(ClientError::Refused(f)) if f.text == "refused"),
        "{r:?}"
    );
    assert!(!s.is_closed());
    assert_eq!(s.interest(), Interest::NONE);
}

#[test]
fn a_delta_read_terminal_at_round_0_ends_the_session() {
    let (mut s, peer) = pair();
    let sent = submit_delta_read(&mut s, &schema_a());
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&delta_terminal(7, 0));
    let e = await_reply(&mut s, sent).expect_err("round 0 continues nothing");
    assert!(
        matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
        "{e:?}"
    );
    assert!(s.is_closed());
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
        let sent = s.submit_resolve("s.t");
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

// ── Pushed trains ────────────────────────────────────────────────────────────

/// One pushed train of subscription `sub`: its opening frame, one block of
/// `rows` and the terminal at `(tag 1, round)`.
fn pushed_train(view: u64, sub: u64, rows: &[u64], round: u64) -> Vec<u8> {
    let mut out = framed(&pushed_marker(view, sub));
    out.extend(framed(&reply_rows(view, &batch_a(rows), 0, true)));
    out.extend(framed(&delta_terminal(view, round)));
    out
}

/// The cursor `(tag 1, tick)`.
fn cursor(tick: u64) -> DeltaCursor {
    DeltaCursor::from_pair(1, tick).unwrap()
}

/// Subscribe to view 40 from round 3 as subscription 5; that id. The request
/// is a delta read, answered by view 40's train.
fn subscribe(s: &mut Session) -> u64 {
    s.subscribe(5, 40.into(), cursor(3), &schema_a(), &[]).unwrap();
    5
}

/// A pushed train between two replies is set aside whole for the subscription
/// it is of, and each reply still reaches the request it answers. A train of a
/// subscription the session does not hold is dropped.
#[test]
fn a_pushed_train_between_replies_is_set_aside() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s);
    let first = s.submit(COMMIT);
    let second = s.submit(COMMIT);
    let mut bytes = framed(&delta_terminal(40, 3));
    bytes.extend(framed(&reply_ctrl(0, 1)));
    bytes.extend(pushed_train(40, sub, &[1, 2], 9));
    bytes.extend(pushed_train(40, sub + 1, &[3], 9));
    bytes.extend(pushed_train(40, sub, &[4], 11));
    bytes.extend(framed(&reply_ctrl(0, 2)));
    peer.send_bytes(&bytes);
    assert_eq!(await_reply(&mut s, first).unwrap(), 1);
    assert_eq!(await_reply(&mut s, second).unwrap(), 2);
    let (blocks, next) = s.take_pushed(sub).unwrap();
    assert_eq!((blocks.len(), next), (2, cursor(11)), "its trains as one, in order");
    let (blocks, next) = s.take_pushed(sub).unwrap();
    assert_eq!((blocks.len(), next), (0, cursor(11)), "handed out once");
    assert!(s.take_pushed(sub + 1).is_err(), "nobody holds it");
}

/// What a subscription's own read brings is its first train, taken as a
/// pushed one is.
#[test]
fn a_subscriptions_read_is_its_first_train() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s);
    assert_eq!(
        s.take_pushed(sub).unwrap().1,
        cursor(3),
        "unanswered, it stands where it asked from"
    );
    let sent = s.submit(COMMIT);
    let mut bytes = framed(&reply_rows(40, &batch_a(&[1, 2]), 0, true));
    bytes.extend(framed(&delta_terminal(40, 9)));
    bytes.extend(pushed_train(40, sub, &[3], 11));
    bytes.extend(framed(&reply_ctrl(0, 1)));
    peer.send_bytes(&bytes);
    await_reply(&mut s, sent).unwrap();
    let (blocks, next) = s.take_pushed(sub).unwrap();
    assert_eq!(
        (blocks.len(), next),
        (2, cursor(11)),
        "the read's rows, then what was pushed"
    );
}

/// A kept poll whose reader went sends nothing more. A subscription it was
/// answered with stays the session's until a sync fails to name it, which is
/// what tells the server; one answered afterwards is never held.
#[test]
fn an_abandoned_kept_poll_is_ended_by_the_next_sync() {
    let (mut s, peer) = pair();
    let first = 3;
    s.submit_delta_poll(first, &[delta_item(7), delta_item(8)]);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&delta_terminal(7, 9));
    s.step(Interest::READ);
    s.abandon_poll();
    assert_eq!(s.requests_sent(), 1, "the poll alone");
    let mut bytes = framed(&delta_terminal(8, 9));
    bytes.extend(pushed_train(8, first + 1, &[1], 11));
    peer.send_bytes(&bytes);
    while !s.interest().is_empty() {
        s.step(Interest::from_revents(
            poll_fd(s.as_raw_fd(), s.interest().poll_events(), None).unwrap(),
        ));
    }
    assert!(!s.is_closed());
    assert!(s.take_pushed(first).is_ok(), "answered before its reader went");
    let gone = s.take_pushed(first + 1).expect_err("answered to nobody");
    assert!(gone.to_string().contains("not held"), "{gone:?}");

    let synced = s.submit_sync(&[], Duration::from_secs(60));
    s.step(Interest::WRITE);
    assert_eq!(held_named(&peer.recv()), [0u64; 0], "no owner named either");
    peer.send(&reply_ctrl(0, 0));
    await_reply(&mut s, synced).unwrap();
    let gone = s.take_pushed(first).expect_err("nobody named it");
    assert!(gone.to_string().contains("not held"), "{gone:?}");
    assert!(s.polled_out() && !s.polls.untaken());
}

/// The ids a SYNC_PUSHED request names.
fn held_named(request: &[u8]) -> Vec<u64> {
    let ctrl = peek_control_block(request).unwrap();
    assert_eq!(ctrl.hdr.flags.verb, gnitz_wire::ClientVerb::SyncPushed);
    let mut held = txn_frame::decode_held(&request[ctrl.blob]).unwrap();
    held.sort_unstable();
    held
}

#[test]
fn a_sync_of_no_subscription_is_no_request() {
    let (mut s, _peer) = pair();
    let synced = s.submit_sync(&[], Duration::from_secs(60));
    assert_eq!(s.requests_sent(), 0);
    await_reply(&mut s, synced).unwrap();
}

/// A sync names the subscriptions its caller holds, and one left out is the
/// session's no more. The server is told of the last one let go of by a sync
/// naming none, after which a sync is no request again.
#[test]
fn a_sync_names_what_is_held_and_ends_the_rest() {
    let (mut s, peer) = pair();
    for id in [5, 6] {
        s.subscribe(id, 40.into(), cursor(3), &schema_a(), &[]).unwrap();
    }
    let mut script = Vec::new();
    for _ in 0..2 {
        script.extend(framed(&delta_terminal(40, 3)));
    }
    for (held, named) in [(&[5, 6][..], &[5, 6][..]), (&[6, 9], &[6]), (&[], &[])] {
        let synced = s.submit_sync(held, Duration::ZERO);
        s.step(Interest::WRITE);
        let request = loop {
            let request = peer.recv();
            if peek_control_block(&request).unwrap().hdr.flags.verb == gnitz_wire::ClientVerb::SyncPushed {
                break request;
            }
        };
        assert_eq!(held_named(&request), named, "holding {held:?}");
        script.extend(framed(&reply_ctrl(0, 0)));
        peer.send_bytes(&std::mem::take(&mut script));
        await_reply(&mut s, synced).unwrap();
    }
    assert!(s.take_pushed(5).is_err() && s.take_pushed(6).is_err());
    let sent = s.requests_sent();
    let synced = s.submit_sync(&[], Duration::ZERO);
    await_reply(&mut s, synced).unwrap();
    assert_eq!(s.requests_sent(), sent, "the server holds none");
}

#[test]
fn a_refused_sync_moves_no_subscription() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s);
    let synced = s.submit_sync(&[sub], Duration::ZERO);
    let mut bytes = framed(&delta_terminal(40, 3));
    bytes.extend(framed(&reply_status(0, WireStatus::Error, "refused")));
    peer.send_bytes(&bytes);
    assert!(matches!(await_reply(&mut s, synced), Err(ClientError::Refused(_))));
    assert_eq!(s.take_pushed(sub).unwrap().1, cursor(3));
}

/// A subscription a session at its cap refuses is no subscription: the
/// refusal is returned, and the session holds nothing under the id.
#[test]
fn a_refused_subscribe_holds_nothing() {
    let (mut s, _peer) = pair();
    s.end(ClientError::Closed);
    let refused = s.subscribe(5, 40.into(), cursor(3), &schema_a(), &[]);
    assert!(matches!(refused, Err(ClientError::Closed)), "{refused:?}");
    let gone = s.take_pushed(5).expect_err("nothing is held");
    assert!(gone.to_string().contains("not held"), "{gone:?}");
}

/// A train that ends in a fault ends its subscription and nothing else: the
/// request behind it is answered.
#[test]
fn a_pushed_fault_ends_its_subscription_and_no_request() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s);
    let sent = s.submit(COMMIT);
    let mut bytes = framed(&delta_terminal(40, 3));
    bytes.extend(pushed_train(40, sub, &[1], 9));
    bytes.extend(framed(&pushed_marker(40, sub)));
    bytes.extend(framed(&reply_status(40, WireStatus::Error, "lagged")));
    bytes.extend(framed(&reply_ctrl(0, 3)));
    peer.send_bytes(&bytes);
    assert_eq!(await_reply(&mut s, sent).unwrap(), 3);
    let ended = s.take_pushed(sub);
    assert!(matches!(ended, Err(ClientError::Refused(_))), "the fault is its end");
    let gone = s.take_pushed(sub).expect_err("the session holds it no more");
    assert!(gone.to_string().contains("not held"), "{gone:?}");
}

/// A subscription whose read the server refuses — the request whole, or its
/// view alone — is ended by the refusal, as by a fault train.
#[test]
fn a_refused_subscription_is_ended_by_its_refusal() {
    for named in [0, 40] {
        let (mut s, peer) = pair();
        let sub = subscribe(&mut s);
        let sent = s.submit(COMMIT);
        let mut bytes = framed(&reply_status(named, WireStatus::DeltaExpired, "refused"));
        bytes.extend(framed(&reply_ctrl(0, 3)));
        peer.send_bytes(&bytes);
        assert_eq!(await_reply(&mut s, sent).unwrap(), 3);
        let ended = s.take_pushed(sub);
        assert!(
            matches!(&ended, Err(ClientError::Refused(f)) if f.status == WireStatus::DeltaExpired),
            "naming {named}: {ended:?}"
        );
    }
}

/// A frame of a pushed train naming another relation than the train opened
/// for ends the session, as a misdirected reply frame does.
#[test]
fn a_pushed_train_naming_another_view_ends_the_session() {
    let (mut s, peer) = pair();
    let sent = s.submit(COMMIT);
    let mut bytes = framed(&pushed_marker(40, 5));
    bytes.extend(framed(&delta_terminal(41, 9)));
    peer.send_bytes(&bytes);
    assert!(matches!(await_reply(&mut s, sent), Err(ClientError::ConnectionLost(_))));
    assert!(s.is_closed());
}
