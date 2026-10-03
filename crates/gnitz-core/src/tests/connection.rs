//! The spine under bytes only a test can arrange: a scripted peer on the far
//! end of a socketpair feeds reply frames one `step` at a time.

use super::*;
use crate::client::await_slot;
use crate::protocol::transport::poll_fd;
use crate::test_support::{framed, kv_rows, kv_schema, reply_ctrl, reply_status, session_pair as pair};
use crate::BatchAppender;
use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TypeCode};

/// Submit a read of every row of `tid`, decoded under `schema`.
fn submit_scan(s: &mut Session, tid: u64, schema: &Arc<Schema>) -> Result<SlotId, ClientError> {
    let spec = ReadSpec::all_rows(ReadBound::None);
    s.submit(Request::ScanSpec {
        target_id: tid,
        spec: &spec,
        reply_schema: schema,
    })
}

/// A request ACKed at target 0, whose reply is [`Reply::Ack`].
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
    let slot = submit_scan(&mut s, 7, &schema).unwrap();
    assert_eq!(s.interest(), Interest::BOTH);
    assert!(s.step(Interest::WRITE).is_empty());
    assert_eq!(s.interest(), Interest::READ);
    peer.recv();

    // Three frames, one per step: nothing completes until the terminal.
    peer.send(&reply_rows(7, &batch_a(&[1, 2]), 0, true));
    assert!(s.step(Interest::READ).is_empty());
    peer.send(&reply_rows(7, &batch_a(&[3]), 0, true));
    assert!(s.step(Interest::READ).is_empty());
    peer.send(&reply_rows(7, &batch_a(&[4, 5]), 0, false));
    let mut done = s.step(Interest::READ);
    assert_eq!(done.len(), 1);
    let (id, reply) = done.pop().unwrap();
    assert_eq!(id, slot);
    let Reply::Scan(r) = reply.unwrap() else { panic!("scan") };
    assert_eq!(r.batch, batch_a(&[1, 2, 3, 4, 5]));
    assert!(Arc::ptr_eq(&r.schema, &schema), "the schema the request named");
    assert!(s.step(Interest::READ).is_empty());
    assert_eq!(s.interest(), Interest::NONE);
}

#[test]
fn scan_multi_decodes_each_train_under_its_own_relation() {
    let (mut s, peer) = pair();
    let (sa, sb) = (schema_a(), schema_b());
    // Each train must decode under the schema paired with its relation — a
    // two-frame train for relation 2 included.
    let slot = s
        .submit(Request::ScanMulti(vec![(1, Arc::clone(&sa)), (2, Arc::clone(&sb))]))
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
    assert!(s.step(Interest::READ).is_empty(), "one of two trains");
    peer.send(&reply_rows(2, &batch_b(&[20]), 0, true));
    peer.send(&reply_rows(2, &batch_b(&[21]), 0, false));
    let Reply::Multi(replies) = await_slot(&mut s, &mut None, slot).unwrap() else {
        panic!("multi")
    };
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
    let slot = submit_scan(&mut s, 7, &sa).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&encode_frame(
        reply_header(7, 0, false),
        &[],
        Some(&sa.to_block()),
        Some(&batch_a(&[5])),
    ));
    let e = await_slot(&mut s, &mut None, slot).expect_err("a schema block on a read is a decode error");
    assert!(
        matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
        "{e:?}"
    );
    assert!(s.is_closed());
}

/// A status frame completes the slot at the head as that status's refusal —
/// whether it replaces a later train of a multi-read, answers a multi-read with
/// no train at all, or answers a commit — and the connection keeps serving.
#[test]
fn a_status_frame_fails_its_slot_alone() {
    let sa = schema_a();
    for (what, req, trains_before, status) in [
        (
            "the second of three trains",
            Request::ScanMulti((1..=3).map(|tid| (tid, Arc::clone(&sa))).collect()),
            1,
            WireStatus::Error,
        ),
        (
            "an empty multi-read",
            Request::ScanMulti(Vec::new()),
            0,
            WireStatus::NotFound,
        ),
        ("a commit", COMMIT, 0, WireStatus::TxnConflict),
    ] {
        let (mut s, peer) = pair();
        let slot = s.submit(req).unwrap();
        s.step(Interest::WRITE);
        peer.recv();
        for tid in 1..=trains_before {
            peer.send(&reply_rows(tid, &batch_a(&[tid]), 0, false));
        }
        peer.send(&reply_status(0, status, "refused"));
        let r = await_slot(&mut s, &mut None, slot);
        assert!(
            matches!(&r, Err(ClientError::Refused(f)) if f.status == status && f.text == "refused"),
            "{what}: {r:?}"
        );
        assert!(!s.is_closed(), "{what}");
        assert_eq!(s.interest(), Interest::NONE, "{what}: nothing left pending");

        let slot = submit_scan(&mut s, 3, &sa).unwrap();
        s.step(Interest::WRITE);
        peer.recv();
        peer.send(&reply_rows(3, &batch_a(&[5]), 42, false));
        let Reply::Scan(r) = await_slot(&mut s, &mut None, slot).unwrap() else {
            panic!("scan")
        };
        assert_eq!(r.lsn, Some(42), "{what}: the next request completes normally");
    }
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
    for (what, req) in [
        ("a DDL family that is no system table", Request::DdlTxn(&ddl)),
        (
            "a push whose batch is not in its schema's layout",
            Request::Push {
                target_id: 4,
                schema: &sa,
                batch: &wrong_layout,
                mode: WireConflictMode::Update,
            },
        ),
        (
            "a push whose frame is past the server's ingress cap",
            Request::Push {
                target_id: 4,
                schema: &sb,
                batch: &oversize,
                mode: WireConflictMode::Update,
            },
        ),
    ] {
        assert!(s.submit(req).is_err(), "{what}");
        assert_eq!(s.requests_sent(), 0, "{what}");
        assert_eq!(s.interest(), Interest::NONE, "{what}");
        assert!(!s.is_closed(), "{what}");
    }
}

#[test]
fn one_read_serves_two_slots_and_leaves_nothing_buffered() {
    let (mut s, peer) = pair();
    let sa = schema_a();
    let s1 = submit_scan(&mut s, 1, &sa).unwrap();
    let s2 = submit_scan(&mut s, 2, &sa).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    peer.recv();
    let empty = ZSetBatch::new(&sa);
    let mut both = framed(&reply_rows(1, &empty, 11, false));
    both.extend(framed(&reply_rows(2, &empty, 22, false)));
    peer.send_bytes(&both);
    let done = s.step(Interest::READ);
    let ids: Vec<SlotId> = done.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![s1, s2], "in request order");
    for (id, r) in done {
        let Reply::Scan(r) = r.unwrap() else { panic!("scan") };
        assert_eq!(r.lsn, Some(if id == s1 { 11 } else { 22 }));
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
    let a = s.submit(COMMIT).unwrap();
    let b = s
        .submit(Request::Push {
            target_id: 5,
            schema: &sa,
            batch: &rows,
            mode: WireConflictMode::Update,
        })
        .unwrap();
    assert!(s.step(Interest::WRITE).is_empty());
    peer.recv();
    peer.recv();
    // The push to 5 is answered by a frame naming 6.
    let mut both = framed(&reply_ctrl(0, 11));
    both.extend(framed(&reply_ctrl(6, 1)));
    peer.send_bytes(&both);
    let done = s.step(Interest::READ);
    assert_eq!(done.len(), 2);
    assert_eq!(done[0].0, a);
    assert!(matches!(done[0].1, Ok(Reply::Ack(11))), "{:?}", done[0].1);
    assert_eq!(done[1].0, b);
    assert!(
        matches!(
            &done[1].1,
            Err(ClientError::ConnectionLost(ProtocolError::DecodeError(_)))
        ),
        "{:?}",
        done[1].1
    );
    assert!(s.is_closed());
    assert_eq!(s.interest(), Interest::NONE);
    assert!(matches!(
        submit_scan(&mut s, 1, &sa),
        Err(ClientError::ConnectionLost(_))
    ));
    assert!(s.step(Interest::READ).is_empty());
}

/// A peer that answered and then went is still readable after the write
/// that finds it gone: the answer is delivered, the unanswered slot fails.
#[test]
fn a_reply_readable_behind_a_failed_flush_is_delivered() {
    let (mut s, peer) = pair();
    let a = s.submit(COMMIT).unwrap();
    assert!(s.step(Interest::WRITE).is_empty());
    peer.recv();
    peer.send(&reply_ctrl(0, 11));
    drop(peer);
    let b = submit_scan(&mut s, 5, &schema_a()).unwrap();
    let done = s.step(Interest::WRITE);
    assert_eq!(done.len(), 2, "{done:?}");
    assert_eq!(done[0].0, a);
    assert!(matches!(done[0].1, Ok(Reply::Ack(11))), "{:?}", done[0].1);
    assert_eq!(done[1].0, b);
    assert!(
        matches!(&done[1].1, Err(ClientError::ConnectionLost(ProtocolError::IoError(_)))),
        "{:?}",
        done[1].1
    );
}

#[test]
fn a_peer_that_closes_surfaces_an_io_error_rather_than_a_park() {
    let (mut s, peer) = pair();
    let slot = submit_scan(&mut s, 5, &schema_a()).unwrap();
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
    let done = s.step(Interest::READ);
    assert_eq!(done.len(), 1, "{done:?}");
    assert_eq!(done[0].0, slot);
    assert!(
        matches!(&done[0].1, Err(ClientError::ConnectionLost(ProtocolError::IoError(_)))),
        "{:?}",
        done[0].1
    );
}

#[test]
fn close_abandons_every_pending_slot_and_refuses_further_work() {
    let (mut s, _peer) = pair();
    let sa = schema_a();
    let a = submit_scan(&mut s, 1, &sa).unwrap();
    let b = submit_scan(&mut s, 2, &sa).unwrap();
    assert_eq!(s.interest(), Interest::BOTH);
    let done = s.close();
    let ids: Vec<SlotId> = done.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![a, b], "in submit order");
    assert!(done.iter().all(|(_, r)| matches!(r, Err(ClientError::Closed))));
    assert_eq!(s.interest(), Interest::NONE);
    assert!(matches!(submit_scan(&mut s, 3, &sa), Err(ClientError::Closed)));
    assert!(s.step(Interest::BOTH).is_empty());
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
        s.submit(Request::Push {
            target_id: 4,
            schema: &sa,
            batch: &b,
            mode: WireConflictMode::Update,
        })
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
    let mut last = SlotId(0);
    for _ in 0..MAX_IN_FLIGHT {
        assert!(!s.at_capacity());
        let id = submit_scan(&mut s, 1, &sa).unwrap();
        assert!(id > last, "monotonic");
        last = id;
    }
    assert!(s.at_capacity());
    assert!(matches!(submit_scan(&mut s, 1, &sa), Err(ClientError::Refused(_))));
}

/// A one-view delta poll answered by a block and then `behind`, all in one
/// read, stepped with a sink that panics on the block. Returns the session the
/// unwind left.
fn poll_whose_sink_panics(behind: &[Vec<u8>]) -> (Session, crate::test_support::Peer) {
    let (mut s, peer) = pair();
    let poll = Encoded::delta_poll(&[txn_frame::DeltaPollItem {
        view_id: 7,
        after_tick: 4,
        reply_layout: schema_a().layout_digest(),
    }]);
    s.enqueue(poll.unwrap()).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    let mut wire = framed(&reply_rows(7, &batch_a(&[1]), 0, true));
    for frame in behind {
        wire.extend(framed(frame));
    }
    peer.send_bytes(&wire);
    let mut sink = |_: SlotId, p: Polled| {
        if matches!(p, Polled::Block(_)) {
            panic!("the sink's own failure")
        }
    };
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        s.step_polling(Interest::READ, Some(&mut sink))
    }));
    assert!(unwound.is_err(), "the sink's panic unwinds out of the step");
    (s, peer)
}

/// A sink that panics on a block finds every frame of that read already fed:
/// the terminal behind the block ended the poll, and the session answers the
/// next request.
#[test]
fn a_panicking_sink_leaves_the_frames_behind_its_block_fed() {
    let (mut s, peer) = poll_whose_sink_panics(&[encode_frame(reply_header(7, 9, false), &[], None, None)]);
    assert!(!s.is_closed());
    assert_eq!(
        s.interest(),
        Interest::NONE,
        "the terminal behind the block ended the poll"
    );
    let slot = s.submit(COMMIT).unwrap();
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&reply_ctrl(0, 11));
    let done = s.step(Interest::READ);
    assert!(
        matches!(&done[..], [(id, Ok(Reply::Ack(11)))] if *id == slot),
        "{done:?}"
    );
}

/// A frame that fails behind the block ends the session before the sink runs,
/// so the unwind leaves it closed rather than half-fed.
#[test]
fn a_panicking_sink_finds_a_failure_behind_its_block_already_recorded() {
    // The poll's position is on view 7; the frame behind the block names 8.
    let (mut s, _peer) = poll_whose_sink_panics(&[reply_ctrl(8, 1)]);
    assert!(s.is_closed());
    assert!(matches!(s.submit(COMMIT), Err(ClientError::ConnectionLost(_))));
}

/// A delta poll that fails whole — refused at target 0, or cut off by the
/// connection — still ends every view it had yet to answer, once each.
#[test]
fn a_poll_that_fails_whole_ends_each_unanswered_view() {
    let item = |view_id| txn_frame::DeltaPollItem {
        view_id,
        after_tick: 4,
        reply_layout: schema_a().layout_digest(),
    };
    for refused in [true, false] {
        let (mut s, peer) = pair();
        let slot = s
            .enqueue(Encoded::delta_poll(&[item(7), item(8), item(9)]).unwrap())
            .unwrap();
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
        let mut ends = Vec::new();
        let mut sink = |id: SlotId, p: Polled| {
            assert_eq!(id, slot);
            match p {
                Polled::End(end) => ends.push(end),
                Polled::Block(_) => panic!("no view sent a block"),
            }
        };
        let mut done = Vec::new();
        while done.is_empty() {
            done = s.step_polling(Interest::READ, Some(&mut sink));
        }
        assert!(matches!(&done[..], [(id, Err(_))] if *id == slot), "{done:?}");
        assert_eq!(s.is_closed(), !refused);
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
        let slot = s.submit(Request::Resolve("s.t")).unwrap();
        s.step(Interest::WRITE);
        peer.recv();
        peer.send(&frame);
        let e = await_slot(&mut s, &mut None, slot).expect_err(what);
        assert!(
            matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
            "{what}: {e:?}"
        );
        assert!(s.is_closed(), "{what}");
    }
}
