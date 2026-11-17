//! The spine under bytes only a test can arrange: a scripted peer on the far
//! end of a socketpair feeds reply frames one `step` at a time.

use super::*;
use crate::test_support::{
    Peer, await_reply, decode_wal_block, delta_frame, delta_terminal, encode_frame, kv_rows, kv_schema, pushed_marker,
    pushed_train, reply_ctrl, reply_rows, reply_status, session_pair as pair, step_until_idle, sub_id, sync_answer,
    sync_header, sync_held,
};
use crate::transport::poll_fd;
use crate::{BatchAppender, PkColumn};
use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TypeCode, WireStatus, framed};

/// Submit a read of every row of `tid`, decoded under `schema`.
fn submit_scan(s: &mut Session, tid: u64, schema: &Arc<Schema>) -> Sent<ScanReply> {
    s.submit_scan(tid.into(), &ReadSpec::all_rows(ReadBound::None), schema)
}

/// A request ACKed at target 0.
const COMMIT: Request<'static> = Request::PushTxn(&[]);

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
    let items = gnitz_wire::multi_frame::decode_scan_multi(&req[ctrl.body]).unwrap();
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
    let ddl = [(gnitz_wire::SysFamily::Schema, ZSetBatch::new(&sa))];
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
        ("a DDL batch not in its family's layout", Request::DdlTxn(&ddl)),
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
    let ready = poll_fd(
        s.as_fd(),
        Interest::READ,
        Some(Instant::now() + std::time::Duration::from_secs(5)),
    )
    .expect("poll");
    assert!(ready.read, "a hangup wakes a read park");
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
    let one = s.transport.queued_bytes();
    assert!(one > b.len() * 32, "the frame carries the batch");
    assert!(push(&mut s).is_none());
    assert_eq!(s.transport.queued_bytes(), 2 * one, "bytes, not submits");

    // The cap is checked before queueing, so the push that crosses it goes
    // through and it is the *next* submit that is refused.
    while !s.at_capacity() {
        assert!(push(&mut s).is_none());
    }
    let queued = s.transport.queued_bytes();
    assert!(
        queued - one < MAX_QUEUED_BYTES,
        "the last push was admitted under the cap"
    );
    let sent = s.requests_sent();
    assert!(matches!(push(&mut s), Some(Err(ClientError::Refused(_)))));
    assert_eq!(
        (s.transport.queued_bytes(), s.requests_sent()),
        (queued, sent),
        "the refusal queued nothing"
    );
    assert!(s.interest().write, "nothing was flushed, so it is all still queued");

    // Writing gives the budget back — the counter tracks the write cursor,
    // not just the pushes.
    s.step(Interest::WRITE);
    assert!(s.transport.queued_bytes() < queued, "a flush releases what it wrote");
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
fn delta_item(view: u64) -> multi_frame::DeltaPollItem<'static> {
    multi_frame::DeltaPollItem {
        view: view.into(),
        from: DeltaCursor::from_pair(1, 4),
        reply_layout: schema_a().layout().layout_digest(),
        spec: &[],
    }
}

/// A delta poll that fails whole — refused at target 0, or cut off by the
/// connection — still ends every view it had yet to answer, once each.
#[test]
fn a_poll_that_fails_whole_ends_each_unanswered_view() {
    for refused in [true, false] {
        let (mut s, peer) = pair();
        s.submit_delta_poll(sub_id(1), &[delta_item(7), delta_item(8), delta_item(9)]);
        s.step(Interest::WRITE);
        peer.recv();
        // View 7 is answered; the failure finds 8 and 9 open.
        let mut wire = framed(&delta_terminal(7, 1, 9));
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
        assert!(matches!(s.take_pushed(sub_id(1)), Ok((b, _)) if b.is_empty()));
        for i in (1..3).map(|i| kept_as(sub_id(1), i)) {
            assert!(s.take_reading(i).is_none(), "view {i} ended");
            match s.take_pushed(i) {
                Err(ClientError::Refused(f)) => assert!(refused && f.text == "refused"),
                Err(ClientError::ConnectionLost(_)) => assert!(!refused),
                other => panic!("{other:?}"),
            }
            let gone = s.take_pushed(i).expect_err("ended once");
            assert!(gone.to_string().contains("not held"), "{gone:?}");
        }
    }
}

/// A poll whose ids are released parks nothing: its trains are read off the
/// socket and dropped, and only the next poll's are parked.
#[test]
fn a_released_poll_parks_nothing() {
    let (mut s, peer) = pair();
    s.submit_delta_poll(sub_id(1), &[delta_item(7)]);
    s.release(sub_id(1));
    s.submit_delta_poll(sub_id(2), &[delta_item(8)]);
    s.step(Interest::WRITE);
    let mut wire = Vec::new();
    for tid in [7, 8] {
        wire.extend(framed(&delta_frame(tid, Some(&batch_a(&[tid])), None)));
        wire.extend(framed(&delta_terminal(tid, 1, 9)));
    }
    peer.send_bytes(&wire);
    step_until_idle(&mut s);
    assert!(
        s.take_reading(sub_id(1)).is_none(),
        "nothing parked for the released poll"
    );
    assert!(
        s.take_pushed(sub_id(1)).is_err(),
        "nothing parked for the released poll"
    );
    let (blocks, _) = s.take_pushed(sub_id(2)).expect("the next poll's train is held");
    assert_eq!(blocks.len(), 1);
    assert_eq!(decode_wal_block(blocks[0].block(), &schema_a()).unwrap(), batch_a(&[8]));
    assert!(!s.inbox.polls.untaken());
}

/// A train pushed in the same read as a poll's terminal joins the train the
/// item is held with: its blocks behind the poll's, and its cursor.
#[test]
fn a_train_pushed_behind_a_polls_terminal_joins_it() {
    let (mut s, peer) = pair();
    s.submit_delta_poll(sub_id(3), &[delta_item(7)]);
    s.step(Interest::WRITE);
    peer.recv();
    let mut wire = framed(&delta_frame(7, Some(&batch_a(&[1])), None));
    wire.extend(framed(&delta_terminal(7, 1, 9)));
    wire.extend(pushed_train(7, sub_id(3), &batch_a(&[2]), Ok((1, 11))));
    peer.send_bytes(&wire);
    step_until_idle(&mut s);
    let (blocks, at) = s.take_pushed(sub_id(3)).expect("the item is held");
    let rows: Vec<ZSetBatch> = blocks
        .iter()
        .map(|b| decode_wal_block(b.block(), &schema_a()).unwrap())
        .collect();
    assert_eq!((rows, at), (vec![batch_a(&[1]), batch_a(&[2])], cursor(11)));
}

/// A train in progress hands out its blocks so far, and once its id is
/// released the rest of it and what is pushed for the id are dropped.
#[test]
fn a_poll_released_mid_train_drops_the_rest() {
    let (mut s, peer) = pair();
    s.submit_delta_poll(sub_id(3), &[delta_item(7)]);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&delta_frame(7, Some(&batch_a(&[1])), None));
    s.step(Interest::READ);
    assert_eq!(s.take_reading(sub_id(3)).map(|b| b.len()), Some(1), "its block so far");
    s.release(sub_id(3));
    let after = s.submit(COMMIT);
    let mut wire = framed(&delta_frame(7, Some(&batch_a(&[2])), None));
    wire.extend(framed(&delta_terminal(7, 1, 9)));
    wire.extend(pushed_train(7, sub_id(3), &batch_a(&[3]), Ok((1, 11))));
    wire.extend(framed(&reply_ctrl(0, 1)));
    peer.send_bytes(&wire);
    assert_eq!(await_reply(&mut s, after).unwrap(), 1);
    assert!(!s.is_closed());
    assert!(s.take_reading(sub_id(3)).is_none() && s.take_pushed(sub_id(3)).is_err());
    assert!(!s.inbox.polls.untaken(), "nothing parked");
}

/// A read of view 7's delta feed after `(tag 1, tick 4)`.
fn submit_delta_read(s: &mut Session, schema: &Arc<Schema>) -> Sent<(ScanReply, DeltaCursor)> {
    s.submit_delta_read(7.into(), DeltaCursor::from_pair(1, 4), schema, &[], None)
}

#[test]
fn a_delta_read_yields_its_trains_rows_and_the_terminals_cursor() {
    let (mut s, peer) = pair();
    let schema = schema_a();
    let sent = submit_delta_read(&mut s, &schema);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&delta_frame(7, Some(&batch_a(&[1, 2])), None));
    peer.send(&delta_frame(7, Some(&batch_a(&[3])), None));
    peer.send(&delta_terminal(7, 1, 9));
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
    peer.send(&delta_terminal(7, 1, 0));
    let e = await_reply(&mut s, sent).expect_err("round 0 continues nothing");
    assert!(
        matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
        "{e:?}"
    );
    assert!(s.is_closed());
}

/// A RESOLVE reply that does not decode ends the session, whichever part of
/// its descriptor is the malformed one.
#[test]
fn a_resolve_reply_that_does_not_decode_ends_the_session() {
    let schema = schema_a();
    let record = schema.to_record();
    let desc = |schema: &[u8]| {
        RelDescriptorBlob {
            schema: schema.into(),
            ..Default::default()
        }
        .encode()
    };
    let whole = desc(record);
    let named = ControlHeader { target_id: 7, ..Default::default() };
    for (what, frame) in [
        ("no descriptor", encode_frame(named, &[], None)),
        (
            "a truncated descriptor",
            encode_frame(named, &whole[..whole.len() - 1], None),
        ),
        (
            "a truncated schema record",
            encode_frame(named, &desc(&record[..2]), None),
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

/// A snapshot page's reply names each worker train it carries by its last
/// frame; a page naming none ends the session, and one naming a worker is
/// answered with where that worker's rows end.
#[test]
fn a_snapshot_page_naming_no_worker_ends_the_session() {
    let named = |scan_last| ControlHeader {
        target_id: 7,
        flags: gnitz_wire::WireFlags { scan_last, ..Default::default() },
        ..Default::default()
    };
    let (mut s, peer) = pair();
    let sent = s.submit_snapshot_page(1, 7, &[], 10, &schema_a());
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&encode_frame(named(true), &[], None));
    assert_eq!(await_reply(&mut s, sent).unwrap().worker_ends, [0]);

    let sent = s.submit_snapshot_page(1, 7, &[], 10, &schema_a());
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&encode_frame(named(false), &[], None));
    let e = await_reply(&mut s, sent).expect_err("a page naming no worker");
    assert!(
        matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
        "{e:?}"
    );
    assert!(s.is_closed());
}

/// A page's rows arrive as several workers' trains of continuation frames; the
/// batch holds them in arrival order with their weights and strings, and each
/// worker's end is where its rows stop in that batch.
#[test]
fn a_multi_frame_page_decodes_in_arrival_order() {
    let schema = schema_b();
    let rows = |rows: &[(u64, i64)]| {
        let mut b = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut b);
        for &(pk, w) in rows {
            app.add_row(pk as u128, w)
                .str_val(&format!("a string longer than inline {pk}"))
                .f64_val(pk as f64);
        }
        b
    };
    let frame = |batch: &ZSetBatch, continuation, scan_last| {
        let hdr = ControlHeader {
            target_id: 7,
            flags: gnitz_wire::WireFlags {
                continuation,
                scan_last,
                ..Default::default()
            },
            ..Default::default()
        };
        encode_frame(hdr, &[], Some(batch))
    };
    let (mut s, peer) = pair();
    let mut sent = s.submit_snapshot_page(1, 7, &[], 10, &schema);
    s.step(Interest::WRITE);
    peer.recv();
    for f in [
        frame(&rows(&[(1, 1), (2, 3)]), true, true),
        frame(&rows(&[(3, -1)]), true, false),
        frame(&rows(&[(4, 2), (5, 1)]), true, true),
        frame(&rows(&[(6, 7)]), false, true),
    ] {
        assert!(sent.try_take().is_none());
        peer.send(&f);
        s.step(Interest::READ);
    }
    let page = sent.try_take().unwrap().unwrap();
    assert_eq!(page.batch, rows(&[(1, 1), (2, 3), (3, -1), (4, 2), (5, 1), (6, 7)]));
    assert_eq!(page.worker_ends, [2, 5, 6]);

    // A terminal that carries no rows decodes the held frames.
    let mut sent = submit_scan(&mut s, 7, &schema);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&frame(&rows(&[(1, 2)]), true, false));
    peer.send(&frame(&rows(&[(2, 1)]), true, false));
    s.step(Interest::READ);
    assert!(sent.try_take().is_none());
    peer.send(&reply_ctrl(7, 0));
    assert_eq!(await_reply(&mut s, sent).unwrap().batch, rows(&[(1, 2), (2, 1)]));
}

/// A continuation frame whose data block header does not parse ends the
/// session on that frame, before the train's terminal.
#[test]
fn a_malformed_block_mid_train_ends_the_session_at_once() {
    let (mut s, peer) = pair();
    let schema = schema_a();
    let sent = submit_scan(&mut s, 7, &schema);
    s.step(Interest::WRITE);
    peer.recv();
    let mut bad = reply_rows(7, &batch_a(&[2]), 0, true);
    let data = peek_control_block(&bad).unwrap().data.unwrap();
    // The header's second word, the row count.
    bad[data.start + 4] ^= 0xff;
    peer.send(&reply_rows(7, &batch_a(&[1]), 0, true));
    peer.send(&bad);
    s.step(Interest::READ);
    assert!(s.is_closed());
    let e = await_reply(&mut s, sent).expect_err("a malformed block");
    assert!(
        matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(m)) if m.contains("too short for its rows")),
        "{e:?}"
    );
}

// ── Pushed trains ────────────────────────────────────────────────────────────

/// The cursor `(tag 1, tick)`.
fn cursor(tick: u64) -> DeltaCursor {
    DeltaCursor::from_pair(1, tick).unwrap()
}

/// A read of view 40 that keeps it as subscription `id`.
fn submit_kept_read(s: &mut Session, id: SubId) -> Sent<(ScanReply, DeltaCursor)> {
    s.submit_delta_read(40.into(), None, &schema_a(), &[], Some(id))
}

/// Subscribe to view 40 as subscription `id`: a kept read, answered here with
/// no row and round 3.
fn subscribe_as(s: &mut Session, peer: &Peer, id: SubId) {
    let sent = submit_kept_read(s, id);
    peer.send(&delta_terminal(40, 1, 3));
    assert_eq!(await_reply(s, sent).unwrap().1, cursor(3));
}

/// [`subscribe_as`] subscription 5; that id.
fn subscribe(s: &mut Session, peer: &Peer) -> SubId {
    subscribe_as(s, peer, sub_id(5));
    sub_id(5)
}

/// A pushed train between two replies is set aside whole for the subscription
/// it is of, and each reply still reaches the request it answers. A train of a
/// subscription the session does not hold is dropped.
#[test]
fn a_pushed_train_between_replies_is_set_aside() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let first = s.submit(COMMIT);
    let second = s.submit(COMMIT);
    let mut bytes = framed(&reply_ctrl(0, 1));
    bytes.extend(pushed_train(40, sub, &batch_a(&[1, 2]), Ok((1, 9))));
    bytes.extend(pushed_train(40, kept_as(sub, 1), &batch_a(&[3]), Ok((1, 9))));
    bytes.extend(pushed_train(40, sub, &batch_a(&[4]), Ok((1, 11))));
    bytes.extend(framed(&reply_ctrl(0, 2)));
    peer.send_bytes(&bytes);
    assert_eq!(await_reply(&mut s, first).unwrap(), 1);
    assert_eq!(await_reply(&mut s, second).unwrap(), 2);
    let (blocks, next) = s.take_pushed(sub).unwrap();
    assert_eq!((blocks.len(), next), (2, cursor(11)), "its trains as one, in order");
    let (blocks, next) = s.take_pushed(sub).unwrap();
    assert_eq!((blocks.len(), next), (0, cursor(11)), "handed out once");
    assert!(s.take_pushed(kept_as(sub, 1)).is_err(), "nobody holds it");
}

/// A released subscription parks nothing pushed for it afterwards, so the next
/// sync still waits.
#[test]
fn a_released_subscription_parks_no_pushed_train() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    s.release(sub);
    let after = s.submit(COMMIT);
    let mut bytes = pushed_train(40, sub, &batch_a(&[1, 2]), Ok((1, 9)));
    bytes.extend(framed(&reply_ctrl(0, 1)));
    peer.send_bytes(&bytes);
    assert_eq!(await_reply(&mut s, after).unwrap(), 1);
    assert!(!s.inbox.polls.untaken(), "nothing parked");
    assert!(s.take_pushed(sub).is_err(), "nobody holds it");
    let _synced = s.submit_sync(&[], Duration::from_secs(60));
    s.step(Interest::WRITE);
    let (_read, _commit, request) = (peer.recv(), peer.recv(), peer.recv());
    let hdr = peek_control_block(&request).unwrap().hdr;
    assert_eq!(hdr.flags.verb, Some(gnitz_wire::ClientVerb::SyncPushed));
    assert_eq!(hdr.arg0, 60_000, "the sync waits");
}

/// A kept read is answered as any delta read is, and its terminal leaves the
/// session holding the subscription at the cursor it carries: a train pushed
/// right behind the reply is the subscription's.
#[test]
fn a_kept_reads_terminal_parks_its_cursor() {
    let (mut s, peer) = pair();
    let sent = submit_kept_read(&mut s, sub_id(5));
    assert!(s.take_pushed(sub_id(5)).is_err(), "unanswered, nothing is held");
    let mut bytes = framed(&delta_frame(40, Some(&batch_a(&[1, 2])), None));
    bytes.extend(framed(&delta_terminal(40, 1, 9)));
    bytes.extend(pushed_train(40, sub_id(5), &batch_a(&[3]), Ok((1, 11))));
    peer.send_bytes(&bytes);
    let (reply, at) = await_reply(&mut s, sent).unwrap();
    assert_eq!((reply.batch, at), (batch_a(&[1, 2]), cursor(9)));
    let synced = s.submit_sync(&[sub_id(5)], Duration::ZERO);
    assert_eq!(s.requests_sent(), 2, "the server holds one, so the sync is a request");
    peer.send(&sync_answer(None));
    await_reply(&mut s, synced).unwrap();
    let (blocks, next) = s.take_pushed(sub_id(5)).unwrap();
    assert_eq!(
        (blocks.len(), next),
        (1, cursor(11)),
        "what was pushed, not the read's rows"
    );
}

/// A kept poll whose reader went sends nothing more. A subscription it was
/// answered with stays the session's until a sync fails to name it, which is
/// what tells the server; one answered afterwards is never held.
#[test]
fn an_abandoned_kept_poll_is_ended_by_the_next_sync() {
    let (mut s, peer) = pair();
    let first = sub_id(3);
    s.submit_delta_poll(first, &[delta_item(7), delta_item(8)]);
    s.step(Interest::WRITE);
    peer.recv();
    peer.send(&delta_terminal(7, 1, 9));
    s.step(Interest::READ);
    // The reader took view 7's train, and went before 8's.
    assert!(s.take_reading(first).is_none() && s.take_pushed(first).is_ok());
    s.release(kept_as(first, 1));
    assert_eq!(s.requests_sent(), 1, "the poll alone");
    let mut bytes = framed(&delta_terminal(8, 1, 9));
    bytes.extend(pushed_train(8, kept_as(first, 1), &batch_a(&[1]), Ok((1, 11))));
    peer.send_bytes(&bytes);
    step_until_idle(&mut s);
    assert!(!s.is_closed());
    assert!(s.take_pushed(first).is_ok(), "answered before its reader went");
    let gone = s.take_pushed(kept_as(first, 1)).expect_err("answered to nobody");
    assert!(gone.to_string().contains("not held"), "{gone:?}");

    let synced = s.submit_sync(&[], Duration::from_secs(60));
    s.step(Interest::WRITE);
    assert_eq!(sync_held(&peer.recv()), [], "no owner named either");
    peer.send(&sync_answer(None));
    await_reply(&mut s, synced).unwrap();
    let gone = s.take_pushed(first).expect_err("nobody named it");
    assert!(gone.to_string().contains("not held"), "{gone:?}");
    assert!(!s.inbox.polls.untaken());
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
        subscribe_as(&mut s, &peer, sub_id(id));
    }
    for (held, named) in [(&[5, 6][..], &[5, 6][..]), (&[6, 9], &[6]), (&[], &[])] {
        let held: Vec<SubId> = held.iter().copied().map(sub_id).collect();
        let named: Vec<SubId> = named.iter().copied().map(sub_id).collect();
        let synced = s.submit_sync(&held, Duration::ZERO);
        s.step(Interest::WRITE);
        let request = loop {
            let request = peer.recv();
            if peek_control_block(&request).unwrap().hdr.flags.verb == Some(gnitz_wire::ClientVerb::SyncPushed) {
                break request;
            }
        };
        assert_eq!(sync_held(&request), named, "holding {held:?}");
        peer.send(&sync_answer(None));
        await_reply(&mut s, synced).unwrap();
    }
    assert!(s.take_pushed(sub_id(5)).is_err() && s.take_pushed(sub_id(6)).is_err());
    let sent = s.requests_sent();
    let synced = s.submit_sync(&[], Duration::ZERO);
    await_reply(&mut s, synced).unwrap();
    assert_eq!(s.requests_sent(), sent, "the server holds none");
}

#[test]
fn a_refused_sync_moves_no_subscription() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let synced = s.submit_sync(&[sub], Duration::ZERO);
    peer.send(&sync_answer(Some("refused")));
    assert!(matches!(await_reply(&mut s, synced), Err(ClientError::Refused(_))));
    assert_eq!(s.take_pushed(sub).unwrap().1, cursor(3));
}

/// A sync holds no place among the replies: the requests submitted behind it
/// are answered while it is unanswered, and a train pushed meanwhile is its
/// subscription's when the answer comes.
#[test]
fn a_request_behind_a_sync_is_answered_before_it() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let mut synced = s.submit_sync(&[sub], Duration::from_secs(60));
    let first = s.submit(COMMIT);
    let second = s.submit(COMMIT);
    let mut bytes = framed(&reply_ctrl(0, 1));
    bytes.extend(pushed_train(40, sub, &batch_a(&[1]), Ok((1, 9))));
    bytes.extend(framed(&reply_ctrl(0, 2)));
    peer.send_bytes(&bytes);
    assert_eq!(await_reply(&mut s, first).unwrap(), 1);
    assert_eq!(await_reply(&mut s, second).unwrap(), 2);
    assert!(synced.try_take().is_none(), "no reply answers a sync");
    assert!(s.interest().read, "the sync is still owed its answer");

    peer.send(&sync_answer(None));
    await_reply(&mut s, synced).unwrap();
    let (blocks, next) = s.take_pushed(sub).unwrap();
    assert_eq!((blocks.len(), next), (1, cursor(9)));
    assert!(s.interest().is_empty());
}

/// Syncs are answered in the order they were submitted, each by one answer.
#[test]
fn syncs_are_answered_in_order() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let held = s.submit_sync(&[sub], Duration::from_secs(60));
    let mut behind = s.submit_sync(&[sub], Duration::ZERO);
    peer.send(&sync_answer(Some("refused")));
    assert!(matches!(await_reply(&mut s, held), Err(ClientError::Refused(_))));
    assert!(behind.try_take().is_none(), "one answer, one sync");
    peer.send(&sync_answer(None));
    await_reply(&mut s, behind).expect("the second answer is the second sync's");
}

/// A sync sent behind an unanswered one asks for no hold, whatever its caller
/// asked for: the one ahead may be answered with trains whose reader is gone.
#[test]
fn a_sync_behind_an_unanswered_one_is_not_held() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    peer.recv();
    let _first = s.submit_sync(&[sub], Duration::from_secs(60));
    let _behind = s.submit_sync(&[sub], Duration::from_secs(60));
    s.step(Interest::WRITE);
    assert_eq!(sync_header(&peer.recv()).arg0, 60_000);
    assert_eq!(sync_header(&peer.recv()).arg0, 0);
}

/// An answer to a sync nobody sent is a frame this session cannot place, and
/// a session that ends fails the syncs it had not been answered.
#[test]
fn a_sync_ends_with_its_session() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let synced = s.submit_sync(&[sub], Duration::from_secs(60));
    let mut bytes = framed(&sync_answer(None));
    bytes.extend(framed(&sync_answer(None)));
    peer.send_bytes(&bytes);
    await_reply(&mut s, synced).expect("its own answer came first");
    while !s.is_closed() {
        s.step(Interest::READ);
    }

    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let synced = s.submit_sync(&[sub], Duration::from_secs(60));
    drop(peer);
    assert!(matches!(
        await_reply(&mut s, synced),
        Err(ClientError::ConnectionLost(_))
    ));
}

/// A train that ends in a fault ends its subscription and nothing else: the
/// request behind it is answered.
#[test]
fn a_pushed_fault_ends_its_subscription_and_no_request() {
    let (mut s, peer) = pair();
    let sub = subscribe(&mut s, &peer);
    let sent = s.submit(COMMIT);
    let mut bytes = pushed_train(40, sub, &batch_a(&[1]), Ok((1, 9)));
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

/// A kept read the server refuses — the request whole, or its view alone —
/// is a refused read and nothing else: the session holds nothing under the
/// id, and has no subscription to sync.
#[test]
fn a_refused_kept_read_parks_nothing() {
    for named in [0, 40] {
        let (mut s, peer) = pair();
        let sent = submit_kept_read(&mut s, sub_id(5));
        peer.send(&reply_status(named, WireStatus::DeltaExpired, "refused"));
        let refused = await_reply(&mut s, sent);
        assert!(
            matches!(&refused, Err(e) if e.status() == Some(WireStatus::DeltaExpired)),
            "naming {named}: {refused:?}"
        );
        let gone = s.take_pushed(sub_id(5)).expect_err("nothing is held");
        assert!(gone.to_string().contains("not held"), "naming {named}: {gone:?}");
        let synced = s.submit_sync(&[sub_id(5)], Duration::ZERO);
        await_reply(&mut s, synced).unwrap();
        assert_eq!(s.requests_sent(), 1, "naming {named}: the read alone");
    }
}

/// A frame of a pushed train naming another relation than the train opened
/// for ends the session, as a misdirected reply frame does.
#[test]
fn a_pushed_train_naming_another_view_ends_the_session() {
    let (mut s, peer) = pair();
    let sent = s.submit(COMMIT);
    let mut bytes = framed(&pushed_marker(40, sub_id(5)));
    bytes.extend(framed(&delta_terminal(41, 1, 9)));
    peer.send_bytes(&bytes);
    assert!(matches!(await_reply(&mut s, sent), Err(ClientError::ConnectionLost(_))));
    assert!(s.is_closed());
}

/// A pushed train opens with a bare head naming its subscription: an opener
/// carrying anything else, or naming none, ends the session.
#[test]
fn a_pushed_train_opener_that_is_not_bare_ends_the_session() {
    let marker = |flags: gnitz_wire::WireFlags, status| ControlHeader {
        target_id: 40,
        arg0: 5,
        status,
        flags: gnitz_wire::WireFlags { lane: WireLane::PushedTrain, ..flags },
        ..Default::default()
    };
    let bare = gnitz_wire::WireFlags::default();
    let continued = gnitz_wire::WireFlags { continuation: true, ..bare };
    let ok = WireStatus::Ok;
    for (what, frame) in [
        (
            "a data block",
            encode_frame(marker(bare, ok), &[], Some(&batch_a(&[1]))),
        ),
        ("a blob", encode_frame(marker(bare, ok), b"why", None)),
        ("a continuation", encode_frame(marker(continued, ok), &[], None)),
        (
            "a fault",
            encode_frame(marker(bare, WireStatus::Error), b"lagged", None),
        ),
        (
            "no subscription",
            encode_frame(ControlHeader { arg0: 0, ..marker(bare, ok) }, &[], None),
        ),
    ] {
        let (mut s, peer) = pair();
        let sent = s.submit(COMMIT);
        peer.send(&frame);
        let e = await_reply(&mut s, sent).expect_err(what);
        assert!(
            matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(_))),
            "{what}: {e:?}"
        );
        assert!(s.is_closed(), "{what}");
    }
}

// ── Request frames ───────────────────────────────────────────────────────────

/// A frame's blob, decoded as a schema record.
fn frame_schema(buf: &[u8], ctrl: &DecodedControl) -> Schema {
    Schema::from_record(&buf[ctrl.blob.clone()]).expect("a schema record decodes")
}

/// A frame's data block, decoded under `schema`.
fn frame_data(buf: &[u8], ctrl: &DecodedControl, schema: &Schema) -> Option<ZSetBatch> {
    ctrl.data
        .clone()
        .map(|r| decode_wal_block(&buf[r], schema).expect("a data block decodes"))
}

/// A multi-family `PUSH_TXN` frame (both modes, a repeated tid, a delete
/// family). The frame layout is `gnitz_wire::multi_frame`'s and is tested there;
/// what this pins is the adapter above it: each family's two blocks are built
/// from *that* family's schema, batch, tid and basis, in that order.
#[test]
fn push_txn_families_carry_their_own_schema_and_batch() {
    let schema = Arc::new(kv_schema(TypeCode::I64));
    let b0 = kv_rows(&[(1, 10, 1), (2, 20, 1)]);
    let b1 = kv_rows(&[(3, 30, 1)]);
    let b2 = ZSetBatch::retraction(&schema, PkColumn::from_natives(&schema, [4]));

    let family = |tid: u64, batch, mode, basis| PushFamily {
        target: tid.into(),
        schema: Arc::clone(&schema),
        batch: Arc::new(batch),
        mode,
        basis,
    };
    let blind = gnitz_wire::multi_frame::BLIND;
    let families = [
        family(16, b0, WireConflictMode::Update, 42),
        family(17, b1, WireConflictMode::Error, blind),
        family(16, b2, WireConflictMode::Update, blind),
    ];
    let mut payload = Vec::new();
    append_push_txn(&mut payload, &families);

    let ctrl = peek_control_block(&payload).unwrap();
    let decoded = gnitz_wire::multi_frame::decode_items(&payload, ctrl.body, ClientVerb::PushTxn).unwrap();
    assert_eq!(decoded.len(), families.len());
    for (fam, want) in decoded.iter().zip(&families) {
        let got = (fam.hdr.target_id, fam.hdr.flags.conflict_mode, fam.hdr.arg0);
        assert_eq!(got, (want.target.tid, want.mode, want.basis));
        let block_schema = frame_schema(&payload, fam);
        assert_eq!(block_schema, *schema);
        assert_eq!(frame_data(&payload, fam, &block_schema).as_ref(), Some(&*want.batch));
    }
}

/// A push frame carries its schema record as its blob, and its rows; an empty
/// batch still ships the record, but no data block.
#[test]
fn a_frame_carries_its_schema_and_its_rows_unless_empty() {
    let schema = kv_schema(TypeCode::I64);
    let hdr = ControlHeader { target_id: 42, ..Default::default() };
    for batch in [
        kv_rows(&[(1, 100, 1), (2, 200, 1), (3, 300, 1)]),
        ZSetBatch::new(&schema),
    ] {
        let buf = encode_frame(hdr, schema.to_record(), Some(&batch));
        let ctrl = peek_control_block(&buf).unwrap();
        assert_eq!(ctrl.hdr, hdr);
        assert_eq!(frame_schema(&buf, &ctrl), schema);
        let want = (!batch.is_empty()).then_some(&batch);
        assert_eq!(frame_data(&buf, &ctrl, &schema).as_ref(), want);
    }
}
