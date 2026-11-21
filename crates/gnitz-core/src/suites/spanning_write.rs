//! Writes spanning several request frames against a real server: one write
//! committed whole, or nothing of it; another frame inside one; a write past
//! the server's limit for one.

use std::time::{Duration, Instant};

use super::*;
use crate::ClientTransport;
use crate::connection::PushFamily;
use crate::transport::poll_fd;
use gnitz_wire::multi_frame::BLIND;
use gnitz_wire::{ClientVerb, ControlHeader, Target, WireStatus, append_frame, peek_control_block};

/// The session's writes cut into frames of this many bytes: a few hundred
/// rows of a four-column table each.
const FRAME_BYTES: usize = 16 << 10;

/// A `(pk, a, b)` table on `client`'s server, and a session on it cutting
/// writes at [`FRAME_BYTES`].
fn spanning_session(srv: &ServerHandle) -> (GnitzClient, Session, u64, Arc<Schema>) {
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(
        &mut client,
        schema_of(&[("pk", TypeCode::I64), ("a", TypeCode::I64), ("b", TypeCode::I64)]),
    );
    let mut s = Session::connect(srv.sock_path()).unwrap();
    s.set_write_frame_bytes(FRAME_BYTES);
    (client, s, tid, schema)
}

/// The answer to `req`, submitted on `s`.
fn answer(s: &mut Session, req: Request<'_>) -> Result<u64, ClientError> {
    let sent = s.submit(req);
    drive(s, sent).0
}

fn push(tid: u64, batch: &Arc<ZSetBatch>, mode: WireConflictMode) -> Request<'_> {
    Request::Push { target: tid.into(), batch, mode }
}

/// A push of many frames commits whole; one whose key in its first frame is
/// repeated in its last commits nothing, refused as a duplicate within the
/// batch; an upsert of every row again leaves each key held once.
#[test]
fn a_push_over_several_frames_commits_whole_or_not_at_all() {
    let srv = ServerHandle::start_n(4);
    let (mut client, mut s, tid, schema) = spanning_session(&srv);
    let n = 5_000u64;
    let all = Arc::new(rows(&schema, 0..n));
    answer(&mut s, push(tid, &all, WireConflictMode::Error)).unwrap();
    assert_eq!(scan_all(&mut client, tid, &schema).len() as u64, n);

    // Fresh keys, the first of them repeated at the very end.
    let mut again = rows(&schema, n..2 * n);
    again.extend_from_owned(rows(&schema, [n]));
    let refused = answer(&mut s, push(tid, &Arc::new(again), WireConflictMode::Error));
    let e = refused.expect_err("a duplicate in the last frame").to_string();
    assert!(e.contains("Batch contains multiple rows with key"), "{e}");
    assert_eq!(
        scan_all(&mut client, tid, &schema).len() as u64,
        n,
        "nothing of it committed"
    );

    // An upsert of every key with new payloads leaves each key held once, at
    // its new payload.
    let mut upsert = ZSetBatch::new(&schema);
    let mut app = upsert.builder();
    for pk in 0..n {
        app.begin_row(pk as u128, 1).put_int(pk as u128 + 7).put_int(9);
    }
    let upsert = Arc::new(upsert);
    answer(&mut s, push(tid, &upsert, WireConflictMode::Update)).unwrap();
    assert_eq!(
        weighted_rows(&scan_all(&mut client, tid, &schema)),
        weighted_rows(&upsert)
    );
}

/// A PUSH_TXN of many frames commits every family as one zone, a family cut
/// across frames included.
#[test]
fn a_push_txn_over_several_frames_commits_every_family() {
    let srv = ServerHandle::start_n(4);
    let (mut client, mut s, t1, schema) = spanning_session(&srv);
    let (_, t2, _) = create_table(&mut client, (*schema).clone());
    let family = |tid: u64, batch| PushFamily {
        target: tid.into(),
        batch: Arc::new(batch),
        mode: WireConflictMode::Error,
        basis: BLIND,
    };
    let families = [family(t1, rows(&schema, 0..3_000)), family(t2, rows(&schema, 0..4_000))];
    answer(&mut s, Request::PushTxn(&families)).unwrap();
    assert_eq!(scan_all(&mut client, t1, &schema).len(), 3_000);
    assert_eq!(scan_all(&mut client, t2, &schema).len(), 4_000);
}

/// A PUSH_TXN whose family in its last frame was read at a basis a commit
/// has since passed conflicts whole: the families of its earlier frames
/// commit nothing.
#[test]
fn an_occ_conflict_in_the_last_frame_refuses_the_whole_txn() {
    let srv = ServerHandle::start_n(4);
    let (mut client, mut s, t1, schema) = spanning_session(&srv);
    let (_, t2, _) = create_table(&mut client, (*schema).clone());
    answer(
        &mut s,
        push(t2, &Arc::new(rows(&schema, [0])), WireConflictMode::Update),
    )
    .unwrap();
    let family = |tid: u64, batch, basis| PushFamily {
        target: tid.into(),
        batch: Arc::new(batch),
        mode: WireConflictMode::Update,
        basis,
    };
    // Read before the push above: basis 1 is past by every commit.
    let families = [
        family(t1, rows(&schema, 0..3_000), BLIND),
        family(t2, rows(&schema, [1]), 1),
    ];
    let e = answer(&mut s, Request::PushTxn(&families)).expect_err("a stale basis");
    assert!(
        matches!(&e, ClientError::Refused(f) if f.status == WireStatus::TxnConflict),
        "{e}"
    );
    assert_eq!(scan_all(&mut client, t1, &schema).len(), 0);
    assert_eq!(scan_all(&mut client, t2, &schema).len(), 1);
}

/// The answers to the requests sent on `t`, each one's header and text, until
/// the server closes the connection.
fn answers_until_closed(t: &mut ClientTransport) -> Vec<(ControlHeader, String)> {
    let mut out = Vec::new();
    let until = Instant::now() + Duration::from_secs(30);
    while t.wants_write() && t.flush().is_ok() {}
    loop {
        poll_fd(t.as_fd(), Interest::READ, Some(until)).unwrap();
        let read = t.read(|frame| {
            let ctrl = peek_control_block(&frame).unwrap();
            let text = String::from_utf8_lossy(&frame[ctrl.blob.clone()]).into_owned();
            out.push((ctrl.hdr, text));
            Ok(())
        });
        if read.is_err() {
            return out;
        }
    }
}

/// A frame that is not the write's inside a write's frames — one of another
/// verb, or one that does not decode — refuses the write, which commits
/// nothing, its last frame included, and closes the connection: the one
/// answer is the write's.
#[test]
fn a_frame_inside_a_writes_frames_refuses_it_and_closes() {
    let srv = ServerHandle::start_n(4);
    let (mut client, _, tid, schema) = spanning_session(&srv);
    let batch = rows(&schema, 0..10);
    let frame = |more: bool| {
        let mut hdr = ControlHeader::naming(ClientVerb::Push, Target::from(tid), 0);
        hdr.flags.continuation = more;
        let mut frame = Vec::new();
        append_frame(&mut frame, &hdr, schema.shape_record(), Some(&batch.view().regions()));
        frame
    };
    let mut alloc = Vec::new();
    append_frame(
        &mut alloc,
        &ControlHeader::naming(ClientVerb::AllocIds, Target::from(0), 1),
        &[],
        None,
    );
    for interloper in [alloc, b"not a frame".to_vec()] {
        let mut t = ClientTransport::connect(srv.sock_path(), Instant::now() + Duration::from_secs(10)).unwrap();
        for f in [frame(true), interloper, frame(false)] {
            t.enqueue(&f).unwrap();
        }
        let got = answers_until_closed(&mut t);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!((got[0].0.target_id, got[0].0.status), (tid, WireStatus::Error));
        assert!(got[0].1.contains("broken off by another request"), "{}", got[0].1);
        assert_eq!(scan_all(&mut client, tid, &schema).len(), 0);
    }
}

/// A write whose frames pass the server's limit for one is refused with a
/// fault, its frames dropped; it commits nothing, and the connection serves the
/// next request.
#[test]
fn a_write_past_the_server_limit_is_refused_and_the_connection_kept() {
    let srv = ServerHandle::start_with_env(4, &[("GNITZ_WRITE_SPAN_BYTES", "100000")]);
    let (mut client, mut s, tid, schema) = spanning_session(&srv);
    let big = Arc::new(rows(&schema, 0..20_000));
    let e = answer(&mut s, push(tid, &big, WireConflictMode::Update));
    let e = e.expect_err("past the limit").to_string();
    assert!(e.contains("100000-byte limit"), "{e}");
    let small = Arc::new(rows(&schema, 0..100));
    answer(&mut s, push(tid, &small, WireConflictMode::Update)).unwrap();
    assert_eq!(scan_all(&mut client, tid, &schema).len(), 100);
}
