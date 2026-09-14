use super::*;

/// Every non-OK status maps to its own error, and a `STATUS_ERROR` whose text
/// is absent or blank falls back to the default rather than surfacing a blank
/// one — the warm-push guard's rejection must stay legible.
///
/// Driven through a real encoded control frame, not a hand-built `Message`:
/// which statuses reach the classifier carrying text is the decoder's decision,
/// and a hand-built one can express a state the decoder never produces.
#[test]
fn check_response_classifies_every_status() {
    use crate::protocol::message::{encode_control_block, parse_response_frame};
    use crate::protocol::Header;

    let classify = |status: u32, text: &str| {
        let hdr = Header {
            status,
            target_id: 0,
            client_id: 0,
            flags: 0,
            seek_pk: 77,
            seek_col_idx: 0,
            request_id: 0,
        };
        let frame = encode_control_block(&hdr, text, &[]);
        let mut msg = parse_response_frame(&frame, None)
            .expect("a control frame parses")
            .message;
        check_response(&mut msg).map(|()| msg)
    };
    let err = |status, text: &str| classify(status, text).expect_err("a non-OK status is an error");

    assert!(matches!(err(STATUS_SCHEMA_MISMATCH, ""), ClientError::SchemaMismatch));
    assert!(matches!(err(STATUS_DELTA_EXPIRED, ""), ClientError::DeltaExpired));
    assert!(matches!(
        err(STATUS_TXN_CONFLICT, ""),
        ClientError::TxnConflict { fresh_basis: 77 }
    ));
    assert!(matches!(err(STATUS_NO_INDEX, ""), ClientError::ServerError(m) if m.contains("no index")));
    assert!(matches!(err(999, ""), ClientError::ServerError(m) if m.contains("unrecognized status 999")));

    // The server formats real text for STATUS_SAL_FULL, and it must survive the
    // decode: gating the text on STATUS_ERROR made `SalFull`'s payload dead.
    let sal = err(STATUS_SAL_FULL, "SAL full: Push group did not fit");
    assert!(
        matches!(&sal, ClientError::SalFull(m) if m == "SAL full: Push group did not fit"),
        "{sal:?}"
    );

    for (text, want) in [("", "unknown server error"), ("real error", "real error")] {
        assert!(
            matches!(err(STATUS_ERROR, text), ClientError::ServerError(m) if m == want),
            "{text:?}"
        );
    }
    assert!(classify(STATUS_OK, "").is_ok());
}

mod spine_tests {
    //! The spine under bytes only a test can arrange: a scripted peer on the far
    //! end of a socketpair feeds reply frames one `step` at a time.

    use crate::connection::*;
    use crate::protocol::codec::encode_schema_block;
    use crate::protocol::message::{encode_control_block, encode_message_noschema_parts};
    use crate::protocol::transport::poll_fd;
    use crate::protocol::{BatchAppender, ColumnDef, Header, TypeCode};
    use crate::test_support::{established, framed, make_socketpair, raw_read_frame, raw_send, reply_ctrl};

    /// The version a request for `tid` would stamp now; `0` = nothing cached.
    fn stamped(s: &mut Session, tid: u64) -> u16 {
        s.cached_hint(tid).map_or(0, |h| h.1)
    }
    use std::os::fd::OwnedFd;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// A scripted peer: the raw far end of the socketpair.
    struct Peer(OwnedFd);

    impl Peer {
        /// Write one length-prefixed frame.
        fn send(&self, payload: &[u8]) {
            raw_send(&self.0, &framed(payload));
        }

        /// Read one request frame the session wrote.
        fn drain_request(&self) -> Vec<u8> {
            raw_read_frame(&self.0)
        }
    }

    fn pair() -> (Session, Peer) {
        let (a, b) = make_socketpair();
        (Session::from_transport(established(a)), Peer(b))
    }

    fn schema_a() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        }
    }

    fn schema_b() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
                ColumnDef::new("f", TypeCode::F64, false),
            ],
            pk_cols: vec![0],
        }
    }

    fn batch_a(pks: &[u64]) -> ZSetBatch {
        let schema = schema_a();
        let mut b = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut b, &schema);
        for &pk in pks {
            app.add_row(pk as u128, 1).i64_val(pk as i64 * 10);
        }
        b
    }

    fn batch_b(pks: &[u64]) -> ZSetBatch {
        let schema = schema_b();
        let mut b = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut b, &schema);
        for &pk in pks {
            app.add_row(pk as u128, 1)
                .str_val(&format!("s{pk}"))
                .f64_val(pk as f64 * 0.5);
        }
        b
    }

    /// A reply frame carrying its schema block at `version`, data, and `lsn` in
    /// `seek_pk`; `cont` sets `FLAG_CONTINUATION`.
    fn reply_cold(tid: u64, version: u16, schema: &Schema, batch: &ZSetBatch, lsn: u128, cont: bool) -> Vec<u8> {
        let mut flags = wire_flags_set_schema_version(0, version);
        if cont {
            flags |= FLAG_CONTINUATION;
        }
        encode_message_parts(tid, 0, flags, lsn, &[], 0, Some((schema, batch))).to_vec()
    }

    /// A hint-only reply frame: data, no schema block, `version` in the flags.
    fn reply_warm(tid: u64, version: u16, schema: &Schema, batch: &ZSetBatch, cont: bool) -> Vec<u8> {
        let mut flags = wire_flags_set_schema_version(0, version);
        if cont {
            flags |= FLAG_CONTINUATION;
        }
        encode_message_noschema_parts(tid, 0, flags, schema, batch).to_vec()
    }

    fn reply_status(status: u32, text: &str, seek_pk: u128) -> Vec<u8> {
        encode_control_block(&Header { status, seek_pk, ..Header::default() }, text, &[])
    }

    /// A cache entry for `tid` at `version`, warmed by one cold-answered scan —
    /// the precondition for a push that encodes schema-less.
    fn warm(s: &mut Session, peer: &Peer, tid: u64, version: u16, schema: &Schema) {
        let slot = s.submit(Request::scan(tid)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(tid, version, schema, &ZSetBatch::new(schema), 0, false));
        drive(s, slot).unwrap();
    }

    /// Drive `s` until `slot` completes, parking in `poll` between steps — the
    /// concurrent driver's loop, over one slot.
    fn drive(s: &mut Session, slot: SlotId) -> Result<Reply, ClientError> {
        let mut ready = Interest::WRITE;
        loop {
            let mut done = s.step(ready)?;
            if let Some(i) = done.iter().position(|(id, _)| *id == slot) {
                return done.swap_remove(i).1;
            }
            let interest = s.interest();
            assert!(!interest.is_empty(), "slot pending with no interest");
            let rev = poll_fd(
                s.as_raw_fd(),
                interest.poll_events(),
                Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
                true,
            )
            .expect("poll");
            ready = Interest::from_revents(rev);
        }
    }

    #[test]
    fn train_split_across_continuation_frames_completes_once() {
        let (mut s, peer) = pair();
        let schema = schema_a();
        let slot = s.submit(Request::scan(7)).unwrap();
        assert_eq!(s.interest(), Interest::BOTH);
        assert!(s.step(Interest::WRITE).unwrap().is_empty());
        assert_eq!(s.interest(), Interest::READ);
        peer.drain_request();

        // Three frames, one per step: nothing completes until the terminal.
        peer.send(&reply_cold(7, 3, &schema, &batch_a(&[1, 2]), 0, true));
        assert!(s.step(Interest::READ).unwrap().is_empty());
        peer.send(&reply_warm(7, 3, &schema, &batch_a(&[3]), true));
        assert!(s.step(Interest::READ).unwrap().is_empty());
        peer.send(&reply_warm(7, 3, &schema, &batch_a(&[4, 5]), false));
        let mut done = s.step(Interest::READ).unwrap();
        assert_eq!(done.len(), 1);
        let (id, reply) = done.pop().unwrap();
        assert_eq!(id, slot);
        let Reply::Scan(r) = reply.unwrap() else { panic!("scan") };
        assert_eq!(r.batch.pks.to_vec_u128(&schema_a()), vec![1, 2, 3, 4, 5]);
        assert_eq!(*r.schema, schema_a(), "the block the train carried");
        // Absorbed into the cache under version 3.
        assert_eq!(stamped(&mut s, 7), 3);
        assert!(s.step(Interest::READ).unwrap().is_empty());
        assert_eq!(s.interest(), Interest::NONE);
    }

    #[test]
    fn scan_multi_decodes_each_train_under_its_own_relation() {
        let (mut s, peer) = pair();
        let (sa, sb) = (schema_a(), schema_b());
        // Warm both relations first, under different schemas.
        warm(&mut s, &peer, 1, 1, &sa);
        warm(&mut s, &peer, 2, 1, &sb);

        // Now the multi: both trains reply warm, and each must decode under its
        // own relation's schema — a two-frame train for relation 2 included.
        let slot = s.submit(Request::ScanMulti(&[1, 2])).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_warm(1, 1, &sa, &batch_a(&[10, 11]), false));
        assert!(s.step(Interest::READ).unwrap().is_empty(), "one of two trains");
        peer.send(&reply_warm(2, 1, &sb, &batch_b(&[20]), true));
        peer.send(&reply_warm(2, 1, &sb, &batch_b(&[21]), false));
        let Reply::Multi(replies) = drive(&mut s, slot).unwrap() else {
            panic!("multi")
        };
        assert_eq!(replies.len(), 2);
        let d0 = &replies[0].batch;
        assert_eq!(d0.payload.len(), 1);
        assert_eq!(d0.pks.to_vec_u128(&sa), vec![10, 11]);
        let d1 = &replies[1].batch;
        assert_eq!(d1.payload.len(), 2);
        assert_eq!(d1.pks.to_vec_u128(&sb), vec![20, 21]);
        let strs: Vec<&[u8]> = d1.payload[0]
            .bytes
            .as_chunks::<16>()
            .0
            .iter()
            .map(|cell| gnitz_wire::german_string_content(cell, &d1.blob))
            .collect();
        assert_eq!(strs, [b"s20".as_slice(), b"s21".as_slice()]);
        // Each reply carries its own relation's schema, resolved off the cache.
        assert_eq!(replies[0].schema.columns.len(), 2);
        assert_eq!(replies[1].schema.columns.len(), 3);
        assert_eq!(s.interest(), Interest::NONE);
    }

    /// A warm read decodes under the hint its request was stamped with, even when
    /// the cache entry is evicted before its reply arrives: 64 other relations'
    /// schema blocks are absorbed between the submit and the reply.
    #[test]
    fn a_warm_read_decodes_under_its_stamped_hint_after_eviction() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let t = 1000;
        warm(&mut s, &peer, t, 7, &sa);

        let others: Vec<u64> = (1..=64).collect();
        for &k in &others {
            s.submit(Request::scan(k)).unwrap();
        }
        let slot = s.submit(Request::scan(t)).unwrap();
        s.step(Interest::WRITE).unwrap();
        for _ in 0..=others.len() {
            peer.drain_request();
        }
        let empty = ZSetBatch::new(&sa);
        for &k in &others {
            peer.send(&reply_cold(k, 1, &sa, &empty, 0, false));
        }
        peer.send(&reply_warm(t, 7, &sa, &batch_a(&[5, 6]), false));
        let Reply::Scan(r) = drive(&mut s, slot).unwrap() else {
            panic!("scan")
        };
        assert_eq!(r.batch.pks.to_vec_u128(&sa), vec![5, 6]);
        assert_eq!(stamped(&mut s, t), 0, "the entry was evicted before the reply");
    }

    #[test]
    fn scan_multi_error_on_kth_train_fails_slot_and_connection_stays_usable() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let slot = s.submit(Request::ScanMulti(&[1, 2, 3])).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(1, 1, &sa, &batch_a(&[1]), 0, false));
        // The second train is replaced by one error frame and nothing follows.
        peer.send(&reply_status(STATUS_ERROR, "relation 2 vanished", 0));
        let r = drive(&mut s, slot);
        assert!(matches!(r, Err(ClientError::ServerError(ref m)) if m == "relation 2 vanished"));
        assert_eq!(s.interest(), Interest::NONE, "nothing left pending");

        // The next request on the same connection completes normally.
        let slot = s.submit(Request::scan(3)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(3, 1, &sa, &batch_a(&[5]), 42, false));
        let Reply::Scan(r) = drive(&mut s, slot).unwrap() else {
            panic!("scan")
        };
        assert_eq!(r.lsn, Some(42));
    }

    #[test]
    fn one_read_serves_two_slots_and_leaves_nothing_buffered() {
        let (mut s, peer) = pair();
        let s1 = s.submit(Request::scan(1)).unwrap();
        let s2 = s.submit(Request::scan(2)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.drain_request();
        let empty = ZSetBatch::new(&schema_a());
        let mut both = framed(&reply_cold(1, 1, &schema_a(), &empty, 11, false));
        both.extend(framed(&reply_cold(2, 1, &schema_a(), &empty, 22, false)));
        raw_send(&peer.0, &both);
        let done = s.step(Interest::READ).unwrap();
        let ids: Vec<SlotId> = done.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![s1, s2], "in request order");
        for (id, r) in done {
            let Reply::Scan(r) = r.unwrap() else { panic!("scan") };
            assert_eq!(r.lsn, Some(if id == s1 { 11 } else { 22 }));
        }
        assert_eq!(s.interest(), Interest::NONE);
    }

    #[test]
    fn a_status_frame_completes_its_slot_and_leaves_the_connection_usable() {
        // `check_response` owns the status → error table; what the spine adds is
        // that a status frame completes its slot rather than erroring `step`,
        // and carries the frame's `seek_pk` into the error it hands back.
        let (mut s, peer) = pair();
        let slot = s.submit(Request::RawFrame(reply_ctrl(0, 0))).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_status(STATUS_TXN_CONFLICT, "", 77));
        let r = drive(&mut s, slot);
        assert!(
            matches!(r, Err(ClientError::TxnConflict { fresh_basis: 77 })),
            "the basis rides the frame: {r:?}"
        );
        assert!(!s.closed);
        assert_eq!(s.interest(), Interest::NONE, "nothing left pending");
    }

    #[test]
    fn out_of_order_reply_is_a_step_error_and_closes_the_blocking_client() {
        let (mut s, peer) = pair();
        // The peer answers a scan of 5 with a frame naming 6.
        peer.send(&reply_ctrl(6, 1));
        let r = s.scan(5);
        assert!(matches!(r, Err(ClientError::Protocol(_))), "{r:?}");
        assert_eq!(s.interest(), Interest::NONE);
        // The next call reports the connection closed instead of submitting.
        let r = s.scan(5);
        assert!(matches!(r, Err(ClientError::Closed)));
        assert!(matches!(s.submit(Request::scan(1)), Err(ClientError::Closed)));
        assert!(s.step(Interest::READ).unwrap().is_empty());
    }

    #[test]
    fn a_peer_that_closes_surfaces_an_io_error_rather_than_a_park() {
        let (mut s, peer) = pair();
        s.submit(Request::scan(5)).unwrap();
        s.step(Interest::WRITE).unwrap();
        // The request is on the wire and unread, so dropping the peer draws a
        // reset. A hangup folds into read readiness, so the waiting driver wakes.
        drop(peer);
        let rev = poll_fd(
            s.as_raw_fd(),
            Interest::READ.poll_events(),
            Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
            true,
        )
        .expect("poll");
        assert!(Interest::from_revents(rev).read, "a hangup wakes a read park");
        let r = s.step(Interest::READ);
        assert!(
            matches!(r, Err(ClientError::Protocol(ProtocolError::IoError(_)))),
            "{r:?}"
        );
    }

    #[test]
    fn close_abandons_every_pending_slot_and_refuses_further_work() {
        let (mut s, _peer) = pair();
        s.submit(Request::scan(1)).unwrap();
        s.submit(Request::scan(2)).unwrap();
        assert_eq!(s.interest(), Interest::BOTH);
        s.close();
        assert_eq!(s.interest(), Interest::NONE);
        assert!(matches!(s.submit(Request::scan(3)), Err(ClientError::Closed)));
        assert!(s.step(Interest::BOTH).unwrap().is_empty());
    }

    /// The in-flight count bounds no memory on its own, so the byte cap is what
    /// stops a driver that submits without ever flushing.
    #[test]
    fn queued_bytes_tracks_the_write_cursor_and_caps_submission() {
        let (mut s, _peer) = pair();
        let sa = schema_a();
        let b = batch_a(&[1, 2, 3, 4]);
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

        // The cap is checked before queueing, so one frame up to the peer's
        // egress limit always goes through and it is the *next* submit that is
        // refused.
        s.submit(Request::RawFrame(vec![0u8; MAX_QUEUED_BYTES])).unwrap();
        assert!(s.queued_bytes() >= MAX_QUEUED_BYTES);
        let r = push(&mut s);
        assert!(
            matches!(r, Err(ClientError::ServerError(ref m)) if m.contains("unwritten bytes queued")),
            "the byte cap, not the count one: {r:?}"
        );
        assert!(s.interest().write, "nothing was flushed, so it is all still queued");

        // Writing gives the budget back — the counter tracks the write cursor,
        // not just the pushes.
        let queued = s.queued_bytes();
        s.step(Interest::WRITE).unwrap();
        assert!(s.queued_bytes() < queued, "a flush releases what it wrote");
    }

    #[test]
    fn in_flight_cap_raises_rather_than_hanging() {
        let (mut s, _peer) = pair();
        let mut last = SlotId(0);
        for _ in 0..MAX_IN_FLIGHT {
            let id = s.submit(Request::scan(1)).unwrap();
            assert!(id > last, "monotonic");
            last = id;
        }
        let r = s.submit(Request::scan(1));
        assert!(matches!(r, Err(ClientError::ServerError(ref m)) if m.contains("in flight")));
    }

    /// Deliver `SIGUSR1` to the calling thread until `stop` is set, with a no-op
    /// handler installed without `SA_RESTART` — how CPython's handlers land — so
    /// a park in `poll(2)` returns `EINTR`. Repeating rather than one-shot: a
    /// single signal that lands before the park is entered leaves the park to
    /// block forever, which libtest has no timeout to break.
    fn interrupt_self_until(stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        extern "C" fn noop(_: libc::c_int) {}
        // SAFETY: installing a trivial handler for a signal nothing else in the
        // test binary uses.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = noop as extern "C" fn(libc::c_int) as usize;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
        }
        let me = unsafe { libc::pthread_self() };
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(1));
                // SAFETY: the target is the test thread, which outlives this loop.
                unsafe { libc::pthread_kill(me, libc::SIGUSR1) };
            }
        })
    }

    /// An aborted park leaves its slot pending — the frame is already on the
    /// wire, so the next call drains that reply before its own. The hook is the
    /// host's and outlives the session it was installed on, which is what
    /// [`crate::GnitzClient::reconnect`] relies on to keep a blocking call
    /// Ctrl-C-interruptible across a replaced connection.
    #[test]
    fn an_aborted_park_leaves_its_slot_pending_and_the_next_call_drains_it_first() {
        // Installed on one session, taken out, and moved onto another.
        let (mut donor, _donor_peer) = pair();
        let mut fired = false;
        donor.set_park_hook(Some(Box::new(move || {
            if std::mem::replace(&mut fired, true) {
                Ok(())
            } else {
                Err(ClientError::ServerError("interrupted".into()))
            }
        })));
        let hook = donor.take_park_hook();
        assert!(hook.is_some(), "the installed hook comes back out");
        assert!(donor.take_park_hook().is_none(), "and taking it leaves none behind");

        let (mut s, peer) = pair();
        s.set_park_hook(hook);
        let stop = Arc::new(AtomicBool::new(false));
        let sig = interrupt_self_until(Arc::clone(&stop));

        // The peer never answers, so the scan parks; the signal makes the park
        // return `EINTR`, which is the one place the hook runs.
        let r = s.scan(1);
        assert!(matches!(r, Err(ClientError::ServerError(ref m)) if m == "interrupted"));
        stop.store(true, Ordering::Relaxed);
        sig.join().unwrap();
        assert_eq!(s.interest(), Interest::READ, "the abandoned slot stays pending");

        // The peer answers both requests; the second call must get the second.
        let empty = ZSetBatch::new(&schema_a());
        peer.drain_request();
        peer.send(&reply_cold(1, 1, &schema_a(), &empty, 100, false));
        let h = std::thread::spawn(move || {
            peer.drain_request();
            peer.send(&reply_cold(1, 1, &schema_a(), &empty, 200, false));
            peer
        });
        assert_eq!(s.scan(1).unwrap().lsn, Some(200));
        let _peer = h.join().unwrap();
        assert_eq!(s.interest(), Interest::NONE);
    }

    #[test]
    fn raw_scan_spec_keeps_blocks_undecoded_and_off_the_cache() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let reply_schema = crate::protocol::ReplySchema::new(std::sync::Arc::new(sa.clone()), 9);
        let spec = [1u8, 2, 3];
        let slot = s
            .submit(Request::ScanSpec {
                target_id: 9,
                spec: &spec,
                reply_schema: &reply_schema,
                raw: true,
            })
            .unwrap();
        s.step(Interest::WRITE).unwrap();
        let req = peer.drain_request();
        assert!(
            req.len() >= spec.len() + encode_schema_block(&sa, 9).len(),
            "the spec and the reply schema both ride the request"
        );
        // Two hint-only frames: the server sends no schema block for a SCAN_SPEC.
        peer.send(&reply_warm(9, 0, &sa, &batch_a(&[1, 2]), true));
        peer.send(&reply_warm(9, 0, &sa, &batch_a(&[3]), false));
        let Reply::Raw { blocks, terminal } = drive(&mut s, slot).unwrap() else {
            panic!("raw")
        };
        assert_eq!(blocks.len(), 2);
        let (b0, _) = crate::protocol::decode_wal_block(blocks[0].block(), &sa).unwrap();
        assert_eq!(b0.pks.to_vec_u128(&sa), vec![1, 2]);
        assert_eq!(terminal.target_id, 9);
        assert_eq!(stamped(&mut s, 9), 0, "a scan_spec absorbs nothing");
    }

    #[test]
    fn push_retries_cold_on_schema_mismatch() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let b = batch_a(&[1]);
        warm(&mut s, &peer, 4, 2, &sa);
        // The server rejects the warm push; the blocking verb retries it cold.
        let h = std::thread::spawn(move || {
            peer.drain_request();
            peer.send(&reply_status(STATUS_SCHEMA_MISMATCH, "", 0));
            peer.drain_request();
            peer.send(&reply_ctrl(4, 555));
            peer
        });
        let lsn = s.push_with_mode(4, &sa, &b, WireConflictMode::Update).unwrap();
        assert_eq!(lsn, 555);
        let _peer = h.join().unwrap();
        assert_eq!(s.requests_sent(), 3, "scan, warm push, cold retry — one per frame");
    }

    #[test]
    fn a_mismatch_evicts_the_cache_and_the_next_push_goes_out_cold() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let b = batch_a(&[1]);
        // Warm relation 4 at version 2, so both pushes below encode schema-less.
        warm(&mut s, &peer, 4, 2, &sa);

        let push = |s: &mut Session| {
            s.submit(Request::Push {
                target_id: 4,
                schema: &sa,
                batch: &b,
                mode: WireConflictMode::Update,
            })
            .unwrap()
        };
        let (a, c) = (push(&mut s), push(&mut s));
        s.step(Interest::WRITE).unwrap();
        let warm_len = peer.drain_request().len();
        assert_eq!(peer.drain_request().len(), warm_len, "both encoded at the stale stamp");

        // The first mismatch fails its own slot and evicts the entry.
        peer.send(&reply_status(STATUS_SCHEMA_MISMATCH, "", 0));
        let mut done = s.step(Interest::READ).unwrap();
        assert_eq!(done.len(), 1);
        let (id, r) = done.pop().unwrap();
        assert_eq!(id, a);
        assert!(matches!(r, Err(ClientError::SchemaMismatch)));
        assert!(!s.closed, "a per-slot error leaves the connection usable");
        assert_eq!(stamped(&mut s, 4), 0, "the entry is gone");

        // Submitted after the eviction: cold, longer by the schema block.
        let cold = push(&mut s);
        s.step(Interest::WRITE).unwrap();
        let cold_len = peer.drain_request().len();
        assert_eq!(
            cold_len - warm_len,
            encode_schema_block(&sa, 4).len(),
            "the cold frame is longer by exactly the schema block the warm ones omitted"
        );

        // The second stale push fails too — every push encoded at the stale stamp does.
        peer.send(&reply_status(STATUS_SCHEMA_MISMATCH, "", 0));
        let mut done = s.step(Interest::READ).unwrap();
        assert_eq!(done.len(), 1);
        let (id, r) = done.pop().unwrap();
        assert_eq!(id, c);
        assert!(matches!(r, Err(ClientError::SchemaMismatch)));

        // The cold push's ACK carries the block at the new version, which puts the
        // next push back on the warm path.
        peer.send(&reply_cold(4, 5, &sa, &batch_a(&[]), 999, false));
        let Reply::Lsn(lsn) = drive(&mut s, cold).unwrap() else {
            panic!("push ACK")
        };
        assert_eq!(lsn, 999);
        assert_eq!(stamped(&mut s, 4), 5);
        push(&mut s);
        s.step(Interest::WRITE).unwrap();
        assert_eq!(peer.drain_request().len(), warm_len, "warm again");
    }
}
