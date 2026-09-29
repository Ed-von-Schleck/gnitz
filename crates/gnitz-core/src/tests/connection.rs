mod spine_tests {
    //! The spine under bytes only a test can arrange: a scripted peer on the far
    //! end of a socketpair feeds reply frames one `step` at a time.

    use crate::connection::*;
    use crate::protocol::message::encode_frame;
    use crate::protocol::transport::poll_fd;
    use crate::test_support::{framed, raw_read_frame, raw_send, reply_ctrl, session_pair as pair, Peer};
    use crate::BatchAppender;
    use crate::GnitzClient;
    use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TypeCode, WireFault, WireStatus};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    impl Peer {
        /// Read one request frame the session wrote.
        fn drain_request(&self) -> Vec<u8> {
            raw_read_frame(&self.0)
        }
    }

    /// Submit a read of every row of `tid`, decoded under `schema`.
    fn submit_scan(s: &mut Session, tid: u64, schema: &Arc<Schema>) -> Result<SlotId, ClientError> {
        let spec = ReadSpec::all_rows(ReadBound::None);
        s.submit(Request::ScanSpec {
            target_id: tid,
            spec: &spec,
            reply_schema: schema,
        })
    }

    fn schema_a() -> Arc<Schema> {
        Arc::new(Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        })
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

    fn reply_status(status: WireStatus, text: &str, arg0: u64) -> Vec<u8> {
        let hdr = ControlHeader { status, arg0, ..Default::default() };
        encode_frame(hdr, text.as_bytes(), None, None)
    }

    /// Drive `s` until `slot` completes, parking in `poll` between steps — the
    /// concurrent driver's loop, over one slot.
    fn drive(s: &mut Session, slot: SlotId) -> Result<Reply, ClientError> {
        let mut ready = Interest::WRITE;
        loop {
            let mut done = s.step(ready);
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
        let slot = submit_scan(&mut s, 7, &schema).unwrap();
        assert_eq!(s.interest(), Interest::BOTH);
        assert!(s.step(Interest::WRITE).is_empty());
        assert_eq!(s.interest(), Interest::READ);
        peer.drain_request();

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
        assert_eq!(r.batch.pks.to_vec_u128(&schema_a()), vec![1, 2, 3, 4, 5]);
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
        let slot = s.submit(Request::ScanMulti(&[(1, &sa), (2, &sb)])).unwrap();
        s.step(Interest::WRITE);
        let req = peer.drain_request();
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
        let strs: Vec<&str> = (0..d1.len()).map(|r| gnitz_expr::payload_str(d1, r, 0)).collect();
        assert_eq!(strs, ["s20", "s21"]);
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
        peer.drain_request();
        peer.send(&encode_frame(
            reply_header(7, 0, false),
            &[],
            Some(&sa.to_block()),
            Some(&batch_a(&[5])),
        ));
        let e = drive(&mut s, slot).expect_err("a schema block on a read is a decode error");
        assert!(
            matches!(&e, ClientError::ConnectionLost(ProtocolError::DecodeError(m))
                if m.contains("schema block")),
            "{e:?}"
        );
        assert!(s.is_closed());
    }

    #[test]
    fn scan_multi_error_on_kth_train_fails_slot_and_connection_stays_usable() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let slot = s.submit(Request::ScanMulti(&[(1, &sa), (2, &sa), (3, &sa)])).unwrap();
        s.step(Interest::WRITE);
        peer.drain_request();
        peer.send(&reply_rows(1, &batch_a(&[1]), 0, false));
        // The second train is replaced by one error frame and nothing follows.
        peer.send(&reply_status(WireStatus::Error, "relation 2 vanished", 0));
        let r = drive(&mut s, slot);
        assert!(matches!(r, Err(ClientError::Refused(WireFault { text: ref m, .. })) if m == "relation 2 vanished"));
        assert_eq!(s.interest(), Interest::NONE, "nothing left pending");

        // The next request on the same connection completes normally.
        let slot = submit_scan(&mut s, 3, &sa).unwrap();
        s.step(Interest::WRITE);
        peer.drain_request();
        peer.send(&reply_rows(3, &batch_a(&[5]), 42, false));
        let Reply::Scan(r) = drive(&mut s, slot).unwrap() else {
            panic!("scan")
        };
        assert_eq!(r.lsn, Some(42));
    }

    /// An empty list is the server's to refuse: its one frame-level fault ends a
    /// slot that has no position to fill, and the connection keeps serving.
    #[test]
    fn an_empty_scan_multi_refused_by_the_server_fails_its_slot_alone() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let slot = s.submit(Request::ScanMulti(&[])).unwrap();
        s.step(Interest::WRITE);
        peer.drain_request();
        peer.send(&reply_status(WireStatus::Error, "SCAN_MULTI: empty item list", 0));
        let r = drive(&mut s, slot);
        assert!(
            matches!(r, Err(ClientError::Refused(WireFault { text: ref m, .. })) if m.contains("empty item list")),
            "{r:?}"
        );
        assert!(!s.is_closed());
        assert_eq!(s.interest(), Interest::NONE, "nothing left pending");

        let slot = submit_scan(&mut s, 3, &sa).unwrap();
        s.step(Interest::WRITE);
        peer.drain_request();
        peer.send(&reply_rows(3, &batch_a(&[5]), 42, false));
        let Reply::Scan(r) = drive(&mut s, slot).unwrap() else {
            panic!("scan")
        };
        assert_eq!(r.lsn, Some(42));
    }

    #[test]
    fn one_read_serves_two_slots_and_leaves_nothing_buffered() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let s1 = submit_scan(&mut s, 1, &sa).unwrap();
        let s2 = submit_scan(&mut s, 2, &sa).unwrap();
        s.step(Interest::WRITE);
        peer.drain_request();
        peer.drain_request();
        let empty = ZSetBatch::new(&sa);
        let mut both = framed(&reply_rows(1, &empty, 11, false));
        both.extend(framed(&reply_rows(2, &empty, 22, false)));
        raw_send(&peer.0, &both);
        let done = s.step(Interest::READ);
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
        // A status frame completes its slot rather than erroring `step`.
        let (mut s, peer) = pair();
        let slot = s.submit(Request::RawFrame(reply_ctrl(0, 0))).unwrap();
        s.step(Interest::WRITE);
        peer.drain_request();
        peer.send(&reply_status(WireStatus::TxnConflict, "conflict", 77));
        let r = drive(&mut s, slot);
        assert!(
            matches!(
                r,
                Err(ClientError::Refused(WireFault { status: WireStatus::TxnConflict, .. }))
            ),
            "the status picks the variant: {r:?}"
        );
        assert!(!s.is_closed());
        assert_eq!(s.interest(), Interest::NONE, "nothing left pending");
    }

    #[test]
    fn out_of_order_reply_fails_the_slot_and_ends_the_session() {
        let (s, peer) = pair();
        let mut c = GnitzClient::from_session(s);
        let (sa, b) = (schema_a(), batch_a(&[1]));
        // The peer answers a push to 5 with a frame naming 6.
        peer.send(&reply_ctrl(6, 1));
        let r = c.push(5, &sa, &b, WireConflictMode::Update);
        assert!(
            matches!(&r, Err(ClientError::ConnectionLost(ProtocolError::DecodeError(_)))),
            "{r:?}"
        );
        assert_eq!(c.session.interest(), Interest::NONE);
        // The next call reports the loss instead of submitting.
        let r = c.push(5, &sa, &b, WireConflictMode::Update);
        assert!(matches!(r, Err(ClientError::ConnectionLost(_))), "{r:?}");
        assert!(matches!(
            submit_scan(&mut c.session, 1, &sa),
            Err(ClientError::ConnectionLost(_))
        ));
        assert!(c.session.step(Interest::READ).is_empty());
    }

    /// A reply that decoded ahead of the frame that broke the framing reaches
    /// its slot; only the slot the bad frame was for, and any behind it, fail.
    #[test]
    fn replies_completed_before_a_fatal_frame_are_delivered() {
        let (mut s, peer) = pair();
        let (sa, rows) = (schema_a(), batch_a(&[1]));
        let a = s.submit(Request::RawFrame(reply_ctrl(0, 0))).unwrap();
        let b = s
            .submit(Request::Push {
                target_id: 5,
                schema: &sa,
                batch: &rows,
                mode: WireConflictMode::Update,
            })
            .unwrap();
        assert!(s.step(Interest::WRITE).is_empty());
        peer.drain_request();
        peer.drain_request();
        let mut both = framed(&reply_ctrl(0, 11));
        both.extend(framed(&reply_ctrl(6, 1)));
        raw_send(&peer.0, &both);
        let done = s.step(Interest::READ);
        assert_eq!(done.len(), 2);
        assert_eq!(done[0].0, a);
        assert!(matches!(done[0].1, Ok(Reply::Lsn(11))), "{:?}", done[0].1);
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
    }

    /// A peer that answered and then went is still readable after the write
    /// that finds it gone: the answer is delivered, the unanswered slot fails.
    #[test]
    fn a_reply_readable_behind_a_failed_flush_is_delivered() {
        let (mut s, peer) = pair();
        let a = s.submit(Request::RawFrame(reply_ctrl(0, 0))).unwrap();
        assert!(s.step(Interest::WRITE).is_empty());
        peer.drain_request();
        peer.send(&reply_ctrl(0, 11));
        drop(peer);
        let b = submit_scan(&mut s, 5, &schema_a()).unwrap();
        let done = s.step(Interest::WRITE);
        assert_eq!(done.len(), 2, "{done:?}");
        assert_eq!(done[0].0, a);
        assert!(matches!(done[0].1, Ok(Reply::Lsn(11))), "{:?}", done[0].1);
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
            Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
            true,
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
            matches!(r, Err(ClientError::Refused(WireFault { text: ref m, .. })) if m.contains("unwritten bytes queued")),
            "the byte cap, not the count one: {r:?}"
        );
        assert!(s.interest().write, "nothing was flushed, so it is all still queued");

        // Writing gives the budget back — the counter tracks the write cursor,
        // not just the pushes.
        let queued = s.queued_bytes();
        s.step(Interest::WRITE);
        assert!(s.queued_bytes() < queued, "a flush releases what it wrote");
    }

    #[test]
    fn in_flight_cap_raises_rather_than_hanging() {
        let (mut s, _peer) = pair();
        let sa = schema_a();
        let mut last = SlotId(0);
        for _ in 0..MAX_IN_FLIGHT {
            let id = submit_scan(&mut s, 1, &sa).unwrap();
            assert!(id > last, "monotonic");
            last = id;
        }
        let r = submit_scan(&mut s, 1, &sa);
        assert!(matches!(r, Err(ClientError::Refused(WireFault { text: ref m, .. })) if m.contains("in flight")));
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
    /// wire, so the next call drains that reply before its own.
    #[test]
    fn an_aborted_park_leaves_its_slot_pending_and_the_next_call_drains_it_first() {
        let (s, peer) = pair();
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
        let (sa, spec) = (schema_a(), ReadSpec::all_rows(ReadBound::None));
        let r = c.scan_spec(1, &spec, &sa);
        assert!(matches!(r, Err(ClientError::Interrupted(ref e)) if e.to_string() == "interrupted"));
        stop.store(true, Ordering::Relaxed);
        sig.join().unwrap();
        assert_eq!(c.session.interest(), Interest::READ, "the abandoned slot stays pending");

        // The peer answers both requests; the second call must get the second.
        let empty = ZSetBatch::new(&sa);
        peer.drain_request();
        peer.send(&reply_rows(1, &empty, 100, false));
        let h = std::thread::spawn(move || {
            peer.drain_request();
            peer.send(&reply_rows(1, &empty, 200, false));
            peer
        });
        assert_eq!(c.scan_spec(1, &spec, &sa).unwrap().lsn, Some(200));
        let _peer = h.join().unwrap();
        assert_eq!(c.session.interest(), Interest::NONE);
    }

    #[test]
    fn a_delta_read_keeps_blocks_undecoded() {
        let (s, peer) = pair();
        let mut c = GnitzClient::from_session(s);
        let sa = schema_a();
        let want = sa.layout_digest();
        let h = std::thread::spawn(move || {
            let req = peer.drain_request();
            let ctrl = peek_control_block(&req).unwrap();
            let items = gnitz_wire::txn_frame::decode_delta_poll(&req[ctrl.body]).unwrap();
            assert_eq!(items[0].reply_layout, want, "the reply layout rides the request");
            peer.send(&reply_rows(9, &batch_a(&[1, 2]), 0, true));
            // The terminal's `arg0` is the round the cursor moves to.
            peer.send(&reply_rows(9, &batch_a(&[3]), 5, false));
            peer
        });
        let (blocks, cursor) = c
            .delta_read_raw(gnitz_wire::txn_frame::DeltaPollItem {
                view_id: 9,
                after_tick: 4,
                reply_layout: want,
            })
            .unwrap();
        assert_eq!(cursor.tick.get(), 5);
        let _peer = h.join().unwrap();
        assert_eq!(blocks.len(), 2);
        let b0 = crate::test_support::decode_wal_block(blocks[0].block(), &sa).unwrap();
        assert_eq!(b0.pks.to_vec_u128(&sa), vec![1, 2]);
    }
}
