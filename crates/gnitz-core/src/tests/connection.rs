use super::*;

fn error_msg(error_text: Option<String>) -> Message {
    Message {
        status: STATUS_ERROR,
        target_id: 0,
        flags: 0,
        seek_pk: 0,
        schema: None,
        data_batch: None,
        error_text,
        seek_pk_extra: Vec::new(),
    }
}

// `Message` does not implement Debug, so match the Result rather than
// calling unwrap_err (which would require the Ok variant to be Debug).
fn server_error_text(msg: Message) -> String {
    match check_response(msg) {
        Err(ClientError::ServerError(s)) => s,
        Err(other) => panic!("expected ServerError, got {other:?}"),
        Ok(_) => panic!("expected an error"),
    }
}

#[test]
fn check_response_empty_error_text_falls_back_to_default() {
    // A STATUS_ERROR with Some("") must surface the default text, not a
    // blank ServerError — the warm-push guard's rejection must be legible.
    assert_eq!(
        server_error_text(error_msg(Some(String::new()))),
        "unknown server error"
    );
}

#[test]
fn check_response_none_error_text_falls_back_to_default() {
    assert_eq!(server_error_text(error_msg(None)), "unknown server error");
}

#[test]
fn check_response_nonempty_error_text_preserved() {
    assert_eq!(server_error_text(error_msg(Some("real error".into()))), "real error");
}

mod spine_tests {
    //! The spine under bytes only a test can arrange: a scripted peer on the far
    //! end of a socketpair feeds reply frames one `step` at a time.

    use crate::connection::*;
    use crate::protocol::codec::encode_schema_block;
    use crate::protocol::message::{encode_control_block, encode_message_noschema_parts};
    use crate::protocol::transport::poll_fd;
    use crate::protocol::{ColData, ColumnDef, Header, PkColumn, TypeCode};
    use crate::test_support::{established, framed, make_socketpair, raw_read_frame, raw_send};
    use std::os::fd::OwnedFd;

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
        let mut vals = Vec::new();
        for &pk in pks {
            vals.extend_from_slice(&((pk as i64) * 10).to_le_bytes());
        }
        ZSetBatch {
            pks: PkColumn::from_u128s(8, pks.iter().map(|&p| p as u128)),
            weights: vec![1; pks.len()],
            nulls: vec![0; pks.len()],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vals)],
        }
    }

    fn batch_b(pks: &[u64]) -> ZSetBatch {
        let mut f = Vec::new();
        for &pk in pks {
            f.extend_from_slice(&(pk as f64 * 0.5).to_le_bytes());
        }
        ZSetBatch {
            pks: PkColumn::from_u128s(8, pks.iter().map(|&p| p as u128)),
            weights: vec![1; pks.len()],
            nulls: vec![0; pks.len()],
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Strings(pks.iter().map(|p| Some(format!("s{p}"))).collect()),
                ColData::Fixed(f),
            ],
        }
    }

    /// A reply frame carrying its schema block at `version`, data, and `lsn` in
    /// `seek_pk`; `cont` sets `FLAG_CONTINUATION`.
    fn reply_cold(tid: u64, version: u16, schema: &Schema, batch: &ZSetBatch, lsn: u128, cont: bool) -> Vec<u8> {
        let mut flags = wire_flags_set_schema_version(0, version);
        if cont {
            flags |= FLAG_CONTINUATION;
        }
        encode_message_parts(tid, 0, flags, &PkTuple::from_u128_narrow(lsn), 0, Some((schema, batch))).to_vec()
    }

    /// A hint-only reply frame: data, no schema block, `version` in the flags.
    fn reply_warm(tid: u64, version: u16, schema: &Schema, batch: &ZSetBatch, cont: bool) -> Vec<u8> {
        let mut flags = wire_flags_set_schema_version(0, version);
        if cont {
            flags |= FLAG_CONTINUATION;
        }
        encode_message_noschema_parts(tid, 0, flags, schema, batch).to_vec()
    }

    /// A control-only terminal reply with `lsn` in `seek_pk`.
    fn reply_ctrl(tid: u64, lsn: u128) -> Vec<u8> {
        encode_control_frame(tid, 0, 0, lsn, 0, &[])
    }

    fn reply_status(status: u32, text: &str, seek_pk: u128) -> Vec<u8> {
        encode_control_block(
            &Header {
                status,
                seek_pk,
                ..Header::default()
            },
            text,
            &[],
        )
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
                Some(std::time::Duration::from_secs(5)),
                true,
            )
            .expect("poll");
            ready = Interest::from_revents(rev);
        }
    }

    fn scan_req(tid: u64) -> Request<'static> {
        Request::Read {
            target_id: tid,
            flags: 0,
            seek_pk: 0,
            seek_col_idx: 0,
            seek_pk_extra: &[],
        }
    }

    #[test]
    fn train_split_across_continuation_frames_completes_once() {
        let (mut s, peer) = pair();
        let schema = schema_a();
        let slot = s.submit(scan_req(7)).unwrap();
        assert_eq!(
            s.interest(),
            Interest {
                read: true,
                write: true
            }
        );
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
        let Reply::Scan((schema, data, _)) = reply.unwrap() else {
            panic!("scan")
        };
        assert_eq!(data.unwrap().pks.to_vec_u128(), vec![1, 2, 3, 4, 5]);
        assert!(schema.is_some(), "the block the train carried");
        // Absorbed into the cache under version 3.
        assert_eq!(s.cached_schema_version(7), 3);
        assert!(s.step(Interest::READ).unwrap().is_empty());
        assert_eq!(s.interest(), Interest::NONE);
    }

    #[test]
    fn scan_multi_decodes_each_train_under_its_own_relation() {
        let (mut s, peer) = pair();
        let (sa, sb) = (schema_a(), schema_b());
        // Warm both relations first, under different schemas.
        let slot = s.submit(scan_req(1)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(1, 1, &sa, &batch_a(&[1]), 0, false));
        drive(&mut s, slot).unwrap();
        let slot = s.submit(scan_req(2)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(2, 1, &sb, &batch_b(&[9]), 0, false));
        drive(&mut s, slot).unwrap();

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
        let d0 = replies[0].1.as_ref().unwrap();
        assert_eq!(d0.columns.len(), 2);
        assert_eq!(d0.pks.to_vec_u128(), vec![10, 11]);
        let d1 = replies[1].1.as_ref().unwrap();
        assert_eq!(d1.columns.len(), 3);
        assert_eq!(d1.pks.to_vec_u128(), vec![20, 21]);
        match &d1.columns[1] {
            ColData::Strings(v) => assert_eq!(v.as_slice(), [Some("s20".into()), Some("s21".into())]),
            _ => panic!("strings"),
        }
        // Each reply carries its own relation's schema, resolved off the cache.
        assert_eq!(replies[0].0.as_ref().unwrap().columns.len(), 2);
        assert_eq!(replies[1].0.as_ref().unwrap().columns.len(), 3);
        assert_eq!(s.interest(), Interest::NONE);
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
        let slot = s.submit(scan_req(3)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(3, 1, &sa, &batch_a(&[5]), 42, false));
        let Reply::Scan((_, _, lsn)) = drive(&mut s, slot).unwrap() else {
            panic!("scan")
        };
        assert_eq!(lsn, 42);
    }

    #[test]
    fn one_read_serves_two_slots_and_leaves_nothing_buffered() {
        let (mut s, peer) = pair();
        let s1 = s.submit(scan_req(1)).unwrap();
        let s2 = s.submit(scan_req(2)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.drain_request();
        let mut both = framed(&reply_ctrl(1, 11));
        both.extend(framed(&reply_ctrl(2, 22)));
        raw_send(&peer.0, &both);
        let done = s.step(Interest::READ).unwrap();
        let ids: Vec<SlotId> = done.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![s1, s2], "in request order");
        for (id, r) in done {
            let Reply::Scan((_, _, lsn)) = r.unwrap() else {
                panic!("scan")
            };
            assert_eq!(lsn, if id == s1 { 11 } else { 22 });
        }
        assert_eq!(s.interest(), Interest::NONE);
    }

    #[test]
    fn status_frames_complete_their_slot_with_the_classified_error() {
        let (mut s, peer) = pair();
        let slot = s.submit(Request::Uncorrelated(reply_ctrl(0, 0))).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_status(STATUS_TXN_CONFLICT, "", 77));
        let r = drive(&mut s, slot);
        assert!(
            matches!(r, Err(ClientError::TxnConflict { fresh_basis: 77 })),
            "the basis rides the frame"
        );
        // A mismatch, a delta expiry: per-slot errors, connection open.
        for (status, want) in [
            (STATUS_SCHEMA_MISMATCH, "mismatch"),
            (STATUS_DELTA_EXPIRED, "expired"),
            (STATUS_NO_INDEX, "no index"),
        ] {
            let slot = s.submit(scan_req(1)).unwrap();
            s.step(Interest::WRITE).unwrap();
            peer.drain_request();
            peer.send(&reply_status(status, "", 0));
            let r = drive(&mut s, slot);
            match (want, r) {
                ("mismatch", Err(ClientError::SchemaMismatch)) => {}
                ("expired", Err(ClientError::DeltaExpired)) => {}
                ("no index", Err(ClientError::ServerError(_))) => {}
                (w, r) => panic!("{w}: {r:?}", r = r.map(|_| ())),
            }
        }
        assert!(!s.closed);
    }

    #[test]
    fn out_of_order_reply_is_a_step_error_and_closes_the_blocking_client() {
        let (mut s, peer) = pair();
        // The peer answers a scan of 5 with a frame naming 6.
        peer.send(&reply_ctrl(6, 1));
        let r = s.scan(5);
        assert!(matches!(r, Err(ClientError::Protocol(_))), "{r:?}", r = r.map(|_| ()));
        assert_eq!(s.interest(), Interest::NONE);
        // The next call reports the connection closed instead of submitting.
        let r = s.scan(5);
        assert!(matches!(r, Err(ClientError::ServerError(ref m)) if m == "connection closed"));
        assert!(matches!(s.submit(scan_req(1)), Err(ClientError::ServerError(_))));
        assert!(s.step(Interest::READ).unwrap().is_empty());
    }

    #[test]
    fn peer_closing_while_parked_completes_the_operation_with_an_error() {
        let (mut s, peer) = pair();
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            drop(peer);
        });
        // EOF, or ECONNRESET when the unread request draws a reset: either way
        // an I/O error, and the connection is closed behind it.
        let r = s.scan(5);
        assert!(
            matches!(r, Err(ClientError::Protocol(ProtocolError::IoError(_)))),
            "{r:?}",
            r = r.map(|_| ())
        );
        assert!(matches!(s.scan(5), Err(ClientError::ServerError(ref m)) if m == "connection closed"));
        h.join().unwrap();
    }

    #[test]
    fn close_abandons_every_pending_slot_and_refuses_further_work() {
        let (mut s, _peer) = pair();
        s.submit(scan_req(1)).unwrap();
        s.submit(scan_req(2)).unwrap();
        assert_eq!(
            s.interest(),
            Interest {
                read: true,
                write: true
            }
        );
        s.close();
        assert_eq!(s.interest(), Interest::NONE);
        assert!(matches!(s.submit(scan_req(3)), Err(ClientError::ServerError(ref m)) if m == "connection closed"));
        assert!(s
            .step(Interest {
                read: true,
                write: true
            })
            .unwrap()
            .is_empty());
    }

    /// The count cap does not bound bytes: an encoded push is a full copy of its
    /// batch, so a driver that never flushes pins memory in proportion to what it
    /// pushed, not to how many times.
    #[test]
    fn queued_bytes_cap_raises_before_the_in_flight_count_does() {
        let (mut s, _peer) = pair();
        let sa = schema_a();
        // 32 bytes a row, so 8192 rows is a 256 KiB frame and the byte cap lands
        // around 256 submits — an order of magnitude under `MAX_IN_FLIGHT`.
        let b = batch_a(&(0..8192).collect::<Vec<u64>>());
        let mut n = 0usize;
        loop {
            let r = s.submit(Request::Push {
                target_id: 4,
                schema: &sa,
                batch: &b,
                mode: WireConflictMode::Update,
            });
            match r {
                Ok(_) => n += 1,
                Err(ClientError::ServerError(m)) => {
                    assert!(m.contains("unwritten bytes queued"), "wrong cap: {m}");
                    break;
                }
                Err(e) => panic!("{e}"),
            }
            assert!(n < MAX_IN_FLIGHT, "the byte cap must bite before the count one");
        }
        assert!(s.queued_bytes() >= MAX_QUEUED_BYTES);
        assert!(s.interest().write, "nothing was flushed, so it is all still queued");

        // Writing gives the budget back — the counter tracks the write cursor, not
        // just the pushes.
        let queued = s.queued_bytes();
        s.step(Interest::WRITE).unwrap();
        assert!(s.queued_bytes() < queued, "a flush releases what it wrote");
    }

    #[test]
    fn in_flight_cap_raises_rather_than_hanging() {
        let (mut s, _peer) = pair();
        let mut last = SlotId(0);
        for _ in 0..MAX_IN_FLIGHT {
            let id = s.submit(scan_req(1)).unwrap();
            assert!(id > last, "monotonic");
            last = id;
        }
        let r = s.submit(scan_req(1));
        assert!(matches!(r, Err(ClientError::ServerError(ref m)) if m.contains("in flight")));
        s.close();
        assert!(s.submit(scan_req(1)).is_err(), "and the cap is not what refuses now");
    }

    /// Deliver `SIGUSR1` to the calling thread `after` from now, with a no-op
    /// handler installed without `SA_RESTART` — how CPython's handlers land — so
    /// a park in `poll(2)` returns `EINTR`.
    fn signal_self_after(after: std::time::Duration) -> std::thread::JoinHandle<()> {
        extern "C" fn noop(_: libc::c_int) {}
        // SAFETY: installing a trivial handler for a signal nothing else in the
        // test binary uses; `pthread_kill` targets a live thread id.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = noop as extern "C" fn(libc::c_int) as usize;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
        }
        let me = unsafe { libc::pthread_self() };
        std::thread::spawn(move || {
            std::thread::sleep(after);
            // SAFETY: the target thread is the test thread, parked in `poll`.
            unsafe { libc::pthread_kill(me, libc::SIGUSR1) };
        })
    }

    #[test]
    fn aborted_park_leaves_the_slot_pending_and_the_next_call_drains_it_first() {
        let (mut s, peer) = pair();
        let fire = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let f = std::sync::Arc::clone(&fire);
        s.set_park_hook(Some(Box::new(move || {
            if f.swap(false, std::sync::atomic::Ordering::Relaxed) {
                Err(ClientError::ServerError("interrupted".into()))
            } else {
                Ok(())
            }
        })));
        let sig = signal_self_after(std::time::Duration::from_millis(100));
        // First scan: the signal interrupts the park, the hook runs and aborts
        // it. The frame is already on the wire, so the server will answer it.
        let r = s.scan(1);
        assert!(matches!(r, Err(ClientError::ServerError(ref m)) if m == "interrupted"));
        sig.join().unwrap();
        assert_eq!(s.interest(), Interest::READ, "the abandoned slot stays pending");
        // The peer answers both requests; the second call must get the second.
        peer.drain_request();
        peer.send(&reply_ctrl(1, 100));
        let h = std::thread::spawn(move || {
            let req = peer.drain_request();
            peer.send(&reply_ctrl(1, 200));
            (peer, req)
        });
        let (_, _, lsn) = s.scan(1).unwrap();
        assert_eq!(lsn, 200);
        let (_peer, _) = h.join().unwrap();
        assert_eq!(s.interest(), Interest::NONE);
    }

    /// The hook is the host's and outlives the session it was installed on — what
    /// [`crate::GnitzClient::reconnect`] relies on when it replaces the connection
    /// and keeps a blocking call Ctrl-C-interruptible.
    #[test]
    fn a_taken_park_hook_moves_to_the_session_that_replaces_it() {
        let (mut old, _old_peer) = pair();
        old.set_park_hook(Some(Box::new(|| Err(ClientError::ServerError("interrupted".into())))));
        let hook = old.take_park_hook();
        assert!(hook.is_some(), "the installed hook comes back out");
        assert!(old.take_park_hook().is_none(), "and taking it leaves none behind");

        // The peer never answers, so the scan parks; the signal makes the park
        // return `EINTR`, which is the one place the hook runs.
        let (mut fresh, _peer) = pair();
        fresh.set_park_hook(hook);
        let sig = signal_self_after(std::time::Duration::from_millis(100));
        let r = fresh.scan(1);
        assert!(
            matches!(r, Err(ClientError::ServerError(ref m)) if m == "interrupted"),
            "the moved hook must abort the park on the new session",
        );
        sig.join().unwrap();
    }

    #[test]
    fn raw_scan_spec_keeps_blocks_undecoded_and_off_the_cache() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let spec = [1u8, 2, 3];
        let slot = s
            .submit(Request::ScanSpec {
                target_id: 9,
                spec: &spec,
                reply_schema: &sa,
                raw: true,
            })
            .unwrap();
        s.step(Interest::WRITE).unwrap();
        let req = peer.drain_request();
        assert!(req.len() > spec.len(), "the spec and reply schema ride the request");
        // Two hint-only frames: the server sends no schema block for a SCAN_SPEC.
        peer.send(&reply_warm(9, 0, &sa, &batch_a(&[1, 2]), true));
        peer.send(&reply_warm(9, 0, &sa, &batch_a(&[3]), false));
        let Reply::Raw { blocks, terminal } = drive(&mut s, slot).unwrap() else {
            panic!("raw")
        };
        assert_eq!(blocks.len(), 2);
        let (b0, _) = crate::protocol::decode_wal_block(blocks[0].block(), &sa).unwrap();
        assert_eq!(b0.pks.to_vec_u128(), vec![1, 2]);
        assert_eq!(terminal.target_id, 9);
        assert_eq!(s.cached_schema_version(9), 0, "a scan_spec absorbs nothing");
    }

    #[test]
    fn push_retries_cold_on_schema_mismatch() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let b = batch_a(&[1]);
        // Warm the cache at version 2, then let the server reject the warm push.
        let slot = s.submit(scan_req(4)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(4, 2, &sa, &b, 0, false));
        drive(&mut s, slot).unwrap();
        let h = std::thread::spawn(move || {
            let warm = peer.drain_request();
            peer.send(&reply_status(STATUS_SCHEMA_MISMATCH, "", 0));
            let cold = peer.drain_request();
            peer.send(&reply_ctrl(4, 555));
            (warm.len(), cold.len(), peer)
        });
        let lsn = s.push_with_mode(4, &sa, &b, WireConflictMode::Update).unwrap();
        assert_eq!(lsn, 555);
        let (warm, cold, _peer) = h.join().unwrap();
        assert!(cold > warm, "the retry carries the schema block the warm frame omitted");
        assert_eq!(s.requests_sent(), 3, "counted once per frame, on enqueue");
    }

    #[test]
    fn a_mismatch_evicts_the_cache_and_the_next_push_goes_out_cold() {
        let (mut s, peer) = pair();
        let sa = schema_a();
        let b = batch_a(&[1]);
        // Warm relation 4 at version 2, so both pushes below encode schema-less.
        let slot = s.submit(scan_req(4)).unwrap();
        s.step(Interest::WRITE).unwrap();
        peer.drain_request();
        peer.send(&reply_cold(4, 2, &sa, &b, 0, false));
        drive(&mut s, slot).unwrap();

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
        assert_eq!(s.cached_schema_version(4), 0, "the entry is gone");

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
        assert_eq!(s.cached_schema_version(4), 5);
        push(&mut s);
        s.step(Interest::WRITE).unwrap();
        assert_eq!(peer.drain_request().len(), warm_len, "warm again");
    }
}
