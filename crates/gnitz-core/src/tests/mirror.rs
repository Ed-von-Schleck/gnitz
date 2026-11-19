//! The mirroring seam under a scripted peer, and under a second [`MirrorStore`]
//! implementor.

use super::*;
use crate::BlockingHost;
use crate::block_on;
use crate::test_support::{
    Peer, client_pair, delta_frame, delta_terminal, encode_frame, interrupt_self_until, kv_rows, kv_schema,
    pushed_train, rel, reply_ctrl, reply_status, sync_answer, sync_header, sync_held,
};
use crate::transport::{Interest, poll_fd};
use gnitz_wire::{ControlHeader, RelDescriptorBlob, TypeCode, framed, peek_control_block};
use std::os::fd::AsFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// The feed tag every cursor here carries.
const TAG: u64 = 0xFEED;

/// One poll of every mirrored view, which must succeed at the call level.
fn poll(client: &mut GnitzClient) -> Vec<PollOutcome> {
    block_on(client.sync(Duration::ZERO)).expect("poll").mirrored
}

// ---------------------------------------------------------------------------
// A recording store
// ---------------------------------------------------------------------------

/// One call the state machine made through the seam.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Register(u64),
    Forget(u64),
    /// `tid` registered again, erased and opened for its whole value.
    Refill(u64),
    /// One block of a refill of `tid`.
    Fill(u64),
    /// `(tid, the round the refill was sealed at)`.
    Seal(u64, u64),
    /// `(tid, the round the copy moved to)`.
    Advance(u64, u64),
}

#[derive(Clone, Default)]
struct Log {
    events: Arc<Mutex<Vec<Ev>>>,
    /// The copy the next `register` retracts before it fails.
    fail_register_retracting: Arc<Mutex<Option<u64>>>,
    /// The copy whose next `fill` fails.
    fail_fill_of: Arc<Mutex<Option<u64>>>,
}

impl Log {
    fn push(&self, e: Ev) {
        self.events.lock().unwrap().push(e);
    }
    fn take(&self) -> Vec<Ev> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
    /// Fail the store's next `register`, once, after it retracted `old`.
    fn fail_next_register_retracting(&self, old: u64) {
        *self.fail_register_retracting.lock().unwrap() = Some(old);
    }
    /// Fail the store's next `fill` of `tid`, once.
    fn fail_next_fill_of(&self, tid: u64) {
        *self.fail_fill_of.lock().unwrap() = Some(tid);
    }
    /// Whether any event so far matches, without draining — for a test that
    /// watches the log while the call under test is still running.
    fn saw(&self, f: impl Fn(&Ev) -> bool) -> bool {
        self.events.lock().unwrap().iter().any(f)
    }
}

/// A store that holds feed positions and a call log — everything the
/// reconciliation reads back through the seam, and nothing else.
struct StubStore {
    cursors: HashMap<u64, DeltaCursor>,
    log: Log,
}

impl MirrorStore for StubStore {
    fn register(&mut self, _name: &RelName, desc: &RelDescriptor) -> Result<(), String> {
        self.log.push(Ev::Register(desc.tid));
        if let Some(old) = self.log.fail_register_retracting.lock().unwrap().take() {
            self.cursors.remove(&old);
            return Err("stub: the copy could not be entered".into());
        }
        Ok(())
    }

    fn forget(&mut self, tid: u64) -> Result<(), String> {
        self.log.push(Ev::Forget(tid));
        self.cursors.remove(&tid);
        Ok(())
    }

    fn refill(&mut self, _name: &RelName, desc: &RelDescriptor) -> Result<(), String> {
        self.log.push(Ev::Refill(desc.tid));
        self.cursors.remove(&desc.tid);
        Ok(())
    }

    fn fill(&mut self, tid: u64, blocks: &[&[u8]], sealed_at: Option<DeltaCursor>) -> Result<(), String> {
        let failing = self.log.fail_fill_of.lock().unwrap().take_if(|failing| *failing == tid);
        if failing.is_some() {
            return Err("stub: the block could not be applied".into());
        }
        blocks.iter().for_each(|_| self.log.push(Ev::Fill(tid)));
        if let Some(cursor) = sealed_at {
            self.log.push(Ev::Seal(tid, cursor.tick.get()));
            self.cursors.insert(tid, cursor);
        }
        Ok(())
    }

    fn advance(&mut self, tid: u64, _b: &[&[u8]], next: DeltaCursor) -> Result<(), String> {
        if !self.cursors.contains_key(&tid) {
            return Err(format!("stub: advance of {tid}, which holds no cursor"));
        }
        self.log.push(Ev::Advance(tid, next.tick.get()));
        self.cursors.insert(tid, next);
        Ok(())
    }

    fn scan_spec(
        &mut self,
        _tid: u64,
        _spec: gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<ZSetBatch, String> {
        Ok(ZSetBatch::new(reply_schema))
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.cursors.get(&tid).copied()
    }

    fn checkpoint(&mut self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The scripted peer
// ---------------------------------------------------------------------------

/// How long a request that is on its way may take. Only ever paid in full by a
/// regression, which then fails rather than hangs.
///
/// A script's peer is dropped when the script ends, so a request it does not
/// expect fails fast instead of parking the client; each test counts
/// `requests_sent` to catch one.
const PATIENCE: Duration = Duration::from_secs(5);

impl Peer {
    /// The next request frame, which must already be on its way.
    fn expect_frame(&self, what: &str) -> Vec<u8> {
        let ready = poll_fd(self.0.as_fd(), Interest::READ, Some(Instant::now() + PATIENCE));
        assert!(ready.is_ok_and(|ready| ready.read), "{what}: the request never arrived");
        self.recv()
    }

    /// The target id of the next request.
    fn expect_request(&self, what: &str) -> u64 {
        let frame = self.expect_frame(what);
        peek_control_block(&frame).expect("a control header").hdr.target_id
    }

    /// The view ids the next request, a DELTA_POLL, names in request order.
    fn expect_poll(&self, what: &str) -> Vec<u64> {
        self.expect_kept(what).into_iter().map(|(tid, _)| tid).collect()
    }

    /// The next request, a DELTA_POLL: each view it names, in request order,
    /// with the id it asks to keep that view's subscription under.
    fn expect_kept(&self, what: &str) -> Vec<(u64, SubId)> {
        let frame = self.expect_frame(what);
        let ctrl = peek_control_block(&frame).expect("a control header");
        assert_eq!(ctrl.hdr.flags.verb, Some(gnitz_wire::ClientVerb::DeltaPoll), "{what}");
        let poll =
            gnitz_wire::multi_frame::decode_delta_poll(&frame[ctrl.body.clone()], &ctrl.hdr).expect("a delta poll");
        let kept = poll
            .kept()
            .unwrap_or_else(|| panic!("{what}: a poll keeping its views"));
        poll.items
            .iter()
            .zip(kept)
            .map(|(item, id)| (item.view.tid, id))
            .collect()
    }

    /// The subscription ids the next request, a SYNC_PUSHED, names as held,
    /// in ascending order.
    fn expect_sync_naming(&self, what: &str) -> Vec<SubId> {
        sync_held(&self.expect_frame(what))
    }

    /// Send [`delta_terminal`]: one view's answer inside a poll.
    fn reply_watermark(&self, target_id: u64, tag: u64, tick: u64) {
        self.send(&delta_terminal(target_id, tag, tick));
    }

    /// The next request, which must be a SYNC_PUSHED: its control header.
    fn expect_sync(&self, what: &str) -> ControlHeader {
        sync_header(&self.expect_frame(what))
    }

    /// One [`pushed_train`] of subscription `sub` to `view`: `rows`, ending at
    /// round `tick` — or, with no round, in a fault.
    fn push_train(&self, view: u64, sub: SubId, rows: &[(u64, i64, i64)], tick: Option<u64>) {
        let end = tick.map(|tick| (TAG, tick)).ok_or("lagged");
        self.send_bytes(&pushed_train(view, sub, &kv_rows(rows), end));
    }

    /// A RESOLVE answering with `tid`: a view descriptor carrying a feed.
    fn reply_resolved(&self, tid: u64) {
        let schema = kv_schema(TypeCode::I64);
        let record = crate::test_support::named_record(&schema);
        let blob = RelDescriptorBlob {
            class: RelClass::FedView,
            pk_repeats: false,
            serial: false,
            indexes: vec![],
            schema: &record,
        };
        let hdr = ControlHeader::ack(tid, 0);
        self.send(&encode_frame(hdr, &blob.encode(), None));
    }
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// A client on a scripted peer with `views` mirrored through `bind`: each
/// `(tid, name, tick)` is registered under `s.name`, confirmed, and, at a
/// non-zero tick, holds a feed position. Nothing goes over the wire.
fn fixture(views: &[(u64, &str, u64)]) -> (GnitzClient, Peer, Log) {
    let (mut client, peer) = client_pair();
    let log = Log::default();
    let cursors = views
        .iter()
        .filter_map(|&(tid, _, tick)| Some((tid, DeltaCursor::from_pair(TAG, tick)?)))
        .collect();
    let store = StubStore { cursors, log: log.clone() };
    block_on(client.attach_mirror(move || Ok(store))).unwrap();
    let schema = kv_schema(TypeCode::I64);
    for &(tid, name, _) in views {
        let desc = RelDescriptor {
            tid,
            class: RelClass::FedView,
            pk_repeats: false,
            serial: false,
            schema: Arc::clone(&schema),
            indexes: Vec::new(),
            token: 0,
        };
        let entry = MirroredView {
            name: rel("s", name),
            upstream: Target::from(&desc),
            desc: Arc::new(desc),
            spec: gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None).encode(),
            plan: None,
            confirmed: false,
            link: Link::Due,
            owed_reseed: false,
        };
        block_on(client.bind(entry)).unwrap();
        client.mirror.as_mut().unwrap().views.get_mut(&tid).unwrap().confirmed = true;
    }
    assert!(
        log.take().iter().all(|e| matches!(e, Ev::Register(_))),
        "registration tears nothing down"
    );
    (client, peer, log)
}

/// Put `host` in place of `client`'s, attached to its session.
fn rehost(client: &mut GnitzClient, host: BlockingHost) {
    client.host = Box::new(host);
    client
        .host
        .attach(client.session.as_fd().try_clone_to_owned().unwrap())
        .unwrap();
}

// ---------------------------------------------------------------------------
// Frame count
// ---------------------------------------------------------------------------

/// One poll names every view in one request, however many there are — a wait
/// is the request's, so views split across two would wait separately — and
/// every view advances.
#[test]
fn one_poll_writes_one_request_for_every_view() {
    for m in [6usize, 200] {
        let names: Vec<String> = (0..m).map(|i| format!("v{i}")).collect();
        let views: Vec<(u64, &str, u64)> = names
            .iter()
            .enumerate()
            .map(|(i, n)| (100 + i as u64, n.as_str(), 4))
            .collect();
        let (mut client, peer, _log) = fixture(&views);

        let h = std::thread::spawn(move || {
            let ids = peer.expect_poll("the poll");
            for &id in &ids {
                peer.reply_watermark(id, TAG, 9);
            }
            ids
        });
        let report = poll(&mut client);
        let seen = h.join().unwrap();

        assert_eq!(
            client.requests_sent(),
            1,
            "{m} views: one poll reads every copy and keeps each subscribed"
        );
        let mut ids = seen;
        ids.sort_unstable();
        assert_eq!(
            ids,
            (100..100 + m as u64).collect::<Vec<_>>(),
            "every view rides a request"
        );
        assert_eq!(report.len(), m, "one entry per view");
        assert!(
            report
                .iter()
                .all(|o| matches!(o.result, PollResult::Advanced) && o.cursor.is_some_and(|c| c.tick.get() == 9)),
            "{m} views: {report:?}"
        );
    }
}

/// A refused registration pays exactly one RESOLVE, a refused cursor a whole
/// read and no RESOLVE, and every other refusal nothing — a dead socket or a
/// poisoned reply stops buying a second doomed round trip.
#[test]
fn only_a_stale_registration_pays_a_resolve() {
    for status in [WireStatus::StaleCatalog, WireStatus::DeltaExpired, WireStatus::Error] {
        let (mut client, peer, _log) = fixture(&[(7, "v", 4)]);
        let h = std::thread::spawn(move || {
            assert_eq!(peer.expect_poll("the poll"), vec![7]);
            peer.send(&reply_status(7, status, "refused"));
            match status {
                WireStatus::StaleCatalog => {
                    // "No such relation", so the view fails as not found.
                    assert_eq!(peer.expect_request("the re-resolve"), 0);
                    peer.send(&reply_ctrl(0, 0));
                }
                WireStatus::DeltaExpired => {
                    assert_eq!(peer.expect_poll("the whole read"), vec![7]);
                    peer.reply_watermark(7, TAG, 20);
                }
                _ => {}
            }
        });
        let report = poll(&mut client);
        h.join().unwrap();
        let (requests, reseeded) = match status {
            WireStatus::StaleCatalog => (2, false),
            WireStatus::DeltaExpired => (2, true),
            _ => (1, false),
        };
        assert_eq!(client.requests_sent(), requests, "status {status:?}");
        let [o] = report.as_slice() else {
            panic!("status {status:?}: got {report:?}");
        };
        assert_eq!(o.view_id, 7);
        assert_eq!(o.result.reseeded(), reseeded, "status {status:?}: got {report:?}");
        assert_eq!(
            matches!(o.result, PollResult::Failed(_)),
            !reseeded,
            "status {status:?}: got {report:?}"
        );
        assert!(
            client.cursor_of(7).is_some(),
            "status {status:?}: a failed poll leaves the copy answering its last round",
        );
    }
}

/// A bootstrap's first block reaches the store before its terminal is sent.
#[test]
fn a_bootstrap_fills_the_store_frame_by_frame() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0)]);
    let watched = log.clone();
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the bootstrap"), vec![7]);
        peer.send(&delta_frame(7, Some(&kv_rows(&[(1, 10, 1)])), None));
        let until = Instant::now() + PATIENCE;
        while !watched.saw(|e| matches!(e, Ev::Fill(7))) {
            assert!(Instant::now() < until, "the first block never reached the store");
            std::thread::sleep(Duration::from_millis(1));
        }
        peer.send(&delta_frame(7, Some(&kv_rows(&[(2, 20, 1)])), Some((TAG, 20))));
    });
    let report = poll(&mut client);
    h.join().unwrap();
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 7 && o.result.reseeded()),
        "{report:?}"
    );
    assert_eq!(log.take(), [Ev::Refill(7), Ev::Fill(7), Ev::Fill(7), Ev::Seal(7, 20)]);
}

/// Copies holding no cursor are read whole in the request that advances the
/// rest, however many they are.
#[test]
fn copies_without_a_cursor_are_read_whole_in_the_one_request() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0), (8, "b", 0), (9, "c", 4)]);
    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        for &id in &ids {
            peer.reply_watermark(id, TAG, 20);
        }
        ids.len()
    });
    let report = poll(&mut client);
    assert_eq!(h.join().unwrap(), 3, "every view rides the request");
    assert_eq!(client.requests_sent(), 1);
    let mut reseeded: Vec<(u64, bool)> = report.iter().map(|o| (o.view_id, o.result.reseeded())).collect();
    reseeded.sort_unstable();
    assert_eq!(reseeded, [(7, true), (8, true), (9, false)], "{report:?}");
    let mut events = log.take();
    events.sort_unstable_by_key(|e| format!("{e:?}"));
    assert_eq!(
        events,
        [
            Ev::Advance(9, 20),
            Ev::Refill(7),
            Ev::Refill(8),
            Ev::Seal(7, 20),
            Ev::Seal(8, 20),
        ],
    );
}

/// A block the store refuses fails the view it is of and nothing else: the
/// rest of that view's train is dropped, the views behind it are read, and the
/// subscription the request asked to keep for it is one the next sync does not
/// name.
#[test]
fn a_refused_block_fails_its_view_and_no_other() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0), (8, "b", 4)]);
    log.fail_next_fill_of(7);
    let h = std::thread::spawn(move || {
        let mut kept = None;
        for (tid, id) in peer.expect_kept("the poll") {
            if tid == 7 {
                peer.send(&delta_frame(7, Some(&kv_rows(&[(1, 10, 1)])), None));
                peer.send(&delta_frame(7, Some(&kv_rows(&[(2, 20, 1)])), None));
            } else {
                kept = Some(id);
            }
            peer.reply_watermark(tid, TAG, 20);
        }
        (peer, kept)
    });
    let report = poll(&mut client);
    let (peer, kept) = h.join().unwrap();
    drop(client.begin_sync(Duration::ZERO));
    client.session.step(crate::Interest::WRITE);
    assert_eq!(
        peer.expect_sync_naming("the next sync"),
        [kept.unwrap()],
        "8's, and not 7's"
    );
    let failed: Vec<u64> = report
        .iter()
        .filter(|o| matches!(o.result, PollResult::Failed(ClientError::Mirror(_))))
        .map(|o| o.view_id)
        .collect();
    assert_eq!(failed, [7], "{report:?}");
    assert!(log.saw(|e| *e == Ev::Advance(8, 20)), "the view behind it is read");
    assert!(!log.saw(|e| matches!(e, Ev::Fill(7) | Ev::Seal(7, _))));
}

/// A whole read whose train ends refused in the same step as its blocks
/// leaves the copy alone: nothing is registered, erased or filled for it.
#[test]
fn a_whole_read_refused_mid_train_leaves_the_copy() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0)]);
    log.take();
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the poll"), vec![7]);
        let mut wire = framed(&delta_frame(7, Some(&kv_rows(&[(1, 10, 1)])), None));
        wire.extend(framed(&reply_status(7, WireStatus::Error, "refused")));
        peer.send_bytes(&wire);
        peer
    });
    let report = poll(&mut client);
    let _peer = h.join().unwrap();
    assert!(
        matches!(
            report[..],
            [PollOutcome {
                view_id: 7,
                result: PollResult::Failed(_),
                ..
            }]
        ),
        "{report:?}"
    );
    assert!(!log.saw(|e| matches!(e, Ev::Register(7) | Ev::Refill(7) | Ev::Fill(7) | Ev::Seal(7, _))));
}

/// A train pushed in the same read as a whole read's terminal is part of the
/// copy: it is sealed at the pushed cursor with the pushed rows filled behind
/// the read's.
#[test]
fn a_train_pushed_behind_a_whole_read_is_sealed_with_it() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0)]);
    let h = std::thread::spawn(move || {
        let [(7, id)] = peer.expect_kept("the bootstrap")[..] else {
            panic!("one view, 7");
        };
        let mut wire = framed(&delta_frame(7, Some(&kv_rows(&[(1, 10, 1)])), None));
        wire.extend(framed(&delta_terminal(7, TAG, 20)));
        wire.extend(pushed_train(7, id, &kv_rows(&[(2, 20, 1)]), Ok((TAG, 25))));
        peer.send_bytes(&wire);
    });
    poll(&mut client);
    h.join().unwrap();
    assert_eq!(log.take(), [Ev::Refill(7), Ev::Fill(7), Ev::Fill(7), Ev::Seal(7, 25)]);
    assert_eq!(client.cursor_of(7).map(|c| c.tick.get()), Some(25));
}

/// A delta read abandoned mid-train applies none of its rows: the copy stays
/// at its cursor, the rest of the train and what is pushed for the id it asked
/// to keep are dropped, and the next poll reads it again from that cursor.
#[test]
fn a_read_abandoned_mid_train_applies_nothing() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4)]);
    let reading = Arc::new(AtomicBool::new(false));
    let watched = Arc::clone(&reading);
    rehost(
        &mut client,
        BlockingHost::with_hook(move || {
            if watched.load(Ordering::Relaxed) {
                Err("interrupted".into())
            } else {
                Ok(())
            }
        }),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));
    let h = std::thread::spawn(move || {
        let [(7, id)] = peer.expect_kept("the read")[..] else {
            panic!("one view, 7");
        };
        // The train is never ended, so the call parks and is interrupted.
        peer.send(&delta_frame(7, Some(&kv_rows(&[(1, 10, 1)])), None));
        reading.store(true, Ordering::Relaxed);
        (peer, id)
    });
    let r = block_on(client.sync(Duration::ZERO)).map(|s| s.mirrored);
    assert!(matches!(r, Err(ClientError::Interrupted(_))), "{r:?}");
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    rehost(&mut client, BlockingHost::default());
    let (peer, abandoned) = h.join().unwrap();
    assert!(log.take().is_empty(), "no row of the train is applied");
    assert_eq!(client.cursor_of(7).map(|c| c.tick.get()), Some(4));

    let mut wire = framed(&delta_frame(7, Some(&kv_rows(&[(2, 20, 1)])), Some((TAG, 11))));
    wire.extend(pushed_train(7, abandoned, &kv_rows(&[(3, 30, 1)]), Ok((TAG, 12))));
    peer.send_bytes(&wire);
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_request("an id allocation"), 0);
        peer.send(&reply_ctrl(0, 4242));
        assert_eq!(
            peer.expect_sync_naming("the next sync"),
            [],
            "the abandoned id is not held"
        );
        peer.send(&sync_answer(None));
        let [(7, id)] = peer.expect_kept("the read again")[..] else {
            panic!("one view, 7");
        };
        peer.reply_watermark(7, TAG, 13);
        id
    });
    // Behind the rest of the abandoned train and the pushed one.
    assert_eq!(block_on(client.alloc_ids(1)).unwrap(), 4242);
    let gone = client.session.take_pushed(abandoned).expect_err("dropped");
    assert!(gone.to_string().contains("not held"), "{gone:?}");
    poll(&mut client);
    assert_ne!(h.join().unwrap(), abandoned);
    assert_eq!(log.take(), [Ev::Advance(7, 13)], "read again from its own cursor");
}

// ---------------------------------------------------------------------------
// Slot correlation
// ---------------------------------------------------------------------------

/// A leftover delta poll from an aborted call does not misdirect a reply.
///
/// Both views carry the same schema, so a block delivered to the wrong one would
/// decode cleanly and apply the other's rows and weights with no error. What
/// separates them here is the round each reply names.
#[test]
fn a_leftover_poll_does_not_shift_the_replies() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
    // A poll submitted and its reader gone: what an aborting park leaves behind.
    let from = DeltaCursor::from_pair(TAG, 4);
    let abandoned = DeltaPollItem {
        view: 7.into(),
        from,
        reply_layout: kv_schema(TypeCode::I64).layout().layout_digest(),
        spec: &[],
    };
    drop(DeltaPoll::start(
        &mut client.session,
        &mut client.next_sub,
        &[abandoned],
    ));

    let h = std::thread::spawn(move || {
        let abandoned = peer.expect_kept("the abandoned poll");
        assert_eq!(abandoned.iter().map(|&(tid, _)| tid).collect::<Vec<_>>(), [7]);
        let ids = peer.expect_poll("the poll");
        // The abandoned train first, then one terminal per view, each at a round
        // derived from the view that asked — so a shifted reply lands visibly
        // wrong.
        peer.reply_watermark(7, TAG, 50);
        for &tid in &ids {
            peer.reply_watermark(tid, TAG, 100 + tid);
        }
    });
    poll(&mut client);
    h.join().unwrap();

    let mut applied = log.take();
    applied.sort_unstable_by_key(|e| format!("{e:?}"));
    assert_eq!(
        applied,
        [Ev::Advance(7, 107), Ev::Advance(8, 108)],
        "each view took the reply its own request opened, and nothing else",
    );
}

// ---------------------------------------------------------------------------
// Round ordering
// ---------------------------------------------------------------------------

/// No recovery reaches the store until every train of its round is applied: a
/// recovery can re-point a registration at a view whose reply is still in hand,
/// and that reply applied after the recovery's own fetch doubles its weights.
#[test]
fn no_recovery_runs_before_every_ingest_has() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);

    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        // The first position's view is the one that no longer resolves.
        peer.send(&reply_status(ids[0], WireStatus::StaleCatalog, "stale"));
        peer.reply_watermark(ids[1], TAG, 11);
        // The recovery, and the round it leaves a view to sync in.
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(9);
        assert_eq!(peer.expect_poll("the next round"), vec![9]);
        peer.reply_watermark(9, TAG, 20);
        (ids[0], ids[1])
    });
    let report = poll(&mut client);
    let (gone, alive) = h.join().unwrap();
    assert_eq!(
        client.requests_sent(),
        3,
        "both views ride one request, and the moved one is read whole in another"
    );

    let events = log.take();
    let first_ingest = events
        .iter()
        .position(|e| matches!(e, Ev::Advance(t, 11) if *t == alive))
        .expect("the round ingested the reply it had");
    let first_recovery = events.iter().position(|e| !matches!(e, Ev::Advance(..)));
    assert!(
        first_recovery.is_some_and(|t| t > first_ingest),
        "a recovery ran before its round finished ingesting: {events:?}",
    );
    assert!(
        events.contains(&Ev::Seal(9, 20)) && !events.iter().any(|e| matches!(e, Ev::Refill(t) if *t == gone)),
        "the moved view is read whole at its new id: {events:?}",
    );
    let mut ids: Vec<u64> = report.iter().map(|o| o.view_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![alive, 9], "one entry per view, the moved one at its new id");
}

/// A recovery onto an id that already holds a cursor advances it instead of
/// erasing a good copy: the refused registration's name now resolves to a view
/// this same poll already advanced.
#[test]
fn a_reseed_onto_a_live_copy_does_not_erase_it() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0), (8, "b", 4)]);

    let h = std::thread::spawn(move || {
        let mut kept = None;
        for (tid, id) in peer.expect_kept("the poll") {
            match tid {
                8 => {
                    kept = Some(id);
                    peer.reply_watermark(8, TAG, 11)
                }
                _ => peer.send(&reply_status(7, WireStatus::StaleCatalog, "stale")),
            }
        }
        // The registration lands on 8, whose copy is advanced from where the
        // first round left it rather than re-read, under a subscription of
        // its own: the replaced registration's is nobody's.
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(8);
        let next = peer.expect_kept("the next round");
        assert!(matches!(next[..], [(8, id)] if Some(id) != kept), "{next:?}");
        peer.reply_watermark(8, TAG, 12);
    });
    let report = poll(&mut client);
    h.join().unwrap();
    assert_eq!(client.requests_sent(), 3, "the poll, the resolve, the next round");

    let events = log.take();
    assert!(
        !events.iter().any(|e| matches!(e, Ev::Refill(8) | Ev::Seal(8, _))),
        "the live copy must be neither erased nor re-read whole: {events:?}",
    );
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 8 && o.cursor.is_some_and(|c| c.tick.get() == 12)),
        "one entry, at the id the view ended up under: {report:?}",
    );
    assert_eq!(client.mirrored_ids(), [8], "the retired registration is gone");
}

/// A recovery that lands on a view whose own poll was refused as expired reads
/// it whole once, whichever of the two refusals is taken first.
#[test]
fn a_recovery_onto_an_expired_view_reads_it_whole_once() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);

    let h = std::thread::spawn(move || {
        for id in peer.expect_poll("the poll") {
            let status = if id == 7 {
                WireStatus::StaleCatalog
            } else {
                WireStatus::DeltaExpired
            };
            peer.send(&reply_status(id, status, "refused"));
        }
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(8);
        assert_eq!(peer.expect_poll("the whole read"), vec![8]);
        peer.reply_watermark(8, TAG, 20);
    });
    let report = poll(&mut client);
    h.join().unwrap();
    assert_eq!(client.requests_sent(), 3, "the poll, the resolve, the whole read");

    let events = log.take();
    let whole: Vec<&Ev> = events
        .iter()
        .filter(|e| matches!(e, Ev::Refill(_) | Ev::Seal(..)))
        .collect();
    assert_eq!(whole, [&Ev::Refill(8), &Ev::Seal(8, 20)], "{events:?}");
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 8 && o.result.reseeded()),
        "{report:?}"
    );
}

/// A recovery that moves a registration and then fails is reported at the id
/// the view is mirrored under, which is the one `forget_view` takes.
#[test]
fn a_recovery_that_moves_and_then_fails_is_reported_at_the_new_id() {
    let (mut client, peer, _log) = fixture(&[(7, "a", 4)]);

    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the poll"), vec![7]);
        peer.send(&reply_status(7, WireStatus::StaleCatalog, "stale"));
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(9);
        // The connection goes with the whole read unanswered.
        assert_eq!(peer.expect_poll("the whole read"), vec![9]);
    });
    let report = poll(&mut client);
    h.join().unwrap();

    assert!(
        matches!(
            report.as_slice(),
            [o] if o.view_id == 9 && o.cursor.is_none() && matches!(o.result, PollResult::Failed(ClientError::ConnectionLost(_)))
        ),
        "{report:?}"
    );
    assert_eq!(client.mirrored_ids(), [9]);
    assert!(client.cursor_of(9).is_none(), "a copy never synced answers no read");
}

/// A registration whose store record went with a `register` that failed after
/// its retraction is entered again by the next poll's whole read.
#[test]
fn a_registration_the_store_lost_is_entered_again_by_the_next_poll() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4)]);
    log.fail_next_register_retracting(7);

    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the poll"), vec![7]);
        peer.send(&reply_status(7, WireStatus::StaleCatalog, "stale"));
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(9);
        peer
    });
    let report = poll(&mut client);
    let peer = h.join().unwrap();
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 7 && matches!(o.result, PollResult::Failed(ClientError::Mirror(_)))),
        "{report:?}"
    );
    assert_eq!(log.take(), [Ev::Register(9)]);

    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the whole read"), vec![7]);
        peer.reply_watermark(7, TAG, 20);
    });
    let report = poll(&mut client);
    h.join().unwrap();
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 7 && o.result.reseeded()),
        "{report:?}"
    );
    assert_eq!(log.take(), [Ev::Refill(7), Ev::Seal(7, 20)]);
}

/// An interrupted poll discards its report, so a reseed it ran is still owed:
/// the next poll reports that view `Reseeded`, and the one after that does not.
#[test]
fn an_interrupted_poll_reannounces_its_reseed() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0), (8, "b", 4)]);
    // As a view never read on this connection stands.
    client.mirror.as_mut().unwrap().views.get_mut(&7).unwrap().confirmed = false;
    let watched = log.clone();
    rehost(
        &mut client,
        BlockingHost::with_hook(move || {
            if watched.saw(|e| matches!(e, Ev::Seal(7, _))) {
                Err("interrupted".into())
            } else {
                Ok(())
            }
        }),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));

    let h = std::thread::spawn(move || {
        // The round reads the cursor-less view whole before any recovery.
        for id in peer.expect_poll("the poll") {
            match id {
                7 => peer.reply_watermark(7, TAG, 20),
                _ => peer.send(&reply_status(8, WireStatus::StaleCatalog, "stale")),
            }
        }
        // 8's re-resolve is never answered, so the call parks and is interrupted.
        peer.expect_request("the re-resolve");
        peer
    });
    let r = block_on(client.sync(Duration::ZERO)).map(|s| s.mirrored);
    assert!(matches!(r, Err(ClientError::Interrupted(_))), "{r:?}");
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    rehost(&mut client, BlockingHost::default());
    let peer = h.join().unwrap();

    let h = std::thread::spawn(move || {
        // The abandoned re-resolve.
        peer.send(&reply_ctrl(0, 0));
        // 7's whole read left it subscribed, so the first poll is a sync and
        // 8's delta read; after it both are subscribed, and the next is a sync.
        peer.expect_sync("the first poll");
        peer.send(&sync_answer(None));
        assert_eq!(peer.expect_poll("a poll"), vec![8]);
        peer.reply_watermark(8, TAG, 21);
        peer.expect_sync("the second poll");
        peer.send(&sync_answer(None));
    });
    let mut results = Vec::new();
    for _ in 0..2 {
        let mut report: Vec<(u64, bool)> = poll(&mut client)
            .iter()
            .map(|o| (o.view_id, o.result.reseeded()))
            .collect();
        report.sort_unstable();
        results.push(report);
    }
    h.join().unwrap();
    assert_eq!(
        results,
        [vec![(7, true), (8, false)], vec![(7, false), (8, false)]],
        "the owed reseed is announced once, on the next report that reaches a caller",
    );
    assert!(
        client.cursor_of(7).is_some(),
        "7 was read whole and advanced since: its copy answers"
    );
    assert!(
        client.cursor_of(8).is_some(),
        "8 was read and subscribed: its copy answers"
    );
}

/// A poll reply that goes wrong part-way fails every view it did not answer
/// rather than raising, and ingests nothing under them: a terminal naming the
/// wrong view (both share a schema, so a misdirected block would decode
/// cleanly), a batch cut short by the peer going away, and a fault naming no
/// view, which fails the request but not the connection.
#[test]
fn a_broken_poll_reply_fails_the_views_it_never_answered() {
    type Script = fn(&Peer, &[u64]);
    let cases: [(&str, Script, usize, bool); 3] = [
        ("misdirected", |p, ids| p.reply_watermark(ids[1], TAG, 11), 0, true),
        ("short", |p, ids| p.reply_watermark(ids[0], TAG, 11), 1, true),
        (
            "a request-level fault",
            |p, _| p.send(&reply_status(0, WireStatus::Error, "refused")),
            0,
            false,
        ),
    ];
    for (what, script, answered, closes) in cases {
        let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
        let h = std::thread::spawn(move || {
            let ids = peer.expect_poll("the poll");
            script(&peer, &ids);
            ids
        });
        let report = poll(&mut client);
        let ids = h.join().unwrap();

        for (i, id) in ids.iter().enumerate() {
            let o = report.iter().find(|o| o.view_id == *id).expect("an entry per view");
            if i < answered {
                assert!(
                    matches!(o.result, PollResult::Advanced) && o.cursor.is_some_and(|c| c.tick.get() == 11),
                    "{what}: the view that was answered keeps its round: {report:?}",
                );
            } else {
                assert!(matches!(o.result, PollResult::Failed(_)), "{what}: {report:?}");
                assert!(
                    !log.saw(|e| matches!(e, Ev::Seal(t, _) | Ev::Advance(t, _) if t == id)),
                    "{what}: nothing is ingested under an unanswered view",
                );
            }
        }
        assert_eq!(client.session.is_closed(), closes, "{what}");
    }
}

/// A view's blocks are ingested as its terminal arrives, not once the whole
/// batch has been read — which is what keeps one train resident where M would
/// otherwise be.
#[test]
fn each_view_is_ingested_before_the_next_one_is_answered() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
    let watched = log.clone();

    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        peer.reply_watermark(ids[0], TAG, 11);
        // Nothing else is on the wire, so an ingest seen before the second
        // terminal goes out ran off the first position's.
        let deadline = Instant::now() + PATIENCE;
        while !watched.saw(|e| matches!(e, Ev::Seal(t, _) | Ev::Advance(t, _) if *t == ids[0])) {
            assert!(
                Instant::now() < deadline,
                "the first view's blocks must reach the store before the second view is answered",
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        peer.reply_watermark(ids[1], TAG, 12);
    });
    let report = poll(&mut client);
    h.join().unwrap();

    assert!(report.iter().all(|o| matches!(o.result, PollResult::Advanced)));
}

/// A dead connection fails **every view's** poll with the loss rather than
/// raising or parking — whether the poll is what finds it dead, or an earlier
/// verb already did. The copies are intact, and every entry is worth reading.
#[test]
fn a_dead_connection_fails_each_view_rather_than_the_call() {
    for lost_before in [false, true] {
        let (mut client, peer, _log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
        drop(peer);
        if lost_before {
            let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None);
            let r = block_on(client.scan_served(1, &spec, &kv_schema(TypeCode::I64)));
            assert!(matches!(r, Err(ClientError::ConnectionLost(_))), "{r:?}");
        }

        let report = poll(&mut client);
        let mut ids: Vec<u64> = report.iter().map(|o| o.view_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![7, 8], "one entry per view: {report:?}");
        assert!(
            report
                .iter()
                .all(|o| matches!(o.result, PollResult::Failed(ClientError::ConnectionLost(_)))),
            "lost before: {lost_before}: {report:?}",
        );
        assert!(
            client.cursor_of(7).is_some() && client.cursor_of(8).is_some(),
            "and both copies still answer at the round they reached",
        );
    }
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

/// Two copies read by one poll, which left each subscribed: the id each view
/// is subscribed under, by view.
fn subscribed() -> (GnitzClient, Peer, Log, HashMap<u64, SubId>) {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
    let h = std::thread::spawn(move || {
        let kept = peer.expect_kept("the poll");
        for &(id, _) in &kept {
            peer.reply_watermark(id, TAG, 9);
        }
        (peer, kept.into_iter().collect())
    });
    poll(&mut client);
    let (peer, subs) = h.join().unwrap();
    log.take();
    (client, peer, log, subs)
}

/// Once its copy is subscribed a poll is one request whatever it carries: the
/// trains pushed ahead of its answer are applied as one, and a view sent a
/// train with no rows moves to the round it ends at.
#[test]
fn a_subscribed_copy_is_advanced_by_what_was_pushed() {
    let (mut client, peer, log, subs) = subscribed();
    let h = std::thread::spawn(move || {
        peer.expect_sync("the poll");
        peer.push_train(7, subs[&7], &[(1, 10, 1)], Some(11));
        peer.push_train(7, subs[&7], &[(1, 10, -1)], Some(12));
        peer.push_train(8, subs[&8], &[], Some(12));
        peer.send(&sync_answer(None));
    });
    let before = client.requests_sent();
    let mut report: Vec<(u64, u64)> = poll(&mut client)
        .iter()
        .map(|o| {
            assert!(matches!(o.result, PollResult::Advanced), "{o:?}");
            (o.view_id, o.cursor.expect("a copy that answers").tick.get())
        })
        .collect();
    h.join().unwrap();
    report.sort_unstable();
    assert_eq!(client.requests_sent() - before, 1, "one request for both views");
    assert_eq!(report, [(7, 12), (8, 12)]);
    let mut applied = log.take();
    applied.sort_unstable_by_key(|e| format!("{e:?}"));
    assert_eq!(applied, [Ev::Advance(7, 12), Ev::Advance(8, 12)]);
}

/// A train pushed behind another request's reply waits for the poll that
/// applies it, and misdirects nothing.
#[test]
fn a_train_behind_another_reply_waits_for_its_poll() {
    let (mut client, peer, log, subs) = subscribed();
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_request("an id allocation"), 0);
        peer.send(&reply_ctrl(0, 4242));
        peer.push_train(8, subs[&8], &[(2, 20, 1)], Some(15));
        peer.expect_sync("the poll");
        peer.send(&sync_answer(None));
    });
    assert_eq!(block_on(client.alloc_ids(1)).unwrap(), 4242);
    assert!(log.take().is_empty(), "a copy moves only in a poll");
    poll(&mut client);
    h.join().unwrap();
    assert_eq!(log.take(), [Ev::Advance(8, 15)], "a view sent nothing does not move");
}

/// A subscription the server ended is continued by a delta read from the
/// copy's own cursor in the same poll, which keeps it subscribed again; the
/// other view's is untouched.
#[test]
fn a_subscription_that_ended_is_continued_by_a_delta_read() {
    let (mut client, peer, log, subs) = subscribed();
    let h = std::thread::spawn(move || {
        peer.expect_sync("the poll");
        peer.push_train(7, subs[&7], &[], None);
        peer.send(&sync_answer(None));
        let again = peer.expect_kept("the delta read");
        assert!(matches!(again[..], [(7, id)] if id != subs[&7]), "{again:?}");
        peer.reply_watermark(7, TAG, 13);
    });
    let before = client.requests_sent();
    let report = poll(&mut client);
    h.join().unwrap();
    assert_eq!(client.requests_sent() - before, 2, "the sync, then the delta read");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "an ended subscription is nobody's failure: {report:?}"
    );
    assert_eq!(log.take(), [Ev::Advance(7, 13)]);
}

/// A recovery that lands on a view its pushed train already advanced in the
/// same poll reports that view once, for its last sync.
#[test]
fn a_recovery_onto_a_view_advanced_by_its_push_is_reported_once() {
    let (mut client, peer, log, subs) = subscribed();
    let h = std::thread::spawn(move || {
        peer.expect_sync("the poll");
        peer.push_train(7, subs[&7], &[], None);
        peer.push_train(8, subs[&8], &[], Some(12));
        peer.send(&sync_answer(None));
        let read = peer.expect_kept("the delta read");
        assert!(matches!(read[..], [(7, _)]), "{read:?}");
        peer.send(&reply_status(7, WireStatus::StaleCatalog, "stale"));
        // The registration of 7 re-resolves to 8.
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(8);
        let next = peer.expect_kept("the next round");
        assert!(matches!(next[..], [(8, _)]), "{next:?}");
        peer.reply_watermark(8, TAG, 13);
    });
    let before = client.requests_sent();
    let report = poll(&mut client);
    h.join().unwrap();
    assert_eq!(
        client.requests_sent() - before,
        4,
        "the sync, the delta read, the resolve, the next round"
    );
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 8 && o.cursor.is_some_and(|c| c.tick.get() == 13)),
        "one entry for 8, for its last sync: {report:?}",
    );
    assert_eq!(client.mirrored_ids(), [8]);
    let events = log.take();
    assert!(
        !events.iter().any(|e| matches!(e, Ev::Refill(8) | Ev::Seal(8, _))),
        "the live copy must be neither erased nor re-read whole: {events:?}",
    );
}

/// Subscribe a reader to view 40 after round 9, its read answered with no row
/// and that round again; the subscription's id.
fn subscribe_reader(client: &mut GnitzClient, peer: Peer) -> (SubId, Peer) {
    let from = DeltaCursor::from_pair(TAG, 9).unwrap();
    let whole = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None).encode();
    let h = std::thread::spawn(move || {
        let kept = peer.expect_kept("the reader's subscription");
        peer.reply_watermark(40, from.tag, from.tick.get());
        (peer, kept)
    });
    let schema = kv_schema(TypeCode::I64);
    let (sub, rows, at) = block_on(client.subscribe(Target::from(40), Some(from), &schema, &whole)).expect("subscribe");
    let (peer, kept) = h.join().unwrap();
    assert_eq!((kept, rows.batch.len(), at), (vec![(40, sub)], 0, from));
    (sub, peer)
}

#[test]
fn one_sync_serves_a_reader_and_the_mirror() {
    let (mut client, peer, log, subs) = subscribed();
    let (sub, peer) = subscribe_reader(&mut client, peer);
    let h = std::thread::spawn(move || {
        peer.expect_sync("the sync");
        peer.push_train(7, subs[&7], &[(1, 10, 1)], Some(11));
        peer.push_train(40, sub, &[(2, 20, 1)], Some(11));
        peer.send(&sync_answer(None));
    });
    let before = client.requests_sent();
    let synced = block_on(client.sync(Duration::ZERO)).expect("one sync");
    h.join().unwrap();
    assert_eq!(client.requests_sent() - before, 1, "one request for both");
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    let (rows, at) = pushed.result.as_ref().expect("the subscription is held");
    assert_eq!((pushed.sub, rows.batch.len(), at.tick.get()), (sub, 1, 11));
    assert_eq!(synced.mirrored.len(), 2, "one entry per view");
    assert_eq!(log.take(), [Ev::Advance(7, 11)]);
    let again = client.session.take_pushed(sub).expect("the subscription is held");
    assert!(again.0.is_empty(), "one sync takes everything it brought");
}

#[test]
fn a_sync_nobody_received_is_reported_by_the_next() {
    let (mut client, peer, _log) = fixture(&[]);
    let (sub, peer) = subscribe_reader(&mut client, peer);
    let h = std::thread::spawn(move || {
        peer.expect_sync("the sync");
        peer.push_train(40, sub, &[(1, 10, 1), (2, 20, 2)], Some(11));
        peer.send(&sync_answer(None));
        peer
    });
    // Its caller drops the reply unread.
    sync_unreceived(&mut client);
    let peer = h.join().unwrap();

    let h = std::thread::spawn(move || {
        let hdr = peer.expect_sync("the next sync");
        assert_eq!(hdr.arg0, 0, "a held delivery is no reason to wait");
        peer.push_train(40, sub, &[(1, 10, -1)], Some(12));
        peer.send(&sync_answer(None));
        peer
    });
    let synced = block_on(client.sync(Duration::from_secs(30))).expect("the next sync");
    let peer = h.join().unwrap();
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    let (rows, at) = pushed.result.as_ref().expect("the subscription is held");
    assert_eq!((pushed.sub, at.tick.get()), (sub, 12), "under the later cursor");
    let pks: Vec<u128> = (0..rows.batch.len()).map(|i| rows.batch.pk(i)).collect();
    assert_eq!(pks, [1, 2, 1], "the unreceived rows first");
    assert_eq!(rows.batch.view().weights().collect::<Vec<_>>(), [1, 2, -1]);

    let h = std::thread::spawn(move || {
        peer.expect_sync("the sync after");
        peer.send(&sync_answer(None));
    });
    let synced = block_on(client.sync(Duration::ZERO)).expect("the sync after");
    h.join().unwrap();
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    let (rows, _) = pushed.result.as_ref().expect("the subscription is held");
    assert!(rows.batch.is_empty(), "reported once");
}

/// A sync whose caller drops its delivery unread.
fn sync_unreceived(client: &mut GnitzClient) {
    let synced = client.begin_sync(Duration::ZERO);
    let synced = block_on(client.wait(synced));
    drop(block_on(client.finish_sync(synced)).expect("the sync"));
}

/// A reseed in a delivery its caller dropped unread is reported again by the
/// next sync, which is not held.
#[test]
fn an_unreceived_reseed_is_reported_by_the_next_sync() {
    let (mut client, peer, _log) = fixture(&[(7, "a", 0)]);
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the whole read"), vec![7]);
        peer.reply_watermark(7, TAG, 20);
        peer
    });
    sync_unreceived(&mut client);
    let peer = h.join().unwrap();

    let h = std::thread::spawn(move || {
        let hdr = peer.expect_sync("the next sync");
        assert_eq!(hdr.arg0, 0, "an owed reseed is no reason to wait");
        peer.send(&sync_answer(None));
    });
    let synced = block_on(client.sync(Duration::from_secs(30))).expect("the next sync");
    h.join().unwrap();
    assert!(
        matches!(synced.mirrored.as_slice(), [o] if o.view_id == 7 && o.result.reseeded()),
        "{:?}",
        synced.mirrored
    );
}

#[test]
fn an_unreceived_end_is_reported_once() {
    let (mut client, peer, _log) = fixture(&[]);
    let (sub, peer) = subscribe_reader(&mut client, peer);
    let h = std::thread::spawn(move || {
        peer.expect_sync("the sync");
        peer.push_train(40, sub, &[], None);
        peer.send(&sync_answer(None));
        peer
    });
    sync_unreceived(&mut client);
    let peer = h.join().unwrap();

    let h = std::thread::spawn(move || {
        let held = peer.expect_sync_naming("the next sync");
        assert!(held.is_empty(), "{held:?}");
        peer.send(&sync_answer(None));
    });
    let synced = block_on(client.sync(Duration::ZERO)).expect("the next sync");
    h.join().unwrap();
    assert!(matches!(synced.pushed[..], [crate::Pushed { sub: s, result: Err(_) }] if s == sub));
    let synced = block_on(client.sync(Duration::ZERO)).expect("the sync after");
    assert!(synced.pushed.is_empty(), "the end is reported once");
}

#[test]
fn an_interrupted_sync_keeps_a_readers_deltas() {
    let (mut client, peer, _log) = fixture(&[(7, "a", 4)]);
    let (sub, peer) = subscribe_reader(&mut client, peer);
    let reading = Arc::new(AtomicBool::new(false));
    let watched = Arc::clone(&reading);
    rehost(
        &mut client,
        BlockingHost::with_hook(move || {
            if watched.load(Ordering::Relaxed) {
                Err("interrupted".into())
            } else {
                Ok(())
            }
        }),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));

    let h = std::thread::spawn(move || {
        peer.expect_sync("the sync");
        peer.push_train(40, sub, &[(2, 20, 1)], Some(11));
        peer.send(&sync_answer(None));
        // The mirror's read is never answered, so the call parks and is
        // interrupted.
        let read = peer.expect_kept("the mirror's read");
        assert!(matches!(read[..], [(7, _)]), "{read:?}");
        reading.store(true, Ordering::Relaxed);
        (peer, read[0].1)
    });
    let r = block_on(client.sync(Duration::ZERO)).map(|s| s.mirrored);
    assert!(matches!(r, Err(ClientError::Interrupted(_))), "{r:?}");
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    rehost(&mut client, BlockingHost::default());
    let (peer, abandoned) = h.join().unwrap();

    let h = std::thread::spawn(move || {
        // The abandoned read is answered, and the subscription it asked to
        // keep is one the next sync does not name.
        peer.reply_watermark(7, TAG, 11);
        let held = peer.expect_sync_naming("the next sync");
        assert!(held == [sub] && sub != abandoned, "{held:?}");
        peer.send(&sync_answer(None));
        assert_eq!(peer.expect_poll("the mirror's read"), vec![7]);
        peer.reply_watermark(7, TAG, 11);
    });
    let synced = block_on(client.sync(Duration::ZERO)).expect("the next sync");
    h.join().unwrap();
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    let (rows, at) = pushed.result.as_ref().expect("the subscription is held");
    assert_eq!((pushed.sub, rows.batch.len(), at.tick.get()), (sub, 1, 11));
}

#[test]
fn a_view_that_keeps_failing_does_not_end_the_hold() {
    let (mut client, peer, _log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
    let h = std::thread::spawn(move || {
        // The first sync reads both: `a` advances, `b` is refused for good.
        for id in peer.expect_poll("the first read") {
            match id {
                7 => peer.reply_watermark(7, TAG, 9),
                _ => peer.send(&reply_status(id, WireStatus::Error, "gone")),
            }
        }
        let hdr = peer.expect_sync("the second sync");
        peer.send(&sync_answer(None));
        assert_eq!(peer.expect_poll("the retry"), vec![8]);
        peer.send(&reply_status(8, WireStatus::Error, "gone"));
        hdr.arg0
    });
    let failed = |synced: crate::Synced| {
        let failed = synced
            .mirrored
            .iter()
            .filter(|o| matches!(o.result, PollResult::Failed(_)));
        failed.count()
    };
    assert_eq!(failed(block_on(client.sync(Duration::ZERO)).unwrap()), 1);
    assert_eq!(failed(block_on(client.sync(Duration::from_secs(30))).unwrap()), 1);
    assert_eq!(h.join().unwrap(), 30_000, "the sync asked to be held");
}

#[test]
fn a_sync_over_an_untaken_train_asks_for_no_hold() {
    let (mut client, peer, log, subs) = subscribed();
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_request("an id allocation"), 0);
        // Ahead of the reply, as the trains of a sync nobody finished are.
        peer.push_train(8, subs[&8], &[(2, 20, 1)], Some(15));
        peer.send(&reply_ctrl(0, 4242));
        let hdr = peer.expect_sync("the sync");
        peer.send(&sync_answer(None));
        hdr.arg0
    });
    assert_eq!(block_on(client.alloc_ids(1)).unwrap(), 4242);
    let synced = block_on(client.sync(Duration::from_secs(30))).unwrap();
    assert_eq!(synced.mirrored.len(), 2);
    assert_eq!(h.join().unwrap(), 0, "no hold in front of a train already here");
    assert_eq!(log.take(), [Ev::Advance(8, 15)]);
}

/// A view let go of ends its subscription: the next sync does not name it.
#[test]
fn a_forgotten_view_ends_its_subscription() {
    let (mut client, peer, _log, subs) = subscribed();
    let h = std::thread::spawn(move || {
        let mut both: Vec<SubId> = subs.values().copied().collect();
        both.sort_unstable();
        assert_eq!(peer.expect_sync_naming("the poll"), both);
        peer.send(&sync_answer(None));
        assert_eq!(peer.expect_sync_naming("the poll after"), [subs[&8]]);
        peer.send(&sync_answer(None));
    });
    poll(&mut client);
    block_on(client.forget_view(7)).unwrap();
    poll(&mut client);
    h.join().unwrap();
}

/// A sync made while the connection is at its in-flight cap waits for room
/// rather than being refused, which would end every subscription.
#[test]
fn a_sync_at_the_in_flight_cap_waits_for_room() {
    let (mut client, peer, log, subs) = subscribed();
    let (sub, peer) = subscribe_reader(&mut client, peer);
    let detached: Vec<_> = (0..crate::connection::MAX_IN_FLIGHT)
        .map(|_| client.ack(crate::connection::Request::AllocIds(1)).detach())
        .collect();
    assert!(client.session.at_capacity());
    let h = std::thread::spawn(move || {
        for i in 0..crate::connection::MAX_IN_FLIGHT {
            assert_eq!(peer.expect_request("an id allocation"), 0);
            peer.send(&reply_ctrl(0, i as u64));
        }
        peer.expect_sync("the sync");
        peer.push_train(7, subs[&7], &[(1, 10, 1)], Some(11));
        peer.push_train(40, sub, &[(2, 20, 1)], Some(11));
        peer.send(&sync_answer(None));
        peer
    });
    let synced = block_on(client.sync(Duration::ZERO)).expect("the sync");
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    assert!(
        pushed.result.is_ok(),
        "the reader's subscription ended: {:?}",
        pushed.result
    );
    assert!(
        synced.mirrored.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "{:?}",
        synced.mirrored
    );
    let _peer = h.join().unwrap();
    assert_eq!(log.take(), [Ev::Advance(7, 11)]);
    drop(detached);
}
