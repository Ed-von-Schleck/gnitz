//! The mirroring seam under a scripted peer, and under a second [`MirrorStore`]
//! implementor.
//!
//! What these pin is otherwise unassertable: the *shape* of a poll — how many
//! requests it writes before it reads a reply, which failure classes pay a
//! resolve, and the order its rounds run in — none of which shows up in the
//! rows an acceptance test compares. The store here holds no rows at all; it
//! records the calls the state machine makes, which no live store would let a
//! test see.

use super::*;
use crate::block_on;
use crate::protocol::message::encode_frame;
use crate::protocol::transport::poll_fd;
use crate::test_support::{interrupt_self_until, kv_schema, rel, reply_ctrl, reply_status, session_pair, Peer};
use crate::BlockingHost;
use gnitz_wire::control::peek_control_block;
use gnitz_wire::control::ControlHeader;
use gnitz_wire::RelDescriptorBlob;
use gnitz_wire::TypeCode;
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The feed tag every cursor here carries.
const TAG: u64 = 0xFEED;

// ---------------------------------------------------------------------------
// A recording store
// ---------------------------------------------------------------------------

/// One call the state machine made through the seam.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Register(u64),
    DropCursor(u64),
    Forget(u64),
    /// `tid`'s copy erased for a refill.
    Erase(u64),
    /// One block of a refill of `tid`.
    Fill(u64),
    /// `(tid, the round the refill was sealed at)`.
    Reseed(u64, u64),
    /// `(tid, the round the copy moved to)`.
    Advance(u64, u64),
}

#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<Ev>>>, Arc<Mutex<Option<u64>>>);

impl Log {
    fn push(&self, e: Ev) {
        self.0.lock().unwrap().push(e);
    }
    fn take(&self) -> Vec<Ev> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
    /// Fail the store's next `register`, once, after it retracted `old`.
    fn fail_next_register_retracting(&self, old: u64) {
        *self.1.lock().unwrap() = Some(old);
    }
    /// Whether any event so far matches, without draining — for a test that
    /// watches the log while the call under test is still running.
    fn saw(&self, f: impl Fn(&Ev) -> bool) -> bool {
        self.0.lock().unwrap().iter().any(f)
    }
}

/// A store that holds feed positions and a call log — everything the
/// reconciliation reads back through the seam, and nothing else.
struct StubStore {
    cursors: HashMap<u64, DeltaCursor>,
    log: Log,
}

impl MirrorStore for StubStore {
    fn base_dir(&self) -> &str {
        "stub"
    }

    fn register(&mut self, _name: &RelName, desc: &RelDescriptor) -> Result<(), MirrorError> {
        self.log.push(Ev::Register(desc.tid));
        if let Some(old) = self.log.1.lock().unwrap().take() {
            self.cursors.remove(&old);
            return Err(MirrorError::Engine("stub: the copy could not be entered".into()));
        }
        Ok(())
    }

    fn drop_cursor(&mut self, tid: u64) {
        self.log.push(Ev::DropCursor(tid));
        self.cursors.remove(&tid);
    }

    fn forget(&mut self, tid: u64) -> Result<(), MirrorError> {
        self.log.push(Ev::Forget(tid));
        self.cursors.remove(&tid);
        Ok(())
    }

    fn refill(&mut self, tid: u64) -> Result<(), MirrorError> {
        self.log.push(Ev::Erase(tid));
        self.cursors.remove(&tid);
        Ok(())
    }

    fn fill(&mut self, tid: u64, blocks: &[&[u8]]) -> Result<(), MirrorError> {
        blocks.iter().for_each(|_| self.log.push(Ev::Fill(tid)));
        Ok(())
    }

    fn seal(&mut self, tid: u64, cursor: DeltaCursor) -> Result<(), MirrorError> {
        self.log.push(Ev::Reseed(tid, cursor.tick.get()));
        self.cursors.insert(tid, cursor);
        Ok(())
    }

    fn advance(&mut self, tid: u64, _b: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError> {
        if !self.cursors.contains_key(&tid) {
            return Err(MirrorError::Engine(format!(
                "stub: advance of {tid}, which holds no cursor"
            )));
        }
        self.log.push(Ev::Advance(tid, next.tick.get()));
        self.cursors.insert(tid, next);
        Ok(())
    }

    fn scan_spec(
        &mut self,
        _tid: u64,
        _spec: gnitz_wire::ReadSpec,
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError> {
        Ok(ZSetBatch::new(reply_schema))
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.cursors.get(&tid).copied()
    }

    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        Ok(())
    }

    fn poisoned(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// The scripted peer
// ---------------------------------------------------------------------------

/// How long a request that is on its way may take. Only ever paid in full by a
/// regression, which then fails rather than hangs.
const PATIENCE: Duration = Duration::from_secs(5);

// A script's peer is dropped when the script ends, so a request it does not
// expect fails fast instead of parking the client; each test counts
// `requests_sent` to catch one.
impl Peer {
    /// The next request frame, which must already be on its way.
    fn expect_frame(&self, what: &str) -> Vec<u8> {
        let ready = poll_fd(self.0.as_raw_fd(), libc::POLLIN, Some(Instant::now() + PATIENCE));
        assert!(
            ready.is_ok_and(|revents| revents & libc::POLLIN != 0),
            "{what}: the request never arrived"
        );
        self.recv()
    }

    /// The target id of the next request.
    fn expect_request(&self, what: &str) -> u64 {
        let frame = self.expect_frame(what);
        peek_control_block(&frame).expect("a control header").hdr.target_id
    }

    /// The view ids the next request, a DELTA_POLL, names in request order.
    fn expect_poll(&self, what: &str) -> Vec<u64> {
        let frame = self.expect_frame(what);
        let ctrl = peek_control_block(&frame).expect("a control header");
        gnitz_wire::txn_frame::decode_delta_poll(&ctrl.hdr, &frame[ctrl.body.clone()])
            .expect("a delta poll")
            .1
            .into_iter()
            .map(|v| v.view.tid)
            .collect()
    }

    /// One view's answer inside a poll: a control-only terminal frame naming the
    /// view and carrying the cursor `(tag, tick)`.
    fn reply_watermark(&self, target_id: u64, tag: u64, tick: u64) {
        let h = ControlHeader {
            target_id,
            arg0: tick,
            arg1: tag,
            ..Default::default()
        };
        self.send(&encode_frame(h, &[], None, None));
    }

    /// A RESOLVE answering with `tid`: the schema block plus a view descriptor
    /// carrying a feed.
    fn reply_resolved(&self, tid: u64) {
        let schema = kv_schema(TypeCode::I64);
        let blob = RelDescriptorBlob {
            class: RelClass::FedView,
            ..Default::default()
        };
        let hdr = ControlHeader { target_id: tid, ..Default::default() };
        self.send(&encode_frame(
            hdr,
            &blob.encode(),
            Some(&schema.to_block()),
            Some(&ZSetBatch::new(&schema)),
        ));
    }
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// A client on a scripted peer with `views` mirrored through `bind`: each
/// `(tid, name, tick)` is registered under `s.name`, confirmed, and, at a
/// non-zero tick, holds a feed position. Nothing goes over the wire.
fn fixture(views: &[(u64, &str, u64)]) -> (GnitzClient, Peer, Log) {
    let (session, peer) = session_pair();
    let mut client = GnitzClient::from_session(session);
    let log = Log::default();
    let cursors = views
        .iter()
        .filter_map(|&(tid, _, tick)| Some((tid, DeltaCursor::from_pair(TAG, tick)?)))
        .collect();
    client.attach_mirror(StubStore { cursors, log: log.clone() }).unwrap();
    let schema = Arc::new(kv_schema(TypeCode::I64));
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
        let entry = MirroredView::new(rel("s", name), Subscription::whole(Arc::new(desc)), None).unwrap();
        block_on(client.bind(entry)).unwrap();
        client.mirror.as_mut().unwrap().views.get_mut(&tid).unwrap().confirmed = true;
    }
    assert!(
        log.take().iter().all(|e| matches!(e, Ev::Register(_))),
        "registration tears nothing down"
    );
    (client, peer, log)
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
        let report = block_on(client.poll_mirror(Duration::ZERO)).expect("every view advances");
        let seen = h.join().unwrap();

        assert_eq!(client.requests_sent(), 1, "{m} views");
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
        let report = block_on(client.poll_mirror(Duration::ZERO)).expect("a per-view failure is not the call's");
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
            client.mirrors(7),
            "status {status:?}: a failed poll leaves the copy answering its last round",
        );
    }
}

/// A bootstrap's first block reaches the store before its terminal is sent.
#[test]
fn a_bootstrap_fills_the_store_frame_by_frame() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0)]);
    let watched = log.clone();
    let block = |rows: &[(u64, i64, i64)], tick: u64, continuation: bool| {
        let hdr = ControlHeader {
            target_id: 7,
            flags: gnitz_wire::WireFlags { continuation, ..Default::default() },
            arg0: tick,
            arg1: TAG,
            ..Default::default()
        };
        encode_frame(hdr, &[], None, Some(&crate::test_support::kv_rows(rows)))
    };
    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the bootstrap"), vec![7]);
        peer.send(&block(&[(1, 10, 1)], 0, true));
        let until = Instant::now() + PATIENCE;
        while !watched.saw(|e| matches!(e, Ev::Fill(7))) {
            assert!(Instant::now() < until, "the first block never reached the store");
            std::thread::sleep(Duration::from_millis(1));
        }
        peer.send(&block(&[(2, 20, 1)], 20, false));
    });
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("the bootstrap lands");
    h.join().unwrap();
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 7 && o.result.reseeded()),
        "{report:?}"
    );
    assert_eq!(
        log.take(),
        [
            Ev::Register(7),
            Ev::Erase(7),
            Ev::Fill(7),
            Ev::Fill(7),
            Ev::Reseed(7, 20),
        ],
    );
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
    let abandoned = DeltaPollItem {
        view: 7.into(),
        tag: TAG,
        after_tick: 4,
        reply_layout: kv_schema(TypeCode::I64).layout_digest(),
        spec: &[],
    };
    drop(DeltaPoll::start(&mut client.session, &[abandoned], Duration::ZERO));

    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the abandoned poll"), vec![7]);
        let ids = peer.expect_poll("the poll");
        // The abandoned train first, then one terminal per view, each at a round
        // derived from the view that asked — so a shifted reply lands visibly
        // wrong.
        peer.reply_watermark(7, TAG, 50);
        for &tid in &ids {
            peer.reply_watermark(tid, TAG, 100 + tid);
        }
    });
    block_on(client.poll_mirror(Duration::ZERO)).expect("the leftover train is drained, not applied");
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
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("a recovered view is not the call's failure");
    let (gone, alive) = h.join().unwrap();
    assert_eq!(client.requests_sent(), 3, "both views ride one request");

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
        events.contains(&Ev::Reseed(9, 20)) && !events.iter().any(|e| matches!(e, Ev::Erase(t) if *t == gone)),
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
        assert_eq!(
            peer.expect_poll("the poll"),
            vec![8],
            "only the view with a round to poll after"
        );
        peer.reply_watermark(8, TAG, 11);
        assert_eq!(peer.expect_poll("the whole read"), vec![7]);
        peer.send(&reply_status(7, WireStatus::StaleCatalog, "stale"));
        // The registration lands on 8, whose copy is advanced from where the
        // first round left it rather than re-read.
        assert_eq!(peer.expect_request("the re-resolve"), 0);
        peer.reply_resolved(8);
        assert_eq!(peer.expect_poll("the next round"), vec![8]);
        peer.reply_watermark(8, TAG, 12);
    });
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("the recovery lands on a live copy");
    h.join().unwrap();
    assert_eq!(client.requests_sent(), 4, "the live copy is not re-read");

    let events = log.take();
    assert!(
        !events.iter().any(|e| matches!(e, Ev::Erase(8) | Ev::Reseed(8, _))),
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
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("both recoveries land");
    h.join().unwrap();
    assert_eq!(client.requests_sent(), 3);

    let events = log.take();
    let whole: Vec<&Ev> = events
        .iter()
        .filter(|e| matches!(e, Ev::Erase(_) | Ev::Reseed(..)))
        .collect();
    assert_eq!(whole, [&Ev::Erase(8), &Ev::Reseed(8, 20)], "{events:?}");
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
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("a lost connection is each view's failure");
    h.join().unwrap();

    assert!(
        matches!(
            report.as_slice(),
            [o] if o.view_id == 9 && o.cursor.is_none() && matches!(o.result, PollResult::Failed(ClientError::ConnectionLost(_)))
        ),
        "{report:?}"
    );
    assert_eq!(client.mirrored_ids(), [9]);
    assert!(!client.mirrors(9), "a copy never synced answers no read");
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
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("a store refusal is the view's failure");
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
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("the view is read whole");
    h.join().unwrap();
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 7 && o.result.reseeded()),
        "{report:?}"
    );
    assert_eq!(log.take(), [Ev::Register(7), Ev::Erase(7), Ev::Reseed(7, 20)]);
}

/// An interrupted poll discards its report, so a reseed it ran is still owed:
/// the next poll reports that view `Reseeded`, and the one after that does not.
#[test]
fn an_interrupted_poll_reannounces_its_reseed() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0), (8, "b", 4)]);
    let watched = log.clone();
    client.host = Box::new(BlockingHost::with_hook(Box::new(move || {
        if watched.saw(|e| matches!(e, Ev::Reseed(7, _))) {
            Err("interrupted".into())
        } else {
            Ok(())
        }
    })));
    client.host.attach(client.session.as_fd()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));

    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_poll("the poll"), vec![8]);
        peer.send(&reply_status(8, WireStatus::StaleCatalog, "stale"));
        // The round reads the cursor-less view whole before any recovery.
        assert_eq!(peer.expect_poll("the bootstrap"), vec![7]);
        peer.reply_watermark(7, TAG, 20);
        // 8's re-resolve is never answered, so the call parks and is interrupted.
        peer.expect_request("the re-resolve");
        peer
    });
    let r = block_on(client.poll_mirror(Duration::ZERO));
    assert!(matches!(r, Err(ClientError::Interrupted(_))), "{r:?}");
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    client.host = Box::new(BlockingHost::default());
    client.host.attach(client.session.as_fd()).unwrap();
    let peer = h.join().unwrap();

    let h = std::thread::spawn(move || {
        peer.send(&reply_ctrl(0, 0)); // the abandoned re-resolve
        for tick in [21, 22] {
            for id in peer.expect_poll("a poll") {
                peer.reply_watermark(id, TAG, tick);
            }
        }
    });
    let mut results = Vec::new();
    for _ in 0..2 {
        let mut report: Vec<(u64, bool)> = block_on(client.poll_mirror(Duration::ZERO))
            .expect("both views advance")
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
        let report = block_on(client.poll_mirror(Duration::ZERO)).expect("a broken reply is reported per view");
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
                    !log.saw(|e| matches!(e, Ev::Reseed(t, _) | Ev::Advance(t, _) if t == id)),
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
        while !watched.saw(|e| matches!(e, Ev::Reseed(t, _) | Ev::Advance(t, _) if *t == ids[0])) {
            assert!(
                Instant::now() < deadline,
                "the first view's blocks must reach the store before the second view is answered",
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        peer.reply_watermark(ids[1], TAG, 12);
    });
    let report = block_on(client.poll_mirror(Duration::ZERO)).expect("both views advance");
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
            let r = block_on(client.scan_spec(1, &spec, &Arc::new(kv_schema(TypeCode::I64))));
            assert!(matches!(r, Err(ClientError::ConnectionLost(_))), "{r:?}");
        }

        let report = block_on(client.poll_mirror(Duration::ZERO)).expect("a dead socket is not the call's own failure");
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
            client.mirrors(7) && client.mirrors(8),
            "and both copies still answer at the round they reached",
        );
    }
}
