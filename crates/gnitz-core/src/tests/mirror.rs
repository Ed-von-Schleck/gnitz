//! The mirroring seam under a scripted peer, and under a second [`MirrorStore`]
//! implementor.
//!
//! What these pin is otherwise unassertable: the *shape* of a poll — how many
//! requests it writes before it reads a reply, which failure classes pay a
//! probe, and the order the two phases run in — none of which shows up in the
//! rows an acceptance test compares. The store here holds no rows at all; it
//! records the calls the state machine makes, which no live store would let a
//! test see.

use super::*;
use crate::block_on;
use crate::protocol::message::encode_frame;
use crate::protocol::transport::poll_fd;
use crate::test_support::{interrupt_self_until, kv_schema, reply_ctrl, reply_status, session_pair, Peer};
use crate::BlockingHost;
use gnitz_wire::control::peek_control_block;
use gnitz_wire::control::ControlHeader;
use gnitz_wire::txn_frame::DELTA_POLL_MAX_VIEWS;
use gnitz_wire::RelDescriptorBlob;
use gnitz_wire::{TypeCode, WireStatus};
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The feed tag every cursor here carries; a reply must echo it or the cursor
/// stops continuing.
const TAG: u64 = 0xFEED;

// ---------------------------------------------------------------------------
// A recording store
// ---------------------------------------------------------------------------

/// One call the state machine made through the seam.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Invalidate(u64, Invalidate),
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
struct Log(Arc<Mutex<Vec<Ev>>>);

impl Log {
    fn push(&self, e: Ev) {
        self.0.lock().unwrap().push(e);
    }
    fn take(&self) -> Vec<Ev> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
    /// Whether any event so far matches, without draining — for a test that
    /// watches the log while the call under test is still running.
    fn saw(&self, f: impl Fn(&Ev) -> bool) -> bool {
        self.0.lock().unwrap().iter().any(f)
    }
}

/// A store that holds names, feed positions and a call log — everything the
/// reconciliation state machine reads back through the seam, and nothing else.
struct StubStore {
    cursors: HashMap<u64, DeltaCursor>,
    names: HashMap<u64, String>,
    log: Log,
}

impl MirrorStore for StubStore {
    fn base_dir(&self) -> &str {
        "stub"
    }

    fn register(&mut self, schema_name: &str, name: &str, desc: &RelDescriptor) -> Result<Option<u64>, MirrorError> {
        let tid = desc.tid;
        let qname = format!("{schema_name}.{name}");
        // The live store's name rule; its layout rule is not modelled.
        let renamed = self
            .names
            .iter()
            .find(|(&t, n)| t != tid && **n == qname)
            .map(|(&t, _)| t);
        if let Some(old) = renamed {
            self.invalidate(old, Invalidate::Registration)?;
        }
        self.names.insert(tid, qname);
        Ok(renamed)
    }

    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        self.log.push(Ev::Invalidate(tid, level));
        self.cursors.remove(&tid);
        if level == Invalidate::Registration {
            self.names.remove(&tid);
        }
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

    fn clear_cursors(&mut self) {
        self.cursors.clear();
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
        gnitz_wire::txn_frame::decode_delta_poll(&frame[ctrl.body])
            .expect("a delta poll")
            .into_iter()
            .map(|v| v.view_id)
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
/// `(tid, name, tick)` is registered under `s.name` and, at a non-zero tick,
/// holds a feed position. Nothing goes over the wire.
fn fixture(views: &[(u64, &str, u64)]) -> (GnitzClient, Peer, Log) {
    let (session, peer) = session_pair();
    let mut client = GnitzClient::from_session(session);
    let log = Log::default();
    let cursors = views
        .iter()
        .filter_map(|&(tid, _, tick)| Some((tid, DeltaCursor::from_pair(TAG, tick)?)))
        .collect();
    client
        .attach_mirror(StubStore {
            cursors,
            names: HashMap::new(),
            log: log.clone(),
        })
        .unwrap();
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
        block_on(client.bind("s", name, Arc::new(desc))).unwrap();
    }
    assert!(log.take().is_empty(), "registration tears nothing down");
    (client, peer, log)
}

// ---------------------------------------------------------------------------
// Frame count
// ---------------------------------------------------------------------------

/// One poll names every view in as few requests as the per-request cap allows —
/// one up to the cap, a second past it — and every view advances.
#[test]
fn one_poll_writes_one_request_per_chunk_of_views() {
    for want in [vec![6], vec![DELTA_POLL_MAX_VIEWS, 1]] {
        let m: usize = want.iter().sum();
        let names: Vec<String> = (0..m).map(|i| format!("v{i}")).collect();
        let views: Vec<(u64, &str, u64)> = names
            .iter()
            .enumerate()
            .map(|(i, n)| (100 + i as u64, n.as_str(), 4))
            .collect();
        let (mut client, peer, _log) = fixture(&views);

        let chunks = want.len();
        let h = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for _ in 0..chunks {
                let ids = peer.expect_poll("a chunk");
                for &id in &ids {
                    peer.reply_watermark(id, TAG, 9);
                }
                seen.push(ids);
            }
            seen
        });
        let report = block_on(client.poll_mirror()).expect("every view advances");
        let seen = h.join().unwrap();

        assert_eq!(client.requests_sent(), chunks as u64, "{m} views");
        assert_eq!(seen.iter().map(Vec::len).collect::<Vec<_>>(), want);
        let mut ids = seen.concat();
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

/// A refusal naming a vanished relation pays exactly one probe; every other
/// refusal pays none — a dead socket or a poisoned reply stops buying a second
/// doomed round trip.
#[test]
fn only_a_vanished_relation_pays_a_probe() {
    for (status, requests) in [(WireStatus::NotFound, 2), (WireStatus::Error, 1)] {
        let (mut client, peer, _log) = fixture(&[(7, "v", 4)]);
        let h = std::thread::spawn(move || {
            assert_eq!(peer.expect_poll("the poll"), vec![7]);
            peer.send(&reply_status(7, status, "gone"));
            if status == WireStatus::NotFound {
                // "No such relation", so the recovery is a `Failed` either way.
                peer.expect_request("the probe");
                peer.send(&reply_ctrl(0, 0));
            }
        });
        let report = block_on(client.poll_mirror()).expect("a per-view failure is not the call's");
        h.join().unwrap();
        assert_eq!(client.requests_sent(), requests, "status {status:?}");
        assert!(
            matches!(report.as_slice(), [o] if o.view_id == 7 && matches!(o.result, PollResult::Failed(_))),
            "status {status:?}: got {report:?}",
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
        peer.expect_request("the re-resolve");
        peer.reply_resolved(7);
        assert_eq!(peer.expect_poll("the bootstrap"), vec![7]);
        peer.send(&block(&[(1, 10, 1)], 0, true));
        let until = Instant::now() + PATIENCE;
        while !watched.saw(|e| matches!(e, Ev::Fill(7))) {
            assert!(Instant::now() < until, "the first block never reached the store");
            std::thread::sleep(Duration::from_millis(1));
        }
        peer.send(&block(&[(2, 20, 1)], 20, false));
    });
    let report = block_on(client.poll_mirror()).expect("the bootstrap lands");
    h.join().unwrap();
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 7 && o.result.reseeded()),
        "{report:?}"
    );
    assert_eq!(
        log.take(),
        [
            Ev::Invalidate(7, Invalidate::Cursor),
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
        view_id: 7,
        after_tick: 4,
        reply_layout: kv_schema(TypeCode::I64).layout_digest(),
    };
    drop(DeltaPoll::start(&mut client.session, &[abandoned]));

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
    block_on(client.poll_mirror()).expect("the leftover train is drained, not applied");
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
// Phase ordering
// ---------------------------------------------------------------------------

/// No teardown reaches the store until every phase-1 ingest has run.
///
/// One view is refused as gone and the other advances. Recovering inside the
/// ingest loop would re-resolve and re-fetch while a reply for the second view
/// was still in hand, and applying that reply after the recovery's own fetch
/// doubles every weight in the overlap.
#[test]
fn no_recovery_runs_before_every_ingest_has() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);

    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        // The first position's view is the one that vanished.
        peer.send(&reply_status(ids[0], WireStatus::NotFound, "gone"));
        peer.reply_watermark(ids[1], TAG, 11);
        // Phase 2: the probe, the re-resolve it feeds, and the bootstrap.
        peer.expect_request("the probe");
        peer.reply_resolved(9);
        peer.expect_request("the re-resolve");
        peer.reply_resolved(9);
        assert_eq!(peer.expect_poll("the bootstrap"), vec![9]);
        peer.reply_watermark(9, TAG, 20);
        (ids[0], ids[1])
    });
    let report = block_on(client.poll_mirror()).expect("a recovered view is not the call's failure");
    let (gone, alive) = h.join().unwrap();
    assert_eq!(client.requests_sent(), 4, "both views ride one request");

    let events = log.take();
    let first_ingest = events
        .iter()
        .position(|e| matches!(e, Ev::Advance(t, 11) if *t == alive))
        .expect("phase 1 ingested the reply it had");
    let first_teardown = events.iter().position(|e| matches!(e, Ev::Invalidate(..)));
    assert!(
        first_teardown.is_none_or(|t| t > first_ingest),
        "a recovery ran before phase 1 finished ingesting: {events:?}",
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Ev::Invalidate(t, Invalidate::Cursor) if *t == gone)),
        "the vanished view is reseeded by name: {events:?}",
    );
    let mut ids: Vec<u64> = report.iter().map(|o| o.view_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![alive, 9], "one entry per view, the moved one at its new id");
}

/// A reseed onto an id that already holds a cursor advances it instead
/// of erasing a good copy: the cursor-less registration's name now resolves to
/// a view this same poll already advanced.
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
        // Phase 2 re-resolves the cursor-less registration; it lands on 8, whose
        // copy is advanced from where phase 1 left it rather than re-read.
        peer.expect_request("the re-resolve");
        peer.reply_resolved(8);
        assert_eq!(peer.expect_poll("the follow-up poll"), vec![8]);
        peer.reply_watermark(8, TAG, 12);
    });
    let report = block_on(client.poll_mirror()).expect("the reseed lands on a live copy");
    h.join().unwrap();
    assert_eq!(client.requests_sent(), 3, "the live copy is not re-read");

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
        peer.send(&reply_status(8, WireStatus::NotFound, "gone"));
        // Phase 2 recovers the cursor-less view first: re-resolve, bootstrap.
        peer.expect_request("the re-resolve");
        peer.reply_resolved(7);
        assert_eq!(peer.expect_poll("the bootstrap"), vec![7]);
        peer.reply_watermark(7, TAG, 20);
        // 8's probe is never answered, so the call parks and is interrupted.
        peer.expect_request("the probe");
        peer
    });
    let r = block_on(client.poll_mirror());
    assert!(matches!(r, Err(ClientError::Interrupted(_))), "{r:?}");
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    client.host = Box::new(BlockingHost::default());
    client.host.attach(client.session.as_fd()).unwrap();
    let peer = h.join().unwrap();

    let h = std::thread::spawn(move || {
        peer.send(&reply_ctrl(0, 0)); // the abandoned probe
        for tick in [21, 22] {
            for id in peer.expect_poll("a poll") {
                peer.reply_watermark(id, TAG, tick);
            }
        }
    });
    let mut results = Vec::new();
    for _ in 0..2 {
        let mut report: Vec<(u64, bool)> = block_on(client.poll_mirror())
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
        let report = block_on(client.poll_mirror()).expect("a broken reply is reported per view");
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
    let report = block_on(client.poll_mirror()).expect("both views advance");
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

        let report = block_on(client.poll_mirror()).expect("a dead socket is not the call's own failure");
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
