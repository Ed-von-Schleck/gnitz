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
use crate::connection::{Request, Session};
use crate::protocol::message::{encode_control_block, encode_message_parts};
use crate::protocol::transport::poll_fd;
use crate::protocol::{decode_control_block, ColumnDef, Header, TypeCode, STATUS_ERROR, STATUS_NOT_FOUND};
use crate::test_support::{established, framed, make_socketpair, raw_read_frame, raw_send};
use gnitz_wire::RelDescriptorBlob;
use std::collections::HashMap;
use std::os::fd::{AsRawFd, OwnedFd};
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
    Register(u64, String),
    Invalidate(u64, Invalidate),
    /// `(tid, shape, the round the cursor moved to)`.
    Ingest(u64, Shape, u64),
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

    fn register(&mut self, tid: u64, schema_name: &str, name: &str, _s: &Schema) -> Result<Option<u64>, MirrorError> {
        let qname = format!("{schema_name}.{name}");
        self.log.push(Ev::Register(tid, qname.clone()));
        // The live store's rule: a record under this name at another id lost it.
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

    fn ingest(&mut self, tid: u64, _b: Vec<RawBlock>, shape: Shape, next: DeltaCursor) -> Result<(), MirrorError> {
        self.log.push(Ev::Ingest(tid, shape, next.tick));
        self.cursors.insert(tid, next);
        Ok(())
    }

    fn scan(&mut self, _tid: u64, schema: &Schema) -> Result<ZSetBatch, MirrorError> {
        Ok(ZSetBatch::new(schema))
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

/// The raw far end of the socketpair.
struct Peer(OwnedFd);

/// How long a request that is on its way may take. Only ever paid in full by a
/// regression, which then fails rather than hangs.
const PATIENCE: Duration = Duration::from_secs(5);
/// How long to wait before concluding nothing more is coming. The client is
/// either already blocked in `poll(2)` or writing, so this is slack, not a race.
const QUIET: Duration = Duration::from_millis(500);

impl Peer {
    fn send(&self, payload: &[u8]) {
        raw_send(&self.0, &framed(payload));
    }

    /// Whether another request frame arrives within `d`. An expired timeout is
    /// `WouldBlock`, which is the "nothing came" answer rather than a failure.
    fn waits(&self, d: Duration) -> bool {
        match poll_fd(self.0.as_raw_fd(), libc::POLLIN, Some(Instant::now() + d), true) {
            Ok(revents) => revents & libc::POLLIN != 0,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
            Err(e) => panic!("poll on the peer fd: {e}"),
        }
    }

    /// The target id of the next request, which must already be on its way.
    fn expect_request(&self, what: &str) -> u64 {
        assert!(self.waits(PATIENCE), "{what}: the request never arrived");
        let frame = raw_read_frame(&self.0);
        decode_control_block(&frame).expect("a control block").0.target_id
    }

    /// The view ids one DELTA_POLL frame names, in request order.
    fn expect_poll(&self, what: &str) -> Vec<u64> {
        assert!(self.waits(PATIENCE), "{what}: the poll never arrived");
        let frame = raw_read_frame(&self.0);
        gnitz_wire::txn_frame::decode_delta_poll(&frame)
            .expect("a delta poll")
            .into_iter()
            .map(|v| v.view_id)
            .collect()
    }

    fn quiet(&self, what: &str) {
        assert!(!self.waits(QUIET), "{what}: an unexpected extra request");
    }

    /// One view's answer inside a poll: a control-only terminal frame naming the
    /// view and carrying the watermark `(tag, tick)`.
    fn reply_watermark(&self, target_id: u64, tag: u64, tick: u64) {
        let h = Header {
            target_id,
            seek_pk: gnitz_wire::pack_delta_watermark(tag, tick),
            ..Header::default()
        };
        self.send(&encode_control_block(&h, "", &[]));
    }

    fn reply_status(&self, target_id: u64, status: u32, text: &str) {
        let h = Header { status, target_id, ..Header::default() };
        self.send(&encode_control_block(&h, text, &[]));
    }

    /// A RESOLVE answering "no such relation": an empty descriptor blob.
    fn reply_absent(&self) {
        self.send(&encode_control_block(&Header::default(), "", &[]));
    }

    /// A RESOLVE answering with `tid`: the schema block plus a view descriptor
    /// carrying a feed.
    fn reply_resolved(&self, tid: u64) {
        let schema = view_schema();
        let blob = RelDescriptorBlob {
            class: RelClass::View,
            delta: true,
            ..Default::default()
        };
        let empty = ZSetBatch::new(&schema);
        let parts = encode_message_parts(tid, 0, 0, 0, &blob.encode(), 0, Some((&schema, &empty)));
        self.send(&parts.to_vec());
    }
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

fn view_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("val", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    }
}

/// A client on a scripted peer with `views` mirrored: each `(tid, name, tick)`
/// gets a registration and, at a non-zero tick, a feed position.
///
/// The registrations are installed directly rather than through `mirror_view`,
/// so a test scripts only the traffic it is about. `prime` runs on the session
/// first, for the one test that needs a slot left pending behind the client's
/// back.
fn fixture_priming(views: &[(u64, &str, u64)], prime: impl FnOnce(&mut Session)) -> (GnitzClient, Peer, Log) {
    let (a, b) = make_socketpair();
    let mut session = Session::from_transport(established(a));
    prime(&mut session);
    let mut client = GnitzClient::from_session(session);

    let log = Log::default();
    let mut store = StubStore {
        cursors: HashMap::new(),
        names: HashMap::new(),
        log: log.clone(),
    };
    for &(tid, name, tick) in views {
        store.names.insert(tid, format!("s.{name}"));
        if tick != 0 {
            store.cursors.insert(tid, DeltaCursor { tag: TAG, tick });
        }
    }
    client.attach_mirror(store).unwrap();

    let schema = Arc::new(view_schema());
    for &(tid, name, _) in views {
        let entry = MirroredView {
            schema_name: "s".to_string(),
            name: name.to_string(),
            desc: Arc::new(RelDescriptor {
                tid,
                class: RelClass::View,
                replicated: false,
                delta: true,
                schema: Arc::clone(&schema),
                indexes: Arc::new(Vec::new()),
            }),
            delta_reply: Arc::new(ReplySchema::new(Arc::new(delta_reply_schema(&schema).unwrap()), tid)),
        };
        client.mirror.as_deref_mut().unwrap().views.insert(tid, entry);
    }
    log.take();
    (client, Peer(b), log)
}

fn fixture(views: &[(u64, &str, u64)]) -> (GnitzClient, Peer, Log) {
    fixture_priming(views, |_| {})
}

// ---------------------------------------------------------------------------
// Frame count
// ---------------------------------------------------------------------------

/// One poll over M views writes exactly **one** request naming all of them.
#[test]
fn one_poll_writes_one_request_naming_every_view() {
    const M: usize = 6;
    let names: Vec<String> = (0..M).map(|i| format!("v{i}")).collect();
    let views: Vec<(u64, &str, u64)> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (100 + i as u64, n.as_str(), 4))
        .collect();
    let (mut client, peer, _log) = fixture(&views);

    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        peer.quiet("one frame for all M views");
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (100..100 + M as u64).collect::<Vec<_>>(),
            "every view rides the one request"
        );
        for id in ids {
            peer.reply_watermark(id, TAG, 9);
        }
        peer
    });
    let report = client.poll_mirror().expect("every view advances");
    let _peer = h.join().unwrap();

    assert_eq!(report.len(), M, "one entry per view");
    assert!(report.iter().all(|o| matches!(o.result, PollResult::Advanced)));
    assert!(report.iter().all(|o| o.cursor.is_some_and(|c| c.tick == 9)));
}

/// A refusal naming a vanished relation pays exactly one probe; every other
/// refusal pays none — a dead socket or a poisoned reply stops buying a second
/// doomed round trip.
#[test]
fn only_a_vanished_relation_pays_a_probe() {
    for (status, expected) in [(STATUS_NOT_FOUND, true), (STATUS_ERROR, false)] {
        let (mut client, peer, _log) = fixture(&[(7, "v", 4)]);
        let h = std::thread::spawn(move || {
            assert_eq!(peer.expect_poll("the poll"), vec![7]);
            peer.reply_status(7, status, "gone");
            // Answer any probe with "no such relation", so the recovery is a
            // `Failed` either way and the two runs differ only in the count.
            let probed = peer.waits(QUIET);
            if probed {
                peer.expect_request("the probe");
                peer.reply_absent();
                peer.quiet("one probe, not two");
            }
            probed
        });
        let report = client.poll_mirror().expect("a per-view failure is not the call's");
        assert_eq!(
            h.join().unwrap(),
            expected,
            "status {status} probed the wrong number of times"
        );
        assert!(
            matches!(report.as_slice(), [o] if o.view_id == 7 && matches!(o.result, PollResult::Failed(_))),
            "status {status}: got {report:?}",
        );
        assert!(
            client.mirrors(7),
            "status {status}: a failed poll leaves the copy answering its last round",
        );
    }
}

// ---------------------------------------------------------------------------
// Slot correlation
// ---------------------------------------------------------------------------

/// A leftover pending slot from an aborted call does not misdirect a reply.
///
/// Both views carry the same schema, so a block delivered to the wrong one would
/// decode cleanly and apply the other's rows and weights with no error. What
/// separates them here is the round each reply names.
#[test]
fn a_leftover_slot_does_not_shift_the_replies() {
    let schema = Arc::new(view_schema());
    let rs = ReplySchema::new(Arc::clone(&schema), 7);
    let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None);
    // A slot submitted and never drained: what an aborting park leaves behind.
    let (mut client, peer, _log) = fixture_priming(&[(7, "a", 4), (8, "b", 4)], |s| {
        s.submit(Request::ScanSpec {
            target_id: 7,
            spec: &spec,
            reply_schema: &rs,
        })
        .unwrap();
    });

    let h = std::thread::spawn(move || {
        assert_eq!(peer.expect_request("the abandoned scan"), 7);
        let ids = peer.expect_poll("the poll");
        // The abandoned train first, then one terminal per view, each at a round
        // derived from the view that asked — so a shifted reply lands visibly
        // wrong.
        peer.reply_watermark(7, 0, 0);
        for &tid in &ids {
            peer.reply_watermark(tid, TAG, 100 + tid);
        }
        peer
    });
    let report = client
        .poll_mirror()
        .expect("the leftover train is drained, not applied");
    let _peer = h.join().unwrap();

    let mut ticks: Vec<(u64, u64)> = report
        .iter()
        .map(|o| (o.view_id, o.cursor.expect("a round").tick))
        .collect();
    ticks.sort_unstable();
    assert_eq!(
        ticks,
        vec![(7, 107), (8, 108)],
        "each view took the reply its own request opened",
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
        peer.quiet("both views ride one request");
        // The first position's view is the one that vanished.
        peer.reply_status(ids[0], STATUS_NOT_FOUND, "gone");
        peer.reply_watermark(ids[1], TAG, 11);
        // Phase 2: the probe, the re-resolve it feeds, and the bootstrap.
        peer.expect_request("the probe");
        peer.reply_resolved(9);
        peer.expect_request("the re-resolve");
        peer.reply_resolved(9);
        assert_eq!(peer.expect_poll("the bootstrap"), vec![9]);
        peer.reply_watermark(9, TAG, 20);
        (peer, ids[0], ids[1])
    });
    let report = client
        .poll_mirror()
        .expect("a recovered view is not the call's failure");
    let (_peer, gone, alive) = h.join().unwrap();

    let events = log.take();
    let first_ingest = events
        .iter()
        .position(|e| matches!(e, Ev::Ingest(t, Shape::Stamped, 11) if *t == alive))
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

/// `reseed_by_name` onto an id that already holds a cursor advances it instead
/// of erasing a good copy.
///
/// The cursor-less registration's name now resolves to a view this same poll
/// already advanced. The old tail bootstrapped it unconditionally, discarding a
/// copy that was correct and re-reading the whole view.
#[test]
fn a_reseed_onto_a_live_copy_does_not_erase_it() {
    let (mut client, peer, log) = fixture(&[(7, "a", 0), (8, "a", 4)]);

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
        peer.quiet("the live copy is not re-read");
        peer
    });
    let report = client.poll_mirror().expect("the reseed lands on a live copy");
    let _peer = h.join().unwrap();

    let events = log.take();
    assert!(
        !events.iter().any(|e| matches!(e, Ev::Invalidate(8, Invalidate::Copy))),
        "the live copy must not be erased: {events:?}",
    );
    assert!(
        !events.iter().any(|e| matches!(e, Ev::Ingest(8, Shape::Plain, _))),
        "and must not be re-read whole: {events:?}",
    );
    assert!(
        matches!(report.as_slice(), [o] if o.view_id == 8 && o.cursor.is_some_and(|c| c.tick == 12)),
        "one entry, at the id the view ended up under: {report:?}",
    );
    assert!(!client.mirrored_ids().contains(&7), "the retired registration is gone");
}

/// A reply whose terminals arrive in the wrong order is refused, not ingested:
/// both views share a schema, so a misdirected block would decode cleanly and
/// leave a copy holding another view's rows.
#[test]
fn a_misdirected_view_reply_is_refused_rather_than_ingested() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);

    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        // The second position's view, answered at the first.
        peer.reply_watermark(ids[1], TAG, 11);
        peer
    });
    let report = client.poll_mirror().expect("a protocol error is reported per view");
    let _peer = h.join().unwrap();

    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Failed(_))),
        "no view may be advanced off a misdirected reply: {report:?}",
    );
    assert!(!log.saw(|e| matches!(e, Ev::Ingest(..))), "and nothing may be ingested",);
}

/// A batch that answers fewer views than it named fails the rest rather than
/// reporting them as advanced.
#[test]
fn a_short_batch_reply_fails_the_views_it_never_answered() {
    let (mut client, peer, log) = fixture(&[(7, "a", 4), (8, "b", 4)]);

    let h = std::thread::spawn(move || {
        let ids = peer.expect_poll("the poll");
        peer.reply_watermark(ids[0], TAG, 11);
        drop(peer); // the rest of the batch never comes
        ids
    });
    let report = client
        .poll_mirror()
        .expect("a short reply is not the call's own failure");
    let ids = h.join().unwrap();

    let answered = report.iter().find(|o| o.view_id == ids[0]).expect("an entry");
    assert!(
        matches!(answered.result, PollResult::Advanced) && answered.cursor.is_some_and(|c| c.tick == 11),
        "the view that was answered keeps its round: {report:?}",
    );
    let missing = report.iter().find(|o| o.view_id == ids[1]).expect("an entry");
    assert!(
        matches!(missing.result, PollResult::Failed(_)),
        "the view that was not answered fails: {report:?}",
    );
    assert!(
        !log.saw(|e| matches!(e, Ev::Ingest(t, _, _) if *t == ids[1])),
        "and nothing is ingested under it",
    );
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
        let deadline = std::time::Instant::now() + PATIENCE;
        while !watched.saw(|e| matches!(e, Ev::Ingest(t, _, _) if *t == ids[0])) {
            assert!(
                std::time::Instant::now() < deadline,
                "the first view's blocks must reach the store before the second view is answered",
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        peer.reply_watermark(ids[1], TAG, 12);
        peer
    });
    let report = client.poll_mirror().expect("both views advance");
    let _peer = h.join().unwrap();

    assert!(report.iter().all(|o| matches!(o.result, PollResult::Advanced)));
}

/// A host holding more views than one request may name chunks, and every view
/// still advances — asserted here rather than against a real server, where the
/// same claim would cost 65 `CREATE VIEW`s and their backfills.
#[test]
fn a_view_count_past_the_batch_cap_chunks() {
    let m = gnitz_wire::txn_frame::DELTA_POLL_MAX_VIEWS + 1;
    let names: Vec<String> = (0..m).map(|i| format!("v{i}")).collect();
    let views: Vec<(u64, &str, u64)> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (100 + i as u64, n.as_str(), 4))
        .collect();
    let (mut client, peer, _log) = fixture(&views);

    let h = std::thread::spawn(move || {
        let mut seen = Vec::new();
        for what in ["the full chunk", "the remainder"] {
            let ids = peer.expect_poll(what);
            for &id in &ids {
                peer.reply_watermark(id, TAG, 9);
            }
            seen.push(ids);
        }
        peer.quiet("two requests cover every view");
        (peer, seen)
    });
    let report = client.poll_mirror().expect("poll");
    let (_peer, seen) = h.join().unwrap();

    assert_eq!(
        seen.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![gnitz_wire::txn_frame::DELTA_POLL_MAX_VIEWS, 1],
        "the cap fills the first request and the rest opens a second",
    );
    assert_eq!(report.len(), m, "one entry per view");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "every view advances across the chunk boundary: {report:?}",
    );
}

/// A dead connection fails **every view's** poll rather than raising.
///
/// Batching the requests must not turn one transport failure into the call's:
/// the copies are intact, and every entry in the report is still worth reading.
#[test]
fn a_dead_connection_fails_each_view_rather_than_the_call() {
    let (mut client, peer, _log) = fixture(&[(7, "a", 4), (8, "b", 4)]);
    drop(peer);

    let report = client
        .poll_mirror()
        .expect("a dead socket is not the call's own failure");
    let mut ids: Vec<u64> = report.iter().map(|o| o.view_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![7, 8], "one entry per view: {report:?}");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Failed(_))),
        "each view names its own failure: {report:?}",
    );
    assert!(
        client.mirrors(7) && client.mirrors(8),
        "and both copies still answer at the round they reached",
    );
}
