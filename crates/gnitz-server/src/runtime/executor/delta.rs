//! The delta feed's two readers: DELTA_POLL, which reads a view's rounds from
//! the cursor each request names, and the pushed feeds, where the server
//! re-issues that read itself for the items a poll asked it to keep.
//!
//! A cursor is a round of its own view, so the subscribers standing at one
//! share one read. The copy's cursor stays the client's: a subscription can
//! end at any point, and its client continues from that cursor by polling.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};

use super::peer::Peer;
use super::tick::fresh_read_lock;
use super::{forward_scan, send_fault, send_msg, target_kind, Access, CatalogLock, Shared};
use crate::catalog::CatalogEngine;
use crate::runtime::frame::WireMsg;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::{PeerGone, SendBody, TrainLease};
use crate::runtime::sal::Read;
use crate::runtime::sync::{chan, oneshot, select2, ReadGuard};
use gnitz_wire::txn_frame::{decode_delta_items, delta_poll_kept, DeltaPollItem, SubId};
use gnitz_wire::{ControlHeader, Target, WireFault, WireStatus};

/// Delta reads at one SAL cut: the ceiling on the leases and trains a
/// DELTA_POLL or a pump pass puts on the master at a time.
const DELTA_CUT_READS: usize = 64;

/// The longest a SYNC_PUSHED is held, whatever wait it asks for.
const SYNC_MAX_WAIT: Duration = Duration::from_secs(3600);

/// One cut of delta reads, each given as `(view, after_tick, spec,
/// reply_layout)`: a read of `view`'s deltas after `after_tick`. Each lease is
/// paired with the round its read reaches, the view's last when the read was
/// written. No reads take no SAL hold.
async fn delta_cut<'s>(
    disp: &MasterDispatcher,
    reads: impl IntoIterator<Item = (u64, u64, &'s [u8], u64)>,
) -> Result<Vec<(TrainLease, u64)>, WireFault> {
    let mut reads = reads.into_iter().peekable();
    if reads.peek().is_none() {
        return Ok(Vec::new());
    }
    let mut reached = Vec::with_capacity(reads.size_hint().0);
    let leases = disp
        .scan_cut(|cut| {
            for (view, after_tick, spec, reply_layout) in reads {
                cut.read(Read::Delta {
                    view,
                    after_tick,
                    reply_layout,
                    spec: spec.into(),
                })?;
                reached.push(disp.last_delta_round(view));
            }
            Ok(())
        })
        .await?;
    Ok(leases.into_iter().zip(reached).collect())
}

// ---------------------------------------------------------------------------
// DELTA_POLL
// ---------------------------------------------------------------------------

/// What one view of a poll is answered with.
enum PollPosition {
    /// The view cannot be read at all.
    Fault(WireFault),
    /// The cursor already stands at `round`, the view's last, so its
    /// terminal is master-local: a fan-out per quiet read would cost a wakeup
    /// per worker. `tag` is the view's cursor tag.
    UpToDate { round: u64, tag: u64 },
    /// The view moved, or is read whole, and takes the next dispatch of the
    /// poll's cut. `tag` is the view's cursor tag.
    Moved { tag: u64 },
}

/// Where a poll of `item` stands.
fn poll_position(shared: &Shared, _catalog: &ReadGuard<CatalogLock>, item: DeltaPollItem) -> PollPosition {
    let tid = item.view.tid;
    let kind = match target_kind(&shared.cat(), item.view, Access::Read) {
        Ok(kind) => kind,
        Err(fault) => return PollPosition::Fault(fault),
    };
    if !kind.has_delta_feed() {
        return PollPosition::Fault(WireFault::from(format!(
            "delta_read: relation {tid} carries no delta feed; \
             create the view WITH (delta = '<size>') to subscribe to it"
        )));
    }
    let tag = shared.feeds.cursor_tag(&shared.cat(), tid, item.spec);
    let Some(cursor) = item.from else {
        return PollPosition::Moved { tag };
    };
    if cursor.tag != tag {
        return PollPosition::Fault(foreign_cursor(tid));
    }
    let last = shared.dispatcher.last_delta_round(tid);
    if cursor.tick.get() >= last {
        PollPosition::UpToDate { round: last, tag }
    } else {
        PollPosition::Moved { tag }
    }
}

/// The refusal of a cursor whose tag is not the view's.
fn foreign_cursor(view: u64) -> WireFault {
    WireFault {
        status: WireStatus::DeltaExpired,
        text: format!("delta cursor of relation {view} names another boot, relation or spec; re-read at 0"),
    }
}

/// Whether `feed` still reads the view it was keyed under: the view exists, and
/// is the registration its tag was taken of.
fn feed_readable(shared: &Shared, feed: &Feed) -> Result<(), WireFault> {
    let cat = shared.cat();
    let view = feed.key.view;
    target_kind(&cat, view, Access::Read)?;
    if shared.feeds.cursor_tag(&cat, view.tid, &feed.key.spec) != feed.tag {
        return Err(foreign_cursor(view.tid));
    }
    Ok(())
}

/// DELTA_POLL: read N views in one request, each from its own cursor to the
/// last round that reached it, and keep each one the request asks to as a
/// subscription of `subs` from there.
///
/// `Err` rejects the frame; a view's own failure is its fault frame, and the
/// rest of the poll continues.
pub(super) async fn handle_delta_poll(
    shared: &Rc<Shared>,
    peer: &Peer,
    subs: &mut Subscriptions,
    prologue: &ControlHeader,
    body: &[u8],
) -> Result<(), WireFault> {
    let views = decode_delta_items(body).map_err(|e| format!("decode error: {e}"))?;
    let disp = &shared.dispatcher;
    let mut kept = delta_poll_kept(prologue, views.len()).map_err(|e| format!("decode error: {e}"))?;
    // The poll drained once, for this lock; a later slice takes the lock alone.
    let mut first_lock = Some(
        fresh_read_lock(
            shared,
            views.iter().map(|v| v.view.tid),
            shared.dispatcher.reactor().now(),
        )
        .await?,
    );

    // One slice at a time: one catalog lock and — for however many of its views
    // moved — one broadcast. A view read whole is a slice of its own: a cut holds
    // the workers until its last train is taken, and that train is a view long.
    let slices = views
        .chunk_by(|a, b| a.from.is_some() && b.from.is_some())
        .flat_map(|run| run.chunks(DELTA_CUT_READS));
    for slice in slices {
        // ── Phase 1: classify under the catalog lock, dispatch one cut ─────
        let catalog = match first_lock.take() {
            Some(g) => g,
            None => shared.catalog_rwlock.read().await,
        };
        let positions: Vec<PollPosition> = slice.iter().map(|&v| poll_position(shared, &catalog, v)).collect();
        let moved = || {
            let judged = slice.iter().zip(&positions);
            judged.filter_map(|(v, p)| matches!(p, PollPosition::Moved { .. }).then_some(v))
        };
        // Each moved view's read, with the round it reaches.
        let mut dispatches = delta_cut(
            disp,
            moved().map(|i| (i.view.tid, i.from.map_or(0, |c| c.tick.get()), i.spec, i.reply_layout)),
        )
        .await?
        .into_iter();
        // Phase 2 reads no catalog state, and holding the lock across the
        // forward would block DDL.
        drop(catalog);

        // ── Phase 2: one terminal per view, in request order ───────────────
        // Taken in step with the `Moved`s that were pushed. A dispatch left
        // undrained — an earlier return dropped it — discards the rest of its
        // train at the ring boundary.
        for (item, position) in slice.iter().zip(positions) {
            // The round the view's terminal names, and its cursor tag.
            let result = match position {
                PollPosition::Fault(fault) => Err(fault),
                PollPosition::UpToDate { round, tag } => Ok((round, tag)),
                PollPosition::Moved { tag } => {
                    let (lease, round) = dispatches.next().expect("one dispatch per moved view");
                    forward_scan(peer, &lease).await.map(|()| (round, tag))
                }
            };
            let tid = item.view.tid;
            if let Some(id) = kept.as_mut().and_then(Iterator::next) {
                subs.leave(&shared.feeds, |kept| kept == id);
                if let Ok((round, tag)) = result {
                    subs.join(&shared.feeds, id, item, round, tag);
                }
            }
            match result {
                // Corked, not sent: the terminal joins whatever the forward corked.
                Ok((round, tag)) => send_msg(peer, terminal(tid, round, tag)),
                Err(fault) => send_fault(peer, tid, &fault),
            }
            // Carry no more than the budget into the next view, and learn here
            // rather than at the end if the client is gone.
            if peer.flush_if_full().await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// The frame that ends a delta train: the cursor to read `view` from next.
fn terminal(view: u64, round: u64, tag: u64) -> WireMsg<'static> {
    WireMsg {
        target_id: view,
        arg0: round,
        arg1: tag,
        ..Default::default()
    }
}

/// The catalog read lock a sync of the views `ids` is answered under, every
/// commit reaching one of them ticked. A `quiet` sync is first held until a
/// relation one of them reads changes or `until` comes.
async fn hold_lock(
    shared: &Rc<Shared>,
    ids: &[u64],
    until: Instant,
    quiet: impl FnOnce() -> bool,
) -> Result<ReadGuard<CatalogLock>, WireFault> {
    let ids = || ids.iter().copied();
    let waiting = until > shared.dispatcher.reactor().now();
    let g = fresh_read_lock(shared, ids(), until).await?;
    if !waiting {
        return Ok(g);
    }
    // No await from the test to the park, so no commit is acknowledged between
    // them. The drain above was one: a commit it did not take is un-ticked here.
    let mut watched = shared.cat().dag.source_closure(ids());
    if !(shared.ticks.all_ticked(&watched) && quiet()) {
        return Ok(g);
    }
    // A commit or a drop reaches a view through the view or anything it reads.
    watched.extend(ids());
    let mut parked = shared.feeds.waiters.park(watched);
    drop(g);
    select2(&mut parked.woken, shared.dispatcher.reactor().sleep_until(until)).await;
    drop(parked);
    fresh_read_lock(shared, ids(), until).await
}

// ---------------------------------------------------------------------------
// Pushed feeds
// ---------------------------------------------------------------------------

/// What subscribers share a read of: one view, as one RESOLVE answered it,
/// under one spec and reply layout.
#[derive(Clone, PartialEq, Eq, Hash)]
struct FeedKey {
    view: Target,
    layout: u64,
    spec: Vec<u8>,
}

struct Subscriber {
    /// The id its connection asked for it under.
    id: SubId,
    /// The round this subscriber has been queued every row through; `None`
    /// once its feed ended it.
    after_tick: Cell<Option<u64>>,
    /// The round its client's cursor was last sent: the one it joined at, or
    /// that of its last train.
    told: Cell<u64>,
    out: Rc<Outbox>,
}

impl Subscriber {
    /// Whether no round since this subscriber's cursor reached the view it is
    /// fed, whose last round is `last`.
    fn caught_up(&self, last: u64) -> bool {
        self.after_tick.get().is_some_and(|after| after >= last)
    }
}

/// The pushed trains queued for one connection, which its own task ships
/// between two replies.
#[derive(Default)]
struct Outbox {
    trains: RefCell<Vec<Train>>,
    unsynced: Cell<usize>,
}

/// One queued train: the subscription's `(view, id)`, whose opening marker is
/// encoded when the train ships, and the frames after it, which every
/// connection sent the same ones shares.
struct Train {
    view: u64,
    id: SubId,
    body: Rc<Vec<u8>>,
}

impl Outbox {
    /// Bytes queued since the connection's last SYNC_PUSHED was answered:
    /// what its client may hold unread.
    fn unsynced(&self) -> usize {
        self.unsynced.get()
    }

    /// A SYNC_PUSHED was answered behind everything queued.
    fn synced(&self) {
        self.unsynced.set(0);
    }

    /// Whether every train queued has been shipped.
    fn is_empty(&self) -> bool {
        self.trains.borrow().is_empty()
    }

    /// Queue one train of subscription `id` to `view`: its opening marker
    /// and the frames after it.
    fn send(&self, view: u64, id: SubId, body: Rc<Vec<u8>>) {
        self.unsynced.set(self.unsynced.get() + MARKER_BYTES + body.len());
        self.trains.borrow_mut().push(Train { view, id, body });
    }
}

/// One [`FeedKey`]'s subscribers.
struct Feed {
    key: FeedKey,
    /// The cursor tag of every read of `key`.
    tag: u64,
    members: RefCell<Vec<Rc<Subscriber>>>,
    /// A request waits for the pump to read it. The pump reads only such a
    /// feed: a round nobody asks for is read with the rounds after it, as a
    /// poll would read them.
    asked: Cell<bool>,
}

/// The SYNC_PUSHEDs held back with nothing to report, each with the relations
/// whose change ends its wait.
#[derive(Default)]
struct SyncWaiters {
    next_id: Cell<u64>,
    parked: RefCell<Vec<ParkedSync>>,
}

struct ParkedSync {
    id: u64,
    watched: FxHashSet<u64>,
    wake: oneshot::Sender<()>,
}

/// One sync's place among the [`SyncWaiters`], given up on drop.
struct Parked<'a> {
    waiters: &'a SyncWaiters,
    id: u64,
    woken: oneshot::Receiver<()>,
}

impl SyncWaiters {
    fn park(&self, watched: FxHashSet<u64>) -> Parked<'_> {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        let (wake, woken) = oneshot::channel();
        self.parked.borrow_mut().push(ParkedSync { id, watched, wake });
        Parked { waiters: self, id, woken }
    }

    /// Wake every sync watching `relation`.
    fn wake(&self, relation: u64) {
        let mut parked = self.parked.borrow_mut();
        for sync in parked.extract_if(.., |p| p.watched.contains(&relation)) {
            sync.wake.send(());
        }
    }
}

impl Drop for Parked<'_> {
    fn drop(&mut self) {
        self.waiters.parked.borrow_mut().retain(|p| p.id != self.id);
    }
}

/// A `u64` from the OS entropy pool for [`Feeds::cursor_tag`], so two
/// back-to-back boots cannot collide.
fn boot_nonce() -> u64 {
    let rng = ring::rand::SystemRandom::new();
    u64::from_ne_bytes(
        ring::rand::generate(&rng)
            .expect("the OS random number generator")
            .expose(),
    )
}

/// The delta feed's state: the feeds with a subscriber, the way to the one task
/// that reads for them, [`pump`], the syncs held back, and what every cursor tag
/// of this boot is drawn from.
pub(super) struct Feeds {
    live: RefCell<FxHashMap<FeedKey, Rc<Feed>>>,
    /// The syncs waiting for the pump's next pass.
    asked: chan::Sender<oneshot::Sender<()>>,
    waiters: SyncWaiters,
    /// The most bytes of pushed trains sent one connection between two of its
    /// SYNC_PUSHEDs, and so the most its client holds unread
    /// (`GNITZ_PUSH_QUEUE_BYTES`). A subscription whose train would pass it
    /// is ended, and continues by polling.
    queue_bytes: usize,
    /// A `u64` taken from the OS at boot, mixed into every cursor tag;
    /// [`Feeds::cursor_tag`] states what the tag answers.
    boot_nonce: u64,
}

impl Feeds {
    /// The feeds, and the requests [`pump`] is to be spawned on.
    pub(super) fn new() -> (Feeds, chan::Receiver<oneshot::Sender<()>>) {
        let (asked, asks) = chan::unbounded();
        let feeds = Feeds {
            live: RefCell::default(),
            asked,
            waiters: SyncWaiters::default(),
            queue_bytes: gnitz_foundation::env::env_num("GNITZ_PUSH_QUEUE_BYTES", 8usize << 20),
            boot_nonce: boot_nonce(),
        };
        (feeds, asks)
    }

    /// Wake every sync watching `relation`.
    pub(super) fn wake(&self, relation: u64) {
        self.waiters.wake(relation);
    }

    /// The cursor tag a delta reply of `view` carries: distinct for every (boot,
    /// registration) and, with fair odds, every spec the view is read under — so
    /// a cursor from another boot, from another view or a dropped one, or polled
    /// under another spec is recognizably foreign. A registration is
    /// [`CatalogEngine::registration`]; `spec` is the request's encoded bytes.
    pub(super) fn cursor_tag(&self, cat: &CatalogEngine, view: u64, spec: &[u8]) -> u64 {
        let registration = cat
            .registration(view)
            .expect("a cursor tag is taken of a registered view");
        // splitmix64's finalizer, a bijection on u64: under one spec, two
        // registrations of one boot never share a tag.
        let mut z = registration.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.boot_nonce ^ (z ^ (z >> 31)) ^ gnitz_wire::checksum(spec)
    }

    /// `feed` lost a subscriber: forget it with its last.
    fn retire(&self, feed: &Rc<Feed>) {
        if !feed.members.borrow().is_empty() {
            return;
        }
        let mut live = self.live.borrow_mut();
        if live.get(&feed.key).is_some_and(|live| Rc::ptr_eq(live, feed)) {
            live.remove(&feed.key);
        }
    }

    /// End every subscription of `feed` with `fault`.
    fn end(&self, feed: &Rc<Feed>, fault: &WireFault) {
        fail_subscribers(feed.key.view.tid, &feed.members.take(), fault);
        self.retire(feed);
    }
}

/// One connection's side of the pushed feeds.
#[derive(Default)]
pub(super) struct Subscriptions {
    asked: Asked,
    /// The trains queued for this connection.
    out: Rc<Outbox>,
    /// The wait of the oldest sync, over `asked` as it was when the wait began.
    wait: Option<SyncWait>,
}

/// What a connection asked for and still has.
#[derive(Default)]
struct Asked {
    /// Its subscriptions, each with its feed. An entry outlives a subscription
    /// its feed ended, until a sync next waits.
    held: Vec<(Rc<Subscriber>, Rc<Feed>)>,
    /// Its unanswered syncs, oldest first: until when each may be held, or
    /// what refused it.
    syncs: VecDeque<Result<Instant, WireFault>>,
}

type SyncWait = Pin<Box<dyn Future<Output = Result<(), WireFault>>>>;

impl Subscriptions {
    /// `asked`, to change it: the wait begins again.
    fn asked_mut(&mut self) -> &mut Asked {
        self.wait = None;
        &mut self.asked
    }

    /// Keep `item` as the subscription `id`, queued through `round`; `tag` is
    /// the cursor tag of its reads.
    fn join(&mut self, feeds: &Feeds, id: SubId, item: &DeltaPollItem, round: u64, tag: u64) {
        let joined = Rc::new(Subscriber {
            id,
            after_tick: Cell::new(Some(round)),
            told: Cell::new(round),
            out: Rc::clone(&self.out),
        });
        let key = FeedKey {
            view: item.view,
            layout: item.reply_layout,
            spec: item.spec.to_vec(),
        };
        // A feed of the view's earlier registration ends here.
        let stale = feeds.live.borrow().get(&key).filter(|f| f.tag != tag).cloned();
        if let Some(stale) = stale {
            feeds.end(&stale, &foreign_cursor(item.view.tid));
        }
        let feed = Rc::clone(feeds.live.borrow_mut().entry(key.clone()).or_insert_with(|| {
            Rc::new(Feed {
                key,
                tag,
                members: RefCell::default(),
                asked: Cell::new(false),
            })
        }));
        feed.members.borrow_mut().push(Rc::clone(&joined));
        self.asked_mut().held.push((joined, feed));
    }

    /// Send each subscription queued through rounds that left it no row where
    /// it stands, as a train of its terminal alone: its client's cursor passes
    /// those rounds too.
    fn tell(&self, peer: &Peer) {
        debug_assert!(self.out.is_empty(), "a position is sent behind every train");
        for (sub, feed) in &self.asked.held {
            let Some(after) = sub.after_tick.get() else { continue };
            if sub.told.replace(after) != after {
                let tid = feed.key.view.tid;
                peer.cork_with(|out| {
                    pushed_marker(tid, sub.id).encode_framed(out);
                    terminal(tid, after, feed.tag).encode_framed(out);
                });
            }
        }
    }

    /// Leave the feed of every subscription whose id `gone` names.
    pub(super) fn leave(&mut self, feeds: &Feeds, gone: impl Fn(SubId) -> bool) {
        for (sub, feed) in self.asked_mut().held.extract_if(.., |(sub, _)| gone(sub.id)) {
            feed.members.borrow_mut().retain(|m| !Rc::ptr_eq(m, &sub));
            feeds.retire(&feed);
        }
    }

    /// SYNC_PUSHED: end every subscription `held` does not name, and owe the
    /// connection a sync held up to `wait_ms`. An undecodable `held` is what
    /// that sync is refused with.
    pub(super) fn ask_sync(&mut self, shared: &Shared, held: Result<Vec<SubId>, WireFault>, wait_ms: u64) {
        if let Ok(held) = &held {
            self.leave(&shared.feeds, |id| !held.contains(&id));
        }
        let now = shared.dispatcher.reactor().now();
        self.asked_mut()
            .syncs
            .push_back(held.map(|_| now + Duration::from_millis(wait_ms).min(SYNC_MAX_WAIT)));
    }

    /// The [`sync_wait`] of the oldest unanswered sync, begun if none is
    /// under way.
    pub(super) fn sync_wait(&mut self, shared: &Rc<Shared>) -> Option<&mut SyncWait> {
        if self.wait.is_none() {
            let Asked { held, syncs } = &mut self.asked;
            let deadline = match syncs.len() {
                0 => return None,
                1 => syncs[0].clone(),
                // Answered in the order asked, so held no longer.
                _ => syncs[0].clone().map(|_| shared.dispatcher.reactor().now()),
            };
            held.retain(|(sub, _)| sub.after_tick.get().is_some());
            let (shared, out, held) = (Rc::clone(shared), Rc::clone(&self.out), held.clone());
            self.wait = Some(Box::pin(async move { sync_wait(&shared, &out, deadline?, held).await }));
        }
        self.wait.as_mut()
    }

    /// Answer the oldest sync, whose wait ended as `synced`: the trains
    /// queued, then the frame that ends it.
    pub(super) async fn answer_sync(&mut self, peer: &Peer, synced: Result<(), WireFault>) {
        self.asked_mut().syncs.pop_front();
        if self.ship(peer).await.is_err() {
            return;
        }
        let end = match &synced {
            Ok(()) => {
                self.tell(peer);
                self.out.synced();
                WireMsg::default()
            }
            Err(fault) => WireMsg::fault(fault),
        };
        let flags = gnitz_wire::WireFlags {
            lane: gnitz_wire::WireLane::SyncAnswer,
            ..Default::default()
        };
        peer.cork_with(|out| WireMsg { flags, ..end }.encode_framed(out));
    }

    /// Ship every queued train behind what is corked.
    pub(super) async fn ship(&self, peer: &Peer) -> Result<(), PeerGone> {
        loop {
            let trains = self.out.trains.take();
            if trains.is_empty() {
                return Ok(());
            }
            for Train { view, id, body } in trains {
                peer.cork_with(|out| pushed_marker(view, id).encode_framed(out));
                peer.send(SendBody::Shared(body)).await?;
            }
        }
    }
}

/// The framed size of every [`pushed_marker`]: a head with no blob, schema
/// or data.
const MARKER_BYTES: usize = gnitz_wire::FRAME_LEN_PREFIX_BYTES + gnitz_wire::frame_head_size(0, None);

/// The frame that opens a pushed train of subscription `id`.
fn pushed_marker(view: u64, id: SubId) -> WireMsg<'static> {
    let marker = WireMsg {
        target_id: view,
        arg0: id.get(),
        flags: gnitz_wire::WireFlags {
            lane: gnitz_wire::WireLane::PushedTrain,
            ..Default::default()
        },
        ..Default::default()
    };
    debug_assert_eq!(gnitz_wire::FRAME_LEN_PREFIX_BYTES + marker.size(), MARKER_BYTES);
    marker
}

/// `msg`'s framed bytes, for a subscriber's queue.
fn framed(msg: WireMsg<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    msg.encode_framed(&mut out);
    out
}

/// End `members`' subscriptions with `fault`. A fault frame is queued whatever
/// the queue holds: it is what tells the subscriber to stop waiting.
fn fail_subscribers<'a>(view: u64, members: impl IntoIterator<Item = &'a Rc<Subscriber>>, fault: &WireFault) {
    let frame = Rc::new(framed(WireMsg { target_id: view, ..WireMsg::fault(fault) }));
    for m in members {
        m.after_tick.set(None);
        m.out.send(view, m.id, Rc::clone(&frame));
    }
}

fn lagged(view: u64) -> WireFault {
    format!("subscription to relation {view} fell behind what one connection is queued; poll from its cursor").into()
}

/// Wait until a sync of `held` can be answered: each subscription is queued
/// every round acknowledged before it, and one was queued something since the
/// last sync or `deadline` has passed.
async fn sync_wait(
    shared: &Rc<Shared>,
    out: &Outbox,
    deadline: Instant,
    mut held: Vec<(Rc<Subscriber>, Rc<Feed>)>,
) -> Result<(), WireFault> {
    let feeds = &shared.feeds;
    // Whether a subscription has been queued every round its view was reached
    // by. Asked under the catalog lock.
    let caught_up = |(sub, feed): &(Rc<Subscriber>, Rc<Feed>)| {
        feed_readable(shared, feed).is_ok() && sub.caught_up(shared.dispatcher.last_delta_round(feed.key.view.tid))
    };
    loop {
        held.retain(|(sub, _)| sub.after_tick.get().is_some());
        let ids: Vec<u64> = held.iter().map(|(_, feed)| feed.key.view.tid).collect();
        // With no subscription there is nothing to hold for.
        let until = if ids.is_empty() {
            shared.dispatcher.reactor().now()
        } else {
            deadline
        };
        let quiet = || out.unsynced() == 0 && held.iter().all(caught_up);
        let catalog = hold_lock(shared, &ids, until, quiet).await?;
        // Every commit acknowledged before the sync is ticked here, so only a
        // subscription behind asks for a pump.
        let mut asked = false;
        for joined in held.iter().filter(|(sub, _)| sub.after_tick.get().is_some()) {
            if !caught_up(joined) {
                joined.1.asked.set(true);
                asked = true;
            }
        }
        if asked {
            let (done, pumped) = oneshot::channel();
            feeds.asked.send(done);
            drop(catalog);
            pumped.await;
        } else {
            drop(catalog);
        }
        if out.unsynced() > 0 || shared.dispatcher.reactor().now() >= until {
            return Ok(());
        }
    }
}

/// Read the feeds the syncs on `asks` ask for. A pass answers the syncs
/// waiting when it starts: one read per distinct cursor behind among a feed's
/// subscribers, each subscriber queued its train.
pub(super) async fn pump(shared: Rc<Shared>, mut asks: chan::Receiver<oneshot::Sender<()>>) {
    let feeds = &shared.feeds;
    let disp = &shared.dispatcher;
    loop {
        // Taken together, so a pass reads every feed its requests asked for
        // and positions each after they asked.
        let mut waiting = vec![asks.recv().await];
        while let Some(done) = asks.try_recv() {
            waiting.push(done);
        }
        let asked: Vec<Rc<Feed>> = {
            let live = feeds.live.borrow();
            live.values().filter(|feed| feed.asked.take()).cloned().collect()
        };
        // Each distinct cursor that is behind, of the feeds still readable.
        let mut first_lock = Some(shared.catalog_rwlock.read().await);
        let mut behind: Vec<(&Rc<Feed>, u64)> = Vec::new();
        for feed in &asked {
            if let Err(fault) = feed_readable(&shared, feed) {
                feeds.end(feed, &fault);
                continue;
            }
            let first = behind.len();
            let last = disp.last_delta_round(feed.key.view.tid);
            for m in feed.members.borrow().iter().filter(|m| !m.caught_up(last)) {
                let cursor = m.after_tick.get().expect("a member is one its feed has not ended");
                if behind[first..].iter().all(|&(_, read)| read != cursor) {
                    behind.push((feed, cursor));
                }
            }
        }
        for slice in behind.chunks(DELTA_CUT_READS) {
            let catalog = match first_lock.take() {
                Some(g) => g,
                None => shared.catalog_rwlock.read().await,
            };
            let mut reads = Vec::with_capacity(slice.len());
            for &(feed, cursor) in slice {
                match feed_readable(&shared, feed) {
                    Ok(_) => reads.push((feed, cursor)),
                    Err(fault) => feeds.end(feed, &fault),
                }
            }
            let cut = delta_cut(
                disp,
                reads
                    .iter()
                    .map(|&(feed, cursor)| (feed.key.view.tid, cursor, &*feed.key.spec, feed.key.layout)),
            )
            .await;
            drop(catalog);
            match cut {
                Ok(cut) => {
                    for ((feed, cursor), (lease, round)) in reads.into_iter().zip(cut) {
                        queue_train(&shared, feed, cursor, round, lease).await;
                    }
                }
                Err(fault) => reads.iter().for_each(|(feed, _)| feeds.end(feed, &fault)),
            }
        }
        drop(first_lock);
        for done in waiting {
            done.send(());
        }
    }
}

/// Queue the subscribers of `feed` standing at `cursor` the train `lease`
/// reads: the feed's rounds after `cursor`, through `round`, the view's last.
async fn queue_train(shared: &Shared, feed: &Rc<Feed>, cursor: u64, round: u64, lease: TrainLease) {
    let tid = feed.key.view.tid;
    let cap = shared.feeds.queue_bytes;
    // Copied off the ring, so no subscriber's pace holds a ring slot. A train
    // past the cap is not read to its end.
    let mut body = Vec::new();
    let read = loop {
        match lease.next().await {
            Ok(Some(_)) if body.len() > cap => break Err(lagged(tid)),
            Ok(Some(frame)) => body.extend_from_slice(frame.slot.frame_bytes()),
            Ok(None) => break Ok(()),
            Err(fault) => break Err(fault),
        }
    };
    drop(lease);
    // The subscribers this read is for: a round is past every cursor that was
    // behind it, so none is taken for a later read's.
    let mut members = feed.members.borrow_mut();
    let reading = members.iter().filter(|m| m.after_tick.get() == Some(cursor));
    match read {
        Err(fault) => fail_subscribers(tid, reading, &fault),
        Ok(()) => {
            // Rounds that left the subscription no row are sent as no train.
            let rows = !body.is_empty();
            terminal(tid, round, feed.tag).encode_framed(&mut body);
            let body = Rc::new(body);
            for m in reading {
                if rows && m.out.unsynced() + body.len() > cap {
                    fail_subscribers(tid, [m], &lagged(tid));
                    continue;
                }
                m.after_tick.set(Some(round));
                if rows {
                    m.told.set(round);
                    m.out.send(tid, m.id, Rc::clone(&body));
                }
            }
        }
    }
    members.retain(|m| m.after_tick.get().is_some());
    drop(members);
    shared.feeds.retire(feed);
}

#[cfg(test)]
#[path = "tests/delta.rs"]
mod tests;
