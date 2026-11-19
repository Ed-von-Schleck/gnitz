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

use super::tick::fresh_read_lock;
use super::{Access, CatalogLock, Shared, cork_fault, cork_msg, decode_fault, forward_scan, target_kind};
use crate::runtime::frame::WireMsg;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::{Peer, PeerGone, SendBody, Train, TrainLease};
use crate::runtime::sal::Read;
use crate::runtime::sync::{ReadGuard, chan, oneshot, select2};
use gnitz_wire::multi_frame::{DeltaCursor, DeltaPollItem, SubId, decode_delta_poll};
use gnitz_wire::{ControlHeader, Target, WireFault, WireStatus};

/// Delta reads at one SAL cut: the ceiling on the trains a DELTA_POLL or a
/// pump pass puts on the master at a time.
const DELTA_CUT_READS: usize = 64;

/// The longest a SYNC_PUSHED is held, whatever wait it asks for.
const SYNC_MAX_WAIT: Duration = Duration::from_secs(3600);

/// The reads of one delta cut, taken in the order given.
struct CutReads {
    /// The cut's lease and the round each of its reads reaches; `None` for a
    /// cut of no reads, or the refusal that ended the cut, the reads written
    /// ahead of it given up with their lease.
    cut: Result<Option<(TrainLease, std::vec::IntoIter<u64>)>, WireFault>,
}

impl CutReads {
    /// The next read's train and the round it reaches, or the cut's refusal.
    async fn next(&mut self) -> Result<(Train<'_>, u64), WireFault> {
        match &mut self.cut {
            Ok(cut) => {
                let (lease, rounds) = cut.as_mut().expect("one train per read");
                let round = rounds.next().expect("one train per read");
                Ok((lease.next_train().await, round))
            }
            Err(fault) => Err(fault.clone()),
        }
    }
}

/// One cut of delta reads, each given as `(view, after_tick, spec,
/// reply_layout)`: a read of `view`'s deltas after `after_tick`. Each read is
/// paired with the round it reaches, the view's last when the read was
/// written. No reads take no SAL hold.
async fn delta_cut<'s>(
    disp: &MasterDispatcher,
    reads: impl IntoIterator<Item = (u64, u64, &'s [u8], u64)>,
) -> CutReads {
    let mut reads = reads.into_iter().peekable();
    if reads.peek().is_none() {
        return CutReads { cut: Ok(None) };
    }
    let mut rounds = Vec::with_capacity(reads.size_hint().0);
    let cut = disp.scan_cut(|cut| {
        for (view, after_tick, spec, reply_layout) in reads {
            cut.read(Read::Delta { view, after_tick, reply_layout, spec })?;
            rounds.push(disp.last_delta_round(view));
        }
        Ok(())
    });
    CutReads {
        cut: cut.await.map(|lease| Some((lease, rounds.into_iter()))),
    }
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
    let tag = shared.feeds.cursor_tag(tid, item.spec);
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

/// Whether `feed` still reads the view it was keyed under: the view exists and
/// resolves as it did. A view id is never created under twice, so that is the
/// view its tag was taken of.
fn feed_readable(shared: &Shared, feed: &Feed) -> Result<(), WireFault> {
    target_kind(&shared.cat(), feed.key.view, Access::Read).map(drop)
}

/// DELTA_POLL: read N views in one request, each from its own cursor to the
/// last round that reached it, and keep each one the request asks to as a
/// subscription of `subs` from there.
///
/// `Err` rejects the frame, a decode error or a failed drain, ahead of anything
/// corked; a view's own failure, its read refused included, is its fault frame,
/// and the rest of the poll continues.
pub(super) async fn handle_delta_poll(
    shared: &Shared,
    peer: &mut Peer,
    subs: &mut Subscriptions,
    prologue: &ControlHeader,
    body: &[u8],
) -> Result<(), WireFault> {
    let poll = decode_delta_poll(body, prologue).map_err(decode_fault)?;
    let views = &poll.items;
    let disp = &shared.dispatcher;
    let mut kept = poll.kept();
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
        .await;
        // Phase 2 reads no catalog state, and holding the lock across the
        // forward would block DDL.
        drop(catalog);

        // ── Phase 2: one terminal per view, in request order ───────────────
        // Taken in step with the `Moved`s that were pushed. A train left
        // unread is read off when the next read is taken, or dropped with the
        // cut's lease.
        for (item, position) in slice.iter().zip(positions) {
            // The round the view's terminal names, and its cursor tag.
            let result = match position {
                PollPosition::Fault(fault) => Err(fault),
                PollPosition::UpToDate { round, tag } => Ok((round, tag)),
                PollPosition::Moved { tag } => match dispatches.next().await {
                    Ok((train, round)) => forward_scan(peer, train).await.map(|()| (round, tag)),
                    Err(fault) => Err(fault),
                },
            };
            let tid = item.view.tid;
            if let Some(id) = kept.as_mut().and_then(Iterator::next) {
                subs.leave(&shared.feeds, |kept| kept == id);
                if let Ok((round, tag)) = result {
                    subs.join(&shared.feeds, id, item, round, tag);
                }
            }
            match result {
                Ok((round, tag)) => cork_msg(peer, terminal(tid, round, tag)),
                Err(fault) => cork_fault(peer, tid, &fault),
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
    let at = DeltaCursor::from_pair(tag, round).expect("round 0 is the boot round, never emitted");
    let msg = WireMsg {
        hdr: ControlHeader::delta_terminal(view, at),
        ..Default::default()
    };
    debug_assert_eq!(gnitz_wire::FRAME_LEN_PREFIX_BYTES + msg.size(), HEAD_FRAME_BYTES);
    msg
}

/// The framed size of every [`terminal`] and [`pushed_marker`]: a head with no
/// blob or data.
const HEAD_FRAME_BYTES: usize = gnitz_wire::FRAME_LEN_PREFIX_BYTES + gnitz_wire::CTRL_HEADER_SIZE;

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
    trains: RefCell<Vec<QueuedTrain>>,
    unsynced: Cell<usize>,
}

/// One queued train: the subscription's `(view, id)`, whose opening marker is
/// encoded when the train ships, and the frames after it, which every
/// connection sent the same ones shares.
struct QueuedTrain {
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
        self.unsynced.update(|v| v + HEAD_FRAME_BYTES + body.len());
        self.trains.borrow_mut().push(QueuedTrain { view, id, body });
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
/// that reads for them, [`pump`], and what every cursor tag of this boot is
/// drawn from.
pub(super) struct Feeds {
    live: RefCell<FxHashMap<FeedKey, Rc<Feed>>>,
    /// The syncs waiting for the pump's next pass.
    asked: chan::Sender<oneshot::Sender<()>>,
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
            queue_bytes: gnitz_foundation::env::env_num("GNITZ_PUSH_QUEUE_BYTES", 8usize << 20),
            boot_nonce: boot_nonce(),
        };
        (feeds, asks)
    }

    /// The cursor tag a delta reply of `view` carries: distinct for every (boot,
    /// view) and, with fair odds, every spec the view is read under — so a cursor
    /// from another boot or another view, or polled under another spec, is
    /// recognizably foreign. A view id is never created under twice, so a dropped
    /// view's cursor is refused as one of an unregistered relation. `spec` is the
    /// request's encoded bytes.
    fn cursor_tag(&self, view: u64, spec: &[u8]) -> u64 {
        // A bijection: under one spec, two views of one boot never share a tag.
        self.boot_nonce ^ gnitz_foundation::rng::splitmix(view) ^ gnitz_wire::checksum(spec)
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
    fn tell(&self, peer: &mut Peer) {
        debug_assert!(self.out.is_empty(), "a position is sent behind every train");
        for (sub, feed) in &self.asked.held {
            let Some(after) = sub.after_tick.get() else { continue };
            if sub.told.replace(after) != after {
                let tid = feed.key.view.tid;
                cork_msg(peer, pushed_marker(tid, sub.id));
                cork_msg(peer, terminal(tid, after, feed.tag));
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
    pub(super) async fn answer_sync(&mut self, peer: &mut Peer, synced: Result<(), WireFault>) {
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
            Err(fault) => WireMsg::fault(0, fault),
        };
        let hdr = ControlHeader::sync_answer(end.hdr.status);
        cork_msg(peer, WireMsg { hdr, ..end });
    }

    /// Ship every queued train behind what is corked.
    pub(super) async fn ship(&self, peer: &mut Peer) -> Result<(), PeerGone> {
        // Each pass swaps the drained Vec back in for the trains queued since,
        // so the Outbox keeps one allocation across ships.
        let mut batch = Vec::new();
        loop {
            std::mem::swap(&mut batch, &mut *self.out.trains.borrow_mut());
            if batch.is_empty() {
                return Ok(());
            }
            for QueuedTrain { view, id, body } in batch.drain(..) {
                cork_msg(peer, pushed_marker(view, id));
                peer.send(SendBody::Shared(body)).await?;
            }
        }
    }
}

/// The frame that opens a pushed train of subscription `id`.
fn pushed_marker(view: u64, id: SubId) -> WireMsg<'static> {
    let marker = WireMsg {
        hdr: ControlHeader::pushed_train(view, id),
        ..Default::default()
    };
    debug_assert_eq!(gnitz_wire::FRAME_LEN_PREFIX_BYTES + marker.size(), HEAD_FRAME_BYTES);
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
    let frame = Rc::new(framed(WireMsg::fault(view, fault)));
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
    shared: &Shared,
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
        let views = || held.iter().map(|(_, feed)| feed.key.view.tid);
        // With no subscription there is nothing to hold for.
        let until = if held.is_empty() {
            shared.dispatcher.reactor().now()
        } else {
            deadline
        };
        let catalog = fresh_read_lock(shared, views(), until).await?;
        // Nothing to report: held until a relation one of the views reads
        // gains an un-ticked commit, a DDL, or `until`. Those wakes are every
        // way a sync held here gains something to report: a lag or a failed
        // read ends only a subscriber behind, whose connection is not held,
        // and a feed ended as unreadable became so by a DDL. A commit the
        // drain above did not take is un-ticked here, and refuses the watch.
        if until > shared.dispatcher.reactor().now() && out.unsynced() == 0 && held.iter().all(caught_up) {
            let watched: FxHashSet<u64> = {
                let cat = shared.cat();
                views().flat_map(|v| cat.dag.roots_of(v).iter().copied()).collect()
            };
            if let Some(mut watching) = shared.dispatcher.watch(watched) {
                drop(catalog);
                select2(&mut watching.woken, shared.dispatcher.reactor().sleep_until(until)).await;
                continue;
            }
        }
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
    // Scratch kept across passes; each pass leaves every one empty.
    let mut waiting: Vec<oneshot::Sender<()>> = Vec::new();
    let mut asked: Vec<Rc<Feed>> = Vec::new();
    let mut behind: Vec<(Rc<Feed>, u64)> = Vec::new();
    let mut reads: Vec<(Rc<Feed>, u64)> = Vec::new();
    loop {
        // Taken together, so a pass reads every feed its requests asked for
        // and positions each after they asked.
        waiting.push(asks.recv().await);
        while let Some(done) = asks.try_recv() {
            waiting.push(done);
        }
        asked.extend(feeds.live.borrow().values().filter(|feed| feed.asked.take()).cloned());
        // Each distinct cursor that is behind, of the feeds still readable.
        let mut first_lock = Some(shared.catalog_rwlock.read().await);
        for feed in asked.drain(..) {
            if let Err(fault) = feed_readable(&shared, &feed) {
                feeds.end(&feed, &fault);
                continue;
            }
            let first = behind.len();
            let last = disp.last_delta_round(feed.key.view.tid);
            for m in feed.members.borrow().iter().filter(|m| !m.caught_up(last)) {
                let cursor = m.after_tick.get().expect("a member is one its feed has not ended");
                if behind[first..].iter().all(|&(_, read)| read != cursor) {
                    behind.push((Rc::clone(&feed), cursor));
                }
            }
        }
        for slice in behind.chunks(DELTA_CUT_READS) {
            let catalog = match first_lock.take() {
                Some(g) => g,
                None => shared.catalog_rwlock.read().await,
            };
            for (feed, cursor) in slice {
                match feed_readable(&shared, feed) {
                    Ok(_) => reads.push((Rc::clone(feed), *cursor)),
                    Err(fault) => feeds.end(feed, &fault),
                }
            }
            let mut cut = delta_cut(
                disp,
                reads
                    .iter()
                    .map(|(feed, cursor)| (feed.key.view.tid, *cursor, &*feed.key.spec, feed.key.layout)),
            )
            .await;
            drop(catalog);
            for (feed, cursor) in reads.drain(..) {
                queue_train(&shared, &feed, cursor, cut.next().await).await;
            }
        }
        behind.clear();
        drop(first_lock);
        for done in waiting.drain(..) {
            done.send(());
        }
    }
}

/// Queue the subscribers of `feed` standing at `cursor` the train of `read`:
/// the feed's rounds after `cursor`, through the round it reaches, the view's
/// last. A refused read fails those subscribers alone.
async fn queue_train(shared: &Shared, feed: &Rc<Feed>, cursor: u64, read: Result<(Train<'_>, u64), WireFault>) {
    let tid = feed.key.view.tid;
    let cap = shared.feeds.queue_bytes;
    let read = match read {
        Ok((train, round)) => copy_train(train, tid, cap).await.map(|body| (body, round)),
        Err(fault) => Err(fault),
    };
    // The subscribers this read is for: a round is past every cursor that was
    // behind it, so none is taken for a later read's.
    let mut members = feed.members.borrow_mut();
    let reading = members.iter().filter(|m| m.after_tick.get() == Some(cursor));
    match read {
        Err(fault) => fail_subscribers(tid, reading, &fault),
        // Rounds that left the subscription no row are sent as no train.
        Ok((body, round)) if body.is_empty() => {
            for m in reading {
                m.after_tick.set(Some(round));
            }
        }
        Ok((mut body, round)) => {
            terminal(tid, round, feed.tag).encode_framed(&mut body);
            // Rows from fewer workers than reserved for: the rest goes back,
            // so a queued train holds at most twice its length.
            if body.capacity() > 2 * body.len() {
                body.shrink_to_fit();
            }
            let body = Rc::new(body);
            for m in reading {
                if m.out.unsynced() + body.len() > cap {
                    fail_subscribers(tid, [m], &lagged(tid));
                    continue;
                }
                m.after_tick.set(Some(round));
                m.told.set(round);
                m.out.send(tid, m.id, Rc::clone(&body));
            }
        }
    }
    members.retain(|m| m.after_tick.get().is_some());
    drop(members);
    shared.feeds.retire(feed);
}

/// The frames of `train`, copied off the ring so no subscriber's pace holds a
/// ring slot. A train past `cap` is not read to its end.
async fn copy_train(train: Train<'_>, tid: u64, cap: usize) -> Result<Vec<u8>, WireFault> {
    let mut body = Vec::new();
    while let Some(frame) = train.next().await? {
        if body.len() > cap {
            return Err(lagged(tid));
        }
        let bytes = frame.slot.frame_bytes();
        if body.is_empty() {
            // Room for one frame this size from each worker and the terminal,
            // capped near where the lagged check stops reading.
            let workers = train.workers().len();
            body.reserve((bytes.len() * workers).min(cap) + HEAD_FRAME_BYTES);
        }
        body.extend_from_slice(bytes);
    }
    Ok(body)
}

#[cfg(test)]
#[path = "tests/delta.rs"]
mod tests;
