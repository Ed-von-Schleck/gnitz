//! The delta feed's two readers: DELTA_POLL, which reads a view's rounds from
//! the cursor each request names, and the pushed feeds, where the server
//! re-issues that read itself for the items a poll asked it to keep.
//!
//! A cursor is a round of its own view, so the subscribers standing at one
//! share one read. The copy's cursor stays the client's: a subscription can
//! end at any point, and its client continues from that cursor by polling.
//!
//! A child of `executor`, reading that module's private items with no
//! visibility widened.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use super::{
    all_ticked, encode_response_into, finish_scan_fanout, fresh_read_lock, send_ack, target_kind, terminal_scan_msg,
    Access, Shared,
};
use crate::runtime::master::forward_scan;
use crate::runtime::peer::{Outbox, Peer};
use crate::runtime::reactor::{oneshot, select2, Either, ReadGuard, TrainLease};
use crate::runtime::wire as ipc;
use gnitz_wire::control::{ControlHeader, Target};
use gnitz_wire::txn_frame::{decode_delta_items, delta_poll_kept, DeltaPollItem};
use gnitz_wire::{WireFault, WireStatus};

/// Views one DELTA_POLL reads at one SAL cut: the ceiling on the leases and
/// reply trains a poll puts on the master at a time. A poll naming more is
/// answered a slice at a time.
const DELTA_POLL_CUT_VIEWS: usize = 64;

/// The longest a SYNC_PUSHED is held, whatever wait it asks for.
const SYNC_MAX_WAIT: Duration = Duration::from_secs(3600);

// ---------------------------------------------------------------------------
// DELTA_POLL
// ---------------------------------------------------------------------------

/// What one view of a poll is answered with.
enum PollPosition {
    /// The view cannot be read at all.
    Fault(WireFault),
    /// The cursor already stands at this round, the view's last, so its
    /// terminal is master-local: a fan-out per quiet read would cost a wakeup
    /// per worker.
    UpToDate(u64),
    /// The view moved, or is read whole, and takes the next dispatch of the
    /// poll's cut.
    Moved,
}

/// Where a poll of `item` stands.
fn poll_position(shared: &Shared, _catalog: &ReadGuard, item: DeltaPollItem) -> PollPosition {
    let tid = item.view.tid;
    let kind = match target_kind(shared, item.view, Access::UserRead) {
        Ok(kind) => kind,
        Err(fault) => return PollPosition::Fault(fault),
    };
    if !kind.has_delta_feed() {
        return PollPosition::Fault(WireFault::from(format!(
            "delta_read: relation {tid} carries no delta feed; \
             create the view WITH (delta = '<size>') to subscribe to it"
        )));
    }
    let disp = shared.disp();
    match item.from {
        Some(cursor) if cursor.tag != disp.delta_cursor_tag(tid, item.spec) => PollPosition::Fault(WireFault {
            status: WireStatus::DeltaExpired,
            text: format!("delta cursor of relation {tid} names another boot, relation or spec; re-read at 0"),
        }),
        Some(cursor) if cursor.tick.get() >= disp.last_delta_round(tid) => {
            PollPosition::UpToDate(disp.last_delta_round(tid))
        }
        _ => PollPosition::Moved,
    }
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
    let disp = shared.disp();
    let mut kept = delta_poll_kept(prologue);
    // The poll drained once, for this lock; a later slice takes the lock alone.
    let mut first_lock = Some(fresh_read_lock(shared, views.iter().map(|v| v.view.tid), false).await?);

    // One slice at a time: one catalog lock and — for however many of its views
    // moved — one broadcast. A view read whole is a slice of its own: a cut holds
    // the workers until its last train is taken, and that train is a view long.
    let slices = views
        .chunk_by(|a, b| a.from.is_some() && b.from.is_some())
        .flat_map(|run| run.chunks(DELTA_POLL_CUT_VIEWS));
    for slice in slices {
        // ── Phase 1: classify under the catalog lock, dispatch one cut ─────
        let catalog = match first_lock.take() {
            Some(g) => g,
            None => shared.catalog_rwlock.read().await,
        };
        let positions: Vec<PollPosition> = slice.iter().map(|&v| poll_position(shared, &catalog, v)).collect();
        let moved = || {
            let judged = slice.iter().zip(&positions);
            judged.filter_map(|(v, p)| matches!(p, PollPosition::Moved).then_some(v))
        };
        // The round each moved view's read reaches.
        let mut reached = Vec::new();
        let dispatches = if moved().next().is_none() {
            Vec::new()
        } else {
            disp.scan_cut(|cut| {
                for item in moved() {
                    let after = item.from.map_or(0, |cursor| cursor.tick.get());
                    reached.push(cut.delta(item.view.tid, after, item.spec, item.reply_layout)?);
                }
                Ok(())
            })
            .await?
        };
        // Phase 2 reads no catalog state, and holding the lock across the
        // forward would block DDL.
        drop(catalog);

        // ── Phase 2: one terminal per view, in request order ───────────────
        // Taken in step with the `Moved`s that were pushed. A dispatch left
        // undrained — an earlier return dropped it — discards the rest of its
        // train at the ring boundary.
        let mut dispatches = dispatches.into_iter().zip(reached);
        for (item, position) in slice.iter().zip(positions) {
            let result = match position {
                PollPosition::Fault(fault) => Err(fault),
                PollPosition::UpToDate(round) => Ok(round),
                PollPosition::Moved => {
                    let (lease, round) = dispatches.next().expect("one dispatch per moved view");
                    forward_scan(peer, &lease).await.map(|()| round)
                }
            };
            if let Some(id) = kept.as_mut().and_then(Iterator::next) {
                subs.leave(&shared.feeds, Some(id));
                if let Ok(round) = result {
                    subs.join(&shared.feeds, id, item, round, peer.outbox());
                }
            }
            let tag = disp.delta_cursor_tag(item.view.tid, item.spec);
            finish_scan_fanout(peer, item.view.tid, tag, result);
            // Carry no more than the budget into the next view, and learn here
            // rather than at the end if the client is gone.
            if peer.flush_if_full().await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// The catalog read lock a sync of the views `ids` is answered under, every
/// commit reaching one of them ticked. A `quiet` request is first held until a
/// relation one of them reads changes, `wait` passes, or the client sends its
/// next request or goes; `true` while it may be held again.
async fn hold_lock(
    shared: &Rc<Shared>,
    peer: &Peer,
    ids: &[u64],
    wait: Duration,
    quiet: impl FnOnce() -> bool,
) -> Result<(ReadGuard, bool), WireFault> {
    let ids = || ids.iter().copied();
    let waiting = !wait.is_zero();
    let g = fresh_read_lock(shared, ids(), waiting).await?;
    if !waiting {
        return Ok((g, false));
    }
    // No await from the test to the park, so no commit is acknowledged between
    // them. The drain above was one: a commit it did not take is un-ticked here.
    let mut watched = shared.cat().dag.source_closure(ids());
    if !(all_ticked(shared, &watched) && quiet()) {
        return Ok((g, true));
    }
    // A commit or a drop reaches a view through the view or anything it reads.
    watched.extend(ids());
    let mut parked = shared.sync_waiters.park(watched);
    drop(g);
    let released = select2(shared.disp().reactor().sleep(wait), peer.next_request_ready());
    let woken = matches!(select2(&mut parked.woken, released).await, Either::A(_));
    drop(parked);
    Ok((fresh_read_lock(shared, ids(), true).await?, woken))
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
    id: u64,
    /// The round this subscriber has been queued every row through; `None`
    /// once its feed ended it.
    after_tick: Cell<Option<u64>>,
    /// The round its client's cursor was last sent: the one it joined at, or
    /// that of its last train.
    told: Cell<u64>,
    out: Rc<Outbox>,
}

impl Subscriber {
    /// Whether no round since this subscriber's cursor reached the view `tid`
    /// it is fed.
    fn caught_up(&self, shared: &Shared, tid: u64) -> bool {
        let last = shared.disp().last_delta_round(tid);
        self.after_tick.get().is_some_and(|after| after >= last)
    }
}

/// One [`FeedKey`]'s subscribers.
struct Feed {
    key: FeedKey,
    members: RefCell<Vec<Rc<Subscriber>>>,
    /// A request waits for the pump to read it. The pump reads only such a
    /// feed: a round nobody asks for is read with the rounds after it, as a
    /// poll would read them.
    asked: Cell<bool>,
}

/// The feeds with a subscriber, and the one task that reads for them.
#[derive(Default)]
pub(super) struct Feeds {
    live: RefCell<FxHashMap<FeedKey, Rc<Feed>>>,
    /// The requests waiting for the pump's next pass.
    syncing: RefCell<Vec<oneshot::Sender<()>>>,
    /// Whether the pump runs: it is spawned by the first request to wait, and
    /// ends once none does.
    pumping: Cell<bool>,
}

impl Feeds {
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

/// The subscriptions one connection holds, each with its feed. An entry
/// outlives a subscription its feed ended, until the connection's next
/// SYNC_PUSHED.
#[derive(Default)]
pub(super) struct Subscriptions(Vec<(Rc<Subscriber>, Rc<Feed>)>);

impl Subscriptions {
    /// Keep `item` as the subscription `id`, queued through `round` on `out`.
    fn join(&mut self, feeds: &Feeds, id: u64, item: &DeltaPollItem, round: u64, out: Rc<Outbox>) {
        let joined = Rc::new(Subscriber {
            id,
            after_tick: Cell::new(Some(round)),
            told: Cell::new(round),
            out,
        });
        let key = FeedKey {
            view: item.view,
            layout: item.reply_layout,
            spec: item.spec.to_vec(),
        };
        let feed = Rc::clone(feeds.live.borrow_mut().entry(key.clone()).or_insert_with(|| {
            Rc::new(Feed {
                key,
                members: RefCell::default(),
                asked: Cell::new(false),
            })
        }));
        feed.members.borrow_mut().push(Rc::clone(&joined));
        self.0.push((joined, feed));
    }

    /// Send each subscription queued through rounds that left it no row where
    /// it stands, as a train of its terminal alone: its client's cursor passes
    /// those rounds too.
    fn tell(&self, shared: &Shared, peer: &Peer) {
        debug_assert!(peer.outbox().is_empty(), "a position is sent behind every train");
        for (sub, feed) in &self.0 {
            let Some(after) = sub.after_tick.get() else { continue };
            if sub.told.replace(after) != after {
                let tid = feed.key.view.tid;
                let tag = shared.disp().delta_cursor_tag(tid, &feed.key.spec);
                peer.cork_with(|out| {
                    out.extend_from_slice(&pushed_marker(tid, sub.id));
                    encode_response_into(out, terminal_scan_msg(tid, after, tag));
                });
            }
        }
    }

    /// Leave `id`'s feed; `None` for every one.
    pub(super) fn leave(&mut self, feeds: &Feeds, id: Option<u64>) {
        for (sub, feed) in self.0.extract_if(.., |(sub, _)| id.is_none_or(|id| id == sub.id)) {
            feed.members.borrow_mut().retain(|m| !Rc::ptr_eq(m, &sub));
            feeds.retire(&feed);
        }
    }
}

/// The frame that opens a pushed train of subscription `id`.
fn pushed_marker(view: u64, id: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let flags = gnitz_wire::WireFlags {
        pushed: true,
        continuation: true,
        ..Default::default()
    };
    encode_response_into(
        &mut out,
        ipc::WireMsg {
            target_id: view,
            arg0: id,
            flags,
            ..Default::default()
        },
    );
    out
}

/// End `members`' subscriptions with `fault`. A fault frame is queued whatever
/// the queue holds: it is what tells the subscriber to stop waiting.
fn fail_subscribers<'a>(view: u64, members: impl IntoIterator<Item = &'a Rc<Subscriber>>, fault: &WireFault) {
    let mut frame = Vec::new();
    encode_response_into(
        &mut frame,
        ipc::WireMsg {
            target_id: view,
            ..ipc::WireMsg::fault(fault)
        },
    );
    let frame = Rc::new(frame);
    for m in members {
        m.after_tick.set(None);
        m.out.send(pushed_marker(view, m.id), Rc::clone(&frame));
    }
}

fn lagged(view: u64) -> WireFault {
    format!("subscription to relation {view} fell behind what one connection is queued; poll from its cursor").into()
}

/// SYNC_PUSHED: bring every subscription of the connection up to the pushes
/// acknowledged before this request, and hold the reply up to `wait_ms` while
/// none of them has been sent anything since the last one.
pub(super) async fn handle_sync_pushed(
    shared: &Rc<Shared>,
    peer: &Peer,
    subs: &mut Subscriptions,
    wait_ms: u64,
) -> Result<(), WireFault> {
    let deadline = Instant::now() + Duration::from_millis(wait_ms).min(SYNC_MAX_WAIT);
    let out = peer.outbox();
    let feeds = &shared.feeds;
    // Whether a subscription has been queued every round its view was reached
    // by. Asked under the catalog lock.
    let caught_up = |(sub, feed): &(Rc<Subscriber>, Rc<Feed>)| {
        let view = feed.key.view;
        target_kind(shared, view, Access::UserRead).is_ok() && sub.caught_up(shared, view.tid)
    };
    loop {
        // A subscription its feed ended is gone here too.
        subs.0.retain(|(sub, _)| sub.after_tick.get().is_some());
        let ids: Vec<u64> = subs.0.iter().map(|(_, feed)| feed.key.view.tid).collect();
        // With no subscription there is nothing to hold for.
        let left = match ids.is_empty() {
            true => Duration::ZERO,
            false => deadline.saturating_duration_since(Instant::now()),
        };
        let quiet = || out.unsynced() == 0 && subs.0.iter().all(caught_up);
        let (catalog, may_hold) = hold_lock(shared, peer, &ids, left, quiet).await?;
        // Every commit acknowledged before this request is ticked here, so a
        // subscription that is caught up waits for no pump: the steady state
        // of a poll. One its feed ended while the lock was awaited has its
        // fault train queued.
        let mut asked = false;
        for joined in subs.0.iter().filter(|(sub, _)| sub.after_tick.get().is_some()) {
            if !caught_up(joined) {
                joined.1.asked.set(true);
                asked = true;
            }
        }
        if asked {
            let (done, pumped) = oneshot::channel();
            feeds.syncing.borrow_mut().push(done);
            if !feeds.pumping.replace(true) {
                let reactor = shared.disp().reactor().clone();
                reactor.spawn(pump(Rc::clone(shared)));
            }
            drop(catalog);
            pumped.await;
        } else {
            drop(catalog);
        }
        if peer.ship_pushed().await.is_err() {
            return Ok(());
        }
        if out.unsynced() > 0 || !may_hold {
            break;
        }
    }
    subs.tell(shared, peer);
    send_ack(peer, 0, 0);
    out.synced();
    Ok(())
}

/// Read the feeds requests ask for, for as long as one waits. A pass answers
/// the requests waiting when it starts: it reads each feed they asked for at
/// one SAL cut, one read per distinct cursor behind among the feed's
/// subscribers — in the steady state one read, however many they are — and
/// queues each subscriber its train.
async fn pump(shared: Rc<Shared>) {
    let feeds = &shared.feeds;
    let disp = shared.disp();
    loop {
        // Taken together, so a pass reads every feed its requests asked for
        // and positions each after they asked.
        let waiting = feeds.syncing.take();
        if waiting.is_empty() {
            feeds.pumping.set(false);
            return;
        }
        let asked: Vec<Rc<Feed>> = {
            let live = feeds.live.borrow();
            live.values().filter(|feed| feed.asked.take()).cloned().collect()
        };
        for slice in asked.chunks(DELTA_POLL_CUT_VIEWS) {
            let catalog = shared.catalog_rwlock.read().await;
            // Each distinct cursor that is behind. No await from here to the
            // cut, so no round lands between a position and what is made of it.
            let mut behind: Vec<(&Rc<Feed>, u64)> = Vec::new();
            for feed in slice {
                let view = feed.key.view;
                if let Err(fault) = target_kind(&shared, view, Access::UserRead) {
                    feeds.end(feed, &fault);
                    continue;
                }
                let first = behind.len();
                for m in feed.members.borrow().iter().filter(|m| !m.caught_up(&shared, view.tid)) {
                    let cursor = m.after_tick.get().expect("a member is one its feed has not ended");
                    if behind[first..].iter().all(|&(_, read)| read != cursor) {
                        behind.push((feed, cursor));
                    }
                }
            }
            if behind.is_empty() {
                continue;
            }
            // The round each read reaches.
            let mut reached = Vec::with_capacity(behind.len());
            let leases = disp
                .scan_cut(|cut| {
                    for &(feed, cursor) in &behind {
                        let FeedKey { view, layout, ref spec } = feed.key;
                        reached.push(cut.delta(view.tid, cursor, spec, layout)?);
                    }
                    Ok(())
                })
                .await;
            drop(catalog);
            match leases {
                Ok(leases) => {
                    for (((feed, cursor), round), lease) in behind.into_iter().zip(reached).zip(leases) {
                        queue_train(&shared, feed, cursor, round, lease).await;
                    }
                }
                Err(fault) => behind.iter().for_each(|(feed, _)| feeds.end(feed, &fault)),
            }
        }
        for done in waiting {
            done.send(());
        }
    }
}

/// Queue the subscribers of `feed` standing at `cursor` the train `lease`
/// reads: the feed's rounds after `cursor`, through `round`, the view's last.
async fn queue_train(shared: &Shared, feed: &Rc<Feed>, cursor: u64, round: u64, lease: TrainLease) {
    let tid = feed.key.view.tid;
    let cap = shared.push_queue_bytes;
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
            let tag = shared.disp().delta_cursor_tag(tid, &feed.key.spec);
            encode_response_into(&mut body, terminal_scan_msg(tid, round, tag));
            let body = Rc::new(body);
            for m in reading {
                if rows && m.out.unsynced() + body.len() > cap {
                    fail_subscribers(tid, [m], &lagged(tid));
                    continue;
                }
                m.after_tick.set(Some(round));
                if rows {
                    m.told.set(round);
                    m.out.send(pushed_marker(tid, m.id), Rc::clone(&body));
                }
            }
        }
    }
    members.retain(|m| m.after_tick.get().is_some());
    drop(members);
    shared.feeds.retire(feed);
}
