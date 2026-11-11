//! Tick scheduling and read freshness: the pending-tick ledger and the waits
//! on it, the task that ticks the views on request, the requests that wake it,
//! and the read lock that asks for a drain when a read would otherwise miss a
//! commit no tick has taken yet.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::{park_until, CatalogLock, Shared};
use crate::query::DagEngine;
use crate::runtime::master::ApplyLock;
use crate::runtime::reactor::AckWait;
use crate::runtime::sal::SalExcl;
use crate::runtime::sync::{chan, oneshot, select2, Either, ReadGuard, WriteGuard};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::RelationKind;
use gnitz_wire::WireFault;
use rustc_hash::{FxHashMap, FxHashSet};

/// `GNITZ_INJECT_TICK_EMIT_ERROR`: fail the next tick emit, once, as a full SAL
/// would.
static TICK_EMIT_ERROR: Seam = Seam::new("GNITZ_INJECT_TICK_EMIT_ERROR");

/// `GNITZ_INJECT_TICK_HOLD_FOR_DDL`: hold the first tick that emitted a group in
/// flight, the apply lock held and its ACKs outstanding, until a DDL or a
/// checkpoint parks on that lock. With no checkpoint asked for, a DDL then
/// provably lands mid-tick. One-shot.
static TICK_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_TICK_HOLD_FOR_DDL");

/// Poll bound for [`TICK_HOLD_FOR_DDL`], in 1 ms ticks.
const TICK_HOLD_MAX_POLLS: u32 = 10_000;

/// The tick loop's handle and pacing, and the pending-tick ledger with the two
/// waits on it: what the next tick owes and what the last one covered, the
/// drains waiting for the next emit, and the watches waiting for a relation's
/// first un-ticked commit. `pending` grows under the SAL hold its commit groups
/// are written under; a dropped relation's entry is removed under the catalog
/// write guard.
pub(super) struct Ticks {
    /// The requests to [`tick_loop`]: tick whatever is pending, even nothing,
    /// by the time sent. A commit that brings a tid to the row coalesce
    /// threshold asks for a tick now; a drain asks for one by its deadline.
    tx: chan::Sender<Instant>,
    /// The drains awaiting a verdict. Each sent [`tick_loop`] a request of its
    /// own as it was pushed here.
    drains: RefCell<Vec<oneshot::Sender<Result<(), WireFault>>>>,
    /// How long after a tick the next one waits for a request to fall due
    /// (`GNITZ_PATIENT_TICK_GAP_MS`): a held sync is re-issued the moment it is
    /// answered, so unpaced it would tick per commit.
    patient_gap: Duration,
    /// Tables with a pending delta, each with the row count feeding the
    /// coalesce threshold.
    pending: RefCell<FxHashMap<u64, usize>>,
    /// The waits for a relation to gain an un-ticked commit. An entry leaves
    /// when a relation it names enters `pending`, at a `wake_watches`, or with
    /// its [`Watching`].
    watches: RefCell<Vec<Watch>>,
    /// The id of the next [`Watch`].
    next_watch: Cell<u64>,
    /// Scratch for the tids a tick is emitted for, kept across ticks.
    tids: RefCell<Vec<u64>>,
    /// The pending rows of one table that fire an auto-tick.
    /// `GNITZ_TICK_COALESCE_ROWS` lowers it, so tests reach it on small tables.
    coalesce_rows: usize,
    /// The SAL watermark the last emitted tick snapshotted: every commit at or
    /// below it is reflected in every view, as a read written to the SAL from
    /// now on sees it.
    last_lsn: Cell<u64>,
}

/// The requests [`tick_loop`] receives.
pub(super) struct TickRequests(chan::Receiver<Instant>);

struct Watch {
    id: u64,
    relations: FxHashSet<u64>,
    wake: oneshot::Sender<()>,
}

/// One wait registered by [`Ticks::watch`], given up on drop.
pub(super) struct Watching<'a> {
    ticks: &'a Ticks,
    id: u64,
    /// Resolves when a watched relation gains an un-ticked commit, or at a
    /// [`Ticks::wake_watches`].
    pub(super) woken: oneshot::Receiver<()>,
}

impl Drop for Watching<'_> {
    fn drop(&mut self) {
        self.ticks.watches.borrow_mut().retain(|w| w.id != self.id);
    }
}

impl Ticks {
    /// The ticks' handle, and the requests [`tick_loop`] is to be spawned on.
    /// `boot_seed` is the SAL watermark after the recovery tick sweep: every
    /// commit recovered at boot is reflected in every view.
    pub(super) fn new(boot_seed: u64) -> (Ticks, TickRequests) {
        let (tx, rx) = chan::unbounded();
        let ticks = Ticks {
            tx,
            drains: RefCell::new(Vec::new()),
            patient_gap: Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_PATIENT_TICK_GAP_MS", 10)),
            pending: RefCell::new(FxHashMap::default()),
            watches: RefCell::new(Vec::new()),
            next_watch: Cell::new(0),
            tids: RefCell::new(Vec::new()),
            coalesce_rows: gnitz_foundation::env::env_num("GNITZ_TICK_COALESCE_ROWS", 10_000),
            last_lsn: Cell::new(boot_seed),
        };
        (ticks, TickRequests(rx))
    }

    /// True when no relation in the source closure of any of `targets` has a
    /// commit no emitted tick has taken, so a read of them misses nothing.
    /// A commit is pending from its layout on, ahead of its ACK, so this errs
    /// towards false, which costs one extra drain.
    pub(super) fn all_ticked(&self, dag: &DagEngine, targets: impl IntoIterator<Item = u64>) -> bool {
        self.none_pending(
            targets
                .into_iter()
                .flat_map(|t| dag.source_closure_of(t).iter().copied()),
        )
    }

    /// True when none of `relations` has a commit no emitted tick has taken.
    fn none_pending(&self, relations: impl IntoIterator<Item = u64>) -> bool {
        let pending = self.pending.borrow();
        pending.is_empty() || relations.into_iter().all(|s| !pending.contains_key(&s))
    }

    /// Wait for one of `relations` to gain a commit no emitted tick has taken;
    /// `None` when one already has. The check and the registration share no
    /// await, so no commit falls between them.
    pub(super) fn watch(&self, relations: FxHashSet<u64>) -> Option<Watching<'_>> {
        if !self.none_pending(relations.iter().copied()) {
            return None;
        }
        let id = self.next_watch.get();
        self.next_watch.set(id + 1);
        let (wake, woken) = oneshot::channel();
        self.watches.borrow_mut().push(Watch { id, relations, wake });
        Some(Watching { ticks: self, id, woken })
    }

    /// Wake every watch naming `relation`, which just entered `pending`.
    fn wake(&self, relation: u64) {
        let mut watches = self.watches.borrow_mut();
        for watch in watches.extract_if(.., |w| w.relations.contains(&relation)) {
            watch.wake.send(());
        }
    }

    /// Wake every watch. A watch names relations read off a catalog its holder
    /// has released, and a DDL's own tick passes through no `pending` entry.
    pub(super) fn wake_watches(&self) {
        for watch in self.watches.take() {
            watch.wake.send(());
        }
    }

    /// Drop a dropped relation's pending entry.
    pub(super) fn forget(&self, id: u64) {
        self.pending.borrow_mut().remove(&id);
    }

    /// The SAL watermark the last emitted tick snapshotted.
    pub(super) fn last_lsn(&self) -> u64 {
        self.last_lsn.get()
    }

    /// Wait for the next emit of everything pending, whichever task runs it;
    /// a refusal of that emit is the `Err`. The tick loop is asked for one by
    /// `by`. A request whose drain another task's emit answered stays queued:
    /// the loop then ticks what was committed since.
    async fn drain(&self, by: Instant) -> Result<(), WireFault> {
        let (done, verdict) = oneshot::channel();
        // No await between the two: the request is what guarantees the sender
        // an answer.
        self.drains.borrow_mut().push(done);
        self.tx.send(by);
        verdict.await
    }

    /// Answer every drain awaiting a verdict.
    fn answer_drains(&self, verdict: Result<(), WireFault>) {
        for done in self.drains.borrow_mut().drain(..) {
            done.send(verdict.clone());
        }
    }
}

impl Shared {
    /// Credit `rows` against each scanned tid's pending-tick count, and fire
    /// the auto-tick when a tid stands at or above the coalesce threshold.
    /// Below it a push only accumulates, and is taken by the next emit,
    /// whichever task runs it. A tid's first commit since the emit that last
    /// took it wakes the watches naming it. `_laid_out` is the SAL hold the
    /// rows' groups were written under.
    pub(super) fn note_commit_rows(&self, _laid_out: &SalExcl<'_>, rows: impl Iterator<Item = (u64, usize)>) {
        let ticks = &self.ticks;
        let mut pending = ticks.pending.borrow_mut();
        let dag = &self.cat().dag;
        for (tid, n) in rows.filter(|&(tid, _)| dag.is_scanned(tid)) {
            *pending.entry(tid).or_insert_with(|| {
                ticks.wake(tid);
                0
            }) += n;
        }
        if pending.values().any(|&rows| rows >= ticks.coalesce_rows) {
            ticks.tx.send(self.dispatcher.reactor().now());
        }
    }

    /// Tick every table with a pending delta as one Tick group, and answer the
    /// wait for its workers' ACKs; `None` when nothing was pending. A refused emit
    /// leaves every delta pending. `alone` is the caller's hold of the apply
    /// lock, which it keeps through those ACKs.
    ///
    /// Every drain waiting is answered with the emit's verdict, at the emit and
    /// not its ACKs: its reader's scan follows the Tick group in the SAL. The
    /// answer shares no await with the read of `pending`, so every commit
    /// pending when the drain was asked for is in the group.
    pub(super) async fn emit_pending_tick(&self, alone: &WriteGuard<ApplyLock>) -> Result<Option<AckWait>, WireFault> {
        let ticks = &self.ticks;
        let sal = self.dispatcher.sal();
        let emitted = async {
            if ticks.pending.borrow().is_empty() {
                ticks.last_lsn.set(sal.watermark());
                return Ok(None);
            }
            // The hold `note_commit_rows` is called under, so the tids and the
            // watermark read here are of the same commits.
            let mut excl = sal.lock().await;
            if TICK_EMIT_ERROR.take_once() {
                return Err("injected tick emit error".to_string().into());
            }
            let wait = {
                let mut tids = ticks.tids.borrow_mut();
                tids.clear();
                tids.extend(ticks.pending.borrow().keys().copied());
                self.dispatcher.emit_tick(alone, &mut excl, &tids)?
            };
            ticks.pending.borrow_mut().clear();
            ticks.last_lsn.set(sal.watermark());
            Ok(Some(wait))
        }
        .await;
        ticks.answer_drains(emitted.as_ref().map(|_| ()).map_err(WireFault::clone));
        emitted
    }
}

/// One tick of everything pending per batch of requests. A batch is ticked
/// once its first request is due, or [`Ticks::patient_gap`] after the loop's
/// last tick where that is sooner.
pub(super) async fn tick_loop(shared: Rc<Shared>, TickRequests(mut rx): TickRequests) {
    let reactor = shared.dispatcher.reactor();
    let mut patient_from = reactor.now();
    loop {
        let mut due = patient_from;
        let mut request = rx.recv().await;
        loop {
            due = due.min(request);
            request = match rx.try_recv() {
                Some(t) => t,
                None => match select2(rx.recv(), reactor.sleep_until(due)).await {
                    Either::A(t) => t,
                    Either::B(()) => break,
                },
            };
        }

        let ticking = shared.dispatcher.apply_lock().await;
        match shared.emit_pending_tick(&ticking).await {
            Err(e) => gnitz_warn!("tick error: {}", e),
            Ok(Some(wait)) => {
                if TICK_HOLD_FOR_DDL.take_once() {
                    park_until(&shared, TICK_HOLD_MAX_POLLS, "tick hold", || ticking.contended()).await;
                }
                wait.acks().await;
            }
            Ok(None) => {}
        }
        patient_from = reactor.now() + shared.ticks.patient_gap;
    }
}

/// The catalog read lock for a read of `targets`, behind a drain when an
/// un-ticked commit reaches one of them: the lock of a read that may reach a
/// view's rows (SCAN, DELTA_POLL, a held sync). A read of catalog shape or of
/// pinned base rows takes the plain guard.
///
/// Only a view has sources, so any other target — an absent one included — is
/// fresh. Freshness is judged when the read arrives, off the lock: every commit
/// acknowledged before the request is pending or ticked by then, and one laid
/// out while the read waits for the lock was acknowledged after it. The guard
/// is taken after the drain, so a DDL never waits a drain out: a caller
/// validates its targets under the guard handed back.
///
/// A failed tick is reported, not swallowed: its views are stale, and serving
/// them under `WireStatus::Ok` is a silent stale read. Every emit holds the
/// apply lock through its ACKs, so the drain also puts the read behind a tick
/// already running.
pub(super) async fn fresh_read_lock(
    shared: &Shared,
    targets: impl IntoIterator<Item = u64>,
    drain_by: Instant,
) -> Result<ReadGuard<CatalogLock>, WireFault> {
    if !shared.ticks.all_ticked(&shared.cat().dag, targets) {
        shared.ticks.drain(drain_by).await?;
    }
    Ok(shared.catalog_rwlock.read().await)
}

/// The LSN at or below which every commit is reflected in a worker read of a
/// `kind` relation whose group is written under `_cut`: every zone at or below
/// the SAL watermark precedes that group in the log. A view reflects its last
/// tick.
pub(super) fn read_watermark(shared: &Shared, kind: RelationKind, _cut: &SalExcl<'_>) -> u64 {
    match kind {
        RelationKind::BaseTable => shared.dispatcher.sal().watermark(),
        RelationKind::View(_) => shared.ticks.last_lsn(),
        RelationKind::SystemCatalog | RelationKind::Stream => {
            unreachable!("a system family is read off the master, and a stream is never read")
        }
    }
}

#[cfg(test)]
#[path = "tests/tick.rs"]
mod tests;
