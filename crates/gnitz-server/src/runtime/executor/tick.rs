//! Tick scheduling and read freshness: the pending-tick ledger, the one task
//! that ticks the views, the triggers that wake it, and the read lock that
//! drains it when a read would otherwise miss a commit no tick has taken yet.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::ddl::{hold_tick_for_ddl, TICK_HOLD_FOR_DDL};
use super::Shared;
use crate::runtime::reactor::AckLease;
use crate::runtime::sal::SalExcl;
use crate::runtime::sync::{chan, oneshot, select2, AsyncRwLock, Either, ReadGuard};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::RelationKind;
use gnitz_wire::WireFault;
use rustc_hash::{FxHashMap, FxHashSet};

/// `GNITZ_INJECT_TICK_EMIT_ERROR`: fail the next tick emit, once, as a full SAL
/// would.
static TICK_EMIT_ERROR: Seam = Seam::new("GNITZ_INJECT_TICK_EMIT_ERROR");

/// One tick request to [`tick_loop`], sent only through [`Ticks`]' sender.
enum TickTrigger {
    /// Fire-and-forget trigger from a commit when a tid crosses the row
    /// coalesce threshold. Tids come from [`Ticks`]' pending ledger.
    Auto,
    /// Asked for by a read or by the checkpoint: tick whatever is pending —
    /// even nothing — by `by`, and report the tick's verdict on `done`.
    Drain {
        done: oneshot::Sender<Result<(), WireFault>>,
        by: Instant,
    },
}

/// The tick loop's handle and pacing, and the pending-tick ledger: what the
/// next tick owes and what the last one covered. `pending` grows under the SAL
/// hold its commit groups are written under; a dropped relation's entry is
/// removed under the catalog write guard.
pub(super) struct Ticks {
    /// Tick trigger sender. Private to this module, so [`Ticks::drain`] and
    /// [`Shared::note_commit_rows`] are the only code that sends a trigger.
    tx: chan::Sender<TickTrigger>,
    /// Held shared by every tick, so a writer runs with no tick in flight.
    pub(super) gate: AsyncRwLock,
    /// How long after a tick the next one waits for a trigger to fall due
    /// (`GNITZ_PATIENT_TICK_GAP_MS`): a held sync is re-issued the moment it is
    /// answered, so unpaced it would tick per commit.
    patient_gap: Duration,
    /// Tables with a pending delta, each with the row count feeding the
    /// coalesce threshold.
    pending: RefCell<FxHashMap<u64, usize>>,
    /// The pending rows of one table that fire an auto-tick.
    /// `GNITZ_TICK_COALESCE_ROWS` lowers it, so tests reach it on small tables.
    coalesce_rows: usize,
    /// The SAL watermark the last emitted tick snapshotted: every commit at or
    /// below it is reflected in every view, as a read written to the SAL from
    /// now on sees it.
    last_lsn: Cell<u64>,
}

/// The triggers [`tick_loop`] receives.
pub(super) struct TickTriggers(chan::Receiver<TickTrigger>);

impl Ticks {
    /// The ticks' handle, and the triggers [`tick_loop`] is to be spawned on.
    /// `boot_seed` is the SAL watermark after the recovery tick sweep: every
    /// commit recovered at boot is reflected in every view.
    pub(super) fn new(boot_seed: u64) -> (Ticks, TickTriggers) {
        let (tx, rx) = chan::unbounded();
        let ticks = Ticks {
            tx,
            gate: AsyncRwLock::default(),
            patient_gap: Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_PATIENT_TICK_GAP_MS", 10)),
            pending: RefCell::new(FxHashMap::default()),
            coalesce_rows: gnitz_foundation::env::env_num("GNITZ_TICK_COALESCE_ROWS", 10_000),
            last_lsn: Cell::new(boot_seed),
        };
        (ticks, TickTriggers(rx))
    }

    /// True when no relation of `sources` has a commit no emitted tick has
    /// taken, so a read of a view whose source closure they are misses nothing.
    /// A commit is pending from its layout on, ahead of its ACK, so this errs
    /// towards false, which costs one extra drain.
    pub(super) fn all_ticked(&self, sources: &FxHashSet<u64>) -> bool {
        let pending = self.pending.borrow();
        sources.iter().all(|s| !pending.contains_key(s))
    }

    /// Drop a dropped relation's pending entry. Ids are never reused, so
    /// nothing else would ever reclaim it.
    pub(super) fn forget(&self, id: u64) {
        self.pending.borrow_mut().remove(&id);
    }

    /// Send the tick loop a [`TickTrigger::Drain`] due `by`; the receiver is its
    /// verdict.
    pub(super) fn drain(&self, by: Instant) -> oneshot::Receiver<Result<(), WireFault>> {
        let (done, rx) = oneshot::channel();
        self.tx.send(TickTrigger::Drain { done, by });
        rx
    }
}

impl Shared {
    /// Credit `rows` against each scanned tid's pending-tick count, and fire
    /// the auto-tick when a tid stands at or above the coalesce threshold.
    /// Below it a push only accumulates, and nothing ticks until a read or a
    /// delta poll asks for a drain. `_laid_out` is the SAL hold the rows'
    /// groups were written under.
    pub(super) fn note_commit_rows(&self, _laid_out: &SalExcl<'_>, rows: impl Iterator<Item = (u64, usize)>) {
        let ticks = &self.ticks;
        let mut pending = ticks.pending.borrow_mut();
        let dag = &self.cat().dag;
        for (tid, n) in rows.filter(|&(tid, _)| dag.is_scanned(tid)) {
            *pending.entry(tid).or_insert(0) += n;
        }
        if pending.values().any(|&rows| rows >= ticks.coalesce_rows) {
            ticks.tx.send(TickTrigger::Auto);
        }
    }

    /// Tick every table with a pending delta as one Tick group, and answer the
    /// lease its workers ACK on; `None` when nothing was pending. A refused emit
    /// leaves every delta pending.
    pub(super) async fn emit_pending_tick(&self) -> Result<Option<AckLease>, WireFault> {
        let ticks = &self.ticks;
        let sal = self.dispatcher.sal();
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
        let tids: Vec<u64> = ticks.pending.borrow().keys().copied().collect();
        let lease = self.dispatcher.emit_tick(&mut excl, &tids)?;
        ticks.pending.borrow_mut().clear();
        ticks.last_lsn.set(sal.watermark());
        Ok(Some(lease))
    }
}

/// One tick of everything pending per batch of `TickTrigger`s. A batch is
/// ticked once its first trigger is due, or [`Ticks::patient_gap`] after the
/// last tick where that is sooner.
pub(super) async fn tick_loop(shared: Rc<Shared>, TickTriggers(mut rx): TickTriggers) {
    // The batch's `Drain` repliers.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    let reactor = shared.dispatcher.reactor();
    let mut patient_from = reactor.now();
    loop {
        let mut due = patient_from;
        let mut trigger = rx.recv().await;
        loop {
            match trigger {
                TickTrigger::Auto => due = reactor.now(),
                TickTrigger::Drain { done, by } => {
                    due = due.min(by);
                    dones.push(done);
                }
            }
            trigger = match rx.try_recv() {
                Some(t) => t,
                None => match select2(rx.recv(), reactor.sleep_until(due)).await {
                    Either::A(t) => t,
                    Either::B(()) => break,
                },
            };
        }

        let _ticking = shared.ticks.gate.read().await;
        let emitted = shared.emit_pending_tick().await;
        if let Err(e) = &emitted {
            gnitz_warn!("tick error: {}", e);
        }
        // At the emit, not its ACKs: a waiting reader's scan follows the tick's
        // group in the SAL.
        let verdict = emitted.as_ref().map(|_| ()).map_err(WireFault::clone);
        for done in dones.drain(..) {
            done.send(verdict.clone());
        }
        if let Ok(Some(lease)) = emitted {
            if TICK_HOLD_FOR_DDL.take_once() {
                hold_tick_for_ddl(&shared).await;
            }
            lease.acks().await;
        }
        patient_from = reactor.now() + shared.ticks.patient_gap;
    }
}

/// The catalog read lock for a read of `targets`, behind a drain when an
/// un-ticked commit reaches one of them. The one read-lock entry of every read
/// verb.
///
/// Only a view has sources, so any other target — an absent one included — is
/// fresh. The guard is dropped across the drain, so a DDL queued on the lock
/// does not wait it out: a caller validates its targets under the guard handed
/// back.
///
/// A failed tick is reported, not swallowed: its views are stale, and serving
/// them under `WireStatus::Ok` is a silent stale read. The tick loop is serial,
/// so the drain also puts the read behind a tick already running.
pub(super) async fn fresh_read_lock(
    shared: &Rc<Shared>,
    targets: impl IntoIterator<Item = u64>,
    drain_by: Instant,
) -> Result<ReadGuard, WireFault> {
    let g = shared.catalog_rwlock.read().await;
    if shared.ticks.all_ticked(&shared.cat().dag.source_closure(targets)) {
        return Ok(g);
    }
    drop(g);
    shared.ticks.drain(drain_by).await?;
    Ok(shared.catalog_rwlock.read().await)
}

/// The LSN at or below which every commit is reflected in a read of a `kind`
/// relation. Called under the read's own SAL hold, so every zone at or below
/// the SAL watermark precedes the read's group in the log. A view reflects its
/// last tick.
pub(super) fn read_watermark(shared: &Shared, kind: RelationKind) -> u64 {
    match kind {
        RelationKind::BaseTable => shared.dispatcher.sal().watermark(),
        _ => shared.ticks.last_lsn.get(),
    }
}
