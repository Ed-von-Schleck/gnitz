//! Tick scheduling and read freshness: the one task that ticks the views, the
//! triggers that wake it, and the read lock that drains it when a read would
//! otherwise miss a commit no tick has taken yet. The pending-tick ledger is
//! the dispatcher's.

use std::rc::Rc;
use std::time::{Duration, Instant};

use super::ddl::{hold_tick_for_ddl, TICK_HOLD_FOR_DDL};
use super::Shared;
use crate::runtime::reactor::{chan, oneshot, select2, AsyncRwLock, Either, ReadGuard};
use crate::runtime::sal::SalExcl;
use gnitz_store::relation::RelationKind;
use gnitz_wire::WireFault;

/// One tick request to [`tick_loop`], sent only through [`Ticks`]' sender.
enum TickTrigger {
    /// Fire-and-forget trigger from INSERT when a tid crosses the row
    /// coalesce threshold.  Tids come from the dispatcher's pending ledger.
    Auto,
    /// Asked for by a read or by the checkpoint: tick whatever is pending —
    /// even nothing — by `by`, and report the tick's verdict on `done`.
    Drain {
        done: oneshot::Sender<Result<(), WireFault>>,
        by: Instant,
    },
}

/// The tick loop's handle and pacing.
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
}

/// The triggers [`tick_loop`] receives.
pub(super) struct TickTriggers(chan::Receiver<TickTrigger>);

impl Ticks {
    /// The ticks' handle, and the triggers [`tick_loop`] is to be spawned on.
    pub(super) fn new() -> (Ticks, TickTriggers) {
        let (tx, rx) = chan::unbounded();
        let ticks = Ticks {
            tx,
            gate: AsyncRwLock::default(),
            patient_gap: Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_PATIENT_TICK_GAP_MS", 10)),
        };
        (ticks, TickTriggers(rx))
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
    /// Credit `rows` to the dispatcher's pending ticks, and fire the auto-tick
    /// when a tid crosses the coalesce threshold. `laid_out` is the SAL hold the
    /// rows' groups were written under.
    pub(super) fn note_commit_rows(&self, laid_out: &SalExcl<'_>, rows: impl Iterator<Item = (u64, usize)>) {
        if self.dispatcher.note_pending(laid_out, rows) {
            self.ticks.tx.send(TickTrigger::Auto);
        }
    }
}

/// One tick of everything pending per batch of `TickTrigger`s. A batch is
/// ticked once its first trigger is due, or [`Ticks::patient_gap`] after the
/// last tick where that is sooner.
pub(super) async fn tick_loop(shared: Rc<Shared>, TickTriggers(mut rx): TickTriggers) {
    // The batch's `Drain` repliers.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    let mut patient_from = Instant::now();
    loop {
        let mut due = patient_from;
        let mut trigger = rx.recv().await;
        loop {
            match trigger {
                TickTrigger::Auto => due = Instant::now(),
                TickTrigger::Drain { done, by } => {
                    due = due.min(by);
                    dones.push(done);
                }
            }
            trigger = match rx.try_recv() {
                Some(t) => t,
                None => {
                    let hold = due.saturating_duration_since(Instant::now());
                    if hold.is_zero() {
                        break;
                    }
                    match select2(rx.recv(), shared.dispatcher.reactor().sleep(hold)).await {
                        Either::A(t) => t,
                        Either::B(()) => break,
                    }
                }
            };
        }

        let _ticking = shared.ticks.gate.read().await;
        let emitted = shared.dispatcher.emit_pending_tick().await;
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
        patient_from = Instant::now() + shared.ticks.patient_gap;
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
    if shared.dispatcher.all_ticked(&shared.cat().dag.source_closure(targets)) {
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
        _ => shared.dispatcher.last_tick_lsn(),
    }
}
