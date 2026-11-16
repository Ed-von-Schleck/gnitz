//! Tick scheduling and read freshness: the task that ticks the views on
//! request, the requests that wake it,
//! and the read lock that asks for a drain when a read would otherwise miss a
//! commit no tick has taken yet.

use std::rc::Rc;
use std::time::{Duration, Instant};

use super::{CatalogLock, Shared, park_until};
use crate::runtime::sal::SalExcl;
use crate::runtime::sync::{Either, ReadGuard, chan, select2};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::RelationKind;
use gnitz_wire::WireFault;

/// `GNITZ_INJECT_TICK_HOLD_FOR_DDL`: hold the first tick that emitted a group in
/// flight, the apply lock held and its ACKs outstanding, until a DDL or a
/// checkpoint parks on that lock. With no checkpoint asked for, a DDL then
/// provably lands mid-tick. One-shot.
static TICK_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_TICK_HOLD_FOR_DDL");

/// Poll bound for [`TICK_HOLD_FOR_DDL`], in 1 ms ticks.
const TICK_HOLD_MAX_POLLS: u32 = 10_000;

/// The tick loop's handle and pacing.
pub(super) struct Ticks {
    /// The requests to [`tick_loop`]: tick whatever is pending, even nothing,
    /// by the time sent. A commit that brings a tid to the row coalesce
    /// threshold asks for a tick now; a drain asks for one by its deadline.
    tx: chan::Sender<Instant>,
    /// How long after a tick the next one waits for a request to fall due
    /// (`GNITZ_PATIENT_TICK_GAP_MS`): a held sync is re-issued the moment it is
    /// answered, so unpaced it would tick per commit.
    patient_gap: Duration,
    /// The pending rows of one table that fire an auto-tick.
    /// `GNITZ_TICK_COALESCE_ROWS` lowers it, so tests reach it on small tables.
    coalesce_rows: usize,
}

/// The requests [`tick_loop`] receives.
pub(super) struct TickRequests(chan::Receiver<Instant>);

impl Ticks {
    /// The ticks' handle, and the requests [`tick_loop`] is to be spawned on.
    pub(super) fn new() -> (Ticks, TickRequests) {
        let (tx, rx) = chan::unbounded();
        let ticks = Ticks {
            tx,
            patient_gap: Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_PATIENT_TICK_GAP_MS", 10)),
            coalesce_rows: gnitz_foundation::env::env_num("GNITZ_TICK_COALESCE_ROWS", 10_000),
        };
        (ticks, TickRequests(rx))
    }
}

impl Shared {
    /// Credit `rows` against each tid's pending-tick count, and fire the
    /// auto-tick when a tid stands at or above the coalesce threshold. Below
    /// it a push only accumulates, and is taken by the next emit, whichever
    /// task runs it. `laid_out` is the SAL hold the rows' groups were written
    /// under.
    pub(super) fn note_commit_rows(&self, laid_out: &SalExcl<'_>, rows: impl Iterator<Item = (u64, usize)>) {
        for (tid, n) in rows {
            self.dispatcher.credit(laid_out, tid, n);
        }
        if self.dispatcher.pending_reaches(self.ticks.coalesce_rows) {
            self.ticks.tx.send(self.dispatcher.reactor().now());
        }
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
        match shared.dispatcher.emit_tick(&ticking).await {
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
    if !shared.dispatcher.all_ticked(targets) {
        // Wait for the next emit of everything pending. A request whose wait
        // another task's emit answered stays queued: the loop then ticks what
        // was committed since. No await between the two: the request is what
        // guarantees the wait an answer.
        let verdict = shared.dispatcher.next_emit();
        shared.ticks.tx.send(drain_by);
        verdict.await?;
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
        RelationKind::View(_) => shared.dispatcher.last_lsn(),
        RelationKind::SystemCatalog | RelationKind::Stream => {
            unreachable!("a system family is read off the master, and a stream is never read")
        }
    }
}

#[cfg(test)]
#[path = "tests/tick.rs"]
mod tests;
