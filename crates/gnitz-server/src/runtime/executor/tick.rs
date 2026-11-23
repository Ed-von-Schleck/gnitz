//! Tick scheduling and read freshness: the task that ticks the views on
//! request, and the read lock that asks for a drain when a read would
//! otherwise miss a commit no tick has taken yet.

use gnitz_foundation::gnitz_warn;
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::{CatalogLock, Shared, park_until};
use crate::runtime::sync::{Either, ReadGuard, select2};
use gnitz_foundation::fault::Switch;
use gnitz_wire::WireFault;

/// `GNITZ_INJECT_TICK_HOLD_FOR_DDL`: hold the first tick that emitted a group in
/// flight, the apply lock held and its ACKs outstanding, until a DDL or a
/// checkpoint parks on that lock. With no checkpoint asked for, a DDL then
/// provably lands mid-tick. One-shot.
static TICK_HOLD_FOR_DDL: Switch = Switch::new("GNITZ_INJECT_TICK_HOLD_FOR_DDL");

/// Poll bound for [`TICK_HOLD_FOR_DDL`], in 1 ms ticks.
const TICK_HOLD_MAX_POLLS: u32 = 10_000;

/// One tick of everything pending per batch of the dispatcher's tick requests.
/// A batch is ticked once its first request is due, or `patient_gap` after the
/// loop's last tick where that is sooner. `patient_gap`
/// (`GNITZ_PATIENT_TICK_GAP_MS`) paces the loop: a held sync is re-issued the
/// moment it is answered, so unpaced it would tick per commit.
pub(super) async fn tick_loop(shared: Rc<Shared>, patient_gap: Duration) {
    let reactor = shared.dispatcher.reactor();
    let mut rx = shared.dispatcher.tick_requests();
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
                wait.await;
            }
            Ok(None) => {}
        }
        patient_from = reactor.now() + patient_gap;
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
/// already running. A drain whose wait another task's emit answered leaves its
/// request queued: the loop then ticks what was committed since.
pub(super) async fn fresh_read_lock(
    shared: &Shared,
    targets: impl IntoIterator<Item = u64>,
    drain_by: Instant,
) -> Result<ReadGuard<CatalogLock>, WireFault> {
    if !shared.dispatcher.all_ticked(targets) {
        shared.dispatcher.drain(drain_by).await?;
    }
    Ok(shared.catalog_rwlock.read().await)
}

#[cfg(test)]
#[path = "tests/tick.rs"]
mod tests;
