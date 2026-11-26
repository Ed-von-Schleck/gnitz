//! The clock: `_system.clock` kept at the current time while a view reads it.
//!
//! Each step commits the clock's one row at the new time in update mode, so the
//! ingest retracts the old row: one push, and the ticks it queues, per step,
//! with no fdatasync, since no restart recovers it. The task steps it every
//! resolution while a view scans it; a DDL creating a view that scans it steps
//! it first, and so does the boot, so no view is computed against a time older
//! than its creation or its process. While no view scans the clock and no such
//! DDL runs, nothing is written. Within one process the clock never moves back,
//! whatever the wall clock does; a view reaching it is rebuilt at boot, so no
//! view carries a time over from an earlier process.

use std::cell::Cell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gnitz_foundation::gnitz_warn;
use gnitz_wire::{CLOCK, WireConflictMode};
use gnitz_zset::repr::BatchBuilder;

use super::Shared;
use super::family::{FamilyRows, TxnFamily};
use super::write::commit_write;
use crate::runtime::sync::AsyncRwLock;

/// The wall clock in TIMESTAMP's unit, microseconds since the Unix epoch.
fn wall_micros() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
}

/// The tag of the clock lock: one step at a time, so two steps never commit
/// out of order.
enum ClockLock {}

/// The time the clock was last stepped to, `0` before its first step, and the
/// lock every step takes.
#[derive(Default)]
pub(super) struct Clock {
    lock: AsyncRwLock<ClockLock>,
    last: Cell<i64>,
}

/// Step the clock every `resolution` (`GNITZ_CLOCK_RESOLUTION_MS`) while a
/// view scans it.
pub(super) async fn clock_loop(shared: &'static Shared, resolution: Duration) {
    let reactor = shared.dispatcher.reactor();
    loop {
        reactor.sleep(resolution).await;
        if !shared.cat().dag.is_scanned(CLOCK.id) {
            continue;
        }
        if let Err(e) = advance(shared).await {
            gnitz_warn!("clock: the step failed: {e}");
        }
    }
}

/// Step the clock to the wall clock, unless it is already there. Takes the
/// clock lock, then the catalog read guard: a caller holds neither.
pub(super) async fn advance(shared: &Shared) -> Result<(), String> {
    let _held = shared.clock.lock.write().await;
    let last = shared.clock.last.get();
    let now = wall_micros().max(last);
    if now == last {
        return Ok(());
    }
    step(shared, now).await?;
    shared.clock.last.set(now);
    Ok(())
}

/// Commit the clock's row at `now`.
async fn step(shared: &Shared, now: i64) -> Result<(), String> {
    // Under the catalog guard, as every write is, so no DDL lands mid-commit.
    let _catalog = shared.catalog_rwlock.read().await;
    let schema = {
        let cat = shared.cat();
        let rel = cat
            .registry
            .relation(CLOCK.id)
            .ok_or("the clock table is not registered")?;
        rel.schema()
    };
    let mut b = BatchBuilder::new(&schema);
    b.begin_row_natives(&[0], 1);
    b.put_int(now as u64 as u128);
    b.end_row();
    let family = TxnFamily::new(CLOCK.id, WireConflictMode::Update, FamilyRows::Built(b.finish()));
    commit_write(shared, vec![family], Vec::new(), false)
        .await
        .map(drop)
        .map_err(|e| e.text)
}
