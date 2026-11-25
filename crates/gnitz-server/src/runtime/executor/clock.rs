//! The clock: the task that keeps `_system.clock` at the current time while a
//! view reads it.
//!
//! Each step commits the clock's one row at the new time in update mode, so the
//! ingest retracts the old row: one push, and the ticks it queues, per step,
//! with no fdatasync, since no restart recovers it. While no view scans the
//! clock nothing is written. Within one process the
//! clock never moves back, whatever the wall clock does; a view reaching it is
//! rebuilt at boot, so no view carries a time over from an earlier process.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gnitz_foundation::gnitz_warn;
use gnitz_wire::{CLOCK, WireConflictMode};
use gnitz_zset::repr::BatchBuilder;

use super::Shared;
use super::family::{FamilyRows, TxnFamily};
use super::write::commit_write;

/// The wall clock in TIMESTAMP's unit, microseconds since the Unix epoch.
fn wall_micros() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
}

/// Step the clock every `resolution` (`GNITZ_CLOCK_RESOLUTION_MS`).
pub(super) async fn clock_loop(shared: &'static Shared, resolution: Duration) {
    let reactor = shared.dispatcher.reactor();
    let mut last = i64::MIN;
    loop {
        reactor.sleep(resolution).await;
        let now = wall_micros().max(last);
        if now == last || !shared.cat().dag.is_scanned(CLOCK.id) {
            continue;
        }
        match step(shared, now).await {
            Ok(()) => last = now,
            Err(e) => gnitz_warn!("clock: the step to {now} failed: {e}"),
        }
    }
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
