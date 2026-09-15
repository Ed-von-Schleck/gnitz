//! Zone-LSN accounting shared by the durable allocators (committer, DDL,
//! SERIAL): the allocation high-water and the durability watermark, paired in
//! one type so a caller cannot reserve from one cell and publish to the other.

use std::cell::Cell;

/// Two watermarks with one contract: `reserved` (the allocation high-water)
/// may lead `published` (the durability watermark) while a zone's fdatasync is
/// in flight; `published` never overtakes `reserved`. Every durable allocator
/// reserves its zone LSN via [`reserve`](Self::reserve) — under
/// a `SalExcl` (committer, SERIAL) or the catalog write lock with the
/// committer quiesced (DDL), so reservation order == SAL write order — and
/// publishes via [`publish`](Self::publish) only after its fsync completes, so
/// readers never see an LSN whose data is not yet on disk.
pub struct ZoneLsnAllocator {
    reserved: Cell<u64>,
    published: Cell<u64>,
}

impl ZoneLsnAllocator {
    /// Seed both watermarks; every zone LSN reserved afterwards exceeds `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            reserved: Cell::new(seed),
            published: Cell::new(seed),
        }
    }

    /// Reserve the next zone LSN, stepping past `floor` when a family counter
    /// has drifted above the high-water: un-pinned sys-table ingests auto-bump
    /// family counters, and a checkpoint persists drifted counters as recovery
    /// dedup watermarks — a zone LSN at or below such a watermark would have
    /// its committed-but-unflushed deltas deduped away on recovery. The
    /// committer passes 0 (a user-table push pins no system-family counter);
    /// SERIAL and DDL pass the counters of the families their zone writes.
    /// Strictly monotone: no two zones ever collide, and a failed zone's
    /// reserved LSN (never published) is not reused. No `.await` between load
    /// and store, so the read-modify-write is atomic on the single-threaded
    /// reactor.
    pub fn reserve(&self, floor: u64) -> u64 {
        let zone = self.reserved.get().max(floor) + 1;
        self.reserved.set(zone);
        zone
    }

    /// Publish a reserved zone once its fdatasync completed. Monotone max:
    /// locks drop before the fsync await, so fsyncs can complete out of order —
    /// but SAL write order == zone order, so a completed fsync implies every
    /// lower-numbered zone is already durable; `max` never regresses the
    /// watermark.
    pub fn publish(&self, zone: u64) {
        self.published.set(self.published.get().max(zone));
    }

    /// The durability watermark SCAN/SEEK and tick emission report.
    pub fn published(&self) -> u64 {
        self.published.get()
    }
}

#[cfg(test)]
#[path = "tests/lsn.rs"]
mod tests;
