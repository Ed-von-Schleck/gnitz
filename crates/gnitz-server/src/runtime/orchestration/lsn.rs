//! Zone-LSN accounting shared by the durable allocators (committer, DDL,
//! SERIAL): the allocation high-water and the durability watermark, paired in
//! one type so a caller cannot reserve from one cell and publish to the other.

use std::cell::Cell;

/// Two watermarks with one contract: `reserved` (the allocation high-water)
/// may lead `published` (the durability watermark) while a zone's fdatasync is
/// in flight; `published` never overtakes `reserved`. Every durable allocator
/// reserves its zone LSN via [`reserve`](Self::reserve) — under
/// `sal_writer_excl` (committer, SERIAL) or the catalog write lock with the
/// committer quiesced (DDL), so reservation order == SAL write order — and
/// publishes via [`publish`](Self::publish) only after its fsync completes, so
/// readers never see an LSN whose data is not yet on disk.
pub struct ZoneLsnAllocator {
    reserved: Cell<u64>,
    published: Cell<u64>,
}

impl ZoneLsnAllocator {
    /// Seed both watermarks; boot passes `max_table_current_lsn()` so every
    /// zone LSN stays strictly greater than each table's counter across
    /// restarts.
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
mod tests {
    use super::*;

    /// Overlapping reservations get distinct, strictly-monotone zones; a
    /// drifted family counter (floor) lifts the next zone past itself and a
    /// lower floor never lowers one.
    #[test]
    fn reserve_is_monotone_and_dominates_the_floor() {
        let lsns = ZoneLsnAllocator::new(100);
        assert_eq!(lsns.reserve(0), 101, "first zone is high-water + 1");
        assert_eq!(lsns.reserve(0), 102, "second zone steps past the first");
        assert_eq!(lsns.reserve(150), 151, "a drifted floor lifts the zone past it");
        assert_eq!(lsns.reserve(10), 152, "a low floor never lowers the next zone");
    }

    /// Publish is monotone-max: a later zone's fsync completing first must not
    /// be regressed by an earlier zone's late completion.
    #[test]
    fn publish_is_monotone_max() {
        let lsns = ZoneLsnAllocator::new(100);
        let (a, b) = (lsns.reserve(0), lsns.reserve(0));
        lsns.publish(b);
        assert_eq!(lsns.published(), b);
        lsns.publish(a);
        assert_eq!(lsns.published(), b, "a late lower zone never lowers the watermark");
    }
}
