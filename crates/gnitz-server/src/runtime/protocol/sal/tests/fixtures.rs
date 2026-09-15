//! The SAL fixture: a log over its own shared region, written by the
//! production writer, plus the reader every assertion goes through.
//!
//! A child of `sal`, so it reaches `write_slots` and the cursor directly and
//! no test-only writer path has to exist in production. `pub(crate)` because
//! the worker drain and the block-integrity suite need a real SAL too.

use super::*;
use crate::runtime::master::scatter::{with_commit_indices, with_group};
use crate::runtime::test_support::SharedRegion;
use crate::runtime::w2m::SalWake;
use crate::runtime::wire as ipc;

/// Each worker ring's size: room for a few control frames, which is all a SAL
/// test ever sends back over one.
const RING_BYTES: usize = 64 * 1024;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;

/// A SAL over its own shared region. Every group's framing, digest and cursor
/// advance come from the code under test rather than from a reimplementation.
pub(crate) struct TestLog {
    region: SharedRegion,
    pub(crate) writer: SalWriter,
    pub(crate) size: usize,
    /// One W2M ring per worker, carrying the park the writer's wakes land on.
    /// Leaked, as the `'static` wakes require.
    rings: Vec<*mut u8>,
}

impl TestLog {
    /// A log of `size` bytes whose groups carry `workers` slots, positioned at
    /// cursor 0 in `epoch`.
    pub(crate) fn new(size: usize, workers: usize, epoch: u32) -> TestLog {
        let region = SharedRegion::new(size);
        let rings: Vec<*mut u8> = (0..workers)
            .map(|_| unsafe { crate::runtime::w2m::fixtures::test_ring(RING_BYTES) }.leak())
            .collect();
        // SAFETY: every ring is initialized above and leaked.
        let wakes = rings.iter().map(|&p| unsafe { SalWake::new(p) }).collect();
        let writer = SalWriter::new(region.ptr(), -1, size, workers, wakes);
        writer.epoch.set(epoch);
        TestLog { region, writer, size, rings }
    }

    /// Worker `w`'s W2M ring.
    pub(crate) fn ring(&self, w: usize) -> *mut u8 {
        self.rings[w]
    }

    pub(crate) fn ptr(&self) -> *mut u8 {
        self.region.ptr()
    }

    pub(crate) fn log(&self) -> SalLog {
        unsafe { SalLog::new(self.region.ptr() as *const u8, self.size) }
    }

    pub(crate) fn cursor(&self) -> u64 {
        self.writer.write_cursor.get()
    }

    /// Place the cursor and epoch by hand — the shapes no `rewind` produces:
    /// two groups at consecutive offsets under different epochs, a cursor
    /// parked just under a cap, or a shorter later epoch laid over an earlier
    /// one's groups so they stand past its frontier.
    pub(crate) fn seek(&self, cursor: u64, epoch: u32) {
        self.writer.write_cursor.set(cursor);
        self.writer.epoch.set(epoch);
    }

    /// Append one group whose slots are `payloads` verbatim; returns its base.
    pub(crate) fn write(&self, target: u32, lsn: u64, kind: SalMessageKind, payloads: &[&[u8]]) -> u64 {
        self.try_write(target, lsn, kind, false, payloads).expect("group fits")
    }

    /// Same, with the zone-start byte spelled by the caller and the writer's
    /// verdict reported — below the zone-state layer, so a shape the production
    /// writer never lays down (a start with no commit, a sentinel with no zone
    /// open) can be.
    pub(crate) fn try_write(
        &self,
        target: u32,
        lsn: u64,
        kind: SalMessageKind,
        zone_start: bool,
        payloads: &[&[u8]],
    ) -> Result<u64, SalFit> {
        let base = self.cursor();
        let (at, word) = self.lay_out(target, lsn, kind, zone_start, payloads)?;
        self.writer.publish(at, word);
        Ok(base)
    }

    /// [`Self::try_write`] into `scope`, so the group stays unpublished until
    /// the scope commits. `zone_start` is spelled by the caller, as there.
    pub(crate) fn try_write_in(
        &self,
        scope: &SalScope,
        target: u32,
        kind: SalMessageKind,
        zone_start: bool,
        payloads: &[&[u8]],
    ) -> Result<u64, SalFit> {
        let base = self.cursor();
        self.lay_out(target, scope.lsn, kind, zone_start, payloads)?;
        if zone_start {
            scope.zone_open.set(true);
        }
        Ok(base)
    }

    /// Lay one group of verbatim slot payloads out, unpublished: `(base, prefix
    /// word)`, the shape [`SalWriter::write_slots`] answers with.
    fn lay_out(
        &self,
        target: u32,
        lsn: u64,
        kind: SalMessageKind,
        zone_start: bool,
        payloads: &[&[u8]],
    ) -> Result<(usize, u64), SalFit> {
        let sizes: Vec<u32> = payloads.iter().map(|p| p.len() as u32).collect();
        self.writer
            .write_slots(target, lsn, kind, zone_start, 0, false, true, &sizes, |w, slot| {
                slot.copy_from_slice(payloads[w])
            })
    }

    /// A raw commit sentinel at `lsn` — a slotless group — whatever zone state
    /// the writer holds. Returns its base.
    pub(crate) fn sentinel(&self, lsn: u64) -> u64 {
        self.try_write(0, lsn, SalMessageKind::ZoneCommit, false, &[])
            .expect("sentinel fits")
    }

    /// One scattered `Push` group of `batch` over this log's slots: rows
    /// PK-partitioned by `schema`, one request id per slot. `write` decides
    /// where it goes — the writer's publishes, a scope's defers. Returns its base.
    pub(crate) fn push_group(
        &self,
        lsn: u64,
        tid: u32,
        schema: SchemaDescriptor,
        batch: &Batch,
        write: impl FnOnce(&DirectGroup) -> Result<(), WireFault>,
    ) -> u64 {
        let base = self.cursor();
        let nw = self.writer.num_workers();
        let relation = ipc::WireSchema::encoded(tid as i64, schema);
        let group = DirectGroup {
            targets: GroupTargets::all(0),
            lsn,
            ..DirectGroup::new(SalMessageKind::Push)
        };
        with_commit_indices(batch, &schema, nw, |wi| {
            with_group(batch, wi, &relation, group, write).expect("group fits")
        });
        base
    }
}

/// The group at `base` under `epoch`, and the cursor past it.
pub(crate) fn group_and_next(log: SalLog, base: u64, epoch: u32) -> (SalMessage, u64) {
    match log.read_at(base, EpochGate::Walk(epoch)) {
        SalStep::Group(msg, next) => (msg, next),
        _ => panic!("a group is published at offset {base}"),
    }
}

/// The group published at `base`, walked at the log's current epoch.
pub(crate) fn group_at(log: SalLog, base: u64) -> SalMessage {
    group_and_next(log, base, log.walk_epoch()).0
}
