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

/// A SAL over its own file. Every group's framing, digest and cursor advance
/// come from the code under test rather than from a reimplementation.
pub(crate) struct TestLog {
    /// The whole file, anchor page included.
    file: SharedRegion,
    pub(crate) writer: SalWriter,
    /// The ring's length, the anchor page excluded.
    pub(crate) size: usize,
    /// One W2M ring per worker, carrying the park the writer's wakes land on.
    /// Leaked, as the `'static` wakes require.
    rings: Vec<*mut u8>,
}

impl TestLog {
    /// A log with a `size`-byte ring whose groups carry `workers` slots,
    /// positioned at cursor 0 in `epoch`, which the anchor records.
    pub(crate) fn new(size: usize, workers: usize, epoch: u32) -> TestLog {
        let file = SharedRegion::new(ANCHOR_BYTES + size);
        let rings: Vec<*mut u8> = (0..workers)
            .map(|_| unsafe { crate::runtime::w2m::fixtures::test_ring(RING_BYTES) }.leak())
            .collect();
        // SAFETY: every ring is initialized above and leaked.
        let wakes = rings.iter().map(|&p| unsafe { SalWake::new(p) }).collect();
        // SAFETY: the region outlives the writer.
        let writer = SalWriter::new(
            unsafe { SalLog::new(file.ptr(), file.size()) },
            file.fd(),
            workers,
            wakes,
        );
        let log = TestLog { file, writer, size, rings };
        log.seek(0, epoch);
        log
    }

    /// Worker `w`'s W2M ring.
    pub(crate) fn ring(&self, w: usize) -> *mut u8 {
        self.rings[w]
    }

    /// The ring's first byte — offset 0 of every group base.
    pub(crate) fn ptr(&self) -> *mut u8 {
        self.writer.log.ring
    }

    /// The anchor page's first byte.
    pub(crate) fn anchor_ptr(&self) -> *mut u8 {
        self.file.ptr()
    }

    pub(crate) fn log(&self) -> SalLog {
        self.writer.log
    }

    pub(crate) fn cursor(&self) -> u64 {
        self.writer.write_cursor.get()
    }

    /// Place the cursor and epoch by hand, anchoring the epoch.
    pub(crate) fn seek(&self, cursor: u64, epoch: u32) {
        self.writer.write_cursor.set(cursor);
        self.writer.epoch.set(epoch);
        self.writer.write_anchor(epoch, self.writer.synced.get());
    }

    /// Anchor `offset` as covered by a completed `fdatasync` in the live epoch.
    pub(crate) fn synced_through(&self, offset: u64) {
        self.writer.mark_synced(self.writer.epoch(), offset);
    }

    /// Append one group whose slots are `payloads` verbatim; returns its base.
    pub(crate) fn write(&self, target: u32, lsn: u64, kind: SalMessageKind, payloads: &[&[u8]]) -> u64 {
        self.try_write(target, lsn, kind, 0, payloads).expect("group fits")
    }

    /// [`Self::write`] with the flag byte given and the writer's verdict
    /// reported.
    pub(crate) fn try_write(
        &self,
        target: u32,
        lsn: u64,
        kind: SalMessageKind,
        flags: u8,
        payloads: &[&[u8]],
    ) -> Result<u64, SalFit> {
        let base = self.cursor();
        let (at, word) = self.lay_out(target, lsn, kind, flags, payloads)?;
        self.writer.publish(at, word);
        Ok(base)
    }

    /// A zone member of `scope` whose slots are `payloads` verbatim, unpublished
    /// until the scope commits.
    pub(crate) fn try_write_in(
        &self,
        scope: &SalScope,
        target: u32,
        kind: SalMessageKind,
        payloads: &[&[u8]],
    ) -> Result<u64, SalFit> {
        let base = self.cursor();
        self.lay_out(target, scope.lsn, kind, 0, payloads)?;
        scope.last_member.set(Some(base));
        Ok(base)
    }

    /// Lay one group of verbatim slot payloads out, unpublished: `(base, prefix
    /// word)`, the shape [`SalWriter::write_slots`] answers with.
    fn lay_out(
        &self,
        target: u32,
        lsn: u64,
        kind: SalMessageKind,
        flags: u8,
        payloads: &[&[u8]],
    ) -> Result<(usize, u64), SalFit> {
        let sizes: Vec<u32> = payloads.iter().map(|p| p.len() as u32).collect();
        self.writer.write_slots(target, lsn, kind, flags, 0, &sizes, |w, slot| {
            slot.copy_from_slice(payloads[w])
        })
    }

    /// One scattered `Push` group of `batch` over this log's slots: rows
    /// PK-partitioned by `schema`, one request id per slot. `write` decides
    /// where it goes — the writer's publishes, a scope's defers. Returns its base.
    pub(crate) fn push_group(
        &self,
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

/// The group published at `base`, walked at the anchor's epoch.
pub(crate) fn group_at(log: SalLog, base: u64) -> SalMessage {
    group_and_next(log, base, log.anchor().expect("the anchor verifies").0).0
}
