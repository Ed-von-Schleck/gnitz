//! The SAL fixture: a log over its own shared region, written by the
//! production writer, plus the reader every assertion goes through.
//!
//! A child of `sal`, so it reaches `write_slots` and the cursor directly and
//! no test-only writer path has to exist in production.

use std::os::fd::BorrowedFd;

use super::*;
use crate::runtime::master::scatter::with_routed;
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
    /// The anchor page's first byte, the start of the whole (leaked) file.
    anchor: *mut u8,
    pub(crate) writer: SalWriter,
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
        // SAFETY: the region is leaked below, so it is mapped and its fd open for
        // the rest of the process.
        let writer = SalWriter::new(
            unsafe { SalLog::new(file.ptr(), file.size()) },
            unsafe { BorrowedFd::borrow_raw(file.fd()) },
            workers,
            wakes,
        );
        let anchor = file.leak();
        let log = TestLog { anchor, writer, rings };
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
        self.anchor
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

    /// The exact number of SAL bytes [`SalWriter::write`] consumes for `g`; the
    /// checkpoint band is not included.
    pub(crate) fn footprint(&self, g: &DirectGroup) -> usize {
        let nw = self.writer.num_workers();
        let mut sizes = vec![0u32; nw];
        g.slot_sizes_into(&mut sizes);
        group_total_size(nw, sizes.iter().copied())
    }

    /// Append one group whose slots are `payloads` verbatim; returns its base.
    pub(crate) fn write(&self, target: u64, lsn: u64, kind: SalMessageKind, payloads: &[&[u8]]) -> u64 {
        self.try_write(target, lsn, kind, 0, payloads).expect("group fits")
    }

    /// [`Self::write`] with the flag byte given and the writer's verdict
    /// reported.
    pub(super) fn try_write(
        &self,
        target: u64,
        lsn: u64,
        kind: SalMessageKind,
        flags: u8,
        payloads: &[&[u8]],
    ) -> Result<u64, WireFault> {
        let base = self.lay_out(target, lsn, kind, flags, payloads)?;
        self.writer.publish(base);
        Ok(base as u64)
    }

    /// Lay one group of verbatim slot payloads out, unpublished, and return its
    /// base, as [`SalWriter::write_slots`] does.
    fn lay_out(
        &self,
        target: u64,
        lsn: u64,
        kind: SalMessageKind,
        flags: u8,
        payloads: &[&[u8]],
    ) -> Result<usize, WireFault> {
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
        tid: u64,
        schema: SchemaDescriptor,
        batch: &Batch,
        write: impl FnOnce(&DirectGroup) -> Result<(), WireFault>,
    ) -> u64 {
        let base = self.cursor();
        let nw = self.writer.num_workers();
        let relation = ipc::WireSchema::encoded(tid, schema);
        with_routed(batch, &relation, nw, |_, data| {
            write(&DirectGroup {
                template: relation.frame(WireMsg::default()),
                data,
                targets: GroupTargets::all(0),
                ..DirectGroup::new(SalMessageKind::Push)
            })
            .expect("group fits")
        });
        base
    }
}

impl TestLog {
    /// One committed, synced zone at `lsn` of whole-batch DdlSync groups.
    pub(crate) fn ddl_zone(&self, lsn: u64, groups: &[(u64, SchemaDescriptor, &Batch)]) {
        let scope = self.writer.begin(lsn, "test");
        for &(tid, schema, batch) in groups {
            let relation = ipc::WireSchema::encoded(tid, schema);
            let group = DirectGroup {
                template: relation.frame(WireMsg::default()),
                data: GroupData::Same(WireData::Whole(batch)),
                ..DirectGroup::new(SalMessageKind::DdlSync)
            };
            scope.write(&group, true).expect("group fits");
        }
        scope.commit();
        self.synced_through(self.cursor());
    }
}

/// The group at `base` under `epoch`.
pub(crate) fn group_in(log: SalLog, base: u64, epoch: u32) -> SalMessage {
    match log.read_at(base, EpochGate::Walk(epoch)) {
        SalStep::Group(msg) => msg,
        _ => panic!("a group is published at offset {base}"),
    }
}

/// The group published at `base`, walked at the anchor's epoch.
pub(crate) fn group_at(log: SalLog, base: u64) -> SalMessage {
    group_in(log, base, log.anchor().expect("the anchor verifies").0)
}

/// A payload-less group outside every zone.
pub(crate) fn bare_message(kind: SalMessageKind, target_id: u64) -> SalMessage {
    SalMessage {
        lsn: 0,
        kind,
        zone_end: false,
        target_id,
        base: 0,
        end: 0,
        request_id: 0,
        in_request_order: false,
        payload: &[],
        dir: &[],
    }
}
