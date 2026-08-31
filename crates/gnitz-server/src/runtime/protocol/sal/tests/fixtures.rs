//! The SAL fixture: a log over its own shared region, written by the
//! production writer, plus the reader every assertion goes through.
//!
//! A child of `sal`, so it reaches `begin`/`finish` and the cursor directly and
//! no test-only writer path has to exist in production. `pub(crate)` because
//! the worker drain and the block-integrity suite need a real SAL too.

use super::*;
use crate::runtime::test_support::SharedRegion;

/// A SAL over its own shared region. Every group's framing, digest and cursor
/// advance come from the code under test rather than from a reimplementation.
pub(crate) struct TestLog {
    region: SharedRegion,
    pub(crate) writer: SalWriter,
    pub(crate) size: usize,
    workers: usize,
}

impl TestLog {
    /// A log of `size` bytes whose groups carry `workers` slots, positioned at
    /// cursor 0 in `epoch`.
    pub(crate) fn new(size: usize, workers: usize, epoch: u32) -> TestLog {
        let region = SharedRegion::new(size);
        let writer = SalWriter::new(region.ptr(), -1, size as u64, workers);
        writer.epoch.set(epoch);
        TestLog {
            region,
            writer,
            size,
            workers,
        }
    }

    /// Start a fresh writer at cursor 0 in `epoch` over the same bytes — the
    /// leftover shape, where a shorter later pass leaves an earlier epoch's
    /// groups standing past its own frontier.
    pub(crate) fn relayout(&mut self, epoch: u32) {
        self.writer = SalWriter::new(self.region.ptr(), -1, self.size as u64, self.workers);
        self.writer.epoch.set(epoch);
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
    /// two groups at consecutive offsets under different epochs, or a cursor
    /// parked just under a cap.
    pub(crate) fn seek(&self, cursor: u64, epoch: u32) {
        self.writer.write_cursor.set(cursor);
        self.writer.epoch.set(epoch);
    }

    /// Append one group whose slots are `payloads` verbatim; returns its base.
    pub(crate) fn write(&self, target: u32, lsn: u64, kind: SalMessageKind, payloads: &[&[u8]]) -> u64 {
        self.try_write(target, lsn, kind, ZoneMark::Plain, payloads)
            .expect("group fits")
    }

    /// Same, reporting the writer's verdict — for the tests about refusal.
    pub(crate) fn try_write(
        &self,
        target: u32,
        lsn: u64,
        kind: SalMessageKind,
        mark: ZoneMark,
        payloads: &[&[u8]],
    ) -> Result<u64, SalFit> {
        let base = self.cursor();
        let sizes: Vec<u32> = payloads.iter().map(|p| p.len() as u32).collect();
        let group = self.writer.begin(target, lsn, kind, mark, &sizes)?;
        unsafe { group.for_each_slot(|w, slot| slot.copy_from_slice(payloads[w])) };
        self.writer.finish(group);
        Ok(base)
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
