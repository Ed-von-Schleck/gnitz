//! The SAL fixture: a log over its own shared region, written by the
//! production writer, plus the reader every assertion goes through.
//!
//! A child of `writer`, so it reaches `write_slots` and the cursor directly: the
//! forged groups below are what no scope writes — arbitrary payload bytes, an
//! open zone, a read answered on no id — and every other group goes
//! through [`SalExcl`]. The one test `impl` of the writer: the log shapes and
//! the damage every SAL test builds on are here.

use super::*;
use crate::runtime::ServerExit;
use crate::runtime::sal::format::RequestHead;
use crate::runtime::sal::log::SalMessage;
use crate::runtime::sal::reader::SalReader;
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::fixtures::id;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_zset::schema::encode_schema_record;
use std::fs::File;
use std::ops::Range;

/// A control-only push into `tid`: one that carries no rows.
pub(in crate::runtime::sal) fn control_push(tid: u64) -> Group<'static> {
    Group::push(tid, PkRule::Enforce, &[], Routed::Same(None))
}

/// `read` sent to every worker, as no writer frames it: answered on no id.
pub(in crate::runtime::sal) fn unanswered<'a>(read: &'a Read<'_>) -> Group<'a> {
    Group {
        reply: None,
        ..Group::read(read, WorkerSet::ALL, None, id(1))
    }
}

/// The two rows every push of [`SalWriter::push_zone`] carries.
pub(crate) fn zone_rows() -> Batch {
    make_batch(&make_schema_u64_i64(), &[(1, 1, 10), (2, 1, 20)])
}

/// A writer over a fresh `size`-byte ring written at `workers` workers, rewound
/// to cursor 0 of `epoch` as a boot leaves it.
pub(crate) fn test_log(size: usize, workers: usize, epoch: u32) -> SalWriter {
    let len = ANCHOR_BYTES + size;
    // Leaked: open and mapped for the rest of the process.
    let file: &'static File = Box::leak(Box::new(gnitz_foundation::posix_io::memfd(c"test_sal").expect("memfd")));
    let parks = WorkerParks::create(workers).expect("map the parks");
    SalWriter::rewound(SalLog::map(file, len).expect("map the test SAL"), parks, epoch)
}

impl SalWriter {
    /// The log bytes a push of `rows` under the schema record `record` occupies,
    /// wherever [`SalScope::push`] lays it out. Neither its target nor its rule
    /// sizes a group.
    pub(crate) fn push_footprint(&self, record: &[u8], rows: Routed<'_>) -> usize {
        self.footprint(&Group::push(0, PkRule::Enforce, record, rows))
    }

    /// Every worker's park: where the writer's wakes land.
    pub(crate) fn parks(&self) -> WorkerParks {
        self.parks
    }

    /// The ring bytes `at`, which a test damages or reads raw.
    pub(crate) fn ring_mut(&mut self, at: Range<usize>) -> &mut [u8] {
        assert!(at.start <= at.end && at.end <= self.log.ring_len, "inside the ring");
        // SAFETY: bounded directly above; `&mut self` holds every other
        // reference of the test off, and no reader runs meanwhile.
        unsafe { self.log.span_mut(at.start, at.len()) }
    }

    /// The publication prefix word of the group at `base`, as bytes.
    pub(crate) fn prefix_mut(&mut self, base: u64) -> &mut [u8] {
        self.ring_mut(base as usize..base as usize + PREFIX_BYTES)
    }

    /// The anchor word at the head of the file, as bytes.
    pub(crate) fn anchor_mut(&mut self) -> &mut [u8] {
        // SAFETY: the anchor page is mapped ahead of the ring; `&mut self` as
        // `ring_mut`.
        unsafe { std::slice::from_raw_parts_mut(self.log.ring.sub(ANCHOR_BYTES), PREFIX_BYTES) }
    }

    /// The format word behind the anchor, as bytes.
    pub(crate) fn format_mut(&mut self) -> &mut [u8] {
        // SAFETY: as `anchor_mut`, the next word of the same page.
        unsafe { std::slice::from_raw_parts_mut(self.log.ring.sub(ANCHOR_BYTES - PREFIX_BYTES), PREFIX_BYTES) }
    }

    /// Worker `w`'s live drain from cursor 0 of the epoch groups are written at.
    pub(crate) fn reader(&self, w: u32) -> SalReader {
        SalReader::new(self.log, w, self.epoch.get())
    }

    pub(crate) fn log(&self) -> &SalLog {
        &self.log
    }

    pub(crate) fn cursor(&self) -> u64 {
        self.write_cursor.get()
    }

    /// The epoch groups are written at.
    pub(crate) fn epoch(&self) -> u32 {
        self.epoch.get()
    }

    /// The most bytes of pushes that fit directly behind this epoch's checkpoint.
    pub(crate) fn push_room(&self) -> usize {
        group_limit(SalMessageKind::Push, self.log.ring_len) - self.checkpoint_floor.get() as usize
    }

    /// The writer with its checkpoint threshold at `bytes`.
    pub(crate) fn checkpoint_at(mut self, bytes: u64) -> Self {
        self.checkpoint_threshold = bytes;
        self
    }

    /// Sole write access; nothing else in a test holds it.
    pub(crate) fn excl(&self) -> SalExcl<'_> {
        try_poll_once(self.lock()).expect("uncontended")
    }

    /// Place the write cursor by hand.
    pub(crate) fn seek(&self, cursor: u64) {
        self.write_cursor.set(cursor);
    }

    /// Append one group — a member of an open zone when `zoned` — sending worker
    /// `w` the bytes `payloads[w]` verbatim, an empty one being a worker the
    /// group does not address; returns its base.
    pub(in crate::runtime::sal) fn forge(
        &self,
        target: u64,
        zoned: bool,
        kind: SalMessageKind,
        payloads: &[&[u8]],
    ) -> u64 {
        self.try_forge(target, zoned, kind, payloads).expect("group fits")
    }

    /// [`Self::forge`] with the writer's verdict reported.
    pub(in crate::runtime::sal) fn try_forge(
        &self,
        target: u64,
        zoned: bool,
        kind: SalMessageKind,
        payloads: &[&[u8]],
    ) -> Result<u64, SalFull> {
        let written = || payloads.iter().enumerate().filter(|(_, p)| !p.is_empty());
        let sizes: Vec<usize> = written().map(|(_, p)| p.len()).collect();
        let request = RequestHead {
            kind,
            target_id: target,
            arg0: 0,
            arg1: 0,
        };
        let head = GroupHead::new(request, zoned, false, None, written().map(|(w, _)| w).collect());
        let bytes: Vec<&[u8]> = written().map(|(_, p)| *p).collect();
        let base = self.cursor();
        let end =
            (self.write_slots(base, head, &sizes, |i, slot| slot.copy_from_slice(bytes[i]))).map_err(|mut full| {
                self.judge(base, &mut full);
                full
            })?;
        self.publish(base);
        self.seek(end);
        Ok(base)
    }

    /// One committed zone of `groups`: its LSN and every member's base.
    pub(in crate::runtime::sal) fn commit_zone(&self, groups: &[Group]) -> (u64, Vec<u64>) {
        let mut excl = self.excl();
        let mut scope = excl.begin();
        let bases = groups
            .iter()
            .map(|g| {
                let base = scope.end();
                scope.write(g, true).expect("group fits");
                base
            })
            .collect();
        (scope.commit().expect("the zone was open"), bases)
    }

    /// One committed zone of `DdlSync` groups, as a DDL's broadcasts write it.
    pub(crate) fn ddl_zone(&self, groups: &[(SysFamily, &Batch)]) {
        let mut excl = self.excl();
        let mut scope = excl.begin();
        for &(family, batch) in groups {
            scope.ddl_sync(family, batch).expect("group fits");
        }
        scope.commit().expect("the zone was open");
    }

    /// A committed zone of one control-only `Push` group per target: its LSN and
    /// every member's base.
    pub(crate) fn zone(&self, targets: &[u64]) -> (u64, Vec<u64>) {
        let groups: Vec<Group> = targets.iter().map(|&tid| control_push(tid)).collect();
        self.commit_zone(&groups)
    }

    /// A committed zone of one `Push` group per target, every worker sent its
    /// own payload of [`zone_rows`].
    pub(crate) fn push_zone(&self, targets: &[u64]) -> (u64, Vec<u64>) {
        let batch = zone_rows();
        let each = vec![batch.wire_whole(); self.num_workers()];
        let record = encode_schema_record(batch.schema());
        let groups: Vec<Group> = targets
            .iter()
            .map(|&t| Group::push(t, PkRule::Enforce, &record, Routed::Each(&each)))
            .collect();
        self.commit_zone(&groups)
    }

    /// The command group the committer fires between zones, outside every zone.
    /// Returns its base.
    pub(crate) fn command(&self) -> u64 {
        self.forge(9, false, SalMessageKind::Tick, &[&[0u8; 64]])
    }

    /// The first member of a zone that never closes — what a crash between a
    /// zone's groups leaves: published, and its header intact. Returns its base.
    pub(crate) fn open_member(&self) -> u64 {
        self.forge(21, true, SalMessageKind::Push, &[&[0u8; 64]])
    }

    /// The unsealed header of the group at `base`, sized by its own payload
    /// count.
    pub(crate) fn header_mut(&mut self, base: u64) -> &mut [u8] {
        // SAFETY: a test only asks for a header it wrote, and `&mut self` holds
        // every other reference off.
        unsafe { self.log.header_mut(base) }
    }

    /// Flip one bit of the header at `base`.
    pub(crate) fn damage_header(&mut self, base: u64) {
        self.header_mut(base)[0] ^= 1;
    }

    /// Corrupt one byte of worker `w`'s payload of the group at `base`.
    pub(crate) fn damage_slot(&mut self, base: u64, w: u32) {
        let msg = group_at(&self.log, base);
        let slot = msg.slot(w).expect("the group addresses the worker");
        // The payloads from the worker's own onward end the group.
        let behind: usize = msg
            .payloads()
            .skip_while(|p| !std::ptr::eq(*p, slot))
            .map(|p| p.len().next_multiple_of(8))
            .sum();
        let at = msg.end as usize - behind + slot.len() / 2;
        self.ring_mut(at..at + 1)[0] ^= 0xFF;
    }
}

impl SalExcl<'_> {
    /// Write and publish `g` outside every zone, whatever its shape.
    pub(in crate::runtime::sal) fn write(&mut self, g: &Group) -> Result<(), SalFull> {
        self.write_group(g)
    }

    /// Push `rows` into `tid`, a unit of its own outside every zone: a push no
    /// restart recovers.
    pub(crate) fn push(&mut self, tid: u64, record: &[u8], rows: Routed<'_>) -> Result<(), SalFull> {
        let mut scope = self.begin();
        scope.push(tid, PkRule::Enforce, record, rows, false)?;
        scope.commit();
        Ok(())
    }
}

impl SalScope<'_, '_> {
    /// The end of the scope's latest group: where its next one is laid out.
    pub(crate) fn end(&self) -> u64 {
        self.staged.end
    }
}

/// The group published at `base`, walked at the anchor's epoch.
pub(crate) fn group_at(log: &SalLog, base: u64) -> SalMessage<'_> {
    let epoch = log.anchor().expect("the anchor verifies");
    assert_ne!(log.prefix_word(base), 0, "a group is published at offset {base}");
    log.group_at(base, epoch)
        .unwrap_or_else(|| panic!("the group at offset {base} verifies"))
}

/// A read of `tid` under the blob `spec`.
pub(crate) fn scan(tid: u64, spec: &[u8]) -> Read<'_> {
    Read::ScanSpec { tid, reply_layout: 0, spec }
}

/// Run the test `internal` of the module `module` in a re-exec'd child and
/// require it to fail-stop with `reason` on stderr.
pub(in crate::runtime::sal) fn assert_fail_stops(module: &str, internal: &str, reason: &str) {
    let out = crate::test_support::run_test_in_child(module, internal, &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(ServerExit::FailStop.code()),
        "{internal} must fail-stop (exit 134)\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&out.stdout)
    );
    // The code alone would be satisfied by any other fatal abort reached first.
    assert!(
        stderr.contains(reason),
        "{internal} aborted for the wrong reason\nstderr:\n{stderr}"
    );
}
