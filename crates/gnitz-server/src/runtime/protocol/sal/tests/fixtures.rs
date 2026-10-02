//! The SAL fixture: a log over its own shared region, written by the
//! production writer, plus the reader every assertion goes through.
//!
//! A child of `sal`, so it reaches `write_slots` and the cursor directly: the
//! raw writes below forge what no scope writes — arbitrary LSNs and flags, an
//! open zone — and every other group goes through [`SalExcl`].

use std::os::fd::FromRawFd;

use super::*;
use crate::catalog::SysFamily;
use crate::runtime::test_support::try_poll_once;
use gnitz_zset::schema::encode_schema_block;

/// A writer over a fresh `size`-byte ring, written at one worker per park of
/// `parks`. Unrewound, as a boot's writer is before [`SalExcl::boot_rewind`].
pub(crate) fn test_writer(size: usize, parks: WorkerParks) -> SalWriter {
    let len = ANCHOR_BYTES + size;
    let fd = unsafe { libc::memfd_create(c"test_sal".as_ptr(), libc::MFD_CLOEXEC) };
    assert!(fd >= 0, "memfd_create: {}", std::io::Error::last_os_error());
    // SAFETY: a descriptor nothing else owns. Leaked: open and mapped for the rest of the process.
    let file: &'static File = Box::leak(Box::new(unsafe { File::from_raw_fd(fd) }));
    SalWriter::new(SalLog::map(file, len).expect("map the test SAL"), parks)
}

/// A SAL over its own file.
pub(crate) struct TestLog {
    pub(crate) writer: SalWriter,
    /// The parks the writer's wakes land on.
    parks: WorkerParks,
}

impl TestLog {
    /// A log with a `size`-byte ring written at `workers` workers, rewound to
    /// cursor 0 of `epoch`.
    pub(crate) fn new(size: usize, workers: usize, epoch: u32) -> TestLog {
        let parks = WorkerParks::create(workers).expect("map the parks");
        let writer = test_writer(size, parks);
        writer.rewind(epoch);
        TestLog { writer, parks }
    }

    /// Every worker's park.
    pub(crate) fn parks(&self) -> WorkerParks {
        self.parks
    }

    /// The ring's first byte — offset 0 of every group base.
    pub(crate) fn ptr(&self) -> *mut u8 {
        self.writer.log.ring
    }

    /// The anchor page's first byte, the start of the whole file.
    pub(crate) fn anchor_ptr(&self) -> *mut u8 {
        self.writer.log.anchor_record().cast()
    }

    pub(crate) fn log(&self) -> SalLog {
        self.writer.log
    }

    pub(crate) fn cursor(&self) -> u64 {
        self.writer.write_cursor.get()
    }

    /// Sole write access; nothing else in a test holds it.
    pub(crate) fn excl(&self) -> SalExcl<'_> {
        try_poll_once(self.writer.lock()).expect("uncontended")
    }

    /// Place the write cursor by hand.
    pub(crate) fn seek(&self, cursor: u64) {
        self.writer.write_cursor.set(cursor);
    }

    /// Anchor `offset` as covered by a completed `fdatasync` in the live epoch.
    pub(crate) fn synced_through(&self, offset: u64) {
        self.writer.mark_synced(self.writer.epoch(), offset);
    }

    /// Append one group sending worker `w` the bytes `payloads[w]` verbatim, an
    /// empty one being a worker the group does not address; returns its base.
    pub(super) fn write(&self, target: u64, lsn: u64, kind: SalMessageKind, payloads: &[&[u8]]) -> u64 {
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
        let written = || payloads.iter().enumerate().filter(|(_, p)| !p.is_empty());
        let sizes: Vec<usize> = written().map(|(_, p)| p.len()).collect();
        let head = GroupHead {
            lsn,
            kind,
            flags,
            request_id: 0,
            target_id: target,
            targets: written().fold(WorkerSet::EMPTY, |set, (w, _)| set.with(w)),
        };
        let mut bytes = written().map(|(_, p)| *p);
        let base = self.writer.write_slots(head, &sizes, |_, slot| {
            slot.copy_from_slice(bytes.next().expect("one payload per size"))
        })?;
        self.writer.publish(base);
        Ok(base)
    }

    /// One committed zone of `groups`: its LSN and every member's base.
    pub(crate) fn commit_zone(&self, groups: &[DirectGroup]) -> (u64, Vec<u64>) {
        let mut excl = self.excl();
        let scope = excl.begin("test");
        let bases = groups
            .iter()
            .map(|g| {
                let base = self.cursor();
                scope.write(g, true).expect("group fits");
                base
            })
            .collect();
        let lsn = scope.lsn();
        assert!(scope.commit(), "the zone was open");
        (lsn, bases)
    }

    /// One committed, synced zone of `DdlSync` groups, as a DDL's broadcasts
    /// write it. Returns its LSN.
    pub(crate) fn ddl_zone(&self, groups: &[(SysFamily, &Batch)]) -> u64 {
        let records: Vec<Vec<u8>> = groups
            .iter()
            .map(|(family, _)| encode_schema_block(family.schema()))
            .collect();
        let groups: Vec<DirectGroup> = records
            .iter()
            .zip(groups)
            .map(|(record, &(family, batch))| DirectGroup::ddl_sync(family.id(), record, batch))
            .collect();
        let (lsn, _) = self.commit_zone(&groups);
        self.synced_through(self.cursor());
        lsn
    }

    /// The header of the group at `base`, sized by its own payload count.
    pub(crate) fn header_mut(&mut self, base: u64) -> &mut [u8] {
        let at = base as usize + PREFIX_BYTES;
        // SAFETY: a test only asks for a header it wrote, so both reads are mapped.
        unsafe {
            let fixed = std::slice::from_raw_parts(self.ptr().add(at), HDR_PREFIX);
            let payloads = payload_count(fixed[OFF_FLAGS], WorkerSet(read_u64_le(fixed, OFF_TARGETS)));
            std::slice::from_raw_parts_mut(self.ptr().add(at), group_header_size(payloads))
        }
    }

    /// Flip one bit of the header at `base`.
    pub(crate) fn damage_header(&mut self, base: u64) {
        self.header_mut(base)[0] ^= 1;
    }
}

/// The group published at `base`, walked at the anchor's epoch.
pub(crate) fn group_at(log: SalLog, base: u64) -> SalMessage {
    let epoch = log.anchor().expect("the anchor verifies").0;
    match log.read_at(base, EpochGate::Walk(epoch)) {
        SalStep::Group(msg) => msg,
        _ => panic!("a group is published at offset {base}"),
    }
}
