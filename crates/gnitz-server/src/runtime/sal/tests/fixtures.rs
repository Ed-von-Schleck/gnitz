//! The SAL fixture: a log over its own shared region, written by the
//! production writer, plus the reader every assertion goes through.
//!
//! A child of `sal`, so it reaches `write_slots` and the cursor directly: the
//! raw writes below forge what no scope writes — arbitrary LSNs, an open zone —
//! and every other group goes through [`SalExcl`].

use super::*;
use crate::catalog::SysFamily;
use crate::runtime::test_support::try_poll_once;

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
    /// Every worker's park: where the writer's wakes land.
    pub(crate) fn parks(&self) -> WorkerParks {
        self.parks
    }

    /// The ring's first byte — offset 0 of every group base.
    pub(crate) fn ptr(&self) -> *mut u8 {
        self.log.ring
    }

    /// The anchor page's first byte, the start of the whole file.
    pub(crate) fn anchor_ptr(&self) -> *mut u8 {
        self.log.ring.wrapping_sub(ANCHOR_BYTES)
    }

    pub(crate) fn log(&self) -> SalLog {
        self.log
    }

    pub(crate) fn cursor(&self) -> u64 {
        self.write_cursor.get()
    }

    /// Sole write access; nothing else in a test holds it.
    pub(crate) fn excl(&self) -> SalExcl<'_> {
        try_poll_once(self.lock()).expect("uncontended")
    }

    /// Place the write cursor by hand.
    pub(crate) fn seek(&self, cursor: u64) {
        self.write_cursor.set(cursor);
    }

    /// Append one group sending worker `w` the bytes `payloads[w]` verbatim, an
    /// empty one being a worker the group does not address; returns its base.
    pub(super) fn write(&self, target: u64, lsn: u64, kind: SalMessageKind, payloads: &[&[u8]]) -> u64 {
        self.try_write(target, lsn, kind, payloads).expect("group fits")
    }

    /// [`Self::write`] with the writer's verdict reported.
    pub(super) fn try_write(
        &self,
        target: u64,
        lsn: u64,
        kind: SalMessageKind,
        payloads: &[&[u8]],
    ) -> Result<u64, WireFault> {
        let written = || payloads.iter().enumerate().filter(|(_, p)| !p.is_empty());
        let sizes: Vec<usize> = written().map(|(_, p)| p.len()).collect();
        let head = GroupHead {
            lsn,
            kind,
            flags: 0,
            width: self.parks.len() as u8,
            reply: None,
            target_id: target,
            targets: written().map(|(w, _)| w).collect(),
        };
        let mut bytes = written().map(|(_, p)| *p);
        let base = self.write_slots(head, &sizes, |slot| {
            slot.copy_from_slice(bytes.next().expect("one payload per size"))
        })?;
        self.publish(base);
        Ok(base)
    }

    /// One committed zone of `groups`: its LSN and every member's base.
    pub(crate) fn commit_zone(&self, groups: &[SalGroup]) -> (u64, Vec<u64>) {
        let mut excl = self.excl();
        let mut scope = excl.begin("test");
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

    /// One committed zone of `DdlSync` groups, as a DDL's broadcasts write it.
    pub(crate) fn ddl_zone(&self, groups: &[(SysFamily, &Batch)]) {
        let groups: Vec<SalGroup> = groups
            .iter()
            .map(|&(family, batch)| SalGroup::ddl_sync(family, batch))
            .collect();
        self.commit_zone(&groups);
    }

    /// The header of the group at `base`, sized by its own payload count.
    pub(crate) fn header_mut(&mut self, base: u64) -> &mut [u8] {
        // SAFETY: a test only asks for a header it wrote, and `&mut self` holds
        // every other reference off.
        unsafe { self.log.header_mut(base) }
    }

    /// Flip one bit of the header at `base`.
    pub(crate) fn damage_header(&mut self, base: u64) {
        self.header_mut(base)[0] ^= 1;
    }
}

/// The group published at `base`, walked at the anchor's epoch.
pub(crate) fn group_at(log: SalLog, base: u64) -> SalMessage {
    let epoch = log.anchor().expect("the anchor verifies");
    match log.read_at(base, EpochGate::Walk(epoch)) {
        SalStep::Group(msg) => msg,
        _ => panic!("a group is published at offset {base}"),
    }
}

/// The targets of a push of `data` nothing answers: the workers that hold its rows.
pub(crate) fn holders_of(data: &GroupData) -> GroupTargets {
    GroupTargets {
        set: data.holders(),
        ..GroupTargets::SILENT
    }
}
