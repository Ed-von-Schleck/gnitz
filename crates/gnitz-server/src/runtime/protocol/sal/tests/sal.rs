use super::fixtures::{group_at, TestLog};
use super::{
    anchor_stores, decode_sal_frame, epoch_word, group_header_size, prefix_atomic, stamp_digest, Apply, DirectGroup,
    EpochGate, GroupData, GroupHead, GroupTargets, Read, SalLog, SalMessageKind, SalReader, SalRequest, SalStep,
    WorkerSet, ANCHOR_BYTES, ANCHOR_RECORD, CHECKPOINT_RESERVE, FLAG_IN_REQUEST_ORDER, FLAG_SHARED, FLAG_ZONE_END,
    HDR_PREFIX, KNOWN_FLAGS, MAX_WORKERS, OFF_FLAGS, OFF_KIND, OFF_LSN, OFF_TARGETS, OFF_WIDTH, PREFIX_BYTES, PRESENT,
};
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use crate::runtime::w2m::fixtures::test_rings;
use crate::runtime::wire::WireMsg;
use crate::test_support::{
    make_batch, make_batch_raw, make_schema_u64_i64, make_string_batch, sweep_bit_flips, weighted_rows,
};
use gnitz_wire::control::{peek_control_block, ControlHeader, CTRL_HEADER_SIZE};
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_wire::{ClientVerb, TypeCode, WireFlags, WireStatus};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{encode_schema_block, SchemaColumn, SchemaDescriptor};
use std::fs::File;
use std::sync::atomic::Ordering;

/// A control-only group addressed to `target_id`.
fn to(target_id: u64) -> DirectGroup<'static> {
    DirectGroup::new(Apply::DdlSync { family: target_id })
}

/// A read of `tid` under the blob `spec`.
pub(super) fn scan(tid: u64, spec: &[u8]) -> Read<'_> {
    Read::ScanSpec { tid, reply_layout: 0, spec: spec.into() }
}

// ---------------------------------------------------------------------------
// Framing: payloads, the directory and the cursor
// ---------------------------------------------------------------------------

/// A per-worker group gives each addressed worker its own bytes at every
/// width — unaddressed workers interleaved, odd sizes padded — and the next
/// group starts where it ends.
#[test]
fn a_per_worker_group_gives_each_addressed_worker_its_own_bytes() {
    for width in [1usize, 2, 3, MAX_WORKERS] {
        let log = TestLog::new(1 << 20, width, 1);
        let bufs: Vec<Vec<u8>> = (0..width)
            .map(|w| if w % 2 == 1 { vec![] } else { vec![w as u8; 97 + w] })
            .collect();
        let payloads: Vec<&[u8]> = bufs.iter().map(Vec::as_slice).collect();
        log.write(7, 9, SalMessageKind::ScanSpec, &payloads);
        log.write(8, 0, SalMessageKind::ScanSpec, &payloads);

        let groups: Vec<_> = log.log().walk(1).collect();
        assert_eq!(groups.len(), 2, "width {width}");
        assert_eq!(groups[1].base, groups[0].end, "width {width}: the stride");
        assert_eq!(groups[1].end, log.cursor(), "width {width}: the writer's advance");
        for (g, header) in groups.iter().zip([(7, 9), (8, 0)]) {
            assert_eq!((g.target_id, g.lsn, g.width() as usize), (header.0, header.1, width));
            assert_eq!(
                g.payloads().count(),
                width.div_ceil(2),
                "width {width}: one per addressed worker"
            );
            for (w, buf) in bufs.iter().enumerate() {
                let written = (!buf.is_empty()).then_some(buf.as_slice());
                assert_eq!(g.slot(w as u32), written, "width {width}: worker {w}");
            }
            assert_eq!(g.slot(width as u32), None, "width {width}: a worker past it");
        }
    }
}

/// A group whose every addressed worker is sent the same message holds it once:
/// one directory entry, the same bytes for each of them, none for the rest.
#[test]
fn a_shared_group_gives_every_addressed_worker_the_same_bytes() {
    let log = TestLog::new(1 << 20, 4, 1);
    let batch = make_batch(&make_schema_u64_i64(), &[(1, 1, 10), (2, 1, 20)]);
    let record = encode_schema_block(batch.schema());
    let push = |set| {
        DirectGroup::push(
            16,
            &record,
            GroupData::Same(batch.wire_whole()),
            GroupTargets { set, ..GroupTargets::UNADDRESSED },
        )
    };
    let excl = log.excl();
    excl.write(&push(WorkerSet::ALL)).expect("group fits");
    let second = log.cursor();
    excl.write(&push(WorkerSet::one(1).with(3))).expect("group fits");
    let third = log.cursor();
    excl.write(&push(WorkerSet::EMPTY)).expect("group fits");

    for (base, addressed) in [
        (0, [true; 4]),
        (second, [false, true, false, true]),
        (third, [false; 4]),
    ] {
        let msg = group_at(log.log(), base);
        let payloads: Vec<_> = msg.payloads().collect();
        assert_eq!(payloads.len(), 1, "one directory entry");
        assert_eq!(msg.width(), 4);
        for (w, addressed) in addressed.into_iter().enumerate() {
            assert_eq!(msg.slot(w as u32), addressed.then_some(payloads[0]), "worker {w}");
        }
    }
    assert_eq!(
        third - second,
        second,
        "a group is as long whichever workers share its payload"
    );
}

/// A group that does not fit is refused `SalFull` and leaves the log as it was.
/// Only one that fits an empty log calls for a checkpoint, and a terminal group
/// still fits once an ordinary one no longer does.
#[test]
fn a_group_that_does_not_fit_is_refused_sal_full() {
    let cap = 1024;
    let log = TestLog::new(PREFIX_BYTES + CHECKPOINT_RESERVE + cap, 1, 1);
    let refuse = |payload: usize| {
        let before = log.cursor();
        let err = log
            .try_write(0, 0, SalMessageKind::ScanSpec, 0, &[&vec![0; payload]])
            .expect_err("the group must be refused");
        assert_eq!(err.status, WireStatus::SalFull);
        assert_eq!(log.cursor(), before, "the log is untouched");
    };

    refuse(cap);
    assert!(!log.writer.needs_checkpoint(), "no checkpoint admits it");

    log.seek((cap - PREFIX_BYTES) as u64);
    refuse(8);
    assert!(log.writer.needs_checkpoint(), "a checkpoint admits it");

    log.try_write(0, 0, SalMessageKind::Flush, 0, &[&[0u8; CTRL_HEADER_SIZE]])
        .expect("a checkpoint round must still fit");

    log.excl().checkpoint_reset();
    assert!(!log.writer.needs_checkpoint(), "the reset clears the refusal");
}

/// `wal.sal` is never truncated, so a group can land on an offset a wider group
/// used before it. The set it addresses is what makes the leftover directory
/// entries unreachable.
#[test]
fn a_group_exposes_only_its_own_payloads_over_a_wider_groups_leftover() {
    let log = TestLog::new(1 << 20, 8, 1);
    // A group for all eight workers leaves a fully populated directory at offset 0.
    let wide: Vec<Vec<u8>> = (0..8).map(|i| vec![0xA0 + i as u8; 64]).collect();
    let wide_refs: Vec<&[u8]> = wide.iter().map(|b| b.as_slice()).collect();
    log.write(0, 0, SalMessageKind::ScanSpec, &wide_refs);

    // The same offset rewritten by a group for two of them.
    log.writer.rewind(2);
    log.write(0, 0, SalMessageKind::ScanSpec, &[&[0x11u8; 32], &[0x22u8; 32]]);

    let msg = group_at(log.log(), 0);
    assert_eq!(msg.payloads().count(), 2, "the group's own count is the narrow one");
    let lens: Vec<_> = (0..8).map(|w| msg.slot(w).map(<[u8]>::len)).collect();
    assert_eq!(lens, [Some(32), Some(32), None, None, None, None, None, None]);
}

// ---------------------------------------------------------------------------
// The publication scope
// ---------------------------------------------------------------------------

/// Nothing a scope lays out is visible until it commits; then all of it is, in
/// layout order: every zoned group at the scope's LSN, only the last marked
/// `ZONE_END`, and the unzoned ones — on either side of it — at LSN 0.
#[test]
fn a_scope_publishes_its_zone_at_commit() {
    let log = TestLog::new(1 << 20, 1, 1);
    let mut excl = log.excl();
    let scope = excl.begin("test");
    let mut bases = Vec::new();
    for (target, zoned) in [(200, true), (201, false), (202, true), (203, false)] {
        bases.push(log.cursor());
        scope.write(&to(target), zoned).expect("group fits");
    }
    for &base in &bases {
        assert!(matches!(log.log().read_at(base, EpochGate::Live(1)), SalStep::Absent));
    }

    let lsn = scope.lsn();
    assert!(scope.commit(), "the zone was open");
    let read: Vec<_> = bases
        .iter()
        .map(|&base| {
            let m = group_at(log.log(), base);
            (m.target_id, m.lsn, m.zone_end)
        })
        .collect();
    assert_eq!(
        read,
        [(200, lsn, false), (201, 0, false), (202, lsn, true), (203, 0, false)]
    );
}

/// Rolling back to a savepoint takes back every group laid out since — the
/// zone's last member included — so the scope still commits the zone before
/// it; a scope dropped uncommitted takes its whole span back.
#[test]
fn a_rolled_back_group_is_taken_back_and_the_rest_commits() {
    let log = TestLog::new(1 << 20, 1, 1);
    let mut excl = log.excl();
    let scope = excl.begin("test");
    scope.write(&to(1), true).expect("group fits");
    let savepoint = scope.savepoint();
    let taken_back = log.cursor();
    scope.write(&to(2), true).expect("group fits");
    scope.write(&to(3), false).expect("group fits");
    scope.roll_back(savepoint);
    assert_eq!(log.cursor(), taken_back, "the cursor is restored");
    assert!(scope.commit(), "the zone before the savepoint closes");

    assert!(group_at(log.log(), 0).zone_end, "the member left is the closing one");
    assert!(matches!(
        log.log().read_at(taken_back, EpochGate::Live(1)),
        SalStep::Absent
    ));

    let scope = excl.begin("test");
    scope.write(&to(4), true).expect("group fits");
    drop(scope);
    assert_eq!(log.cursor(), taken_back, "an uncommitted scope takes its span back");
    assert!(matches!(
        log.log().read_at(taken_back, EpochGate::Live(1)),
        SalStep::Absent
    ));
}

/// A scope's LSN rises with the log, across a checkpoint too, and the watermark
/// follows the last scope that published anything.
#[test]
fn zone_lsns_rise_with_the_log_and_the_watermark_follows_the_commits() {
    let log = TestLog::new(1 << 20, 1, 1);
    let mut excl = log.excl();
    excl.boot_rewind(1);
    let boot = log.writer.watermark();

    let empty = excl.begin("test");
    let first = empty.lsn();
    assert!(first > boot, "every LSN this boot is above the boot watermark");
    assert!(!empty.commit());
    assert_eq!(log.writer.watermark(), boot, "an empty scope publishes nothing");

    let stream = excl.begin("test");
    assert_eq!(stream.lsn(), first, "and leaves its LSN to the next scope");
    stream.write(&to(1), false).expect("group fits");
    assert!(!stream.commit(), "an unzoned group closes no zone");
    assert_eq!(log.writer.watermark(), first);
    assert_eq!(group_at(log.log(), 0).target_id, 1, "but it is published");

    let zoned = excl.begin("test");
    let second = zoned.lsn();
    assert!(second > first);
    zoned.write(&to(2), true).expect("group fits");
    assert!(zoned.commit());
    assert_eq!(log.writer.watermark(), second);

    excl.checkpoint_reset();
    assert!(excl.begin("test").lsn() > second, "a checkpoint keeps LSNs rising");
}

// ---------------------------------------------------------------------------
// Workers: who a group reaches, and the live drain that reads it
// ---------------------------------------------------------------------------

/// A reader steps past a group that ends its epoch into the next one on its
/// own: what follows in the old epoch is never read, and the next epoch's first
/// group is.
#[test]
fn a_reader_leaves_its_epoch_on_stepping_past_a_flush() {
    for round in [Apply::Flush, Apply::FlushEph { generation: 0 }] {
        let kind = SalRequest::from(round.clone()).kind();
        let log = TestLog::new(1 << 20, 2, 1);
        let reader = SalReader::new(log.log(), 0, 1);
        let read = || reader.next().map(|(m, _)| (m.kind, m.target_id));
        let mut excl = log.excl();
        excl.write(&to(1)).expect("group fits");
        excl.write(&DirectGroup::new(round)).expect("group fits");
        excl.write(&to(2)).expect("group fits");

        assert_eq!(read(), Some((SalMessageKind::DdlSync, 1)));
        assert_eq!(read(), Some((kind, 0)));
        assert_eq!(read(), None, "{kind:?}: nothing behind it is read");
        assert!(reader.is_empty(), "{kind:?}: the old epoch's groups are leftovers");

        excl.checkpoint_reset();
        excl.write(&to(3)).expect("group fits");
        assert_eq!(read(), Some((SalMessageKind::DdlSync, 3)), "{kind:?}: the next epoch");
    }
}

/// A worker process parks on the SAL across a checkpoint reset, and reports each
/// group it wakes to back over its ring.
#[test]
fn sal_cross_process_checkpoint() {
    let (mut writers, receiver) = test_rings(&[4096]);
    let mut writer = writers.pop().unwrap();
    let log = TestLog::new(1 << 20, 1, 1);

    let child = || {
        let reader = SalReader::new(log.log(), 0, 1);
        for _ in 0..2 {
            let slot = loop {
                log.parks().park(0).park(|| reader.is_empty());
                if let Some((_, slot)) = reader.next() {
                    break slot;
                }
            };
            let Ok(control) = peek_control_block(slot) else {
                panic!()
            };
            writer.send_msg(
                0,
                &WireMsg {
                    arg0: control.hdr.arg0,
                    ..Default::default()
                },
            );
        }
    };

    let pid = unsafe { fork_child(child) };

    let report = || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Some(slot) = receiver.try_read_slot(0) {
                return slot.control().hdr.arg0;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("the worker never reported");
    };

    for round in [1, 2] {
        let mut excl = log.excl();
        if round == 2 {
            excl.checkpoint_reset();
        }
        // Round 1 ends its epoch, so round 2 is read at cursor 0 of the next.
        excl.write(&DirectGroup::new(Apply::FlushEph { generation: round }))
            .expect("group fits");
        // Dropping the lock is the wake.
        drop(excl);
        assert_eq!(report(), round, "round {round}");
    }

    unsafe { assert_child_exited_ok(pid) };
}

/// Membership, the launched-worker bound and rank, at a small worker count and
/// at the full word.
#[test]
fn a_worker_set_bounds_to_the_launched_workers() {
    let s = WorkerSet::one(1).with(3).with(6);
    assert_eq!(s.len(), 3);
    assert!(s.contains(3) && !s.contains(2));
    assert_eq!(s.without(3), WorkerSet::one(1).with(6));
    assert_eq!(s.within(4), WorkerSet::one(1).with(3));
    assert_eq!(s.iter().collect::<Vec<_>>(), vec![1, 3, 6]);
    assert_eq!([1, 3, 6].map(|w| s.rank(w)), [0, 1, 2]);

    assert_eq!(WorkerSet::ALL.within(4).len(), 4);
    assert_eq!(WorkerSet::ALL.within(MAX_WORKERS), WorkerSet::ALL);
    assert_eq!(WorkerSet::EMPTY.len(), 0);
    assert_eq!(WorkerSet::ALL.rank(MAX_WORKERS - 1), MAX_WORKERS - 1);
    assert_eq!(WorkerSet::EMPTY.rank(7), 0);

    assert_eq!(WorkerSet::one(1).union(WorkerSet::one(3)), WorkerSet::one(1).with(3));
    assert_eq!(s.union(WorkerSet::EMPTY), s);
}

/// A group written to 2 of 4 workers is read by those two alone, both answering
/// on the one request id.
#[test]
fn a_group_is_read_by_the_workers_it_addresses_on_one_id() {
    let log = TestLog::new(1 << 20, 4, 1);
    let group = DirectGroup {
        targets: GroupTargets {
            set: WorkerSet::one(1).with(3),
            request_id: 40,
            in_request_order: true,
        },
        ..DirectGroup::new(scan(0, &[]))
    };
    let excl = log.excl();
    excl.write(&group).expect("group fits");
    excl.write(&DirectGroup::new(scan(0, &[]))).expect("group fits");

    for w in 0..4u32 {
        let reader = SalReader::new(log.log(), w, 1);
        let mut read = std::iter::from_fn(|| reader.next().map(|(m, _)| (m.request_id, m.in_request_order)));
        if w == 1 || w == 3 {
            assert_eq!(
                read.next(),
                Some((40, true)),
                "worker {w} reads the group written to it"
            );
        }
        assert_eq!(read.next(), Some((0, false)), "worker {w} reads the unaddressed group");
        assert_eq!(read.next(), None, "worker {w}");
    }
}

/// Every member of a cut reads one count, two cuts read two, and a worker a
/// cut's first member does not address reads the same count as one it does.
#[test]
fn every_worker_numbers_a_cut_alike() {
    let log = TestLog::new(1 << 20, 2, 1);
    let member = |set, request_id, in_request_order| DirectGroup {
        targets: GroupTargets { set, request_id, in_request_order },
        ..DirectGroup::new(scan(0, &[]))
    };
    let excl = log.excl();
    // One cut whose first member addresses worker 0 alone, then a cut of one.
    excl.write(&member(WorkerSet::one(0), 1, false)).expect("group fits");
    excl.write(&member(WorkerSet::ALL, 2, true)).expect("group fits");
    excl.write(&member(WorkerSet::ALL, 3, true)).expect("group fits");
    excl.write(&member(WorkerSet::ALL, 4, false)).expect("group fits");

    let cuts = |w| {
        let reader = SalReader::new(log.log(), w, 1);
        std::iter::from_fn(|| reader.next().map(|(m, _)| (m.request_id, reader.cut()))).collect::<Vec<_>>()
    };
    assert_eq!(cuts(0), [(1, 1), (2, 1), (3, 1), (4, 2)]);
    assert_eq!(cuts(1), [(2, 1), (3, 1), (4, 2)]);
}

/// Dropping a `SalExcl` wakes each worker a group written under it reached —
/// once, however many groups reached it — and no other.
#[test]
fn dropping_a_sal_excl_wakes_exactly_the_workers_it_reached() {
    let log = TestLog::new(1 << 20, 4, 1);
    let seqs = || (0..4).map(|w| log.parks().wake_seq(w)).collect::<Vec<_>>();
    let leased = |set| DirectGroup {
        targets: GroupTargets {
            set,
            request_id: 1,
            in_request_order: false,
        },
        ..DirectGroup::new(scan(0, &[]))
    };

    drop(log.excl());
    assert_eq!(seqs(), [0, 0, 0, 0], "nothing written, nothing woken");

    {
        let excl = log.excl();
        excl.write(&leased(WorkerSet::one(2))).expect("group fits");
        excl.write(&leased(WorkerSet::one(2).with(0))).expect("group fits");
        assert_eq!(seqs(), [0, 0, 0, 0], "no wake before the drop");
    }
    assert_eq!(seqs(), [1, 0, 1, 0], "a worker reached twice is woken once");

    log.excl()
        .write(&DirectGroup::new(SalRequest::Shutdown))
        .expect("group fits");
    assert_eq!(
        seqs(),
        [2, 1, 2, 1],
        "an unaddressed group reaches every launched worker"
    );
}

#[test]
fn a_reader_is_empty_only_while_no_group_is_readable_at_its_cursor() {
    let log = TestLog::new(1 << 20, 2, 1);
    let reader = SalReader::new(log.log(), 0, 1);
    assert!(reader.is_empty(), "an unwritten log");

    log.write(0, 0, SalMessageKind::ScanSpec, &[&[], &[0u8; 32]]);
    assert!(!reader.is_empty(), "another worker's unicast");
    assert!(reader.next().is_none(), "holds nothing for worker 0");
    assert!(reader.is_empty(), "and the cursor is past it");

    log.write(0, 0, SalMessageKind::ScanSpec, &[&[0u8; 32], &[]]);
    assert!(!reader.is_empty());
    assert!(reader.next().is_some());
    assert!(reader.is_empty());

    assert!(
        SalReader::new(log.log(), 0, 2).is_empty(),
        "an older epoch's group at the cursor"
    );
}

/// A group carrying per-worker extras sends each worker its own blob in place
/// of the request's.
#[test]
fn a_group_with_per_worker_extras_sends_each_worker_its_own_blob() {
    let log = TestLog::new(1 << 20, 3, 1);
    let extras: Vec<Vec<u8>> = vec![vec![1; 40], Vec::new(), vec![3; 400]];
    let group = DirectGroup {
        extras: Some(&extras),
        ..DirectGroup::new(scan(16, &[9; 7]))
    };
    log.excl().write(&group).expect("group fits");

    let msg = group_at(log.log(), 0);
    assert_eq!(msg.payloads().count(), extras.len(), "one payload per worker");
    for (w, extra) in extras.iter().enumerate() {
        let slot = msg.slot(w as u32).expect("every worker is written");
        let control = peek_control_block(slot).expect("a payload decodes");
        assert_eq!(slot[control.blob], **extra, "worker {w}");
    }
}

// ---------------------------------------------------------------------------
// The group header's own integrity: a digest over the header, which names the
// group's byte offset. Every field swept below drives a routing or replay
// decision, so the digest is what stands between a damaged log and a wrong one.
// ---------------------------------------------------------------------------

/// Every scalar field and every directory entry lives inside the digested span;
/// the digest field itself is excluded from it but is what the verdict compares
/// against. One bit anywhere in the header must therefore fail the read.
#[test]
fn every_single_bit_flip_in_a_group_header_is_rejected() {
    let mut log = TestLog::new(1 << 20, 3, 1);
    let buf = vec![0x5Au8; 64];
    log.write(42, 100, SalMessageKind::DdlSync, &[&buf, &[], &buf]);

    let view = log.log();
    assert!(matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Group(..)));

    let hdr = log.header_mut(0);
    let hdr_len = hdr.len();
    sweep_bit_flips(hdr, 0..hdr_len, |byte, bit, _| {
        assert!(
            matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
            "header byte {byte} bit {bit} must fail the digest"
        );
    });
}

/// A header names its own byte offset inside the digested span, which makes a
/// valid header self-locating: a header that verified anywhere it was copied to
/// would let a walk read one group's header over another's bytes.
#[test]
fn a_header_only_verifies_at_the_offset_it_was_published_at() {
    let mut log = TestLog::new(1 << 20, 1, 1);
    let buf = vec![0x11u8; 64];
    log.write(42, 100, SalMessageKind::ScanSpec, &[&buf]);
    let second = log.write(43, 101, SalMessageKind::ScanSpec, &[&buf]);

    // Group B's whole header over group A's — same epoch, same shape, a
    // different address.
    let b_hdr = log.header_mut(second).to_vec();
    log.header_mut(0).copy_from_slice(&b_hdr);
    assert!(
        matches!(log.log().read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "a header published elsewhere must not verify here"
    );
}

/// The probe bounds each header read by the mapping before making it. Views
/// that end one byte short of the fixed part and of the directory sit over a
/// digest-valid header, which a read past the view would verify and then find
/// the stride overrunning — `Absent` — so `Corrupt` is the bound's own verdict.
#[test]
fn a_probe_never_reads_past_the_end_of_the_mapping() {
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(0, 0, SalMessageKind::ScanSpec, &[&[0u8; 8]]);
    let read_in = |len: usize| SalLog { ring_len: len, ..log.log() }.read_at(0, EpochGate::Walk(1));

    let header = PREFIX_BYTES + group_header_size(1);
    assert!(
        matches!(read_in(PREFIX_BYTES + HDR_PREFIX - 1), SalStep::Corrupt),
        "the fixed part"
    );
    assert!(matches!(read_in(header - 1), SalStep::Corrupt), "the directory");
    assert!(
        matches!(read_in(header), SalStep::Absent),
        "the whole header, not the payload"
    );
}

// ---------------------------------------------------------------------------
// Header fields: every one round-trips, and a value this build does not know
// is corruption rather than a guess.
// ---------------------------------------------------------------------------

/// Re-stamp the digest of the group at offset 0 over its edited header, so the
/// decode below the digest is what a read exercises.
fn restamp_header(log: &mut TestLog, edit: impl FnOnce(&mut [u8])) {
    let hdr = log.header_mut(0);
    edit(hdr);
    stamp_digest(hdr);
}

/// Every scalar the group header carries survives the write and the read, the
/// top epoch and a nonzero base included. A header is authenticated by a
/// digest over its whole span, so an encode/decode offset that drifted would
/// verify and hand back the wrong field value with no error anywhere.
#[test]
fn every_header_field_round_trips() {
    let log = TestLog::new(1 << 20, 5, u32::MAX);
    let first = log.write(1, 0, SalMessageKind::ScanSpec, &[&[0u8; 8]]);
    let targets = WorkerSet::one(0).with(2).with(4);
    let head = |flags, request_id| GroupHead {
        lsn: 0x0102_0304_0506_0708,
        kind: SalMessageKind::Backfill,
        flags,
        request_id,
        target_id: 0xABCD_1234,
        targets,
    };
    let per_worker = log
        .writer
        .write_slots(
            head(FLAG_ZONE_END | FLAG_IN_REQUEST_ORDER, 0x0A0B_0C0D),
            &[8, 0, 24],
            |_, b| b.fill(0),
        )
        .expect("group fits");
    let shared = log
        .writer
        .write_slots(head(FLAG_SHARED, 0), &[40], |_, b| b.fill(0))
        .expect("group fits");
    log.writer.publish_range(per_worker, log.cursor());
    assert!(first < per_worker && per_worker < shared);

    let msg = group_at(log.log(), per_worker);
    assert_eq!(msg.lsn, 0x0102_0304_0506_0708);
    assert_eq!(msg.kind, SalMessageKind::Backfill);
    assert!(msg.zone_end && msg.in_request_order && !msg.shared);
    assert_eq!(msg.request_id, 0x0A0B_0C0D);
    assert_eq!(msg.target_id, 0xABCD_1234);
    assert_eq!((msg.base, msg.end), (per_worker, shared));
    assert_eq!((msg.width(), msg.targets), (5, targets));
    assert_eq!(
        msg.payloads().map(<[u8]>::len).collect::<Vec<_>>(),
        vec![8, 0, 24],
        "every directory entry, the empty payload included"
    );

    let msg = group_at(log.log(), shared);
    assert!(msg.shared && !msg.zone_end && !msg.in_request_order);
    assert_eq!((msg.width(), msg.targets, msg.request_id), (5, targets, 0));
    assert_eq!(msg.payloads().map(<[u8]>::len).collect::<Vec<_>>(), vec![40]);
    assert!(
        matches!(log.log().read_at(shared, EpochGate::Live(u32::MAX)), SalStep::Group(_)),
        "the prefix's epoch copy passes the live gate"
    );
}

/// A digest-valid header whose kind ordinal or flag bits name nothing this
/// build knows reads back `Corrupt`: it was written by another layout, and
/// guessing is not an option. The re-stamp is proven live by a known ordinal
/// first, so a `Corrupt` below cannot be the digest's verdict.
#[test]
fn an_unknown_ordinal_in_a_verified_header_is_corrupt() {
    let mut log = TestLog::new(1 << 20, 1, 1);
    log.write(0, 1, SalMessageKind::ScanSpec, &[&[0u8; 8]]);
    let view = log.log();

    restamp_header(&mut log, |hdr| hdr[OFF_KIND] = SalMessageKind::Tick.as_wire());
    assert_eq!(group_at(view, 0).kind, SalMessageKind::Tick, "the re-stamp verifies");

    let unknown = (0..=u8::MAX)
        .find(|&b| SalMessageKind::from_wire(b).is_none())
        .expect("fewer than 256 kinds");
    restamp_header(&mut log, |hdr| hdr[OFF_KIND] = unknown);
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "an unknown kind ordinal must read as corruption"
    );

    restamp_header(&mut log, |hdr| {
        hdr[OFF_KIND] = SalMessageKind::ScanSpec.as_wire();
        hdr[OFF_FLAGS] = FLAG_ZONE_END | FLAG_IN_REQUEST_ORDER;
    });
    assert!(group_at(view, 0).zone_end, "both known flags verify");

    restamp_header(&mut log, |hdr| gnitz_wire::write_u64_le(hdr, OFF_LSN, 0));
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "a zone end outside every zone must read as corruption"
    );

    restamp_header(&mut log, |hdr| {
        gnitz_wire::write_u64_le(hdr, OFF_LSN, 1);
        hdr[OFF_FLAGS] = KNOWN_FLAGS + 1;
    });
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "an unknown flag bit must read as corruption"
    );

    restamp_header(&mut log, |hdr| hdr[OFF_FLAGS] = 0);
    assert_eq!(group_at(view, 0).width(), 1, "the re-stamp verifies");
    restamp_header(&mut log, |hdr| hdr[OFF_WIDTH] = MAX_WORKERS as u8 + 1);
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "a width past the most workers a cluster runs must read as corruption"
    );

    // Shared, so the addressed set does not size the header.
    restamp_header(&mut log, |hdr| {
        hdr[OFF_WIDTH] = 1;
        hdr[OFF_FLAGS] = FLAG_SHARED;
    });
    assert!(group_at(view, 0).shared, "the re-stamp verifies");
    restamp_header(&mut log, |hdr| {
        gnitz_wire::write_u64_le(hdr, OFF_TARGETS, 0b11);
    });
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "a worker addressed past the group's width must read as corruption"
    );
}

// ---------------------------------------------------------------------------
// The anchor: a kill can follow any of a write's stores.
// ---------------------------------------------------------------------------

/// An anchor write started from each state a kill can leave resolves, after each
/// of its stores, to the word in force before it or to the new one — and to the
/// new one once all are made.
#[test]
fn an_anchor_write_resolves_to_the_old_word_or_the_new_after_every_store() {
    let sum = |w: u64| gnitz_wire::checksum(&w.to_le_bytes());
    let (older, old, new, stale) = (
        epoch_word(6, 0),
        epoch_word(7, 4096),
        epoch_word(7, 8192),
        epoch_word(9, 64),
    );
    // `[front, checksum, spare]`.
    let starts: [(&str, [u64; 3]); 5] = [
        ("the fresh file", [0, 0, 0]),
        ("the front verifies, the spare fresh", [old, sum(old), 0]),
        ("the front verifies, the spare older", [old, sum(old), older]),
        ("the front verifies, the spare stale", [old, sum(old), stale]),
        ("the front stale, the spare verifies", [stale, sum(old), old]),
    ];
    for (case, start) in starts {
        let log = TestLog::new(1 << 20, 1, 1);
        let record = log.log().anchor_record();
        let store = |at: usize, w: u64| unsafe { gnitz_wire::write_u64_le(&mut *record, at, w) };
        let lay = || start.into_iter().enumerate().for_each(|(i, w)| store(i * 8, w));

        lay();
        let in_force = log.log().anchor_word().unwrap_or_else(|| panic!("{case}: resolves"));
        assert_eq!(in_force, if start == [0; 3] { 0 } else { old }, "{case}");
        for (n, (at, value)) in anchor_stores(in_force, new).into_iter().enumerate() {
            store(at, value);
            let got = log.log().anchor_word();
            assert!(
                got == Some(in_force) || got == Some(new),
                "{case}: after store {n} the anchor reads {got:x?}"
            );
        }
        assert_eq!(log.log().anchor_word(), Some(new), "{case}: every store made");
        let done: [u8; ANCHOR_RECORD] = unsafe { *record };

        // The writer makes exactly those stores.
        lay();
        log.writer.write_anchor(7, 8192);
        assert_eq!(unsafe { *record }, done, "{case}: the writer's own stores");
    }
}

// ---------------------------------------------------------------------------
// The live drain's response to the two verdicts the digest can produce: a
// leftover parks, damage aborts.
// ---------------------------------------------------------------------------

/// The prefix gate compares an unauthenticated copy of the epoch, so a leftover
/// whose prefix epoch flipped up to the current one passes it with an entirely
/// intact older header. Only the header's own epoch rejects it — and it must
/// park, not abort: this is a leftover, not damage.
#[test]
fn the_live_path_parks_on_a_leftover_whose_prefix_epoch_was_raised() {
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(7, 11, SalMessageKind::ScanSpec, &[&[0u8; 32]]);
    // Raise only the prefix's epoch copy: 1 -> 2, header untouched.
    unsafe { prefix_atomic(log.ptr(), 0).store(epoch_word(2, PRESENT), Ordering::Relaxed) };
    let reader = SalReader::new(log.log(), 0, 2);
    assert!(
        reader.next().is_none(),
        "a previous epoch's group must park, whatever its prefix claims"
    );
}

/// A digest mismatch under a passing epoch gate can only be corruption, and the
/// live drain fail-stops rather than reading it as end-of-log.
#[test]
fn the_live_path_aborts_on_a_damaged_header() {
    let name = "the_live_path_aborts_on_a_damaged_header_internal";
    let out = crate::test_support::run_test_in_child(module_path!(), name, &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(134),
        "{name} must fail-stop (exit 134)\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&out.stdout)
    );
    // The code alone would be satisfied by any other fatal abort reached first.
    assert!(
        stderr.contains("is corrupt — the log is damaged"),
        "{name} aborted for the wrong reason\nstderr:\n{stderr}"
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn the_live_path_aborts_on_a_damaged_header_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let mut log = TestLog::new(1 << 20, 1, 1);
    log.write(7, 11, SalMessageKind::ScanSpec, &[&[0u8; 32]]);
    // A sanity read before the damage, so the abort below is the damage.
    let reader = SalReader::new(log.log(), 0, 1);
    assert!(reader.next().is_some());
    log.damage_header(0);

    let reader = SalReader::new(log.log(), 0, 1);
    let _ = reader.next();
    unreachable!("a damaged header on the live path must fail-stop, not park");
}

// ---------------------------------------------------------------------------
// The frame decode
// ---------------------------------------------------------------------------

/// `rest` addressed to relation `tid`, carrying its schema `record`, with the
/// caller's own header fields kept.
fn frame<'a>(tid: u64, record: &'a [u8], rest: WireMsg<'a>) -> WireMsg<'a> {
    WireMsg {
        target_id: tid,
        schema_block: Some(record),
        ..rest
    }
}

/// A SAL slot's rows, its record decoded.
fn sal_rows(wire: &[u8]) -> Result<Option<Batch>, String> {
    decode_sal_frame(wire, |_, _| None).map(|(_, rows)| rows)
}

/// Eight rows of unequal region strides: a 4-byte PK, then stride-4 and
/// stride-2 payloads.
fn padded_batch() -> Batch {
    let sd = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I16, false),
        ],
        &[0],
    );
    let mut bb = BatchBuilder::new(&sd);
    for i in 0..8u32 {
        bb.begin_row(i as u128, i as i64 + 1);
        bb.put_int(i as u128 * 10);
        bb.put_int(i as u128 * 3);
        bb.end_row();
    }
    bb.finish()
}

/// Every frame shape decodes to what was sent, and no proper prefix of it
/// decodes.
#[test]
fn every_frame_shape_round_trips_and_no_prefix_decodes() {
    let fixed = make_schema_u64_i64();
    let consolidated = make_batch(&fixed, &[(1, 2, -5), (2, -1, 7), (9, 3, 0)]);
    let empty = Batch::empty_with_schema(&fixed);
    let raw = make_batch_raw(
        &fixed,
        &(0..8).map(|i| (7 - i, i as i64 + 1, i as i64 * 10)).collect::<Vec<_>>(),
    );
    let long = b"a string long enough to leave the inline prefix".as_slice();
    let strings = make_string_batch(&[(1, 1, b"inline".as_slice()), (2, -2, long), (3, 1, long)]);
    let padded = padded_batch();
    let row_width = (raw.wire_whole().unwrap().byte_size() - WAL_HEADER_SIZE) / raw.len();
    let range = raw.wire_rows_within(2, WAL_HEADER_SIZE + 3 * row_width).unwrap();
    assert_eq!(range.rows(), 3, "a budget of three rows frames three");
    let whole = |b: &Batch| (0..b.len()).collect::<Vec<_>>();

    let fixed_rel = (5, encode_schema_block(&fixed));
    let string_rel = (6, encode_schema_block(strings.schema()));
    let padded_rel = (7, encode_schema_block(padded.schema()));
    // Each message beside the source batch and the rows of it the message sends,
    // in the order it sends them.
    let shapes = [
        (
            None,
            WireMsg {
                target_id: 0xDEAD,
                flags: WireFlags {
                    verb: ClientVerb::PushTxn,
                    continuation: true,
                    ..Default::default()
                },
                arg0: 0x1111_2222_3333_4444,
                arg1: 0x5555,
                ..Default::default()
            },
            None,
        ),
        (Some(&fixed_rel), WireMsg::default(), None),
        (
            Some(&fixed_rel),
            WireMsg {
                data: consolidated.wire_whole(),
                ..Default::default()
            },
            Some((&consolidated, whole(&consolidated))),
        ),
        (
            Some(&fixed_rel),
            WireMsg {
                data: empty.wire_whole(),
                ..Default::default()
            },
            None,
        ),
        (
            Some(&string_rel),
            WireMsg {
                data: strings.wire_whole(),
                ..Default::default()
            },
            Some((&strings, whole(&strings))),
        ),
        (
            Some(&fixed_rel),
            WireMsg {
                flags: WireFlags::train_frame(true),
                data: Some(range),
                ..Default::default()
            },
            Some((&raw, vec![2, 3, 4])),
        ),
        (
            Some(&padded_rel),
            WireMsg {
                data: padded.wire_listed(&[6, 1, 3]),
                ..Default::default()
            },
            Some((&padded, vec![6, 1, 3])),
        ),
        (
            None,
            WireMsg {
                target_id: 7,
                status: WireStatus::Error,
                blob: b"something went wrong",
                ..Default::default()
            },
            None,
        ),
    ];

    for (shape, (rel, msg, sent)) in shapes.into_iter().enumerate() {
        let msg = rel.map_or(msg, |(tid, record)| frame(*tid, record, msg));
        let wire = msg.encode_to_vec();
        let control = peek_control_block(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        let rows = sal_rows(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        let hdr = ControlHeader {
            status: msg.status,
            target_id: msg.target_id,
            flags: msg.flags,
            arg0: msg.arg0,
            arg1: msg.arg1,
        };
        assert_eq!(control.hdr, hdr, "shape {shape}");
        assert_eq!(&wire[control.blob], msg.blob, "shape {shape}");
        assert_eq!(control.schema.map(|r| &wire[r]), msg.schema_block, "shape {shape}");
        match (sent, rows) {
            (None, None) => {}
            (Some((src, rows)), Some(got)) => {
                let all = weighted_rows(src);
                let want: Vec<_> = rows.iter().map(|&i| all[i].clone()).collect();
                assert_eq!(weighted_rows(&got), want, "shape {shape}");
            }
            (want, got) => panic!(
                "shape {shape}: sent rows {}, decoded rows {}",
                want.is_some(),
                got.is_some()
            ),
        }
        for cut in 0..wire.len() {
            assert!(
                sal_rows(&wire[..cut]).is_err(),
                "shape {shape}: a {cut}/{}-byte prefix decodes",
                wire.len()
            );
        }
    }
}

/// A slot's rows are laid out under the schema `known` answers for its target
/// id and record, else under the record itself — and need a record.
#[test]
fn a_sal_slot_lays_its_rows_out_under_the_known_schema_or_its_record() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let junk = [0xFFu8; 12];
    let wire = WireMsg {
        target_id: 77,
        schema_block: Some(&junk),
        data: batch.wire_whole(),
        ..Default::default()
    }
    .encode_to_vec();
    let got = decode_sal_frame(&wire, |tid, record| {
        assert_eq!((tid, record), (77, junk.as_slice()));
        Some(sd)
    })
    .expect("the known schema lays the rows out")
    .1
    .expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));
    assert!(sal_rows(&wire).is_err(), "the junk record itself does not decode");

    let record = encode_schema_block(&sd);
    let framed = frame(
        77,
        &record,
        WireMsg {
            data: batch.wire_whole(),
            ..Default::default()
        },
    )
    .encode_to_vec();
    let got = sal_rows(&framed).expect("the record decodes").expect("rows");
    assert_eq!(weighted_rows(&got), weighted_rows(&batch));

    let bare = WireMsg {
        target_id: 77,
        data: batch.wire_whole(),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        sal_rows(&bare).err().as_deref(),
        Some("a data block without a schema block")
    );
}

/// The file reaches `len` before the mapping exists, so the store lands on a
/// real page instead of raising SIGBUS; a remap keeps what the file holds, and a
/// smaller one does not shrink it.
#[test]
fn mapping_extends_the_file_and_never_shrinks_it() {
    let file: &'static File = Box::leak(Box::new(tempfile::tempfile().unwrap()));
    let len = || file.metadata().unwrap().len();
    let log = SalLog::map(file, ANCHOR_BYTES + 8192).unwrap();
    assert_eq!(len(), (ANCHOR_BYTES + 8192) as u64);
    unsafe { log.ring.add(4096).write(42) };
    let log = SalLog::map(file, ANCHOR_BYTES + 4097).unwrap();
    assert_eq!(len(), (ANCHOR_BYTES + 8192) as u64);
    assert_eq!(unsafe { log.ring.add(4096).read() }, 42);
}
