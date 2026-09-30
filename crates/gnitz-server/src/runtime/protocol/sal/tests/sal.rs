use super::fixtures::{group_at, TestLog};
use super::{
    epoch_word, group_header_size, prefix_atomic, stamp_digest, DirectGroup, EpochGate, GroupTargets, SalLog,
    SalMessageKind, SalReader, SalStep, WorkerSet, ANCHOR_BYTES, CHECKPOINT_RESERVE, FLAG_IN_REQUEST_ORDER,
    FLAG_ZONE_END, HDR_PREFIX, MAX_WORKERS, OFF_FLAGS, OFF_KIND, OFF_LSN, PREFIX_BYTES, PRESENT,
};
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use crate::runtime::w2m::fixtures::sal_wake_seq;
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::wire::{decode_sal_slot, WireMsg};
use crate::test_support::sweep_bit_flips;
use gnitz_wire::control::CTRL_HEADER_SIZE;
use gnitz_wire::WireStatus;
use std::sync::atomic::Ordering;

/// A control-only group addressed to `target_id`.
fn to(target_id: u64) -> DirectGroup<'static> {
    DirectGroup {
        template: WireMsg { target_id, ..Default::default() },
        ..DirectGroup::new(SalMessageKind::DdlSync)
    }
}

// ---------------------------------------------------------------------------
// Framing: slots, the directory and the cursor
// ---------------------------------------------------------------------------

/// A group's slots read back as written at every width — empty slots
/// interleaved, odd sizes padded — and the next group starts where it ends.
#[test]
fn a_group_reads_back_its_slots_at_every_width() {
    for slots in [1usize, 2, 3, MAX_WORKERS] {
        let log = TestLog::new(1 << 20, 1, 1);
        let bufs: Vec<Vec<u8>> = (0..slots)
            .map(|w| if w % 2 == 1 { vec![] } else { vec![w as u8; 97 + w] })
            .collect();
        let payloads: Vec<&[u8]> = bufs.iter().map(Vec::as_slice).collect();
        log.write(7, 9, SalMessageKind::ScanSpec, &payloads);
        log.write(8, 0, SalMessageKind::ScanSpec, &payloads);

        let groups: Vec<_> = log.log().walk(1).collect();
        assert_eq!(groups.len(), 2, "{slots} slots");
        assert_eq!(groups[1].base, groups[0].end, "{slots} slots: the stride");
        assert_eq!(groups[1].end, log.cursor(), "{slots} slots: the writer's advance");
        for (g, header) in groups.iter().zip([(7, 9), (8, 0)]) {
            assert_eq!((g.target_id, g.lsn, g.slots() as usize), (header.0, header.1, slots));
            for (w, buf) in bufs.iter().enumerate() {
                let written = (!buf.is_empty()).then_some(buf.as_slice());
                assert_eq!(g.slot(w as u32), written, "{slots} slots: slot {w}");
            }
        }
    }
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

    let flush_slots = vec![&[0u8; CTRL_HEADER_SIZE][..]; MAX_WORKERS];
    log.try_write(0, 0, SalMessageKind::Flush, 0, &flush_slots)
        .expect("a MAX_WORKERS checkpoint round must still fit");

    log.excl().checkpoint_reset();
    assert!(!log.writer.needs_checkpoint(), "the reset clears the refusal");
}

/// `wal.sal` is never truncated, so a group can land on an offset a wider group
/// used before it. The narrow group's recorded slot count is what makes the
/// leftover entries unreachable.
#[test]
fn a_group_hides_the_slots_of_a_wider_group_at_the_same_offset() {
    let log = TestLog::new(1 << 20, 1, 1);
    // An 8-worker group leaves a fully populated directory at offset 0.
    let wide: Vec<Vec<u8>> = (0..8).map(|i| vec![0xA0 + i as u8; 64]).collect();
    let wide_refs: Vec<&[u8]> = wide.iter().map(|b| b.as_slice()).collect();
    log.write(0, 0, SalMessageKind::ScanSpec, &wide_refs);

    // The same offset rewritten by a 2-worker topology.
    log.writer.rewind(2);
    log.write(0, 0, SalMessageKind::ScanSpec, &[&[0x11u8; 32], &[0x22u8; 32]]);

    let msg = group_at(log.log(), 0);
    assert_eq!(msg.slots(), 2, "the group's own count is the narrow one");
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

/// A worker process parks on the SAL across a checkpoint reset, and reports each
/// group it wakes to back over its ring.
#[test]
fn sal_cross_process_checkpoint() {
    let log = TestLog::new(1 << 20, 1, 1);
    let ring = log.ring(0);

    let child = || {
        let reader = SalReader::new(log.log(), 0, 1);
        let writer = W2mWriter::new(ring);
        for _ in 0..2 {
            let msg = loop {
                writer.sal_park().park(|| reader.is_empty());
                if let Some((msg, _)) = reader.next() {
                    break msg;
                }
            };
            // Round 2 is written at cursor 0 of the next epoch.
            reader.rewind();
            writer.send_msg(
                0,
                &WireMsg {
                    target_id: msg.target_id,
                    ..Default::default()
                },
            );
        }
    };

    let pid = unsafe { fork_child(child) };

    let receiver = W2mReceiver::new(vec![ring]);
    let report = || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Some(slot) = receiver.try_read_slot(0) {
                return slot.control().hdr.target_id;
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
        excl.write(&to(round)).expect("group fits");
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

    assert!(WorkerSet::one(0).with(1).covers(2) && !WorkerSet::one(1).covers(2));
    assert!(WorkerSet::ALL.covers(4) && WorkerSet::ALL.covers(MAX_WORKERS));
    assert!(WorkerSet::EMPTY.covers(0) && !WorkerSet::EMPTY.covers(1));
}

/// A group leased to 2 of 4 workers writes only those slots, both answering on
/// the one request id, and is sized for exactly those slots.
#[test]
fn a_leased_group_writes_only_its_set_on_one_id() {
    let log = TestLog::new(1 << 20, 4, 1);
    let group = DirectGroup {
        targets: GroupTargets::Leased {
            set: WorkerSet::one(1).with(3),
            request_id: 40,
            in_request_order: true,
        },
        ..DirectGroup::new(SalMessageKind::ScanSpec)
    };
    log.writer.write(&group).expect("group fits");
    log.writer
        .write(&DirectGroup::new(SalMessageKind::ScanSpec))
        .expect("group fits");

    for w in 0..4u32 {
        let reader = SalReader::new(log.log(), w, 1);
        let mut read = std::iter::from_fn(|| reader.next().map(|(m, _)| (m.request_id, m.in_request_order)));
        if w == 1 || w == 3 {
            assert_eq!(read.next(), Some((40, true)), "worker {w} reads the leased group");
        }
        assert_eq!(read.next(), Some((0, false)), "worker {w} reads the unaddressed group");
        assert_eq!(read.next(), None, "worker {w}");
    }
}

/// Dropping a `SalExcl` wakes each worker a group written under it reached —
/// once, however many groups reached it — and no other.
#[test]
fn dropping_a_sal_excl_wakes_exactly_the_workers_it_reached() {
    let log = TestLog::new(1 << 20, 4, 1);
    let seqs = || (0..4).map(|w| unsafe { sal_wake_seq(log.ring(w)) }).collect::<Vec<_>>();
    let leased = |set| DirectGroup {
        targets: GroupTargets::Leased {
            set,
            request_id: 1,
            in_request_order: false,
        },
        ..DirectGroup::new(SalMessageKind::ScanSpec)
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
        .write(&DirectGroup::new(SalMessageKind::Shutdown))
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

/// A group carrying per-worker extras writes each slot its own blob in place
/// of the template's.
#[test]
fn a_group_with_per_worker_extras_writes_each_slot_its_own_blob() {
    let log = TestLog::new(1 << 20, 3, 1);
    let extras: Vec<Vec<u8>> = vec![vec![1; 40], Vec::new(), vec![3; 400]];
    let group = DirectGroup {
        template: WireMsg {
            target_id: 16,
            blob: &[9; 7],
            ..Default::default()
        },
        extras: Some(&extras),
        ..DirectGroup::new(SalMessageKind::ScanSpec)
    };
    log.writer.write(&group).expect("group fits");

    let msg = group_at(log.log(), 0);
    for (w, extra) in extras.iter().enumerate() {
        let slot = msg.slot(w as u32).expect("every worker is written");
        let decoded = decode_sal_slot(slot, |_, _| None).expect("a slot decodes");
        assert_eq!(decoded.blob, *extra, "worker {w}");
    }
}

// ---------------------------------------------------------------------------
// The group header's own integrity: a digest over the header, seeded with the
// group's byte offset. Every field swept below drives a routing or replay
// decision, so the digest is what stands between a damaged log and a wrong one.
// ---------------------------------------------------------------------------

/// `lsn`, the kind and flag bytes, `target_id`, `slot_count`, the epoch
/// word and every directory entry live inside the digested span; the digest field itself is excluded from
/// it but is what the verdict compares against. One bit anywhere in the header
/// must therefore fail the read.
#[test]
fn every_single_bit_flip_in_a_group_header_is_rejected() {
    let mut log = TestLog::new(1 << 20, 1, 1);
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

/// The digest is seeded with the group's byte offset, which makes a valid header
/// self-locating: a header that verified anywhere it was copied to would let a
/// walk read one group's header over another's bytes.
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
    let read_in =
        |len: usize| unsafe { SalLog::new(log.anchor_ptr(), ANCHOR_BYTES + len) }.read_at(0, EpochGate::Walk(1));

    let header = PREFIX_BYTES + group_header_size(1);
    assert!(
        matches!(read_in(PREFIX_BYTES + HDR_PREFIX - 1), SalStep::Corrupt),
        "the fixed part"
    );
    assert!(matches!(read_in(header - 1), SalStep::Corrupt), "the directory");
    assert!(
        matches!(read_in(header), SalStep::Absent),
        "the whole header, not the slot"
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
    stamp_digest(0, hdr);
}

/// Every scalar the group header carries survives `write_slots` →
/// `probe_header`, the top epoch included. A header is authenticated by a
/// digest over its whole span, so an encode/decode offset that drifted would
/// verify and hand back the wrong field value with no error anywhere.
#[test]
fn every_header_field_round_trips() {
    let log = TestLog::new(1 << 20, 3, u32::MAX);
    let payloads: [&[u8]; 3] = [&[0u8; 8], &[], &[0u8; 24]];
    log.try_write(
        0xABCD_1234,
        0x0102_0304_0506_0708,
        SalMessageKind::Backfill,
        FLAG_ZONE_END | FLAG_IN_REQUEST_ORDER,
        &payloads,
    )
    .expect("group fits");

    let hdr = log.log().probe_header(0).expect("the header verifies");
    assert_eq!(hdr.lsn, 0x0102_0304_0506_0708);
    assert_eq!(hdr.kind, SalMessageKind::Backfill);
    assert_eq!(hdr.flags, FLAG_ZONE_END | FLAG_IN_REQUEST_ORDER);
    assert_eq!(hdr.target_id, 0xABCD_1234);
    assert_eq!(hdr.epoch, u32::MAX);
    assert_eq!(
        super::dir_sizes(hdr.dir).collect::<Vec<_>>(),
        vec![8, 0, 24],
        "every directory entry, the empty slot included"
    );
    assert!(
        matches!(log.log().read_at(0, EpochGate::Live(u32::MAX)), SalStep::Group(_)),
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
        hdr[OFF_FLAGS] = 4;
    });
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt),
        "an unknown flag bit must read as corruption"
    );
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
