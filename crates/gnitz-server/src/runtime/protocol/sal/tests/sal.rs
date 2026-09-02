use super::fixtures::{group_and_next, group_at, TestLog};
use super::{
    effective_max, group_digest, group_header_size, group_total_size, pack_prefix, DirectGroup, EpochGate, GroupData,
    SalMessageKind, SalStep, CHECKPOINT_RESERVE, MIN_SAL_BYTES, OFF_DIGEST, OFF_KIND, OFF_ZONE_START, PREFIX_BYTES,
    SENTINEL_SIZE,
};
use crate::runtime::m2w::{self, Wake};
use crate::runtime::test_support::assert_child_exited_ok;
use crate::test_support::{make_batch_raw, sweep_bit_flips};
use gnitz_wire::align8;
use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;
use gnitz_wire::MAX_WORKERS;
use std::sync::atomic::{AtomicU64, Ordering};

/// The publication prefix word at offset 0.
fn prefix_word(log: &TestLog) -> u64 {
    unsafe { AtomicU64::from_ptr(log.ptr().cast()).load(Ordering::Acquire) }
}

#[test]
fn sal_round_trip() {
    let log = TestLog::new(1 << 20, 1, 1);
    let bufs: Vec<Vec<u8>> = vec![vec![0xAA; 100], vec![], vec![0xBB; 200], vec![0xCC; 50]];
    let payloads: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
    log.write(42, 100, SalMessageKind::Scan, &payloads);

    let (msg, next) = group_and_next(log.log(), 0, 1);
    assert_eq!(msg.lsn, 100);
    assert_eq!(msg.target_id, 42);
    assert_eq!(next, log.cursor());
    for (w, buf) in bufs.iter().enumerate() {
        let slot = msg.slot(w as u32);
        if buf.is_empty() {
            assert!(slot.is_none(), "no data slot for worker {w}");
        } else {
            assert_eq!(slot.expect("data slot"), buf.as_slice());
        }
    }
}

#[test]
fn sal_multiple_groups() {
    let log = TestLog::new(1 << 20, 1, 1);
    for g in 0..3u64 {
        let buf = vec![(g + 1) as u8; 64];
        log.write(g as u32, g * 10, SalMessageKind::Scan, &[&buf, &[]]);
    }

    let mut rc = 0u64;
    for g in 0..3u64 {
        let (msg, next) = group_and_next(log.log(), rc, 1);
        assert_eq!(msg.lsn, g * 10);
        assert_eq!(msg.target_id, g as u32);
        assert_eq!(msg.slot(0).expect("data slot"), vec![(g + 1) as u8; 64].as_slice());
        rc = next;
    }
    assert!(matches!(log.log().read_at(rc, EpochGate::Walk(1)), SalStep::Absent));
}

#[test]
fn sal_full_error() {
    // Sized off the ordinary cap, not off the raw mapping: with a bare 256-byte
    // region the reserve alone would refuse the group and the size arithmetic
    // this test exists for would never run.
    let payload = 256usize;
    let log = TestLog::new(SENTINEL_SIZE + CHECKPOINT_RESERVE + payload, 1, 1);
    let buf = vec![0xFF; payload];
    assert!(
        log.try_write(0, 0, SalMessageKind::Scan, false, &[&buf]).is_err(),
        "a group overrunning the ordinary cap must be refused"
    );
}

#[test]
fn sal_checkpoint_reset() {
    // Small: the whole region is zeroed below, and that memset is the test.
    let log = TestLog::new(128 << 10, 1, 1);
    log.write(0, 0, SalMessageKind::Scan, &[&[0x11u8; 32]]);

    unsafe { std::ptr::write_bytes(log.ptr(), 0, log.size) };
    log.seek(0, 2);
    log.write(0, 0, SalMessageKind::Scan, &[&[0x22u8; 32]]);

    let msg = group_at(log.log(), 0);
    assert_eq!(log.log().walk_epoch(), 2);
    assert_eq!(msg.slot(0).expect("data slot"), vec![0x22u8; 32].as_slice());
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
    log.write(0, 0, SalMessageKind::Scan, &wide_refs);

    // The same offset rewritten by a 2-worker topology.
    log.seek(0, 2);
    log.write(0, 0, SalMessageKind::Scan, &[&[0x11u8; 32], &[0x22u8; 32]]);

    let msg = group_at(log.log(), 0);
    assert_eq!(msg.slots(), 2, "the group's own count is the narrow one");
    for w in 0..8u32 {
        match msg.slot(w) {
            Some(bytes) if w < 2 => assert_eq!(bytes.len(), 32, "slot {w} is the narrow group's"),
            Some(_) => panic!("slot {w} was never written by this group"),
            None => assert!(w >= 2, "slot {w} must be readable"),
        }
    }
}

/// A group's slot count is the writer's own worker count, which the server
/// bounds at startup — so a wider one is a logic fault, not an input.
#[test]
#[should_panic(expected = "MAX_WORKERS")]
fn a_group_wider_than_max_workers_is_a_logic_fault() {
    let log = TestLog::new(1 << 20, 1, 1);
    let payloads = vec![&[][..]; MAX_WORKERS + 1];
    let _ = log.try_write(0, 0, SalMessageKind::Scan, false, &payloads);
}

/// The worst-case footprint of a terminal group: a `MAX_WORKERS` broadcast whose
/// every slot is a bare control block. All three emitters — `sync_round`,
/// `shutdown_workers` and `write_checkpoint_group` — carry neither a schema block
/// nor data.
fn worst_case_terminal_group() -> usize {
    group_total_size(
        group_header_size(MAX_WORKERS),
        std::iter::repeat_n(CTRL_BLOCK_SIZE_NO_BLOB as u32, MAX_WORKERS),
    )
}

#[test]
fn effective_max_reserves_by_kind() {
    let mmap = 1usize << 30;
    assert_eq!(
        effective_max(SalMessageKind::Scan, mmap),
        mmap - SENTINEL_SIZE - CHECKPOINT_RESERVE
    );
    for terminal in [
        SalMessageKind::Flush,
        SalMessageKind::FlushEph,
        SalMessageKind::Shutdown,
    ] {
        assert_eq!(
            effective_max(terminal, mmap),
            mmap - SENTINEL_SIZE,
            "a terminal group may spend the checkpoint reserve"
        );
    }
    assert_eq!(
        effective_max(SalMessageKind::ZoneCommit, mmap),
        mmap - CHECKPOINT_RESERVE,
        "a sentinel may spend the sentinel headroom but not the reserve"
    );
}

#[test]
fn checkpoint_reserve_holds_two_terminal_groups() {
    // Two, not one: the watchdog's crash arm broadcasts a shutdown without the
    // SAL mutex exactly while a committer flush round is parked awaiting the dead
    // worker's ACK, so both can land in the reserve band. Derived from the
    // constants so a wider control block or MAX_WORKERS trips here rather than in
    // production.
    let terminal = worst_case_terminal_group();
    assert!(
        CHECKPOINT_RESERVE >= 2 * terminal + SENTINEL_SIZE,
        "CHECKPOINT_RESERVE ({CHECKPOINT_RESERVE}) must cover two {terminal}-byte terminal groups plus a sentinel"
    );
}

#[test]
fn ordinary_cap_at_the_sal_floor_still_admits_a_group() {
    // The floor is the smallest configurable mapping; the reserve must leave the
    // overwhelming majority of it to ordinary groups.
    let cap = effective_max(SalMessageKind::Scan, MIN_SAL_BYTES);
    assert!(
        cap > MIN_SAL_BYTES - MIN_SAL_BYTES / 64,
        "the reserve must not eat the SAL floor: cap {cap} of {MIN_SAL_BYTES}"
    );
}

#[test]
fn terminal_and_sentinel_fit_where_an_ordinary_group_does_not() {
    // With the cursor just under the ordinary cap, the log still admits a
    // full-width checkpoint round and a zone-closing sentinel.
    let size = MIN_SAL_BYTES;
    let log = TestLog::new(size, 1, 1);
    let cursor = effective_max(SalMessageKind::Scan, size) as u64 - PREFIX_BYTES as u64;

    log.seek(cursor, 1);
    assert!(
        log.try_write(0, 0, SalMessageKind::Scan, false, &[&[]]).is_err(),
        "an ordinary group must be refused once the cursor passes the ordinary cap"
    );

    log.seek(cursor, 1);
    log.sentinel(0);

    log.seek(cursor, 1);
    let flush_slots = vec![&[0u8; CTRL_BLOCK_SIZE_NO_BLOB][..]; MAX_WORKERS];
    log.try_write(0, 0, SalMessageKind::Flush, false, &flush_slots)
        .expect("a MAX_WORKERS checkpoint round must still fit");
}

/// Each group's epoch is read from its own header, so two groups at consecutive
/// offsets under different epochs read back as they were written — a shape no
/// rewind can produce, and the reason the fixture can place the cursor by hand.
#[test]
fn sal_epoch_fence() {
    let log = TestLog::new(1 << 20, 1, 5);
    let buf = vec![0x33u8; 32];
    log.write(0, 0, SalMessageKind::Scan, &[&buf]);
    let second = log.cursor();
    log.seek(second, 6);
    log.write(0, 0, SalMessageKind::Scan, &[&buf]);

    let view = log.log();
    group_and_next(view, 0, 5);
    group_and_next(view, second, 6);
    assert!(matches!(view.read_at(second, EpochGate::Walk(5)), SalStep::Absent));
}

/// Two groups at lsn=K, a sentinel at lsn=K, then a group at lsn=K+1: on
/// read-back the sentinel must be the only [`SalMessageKind::ZoneCommit`].
/// Framing only; the zone rule is `sal::zone`'s.
#[test]
fn commit_sentinel_round_trip() {
    let log = TestLog::new(1 << 20, 1, 1);
    let buf = vec![0xAAu8; 32];
    log.write(100, 7, SalMessageKind::Scan, &[&buf, &buf]);
    log.write(101, 7, SalMessageKind::Scan, &[&buf, &buf]);
    log.sentinel(7);
    log.write(102, 8, SalMessageKind::Scan, &[&buf, &buf]);

    let view = log.log();
    let mut cursor = 0u64;
    let mut seen = Vec::new();
    while let SalStep::Group(msg, next) = view.read_at(cursor, EpochGate::Walk(1)) {
        seen.push((msg.lsn, msg.kind));
        cursor = next;
    }
    assert_eq!(
        seen,
        vec![
            (7, SalMessageKind::Scan),
            (7, SalMessageKind::Scan),
            (7, SalMessageKind::ZoneCommit),
            (8, SalMessageKind::Scan),
        ]
    );
}

/// A zone as the committer writes one: two broadcast groups and a closing
/// sentinel at a single LSN. From *every* worker's slot all three read back that
/// LSN, the caller's own kind survives, only the first is marked as the zone's
/// start, and only the sentinel is a `ZoneCommit` and carries no payload.
#[test]
fn zone_two_groups_one_sentinel() {
    let log = TestLog::new(1 << 20, 1, 1);
    let nw = 4u32;
    let zone_lsn = 17u64;

    let buf_a = vec![0xC0u8; 64];
    let a: Vec<&[u8]> = (0..nw).map(|_| buf_a.as_slice()).collect();
    let b1 = log
        .try_write(200, zone_lsn, SalMessageKind::Push, true, &a)
        .expect("group fits");

    let buf_b = vec![0x7Au8; 96];
    let b: Vec<&[u8]> = (0..nw).map(|_| buf_b.as_slice()).collect();
    let b2 = log.write(201, zone_lsn, SalMessageKind::Push, &b);

    let b3 = log.sentinel(zone_lsn);

    let view = log.log();
    let (m1, m2, m3) = (group_at(view, b1), group_at(view, b2), group_at(view, b3));
    assert_eq!((m1.lsn, m2.lsn, m3.lsn), (zone_lsn, zone_lsn, zone_lsn));
    assert_eq!((m1.target_id, m2.target_id), (200, 201));
    assert_eq!((m1.kind, m2.kind), (SalMessageKind::Push, SalMessageKind::Push));
    assert_eq!((m1.zone_start, m2.zone_start, m3.zone_start), (true, false, false));
    assert_eq!(m3.kind, SalMessageKind::ZoneCommit);
    for w in 0..nw {
        assert!(m1.slot(w).is_some(), "first group has data for worker {w}");
        assert!(m2.slot(w).is_some(), "second group has data for worker {w}");
        assert!(m3.slot(w).is_none(), "sentinel carries no payload");
    }
}

#[test]
fn sal_cross_process_checkpoint() {
    let log = TestLog::new(1 << 20, 1, 1);
    let efd = m2w::eventfd_create().unwrap();
    let efd2 = m2w::eventfd_create().unwrap();
    assert!(efd >= 0 && efd2 >= 0);

    // Both payloads exist before the fork, so the child allocates nothing.
    let buf = vec![0xAAu8; 64];
    let buf2 = vec![0xBBu8; 64];

    let pid = unsafe { libc::fork() };
    if pid == 0 {
        log.try_write(0, 10, SalMessageKind::Scan, false, &[&buf])
            .expect("group fits");
        m2w::eventfd_signal(efd);

        // Wait for the parent to finish reading round 1 before zeroing the
        // region under it.
        if m2w::eventfd_wait(efd2, 5000) != Wake::Signalled {
            unsafe { libc::_exit(1) };
        }

        unsafe { std::ptr::write_bytes(log.ptr(), 0, log.size) };
        log.seek(0, 2);
        log.try_write(0, 20, SalMessageKind::Scan, false, &[&buf2])
            .expect("group fits");
        m2w::eventfd_signal(efd);
        unsafe { libc::_exit(0) };
    }

    assert_eq!(m2w::eventfd_wait(efd, 5000), Wake::Signalled, "round 1 never signalled");
    let round1 = group_and_next(log.log(), 0, 1).0;
    assert_eq!(round1.lsn, 10);
    assert_eq!(round1.target_id, 0);
    assert_eq!(round1.slot(0).expect("data slot"), buf.as_slice());
    m2w::eventfd_signal(efd2);

    assert_eq!(m2w::eventfd_wait(efd, 5000), Wake::Signalled, "round 2 never signalled");
    assert_eq!(group_and_next(log.log(), 0, 2).0.lsn, 20);

    unsafe { assert_child_exited_ok(pid) };
    unsafe {
        libc::close(efd);
        libc::close(efd2);
    }
}

/// The live drain's epoch gate reads the group's `(epoch << 32 | payload_size)`
/// prefix BEFORE any header byte. A mismatched expectation parks the reader; a
/// matching one reads the group normally.
#[test]
fn sal_prefix_epoch_gate() {
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(7, 11, SalMessageKind::Scan, &[&[0x5Au8; 48]]);

    let view = log.log();
    assert!(
        matches!(view.read_at(0, EpochGate::Live(2)), SalStep::Absent),
        "an epoch-mismatched slot must park the reader"
    );
    let SalStep::Group(msg, cursor) = view.read_at(0, EpochGate::Live(1)) else {
        panic!("a matching epoch reads the group");
    };
    assert_eq!((msg.lsn, msg.target_id), (11, 7));
    assert_eq!(cursor, log.cursor());
}

/// The `(epoch << 32 | payload_size)` prefix word must round-trip at the
/// boundaries: a one-slot group with no data (payload_size == its header size)
/// at epoch `u32::MAX`, and a multi-MiB group at epoch 1.
#[test]
fn sal_prefix_packing_boundaries() {
    // The multi-MiB group below plus the reserve; nothing here needs more.
    let log = TestLog::new(2 << 20, 1, u32::MAX);

    log.write(0, 0, SalMessageKind::Scan, &[&[]]);
    assert_eq!(prefix_word(&log), pack_prefix(u32::MAX, group_header_size(1)));
    let (_, next) = group_and_next(log.log(), 0, u32::MAX);
    assert_eq!(next, (PREFIX_BYTES + group_header_size(1)) as u64);
    assert_eq!(next, log.cursor());

    // A payload past 2^20, epoch 1: the low half of the prefix word is the one
    // that would truncate. Only the first group's bytes need clearing — the one
    // written over them is strictly larger.
    unsafe { std::ptr::write_bytes(log.ptr(), 0, PREFIX_BYTES + group_header_size(1)) };
    log.seek(0, 1);
    let big = vec![0xEEu8; 1 << 20];
    log.write(0, 0, SalMessageKind::Scan, &[&big]);
    let expected_payload = group_header_size(1) + (1 << 20); // already 8-aligned
    assert_eq!(prefix_word(&log), pack_prefix(1, expected_payload));
    let (msg, next) = group_and_next(log.log(), 0, 1);
    assert_eq!(next, (PREFIX_BYTES + expected_payload) as u64);
    assert_eq!(msg.slot(0).expect("data slot").len(), 1 << 20);
}

// ---------------------------------------------------------------------------
// The group directory: sizes only, and the alignment every base rests on.
// ---------------------------------------------------------------------------

/// Every group base is 8-aligned, which the resync scan's `.step_by(8)` and the
/// naturally-aligned publication store both rest on. An odd slot count is where
/// a directory sized in 4-byte units would lose it.
#[test]
fn every_group_header_size_is_8_aligned() {
    for slots in 0..=MAX_WORKERS {
        assert_eq!(
            group_header_size(slots) % 8,
            0,
            "a {slots}-slot header must keep group bases 8-aligned"
        );
    }

    // The odd case end to end: one worker, so the directory is a single u32.
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(0, 0, SalMessageKind::Scan, &[&[0u8; 24]]);
    let second = log.cursor();
    assert_eq!(second % 8, 0, "a one-worker group's successor base must be 8-aligned");
    log.write(0, 0, SalMessageKind::Scan, &[&[0u8; 24]]);
    assert_eq!(log.cursor() % 8, 0);
}

/// The stride the reader derives from the authenticated directory must equal the
/// `payload_size` the writer put in the prefix, at every width — including one
/// with empty slots interleaved among non-empty ones, where `align8(0) = 0` is
/// what makes the derivation exact.
#[test]
fn the_derived_stride_equals_the_writers_payload_size_at_every_width() {
    let log = TestLog::new(128 << 10, 1, 1);
    // Every iteration writes at offset 0, so only the widest group's bytes can be
    // left over from the one before it.
    let widest = PREFIX_BYTES + group_header_size(MAX_WORKERS) + MAX_WORKERS * align8(100);
    let buf = vec![0x33u8; 100];
    for &slots in &[1usize, 2, 3, 4, MAX_WORKERS] {
        unsafe { std::ptr::write_bytes(log.ptr(), 0, widest) };
        log.seek(0, 1);
        // Every other slot empty from slot 1 on, so the widths past 1 all carry
        // the interleaved shape.
        let payloads: Vec<&[u8]> = (0..slots)
            .map(|w| if w % 2 == 1 { &[][..] } else { buf.as_slice() })
            .collect();
        log.write(7, 9, SalMessageKind::Scan, &payloads);

        let (msg, next) = group_and_next(log.log(), 0, 1);
        assert_eq!(msg.slots() as usize, slots);
        let expected = group_header_size(slots) + payloads.iter().filter(|p| !p.is_empty()).count() * align8(100);
        assert_eq!(
            prefix_word(&log),
            pack_prefix(1, expected),
            "group_header_size disagrees at {slots} slots"
        );
        assert_eq!(
            next as usize,
            PREFIX_BYTES + expected,
            "the derived stride must equal the prefix's payload_size at {slots} slots"
        );
        assert_eq!(next, log.cursor(), "and the writer's own cursor advance");
    }
}

// ---------------------------------------------------------------------------
// Deferred publication: a group is laid out invisibly, and a rolled-back
// transaction leaves the log exactly as it was.
// ---------------------------------------------------------------------------

/// A transaction whose second family does not fit leaves the SAL byte-for-byte
/// as it was: the cursor is restored, and a reader at the zone start sees
/// `Absent` — the partial zone is invisible, not merely uncommitted.
#[test]
fn a_rolled_back_transaction_publishes_nothing() {
    // Sized so the first family fits under the ordinary cap and the second
    // does not.
    let payload = 4096usize;
    let log = TestLog::new(SENTINEL_SIZE + CHECKPOINT_RESERVE + payload + 512, 1, 1);
    let buf = vec![0xD1u8; payload];

    // A committed group ahead of the transaction, so the rollback must restore a
    // non-zero cursor and must not disturb what came before it.
    log.write(1, 1, SalMessageKind::Push, &[&[0u8; 64]]);
    let before_cursor = log.cursor();

    let scope = log.writer.defer_publication();
    let savepoint = scope.savepoint();
    let zone_start = log.cursor();
    log.try_write(2, 2, SalMessageKind::Push, true, &[&buf])
        .expect("the first family fits");
    assert!(
        log.try_write(3, 2, SalMessageKind::Push, false, &[&buf]).is_err(),
        "the second family must not fit"
    );
    scope.roll_back(savepoint);
    drop(scope);

    assert_eq!(log.cursor(), before_cursor, "the cursor is restored");
    assert!(
        matches!(log.log().read_at(zone_start, EpochGate::Live(1)), SalStep::Absent),
        "the rolled-back zone must be invisible, not merely uncommitted"
    );
    // The group before the transaction is untouched.
    assert_eq!(group_at(log.log(), 0).target_id, 1);
}

/// Inside a deferred scope a laid-out group stays invisible until the scope
/// publishes, and every one of them appears at once.
#[test]
fn a_deferred_group_is_invisible_until_the_scope_publishes() {
    let log = TestLog::new(1 << 20, 1, 1);
    let scope = log.writer.defer_publication();
    log.try_write(1, 1, SalMessageKind::Push, true, &[&[0u8; 32]])
        .expect("group fits");
    let second = log.cursor();
    log.try_write(2, 1, SalMessageKind::Push, false, &[&[0u8; 32]])
        .expect("group fits");

    assert!(matches!(log.log().read_at(0, EpochGate::Live(1)), SalStep::Absent));
    assert!(matches!(log.log().read_at(second, EpochGate::Live(1)), SalStep::Absent));

    drop(scope);
    assert_eq!(group_at(log.log(), 0).target_id, 1);
    assert_eq!(group_at(log.log(), second).target_id, 2);
}

// ---------------------------------------------------------------------------
// Footprint exactness: the exchange relay sizes its group before taking the SAL
// lock and writes it after.
// ---------------------------------------------------------------------------

/// `footprint` must equal what `write` consumes: a prediction below the truth
/// turns a reclaim-and-retry into a fatal refusal on a relay no worker can go
/// without.
///
/// Both an empty slot and a populated one are covered: a dataless slot still
/// carries the group's schema block, so only the data block distinguishes them.
#[test]
fn footprint_equals_emitted_bytes() {
    use crate::runtime::wire::{WireData, WireMsg};
    use crate::test_support::{make_batch, make_schema_u64_i64};

    let nw = 4;
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let block = crate::catalog::encode_schema_block(&schema, 16);

    let log = TestLog::new(1 << 20, nw, 1);

    // Slot 2 stays empty: the relay passes a dataless slot for a zero-row worker.
    let worker_data = [
        WireData::Whole(Some(&batch)),
        WireData::Whole(Some(&batch)),
        WireData::Whole(None),
        WireData::Whole(Some(&batch)),
    ];
    let group = DirectGroup {
        template: WireMsg {
            target_id: 16,
            seek_pk: 7,
            seek_col_idx: 1,
            schema_block: Some(&block),
            ..Default::default()
        },
        data: GroupData::PerWorker(&worker_data),
        ..DirectGroup::new(SalMessageKind::ExchangeRelay)
    };

    let predicted = log.writer.footprint(&group);
    let before = log.cursor();
    log.writer.write(&group).expect("group fits");
    assert_eq!(
        (log.cursor() - before) as usize,
        predicted,
        "footprint must equal emitted bytes"
    );
}

/// A schema with no German-string column takes the one-copy scatter, whatever
/// its column widths: `with_group` hands the writer [`WireData::Scattered`]
/// slots rather than per-worker sub-`Batch`es. A stride-4 payload column is the
/// case that used to fall off it.
#[test]
fn a_narrow_fixed_width_schema_scatters_in_one_copy() {
    use crate::runtime::master::scatter::{with_commit_indices, with_group};
    use crate::runtime::wire::{WireData, WireSchema};
    use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
    use gnitz_wire::type_code;

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I32, 0),
        ],
        &[0],
    );
    let batch = make_batch_raw(&schema, &[(0, 1, 0), (1, 1, 1), (2, 1, 2)]);

    let relation = WireSchema::encoded(16, schema);
    with_commit_indices(&batch, &schema, 4, |wi| {
        with_group(&batch, wi, &relation, DirectGroup::new(SalMessageKind::Push), |g| {
            let GroupData::PerWorker(slots) = g.data else {
                panic!("a scatter group carries per-worker slots");
            };
            for (w, slot) in slots.iter().enumerate() {
                assert!(
                    matches!(slot, WireData::Scattered { .. }),
                    "slot {w} must scatter straight into the SAL slot"
                );
            }
        })
    });
}

// ---------------------------------------------------------------------------
// The group header's own integrity: a digest over the header, seeded with the
// group's byte offset. Every field swept below drives a routing or replay
// decision, so the digest is what stands between a damaged log and a wrong one.
// ---------------------------------------------------------------------------

/// `lsn`, the kind and zone-start bytes, `target_id`, `slot_count`, the epoch
/// word and every directory entry live inside the digested span; the digest field itself is excluded from
/// it but is what the verdict compares against. One bit anywhere in the header
/// must therefore fail the read.
#[test]
fn every_single_bit_flip_in_a_group_header_is_rejected() {
    let log = TestLog::new(1 << 20, 1, 1);
    let buf = vec![0x5Au8; 64];
    log.write(42, 100, SalMessageKind::DdlSync, &[&buf, &[], &buf]);

    let view = log.log();
    let hdr_len = group_header_size(3);
    assert!(matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Group(..)));

    let hdr = unsafe { std::slice::from_raw_parts_mut(log.ptr().add(PREFIX_BYTES), hdr_len) };
    sweep_bit_flips(hdr, 0..hdr_len, |byte, bit, _| {
        assert!(
            matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt(0)),
            "header byte {byte} bit {bit} must fail the digest"
        );
    });
}

/// The digest is seeded with the group's byte offset, which makes a valid header
/// self-locating: the resync scan trials every 8-byte-aligned word, so a header
/// that verified anywhere it was copied to would let the walk resume on the wrong
/// group.
#[test]
fn a_header_only_verifies_at_the_offset_it_was_published_at() {
    let log = TestLog::new(1 << 20, 1, 1);
    let buf = vec![0x11u8; 64];
    log.write(42, 100, SalMessageKind::Scan, &[&buf]);
    let second = log.write(43, 101, SalMessageKind::Scan, &[&buf]);

    let hdr_len = group_header_size(1);
    // Group B's whole header over group A's — same epoch, same shape, a
    // different address.
    unsafe {
        let b_hdr: Vec<u8> =
            std::slice::from_raw_parts(log.ptr().add(second as usize + PREFIX_BYTES), hdr_len).to_vec();
        std::ptr::copy_nonoverlapping(b_hdr.as_ptr(), log.ptr().add(PREFIX_BYTES), hdr_len);
    }
    assert!(
        matches!(log.log().read_at(0, EpochGate::Walk(1)), SalStep::Corrupt(0)),
        "a header published elsewhere must not verify here"
    );
}

/// The probe's two mapping bounds protect different reads, and both must reject
/// before the read they guard. The region is mapped at exactly `size` bytes, so a
/// read past it faults rather than merely returning garbage.
#[test]
fn a_probe_never_reads_past_the_end_of_the_mapping() {
    let size = 1 << 20;
    let log = TestLog::new(size, 1, 1);
    // A non-zero tail, so the prefix test does not answer for either fixture.
    unsafe { std::ptr::write_bytes(log.ptr().add(size - 4096), 0xEE, 4096) };
    let view = log.log();

    // (a) The fixed part does not fit: `slot_count` would be read 16 bytes past
    // the mapping.
    assert!(
        matches!(
            view.read_at((size - PREFIX_BYTES) as u64, EpochGate::Walk(1)),
            SalStep::Corrupt(_)
        ),
        "a candidate 8 bytes from the end must be rejected before the slot-count read"
    );

    // (b) The fixed part fits but a MAX_WORKERS directory does not.
    let base = size - PREFIX_BYTES - group_header_size(0);
    let hdr = unsafe { std::slice::from_raw_parts_mut(log.ptr().add(base + PREFIX_BYTES), group_header_size(0)) };
    gnitz_wire::write_u32_le(hdr, 16, MAX_WORKERS as u32);
    assert!(
        matches!(view.read_at(base as u64, EpochGate::Walk(1)), SalStep::Corrupt(_)),
        "a MAX_WORKERS directory that overruns the mapping must be rejected before the digest read"
    );
}

// ---------------------------------------------------------------------------
// Group kinds: the ordinal byte round-trips, and one this build does not know
// is corruption rather than a guess.
// ---------------------------------------------------------------------------

/// Re-stamp the digest of the group at offset 0 over its edited header, so the
/// decode below the digest is what a read exercises.
fn restamp_header(log: &TestLog, edit: impl FnOnce(&mut [u8])) {
    let hdr = unsafe { std::slice::from_raw_parts_mut(log.ptr().add(PREFIX_BYTES), group_header_size(1)) };
    edit(hdr);
    let digest = group_digest(0, hdr);
    gnitz_wire::write_u64_le(hdr, OFF_DIGEST, digest);
}

/// Every kind a writer can name reads back as itself, under both zone-start
/// values.
#[test]
fn every_kind_round_trips_through_the_header() {
    let log = TestLog::new(1 << 20, 1, 1);
    for zone_start in [false, true] {
        for &kind in SalMessageKind::ALL {
            log.seek(0, 1);
            log.try_write(0, 0, kind, zone_start, &[&[0u8; 8]]).expect("group fits");
            let msg = group_at(log.log(), 0);
            assert_eq!(msg.kind, kind, "kind {kind:?} (zone_start={zone_start})");
            assert_eq!(msg.zone_start, zone_start, "{kind:?}");
        }
    }
}

/// A digest-valid header whose kind ordinal or zone-start byte names nothing
/// this build knows reads back `Corrupt`: it was written by another layout, and
/// guessing is not an option. The re-stamp is proven live by a known ordinal
/// first, so a `Corrupt` below cannot be the digest's verdict.
#[test]
fn an_unknown_ordinal_in_a_verified_header_is_corrupt() {
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(0, 0, SalMessageKind::Scan, &[&[0u8; 8]]);
    let view = log.log();

    restamp_header(&log, |hdr| hdr[OFF_KIND] = SalMessageKind::Tick.as_wire());
    assert_eq!(group_at(view, 0).kind, SalMessageKind::Tick, "the re-stamp verifies");

    let unknown = (0..=u8::MAX)
        .find(|&b| SalMessageKind::from_wire(b).is_none())
        .expect("fewer than 256 kinds");
    restamp_header(&log, |hdr| hdr[OFF_KIND] = unknown);
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt(0)),
        "an unknown kind ordinal must read as corruption"
    );

    restamp_header(&log, |hdr| {
        hdr[OFF_KIND] = SalMessageKind::Scan.as_wire();
        hdr[OFF_ZONE_START] = 2;
    });
    assert!(
        matches!(view.read_at(0, EpochGate::Walk(1)), SalStep::Corrupt(0)),
        "a zone-start byte outside {{0, 1}} must read as corruption"
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
    use crate::runtime::sal::SalReader;
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(7, 11, SalMessageKind::Scan, &[&[0u8; 32]]);
    // Raise only the prefix's epoch copy: 1 -> 2, header untouched.
    unsafe {
        let word = log.ptr() as *mut u64;
        *word = (*word & 0xFFFF_FFFF) | (2u64 << 32);
    }
    let reader = unsafe { SalReader::new(log.ptr() as *const u8, 0, log.size, 1) };
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
        stderr.contains("SAL group header failed its digest"),
        "{name} aborted for the wrong reason\nstderr:\n{stderr}"
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn the_live_path_aborts_on_a_damaged_header_internal() {
    use crate::runtime::sal::SalReader;
    if !crate::test_support::in_child_test() {
        return;
    }
    let log = TestLog::new(1 << 20, 1, 1);
    log.write(7, 11, SalMessageKind::Scan, &[&[0u8; 32]]);
    // A sanity read before the damage, so the abort below is the damage.
    let reader = unsafe { SalReader::new(log.ptr() as *const u8, 0, log.size, 0) };
    assert!(reader.next().is_some());
    unsafe { *log.ptr().add(PREFIX_BYTES) ^= 1 };

    let reader = unsafe { SalReader::new(log.ptr() as *const u8, 0, log.size, 0) };
    let _ = reader.next();
    unreachable!("a damaged header on the live path must fail-stop, not park");
}
