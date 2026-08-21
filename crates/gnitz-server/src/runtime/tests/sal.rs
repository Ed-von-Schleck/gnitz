use crate::runtime::posix;
use crate::runtime::posix::write_u32_raw;
use crate::runtime::sal::{
    atomic_load_u64, effective_max, group_header_size, sal_begin_group, sal_probe_header, sal_read_group_header,
    sal_tail_slot_count, sal_write_group, EpochGate, SalMessage, SalReader, SalStep, SalWriter, CHECKPOINT_RESERVE,
    FLAG_DDL_SYNC, FLAG_FLUSH, FLAG_FLUSH_EPH, FLAG_SHUTDOWN, FLAG_TXN_COMMIT, MAX_WORKERS, MIN_SAL_BYTES,
    SENTINEL_SIZE,
};
use gnitz_engine_testkit::{sweep_bit_flips, SharedRegion};
use gnitz_wire::align8;
use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;

fn make_test_data(val: u8, len: usize) -> Vec<u8> {
    vec![val; len]
}

/// The group `worker` reads at `base` and the cursor past it, or a panic if the
/// bytes are not a group.
unsafe fn group_at(ptr: *const u8, base: u64, worker: u32, size: usize) -> (SalMessage<'static>, u64) {
    match sal_read_group_header(ptr, base, worker, EpochGate::Any, size as u64) {
        SalStep::Group(msg, next) => (msg, next),
        _ => panic!("group present at offset {base}"),
    }
}

/// The authenticated epoch of the header at `base`.
unsafe fn epoch_at(ptr: *const u8, base: u64, size: usize) -> u32 {
    sal_probe_header(ptr, base, size as u64).expect("header verifies").1
}

#[test]
fn test_sal_round_trip() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let bufs: Vec<Vec<u8>> = vec![
            make_test_data(0xAA, 100),
            vec![],
            make_test_data(0xBB, 200),
            make_test_data(0xCC, 50),
        ];

        let payloads: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
        let new_cursor = sal_write_group(ptr, 0, 42, 100, 0, 1, size as u64, &payloads).expect("group fits");
        assert!(new_cursor > 0);

        for w in 0..4u32 {
            let (rr, next) = group_at(ptr, 0, w, size);
            assert_eq!(rr.lsn, 100);
            assert_eq!(rr.target_id, 42);
            assert_eq!(epoch_at(ptr, 0, size), 1);
            assert_eq!(next, new_cursor);

            if bufs[w as usize].is_empty() {
                assert!(rr.wire_data.is_none(), "no data slot for worker {w}");
            } else {
                assert_eq!(rr.wire_data.expect("data slot"), bufs[w as usize].as_slice());
            }
        }
    }
}

#[test]
fn test_sal_unicast_isolation() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf = make_test_data(0xDD, 128);
        let payloads: [&[u8]; 4] = [&[], &[], &buf, &[]];

        sal_write_group(ptr, 0, 10, 1, 0, 1, size as u64, &payloads).expect("group fits");

        for w in [0u32, 1, 3] {
            let (rr, next) = group_at(ptr, 0, w, size);
            assert!(rr.wire_data.is_none(), "no data slot for worker {w}");
            assert!(next > 0);
        }
        let (rr, _) = group_at(ptr, 0, 2, size);
        assert_eq!(rr.wire_data.expect("data slot"), buf.as_slice());
    }
}

#[test]
fn test_sal_multiple_groups() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let mut cursor = 0u64;
        for g in 0..3u64 {
            let buf = make_test_data((g + 1) as u8, 64);
            let payloads: [&[u8]; 2] = [&buf, &[]];
            cursor = sal_write_group(ptr, cursor, g as u32, g * 10, 0, 1, size as u64, &payloads).expect("group fits");
        }

        let mut rc = 0u64;
        for g in 0..3u64 {
            let (rr, next) = group_at(ptr, rc, 0, size);
            assert_eq!(rr.lsn, g * 10);
            assert_eq!(rr.target_id, g as u32);
            assert_eq!(rr.wire_data.expect("data slot"), vec![(g + 1) as u8; 64].as_slice());
            rc = next;
        }
        assert!(matches!(
            sal_read_group_header(ptr, rc, 0, EpochGate::Any, size as u64),
            SalStep::Absent
        ));
    }
}

#[test]
fn test_sal_epoch_write_read() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf = make_test_data(0x11, 32);
        sal_write_group(ptr, 0, 0, 0, 0, 42, size as u64, &[&buf]).expect("group fits");

        group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), 42);
    }
}

#[test]
fn test_sal_full_error() {
    unsafe {
        // Sized off the ordinary cap, not off the raw mapping: with a bare 256-byte
        // region the reserve alone would refuse the group and the size arithmetic
        // this test exists for would never run.
        let payload = 256usize;
        let size = SENTINEL_SIZE + CHECKPOINT_RESERVE + payload;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf = make_test_data(0xFF, payload);
        assert!(
            sal_write_group(ptr, 0, 0, 0, 0, 1, size as u64, &[&buf]).is_none(),
            "a group overrunning the ordinary cap must be rejected"
        );
    }
}

#[test]
fn test_sal_cross_process() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let efd = posix::eventfd_create();
        assert!(efd >= 0);

        let pid = libc::fork();
        if pid == 0 {
            let buf = make_test_data(0x77, 128);
            sal_write_group(ptr, 0, 99, 555, 0, 1, size as u64, &[&buf]).expect("group fits");
            posix::eventfd_signal(efd);
            libc::_exit(0);
        }

        let r = posix::eventfd_wait(efd, 5000);
        assert!(r > 0, "eventfd timed out");

        let (rr, _) = group_at(ptr, 0, 0, size);
        assert_eq!(rr.lsn, 555);
        assert_eq!(rr.target_id, 99);
        assert_eq!(rr.wire_data.expect("data slot"), vec![0x77u8; 128].as_slice());

        let mut status = 0i32;
        libc::waitpid(pid, &mut status, 0);
        libc::close(efd);
    }
}

#[test]
fn test_sal_checkpoint_reset() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf1 = make_test_data(0x11, 32);
        sal_write_group(ptr, 0, 0, 0, 0, 1, size as u64, &[&buf1]).expect("group fits");

        std::ptr::write_bytes(ptr, 0, size);
        let buf2 = make_test_data(0x22, 32);
        sal_write_group(ptr, 0, 0, 0, 0, 2, size as u64, &[&buf2]).expect("group fits");

        let (rr, _) = group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), 2);
        assert_eq!(rr.wire_data.expect("data slot"), vec![0x22u8; 32].as_slice());
    }
}

/// `wal.sal` is never truncated, so a group can land on an offset a wider group
/// used before it. The narrow group's recorded slot count is what makes the
/// leftover entries unreachable — both to a reader asking for one of them and to
/// the tail-count probe boot replay steers by.
#[test]
fn a_group_hides_the_slots_of_a_wider_group_at_the_same_offset() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        // An 8-worker group leaves a fully populated directory at offset 0.
        let wide: Vec<Vec<u8>> = (0..8).map(|i| make_test_data(0xA0 + i as u8, 64)).collect();
        let wide_refs: Vec<&[u8]> = wide.iter().map(|b| b.as_slice()).collect();
        sal_write_group(ptr, 0, 0, 0, 0, 1, size as u64, &wide_refs).expect("group fits");

        // The same offset rewritten by a 2-worker topology.
        let narrow = [make_test_data(0x11, 32), make_test_data(0x22, 32)];
        let narrow_refs: Vec<&[u8]> = narrow.iter().map(|b| b.as_slice()).collect();
        sal_write_group(ptr, 0, 0, 0, 0, 2, size as u64, &narrow_refs).expect("group fits");

        assert_eq!(
            sal_tail_slot_count(ptr, size as u64),
            Some(2),
            "the tail's own count is the narrow one"
        );
        for w in 0..8u32 {
            let (r, _) = group_at(ptr, 0, w, size);
            assert_eq!(r.slots, 2);
            if w < 2 {
                assert_eq!(
                    r.wire_data.expect("narrow slot").len(),
                    32,
                    "slot {w} is the narrow group's"
                );
            } else {
                assert!(r.wire_data.is_none(), "slot {w} was never written by this group");
            }
        }
    }
}

#[test]
fn sal_begin_group_rejects_too_many_workers() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let sizes = [0u32; MAX_WORKERS + 1];
        let result = sal_begin_group(ptr, 0, size, 0, 0, 0, 1, &sizes[..MAX_WORKERS + 1]);
        assert!(
            result.is_none(),
            "sal_begin_group must reject more than MAX_WORKERS entries"
        );
    }
}

#[test]
fn sal_begin_group_rejects_cursor_overflow() {
    unsafe {
        let size = SENTINEL_SIZE + CHECKPOINT_RESERVE + 512;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        // Push the cursor so close to the ordinary cap that the group header
        // won't fit under it.
        let sizes = [0u32; 1];
        let result = sal_begin_group(ptr, effective_max(0, size) - 1, size, 0, 0, 0, 1, &sizes[..1]);
        assert!(
            result.is_none(),
            "sal_begin_group must reject when cursor + total > the ordinary cap"
        );
    }
}

/// The worst-case footprint of a terminal group: a `MAX_WORKERS` broadcast whose
/// every slot is a bare control block. All three emitters — `sync_round`,
/// `shutdown_workers` and `write_checkpoint_group` — carry neither a schema block
/// nor data.
fn worst_case_terminal_group() -> usize {
    8 + group_header_size(MAX_WORKERS) + MAX_WORKERS * align8(CTRL_BLOCK_SIZE_NO_BLOB)
}

#[test]
fn effective_max_reserves_by_flag() {
    let mmap = 1usize << 30;
    assert_eq!(effective_max(0, mmap), mmap - SENTINEL_SIZE - CHECKPOINT_RESERVE);
    for terminal in [FLAG_FLUSH, FLAG_FLUSH_EPH, FLAG_SHUTDOWN] {
        assert_eq!(
            effective_max(terminal, mmap),
            mmap - SENTINEL_SIZE,
            "a terminal group may spend the checkpoint reserve"
        );
    }
    assert_eq!(
        effective_max(FLAG_DDL_SYNC | FLAG_TXN_COMMIT, mmap),
        mmap - CHECKPOINT_RESERVE,
        "a sentinel may spend the sentinel headroom but not the reserve"
    );
}

#[test]
fn checkpoint_reserve_holds_two_terminal_groups() {
    // Two, not one: the watchdog's crash arm broadcasts FLAG_SHUTDOWN without the
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
    let cap = effective_max(0, MIN_SAL_BYTES);
    assert!(
        cap > MIN_SAL_BYTES - MIN_SAL_BYTES / 64,
        "the reserve must not eat the SAL floor: cap {cap} of {MIN_SAL_BYTES}"
    );
}

#[test]
fn terminal_and_sentinel_fit_where_an_ordinary_group_does_not() {
    unsafe {
        // Invariant 1: with the cursor just under the ordinary cap, the log still
        // admits a full-width checkpoint round and a zone-closing sentinel.
        let size = MIN_SAL_BYTES;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let cursor = effective_max(0, size) - 8;

        let sizes = [0u32; 1];
        assert!(
            sal_begin_group(ptr, cursor, size, 0, 0, 0, 1, &sizes[..1]).is_none(),
            "an ordinary group must be refused once the cursor passes the ordinary cap"
        );
        sal_begin_group(ptr, cursor, size, 0, 0, FLAG_DDL_SYNC | FLAG_TXN_COMMIT, 1, &[])
            .expect("the zone-closing sentinel must still fit")
            .commit();

        let flush_sizes = [CTRL_BLOCK_SIZE_NO_BLOB as u32; MAX_WORKERS];
        sal_begin_group(ptr, cursor, size, 0, 0, FLAG_FLUSH, 1, &flush_sizes)
            .expect("a MAX_WORKERS checkpoint round must still fit")
            .commit();
    }
}

#[test]
fn test_sal_epoch_fence() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf = make_test_data(0x33, 32);
        let c1 = sal_write_group(ptr, 0, 0, 0, 0, 5, size as u64, &[&buf]).expect("group fits");
        sal_write_group(ptr, c1, 0, 0, 0, 6, size as u64, &[&buf]).expect("group fits");

        let (_, next) = group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), 5);
        group_at(ptr, next, 0, size);
        assert_eq!(epoch_at(ptr, next, size), 6);
    }
}

#[test]
fn test_commit_sentinel_round_trip() {
    // Zone shape: two normal groups at lsn=K, one sentinel at lsn=K,
    // then an unrelated group at lsn=K+1. After read-back the third
    // group must be the only one with FLAG_TXN_COMMIT set.
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        // Two normal groups at the same LSN.
        let buf = make_test_data(0xAA, 32);
        let payloads: [&[u8]; 2] = [&buf, &buf];
        let c1 = sal_write_group(ptr, 0, 100, 7, 0, 1, size as u64, &payloads).expect("group fits");
        let c2 = sal_write_group(ptr, c1, 101, 7, 0, 1, size as u64, &payloads).expect("group fits");

        // Sentinel via SalWriter at the same LSN.
        let writer = SalWriter::new(ptr, -1, size as u64, 2);
        writer.reset(c2, 1);
        writer.write_commit_sentinel(7).unwrap();

        // Trailing group at the next LSN.
        sal_write_group(ptr, writer.cursor(), 102, 8, 0, 1, size as u64, &payloads).expect("group fits");

        // Walk the SAL via SalReader (worker 0 perspective).
        let reader = SalReader::for_walk(ptr as *const u8, 0, size);
        let (m1, c1) = reader.try_read(0, EpochGate::Any).unwrap();
        let (m2, c2) = reader.try_read(c1, EpochGate::Any).unwrap();
        let (m3, c3) = reader.try_read(c2, EpochGate::Any).unwrap();
        let (m4, _) = reader.try_read(c3, EpochGate::Any).unwrap();

        assert_eq!(m1.lsn, 7);
        assert_eq!(m2.lsn, 7);
        assert_eq!(m3.lsn, 7);
        assert_eq!(m4.lsn, 8);
        assert_eq!(m1.flags & FLAG_TXN_COMMIT, 0);
        assert_eq!(m2.flags & FLAG_TXN_COMMIT, 0);
        assert_eq!(m3.flags & FLAG_TXN_COMMIT, FLAG_TXN_COMMIT);
        assert_eq!(m3.flags & FLAG_DDL_SYNC, FLAG_DDL_SYNC);
        assert_eq!(m4.flags & FLAG_TXN_COMMIT, 0);
    }
}

#[test]
fn test_commit_sentinel_zero_payload() {
    // The sentinel must produce wire_data=None for every worker — it is
    // a header-only group and must not be misread as a DDL_SYNC batch.
    let size = 1 << 20;
    let region = SharedRegion::new(size);
    let ptr = region.ptr();

    let writer = SalWriter::new(ptr, -1, size as u64, 4);
    writer.reset(0, 1);
    writer.write_commit_sentinel(123).unwrap();

    for w in 0..4 {
        let reader = SalReader::for_walk(ptr as *const u8, w, size);
        let (msg, _) = reader.try_read(0, EpochGate::Any).unwrap();
        assert_eq!(msg.lsn, 123);
        assert!(
            msg.wire_data.is_none(),
            "sentinel must carry no per-worker payload for worker {w}"
        );
    }
}

#[test]
fn test_batched_push_shares_zone_lsn() {
    // Phase 6 invariant: every push group in a batched commit carries
    // the same zone LSN, followed by exactly one commit sentinel. Two
    // pipelined pushes thus produce three SAL groups: push, push, sentinel.
    use crate::runtime::sal::FLAG_PUSH;
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let nw = 4u32;
        let zone_lsn = 42u64;

        let payload = make_test_data(0xDD, 64);
        let payloads: Vec<&[u8]> = (0..nw).map(|_| payload.as_slice()).collect();
        // Two push groups at the same LSN.
        let c1 = sal_write_group(ptr, 0, 1000, zone_lsn, FLAG_PUSH, 1, size as u64, &payloads).expect("group fits");
        let c2 = sal_write_group(ptr, c1, 1001, zone_lsn, FLAG_PUSH, 1, size as u64, &payloads).expect("group fits");

        // Closing sentinel.
        let writer = SalWriter::new(ptr, -1, size as u64, nw as usize);
        writer.reset(c2, 1);
        writer.write_commit_sentinel(zone_lsn).unwrap();

        let reader = SalReader::for_walk(ptr as *const u8, 0, size);
        let (m1, c1) = reader.try_read(0, EpochGate::Any).unwrap();
        let (m2, c2) = reader.try_read(c1, EpochGate::Any).unwrap();
        let (m3, _) = reader.try_read(c2, EpochGate::Any).unwrap();
        assert_eq!(m1.lsn, zone_lsn);
        assert_eq!(m2.lsn, zone_lsn);
        assert_eq!(m3.lsn, zone_lsn);
        assert_eq!(m1.flags & FLAG_PUSH, FLAG_PUSH);
        assert_eq!(m2.flags & FLAG_PUSH, FLAG_PUSH);
        assert_eq!(m3.flags & FLAG_TXN_COMMIT, FLAG_TXN_COMMIT);
        // Recovery's collect_committed_lsns must see this LSN as committed.
        let committed = {
            // Inline scan of the SAL: same logic as bootstrap's pass 1.
            let mut set = std::collections::HashSet::new();
            let mut off = 0u64;
            while (off as usize) + 8 < size {
                let (msg, next) = match reader.try_read(off, EpochGate::Any) {
                    Some(v) => v,
                    None => break,
                };
                off = next;
                if msg.flags & FLAG_TXN_COMMIT != 0 {
                    set.insert(msg.lsn);
                }
            }
            set
        };
        assert!(committed.contains(&zone_lsn), "sentinel must commit the push zone");
    }
}

#[test]
fn test_zone_two_groups_one_sentinel() {
    // Phase 3 shape: a CREATE TABLE emits two broadcasts (e.g. COL_TAB
    // then TABLE_TAB) at one zone-LSN, then a sentinel at the same LSN.
    // Recovery must see all three groups carry the same LSN, and only
    // the sentinel must carry FLAG_TXN_COMMIT.
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let nw = 4u32;

        let buf_col = make_test_data(0xC0, 64);
        let buf_tab = make_test_data(0x7A, 96);
        let zone_lsn = 17u64;

        // Group 1: COL_TAB rows.
        let payloads_col: Vec<&[u8]> = (0..nw).map(|_| buf_col.as_slice()).collect();
        let c1 = sal_write_group(ptr, 0, 200, zone_lsn, 0, 1, size as u64, &payloads_col).expect("group fits");

        // Group 2: TABLE_TAB row, same LSN.
        let payloads_tab: Vec<&[u8]> = (0..nw).map(|_| buf_tab.as_slice()).collect();
        let c2 = sal_write_group(ptr, c1, 201, zone_lsn, 0, 1, size as u64, &payloads_tab).expect("group fits");

        // Sentinel.
        let writer = SalWriter::new(ptr, -1, size as u64, nw as usize);
        writer.reset(c2, 1);
        writer.write_commit_sentinel(zone_lsn).unwrap();

        // Walk on every worker's perspective; assert zone shape.
        for w in 0..nw {
            let reader = SalReader::for_walk(ptr as *const u8, w, size);
            let (m1, c1) = reader.try_read(0, EpochGate::Any).unwrap();
            let (m2, c2) = reader.try_read(c1, EpochGate::Any).unwrap();
            let (m3, _) = reader.try_read(c2, EpochGate::Any).unwrap();
            assert_eq!(m1.lsn, zone_lsn);
            assert_eq!(m2.lsn, zone_lsn);
            assert_eq!(m3.lsn, zone_lsn);
            assert_eq!(m1.flags & FLAG_TXN_COMMIT, 0);
            assert_eq!(m2.flags & FLAG_TXN_COMMIT, 0);
            assert_eq!(m3.flags & FLAG_TXN_COMMIT, FLAG_TXN_COMMIT);
            assert!(m1.wire_data.is_some(), "first DDL group has data");
            assert!(m2.wire_data.is_some(), "second DDL group has data");
            assert!(m3.wire_data.is_none(), "sentinel carries no data");
        }
    }
}

#[test]
fn test_two_groups_same_lsn() {
    // Phase 1 invariant: the SAL writer accepts whatever LSN the caller
    // supplies — it no longer owns a counter. Two groups written at the
    // same LSN must both carry it on read-back. Phase 3 builds a zone on
    // top of this: multiple groups + a sentinel, all sharing one LSN.
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf1 = make_test_data(0x10, 32);
        let c1 = sal_write_group(ptr, 0, 7, 42, 0, 1, size as u64, &[&buf1]).expect("group fits");

        let buf2 = make_test_data(0x20, 32);
        sal_write_group(ptr, c1, 8, 42, 0, 1, size as u64, &[&buf2]).expect("group fits");

        let (rr1, next) = group_at(ptr, 0, 0, size);
        let (rr2, _) = group_at(ptr, next, 0, size);
        assert_eq!(rr1.lsn, 42);
        assert_eq!(rr2.lsn, 42);
        assert_eq!(rr1.target_id, 7);
        assert_eq!(rr2.target_id, 8);
    }
}

#[test]
fn test_sal_cross_process_checkpoint() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let efd = posix::eventfd_create();
        let efd2 = posix::eventfd_create();
        assert!(efd >= 0 && efd2 >= 0);

        let pid = libc::fork();
        if pid == 0 {
            let buf = make_test_data(0xAA, 64);
            sal_write_group(ptr, 0, 0, 10, 0, 1, size as u64, &[&buf]).expect("group fits");
            posix::eventfd_signal(efd);

            posix::eventfd_wait(efd2, 5000);

            std::ptr::write_bytes(ptr, 0, size);
            let buf2 = make_test_data(0xBB, 64);
            sal_write_group(ptr, 0, 0, 20, 0, 2, size as u64, &[&buf2]).expect("group fits");
            posix::eventfd_signal(efd);
            libc::_exit(0);
        }

        posix::eventfd_wait(efd, 5000);
        let (rr1, _) = group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), 1);
        assert_eq!(rr1.lsn, 10);
        posix::eventfd_signal(efd2);

        posix::eventfd_wait(efd, 5000);
        let (rr2, _) = group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), 2);
        assert_eq!(rr2.lsn, 20);

        let mut status = 0i32;
        libc::waitpid(pid, &mut status, 0);
        libc::close(efd);
    }
}

#[test]
fn test_sal_prefix_epoch_gate() {
    // The reader-side epoch gate: the group's (epoch << 32 | payload_size)
    // prefix is checked BEFORE any header byte is read. A mismatched
    // expectation parks the reader (no message, cursor unmoved); a matching
    // or absent expectation reads the group normally.
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf = make_test_data(0x5A, 48);
        let new_cursor = sal_write_group(ptr, 0, 7, 11, 0, 1, size as u64, &[&buf]).expect("group fits");

        let reader = SalReader::new(ptr as *const u8, 0, size, -1, 1);

        // Wrong expected epoch: parked without touching header bytes.
        assert!(
            reader.try_read(0, EpochGate::Live(2)).is_none(),
            "epoch-mismatched slot must park the reader"
        );

        // Matching epoch: consumable.
        let (msg, cursor) = reader
            .try_read(0, EpochGate::Live(1))
            .expect("matching epoch reads the group");
        assert_eq!(epoch_at(ptr, 0, size), 1);
        assert_eq!(msg.lsn, 11);
        assert_eq!(msg.target_id, 7);
        assert_eq!(cursor, new_cursor);

        // Ungated recovery read: consumable without knowing the epoch.
        let (msg, _) = reader
            .try_read(0, EpochGate::Any)
            .expect("recovery walk reads without an expectation");
        assert_eq!(msg.lsn, 11);
        assert_eq!(epoch_at(ptr, 0, size), 1);
    }
}

#[test]
fn test_sal_prefix_packing_boundaries() {
    // The (epoch << 32 | payload_size) prefix word must round-trip at the
    // boundaries: a one-slot group with no data (payload_size == its header
    // size) at epoch u32::MAX, and a multi-MiB group at epoch 1.
    unsafe {
        let size = 8 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        // Header-only group (no worker data), epoch u32::MAX.
        let empty: [&[u8]; 1] = [&[]];
        let new_cursor = sal_write_group(ptr, 0, 0, 0, 0, u32::MAX, size as u64, &empty).expect("group fits");
        let word = atomic_load_u64(ptr);
        assert_eq!((word >> 32) as u32, u32::MAX);
        assert_eq!((word & 0xFFFF_FFFF) as usize, group_header_size(1));
        let (_, next) = group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), u32::MAX);
        assert_eq!(next, (8 + group_header_size(1)) as u64);
        assert_eq!(new_cursor, next);

        // Multi-MiB group, epoch 1.
        std::ptr::write_bytes(ptr, 0, size);
        let big = make_test_data(0xEE, 3 << 20);
        sal_write_group(ptr, 0, 0, 0, 0, 1, size as u64, &[&big]).expect("group fits");
        let expected_payload = group_header_size(1) + (3 << 20); // 3 MiB is already 8-aligned
        let word = atomic_load_u64(ptr);
        assert_eq!((word >> 32) as u32, 1);
        assert_eq!((word & 0xFFFF_FFFF) as usize, expected_payload);
        let (rr, next) = group_at(ptr, 0, 0, size);
        assert_eq!(epoch_at(ptr, 0, size), 1);
        assert_eq!(next, (8 + expected_payload) as u64);
        assert_eq!(rr.wire_data.expect("data slot").len(), 3 << 20);
    }
}

// ---------------------------------------------------------------------------
// scatter_group_footprint exactness: the committer's per-transaction fit check
// and Phase-B fail-stop depend on it equaling the bytes emission consumes.
// ---------------------------------------------------------------------------

/// Emit `batch` (partitioned or broadcast) through `scatter_wire_group` and
/// assert the cursor advance equals `scatter_group_footprint`'s prediction.
unsafe fn assert_footprint_exact(
    schema: &gnitz_engine::schema::SchemaDescriptor,
    batch: &gnitz_engine::storage::Batch,
    broadcast: bool,
    nw: usize,
) {
    use crate::runtime::master::scatter::with_commit_indices;
    use crate::runtime::sal::FLAG_PUSH;
    use crate::runtime::wire::build_schema_wire_block;
    use gnitz_engine::storage::compute_wire_props;

    // Drive the production dispatch: `broadcast` is exactly the `Replicated`
    // placement `with_commit_indices` routes on, and nothing in the wire encoding
    // reads it.
    let placement = if broadcast {
        gnitz_engine::schema::Placement::Replicated
    } else {
        gnitz_engine::schema::Placement::KEYED_DEFAULT
    };
    let schema = &schema.with_placement(placement);

    let size = 1 << 20;
    let region = SharedRegion::new(size);
    let ptr = region.ptr();
    let writer = SalWriter::new(ptr, -1, size as u64, nw);
    writer.reset(0, 1); // epoch >= 1 for sal_begin_group's debug_assert

    let target_id = 16u32;
    let block = build_schema_wire_block(schema, target_id);
    let props = compute_wire_props(schema);
    let req_ids: Vec<u64> = (0..nw as u64).map(|i| i + 1).collect();

    let emit = |wi: &[Vec<u32>]| {
        let predicted =
            writer.scatter_group_footprint(batch, wi, schema, target_id, Some(block.as_slice()), Some(props));
        let before = writer.cursor();
        writer
            .scatter_wire_group(
                batch,
                wi,
                schema,
                target_id,
                7,
                FLAG_PUSH,
                0,
                0,
                &req_ids,
                Some(block.as_slice()),
                Some(props),
            )
            .expect("group fits");
        let actual = (writer.cursor() - before) as usize;
        assert_eq!(predicted, actual, "scatter_group_footprint must equal emitted bytes");
    };
    with_commit_indices(batch, schema, nw, emit);
}

#[test]
fn test_scatter_group_footprint_wire_safe_partitioned_and_empty_slots() {
    // A small partitioned batch over 4 workers leaves some worker slots empty
    // (schema-only), exercising the count-0 branch of the closed form.
    use gnitz_engine_testkit::{make_batch, make_schema_u64_i64};
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)]);
    unsafe { assert_footprint_exact(&schema, &batch, false, 4) };
}

#[test]
fn test_scatter_group_footprint_wire_safe_broadcast() {
    // Broadcast fills every worker slot with all rows (replicated family shape),
    // and the schema block is paid once per worker.
    use gnitz_engine_testkit::{make_batch, make_schema_u64_i64};
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    unsafe { assert_footprint_exact(&schema, &batch, true, 4) };
}

#[test]
fn test_scatter_group_footprint_non_wire_safe_shared_span_dedup() {
    // A string (non-wire-safe) batch whose two rows reference the SAME source
    // span: the per-worker sub-batch's BlobCache copies the span once, so
    // measuring the materialized sub-batch (not a naive per-row sum) is the only
    // byte-exact computation. Broadcast so both rows land in one slot.
    use gnitz_engine::storage::Batch;
    use gnitz_engine_testkit::make_schema_pk_u64_payload_string;
    use gnitz_wire::encode_german_string;
    let schema = make_schema_pk_u64_payload_string();
    let mut blob: Vec<u8> = Vec::new();
    let span = b"a long shared string span well over twelve bytes";
    let gs = encode_german_string(span, &mut blob); // appended once → shared offset

    let mut batch = Batch::with_capacity(schema, 2);
    for pk in [1u128, 2u128] {
        batch.extend_pk(pk);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &gs); // both rows reference the same source span
        batch.count += 1;
    }
    batch.blob = blob;
    unsafe { assert_footprint_exact(&schema, &batch, true, 3) };
}

/// `group_footprint_direct` must equal what `write_group_direct` consumes: the
/// exchange relay sizes its group before taking the SAL lock and writes it
/// after, and a prediction below the truth turns a reclaim-and-retry into a
/// fatal `SAL full` on a relay no worker can go without.
///
/// Both an empty slot (no rows → control block only) and a populated one are
/// covered, since only a populated slot pays the schema block and the data.
#[test]
fn test_group_footprint_direct_equals_emitted_bytes() {
    use crate::runtime::sal::DirectGroup;
    use crate::runtime::sal::FLAG_EXCHANGE_RELAY;
    use crate::runtime::wire::{WireData, WireMsg};
    use gnitz_engine_testkit::{make_batch, make_schema_u64_i64};

    let nw = 4;
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

    let size = 1 << 20;
    let region = SharedRegion::new(size);
    let writer = SalWriter::new(region.ptr(), -1, size as u64, nw);
    writer.reset(0, 1); // epoch >= 1 for sal_begin_group's debug_assert

    // Slot 2 stays empty: `relay_data` passes a dataless slot for a zero-row worker.
    let req_ids = [0u64; 4];
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
            schema: Some(&schema),
            ..Default::default()
        },
        worker_data: &worker_data,
        req_ids: &req_ids,
        unicast_worker: None,
    };

    let predicted = writer.group_footprint_direct(&group);
    let before = writer.cursor();
    writer
        .write_group_direct(&group, 0, FLAG_EXCHANGE_RELAY)
        .expect("group fits");
    let actual = (writer.cursor() - before) as usize;
    assert_eq!(predicted, actual, "group_footprint_direct must equal emitted bytes");
}

// ---------------------------------------------------------------------------
// The group header's own integrity: a digest over the header, seeded with the
// group's byte offset. Every one of the fields swept below is a routing or
// replay decision that was taken entirely on trust before.
// ---------------------------------------------------------------------------

/// `lsn`, `flags`, `target_id`, `slot_count`, the epoch word and every directory
/// entry live inside the digested span; the digest field itself is excluded from
/// it but is what the verdict compares against. One bit anywhere in the header
/// must therefore fail the probe.
#[test]
fn every_single_bit_flip_in_a_group_header_is_rejected() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let buf = make_test_data(0x5A, 64);
        let payloads: [&[u8]; 3] = [&buf, &[], &buf];
        sal_write_group(ptr, 0, 42, 100, FLAG_DDL_SYNC, 1, size as u64, &payloads).expect("group fits");

        let hdr_len = group_header_size(3);
        assert!(
            sal_probe_header(ptr, 0, size as u64).is_some(),
            "the clean header verifies"
        );

        let hdr = std::slice::from_raw_parts_mut(ptr.add(8), hdr_len);
        sweep_bit_flips(hdr, 0..hdr_len, |byte, bit, _| {
            assert!(
                sal_probe_header(ptr, 0, size as u64).is_none(),
                "header byte {byte} bit {bit} must fail the digest"
            );
        });
    }
}

/// The digest is seeded with the group's byte offset, which makes a valid header
/// self-locating: the resync scan trials every 8-byte-aligned word, so a header
/// that verified anywhere it was copied to would let the walk resume on the wrong
/// group.
#[test]
fn a_header_only_verifies_at_the_offset_it_was_published_at() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let buf = make_test_data(0x11, 64);
        let cur = sal_write_group(ptr, 0, 42, 100, 0, 1, size as u64, &[&buf]).expect("group fits");
        sal_write_group(ptr, cur, 43, 101, 0, 1, size as u64, &[&buf]).expect("group fits");

        let hdr_len = group_header_size(1);
        let b_hdr: Vec<u8> = std::slice::from_raw_parts(ptr.add(cur as usize + 8), hdr_len).to_vec();
        // Group B's whole header over group A's — same epoch, same shape, a
        // different address.
        std::ptr::copy_nonoverlapping(b_hdr.as_ptr(), ptr.add(8), hdr_len);
        assert!(
            sal_probe_header(ptr, 0, size as u64).is_none(),
            "a header published elsewhere must not verify here"
        );
    }
}

/// The probe's two mapping bounds protect different reads, and both must reject
/// before the read they guard. The region is mapped at exactly `size` bytes, so a
/// read past it faults rather than merely returning garbage.
#[test]
fn a_probe_never_reads_past_the_end_of_the_mapping() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        // A non-zero tail, so the prefix test does not answer for either fixture.
        std::ptr::write_bytes(ptr.add(size - 4096), 0xEE, 4096);

        // (a) The fixed part does not fit: `slot_count` would be read 16 bytes
        // past the mapping.
        assert!(
            sal_probe_header(ptr, (size - 8) as u64, size as u64).is_none(),
            "a candidate 8 bytes from the end must be rejected before the slot-count read"
        );

        // (b) The fixed part fits but a MAX_WORKERS directory does not.
        let base = size - 8 - group_header_size(0);
        write_u32_raw(ptr, base + 8 + 16, MAX_WORKERS as u32);
        assert!(
            sal_probe_header(ptr, base as u64, size as u64).is_none(),
            "a MAX_WORKERS directory that overruns the mapping must be rejected before the digest read"
        );
    }
}

/// The stride the reader derives from the authenticated directory must equal the
/// `payload_size` the writer put in the prefix, at every width — including one
/// with empty slots interleaved among non-empty ones, where `align8(0) = 0` is
/// what makes the derivation exact.
#[test]
fn the_derived_stride_equals_the_writers_payload_size_at_every_width() {
    unsafe {
        let size = 8 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        for &slots in &[1usize, 2, 4, MAX_WORKERS] {
            std::ptr::write_bytes(ptr, 0, size);
            let buf = make_test_data(0x33, 100);
            // Every other slot empty from slot 1 on, so the widths past 1 all carry
            // the interleaved shape.
            let payloads: Vec<&[u8]> = (0..slots)
                .map(|w| if w % 2 == 1 { &[][..] } else { buf.as_slice() })
                .collect();
            let cursor = sal_write_group(ptr, 0, 7, 9, 0, 1, size as u64, &payloads).expect("group fits");

            let (probed, epoch) = sal_probe_header(ptr, 0, size as u64).expect("header verifies");
            assert_eq!(probed as usize, slots);
            assert_eq!(epoch, 1);

            let word = atomic_load_u64(ptr);
            let payload_size = (word & 0xFFFF_FFFF) as usize;
            let (_, next) = group_at(ptr, 0, 0, size);
            assert_eq!(
                next as usize,
                8 + payload_size,
                "the derived stride must equal the prefix's payload_size at {slots} slots"
            );
            assert_eq!(next, cursor, "and the writer's own cursor advance");

            // The writer's own header arithmetic, which the committer's fit check
            // rests on, must agree with the layout byte for byte.
            let expected = group_header_size(slots) + payloads.iter().filter(|p| !p.is_empty()).count() * align8(100);
            assert_eq!(payload_size, expected, "group_header_size disagrees at {slots} slots");
        }
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
    let size = 1 << 20;
    let region = SharedRegion::new(size);
    let ptr = region.ptr();
    unsafe {
        sal_write_group(ptr, 0, 7, 11, 0, 1, size as u64, &[&[0u8; 32]]).expect("group fits");
        // Raise only the prefix's epoch copy: 1 -> 2, header untouched.
        let word = ptr as *mut u64;
        *word = (*word & 0xFFFF_FFFF) | (2u64 << 32);
    }
    let reader = SalReader::new(ptr as *const u8, 0, size, -1, 2);
    assert!(
        reader.next().is_none(),
        "a previous epoch's group must park, whatever its prefix claims"
    );
}

/// A digest mismatch under a passing epoch gate can only be corruption, and the
/// live drain fail-stops rather than reading it as end-of-log.
#[test]
fn the_live_path_aborts_on_a_damaged_header() {
    gnitz_engine_testkit::assert_test_aborts_134("the_live_path_aborts_on_a_damaged_header_internal", &[]);
}

/// Runs only in the re-exec'd abort child.
#[test]
fn the_live_path_aborts_on_a_damaged_header_internal() {
    if !gnitz_engine_testkit::in_abort_child() {
        return;
    }
    let size = 1 << 20;
    let region = SharedRegion::new(size);
    let ptr = region.ptr();
    unsafe {
        sal_write_group(ptr, 0, 7, 11, 0, 1, size as u64, &[&[0u8; 32]]).expect("group fits");
        // A sanity read before the damage, so the abort below is the damage.
        assert!(SalReader::new(ptr as *const u8, 0, size, -1, 1).next().is_some());
        *ptr.add(8) ^= 1;
    }
    let reader = SalReader::new(ptr as *const u8, 0, size, -1, 1);
    let _ = reader.next();
    unreachable!("a damaged header on the live path must fail-stop, not park");
}
