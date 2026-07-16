use crate::foundation::posix_io;
use crate::runtime::sal::{
    atomic_load_u64, sal_begin_group, sal_read_group_header, sal_write_group, SalReader, SalWriter, FLAG_DDL_SYNC,
    FLAG_TXN_COMMIT, GROUP_HEADER_SIZE, MAX_WORKERS,
};
use crate::test_support::SharedRegion;

fn make_test_data(val: u8, len: usize) -> Vec<u8> {
    vec![val; len]
}

#[test]
fn test_sal_round_trip() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let nw = 4u32;
        let bufs: Vec<Vec<u8>> = vec![
            make_test_data(0xAA, 100),
            vec![],
            make_test_data(0xBB, 200),
            make_test_data(0xCC, 50),
        ];

        let payloads: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
        let new_cursor = sal_write_group(ptr, 0, 42, 100, 0, 1, size as u64, &payloads).expect("group fits");
        assert!(new_cursor > 0);
        let _ = nw;

        for w in 0..4u32 {
            let rr = sal_read_group_header(ptr, 0, w, None).expect("group present");
            assert_eq!(rr.lsn, 100);
            assert_eq!(rr.target_id, 42);
            assert_eq!(rr.epoch, 1);
            assert_eq!(rr.advance, new_cursor);

            if bufs[w as usize].is_empty() {
                assert!(rr.data_ptr.is_null(), "no data slot for worker {w}");
            } else {
                assert!(!rr.data_ptr.is_null());
                let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
                assert_eq!(data, bufs[w as usize].as_slice());
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
        let nw = 4u32;

        let buf = make_test_data(0xDD, 128);
        let payloads: [&[u8]; 4] = [&[], &[], &buf, &[]];

        sal_write_group(ptr, 0, 10, 1, 0, 1, size as u64, &payloads).expect("group fits");
        let _ = nw;

        for w in [0u32, 1, 3] {
            let rr = sal_read_group_header(ptr, 0, w, None).expect("group present");
            assert!(rr.data_ptr.is_null(), "no data slot for worker {w}");
            assert!(rr.advance > 0);
        }
        let rr = sal_read_group_header(ptr, 0, 2, None).expect("group present");
        assert!(!rr.data_ptr.is_null());
        let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
        assert_eq!(data, buf.as_slice());
    }
}

#[test]
fn test_sal_multiple_groups() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let nw = 2u32;

        let mut cursor = 0u64;
        for g in 0..3u64 {
            let buf = make_test_data((g + 1) as u8, 64);
            let payloads: [&[u8]; 2] = [&buf, &[]];
            cursor = sal_write_group(ptr, cursor, g as u32, g * 10, 0, 1, size as u64, &payloads).expect("group fits");
        }
        let _ = nw;

        let mut rc = 0u64;
        for g in 0..3u64 {
            let rr = sal_read_group_header(ptr, rc, 0, None).expect("group present");
            assert!(!rr.data_ptr.is_null());
            assert_eq!(rr.lsn, g * 10);
            assert_eq!(rr.target_id, g as u32);
            let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
            assert_eq!(data, vec![(g + 1) as u8; 64].as_slice());
            rc += rr.advance;
        }
        assert!(sal_read_group_header(ptr, rc, 0, None).is_none());
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

        let rr = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert_eq!(rr.epoch, 42);
    }
}

#[test]
fn test_sal_full_error() {
    unsafe {
        let size = 256;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let buf = make_test_data(0xFF, 100);
        assert!(
            sal_write_group(ptr, 0, 0, 0, 0, 1, size as u64, &[&buf]).is_none(),
            "group larger than the mmap must be rejected"
        );
    }
}

#[test]
fn test_sal_cross_process() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let efd = posix_io::eventfd_create();
        assert!(efd >= 0);

        let pid = libc::fork();
        if pid == 0 {
            let buf = make_test_data(0x77, 128);
            sal_write_group(ptr, 0, 99, 555, 0, 1, size as u64, &[&buf]).expect("group fits");
            posix_io::eventfd_signal(efd);
            libc::_exit(0);
        }

        let r = posix_io::eventfd_wait(efd, 5000);
        assert!(r > 0, "eventfd timed out");

        let rr = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert!(!rr.data_ptr.is_null());
        assert_eq!(rr.lsn, 555);
        assert_eq!(rr.target_id, 99);
        let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
        assert_eq!(data, vec![0x77u8; 128].as_slice());

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

        let rr = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert_eq!(rr.epoch, 2);
        let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
        assert_eq!(data, vec![0x22u8; 32].as_slice());
    }
}

#[test]
fn sal_begin_group_rejects_too_many_workers() {
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        let sizes = [0u32; MAX_WORKERS + 1];
        // num_workers = MAX_WORKERS + 1 must return None.
        let result = sal_begin_group(ptr, 0, size, MAX_WORKERS + 1, 0, 0, 0, 1, &sizes[..MAX_WORKERS + 1]);
        assert!(
            result.is_none(),
            "sal_begin_group must reject num_workers > MAX_WORKERS"
        );
    }
}

#[test]
fn sal_begin_group_rejects_cursor_overflow() {
    unsafe {
        let size = 512usize;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();
        // Push the cursor so close to the end that the group header won't fit.
        let sizes = [0u32; 1];
        let result = sal_begin_group(ptr, size - 1, size, 1, 0, 0, 0, 1, &sizes[..1]);
        assert!(
            result.is_none(),
            "sal_begin_group must reject when cursor + total > mmap_size"
        );
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

        let rr1 = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert_eq!(rr1.epoch, 5);
        let rr2 = sal_read_group_header(ptr, rr1.advance, 0, None).expect("group present");
        assert_eq!(rr2.epoch, 6);
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
        let nw = 2u32;

        // Two normal groups at the same LSN.
        let buf = make_test_data(0xAA, 32);
        let payloads: [&[u8]; 2] = [&buf, &buf];
        let c1 = sal_write_group(ptr, 0, 100, 7, 0, 1, size as u64, &payloads).expect("group fits");
        let c2 = sal_write_group(ptr, c1, 101, 7, 0, 1, size as u64, &payloads).expect("group fits");
        let _ = nw;

        // Sentinel via SalWriter at the same LSN. m2w_efds empty so
        // signal_all is a no-op (we never call it here anyway, but its
        // presence in SalWriter::new requires the vec).
        let efd1 = posix_io::eventfd_create();
        let efd2 = posix_io::eventfd_create();
        assert!(efd1 >= 0 && efd2 >= 0);
        let mut writer = SalWriter::new(ptr, -1, size as u64, vec![efd1, efd2]);
        writer.reset(c2, 1);
        writer.write_commit_sentinel(7).unwrap();

        // Trailing group at the next LSN.
        sal_write_group(ptr, writer.cursor(), 102, 8, 0, 1, size as u64, &payloads).expect("group fits");

        // Walk the SAL via SalReader (worker 0 perspective).
        let reader = SalReader::new(ptr as *const u8, 0, size, efd1);
        let (m1, c1) = reader.try_read(0, None).unwrap();
        let (m2, c2) = reader.try_read(c1, None).unwrap();
        let (m3, c3) = reader.try_read(c2, None).unwrap();
        let (m4, _) = reader.try_read(c3, None).unwrap();

        assert_eq!(m1.lsn, 7);
        assert_eq!(m2.lsn, 7);
        assert_eq!(m3.lsn, 7);
        assert_eq!(m4.lsn, 8);
        assert_eq!(m1.flags & FLAG_TXN_COMMIT, 0);
        assert_eq!(m2.flags & FLAG_TXN_COMMIT, 0);
        assert_eq!(m3.flags & FLAG_TXN_COMMIT, FLAG_TXN_COMMIT);
        assert_eq!(m3.flags & FLAG_DDL_SYNC, FLAG_DDL_SYNC);
        assert_eq!(m4.flags & FLAG_TXN_COMMIT, 0);

        libc::close(efd1);
        libc::close(efd2);
    }
}

#[test]
fn test_commit_sentinel_zero_payload() {
    // The sentinel must produce wire_data=None for every worker — it is
    // a header-only group and must not be misread as a DDL_SYNC batch.
    unsafe {
        let size = 1 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        let efd1 = posix_io::eventfd_create();
        let efd2 = posix_io::eventfd_create();
        let efd3 = posix_io::eventfd_create();
        let efd4 = posix_io::eventfd_create();
        assert!(efd1 >= 0 && efd2 >= 0 && efd3 >= 0 && efd4 >= 0);
        let mut writer = SalWriter::new(ptr, -1, size as u64, vec![efd1, efd2, efd3, efd4]);
        writer.reset(0, 1);
        writer.write_commit_sentinel(123).unwrap();

        for w in 0..4 {
            let reader = SalReader::new(ptr as *const u8, w, size, efd1);
            let (msg, _) = reader.try_read(0, None).unwrap();
            assert_eq!(msg.lsn, 123);
            assert!(
                msg.wire_data.is_none(),
                "sentinel must carry no per-worker payload for worker {w}"
            );
        }

        libc::close(efd1);
        libc::close(efd2);
        libc::close(efd3);
        libc::close(efd4);
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
        let nw = 2u32;
        let zone_lsn = 42u64;

        let payload = make_test_data(0xDD, 64);
        let payloads: Vec<&[u8]> = (0..nw).map(|_| payload.as_slice()).collect();
        // Two push groups at the same LSN.
        let c1 = sal_write_group(ptr, 0, 1000, zone_lsn, FLAG_PUSH, 1, size as u64, &payloads).expect("group fits");
        let c2 = sal_write_group(ptr, c1, 1001, zone_lsn, FLAG_PUSH, 1, size as u64, &payloads).expect("group fits");

        // Closing sentinel.
        let efds: Vec<i32> = (0..nw).map(|_| posix_io::eventfd_create()).collect();
        let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
        writer.reset(c2, 1);
        writer.write_commit_sentinel(zone_lsn).unwrap();

        let reader = SalReader::new(ptr as *const u8, 0, size, efds[0]);
        let (m1, c1) = reader.try_read(0, None).unwrap();
        let (m2, c2) = reader.try_read(c1, None).unwrap();
        let (m3, _) = reader.try_read(c2, None).unwrap();
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
                let (msg, next) = match reader.try_read(off, None) {
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

        for &e in &efds {
            libc::close(e);
        }
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
        let efds: Vec<i32> = (0..nw).map(|_| posix_io::eventfd_create()).collect();
        for &e in &efds {
            assert!(e >= 0);
        }
        let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
        writer.reset(c2, 1);
        writer.write_commit_sentinel(zone_lsn).unwrap();

        // Walk on every worker's perspective; assert zone shape.
        for w in 0..nw {
            let reader = SalReader::new(ptr as *const u8, w, size, efds[w as usize]);
            let (m1, c1) = reader.try_read(0, None).unwrap();
            let (m2, c2) = reader.try_read(c1, None).unwrap();
            let (m3, _) = reader.try_read(c2, None).unwrap();
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

        for &e in &efds {
            libc::close(e);
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
        let nw = 1u32;

        let buf1 = make_test_data(0x10, 32);
        let c1 = sal_write_group(ptr, 0, 7, 42, 0, 1, size as u64, &[&buf1]).expect("group fits");
        let _ = nw;

        let buf2 = make_test_data(0x20, 32);
        sal_write_group(ptr, c1, 8, 42, 0, 1, size as u64, &[&buf2]).expect("group fits");

        let rr1 = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        let rr2 = sal_read_group_header(ptr, rr1.advance, 0, None).expect("group present");
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
        let efd = posix_io::eventfd_create();
        let efd2 = posix_io::eventfd_create();
        assert!(efd >= 0 && efd2 >= 0);

        let pid = libc::fork();
        if pid == 0 {
            let buf = make_test_data(0xAA, 64);
            sal_write_group(ptr, 0, 0, 10, 0, 1, size as u64, &[&buf]).expect("group fits");
            posix_io::eventfd_signal(efd);

            posix_io::eventfd_wait(efd2, 5000);

            std::ptr::write_bytes(ptr, 0, size);
            let buf2 = make_test_data(0xBB, 64);
            sal_write_group(ptr, 0, 0, 20, 0, 2, size as u64, &[&buf2]).expect("group fits");
            posix_io::eventfd_signal(efd);
            libc::_exit(0);
        }

        posix_io::eventfd_wait(efd, 5000);
        let rr1 = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert_eq!(rr1.epoch, 1);
        assert_eq!(rr1.lsn, 10);
        posix_io::eventfd_signal(efd2);

        posix_io::eventfd_wait(efd, 5000);
        let rr2 = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert_eq!(rr2.epoch, 2);
        assert_eq!(rr2.lsn, 20);

        let mut status = 0i32;
        libc::waitpid(pid, &mut status, 0);
        libc::close(efd);
        libc::close(efd2);
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

        let reader = SalReader::new(ptr as *const u8, 0, size, -1);

        // Wrong expected epoch: parked without touching header bytes.
        assert!(
            reader.try_read(0, Some(2)).is_none(),
            "epoch-mismatched slot must park the reader"
        );

        // Matching epoch: consumable.
        let (msg, cursor) = reader.try_read(0, Some(1)).expect("matching epoch reads the group");
        assert_eq!(msg.epoch, 1);
        assert_eq!(msg.lsn, 11);
        assert_eq!(msg.target_id, 7);
        assert_eq!(cursor, new_cursor);

        // Ungated recovery read: consumable without knowing the epoch.
        let (msg, _) = reader
            .try_read(0, None)
            .expect("recovery walk reads without an expectation");
        assert_eq!(msg.epoch, 1);
    }
}

#[test]
fn test_sal_prefix_packing_boundaries() {
    // The (epoch << 32 | payload_size) prefix word must round-trip at the
    // boundaries: a header-only group (payload_size == GROUP_HEADER_SIZE)
    // with epoch u32::MAX, and a multi-MiB group with epoch 1.
    unsafe {
        let size = 8 << 20;
        let region = SharedRegion::new(size);
        let ptr = region.ptr();

        // Header-only group (no worker data), epoch u32::MAX.
        let empty: [&[u8]; 1] = [&[]];
        let new_cursor = sal_write_group(ptr, 0, 0, 0, 0, u32::MAX, size as u64, &empty).expect("group fits");
        let word = atomic_load_u64(ptr);
        assert_eq!((word >> 32) as u32, u32::MAX);
        assert_eq!((word & 0xFFFF_FFFF) as usize, GROUP_HEADER_SIZE);
        let rr = sal_read_group_header(ptr, 0, 0, None).expect("group present");
        assert_eq!(rr.epoch, u32::MAX);
        assert_eq!(rr.advance, (8 + GROUP_HEADER_SIZE) as u64);
        assert_eq!(new_cursor, rr.advance);

        // Multi-MiB group, epoch 1.
        std::ptr::write_bytes(ptr, 0, size);
        let big = make_test_data(0xEE, 3 << 20);
        sal_write_group(ptr, 0, 0, 0, 0, 1, size as u64, &[&big]).expect("group fits");
        let expected_payload = GROUP_HEADER_SIZE + (3 << 20); // 3 MiB is already 8-aligned
        let word = atomic_load_u64(ptr);
        assert_eq!((word >> 32) as u32, 1);
        assert_eq!((word & 0xFFFF_FFFF) as usize, expected_payload);
        let rr = sal_read_group_header(ptr, 0, 0, Some(1)).expect("group present");
        assert!(!rr.data_ptr.is_null());
        assert_eq!(rr.epoch, 1);
        assert_eq!(rr.advance, (8 + expected_payload) as u64);
        assert_eq!(rr.data_size as usize, 3 << 20);
    }
}

// ---------------------------------------------------------------------------
// wire_group_footprint exactness (load-bearing: the committer's per-transaction
// fit check and Phase-B fail-stop depend on it equaling the bytes emission
// actually consumes).
// ---------------------------------------------------------------------------

/// Emit `batch` (partitioned or broadcast) through `scatter_wire_group` and
/// assert the cursor advance equals `wire_group_footprint`'s prediction.
unsafe fn assert_footprint_exact(
    schema: &crate::schema::SchemaDescriptor,
    batch: &crate::storage::Batch,
    broadcast: bool,
    nw: usize,
) {
    use crate::ops::{with_broadcast_indices, with_worker_indices};
    use crate::runtime::sal::FLAG_PUSH;
    use crate::runtime::wire::build_schema_wire_block;
    use crate::storage::compute_wire_props;

    let size = 1 << 20;
    let region = SharedRegion::new(size);
    let ptr = region.ptr();
    let efds: Vec<i32> = (0..nw).map(|_| posix_io::eventfd_create()).collect();
    let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
    writer.reset(0, 1); // epoch >= 1 for sal_begin_group's debug_assert

    let target_id = 16u32;
    let block = build_schema_wire_block(schema, &[], 0, target_id);
    let props = compute_wire_props(schema);
    let req_ids: Vec<u64> = (0..nw as u64).map(|i| i + 1).collect();

    let emit = |wi: &[Vec<u32>]| {
        let predicted = writer.wire_group_footprint(batch, wi, schema, block.len(), props);
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
        assert_eq!(predicted, actual, "wire_group_footprint must equal emitted bytes");
    };
    if broadcast {
        with_broadcast_indices(batch, nw, emit);
    } else {
        with_worker_indices(batch, schema, nw, emit);
    }
    for &e in &efds {
        libc::close(e);
    }
}

#[test]
fn test_wire_group_footprint_wire_safe_partitioned_and_empty_slots() {
    // A small partitioned batch over 4 workers leaves some worker slots empty
    // (schema-only), exercising the count-0 branch of the closed form.
    use crate::test_support::{make_batch, make_schema_u64_i64};
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)]);
    unsafe { assert_footprint_exact(&schema, &batch, false, 4) };
}

#[test]
fn test_wire_group_footprint_wire_safe_broadcast() {
    // Broadcast fills every worker slot with all rows (replicated family shape),
    // and the schema block is paid once per worker.
    use crate::test_support::{make_batch, make_schema_u64_i64};
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    unsafe { assert_footprint_exact(&schema, &batch, true, 4) };
}

#[test]
fn test_wire_group_footprint_non_wire_safe_shared_span_dedup() {
    // A string (non-wire-safe) batch whose two rows reference the SAME source
    // span: the per-worker sub-batch's BlobCache copies the span once, so
    // measuring the materialized sub-batch (not a naive per-row sum) is the only
    // byte-exact computation. Broadcast so both rows land in one slot.
    use crate::storage::Batch;
    use crate::test_support::{german_string, make_schema_pk_u64_payload_string};
    let schema = make_schema_pk_u64_payload_string();
    let mut blob: Vec<u8> = Vec::new();
    let span = b"a long shared string span well over twelve bytes";
    let gs = german_string(span, &mut blob); // appended once → shared offset

    let mut batch = Batch::with_schema(schema, 2);
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
