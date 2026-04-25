use crate::runtime::sal::{
    sal_write_group, sal_read_group_header, sal_begin_group, SalReader, SalWriter,
    MAX_WORKERS, FLAG_DDL_SYNC, FLAG_TXN_COMMIT,
    SAL_STATUS_HAS_DATA, SAL_STATUS_NO_DATA_FOR_WORKER, SAL_STATUS_NO_MESSAGE,
};
use crate::runtime::sys as ipc_sys;

unsafe fn alloc_mmap(size: usize) -> *mut u8 {
    let ptr = libc::mmap(
        std::ptr::null_mut(),
        size,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_ANONYMOUS | libc::MAP_SHARED,
        -1,
        0,
    );
    assert_ne!(ptr, libc::MAP_FAILED);
    std::ptr::write_bytes(ptr as *mut u8, 0, size);
    ptr as *mut u8
}

unsafe fn free_mmap(ptr: *mut u8, size: usize) {
    libc::munmap(ptr as *mut libc::c_void, size);
}

fn make_test_data(val: u8, len: usize) -> Vec<u8> {
    vec![val; len]
}

#[test]
fn test_sal_round_trip() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);

        let nw = 4u32;
        let bufs: Vec<Vec<u8>> = vec![
            make_test_data(0xAA, 100),
            vec![],
            make_test_data(0xBB, 200),
            make_test_data(0xCC, 50),
        ];

        let mut ptrs: Vec<*const u8> = Vec::new();
        let mut sizes: Vec<u32> = Vec::new();
        for b in &bufs {
            if b.is_empty() {
                ptrs.push(std::ptr::null());
                sizes.push(0);
            } else {
                ptrs.push(b.as_ptr());
                sizes.push(b.len() as u32);
            }
        }

        let res = sal_write_group(
            ptr, 0, nw, 42, 100, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(res.status, 0);
        assert!(res.new_cursor > 0);

        for w in 0..4u32 {
            let rr = sal_read_group_header(ptr, 0, w);
            assert_eq!(rr.lsn, 100);
            assert_eq!(rr.target_id, 42);
            assert_eq!(rr.epoch, 1);
            assert_eq!(rr.advance, res.new_cursor);

            if bufs[w as usize].is_empty() {
                assert_eq!(rr.status, SAL_STATUS_NO_DATA_FOR_WORKER);
            } else {
                assert_eq!(rr.status, SAL_STATUS_HAS_DATA);
                let data = std::slice::from_raw_parts(
                    rr.data_ptr, rr.data_size as usize
                );
                assert_eq!(data, bufs[w as usize].as_slice());
            }
        }

        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_unicast_isolation() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);
        let nw = 4u32;

        let buf = make_test_data(0xDD, 128);
        let mut ptrs = vec![std::ptr::null(); 4];
        let mut sizes = vec![0u32; 4];
        ptrs[2] = buf.as_ptr();
        sizes[2] = buf.len() as u32;

        let res = sal_write_group(
            ptr, 0, nw, 10, 1, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(res.status, 0);

        for w in [0u32, 1, 3] {
            let rr = sal_read_group_header(ptr, 0, w);
            assert_eq!(rr.status, SAL_STATUS_NO_DATA_FOR_WORKER);
            assert!(rr.advance > 0);
        }
        let rr = sal_read_group_header(ptr, 0, 2);
        assert_eq!(rr.status, SAL_STATUS_HAS_DATA);
        let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
        assert_eq!(data, buf.as_slice());

        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_multiple_groups() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);
        let nw = 2u32;

        let mut cursor = 0u64;
        for g in 0..3u64 {
            let buf = make_test_data((g + 1) as u8, 64);
            let ptrs = [buf.as_ptr(), std::ptr::null()];
            let sizes = [buf.len() as u32, 0u32];

            let res = sal_write_group(
                ptr, cursor, nw, g as u32, g * 10, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            assert_eq!(res.status, 0);
            cursor = res.new_cursor;
        }

        let mut rc = 0u64;
        for g in 0..3u64 {
            let rr = sal_read_group_header(ptr, rc, 0);
            assert_eq!(rr.status, SAL_STATUS_HAS_DATA);
            assert_eq!(rr.lsn, g * 10);
            assert_eq!(rr.target_id, g as u32);
            let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
            assert_eq!(data, vec![(g + 1) as u8; 64].as_slice());
            rc += rr.advance;
        }
        let rr = sal_read_group_header(ptr, rc, 0);
        assert_eq!(rr.status, SAL_STATUS_NO_MESSAGE);

        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_epoch_write_read() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);

        let buf = make_test_data(0x11, 32);
        let ptrs = [buf.as_ptr()];
        let sizes = [buf.len() as u32];

        let res = sal_write_group(
            ptr, 0, 1, 0, 0, 0, 42, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(res.status, 0);

        let rr = sal_read_group_header(ptr, 0, 0);
        assert_eq!(rr.epoch, 42);

        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_full_error() {
    unsafe {
        let size = 256;
        let ptr = alloc_mmap(size);

        let buf = make_test_data(0xFF, 100);
        let ptrs = [buf.as_ptr()];
        let sizes = [buf.len() as u32];

        let res = sal_write_group(
            ptr, 0, 1, 0, 0, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(res.status, -1);

        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_cross_process() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);
        let efd = ipc_sys::eventfd_create();
        assert!(efd >= 0);

        let pid = libc::fork();
        if pid == 0 {
            let buf = make_test_data(0x77, 128);
            let ptrs = [buf.as_ptr()];
            let sizes = [buf.len() as u32];
            sal_write_group(
                ptr, 0, 1, 99, 555, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            ipc_sys::eventfd_signal(efd);
            libc::_exit(0);
        }

        let r = ipc_sys::eventfd_wait(efd, 5000);
        assert!(r > 0, "eventfd timed out");

        let rr = sal_read_group_header(ptr, 0, 0);
        assert_eq!(rr.status, SAL_STATUS_HAS_DATA);
        assert_eq!(rr.lsn, 555);
        assert_eq!(rr.target_id, 99);
        let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
        assert_eq!(data, vec![0x77u8; 128].as_slice());

        let mut status = 0i32;
        libc::waitpid(pid, &mut status, 0);
        free_mmap(ptr, size);
        libc::close(efd);
    }
}

#[test]
fn test_sal_checkpoint_reset() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);

        let buf1 = make_test_data(0x11, 32);
        let ptrs = [buf1.as_ptr()];
        let sizes = [buf1.len() as u32];
        let res = sal_write_group(
            ptr, 0, 1, 0, 0, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(res.status, 0);

        std::ptr::write_bytes(ptr, 0, size);
        let buf2 = make_test_data(0x22, 32);
        let ptrs2 = [buf2.as_ptr()];
        let sizes2 = [buf2.len() as u32];
        let res2 = sal_write_group(
            ptr, 0, 1, 0, 0, 0, 2, size as u64,
            ptrs2.as_ptr(), sizes2.as_ptr(),
        );
        assert_eq!(res2.status, 0);

        let rr = sal_read_group_header(ptr, 0, 0);
        assert_eq!(rr.epoch, 2);
        let data = std::slice::from_raw_parts(rr.data_ptr, rr.data_size as usize);
        assert_eq!(data, vec![0x22u8; 32].as_slice());

        free_mmap(ptr, size);
    }
}

#[test]
fn sal_begin_group_rejects_too_many_workers() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);
        let sizes = [0u32; MAX_WORKERS + 1];
        // num_workers = MAX_WORKERS + 1 must return None.
        let result = sal_begin_group(
            ptr, 0, size,
            MAX_WORKERS + 1, 0, 0, 0, 1,
            &sizes[..MAX_WORKERS + 1],
        );
        assert!(result.is_none(), "sal_begin_group must reject num_workers > MAX_WORKERS");
        free_mmap(ptr, size);
    }
}

#[test]
fn sal_begin_group_rejects_cursor_overflow() {
    unsafe {
        let size = 512usize;
        let ptr = alloc_mmap(size);
        // Push the cursor so close to the end that the group header won't fit.
        let sizes = [0u32; 1];
        let result = sal_begin_group(
            ptr, size - 1, size,
            1, 0, 0, 0, 1,
            &sizes[..1],
        );
        assert!(result.is_none(), "sal_begin_group must reject when cursor + total > mmap_size");
        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_epoch_fence() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);

        let buf = make_test_data(0x33, 32);
        let ptrs = [buf.as_ptr()];
        let sizes = [buf.len() as u32];

        let res1 = sal_write_group(
            ptr, 0, 1, 0, 0, 0, 5, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        let res2 = sal_write_group(
            ptr, res1.new_cursor, 1, 0, 0, 0, 6, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(res2.status, 0);

        let rr1 = sal_read_group_header(ptr, 0, 0);
        assert_eq!(rr1.epoch, 5);
        let rr2 = sal_read_group_header(ptr, rr1.advance, 0);
        assert_eq!(rr2.epoch, 6);

        free_mmap(ptr, size);
    }
}

#[test]
fn test_commit_sentinel_round_trip() {
    // Zone shape: two normal groups at lsn=K, one sentinel at lsn=K,
    // then an unrelated group at lsn=K+1. After read-back the third
    // group must be the only one with FLAG_TXN_COMMIT set.
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);
        let nw = 2u32;

        // Two normal groups at the same LSN.
        let buf = make_test_data(0xAA, 32);
        let ptrs = [buf.as_ptr(), buf.as_ptr()];
        let sizes = [buf.len() as u32, buf.len() as u32];
        let r1 = sal_write_group(
            ptr, 0, nw, 100, 7, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(r1.status, 0);
        let r2 = sal_write_group(
            ptr, r1.new_cursor, nw, 101, 7, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(r2.status, 0);

        // Sentinel via SalWriter at the same LSN. m2w_efds empty so
        // signal_all is a no-op (we never call it here anyway, but its
        // presence in SalWriter::new requires the vec).
        let efd1 = ipc_sys::eventfd_create();
        let efd2 = ipc_sys::eventfd_create();
        assert!(efd1 >= 0 && efd2 >= 0);
        let mut writer = SalWriter::new(ptr, -1, size as u64, vec![efd1, efd2]);
        writer.reset(r2.new_cursor, 1);
        writer.write_commit_sentinel(7).unwrap();

        // Trailing group at the next LSN.
        let r4 = sal_write_group(
            ptr, writer.cursor(), nw, 102, 8, 0, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(r4.status, 0);

        // Walk the SAL via SalReader (worker 0 perspective).
        let reader = SalReader::new(ptr as *const u8, 0, size, efd1);
        let (m1, c1) = reader.try_read(0).unwrap();
        let (m2, c2) = reader.try_read(c1).unwrap();
        let (m3, c3) = reader.try_read(c2).unwrap();
        let (m4, _)  = reader.try_read(c3).unwrap();

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
        free_mmap(ptr, size);
    }
}

#[test]
fn test_commit_sentinel_zero_payload() {
    // The sentinel must produce wire_data=None for every worker — it is
    // a header-only group and must not be misread as a DDL_SYNC batch.
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);

        let efd1 = ipc_sys::eventfd_create();
        let efd2 = ipc_sys::eventfd_create();
        let efd3 = ipc_sys::eventfd_create();
        let efd4 = ipc_sys::eventfd_create();
        assert!(efd1 >= 0 && efd2 >= 0 && efd3 >= 0 && efd4 >= 0);
        let mut writer = SalWriter::new(ptr, -1, size as u64, vec![efd1, efd2, efd3, efd4]);
        writer.reset(0, 1);
        writer.write_commit_sentinel(123).unwrap();

        for w in 0..4 {
            let reader = SalReader::new(ptr as *const u8, w, size, efd1);
            let (msg, _) = reader.try_read(0).unwrap();
            assert_eq!(msg.lsn, 123);
            assert!(msg.wire_data.is_none(),
                "sentinel must carry no per-worker payload for worker {}", w);
        }

        libc::close(efd1);
        libc::close(efd2);
        libc::close(efd3);
        libc::close(efd4);
        free_mmap(ptr, size);
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
        let ptr = alloc_mmap(size);
        let nw = 2u32;
        let zone_lsn = 42u64;

        let payload = make_test_data(0xDD, 64);
        let ptrs: Vec<*const u8> = (0..nw).map(|_| payload.as_ptr()).collect();
        let sizes: Vec<u32> = (0..nw).map(|_| payload.len() as u32).collect();
        // Two push groups at the same LSN.
        let r1 = sal_write_group(
            ptr, 0, nw, 1000, zone_lsn, FLAG_PUSH, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(r1.status, 0);
        let r2 = sal_write_group(
            ptr, r1.new_cursor, nw, 1001, zone_lsn, FLAG_PUSH, 1, size as u64,
            ptrs.as_ptr(), sizes.as_ptr(),
        );
        assert_eq!(r2.status, 0);

        // Closing sentinel.
        let efds: Vec<i32> = (0..nw).map(|_| ipc_sys::eventfd_create()).collect();
        let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
        writer.reset(r2.new_cursor, 1);
        writer.write_commit_sentinel(zone_lsn).unwrap();

        let reader = SalReader::new(ptr as *const u8, 0, size, efds[0]);
        let (m1, c1) = reader.try_read(0).unwrap();
        let (m2, c2) = reader.try_read(c1).unwrap();
        let (m3, _)  = reader.try_read(c2).unwrap();
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
                let (msg, next) = match reader.try_read(off) {
                    Some(v) => v, None => break,
                };
                off = next;
                if msg.flags & FLAG_TXN_COMMIT != 0 { set.insert(msg.lsn); }
            }
            set
        };
        assert!(committed.contains(&zone_lsn),
            "sentinel must commit the push zone");

        for &e in &efds { libc::close(e); }
        free_mmap(ptr, size);
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
        let ptr = alloc_mmap(size);
        let nw = 4u32;

        let buf_col = make_test_data(0xC0, 64);
        let buf_tab = make_test_data(0x7A, 96);
        let zone_lsn = 17u64;

        // Group 1: COL_TAB rows.
        let ptrs_col: Vec<*const u8> = (0..nw).map(|_| buf_col.as_ptr()).collect();
        let sizes_col: Vec<u32> = (0..nw).map(|_| buf_col.len() as u32).collect();
        let r1 = sal_write_group(
            ptr, 0, nw, 200, zone_lsn, 0, 1, size as u64,
            ptrs_col.as_ptr(), sizes_col.as_ptr(),
        );
        assert_eq!(r1.status, 0);

        // Group 2: TABLE_TAB row, same LSN.
        let ptrs_tab: Vec<*const u8> = (0..nw).map(|_| buf_tab.as_ptr()).collect();
        let sizes_tab: Vec<u32> = (0..nw).map(|_| buf_tab.len() as u32).collect();
        let r2 = sal_write_group(
            ptr, r1.new_cursor, nw, 201, zone_lsn, 0, 1, size as u64,
            ptrs_tab.as_ptr(), sizes_tab.as_ptr(),
        );
        assert_eq!(r2.status, 0);

        // Sentinel.
        let efds: Vec<i32> = (0..nw).map(|_| ipc_sys::eventfd_create()).collect();
        for &e in &efds { assert!(e >= 0); }
        let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
        writer.reset(r2.new_cursor, 1);
        writer.write_commit_sentinel(zone_lsn).unwrap();

        // Walk on every worker's perspective; assert zone shape.
        for w in 0..nw {
            let reader = SalReader::new(ptr as *const u8, w, size, efds[w as usize]);
            let (m1, c1) = reader.try_read(0).unwrap();
            let (m2, c2) = reader.try_read(c1).unwrap();
            let (m3, _)  = reader.try_read(c2).unwrap();
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

        for &e in &efds { libc::close(e); }
        free_mmap(ptr, size);
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
        let ptr = alloc_mmap(size);
        let nw = 1u32;

        let buf1 = make_test_data(0x10, 32);
        let ptrs1 = [buf1.as_ptr()];
        let sizes1 = [buf1.len() as u32];
        let res1 = sal_write_group(
            ptr, 0, nw, 7, 42, 0, 1, size as u64,
            ptrs1.as_ptr(), sizes1.as_ptr(),
        );
        assert_eq!(res1.status, 0);

        let buf2 = make_test_data(0x20, 32);
        let ptrs2 = [buf2.as_ptr()];
        let sizes2 = [buf2.len() as u32];
        let res2 = sal_write_group(
            ptr, res1.new_cursor, nw, 8, 42, 0, 1, size as u64,
            ptrs2.as_ptr(), sizes2.as_ptr(),
        );
        assert_eq!(res2.status, 0);

        let rr1 = sal_read_group_header(ptr, 0, 0);
        let rr2 = sal_read_group_header(ptr, rr1.advance, 0);
        assert_eq!(rr1.lsn, 42);
        assert_eq!(rr2.lsn, 42);
        assert_eq!(rr1.target_id, 7);
        assert_eq!(rr2.target_id, 8);

        free_mmap(ptr, size);
    }
}

#[test]
fn test_sal_cross_process_checkpoint() {
    unsafe {
        let size = 1 << 20;
        let ptr = alloc_mmap(size);
        let efd = ipc_sys::eventfd_create();
        let efd2 = ipc_sys::eventfd_create();
        assert!(efd >= 0 && efd2 >= 0);

        let pid = libc::fork();
        if pid == 0 {
            let buf = make_test_data(0xAA, 64);
            let ptrs = [buf.as_ptr()];
            let sizes = [buf.len() as u32];
            sal_write_group(
                ptr, 0, 1, 0, 10, 0, 1, size as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            );
            ipc_sys::eventfd_signal(efd);

            ipc_sys::eventfd_wait(efd2, 5000);

            std::ptr::write_bytes(ptr, 0, size);
            let buf2 = make_test_data(0xBB, 64);
            let ptrs2 = [buf2.as_ptr()];
            let sizes2 = [buf2.len() as u32];
            sal_write_group(
                ptr, 0, 1, 0, 20, 0, 2, size as u64,
                ptrs2.as_ptr(), sizes2.as_ptr(),
            );
            ipc_sys::eventfd_signal(efd);
            libc::_exit(0);
        }

        ipc_sys::eventfd_wait(efd, 5000);
        let rr1 = sal_read_group_header(ptr, 0, 0);
        assert_eq!(rr1.epoch, 1);
        assert_eq!(rr1.lsn, 10);
        ipc_sys::eventfd_signal(efd2);

        ipc_sys::eventfd_wait(efd, 5000);
        let rr2 = sal_read_group_header(ptr, 0, 0);
        assert_eq!(rr2.epoch, 2);
        assert_eq!(rr2.lsn, 20);

        let mut status = 0i32;
        libc::waitpid(pid, &mut status, 0);
        free_mmap(ptr, size);
        libc::close(efd);
        libc::close(efd2);
    }
}
