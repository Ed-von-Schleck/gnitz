use crate::runtime::sal::{
    sal_write_group, sal_read_group_header,
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
