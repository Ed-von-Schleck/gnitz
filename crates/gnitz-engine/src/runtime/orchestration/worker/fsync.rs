//! io_uring batched fdatasync: the `uring_batch_fdatasync` durability primitive.

/// Submit one FSYNC(DATASYNC) SQE per fd and await completion of all of them.
/// Drains the SQ when full so a chunk of more than `sq_entries` fds requires
/// multiple `submit_and_wait` calls.
pub(crate) fn uring_batch_fdatasync(ring: &mut io_uring::IoUring, fds: &[libc::c_int]) -> Result<(), String> {
    uring_batch_fdatasync_with(ring, fds, |r, want| r.submit_and_wait(want))
}

fn uring_batch_fdatasync_with(
    ring: &mut io_uring::IoUring,
    fds: &[libc::c_int],
    mut submit: impl FnMut(&mut io_uring::IoUring, usize) -> std::io::Result<usize>,
) -> Result<(), String> {
    if fds.is_empty() {
        return Ok(());
    }
    let sq_capacity = ring.params().sq_entries() as usize;
    let mut completed = 0usize;
    let mut pushed = 0usize;
    while completed < fds.len() {
        while pushed < fds.len() {
            if ring.submission().len() >= sq_capacity {
                break;
            }
            let sqe = io_uring::opcode::Fsync::new(io_uring::types::Fd(fds[pushed]))
                .flags(io_uring::types::FsyncFlags::DATASYNC)
                .build();
            unsafe {
                ring.submission().push(&sqe).map_err(|_| "uring SQ push".to_string())?;
            }
            pushed += 1;
        }
        let want = pushed - completed;
        match submit(ring, want) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => {
                // Drain any CQEs that arrived before the signal.
                for cqe in ring.completion() {
                    if cqe.result() < 0 {
                        return Err(format!("fdatasync via uring failed: {}", cqe.result()));
                    }
                    completed += 1;
                }
                continue;
            }
            Err(e) => return Err(format!("uring submit_and_wait: {e}")),
        }
        for cqe in ring.completion() {
            if cqe.result() < 0 {
                return Err(format!("fdatasync via uring failed: {}", cqe.result()));
            }
            completed += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an io_uring ring, or return `None` if the platform denies the
    /// syscall (no io_uring support, no CAP_SYS_ADMIN, or AppArmor/seccomp
    /// restriction) so the caller can skip rather than panic.
    fn try_new_ring(entries: u32) -> Option<io_uring::IoUring> {
        match io_uring::IoUring::new(entries) {
            Ok(r) => Some(r),
            Err(e)
                if e.raw_os_error()
                    .is_some_and(|c| c == libc::ENOSYS || c == libc::EPERM || c == libc::EACCES) =>
            {
                None
            }
            Err(e) => panic!("io_uring::new: {e}"),
        }
    }

    /// uring_batch_fdatasync must complete one CQE per fd it submitted, and
    /// drain the SQ when the fd count exceeds the ring's SQ entries (forcing
    /// multiple submit_and_wait rounds).
    #[test]
    fn test_uring_batch_fdatasync_chunked() {
        // Tiny ring forces multiple submit_and_wait rounds for >4 fds.
        let Some(mut ring) = try_new_ring(4) else { return };
        let dir = tempfile::tempdir().unwrap();

        let mut fds: Vec<libc::c_int> = Vec::new();
        for i in 0..10 {
            let path = dir.path().join(format!("f_{i}.bin"));
            let path_c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let fd = unsafe {
                libc::open(
                    path_c.as_ptr(),
                    libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
                    0o644 as libc::mode_t,
                )
            };
            assert!(fd >= 0, "open failed: {}", std::io::Error::last_os_error());
            // Some content so fdatasync has data to flush.
            let buf = b"hello";
            let rc = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
            assert_eq!(rc as usize, buf.len());
            fds.push(fd);
        }

        uring_batch_fdatasync(&mut ring, &fds).expect("batch fdatasync");

        for fd in fds {
            unsafe {
                libc::close(fd);
            }
        }
    }

    #[test]
    fn test_uring_batch_fdatasync_empty() {
        let Some(mut ring) = try_new_ring(8) else { return };
        uring_batch_fdatasync(&mut ring, &[]).expect("empty batch should succeed");
    }

    /// EINTR on the first submit_and_wait must not stall: any CQEs that arrived
    /// before the interrupt are drained, the loop retries, and all fds complete.
    #[test]
    fn test_uring_batch_fdatasync_eintr_retries() {
        let Some(mut ring) = try_new_ring(8) else { return };
        let dir = tempfile::tempdir().unwrap();

        let mut fds: Vec<libc::c_int> = Vec::new();
        for i in 0..3 {
            let path = dir.path().join(format!("eintr_{i}.bin"));
            let path_c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let fd = unsafe { libc::open(path_c.as_ptr(), libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o644) };
            assert!(fd >= 0);
            let buf = b"x";
            unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, 1) };
            fds.push(fd);
        }

        let mut call_count = 0usize;
        let result = uring_batch_fdatasync_with(&mut ring, &fds, |r, want| {
            call_count += 1;
            if call_count == 1 {
                // Simulate EINTR without submitting: the SQEs stay queued in the
                // ring buffer. The loop must drain 0 CQEs, continue, and retry.
                Err(std::io::Error::from_raw_os_error(libc::EINTR))
            } else {
                r.submit_and_wait(want)
            }
        });

        assert!(result.is_ok(), "EINTR should be retried, got: {result:?}");
        assert_eq!(call_count, 2, "exactly one EINTR then one successful submit expected");

        for fd in fds {
            unsafe {
                libc::close(fd);
            }
        }
    }
}
