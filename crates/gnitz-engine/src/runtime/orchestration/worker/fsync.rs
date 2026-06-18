//! io_uring batched fdatasync helpers: `dedup_dirfds` and the
//! `uring_batch_fdatasync` durability primitive.

/// Deduplicate a list of (dev, ino, fd) triples by (dev, ino), returning one fd per
/// unique directory inode. Both fields are u64 to handle varying ino_t/dev_t widths.
pub(crate) fn dedup_dirfds(mut inodes: Vec<(u64, u64, libc::c_int)>) -> Vec<libc::c_int> {
    // Returns one fd per unique (dev, ino) so a shared directory is fsynced
    // once. The fds are per-flush and owned by the caller, which closes EVERY
    // collected fd (not just this deduped subset) after the fsync — so this
    // helper must NOT close the duplicates it drops.
    inodes.sort_unstable_by_key(|&(dev, ino, _)| (dev, ino));
    inodes.dedup_by_key(|&mut (dev, ino, _)| (dev, ino));
    inodes.into_iter().map(|(_, _, fd)| fd).collect()
}

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

    #[test]
    fn test_dedup_dirfds_removes_duplicates() {
        // dedup_dirfds only deduplicates by (dev, ino) — it does not close the
        // duplicates it drops (the caller closes every collected fd).
        let inodes = vec![
            (1u64, 10u64, 3i32),
            (1u64, 10u64, 7i32), // duplicate of (1,10)
            (1u64, 20u64, 5i32),
            (2u64, 10u64, 9i32), // same ino, different dev — not a duplicate
        ];
        let mut result = dedup_dirfds(inodes);
        result.sort_unstable();
        assert_eq!(result.len(), 3, "one fd per unique (dev, ino)");
        // No duplicate fds for (1, 10)
        assert!(result.contains(&3i32) || result.contains(&7i32));
        assert!(result.contains(&5i32));
        assert!(result.contains(&9i32));
    }

    #[test]
    fn test_dedup_dirfds_empty() {
        assert_eq!(dedup_dirfds(vec![]), Vec::<libc::c_int>::new());
    }

    #[test]
    fn test_dedup_dirfds_all_unique() {
        let inodes = vec![(1u64, 1u64, 10i32), (1u64, 2u64, 20i32), (2u64, 1u64, 30i32)];
        let mut result = dedup_dirfds(inodes);
        result.sort_unstable();
        assert_eq!(result, vec![10i32, 20i32, 30i32]);
    }

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
