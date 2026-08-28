use super::*;

/// Build a ring, or return `None` if the platform denies the syscall (no
/// io_uring support, no CAP_SYS_ADMIN, or an AppArmor/seccomp restriction)
/// so the caller can skip rather than panic.
fn try_lazy_ring(entries: u32) -> Option<LazyRing> {
    match io_uring::IoUring::new(entries) {
        Ok(r) => Some(LazyRing(Some(r))),
        Err(e) if is_uring_denied(&e) => None,
        Err(e) => panic!("io_uring::new: {e}"),
    }
}

fn open_written(dir: &std::path::Path, name: &str) -> std::fs::File {
    use std::io::Write;
    let mut f = std::fs::File::create(dir.join(name)).expect("create");
    f.write_all(b"hello").expect("write");
    f
}

/// One CQE per submitted fd, including when the fd count exceeds the ring's
/// SQ entries (forcing multiple submit_and_wait rounds).
#[test]
fn batch_sync_chunks_past_the_sq_capacity() {
    // Tiny ring forces multiple submit_and_wait rounds for >4 fds.
    let Some(mut ring) = try_lazy_ring(4) else { return };
    let dir = tempfile::tempdir().unwrap();
    let files: Vec<std::fs::File> = (0..10)
        .map(|i| open_written(dir.path(), &format!("f_{i}.bin")))
        .collect();
    let fds: Vec<libc::c_int> = files.iter().map(|f| f.as_raw_fd()).collect();

    ring.batch_sync(&fds, DATASYNC).expect("batch fdatasync");
}

#[test]
fn batch_sync_of_nothing_does_not_even_build_a_ring() {
    let mut ring = LazyRing::default();
    ring.batch_sync(&[], DATASYNC).expect("empty batch should succeed");
    assert!(ring.0.is_none(), "an empty batch must not pay io_uring_setup");
}

/// EINTR on the first submit must not stall: any CQEs that arrived before
/// the interrupt are drained, the loop retries, and all fds complete.
#[test]
fn batch_sync_retries_after_eintr() {
    let Some(mut ring) = try_lazy_ring(8) else { return };
    let dir = tempfile::tempdir().unwrap();
    let files: Vec<std::fs::File> = (0..3)
        .map(|i| open_written(dir.path(), &format!("eintr_{i}.bin")))
        .collect();
    let fds: Vec<libc::c_int> = files.iter().map(|f| f.as_raw_fd()).collect();

    let mut call_count = 0usize;
    let result = ring.batch_sync_with(&fds, DATASYNC, |r, want| {
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
}
