use super::*;
use std::os::fd::OwnedFd;

fn ring() -> IoUring {
    new_ring().unwrap().expect("io_uring is a build prerequisite")
}

/// An interrupted submit and one that returns with nothing reaped are retried
/// until every fsync completes, leaving the ring empty for the next batch; a
/// submit failing otherwise surfaces its errno.
#[test]
fn batch_sync_reaps_through_interrupted_and_short_submits() {
    let mut ring = ring();
    let files: Vec<File> = (0..16).map(|_| tempfile::tempfile().unwrap()).collect();
    let mut calls = 0;
    batch_sync_with(Some(&mut ring), &files, DATASYNC, |r, want| {
        calls += 1;
        match calls {
            // Neither submits: the SQEs stay queued for the retry.
            1 => Err(std::io::Error::from_raw_os_error(libc::EINTR)),
            2 => Ok(0),
            _ => r.submit_and_wait(want),
        }
    })
    .expect("retried submits");
    assert_eq!(calls, 3);
    assert!(ring.submission().is_empty(), "every SQE was submitted");
    assert!(ring.completion().is_empty(), "every CQE was reaped");

    let got = batch_sync_with(Some(&mut ring), &files, DATASYNC, |_, _| {
        Err(std::io::Error::from_raw_os_error(libc::EIO))
    });
    assert_eq!(got, Err(StorageError::Io(libc::EIO)));
}

/// Two full chunks plus a tail, through a ring and through the blocking
/// fallback, and a directory.
#[test]
fn sync_paths_spans_several_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (0..2 * FD_CHUNK_THRESHOLD + 1)
        .map(|i| {
            let p = dir.path().join(i.to_string());
            File::create(&p).unwrap();
            p
        })
        .collect();
    for mut ring in [Some(ring()), None] {
        sync_paths(&mut ring, &paths, DATASYNC).expect("chunked fdatasync");
        sync_paths(&mut ring, [dir.path()], FsyncFlags::empty()).expect("directory fsync");
    }
}

/// A failed fsync surfaces its errno, from a CQE and from the fallback alike.
#[test]
fn a_failed_fsync_reports_its_errno() {
    let (r, w) = std::io::pipe().unwrap();
    let files = [File::from(OwnedFd::from(r)), File::from(OwnedFd::from(w))];
    for mut ring in [Some(ring()), None] {
        let got = batch_sync_with(ring.as_mut(), &files, DATASYNC, |r, want| r.submit_and_wait(want));
        assert_eq!(got, Err(StorageError::Io(libc::EINVAL)), "a pipe cannot be synced");
    }
}
