use super::*;

fn open_written(dir: &Path, name: &str) -> File {
    use std::io::Write;
    let mut f = File::create(dir.join(name)).expect("create");
    f.write_all(b"hello").expect("write");
    f
}

/// A ring, or `None` where this host denies io_uring, so the caller can skip.
fn ring_or_skip() -> Option<Option<IoUring>> {
    new_ring().expect("io_uring setup").map(Some)
}

/// EINTR on the first submit is retried.
#[test]
fn batch_sync_retries_after_eintr() {
    let Some(mut ring) = ring_or_skip() else { return };
    let dir = tempfile::tempdir().unwrap();
    let files: Vec<File> = (0..3)
        .map(|i| open_written(dir.path(), &format!("eintr_{i}.bin")))
        .collect();

    let mut call_count = 0usize;
    let result = batch_sync_with(&mut ring, &files, DATASYNC, |r, want| {
        call_count += 1;
        if call_count == 1 {
            // EINTR without submitting: the SQEs stay queued for the retry.
            Err(std::io::Error::from_raw_os_error(libc::EINTR))
        } else {
            r.submit_and_wait(want)
        }
    });

    assert!(result.is_ok(), "EINTR should be retried, got: {result:?}");
    assert_eq!(call_count, 2, "exactly one EINTR then one successful submit expected");
}

/// A submit that returns before the fsyncs complete must not end the batch early.
#[test]
fn batch_sync_waits_out_a_short_submit() {
    let Some(mut ring) = ring_or_skip() else { return };
    let dir = tempfile::tempdir().unwrap();
    let files: Vec<File> = (0..16)
        .map(|i| open_written(dir.path(), &format!("short_{i}.bin")))
        .collect();

    batch_sync_with(&mut ring, &files, DATASYNC, |r, _| r.submit()).expect("short submits");
    let r = ring.as_mut().unwrap();
    assert!(r.submission().is_empty(), "every SQE was submitted");
    assert!(r.completion().is_empty(), "every CQE was reaped");

    batch_sync_with(&mut ring, &files, DATASYNC, |r, want| r.submit_and_wait(want))
        .expect("a second batch on the same ring");
}

/// Two full chunks plus a tail, through one ring.
#[test]
fn sync_paths_spans_several_chunks() {
    let Some(mut ring) = ring_or_skip() else { return };
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (0..2 * FD_CHUNK_THRESHOLD + 1)
        .map(|i| {
            let name = format!("chunk_{i}.bin");
            open_written(dir.path(), &name);
            dir.path().join(name)
        })
        .collect();

    sync_paths(&mut ring, &paths, DATASYNC).expect("chunked fdatasync");
}

/// The blocking fallback, for files and a directory.
#[test]
fn sync_paths_without_a_ring() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (0..FD_CHUNK_THRESHOLD + 1)
        .map(|i| {
            let name = format!("blocking_{i}.bin");
            open_written(dir.path(), &name);
            dir.path().join(name)
        })
        .collect();

    sync_paths(&mut None, &paths, DATASYNC).expect("blocking fdatasync");
    sync_paths(&mut None, [dir.path()], FsyncFlags::empty()).expect("blocking directory fsync");
}
