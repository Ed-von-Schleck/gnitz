//! Multi-table durable flush barrier.

use std::collections::BTreeSet;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use io_uring::types::FsyncFlags;
use io_uring::{opcode, types, IoUring};

use super::super::error::StorageError;
use super::table::{FlushWork, Table};

/// The fds one sync batch opens, and the ring's SQ size.
const FD_CHUNK_THRESHOLD: usize = 256;

const DATASYNC: FsyncFlags = FsyncFlags::DATASYNC;

/// Set once this host has denied `io_uring_setup`.
static IO_URING_DENIED: AtomicBool = AtomicBool::new(false);

/// Flush every table in `tables`, stamping each published manifest
/// `checkpoint_gen`.
pub(crate) fn flush_barrier<'a>(
    tables: impl IntoIterator<Item = &'a mut Table>,
    checkpoint_gen: u64,
) -> Result<(), StorageError> {
    let mut work: Vec<(&'a mut Table, FlushWork)> = Vec::new();
    for t in tables {
        if let Some(w) = t.flush_prepare(checkpoint_gen)? {
            work.push((t, w));
        }
    }
    if work.is_empty() {
        return Ok(());
    }
    let mut ring = new_ring()?;
    // Every unsynced shard and every staged manifest, before any rename.
    sync_paths(
        &mut ring,
        work.iter()
            .flat_map(|(t, w)| t.unsynced_paths().chain([w.manifest.tmp_path()])),
        DATASYNC,
    )?;
    let mut dirs = BTreeSet::new();
    let mut published = Vec::with_capacity(work.len());
    for (t, w) in work {
        t.flush_commit(w.manifest)?;
        dirs.extend(w.dirs);
        published.push((t, w.bytes));
    }
    // A rename is metadata: a full fsync, not fdatasync.
    sync_paths(&mut ring, &dirs, FsyncFlags::empty())?;
    // Only now can a superseded compaction input go: every manifest naming it is durable.
    for (t, bytes) in published {
        t.published_durably(bytes);
    }
    Ok(())
}

/// The ring, or `None` for blocking fsync: where this host denies io_uring, or
/// `GNITZ_DISABLE_IO_URING` is set.
fn new_ring() -> Result<Option<IoUring>, StorageError> {
    if IO_URING_DENIED.load(Relaxed) || gnitz_foundation::env::env_flag("GNITZ_DISABLE_IO_URING", false) {
        return Ok(None);
    }
    match IoUring::new(FD_CHUNK_THRESHOLD as u32) {
        Ok(r) => Ok(Some(r)),
        // Docker's default seccomp profile and `kernel.io_uring_disabled=2` report one of these.
        Err(e) if matches!(e.raw_os_error(), Some(libc::ENOSYS | libc::EPERM | libc::EACCES)) => {
            gnitz_warn!("io_uring unavailable ({e}); flushing through blocking fsync");
            IO_URING_DENIED.store(true, Relaxed);
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

/// Sync `paths`, [`FD_CHUNK_THRESHOLD`] open files at a time.
fn sync_paths<P: AsRef<Path>>(
    ring: &mut Option<IoUring>,
    paths: impl IntoIterator<Item = P>,
    flags: FsyncFlags,
) -> Result<(), StorageError> {
    let mut paths = paths.into_iter().peekable();
    while paths.peek().is_some() {
        let files: Vec<File> = paths
            .by_ref()
            .take(FD_CHUNK_THRESHOLD)
            .map(File::open)
            .collect::<Result<_, _>>()?;
        batch_sync(ring, &files, flags)?;
    }
    Ok(())
}

/// Sync every file in `files`; `DATASYNC` in `flags` selects fdatasync.
fn batch_sync(ring: &mut Option<IoUring>, files: &[File], flags: FsyncFlags) -> Result<(), StorageError> {
    batch_sync_with(ring, files, flags, |r, want| r.submit_and_wait(want))
}

/// [`batch_sync`] with the submit call injectable, for tests.
fn batch_sync_with(
    ring: &mut Option<IoUring>,
    files: &[File],
    flags: FsyncFlags,
    mut submit: impl FnMut(&mut IoUring, usize) -> std::io::Result<usize>,
) -> Result<(), StorageError> {
    let Some(ring) = ring else {
        let datasync = flags.contains(DATASYNC);
        return files
            .iter()
            .try_for_each(|f| if datasync { f.sync_data() } else { f.sync_all() })
            .map_err(Into::into);
    };
    for f in files {
        let sqe = opcode::Fsync::new(types::Fd(f.as_raw_fd())).flags(flags).build();
        // SAFETY: `f` outlives every wait below.
        unsafe { ring.submission().push(&sqe) }
            .expect("the SQ holds FD_CHUNK_THRESHOLD entries and starts each batch empty");
    }
    // A wait can return early (a signal, a short submit): reap until every CQE is in.
    let mut reaped = 0;
    while reaped < files.len() {
        match submit(ring, files.len() - reaped) {
            Err(e) if e.raw_os_error() != Some(libc::EINTR) => return Err(e.into()),
            _ => {}
        }
        for cqe in ring.completion() {
            // A CQE reports failure as `-errno`.
            if cqe.result() < 0 {
                return Err(StorageError::Io(-cqe.result()));
            }
            reaped += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/flush_barrier.rs"]
mod tests;
