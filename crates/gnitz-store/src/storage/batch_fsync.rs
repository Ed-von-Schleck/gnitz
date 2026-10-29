//! Batched fsync of many files: one io_uring submission per chunk, with a
//! blocking fallback where the host denies io_uring.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::LazyLock;

use io_uring::types::FsyncFlags;
use io_uring::{opcode, types, IoUring};

use gnitz_zset::repr::StorageError;

/// The fds one sync batch opens, and the ring's SQ size.
const FD_CHUNK_THRESHOLD: usize = 256;

/// Blocking fsync instead of io_uring: `GNITZ_DISABLE_IO_URING`, read once per
/// process, or a host that denied `io_uring_setup`.
static BLOCKING: LazyLock<AtomicBool> =
    LazyLock::new(|| AtomicBool::new(gnitz_foundation::env::env_flag("GNITZ_DISABLE_IO_URING", false)));

/// The ring, or `None` for blocking fsync: where this host denies io_uring, or
/// `GNITZ_DISABLE_IO_URING` is set.
fn new_ring() -> Result<Option<IoUring>, StorageError> {
    if BLOCKING.load(Relaxed) {
        return Ok(None);
    }
    let e = match IoUring::new(FD_CHUNK_THRESHOLD as u32) {
        Ok(r) => return Ok(Some(r)),
        Err(e) => e,
    };
    let Some(why) = gnitz_foundation::host::io_uring_refusal(&e) else {
        return Err(e.into());
    };
    gnitz_warn!("io_uring unavailable ({e}: {why}); flushing through blocking fsync");
    BLOCKING.store(true, Relaxed);
    Ok(None)
}

/// Syncs batches of files through one ring, set up on the first chunk of more
/// than one file: a lone file's sync is one blocking call, cheaper than setting
/// up a ring, and a ring's memory is returned to the locked-memory budget only
/// some time after it closes, so a run of single syncs that each set one up can
/// exhaust it.
#[derive(Default)]
pub(super) struct Syncer {
    /// `None` until a chunk needs it, and where syncs block.
    ring: Option<IoUring>,
}

impl Syncer {
    /// Sync `paths`, [`FD_CHUNK_THRESHOLD`] open files at a time; `DATASYNC`
    /// in `flags` selects fdatasync.
    pub(super) fn sync_paths<P: AsRef<Path>>(
        &mut self,
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
            let ring = if files.len() > 1 { self.ring()? } else { None };
            // A failed batch can leave entries in the ring's queues, so the
            // next batch starts on a fresh ring.
            batch_sync_with(ring, &files, flags, |r, want| r.submit_and_wait(want))
                .inspect_err(|_| self.ring = None)?;
        }
        Ok(())
    }

    fn ring(&mut self) -> Result<Option<&mut IoUring>, StorageError> {
        if self.ring.is_none() {
            self.ring = new_ring()?;
        }
        Ok(self.ring.as_mut())
    }
}

/// Sync every file in `files`, with the submit call injectable for tests.
fn batch_sync_with(
    ring: Option<&mut IoUring>,
    files: &[File],
    flags: FsyncFlags,
    mut submit: impl FnMut(&mut IoUring, usize) -> std::io::Result<usize>,
) -> Result<(), StorageError> {
    let Some(ring) = ring else {
        let datasync = flags.contains(FsyncFlags::DATASYNC);
        return files
            .iter()
            .try_for_each(|f| if datasync { f.sync_data() } else { f.sync_all() })
            .map_err(Into::into);
    };
    for f in files {
        let sqe = opcode::Fsync::new(types::Fd(f.as_raw_fd())).flags(flags).build();
        // SAFETY: an Fsync SQE points at no user memory; the kernel takes its
        // own reference to the file when it submits.
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
#[path = "tests/batch_fsync.rs"]
mod tests;
