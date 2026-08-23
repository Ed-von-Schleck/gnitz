//! Multi-table durable flush barrier.
//!
//! One pass over a table set: prepare each table, batch every fdatasync through
//! a single io_uring, commit each manifest rename, fsync the directories that
//! received one, then drain the deferred compaction deletions. Publishing N
//! tables costs a handful of ring submissions instead of ~4 blocking syscalls
//! per table, and never holds more than [`FD_CHUNK_THRESHOLD`] fds open.

use std::ffi::CStr;
use std::os::fd::{AsRawFd, OwnedFd};

use super::super::error::StorageError;
use super::table::{FlushOutcome, FlushWork, Table};
use crate::foundation::posix_io::open_owned;

/// Concurrent-fd budget for one barrier chunk. Bounds both the per-table
/// accumulation before a chunk publishes and the sub-chunk its by-path sweep
/// opens, so a checkpoint over many relations never holds thousands of fds at
/// once (EMFILE).
const FD_CHUNK_THRESHOLD: usize = 256;

/// Which checkpoint round a barrier publishes.
#[derive(Clone, Copy)]
pub enum FlushRound {
    /// Base tables: a `SalReplay` store publishes a manifest, a rederived one
    /// folds to RAM inline and publishes nothing.
    Base,
    /// View state: force-publish every operator trace and output store,
    /// stamping each manifest with the checkpoint generation.
    Ephemeral(u64),
}

impl FlushRound {
    /// The checkpoint generation to stamp a published manifest with. The base
    /// round stamps 0: only a `Rederive` open reads this field back,
    /// and such a table publishes nothing on the base round.
    pub(super) fn checkpoint_gen(self) -> u64 {
        match self {
            FlushRound::Base => 0,
            FlushRound::Ephemeral(g) => g,
        }
    }
}

/// Flush every table in `tables` through the two-phase publish for `round`.
///
/// Takes raw pointers and dereferences each as `&mut Table`, so the caller owes
/// what a `&mut` would have proved: every pointer must point at a live `Table`,
/// no two may alias, and nothing else may touch any of them until this returns.
/// The signature enforces none of it. The engine's own caller — the worker
/// checkpoint — collects them from the DAG while the engine is single-threaded
/// and cannot yield, which is how it discharges the obligation; a caller that
/// can yield must freeze the set some other way.
pub fn flush_barrier(tables: impl IntoIterator<Item = *mut Table>, round: FlushRound) -> Result<(), StorageError> {
    let mut ring = LazyRing::default();
    let mut pending: Vec<(*mut Table, FlushWork)> = Vec::new();
    let mut pending_fds = 0usize;
    // Tables that published a manifest this round — drained only after every
    // chunk succeeded, so a crash between publish and drain loads the cut
    // manifest over intact files.
    let mut flushed: Vec<*mut Table> = Vec::new();

    for t in tables {
        let work = match unsafe { &mut *t }.flush_prepare(round)? {
            FlushOutcome::Done => continue,
            FlushOutcome::Pending(w) => w,
        };
        // Each work opens one fd per unsynced file (the by-path sweep) plus the
        // manifest `.tmp` fd.
        pending_fds += work.sync_paths().len() + 1;
        pending.push((t, work));
        if pending_fds >= FD_CHUNK_THRESHOLD {
            publish_chunk(&mut ring, &mut pending, &mut flushed)?;
            pending_fds = 0;
        }
    }
    publish_chunk(&mut ring, &mut pending, &mut flushed)?;

    // Every published manifest is durable now, so a superseded compaction input
    // can no longer be unlinked while a manifest still referencing it is
    // unpublished.
    for t in flushed {
        unsafe { &mut *t }.drain_deletions();
    }
    Ok(())
}

/// Make every file in `paths` durable: open each `O_RDONLY` and fdatasync it
/// through one ring submission per sub-chunk, so a large set never holds more
/// than [`FD_CHUNK_THRESHOLD`] fds open at once (EMFILE). The one spelling of
/// "these written files are now on disk" — the checkpoint barrier and the boot
/// relayout both go through it.
pub(super) fn sync_by_path(ring: &mut LazyRing, paths: &[&CStr]) -> Result<(), StorageError> {
    for sub in paths.chunks(FD_CHUNK_THRESHOLD) {
        let owned: Vec<OwnedFd> = sub
            .iter()
            .map(|p| open_owned(p, libc::O_RDONLY).map_err(StorageError::from))
            .collect::<Result<_, _>>()?;
        let raw: Vec<libc::c_int> = owned.iter().map(|f| f.as_raw_fd()).collect();
        ring.batch_sync(&raw, DATASYNC)?;
        // `owned` drops here → fds closed before the next sub-chunk
    }
    Ok(())
}

/// One fd-bounded chunk: batch-fdatasync the manifest `.tmp` fds and the
/// unsynced files, rename each manifest into place, then batch-fsync the
/// directories that received one. Every fd this chunk opened is closed before
/// the next chunk starts.
fn publish_chunk(
    ring: &mut LazyRing,
    pending: &mut Vec<(*mut Table, FlushWork)>,
    flushed: &mut Vec<*mut Table>,
) -> Result<(), StorageError> {
    if pending.is_empty() {
        return Ok(());
    }

    let manifest_fds: Vec<libc::c_int> = pending.iter().map(|(_, w)| w.manifest_fd()).collect();
    ring.batch_sync(&manifest_fds, DATASYNC)?;

    let paths: Vec<&CStr> = pending
        .iter()
        .flat_map(|(_, w)| w.sync_paths().iter().map(|c| c.as_c_str()))
        .collect();
    sync_by_path(ring, &paths)?;

    // Publish, then make the renames durable. A directory needs a full fsync,
    // not fdatasync: the rename is metadata.
    let mut dir_fds: Vec<OwnedFd> = Vec::with_capacity(pending.len());
    for (t, work) in pending.drain(..) {
        dir_fds.push(unsafe { &mut *t }.flush_commit(work)?);
        flushed.push(t);
    }
    let raw: Vec<libc::c_int> = dir_fds.iter().map(|f| f.as_raw_fd()).collect();
    ring.batch_sync(&raw, io_uring::types::FsyncFlags::empty())
}

const DATASYNC: io_uring::types::FsyncFlags = io_uring::types::FsyncFlags::DATASYNC;

/// The process's io_uring verdict, decided by the first barrier that needs a
/// ring and latched from then on — `LazyRing::default()` is constructed fresh on
/// every `flush_barrier` call, so a per-instance verdict would re-decide, and
/// re-read the env override, on every one.
static IO_URING_USABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// The errnos that mean "this platform denies the syscall" rather than "this
/// call failed": Docker's default seccomp profile and
/// `kernel.io_uring_disabled=2` both report one of them. On any of the three the
/// flush path falls back to blocking `fsync`/`fdatasync` — for every caller,
/// server included — rather than failing the flush, and logs a warning once.
fn is_uring_denied(e: &std::io::Error) -> bool {
    e.raw_os_error()
        .is_some_and(|c| c == libc::ENOSYS || c == libc::EPERM || c == libc::EACCES)
}

/// io_uring created on first use: a barrier over a table set that turns out to
/// need no I/O should not pay `io_uring_setup` plus its two mmaps.
#[derive(Default)]
pub(super) struct LazyRing(Option<io_uring::IoUring>);

impl LazyRing {
    /// Submit one FSYNC SQE per fd and await completion of all of them. Drains
    /// the SQ when full, so a batch larger than the ring's SQ entries takes
    /// several `submit_and_wait` calls.
    ///
    /// Where io_uring is unavailable this loops the fds through blocking
    /// `fsync`/`fdatasync` instead. This is the engine's only io_uring user, so
    /// the fallback here is what lets the whole flush path run on a host that
    /// denies the syscall.
    fn batch_sync(&mut self, fds: &[libc::c_int], flags: io_uring::types::FsyncFlags) -> Result<(), StorageError> {
        self.batch_sync_with(fds, flags, |r, want| r.submit_and_wait(want))
    }

    /// `submit` is a seam so the EINTR retry can be exercised; production always
    /// passes `submit_and_wait`.
    fn batch_sync_with(
        &mut self,
        fds: &[libc::c_int],
        flags: io_uring::types::FsyncFlags,
        mut submit: impl FnMut(&mut io_uring::IoUring, usize) -> std::io::Result<usize>,
    ) -> Result<(), StorageError> {
        if fds.is_empty() {
            return Ok(());
        }
        let ring = match self.0 {
            Some(ref mut r) => r,
            None => match new_ring()? {
                Some(r) => self.0.insert(r),
                None => return blocking_sync(fds, flags),
            },
        };
        let sq_capacity = ring.params().sq_entries() as usize;
        let mut completed = 0usize;
        let mut pushed = 0usize;
        while completed < fds.len() {
            while pushed < fds.len() {
                if ring.submission().len() >= sq_capacity {
                    break;
                }
                let sqe = io_uring::opcode::Fsync::new(io_uring::types::Fd(fds[pushed]))
                    .flags(flags)
                    .build();
                unsafe {
                    // A full submission queue, not a syscall failure: no errno.
                    ring.submission().push(&sqe).map_err(|_| StorageError::Io(0))?;
                }
                pushed += 1;
            }
            match submit(ring, pushed - completed) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => {
                    // Drain any CQEs that arrived before the signal, then retry.
                    completed += drain(ring)?;
                    continue;
                }
                Err(e) => {
                    gnitz_warn!("uring submit_and_wait: {}", e);
                    return Err(StorageError::from(e));
                }
            }
            completed += drain(ring)?;
        }
        Ok(())
    }
}

/// Build the ring, or `Ok(None)` where this host denies `io_uring_setup` and the
/// caller must fall back to blocking `fsync`. `GNITZ_DISABLE_IO_URING` forces
/// that fallback without a sysctl, which is what lets `make verify` exercise it.
///
/// Either verdict latches, so the syscall — and the env read that can veto it —
/// happen once per process. A setup that fails for any other reason (ENOMEM,
/// EMFILE) is this one call failing rather than a platform verdict, so it
/// decides nothing and is reported as the flush error it is.
fn new_ring() -> Result<Option<io_uring::IoUring>, StorageError> {
    match IO_URING_USABLE.get() {
        Some(false) => return Ok(None),
        Some(true) => {}
        // Undecided, so the env override gets its say before the first attempt.
        None if crate::foundation::env::env_flag("GNITZ_DISABLE_IO_URING") => {
            let _ = IO_URING_USABLE.set(false);
            return Ok(None);
        }
        None => {}
    }
    match io_uring::IoUring::new(256) {
        Ok(r) => {
            let _ = IO_URING_USABLE.set(true);
            Ok(Some(r))
        }
        Err(e) if is_uring_denied(&e) => {
            gnitz_warn!("io_uring unavailable ({}); flushing through blocking fsync", e);
            let _ = IO_URING_USABLE.set(false);
            Ok(None)
        }
        Err(e) => {
            gnitz_warn!("io_uring::new failed: {}", e);
            Err(StorageError::from(e))
        }
    }
}

/// `batch_sync`'s fallback: one blocking `fsync`/`fdatasync` per fd, retried
/// through EINTR. Same contract — every fd is durable when this returns Ok.
fn blocking_sync(fds: &[libc::c_int], flags: io_uring::types::FsyncFlags) -> Result<(), StorageError> {
    let datasync = flags.contains(DATASYNC);
    for &fd in fds {
        loop {
            let rc = if datasync {
                unsafe { libc::fdatasync(fd) }
            } else {
                unsafe { libc::fsync(fd) }
            };
            if rc == 0 {
                break;
            }
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            gnitz_warn!("blocking fsync failed: {}", err);
            return Err(StorageError::from(err));
        }
    }
    Ok(())
}

/// Consume every ready CQE, returning how many completed. Any failed fsync is
/// the barrier's failure.
fn drain(ring: &mut io_uring::IoUring) -> Result<usize, StorageError> {
    let mut n = 0;
    for cqe in ring.completion() {
        if cqe.result() < 0 {
            gnitz_warn!("fsync via uring failed: {}", cqe.result());
            // A CQE reports failure as `-errno`.
            return Err(StorageError::Io(-cqe.result()));
        }
        n += 1;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
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
}
