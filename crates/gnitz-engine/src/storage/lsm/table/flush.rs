//! Two-phase flush state machine for [`Table`].
//!
//! The flush/spill path carved off `Table`'s ingest/cursor/lookup surface:
//! `flush_prepare` (Phase 1 — consolidate, then write the shard image to a
//! `.tmp`, returning `FlushWork`), `flush_commit` (Phase 2 — fsync, rename into
//! place, publish the manifest), the synchronous `flush`/`flush_inner`
//! wrappers, and in-memory ceiling enforcement (`compact_in_memory` /
//! `spill_in_memory_to_disk`). `Table`'s fields are read directly here — `flush`
//! is a child module of the `table` module that defines the struct.

use std::ffi::CString;
use std::rc::Rc;

use super::super::error::StorageError;
use super::super::memtable;
use super::super::shard_file::{self, PkUniqueChecker};
use super::{
    FlushOutcome, FlushWork, FoundSource, Persistence, ShardRename, Table, EPHEMERAL_INMEM_CEILING,
    INMEM_COMPACT_THRESHOLD,
};
use crate::foundation::posix_io::{fdatasync_eintr, fsync_eintr};

impl Table {
    /// Flush memtable to shard.  Persistent tables also update manifest.
    pub fn flush(&mut self) -> Result<bool, StorageError> {
        self.flush_inner(self.persistence, true)
    }

    /// Flush with durable shard naming and manifest update, regardless of
    /// WAL state.  Used by checkpoint: system tables disable WAL (SAL
    /// provides durability) but still need manifest-tracked shards so
    /// the data survives restart.
    pub fn flush_durable(&mut self) -> Result<bool, StorageError> {
        self.flush_inner(Persistence::Durable, true)
    }

    /// Open the partition directory fd on demand (`O_RDONLY|O_DIRECTORY`).
    /// Opened per flush/compaction rather than held for the table's lifetime,
    /// so a 256-partition table pins 0 directory fds at rest instead of 256
    /// (which exhausted the default `ulimit -n` after a handful of tables).
    /// The caller owns the returned fd and must close it.
    pub(super) fn open_dirfd(&self) -> Result<libc::c_int, StorageError> {
        let dir_c = CString::new(self.directory.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let fd = unsafe { libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
        if fd < 0 {
            return Err(StorageError::Io);
        }
        Ok(fd)
    }

    /// Phase 1 of two-phase flush, honoring the table's intrinsic persistence.
    ///
    /// `Ephemeral` (index circuits, views) with a non-empty consolidated
    /// snapshot:
    ///   append the snapshot to `in_memory_l0` — no file I/O, no manifest. The
    ///   memtable IS reset. The run set is folded once it passes
    ///   `INMEM_COMPACT_THRESHOLD`, and spilled to a real `eph_shard` only when
    ///   `in_memory_bytes()` exceeds `EPHEMERAL_INMEM_CEILING` (the spill path
    ///   restores page-cache reclaimability for the rare oversized table).
    ///   `DoneInline` is returned.
    ///
    /// `Durable` and a non-empty consolidated snapshot:
    ///   write shard `.tmp` (no fdatasync), open MappedShard against it for
    ///   metadata, write manifest `.tmp`, return `Pending(FlushWork)`. The
    ///   memtable is NOT reset.
    ///
    /// Empty memtable or `consolidated.count == 0`:
    ///   reset memtable and return `Empty`. The cancelled rows are still
    ///   recoverable from the SAL.
    pub fn flush_prepare(&mut self) -> Result<FlushOutcome, StorageError> {
        self.flush_prepare_with(self.persistence)
    }

    /// `flush_prepare` with an explicit per-flush persistence. Internal only:
    /// the sole override of the table's own value is the worker memonly path
    /// (`ingest_owned_batch_memonly` forces `Ephemeral` overflow flushes on a
    /// `Durable` system table — master owns durability via the SAL).
    fn flush_prepare_with(&mut self, persistence: Persistence) -> Result<FlushOutcome, StorageError> {
        self.found_source = FoundSource::None;
        if self.memtable.is_empty() {
            return Ok(FlushOutcome::Empty);
        }

        let snapshot = self.memtable.consolidate_for_flush();
        if snapshot.count == 0 {
            self.memtable.reset();
            return Ok(FlushOutcome::Empty);
        }

        if persistence == Persistence::Ephemeral {
            // In-memory ephemeral flush: no file I/O. `snapshot` is the
            // (PK,payload)-consolidated run; the memtable references it until
            // reset(), after which `in_memory_l0` keeps it alive.
            self.cached_full_scan = None;
            self.in_memory_l0.push(snapshot);
            self.memtable.reset();
            self.enforce_inmem_bound()?;
            return Ok(FlushOutcome::DoneInline);
        }

        let shard_name = format!("shard_{}_{}.db", self.table_id, self.current_lsn);
        let lsn_max = self.current_lsn.saturating_sub(1);

        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let regions = snapshot.regions();

        // Compute PkUnique flag by observing all rows in sorted order.
        // Only run the checker for base tables; memtable runs are always ZSet.
        let flush_flags = if self.can_tag_pk_unique {
            let mut checker = PkUniqueChecker::new();
            for i in 0..snapshot.count {
                checker.observe(snapshot.get_pk_bytes(i), snapshot.get_weight(i));
            }
            checker.flags_if(true)
        } else {
            0
        };

        // Owned for this flush only. The durable branch moves ownership into
        // `ShardRename` (whose Drop closes it on a rollback) and hands it to the
        // committer.
        let dirfd = self.open_dirfd()?;

        let prepared = match shard_file::write_shard_streaming_prepare(
            dirfd,
            &name_c,
            self.table_id,
            snapshot.count as u32,
            &regions,
            flush_flags,
        ) {
            Ok(p) => p,
            Err(e) => {
                unsafe {
                    libc::close(dirfd);
                }
                return Err(e);
            }
        };
        let shard_fd = prepared.fd;
        let tmp_name = prepared.tmp_name;

        let mut work = FlushWork {
            shard_fd: Some(shard_fd),
            shard_rename: Some(ShardRename {
                dirfd,
                tmp_name: tmp_name.clone(),
                final_name: name_c.clone(),
            }),
            manifest: None,
            pending_shard: None,
        };

        let tmp_full_c = CString::new(format!(
            "{}/{}",
            self.directory,
            tmp_name.to_str().map_err(|_| StorageError::InvalidPath)?,
        ))
        .map_err(|_| StorageError::InvalidPath)?;
        let final_full = format!("{}/{}", self.directory, shard_name);

        let pending = self
            .shard_index
            .open_shard_for_pending(&tmp_full_c, final_full, 0, lsn_max)?;

        let manifest_path = self
            .manifest_path
            .as_ref()
            .expect("durable table must have manifest_path");
        let manifest_c = CString::new(manifest_path.as_str()).map_err(|_| StorageError::InvalidPath)?;
        work.manifest = Some(self.shard_index.prepare_manifest_with_pending(&manifest_c, &pending)?);
        work.pending_shard = Some(pending);

        Ok(FlushOutcome::Pending(Box::new(work)))
    }

    /// Phase 3: rename shard then manifest into final names; insert
    /// pending_shard into shard_index; reset the memtable. Returns the
    /// per-flush directory fd (opened in `flush_prepare`, owned by the
    /// returned `Some`) for the caller to `fsync` after all renames in the
    /// worker batch and then **close**. On any error path the fd is closed
    /// here (or by `FlushWork`'s Drop, when `shard_rename` is restored) so a
    /// failed commit does not leak it.
    pub fn flush_commit(&mut self, mut work: FlushWork) -> Result<Option<libc::c_int>, StorageError> {
        // Rename shard .tmp → final.
        let dirfd = if let Some(rename) = work.shard_rename.take() {
            let rc = unsafe {
                libc::renameat(
                    rename.dirfd,
                    rename.tmp_name.as_ptr(),
                    rename.dirfd,
                    rename.final_name.as_ptr(),
                )
            };
            if rc < 0 {
                // Restore so Drop unlinks the shard .tmp and closes the fd.
                work.shard_rename = Some(rename);
                return Err(StorageError::Io);
            }
            // Rename succeeded: the fd is no longer owned by `ShardRename`
            // (consumed by `take()`), so ownership moves to the returned value.
            Some(rename.dirfd)
        } else {
            None
        };

        // Rename manifest .tmp → final. fd already closed by close_fds.
        if let Some(m) = work.manifest.take() {
            let rc = unsafe { libc::rename(m.tmp_path.as_ptr(), m.final_path.as_ptr()) };
            if rc < 0 {
                // Restore so Drop unlinks the manifest .tmp. The shard is
                // already at its final path and will be collected by orphan GC.
                work.manifest = Some(m);
                if let Some(fd) = dirfd {
                    unsafe {
                        libc::close(fd);
                    }
                }
                return Err(StorageError::Io);
            }
        }

        // Publish the pending shard into the index.
        if let Some(pending) = work.pending_shard.take() {
            if let Err(e) = self.shard_index.add_opened_shard(pending) {
                if let Some(fd) = dirfd {
                    unsafe {
                        libc::close(fd);
                    }
                }
                return Err(e);
            }
        }

        self.memtable.reset();
        Ok(dirfd)
    }

    pub(super) fn flush_inner(&mut self, persistence: Persistence, sync_dir: bool) -> Result<bool, StorageError> {
        match self.flush_prepare_with(persistence)? {
            FlushOutcome::Empty => Ok(false),
            FlushOutcome::DoneInline => Ok(true),
            FlushOutcome::Pending(mut work) => {
                if let Some(fd) = work.shard_fd() {
                    if fdatasync_eintr(fd).is_err() {
                        return Err(StorageError::Io);
                    }
                }
                if let Some(fd) = work.manifest_fd() {
                    if fdatasync_eintr(fd).is_err() {
                        return Err(StorageError::Io);
                    }
                }
                work.close_fds();
                // `flush_commit` returns the owned per-flush dir fd; fsync it
                // (when durability is requested) then close it — it is not held
                // for the table's lifetime.
                let dirfd = self.flush_commit(*work)?;
                if let Some(fd) = dirfd {
                    let ok = if sync_dir { fsync_eintr(fd).is_ok() } else { true };
                    unsafe {
                        libc::close(fd);
                    }
                    if !ok {
                        return Err(StorageError::Io);
                    }
                }
                Ok(true)
            }
        }
    }

    // ------------------------------------------------------------------
    // In-memory ephemeral run set (non-durable flushes)
    // ------------------------------------------------------------------

    /// The effective per-table heap ceiling — `EPHEMERAL_INMEM_CEILING` in
    /// production, or the test override when one is set.
    fn inmem_ceiling(&self) -> usize {
        #[cfg(test)]
        {
            if let Some(c) = self.inmem_ceiling_override {
                return c;
            }
        }
        EPHEMERAL_INMEM_CEILING
    }

    /// Heap footprint of `in_memory_l0` — the byte total that drives the spill
    /// ceiling. Recomputed on demand rather than cached: the run set is bounded
    /// by `INMEM_COMPACT_THRESHOLD`, so this is a handful of `total_bytes()`
    /// arithmetic calls, with no cached field for every mutation of
    /// `in_memory_l0` to keep in sync.
    pub(super) fn in_memory_bytes(&self) -> usize {
        self.in_memory_l0.iter().map(|r| r.total_bytes()).sum()
    }

    /// Keep `in_memory_l0` bounded after a non-durable flush. Called by
    /// `flush_prepare`'s in-memory branch (the only site that grows the set),
    /// so on return `in_memory_l0.len() <= INMEM_COMPACT_THRESHOLD` and
    /// `in_memory_bytes() <= EPHEMERAL_INMEM_CEILING` always hold.
    fn enforce_inmem_bound(&mut self) -> Result<(), StorageError> {
        if self.in_memory_l0.len() > INMEM_COMPACT_THRESHOLD {
            self.compact_in_memory();
        }
        if self.in_memory_bytes() > self.inmem_ceiling() {
            self.spill_in_memory_to_disk()?;
        }
        Ok(())
    }

    /// Fold `in_memory_l0` to a single net-state run. Reuses the memtable's
    /// N-way consolidation primitive; drops net-zero (PK,payload) rows.
    fn compact_in_memory(&mut self) {
        if self.in_memory_l0.len() <= 1 {
            return;
        }
        let merged = memtable::consolidate_runs(&self.in_memory_l0, &self.schema);
        self.in_memory_l0.clear();
        if merged.count > 0 {
            self.in_memory_l0.push(merged);
        }
    }

    /// Ceiling breach: fold to net state first. Folding cancels cross-flush
    /// churn (an insert in flush N against its retraction in N+1) and can
    /// reclaim enough to fall back under the ceiling — in which case this
    /// returns without touching disk. Otherwise write the folded run to a real
    /// `eph_shard` (the historical non-durable disk path), register it, drop it
    /// from heap, then compact the disk tier.
    ///
    /// After a spill the table carries disk runs + future heap runs; cross-tier
    /// churn does NOT fold (neither `compact_in_memory`, which is heap-only, nor
    /// `run_compact`, which is disk-only, sees both). This is bounded: it only
    /// occurs for tables that breached the ceiling, affects disk footprint not
    /// heap (heap stays <= ceiling by construction), and the disk tier still
    /// self-compacts. Eliminating it would need a unified disk+heap compaction —
    /// out of scope.
    ///
    /// Transactional. The consolidated run stays in `in_memory_l0` until the
    /// shard is both written and registered; heap is dropped only on the commit
    /// path. A `write_shard_streaming` failure leaves the run in heap for retry
    /// with nothing on disk to clean up; an `add_shard` failure unlinks the
    /// just-written shard before returning. No error can lose the integrated
    /// state or strand an orphan file.
    fn spill_in_memory_to_disk(&mut self) -> Result<(), StorageError> {
        self.compact_in_memory();
        // Folding may have dropped the net state back under the ceiling (heavy
        // churn cancels to near-nothing); if so there is nothing to spill.
        if self.in_memory_bytes() <= self.inmem_ceiling() {
            return Ok(());
        }
        // Above the ceiling the byte total is > 0, so `compact_in_memory` left
        // exactly one net-state run (count > 0) in heap — `flush_prepare` never
        // enqueues an empty snapshot and `compact_in_memory` only re-pushes
        // `merged.count > 0`. Borrow it — do not remove — so a write/register
        // failure below leaves heap intact for retry.
        let run = Rc::clone(
            self.in_memory_l0
                .first()
                .expect("compaction leaves one net-state run when above the spill ceiling"),
        );

        self.flush_seq += 1;
        let pid = unsafe { libc::getpid() };
        let shard_name = format!(
            "eph_shard_{}_{}_{}_{}.db",
            self.table_id, pid, self.flush_seq, self.current_lsn
        );
        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;

        let dirfd = self.open_dirfd()?;
        let res = shard_file::write_shard_streaming(
            dirfd,
            &name_c,
            self.table_id,
            run.count as u32,
            &run.regions(),
            false,
            0,
        );
        unsafe {
            libc::close(dirfd);
        }
        res?; // Write failed: heap still owns `run`; no on-disk residue.

        let final_full = format!("{}/{}", self.directory, shard_name);
        if let Err(e) = self.shard_index.add_shard(&final_full, 0, 0) {
            // Registration failed: unlink the shard we wrote, keep heap intact.
            let _ = std::fs::remove_file(&final_full);
            return Err(e);
        }

        // Commit: the run is on disk and registered — safe to drop from heap.
        self.in_memory_l0.clear();

        // Bound the spilled disk L0. A repeatedly-spilling table is read only
        // via the non-compacting `open_cursor`, so without this its `eph_shard`s
        // accumulate unbounded and every cursor merges them all.
        // `compact_if_needed` is a no-op until the disk tier crosses its own
        // threshold and skips the manifest (None) for this non-durable table.
        self.compact_if_needed()?;
        Ok(())
    }
}
