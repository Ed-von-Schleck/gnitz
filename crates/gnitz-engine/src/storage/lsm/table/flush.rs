//! Two-phase flush state machine for [`Table`].
//!
//! The flush/spill path carved off `Table`'s ingest/cursor/lookup surface:
//! `flush_to_ram` (ingest overflow — fold the memtable into the RAM tier, no
//! file I/O), `flush_prepare` (Phase 1 of the barrier / durable path — fold,
//! write the shard one-shot at its final name, stage the manifest `.tmp`,
//! returning `FlushWork`), `flush_commit` (Phase 2 — rename the manifest into
//! place), the synchronous `flush` wrapper, and in-memory ceiling enforcement
//! (`compact_in_memory` / `spill_in_memory_to_disk`). `Table`'s fields are read
//! directly here — `flush` is a child module of the `table` module that defines
//! the struct.

use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::rc::Rc;

use super::super::batch::Batch;
use super::super::error::StorageError;
use super::super::manifest::PreparedManifest;
use super::super::memtable;
use super::super::shard_file::{self, PkUniqueChecker};
use super::{
    FlushOutcome, FlushWork, FoundSource, InMemRun, RecoverySource, Table, INMEM_CEILING, INMEM_COMPACT_THRESHOLD,
};
use crate::foundation::posix_io::{fdatasync_eintr, fsync_eintr, open_owned};

impl Table {
    /// Synchronous flush. `Rederive` tables fold into the RAM tier (`DoneInline`);
    /// `SalReplay` tables fold memtable + L0 into one shard written at its final
    /// name (unsynced), fdatasync every unsynced file by path + the manifest
    /// `.tmp`, rename the manifest, fsync the dir, then drain deferred cleanup.
    /// The sole synchronous fsync entry point — used by manual FLUSH and the
    /// system-table checkpoint (which never reach the worker-barrier drain).
    pub fn flush(&mut self) -> Result<bool, StorageError> {
        match self.flush_prepare()? {
            FlushOutcome::Empty => Ok(false),
            FlushOutcome::DoneInline => Ok(true),
            FlushOutcome::Pending(work) => {
                // Sweep: fdatasync every unpublished/unsynced file by path (prior
                // spills + this barrier's folded shard) before the manifest rename.
                for p in work.sync_paths() {
                    let fd = open_owned(p, libc::O_RDONLY).ok_or(StorageError::Io)?;
                    if fdatasync_eintr(fd.as_raw_fd()).is_err() {
                        return Err(StorageError::Io);
                    }
                }
                if fdatasync_eintr(work.manifest_fd()).is_err() {
                    return Err(StorageError::Io);
                }
                // fsync the per-flush dir fd; it is not held for the table's life.
                let dirfd = self.flush_commit(work)?;
                if fsync_eintr(dirfd.as_raw_fd()).is_err() {
                    return Err(StorageError::Io);
                }
                // Deferred compaction cleanup: the manifest now references the
                // compacted index durably, so superseded inputs are safe to
                // unlink. (The worker barrier drains user families instead.)
                self.drain_deletions();
                Ok(true)
            }
        }
    }

    /// Open the partition directory fd on demand (`O_RDONLY|O_DIRECTORY`).
    /// Opened per flush/compaction rather than held for the table's lifetime,
    /// so a 256-partition table pins 0 directory fds at rest instead of 256
    /// (which exhausted the default `ulimit -n` after a handful of tables).
    /// The caller owns the returned `OwnedFd`, which closes it on drop — so an
    /// error `?` anywhere downstream releases it with no manual close.
    pub(super) fn open_dirfd(&self) -> Result<OwnedFd, StorageError> {
        let dir_c = CString::new(self.directory.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let fd = unsafe { libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
        if fd < 0 {
            return Err(StorageError::Io);
        }
        // SAFETY: `fd` is a fresh, valid descriptor from `open`; ownership
        // transfers to the `OwnedFd`, the sole closer from here on.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    // ------------------------------------------------------------------
    // RAM-tier fold (ingest overflow)
    // ------------------------------------------------------------------

    /// Fold the residual memtable into the RAM tier (no spill). Handles the
    /// empty / count==0 cases. `InMemRun::from_batch` builds the per-run bloom.
    fn fold_memtable_into_l0(&mut self) {
        if self.memtable.is_empty() {
            return;
        }
        let snapshot = self.memtable.consolidate_for_flush();
        if snapshot.count > 0 {
            self.cached_full_scan = None;
            self.in_memory_l0.push(InMemRun::from_batch(snapshot));
        }
        self.memtable.reset();
    }

    /// The ingest-overflow path for **every** table: fold the memtable into the
    /// RAM tier, then keep that tier bounded (fold at `>INMEM_COMPACT_THRESHOLD`
    /// runs, spill past `INMEM_CEILING`). No file I/O unless the ceiling is
    /// breached; durability lives in the fsynced SAL until the checkpoint
    /// barrier folds the tier into a durable shard. Both callers (ingest
    /// overflow, `flush_prepare`) reset `found_source` before calling.
    pub(super) fn flush_to_ram(&mut self) -> Result<(), StorageError> {
        self.fold_memtable_into_l0();
        self.enforce_inmem_bound()
    }

    /// Basename and manifest `max_lsn` for the next shard written from the RAM
    /// tier (spill or barrier) — the unified `shard_{tid}_{lsn}.db` naming.
    /// `current_lsn` bumps once per ingest and at most one shard is written per
    /// ingest, so names are unique for this `Table` object. Directories with
    /// several writer processes (secondary-index dirs, the workers' inherited
    /// copies of `_sys` tables) can still collide across processes — tolerable
    /// today only because `Rederive` state is erased at open and readers pin
    /// their own mmap'd inodes; reloading such state requires making those
    /// directories single-writer.
    fn next_shard_name_and_lsn(&self) -> (String, u64) {
        (
            format!("shard_{}_{}.db", self.table_id, self.current_lsn),
            self.current_lsn - 1,
        )
    }

    /// `SHARD_FLAG_PK_UNIQUE` (or 0) for a consolidated run about to become a
    /// shard, computed by observing all rows in sorted order. Only base tables
    /// set `can_tag_pk_unique`; every other table's runs stay untagged ZSet.
    fn shard_flush_flags(&self, run: &Batch) -> u8 {
        if !self.can_tag_pk_unique {
            return 0;
        }
        let mut checker = PkUniqueChecker::new();
        for i in 0..run.count {
            checker.observe(run.get_pk_bytes(i), run.get_weight(i));
        }
        checker.flags_if(true)
    }

    // ------------------------------------------------------------------
    // Barrier / durable flush (two-phase)
    // ------------------------------------------------------------------

    /// Phase 1 of the barrier flush, branching on the table's `recovery_source`.
    ///
    /// `Rederive` (index circuits, views): fold into the RAM tier and return
    /// `DoneInline` — no file I/O, no manifest. These are rebuilt from their
    /// sources at open.
    ///
    /// `SalReplay` (base tables, master system tables): fold the memtable into
    /// the RAM tier, then gate on three disjuncts. The tier is published iff it
    /// holds new state (`in_memory_l0` non-empty after the fold), OR the index
    /// carries unpublished/unsynced spills (`has_unsynced` — else the checkpoint's
    /// global SAL reset would drop acknowledged rows), OR compaction has
    /// superseded files since the last publish (`has_pending_deletions` — else the
    /// deferred drain would unlink files the surviving manifest still references).
    /// When none hold the tier is `Empty` (SAL-recoverable).
    ///
    /// On a publish the folded run (if any) is committed to disk via
    /// `persist_l0_run` — the same commit point the spill path uses. The
    /// barrier's by-path sweep fdatasyncs every unsynced file; `flush_commit`
    /// renames the manifest alone.
    pub fn flush_prepare(&mut self) -> Result<FlushOutcome, StorageError> {
        self.found_source = FoundSource::None;
        if self.recovery_source != RecoverySource::SalReplay {
            self.flush_to_ram()?;
            return Ok(FlushOutcome::DoneInline);
        }
        self.prepare_persist()
    }

    /// The force-persist body shared by the base barrier round and the ephemeral
    /// checkpoint round: fold the memtable into the RAM tier, gate on the three
    /// disjuncts, then commit one folded net-state shard and stage the manifest.
    /// The tier is published iff it holds new state (`in_memory_l0` non-empty
    /// after the fold), OR the index carries unpublished/unsynced spills
    /// (`has_unsynced` — else the checkpoint's global SAL reset would drop
    /// acknowledged rows), OR compaction has superseded files since the last
    /// publish (`has_pending_deletions` — else the deferred drain would unlink
    /// files the surviving manifest still references). When none hold the tier is
    /// `Empty`. Callers reset `found_source` before invoking (the fold reads it).
    fn prepare_persist(&mut self) -> Result<FlushOutcome, StorageError> {
        // Fold-first, then gate, then one shard.
        self.fold_memtable_into_l0();
        self.compact_in_memory();

        if !self.in_memory_l0.is_empty() {
            self.persist_l0_run()?;
        } else if !self.shard_index.has_unsynced() && !self.shard_index.has_pending_deletions() {
            // Nothing ingested since the last checkpoint, no unpublished spills,
            // and nothing compacted — the SAL covers it.
            return Ok(FlushOutcome::Empty);
        }
        // Otherwise fall through to publish: capture unpublished spills into a
        // durable manifest before the SAL reset (else the global reset drops
        // them), and/or republish over a compacted index so the deferred drain
        // can unlink the superseded inputs.

        let sync_paths = self
            .shard_index
            .unsynced_paths()
            .iter()
            .map(|p| CString::new(p.as_str()).map_err(|_| StorageError::InvalidPath))
            .collect::<Result<Vec<_>, _>>()?;
        let manifest = self.prepare_manifest_tmp()?;
        Ok(FlushOutcome::Pending(FlushWork { sync_paths, manifest }))
    }

    /// Phase 1 of the **ephemeral** checkpoint round: force-persist this table's
    /// RAM tier to a durable, generation-stamped shard + manifest regardless of
    /// `recovery_source`. Used for the `RederiveCheckpointed` view operator-trace
    /// tables and output stores the ephemeral round persists (their manifests are
    /// stamped with the checkpoint generation via
    /// `worker_ctx::committed_generation`). The retained three-disjunct gate
    /// means a table dirtied only by deferred compaction still republishes.
    pub fn flush_prepare_ephemeral(&mut self) -> Result<FlushOutcome, StorageError> {
        self.found_source = FoundSource::None;
        self.prepare_persist()
    }

    /// Commit the RAM tier's single folded net-state run (left by
    /// `compact_in_memory`, count > 0) to disk: write it **unsynced** at its
    /// final `shard_{tid}_{lsn}.db` name, register it in the index, mark it for
    /// the barrier's by-path fdatasync sweep, and drop it from heap. The shared
    /// commit point of the spill and barrier paths.
    ///
    /// Transactional. The run is borrowed — not removed — so a write failure
    /// leaves heap intact for retry with nothing on disk; a registration
    /// failure unlinks the just-written shard before returning. Heap is cleared
    /// only once the shard is written and registered.
    fn persist_l0_run(&mut self) -> Result<(), StorageError> {
        let run = Rc::clone(
            &self
                .in_memory_l0
                .first()
                .expect("compact_in_memory leaves one net-state run")
                .batch,
        );
        let (shard_name, lsn_max) = self.next_shard_name_and_lsn();
        let name_c = CString::new(shard_name.as_str()).map_err(|_| StorageError::InvalidPath)?;
        let flush_flags = self.shard_flush_flags(&run);

        let dirfd = self.open_dirfd()?;
        let res = shard_file::write_shard_streaming(
            dirfd.as_raw_fd(),
            &name_c,
            self.table_id,
            run.count as u32,
            &run.regions(),
            false, // unsynced; the barrier sweep fdatasyncs it by path
            flush_flags,
        );
        drop(dirfd);
        res?; // Write failed: heap still owns `run`; no on-disk residue.

        // Register with real LSNs so a reopen seeds `current_lsn = max_lsn() + 1`.
        let final_full = format!("{}/{}", self.directory, shard_name);
        if let Err(e) = self.shard_index.add_shard(&final_full, 0, lsn_max) {
            // Registration failed: unlink the shard we wrote, keep heap intact.
            let _ = std::fs::remove_file(&final_full);
            return Err(e);
        }
        // Unsynced on disk: the next barrier's sweep fdatasyncs it (and the gate's
        // `has_unsynced` disjunct forces that barrier before any SAL reset).
        self.shard_index.mark_unsynced(&final_full);

        // Commit: the run is on disk and registered — safe to drop from heap.
        self.in_memory_l0.clear();
        Ok(())
    }

    /// Stage the manifest `.tmp` over the current index for the barrier to
    /// rename. The barrier's one-shot shard write already registered its shard
    /// via `add_shard`, so the live index is authoritative.
    fn prepare_manifest_tmp(&self) -> Result<PreparedManifest, StorageError> {
        let manifest_c = CString::new(self.manifest_full_path()).map_err(|_| StorageError::InvalidPath)?;
        self.shard_index.prepare_manifest(&manifest_c)
    }

    /// Phase 2: rename the manifest `.tmp` into place and return the per-flush
    /// directory fd for the caller to `fsync` after all renames in the worker
    /// batch. The folded shard was already written at its final name and
    /// registered by `flush_prepare`, the RAM tier already cleared, and the
    /// barrier sweep has fdatasync'd every unsynced file — so all that remains is
    /// the manifest rename. On a rename failure the `.tmp` is unlinked by
    /// `PreparedManifest`'s Drop and the shard survives as an orphan (GC'd next
    /// open); every fd is released through its `OwnedFd` on every path.
    pub(crate) fn flush_commit(&mut self, work: FlushWork) -> Result<OwnedFd, StorageError> {
        let mut m = work.manifest;
        let rc = unsafe { libc::rename(m.tmp_path.as_ptr(), m.final_path.as_ptr()) };
        if rc < 0 {
            // `m` drops here: `PreparedManifest`'s Drop unlinks the .tmp.
            return Err(StorageError::Io);
        }
        m.committed = true; // renamed; suppress the .tmp unlink in Drop

        // The files in `unsynced` were fdatasync'd by the barrier sweep and are
        // now referenced by the renamed manifest; clear so the next barrier does
        // not re-sync already-durable files. No concurrent writer: single-threaded
        // worker, barrier holds sal_writer_excl.
        self.shard_index.clear_unsynced();
        self.open_dirfd()
    }

    // ------------------------------------------------------------------
    // In-memory run set bounding + spill
    // ------------------------------------------------------------------

    /// The effective per-table heap ceiling — `INMEM_CEILING` in production, or
    /// the test override when one is set.
    fn inmem_ceiling(&self) -> usize {
        #[cfg(test)]
        {
            if let Some(c) = self.inmem_ceiling_override {
                return c;
            }
        }
        INMEM_CEILING
    }

    /// Heap footprint of `in_memory_l0` — the byte total that drives the spill
    /// ceiling. Recomputed on demand rather than cached: the run set is bounded
    /// by `INMEM_COMPACT_THRESHOLD`, so this is a handful of `total_bytes()`
    /// arithmetic calls, with no cached field for every mutation of
    /// `in_memory_l0` to keep in sync.
    pub(super) fn in_memory_bytes(&self) -> usize {
        self.in_memory_l0.iter().map(|r| r.batch.total_bytes()).sum()
    }

    /// Keep `in_memory_l0` bounded after a fold into the RAM tier. Called by
    /// `flush_to_ram` (the only site that grows the set), so on return
    /// `in_memory_l0.len() <= INMEM_COMPACT_THRESHOLD` and
    /// `in_memory_bytes() <= INMEM_CEILING` always hold.
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
        let batches: Vec<Rc<Batch>> = self.in_memory_l0.iter().map(|r| Rc::clone(&r.batch)).collect();
        let merged = memtable::consolidate_runs(&batches, &self.schema);
        self.in_memory_l0.clear();
        if merged.count > 0 {
            self.in_memory_l0.push(InMemRun::from_batch(merged)); // rebuilds run bloom
        }
    }

    /// Ceiling breach: fold to net state first. Folding cancels cross-flush
    /// churn (an insert in flush N against its retraction in N+1) and can
    /// reclaim enough to fall back under the ceiling — in which case this
    /// returns without touching disk. Otherwise commit the folded run to disk
    /// (`persist_l0_run`), then compact the disk tier.
    ///
    /// Spills are written **unsynced** (`durable = false`) and marked in the
    /// index's `unsynced` set. For `SalReplay` the next barrier fdatasyncs them
    /// by path before publishing (the `has_unsynced` gate disjunct guarantees a
    /// barrier fires while unpublished spills exist, so the checkpoint's global
    /// SAL reset never drops an acknowledged spill); the PK-unique tag stays (a
    /// read-path property, not a durability one). Rederive spills publish no
    /// manifest and are erased+rebuilt at open, so their `unsynced` marks are
    /// pruned by disk compaction and never swept.
    ///
    /// After a spill the table carries disk runs + future heap runs; cross-tier
    /// churn does NOT fold (neither `compact_in_memory`, which is heap-only, nor
    /// `run_compact`, which is disk-only, sees both). This is bounded: it only
    /// occurs for tables that breached the ceiling, affects disk footprint not
    /// heap (heap stays <= ceiling by construction), and the disk tier still
    /// self-compacts.
    fn spill_in_memory_to_disk(&mut self) -> Result<(), StorageError> {
        self.compact_in_memory();
        // Folding may have dropped the net state back under the ceiling (heavy
        // churn cancels to near-nothing); if so there is nothing to spill.
        // Above the ceiling the byte total is > 0, so `compact_in_memory` left
        // exactly one net-state run (count > 0) in heap — `flush_to_ram` never
        // enqueues an empty snapshot and `compact_in_memory` only re-pushes
        // `merged.count > 0`.
        if self.in_memory_bytes() <= self.inmem_ceiling() {
            return Ok(());
        }
        self.persist_l0_run()?;

        // Bound the spilled disk L0. A repeatedly-spilling table is read only
        // via the non-compacting `open_cursor`, so without this its shards
        // accumulate unbounded and every cursor merges them all.
        // `compact_if_needed` publishes no manifest: it defers cleanup to the
        // next barrier for SalReplay and drains inline for Rederive.
        self.compact_if_needed()?;
        Ok(())
    }
}
