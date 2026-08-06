//! Two-phase flush state machine for [`Table`].
//!
//! The flush/spill path carved off `Table`'s ingest/cursor/lookup surface:
//! `flush_to_ram` (ingest overflow — fold the memtable into the RAM tier, no
//! file I/O), `flush_prepare` (Phase 1 of the barrier / durable path — fold,
//! write the shard one-shot at its final name, stage the manifest `.tmp`,
//! returning `FlushWork`), `flush_commit` (Phase 2 — rename the manifest into
//! place), the synchronous `flush` wrapper, and the ceiling spill
//! (`spill_in_memory_to_disk`). `Table`'s fields are read directly here — `flush`
//! is a child module of the `table` module that defines the struct.

use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use super::super::batch::Batch;
use super::super::error::StorageError;
use super::super::flush_barrier::FlushRound;
use super::super::shard_file;
use super::{FlushOutcome, FlushWork, RecoverySource, Table};
use crate::foundation::posix_io::open_owned;

impl Table {
    /// Synchronous flush of this one table through the shared barrier:
    /// `Rederive` tables fold into the RAM tier and publish nothing; a
    /// `SalReplay` table folds memtable + L0 into one shard, syncs it and the
    /// staged manifest, renames the manifest into place, fsyncs the directory,
    /// and drains its deferred compaction cleanup. Used by manual FLUSH and the
    /// system-table checkpoint, which never reach the worker's checkpoint round.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        super::super::flush_barrier::flush_barrier([self as *mut Table], super::super::flush_barrier::FlushRound::Base)
    }

    /// Open the partition directory fd on demand (`O_RDONLY|O_DIRECTORY`).
    /// Opened per flush/compaction rather than held for the table's lifetime,
    /// so a 256-partition table pins 0 directory fds at rest instead of 256
    /// (which exhausted the default `ulimit -n` after a handful of tables).
    /// The caller owns the returned `OwnedFd`, which closes it on drop — so an
    /// error `?` anywhere downstream releases it with no manual close.
    ///
    /// An absent directory is created here: a `Rederive` table opens dirless
    /// (`Table::new` defers the create because in steady state such a table
    /// never writes a file), and this is the single choke point every file
    /// write goes through. NOCOW (btrfs; silently ignored elsewhere) is applied
    /// to every opened fd — a cheap idempotent ioctl, and the one place that
    /// covers dirs created lazily here as well as dirs pre-created by the
    /// catalog's layout staging (index dirs), so files written into either
    /// inherit the flag.
    pub(super) fn open_dirfd(&self) -> Result<OwnedFd, StorageError> {
        let dir_c = super::super::cstr(self.directory.as_str())?;
        let fd = match open_owned(&dir_c, libc::O_RDONLY | libc::O_DIRECTORY) {
            Some(fd) => fd,
            None => {
                let dir_c = super::ensure_dir(&self.directory)?;
                open_owned(&dir_c, libc::O_RDONLY | libc::O_DIRECTORY).ok_or(StorageError::Io)?
            }
        };
        crate::foundation::posix_io::try_set_nocow(fd.as_raw_fd());
        Ok(fd)
    }

    // ------------------------------------------------------------------
    // RAM-tier fold (ingest overflow)
    // ------------------------------------------------------------------

    /// Fold the residual memtable into the RAM tier (no spill).
    fn fold_memtable_into_l0(&mut self) {
        if let Some(run) = self.memtable.fold_to_single(&self.schema) {
            self.cached_full_scan = None;
            self.ram_tier.push(run, &self.schema);
        }
        self.memtable.clear();
    }

    /// The ingest-overflow path for **every** table: fold the memtable into the
    /// RAM tier, then spill that tier if it is over its ceiling. No file I/O
    /// unless the ceiling is breached; durability lives in the fsynced SAL until
    /// the checkpoint barrier folds the tier into a durable shard.
    pub(super) fn flush_to_ram(&mut self) -> Result<(), StorageError> {
        self.fold_memtable_into_l0();
        if self.ram_tier.is_full() {
            self.spill_in_memory_to_disk()?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Barrier / durable flush (two-phase)
    // ------------------------------------------------------------------

    /// Phase 1 of the barrier flush: fold the memtable into the RAM tier, decide
    /// whether to publish, and if so commit one folded net-state shard and stage
    /// the manifest. The barrier's by-path sweep fdatasyncs every unsynced file;
    /// `flush_commit` renames the manifest alone.
    ///
    /// A rederived table on the **base** round publishes nothing — it is rebuilt
    /// from its sources at open — and just folds to RAM inline.
    ///
    /// Otherwise the tier is published iff it holds new state (the RAM tier is
    /// non-empty after the fold), OR the index carries unpublished/unsynced
    /// spills (`has_unsynced` — else the checkpoint's global SAL reset would drop
    /// acknowledged rows), OR compaction has superseded files since the last
    /// publish (`has_pending_deletions` — else the deferred drain would unlink
    /// files the surviving manifest still references).
    ///
    /// The **ephemeral** round overrides that gate and publishes unconditionally,
    /// even for an unchanged or empty partition: the boot resume verdict
    /// (`compute_invalid_views`) and the per-partition conditional load
    /// (`Table::new`) both require **every** view partition's manifest to carry
    /// the current checkpoint generation. A gated round would leave an empty
    /// partition with no manifest and an unchanged partition at an older
    /// generation, and the verdict would then reject the whole (valid) view every
    /// restart — resume would never trigger.
    pub(in crate::storage) fn flush_prepare(&mut self, round: FlushRound) -> Result<FlushOutcome, StorageError> {
        let generation = round.generation();
        if generation.is_none() && self.recovery_source != RecoverySource::SalReplay {
            self.flush_to_ram()?;
            return Ok(FlushOutcome::Done);
        }

        // Fold-first, then gate, then one shard.
        self.fold_memtable_into_l0();
        if let Some(run) = self.ram_tier.fold_to_single(&self.schema) {
            self.persist_l0_run(run)?;
        } else if generation.is_none() && !self.shard_index.has_unsynced() && !self.shard_index.has_pending_deletions()
        {
            // Nothing ingested since the last checkpoint, no unpublished spills,
            // and nothing compacted — the SAL covers it (base round only).
            return Ok(FlushOutcome::Done);
        }
        // Otherwise publish: capture unpublished spills into a durable manifest
        // before the SAL reset (else the global reset drops them), republish over
        // a compacted index so the deferred drain can unlink the superseded
        // inputs, and — on the ephemeral round — re-stamp an unchanged or empty
        // partition at the current generation.
        let sync_paths = super::super::shard_index::to_cstrings(self.shard_index.unsynced_paths())?;
        let manifest_c = super::super::cstr(self.manifest_full_path())?;
        let manifest = self
            .shard_index
            .prepare_manifest(&manifest_c, generation.unwrap_or(0))?;
        Ok(FlushOutcome::Pending(FlushWork { sync_paths, manifest }))
    }

    /// Commit the RAM tier's single folded net-state run to disk at its final
    /// `shard_{tid}_{lsn}.db` name, register it, drop it from heap, and compact
    /// if it pushed the disk tier over its threshold. The shared commit point of
    /// the spill and barrier paths.
    ///
    /// The name is unique for this `Table`: `current_lsn` bumps once per ingest
    /// and at most one shard is written per ingest. Two *processes* writing one
    /// directory (secondary-index dirs, the workers' inherited `SalReplay` `_sys`
    /// copies) can still collide — those directories need a single writer.
    ///
    /// Transactional. The run is borrowed — not removed — so a write failure
    /// leaves heap intact for retry with nothing on disk; a registration
    /// failure unlinks the just-written shard before returning. Heap is cleared
    /// only once the shard is written and registered.
    fn persist_l0_run(&mut self, run: Rc<Batch>) -> Result<(), StorageError> {
        let shard_name = super::super::naming::spill_shard_name(self.table_id, self.current_lsn);
        let lsn_max = self.current_lsn - 1;
        let name_c = super::super::cstr(shard_name.as_str())?;

        let dirfd = self.open_dirfd()?;
        let res = shard_file::write_shard_streaming(
            dirfd.as_raw_fd(),
            &name_c,
            run.count as u32,
            &run.regions(),
            &self.schema,
            // L0 spill/checkpoint shards stay plain (no FoR packing).
            shard_file::ShardWriteOpts::default(),
        );
        drop(dirfd);
        res?; // Write failed: heap still owns `run`; no on-disk residue.

        // Real LSNs, so a reopen seeds `current_lsn = max_lsn() + 1`.
        let final_full = format!("{}/{}", self.directory, shard_name);
        if let Err(e) = self.shard_index.add_unsynced_shard(&final_full, lsn_max) {
            // Registration failed: unlink the shard we wrote, keep heap intact.
            let _ = std::fs::remove_file(&final_full);
            return Err(e);
        }

        // Commit: the run is on disk and registered — safe to drop from heap.
        self.ram_tier.clear();

        // The only path that grows L0, so the only place its fan-in can cross
        // `L0_COMPACT_THRESHOLD`. Publishes no manifest, so a barrier caller
        // stages one describing the already-compacted index.
        self.compact_if_needed()
    }

    /// Phase 2: rename the manifest `.tmp` into place and return the per-flush
    /// directory fd for the caller to `fsync` after all renames in the worker
    /// batch. The folded shard was already written at its final name and
    /// registered by `flush_prepare`, the RAM tier already cleared, and the
    /// barrier sweep has fdatasync'd every unsynced file — so all that remains is
    /// the manifest rename. On a rename failure the `.tmp` is unlinked by
    /// `PreparedManifest`'s Drop and the shard survives as an orphan (GC'd next
    /// open); every fd is released through its `OwnedFd` on every path.
    pub(in crate::storage) fn flush_commit(&mut self, work: FlushWork) -> Result<OwnedFd, StorageError> {
        work.manifest.commit()?;

        // The files in `unsynced` were fdatasync'd by the barrier sweep and are
        // now referenced by the renamed manifest; clear so the next barrier does
        // not re-sync already-durable files. No concurrent writer: single-threaded
        // worker, barrier holds sal_writer_excl.
        self.shard_index.clear_unsynced();
        self.open_dirfd()
    }

    /// Ceiling breach: fold the RAM tier to net state first. Folding cancels
    /// cross-flush churn (an insert in flush N against its retraction in N+1)
    /// and can reclaim enough to fall back under the ceiling — in which case
    /// this returns without touching disk. Otherwise commit the folded run
    /// (`persist_l0_run`), which also bounds the disk tier: a repeatedly-spilling
    /// table is read only via the non-compacting `open_cursor`, so without that
    /// its shards would accumulate unbounded and every cursor would merge them
    /// all.
    ///
    /// Spills are written **unsynced** and marked in the index's `unsynced` set.
    /// For `SalReplay` the next barrier fdatasyncs them by path before publishing
    /// (the `has_unsynced` gate disjunct guarantees a barrier fires while
    /// unpublished spills exist, so the checkpoint's global SAL reset never drops
    /// an acknowledged spill). Rederive spills publish no manifest and are
    /// erased+rebuilt at open, so their `unsynced` marks are pruned by disk
    /// compaction and never swept.
    ///
    /// After a spill the table carries disk runs + future heap runs; cross-tier
    /// churn does NOT fold (the RAM-tier fold is heap-only and `run_compact` is
    /// disk-only, so neither sees both). This is bounded: it only occurs for
    /// tables that breached the ceiling, affects disk footprint not heap (heap
    /// stays <= ceiling by construction), and the disk tier still self-compacts.
    fn spill_in_memory_to_disk(&mut self) -> Result<(), StorageError> {
        let Some(run) = self.ram_tier.fold_to_single(&self.schema) else {
            return Ok(());
        };
        // Folding may have dropped the net state back under the ceiling (heavy
        // churn cancels to near-nothing); if so there is nothing to spill.
        if !self.ram_tier.is_full() {
            return Ok(());
        }
        self.persist_l0_run(run)
    }
}
