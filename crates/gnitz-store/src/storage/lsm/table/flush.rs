//! Two-phase flush state machine for [`Table`].
//!
//! The flush/spill path carved off `Table`'s ingest/cursor/lookup surface: the
//! ingest-overflow fold and its ceiling spill, the barrier's prepare/commit pair
//! and the synchronous `flush` wrapper that drives one table through it, and the
//! shard commit both the spill and the barrier share. `Table`'s fields are read
//! directly here — `flush` is a child module of the `table` module that defines
//! the struct.

use std::os::fd::OwnedFd;
use std::rc::Rc;

use super::super::batch::Batch;
use super::super::error::StorageError;
use super::super::flush_barrier::FlushRound;
use super::super::shard_file;
use super::{FlushWork, Table};

impl Table {
    /// Synchronous flush of this one table through the shared barrier. It runs a
    /// **base** round, so a `Rederive` table only folds into the RAM tier; a
    /// `SalReplay` table folds memtable + RAM tier into one shard, syncs it and
    /// the staged manifest, renames the manifest into place, fsyncs the
    /// directory, and drains its deferred compaction cleanup.
    ///
    /// Its one production caller is the post-backfill fold, and that runs on a
    /// view — so the durable arm is reached from tests alone.
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        super::super::flush_barrier::flush_barrier([&mut *self], FlushRound::Base)
    }

    // ------------------------------------------------------------------
    // RAM-tier fold (ingest overflow)
    // ------------------------------------------------------------------

    /// Fold the residual memtable into the RAM tier (no spill).
    /// `cached_full_scan` survives it: both tiers are merged and
    /// ghost-eliminated alike, so the row set does not move.
    fn fold_memtable_into_ram_tier(&mut self) {
        if let Some(run) = self.memtable.fold_to_single(&self.shard_index.schema) {
            self.ram_tier.push(run, &self.shard_index.schema);
        }
        self.memtable.clear();
    }

    /// The ingest-overflow path for **every** table: fold the memtable into the
    /// RAM tier, and spill that tier only if folding it to net state leaves it
    /// still over the ceiling — churn routinely cancels back under. Durability
    /// lives in the fsynced SAL until a barrier writes the durable shard, so
    /// nothing here touches disk unless the ceiling really is breached.
    ///
    /// Spills are written **unsynced** and register as owing a sweep: a
    /// `SalReplay` table's are fdatasync'd by the next barrier, a `Rederive`
    /// table's by the ephemeral round, and the latter are erased and rebuilt at
    /// open if no round reaches them.
    ///
    /// A spilled table then holds disk runs *and* heap runs, and churn across the
    /// two never cancels — the RAM-tier fold is heap-only, `run_compact` is
    /// disk-only. That costs disk footprint, not heap (which stays ≤ the ceiling
    /// by construction), and the disk tier still self-compacts.
    pub(crate) fn flush_to_ram(&mut self) -> Result<(), StorageError> {
        self.fold_memtable_into_ram_tier();
        if !self.ram_tier.is_full() {
            return Ok(());
        }
        let Some(run) = self.ram_tier.fold_to_single(&self.shard_index.schema) else {
            return Ok(());
        };
        if !self.ram_tier.is_full() {
            return Ok(());
        }
        self.persist_ram_tier(run)
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
    /// from its sources at open — and just folds to RAM inline. The base round
    /// stamps generation 0, so a manifest published here would be the one a
    /// `Rederive` open accepts while the resume generation is still 0: the view
    /// would resume from a base-round snapshot its operator traces never matched,
    /// and the replayed delta would land twice.
    ///
    /// Every other table publishes on both rounds, even when unchanged or empty,
    /// because two boot decisions read "has a manifest" as a fact about the
    /// cluster: the resume verdict wants every view child at the current
    /// generation, and the relayout wants a complete `w{k}of{n}` set. A gated
    /// round would leave a skewed relation's empty children indistinguishable
    /// from a relation that had never been checkpointed. That costs `W` manifests
    /// per relation per checkpoint, whose `fdatasync`s `flush_barrier` batches
    /// through one ring.
    pub(in crate::storage) fn flush_prepare(&mut self, round: FlushRound) -> Result<Option<FlushWork>, StorageError> {
        if matches!(round, FlushRound::Base) && self.is_rederived() {
            self.flush_to_ram()?;
            return Ok(None);
        }

        // Fold-first, then one shard.
        self.fold_memtable_into_ram_tier();
        if let Some(run) = self.ram_tier.fold_to_single(&self.shard_index.schema) {
            self.persist_ram_tier(run)?;
        }
        // Publish: capture unpublished spills into a durable manifest before the
        // SAL reset (else the global reset drops them), republish over a compacted
        // index so the deferred drain can unlink the superseded inputs, and
        // re-stamp an unchanged or empty child.
        let sync_paths = super::super::to_cstrings(self.shard_index.unsynced_paths())?;
        let manifest_c = super::super::cstr(self.manifest_full_path())?;
        let manifest = self.shard_index.prepare_manifest(&manifest_c, round.checkpoint_gen())?;
        Ok(Some(FlushWork { sync_paths, manifest }))
    }

    /// Commit the RAM tier's single folded net-state run to disk at its final
    /// `shard_{tid}_{lsn}.db` name, register it, drop it from heap, and compact
    /// if it pushed the disk tier over its threshold. The shared commit point of
    /// the spill and barrier paths.
    ///
    /// The name is unique for this `Table`: `current_lsn` bumps once per ingest
    /// and at most one shard is written per ingest, so a directory needs a single
    /// writing process. The boot repartition writes through the compaction
    /// grammar instead, because it stamps a manifest LSN floor below the names it
    /// wrote and this counter is re-derived from that floor at the next open.
    ///
    /// Transactional. The run is borrowed — not removed — so a write failure
    /// leaves heap intact for retry with nothing on disk; a registration
    /// failure unlinks the just-written shard before returning. Heap is cleared
    /// only once the shard is written and registered.
    fn persist_ram_tier(&mut self, run: Rc<Batch>) -> Result<(), StorageError> {
        let shard_name = super::super::naming::spill_shard_name(self.shard_index.table_id, self.current_lsn);
        let lsn_max = self.current_lsn - 1;
        // Real LSNs, so a reopen seeds `current_lsn = max_lsn() + 1`.
        let final_full = format!("{}/{}", self.shard_index.output_dir, shard_name);
        let full_c = super::super::cstr(final_full.as_str())?;

        // Write failed: heap still owns `run`; no on-disk residue.
        run.write_as_shard(
            &full_c,
            // L0 spill/checkpoint shards stay plain (no FoR packing), and carry
            // a PK filter only where something point-probes this store.
            shard_file::ShardWriteOpts {
                skip_pk_filter: self.shard_index.skip_pk_filter(),
                ..Default::default()
            },
        )?;

        if let Err(e) = self.shard_index.add_unsynced_shard(&final_full, lsn_max) {
            // Registration failed: unlink the shard we wrote, keep heap intact.
            let _ = std::fs::remove_file(&final_full);
            return Err(e);
        }

        // Commit: the run is on disk and registered — safe to drop from heap.
        self.ram_tier.clear();
        // The capacity sweep below can dehydrate or drop live rows, so this is
        // where the materialized scan stops being a copy of the row set.
        self.cached_full_scan.set(None);

        // The only path that grows L0, so the only place its fan-in can cross
        // `L0_COMPACT_THRESHOLD`. Publishes no manifest, so a barrier caller
        // stages one describing the already-compacted index.
        self.compact_if_needed()?;

        // The one capacity trigger, and it sits here rather than inside
        // `compact_if_needed` because that one's `should_compact` early return
        // would skip exactly the spills that did not also cross the file-count
        // threshold. This is the only place a store's shard bytes can grow, so
        // one trigger covers every store.
        self.shard_index.enforce_capacity()
    }

    /// Phase 2: rename the manifest `.tmp` into place and return the per-flush
    /// directory fd for the caller to `fsync` after all renames in the worker
    /// batch. The folded shard was already written at its final name and
    /// registered by `flush_prepare`, the RAM tier already cleared, and the
    /// barrier sweep has fdatasync'd every unsynced file — so all that remains is
    /// the manifest rename. On a rename failure the `.tmp` is unlinked by
    /// `StagedFile`'s Drop and the shard survives as an orphan (GC'd next
    /// open); every fd is released through its `OwnedFd` on every path.
    pub(in crate::storage) fn flush_commit(&mut self, work: FlushWork) -> Result<OwnedFd, StorageError> {
        work.manifest.commit()?;

        // The files the sweep list named were fdatasync'd by the barrier and are
        // now referenced by the renamed manifest; clear so the next barrier does
        // not re-sync already-durable files. No concurrent writer: single-threaded
        // worker, and the master's checkpoint round holds its SAL writer.
        self.shard_index.clear_unsynced();
        super::open_table_dirfd(&self.shard_index.output_dir)
    }
}
