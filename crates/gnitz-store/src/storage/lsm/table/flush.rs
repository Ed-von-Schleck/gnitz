//! [`Table`]'s flush path: the overflow fold and spill, and the barrier's prepare/commit.

use std::path::Path;
use std::rc::Rc;

use super::super::batch::Batch;
use super::super::error::StorageError;
use super::super::manifest;
use super::super::shard_file;
use super::{FlushWork, Table};

impl Table {
    /// The base round's barrier over this one table.
    #[cfg(test)]
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        assert!(!self.is_rederived(), "the base round never visits a rederived table");
        super::super::flush_barrier::flush_barrier([&mut *self], 0)
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

    /// Fold the memtable into the RAM tier, spilling the tier to an unsynced
    /// shard only if its net state is still over the ceiling.
    pub(crate) fn flush_to_ram(&mut self) -> Result<(), StorageError> {
        self.fold_memtable_into_ram_tier();
        if !self.ram_tier.is_full() {
            return Ok(());
        }
        let Some(run) = self.ram_tier.fold_to_single(&self.shard_index.schema) else {
            return Ok(());
        };
        if self.held_in_ram || !self.ram_tier.is_full() {
            return Ok(());
        }
        self.persist_ram_tier(run)
    }

    // ------------------------------------------------------------------
    // Barrier / durable flush (two-phase)
    // ------------------------------------------------------------------

    /// Fold memtable and RAM tier into one shard and stage the manifest naming
    /// it; `None` when that manifest is the one this process last made durable.
    pub(in crate::storage) fn flush_prepare(&mut self, checkpoint_gen: u64) -> Result<Option<FlushWork>, StorageError> {
        // Fold-first, then one shard.
        self.fold_memtable_into_ram_tier();
        if let Some(run) = self.ram_tier.fold_to_single(&self.shard_index.schema) {
            self.persist_ram_tier(run)?;
        }
        let bytes = manifest::encode(&self.shard_index.manifest(checkpoint_gen));
        if self.durable_manifest.as_deref() == Some(&bytes[..]) {
            debug_assert!(
                self.unsynced_paths().next().is_none(),
                "a durable manifest names an unsynced shard"
            );
            return Ok(None);
        }
        let first_publish = self.durable_manifest.is_none();
        // Unknown until `published_durably`: a failed publish may have renamed.
        self.durable_manifest = None;
        let manifest = manifest::prepare(&self.shard_index.output_dir, &bytes)?;
        // A first publish also makes the store's and its relation's directory entries durable.
        let entry_dirs = if first_publish { 2 } else { 0 };
        let dirs = Path::new(&self.shard_index.output_dir)
            .ancestors()
            .take(1 + entry_dirs)
            .filter(|d| !d.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .collect();
        Ok(Some(FlushWork { manifest, bytes, dirs }))
    }

    /// Every live shard no published manifest names yet.
    pub(in crate::storage) fn unsynced_paths(&self) -> impl Iterator<Item = &str> {
        self.shard_index.unsynced_paths()
    }

    /// Write the RAM tier's folded run as an unsynced shard and move it from heap
    /// to the shard index.
    fn persist_ram_tier(&mut self, run: Rc<Batch>) -> Result<(), StorageError> {
        let shard_name = super::super::naming::spill_shard_name(self.current_lsn);
        let lsn_max = self.current_lsn - 1;
        // So a reopen seeds `current_lsn` above every spill name.
        let final_full = format!("{}/{}", self.shard_index.output_dir, shard_name);
        debug_assert!(
            !Path::new(&final_full).exists(),
            "a second shard at one LSN would replace the first"
        );

        // Write failed: heap still owns `run`; no on-disk residue.
        run.write_as_shard(
            &final_full,
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

        self.compact_if_needed()?;
        self.shard_index.enforce_capacity()
    }

    /// Rename the staged manifest into place, marking every shard it names published.
    pub(in crate::storage) fn flush_commit(&mut self, manifest: super::super::StagedFile) -> Result<(), StorageError> {
        manifest.commit()?;
        self.shard_index.clear_unsynced();
        Ok(())
    }
}
