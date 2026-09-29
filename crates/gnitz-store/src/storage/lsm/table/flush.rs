//! [`Table`]'s flush path: the overflow fold and spill, and the multi-table
//! durable flush barrier.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use io_uring::types::FsyncFlags;

use super::super::batch_fsync::{new_ring, sync_paths};
use super::super::manifest::{self, Manifest};
use super::super::run_set::TrimmedRun;
use super::Table;
use crate::storage::error::StorageError;

/// A publish [`Table::flush_prepare`] staged and [`flush_barrier`] completes.
pub(super) struct FlushWork {
    manifest_tmp: String,
    bytes: Vec<u8>,
    /// The shard index's last seq when the manifest was built.
    names_through: u64,
    /// Fsynced once the manifest is renamed.
    dirs: Vec<PathBuf>,
}

/// Durably publish every table in `tables`, each manifest carrying
/// `checkpoint_mark`.
pub(crate) fn flush_barrier<'a>(
    tables: impl IntoIterator<Item = &'a mut Table>,
    checkpoint_mark: u64,
) -> Result<(), StorageError> {
    let mut work: Vec<(&'a mut Table, FlushWork)> = Vec::new();
    for t in tables {
        if let Some(w) = t.flush_prepare(checkpoint_mark)? {
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
            .flat_map(|(t, w)| t.shard_index.unsynced_paths().chain([w.manifest_tmp.clone()])),
        FsyncFlags::DATASYNC,
    )?;
    let mut dirs = BTreeSet::new();
    let mut published = Vec::with_capacity(work.len());
    for (t, w) in work {
        // The rename publishes every shard the manifest names.
        manifest::commit(&t.shard_index.output_dir)?;
        t.shard_index.mark_published(w.names_through);
        dirs.extend(w.dirs);
        published.push((t, w.bytes));
    }
    // A rename is metadata: a full fsync, not fdatasync.
    sync_paths(&mut ring, &dirs, FsyncFlags::empty())?;
    // No durable manifest names a superseded shard any more.
    for (t, bytes) in published {
        t.durable_manifest = Some(bytes);
        t.shard_index.unlink_retired();
    }
    Ok(())
}

impl Table {
    /// The base round's barrier over this one table.
    #[cfg(test)]
    pub(crate) fn flush(&mut self) -> Result<(), StorageError> {
        assert!(!self.is_rederived(), "the base round never visits a rederived table");
        flush_barrier([&mut *self], 0)
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
    pub(crate) fn fold_to_ram(&mut self) -> Result<(), StorageError> {
        self.fold_memtable_into_ram_tier();
        if self.held_in_ram || !self.ram_tier.is_full() {
            return Ok(());
        }
        let Some(run) = self.ram_tier.fold_to_single(&self.shard_index.schema) else {
            return Ok(());
        };
        // The fold's cancellation can bring the tier back under its ceiling.
        if !self.ram_tier.is_full() {
            return Ok(());
        }
        self.spill_ram_tier(run)
    }

    // ------------------------------------------------------------------
    // Barrier prepare
    // ------------------------------------------------------------------

    /// Fold memtable and RAM tier into one shard and stage the manifest naming
    /// it; `None` when that manifest is the one this process last made durable.
    pub(super) fn flush_prepare(&mut self, checkpoint_mark: u64) -> Result<Option<FlushWork>, StorageError> {
        // Fold-first, then one shard.
        self.fold_memtable_into_ram_tier();
        if let Some(run) = self.ram_tier.fold_to_single(&self.shard_index.schema) {
            self.spill_ram_tier(run)?;
        }
        let names_through = self.shard_index.last_seq();
        let bytes = manifest::encode(&Manifest {
            checkpoint_mark,
            caller_record: self.caller_record.clone(),
            shards: self.shard_index.shard_set(),
        });
        if self.durable_manifest.as_deref() == Some(&bytes[..]) {
            debug_assert!(
                self.shard_index.unsynced_paths().next().is_none(),
                "a durable manifest names an unsynced shard"
            );
            return Ok(None);
        }
        let first_publish = self.durable_manifest.is_none();
        // Unknown until the barrier records it: a failed publish may have renamed.
        self.durable_manifest = None;
        let manifest_tmp = manifest::prepare(&self.shard_index.output_dir, &bytes)?;
        // A first publish also makes the store's and its relation's directory entries durable.
        let entry_dirs = if first_publish { 2 } else { 0 };
        let dirs = Path::new(&self.shard_index.output_dir)
            .ancestors()
            .take(1 + entry_dirs)
            .filter(|d| !d.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .collect();
        Ok(Some(FlushWork { manifest_tmp, bytes, names_through, dirs }))
    }

    /// Move the RAM tier's folded run to an unsynced L0 shard, then run the disk
    /// tier's upkeep.
    fn spill_ram_tier(&mut self, run: TrimmedRun) -> Result<(), StorageError> {
        self.shard_index.append_l0_run(&run)?;
        self.ram_tier.clear();
        // Free the spilled rows before compaction allocates.
        drop(run);
        // The sweep in `maintain` can dehydrate or drop live rows.
        self.cached_full_scan.set(None);
        self.shard_index.maintain()
    }
}
