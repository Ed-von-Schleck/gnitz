//! [`Table`]'s flush path: the overflow fold and spill, and the multi-table
//! durable flush barrier.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use io_uring::types::FsyncFlags;

use super::super::batch_fsync::{new_ring, sync_paths};
use super::super::manifest::{self, Manifest};
use super::Table;
use gnitz_zset::repr::StorageError;

/// A publish [`Table::flush_prepare`] staged and [`flush_barrier`] completes.
pub(super) struct FlushWork {
    bytes: Vec<u8>,
    /// Whether a manifest was staged for the rename. Without one the manifest
    /// in place already holds `bytes`, and only `dirs` are owed.
    staged: bool,
    /// Fsynced once the manifest is in place.
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
        work.iter().flat_map(|(t, w)| {
            t.shard_index
                .unsynced_paths()
                .chain(w.staged.then(|| manifest::staging_path(&t.shard_index.output_dir)))
        }),
        FsyncFlags::DATASYNC,
    )?;
    let mut dirs = BTreeSet::new();
    for (t, w) in &mut work {
        if w.staged {
            // The rename publishes every shard the manifest names.
            manifest::commit(&t.shard_index.output_dir)?;
            t.shard_index.mark_published();
        }
        dirs.extend(std::mem::take(&mut w.dirs));
    }
    // A rename is metadata: a full fsync, not fdatasync.
    sync_paths(&mut ring, &dirs, FsyncFlags::empty())?;
    // No durable manifest names a superseded shard any more.
    for (t, w) in work {
        t.manifest_in_place = Some(w.bytes);
        t.manifest_synced = true;
        t.shard_index.unlink_retired();
    }
    Ok(())
}

impl Table {
    // ------------------------------------------------------------------
    // RAM-tier fold (ingest overflow)
    // ------------------------------------------------------------------

    /// Fold the residual memtable into the RAM tier (no spill).
    /// `cached_full_scan` survives it: both tiers are merged and
    /// ghost-eliminated alike, so the row set does not move.
    fn fold_memtable_into_ram_tier(&mut self) {
        self.memtable.drain_into(&mut self.ram_tier, &self.shard_index.schema);
    }

    /// Fold the memtable into the RAM tier, spilling the tier to an unsynced
    /// shard unless its net state leaves it room.
    pub(crate) fn fold_to_ram(&mut self) -> Result<(), StorageError> {
        self.fold_memtable_into_ram_tier();
        if self.held_in_ram || !self.ram_tier.is_full() {
            return Ok(());
        }
        // The fold's cancellation can bring the tier back under its ceiling. A
        // tier it leaves crowded is over the ceiling again within the little
        // room it has left, and every such crossing folds the whole tier.
        self.ram_tier.fold(&self.shard_index.schema);
        if !self.ram_tier.is_crowded() {
            return Ok(());
        }
        self.spill_ram_tier()
    }

    // ------------------------------------------------------------------
    // Barrier prepare
    // ------------------------------------------------------------------

    /// Fold memtable and RAM tier into one shard and stage the manifest naming
    /// it, unless it is the one in place; `None` when that one is also synced.
    pub(super) fn flush_prepare(&mut self, checkpoint_mark: u64) -> Result<Option<FlushWork>, StorageError> {
        // Fold-first, then one shard.
        self.fold_memtable_into_ram_tier();
        self.spill_ram_tier()?;
        // Rows above the cut go to a shard of their own, which no compaction
        // folds below it.
        let schema = self.shard_index.schema;
        self.pending
            .spill(&schema, |run| self.shard_index.append_pending_run(run))?;
        // A manifest names a tree; a fold under way has outputs in none.
        self.shard_index.finish_fold()?;
        let bytes = manifest::encode(&Manifest {
            checkpoint_mark,
            caller_record: self.caller_record.clone(),
            shards: self.shard_index.shard_set(),
        });
        let staged = self.manifest_in_place.as_deref() != Some(&bytes[..]);
        debug_assert!(
            staged || self.shard_index.unsynced_paths().next().is_none(),
            "the manifest in place names an unsynced shard"
        );
        if !staged && self.manifest_synced {
            return Ok(None);
        }
        // Until the barrier records otherwise: a failed publish may have renamed.
        let first_sync = !std::mem::take(&mut self.manifest_synced);
        if staged {
            self.manifest_in_place = None;
            manifest::prepare(&self.shard_index.output_dir, &bytes)?;
        }
        // A first sync also makes the directory's own entry and its parent's durable.
        let entry_dirs = if first_sync { 2 } else { 0 };
        let dirs = Path::new(&self.shard_index.output_dir)
            .ancestors()
            .take(1 + entry_dirs)
            .filter(|d| !d.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .collect();
        Ok(Some(FlushWork { bytes, staged, dirs }))
    }

    /// Move the RAM tier's rows to an unsynced L0 shard.
    fn spill_ram_tier(&mut self) -> Result<(), StorageError> {
        let schema = self.shard_index.schema;
        self.ram_tier.spill(&schema, |run| self.shard_index.append_l0_run(run))
    }
}
