//! [`Table`]'s flush path: the overflow fold and spill, and the multi-table
//! durable flush barrier.

use super::super::shard_index::{FlushWork, ShardIndex};
use super::Table;
use gnitz_zset::repr::StorageError;

/// Durably publish every table in `tables`, each manifest carrying
/// `checkpoint_mark`.
pub(crate) fn flush_barrier<'a>(
    tables: impl IntoIterator<Item = &'a mut Table>,
    checkpoint_mark: u64,
) -> Result<(), StorageError> {
    let mut work = Vec::new();
    for t in tables {
        if let Some(w) = t.flush_prepare(checkpoint_mark)? {
            work.push((&mut t.shard_index, w));
        }
    }
    ShardIndex::publish(work)
}

impl Table {
    // ------------------------------------------------------------------
    // RAM-tier fold (ingest overflow)
    // ------------------------------------------------------------------

    /// Fold the memtable into the RAM tier, spilling the tier to an unsynced
    /// shard unless its net state leaves it room. `cached_full_scan` survives
    /// it: the row set does not move.
    pub(crate) fn fold_to_ram(&mut self) -> Result<(), StorageError> {
        self.rejudge_live_floor();
        self.memtable.drain_into(&mut self.ram_tier, &self.shard_index.schema);
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

    /// Fold memtable and RAM tier into one shard and [stage](ShardIndex::stage)
    /// the manifest naming it.
    pub(super) fn flush_prepare(&mut self, checkpoint_mark: u64) -> Result<Option<FlushWork>, StorageError> {
        debug_assert!(!self.held_in_ram, "a barrier over a store held in RAM only");
        // The memtable drains into the RAM tier, which spills as one shard.
        self.rejudge_live_floor();
        self.memtable.drain_into(&mut self.ram_tier, &self.shard_index.schema);
        self.spill_ram_tier()?;
        // Rows above the cut go to a shard of their own, which no compaction
        // folds below it.
        let schema = self.shard_index.schema;
        self.pending
            .spill(&schema, |run| self.shard_index.append_pending_run(run))?;
        self.shard_index.stage(checkpoint_mark)
    }

    /// Move the RAM tier's rows to an unsynced L0 shard.
    fn spill_ram_tier(&mut self) -> Result<(), StorageError> {
        let schema = self.shard_index.schema;
        self.ram_tier.spill(&schema, |run| self.shard_index.append_l0_run(run))
    }
}
