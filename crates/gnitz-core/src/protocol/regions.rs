//! A client `ZSetBatch` as its §6 region list — what [`gnitz_wire::wal::WalBlock`]
//! frames into a WAL block. A batch already holds every region in place, so this
//! is a list of borrows: PK, weight, null bitmap, one region per payload slot in
//! slot order, blob heap last.

use super::types::ZSetBatch;
use gnitz_wire::as_le_bytes;
use gnitz_wire::wal::{Regions, WalBlock};

impl ZSetBatch {
    /// This batch as the §6 canonical region list. Panics unless every payload
    /// region is the length its type and the row count imply — the rule
    /// `ZSetBatch::validate` applies on the push path.
    pub(crate) fn regions(&self) -> Regions<'_> {
        self.check_columns()
            .expect("ZSetBatch payload regions match their types");
        let mut regions = Regions::new();
        regions.push(self.pks.region());
        regions.push(as_le_bytes(&self.weights));
        regions.push(as_le_bytes(&self.nulls));
        for c in &self.payload {
            regions.push(&c.bytes);
        }
        regions.push(&self.blob); // the blob arena is always the last region
        regions
    }

    /// This batch as the WAL block that frames it under `table_id`.
    pub(crate) fn wal_block(&self, table_id: u64) -> WalBlock<'_> {
        WalBlock {
            table_id: table_id as u32,
            entry_count: self.len() as u32,
            regions: self.regions(),
        }
    }
}

#[cfg(test)]
#[path = "tests/regions.rs"]
mod tests;
