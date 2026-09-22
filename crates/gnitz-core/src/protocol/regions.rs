//! A client `ZSetBatch` as its §6 region list — what [`gnitz_wire::wal::WalBlock`]
//! frames into a WAL block. A batch already holds every region in place, so this
//! is a list of borrows: PK, weight, null bitmap, one region per payload slot in
//! slot order, blob heap last.

use super::types::ZSetBatch;
use gnitz_wire::as_le_bytes;
use gnitz_wire::wal::WalBlock;
use gnitz_wire::Regions;

impl ZSetBatch {
    /// This batch as the §6 canonical region list. Panics unless every payload
    /// region is the length its type and the row count imply — the rule
    /// `ZSetBatch::validate` applies on the push path.
    pub(crate) fn regions<'s>(&'s self, out: &mut Regions<'s>) {
        self.check_columns()
            .expect("ZSetBatch payload regions match their types");
        out.clear();
        out.push(self.pks.region());
        out.push(as_le_bytes(&self.weights));
        out.push(as_le_bytes(&self.nulls));
        for c in &self.payload {
            out.push(&c.bytes);
        }
        out.push(&self.blob); // the blob arena is always the last region
    }

    /// This batch as the WAL block that frames it under `table_id`.
    pub(crate) fn wal_block(&self, table_id: u64) -> WalBlock<'_> {
        let mut block = WalBlock::new(table_id as u32, self.len() as u32);
        self.regions(&mut block.regions);
        block
    }
}

#[cfg(test)]
#[path = "tests/regions.rs"]
mod tests;
