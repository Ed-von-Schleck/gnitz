//! A client `ZSetBatch` as its §6 region list — what `gnitz_wire::wal::encode`
//! frames into a WAL block. A batch already holds every region in place, so this
//! is a list of borrows: PK, weight, null bitmap, one region per payload slot in
//! slot order, blob heap last.

use super::types::ZSetBatch;
use gnitz_wire::as_le_bytes;

/// `batch` as the §6 canonical region list — exactly what
/// [`gnitz_wire::wal::encode`] frames.
///
/// Every region is indexed absolutely by the framer, so a region whose length
/// contradicts its type would read out of bounds one crate away from its cause.
/// `check_columns` is the same rule `ZSetBatch::validate` applies on the push
/// path, restated here because a list can be built for a batch that never went
/// through it.
pub(crate) fn regions(batch: &ZSetBatch) -> Vec<&[u8]> {
    batch
        .check_columns()
        .expect("ZSetBatch payload regions match their types");
    let mut regions: Vec<&[u8]> = Vec::with_capacity(gnitz_wire::wal::num_regions(batch.payload.len()));
    regions.push(batch.pks.region());
    regions.push(as_le_bytes(&batch.weights));
    regions.push(as_le_bytes(&batch.nulls));
    regions.extend(batch.payload.iter().map(|c| c.bytes.as_slice()));
    regions.push(&batch.blob); // the blob arena is always the last region
    regions
}

#[cfg(test)]
#[path = "tests/regions.rs"]
mod tests;
