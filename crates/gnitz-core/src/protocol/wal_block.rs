//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::types::{check_not_null, Schema, ZSetBatch};
use gnitz_wire::{as_le_bytes, Regions, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

impl ZSetBatch {
    /// This batch as its canonical region list.
    pub(crate) fn wire_regions(&self) -> Regions<'_> {
        let mut out = Regions::new();
        out.push(self.pks.region());
        out.push(as_le_bytes(&self.weights));
        out.push(as_le_bytes(&self.nulls));
        for c in &self.payload {
            out.push(&c.bytes);
        }
        out.push(&self.blob);
        out
    }
}

/// Decode a WAL block under `schema`, appending its rows to `sink`. On error
/// `sink` is not usable.
pub(crate) fn decode_wal_block_into(sink: &mut ZSetBatch, data: &[u8], schema: &Schema) -> Result<(), ProtocolError> {
    let layout = schema.layout();
    let (rows, fixed, heap, _) = gnitz_wire::wal::parse_block(data, layout.row_width())
        .map_err(|e| ProtocolError::DecodeError(format!("WAL {e}")))?;
    let mut regions = Regions::new();
    for r in 0..layout.num_regions() {
        regions.push(&fixed[layout.region_start(r, rows)..][..rows * layout.region_stride(r)]);
    }
    regions.push(heap);
    append_regions(sink, &regions, schema, true)
}

/// Append to `sink`, a batch of `schema`, the rows of a canonical region list
/// this process's own store produced. Its German cells are rebased and not
/// checked, the store having checked them when it ingested them; the layout
/// and NOT NULL are checked as for a block. On error `sink` is not usable.
pub fn append_own_regions(sink: &mut ZSetBatch, regions: &[&[u8]], schema: &Schema) -> Result<(), ProtocolError> {
    append_regions(sink, regions, schema, false)
}

/// Append the rows laid out as the canonical region list `regions` under
/// `schema` to `sink`, a batch of `schema`. `foreign` regions crossed a trust
/// boundary, so each German cell is checked for canonical form as it is copied.
fn append_regions(
    sink: &mut ZSetBatch,
    regions: &[&[u8]],
    schema: &Schema,
    foreign: bool,
) -> Result<(), ProtocolError> {
    debug_assert!(sink.layout_matches(schema).is_ok() && sink.check_columns().is_ok());
    let count = regions.get(REG_WEIGHT).map_or(0, |w| w.len() / 8);
    if regions.len() != gnitz_wire::num_regions(sink.payload.len())
        || regions[REG_PK].len() != count * sink.pks.stride()
        || regions[REG_NULL_BMP].len() != count * 8
        || !sink
            .payload
            .iter()
            .zip(&regions[REG_PAYLOAD_START..])
            .all(|(c, r)| r.len() == count * c.stride())
    {
        return Err(ProtocolError::DecodeError(
            "a region list that does not lay out its schema".into(),
        ));
    }
    sink.reserve(count);
    sink.pks.push_region_bytes(regions[REG_PK]);
    gnitz_wire::extend_from_le_bytes(&mut sink.weights, regions[REG_WEIGHT]);
    let nulls_at = sink.nulls.len();
    gnitz_wire::extend_from_le_bytes(&mut sink.nulls, regions[REG_NULL_BMP]);
    check_not_null(&sink.nulls[nulls_at..], schema).map_err(ProtocolError::DecodeError)?;

    // A German cell's heap offset is relative to its own block's heap.
    let block_blob = regions[REG_PAYLOAD_START + sink.payload.len()];
    let blob_base = sink.blob.len();
    sink.blob.extend_from_slice(block_blob);

    for (pi, col) in sink.payload.iter_mut().enumerate() {
        let german = col.tc().is_german_string();
        let dst = &mut col.bytes;
        let at = dst.len();
        dst.extend_from_slice(regions[REG_PAYLOAD_START + pi]);
        if german {
            if foreign && !gnitz_wire::german_string_region_ok(&dst[at..], block_blob) {
                return Err(ProtocolError::DecodeError(format!(
                    "column {}: German string cell is not in canonical form",
                    schema.layout().payload_col_idx(pi)
                )));
            }
            gnitz_wire::shift_german_string_heaps(&mut dst[at..], blob_base);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/wal_block.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/wal_block.rs"]
mod benches;
