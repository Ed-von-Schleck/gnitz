//! WAL-block encode/decode for the client wire codec.

use super::error::ProtocolError;
use super::types::{check_not_null, Schema, ZSetBatch};
use gnitz_expr::SchemaFacts;
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

/// Each fixed region's per-row width under `schema`, in canonical order.
fn fixed_strides(schema: &Schema) -> impl Iterator<Item = usize> + '_ {
    [schema.pk_stride(), 8, 8]
        .into_iter()
        .chain(schema.payload_columns().map(|(_, _, c)| c.ty.tc.wire_stride()))
}

/// Decode a WAL block under `schema`, appending its rows to `sink`.
pub(crate) fn decode_wal_block_into(sink: &mut ZSetBatch, data: &[u8], schema: &Schema) -> Result<(), ProtocolError> {
    let (rows, fixed, heap, _) = gnitz_wire::wal::parse_block(data, fixed_strides(schema).sum())
        .map_err(|e| ProtocolError::DecodeError(format!("WAL {e}")))?;
    let mut regions = Regions::new();
    let mut at = 0;
    for s in fixed_strides(schema) {
        regions.push(&fixed[at..at + rows * s]);
        at += rows * s;
    }
    regions.push(heap);
    decode_regions_into(sink, &regions, schema)
}

/// Decode the rows laid out as the canonical region list `regions` under
/// `schema`, appending them to `sink`, a batch of `schema`. On error `sink` is
/// not usable.
pub fn decode_regions_into(sink: &mut ZSetBatch, regions: &[&[u8]], schema: &Schema) -> Result<(), ProtocolError> {
    debug_assert!(sink.layout_matches(schema).is_ok() && sink.check_columns().is_ok());
    let count = regions.get(REG_WEIGHT).map_or(0, |w| w.len() / 8);
    if regions.len() != gnitz_wire::num_regions(schema.num_payload_cols())
        || !fixed_strides(schema).zip(regions).all(|(s, r)| r.len() == count * s)
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
    let block_blob = regions[REG_PAYLOAD_START + schema.num_payload_cols()];
    let blob_base = sink.blob.len();
    sink.blob.extend_from_slice(block_blob);

    for (pi, ci, col) in schema.payload_columns() {
        let dst = &mut sink.payload[pi].bytes;
        let at = dst.len();
        dst.extend_from_slice(regions[REG_PAYLOAD_START + pi]);
        if col.ty.tc.is_german_string() {
            for cell in dst[at..].as_chunks_mut::<16>().0 {
                if !gnitz_wire::german_string_cell_ok(cell, block_blob) {
                    return Err(ProtocolError::DecodeError(format!(
                        "column {ci}: German string cell is not in canonical form"
                    )));
                }
                gnitz_wire::shift_german_string_heaps(cell, blob_base);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/wal_block.rs"]
mod tests;
