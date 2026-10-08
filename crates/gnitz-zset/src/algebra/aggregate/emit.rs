//! The reduce output row emitter.

use super::agg::{Agg, AggValues};
use crate::repr::{Batch, MemBatch};
use crate::schema::ColumnLocator;

/// Emit one `[key…, group columns…, aggregates…]` row at weight +1, copying the
/// group columns from `group`'s row at its locators — the ground row has none —
/// and each of `aggs` from group `g` of `vals`.
pub(crate) fn emit_reduce_row(
    output: &mut Batch,
    group: Option<(&MemBatch, usize, &[ColumnLocator])>,
    out_pk_bytes: &[u8],
    aggs: &[Agg],
    vals: &AggValues,
    g: usize,
) {
    output.begin_row(out_pk_bytes, 1);
    if let Some((src, row, locs)) = group {
        output.append_cells_from(0, locs, src, row);
    }
    for (k, agg) in aggs.iter().enumerate() {
        vals.emit(k, agg, g, output);
    }
    output.commit_row();
}
