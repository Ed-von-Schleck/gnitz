//! The reduce output row emitter.

use super::agg::Accumulator;
use crate::repr::{Batch, MemBatch};
use crate::schema::ColumnLocator;

/// Emit one `[key…, group columns…, aggregates…]` row at weight +1, copying the
/// group columns from `group`'s row at its locators; the ground row has none.
pub(crate) fn emit_reduce_row(
    output: &mut Batch,
    group: Option<(&MemBatch, usize, &[ColumnLocator])>,
    out_pk_bytes: &[u8],
    accs: &[Accumulator],
) {
    output.begin_row(out_pk_bytes, 1);
    let mut null_word: u64 = 0;
    if let Some((src, row, locs)) = group {
        output.append_cells_from(0, locs, src, row, &mut null_word);
    }
    for acc in accs {
        acc.emit(output, &mut null_word);
    }
    output.commit_row(null_word);
}
