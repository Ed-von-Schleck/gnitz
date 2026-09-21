//! The reduce output row emitter.

use crate::storage::{Batch, MemBatch};
use gnitz_wire::WideKind;

use super::agg::{Accumulator, AggValue};
use super::plan::ReduceShape;

/// Emit one aggregate column: the value truncated to the column width, or NULL.
#[inline]
fn emit_agg_col(output: &mut Batch, acc: &Accumulator, out_pi: usize, null_word: &mut u64) {
    let cs = acc.out_size();
    let Some(value) = acc.value() else {
        gnitz_wire::null_word_set(null_word, out_pi, true);
        output.fill_col_zero(out_pi, cs);
        return;
    };
    match value {
        AggValue::Bits(bits) => output.extend_col(out_pi, &bits.to_le_bytes()[..cs]),
        AggValue::Wide(WideKind::Bytes, v) => output.extend_col_blob(out_pi, v),
        AggValue::Wide(WideKind::Fixed(_), v) => {
            debug_assert_eq!(v.len(), cs, "a 16-byte extreme fills its output column");
            output.extend_col(out_pi, v);
        }
    }
}

/// Emit one `[key…, group columns…, aggregates…]` row at weight +1: a group's
/// new value, copying its group columns from `exemplar`, or the ground row,
/// which has none.
pub(super) fn emit_reduce_row(
    output: &mut Batch,
    exemplar: Option<(&MemBatch, usize)>,
    out_pk_bytes: &[u8],
    accs: &[Accumulator],
    shape: &ReduceShape,
) {
    let exemplar_locs = shape.key.exemplar_locs();
    debug_assert!(
        exemplar.is_some() || exemplar_locs.is_empty(),
        "only an exemplar-free layout emits without an exemplar row"
    );
    output.begin_row(out_pk_bytes, 1);
    let mut null_word: u64 = 0;

    if let Some((input_mb, exemplar_row)) = exemplar {
        for (out_pi, loc) in exemplar_locs.iter().enumerate() {
            output.append_cell_from(out_pi, loc, input_mb, exemplar_row, &mut null_word);
        }
    }
    for (k, acc) in accs.iter().enumerate() {
        emit_agg_col(output, acc, exemplar_locs.len() + k, &mut null_word);
    }

    output.commit_row(null_word);
}
