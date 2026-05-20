mod program_tests;
mod batch_tests;
mod plan_tests;

use super::batch::{EvalScratch, MORSEL, NULL_WORDS_PER_REG, eval_batch};
use super::program::ExprProgram;
use crate::storage::MemBatch;

#[inline]
pub(super) fn float_to_bits(f: f64) -> i64 {
    i64::from_ne_bytes(f.to_ne_bytes())
}

#[inline]
pub(super) fn bits_to_float(bits: i64) -> f64 {
    f64::from_ne_bytes(bits.to_ne_bytes())
}

/// Test helper: run `prog` at m=1 over (mb, row) and report (value, is_null).
/// Mirrors the deleted per-row interpreter API so existing tests continue to
/// exercise the same edges through the single-path evaluator.
pub(super) fn eval_predicate_via_batch(
    prog: &ExprProgram,
    mb: &MemBatch,
    row: usize,
) -> (i64, bool) {
    if prog.num_regs == 0 {
        return (0, true);
    }
    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(prog.num_regs as usize, false, 1);
    eval_batch(prog, mb, row, 1, &mut scratch);
    let r = prog.result_reg as usize;
    let val = scratch.regs[r * MORSEL];
    let is_null = (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0;
    (val, is_null)
}

/// Test helper: run `prog` at m=1, then for each EMIT instruction read the
/// source register and apply null→0 emit semantics. Returns
/// (predicate_value, predicate_is_null, emit_null_mask, emit_buffers).
/// Mirrors the deleted `eval_with_emit` API.
pub(super) fn eval_with_emit_via_batch(
    prog: &ExprProgram,
    mb: &MemBatch,
    row: usize,
) -> (i64, bool, u64, Vec<i64>) {
    let mut scratch = EvalScratch::new();
    scratch.ensure_capacity(prog.num_regs.max(1) as usize, false, 1);
    eval_batch(prog, mb, row, 1, &mut scratch);

    let mut emit_vals: Vec<i64> = Vec::new();
    let mut emit_null_mask: u64 = 0;
    for instr in prog.code.chunks_exact(4) {
        if instr[0] != super::program::EXPR_EMIT {
            continue;
        }
        let reg = instr[2] as usize;
        let payload_col = instr[1] as usize;
        let is_null =
            (scratch.null_bits[reg * NULL_WORDS_PER_REG] & 1) != 0;
        if is_null {
            emit_vals.push(0);
            emit_null_mask |= 1u64 << payload_col;
        } else {
            emit_vals.push(scratch.regs[reg * MORSEL]);
        }
    }

    if prog.num_regs == 0 {
        return (0, true, emit_null_mask, emit_vals);
    }
    let r = prog.result_reg as usize;
    let val = scratch.regs[r * MORSEL];
    let is_null = (scratch.null_bits[r * NULL_WORDS_PER_REG] & 1) != 0;
    (val, is_null, emit_null_mask, emit_vals)
}
