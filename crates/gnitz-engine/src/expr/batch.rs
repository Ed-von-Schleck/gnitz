//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;

use super::program::{
    ExprProgram,
    EXPR_LOAD_COL_INT, EXPR_LOAD_COL_FLOAT,
    EXPR_LOAD_CONST, EXPR_LOAD_PAYLOAD_INT, EXPR_LOAD_PAYLOAD_FLOAT, EXPR_LOAD_PK_INT,
    EXPR_INT_ADD, EXPR_INT_SUB, EXPR_INT_MUL, EXPR_INT_DIV, EXPR_INT_MOD, EXPR_INT_NEG,
    EXPR_FLOAT_ADD, EXPR_FLOAT_SUB, EXPR_FLOAT_MUL, EXPR_FLOAT_DIV, EXPR_FLOAT_NEG,
    EXPR_CMP_EQ, EXPR_CMP_NE, EXPR_CMP_GT, EXPR_CMP_GE, EXPR_CMP_LT, EXPR_CMP_LE,
    EXPR_FCMP_EQ, EXPR_FCMP_NE, EXPR_FCMP_GT, EXPR_FCMP_GE, EXPR_FCMP_LT, EXPR_FCMP_LE,
    EXPR_BOOL_AND, EXPR_BOOL_OR, EXPR_BOOL_NOT,
    EXPR_IS_NULL, EXPR_IS_NOT_NULL,
    EXPR_INT_TO_FLOAT,
    EXPR_COPY_COL, EXPR_EMIT_NULL, EXPR_EMIT,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LT_CONST, EXPR_STR_COL_LE_CONST,
    EXPR_STR_COL_EQ_COL, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LE_COL,
    compare_col_string_vs_const,
};
use crate::storage::MemBatch;
use crate::schema::compare_german_strings;
use crate::util::read_u64_le;

pub(in crate::expr) const MORSEL: usize = 256;
pub(in crate::expr) const NULL_WORDS_PER_REG: usize = MORSEL / 64; // 4

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

pub(in crate::expr) struct EvalScratch {
    /// Register buffers, register-major layout: regs[reg * MORSEL + row].
    pub(in crate::expr) regs: Vec<i64>,
    /// Null bitmask, register-major: null_bits[reg * NULL_WORDS_PER_REG + word].
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(in crate::expr) null_bits: Vec<u64>,
    /// Per-row filter bitmask; set by filter_batch, ignored by execute_map.
    pub(in crate::expr) filter_bits: Vec<u64>,
    pub(in crate::expr) no_nulls: bool,
}

#[allow(dead_code)]
impl EvalScratch {
    pub(in crate::expr) fn new() -> Self {
        EvalScratch {
            regs: Vec::new(),
            null_bits: Vec::new(),
            filter_bits: Vec::new(),
            no_nulls: false,
        }
    }

    /// Ensure the scratch buffer can hold `num_regs` registers and `(n+63)/64`
    /// filter words.  Does not shrink.
    pub(in crate::expr) fn ensure_capacity(&mut self, num_regs: usize, no_nulls: bool, n: usize) {
        let reg_cap = num_regs * MORSEL;
        if self.regs.len() < reg_cap {
            self.regs.resize(reg_cap, 0);
        }
        self.no_nulls = no_nulls;
        if !no_nulls {
            let null_cap = num_regs * NULL_WORDS_PER_REG;
            if self.null_bits.len() < null_cap {
                self.null_bits.resize(null_cap, 0);
            }
        }
        let filter_words = n.div_ceil(64);
        if self.filter_bits.len() < filter_words {
            self.filter_bits.resize(filter_words, 0);
        }
    }

    pub(in crate::expr) fn reg_slice(&self, reg: usize, m: usize) -> &[i64] {
        &self.regs[reg * MORSEL..reg * MORSEL + m]
    }

    pub(in crate::expr) fn reg_mut(&mut self, reg: usize, m: usize) -> &mut [i64] {
        &mut self.regs[reg * MORSEL..reg * MORSEL + m]
    }

    /// Split borrows: two shared sources + one mutable destination.
    /// Safety: SSA guarantees d != a and d != b (the debug_assert enforces this).
    pub(in crate::expr) fn reg3(&mut self, a: usize, b: usize, d: usize, m: usize)
        -> (&[i64], &[i64], &mut [i64])
    {
        debug_assert!(d != a && d != b, "reg3: dst aliases src register");
        unsafe {
            let ptr = self.regs.as_mut_ptr();
            let ra = std::slice::from_raw_parts(ptr.add(a * MORSEL), m);
            let rb = std::slice::from_raw_parts(ptr.add(b * MORSEL), m);
            let rd = std::slice::from_raw_parts_mut(ptr.add(d * MORSEL), m);
            (ra, rb, rd)
        }
    }

    pub(in crate::expr) fn null_words(&self, reg: usize, words: usize) -> &[u64] {
        &self.null_bits[reg * NULL_WORDS_PER_REG..reg * NULL_WORDS_PER_REG + words]
    }

    pub(in crate::expr) fn null_words_mut(&mut self, reg: usize, words: usize) -> &mut [u64] {
        &mut self.null_bits[reg * NULL_WORDS_PER_REG..reg * NULL_WORDS_PER_REG + words]
    }

    /// Split borrows for null bit words.
    /// Safety: SSA guarantees d != a and d != b.
    pub(in crate::expr) fn null_words3(&mut self, a: usize, b: usize, d: usize, words: usize)
        -> (&[u64], &[u64], &mut [u64])
    {
        debug_assert!(d != a && d != b, "null_words3: dst aliases src register");
        unsafe {
            let ptr = self.null_bits.as_mut_ptr();
            let ra = std::slice::from_raw_parts(ptr.add(a * NULL_WORDS_PER_REG), words);
            let rb = std::slice::from_raw_parts(ptr.add(b * NULL_WORDS_PER_REG), words);
            let rd = std::slice::from_raw_parts_mut(ptr.add(d * NULL_WORDS_PER_REG), words);
            (ra, rb, rd)
        }
    }

    /// Zero the null bits for one register's morsel region.
    pub(in crate::expr) fn clear_null_reg(&mut self, reg: usize, m: usize) {
        if self.no_nulls { return; }
        let words = m.div_ceil(64);
        let base = reg * NULL_WORDS_PER_REG;
        for w in 0..words { self.null_bits[base + w] = 0; }
    }
}

// ---------------------------------------------------------------------------
// Null-bit helpers
// ---------------------------------------------------------------------------

/// Propagate binary null: dst_null = a_null | b_null (word-at-a-time).
fn null_or2(s: &mut EvalScratch, dst: usize, a: usize, b: usize, m: usize) {
    if s.no_nulls { return; }
    let words = m.div_ceil(64);
    let (na, nb, nd) = s.null_words3(a, b, dst, words);
    for w in 0..words { nd[w] = na[w] | nb[w]; }
}

/// Propagate unary null: dst_null = src_null (word-at-a-time).
fn null_or1(s: &mut EvalScratch, dst: usize, src: usize, m: usize) {
    if s.no_nulls { return; }
    if dst == src { return; }
    let words = m.div_ceil(64);
    let base_s = src * NULL_WORDS_PER_REG;
    let base_d = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        s.null_bits[base_d + w] = s.null_bits[base_s + w];
    }
}

/// Fill null bits for a physical payload column `pi` into register `di`.
fn fill_null_bits_pi(
    s: &mut EvalScratch,
    di: usize,
    null_bmp: &[u8],
    morsel_start: usize,
    m: usize,
    pi: usize,
) {
    if s.no_nulls { return; }
    let words = m.div_ceil(64);
    let base = di * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        let mut word: u64 = 0;
        for i in lo..hi {
            let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
            if (row_null >> pi) & 1 != 0 {
                word |= 1u64 << (i - lo);
            }
        }
        s.null_bits[base + w] = word;
    }
}

/// Fill null bits for logical column `ci` (handles PK: always non-null).
fn fill_null_bits_ci(
    s: &mut EvalScratch,
    di: usize,
    null_bmp: &[u8],
    morsel_start: usize,
    m: usize,
    ci: usize,
    pki: usize,
) {
    if s.no_nulls { return; }
    if ci == pki {
        s.clear_null_reg(di, m);
        return;
    }
    let pi = if ci < pki { ci } else { ci - 1 };
    fill_null_bits_pi(s, di, null_bmp, morsel_start, m, pi);
}

// ---------------------------------------------------------------------------
// String comparison helpers — shared by *_CONST and *_COL match arms
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_const(
    scratch: &mut EvalScratch,
    mb: &MemBatch,
    prog: &ExprProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    ci: usize,
    pki: usize,
    const_idx: usize,
    pred: impl Fn(Ordering) -> bool,
) {
    let pi = if ci < pki { ci } else { ci - 1 };
    fill_null_bits_ci(scratch, dst, mb.null_bmp, morsel_start, m, ci, pki);
    let col_data = mb.col_data[pi];
    let blob = mb.blob;
    let const_bytes = &prog.const_strings[const_idx];
    let const_prefix = prog.const_prefixes[const_idx];
    let const_len = prog.const_lengths[const_idx];
    let base_d = dst * MORSEL;
    // Compute results unconditionally (null rows produce a valid but irrelevant value).
    // Keeping the comparison out of a branch-per-row allows LLVM to vectorize this loop.
    for i in 0..m {
        let off = (morsel_start + i) * 16;
        let s = &col_data[off..off + 16];
        scratch.regs[base_d + i] =
            pred(compare_col_string_vs_const(s, blob, const_bytes, const_prefix, const_len)) as i64;
    }
    // Zero out results for null rows using the already-computed null mask.
    if !scratch.no_nulls {
        let words = m.div_ceil(64);
        for w in 0..words {
            let null_word = scratch.null_bits[dst * NULL_WORDS_PER_REG + w];
            if null_word != 0 {
                let lo = w * 64;
                let hi = (lo + 64).min(m);
                for bit in 0..(hi - lo) {
                    if (null_word >> bit) & 1 != 0 {
                        scratch.regs[base_d + lo + bit] = 0;
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_col(
    scratch: &mut EvalScratch,
    mb: &MemBatch,
    dst: usize,
    morsel_start: usize,
    m: usize,
    ci_a: usize,
    ci_b: usize,
    pki: usize,
    pred: impl Fn(Ordering) -> bool,
) {
    fill_null_bits_ci(scratch, dst, mb.null_bmp, morsel_start, m, ci_a, pki);
    if !scratch.no_nulls {
        let words = m.div_ceil(64);
        let pi_b = if ci_b < pki { ci_b } else { ci_b - 1 };
        for w in 0..words {
            let lo = w * 64;
            let hi = (lo + 64).min(m);
            let mut extra: u64 = 0;
            for i in lo..hi {
                let row_null = read_u64_le(mb.null_bmp, (morsel_start + i) * 8);
                let is_null_b = ci_b != pki && (row_null >> pi_b) & 1 != 0;
                if is_null_b { extra |= 1u64 << (i - lo); }
            }
            scratch.null_bits[dst * NULL_WORDS_PER_REG + w] |= extra;
        }
    }
    let pi_a = if ci_a < pki { ci_a } else { ci_a - 1 };
    let pi_b = if ci_b < pki { ci_b } else { ci_b - 1 };
    let col_a = mb.col_data[pi_a];
    let col_b = mb.col_data[pi_b];
    let blob = mb.blob;
    let base_d = dst * MORSEL;
    // Compute results unconditionally; null rows produce a valid but irrelevant value.
    for i in 0..m {
        let row = morsel_start + i;
        let s1 = &col_a[row * 16..row * 16 + 16];
        let s2 = &col_b[row * 16..row * 16 + 16];
        scratch.regs[base_d + i] = pred(compare_german_strings(s1, blob, s2, blob)) as i64;
    }
    // Zero out results for null rows using the already-computed null mask.
    if !scratch.no_nulls {
        let words = m.div_ceil(64);
        for w in 0..words {
            let null_word = scratch.null_bits[dst * NULL_WORDS_PER_REG + w];
            if null_word != 0 {
                let lo = w * 64;
                let hi = (lo + 64).min(m);
                for bit in 0..(hi - lo) {
                    if (null_word >> bit) & 1 != 0 {
                        scratch.regs[base_d + lo + bit] = 0;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// eval_batch — single morsel
// ---------------------------------------------------------------------------

/// Evaluate `prog` over one morsel of `mb` (`morsel_start..morsel_start+m`).
/// Results land in `scratch.regs`; null bits in `scratch.null_bits`.
///
/// Callers loop over morsels and call this function once per morsel.
#[allow(clippy::needless_range_loop)]
pub(in crate::expr) fn eval_batch(
    prog: &ExprProgram,
    mb: &MemBatch,
    morsel_start: usize,
    m: usize,
    pki: usize,
    scratch: &mut EvalScratch,
) {
    for instr in prog.code.chunks_exact(4) {
        let op  = instr[0];
        let dst = instr[1] as usize;
        let a1  = instr[2];
        let a2  = instr[3];

        match op {
            // ----------------------------------------------------------------
            // Handled at batch level — skip in inner loop
            // ----------------------------------------------------------------
            EXPR_COPY_COL | EXPR_EMIT_NULL | EXPR_EMIT => {}

            // ----------------------------------------------------------------
            // Load operations
            // ----------------------------------------------------------------
            EXPR_LOAD_PAYLOAD_INT => {
                let pi = a1 as usize;
                let (col_size, col_tc) = if prog.payload_col_sizes.is_empty() {
                    (8usize, crate::schema::type_code::I64)
                } else {
                    (prog.payload_col_sizes[pi] as usize, prog.payload_col_type_codes[pi])
                };
                let is_signed = matches!(col_tc,
                    crate::schema::type_code::I8 |
                    crate::schema::type_code::I16 |
                    crate::schema::type_code::I32 |
                    crate::schema::type_code::I64);
                let col_data = mb.col_data[pi];
                let dst_reg = scratch.reg_mut(dst, m);
                match col_size {
                    8 => {
                        let b = &col_data[morsel_start * 8..(morsel_start + m) * 8];
                        for (i, c) in b.chunks_exact(8).enumerate() {
                            dst_reg[i] = i64::from_le_bytes(c.try_into().unwrap());
                        }
                    }
                    4 => {
                        let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                        if is_signed {
                            for (i, c) in b.chunks_exact(4).enumerate() {
                                dst_reg[i] = i32::from_le_bytes(c.try_into().unwrap()) as i64;
                            }
                        } else {
                            for (i, c) in b.chunks_exact(4).enumerate() {
                                dst_reg[i] = u32::from_le_bytes(c.try_into().unwrap()) as i64;
                            }
                        }
                    }
                    2 => {
                        let b = &col_data[morsel_start * 2..(morsel_start + m) * 2];
                        if is_signed {
                            for (i, c) in b.chunks_exact(2).enumerate() {
                                dst_reg[i] = i16::from_le_bytes(c.try_into().unwrap()) as i64;
                            }
                        } else {
                            for (i, c) in b.chunks_exact(2).enumerate() {
                                dst_reg[i] = u16::from_le_bytes(c.try_into().unwrap()) as i64;
                            }
                        }
                    }
                    1 => {
                        let b = &col_data[morsel_start..morsel_start + m];
                        if is_signed {
                            for (i, &byte) in b.iter().enumerate() {
                                dst_reg[i] = byte as i8 as i64;
                            }
                        } else {
                            for (i, &byte) in b.iter().enumerate() {
                                dst_reg[i] = byte as i64;
                            }
                        }
                    }
                    _ => {}
                }
                fill_null_bits_pi(scratch, dst, mb.null_bmp, morsel_start, m, pi);
            }

            EXPR_LOAD_PAYLOAD_FLOAT => {
                let pi = a1 as usize;
                let col_size = if prog.payload_col_sizes.is_empty() {
                    8usize
                } else {
                    prog.payload_col_sizes[pi] as usize
                };
                let col_data = mb.col_data[pi];
                let dst_reg = scratch.reg_mut(dst, m);
                if col_size == 4 {
                    let b = &col_data[morsel_start * 4..(morsel_start + m) * 4];
                    for (i, c) in b.chunks_exact(4).enumerate() {
                        let bits = u32::from_le_bytes(c.try_into().unwrap());
                        dst_reg[i] = f64::to_bits(f32::from_bits(bits) as f64) as i64;
                    }
                } else {
                    let b = &col_data[morsel_start * 8..(morsel_start + m) * 8];
                    for (i, c) in b.chunks_exact(8).enumerate() {
                        dst_reg[i] = i64::from_le_bytes(c.try_into().unwrap());
                    }
                }
                fill_null_bits_pi(scratch, dst, mb.null_bmp, morsel_start, m, pi);
            }

            EXPR_LOAD_PK_INT => {
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] =
                        read_u64_le(mb.pk, (morsel_start + i) * 16) as i64;
                }
                scratch.clear_null_reg(dst, m);
            }

            EXPR_LOAD_CONST => {
                let val: i64 = (a2 << 32) | (a1 & 0xFFFF_FFFF);
                let base_d = dst * MORSEL;
                for i in 0..m { scratch.regs[base_d + i] = val; }
                scratch.clear_null_reg(dst, m);
            }

            // Legacy: LOAD_COL_INT/FLOAT — only appear in unresolved programs (tests).
            EXPR_LOAD_COL_INT => {
                let ci = a1 as usize;
                fill_null_bits_ci(scratch, dst, mb.null_bmp, morsel_start, m, ci, pki);
                let base_d = dst * MORSEL;
                if ci == pki {
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            read_u64_le(mb.pk, (morsel_start + i) * 16) as i64;
                    }
                } else {
                    let pi = if ci < pki { ci } else { ci - 1 };
                    let col_data = mb.col_data[pi];
                    for i in 0..m {
                        let off = (morsel_start + i) * 8;
                        scratch.regs[base_d + i] =
                            i64::from_le_bytes(col_data[off..off + 8].try_into().unwrap());
                    }
                }
            }

            EXPR_LOAD_COL_FLOAT => {
                let ci = a1 as usize;
                fill_null_bits_ci(scratch, dst, mb.null_bmp, morsel_start, m, ci, pki);
                let pi = if ci < pki { ci } else { ci - 1 };
                let col_data = mb.col_data[pi];
                let base_d = dst * MORSEL;
                for i in 0..m {
                    let off = (morsel_start + i) * 8;
                    scratch.regs[base_d + i] =
                        i64::from_le_bytes(col_data[off..off + 8].try_into().unwrap());
                }
            }

            // ----------------------------------------------------------------
            // Integer arithmetic
            // ----------------------------------------------------------------
            EXPR_INT_ADD => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = ra[i].wrapping_add(rb[i]); }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_INT_SUB => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = ra[i].wrapping_sub(rb[i]); }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_INT_MUL => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = ra[i].wrapping_mul(rb[i]); }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_INT_DIV => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                let words = m.div_ceil(64);
                let mut zero_mask = [0u64; NULL_WORDS_PER_REG];
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        if rb[i] == 0 {
                            zero_mask[i / 64] |= 1u64 << (i % 64);
                        }
                        let divisor = if rb[i] == 0 { 1 } else { rb[i] };
                        rd[i] = ra[i].wrapping_div(divisor);
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
                if !scratch.no_nulls {
                    let base_null_d = dst * NULL_WORDS_PER_REG;
                    for w in 0..words {
                        scratch.null_bits[base_null_d + w] |= zero_mask[w];
                    }
                }
            }
            EXPR_INT_MOD => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                let words = m.div_ceil(64);
                let mut zero_mask = [0u64; NULL_WORDS_PER_REG];
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        if rb[i] == 0 {
                            zero_mask[i / 64] |= 1u64 << (i % 64);
                        }
                        let divisor = if rb[i] == 0 { 1 } else { rb[i] };
                        rd[i] = ra[i].wrapping_rem(divisor);
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
                if !scratch.no_nulls {
                    let base_null_d = dst * NULL_WORDS_PER_REG;
                    for w in 0..words {
                        scratch.null_bits[base_null_d + w] |= zero_mask[w];
                    }
                }
            }
            EXPR_INT_NEG => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] = scratch.regs[base_a + i].wrapping_neg();
                }
                null_or1(scratch, dst, ai, m);
            }

            // ----------------------------------------------------------------
            // Float arithmetic
            // ----------------------------------------------------------------
            EXPR_FLOAT_ADD => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        let fa = f64::from_bits(ra[i] as u64);
                        let fb = f64::from_bits(rb[i] as u64);
                        rd[i] = f64::to_bits(fa + fb) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FLOAT_SUB => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        let fa = f64::from_bits(ra[i] as u64);
                        let fb = f64::from_bits(rb[i] as u64);
                        rd[i] = f64::to_bits(fa - fb) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FLOAT_MUL => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        let fa = f64::from_bits(ra[i] as u64);
                        let fb = f64::from_bits(rb[i] as u64);
                        rd[i] = f64::to_bits(fa * fb) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FLOAT_DIV => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                let words = m.div_ceil(64);
                let mut zero_mask = [0u64; NULL_WORDS_PER_REG];
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        let fa = f64::from_bits(ra[i] as u64);
                        let fb = f64::from_bits(rb[i] as u64);
                        if fb == 0.0 {
                            zero_mask[i / 64] |= 1u64 << (i % 64);
                        }
                        let fb_safe = if fb == 0.0 { 1.0 } else { fb };
                        rd[i] = f64::to_bits(fa / fb_safe) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
                if !scratch.no_nulls {
                    let base_null_d = dst * NULL_WORDS_PER_REG;
                    for w in 0..words {
                        scratch.null_bits[base_null_d + w] |= zero_mask[w];
                    }
                }
            }
            EXPR_FLOAT_NEG => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    let fa = f64::from_bits(scratch.regs[base_a + i] as u64);
                    scratch.regs[base_d + i] = f64::to_bits(-fa) as i64;
                }
                null_or1(scratch, dst, ai, m);
            }

            // ----------------------------------------------------------------
            // Integer comparisons
            // ----------------------------------------------------------------
            EXPR_CMP_EQ => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = (ra[i] == rb[i]) as i64; }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_CMP_NE => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = (ra[i] != rb[i]) as i64; }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_CMP_GT => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = (ra[i] > rb[i]) as i64; }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_CMP_GE => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = (ra[i] >= rb[i]) as i64; }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_CMP_LT => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = (ra[i] < rb[i]) as i64; }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_CMP_LE => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m { rd[i] = (ra[i] <= rb[i]) as i64; }
                }
                null_or2(scratch, dst, ai, bi, m);
            }

            // ----------------------------------------------------------------
            // Float comparisons
            // ----------------------------------------------------------------
            EXPR_FCMP_EQ => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        rd[i] = (f64::from_bits(ra[i] as u64) == f64::from_bits(rb[i] as u64)) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FCMP_NE => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        rd[i] = (f64::from_bits(ra[i] as u64) != f64::from_bits(rb[i] as u64)) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FCMP_GT => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        rd[i] = (f64::from_bits(ra[i] as u64) > f64::from_bits(rb[i] as u64)) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FCMP_GE => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        rd[i] = (f64::from_bits(ra[i] as u64) >= f64::from_bits(rb[i] as u64)) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FCMP_LT => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        rd[i] = (f64::from_bits(ra[i] as u64) < f64::from_bits(rb[i] as u64)) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }
            EXPR_FCMP_LE => {
                let ai = a1 as usize; let bi = a2 as usize;
                {
                    let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
                    for i in 0..m {
                        rd[i] = (f64::from_bits(ra[i] as u64) <= f64::from_bits(rb[i] as u64)) as i64;
                    }
                }
                null_or2(scratch, dst, ai, bi, m);
            }

            // ----------------------------------------------------------------
            // Boolean 3VL
            // ----------------------------------------------------------------
            EXPR_BOOL_AND => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                let words = m.div_ceil(64);
                if scratch.no_nulls {
                    let base_a = ai * MORSEL;
                    let base_b = bi * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            ((scratch.regs[base_a + i] != 0) && (scratch.regs[base_b + i] != 0)) as i64;
                    }
                } else {
                    for w in 0..words {
                        let base = w * 64;
                        let chunk_m = (base + 64).min(m) - base;
                        let mut va: u64 = 0;
                        let mut vb: u64 = 0;
                        for i in 0..chunk_m {
                            if scratch.regs[ai * MORSEL + base + i] != 0 { va |= 1u64 << i; }
                            if scratch.regs[bi * MORSEL + base + i] != 0 { vb |= 1u64 << i; }
                        }
                        let na = scratch.null_bits[ai * NULL_WORDS_PER_REG + w];
                        let nb = scratch.null_bits[bi * NULL_WORDS_PER_REG + w];
                        let definite_false = (!na & !va) | (!nb & !vb);
                        let nd = !definite_false & (na | nb);
                        let result_bits = !nd & va & vb;
                        scratch.null_bits[dst * NULL_WORDS_PER_REG + w] = nd;
                        for i in 0..chunk_m {
                            scratch.regs[dst * MORSEL + base + i] = ((result_bits >> i) & 1) as i64;
                        }
                    }
                }
            }
            EXPR_BOOL_OR => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                let words = m.div_ceil(64);
                if scratch.no_nulls {
                    let base_a = ai * MORSEL;
                    let base_b = bi * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            ((scratch.regs[base_a + i] != 0) || (scratch.regs[base_b + i] != 0)) as i64;
                    }
                } else {
                    for w in 0..words {
                        let base = w * 64;
                        let chunk_m = (base + 64).min(m) - base;
                        let mut va: u64 = 0;
                        let mut vb: u64 = 0;
                        for i in 0..chunk_m {
                            if scratch.regs[ai * MORSEL + base + i] != 0 { va |= 1u64 << i; }
                            if scratch.regs[bi * MORSEL + base + i] != 0 { vb |= 1u64 << i; }
                        }
                        let na = scratch.null_bits[ai * NULL_WORDS_PER_REG + w];
                        let nb = scratch.null_bits[bi * NULL_WORDS_PER_REG + w];
                        let definite_true = (!na & va) | (!nb & vb);
                        let nd = !definite_true & (na | nb);
                        let result_bits = definite_true;
                        scratch.null_bits[dst * NULL_WORDS_PER_REG + w] = nd;
                        for i in 0..chunk_m {
                            scratch.regs[dst * MORSEL + base + i] = ((result_bits >> i) & 1) as i64;
                        }
                    }
                }
            }
            EXPR_BOOL_NOT => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] = (scratch.regs[base_a + i] == 0) as i64;
                }
                null_or1(scratch, dst, ai, m);
            }

            // ----------------------------------------------------------------
            // IS NULL / IS NOT NULL
            // ----------------------------------------------------------------
            EXPR_IS_NULL => {
                // a1 is a logical column index (NOT remapped by resolve_column_indices)
                let ci = a1 as usize;
                scratch.clear_null_reg(dst, m);
                let base_d = dst * MORSEL;
                if ci == pki {
                    for i in 0..m { scratch.regs[base_d + i] = 0; }
                } else {
                    let pi = if ci < pki { ci } else { ci - 1 };
                    let null_bmp = mb.null_bmp;
                    for i in 0..m {
                        let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
                        scratch.regs[base_d + i] = ((row_null >> pi) & 1) as i64;
                    }
                }
            }
            EXPR_IS_NOT_NULL => {
                let ci = a1 as usize;
                scratch.clear_null_reg(dst, m);
                let base_d = dst * MORSEL;
                if ci == pki {
                    for i in 0..m { scratch.regs[base_d + i] = 1; }
                } else {
                    let pi = if ci < pki { ci } else { ci - 1 };
                    let null_bmp = mb.null_bmp;
                    for i in 0..m {
                        let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
                        scratch.regs[base_d + i] = (((row_null >> pi) & 1) ^ 1) as i64;
                    }
                }
            }

            // ----------------------------------------------------------------
            // Type cast
            // ----------------------------------------------------------------
            EXPR_INT_TO_FLOAT => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] =
                        f64::to_bits(scratch.regs[base_a + i] as f64) as i64;
                }
                null_or1(scratch, dst, ai, m);
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant)
            // ----------------------------------------------------------------
            EXPR_STR_COL_EQ_CONST => eval_str_col_vs_const(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as usize, pki, a2 as usize, |o| o == Ordering::Equal,
            ),
            EXPR_STR_COL_LT_CONST => eval_str_col_vs_const(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as usize, pki, a2 as usize, |o| o == Ordering::Less,
            ),
            EXPR_STR_COL_LE_CONST => eval_str_col_vs_const(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as usize, pki, a2 as usize, |o| o != Ordering::Greater,
            ),

            // ----------------------------------------------------------------
            // String comparisons (column vs column)
            // ----------------------------------------------------------------
            EXPR_STR_COL_EQ_COL => eval_str_col_vs_col(
                scratch, mb, dst, morsel_start, m,
                a1 as usize, a2 as usize, pki, |o| o == Ordering::Equal,
            ),
            EXPR_STR_COL_LT_COL => eval_str_col_vs_col(
                scratch, mb, dst, morsel_start, m,
                a1 as usize, a2 as usize, pki, |o| o == Ordering::Less,
            ),
            EXPR_STR_COL_LE_COL => eval_str_col_vs_col(
                scratch, mb, dst, morsel_start, m,
                a1 as usize, a2 as usize, pki, |o| o != Ordering::Greater,
            ),

            _ => {}
        }
    }
}
