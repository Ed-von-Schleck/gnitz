//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;

use crate::expr::{
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

pub const MORSEL: usize = 256;
pub(crate) const NULL_WORDS_PER_REG: usize = MORSEL / 64; // 4

// ---------------------------------------------------------------------------
// EvalScratch — the SoA register file for batch evaluation
// ---------------------------------------------------------------------------

pub struct EvalScratch {
    /// Register buffers, register-major layout: regs[reg * MORSEL + row].
    pub(crate) regs: Vec<i64>,
    /// Null bitmask, register-major: null_bits[reg * NULL_WORDS_PER_REG + word].
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(crate) null_bits: Vec<u64>,
    /// Per-row filter bitmask; set by filter_batch, ignored by execute_map.
    pub filter_bits: Vec<u64>,
    pub(crate) no_nulls: bool,
}

impl EvalScratch {
    pub fn new() -> Self {
        EvalScratch {
            regs: Vec::new(),
            null_bits: Vec::new(),
            filter_bits: Vec::new(),
            no_nulls: false,
        }
    }

    /// Ensure the scratch buffer can hold `num_regs` registers and `(n+63)/64`
    /// filter words.  Does not shrink.
    pub fn ensure_capacity(&mut self, num_regs: usize, no_nulls: bool, n: usize) {
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
        let filter_words = (n + 63) / 64;
        if self.filter_bits.len() < filter_words {
            self.filter_bits.resize(filter_words, 0);
        }
    }

    pub fn reg_slice(&self, reg: usize, m: usize) -> &[i64] {
        &self.regs[reg * MORSEL..reg * MORSEL + m]
    }

    pub fn reg_mut(&mut self, reg: usize, m: usize) -> &mut [i64] {
        &mut self.regs[reg * MORSEL..reg * MORSEL + m]
    }

    /// Split borrows: two shared sources + one mutable destination.
    /// Safety: SSA guarantees d != a and d != b (the debug_assert enforces this).
    pub fn reg3(&mut self, a: usize, b: usize, d: usize, m: usize)
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

    pub fn null_words(&self, reg: usize, words: usize) -> &[u64] {
        &self.null_bits[reg * NULL_WORDS_PER_REG..reg * NULL_WORDS_PER_REG + words]
    }

    pub fn null_words_mut(&mut self, reg: usize, words: usize) -> &mut [u64] {
        &mut self.null_bits[reg * NULL_WORDS_PER_REG..reg * NULL_WORDS_PER_REG + words]
    }

    /// Split borrows for null bit words.
    /// Safety: SSA guarantees d != a and d != b.
    pub fn null_words3(&mut self, a: usize, b: usize, d: usize, words: usize)
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
    pub fn clear_null_reg(&mut self, reg: usize, m: usize) {
        if self.no_nulls { return; }
        let words = (m + 63) / 64;
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
    let words = (m + 63) / 64;
    let (na, nb, nd) = s.null_words3(a, b, dst, words);
    for w in 0..words { nd[w] = na[w] | nb[w]; }
}

/// Propagate unary null: dst_null = src_null (word-at-a-time).
fn null_or1(s: &mut EvalScratch, dst: usize, src: usize, m: usize) {
    if s.no_nulls { return; }
    if dst == src { return; }
    let words = (m + 63) / 64;
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
    let words = (m + 63) / 64;
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
    for i in 0..m {
        let is_null = !scratch.no_nulls &&
            (scratch.null_bits[dst * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
        if is_null {
            scratch.regs[base_d + i] = 0;
        } else {
            let off = (morsel_start + i) * 16;
            let s = &col_data[off..off + 16];
            scratch.regs[base_d + i] =
                pred(compare_col_string_vs_const(s, blob, const_bytes, const_prefix, const_len)) as i64;
        }
    }
}

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
        let words = (m + 63) / 64;
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
    for i in 0..m {
        let is_null = !scratch.no_nulls &&
            (scratch.null_bits[dst * NULL_WORDS_PER_REG + i / 64] >> (i % 64)) & 1 != 0;
        if is_null {
            scratch.regs[base_d + i] = 0;
        } else {
            let row = morsel_start + i;
            let s1 = &col_a[row * 16..row * 16 + 16];
            let s2 = &col_b[row * 16..row * 16 + 16];
            scratch.regs[base_d + i] = pred(compare_german_strings(s1, blob, s2, blob)) as i64;
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
pub(crate) fn eval_batch(
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
                        read_u64_le(mb.pk_lo, (morsel_start + i) * 8) as i64;
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
                            read_u64_le(mb.pk_lo, (morsel_start + i) * 8) as i64;
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
                let words = (m + 63) / 64;
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
                let words = (m + 63) / 64;
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
                let words = (m + 63) / 64;
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
                let words = (m + 63) / 64;
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
                let words = (m + 63) / 64;
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::ExprProgram;
    use crate::storage::Batch;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

    fn make_schema_2col() -> SchemaDescriptor {
        let mut cols = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
        cols[0] = SchemaColumn { type_code: type_code::U64, size: 8, nullable: 0, _pad: 0 };
        cols[1] = SchemaColumn { type_code: type_code::I64, size: 8, nullable: 1, _pad: 0 };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns: cols }
    }

    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    #[test]
    fn test_scratch_reg3() {
        let mut s = EvalScratch::new();
        s.ensure_capacity(3, true, MORSEL);
        let m = 4;
        // Fill regs 0 and 1 with known values
        for i in 0..m { s.regs[0 * MORSEL + i] = (i as i64) + 1; }
        for i in 0..m { s.regs[1 * MORSEL + i] = (i as i64) * 10; }
        // Use reg3 to add reg0 + reg1 → reg2
        {
            let (ra, rb, rd) = s.reg3(0, 1, 2, m);
            for i in 0..m { rd[i] = ra[i] + rb[i]; }
        }
        assert_eq!(&s.regs[2 * MORSEL..2 * MORSEL + m], &[1, 12, 23, 34]);
    }

    #[test]
    fn test_scratch_null_words3() {
        let mut s = EvalScratch::new();
        s.ensure_capacity(3, false, MORSEL);
        let words = 1;
        s.null_bits[0 * NULL_WORDS_PER_REG] = 0b1010;
        s.null_bits[1 * NULL_WORDS_PER_REG] = 0b0110;
        {
            let (na, nb, nd) = s.null_words3(0, 1, 2, words);
            nd[0] = na[0] | nb[0];
        }
        assert_eq!(s.null_bits[2 * NULL_WORDS_PER_REG], 0b1110);
    }

    #[test]
    fn test_eval_batch_add() {
        use crate::expr;
        let schema = make_schema_2col();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let mb = batch.as_mem_batch();

        // r0 = pk (LOAD_PK_INT), r1 = col[1] (LOAD_PAYLOAD_INT), r2 = r0 + r1
        let code = vec![
            expr::EXPR_LOAD_PK_INT, 0, 0, 0,
            expr::EXPR_LOAD_PAYLOAD_INT, 1, 0, 0,  // pi=0 (first payload)
            expr::EXPR_INT_ADD, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let mut scratch = EvalScratch::new();
        scratch.ensure_capacity(3, true, 3);
        eval_batch(&prog, &mb, 0, 3, 0, &mut scratch);

        // row 0: pk=1, val=10, sum=11
        assert_eq!(scratch.regs[2 * MORSEL + 0], 11);
        // row 1: pk=2, val=20, sum=22
        assert_eq!(scratch.regs[2 * MORSEL + 1], 22);
        // row 2: pk=3, val=30, sum=33
        assert_eq!(scratch.regs[2 * MORSEL + 2], 33);
    }

    #[test]
    fn test_eval_batch_matches_eval_predicate() {
        use crate::expr::{self, eval_predicate, ExprProgram};

        let schema = make_schema_2col();
        // Build a predicate: col[1] > 15
        let code = vec![
            expr::EXPR_LOAD_PAYLOAD_INT, 0, 0, 0,  // r0 = payload[0]
            expr::EXPR_LOAD_CONST, 1, 15, 0,        // r1 = 15
            expr::EXPR_CMP_GT, 2, 0, 1,             // r2 = r0 > r1
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let mut resolved = ExprProgram::new(prog.code.clone(), 3, 2, vec![]);
        resolved.resolve_column_indices(0);

        let rows: &[(u64, i64, i64)] = &[(1, 1, 5), (2, 1, 15), (3, 1, 25), (4, 1, 0)];
        let batch = make_batch(&schema, rows);
        let mb = batch.as_mem_batch();

        let mut scratch = EvalScratch::new();
        scratch.ensure_capacity(3, true, 4);
        eval_batch(&resolved, &mb, 0, 4, 0, &mut scratch);

        for (i, &(_, _, val)) in rows.iter().enumerate() {
            let batch_result = scratch.regs[2 * MORSEL + i] != 0;
            let (row_val, row_null) = eval_predicate(&prog, &mb, i, 0);
            let row_result = !row_null && row_val != 0;
            assert_eq!(
                batch_result, row_result,
                "row {i}: val={val} batch={batch_result} row={row_result}",
            );
        }
    }
}
