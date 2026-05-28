//! Morsel-oriented batch expression evaluator.
//!
//! `eval_batch` processes one morsel at a time (up to MORSEL rows), applying
//! all expression opcodes as columnar loops over register buffers.  Base
//! pointers are hoisted outside the inner loop, letting LLVM auto-vectorize
//! every arithmetic opcode.

use std::cmp::Ordering;

use super::program::{
    ExprProgram,
    EXPR_LOAD_CONST, EXPR_LOAD_PAYLOAD_INT, EXPR_LOAD_PAYLOAD_FLOAT,
    EXPR_LOAD_PK_UNSIGNED_INT, EXPR_LOAD_PK_SIGNED_INT,
    EXPR_INT_ADD, EXPR_INT_SUB, EXPR_INT_MUL, EXPR_INT_DIV, EXPR_INT_MOD, EXPR_INT_NEG,
    EXPR_UDIV, EXPR_UMOD,
    EXPR_FLOAT_ADD, EXPR_FLOAT_SUB, EXPR_FLOAT_MUL, EXPR_FLOAT_DIV, EXPR_FLOAT_NEG,
    EXPR_CMP_EQ, EXPR_CMP_NE, EXPR_CMP_GT, EXPR_CMP_GE, EXPR_CMP_LT, EXPR_CMP_LE,
    EXPR_UCMP_GT, EXPR_UCMP_GE, EXPR_UCMP_LT, EXPR_UCMP_LE,
    EXPR_FCMP_EQ, EXPR_FCMP_NE, EXPR_FCMP_GT, EXPR_FCMP_GE, EXPR_FCMP_LT, EXPR_FCMP_LE,
    EXPR_BOOL_AND, EXPR_BOOL_OR, EXPR_BOOL_NOT,
    EXPR_IS_NULL, EXPR_IS_NOT_NULL,
    EXPR_INT_TO_FLOAT, EXPR_UINT_TO_FLOAT,
    EXPR_COPY_COL, EXPR_EMIT_NULL, EXPR_EMIT,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LT_CONST, EXPR_STR_COL_LE_CONST,
    EXPR_STR_COL_EQ_COL, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LE_COL,
    compare_col_string_vs_const,
};
use crate::storage::MemBatch;
use crate::schema::{compare_german_strings, PAYLOAD_MAPPING_PK_SENTINEL};
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
    /// Packed truthy bits, register-major; bridges boolean producers and
    /// consumers on the nullable arm without per-row repack from `regs`.
    /// Empty (capacity 0) when `no_nulls` is true.
    pub(in crate::expr) bool_bits: Vec<u64>,
    /// Per-row filter bitmask; set by filter_batch, ignored by execute_map.
    pub(in crate::expr) filter_bits: Vec<u64>,
    pub(in crate::expr) no_nulls: bool,
}

impl EvalScratch {
    pub(in crate::expr) fn new() -> Self {
        EvalScratch {
            regs: Vec::new(),
            null_bits: Vec::new(),
            bool_bits: Vec::new(),
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
            if self.bool_bits.len() < null_cap {
                self.bool_bits.resize(null_cap, 0);
            }
        }
        let filter_words = n.div_ceil(64);
        if self.filter_bits.len() < filter_words {
            self.filter_bits.resize(filter_words, 0);
        }
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

/// Fill null bits for a resolved payload byte (handles PK SENTINEL: always non-null).
fn fill_null_bits_for_resolved(
    s: &mut EvalScratch,
    di: usize,
    null_bmp: &[u8],
    morsel_start: usize,
    m: usize,
    pi_byte: u8,
) {
    if s.no_nulls { return; }
    if pi_byte == PAYLOAD_MAPPING_PK_SENTINEL {
        s.clear_null_reg(di, m);
        return;
    }
    fill_null_bits_pi(s, di, null_bmp, morsel_start, m, pi_byte as usize);
}

/// Shared BOOL_AND / BOOL_OR word-level 3VL kernel (nullable arm).
/// `va`/`vb` come from `bool_bits`; `na`/`nb` from `null_bits`.
/// Writes `bool_bits[dst]` and `null_bits[dst]` at word granularity.
fn bool_and_or_word_loop(
    scratch: &mut EvalScratch,
    dst: usize,
    ai: usize,
    bi: usize,
    m: usize,
    is_or: bool,
) {
    let words = m.div_ceil(64);
    let base_a_n = ai * NULL_WORDS_PER_REG;
    let base_b_n = bi * NULL_WORDS_PER_REG;
    let base_d_n = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        let va = scratch.bool_bits[base_a_n + w];
        let vb = scratch.bool_bits[base_b_n + w];
        let na = scratch.null_bits[base_a_n + w];
        let nb = scratch.null_bits[base_b_n + w];
        let (result_bits, nd) = if is_or {
            // SQL 3VL OR: definite_true wherever a non-null side is true;
            // result-null only when neither side is definitely-true and at
            // least one side is null.
            let definite_true = (!na & va) | (!nb & vb);
            let nd = !definite_true & (na | nb);
            (definite_true, nd)
        } else {
            let definite_false = (!na & !va) | (!nb & !vb);
            let nd = !definite_false & (na | nb);
            let result_bits = !nd & va & vb;
            (result_bits, nd)
        };
        scratch.bool_bits[base_d_n + w] = result_bits;
        scratch.null_bits[base_d_n + w] = nd;
    }
}

/// Unpack `bool_bits[dst]` into `regs[dst]` as 0/1 i64 per row.
fn unpack_bool_to_regs(scratch: &mut EvalScratch, dst: usize, m: usize) {
    let words = m.div_ceil(64);
    let base_r = dst * MORSEL;
    let base_b = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        let bits = scratch.bool_bits[base_b + w];
        for i in lo..hi {
            scratch.regs[base_r + i] = ((bits >> (i - lo)) & 1) as i64;
        }
    }
}

/// Pack one register's i64 truthy bits into `bool_bits[dst]`.
#[inline]
fn pack_to_bool_bits(scratch: &mut EvalScratch, dst: usize, m: usize) {
    let words = m.div_ceil(64);
    let base_r = dst * MORSEL;
    let base_b = dst * NULL_WORDS_PER_REG;
    for w in 0..words {
        let lo = w * 64;
        let hi = (lo + 64).min(m);
        let mut bits: u64 = 0;
        for i in lo..hi {
            bits |= ((scratch.regs[base_r + i] != 0) as u64) << (i - lo);
        }
        scratch.bool_bits[base_b + w] = bits;
    }
}

/// Bridge for producers that wrote `regs[dst]` and may have a downstream BOOL
/// consumer (or, for filters, a bit_only result_reg). Non-bool producers reach
/// a BOOL consumer through this path without restructuring their inner loop.
#[inline]
fn maybe_pack_bool_bits(scratch: &mut EvalScratch, prog: &ExprProgram, dst: usize, m: usize) {
    if scratch.no_nulls { return; }
    if prog.needs_bool_pack(dst) {
        pack_to_bool_bits(scratch, dst, m);
    }
}

/// Walk `dst`'s null mask and zero the matching register entries. Used by
/// the string-comparison helpers, which compute results unconditionally for
/// vectorization and then clear null rows in a post-pass.
fn zero_null_rows(scratch: &mut EvalScratch, dst: usize, m: usize) {
    if scratch.no_nulls { return; }
    let base_d = dst * MORSEL;
    let base_null = dst * NULL_WORDS_PER_REG;
    let words = m.div_ceil(64);
    for w in 0..words {
        let mut null_word = scratch.null_bits[base_null + w];
        let lo = w * 64;
        while null_word != 0 {
            let bit = null_word.trailing_zeros() as usize;
            scratch.regs[base_d + lo + bit] = 0;
            null_word &= null_word - 1;
        }
    }
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
    pi_byte: u8,
    const_idx: usize,
    pred: impl Fn(Ordering) -> bool,
) {
    debug_assert!(
        pi_byte != PAYLOAD_MAPPING_PK_SENTINEL,
        "eval_str_col_vs_const: PK column is never a string",
    );
    let pi = pi_byte as usize;
    fill_null_bits_for_resolved(scratch, dst, mb.null_bmp(), morsel_start, m, pi_byte);
    let col_data = mb.col_data(pi, 16);
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
    zero_null_rows(scratch, dst, m);
    maybe_pack_bool_bits(scratch, prog, dst, m);
}

#[allow(clippy::too_many_arguments)]
fn eval_str_col_vs_col(
    scratch: &mut EvalScratch,
    mb: &MemBatch,
    prog: &ExprProgram,
    dst: usize,
    morsel_start: usize,
    m: usize,
    pi_byte_a: u8,
    pi_byte_b: u8,
    pred: impl Fn(Ordering) -> bool,
) {
    debug_assert!(
        pi_byte_a != PAYLOAD_MAPPING_PK_SENTINEL
            && pi_byte_b != PAYLOAD_MAPPING_PK_SENTINEL,
        "eval_str_col_vs_col: PK column is never a string",
    );
    fill_null_bits_for_resolved(scratch, dst, mb.null_bmp(), morsel_start, m, pi_byte_a);
    if !scratch.no_nulls {
        let words = m.div_ceil(64);
        let pi_b = pi_byte_b as usize;
        for w in 0..words {
            let lo = w * 64;
            let hi = (lo + 64).min(m);
            let mut extra: u64 = 0;
            for i in lo..hi {
                let row_null = read_u64_le(mb.null_bmp(), (morsel_start + i) * 8);
                if (row_null >> pi_b) & 1 != 0 { extra |= 1u64 << (i - lo); }
            }
            scratch.null_bits[dst * NULL_WORDS_PER_REG + w] |= extra;
        }
    }
    let pi_a = pi_byte_a as usize;
    let pi_b = pi_byte_b as usize;
    let col_a = mb.col_data(pi_a, 16);
    let col_b = mb.col_data(pi_b, 16);
    let blob = mb.blob;
    let base_d = dst * MORSEL;
    // Compute results unconditionally; null rows produce a valid but irrelevant value.
    for i in 0..m {
        let row = morsel_start + i;
        let s1 = &col_a[row * 16..row * 16 + 16];
        let s2 = &col_b[row * 16..row * 16 + 16];
        scratch.regs[base_d + i] = pred(compare_german_strings(s1, blob, s2, blob)) as i64;
    }
    zero_null_rows(scratch, dst, m);
    maybe_pack_bool_bits(scratch, prog, dst, m);
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
    scratch: &mut EvalScratch,
) {
    debug_assert!(
        prog.resolved,
        "eval_batch: program must be resolved via resolve_column_indices",
    );

    // Four opcode families share one shape each; these macros collapse them
    // to one line per opcode. `int_div_like!` covers DIV/MOD/FLOAT_DIV which
    // additionally merge a zero-divisor mask into the destination null word.
    macro_rules! bin_int {
        ($a:expr, $b:expr, $d:expr, $op:ident) => {{
            let ai = $a as usize; let bi = $b as usize;
            {
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m { rd[i] = ra[i].$op(rb[i]); }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, prog, $d, m);
        }};
    }
    macro_rules! cmp_int {
        ($a:expr, $b:expr, $d:expr, $op:tt) => {{
            let ai = $a as usize; let bi = $b as usize;
            {
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m { rd[i] = (ra[i] $op rb[i]) as i64; }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, prog, $d, m);
        }};
    }
    // Unsigned variant of cmp_int: reinterpret the i64 register bit pattern as
    // u64 so values >= 2^63 compare as the large positives they represent.
    macro_rules! cmp_uint {
        ($a:expr, $b:expr, $d:expr, $op:tt) => {{
            let ai = $a as usize; let bi = $b as usize;
            {
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m { rd[i] = ((ra[i] as u64) $op (rb[i] as u64)) as i64; }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, prog, $d, m);
        }};
    }
    macro_rules! bin_float {
        ($a:expr, $b:expr, $d:expr, $op:tt) => {{
            let ai = $a as usize; let bi = $b as usize;
            {
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m {
                    let fa = f64::from_bits(ra[i] as u64);
                    let fb = f64::from_bits(rb[i] as u64);
                    rd[i] = f64::to_bits(fa $op fb) as i64;
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, prog, $d, m);
        }};
    }
    macro_rules! cmp_float {
        ($a:expr, $b:expr, $d:expr, $op:tt) => {{
            let ai = $a as usize; let bi = $b as usize;
            {
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m {
                    let fa = f64::from_bits(ra[i] as u64);
                    let fb = f64::from_bits(rb[i] as u64);
                    rd[i] = (fa $op fb) as i64;
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            maybe_pack_bool_bits(scratch, prog, $d, m);
        }};
    }
    // DIV-shaped op: compute per-row, mark divide-by-zero rows null, substitute
    // a safe divisor so the computed slot holds a defined (irrelevant) value.
    // `body(a, b)` must return `(result: i64, is_zero: bool)`.
    macro_rules! div_like {
        ($a:expr, $b:expr, $d:expr, |$a_i:ident, $b_i:ident| $body:expr) => {{
            let ai = $a as usize; let bi = $b as usize;
            let mut zero_mask = [0u64; NULL_WORDS_PER_REG];
            {
                let (ra, rb, rd) = scratch.reg3(ai, bi, $d, m);
                for i in 0..m {
                    let $a_i = ra[i];
                    let $b_i = rb[i];
                    let (val, is_zero): (i64, bool) = $body;
                    rd[i] = val;
                    if is_zero { zero_mask[i / 64] |= 1u64 << (i % 64); }
                }
            }
            null_or2(scratch, $d, ai, bi, m);
            if !scratch.no_nulls {
                let base_null_d = $d * NULL_WORDS_PER_REG;
                let words = m.div_ceil(64);
                for w in 0..words { scratch.null_bits[base_null_d + w] |= zero_mask[w]; }
            }
            maybe_pack_bool_bits(scratch, prog, $d, m);
        }};
    }

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
                let (size_u8, col_tc) = prog.payload_col_info[pi];
                let col_size = size_u8 as usize;
                let is_signed = matches!(col_tc,
                    crate::schema::type_code::I8 |
                    crate::schema::type_code::I16 |
                    crate::schema::type_code::I32 |
                    crate::schema::type_code::I64);
                let col_data = mb.col_data(pi, col_size);
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
                fill_null_bits_pi(scratch, dst, mb.null_bmp(), morsel_start, m, pi);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            EXPR_LOAD_PAYLOAD_FLOAT => {
                let pi = a1 as usize;
                let col_size = prog.payload_col_info[pi].0 as usize;
                let col_data = mb.col_data(pi, col_size);
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
                fill_null_bits_pi(scratch, dst, mb.null_bmp(), morsel_start, m, pi);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // a1 packs (pk_byte_offset << 16) | (col_size << 8) | type_code.
            // Unsigned: the addressed OPK column is big-endian, so right-align
            // into a 16-byte buffer and from_be_bytes recovers the native value.
            EXPR_LOAD_PK_UNSIGNED_INT => {
                let packed = a1 as u64;
                let byte_offset = (packed >> 16) as usize;
                let col_size = ((packed >> 8) & 0xFF) as usize;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    let opk = mb.get_pk_bytes(morsel_start + i);
                    let mut buf = [0u8; 16];
                    buf[16 - col_size..].copy_from_slice(&opk[byte_offset..byte_offset + col_size]);
                    scratch.regs[base_d + i] = u128::from_be_bytes(buf) as i64;
                }
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }
            // Signed: decode the OPK column back to native LE (un-flips the sign
            // bit), then sign-extend to i64 at the exact column width. Padding a
            // narrow type before from_le_bytes would destroy sign extension.
            EXPR_LOAD_PK_SIGNED_INT => {
                let packed = a1 as u64;
                let byte_offset = (packed >> 16) as usize;
                let col_size = ((packed >> 8) & 0xFF) as usize;
                let type_code = (packed & 0xFF) as u8;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    let opk = mb.get_pk_bytes(morsel_start + i);
                    let mut le = [0u8; 8];
                    gnitz_wire::decode_pk_column(
                        &opk[byte_offset..byte_offset + col_size],
                        type_code,
                        &mut le[..col_size],
                    );
                    scratch.regs[base_d + i] = match col_size {
                        8 => i64::from_le_bytes(le),
                        4 => i32::from_le_bytes(le[..4].try_into().unwrap()) as i64,
                        2 => i16::from_le_bytes(le[..2].try_into().unwrap()) as i64,
                        1 => le[0] as i8 as i64,
                        _ => unreachable!("signed PK column size {col_size}"),
                    };
                }
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            EXPR_LOAD_CONST => {
                let val: i64 = (a2 << 32) | (a1 & 0xFFFF_FFFF);
                let base_d = dst * MORSEL;
                for i in 0..m { scratch.regs[base_d + i] = val; }
                scratch.clear_null_reg(dst, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // ----------------------------------------------------------------
            // Integer arithmetic
            // ----------------------------------------------------------------
            EXPR_INT_ADD => bin_int!(a1, a2, dst, wrapping_add),
            EXPR_INT_SUB => bin_int!(a1, a2, dst, wrapping_sub),
            EXPR_INT_MUL => bin_int!(a1, a2, dst, wrapping_mul),
            EXPR_INT_DIV => div_like!(a1, a2, dst, |a, b| {
                let is_zero = b == 0;
                let d = if is_zero { 1 } else { b };
                (a.wrapping_div(d), is_zero)
            }),
            EXPR_INT_MOD => div_like!(a1, a2, dst, |a, b| {
                let is_zero = b == 0;
                let d = if is_zero { 1 } else { b };
                (a.wrapping_rem(d), is_zero)
            }),
            // Unsigned (U64) division/modulo: reinterpret operands as u64 so the
            // quotient/remainder is correct for dividends >= 2^63. Zero divisor
            // marks the row NULL, matching the signed INT_DIV/INT_MOD path.
            EXPR_UDIV => div_like!(a1, a2, dst, |a, b| {
                let is_zero = b == 0;
                let d = if is_zero { 1u64 } else { b as u64 };
                (((a as u64) / d) as i64, is_zero)
            }),
            EXPR_UMOD => div_like!(a1, a2, dst, |a, b| {
                let is_zero = b == 0;
                let d = if is_zero { 1u64 } else { b as u64 };
                (((a as u64) % d) as i64, is_zero)
            }),
            EXPR_INT_NEG => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] = scratch.regs[base_a + i].wrapping_neg();
                }
                null_or1(scratch, dst, ai, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // ----------------------------------------------------------------
            // Float arithmetic
            // ----------------------------------------------------------------
            EXPR_FLOAT_ADD => bin_float!(a1, a2, dst, +),
            EXPR_FLOAT_SUB => bin_float!(a1, a2, dst, -),
            EXPR_FLOAT_MUL => bin_float!(a1, a2, dst, *),
            EXPR_FLOAT_DIV => div_like!(a1, a2, dst, |a, b| {
                let fa = f64::from_bits(a as u64);
                let fb = f64::from_bits(b as u64);
                let is_zero = fb == 0.0;
                let fb_safe = if is_zero { 1.0 } else { fb };
                (f64::to_bits(fa / fb_safe) as i64, is_zero)
            }),
            EXPR_FLOAT_NEG => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    let fa = f64::from_bits(scratch.regs[base_a + i] as u64);
                    scratch.regs[base_d + i] = f64::to_bits(-fa) as i64;
                }
                null_or1(scratch, dst, ai, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // ----------------------------------------------------------------
            // Integer comparisons
            // ----------------------------------------------------------------
            EXPR_CMP_EQ => cmp_int!(a1, a2, dst, ==),
            EXPR_CMP_NE => cmp_int!(a1, a2, dst, !=),
            EXPR_CMP_GT => cmp_int!(a1, a2, dst, >),
            EXPR_CMP_GE => cmp_int!(a1, a2, dst, >=),
            EXPR_CMP_LT => cmp_int!(a1, a2, dst, <),
            EXPR_CMP_LE => cmp_int!(a1, a2, dst, <=),

            // Unsigned (U64) integer comparisons
            EXPR_UCMP_GT => cmp_uint!(a1, a2, dst, >),
            EXPR_UCMP_GE => cmp_uint!(a1, a2, dst, >=),
            EXPR_UCMP_LT => cmp_uint!(a1, a2, dst, <),
            EXPR_UCMP_LE => cmp_uint!(a1, a2, dst, <=),

            // ----------------------------------------------------------------
            // Float comparisons
            // ----------------------------------------------------------------
            EXPR_FCMP_EQ => cmp_float!(a1, a2, dst, ==),
            EXPR_FCMP_NE => cmp_float!(a1, a2, dst, !=),
            EXPR_FCMP_GT => cmp_float!(a1, a2, dst, >),
            EXPR_FCMP_GE => cmp_float!(a1, a2, dst, >=),
            EXPR_FCMP_LT => cmp_float!(a1, a2, dst, <),
            EXPR_FCMP_LE => cmp_float!(a1, a2, dst, <=),

            // ----------------------------------------------------------------
            // Boolean 3VL
            // ----------------------------------------------------------------
            EXPR_BOOL_AND => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                if scratch.no_nulls {
                    let base_a = ai * MORSEL;
                    let base_b = bi * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            ((scratch.regs[base_a + i] != 0) && (scratch.regs[base_b + i] != 0)) as i64;
                    }
                } else {
                    // Nullable arm: word-level u64 3VL on packed truthy bits.
                    // Upstream producers populate `bool_bits` via
                    // `needs_bool_pack`; the unpack to `regs[dst]` is skipped
                    // for bit_only destinations whose only readers are BOOL.
                    bool_and_or_word_loop(scratch, dst, ai, bi, m, /* is_or = */ false);
                    if !prog.is_bit_only(dst) {
                        unpack_bool_to_regs(scratch, dst, m);
                    }
                }
            }
            EXPR_BOOL_OR => {
                let ai = a1 as usize;
                let bi = a2 as usize;
                if scratch.no_nulls {
                    let base_a = ai * MORSEL;
                    let base_b = bi * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] =
                            ((scratch.regs[base_a + i] != 0) || (scratch.regs[base_b + i] != 0)) as i64;
                    }
                } else {
                    bool_and_or_word_loop(scratch, dst, ai, bi, m, /* is_or = */ true);
                    if !prog.is_bit_only(dst) {
                        unpack_bool_to_regs(scratch, dst, m);
                    }
                }
            }
            EXPR_BOOL_NOT => {
                let ai = a1 as usize;
                if scratch.no_nulls {
                    let base_a = ai * MORSEL;
                    let base_d = dst * MORSEL;
                    for i in 0..m {
                        scratch.regs[base_d + i] = (scratch.regs[base_a + i] == 0) as i64;
                    }
                    null_or1(scratch, dst, ai, m);
                } else {
                    let words = m.div_ceil(64);
                    let base_a = ai * NULL_WORDS_PER_REG;
                    let base_d = dst * NULL_WORDS_PER_REG;
                    for w in 0..words {
                        let va = scratch.bool_bits[base_a + w];
                        let na = scratch.null_bits[base_a + w];
                        // 3VL: NOT NULL = NULL. Result truthy = !va & !na (cleared
                        // in null positions); null-bits unchanged.
                        scratch.bool_bits[base_d + w] = !va & !na;
                        scratch.null_bits[base_d + w] = na;
                    }
                    if !prog.is_bit_only(dst) {
                        unpack_bool_to_regs(scratch, dst, m);
                    }
                }
            }

            // ----------------------------------------------------------------
            // IS NULL / IS NOT NULL
            // ----------------------------------------------------------------
            EXPR_IS_NULL => {
                let pi_byte = a1 as u8;
                scratch.clear_null_reg(dst, m);
                let base_d = dst * MORSEL;
                if pi_byte == PAYLOAD_MAPPING_PK_SENTINEL {
                    for i in 0..m { scratch.regs[base_d + i] = 0; }
                } else {
                    let pi = pi_byte as usize;
                    let null_bmp = mb.null_bmp();
                    for i in 0..m {
                        let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
                        scratch.regs[base_d + i] = ((row_null >> pi) & 1) as i64;
                    }
                }
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }
            EXPR_IS_NOT_NULL => {
                let pi_byte = a1 as u8;
                scratch.clear_null_reg(dst, m);
                let base_d = dst * MORSEL;
                if pi_byte == PAYLOAD_MAPPING_PK_SENTINEL {
                    for i in 0..m { scratch.regs[base_d + i] = 1; }
                } else {
                    let pi = pi_byte as usize;
                    let null_bmp = mb.null_bmp();
                    for i in 0..m {
                        let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
                        scratch.regs[base_d + i] = (((row_null >> pi) & 1) ^ 1) as i64;
                    }
                }
                maybe_pack_bool_bits(scratch, prog, dst, m);
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
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }
            // Unsigned (U64) → float: reinterpret the register as u64 first so
            // values >= 2^63 cast to the correct large positive float.
            EXPR_UINT_TO_FLOAT => {
                let ai = a1 as usize;
                let base_a = ai * MORSEL;
                let base_d = dst * MORSEL;
                for i in 0..m {
                    scratch.regs[base_d + i] =
                        f64::to_bits(scratch.regs[base_a + i] as u64 as f64) as i64;
                }
                null_or1(scratch, dst, ai, m);
                maybe_pack_bool_bits(scratch, prog, dst, m);
            }

            // ----------------------------------------------------------------
            // String comparisons (column vs constant)
            // ----------------------------------------------------------------
            EXPR_STR_COL_EQ_CONST => eval_str_col_vs_const(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as u8, a2 as usize, |o| o == Ordering::Equal,
            ),
            EXPR_STR_COL_LT_CONST => eval_str_col_vs_const(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as u8, a2 as usize, |o| o == Ordering::Less,
            ),
            EXPR_STR_COL_LE_CONST => eval_str_col_vs_const(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as u8, a2 as usize, |o| o != Ordering::Greater,
            ),

            // ----------------------------------------------------------------
            // String comparisons (column vs column)
            // ----------------------------------------------------------------
            EXPR_STR_COL_EQ_COL => eval_str_col_vs_col(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as u8, a2 as u8, |o| o == Ordering::Equal,
            ),
            EXPR_STR_COL_LT_COL => eval_str_col_vs_col(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as u8, a2 as u8, |o| o == Ordering::Less,
            ),
            EXPR_STR_COL_LE_COL => eval_str_col_vs_col(
                scratch, mb, prog, dst, morsel_start, m,
                a1 as u8, a2 as u8, |o| o != Ordering::Greater,
            ),

            _ => {}
        }
    }
}
