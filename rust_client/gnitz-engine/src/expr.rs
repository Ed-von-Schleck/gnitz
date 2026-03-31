//! Expression bytecode interpreter for SQL scalar functions.
//!
//! Evaluates compiled expression programs against columnar batch rows.
//! Port of gnitz/dbsp/expr.py — same bytecode format, same semantics.

use crate::compact::{
    compare_german_strings, string_byte,
};
use crate::merge::MemBatch;
use crate::util::read_u32_le;

// ---------------------------------------------------------------------------
// Expression opcodes (must match expr.py)
// ---------------------------------------------------------------------------

pub const EXPR_LOAD_COL_INT: i64 = 1;
pub const EXPR_LOAD_COL_FLOAT: i64 = 2;
pub const EXPR_LOAD_CONST: i64 = 3;

pub const EXPR_INT_ADD: i64 = 4;
pub const EXPR_INT_SUB: i64 = 5;
pub const EXPR_INT_MUL: i64 = 6;
pub const EXPR_INT_DIV: i64 = 7;
pub const EXPR_INT_MOD: i64 = 8;
pub const EXPR_INT_NEG: i64 = 9;

pub const EXPR_FLOAT_ADD: i64 = 10;
pub const EXPR_FLOAT_SUB: i64 = 11;
pub const EXPR_FLOAT_MUL: i64 = 12;
pub const EXPR_FLOAT_DIV: i64 = 13;
pub const EXPR_FLOAT_NEG: i64 = 14;

pub const EXPR_CMP_EQ: i64 = 15;
pub const EXPR_CMP_NE: i64 = 16;
pub const EXPR_CMP_GT: i64 = 17;
pub const EXPR_CMP_GE: i64 = 18;
pub const EXPR_CMP_LT: i64 = 19;
pub const EXPR_CMP_LE: i64 = 20;

pub const EXPR_FCMP_EQ: i64 = 21;
pub const EXPR_FCMP_NE: i64 = 22;
pub const EXPR_FCMP_GT: i64 = 23;
pub const EXPR_FCMP_GE: i64 = 24;
pub const EXPR_FCMP_LT: i64 = 25;
pub const EXPR_FCMP_LE: i64 = 26;

pub const EXPR_BOOL_AND: i64 = 27;
pub const EXPR_BOOL_OR: i64 = 28;
pub const EXPR_BOOL_NOT: i64 = 29;

pub const EXPR_IS_NULL: i64 = 30;
pub const EXPR_IS_NOT_NULL: i64 = 31;

pub const EXPR_EMIT: i64 = 32;
pub const EXPR_INT_TO_FLOAT: i64 = 33;
pub const EXPR_COPY_COL: i64 = 34;

pub const EXPR_STR_COL_EQ_CONST: i64 = 40;
pub const EXPR_STR_COL_LT_CONST: i64 = 41;
pub const EXPR_STR_COL_LE_CONST: i64 = 42;
pub const EXPR_STR_COL_EQ_COL: i64 = 43;
pub const EXPR_STR_COL_LT_COL: i64 = 44;
pub const EXPR_STR_COL_LE_COL: i64 = 45;

pub const EXPR_EMIT_NULL: i64 = 46;

// ---------------------------------------------------------------------------
// ExprProgram — immutable bytecode container
// ---------------------------------------------------------------------------

pub struct ExprProgram {
    pub code: Vec<i64>,
    pub num_regs: u32,
    pub result_reg: u32,
    pub num_instrs: u32,
    pub const_strings: Vec<Vec<u8>>,
    pub const_prefixes: Vec<u32>,
    pub const_lengths: Vec<u32>,
}

impl ExprProgram {
    pub fn new(
        code: Vec<i64>,
        num_regs: u32,
        result_reg: u32,
        const_strings: Vec<Vec<u8>>,
    ) -> Self {
        let num_instrs = code.len() as u32 / 4;
        let mut const_prefixes = Vec::with_capacity(const_strings.len());
        let mut const_lengths = Vec::with_capacity(const_strings.len());
        for s in &const_strings {
            const_prefixes.push(compute_prefix(s));
            const_lengths.push(s.len() as u32);
        }
        gnitz_debug!("expr_program: instrs={} regs={} consts={}", num_instrs, num_regs, const_strings.len());
        ExprProgram {
            code,
            num_regs,
            result_reg,
            num_instrs,
            const_strings,
            const_prefixes,
            const_lengths,
        }
    }
}

/// Compute the 4-byte prefix of a string for German String comparison.
fn compute_prefix(s: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = s.len().min(4);
    buf[..n].copy_from_slice(&s[..n]);
    u32::from_le_bytes(buf)
}

// ---------------------------------------------------------------------------
// Float bit-reinterpretation (matches RPython float2longlong / longlong2float)
// ---------------------------------------------------------------------------

#[inline]
fn float_to_bits(f: f64) -> i64 {
    i64::from_ne_bytes(f.to_ne_bytes())
}

#[inline]
fn bits_to_float(bits: i64) -> f64 {
    f64::from_ne_bytes(bits.to_ne_bytes())
}

// ---------------------------------------------------------------------------
// Column read helpers
// ---------------------------------------------------------------------------

/// Read a signed integer column value from a batch row.
/// Handles PK special-casing and payload index mapping.
#[inline]
fn read_col_int(batch: &MemBatch, row: usize, col_idx: usize, pk_index: usize) -> i64 {
    if col_idx == pk_index {
        batch.get_pk_lo(row) as i64
    } else {
        let pi = if col_idx < pk_index { col_idx } else { col_idx - 1 };
        // Read as i64 from the payload column (always 8 bytes for int types)
        let ptr = batch.get_col_ptr(row, pi, 8);
        i64::from_le_bytes(ptr.try_into().unwrap())
    }
}

/// Read a float column value from a batch row, returning as i64 bits.
#[inline]
fn read_col_float(batch: &MemBatch, row: usize, col_idx: usize, pk_index: usize) -> i64 {
    if col_idx == pk_index {
        float_to_bits(0.0)
    } else {
        let pi = if col_idx < pk_index { col_idx } else { col_idx - 1 };
        let ptr = batch.get_col_ptr(row, pi, 8);
        // Raw bytes — already in float bit representation
        i64::from_le_bytes(ptr.try_into().unwrap())
    }
}

/// Check if a column is null for a given row.
#[inline]
fn is_col_null(null_word: u64, col_idx: usize, pk_index: usize) -> bool {
    if col_idx == pk_index {
        false
    } else {
        let pi = if col_idx < pk_index { col_idx } else { col_idx - 1 };
        (null_word >> pi) & 1 != 0
    }
}

/// Get the German String struct (16 bytes) and blob slice for a string column.
#[inline]
fn get_str_struct<'a>(
    batch: &'a MemBatch,
    row: usize,
    col_idx: usize,
    pk_index: usize,
) -> (&'a [u8], &'a [u8]) {
    let pi = if col_idx < pk_index { col_idx } else { col_idx - 1 };
    let struct_bytes = batch.get_col_ptr(row, pi, 16);
    (struct_bytes, batch.blob)
}

// ---------------------------------------------------------------------------
// String comparison helpers (column vs constant)
// ---------------------------------------------------------------------------

/// Compare a German String column value against a constant byte string.
/// Returns Ordering.
fn compare_col_string_vs_const(
    struct_bytes: &[u8],
    blob: &[u8],
    const_bytes: &[u8],
    const_prefix: u32,
    const_len: u32,
) -> std::cmp::Ordering {
    let col_len = read_u32_le(struct_bytes, 0) as usize;
    let c_len = const_len as usize;
    let min_len = col_len.min(c_len);

    // Compare prefix bytes (first 4 bytes)
    let limit = min_len.min(4);
    let col_prefix = &struct_bytes[4..4 + limit];
    let const_prefix_bytes = const_prefix.to_le_bytes();
    for i in 0..limit {
        if col_prefix[i] != const_prefix_bytes[i] {
            return col_prefix[i].cmp(&const_prefix_bytes[i]);
        }
    }

    if min_len <= 4 {
        return col_len.cmp(&c_len);
    }

    // Compare remaining bytes
    for i in 4..min_len {
        let ca = string_byte(struct_bytes, blob, col_len, i);
        let cb = const_bytes[i];
        if ca != cb {
            return ca.cmp(&cb);
        }
    }
    col_len.cmp(&c_len)
}

/// Check equality of a German String column value against a constant byte string.
fn col_string_equals_const(
    struct_bytes: &[u8],
    blob: &[u8],
    const_bytes: &[u8],
    const_prefix: u32,
    const_len: u32,
) -> bool {
    let col_len = read_u32_le(struct_bytes, 0) as usize;
    let c_len = const_len as usize;
    if col_len != c_len {
        return false;
    }
    // Compare prefix
    let col_prefix = read_u32_le(struct_bytes, 4);
    if col_prefix != const_prefix {
        return false;
    }
    if col_len <= 4 {
        return true;
    }
    // Compare remaining bytes
    for i in 4..col_len {
        if string_byte(struct_bytes, blob, col_len, i) != const_bytes[i] {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Evaluation: predicate mode (no output)
// ---------------------------------------------------------------------------

/// Evaluate expression as predicate against a batch row.
/// Returns (value, is_null).
pub fn eval_predicate(
    prog: &ExprProgram,
    batch: &MemBatch,
    row: usize,
    pk_index: u32,
) -> (i64, bool) {
    let pki = pk_index as usize;
    let null_word = batch.get_null_word(row);
    let mut regs = vec![0i64; prog.num_regs as usize];
    let mut null = vec![false; prog.num_regs as usize];

    for i in 0..prog.num_instrs as usize {
        let base = i * 4;
        let op = prog.code[base];
        let dst = prog.code[base + 1] as usize;
        let a1 = prog.code[base + 2];
        let a2 = prog.code[base + 3];

        match op {
            EXPR_LOAD_COL_INT => {
                let ci = a1 as usize;
                null[dst] = is_col_null(null_word, ci, pki);
                regs[dst] = read_col_int(batch, row, ci, pki);
            }
            EXPR_LOAD_COL_FLOAT => {
                let ci = a1 as usize;
                null[dst] = is_col_null(null_word, ci, pki);
                regs[dst] = read_col_float(batch, row, ci, pki);
            }
            EXPR_LOAD_CONST => {
                null[dst] = false;
                regs[dst] = (a2 << 32) | (a1 & 0xFFFF_FFFF);
            }
            EXPR_INT_ADD => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = regs[a1 as usize].wrapping_add(regs[a2 as usize]);
            }
            EXPR_INT_SUB => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = regs[a1 as usize].wrapping_sub(regs[a2 as usize]);
            }
            EXPR_INT_MUL => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = regs[a1 as usize].wrapping_mul(regs[a2 as usize]);
            }
            EXPR_INT_DIV => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null[dst] = true;
                    regs[dst] = 0;
                } else {
                    null[dst] = null[a1 as usize] || null[a2 as usize];
                    regs[dst] = regs[a1 as usize].wrapping_div(d);
                }
            }
            EXPR_INT_MOD => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null[dst] = true;
                    regs[dst] = 0;
                } else {
                    null[dst] = null[a1 as usize] || null[a2 as usize];
                    regs[dst] = regs[a1 as usize].wrapping_rem(d);
                }
            }
            EXPR_INT_NEG => {
                null[dst] = null[a1 as usize];
                regs[dst] = regs[a1 as usize].wrapping_neg();
            }
            EXPR_FLOAT_ADD => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) + bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_SUB => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) - bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_MUL => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) * bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_DIV => {
                let rhs = bits_to_float(regs[a2 as usize]);
                if rhs == 0.0 {
                    null[dst] = true;
                    regs[dst] = 0;
                } else {
                    null[dst] = null[a1 as usize] || null[a2 as usize];
                    regs[dst] = float_to_bits(bits_to_float(regs[a1 as usize]) / rhs);
                }
            }
            EXPR_FLOAT_NEG => {
                null[dst] = null[a1 as usize];
                regs[dst] = float_to_bits(-bits_to_float(regs[a1 as usize]));
            }
            EXPR_CMP_EQ => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] == regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_NE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] != regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] > regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] >= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] < regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] <= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_FCMP_EQ => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    == bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_NE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    != bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    > bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    >= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    < bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    <= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_AND => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] != 0 && regs[a2 as usize] != 0 {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_OR => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] != 0 || regs[a2 as usize] != 0 {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_NOT => {
                null[dst] = null[a1 as usize];
                regs[dst] = if regs[a1 as usize] == 0 { 1 } else { 0 };
            }
            EXPR_IS_NULL => {
                null[dst] = false;
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    1
                } else {
                    0
                };
            }
            EXPR_IS_NOT_NULL => {
                null[dst] = false;
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    0
                } else {
                    1
                };
            }
            EXPR_INT_TO_FLOAT => {
                null[dst] = null[a1 as usize];
                regs[dst] = float_to_bits(regs[a1 as usize] as f64);
            }
            EXPR_STR_COL_EQ_CONST => {
                null[dst] = is_col_null(null_word, a1 as usize, pki);
                if !null[dst] {
                    let (s, blob) = get_str_struct(batch, row, a1 as usize, pki);
                    let ci = a2 as usize;
                    regs[dst] = if col_string_equals_const(
                        s,
                        blob,
                        &prog.const_strings[ci],
                        prog.const_prefixes[ci],
                        prog.const_lengths[ci],
                    ) {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LT_CONST => {
                null[dst] = is_col_null(null_word, a1 as usize, pki);
                if !null[dst] {
                    let (s, blob) = get_str_struct(batch, row, a1 as usize, pki);
                    let ci = a2 as usize;
                    let cmp = compare_col_string_vs_const(
                        s,
                        blob,
                        &prog.const_strings[ci],
                        prog.const_prefixes[ci],
                        prog.const_lengths[ci],
                    );
                    regs[dst] = if cmp == std::cmp::Ordering::Less { 1 } else { 0 };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LE_CONST => {
                null[dst] = is_col_null(null_word, a1 as usize, pki);
                if !null[dst] {
                    let (s, blob) = get_str_struct(batch, row, a1 as usize, pki);
                    let ci = a2 as usize;
                    let cmp = compare_col_string_vs_const(
                        s,
                        blob,
                        &prog.const_strings[ci],
                        prog.const_prefixes[ci],
                        prog.const_lengths[ci],
                    );
                    regs[dst] = if cmp != std::cmp::Ordering::Greater {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_EQ_COL => {
                let n_a1 = is_col_null(null_word, a1 as usize, pki);
                let n_a2 = is_col_null(null_word, a2 as usize, pki);
                null[dst] = n_a1 || n_a2;
                if !null[dst] {
                    let (s1, blob1) = get_str_struct(batch, row, a1 as usize, pki);
                    let (s2, blob2) = get_str_struct(batch, row, a2 as usize, pki);
                    regs[dst] = if compare_german_strings(s1, blob1, s2, blob2)
                        == std::cmp::Ordering::Equal
                    {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LT_COL => {
                let n_a1 = is_col_null(null_word, a1 as usize, pki);
                let n_a2 = is_col_null(null_word, a2 as usize, pki);
                null[dst] = n_a1 || n_a2;
                if !null[dst] {
                    let (s1, blob1) = get_str_struct(batch, row, a1 as usize, pki);
                    let (s2, blob2) = get_str_struct(batch, row, a2 as usize, pki);
                    regs[dst] = if compare_german_strings(s1, blob1, s2, blob2)
                        == std::cmp::Ordering::Less
                    {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LE_COL => {
                let n_a1 = is_col_null(null_word, a1 as usize, pki);
                let n_a2 = is_col_null(null_word, a2 as usize, pki);
                null[dst] = n_a1 || n_a2;
                if !null[dst] {
                    let (s1, blob1) = get_str_struct(batch, row, a1 as usize, pki);
                    let (s2, blob2) = get_str_struct(batch, row, a2 as usize, pki);
                    regs[dst] = if compare_german_strings(s1, blob1, s2, blob2)
                        != std::cmp::Ordering::Greater
                    {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            // EMIT, COPY_COL, EMIT_NULL: no-ops in predicate mode
            _ => {}
        }
    }

    if prog.num_regs == 0 {
        return (0, true);
    }
    (regs[prog.result_reg as usize], null[prog.result_reg as usize])
}

// ---------------------------------------------------------------------------
// Evaluation: with EMIT output for map batch path
// ---------------------------------------------------------------------------

/// Target for EMIT output: pre-allocated column buffer.
pub struct EmitTarget {
    pub base: *mut u8,
    pub stride: usize,
    pub payload_col: usize,
}

/// Evaluate expression with EMIT output against a batch row.
/// Writes EMIT values to pre-allocated column buffers.
/// Returns (result_value, result_is_null, emit_null_mask).
pub fn eval_with_emit(
    prog: &ExprProgram,
    batch: &MemBatch,
    row: usize,
    pk_index: u32,
    null_word: u64,
    emit_targets: &[EmitTarget],
) -> (i64, bool, u64) {
    let pki = pk_index as usize;
    let mut regs = vec![0i64; prog.num_regs as usize];
    let mut null = vec![false; prog.num_regs as usize];
    let mut emit_null_mask: u64 = 0;
    let mut emit_idx: usize = 0;

    for i in 0..prog.num_instrs as usize {
        let base = i * 4;
        let op = prog.code[base];
        let dst = prog.code[base + 1] as usize;
        let a1 = prog.code[base + 2];
        let a2 = prog.code[base + 3];

        match op {
            EXPR_COPY_COL | EXPR_EMIT_NULL => {
                // Handled at batch level
            }
            EXPR_LOAD_COL_INT => {
                let ci = a1 as usize;
                null[dst] = is_col_null(null_word, ci, pki);
                regs[dst] = read_col_int(batch, row, ci, pki);
            }
            EXPR_LOAD_COL_FLOAT => {
                let ci = a1 as usize;
                null[dst] = is_col_null(null_word, ci, pki);
                regs[dst] = read_col_float(batch, row, ci, pki);
            }
            EXPR_LOAD_CONST => {
                null[dst] = false;
                regs[dst] = (a2 << 32) | (a1 & 0xFFFF_FFFF);
            }
            EXPR_INT_ADD => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = regs[a1 as usize].wrapping_add(regs[a2 as usize]);
            }
            EXPR_INT_SUB => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = regs[a1 as usize].wrapping_sub(regs[a2 as usize]);
            }
            EXPR_INT_MUL => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = regs[a1 as usize].wrapping_mul(regs[a2 as usize]);
            }
            EXPR_INT_DIV => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null[dst] = true;
                    regs[dst] = 0;
                } else {
                    null[dst] = null[a1 as usize] || null[a2 as usize];
                    regs[dst] = regs[a1 as usize].wrapping_div(d);
                }
            }
            EXPR_INT_MOD => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null[dst] = true;
                    regs[dst] = 0;
                } else {
                    null[dst] = null[a1 as usize] || null[a2 as usize];
                    regs[dst] = regs[a1 as usize].wrapping_rem(d);
                }
            }
            EXPR_INT_NEG => {
                null[dst] = null[a1 as usize];
                regs[dst] = regs[a1 as usize].wrapping_neg();
            }
            EXPR_FLOAT_ADD => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) + bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_SUB => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) - bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_MUL => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) * bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_DIV => {
                let rhs = bits_to_float(regs[a2 as usize]);
                if rhs == 0.0 {
                    null[dst] = true;
                    regs[dst] = 0;
                } else {
                    null[dst] = null[a1 as usize] || null[a2 as usize];
                    regs[dst] = float_to_bits(bits_to_float(regs[a1 as usize]) / rhs);
                }
            }
            EXPR_FLOAT_NEG => {
                null[dst] = null[a1 as usize];
                regs[dst] = float_to_bits(-bits_to_float(regs[a1 as usize]));
            }
            EXPR_CMP_EQ => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] == regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_NE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] != regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] > regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] >= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] < regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] <= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_FCMP_EQ => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    == bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_NE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    != bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    > bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    >= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LT => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    < bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LE => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    <= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_AND => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] != 0 && regs[a2 as usize] != 0 {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_OR => {
                null[dst] = null[a1 as usize] || null[a2 as usize];
                regs[dst] = if regs[a1 as usize] != 0 || regs[a2 as usize] != 0 {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_NOT => {
                null[dst] = null[a1 as usize];
                regs[dst] = if regs[a1 as usize] == 0 { 1 } else { 0 };
            }
            EXPR_IS_NULL => {
                null[dst] = false;
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    1
                } else {
                    0
                };
            }
            EXPR_IS_NOT_NULL => {
                null[dst] = false;
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    0
                } else {
                    1
                };
            }
            EXPR_INT_TO_FLOAT => {
                null[dst] = null[a1 as usize];
                regs[dst] = float_to_bits(regs[a1 as usize] as f64);
            }
            EXPR_EMIT => {
                if emit_idx < emit_targets.len() {
                    let et = &emit_targets[emit_idx];
                    let ptr = unsafe { et.base.add(row * et.stride) };
                    if null[a1 as usize] {
                        unsafe {
                            std::ptr::write_unaligned(ptr as *mut i64, 0);
                        }
                        emit_null_mask |= 1u64 << et.payload_col;
                    } else {
                        unsafe {
                            std::ptr::write_unaligned(ptr as *mut i64, regs[a1 as usize]);
                        }
                    }
                    emit_idx += 1;
                }
            }
            EXPR_STR_COL_EQ_CONST => {
                null[dst] = is_col_null(null_word, a1 as usize, pki);
                if !null[dst] {
                    let (s, blob) = get_str_struct(batch, row, a1 as usize, pki);
                    let ci = a2 as usize;
                    regs[dst] = if col_string_equals_const(
                        s,
                        blob,
                        &prog.const_strings[ci],
                        prog.const_prefixes[ci],
                        prog.const_lengths[ci],
                    ) {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LT_CONST => {
                null[dst] = is_col_null(null_word, a1 as usize, pki);
                if !null[dst] {
                    let (s, blob) = get_str_struct(batch, row, a1 as usize, pki);
                    let ci = a2 as usize;
                    let cmp = compare_col_string_vs_const(
                        s,
                        blob,
                        &prog.const_strings[ci],
                        prog.const_prefixes[ci],
                        prog.const_lengths[ci],
                    );
                    regs[dst] = if cmp == std::cmp::Ordering::Less { 1 } else { 0 };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LE_CONST => {
                null[dst] = is_col_null(null_word, a1 as usize, pki);
                if !null[dst] {
                    let (s, blob) = get_str_struct(batch, row, a1 as usize, pki);
                    let ci = a2 as usize;
                    let cmp = compare_col_string_vs_const(
                        s,
                        blob,
                        &prog.const_strings[ci],
                        prog.const_prefixes[ci],
                        prog.const_lengths[ci],
                    );
                    regs[dst] = if cmp != std::cmp::Ordering::Greater {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_EQ_COL => {
                let n_a1 = is_col_null(null_word, a1 as usize, pki);
                let n_a2 = is_col_null(null_word, a2 as usize, pki);
                null[dst] = n_a1 || n_a2;
                if !null[dst] {
                    let (s1, blob1) = get_str_struct(batch, row, a1 as usize, pki);
                    let (s2, blob2) = get_str_struct(batch, row, a2 as usize, pki);
                    regs[dst] = if compare_german_strings(s1, blob1, s2, blob2)
                        == std::cmp::Ordering::Equal
                    {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LT_COL => {
                let n_a1 = is_col_null(null_word, a1 as usize, pki);
                let n_a2 = is_col_null(null_word, a2 as usize, pki);
                null[dst] = n_a1 || n_a2;
                if !null[dst] {
                    let (s1, blob1) = get_str_struct(batch, row, a1 as usize, pki);
                    let (s2, blob2) = get_str_struct(batch, row, a2 as usize, pki);
                    regs[dst] = if compare_german_strings(s1, blob1, s2, blob2)
                        == std::cmp::Ordering::Less
                    {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            EXPR_STR_COL_LE_COL => {
                let n_a1 = is_col_null(null_word, a1 as usize, pki);
                let n_a2 = is_col_null(null_word, a2 as usize, pki);
                null[dst] = n_a1 || n_a2;
                if !null[dst] {
                    let (s1, blob1) = get_str_struct(batch, row, a1 as usize, pki);
                    let (s2, blob2) = get_str_struct(batch, row, a2 as usize, pki);
                    regs[dst] = if compare_german_strings(s1, blob1, s2, blob2)
                        != std::cmp::Ordering::Greater
                    {
                        1
                    } else {
                        0
                    };
                } else {
                    regs[dst] = 0;
                }
            }
            _ => {}
        }
    }

    if prog.num_regs == 0 {
        return (0, true, emit_null_mask);
    }
    (
        regs[prog.result_reg as usize],
        null[prog.result_reg as usize],
        emit_null_mask,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor};
    use crate::memtable::OwnedBatch;

    fn make_schema(num_cols: u32, pk_index: u32, col_types: &[(u8, u8)]) -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        for (i, &(tc, sz)) in col_types.iter().enumerate() {
            columns[i] = SchemaColumn {
                type_code: tc,
                size: sz,
                nullable: if i == pk_index as usize { 0 } else { 1 },
                _pad: 0,
            };
        }
        SchemaDescriptor {
            num_columns: num_cols,
            pk_index,
            columns,
        }
    }

    fn make_int_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, u64, &[i64])],
    ) -> OwnedBatch {
        let npc = schema.num_columns as usize - 1;
        let mut batch = OwnedBatch::empty(npc);
        batch.schema = Some(*schema);
        for &(pk, weight, null_word, cols) in rows {
            batch.pk_lo.extend_from_slice(&pk.to_le_bytes());
            batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            batch.weight.extend_from_slice(&weight.to_le_bytes());
            batch.null_bmp.extend_from_slice(&null_word.to_le_bytes());
            let mut pi = 0;
            for ci in 0..schema.num_columns as usize {
                if ci == schema.pk_index as usize {
                    continue;
                }
                if pi < cols.len() {
                    batch.col_data[pi].extend_from_slice(&cols[pi].to_le_bytes());
                }
                pi += 1;
            }
            batch.count += 1;
        }
        batch
    }

    #[test]
    fn test_int_comparisons() {
        // Schema: 2 columns, col0=PK(U64), col1=I64
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
        let mb = batch.as_mem_batch();

        // r0 = load_col_int(1), r1 = load_const(42), r2 = cmp_eq(r0, r1)
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0, // r0 = col[1]
            EXPR_LOAD_CONST, 1, 42, 0, // r1 = 42
            EXPR_CMP_EQ, 2, 0, 1, // r2 = (r0 == r1)
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);
        assert!(!is_null);

        // Test NE
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 42, 0,
            EXPR_CMP_NE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);

        // Test GT
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 10, 0,
            EXPR_CMP_GT, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);
    }

    #[test]
    fn test_int_arithmetic() {
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
        let mb = batch.as_mem_batch();

        // ADD: 10 + 3 = 13
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_INT_ADD, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 13);

        // DIV by zero → NULL
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_INT_DIV, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);

        // MOD by zero → NULL
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_INT_MOD, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);

        // NEG: -10
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_INT_NEG, 1, 0, 0,
        ];
        let prog = ExprProgram::new(code, 2, 1, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, -10);
    }

    #[test]
    fn test_float_arithmetic_and_comparison() {
        let schema = make_schema(3, 0, &[(8, 8), (10, 8), (10, 8)]);
        // Store floats as i64 bits
        let a_bits = float_to_bits(3.14);
        let b_bits = float_to_bits(2.0);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[a_bits, b_bits])]);
        let mb = batch.as_mem_batch();

        // FLOAT_ADD: 3.14 + 2.0
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 1, 0,
            EXPR_LOAD_COL_FLOAT, 1, 2, 0,
            EXPR_FLOAT_ADD, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        let result = bits_to_float(val);
        assert!((result - 5.14).abs() < 1e-10);

        // FLOAT_DIV by zero → NULL
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0, // zero bits
            EXPR_FLOAT_DIV, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);

        // FCMP_GT: 3.14 > 2.0
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 1, 0,
            EXPR_LOAD_COL_FLOAT, 1, 2, 0,
            EXPR_FCMP_GT, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);
    }

    #[test]
    fn test_null_propagation() {
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        // col1 is null (bit 0 set), col2 is not null
        let batch = make_int_batch(&schema, &[(1, 1, 1, &[0, 5])]);
        let mb = batch.as_mem_batch();

        // ADD with one null operand → null
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_INT_ADD, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);
    }

    #[test]
    fn test_is_null_is_not_null() {
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        // col1 is null (bit 0 set), col2 is not null
        let batch = make_int_batch(&schema, &[(1, 1, 1, &[0, 5])]);
        let mb = batch.as_mem_batch();

        // IS_NULL(col1) → 1 (always non-null result)
        let code = vec![EXPR_IS_NULL, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);
        assert!(!is_null);

        // IS_NOT_NULL(col1) → 0
        let code = vec![EXPR_IS_NOT_NULL, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
        assert!(!is_null);

        // IS_NULL(col2) → 0
        let code = vec![EXPR_IS_NULL, 0, 2, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_boolean_combinators() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[1])]);
        let mb = batch.as_mem_batch();

        // AND(1, 0) → 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_BOOL_AND, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);

        // OR(1, 0) → 1
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_BOOL_OR, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // NOT(1) → 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_BOOL_NOT, 1, 0, 0,
        ];
        let prog = ExprProgram::new(code, 2, 1, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_load_const_encoding() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[0])]);
        let mb = batch.as_mem_batch();

        // Test large constant: 0x00000001_00000002 = (1 << 32) | 2 = 4294967298
        // a1 = lo 32 bits = 2, a2 = hi 32 bits = 1
        let code = vec![EXPR_LOAD_CONST, 0, 2, 1];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(!is_null);
        assert_eq!(val, (1i64 << 32) | 2);

        // Test negative constant: -1 → lo=0xFFFFFFFF, hi=0xFFFFFFFF
        // As i64: a1 = -1 (sign-extended from 32 bits), a2 = -1
        // Reconstruction: (-1 << 32) | (-1 & 0xFFFFFFFF) = 0xFFFFFFFF_FFFFFFFF = -1
        let code = vec![EXPR_LOAD_CONST, 0, -1, -1];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, -1);
    }

    #[test]
    fn test_string_eq_const() {
        let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);
        // Build a batch with a short string "hello"
        let npc = 1;
        let mut batch = OwnedBatch::empty(npc);
        batch.schema = Some(schema);
        batch.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch.weight.extend_from_slice(&1i64.to_le_bytes());
        batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        // German string struct for "hello" (5 bytes, inline)
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&5u32.to_le_bytes()); // length = 5
        gs[4..9].copy_from_slice(b"hello"); // prefix(4) + suffix(1) inline
        batch.col_data[0].extend_from_slice(&gs);
        batch.count = 1;

        let mb = batch.as_mem_batch();

        // STR_COL_EQ_CONST(col1, "hello") → 1
        let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![b"hello".to_vec()]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(!is_null);
        assert_eq!(val, 1);

        // STR_COL_EQ_CONST(col1, "world") → 0
        let code = vec![EXPR_STR_COL_EQ_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![b"world".to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_int_to_float() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
        let mb = batch.as_mem_batch();

        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_INT_TO_FLOAT, 1, 0, 0,
        ];
        let prog = ExprProgram::new(code, 2, 1, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(bits_to_float(val), 42.0);
    }

    #[test]
    fn test_emit_with_targets() {
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 20])]);
        let mb = batch.as_mem_batch();

        // Compute col1 + col2, EMIT to payload col 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_INT_ADD, 2, 0, 1,
            EXPR_EMIT, 0, 2, 0, // emit r2 to payload col 0
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);

        let mut out_buf = [0u8; 8];
        let targets = [EmitTarget {
            base: out_buf.as_mut_ptr(),
            stride: 8,
            payload_col: 0,
        }];
        let (val, is_null, mask) =
            eval_with_emit(&prog, &mb, 0, 0, 0, &targets);
        assert_eq!(val, 30); // 10 + 20
        assert!(!is_null);
        assert_eq!(mask, 0);
        let written = i64::from_le_bytes(out_buf);
        assert_eq!(written, 30);
    }

    #[test]
    fn test_div_by_zero_null_semantics() {
        // Schema: pk(u64), col1(i64), col2(i64)
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        // Row: pk=1, col1=10, col2=3; null_word=0 (no nulls)
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[10, 3])]);
        let mb = batch.as_mem_batch();

        // 1. INT_DIV by literal 0 → NULL
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_INT_DIV, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);

        // 2. INT_MOD by literal 0 → NULL
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_INT_MOD, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);

        // 3. FLOAT_DIV by 0.0 bits → NULL
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_FLOAT_DIV, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(is_null);

        // 4. INT_DIV by non-null non-zero → correct quotient, not null
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_INT_DIV, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert!(!is_null);
        assert_eq!(val, 3); // 10 / 3 = 3

        // 5. INT_DIV where divisor column is null → NULL (null propagation)
        // null_word bit 0 = col1 null; use col2 (bit 1) as divisor with col1 null
        // Schema col indices: col1=payload_idx 0, col2=payload_idx 1
        // Build a row where col2 is null (null_word bit 1 set)
        let batch_null_div = make_int_batch(&schema, &[(1, 1, 2, &[10, 3])]);
        let mb_null_div = batch_null_div.as_mem_batch();
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_INT_DIV, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (_, is_null) = eval_predicate(&prog, &mb_null_div, 0, 0);
        assert!(is_null);

        // 6. EMIT of INT_DIV-by-zero result → emit_null_mask bit set, buffer contains 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 0, 0,
            EXPR_INT_DIV, 2, 0, 1,
            EXPR_EMIT, 0, 2, 0,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let mut out_buf = [0xffu8; 8];
        let targets = [EmitTarget {
            base: out_buf.as_mut_ptr(),
            stride: 8,
            payload_col: 0,
        }];
        let (_, _, mask) = eval_with_emit(&prog, &mb, 0, 0, 0, &targets);
        assert_eq!(mask & 1, 1); // bit 0 set → null
        let written = i64::from_le_bytes(out_buf);
        assert_eq!(written, 0);
    }
}
