//! Expression bytecode interpreter for SQL scalar functions.
//!
//! Evaluates compiled expression programs against columnar batch rows.

use crate::schema::{
    compare_german_strings, german_string_tail, SchemaDescriptor, type_code as tc,
};
use crate::storage::MemBatch;
use crate::util::read_u32_le;

gnitz_wire::cast_consts! { pub i64;
    EXPR_LOAD_COL_INT, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_CONST,
    EXPR_INT_ADD, EXPR_INT_SUB, EXPR_INT_MUL, EXPR_INT_DIV,
    EXPR_INT_MOD, EXPR_INT_NEG,
    EXPR_FLOAT_ADD, EXPR_FLOAT_SUB, EXPR_FLOAT_MUL, EXPR_FLOAT_DIV, EXPR_FLOAT_NEG,
    EXPR_CMP_EQ, EXPR_CMP_NE, EXPR_CMP_GT, EXPR_CMP_GE, EXPR_CMP_LT, EXPR_CMP_LE,
    EXPR_FCMP_EQ, EXPR_FCMP_NE, EXPR_FCMP_GT, EXPR_FCMP_GE, EXPR_FCMP_LT, EXPR_FCMP_LE,
    EXPR_BOOL_AND, EXPR_BOOL_OR, EXPR_BOOL_NOT,
    EXPR_IS_NULL, EXPR_IS_NOT_NULL,
    EXPR_EMIT, EXPR_INT_TO_FLOAT, EXPR_COPY_COL,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LT_CONST, EXPR_STR_COL_LE_CONST,
    EXPR_STR_COL_EQ_COL, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LE_COL,
    EXPR_EMIT_NULL,
    EXPR_LOAD_PAYLOAD_INT, EXPR_LOAD_PAYLOAD_FLOAT, EXPR_LOAD_PK_INT,
}

// ---------------------------------------------------------------------------
// Register file constants and null-mask helpers
// ---------------------------------------------------------------------------

pub const MAX_REGS: usize = 64;

#[inline]
fn set_null_bit(mask: u64, bit: usize, val: bool) -> u64 {
    (mask & !(1u64 << bit)) | ((val as u64) << bit)
}

#[inline]
fn prop_null1(mask: u64, dst: usize, a1: usize) -> u64 {
    let n = (mask >> a1) & 1;
    (mask & !(1u64 << dst)) | (n << dst)
}

#[inline]
fn prop_null2(mask: u64, dst: usize, a1: usize, a2: usize) -> u64 {
    let n = ((mask >> a1) | (mask >> a2)) & 1;
    (mask & !(1u64 << dst)) | (n << dst)
}

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
    /// Byte size of each physical payload column (indexed by payload_col).
    /// Empty in legacy mode — callers that set this via set_payload_col_info
    /// get correct narrow-type (TINYINT/SMALLINT/INT) handling.
    pub payload_col_sizes: Vec<u8>,
    /// Type code of each physical payload column (same indexing as payload_col_sizes).
    pub payload_col_type_codes: Vec<u8>,
}

impl ExprProgram {
    pub fn new(
        code: Vec<i64>,
        num_regs: u32,
        result_reg: u32,
        const_strings: Vec<Vec<u8>>,
    ) -> Self {
        assert_eq!(
            code.len() % 4,
            0,
            "ExprProgram: code length {} is not a multiple of 4",
            code.len()
        );
        let num_instrs = code.len() as u32 / 4;
        assert!(
            num_regs as usize <= MAX_REGS,
            "ExprProgram: num_regs={} exceeds MAX_REGS={}",
            num_regs,
            MAX_REGS
        );
        assert!(
            num_regs == 0 || result_reg < num_regs,
            "ExprProgram: result_reg={} >= num_regs={}",
            result_reg,
            num_regs
        );
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
            payload_col_sizes: Vec::new(),
            payload_col_type_codes: Vec::new(),
        }
    }
}

/// Compute the 4-byte prefix of a string for German String comparison.
///
/// Stored as big-endian so that a single integer comparison (`<`/`>`) is
/// equivalent to lexicographic byte comparison of the first four characters.
/// Both sides (column struct and const) zero-pad unused bytes, so the
/// comparison is correct for any string length without masking.
fn compute_prefix(s: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = s.len().min(4);
    buf[..n].copy_from_slice(&s[..n]);
    u32::from_be_bytes(buf)
}

impl ExprProgram {
    /// Rewrite LOAD_COL_INT/FLOAT bytecode instructions to use physical payload
    /// indices, eliminating the runtime pk_index branch in the interpreter loop.
    ///
    /// - `LOAD_COL_INT` with logical index == pk_index  → `LOAD_PK_INT`
    /// - `LOAD_COL_INT` with logical index != pk_index  → `LOAD_PAYLOAD_INT` with physical index
    /// - `LOAD_COL_FLOAT` (PK is never float)           → `LOAD_PAYLOAD_FLOAT` with physical index
    ///
    /// Must be called once per program, before the first call to `eval_predicate`
    /// or `eval_with_emit`. Called automatically by `Plan::from_predicate` and
    /// `Plan::from_map`.
    pub fn resolve_column_indices(&mut self, pk_index: u32) {
        let pki = pk_index as i64;
        for instr in self.code.chunks_exact_mut(4) {
            let op = instr[0];
            let a1 = instr[2];
            match op {
                EXPR_LOAD_COL_INT => {
                    if a1 == pki {
                        instr[0] = EXPR_LOAD_PK_INT;
                    } else {
                        let phys = if a1 < pki { a1 } else { a1 - 1 };
                        instr[0] = EXPR_LOAD_PAYLOAD_INT;
                        instr[2] = phys;
                    }
                }
                EXPR_LOAD_COL_FLOAT => {
                    let phys = if a1 < pki { a1 } else { a1 - 1 };
                    instr[0] = EXPR_LOAD_PAYLOAD_FLOAT;
                    instr[2] = phys;
                }
                _ => {}
            }
        }
    }

    /// Populate payload column size and type-code tables from a schema.
    /// Must be called before `eval_predicate` / `eval_with_emit` when the
    /// program contains `EXPR_LOAD_PAYLOAD_INT` or `EXPR_LOAD_PAYLOAD_FLOAT`
    /// instructions referencing narrow-type columns (TINYINT / SMALLINT / INT).
    pub fn set_payload_col_info(&mut self, schema: &SchemaDescriptor) {
        self.payload_col_sizes.clear();
        self.payload_col_type_codes.clear();
        for (_, _, col) in schema.payload_columns() {
            self.payload_col_sizes.push(col.size);
            self.payload_col_type_codes.push(col.type_code);
        }
    }
}

// ---------------------------------------------------------------------------
// Float bit-reinterpretation (matches float2longlong / longlong2float)
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
        batch.get_pk(row) as i64
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

/// Read a physical payload column value with correct sign/zero extension for narrow types.
#[inline]
fn read_payload_as_i64(bytes: &[u8], size: usize, type_code: u8) -> i64 {
    if matches!(type_code, tc::I8 | tc::I16 | tc::I32 | tc::I64) {
        crate::schema::read_signed(bytes, size)
    } else {
        let mut buf = [0u8; 8];
        buf[..size].copy_from_slice(bytes);
        i64::from_le_bytes(buf)
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

    // Single big-endian integer comparison of the 4-byte prefix.
    // Both sides zero-pad bytes beyond their actual length, so no masking
    // is needed: short strings naturally sort below longer ones with the
    // same prefix, and the zero padding never causes false equality when
    // lengths differ (the trailing length comparison resolves that).
    let col_pfx = u32::from_be_bytes(struct_bytes[4..8].try_into().unwrap());
    if col_pfx != const_prefix {
        return col_pfx.cmp(&const_prefix);
    }

    if min_len <= 4 {
        return col_len.cmp(&c_len);
    }

    // Bulk suffix comparison — vectorised memcmp via [u8]::cmp.
    let col_tail = german_string_tail(struct_bytes, blob, col_len, min_len);
    match col_tail.cmp(&const_bytes[4..min_len]) {
        std::cmp::Ordering::Equal => col_len.cmp(&c_len),
        ord => ord,
    }
}

/// Check equality of a German String column value against a constant byte string.
fn col_string_equals_const(
    struct_bytes: &[u8],
    blob: &[u8],
    const_bytes: &[u8],
    const_prefix: u32,
    const_len: u32,
) -> bool {
    compare_col_string_vs_const(struct_bytes, blob, const_bytes, const_prefix, const_len)
        == std::cmp::Ordering::Equal
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
    let mut regs: [i64; MAX_REGS] = [0; MAX_REGS];
    let mut null_mask: u64 = 0;

    // chunks_exact(4) lets LLVM prove each access is in-bounds, eliminating
    // the per-element bounds checks that the indexed loop cannot avoid.
    for instr in prog.code.chunks_exact(4) {
        let op  = instr[0];
        let dst = instr[1] as usize;
        let a1  = instr[2];
        let a2  = instr[3];

        match op {
            // Resolved opcodes: emitted by resolve_column_indices.
            // Physical payload index is already in a1 — no pk_index branch.
            EXPR_LOAD_PAYLOAD_INT => {
                let pi = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, (null_word >> pi) & 1 != 0);
                if prog.payload_col_sizes.is_empty() {
                    let ptr = batch.get_col_ptr(row, pi, 8);
                    regs[dst] = i64::from_le_bytes(ptr.try_into().unwrap());
                } else {
                    let col_size = prog.payload_col_sizes[pi] as usize;
                    let col_tc = prog.payload_col_type_codes[pi];
                    let ptr = batch.get_col_ptr(row, pi, col_size);
                    regs[dst] = read_payload_as_i64(ptr, col_size, col_tc);
                }
            }
            EXPR_LOAD_PAYLOAD_FLOAT => {
                let pi = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, (null_word >> pi) & 1 != 0);
                if prog.payload_col_sizes.is_empty() {
                    let ptr = batch.get_col_ptr(row, pi, 8);
                    regs[dst] = i64::from_le_bytes(ptr.try_into().unwrap());
                } else {
                    let col_size = prog.payload_col_sizes[pi] as usize;
                    let ptr = batch.get_col_ptr(row, pi, col_size);
                    regs[dst] = if col_size == 4 {
                        let bits = u32::from_le_bytes(ptr.try_into().unwrap());
                        float_to_bits(f32::from_bits(bits) as f64)
                    } else {
                        i64::from_le_bytes(ptr.try_into().unwrap())
                    };
                }
            }
            EXPR_LOAD_PK_INT => {
                null_mask &= !(1u64 << dst);
                regs[dst] = batch.get_pk(row) as i64;
            }
            // Legacy opcodes: used by unresolved programs (e.g. direct tests).
            EXPR_LOAD_COL_INT => {
                let ci = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, ci, pki));
                regs[dst] = read_col_int(batch, row, ci, pki);
            }
            EXPR_LOAD_COL_FLOAT => {
                let ci = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, ci, pki));
                regs[dst] = read_col_float(batch, row, ci, pki);
            }
            EXPR_LOAD_CONST => {
                null_mask &= !(1u64 << dst);
                // a1 = low 32 bits (sign-extended i64), a2 = high 32 bits.
                regs[dst] = (a2 << 32) | (a1 & 0xFFFF_FFFF);
            }
            EXPR_INT_ADD => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = regs[a1 as usize].wrapping_add(regs[a2 as usize]);
            }
            EXPR_INT_SUB => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = regs[a1 as usize].wrapping_sub(regs[a2 as usize]);
            }
            EXPR_INT_MUL => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = regs[a1 as usize].wrapping_mul(regs[a2 as usize]);
            }
            EXPR_INT_DIV => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null_mask |= 1u64 << dst;
                    regs[dst] = 0;
                } else {
                    null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                    regs[dst] = regs[a1 as usize].wrapping_div(d);
                }
            }
            EXPR_INT_MOD => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null_mask |= 1u64 << dst;
                    regs[dst] = 0;
                } else {
                    null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                    regs[dst] = regs[a1 as usize].wrapping_rem(d);
                }
            }
            EXPR_INT_NEG => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = regs[a1 as usize].wrapping_neg();
            }
            EXPR_FLOAT_ADD => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) + bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_SUB => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) - bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_MUL => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) * bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_DIV => {
                let rhs = bits_to_float(regs[a2 as usize]);
                if rhs == 0.0 {
                    null_mask |= 1u64 << dst;
                    regs[dst] = 0;
                } else {
                    null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                    regs[dst] = float_to_bits(bits_to_float(regs[a1 as usize]) / rhs);
                }
            }
            EXPR_FLOAT_NEG => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = float_to_bits(-bits_to_float(regs[a1 as usize]));
            }
            EXPR_CMP_EQ => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] == regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_NE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] != regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] > regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] >= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] < regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] <= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_FCMP_EQ => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    == bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_NE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    != bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    > bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    >= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    < bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    <= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_AND => {
                // SQL 3VL: FALSE AND anything = FALSE; TRUE AND NULL = NULL.
                let n1 = (null_mask >> (a1 as usize)) & 1 != 0;
                let n2 = (null_mask >> (a2 as usize)) & 1 != 0;
                let v1 = regs[a1 as usize] != 0;
                let v2 = regs[a2 as usize] != 0;
                let definite_false = (!n1 && !v1) || (!n2 && !v2);
                let is_null = !definite_false && (n1 || n2);
                null_mask = set_null_bit(null_mask, dst, is_null);
                regs[dst] = if !is_null && v1 && v2 { 1 } else { 0 };
            }
            EXPR_BOOL_OR => {
                // SQL 3VL: TRUE OR anything = TRUE; FALSE OR NULL = NULL.
                let n1 = (null_mask >> (a1 as usize)) & 1 != 0;
                let n2 = (null_mask >> (a2 as usize)) & 1 != 0;
                let v1 = regs[a1 as usize] != 0;
                let v2 = regs[a2 as usize] != 0;
                let definite_true = (!n1 && v1) || (!n2 && v2);
                let is_null = !definite_true && (n1 || n2);
                null_mask = set_null_bit(null_mask, dst, is_null);
                regs[dst] = if definite_true { 1 } else { 0 };
            }
            EXPR_BOOL_NOT => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = if regs[a1 as usize] == 0 { 1 } else { 0 };
            }
            EXPR_IS_NULL => {
                null_mask &= !(1u64 << dst);
                // a1 is intentionally a logical column index: is_col_null handles
                // the logical→physical mapping internally. Unlike LOAD_COL_INT, these
                // opcodes are NOT remapped by resolve_column_indices.
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    1
                } else {
                    0
                };
            }
            EXPR_IS_NOT_NULL => {
                null_mask &= !(1u64 << dst);
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    0
                } else {
                    1
                };
            }
            EXPR_INT_TO_FLOAT => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = float_to_bits(regs[a1 as usize] as f64);
            }
            EXPR_STR_COL_EQ_CONST => {
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, a1 as usize, pki));
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, a1 as usize, pki));
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, a1 as usize, pki));
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, n_a1 || n_a2);
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, n_a1 || n_a2);
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, n_a1 || n_a2);
                if (null_mask >> dst) & 1 == 0 {
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
        return (0, true);
    }
    (
        regs[prog.result_reg as usize],
        (null_mask >> prog.result_reg) & 1 != 0,
    )
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
    let mut regs: [i64; MAX_REGS] = [0; MAX_REGS];
    let mut null_mask: u64 = 0;
    let mut emit_null_mask: u64 = 0;
    let mut emit_idx: usize = 0;

    for instr in prog.code.chunks_exact(4) {
        let op  = instr[0];
        let dst = instr[1] as usize;
        let a1  = instr[2];
        let a2  = instr[3];

        match op {
            EXPR_COPY_COL | EXPR_EMIT_NULL => {
                // Handled at batch level
            }
            // Resolved opcodes (set by resolve_column_indices):
            EXPR_LOAD_PAYLOAD_INT => {
                let pi = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, (null_word >> pi) & 1 != 0);
                if prog.payload_col_sizes.is_empty() {
                    let ptr = batch.get_col_ptr(row, pi, 8);
                    regs[dst] = i64::from_le_bytes(ptr.try_into().unwrap());
                } else {
                    let col_size = prog.payload_col_sizes[pi] as usize;
                    let col_tc = prog.payload_col_type_codes[pi];
                    let ptr = batch.get_col_ptr(row, pi, col_size);
                    regs[dst] = read_payload_as_i64(ptr, col_size, col_tc);
                }
            }
            EXPR_LOAD_PAYLOAD_FLOAT => {
                let pi = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, (null_word >> pi) & 1 != 0);
                if prog.payload_col_sizes.is_empty() {
                    let ptr = batch.get_col_ptr(row, pi, 8);
                    regs[dst] = i64::from_le_bytes(ptr.try_into().unwrap());
                } else {
                    let col_size = prog.payload_col_sizes[pi] as usize;
                    let ptr = batch.get_col_ptr(row, pi, col_size);
                    regs[dst] = if col_size == 4 {
                        let bits = u32::from_le_bytes(ptr.try_into().unwrap());
                        float_to_bits(f32::from_bits(bits) as f64)
                    } else {
                        i64::from_le_bytes(ptr.try_into().unwrap())
                    };
                }
            }
            EXPR_LOAD_PK_INT => {
                null_mask &= !(1u64 << dst);
                regs[dst] = batch.get_pk(row) as i64;
            }
            // Legacy opcodes:
            EXPR_LOAD_COL_INT => {
                let ci = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, ci, pki));
                regs[dst] = read_col_int(batch, row, ci, pki);
            }
            EXPR_LOAD_COL_FLOAT => {
                let ci = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, ci, pki));
                regs[dst] = read_col_float(batch, row, ci, pki);
            }
            EXPR_LOAD_CONST => {
                null_mask &= !(1u64 << dst);
                // a1 = low 32 bits (sign-extended i64), a2 = high 32 bits.
                regs[dst] = (a2 << 32) | (a1 & 0xFFFF_FFFF);
            }
            EXPR_INT_ADD => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = regs[a1 as usize].wrapping_add(regs[a2 as usize]);
            }
            EXPR_INT_SUB => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = regs[a1 as usize].wrapping_sub(regs[a2 as usize]);
            }
            EXPR_INT_MUL => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = regs[a1 as usize].wrapping_mul(regs[a2 as usize]);
            }
            EXPR_INT_DIV => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null_mask |= 1u64 << dst;
                    regs[dst] = 0;
                } else {
                    null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                    regs[dst] = regs[a1 as usize].wrapping_div(d);
                }
            }
            EXPR_INT_MOD => {
                let d = regs[a2 as usize];
                if d == 0 {
                    null_mask |= 1u64 << dst;
                    regs[dst] = 0;
                } else {
                    null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                    regs[dst] = regs[a1 as usize].wrapping_rem(d);
                }
            }
            EXPR_INT_NEG => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = regs[a1 as usize].wrapping_neg();
            }
            EXPR_FLOAT_ADD => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) + bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_SUB => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) - bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_MUL => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = float_to_bits(
                    bits_to_float(regs[a1 as usize]) * bits_to_float(regs[a2 as usize]),
                );
            }
            EXPR_FLOAT_DIV => {
                let rhs = bits_to_float(regs[a2 as usize]);
                if rhs == 0.0 {
                    null_mask |= 1u64 << dst;
                    regs[dst] = 0;
                } else {
                    null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                    regs[dst] = float_to_bits(bits_to_float(regs[a1 as usize]) / rhs);
                }
            }
            EXPR_FLOAT_NEG => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = float_to_bits(-bits_to_float(regs[a1 as usize]));
            }
            EXPR_CMP_EQ => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] == regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_NE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] != regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] > regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_GE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] >= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] < regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_CMP_LE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if regs[a1 as usize] <= regs[a2 as usize] { 1 } else { 0 };
            }
            EXPR_FCMP_EQ => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    == bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_NE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    != bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    > bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_GE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    >= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LT => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    < bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_FCMP_LE => {
                null_mask = prop_null2(null_mask, dst, a1 as usize, a2 as usize);
                regs[dst] = if bits_to_float(regs[a1 as usize])
                    <= bits_to_float(regs[a2 as usize])
                {
                    1
                } else {
                    0
                };
            }
            EXPR_BOOL_AND => {
                // SQL 3VL: FALSE AND anything = FALSE; TRUE AND NULL = NULL.
                let n1 = (null_mask >> (a1 as usize)) & 1 != 0;
                let n2 = (null_mask >> (a2 as usize)) & 1 != 0;
                let v1 = regs[a1 as usize] != 0;
                let v2 = regs[a2 as usize] != 0;
                let definite_false = (!n1 && !v1) || (!n2 && !v2);
                let is_null = !definite_false && (n1 || n2);
                null_mask = set_null_bit(null_mask, dst, is_null);
                regs[dst] = if !is_null && v1 && v2 { 1 } else { 0 };
            }
            EXPR_BOOL_OR => {
                // SQL 3VL: TRUE OR anything = TRUE; FALSE OR NULL = NULL.
                let n1 = (null_mask >> (a1 as usize)) & 1 != 0;
                let n2 = (null_mask >> (a2 as usize)) & 1 != 0;
                let v1 = regs[a1 as usize] != 0;
                let v2 = regs[a2 as usize] != 0;
                let definite_true = (!n1 && v1) || (!n2 && v2);
                let is_null = !definite_true && (n1 || n2);
                null_mask = set_null_bit(null_mask, dst, is_null);
                regs[dst] = if definite_true { 1 } else { 0 };
            }
            EXPR_BOOL_NOT => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = if regs[a1 as usize] == 0 { 1 } else { 0 };
            }
            EXPR_IS_NULL => {
                null_mask &= !(1u64 << dst);
                // a1 is intentionally a logical column index: is_col_null handles
                // the logical→physical mapping internally. Unlike LOAD_COL_INT, these
                // opcodes are NOT remapped by resolve_column_indices.
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    1
                } else {
                    0
                };
            }
            EXPR_IS_NOT_NULL => {
                null_mask &= !(1u64 << dst);
                regs[dst] = if is_col_null(null_word, a1 as usize, pki) {
                    0
                } else {
                    1
                };
            }
            EXPR_INT_TO_FLOAT => {
                null_mask = prop_null1(null_mask, dst, a1 as usize);
                regs[dst] = float_to_bits(regs[a1 as usize] as f64);
            }
            EXPR_EMIT => {
                // The number of EXPR_EMIT instructions is statically determined
                // by the bytecode. Silent-skip on overflow is worse than a panic
                // (it silently corrupts output). Assert in debug; panic via
                // indexing in release if the invariant is ever broken.
                debug_assert!(
                    emit_idx < emit_targets.len(),
                    "EXPR_EMIT: emit_idx {} >= emit_targets.len() {}",
                    emit_idx, emit_targets.len()
                );
                let et = &emit_targets[emit_idx];
                let ptr = unsafe { et.base.add(row * et.stride) };
                if (null_mask >> a1) & 1 != 0 {
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
            EXPR_STR_COL_EQ_CONST => {
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, a1 as usize, pki));
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, a1 as usize, pki));
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, is_col_null(null_word, a1 as usize, pki));
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, n_a1 || n_a2);
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, n_a1 || n_a2);
                if (null_mask >> dst) & 1 == 0 {
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
                null_mask = set_null_bit(null_mask, dst, n_a1 || n_a2);
                if (null_mask >> dst) & 1 == 0 {
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
        (null_mask >> prog.result_reg) & 1 != 0,
        emit_null_mask,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor};
    use crate::storage::Batch;

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
    ) -> Batch {
        let mut batch = Batch::with_schema(*schema, rows.len().max(1));
        batch.count = 0;
        for &(pk, weight, null_word, cols) in rows {
            batch.extend_pk_lo(&pk.to_le_bytes());
            batch.extend_pk_hi(&0u64.to_le_bytes());
            batch.extend_weight(&weight.to_le_bytes());
            batch.extend_null_bmp(&null_word.to_le_bytes());
            let mut pi = 0;
            for ci in 0..schema.num_columns as usize {
                if ci == schema.pk_index as usize {
                    continue;
                }
                if pi < cols.len() {
                    batch.extend_col(pi, &cols[pi].to_le_bytes());
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
        let mut batch = Batch::with_schema(schema, 1);
        batch.count = 0;
        batch.extend_pk_lo(&1u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        // German string struct for "hello" (5 bytes, inline)
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&5u32.to_le_bytes()); // length = 5
        gs[4..9].copy_from_slice(b"hello"); // prefix(4) + suffix(1) inline
        batch.extend_col(0, &gs);
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

    #[test]
    fn test_cmp_ge_lt_le() {
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[42])]);
        let mb = batch.as_mem_batch();

        // GE: 42 >= 42 → 1
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 42, 0,
            EXPR_CMP_GE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // GE: 42 >= 43 → 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 43, 0,
            EXPR_CMP_GE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);

        // LT: 42 < 43 → 1
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 43, 0,
            EXPR_CMP_LT, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // LT: 42 < 42 → 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 42, 0,
            EXPR_CMP_LT, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);

        // LE: 42 <= 42 → 1
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 42, 0,
            EXPR_CMP_LE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // LE: 42 <= 41 → 0
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_CONST, 1, 41, 0,
            EXPR_CMP_LE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_fcmp_eq_ne_lt_le() {
        let schema = make_schema(3, 0, &[(8, 8), (10, 8), (10, 8)]);
        let a_bits = float_to_bits(3.14);
        let b_bits = float_to_bits(2.0);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[a_bits, b_bits])]);
        let mb = batch.as_mem_batch();

        // FCMP_EQ: 3.14 == 3.14 → 1
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 1, 0,
            EXPR_LOAD_COL_FLOAT, 1, 1, 0,
            EXPR_FCMP_EQ, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // FCMP_EQ: 3.14 == 2.0 → 0
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 1, 0,
            EXPR_LOAD_COL_FLOAT, 1, 2, 0,
            EXPR_FCMP_EQ, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);

        // FCMP_NE: 3.14 != 2.0 → 1
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 1, 0,
            EXPR_LOAD_COL_FLOAT, 1, 2, 0,
            EXPR_FCMP_NE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // FCMP_LT: 2.0 < 3.14 → 1
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 2, 0,
            EXPR_LOAD_COL_FLOAT, 1, 1, 0,
            EXPR_FCMP_LT, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // FCMP_LE: 2.0 <= 2.0 → 1
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 2, 0,
            EXPR_LOAD_COL_FLOAT, 1, 2, 0,
            EXPR_FCMP_LE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // FCMP_GE: 2.0 >= 3.14 → 0
        let code = vec![
            EXPR_LOAD_COL_FLOAT, 0, 2, 0,
            EXPR_LOAD_COL_FLOAT, 1, 1, 0,
            EXPR_FCMP_GE, 2, 0, 1,
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_string_lt_le_const() {
        let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);
        let mut batch = Batch::with_schema(schema, 1);
        batch.count = 0;
        batch.extend_pk_lo(&1u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&5u32.to_le_bytes());
        gs[4..9].copy_from_slice(b"hello");
        batch.extend_col(0, &gs);
        batch.count = 1;
        let mb = batch.as_mem_batch();

        // STR_COL_LT_CONST: "hello" < "world" → 1
        let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![b"world".to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // STR_COL_LT_CONST: "hello" < "hello" → 0
        let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![b"hello".to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);

        // STR_COL_LE_CONST: "hello" <= "hello" → 1
        let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![b"hello".to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // STR_COL_LE_CONST: "hello" <= "hella" → 0
        let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
        let prog = ExprProgram::new(code, 1, 0, vec![b"hella".to_vec()]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_string_col_eq_col() {
        // Schema: pk(U64), str_a(STRING), str_b(STRING)
        let schema = make_schema(3, 0, &[(8, 8), (11, 16), (11, 16)]);
        let mut batch = Batch::with_schema(schema, 2);
        batch.count = 0;

        // Row 0: str_a="abc", str_b="abc" (equal)
        batch.extend_pk_lo(&1u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        let mut gs_a = [0u8; 16];
        gs_a[0..4].copy_from_slice(&3u32.to_le_bytes());
        gs_a[4..7].copy_from_slice(b"abc");
        batch.extend_col(0, &gs_a);
        let mut gs_b = [0u8; 16];
        gs_b[0..4].copy_from_slice(&3u32.to_le_bytes());
        gs_b[4..7].copy_from_slice(b"abc");
        batch.extend_col(1, &gs_b);
        batch.count += 1;

        // Row 1: str_a="abc", str_b="xyz" (not equal)
        batch.extend_pk_lo(&2u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &gs_a);
        let mut gs_c = [0u8; 16];
        gs_c[0..4].copy_from_slice(&3u32.to_le_bytes());
        gs_c[4..7].copy_from_slice(b"xyz");
        batch.extend_col(1, &gs_c);
        batch.count += 1;

        let mb = batch.as_mem_batch();

        // Row 0: col1 == col2 → 1
        let code = vec![EXPR_STR_COL_EQ_COL, 0, 1, 2];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);

        // Row 1: col1 == col2 → 0
        let (val, _) = eval_predicate(&prog, &mb, 1, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_complex_predicate() {
        // Schema: pk(U64), a(I64), b(I64), c(I64)
        let schema = make_schema(4, 0, &[(8, 8), (9, 8), (9, 8), (9, 8)]);
        // Row: a=15, b=50, c=42
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[15, 50, 42])]);
        let mb = batch.as_mem_batch();

        // (a > 10 AND b < 100) OR c == 42
        // r0=col1(a), r1=10, r2=(a>10), r3=col2(b), r4=100, r5=(b<100)
        // r6=(r2 AND r5), r7=col3(c), r8=42, r9=(c==42), r10=(r6 OR r9)
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,   // r0 = a = 15
            EXPR_LOAD_CONST, 1, 10, 0,    // r1 = 10
            EXPR_CMP_GT, 2, 0, 1,         // r2 = (15 > 10) = 1
            EXPR_LOAD_COL_INT, 3, 2, 0,   // r3 = b = 50
            EXPR_LOAD_CONST, 4, 100, 0,   // r4 = 100
            EXPR_CMP_LT, 5, 3, 4,         // r5 = (50 < 100) = 1
            EXPR_BOOL_AND, 6, 2, 5,       // r6 = (1 AND 1) = 1
            EXPR_LOAD_COL_INT, 7, 3, 0,   // r7 = c = 42
            EXPR_LOAD_CONST, 8, 42, 0,    // r8 = 42
            EXPR_CMP_EQ, 9, 7, 8,         // r9 = (42 == 42) = 1
            EXPR_BOOL_OR, 10, 6, 9,       // r10 = (1 OR 1) = 1
        ];
        let prog = ExprProgram::new(code, 11, 10, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 1);
        assert!(!is_null);

        // Test with a=5 (a>10 false), b=50, c=99 (c==42 false) → false
        let batch2 = make_int_batch(&schema, &[(1, 1, 0, &[5, 50, 99])]);
        let mb2 = batch2.as_mem_batch();
        let (val, _) = eval_predicate(&prog, &mb2, 0, 0);
        assert_eq!(val, 0);
    }

    #[test]
    fn test_emit_null_opcode() {
        // Schema: pk(U64), val(I64)
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[10])]);
        let mb = batch.as_mem_batch();

        // EMIT_NULL is handled at batch level, not per-row eval.
        // Verify it doesn't crash and is treated as a no-op in eval_with_emit.
        let code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,  // r0 = col1
            EXPR_EMIT_NULL, 0, 0, 0,     // emit null — batch-level, no-op here
        ];
        let prog = ExprProgram::new(code, 1, 0, vec![]);

        let mut out_buf = [0u8; 8];
        let targets = [EmitTarget {
            base: out_buf.as_mut_ptr(),
            stride: 8,
            payload_col: 0,
        }];
        let (val, _, mask) = eval_with_emit(&prog, &mb, 0, 0, 0, &targets);
        // EMIT_NULL is a no-op at per-row level (batch-level handles it),
        // so emit_null_mask stays 0 and result reg r0 holds the loaded value.
        assert_eq!(mask, 0);
        assert_eq!(val, 10);
    }

    #[test]
    fn test_zero_regs_program() {
        // ExprProgram with num_regs=0 (pure COPY_COL) must not crash.
        let schema = make_schema(2, 0, &[(8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(1, 1, 0, &[100])]);
        let mb = batch.as_mem_batch();

        // One COPY_COL instruction: copy col 1 → payload 0, type I64=9
        let code = vec![EXPR_COPY_COL, 9, 1, 0];
        let prog = ExprProgram::new(code, 0, 0, vec![]);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        // With num_regs=0, result should be (0, true) — sentinel
        assert_eq!(val, 0);
        assert!(is_null);
    }

    // ---------------------------------------------------------------------------
    // Fix 2: resolve_column_indices
    // ---------------------------------------------------------------------------

    #[test]
    fn test_resolve_column_indices_pk_at_col0() {
        // Schema: pk=col0(U64), col1=I64, col2=I64
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(42, 1, 0, &[10, 20])]);
        let mb = batch.as_mem_batch();

        // LOAD_COL_INT of pk column → LOAD_PK_INT
        let code = vec![EXPR_LOAD_COL_INT, 0, 0, 0]; // col 0 = pk
        let mut prog = ExprProgram::new(code, 1, 0, vec![]);
        prog.resolve_column_indices(0);
        assert_eq!(prog.code[0], EXPR_LOAD_PK_INT);
        let (val, is_null) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 42);
        assert!(!is_null);

        // LOAD_COL_INT of col1 (logical 1) → LOAD_PAYLOAD_INT, physical 0
        let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
        let mut prog = ExprProgram::new(code, 1, 0, vec![]);
        prog.resolve_column_indices(0);
        assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
        assert_eq!(prog.code[2], 0); // physical payload index
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 10);

        // LOAD_COL_INT of col2 (logical 2) → LOAD_PAYLOAD_INT, physical 1
        let code = vec![EXPR_LOAD_COL_INT, 0, 2, 0];
        let mut prog = ExprProgram::new(code, 1, 0, vec![]);
        prog.resolve_column_indices(0);
        assert_eq!(prog.code[2], 1);
        let (val, _) = eval_predicate(&prog, &mb, 0, 0);
        assert_eq!(val, 20);
    }

    #[test]
    fn test_resolve_column_indices_pk_at_middle() {
        // Schema: col0=I64, pk=col1(U64), col2=I64
        // Physical payload layout: [col0=payload0, col2=payload1]
        let schema = make_schema(3, 1, &[(9, 8), (8, 8), (9, 8)]);
        let batch = make_int_batch(&schema, &[(99, 1, 0, &[5, 7])]);
        let mb = batch.as_mem_batch();

        // col0 (logical 0, before pk) → physical 0
        let code = vec![EXPR_LOAD_COL_INT, 0, 0, 0];
        let mut prog = ExprProgram::new(code, 1, 0, vec![]);
        prog.resolve_column_indices(1);
        assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
        assert_eq!(prog.code[2], 0);
        let (val, _) = eval_predicate(&prog, &mb, 0, 1);
        assert_eq!(val, 5);

        // col1 (logical 1 = pk) → LOAD_PK_INT
        let code = vec![EXPR_LOAD_COL_INT, 0, 1, 0];
        let mut prog = ExprProgram::new(code, 1, 0, vec![]);
        prog.resolve_column_indices(1);
        assert_eq!(prog.code[0], EXPR_LOAD_PK_INT);
        let (val, _) = eval_predicate(&prog, &mb, 0, 1);
        assert_eq!(val, 99);

        // col2 (logical 2, after pk) → physical 1
        let code = vec![EXPR_LOAD_COL_INT, 0, 2, 0];
        let mut prog = ExprProgram::new(code, 1, 0, vec![]);
        prog.resolve_column_indices(1);
        assert_eq!(prog.code[0], EXPR_LOAD_PAYLOAD_INT);
        assert_eq!(prog.code[2], 1);
        let (val, _) = eval_predicate(&prog, &mb, 0, 1);
        assert_eq!(val, 7);
    }

    // ---------------------------------------------------------------------------
    // Fix 3: German string prefix big-endian ordering
    // ---------------------------------------------------------------------------

    /// Build a 16-byte inline German String struct for use in test batches.
    fn make_german_string(s: &[u8]) -> [u8; 16] {
        assert!(s.len() <= 12, "test helper only handles inline strings (≤ 12 bytes)");
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&(s.len() as u32).to_le_bytes());
        let pfx = s.len().min(4);
        gs[4..4 + pfx].copy_from_slice(&s[..pfx]);
        if s.len() > 4 {
            gs[8..8 + (s.len() - 4)].copy_from_slice(&s[4..]);
        }
        gs
    }

    /// Build a 1-row batch with the given schema, containing `s` as the first payload column.
    fn make_single_string_batch(schema: SchemaDescriptor, s: &[u8]) -> Batch {
        let gs = make_german_string(s);
        let mut batch = Batch::with_schema(schema, 1);
        batch.count = 0;
        batch.extend_pk_lo(&1u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &gs);
        batch.count = 1;
        batch
    }

    #[test]
    fn test_string_prefix_ordering() {
        // Schema: pk=col0(U64), col1=STRING
        let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);

        // (col_string, const_string, expected_lt)
        let cases: &[(&[u8], &[u8], bool)] = &[
            (b"abc",   b"abd",    true),  // differ at prefix byte 2
            (b"abd",   b"abc",    false), // reversed
            (b"a",     b"b",      true),  // single-char, differ at byte 0
            (b"abcd",  b"abce",   true),  // differ at prefix byte 3 (boundary)
            (b"abcde", b"abcdf",  true),  // differ at byte 4 (beyond prefix)
            (b"hello", b"world",  true),  // differ at byte 0
            (b"he",    b"hello",  true),  // col shorter, same available bytes
            (b"hello", b"he",     false), // col longer
            (b"",      b"a",      true),  // empty < non-empty
            (b"a",     b"",       false), // non-empty > empty
            (b"abcd",  b"abcd",   false), // equal 4-byte strings, not lt
            (b"abcde", b"abcde",  false), // equal 5-byte strings, not lt
            // LE-vs-BE regression: "ba" > "ac" because 'b'>'a' at byte 0.
            // With LE integer comparison, from_le("ba")=0x6162 < from_le("ac")=0x6361,
            // which would wrongly return true. BE comparison gives the correct false.
            (b"ba",    b"ac",     false),
            (b"ac",    b"ba",     true),
            (b"ba",    b"ab",     false), // byte 0 same first char but reversed positions
            (b"ab",    b"ba",     true),
        ];

        for &(col_s, const_s, expected_lt) in cases {
            let batch = make_single_string_batch(schema, col_s);
            let mb = batch.as_mem_batch();
            let code = vec![EXPR_STR_COL_LT_CONST, 0, 1, 0];
            let prog = ExprProgram::new(code, 1, 0, vec![const_s.to_vec()]);
            let (val, _) = eval_predicate(&prog, &mb, 0, 0);
            assert_eq!(
                val != 0, expected_lt,
                "STR_COL_LT_CONST: {:?} < {:?} expected {}, got {}",
                col_s, const_s, expected_lt, val != 0
            );
        }
    }

    // SQL three-valued logic (3VL) tests for BOOL_AND and BOOL_OR.
    // SQL requires NULL AND FALSE = FALSE and NULL OR TRUE = TRUE,
    // which is stricter than pure null propagation.
    #[test]
    fn test_bool_and_or_three_valued_logic() {
        // Schema: pk(U64), col1(I64), col2(I64) — both nullable
        let schema = make_schema(3, 0, &[(8, 8), (9, 8), (9, 8)]);
        // null_word bits: bit 0 = col1 null, bit 1 = col2 null
        let batch = make_int_batch(&schema, &[
            (1, 1, 0, &[1, 0]), // row0: T, F
            (2, 1, 0, &[0, 1]), // row1: F, T
            (3, 1, 2, &[1, 0]), // row2: T, NULL
            (4, 1, 2, &[0, 0]), // row3: F, NULL
            (5, 1, 1, &[0, 1]), // row4: NULL, T
            (6, 1, 1, &[0, 0]), // row5: NULL, F
            (7, 1, 3, &[0, 0]), // row6: NULL, NULL
        ]);
        let mb = batch.as_mem_batch();

        let and_code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_BOOL_AND, 2, 0, 1,
        ];
        let and_prog = ExprProgram::new(and_code, 3, 2, vec![]);

        let or_code = vec![
            EXPR_LOAD_COL_INT, 0, 1, 0,
            EXPR_LOAD_COL_INT, 1, 2, 0,
            EXPR_BOOL_OR, 2, 0, 1,
        ];
        let or_prog = ExprProgram::new(or_code, 3, 2, vec![]);

        // AND cases
        let (v, n) = eval_predicate(&and_prog, &mb, 0, 0);
        assert_eq!(v, 0, "T AND F = F"); assert!(!n);
        let (v, n) = eval_predicate(&and_prog, &mb, 1, 0);
        assert_eq!(v, 0, "F AND T = F"); assert!(!n);
        let (_, n) = eval_predicate(&and_prog, &mb, 2, 0);
        assert!(n, "T AND NULL = NULL");
        let (v, n) = eval_predicate(&and_prog, &mb, 3, 0);
        assert_eq!(v, 0, "F AND NULL = F"); assert!(!n, "F AND NULL must not be null (SQL 3VL)");
        let (_, n) = eval_predicate(&and_prog, &mb, 4, 0);
        assert!(n, "NULL AND T = NULL");
        let (v, n) = eval_predicate(&and_prog, &mb, 5, 0);
        assert_eq!(v, 0, "NULL AND F = F"); assert!(!n, "NULL AND F must not be null (SQL 3VL)");
        let (_, n) = eval_predicate(&and_prog, &mb, 6, 0);
        assert!(n, "NULL AND NULL = NULL");

        // OR cases
        let (v, n) = eval_predicate(&or_prog, &mb, 0, 0);
        assert_eq!(v, 1, "T OR F = T"); assert!(!n);
        let (v, n) = eval_predicate(&or_prog, &mb, 1, 0);
        assert_eq!(v, 1, "F OR T = T"); assert!(!n);
        let (v, n) = eval_predicate(&or_prog, &mb, 2, 0);
        assert_eq!(v, 1, "T OR NULL = T"); assert!(!n, "T OR NULL must not be null (SQL 3VL)");
        let (_, n) = eval_predicate(&or_prog, &mb, 3, 0);
        assert!(n, "F OR NULL = NULL");
        let (v, n) = eval_predicate(&or_prog, &mb, 4, 0);
        assert_eq!(v, 1, "NULL OR T = T"); assert!(!n, "NULL OR T must not be null (SQL 3VL)");
        let (_, n) = eval_predicate(&or_prog, &mb, 5, 0);
        assert!(n, "NULL OR F = NULL");
        let (_, n) = eval_predicate(&or_prog, &mb, 6, 0);
        assert!(n, "NULL OR NULL = NULL");
    }

    #[test]
    fn test_string_prefix_le_ordering() {
        // Spot-check LE (≤) to cover the equality boundary
        let schema = make_schema(2, 0, &[(8, 8), (11, 16)]);

        let cases: &[(&[u8], &[u8], bool)] = &[
            (b"abc",  b"abc",  true),  // equal → le
            (b"abc",  b"abd",  true),  // less → le
            (b"abd",  b"abc",  false), // greater → not le
            (b"abcd", b"abcd", true),  // equal 4-byte (prefix boundary)
            (b"he",   b"he",   true),  // equal short
            // LE-vs-BE regression: "ba" > "ac", so "ba" <= "ac" must be false.
            (b"ba",   b"ac",   false),
            (b"ac",   b"ba",   true),
        ];

        for &(col_s, const_s, expected_le) in cases {
            let batch = make_single_string_batch(schema, col_s);
            let mb = batch.as_mem_batch();
            let code = vec![EXPR_STR_COL_LE_CONST, 0, 1, 0];
            let prog = ExprProgram::new(code, 1, 0, vec![const_s.to_vec()]);
            let (val, _) = eval_predicate(&prog, &mb, 0, 0);
            assert_eq!(
                val != 0, expected_le,
                "STR_COL_LE_CONST: {:?} <= {:?} expected {}, got {}",
                col_s, const_s, expected_le, val != 0
            );
        }
    }
}
