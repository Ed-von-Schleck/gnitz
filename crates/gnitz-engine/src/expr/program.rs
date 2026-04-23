//! Expression bytecode interpreter for SQL scalar functions.
//!
//! Evaluates compiled expression programs against columnar batch rows.

use crate::schema::{
    compare_german_strings, german_string_tail, SchemaDescriptor, type_code as tc,
};
use crate::storage::MemBatch;
use crate::util::read_u32_le;

gnitz_wire::cast_consts! { pub(crate) i64;
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

pub(crate) const MAX_REGS: usize = 64;

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

/// SQL 3VL AND: FALSE AND anything = FALSE; TRUE AND NULL = NULL.
#[inline(always)]
fn eval_bool_and(null_mask: u64, regs: &[i64], dst: usize, a1: usize, a2: usize) -> (u64, i64) {
    let n1 = (null_mask >> a1) & 1 != 0;
    let n2 = (null_mask >> a2) & 1 != 0;
    let v1 = regs[a1] != 0;
    let v2 = regs[a2] != 0;
    let definite_false = (!n1 && !v1) || (!n2 && !v2);
    let is_null = !definite_false && (n1 || n2);
    (set_null_bit(null_mask, dst, is_null), if !is_null && v1 && v2 { 1 } else { 0 })
}

/// SQL 3VL OR: TRUE OR anything = TRUE; FALSE OR NULL = NULL.
#[inline(always)]
fn eval_bool_or(null_mask: u64, regs: &[i64], dst: usize, a1: usize, a2: usize) -> (u64, i64) {
    let n1 = (null_mask >> a1) & 1 != 0;
    let n2 = (null_mask >> a2) & 1 != 0;
    let v1 = regs[a1] != 0;
    let v2 = regs[a2] != 0;
    let definite_true = (!n1 && v1) || (!n2 && v2);
    let is_null = !definite_true && (n1 || n2);
    (set_null_bit(null_mask, dst, is_null), if definite_true { 1 } else { 0 })
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
        let prog = ExprProgram {
            code,
            num_regs,
            result_reg,
            num_instrs,
            const_strings,
            const_prefixes,
            const_lengths,
            payload_col_sizes: Vec::new(),
            payload_col_type_codes: Vec::new(),
        };
        // SSA assert: binary ALU ops must not alias dst with either source.
        // Hard assert (not debug-only): fires at compile time for compiler bugs.
        for instr in prog.code.chunks_exact(4) {
            let (op, dst, a1, a2) = (instr[0], instr[1], instr[2], instr[3]);
            if is_binary_alu_op(op) {
                assert!(
                    dst != a1 && dst != a2,
                    "ExprProgram: register aliasing dst={dst} a1={a1} a2={a2} op={op}",
                );
            }
        }
        prog
    }

    /// Returns true if no opcode in this program can produce a NULL result.
    /// Used by the batch evaluator to skip null-bit tracking entirely.
    pub fn is_strictly_non_nullable(&self, schema: &crate::schema::SchemaDescriptor) -> bool {
        let pki = schema.pk_index as usize;
        for instr in self.code.chunks_exact(4) {
            let op = instr[0];
            let a1 = instr[2] as usize;
            match op {
                // Division/modulo produce NULL on zero divisor
                EXPR_INT_DIV | EXPR_INT_MOD | EXPR_FLOAT_DIV => return false,
                // IS_NULL / IS_NOT_NULL read null bits from the batch
                EXPR_IS_NULL | EXPR_IS_NOT_NULL => return false,
                // Payload load: null if the underlying column is nullable
                EXPR_LOAD_PAYLOAD_INT | EXPR_LOAD_PAYLOAD_FLOAT => {
                    let pi = a1;
                    // Physical payload index pi maps to column index ci
                    let ci = if pi < pki { pi } else { pi + 1 };
                    if ci < schema.num_columns as usize && schema.columns[ci].nullable != 0 {
                        return false;
                    }
                }
                _ => {}
            }
        }
        true
    }

    /// True iff every instruction is COPY_COL with source columns 1, 2, 3, … in order.
    /// An identity projection that the compiler can elide.
    pub(crate) fn is_sequential_copy_projection(&self) -> bool {
        let n = self.code.len();
        if n == 0 || n % 4 != 0 {
            return false;
        }
        let mut expected_src: i64 = 1;
        let mut i = 0;
        while i < n {
            if self.code[i] != EXPR_COPY_COL {
                return false;
            }
            if self.code[i + 2] != expected_src {
                return false;
            }
            expected_src += 1;
            i += 4;
        }
        true
    }

    /// Classify each output-producing instruction in bytecode order.
    pub(crate) fn classify_output_cols(&self) -> Vec<OutputColKind> {
        let mut out_cols = Vec::new();
        let mut emit_count = 0usize;
        for i in 0..self.num_instrs as usize {
            let base = i * 4;
            let op = self.code[base];
            if op == EXPR_COPY_COL {
                let src_col = self.code[base + 2] as usize;
                out_cols.push(OutputColKind::CopyCol(src_col));
            } else if op == EXPR_EMIT {
                out_cols.push(OutputColKind::Emit(emit_count));
                emit_count += 1;
            } else if op == EXPR_EMIT_NULL {
                out_cols.push(OutputColKind::EmitNull);
            }
        }
        out_cols
    }

    /// Evaluate EMIT instructions for a single finalized row.
    /// Returns (emit_buffers, eval_emit_mask): one 8-byte slot per EMIT column.
    pub(crate) fn eval_finalized(
        &self,
        mb: &MemBatch,
        row: usize,
        pk_index: u32,
        null_word: u64,
    ) -> (Vec<Vec<u8>>, u64) {
        let emit_count = (0..self.num_instrs as usize)
            .filter(|&i| self.code[i * 4] == EXPR_EMIT)
            .count();
        let mut emit_bufs: Vec<Vec<u8>> = (0..emit_count)
            .map(|_| vec![0u8; 8])
            .collect();
        let emit_targets: Vec<EmitTarget> = emit_bufs.iter_mut().enumerate()
            .map(|(i, buf)| EmitTarget { base: buf.as_mut_ptr(), stride: 0, payload_col: i })
            .collect();
        let (_, _, mask) = eval_with_emit(self, mb, row, pk_index, null_word, &emit_targets);
        (emit_bufs, mask)
    }
}

/// Classify each output-producing instruction.
pub(crate) enum OutputColKind {
    CopyCol(usize),
    Emit(usize),
    EmitNull,
}

/// Compute the 4-byte prefix of a string for German String comparison.
///
/// Stored as big-endian so that a single integer comparison (`<`/`>`) is
/// equivalent to lexicographic byte comparison of the first four characters.
/// Both sides (column struct and const) zero-pad unused bytes, so the
/// comparison is correct for any string length without masking.
fn is_binary_alu_op(op: i64) -> bool {
    matches!(op,
        EXPR_INT_ADD | EXPR_INT_SUB | EXPR_INT_MUL | EXPR_INT_DIV | EXPR_INT_MOD |
        EXPR_FLOAT_ADD | EXPR_FLOAT_SUB | EXPR_FLOAT_MUL | EXPR_FLOAT_DIV |
        EXPR_CMP_EQ | EXPR_CMP_NE | EXPR_CMP_GT | EXPR_CMP_GE | EXPR_CMP_LT | EXPR_CMP_LE |
        EXPR_FCMP_EQ | EXPR_FCMP_NE | EXPR_FCMP_GT | EXPR_FCMP_GE | EXPR_FCMP_LT | EXPR_FCMP_LE |
        EXPR_BOOL_AND | EXPR_BOOL_OR
    )
}

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
pub(in crate::expr) fn float_to_bits(f: f64) -> i64 {
    i64::from_ne_bytes(f.to_ne_bytes())
}

#[inline]
pub(in crate::expr) fn bits_to_float(bits: i64) -> f64 {
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

/// Load an integer payload column, applying narrow-type sign/zero-extension.
#[inline(always)]
fn load_payload_int(prog: &ExprProgram, batch: &MemBatch, row: usize, pi: usize) -> i64 {
    if prog.payload_col_sizes.is_empty() {
        let ptr = batch.get_col_ptr(row, pi, 8);
        i64::from_le_bytes(ptr.try_into().unwrap())
    } else {
        let col_size = prog.payload_col_sizes[pi] as usize;
        let col_tc = prog.payload_col_type_codes[pi];
        let ptr = batch.get_col_ptr(row, pi, col_size);
        read_payload_as_i64(ptr, col_size, col_tc)
    }
}

/// Load a float payload column, widening f32→f64 for narrow types.
#[inline(always)]
fn load_payload_float(prog: &ExprProgram, batch: &MemBatch, row: usize, pi: usize) -> i64 {
    if prog.payload_col_sizes.is_empty() {
        let ptr = batch.get_col_ptr(row, pi, 8);
        i64::from_le_bytes(ptr.try_into().unwrap())
    } else {
        let col_size = prog.payload_col_sizes[pi] as usize;
        let ptr = batch.get_col_ptr(row, pi, col_size);
        if col_size == 4 {
            let bits = u32::from_le_bytes(ptr.try_into().unwrap());
            float_to_bits(f32::from_bits(bits) as f64)
        } else {
            i64::from_le_bytes(ptr.try_into().unwrap())
        }
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
pub(in crate::expr) fn compare_col_string_vs_const(
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
pub(in crate::expr) fn col_string_equals_const(
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
pub(in crate::expr) fn eval_predicate(
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
                regs[dst] = load_payload_int(prog, batch, row, pi);
            }
            EXPR_LOAD_PAYLOAD_FLOAT => {
                let pi = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, (null_word >> pi) & 1 != 0);
                regs[dst] = load_payload_float(prog, batch, row, pi);
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
                (null_mask, regs[dst]) = eval_bool_and(null_mask, &regs, dst, a1 as usize, a2 as usize);
            }
            EXPR_BOOL_OR => {
                (null_mask, regs[dst]) = eval_bool_or(null_mask, &regs, dst, a1 as usize, a2 as usize);
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
pub(in crate::expr) struct EmitTarget {
    pub base: *mut u8,
    pub stride: usize,
    pub payload_col: usize,
}

/// Evaluate expression with EMIT output against a batch row.
/// Writes EMIT values to pre-allocated column buffers.
/// Returns (result_value, result_is_null, emit_null_mask).
pub(in crate::expr) fn eval_with_emit(
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
                regs[dst] = load_payload_int(prog, batch, row, pi);
            }
            EXPR_LOAD_PAYLOAD_FLOAT => {
                let pi = a1 as usize;
                null_mask = set_null_bit(null_mask, dst, (null_word >> pi) & 1 != 0);
                regs[dst] = load_payload_float(prog, batch, row, pi);
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
                (null_mask, regs[dst]) = eval_bool_and(null_mask, &regs, dst, a1 as usize, a2 as usize);
            }
            EXPR_BOOL_OR => {
                (null_mask, regs[dst]) = eval_bool_or(null_mask, &regs, dst, a1 as usize, a2 as usize);
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
