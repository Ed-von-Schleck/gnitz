//! Expression bytecode interpreter for SQL scalar functions.
//!
//! Evaluates compiled expression programs against columnar batch rows.

use crate::schema::{
    german_string_tail, SchemaDescriptor, PAYLOAD_MAPPING_PK_SENTINEL,
};
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
    /// (size, type_code) for each physical payload column. Populated by
    /// `resolve_column_indices`; empty until then.
    pub(in crate::expr) payload_col_info: Vec<(u8, u8)>,
    /// True once `resolve_column_indices` has rewritten every column-bearing
    /// opcode's operand byte into payload-index-or-SENTINEL form. Eval entry
    /// points debug-assert this; a Plan-bypass caller that forgets to resolve
    /// gets a clear assertion rather than silent miscompute.
    pub(in crate::expr) resolved: bool,
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
        // The BOOL_AND/BOOL_OR 3VL paths and null-bit propagation in
        // `eval_batch` operate on `u64` words indexed by register, so the
        // register file is capped at 64.
        assert!(
            num_regs <= 64,
            "ExprProgram: num_regs={num_regs} exceeds the 64-register limit",
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
            payload_col_info: Vec::new(),
            resolved: false,
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
    ///
    /// Must be called after `resolve_column_indices`: column-bearing operands
    /// are read as resolved payload bytes (SENTINEL → PK, never nullable;
    /// otherwise dense payload index 0..N-1).
    pub fn is_strictly_non_nullable(&self, schema: &crate::schema::SchemaDescriptor) -> bool {
        let nullable_payload = |a: usize| -> bool {
            a < schema.num_payload_cols()
                && schema.columns[schema.payload_col_idx(a)].nullable != 0
        };
        for instr in self.code.chunks_exact(4) {
            let op = instr[0];
            let a1 = instr[2] as usize;
            let a2 = instr[3] as usize;
            match op {
                // Division/modulo produce NULL on zero divisor
                EXPR_INT_DIV | EXPR_INT_MOD | EXPR_FLOAT_DIV => return false,
                // IS_NULL / IS_NOT_NULL read null bits from the batch
                EXPR_IS_NULL | EXPR_IS_NOT_NULL => return false,
                // Column reads: null if the underlying column is nullable.
                // STR_COL_*_CONST and one-side of STR_COL_*_COL use a1 as the
                // resolved payload byte; STR_COL_*_COL also uses a2.
                EXPR_LOAD_PAYLOAD_INT | EXPR_LOAD_PAYLOAD_FLOAT
                | EXPR_STR_COL_EQ_CONST | EXPR_STR_COL_LT_CONST | EXPR_STR_COL_LE_CONST
                    if nullable_payload(a1) => return false,
                EXPR_STR_COL_EQ_COL | EXPR_STR_COL_LT_COL | EXPR_STR_COL_LE_COL
                    if nullable_payload(a1) || nullable_payload(a2) => return false,
                _ => {}
            }
        }
        true
    }

    /// True iff every instruction is COPY_COL with source columns 1, 2, 3, … in order.
    /// An identity projection that the compiler can elide.
    pub(crate) fn is_sequential_copy_projection(&self) -> bool {
        if self.code.is_empty() {
            return false;
        }
        self.code
            .chunks_exact(4)
            .enumerate()
            .all(|(i, instr)| instr[0] == EXPR_COPY_COL && instr[2] == (i + 1) as i64)
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

}

/// Classify each output-producing instruction.
pub(crate) enum OutputColKind {
    CopyCol(usize),
    Emit(usize),
    EmitNull,
}

fn is_binary_alu_op(op: i64) -> bool {
    matches!(op,
        EXPR_INT_ADD | EXPR_INT_SUB | EXPR_INT_MUL | EXPR_INT_DIV | EXPR_INT_MOD |
        EXPR_FLOAT_ADD | EXPR_FLOAT_SUB | EXPR_FLOAT_MUL | EXPR_FLOAT_DIV |
        EXPR_CMP_EQ | EXPR_CMP_NE | EXPR_CMP_GT | EXPR_CMP_GE | EXPR_CMP_LT | EXPR_CMP_LE |
        EXPR_FCMP_EQ | EXPR_FCMP_NE | EXPR_FCMP_GT | EXPR_FCMP_GE | EXPR_FCMP_LT | EXPR_FCMP_LE |
        EXPR_BOOL_AND | EXPR_BOOL_OR
    )
}

/// 4-byte prefix of `s` as big-endian u32 for ordered comparison.
/// Short strings zero-pad on the right so that integer `<`/`>` matches
/// lexicographic byte order against any other zero-padded 4-byte prefix.
fn compute_prefix(s: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = s.len().min(4);
    buf[..n].copy_from_slice(&s[..n]);
    u32::from_be_bytes(buf)
}

impl ExprProgram {
    /// Rewrite every column-bearing opcode's operand byte from a logical
    /// column index into either a dense payload index (0..N-1) or the
    /// `PAYLOAD_MAPPING_PK_SENTINEL` byte. After this pass eval handlers
    /// branch on `a1 == SENTINEL` instead of `ci == pki`, and the
    /// pk_index/payload arithmetic disappears from the inner loop.
    ///
    /// Opcode rewrites:
    /// - `LOAD_COL_INT` for PK → `LOAD_PK_INT`; otherwise `LOAD_PAYLOAD_INT`
    /// - `LOAD_COL_FLOAT` → `LOAD_PAYLOAD_FLOAT` (PK is never float)
    /// - `IS_NULL`, `IS_NOT_NULL`, `COPY_COL`, `STR_COL_{EQ,LT,LE}_CONST`:
    ///   a1 ← `payload_mapping_byte(ci)` (opcode unchanged)
    /// - `STR_COL_{EQ,LT,LE}_COL`: a1 AND a2 ← `payload_mapping_byte(ci)`
    ///
    /// Idempotent: a second call is a no-op (gated by `self.resolved`).
    /// Must be called once per program, before the first call to
    /// `eval_predicate` / `eval_with_emit` / `eval_batch`. Called
    /// automatically by `Plan::from_predicate` and `Plan::from_map`.
    pub fn resolve_column_indices(&mut self, schema: &SchemaDescriptor) {
        if self.resolved {
            return;
        }
        let sentinel = PAYLOAD_MAPPING_PK_SENTINEL as i64;
        for instr in self.code.chunks_exact_mut(4) {
            let op = instr[0];
            match op {
                EXPR_LOAD_COL_INT => {
                    let byte = schema.payload_mapping_byte(instr[2] as usize) as i64;
                    if byte == sentinel {
                        instr[0] = EXPR_LOAD_PK_INT;
                    } else {
                        instr[0] = EXPR_LOAD_PAYLOAD_INT;
                        instr[2] = byte;
                    }
                }
                EXPR_LOAD_COL_FLOAT => {
                    let ci = instr[2] as usize;
                    debug_assert!(
                        !schema.is_pk_col(ci),
                        "resolve_column_indices: LOAD_COL_FLOAT references PK column {ci} (PK is never float)",
                    );
                    instr[0] = EXPR_LOAD_PAYLOAD_FLOAT;
                    instr[2] = schema.payload_mapping_byte(ci) as i64;
                }
                EXPR_IS_NULL | EXPR_IS_NOT_NULL | EXPR_COPY_COL
                | EXPR_STR_COL_EQ_CONST | EXPR_STR_COL_LT_CONST | EXPR_STR_COL_LE_CONST => {
                    instr[2] = schema.payload_mapping_byte(instr[2] as usize) as i64;
                }
                EXPR_STR_COL_EQ_COL | EXPR_STR_COL_LT_COL | EXPR_STR_COL_LE_COL => {
                    instr[2] = schema.payload_mapping_byte(instr[2] as usize) as i64;
                    instr[3] = schema.payload_mapping_byte(instr[3] as usize) as i64;
                }
                _ => {}
            }
        }
        self.payload_col_info.clear();
        self.payload_col_info.extend(
            schema.payload_columns().map(|(_, _, col)| (col.size(), col.type_code))
        );
        self.resolved = true;
    }
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
