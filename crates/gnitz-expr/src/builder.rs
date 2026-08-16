//! [`ExprBuilder`]: the emitter that turns a client-side expression into the
//! wire opcode program, and [`ExprProgram`], the blob it produces.
//!
//! This is one of the two tables over the opcode space; the other is
//! [`LogicalProgram::from_wire`](crate::LogicalProgram::from_wire), which reads
//! the same words back. They sit in one crate so a unit test can hold both — see
//! the drift tests at the bottom of this file, which are the only check that the
//! emitter and the decoder agree on any opcode. Everything reachable from here
//! is infallible: a program's *validity* is decided at the decode boundary, so a
//! caller can build first and be told what is unsupported afterwards.

use gnitz_wire::{
    TrimMode, TypeCode, EXPR_BOOL_AND, EXPR_BOOL_NOT, EXPR_BOOL_OR, EXPR_CMP_EQ, EXPR_CMP_GE, EXPR_CMP_GT, EXPR_CMP_LE,
    EXPR_CMP_LT, EXPR_CMP_NE, EXPR_COPY_COL, EXPR_EMIT, EXPR_FCMP_EQ, EXPR_FCMP_GE, EXPR_FCMP_GT, EXPR_FCMP_LE,
    EXPR_FCMP_LT, EXPR_FCMP_NE, EXPR_FLOAT_ABS, EXPR_FLOAT_ADD, EXPR_FLOAT_CEIL, EXPR_FLOAT_DIV, EXPR_FLOAT_FLOOR,
    EXPR_FLOAT_MAX2, EXPR_FLOAT_MIN2, EXPR_FLOAT_MUL, EXPR_FLOAT_NEG, EXPR_FLOAT_ROUND, EXPR_FLOAT_SUB,
    EXPR_FLOAT_TO_F32, EXPR_FLOAT_TO_INT, EXPR_FLOAT_TO_STR, EXPR_FLOAT_TRUNC, EXPR_INT_ABS, EXPR_INT_ADD,
    EXPR_INT_CAST, EXPR_INT_DIV, EXPR_INT_IN_SET, EXPR_INT_MAX2, EXPR_INT_MIN2, EXPR_INT_MOD, EXPR_INT_MUL,
    EXPR_INT_NEG, EXPR_INT_SUB, EXPR_INT_TO_FLOAT, EXPR_INT_TO_STR, EXPR_IS_NOT_NULL, EXPR_IS_NULL,
    EXPR_LOAD_COL_FLOAT, EXPR_LOAD_COL_INT, EXPR_LOAD_COL_STR, EXPR_LOAD_CONST, EXPR_LOAD_CONST_STR, EXPR_LOAD_NULL,
    EXPR_LOAD_NULL_STR, EXPR_SELECT, EXPR_STR_CMP_EQ, EXPR_STR_CMP_LE, EXPR_STR_CMP_LT, EXPR_STR_COL_EQ_COL,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LE_COL, EXPR_STR_COL_LE_CONST, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LT_CONST,
    EXPR_STR_CONCAT, EXPR_STR_CONCAT_NN, EXPR_STR_ILIKE, EXPR_STR_LEN_BYTES, EXPR_STR_LEN_CHARS, EXPR_STR_LIKE,
    EXPR_STR_LOWER, EXPR_STR_SELECT, EXPR_STR_SUBSTR, EXPR_STR_TO_FLOAT, EXPR_STR_TO_INT, EXPR_STR_TRIM,
    EXPR_STR_UPPER, STR_SUBSTR_NO_LEN,
};

/// A compiled expression program: a flat list of 4-word instructions
/// (opcode, dst_reg, arg1, arg2) plus metadata for embedding in filter params.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExprProgram {
    pub num_regs: u32,
    pub result_reg: u32,
    pub code: Vec<u32>,
    /// The byte-transparent const pool: length-prefixed raw byte strings. Holds
    /// german-string cells (string comparisons) and packed sorted-i64 arrays
    /// (`INT_IN_SET` value pools) alike — each entry is read by the opcode that
    /// indexes it, never as text.
    pub const_strings: Vec<Vec<u8>>,
}

impl ExprProgram {
    /// Serialise to a self-contained byte sequence (magic "EXPR"), suitable for a BLOB column.
    pub fn encode(&self) -> Vec<u8> {
        let strs: Vec<&[u8]> = self.const_strings.iter().map(Vec::as_slice).collect();
        gnitz_wire::encode_expr_blob(self.num_regs, self.result_reg, &self.code, &strs)
    }
}

/// Builds an expression bytecode program with automatic register allocation.
#[derive(Clone)]
pub struct ExprBuilder {
    code: Vec<u32>,
    next_reg: u32,
    const_strings: Vec<Vec<u8>>,
}

impl Default for ExprBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ExprBuilder {
    pub fn new() -> Self {
        ExprBuilder {
            code: Vec::new(),
            next_reg: 0,
            const_strings: Vec::new(),
        }
    }

    fn alloc_reg(&mut self) -> u32 {
        let r = self.next_reg;
        self.next_reg += 1;
        r
    }

    fn emit(&mut self, op: u32, dst: u32, a1: u32, a2: u32) {
        self.code.extend_from_slice(&[op, dst, a1, a2]);
    }

    /// Allocate a destination register, emit a two-operand op into it, return it.
    fn binary_op(&mut self, op: u32, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(op, dst, a, b);
        dst
    }

    /// Allocate a destination register, emit a one-operand op into it, return it.
    /// A unary op is just a binary op whose second operand is unused.
    fn unary_op(&mut self, op: u32, a: u32) -> u32 {
        self.binary_op(op, a, 0)
    }

    pub fn load_col_int(&mut self, col_idx: usize) -> u32 {
        self.unary_op(EXPR_LOAD_COL_INT, col_idx as u32)
    }

    pub fn load_col_float(&mut self, col_idx: usize) -> u32 {
        self.unary_op(EXPR_LOAD_COL_FLOAT, col_idx as u32)
    }

    pub fn load_const(&mut self, value: i64) -> u32 {
        let (a1, a2) = gnitz_wire::encode_load_const(value);
        self.binary_op(EXPR_LOAD_CONST, a1, a2)
    }

    // --- Integer arithmetic ---

    pub fn add(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_ADD, a, b)
    }

    pub fn sub(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_SUB, a, b)
    }

    pub fn mul(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_MUL, a, b)
    }

    pub fn div(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_DIV, a, b)
    }

    pub fn modulo(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_MOD, a, b)
    }

    pub fn neg_int(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_INT_NEG, a)
    }

    // --- Float arithmetic ---

    pub fn float_add(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FLOAT_ADD, a, b)
    }

    pub fn float_sub(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FLOAT_SUB, a, b)
    }

    pub fn float_mul(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FLOAT_MUL, a, b)
    }

    pub fn float_div(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FLOAT_DIV, a, b)
    }

    pub fn float_neg(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_NEG, a)
    }

    // --- Integer comparison ---

    pub fn cmp_eq(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_CMP_EQ, a, b)
    }

    pub fn cmp_ne(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_CMP_NE, a, b)
    }

    pub fn cmp_gt(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_CMP_GT, a, b)
    }

    pub fn cmp_ge(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_CMP_GE, a, b)
    }

    pub fn cmp_lt(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_CMP_LT, a, b)
    }

    pub fn cmp_le(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_CMP_LE, a, b)
    }

    // --- Float comparison ---

    pub fn fcmp_eq(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FCMP_EQ, a, b)
    }

    pub fn fcmp_ne(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FCMP_NE, a, b)
    }

    pub fn fcmp_gt(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FCMP_GT, a, b)
    }

    pub fn fcmp_ge(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FCMP_GE, a, b)
    }

    pub fn fcmp_lt(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FCMP_LT, a, b)
    }

    pub fn fcmp_le(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FCMP_LE, a, b)
    }

    // --- Boolean ---

    pub fn bool_and(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_BOOL_AND, a, b)
    }

    pub fn bool_or(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_BOOL_OR, a, b)
    }

    pub fn bool_not(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_BOOL_NOT, a)
    }

    // --- Null checks ---

    pub fn is_null(&mut self, col_idx: usize) -> u32 {
        self.unary_op(EXPR_IS_NULL, col_idx as u32)
    }

    pub fn is_not_null(&mut self, col_idx: usize) -> u32 {
        self.unary_op(EXPR_IS_NOT_NULL, col_idx as u32)
    }

    // --- Type conversion ---

    pub fn int_to_float(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_INT_TO_FLOAT, src)
    }

    // --- Numeric scalar functions ---

    pub fn int_abs(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_INT_ABS, src)
    }
    pub fn float_abs(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_ABS, src)
    }
    pub fn float_floor(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_FLOOR, src)
    }
    pub fn float_ceil(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_CEIL, src)
    }
    pub fn float_round(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_ROUND, src)
    }
    pub fn float_trunc(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_TRUNC, src)
    }
    pub fn float_to_f32(&mut self, src: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_TO_F32, src)
    }
    pub fn int_cast(&mut self, src: u32, to: TypeCode) -> u32 {
        self.binary_op(EXPR_INT_CAST, src, to as u32)
    }
    pub fn float_to_int(&mut self, src: u32, to: TypeCode) -> u32 {
        self.binary_op(EXPR_FLOAT_TO_INT, src, to as u32)
    }
    pub fn int_max2(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_MAX2, a, b)
    }
    pub fn int_min2(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_INT_MIN2, a, b)
    }
    pub fn float_max2(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FLOAT_MAX2, a, b)
    }
    pub fn float_min2(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_FLOAT_MIN2, a, b)
    }

    // --- Conditional ---

    pub fn select(&mut self, cond: u32, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_SELECT, dst, cond, gnitz_wire::pack_operand_pair(a, b));
        dst
    }

    /// Materialize a NULL value into a fresh register (value 0, null bit set).
    pub fn load_null(&mut self) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_NULL, dst, 0, 0);
        dst
    }

    // --- Output opcodes ---

    pub fn emit_col(&mut self, src_reg: u32, payload_col_idx: u32) {
        self.emit(EXPR_EMIT, 0, src_reg, payload_col_idx);
    }

    /// Copy input column `src_col_idx` verbatim into output payload slot
    /// `payload_col_idx`. No type operand: the engine resolves the source
    /// locator (PK byte window or dense payload slot) and both widths from the
    /// schemas it validates the program against, so a restated type code could
    /// only ever disagree with them.
    pub fn copy_col(&mut self, src_col_idx: u32, payload_col_idx: u32) {
        self.emit(EXPR_COPY_COL, 0, src_col_idx, payload_col_idx);
    }

    // --- Const pool (byte-transparent) ---

    /// Push a raw byte string into the const pool and return its index. The pool
    /// is byte-transparent: german-string cells and packed i64 sets share it,
    /// each interpreted by the opcode that indexes it. Every call pushes a fresh
    /// entry — no cross-call dedup.
    pub fn add_const_bytes(&mut self, bytes: Vec<u8>) -> u32 {
        let idx = self.const_strings.len() as u32;
        self.const_strings.push(bytes);
        idx
    }

    pub fn add_const_string(&mut self, s: String) -> u32 {
        self.add_const_bytes(s.into_bytes())
    }

    /// Push an i64 value pool for `INT_IN_SET`, packed as `N × 8-byte LE`, and
    /// return its const index. Sorting is not a wire contract: the engine's
    /// `resolve` sorts the decoded pool before binary-searching it, because set
    /// membership does not depend on order and trusting the client here would
    /// turn a skewed pool into a wrong answer. Callers still sort (and dedup) to
    /// keep the pool small.
    pub fn add_const_int_set(&mut self, values: &[i64]) -> u32 {
        let mut bytes = Vec::with_capacity(values.len() * 8);
        for v in values {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        self.add_const_bytes(bytes)
    }

    // --- Integer set membership ---

    /// `value_reg IN <set at const_idx>` → a fresh 0/1 boolean register. NULL
    /// input propagates to NULL (the engine copies the operand's null word).
    pub fn int_in_set(&mut self, value_reg: u32, const_idx: u32) -> u32 {
        self.binary_op(EXPR_INT_IN_SET, value_reg, const_idx)
    }

    // --- String comparisons ---

    pub fn str_col_eq_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        self.binary_op(EXPR_STR_COL_EQ_CONST, col_idx as u32, const_idx)
    }

    pub fn str_col_lt_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        self.binary_op(EXPR_STR_COL_LT_CONST, col_idx as u32, const_idx)
    }

    pub fn str_col_le_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        self.binary_op(EXPR_STR_COL_LE_CONST, col_idx as u32, const_idx)
    }

    pub fn str_col_eq_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        self.binary_op(EXPR_STR_COL_EQ_COL, col_a as u32, col_b as u32)
    }

    pub fn str_col_lt_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        self.binary_op(EXPR_STR_COL_LT_COL, col_a as u32, col_b as u32)
    }

    pub fn str_col_le_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        self.binary_op(EXPR_STR_COL_LE_COL, col_a as u32, col_b as u32)
    }

    // --- String registers ---

    pub fn load_col_str(&mut self, col_idx: usize) -> u32 {
        self.unary_op(EXPR_LOAD_COL_STR, col_idx as u32)
    }

    pub fn load_const_str(&mut self, const_idx: u32) -> u32 {
        self.unary_op(EXPR_LOAD_CONST_STR, const_idx)
    }

    /// Materialize a NULL *string* into a fresh register. A string context must
    /// emit this rather than [`Self::load_null`], so the register's class
    /// matches what its consumers read.
    pub fn load_null_str(&mut self) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_NULL_STR, dst, 0, 0);
        dst
    }

    pub fn str_select(&mut self, cond: u32, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_SELECT, dst, cond, gnitz_wire::pack_operand_pair(a, b));
        dst
    }

    pub fn str_cmp_eq(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_STR_CMP_EQ, a, b)
    }
    pub fn str_cmp_lt(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_STR_CMP_LT, a, b)
    }
    pub fn str_cmp_le(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_STR_CMP_LE, a, b)
    }
    pub fn str_len_bytes(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_STR_LEN_BYTES, a)
    }
    pub fn str_len_chars(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_STR_LEN_CHARS, a)
    }
    pub fn str_upper(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_STR_UPPER, a)
    }
    pub fn str_lower(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_STR_LOWER, a)
    }

    /// `len = None` (the no-FOR form) rides the `0xFFFF` sentinel, which no real
    /// register can collide with.
    pub fn str_substr(&mut self, src: u32, start: u32, len: Option<u32>) -> u32 {
        let dst = self.alloc_reg();
        let len_word = len.unwrap_or(STR_SUBSTR_NO_LEN);
        self.emit(
            EXPR_STR_SUBSTR,
            dst,
            src,
            gnitz_wire::pack_operand_pair(start, len_word),
        );
        dst
    }

    /// `set_idx` names the const-pool entry holding the bytes to strip.
    pub fn str_trim(&mut self, src: u32, mode: TrimMode, set_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(
            EXPR_STR_TRIM,
            dst,
            gnitz_wire::pack_operand_pair(src, mode.to_wire()),
            set_idx,
        );
        dst
    }

    /// `src [I]LIKE <pattern at pat_idx>` → a fresh 0/1 boolean register.
    /// `escape = None` disables escaping and rides as byte 0; `pat_idx` names the
    /// const-pool entry holding the raw pattern bytes. `ci` picks ILIKE's
    /// ASCII-case-insensitive opcode.
    pub fn str_like(&mut self, src: u32, escape: Option<u8>, pat_idx: u32, ci: bool) -> u32 {
        let dst = self.alloc_reg();
        let op = if ci { EXPR_STR_ILIKE } else { EXPR_STR_LIKE };
        self.emit(
            op,
            dst,
            gnitz_wire::pack_operand_pair(src, escape.unwrap_or(0) as u32),
            pat_idx,
        );
        dst
    }

    /// SQL `||`: NULL in either operand, NULL out.
    pub fn str_concat(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_STR_CONCAT, a, b)
    }

    /// One `CONCAT(…)` fold step: a NULL `b` contributes the empty string, while
    /// a NULL accumulator still propagates.
    pub fn str_concat_nn(&mut self, a: u32, b: u32) -> u32 {
        self.binary_op(EXPR_STR_CONCAT_NN, a, b)
    }

    pub fn int_to_str(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_INT_TO_STR, a)
    }
    pub fn float_to_str(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_FLOAT_TO_STR, a)
    }
    pub fn str_to_int(&mut self, a: u32, to: TypeCode) -> u32 {
        self.binary_op(EXPR_STR_TO_INT, a, to as u32)
    }
    pub fn str_to_float(&mut self, a: u32) -> u32 {
        self.unary_op(EXPR_STR_TO_FLOAT, a)
    }

    pub fn build(self, result_reg: u32) -> ExprProgram {
        ExprProgram {
            num_regs: self.next_reg,
            result_reg,
            code: self.code,
            const_strings: self.const_strings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CmpOp, ExprValidateErr, FloatUnaryOp, IntUnaryOp, LogicalInstr as LI, LogicalProgram, StrOp};
    use std::collections::BTreeSet;

    /// An emitter method paired with the operator its opcode decodes to. The
    /// tables below drive whole operator families through one loop, so the
    /// per-operator cases read as data rather than as repeated call sites.
    type BinEmit<Op> = (fn(&mut ExprBuilder, u32, u32) -> u32, Op);
    type UnEmit<Op> = (fn(&mut ExprBuilder, u32) -> u32, Op);
    type ColConstEmit = (fn(&mut ExprBuilder, usize, u32) -> u32, StrOp);
    type ColColEmit = (fn(&mut ExprBuilder, usize, usize) -> u32, StrOp);

    /// Pairs each emitter call with the [`LogicalInstr`] `from_wire` must read
    /// back from it, then checks the pairing by decoding what was emitted.
    ///
    /// The emitter here and the decoder in `program.rs` are separately-maintained
    /// tables over the same opcode space. Nothing else compares them, and the
    /// per-opcode question they can silently disagree on is *which operand word
    /// carries the packed pair* — `a1` for TRIM and LIKE, `a2` for SELECT and
    /// SUBSTR. A swap there compiles clean and miscomputes.
    ///
    /// The surface needs more instructions than the 64-register file holds, so
    /// it is checked in chunks: [`Drift::check`] decodes and asserts what has
    /// accumulated, then starts a fresh program. Opcodes seen across all chunks
    /// accumulate in `seen` for the completeness test.
    struct Drift {
        b: ExprBuilder,
        want: Vec<LI>,
        seen: BTreeSet<u32>,
    }

    /// The operand registers a chunk works from. Re-emitted per chunk, since
    /// registers do not survive [`Drift::check`].
    struct Base {
        int: u32,
        flt: u32,
        konst: u32,
        boolean: u32,
        str_a: u32,
        str_b: u32,
    }

    impl Drift {
        fn new() -> Self {
            Drift {
                b: ExprBuilder::new(),
                want: Vec::new(),
                seen: BTreeSet::new(),
            }
        }

        /// Emit one value-producing instruction; `want` receives its allocated
        /// destination register.
        fn one(&mut self, emit: impl FnOnce(&mut ExprBuilder) -> u32, want: impl FnOnce(u16) -> LI) -> u32 {
            let dst = emit(&mut self.b);
            self.want.push(want(dst as u16));
            dst
        }

        /// Emit one instruction that allocates no register (the output opcodes).
        fn void(&mut self, emit: impl FnOnce(&mut ExprBuilder), want: LI) {
            emit(&mut self.b);
            self.want.push(want);
        }

        /// The loads every chunk's operands come from.
        fn base(&mut self) -> Base {
            let int = self.one(|b| b.load_col_int(3), |dst| LI::LoadColInt { dst, col: 3 });
            let flt = self.one(|b| b.load_col_float(4), |dst| LI::LoadColFloat { dst, col: 4 });
            let konst = self.one(
                |b| b.load_const(-1_234_567_890_123),
                |dst| LI::LoadConst {
                    dst,
                    val: -1_234_567_890_123,
                },
            );
            let boolean = self.one(
                |b| b.cmp_gt(int, konst),
                |dst| LI::Cmp {
                    op: CmpOp::Gt,
                    dst,
                    a: int as u16,
                    b: konst as u16,
                },
            );
            let str_a = self.one(|b| b.load_col_str(7), |dst| LI::LoadColStr { dst, col: 7 });
            let str_b = self.one(|b| b.load_col_str(8), |dst| LI::LoadColStr { dst, col: 8 });
            Base {
                int,
                flt,
                konst,
                boolean,
                str_a,
                str_b,
            }
        }

        /// Decode what has accumulated and require every instruction to be what
        /// its emitter claimed, then start a fresh program.
        fn check(&mut self) {
            let prog = std::mem::take(&mut self.b).build(0);
            self.seen.extend(prog.code.chunks_exact(4).map(|q| q[0]));
            let decoded = LogicalProgram::from_wire(&prog.code, prog.num_regs, prog.result_reg, prog.const_strings)
                .expect("the builder must only emit decodable programs");
            assert_eq!(decoded.instrs(), self.want.as_slice());
            self.want.clear();
        }
    }

    /// Drive **every** emitter and require each decoded instruction to be what
    /// the call claimed.
    ///
    /// The programs are deliberately not type-coherent — `from_wire` maps opcodes
    /// and operands without a schema, and `validate`'s type rules are covered
    /// elsewhere. What is under test is only emit ↔ decode agreement.
    fn drive_every_emitter() -> Drift {
        let mut d = Drift::new();

        // ── Chunk 1: arithmetic, comparison, boolean, numeric functions ──────
        let b = d.base();
        let (i, f, k, c) = (b.int, b.flt, b.konst, b.boolean);

        d.one(
            |x| x.add(i, k),
            |dst| LI::IntAdd {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.sub(i, k),
            |dst| LI::IntSub {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.mul(i, k),
            |dst| LI::IntMul {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.div(i, k),
            |dst| LI::IntDiv {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.modulo(i, k),
            |dst| LI::IntMod {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.float_add(f, f),
            |dst| LI::FloatAdd {
                dst,
                a: f as u16,
                b: f as u16,
            },
        );
        d.one(
            |x| x.float_sub(f, f),
            |dst| LI::FloatSub {
                dst,
                a: f as u16,
                b: f as u16,
            },
        );
        d.one(
            |x| x.float_mul(f, f),
            |dst| LI::FloatMul {
                dst,
                a: f as u16,
                b: f as u16,
            },
        );
        d.one(
            |x| x.float_div(f, f),
            |dst| LI::FloatDiv {
                dst,
                a: f as u16,
                b: f as u16,
            },
        );

        let cmps: [BinEmit<CmpOp>; 6] = [
            (ExprBuilder::cmp_eq, CmpOp::Eq),
            (ExprBuilder::cmp_ne, CmpOp::Ne),
            (ExprBuilder::cmp_gt, CmpOp::Gt),
            (ExprBuilder::cmp_ge, CmpOp::Ge),
            (ExprBuilder::cmp_lt, CmpOp::Lt),
            (ExprBuilder::cmp_le, CmpOp::Le),
        ];
        for (emit, op) in cmps {
            d.one(
                |x| emit(x, i, k),
                |dst| LI::Cmp {
                    op,
                    dst,
                    a: i as u16,
                    b: k as u16,
                },
            );
        }
        let fcmps: [BinEmit<CmpOp>; 6] = [
            (ExprBuilder::fcmp_eq, CmpOp::Eq),
            (ExprBuilder::fcmp_ne, CmpOp::Ne),
            (ExprBuilder::fcmp_gt, CmpOp::Gt),
            (ExprBuilder::fcmp_ge, CmpOp::Ge),
            (ExprBuilder::fcmp_lt, CmpOp::Lt),
            (ExprBuilder::fcmp_le, CmpOp::Le),
        ];
        for (emit, op) in fcmps {
            d.one(
                |x| emit(x, f, f),
                |dst| LI::FCmp {
                    op,
                    dst,
                    a: f as u16,
                    b: f as u16,
                },
            );
        }

        d.one(
            |x| x.bool_and(i, k),
            |dst| LI::BoolAnd {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.bool_or(i, k),
            |dst| LI::BoolOr {
                dst,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(|x| x.bool_not(c), |dst| LI::BoolNot { dst, a: c as u16 });
        d.one(|x| x.is_null(5), |dst| LI::IsNull { dst, col: 5 });
        d.one(|x| x.is_not_null(6), |dst| LI::IsNotNull { dst, col: 6 });
        d.one(|x| x.int_to_float(i), |dst| LI::IntToFloat { dst, a: i as u16 });

        let int_unaries: [UnEmit<IntUnaryOp>; 2] = [
            (ExprBuilder::neg_int, IntUnaryOp::Neg),
            (ExprBuilder::int_abs, IntUnaryOp::Abs),
        ];
        for (emit, op) in int_unaries {
            d.one(|x| emit(x, i), |dst| LI::IntUnary { op, dst, a: i as u16 });
        }
        let float_unaries: [UnEmit<FloatUnaryOp>; 6] = [
            (ExprBuilder::float_neg, FloatUnaryOp::Neg),
            (ExprBuilder::float_abs, FloatUnaryOp::Abs),
            (ExprBuilder::float_floor, FloatUnaryOp::Floor),
            (ExprBuilder::float_ceil, FloatUnaryOp::Ceil),
            (ExprBuilder::float_round, FloatUnaryOp::Round),
            (ExprBuilder::float_trunc, FloatUnaryOp::Trunc),
        ];
        for (emit, op) in float_unaries {
            d.one(|x| emit(x, f), |dst| LI::FloatUnary { op, dst, a: f as u16 });
        }
        d.one(|x| x.float_to_f32(f), |dst| LI::FloatToF32 { dst, a: f as u16 });
        d.one(
            |x| x.int_cast(i, TypeCode::I32),
            |dst| LI::IntCast {
                dst,
                a: i as u16,
                tc: TypeCode::I32 as u32,
            },
        );
        d.one(
            |x| x.float_to_int(f, TypeCode::I16),
            |dst| LI::FloatToInt {
                dst,
                a: f as u16,
                tc: TypeCode::I16 as u32,
            },
        );
        for is_max in [true, false] {
            d.one(
                |x| if is_max { x.int_max2(i, k) } else { x.int_min2(i, k) },
                |dst| LI::IntMinMax2 {
                    dst,
                    a: i as u16,
                    b: k as u16,
                    is_max,
                },
            );
            d.one(
                |x| if is_max { x.float_max2(f, f) } else { x.float_min2(f, f) },
                |dst| LI::FloatMinMax2 {
                    dst,
                    a: f as u16,
                    b: f as u16,
                    is_max,
                },
            );
        }
        d.check();

        // ── Chunk 2: the packed-operand opcodes, strings, and the outputs ────
        let b = d.base();
        let (i, f, k, c, s0, s1) = (b.int, b.flt, b.konst, b.boolean, b.str_a, b.str_b);

        // SELECT and STR_SELECT pack `(a, b)` into the **a2** word.
        d.one(
            |x| x.select(c, i, k),
            |dst| LI::Select {
                dst,
                cond: c as u16,
                a: i as u16,
                b: k as u16,
            },
        );
        d.one(
            |x| x.str_select(c, s0, s1),
            |dst| LI::StrSelect {
                dst,
                cond: c as u16,
                a: s0 as u16,
                b: s1 as u16,
            },
        );
        d.one(|x| x.load_null(), |dst| LI::LoadNull { dst });
        d.one(|x| x.load_null_str(), |dst| LI::LoadNullStr { dst });

        // The const pool allocates no register.
        let pat = d.b.add_const_string("a%b".to_string());
        let set = d.b.add_const_int_set(&[1, 2, 3]);
        let trim_set = d.b.add_const_bytes(b" ".to_vec());
        d.one(
            |x| x.load_const_str(pat),
            |dst| LI::LoadConstStr { dst, const_idx: pat },
        );
        d.one(
            |x| x.int_in_set(i, set),
            |dst| LI::IntInSet {
                dst,
                value_reg: i as u16,
                set_idx: set,
            },
        );

        let str_consts: [ColConstEmit; 3] = [
            (ExprBuilder::str_col_eq_const, StrOp::Eq),
            (ExprBuilder::str_col_lt_const, StrOp::Lt),
            (ExprBuilder::str_col_le_const, StrOp::Le),
        ];
        for (emit, op) in str_consts {
            d.one(
                |x| emit(x, 7, pat),
                |dst| LI::StrColConst {
                    op,
                    dst,
                    col: 7,
                    const_idx: pat,
                },
            );
        }
        let str_cols: [ColColEmit; 3] = [
            (ExprBuilder::str_col_eq_col, StrOp::Eq),
            (ExprBuilder::str_col_lt_col, StrOp::Lt),
            (ExprBuilder::str_col_le_col, StrOp::Le),
        ];
        for (emit, op) in str_cols {
            d.one(
                |x| emit(x, 7, 8),
                |dst| LI::StrColCol {
                    op,
                    dst,
                    col_a: 7,
                    col_b: 8,
                },
            );
        }
        let str_cmps: [BinEmit<StrOp>; 3] = [
            (ExprBuilder::str_cmp_eq, StrOp::Eq),
            (ExprBuilder::str_cmp_lt, StrOp::Lt),
            (ExprBuilder::str_cmp_le, StrOp::Le),
        ];
        for (emit, op) in str_cmps {
            d.one(
                |x| emit(x, s0, s1),
                |dst| LI::StrCmp {
                    op,
                    dst,
                    a: s0 as u16,
                    b: s1 as u16,
                },
            );
        }
        for chars in [false, true] {
            d.one(
                |x| {
                    if chars {
                        x.str_len_chars(s0)
                    } else {
                        x.str_len_bytes(s0)
                    }
                },
                |dst| LI::StrLen {
                    dst,
                    a: s0 as u16,
                    chars,
                },
            );
        }
        for upper in [true, false] {
            d.one(
                |x| if upper { x.str_upper(s0) } else { x.str_lower(s0) },
                |dst| LI::StrCase {
                    dst,
                    a: s0 as u16,
                    upper,
                },
            );
        }

        // SUBSTR packs `(start, len)` into a2 — both forms of `len`.
        for len in [Some(k), None] {
            d.one(
                |x| x.str_substr(s0, i, len),
                |dst| LI::StrSubstr {
                    dst,
                    src: s0 as u16,
                    start_reg: i as u16,
                    len_reg: len.map(|r| r as u16),
                },
            );
        }
        // TRIM packs `(src, mode)` into **a1**, `set_idx` in a2 — the mirror of
        // SUBSTR, and the pair most likely to be swapped.
        for mode in [TrimMode::Both, TrimMode::Leading, TrimMode::Trailing] {
            d.one(
                |x| x.str_trim(s0, mode, trim_set),
                |dst| LI::StrTrim {
                    dst,
                    a: s0 as u16,
                    mode: mode.to_wire(),
                    set_idx: trim_set,
                },
            );
        }
        // LIKE packs `(src, escape)` into a1 as well; both case modes.
        for (ci, escape) in [(false, Some(b'!')), (true, None)] {
            d.one(
                |x| x.str_like(s0, escape, pat, ci),
                |dst| LI::StrLike {
                    dst,
                    src: s0 as u16,
                    escape: escape.unwrap_or(0) as u32,
                    pat_idx: pat,
                    ci,
                },
            );
        }
        for skip_null in [false, true] {
            d.one(
                |x| {
                    if skip_null {
                        x.str_concat_nn(s0, s1)
                    } else {
                        x.str_concat(s0, s1)
                    }
                },
                |dst| LI::StrConcat {
                    dst,
                    a: s0 as u16,
                    b: s1 as u16,
                    skip_null,
                },
            );
        }
        d.one(|x| x.int_to_str(i), |dst| LI::IntToStr { dst, a: i as u16 });
        d.one(|x| x.float_to_str(f), |dst| LI::FloatToStr { dst, a: f as u16 });
        d.one(
            |x| x.str_to_int(s0, TypeCode::I64),
            |dst| LI::StrToInt {
                dst,
                a: s0 as u16,
                tc: TypeCode::I64 as u32,
            },
        );
        d.one(|x| x.str_to_float(s0), |dst| LI::StrToFloat { dst, a: s0 as u16 });

        // The output opcodes allocate no destination register.
        d.void(|x| x.emit_col(i, 2), LI::Emit { src: i as u16, out: 2 });
        d.void(|x| x.copy_col(9, 3), LI::CopyCol { src_col: 9, out: 3 });
        d.check();

        d
    }

    /// Emit through `ExprBuilder`, decode through `LogicalProgram::from_wire`,
    /// and require every instruction to be what the emitter claimed.
    #[test]
    fn every_emitter_decodes_to_the_instruction_it_claims() {
        drive_every_emitter();
    }

    /// Every opcode the decoder accepts must be reachable from the builder. An
    /// opcode only one table knows is drift the type system cannot see: the
    /// engine would accept a program no client can produce, or reject one it can.
    #[test]
    fn the_builder_reaches_every_opcode_the_decoder_accepts() {
        let emitted = drive_every_emitter().seen;
        // An opcode is "accepted" iff `from_wire` does not reject it as unknown;
        // every arm maps its operands without inspecting them, so an all-zero
        // instruction probes the opcode table alone.
        let accepted: BTreeSet<u32> = (0..=256u32)
            .filter(|&op| {
                !matches!(
                    LogicalProgram::from_wire(&[op, 0, 0, 0], 1, 0, Vec::new()),
                    Err(ExprValidateErr::UnknownOpcode(_))
                )
            })
            .collect();
        assert_eq!(emitted, accepted, "emitted opcodes vs. opcodes from_wire accepts");
    }

    #[test]
    fn encode_round_trips_through_wire_decoder() {
        let mut b = ExprBuilder::new();
        let c = b.load_const(1_234_567_890_123);
        let col = b.load_col_int(0);
        let cond = b.cmp_gt(col, c);
        let s_idx = b.add_const_string("längre sträng".to_string());
        let _ = b.str_col_eq_const(1, s_idx);
        let _ = b.add_const_string(String::new());
        let sel = b.select(cond, col, c);
        let prog = b.build(sel);

        let blob = prog.encode();
        let dec = gnitz_wire::decode_expr_blob(&blob).unwrap();
        assert_eq!(dec.num_regs, prog.num_regs);
        assert_eq!(dec.result_reg, prog.result_reg);
        assert_eq!(dec.code, prog.code);
        // Both sides are now `Vec<Vec<u8>>` — compare the byte-transparent pool directly.
        assert_eq!(dec.const_strings, prog.const_strings);
    }
}
