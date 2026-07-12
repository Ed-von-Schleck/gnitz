use gnitz_wire::{
    EXPR_BOOL_AND, EXPR_BOOL_NOT, EXPR_BOOL_OR, EXPR_CMP_EQ, EXPR_CMP_GE, EXPR_CMP_GT, EXPR_CMP_LE, EXPR_CMP_LT,
    EXPR_CMP_NE, EXPR_COPY_COL, EXPR_EMIT, EXPR_FCMP_EQ, EXPR_FCMP_GE, EXPR_FCMP_GT, EXPR_FCMP_LE, EXPR_FCMP_LT,
    EXPR_FCMP_NE, EXPR_FLOAT_ADD, EXPR_FLOAT_DIV, EXPR_FLOAT_MUL, EXPR_FLOAT_NEG, EXPR_FLOAT_SUB, EXPR_INT_ADD,
    EXPR_INT_DIV, EXPR_INT_MOD, EXPR_INT_MUL, EXPR_INT_NEG, EXPR_INT_SUB, EXPR_INT_TO_FLOAT, EXPR_IS_NOT_NULL,
    EXPR_IS_NULL, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_LOAD_NULL, EXPR_SELECT,
    EXPR_STR_COL_EQ_COL, EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LE_COL, EXPR_STR_COL_LE_CONST, EXPR_STR_COL_LT_COL,
    EXPR_STR_COL_LT_CONST,
};

/// A compiled expression program: a flat list of 4-word instructions
/// (opcode, dst_reg, arg1, arg2) plus metadata for embedding in filter params.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExprProgram {
    pub num_regs: u32,
    pub result_reg: u32,
    pub code: Vec<u32>,
    pub const_strings: Vec<String>,
}

impl ExprProgram {
    /// Serialise to a self-contained byte sequence (magic "EXPR"), suitable for a BLOB column.
    pub fn encode(&self) -> Vec<u8> {
        let strs: Vec<&[u8]> = self.const_strings.iter().map(String::as_bytes).collect();
        gnitz_wire::encode_expr_blob(self.num_regs, self.result_reg, &self.code, &strs)
    }
}

/// Builds an expression bytecode program with automatic register allocation.
pub struct ExprBuilder {
    code: Vec<u32>,
    next_reg: u32,
    const_strings: Vec<String>,
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

    // --- Conditional ---

    pub fn select(&mut self, cond: u32, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_SELECT, dst, cond, gnitz_wire::encode_select_operands(a, b));
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

    pub fn copy_col(&mut self, type_code: u32, src_col_idx: u32, payload_col_idx: u32) {
        self.emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx);
    }

    // --- String constants ---

    pub fn add_const_string(&mut self, s: String) -> u32 {
        let idx = self.const_strings.len() as u32;
        self.const_strings.push(s);
        idx
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
        let expected: Vec<Vec<u8>> = prog.const_strings.iter().map(|s| s.as_bytes().to_vec()).collect();
        assert_eq!(dec.const_strings, expected);
    }
}
