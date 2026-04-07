// Expression opcodes — must match gnitz/dbsp/expr.py
const EXPR_LOAD_COL_INT:   u32 = 1;
const EXPR_LOAD_COL_FLOAT: u32 = 2;
const EXPR_LOAD_CONST:     u32 = 3;
const EXPR_INT_ADD:        u32 = 4;
const EXPR_INT_SUB:        u32 = 5;
const EXPR_INT_MUL:        u32 = 6;
const EXPR_INT_DIV:        u32 = 7;
const EXPR_INT_MOD:        u32 = 8;
const EXPR_INT_NEG:        u32 = 9;
const EXPR_FLOAT_ADD:      u32 = 10;
const EXPR_FLOAT_SUB:      u32 = 11;
const EXPR_FLOAT_MUL:      u32 = 12;
const EXPR_FLOAT_DIV:      u32 = 13;
const EXPR_FLOAT_NEG:      u32 = 14;
const EXPR_CMP_EQ:         u32 = 15;
const EXPR_CMP_NE:         u32 = 16;
const EXPR_CMP_GT:         u32 = 17;
const EXPR_CMP_GE:         u32 = 18;
const EXPR_CMP_LT:         u32 = 19;
const EXPR_CMP_LE:         u32 = 20;
const EXPR_FCMP_EQ:        u32 = 21;
const EXPR_FCMP_NE:        u32 = 22;
const EXPR_FCMP_GT:        u32 = 23;
const EXPR_FCMP_GE:        u32 = 24;
const EXPR_FCMP_LT:        u32 = 25;
const EXPR_FCMP_LE:        u32 = 26;
const EXPR_BOOL_AND:       u32 = 27;
const EXPR_BOOL_OR:        u32 = 28;
const EXPR_BOOL_NOT:       u32 = 29;
const EXPR_IS_NULL:        u32 = 30;
const EXPR_IS_NOT_NULL:    u32 = 31;

// Output opcodes (Phase 4: computed projections)
const EXPR_EMIT:           u32 = 32;
const EXPR_INT_TO_FLOAT:   u32 = 33;
const EXPR_COPY_COL:       u32 = 34;

// String comparison opcodes — must match gnitz/dbsp/expr.py
const EXPR_STR_COL_EQ_CONST: u32 = 40;
const EXPR_STR_COL_LT_CONST: u32 = 41;
const EXPR_STR_COL_LE_CONST: u32 = 42;
const EXPR_STR_COL_EQ_COL:   u32 = 43;
const EXPR_STR_COL_LT_COL:   u32 = 44;
const EXPR_STR_COL_LE_COL:   u32 = 45;
const EXPR_EMIT_NULL:        u32 = 46;

/// A compiled expression program: a flat list of 4-word instructions
/// (opcode, dst_reg, arg1, arg2) plus metadata for embedding in filter params.
#[derive(Clone, Debug)]
pub struct ExprProgram {
    pub num_regs:     u32,
    pub result_reg:   u32,
    pub code:         Vec<u32>,
    pub const_strings: Vec<String>,
}

/// Builds an expression bytecode program with automatic register allocation.
pub struct ExprBuilder {
    code:          Vec<u32>,
    next_reg:      u32,
    const_strings: Vec<String>,
}

impl ExprBuilder {
    pub fn new() -> Self {
        ExprBuilder { code: Vec::new(), next_reg: 0, const_strings: Vec::new() }
    }

    fn alloc_reg(&mut self) -> u32 {
        let r = self.next_reg;
        self.next_reg += 1;
        r
    }

    fn emit(&mut self, op: u32, dst: u32, a1: u32, a2: u32) {
        self.code.extend_from_slice(&[op, dst, a1, a2]);
    }

    pub fn load_col_int(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_COL_INT, dst, col_idx as u32, 0);
        dst
    }

    pub fn load_col_float(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_COL_FLOAT, dst, col_idx as u32, 0);
        dst
    }

    /// Full i64 constant. Low 32 bits in arg1, high 32 bits in arg2.
    /// Reconstructed as: `(a2 << 32) | (a1 & 0xFFFFFFFF)`.
    pub fn load_const(&mut self, value: i64) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_CONST, dst, value as u32, (value >> 32) as u32);
        dst
    }

    // --- Integer arithmetic ---

    pub fn add(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_ADD, dst, a, b);
        dst
    }

    pub fn sub(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_SUB, dst, a, b);
        dst
    }

    pub fn mul(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_MUL, dst, a, b);
        dst
    }

    pub fn div(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_DIV, dst, a, b);
        dst
    }

    pub fn modulo(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_MOD, dst, a, b);
        dst
    }

    pub fn neg_int(&mut self, a: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_NEG, dst, a, 0);
        dst
    }

    // --- Float arithmetic ---

    pub fn float_add(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_ADD, dst, a, b);
        dst
    }

    pub fn float_sub(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_SUB, dst, a, b);
        dst
    }

    pub fn float_mul(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_MUL, dst, a, b);
        dst
    }

    pub fn float_div(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_DIV, dst, a, b);
        dst
    }

    pub fn float_neg(&mut self, a: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_NEG, dst, a, 0);
        dst
    }

    // --- Integer comparison ---

    pub fn cmp_eq(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_EQ, dst, a, b);
        dst
    }

    pub fn cmp_ne(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_NE, dst, a, b);
        dst
    }

    pub fn cmp_gt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_GT, dst, a, b);
        dst
    }

    pub fn cmp_ge(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_GE, dst, a, b);
        dst
    }

    pub fn cmp_lt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_LT, dst, a, b);
        dst
    }

    pub fn cmp_le(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_LE, dst, a, b);
        dst
    }

    // --- Float comparison ---

    pub fn fcmp_eq(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_EQ, dst, a, b);
        dst
    }

    pub fn fcmp_ne(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_NE, dst, a, b);
        dst
    }

    pub fn fcmp_gt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_GT, dst, a, b);
        dst
    }

    pub fn fcmp_ge(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_GE, dst, a, b);
        dst
    }

    pub fn fcmp_lt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_LT, dst, a, b);
        dst
    }

    pub fn fcmp_le(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_LE, dst, a, b);
        dst
    }

    // --- Boolean ---

    pub fn bool_and(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_BOOL_AND, dst, a, b);
        dst
    }

    pub fn bool_or(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_BOOL_OR, dst, a, b);
        dst
    }

    pub fn bool_not(&mut self, a: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_BOOL_NOT, dst, a, 0);
        dst
    }

    // --- Null checks ---

    pub fn is_null(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_IS_NULL, dst, col_idx as u32, 0);
        dst
    }

    pub fn is_not_null(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_IS_NOT_NULL, dst, col_idx as u32, 0);
        dst
    }

    // --- Type conversion ---

    pub fn int_to_float(&mut self, src: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_TO_FLOAT, dst, src, 0);
        dst
    }

    // --- Output opcodes ---

    pub fn emit_col(&mut self, src_reg: u32, payload_col_idx: u32) {
        self.emit(EXPR_EMIT, 0, src_reg, payload_col_idx);
    }

    pub fn copy_col(&mut self, type_code: u32, src_col_idx: u32, payload_col_idx: u32) {
        self.emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx);
    }

    pub fn emit_null(&mut self, payload_col_idx: u32) {
        self.emit(EXPR_EMIT_NULL, 0, payload_col_idx, 0);
    }

    // --- String constants ---

    pub fn add_const_string(&mut self, s: String) -> u32 {
        let idx = self.const_strings.len() as u32;
        self.const_strings.push(s);
        idx
    }

    // --- String comparisons ---

    pub fn str_col_eq_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_EQ_CONST, dst, col_idx as u32, const_idx);
        dst
    }

    pub fn str_col_lt_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LT_CONST, dst, col_idx as u32, const_idx);
        dst
    }

    pub fn str_col_le_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LE_CONST, dst, col_idx as u32, const_idx);
        dst
    }

    pub fn str_col_eq_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_EQ_COL, dst, col_a as u32, col_b as u32);
        dst
    }

    pub fn str_col_lt_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LT_COL, dst, col_a as u32, col_b as u32);
        dst
    }

    pub fn str_col_le_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LE_COL, dst, col_a as u32, col_b as u32);
        dst
    }

    pub fn build(self, result_reg: u32) -> ExprProgram {
        ExprProgram {
            num_regs:      self.next_reg,
            result_reg,
            code:          self.code,
            const_strings: self.const_strings,
        }
    }
}
