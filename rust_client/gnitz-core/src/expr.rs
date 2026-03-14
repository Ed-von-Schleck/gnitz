// Expression opcodes — must match gnitz/dbsp/expr.py
const EXPR_LOAD_COL_INT:   u32 = 1;
const EXPR_LOAD_COL_FLOAT: u32 = 2;
const EXPR_LOAD_CONST:     u32 = 3;
const EXPR_INT_ADD:        u32 = 4;
const EXPR_INT_SUB:        u32 = 5;
const EXPR_CMP_EQ:         u32 = 15;
const EXPR_CMP_NE:         u32 = 16;
const EXPR_CMP_GT:         u32 = 17;
const EXPR_CMP_GE:         u32 = 18;
const EXPR_CMP_LT:         u32 = 19;
const EXPR_CMP_LE:         u32 = 20;
const EXPR_BOOL_AND:       u32 = 27;
const EXPR_BOOL_OR:        u32 = 28;
const EXPR_BOOL_NOT:       u32 = 29;
const EXPR_IS_NULL:        u32 = 30;
const EXPR_IS_NOT_NULL:    u32 = 31;

/// A compiled expression program: a flat list of 4-word instructions
/// (opcode, dst_reg, arg1, arg2) plus metadata for embedding in filter params.
#[derive(Clone, Debug)]
pub struct ExprProgram {
    pub num_regs:   u32,
    pub result_reg: u32,
    pub code:       Vec<u32>,
}

/// Builds an expression bytecode program with automatic register allocation.
pub struct ExprBuilder {
    code:     Vec<u32>,
    next_reg: u32,
}

impl ExprBuilder {
    pub fn new() -> Self {
        ExprBuilder { code: Vec::new(), next_reg: 0 }
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

    /// `value as u32` — lower 32 bits; sufficient for typical filter constants.
    pub fn load_const(&mut self, value: i64) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_CONST, dst, value as u32, 0);
        dst
    }

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

    pub fn build(self, result_reg: u32) -> ExprProgram {
        ExprProgram {
            num_regs:   self.next_reg,
            result_reg,
            code:       self.code,
        }
    }
}
