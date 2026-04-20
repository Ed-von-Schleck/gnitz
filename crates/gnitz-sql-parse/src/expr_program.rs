//! Expression bytecode program + fluent builder. Pure data structure —
//! no client coupling — so `gnitz-engine` can link it directly for
//! server-side view compile.
//!
//! This mirrors `gnitz_core::expr` verbatim; the duplication is
//! deliberate for Phase 1 (the client path is left alone). Phase 4/5's
//! client-DDL deletion is the cleanup point.

use gnitz_wire::{
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
};

#[derive(Clone, Debug)]
pub struct ExprProgram {
    pub num_regs:      u32,
    pub result_reg:    u32,
    pub code:          Vec<u32>,
    pub const_strings: Vec<String>,
}

pub struct ExprBuilder {
    code:          Vec<u32>,
    next_reg:      u32,
    const_strings: Vec<String>,
}

impl Default for ExprBuilder {
    fn default() -> Self { Self::new() }
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

    pub fn load_const(&mut self, value: i64) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_CONST, dst, value as u32, (value >> 32) as u32);
        dst
    }

    pub fn add(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_INT_ADD, dst, a, b); dst }
    pub fn sub(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_INT_SUB, dst, a, b); dst }
    pub fn mul(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_INT_MUL, dst, a, b); dst }
    pub fn div(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_INT_DIV, dst, a, b); dst }
    pub fn modulo(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_INT_MOD, dst, a, b); dst }
    pub fn neg_int(&mut self, a: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_INT_NEG, dst, a, 0); dst }

    pub fn float_add(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FLOAT_ADD, dst, a, b); dst }
    pub fn float_sub(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FLOAT_SUB, dst, a, b); dst }
    pub fn float_mul(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FLOAT_MUL, dst, a, b); dst }
    pub fn float_div(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FLOAT_DIV, dst, a, b); dst }
    pub fn float_neg(&mut self, a: u32)          -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FLOAT_NEG, dst, a, 0); dst }

    pub fn cmp_eq(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_CMP_EQ, dst, a, b); dst }
    pub fn cmp_ne(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_CMP_NE, dst, a, b); dst }
    pub fn cmp_gt(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_CMP_GT, dst, a, b); dst }
    pub fn cmp_ge(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_CMP_GE, dst, a, b); dst }
    pub fn cmp_lt(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_CMP_LT, dst, a, b); dst }
    pub fn cmp_le(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_CMP_LE, dst, a, b); dst }

    pub fn fcmp_eq(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FCMP_EQ, dst, a, b); dst }
    pub fn fcmp_ne(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FCMP_NE, dst, a, b); dst }
    pub fn fcmp_gt(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FCMP_GT, dst, a, b); dst }
    pub fn fcmp_ge(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FCMP_GE, dst, a, b); dst }
    pub fn fcmp_lt(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FCMP_LT, dst, a, b); dst }
    pub fn fcmp_le(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_FCMP_LE, dst, a, b); dst }

    pub fn bool_and(&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_BOOL_AND, dst, a, b); dst }
    pub fn bool_or (&mut self, a: u32, b: u32) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_BOOL_OR,  dst, a, b); dst }
    pub fn bool_not(&mut self, a: u32)         -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_BOOL_NOT, dst, a, 0); dst }

    pub fn is_null    (&mut self, col_idx: usize) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_IS_NULL,     dst, col_idx as u32, 0); dst }
    pub fn is_not_null(&mut self, col_idx: usize) -> u32 { let dst = self.alloc_reg(); self.emit(EXPR_IS_NOT_NULL, dst, col_idx as u32, 0); dst }

    pub fn int_to_float(&mut self, src: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_TO_FLOAT, dst, src, 0);
        dst
    }

    pub fn emit_col(&mut self, src_reg: u32, payload_col_idx: u32) {
        self.emit(EXPR_EMIT, 0, src_reg, payload_col_idx);
    }

    pub fn copy_col(&mut self, type_code: u32, src_col_idx: u32, payload_col_idx: u32) {
        self.emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx);
    }

    pub fn emit_null(&mut self, payload_col_idx: u32) {
        self.emit(EXPR_EMIT_NULL, 0, payload_col_idx, 0);
    }

    pub fn add_const_string(&mut self, s: String) -> u32 {
        let idx = self.const_strings.len() as u32;
        self.const_strings.push(s);
        idx
    }

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
