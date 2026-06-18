mod batch;
mod plan;
mod program;

#[cfg(test)]
mod tests;

// External (pub): appear in compiler.rs/vm.rs pub interfaces
pub use plan::ScalarFuncKind;
pub use program::ExprProgram;

pub(crate) use plan::{FinalizeContext, Plan};
pub(crate) use program::OutputColKind;

// Opcode constants — consumed by hand-written test bytecode in vm.rs,
// ops/reduce.rs, and ops/linear.rs (all #[cfg(test)]). Per-target test
// builds (lib vs bin) reference different subsets, so the import is
// liberally exposed under #[cfg(test)] with an allow for the unreferenced
// names in each build.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use program::{
    EXPR_BOOL_AND, EXPR_BOOL_NOT, EXPR_BOOL_OR, EXPR_CMP_EQ, EXPR_CMP_GE, EXPR_CMP_GT, EXPR_CMP_LE, EXPR_CMP_LT,
    EXPR_CMP_NE, EXPR_COPY_COL, EXPR_EMIT, EXPR_EMIT_NULL, EXPR_FCMP_EQ, EXPR_FCMP_GE, EXPR_FCMP_GT, EXPR_FCMP_LE,
    EXPR_FCMP_LT, EXPR_FCMP_NE, EXPR_FLOAT_ADD, EXPR_FLOAT_DIV, EXPR_FLOAT_MUL, EXPR_FLOAT_NEG, EXPR_FLOAT_SUB,
    EXPR_INT_ADD, EXPR_INT_DIV, EXPR_INT_MOD, EXPR_INT_MUL, EXPR_INT_NEG, EXPR_INT_SUB, EXPR_INT_TO_FLOAT,
    EXPR_IS_NOT_NULL, EXPR_IS_NULL, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_LOAD_PAYLOAD_FLOAT,
    EXPR_LOAD_PAYLOAD_INT, EXPR_LOAD_PK_SIGNED_INT, EXPR_LOAD_PK_UNSIGNED_INT, EXPR_STR_COL_EQ_COL,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LE_COL, EXPR_STR_COL_LE_CONST, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LT_CONST,
    EXPR_UCMP_GE, EXPR_UCMP_GT, EXPR_UCMP_LE, EXPR_UCMP_LT, EXPR_UDIV, EXPR_UINT_TO_FLOAT, EXPR_UMOD,
};
