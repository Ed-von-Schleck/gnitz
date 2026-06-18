//! Expression bytecode opcodes.

pub const EXPR_LOAD_COL_INT: u32 = 1;
pub const EXPR_LOAD_COL_FLOAT: u32 = 2;
pub const EXPR_LOAD_CONST: u32 = 3;
pub const EXPR_INT_ADD: u32 = 4;
pub const EXPR_INT_SUB: u32 = 5;
pub const EXPR_INT_MUL: u32 = 6;
pub const EXPR_INT_DIV: u32 = 7;
pub const EXPR_INT_MOD: u32 = 8;
pub const EXPR_INT_NEG: u32 = 9;
pub const EXPR_FLOAT_ADD: u32 = 10;
pub const EXPR_FLOAT_SUB: u32 = 11;
pub const EXPR_FLOAT_MUL: u32 = 12;
pub const EXPR_FLOAT_DIV: u32 = 13;
pub const EXPR_FLOAT_NEG: u32 = 14;
pub const EXPR_CMP_EQ: u32 = 15;
pub const EXPR_CMP_NE: u32 = 16;
pub const EXPR_CMP_GT: u32 = 17;
pub const EXPR_CMP_GE: u32 = 18;
pub const EXPR_CMP_LT: u32 = 19;
pub const EXPR_CMP_LE: u32 = 20;
pub const EXPR_FCMP_EQ: u32 = 21;
pub const EXPR_FCMP_NE: u32 = 22;
pub const EXPR_FCMP_GT: u32 = 23;
pub const EXPR_FCMP_GE: u32 = 24;
pub const EXPR_FCMP_LT: u32 = 25;
pub const EXPR_FCMP_LE: u32 = 26;
pub const EXPR_BOOL_AND: u32 = 27;
pub const EXPR_BOOL_OR: u32 = 28;
pub const EXPR_BOOL_NOT: u32 = 29;
pub const EXPR_IS_NULL: u32 = 30;
pub const EXPR_IS_NOT_NULL: u32 = 31;
pub const EXPR_EMIT: u32 = 32;
pub const EXPR_INT_TO_FLOAT: u32 = 33;
pub const EXPR_COPY_COL: u32 = 34;
pub const EXPR_STR_COL_EQ_CONST: u32 = 40;
pub const EXPR_STR_COL_LT_CONST: u32 = 41;
pub const EXPR_STR_COL_LE_CONST: u32 = 42;
pub const EXPR_STR_COL_EQ_COL: u32 = 43;
pub const EXPR_STR_COL_LT_COL: u32 = 44;
pub const EXPR_STR_COL_LE_COL: u32 = 45;
pub const EXPR_EMIT_NULL: u32 = 46;
// Resolved-column opcodes: emitted by ExprProgram::resolve_column_indices.
// LOAD_COL_INT/FLOAT use logical (schema) indices; these use physical payload
// indices (pk_index already stripped) so the interpreter inner loop is branch-free.
pub const EXPR_LOAD_PAYLOAD_INT: u32 = 47;
pub const EXPR_LOAD_PAYLOAD_FLOAT: u32 = 48;
// PK-region integer loads. a1 packs (pk_byte_offset << 16) | (col_size << 8)
// | type_code so a single column of a compound OPK PK can be addressed.
pub const EXPR_LOAD_PK_UNSIGNED_INT: u32 = 49;
pub const EXPR_LOAD_PK_SIGNED_INT: u32 = 50;
// Unsigned (U64) comparison/arithmetic/cast forms. The i64 register holds the
// raw u64 bit pattern, so these reinterpret operands as u64 for correctness
// when the value is >= 2^63.
pub const EXPR_UCMP_GT: u32 = 51;
pub const EXPR_UCMP_GE: u32 = 52;
pub const EXPR_UCMP_LT: u32 = 53;
pub const EXPR_UCMP_LE: u32 = 54;
pub const EXPR_UDIV: u32 = 55;
pub const EXPR_UMOD: u32 = 56;
pub const EXPR_UINT_TO_FLOAT: u32 = 57;
