use gnitz_protocol::{Schema, TypeCode};
use gnitz_core::ExprBuilder;
use crate::error::GnitzSqlError;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp};

/// Compile a BoundExpr to ExprBuilder opcodes. Returns the result register.
pub fn compile_bound_expr(
    expr:   &BoundExpr,
    schema: &Schema,
    eb:     &mut ExprBuilder,
) -> Result<u32, GnitzSqlError> {
    match expr {
        BoundExpr::ColRef(idx) => {
            let tc = schema.columns[*idx].type_code;
            let reg = match tc {
                TypeCode::F32 | TypeCode::F64 => eb.load_col_float(*idx),
                _ => eb.load_col_int(*idx),
            };
            Ok(reg)
        }
        BoundExpr::LitInt(v) => {
            Ok(eb.load_const(*v))
        }
        BoundExpr::LitFloat(_v) => {
            // ExprBuilder only has load_const (integer). Float literals are stored as i64 bits.
            // Float literals are stored as i64 bit patterns (only integer ExprVM ops exist).
            Ok(eb.load_const(_v.to_bits() as i64))
        }
        BoundExpr::BinOp(left, op, right) => {
            let l = compile_bound_expr(left, schema, eb)?;
            let r = compile_bound_expr(right, schema, eb)?;
            let result = match op {
                BinOp::Add => eb.add(l, r),
                BinOp::Sub => eb.sub(l, r),
                BinOp::Mul => eb.mul(l, r),
                BinOp::Div => eb.div(l, r),
                BinOp::Mod => eb.modulo(l, r),
                BinOp::Eq  => eb.cmp_eq(l, r),
                BinOp::Ne  => eb.cmp_ne(l, r),
                BinOp::Gt  => eb.cmp_gt(l, r),
                BinOp::Ge  => eb.cmp_ge(l, r),
                BinOp::Lt  => eb.cmp_lt(l, r),
                BinOp::Le  => eb.cmp_le(l, r),
                BinOp::And => eb.bool_and(l, r),
                BinOp::Or  => eb.bool_or(l, r),
            };
            Ok(result)
        }
        BoundExpr::UnaryOp(op, inner) => {
            let a = compile_bound_expr(inner, schema, eb)?;
            let result = match op {
                UnaryOp::Neg => eb.neg_int(a),
                UnaryOp::Not => eb.bool_not(a),
            };
            Ok(result)
        }
        BoundExpr::IsNull(idx) => {
            Ok(eb.is_null(*idx))
        }
        BoundExpr::IsNotNull(idx) => {
            Ok(eb.is_not_null(*idx))
        }
    }
}
