use gnitz_protocol::{Schema, TypeCode};
use gnitz_core::ExprBuilder;
use crate::error::GnitzSqlError;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp};

/// Compile a BoundExpr to ExprBuilder opcodes.
/// Returns `(result_reg, is_float)` where `is_float` indicates the register
/// holds f64 bit-pattern rather than a plain i64.
pub fn compile_bound_expr(
    expr:   &BoundExpr,
    schema: &Schema,
    eb:     &mut ExprBuilder,
) -> Result<(u32, bool), GnitzSqlError> {
    match expr {
        BoundExpr::ColRef(idx) => {
            let tc = schema.columns[*idx].type_code;
            match tc {
                TypeCode::F32 | TypeCode::F64 => Ok((eb.load_col_float(*idx), true)),
                _ => Ok((eb.load_col_int(*idx), false)),
            }
        }
        BoundExpr::LitInt(v) => {
            Ok((eb.load_const(*v), false))
        }
        BoundExpr::LitFloat(v) => {
            Ok((eb.load_const(v.to_bits() as i64), true))
        }
        BoundExpr::LitStr(_) => {
            Err(GnitzSqlError::Unsupported(
                "string literals not supported in view expressions".to_string()
            ))
        }
        BoundExpr::BinOp(left, op, right) => {
            let (mut l, l_float) = compile_bound_expr(left, schema, eb)?;
            let (mut r, r_float) = compile_bound_expr(right, schema, eb)?;

            // Boolean ops never need float cast
            if matches!(op, BinOp::And) {
                return Ok((eb.bool_and(l, r), false));
            }
            if matches!(op, BinOp::Or) {
                return Ok((eb.bool_or(l, r), false));
            }

            let is_float = l_float || r_float;

            // Cast int operand to float if mixed
            if is_float && !l_float {
                l = eb.int_to_float(l);
            }
            if is_float && !r_float {
                r = eb.int_to_float(r);
            }

            match (op, is_float) {
                // Arithmetic
                (BinOp::Add, false) => Ok((eb.add(l, r), false)),
                (BinOp::Add, true)  => Ok((eb.float_add(l, r), true)),
                (BinOp::Sub, false) => Ok((eb.sub(l, r), false)),
                (BinOp::Sub, true)  => Ok((eb.float_sub(l, r), true)),
                (BinOp::Mul, false) => Ok((eb.mul(l, r), false)),
                (BinOp::Mul, true)  => Ok((eb.float_mul(l, r), true)),
                (BinOp::Div, false) => Ok((eb.div(l, r), false)),
                (BinOp::Div, true)  => Ok((eb.float_div(l, r), true)),
                (BinOp::Mod, false) => Ok((eb.modulo(l, r), false)),
                (BinOp::Mod, true)  => Err(GnitzSqlError::Unsupported(
                    "float modulo not supported".to_string()
                )),
                // Comparisons — result is always int (0/1)
                (BinOp::Eq, false) => Ok((eb.cmp_eq(l, r), false)),
                (BinOp::Eq, true)  => Ok((eb.fcmp_eq(l, r), false)),
                (BinOp::Ne, false) => Ok((eb.cmp_ne(l, r), false)),
                (BinOp::Ne, true)  => Ok((eb.fcmp_ne(l, r), false)),
                (BinOp::Gt, false) => Ok((eb.cmp_gt(l, r), false)),
                (BinOp::Gt, true)  => Ok((eb.fcmp_gt(l, r), false)),
                (BinOp::Ge, false) => Ok((eb.cmp_ge(l, r), false)),
                (BinOp::Ge, true)  => Ok((eb.fcmp_ge(l, r), false)),
                (BinOp::Lt, false) => Ok((eb.cmp_lt(l, r), false)),
                (BinOp::Lt, true)  => Ok((eb.fcmp_lt(l, r), false)),
                (BinOp::Le, false) => Ok((eb.cmp_le(l, r), false)),
                (BinOp::Le, true)  => Ok((eb.fcmp_le(l, r), false)),
                // And/Or handled above
                (BinOp::And, _) | (BinOp::Or, _) => unreachable!(),
            }
        }
        BoundExpr::UnaryOp(op, inner) => {
            let (a, a_float) = compile_bound_expr(inner, schema, eb)?;
            match op {
                UnaryOp::Neg => {
                    if a_float {
                        Ok((eb.float_neg(a), true))
                    } else {
                        Ok((eb.neg_int(a), false))
                    }
                }
                UnaryOp::Not => Ok((eb.bool_not(a), false)),
            }
        }
        BoundExpr::IsNull(idx) => {
            Ok((eb.is_null(*idx), false))
        }
        BoundExpr::IsNotNull(idx) => {
            Ok((eb.is_not_null(*idx), false))
        }
    }
}
