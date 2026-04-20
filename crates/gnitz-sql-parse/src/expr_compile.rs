//! Compile a `BoundExpr` into expression bytecode.

use gnitz_protocol::{Schema, TypeCode};

use crate::error::GnitzSqlParseError;
use crate::expr_program::ExprBuilder;
use crate::plan_types::{BinOp, BoundExpr, UnaryOp};

fn try_compile_string_cmp(
    left: &BoundExpr, op: &BinOp, right: &BoundExpr,
    schema: &Schema, eb: &mut ExprBuilder,
) -> Result<Option<(u32, bool)>, GnitzSqlParseError> {
    if let (BoundExpr::ColRef(idx), BoundExpr::LitStr(s)) = (left, right) {
        if schema.columns[*idx].type_code == TypeCode::String {
            let const_idx = eb.add_const_string(s.clone());
            let reg = match op {
                BinOp::Eq => eb.str_col_eq_const(*idx, const_idx),
                BinOp::Ne => { let r = eb.str_col_eq_const(*idx, const_idx); eb.bool_not(r) },
                BinOp::Lt => eb.str_col_lt_const(*idx, const_idx),
                BinOp::Le => eb.str_col_le_const(*idx, const_idx),
                BinOp::Gt => { let r = eb.str_col_le_const(*idx, const_idx); eb.bool_not(r) },
                BinOp::Ge => { let r = eb.str_col_lt_const(*idx, const_idx); eb.bool_not(r) },
                _ => return Err(GnitzSqlParseError::Unsupported(
                    format!("operator {:?} not supported for strings", op),
                )),
            };
            return Ok(Some((reg, false)));
        }
    }
    if let (BoundExpr::LitStr(s), BoundExpr::ColRef(idx)) = (left, right) {
        if schema.columns[*idx].type_code == TypeCode::String {
            let const_idx = eb.add_const_string(s.clone());
            let reg = match op {
                BinOp::Eq => eb.str_col_eq_const(*idx, const_idx),
                BinOp::Ne => { let r = eb.str_col_eq_const(*idx, const_idx); eb.bool_not(r) },
                BinOp::Gt => eb.str_col_lt_const(*idx, const_idx),
                BinOp::Ge => eb.str_col_le_const(*idx, const_idx),
                BinOp::Lt => { let r = eb.str_col_le_const(*idx, const_idx); eb.bool_not(r) },
                BinOp::Le => { let r = eb.str_col_lt_const(*idx, const_idx); eb.bool_not(r) },
                _ => return Err(GnitzSqlParseError::Unsupported(
                    format!("operator {:?} not supported for strings", op),
                )),
            };
            return Ok(Some((reg, false)));
        }
    }
    if let (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) = (left, right) {
        if schema.columns[*a].type_code == TypeCode::String
           && schema.columns[*b].type_code == TypeCode::String {
            let reg = match op {
                BinOp::Eq => eb.str_col_eq_col(*a, *b),
                BinOp::Ne => { let r = eb.str_col_eq_col(*a, *b); eb.bool_not(r) },
                BinOp::Lt => eb.str_col_lt_col(*a, *b),
                BinOp::Le => eb.str_col_le_col(*a, *b),
                BinOp::Gt => eb.str_col_lt_col(*b, *a),
                BinOp::Ge => eb.str_col_le_col(*b, *a),
                _ => return Err(GnitzSqlParseError::Unsupported(
                    format!("operator {:?} not supported for strings", op),
                )),
            };
            return Ok(Some((reg, false)));
        }
    }
    Ok(None)
}

/// Compile a `BoundExpr` into opcodes on `eb`. Returns (result_reg, is_float).
pub fn compile_bound_expr(
    expr: &BoundExpr, schema: &Schema, eb: &mut ExprBuilder,
) -> Result<(u32, bool), GnitzSqlParseError> {
    match expr {
        BoundExpr::ColRef(idx) => {
            let tc = schema.columns[*idx].type_code;
            match tc {
                TypeCode::U128 => Err(GnitzSqlParseError::Unsupported(format!(
                    "column {:?} is U128; U128 columns cannot be used in view \
                     expressions (use a primary-key seek or CREATE INDEX instead)",
                    schema.columns[*idx].name,
                ))),
                TypeCode::F32 | TypeCode::F64 => Ok((eb.load_col_float(*idx), true)),
                _ => Ok((eb.load_col_int(*idx), false)),
            }
        }
        BoundExpr::LitInt(v)   => Ok((eb.load_const(*v), false)),
        BoundExpr::LitFloat(v) => Ok((eb.load_const(v.to_bits() as i64), true)),
        BoundExpr::LitStr(_)   => Err(GnitzSqlParseError::Unsupported(
            "string literals not supported in view expressions".into(),
        )),
        BoundExpr::BinOp(left, op, right) => {
            if let Some(result) = try_compile_string_cmp(left, op, right, schema, eb)? {
                return Ok(result);
            }
            let (mut l, l_float) = compile_bound_expr(left, schema, eb)?;
            let (mut r, r_float) = compile_bound_expr(right, schema, eb)?;
            if matches!(op, BinOp::And) { return Ok((eb.bool_and(l, r), false)); }
            if matches!(op, BinOp::Or)  { return Ok((eb.bool_or(l, r),  false)); }
            let is_float = l_float || r_float;
            if is_float && !l_float { l = eb.int_to_float(l); }
            if is_float && !r_float { r = eb.int_to_float(r); }
            match (op, is_float) {
                (BinOp::Add, false) => Ok((eb.add(l, r),       false)),
                (BinOp::Add, true)  => Ok((eb.float_add(l, r), true)),
                (BinOp::Sub, false) => Ok((eb.sub(l, r),       false)),
                (BinOp::Sub, true)  => Ok((eb.float_sub(l, r), true)),
                (BinOp::Mul, false) => Ok((eb.mul(l, r),       false)),
                (BinOp::Mul, true)  => Ok((eb.float_mul(l, r), true)),
                (BinOp::Div, false) => Ok((eb.div(l, r),       false)),
                (BinOp::Div, true)  => Ok((eb.float_div(l, r), true)),
                (BinOp::Mod, false) => Ok((eb.modulo(l, r),    false)),
                (BinOp::Mod, true)  => Err(GnitzSqlParseError::Unsupported(
                    "float modulo not supported".into(),
                )),
                (BinOp::Eq, false) => Ok((eb.cmp_eq(l, r),  false)),
                (BinOp::Eq, true)  => Ok((eb.fcmp_eq(l, r), false)),
                (BinOp::Ne, false) => Ok((eb.cmp_ne(l, r),  false)),
                (BinOp::Ne, true)  => Ok((eb.fcmp_ne(l, r), false)),
                (BinOp::Gt, false) => Ok((eb.cmp_gt(l, r),  false)),
                (BinOp::Gt, true)  => Ok((eb.fcmp_gt(l, r), false)),
                (BinOp::Ge, false) => Ok((eb.cmp_ge(l, r),  false)),
                (BinOp::Ge, true)  => Ok((eb.fcmp_ge(l, r), false)),
                (BinOp::Lt, false) => Ok((eb.cmp_lt(l, r),  false)),
                (BinOp::Lt, true)  => Ok((eb.fcmp_lt(l, r), false)),
                (BinOp::Le, false) => Ok((eb.cmp_le(l, r),  false)),
                (BinOp::Le, true)  => Ok((eb.fcmp_le(l, r), false)),
                (BinOp::And, _) | (BinOp::Or, _) => unreachable!(),
            }
        }
        BoundExpr::UnaryOp(op, inner) => {
            let (a, a_float) = compile_bound_expr(inner, schema, eb)?;
            match op {
                UnaryOp::Neg => if a_float { Ok((eb.float_neg(a), true)) }
                                else       { Ok((eb.neg_int(a),   false)) },
                UnaryOp::Not => Ok((eb.bool_not(a), false)),
            }
        }
        BoundExpr::IsNull(idx)    => Ok((eb.is_null(*idx),     false)),
        BoundExpr::IsNotNull(idx) => Ok((eb.is_not_null(*idx), false)),
        BoundExpr::AggCall { .. } => Err(GnitzSqlParseError::Unsupported(
            "aggregate function not allowed in expression context".into(),
        )),
    }
}
