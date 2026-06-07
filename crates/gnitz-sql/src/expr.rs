use gnitz_core::{Schema, TypeCode};
use gnitz_core::ExprBuilder;
use crate::error::GnitzSqlError;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp};

/// Try to compile a string comparison (col vs const, const vs col, col vs col).
/// Returns Some((reg, false)) if this is a string comparison, None otherwise.
fn try_compile_string_cmp(
    left: &BoundExpr, op: &BinOp, right: &BoundExpr,
    schema: &Schema, eb: &mut ExprBuilder,
) -> Result<Option<(u32, bool)>, GnitzSqlError> {
    // ColRef(string) op LitStr(s), or LitStr(s) op ColRef(string).
    // For the literal-on-left form we swap operands and transpose the
    // comparison so a single `col <cmp> 'lit'` dispatch covers both:
    //   'A' > col  ↔  col < 'A'      'A' >= col ↔ col <= 'A'
    //   'A' < col  ↔  col > 'A'      'A' <= col ↔ col >= 'A'
    // Eq/Ne are symmetric. `cmp` (the transposed op) drives only the register
    // dispatch; Unsupported errors still report the original `op`.
    let col_lit = match (left, right) {
        (BoundExpr::ColRef(idx), BoundExpr::LitStr(s)) => Some((*idx, s, *op)),
        (BoundExpr::LitStr(s), BoundExpr::ColRef(idx)) => Some((*idx, s, match op {
            BinOp::Lt => BinOp::Gt,
            BinOp::Gt => BinOp::Lt,
            BinOp::Le => BinOp::Ge,
            BinOp::Ge => BinOp::Le,
            other     => *other,  // Eq/Ne symmetric; rest fall through to Unsupported
        })),
        _ => None,
    };
    if let Some((idx, s, cmp)) = col_lit {
        if schema.columns[idx].type_code == TypeCode::String {
            let const_idx = eb.add_const_string(s.clone());
            let reg = match cmp {
                BinOp::Eq => eb.str_col_eq_const(idx, const_idx),
                BinOp::Ne => { let r = eb.str_col_eq_const(idx, const_idx); eb.bool_not(r) },
                BinOp::Lt => eb.str_col_lt_const(idx, const_idx),
                BinOp::Le => eb.str_col_le_const(idx, const_idx),
                BinOp::Gt => { let r = eb.str_col_le_const(idx, const_idx); eb.bool_not(r) },
                BinOp::Ge => { let r = eb.str_col_lt_const(idx, const_idx); eb.bool_not(r) },
                _ => return Err(GnitzSqlError::Unsupported(
                    format!("operator {:?} not supported for strings", op)
                )),
            };
            return Ok(Some((reg, false)));
        }
    }
    // ColRef(string) op ColRef(string)
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
                _ => return Err(GnitzSqlError::Unsupported(
                    format!("operator {:?} not supported for strings", op)
                )),
            };
            return Ok(Some((reg, false)));
        }
    }
    Ok(None)
}

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
                TypeCode::U128 | TypeCode::UUID => Err(GnitzSqlError::Unsupported(
                    format!(
                        "column {:?} is {}; U128/UUID columns cannot be used in view \
                         expressions (use a primary-key seek or CREATE INDEX instead)",
                        schema.columns[*idx].name,
                        if tc == TypeCode::UUID { "UUID" } else { "U128" },
                    )
                )),
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
            // String comparison detection
            if let Some(result) = try_compile_string_cmp(left, op, right, schema, eb)? {
                return Ok(result);
            }

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
        BoundExpr::AggCall { .. } => {
            Err(GnitzSqlError::Unsupported(
                "aggregate function not allowed in expression context".to_string()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, Schema, ExprProgram};

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef { name: name.into(), type_code: tc, is_nullable: true,
                    fk_table_id: 0, fk_col_idx: 0 }
    }

    /// col 0 = pk (U64), col 1 = s (String), col 2 = t (String).
    fn str_schema() -> Schema {
        Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                col("s",  TypeCode::String),
                col("t",  TypeCode::String),
            ],
            pk_cols: vec![0],
        }
    }

    fn compile(left: &BoundExpr, op: BinOp, right: &BoundExpr, schema: &Schema) -> ExprProgram {
        let mut eb = ExprBuilder::new();
        let (reg, _) = try_compile_string_cmp(left, &op, right, schema, &mut eb)
            .expect("compile ok")
            .expect("recognized as a string comparison");
        eb.build(reg)
    }

    /// `col <op> 'lit'` and the transposed `'lit' <op'> col` must compile to the
    /// byte-identical predicate program for every comparison operator.
    #[test]
    fn string_cmp_col_lit_is_symmetric() {
        let schema = str_schema();
        let s   = BoundExpr::ColRef(1);
        let lit = BoundExpr::LitStr("x".to_string());
        for (col_op, lit_op) in [
            (BinOp::Gt, BinOp::Lt),  // s > 'x'  ≡  'x' < s
            (BinOp::Lt, BinOp::Gt),  // s < 'x'  ≡  'x' > s
            (BinOp::Ge, BinOp::Le),  // s >= 'x' ≡  'x' <= s
            (BinOp::Le, BinOp::Ge),  // s <= 'x' ≡  'x' >= s
            (BinOp::Eq, BinOp::Eq),  // symmetric
            (BinOp::Ne, BinOp::Ne),  // symmetric
        ] {
            assert_eq!(
                compile(&s, col_op, &lit, &schema),
                compile(&lit, lit_op, &s, &schema),
                "col {col_op:?} 'lit' must match 'lit' {lit_op:?} col",
            );
        }
    }

    /// `a <op> b` (two string columns) is unaffected by the symmetrization.
    #[test]
    fn string_cmp_col_vs_col_unchanged() {
        let schema = str_schema();
        let a = BoundExpr::ColRef(1);
        let b = BoundExpr::ColRef(2);
        let got = compile(&a, BinOp::Lt, &b, &schema);
        let mut eb = ExprBuilder::new();
        let reg = eb.str_col_lt_col(1, 2);
        assert_eq!(got, eb.build(reg), "a < b must stay str_col_lt_col(a, b)");
    }

    /// An unsupported operator reports the original op in the error message.
    #[test]
    fn string_cmp_unsupported_names_op() {
        let schema = str_schema();
        let s   = BoundExpr::ColRef(1);
        let lit = BoundExpr::LitStr("x".to_string());
        let mut eb = ExprBuilder::new();
        let err = try_compile_string_cmp(&lit, &BinOp::Add, &s, &schema, &mut eb)
            .expect_err("Add is not a string comparison");
        assert!(err.to_string().contains("Add"), "error must name op: {err}");
    }
}
