use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, UnaryOp};
use gnitz_core::ExprBuilder;
use gnitz_core::{Schema, TypeCode};

/// Try to compile a string comparison (col vs const, const vs col, col vs col).
/// Returns Some((reg, false)) if this is a string comparison, None otherwise.
fn try_compile_string_cmp(
    left: &BoundExpr,
    op: &BinOp,
    right: &BoundExpr,
    schema: &Schema,
    eb: &mut ExprBuilder,
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
        (BoundExpr::LitStr(s), BoundExpr::ColRef(idx)) => Some((
            *idx,
            s,
            match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Gt => BinOp::Lt,
                BinOp::Le => BinOp::Ge,
                BinOp::Ge => BinOp::Le,
                other => *other, // Eq/Ne symmetric; rest fall through to Unsupported
            },
        )),
        _ => None,
    };
    if let Some((idx, s, cmp)) = col_lit {
        // STRING and BLOB share the 16-byte German-string layout and the engine's
        // `str_col_*` opcodes content-compare both (via `compare_german_strings`),
        // so a BLOB column-vs-literal comparison lowers here too, not to the
        // integer path (which would read the descriptor bytes as a garbage int).
        if schema.columns[idx].type_code.is_german_string() {
            let const_idx = eb.add_const_string(s.clone());
            let reg = match cmp {
                BinOp::Eq => eb.str_col_eq_const(idx, const_idx),
                BinOp::Ne => {
                    let r = eb.str_col_eq_const(idx, const_idx);
                    eb.bool_not(r)
                }
                BinOp::Lt => eb.str_col_lt_const(idx, const_idx),
                BinOp::Le => eb.str_col_le_const(idx, const_idx),
                BinOp::Gt => {
                    let r = eb.str_col_le_const(idx, const_idx);
                    eb.bool_not(r)
                }
                BinOp::Ge => {
                    let r = eb.str_col_lt_const(idx, const_idx);
                    eb.bool_not(r)
                }
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "operator {op:?} not supported for strings/blobs"
                    )))
                }
            };
            return Ok(Some((reg, false)));
        }
    }
    // ColRef(string/blob) op ColRef(string/blob)
    if let (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) = (left, right) {
        if schema.columns[*a].type_code.is_german_string() && schema.columns[*b].type_code.is_german_string() {
            let reg = match op {
                BinOp::Eq => eb.str_col_eq_col(*a, *b),
                BinOp::Ne => {
                    let r = eb.str_col_eq_col(*a, *b);
                    eb.bool_not(r)
                }
                BinOp::Lt => eb.str_col_lt_col(*a, *b),
                BinOp::Le => eb.str_col_le_col(*a, *b),
                BinOp::Gt => eb.str_col_lt_col(*b, *a),
                BinOp::Ge => eb.str_col_le_col(*b, *a),
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "operator {op:?} not supported for strings/blobs"
                    )))
                }
            };
            return Ok(Some((reg, false)));
        }
    }
    Ok(None)
}

/// Compile a BoundExpr to ExprBuilder opcodes.
/// Returns `(result_reg, is_float)` where `is_float` indicates the register
/// holds f64 bit-pattern rather than a plain i64.
pub(crate) fn compile_bound_expr(
    expr: &BoundExpr,
    schema: &Schema,
    eb: &mut ExprBuilder,
) -> Result<(u32, bool), GnitzSqlError> {
    match expr {
        BoundExpr::ColRef(idx) => {
            // Every 16-byte column (`wire_stride == 16`) must be rejected here: the
            // engine's `EXPR_LOAD_PAYLOAD_INT` handler has arms only for 1/2/4/8-byte
            // columns, so a 16-byte column hits its no-op arm and the following op
            // reads stale scratch bytes — silent corruption, no error. A *valid*
            // string/blob use is one of the six comparisons, which
            // `try_compile_string_cmp` intercepts before any recursion reaches this
            // arm; so a STRING/BLOB landing here is arithmetic or a mixed-type
            // comparison (`a.s > b.int`) and must error, not load garbage.
            let tc = schema.columns[*idx].type_code;
            match tc {
                TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => Err(GnitzSqlError::Unsupported(format!(
                    "column {:?} is {tc:?}; 128-bit columns cannot be used in view \
                     expressions (use a primary-key seek or CREATE INDEX instead)",
                    schema.columns[*idx].name,
                ))),
                TypeCode::String | TypeCode::Blob => Err(GnitzSqlError::Unsupported(format!(
                    "column {:?} is {tc:?}; string/blob columns support only =, <>, <, <=, \
                     >, >= against another string/blob column or a string literal — not \
                     arithmetic or comparison with a non-string column",
                    schema.columns[*idx].name,
                ))),
                TypeCode::F32 | TypeCode::F64 => Ok((eb.load_col_float(*idx), true)),
                _ => Ok((eb.load_col_int(*idx), false)),
            }
        }
        BoundExpr::LitInt(v) => Ok((eb.load_const(*v), false)),
        BoundExpr::LitFloat(v) => Ok((eb.load_const(v.to_bits() as i64), true)),
        BoundExpr::LitStr(_) => Err(GnitzSqlError::Unsupported(
            "string literals not supported in view expressions".to_string(),
        )),
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
                (BinOp::Add, true) => Ok((eb.float_add(l, r), true)),
                (BinOp::Sub, false) => Ok((eb.sub(l, r), false)),
                (BinOp::Sub, true) => Ok((eb.float_sub(l, r), true)),
                (BinOp::Mul, false) => Ok((eb.mul(l, r), false)),
                (BinOp::Mul, true) => Ok((eb.float_mul(l, r), true)),
                (BinOp::Div, false) => Ok((eb.div(l, r), false)),
                (BinOp::Div, true) => Ok((eb.float_div(l, r), true)),
                (BinOp::Mod, false) => Ok((eb.modulo(l, r), false)),
                (BinOp::Mod, true) => Err(GnitzSqlError::Unsupported("float modulo not supported".to_string())),
                // Comparisons — result is always int (0/1)
                (BinOp::Eq, false) => Ok((eb.cmp_eq(l, r), false)),
                (BinOp::Eq, true) => Ok((eb.fcmp_eq(l, r), false)),
                (BinOp::Ne, false) => Ok((eb.cmp_ne(l, r), false)),
                (BinOp::Ne, true) => Ok((eb.fcmp_ne(l, r), false)),
                (BinOp::Gt, false) => Ok((eb.cmp_gt(l, r), false)),
                (BinOp::Gt, true) => Ok((eb.fcmp_gt(l, r), false)),
                (BinOp::Ge, false) => Ok((eb.cmp_ge(l, r), false)),
                (BinOp::Ge, true) => Ok((eb.fcmp_ge(l, r), false)),
                (BinOp::Lt, false) => Ok((eb.cmp_lt(l, r), false)),
                (BinOp::Lt, true) => Ok((eb.fcmp_lt(l, r), false)),
                (BinOp::Le, false) => Ok((eb.cmp_le(l, r), false)),
                (BinOp::Le, true) => Ok((eb.fcmp_le(l, r), false)),
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
        BoundExpr::IsNull(idx) => Ok((eb.is_null(*idx), false)),
        BoundExpr::IsNotNull(idx) => Ok((eb.is_not_null(*idx), false)),
        BoundExpr::AggCall { .. } => Err(GnitzSqlError::Unsupported(
            "aggregate function not allowed in expression context".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, ExprProgram, Schema};

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_code: tc,
            is_nullable: true,
            fk_table_id: 0,
            fk_col_idx: 0,
        }
    }

    /// col 0 = pk (U64), col 1 = s (String), col 2 = t (String).
    fn str_schema() -> Schema {
        Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                col("s", TypeCode::String),
                col("t", TypeCode::String),
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
        let s = BoundExpr::ColRef(1);
        let lit = BoundExpr::LitStr("x".to_string());
        for (col_op, lit_op) in [
            (BinOp::Gt, BinOp::Lt), // s > 'x'  ≡  'x' < s
            (BinOp::Lt, BinOp::Gt), // s < 'x'  ≡  'x' > s
            (BinOp::Ge, BinOp::Le), // s >= 'x' ≡  'x' <= s
            (BinOp::Le, BinOp::Ge), // s <= 'x' ≡  'x' >= s
            (BinOp::Eq, BinOp::Eq), // symmetric
            (BinOp::Ne, BinOp::Ne), // symmetric
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
        let s = BoundExpr::ColRef(1);
        let lit = BoundExpr::LitStr("x".to_string());
        let mut eb = ExprBuilder::new();
        let err = try_compile_string_cmp(&lit, &BinOp::Add, &s, &schema, &mut eb)
            .expect_err("Add is not a string comparison");
        assert!(err.to_string().contains("Add"), "error must name op: {err}");
    }

    /// col 0 = pk (U64), col 1 = b (Blob), col 2 = c (Blob) — the BLOB analogue of
    /// `str_schema`, same column positions so the two compile identically.
    fn blob_schema() -> Schema {
        Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                col("b", TypeCode::Blob),
                col("c", TypeCode::Blob),
            ],
            pk_cols: vec![0],
        }
    }

    /// A BLOB comparison lowers to the same German-string content opcodes as STRING
    /// (§6.6a): blob col-vs-col and col-vs-literal compile byte-identically to the
    /// STRING form for every comparison operator.
    #[test]
    fn blob_cmp_matches_string_cmp() {
        let ss = str_schema();
        let bs = blob_schema();
        let col1 = BoundExpr::ColRef(1);
        let col2 = BoundExpr::ColRef(2);
        let lit = BoundExpr::LitStr("x".to_string());
        for op in [BinOp::Eq, BinOp::Ne, BinOp::Lt, BinOp::Le, BinOp::Gt, BinOp::Ge] {
            assert_eq!(
                compile(&col1, op, &col2, &bs),
                compile(&col1, op, &col2, &ss),
                "blob {op:?} blob must match string {op:?} string"
            );
            assert_eq!(
                compile(&col1, op, &lit, &bs),
                compile(&col1, op, &lit, &ss),
                "blob {op:?} 'lit' must match string {op:?} 'lit'"
            );
        }
    }

    /// col 0 = pk (U64), col 1 = s (String), col 2 = n (I64).
    fn string_int_schema() -> Schema {
        Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                col("s", TypeCode::String),
                col("n", TypeCode::I64),
            ],
            pk_cols: vec![0],
        }
    }

    /// A string-vs-int comparison (`a.s > b.n`) is NOT a content comparison
    /// (`try_compile_string_cmp` declines a mixed pair), so it reaches the `ColRef`
    /// integer-load path with a string column and must error — the §6.6b corruption
    /// guard, now a clean `Unsupported` instead of a garbage int load.
    #[test]
    fn mixed_string_int_cmp_rejects() {
        let schema = string_int_schema();
        let expr = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Gt,
            Box::new(BoundExpr::ColRef(2)),
        );
        let mut eb = ExprBuilder::new();
        let err = compile_bound_expr(&expr, &schema, &mut eb).expect_err("string > int must not compile");
        assert!(
            matches!(err, GnitzSqlError::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
    }

    /// String arithmetic (`a.s + 1`) likewise reaches the integer-load path and is
    /// rejected at CREATE rather than silently miscompiled (§6.6b).
    #[test]
    fn string_arithmetic_rejects() {
        let schema = str_schema();
        let expr = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Add,
            Box::new(BoundExpr::LitInt(1)),
        );
        let mut eb = ExprBuilder::new();
        let err = compile_bound_expr(&expr, &schema, &mut eb).expect_err("string + 1 must not compile");
        assert!(
            matches!(err, GnitzSqlError::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
    }
}
