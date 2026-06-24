use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, UnaryOp};
use crate::types::is_wide_int;
use gnitz_core::ExprBuilder;
use gnitz_core::Schema;

/// One structural walk of [`BoundExpr`], parameterized by what each node
/// *produces*. The single `match` in [`lower_bound_expr`] is the sole walk of
/// the enum: adding a `BoundExpr` variant makes it non-exhaustive, failing
/// *every* backend to compile (variant-coverage parity). A backend that cannot
/// represent a node returns a typed `Unsupported` arm — never a panic.
///
/// `binop`/`unop` receive their operands *unevaluated* (`&BoundExpr`) and drive
/// the recursion themselves via [`lower_bound_expr`]; this is load-bearing — the
/// opcode backend intercepts string comparisons before recursing into a string
/// literal, and the interpreter short-circuits `AND`/`OR` to avoid evaluating an
/// operand whose error would otherwise leak.
pub(crate) trait BoundExprBackend {
    type Out;
    fn col_ref(&mut self, col: usize) -> Result<Self::Out, GnitzSqlError>;
    fn lit_int(&mut self, v: i64) -> Result<Self::Out, GnitzSqlError>;
    fn lit_float(&mut self, v: f64) -> Result<Self::Out, GnitzSqlError>;
    fn lit_str(&mut self, s: &str) -> Result<Self::Out, GnitzSqlError>;
    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<Self::Out, GnitzSqlError>;
    fn unop(&mut self, op: UnaryOp, inner: &BoundExpr) -> Result<Self::Out, GnitzSqlError>;
    /// `IS NULL` (`want_null = true`) and `IS NOT NULL` (`want_null = false`).
    fn null_test(&mut self, col: usize, want_null: bool) -> Result<Self::Out, GnitzSqlError>;
    fn agg_call(&mut self) -> Result<Self::Out, GnitzSqlError>;
}

/// Dispatch a single `BoundExpr` node to the backend. The lone `match` over the
/// enum — every backend funnels through it.
pub(crate) fn lower_bound_expr<B: BoundExprBackend>(
    expr: &BoundExpr,
    backend: &mut B,
) -> Result<B::Out, GnitzSqlError> {
    match expr {
        BoundExpr::ColRef(c) => backend.col_ref(*c),
        BoundExpr::LitInt(v) => backend.lit_int(*v),
        BoundExpr::LitFloat(v) => backend.lit_float(*v),
        BoundExpr::LitStr(s) => backend.lit_str(s),
        BoundExpr::BinOp(l, op, r) => backend.binop(l, *op, r),
        BoundExpr::UnaryOp(op, inner) => backend.unop(*op, inner),
        BoundExpr::IsNull(c) => backend.null_test(*c, true),
        BoundExpr::IsNotNull(c) => backend.null_test(*c, false),
        BoundExpr::AggCall { .. } => backend.agg_call(),
    }
}

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

/// Lowers a `BoundExpr` to `ExprProgram` opcodes for the server-side circuit.
/// `Out = (result_reg, is_float)`, where `is_float` indicates the register holds
/// an f64 bit-pattern rather than a plain i64.
pub(crate) struct OpcodeBackend<'a> {
    schema: &'a Schema,
    eb: &'a mut ExprBuilder,
}

impl BoundExprBackend for OpcodeBackend<'_> {
    type Out = (u32, bool);

    fn col_ref(&mut self, idx: usize) -> Result<Self::Out, GnitzSqlError> {
        // Every 16-byte column must be rejected here: the engine's payload integer
        // load handler has arms only for 1/2/4/8-byte columns, so a 16-byte column
        // hits its no-op arm and the following op reads stale scratch bytes —
        // silent corruption, no error. A *valid* string/blob use
        // is one of the six comparisons, which `binop` intercepts via
        // `try_compile_string_cmp` before any recursion reaches this arm; so a
        // STRING/BLOB landing here is arithmetic or a mixed-type comparison
        // (`a.s > b.int`) and must error, not load garbage.
        let tc = self.schema.columns[idx].type_code;
        if is_wide_int(tc) {
            return Err(GnitzSqlError::Unsupported(format!(
                "column {:?} is {tc:?}; 128-bit columns cannot be used in view \
                 expressions (use a primary-key seek or CREATE INDEX instead)",
                self.schema.columns[idx].name,
            )));
        }
        if tc.is_german_string() {
            return Err(GnitzSqlError::Unsupported(format!(
                "column {:?} is {tc:?}; string/blob columns support only =, <>, <, <=, \
                 >, >= against another string/blob column or a string literal — not \
                 arithmetic or comparison with a non-string column",
                self.schema.columns[idx].name,
            )));
        }
        if tc.is_float() {
            return Ok((self.eb.load_col_float(idx), true));
        }
        Ok((self.eb.load_col_int(idx), false))
    }

    fn lit_int(&mut self, v: i64) -> Result<Self::Out, GnitzSqlError> {
        Ok((self.eb.load_const(v), false))
    }

    fn lit_float(&mut self, v: f64) -> Result<Self::Out, GnitzSqlError> {
        Ok((self.eb.load_const(v.to_bits() as i64), true))
    }

    fn lit_str(&mut self, _s: &str) -> Result<Self::Out, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "string literals not supported in view expressions".to_string(),
        ))
    }

    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<Self::Out, GnitzSqlError> {
        // String comparison detection — intercept before recursing into operands
        // (a bare string literal/column would otherwise error in `lit_str`/`col_ref`).
        if let Some(result) = try_compile_string_cmp(left, &op, right, self.schema, self.eb)? {
            return Ok(result);
        }

        let (mut l, l_float) = lower_bound_expr(left, self)?;
        let (mut r, r_float) = lower_bound_expr(right, self)?;

        // Boolean ops never need float cast
        if matches!(op, BinOp::And) {
            return Ok((self.eb.bool_and(l, r), false));
        }
        if matches!(op, BinOp::Or) {
            return Ok((self.eb.bool_or(l, r), false));
        }

        let is_float = l_float || r_float;

        // Cast int operand to float if mixed
        if is_float && !l_float {
            l = self.eb.int_to_float(l);
        }
        if is_float && !r_float {
            r = self.eb.int_to_float(r);
        }

        match (op, is_float) {
            // Arithmetic
            (BinOp::Add, false) => Ok((self.eb.add(l, r), false)),
            (BinOp::Add, true) => Ok((self.eb.float_add(l, r), true)),
            (BinOp::Sub, false) => Ok((self.eb.sub(l, r), false)),
            (BinOp::Sub, true) => Ok((self.eb.float_sub(l, r), true)),
            (BinOp::Mul, false) => Ok((self.eb.mul(l, r), false)),
            (BinOp::Mul, true) => Ok((self.eb.float_mul(l, r), true)),
            (BinOp::Div, false) => Ok((self.eb.div(l, r), false)),
            (BinOp::Div, true) => Ok((self.eb.float_div(l, r), true)),
            (BinOp::Mod, false) => Ok((self.eb.modulo(l, r), false)),
            (BinOp::Mod, true) => Err(GnitzSqlError::Unsupported("float modulo not supported".to_string())),
            // Comparisons — result is always int (0/1)
            (BinOp::Eq, false) => Ok((self.eb.cmp_eq(l, r), false)),
            (BinOp::Eq, true) => Ok((self.eb.fcmp_eq(l, r), false)),
            (BinOp::Ne, false) => Ok((self.eb.cmp_ne(l, r), false)),
            (BinOp::Ne, true) => Ok((self.eb.fcmp_ne(l, r), false)),
            (BinOp::Gt, false) => Ok((self.eb.cmp_gt(l, r), false)),
            (BinOp::Gt, true) => Ok((self.eb.fcmp_gt(l, r), false)),
            (BinOp::Ge, false) => Ok((self.eb.cmp_ge(l, r), false)),
            (BinOp::Ge, true) => Ok((self.eb.fcmp_ge(l, r), false)),
            (BinOp::Lt, false) => Ok((self.eb.cmp_lt(l, r), false)),
            (BinOp::Lt, true) => Ok((self.eb.fcmp_lt(l, r), false)),
            (BinOp::Le, false) => Ok((self.eb.cmp_le(l, r), false)),
            (BinOp::Le, true) => Ok((self.eb.fcmp_le(l, r), false)),
            // And/Or handled above
            (BinOp::And, _) | (BinOp::Or, _) => unreachable!(),
        }
    }

    fn unop(&mut self, op: UnaryOp, inner: &BoundExpr) -> Result<Self::Out, GnitzSqlError> {
        let (a, a_float) = lower_bound_expr(inner, self)?;
        match op {
            UnaryOp::Neg => {
                if a_float {
                    Ok((self.eb.float_neg(a), true))
                } else {
                    Ok((self.eb.neg_int(a), false))
                }
            }
            UnaryOp::Not => Ok((self.eb.bool_not(a), false)),
        }
    }

    fn null_test(&mut self, col: usize, want_null: bool) -> Result<Self::Out, GnitzSqlError> {
        if want_null {
            Ok((self.eb.is_null(col), false))
        } else {
            Ok((self.eb.is_not_null(col), false))
        }
    }

    fn agg_call(&mut self) -> Result<Self::Out, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "aggregate function not allowed in expression context".to_string(),
        ))
    }
}

/// Compile a BoundExpr to ExprBuilder opcodes, returning the result register.
/// The internal `is_float` bit that drives float-cast lowering is consumed only
/// inside the `OpcodeBackend` recursion and never escapes here.
pub(crate) fn compile_bound_expr(
    expr: &BoundExpr,
    schema: &Schema,
    eb: &mut ExprBuilder,
) -> Result<u32, GnitzSqlError> {
    let mut backend = OpcodeBackend { schema, eb };
    lower_bound_expr(expr, &mut backend).map(|(reg, _)| reg)
}

/// Compile a standalone BoundExpr into a finished `ExprProgram` (fresh
/// `ExprBuilder`, result register wired up). The one-shot form behind every
/// WHERE/HAVING/residual filter that needs a whole program rather than a
/// register threaded into a larger one.
pub(crate) fn compile_bound_expr_to_program(
    expr: &BoundExpr,
    schema: &Schema,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let reg = compile_bound_expr(expr, schema, &mut eb)?;
    Ok(eb.build(reg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, ExprProgram, Schema, TypeCode};

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef::new(name, tc, true)
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
