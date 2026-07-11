use super::resolve::find_unique_column;
use crate::ast_util::{
    agg_func_from_name, fn_name_is, function_positional_args, reject_unsupported_fn_qualifiers, single_fn_name,
    single_relation_col_name,
};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BinOp, BoundExpr, UnaryOp};
use crate::types::is_min_max_orderable;
use gnitz_core::Schema;
use sqlparser::ast::{
    BinaryOperator, CaseWhen, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator, Value,
};

/// Bind an expression against a single-relation schema (WHERE, projections,
/// set-op branches, DML). The structural recursion lives in `bind_structural`;
/// the `SingleTable` leaf supplies the three schema-aware decisions (column
/// lookup, aggregate calls, `IS [NOT] NULL`). Context is fully determined by
/// `(expr, schema)` — no binder state is involved.
pub(crate) fn bind_single_table(expr: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
    bind_structural(expr, &SingleTable { schema })
}

/// [`bind_single_table`] with the one `[NOT] EXISTS`/`[NOT] IN (SELECT …)` node
/// resolved to the constant `mark` — the mark builder's per-branch binding: on
/// the matched branch the subquery node is its truth value there (EXISTS → 1,
/// NOT EXISTS → 0), on the unmatched branch the complement, and the surrounding
/// OR/NOT/CASE evaluates over the constant like any other expression.
pub(crate) fn bind_single_table_mark(expr: &Expr, schema: &Schema, mark: i64) -> Result<BoundExpr, GnitzSqlError> {
    struct MarkLeaf<'a> {
        inner: SingleTable<'a>,
        mark: i64,
    }
    impl LeafBinder for MarkLeaf<'_> {
        fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
            self.inner.bind_column(e)
        }
        fn bind_function(&self, f: &Function) -> Result<BoundExpr, GnitzSqlError> {
            self.inner.bind_function(f)
        }
        fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
            self.inner.bind_null_test(inner, want_null)
        }
        fn bind_subquery(&self, _e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
            Ok(BoundExpr::LitInt(self.mark))
        }
    }
    bind_structural(
        expr,
        &MarkLeaf {
            inner: SingleTable { schema },
            mark,
        },
    )
}

// ---------------------------------------------------------------------------
// Shared structural expression binding
// ---------------------------------------------------------------------------
//
// One `Expr → BoundExpr` recursion, parametrized by a three-method leaf. The
// only things that differ across GnitzDB's binding contexts (WHERE/projection,
// HAVING, JOIN residual) are the three schema-aware leaves — column reference,
// function call, and `IS [NOT] NULL`. Everything structural (literals, the full
// operator map, unary ops, the BETWEEN desugar, Nested unwrap) lives here once,
// so a new operator or fold lands on every context at the same time and cannot
// silently drift between hand-copied walks.

/// The three schema-aware leaves of `bind_structural`.
pub(crate) trait LeafBinder {
    /// An `Identifier` / `CompoundIdentifier` column reference.
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError>;
    /// A function call (aggregates, or a context-specific rejection).
    fn bind_function(&self, f: &Function) -> Result<BoundExpr, GnitzSqlError>;
    /// `inner IS [NOT] NULL` (`want_null` picks IS NULL vs IS NOT NULL).
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError>;
    /// A `[NOT] EXISTS` / `[NOT] IN (SELECT …)` node. Only the mark builder's
    /// leaf overrides this — it resolves the node to the branch's `0/1` constant
    /// ([`bind_single_table_mark`]); every other context keeps the placement
    /// rejection. (The top-level-AND-conjunct filter path never binds the node:
    /// the exists builder intercepts it before binding.)
    fn bind_subquery(&self, _e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "[NOT] EXISTS/IN (SELECT …) is only supported in a single-table CREATE VIEW \
             (in the WHERE clause or the SELECT list)"
                .into(),
        ))
    }
}

/// The one structural recursion. Needs no schema — every schema-aware decision
/// is a leaf method. Generic over `L` (static dispatch) so a leaf's
/// `bind_function` can recurse via `bind_structural(arg, self)`.
pub(crate) fn bind_structural<L: LeafBinder>(expr: &Expr, leaf: &L) -> Result<BoundExpr, GnitzSqlError> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => leaf.bind_column(expr),
        // COALESCE / NULLIF are structural desugars into CASE, intercepted before
        // the leaf's `bind_function` so all three leaf impls stay untouched. Every
        // other name falls through to the leaf (aggregates, or a context rejection).
        Expr::Function(f) if fn_name_is(f, "coalesce") => {
            bind_coalesce(&function_positional_args(f, "COALESCE")?, leaf)
        }
        Expr::Function(f) if fn_name_is(f, "nullif") => bind_nullif(&function_positional_args(f, "NULLIF")?, leaf),
        Expr::Function(f) => leaf.bind_function(f),
        Expr::IsNull(i) => leaf.bind_null_test(i, true),
        Expr::IsNotNull(i) => leaf.bind_null_test(i, false),
        Expr::Nested(i) => bind_structural(i, leaf),
        Expr::Value(vws) => bind_literal(&vws.value),
        // Searched CASE binds each `(condition, result)`; the simple-operand form
        // desugars each WHEN value `w` to `operand = w` (operand re-bound per
        // branch, like the BETWEEN desugar). A missing ELSE is the implicit
        // ELSE NULL (`else_ = None`, lowered to `load_null`).
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let mut branches = Vec::with_capacity(conditions.len());
            for CaseWhen { condition, result } in conditions {
                let cond = match operand {
                    Some(op) => BoundExpr::BinOp(
                        Box::new(bind_structural(op, leaf)?),
                        BinOp::Eq,
                        Box::new(bind_structural(condition, leaf)?),
                    ),
                    None => bind_structural(condition, leaf)?,
                };
                branches.push((cond, bind_structural(result, leaf)?));
            }
            let else_ = else_result
                .as_ref()
                .map(|e| bind_structural(e, leaf))
                .transpose()?
                .map(Box::new);
            Ok(BoundExpr::Case { branches, else_ })
        }
        Expr::BinaryOp { left, op, right } => {
            let l = bind_structural(left, leaf)?;
            let r = bind_structural(right, leaf)?;
            Ok(BoundExpr::BinOp(Box::new(l), map_binop(op)?, Box::new(r)))
        }
        Expr::UnaryOp { op, expr } => {
            let inner = bind_structural(expr, leaf)?;
            let uop = match op {
                UnaryOperator::Minus => UnaryOp::Neg,
                UnaryOperator::Not => UnaryOp::Not,
                o => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "unary operator {o:?} not supported"
                    )))
                }
            };
            Ok(BoundExpr::UnaryOp(uop, Box::new(inner)))
        }
        // `e BETWEEN lo AND hi` ≡ `e >= lo AND e <= hi`; NOT BETWEEN negates it.
        // NULL semantics are SQL-correct under either form (a NULL operand makes
        // the AND NULL, and NOT(NULL) is NULL → the row is excluded).
        Expr::Between {
            expr: e,
            negated,
            low,
            high,
        } => {
            let ge = BoundExpr::BinOp(
                Box::new(bind_structural(e, leaf)?),
                BinOp::Ge,
                Box::new(bind_structural(low, leaf)?),
            );
            let le = BoundExpr::BinOp(
                Box::new(bind_structural(e, leaf)?),
                BinOp::Le,
                Box::new(bind_structural(high, leaf)?),
            );
            let between = BoundExpr::BinOp(Box::new(ge), BinOp::And, Box::new(le));
            Ok(if *negated {
                BoundExpr::UnaryOp(UnaryOp::Not, Box::new(between))
            } else {
                between
            })
        }
        // `e IN (l1, …, ln)` ≡ `e = l1 OR … OR e = ln`; NOT IN negates it. SQL 3VL
        // is preserved by the desugar itself: a NULL `e` makes every Eq NULL, the
        // OR-chain NULL, and NOT(NULL) NULL — the row is excluded either way (same
        // argument as the BETWEEN desugar above). The tested expression is bound
        // once and cloned into each Eq.
        Expr::InList { expr: e, list, negated } => {
            let (first, rest) = list
                .split_first()
                .ok_or_else(|| GnitzSqlError::Unsupported("IN with an empty list".into()))?;
            let tested = bind_structural(e, leaf)?;
            let eq = |item| -> Result<BoundExpr, GnitzSqlError> {
                Ok(BoundExpr::BinOp(
                    Box::new(tested.clone()),
                    BinOp::Eq,
                    Box::new(bind_structural(item, leaf)?),
                ))
            };
            let mut chain = eq(first)?;
            for item in rest {
                chain = BoundExpr::BinOp(Box::new(chain), BinOp::Or, Box::new(eq(item)?));
            }
            Ok(if *negated {
                BoundExpr::UnaryOp(UnaryOp::Not, Box::new(chain))
            } else {
                chain
            })
        }
        // Subquery placement is the leaf's decision: the mark builder binds the
        // node to its branch constant; every other leaf keeps the default
        // placement rejection (HAVING, DML, a direct SELECT, …).
        Expr::Exists { .. } | Expr::InSubquery { .. } => leaf.bind_subquery(expr),
        Expr::Subquery(_) => Err(GnitzSqlError::Unsupported("scalar subqueries are not supported".into())),
        Expr::AnyOp { .. } | Expr::AllOp { .. } => Err(GnitzSqlError::Unsupported(
            "ANY/SOME/ALL subquery comparisons are not supported".into(),
        )),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "expression type not supported: {expr:?}"
        ))),
    }
}

/// sqlparser binary op → `BinOp` (the single, complete map).
fn map_binop(op: &BinaryOperator) -> Result<BinOp, GnitzSqlError> {
    Ok(match op {
        BinaryOperator::Plus => BinOp::Add,
        BinaryOperator::Minus => BinOp::Sub,
        BinaryOperator::Multiply => BinOp::Mul,
        BinaryOperator::Divide => BinOp::Div,
        BinaryOperator::Modulo => BinOp::Mod,
        BinaryOperator::Eq => BinOp::Eq,
        BinaryOperator::NotEq => BinOp::Ne,
        BinaryOperator::Gt => BinOp::Gt,
        BinaryOperator::GtEq => BinOp::Ge,
        BinaryOperator::Lt => BinOp::Lt,
        BinaryOperator::LtEq => BinOp::Le,
        BinaryOperator::And => BinOp::And,
        BinaryOperator::Or => BinOp::Or,
        o => {
            return Err(GnitzSqlError::Unsupported(format!(
                "binary operator {o:?} not supported"
            )))
        }
    })
}

/// Number/string literal → `BoundExpr`.
fn bind_literal(v: &Value) -> Result<BoundExpr, GnitzSqlError> {
    match v {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(BoundExpr::LitInt(i))
            } else if n.contains(['.', 'e', 'E']) {
                n.parse::<f64>()
                    .map(BoundExpr::LitFloat)
                    .map_err(|_| GnitzSqlError::Bind(format!("invalid number literal: {n}")))
            } else {
                // Non-fractional literal that overflows i64. `BoundExpr::LitInt` is
                // i64-only; representing it as f64 would run an integer-column
                // comparison through a lossy 52-bit mantissa (e.g. u64::MAX matches
                // the wrong rows).
                Err(GnitzSqlError::Bind(format!("integer literal out of range: {n}")))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(BoundExpr::LitStr(s.clone())),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "value type not supported in expressions: {v:?}"
        ))),
    }
}

/// `COALESCE(args…)` desugars right-to-left into CASE: the first non-NULL operand
/// is the result. Total over arity via explicit base cases; a literal operand is
/// folded at the AST level so it never reaches `bind_null_test` (which resolves a
/// *column* and would reject a literal with "expected a column reference").
fn bind_coalesce<L: LeafBinder>(args: &[&Expr], leaf: &L) -> Result<BoundExpr, GnitzSqlError> {
    let [a, rest @ ..] = args else {
        // COALESCE() ≡ NULL (parser forbids; stay total) — also the tail after an
        // all-NULL-literals argument list.
        return Ok(BoundExpr::LitNull);
    };
    if let Expr::Value(v) = a {
        // A NULL literal contributes nothing; any other literal is non-null → the result.
        return match &v.value {
            Value::Null => bind_coalesce(rest, leaf),
            _ => bind_literal(&v.value),
        };
    }
    if rest.is_empty() {
        return bind_structural(a, leaf); // last operand: its value is the result
    }
    let cond = leaf.bind_null_test(a, /* want_null = */ false)?; // IsNotNull(col) | LitInt(1)
    if matches!(cond, BoundExpr::LitInt(1)) {
        return bind_structural(a, leaf); // provably non-null (NOT NULL column) → always taken
    }
    Ok(BoundExpr::Case {
        branches: vec![(cond, bind_structural(a, leaf)?)],
        else_: Some(Box::new(bind_coalesce(rest, leaf)?)),
    })
}

/// `NULLIF(a, b)` ≡ `CASE WHEN a = b THEN NULL ELSE a END`. Both operands bind
/// through `bind_structural` (not `bind_null_test`), so NULLIF has no
/// literal-operand pitfall.
fn bind_nullif<L: LeafBinder>(args: &[&Expr], leaf: &L) -> Result<BoundExpr, GnitzSqlError> {
    let [a, b] = args else {
        return Err(GnitzSqlError::Unsupported(
            "NULLIF: requires exactly two arguments".into(),
        ));
    };
    let a_bound = bind_structural(a, leaf)?;
    let cond = BoundExpr::BinOp(
        Box::new(a_bound.clone()),
        BinOp::Eq,
        Box::new(bind_structural(b, leaf)?),
    );
    Ok(BoundExpr::Case {
        branches: vec![(cond, BoundExpr::LitNull)],
        else_: Some(Box::new(a_bound)),
    })
}

/// Shared NOT-NULL fold: a never-null value makes `IS [NOT] NULL` a constant
/// (never null, never — for IS NULL — true), keeping the null-tracking opcode
/// (which forces `eval_batch`'s slow path — `is_strictly_non_nullable` returns
/// false on any is_null) out of the program. `nullable` is the caller's
/// authoritative nullability fact for the value at column `idx` — a schema
/// column's `is_nullable`, or a HAVING aggregate's structural nullability.
pub(crate) fn fold_null_test(nullable: bool, idx: usize, want_null: bool) -> BoundExpr {
    if !nullable {
        BoundExpr::LitInt(i64::from(!want_null))
    } else if want_null {
        BoundExpr::IsNull(idx)
    } else {
        BoundExpr::IsNotNull(idx)
    }
}

/// Leaf for a single-relation schema (WHERE, projections, set-ops, DML).
pub(crate) struct SingleTable<'a> {
    pub schema: &'a Schema,
}

impl SingleTable<'_> {
    /// The column index of an `Identifier` / two-part `CompoundIdentifier`. The
    /// qualifier on a compound ref is informational in a single-table context
    /// (it carries no disambiguating information — a duplicated name is ambiguous
    /// even when qualified).
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        let name = single_relation_col_name(e)
            .ok_or_else(|| GnitzSqlError::Unsupported("expected a column reference".into()))?;
        find_unique_column(&self.schema.columns, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found")))
    }
}

impl LeafBinder for SingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(self.idx(e)?))
    }
    fn bind_function(&self, func: &Function) -> Result<BoundExpr, GnitzSqlError> {
        reject_unsupported_fn_qualifiers(func, "aggregates")?;
        // The one name→aggregate map (`agg_func_from_name`) — no allocation on
        // the hit path; the argument shape then picks the COUNT variant and
        // validates arity.
        let Some(agg_func) = single_fn_name(func).and_then(agg_func_from_name) else {
            return Err(GnitzSqlError::Unsupported(format!(
                "function '{}' not supported",
                func.name.to_string().to_ascii_lowercase()
            )));
        };
        match agg_func {
            AggFunc::Count | AggFunc::CountNonNull => {
                if let FunctionArguments::List(list) = &func.args {
                    if list.args.len() == 1 {
                        match &list.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                return Ok(BoundExpr::AggCall {
                                    func: AggFunc::Count,
                                    arg: None,
                                });
                            }
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                                let bound = bind_structural(inner, self)?;
                                return Ok(BoundExpr::AggCall {
                                    func: AggFunc::CountNonNull,
                                    arg: Some(Box::new(bound)),
                                });
                            }
                            _ => {}
                        }
                    }
                }
                Err(GnitzSqlError::Unsupported(
                    "COUNT: unsupported argument form".to_string(),
                ))
            }
            AggFunc::Sum | AggFunc::Min | AggFunc::Max | AggFunc::Avg => {
                let name = match agg_func {
                    AggFunc::Sum => "sum",
                    AggFunc::Min => "min",
                    AggFunc::Max => "max",
                    AggFunc::Avg => "avg",
                    AggFunc::Count | AggFunc::CountNonNull => unreachable!(),
                };
                if let FunctionArguments::List(list) = &func.args {
                    if list.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0] {
                            let bound = bind_structural(inner, self)?;
                            // MIN/MAX have no correct accumulator path for wide
                            // (U128/UUID) types — the i64 slot cannot hold them —
                            // Blob has no ordering, and the String comparator in
                            // `decode_signed` reads the prefix as LE signed i64,
                            // which orders by neither bytes nor signedness. Reject
                            // here so the operator only sees types it can compare
                            // correctly.
                            if matches!(agg_func, AggFunc::Min | AggFunc::Max) {
                                let arg_ty = bound.infer_type(self.schema);
                                if !is_min_max_orderable(arg_ty) {
                                    return Err(GnitzSqlError::Unsupported(format!(
                                        "{}: not supported on {:?} columns",
                                        name.to_uppercase(),
                                        arg_ty
                                    )));
                                }
                            }
                            return Ok(BoundExpr::AggCall {
                                func: agg_func,
                                arg: Some(Box::new(bound)),
                            });
                        }
                    }
                }
                Err(GnitzSqlError::Unsupported(format!(
                    "{name}: requires exactly one column argument"
                )))
            }
        }
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        let idx = self.idx(inner)?;
        Ok(fold_null_test(self.schema.columns[idx].is_nullable, idx, want_null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, Schema, TypeCode};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef::new(name, tc, false)
    }

    fn schema_with_val(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col("pk", TypeCode::U64), col("c", val_tc)],
            pk_cols: vec![0],
        }
    }

    fn parse(src: &str) -> Expr {
        let dialect = GenericDialect {};
        Parser::new(&dialect).try_with_sql(src).unwrap().parse_expr().unwrap()
    }

    fn assert_unsupported(r: Result<BoundExpr, GnitzSqlError>, want_substr: &str) {
        match r.unwrap_err() {
            GnitzSqlError::Unsupported(msg) => {
                assert!(
                    msg.contains(want_substr),
                    "got Unsupported({msg:?}), expected to contain {want_substr:?}"
                );
            }
            e => panic!("expected Unsupported, got {e:?}"),
        }
    }

    fn num(n: &str) -> Result<BoundExpr, GnitzSqlError> {
        bind_literal(&Value::Number(n.into(), false))
    }

    #[test]
    fn bind_literal_rejects_out_of_i64_range_integer() {
        // u64::MAX overflows i64 and is non-fractional → rejected, not coerced to f64.
        assert!(matches!(num("18446744073709551615"), Err(GnitzSqlError::Bind(_))));
    }

    #[test]
    fn bind_literal_accepts_fractional_and_exponent_floats() {
        assert!(matches!(num("1.5"), Ok(BoundExpr::LitFloat(_))));
        assert!(matches!(num("1e3"), Ok(BoundExpr::LitFloat(_))));
    }

    #[test]
    fn bind_literal_accepts_in_range_integer() {
        assert!(matches!(num("42"), Ok(BoundExpr::LitInt(42))));
    }

    #[test]
    fn test_bind_between_desugars_to_comparison_tree() {
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
                                                     // `c BETWEEN 1 AND 9` ≡ `c >= 1 AND c <= 9` — a residual BETWEEN now binds
                                                     // (regression guard for the new Expr::Between arm) instead of Unsupported.
        match bind_single_table(&parse("c BETWEEN 1 AND 9"), &schema).unwrap() {
            BoundExpr::BinOp(l, BinOp::And, r) => {
                assert!(matches!(*l, BoundExpr::BinOp(_, BinOp::Ge, _)));
                assert!(matches!(*r, BoundExpr::BinOp(_, BinOp::Le, _)));
            }
            other => panic!("expected And(Ge, Le), got {other:?}"),
        }
        // `c NOT BETWEEN 1 AND 9` ≡ NOT(c >= 1 AND c <= 9).
        match bind_single_table(&parse("c NOT BETWEEN 1 AND 9"), &schema).unwrap() {
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                assert!(matches!(*inner, BoundExpr::BinOp(_, BinOp::And, _)))
            }
            other => panic!("expected Not(And(..)), got {other:?}"),
        }
    }

    #[test]
    fn test_binder_rejects_min_max_unsupported_types() {
        for &tc in &[
            TypeCode::U128,
            TypeCode::UUID,
            TypeCode::Blob,
            TypeCode::String,
            TypeCode::I128,
        ] {
            let schema = schema_with_val(tc);
            for fname in &["MIN", "MAX"] {
                let expr = parse(&format!("{fname}(c)"));
                let r = bind_single_table(&expr, &schema);
                assert_unsupported(r, fname);
            }
        }
    }

    #[test]
    fn test_binder_accepts_min_max_orderable_types() {
        // Types the operator can compare correctly:
        // narrow unsigned + zero-extend, signed, U64 (with the unsigned fix),
        // and floats.
        let accepted = [
            TypeCode::U8,
            TypeCode::U16,
            TypeCode::U32,
            TypeCode::U64,
            TypeCode::I8,
            TypeCode::I16,
            TypeCode::I32,
            TypeCode::I64,
            TypeCode::F32,
            TypeCode::F64,
        ];
        for &tc in &accepted {
            let schema = schema_with_val(tc);
            for fname in &["MIN", "MAX"] {
                let expr = parse(&format!("{fname}(c)"));
                let r = bind_single_table(&expr, &schema);
                assert!(r.is_ok(), "expected {}({:?}) to bind, got {:?}", fname, tc, r.err());
            }
        }
    }

    /// `t.x IS [NOT] NULL` on a *qualified* (CompoundIdentifier) column now binds
    /// like the unqualified form in every `bind_expr` context — the unified core
    /// reaches the shared null-test leaf, which the old bare-`Identifier`-only arm
    /// rejected with "IS NULL on non-column expression".
    #[test]
    fn test_compound_identifier_null_test_binds() {
        // Non-nullable column: folds to the constant (0 for IS NULL, 1 for IS NOT NULL).
        let nn = schema_with_val(TypeCode::I64); // (pk U64 NOT NULL, c I64 NOT NULL)
        assert!(matches!(
            bind_single_table(&parse("t.c IS NULL"), &nn).unwrap(),
            BoundExpr::LitInt(0)
        ));
        assert!(matches!(
            bind_single_table(&parse("t.c IS NOT NULL"), &nn).unwrap(),
            BoundExpr::LitInt(1)
        ));
        // Nullable column: binds to the IsNull / IsNotNull opcode at the column index.
        let nullable = Schema {
            columns: vec![col("pk", TypeCode::U64), ColumnDef::new("c", TypeCode::I64, true)],
            pk_cols: vec![0],
        };
        assert!(matches!(
            bind_single_table(&parse("t.c IS NULL"), &nullable).unwrap(),
            BoundExpr::IsNull(1)
        ));
        assert!(matches!(
            bind_single_table(&parse("t.c IS NOT NULL"), &nullable).unwrap(),
            BoundExpr::IsNotNull(1)
        ));
    }

    /// Every aggregate qualifier the binder does not implement must be rejected,
    /// not silently dropped to the plain aggregate. Exercised through
    /// `bind_single_table` so the guard's wiring into `bind_function` is covered,
    /// not just the helper in isolation.
    #[test]
    fn test_binder_rejects_aggregate_qualifiers() {
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
        for (src, want) in [
            ("COUNT(DISTINCT c)", "DISTINCT"),
            ("SUM(DISTINCT c)", "DISTINCT"),
            ("MIN(DISTINCT c)", "DISTINCT"), // no-op distinct, rejected for a uniform surface
            ("SUM(c) FILTER (WHERE c > 0)", "FILTER"),
            ("SUM(c) OVER (PARTITION BY pk)", "OVER"),
        ] {
            assert_unsupported(bind_single_table(&parse(src), &schema), want);
        }
    }

    /// `c IN (…)` desugars to an Eq/Or chain; `NOT IN` wraps it in Not. A single
    /// element degenerates to the bare Eq (no Or wrapper).
    #[test]
    fn test_bind_in_list_desugars_to_or_chain() {
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
        match bind_single_table(&parse("c IN (1, 2)"), &schema).unwrap() {
            BoundExpr::BinOp(l, BinOp::Or, r) => {
                assert!(matches!(*l, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(matches!(*r, BoundExpr::BinOp(_, BinOp::Eq, _)));
            }
            other => panic!("expected Or(Eq, Eq), got {other:?}"),
        }
        assert!(matches!(
            bind_single_table(&parse("c IN (7)"), &schema).unwrap(),
            BoundExpr::BinOp(_, BinOp::Eq, _)
        ));
        match bind_single_table(&parse("c NOT IN (1, 2)"), &schema).unwrap() {
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                assert!(matches!(*inner, BoundExpr::BinOp(_, BinOp::Or, _)))
            }
            other => panic!("expected Not(Or(..)), got {other:?}"),
        }
        // Left-assoc chain at n = 3: Or(Or(Eq, Eq), Eq).
        match bind_single_table(&parse("c IN (1, 2, 3)"), &schema).unwrap() {
            BoundExpr::BinOp(l, BinOp::Or, r) => {
                assert!(matches!(*l, BoundExpr::BinOp(_, BinOp::Or, _)));
                assert!(matches!(*r, BoundExpr::BinOp(_, BinOp::Eq, _)));
            }
            other => panic!("expected Or(Or, Eq), got {other:?}"),
        }
    }

    /// String and float list elements bind through the same literal leaves plain
    /// `=` uses; an empty list is rejected (constructed directly — the parser
    /// won't produce one).
    #[test]
    fn test_bind_in_list_string_float_and_empty() {
        let s = schema_with_val(TypeCode::String);
        assert!(bind_single_table(&parse("c IN ('a', 'b')"), &s).is_ok());
        let f = schema_with_val(TypeCode::F64);
        assert!(bind_single_table(&parse("c IN (1.5, 2.5)"), &f).is_ok());
        let empty = Expr::InList {
            expr: Box::new(parse("c")),
            list: vec![],
            negated: false,
        };
        assert_unsupported(bind_single_table(&empty, &f), "empty list");
    }

    /// Subquery expressions outside the supported placements get the targeted
    /// message, not the generic catch-all — while the mark leaf binds the same
    /// nodes to its branch constant.
    #[test]
    fn test_bind_rejects_subquery_expressions_with_targeted_messages() {
        let schema = schema_with_val(TypeCode::I64);
        for src in [
            "EXISTS (SELECT c FROM t)",
            "NOT EXISTS (SELECT c FROM t)",
            "c IN (SELECT c FROM t)",
            "c NOT IN (SELECT c FROM t)",
            "c = 1 OR EXISTS (SELECT c FROM t)",
        ] {
            assert_unsupported(
                bind_single_table(&parse(src), &schema),
                "only supported in a single-table CREATE VIEW",
            );
            // The mark leaf resolves the subquery node to its 0/1 constant.
            assert!(bind_single_table_mark(&parse(src), &schema, 1).is_ok());
        }
        // The subquery node is the constant; surrounding structure survives:
        // `c = 1 OR EXISTS(…)` at mark=0 binds as `(c = 1) OR 0`.
        match bind_single_table_mark(&parse("c = 1 OR EXISTS (SELECT c FROM t)"), &schema, 0).unwrap() {
            BoundExpr::BinOp(_, BinOp::Or, r) => assert!(matches!(*r, BoundExpr::LitInt(0))),
            other => panic!("expected `… OR 0`, got {other:?}"),
        }
        assert_unsupported(
            bind_single_table(&parse("c = ANY (SELECT c FROM t)"), &schema),
            "ANY/SOME/ALL",
        );
        assert_unsupported(
            bind_single_table(&parse("c > ALL (SELECT c FROM t)"), &schema),
            "ANY/SOME/ALL",
        );
        assert_unsupported(
            bind_single_table(&parse("(SELECT c FROM t) = 1"), &schema),
            "scalar subqueries",
        );
    }

    fn nullable_schema(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col("pk", TypeCode::U64), ColumnDef::new("c", val_tc, true)],
            pk_cols: vec![0],
        }
    }

    /// Searched `CASE WHEN … THEN … [ELSE …] END` binds each branch; a missing
    /// ELSE is `else_ = None` (implicit NULL).
    #[test]
    fn test_bind_searched_case() {
        let s = nullable_schema(TypeCode::I64);
        match bind_single_table(&parse("CASE WHEN c > 0 THEN 1 WHEN c < 0 THEN 2 ELSE 3 END"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 2);
                assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Gt, _)));
                assert!(matches!(branches[0].1, BoundExpr::LitInt(1)));
                assert!(matches!(branches[1].0, BoundExpr::BinOp(_, BinOp::Lt, _)));
                assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(3))));
            }
            other => panic!("expected Case, got {other:?}"),
        }
        // Missing ELSE → else_ = None.
        match bind_single_table(&parse("CASE WHEN c > 0 THEN 1 END"), &s).unwrap() {
            BoundExpr::Case { else_, .. } => assert!(else_.is_none(), "missing ELSE → None"),
            other => panic!("expected Case, got {other:?}"),
        }
    }

    /// Simple-operand `CASE c WHEN v THEN … END` desugars each WHEN to `c = v`.
    #[test]
    fn test_bind_simple_case_desugars_to_operand_eq() {
        let s = nullable_schema(TypeCode::I64);
        match bind_single_table(&parse("CASE c WHEN 1 THEN 10 WHEN 2 THEN 20 END"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 2);
                assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(matches!(branches[1].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(else_.is_none());
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    /// COALESCE base cases and shape.
    #[test]
    fn test_bind_coalesce_base_cases() {
        let s = nullable_schema(TypeCode::I64);
        // COALESCE(c) → c.
        assert!(matches!(
            bind_single_table(&parse("COALESCE(c)"), &s).unwrap(),
            BoundExpr::ColRef(1)
        ));
        // COALESCE(NULL, c) → c (a NULL literal contributes nothing).
        assert!(matches!(
            bind_single_table(&parse("COALESCE(NULL, c)"), &s).unwrap(),
            BoundExpr::ColRef(1)
        ));
        // COALESCE(c, 0) → Case{[(IsNotNull(c), c)], else: 0}.
        match bind_single_table(&parse("COALESCE(c, 0)"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 1);
                assert!(matches!(branches[0].0, BoundExpr::IsNotNull(1)));
                assert!(matches!(branches[0].1, BoundExpr::ColRef(1)));
                assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(0))));
            }
            other => panic!("expected Case, got {other:?}"),
        }
        // A NOT NULL column folds COALESCE(c, 0) to c directly (provably non-null).
        let nn = schema_with_val(TypeCode::I64);
        assert!(matches!(
            bind_single_table(&parse("COALESCE(c, 0)"), &nn).unwrap(),
            BoundExpr::ColRef(1)
        ));
    }

    /// A 3-arg COALESCE nests right-to-left.
    #[test]
    fn test_bind_coalesce_nested_three_arg() {
        let s = Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                ColumnDef::new("a", TypeCode::I64, true),
                ColumnDef::new("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        match bind_single_table(&parse("COALESCE(a, b, 0)"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert!(matches!(branches[0].0, BoundExpr::IsNotNull(1)));
                match else_.as_deref() {
                    Some(BoundExpr::Case {
                        branches: inner,
                        else_: inner_else,
                    }) => {
                        assert!(matches!(inner[0].0, BoundExpr::IsNotNull(2)));
                        assert!(matches!(inner_else.as_deref(), Some(BoundExpr::LitInt(0))));
                    }
                    other => panic!("expected nested Case, got {other:?}"),
                }
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    /// A computed COALESCE operand (`c + 1`) reaches `bind_null_test`, which
    /// resolves a *column*, so it errors "expected a column reference" rather than
    /// mis-binding.
    #[test]
    fn test_bind_coalesce_computed_operand_rejected() {
        let s = nullable_schema(TypeCode::I64);
        assert_unsupported(
            bind_single_table(&parse("COALESCE(c + 1, 0)"), &s),
            "expected a column reference",
        );
    }

    /// `NULLIF(a, b)` → `CASE WHEN a = b THEN NULL ELSE a END`; wrong arity errors.
    #[test]
    fn test_bind_nullif() {
        let s = nullable_schema(TypeCode::I64);
        match bind_single_table(&parse("NULLIF(c, 0)"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 1);
                assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(matches!(branches[0].1, BoundExpr::LitNull));
                assert!(matches!(else_.as_deref(), Some(BoundExpr::ColRef(1))));
            }
            other => panic!("expected Case, got {other:?}"),
        }
        // Both operands bind through bind_structural, so a computed operand is fine.
        assert!(bind_single_table(&parse("NULLIF(c + 1, 0)"), &s).is_ok());
        // Wrong arity rejected.
        assert_unsupported(bind_single_table(&parse("NULLIF(c)"), &s), "exactly two");
    }

    /// The accepted qualifier (`ALL`, the SQL default — `COUNT(ALL c)` ≡
    /// `COUNT(c)`) and the plain forms must keep binding.
    #[test]
    fn test_binder_accepts_all_and_plain_aggregates() {
        let schema = schema_with_val(TypeCode::I64);
        for src in [
            "COUNT(*)",
            "COUNT(c)",
            "COUNT(ALL c)",
            "SUM(c)",
            "MIN(c)",
            "MAX(c)",
            "AVG(c)",
        ] {
            assert!(
                bind_single_table(&parse(src), &schema).is_ok(),
                "expected {src} to bind"
            );
        }
    }
}
