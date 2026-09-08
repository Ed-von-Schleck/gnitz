use super::*;
use crate::test_support::parse_expr_sql;
use gnitz_core::{ColumnDef, Schema, TypeCode};

/// Bind against `schema` as relation `t` — the alias every qualified reference
/// in this file writes.
fn bind1(e: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
    bind_single_table(e, schema, "t")
}

fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

fn schema_with_val(val_tc: TypeCode) -> Schema {
    Schema {
        columns: vec![col("pk", TypeCode::U64), col("c", val_tc)],
        pk_cols: vec![0],
    }
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

/// A null test on a PK column never survives binding: a PK column is always
/// non-nullable, so `null_test` settles it as a literal.
#[test]
fn null_test_on_pk_column_folds_to_a_literal() {
    let schema = schema_with_val(TypeCode::I64); // pk is NOT NULL
    assert!(matches!(
        bind1(&parse_expr_sql("pk IS NULL"), &schema).unwrap(),
        BoundExpr::LitInt(0)
    ));
    assert!(matches!(
        bind1(&parse_expr_sql("pk IS NOT NULL"), &schema).unwrap(),
        BoundExpr::LitInt(1)
    ));
}

#[test]
fn test_bind_between_desugars_to_comparison_tree() {
    let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
                                                 // `c BETWEEN 1 AND 9` ≡ `c >= 1 AND c <= 9` — a residual BETWEEN now binds
                                                 // (regression guard for the new Expr::Between arm) instead of Unsupported.
    match bind1(&parse_expr_sql("c BETWEEN 1 AND 9"), &schema).unwrap() {
        BoundExpr::BinOp(l, BinOp::And, r) => {
            assert!(matches!(*l, BoundExpr::BinOp(_, BinOp::Ge, _)));
            assert!(matches!(*r, BoundExpr::BinOp(_, BinOp::Le, _)));
        }
        other => panic!("expected And(Ge, Le), got {other:?}"),
    }
    // `c NOT BETWEEN 1 AND 9` ≡ NOT(c >= 1 AND c <= 9).
    match bind1(&parse_expr_sql("c NOT BETWEEN 1 AND 9"), &schema).unwrap() {
        BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
            assert!(matches!(*inner, BoundExpr::BinOp(_, BinOp::And, _)))
        }
        other => panic!("expected Not(And(..)), got {other:?}"),
    }
}

/// `t.x IS [NOT] NULL` on a *qualified* (CompoundIdentifier) column binds
/// like the unqualified form on every `bind_structural` surface — the unified
/// core reaches the shared null-test leaf, which a bare-`Identifier`-only arm
/// would reject with "IS NULL on non-column expression".
#[test]
fn test_compound_identifier_null_test_binds() {
    // Non-nullable column: folds to the constant (0 for IS NULL, 1 for IS NOT NULL).
    let nn = schema_with_val(TypeCode::I64); // (pk U64 NOT NULL, c I64 NOT NULL)
    assert!(matches!(
        bind1(&parse_expr_sql("t.c IS NULL"), &nn).unwrap(),
        BoundExpr::LitInt(0)
    ));
    assert!(matches!(
        bind1(&parse_expr_sql("t.c IS NOT NULL"), &nn).unwrap(),
        BoundExpr::LitInt(1)
    ));
    // Nullable column: a null test over the column reference.
    let nullable = Schema {
        columns: vec![col("pk", TypeCode::U64), ColumnDef::new("c", TypeCode::I64, true)],
        pk_cols: vec![0],
    };
    assert!(matches!(
        bind1(&parse_expr_sql("t.c IS NULL"), &nullable).unwrap(),
        BoundExpr::NullTest { inner: _, want_null: true }
    ));
    assert!(matches!(
        bind1(&parse_expr_sql("t.c IS NOT NULL"), &nullable).unwrap(),
        BoundExpr::NullTest { inner: _, want_null: false }
    ));
}

/// Every aggregate qualifier the binder does not implement must be rejected,
/// not silently dropped to the plain aggregate. Exercised through
/// `bind_single_table` so the guard's wiring into `bind_function` is covered,
/// not just the helper in isolation. DISTINCT is a qualifier the grouped binder
/// honours, so here it is the aggregate itself that is out of place.
#[test]
fn test_binder_rejects_aggregate_qualifiers() {
    let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
    for (src, want) in [
        ("COUNT(DISTINCT c)", "aggregate function not allowed"),
        ("SUM(c) FILTER (WHERE c > 0)", "FILTER"),
        ("SUM(c) OVER (PARTITION BY pk)", "OVER"),
    ] {
        assert_unsupported(bind1(&parse_expr_sql(src), &schema), want);
    }
}

/// `c IN (…)` with two or more items binds faithfully to an `InList` node
/// (un-desugared); a one-item list folds to the `Eq` it is; `NOT IN` wraps the
/// result in `Not`. The tested operand is bound once as `inner`; every list
/// item is bound in order into `items`. Lowering — not binding — chooses the
/// `IntInSet` fast path vs the OR-chain fallback.
#[test]
fn test_bind_in_list_folds_one_item_else_binds_faithful() {
    let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
    match bind1(&parse_expr_sql("c IN (1, 2)"), &schema).unwrap() {
        BoundExpr::InList { inner, items } => {
            assert!(matches!(*inner, BoundExpr::ColRef(1)));
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], BoundExpr::LitInt(1)));
            assert!(matches!(items[1], BoundExpr::LitInt(2)));
        }
        other => panic!("expected InList, got {other:?}"),
    }
    // A single element folds to the equality it spells — the shape the `access`
    // recognizers match on, identical to what `c = 7` binds to.
    match bind1(&parse_expr_sql("c IN (7)"), &schema).unwrap() {
        BoundExpr::BinOp(inner, BinOp::Eq, item) => {
            assert!(matches!(*inner, BoundExpr::ColRef(1)));
            assert!(matches!(*item, BoundExpr::LitInt(7)));
        }
        other => panic!("expected Eq, got {other:?}"),
    }
    // NOT IN wraps whichever node the arity picked.
    for (src, folded) in [("c NOT IN (7)", true), ("c NOT IN (1, 2)", false)] {
        match bind1(&parse_expr_sql(src), &schema).unwrap() {
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                assert_eq!(matches!(*inner, BoundExpr::BinOp(_, BinOp::Eq, _)), folded, "{src}")
            }
            other => panic!("{src}: expected Not(_), got {other:?}"),
        }
    }
    // sqlparser lexes the minus separately; the binder folds it into the
    // literal, so every consumer reads one constant shape.
    match bind1(&parse_expr_sql("c IN (-1, -2)"), &schema).unwrap() {
        BoundExpr::InList { items, .. } => {
            assert_eq!(items, vec![BoundExpr::LitInt(-1), BoundExpr::LitInt(-2)]);
        }
        other => panic!("expected InList, got {other:?}"),
    }
}

/// String and float list elements bind through the same literal leaves plain
/// `=` uses; an empty list is rejected (constructed directly — the parser
/// won't produce one).
#[test]
fn test_bind_in_list_string_float_and_empty() {
    let s = schema_with_val(TypeCode::String);
    assert!(bind1(&parse_expr_sql("c IN ('a', 'b')"), &s).is_ok());
    let f = schema_with_val(TypeCode::F64);
    assert!(bind1(&parse_expr_sql("c IN (1.5, 2.5)"), &f).is_ok());
    let empty = Expr::InList {
        expr: Box::new(parse_expr_sql("c")),
        list: vec![],
        negated: false,
    };
    assert_unsupported(bind1(&empty, &f), "empty list");
}

/// Subquery expressions get the targeted per-kind message from the default
/// `bind_subquery` leaf, not the generic catch-all. (The HIR view leaf
/// overrides `bind_subquery` to record the node for decorrelation instead.)
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
            bind1(&parse_expr_sql(src), &schema),
            "only supported in a single-table CREATE VIEW",
        );
    }
    assert_unsupported(
        bind1(&parse_expr_sql("c = ANY (SELECT c FROM t)"), &schema),
        "ANY/SOME/ALL",
    );
    assert_unsupported(
        bind1(&parse_expr_sql("c > ALL (SELECT c FROM t)"), &schema),
        "ANY/SOME/ALL",
    );
    assert_unsupported(
        bind1(&parse_expr_sql("(SELECT c FROM t) = 1"), &schema),
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
    match bind1(
        &parse_expr_sql("CASE WHEN c > 0 THEN 1 WHEN c < 0 THEN 2 ELSE 3 END"),
        &s,
    )
    .unwrap()
    {
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
    match bind1(&parse_expr_sql("CASE WHEN c > 0 THEN 1 END"), &s).unwrap() {
        BoundExpr::Case { else_, .. } => assert!(else_.is_none(), "missing ELSE → None"),
        other => panic!("expected Case, got {other:?}"),
    }
}

/// Simple-operand `CASE c WHEN v THEN … END` desugars each WHEN to `c = v`.
#[test]
fn test_bind_simple_case_desugars_to_operand_eq() {
    let s = nullable_schema(TypeCode::I64);
    match bind1(&parse_expr_sql("CASE c WHEN 1 THEN 10 WHEN 2 THEN 20 END"), &s).unwrap() {
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
        bind1(&parse_expr_sql("COALESCE(c)"), &s).unwrap(),
        BoundExpr::ColRef(1)
    ));
    // COALESCE(NULL, c) → c (a NULL literal contributes nothing).
    assert!(matches!(
        bind1(&parse_expr_sql("COALESCE(NULL, c)"), &s).unwrap(),
        BoundExpr::ColRef(1)
    ));
    // COALESCE(c, 0) → Case{[(c IS NOT NULL, c)], else: 0}.
    match bind1(&parse_expr_sql("COALESCE(c, 0)"), &s).unwrap() {
        BoundExpr::Case { branches, else_ } => {
            assert_eq!(branches.len(), 1);
            assert!(matches!(
                branches[0].0,
                BoundExpr::NullTest { inner: _, want_null: false }
            ));
            assert!(matches!(branches[0].1, BoundExpr::ColRef(1)));
            assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(0))));
        }
        other => panic!("expected Case, got {other:?}"),
    }
    // A NOT NULL column folds COALESCE(c, 0) to c directly (provably non-null).
    let nn = schema_with_val(TypeCode::I64);
    assert!(matches!(
        bind1(&parse_expr_sql("COALESCE(c, 0)"), &nn).unwrap(),
        BoundExpr::ColRef(1)
    ));
    // sqlparser accepts the empty argument list, so the arity check is what
    // rejects it — a bare NULL would silently type the whole expression.
    assert_unsupported(bind1(&parse_expr_sql("COALESCE()"), &s), "at least one argument");
}

/// `scalar_call` matches before the leaf ever sees a call, so a name in both
/// tables would shadow the aggregate in every binding context at once.
#[test]
fn no_scalar_call_name_is_also_an_aggregate_name() {
    for (name, _) in SCALAR_CALLS {
        assert!(
            crate::ast_util::agg_func_from_name(name).is_none(),
            "'{name}' is both a scalar call and an aggregate"
        );
    }
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
    match bind1(&parse_expr_sql("COALESCE(a, b, 0)"), &s).unwrap() {
        BoundExpr::Case { branches, else_ } => {
            assert!(matches!(
                branches[0].0,
                BoundExpr::NullTest { inner: _, want_null: false }
            ));
            match else_.as_deref() {
                Some(BoundExpr::Case { branches: inner, else_: inner_else }) => {
                    assert!(matches!(inner[0].0, BoundExpr::NullTest { inner: _, want_null: false }));
                    assert!(matches!(inner_else.as_deref(), Some(BoundExpr::LitInt(0))));
                }
                other => panic!("expected nested Case, got {other:?}"),
            }
        }
        other => panic!("expected Case, got {other:?}"),
    }
}

/// A computed COALESCE operand (`c + 1`) is null-tested over its own value: the
/// leaf declines it and the condition is a `NullTest` over the `BinOp`.
#[test]
fn test_bind_coalesce_computed_operand_tests_its_value() {
    let s = nullable_schema(TypeCode::I64);
    match bind1(&parse_expr_sql("COALESCE(c + 1, 0)"), &s).unwrap() {
        BoundExpr::Case { branches, else_ } => {
            assert_eq!(branches.len(), 1);
            match &branches[0].0 {
                BoundExpr::NullTest { inner, want_null: false } => {
                    assert!(matches!(**inner, BoundExpr::BinOp(_, BinOp::Add, _)));
                }
                other => panic!("expected NullTest over the sum, got {other:?}"),
            }
            assert!(matches!(branches[0].1, BoundExpr::BinOp(_, BinOp::Add, _)));
            assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(0))));
        }
        other => panic!("expected Case, got {other:?}"),
    }
}

/// `NULLIF(a, b)` → `CASE WHEN a = b THEN NULL ELSE a END`; wrong arity errors.
#[test]
fn test_bind_nullif() {
    let s = nullable_schema(TypeCode::I64);
    match bind1(&parse_expr_sql("NULLIF(c, 0)"), &s).unwrap() {
        BoundExpr::Case { branches, else_ } => {
            assert_eq!(branches.len(), 1);
            assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
            assert!(matches!(branches[0].1, BoundExpr::LitNull));
            assert!(matches!(else_.as_deref(), Some(BoundExpr::ColRef(1))));
        }
        other => panic!("expected Case, got {other:?}"),
    }
    // Both operands bind through bind_structural, so a computed operand is fine.
    assert!(bind1(&parse_expr_sql("NULLIF(c + 1, 0)"), &s).is_ok());
    // Wrong arity rejected.
    assert_unsupported(bind1(&parse_expr_sql("NULLIF(c)"), &s), "exactly two");
}

/// Only a grouped context admits an aggregate; the single-table leaf rejects
/// every well-formed one — including the accepted `ALL` qualifier — by name,
/// after the qualifier check has had its say.
#[test]
fn aggregates_are_rejected_outside_a_grouped_context() {
    let schema = schema_with_val(TypeCode::I64);
    for src in [
        "COUNT(*)",
        "COUNT(c)",
        "COUNT(ALL c)",
        "SUM(c)",
        "MIN(c)",
        "MAX(c)",
        "AVG(c)",
        "ABS(SUM(c))",
    ] {
        assert_unsupported(bind1(&parse_expr_sql(src), &schema), "aggregate function not allowed");
    }
}

// -----------------------------------------------------------------------
// Numeric scalar functions and numeric CAST
// -----------------------------------------------------------------------

fn bind_num(src: &str) -> Result<BoundExpr, GnitzSqlError> {
    bind1(&parse_expr_sql(src), &schema_with_val(TypeCode::I64))
}

/// The transcendental names and SIGN reach the unary node; POWER and POW are
/// the `Pow` operator.
#[test]
fn transcendental_names_bind_to_their_function() {
    for (src, want) in [
        ("SQRT(c)", NumFunc::Unary(FloatUnaryOp::Sqrt)),
        ("ln(c)", NumFunc::Unary(FloatUnaryOp::Ln)),
        ("LOG(c)", NumFunc::Unary(FloatUnaryOp::Log10)),
        ("EXP(c)", NumFunc::Unary(FloatUnaryOp::Exp)),
        ("SIGN(c)", NumFunc::Unary(FloatUnaryOp::Sign)),
    ] {
        match bind_num(src).unwrap() {
            BoundExpr::Func { f, .. } => assert_eq!(f, want, "{src}"),
            other => panic!("{src}: expected Func, got {other:?}"),
        }
    }
    for src in ["POWER(c, 2)", "pow(c, 2)"] {
        assert!(
            matches!(bind_num(src).unwrap(), BoundExpr::BinOp(_, BinOp::Pow, _)),
            "{src}"
        );
    }
    assert_unsupported(bind_num("POWER(c)"), "exactly two arguments");
    assert_unsupported(bind_num("LOG(c, 2)"), "exactly one argument");
}

fn assert_plan_err(r: Result<BoundExpr, GnitzSqlError>, want_substr: &str) {
    match r.unwrap_err() {
        GnitzSqlError::Plan(msg) => assert!(
            msg.contains(want_substr),
            "got Plan({msg:?}), expected to contain {want_substr:?}"
        ),
        e => panic!("expected Plan, got {e:?}"),
    }
}

/// Every unary numeric name binds to its `NumFunc`. `CEIL`/`FLOOR` arrive as
/// their own AST nodes and `CEILING` as a plain call — all three must land on
/// the same two IR nodes.
#[test]
fn unary_numeric_functions_bind_to_their_numfunc() {
    for (src, want) in [
        ("ABS(c)", NumFunc::Unary(FloatUnaryOp::Abs)),
        ("abs(c)", NumFunc::Unary(FloatUnaryOp::Abs)),
        ("CEIL(c)", NumFunc::Unary(FloatUnaryOp::Ceil)),
        ("CEILING(c)", NumFunc::Unary(FloatUnaryOp::Ceil)),
        ("FLOOR(c)", NumFunc::Unary(FloatUnaryOp::Floor)),
        ("TRUNC(c)", NumFunc::Unary(FloatUnaryOp::Trunc)),
        ("ROUND(c)", NumFunc::Unary(FloatUnaryOp::Round)),
        ("ROUND(c, 2)", NumFunc::Round(2)),
    ] {
        match bind_num(src).unwrap() {
            BoundExpr::Func { f, arg } => {
                assert_eq!(f, want, "{src}");
                assert!(matches!(*arg, BoundExpr::ColRef(1)), "{src}");
            }
            other => panic!("{src}: expected Func, got {other:?}"),
        }
    }
    assert_unsupported(bind_num("ABS(c, 1)"), "exactly one argument");
    assert_unsupported(bind_num("TRUNC(c, 2)"), "exactly one argument");
}

/// `CEIL(x TO DAY)` and `CEIL(x, 2)` parse into the same node with a
/// non-empty field; dropping it would silently compute plain `CEIL(x)`.
#[test]
fn ceil_floor_reject_the_field_carrying_forms() {
    assert_unsupported(bind_num("CEIL(c TO DAY)"), "CEIL");
    assert_unsupported(bind_num("FLOOR(c TO DAY)"), "FLOOR");
    assert_unsupported(bind_num("CEIL(c, 2)"), "CEIL");
}

#[test]
fn round_scale_must_be_a_small_integer_literal() {
    for (src, want) in [("ROUND(c, 2)", 2i8), ("ROUND(c, -2)", -2), ("ROUND(c, +15)", 15)] {
        match bind_num(src).unwrap() {
            BoundExpr::Func { f: NumFunc::Round(n), .. } => assert_eq!(n, want, "{src}"),
            other => panic!("{src}: expected Round, got {other:?}"),
        }
    }
    for src in ["ROUND(c, 16)", "ROUND(c, -16)", "ROUND(c, 2.5)", "ROUND(c, c)"] {
        assert_plan_err(bind_num(src), "scale must be an integer literal");
    }
    assert_unsupported(bind_num("ROUND(c, 1, 2)"), "one or two arguments");
}

/// MOD is a pure desugar onto `%`, so it inherits `IntArith`'s `Mod` total semantics
/// (zero divisor NULLs the row) with no opcode of its own.
#[test]
fn mod_desugars_to_the_modulo_binop() {
    match bind_num("MOD(c, 2)").unwrap() {
        BoundExpr::BinOp(l, BinOp::Mod, r) => {
            assert!(matches!(*l, BoundExpr::ColRef(1)));
            assert!(matches!(*r, BoundExpr::LitInt(2)));
        }
        other => panic!("expected BinOp(Mod), got {other:?}"),
    }
    assert_unsupported(bind_num("MOD(c)"), "exactly two arguments");
}

/// GREATEST/LEAST keep every argument as written — no literal or column
/// restriction, and no null-test rewrite: NULL skipping is the opcode's.
#[test]
fn greatest_least_bind_n_ary_with_computed_args() {
    match bind_num("GREATEST(c, c + 1, -1, NULL)").unwrap() {
        BoundExpr::MinMaxN { is_max, args } => {
            assert!(is_max);
            assert_eq!(args.len(), 4);
            assert!(matches!(args[1], BoundExpr::BinOp(_, BinOp::Add, _)));
            assert_eq!(args[2], BoundExpr::LitInt(-1));
            assert!(matches!(args[3], BoundExpr::LitNull));
        }
        other => panic!("expected MinMaxN, got {other:?}"),
    }
    assert!(matches!(
        bind_num("LEAST(c)").unwrap(),
        BoundExpr::MinMaxN { is_max: false, .. }
    ));
}

/// The `NULL` literal is an ordinary value, not a GREATEST/LEAST special
/// case: it binds wherever a literal does — under `IS NULL` too, as a null
/// test over the literal's value.
#[test]
fn null_literal_binds_wherever_a_literal_does() {
    for src in [
        "NULL",
        "CASE WHEN c > 0 THEN 1 ELSE NULL END",
        "CASE WHEN c > 0 THEN NULL ELSE 1 END",
        "GREATEST(c, NULL)",
        "COALESCE(NULL, c)",
        "CAST(NULL AS BIGINT)",
        "ABS(NULL)",
    ] {
        assert!(bind_num(src).is_ok(), "expected {src} to bind");
    }
    assert!(matches!(bind_num("NULL").unwrap(), BoundExpr::LitNull));
    // COALESCE folds a leading NULL away rather than making it the result.
    assert!(matches!(bind_num("COALESCE(NULL, c)").unwrap(), BoundExpr::ColRef(1)));
    // A NULL literal's null test is settled at bind time, as a literal's is.
    assert!(matches!(bind_num("NULL IS NULL").unwrap(), BoundExpr::LitInt(1)));
    assert!(matches!(bind_num("NULL IS NOT NULL").unwrap(), BoundExpr::LitInt(0)));
    assert!(matches!(bind_num("1 IS NULL").unwrap(), BoundExpr::LitInt(0)));
}

/// All four cast kinds mean the same thing here — a failed cast is a NULL,
/// which is what TRY_CAST/SAFE_CAST are documented to do.
#[test]
fn every_cast_kind_binds_to_one_node() {
    for src in [
        "CAST(c AS BIGINT)",
        "c::BIGINT",
        "TRY_CAST(c AS BIGINT)",
        "SAFE_CAST(c AS BIGINT)",
    ] {
        match bind_num(src).unwrap() {
            BoundExpr::Cast { expr, to } => {
                assert_eq!(to, TypeCode::I64.into(), "{src}");
                assert!(matches!(*expr, BoundExpr::ColRef(1)), "{src}");
            }
            other => panic!("{src}: expected Cast, got {other:?}"),
        }
    }
}

#[test]
fn cast_accepts_every_numeric_target_and_rejects_the_rest() {
    for (src, want) in [
        ("CAST(c AS TINYINT)", TypeCode::I8),
        ("CAST(c AS SMALLINT)", TypeCode::I16),
        ("CAST(c AS INT)", TypeCode::I32),
        ("CAST(c AS TINYINT UNSIGNED)", TypeCode::U8),
        ("CAST(c AS INT UNSIGNED)", TypeCode::U32),
        ("CAST(c AS BIGINT UNSIGNED)", TypeCode::U64),
        ("CAST(c AS FLOAT)", TypeCode::F32),
        ("CAST(c AS DOUBLE)", TypeCode::F64),
        ("CAST(c AS REAL)", TypeCode::F64),
    ] {
        match bind_num(src).unwrap() {
            BoundExpr::Cast { to, .. } => assert_eq!(to, want.into(), "{src}"),
            other => panic!("{src}: expected Cast, got {other:?}"),
        }
    }
    // STRING is a cast target too — the VM has a string register class.
    for src in ["CAST(c AS TEXT)", "CAST(c AS VARCHAR(10))", "CAST(c AS CHAR(4))"] {
        match bind_num(src).unwrap() {
            BoundExpr::Cast { to, .. } => assert_eq!(to, TypeCode::String.into(), "{src}"),
            other => panic!("{src}: expected Cast, got {other:?}"),
        }
    }
    // The 16-byte integer-ish targets have no register at all.
    for src in ["CAST(c AS UUID)", "CAST(c AS DECIMAL(38,0))"] {
        assert_unsupported(bind_num(src), "is not supported");
    }
    // BOOLEAN has no gnitz type at all, so it rejects one level earlier.
    assert!(bind_num("CAST(c AS BOOLEAN)").is_err());
    assert_unsupported(bind_num("CAST(c AS INT ARRAY)"), "ARRAY");
}

/// The shared qualifier inventory applies to the new names too — a dropped
/// `FILTER`/`OVER`/`DISTINCT` would compute the plain call.
#[test]
fn scalar_functions_reject_call_qualifiers() {
    assert_unsupported(bind_num("ABS(DISTINCT c)"), "DISTINCT");
    assert_unsupported(bind_num("ABS(c) OVER ()"), "OVER");
    assert_unsupported(bind_num("GREATEST(c) FILTER (WHERE c > 0)"), "FILTER");
}

/// Bind against a schema whose `c` is a STRING, for the string surface.
fn bind_str(src: &str) -> Result<BoundExpr, GnitzSqlError> {
    bind1(&parse_expr_sql(src), &schema_with_val(TypeCode::String))
}

/// The `(pattern, escape, ci)` a LIKE bound to, unwrapping a `NOT` if one is
/// there.
fn like_parts(src: &str) -> (String, Option<u8>, bool, bool) {
    let (e, negated) = match bind_str(src).unwrap() {
        BoundExpr::UnaryOp(UnaryOp::Not, inner) => (*inner, true),
        other => (other, false),
    };
    match e {
        BoundExpr::Like { pattern, escape, ci, .. } => (pattern, escape, ci, negated),
        other => panic!("expected Like, got {other:?}"),
    }
}

#[test]
fn like_binds_its_pattern_escape_and_case_folding() {
    assert_eq!(like_parts("c LIKE 'a%'"), ("a%".to_string(), Some(b'\\'), false, false));
    assert_eq!(like_parts("c ILIKE 'a%'"), ("a%".to_string(), Some(b'\\'), true, false));
    assert_eq!(
        like_parts("c NOT LIKE 'a%'"),
        ("a%".to_string(), Some(b'\\'), false, true)
    );
    assert_eq!(
        like_parts("c NOT ILIKE 'a%'"),
        ("a%".to_string(), Some(b'\\'), true, true)
    );
    // `ESCAPE ''` disables escaping; any other single ASCII byte overrides.
    assert_eq!(
        like_parts(r"c LIKE 'a\%' ESCAPE ''"),
        (r"a\%".to_string(), None, false, false)
    );
    assert_eq!(
        like_parts("c LIKE 'a!%' ESCAPE '!'"),
        ("a!%".to_string(), Some(b'!'), false, false)
    );
    // Parentheses around the literal are peeled, as they are in an operand
    // position.
    assert_eq!(like_parts("c LIKE ('a%')").0, "a%");
}

#[test]
fn like_rejects_what_it_cannot_bake_in() {
    assert_unsupported(bind_str("c LIKE c"), "LIKE pattern must be a string literal");
    assert_unsupported(bind_str("c LIKE NULL"), "LIKE pattern must be a string literal");
    assert_unsupported(bind_str("c LIKE 1"), "LIKE pattern must be a string literal");
    assert_unsupported(bind_str("c LIKE ANY ('a%')"), "LIKE ANY is not supported");
    // Two characters, non-ASCII, and NUL are all rejected escapes.
    for esc in ["'ab'", "'é'", "'\0'", "1"] {
        assert_unsupported(bind_str(&format!("c LIKE 'a' ESCAPE {esc}")), "ESCAPE must be a single");
    }
}

/// A pattern ending in a *live* escape is rejected; one whose trailing
/// escape is itself escaped is legal, and only the tokenizer walk tells
/// them apart.
#[test]
fn like_rejects_a_pattern_ending_in_a_live_escape() {
    match bind_str(r"c LIKE 'ab\'").unwrap_err() {
        GnitzSqlError::Plan(msg) => assert_eq!(msg, "LIKE pattern must not end with escape character"),
        e => panic!("expected Plan, got {e:?}"),
    }
    assert_eq!(like_parts(r"c LIKE 'ab\\'").0, r"ab\\");
    // With escaping disabled the byte is ordinary.
    assert_eq!(like_parts(r"c LIKE 'ab\' ESCAPE ''").0, r"ab\");
}

/// Every string function name reaches the same IR node, whichever of its
/// spellings is written. `LENGTH` and its two SQL-standard aliases must land
/// on the *character* measure and `OCTET_LENGTH` on the byte one — swapping
/// them is invisible until a multibyte value shows up.
#[test]
fn string_function_names_bind_to_their_measure_and_transform() {
    for (src, want) in [
        ("UPPER(c)", StrFunc::Upper),
        ("lower(c)", StrFunc::Lower),
        ("LENGTH(c)", StrFunc::LenChars),
        ("CHAR_LENGTH(c)", StrFunc::LenChars),
        ("character_length(c)", StrFunc::LenChars),
        ("OCTET_LENGTH(c)", StrFunc::LenBytes),
        ("REVERSE(c)", StrFunc::Reverse),
        ("LEFT(c, 2)", StrFunc::Left),
        ("right(c, 2)", StrFunc::Right),
        ("STRPOS(c, 'x')", StrFunc::Pos),
        ("REPLACE(c, 'a', 'b')", StrFunc::Replace),
        ("LPAD(c, 5)", StrFunc::Lpad),
        ("RPAD(c, 5, 'ab')", StrFunc::Rpad),
        ("SPLIT_PART(c, ',', 2)", StrFunc::SplitPart),
    ] {
        match bind_str(src).unwrap() {
            BExpr::StrCall { f, args } => {
                assert_eq!(f, want, "{src}");
                assert_eq!(args.len(), f.signature().len(), "{src}: sized by the signature");
            }
            other => panic!("{src}: expected StrCall, got {other:?}"),
        }
    }
    for src in ["UPPER()", "UPPER(c, c)", "LENGTH()"] {
        assert_unsupported(bind_str(src), "exactly one argument");
    }
    assert_unsupported(bind_str("LEFT(c)"), "exactly two arguments");
    assert_unsupported(bind_str("REPLACE(c, 'a')"), "exactly three arguments");
    assert_unsupported(bind_str("LPAD(c)"), "two or three arguments");
    assert_unsupported(bind_str("RPAD(c, 1, 'x', 'y')"), "two or three arguments");
}

/// Every string function's spelling reads back out of the name table, and
/// re-binds to the same function — the two directions of one map.
#[test]
fn every_string_function_has_a_spelling_that_binds_back() {
    for f in [
        StrFunc::Upper,
        StrFunc::Lower,
        StrFunc::LenBytes,
        StrFunc::LenChars,
        StrFunc::Reverse,
        StrFunc::Left,
        StrFunc::Right,
        StrFunc::Pos,
        StrFunc::Replace,
        StrFunc::Lpad,
        StrFunc::Rpad,
        StrFunc::SplitPart,
    ] {
        let name = str_func_name(f);
        let args = ["c", "c, 1", "c, c", "c, 'a', 'b'", "c, 1, 'x'", "c, ',', 1"]
            .into_iter()
            .find(|a| a.split(',').count() == f.signature().len() && (f != StrFunc::Pos || *a == "c, c"))
            .unwrap();
        let src = format!("{name}({args})");
        match bind_str(&src).unwrap() {
            BExpr::StrCall { f: bound, .. } => assert_eq!(bound, f, "{src}"),
            other => panic!("{src}: expected StrCall, got {other:?}"),
        }
    }
}

/// `LPAD`/`RPAD` without a fill carry one space as their third argument, so
/// the node always holds the full signature.
#[test]
fn pad_without_a_fill_defaults_to_one_space() {
    match bind_str("LPAD(c, 5)").unwrap() {
        BExpr::StrCall { args, .. } => assert_eq!(args[2], BoundExpr::LitStr(" ".into())),
        other => panic!("expected StrCall, got {other:?}"),
    }
}

/// `POSITION(needle IN hay)` is `STRPOS(hay, needle)`: the same node with the
/// arguments swapped into call order.
#[test]
fn position_binds_as_strpos_with_swapped_arguments() {
    match bind_str("POSITION('x' IN c)").unwrap() {
        BExpr::StrCall { f: StrFunc::Pos, args } => {
            assert!(matches!(args[0], BoundExpr::ColRef(1)));
            assert_eq!(args[1], BoundExpr::LitStr("x".into()));
        }
        other => panic!("expected STRPOS, got {other:?}"),
    }
}

/// `IF(c, a, b)` is a one-branch CASE; `IFNULL`/`NVL` are two-argument COALESCE.
#[test]
fn if_ifnull_and_nvl_desugar_onto_case_and_coalesce() {
    let s = nullable_schema(TypeCode::I64);
    match bind1(&parse_expr_sql("IF(c > 1, 10, 20)"), &s).unwrap() {
        BoundExpr::Case { branches, else_ } => {
            assert_eq!(branches.len(), 1);
            assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Gt, _)));
            assert!(matches!(branches[0].1, BoundExpr::LitInt(10)));
            assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(20))));
        }
        other => panic!("expected Case, got {other:?}"),
    }
    for src in ["IFNULL(c, 0)", "NVL(c, 0)"] {
        let coalesce = bind1(&parse_expr_sql("COALESCE(c, 0)"), &s).unwrap();
        assert_eq!(bind1(&parse_expr_sql(src), &s).unwrap(), coalesce, "{src}");
    }
    assert_unsupported(bind1(&parse_expr_sql("IFNULL(c, 0, 1)"), &s), "exactly two arguments");
    assert_unsupported(bind1(&parse_expr_sql("IF(c, 1)"), &s), "exactly three arguments");
}

/// `a IS [NOT] DISTINCT FROM b` is one definite CASE for both polarities: with
/// a NULL on either side the null flags are compared with the same operator
/// as the values. A never-null operand folds its null test to a constant, as
/// any null test does, and two never-null operands are the plain comparison.
#[test]
fn is_distinct_from_desugars_to_a_definite_case() {
    let s = nullable_schema(TypeCode::I64);
    for (src, op) in [
        ("c IS DISTINCT FROM pk", BinOp::Ne),
        ("c IS NOT DISTINCT FROM pk", BinOp::Eq),
    ] {
        match bind1(&parse_expr_sql(src), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 1, "{src}");
                // `c IS NULL` over the nullable column is a real test; `pk IS
                // NULL` over the never-null PK folds to 0.
                let (cond, then) = &branches[0];
                assert!(
                    matches!(cond, BoundExpr::BinOp(a, BinOp::Or, b)
                        if matches!(**a, BoundExpr::NullTest { want_null: true, .. }) && matches!(**b, BoundExpr::LitInt(0))),
                    "{src}: {cond:?}"
                );
                assert!(matches!(then, BoundExpr::BinOp(_, o, _) if *o == op), "{src}: {then:?}");
                assert!(
                    matches!(else_.as_deref(), Some(BoundExpr::BinOp(_, o, _)) if *o == op),
                    "{src}"
                );
            }
            other => panic!("{src}: expected Case, got {other:?}"),
        }
    }
    assert!(matches!(
        bind1(&parse_expr_sql("pk IS DISTINCT FROM 1"), &s).unwrap(),
        BoundExpr::BinOp(_, BinOp::Ne, _)
    ));
}

/// `IS NULL` over anything but a column is a null test over the value.
#[test]
fn null_test_over_a_computed_operand_tests_its_value() {
    let s = nullable_schema(TypeCode::I64);
    match bind1(&parse_expr_sql("(c + 1) IS NOT NULL"), &s).unwrap() {
        BoundExpr::NullTest { inner, want_null: false } => {
            assert!(matches!(*inner, BoundExpr::BinOp(_, BinOp::Add, _)));
        }
        other => panic!("expected NullTest, got {other:?}"),
    }
    // A name that does not resolve is still that error, not a null test over
    // nothing.
    assert!(bind1(&parse_expr_sql("nope IS NULL"), &s).is_err());
}

/// The full TRIM syntax matrix collapses to `(mode, set)`. The keyword form
/// and the `LTRIM`/`RTRIM` calls must agree, since they lower identically.
#[test]
fn trim_syntax_matrix_collapses_to_a_mode_and_a_byte_set() {
    for (src, mode, set) in [
        ("TRIM(c)", TrimMode::Both, " "),
        ("TRIM(BOTH c)", TrimMode::Both, " "),
        ("TRIM(LEADING c)", TrimMode::Leading, " "),
        ("TRIM(TRAILING c)", TrimMode::Trailing, " "),
        ("TRIM(LEADING 'xy' FROM c)", TrimMode::Leading, "xy"),
        ("TRIM(TRAILING 'xy' FROM c)", TrimMode::Trailing, "xy"),
        ("TRIM('xy' FROM c)", TrimMode::Both, "xy"),
        ("LTRIM(c)", TrimMode::Leading, " "),
        ("RTRIM(c)", TrimMode::Trailing, " "),
        ("LTRIM(c, 'xy')", TrimMode::Leading, "xy"),
        ("RTRIM(c, 'xy')", TrimMode::Trailing, "xy"),
    ] {
        match bind_str(src).unwrap() {
            BExpr::TrimCall { mode: m, set: st, .. } => assert_eq!((m, st.as_str()), (mode, set), "{src}"),
            other => panic!("{src}: expected TrimCall, got {other:?}"),
        }
    }
}

/// The trim set is compile-time data the engine bakes into a membership
/// table, so it must be a literal — and ASCII, which is what keeps a
/// byte-wise strip from splitting a UTF-8 sequence.
#[test]
fn trim_set_must_be_an_ascii_literal() {
    for src in ["TRIM(c FROM c)", "LTRIM(c, c)", "TRIM('ä' FROM c)", "TRIM(NULL FROM c)"] {
        assert_unsupported(bind_str(src), "ASCII string literal");
    }
    // Parentheses around the literal are peeled, as they are in an operand
    // position.
    match bind_str("LTRIM(c, ('ab'))").unwrap() {
        BoundExpr::TrimCall { set, .. } => assert_eq!(set, "ab"),
        other => panic!("expected TrimCall, got {other:?}"),
    }
}

/// `SUBSTR` and `SUBSTRING`, the `FROM/FOR` form and the comma form, all
/// arrive as one AST node; an absent FROM starts the window at 1.
#[test]
fn substring_spellings_bind_to_one_node() {
    for src in [
        "SUBSTRING(c FROM 2 FOR 3)",
        "SUBSTRING(c, 2, 3)",
        "SUBSTR(c, 2, 3)",
        "SUBSTR(c FROM 2 FOR 3)",
    ] {
        match bind_str(src).unwrap() {
            BExpr::Substr { start, len, .. } => {
                assert!(matches!(*start, BExpr::LitInt(2)), "{src}");
                assert!(matches!(len.as_deref(), Some(BExpr::LitInt(3))), "{src}");
            }
            other => panic!("{src}: expected Substr, got {other:?}"),
        }
    }
    match bind_str("SUBSTRING(c)").unwrap() {
        BExpr::Substr { start, len, .. } => {
            assert!(matches!(*start, BExpr::LitInt(1)), "an absent FROM starts at 1");
            assert!(len.is_none());
        }
        other => panic!("expected Substr, got {other:?}"),
    }
}

#[test]
fn concat_binds_any_arity_and_the_operator_maps_to_its_own_binop() {
    match bind_str("CONCAT(c, 'x', 42)").unwrap() {
        BExpr::ConcatN { args } => assert_eq!(args.len(), 3),
        other => panic!("expected ConcatN, got {other:?}"),
    }
    assert!(matches!(bind_str("CONCAT(c)").unwrap(), BExpr::ConcatN { .. }));
    assert_unsupported(bind_str("CONCAT()"), "at least one argument");
    assert!(matches!(
        bind_str("c || 'x'").unwrap(),
        BExpr::BinOp(_, BinOp::Concat, _)
    ));
}

/// The walkers see through the two keyword-dispatched nodes. `EXCLUDED` is
/// the observable: a reference the walk cannot reach is one the `EXCLUDED`
/// guard and the aggregate collectors would silently miss.
#[test]
fn expr_operands_reaches_inside_substring_and_trim() {
    use crate::ast_util::expr_operands;
    for src in [
        "SUBSTRING(EXCLUDED.c FROM 1)",
        "SUBSTRING(c FROM EXCLUDED.n)",
        "SUBSTRING(c FROM 1 FOR EXCLUDED.n)",
        "TRIM(EXCLUDED.c)",
        "TRIM('x' FROM EXCLUDED.c)",
    ] {
        let e = parse_expr_sql(src);
        let found = expr_operands(&e).iter().any(|o| format!("{o}").contains("EXCLUDED"));
        assert!(found, "{src}: the walker must reach the EXCLUDED reference");
    }
}
