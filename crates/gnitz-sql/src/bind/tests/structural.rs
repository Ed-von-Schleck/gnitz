use super::*;
use crate::hir::bind_single_table;
use crate::ir::{BoundExpr, NumLit};
use crate::test_support::{bind_sql, col, ncol, parse_expr_sql, rejected};
use gnitz_wire::TypeCode;

/// `(pk U64, c I64 nullable, n I64 NOT NULL)`. A function's operand types are
/// lowering's to check, so one schema serves the numeric and the string surface
/// alike.
fn schema() -> gnitz_core::Schema {
    crate::test_support::schema(
        vec![
            col("pk", TypeCode::U64),
            ncol("c", TypeCode::I64),
            col("n", TypeCode::I64),
        ],
        &[0],
    )
}

fn b(src: &str) -> BoundExpr {
    bind_sql(src, &schema()).unwrap_or_else(|e| panic!("{src}: {e}"))
}

fn b_err(src: &str) -> String {
    rejected(bind_sql(src, &schema()))
}

const C: BoundExpr = BoundExpr::ColRef(1);

fn bx(e: BoundExpr) -> Box<BoundExpr> {
    Box::new(e)
}

/// Every desugar and alias binds to exactly the tree its written-out form
/// binds to, so a desugar that swaps or drops an operand shows up here.
#[test]
fn each_sugar_binds_to_the_tree_its_plain_spelling_binds_to() {
    for (sugar, plain) in [
        ("c BETWEEN 1 AND 9", "c >= 1 AND c <= 9"),
        ("c NOT BETWEEN 1 AND 9", "NOT (c >= 1 AND c <= 9)"),
        // A one-item list is the equality the access recognizers match on.
        ("c IN (7)", "c = 7"),
        ("c NOT IN (7)", "NOT (c = 7)"),
        ("c NOT IN (1, 2)", "NOT (c IN (1, 2))"),
        (
            "CASE c WHEN 1 THEN 10 WHEN 2 THEN 20 END",
            "CASE WHEN c = 1 THEN 10 WHEN c = 2 THEN 20 END",
        ),
        ("NULLIF(c, 0)", "CASE WHEN c = 0 THEN NULL ELSE c END"),
        // The IS tests take a NULL to a definite answer: neither `x` nor
        // `NOT x` is taken over one.
        ("(c > 1) IS TRUE", "CASE WHEN c > 1 THEN TRUE ELSE FALSE END"),
        ("(c > 1) IS NOT TRUE", "CASE WHEN c > 1 THEN FALSE ELSE TRUE END"),
        ("(c > 1) IS FALSE", "CASE WHEN NOT (c > 1) THEN TRUE ELSE FALSE END"),
        ("(c > 1) IS NOT FALSE", "CASE WHEN NOT (c > 1) THEN FALSE ELSE TRUE END"),
        ("(c > 1) IS UNKNOWN", "(c > 1) IS NULL"),
        ("(n > 1) IS NOT UNKNOWN", "TRUE"),
        // A literal cast to BOOLEAN is the literal.
        ("CAST('yes' AS BOOLEAN)", "TRUE"),
        ("CAST(0 AS BOOLEAN)", "FALSE"),
        ("CAST(-3 AS BOOLEAN)", "TRUE"),
        ("IF(c > 1, 10, 20)", "CASE WHEN c > 1 THEN 10 ELSE 20 END"),
        // COALESCE stops at the first operand provably never NULL and skips a NULL one.
        ("COALESCE(c)", "c"),
        ("COALESCE(NULL, c)", "c"),
        ("COALESCE(n, c)", "n"),
        ("COALESCE(c, 0)", "CASE WHEN c IS NOT NULL THEN c ELSE 0 END"),
        (
            "COALESCE(c + 1, 0)",
            "CASE WHEN (c + 1) IS NOT NULL THEN c + 1 ELSE 0 END",
        ),
        ("COALESCE(c, c, 0)", "COALESCE(c, COALESCE(c, 0))"),
        ("IFNULL(c, 0)", "COALESCE(c, 0)"),
        ("NVL(c, 0)", "COALESCE(c, 0)"),
        (
            "c IS DISTINCT FROM pk",
            "CASE WHEN c IS NULL OR pk IS NULL THEN (c IS NULL) <> (pk IS NULL) ELSE c <> pk END",
        ),
        (
            "c IS NOT DISTINCT FROM pk",
            "CASE WHEN c IS NULL OR pk IS NULL THEN (c IS NULL) = (pk IS NULL) ELSE c = pk END",
        ),
        ("pk IS DISTINCT FROM 1", "pk <> 1"),
        // A null test whose answer is settled at bind time is that literal.
        ("pk IS NULL", "FALSE"),
        ("t.n IS NOT NULL", "TRUE"),
        ("n + 1 IS NULL", "FALSE"),
        ("NULL IS NULL", "TRUE"),
        ("NULL IS NOT NULL", "FALSE"),
        ("1 IS NULL", "FALSE"),
        ("T.c IS NULL", "c IS NULL"),
        ("MOD(c, 2)", "c % 2"),
        ("pow(c, 2)", "POWER(c, 2)"),
        ("CEILING(c)", "CEIL(c)"),
        ("POSITION('x' IN c)", "STRPOS(c, 'x')"),
        ("SUBSTRING(c FROM 2 FOR 3)", "SUBSTR(c, 2, 3)"),
        ("SUBSTRING(c, 2, 3)", "SUBSTR(c, 2, 3)"),
        ("SUBSTR(c FROM 2 FOR 3)", "SUBSTR(c, 2, 3)"),
        ("SUBSTRING(c)", "SUBSTR(c, 1)"),
        ("LPAD(c, 5)", "LPAD(c, 5, ' ')"),
        ("RPAD(c, 5)", "RPAD(c, 5, ' ')"),
        ("TRIM(c)", "TRIM(BOTH ' ' FROM c)"),
        ("TRIM(BOTH c)", "TRIM(BOTH ' ' FROM c)"),
        ("TRIM('xy' FROM c)", "TRIM(BOTH 'xy' FROM c)"),
        ("TRIM(LEADING c)", "TRIM(LEADING ' ' FROM c)"),
        ("TRIM(TRAILING c)", "TRIM(TRAILING ' ' FROM c)"),
        ("LTRIM(c)", "TRIM(LEADING ' ' FROM c)"),
        ("RTRIM(c)", "TRIM(TRAILING ' ' FROM c)"),
        ("LTRIM(c, 'xy')", "TRIM(LEADING 'xy' FROM c)"),
        ("RTRIM(c, ('xy'))", "TRIM(TRAILING 'xy' FROM c)"),
        ("DATE '2020-01-01'", "CAST('2020-01-01' AS DATE)"),
        ("c::BIGINT", "CAST(c AS BIGINT)"),
        ("TRY_CAST(c AS BIGINT)", "CAST(c AS BIGINT)"),
        ("SAFE_CAST(c AS BIGINT)", "CAST(c AS BIGINT)"),
        ("c NOT ILIKE 'a%'", "NOT (c ILIKE 'a%')"),
        ("c LIKE ('a%')", "c LIKE 'a%'"),
        ("EXTRACT(years FROM c)", "EXTRACT(YEAR FROM c)"),
        ("DATE_PART('Year', c)", "EXTRACT(YEAR FROM c)"),
    ] {
        assert_eq!(b(sugar), b(plain), "{sugar} ≡ {plain}");
    }
}

/// The ground rows under the equivalences above, so both sides of one cannot
/// be wrong together.
#[test]
fn plain_forms_bind_to_their_node() {
    use BoundExpr as E;
    let lit_str = |s: &str| E::LitStr(s.into());
    for (src, want) in [
        ("c IS NULL", E::NullTest { inner: bx(C), want_null: true }),
        (
            "c IN (-1, 2)",
            E::InList {
                inner: bx(C),
                items: vec![E::LitInt(-1), E::LitInt(2)],
            },
        ),
        (
            "CASE WHEN c > 0 THEN 1 ELSE 3 END",
            E::Case {
                branches: vec![(E::bin(C, BinOp::Gt, E::LitInt(0)), E::LitInt(1))],
                else_: bx(E::LitInt(3)),
            },
        ),
        (
            "CASE WHEN c > 0 THEN 1 END",
            E::Case {
                branches: vec![(E::bin(C, BinOp::Gt, E::LitInt(0)), E::LitInt(1))],
                else_: bx(E::LitNull),
            },
        ),
        (
            "GREATEST(c, c + 1, -1, NULL)",
            E::MinMaxN {
                is_max: true,
                args: vec![C, E::bin(C, BinOp::Add, E::LitInt(1)), E::LitInt(-1), E::LitNull],
            },
        ),
        ("LEAST(c)", E::MinMaxN { is_max: false, args: vec![C] }),
        ("POWER(c, 2)", E::bin(C, BinOp::Pow, E::LitInt(2))),
        (
            "CONCAT(c, 'x', 42)",
            E::ConcatN {
                args: vec![C, lit_str("x"), E::LitInt(42)],
            },
        ),
        ("c || 'x'", E::bin(C, BinOp::Concat, lit_str("x"))),
        (
            "STRPOS(c, 'x')",
            E::StrCall {
                f: StrFunc::Pos,
                args: vec![C, lit_str("x")],
            },
        ),
        (
            "SUBSTR(c, 2, 3)",
            E::StrCall {
                f: StrFunc::Substr,
                args: vec![C, E::LitInt(2), E::LitInt(3)],
            },
        ),
        (
            "LPAD(c, 5, ' ')",
            E::StrCall {
                f: StrFunc::Lpad,
                args: vec![C, E::LitInt(5), lit_str(" ")],
            },
        ),
        (
            "TRIM(LEADING 'xy' FROM c)",
            E::TrimCall {
                s: bx(C),
                mode: TrimMode::Leading,
                set: "xy".into(),
            },
        ),
        ("CAST(c AS BIGINT)", E::Cast { expr: bx(C), to: TypeCode::I64.into() }),
        // A 16-byte target binds; it is the lowering that refuses it.
        ("CAST(c AS UUID)", E::Cast { expr: bx(C), to: TypeCode::UUID.into() }),
        ("EXTRACT(YEAR FROM c)", E::Calendar { op: CalendarOp::Year, arg: bx(C) }),
        (
            "DATE_TRUNC('month', c)",
            E::Calendar {
                op: CalendarOp::Month.trunc_of().unwrap(),
                arg: bx(C),
            },
        ),
        ("NULL", E::LitNull),
        ("ROUND(c, 2)", E::Func { f: NumFunc::Round(2), arg: bx(C) }),
        ("ROUND(c, -2)", E::Func { f: NumFunc::Round(-2), arg: bx(C) }),
        ("ROUND(c, +15)", E::Func { f: NumFunc::Round(15), arg: bx(C) }),
    ] {
        assert_eq!(b(src), want, "{src}");
    }
}

/// Every binary operator lands on its own `BinOp` with its operands in place.
#[test]
fn each_binary_operator_maps_to_its_binop() {
    for (op, want) in [
        ("+", BinOp::Add),
        ("-", BinOp::Sub),
        ("*", BinOp::Mul),
        ("/", BinOp::Div),
        ("%", BinOp::Mod),
        ("=", BinOp::Eq),
        ("<>", BinOp::Ne),
        ("!=", BinOp::Ne),
        (">", BinOp::Gt),
        (">=", BinOp::Ge),
        ("<", BinOp::Lt),
        ("<=", BinOp::Le),
        ("AND", BinOp::And),
        ("OR", BinOp::Or),
        ("||", BinOp::Concat),
    ] {
        assert_eq!(
            b(&format!("c {op} n")),
            BoundExpr::bin(C, want, BoundExpr::ColRef(2)),
            "{op}"
        );
    }
}

/// Every unary numeric name binds to its `NumFunc` over its one argument.
/// `CEIL`/`FLOOR` arrive as their own AST nodes, the rest as plain calls.
#[test]
fn unary_numeric_functions_bind_to_their_numfunc() {
    use FloatUnaryOp as F;
    for (src, op) in [
        ("ABS(c)", F::Abs),
        ("abs(c)", F::Abs),
        ("CEIL(c)", F::Ceil),
        ("FLOOR(c)", F::Floor),
        ("TRUNC(c)", F::Trunc),
        ("ROUND(c)", F::Round),
        ("ROUND(c, 0)", F::Round),
        ("SQRT(c)", F::Sqrt),
        ("ln(c)", F::Ln),
        ("LOG(c)", F::Log10),
        ("EXP(c)", F::Exp),
        ("SIGN(c)", F::Sign),
        ("-c", F::Neg),
    ] {
        assert_eq!(b(src), BoundExpr::Func { f: NumFunc::Unary(op), arg: bx(C) }, "{src}");
    }
}

/// Every string function name reaches its IR node, whichever spelling is
/// written, and the spelling `StrFunc::sql_name` gives binds to the same node.
/// `LENGTH` and its SQL-standard aliases must land on the *character* measure
/// and `OCTET_LENGTH` on the byte one — swapping them is invisible until a
/// multibyte value shows up.
#[test]
fn string_function_names_bind_to_their_function() {
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
        ("LPAD(c, 5, 'ab')", StrFunc::Lpad),
        ("RPAD(c, 5, 'ab')", StrFunc::Rpad),
        ("SPLIT_PART(c, ',', 2)", StrFunc::SplitPart),
        ("SUBSTR(c, 2, 3)", StrFunc::Substr),
    ] {
        let bound = b(src);
        assert!(
            matches!(bound, BoundExpr::StrCall { f, .. } if f == want),
            "{src}: {bound:?}"
        );
        let args = &src[src.find('(').unwrap()..];
        assert_eq!(b(&format!("{}{args}", want.sql_name())), bound, "{src}");
    }
}

/// The LIKE pattern is encoded under the escape the binder settled on: `\` by
/// default, none for `ESCAPE ''`, else the one character written — multi-byte
/// or NUL included.
#[test]
fn like_encodes_its_pattern_under_the_written_escape() {
    for (src, pattern, escape, ci) in [
        ("c LIKE 'a%'", "a%", Some('\\'), false),
        ("c ILIKE 'a%'", "a%", Some('\\'), true),
        (r"c LIKE 'ab\\'", r"ab\\", Some('\\'), false),
        (r"c LIKE 'a\%' ESCAPE ''", r"a\%", None, false),
        ("c LIKE 'a!%' ESCAPE '!'", "a!%", Some('!'), false),
        ("c LIKE 'aé%b' ESCAPE 'é'", "aé%b", Some('é'), false),
        ("c LIKE 'a\0%' ESCAPE '\0'", "a\0%", Some('\0'), false),
    ] {
        let want = BoundExpr::Like {
            s: bx(C),
            pattern: LikePattern::encode(pattern, escape).unwrap(),
            ci,
        };
        assert_eq!(b(src), want, "{src}");
    }
}

/// A minus over a literal folds into it at bind, keeping `LitWide`'s invariant
/// that its value does not fit `i64`.
#[test]
fn a_negated_literal_folds_and_keeps_the_wide_invariant() {
    assert_eq!(b("-9223372036854775808"), BExpr::LitInt(i64::MIN));
    assert_eq!(
        b("-(-9223372036854775808)"),
        BExpr::LitWide(NumLit { mag: 1 << 63, neg: false })
    );
    assert_eq!(
        b("-170141183460469231731687303715884105728"),
        BExpr::LitWide(NumLit { mag: 1 << 127, neg: true })
    );
    assert_eq!(b("-1.5"), BExpr::LitFloat { v: -1.5, dec: Some((-15, 1)) });
}

/// A constant position peels parentheses and signs, in any order, into the
/// literal they spell; anything else — including shapes the binder folds to a
/// literal — is refused.
#[test]
fn bind_constant_reads_a_literal_through_parens_and_signs() {
    let constant = |src| bind_constant(&parse_expr_sql(src));
    for (src, want) in [
        ("5", 5),
        ("+5", 5),
        ("-5", -5),
        ("((-5))", -5),
        ("(-(5))", -5),
        ("-(-5)", 5),
        ("-(+5)", -5),
        ("-0", 0),
    ] {
        assert_eq!(constant(src).unwrap(), BExpr::LitInt(want), "{src}");
    }
    // A sign over NULL is the NULL it spells, which is what an INSERT cell reads.
    for src in ["+NULL", "-NULL", "NULL"] {
        assert_eq!(constant(src).unwrap(), BExpr::LitNull, "{src}");
    }
    assert_eq!(constant("'abc'").unwrap(), BExpr::LitStr("abc".into()));
    assert_eq!(constant("CAST(-1 AS BOOLEAN)").unwrap(), BExpr::LitBool(true));
    assert_eq!(
        constant("DATE '2020-01-01'").unwrap(),
        BExpr::LitTemporal { tc: TypeCode::Date, v: 18262 }
    );
    for (src, want) in [
        ("-'abc'", "expected a constant"),
        ("+'abc'", "unary operator + not supported"),
        ("a", "expected a constant"),
        ("1 + 1", "expected a constant"),
        ("-(a)", "expected a constant"),
        ("ABS(1)", "expected a constant"),
        ("COALESCE(2, x)", "expected a constant"),
        ("NULL IS NULL", "expected a constant"),
        ("CAST(5 AS INT)", "expected a constant"),
    ] {
        let msg = rejected(constant(src));
        assert!(msg.contains(want), "{src}: {msg}");
    }
}

/// Every refusal names what it refuses — never a silently dropped qualifier,
/// argument or field.
#[test]
fn each_unsupported_form_is_rejected_by_name() {
    const AGG: &str = "aggregate functions are not allowed here";
    const WINDOW: &str = "window functions (OVER) are only supported";
    const SUBQUERY: &str = "only supported in a single-table CREATE VIEW";
    for (src, want) in [
        // Columns.
        ("nope", "column 'nope' not found"),
        ("nope IS NULL", "column 'nope' not found"),
        // An operand COALESCE folds away is still bound.
        ("COALESCE(pk, no_such_col)", "column 'no_such_col' not found"),
        ("COALESCE(1, FOO(c))", "function 'foo' not supported"),
        ("x.c", "table alias 'x' not found (the relation in scope is 't')"),
        ("a.b.c", "expected a column reference"),
        // Names.
        ("FOO(c)", "function 'foo' not supported"),
        ("RANDOM()", "RANDOM: a non-deterministic function is not supported"),
        ("now()", "NOW: a non-deterministic function is not supported"),
        // Only a grouped context admits an aggregate — `ALL` and `DISTINCT` included.
        ("COUNT(*)", AGG),
        ("COUNT(ALL c)", AGG),
        ("COUNT(DISTINCT c)", AGG),
        ("MIN(c)", AGG),
        ("ABS(SUM(c))", AGG),
        ("SUM(c) FILTER (WHERE c > 0)", "FILTER (WHERE …) is not supported"),
        ("SUM(c) OVER (PARTITION BY pk)", WINDOW),
        ("ABS(c) OVER ()", WINDOW),
        ("ABS(DISTINCT c)", "ABS: DISTINCT is not supported"),
        (
            "GREATEST(c) FILTER (WHERE c > 0)",
            "GREATEST: FILTER (WHERE …) is not supported",
        ),
        // Subqueries.
        ("EXISTS (SELECT c FROM t)", SUBQUERY),
        ("NOT EXISTS (SELECT c FROM t)", SUBQUERY),
        ("c IN (SELECT c FROM t)", SUBQUERY),
        ("c NOT IN (SELECT c FROM t)", SUBQUERY),
        ("c = 1 OR EXISTS (SELECT c FROM t)", SUBQUERY),
        ("c = ANY (SELECT c FROM t)", "ANY/SOME/ALL"),
        ("c > ALL (SELECT c FROM t)", "ANY/SOME/ALL"),
        ("(SELECT c FROM t) = 1", "scalar subqueries"),
        // Operators and forms. Unary `+` folds over a numeric literal only.
        ("+c", "unary operator + not supported"),
        ("+(c > 1)", "unary operator + not supported"),
        ("c & 1", "binary operator & not supported"),
        ("c IS UNKNOWN", "IS UNKNOWN takes a BOOLEAN"),
        ("CAST('maybe' AS BOOLEAN)", "invalid BOOLEAN literal: 'maybe'"),
        // Arity. A call reads its arguments by position, so an extra one would
        // be dropped silently.
        ("COALESCE()", "at least one argument"),
        ("CONCAT()", "at least one argument"),
        ("NULLIF(c)", "exactly two arguments"),
        ("IFNULL(c, 0, 1)", "exactly two arguments"),
        ("IF(c, 1)", "exactly three arguments"),
        ("POWER(c)", "exactly two arguments"),
        ("MOD(c)", "exactly two arguments"),
        ("LOG(c, 2)", "exactly one argument"),
        ("ABS(c, 1)", "exactly one argument"),
        ("TRUNC(c, 2)", "exactly one argument"),
        ("UPPER()", "exactly one argument"),
        ("UPPER(c, c)", "exactly one argument"),
        ("LEFT(c)", "exactly two arguments"),
        ("REPLACE(c, 'a')", "exactly three arguments"),
        ("LPAD(c)", "two or three arguments"),
        ("RPAD(c, 1, 'x', 'y')", "two or three arguments"),
        ("ROUND(c, 1, 2)", "one or two arguments"),
        // ROUND's scale, and the field CEIL/FLOOR would drop.
        ("ROUND(c, 16)", "scale must be an integer literal"),
        ("ROUND(c, -16)", "scale must be an integer literal"),
        ("ROUND(c, 2.5)", "scale must be an integer literal"),
        ("ROUND(c, c)", "scale must be an integer literal"),
        ("CEIL(c TO DAY)", "CEIL: only the plain CEIL(x) form is supported"),
        ("CEIL(c, 2)", "CEIL: only the plain CEIL(x) form is supported"),
        ("FLOOR(c TO DAY)", "FLOOR: only the plain FLOOR(x) form is supported"),
        // CAST and typed literals.
        ("CAST(c AS INT ARRAY)", "CAST: an ARRAY target type is not supported"),
        ("CAST(c AS INT FORMAT 'x')", "CAST: FORMAT is not supported"),
        ("CAST('2020-13-01' AS DATE)", "invalid DATE literal: '2020-13-01'"),
        ("DATE 5", "must be a single-quoted string"),
        ("DATE NULL", "must be a single-quoted string"),
        // Calendar units.
        (
            "EXTRACT(MILLISECOND FROM c)",
            "EXTRACT: field MILLISECOND is not supported",
        ),
        ("DATE_TRUNC('dow', c)", r#"DATE_TRUNC: unit "dow" is not supported"#),
        ("DATE_TRUNC(c, c)", "DATE_TRUNC: the unit must be a string literal"),
        // LIKE's pattern and escape are baked in, so each must be a literal.
        ("c LIKE c", "LIKE pattern must be a string literal"),
        ("c LIKE NULL", "LIKE pattern must be a string literal"),
        ("c LIKE 1", "LIKE pattern must be a string literal"),
        ("c LIKE ANY ('a%')", "LIKE: ANY is not supported"),
        ("c LIKE 'a' ESCAPE 'ab'", "ESCAPE must be a single"),
        ("c LIKE 'a' ESCAPE 1", "ESCAPE must be a single"),
        (r"c LIKE 'ab\'", "LIKE pattern must not end with escape character"),
        // The trim set is baked into a membership table, and must be ASCII so a
        // byte-wise strip cannot split a UTF-8 sequence.
        ("TRIM(c FROM c)", "ASCII string literal"),
        ("LTRIM(c, c)", "ASCII string literal"),
        ("TRIM('ä' FROM c)", "ASCII string literal"),
        ("TRIM(NULL FROM c)", "ASCII string literal"),
        ("TRIM(c, 'xy')", "TRIM: the (… , <characters>) form is not supported"),
    ] {
        let msg = b_err(src);
        assert!(msg.contains(want), "{src}: {msg}");
    }
    // The parser never produces an empty list; the binder still refuses one.
    let empty = Expr::InList {
        expr: Box::new(parse_expr_sql("c")),
        list: vec![],
        negated: false,
    };
    assert!(rejected(bind_single_table(&empty, &schema(), "t")).contains("empty list"));
}

/// `scalar_call` matches before the leaf ever sees a call, so a name in two
/// tables would silently bind as the scalar call in every context at once.
#[test]
fn every_structurally_bound_name_is_unique() {
    for (i, (name, _)) in SCALAR_CALLS.iter().enumerate() {
        assert!(
            SCALAR_CALLS[..i].iter().all(|(n, _)| !n.eq_ignore_ascii_case(name)),
            "'{name}' twice"
        );
        assert!(
            crate::agg::agg_func_from_name(name).is_none(),
            "'{name}' is an aggregate"
        );
        assert!(
            !VOLATILE_FNS.iter().any(|v| v.eq_ignore_ascii_case(name)),
            "'{name}' is volatile"
        );
    }
}
