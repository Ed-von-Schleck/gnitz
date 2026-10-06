use super::*;
use crate::test_support::{bind_sql, bind_where, col, ncol, schema};
use gnitz_core::Schema;
use gnitz_wire::ColumnDef;

/// `(pk | u U64 | u32 | i8 | i I64 | f F64 | f32 | s STRING | d DATE | ts TIMESTAMP
/// | p DECIMAL(·,2) | q DECIMAL(·,3))`, every payload column nullable.
fn typed() -> Schema {
    use TypeCode::*;
    let (t, dec) = (ColType::of, ColType::decimal);
    let payload = [
        ("u", t(U64)),
        ("u32", t(U32)),
        ("i8", t(I8)),
        ("i", t(I64)),
        ("f", t(F64)),
        ("f32", t(F32)),
        ("s", t(String)),
        ("d", t(Date)),
        ("ts", t(Timestamp)),
        ("p", dec(2)),
        ("q", dec(3)),
    ];
    let mut columns = vec![col("pk", U64)];
    columns.extend(payload.map(|(n, ty)| ColumnDef::typed(n, ty, true)));
    schema(columns, &[0])
}

/// Every node types as the value it computes.
#[test]
fn each_node_types_as_the_value_it_computes() {
    use TypeCode::*;
    let (t, dec) = (ColType::of, ColType::decimal);
    let s = typed();
    for (sql, want) in [
        // Literals. NULL is the blend's neutral I64; a wide literal a U64 register
        // holds is U64.
        ("NULL", t(I64)),
        ("1.5", t(F64)),
        ("'x'", t(String)),
        ("DATE '2020-01-01'", t(Date)),
        ("18446744073709551615", t(U64)),
        ("18446744073709551616", t(I64)),
        ("-18446744073709551616", t(I64)),
        // Booleans.
        ("u > u32", t(Bool)),
        ("TRUE", t(Bool)),
        ("u > 1 AND i < 2", t(Bool)),
        ("CASE WHEN TRUE THEN u > 1 END", t(Bool)),
        ("CAST(i AS BOOLEAN)", t(Bool)),
        ("NOT (u > 1)", t(Bool)),
        ("u IS NULL", t(Bool)),
        ("u IS NOT NULL", t(Bool)),
        ("u IN (1, 2)", t(Bool)),
        ("s LIKE 'a%'", t(Bool)),
        // Arithmetic keeps U64, so a materialized column re-seeds a downstream
        // unsigned compare; a narrow integer computes in I64.
        ("u + u", t(U64)),
        ("u + f", t(F64)),
        ("u32 + u32", t(I64)),
        ("POWER(i, 2)", t(F64)),
        ("-u32", t(I64)),
        ("-i8", t(I64)),
        ("-f32", t(F64)),
        ("-u", t(U64)),
        ("-p", dec(2)),
        // A shift keeps the temporal type; a difference is an integer.
        ("d + 1", t(Date)),
        ("ts - d", t(I64)),
        ("d - d", t(I64)),
        ("DATE_TRUNC('month', ts)", t(Timestamp)),
        ("EXTRACT(YEAR FROM d)", t(I64)),
        // Strings: the measures and STRPOS are integers.
        ("LENGTH(s)", t(I64)),
        ("OCTET_LENGTH(s)", t(I64)),
        ("STRPOS(s, 'x')", t(I64)),
        ("UPPER(s)", t(String)),
        ("s || 'x'", t(String)),
        ("CONCAT(s, 1)", t(String)),
        ("TRIM(s)", t(String)),
        // CASE and GREATEST blend their results; a NULL one is neutral.
        ("CASE WHEN TRUE THEN u ELSE i END", t(U64)),
        ("CASE WHEN TRUE THEN f ELSE u END", t(F64)),
        ("CASE WHEN TRUE THEN NULL END", t(I64)),
        ("CASE WHEN TRUE THEN NULL WHEN TRUE THEN u END", t(U64)),
        ("CASE WHEN TRUE THEN d ELSE ts END", t(Timestamp)),
        ("CASE WHEN TRUE THEN s ELSE 'x' END", t(String)),
        ("CASE WHEN TRUE THEN p ELSE 0.5 END", dec(2)),
        ("GREATEST(p, q, i)", dec(3)),
        // CAST: a float target is an f64 register, anything else its target, which
        // the cast range-checks into.
        ("CAST(i AS FLOAT)", t(F64)),
        ("CAST(i AS SMALLINT)", t(I16)),
        ("CAST(i AS DECIMAL(10, 3))", dec(3)),
        // DECIMAL: `*` adds scales, `+`/`-`/`%` take the wider, `/` and a float
        // operand lift to F64; a float literal beside a DECIMAL is the decimal it
        // spells, unless no register holds it.
        ("p + q", dec(3)),
        ("p - i", dec(2)),
        ("p * q", dec(5)),
        ("p * i", dec(2)),
        ("p % q", dec(3)),
        ("p / q", t(F64)),
        ("p / 3", t(F64)),
        ("p + f", t(F64)),
        ("p > q", t(Bool)),
        ("p * 1.1", dec(3)),
        ("p + 1.255", dec(3)),
        ("i * 1.1", t(F64)),
        ("p * 0.1234567890123456789", t(F64)),
        ("ROUND(q, 1)", dec(1)),
        ("ROUND(q, 5)", dec(3)),
        ("FLOOR(q)", dec(0)),
        ("ABS(q)", dec(3)),
        ("SQRT(q)", t(F64)),
        ("SIGN(q)", t(I64)),
    ] {
        let bound = bind_sql(sql, &s).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(bound.infer_ty(&s.columns), want, "{sql}");
    }
}

/// The blend of two types, in either order: String > F64 > DECIMAL > U64 > I64,
/// a temporal type absorbing I64 and DATE meeting TIMESTAMP at TIMESTAMP.
#[test]
fn unify_blend_type_rule() {
    use TypeCode::*;
    let (t, dec) = (ColType::of, ColType::decimal);
    for (a, b, want) in [
        (t(String), t(F64), t(String)),
        (t(U64), t(F64), t(F64)),
        (t(F32), t(I64), t(F64)),
        (dec(2), dec(3), dec(3)),
        (dec(2), t(U64), dec(2)),
        (t(U64), t(I64), t(U64)),
        (t(I64), t(I64), t(I64)),
        // A narrow unsigned value stays below 2^63.
        (t(U32), t(U16), t(I64)),
        (t(Date), t(I64), t(Date)),
        (t(Bool), t(I64), t(Bool)),
        (t(Bool), t(Bool), t(Bool)),
        (t(Date), t(Date), t(Date)),
        (t(Date), t(Timestamp), t(Timestamp)),
    ] {
        assert_eq!(unify_blend_type(a, b), want, "{a} with {b}");
        assert_eq!(unify_blend_type(b, a), want, "{b} with {a}");
    }
}

/// Arithmetic with a temporal operand: a shift keeps the type, a difference is
/// an integer, and nothing else is typed.
#[test]
fn temporal_arithmetic_is_an_allow_list() {
    use TypeCode::*;
    let t = |op, l: TypeCode, r: TypeCode| temporal_arith_type(op, l.into(), r.into());
    assert_eq!(t(BinOp::Add, Date, I64), Some(Date.into()));
    assert_eq!(t(BinOp::Sub, Timestamp, I32), Some(Timestamp.into()));
    assert_eq!(t(BinOp::Add, I64, Date), Some(Date.into()));
    assert_eq!(t(BinOp::Sub, Date, Date), Some(I64.into()));
    assert_eq!(t(BinOp::Sub, Timestamp, Date), Some(I64.into()));
    for (op, l, r) in [
        (BinOp::Mul, Date, I64),
        (BinOp::Div, Timestamp, I64),
        (BinOp::Mod, Timestamp, I64),
        (BinOp::Sub, I64, Date),
        (BinOp::Add, Date, Date),
        (BinOp::Add, Date, F64),
    ] {
        assert_eq!(t(op, l, r), None, "{op:?} {l:?} {r:?}");
    }
}

/// The numeric functions' result types: the rounding family and ABS keep the
/// argument's register image, the transcendentals lift to F64, and SIGN is a
/// signed integer over any integer argument and a float over a float one.
#[test]
fn num_func_result_types() {
    use FloatUnaryOp as F;
    use TypeCode::*;
    let ty = |f: NumFunc, arg: TypeCode| f.result_type(arg.into());
    for f in [F::Abs, F::Floor, F::Ceil, F::Trunc, F::Round].map(NumFunc::Unary) {
        assert_eq!(ty(f, U64), U64.into(), "{f:?}");
        assert_eq!(ty(f, I32), I64.into(), "{f:?}");
        assert_eq!(ty(f, F32), F64.into(), "{f:?}");
    }
    assert_eq!(ty(NumFunc::Round(2), U64), U64.into());
    assert_eq!(ty(NumFunc::Round(-1), I64), F64.into());
    for f in [F::Sqrt, F::Ln, F::Log10, F::Exp].map(NumFunc::Unary) {
        assert_eq!(ty(f, I64), F64.into(), "{f:?}");
        assert_eq!(ty(f, F64), F64.into(), "{f:?}");
    }
    let sign = NumFunc::Unary(F::Sign);
    assert_eq!(ty(sign, U64), I64.into());
    assert_eq!(ty(sign, I8), I64.into());
    assert_eq!(ty(sign, F32), F64.into());
}

/// Each shape with `{x}` a NOT NULL column and with it a nullable one:
/// `never_null_with` proves the first exactly when no kernel the shape lowers to
/// makes a NULL of its own, and never proves the second.
#[test]
fn never_null_follows_the_kernels_that_make_a_null() {
    // `c`, `nts` and `nu` are nullable, every other column NOT NULL.
    let s = schema(
        vec![
            col("pk", TypeCode::U64),
            col("n", TypeCode::I64),
            ncol("c", TypeCode::I64),
            col("d", TypeCode::Date),
            col("ts", TypeCode::Timestamp),
            ncol("nts", TypeCode::Timestamp),
            ncol("nu", TypeCode::U64),
            ColumnDef::typed("p", ColType::decimal(2), false),
        ],
        &[0],
    );
    let never_null = |sql: &str| {
        bind_sql(sql, &s)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .never_null_with(&|i: &usize| s.columns[*i].is_nullable, &|i: &usize| s.columns[*i].ty)
    };
    for (shape, over_not_null) in [
        ("{x} + 1", true),
        ("POWER({x}, 2)", true),
        ("NOT {x}", true),
        ("ABS({x})", true),
        ("{x} LIKE 'a%'", true),
        ("TRIM({x})", true),
        ("UPPER({x})", true),
        ("EXTRACT(YEAR FROM {x})", true),
        // A zero divisor is NULL, so only a non-zero literal one is proven.
        ("{x} / 2", true),
        ("{x} % 2", true),
        ("{x} / 0", false),
        ("{x} % 0", false),
        ("{x} / {x}", false),
        // A string past `u32::MAX` bytes is NULL.
        ("{x} || {x}", false),
        ("CONCAT({x})", false),
        ("REPLACE({x}, {x}, {x})", false),
        // SUBSTRING's negative length is NULL; without one it cannot be.
        ("SUBSTR({x}, 1)", true),
        ("SUBSTR({x}, 1, 2)", false),
        ("{x} IN (1, 2)", true),
        ("{x} IN (1, c)", false),
        // GREATEST skips a NULL argument.
        ("GREATEST({x}, NULL)", true),
        // Text renders any scalar; every other target can refuse a value.
        ("CAST({x} AS VARCHAR)", true),
        ("CAST({x} AS SMALLINT)", false),
        ("CASE WHEN TRUE THEN {x} ELSE {x} END", true),
        ("CASE WHEN TRUE THEN c ELSE {x} END", false),
        ("CASE WHEN TRUE THEN {x} END", false),
        // A signed operand of an unsigned result is range-cast, and an unsigned
        // operand of temporal arithmetic is: either cast can refuse a value.
        ("GREATEST({x}, nu)", false),
        ("CASE WHEN TRUE THEN {x} ELSE pk END", false),
        ("pk / -2 + 0 * {x}", false),
        ("d + pk < d OR {x} = 1", false),
    ] {
        assert_eq!(never_null(&shape.replace("{x}", "n")), over_not_null, "{shape}");
        assert!(
            !never_null(&shape.replace("{x}", "c")),
            "{shape} over a nullable column"
        );
    }
    for (sql, want) in [
        ("1.5", true),
        ("DATE '2020-01-01'", true),
        ("NULL", false),
        ("c IS NULL", true),
        // A DATE meeting a TIMESTAMP is widened to microseconds, which a day count
        // past `i64` microseconds has none of.
        ("d - d", true),
        ("ts - d", false),
        ("d = ts", false),
        ("d + 1 < ts", false),
        ("CASE WHEN n = 1 THEN d ELSE ts END", false),
        ("ts IN (d, ts)", false),
        // GREATEST skips the DATE it NULLed, so another argument must hold.
        ("GREATEST(d, ts)", true),
        ("GREATEST(d, nts)", false),
        // A DECIMAL is an `i64`, so an unsigned operand brought to one is range-cast.
        ("p + n", true),
        ("p = n", true),
        ("p + pk", false),
        ("p = pk", false),
        ("pk IN (p, p)", false),
        ("CASE WHEN n = 1 THEN p ELSE pk END", false),
        ("GREATEST(p, pk)", true),
    ] {
        assert_eq!(never_null(sql), want, "{sql}");
    }
}

/// A predicate splits at every top-level AND, however the tree nests, in written
/// order.
#[test]
fn conjuncts_are_the_leaves_of_the_and_tree_in_order() {
    let s = typed();
    let leaves: Vec<BoundExpr> = ["u = 1", "i = 2 OR f = 0", "d = 3"]
        .map(|sql| bind_sql(sql, &s).unwrap())
        .to_vec();
    for sql in [
        "u = 1 AND (i = 2 OR f = 0) AND d = 3",
        "u = 1 AND ((i = 2 OR f = 0) AND d = 3)",
    ] {
        assert_eq!(bind_where(sql, &s), leaves, "{sql}");
    }
}
