use super::*;
use crate::expr_lower::compile_scalar_evaluator;
use crate::test_support::Cell::{self, Int, Null};
use crate::test_support::{batch_of, bind_sql, bind_where, col, ncol, schema};
use gnitz_core::Schema;
use gnitz_expr::ScalarCell;

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
    columns.extend(payload.map(|(n, ty)| ncol(n, ty)));
    schema(columns, &[0])
}

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
        // A shift keeps the temporal type, either side; a difference is an integer.
        ("d + 1", t(Date)),
        ("1 + d", t(Date)),
        ("ts - i8", t(Timestamp)),
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
        // The rounding family and ABS keep the argument's register image, the
        // transcendentals lift to F64, SIGN is I64 over an integer and F64 over a float.
        ("ABS(u)", t(U64)),
        ("FLOOR(u32)", t(I64)),
        ("CEIL(f32)", t(F64)),
        ("TRUNC(i8)", t(I64)),
        ("ROUND(u, 2)", t(U64)),
        ("ROUND(i, -1)", t(F64)),
        ("LN(i)", t(F64)),
        ("EXP(f)", t(F64)),
        ("SIGN(u)", t(I64)),
        ("SIGN(i8)", t(I64)),
        ("SIGN(f32)", t(F64)),
    ] {
        assert_eq!(bind_sql(sql, &s).infer_ty(s.columns()), want, "{sql}");
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
        (t(Bool), t(Date), t(Bool)),
        (t(Bool), t(Timestamp), t(Bool)),
    ] {
        assert_eq!(unify_blend_type(a, b), want, "{a} with {b}");
        assert_eq!(unify_blend_type(b, a), want, "{b} with {a}");
    }
}

/// Each shape with `{x}` a NOT NULL column of its operand type and with it a
/// nullable one:
/// `never_null` proves the first exactly when no kernel the shape lowers to
/// makes a NULL of its own, and never proves the second. What it proves, the
/// evaluator holds to over every column at its edges.
#[test]
fn never_null_follows_the_kernels_that_make_a_null() {
    // Each operand type as its NOT NULL and its nullable column.
    const INT: (&str, &str) = ("n", "c");
    const STR: (&str, &str) = ("s", "ns");
    const BOOL: (&str, &str) = ("b", "nb");
    const DATE: (&str, &str) = ("d", "nd");
    use TypeCode::*;
    let s = schema(
        vec![
            col("pk", U64),
            col("n", I64),
            ncol("c", I64),
            col("d", Date),
            col("ts", Timestamp),
            ncol("nts", Timestamp),
            ncol("nu", U64),
            col("p", ColType::decimal(2)),
            col("s", String),
            ncol("ns", String),
            col("b", Bool),
            ncol("nb", Bool),
            ncol("nd", Date),
        ],
        &[0],
    );
    let never_null = |sql: &str| bind_sql(sql, &s).never_null(s.columns());
    let mut proven = Vec::new();
    for (shape, over_not_null, (x, nx)) in [
        ("{x} + 1", true, INT),
        ("POWER({x}, 2)", true, INT),
        ("NOT {x}", true, BOOL),
        ("ABS({x})", true, INT),
        ("{x} LIKE 'a%'", true, STR),
        ("TRIM({x})", true, STR),
        ("UPPER({x})", true, STR),
        ("EXTRACT(YEAR FROM {x})", true, DATE),
        // A zero divisor is NULL, so only a non-zero literal one is proven.
        ("{x} / 2", true, INT),
        ("{x} % 2", true, INT),
        ("{x} / 0", false, INT),
        ("{x} % 0", false, INT),
        ("{x} / {x}", false, INT),
        // A string past `u32::MAX` bytes is NULL.
        ("{x} || {x}", false, STR),
        ("CONCAT({x})", false, STR),
        ("REPLACE({x}, {x}, {x})", false, STR),
        // SUBSTRING's negative length is NULL; without one it cannot be.
        ("SUBSTR({x}, 1)", true, STR),
        ("SUBSTR({x}, 1, 2)", false, STR),
        ("{x} IN (1, 2)", true, INT),
        ("{x} IN (1, c)", false, INT),
        // GREATEST skips a NULL argument.
        ("GREATEST({x}, NULL)", true, INT),
        // Text renders any scalar; every other target can refuse a value.
        ("CAST({x} AS VARCHAR)", true, INT),
        ("CAST({x} AS SMALLINT)", false, INT),
        ("CASE WHEN TRUE THEN {x} ELSE {x} END", true, INT),
        ("CASE WHEN TRUE THEN c ELSE {x} END", false, INT),
        ("CASE WHEN TRUE THEN {x} END", false, INT),
        // A signed operand of an unsigned result is range-cast, and an unsigned
        // operand of temporal arithmetic is: either cast can refuse a value.
        ("GREATEST({x}, nu)", false, INT),
        ("CASE WHEN TRUE THEN {x} ELSE pk END", false, INT),
        ("pk / -2 + 0 * {x}", false, INT),
        ("d + pk < d OR {x} = 1", false, INT),
    ] {
        let sql = shape.replace("{x}", x);
        assert_eq!(never_null(&sql), over_not_null, "{shape}");
        if over_not_null {
            proven.push(sql);
        }
        assert!(!never_null(&shape.replace("{x}", nx)), "{shape} over a nullable column");
    }
    for (sql, want) in [
        ("1.5", true),
        ("DATE '2020-01-01'", true),
        ("NULL", false),
        ("c IS NULL", true),
        // A DATE meeting a TIMESTAMP is widened to microseconds, which a day count
        // past `i64` microseconds has none of.
        ("d - d", true),
        // DATE ± days is NULL outside DATE's range.
        ("d + 1", false),
        ("d - n", false),
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
        if want {
            proven.push(sql.to_string());
        }
    }
    // Each NOT NULL column cycles through its edges, each nullable one is NULL on
    // every other row.
    let i64s = [0, 1, -1, i128::from(i64::MIN), i128::from(i64::MAX)];
    let days = [0, i128::from(i32::MIN), i128::from(i32::MAX)];
    let strs = ["", "a", " é "];
    let rows: Vec<Vec<Cell>> = (0..30)
        .map(|r| {
            let (v, day, st, bit) = (
                Int(i64s[r % 5]),
                Int(days[r % 3]),
                Cell::Str(strs[r % 3]),
                Int((r % 2) as i128),
            );
            let maybe = |cell| if r % 2 == 0 { Null } else { cell };
            let u64_max = Int(i128::from(u64::MAX));
            vec![
                v,
                maybe(v),
                day,
                v,
                maybe(v),
                maybe(u64_max),
                v,
                st,
                maybe(st),
                bit,
                maybe(bit),
                maybe(day),
            ]
        })
        .collect();
    let batch = batch_of(&s, &rows);
    for sql in &proven {
        let mut ev = compile_scalar_evaluator(&bind_sql(sql, &s), &s).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut nulls = 0;
        ev.for_each(&batch.view(), |cell| {
            nulls += matches!(cell, ScalarCell::Int(None) | ScalarCell::Str(None)) as usize;
        });
        assert_eq!(nulls, 0, "{sql} is proven never NULL");
    }
}

/// A predicate splits at every top-level AND, however the tree nests, in written
/// order.
#[test]
fn conjuncts_are_the_leaves_of_the_and_tree_in_order() {
    let s = typed();
    let leaves: Vec<BoundExpr> = ["u = 1", "i = 2 OR f = 0", "d = 3"]
        .map(|sql| bind_sql(sql, &s))
        .to_vec();
    for sql in [
        "u = 1 AND (i = 2 OR f = 0) AND d = 3",
        "u = 1 AND ((i = 2 OR f = 0) AND d = 3)",
    ] {
        assert_eq!(bind_where(sql, &s), leaves, "{sql}");
    }
}
