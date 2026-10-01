use super::*;
use crate::test_support::Cell::{self, Int, Null, Str, F64};
use crate::test_support::{batch_of, bind_sql, rejected, two_col, typed_schema};
use gnitz_expr::{ExprResults, FloatArithOp, IntUnaryOp, RowFilter};
use gnitz_wire::as_le_bytes;
use gnitz_wire::TypeCode as T;

// Every expression here is SQL bound over `typed_schema`: `(pk, c1, c2, …)`, the
// payload columns nullable and of the types the test lists.

fn dec(scale: u8) -> ColType {
    ColType::decimal(scale)
}

/// `sql` lowered to its program.
fn lowered<Ty: Into<ColType> + Copy>(sql: &str, tys: &[Ty]) -> LogicalProgram {
    let s = typed_schema(tys);
    let bound = bind_sql(sql, &s).unwrap_or_else(|e| panic!("{sql}: {e}"));
    compile_bound_expr_to_program(&bound, &s.columns).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// `sql` lowered to its instructions. A register is its instruction's position.
fn instrs<Ty: Into<ColType> + Copy>(sql: &str, tys: &[Ty]) -> Vec<L> {
    lowered(sql, tys).instrs().to_vec()
}

/// The message lowering rejects `sql` with.
fn lower_rejected<Ty: Into<ColType> + Copy>(sql: &str, tys: &[Ty]) -> String {
    let s = typed_schema(tys);
    let bound = bind_sql(sql, &s).unwrap_or_else(|e| panic!("{sql}: {e}"));
    rejected(compile_bound_expr_to_program(&bound, &s.columns))
}

/// Assert `sql` evaluates to `want` on each of `rows`, in the register class its
/// inferred type names: a float as its value, a string as its text.
#[track_caller]
fn assert_eval<Ty: Into<ColType> + Copy>(sql: &str, tys: &[Ty], rows: &[&[Cell]], want: &[Cell]) {
    let s = typed_schema(tys);
    let bound = bind_sql(sql, &s).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let ty = bound.infer_ty(&s.columns).tc;
    let mut ev = compile_scalar_evaluator(&bound, &s).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let out = ev.eval_all(&batch_of(&s, rows));
    let got: Vec<Cell> = match &out {
        ExprResults::Int(vals) => {
            assert_ne!(ty, T::String, "{sql}: typed a string, computed a scalar");
            vals.iter()
                .map(|v| match *v {
                    None => Null,
                    Some(v) if ty.is_float() => F64(f64::from_bits(v as u64)),
                    Some(v) => Int(v),
                })
                .collect()
        }
        ExprResults::Str { bytes, spans } => {
            assert_eq!(ty, T::String, "{sql}: computed a string, typed {ty}");
            spans
                .iter()
                .map(|span| match *span {
                    None => Null,
                    Some((at, len)) => Str(std::str::from_utf8(&bytes[at..at + len]).unwrap()),
                })
                .collect()
        }
    };
    assert_eq!(got, want, "{sql} over {rows:?}");
}

/// [`assert_eval`] per row of `(sql, row, value)`.
#[track_caller]
fn check<Ty: Into<ColType> + Copy>(tys: &[Ty], rows: &[(&str, &[Cell], Cell)]) {
    for &(sql, row, want) in rows {
        assert_eval(sql, tys, &[row], &[want]);
    }
}

// ------------------------------------------------------------------
// What each form computes
// ------------------------------------------------------------------

/// The string functions, each argument in the position its signature names, and
/// the string channel's NULL rules: `||` propagates a NULL where CONCAT reads it
/// as the empty string and casts a number to text.
#[test]
fn the_string_forms_compute_their_values() {
    check(
        &[T::String, T::String],
        &[
            ("STRPOS(c1, c2)", &[Str("héllo"), Str("l")], Int(3)),
            ("POSITION(c2 IN c1)", &[Str("héllo"), Str("l")], Int(3)),
            ("REPLACE(c1, 'l', c2)", &[Str("hello"), Str("L")], Str("heLLo")),
            ("SPLIT_PART(c1, c2, 2)", &[Str("a,b"), Str(",")], Str("b")),
            ("c1 || c2", &[Str("a"), Str("b")], Str("ab")),
            ("c1 || c2", &[Str("a"), Null], Null),
            ("c1 < c2", &[Str("a"), Str("b")], Int(1)),
            ("UPPER(c1) = UPPER(c2)", &[Str("ab"), Str("AB")], Int(1)),
            ("'x' IN (c1, c2)", &[Str("a"), Str("x")], Int(1)),
            ("NULLIF(c1, c2)", &[Str("x"), Str("x")], Null),
            ("NULLIF(c1, c2)", &[Str("x"), Str("y")], Str("x")),
            ("COALESCE(c1, c2)", &[Null, Str("y")], Str("y")),
        ],
    );
    check(
        &[T::String],
        &[
            ("LENGTH(c1)", &[Str("hé")], Int(2)),
            ("OCTET_LENGTH(c1)", &[Str("hé")], Int(3)),
            ("UPPER(c1)", &[Str("aB")], Str("AB")),
            ("LOWER(c1)", &[Str("aB")], Str("ab")),
            ("REVERSE(c1)", &[Str("ab")], Str("ba")),
            ("LEFT(c1, 2)", &[Str("abc")], Str("ab")),
            ("RIGHT(c1, 2)", &[Str("abc")], Str("bc")),
            ("LPAD(c1, 3, '*')", &[Str("a")], Str("**a")),
            ("RPAD(c1, 3, '*')", &[Str("a")], Str("a**")),
            ("SUBSTR(c1, 2)", &[Str("abc")], Str("bc")),
            ("SUBSTR(c1, 2, 1)", &[Str("abc")], Str("b")),
            ("TRIM(LEADING 'x' FROM c1)", &[Str("xax")], Str("ax")),
            ("TRIM(TRAILING 'x' FROM c1)", &[Str("xax")], Str("xa")),
            ("TRIM(BOTH 'x' FROM c1)", &[Str("xax")], Str("a")),
            ("c1 LIKE 'A%'", &[Str("ab")], Int(0)),
            ("c1 ILIKE 'A%'", &[Str("ab")], Int(1)),
            ("c1 NOT LIKE 'a%'", &[Str("ab")], Int(0)),
            ("UPPER(c1) LIKE 'A%'", &[Str("ab")], Int(1)),
            ("c1 LIKE 'a%'", &[Null], Null),
            ("c1 = 'ab'", &[Str("ab")], Int(1)),
            ("c1 IN ('a', 'b')", &[Str("b")], Int(1)),
            ("c1 || NULL", &[Str("a")], Null),
            ("CONCAT(c1, NULL, 1, 1.5)", &[Str("a")], Str("a11.5")),
            ("CONCAT(c1)", &[Null], Str("")),
            ("COALESCE(c1, 'dflt')", &[Null], Str("dflt")),
            ("CASE WHEN c1 = 'a' THEN 'x' ELSE c1 END", &[Str("a")], Str("x")),
            ("c1 IS NULL", &[Null], Int(1)),
            ("UPPER(c1) IS NOT NULL", &[Null], Int(0)),
            ("CAST(c1 AS BIGINT)", &[Str("42")], Int(42)),
            ("CAST(c1 AS DOUBLE)", &[Str("1.5")], F64(1.5)),
            ("CAST(c1 AS VARCHAR)", &[Str("x")], Str("x")),
        ],
    );
}

/// The numeric forms. An integer stays an integer through the rounding family —
/// a float detour would lose it past 2^53 — and lifts only where the result is a
/// float by definition.
#[test]
fn the_numeric_forms_compute_their_values() {
    let big = (1 << 53) + 1;
    check(
        &[T::I64],
        &[
            ("FLOOR(c1)", &[Int(big)], Int(big)),
            ("CEIL(c1)", &[Int(big)], Int(big)),
            ("ROUND(c1)", &[Int(big)], Int(big)),
            ("TRUNC(c1)", &[Int(big)], Int(big)),
            ("ROUND(c1, 2)", &[Int(big)], Int(big)),
            ("ROUND(c1, 15)", &[Int(big)], Int(big)),
            ("ROUND(c1, -2)", &[Int(1234)], F64(1200.0)),
            ("ABS(c1)", &[Int(-5)], Int(5)),
            ("-c1", &[Int(5)], Int(-5)),
            ("SIGN(c1)", &[Int(-5)], Int(-1)),
            ("SQRT(c1)", &[Int(16)], F64(4.0)),
            ("POWER(c1, 2)", &[Int(3)], F64(9.0)),
            ("c1 % 3", &[Int(7)], Int(1)),
            ("c1 / 0", &[Int(7)], Null),
            // CASE takes its first truthy branch, in the class its results blend to.
            (
                "CASE WHEN c1 > 0 THEN 1 WHEN c1 > 5 THEN 2 ELSE 3 END",
                &[Int(9)],
                Int(1),
            ),
            (
                "CASE WHEN c1 > 0 THEN 1 WHEN c1 > 5 THEN 2 ELSE 3 END",
                &[Int(-1)],
                Int(3),
            ),
            ("CASE WHEN c1 > 0 THEN c1 ELSE 1.5 END", &[Int(2)], F64(2.0)),
            ("CASE WHEN c1 > 0 THEN 'pos' END", &[Int(-1)], Null),
            ("CASE WHEN c1 > 0 THEN NULL END", &[Int(1)], Null),
            ("GREATEST(c1, 1.5)", &[Int(1)], F64(1.5)),
            ("LEAST(c1, 3)", &[Int(5)], Int(3)),
            ("GREATEST(c1, NULL)", &[Int(1)], Int(1)),
            ("c1 IN (1, 2)", &[Int(2)], Int(1)),
            // A list with no member among the operand's values holds on no row.
            ("c1 IN (1.5, 2.5)", &[Int(2)], Int(0)),
            ("c1 IN (1.5, 2.5)", &[Null], Null),
            ("(c1 + 1) IS NULL", &[Null], Int(1)),
            ("CAST(c1 AS FLOAT)", &[Int(16777217)], F64(16777216.0)),
            ("CAST(c1 AS DOUBLE)", &[Int(16777217)], F64(16777217.0)),
            ("CAST(c1 AS VARCHAR)", &[Int(-42)], Str("-42")),
            ("CAST(c1 AS TINYINT)", &[Int(5)], Int(5)),
            ("CAST(c1 AS TINYINT)", &[Int(300)], Null),
            // A typed string is a cast of its literal.
            ("CAST(NULL AS DATE)", &[Int(0)], Null),
            ("CAST(5 AS DATE)", &[Int(0)], Int(5)),
            ("DECIMAL(10,2) '1.5'", &[Int(0)], Int(150)),
            ("BIGINT '5'", &[Int(0)], Int(5)),
            // Two literals: the typed one is the operand.
            ("'2020-01-01' = DATE '2020-01-01'", &[Int(0)], Int(1)),
            // The elided cast keeps its U64 tracking, so the constant compares unsigned.
            ("CAST(5 AS BIGINT UNSIGNED) < 18446744073709551615", &[Int(0)], Int(1)),
        ],
    );
    check(
        &[T::F64],
        &[
            ("FLOOR(c1)", &[F64(1.5)], F64(1.0)),
            // A float tie rounds to even.
            ("ROUND(c1)", &[F64(2.5)], F64(2.0)),
            ("ROUND(c1)", &[F64(3.5)], F64(4.0)),
            ("ROUND(c1, 1)", &[F64(2.26)], F64(2.3)),
            ("ROUND(c1, -1)", &[F64(26.0)], F64(30.0)),
            ("SIGN(c1)", &[F64(-2.0)], F64(-1.0)),
            ("c1 > 0.5", &[F64(1.0)], Int(1)),
            ("c1 IN (1, 2)", &[F64(2.0)], Int(1)),
            // An unsigned constant lifts to the float it is.
            ("c1 < 18446744073709551615", &[F64(1e19)], Int(1)),
            ("CAST(c1 AS INT)", &[F64(2.7)], Int(2)),
            ("CAST(c1 AS VARCHAR)", &[F64(1.5)], Str("1.5")),
            ("CAST(c1 AS DECIMAL(10, 2))", &[F64(2.675)], Int(268)),
        ],
    );
    // A negation computes signed, so ABS over it computes too — even over an
    // unsigned column, whose own ABS is the identity.
    check(&[T::U32], &[("ABS(-c1)", &[Int(5)], Int(5))]);
    // A DECIMAL is an i64, so a U64 at or above 2^63 has none.
    check(&[T::U64], &[("CAST(c1 AS DECIMAL(10, 1))", &[Int(1 << 63)], Null)]);
}

/// The calendar reads a DATE in days and a TIMESTAMP in microseconds, and the
/// two meet at TIMESTAMP: in a comparison, a difference and a CASE.
#[test]
fn the_temporal_forms_compute_their_values() {
    let day = i128::from(gnitz_expr::calendar::MICROS_PER_DAY);
    check(
        &[T::Date],
        &[
            ("EXTRACT(YEAR FROM c1)", &[Int(18262)], Int(2020)),
            ("CAST(c1 AS TIMESTAMP)", &[Int(18262)], Int(18262 * day)),
            // A computed DATE is an unchecked i64 register, so no range is assumed.
            ("(c1 + 1) IN (2147483648, 5)", &[Int(2147483647)], Int(1)),
            ("COALESCE(c1, 3000000000) = 3000000000", &[Null], Int(1)),
        ],
    );
    check(
        &[T::Timestamp],
        &[
            ("EXTRACT(YEAR FROM c1)", &[Int(18262 * day)], Int(2020)),
            ("CAST(c1 AS DATE)", &[Int(18262 * day + 5)], Int(18262)),
            ("DATE_TRUNC('day', c1)", &[Int(18262 * day + 5)], Int(18262 * day)),
        ],
    );
    // d = day 10; ts = day 10 at midnight, and at noon.
    let tys = [T::Date, T::Timestamp, T::I64];
    let rows: [&[Cell]; 2] = [
        &[Int(10), Int(10 * day), Int(1)],
        &[Int(10), Int(10 * day + day / 2), Int(0)],
    ];
    for (sql, want) in [
        ("c1 = c2", [Int(1), Int(0)]),
        ("c1 > c2", [Int(0), Int(0)]),
        ("c1 < c2", [Int(0), Int(1)]),
        ("c2 - c1", [Int(0), Int(day / 2)]),
        (
            "CASE WHEN c3 = 1 THEN c1 ELSE c2 END",
            [Int(10 * day), Int(10 * day + day / 2)],
        ),
    ] {
        assert_eval(sql, &tys, &rows, &want);
    }
}

/// DECIMAL is integer arithmetic on the stored values: operands are brought to
/// the wider scale, a product keeps both, a literal is exact at the scale it
/// meets, and the rounding family rounds half away from zero.
#[test]
fn the_decimal_forms_compute_their_values() {
    // p = 1.25 at scale 2, q = 0.005 at scale 3, i = 3.
    let row: &[Cell] = &[Int(125), Int(5), Int(3)];
    check(
        &[dec(2), dec(3), T::I64.into()],
        &[
            ("c1 + c2", row, Int(1255)),
            ("c1 * c2", row, Int(625)),
            ("c1 * c3", row, Int(375)),
            ("c1 - 1", row, Int(25)),
            ("c1 + 0.1", row, Int(135)),
            ("c1 > c2", row, Int(1)),
            ("c1 / c2", row, F64(250.0)),
            // A longer literal widens the column, never rounds the literal.
            ("c1 = 1.250", row, Int(1)),
            ("c1 = 1.251", row, Int(0)),
            ("c1 IN (1.25, 2)", row, Int(1)),
            ("c1 IN (1.25, 2)", &[Int(200), Null, Null], Int(1)),
            ("c1 IN (1.25, 2)", &[Int(201), Null, Null], Int(0)),
            ("CAST(c3 AS DECIMAL(10, 2))", row, Int(300)),
            ("CAST(c2 AS DECIMAL(10, 2))", row, Int(1)),
            ("CAST(c2 AS DECIMAL(10, 2))", &[Null, Int(4), Null], Int(0)),
            ("CAST(c1 AS DECIMAL(10, 3))", row, Int(1250)),
            ("CAST(c1 AS BIGINT)", &[Int(150), Null, Null], Int(2)),
            ("CAST(c1 AS TINYINT)", &[Int(30000), Null, Null], Null),
            ("CAST(c1 AS DOUBLE)", row, F64(1.25)),
            ("CAST(c1 AS VARCHAR)", row, Str("1.25")),
            ("CAST(1.005 AS DECIMAL(10, 2))", row, Int(101)),
            ("CAST('2.5' AS DECIMAL(10, 2))", row, Int(250)),
        ],
    );
    for (sql, v, want) in [
        ("ROUND(c1, 1)", 125, 13),
        ("ROUND(c1, 1)", -125, -13),
        ("ROUND(c1, 1)", 124, 12),
        ("ROUND(c1)", 150, 2),
        ("ROUND(c1)", -150, -2),
        ("FLOOR(c1)", -101, -2),
        ("FLOOR(c1)", 199, 1),
        ("CEIL(c1)", 101, 2),
        ("CEIL(c1)", -199, -1),
        ("TRUNC(c1)", -199, -1),
        ("ABS(c1)", -199, 199),
        ("-c1", 199, -199),
        ("SIGN(c1)", -199, -1),
    ] {
        assert_eval(sql, &[dec(2)], &[&[Int(v)]], &[Int(want)]);
    }
}

// ------------------------------------------------------------------
// Comparisons against the exact answer
// ------------------------------------------------------------------

/// Each comparison as SQL spells it, beside the operator that holds with the
/// operands transposed and the opcode operand it lowers to.
const CMP: [(&str, &str, CmpOp); 6] = [
    ("=", "=", CmpOp::Eq),
    ("<>", "<>", CmpOp::Ne),
    ("<", ">", CmpOp::Lt),
    ("<=", ">=", CmpOp::Le),
    (">", "<", CmpOp::Gt),
    (">=", "<=", CmpOp::Ge),
];

fn holds(op: CmpOp, a: i128, b: i128) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Le => a <= b,
        CmpOp::Gt => a > b,
        CmpOp::Ge => a >= b,
    }
}

/// Every operator in both operand orders, `c1` against each literal, on each
/// row: the VM's answer is the exact rational one, and a NULL row stays NULL.
/// `rows` are the stored integers of a `ty` column.
fn assert_literal_compares_exact(ty: ColType, lits: &[&str], rows: &[Option<i128>]) {
    let table: Vec<[Cell; 1]> = rows.iter().map(|v| [v.map_or(Null, Int)]).collect();
    let table: Vec<&[Cell]> = table.iter().map(|r| &r[..]).collect();
    let col_scale = 10i128.pow(u32::from(ty.scale));
    for text in lits {
        let (mag, s) = gnitz_wire::decimal::decimal_of_number_text(text.trim_start_matches('-')).expect(text);
        let v = if text.starts_with('-') { -mag } else { mag };
        let lit_scale = 10i128.pow(u32::from(s));
        for (op, conv, cmp) in CMP {
            let want: Vec<Cell> = rows
                .iter()
                .map(|x| x.map_or(Null, |x| Int(i128::from(holds(cmp, x * lit_scale, v * col_scale)))))
                .collect();
            for sql in [format!("c1 {op} {text}"), format!("{text} {conv} c1")] {
                assert_eval(&sql, &[ty], &table, &want);
            }
        }
    }
}

#[test]
fn a_u64_column_against_a_literal_outside_its_range_is_exact() {
    let top = i128::from(u64::MAX);
    assert_literal_compares_exact(
        T::U64.into(),
        &[
            "-5",
            "-1",
            "0",
            "1",
            "9223372036854775808",
            "18446744073709551615",
            "18446744073709551616",
        ],
        &[Some(0), Some(1), Some(1 << 63), Some((1 << 63) - 1), Some(top), None],
    );
}

#[test]
fn an_i8_column_against_its_edges_is_exact() {
    assert_literal_compares_exact(
        T::I8.into(),
        &["-129", "-128", "127", "128"],
        &[Some(-128), Some(-1), Some(0), Some(127), None],
    );
}

#[test]
fn a_bigint_column_against_a_fraction_is_exact() {
    let p53 = 1i128 << 53;
    assert_literal_compares_exact(
        T::I64.into(),
        &[
            "1.5",
            "-1.5",
            "2.5",
            "-2.5",
            "9007199254740993.5",
            "-9007199254740992.5",
        ],
        &[
            Some(-3),
            Some(-2),
            Some(-1),
            Some(1),
            Some(2),
            Some(3),
            Some(p53),
            Some(p53 + 1),
            Some(p53 + 2),
            Some(-p53 - 1),
            None,
        ],
    );
}

/// A literal finer than a DECIMAL column's scale is decided at the column's
/// scale — one comparison against the neighbour, no scale-up, so one whose
/// scale-up would wrap an `i64` is exact too.
#[test]
fn a_decimal_column_against_a_finer_literal_is_exact() {
    assert_literal_compares_exact(
        dec(2),
        &["1.005", "1.000000000000000001"],
        &[None, Some(100), Some(101), Some(1000), Some(-1000)],
    );
    assert_eq!(
        instrs("c1 < 1.005", &[dec(2)]),
        [
            L::LoadColInt { col: 1 },
            L::LoadConst { val: 100, unsigned: false },
            L::Cmp { op: CmpOp::Le, a: Reg(0), b: Reg(1) }
        ]
    );
}

/// A U64 and a signed operand each read with their own signedness.
#[test]
fn a_u64_and_a_signed_column_compare_exactly() {
    let top = i128::from(u64::MAX);
    let pairs = [(top, -1), (0, -1), (1, 1), (1 << 63, i128::from(i64::MAX)), (5, 7)];
    let rows = pairs.map(|(u, i)| [Int(u), Int(i)]);
    let rows: Vec<&[Cell]> = rows.iter().map(|r| &r[..]).collect();
    for (op, _, cmp) in CMP {
        let want = pairs.map(|(u, i)| Int(i128::from(holds(cmp, u, i))));
        assert_eval(&format!("c1 {op} c2"), &[T::U64, T::I64], &rows, &want);
    }
}

// ------------------------------------------------------------------
// Where the instruction is the contract
// ------------------------------------------------------------------

/// The elision rule keys on the register IMAGE, not value-domain containment.
/// A U32 value fits U64's domain, but the engine taints a register U64 only
/// for a U64-typed load, so eliding U32 -> U64 would leave the register
/// signed while the client declares it U64 — flipping downstream compares and
/// vacating a later range check.
#[test]
fn cast_elision_preserves_u64_tracking() {
    for (sql, emits) in [
        ("CAST(c1 AS BIGINT)", false),
        ("CAST(c2 AS BIGINT)", false),
        ("CAST(c1 AS INT)", false),
        ("CAST(c4 AS BIGINT UNSIGNED)", false),
        ("CAST(c2 AS BIGINT UNSIGNED)", true),
        ("CAST(c3 AS BIGINT UNSIGNED)", true),
        ("CAST(c1 AS TINYINT)", true),
        ("CAST(c4 AS BIGINT)", true),
        ("CAST(-c1 AS INT)", true),
        ("CAST(NULL AS BIGINT)", false),
        ("CAST(NULL AS TINYINT)", true),
        ("CAST(5 AS INT)", false),
        ("CAST(5 AS BIGINT UNSIGNED)", true),
        ("CAST(300 AS TINYINT)", true),
    ] {
        let instrs = instrs(sql, &[T::I32, T::U32, T::I8, T::U64]);
        assert_eq!(
            instrs.iter().any(|i| matches!(i, L::IntCast { .. })),
            emits,
            "{sql}: {instrs:?}"
        );
    }
}

/// Over an integer register the rounding family is the identity, and so is ABS
/// of an unsigned one: nothing is emitted.
#[test]
fn an_integer_argument_folds_the_identity_transforms_away() {
    let load = [L::LoadColInt { col: 1 }];
    for sql in ["FLOOR(c1)", "CEIL(c1)", "ROUND(c1)", "TRUNC(c1)", "ROUND(c1, 2)"] {
        assert_eq!(instrs(sql, &[T::I32]), load, "{sql}");
    }
    for tc in [T::U32, T::U64] {
        assert_eq!(instrs("ABS(c1)", &[tc]), load, "{tc:?}");
    }
    assert_eq!(
        instrs("ABS(c1)", &[T::I32]),
        [load[0], L::IntUnary { op: IntUnaryOp::Abs, a: Reg(0) }]
    );
}

/// `ROUND(x)` over a float is the bare opcode. A positive scale multiplies first
/// and divides back, a negative one does the mirror: only the positive powers of
/// ten are exact in f64, so neither direction loads `10^-n`.
#[test]
fn a_float_round_scales_by_a_positive_power_in_either_direction() {
    assert_eq!(
        instrs("ROUND(c1)", &[T::F64]),
        [
            L::LoadColFloat { col: 1 },
            L::FloatUnary { op: FloatUnaryOp::Round, a: Reg(0) }
        ]
    );
    // The scaling steps in order: (M)ul, (D)iv, (R)ound.
    let steps = |sql: &str| -> String {
        instrs(sql, &[T::F64])
            .iter()
            .filter_map(|i| match i {
                L::FloatArith { op: FloatArithOp::Mul, .. } => Some('M'),
                L::FloatArith { op: FloatArithOp::Div, .. } => Some('D'),
                L::FloatUnary { op: FloatUnaryOp::Round, .. } => Some('R'),
                _ => None,
            })
            .collect()
    };
    assert_eq!(steps("ROUND(c1, 2)"), "MRD");
    assert_eq!(steps("ROUND(c1, -2)"), "DRM");
}

/// GREATEST/LEAST is a left chain of 2-ary opcodes, so its registers are linear
/// in arity and the register file bounds it. A U64 argument heads the fold
/// wherever it was written: the engine taints a register unsigned only from the
/// first U64 operand onward, so an earlier signed pair would compare signed.
#[test]
fn min_max_n_is_a_left_fold_headed_by_its_u64_argument() {
    assert_eq!(
        instrs("GREATEST(c1, c2, c3)", &[T::I64, T::I64, T::I64]),
        [
            L::LoadColInt { col: 1 },
            L::LoadColInt { col: 2 },
            L::IntMinMax2 { a: Reg(0), b: Reg(1), is_max: true },
            L::LoadColInt { col: 3 },
            L::IntMinMax2 { a: Reg(2), b: Reg(3), is_max: true },
        ]
    );
    for sql in ["GREATEST(-1, 1, c1)", "GREATEST(c1, -1, 1)"] {
        assert_eq!(instrs(sql, &[T::U64])[0], L::LoadColInt { col: 1 }, "{sql}");
    }
    // Distinct literals: the builder folds identical instructions.
    let greatest = |n: usize| {
        let args: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        format!("GREATEST({})", args.join(", "))
    };
    assert_eq!(instrs(&greatest(32), &[T::I64]).len(), 63);
    assert!(lower_rejected(&greatest(33), &[T::I64]).contains("reg"));
}

/// A string column against a literal or another column is one fused opcode over
/// the cells themselves, with the literal on either side; a BLOB column lowers
/// identically. A computed operand has no fused form and compares through the
/// register channel, where each comparison has its own opcode too.
#[test]
fn string_comparisons_fuse_over_columns_and_fall_back_to_registers() {
    let strings = [T::String, T::String];
    for (op, conv, cmp) in CMP {
        let col_lit = instrs(&format!("c1 {op} 'x'"), &strings);
        assert!(
            matches!(col_lit[..], [L::StrColConst { op, col: 1, .. }] if op == cmp),
            "{op}: {col_lit:?}"
        );
        assert_eq!(instrs(&format!("'x' {conv} c1"), &strings), col_lit, "'x' {conv} c1");
        let col_col = instrs(&format!("c1 {op} c2"), &strings);
        assert_eq!(col_col, [L::StrColCol { op: cmp, col_a: 1, col_b: 2 }], "{op}");
        let blobs = [T::Blob, T::Blob];
        assert_eq!(instrs(&format!("c1 {op} 'x'"), &blobs), col_lit, "blob {op} 'x'");
        assert_eq!(instrs(&format!("c1 {op} c2"), &blobs), col_col, "blob {op} blob");
        assert_eq!(
            instrs(&format!("UPPER(c1) {op} UPPER(c2)"), &strings),
            [
                L::LoadColStr { col: 1 },
                L::StrCase { a: Reg(0), upper: true },
                L::LoadColStr { col: 2 },
                L::StrCase { a: Reg(2), upper: true },
                L::StrCmp { op: cmp, a: Reg(1), b: Reg(3) },
            ],
            "{op}"
        );
    }
}

/// An integer operand whose items all place among its values is one `IntInSet`
/// over a sorted, deduplicated pool, whatever the list's length; an item that
/// names no value of the operand drops out, and one that places nowhere keeps
/// the OR chain.
#[test]
fn an_integer_in_list_is_one_set_probe() {
    let is_set = |p: &LogicalProgram| {
        matches!(
            p.instrs(),
            [L::LoadColInt { col: 1 }, L::IntInSet { value_reg: Reg(0), .. }]
        )
    };
    let p = lowered("c1 IN (2, 1, 1, -1)", &[T::I64]);
    assert!(is_set(&p), "{:?}", p.instrs());
    assert_eq!(p.const_strings(), [as_le_bytes(&[-1i64, 1, 2]).to_vec()]);

    let items: Vec<String> = (0..500).map(|i| i.to_string()).collect();
    assert!(is_set(&lowered(&format!("c1 IN ({})", items.join(", ")), &[T::I64])));

    let p = lowered("c1 IN (1.5, 2)", &[T::I64]);
    assert!(is_set(&p), "{:?}", p.instrs());
    assert_eq!(p.const_strings(), [as_le_bytes(&[2i64]).to_vec()]);
    // Exact at a DECIMAL column's scale.
    let p = lowered("c1 IN (1.25, 2)", &[dec(2)]);
    assert!(is_set(&p), "{:?}", p.instrs());
    assert_eq!(p.const_strings(), [as_le_bytes(&[125i64, 200]).to_vec()]);

    let chain = instrs("c1 IN (1, 1e400)", &[T::I64]);
    assert!(!chain.iter().any(|i| matches!(i, L::IntInSet { .. })), "{chain:?}");
}

/// Any other list is the `operand = item` OR chain. The operand is lowered once
/// for the whole list — each term re-lowers it and the builder folds the
/// identical instructions — and a string term stays a fused compare, so a term
/// costs one register and the OR one more.
#[test]
fn an_in_list_or_chain_shares_its_operand() {
    assert_eq!(
        instrs("c1 IN (1, c2)", &[T::I64, T::I64]),
        [
            L::LoadColInt { col: 1 },
            L::LoadConst { val: 1, unsigned: false },
            L::Cmp { op: CmpOp::Eq, a: Reg(0), b: Reg(1) },
            L::LoadColInt { col: 2 },
            L::Cmp { op: CmpOp::Eq, a: Reg(0), b: Reg(3) },
            L::BoolBinary { a: Reg(2), b: Reg(4), is_or: true },
        ]
    );
    let strings = [T::String, T::String];
    let fused = |i: &L, col| matches!(*i, L::StrColConst { op: CmpOp::Eq, col: c, .. } if c == col);
    let or = L::BoolBinary { a: Reg(0), b: Reg(1), is_or: true };
    // A literal operand is read through the converse arm of the fused compare.
    for (sql, cols) in [("c1 IN ('a', 'b')", [1, 1]), ("'x' IN (c1, c2)", [1, 2])] {
        let got = instrs(sql, &strings);
        assert!(
            got.len() == 3 && fused(&got[0], cols[0]) && fused(&got[1], cols[1]) && got[2] == or,
            "{sql}: {got:?}"
        );
    }
    let tags = |n: usize| {
        let items: Vec<String> = (0..n).map(|i| format!("'tag{i}'")).collect();
        items.join(", ")
    };
    assert_eq!(instrs(&format!("c1 IN ({})", tags(22)), &strings).len(), 2 * 22 - 1);
    // Load, UPPER, then per item its constant and compare, and the ORs between.
    assert_eq!(
        instrs(&format!("UPPER(c1) IN ({})", tags(14)), &strings).len(),
        2 + 2 * 14 + 13
    );
}

/// A wide literal against a 64-bit column is decided by where it falls: one
/// past the column's range holds on no row, with no constant loaded.
#[test]
fn a_wide_literal_past_the_column_is_decided_without_a_constant() {
    assert_eq!(
        instrs("c1 = 18446744073709551615", &[T::I64]),
        [L::LoadColInt { col: 1 }, L::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(0) }]
    );
}

/// A null test over a column reads the batch bitmap, with no load; over anything
/// else it tests the register the value computes into.
#[test]
fn null_test_lowers_by_its_operand() {
    assert_eq!(
        instrs("c1 IS NULL", &[T::String]),
        [L::IsNull { col: 1, invert: false }]
    );
    assert_eq!(
        instrs("c1 IS NOT NULL", &[T::String]),
        [L::IsNull { col: 1, invert: true }]
    );
    assert_eq!(
        instrs("UPPER(c1) IS NULL", &[T::String]),
        [
            L::LoadColStr { col: 1 },
            L::StrCase { a: Reg(0), upper: true },
            L::IsNullReg { a: Reg(1), invert: false }
        ]
    );
    assert_eq!(
        instrs("(c1 + 1) IS NOT NULL", &[T::I64]).last(),
        Some(&L::IsNullReg { a: Reg(2), invert: true })
    );
}

// ------------------------------------------------------------------
// What lowering refuses
// ------------------------------------------------------------------

/// Lowering reads each operand through the class its position needs, so a wrong
/// one is a SQL error naming the rule rather than an engine-side class mismatch.
#[test]
fn each_refused_form_names_its_rule() {
    let t = ColType::of;
    let q7 = "c2 * c2 * c2 * c2 * c2 * c2 * c2";
    let cases: Vec<(String, Vec<ColType>, &str)> = [
        // A string in a numeric position.
        ("c1 + 1", vec![t(T::String)], "is not supported on a string operand"),
        ("c1 AND 'x'", vec![t(T::String)], "column \"c1\" is a string"),
        ("c1 OR 'x'", vec![t(T::String)], "column \"c1\" is a string"),
        ("NOT c1", vec![t(T::String)], "column \"c1\" is a string"),
        ("-c1", vec![t(T::String)], "column \"c1\" is a string"),
        ("ABS(c1)", vec![t(T::String)], "column \"c1\" is a string"),
        ("ROUND(c1, 2)", vec![t(T::String)], "column \"c1\" is a string"),
        ("GREATEST(c1, 'x')", vec![t(T::String)], "column \"c1\" is a string"),
        ("SUBSTR(c1, c1)", vec![t(T::String)], "column \"c1\" is a string"),
        // A comparison carries no implicit cast.
        ("c1 = 1", vec![t(T::String)], "needs both operands to be strings"),
        (
            "c1 > c2",
            vec![t(T::String), t(T::I64)],
            "needs both operands to be strings",
        ),
        // A number in a string position; only CONCAT casts one.
        ("c1 || 1", vec![t(T::String)], "expected a string value here"),
        (
            "REPLACE(c1, 2, 'b')",
            vec![t(T::String)],
            "expected a string value here",
        ),
        ("c1 LIKE 'a'", vec![t(T::I32)], "expected a string value here"),
        (
            "CASE WHEN 1 THEN 'x' ELSE 0 END",
            vec![t(T::I64)],
            "expected a string value here",
        ),
        // A float in an integer position.
        (
            "LEFT(c1, 1.5)",
            vec![t(T::String)],
            "LEFT: argument 2 must be an integer expression",
        ),
        (
            "SUBSTR(c1, 2, 1.5)",
            vec![t(T::String)],
            "SUBSTRING: argument 3 must be an integer expression",
        ),
        // The types no register holds.
        (
            "LOWER(c1)",
            vec![t(T::Blob)],
            "column \"c1\" is BLOB; blob columns support only",
        ),
        (
            "c1 + 1",
            vec![t(T::U128)],
            "column \"c1\" is U128; 128-bit columns cannot be used",
        ),
        (
            "c1 IN (1, 2)",
            vec![t(T::I128)],
            "column \"c1\" is I128; 128-bit columns cannot be used",
        ),
        (
            "GREATEST(c1, c1)",
            vec![t(T::UUID)],
            "column \"c1\" is UUID; 128-bit columns cannot be used",
        ),
        ("CAST(c1 AS UUID)", vec![t(T::I64)], "CAST to UUID is not supported"),
        ("CAST(c1 AS UINT128)", vec![t(T::I64)], "CAST to U128 is not supported"),
        (
            "UUID '00000000-0000-0000-0000-000000000001'",
            vec![t(T::I64)],
            "CAST to UUID is not supported",
        ),
        (
            "c1 + 18446744073709551616",
            vec![t(T::I64)],
            "integer literal 18446744073709551616 does not fit a 64-bit register",
        ),
        ("c1 % 1.5", vec![t(T::F64)], "float modulo not supported"),
        // Temporal values: arithmetic outside the allow-list, a blend with another
        // type, and a string that is no literal.
        ("c1 * 2", vec![t(T::Date)], "not supported on a DATE/TIMESTAMP operand"),
        (
            "c1 / 1000000",
            vec![t(T::Timestamp)],
            "not supported on a DATE/TIMESTAMP operand",
        ),
        ("5 - c1", vec![t(T::Date)], "not supported on a DATE/TIMESTAMP operand"),
        ("c1 + c1", vec![t(T::Date)], "not supported on a DATE/TIMESTAMP operand"),
        (
            "CASE WHEN 1 THEN c1 ELSE 1.5 END",
            vec![t(T::Date)],
            "cannot mix DATE with F64",
        ),
        (
            "CAST(c1 AS DATE)",
            vec![t(T::String)],
            "CAST of a string to DATE is supported for a literal only",
        ),
        (
            "EXTRACT(YEAR FROM c1)",
            vec![t(T::I64)],
            "a calendar function takes a DATE or TIMESTAMP; column \"c1\" is I64",
        ),
        // A string compared with an integer-stored column must spell a value of it.
        ("c1 = 'abc'", vec![dec(2)], "invalid DECIMAL(18, 2) literal: 'abc'"),
        ("c1 = 'abc'", vec![t(T::I64)], "invalid I64 literal: 'abc'"),
        (
            "c1 + c2",
            vec![dec(2), t(T::String)],
            "is not supported between a DECIMAL and a string",
        ),
        // A scale no register holds, through a product and through each blend
        // that would widen an operand to it.
        (
            "c2 * c2 * c2 * c2 * c2 * c2 * c2 * c2",
            vec![dec(2), dec(3)],
            "DECIMAL scale 24 exceeds 18",
        ),
        (
            &format!("c1 + {q7}"),
            vec![dec(2), dec(3)],
            "DECIMAL scale 21 exceeds 18",
        ),
        (
            &format!("CASE WHEN 1 THEN c1 ELSE {q7} END"),
            vec![dec(2), dec(3)],
            "DECIMAL scale 21 exceeds 18",
        ),
    ]
    .map(|(sql, tys, needle): (&str, _, _)| (sql.to_string(), tys, needle))
    .into();
    for (sql, tys, needle) in cases {
        let m = lower_rejected(&sql, &tys);
        assert!(m.contains(needle), "{sql}: {m:?} does not name {needle:?}");
    }
}

// ------------------------------------------------------------------
// The filter program and the wire predicate
// ------------------------------------------------------------------

/// A conjunct its connectives settle true over literals — the binder's fold of
/// `IS NOT NULL` on a NOT NULL column, say — is dropped wherever it sits, so it
/// costs no `BoolBinary` per row; nothing but such conjuncts is no program at
/// all. One they leave open or settle false keeps its program.
#[test]
fn a_filter_program_drops_the_conjuncts_settled_true() {
    let s = typed_schema(&[T::I64]);
    let p = bind_sql("c1 > 1", &s).unwrap();
    let (t, f) = (BoundExpr::LitInt(1), BoundExpr::LitInt(0));
    let or = |a: &BoundExpr, b: &BoundExpr| BoundExpr::bin(a.clone(), BinOp::Or, b.clone());
    let and = |a: &BoundExpr, b: &BoundExpr| BoundExpr::bin(a.clone(), BinOp::And, b.clone());
    let not = |a: &BoundExpr| BoundExpr::Not(Box::new(a.clone()));
    let program = |conjuncts: &[&BoundExpr]| {
        compile_filter_program(conjuncts.iter().copied(), &s.columns)
            .expect("lowers")
            .map(|p| p.instrs().to_vec())
    };
    let alone = program(&[&p]).expect("a program");
    assert_eq!(program(&[&t, &p, &t]), Some(alone));
    assert_eq!(program(&[&t, &t]), None);
    assert_eq!(program(&[]), None);
    assert_eq!(program(&[&f]), Some(vec![L::LoadConst { val: 0, unsigned: false }]));
    for dropped in [or(&p, &t), not(&and(&f, &p)), and(&t, &or(&t, &f))] {
        assert_eq!(program(&[&dropped]), None, "{dropped:?}");
    }
    for kept in [or(&f, &p), and(&p, &t), not(&t), and(&p, &f), or(&f, &f)] {
        assert!(program(&[&kept]).is_some(), "{kept:?}");
    }
}

/// Every conjunct is a boolean, a lone one included: a string column on its own
/// draws the lowering's message rather than reaching the resolver.
#[test]
fn a_lone_string_conjunct_is_rejected_by_lowering() {
    let s = typed_schema(&[T::String]);
    let m = rejected(compile_filter_program([&BoundExpr::ColRef(1)], &s.columns));
    assert!(m.contains("column \"c1\" is a string"), "{m}");
}

/// The rows of `batch` that pass every conjunct, through the wire blob a read ships.
fn residual_rows(conjuncts: &[&BoundExpr], batch: &gnitz_core::ZSetBatch, schema: &Schema) -> Vec<usize> {
    let blob = compile_wire_conjuncts(conjuncts.iter().copied(), &schema.columns).expect("residual must compile");
    let mut filter = RowFilter::for_read(&blob, &gnitz_wire::ReadBound::None, schema).expect("the blob resolves");
    let mut ranges = Vec::new();
    filter.ranges(batch, &mut ranges);
    ranges.into_iter().flat_map(|(s, e)| s..e).collect()
}

/// A residual keeps a row only where it is TRUE: a NULL conjunct excludes the
/// row bare, compared, and beside a conjunct that is TRUE or FALSE.
#[test]
fn a_null_residual_conjunct_excludes_the_row() {
    let schema = two_col(T::I64);
    let batch = batch_of(&schema, &[&[Null]]); // pk = 1
    let bind = |sql| bind_sql(sql, &schema).unwrap();
    for conjuncts in [
        vec![BoundExpr::ColRef(1)],
        vec![bind("val = 0")],
        vec![bind("pk = 1"), bind("val = 0")],
        vec![bind("pk = 2"), bind("val = 0")],
    ] {
        let refs: Vec<&BoundExpr> = conjuncts.iter().collect();
        assert_eq!(residual_rows(&refs, &batch, &schema), [0usize; 0], "{conjuncts:?}");
    }
    assert_eq!(residual_rows(&[&bind("pk = 1")], &batch, &schema), [0]);
}

/// No conjuncts, and a conjunct settled true, ship no blob and keep every row;
/// one settled false drops every row.
#[test]
fn the_residual_keep_every_row_exits() {
    let schema = two_col(T::I64);
    let batch = batch_of(&schema, &[&[Int(7)]]);
    let (t, f) = (BoundExpr::LitInt(1), BoundExpr::LitInt(0));
    assert!(compile_wire_conjuncts([&t], &schema.columns).unwrap().is_empty());
    assert_eq!(residual_rows(&[], &batch, &schema), [0]);
    assert_eq!(residual_rows(&[&t], &batch, &schema), [0]);
    assert_eq!(residual_rows(&[&f], &batch, &schema), [0usize; 0]);
}
