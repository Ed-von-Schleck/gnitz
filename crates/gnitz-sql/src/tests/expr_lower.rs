use super::*;
use crate::ir::NumLit;
use crate::test_support::lit;
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_expr::{CmpOp, ExprValidateErr, FloatArithOp, IntUnaryOp, LogicalInstr, LogicalInstr as L, Sink};

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

fn compile(left: &BoundExpr, op: BinOp, right: &BoundExpr, schema: &Schema) -> Vec<LogicalInstr> {
    let mut eb = ExprBuilder::new();
    let reg = OpcodeBackend { cols: &schema.columns, eb: &mut eb }
        .string_cmp(left, op, right)
        .expect("recognized as a string comparison");
    eb.build(vec![Sink::Reg(reg)])
        .expect("a well-formed program")
        .instrs()
        .to_vec()
}

fn cast_schema() -> Schema {
    Schema {
        columns: vec![
            col("pk", TypeCode::U64),
            col("i32", TypeCode::I32),
            col("u32", TypeCode::U32),
            col("i8", TypeCode::I8),
            col("u64", TypeCode::U64),
        ],
        pk_cols: vec![0],
    }
}

/// The typed instructions `expr` lowers to, plus its result register's class;
/// `lower_instrs` is the same without the class. Typed rather than wire words:
/// these are tests of the *lowerer*, so no encoding change reaches this file.
fn lower_instrs_kind(expr: &BoundExpr, schema: &Schema) -> (Vec<LogicalInstr>, ExprKind) {
    let mut eb = ExprBuilder::new();
    let (reg, kind) = OpcodeBackend { cols: &schema.columns, eb: &mut eb }
        .lower(expr)
        .expect("lowers");
    let prog = eb.build(vec![Sink::Reg(reg)]).expect("a well-formed program");
    (prog.instrs().to_vec(), kind)
}

fn lower_instrs_isf(expr: &BoundExpr, schema: &Schema) -> (Vec<LogicalInstr>, bool) {
    let (instrs, kind) = lower_instrs_kind(expr, schema);
    (instrs, kind == ExprKind::Float)
}

fn lower_instrs(expr: &BoundExpr, schema: &Schema) -> Vec<LogicalInstr> {
    lower_instrs_isf(expr, schema).0
}

/// `contains` over typed instructions: a variant carrying fields is *matched*,
/// not compared, so every predicate below is a `matches!`.
fn has(instrs: &[LogicalInstr], p: impl Fn(&LogicalInstr) -> bool) -> bool {
    instrs.iter().any(p)
}

fn count(instrs: &[LogicalInstr], p: impl Fn(&LogicalInstr) -> bool) -> usize {
    instrs.iter().filter(|i| p(i)).count()
}

/// The lowering error `expr` produces, for the reject cases.
fn lower_err(expr: &BoundExpr, schema: &Schema) -> GnitzSqlError {
    let mut eb = ExprBuilder::new();
    OpcodeBackend { cols: &schema.columns, eb: &mut eb }
        .lower(expr)
        .expect_err("must be rejected")
}

/// Lower `expr` and hold it to the same rules the engine does.
///
/// This is the check that matters for the string channel, and it is not an
/// opcode snapshot: assembling the program holds every register operand to
/// the class its opcode reads, so a lowering that mixed the classes — a
/// scalar `LoadNull` into a string CASE, a string register into `bool_and` —
/// is rejected there. `resolve_filter` then refuses a string result
/// register, which is what confirms the expression really produced a string.
fn assert_str_program(expr: &BoundExpr, schema: &Schema) {
    let logical = compile_bound_expr_to_program(expr, &schema.columns).expect("lowers");
    match logical.resolve_filter(schema) {
        Err(ExprValidateErr::RegClassMismatch { .. }) => {}
        Err(e) => panic!("rejected for the wrong reason: {e:?}"),
        Ok(_) => panic!("a string result register must not resolve as a filter"),
    }
}

/// The float instructions a fold-to-identity must never emit.
fn is_float_op(i: &LogicalInstr) -> bool {
    matches!(
        i,
        L::IntToFloat { .. }
            | L::FloatArith {
                op: FloatArithOp::Mul | FloatArithOp::Div,
                ..
            }
            | L::FloatUnary {
                op: FloatUnaryOp::Round | FloatUnaryOp::Floor | FloatUnaryOp::Ceil,
                ..
            }
    )
}

fn is_round(instrs: &[LogicalInstr]) -> bool {
    has(instrs, |i| matches!(i, L::FloatUnary { op: FloatUnaryOp::Round, .. }))
}

fn cast_emits(expr: &BoundExpr, to: TypeCode, schema: &Schema) -> bool {
    let cast = BoundExpr::Cast {
        expr: Box::new(expr.clone()),
        to: to.into(),
    };
    has(&lower_instrs(&cast, schema), |i| matches!(i, L::IntCast { .. }))
}

/// The elision rule keys on the register IMAGE, not value-domain containment.
/// A U32 value fits U64's domain, but the engine taints a register U64 only
/// for a U64-typed load, so eliding U32 -> U64 would leave the register
/// signed while the client declares it U64 — flipping downstream compares and
/// vacating a later range check.
#[test]
fn cast_elision_preserves_u64_tracking() {
    let s = cast_schema();
    assert!(
        !cast_emits(&BoundExpr::ColRef(1), TypeCode::I64, &s),
        "i32 -> BIGINT elides"
    );
    assert!(
        !cast_emits(&BoundExpr::ColRef(2), TypeCode::I64, &s),
        "u32 -> BIGINT elides"
    );
    assert!(
        !cast_emits(&BoundExpr::ColRef(1), TypeCode::I32, &s),
        "i32 -> INT elides"
    );
    assert!(
        !cast_emits(&BoundExpr::ColRef(4), TypeCode::U64, &s),
        "u64 -> U64 elides"
    );
    assert!(
        cast_emits(&BoundExpr::ColRef(2), TypeCode::U64, &s),
        "u32 -> BIGINT UNSIGNED emits"
    );
    assert!(
        cast_emits(&BoundExpr::ColRef(3), TypeCode::U64, &s),
        "i8 -> BIGINT UNSIGNED emits"
    );
    assert!(
        cast_emits(&BoundExpr::ColRef(1), TypeCode::I8, &s),
        "i32 -> TINYINT emits"
    );
    assert!(
        cast_emits(&BoundExpr::ColRef(4), TypeCode::I64, &s),
        "u64 -> BIGINT emits"
    );
    let neg = func(NumFunc::Unary(FloatUnaryOp::Neg), BoundExpr::ColRef(1));
    assert!(cast_emits(&neg, TypeCode::I32, &s), "CAST(-i32col AS INT) emits");
    assert!(
        !cast_emits(&BoundExpr::LitNull, TypeCode::I64, &s),
        "CAST(NULL AS BIGINT) elides"
    );
    assert!(
        cast_emits(&BoundExpr::LitNull, TypeCode::I8, &s),
        "CAST(NULL AS TINYINT) emits"
    );
}

fn round_instrs(n: i8, arg: &BoundExpr, schema: &Schema) -> (Vec<LogicalInstr>, bool) {
    lower_instrs_isf(&func(NumFunc::Round(n), arg.clone()), schema)
}

/// n >= 0 on an integer argument is the identity: no opcode, and the value
/// keeps its integer type rather than going through an f64 lift that would
/// mangle any |x| >= 2^53.
#[test]
fn round_scaled_folds_integer_nonnegative_scale() {
    let s = cast_schema();
    let icol = BoundExpr::ColRef(1);
    for n in [0i8, 2, 15] {
        let (instrs, isf) = round_instrs(n, &icol, &s);
        assert!(!isf, "n={n}: stays integer");
        assert!(!has(&instrs, is_float_op), "n={n}: no float arithmetic");
    }
    assert!(round_instrs(2, &lit("1.5"), &s).1, "float arg stays float");
    let (instrs, isf) = round_instrs(-2, &icol, &s);
    assert!(isf, "negative scale yields F64");
    assert!(is_round(&instrs), "negative scale rounds");
}

/// `ROUND(x)` is the bare unary opcode, not a scale-by-1 round trip.
#[test]
fn unscaled_round_is_plain_round() {
    let s = cast_schema();
    let (instrs, _) = lower_instrs_isf(&func(NumFunc::Unary(FloatUnaryOp::Round), lit("2.5")), &s);
    assert!(is_round(&instrs));
    assert!(!has(&instrs, |i| matches!(
        i,
        L::FloatArith {
            op: FloatArithOp::Mul | FloatArithOp::Div,
            ..
        }
    )));
}

/// A positive scale multiplies first and divides back; a negative one does
/// the mirror. Only the positive powers of ten are exactly representable in
/// f64, which is why neither direction loads `10^-n`.
#[test]
fn round_scaled_picks_the_scaling_direction() {
    let s = cast_schema();
    // The scaling steps in order, spelled as one letter each: (M)ul, (D)iv,
    // (R)ound — everything else the lowering emits is dropped.
    let steps = |n: i8| -> String {
        round_instrs(n, &lit("1.5"), &s)
            .0
            .iter()
            .filter_map(|i| match i {
                L::FloatArith { op: FloatArithOp::Mul, .. } => Some('M'),
                L::FloatArith { op: FloatArithOp::Div, .. } => Some('D'),
                L::FloatUnary { op: FloatUnaryOp::Round, .. } => Some('R'),
                _ => None,
            })
            .collect()
    };
    assert_eq!(steps(2), "MRD");
    assert_eq!(steps(-2), "DRM");
}

fn func(f: NumFunc, arg: BoundExpr) -> BoundExpr {
    BoundExpr::Func { f, arg: Box::new(arg) }
}

/// A negation computes in the i64 register, so ABS over it must compute too —
/// even over an unsigned column, whose own ABS is the identity.
#[test]
fn abs_of_a_negated_unsigned_column_computes_both_kernels() {
    let s = cast_schema(); // col 2 = u32
    let neg = func(NumFunc::Unary(FloatUnaryOp::Neg), BoundExpr::ColRef(2));
    let instrs = lower_instrs(&func(NumFunc::Unary(FloatUnaryOp::Abs), neg), &s);
    assert!(
        matches!(
            instrs[..],
            [
                L::LoadColInt { .. },
                L::IntUnary { op: IntUnaryOp::Neg, .. },
                L::IntUnary { op: IntUnaryOp::Abs, .. }
            ]
        ),
        "{instrs:?}"
    );
}

/// The identity folds: rounding an integer is the integer, and an unsigned
/// register value is already non-negative. Both must emit nothing — the f64
/// lift a naive FLOOR would take mangles any integer >= 2^53.
#[test]
fn integer_arguments_fold_the_unary_transforms_away() {
    let s = cast_schema(); // col 1 = i32, col 2 = u32, col 4 = u64
    for f in [
        NumFunc::Unary(FloatUnaryOp::Floor),
        NumFunc::Unary(FloatUnaryOp::Ceil),
        NumFunc::Unary(FloatUnaryOp::Round),
        NumFunc::Round(2),
        NumFunc::Unary(FloatUnaryOp::Trunc),
    ] {
        let instrs = lower_instrs(&func(f, BoundExpr::ColRef(1)), &s);
        assert!(!has(&instrs, is_float_op), "{f:?}(i32col) folds away");
    }
    let int_abs = |i: &LogicalInstr| matches!(i, L::IntUnary { op: IntUnaryOp::Abs, .. });
    for c in [2usize, 4] {
        let instrs = lower_instrs(&func(NumFunc::Unary(FloatUnaryOp::Abs), BoundExpr::ColRef(c)), &s);
        assert!(!has(&instrs, int_abs), "ABS over an unsigned column folds away");
    }
    // A signed integer argument does need the opcode.
    assert!(has(
        &lower_instrs(&func(NumFunc::Unary(FloatUnaryOp::Abs), BoundExpr::ColRef(1)), &s),
        int_abs
    ));
    // A float argument takes the float opcode in every case.
    assert!(has(
        &lower_instrs(&func(NumFunc::Unary(FloatUnaryOp::Floor), lit("1.5")), &s),
        |i| matches!(i, L::FloatUnary { op: FloatUnaryOp::Floor, .. })
    ));
}

fn min_max(is_max: bool, args: Vec<BoundExpr>) -> BoundExpr {
    BoundExpr::MinMaxN { is_max, args }
}

/// The fold is a left chain of 2-ary opcodes: n loads and n-1 folds, so the
/// register count is linear in arity rather than squared.
#[test]
fn min_max_n_folds_linearly_and_picks_the_float_domain() {
    let s = cast_schema();
    for n in [1usize, 3] {
        let args: Vec<BoundExpr> = (0..n).map(|_| BoundExpr::ColRef(1)).collect();
        let instrs = lower_instrs(&min_max(true, args), &s);
        assert_eq!(
            count(&instrs, |i| matches!(i, L::IntMinMax2 { is_max: true, .. })),
            n - 1,
            "arity {n}: n-1 folds"
        );
    }
    assert!(has(
        &lower_instrs(&min_max(false, vec![BoundExpr::ColRef(1); 2]), &s),
        |i| matches!(i, L::IntMinMax2 { is_max: false, .. })
    ));
    // One float argument lifts every integer argument and switches the whole
    // fold to the float opcodes — the same global unification CASE applies.
    let mixed = min_max(true, vec![BoundExpr::ColRef(1), lit("1.5")]);
    let instrs = lower_instrs(&mixed, &s);
    assert!(has(&instrs, |i| matches!(i, L::FloatMinMax2 { is_max: true, .. })));
    assert!(has(&instrs, |i| matches!(i, L::IntToFloat { .. })));
    assert!(!has(&instrs, |i| matches!(i, L::IntMinMax2 { .. })));
}

/// With a U64 argument anywhere in the list, one U64 argument is rotated to
/// the head of the fold. The engine's unsigned taint only exists from the
/// first U64 operand onward, so without the rotation an earlier signed pair
/// would compare signed and the result would depend on argument order.
#[test]
fn min_max_n_rotates_a_u64_argument_to_the_fold_head() {
    let s = cast_schema(); // col 4 = u64
                           // The fold's head operand: the first instruction's column.
    let head_operand = |args: Vec<BoundExpr>| -> u32 {
        let mut eb = ExprBuilder::new();
        let (reg, _) = OpcodeBackend { cols: &s.columns, eb: &mut eb }
            .lower(&min_max(true, args))
            .expect("lowers");
        let prog = eb.build(vec![Sink::Reg(reg)]).expect("a well-formed program");
        match prog.instrs().first().expect("non-empty") {
            &LogicalInstr::LoadColInt { col, .. } => col,
            other => panic!("the fold must open with a column load, got {other:?}"),
        }
    };
    let neg1 = BoundExpr::LitInt(-1);
    let u = BoundExpr::ColRef(4);
    assert_eq!(
        head_operand(vec![neg1.clone(), BoundExpr::LitInt(1), u.clone()]),
        head_operand(vec![u.clone(), neg1, BoundExpr::LitInt(1)]),
        "the U64 argument heads the fold in either written order"
    );
}

/// A register file holds an 8-byte image, so the 16-byte types and the
/// German strings have no extremum here. The backend holds the column defs,
/// so this is its rejection to make — the schema-free binder cannot.
#[test]
fn min_max_n_rejects_non_numeric_arguments() {
    let s = Schema {
        columns: vec![
            col("pk", TypeCode::U64),
            col("s", TypeCode::String),
            col("w", TypeCode::U128),
        ],
        pk_cols: vec![0],
    };
    for c in [1usize, 2] {
        let err = lower_err(&min_max(true, vec![BoundExpr::ColRef(c), BoundExpr::ColRef(c)]), &s);
        assert!(matches!(err, GnitzSqlError::Rejected(_)), "column {c}");
    }
}

/// The register file bounds arity.
#[test]
fn min_max_n_arity_is_bounded_by_the_register_file() {
    let s = cast_schema();
    // Distinct literals: the builder folds identical instructions, so forty
    // reads of one column would be one register.
    let args: Vec<BoundExpr> = (0..40).map(BoundExpr::LitInt).collect();
    let mut eb = ExprBuilder::new();
    let (reg, _) = OpcodeBackend { cols: &s.columns, eb: &mut eb }
        .lower(&min_max(true, args))
        .expect("lowering itself does not bound arity");
    match eb.build(vec![Sink::Reg(reg)]).map_err(GnitzSqlError::from).err() {
        Some(GnitzSqlError::Rejected(msg)) => assert!(msg.contains("reg"), "got {msg:?}"),
        other => panic!("expected a register-budget rejection, got {other:?}"),
    }
}

/// A float source truncates through `FLOAT_TO_INT`; an integer source
/// widens through `INT_TO_FLOAT`, and only an F32 target rounds afterwards.
#[test]
fn cast_picks_the_conversion_opcode_from_the_source_domain() {
    let s = cast_schema();
    let cast = |e: BoundExpr, to: TypeCode| BoundExpr::Cast { expr: Box::new(e), to: to.into() };
    let f = lit("2.7");

    let to_f32 = |i: &LogicalInstr| matches!(i, L::FloatToF32 { .. });
    let lift = |i: &LogicalInstr| matches!(i, L::IntToFloat { .. });

    assert!(has(&lower_instrs(&cast(f.clone(), TypeCode::I32), &s), |i| matches!(
        i,
        L::FloatToInt { .. }
    )));
    // float -> DOUBLE is a no-op: the register already holds an f64.
    let instrs = lower_instrs(&cast(f.clone(), TypeCode::F64), &s);
    assert!(!has(&instrs, to_f32) && !has(&instrs, lift));
    assert!(has(&lower_instrs(&cast(f, TypeCode::F32), &s), to_f32));

    let instrs = lower_instrs(&cast(BoundExpr::ColRef(1), TypeCode::F64), &s);
    assert!(has(&instrs, lift) && !has(&instrs, to_f32));
    // int -> FLOAT is the two-step lift; the double rounding is committed.
    let instrs = lower_instrs(&cast(BoundExpr::ColRef(1), TypeCode::F32), &s);
    assert!(has(&instrs, lift) && has(&instrs, to_f32));
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

/// The column/literal interception *declines* a non-comparison operator
/// rather than erroring, so `strcol || 'lit'` reaches the register channel;
/// the rejection of a genuinely undefined operator moves there and still
/// names it.
#[test]
fn string_cmp_interception_declines_non_comparisons() {
    let schema = str_schema();
    let s = BoundExpr::ColRef(1);
    let lit = BoundExpr::LitStr("x".to_string());
    let mut eb = ExprBuilder::new();
    assert!(OpcodeBackend { cols: &schema.columns, eb: &mut eb }
        .string_cmp(&lit, BinOp::Add, &s)
        .is_none());
    assert!(
        eb.build(vec![]).expect("a well-formed program").instrs().is_empty(),
        "a declined shape must emit nothing"
    );

    let add = BoundExpr::bin(lit, BinOp::Add, s);
    let err = lower_err(&add, &schema);
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

/// A BLOB comparison lowers to the same German-string content opcodes as
/// STRING: blob col-vs-col and col-vs-literal compile byte-identically to the
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

fn case_schema() -> Schema {
    Schema {
        columns: vec![
            col("pk", TypeCode::U64),
            col("i", TypeCode::I64),
            col("f", TypeCode::F64),
        ],
        pk_cols: vec![0],
    }
}

/// CASE lowers to a SELECT; float unification is one global decision —
/// an all-int CASE lifts nothing, a mixed int/float CASE lifts every int
/// branch to float up front (never a per-pair blend of int/float bits).
#[test]
fn case_float_unification_lifts_int_branches() {
    let schema = case_schema();
    let gt0 = || BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::LitInt(0));

    // All-int CASE: a SELECT, but no float lift.
    let case_int = BoundExpr::Case {
        branches: vec![(gt0(), BoundExpr::ColRef(1))],
        else_: Some(Box::new(BoundExpr::LitInt(0))),
    };
    let prog = compile_bound_expr_to_program(&case_int, &schema.columns).unwrap();
    assert!(
        has(prog.instrs(), |i| matches!(i, L::Select { .. })),
        "CASE must lower to a SELECT"
    );
    assert!(
        !has(prog.instrs(), |i| matches!(i, L::IntToFloat { .. })),
        "all-int CASE needs no float lift"
    );

    // Mixed int/float CASE: the int else is lifted to float up front.
    let case_mixed = BoundExpr::Case {
        branches: vec![(gt0(), BoundExpr::ColRef(2))],
        else_: Some(Box::new(BoundExpr::ColRef(1))),
    };
    let prog = compile_bound_expr_to_program(&case_mixed, &schema.columns).unwrap();
    assert!(
        has(prog.instrs(), |i| matches!(i, L::Select { .. })),
        "CASE must lower to a SELECT"
    );
    assert!(
        has(prog.instrs(), |i| matches!(i, L::IntToFloat { .. })),
        "mixed CASE lifts int branches to float"
    );
}

/// A string-typed CASE lowers through the string channel to a `StrSelect`
/// fold, and its result is a string register.
#[test]
fn case_string_branches_compile_through_the_string_channel() {
    let schema = case_schema();
    let case_str = BoundExpr::Case {
        branches: vec![(
            BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::LitInt(0)),
            BoundExpr::LitStr("x".into()),
        )],
        else_: Some(Box::new(BoundExpr::LitStr("y".into()))),
    };
    assert_eq!(lower_instrs_kind(&case_str, &schema).1, ExprKind::Str);
    // The engine's class validator is the real check: it rejects a scalar
    // register reaching a string operand, so a CASE that blended its string
    // branches with the numeric SELECT would fail here rather than compile.
    assert_str_program(&case_str, &schema);
}

// ------------------------------------------------------------------
// IN-list lowering: `IntInSet` fast path vs OR-chain fallback
// ------------------------------------------------------------------

/// col0 = pk (U64), col1 = a (I64), col2 = b (I64).
fn two_int_schema() -> Schema {
    Schema {
        columns: vec![
            col("pk", TypeCode::U64),
            col("a", TypeCode::I64),
            col("b", TypeCode::I64),
        ],
        pk_cols: vec![0],
    }
}

fn in_list(inner: BoundExpr, items: Vec<BoundExpr>) -> BoundExpr {
    BoundExpr::InList { inner: Box::new(inner), items }
}

/// An integer operand with all-integer-literal items → one `IntInSet`; the
/// binder folds a negated literal, so a negative item is a literal like any
/// other.
#[test]
fn in_list_int_emits_int_in_set() {
    let schema = two_int_schema();
    let items = vec![BoundExpr::LitInt(-1), BoundExpr::LitInt(2)];
    let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
    assert!(matches!(prog.instrs(), [L::LoadColInt { .. }, L::IntInSet { .. }]));
}

/// The pool is sorted and deduplicated: `a IN (1, 1, 2)` packs 2 i64s (16
/// bytes), not 3.
#[test]
fn in_list_int_pool_is_sorted_and_deduped() {
    let schema = two_int_schema();
    let items = vec![BoundExpr::LitInt(2), BoundExpr::LitInt(1), BoundExpr::LitInt(1)];
    let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
    assert_eq!(prog.const_strings().len(), 1, "one const-pool entry (the packed set)");
    assert_eq!(
        prog.const_strings()[0].len(),
        2 * 8,
        "duplicate 1 must collapse: 2 i64s = 16 bytes"
    );
    // Packed ascending: [1, 2].
    assert_eq!(&prog.const_strings()[0][0..8], &1i64.to_le_bytes());
    assert_eq!(&prog.const_strings()[0][8..16], &2i64.to_le_bytes());
}

/// The motivating fix: a large integer IN list compiles to O(1) registers.
/// The OR-chain needs ~4N registers and blows the 64-register cap at N=17
/// (`TooManyRegs`); the fast path is register-flat regardless of N.
#[test]
fn in_list_large_int_list_compiles_within_register_cap() {
    let schema = two_int_schema();
    let items: Vec<BoundExpr> = (0..500).map(BoundExpr::LitInt).collect();
    let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
    assert!(
        prog.instrs().len() <= 4,
        "membership is O(1) registers; got {} for a 500-element list",
        prog.instrs().len()
    );
    assert_eq!(
        prog.const_strings()[0].len(),
        500 * 8,
        "the whole set rides one pool entry"
    );
}

/// A float operand falls back to the OR-chain (int-cast + fcmp), never
/// `IntInSet`.
#[test]
fn in_list_float_operand_falls_back_to_or_chain() {
    let schema = case_schema(); // col2 = f (F64)
    let prog = compile_bound_expr_to_program(
        &in_list(BoundExpr::ColRef(2), vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)]),
        &schema.columns,
    )
    .unwrap();
    assert!(
        !has(prog.instrs(), |i| matches!(i, L::IntInSet { .. })),
        "float IN must not emit IntInSet"
    );
    assert!(
        has(prog.instrs(), |i| matches!(i, L::FCmp { op: CmpOp::Eq, .. })),
        "float IN lowers to fcmp OR-chain"
    );
}

/// A string operand falls back to the OR-chain (str_col_eq_const).
#[test]
fn in_list_string_operand_falls_back_to_or_chain() {
    let schema = str_schema(); // col1 = s (String)
    let prog = compile_bound_expr_to_program(
        &in_list(
            BoundExpr::ColRef(1),
            vec![BoundExpr::LitStr("a".into()), BoundExpr::LitStr("b".into())],
        ),
        &schema.columns,
    )
    .unwrap();
    assert!(
        !has(prog.instrs(), |i| matches!(i, L::IntInSet { .. })),
        "string IN must not emit IntInSet"
    );
    assert!(
        has(prog.instrs(), |i| matches!(i, L::StrColConst { op: CmpOp::Eq, .. })),
        "string IN lowers to str_col_eq_const"
    );
}

/// A string `IN` list is an OR chain over fused compares: one register per
/// compare plus one per OR. Through the register channel each term would
/// spend three, which is what puts a longer list over the 64-register cap.
#[test]
fn in_list_string_list_spends_one_register_per_fused_compare() {
    let schema = str_schema();
    let items: Vec<BoundExpr> = (0..22).map(|i| BoundExpr::LitStr(format!("tag{i}"))).collect();
    let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
    assert_eq!(prog.instrs().len(), 2 * 22 - 1);
}

/// The operand costs one register for the whole list, not one per item: each
/// term re-lowers it and the builder folds the identical instructions — which
/// is what puts `UPPER(c) IN (…14 items…)` inside the register budget at all.
#[test]
fn in_list_shares_a_computed_operand_across_the_fold() {
    let schema = str_schema();
    let upper = BoundExpr::StrCall {
        f: StrFunc::Upper,
        args: vec![str_col(1)],
    };
    let items: Vec<BoundExpr> = (0..14).map(|i| BoundExpr::LitStr(format!("tag{i}"))).collect();
    let prog = compile_bound_expr_to_program(&in_list(upper, items), &schema.columns)
        .expect("the shared operand fits the register budget");
    assert_eq!(
        count(prog.instrs(), |i| matches!(i, L::StrCase { upper: true, .. })),
        1,
        "the operand is folded once, not once per item: {:?}",
        prog.instrs()
    );
}

/// An integer column operand is never fused into the per-item compare, so its
/// load is shared the same way: one for the whole list rather than one per
/// item. (A literal list would take `IntInSet`; one non-literal item is
/// what forces the fold.)
#[test]
fn in_list_shares_an_integer_column_operand() {
    let schema = two_int_schema();
    let prog = compile_bound_expr_to_program(
        &in_list(
            BoundExpr::ColRef(1),
            vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2), BoundExpr::ColRef(2)],
        ),
        &schema.columns,
    )
    .unwrap();
    assert_eq!(
        count(prog.instrs(), |i| matches!(i, L::LoadColInt { .. })),
        2,
        "one load for the operand, one for the column item: {:?}",
        prog.instrs()
    );
}

/// A *literal* operand stays fused: `'x' IN (col_a, col_b)` binds with the
/// literal on the left, and the fused column comparison reads it through the
/// converse arm rather than pushing the pair onto the register channel and
/// losing the 4-byte-prefix short circuit.
#[test]
fn in_list_keeps_the_fused_compare_for_a_literal_operand() {
    let schema = str_schema();
    let prog =
        compile_bound_expr_to_program(&in_list(str_lit("x"), vec![str_col(1), str_col(2)]), &schema.columns).unwrap();
    assert!(matches!(
        prog.instrs(),
        [
            L::StrColConst { op: CmpOp::Eq, .. },
            L::StrColConst { op: CmpOp::Eq, .. },
            L::BoolBinary { is_or: true, .. }
        ]
    ));
}

/// A non-literal item (a column) forces the OR-chain even for an int operand.
#[test]
fn in_list_non_literal_item_falls_back_to_or_chain() {
    let schema = two_int_schema();
    let prog = compile_bound_expr_to_program(
        &in_list(BoundExpr::ColRef(1), vec![BoundExpr::LitInt(1), BoundExpr::ColRef(2)]),
        &schema.columns,
    )
    .unwrap();
    assert!(
        !has(prog.instrs(), |i| matches!(i, L::IntInSet { .. })),
        "non-literal item must not emit IntInSet"
    );
    assert!(
        has(prog.instrs(), |i| matches!(i, L::Cmp { op: CmpOp::Eq, .. })),
        "non-literal item lowers to cmp OR-chain"
    );
}

/// A fractional item names no integer, so it drops out of the set; a literal
/// that places nowhere (`1e400`, past every decimal) keeps the OR chain.
#[test]
fn in_list_places_its_items_among_the_operand_values() {
    let schema = two_int_schema();
    let prog = compile_bound_expr_to_program(
        &in_list(BoundExpr::ColRef(1), vec![lit("1.5"), BoundExpr::LitInt(2)]),
        &schema.columns,
    )
    .unwrap();
    assert!(has(prog.instrs(), |i| matches!(i, L::IntInSet { .. })));
    assert_eq!(prog.const_strings(), [gnitz_wire::as_le_bytes(&[2i64]).to_vec()]);

    let prog = compile_bound_expr_to_program(
        &in_list(BoundExpr::ColRef(1), vec![BoundExpr::LitInt(1), lit("1e400")]),
        &schema.columns,
    )
    .unwrap();
    assert!(!has(prog.instrs(), |i| matches!(i, L::IntInSet { .. })));
}

/// A wide literal against a 64-bit column is decided by where it falls; one
/// that no register holds is refused anywhere else, naming where it is usable.
#[test]
fn lit_wide_is_placed_or_rejected_at_compile_boundary() {
    let schema = two_int_schema(); // (pk U64, a I64, b I64)
    let expr = BoundExpr::bin(
        BoundExpr::ColRef(1),
        BinOp::Eq,
        BoundExpr::LitWide(NumLit { mag: u64::MAX.into(), neg: false }),
    );
    let prog = compile_bound_expr_to_program(&expr, &schema.columns).unwrap();
    assert!(matches!(
        prog.instrs(),
        [L::LoadColInt { col: 1 }, L::Cmp { op: CmpOp::Ne, a: Reg(0), b: Reg(0) }]
    ));

    let expr = BoundExpr::bin(
        BoundExpr::ColRef(1),
        BinOp::Add,
        BoundExpr::LitWide(NumLit { mag: 1 << 64, neg: false }),
    );
    let err = compile_bound_expr_to_program(&expr, &schema.columns).expect_err("wide literal must not compile");
    match err {
        GnitzSqlError::Rejected(msg) => {
            assert!(msg.contains("does not fit a 64-bit register"), "message: {msg}");
            assert!(msg.contains("18446744073709551616"), "message names the literal: {msg}");
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

/// A wide-int (U128) operand takes the OR-chain, whose integer-load path has no
/// 16-byte slot and rejects.
#[test]
fn in_list_wide_int_operand_rejects() {
    let schema = Schema {
        columns: vec![col("pk", TypeCode::U64), col("w", TypeCode::U128)],
        pk_cols: vec![0],
    };
    let err = compile_bound_expr_to_program(
        &in_list(BoundExpr::ColRef(1), vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)]),
        &schema.columns,
    )
    .expect_err("wide-int IN must not compile");
    assert!(
        matches!(err, GnitzSqlError::Rejected(_)),
        "expected Unsupported, got {err:?}"
    );
}

// -----------------------------------------------------------------------
// The string channel
// -----------------------------------------------------------------------

fn str_col(i: usize) -> BoundExpr {
    BoundExpr::ColRef(i)
}
fn str_lit(s: &str) -> BoundExpr {
    BoundExpr::LitStr(s.to_string())
}

/// A plain `col op 'lit'` or `col op col` keeps the fused 16-byte-cell
/// opcodes; the register channel exists for computed operands. The fused
/// kernel short-circuits on the cell's 4-byte prefix, which a `StrView` does
/// not carry, and spends one register where the channel spends three.
#[test]
fn plain_column_comparisons_keep_the_specialized_opcodes() {
    let schema = str_schema();
    let instrs = lower_instrs(&BoundExpr::bin(str_col(1), BinOp::Eq, str_lit("x")), &schema);
    assert!(matches!(instrs[..], [L::StrColConst { op: CmpOp::Eq, .. }]));
    let cols = lower_instrs(&BoundExpr::bin(str_col(1), BinOp::Lt, str_col(2)), &schema);
    assert!(matches!(cols[..], [L::StrColCol { op: CmpOp::Lt, .. }]));
}

/// A *computed* operand has no specialized form, so it falls through to the
/// register compare. The engine's validator is what proves the operands were
/// built in the right class.
#[test]
fn computed_operands_compare_through_the_register_channel() {
    let schema = str_schema();
    let upper_eq = BoundExpr::bin(
        BoundExpr::StrCall {
            f: StrFunc::Upper,
            args: vec![str_col(1)],
        },
        BinOp::Eq,
        str_lit("X"),
    );
    let instrs = lower_instrs(&upper_eq, &schema);
    assert!(
        has(&instrs, |i| matches!(i, L::StrCmp { op: CmpOp::Eq, .. })),
        "{instrs:?}"
    );
    assert!(!has(&instrs, |i| matches!(i, L::StrColConst { .. })), "{instrs:?}");
    // The comparison is a boolean, so it is a legitimate filter predicate.
    let p = compile_bound_expr_to_program(&upper_eq, &schema.columns).expect("lowers");
    assert!(p.resolve_filter(&schema).is_ok());
}

/// Every comparison has its own opcode on the register channel too, so none
/// pays a swap or a `BoolNot`.
#[test]
fn register_compare_has_all_six_operators() {
    let schema = str_schema();
    let up = |i| BoundExpr::StrCall {
        f: StrFunc::Upper,
        args: vec![str_col(i)],
    };
    for (op, want) in [
        (BinOp::Eq, CmpOp::Eq),
        (BinOp::Ne, CmpOp::Ne),
        (BinOp::Gt, CmpOp::Gt),
        (BinOp::Ge, CmpOp::Ge),
        (BinOp::Lt, CmpOp::Lt),
        (BinOp::Le, CmpOp::Le),
    ] {
        let instrs = lower_instrs(&BoundExpr::bin(up(1), op, up(2)), &schema);
        assert!(
            matches!(
                instrs[..],
                [
                    L::LoadColStr { .. },
                    L::StrCase { upper: true, .. },
                    L::LoadColStr { .. },
                    L::StrCase { upper: true, .. },
                    L::StrCmp { op: got, .. },
                ] if got == want
            ),
            "{op:?}: {instrs:?}"
        );
    }
}

#[test]
fn concat_operator_and_function_compile_and_differ_in_their_null_rule() {
    let schema = str_schema();
    // `||` is NULL-propagating, so `s || NULL` is a NULL string rather than a
    // type error — `str_operand` intercepts the literal before recursing.
    let pipe_null = BoundExpr::bin(str_col(1), BinOp::Concat, BoundExpr::LitNull);
    assert_str_program(&pipe_null, &schema);
    assert!(has(&lower_instrs(&pipe_null, &schema), |i| matches!(i, L::LoadNullStr)));

    let pipe = BoundExpr::bin(str_col(1), BinOp::Concat, str_lit("x"));
    assert!(has(&lower_instrs(&pipe, &schema), |i| matches!(
        i,
        L::StrConcat { skip_null: false, .. }
    )));

    // CONCAT folds through the null-as-empty step instead, and seeds the fold
    // so even a single argument is non-NULL.
    let one = BoundExpr::ConcatN { args: vec![str_col(1)] };
    let instrs = lower_instrs(&one, &schema);
    assert!(
        has(&instrs, |i| matches!(i, L::StrConcat { skip_null: true, .. })),
        "{instrs:?}"
    );
    assert!(
        !has(&instrs, |i| matches!(i, L::StrConcat { skip_null: false, .. })),
        "{instrs:?}"
    );
    assert_str_program(&one, &schema);
}

/// CONCAT is the one place a numeric argument is cast to text implicitly;
/// `||` is not, so it stays a typed error there.
#[test]
fn concat_casts_numeric_arguments_but_the_operator_does_not() {
    let schema = str_schema();
    let mixed = BoundExpr::ConcatN {
        args: vec![str_col(1), BoundExpr::LitInt(42), lit("1.5")],
    };
    let instrs = lower_instrs(&mixed, &schema);
    assert!(has(&instrs, |i| matches!(i, L::IntToStr { .. })), "{instrs:?}");
    assert!(has(&instrs, |i| matches!(i, L::FloatToStr { .. })), "{instrs:?}");
    assert_str_program(&mixed, &schema);

    let pipe_int = BoundExpr::bin(str_col(1), BinOp::Concat, BoundExpr::LitInt(1));
    assert!(lower_err(&pipe_int, &schema).to_string().contains("string"));
}

/// NULLIF on strings desugars to `CASE WHEN a = b THEN NULL ELSE a END`, so
/// it exercises both halves of the rule at once: the comparison compiles
/// through the string channel, and the `LitNull` branch must become a NULL
/// *string* or the CASE would blend two classes.
#[test]
fn string_nullif_and_coalesce_desugars_compile() {
    let schema = str_schema();
    let nullif = BoundExpr::Case {
        branches: vec![(BoundExpr::bin(str_col(1), BinOp::Eq, str_col(2)), BoundExpr::LitNull)],
        else_: Some(Box::new(str_col(1))),
    };
    assert_str_program(&nullif, &schema);

    let coalesce = BoundExpr::Case {
        branches: vec![(
            BoundExpr::NullTest {
                inner: Box::new(BoundExpr::ColRef(1)),
                want_null: false,
            },
            str_col(1),
        )],
        else_: Some(Box::new(str_lit("default"))),
    };
    assert_str_program(&coalesce, &schema);
}

/// An all-NULL CASE carries no type signal at all, so it keeps the
/// pre-existing "ambiguous NULL defaults to I64" behaviour rather than
/// silently becoming a string.
#[test]
fn an_all_null_case_still_types_as_an_integer() {
    let schema = str_schema();
    let all_null = BoundExpr::Case {
        branches: vec![(BoundExpr::LitInt(1), BoundExpr::LitNull)],
        else_: None,
    };
    assert_eq!(all_null.infer_ty(&schema.columns).tc, TypeCode::I64);
    assert_eq!(lower_instrs_kind(&all_null, &schema).1, ExprKind::Int);
}

#[test]
fn mixed_string_and_numeric_case_branches_are_a_typed_error() {
    let schema = str_schema();
    let mixed = BoundExpr::Case {
        branches: vec![(BoundExpr::LitInt(1), str_lit("x"))],
        else_: Some(Box::new(BoundExpr::LitInt(0))),
    };
    assert!(matches!(lower_err(&mixed, &schema), GnitzSqlError::Rejected(_)));
}

/// Every numeric position reads its operands through `lower_num`, so a
/// string there is a SQL error rather than an engine-side class mismatch the
/// user would see as an opaque internal enum.
#[test]
fn strings_in_numeric_positions_are_rejected_by_lowering() {
    let schema = str_schema();
    let s = || str_col(1);
    let cases: Vec<BoundExpr> = vec![
        BoundExpr::bin(s(), BinOp::Add, BoundExpr::LitInt(1)),
        BoundExpr::bin(s(), BinOp::And, str_lit("x")),
        BoundExpr::bin(s(), BinOp::Or, str_lit("x")),
        BoundExpr::Func {
            f: NumFunc::Unary(FloatUnaryOp::Abs),
            arg: Box::new(s()),
        },
        BoundExpr::Func { f: NumFunc::Round(2), arg: Box::new(s()) },
        BoundExpr::MinMaxN {
            is_max: true,
            args: vec![s(), str_lit("x")],
        },
        func(NumFunc::Unary(FloatUnaryOp::Neg), s()),
        BoundExpr::Not(Box::new(s())),
        BoundExpr::StrCall { f: StrFunc::Substr, args: vec![s(), s()] },
    ];
    for e in &cases {
        assert!(
            matches!(lower_err(e, &schema), GnitzSqlError::Rejected(_)),
            "{e:?} must be rejected by lowering"
        );
    }
    // A comparison carries no implicit cast either way — against a literal or
    // against an integer column (`pk` here).
    let mixed = BoundExpr::bin(s(), BinOp::Eq, BoundExpr::LitInt(1));
    assert!(lower_err(&mixed, &schema).to_string().contains("strings"));
    let mixed_cols = BoundExpr::bin(s(), BinOp::Gt, BoundExpr::ColRef(0));
    assert!(lower_err(&mixed_cols, &schema).to_string().contains("strings"));
}

/// BLOB keeps exactly its existing comparison support: the column/literal
/// shapes compile, and everything else — including the string functions —
/// rejects.
#[test]
fn blob_columns_stay_outside_the_string_surface() {
    let schema = blob_schema();
    let cmp = BoundExpr::bin(str_col(1), BinOp::Eq, str_lit("x"));
    assert!(matches!(
        lower_instrs(&cmp, &schema)[..],
        [L::StrColConst { op: CmpOp::Eq, .. }]
    ));

    let upper = BoundExpr::StrCall {
        f: StrFunc::Lower,
        args: vec![str_col(1)],
    };
    assert!(lower_err(&upper, &schema).to_string().contains("blob"));
}

/// A string source must never reach the numeric elide test: STRING's
/// register image is I64, so it would match and drop the cast, reading the
/// 16-byte descriptor as an integer.
#[test]
fn cast_from_a_string_emits_a_parse_and_is_never_elided() {
    let schema = str_schema();
    let to = |tc: TypeCode| BoundExpr::Cast {
        expr: Box::new(str_col(1)),
        to: tc.into(),
    };
    assert!(matches!(
        lower_instrs(&to(TypeCode::I64), &schema)[..],
        [L::LoadColStr { .. }, L::StrToInt { .. }]
    ));
    assert!(matches!(
        lower_instrs(&to(TypeCode::F64), &schema)[..],
        [L::LoadColStr { .. }, L::StrToFloat { .. }]
    ));
    assert!(matches!(
        lower_instrs(&to(TypeCode::F32), &schema)[..],
        [L::LoadColStr { .. }, L::StrToFloat { .. }, L::FloatToF32 { .. }]
    ));
    // STRING → STRING is the identity.
    assert!(matches!(
        lower_instrs(&to(TypeCode::String), &schema)[..],
        [L::LoadColStr { .. }]
    ));
}

#[test]
fn cast_to_text_emits_the_numeric_to_text_opcode_for_its_source_domain() {
    let schema = cast_schema();
    let to_text = |c| BoundExpr::Cast {
        expr: Box::new(BoundExpr::ColRef(c)),
        to: TypeCode::String.into(),
    };
    assert!(has(&lower_instrs(&to_text(1), &schema), |i| matches!(
        i,
        L::IntToStr { .. }
    )));
    let f = BoundExpr::Cast {
        expr: Box::new(lit("1.5")),
        to: TypeCode::String.into(),
    };
    assert!(has(&lower_instrs(&f, &schema), |i| matches!(i, L::FloatToStr { .. })));
    assert_str_program(&to_text(1), &schema);
}

fn like_of(s: BoundExpr, pattern: &str, ci: bool) -> BoundExpr {
    BoundExpr::Like {
        s: Box::new(s),
        pattern: gnitz_expr::LikePattern::encode(pattern, None).unwrap(),
        ci,
    }
}

/// The pattern is a plain pool entry holding the encoded bytes, and `ci` rides
/// on the LIKE instruction itself, not on a second opcode.
#[test]
fn like_lowers_to_one_opcode_over_its_pattern() {
    let schema = str_schema();
    let prog = |ci| compile_bound_expr_to_program(&like_of(str_col(1), "a%", ci), &schema.columns).unwrap();

    let p = prog(false);
    assert!(matches!(
        p.instrs(),
        [L::LoadColStr { .. }, L::StrLike { ci: false, .. }]
    ));
    assert_eq!(
        p.const_strings()[0],
        gnitz_expr::LikePattern::encode("a%", None).unwrap().as_bytes()
    );
    // ILIKE is the same shape, `ci` set.
    assert!(matches!(
        prog(true).instrs(),
        [L::LoadColStr { .. }, L::StrLike { ci: true, .. }]
    ));
}

/// The subject is an ordinary string operand: any expression the string
/// channel produces, including the bare `NULL` its `LitNull` rule catches.
#[test]
fn like_takes_a_computed_subject_and_propagates_a_null_literal() {
    let schema = str_schema();
    let upper = BoundExpr::StrCall {
        f: StrFunc::Upper,
        args: vec![str_col(1)],
    };
    assert!(matches!(
        lower_instrs(&like_of(upper, "A%", false), &schema)[..],
        [L::LoadColStr { .. }, L::StrCase { upper: true, .. }, L::StrLike { .. }]
    ));
    assert!(matches!(
        lower_instrs(&like_of(BoundExpr::LitNull, "a", false), &schema)[..],
        [L::LoadNullStr, L::StrLike { .. }]
    ));
    // The verdict is a boolean, so `NOT LIKE` reads it like any other.
    let negated = BoundExpr::Not(Box::new(like_of(str_col(1), "a", false)));
    assert!(matches!(
        lower_instrs(&negated, &schema)[..],
        [L::LoadColStr { .. }, L::StrLike { .. }, L::BoolNot { .. }]
    ));
}

/// The subject goes through the shared string-operand channel, so a numeric
/// one draws that channel's rejection rather than a LIKE-specific one.
#[test]
fn like_over_a_numeric_operand_is_a_typed_error() {
    let schema = case_schema(); // col1 = i (I64)
    let err = compile_bound_expr_to_program(&like_of(BoundExpr::ColRef(1), "a", false), &schema.columns)
        .expect_err("a numeric subject has no string channel");
    let GnitzSqlError::Rejected(msg) = &err else {
        panic!("expected Unsupported, got {err:?}")
    };
    assert!(msg.contains("expected a string value"), "got {msg}");
}

/// The transcendentals lift an integer argument to float and always answer
/// in the float domain; SIGN stays in its argument's domain, taking the
/// integer opcode over an integer and the float one over a float.
#[test]
fn transcendentals_lift_and_sign_keeps_the_domain() {
    let s = cast_schema(); // col 1 = i32, col 4 = u64
    for want in [
        FloatUnaryOp::Sqrt,
        FloatUnaryOp::Ln,
        FloatUnaryOp::Log10,
        FloatUnaryOp::Exp,
    ] {
        let f = NumFunc::Unary(want);
        let (instrs, is_float) = lower_instrs_isf(&func(f, BoundExpr::ColRef(1)), &s);
        assert!(
            matches!(
                instrs[..],
                [L::LoadColInt { .. }, L::IntToFloat { .. }, L::FloatUnary { op, .. }] if op == want
            ),
            "{f:?}: {instrs:?}"
        );
        assert!(is_float);
        let (instrs, _) = lower_instrs_isf(&func(f, lit("2.0")), &s);
        assert!(
            matches!(instrs.last(), Some(&L::FloatUnary { op, .. }) if op == want),
            "{f:?} over a float takes no lift"
        );
    }
    for c in [1usize, 4] {
        let (instrs, is_float) = lower_instrs_isf(&func(NumFunc::Unary(FloatUnaryOp::Sign), BoundExpr::ColRef(c)), &s);
        assert!(matches!(
            instrs[..],
            [L::LoadColInt { .. }, L::IntUnary { op: IntUnaryOp::Sign, .. }]
        ));
        assert!(!is_float);
    }
    let (instrs, is_float) = lower_instrs_isf(&func(NumFunc::Unary(FloatUnaryOp::Sign), lit("-2.0")), &s);
    assert!(matches!(
        instrs.last(),
        Some(L::FloatUnary { op: FloatUnaryOp::Sign, .. })
    ));
    assert!(is_float);
}

/// POWER lifts both integer operands to float and is a float result.
#[test]
fn power_lifts_both_operands_to_float() {
    let s = cast_schema();
    let pow = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Pow, BoundExpr::LitInt(2));
    let (instrs, is_float) = lower_instrs_isf(&pow, &s);
    assert!(is_float);
    assert!(
        matches!(instrs.last(), Some(L::FloatArith { op: FloatArithOp::Pow, .. })),
        "{instrs:?}"
    );
    // Both operands reach POWER in the float domain. The column takes the lift;
    // the literal's lift is folded to a float constant by `ExprBuilder::emit`,
    // so it is a `LoadConst` of the f64 bit pattern and not a second
    // `IntToFloat`.
    assert_eq!(count(&instrs, |i| matches!(i, L::IntToFloat { .. })), 1, "{instrs:?}");
    assert!(
        has(
            &instrs,
            |i| matches!(i, &L::LoadConst { val, unsigned: false } if f64::from_bits(val as u64) == 2.0)
        ),
        "{instrs:?}"
    );
}

/// Every string function lowers to its opcode with the class its result type
/// states, reading each argument through the class its signature states.
#[test]
fn string_calls_lower_to_their_opcodes() {
    let schema = str_schema();
    let call = |f, args: Vec<BoundExpr>| BoundExpr::StrCall { f, args };
    let n = || BoundExpr::LitInt(2);
    // The instruction each call must end on, as a predicate over the typed form.
    type Last = fn(&LogicalInstr) -> bool;
    for (expr, last, kind) in [
        (
            call(StrFunc::Reverse, vec![str_col(1)]),
            (|i| matches!(i, L::StrReverse { .. })) as Last,
            ExprKind::Str,
        ),
        (
            call(StrFunc::Left, vec![str_col(1), n()]),
            |i| matches!(i, L::StrSide { left: true, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Right, vec![str_col(1), n()]),
            |i| matches!(i, L::StrSide { left: false, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Pos, vec![str_col(1), str_lit("x")]),
            |i| matches!(i, L::StrPos { .. }),
            ExprKind::Int,
        ),
        (
            call(StrFunc::Replace, vec![str_col(1), str_lit("a"), str_lit("b")]),
            |i| matches!(i, L::StrReplace { .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Lpad, vec![str_col(1), n(), str_lit(" ")]),
            |i| matches!(i, L::StrPad { left: true, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Rpad, vec![str_col(1), n(), str_lit(" ")]),
            |i| matches!(i, L::StrPad { left: false, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::SplitPart, vec![str_col(1), str_lit(","), n()]),
            |i| matches!(i, L::StrSplitPart { .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Substr, vec![str_col(1), n()]),
            |i| matches!(i, L::StrSubstr { len_reg: None, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Substr, vec![str_col(1), n(), n()]),
            |i| matches!(i, L::StrSubstr { len_reg: Some(_), .. }),
            ExprKind::Str,
        ),
    ] {
        let (instrs, got_kind) = lower_instrs_kind(&expr, &schema);
        assert!(instrs.last().is_some_and(last), "{expr:?}: {instrs:?}");
        assert_eq!(got_kind, kind, "{expr:?}");
    }
    // An integer position rejects a float, naming the function and position;
    // a string position rejects a number.
    let err = lower_err(&call(StrFunc::Left, vec![str_col(1), lit("1.5")]), &schema);
    assert!(err.to_string().contains("LEFT: argument 2 must be an integer"), "{err}");
    let err = lower_err(&call(StrFunc::Substr, vec![str_col(1), n(), lit("1.5")]), &schema);
    assert!(
        err.to_string().contains("SUBSTRING: argument 3 must be an integer"),
        "{err}"
    );
    let err = lower_err(&call(StrFunc::Replace, vec![str_col(1), n(), str_lit("b")]), &schema);
    assert!(err.to_string().contains("expected a string value"), "{err}");
}

/// A null test over a column reads the batch bitmap; over anything else it
/// tests the register the value computes into — a string register as readily
/// as a scalar one.
#[test]
fn null_test_lowers_by_its_operand() {
    let schema = str_schema(); // every column nullable
    let test = |inner, want_null| BoundExpr::NullTest { inner: Box::new(inner), want_null };
    assert!(matches!(
        lower_instrs(&test(str_col(1), true), &schema)[..],
        [L::IsNull { invert: false, .. }]
    ));
    assert!(matches!(
        lower_instrs(&test(str_col(1), false), &schema)[..],
        [L::IsNull { invert: true, .. }]
    ));
    let upper = BoundExpr::StrCall {
        f: StrFunc::Upper,
        args: vec![str_col(1)],
    };
    assert!(matches!(
        lower_instrs(&test(upper, true), &schema)[..],
        [
            L::LoadColStr { .. },
            L::StrCase { upper: true, .. },
            L::IsNullReg { invert: false, .. }
        ]
    ));
    let sum = BoundExpr::bin(BoundExpr::ColRef(0), BinOp::Add, BoundExpr::LitInt(1));
    let instrs = lower_instrs(&test(sum, false), &schema);
    assert!(matches!(instrs.last(), Some(L::IsNullReg { invert: true, .. })));
}

/// A true-constant conjunct — the binder's fold of `IS NOT NULL` on a
/// non-nullable column — is dropped wherever it sits, so `a > 1 AND <true>`
/// costs no `BoolBinary` per row; a list of nothing but true constants is the
/// statically-true verdict, and a false constant keeps its program.
#[test]
fn filter_program_drops_true_constant_conjuncts_in_any_position() {
    let schema = two_int_schema();
    let gt = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::LitInt(1));
    let t = BoundExpr::LitInt(1);
    let f = BoundExpr::LitInt(0);
    let instrs = |conjuncts: &[&BoundExpr]| {
        compile_filter_program(conjuncts.iter().copied(), &schema.columns)
            .expect("lowers")
            .map(|p| p.instrs().to_vec())
    };
    let alone = instrs(&[&gt]).expect("a program");
    assert_eq!(instrs(&[&t, &gt, &t]), Some(alone));
    assert_eq!(instrs(&[&t, &t]), None);
    assert_eq!(instrs(&[]), None);
    assert!(matches!(
        instrs(&[&f]).as_deref(),
        Some([L::LoadConst { val: 0, unsigned: false }])
    ));
}

/// A conjunct its connectives settle true over literals is dropped; one they
/// leave open or settle false keeps its program.
#[test]
fn filter_program_drops_conjuncts_settled_true() {
    let schema = two_int_schema();
    let p = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::LitInt(1));
    let (t, f) = (BoundExpr::LitInt(1), BoundExpr::LitInt(0));
    let or = |a: &BoundExpr, b: &BoundExpr| BoundExpr::bin(a.clone(), BinOp::Or, b.clone());
    let and = |a: &BoundExpr, b: &BoundExpr| BoundExpr::bin(a.clone(), BinOp::And, b.clone());
    let not = |a: &BoundExpr| BoundExpr::Not(Box::new(a.clone()));
    let dropped = |e: &BoundExpr| compile_filter_program([e], &schema.columns).expect("lowers").is_none();
    assert!(dropped(&or(&p, &t)));
    assert!(dropped(&not(&and(&f, &p))));
    assert!(dropped(&and(&t, &or(&t, &f))));
    for kept in [or(&f, &p), and(&p, &t), not(&t), and(&p, &f), or(&f, &f)] {
        assert!(!dropped(&kept));
    }
}

/// Every conjunct is a boolean, a lone one included: a string column on its
/// own draws the lowering's own message rather than reaching the resolver.
#[test]
fn a_lone_string_conjunct_is_rejected_by_lowering() {
    let schema = str_schema();
    let err = compile_filter_program([&str_col(1)], &schema.columns).expect_err("a string is not a predicate");
    assert!(err.to_string().contains("strings"), "{err}");
}

/// col 0 = pk (U64), col 1 = p DECIMAL(·,2), col 2 = q DECIMAL(·,3), col 3 = i (I64).
fn decimal_schema() -> Schema {
    Schema {
        columns: vec![
            col("pk", TypeCode::U64),
            ColumnDef::typed("p", gnitz_core::ColType::decimal(2), true),
            ColumnDef::typed("q", gnitz_core::ColType::decimal(3), true),
            col("i", TypeCode::I64),
        ],
        pk_cols: vec![0],
    }
}

/// Evaluate `expr` over one row `(p, q, i)` of the decimal schema, as the
/// stored integers; `p` may be NULL.
fn eval_decimal_row(expr: &BoundExpr, p: impl Into<Option<i64>>, q: i64, i: i64) -> Option<i128> {
    let schema = decimal_schema();
    let mut batch = gnitz_core::ZSetBatch::new(&schema);
    let mut row = gnitz_core::BatchAppender::new(&mut batch, &schema);
    row.add_row(1u128, 1);
    match p.into() {
        Some(p) => row.i64_val(p),
        None => row.null(),
    }
    .i64_val(q)
    .i64_val(i);
    let mut ev = compile_scalar_evaluator(expr, &schema).expect("lowers");
    match ev.eval_all(&batch) {
        gnitz_expr::ExprResults::Int(vals) => vals[0],
        gnitz_expr::ExprResults::Str { .. } => panic!("a scalar expression"),
    }
}

/// DECIMAL lowering is integer arithmetic on the stored values: operands are
/// brought to the wider scale, a product keeps both scales, a literal is
/// folded exactly at the scale it is compared at, and the rounding family is
/// exact half-away-from-zero integer division.
#[test]
fn decimal_arithmetic_lowers_to_scaled_integer_ops() {
    let c = |i: usize| BoundExpr::ColRef(i);
    // p = 1.25, q = 0.005, i = 3
    let (p, q, i) = (125, 5, 3);
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Add, c(2)), p, q, i),
        Some(1255)
    ); // 1.255
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Mul, c(2)), p, q, i),
        Some(625)
    ); // 0.00625
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Mul, c(3)), p, q, i),
        Some(375)
    ); // 3.75
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Sub, BoundExpr::LitInt(1)), p, q, i),
        Some(25)
    ); // 0.25
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Add, lit("0.1")), p, q, i),
        Some(135)
    );
    // A comparison against a longer literal widens the column, never rounds
    // the literal: 1.25 = 1.250 holds, 1.25 = 1.251 does not.
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Eq, lit("1.250")), p, q, i),
        Some(1)
    );
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Eq, lit("1.251")), p, q, i),
        Some(0)
    );
    assert_eq!(
        eval_decimal_row(&BoundExpr::bin(c(1), BinOp::Gt, c(2)), p, q, i),
        Some(1)
    );
    // Division is a float: 1.25 / 0.005 = 250.0.
    let div = BoundExpr::bin(c(1), BinOp::Div, c(2));
    assert_eq!(
        eval_decimal_row(&div, p, q, i).map(|b| f64::from_bits(b as u64)),
        Some(250.0)
    );
}

/// Over a DECIMAL the rounding family is integer arithmetic on the scale, not a
/// float detour: ROUND is half away from zero in both signs, FLOOR/CEIL move
/// toward and away from -∞, and TRUNC drops the fraction.
#[test]
fn decimal_rounding_family_is_exact_integer_division() {
    let (_p, q, i) = (125, 5, 3);
    let c = |i: usize| BoundExpr::ColRef(i);
    let func = |f, e: BoundExpr| BoundExpr::Func { f, arg: Box::new(e) };
    for (f, v, want) in [
        (NumFunc::Round(1), 125, 13),
        (NumFunc::Round(1), -125, -13),
        (NumFunc::Round(1), 124, 12),
        (NumFunc::Unary(FloatUnaryOp::Round), 150, 2),
        (NumFunc::Unary(FloatUnaryOp::Round), -150, -2),
        (NumFunc::Unary(FloatUnaryOp::Floor), -101, -2),
        (NumFunc::Unary(FloatUnaryOp::Floor), 199, 1),
        (NumFunc::Unary(FloatUnaryOp::Ceil), 101, 2),
        (NumFunc::Unary(FloatUnaryOp::Ceil), -199, -1),
        (NumFunc::Unary(FloatUnaryOp::Trunc), -199, -1),
        (NumFunc::Unary(FloatUnaryOp::Abs), -199, 199),
        (NumFunc::Unary(FloatUnaryOp::Neg), 199, -199),
        (NumFunc::Unary(FloatUnaryOp::Sign), -199, -1),
    ] {
        assert_eq!(eval_decimal_row(&func(f, c(1)), v, q, i), Some(want), "{f:?} over {v}");
    }
}

/// CAST to and from a DECIMAL: widening is exact, narrowing rounds half away
/// from zero, and a literal — numeric or the one string form — folds to the
/// constant it is at the target scale.
#[test]
fn decimal_casts_round_at_the_target_scale() {
    let (p, q, i) = (125, 5, 3);
    let c = |i: usize| BoundExpr::ColRef(i);
    let cast = |e: BoundExpr, to| BoundExpr::Cast { expr: Box::new(e), to };
    let dec = gnitz_core::ColType::decimal;
    assert_eq!(eval_decimal_row(&cast(c(3), dec(2)), p, q, i), Some(300));
    assert_eq!(eval_decimal_row(&cast(c(2), dec(2)), p, 5, i), Some(1)); // 0.005 → 0.01
    assert_eq!(eval_decimal_row(&cast(c(2), dec(2)), p, 4, i), Some(0));
    assert_eq!(eval_decimal_row(&cast(c(1), dec(3)), p, q, i), Some(1250));
    assert_eq!(eval_decimal_row(&cast(c(1), TypeCode::I64.into()), 150, q, i), Some(2));
    assert_eq!(eval_decimal_row(&cast(lit("1.005"), dec(2)), p, q, i), Some(101));
    assert_eq!(
        eval_decimal_row(&cast(BoundExpr::LitStr("2.5".into()), dec(2)), p, q, i),
        Some(250)
    );
    assert_eq!(
        eval_decimal_row(&cast(c(1), TypeCode::F64.into()), p, q, i).map(|b| f64::from_bits(b as u64)),
        Some(1.25)
    );
}

/// The `IN` fast path takes a DECIMAL's literals exact at the column's scale —
/// so it stays one `IntInSet` — and a product past the scale cap has no
/// register, so it is refused at plan time.
#[test]
fn decimal_in_set_is_exact_and_a_wide_product_is_refused() {
    let (p, q, i) = (125, 5, 3);
    let c = |i: usize| BoundExpr::ColRef(i);
    let in_list = BoundExpr::InList {
        inner: Box::new(c(1)),
        items: vec![lit("1.25"), BoundExpr::LitInt(2)],
    };
    assert!(has(&lower_instrs(&in_list, &decimal_schema()), |i| matches!(
        i,
        L::IntInSet { .. }
    )));
    assert_eq!(eval_decimal_row(&in_list, p, q, i), Some(1));
    assert_eq!(eval_decimal_row(&in_list, 200, q, i), Some(1));
    assert_eq!(eval_decimal_row(&in_list, 201, q, i), Some(0));
    // A product past the scale cap is refused at plan time.
    let wide = BoundExpr::bin(
        BoundExpr::bin(c(2), BinOp::Mul, c(2)),
        BinOp::Mul,
        BoundExpr::bin(c(2), BinOp::Mul, c(2)),
    );
    let err = compile_bound_expr_to_program(
        &BoundExpr::bin(wide.clone(), BinOp::Mul, wide),
        &decimal_schema().columns,
    );
    assert!(err.unwrap_err().to_string().contains("scale"));
}

/// A blend can name a scale no register can hold — `price + q*q*…` widens the
/// left operand to the product's scale — so the cap belongs on every coercion,
/// not on the product alone. Past it the plan is refused, never scaled by a
/// `10^n` that overflows `i64`.
#[test]
fn a_blend_past_the_scale_cap_is_refused_not_scaled() {
    let s = decimal_schema();
    let c = |i: usize| BoundExpr::ColRef(i);
    // q is scale 3, so q^7 types as scale 21 and p (scale 2) would widen by 19.
    let q7 = (0..6).fold(c(2), |acc, _| BoundExpr::bin(acc, BinOp::Mul, c(2)));
    let err = compile_bound_expr_to_program(&BoundExpr::bin(c(1), BinOp::Add, q7.clone()), &s.columns)
        .expect_err("a scale past the cap has no register");
    assert!(err.to_string().contains("scale"), "{err}");
    let case = BoundExpr::Case {
        branches: vec![(BoundExpr::LitInt(1), c(1))],
        else_: Some(Box::new(q7)),
    };
    let err = compile_bound_expr_to_program(&case, &s.columns).expect_err("same cap through a CASE blend");
    assert!(err.to_string().contains("scale"), "{err}");
}

/// A comparison against a literal finer than the column's scale is decided at
/// the column's scale, in either operand order: every ordering agrees with the
/// exact rational comparison, `=` holds on no row and `<>` on every non-NULL
/// one, and a NULL row stays NULL — including a literal whose scale-up would
/// wrap an `i64`.
#[test]
fn a_comparison_against_a_finer_literal_is_exact() {
    let schema = decimal_schema();
    let ops = [BinOp::Eq, BinOp::Ne, BinOp::Lt, BinOp::Le, BinOp::Gt, BinOp::Ge];
    for text in ["1.005", "1.000000000000000001"] {
        let (v, s) = gnitz_wire::decimal::decimal_of_number_text(text).expect("a decimal");
        for p in [None, Some(100), Some(101), Some(1000), Some(-1000)] {
            for op in ops {
                // p / 10^2 against v / 10^s, cross-multiplied.
                let want = p.map(|p| {
                    let (a, b) = (i128::from(p) * 10i128.pow(u32::from(s)), v * 100);
                    let holds = match op {
                        BinOp::Eq => a == b,
                        BinOp::Ne => a != b,
                        BinOp::Lt => a < b,
                        BinOp::Le => a <= b,
                        BinOp::Gt => a > b,
                        _ => a >= b,
                    };
                    i128::from(holds)
                });
                let col_left = BoundExpr::bin(BoundExpr::ColRef(1), op, lit(text));
                assert_eq!(eval_decimal_row(&col_left, p, 0, 0), want, "{p:?} {op:?} {text}");
                let col_right = BoundExpr::bin(lit(text), op.converse(), BoundExpr::ColRef(1));
                assert_eq!(
                    eval_decimal_row(&col_right, p, 0, 0),
                    want,
                    "{text} {:?} {p:?}",
                    op.converse()
                );
            }
        }
    }
    // One comparison against the neighbour, no scale-up.
    let instrs = lower_instrs(&BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Lt, lit("1.005")), &schema);
    assert!(
        matches!(
            instrs[..],
            [
                L::LoadColInt { .. },
                L::LoadConst { val: 101, unsigned: false },
                L::Cmp { op: CmpOp::Lt, .. }
            ]
        ),
        "{instrs:?}"
    );
}

// ------------------------------------------------------------------
// Comparisons against a literal, and mixed-type operands, against the exact
// answer
// ------------------------------------------------------------------

/// `(pk U64, c1, c2, …)` with the payload columns of types `tcs`, all nullable.
fn typed_schema(tcs: &[TypeCode]) -> Schema {
    let mut columns = vec![ColumnDef::new("pk", TypeCode::U64, false)];
    columns.extend(tcs.iter().enumerate().map(|(i, tc)| col(&format!("c{}", i + 1), *tc)));
    Schema { columns, pk_cols: vec![0] }
}

/// `sql` bound over [`typed_schema`] and evaluated on each row, given as the
/// payload columns' values (`None` is NULL).
fn eval_sql_rows(sql: &str, tcs: &[TypeCode], rows: &[Vec<Option<i128>>]) -> Vec<Option<i128>> {
    let schema = typed_schema(tcs);
    let expr = crate::bind::bind_single_table(&crate::test_support::parse_expr_sql(sql), &schema, "t")
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut batch = gnitz_core::ZSetBatch::new(&schema);
    for (r, row) in rows.iter().enumerate() {
        batch.pks.push_u128(&schema, r as u128);
        batch.weights.push(1);
        let mut nulls = 0u64;
        for (pi, (v, tc)) in row.iter().zip(tcs).enumerate() {
            let fi = FixedInt::from_type_code(*tc).expect("an integer-stored column");
            match v {
                Some(v) => batch.payload[pi]
                    .bytes
                    .extend_from_slice(&fi.pack(*v).to_le_bytes()[..fi.width()]),
                None => {
                    batch.payload[pi].push_zero();
                    gnitz_wire::null_word_set(&mut nulls, pi, true);
                }
            }
        }
        batch.nulls.push(nulls);
    }
    let mut ev = compile_scalar_evaluator(&expr, &schema).unwrap_or_else(|e| panic!("{sql}: {e}"));
    match ev.eval_all(&batch) {
        gnitz_expr::ExprResults::Int(vals) => vals,
        gnitz_expr::ExprResults::Str { .. } => panic!("a scalar expression"),
    }
}

const CMP_SQL: [(&str, &str); 6] = [
    ("=", "="),
    ("<>", "<>"),
    ("<", ">"),
    ("<=", ">="),
    (">", "<"),
    (">=", "<="),
];

fn holds(op: &str, a: i128, b: i128) -> bool {
    match op {
        "=" => a == b,
        "<>" => a != b,
        "<" => a < b,
        "<=" => a <= b,
        ">" => a > b,
        _ => a >= b,
    }
}

/// Every operator in both operand orders, `c1` against each literal, on each
/// row: the VM's answer is the exact one, and a NULL row stays NULL.
fn assert_literal_compares_exact(tc: TypeCode, lits: &[&str], rows: &[Option<i128>]) {
    let table: Vec<Vec<Option<i128>>> = rows.iter().map(|v| vec![*v]).collect();
    for text in lits {
        let (mag, s) = gnitz_wire::decimal::decimal_of_number_text(text.trim_start_matches('-')).expect(text);
        let v = if text.starts_with('-') { -mag } else { mag };
        let scale = 10i128.pow(u32::from(s));
        for (op, conv) in CMP_SQL {
            let want: Vec<Option<i128>> = rows
                .iter()
                .map(|x| x.map(|x| i128::from(holds(op, x * scale, v))))
                .collect();
            for sql in [format!("c1 {op} {text}"), format!("{text} {conv} c1")] {
                assert_eq!(eval_sql_rows(&sql, &[tc], &table), want, "{tc:?}: {sql}");
            }
        }
    }
}

#[test]
fn a_u64_column_against_a_literal_outside_its_range_is_exact() {
    let top = i128::from(u64::MAX);
    assert_literal_compares_exact(
        TypeCode::U64,
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
        TypeCode::I8,
        &["-129", "-128", "127", "128"],
        &[Some(-128), Some(-1), Some(0), Some(127), None],
    );
}

#[test]
fn a_bigint_column_against_a_fraction_is_exact() {
    let p53 = 1i128 << 53;
    assert_literal_compares_exact(
        TypeCode::I64,
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

/// The shapes where one side's type or range cannot be read off the literal.
#[test]
fn a_literal_comparison_reads_the_operand_it_meets() {
    let one = |sql: &str, tcs: &[TypeCode], row: Vec<Option<i128>>| eval_sql_rows(sql, tcs, &[row])[0];
    // The elided cast keeps its U64 tracking, so the constant compares unsigned.
    assert_eq!(
        one(
            "CAST(5 AS BIGINT UNSIGNED) < 18446744073709551615",
            &[TypeCode::I64],
            vec![Some(0)]
        ),
        Some(1)
    );
    // A computed DATE is an unchecked i64 register, so no range is assumed.
    assert_eq!(
        one("COALESCE(c1, 3000000000) = 3000000000", &[TypeCode::Date], vec![None]),
        Some(1)
    );
    // Two literals: the typed one is the operand.
    assert_eq!(
        one("'2020-01-01' = DATE '2020-01-01'", &[TypeCode::I64], vec![Some(0)]),
        Some(1)
    );
}

/// A U64 and a signed operand each read with their own signedness.
#[test]
fn a_u64_and_a_signed_column_compare_exactly() {
    let top = i128::from(u64::MAX);
    let pairs = [(top, -1), (0, -1), (1, 1), (1 << 63, i128::from(i64::MAX)), (5, 7)];
    let rows: Vec<Vec<Option<i128>>> = pairs.iter().map(|&(u, i)| vec![Some(u), Some(i)]).collect();
    for (op, _) in CMP_SQL {
        let want: Vec<Option<i128>> = pairs.iter().map(|&(u, i)| Some(i128::from(holds(op, u, i)))).collect();
        let sql = format!("c1 {op} c2");
        assert_eq!(
            eval_sql_rows(&sql, &[TypeCode::U64, TypeCode::I64], &rows),
            want,
            "{sql}"
        );
    }
}

/// An unsigned constant lifts to the float it is.
#[test]
fn a_float_compare_lifts_a_u64_literal_unsigned() {
    let schema = typed_schema(&[TypeCode::F64]);
    let expr = crate::bind::bind_single_table(
        &crate::test_support::parse_expr_sql("c1 < 18446744073709551615"),
        &schema,
        "t",
    )
    .unwrap();
    let instrs = lower_instrs(&expr, &schema);
    assert!(has(
        &instrs,
        |i| matches!(i, L::LoadConst { val, .. } if f64::from_bits(*val as u64) == 1.8446744073709552e19)
    ));
}

/// A U64 value is range-cast to the i64 a DECIMAL is before it is scaled, so a
/// negation of the product reads signed.
#[test]
fn a_u64_operand_of_a_decimal_is_signed() {
    assert_eq!(
        eval_sql_rows("-(c1 * 1.5) < 0", &[TypeCode::U64], &[vec![Some(2)]]),
        vec![Some(1)]
    );
}

/// DATE and TIMESTAMP meet at TIMESTAMP: a comparison and a difference read
/// both in microseconds, and a CASE mixing them is a TIMESTAMP.
#[test]
fn a_date_meets_a_timestamp_in_microseconds() {
    let day = i128::from(gnitz_expr::calendar::MICROS_PER_DAY);
    let tcs = [TypeCode::Date, TypeCode::Timestamp, TypeCode::I64];
    // d = day 10; ts = day 10 at midnight, and at noon.
    let rows = vec![
        vec![Some(10), Some(10 * day), Some(1)],
        vec![Some(10), Some(10 * day + day / 2), Some(0)],
    ];
    assert_eq!(eval_sql_rows("c1 = c2", &tcs, &rows), vec![Some(1), Some(0)]);
    assert_eq!(eval_sql_rows("c1 > c2", &tcs, &rows), vec![Some(0), Some(0)]);
    assert_eq!(eval_sql_rows("c1 < c2", &tcs, &rows), vec![Some(0), Some(1)]);
    assert_eq!(eval_sql_rows("c2 - c1", &tcs, &rows), vec![Some(0), Some(day / 2)]);
    let case = "CASE WHEN c3 = 1 THEN c1 ELSE c2 END";
    assert_eq!(
        eval_sql_rows(case, &tcs, &rows),
        vec![Some(10 * day), Some(10 * day + day / 2)]
    );
    let schema = typed_schema(&tcs);
    let bound = crate::bind::bind_single_table(&crate::test_support::parse_expr_sql(case), &schema, "t").unwrap();
    assert_eq!(bound.infer_ty(&schema.columns).tc, TypeCode::Timestamp);
}

/// Arithmetic with no temporal meaning, and a temporal value blended with a
/// non-temporal type, are refused.
#[test]
fn temporal_arithmetic_outside_the_allow_list_is_refused() {
    let schema = typed_schema(&[TypeCode::Date, TypeCode::Timestamp, TypeCode::I64]);
    for (sql, needle) in [
        ("c1 * 2", "not supported on a DATE/TIMESTAMP operand"),
        ("c2 / 1000000", "not supported on a DATE/TIMESTAMP operand"),
        ("5 - c1", "not supported on a DATE/TIMESTAMP operand"),
        ("c1 + c1", "not supported on a DATE/TIMESTAMP operand"),
        ("CASE WHEN c3 = 1 THEN c1 ELSE 1.5 END", "cannot mix DATE with F64"),
    ] {
        let expr = crate::bind::bind_single_table(&crate::test_support::parse_expr_sql(sql), &schema, "t").unwrap();
        let err = compile_bound_expr_to_program(&expr, &schema.columns).expect_err(sql);
        assert!(err.to_string().contains(needle), "{sql}: {err}");
    }
}

/// A typed string is `CAST('…' AS type)`, and a CAST to DATE of a non-string
/// operand is the fixed-int cast any other operand of that type takes.
#[test]
fn a_typed_string_and_a_literal_cast_to_date_evaluate() {
    let row = [vec![Some(1)]];
    let bound = crate::bind::bind_single_table(
        &crate::test_support::parse_expr_sql("CAST(NULL AS DATE)"),
        &typed_schema(&[TypeCode::I64]),
        "t",
    )
    .unwrap();
    assert!(matches!(bound, BExpr::Cast { .. }), "{bound:?}");
    for (sql, want) in [
        ("CAST(NULL AS DATE)", None),
        ("CAST(5 AS DATE)", Some(5)),
        ("DECIMAL(10,2) '1.5'", Some(150)),
        ("BIGINT '5'", Some(5)),
    ] {
        assert_eq!(eval_sql_rows(sql, &[TypeCode::I64], &row), vec![want], "{sql}");
    }
}

/// A CAST to a 16-byte type is refused where it is lowered, whatever spelled it.
#[test]
fn a_cast_to_a_16_byte_type_is_refused_by_lowering() {
    let schema = typed_schema(&[TypeCode::I64]);
    for sql in [
        "CAST(c1 AS UUID)",
        "CAST(c1 AS UINT128)",
        "UUID '00000000-0000-0000-0000-000000000001'",
    ] {
        let expr = crate::bind::bind_single_table(&crate::test_support::parse_expr_sql(sql), &schema, "t")
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let err = compile_bound_expr_to_program(&expr, &schema.columns).expect_err(sql);
        assert!(err.to_string().contains("is not supported"), "{sql}: {err}");
    }
}

/// A string compared with a column stored as an integer is that type's spelling
/// or an error naming the type.
#[test]
fn a_string_that_spells_no_value_of_the_operand_type_is_refused() {
    let schema = decimal_schema();
    for (sql, needle) in [
        ("p = 'abc'", "invalid DECIMAL(18, 2) literal: 'abc'"),
        ("i = 'abc'", "invalid I64 literal: 'abc'"),
    ] {
        let expr = crate::bind::bind_single_table(&crate::test_support::parse_expr_sql(sql), &schema, "t").unwrap();
        let err = compile_bound_expr_to_program(&expr, &schema.columns).expect_err(sql);
        assert!(err.to_string().contains(needle), "{sql}: {err}");
    }
}

// ------------------------------------------------------------------
// A DML residual over the transaction's buffered rows
// ------------------------------------------------------------------

/// Which rows of `batch` inside `bound` pass every conjunct, through the wire blob
/// a read ships.
fn residual_rows(
    conjuncts: &[&BoundExpr],
    bound: &gnitz_wire::ReadBound,
    batch: &gnitz_core::ZSetBatch,
    schema: &Schema,
) -> Vec<usize> {
    let blob = compile_wire_conjuncts(conjuncts.iter().copied(), &schema.columns).expect("residual must compile");
    let mut filter = gnitz_expr::RowFilter::for_read(&blob, bound, schema).expect("the blob resolves");
    let mut ranges = Vec::new();
    filter.ranges(batch, &mut ranges);
    ranges.into_iter().flat_map(|(s, e)| s..e).collect()
}

/// Which rows `pred` alone selects, under no bound.
fn residual_matches(pred: &BoundExpr, batch: &gnitz_core::ZSetBatch, schema: &Schema) -> Vec<usize> {
    residual_rows(&[pred], &gnitz_wire::ReadBound::None, batch, schema)
}

fn col_eq(col: usize, v: i64) -> BoundExpr {
    BoundExpr::bin(BoundExpr::ColRef(col), BinOp::Eq, BoundExpr::LitInt(v))
}

/// One row: PK `pk`, a single zero payload cell. The shape every PK-addressing
/// test wants — the payload exists only because the schema declares it.
fn pk_row(schema: &Schema, pk: u128) -> gnitz_core::ZSetBatch {
    let mut batch = gnitz_core::ZSetBatch::new(schema);
    gnitz_core::BatchAppender::new(&mut batch, schema)
        .add_row(pk, 1)
        .i64_val(0);
    batch
}

#[test]
fn a_null_residual_col_excludes_the_row_bare_and_compared() {
    // `WHERE val` and `WHERE val = 0` must both miss a NULL row.
    let schema = crate::test_support::two_col(TypeCode::I64);
    let batch = crate::test_support::batch_2col(vec![0u8; 8], TypeCode::I64, 0b1);
    assert!(residual_matches(&BoundExpr::ColRef(1), &batch, &schema).is_empty());
    assert!(residual_matches(&col_eq(1, 0), &batch, &schema).is_empty());
}

/// A NULL conjunct excludes the row whether the other side is TRUE or FALSE.
#[test]
fn a_residual_and_fold_null_conjunct_excludes_either_way() {
    let schema = crate::test_support::two_col(TypeCode::I64);
    let batch = crate::test_support::batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // val is NULL
    let val_null = col_eq(1, 0); // NULL = 0 → UNKNOWN
                                 // pk = 1 → TRUE, pk = 2 → FALSE
    for other in [col_eq(0, 1), col_eq(0, 2)] {
        assert!(
            residual_rows(&[&other, &val_null], &gnitz_wire::ReadBound::None, &batch, &schema).is_empty(),
            "a NULL conjunct must exclude the row"
        );
    }
}

/// A signed PK round-trips through the client batch's OPK encode and the
/// kernel's decode.
#[test]
fn a_signed_pk_residual_reads_back_through_the_opk_region() {
    let schema = crate::test_support::pk_schema(TypeCode::I64);
    let batch = pk_row(&schema, ((-1i64) as u64) as u128);
    assert_eq!(residual_matches(&col_eq(0, -1), &batch, &schema), vec![0]);
}

#[test]
fn a_compound_pk_residual_reads_each_column_at_its_offset() {
    let schema = crate::test_support::compound_schema_u64_u64();
    let mut batch = gnitz_core::ZSetBatch::new(&schema);
    let mut pk_bytes = [0u8; 16];
    pk_bytes[..8].copy_from_slice(&7u64.to_le_bytes());
    pk_bytes[8..16].copy_from_slice(&9u64.to_le_bytes());
    // Not `BatchAppender::add_row`: its scalar `u128` PK would sign-extend
    // across the second column of a compound key.
    batch.pks.push_bytes(&schema, &pk_bytes);
    batch.weights.push(1);
    batch.nulls.push(0);
    gnitz_core::BatchAppender::new(&mut batch, &schema).i64_val(42);
    let none = gnitz_wire::ReadBound::None;
    // Both PK columns and the payload, as one three-conjunct AND chain.
    let (a, b, v) = (col_eq(0, 7), col_eq(1, 9), col_eq(2, 42));
    assert_eq!(residual_rows(&[&a, &b, &v], &none, &batch, &schema), vec![0]);
    // The second PK column is addressed at its own OPK byte offset, not the
    // first one's.
    assert!(residual_rows(&[&col_eq(1, 7)], &none, &batch, &schema).is_empty());
}

/// The lowering refuses a 128-bit column, naming the type.
#[test]
fn a_wide_residual_column_is_rejected() {
    let wide_pk = crate::test_support::pk_schema(TypeCode::U128);
    let err = compile_wire_conjuncts([&col_eq(0, 1)], &wide_pk.columns).expect_err("U128 PK must be rejected");
    assert!(err.to_string().contains("U128"), "error must name the type: {err}");
    let uuid = crate::test_support::uuid_schema_payload();
    let err = compile_wire_conjuncts([&BoundExpr::ColRef(1)], &uuid.columns).expect_err("UUID column must be rejected");
    assert!(err.to_string().contains("UUID"), "error should mention UUID: {err}");
}

/// F32 and F64 both filter.
#[test]
fn a_float_residual_filters() {
    for (tc, bytes) in [
        (TypeCode::F32, 1.0f32.to_le_bytes().to_vec()),
        (TypeCode::F64, 1.0f64.to_le_bytes().to_vec()),
    ] {
        let schema = crate::test_support::two_col(tc);
        let batch = crate::test_support::batch_2col(bytes, tc, 0);
        let gt = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, lit("0.5"));
        assert_eq!(residual_matches(&gt, &batch, &schema), vec![0], "{tc:?} > 0.5");
        let lt = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Lt, lit("0.5"));
        assert!(residual_matches(&lt, &batch, &schema).is_empty(), "{tc:?} < 0.5");
    }
}

/// A string residual reads the batch's German cells against its blob arena.
#[test]
fn a_string_residual_filters() {
    let schema = crate::test_support::two_col(TypeCode::String);
    let mut batch = gnitz_core::ZSetBatch::new(&schema);
    let mut app = gnitz_core::BatchAppender::new(&mut batch, &schema);
    for (i, s) in ["alpha", "beta"].iter().enumerate() {
        app.add_row(i as u128 + 1, 1).str_val(s);
    }
    let pred = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Eq, BoundExpr::LitStr("beta".to_string()));
    assert_eq!(residual_matches(&pred, &batch, &schema), vec![1]);
}

/// No conjuncts, and a conjunct the binder folded to true, ship no blob and keep
/// every row; one folded to false drops every row.
#[test]
fn the_residual_keep_every_row_exits() {
    let schema = crate::test_support::two_col(TypeCode::I64);
    let batch = crate::test_support::batch_2col(7i64.to_le_bytes().to_vec(), TypeCode::I64, 0);
    let none = gnitz_wire::ReadBound::None;
    assert!(compile_wire_conjuncts([&BoundExpr::LitInt(1)], &schema.columns)
        .unwrap()
        .is_empty());
    assert_eq!(residual_rows(&[], &none, &batch, &schema), vec![0]);
    assert_eq!(residual_matches(&BoundExpr::LitInt(1), &batch, &schema), vec![0]);
    assert!(residual_matches(&BoundExpr::LitInt(0), &batch, &schema).is_empty());
}

/// A walk narrows the rows before the residual tests them, alone or beside one.
#[test]
fn a_walk_and_a_residual_both_apply() {
    let schema = crate::test_support::two_col(TypeCode::I64);
    let mut batch = gnitz_core::ZSetBatch::new(&schema);
    {
        let mut a = gnitz_core::BatchAppender::new(&mut batch, &schema);
        for (pk, v) in [(1u128, 10i64), (5, 50), (6, 0), (9, 90)] {
            a.add_row(pk, 1).i64_val(v);
        }
    }
    let range = gnitz_wire::KeyRange::new(
        gnitz_wire::PkColList::from_slice(&[0]),
        &[],
        gnitz_wire::Cut::after(1),
        gnitz_wire::Cut::before(9),
    );
    let walk = gnitz_wire::ReadBound::Range(range);
    assert_eq!(residual_rows(&[], &walk, &batch, &schema), vec![1, 2]);
    let positive = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::LitInt(0));
    assert_eq!(residual_rows(&[&positive], &walk, &batch, &schema), vec![1]);
}
