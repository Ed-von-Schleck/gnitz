use super::*;
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_expr::LogicalInstr;
use gnitz_wire::ExprOp;

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
    let reg =
        try_compile_string_cmp(left, &op, right, &schema.columns, &mut eb).expect("recognized as a string comparison");
    eb.build(Some(reg)).expect("a well-formed program").instrs().to_vec()
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

/// The opcodes `expr` lowers to, plus its result register's class.
/// `lower_ops` is the same without the class.
fn lower_ops_kind(expr: &BoundExpr, schema: &Schema) -> (Vec<u32>, ExprKind) {
    let mut eb = ExprBuilder::new();
    let (reg, kind) = OpcodeBackend {
        cols: &schema.columns,
        eb: &mut eb,
    }
    .lower(expr)
    .expect("lowers");
    (opcodes(&eb.build(Some(reg)).expect("a well-formed program")), kind)
}

fn lower_ops_isf(expr: &BoundExpr, schema: &Schema) -> (Vec<u32>, bool) {
    let (ops, kind) = lower_ops_kind(expr, schema);
    (ops, kind == ExprKind::Float)
}

fn lower_ops(expr: &BoundExpr, schema: &Schema) -> Vec<u32> {
    lower_ops_isf(expr, schema).0
}

/// The lowering error `expr` produces, for the reject cases.
fn lower_err(expr: &BoundExpr, schema: &Schema) -> GnitzSqlError {
    let mut eb = ExprBuilder::new();
    OpcodeBackend {
        cols: &schema.columns,
        eb: &mut eb,
    }
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

/// The float opcodes a fold-to-identity must never emit.
const FLOAT_OPS: [u32; 6] = [
    ExprOp::IntToFloat.as_wire(),
    ExprOp::FloatMul.as_wire(),
    ExprOp::FloatDiv.as_wire(),
    ExprOp::FloatRound.as_wire(),
    ExprOp::FloatFloor.as_wire(),
    ExprOp::FloatCeil.as_wire(),
];

fn cast_emits(expr: &BoundExpr, to: TypeCode, schema: &Schema) -> bool {
    let cast = BoundExpr::Cast {
        expr: Box::new(expr.clone()),
        to,
    };
    lower_ops(&cast, schema).contains(&ExprOp::IntCast.as_wire())
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
    let neg = BoundExpr::UnaryOp(UnaryOp::Neg, Box::new(BoundExpr::ColRef(1)));
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

fn round_ops(n: i8, arg: &BoundExpr, schema: &Schema) -> (Vec<u32>, bool) {
    lower_ops_isf(&func(NumFunc::Round(n), arg.clone()), schema)
}

/// n >= 0 on an integer argument is the identity: no opcode, and the value
/// keeps its integer type rather than going through an f64 lift that would
/// mangle any |x| >= 2^53.
#[test]
fn round_scaled_folds_integer_nonnegative_scale() {
    let s = cast_schema();
    let icol = BoundExpr::ColRef(1);
    for n in [0i8, 2, 15] {
        let (ops, isf) = round_ops(n, &icol, &s);
        assert!(!isf, "n={n}: stays integer");
        assert!(!ops.iter().any(|o| FLOAT_OPS.contains(o)), "n={n}: no float arithmetic");
    }
    assert!(round_ops(2, &BoundExpr::LitFloat(1.5), &s).1, "float arg stays float");
    let (ops, isf) = round_ops(-2, &icol, &s);
    assert!(isf, "negative scale yields F64");
    assert!(ops.contains(&ExprOp::FloatRound.as_wire()), "negative scale rounds");
}

/// n == 0 is the bare 1-arg ROUND opcode, not a scale-by-1 round trip.
#[test]
fn round_scaled_zero_is_plain_round() {
    let s = cast_schema();
    let (ops, _) = round_ops(0, &BoundExpr::LitFloat(2.5), &s);
    assert!(ops.contains(&ExprOp::FloatRound.as_wire()));
    assert!(!ops.contains(&ExprOp::FloatMul.as_wire()));
    assert!(!ops.contains(&ExprOp::FloatDiv.as_wire()));
}

/// A positive scale multiplies first and divides back; a negative one does
/// the mirror. Only the positive powers of ten are exactly representable in
/// f64, which is why neither direction loads `10^-n`.
#[test]
fn round_scaled_picks_the_scaling_direction() {
    let s = cast_schema();
    let ops = |n: i8| -> Vec<u32> {
        round_ops(n, &BoundExpr::LitFloat(1.5), &s)
            .0
            .into_iter()
            .filter(|op| {
                [
                    ExprOp::FloatMul.as_wire(),
                    ExprOp::FloatDiv.as_wire(),
                    ExprOp::FloatRound.as_wire(),
                ]
                .contains(op)
            })
            .collect()
    };
    assert_eq!(
        ops(2),
        vec![
            ExprOp::FloatMul.as_wire(),
            ExprOp::FloatRound.as_wire(),
            ExprOp::FloatDiv.as_wire()
        ]
    );
    assert_eq!(
        ops(-2),
        vec![
            ExprOp::FloatDiv.as_wire(),
            ExprOp::FloatRound.as_wire(),
            ExprOp::FloatMul.as_wire()
        ]
    );
}

fn func(f: NumFunc, arg: BoundExpr) -> BoundExpr {
    BoundExpr::Func { f, arg: Box::new(arg) }
}

/// The identity folds: rounding an integer is the integer, and an unsigned
/// register value is already non-negative. Both must emit nothing — the f64
/// lift a naive FLOOR would take mangles any integer >= 2^53.
#[test]
fn integer_arguments_fold_the_unary_transforms_away() {
    let s = cast_schema(); // col 1 = i32, col 2 = u32, col 4 = u64
    for f in [NumFunc::Floor, NumFunc::Ceil, NumFunc::Round(0), NumFunc::Trunc] {
        let ops = lower_ops(&func(f, BoundExpr::ColRef(1)), &s);
        assert!(!ops.iter().any(|o| FLOAT_OPS.contains(o)), "{f:?}(i32col) folds away");
    }
    for c in [2usize, 4] {
        let ops = lower_ops(&func(NumFunc::Abs, BoundExpr::ColRef(c)), &s);
        assert!(
            !ops.contains(&ExprOp::IntAbs.as_wire()),
            "ABS over an unsigned column folds away"
        );
    }
    // A signed integer argument does need the opcode.
    assert!(lower_ops(&func(NumFunc::Abs, BoundExpr::ColRef(1)), &s).contains(&ExprOp::IntAbs.as_wire()));
    // A float argument takes the float opcode in every case.
    assert!(lower_ops(&func(NumFunc::Floor, BoundExpr::LitFloat(1.5)), &s).contains(&ExprOp::FloatFloor.as_wire()));
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
        let ops = lower_ops(&min_max(true, args), &s);
        assert_eq!(
            ops.iter().filter(|&&o| o == ExprOp::IntMax2.as_wire()).count(),
            n - 1,
            "arity {n}: n-1 folds"
        );
    }
    assert!(lower_ops(&min_max(false, vec![BoundExpr::ColRef(1); 2]), &s).contains(&ExprOp::IntMin2.as_wire()));
    // One float argument lifts every integer argument and switches the whole
    // fold to the float opcodes — the same global unification CASE applies.
    let mixed = min_max(true, vec![BoundExpr::ColRef(1), BoundExpr::LitFloat(1.5)]);
    let ops = lower_ops(&mixed, &s);
    assert!(ops.contains(&ExprOp::FloatMax2.as_wire()));
    assert!(ops.contains(&ExprOp::IntToFloat.as_wire()));
    assert!(!ops.contains(&ExprOp::IntMax2.as_wire()));
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
        let (reg, _) = OpcodeBackend {
            cols: &s.columns,
            eb: &mut eb,
        }
        .lower(&min_max(true, args))
        .expect("lowers");
        let prog = eb.build(Some(reg)).expect("a well-formed program");
        match prog.instrs().first().expect("non-empty") {
            &LogicalInstr::LoadColInt { col, .. } => col,
            other => panic!("the fold must open with a column load, got {other:?}"),
        }
    };
    let neg1 = BoundExpr::UnaryOp(UnaryOp::Neg, Box::new(BoundExpr::LitInt(1)));
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
        let mut eb = ExprBuilder::new();
        let r = OpcodeBackend {
            cols: &s.columns,
            eb: &mut eb,
        }
        .lower(&min_max(true, vec![BoundExpr::ColRef(c), BoundExpr::ColRef(c)]));
        assert!(matches!(r, Err(GnitzSqlError::Unsupported(_))), "column {c}");
    }
}

/// The 64-register file bounds arity. The overflow is not a special case:
/// it is the same `TooManyRegs` any large expression hits, surfaced as a
/// plan-time `Unsupported` by the builder's one exit.
#[test]
fn min_max_n_arity_is_bounded_by_the_register_file() {
    let s = cast_schema();
    let args: Vec<BoundExpr> = (0..40).map(|_| BoundExpr::ColRef(1)).collect();
    let mut eb = ExprBuilder::new();
    let (reg, _) = OpcodeBackend {
        cols: &s.columns,
        eb: &mut eb,
    }
    .lower(&min_max(true, args))
    .expect("lowering itself does not bound arity");
    match eb.build(Some(reg)).map_err(expr_unsupported).err() {
        Some(GnitzSqlError::Unsupported(msg)) => assert!(msg.contains("reg"), "got {msg:?}"),
        other => panic!("expected a register-budget rejection, got {other:?}"),
    }
}

/// A float source truncates through `FLOAT_TO_INT`; an integer source
/// widens through `INT_TO_FLOAT`, and only an F32 target rounds afterwards.
#[test]
fn cast_picks_the_conversion_opcode_from_the_source_domain() {
    let s = cast_schema();
    let cast = |e: BoundExpr, to| BoundExpr::Cast { expr: Box::new(e), to };
    let f = BoundExpr::LitFloat(2.7);

    assert!(lower_ops(&cast(f.clone(), TypeCode::I32), &s).contains(&ExprOp::FloatToInt.as_wire()));
    // float -> DOUBLE is a no-op: the register already holds an f64.
    let ops = lower_ops(&cast(f.clone(), TypeCode::F64), &s);
    assert!(!ops.contains(&ExprOp::FloatToF32.as_wire()) && !ops.contains(&ExprOp::IntToFloat.as_wire()));
    assert!(lower_ops(&cast(f, TypeCode::F32), &s).contains(&ExprOp::FloatToF32.as_wire()));

    let ops = lower_ops(&cast(BoundExpr::ColRef(1), TypeCode::F64), &s);
    assert!(ops.contains(&ExprOp::IntToFloat.as_wire()) && !ops.contains(&ExprOp::FloatToF32.as_wire()));
    // int -> FLOAT is the two-step lift; the double rounding is committed.
    let ops = lower_ops(&cast(BoundExpr::ColRef(1), TypeCode::F32), &s);
    assert!(ops.contains(&ExprOp::IntToFloat.as_wire()) && ops.contains(&ExprOp::FloatToF32.as_wire()));
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
    let reg = eb.emit(L::StrColCol {
        op: StrOp::Lt,
        col_a: 1,
        col_b: 2,
    });
    let want = eb.build(Some(reg)).expect("a well-formed program");
    assert_eq!(got, want.instrs(), "a < b must stay str_col_lt_col(a, b)");
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
    assert!(try_compile_string_cmp(&lit, &BinOp::Add, &s, &schema.columns, &mut eb).is_none());
    assert!(
        eb.build(None).expect("a well-formed program").instrs().is_empty(),
        "a declined shape must emit nothing"
    );

    let add = BoundExpr::BinOp(Box::new(lit), BinOp::Add, Box::new(s));
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
    let err = compile_bound_expr(&expr, &schema.columns, &mut eb).expect_err("string > int must not compile");
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

/// Each instruction's wire opcode, off the one encode table.
fn opcodes(p: &LogicalProgram) -> Vec<u32> {
    p.instrs().iter().map(|i| i.to_wire()[0]).collect()
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
    let gt0 = || {
        BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Gt,
            Box::new(BoundExpr::LitInt(0)),
        )
    };

    // All-int CASE: a SELECT, but no float lift.
    let case_int = BoundExpr::Case {
        branches: vec![(gt0(), BoundExpr::ColRef(1))],
        else_: Some(Box::new(BoundExpr::LitInt(0))),
    };
    let ops = opcodes(&compile_bound_expr_to_program(&case_int, &schema.columns).unwrap());
    assert!(ops.contains(&ExprOp::Select.as_wire()), "CASE must lower to a SELECT");
    assert!(
        !ops.contains(&ExprOp::IntToFloat.as_wire()),
        "all-int CASE needs no float lift"
    );

    // Mixed int/float CASE: the int else is lifted to float up front.
    let case_mixed = BoundExpr::Case {
        branches: vec![(gt0(), BoundExpr::ColRef(2))],
        else_: Some(Box::new(BoundExpr::ColRef(1))),
    };
    let ops = opcodes(&compile_bound_expr_to_program(&case_mixed, &schema.columns).unwrap());
    assert!(ops.contains(&ExprOp::Select.as_wire()), "CASE must lower to a SELECT");
    assert!(
        ops.contains(&ExprOp::IntToFloat.as_wire()),
        "mixed CASE lifts int branches to float"
    );
}

/// A string-typed CASE result reaches the integer/string-load path and is
/// rejected (the register file carries 8-byte values only).
#[test]
fn case_string_branches_compile_through_the_string_channel() {
    let schema = case_schema();
    let case_str = BoundExpr::Case {
        branches: vec![(
            BoundExpr::BinOp(
                Box::new(BoundExpr::ColRef(1)),
                BinOp::Gt,
                Box::new(BoundExpr::LitInt(0)),
            ),
            BoundExpr::LitStr("x".into()),
        )],
        else_: Some(Box::new(BoundExpr::LitStr("y".into()))),
    };
    assert_eq!(lower_ops_kind(&case_str, &schema).1, ExprKind::Str);
    // The engine's class validator is the real check: it rejects a scalar
    // register reaching a string operand, so a CASE that blended its string
    // branches with the numeric SELECT would fail here rather than compile.
    assert_str_program(&case_str, &schema);
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
    let err = compile_bound_expr(&expr, &schema.columns, &mut eb).expect_err("string + 1 must not compile");
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

// ------------------------------------------------------------------
// IN-list lowering: INT_IN_SET fast path vs OR-chain fallback
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
    BoundExpr::InList {
        inner: Box::new(inner),
        items,
    }
}

/// `-v` binds to `UnaryOp(Neg, LitInt(v))` (sqlparser lexes the minus
/// separately); the fold path must still emit INT_IN_SET.
fn neg_lit(v: i64) -> BoundExpr {
    BoundExpr::UnaryOp(UnaryOp::Neg, Box::new(BoundExpr::LitInt(v)))
}

/// An integer operand with all-integer-literal items → one INT_IN_SET, for
/// both positive and negative-literal lists.
#[test]
fn in_list_int_emits_int_in_set() {
    let schema = two_int_schema();
    for items in [
        vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2), BoundExpr::LitInt(3)],
        vec![neg_lit(1), neg_lit(2)],
    ] {
        let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
        let ops = opcodes(&prog);
        assert!(
            ops.contains(&ExprOp::IntInSet.as_wire()),
            "int IN must emit INT_IN_SET, ops={ops:?}"
        );
    }
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
/// INT_IN_SET.
#[test]
fn in_list_float_operand_falls_back_to_or_chain() {
    let schema = case_schema(); // col2 = f (F64)
    let prog = compile_bound_expr_to_program(
        &in_list(BoundExpr::ColRef(2), vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)]),
        &schema.columns,
    )
    .unwrap();
    let ops = opcodes(&prog);
    assert!(
        !ops.contains(&ExprOp::IntInSet.as_wire()),
        "float IN must not emit INT_IN_SET"
    );
    assert!(
        ops.contains(&ExprOp::FcmpEq.as_wire()),
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
    let ops = opcodes(&prog);
    assert!(
        !ops.contains(&ExprOp::IntInSet.as_wire()),
        "string IN must not emit INT_IN_SET"
    );
    assert!(
        ops.contains(&ExprOp::StrColEqConst.as_wire()),
        "string IN lowers to str_col_eq_const"
    );
}

/// Why the fused compare cannot be dropped in favour of the register
/// channel, stated as a budget rather than a speed. A string `IN` list is an
/// OR chain, and a register *is* an instruction, so the per-term instruction
/// cost is what caps the list: one per fused compare plus one per OR, over
/// a `MAX_REGS` of 64. At 22 items that is 43 and compiles; through the
/// register channel each term would spend three instead of one, putting the
/// same list over the cap.
#[test]
fn in_list_string_list_fits_only_because_the_compare_is_fused() {
    let schema = str_schema();
    let items: Vec<BoundExpr> = (0..22).map(|i| BoundExpr::LitStr(format!("tag{i}"))).collect();
    let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
    assert_eq!(
        prog.instrs().len(),
        2 * 22 - 1,
        "one register per fused compare, one per OR"
    );
    assert!(
        prog.instrs().len() + 22 > 64,
        "the same list must not fit once each term costs a third register"
    );
}

/// The operand is lowered once for the whole list, not once per item — which
/// is what puts a non-`ColRef` operand inside the register budget at all:
/// `UPPER(c) IN (…14 items…)` costs five registers per item unhoisted.
#[test]
fn in_list_hoists_a_computed_operand_out_of_the_fold() {
    let schema = str_schema();
    let upper = BoundExpr::StrCall {
        f: StrFunc::Upper,
        arg: Box::new(str_col(1)),
    };
    let items: Vec<BoundExpr> = (0..14).map(|i| BoundExpr::LitStr(format!("tag{i}"))).collect();
    let prog = compile_bound_expr_to_program(&in_list(upper, items), &schema.columns)
        .expect("the hoisted operand fits the register budget");
    let ops = opcodes(&prog);
    assert_eq!(
        ops.iter().filter(|&&o| o == ExprOp::StrUpper.as_wire()).count(),
        1,
        "the operand is folded once, not once per item: {ops:?}"
    );
}

/// An integer column operand is never fused into the per-item compare, so it
/// is hoisted too: one column load for the whole list rather than one per
/// item. (A literal list would take `INT_IN_SET`; one non-literal item is
/// what forces the fold.)
#[test]
fn in_list_hoists_an_integer_column_operand() {
    let schema = two_int_schema();
    let prog = compile_bound_expr_to_program(
        &in_list(
            BoundExpr::ColRef(1),
            vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2), BoundExpr::ColRef(2)],
        ),
        &schema.columns,
    )
    .unwrap();
    let ops = opcodes(&prog);
    assert_eq!(
        ops.iter().filter(|&&o| o == ExprOp::LoadColInt.as_wire()).count(),
        2,
        "one load for the operand, one for the column item: {ops:?}"
    );
}

/// A *literal* operand is not hoisted: `'x' IN (col_a, col_b)` binds with the
/// literal on the left, and the fused column comparison reads it through the
/// converse arm. Hoisting it would push the pair onto the register channel
/// and lose the 4-byte-prefix short circuit.
#[test]
fn in_list_keeps_the_fused_compare_for_a_literal_operand() {
    let schema = str_schema();
    let prog =
        compile_bound_expr_to_program(&in_list(lit("x"), vec![str_col(1), str_col(2)]), &schema.columns).unwrap();
    let ops = opcodes(&prog);
    assert_eq!(
        ops,
        [
            ExprOp::StrColEqConst.as_wire(),
            ExprOp::StrColEqConst.as_wire(),
            ExprOp::BoolOr.as_wire()
        ]
    );
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
    let ops = opcodes(&prog);
    assert!(
        !ops.contains(&ExprOp::IntInSet.as_wire()),
        "non-literal item must not emit INT_IN_SET"
    );
    assert!(
        ops.contains(&ExprOp::CmpEq.as_wire()),
        "non-literal item lowers to cmp OR-chain"
    );
}

/// A float-literal item forces the OR-chain even for an int operand
/// (`a IN (1, 2.5)`).
#[test]
fn in_list_float_literal_item_falls_back_to_or_chain() {
    let schema = two_int_schema();
    let prog = compile_bound_expr_to_program(
        &in_list(
            BoundExpr::ColRef(1),
            vec![BoundExpr::LitInt(1), BoundExpr::LitFloat(2.5)],
        ),
        &schema.columns,
    )
    .unwrap();
    assert!(
        !opcodes(&prog).contains(&ExprOp::IntInSet.as_wire()),
        "a float-literal item must not emit INT_IN_SET"
    );
}

/// A surviving `LitWide` literal (an un-servable wide comparison, e.g.
/// `non_indexed_u64 = 18446744073709551615`) rejects at the compile boundary
/// with the shared wide-int message — the honest surfacing of the VM's
/// 8-byte-slot limitation, for every backend that funnels through this walk.
#[test]
fn lit_wide_rejects_at_compile_boundary() {
    let schema = two_int_schema(); // (pk U64, a I64, b I64)
    let expr = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(1)),
        BinOp::Eq,
        Box::new(BoundExpr::LitWide("18446744073709551615".to_string())),
    );
    let err = compile_bound_expr_to_program(&expr, &schema.columns).expect_err("wide literal must not compile");
    match err {
        GnitzSqlError::Unsupported(msg) => {
            assert!(msg.contains(crate::ir::WIDE_INT_UNSUPPORTED), "message: {msg}");
            assert!(msg.contains("18446744073709551615"), "message names the literal: {msg}");
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
        matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

// -----------------------------------------------------------------------
// The string channel
// -----------------------------------------------------------------------

fn str_col(i: usize) -> BoundExpr {
    BoundExpr::ColRef(i)
}
fn lit(s: &str) -> BoundExpr {
    BoundExpr::LitStr(s.to_string())
}

/// A plain `col op 'lit'` must keep the specialized 16-byte-cell opcodes;
/// the register channel exists for computed operands. Routing this shape
/// through it would build a string register per row for nothing: the fused
/// kernel short-circuits on the cell's 4-byte prefix, which a `StrView`
/// does not carry. `str_const_filter_bench` in gnitz-expr drives either
/// channel over four value domains under `perf`; the prefix collision rate
/// controls the gap, and nothing bounds that rate, so there is no one number
/// to quote here.
///
/// The register cap is the part that does not depend on speed at all. A
/// register is the index of the instruction that writes it, and `MAX_REGS`
/// is 64. A string
/// `IN (N)` list lowers to an OR chain; each term intercepted into a fused
/// compare costs one register, plus one per OR — `2N-1`. Through the
/// register channel a term is a column load, a const load and a compare,
/// three registers where the fused form spends one, so the same list caps
/// strictly lower. `in_list_string_list_fits_only_because_the_compare_is_fused`
/// pins the case that separates them.
#[test]
fn plain_column_comparisons_keep_the_specialized_opcodes() {
    let schema = str_schema();
    let ops = lower_ops(
        &BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Eq, Box::new(lit("x"))),
        &schema,
    );
    assert_eq!(ops, [ExprOp::StrColEqConst.as_wire()]);
    let cols = lower_ops(
        &BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Lt, Box::new(str_col(2))),
        &schema,
    );
    assert_eq!(cols, [ExprOp::StrColLtCol.as_wire()]);
}

/// A *computed* operand has no specialized form, so it falls through to the
/// register compare. The engine's validator is what proves the operands were
/// built in the right class.
#[test]
fn computed_operands_compare_through_the_register_channel() {
    let schema = str_schema();
    let upper_eq = BoundExpr::BinOp(
        Box::new(BoundExpr::StrCall {
            f: StrFunc::Upper,
            arg: Box::new(str_col(1)),
        }),
        BinOp::Eq,
        Box::new(lit("X")),
    );
    let ops = lower_ops(&upper_eq, &schema);
    assert!(ops.contains(&ExprOp::StrCmpEq.as_wire()), "{ops:?}");
    assert!(!ops.contains(&ExprOp::StrColEqConst.as_wire()), "{ops:?}");
    // The comparison is a boolean, so it is a legitimate filter predicate.
    let p = compile_bound_expr_to_program(&upper_eq, &schema.columns).expect("lowers");
    assert!(p.resolve_filter(&schema).is_ok());
}

/// `NE`, `GT` and `GE` have no opcode of their own on either path; they ride
/// the three that exist. Getting the swap backwards is invisible until the
/// operands differ, so drive it against the transposition directly.
#[test]
fn register_compare_derives_ne_gt_ge_from_the_three_opcodes() {
    let schema = str_schema();
    let up = |i| BoundExpr::StrCall {
        f: StrFunc::Upper,
        arg: Box::new(str_col(i)),
    };
    let cmp = |op| lower_ops(&BoundExpr::BinOp(Box::new(up(1)), op, Box::new(up(2))), &schema);
    let tail = |ops: Vec<u32>| ops[ops.len() - 1];
    assert_eq!(tail(cmp(BinOp::Lt)), ExprOp::StrCmpLt.as_wire());
    assert_eq!(tail(cmp(BinOp::Le)), ExprOp::StrCmpLe.as_wire());
    // GT/GE swap the operands rather than negating, so no BOOL_NOT appears.
    assert_eq!(tail(cmp(BinOp::Gt)), ExprOp::StrCmpLt.as_wire());
    assert_eq!(tail(cmp(BinOp::Ge)), ExprOp::StrCmpLe.as_wire());
    // NE is the negation of EQ.
    assert_eq!(tail(cmp(BinOp::Ne)), ExprOp::BoolNot.as_wire());
}

#[test]
fn concat_operator_and_function_compile_and_differ_in_their_null_rule() {
    let schema = str_schema();
    // `||` is NULL-propagating, so `s || NULL` is a NULL string rather than a
    // type error — `str_operand` intercepts the literal before recursing.
    let pipe_null = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Concat, Box::new(BoundExpr::LitNull));
    assert_str_program(&pipe_null, &schema);
    assert!(lower_ops(&pipe_null, &schema).contains(&ExprOp::LoadNullStr.as_wire()));

    let pipe = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Concat, Box::new(lit("x")));
    assert!(lower_ops(&pipe, &schema).contains(&ExprOp::StrConcat.as_wire()));

    // CONCAT folds through the null-as-empty step instead, and seeds the fold
    // so even a single argument is non-NULL.
    let one = BoundExpr::ConcatN { args: vec![str_col(1)] };
    let ops = lower_ops(&one, &schema);
    assert!(ops.contains(&ExprOp::StrConcatNn.as_wire()), "{ops:?}");
    assert!(!ops.contains(&ExprOp::StrConcat.as_wire()), "{ops:?}");
    assert_str_program(&one, &schema);
}

/// CONCAT is the one place a numeric argument is cast to text implicitly;
/// `||` is not, so it stays a typed error there.
#[test]
fn concat_casts_numeric_arguments_but_the_operator_does_not() {
    let schema = str_schema();
    let mixed = BoundExpr::ConcatN {
        args: vec![str_col(1), BoundExpr::LitInt(42), BoundExpr::LitFloat(1.5)],
    };
    let ops = lower_ops(&mixed, &schema);
    assert!(ops.contains(&ExprOp::IntToStr.as_wire()), "{ops:?}");
    assert!(ops.contains(&ExprOp::FloatToStr.as_wire()), "{ops:?}");
    assert_str_program(&mixed, &schema);

    let pipe_int = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Concat, Box::new(BoundExpr::LitInt(1)));
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
        branches: vec![(
            BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Eq, Box::new(str_col(2))),
            BoundExpr::LitNull,
        )],
        else_: Some(Box::new(str_col(1))),
    };
    assert_str_program(&nullif, &schema);

    let coalesce = BoundExpr::Case {
        branches: vec![(BoundExpr::IsNotNull(1), str_col(1))],
        else_: Some(Box::new(lit("default"))),
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
    assert_eq!(all_null.infer_type(&schema.columns), TypeCode::I64);
    assert_eq!(lower_ops_kind(&all_null, &schema).1, ExprKind::Int);
}

#[test]
fn mixed_string_and_numeric_case_branches_are_a_typed_error() {
    let schema = str_schema();
    let mixed = BoundExpr::Case {
        branches: vec![(BoundExpr::LitInt(1), lit("x"))],
        else_: Some(Box::new(BoundExpr::LitInt(0))),
    };
    assert!(matches!(lower_err(&mixed, &schema), GnitzSqlError::Unsupported(_)));
}

/// Every numeric position reads its operands through `lower_num`, so a
/// string there is a SQL error rather than an engine-side class mismatch the
/// user would see as an opaque internal enum.
#[test]
fn strings_in_numeric_positions_are_rejected_by_lowering() {
    let schema = str_schema();
    let s = || str_col(1);
    let cases: Vec<BoundExpr> = vec![
        BoundExpr::BinOp(Box::new(s()), BinOp::Add, Box::new(BoundExpr::LitInt(1))),
        BoundExpr::BinOp(Box::new(s()), BinOp::And, Box::new(lit("x"))),
        BoundExpr::BinOp(Box::new(s()), BinOp::Or, Box::new(lit("x"))),
        BoundExpr::Func {
            f: NumFunc::Abs,
            arg: Box::new(s()),
        },
        BoundExpr::Func {
            f: NumFunc::Round(2),
            arg: Box::new(s()),
        },
        BoundExpr::MinMaxN {
            is_max: true,
            args: vec![s(), lit("x")],
        },
        BoundExpr::UnaryOp(UnaryOp::Neg, Box::new(s())),
        BoundExpr::UnaryOp(UnaryOp::Not, Box::new(s())),
        BoundExpr::Substr {
            s: Box::new(s()),
            start: Box::new(s()),
            len: None,
        },
    ];
    for e in &cases {
        assert!(
            matches!(lower_err(e, &schema), GnitzSqlError::Unsupported(_)),
            "{e:?} must be rejected by lowering"
        );
    }
    // A comparison carries no implicit cast either way.
    let mixed = BoundExpr::BinOp(Box::new(s()), BinOp::Eq, Box::new(BoundExpr::LitInt(1)));
    assert!(lower_err(&mixed, &schema).to_string().contains("strings"));
}

/// BLOB keeps exactly its existing comparison support: the column/literal
/// shapes compile, and everything else — including the string functions —
/// rejects.
#[test]
fn blob_columns_stay_outside_the_string_surface() {
    let schema = blob_schema();
    let cmp = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Eq, Box::new(lit("x")));
    assert_eq!(lower_ops(&cmp, &schema), [ExprOp::StrColEqConst.as_wire()]);

    let upper = BoundExpr::StrCall {
        f: StrFunc::Lower,
        arg: Box::new(str_col(1)),
    };
    assert!(lower_err(&upper, &schema).to_string().contains("blob"));
}

/// A string source must never reach the numeric elide test: STRING's
/// register image is I64, so it would match and drop the cast, reading the
/// 16-byte descriptor as an integer.
#[test]
fn cast_from_a_string_emits_a_parse_and_is_never_elided() {
    let schema = str_schema();
    let to = |tc| BoundExpr::Cast {
        expr: Box::new(str_col(1)),
        to: tc,
    };
    assert_eq!(
        lower_ops(&to(TypeCode::I64), &schema),
        [ExprOp::LoadColStr.as_wire(), ExprOp::StrToInt.as_wire()]
    );
    assert_eq!(
        lower_ops(&to(TypeCode::F64), &schema),
        [ExprOp::LoadColStr.as_wire(), ExprOp::StrToFloat.as_wire()]
    );
    assert_eq!(
        lower_ops(&to(TypeCode::F32), &schema),
        [
            ExprOp::LoadColStr.as_wire(),
            ExprOp::StrToFloat.as_wire(),
            ExprOp::FloatToF32.as_wire()
        ]
    );
    // STRING → STRING is the identity.
    assert_eq!(
        lower_ops(&to(TypeCode::String), &schema),
        [ExprOp::LoadColStr.as_wire()]
    );
}

#[test]
fn cast_to_text_emits_the_numeric_to_text_opcode_for_its_source_domain() {
    let schema = cast_schema();
    let to_text = |c| BoundExpr::Cast {
        expr: Box::new(BoundExpr::ColRef(c)),
        to: TypeCode::String,
    };
    assert!(lower_ops(&to_text(1), &schema).contains(&ExprOp::IntToStr.as_wire()));
    let f = BoundExpr::Cast {
        expr: Box::new(BoundExpr::LitFloat(1.5)),
        to: TypeCode::String,
    };
    assert!(lower_ops(&f, &schema).contains(&ExprOp::FloatToStr.as_wire()));
    assert_str_program(&to_text(1), &schema);
}

fn like_of(s: BoundExpr, pattern: &str, escape: Option<u8>, ci: bool) -> BoundExpr {
    BoundExpr::Like {
        s: Box::new(s),
        pattern: pattern.to_string(),
        escape,
        ci,
    }
}

/// The pattern is a plain pool entry and the escape is carried by the LIKE
/// instruction itself, not by a second opcode.
#[test]
fn like_lowers_to_one_opcode_carrying_its_escape() {
    let schema = str_schema();
    let prog =
        |escape, ci| compile_bound_expr_to_program(&like_of(str_col(1), "a%", escape, ci), &schema.columns).unwrap();
    // The escape rides on the LIKE instruction itself.
    let escape_of = |p: &LogicalProgram| match p.instrs()[1] {
        LogicalInstr::StrLike { escape, .. } => escape,
        ref other => panic!("expected a LIKE instruction, got {other:?}"),
    };

    let p = prog(Some(b'\\'), false);
    assert_eq!(opcodes(&p), [ExprOp::LoadColStr.as_wire(), ExprOp::StrLike.as_wire()]);
    assert_eq!(p.const_strings()[0], b"a%");
    assert_eq!(escape_of(&p), std::num::NonZeroU8::new(b'\\'));
    // ILIKE is the same shape under the case-insensitive opcode …
    assert_eq!(
        opcodes(&prog(Some(b'\\'), true)),
        [ExprOp::LoadColStr.as_wire(), ExprOp::StrIlike.as_wire()]
    );
    // … and `ESCAPE ''` is the absence of an escape.
    assert_eq!(escape_of(&prog(None, false)), None);
}

/// The subject is an ordinary string operand: any expression the string
/// channel produces, including the bare `NULL` its `LitNull` rule catches.
#[test]
fn like_takes_a_computed_subject_and_propagates_a_null_literal() {
    let schema = str_schema();
    let upper = BoundExpr::StrCall {
        f: StrFunc::Upper,
        arg: Box::new(str_col(1)),
    };
    assert_eq!(
        lower_ops(&like_of(upper, "A%", Some(b'\\'), false), &schema),
        [
            ExprOp::LoadColStr.as_wire(),
            ExprOp::StrUpper.as_wire(),
            ExprOp::StrLike.as_wire()
        ]
    );
    assert_eq!(
        lower_ops(&like_of(BoundExpr::LitNull, "a", Some(b'\\'), false), &schema),
        [ExprOp::LoadNullStr.as_wire(), ExprOp::StrLike.as_wire()]
    );
    // The verdict is a boolean, so `NOT LIKE` reads it like any other.
    let negated = BoundExpr::UnaryOp(UnaryOp::Not, Box::new(like_of(str_col(1), "a", Some(b'\\'), false)));
    assert_eq!(
        lower_ops(&negated, &schema),
        [
            ExprOp::LoadColStr.as_wire(),
            ExprOp::StrLike.as_wire(),
            ExprOp::BoolNot.as_wire()
        ]
    );
}

/// The subject goes through the shared string-operand channel, so a numeric
/// one draws that channel's rejection rather than a LIKE-specific one.
#[test]
fn like_over_a_numeric_operand_is_a_typed_error() {
    let schema = case_schema(); // col1 = i (I64)
    let err = compile_bound_expr_to_program(&like_of(BoundExpr::ColRef(1), "a", Some(b'\\'), false), &schema.columns)
        .expect_err("a numeric subject has no string channel");
    let GnitzSqlError::Unsupported(msg) = &err else {
        panic!("expected Unsupported, got {err:?}")
    };
    assert!(msg.contains("expected a string value"), "got {msg}");
}

/// A computed STRING column must be *declared* STRING. The register image
/// maps STRING to I64, which was right while every computed value was an
/// 8-byte register.
#[test]
fn a_computed_string_projection_declares_a_string_column() {
    let schema = str_schema();
    let e = BoundExpr::StrCall {
        f: StrFunc::Upper,
        arg: Box::new(str_col(1)),
    };
    let nominal = e.infer_type(&schema.columns);
    assert_eq!(nominal, TypeCode::String);
    let def = ColumnDef::computed(None, 0, nominal);
    assert_eq!(def.type_code, TypeCode::String);
    assert!(def.is_nullable);
    // A numeric expression still takes its register image.
    assert_eq!(ColumnDef::computed(None, 0, TypeCode::F32).type_code, TypeCode::F64);
}
