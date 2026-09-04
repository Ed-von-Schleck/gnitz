use super::*;
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_expr::{CmpOp, ExprValidateErr, FloatArithOp, IntUnaryOp, LogicalInstr, LogicalInstr as L};

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
        try_compile_string_cmp(left, op, right, &schema.columns, &mut eb).expect("recognized as a string comparison");
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

/// The typed instructions `expr` lowers to, plus its result register's class;
/// `lower_instrs` is the same without the class. Typed rather than wire words:
/// these are tests of the *lowerer*, so no encoding change reaches this file.
fn lower_instrs_kind(expr: &BoundExpr, schema: &Schema) -> (Vec<LogicalInstr>, ExprKind) {
    let mut eb = ExprBuilder::new();
    let (reg, kind) = OpcodeBackend { cols: &schema.columns, eb: &mut eb }
        .lower(expr)
        .expect("lowers");
    let prog = eb.build(Some(reg)).expect("a well-formed program");
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
    let cast = BoundExpr::Cast { expr: Box::new(expr.clone()), to };
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
    assert!(
        round_instrs(2, &BoundExpr::LitFloat(1.5), &s).1,
        "float arg stays float"
    );
    let (instrs, isf) = round_instrs(-2, &icol, &s);
    assert!(isf, "negative scale yields F64");
    assert!(is_round(&instrs), "negative scale rounds");
}

/// `ROUND(x)` is the bare unary opcode, not a scale-by-1 round trip.
#[test]
fn unscaled_round_is_plain_round() {
    let s = cast_schema();
    let (instrs, _) = lower_instrs_isf(&func(NumFunc::Unary(FloatUnaryOp::Round), BoundExpr::LitFloat(2.5)), &s);
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
        round_instrs(n, &BoundExpr::LitFloat(1.5), &s)
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
        &lower_instrs(&func(NumFunc::Unary(FloatUnaryOp::Floor), BoundExpr::LitFloat(1.5)), &s),
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
    let mixed = min_max(true, vec![BoundExpr::ColRef(1), BoundExpr::LitFloat(1.5)]);
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
        let prog = eb.build(Some(reg)).expect("a well-formed program");
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
        assert!(matches!(err, GnitzSqlError::Unsupported(_)), "column {c}");
    }
}

/// The 64-register file bounds arity. The overflow is not a special case:
/// it is the same `TooManyRegs` any large expression hits, surfaced as a
/// plan-time `Unsupported` by the builder's one exit.
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
    match eb.build(Some(reg)).map_err(GnitzSqlError::from).err() {
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
    assert!(try_compile_string_cmp(&lit, BinOp::Add, &s, &schema.columns, &mut eb).is_none());
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
            BoundExpr::BinOp(
                Box::new(BoundExpr::ColRef(1)),
                BinOp::Gt,
                Box::new(BoundExpr::LitInt(0)),
            ),
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
    BoundExpr::InList { inner: Box::new(inner), items }
}

/// An integer operand with all-integer-literal items → one INT_IN_SET; the
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
/// INT_IN_SET.
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
        "float IN must not emit INT_IN_SET"
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
        "string IN must not emit INT_IN_SET"
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
/// item. (A literal list would take `INT_IN_SET`; one non-literal item is
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
        compile_bound_expr_to_program(&in_list(lit("x"), vec![str_col(1), str_col(2)]), &schema.columns).unwrap();
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
        "non-literal item must not emit INT_IN_SET"
    );
    assert!(
        has(prog.instrs(), |i| matches!(i, L::Cmp { op: CmpOp::Eq, .. })),
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
        !has(prog.instrs(), |i| matches!(i, L::IntInSet { .. })),
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

/// A plain `col op 'lit'` or `col op col` keeps the fused 16-byte-cell
/// opcodes; the register channel exists for computed operands. The fused
/// kernel short-circuits on the cell's 4-byte prefix, which a `StrView` does
/// not carry, and spends one register where the channel spends three.
#[test]
fn plain_column_comparisons_keep_the_specialized_opcodes() {
    let schema = str_schema();
    let instrs = lower_instrs(
        &BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Eq, Box::new(lit("x"))),
        &schema,
    );
    assert!(matches!(instrs[..], [L::StrColConst { op: CmpOp::Eq, .. }]));
    let cols = lower_instrs(
        &BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Lt, Box::new(str_col(2))),
        &schema,
    );
    assert!(matches!(cols[..], [L::StrColCol { op: CmpOp::Lt, .. }]));
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
            args: vec![str_col(1)],
        }),
        BinOp::Eq,
        Box::new(lit("X")),
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
/// pays a swap or a `BOOL_NOT`.
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
        let instrs = lower_instrs(&BoundExpr::BinOp(Box::new(up(1)), op, Box::new(up(2))), &schema);
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
    let pipe_null = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Concat, Box::new(BoundExpr::LitNull));
    assert_str_program(&pipe_null, &schema);
    assert!(has(&lower_instrs(&pipe_null, &schema), |i| matches!(i, L::LoadNullStr)));

    let pipe = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Concat, Box::new(lit("x")));
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
        args: vec![str_col(1), BoundExpr::LitInt(42), BoundExpr::LitFloat(1.5)],
    };
    let instrs = lower_instrs(&mixed, &schema);
    assert!(has(&instrs, |i| matches!(i, L::IntToStr { .. })), "{instrs:?}");
    assert!(has(&instrs, |i| matches!(i, L::FloatToStr { .. })), "{instrs:?}");
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
        branches: vec![(
            BoundExpr::NullTest {
                inner: Box::new(BoundExpr::ColRef(1)),
                want_null: false,
            },
            str_col(1),
        )],
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
    assert_eq!(lower_instrs_kind(&all_null, &schema).1, ExprKind::Int);
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
            f: NumFunc::Unary(FloatUnaryOp::Abs),
            arg: Box::new(s()),
        },
        BoundExpr::Func { f: NumFunc::Round(2), arg: Box::new(s()) },
        BoundExpr::MinMaxN { is_max: true, args: vec![s(), lit("x")] },
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
    // A comparison carries no implicit cast either way — against a literal or
    // against an integer column (`pk` here).
    let mixed = BoundExpr::BinOp(Box::new(s()), BinOp::Eq, Box::new(BoundExpr::LitInt(1)));
    assert!(lower_err(&mixed, &schema).to_string().contains("strings"));
    let mixed_cols = BoundExpr::BinOp(Box::new(s()), BinOp::Gt, Box::new(BoundExpr::ColRef(0)));
    assert!(lower_err(&mixed_cols, &schema).to_string().contains("strings"));
}

/// BLOB keeps exactly its existing comparison support: the column/literal
/// shapes compile, and everything else — including the string functions —
/// rejects.
#[test]
fn blob_columns_stay_outside_the_string_surface() {
    let schema = blob_schema();
    let cmp = BoundExpr::BinOp(Box::new(str_col(1)), BinOp::Eq, Box::new(lit("x")));
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
    let to = |tc| BoundExpr::Cast { expr: Box::new(str_col(1)), to: tc };
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
        to: TypeCode::String,
    };
    assert!(has(&lower_instrs(&to_text(1), &schema), |i| matches!(
        i,
        L::IntToStr { .. }
    )));
    let f = BoundExpr::Cast {
        expr: Box::new(BoundExpr::LitFloat(1.5)),
        to: TypeCode::String,
    };
    assert!(has(&lower_instrs(&f, &schema), |i| matches!(i, L::FloatToStr { .. })));
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
    assert!(matches!(
        p.instrs(),
        [L::LoadColStr { .. }, L::StrLike { ci: false, .. }]
    ));
    assert_eq!(p.const_strings()[0], b"a%");
    assert_eq!(escape_of(&p), std::num::NonZeroU8::new(b'\\'));
    // ILIKE is the same shape, `ci` set …
    assert!(matches!(
        prog(Some(b'\\'), true).instrs(),
        [L::LoadColStr { .. }, L::StrLike { ci: true, .. }]
    ));
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
        args: vec![str_col(1)],
    };
    assert!(matches!(
        lower_instrs(&like_of(upper, "A%", Some(b'\\'), false), &schema)[..],
        [L::LoadColStr { .. }, L::StrCase { upper: true, .. }, L::StrLike { .. }]
    ));
    assert!(matches!(
        lower_instrs(&like_of(BoundExpr::LitNull, "a", Some(b'\\'), false), &schema)[..],
        [L::LoadNullStr, L::StrLike { .. }]
    ));
    // The verdict is a boolean, so `NOT LIKE` reads it like any other.
    let negated = BoundExpr::UnaryOp(UnaryOp::Not, Box::new(like_of(str_col(1), "a", Some(b'\\'), false)));
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
    let err = compile_bound_expr_to_program(&like_of(BoundExpr::ColRef(1), "a", Some(b'\\'), false), &schema.columns)
        .expect_err("a numeric subject has no string channel");
    let GnitzSqlError::Unsupported(msg) = &err else {
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
        let (instrs, _) = lower_instrs_isf(&func(f, BoundExpr::LitFloat(2.0)), &s);
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
    let (instrs, is_float) = lower_instrs_isf(&func(NumFunc::Unary(FloatUnaryOp::Sign), BoundExpr::LitFloat(-2.0)), &s);
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
    let pow = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(1)),
        BinOp::Pow,
        Box::new(BoundExpr::LitInt(2)),
    );
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
            |i| matches!(i, &L::LoadConst { val } if f64::from_bits(val as u64) == 2.0)
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
            call(StrFunc::Pos, vec![str_col(1), lit("x")]),
            |i| matches!(i, L::StrPos { .. }),
            ExprKind::Int,
        ),
        (
            call(StrFunc::Replace, vec![str_col(1), lit("a"), lit("b")]),
            |i| matches!(i, L::StrReplace { .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Lpad, vec![str_col(1), n(), lit(" ")]),
            |i| matches!(i, L::StrPad { left: true, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::Rpad, vec![str_col(1), n(), lit(" ")]),
            |i| matches!(i, L::StrPad { left: false, .. }),
            ExprKind::Str,
        ),
        (
            call(StrFunc::SplitPart, vec![str_col(1), lit(","), n()]),
            |i| matches!(i, L::StrSplitPart { .. }),
            ExprKind::Str,
        ),
    ] {
        let (instrs, got_kind) = lower_instrs_kind(&expr, &schema);
        assert!(instrs.last().is_some_and(last), "{expr:?}: {instrs:?}");
        assert_eq!(got_kind, kind, "{expr:?}");
    }
    // An integer position rejects a float, naming the function and position;
    // a string position rejects a number.
    let err = lower_err(
        &call(StrFunc::Left, vec![str_col(1), BoundExpr::LitFloat(1.5)]),
        &schema,
    );
    assert!(err.to_string().contains("LEFT: argument 2 must be an integer"), "{err}");
    let err = lower_err(&call(StrFunc::Replace, vec![str_col(1), n(), lit("b")]), &schema);
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
    let sum = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(0)),
        BinOp::Add,
        Box::new(BoundExpr::LitInt(1)),
    );
    let instrs = lower_instrs(&test(sum, false), &schema);
    assert!(matches!(instrs.last(), Some(L::IsNullReg { invert: true, .. })));
}

/// A true-constant conjunct — the binder's fold of `IS NOT NULL` on a
/// non-nullable column — is dropped wherever it sits, so `a > 1 AND <true>`
/// costs no `BOOL_AND` per row; a list of nothing but true constants is the
/// statically-true verdict, and a false constant keeps its program.
#[test]
fn filter_program_drops_true_constant_conjuncts_in_any_position() {
    let schema = two_int_schema();
    let gt = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(1)),
        BinOp::Gt,
        Box::new(BoundExpr::LitInt(1)),
    );
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
    assert!(matches!(instrs(&[&f]).as_deref(), Some([L::LoadConst { val: 0 }])));
}

/// Every conjunct is a boolean, a lone one included: a string column on its
/// own draws the lowering's own message rather than reaching the resolver.
#[test]
fn a_lone_string_conjunct_is_rejected_by_lowering() {
    let schema = str_schema();
    let err = compile_filter_program([&str_col(1)], &schema.columns).expect_err("a string is not a predicate");
    assert!(err.to_string().contains("strings"), "{err}");
}
