use super::*;
use crate::test_support::lit;
use gnitz_core::{ColumnDef, Schema};

fn schema(cols: &[TypeCode]) -> Schema {
    Schema {
        columns: cols
            .iter()
            .enumerate()
            .map(|(i, tc)| ColumnDef::new(format!("c{i}"), *tc, i != 0))
            .collect(),
        pk_cols: vec![0],
    }
}

#[test]
fn unify_blend_type_rule() {
    use TypeCode::*;
    // Any float → F64.
    assert_eq!(unify_blend_type(U64.into(), F64.into()), F64.into());
    assert_eq!(unify_blend_type(F32.into(), I64.into()), F64.into());
    // Else any U64 → U64.
    assert_eq!(unify_blend_type(U64.into(), I64.into()), U64.into());
    assert_eq!(unify_blend_type(I64.into(), U64.into()), U64.into());
    // Else I64 — narrow unsigned stays I64 (its value stays < 2^63).
    assert_eq!(unify_blend_type(I64.into(), I64.into()), I64.into());
    assert_eq!(unify_blend_type(U32.into(), U16.into()), I64.into());
}

#[test]
fn binop_arithmetic_preserves_u64() {
    // pk U64, c1 U64, c2 U64, c3 U32, c4 F64.
    let s = schema(&[
        TypeCode::U64,
        TypeCode::U64,
        TypeCode::U64,
        TypeCode::U32,
        TypeCode::F64,
    ]);
    let add = |a, b| BoundExpr::bin(BoundExpr::ColRef(a), BinOp::Add, BoundExpr::ColRef(b));
    // u64 + u64 → U64: a materialized column must re-seed a downstream
    // unsigned compare.
    assert_eq!(add(1, 2).infer_ty(&s.columns).tc, TypeCode::U64);
    // u64 + f64 → F64.
    assert_eq!(add(1, 4).infer_ty(&s.columns).tc, TypeCode::F64);
    // u32 + u32 → I64 (unchanged; value stays < 2^63).
    assert_eq!(add(3, 3).infer_ty(&s.columns).tc, TypeCode::I64);
    // Comparisons stay I64.
    assert_eq!(
        BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::ColRef(2))
            .infer_ty(&s.columns)
            .tc,
        TypeCode::I64
    );
}

#[test]
fn case_infer_type_folds_branches() {
    let s = schema(&[TypeCode::U64, TypeCode::U64, TypeCode::I64, TypeCode::F64]);
    let case = |branches, else_| BoundExpr::Case { branches, else_ };
    // CASE with a U64 result branch and an I64 else → U64.
    assert_eq!(
        case(
            vec![(BoundExpr::LitInt(1), BoundExpr::ColRef(1))],
            Some(Box::new(BoundExpr::ColRef(2)))
        )
        .infer_ty(&s.columns)
        .tc,
        TypeCode::U64
    );
    // A float branch dominates → F64.
    assert_eq!(
        case(
            vec![(BoundExpr::LitInt(1), BoundExpr::ColRef(3))],
            Some(Box::new(BoundExpr::ColRef(1)))
        )
        .infer_ty(&s.columns)
        .tc,
        TypeCode::F64
    );
    // All-NULL CASE stays I64 (LitNull is the neutral element, seed I64).
    assert_eq!(
        case(vec![(BoundExpr::LitInt(1), BoundExpr::LitNull)], None)
            .infer_ty(&s.columns)
            .tc,
        TypeCode::I64
    );
    // A NULL branch never drags a U64 sibling back down.
    assert_eq!(
        case(
            vec![
                (BoundExpr::LitInt(1), BoundExpr::LitNull),
                (BoundExpr::LitInt(1), BoundExpr::ColRef(1)),
            ],
            None
        )
        .infer_ty(&s.columns)
        .tc,
        TypeCode::U64
    );
}

/// A node types as the value it computes: a negation is the numeric function
/// over its operand's register, so a narrow integer negates in I64 (where
/// `-(-2^31)` lives), a float in F64, while U64 keeps its tracking and a DECIMAL
/// its scale.
#[test]
fn neg_types_as_the_register_it_computes() {
    let cols = [
        ColumnDef::new("pk", TypeCode::U64, false),
        ColumnDef::new("u32", TypeCode::U32, true),
        ColumnDef::new("i8", TypeCode::I8, true),
        ColumnDef::new("f32", TypeCode::F32, true),
        ColumnDef::new("u64", TypeCode::U64, true),
        ColumnDef::typed("d", ColType::decimal(2), true),
    ];
    let neg = |c: usize| {
        BoundExpr::Func {
            f: NumFunc::Unary(FloatUnaryOp::Neg),
            arg: Box::new(BoundExpr::ColRef(c)),
        }
        .infer_ty(&cols)
    };
    assert_eq!(neg(1), TypeCode::I64.into());
    assert_eq!(neg(2), TypeCode::I64.into());
    assert_eq!(neg(3), TypeCode::F64.into());
    assert_eq!(neg(4), TypeCode::U64.into());
    assert_eq!(neg(5), ColType::decimal(2));
}

/// A cast to a float computes an f64 register whatever its declared width; a
/// narrowing integer or DECIMAL cast is its target, which it range-checks into.
#[test]
fn a_cast_types_as_its_register_or_its_range_checked_target() {
    let s = schema(&[TypeCode::U64, TypeCode::I64]);
    let cast = |to: ColType| BoundExpr::Cast { expr: Box::new(BoundExpr::ColRef(1)), to }.infer_ty(&s.columns);
    assert_eq!(cast(TypeCode::F32.into()), TypeCode::F64.into());
    assert_eq!(cast(TypeCode::I16.into()), TypeCode::I16.into());
    assert_eq!(cast(ColType::decimal(3)), ColType::decimal(3));
}

/// A temporal literal types as its temporal type, spells its storage integer,
/// and is never NULL.
#[test]
fn a_temporal_literal_is_a_typed_non_null_integer() {
    let s = schema(&[TypeCode::U64]);
    let date = BoundExpr::LitTemporal { tc: TypeCode::Date, v: 18262 };
    assert_eq!(date.infer_ty(&s.columns), TypeCode::Date.into());
    assert_eq!(date.int_literal(), Some(18262));
    assert!(date.never_null_with(&|_: &usize| true));
}

/// Each shape over a NOT NULL column and over a nullable one: `never_null_with`
/// proves the first exactly when the engine kernels the node lowers to make no
/// NULL of their own, and never proves the second.
#[test]
fn never_null_follows_the_engine_null_table() {
    use BoundExpr as E;
    let nn = |e: &E| e.never_null_with(&|i: &usize| *i == 1);
    type Shape = fn(E) -> E;
    let shapes: &[(Shape, bool)] = &[
        (|x| E::bin(x, BinOp::Add, E::LitInt(1)), true),
        (|x| E::bin(x, BinOp::Pow, E::LitInt(2)), true),
        (|x| E::bin(x, BinOp::Div, E::LitInt(2)), true),
        (|x| E::bin(x.clone(), BinOp::Div, x), false),
        (|x| E::bin(x.clone(), BinOp::Concat, x), false),
        (|x| E::Not(Box::new(x)), true),
        (
            |x| E::Func {
                f: NumFunc::Unary(FloatUnaryOp::Abs),
                arg: Box::new(x),
            },
            true,
        ),
        (
            |x| E::Like {
                s: Box::new(x),
                pattern: "a%".into(),
                escape: None,
                ci: false,
            },
            true,
        ),
        (
            |x| E::TrimCall {
                s: Box::new(x),
                mode: TrimMode::Both,
                set: " ".into(),
            },
            true,
        ),
        (|x| E::Calendar { op: CalendarOp::Year, arg: Box::new(x) }, true),
        (
            |x| E::Calendar {
                op: CalendarOp::ToMicros,
                arg: Box::new(x),
            },
            false,
        ),
        (
            |x| E::InList {
                inner: Box::new(x),
                items: vec![E::LitInt(1), E::LitInt(2)],
            },
            true,
        ),
        (|x| E::MinMaxN { is_max: true, args: vec![x, E::LitNull] }, true),
        (|x| E::StrCall { f: StrFunc::Upper, args: vec![x] }, true),
        (
            |x| E::StrCall {
                f: StrFunc::Substr,
                args: vec![x, E::LitInt(1)],
            },
            true,
        ),
        (
            |x| E::StrCall {
                f: StrFunc::Substr,
                args: vec![x, E::LitInt(1), E::LitInt(2)],
            },
            false,
        ),
        (
            |x| E::StrCall {
                f: StrFunc::Replace,
                args: vec![x.clone(), x.clone(), x],
            },
            false,
        ),
        (
            |x| E::Cast {
                expr: Box::new(x),
                to: TypeCode::String.into(),
            },
            true,
        ),
        (
            |x| E::Cast {
                expr: Box::new(x),
                to: TypeCode::I16.into(),
            },
            false,
        ),
        (|x| E::ConcatN { args: vec![x] }, false),
        (
            |x| E::Case {
                branches: vec![(E::LitInt(1), x.clone())],
                else_: Some(Box::new(x)),
            },
            true,
        ),
        (
            |x| E::Case {
                branches: vec![(E::LitInt(1), x)],
                else_: None,
            },
            false,
        ),
    ];
    for &(shape, over_not_null) in shapes {
        let e = shape(E::ColRef(0));
        assert_eq!(nn(&e), over_not_null, "{e:?}");
        let e = shape(E::ColRef(1));
        assert!(!nn(&e), "{e:?}");
    }
    assert!(nn(&lit("1.5")) && !nn(&E::LitNull));
}

#[test]
fn lit_null_infers_i64() {
    let s = schema(&[TypeCode::U64, TypeCode::I64]);
    assert_eq!(BoundExpr::LitNull.infer_ty(&s.columns).tc, TypeCode::I64);
}

/// Pins the `infer_ty_with` arms the tests above do not reach — the
/// literal, `NOT`, null-test and InList ones.
#[test]
fn infer_type_covers_remaining_arms() {
    // pk U64, c1 U64, c2 String.
    let s = schema(&[TypeCode::U64, TypeCode::U64, TypeCode::String]);

    // Literal arms fix their type.
    assert_eq!(lit("1.5").infer_ty(&s.columns).tc, TypeCode::F64);
    assert_eq!(BoundExpr::LitStr("x".into()).infer_ty(&s.columns).tc, TypeCode::String);

    // NOT is boolean I64.
    let not = BoundExpr::Not(Box::new(BoundExpr::ColRef(1)));
    assert_eq!(not.infer_ty(&s.columns).tc, TypeCode::I64);

    // IS [NOT] NULL are boolean I64.
    for want_null in [true, false] {
        let t = BoundExpr::NullTest {
            inner: Box::new(BoundExpr::ColRef(1)),
            want_null,
        };
        assert_eq!(t.infer_ty(&s.columns).tc, TypeCode::I64);
    }

    // InList is a boolean membership test → I64.
    assert_eq!(
        BoundExpr::InList {
            inner: Box::new(BoundExpr::ColRef(1)),
            items: vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)],
        }
        .infer_ty(&s.columns)
        .tc,
        TypeCode::I64
    );
}

/// The numeric functions' result types: the rounding family and ABS keep the
/// argument's register image, the transcendentals lift to F64, and SIGN is a
/// signed integer over any integer argument and a float over a float one.
#[test]
fn num_func_result_types() {
    for f in [
        NumFunc::Unary(FloatUnaryOp::Abs),
        NumFunc::Unary(FloatUnaryOp::Floor),
        NumFunc::Unary(FloatUnaryOp::Ceil),
        NumFunc::Unary(FloatUnaryOp::Trunc),
        NumFunc::Round(2),
    ] {
        assert_eq!(f.result_type(TypeCode::U64.into()), TypeCode::U64.into(), "{f:?}");
        assert_eq!(f.result_type(TypeCode::I32.into()), TypeCode::I64.into(), "{f:?}");
        assert_eq!(f.result_type(TypeCode::F32.into()), TypeCode::F64.into(), "{f:?}");
    }
    assert_eq!(
        NumFunc::Round(-1).result_type(TypeCode::I64.into()),
        TypeCode::F64.into()
    );
    for f in [
        NumFunc::Unary(FloatUnaryOp::Sqrt),
        NumFunc::Unary(FloatUnaryOp::Ln),
        NumFunc::Unary(FloatUnaryOp::Log10),
        NumFunc::Unary(FloatUnaryOp::Exp),
    ] {
        assert_eq!(f.result_type(TypeCode::I64.into()), TypeCode::F64.into(), "{f:?}");
        assert_eq!(f.result_type(TypeCode::F64.into()), TypeCode::F64.into(), "{f:?}");
    }
    assert_eq!(
        NumFunc::Unary(FloatUnaryOp::Sign).result_type(TypeCode::U64.into()),
        TypeCode::I64.into()
    );
    assert_eq!(
        NumFunc::Unary(FloatUnaryOp::Sign).result_type(TypeCode::I8.into()),
        TypeCode::I64.into()
    );
    assert_eq!(
        NumFunc::Unary(FloatUnaryOp::Sign).result_type(TypeCode::F32.into()),
        TypeCode::F64.into()
    );
    // POWER is float over any operands.
    let s = schema(&[TypeCode::U64, TypeCode::I64]);
    let pow = BoundExpr::bin(BoundExpr::ColRef(1), BinOp::Pow, BoundExpr::LitInt(2));
    assert_eq!(pow.infer_ty(&s.columns).tc, TypeCode::F64);
}

/// Every string function's result type and signature agree with its node's
/// arity: the measures and STRPOS are integers, everything else a string.
#[test]
fn str_func_result_types_and_signatures() {
    use crate::ir::{StrArg, StrFunc};
    for f in [StrFunc::LenBytes, StrFunc::LenChars, StrFunc::Pos] {
        assert_eq!(f.result_type(), TypeCode::I64, "{f:?}");
    }
    for f in [
        StrFunc::Upper,
        StrFunc::Lower,
        StrFunc::Reverse,
        StrFunc::Left,
        StrFunc::Right,
        StrFunc::Replace,
        StrFunc::Lpad,
        StrFunc::Rpad,
        StrFunc::SplitPart,
        StrFunc::Substr,
    ] {
        assert_eq!(f.result_type(), TypeCode::String, "{f:?}");
    }
    assert_eq!(StrFunc::Substr.signature(), &[StrArg::Str, StrArg::Int, StrArg::IntOpt]);
    assert_eq!(StrFunc::Left.signature(), &[StrArg::Str, StrArg::Int]);
    assert_eq!(
        StrFunc::Lpad.signature(),
        &[StrArg::Str, StrArg::Int, StrArg::StrOr(" ")]
    );
    assert_eq!(StrFunc::SplitPart.signature(), &[StrArg::Str, StrArg::Str, StrArg::Int]);
    // The first argument of every string function is the string it acts on.
    for f in [StrFunc::Pos, StrFunc::Replace, StrFunc::Reverse] {
        assert_eq!(f.signature()[0], StrArg::Str);
    }
}

/// DECIMAL typing: a float literal is adopted at its own scale beside a
/// DECIMAL operand and stays a float elsewhere; `*` adds scales, `+`/`-` take
/// the wider, `/` and a float operand lift to F64, and the rounding family
/// lands on the scale it names.
#[test]
fn decimal_arithmetic_and_blend_typing() {
    let cols = vec![
        ColumnDef::new("pk", TypeCode::U64, false),
        ColumnDef::typed("p", ColType::decimal(2), true),
        ColumnDef::typed("q", ColType::decimal(3), true),
        ColumnDef::new("i", TypeCode::I64, true),
        ColumnDef::new("f", TypeCode::F64, true),
    ];
    let c = |i: usize| BoundExpr::ColRef(i);
    let ty = |e: &BoundExpr| e.infer_ty(&cols);
    let dec = ColType::decimal;
    let f64 = ColType::of(TypeCode::F64);
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Add, c(2))), dec(3));
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Sub, c(3))), dec(2));
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Mul, c(2))), dec(5));
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Mul, c(3))), dec(2));
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Mod, c(2))), dec(3));
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Div, c(2))), f64);
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Div, BoundExpr::LitInt(3))), f64);
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Add, c(4))), f64);
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Gt, c(2))), ColType::of(TypeCode::I64));
    // `price * 1.1` is exact at three places; `i * 1.1` is the float it was.
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Mul, lit("1.1"))), dec(3));
    assert_eq!(ty(&BoundExpr::bin(c(1), BinOp::Add, lit("1.255"))), dec(3));
    assert_eq!(ty(&BoundExpr::bin(c(3), BinOp::Mul, lit("1.1"))), f64);
    // CASE / GREATEST blend to the wider scale, adopting a literal the same way.
    let case = BoundExpr::Case {
        branches: vec![(BoundExpr::LitInt(1), c(1))],
        else_: Some(Box::new(lit("0.5"))),
    };
    assert_eq!(ty(&case), dec(2));
    assert_eq!(
        ty(&BoundExpr::MinMaxN {
            is_max: true,
            args: vec![c(1), c(2), c(3)]
        }),
        dec(3)
    );
    let func = |f, e: BoundExpr| BoundExpr::Func { f, arg: Box::new(e) };
    assert_eq!(ty(&func(NumFunc::Round(1), c(2))), dec(1));
    assert_eq!(ty(&func(NumFunc::Round(5), c(2))), dec(3));
    assert_eq!(ty(&func(NumFunc::Unary(FloatUnaryOp::Floor), c(2))), dec(0));
    assert_eq!(ty(&func(NumFunc::Unary(FloatUnaryOp::Abs), c(2))), dec(3));
    assert_eq!(ty(&func(NumFunc::Unary(FloatUnaryOp::Sqrt), c(2))), f64);
    assert_eq!(
        ty(&func(NumFunc::Unary(FloatUnaryOp::Sign), c(2))),
        ColType::of(TypeCode::I64)
    );
}
