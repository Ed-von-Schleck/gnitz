use super::*;
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
    assert_eq!(unify_blend_type(U64, F64), F64);
    assert_eq!(unify_blend_type(F32, I64), F64);
    // Else any U64 → U64.
    assert_eq!(unify_blend_type(U64, I64), U64);
    assert_eq!(unify_blend_type(I64, U64), U64);
    // Else I64 — narrow unsigned stays I64 (its value stays < 2^63).
    assert_eq!(unify_blend_type(I64, I64), I64);
    assert_eq!(unify_blend_type(U32, U16), I64);
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
    let add = |a, b| {
        BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(a)),
            BinOp::Add,
            Box::new(BoundExpr::ColRef(b)),
        )
    };
    // u64 + u64 → U64 (the folded-in correctness fix: must re-seed a
    // downstream unsigned compare).
    assert_eq!(add(1, 2).infer_type(&s.columns), TypeCode::U64);
    // u64 + f64 → F64.
    assert_eq!(add(1, 4).infer_type(&s.columns), TypeCode::F64);
    // u32 + u32 → I64 (unchanged; value stays < 2^63).
    assert_eq!(add(3, 3).infer_type(&s.columns), TypeCode::I64);
    // Comparisons stay I64.
    assert_eq!(
        BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Gt,
            Box::new(BoundExpr::ColRef(2))
        )
        .infer_type(&s.columns),
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
        .infer_type(&s.columns),
        TypeCode::U64
    );
    // A float branch dominates → F64.
    assert_eq!(
        case(
            vec![(BoundExpr::LitInt(1), BoundExpr::ColRef(3))],
            Some(Box::new(BoundExpr::ColRef(1)))
        )
        .infer_type(&s.columns),
        TypeCode::F64
    );
    // All-NULL CASE stays I64 (LitNull is the neutral element, seed I64).
    assert_eq!(
        case(vec![(BoundExpr::LitInt(1), BoundExpr::LitNull)], None).infer_type(&s.columns),
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
        .infer_type(&s.columns),
        TypeCode::U64
    );
}

/// `infer_type` reports an expression's *nominal* type — `Neg` preserves its
/// operand's. What a computed column is *declared* as is a separate rule
/// (`register_image`, applied where the column def is built), because a
/// stores a whole 8-byte register: an F32 negation computes in f64, and
/// declaring the column F32 shipped the low half of the double.
#[test]
fn neg_preserves_operand_type_and_register_image_widens_it() {
    let s = schema(&[TypeCode::U64, TypeCode::F32, TypeCode::F64, TypeCode::U32, TypeCode::I8]);
    let neg = |c: usize| BoundExpr::UnaryOp(UnaryOp::Neg, Box::new(BoundExpr::ColRef(c))).infer_type(&s.columns);
    assert_eq!((neg(1), neg(1).register_image()), (TypeCode::F32, TypeCode::F64));
    assert_eq!((neg(2), neg(2).register_image()), (TypeCode::F64, TypeCode::F64));
    assert_eq!((neg(3), neg(3).register_image()), (TypeCode::U32, TypeCode::I64));
    assert_eq!((neg(4), neg(4).register_image()), (TypeCode::I8, TypeCode::I64));
}

#[test]
fn lit_null_infers_i64() {
    let s = schema(&[TypeCode::U64, TypeCode::I64]);
    assert_eq!(BoundExpr::LitNull.infer_type(&s.columns), TypeCode::I64);
}

/// Pins the `infer_type` arms the tests above do not reach — the
/// literal/unary/null-test/agg/InList ones — so the generic
/// `infer_type_with` core is covered locally rather than only by `make e2e`.
#[test]
fn infer_type_covers_remaining_arms() {
    // pk U64, c1 U64, c2 String.
    let s = schema(&[TypeCode::U64, TypeCode::U64, TypeCode::String]);

    // Literal arms fix their type.
    assert_eq!(BoundExpr::LitFloat(1.5).infer_type(&s.columns), TypeCode::F64);
    assert_eq!(BoundExpr::LitStr("x".into()).infer_type(&s.columns), TypeCode::String);

    // UnaryOp(Neg) recurses into the inner type (U64 preserved); Not is boolean I64.
    let neg = BoundExpr::UnaryOp(UnaryOp::Neg, Box::new(BoundExpr::ColRef(1)));
    assert_eq!(neg.infer_type(&s.columns), TypeCode::U64);
    let not = BoundExpr::UnaryOp(UnaryOp::Not, Box::new(BoundExpr::ColRef(1)));
    assert_eq!(not.infer_type(&s.columns), TypeCode::I64);

    // IS [NOT] NULL are boolean I64.
    assert_eq!(BoundExpr::IsNull(1).infer_type(&s.columns), TypeCode::I64);
    assert_eq!(BoundExpr::IsNotNull(1).infer_type(&s.columns), TypeCode::I64);

    // AggCall: AVG is always F64; MIN/MAX inherit the argument type
    // (U64 here) and fall back to I64 with no argument; every other
    // aggregate (COUNT/SUM/…) is I64.
    assert_eq!(
        BoundExpr::AggCall {
            func: AggFunc::Avg,
            arg: Some(Box::new(BoundExpr::ColRef(1)))
        }
        .infer_type(&s.columns),
        TypeCode::F64
    );
    assert_eq!(
        BoundExpr::AggCall {
            func: AggFunc::Max,
            arg: Some(Box::new(BoundExpr::ColRef(1)))
        }
        .infer_type(&s.columns),
        TypeCode::U64
    );
    assert_eq!(
        BoundExpr::AggCall {
            func: AggFunc::Min,
            arg: None
        }
        .infer_type(&s.columns),
        TypeCode::I64
    );
    assert_eq!(
        BoundExpr::AggCall {
            func: AggFunc::Sum,
            arg: Some(Box::new(BoundExpr::ColRef(1)))
        }
        .infer_type(&s.columns),
        TypeCode::I64
    );
    assert_eq!(
        BoundExpr::AggCall {
            func: AggFunc::Count,
            arg: None
        }
        .infer_type(&s.columns),
        TypeCode::I64
    );

    // InList is a boolean membership test → I64.
    assert_eq!(
        BoundExpr::InList {
            inner: Box::new(BoundExpr::ColRef(1)),
            items: vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)],
        }
        .infer_type(&s.columns),
        TypeCode::I64
    );
}
