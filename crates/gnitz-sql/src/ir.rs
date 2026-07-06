use gnitz_core::{Schema, TypeCode};

#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum AggFunc {
    Count,
    CountNonNull,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Debug)]
pub(crate) enum BoundExpr {
    ColRef(usize),
    LitInt(i64),
    LitFloat(f64),
    LitStr(String),
    /// SQL `NULL` literal / an implicit CASE ELSE. Lowers to `load_null`; its
    /// inferred type is `I64`, the neutral element of `unify_numeric` (so a NULL
    /// branch never drags a U64/float sibling back down).
    LitNull,
    BinOp(Box<BoundExpr>, BinOp, Box<BoundExpr>),
    UnaryOp(UnaryOp, Box<BoundExpr>),
    IsNull(usize),
    IsNotNull(usize),
    AggCall {
        func: AggFunc,
        arg: Option<Box<BoundExpr>>,
    },
    /// Searched CASE: `(condition, result)` branches taken in order, with an
    /// optional ELSE (implicit ELSE NULL when absent). `COALESCE`/`NULLIF`
    /// desugar into this variant during binding.
    Case {
        branches: Vec<(BoundExpr, BoundExpr)>,
        else_: Option<Box<BoundExpr>>,
    },
}

/// Common numeric type for arithmetic and conditional blends, matching the
/// engine's runtime register rule (`propagate_u64`): any float operand → F64;
/// else any U64 operand → U64 (so the materialized column re-seeds a downstream
/// unsigned compare); else I64. All three are 8-byte slots — a pure type-label
/// decision (the integer arithmetic itself is bit-identical either way).
pub(crate) fn unify_numeric(a: TypeCode, b: TypeCode) -> TypeCode {
    if matches!(a, TypeCode::F32 | TypeCode::F64) || matches!(b, TypeCode::F32 | TypeCode::F64) {
        TypeCode::F64
    } else if matches!(a, TypeCode::U64) || matches!(b, TypeCode::U64) {
        TypeCode::U64
    } else {
        TypeCode::I64
    }
}

impl BoundExpr {
    pub(crate) fn infer_type(&self, schema: &Schema) -> TypeCode {
        match self {
            BoundExpr::ColRef(idx) => schema.columns[*idx].type_code,
            BoundExpr::LitInt(_) => TypeCode::I64,
            BoundExpr::LitFloat(_) => TypeCode::F64,
            BoundExpr::LitStr(_) => TypeCode::String,
            BoundExpr::LitNull => TypeCode::I64,
            BoundExpr::BinOp(l, op, r) => {
                let lt = l.infer_type(schema);
                let rt = r.infer_type(schema);
                match op {
                    BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Ge | BinOp::Lt | BinOp::Le | BinOp::And | BinOp::Or => {
                        TypeCode::I64
                    }
                    // Arithmetic preserves U64 (and floats), mirroring the engine's
                    // `propagate_u64`: a materialized `u64 + u64` column must stay
                    // U64 so a downstream compare re-seeds the unsigned variant.
                    _ => unify_numeric(lt, rt),
                }
            }
            BoundExpr::UnaryOp(UnaryOp::Neg, inner) => inner.infer_type(schema),
            BoundExpr::UnaryOp(UnaryOp::Not, _) => TypeCode::I64,
            BoundExpr::IsNull(_) | BoundExpr::IsNotNull(_) => TypeCode::I64,
            BoundExpr::AggCall { func, arg } => match func {
                AggFunc::Avg => TypeCode::F64,
                AggFunc::Min | AggFunc::Max => {
                    if let Some(inner) = arg {
                        inner.infer_type(schema)
                    } else {
                        TypeCode::I64
                    }
                }
                _ => TypeCode::I64,
            },
            // CASE result type = unify_numeric over every result branch and the
            // else, seeded from the else (I64 when implicit), so an all-NULL CASE
            // stays I64 and a U64/float branch is preserved.
            BoundExpr::Case { branches, else_ } => {
                let mut ty = else_.as_ref().map(|e| e.infer_type(schema)).unwrap_or(TypeCode::I64);
                for (_cond, result) in branches {
                    ty = unify_numeric(ty, result.infer_type(schema));
                }
                ty
            }
        }
    }
}

#[derive(Clone, Debug, Copy)]
pub(crate) enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    And,
    Or,
}

#[derive(Clone, Debug, Copy)]
pub(crate) enum UnaryOp {
    Neg,
    Not,
}

#[cfg(test)]
mod tests {
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
    fn unify_numeric_rule() {
        use TypeCode::*;
        // Any float → F64.
        assert_eq!(unify_numeric(U64, F64), F64);
        assert_eq!(unify_numeric(F32, I64), F64);
        // Else any U64 → U64.
        assert_eq!(unify_numeric(U64, I64), U64);
        assert_eq!(unify_numeric(I64, U64), U64);
        // Else I64 — narrow unsigned stays I64 (its value stays < 2^63).
        assert_eq!(unify_numeric(I64, I64), I64);
        assert_eq!(unify_numeric(U32, U16), I64);
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
        assert_eq!(add(1, 2).infer_type(&s), TypeCode::U64);
        // u64 + f64 → F64.
        assert_eq!(add(1, 4).infer_type(&s), TypeCode::F64);
        // u32 + u32 → I64 (unchanged; value stays < 2^63).
        assert_eq!(add(3, 3).infer_type(&s), TypeCode::I64);
        // Comparisons stay I64.
        assert_eq!(
            BoundExpr::BinOp(
                Box::new(BoundExpr::ColRef(1)),
                BinOp::Gt,
                Box::new(BoundExpr::ColRef(2))
            )
            .infer_type(&s),
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
            .infer_type(&s),
            TypeCode::U64
        );
        // A float branch dominates → F64.
        assert_eq!(
            case(
                vec![(BoundExpr::LitInt(1), BoundExpr::ColRef(3))],
                Some(Box::new(BoundExpr::ColRef(1)))
            )
            .infer_type(&s),
            TypeCode::F64
        );
        // All-NULL CASE stays I64 (LitNull is the neutral element, seed I64).
        assert_eq!(
            case(vec![(BoundExpr::LitInt(1), BoundExpr::LitNull)], None).infer_type(&s),
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
            .infer_type(&s),
            TypeCode::U64
        );
    }

    #[test]
    fn lit_null_infers_i64() {
        let s = schema(&[TypeCode::U64, TypeCode::I64]);
        assert_eq!(BoundExpr::LitNull.infer_type(&s), TypeCode::I64);
    }
}
