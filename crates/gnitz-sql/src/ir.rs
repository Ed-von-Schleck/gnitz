use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, TypeCode};

#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum AggFunc {
    Count,
    CountNonNull,
    Sum,
    Min,
    Max,
    Avg,
}

/// The bound-expression IR, generic over its leaf reference type `R`. The three
/// leaf positions (`ColRef`, `IsNull`, `IsNotNull`) carry an `R`; every other
/// variant is structural and leaf-agnostic. The runtime IR pins `R = usize`
/// (the [`BoundExpr`] alias) — a resolved column index into a batch schema — and
/// every existing consumer names that alias. A future view-side leaf type will
/// instantiate `R` as a column-identity reference without touching the enum.
#[derive(Clone, Debug)]
pub(crate) enum BExpr<R> {
    ColRef(R),
    LitInt(i64),
    LitFloat(f64),
    LitStr(String),
    /// A non-fractional integer literal too wide for `i64` (`(i64::MAX, u128::MAX]`
    /// unsigned, or the `i64::MIN` magnitude under an outer `Neg`). The payload is
    /// the raw unsigned decimal magnitude (`Value::Number`'s digit string); a
    /// negative wide literal rides as an outer `UnaryOp(Neg, LitWide)`, mirroring
    /// `LitInt`. `bind_literal` is schemaless, so it cannot choose `i128`-vs-`u128`
    /// parsing — only the access-path recognizer, holding the column `TypeCode`,
    /// parses the string (byte-exactly, via `pk_codec`) into a PK/index seek bound.
    /// Everywhere else a `LitWide` is un-servable: the one reject arm in
    /// `lower_bound_expr` surfaces the VM's real 16-byte-slot limitation honestly.
    LitWide(String),
    /// SQL `NULL` literal / an implicit CASE ELSE. Lowers to `load_null`; its
    /// inferred type is `I64`, the neutral element of `unify_numeric` (so a NULL
    /// branch never drags a U64/float sibling back down).
    LitNull,
    BinOp(Box<BExpr<R>>, BinOp, Box<BExpr<R>>),
    UnaryOp(UnaryOp, Box<BExpr<R>>),
    IsNull(R),
    IsNotNull(R),
    AggCall {
        func: AggFunc,
        arg: Option<Box<BExpr<R>>>,
    },
    /// Searched CASE: `(condition, result)` branches taken in order, with an
    /// optional ELSE (implicit ELSE NULL when absent). `COALESCE`/`NULLIF`
    /// desugar into this variant during binding.
    Case {
        branches: Vec<(BExpr<R>, BExpr<R>)>,
        else_: Option<Box<BExpr<R>>>,
    },
    /// `inner IN (items…)` bound faithfully (un-desugared): each backend decides
    /// how to lower it — a `≤8-byte-integer` operand with all-integer-literal
    /// items compiles to one `INT_IN_SET`; anything else falls back to the
    /// `inner = i0 OR inner = i1 OR …` chain. `NOT IN` is the outer
    /// `UnaryOp(Not, InList)`. `items` is always non-empty (the binder rejects
    /// `IN ()`).
    InList {
        inner: Box<BExpr<R>>,
        items: Vec<BExpr<R>>,
    },
}

/// The runtime bound-expression IR: [`BExpr`] with its leaf reference resolved to
/// a `usize` column index. Every existing consumer names this alias, pinning
/// `R = usize` at every construction and pattern.
pub(crate) type BoundExpr = BExpr<usize>;

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

impl<R> BExpr<R> {
    /// Infer the result type, parameterized over how a leaf reference is typed.
    /// `ColRef` is the only leaf-typed arm — it consults `leaf_ty`; every other
    /// arm is structural (literals fix a type, comparisons/tests are `I64`,
    /// arithmetic/CASE fold via `unify_numeric`). `IsNull`/`IsNotNull`/`InList`
    /// are boolean and never consult `leaf_ty`. The runtime `usize` entry point
    /// is [`BExpr::infer_type`].
    pub(crate) fn infer_type_with<F: Fn(&R) -> TypeCode>(&self, leaf_ty: &F) -> TypeCode {
        match self {
            BExpr::ColRef(r) => leaf_ty(r),
            BExpr::LitInt(_) => TypeCode::I64,
            BExpr::LitFloat(_) => TypeCode::F64,
            BExpr::LitStr(_) => TypeCode::String,
            // A wide literal only ever appears in `col OP wide` (the `BinOp`
            // comparison arm returns `I64` regardless), and no caller consults a
            // wide *literal*'s type — so I64 is inert here, uniform with `LitInt`.
            BExpr::LitWide(_) => TypeCode::I64,
            BExpr::LitNull => TypeCode::I64,
            BExpr::BinOp(l, op, r) => {
                let lt = l.infer_type_with(leaf_ty);
                let rt = r.infer_type_with(leaf_ty);
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
            BExpr::UnaryOp(UnaryOp::Neg, inner) => inner.infer_type_with(leaf_ty),
            BExpr::UnaryOp(UnaryOp::Not, _) => TypeCode::I64,
            BExpr::IsNull(_) | BExpr::IsNotNull(_) => TypeCode::I64,
            BExpr::AggCall { func, arg } => match func {
                AggFunc::Avg => TypeCode::F64,
                AggFunc::Min | AggFunc::Max => {
                    if let Some(inner) = arg {
                        inner.infer_type_with(leaf_ty)
                    } else {
                        TypeCode::I64
                    }
                }
                _ => TypeCode::I64,
            },
            // CASE result type = unify_numeric over every result branch and the
            // else, seeded from the else (I64 when implicit), so an all-NULL CASE
            // stays I64 and a U64/float branch is preserved.
            BExpr::Case { branches, else_ } => {
                let mut ty = else_
                    .as_ref()
                    .map(|e| e.infer_type_with(leaf_ty))
                    .unwrap_or(TypeCode::I64);
                for (_cond, result) in branches {
                    ty = unify_numeric(ty, result.infer_type_with(leaf_ty));
                }
                ty
            }
            // The membership test is a boolean, like the comparison `BinOp` arm.
            BExpr::InList { .. } => TypeCode::I64,
        }
    }
}

impl<R> BExpr<R> {
    /// Rebuild the expression with every leaf reference (the `ColRef` / `IsNull`
    /// / `IsNotNull` positions) mapped through `f`; every other arm maps
    /// structurally. The one leaf-substitution walk — `hir::physical::resolve_refs`
    /// (`ColId → usize`) is an instantiation.
    pub(crate) fn try_map_refs<S, E>(&self, f: &impl Fn(&R) -> Result<S, E>) -> Result<BExpr<S>, E> {
        Ok(match self {
            BExpr::ColRef(r) => BExpr::ColRef(f(r)?),
            BExpr::IsNull(r) => BExpr::IsNull(f(r)?),
            BExpr::IsNotNull(r) => BExpr::IsNotNull(f(r)?),
            BExpr::LitInt(v) => BExpr::LitInt(*v),
            BExpr::LitFloat(v) => BExpr::LitFloat(*v),
            BExpr::LitStr(s) => BExpr::LitStr(s.clone()),
            BExpr::LitWide(s) => BExpr::LitWide(s.clone()),
            BExpr::LitNull => BExpr::LitNull,
            BExpr::BinOp(l, op, r) => BExpr::BinOp(Box::new(l.try_map_refs(f)?), *op, Box::new(r.try_map_refs(f)?)),
            BExpr::UnaryOp(op, inner) => BExpr::UnaryOp(*op, Box::new(inner.try_map_refs(f)?)),
            BExpr::AggCall { func, arg } => BExpr::AggCall {
                func: *func,
                arg: arg.as_deref().map(|a| a.try_map_refs(f)).transpose()?.map(Box::new),
            },
            BExpr::Case { branches, else_ } => BExpr::Case {
                branches: branches
                    .iter()
                    .map(|(c, r)| Ok((c.try_map_refs(f)?, r.try_map_refs(f)?)))
                    .collect::<Result<_, E>>()?,
                else_: else_.as_deref().map(|e| e.try_map_refs(f)).transpose()?.map(Box::new),
            },
            BExpr::InList { inner, items } => BExpr::InList {
                inner: Box::new(inner.try_map_refs(f)?),
                items: items.iter().map(|i| i.try_map_refs(f)).collect::<Result<_, E>>()?,
            },
        })
    }

    /// Visit every leaf reference (the `ColRef` / `IsNull` / `IsNotNull`
    /// positions), depth-first. The one reference-collection walk.
    pub(crate) fn for_each_ref(&self, f: &mut impl FnMut(&R)) {
        match self {
            BExpr::ColRef(r) | BExpr::IsNull(r) | BExpr::IsNotNull(r) => f(r),
            BExpr::LitInt(_) | BExpr::LitFloat(_) | BExpr::LitStr(_) | BExpr::LitWide(_) | BExpr::LitNull => {}
            BExpr::BinOp(l, _, r) => {
                l.for_each_ref(f);
                r.for_each_ref(f);
            }
            BExpr::UnaryOp(_, inner) => inner.for_each_ref(f),
            BExpr::AggCall { arg, .. } => {
                if let Some(a) = arg {
                    a.for_each_ref(f);
                }
            }
            BExpr::Case { branches, else_ } => {
                for (c, r) in branches {
                    c.for_each_ref(f);
                    r.for_each_ref(f);
                }
                if let Some(e) = else_ {
                    e.for_each_ref(f);
                }
            }
            BExpr::InList { inner, items } => {
                inner.for_each_ref(f);
                for i in items {
                    i.for_each_ref(f);
                }
            }
        }
    }
}

impl BExpr<usize> {
    /// The runtime entry point: type a `ColRef(idx)` leaf as the schema column's
    /// declared type (`cols[idx].type_code`, panicking on an
    /// out-of-bounds index — unchanged from the original body).
    pub(crate) fn infer_type(&self, cols: &[ColumnDef]) -> TypeCode {
        self.infer_type_with(&|idx: &usize| cols[*idx].type_code)
    }
}

/// The one message for every un-servable wide-integer literal: the compile
/// boundary ([`crate::lower::lower_bound_expr`]) and the eager mutate guards
/// (`find_wide_literal`) build it via [`wide_int_error`], so the outcome is one
/// uniform error naming the offending literal.
pub(crate) const WIDE_INT_UNSUPPORTED: &str = "wide-integer comparison is only servable as an indexed equality/range";

/// The uniform un-servable-wide-literal error, naming the literal. Shared by the
/// compile boundary and every eager guard so the message cannot drift.
pub(crate) fn wide_int_error(lit: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{WIDE_INT_UNSUPPORTED}: {lit}"))
}

/// The first [`BExpr::LitWide`] anywhere in `e`, if any — an **exhaustive** match
/// over every recursive arm (a naive `matches!(e, LitWide(_))` is dead code: the
/// real shape is `BinOp(ColRef, _, LitWide)`). Naming every variant forces a
/// compile error if a future arm is added. Used by the eager guards: the
/// SET/DO-UPDATE RHS and the UPDATE/DELETE residual are interpreted lazily
/// per-row, so an un-consumed `LitWide` there would silently no-op on an empty
/// match instead of rejecting deterministically.
pub(crate) fn find_wide_literal<R>(e: &BExpr<R>) -> Option<&str> {
    match e {
        BExpr::LitWide(s) => Some(s),
        BExpr::ColRef(_)
        | BExpr::LitInt(_)
        | BExpr::LitFloat(_)
        | BExpr::LitStr(_)
        | BExpr::LitNull
        | BExpr::IsNull(_)
        | BExpr::IsNotNull(_) => None,
        BExpr::BinOp(l, _, r) => find_wide_literal(l).or_else(|| find_wide_literal(r)),
        BExpr::UnaryOp(_, inner) => find_wide_literal(inner),
        BExpr::AggCall { arg, .. } => arg.as_deref().and_then(find_wide_literal),
        BExpr::Case { branches, else_ } => branches
            .iter()
            .find_map(|(c, r)| find_wide_literal(c).or_else(|| find_wide_literal(r)))
            .or_else(|| else_.as_deref().and_then(find_wide_literal)),
        BExpr::InList { inner, items } => find_wide_literal(inner).or_else(|| items.iter().find_map(find_wide_literal)),
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

    #[test]
    fn lit_null_infers_i64() {
        let s = schema(&[TypeCode::U64, TypeCode::I64]);
        assert_eq!(BoundExpr::LitNull.infer_type(&s.columns), TypeCode::I64);
    }

    /// Pins every `infer_type` arm the pre-existing tests do not reach (the
    /// literal/unary/null-test/agg/InList arms), so the verbatim move into the
    /// generic `infer_type_with` core is locally verified behavior-identical
    /// rather than relying on `make e2e`.
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
                items: vec![BoundExpr::LitInt(1)],
            }
            .infer_type(&s.columns),
            TypeCode::I64
        );
    }
}
