use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, TypeCode};

/// TRIM's mode is a wire operand, so the wire crate owns its definition; the IR
/// carries it verbatim.
pub(crate) use gnitz_wire::TrimMode;

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
/// variant is structural and leaf-agnostic. Two instantiations are live: the
/// ad-hoc read path pins `R = usize` (the [`BoundExpr`] alias) — a resolved
/// column index into a batch schema — and the view path pins `R = HirRef`, a
/// column identity that survives the structural rewrites. Neither needed a
/// change to this enum.
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
    /// `OpcodeBackend::lower` surfaces the VM's real 16-byte-slot limitation honestly.
    LitWide(String),
    /// SQL `NULL` literal / an implicit CASE ELSE. Lowers to `load_null`; its
    /// inferred type is `I64`, the neutral element of `unify_blend_type` (so a NULL
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
    /// `inner IN (items…)` bound faithfully (un-desugared): lowering decides the
    /// form — a `≤8-byte-integer` operand with all-integer-literal items compiles to
    /// one `INT_IN_SET`; anything else falls back to the
    /// `inner = i0 OR inner = i1 OR …` chain. `NOT IN` is the outer
    /// `UnaryOp(Not, InList)`. `items` always holds two or more entries: the binder
    /// rejects `IN ()` and folds `IN (l)` to `Eq`, so the recognizers over bound
    /// conjuncts see a one-key list as the equality it is.
    InList {
        inner: Box<BExpr<R>>,
        items: Vec<BExpr<R>>,
    },
    Func {
        f: NumFunc,
        arg: Box<BExpr<R>>,
    },
    MinMaxN {
        is_max: bool,
        args: Vec<BExpr<R>>,
    },
    Cast {
        expr: Box<BExpr<R>>,
        to: TypeCode,
    },
    StrCall {
        f: StrFunc,
        arg: Box<BExpr<R>>,
    },
    /// `SUBSTRING(s FROM start [FOR len])` / `SUBSTR(s, start[, len])`. The
    /// bounds are arbitrary integer expressions, evaluated per row.
    Substr {
        s: Box<BExpr<R>>,
        start: Box<BExpr<R>>,
        len: Option<Box<BExpr<R>>>,
    },
    /// The trim set is compile-time data, not an operand: an ASCII string
    /// literal the binder has already checked, defaulting to a single space.
    TrimCall {
        s: Box<BExpr<R>>,
        mode: TrimMode,
        set: String,
    },
    /// `s [NOT] LIKE/ILIKE 'pattern' [ESCAPE c]`. Like `TrimCall`, one expression
    /// operand plus compile-time data: the pattern and its escape (`None` =
    /// escaping disabled) are literals the binder has already checked, which the
    /// engine tokenizes once per program. `ci` is ILIKE's ASCII-only case
    /// folding. `NOT LIKE` is the outer `UnaryOp(Not, Like)`.
    Like {
        s: Box<BExpr<R>>,
        pattern: String,
        escape: Option<u8>,
        ci: bool,
    },
    /// `CONCAT(args…)` with PostgreSQL's semantics — a NULL argument is the
    /// empty string, so the result is never NULL. Distinct from `BinOp::Concat`
    /// (`||`), which propagates NULL.
    ConcatN {
        args: Vec<BExpr<R>>,
    },
}

/// The unary string transforms and measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StrFunc {
    Upper,
    Lower,
    LenBytes,
    LenChars,
}

impl StrFunc {
    /// What this function produces. Read both by `infer_type_with`, to type the
    /// column a view declares, and by lowering, to class the register it writes
    /// — so a declared STRING column and a scalar register cannot disagree.
    pub(crate) fn result_type(self) -> TypeCode {
        match self {
            StrFunc::Upper | StrFunc::Lower => TypeCode::String,
            StrFunc::LenBytes | StrFunc::LenChars => TypeCode::I64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NumFunc {
    Abs,
    Floor,
    Ceil,
    Trunc,
    /// `ROUND(x, n)`, with `ROUND(x)` as `Round(0)`. The scale rides the node so
    /// lowering, which knows the argument's type, can fold the integer case
    /// instead of the binder committing to float arithmetic it cannot type.
    Round(i8),
}

/// The runtime bound-expression IR: [`BExpr`] with its leaf reference resolved to
/// a `usize` column index. Every existing consumer names this alias, pinning
/// `R = usize` at every construction and pattern.
pub(crate) type BoundExpr = BExpr<usize>;

/// Common type for arithmetic and conditional blends, matching the engine's
/// runtime register rule (`reg_u64`): any float operand → F64; else any U64
/// operand → U64 (so the materialized column re-seeds a downstream unsigned
/// compare); else I64. Those three are 8-byte slots — a pure type-label decision
/// (the integer arithmetic itself is bit-identical either way).
///
/// A STRING operand wins outright, because the two are different register
/// classes rather than two widths of one: a CASE with any string branch is a
/// string CASE, and lowering rejects the genuinely mixed shapes when it reads
/// the branches. Ranking it above F64 is what lets a string CASE be typed by
/// this one fold instead of a second pass beside it.
pub(crate) fn unify_blend_type(a: TypeCode, b: TypeCode) -> TypeCode {
    // Per-operand this is exactly `register_image`; unifying a pair is the
    // String > F64 > U64 > I64 join of the two images.
    match (a.register_image(), b.register_image()) {
        (TypeCode::String, _) | (_, TypeCode::String) => TypeCode::String,
        (TypeCode::F64, _) | (_, TypeCode::F64) => TypeCode::F64,
        (TypeCode::U64, _) | (_, TypeCode::U64) => TypeCode::U64,
        _ => TypeCode::I64,
    }
}

impl<R> BExpr<R> {
    /// Infer the result type, parameterized over how a leaf reference is typed.
    /// `ColRef` is the only leaf-typed arm — it consults `leaf_ty`; every other
    /// arm is structural (literals fix a type, comparisons/tests are `I64`,
    /// arithmetic/CASE fold via `unify_blend_type`). `IsNull`/`IsNotNull`/`InList`
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
                    // Comparisons and the connectives are boolean.
                    o if o.as_cmp().is_some() => TypeCode::I64,
                    BinOp::And | BinOp::Or => TypeCode::I64,
                    BinOp::Concat => TypeCode::String,
                    // Arithmetic preserves U64 (and floats), mirroring the engine's
                    // `reg_u64`: a materialized `u64 + u64` column must stay
                    // U64 so a downstream compare re-seeds the unsigned variant.
                    _ => unify_blend_type(lt, rt),
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
            BExpr::Case { branches, else_ } => Self::case_type(branches, else_.as_deref(), leaf_ty),
            // The membership and pattern tests are booleans, like the comparison
            // `BinOp` arm.
            BExpr::InList { .. } | BExpr::Like { .. } => TypeCode::I64,
            // Every transform is the identity on an integer register and its own
            // IEEE result on a float one — i.e. the argument's register image.
            // Only a negative ROUND scale forces the f64 lift on an integer.
            BExpr::Func { f, arg } => match f {
                NumFunc::Round(n) if *n < 0 => TypeCode::F64,
                _ => arg.infer_type_with(leaf_ty).register_image(),
            },
            // Seeded with `unify_blend_type`'s neutral element, so a one-argument
            // list types as its own register image and an empty one as I64.
            BExpr::MinMaxN { args, .. } => args
                .iter()
                .fold(TypeCode::I64, |ty, a| unify_blend_type(ty, a.infer_type_with(leaf_ty))),
            BExpr::Cast { to, .. } => *to,
            BExpr::StrCall { f, .. } => f.result_type(),
            BExpr::Substr { .. } | BExpr::TrimCall { .. } | BExpr::ConcatN { .. } => TypeCode::String,
        }
    }

    /// A CASE's result type, from its result branches and its else — the one
    /// walk both `infer_type_with` and lowering read it out of. Each branch is
    /// inferred exactly once, which matters because a CASE nested in a CASE would
    /// otherwise double per level.
    ///
    /// `unify_blend_type` over every branch and the else, seeded from the else (I64
    /// when implicit), so a U64/float branch is preserved and any string branch
    /// wins outright.
    ///
    /// `LitNull` carries no type signal — it infers as a hardcoded I64 — so it is
    /// polymorphic here. The consequence is that an all-NULL CASE still types
    /// I64 and so declares an I64 column, exactly as an all-NULL numeric CASE
    /// does today.
    pub(crate) fn case_type<F: Fn(&R) -> TypeCode>(
        branches: &[(BExpr<R>, BExpr<R>)],
        else_: Option<&BExpr<R>>,
        leaf_ty: &F,
    ) -> TypeCode {
        let mut ty = else_.map_or(TypeCode::I64, |e| e.infer_type_with(leaf_ty));
        for (_cond, result) in branches {
            ty = unify_blend_type(ty, result.infer_type_with(leaf_ty));
        }
        ty
    }
}

/// A leaf-bearing position handed to [`BExpr::try_rebuild`]'s callback: a bare
/// column reference, or a null test over one (`true` = `IS NULL`). These are the
/// only three arms that carry an `R`, so they are the only ones a rebuild has to
/// decide anything about.
enum Leaf<'a, R> {
    Col(&'a R),
    NullTest(&'a R, bool),
}

impl<R> BExpr<R> {
    /// Rebuild the expression structurally, handing each leaf-bearing position to
    /// `leaf`. The one **rebuilding** walk: every other arm just recurses, so a
    /// new `BExpr` variant is added here once rather than to each caller below.
    /// (`for_each_ref` is the read-only twin, and `infer_type_with` the typing
    /// one; a new variant fails to compile in all three.)
    ///
    /// `leaf` returns a whole `BExpr<S>`, not a reference, which is what lets the
    /// two instantiations differ in kind — one keeps a leaf a leaf, the other
    /// expands it into an arbitrary sub-expression.
    fn try_rebuild<S, E>(&self, leaf: &impl Fn(Leaf<'_, R>) -> Result<BExpr<S>, E>) -> Result<BExpr<S>, E> {
        let go = |e: &BExpr<R>| e.try_rebuild(leaf);
        let boxed = |e: &BExpr<R>| go(e).map(Box::new);
        let opt = |e: Option<&BExpr<R>>| e.map(boxed).transpose();
        let all = |es: &[BExpr<R>]| es.iter().map(go).collect::<Result<Vec<_>, E>>();
        Ok(match self {
            BExpr::ColRef(r) => leaf(Leaf::Col(r))?,
            BExpr::IsNull(r) => leaf(Leaf::NullTest(r, true))?,
            BExpr::IsNotNull(r) => leaf(Leaf::NullTest(r, false))?,
            BExpr::LitInt(v) => BExpr::LitInt(*v),
            BExpr::LitFloat(v) => BExpr::LitFloat(*v),
            BExpr::LitStr(s) => BExpr::LitStr(s.clone()),
            BExpr::LitWide(s) => BExpr::LitWide(s.clone()),
            BExpr::LitNull => BExpr::LitNull,
            BExpr::BinOp(l, op, r) => BExpr::BinOp(boxed(l)?, *op, boxed(r)?),
            BExpr::UnaryOp(op, inner) => BExpr::UnaryOp(*op, boxed(inner)?),
            BExpr::Func { f, arg } => BExpr::Func {
                f: *f,
                arg: boxed(arg)?,
            },
            BExpr::MinMaxN { is_max, args } => BExpr::MinMaxN {
                is_max: *is_max,
                args: all(args)?,
            },
            BExpr::Cast { expr, to } => BExpr::Cast {
                expr: boxed(expr)?,
                to: *to,
            },
            BExpr::AggCall { func, arg } => BExpr::AggCall {
                func: *func,
                arg: opt(arg.as_deref())?,
            },
            BExpr::Case { branches, else_ } => BExpr::Case {
                branches: branches
                    .iter()
                    .map(|(c, r)| Ok((go(c)?, go(r)?)))
                    .collect::<Result<_, E>>()?,
                else_: opt(else_.as_deref())?,
            },
            BExpr::InList { inner, items } => BExpr::InList {
                inner: boxed(inner)?,
                items: all(items)?,
            },
            BExpr::StrCall { f, arg } => BExpr::StrCall {
                f: *f,
                arg: boxed(arg)?,
            },
            BExpr::Substr { s, start, len } => BExpr::Substr {
                s: boxed(s)?,
                start: boxed(start)?,
                len: opt(len.as_deref())?,
            },
            BExpr::TrimCall { s, mode, set } => BExpr::TrimCall {
                s: boxed(s)?,
                mode: *mode,
                set: set.clone(),
            },
            BExpr::Like { s, pattern, escape, ci } => BExpr::Like {
                s: boxed(s)?,
                pattern: pattern.clone(),
                escape: *escape,
                ci: *ci,
            },
            BExpr::ConcatN { args } => BExpr::ConcatN { args: all(args)? },
        })
    }

    /// Rebuild with every leaf reference mapped through `f`; each leaf stays a
    /// leaf. `hir::physical::resolve_refs` (`ColId → usize`) is its instantiation.
    pub(crate) fn try_map_refs<S, E>(&self, f: &impl Fn(&R) -> Result<S, E>) -> Result<BExpr<S>, E> {
        self.try_rebuild(&|l| {
            Ok(match l {
                Leaf::Col(r) => BExpr::ColRef(f(r)?),
                Leaf::NullTest(r, true) => BExpr::IsNull(f(r)?),
                Leaf::NullTest(r, false) => BExpr::IsNotNull(f(r)?),
            })
        })
    }

    /// Rebuild, expanding each leaf-bearing position into an arbitrary
    /// sub-expression: `on_col` replaces a `ColRef(r)`, `on_null` replaces an
    /// `IsNull(r)` / `IsNotNull(r)` (its `bool` is `want_null`). The leaf-to-
    /// *expression* substitution walk — distinct from [`Self::try_map_refs`],
    /// which keeps each leaf a leaf; the HIR's subquery decorrelation and
    /// mark-constant folding are its two instantiations.
    pub(crate) fn try_expand_leaves<E>(
        &self,
        on_col: &impl Fn(&R) -> Result<BExpr<R>, E>,
        on_null: &impl Fn(&R, bool) -> Result<BExpr<R>, E>,
    ) -> Result<BExpr<R>, E> {
        self.try_rebuild(&|l| match l {
            Leaf::Col(r) => on_col(r),
            Leaf::NullTest(r, want_null) => on_null(r, want_null),
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
            BExpr::Func { arg, .. } => arg.for_each_ref(f),
            BExpr::MinMaxN { args, .. } => args.iter().for_each(|a| a.for_each_ref(f)),
            BExpr::Cast { expr, .. } => expr.for_each_ref(f),
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
            BExpr::StrCall { arg, .. } => arg.for_each_ref(f),
            BExpr::Substr { s, start, len } => {
                s.for_each_ref(f);
                start.for_each_ref(f);
                if let Some(l) = len {
                    l.for_each_ref(f);
                }
            }
            BExpr::TrimCall { s, .. } | BExpr::Like { s, .. } => s.for_each_ref(f),
            BExpr::ConcatN { args } => args.iter().for_each(|a| a.for_each_ref(f)),
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

/// The one home of "a WHERE is the left-associated AND of its conjuncts";
/// `None` when there are none. Every predicate the client compiles — the view
/// filter, the wire ReadSpec residual, the DML residual — is built here.
///
/// Left-association is `reduce`'s own shape, not a downstream requirement:
/// nothing that consumes the folded predicate inspects its association — this
/// crate's own conjunct flatteners recurse into both sides.
pub(crate) fn and_fold(preds: impl IntoIterator<Item = BoundExpr>) -> Option<BoundExpr> {
    preds
        .into_iter()
        .reduce(|acc, e| BExpr::BinOp(Box::new(acc), BinOp::And, Box::new(e)))
}

/// The one message for every un-servable wide-integer literal.
pub(crate) const WIDE_INT_UNSUPPORTED: &str = "wide-integer comparison is only servable as an indexed equality/range";

/// The uniform un-servable-wide-literal error, naming the literal. Built at the
/// compile boundary (`OpcodeBackend::lower`), which is the only place a
/// `LitWide` can survive to.
pub(crate) fn wide_int_error(lit: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{WIDE_INT_UNSUPPORTED}: {lit}"))
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
    /// SQL `||`. NULL-propagating, unlike `CONCAT`.
    Concat,
}

impl BinOp {
    /// The six ordering/equality operators — the ones defined on every scalar
    /// domain, and the only ones defined on strings — mapped to the opcode that
    /// computes them. The single spelling of that set: returning the `CmpOp`
    /// rather than a `bool` means the lowerer reads the mapping off this one
    /// match instead of re-enumerating the six with a catch-all arm.
    pub(crate) fn as_cmp(self) -> Option<gnitz_expr::CmpOp> {
        use gnitz_expr::CmpOp;
        match self {
            BinOp::Eq => Some(CmpOp::Eq),
            BinOp::Ne => Some(CmpOp::Ne),
            BinOp::Lt => Some(CmpOp::Lt),
            BinOp::Le => Some(CmpOp::Le),
            BinOp::Gt => Some(CmpOp::Gt),
            BinOp::Ge => Some(CmpOp::Ge),
            _ => None,
        }
    }

    /// The order-reversing converse: `x OP y` ⟺ `y OP.converse() x`. Only the
    /// four ordering operators flip; `Eq`/`Ne` are symmetric and every other
    /// operator (where operand order is not a comparison at all) passes through
    /// unchanged, so callers that transpose a whole expression can apply this
    /// unconditionally.
    pub(crate) fn converse(self) -> BinOp {
        match self {
            BinOp::Lt => BinOp::Gt,
            BinOp::Gt => BinOp::Lt,
            BinOp::Le => BinOp::Ge,
            BinOp::Ge => BinOp::Le,
            other => other,
        }
    }
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
    /// (`register_image`, applied where the column def is built), because EMIT
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
                items: vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)],
            }
            .infer_type(&s.columns),
            TypeCode::I64
        );
    }
}
