use crate::error::GnitzSqlError;
use gnitz_core::{ColType, ColumnDef, TypeCode};
use gnitz_expr::CalendarOp;
use gnitz_wire::decimal::{decimal_of_f64, rescale};

/// Both are instruction selectors, so the evaluator crate owns their
/// definitions; the IR carries each verbatim rather than restating it.
pub(crate) use gnitz_expr::{FloatUnaryOp, TrimMode};

#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The bound-expression IR, generic over its leaf reference type `R`, which only
/// `ColRef` carries. The ad-hoc read path pins `R = usize` (the [`BoundExpr`]
/// alias), a resolved column index into a batch schema; the view path pins
/// `R = HirRef`, a column identity that survives the structural rewrites.
/// `PartialEq` is structural equality over the *bound* form — what makes "the same
/// expression written twice" decidable after names resolve, so `t.a + b` and
/// `a + b` are one expression. Not `Eq`, because `LitFloat` compares by `f64`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BExpr<R> {
    ColRef(R),
    LitInt(i64),
    LitFloat(f64),
    LitStr(String),
    /// A non-fractional integer literal too wide for `i64` (`(i64::MAX, u128::MAX]`
    /// unsigned, or the `i64::MIN` magnitude under an outer `Neg`). The payload is
    /// the raw unsigned decimal magnitude (`Value::Number`'s digit string); a
    /// negative wide literal rides as an outer `UnaryOp(Neg, LitWide)` — unlike a
    /// `LitInt`, which carries its sign. `bind_literal` is schemaless, so it cannot choose `i128`-vs-`u128`
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
    /// `EXTRACT` / `DATE_PART` / `DATE_TRUNC` over a DATE or TIMESTAMP
    /// argument; lowering checks the argument's type, the binder is schema-free.
    Calendar {
        op: CalendarOp,
        arg: Box<BExpr<R>>,
    },
    /// `inner IS NULL` (`IS NOT NULL` when `want_null` is false). One node for
    /// every operand: a bare nullable column lowers to the column-bitmap opcode,
    /// anything else to a null test over the register it computes into.
    NullTest {
        inner: Box<BExpr<R>>,
        want_null: bool,
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
    /// one `IntInSet`; anything else falls back to the
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
        to: ColType,
    },
    StrCall {
        f: StrFunc,
        args: Vec<BExpr<R>>,
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

/// The string functions: transforms, measures and the multi-argument
/// producers, one node shape for all of them. The argument list's length and
/// classes are [`StrFunc::signature`]'s, established by the binder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StrFunc {
    Upper,
    Lower,
    LenBytes,
    LenChars,
    Reverse,
    Left,
    Right,
    /// `STRPOS(hay, needle)` and `POSITION(needle IN hay)`: 1-based character
    /// index, 0 when absent.
    Pos,
    Replace,
    Lpad,
    Rpad,
    SplitPart,
}

/// The class of one string-function argument. A trailing `StrOr` slot may be
/// omitted from the call and then takes its default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StrArg {
    Str,
    Int,
    StrOr(&'static str),
}

impl StrArg {
    pub(crate) fn default(self) -> Option<&'static str> {
        match self {
            StrArg::StrOr(d) => Some(d),
            StrArg::Str | StrArg::Int => None,
        }
    }
}

impl StrFunc {
    /// What this function produces. Read both by `infer_ty_with`, to type the
    /// column a view declares, and by lowering, to class the register it writes
    /// — so a declared STRING column and a scalar register cannot disagree.
    pub(crate) fn result_type(self) -> TypeCode {
        match self {
            StrFunc::LenBytes | StrFunc::LenChars | StrFunc::Pos => TypeCode::I64,
            StrFunc::Upper
            | StrFunc::Lower
            | StrFunc::Reverse
            | StrFunc::Left
            | StrFunc::Right
            | StrFunc::Replace
            | StrFunc::Lpad
            | StrFunc::Rpad
            | StrFunc::SplitPart => TypeCode::String,
        }
    }

    /// The argument classes in call order — the one statement of each
    /// function's arity, which the binder sizes the list by and lowering reads
    /// the operands through.
    pub(crate) fn signature(self) -> &'static [StrArg] {
        use StrArg::{Int, Str};
        match self {
            StrFunc::Upper | StrFunc::Lower | StrFunc::LenBytes | StrFunc::LenChars | StrFunc::Reverse => &[Str],
            StrFunc::Left | StrFunc::Right => &[Str, Int],
            StrFunc::Pos => &[Str, Str],
            StrFunc::Replace => &[Str, Str, Str],
            StrFunc::Lpad | StrFunc::Rpad => &[Str, Int, StrArg::StrOr(" ")],
            StrFunc::SplitPart => &[Str, Str, Int],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NumFunc {
    /// A float unary kernel, which over an integer register is the identity for
    /// the rounding family, an integer kernel for ABS, NEG and SIGN, and a lift
    /// to F64 for the transcendentals — the split [`NumFunc::result_type`]
    /// states and lowering follows.
    Unary(FloatUnaryOp),
    /// `ROUND(x, n)` — `ROUND(x)` is `Unary(Round)`. The scale rides the node so
    /// lowering, which knows the argument's type, can fold the integer case
    /// instead of the binder committing to float arithmetic it cannot type.
    Round(i8),
}

impl NumFunc {
    /// What this function produces over an argument of type `arg`. Read both by
    /// `infer_ty_with`, to type the column a view declares, and by lowering,
    /// to class the register it writes — so the two cannot disagree.
    pub(crate) fn result_type(self, arg: ColType) -> ColType {
        use FloatUnaryOp as F;
        match self {
            // The transcendentals and a negative scale always lift to float.
            NumFunc::Unary(F::Sqrt | F::Ln | F::Log10 | F::Exp) => ColType::of(TypeCode::F64),
            NumFunc::Round(n) if n < 0 => ColType::of(TypeCode::F64),
            // SIGN's -1/0/1 is signed whatever integer it reads.
            NumFunc::Unary(F::Sign) if arg.tc.is_float() => ColType::of(TypeCode::F64),
            NumFunc::Unary(F::Sign) => ColType::of(TypeCode::I64),
            // A DECIMAL keeps its scale under the sign kernels; rounding to `n`
            // places is a DECIMAL of `n` places, never wider than the argument.
            NumFunc::Unary(F::Neg | F::Abs) if arg.is_decimal() => arg,
            NumFunc::Round(n) if arg.is_decimal() => ColType::decimal((n as u8).min(arg.scale)),
            NumFunc::Unary(F::Floor | F::Ceil | F::Round | F::Trunc) if arg.is_decimal() => ColType::decimal(0),
            // The identity or an integer kernel over an integer register, the
            // IEEE result over a float one: the argument's own register image.
            NumFunc::Unary(F::Neg | F::Abs | F::Floor | F::Ceil | F::Round | F::Trunc) | NumFunc::Round(_) => {
                arg.register_image()
            }
        }
    }
}

/// The runtime bound-expression IR: [`BExpr`] with its leaf reference resolved to
/// a `usize` column index. Every existing consumer names this alias, pinning
/// `R = usize` at every construction and pattern.
pub(crate) type BoundExpr = BExpr<usize>;

/// Common type for arithmetic and conditional blends, matching the engine's
/// runtime register rule (`reg_u64`): any float operand → F64; else any DECIMAL
/// operand → the DECIMAL of the wider scale; else any U64 operand → U64 (so the
/// materialized column re-seeds a downstream unsigned compare); else I64. Those
/// are all 8-byte slots — a pure type-label decision (the integer arithmetic
/// itself is bit-identical either way).
///
/// A STRING operand wins outright, because the two are different register
/// classes rather than two widths of one: a CASE with any string branch is a
/// string CASE, and lowering rejects the genuinely mixed shapes when it reads
/// the branches. Ranking it above F64 is what lets a string CASE be typed by
/// this one fold instead of a second pass beside it.
pub(crate) fn unify_blend_type(a: ColType, b: ColType) -> ColType {
    // Per-operand this is exactly `register_image`; unifying a pair is the
    // String > F64 > DECIMAL > U64 > I64 join of the two images. Past the
    // String, F64, DECIMAL and U64 arms only {I64, DATE, TIMESTAMP} are left,
    // so the two below read: a
    // temporal image absorbs the neutral I64, an equal pair is itself (which is
    // reachable for a temporal pair alone), and DATE with TIMESTAMP falls to the
    // integer both of them are.
    let (a, b) = (a.register_image(), b.register_image());
    match (a.tc, b.tc) {
        (TypeCode::String, _) | (_, TypeCode::String) => ColType::of(TypeCode::String),
        (TypeCode::F64, _) | (_, TypeCode::F64) => ColType::of(TypeCode::F64),
        (TypeCode::Decimal, TypeCode::Decimal) => ColType::decimal(a.scale.max(b.scale)),
        (TypeCode::Decimal, _) => a,
        (_, TypeCode::Decimal) => b,
        (TypeCode::U64, _) | (_, TypeCode::U64) => ColType::of(TypeCode::U64),
        (x, TypeCode::I64) | (TypeCode::I64, x) => ColType::of(x),
        (x, y) if x == y => ColType::of(x),
        _ => ColType::of(TypeCode::I64),
    }
}

/// `+` / `-` with a temporal operand: a date or timestamp shifted by an integer
/// keeps its type, the difference of two of the same type is an integer, and
/// anything else takes the plain blend.
fn temporal_arith_type(op: BinOp, lt: ColType, rt: ColType) -> ColType {
    let is_int = |t: ColType| matches!(t.register_image().tc, TypeCode::I64 | TypeCode::U64);
    match (lt.tc.is_temporal(), rt.tc.is_temporal(), op) {
        (true, true, BinOp::Sub) if lt == rt => ColType::of(TypeCode::I64),
        (true, false, BinOp::Add | BinOp::Sub) if is_int(rt) => lt,
        (false, true, BinOp::Add) if is_int(lt) => rt,
        _ => unify_blend_type(lt, rt),
    }
}

/// The type a binary node with a DECIMAL operand computes in — arithmetic's
/// result, and the scale a comparison's operands meet at.
pub(crate) fn decimal_compute_type(op: BinOp, lt: ColType, rt: ColType) -> ColType {
    let blend = unify_blend_type(lt, rt);
    match op {
        _ if !blend.is_decimal() => blend,
        // Neither has a scale of its own: truncating a quotient to one would
        // make `1 / 3.0` read `0.3`.
        BinOp::Div | BinOp::Pow => ColType::of(TypeCode::F64),
        BinOp::Mul => ColType::decimal(lt.scale + rt.scale),
        _ => blend,
    }
}

/// The types of a node's operands as it combines them: each its own, except a
/// float literal beside a DECIMAL operand, which is read as the exact decimal
/// it spells — so `price * 1.1` stays exact over a DECIMAL column where it
/// stays a float over a DOUBLE one. Typing and lowering both read operands
/// through here, so they cannot disagree on which literal was adopted.
pub(crate) fn operand_tys<R, F: Fn(&R) -> ColType>(items: &[&BExpr<R>], leaf_ty: &F) -> Vec<ColType> {
    let mut tys: Vec<ColType> = items.iter().map(|e| e.infer_ty_with(leaf_ty)).collect();
    adopt_decimal_literals(items, &mut tys);
    tys
}

/// [`operand_tys`] for a binary node's two operands, which needs no heap — the
/// shape every `BinOp` reads, in typing and in lowering alike.
pub(crate) fn operand_ty_pair<R, F: Fn(&R) -> ColType>(l: &BExpr<R>, r: &BExpr<R>, leaf_ty: &F) -> (ColType, ColType) {
    let mut tys = [l.infer_ty_with(leaf_ty), r.infer_ty_with(leaf_ty)];
    adopt_decimal_literals(&[l, r], &mut tys);
    (tys[0], tys[1])
}

/// Re-read each float literal of `items` as the exact decimal it spells, where
/// any operand is a DECIMAL. The adoption both [`operand_tys`] shapes share.
fn adopt_decimal_literals<R>(items: &[&BExpr<R>], tys: &mut [ColType]) {
    if !tys.iter().any(|t| t.is_decimal()) {
        return;
    }
    for (e, ty) in items.iter().zip(tys) {
        if let BExpr::LitFloat(v) = e {
            if let Some((_, scale)) = decimal_of_f64(*v) {
                *ty = ColType::decimal(scale);
            }
        }
    }
}

impl<R> BExpr<R> {
    /// A `DATE '…'` / `TIMESTAMP '…'` literal: its storage integer under a cast
    /// to its type, so it types as a DATE/TIMESTAMP where a bare integer would
    /// not (a difference of two dates is an integer, a date shifted by one is
    /// a date). [`Self::int_literal`] is the inverse.
    pub(crate) fn temporal_lit(to: TypeCode, v: i64) -> Self {
        BExpr::Cast {
            expr: Box::new(BExpr::LitInt(v)),
            to: ColType::of(to),
        }
    }

    /// The integer a literal spells: a plain `LitInt`, or one under the typed
    /// cast a `DATE '…'` / `TIMESTAMP '…'` literal binds to — which is why the
    /// seek and IN-set recognizers read literals through here.
    pub(crate) fn int_literal(&self) -> Option<i64> {
        match self {
            BExpr::LitInt(v) => Some(*v),
            BExpr::Cast { expr, to } if to.tc.is_temporal() => expr.int_literal(),
            _ => None,
        }
    }

    /// The decimal a numeric literal spells, as `(unscaled, scale)` — an integer
    /// at scale 0, a float at the scale it was written with. The consumer
    /// re-expresses it at a column's scale, exactly or by rounding as its own
    /// contract says.
    pub(crate) fn decimal_literal(&self) -> Option<(i64, u8)> {
        match self {
            BExpr::LitInt(v) => Some((*v, 0)),
            BExpr::LitFloat(v) => decimal_of_f64(*v),
            _ => None,
        }
    }

    /// The `i64` this literal is at DECIMAL scale `scale` when it is exactly
    /// representable there — an integer, or a float written with no more
    /// fractional digits than the scale holds. A longer literal is not rounded:
    /// a key or a membership test against it must not match a neighbour.
    pub(crate) fn exact_decimal(&self, scale: u8) -> Option<i64> {
        let (v, s) = self.decimal_literal()?;
        (s <= scale).then(|| rescale(v as i128, s, scale)).flatten()
    }

    /// Infer the result type, parameterized over how a leaf reference is typed.
    /// `ColRef` is the only leaf-typed arm — it consults `leaf_ty`; every other
    /// arm is structural (literals fix a type, comparisons/tests are `I64`,
    /// arithmetic/CASE fold via `unify_blend_type`). `NullTest`/`InList` are
    /// boolean and never consult `leaf_ty`. The runtime `usize` entry point
    /// is [`BExpr::infer_type`].
    pub(crate) fn infer_ty_with<F: Fn(&R) -> ColType>(&self, leaf_ty: &F) -> ColType {
        let int = ColType::of(TypeCode::I64);
        match self {
            BExpr::ColRef(r) => leaf_ty(r),
            BExpr::LitInt(_) => int,
            BExpr::LitFloat(_) => ColType::of(TypeCode::F64),
            BExpr::LitStr(_) => ColType::of(TypeCode::String),
            // A wide literal only ever appears in `col OP wide` (the `BinOp`
            // comparison arm returns `I64` regardless), and no caller consults a
            // wide *literal*'s type — so I64 is inert here, uniform with `LitInt`.
            BExpr::LitWide(_) => int,
            BExpr::LitNull => int,
            BExpr::BinOp(l, op, r) => {
                let (lt, rt) = operand_ty_pair(l, r, leaf_ty);
                match op {
                    // Comparisons and the connectives are boolean.
                    o if o.as_cmp().is_some() => int,
                    BinOp::And | BinOp::Or => int,
                    BinOp::Concat => ColType::of(TypeCode::String),
                    BinOp::Pow => ColType::of(TypeCode::F64),
                    BinOp::Add | BinOp::Sub if lt.tc.is_temporal() || rt.tc.is_temporal() => {
                        temporal_arith_type(*op, lt, rt)
                    }
                    _ if lt.is_decimal() || rt.is_decimal() => decimal_compute_type(*op, lt, rt),
                    // Arithmetic preserves U64 (and floats), mirroring the engine's
                    // `reg_u64`: a materialized `u64 + u64` column must stay
                    // U64 so a downstream compare re-seeds the unsigned variant.
                    _ => unify_blend_type(lt, rt),
                }
            }
            BExpr::UnaryOp(UnaryOp::Neg, inner) => inner.infer_ty_with(leaf_ty),
            BExpr::UnaryOp(UnaryOp::Not, _) => int,
            BExpr::NullTest { .. } => int,
            BExpr::Case { branches, else_ } => Self::case_type(branches, else_.as_deref(), leaf_ty),
            // The membership and pattern tests are booleans, like the comparison
            // `BinOp` arm.
            BExpr::InList { .. } | BExpr::Like { .. } => int,
            BExpr::Func { f, arg } => f.result_type(arg.infer_ty_with(leaf_ty)),
            BExpr::Calendar { op, arg } => match op {
                CalendarOp::ToMicros => ColType::of(TypeCode::Timestamp),
                CalendarOp::ToDays => ColType::of(TypeCode::Date),
                op if op.keeps_type() => arg.infer_ty_with(leaf_ty),
                _ => int,
            },
            // Seeded with `unify_blend_type`'s neutral element, so a one-argument
            // list types as its own register image and an empty one as I64.
            BExpr::MinMaxN { args, .. } => operand_tys(&args.iter().collect::<Vec<_>>(), leaf_ty)
                .into_iter()
                .fold(int, unify_blend_type),
            BExpr::Cast { to, .. } => *to,
            BExpr::StrCall { f, .. } => ColType::of(f.result_type()),
            BExpr::Substr { .. } | BExpr::TrimCall { .. } | BExpr::ConcatN { .. } => ColType::of(TypeCode::String),
        }
    }

    /// Whether the expression can never evaluate to NULL, parameterized over
    /// whether a leaf reference can: arithmetic other than a division,
    /// comparison, and the connectives preserve non-nullness; a division by
    /// anything but a non-zero literal, a CASE without an ELSE, a cast, and
    /// every other node may produce NULL. Conservative: `false` is always safe.
    pub(crate) fn never_null_with<F: Fn(&R) -> bool>(&self, leaf_nullable: &F) -> bool {
        let go = |e: &BExpr<R>| e.never_null_with(leaf_nullable);
        match self {
            BExpr::ColRef(r) => !leaf_nullable(r),
            BExpr::LitInt(_) | BExpr::LitFloat(_) | BExpr::LitStr(_) | BExpr::LitWide(_) => true,
            BExpr::LitNull => false,
            BExpr::BinOp(l, BinOp::Div | BinOp::Mod, r) => matches!(r.as_ref(), BExpr::LitInt(n) if *n != 0) && go(l),
            BExpr::BinOp(_, BinOp::Pow, _) => false,
            BExpr::BinOp(l, _, r) => go(l) && go(r),
            BExpr::UnaryOp(_, inner) => go(inner),
            BExpr::NullTest { .. } => true,
            BExpr::Case { branches, else_ } => {
                else_.as_deref().is_some_and(go) && branches.iter().all(|(_, result)| go(result))
            }
            _ => false,
        }
    }

    /// A CASE's result type, from its result branches and its else — the one
    /// walk both `infer_ty_with` and lowering read it out of. Each branch is
    /// inferred exactly once, which matters because a CASE nested in a CASE would
    /// otherwise double per level.
    ///
    /// `unify_blend_type` over every branch and the else, seeded with I64 (the
    /// implicit else), so a U64/float branch is preserved and any string branch
    /// wins outright.
    ///
    /// `LitNull` carries no type signal — it infers as a hardcoded I64 — so it is
    /// polymorphic here. The consequence is that an all-NULL CASE still types
    /// I64 and so declares an I64 column, exactly as an all-NULL numeric CASE
    /// does today.
    pub(crate) fn case_type<F: Fn(&R) -> ColType>(
        branches: &[(BExpr<R>, BExpr<R>)],
        else_: Option<&BExpr<R>>,
        leaf_ty: &F,
    ) -> ColType {
        let results: Vec<&BExpr<R>> = branches.iter().map(|(_, r)| r).chain(else_).collect();
        operand_tys(&results, leaf_ty)
            .into_iter()
            .fold(ColType::of(TypeCode::I64), unify_blend_type)
    }
}

impl<R> BExpr<R> {
    /// Rebuild the expression structurally, replacing each `ColRef` by whatever
    /// `leaf` returns for it — another leaf, or a whole sub-expression. The one
    /// rebuilding walk (`for_each_ref` reads, `infer_ty_with` types), so a new
    /// variant is added in three places and fails to compile until it is.
    pub(crate) fn try_rebuild<S, E>(&self, leaf: &impl Fn(&R) -> Result<BExpr<S>, E>) -> Result<BExpr<S>, E> {
        let go = |e: &BExpr<R>| e.try_rebuild(leaf);
        let boxed = |e: &BExpr<R>| go(e).map(Box::new);
        let opt = |e: Option<&BExpr<R>>| e.map(boxed).transpose();
        let all = |es: &[BExpr<R>]| es.iter().map(go).collect::<Result<Vec<_>, E>>();
        Ok(match self {
            BExpr::ColRef(r) => leaf(r)?,
            BExpr::NullTest { inner, want_null } => BExpr::NullTest {
                inner: boxed(inner)?,
                want_null: *want_null,
            },
            BExpr::LitInt(v) => BExpr::LitInt(*v),
            BExpr::LitFloat(v) => BExpr::LitFloat(*v),
            BExpr::LitStr(s) => BExpr::LitStr(s.clone()),
            BExpr::LitWide(s) => BExpr::LitWide(s.clone()),
            BExpr::LitNull => BExpr::LitNull,
            BExpr::BinOp(l, op, r) => BExpr::BinOp(boxed(l)?, *op, boxed(r)?),
            BExpr::UnaryOp(op, inner) => BExpr::UnaryOp(*op, boxed(inner)?),
            BExpr::Func { f, arg } => BExpr::Func { f: *f, arg: boxed(arg)? },
            BExpr::Calendar { op, arg } => BExpr::Calendar { op: *op, arg: boxed(arg)? },
            BExpr::MinMaxN { is_max, args } => BExpr::MinMaxN { is_max: *is_max, args: all(args)? },
            BExpr::Cast { expr, to } => BExpr::Cast { expr: boxed(expr)?, to: *to },
            BExpr::Case { branches, else_ } => BExpr::Case {
                branches: branches
                    .iter()
                    .map(|(c, r)| Ok((go(c)?, go(r)?)))
                    .collect::<Result<_, E>>()?,
                else_: opt(else_.as_deref())?,
            },
            BExpr::InList { inner, items } => BExpr::InList { inner: boxed(inner)?, items: all(items)? },
            BExpr::StrCall { f, args } => BExpr::StrCall { f: *f, args: all(args)? },
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

    /// Visit every leaf reference (the `ColRef` positions), depth-first. The
    /// one reference-collection walk.
    pub(crate) fn for_each_ref(&self, f: &mut impl FnMut(&R)) {
        match self {
            BExpr::ColRef(r) => f(r),
            BExpr::NullTest { inner, .. } => inner.for_each_ref(f),
            BExpr::LitInt(_) | BExpr::LitFloat(_) | BExpr::LitStr(_) | BExpr::LitWide(_) | BExpr::LitNull => {}
            BExpr::BinOp(l, _, r) => {
                l.for_each_ref(f);
                r.for_each_ref(f);
            }
            BExpr::UnaryOp(_, inner) => inner.for_each_ref(f),
            BExpr::Func { arg, .. } | BExpr::Calendar { arg, .. } => arg.for_each_ref(f),
            BExpr::MinMaxN { args, .. } => args.iter().for_each(|a| a.for_each_ref(f)),
            BExpr::Cast { expr, .. } => expr.for_each_ref(f),
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
            BExpr::StrCall { args, .. } => args.iter().for_each(|a| a.for_each_ref(f)),
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
    /// declared type (`cols[idx].ty()`, panicking on an out-of-bounds index).
    pub(crate) fn infer_ty(&self, cols: &[ColumnDef]) -> ColType {
        self.infer_ty_with(&|idx: &usize| cols[*idx].ty())
    }

    /// [`Self::infer_ty`]'s type code, for the consumers a DECIMAL's scale does
    /// not concern.
    pub(crate) fn infer_type(&self, cols: &[ColumnDef]) -> TypeCode {
        self.infer_ty(cols).tc
    }
}

/// The one message for every un-servable wide-integer literal.
pub(crate) const WIDE_INT_UNSUPPORTED: &str = "wide-integer comparison is only servable as an indexed equality/range";

/// The uniform un-servable-wide-literal error, naming the literal. Built at the
/// compile boundary (`OpcodeBackend::lower`), which is the only place a
/// `LitWide` can survive to.
pub(crate) fn wide_int_error(lit: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{WIDE_INT_UNSUPPORTED}: {lit}"))
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
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
    /// `POWER(a, b)`: always float, whatever the operands. No operator
    /// spelling — the generic dialect reads `^` as XOR.
    Pow,
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

    /// The five integer arithmetic operators, mapped to the opcode operand that
    /// computes them — [`Self::as_cmp`]'s shape and rationale.
    pub(crate) fn as_int_arith(self) -> Option<gnitz_expr::IntArithOp> {
        use gnitz_expr::IntArithOp;
        match self {
            BinOp::Add => Some(IntArithOp::Add),
            BinOp::Sub => Some(IntArithOp::Sub),
            BinOp::Mul => Some(IntArithOp::Mul),
            BinOp::Div => Some(IntArithOp::Div),
            BinOp::Mod => Some(IntArithOp::Mod),
            _ => None,
        }
    }

    /// [`Self::as_int_arith`]'s float twin. `Mod` is absent: SQL defines no
    /// float modulo, and the float opcode space has none.
    pub(crate) fn as_float_arith(self) -> Option<gnitz_expr::FloatArithOp> {
        use gnitz_expr::FloatArithOp;
        match self {
            BinOp::Add => Some(FloatArithOp::Add),
            BinOp::Sub => Some(FloatArithOp::Sub),
            BinOp::Mul => Some(FloatArithOp::Mul),
            BinOp::Div => Some(FloatArithOp::Div),
            BinOp::Pow => Some(FloatArithOp::Pow),
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

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub(crate) enum UnaryOp {
    Neg,
    Not,
}

#[cfg(test)]
#[path = "tests/ir.rs"]
mod tests;
