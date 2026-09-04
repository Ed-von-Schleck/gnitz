use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, TypeCode};

/// TRIM's mode is a wire operand, so the wire crate owns its definition; the IR
/// carries it verbatim.
pub(crate) use gnitz_expr::FloatUnaryOp;
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
    /// What this function produces. Read both by `infer_type_with`, to type the
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
    /// `infer_type_with`, to type the column a view declares, and by lowering,
    /// to class the register it writes — so the two cannot disagree.
    pub(crate) fn result_type(self, arg: TypeCode) -> TypeCode {
        use FloatUnaryOp as F;
        match self {
            // The transcendentals and a negative scale always lift to float.
            NumFunc::Unary(F::Sqrt | F::Ln | F::Log10 | F::Exp) => TypeCode::F64,
            NumFunc::Round(n) if n < 0 => TypeCode::F64,
            // SIGN's -1/0/1 is signed whatever integer it reads.
            NumFunc::Unary(F::Sign) if arg.is_float() => TypeCode::F64,
            NumFunc::Unary(F::Sign) => TypeCode::I64,
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
    /// arithmetic/CASE fold via `unify_blend_type`). `NullTest`/`InList` are
    /// boolean and never consult `leaf_ty`. The runtime `usize` entry point
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
                    BinOp::Pow => TypeCode::F64,
                    // Arithmetic preserves U64 (and floats), mirroring the engine's
                    // `reg_u64`: a materialized `u64 + u64` column must stay
                    // U64 so a downstream compare re-seeds the unsigned variant.
                    _ => unify_blend_type(lt, rt),
                }
            }
            BExpr::UnaryOp(UnaryOp::Neg, inner) => inner.infer_type_with(leaf_ty),
            BExpr::UnaryOp(UnaryOp::Not, _) => TypeCode::I64,
            BExpr::NullTest { .. } => TypeCode::I64,
            BExpr::Case { branches, else_ } => Self::case_type(branches, else_.as_deref(), leaf_ty),
            // The membership and pattern tests are booleans, like the comparison
            // `BinOp` arm.
            BExpr::InList { .. } | BExpr::Like { .. } => TypeCode::I64,
            BExpr::Func { f, arg } => f.result_type(arg.infer_type_with(leaf_ty)),
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

impl<R> BExpr<R> {
    /// Rebuild the expression structurally, replacing each `ColRef` by whatever
    /// `leaf` returns for it — another leaf, or a whole sub-expression. The one
    /// rebuilding walk (`for_each_ref` reads, `infer_type_with` types), so a new
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
            BExpr::StrCall { f, args } => BExpr::StrCall {
                f: *f,
                args: all(args)?,
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
            BExpr::Func { arg, .. } => arg.for_each_ref(f),
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
    /// declared type (`cols[idx].type_code`, panicking on an out-of-bounds index).
    pub(crate) fn infer_type(&self, cols: &[ColumnDef]) -> TypeCode {
        self.infer_type_with(&|idx: &usize| cols[*idx].type_code)
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
