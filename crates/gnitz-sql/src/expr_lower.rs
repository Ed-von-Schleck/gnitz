//! Expression lowering: a bound `BoundExpr` → the VM's opcode program, as a
//! wire `ExprProgram`, a resolved `Evaluator`, or raw predicate bytes.
//!
//! This is the *scalar* half of lowering. `hir::lower` is the *relational* half
//! (`RelExpr` → DBSP circuit) and calls into this one for every filter, map and
//! projection expression it emits.

use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, NumFunc, StrFunc, TrimMode, UnaryOp};
use crate::types::int_cast_target;
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_expr::{Evaluator, ExprBuilder, ExprValidateErr, FloatUnaryOp, IntUnaryOp, LogicalProgram, StrOp};

/// An IN-list item folds to an integer constant iff it is an integer literal or
/// the unary negation of one (`-1` binds to `UnaryOp(Neg, LitInt(1))` — sqlparser
/// lexes the minus separately). `wrapping_neg` matches the VM's int negate, so the
/// folded image is bit-identical to the runtime OR-chain literal. Anything else
/// → `None` → the OR-chain fallback.
fn fold_int_literal(e: &BoundExpr) -> Option<i64> {
    match e {
        BoundExpr::LitInt(v) => Some(*v),
        BoundExpr::UnaryOp(UnaryOp::Neg, inner) => match inner.as_ref() {
            BoundExpr::LitInt(v) => Some(v.wrapping_neg()),
            _ => None,
        },
        _ => None,
    }
}

/// How one of the six SQL comparisons rides the three string primitives: which
/// primitive to emit, whether to exchange its operands, and whether to negate
/// its result. The one place that mapping is written; `None` is "not a
/// comparison".
///
/// `GT`/`GE` have two realizations. A site that can exchange its operands gets
/// the swap, which is one opcode; `col OP 'lit'` cannot — its opcode shape pins
/// the constant to the right — so it passes `can_swap = false` and pays a
/// `bool_not` and the register that holds its input. Negation is 3VL-correct
/// because `bool_not` propagates the null bit, which is what `!=` already
/// relies on at every site.
fn str_cmp_reduction(op: BinOp, can_swap: bool) -> Option<(StrOp, bool, bool)> {
    Some(match op {
        BinOp::Eq => (StrOp::Eq, false, false),
        BinOp::Ne => (StrOp::Eq, false, true),
        BinOp::Lt => (StrOp::Lt, false, false),
        BinOp::Le => (StrOp::Le, false, false),
        BinOp::Gt if can_swap => (StrOp::Lt, true, false),
        BinOp::Ge if can_swap => (StrOp::Le, true, false),
        BinOp::Gt => (StrOp::Le, false, true),
        BinOp::Ge => (StrOp::Lt, false, true),
        _ => return None,
    })
}

/// Try to compile a string comparison (col vs const, const vs col, col vs col)
/// to the `ExprOp::StrCol*` opcodes, which read the 16-byte cells directly and
/// need no string register. `None` means "not this shape" — including any
/// operator that is not one of the six comparisons, which then falls through to
/// the register channel rather than erroring here (`strcol || 'lit'` would
/// otherwise die before the channel ever ran).
fn try_compile_string_cmp(
    left: &BoundExpr,
    op: &BinOp,
    right: &BoundExpr,
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Option<u32> {
    // Checked before anything is emitted: a later bail would leave a dead
    // const-pool entry behind.
    // ColRef(string) op LitStr(s), or LitStr(s) op ColRef(string). The opcode
    // shape pins the constant to the right, so the literal-on-left form
    // transposes the comparison to get there:
    //   'A' > col  ↔  col < 'A'      'A' >= col ↔ col <= 'A'
    //   'A' < col  ↔  col > 'A'      'A' <= col ↔ col >= 'A'
    // Eq/Ne are symmetric.
    let col_lit = match (left, right) {
        (BoundExpr::ColRef(idx), BoundExpr::LitStr(s)) => Some((*idx, s, *op)),
        (BoundExpr::LitStr(s), BoundExpr::ColRef(idx)) => Some((*idx, s, op.converse())),
        _ => None,
    };
    if let Some((idx, s, cmp)) = col_lit {
        // Resolved before anything is emitted, so a non-comparison declines
        // rather than leaving a dead const-pool entry behind. The constant is
        // pinned to the right here, hence `can_swap = false`.
        let (prim, _, negate) = str_cmp_reduction(cmp, false)?;
        // STRING and BLOB share the 16-byte German-string layout and the engine's
        // `str_col_*` opcodes content-compare both (via `compare_german_strings`),
        // so a BLOB column-vs-literal comparison lowers here too, not to the
        // integer path (which would read the descriptor bytes as a garbage int).
        if cols[idx].type_code.is_german_string() {
            let const_idx = eb.add_const_string(s.clone());
            let reg = eb.str_col_const(prim, idx, const_idx);
            return Some(if negate { eb.bool_not(reg) } else { reg });
        }
    }
    // ColRef(string/blob) op ColRef(string/blob) — two symmetric operands, so
    // GT/GE ride the swap rather than a negation.
    if let (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) = (left, right) {
        if cols[*a].type_code.is_german_string() && cols[*b].type_code.is_german_string() {
            let (prim, swap, negate) = str_cmp_reduction(*op, true)?;
            let (l, r) = if swap { (*b, *a) } else { (*a, *b) };
            let reg = eb.str_col_col(prim, l, r);
            return Some(if negate { eb.bool_not(reg) } else { reg });
        }
    }
    None
}

/// Which register class a lowered node produced. `Int` and `Float` are the two
/// scalar shapes — `Float` means the 8-byte register holds an f64 bit pattern —
/// and `Str` is the string register class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ExprKind {
    Int,
    Float,
    Str,
}

impl ExprKind {
    fn num(is_float: bool) -> Self {
        if is_float {
            ExprKind::Float
        } else {
            ExprKind::Int
        }
    }
}

/// Lowers a `BoundExpr` to `ExprProgram` opcodes for the server-side circuit.
/// Every node produces `(result_reg, ExprKind)`.
///
/// The single `match` in [`OpcodeBackend::lower`] is the sole walk of the enum:
/// adding a `BoundExpr` variant makes it non-exhaustive and fails compilation.
/// `binop` / `case` / `in_list` receive their operands *unevaluated*
/// (`&BoundExpr`) and drive the recursion themselves, which `binop` needs: it
/// intercepts a column/literal string comparison before recursing, so that shape
/// keeps the specialized opcodes instead of going through the register channel.
///
/// Operands are read through two class-specific helpers — [`Self::lower_num`]
/// and [`Self::str_operand`] — rather than through bare `lower`, so no arm has
/// to think about the other class. Bare `lower` survives only where the arm
/// itself dispatches on the kind: `binop`, `cast`, and `case`'s string-ness
/// decision.
struct OpcodeBackend<'a> {
    cols: &'a [ColumnDef],
    eb: &'a mut ExprBuilder,
}

impl OpcodeBackend<'_> {
    /// Lower one node. The lone `match` over `BoundExpr`. Arms that are a single
    /// builder call live here; the ones that recurse have their own method.
    fn lower(&mut self, expr: &BoundExpr) -> Result<(u32, ExprKind), GnitzSqlError> {
        match expr {
            BoundExpr::ColRef(c) => self.col_ref(*c),
            BoundExpr::LitInt(v) => Ok((self.eb.load_const(*v), ExprKind::Int)),
            BoundExpr::LitFloat(v) => Ok((self.eb.load_const(v.to_bits() as i64), ExprKind::Float)),
            BoundExpr::LitStr(s) => {
                let idx = self.eb.add_const_string(s.clone());
                Ok((self.eb.load_const_str(idx), ExprKind::Str))
            }
            // A wide-integer literal has no VM slot (the register file is 8 bytes
            // wide), so one reject arm here states the limitation for every
            // expression that reaches this walk. A *servable* wide seek is
            // consumed into a PK/index bound by the access-path recognizer and
            // never gets here.
            BoundExpr::LitWide(s) => Err(crate::ir::wide_int_error(s)),
            BoundExpr::LitNull => Ok(self.lit_null()),
            BoundExpr::BinOp(l, op, r) => self.binop(l, *op, r),
            BoundExpr::UnaryOp(op, inner) => self.unop(*op, inner),
            BoundExpr::IsNull(c) => Ok((self.eb.is_null(*c, /* invert = */ false), ExprKind::Int)),
            BoundExpr::IsNotNull(c) => Ok((self.eb.is_null(*c, /* invert = */ true), ExprKind::Int)),
            BoundExpr::AggCall { .. } => Err(GnitzSqlError::Unsupported(
                "aggregate function not allowed in expression context".to_string(),
            )),
            BoundExpr::Case { branches, else_ } => self.case(branches, else_.as_deref()),
            BoundExpr::InList { inner, items } => self.in_list(inner, items),
            BoundExpr::Func { f, arg } => self.func(*f, arg),
            BoundExpr::MinMaxN { is_max, args } => self.min_max_n(*is_max, args),
            BoundExpr::Cast { expr, to } => self.cast(expr, *to),
            BoundExpr::StrCall { f, arg } => self.str_call(*f, arg),
            BoundExpr::Substr { s, start, len } => self.substr(s, start, len.as_deref()),
            BoundExpr::TrimCall { s, mode, set } => self.trim_call(s, *mode, set),
            BoundExpr::Like { s, pattern, escape, ci } => self.like(s, pattern, *escape, *ci),
            BoundExpr::ConcatN { args } => self.concat_n(args),
        }
    }

    /// Lower `expr` into a scalar register, reporting whether it holds an f64.
    /// Every numeric operand is read through here, which is what turns a string
    /// in an arithmetic position into a SQL error instead of an engine-side
    /// `RegClassMismatch`.
    fn lower_num(&mut self, expr: &BoundExpr) -> Result<(u32, bool), GnitzSqlError> {
        match self.lower(expr)? {
            (r, ExprKind::Int) => Ok((r, false)),
            (r, ExprKind::Float) => Ok((r, true)),
            (_, ExprKind::Str) => Err(GnitzSqlError::Unsupported(format!(
                "{} is a string; strings support comparison, LIKE/ILIKE, CONCAT/||, \
                 CASE/COALESCE/NULLIF, the string functions and CAST — not this",
                self.describe(expr)
            ))),
        }
    }

    /// Name a rejected operand: a bare column by name, so the message points at
    /// the query text rather than at the expression tree.
    fn describe(&self, e: &BoundExpr) -> String {
        match e {
            BoundExpr::ColRef(i) => format!("column {:?}", self.cols[*i].name),
            BoundExpr::LitStr(_) => "a string literal".to_string(),
            _ => "this operand".to_string(),
        }
    }

    /// Lower `expr` and, if `want_float`, lift an integer register to f64. The
    /// one place an operand is coerced into a node's unified numeric domain.
    fn lower_as(&mut self, expr: &BoundExpr, want_float: bool) -> Result<u32, GnitzSqlError> {
        let (r, is_float) = self.lower_num(expr)?;
        Ok(if want_float && !is_float {
            self.eb.int_to_float(r)
        } else {
            r
        })
    }

    /// The string channel's operand read.
    ///
    /// This is the one place the LitNull-in-string-context rule lives. Lowering
    /// is eager — every arm appends its instruction immediately, and there is no
    /// IR to re-type and re-emit from — so a string context must intercept
    /// `LitNull` *before* recursing into it and emit `load_null_str` rather than
    /// the scalar `load_null`. Every string operand reads through here: a string
    /// CASE's branch results and else, `||`'s operands, CONCAT's arguments, and
    /// each string function's argument.
    ///
    /// It deliberately does not extend to comparisons: `UPPER(s) = NULL` stays a
    /// typed error, consistent with `s = NULL` today.
    ///
    /// `coerce_numeric` is CONCAT's implicit numeric→text cast, and nothing
    /// else's.
    fn str_operand(&mut self, expr: &BoundExpr, coerce_numeric: bool) -> Result<u32, GnitzSqlError> {
        if matches!(expr, BoundExpr::LitNull) {
            return Ok(self.eb.load_null_str());
        }
        match self.lower(expr)? {
            (r, ExprKind::Str) => Ok(r),
            (r, ExprKind::Int) if coerce_numeric => Ok(self.eb.int_to_str(r)),
            (r, ExprKind::Float) if coerce_numeric => Ok(self.eb.float_to_str(r)),
            _ => Err(GnitzSqlError::Unsupported("expected a string value here".to_string())),
        }
    }

    /// A SUBSTRING window bound. A float bound is a typed error rather than a
    /// silent truncation — the opcode reads the register as an integer.
    fn lower_window_bound(&mut self, e: &BoundExpr, what: &str) -> Result<u32, GnitzSqlError> {
        match self.lower_num(e)? {
            (r, false) => Ok(r),
            (_, true) => Err(GnitzSqlError::Unsupported(format!(
                "SUBSTRING: {what} must be an integer expression"
            ))),
        }
    }

    fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<(u32, ExprKind), GnitzSqlError> {
        if let NumFunc::Round(n) = f {
            return self.round(n, arg);
        }
        let (r, is_float) = self.lower_num(arg)?;
        if !is_float {
            // Every transform is the identity on an integer register, and ABS of
            // an unsigned one likewise — the value is non-negative by definition.
            let needs_abs = f == NumFunc::Abs && arg.infer_type(self.cols).is_signed_int();
            return Ok((
                if needs_abs {
                    self.eb.int_unary(IntUnaryOp::Abs, r)
                } else {
                    r
                },
                ExprKind::Int,
            ));
        }
        let reg = match f {
            NumFunc::Abs => self.eb.float_unary(FloatUnaryOp::Abs, r),
            NumFunc::Floor => self.eb.float_unary(FloatUnaryOp::Floor, r),
            NumFunc::Ceil => self.eb.float_unary(FloatUnaryOp::Ceil, r),
            NumFunc::Trunc => self.eb.float_unary(FloatUnaryOp::Trunc, r),
            NumFunc::Round(_) => unreachable!("routed to `round` above"),
        };
        Ok((reg, ExprKind::Float))
    }

    fn str_call(&mut self, f: StrFunc, arg: &BoundExpr) -> Result<(u32, ExprKind), GnitzSqlError> {
        let a = self.str_operand(arg, /* coerce_numeric = */ false)?;
        let reg = match f {
            StrFunc::Upper => self.eb.str_case(a, /* upper = */ true),
            StrFunc::Lower => self.eb.str_case(a, /* upper = */ false),
            StrFunc::LenChars => self.eb.str_len(a, /* chars = */ true),
            StrFunc::LenBytes => self.eb.str_len(a, /* chars = */ false),
        };
        // Class taken from the one result-type statement, not restated here.
        let kind = if f.result_type() == TypeCode::String {
            ExprKind::Str
        } else {
            ExprKind::Int
        };
        Ok((reg, kind))
    }

    fn substr(
        &mut self,
        s: &BoundExpr,
        start: &BoundExpr,
        len: Option<&BoundExpr>,
    ) -> Result<(u32, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let start_reg = self.lower_window_bound(start, "start")?;
        let len_reg = len.map(|l| self.lower_window_bound(l, "length")).transpose()?;
        Ok((self.eb.str_substr(src, start_reg, len_reg), ExprKind::Str))
    }

    fn trim_call(&mut self, s: &BoundExpr, mode: TrimMode, set: &str) -> Result<(u32, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let set_idx = self.eb.add_const_string(set.to_string());
        Ok((self.eb.str_trim(src, mode, set_idx), ExprKind::Str))
    }

    /// `s [I]LIKE 'pattern'`. The result is a boolean, so `NOT LIKE`'s `unop`
    /// reads it like any other.
    fn like(
        &mut self,
        s: &BoundExpr,
        pattern: &str,
        escape: Option<u8>,
        ci: bool,
    ) -> Result<(u32, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let pat_idx = self.eb.add_const_string(pattern.to_string());
        Ok((self.eb.str_like(src, escape, pat_idx, ci), ExprKind::Int))
    }

    /// `CONCAT(args…)`. Seeding the fold with the empty string gives every arity
    /// one shape and makes the one-argument case non-NULL.
    ///
    /// The fold is strictly left, which is what makes `str_concat_nn`'s
    /// asymmetric null rule sound: every argument lands in the `b` operand, so a
    /// NULL argument contributes the empty string while the accumulator's own
    /// NULL — the engine's length-overflow verdict — propagates to the result.
    fn concat_n(&mut self, args: &[BoundExpr]) -> Result<(u32, ExprKind), GnitzSqlError> {
        let empty = self.eb.add_const_string(String::new());
        let mut acc = self.eb.load_const_str(empty);
        for a in args {
            let r = self.str_operand(a, /* coerce_numeric = */ true)?;
            acc = self.eb.str_concat(acc, r, /* skip_null = */ true);
        }
        Ok((acc, ExprKind::Str))
    }

    /// `ROUND(x, n)`. The scale is applied here rather than in the binder because
    /// only lowering knows the argument's type: rounding an integer to a
    /// non-negative scale is the identity, and taking it through f64 instead
    /// would mangle any magnitude past 2^53. Scaling loads the positive power
    /// `10^|n|` in both directions — negative powers of ten are not f64-exact.
    fn round(&mut self, n: i8, arg: &BoundExpr) -> Result<(u32, ExprKind), GnitzSqlError> {
        if n >= 0 && !arg.infer_type(self.cols).is_float() {
            let (r, is_float) = self.lower_num(arg)?;
            return Ok((r, ExprKind::num(is_float)));
        }
        let mut v = self.lower_as(arg, true)?;
        if n == 0 {
            return Ok((self.eb.float_unary(FloatUnaryOp::Round, v), ExprKind::Float));
        }
        let scale = self.eb.load_const(10f64.powi(n.unsigned_abs() as i32).to_bits() as i64);
        // Multiply first for n > 0, divide first for n < 0; then undo.
        let up = n > 0;
        v = if up {
            self.eb.float_mul(v, scale)
        } else {
            self.eb.float_div(v, scale)
        };
        v = self.eb.float_unary(FloatUnaryOp::Round, v);
        Ok((
            if up {
                self.eb.float_div(v, scale)
            } else {
                self.eb.float_mul(v, scale)
            },
            ExprKind::Float,
        ))
    }

    /// GREATEST/LEAST as a left fold of 2-ary extremum opcodes. A non-numeric
    /// argument needs no check here: `lower_as` reads every argument through
    /// `lower_num`, which rejects a string, and a wide column dies at `col_ref`.
    fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<(u32, ExprKind), GnitzSqlError> {
        let types: Vec<TypeCode> = args.iter().map(|a| a.infer_type(self.cols)).collect();
        let unified = types
            .iter()
            .fold(TypeCode::I64, |acc, &t| crate::ir::unify_blend_type(acc, t));
        let any_float = unified.is_float();
        // The engine taints a register unsigned only from the first U64 operand
        // onward, so a U64 argument must head the fold — otherwise an earlier
        // signed pair would compare signed and the answer would depend on the
        // order the arguments were written in.
        let head = types
            .iter()
            .position(|t| t.register_image() == TypeCode::U64)
            .unwrap_or(0);
        let mut acc = self.lower_as(&args[head], any_float)?;
        for (i, a) in args.iter().enumerate() {
            if i == head {
                continue;
            }
            let r = self.lower_as(a, any_float)?;
            acc = match (any_float, is_max) {
                (true, is_max) => self.eb.float_minmax2(is_max, acc, r),
                (false, is_max) => self.eb.int_minmax2(is_max, acc, r),
            };
        }
        Ok((acc, ExprKind::num(any_float)))
    }

    /// CAST, dispatched on the source kind first, then the target — each arm
    /// leaves only the pairings the next one has to consider, so the numeric
    /// elide test at the end sees two scalar sides and nothing else.
    ///
    /// The three integer-target emitters take the width itself, so every arm
    /// that reaches one narrows `to` through
    /// [`int_cast_target`](crate::types::int_cast_target) first.
    fn cast(&mut self, expr: &BoundExpr, to: TypeCode) -> Result<(u32, ExprKind), GnitzSqlError> {
        let (r, kind) = self.lower(expr)?;
        if kind == ExprKind::Str {
            return Ok(match to {
                TypeCode::String => (r, ExprKind::Str),
                TypeCode::F64 => (self.eb.str_to_float(r), ExprKind::Float),
                TypeCode::F32 => {
                    let f = self.eb.str_to_float(r);
                    (self.eb.float_to_f32(f), ExprKind::Float)
                }
                _ => (self.eb.str_to_int(r, int_cast_target(to)?), ExprKind::Int),
            });
        }
        if to == TypeCode::String {
            let reg = if kind == ExprKind::Float {
                self.eb.float_to_str(r)
            } else {
                self.eb.int_to_str(r)
            };
            return Ok((reg, ExprKind::Str));
        }
        if to.is_float() {
            // `lower_as(expr, true)` is "lower once, then int_to_float if the
            // register is an int", so hoisting the lower emits the identical
            // sequence.
            let f = if kind == ExprKind::Int {
                self.eb.int_to_float(r)
            } else {
                r
            };
            // F64 needs nothing: the register already holds an f64.
            let reg = if to == TypeCode::F32 {
                self.eb.float_to_f32(f)
            } else {
                f
            };
            return Ok((reg, ExprKind::Float));
        }
        if kind == ExprKind::Float {
            return Ok((self.eb.float_to_int(r, int_cast_target(to)?), ExprKind::Int));
        }
        // Elide iff the register provably already holds a value inside `to`'s
        // domain *with* `to`'s register image. The image half matters because the
        // engine tracks a register as U64 iff its type is U64, so an elided cast
        // must not change that bit. Only a bare column load qualifies for the
        // narrower exact-type half: `infer_type` is not a value-domain lattice —
        // it types `-i32col` as I32, but the VM negate is a wrapping one on the i64
        // image, so `-(-2^31)` leaves I32.
        let elide = to == expr.infer_type(self.cols).register_image()
            || matches!(expr, BoundExpr::ColRef(i) if self.cols[*i].type_code == to);
        if elide {
            Ok((r, ExprKind::Int))
        } else {
            Ok((self.eb.int_cast(r, int_cast_target(to)?), ExprKind::Int))
        }
    }

    fn col_ref(&mut self, idx: usize) -> Result<(u32, ExprKind), GnitzSqlError> {
        // A 16-byte integer column must be rejected here: the engine's payload
        // integer load handler has arms only for 1/2/4/8-byte columns, so a wide
        // column hits its no-op arm and the following op reads stale scratch
        // bytes — silent corruption, no error.
        let tc = self.cols[idx].type_code;
        if tc.is_wide_int() {
            return Err(GnitzSqlError::Unsupported(format!(
                "column {:?} is {tc:?}; 128-bit columns cannot be used in expressions",
                self.cols[idx].name,
            )));
        }
        // BLOB shares STRING's 16-byte layout but is deliberately outside this
        // surface: its only valid use is one of the six comparisons, which
        // `binop` intercepts before any recursion reaches here.
        if tc.is_german_string() {
            if tc != TypeCode::String {
                return Err(GnitzSqlError::Unsupported(format!(
                    "column {:?} is {tc:?}; blob columns support only =, <>, <, <=, >, >= \
                     against another blob/string column or a string literal",
                    self.cols[idx].name,
                )));
            }
            return Ok((self.eb.load_col_str(idx), ExprKind::Str));
        }
        if tc.is_float() {
            return Ok((self.eb.load_col_float(idx), ExprKind::Float));
        }
        Ok((self.eb.load_col_int(idx), ExprKind::Int))
    }

    /// A scalar NULL: LoadNull carries a zero i64 lane with the null bit set. If
    /// a sibling CASE branch is float, `case` lifts it; a *string* context takes
    /// `str_operand`'s `load_null_str` path instead and never reaches here.
    fn lit_null(&mut self) -> (u32, ExprKind) {
        (self.eb.load_null(), ExprKind::Int)
    }

    fn case(
        &mut self,
        branches: &[(BoundExpr, BoundExpr)],
        else_: Option<&BoundExpr>,
    ) -> Result<(u32, ExprKind), GnitzSqlError> {
        // Decided before any result is lowered, because lowering is eager: a
        // `LitNull` result must be emitted as `load_null_str` in a string CASE,
        // and there is no IR to go back and re-type.
        let case_ty = BoundExpr::case_type(branches, else_, &|idx: &usize| self.cols[*idx].type_code);
        if case_ty == TypeCode::String {
            return self.str_case(branches, else_);
        }
        // Lower every condition and result, plus the else (implicit NULL when
        // absent). Conditions stay as 0/1 int registers; only the result *values*
        // participate in float unification.
        let mut conds = Vec::with_capacity(branches.len());
        let mut results = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            let (cond_reg, _cond_float) = self.lower_num(cond)?;
            conds.push(cond_reg);
            results.push(self.lower_num(result)?);
        }
        let else_out = match else_ {
            Some(e) => self.lower_num(e)?,
            None => (self.lit_null().0, false),
        };

        // Float unification is one GLOBAL decision, not a per-pair fold: ExprOp::Select
        // blends raw i64 bit patterns and converts nothing, so if any result or the
        // else is float, every int branch is lifted to float up front. A per-pair
        // lift (as in `binop`) would blend int and float bit patterns in one
        // register — the natural wrong implementation.
        let any_float = else_out.1 || results.iter().any(|(_, f)| *f);
        let lift = |eb: &mut ExprBuilder, reg: u32, is_float: bool| -> u32 {
            if any_float && !is_float {
                eb.int_to_float(reg)
            } else {
                reg
            }
        };
        // Fold right-to-left: acc = else; per pair acc = select(cond, result, acc),
        // so the first truthy WHEN wins.
        let mut acc = lift(self.eb, else_out.0, else_out.1);
        for i in (0..branches.len()).rev() {
            let result_reg = lift(self.eb, results[i].0, results[i].1);
            acc = self.eb.select(conds[i], result_reg, acc);
        }
        Ok((acc, ExprKind::num(any_float)))
    }

    /// The string half of [`Self::case`]. Conditions stay scalar; every result
    /// and the else read through `str_operand`, so a `LitNull` branch becomes a
    /// NULL *string* and a non-NULL scalar branch is a typed error. An implicit
    /// ELSE is `load_null_str`. No unification pass: string registers carry no
    /// float/int domain to reconcile.
    fn str_case(
        &mut self,
        branches: &[(BoundExpr, BoundExpr)],
        else_: Option<&BoundExpr>,
    ) -> Result<(u32, ExprKind), GnitzSqlError> {
        let mut conds = Vec::with_capacity(branches.len());
        let mut results = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            let (cond_reg, _) = self.lower_num(cond)?;
            conds.push(cond_reg);
            results.push(self.str_operand(result, false)?);
        }
        let mut acc = match else_ {
            Some(e) => self.str_operand(e, false)?,
            None => self.eb.load_null_str(),
        };
        // Right-to-left, so the first truthy WHEN wins — `case`'s fold.
        for i in (0..branches.len()).rev() {
            acc = self.eb.str_select(conds[i], results[i], acc);
        }
        Ok((acc, ExprKind::Str))
    }

    fn in_list(&mut self, inner: &BoundExpr, items: &[BoundExpr]) -> Result<(u32, ExprKind), GnitzSqlError> {
        // Fast path: a ≤8-byte-integer operand + every item a foldable integer
        // literal → one INT_IN_SET. `self.cols` is the schema `inner` was bound
        // against — source schema for a table filter, reduce-output schema for
        // HAVING — so the int gate is correct in both, and a HAVING large-IN
        // compiles here too. Use `is_fixed_int` — the same predicate the VM's
        // `ColKind::FixedIntCol` check applies — not `is_pk_eligible`, which wrongly
        // admits U128/UUID/I128.
        if gnitz_wire::is_fixed_int(inner.infer_type(self.cols) as u8) {
            if let Some(mut values) = items.iter().map(fold_int_literal).collect::<Option<Vec<i64>>>() {
                values.sort_unstable();
                values.dedup();
                let (reg, _is_float) = self.lower_num(inner)?; // integer ⇒ not float
                let idx = self.eb.add_const_int_set(&values);
                return Ok((self.eb.int_in_set(reg, idx), ExprKind::Int));
            }
        }
        // Fallback: fold `inner = item` per item through the existing binop path
        // (float operand → int_to_float + fcmp; non-literal item → column
        // compare). Folded iteratively rather than built as a left-nested OR
        // tree: `lower` descends the left spine before allocating any register,
        // so a long list overflowed the stack before `MAX_REGS` could reject it.
        // The emitted program is unchanged — `binop`'s `Or` arm lowers its left
        // operand fully before its right, which is this same order.
        let (mut acc, _) = self.binop(inner, BinOp::Eq, &items[0])?;
        for it in &items[1..] {
            let (r, _) = self.binop(inner, BinOp::Eq, it)?;
            acc = self.eb.bool_or(acc, r);
        }
        Ok((acc, ExprKind::Int))
    }

    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<(u32, ExprKind), GnitzSqlError> {
        // The column/literal shapes keep the specialized opcodes — intercepted
        // before recursing, so they never build a string register at all.
        if let Some(reg) = try_compile_string_cmp(left, &op, right, self.cols, self.eb) {
            return Ok((reg, ExprKind::Int));
        }

        // AND/OR read scalars and return before the operator dispatch below, so
        // they must reject a string operand themselves: `s AND 'x'` would
        // otherwise feed string registers to `bool_and` and surface as an
        // engine-side class mismatch rather than a SQL error.
        if matches!(op, BinOp::And | BinOp::Or) {
            let (l, _) = self.lower_num(left)?;
            let (r, _) = self.lower_num(right)?;
            let reg = if matches!(op, BinOp::And) {
                self.eb.bool_and(l, r)
            } else {
                self.eb.bool_or(l, r)
            };
            return Ok((reg, ExprKind::Int));
        }

        // `||` reads both operands through the string channel, so `s || NULL` is
        // a NULL string rather than a type error. There is no implicit numeric
        // cast on the operator, unlike CONCAT.
        if matches!(op, BinOp::Concat) {
            let l = self.str_operand(left, false)?;
            let r = self.str_operand(right, false)?;
            return Ok((self.eb.str_concat(l, r, /* skip_null = */ false), ExprKind::Str));
        }

        let (mut l, l_kind) = self.lower(left)?;
        let (mut r, r_kind) = self.lower(right)?;

        if l_kind == ExprKind::Str || r_kind == ExprKind::Str {
            return self.str_binop(op, (l, l_kind), (r, r_kind));
        }
        let (l_float, r_float) = (l_kind == ExprKind::Float, r_kind == ExprKind::Float);
        let is_float = l_float || r_float;

        // Cast int operand to float if mixed
        if is_float && !l_float {
            l = self.eb.int_to_float(l);
        }
        if is_float && !r_float {
            r = self.eb.int_to_float(r);
        }

        match (op, is_float) {
            // Arithmetic
            (BinOp::Add, false) => Ok((self.eb.add(l, r), ExprKind::Int)),
            (BinOp::Add, true) => Ok((self.eb.float_add(l, r), ExprKind::Float)),
            (BinOp::Sub, false) => Ok((self.eb.sub(l, r), ExprKind::Int)),
            (BinOp::Sub, true) => Ok((self.eb.float_sub(l, r), ExprKind::Float)),
            (BinOp::Mul, false) => Ok((self.eb.mul(l, r), ExprKind::Int)),
            (BinOp::Mul, true) => Ok((self.eb.float_mul(l, r), ExprKind::Float)),
            (BinOp::Div, false) => Ok((self.eb.div(l, r), ExprKind::Int)),
            (BinOp::Div, true) => Ok((self.eb.float_div(l, r), ExprKind::Float)),
            (BinOp::Mod, false) => Ok((self.eb.modulo(l, r), ExprKind::Int)),
            (BinOp::Mod, true) => Err(GnitzSqlError::Unsupported("float modulo not supported".to_string())),
            // Comparisons — result is always int (0/1). The operator travels as
            // data all the way to the opcode, so the float/int choice is the only
            // thing left to branch on.
            (BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Ge | BinOp::Lt | BinOp::Le, is_float) => {
                let cmp = op.as_cmp().expect("this arm lists exactly `as_cmp`'s `Some` set");
                let reg = if is_float {
                    self.eb.fcmp(cmp, l, r)
                } else {
                    self.eb.cmp(cmp, l, r)
                };
                Ok((reg, ExprKind::Int))
            }
            // Handled above, before the operands were lowered.
            (BinOp::And, _) | (BinOp::Or, _) | (BinOp::Concat, _) => unreachable!(),
        }
    }

    /// A binary operator with at least one string operand, having already ruled
    /// out `||` and the column/literal fast path. Only the six comparisons are
    /// defined, and both sides must be strings — a comparison carries no
    /// implicit cast.
    fn str_binop(
        &mut self,
        op: BinOp,
        (l, l_kind): (u32, ExprKind),
        (r, r_kind): (u32, ExprKind),
    ) -> Result<(u32, ExprKind), GnitzSqlError> {
        // The operator is reported before the operand kinds, so `strcol + 1`
        // reads as "no such operator on strings" rather than as a demand that
        // its right-hand side become one.
        let Some((prim, swap, negate)) = str_cmp_reduction(op, true) else {
            return Err(GnitzSqlError::Unsupported(format!(
                "operator {op:?} is not supported on a string operand"
            )));
        };
        if l_kind != ExprKind::Str || r_kind != ExprKind::Str {
            return Err(GnitzSqlError::Unsupported(format!(
                "comparison {op:?} against a string needs both operands to be strings"
            )));
        }
        let (l, r) = if swap { (r, l) } else { (l, r) };
        let reg = self.eb.str_cmp(prim, l, r);
        Ok((if negate { self.eb.bool_not(reg) } else { reg }, ExprKind::Int))
    }

    fn unop(&mut self, op: UnaryOp, inner: &BoundExpr) -> Result<(u32, ExprKind), GnitzSqlError> {
        let (a, a_float) = self.lower_num(inner)?;
        match op {
            UnaryOp::Neg => {
                if a_float {
                    Ok((self.eb.float_unary(FloatUnaryOp::Neg, a), ExprKind::Float))
                } else {
                    Ok((self.eb.int_unary(IntUnaryOp::Neg, a), ExprKind::Int))
                }
            }
            UnaryOp::Not => Ok((self.eb.bool_not(a), ExprKind::Int)),
        }
    }
}

/// Compile a BoundExpr to ExprBuilder opcodes, returning the result register.
/// The internal `is_float` bit that drives float-cast lowering is consumed only
/// inside the `OpcodeBackend` recursion and never escapes here.
pub(crate) fn compile_bound_expr(
    expr: &BoundExpr,
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Result<u32, GnitzSqlError> {
    OpcodeBackend { cols, eb }.lower(expr).map(|(reg, _)| reg)
}

/// Compile a standalone BoundExpr into a finished `ExprProgram` (fresh
/// `ExprBuilder`, result register wired up). The one-shot form behind every
/// WHERE/HAVING/residual filter that needs a whole program rather than a
/// register threaded into a larger one.
pub(crate) fn compile_bound_expr_to_program(
    expr: &BoundExpr,
    cols: &[ColumnDef],
) -> Result<gnitz_expr::ExprProgram, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let reg = compile_bound_expr(expr, cols, &mut eb)?;
    Ok(eb.build(reg))
}

/// Compile a whole-predicate WHERE/HAVING filter, or `None` when the binder
/// folded it to a true constant (e.g. `IS NOT NULL` on a non-nullable column) —
/// the caller then skips the filter operator entirely, instead of paying a
/// statically-true per-row evaluation plus a full output-batch copy on every
/// tick of the view's life. A false constant keeps its filter: it must still
/// drop every row.
pub(crate) fn compile_filter_program(
    pred: &BoundExpr,
    cols: &[ColumnDef],
) -> Result<Option<gnitz_expr::ExprProgram>, GnitzSqlError> {
    match pred {
        BoundExpr::LitInt(v) if *v != 0 => Ok(None),
        _ => Ok(Some(compile_bound_expr_to_program(pred, cols)?)),
    }
}

/// Lower the AND of `exprs` into `eb`, returning the result register. Each
/// conjunct lowers into its own register and the result is a `bool_and` fold over
/// them, which is byte-for-byte what `and_fold`'s left-associated tree lowers to
/// — without cloning a conjunct to build that tree. `None` is the statically-true
/// verdict (nothing to test, or a single true-constant conjunct), on which the
/// caller keeps every row rather than compiling a program.
pub(crate) fn lower_conjuncts(
    exprs: &[&BoundExpr],
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Result<Option<u32>, GnitzSqlError> {
    // Only a lone conjunct escapes the fold, so it is the only one that can still
    // be a bare true constant by the time it gets here.
    if exprs.is_empty() || matches!(exprs, [BoundExpr::LitInt(v)] if *v != 0) {
        return Ok(None);
    }
    let mut backend = OpcodeBackend { cols, eb };
    let mut acc = if exprs.len() == 1 {
        backend.lower(exprs[0])?.0
    } else {
        backend.lower_num(exprs[0])?.0
    };
    for e in &exprs[1..] {
        let (r, _) = backend.lower_num(e)?;
        acc = backend.eb.bool_and(acc, r);
    }
    Ok(Some(acc))
}

/// The wire predicate blob for the AND of `exprs` — the conjunct-list form of
/// [`compile_wire_predicate`]. An empty blob is the statically-true verdict (the
/// caller's bound is exact).
pub(crate) fn compile_wire_conjuncts(exprs: &[&BoundExpr], cols: &[ColumnDef]) -> Result<Vec<u8>, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    match lower_conjuncts(exprs, cols, &mut eb)? {
        Some(reg) => encode_validated(eb.build(reg)),
        None => Ok(Vec::new()),
    }
}

/// Encode a compiled program as the wire blob, validating it the way the worker's
/// `LogicalProgram::from_wire` will. Nothing client-side runs it; validating here
/// turns a round-trip `STATUS_ERROR` naming an internal enum into a plan-time
/// `Unsupported`.
fn encode_validated(p: gnitz_expr::ExprProgram) -> Result<Vec<u8>, GnitzSqlError> {
    let blob = p.encode();
    LogicalProgram::from_wire(&p.code, p.num_regs, p.result_reg, p.const_strings).map_err(expr_unsupported)?;
    Ok(blob)
}

/// Compile a whole-predicate WHERE/HAVING/residual filter into the shared
/// evaluator, resolved against the schema the rows it will run over carry.
/// `None` is [`compile_filter_program`]'s statically-true verdict — the caller
/// keeps every row.
///
/// Picks `resolve_filter`, so a bare non-boolean predicate (`HAVING COUNT(*)`)
/// still gets the `bool_bits` bit [`Evaluator::filter`] reads.
pub(crate) fn compile_filter_evaluator(pred: &BoundExpr, schema: &Schema) -> Result<Option<Evaluator>, GnitzSqlError> {
    compile_conjuncts_evaluator(&[pred], schema)
}

/// [`compile_filter_evaluator`] over a conjunct list, AND-combined by
/// [`lower_conjuncts`]. `None` when there is nothing to test — no conjuncts, or a
/// statically-true one — and the caller keeps every row.
pub(crate) fn compile_conjuncts_evaluator(
    preds: &[&BoundExpr],
    schema: &Schema,
) -> Result<Option<Evaluator>, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let Some(reg) = lower_conjuncts(preds, &schema.columns, &mut eb)? else {
        return Ok(None);
    };
    let prog = eb.build_logical(reg).map_err(expr_unsupported)?;
    Ok(Some(prog.resolve_filter(schema).map_err(expr_unsupported)?))
}

/// Compile a scalar (non-predicate) RHS — a SET / `DO UPDATE SET` value — into
/// the shared evaluator, resolved against the schema the rows it will run over
/// carry, and reject a float-typed result.
///
/// The float test has to happen here: nothing downstream can tell a float
/// bit-pattern from an integer, so `append_column_value` would store the raw
/// bits into an integer column.
///
/// Picks `resolve_scalar`, because a SET RHS is read a row at a time. Pairing
/// each resolver with the one way of driving it is why `expr_unsupported` is
/// private to this module.
///
/// It runs the backend itself rather than going through
/// [`compile_bound_expr_to_program`], because it is the one caller that needs
/// the recursion's kind — to reject a float result. Whether the result is a
/// string, and so which read-back applies, the caller asks the returned
/// evaluator (`Evaluator::result_is_str`).
pub(crate) fn compile_scalar_evaluator(expr: &BoundExpr, schema: &Schema) -> Result<Evaluator, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let (reg, kind) = OpcodeBackend {
        cols: &schema.columns,
        eb: &mut eb,
    }
    .lower(expr)?;
    if kind == ExprKind::Float {
        // No target type accepts it: the register holds an f64 bit pattern, and
        // a SET value can only be written to a fixed-width integer or a string
        // column.
        return Err(GnitzSqlError::Unsupported(
            "SET from a floating-point expression is not supported".to_string(),
        ));
    }
    eb.build_logical(reg)
        .map_err(expr_unsupported)?
        .resolve_scalar(schema)
        .map_err(expr_unsupported)
}

/// A shared-evaluator rejection as a SQL-layer `Unsupported`. The wording is
/// `ExprValidateErr`'s own `Display`, so a query rejected here and the same
/// program rejected by the engine's circuit compiler read identically.
fn expr_unsupported(e: ExprValidateErr) -> GnitzSqlError {
    GnitzSqlError::Unsupported(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, Schema, TypeCode};
    use gnitz_expr::ExprProgram;
    use gnitz_wire::ExprOp;

    /// Decode a built program the way the engine will. Production resolves locally
    /// without a wire round-trip, but these tests are about what the *engine*
    /// accepts, so they go through the decoder.
    fn to_logical(p: ExprProgram) -> Result<LogicalProgram, GnitzSqlError> {
        LogicalProgram::from_wire(&p.code, p.num_regs, p.result_reg, p.const_strings).map_err(expr_unsupported)
    }

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

    fn compile(left: &BoundExpr, op: BinOp, right: &BoundExpr, schema: &Schema) -> ExprProgram {
        let mut eb = ExprBuilder::new();
        let reg = try_compile_string_cmp(left, &op, right, &schema.columns, &mut eb)
            .expect("recognized as a string comparison");
        eb.build(reg)
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
        (opcodes(&eb.build(reg)), kind)
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

    /// Lower `expr` and hand the result to the engine's own decoder.
    ///
    /// This is the check that matters for the string channel, and it is not an
    /// opcode snapshot: `from_wire` holds every register operand to the class
    /// its opcode reads, so a lowering that mixed the classes — a scalar
    /// `LoadNull` into a string CASE, a string register into `bool_and` — is
    /// rejected here. `resolve_filter` then refuses a string result register,
    /// which is what confirms the expression really produced a string.
    fn assert_str_program(expr: &BoundExpr, schema: &Schema) {
        let p = compile_bound_expr_to_program(expr, &schema.columns).expect("lowers");
        let logical = to_logical(p).expect("the engine accepts the program");
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
                               // The fold's head operand: the first quad's operand word.
        let head_operand = |args: Vec<BoundExpr>| -> u32 {
            let mut eb = ExprBuilder::new();
            let (reg, _) = OpcodeBackend {
                cols: &s.columns,
                eb: &mut eb,
            }
            .lower(&min_max(true, args))
            .expect("lowers");
            let prog = eb.build(reg);
            let first = prog.code.chunks_exact(4).next().expect("non-empty");
            assert_eq!(first[0], ExprOp::LoadColInt.as_wire(), "the fold opens with a load");
            first[2]
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
    /// plan-time `Unsupported` by the client-side `to_logical`.
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
        match to_logical(eb.build(reg)).err() {
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
        let reg = eb.str_col_col(StrOp::Lt, 1, 2);
        assert_eq!(got, eb.build(reg), "a < b must stay str_col_lt_col(a, b)");
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
        assert!(eb.build(0).code.is_empty(), "a declined shape must emit nothing");

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

    /// Opcode words sit at every 4th position of the flat quad stream.
    fn opcodes(p: &ExprProgram) -> Vec<u32> {
        p.code.chunks_exact(4).map(|q| q[0]).collect()
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
        assert_eq!(prog.const_strings.len(), 1, "one const-pool entry (the packed set)");
        assert_eq!(
            prog.const_strings[0].len(),
            2 * 8,
            "duplicate 1 must collapse: 2 i64s = 16 bytes"
        );
        // Packed ascending: [1, 2].
        assert_eq!(&prog.const_strings[0][0..8], &1i64.to_le_bytes());
        assert_eq!(&prog.const_strings[0][8..16], &2i64.to_le_bytes());
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
            prog.num_regs <= 4,
            "membership is O(1) registers; got {} for a 500-element list",
            prog.num_regs
        );
        assert_eq!(
            prog.const_strings[0].len(),
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
    /// OR chain, and `ExprBuilder::push` never reuses, so the per-term register cost is
    /// what caps the list: one register per fused compare plus one per OR, over
    /// a `MAX_REGS` of 64. At 22 items that is 43 and compiles; through the
    /// register channel each term would spend three instead of one, putting the
    /// same list over the cap.
    #[test]
    fn in_list_string_list_fits_only_because_the_compare_is_fused() {
        let schema = str_schema();
        let items: Vec<BoundExpr> = (0..22).map(|i| BoundExpr::LitStr(format!("tag{i}"))).collect();
        let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
        assert_eq!(prog.num_regs, 2 * 22 - 1, "one register per fused compare, one per OR");
        assert!(
            prog.num_regs + 22 > 64,
            "the same list must not fit once each term costs a third register"
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
    /// The register cap is the part that does not depend on speed at all.
    /// `ExprBuilder::push` never reuses a register and `MAX_REGS` is 64. A string
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
        assert!(to_logical(p).unwrap().resolve_filter(&schema).is_ok());
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

    /// The pattern is a plain pool entry and the escape rides the operand word
    /// beside the source register, TRIM's shape.
    #[test]
    fn like_lowers_to_one_opcode_carrying_its_escape() {
        use gnitz_wire::unpack_operand_pair;
        let schema = str_schema();
        let prog = |escape, ci| {
            compile_bound_expr_to_program(&like_of(str_col(1), "a%", escape, ci), &schema.columns).unwrap()
        };
        // The escape is the second half of the LIKE instruction's `a1` word.
        let escape_of = |p: &ExprProgram| unpack_operand_pair(p.code[6]).1;

        let p = prog(Some(b'\\'), false);
        assert_eq!(opcodes(&p), [ExprOp::LoadColStr.as_wire(), ExprOp::StrLike.as_wire()]);
        assert_eq!(p.const_strings[0], b"a%");
        assert_eq!(escape_of(&p), b'\\' as u16);
        // ILIKE is the same shape under the case-insensitive opcode …
        assert_eq!(
            opcodes(&prog(Some(b'\\'), true)),
            [ExprOp::LoadColStr.as_wire(), ExprOp::StrIlike.as_wire()]
        );
        // … and `ESCAPE ''` rides as byte 0.
        assert_eq!(escape_of(&prog(None, false)), 0);
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
        let err =
            compile_bound_expr_to_program(&like_of(BoundExpr::ColRef(1), "a", Some(b'\\'), false), &schema.columns)
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
}
