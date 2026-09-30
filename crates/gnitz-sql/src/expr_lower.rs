//! Expression lowering: a bound `BoundExpr` → the VM's opcode program, as a
//! `LogicalProgram`, a resolved `ScalarEval`, or raw predicate bytes.
//!
//! This is the *scalar* half of lowering. `hir::lower` is the *relational* half
//! (`RelExpr` → DBSP circuit) and calls into this one for every filter, map and
//! projection expression it emits.

use crate::bind::structural::str_func_name;
use crate::codec::literal::{assign, invalid_literal, place, Compared, Placed};
use crate::error::GnitzSqlError;
use crate::ir::{
    blend_type, decimal_compute_type, operand_ty_pair, operand_tys, temporal_arith_type, BExpr, BinOp, BoundExpr,
    NumFunc, StrArg, StrFunc, TrimMode,
};
use crate::validate::check_decimal_scale;
use gnitz_core::Schema;
use gnitz_expr::{
    CalendarOp, CmpOp, ExprBuilder, FloatArithOp, FloatUnaryOp, IntArithOp, IntUnaryOp, LikePattern, LogicalInstr as L,
    LogicalProgram, Reg, ScalarEval, Sink,
};
use gnitz_wire::decimal::pow10;
use gnitz_wire::{ColType, ColumnDef, FixedInt, TypeCode};

/// Which register class a lowered node produced. `Int`, `Dec` and `Float` are
/// the scalar shapes — `Dec(s)` an integer holding a DECIMAL times `10^s`,
/// `Float` an f64 bit pattern — and `Str` is the string register class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ExprKind {
    Int,
    Dec(u8),
    Float,
    Str,
}

impl ExprKind {
    /// The class a value of type `ty` lowers to, as `infer_ty` and the
    /// lowering agree on it.
    fn of(ty: ColType) -> Self {
        match ty.tc {
            TypeCode::String => ExprKind::Str,
            TypeCode::Decimal => ExprKind::Dec(ty.scale),
            t if t.is_float() => ExprKind::Float,
            _ => ExprKind::Int,
        }
    }

    fn is_float(self) -> bool {
        self == ExprKind::Float
    }
}

/// Lowers a `BoundExpr` to `LogicalInstr`s. Every node produces
/// `(result_reg, ExprKind)`.
///
/// The single `match` in [`OpcodeBackend::lower`] is the sole walk of the enum:
/// adding a `BoundExpr` variant makes it non-exhaustive and fails compilation.
/// The arms that recurse receive their operands *unevaluated* and drive the
/// recursion themselves, which `binop` needs: it intercepts a column/literal
/// string comparison before recursing, so that shape keeps the fused opcodes
/// instead of building string registers.
///
/// Numeric operands are read through [`Self::lower_num`] / [`Self::lower_to`]
/// and string operands through [`Self::str_operand`], so each turns a
/// wrong-class operand into a SQL error instead of an engine-side
/// `RegClassMismatch`.
struct OpcodeBackend<'a> {
    cols: &'a [ColumnDef],
    eb: &'a mut ExprBuilder,
}

impl OpcodeBackend<'_> {
    fn lower(&mut self, expr: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        match expr {
            BoundExpr::ColRef(c) => self.col_ref(*c),
            BoundExpr::LitInt(v) => Ok((self.eb.emit(L::LoadConst { val: *v, unsigned: false }), ExprKind::Int)),
            BoundExpr::LitFloat { v, .. } => Ok((self.eb.const_f64(*v), ExprKind::Float)),
            BoundExpr::LitTemporal { v, .. } => {
                Ok((self.eb.emit(L::LoadConst { val: *v, unsigned: false }), ExprKind::Int))
            }
            BoundExpr::LitStr(s) => {
                let idx = self.eb.add_const_bytes(s.as_bytes());
                Ok((self.eb.emit(L::LoadConstStr { const_idx: idx }), ExprKind::Str))
            }
            BoundExpr::LitWide(lit) => match lit.to_u64() {
                Some(v) => Ok((
                    self.eb.emit(L::LoadConst { val: v as i64, unsigned: true }),
                    ExprKind::Int,
                )),
                None => Err(GnitzSqlError::Rejected(format!(
                    "integer literal {lit} does not fit a 64-bit register; it is usable only in a comparison \
                     or IN list against an integer column, an INSERT value or an UPDATE SET value"
                ))),
            },
            BoundExpr::LitNull => Ok((self.eb.emit(L::LoadNull), ExprKind::Int)),
            BoundExpr::BinOp(l, op, r) => self.binop(l, *op, r),
            BoundExpr::Not(inner) => {
                let (a, _) = self.lower_num(inner)?;
                Ok((self.eb.emit(L::BoolNot { a }), ExprKind::Int))
            }
            BoundExpr::NullTest { inner, want_null } => self.null_test(inner, *want_null),
            BoundExpr::Case { branches, else_ } => self.case(branches, else_.as_deref()),
            BoundExpr::InList { inner, items } => self.in_list(inner, items),
            BoundExpr::Func { f, arg } => self.func(*f, arg),
            BoundExpr::Calendar { op, arg } => self.calendar(*op, arg),
            BoundExpr::MinMaxN { is_max, args } => self.min_max_n(*is_max, args),
            BoundExpr::Cast { expr, to } => self.cast(expr, *to),
            BoundExpr::StrCall { f, args } => self.str_call(*f, args),
            BoundExpr::TrimCall { s, mode, set } => self.trim_call(s, *mode, set),
            BoundExpr::Like { s, pattern, ci } => self.like(s, pattern, *ci),
            BoundExpr::ConcatN { args } => self.concat_n(args),
        }
    }

    fn col_ref(&mut self, idx: usize) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let tc = self.cols[idx].ty.tc;
        let col = idx as u32;
        // Both rejections exist for their wording: the engine's validator refuses
        // a wide column on the integer load too, but names it by position only.
        if tc.is_wide_int() {
            return Err(GnitzSqlError::Rejected(format!(
                "column {:?} is {}; 128-bit columns cannot be used in expressions",
                self.cols[idx].name, self.cols[idx].ty,
            )));
        }
        Ok(match tc {
            TypeCode::Blob => {
                return Err(GnitzSqlError::Rejected(format!(
                    "column {:?} is {}; blob columns support only =, <>, <, <=, >, >= \
                     against another blob/string column or a string literal",
                    self.cols[idx].name, self.cols[idx].ty,
                )))
            }
            TypeCode::String => (self.eb.emit(L::LoadColStr { col }), ExprKind::Str),
            _ if tc.is_float() => (self.eb.emit(L::LoadColFloat { col }), ExprKind::Float),
            _ => (self.eb.emit(L::LoadColInt { col }), ExprKind::of(self.cols[idx].ty)),
        })
    }

    /// `IS [NOT] NULL`. A column reads the batch bitmap directly — no load —
    /// while every other operand is computed and its register's null lane
    /// tested.
    fn null_test(&mut self, inner: &BoundExpr, want_null: bool) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let invert = !want_null;
        let reg = match inner {
            BoundExpr::ColRef(c) => self.eb.emit(L::IsNull { col: *c as u32, invert }),
            _ => {
                let (a, _) = self.lower(inner)?;
                self.eb.emit(L::IsNullReg { a, invert })
            }
        };
        Ok((reg, ExprKind::Int))
    }

    /// Lower `expr` into a scalar register, with its scalar class.
    fn lower_num(&mut self, expr: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        match self.lower(expr)? {
            (_, ExprKind::Str) => Err(GnitzSqlError::Rejected(format!(
                "{} is a string; strings support comparison, LIKE/ILIKE, CONCAT/||, \
                 CASE/COALESCE/NULLIF, the string functions and CAST — not this",
                self.describe(expr)
            ))),
            scalar => Ok(scalar),
        }
    }

    /// A scalar register's value as an f64 register: a DECIMAL is divided back
    /// by its scale, which is exact for every power of ten an `i64` scale names.
    fn as_float(&mut self, r: Reg, kind: ExprKind) -> Reg {
        match kind {
            ExprKind::Float => r,
            ExprKind::Int | ExprKind::Dec(0) => self.eb.emit(L::IntToFloat { a: r }),
            ExprKind::Dec(s) => {
                let f = self.eb.emit(L::IntToFloat { a: r });
                let scale = self.eb.const_f64(pow10(s) as f64);
                self.eb.emit(L::FloatArith { op: FloatArithOp::Div, a: f, b: scale })
            }
            ExprKind::Str => unreachable!("a string register never reaches a numeric coercion"),
        }
    }

    /// A scalar register's value as a DECIMAL of scale `to`: widened exactly,
    /// narrowed by rounding half away from zero, and a float rounded at that
    /// scale.
    fn as_decimal(&mut self, r: Reg, kind: ExprKind, to: u8) -> Reg {
        match kind {
            ExprKind::Int => self.scale_up(r, to),
            ExprKind::Dec(s) if s <= to => self.scale_up(r, to - s),
            ExprKind::Dec(s) => self.round_div(r, s - to),
            ExprKind::Float => {
                let scale = self.eb.const_f64(pow10(to) as f64);
                let v = self.eb.emit(L::FloatArith { op: FloatArithOp::Mul, a: r, b: scale });
                let v = self.eb.emit(L::FloatUnary { op: FloatUnaryOp::Round, a: v });
                self.eb.emit(L::FloatToInt { a: v, fi: FixedInt::I64 })
            }
            ExprKind::Str => unreachable!("a string register never reaches a numeric coercion"),
        }
    }

    /// `r · 10^by`.
    fn scale_up(&mut self, r: Reg, by: u8) -> Reg {
        if by == 0 {
            return r;
        }
        let m = self.eb.emit(L::LoadConst { val: pow10(by), unsigned: false });
        self.eb.emit(L::IntArith { op: IntArithOp::Mul, a: r, b: m })
    }

    /// `r / 10^by` rounded half away from zero: `(r + sign(r)·⌊q/2⌋) / q`, the
    /// VM's division truncating toward zero.
    fn round_div(&mut self, r: Reg, by: u8) -> Reg {
        if by == 0 {
            return r;
        }
        let q = pow10(by);
        let sign = self.eb.emit(L::IntUnary { op: IntUnaryOp::Sign, a: r });
        let half = self.eb.emit(L::LoadConst { val: q / 2, unsigned: false });
        let bias = self.eb.emit(L::IntArith { op: IntArithOp::Mul, a: sign, b: half });
        let v = self.eb.emit(L::IntArith { op: IntArithOp::Add, a: r, b: bias });
        let q = self.eb.emit(L::LoadConst { val: q, unsigned: false });
        self.eb.emit(L::IntArith { op: IntArithOp::Div, a: v, b: q })
    }

    /// `⌊r / 10^by⌋` (`is_ceil` false) or `⌈r / 10^by⌉`: the truncating
    /// quotient, moved one off it where a remainder's sign says truncation went
    /// the other way.
    fn floor_ceil_div(&mut self, r: Reg, by: u8, is_ceil: bool) -> Reg {
        let q = self.eb.emit(L::LoadConst { val: pow10(by), unsigned: false });
        let t = self.eb.emit(L::IntArith { op: IntArithOp::Div, a: r, b: q });
        let m = self.eb.emit(L::IntArith { op: IntArithOp::Mod, a: r, b: q });
        let zero = self.eb.emit(L::LoadConst { val: 0, unsigned: false });
        let (cmp, fix) = if is_ceil {
            (CmpOp::Gt, IntArithOp::Add)
        } else {
            (CmpOp::Lt, IntArithOp::Sub)
        };
        let off = self.eb.emit(L::Cmp { op: cmp, a: m, b: zero });
        self.eb.emit(L::IntArith { op: fix, a: t, b: off })
    }

    /// Lower `e` as a DECIMAL of scale `to`: a literal as the constant an
    /// assignment of it stores, anything else computed and re-expressed.
    fn lower_dec(&mut self, e: &BoundExpr, to: u8) -> Result<Reg, GnitzSqlError> {
        check_decimal_scale(to)?;
        if matches!(e, BoundExpr::LitStr(_)) || e.decimal_literal().is_some() {
            let v = assign(e, ColType::decimal(to)).map_err(GnitzSqlError::Rejected)?;
            return Ok(self.eb.emit(L::LoadConst {
                val: FixedInt::I64.unpack(v),
                unsigned: false,
            }));
        }
        let (mut r, kind) = self.lower_num(e)?;
        // A DECIMAL is an i64, so a U64 value at or above 2^63 has none: the
        // range cast makes it NULL, and the scaling below then runs signed.
        if kind == ExprKind::Int && e.infer_ty(self.cols).register_image().tc == TypeCode::U64 {
            r = self.eb.emit(L::IntCast { a: r, fi: FixedInt::I64 });
        }
        Ok(self.as_decimal(r, kind, to))
    }

    /// Lower `e` into the register class of `target` — the blend a CASE, a
    /// GREATEST or an IN list settled on — so every branch lands in one class.
    fn lower_to(&mut self, e: &BoundExpr, target: ColType) -> Result<Reg, GnitzSqlError> {
        let src = e.infer_ty(self.cols).tc;
        if src.is_temporal() && src != target.tc {
            if (src, target.tc) != (TypeCode::Date, TypeCode::Timestamp) {
                return Err(GnitzSqlError::Rejected(format!(
                    "cannot mix {src} with {target} in one CASE/COALESCE/GREATEST/LEAST"
                )));
            }
            let (a, _) = self.lower_num(e)?;
            return Ok(self.eb.emit(L::Calendar {
                op: CalendarOp::ToMicros,
                a,
                micros: false,
            }));
        }
        match ExprKind::of(target) {
            ExprKind::Str => self.str_operand(e, false),
            ExprKind::Dec(s) => self.lower_dec(e, s),
            ExprKind::Float => {
                let (r, kind) = self.lower_num(e)?;
                Ok(self.as_float(r, kind))
            }
            ExprKind::Int => Ok(self.lower_num(e)?.0),
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

    /// The string channel's operand read. Lowering is eager, so a string
    /// context must intercept `LitNull` *before* recursing into it and emit the
    /// string-class NULL rather than the scalar one. `coerce_numeric` is
    /// CONCAT's implicit numeric→text cast, and nothing else's.
    fn str_operand(&mut self, expr: &BoundExpr, coerce_numeric: bool) -> Result<Reg, GnitzSqlError> {
        if matches!(expr, BoundExpr::LitNull) {
            return Ok(self.eb.emit(L::LoadNullStr));
        }
        match self.lower(expr)? {
            (r, ExprKind::Str) => Ok(r),
            (r, ExprKind::Int) if coerce_numeric => Ok(self.eb.emit(L::IntToStr { a: r })),
            (r, kind @ (ExprKind::Float | ExprKind::Dec(_))) if coerce_numeric => {
                let f = self.as_float(r, kind);
                Ok(self.eb.emit(L::FloatToStr { a: f }))
            }
            _ => Err(GnitzSqlError::Rejected("expected a string value here".to_string())),
        }
    }

    /// An integer operand — a window bound, a count, a field index. A float is a
    /// typed error rather than a silent truncation: the opcode reads the
    /// register as an integer. `what` names the position the error reports.
    fn int_operand(&mut self, e: &BoundExpr, what: impl FnOnce() -> String) -> Result<Reg, GnitzSqlError> {
        match self.lower_num(e)? {
            (r, ExprKind::Int) => Ok(r),
            _ => Err(GnitzSqlError::Rejected(format!(
                "{} must be an integer expression",
                what()
            ))),
        }
    }

    /// The numeric functions. Over an integer register the
    /// rounding family and `ROUND(x, n >= 0)` are the identity, ABS of an
    /// unsigned register likewise, and only NEG, signed ABS and SIGN compute;
    /// over a DECIMAL the rounding family is integer arithmetic on the scale;
    /// everything else lifts to f64. `NumFunc::result_type` states that split
    /// and this reads it, so the declared column and the register cannot
    /// disagree.
    fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        use FloatUnaryOp as F;
        let (r, kind) = self.lower_num(arg)?;
        let arg_ty = arg.infer_ty(self.cols);
        let out = f.result_type(arg_ty);
        if out.tc == TypeCode::F64 {
            let a = self.as_float(r, kind);
            let reg = match f {
                NumFunc::Unary(op) => self.eb.emit(L::FloatUnary { op, a }),
                NumFunc::Round(n) => self.scaled_round(a, n),
            };
            return Ok((reg, ExprKind::Float));
        }
        if let ExprKind::Dec(s) = kind {
            let by = s - out.scale;
            let reg = match f {
                NumFunc::Unary(F::Neg) => self.eb.emit(L::IntUnary { op: IntUnaryOp::Neg, a: r }),
                NumFunc::Unary(F::Abs) => self.eb.emit(L::IntUnary { op: IntUnaryOp::Abs, a: r }),
                NumFunc::Unary(F::Sign) => {
                    return Ok((self.eb.emit(L::IntUnary { op: IntUnaryOp::Sign, a: r }), ExprKind::Int))
                }
                _ if by == 0 => r,
                NumFunc::Unary(F::Round) | NumFunc::Round(_) => self.round_div(r, by),
                NumFunc::Unary(F::Floor) => self.floor_ceil_div(r, by, false),
                NumFunc::Unary(F::Ceil) => self.floor_ceil_div(r, by, true),
                NumFunc::Unary(F::Trunc) => {
                    let q = self.eb.emit(L::LoadConst { val: pow10(by), unsigned: false });
                    self.eb.emit(L::IntArith { op: IntArithOp::Div, a: r, b: q })
                }
                NumFunc::Unary(F::Sqrt | F::Ln | F::Log10 | F::Exp) => unreachable!("typed as F64 above"),
            };
            return Ok((reg, ExprKind::Dec(out.scale)));
        }
        let int_op = match f {
            NumFunc::Unary(F::Neg) => Some(IntUnaryOp::Neg),
            NumFunc::Unary(F::Abs) if arg_ty.tc.is_signed_int() => Some(IntUnaryOp::Abs),
            NumFunc::Unary(F::Sign) => Some(IntUnaryOp::Sign),
            _ => None,
        };
        let reg = int_op.map_or(r, |op| self.eb.emit(L::IntUnary { op, a: r }));
        Ok((reg, ExprKind::Int))
    }

    /// `ROUND(x, n)` over an f64 register: scale by `10^|n|`, round, scale
    /// back. The positive power is loaded in both directions — multiply first
    /// for `n > 0`, divide first for `n < 0` — because negative powers of ten
    /// are not f64-exact.
    fn scaled_round(&mut self, a: Reg, n: i8) -> Reg {
        let scale = self.eb.const_f64(10f64.powi(n.unsigned_abs() as i32));
        let (first, undo) = if n > 0 {
            (FloatArithOp::Mul, FloatArithOp::Div)
        } else {
            (FloatArithOp::Div, FloatArithOp::Mul)
        };
        let v = self.eb.emit(L::FloatArith { op: first, a, b: scale });
        let v = self.eb.emit(L::FloatUnary { op: FloatUnaryOp::Round, a: v });
        self.eb.emit(L::FloatArith { op: undo, a: v, b: scale })
    }

    fn calendar(&mut self, op: CalendarOp, arg: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let micros = match arg.infer_ty(self.cols).tc {
            TypeCode::Timestamp => true,
            TypeCode::Date => false,
            t => {
                return Err(GnitzSqlError::Rejected(format!(
                    "a calendar function takes a DATE or TIMESTAMP; {} is {t}",
                    self.describe(arg)
                )))
            }
        };
        let (r, _) = self.lower_num(arg)?;
        Ok((self.eb.emit(L::Calendar { op, a: r, micros }), ExprKind::Int))
    }

    fn str_call(&mut self, f: StrFunc, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let mut regs = Vec::with_capacity(args.len());
        for (k, (kind, a)) in f.signature().iter().zip(args).enumerate() {
            regs.push(match kind {
                StrArg::Str | StrArg::StrOr(_) => self.str_operand(a, false)?,
                StrArg::Int | StrArg::IntOpt => {
                    self.int_operand(a, || format!("{}: argument {}", str_func_name(f), k + 1))?
                }
            });
        }
        let instr = match (f, regs.as_slice()) {
            (StrFunc::Upper, &[a]) => L::StrCase { a, upper: true },
            (StrFunc::Lower, &[a]) => L::StrCase { a, upper: false },
            (StrFunc::LenChars, &[a]) => L::StrLen { a, chars: true },
            (StrFunc::LenBytes, &[a]) => L::StrLen { a, chars: false },
            (StrFunc::Reverse, &[a]) => L::StrReverse { a },
            (StrFunc::Left, &[src, n_reg]) => L::StrSide { src, n_reg, left: true },
            (StrFunc::Right, &[src, n_reg]) => L::StrSide { src, n_reg, left: false },
            (StrFunc::Pos, &[hay, needle]) => L::StrPos { hay, needle },
            (StrFunc::Replace, &[s, from, to]) => L::StrReplace { s, from, to },
            (StrFunc::Lpad, &[s, n_reg, fill]) => L::StrPad { s, n_reg, fill, left: true },
            (StrFunc::Rpad, &[s, n_reg, fill]) => L::StrPad { s, n_reg, fill, left: false },
            (StrFunc::SplitPart, &[s, delim, n_reg]) => L::StrSplitPart { s, delim, n_reg },
            (StrFunc::Substr, &[src, start_reg]) => L::StrSubstr { src, start_reg, len_reg: None },
            (StrFunc::Substr, &[src, start_reg, len]) => L::StrSubstr { src, start_reg, len_reg: Some(len) },
            _ => unreachable!("the binder sizes a string call's arguments by its signature"),
        };
        Ok((self.eb.emit(instr), ExprKind::of(ColType::of(f.result_type()))))
    }

    fn trim_call(&mut self, s: &BoundExpr, mode: TrimMode, set: &str) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let set_idx = self.eb.add_const_bytes(set.as_bytes());
        Ok((self.eb.emit(L::StrTrim { a: src, mode, set_idx }), ExprKind::Str))
    }

    /// `s [I]LIKE 'pattern'`. The result is a boolean, so `NOT LIKE` reads it
    /// like any other.
    fn like(&mut self, s: &BoundExpr, pattern: &LikePattern, ci: bool) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let pat_idx = self.eb.add_const_bytes(pattern.as_bytes());
        Ok((self.eb.emit(L::StrLike { src, pat_idx, ci }), ExprKind::Int))
    }

    /// `CONCAT(args…)`, a strictly left fold seeded with the empty string, so
    /// every arity has one shape and the one-argument case is non-NULL. Every
    /// argument lands in the `b` operand, where a NULL contributes the empty
    /// string; only the accumulator's own NULL (from `str_concat_nn`)
    /// propagates.
    fn concat_n(&mut self, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let empty = self.eb.add_const_bytes(b"");
        let mut acc = self.eb.emit(L::LoadConstStr { const_idx: empty });
        for a in args {
            let r = self.str_operand(a, true)?;
            acc = self.eb.emit(L::StrConcat { a: acc, b: r, skip_null: true });
        }
        Ok((acc, ExprKind::Str))
    }

    /// GREATEST/LEAST as a left fold of 2-ary extremum opcodes.
    fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let types = operand_tys(&args.iter().collect::<Vec<_>>(), &|i: &usize| self.cols[*i].ty);
        let ty = blend_type(&types);
        // Every argument is a numeric operand: a string one is refused by the
        // numeric read, which names the column where the blend cannot.
        if ty.tc == TypeCode::String {
            for (a, t) in args.iter().zip(&types) {
                if t.tc == TypeCode::String {
                    self.lower_num(a)?;
                }
            }
        }
        // The engine taints a register unsigned only from the first U64 operand
        // onward, so a U64 argument must head the fold — otherwise an earlier
        // signed pair would compare signed and the answer would depend on the
        // order the arguments were written in.
        let head = types
            .iter()
            .position(|t| t.register_image().tc == TypeCode::U64)
            .unwrap_or(0);
        let mut acc = self.lower_to(&args[head], ty)?;
        for (i, a) in args.iter().enumerate() {
            if i == head {
                continue;
            }
            let b = self.lower_to(a, ty)?;
            acc = if ty.tc.is_float() {
                self.eb.emit(L::FloatMinMax2 { a: acc, b, is_max })
            } else {
                self.eb.emit(L::IntMinMax2 { a: acc, b, is_max })
            };
        }
        Ok((acc, ExprKind::of(ty)))
    }

    /// CAST, dispatched on the target class, then the source kind.
    fn cast(&mut self, expr: &BoundExpr, to: ColType) -> Result<(Reg, ExprKind), GnitzSqlError> {
        if to.is_decimal() {
            return Ok((self.lower_dec(expr, to.scale)?, ExprKind::Dec(to.scale)));
        }
        let to = to.tc;
        let (r, kind) = self.lower(expr)?;
        if to.is_temporal() {
            let from = expr.infer_ty(self.cols).tc;
            match (kind, from, to) {
                (ExprKind::Str, _, _) => {
                    return Err(GnitzSqlError::Rejected(format!(
                        "CAST of a string to {to} is supported for a literal only"
                    )))
                }
                (ExprKind::Int, TypeCode::Timestamp, TypeCode::Date)
                | (ExprKind::Int, TypeCode::Date, TypeCode::Timestamp) => {
                    let micros = from == TypeCode::Timestamp;
                    let op = if micros {
                        CalendarOp::ToDays
                    } else {
                        CalendarOp::ToMicros
                    };
                    return Ok((self.eb.emit(L::Calendar { op, a: r, micros }), ExprKind::Int));
                }
                // An integer is already the storage value: the fixed-int path
                // below range-checks it.
                _ => {}
            }
        }
        if to == TypeCode::String {
            let reg = match kind {
                ExprKind::Str => r,
                ExprKind::Int => self.eb.emit(L::IntToStr { a: r }),
                ExprKind::Float | ExprKind::Dec(_) => {
                    let f = self.as_float(r, kind);
                    self.eb.emit(L::FloatToStr { a: f })
                }
            };
            return Ok((reg, ExprKind::Str));
        }
        if to.is_float() {
            let f = match kind {
                ExprKind::Str => self.eb.emit(L::StrToFloat { a: r }),
                scalar => self.as_float(r, scalar),
            };
            return Ok((self.cast_float(f, to), ExprKind::Float));
        }
        let Some(fi) = FixedInt::from_type_code(to) else {
            return Err(GnitzSqlError::Rejected(format!("CAST to {to} is not supported")));
        };
        let reg = match kind {
            ExprKind::Str => self.eb.emit(L::StrToInt { a: r, fi }),
            ExprKind::Float => self.eb.emit(L::FloatToInt { a: r, fi }),
            // Rounded to a whole number, then range-checked into the target
            // like any other i64.
            ExprKind::Dec(s) => {
                let whole = self.round_div(r, s);
                if fi == FixedInt::I64 {
                    whole
                } else {
                    self.eb.emit(L::IntCast { a: whole, fi })
                }
            }
            ExprKind::Int => {
                // Elide iff the register provably already holds a value inside
                // `to`'s domain *with* `to`'s register image: the engine tracks a
                // register as U64 iff its type is U64, so an elided cast must not
                // change that bit. Only a bare column load qualifies for the
                // exact-type half.
                let in_range = |v: i64| {
                    let (lo, hi) = fi.range();
                    (lo..=hi).contains(&(v as i128))
                };
                let elide = to == expr.infer_ty(self.cols).tc.register_image()
                    || matches!(expr, BoundExpr::ColRef(i) if self.cols[*i].ty.tc == to)
                    || matches!(expr, BoundExpr::LitInt(v) if in_range(*v) && to != TypeCode::U64);
                if elide {
                    r
                } else {
                    self.eb.emit(L::IntCast { a: r, fi })
                }
            }
        };
        Ok((reg, ExprKind::Int))
    }

    /// An f64 register as the float type `to`: F64 needs nothing more.
    fn cast_float(&mut self, f: Reg, to: TypeCode) -> Reg {
        if to == TypeCode::F32 {
            self.eb.emit(L::FloatToF32 { a: f })
        } else {
            f
        }
    }

    /// Searched CASE as a right-to-left fold of selects, so the first truthy
    /// WHEN wins. The result class is decided from the branch types before any
    /// result is lowered — lowering is eager, and a `LitNull` branch must be
    /// emitted in the class of its siblings. `Select` blends raw bit patterns,
    /// so in a float CASE every integer branch is lifted; a string CASE reads
    /// its branches through the string channel, where a non-NULL scalar branch
    /// is a typed error.
    fn case(
        &mut self,
        branches: &[(BoundExpr, BoundExpr)],
        else_: Option<&BoundExpr>,
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let results: Vec<&BoundExpr> = branches.iter().map(|(_, r)| r).chain(else_).collect();
        let case_ty = blend_type(&operand_tys(&results, &|idx: &usize| self.cols[*idx].ty));
        let is_str = case_ty.tc == TypeCode::String;
        let mut conds = Vec::with_capacity(branches.len());
        let mut results = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            conds.push(self.lower_num(cond)?.0);
            results.push(self.lower_to(result, case_ty)?);
        }
        let mut acc = match else_ {
            Some(e) => self.lower_to(e, case_ty)?,
            None if is_str => self.eb.emit(L::LoadNullStr),
            None => self.eb.emit(L::LoadNull),
        };
        for (cond, a) in conds.into_iter().zip(results).rev() {
            acc = if is_str {
                self.eb.emit(L::StrSelect { cond, a, b: acc })
            } else {
                self.eb.emit(L::Select { cond, a, b: acc })
            };
        }
        Ok((acc, ExprKind::of(case_ty)))
    }

    /// `inner IN (items…)`. An operand stored as a ≤8-byte integer whose items
    /// all place among its values is one `IntInSet` of the items it holds;
    /// anything else is the `inner = item` OR chain. `self.cols` is the schema
    /// `inner` was bound against, so the gate holds for a HAVING over the reduce
    /// output as much as for a table filter.
    fn in_list(&mut self, inner: &BoundExpr, items: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let ty = inner.infer_ty(self.cols);
        if let Some(fi) = FixedInt::from_type_code(ty.tc) {
            let placed = items
                .iter()
                .map(|i| place(i, ty))
                .collect::<Option<Vec<Placed>>>()
                .filter(|ps| self.range_is_register(inner, fi) || ps.iter().all(|p| !p.is_outside()));
            if let Some(placed) = placed {
                let values: Vec<i64> = placed
                    .iter()
                    .filter_map(|p| match p {
                        Placed::At(v) => Some(fi.unpack(*v)),
                        _ => None,
                    })
                    .collect();
                let c = self.lower_int_operand(inner, ty)?;
                if values.is_empty() {
                    return Ok((self.eb.emit(L::Cmp { op: CmpOp::Ne, a: c, b: c }), ExprKind::Int));
                }
                let set_idx = self.eb.add_const_int_set(values);
                return Ok((self.eb.emit(L::IntInSet { value_reg: c, set_idx }), ExprKind::Int));
            }
        }
        // Each term lowers the operand again; the builder folds the identical
        // instructions, so a non-fused operand is loaded once for the whole list.
        let (mut acc, _) = self.binop(inner, BinOp::Eq, &items[0])?;
        for it in &items[1..] {
            let (r, _) = self.binop(inner, BinOp::Eq, it)?;
            acc = self.eb.emit(L::BoolBinary { a: acc, b: r, is_or: true });
        }
        Ok((acc, ExprKind::Int))
    }

    /// Whether `e`'s register holds only values of `fi`: a column or a CAST is
    /// range-checked, and an 8-byte type spans the register. A computed DATE
    /// (`d + 1`) does not.
    fn range_is_register(&self, e: &BoundExpr, fi: FixedInt) -> bool {
        matches!(e, BoundExpr::ColRef(_) | BoundExpr::Cast { .. }) || fi.width() == 8
    }

    /// `e`, of a type stored as an integer, as the register its values compare
    /// in: a DECIMAL at its own scale.
    fn lower_int_operand(&mut self, e: &BoundExpr, ty: ColType) -> Result<Reg, GnitzSqlError> {
        match ty.is_decimal() {
            true => self.lower_dec(e, ty.scale),
            false => Ok(self.lower_num(e)?.0),
        }
    }

    /// `other CMP lit` where `other` is stored as a ≤8-byte integer: the literal
    /// is placed among `other`'s values, the rule seek keys and written cells
    /// take. `None` leaves the comparison to the general path.
    fn cmp_placed(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<Option<Reg>, GnitzSqlError> {
        // Either side may be the literal. When both are, the operand is the side
        // with an integer-stored type (`'2020-01-01' = DATE '2020-01-01'`).
        let typed = |other: &BoundExpr, lit: &BoundExpr| -> Option<(ColType, FixedInt)> {
            if !lit.is_literal() || matches!(other, BExpr::LitStr(_)) {
                return None;
            }
            let ty = other.infer_ty(self.cols);
            Some((ty, FixedInt::from_type_code(ty.tc)?))
        };
        let (other, lit, op, (ty, fi)) = if let Some(t) = typed(left, right) {
            (left, right, op, t)
        } else if let Some(t) = typed(right, left) {
            (right, left, op.converse(), t)
        } else {
            return Ok(None);
        };
        let Some(cmp) = op.as_cmp() else { return Ok(None) };
        let Some(p) = place(lit, ty) else {
            // A string `place` cannot read spells no value of the type.
            return match lit {
                BExpr::LitStr(s) => Err(GnitzSqlError::Rejected(invalid_literal(ty, s))),
                _ => Ok(None),
            };
        };
        if p.is_outside() && !self.range_is_register(other, fi) {
            return Ok(None);
        }
        let c = self.lower_int_operand(other, ty)?;
        let (op, k) = match p.compare(cmp) {
            Compared::Cmp(op, v) => (op, Some(fi.unpack(v))),
            // `c = c` / `c <> c`: true / false on every row, NULL on a NULL one.
            Compared::Always(holds) => (if holds { CmpOp::Eq } else { CmpOp::Ne }, None),
        };
        let b = match k {
            Some(val) => self.eb.emit(L::LoadConst { val, unsigned: fi == FixedInt::U64 }),
            None => c,
        };
        Ok(Some(self.eb.emit(L::Cmp { op, a: c, b })))
    }

    /// A comparison of two German-string columns, or of one and a string literal,
    /// as a `StrCol*` opcode over the cells themselves — the one legal use of a
    /// BLOB column. `None` for any other shape or operator, which the register
    /// channel lowers instead.
    fn string_cmp(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Option<Reg> {
        let cols = self.cols;
        let is_str_col = |e: &BoundExpr| matches!(e, BoundExpr::ColRef(i) if cols[*i].ty.tc.is_german_string());
        // The opcode pins the constant on the right, so a literal-on-left pair is
        // read as its converse: `'A' < col` is `col > 'A'`.
        let (col, s, cmp) = match (left, right) {
            (BoundExpr::ColRef(c), BoundExpr::LitStr(s)) if is_str_col(left) => (*c, s, op.as_cmp()?),
            (BoundExpr::LitStr(s), BoundExpr::ColRef(c)) if is_str_col(right) => (*c, s, op.converse().as_cmp()?),
            (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) if is_str_col(left) && is_str_col(right) => {
                return Some(self.eb.emit(L::StrColCol {
                    op: op.as_cmp()?,
                    col_a: *a as u32,
                    col_b: *b as u32,
                }));
            }
            _ => return None,
        };
        let const_idx = self.eb.add_const_bytes(s.as_bytes());
        Some(self.eb.emit(L::StrColConst { op: cmp, col: col as u32, const_idx }))
    }

    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        if let BinOp::And | BinOp::Or = op {
            let (a, _) = self.lower_num(left)?;
            let (b, _) = self.lower_num(right)?;
            return Ok((
                self.eb.emit(L::BoolBinary { a, b, is_or: op == BinOp::Or }),
                ExprKind::Int,
            ));
        }
        if let Some(reg) = self.cmp_placed(left, op, right)? {
            return Ok((reg, ExprKind::Int));
        }
        if let Some(reg) = self.string_cmp(left, op, right) {
            return Ok((reg, ExprKind::Int));
        }
        // `||` reads both operands through the string channel, so `s || NULL` is
        // a NULL string rather than a type error. No implicit numeric cast,
        // unlike CONCAT.
        if op == BinOp::Concat {
            let a = self.str_operand(left, false)?;
            let b = self.str_operand(right, false)?;
            return Ok((self.eb.emit(L::StrConcat { a, b, skip_null: false }), ExprKind::Str));
        }
        let (lt, rt) = operand_ty_pair(left, right, &|i: &usize| self.cols[*i].ty);
        if lt.tc.is_temporal() || rt.tc.is_temporal() {
            if op.as_cmp().is_none() && temporal_arith_type(op, lt, rt).is_none() {
                return Err(GnitzSqlError::Rejected(format!(
                    "operator {op:?} is not supported on a DATE/TIMESTAMP operand"
                )));
            }
            // Days meet microseconds at TIMESTAMP.
            if lt.tc.is_temporal() && rt.tc.is_temporal() && lt.tc != rt.tc {
                let ts = ColType::of(TypeCode::Timestamp);
                let l = self.lower_to(left, ts)?;
                let r = self.lower_to(right, ts)?;
                return self.emit_scalar_binop(l, op, r, ExprKind::Int);
            }
        }
        if lt.is_decimal() || rt.is_decimal() {
            return self.decimal_binop(left, op, right, lt, rt);
        }
        let (mut l, l_kind) = self.lower(left)?;
        let (mut r, r_kind) = self.lower(right)?;
        if l_kind == ExprKind::Str || r_kind == ExprKind::Str {
            return self.str_binop(op, (l, l_kind), (r, r_kind));
        }
        let (l_float, r_float) = (l_kind.is_float(), r_kind.is_float());
        // POWER has no integer form: both operands lift.
        let is_float = l_float || r_float || op == BinOp::Pow;
        if is_float {
            l = self.as_float(l, l_kind);
            r = self.as_float(r, r_kind);
        }
        let out = match is_float {
            true => ExprKind::Float,
            false => ExprKind::Int,
        };
        self.emit_scalar_binop(l, op, r, out)
    }

    /// A comparison or arithmetic with a DECIMAL operand: both sides are brought
    /// to the scale [`decimal_compute_type`] names, so `d = 1.005` compares
    /// exactly rather than after rounding the literal.
    fn decimal_binop(
        &mut self,
        left: &BoundExpr,
        op: BinOp,
        right: &BoundExpr,
        lt: ColType,
        rt: ColType,
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let out = decimal_compute_type(op, lt, rt);
        if out.tc == TypeCode::String {
            return Err(GnitzSqlError::Rejected(format!(
                "operator {op:?} is not supported between a DECIMAL and a string"
            )));
        }
        if out.tc == TypeCode::F64 {
            let f64 = ColType::of(TypeCode::F64);
            let l = self.lower_to(left, f64)?;
            let r = self.lower_to(right, f64)?;
            return self.emit_scalar_binop(l, op, r, ExprKind::Float);
        }
        // A product's operands keep their own scales — the multiply adds them.
        let (ls, rs) = match op {
            BinOp::Mul => (lt.scale, rt.scale),
            _ => (out.scale, out.scale),
        };
        check_decimal_scale(out.scale)?;
        let l = self.lower_dec(left, ls)?;
        let r = self.lower_dec(right, rs)?;
        self.emit_scalar_binop(l, op, r, ExprKind::Dec(out.scale))
    }

    /// The comparison-or-arithmetic tail both binop paths end in: `out` is the
    /// class the arithmetic result carries, and picks the float or integer
    /// opcode; a comparison is `Int` whichever class its operands are.
    fn emit_scalar_binop(
        &mut self,
        l: Reg,
        op: BinOp,
        r: Reg,
        out: ExprKind,
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let is_float = out.is_float();
        if let Some(cmp) = op.as_cmp() {
            let reg = match is_float {
                true => self.eb.emit(L::FCmp { op: cmp, a: l, b: r }),
                false => self.eb.emit(L::Cmp { op: cmp, a: l, b: r }),
            };
            return Ok((reg, ExprKind::Int));
        }
        if is_float {
            let op = op
                .as_float_arith()
                .ok_or_else(|| GnitzSqlError::Rejected("float modulo not supported".to_string()))?;
            return Ok((self.eb.emit(L::FloatArith { op, a: l, b: r }), out));
        }
        let op = op
            .as_int_arith()
            .expect("every scalar operator that is not a comparison has an integer form");
        Ok((self.eb.emit(L::IntArith { op, a: l, b: r }), out))
    }

    /// A binary operator with at least one string operand in a register. Only
    /// the six comparisons are defined, and both sides must be strings — a
    /// comparison carries no implicit cast. The operator is reported before the
    /// operand kinds, so `strcol + 1` reads as "no such operator on strings"
    /// rather than as a demand that its right-hand side become one.
    fn str_binop(
        &mut self,
        op: BinOp,
        (a, a_kind): (Reg, ExprKind),
        (b, b_kind): (Reg, ExprKind),
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let Some(cmp) = op.as_cmp() else {
            return Err(GnitzSqlError::Rejected(format!(
                "operator {op:?} is not supported on a string operand"
            )));
        };
        if a_kind != ExprKind::Str || b_kind != ExprKind::Str {
            return Err(GnitzSqlError::Rejected(format!(
                "comparison {op:?} against a string needs both operands to be strings"
            )));
        }
        Ok((self.eb.emit(L::StrCmp { op: cmp, a, b }), ExprKind::Int))
    }
}

/// Compile a BoundExpr into `eb`, returning the result register — the form for
/// a caller threading several expressions into one program.
pub(crate) fn compile_bound_expr(
    expr: &BoundExpr,
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Result<Reg, GnitzSqlError> {
    OpcodeBackend { cols, eb }.lower(expr).map(|(reg, _)| reg)
}

/// Compile a standalone BoundExpr into a finished `LogicalProgram`.
pub(crate) fn compile_bound_expr_to_program(
    expr: &BoundExpr,
    cols: &[ColumnDef],
) -> Result<LogicalProgram, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let reg = compile_bound_expr(expr, cols, &mut eb)?;
    Ok(eb.build(vec![Sink::Reg(reg)])?)
}

/// An expression's truth when its `NOT` / `AND` / `OR` connectives settle it over
/// integer literals: exact wherever it is read, since `true OR NULL` is true and
/// `false AND NULL` is false.
fn constant_truth(e: &BoundExpr) -> Option<bool> {
    let settle = |l: &BoundExpr, r: &BoundExpr, absorbing: bool| match (constant_truth(l), constant_truth(r)) {
        (Some(t), _) | (_, Some(t)) if t == absorbing => Some(absorbing),
        (Some(_), Some(_)) => Some(!absorbing),
        _ => None,
    };
    match e {
        BoundExpr::LitInt(v) => Some(*v != 0),
        BoundExpr::Not(inner) => constant_truth(inner).map(|t| !t),
        BoundExpr::BinOp(l, BinOp::Or, r) => settle(l, r, true),
        BoundExpr::BinOp(l, BinOp::And, r) => settle(l, r, false),
        _ => None,
    }
}

/// The AND of `conjuncts` as a filter program, less its constant-true
/// conjuncts; `None` when none is left.
pub(crate) fn compile_filter_program<'a>(
    conjuncts: impl IntoIterator<Item = &'a BoundExpr>,
    cols: &[ColumnDef],
) -> Result<Option<LogicalProgram>, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let mut backend = OpcodeBackend { cols, eb: &mut eb };
    let mut acc: Option<Reg> = None;
    for e in conjuncts {
        if constant_truth(e) == Some(true) {
            continue;
        }
        let (r, _) = backend.lower_num(e)?;
        acc = Some(match acc {
            Some(a) => backend.eb.emit(L::BoolBinary { a, b: r, is_or: false }),
            None => r,
        });
    }
    Ok(acc.map(|reg| eb.build(vec![Sink::Reg(reg)])).transpose()?)
}

/// The wire predicate blob for the AND of `conjuncts`; empty when statically
/// true.
pub(crate) fn compile_wire_conjuncts<'a>(
    conjuncts: impl IntoIterator<Item = &'a BoundExpr>,
    cols: &[ColumnDef],
) -> Result<Vec<u8>, GnitzSqlError> {
    Ok(compile_filter_program(conjuncts, cols)?
        .map(|p| p.to_blob_bytes())
        .unwrap_or_default())
}

/// A scalar (non-predicate) expression as a resolved evaluator.
pub(crate) fn compile_scalar_evaluator(expr: &BoundExpr, schema: &Schema) -> Result<ScalarEval, GnitzSqlError> {
    Ok(compile_bound_expr_to_program(expr, &schema.columns)?.resolve_scalar(schema)?)
}

#[cfg(test)]
#[path = "tests/expr_lower.rs"]
mod tests;
