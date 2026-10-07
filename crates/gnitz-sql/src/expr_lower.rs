//! Expression lowering: a bound `BoundExpr` → the VM's opcode program, as a
//! `LogicalProgram`, a resolved `ScalarEval`, or raw predicate bytes.
//!
//! This is the *scalar* half of lowering. `hir::lower` is the *relational* half
//! (`RelExpr` → DBSP circuit) and calls into this one for every filter, map and
//! projection expression it emits.

use crate::codec::literal::{assign, float_value, invalid_literal, place, Compared, Placed};
use crate::error::GnitzSqlError;
use crate::ir::{
    bin_types, blend_type, check_decimal_scale, operand_ty_pair, operand_tys, range_step, reads_unsigned, BExpr, BinOp,
    BoundExpr, NumFunc, RangeStep, RegClass, StrArg, StrFunc, BOOL, STRING,
};
use gnitz_core::Schema;
use gnitz_expr::{
    CalendarOp, CmpOp, ExprBuilder, FloatArithOp, FloatUnaryOp, IntArithOp, IntUnaryOp, LogicalInstr as L,
    LogicalProgram, Reg, ScalarEval, Sink,
};
use gnitz_wire::decimal::pow10;
use gnitz_wire::{ColType, ColumnDef, FixedInt, TypeCode};

/// Lowers a `BoundExpr` to `LogicalInstr`s, each node into the register
/// [`Lowering::lower`] returns. That register's class is the [`RegClass`] of
/// the node's inferred type.
///
/// An operand is read through the position it stands in: [`Self::lower_to`]
/// brings it to the type its node combines it at, [`Self::lower_bool`] reads a
/// condition, and each turns a wrong-class operand into a SQL error instead of
/// an engine-side `RegClassMismatch`.
struct Lowering<'a> {
    cols: &'a [ColumnDef],
    eb: &'a mut ExprBuilder,
}

impl Lowering<'_> {
    fn lower(&mut self, expr: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        Ok(match expr {
            BoundExpr::ColRef(c) => self.col_ref(*c)?,
            BoundExpr::LitInt(v) | BoundExpr::LitTemporal { v, .. } => {
                self.eb.emit(L::LoadConst { val: *v, unsigned: false })
            }
            BoundExpr::LitBool(b) => self.eb.emit(L::LoadConst { val: i64::from(*b), unsigned: false }),
            BoundExpr::LitFloat { v, .. } => self.eb.const_f64(*v),
            BoundExpr::LitStr(s) => {
                let const_idx = self.eb.add_const_bytes(s.as_bytes());
                self.eb.emit(L::LoadConstStr { const_idx })
            }
            BoundExpr::LitWide(lit) => match lit.to_u64() {
                Some(v) => self.eb.emit(L::LoadConst { val: v as i64, unsigned: true }),
                None => {
                    return Err(GnitzSqlError::Rejected(format!(
                        "integer literal {lit} does not fit a 64-bit register; it is usable only in a comparison \
                         or IN list against an integer column, an INSERT value or an UPDATE SET value"
                    )))
                }
            },
            BoundExpr::LitNull => self.eb.emit(L::LoadNull),
            BoundExpr::BinOp(l, op, r) => self.binop(l, *op, r)?,
            BoundExpr::Not(inner) => {
                let a = self.lower_bool(inner)?;
                self.eb.emit(L::BoolNot { a })
            }
            BoundExpr::NullTest { inner, want_null } => self.null_test(inner, *want_null)?,
            BoundExpr::Case { branches, else_ } => self.case(branches, else_)?,
            BoundExpr::InList { inner, items } => self.in_list(inner, items)?,
            BoundExpr::Func { f, arg } => self.func(*f, arg)?,
            BoundExpr::Calendar { op, arg } => self.calendar(*op, arg)?,
            BoundExpr::MinMaxN { is_max, args } => self.min_max_n(*is_max, args)?,
            BoundExpr::Cast { expr, to } => self.cast(expr, *to)?,
            BoundExpr::StrCall { f, args } => self.str_call(*f, args)?,
            BoundExpr::TrimCall { s, mode, set } => {
                let a = self.lower_to(s, STRING)?;
                let set_idx = self.eb.add_const_bytes(set.as_bytes());
                self.eb.emit(L::StrTrim { a, mode: *mode, set_idx })
            }
            // The result is a boolean, so `NOT LIKE` reads it like any other.
            BoundExpr::Like { s, pattern, ci } => {
                let src = self.lower_to(s, STRING)?;
                let pat_idx = self.eb.add_const_bytes(pattern.as_bytes());
                self.eb.emit(L::StrLike { src, pat_idx, ci: *ci })
            }
            BoundExpr::ConcatN { args } => self.concat_n(args)?,
        })
    }

    fn ty(&self, e: &BoundExpr) -> ColType {
        e.infer_ty(self.cols)
    }

    /// Whether every value `e`'s register can hold is a value of `fi`.
    fn within(&self, e: &BoundExpr, fi: FixedInt) -> bool {
        e.within(fi, self.cols)
    }

    fn col_ref(&mut self, idx: usize) -> Result<Reg, GnitzSqlError> {
        let c = &self.cols[idx];
        let col = idx as u32;
        // Both rejections exist for their wording: the engine refuses these
        // columns too, but names them by position only.
        if c.ty.tc.is_wide_int() {
            return Err(GnitzSqlError::Rejected(format!(
                "column {:?} is {}; 128-bit columns cannot be used in expressions",
                c.name, c.ty,
            )));
        }
        Ok(match c.ty.tc {
            TypeCode::Blob => {
                return Err(GnitzSqlError::Rejected(format!(
                    "column {:?} is {}; blob columns support only =, <>, <, <=, >, >= \
                     against another blob/string column or a string literal",
                    c.name, c.ty,
                )))
            }
            TypeCode::String => self.eb.emit(L::LoadColStr { col }),
            _ => self.eb.emit(L::LoadCol { col }),
        })
    }

    /// `IS [NOT] NULL`. A column reads the batch bitmap directly — no load —
    /// while every other operand is computed and its register's null lane
    /// tested.
    fn null_test(&mut self, inner: &BoundExpr, want_null: bool) -> Result<Reg, GnitzSqlError> {
        let invert = !want_null;
        Ok(match inner {
            BoundExpr::ColRef(c) => self.eb.emit(L::IsNull { col: *c as u32, invert }),
            _ => {
                let a = self.lower(inner)?;
                self.eb.emit(L::IsNullReg { a, invert })
            }
        })
    }

    /// Lower `expr` as a condition: a BOOLEAN, or the NULL literal, which is
    /// one that holds for no row.
    fn lower_bool(&mut self, expr: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        // Lowered first: an operand refused in itself types as a fallback, and
        // its own refusal is the one to report.
        let r = self.lower(expr)?;
        let ty = self.ty(expr);
        if ty != BOOL && !matches!(expr, BoundExpr::LitNull) {
            return Err(GnitzSqlError::Rejected(format!(
                "{} is {ty}; a condition must be BOOLEAN — compare it, or CAST it AS BOOLEAN",
                self.describe(expr)
            )));
        }
        Ok(r)
    }

    fn not_scalar(&self, e: &BoundExpr) -> GnitzSqlError {
        GnitzSqlError::Rejected(format!(
            "{} is a string; strings support comparison, LIKE/ILIKE, CONCAT/||, \
             CASE/COALESCE/NULLIF, the string functions and CAST — not this",
            self.describe(e)
        ))
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

    /// Lower `e` as a value of type `to` — the one implicit coercion, which
    /// every operand a node combines with another is read through. A literal
    /// is the constant it spells in `to`: lowering is eager, so a NULL takes
    /// its register class here, and a number is exact at a DECIMAL's scale
    /// where scaling its register would round it.
    fn lower_to(&mut self, e: &BoundExpr, to: ColType) -> Result<Reg, GnitzSqlError> {
        let class = RegClass::of(to);
        if let RegClass::Dec(scale) = class {
            check_decimal_scale(scale)?;
        }
        let constant = match (e, class) {
            (BoundExpr::LitNull, RegClass::Str) => return Ok(self.eb.emit(L::LoadNullStr)),
            (BoundExpr::LitNull, _) => return Ok(self.eb.emit(L::LoadNull)),
            (BoundExpr::LitStr(_), RegClass::Dec(_)) => Some(to),
            (_, RegClass::Dec(_)) if e.decimal_literal().is_some() => Some(to),
            // A computed float is whatever binary64 makes of the literal, an
            // infinity included: only a stored cell refuses one.
            (_, RegClass::Float) => match float_value(e) {
                Some(v) => return Ok(self.eb.const_f64(v)),
                None => None,
            },
            _ => None,
        };
        if let Some(ty) = constant {
            let val = FixedInt::I64.unpack(assign(e, ty).map_err(GnitzSqlError::Rejected)?);
            return Ok(self.eb.emit(L::LoadConst { val, unsigned: false }));
        }
        let from = self.ty(e);
        let r = self.lower(e)?;
        let r = match range_step(from, to, self.within(e, FixedInt::U64)) {
            RangeStep::None => r,
            RangeStep::ToMicros => self.eb.emit(L::Calendar {
                op: CalendarOp::ToMicros,
                a: r,
                micros: false,
            }),
            RangeStep::Cast(fi) => self.eb.emit(L::IntCast { a: r, fi }),
        };
        Ok(match (RegClass::of(from), class) {
            (RegClass::Str, RegClass::Str)
            | (RegClass::Int, RegClass::Int)
            | (RegClass::Bool, RegClass::Bool)
            | (RegClass::Float, RegClass::Float) => r,
            // A BOOLEAN is combined with nothing but a BOOLEAN, and no node
            // combines a fraction at an integer type; only CAST narrows one.
            (RegClass::Bool, _) | (_, RegClass::Bool) | (RegClass::Float | RegClass::Dec(_), RegClass::Int) => {
                return Err(GnitzSqlError::Rejected(format!(
                    "{} is {from}; CAST it to use it as {to}",
                    self.describe(e)
                )))
            }
            (RegClass::Str, _) => return Err(self.not_scalar(e)),
            (_, RegClass::Str) => return Err(GnitzSqlError::Rejected("expected a string value here".to_string())),
            (RegClass::Int | RegClass::Dec(0), RegClass::Float) => self.eb.emit(L::IntToFloat { a: r }),
            // Divided back by its scale, which is exact for every power of ten
            // an `i64` scale names.
            (RegClass::Dec(s), RegClass::Float) => {
                let f = self.eb.emit(L::IntToFloat { a: r });
                let scale = self.eb.const_f64(pow10(s) as f64);
                self.eb.emit(L::FloatArith { op: FloatArithOp::Div, a: f, b: scale })
            }
            // Widened exactly, narrowed by rounding half away from zero.
            (RegClass::Int, RegClass::Dec(s)) => self.scale_up(r, s),
            (RegClass::Dec(s), RegClass::Dec(t)) if s <= t => self.scale_up(r, t - s),
            (RegClass::Dec(s), RegClass::Dec(t)) => self.round_div(r, s - t),
            (RegClass::Float, RegClass::Dec(s)) => {
                let scale = self.eb.const_f64(pow10(s) as f64);
                let v = self.eb.emit(L::FloatArith { op: FloatArithOp::Mul, a: r, b: scale });
                let v = self.eb.emit(L::FloatUnary { op: FloatUnaryOp::Round, a: v });
                self.eb.emit(L::FloatToInt { a: v, fi: FixedInt::I64 })
            }
        })
    }

    /// Lower one result of a CASE or one argument of a GREATEST/LEAST at the
    /// type the whole list blends to. A DATE or TIMESTAMP blends only with its
    /// own type, a DATE with a TIMESTAMP too.
    fn lower_blended(&mut self, e: &BoundExpr, blend: ColType) -> Result<Reg, GnitzSqlError> {
        let src = self.ty(e).tc;
        if src.is_temporal() && src != blend.tc && blend.tc != TypeCode::Timestamp {
            return Err(GnitzSqlError::Rejected(format!(
                "cannot mix {src} with {blend} in one CASE/COALESCE/GREATEST/LEAST"
            )));
        }
        self.lower_to(e, blend)
    }

    /// `e` rendered as text: CAST to a string type, and CONCAT's implicit cast
    /// of each argument.
    fn as_text(&mut self, e: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        if matches!(e, BoundExpr::LitNull) {
            return Ok(self.eb.emit(L::LoadNullStr));
        }
        Ok(match RegClass::of(self.ty(e)) {
            RegClass::Str => self.lower(e)?,
            // `true` / `false`, and NULL for a NULL: neither test takes one.
            RegClass::Bool => {
                let text = |s: &str| BoundExpr::LitStr(s.to_string());
                let not = BoundExpr::Not(Box::new(e.clone()));
                self.case(&[(e.clone(), text("true")), (not, text("false"))], &BoundExpr::LitNull)?
            }
            RegClass::Int => {
                let a = self.lower(e)?;
                self.eb.emit(L::IntToStr { a })
            }
            RegClass::Float | RegClass::Dec(_) => {
                let a = self.lower_to(e, ColType::of(TypeCode::F64))?;
                self.eb.emit(L::FloatToStr { a })
            }
        })
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

    /// The numeric functions, in the form [`NumFunc::result_type`] names for
    /// the argument's type: float arithmetic where the result is an F64, else
    /// integer arithmetic on the register — a DECIMAL's at its scale.
    fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        use FloatUnaryOp as F;
        let arg_ty = self.ty(arg);
        match RegClass::of(arg_ty) {
            RegClass::Str => return Err(self.not_scalar(arg)),
            RegClass::Bool => {
                return Err(GnitzSqlError::Rejected(format!(
                    "{} is BOOLEAN; a numeric function takes a number",
                    self.describe(arg)
                )))
            }
            _ => {}
        }
        let out = f.result_type(arg_ty);
        if out.tc == TypeCode::F64 {
            let a = self.lower_to(arg, out)?;
            return Ok(match f {
                NumFunc::Unary(op) => self.eb.emit(L::FloatUnary { op, a }),
                NumFunc::Round(n) => self.scaled_round(a, n),
            });
        }
        let r = self.lower(arg)?;
        let int_op = match f {
            NumFunc::Unary(F::Neg) => Some(IntUnaryOp::Neg),
            NumFunc::Unary(F::Abs) if arg_ty.tc.is_signed_int() => Some(IntUnaryOp::Abs),
            NumFunc::Unary(F::Sign) => Some(IntUnaryOp::Sign),
            _ => None,
        };
        if let Some(op) = int_op {
            return Ok(self.eb.emit(L::IntUnary { op, a: r }));
        }
        // What is left rounds: it drops the digits the result's scale has no
        // place for, which over an integer, and for an unsigned ABS, is none.
        let by = arg_ty.scale - out.scale;
        Ok(match f {
            _ if by == 0 => r,
            NumFunc::Unary(F::Floor) => self.floor_ceil_div(r, by, false),
            NumFunc::Unary(F::Ceil) => self.floor_ceil_div(r, by, true),
            NumFunc::Unary(F::Trunc) => {
                let q = self.eb.emit(L::LoadConst { val: pow10(by), unsigned: false });
                self.eb.emit(L::IntArith { op: IntArithOp::Div, a: r, b: q })
            }
            _ => self.round_div(r, by),
        })
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

    fn calendar(&mut self, op: CalendarOp, arg: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        let micros = match self.ty(arg).tc {
            TypeCode::Timestamp => true,
            TypeCode::Date => false,
            t => {
                return Err(GnitzSqlError::Rejected(format!(
                    "a calendar function takes a DATE or TIMESTAMP; {} is {t}",
                    self.describe(arg)
                )))
            }
        };
        let a = self.lower(arg)?;
        Ok(self.eb.emit(L::Calendar { op, a, micros }))
    }

    fn str_call(&mut self, f: StrFunc, args: &[BoundExpr]) -> Result<Reg, GnitzSqlError> {
        let mut regs = Vec::with_capacity(args.len());
        for (k, (kind, a)) in f.signature().iter().zip(args).enumerate() {
            regs.push(match kind {
                StrArg::Str | StrArg::StrOr(_) => self.lower_to(a, STRING)?,
                // The opcode reads the register as an integer, so a fraction is
                // an error rather than a silent truncation.
                StrArg::Int | StrArg::IntOpt => {
                    if RegClass::of(self.ty(a)) != RegClass::Int {
                        return Err(GnitzSqlError::Rejected(format!(
                            "{}: argument {} must be an integer expression",
                            f.sql_name(),
                            k + 1
                        )));
                    }
                    self.lower(a)?
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
        Ok(self.eb.emit(instr))
    }

    /// `CONCAT(args…)`, a strictly left fold seeded with the empty string, so
    /// every arity has one shape and the one-argument case is non-NULL. Every
    /// argument lands in the `b` operand, where a NULL contributes the empty
    /// string; only the accumulator's own NULL propagates.
    fn concat_n(&mut self, args: &[BoundExpr]) -> Result<Reg, GnitzSqlError> {
        let empty = self.eb.add_const_bytes(b"");
        let mut acc = self.eb.emit(L::LoadConstStr { const_idx: empty });
        for a in args {
            let b = self.as_text(a)?;
            acc = self.eb.emit(L::StrConcat { a: acc, b, skip_null: true });
        }
        Ok(acc)
    }

    /// GREATEST/LEAST as a left fold of 2-ary extremum opcodes.
    fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<Reg, GnitzSqlError> {
        let types = operand_tys(&args.iter().collect::<Vec<_>>(), self.cols);
        if let Some((a, _)) = args.iter().zip(&types).find(|(_, t)| t.tc == TypeCode::String) {
            return Err(self.not_scalar(a));
        }
        let ty = blend_type(&types);
        let mut acc = self.lower_blended(&args[0], ty)?;
        for a in &args[1..] {
            let b = self.lower_blended(a, ty)?;
            acc = if ty.tc.is_float() {
                self.eb.emit(L::FloatMinMax2 { a: acc, b, is_max })
            } else {
                self.eb.emit(L::IntMinMax2 { a: acc, b, is_max })
            };
        }
        Ok(acc)
    }

    /// CAST, dispatched on the source and target classes. A superset of
    /// [`Self::lower_to`]: it also parses a string, renders text and narrows.
    fn cast(&mut self, expr: &BoundExpr, to: ColType) -> Result<Reg, GnitzSqlError> {
        let from = self.ty(expr);
        let from_class = RegClass::of(from);
        if from_class == RegClass::Str && (to.tc.is_temporal() || to == BOOL) {
            return Err(GnitzSqlError::Rejected(format!(
                "CAST of a string to {to} is supported for a literal only"
            )));
        }
        let plain_int = |t: ColType| FixedInt::exact(t.tc).is_some();
        match (from_class, RegClass::of(to)) {
            (_, RegClass::Str) => return self.as_text(expr),
            // A BOOLEAN is the integer 0 or 1, and an integer is true where it
            // is not zero; a fraction and a calendar value are neither.
            (RegClass::Bool, RegClass::Bool) => return self.lower(expr),
            (_, RegClass::Bool) if plain_int(from) => return self.binop(expr, BinOp::Ne, &BoundExpr::LitInt(0)),
            (RegClass::Bool, _) if plain_int(to) => {}
            (RegClass::Bool, _) | (_, RegClass::Bool) => {
                return Err(GnitzSqlError::Rejected(format!(
                    "CAST of {from} to {to} is not supported"
                )))
            }
            (_, RegClass::Dec(_)) => return self.lower_to(expr, to),
            (_, RegClass::Float) => {
                let f = match from_class {
                    RegClass::Str => {
                        let a = self.lower(expr)?;
                        self.eb.emit(L::StrToFloat { a })
                    }
                    _ => self.lower_to(expr, ColType::of(TypeCode::F64))?,
                };
                return Ok(match to.tc {
                    TypeCode::F32 => self.eb.emit(L::FloatToF32 { a: f }),
                    _ => f,
                });
            }
            (_, RegClass::Int) => {}
        }
        if (from.tc, to.tc) == (TypeCode::Date, TypeCode::Timestamp) {
            return self.lower_to(expr, to);
        }
        let a = self.lower(expr)?;
        if (from.tc, to.tc) == (TypeCode::Timestamp, TypeCode::Date) {
            return Ok(self.eb.emit(L::Calendar { op: CalendarOp::ToDays, a, micros: true }));
        }
        // Any other integer into a temporal type is already its storage value,
        // which the range check below admits.
        let Some(fi) = FixedInt::from_type_code(to.tc) else {
            return Err(GnitzSqlError::Rejected(format!("CAST to {to} is not supported")));
        };
        Ok(match from_class {
            RegClass::Str => self.eb.emit(L::StrToInt { a, fi }),
            RegClass::Float => self.eb.emit(L::FloatToInt { a, fi }),
            // Rounded to a whole number, then range-checked into the target
            // like any other i64.
            RegClass::Dec(s) => {
                let whole = self.round_div(a, s);
                if fi == FixedInt::I64 {
                    whole
                } else {
                    self.eb.emit(L::IntCast { a: whole, fi })
                }
            }
            // Elided where the register already holds only values of `to`, read
            // with `to`'s signedness: the engine tracks a register as U64 iff
            // its type is U64, so an elided cast must not change that.
            RegClass::Int | RegClass::Bool
                if self.within(expr, fi) && (fi == FixedInt::U64) == reads_unsigned(from) =>
            {
                a
            }
            RegClass::Int | RegClass::Bool => self.eb.emit(L::IntCast { a, fi }),
        })
    }

    /// Searched CASE as a right-to-left fold of selects, so the first true
    /// WHEN wins. `Select` blends raw bit patterns, so every result is lowered
    /// at the type they blend to.
    fn case(&mut self, branches: &[(BoundExpr, BoundExpr)], else_: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        let results: Vec<&BoundExpr> = branches.iter().map(|(_, r)| r).chain([else_]).collect();
        let ty = blend_type(&operand_tys(&results, self.cols));
        let mut arms = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            arms.push((self.lower_bool(cond)?, self.lower_blended(result, ty)?));
        }
        let mut acc = self.lower_blended(else_, ty)?;
        for (cond, a) in arms.into_iter().rev() {
            acc = if ty.tc == TypeCode::String {
                self.eb.emit(L::StrSelect { cond, a, b: acc })
            } else {
                self.eb.emit(L::Select { cond, a, b: acc })
            };
        }
        Ok(acc)
    }

    /// `inner IN (items…)`. An operand stored as a ≤8-byte integer whose items
    /// all place among its values is one `IntInSet` of the items it holds;
    /// anything else is the `inner = item` OR chain. `self.cols` is the schema
    /// `inner` was bound against, so the gate holds for a HAVING over the reduce
    /// output as much as for a table filter.
    fn in_list(&mut self, inner: &BoundExpr, items: &[BoundExpr]) -> Result<Reg, GnitzSqlError> {
        let ty = self.ty(inner);
        if let Some(fi) = FixedInt::from_type_code(ty.tc) {
            let placed = items
                .iter()
                .map(|i| place(i, ty))
                .collect::<Option<Vec<Placed>>>()
                .filter(|ps| self.within(inner, fi) || ps.iter().all(|p| !p.is_outside()));
            if let Some(placed) = placed {
                let values: Vec<i64> = placed
                    .iter()
                    .filter_map(|p| match p {
                        Placed::At(v) => Some(fi.unpack(*v)),
                        _ => None,
                    })
                    .collect();
                let c = self.lower(inner)?;
                if values.is_empty() {
                    return Ok(self.eb.emit(L::Cmp { op: CmpOp::Ne, a: c, b: c }));
                }
                let set_idx = self.eb.add_const_int_set(values);
                return Ok(self.eb.emit(L::IntInSet { value_reg: c, set_idx }));
            }
        }
        // Each term lowers the operand again; the builder folds the identical
        // instructions, so a non-fused operand is loaded once for the whole list.
        let mut acc = self.binop(inner, BinOp::Eq, &items[0])?;
        for it in &items[1..] {
            let r = self.binop(inner, BinOp::Eq, it)?;
            acc = self.eb.emit(L::BoolBinary { a: acc, b: r, is_or: true });
        }
        Ok(acc)
    }

    /// `other CMP lit` where `other` is stored as a ≤8-byte integer: the literal
    /// is placed among `other`'s values, the rule seek keys and written cells
    /// take. `None` leaves the comparison to the general path.
    fn cmp_placed(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<Option<Reg>, GnitzSqlError> {
        // Either side may be the literal. When both are, the operand is the side
        // with an integer-stored type (`'2020-01-01' = DATE '2020-01-01'`).
        let typed = |other: &BoundExpr, lit: &BoundExpr| -> Option<(ColType, FixedInt)> {
            if !lit.is_literal() || matches!(other, BExpr::LitStr(_) | BExpr::LitNull) {
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
        if p.is_outside() && !self.within(other, fi) {
            return Ok(None);
        }
        let a = self.lower(other)?;
        let (op, b) = match p.compare(cmp) {
            Compared::Cmp(op, v) => {
                let unsigned = fi == FixedInt::U64;
                (op, self.eb.emit(L::LoadConst { val: fi.unpack(v), unsigned }))
            }
            // `c = c` / `c <> c`: true / false on every row, NULL on a NULL one.
            Compared::Always(holds) => (if holds { CmpOp::Eq } else { CmpOp::Ne }, a),
        };
        Ok(Some(self.eb.emit(L::Cmp { op, a, b })))
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

    /// A binary operator: the connectives over two booleans, and every other
    /// one over its operands brought to the types [`bin_types`] names, in the
    /// opcode of the class they share.
    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<Reg, GnitzSqlError> {
        if let BinOp::And | BinOp::Or = op {
            let a = self.lower_bool(left)?;
            let b = self.lower_bool(right)?;
            return Ok(self.eb.emit(L::BoolBinary { a, b, is_or: op == BinOp::Or }));
        }
        if let Some(reg) = self.cmp_placed(left, op, right)? {
            return Ok(reg);
        }
        if let Some(reg) = self.string_cmp(left, op, right) {
            return Ok(reg);
        }
        let (lt, rt) = operand_ty_pair(left, right, self.cols);
        let t = bin_types(op, lt, rt).map_err(GnitzSqlError::Rejected)?;
        if t.out.is_decimal() {
            check_decimal_scale(t.out.scale)?;
        }
        let a = self.lower_to(left, t.l)?;
        let b = self.lower_to(right, t.r)?;
        Ok(self.eb.emit(match (op.as_cmp(), RegClass::of(t.l)) {
            (Some(op), RegClass::Str) => L::StrCmp { op, a, b },
            (Some(op), RegClass::Float) => L::FCmp { op, a, b },
            (Some(op), RegClass::Int | RegClass::Bool | RegClass::Dec(_)) => L::Cmp { op, a, b },
            // `||` propagates a NULL, and casts no number, unlike CONCAT.
            (None, RegClass::Str) => L::StrConcat { a, b, skip_null: false },
            (None, RegClass::Float) => {
                let op = op.as_float_arith().expect("`bin_types` takes no float modulo");
                L::FloatArith { op, a, b }
            }
            (None, RegClass::Int | RegClass::Bool | RegClass::Dec(_)) => {
                let op = op
                    .as_int_arith()
                    .expect("an operator over integers that is no comparison is arithmetic");
                L::IntArith { op, a, b }
            }
        }))
    }
}

/// Compile a BoundExpr into `eb`, returning the result register — the form for
/// a caller threading several expressions into one program.
pub(crate) fn compile_bound_expr(
    expr: &BoundExpr,
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Result<Reg, GnitzSqlError> {
    Lowering { cols, eb }.lower(expr)
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
/// boolean literals: exact wherever it is read, since `true OR NULL` is true and
/// `false AND NULL` is false.
fn constant_truth(e: &BoundExpr) -> Option<bool> {
    let settle = |l: &BoundExpr, r: &BoundExpr, absorbing: bool| match (constant_truth(l), constant_truth(r)) {
        (Some(t), _) | (_, Some(t)) if t == absorbing => Some(absorbing),
        (Some(_), Some(_)) => Some(!absorbing),
        _ => None,
    };
    match e {
        BoundExpr::LitBool(b) => Some(*b),
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
    let mut backend = Lowering { cols, eb: &mut eb };
    let mut acc: Option<Reg> = None;
    for e in conjuncts {
        // A conjunct settled true filters nothing, and is still a condition:
        // it is lowered apart for its refusals alone.
        if constant_truth(e) == Some(true) {
            Lowering { cols, eb: &mut ExprBuilder::new() }.lower_bool(e)?;
            continue;
        }
        let r = backend.lower_bool(e)?;
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
