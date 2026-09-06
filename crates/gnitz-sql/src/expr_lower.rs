//! Expression lowering: a bound `BoundExpr` → the VM's opcode program, as a
//! `LogicalProgram`, a resolved `Evaluator`, or raw predicate bytes.
//!
//! This is the *scalar* half of lowering. `hir::lower` is the *relational* half
//! (`RelExpr` → DBSP circuit) and calls into this one for every filter, map and
//! projection expression it emits.

use crate::bind::structural::str_func_name;
use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, NumFunc, StrArg, StrFunc, TrimMode, UnaryOp};
use gnitz_core::{ColumnDef, FixedInt, Schema, TypeCode};
use gnitz_expr::{
    Evaluator, ExprBuilder, FloatArithOp, FloatUnaryOp, IntUnaryOp, LogicalInstr as L, LogicalProgram, Reg,
};

/// Compile a comparison between two German-string columns, or a column and a
/// string literal, to the `StrCol*` opcodes, which read the 16-byte cells
/// directly and need no string register. STRING and BLOB share that layout, so
/// both are admitted here; a BLOB has no other legal use (`col_ref` rejects it).
///
/// `None` is "not this shape": any other operand, or an operator that is not a
/// comparison — which falls through to the register channel rather than erroring
/// here, so `strcol || 'lit'` still reaches `||`. Nothing is emitted before the
/// shape is settled, so a declined pair leaves no const-pool entry behind.
fn try_compile_string_cmp(
    left: &BoundExpr,
    op: BinOp,
    right: &BoundExpr,
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Option<Reg> {
    let is_str_col = |e: &BoundExpr| matches!(e, BoundExpr::ColRef(i) if cols[*i].type_code.is_german_string());
    // The opcode pins the constant on the right, so a literal-on-left pair is
    // read as its converse: `'A' < col` is `col > 'A'`.
    let (col, s, cmp) = match (left, right) {
        (BoundExpr::ColRef(c), BoundExpr::LitStr(s)) if is_str_col(left) => (*c, s, op.as_cmp()?),
        (BoundExpr::LitStr(s), BoundExpr::ColRef(c)) if is_str_col(right) => (*c, s, op.converse().as_cmp()?),
        (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) if is_str_col(left) && is_str_col(right) => {
            return Some(eb.emit(L::StrColCol {
                op: op.as_cmp()?,
                col_a: *a as u32,
                col_b: *b as u32,
            }));
        }
        _ => return None,
    };
    let const_idx = eb.add_const_string(s.clone());
    Some(eb.emit(L::StrColConst { op: cmp, col: col as u32, const_idx }))
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
/// Numeric operands are read through [`Self::lower_num`] / [`Self::lower_as`]
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
            BoundExpr::LitInt(v) => Ok((self.eb.emit(L::LoadConst { val: *v }), ExprKind::Int)),
            BoundExpr::LitFloat(v) => Ok((self.eb.const_f64(*v), ExprKind::Float)),
            BoundExpr::LitStr(s) => {
                let idx = self.eb.add_const_string(s.clone());
                Ok((self.eb.emit(L::LoadConstStr { const_idx: idx }), ExprKind::Str))
            }
            // The register file is 8 bytes wide, so a wide literal has no slot.
            // A *servable* wide seek is consumed into a PK/index bound by the
            // access-path recognizer and never gets here.
            BoundExpr::LitWide(s) => Err(crate::ir::wide_int_error(s)),
            BoundExpr::LitNull => Ok((self.eb.emit(L::LoadNull), ExprKind::Int)),
            BoundExpr::BinOp(l, op, r) => self.binop(l, *op, r),
            BoundExpr::UnaryOp(UnaryOp::Neg, inner) => self.func(NumFunc::Unary(FloatUnaryOp::Neg), inner),
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                let (a, _) = self.lower_num(inner)?;
                Ok((self.eb.emit(L::BoolNot { a }), ExprKind::Int))
            }
            BoundExpr::NullTest { inner, want_null } => self.null_test(inner, *want_null),
            BoundExpr::Case { branches, else_ } => self.case(branches, else_.as_deref()),
            BoundExpr::InList { inner, items } => self.in_list(inner, items),
            BoundExpr::Func { f, arg } => self.func(*f, arg),
            BoundExpr::MinMaxN { is_max, args } => self.min_max_n(*is_max, args),
            BoundExpr::Cast { expr, to } => self.cast(expr, *to),
            BoundExpr::StrCall { f, args } => self.str_call(*f, args),
            BoundExpr::Substr { s, start, len } => self.substr(s, start, len.as_deref()),
            BoundExpr::TrimCall { s, mode, set } => self.trim_call(s, *mode, set),
            BoundExpr::Like { s, pattern, escape, ci } => self.like(s, pattern, *escape, *ci),
            BoundExpr::ConcatN { args } => self.concat_n(args),
        }
    }

    fn col_ref(&mut self, idx: usize) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let tc = self.cols[idx].type_code;
        let col = idx as u32;
        // Both rejections exist for their wording: the engine's validator refuses
        // a wide column on the integer load too, but names it by position only.
        if tc.is_wide_int() {
            return Err(GnitzSqlError::Unsupported(format!(
                "column {:?} is {tc:?}; 128-bit columns cannot be used in expressions",
                self.cols[idx].name,
            )));
        }
        Ok(match tc {
            TypeCode::Blob => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "column {:?} is {tc:?}; blob columns support only =, <>, <, <=, >, >= \
                     against another blob/string column or a string literal",
                    self.cols[idx].name,
                )))
            }
            TypeCode::String => (self.eb.emit(L::LoadColStr { col }), ExprKind::Str),
            _ if tc.is_float() => (self.eb.emit(L::LoadColFloat { col }), ExprKind::Float),
            _ => (self.eb.emit(L::LoadColInt { col }), ExprKind::Int),
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

    /// Lower `expr` into a scalar register, reporting whether it holds an f64.
    fn lower_num(&mut self, expr: &BoundExpr) -> Result<(Reg, bool), GnitzSqlError> {
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

    /// Lower `expr` and, if `want_float`, lift an integer register to f64.
    fn lower_as(&mut self, expr: &BoundExpr, want_float: bool) -> Result<Reg, GnitzSqlError> {
        let (r, is_float) = self.lower_num(expr)?;
        Ok(if want_float && !is_float {
            self.eb.emit(L::IntToFloat { a: r })
        } else {
            r
        })
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
            (r, ExprKind::Float) if coerce_numeric => Ok(self.eb.emit(L::FloatToStr { a: r })),
            _ => Err(GnitzSqlError::Unsupported("expected a string value here".to_string())),
        }
    }

    /// An integer operand — a window bound, a count, a field index. A float is a
    /// typed error rather than a silent truncation: the opcode reads the
    /// register as an integer. `what` names the position the error reports.
    fn int_operand(&mut self, e: &BoundExpr, what: impl FnOnce() -> String) -> Result<Reg, GnitzSqlError> {
        match self.lower_num(e)? {
            (r, false) => Ok(r),
            (_, true) => Err(GnitzSqlError::Unsupported(format!(
                "{} must be an integer expression",
                what()
            ))),
        }
    }

    /// The numeric functions, unary NEG included. Over an integer register the
    /// rounding family and `ROUND(x, n >= 0)` are the identity, ABS of an
    /// unsigned register likewise, and only NEG, signed ABS and SIGN compute;
    /// everything else lifts to f64. `NumFunc::result_type` states that split
    /// and this reads it, so the declared column and the register cannot
    /// disagree.
    fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let (r, is_float) = self.lower_num(arg)?;
        let arg_ty = arg.infer_type(self.cols);
        if f.result_type(arg_ty) != TypeCode::F64 {
            let int_op = match f {
                NumFunc::Unary(FloatUnaryOp::Neg) => Some(IntUnaryOp::Neg),
                NumFunc::Unary(FloatUnaryOp::Abs) if arg_ty.is_signed_int() => Some(IntUnaryOp::Abs),
                NumFunc::Unary(FloatUnaryOp::Sign) => Some(IntUnaryOp::Sign),
                _ => None,
            };
            let reg = int_op.map_or(r, |op| self.eb.emit(L::IntUnary { op, a: r }));
            return Ok((reg, ExprKind::Int));
        }
        let a = if is_float {
            r
        } else {
            self.eb.emit(L::IntToFloat { a: r })
        };
        let reg = match f {
            NumFunc::Unary(op) => self.eb.emit(L::FloatUnary { op, a }),
            NumFunc::Round(n) => self.scaled_round(a, n),
        };
        Ok((reg, ExprKind::Float))
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

    fn str_call(&mut self, f: StrFunc, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let mut regs = Vec::with_capacity(args.len());
        for (k, (kind, a)) in f.signature().iter().zip(args).enumerate() {
            regs.push(match kind {
                StrArg::Str | StrArg::StrOr(_) => self.str_operand(a, false)?,
                StrArg::Int => self.int_operand(a, || format!("{}: argument {}", str_func_name(f), k + 1))?,
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
            _ => unreachable!("the binder sizes a string call's arguments by its signature"),
        };
        let kind = if f.result_type() == TypeCode::String {
            ExprKind::Str
        } else {
            ExprKind::Int
        };
        Ok((self.eb.emit(instr), kind))
    }

    fn substr(
        &mut self,
        s: &BoundExpr,
        start: &BoundExpr,
        len: Option<&BoundExpr>,
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let start_reg = self.int_operand(start, || "SUBSTRING: start".into())?;
        let len_reg = len
            .map(|l| self.int_operand(l, || "SUBSTRING: length".into()))
            .transpose()?;
        Ok((self.eb.emit(L::StrSubstr { src, start_reg, len_reg }), ExprKind::Str))
    }

    fn trim_call(&mut self, s: &BoundExpr, mode: TrimMode, set: &str) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let set_idx = self.eb.add_const_string(set.to_string());
        Ok((self.eb.emit(L::StrTrim { a: src, mode, set_idx }), ExprKind::Str))
    }

    /// `s [I]LIKE 'pattern'`. The result is a boolean, so `NOT LIKE` reads it
    /// like any other.
    fn like(
        &mut self,
        s: &BoundExpr,
        pattern: &str,
        escape: Option<u8>,
        ci: bool,
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let pat_idx = self.eb.add_const_string(pattern.to_string());
        // A NUL escape is `ESCAPE ''` — no escape at all — which the binder has
        // already rejected as a literal, so nothing is lost by the narrowing.
        let escape = escape.and_then(std::num::NonZeroU8::new);
        Ok((self.eb.emit(L::StrLike { src, escape, pat_idx, ci }), ExprKind::Int))
    }

    /// `CONCAT(args…)`, a strictly left fold seeded with the empty string, so
    /// every arity has one shape and the one-argument case is non-NULL. Every
    /// argument lands in the `b` operand, where a NULL contributes the empty
    /// string; only the accumulator's own NULL (from `str_concat_nn`)
    /// propagates.
    fn concat_n(&mut self, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let empty = self.eb.add_const_string(String::new());
        let mut acc = self.eb.emit(L::LoadConstStr { const_idx: empty });
        for a in args {
            let r = self.str_operand(a, true)?;
            acc = self.eb.emit(L::StrConcat { a: acc, b: r, skip_null: true });
        }
        Ok((acc, ExprKind::Str))
    }

    /// GREATEST/LEAST as a left fold of 2-ary extremum opcodes.
    fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let types: Vec<TypeCode> = args.iter().map(|a| a.infer_type(self.cols)).collect();
        let any_float = types
            .iter()
            .fold(TypeCode::I64, |acc, &t| crate::ir::unify_blend_type(acc, t))
            .is_float();
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
            let b = self.lower_as(a, any_float)?;
            acc = if any_float {
                self.eb.emit(L::FloatMinMax2 { a: acc, b, is_max })
            } else {
                self.eb.emit(L::IntMinMax2 { a: acc, b, is_max })
            };
        }
        Ok((acc, ExprKind::num(any_float)))
    }

    /// CAST, dispatched on the target class, then the source kind.
    fn cast(&mut self, expr: &BoundExpr, to: TypeCode) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let (r, kind) = self.lower(expr)?;
        if to == TypeCode::String {
            let reg = match kind {
                ExprKind::Str => r,
                ExprKind::Float => self.eb.emit(L::FloatToStr { a: r }),
                ExprKind::Int => self.eb.emit(L::IntToStr { a: r }),
            };
            return Ok((reg, ExprKind::Str));
        }
        if to.is_float() {
            let f = match kind {
                ExprKind::Str => self.eb.emit(L::StrToFloat { a: r }),
                ExprKind::Int => self.eb.emit(L::IntToFloat { a: r }),
                ExprKind::Float => r,
            };
            // F64 needs nothing more: the register already holds an f64.
            let reg = if to == TypeCode::F32 {
                self.eb.emit(L::FloatToF32 { a: f })
            } else {
                f
            };
            return Ok((reg, ExprKind::Float));
        }
        // Every remaining target is a `FixedInt`: the float and String branches
        // above peeled the rest, and `is_cast_target` admits nothing else.
        let fi = FixedInt::from_type_code(to).expect("is_cast_target admitted this");
        let reg = match kind {
            ExprKind::Str => self.eb.emit(L::StrToInt { a: r, fi }),
            ExprKind::Float => self.eb.emit(L::FloatToInt { a: r, fi }),
            ExprKind::Int => {
                // Elide iff the register provably already holds a value inside
                // `to`'s domain *with* `to`'s register image: the engine tracks a
                // register as U64 iff its type is U64, so an elided cast must not
                // change that bit. Only a bare column load qualifies for the
                // exact-type half — `infer_type` types `-i32col` as I32, but the
                // VM negate wraps on the i64 image, so `-(-2^31)` leaves I32.
                let elide = to == expr.infer_type(self.cols).register_image()
                    || matches!(expr, BoundExpr::ColRef(i) if self.cols[*i].type_code == to);
                if elide {
                    r
                } else {
                    self.eb.emit(L::IntCast { a: r, fi })
                }
            }
        };
        Ok((reg, ExprKind::Int))
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
        let case_ty = BoundExpr::case_type(branches, else_, &|idx: &usize| self.cols[*idx].type_code);
        let is_str = case_ty == TypeCode::String;
        let any_float = case_ty.is_float();
        let mut conds = Vec::with_capacity(branches.len());
        let mut results = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            conds.push(self.lower_num(cond)?.0);
            results.push(self.case_value(result, is_str, any_float)?);
        }
        let mut acc = match else_ {
            Some(e) => self.case_value(e, is_str, any_float)?,
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
        Ok((
            acc,
            if is_str {
                ExprKind::Str
            } else {
                ExprKind::num(any_float)
            },
        ))
    }

    fn case_value(&mut self, e: &BoundExpr, is_str: bool, any_float: bool) -> Result<Reg, GnitzSqlError> {
        if is_str {
            self.str_operand(e, false)
        } else {
            self.lower_as(e, any_float)
        }
    }

    /// `inner IN (items…)`. A ≤8-byte-integer operand with all-integer-literal
    /// items is one `IntInSet`; anything else is the `inner = item` OR chain.
    /// `self.cols` is the schema `inner` was bound against, so the gate holds
    /// for a HAVING over the reduce output as much as for a table filter.
    fn in_list(&mut self, inner: &BoundExpr, items: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        if gnitz_wire::is_fixed_int(inner.infer_type(self.cols) as u8) {
            let literal = |e: &BoundExpr| match e {
                BoundExpr::LitInt(v) => Some(*v),
                _ => None,
            };
            if let Some(mut values) = items.iter().map(literal).collect::<Option<Vec<i64>>>() {
                values.sort_unstable();
                values.dedup();
                let value_reg = self.lower_num(inner)?.0;
                let set_idx = self.eb.add_const_int_set(&values);
                return Ok((self.eb.emit(L::IntInSet { value_reg, set_idx }), ExprKind::Int));
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

    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        if let Some(reg) = try_compile_string_cmp(left, op, right, self.cols, self.eb) {
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
        let (mut l, l_kind) = self.lower(left)?;
        let (mut r, r_kind) = self.lower(right)?;
        if l_kind == ExprKind::Str || r_kind == ExprKind::Str {
            return self.str_binop(op, (l, l_kind), (r, r_kind));
        }
        if let BinOp::And | BinOp::Or = op {
            let reg = self.eb.emit(L::BoolBinary { a: l, b: r, is_or: op == BinOp::Or });
            return Ok((reg, ExprKind::Int));
        }
        let (l_float, r_float) = (l_kind == ExprKind::Float, r_kind == ExprKind::Float);
        // POWER has no integer form: both operands lift.
        let is_float = l_float || r_float || op == BinOp::Pow;
        if is_float && !l_float {
            l = self.eb.emit(L::IntToFloat { a: l });
        }
        if is_float && !r_float {
            r = self.eb.emit(L::IntToFloat { a: r });
        }
        if let Some(cmp) = op.as_cmp() {
            let reg = if is_float {
                self.eb.emit(L::FCmp { op: cmp, a: l, b: r })
            } else {
                self.eb.emit(L::Cmp { op: cmp, a: l, b: r })
            };
            return Ok((reg, ExprKind::Int));
        }
        if is_float {
            let op = op
                .as_float_arith()
                .ok_or_else(|| GnitzSqlError::Unsupported("float modulo not supported".to_string()))?;
            Ok((self.eb.emit(L::FloatArith { op, a: l, b: r }), ExprKind::Float))
        } else {
            let op = op
                .as_int_arith()
                .expect("every scalar operator that is not a comparison has an integer form");
            Ok((self.eb.emit(L::IntArith { op, a: l, b: r }), ExprKind::Int))
        }
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
            return Err(GnitzSqlError::Unsupported(format!(
                "operator {op:?} is not supported on a string operand"
            )));
        };
        if a_kind != ExprKind::Str || b_kind != ExprKind::Str {
            return Err(GnitzSqlError::Unsupported(format!(
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
    Ok(eb.build(Some(reg))?)
}

/// Compile the AND of `conjuncts` as a filter program. A true-constant conjunct
/// (the binder's fold of `IS NOT NULL` on a non-nullable column) tests nothing
/// and is dropped wherever it sits; `None` is the statically-true verdict once
/// nothing is left, on which the caller keeps every row rather than paying a
/// per-row evaluation. A false constant keeps its program: it must still drop
/// every row.
pub(crate) fn compile_filter_program<'a>(
    conjuncts: impl IntoIterator<Item = &'a BoundExpr>,
    cols: &[ColumnDef],
) -> Result<Option<LogicalProgram>, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let mut backend = OpcodeBackend { cols, eb: &mut eb };
    let mut acc: Option<Reg> = None;
    for e in conjuncts {
        if matches!(e, BoundExpr::LitInt(v) if *v != 0) {
            continue;
        }
        let (r, _) = backend.lower_num(e)?;
        acc = Some(match acc {
            Some(a) => backend.eb.emit(L::BoolBinary { a, b: r, is_or: false }),
            None => r,
        });
    }
    Ok(acc.map(|reg| eb.build(Some(reg))).transpose()?)
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

/// The AND of `conjuncts` as a resolved filter evaluator; `None` when
/// statically true. `resolve_filter` gives a bare non-boolean predicate
/// (`HAVING COUNT(*)`) the `bool_bits` bit [`Evaluator::filter_ranges`] reads.
pub(crate) fn compile_conjuncts_evaluator<'a>(
    conjuncts: impl IntoIterator<Item = &'a BoundExpr>,
    schema: &Schema,
) -> Result<Option<Evaluator>, GnitzSqlError> {
    Ok(compile_filter_program(conjuncts, &schema.columns)?
        .map(|p| p.resolve_filter(schema))
        .transpose()?)
}

/// A scalar (non-predicate) expression — a SET value, a grouped SELECT's
/// finalize item — as a resolved evaluator read a row at a time. Whether the
/// result is a string, and so which read-back applies, the caller asks the
/// evaluator (`Evaluator::result_is_str`).
pub(crate) fn compile_scalar_evaluator(expr: &BoundExpr, schema: &Schema) -> Result<Evaluator, GnitzSqlError> {
    Ok(compile_bound_expr_to_program(expr, &schema.columns)?.resolve_scalar(schema)?)
}

#[cfg(test)]
#[path = "tests/expr_lower.rs"]
mod tests;
