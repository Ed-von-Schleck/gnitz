//! Expression lowering: a bound `BoundExpr` → the VM's opcode program, as a
//! `LogicalProgram`, a resolved `Evaluator`, or raw predicate bytes.
//!
//! This is the *scalar* half of lowering. `hir::lower` is the *relational* half
//! (`RelExpr` → DBSP circuit) and calls into this one for every filter, map and
//! projection expression it emits.

use crate::bind::structural::str_func_name;
use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, NumFunc, StrArg, StrFunc, TrimMode, UnaryOp};
use crate::types::int_cast_target;
use gnitz_core::{ColumnDef, Schema, TypeCode};
use gnitz_expr::{
    Evaluator, ExprBuilder, ExprValidateErr, FloatArithOp, FloatUnaryOp, IntUnaryOp, LogicalInstr as L, LogicalProgram,
    Reg, StrOp,
};

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

/// True iff `expr` can be one side of a fused [`try_compile_string_cmp`] pair:
/// a string literal, or a column carrying the 16-byte German-string layout.
fn fuses_string_cmp(expr: &BoundExpr, cols: &[ColumnDef]) -> bool {
    match expr {
        BoundExpr::LitStr(_) => true,
        BoundExpr::ColRef(idx) => cols[*idx].type_code.is_german_string(),
        _ => false,
    }
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
) -> Option<Reg> {
    // Both operands must be fusable shapes; the arms below pick which pairing.
    // Checked before anything is emitted: a later bail would leave a dead
    // const-pool entry behind.
    if !(fuses_string_cmp(left, cols) && fuses_string_cmp(right, cols)) {
        return None;
    }
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
            let reg = eb.emit(L::StrColConst {
                op: prim,
                col: idx as u32,
                const_idx,
            });
            return Some(if negate { eb.emit(L::BoolNot { a: reg }) } else { reg });
        }
    }
    // ColRef(string/blob) op ColRef(string/blob) — two symmetric operands, so
    // GT/GE ride the swap rather than a negation.
    if let (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) = (left, right) {
        if cols[*a].type_code.is_german_string() && cols[*b].type_code.is_german_string() {
            let (prim, swap, negate) = str_cmp_reduction(*op, true)?;
            let (l, r) = if swap { (*b, *a) } else { (*a, *b) };
            let reg = eb.emit(L::StrColCol {
                op: prim,
                col_a: l as u32,
                col_b: r as u32,
            });
            return Some(if negate { eb.emit(L::BoolNot { a: reg }) } else { reg });
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

/// Lowers a `BoundExpr` to `LogicalInstr`s for the server-side circuit.
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
    fn lower(&mut self, expr: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        match expr {
            BoundExpr::ColRef(c) => self.col_ref(*c),
            BoundExpr::LitInt(v) => Ok((self.eb.emit(L::LoadConst { val: *v }), ExprKind::Int)),
            BoundExpr::LitFloat(v) => Ok((
                self.eb.emit(L::LoadConst {
                    val: v.to_bits() as i64,
                }),
                ExprKind::Float,
            )),
            BoundExpr::LitStr(s) => {
                let idx = self.eb.add_const_string(s.clone());
                Ok((self.eb.emit(L::LoadConstStr { const_idx: idx }), ExprKind::Str))
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
            BoundExpr::NullTest { inner, want_null } => self.null_test(inner, *want_null),
            BoundExpr::AggCall { .. } => Err(GnitzSqlError::Unsupported(
                "aggregate function not allowed in expression context".to_string(),
            )),
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
    /// Every numeric operand is read through here, which is what turns a string
    /// in an arithmetic position into a SQL error instead of an engine-side
    /// `RegClassMismatch`.
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

    /// Lower `expr` and, if `want_float`, lift an integer register to f64. The
    /// one place an operand is coerced into a node's unified numeric domain.
    fn lower_as(&mut self, expr: &BoundExpr, want_float: bool) -> Result<Reg, GnitzSqlError> {
        let (r, is_float) = self.lower_num(expr)?;
        Ok(if want_float && !is_float {
            self.eb.emit(L::IntToFloat { a: r })
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

    fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let op = match f {
            NumFunc::Round(n) => return self.round(n, arg),
            NumFunc::Unary(op) => op,
        };
        let (r, is_float) = self.lower_num(arg)?;
        let arg_ty = arg.infer_type(self.cols);
        if !is_float && f.result_type(arg_ty) != TypeCode::F64 {
            // The integer domain: the rounding family is the identity, ABS of an
            // unsigned register likewise (the value is non-negative by
            // definition), and only NEG, signed ABS and SIGN compute.
            let int_op = match op {
                FloatUnaryOp::Neg => Some(IntUnaryOp::Neg),
                FloatUnaryOp::Abs if arg_ty.is_signed_int() => Some(IntUnaryOp::Abs),
                FloatUnaryOp::Sign => Some(IntUnaryOp::Sign),
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
        Ok((self.eb.emit(L::FloatUnary { op, a }), ExprKind::Float))
    }

    fn str_call(&mut self, f: StrFunc, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let mut regs = Vec::with_capacity(args.len());
        for (k, (kind, a)) in f.signature().iter().zip(args).enumerate() {
            regs.push(match kind {
                StrArg::Str | StrArg::StrOr(_) => self.str_operand(a, /* coerce_numeric = */ false)?,
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
            (StrFunc::Right, &[src, n_reg]) => L::StrSide {
                src,
                n_reg,
                left: false,
            },
            (StrFunc::Pos, &[hay, needle]) => L::StrPos { hay, needle },
            (StrFunc::Replace, &[s, from, to]) => L::StrReplace { s, from, to },
            (StrFunc::Lpad, &[s, n_reg, fill]) => L::StrPad {
                s,
                n_reg,
                fill,
                left: true,
            },
            (StrFunc::Rpad, &[s, n_reg, fill]) => L::StrPad {
                s,
                n_reg,
                fill,
                left: false,
            },
            (StrFunc::SplitPart, &[s, delim, n_reg]) => L::StrSplitPart { s, delim, n_reg },
            _ => unreachable!("the binder sizes a string call's arguments by its signature"),
        };
        // Class taken from the one result-type statement, not restated here.
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
        Ok((
            self.eb.emit(L::StrSubstr {
                src,
                start_reg,
                len_reg,
            }),
            ExprKind::Str,
        ))
    }

    fn trim_call(&mut self, s: &BoundExpr, mode: TrimMode, set: &str) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let src = self.str_operand(s, false)?;
        let set_idx = self.eb.add_const_string(set.to_string());
        Ok((self.eb.emit(L::StrTrim { a: src, mode, set_idx }), ExprKind::Str))
    }

    /// `s [I]LIKE 'pattern'`. The result is a boolean, so `NOT LIKE`'s `unop`
    /// reads it like any other.
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
        Ok((
            self.eb.emit(L::StrLike {
                src,
                escape,
                pat_idx,
                ci,
            }),
            ExprKind::Int,
        ))
    }

    /// `CONCAT(args…)`. Seeding the fold with the empty string gives every arity
    /// one shape and makes the one-argument case non-NULL.
    ///
    /// The fold is strictly left, which is what makes `str_concat_nn`'s
    /// asymmetric null rule sound: every argument lands in the `b` operand, so a
    /// NULL argument contributes the empty string while the accumulator's own
    /// NULL — the engine's length-overflow verdict — propagates to the result.
    fn concat_n(&mut self, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let empty = self.eb.add_const_string(String::new());
        let mut acc = self.eb.emit(L::LoadConstStr { const_idx: empty });
        for a in args {
            let r = self.str_operand(a, /* coerce_numeric = */ true)?;
            acc = self
                .eb
                .emit(L::StrConcat { a: acc, b: r, skip_null: /* skip_null = */ true });
        }
        Ok((acc, ExprKind::Str))
    }

    /// `ROUND(x, n)`. The scale is applied here rather than in the binder because
    /// only lowering knows the argument's type: rounding an integer to a
    /// non-negative scale is the identity, and taking it through f64 instead
    /// would mangle any magnitude past 2^53. Scaling loads the positive power
    /// `10^|n|` in both directions — negative powers of ten are not f64-exact.
    fn round(&mut self, n: i8, arg: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        if n >= 0 && !arg.infer_type(self.cols).is_float() {
            let (r, is_float) = self.lower_num(arg)?;
            return Ok((r, ExprKind::num(is_float)));
        }
        let mut v = self.lower_as(arg, true)?;
        let scale = self.eb.emit(L::LoadConst {
            val: 10f64.powi(n.unsigned_abs() as i32).to_bits() as i64,
        });
        // Multiply first for n > 0, divide first for n < 0; then undo.
        let up = n > 0;
        let (first, undo) = if up {
            (FloatArithOp::Mul, FloatArithOp::Div)
        } else {
            (FloatArithOp::Div, FloatArithOp::Mul)
        };
        v = self.eb.emit(L::FloatArith {
            op: first,
            a: v,
            b: scale,
        });
        v = self.eb.emit(L::FloatUnary {
            op: FloatUnaryOp::Round,
            a: v,
        });
        Ok((
            self.eb.emit(L::FloatArith {
                op: undo,
                a: v,
                b: scale,
            }),
            ExprKind::Float,
        ))
    }

    /// GREATEST/LEAST as a left fold of 2-ary extremum opcodes. A non-numeric
    /// argument needs no check here: `lower_as` reads every argument through
    /// `lower_num`, which rejects a string, and a wide column dies at `col_ref`.
    fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
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
                (true, is_max) => self.eb.emit(L::FloatMinMax2 { a: acc, b: r, is_max }),
                (false, is_max) => self.eb.emit(L::IntMinMax2 { a: acc, b: r, is_max }),
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
    fn cast(&mut self, expr: &BoundExpr, to: TypeCode) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let (r, kind) = self.lower(expr)?;
        if kind == ExprKind::Str {
            return Ok(match to {
                TypeCode::String => (r, ExprKind::Str),
                TypeCode::F64 => (self.eb.emit(L::StrToFloat { a: r }), ExprKind::Float),
                TypeCode::F32 => {
                    let f = self.eb.emit(L::StrToFloat { a: r });
                    (self.eb.emit(L::FloatToF32 { a: f }), ExprKind::Float)
                }
                _ => (
                    self.eb.emit(L::StrToInt {
                        a: r,
                        fi: int_cast_target(to)?,
                    }),
                    ExprKind::Int,
                ),
            });
        }
        if to == TypeCode::String {
            let reg = if kind == ExprKind::Float {
                self.eb.emit(L::FloatToStr { a: r })
            } else {
                self.eb.emit(L::IntToStr { a: r })
            };
            return Ok((reg, ExprKind::Str));
        }
        if to.is_float() {
            // `lower_as(expr, true)` is "lower once, then int_to_float if the
            // register is an int", so hoisting the lower emits the identical
            // sequence.
            let f = if kind == ExprKind::Int {
                self.eb.emit(L::IntToFloat { a: r })
            } else {
                r
            };
            // F64 needs nothing: the register already holds an f64.
            let reg = if to == TypeCode::F32 {
                self.eb.emit(L::FloatToF32 { a: f })
            } else {
                f
            };
            return Ok((reg, ExprKind::Float));
        }
        if kind == ExprKind::Float {
            return Ok((
                self.eb.emit(L::FloatToInt {
                    a: r,
                    fi: int_cast_target(to)?,
                }),
                ExprKind::Int,
            ));
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
            Ok((
                self.eb.emit(L::IntCast {
                    a: r,
                    fi: int_cast_target(to)?,
                }),
                ExprKind::Int,
            ))
        }
    }

    fn col_ref(&mut self, idx: usize) -> Result<(Reg, ExprKind), GnitzSqlError> {
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
            return Ok((self.eb.emit(L::LoadColStr { col: idx as u32 }), ExprKind::Str));
        }
        if tc.is_float() {
            return Ok((self.eb.emit(L::LoadColFloat { col: idx as u32 }), ExprKind::Float));
        }
        Ok((self.eb.emit(L::LoadColInt { col: idx as u32 }), ExprKind::Int))
    }

    /// A scalar NULL: LoadNull carries a zero i64 lane with the null bit set. If
    /// a sibling CASE branch is float, `case` lifts it; a *string* context takes
    /// `str_operand`'s `load_null_str` path instead and never reaches here.
    fn lit_null(&mut self) -> (Reg, ExprKind) {
        (self.eb.emit(L::LoadNull), ExprKind::Int)
    }

    fn case(
        &mut self,
        branches: &[(BoundExpr, BoundExpr)],
        else_: Option<&BoundExpr>,
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
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
        let lift = |eb: &mut ExprBuilder, reg: Reg, is_float: bool| -> Reg {
            if any_float && !is_float {
                eb.emit(L::IntToFloat { a: reg })
            } else {
                reg
            }
        };
        // Fold right-to-left: acc = else; per pair acc = select(cond, result, acc),
        // so the first truthy WHEN wins.
        let mut acc = lift(self.eb, else_out.0, else_out.1);
        for i in (0..branches.len()).rev() {
            let result_reg = lift(self.eb, results[i].0, results[i].1);
            acc = self.eb.emit(L::Select {
                cond: conds[i],
                a: result_reg,
                b: acc,
            });
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
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        let mut conds = Vec::with_capacity(branches.len());
        let mut results = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            let (cond_reg, _) = self.lower_num(cond)?;
            conds.push(cond_reg);
            results.push(self.str_operand(result, false)?);
        }
        let mut acc = match else_ {
            Some(e) => self.str_operand(e, false)?,
            None => self.eb.emit(L::LoadNullStr),
        };
        // Right-to-left, so the first truthy WHEN wins — `case`'s fold.
        for i in (0..branches.len()).rev() {
            acc = self.eb.emit(L::StrSelect {
                cond: conds[i],
                a: results[i],
                b: acc,
            });
        }
        Ok((acc, ExprKind::Str))
    }

    fn in_list(&mut self, inner: &BoundExpr, items: &[BoundExpr]) -> Result<(Reg, ExprKind), GnitzSqlError> {
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
                return Ok((
                    self.eb.emit(L::IntInSet {
                        value_reg: reg,
                        set_idx: idx,
                    }),
                    ExprKind::Int,
                ));
            }
        }
        // Fallback: fold `inner = item` per item (float operand → int_to_float
        // + fcmp; non-literal item → column compare). Folded iteratively rather
        // than built as a left-nested OR tree: `lower` descends the left spine
        // before allocating any register, so a long list overflowed the stack
        // before `MAX_REGS` could reject it.
        //
        // Two registers per item is this fold's floor, so a list that cannot fit
        // is rejected by its length. Below the `INT_IN_SET` return above, which
        // compiles any length into three registers.
        if items.len() * 2 > gnitz_expr::MAX_REGS {
            return Err(GnitzSqlError::Unsupported(format!(
                "IN list of {} items needs more than the {} expression registers",
                items.len(),
                gnitz_expr::MAX_REGS,
            )));
        }
        // Each term lowers the operand again; the builder folds the identical
        // instructions, so a non-fused operand is loaded once for the whole list.
        let (mut acc, _) = self.binop(inner, BinOp::Eq, &items[0])?;
        for it in &items[1..] {
            let (r, _) = self.binop(inner, BinOp::Eq, it)?;
            acc = self.eb.emit(L::BoolBinary {
                a: acc,
                b: r,
                is_or: true,
            });
        }
        Ok((acc, ExprKind::Int))
    }

    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
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
                self.eb.emit(L::BoolBinary {
                    a: l,
                    b: r,
                    is_or: false,
                })
            } else {
                self.eb.emit(L::BoolBinary {
                    a: l,
                    b: r,
                    is_or: true,
                })
            };
            return Ok((reg, ExprKind::Int));
        }

        // `||` reads both operands through the string channel, so `s || NULL` is
        // a NULL string rather than a type error. There is no implicit numeric
        // cast on the operator, unlike CONCAT.
        if matches!(op, BinOp::Concat) {
            let l = self.str_operand(left, false)?;
            let r = self.str_operand(right, false)?;
            return Ok((
                self.eb
                    .emit(L::StrConcat { a: l, b: r, skip_null: /* skip_null = */ false }),
                ExprKind::Str,
            ));
        }

        let l = self.lower(left)?;
        let r = self.lower(right)?;
        self.binop_lowered(op, l, r)
    }

    /// [`Self::binop`] from the point both operands are in registers — the entry
    /// a caller uses when it has lowered one of them itself. `op` is neither
    /// AND/OR nor `||`: those return above, before any operand is lowered.
    fn binop_lowered(
        &mut self,
        op: BinOp,
        (mut l, l_kind): (Reg, ExprKind),
        (mut r, r_kind): (Reg, ExprKind),
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
        if l_kind == ExprKind::Str || r_kind == ExprKind::Str {
            return self.str_binop(op, (l, l_kind), (r, r_kind));
        }
        let (l_float, r_float) = (l_kind == ExprKind::Float, r_kind == ExprKind::Float);
        // POWER has no integer form: both operands lift.
        let is_float = l_float || r_float || matches!(op, BinOp::Pow);

        // Cast int operand to float if mixed
        if is_float && !l_float {
            l = self.eb.emit(L::IntToFloat { a: l });
        }
        if is_float && !r_float {
            r = self.eb.emit(L::IntToFloat { a: r });
        }

        // Arithmetic and comparison alike carry their operator as data, so the
        // float/int choice is the only thing left to branch on. Float modulo is
        // the one hole — `as_float_arith` has no `Mod` — and is rejected before
        // the arithmetic arms, so it cannot fall through to the compare arm.
        if is_float && matches!(op, BinOp::Mod) {
            return Err(GnitzSqlError::Unsupported("float modulo not supported".to_string()));
        }
        if is_float {
            if let Some(fop) = op.as_float_arith() {
                return Ok((self.eb.emit(L::FloatArith { op: fop, a: l, b: r }), ExprKind::Float));
            }
        } else if let Some(iop) = op.as_int_arith() {
            return Ok((self.eb.emit(L::IntArith { op: iop, a: l, b: r }), ExprKind::Int));
        }
        match op {
            // Comparisons — the result is always int (0/1).
            BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Ge | BinOp::Lt | BinOp::Le => {
                let cmp = op.as_cmp().expect("this arm lists exactly `as_cmp`'s `Some` set");
                let reg = if is_float {
                    self.eb.emit(L::FCmp { op: cmp, a: l, b: r })
                } else {
                    self.eb.emit(L::Cmp { op: cmp, a: l, b: r })
                };
                Ok((reg, ExprKind::Int))
            }
            // Handled above, before the operands were lowered.
            BinOp::And | BinOp::Or | BinOp::Concat => unreachable!(),
            // Every arithmetic operator returned above.
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod | BinOp::Pow => unreachable!(),
        }
    }

    /// A binary operator with at least one string operand, having already ruled
    /// out `||` and the column/literal fast path. Only the six comparisons are
    /// defined, and both sides must be strings — a comparison carries no
    /// implicit cast.
    fn str_binop(
        &mut self,
        op: BinOp,
        (l, l_kind): (Reg, ExprKind),
        (r, r_kind): (Reg, ExprKind),
    ) -> Result<(Reg, ExprKind), GnitzSqlError> {
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
        let reg = self.eb.emit(L::StrCmp { op: prim, a: l, b: r });
        Ok((
            if negate {
                self.eb.emit(L::BoolNot { a: reg })
            } else {
                reg
            },
            ExprKind::Int,
        ))
    }

    fn unop(&mut self, op: UnaryOp, inner: &BoundExpr) -> Result<(Reg, ExprKind), GnitzSqlError> {
        match op {
            UnaryOp::Neg => self.func(NumFunc::Unary(FloatUnaryOp::Neg), inner),
            UnaryOp::Not => {
                let (a, _) = self.lower_num(inner)?;
                Ok((self.eb.emit(L::BoolNot { a }), ExprKind::Int))
            }
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
) -> Result<Reg, GnitzSqlError> {
    OpcodeBackend { cols, eb }.lower(expr).map(|(reg, _)| reg)
}

/// Compile a standalone BoundExpr into a finished `LogicalProgram` (fresh
/// `ExprBuilder`, result register wired up). The one-shot form behind every
/// WHERE/HAVING/residual filter that needs a whole program rather than a
/// register threaded into a larger one.
pub(crate) fn compile_bound_expr_to_program(
    expr: &BoundExpr,
    cols: &[ColumnDef],
) -> Result<LogicalProgram, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let reg = compile_bound_expr(expr, cols, &mut eb)?;
    eb.build(Some(reg)).map_err(expr_unsupported)
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
) -> Result<Option<LogicalProgram>, GnitzSqlError> {
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
) -> Result<Option<Reg>, GnitzSqlError> {
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
        acc = backend.eb.emit(L::BoolBinary {
            a: acc,
            b: r,
            is_or: false,
        });
    }
    Ok(Some(acc))
}

/// The wire predicate blob for the AND of `exprs` — the conjunct-list form of
/// [`compile_wire_predicate`]. An empty blob is the statically-true verdict (the
/// caller's bound is exact).
pub(crate) fn compile_wire_conjuncts(exprs: &[&BoundExpr], cols: &[ColumnDef]) -> Result<Vec<u8>, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    match lower_conjuncts(exprs, cols, &mut eb)? {
        Some(reg) => Ok(eb.build(Some(reg)).map_err(expr_unsupported)?.to_blob_bytes()),
        None => Ok(Vec::new()),
    }
}

/// Compile a WHERE/HAVING/residual conjunct list into the shared evaluator,
/// AND-combined by [`lower_conjuncts`] and resolved against the schema the rows
/// it will run over carry. `None` when there is nothing to test — no conjuncts,
/// or a statically-true one — and the caller keeps every row.
///
/// Picks `resolve_filter`, so a bare non-boolean predicate (`HAVING COUNT(*)`)
/// still gets the `bool_bits` bit [`Evaluator::filter_ranges`] reads.
pub(crate) fn compile_conjuncts_evaluator(
    preds: &[&BoundExpr],
    schema: &Schema,
) -> Result<Option<Evaluator>, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let Some(reg) = lower_conjuncts(preds, &schema.columns, &mut eb)? else {
        return Ok(None);
    };
    let prog = eb.build(Some(reg)).map_err(expr_unsupported)?;
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
    eb.build(Some(reg))
        .map_err(expr_unsupported)?
        .resolve_scalar(schema)
        .map_err(expr_unsupported)
}

/// Compile a grouped SELECT's finalize item — an expression over the reduce
/// output — into the shared evaluator, read back one group at a time by
/// `exec::agg_finish`.
///
/// The sibling of [`compile_scalar_evaluator`] without its float rejection: a
/// SET writes its result into an existing column, where a float register has no
/// legal destination, while a finalize result *defines* its output column and
/// AVG's is an F64 by construction. Whether the result is a string, and so which
/// read-back applies, the caller asks the returned evaluator
/// (`Evaluator::result_is_str`).
pub(crate) fn compile_finalize_evaluator(expr: &BoundExpr, schema: &Schema) -> Result<Evaluator, GnitzSqlError> {
    compile_bound_expr_to_program(expr, &schema.columns)?
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
#[path = "tests/expr_lower.rs"]
mod tests;
