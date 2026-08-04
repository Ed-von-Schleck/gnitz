use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, NumFunc, UnaryOp};
use gnitz_core::{ColumnDef, ExprBuilder, Schema, TypeCode};
use gnitz_expr::{Evaluator, ExprValidateErr, LogicalProgram};

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

/// The OR-chain fallback for a non-integer / non-literal `IN`:
/// `inner = i0 OR inner = i1 OR …`. `items` must be non-empty (the binder rejects
/// `IN ()`).
fn in_list_or_chain(inner: &BoundExpr, items: &[BoundExpr]) -> BoundExpr {
    let eq = |it: &BoundExpr| BoundExpr::BinOp(Box::new(inner.clone()), BinOp::Eq, Box::new(it.clone()));
    let mut chain = eq(&items[0]);
    for it in &items[1..] {
        chain = BoundExpr::BinOp(Box::new(chain), BinOp::Or, Box::new(eq(it)));
    }
    chain
}

/// Try to compile a string comparison (col vs const, const vs col, col vs col).
/// Returns Some((reg, false)) if this is a string comparison, None otherwise.
fn try_compile_string_cmp(
    left: &BoundExpr,
    op: &BinOp,
    right: &BoundExpr,
    cols: &[ColumnDef],
    eb: &mut ExprBuilder,
) -> Result<Option<(u32, bool)>, GnitzSqlError> {
    // ColRef(string) op LitStr(s), or LitStr(s) op ColRef(string).
    // For the literal-on-left form we swap operands and transpose the
    // comparison so a single `col <cmp> 'lit'` dispatch covers both:
    //   'A' > col  ↔  col < 'A'      'A' >= col ↔ col <= 'A'
    //   'A' < col  ↔  col > 'A'      'A' <= col ↔ col >= 'A'
    // Eq/Ne are symmetric. `cmp` (the transposed op) drives only the register
    // dispatch; Unsupported errors still report the original `op`.
    let col_lit = match (left, right) {
        (BoundExpr::ColRef(idx), BoundExpr::LitStr(s)) => Some((*idx, s, *op)),
        (BoundExpr::LitStr(s), BoundExpr::ColRef(idx)) => Some((
            *idx,
            s,
            match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Gt => BinOp::Lt,
                BinOp::Le => BinOp::Ge,
                BinOp::Ge => BinOp::Le,
                other => *other, // Eq/Ne symmetric; rest fall through to Unsupported
            },
        )),
        _ => None,
    };
    if let Some((idx, s, cmp)) = col_lit {
        // STRING and BLOB share the 16-byte German-string layout and the engine's
        // `str_col_*` opcodes content-compare both (via `compare_german_strings`),
        // so a BLOB column-vs-literal comparison lowers here too, not to the
        // integer path (which would read the descriptor bytes as a garbage int).
        if cols[idx].type_code.is_german_string() {
            let const_idx = eb.add_const_string(s.clone());
            let reg = match cmp {
                BinOp::Eq => eb.str_col_eq_const(idx, const_idx),
                BinOp::Ne => {
                    let r = eb.str_col_eq_const(idx, const_idx);
                    eb.bool_not(r)
                }
                BinOp::Lt => eb.str_col_lt_const(idx, const_idx),
                BinOp::Le => eb.str_col_le_const(idx, const_idx),
                BinOp::Gt => {
                    let r = eb.str_col_le_const(idx, const_idx);
                    eb.bool_not(r)
                }
                BinOp::Ge => {
                    let r = eb.str_col_lt_const(idx, const_idx);
                    eb.bool_not(r)
                }
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "operator {op:?} not supported for strings/blobs"
                    )))
                }
            };
            return Ok(Some((reg, false)));
        }
    }
    // ColRef(string/blob) op ColRef(string/blob)
    if let (BoundExpr::ColRef(a), BoundExpr::ColRef(b)) = (left, right) {
        if cols[*a].type_code.is_german_string() && cols[*b].type_code.is_german_string() {
            let reg = match op {
                BinOp::Eq => eb.str_col_eq_col(*a, *b),
                BinOp::Ne => {
                    let r = eb.str_col_eq_col(*a, *b);
                    eb.bool_not(r)
                }
                BinOp::Lt => eb.str_col_lt_col(*a, *b),
                BinOp::Le => eb.str_col_le_col(*a, *b),
                BinOp::Gt => eb.str_col_lt_col(*b, *a),
                BinOp::Ge => eb.str_col_le_col(*b, *a),
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "operator {op:?} not supported for strings/blobs"
                    )))
                }
            };
            return Ok(Some((reg, false)));
        }
    }
    Ok(None)
}

/// Lowers a `BoundExpr` to `ExprProgram` opcodes for the server-side circuit.
/// Every node produces `(result_reg, is_float)`, where `is_float` indicates the
/// register holds an f64 bit-pattern rather than a plain i64.
///
/// The single `match` in [`OpcodeBackend::lower`] is the sole walk of the enum:
/// adding a `BoundExpr` variant makes it non-exhaustive and fails compilation.
/// `binop` / `case` / `in_list` receive their operands *unevaluated*
/// (`&BoundExpr`) and drive the recursion themselves, which `binop` needs: it
/// intercepts a string comparison before recursing into a string literal or
/// column, both of which `lower` would otherwise reject.
pub(crate) struct OpcodeBackend<'a> {
    cols: &'a [ColumnDef],
    eb: &'a mut ExprBuilder,
}

impl OpcodeBackend<'_> {
    /// Lower one node. The lone `match` over `BoundExpr`. Arms that are a single
    /// builder call live here; only the four that recurse (`binop`, `case`,
    /// `in_list`, `unop`) and `col_ref`'s type gate have their own method.
    fn lower(&mut self, expr: &BoundExpr) -> Result<(u32, bool), GnitzSqlError> {
        match expr {
            BoundExpr::ColRef(c) => self.col_ref(*c),
            BoundExpr::LitInt(v) => Ok((self.eb.load_const(*v), false)),
            BoundExpr::LitFloat(v) => Ok((self.eb.load_const(v.to_bits() as i64), true)),
            BoundExpr::LitStr(_) => Err(GnitzSqlError::Unsupported(
                "string literals not supported in expressions".to_string(),
            )),
            // A wide-integer literal has no VM slot (the register file is 8 bytes
            // wide), so one reject arm here states the limitation for every
            // expression that reaches this walk. A *servable* wide seek is
            // consumed into a PK/index bound by the access-path recognizer and
            // never gets here.
            BoundExpr::LitWide(s) => Err(crate::ir::wide_int_error(s)),
            BoundExpr::LitNull => Ok(self.lit_null()),
            BoundExpr::BinOp(l, op, r) => self.binop(l, *op, r),
            BoundExpr::UnaryOp(op, inner) => self.unop(*op, inner),
            BoundExpr::IsNull(c) => Ok((self.eb.is_null(*c), false)),
            BoundExpr::IsNotNull(c) => Ok((self.eb.is_not_null(*c), false)),
            BoundExpr::AggCall { .. } => Err(GnitzSqlError::Unsupported(
                "aggregate function not allowed in expression context".to_string(),
            )),
            BoundExpr::Case { branches, else_ } => self.case(branches, else_.as_deref()),
            BoundExpr::InList { inner, items } => self.in_list(inner, items),
            BoundExpr::Func { f, arg } => self.func(*f, arg),
            BoundExpr::MinMaxN { is_max, args } => self.min_max_n(*is_max, args),
            BoundExpr::Cast { expr, to } => self.cast(expr, *to),
        }
    }

    /// Lower `expr` and, if `want_float`, lift an integer register to f64. The
    /// one place an operand is coerced into a node's unified numeric domain.
    fn lower_as(&mut self, expr: &BoundExpr, want_float: bool) -> Result<u32, GnitzSqlError> {
        let (r, is_float) = self.lower(expr)?;
        Ok(if want_float && !is_float {
            self.eb.int_to_float(r)
        } else {
            r
        })
    }

    fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<(u32, bool), GnitzSqlError> {
        if let NumFunc::Round(n) = f {
            return self.round(n, arg);
        }
        let (r, is_float) = self.lower(arg)?;
        if !is_float {
            // Every transform is the identity on an integer register, and ABS of
            // an unsigned one likewise — the value is non-negative by definition.
            let needs_abs = f == NumFunc::Abs && arg.infer_type(self.cols).is_signed_int();
            return Ok((if needs_abs { self.eb.int_abs(r) } else { r }, false));
        }
        let reg = match f {
            NumFunc::Abs => self.eb.float_abs(r),
            NumFunc::Floor => self.eb.float_floor(r),
            NumFunc::Ceil => self.eb.float_ceil(r),
            NumFunc::Trunc => self.eb.float_trunc(r),
            NumFunc::Round(_) => unreachable!("routed to `round` above"),
        };
        Ok((reg, true))
    }

    /// `ROUND(x, n)`. The scale is applied here rather than in the binder because
    /// only lowering knows the argument's type: rounding an integer to a
    /// non-negative scale is the identity, and taking it through f64 instead
    /// would mangle any magnitude past 2^53. Scaling loads the positive power
    /// `10^|n|` in both directions — negative powers of ten are not f64-exact.
    fn round(&mut self, n: i8, arg: &BoundExpr) -> Result<(u32, bool), GnitzSqlError> {
        if n >= 0 && !arg.infer_type(self.cols).is_float() {
            return self.lower(arg);
        }
        let mut v = self.lower_as(arg, true)?;
        if n == 0 {
            return Ok((self.eb.float_round(v), true));
        }
        let scale = self.eb.load_const(10f64.powi(n.unsigned_abs() as i32).to_bits() as i64);
        // Multiply first for n > 0, divide first for n < 0; then undo.
        let up = n > 0;
        v = if up {
            self.eb.float_mul(v, scale)
        } else {
            self.eb.float_div(v, scale)
        };
        v = self.eb.float_round(v);
        Ok((
            if up {
                self.eb.float_div(v, scale)
            } else {
                self.eb.float_mul(v, scale)
            },
            true,
        ))
    }

    /// GREATEST/LEAST as a left fold of 2-ary extremum opcodes. A non-numeric
    /// argument needs no check here: the recursion rejects it (a wide/string
    /// column at `col_ref`, a string literal at `lower`).
    fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<(u32, bool), GnitzSqlError> {
        let types: Vec<TypeCode> = args.iter().map(|a| a.infer_type(self.cols)).collect();
        let unified = types
            .iter()
            .fold(TypeCode::I64, |acc, &t| crate::ir::unify_numeric(acc, t));
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
                (true, true) => self.eb.float_max2(acc, r),
                (true, false) => self.eb.float_min2(acc, r),
                (false, true) => self.eb.int_max2(acc, r),
                (false, false) => self.eb.int_min2(acc, r),
            };
        }
        Ok((acc, any_float))
    }

    fn cast(&mut self, expr: &BoundExpr, to: TypeCode) -> Result<(u32, bool), GnitzSqlError> {
        if to.is_float() {
            let f = self.lower_as(expr, true)?;
            // F64 needs nothing: the register already holds an f64.
            return Ok((
                if to == TypeCode::F32 {
                    self.eb.float_to_f32(f)
                } else {
                    f
                },
                true,
            ));
        }
        let (r, src_float) = self.lower(expr)?;
        if src_float {
            return Ok((self.eb.float_to_int(r, to), false));
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
            Ok((r, false))
        } else {
            Ok((self.eb.int_cast(r, to), false))
        }
    }

    fn col_ref(&mut self, idx: usize) -> Result<(u32, bool), GnitzSqlError> {
        // Every 16-byte column must be rejected here: the engine's payload integer
        // load handler has arms only for 1/2/4/8-byte columns, so a 16-byte column
        // hits its no-op arm and the following op reads stale scratch bytes —
        // silent corruption, no error. A *valid* string/blob use
        // is one of the six comparisons, which `binop` intercepts via
        // `try_compile_string_cmp` before any recursion reaches this arm; so a
        // STRING/BLOB landing here is arithmetic or a mixed-type comparison
        // (`a.s > b.int`) and must error, not load garbage.
        let tc = self.cols[idx].type_code;
        if tc.is_wide_int() {
            return Err(GnitzSqlError::Unsupported(format!(
                "column {:?} is {tc:?}; 128-bit columns cannot be used in expressions",
                self.cols[idx].name,
            )));
        }
        if tc.is_german_string() {
            return Err(GnitzSqlError::Unsupported(format!(
                "column {:?} is {tc:?}; string/blob columns support only =, <>, <, <=, \
                 >, >= against another string/blob column or a string literal — not \
                 arithmetic or comparison with a non-string column",
                self.cols[idx].name,
            )));
        }
        if tc.is_float() {
            return Ok((self.eb.load_col_float(idx), true));
        }
        Ok((self.eb.load_col_int(idx), false))
    }

    /// A NULL value: `is_float = false` — LoadNull carries a zero i64 lane with
    /// the null bit set. If a sibling CASE branch is float, `case` lifts it.
    /// A method rather than an inline arm because `case` also needs it, for the
    /// implicit ELSE.
    fn lit_null(&mut self) -> (u32, bool) {
        (self.eb.load_null(), false)
    }

    fn case(
        &mut self,
        branches: &[(BoundExpr, BoundExpr)],
        else_: Option<&BoundExpr>,
    ) -> Result<(u32, bool), GnitzSqlError> {
        // Lower every condition and result, plus the else (implicit NULL when
        // absent). Conditions stay as 0/1 int registers; only the result *values*
        // participate in float unification.
        let mut conds = Vec::with_capacity(branches.len());
        let mut results = Vec::with_capacity(branches.len());
        for (cond, result) in branches {
            let (cond_reg, _cond_float) = self.lower(cond)?;
            conds.push(cond_reg);
            results.push(self.lower(result)?);
        }
        let else_out = match else_ {
            Some(e) => self.lower(e)?,
            None => self.lit_null(),
        };

        // Float unification is one GLOBAL decision, not a per-pair fold: EXPR_SELECT
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
        Ok((acc, any_float))
    }

    fn in_list(&mut self, inner: &BoundExpr, items: &[BoundExpr]) -> Result<(u32, bool), GnitzSqlError> {
        // Fast path: a ≤8-byte-integer operand + every item a foldable integer
        // literal → one INT_IN_SET. `self.cols` is the schema `inner` was bound
        // against — source schema for a table filter, reduce-output schema for
        // HAVING — so the int gate is correct in both, and a HAVING large-IN
        // compiles here too. Use `is_fixed_int` — the same predicate the VM's
        // `ColKind::FixedInt` check applies — not `is_pk_eligible`, which wrongly
        // admits U128/UUID/I128.
        if gnitz_wire::is_fixed_int(inner.infer_type(self.cols) as u8) {
            if let Some(mut values) = items.iter().map(fold_int_literal).collect::<Option<Vec<i64>>>() {
                values.sort_unstable();
                values.dedup();
                let (reg, _is_float) = self.lower(inner)?; // integer ⇒ not float
                let idx = self.eb.add_const_int_set(&values);
                return Ok((self.eb.int_in_set(reg, idx), false));
            }
        }
        // Fallback: OR-chain via the existing binop path (float operand →
        // int_to_float + fcmp; non-literal item → column compare). Rare,
        // register-limited as today.
        self.lower(&in_list_or_chain(inner, items))
    }

    fn binop(&mut self, left: &BoundExpr, op: BinOp, right: &BoundExpr) -> Result<(u32, bool), GnitzSqlError> {
        // String comparison detection — intercept before recursing into operands
        // (a bare string literal/column would otherwise error in `lit_str`/`col_ref`).
        if let Some(result) = try_compile_string_cmp(left, &op, right, self.cols, self.eb)? {
            return Ok(result);
        }

        let (mut l, l_float) = self.lower(left)?;
        let (mut r, r_float) = self.lower(right)?;

        // Boolean ops never need float cast
        if matches!(op, BinOp::And) {
            return Ok((self.eb.bool_and(l, r), false));
        }
        if matches!(op, BinOp::Or) {
            return Ok((self.eb.bool_or(l, r), false));
        }

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
            (BinOp::Add, false) => Ok((self.eb.add(l, r), false)),
            (BinOp::Add, true) => Ok((self.eb.float_add(l, r), true)),
            (BinOp::Sub, false) => Ok((self.eb.sub(l, r), false)),
            (BinOp::Sub, true) => Ok((self.eb.float_sub(l, r), true)),
            (BinOp::Mul, false) => Ok((self.eb.mul(l, r), false)),
            (BinOp::Mul, true) => Ok((self.eb.float_mul(l, r), true)),
            (BinOp::Div, false) => Ok((self.eb.div(l, r), false)),
            (BinOp::Div, true) => Ok((self.eb.float_div(l, r), true)),
            (BinOp::Mod, false) => Ok((self.eb.modulo(l, r), false)),
            (BinOp::Mod, true) => Err(GnitzSqlError::Unsupported("float modulo not supported".to_string())),
            // Comparisons — result is always int (0/1)
            (BinOp::Eq, false) => Ok((self.eb.cmp_eq(l, r), false)),
            (BinOp::Eq, true) => Ok((self.eb.fcmp_eq(l, r), false)),
            (BinOp::Ne, false) => Ok((self.eb.cmp_ne(l, r), false)),
            (BinOp::Ne, true) => Ok((self.eb.fcmp_ne(l, r), false)),
            (BinOp::Gt, false) => Ok((self.eb.cmp_gt(l, r), false)),
            (BinOp::Gt, true) => Ok((self.eb.fcmp_gt(l, r), false)),
            (BinOp::Ge, false) => Ok((self.eb.cmp_ge(l, r), false)),
            (BinOp::Ge, true) => Ok((self.eb.fcmp_ge(l, r), false)),
            (BinOp::Lt, false) => Ok((self.eb.cmp_lt(l, r), false)),
            (BinOp::Lt, true) => Ok((self.eb.fcmp_lt(l, r), false)),
            (BinOp::Le, false) => Ok((self.eb.cmp_le(l, r), false)),
            (BinOp::Le, true) => Ok((self.eb.fcmp_le(l, r), false)),
            // And/Or handled above
            (BinOp::And, _) | (BinOp::Or, _) => unreachable!(),
        }
    }

    fn unop(&mut self, op: UnaryOp, inner: &BoundExpr) -> Result<(u32, bool), GnitzSqlError> {
        let (a, a_float) = self.lower(inner)?;
        match op {
            UnaryOp::Neg => {
                if a_float {
                    Ok((self.eb.float_neg(a), true))
                } else {
                    Ok((self.eb.neg_int(a), false))
                }
            }
            UnaryOp::Not => Ok((self.eb.bool_not(a), false)),
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
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
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
) -> Result<Option<gnitz_core::ExprProgram>, GnitzSqlError> {
    match pred {
        BoundExpr::LitInt(v) if *v != 0 => Ok(None),
        _ => Ok(Some(compile_bound_expr_to_program(pred, cols)?)),
    }
}

/// [`compile_filter_program`] encoded as the wire predicate blob a `ReadSpec`
/// carries; empty for the statically-true verdict (the bound is exact).
///
/// The program is validated here even though nothing client-side runs it: the
/// worker's `LogicalProgram::from_wire` applies the identical schema-free checks
/// (register cap, opcodes, operand bounds), so doing it locally turns a
/// round-trip `STATUS_ERROR` naming an internal enum into a plan-time
/// `Unsupported`.
pub(crate) fn compile_wire_predicate(pred: &BoundExpr, cols: &[ColumnDef]) -> Result<Vec<u8>, GnitzSqlError> {
    let Some(p) = compile_filter_program(pred, cols)? else {
        return Ok(Vec::new());
    };
    let blob = p.encode();
    to_logical(p)?;
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
    let Some(p) = compile_filter_program(pred, &schema.columns)? else {
        return Ok(None);
    };
    Ok(Some(to_logical(p)?.resolve_filter(schema).map_err(expr_unsupported)?))
}

/// Compile a scalar (non-predicate) RHS — a SET / `DO UPDATE SET` value — into
/// the shared evaluator, resolved against the schema the rows it will run over
/// carry, and reject a float-typed result.
///
/// The float test has to happen here: nothing downstream can tell a float
/// bit-pattern from an integer, so `append_column_value` would store the raw
/// bits into an integer column.
///
/// Picks `resolve_scalar`, because a SET RHS is read through
/// [`Evaluator::eval_row`]. Pairing each resolver with the one way of driving it
/// is why `expr_unsupported` is private to this module.
///
/// It runs the backend itself rather than going through
/// [`compile_bound_expr_to_program`], because it is the one caller that needs
/// the recursion's `is_float` bit — every other one discards it.
pub(crate) fn compile_scalar_evaluator(expr: &BoundExpr, schema: &Schema) -> Result<Evaluator, GnitzSqlError> {
    let mut eb = ExprBuilder::new();
    let (reg, is_float) = OpcodeBackend {
        cols: &schema.columns,
        eb: &mut eb,
    }
    .lower(expr)?;
    if is_float {
        // No target type accepts it: the register holds an f64 bit pattern, and
        // the only column kind a SET value can be written to is a fixed-width
        // integer (or a string, which never reaches this compiler).
        return Err(GnitzSqlError::Unsupported(
            "SET from a floating-point expression is not supported".to_string(),
        ));
    }
    to_logical(eb.build(reg))?
        .resolve_scalar(schema)
        .map_err(expr_unsupported)
}

/// The one place `ExprProgram`'s fields meet [`LogicalProgram::from_wire`]'s
/// parameters. They are byte-for-byte its arguments, so this is a hand-off, not
/// a round trip; the caller then picks the resolver that matches how it will
/// drive the program.
fn to_logical(p: gnitz_core::ExprProgram) -> Result<LogicalProgram, GnitzSqlError> {
    LogicalProgram::from_wire(&p.code, p.num_regs, p.result_reg, p.const_strings).map_err(expr_unsupported)
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
    use gnitz_core::{ColumnDef, ExprProgram, Schema, TypeCode};

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
        let (reg, _) = try_compile_string_cmp(left, &op, right, &schema.columns, &mut eb)
            .expect("compile ok")
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

    /// The opcodes `expr` lowers to, plus whether its result register is a float.
    /// `lower_ops` is the same without the float bit.
    fn lower_ops_isf(expr: &BoundExpr, schema: &Schema) -> (Vec<u32>, bool) {
        let mut eb = ExprBuilder::new();
        let (reg, isf) = OpcodeBackend {
            cols: &schema.columns,
            eb: &mut eb,
        }
        .lower(expr)
        .expect("lowers");
        (opcodes(&eb.build(reg)), isf)
    }

    fn lower_ops(expr: &BoundExpr, schema: &Schema) -> Vec<u32> {
        lower_ops_isf(expr, schema).0
    }

    /// The float opcodes a fold-to-identity must never emit.
    const FLOAT_OPS: [u32; 6] = [
        gnitz_wire::EXPR_INT_TO_FLOAT,
        gnitz_wire::EXPR_FLOAT_MUL,
        gnitz_wire::EXPR_FLOAT_DIV,
        gnitz_wire::EXPR_FLOAT_ROUND,
        gnitz_wire::EXPR_FLOAT_FLOOR,
        gnitz_wire::EXPR_FLOAT_CEIL,
    ];

    fn cast_emits(expr: &BoundExpr, to: TypeCode, schema: &Schema) -> bool {
        let cast = BoundExpr::Cast {
            expr: Box::new(expr.clone()),
            to,
        };
        lower_ops(&cast, schema).contains(&gnitz_wire::EXPR_INT_CAST)
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
        assert!(ops.contains(&gnitz_wire::EXPR_FLOAT_ROUND), "negative scale rounds");
    }

    /// n == 0 is the bare 1-arg ROUND opcode, not a scale-by-1 round trip.
    #[test]
    fn round_scaled_zero_is_plain_round() {
        let s = cast_schema();
        let (ops, _) = round_ops(0, &BoundExpr::LitFloat(2.5), &s);
        assert!(ops.contains(&gnitz_wire::EXPR_FLOAT_ROUND));
        assert!(!ops.contains(&gnitz_wire::EXPR_FLOAT_MUL));
        assert!(!ops.contains(&gnitz_wire::EXPR_FLOAT_DIV));
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
                        gnitz_wire::EXPR_FLOAT_MUL,
                        gnitz_wire::EXPR_FLOAT_DIV,
                        gnitz_wire::EXPR_FLOAT_ROUND,
                    ]
                    .contains(op)
                })
                .collect()
        };
        assert_eq!(
            ops(2),
            vec![
                gnitz_wire::EXPR_FLOAT_MUL,
                gnitz_wire::EXPR_FLOAT_ROUND,
                gnitz_wire::EXPR_FLOAT_DIV
            ]
        );
        assert_eq!(
            ops(-2),
            vec![
                gnitz_wire::EXPR_FLOAT_DIV,
                gnitz_wire::EXPR_FLOAT_ROUND,
                gnitz_wire::EXPR_FLOAT_MUL
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
                !ops.contains(&gnitz_wire::EXPR_INT_ABS),
                "ABS over an unsigned column folds away"
            );
        }
        // A signed integer argument does need the opcode.
        assert!(lower_ops(&func(NumFunc::Abs, BoundExpr::ColRef(1)), &s).contains(&gnitz_wire::EXPR_INT_ABS));
        // A float argument takes the float opcode in every case.
        assert!(lower_ops(&func(NumFunc::Floor, BoundExpr::LitFloat(1.5)), &s).contains(&gnitz_wire::EXPR_FLOAT_FLOOR));
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
                ops.iter().filter(|&&o| o == gnitz_wire::EXPR_INT_MAX2).count(),
                n - 1,
                "arity {n}: n-1 folds"
            );
        }
        assert!(lower_ops(&min_max(false, vec![BoundExpr::ColRef(1); 2]), &s).contains(&gnitz_wire::EXPR_INT_MIN2));
        // One float argument lifts every integer argument and switches the whole
        // fold to the float opcodes — the same global unification CASE applies.
        let mixed = min_max(true, vec![BoundExpr::ColRef(1), BoundExpr::LitFloat(1.5)]);
        let ops = lower_ops(&mixed, &s);
        assert!(ops.contains(&gnitz_wire::EXPR_FLOAT_MAX2));
        assert!(ops.contains(&gnitz_wire::EXPR_INT_TO_FLOAT));
        assert!(!ops.contains(&gnitz_wire::EXPR_INT_MAX2));
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
            assert_eq!(first[0], gnitz_wire::EXPR_LOAD_COL_INT, "the fold opens with a load");
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

        assert!(lower_ops(&cast(f.clone(), TypeCode::I32), &s).contains(&gnitz_wire::EXPR_FLOAT_TO_INT));
        // float -> DOUBLE is a no-op: the register already holds an f64.
        let ops = lower_ops(&cast(f.clone(), TypeCode::F64), &s);
        assert!(!ops.contains(&gnitz_wire::EXPR_FLOAT_TO_F32) && !ops.contains(&gnitz_wire::EXPR_INT_TO_FLOAT));
        assert!(lower_ops(&cast(f, TypeCode::F32), &s).contains(&gnitz_wire::EXPR_FLOAT_TO_F32));

        let ops = lower_ops(&cast(BoundExpr::ColRef(1), TypeCode::F64), &s);
        assert!(ops.contains(&gnitz_wire::EXPR_INT_TO_FLOAT) && !ops.contains(&gnitz_wire::EXPR_FLOAT_TO_F32));
        // int -> FLOAT is the two-step lift; the double rounding is committed.
        let ops = lower_ops(&cast(BoundExpr::ColRef(1), TypeCode::F32), &s);
        assert!(ops.contains(&gnitz_wire::EXPR_INT_TO_FLOAT) && ops.contains(&gnitz_wire::EXPR_FLOAT_TO_F32));
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
        let reg = eb.str_col_lt_col(1, 2);
        assert_eq!(got, eb.build(reg), "a < b must stay str_col_lt_col(a, b)");
    }

    /// An unsupported operator reports the original op in the error message.
    #[test]
    fn string_cmp_unsupported_names_op() {
        let schema = str_schema();
        let s = BoundExpr::ColRef(1);
        let lit = BoundExpr::LitStr("x".to_string());
        let mut eb = ExprBuilder::new();
        let err = try_compile_string_cmp(&lit, &BinOp::Add, &s, &schema.columns, &mut eb)
            .expect_err("Add is not a string comparison");
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
        use gnitz_wire::{EXPR_INT_TO_FLOAT, EXPR_SELECT};
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
        assert!(ops.contains(&EXPR_SELECT), "CASE must lower to a SELECT");
        assert!(!ops.contains(&EXPR_INT_TO_FLOAT), "all-int CASE needs no float lift");

        // Mixed int/float CASE: the int else is lifted to float up front.
        let case_mixed = BoundExpr::Case {
            branches: vec![(gt0(), BoundExpr::ColRef(2))],
            else_: Some(Box::new(BoundExpr::ColRef(1))),
        };
        let ops = opcodes(&compile_bound_expr_to_program(&case_mixed, &schema.columns).unwrap());
        assert!(ops.contains(&EXPR_SELECT), "CASE must lower to a SELECT");
        assert!(
            ops.contains(&EXPR_INT_TO_FLOAT),
            "mixed CASE lifts int branches to float"
        );
    }

    /// A string-typed CASE result reaches the integer/string-load path and is
    /// rejected (the register file carries 8-byte values only).
    #[test]
    fn case_string_branch_rejected() {
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
        assert!(matches!(
            compile_bound_expr_to_program(&case_str, &schema.columns),
            Err(GnitzSqlError::Unsupported(_))
        ));
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
        use gnitz_wire::EXPR_INT_IN_SET;
        let schema = two_int_schema();
        for items in [
            vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2), BoundExpr::LitInt(3)],
            vec![neg_lit(1), neg_lit(2)],
        ] {
            let prog = compile_bound_expr_to_program(&in_list(BoundExpr::ColRef(1), items), &schema.columns).unwrap();
            let ops = opcodes(&prog);
            assert!(
                ops.contains(&EXPR_INT_IN_SET),
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
        use gnitz_wire::{EXPR_FCMP_EQ, EXPR_INT_IN_SET};
        let schema = case_schema(); // col2 = f (F64)
        let prog = compile_bound_expr_to_program(
            &in_list(BoundExpr::ColRef(2), vec![BoundExpr::LitInt(1), BoundExpr::LitInt(2)]),
            &schema.columns,
        )
        .unwrap();
        let ops = opcodes(&prog);
        assert!(!ops.contains(&EXPR_INT_IN_SET), "float IN must not emit INT_IN_SET");
        assert!(ops.contains(&EXPR_FCMP_EQ), "float IN lowers to fcmp OR-chain");
    }

    /// A string operand falls back to the OR-chain (str_col_eq_const).
    #[test]
    fn in_list_string_operand_falls_back_to_or_chain() {
        use gnitz_wire::{EXPR_INT_IN_SET, EXPR_STR_COL_EQ_CONST};
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
        assert!(!ops.contains(&EXPR_INT_IN_SET), "string IN must not emit INT_IN_SET");
        assert!(
            ops.contains(&EXPR_STR_COL_EQ_CONST),
            "string IN lowers to str_col_eq_const"
        );
    }

    /// A non-literal item (a column) forces the OR-chain even for an int operand.
    #[test]
    fn in_list_non_literal_item_falls_back_to_or_chain() {
        use gnitz_wire::{EXPR_CMP_EQ, EXPR_INT_IN_SET};
        let schema = two_int_schema();
        let prog = compile_bound_expr_to_program(
            &in_list(BoundExpr::ColRef(1), vec![BoundExpr::LitInt(1), BoundExpr::ColRef(2)]),
            &schema.columns,
        )
        .unwrap();
        let ops = opcodes(&prog);
        assert!(
            !ops.contains(&EXPR_INT_IN_SET),
            "non-literal item must not emit INT_IN_SET"
        );
        assert!(ops.contains(&EXPR_CMP_EQ), "non-literal item lowers to cmp OR-chain");
    }

    /// A float-literal item forces the OR-chain even for an int operand
    /// (`a IN (1, 2.5)`).
    #[test]
    fn in_list_float_literal_item_falls_back_to_or_chain() {
        use gnitz_wire::EXPR_INT_IN_SET;
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
            !opcodes(&prog).contains(&EXPR_INT_IN_SET),
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
}
