# Numeric scalar functions and numeric CAST

## 1. Problem

The SQL surface has no scalar functions and no CAST. The only callable names are the five
aggregates (`agg_func_from_name`, `crates/gnitz-sql/src/ast_util.rs:91-95`) plus the
COALESCE/NULLIF bind-time desugars (`crates/gnitz-sql/src/bind/structural.rs:72-75`). Every
other function name errors `function '…' not supported` (`ast_util.rs:123-128` via
`classify_agg_call`; `hir/bind.rs:392-407` for the HIR leaf), and `Expr::Cast` / `x::type`
falls into `bind_structural`'s catch-all (`structural.rs:187-189`).

This plan adds the numeric core: `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND(x[, n])`,
`TRUNC(x)`, `MOD(a, b)`, `GREATEST(…)`, `LEAST(…)`, and `CAST(x AS <numeric type>)` /
`x::<numeric type>`. All are pure per-row value transforms — linear map operators in DBSP
terms, incrementally free, no state, no trace. String-typed functions and string CAST are a
separate piece of work (they need a register representation for strings that this plan does
not touch). Deliberately excluded from the surface this plan commits to: regex, math
transcendentals (POWER/SQRT/EXP/LN/LOG), SIGN, and bitwise operators — none of them earns a
slot in the numeric core, and each is a plain register op if ever wanted.

### Prerequisite: `plans/shared-expr-vm.md` lands first

That plan is the reason this one is engine-only on the evaluation side. After it:

- the expr VM lives in **`crates/gnitz-expr/src/{program,batch}.rs`** (a leaf crate over
  `gnitz-wire`); `ScalarFunc`, `EmitCol` and the map/emit kernels stay in
  `crates/gnitz-engine/src/expr/plan.rs`. `LogicalInstr`, `Instr`, `ExprValidateErr` and the
  register analyses are `pub`, so every new variant and enum this plan adds is `pub` too.
- **there is one evaluator.** `crates/gnitz-sql/src/exec/eval.rs` and `HavingEval` are gone;
  `OpcodeBackend` (`lower.rs:191-194`) is the sole `BoundExprBackend` implementor, and DML
  residual filters, ad-hoc aggregate HAVING and `UPDATE … SET` all run the compiled program
  through the shared VM. So every function in this plan reaches every one of those surfaces
  the moment its opcode exists — no second implementation, no per-site capability gate.
  (Whether `BoundExprBackend` is still a trait or has been collapsed to free functions by
  then does not change this plan's work: three arms in the one lowering walk.)
- the client runs `LogicalProgram::validate` before `resolve`, mapping `ExprValidateErr` to
  `GnitzSqlError::Unsupported`, so `TooManyRegs` and this plan's new `BadCastTarget` surface
  as typed SQL errors at CREATE/plan time.

### Global semantic rules (these three rules decide every edge case below)

1. **Total functions, no runtime errors.** Circuit expression evaluation has no error path —
   a view tick cannot abort mid-circuit. The existing precedent is division by zero → NULL
   (`div_like!`, `crates/gnitz-expr/src/batch.rs:475-502`). Every new function follows it: a
   domain error (NaN→int, out-of-range cast, finite f32 overflow) yields SQL NULL, never an
   error and never a clamped value.
2. **Determinism is mandatory.** A retraction re-runs the same expression over the same
   payload and must cancel byte-exactly under (PK, payload) consolidation. Every function here
   is a pure bit-level function of its inputs. Volatile functions (NOW, RANDOM) are
   structurally inadmissible in this engine and are not a "later" item.
3. **Wrapping integer arithmetic.** The engine wraps (`wrapping_add`/`wrapping_neg`,
   `batch.rs:456-471,666-676`); `ABS` follows: `wrapping_abs`, so `ABS(i64::MIN) = i64::MIN`.
   CAST is the exception by design: it is the one operation whose *job* is the range check, so
   an out-of-range cast is NULL (rule 1), not a wrap.

## 2. Function semantics (fully decided)

| Function | Arg type(s) | Result type (`infer_type`) | Semantics |
|---|---|---|---|
| `ABS(x)` | int | `unify_numeric(t,t)` (I64/U64) | `wrapping_abs` on the i64 register. Unsigned-typed arg (U8..U64 per `infer_type`): identity, folded at lowering, no opcode. |
| `ABS(x)` | float | F64 | `f64::abs` (`-0.0 → +0.0`, NaN → NaN). |
| `FLOOR(x)`, `CEIL(x)`, `CEILING(x)` | float | F64 | `f64::floor` / `f64::ceil`. Int arg: identity fold, no opcode. |
| `ROUND(x)` | float | F64 | `f64::round_ties_even` — matches IEEE-754 roundTiesToEven and PostgreSQL `round(float8)` (which uses `rint`); `round(2.5) = 2`. Int arg: identity fold. |
| `ROUND(x, n)` | any numeric, int literal | F64 | `n` must be an integer literal in `-15..=15`, else Bind error. Bind-time desugar: `n ≥ 0` → `ROUND(x * 10ⁿ) / 10ⁿ`; `n < 0` → `ROUND(x / 10⁻ⁿ) * 10⁻ⁿ` — always scaling by the **positive** power `10^{abs(n)}`, which is exactly representable in f64 (10⁰…10¹⁵ are; negative powers are not, which is why the n<0 form divides first). The constant is baked at bind as `LitFloat(10f64.powi(|n|))`, so an int `x` is lifted to float by the arithmetic and the result is always F64 — the identity fold applies to the 1-arg form only. Committed semantics are f64 arithmetic, not decimal: results can differ from exact decimal rounding by double-rounding (e.g. in the last ulp for large `n`), and a huge `x·10ⁿ` can overflow to ±∞ (consistent with existing float-op overflow behavior). PostgreSQL has no `round(float8, int)` at all, so 2-arg ROUND is this plan's own definition. |
| `TRUNC(x)` | float | F64 | `f64::trunc` (toward zero). Int arg: identity fold. One argument only; `TRUNC(x, n)` is a Bind error. |
| `MOD(a, b)` | int, int | I64/U64 | Pure bind-time desugar to `BinOp(a, Mod, b)` — identical to `a % b`, including divisor-0 → NULL and the float-modulo rejection (`lower.rs:355`). |
| `GREATEST(a, …)` / `LEAST(a, …)` | numeric, ≥ 1 arg | `unify_numeric` fold over args | PostgreSQL NULL semantics: NULL args are skipped; result is NULL only when all args are NULL. First-class n-ary IR node lowered as a left fold of the 2-ary null-skipping MAX2/MIN2 opcodes (§2.1) — O(n) registers, computed args fully supported. Non-numeric arg (string/blob/wide-int) → typed error at lowering. Arity is bounded only by the 64-register file (`2n−1` regs pure-int, `3n−2` with float lifts → ≈ 22–32 args; beyond it the client's `validate` rejects with `TooManyRegs`, the same bound every large expression has). |
| `CAST(x AS T)` / `x::T` | numeric → numeric | `T` | §2.2 below. |

NULL propagation for all of the above: NULL in → NULL out (via `null_copy1`,
`batch.rs:151-164`), except GREATEST/LEAST (skip-NULL natively in the opcode).

### 2.1 MAX2/MIN2 semantics

`MAX2(a, b)` (and mirror `MIN2`): if `a` is NULL → `b` (with `b`'s null bit); if `b` is NULL
→ `a`; else the larger (smaller) of the two. Result is NULL iff both are NULL
(`null = a_null & b_null`). A NULL-literal arg (`GREATEST(NULL, x)`) needs no bind-time
special case — `load_null` feeds the opcode and is skipped natively.

- **Integer compare domain is one GLOBAL lowering decision**, exactly like CASE's float
  unification (`lower.rs:270-290`): when the `unify_numeric` fold over all args is U64, the
  whole fold compares unsigned; else signed. Per-instruction, the engine picks unsigned when
  either operand's `reg_tc` is U64 (the ordered-`Cmp` rule,
  `crates/gnitz-expr/src/program.rs:709-715`) with `propagate_u64` (`program.rs:1052-1059`)
  making the taint sticky — but the taint only exists *from the first U64 operand onward*, so
  a naive in-SQL-order fold would compare early signed pairs signed and be
  argument-order-dependent (`GREATEST(-1, 1, u)` ≠ `GREATEST(u, -1, 1)`). The lowering
  therefore **rotates one U64-inferring arg to the head of the fold** whenever the unified
  type is U64: the first MAX2 is then unsigned, the taint propagates, every subsequent fold is
  unsigned, and the result is argument-order-independent (null-skip extremum is commutative
  once the domain is fixed, so the rotation is semantics-free). Consequence, documented: a
  mixed signed/U64 argument list compares in the unsigned domain (`GREATEST(5::u64, -1)` is
  `-1` read as 2⁶⁴−1 → result 2⁶⁴−1 typed U64) — identical to what `WHERE u > -1` already
  does.
- **Float compare** uses `f64::total_cmp` — the same total order the engine sorts rows by
  (CLAUDE.md §5): NaN orders above +∞, `-0.0 < +0.0`. So `GREATEST(5.0, NaN) = NaN`
  (PostgreSQL-compatible, which sorts NaN greatest) and `LEAST(5.0, NaN) = 5.0`, with a
  strict total order guaranteeing determinism.
- `reg_tc` propagation: `propagate_u64(a, b)`, like `IntAdd`/`Select`.

### 2.2 CAST semantics

Accepted targets: every `sql_type_to_typecode` numeric — I8/I16/I32/I64, U8/U16/U32/U64
(`TINYINT`…`BIGINT [UNSIGNED]`), F32 (`FLOAT`), F64 (`DOUBLE`/`DOUBLE PRECISION`/`REAL`).
Everything else (`TEXT`/`VARCHAR`/`CHAR`, `UUID`, `DECIMAL(38|39,0)`, `BOOLEAN`, unknown) is
an `Unsupported` bind error: `CAST to {T:?} is not supported`. Source must be a numeric
expression — a STRING/BLOB/wide-int source is already rejected by the operand lowering
(`OpcodeBackend::col_ref`, `lower.rs:199-228`). `CastKind::Cast` and `CastKind::DoubleColon`
are accepted; `TryCast`/`SafeCast`, a non-`None` `format`, and `array: true` (sqlparser 0.62
parses `CAST(x AS INT ARRAY)` under every dialect) are `Unsupported` — the `Expr::Cast`
destructure names all five fields, no `..`.

**Bind-time literal check:** an integer-literal source (`LitInt` or `Neg(LitInt)`, the same
shapes `fold_int_literal` recognizes, `lower.rs:77-86`) that is statically outside an integer
target's domain is a Bind error at CREATE time (`CAST(300 AS TINYINT)`), not a per-row NULL.

Runtime semantics matrix (register values are i64 images / f64 bit patterns; `src_signed` is
the resolve-time U64 tracking):

- **int → int.** Value unchanged when in the target's domain, NULL otherwise. Statically
  elided (no opcode) when `int_domain_fits(reg_domain(inner), target)`
  (`crates/gnitz-sql/src/types.rs:96-103`), where **`reg_domain` is NOT `infer_type`**:

  ```rust
  /// The static domain of the VALUE in the register, as a type code. infer_type
  /// alone is unsound here: UnaryOp::Neg infers its inner type (`ir.rs:94`,
  /// `infer_type_with`) but the engine negates on the full i64 register —
  /// CAST(-x AS INT) at x = i32::MIN holds +2^31, outside I32. Only nodes whose
  /// register value provably lies in their inferred type's domain may report a
  /// narrow domain.
  fn reg_domain(e: &BoundExpr, cols: &[ColumnDef]) -> TypeCode {
      match e {
          BoundExpr::ColRef(i) => cols[*i].type_code,   // loads are bounded
          BoundExpr::Cast { to, .. } => *to,            // checked-or-NULL by construction
          _ => { let t = e.infer_type(cols); unify_numeric(t, t) }  // I64/U64/F64
      }
  }
  ```

  Otherwise one `EXPR_INT_CAST` opcode range-checks at runtime: in-range keeps the register
  bits (already the correct sign/zero-extended image), out-of-range → NULL. `CAST(-1 AS
  BIGINT UNSIGNED)` → NULL; `CAST(u64col AS BIGINT)` → NULL for values ≥ 2⁶³; `CAST(-x AS
  INT)` at `x = i32::MIN` → NULL (runtime check, per `reg_domain`). Documented quirk:
  `CAST(-CAST(5 AS BIGINT UNSIGNED) AS BIGINT)` is NULL, not −5 — `IntNeg` propagates the
  U64 tag (`program.rs:700-703`), so the negation lives in the U64 domain (2⁶⁴−5).
- **int → F64.** Existing `EXPR_INT_TO_FLOAT` (signed-aware, `batch.rs:878-894`). No new opcode.
- **int → F32.** `EXPR_INT_TO_FLOAT` then `EXPR_FLOAT_TO_F32`. The two-step path
  double-rounds (i64 → 53-bit → 24-bit) and can differ from a correctly-rounded direct
  i64→f32 by 1 ulp for values > 2⁵³; deterministic, committed.
- **float → int.** `EXPR_FLOAT_TO_INT`: truncate toward zero, then range-check against the
  target: NULL for NaN, ±∞, or a truncated value outside the target domain. `CAST(2.7 AS
  INT) = 2`, `CAST(-2.7 AS INT) = -2`, `CAST(1e300 AS INT)` → NULL. (Truncation, not
  rounding, is the committed rule — SQLite/C semantics; a user who wants rounding writes
  `CAST(ROUND(x) AS INT)`.)
- **float → F64.** No-op (register already f64). **float → F32:** `EXPR_FLOAT_TO_F32`:
  round through f32 precision. A *finite* value that overflows the finite f32 range after
  round-to-nearest → NULL (rule 1 — saturating to ±∞ would be the clamp; PostgreSQL errors
  here). NaN → NaN and ±∞ → ±∞ (both are representable f32 values, not range errors).
- **NULL sources** need no special case: `CAST(NULL AS T)` lowers through the same matrix
  (`load_null` feeding the cast opcode); NULL propagates, the dead value 0 is in every
  range. Result type is `T`.

The output *column* type of a computed cast is the target type itself — `CAST(x AS SMALLINT)
AS c` produces an I16 output column (stride 2). The runtime range check makes the narrow EMIT
truncation lossless by construction; a NULL result row writes value 0 + null bit as today.
This is the first path that can produce a computed F32 output column, which the EMIT kernel
currently cannot write (it would slice raw i64 register bits to 4 bytes); §6.3 adds the F32
emit arm.

`reg_tc` flow: `EXPR_INT_CAST` with target U64 sets `reg_tc[dst] = U64` (a downstream ordered
compare picks the unsigned variant); every other cast dst sets 0. `infer_type` returns the
target `TypeCode`, so `unify_numeric` (`ir.rs:77`) sees U64/F64 correctly downstream.

## 4. Client side (gnitz-sql, gnitz-core)

### 4.1 IR (`crates/gnitz-sql/src/ir.rs`)

Three new variants on `BExpr<R>` (`ir.rs:21-65`; `BoundExpr = BExpr<usize>`, `ir.rs:70`) —
the unary-function enum is named `NumFunc` (not "ScalarFunc": the engine already has an
unrelated `pub struct ScalarFunc` in `gnitz-engine/src/expr/plan.rs`, and this enum only ever
holds unary numeric transforms):

```rust
/// Deterministic unary numeric scalar function. Two-arg ROUND and MOD are
/// bind-time desugars and never construct this.
Func {
    f: NumFunc,
    arg: Box<BExpr<R>>,
},
/// GREATEST (is_max) / LEAST (!is_max): n-ary null-skipping extremum,
/// lowered as a left fold of MAX2/MIN2. args is non-empty.
MinMaxN {
    is_max: bool,
    args: Vec<BExpr<R>>,
},
/// CAST(expr AS to) / expr::to, numeric-to-numeric only.
Cast {
    expr: Box<BExpr<R>>,
    to: TypeCode,
},
```

with `enum NumFunc { Abs, Floor, Ceil, Round, Trunc }`.

Every `BExpr` walker is an exhaustive match, so each new variant fails to compile until it is
handled: `infer_type_with` (`ir.rs:94`), `try_map_refs` (`ir.rs:156`), `try_expand_leaves`
(`ir.rs:193`), `for_each_ref` (`ir.rs:249`), and `lower_bound_expr` (`lower.rs:46-70`).
`infer_type` (`ir.rs:286`) arms:

```rust
BExpr::Func { f, arg } => {
    let t = arg.infer_type(cols);
    match f {
        NumFunc::Abs => unify_numeric(t, t),
        NumFunc::Floor | NumFunc::Ceil | NumFunc::Round | NumFunc::Trunc => {
            if t.is_float() { TypeCode::F64 } else { unify_numeric(t, t) }
        }
    }
}
BExpr::MinMaxN { args, .. } => {
    let mut ty = args[0].infer_type(cols);
    for a in &args[1..] { ty = unify_numeric(ty, a.infer_type(cols)); }
    ty
}
BExpr::Cast { to, .. } => *to,
```

### 4.2 Binding (`crates/gnitz-sql/src/bind/structural.rs`)

All interception is structural (schema-free), in `bind_structural`'s match (`structural.rs:66`)
alongside the COALESCE/NULLIF arms (`:72-75`) — scalar functions are context-independent, so
every binding context (WHERE, projection, HAVING, join residual, DML) gets them at once:

- `Expr::Function(f)` name arms (via `fn_name_is`, `ast_util.rs:70`): `"abs"`, `"ceiling"`,
  `"round"`, `"trunc"`, `"mod"`, `"greatest"`, `"least"`. (`CEILING` is NOT
  keyword-dispatched by sqlparser — only `CEIL`/`FLOOR` route to `parse_ceil_floor_expr` — so
  it arrives as a plain function; `CEIL(x)`/`FLOOR(x)` arrive as the dedicated AST nodes below
  and never as functions.) Each arm uses `function_positional_args` (`ast_util.rs:550`) +
  `reject_unsupported_fn_qualifiers` (`ast_util.rs:513`), as COALESCE does, and checks arity:
  - `abs`/`trunc`/`ceiling`: exactly 1 arg → `BExpr::Func`.
  - `round`: 1 arg → `Func`; 2 args → the §2 desugar, emitted unconditionally (the binder
    is schema-free and cannot type the arg; the `LitFloat` scale constant lifts an int `x`
    to float, so 2-arg ROUND always produces F64). The scale must be
    `Expr::Value(ValueWithSpan { value: Value::Number(s, _), .. })` — sqlparser 0.62 wraps
    every literal in `ValueWithSpan { value, span }` (`ast/value.rs:78-83`), and
    `Value::Number` is `(String, bool)` without the `bigdecimal` feature
    (`ast/value.rs:139-142`) — or a `UnaryOp::Minus`/`UnaryOp::Plus` of one, parsed to `i64`,
    in `-15..=15`, else `Bind("ROUND: scale must be an integer literal in -15..=15")`.
  - `mod`: exactly 2 args → `BinOp(a, Mod, b)`.
  - `greatest`/`least`: ≥ 1 arg, each bound via `bind_structural` → `BExpr::MinMaxN`.
    No literal/column restriction, no null-test machinery — NULL skipping is the opcode's.
- `Expr::Floor { expr, field }` / `Expr::Ceil { expr, field }` (`ast/mod.rs:1132-1141`):
  accept only `field == CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)` — the
  bare-call parse — and reject the other two shapes of
  `enum CeilFloorKind { DateTimeField(DateTimeField), Scale(ValueWithSpan) }`
  (`ast/mod.rs:817-822`), i.e. `CEIL(x TO DAY)` and `CEIL(x, 2)`, as `Unsupported`.
- `Expr::Cast { kind, expr, data_type, array, format }` — all five fields destructured, no
  `..` (`ast/mod.rs:1086-1099`): accept `kind ∈ {Cast, DoubleColon}`, `array == false`,
  `format.is_none()`; map `data_type` through `sql_type_to_typecode` (`types.rs:5-30`),
  reject non-numeric targets per §2.2; apply the bind-time literal range check (§2.2);
  produce `BExpr::Cast`.

**Pre-bind AST walkers** (`crates/gnitz-sql/src/ast_util.rs:270-312`): `expr_operands` must
gain arms for the two AST nodes this plan adds to the binder's vocabulary that it does not
already reach — `Expr::Floor`/`Expr::Ceil` contribute `expr`, `Expr::Cast` contributes
`expr` — upholding the invariant its doc comment states ("a node added to the binder's
vocabulary reaches them all at once"). Today these nodes fall into the `_ => Vec::new()`
wildcard (`:310`), harmless only because they fail to bind; with that backstop removed, the
wildcard breaks three consumers:

- `expr_contains_excluded` (`dml/insert.rs:373-380`) — an `EXCLUDED.b` hidden under
  `CAST(EXCLUDED.b AS …)` in `ON CONFLICT DO UPDATE` would pass the guard and silently bind
  to the *existing* row's column: data corruption.
- `for_each_agg_call` (`ast_util.rs:220`) — the shared aggregate pre-collection walker
  (`collect_aggs`, `hir/bind.rs:1263`; `collect_having_aggs`, `dml/group_by.rs:154`) —
  `HAVING CAST(SUM(x) AS INT) > 0` would collect no aggregate.
- `expr_has_aggregate` (`ast_util.rs:188-195`) — `SELECT CAST(SUM(x) AS INT)` would misroute
  to the scalar path.

The plain-`Function` names (abs/ceiling/round/trunc/mod/greatest/least) are already covered
by the existing Function-args arm.

**Ad-hoc HAVING leaf** (`crates/gnitz-sql/src/dml/group_by.rs`). `Having::bind_function`
(`:264`) and the `Expr::Function` arm of `Having::bind_null_test` (`:290`) route every
function name to `resolve_having_mapping` → `having_agg_func` (`:131`), which asserts the
result is an aggregate:

```rust
match (SingleTable { schema: source_schema }).bind_function(func)? {
    BoundExpr::AggCall { func, arg } => Ok((func, agg_arg_col(arg.as_deref())?)),
    other => unreachable!("SingleTable::bind_function only binds aggregate calls, got {other:?}"),
}
```

This plan makes `SingleTable::bind_function` return non-aggregate nodes, so
`HAVING ABS(SUM(x)) > 0` on the ad-hoc path would hit that `unreachable!()` — a panic.
Required fix, in this plan: gate both methods on `is_agg_call` (`ast_util.rs:198`) and fall
through to the ordinary structural recursion when it is false, so the scalar call binds and
its nested aggregate resolves through the leaf. The HIR HAVING leaf already has exactly this
shape (`hir/bind.rs:1461-1466` for `bind_function`, `:1467-1470` for `bind_null_test`) — the
ad-hoc leaf is the one that needs it. While there, drop `dml/group_by.rs:196`'s doc reference
to the non-existent `bind_having_null_test`.

### 4.3 Opcode lowering (`crates/gnitz-sql/src/lower.rs`)

`BoundExprBackend` (`lower.rs:17-42`) gains three methods; `lower_bound_expr`'s match
(`:46-70`) dispatches the three new variants:

```rust
fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<Self::Out, GnitzSqlError>;
fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<Self::Out, GnitzSqlError>;
fn cast(&mut self, expr: &BoundExpr, to: TypeCode) -> Result<Self::Out, GnitzSqlError>;
```

`OpcodeBackend` holds `cols: &[ColumnDef]` and `eb: &mut ExprBuilder` (`lower.rs:191-194`),
and `infer_type` takes `&[ColumnDef]` (`ir.rs:286`), so `self.cols` is the type context
throughout.

`OpcodeBackend::func`:

```rust
let (r, is_float) = lower_bound_expr(arg, self)?;
match f {
    NumFunc::Abs if is_float => Ok((self.eb.float_abs(r), true)),
    NumFunc::Abs => {
        // Unsigned register values are non-negative by definition: identity.
        if !arg.infer_type(self.cols).is_signed_int() { Ok((r, false)) }
        else { Ok((self.eb.int_abs(r), false)) }
    }
    // Floor/Ceil/Round/Trunc: identity on an integer register.
    _ if !is_float => Ok((r, false)),
    NumFunc::Floor => Ok((self.eb.float_floor(r), true)),
    NumFunc::Ceil  => Ok((self.eb.float_ceil(r), true)),
    NumFunc::Round => Ok((self.eb.float_round(r), true)),
    NumFunc::Trunc => Ok((self.eb.float_trunc(r), true)),
}
```

`OpcodeBackend::min_max_n`: gate every arg first — `infer_type(self.cols)` must be neither
`is_wide_int` nor `is_german_string` (both `TypeCode` methods,
`gnitz-wire/src/types.rs:89,99`), else `Unsupported("GREATEST/LEAST: not supported on
{t:?}")` (the backend holds the column defs; the schema-free binder cannot do this). Then
lower every arg, apply the same global float unification `case` uses (`lower.rs:270-290`: any
float arg → lift every int arg via `int_to_float`), and — when the unified type is U64 —
rotate one U64-inferring arg's register to the fold head (§2.1) so the whole fold compares
unsigned. Fold left with `eb.max2`/`eb.min2` (float variants when unified float). Registers:
n loads + (n−1) folds + lifts — O(n).

`OpcodeBackend::cast` implements the §2.2 matrix: compute `d = reg_domain(expr, self.cols)`
(§2.2 — NOT plain `infer_type`), lower the operand, then

- int source, int target: `int_domain_fits(d, to)` → identity; else `eb.int_cast(r, to)`.
- int source, float target: `eb.int_to_float(r)` (+ `eb.float_to_f32` if `to == F32`).
- float source, int target: `eb.float_to_int(r, to)`.
- float source, F64: identity; F32: `eb.float_to_f32(r)`.
- Returned `is_float` = `to.is_float()`.

### 4.4 Builder (`crates/gnitz-core/src/expr.rs`)

Thirteen emitters, all one-liners over the existing `binary_op`/`unary_op` helpers
(`expr.rs:66-70`, `:74-76`): `int_abs`, `float_abs`, `float_floor`, `float_ceil`,
`float_round`, `float_trunc`, `float_to_f32` (plain unary); `max2`/`min2`/`float_max2`/
`float_min2` (plain binary); and `int_cast(reg, tc)` / `float_to_int(reg, tc)` emitted as
`binary_op(OP, reg, tc as u32)` — the target type code rides the `a2` word, the same
one-word-payload convention `add_const_int_set` uses for its pool index (`expr.rs:271-277`;
`TypeCode`'s `#[repr(u8)]` discriminants, `gnitz-wire/src/types.rs:30-31`, are the wire
codes, so `tc as u32` round-trips).

## 5. Wire (`crates/gnitz-wire/src/expr.rs`)

Thirteen new opcode constants — 37–39 fill the existing gap between `EXPR_LOAD_NULL` (36) and
`EXPR_STR_COL_EQ_CONST` (40), the rest continue after the current maximum,
`EXPR_INT_IN_SET` (46):

```rust
pub const EXPR_INT_ABS: u32 = 37;
pub const EXPR_FLOAT_ABS: u32 = 38;
pub const EXPR_FLOAT_FLOOR: u32 = 39;
pub const EXPR_FLOAT_CEIL: u32 = 47;
pub const EXPR_FLOAT_ROUND: u32 = 48;
pub const EXPR_FLOAT_TRUNC: u32 = 49;
/// Truncate-toward-zero float→int cast with target range check:
/// `[EXPR_FLOAT_TO_INT, dst, a, target_tc]`. NaN/±∞/out-of-range → NULL.
pub const EXPR_FLOAT_TO_INT: u32 = 50;
/// Integer domain cast: `[EXPR_INT_CAST, dst, a, target_tc]`. Value unchanged
/// when in the target domain (interpreted per the source's signedness), NULL
/// otherwise.
pub const EXPR_INT_CAST: u32 = 51;
/// Round through f32 precision: `[EXPR_FLOAT_TO_F32, dst, a, 0]`. A finite
/// value beyond ±f32::MAX → NULL; NaN and ±∞ pass through (representable).
pub const EXPR_FLOAT_TO_F32: u32 = 52;
/// Null-skipping 2-ary extremum: `[op, dst, a, b]`. NULL operand → other
/// operand; NULL iff both NULL. Int compare signed/unsigned per U64 tracking;
/// float compare by f64::total_cmp.
pub const EXPR_INT_MAX2: u32 = 53;
pub const EXPR_INT_MIN2: u32 = 54;
pub const EXPR_FLOAT_MAX2: u32 = 55;
pub const EXPR_FLOAT_MIN2: u32 = 56;
```

No blob-format change (`EXPR_BLOB_VERSION` stays 1, `expr.rs:69`; the codes are additive and
client + engine ship together).

## 6. Engine (`crates/gnitz-expr/src/`, plus one `gnitz-engine` emit arm)

### 6.1 `program.rs` — decode, resolve, validate, analyses

Following the file's own op-family pattern (`CmpOp` collapses 12 wire compares into 2
variants, `program.rs:52-59`; `StrOp` 6 into 2, `:63-67`), the five pure float unaries
collapse into **one** variant carrying an operator enum; the per-op wire constants stay
(matching existing wire granularity). New in both `LogicalInstr` (`:76-221`) and `Instr`
(`:231-396`):

```rust
/// Pure float unary transform (no NULL production, no type payload).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FloatUnaryOp { Abs, Floor, Ceil, Round, Trunc }

FloatUnary { op: FloatUnaryOp, dst: u16, a: u16 },
IntAbs     { dst: u16, a: u16 },
/// tc is u32 in LogicalInstr (the raw wire word, validated), u8 in Instr.
FloatToInt { dst: u16, a: u16, tc },
IntCast    { dst: u16, a: u16, tc },          // Instr adds src_signed: bool
FloatToF32 { dst: u16, a: u16 },
IntMinMax2   { dst: u16, a: u16, b: u16, is_max: bool }, // Instr adds signed: bool
FloatMinMax2 { dst: u16, a: u16, b: u16, is_max: bool },
```

(`FloatToF32` stays out of `FloatUnary`: it is the one float unary that can *produce* NULL,
so it needs its own nullability classification and eval shape.)

`from_wire` (`:470`): one arm per wire constant; cast arms carry `tc: q[3]` verbatim (full
u32 — a forged high-bit word must reach `validate`, not be silently truncated).

`resolve` (`:627-849`) arms:

- `FloatUnary`, `FloatToF32`: `reg_tc[dst] = 0`.
- `IntAbs`: `reg_tc[dst] = reg_tc[a]` (like `IntNeg`, `:700-703`).
- `IntCast`: `src_signed = !is_u64(reg_tc[a])`; narrow `tc` to u8;
  `reg_tc[dst] = if tc == U64 { U64 } else { 0 }`.
- `FloatToInt`: narrow `tc`; `reg_tc[dst] = if tc == U64 { U64 } else { 0 }`.
- `IntMinMax2`: `signed = !any_u64(&reg_tc, a, b)` (the ordered-`Cmp` rule, `:709-715`);
  `reg_tc[dst] = propagate_u64(&reg_tc, a, b)` (`:1052-1059`).
- `FloatMinMax2`: `reg_tc[dst] = 0`.

`validate` (`:860-1001`): `FloatUnary`/`IntAbs`/`FloatToF32` join the existing unary arm
(`:923-927`); `IntMinMax2`/`FloatMinMax2` join the binary register-op arm (bounds + SSA
anti-aliasing, `:891-912`). `FloatToInt`/`IntCast` get their own arm: `check_reg(dst)`,
`check_reg(a)`, and `tc <= u8::MAX && gnitz_wire::is_fixed_int(tc as u8)`
(`gnitz-wire/src/types.rs:323-335`), else a new `ExprValidateErr::BadCastTarget { tc: u32 }`
(the enum is at `:32-44`) — a forged blob must not reach eval with a junk type code.

Analyses:

- `each_reg_read` (`:1099-1140`): the five unary shapes read `a`; the two MinMax2 shapes read
  `a` and `b`.
- `classify_registers` (`:1226-1307`): all seven are value producers; unaries add
  `non_bool_read |= 1 << a`, MinMax2 adds both operands — join the
  `IntNeg | FloatNeg | IntToFloat` / `IntAdd…` arms respectively.
- `is_strictly_non_nullable` (`:1313-1369`): `FloatToInt`, `IntCast`, and `FloatToF32` can
  manufacture NULL → `return false` (join the `IntDiv`/`IntMod`/`FloatDiv` arm).
  `FloatUnary`, `IntAbs`, `IntMinMax2`, `FloatMinMax2` only propagate/consume → join the safe
  remainder (MinMax2's output is null only when both inputs are, which the operand columns'
  nullability already accounts for).
- `and_chain_mask` needs no change (spine links are `BoolAnd`-written only).

### 6.2 `batch.rs` — eval arms

`FloatUnary` and `IntAbs` share the `IntNeg`/`FloatNeg` loop shape (`batch.rs:666-676`,
`:722-732`): hoisted bases, per-row transform, `null_copy1`, `maybe_pack_bool_bits`
(`:286-293`). The `FloatUnaryOp` match sits **outside** the row loop (the `Instr::Cmp`
pattern, `:739-753`). Bodies, over the existing `decode_f64` (`:425-427`) / `encode_f64`
(`:428-431`) helpers:

```rust
IntAbs   => regs[d] = regs[a].wrapping_abs()
Abs      => encode_f64(decode_f64(x).abs())
Floor    => encode_f64(decode_f64(x).floor())
Ceil     => encode_f64(decode_f64(x).ceil())
Round    => encode_f64(decode_f64(x).round_ties_even())
Trunc    => encode_f64(decode_f64(x).trunc())
```

`FloatToInt` / `IntCast` / `FloatToF32` need a unary analogue of `div_like!` (`:475-502`):
compute unconditionally, accumulate a per-row fail mask, `null_copy1`, then OR the fail mask
into the dst null word. New macro `unary_null_like!` with body returning
`(val: i64, fail: bool)`:

- `IntCast { tc, src_signed }`: derive `(lo, hi)` per target outside the row loop.
  `src_signed`: fail iff `v < lo_i64 || v > hi_i64`, where for U64 the check degenerates to
  `v < 0`. `!src_signed` (source U64): reinterpret `v as u64`, fail iff above the target
  max. On success the register bits pass through unchanged.
- `FloatToInt { tc }`: `t = decode_f64(x).trunc()`; per-target f64 bounds derived outside
  the loop — I64: `t >= -9223372036854775808.0 && t < 9223372036854775808.0` (2⁶³ is
  f64-exact, 2⁶³−1 is not → exclusive upper); U64: `t >= 0.0 && t < 18446744073709551616.0`;
  narrow targets: inclusive integer-exact bounds. NaN fails all comparisons → fail (rule 1).
  Success value: `t as i64` (signed) / `t as u64 as i64` (unsigned) — in-range by the
  check, so the Rust saturating cast is exact.
- `FloatToF32`: `let f = decode_f64(x); let v32 = f as f32;` fail iff
  `f.is_finite() && v32.is_infinite()` — the IEEE overflow condition (the *rounded* result
  exceeds the finite f32 range; a plain `abs() > f32::MAX` test would wrongly NULL values
  that round down to `f32::MAX`). Value: `encode_f64(v32 as f64)`.

`IntMinMax2` / `FloatMinMax2` cannot use `bin_op!` (`:456-471`; its `null_or2` epilogue,
`:139-148`, is exactly the wrong null rule) — they get a dedicated arm shaped like `Select`'s
nullable path (`:913-943`):

- `no_nulls` arm: straight per-row extremum over `reg3` — int: `max/min` (signed) or
  `(x as u64).max(y as u64) as i64`; float: `total_cmp`-pick.
- nullable arm: read the two operand null words; per word compute `nd = na & nb`; per row
  pick `if a_null { b } else if b_null { a } else { extremum }`; write `nd` to the dst null
  word. Epilogue `maybe_pack_bool_bits`.

### 6.3 F32 emit arm (`crates/gnitz-engine/src/expr/plan.rs`)

`EmitCol` (fields `payload: usize, reg: usize, stride: u8`) gains an `is_f32: bool`, set in
`from_map` when the output column's `type_code == F32`. In the emit kernel inside
`map_rows_into`, the `is_f32` arm takes precedence over the generic narrow-stride slice
(which today truncates `val.to_le_bytes()[..stride]`):
`let bits = (f64::from_bits(val as u64) as f32).to_bits()`, write 4 bytes. The
`float_to_f32` opcode already rounded and range-checked, so this is the mechanical
bits→f32-bits narrowing, not a second rounding. The stride-8 fast arm and the narrow-int arm
are unchanged, as is the NULL-row path (value 0 + null bit).

### 6.4 Untouched by design

`ScalarFunc::from_predicate`/`from_map` construction (beyond §6.3), `op_filter`/`op_map`, the
compiler's `emit.rs` plumbing (it validates and builds opcode-agnostically), the ad-hoc read
path (`catalog/scan_spec.rs` flows through the same
`decode_expr_blob → from_wire → validate → ScalarFunc` pipeline, so the new opcodes and the
F32 emit arm reach it automatically), and the reduce/aggregate layer. The client residual
path (`exec/residual.rs`, `dml/mutate.rs`) and ad-hoc HAVING (`exec/agg_finish.rs`) pick up
every new function through the shared VM with no per-site work. `gnitz-capi` is deliberately
untouched: its `ExprBuilder` mirror is already a partial surface (no float arithmetic
exposed), and extending it is not part of this plan.

## 7. Edit surface

| File | Change |
|---|---|
| `crates/gnitz-wire/src/expr.rs` | 13 opcode constants (37-39, 47-56) |
| `crates/gnitz-core/src/expr.rs` | 13 `ExprBuilder` emitters; import list |
| `crates/gnitz-sql/src/ir.rs:21-65,94,286` | `Func`/`MinMaxN`/`Cast` variants, `NumFunc` enum, `infer_type`/`infer_type_with`/`try_map_refs`/`try_expand_leaves`/`for_each_ref` arms |
| `crates/gnitz-sql/src/bind/structural.rs:66-189` | `Expr::Floor`/`Expr::Ceil`/`Expr::Cast` arms; name interceptions abs/ceiling/round/trunc/mod/greatest/least; ROUND-scale validation + two-sided desugar; CAST literal range check |
| `crates/gnitz-sql/src/ast_util.rs:270-312` | `expr_operands` arms for `Expr::Floor`/`Expr::Ceil`/`Expr::Cast` |
| `crates/gnitz-sql/src/dml/group_by.rs:131,196,264,290` | `is_agg_call` gate + structural fall-through in `Having::bind_function` and `bind_null_test`'s `Expr::Function` arm; stale doc reference |
| `crates/gnitz-sql/src/lower.rs:17-70,191-228` | trait methods `func`/`min_max_n`/`cast`; dispatch arms; `OpcodeBackend` impls; `reg_domain` |
| `crates/gnitz-expr/src/program.rs` | 7 `LogicalInstr` + 7 `Instr` variants, `FloatUnaryOp`; `from_wire`, `resolve`, `validate` (+`BadCastTarget`), `each_reg_read`, `classify_registers`, `is_strictly_non_nullable` arms |
| `crates/gnitz-expr/src/batch.rs` | `FloatUnary`/`IntAbs` arms; `unary_null_like!`; `IntCast`/`FloatToInt`/`FloatToF32` arms; `IntMinMax2`/`FloatMinMax2` arms |
| `crates/gnitz-engine/src/expr/plan.rs` | `EmitCol.is_f32` + emit arm |

## 8. Tests

**Engine unit (`crates/gnitz-engine/src/expr/tests/program_tests.rs`, which exercises
`gnitz-expr` through `Batch`/`MemBatch`):**
- Per-opcode eval over a mixed batch incl. NULL rows: ABS int/float (incl. `i64::MIN`,
  `-0.0`), FLOOR/CEIL/ROUND/TRUNC (incl. `round_ties_even` at `2.5 → 2.0`, `3.5 → 4.0`,
  negatives), FLOAT_TO_F32 precision loss (`0.1`), FLOAT_TO_F32 finite overflow `1e300` →
  NULL, NaN/∞ pass-through.
- IntCast: identity in range; NULL out of range at every target width; U64-source
  (`reg_tc` path) both directions; negative → unsigned target → NULL.
- FloatToInt: trunc both signs; NaN/±∞ → NULL; the 2⁶³/2⁶⁴ exclusive boundaries;
  narrow-target overflow → NULL.
- MinMax2: null-skip on each side, both-NULL → NULL; signed vs U64-tainted compare;
  float `total_cmp` (NaN beats +∞ for MAX2, loses for MIN2; `-0.0` vs `0.0`).
- validate: `BadCastTarget` on junk tc (0, 12/U128, 11/STRING, 255, and a >255 forged
  word); reg bounds/aliasing for the new arms; `no_nulls` stays true for pure float unaries
  over non-null columns and flips false when an `IntCast`/`FloatToF32` appears.
- F32 emit: a map emitting a computed F32 column round-trips the value.

**SQL unit (`lower.rs`, `structural.rs`, `ir.rs` test mods):**
- Fold elisions: `FLOOR(intcol)` no opcode; `ABS(u32col)`/`ABS(u64col)` no opcode;
  `CAST(i32 AS BIGINT)` no opcode; `CAST(i64 AS INT)` emits `EXPR_INT_CAST`;
  **`CAST(-intcol AS INT)` emits `EXPR_INT_CAST`** (the `reg_domain` rule — must NOT elide).
- ROUND 2-arg: both desugar sides (`n ≥ 0` multiply-first, `n < 0` divide-first);
  scale-literal rejection (non-literal, out of range, float); exact negative-scale results
  (`ROUND(881469.0444980001, -5) = 900000.0`).
- MOD desugar ≡ `%`; float MOD rejected; `MOD(x, 0)` → NULL.
- GREATEST/LEAST: MinMaxN shape; computed args accepted (`GREATEST(a+1, b)`); `-1` literal
  arg accepted; NULL-literal arg skipped at runtime; string/U128 arg rejected at lowering;
  4+ args compile (O(n) registers pinned: assert `num_regs` linear in arity); a 40-arg
  GREATEST rejects with `TooManyRegs`; mixed-U64 argument-order independence
  (`GREATEST(-1, 1, u)` ≡ `GREATEST(u, -1, 1)` — the fold-head rotation).
- Walker coverage: `ON CONFLICT DO UPDATE SET a = CAST(EXCLUDED.b AS BIGINT)` is rejected
  by `expr_contains_excluded` (not silently mis-bound); `SELECT CAST(SUM(x) AS INT)` routes
  to the aggregate path; `HAVING CAST(SUM(x) AS INT) > 0` collects the aggregate
  (`for_each_agg_call` reaching through `Expr::Cast`).
- Ad-hoc HAVING leaf: `HAVING ABS(SUM(x)) > 0` on a direct SELECT binds and evaluates (the
  `is_agg_call` gate — without it, a panic).
- CEILING binds (function-name path); `CEIL(x TO DAY)` rejected.
- CAST: `::` binds; TRY_CAST rejected; `CAST(x AS INT ARRAY)` rejected; TEXT/UUID/BOOLEAN
  targets rejected; `CAST(300 AS TINYINT)` → Bind error; infer_type returns target tc (U64
  preservation through `unify_numeric` pinned).

**E2E (`crates/gnitz-py/tests/test_numeric_functions.py`, `GNITZ_WORKERS=4`):**
- A view projecting each function over a table with NULLs; insert → check, update (retract +
  insert) → check the old row vanished (weight cancellation with computed columns), delete →
  empty. This is the retraction-determinism proof for the whole opcode set.
- Filters: `WHERE ABS(x) > k`, `WHERE CAST(f AS INT) = k` (NULL rows excluded),
  `WHERE GREATEST(a, b) >= k`.
- CAST narrowing view: out-of-range source rows show NULL; schema readback shows the narrow
  column type; F32 computed column round-trips; `CAST(1e300 AS FLOAT)` → NULL.
- `HAVING ABS(SUM(x)) > k` on both the view path and the ad-hoc path, asserting equal
  results.
- Ad-hoc SELECT with the same expressions (read-spec path).
- Point-DML `DELETE … WHERE MOD(x, 2) = 0` and `UPDATE … SET n = ABS(n)`; a
  `CAST(u64col AS SMALLINT)` residual on the scan fallback.

## 9. Sequencing

- [ ] Commit 1: wire constants + `ExprBuilder` emitters + `gnitz-expr`
      decode/resolve/validate/eval (+ engine tests) — the engine accepts the opcodes before
      any SQL produces them.
- [ ] Commit 2: IR + binder + lowering for ABS/FLOOR/CEIL/CEILING/ROUND/TRUNC/MOD/
      GREATEST/LEAST, incl. the `expr_operands` arms and the ad-hoc HAVING-leaf
      `is_agg_call` gate (+ SQL unit tests).
- [ ] Commit 3: CAST end-to-end (binder incl. literal check, `reg_domain`, lowering matrix,
      F32 emit arm) + E2E suite.
