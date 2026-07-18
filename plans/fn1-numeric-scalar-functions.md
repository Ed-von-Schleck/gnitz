# Numeric scalar functions and numeric CAST

## 1. Problem

The SQL surface has no scalar functions and no CAST. The only callable names are the five
aggregates (`agg_func_from_name`, `crates/gnitz-sql/src/ast_util.rs:79-90`) plus the
COALESCE/NULLIF bind-time desugars (`crates/gnitz-sql/src/bind/structural.rs:99-102`). Every
other function name errors `function '…' not supported` (`structural.rs:365-370`), and
`Expr::Cast` / `x::type` falls into `bind_structural`'s catch-all (`structural.rs:216-218`).

This plan adds the numeric core: `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND(x[, n])`,
`TRUNC(x)`, `MOD(a, b)`, `GREATEST(…)`, `LEAST(…)`, and `CAST(x AS <numeric type>)` /
`x::<numeric type>`. All are pure per-row value transforms — linear map operators in DBSP
terms, incrementally free, no state, no trace. String-typed functions and string CAST are a
separate piece of work (they need a register representation for strings that this plan does
not touch). Deliberately excluded from the surface this plan commits to: regex, math
transcendentals (POWER/SQRT/EXP/LN/LOG), SIGN, and bitwise operators — none of them earns a
slot in the numeric core, and each is a plain register op if ever wanted.

### Global semantic rules (these four rules decide every edge case below)

1. **Total functions, no runtime errors.** Circuit expression evaluation has no error path —
   a view tick cannot abort mid-circuit. The existing precedent is division by zero → NULL
   (`div_like!`, `crates/gnitz-engine/src/expr/batch.rs:475-501`). Every new function follows
   it: a domain error (NaN→int, out-of-range cast, finite f32 overflow) yields SQL NULL,
   never an error and never a clamped value.
2. **Determinism is mandatory.** A retraction re-runs the same expression over the same
   payload and must cancel byte-exactly under (PK, payload) consolidation. Every function here
   is a pure bit-level function of its inputs. Volatile functions (NOW, RANDOM) are
   structurally inadmissible in this engine and are not a "later" item.
3. **Wrapping integer arithmetic.** The engine wraps (`wrapping_add`/`wrapping_neg`,
   `batch.rs:659-672`); `ABS` follows: `wrapping_abs`, so `ABS(i64::MIN) = i64::MIN`. CAST is
   the exception by design: it is the one operation whose *job* is the range check, so an
   out-of-range cast is NULL (rule 1), not a wrap.
4. **Both backends agree or reject.** Every new `BoundExpr` variant gets an arm in both
   `BoundExprBackend` impls (compiler enforces exhaustiveness through the single `match` in
   `lower_bound_expr`, `crates/gnitz-sql/src/lower.rs:46-64`), and where both backends
   evaluate, they must produce the same rows — a point-DML residual filter must not diverge
   from the identical view filter. The client residual interpreter (`exec/eval.rs`) is
   int-only; float-involving forms return `Unsupported` there, exactly as float
   literals/columns already do (`eval.rs:92-96`). U64 handling in the interpreter must mirror
   the engine's (its U64 columns arrive as bit-reinterpreted i64, `eval.rs:26-29`).
   **Included fix:** the interpreter today returns `Some(0)` for a zero divisor
   (`eval.rs:193-205`) where the engine returns NULL — that is a live filter divergence and
   contradicts rule 1's precedent; this plan changes both `Div` and `Mod` interpreter arms to
   return `Ok(None)` on a zero divisor.

## 2. Function semantics (fully decided)

| Function | Arg type(s) | Result type (`infer_type`) | Semantics |
|---|---|---|---|
| `ABS(x)` | int | `unify_numeric(t,t)` (I64/U64) | `wrapping_abs` on the i64 register. Unsigned-typed arg (U8..U64 per `infer_type`): identity, folded at lowering, no opcode. |
| `ABS(x)` | float | F64 | `f64::abs` (`-0.0 → +0.0`, NaN → NaN). |
| `FLOOR(x)`, `CEIL(x)`, `CEILING(x)` | float | F64 | `f64::floor` / `f64::ceil`. Int arg: identity fold, no opcode. |
| `ROUND(x)` | float | F64 | `f64::round_ties_even` — matches IEEE-754 roundTiesToEven and PostgreSQL `round(float8)` (which uses `rint`); `round(2.5) = 2`. Int arg: identity fold. |
| `ROUND(x, n)` | any numeric, int literal | F64 | `n` must be an integer literal in `-15..=15`, else Bind error. Bind-time desugar: `n ≥ 0` → `ROUND(x * 10ⁿ) / 10ⁿ`; `n < 0` → `ROUND(x / 10⁻ⁿ) * 10⁻ⁿ` — always scaling by the **positive** power `10^{abs(n)}`, which is exactly representable in f64 (10⁰…10¹⁵ are; negative powers are not, which is why the n<0 form divides first). The constant is baked at bind as `LitFloat(10f64.powi(|n|))`, so an int `x` is lifted to float by the arithmetic and the result is always F64 — the identity fold applies to the 1-arg form only. Committed semantics are f64 arithmetic, not decimal: results can differ from exact decimal rounding by double-rounding (e.g. in the last ulp for large `n`), and a huge `x·10ⁿ` can overflow to ±∞ (consistent with existing float-op overflow behavior). PostgreSQL has no `round(float8, int)` at all, so 2-arg ROUND is this plan's own definition. |
| `TRUNC(x)` | float | F64 | `f64::trunc` (toward zero). Int arg: identity fold. One argument only; `TRUNC(x, n)` is a Bind error. |
| `MOD(a, b)` | int, int | I64/U64 | Pure bind-time desugar to `BinOp(a, Mod, b)` — identical to `a % b`, including divisor-0 → NULL (on both backends, after the rule-4 interpreter fix) and the float-modulo rejection (`lower.rs:349-350`). |
| `GREATEST(a, …)` / `LEAST(a, …)` | numeric, ≥ 1 arg | `unify_numeric` fold over args | PostgreSQL NULL semantics: NULL args are skipped; result is NULL only when all args are NULL. First-class n-ary IR node lowered as a left fold of the 2-ary null-skipping MAX2/MIN2 opcodes (§2.1) — O(n) registers, computed args fully supported. Non-numeric arg (string/blob/wide-int) → typed error at lowering. Arity is bounded only by the 64-register file (`2n−1` regs pure-int, `3n−2` with float lifts → ≈ 22–32 args; beyond it CREATE VIEW fails with the engine's `TooManyRegs` rejection, the same bound every large expression has). |
| `CAST(x AS T)` / `x::T` | numeric → numeric | `T` | §2.2 below. |

NULL propagation for all of the above: NULL in → NULL out (via `null_copy1`), except
GREATEST/LEAST (skip-NULL natively in the opcode).

### 2.1 MAX2/MIN2 semantics

`MAX2(a, b)` (and mirror `MIN2`): if `a` is NULL → `b` (with `b`'s null bit); if `b` is NULL
→ `a`; else the larger (smaller) of the two. Result is NULL iff both are NULL
(`null = a_null & b_null`). A NULL-literal arg (`GREATEST(NULL, x)`) needs no bind-time
special case — `load_null` feeds the opcode and is skipped natively.

- **Integer compare domain is one GLOBAL lowering decision**, exactly like CASE's float
  unification (`lower.rs:264-284`): when the `unify_numeric` fold over all args is U64, the
  whole fold compares unsigned; else signed. Per-instruction, the engine picks unsigned when
  either operand's `reg_tc` is U64 (the ordered-`Cmp` rule,
  `crates/gnitz-engine/src/expr/program.rs:709-714`) with `propagate_u64` making the taint
  sticky — but the taint only exists *from the first U64 operand onward*, so a naive
  in-SQL-order fold would compare early signed pairs signed and be argument-order-dependent
  (`GREATEST(-1, 1, u)` ≠ `GREATEST(u, -1, 1)`). The lowering therefore **rotates one
  U64-inferring arg to the head of the fold** whenever the unified type is U64: the first
  MAX2 is then unsigned, the taint propagates, every subsequent fold is unsigned, and the
  result is argument-order-independent (null-skip extremum is commutative once the domain is
  fixed, so the rotation is semantics-free). Consequence, documented: a mixed signed/U64
  argument list compares in the unsigned domain (`GREATEST(5::u64, -1)` is `-1` read as
  2⁶⁴−1 → result 2⁶⁴−1 typed U64) — identical to what `WHERE u > -1` already does.
- **Float compare** uses `f64::total_cmp` — the same total order the engine sorts rows by
  (CLAUDE.md §5): NaN orders above +∞, `-0.0 < +0.0`. So `GREATEST(5.0, NaN) = NaN`
  (PostgreSQL-compatible, which sorts NaN greatest) and `LEAST(5.0, NaN) = 5.0`, with a
  strict total order guaranteeing determinism.
- `reg_tc` propagation: `propagate_u64(a, b)`, like `IntAdd`/`Select` (`program.rs:678-729`).

### 2.2 CAST semantics

Accepted targets: every `sql_type_to_typecode` numeric — I8/I16/I32/I64, U8/U16/U32/U64
(`TINYINT`…`BIGINT [UNSIGNED]`), F32 (`FLOAT`), F64 (`DOUBLE`/`DOUBLE PRECISION`/`REAL`).
Everything else (`TEXT`/`VARCHAR`/`CHAR`, `UUID`, `DECIMAL(38|39,0)`, `BOOLEAN`, unknown) is
an `Unsupported` bind error: `CAST to {T:?} is not supported`. Source must be a numeric
expression — a STRING/BLOB/wide-int source is already rejected by the operand lowering
(`OpcodeBackend::col_ref`, `lower.rs:193-222`). `CastKind::Cast` and `CastKind::DoubleColon`
are accepted; `TryCast`/`SafeCast`, a non-`None` `format`, and `array: true` (sqlparser
parses `CAST(x AS INT ARRAY)` under every dialect) are `Unsupported` — the `Expr::Cast`
destructure names all five fields, no `..`.

**Bind-time literal check:** an integer-literal source (`LitInt` or `Neg(LitInt)`, the same
shapes `fold_int_literal` recognizes, `lower.rs:71-80`) that is statically outside an integer
target's domain is a Bind error at CREATE time (`CAST(300 AS TINYINT)`), not a per-row NULL.

Runtime semantics matrix (register values are i64 images / f64 bit patterns; `src_signed` is
the resolve-time U64 tracking, `program.rs:634-635`):

- **int → int.** Value unchanged when in the target's domain, NULL otherwise. Statically
  elided (no opcode) when `int_domain_fits(reg_domain(inner), target)`
  (`crates/gnitz-sql/src/types.rs:96-103`), where **`reg_domain` is NOT `infer_type`**:

  ```rust
  /// The static domain of the VALUE in the register, as a type code. infer_type
  /// alone is unsound here: UnaryOp::Neg infers its inner type (ir.rs:86) but the
  /// engine negates on the full i64 register — CAST(-x AS INT) at x = i32::MIN
  /// holds +2^31, outside I32. Only nodes whose register value provably lies in
  /// their inferred type's domain may report a narrow domain.
  fn reg_domain(e: &BoundExpr, schema: &Schema) -> TypeCode {
      match e {
          BoundExpr::ColRef(i) => schema.columns[*i].type_code, // loads are bounded
          BoundExpr::Cast { to, .. } => *to,   // checked-or-NULL by construction
          _ => { let t = e.infer_type(schema); unify_numeric(t, t) } // I64/U64/F64
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
truncation (`plan.rs:536-543`) lossless by construction; a NULL result row writes value 0 +
null bit as today (`plan.rs:530-534`). This is the first path that can produce a computed
F32 output column, which the EMIT kernel currently cannot write (it would slice raw i64
register bits to 4 bytes); §6.3 adds the F32 emit arm.

`reg_tc` flow: `EXPR_INT_CAST` with target U64 sets `reg_tc[dst] = U64` (a downstream ordered
compare picks the unsigned variant); every other cast dst sets 0. `infer_type` returns the
target `TypeCode`, so `unify_numeric` sees U64/F64 correctly downstream.

## 4. Client side (gnitz-sql, gnitz-core)

### 4.1 IR (`crates/gnitz-sql/src/ir.rs`)

Three new variants on `BoundExpr` (`ir.rs:14-48`) — the unary-function enum is named
`NumFunc` (not "ScalarFunc": the engine already has an unrelated `pub struct ScalarFunc`,
`crates/gnitz-engine/src/expr/plan.rs:245`, and this enum only ever holds unary numeric
transforms):

```rust
/// Deterministic unary numeric scalar function. Two-arg ROUND and MOD are
/// bind-time desugars and never construct this.
Func {
    f: NumFunc,
    arg: Box<BoundExpr>,
},
/// GREATEST (is_max) / LEAST (!is_max): n-ary null-skipping extremum,
/// lowered as a left fold of MAX2/MIN2. args is non-empty.
MinMaxN {
    is_max: bool,
    args: Vec<BoundExpr>,
},
/// CAST(expr AS to) / expr::to, numeric-to-numeric only.
Cast {
    expr: Box<BoundExpr>,
    to: TypeCode,
},
```

with `enum NumFunc { Abs, Floor, Ceil, Round, Trunc }`.

`infer_type` (`ir.rs:66-113`) arms:

```rust
BoundExpr::Func { f, arg } => {
    let t = arg.infer_type(schema);
    match f {
        NumFunc::Abs => unify_numeric(t, t),
        NumFunc::Floor | NumFunc::Ceil | NumFunc::Round | NumFunc::Trunc => {
            if t.is_float() { TypeCode::F64 } else { unify_numeric(t, t) }
        }
    }
}
BoundExpr::MinMaxN { args, .. } => {
    let mut ty = args[0].infer_type(schema);
    for a in &args[1..] { ty = unify_numeric(ty, a.infer_type(schema)); }
    ty
}
BoundExpr::Cast { to, .. } => *to,
```

### 4.2 Binding (`crates/gnitz-sql/src/bind/structural.rs`)

All interception is structural (schema-free), in `bind_structural`'s match, alongside the
COALESCE/NULLIF arms (`structural.rs:99-102`) — scalar functions are context-independent, so
every binding context (WHERE, projection, HAVING, join residual, DML) gets them at once:

- `Expr::Function(f)` name arms (via `fn_name_is`): `"abs"`, `"ceiling"`, `"round"`,
  `"trunc"`, `"mod"`, `"greatest"`, `"least"`. (`CEILING` is NOT keyword-dispatched by
  sqlparser — only `CEIL`/`FLOOR` route to `parse_ceil_floor_expr` — so it arrives as a
  plain function; `CEIL(x)`/`FLOOR(x)` arrive as the dedicated AST nodes below and never as
  functions.) Each arm uses `function_positional_args` + `reject_unsupported_fn_qualifiers`
  (as COALESCE does) and checks arity:
  - `abs`/`trunc`/`ceiling`: exactly 1 arg → `BoundExpr::Func`.
  - `round`: 1 arg → `Func`; 2 args → the §2 desugar, emitted unconditionally (the binder
    is schema-free and cannot type the arg; the `LitFloat` scale constant lifts an int `x`
    to float, so 2-arg ROUND always produces F64). The scale must be `Expr::Value(Number)`
    or a `UnaryOp::Minus`/`UnaryOp::Plus` of one, parsed to `i64`, in `-15..=15`, else
    `Bind("ROUND: scale must be an integer literal in -15..=15")`.
  - `mod`: exactly 2 args → `BinOp(a, Mod, b)`.
  - `greatest`/`least`: ≥ 1 arg, each bound via `bind_structural` → `BoundExpr::MinMaxN`.
    No literal/column restriction, no null-test machinery — NULL skipping is the opcode's.
- `Expr::Floor { expr, field }` / `Expr::Ceil { expr, field }`: accept only
  `field == CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)` (the bare-call parse,
  sqlparser `parser/mod.rs:2856-2889`) → `BoundExpr::Func`; a `TO <datetime>` field or a
  scale is `Unsupported`.
- `Expr::Cast { kind, expr, data_type, array, format }` (all five fields destructured):
  accept `kind ∈ {Cast, DoubleColon}`, `array == false`, `format.is_none()`; map `data_type`
  through `sql_type_to_typecode` (`types.rs:5-33`), reject non-numeric targets per §2.2;
  apply the bind-time literal range check (§2.2); produce `BoundExpr::Cast`.

The unknown-function error in `SingleTable::bind_function` (`structural.rs:365-370`) is
untouched — names not intercepted still error there.

**Pre-bind AST walkers** (`crates/gnitz-sql/src/ast_util.rs:128-176`): `expr_operands` must
gain arms for the three AST nodes this plan adds to the binder's vocabulary —
`Expr::Floor`/`Expr::Ceil` contribute `expr`, `Expr::Cast` contributes `expr` — upholding
the invariant its doc comment states ("a node added to the binder's vocabulary reaches them
all at once"). Today these nodes fall into the `_ => Vec::new()` wildcard, harmless only
because they fail to bind; with that backstop removed, the wildcard breaks three consumers:
`expr_contains_excluded` (`dml/insert.rs:353-360` — an `EXCLUDED.b` hidden under
`CAST(EXCLUDED.b AS …)` in `ON CONFLICT DO UPDATE` would pass the guard and silently bind
to the *existing* row's column: data corruption), `count_side_refs`
(`plan/view/exists.rs:977-1008` — correlation misclassification), and `expr_has_aggregate`
(`ast_util.rs:119-126` — `CAST(SUM(x) AS INT)` would misroute to the scalar path). The
plain-`Function` names (abs/ceiling/round/trunc/mod/greatest/least) are already covered by
the existing Function-args arm.

**HAVING pre-collection** (`crates/gnitz-sql/src/plan/view/group_by.rs`):
`collect_having_aggs` (`group_by.rs:845-872`) currently treats *every* `Expr::Function` in
HAVING as an aggregate — `having_agg_func` (`group_by.rs:683-691`) calls `bind_function`
directly and propagates its `Unsupported` for any non-aggregate name, so
`HAVING ABS(SUM(x)) > 0` would die before binding (the same pre-existing hole breaks
`HAVING COALESCE(SUM(x), 0) > 5` today). Fix, included here: when
`single_fn_name(f).and_then(agg_func_from_name)` is `None`, both `collect_having_aggs` and
`bind_having_null_test` (`group_by.rs:1011-1018`) fall through to the operand recursion
(`expr_operands` reaches function arguments) instead of erroring — aggregates nested inside
scalar calls are collected, and genuinely unknown names still error at bind. This fixes the
COALESCE-in-HAVING hole as a side effect.

### 4.3 Opcode lowering (`crates/gnitz-sql/src/lower.rs`)

Trait `BoundExprBackend` (`lower.rs:17-42`) gains three methods; `lower_bound_expr`
dispatches the three new variants:

```rust
fn func(&mut self, f: NumFunc, arg: &BoundExpr) -> Result<Self::Out, GnitzSqlError>;
fn min_max_n(&mut self, is_max: bool, args: &[BoundExpr]) -> Result<Self::Out, GnitzSqlError>;
fn cast(&mut self, expr: &BoundExpr, to: TypeCode) -> Result<Self::Out, GnitzSqlError>;
```

`OpcodeBackend::func`:

```rust
let (r, is_float) = lower_bound_expr(arg, self)?;
match f {
    NumFunc::Abs if is_float => Ok((self.eb.float_abs(r), true)),
    NumFunc::Abs => {
        // Unsigned register values are non-negative by definition: identity.
        if !arg.infer_type(self.schema).is_signed_int() { Ok((r, false)) }
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

`OpcodeBackend::min_max_n`: gate every arg first — `infer_type` must be neither
`is_wide_int` nor `is_german_string`, else
`Unsupported("GREATEST/LEAST: not supported on {t:?}")` (the backend holds the schema; the
schema-free binder cannot do this). Then lower every arg, apply the same global float
unification `case` uses (`lower.rs:264-284`: any float arg → lift every int arg via
`int_to_float`), and — when the unified type is U64 — rotate one U64-inferring arg's
register to the fold head (§2.1) so the whole fold compares unsigned. Fold left with
`eb.max2`/`eb.min2` (float variants when unified float). Registers: n loads + (n−1) folds
+ lifts — O(n).

`OpcodeBackend::cast` implements the §2.2 matrix: compute `d = reg_domain(expr, schema)`
(§2.2 — NOT plain `infer_type`), lower the operand, then

- int source, int target: `int_domain_fits(d, to)` → identity; else `eb.int_cast(r, to)`.
- int source, float target: `eb.int_to_float(r)` (+ `eb.float_to_f32` if `to == F32`).
- float source, int target: `eb.float_to_int(r, to)`.
- float source, F64: identity; F32: `eb.float_to_f32(r)`.
- Returned `is_float` = `to.is_float()`.

`InterpBackend` (`crates/gnitz-sql/src/exec/eval.rs:30-243`) — every arm must mirror the
engine bit-for-bit (rule 4), including U64 handling (`self.schema` is available):

- `func`: `Abs` — `if arg.infer_type(self.schema).is_signed_int() { v.wrapping_abs() } else { v }`
  (the unsigned fold, same as the opcode backend; without it `ABS(u64col)` at 2⁶⁴−1 would
  return 1 instead of the identity). `Floor/Ceil/Round/Trunc`: identity on the int value
  (float operands already error at the leaf).
- `min_max_n`: gate args as the opcode backend does; evaluate all args; skip `None`s; all
  `None` → `None`; compare as `u64` when the `unify_numeric` fold over the args is U64,
  else as `i64` — the exact global domain the rotated engine fold computes (§2.1), so the
  two backends agree for every argument order.
- `cast`: int target — `let src_u64 = expr.infer_type(self.schema) == TypeCode::U64;` widen
  the value as `if src_u64 { v as u64 as i128 } else { v as i128 }` and check against
  `FixedInt::range()` (`gnitz-wire/src/types.rs:392-403`), `None` when out of range, value
  unchanged when in range. (Without `src_u64`, `CAST(u64col AS SMALLINT)` at 2⁶⁴−1 would
  pass as −1 where the engine says NULL.) The static elision applies identically
  (`int_domain_fits(reg_domain(..), to)` → skip the check). Float target → `Unsupported`.
- `binop` Div/Mod: the rule-4 fix — zero divisor returns `Ok(None)` (was `Some(0)`,
  `eval.rs:193-205`). No existing test pins `Some(0)` (the `_no_panic` tests use divisor −1
  and assert only `is_ok`); the new zero-divisor parity tests in §8 pin the fix.

### 4.4 Builder (`crates/gnitz-core/src/expr.rs`)

Thirteen emitters, all one-liners over the existing `unary_op`/`binary_op` helpers
(`expr.rs:66-76`): `int_abs`, `float_abs`, `float_floor`, `float_ceil`, `float_round`,
`float_trunc`, `float_to_f32` (plain unary); `max2`/`min2`/`float_max2`/`float_min2`
(plain binary); and `int_cast(reg, tc)` / `float_to_int(reg, tc)` emitted as
`binary_op(OP, reg, tc as u32)` — the target type code rides the `a2` word, the same
one-word-payload convention `INT_IN_SET` uses for its pool index (`TypeCode`'s `#[repr(u8)]`
discriminants are the wire codes, so `tc as u32` round-trips).

## 5. Wire (`crates/gnitz-wire/src/expr.rs`)

Thirteen new opcode constants — 37–39 fill the existing gap, the rest continue after 46:

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

No blob-format change (version stays 1; the codes are additive and client+engine ship
together).

## 6. Engine (`crates/gnitz-engine/src/expr/`)

### 6.1 `program.rs` — decode, resolve, validate, analyses

Following the file's own op-family pattern (`CmpOp` collapses 12 wire compares into 2
variants, `program.rs:50-59`; `StrOp` 6 into 2, `program.rs:63-67`), the five pure float
unaries collapse into **one** variant carrying an operator enum; the per-op wire constants
stay (matching existing wire granularity). New in both `LogicalInstr` (`program.rs:76-221`)
and `Instr` (`program.rs:231-396`):

```rust
/// Pure float unary transform (no NULL production, no type payload).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FloatUnaryOp { Abs, Floor, Ceil, Round, Trunc }

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

`from_wire` (`program.rs:483-587`): one arm per wire constant; cast arms carry `tc: q[3]`
verbatim (full u32 — a forged high-bit word must reach `validate`, not be silently truncated).

`resolve` (`program.rs:641-817`) arms:

- `FloatUnary`, `FloatToF32`: `reg_tc[dst] = 0`.
- `IntAbs`: `reg_tc[dst] = reg_tc[a]` (like `IntNeg`, `program.rs:700-703`).
- `IntCast`: `src_signed = !is_u64(reg_tc[a])`; narrow `tc` to u8;
  `reg_tc[dst] = if tc == U64 { U64 } else { 0 }`.
- `FloatToInt`: narrow `tc`; `reg_tc[dst] = if tc == U64 { U64 } else { 0 }`.
- `IntMinMax2`: `signed = !any_u64(&reg_tc, a, b)` (the ordered-`Cmp` rule,
  `program.rs:709-714`); `reg_tc[dst] = propagate_u64(&reg_tc, a, b)`.
- `FloatMinMax2`: `reg_tc[dst] = 0`.

`validate` (`program.rs:889-999`): `FloatUnary`/`IntAbs`/`FloatToF32` join the existing
unary arm (`program.rs:924-927`); `IntMinMax2`/`FloatMinMax2` join the binary
register-op arm (bounds + SSA anti-aliasing, `program.rs:893-912`). `FloatToInt`/`IntCast`
get their own arm: `check_reg(dst)`, `check_reg(a)`, and
`tc <= u8::MAX && gnitz_wire::is_fixed_int(tc as u8)` (`gnitz-wire/src/types.rs:323`,
re-exported via `crate::schema`), else a new `ExprValidateErr::BadCastTarget { tc: u32 }`
— a forged blob must not reach eval with a junk type code.

Analyses:

- `each_reg_read` (`program.rs:1099-1140`): the five unary shapes read `a`; the two MinMax2
  shapes read `a` and `b`.
- `classify_registers` (`program.rs:1226-1307`): all seven are value producers;
  unaries add `non_bool_read |= 1 << a`, MinMax2 adds both operands — join the
  `IntNeg | FloatNeg | IntToFloat` / `IntAdd…` arms respectively.
- `is_strictly_non_nullable` (`program.rs:1313-1369`): `FloatToInt`, `IntCast`, and
  `FloatToF32` can manufacture NULL → `return false` (join the `IntDiv`/`IntMod`/`FloatDiv`
  arm). `FloatUnary`, `IntAbs`, `IntMinMax2`, `FloatMinMax2` only propagate/consume → join
  the safe remainder (MinMax2's output is null only when both inputs are, which the operand
  columns' nullability already accounts for).
- `and_chain_mask` needs no change (spine links are `BoolAnd`-written only).

### 6.2 `batch.rs` — eval arms

`FloatUnary` and `IntAbs` share the `IntNeg`/`FloatNeg` loop shape (`batch.rs:666-676`,
`722-732`): hoisted bases, per-row transform, `null_copy1`, `maybe_pack_bool_bits`. The
`FloatUnaryOp` match sits **outside** the row loop (the `Instr::Cmp` pattern,
`batch.rs:739-752`). Bodies:

```rust
IntAbs   => regs[d] = regs[a].wrapping_abs()
Abs      => encode_f64(decode_f64(x).abs())
Floor    => encode_f64(decode_f64(x).floor())
Ceil     => encode_f64(decode_f64(x).ceil())
Round    => encode_f64(decode_f64(x).round_ties_even())
Trunc    => encode_f64(decode_f64(x).trunc())
```

`FloatToInt` / `IntCast` / `FloatToF32` need a unary analogue of `div_like!`
(`batch.rs:475-501`): compute unconditionally, accumulate a per-row fail mask, `null_copy1`,
then OR the fail mask into the dst null word. New macro `unary_null_like!` with body
returning `(val: i64, fail: bool)`:

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

`IntMinMax2` / `FloatMinMax2` cannot use `bin_op!` (its `null_or2` epilogue is exactly the
wrong null rule) — they get a dedicated arm shaped like `Select`'s nullable path
(`batch.rs:901-944`):

- `no_nulls` arm: straight per-row extremum over `reg3` — int: `max/min` (signed) or
  `(x as u64).max(y as u64) as i64`; float: `total_cmp`-pick.
- nullable arm: read the two operand null words; per word compute
  `nd = na & nb`; per row pick `if a_null { b } else if b_null { a } else { extremum }`;
  write `nd` to the dst null word. Epilogue `maybe_pack_bool_bits`.

### 6.3 F32 emit arm (`plan.rs`)

`EmitCol` (`plan.rs:230-234`) gains an `is_f32: bool` (set in `from_map`,
`plan.rs:284-325`, when the output column's `type_code == F32`); in the emit kernel
(`plan.rs:517-545`) the `is_f32` arm takes precedence over the generic narrow-stride slice:
`let bits = (f64::from_bits(val as u64) as f32).to_bits()`, write 4 bytes. (The
`float_to_f32` opcode already rounded and range-checked, so this is the mechanical
bits→f32-bits narrowing, not a second rounding.) The stride-8 fast arm and the narrow-int
arm are unchanged.

### 6.4 Untouched by design

`ScalarFunc::from_predicate`/`from_map` construction (beyond §6.3), `op_filter`/`op_map`,
the compiler's `emit.rs` plumbing (`emit.rs:428-433` validates and builds
opcode-agnostically), the ad-hoc read path (`catalog/scan_spec.rs` flows through the same
`decode_expr_blob → from_wire → validate → ScalarFunc` pipeline, so the new opcodes and the
F32 emit arm reach it automatically), and the reduce/aggregate layer. The client residual
path (`exec/residual.rs`, `dml/mutate.rs`) picks up the new IR arms through the shared walk
automatically. `gnitz-capi` is deliberately untouched: its `ExprBuilder` mirror is already a
partial surface (no float arithmetic exposed), and extending it is not part of this plan.

## 7. Edit surface

| File | Change |
|---|---|
| `crates/gnitz-wire/src/expr.rs:3-60` | 13 opcode constants (37-39, 47-56) |
| `crates/gnitz-core/src/expr.rs` | 13 `ExprBuilder` emitters; import list |
| `crates/gnitz-sql/src/ir.rs:14-48,66-113` | `Func`/`MinMaxN`/`Cast` variants, `NumFunc` enum, `infer_type` arms |
| `crates/gnitz-sql/src/bind/structural.rs:93-219` | `Expr::Floor`/`Expr::Ceil`/`Expr::Cast` arms; name interceptions abs/ceiling/round/trunc/mod/greatest/least; ROUND-scale validation + two-sided desugar; CAST literal range check |
| `crates/gnitz-sql/src/ast_util.rs:135-176` | `expr_operands` arms for `Expr::Floor`/`Expr::Ceil`/`Expr::Cast` |
| `crates/gnitz-sql/src/plan/view/group_by.rs:683-691,845-872,1011-1018` | non-aggregate-name fall-through in `collect_having_aggs` / `bind_having_null_test` |
| `crates/gnitz-sql/src/lower.rs:17-64` | trait methods `func`/`min_max_n`/`cast`; dispatch arms; `OpcodeBackend` impls; `reg_domain` |
| `crates/gnitz-sql/src/exec/eval.rs` | `InterpBackend::func`/`min_max_n`/`cast`; Div/Mod zero-divisor → `None` fix (+ test updates) |
| `crates/gnitz-engine/src/expr/program.rs` | 7 `LogicalInstr` + 7 `Instr` variants, `FloatUnaryOp`; `from_wire`, `resolve`, `validate` (+`BadCastTarget`), `each_reg_read`, `classify_registers`, `is_strictly_non_nullable` arms |
| `crates/gnitz-engine/src/expr/batch.rs` | `FloatUnary`/`IntAbs` arms; `unary_null_like!`; `IntCast`/`FloatToInt`/`FloatToF32` arms; `IntMinMax2`/`FloatMinMax2` arms |
| `crates/gnitz-engine/src/expr/plan.rs:230-234,284-325,517-545` | `EmitCol.is_f32` + emit arm |

## 8. Tests

**Engine unit (`crates/gnitz-engine/src/expr/tests/program_tests.rs`):**
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

**SQL unit (`lower.rs`, `structural.rs`, `ir.rs`, `eval.rs` test mods):**
- Fold elisions: `FLOOR(intcol)` no opcode; `ABS(u32col)`/`ABS(u64col)` no opcode;
  `CAST(i32 AS BIGINT)` no opcode; `CAST(i64 AS INT)` emits `EXPR_INT_CAST`;
  **`CAST(-intcol AS INT)` emits `EXPR_INT_CAST`** (the `reg_domain` fix — must NOT elide).
- ROUND 2-arg: both desugar sides (`n ≥ 0` multiply-first, `n < 0` divide-first);
  scale-literal rejection (non-literal, out of range, float); exact negative-scale results
  (`ROUND(881469.0444980001, -5) = 900000.0`).
- MOD desugar ≡ `%`; float MOD rejected; **`MOD(x, 0)` → NULL on both backends**
  (interp Div/Mod fix pinned).
- GREATEST/LEAST: MinMaxN shape; computed args accepted (`GREATEST(a+1, b)`); `-1` literal
  arg accepted; NULL-literal arg skipped at runtime; string/U128 arg rejected at lowering;
  4+ args compile (O(n) registers pinned: assert `num_regs` linear in arity);
  mixed-U64 argument-order independence (`GREATEST(-1, 1, u)` ≡ `GREATEST(u, -1, 1)` — the
  fold-head rotation pinned on both backends).
- Walker coverage: `ON CONFLICT DO UPDATE SET a = CAST(EXCLUDED.b AS BIGINT)` is rejected
  by `expr_contains_excluded` (not silently mis-bound); `SELECT CAST(SUM(x) AS INT)` routes
  to the aggregate path; `HAVING ABS(SUM(x)) > 0` and `HAVING COALESCE(SUM(x), 0) > 5` both
  plan (the collect fall-through).
- CEILING binds (function-name path); `CEIL(x TO DAY)` rejected.
- CAST: `::` binds; TRY_CAST rejected; `CAST(x AS INT ARRAY)` rejected; TEXT/UUID/BOOLEAN
  targets rejected; `CAST(300 AS TINYINT)` → Bind error; infer_type returns target tc (U64
  preservation through `unify_numeric` pinned).
- Interp/engine parity: `CAST(u64col AS SMALLINT)` and `ABS(u64col)` at 2⁶⁴−1 produce
  engine-identical results through `eval_expr`.

**E2E (`crates/gnitz-py/tests/test_numeric_functions.py`, `GNITZ_WORKERS=4`):**
- A view projecting each function over a table with NULLs; insert → check, update (retract +
  insert) → check the old row vanished (weight cancellation with computed columns), delete →
  empty. This is the retraction-determinism proof for the whole opcode set.
- Filters: `WHERE ABS(x) > k`, `WHERE CAST(f AS INT) = k` (NULL rows excluded),
  `WHERE GREATEST(a, b) >= k`.
- CAST narrowing view: out-of-range source rows show NULL; schema readback shows the narrow
  column type; F32 computed column round-trips; `CAST(1e300 AS FLOAT)` → NULL.
- HAVING with `ABS(SUM(x))` (reduce-output schema binding path).
- Ad-hoc SELECT with the same expressions (read-spec path).
- Point-DML `DELETE … WHERE MOD(x, 2) = 0` and a U64 CAST residual (interp parity live).

## 9. Sequencing

- [ ] Commit 1: wire constants + `ExprBuilder` emitters + engine decode/resolve/validate/eval
      (+ engine tests) — the engine accepts the opcodes before any SQL produces them.
- [ ] Commit 2: IR + binder + lowering + interpreter (incl. the Div/Mod zero-divisor fix)
      for ABS/FLOOR/CEIL/CEILING/ROUND/TRUNC/MOD/GREATEST/LEAST (+ SQL unit tests).
- [ ] Commit 3: CAST end-to-end (binder incl. literal check, `reg_domain`, lowering matrix,
      F32 emit arm) + E2E suite.
