# x3: conditional-select opcode (CASE/COALESCE/NULLIF) + mark joins for subqueries in boolean positions

## Problem

Two coupled gaps:

1. The expression VM has no conditional. The opcode space
   (`gnitz-wire/src/expr.rs:1-43`) stops at comparisons/bool-ops/`IS [NOT] NULL`/string
   compares; the only conditional-NULL idiom is the div-by-zero gate
   (`sum / (cnt != 0)`, `plan/view/group_by.rs:567-578`). SQL `CASE WHEN`, `COALESCE`,
   `NULLIF` are unsupported everywhere.
2. `[NOT] EXISTS` / `[NOT] IN` subqueries work only as top-level AND conjuncts of a view
   WHERE. Under `OR`/`NOT`, inside `CASE`, or in the SELECT list they are rejected. The
   standard fix (Hyper/DuckDB) is the **mark join**: compute the subquery's truth value
   as a column, then let ordinary 3VL expression evaluation consume it.

The mark column is a non-nullable `0/1` i64 column and no three-valued mark machinery is
needed, because every subquery predicate the mark path admits is **two-valued**: EXISTS
is always two-valued in SQL, and a mark-position `IN (SELECT …)` is admitted only when
its operand and subquery column are NOT NULL (§5). That is strictly stronger than the
top-level-conjunct form (which rejects only nullable *NOT* IN, since a positive nullable
IN conjunct is two-valued-safe): a mark can be negated or projected, so the "IN mark
would be NULL" case — a NULL outer operand or NULL subquery value — is rejected outright
regardless of polarity rather than encoded as `mark = 0`.

## Prerequisite state

- Semi/anti circuits for equi/band/pure-range correlation exist
  (`plan/view/exists.rs`: `emit_equi_exists`/`emit_range_exists`, `semi_or_anti`, and the
  `ν = positive_diff(a_all, π_A(inner))` null-fill), dispatched by `plan/view/dispatch.rs`
  (`ViewShape::Subquery`) for a single top-level `[NOT] EXISTS`/`[NOT] IN` WHERE conjunct
  (`NOT` peeled into `outer_not`). Views lower through the bound IR (`plan/lp.rs`:
  `Rel::Source/Filter/Project`); hidden-view segments chain via `ViewChain` /
  `create_view_chain`. Verify against the live tree before starting.

## Scope (committed)

1. **`CASE WHEN … [WHEN …] [ELSE …] END`** (searched and simple-operand forms),
   **`COALESCE(a, b, …)`**, **`NULLIF(a, b)`** for integer and float operands in:
   simple-view WHERE and computed projections, HAVING (incl. over aggregate results),
   join residuals, DML residual WHERE, and `UPDATE … SET col = CASE …` (which flows
   through `eval_set_expr` → the client evaluator; tested explicitly). Grouped-view
   SELECT lists and set-op branch projections keep their existing computed-expression
   rejections (`group_by.rs:271`, `set_op.rs:110,126` — clean errors, now with tests
   pinning them). String-typed branch results are rejected (`Unsupported`; the register
   file carries 8-byte values — same boundary as computed projections today).
2. **One subquery predicate in an arbitrary boolean position** of a view WHERE and as a
   projected column: `WHERE x = 1 OR EXISTS(…)`,
   `WHERE NOT (EXISTS(…) AND y > 2)`, `SELECT a.*, EXISTS(…) AS has_child`,
   `CASE WHEN EXISTS(…) THEN … END`. Exactly one subquery per view (two or more →
   compose via stacked views; no regression — multi-subquery is rejected today).
   Per-subquery restrictions carry over from the
   conjunct form (correlation vocabulary, no same-source, single inner FROM) with one
   deliberately **stricter** rule: a mark-position `IN (SELECT …)` requires the outer
   operand **and** the subquery column to be NOT NULL **regardless of polarity** — not
   only for `NOT IN`. In the conjunct form a positive nullable `IN` is two-valued-safe
   (a top-level `AND` drops SQL's `UNKNOWN` and `FALSE` alike), but a mark can be read
   under `NOT` or projected as a value, where `mark = 0` for a NULL key diverges from
   `UNKNOWN` (§5).

Still rejected: scalar subqueries, ANY/ALL, two-or-more subqueries in one view,
subqueries in HAVING/DML/direct SELECT, string CASE branches.

## Design

### 1. Wire + builder: `EXPR_SELECT` and `EXPR_LOAD_NULL`

Instructions are fixed 4-word quads `[op, dst, a1, a2]` (`ExprBuilder::emit`,
`gnitz-core/src/expr.rs:187`; decode `chunks_exact(4)`,
`gnitz-engine/src/expr/program.rs:468-472`). A select needs three register sources; the
register file is capped at 64 (`program.rs:391-394`), so two indices pack into one word.
This is the first two-register packing (LOAD_CONST packs a 64-bit *value* across two
words, `program.rs:481-484`; no precedent packs registers) — documented at both encode
and decode sites. Decode caution: `from_wire` pre-binds loop-level `a = q[2]`,
`b = q[3]` (`program.rs:470-471`); the SELECT arm must bind its own
`cond = q[2] as u16`, `a = (q[3] & 0xFFFF) as u16`, `b = (q[3] >> 16) as u16` under
fresh names — reflexively reusing the loop-level `a`/`b` silently swaps operands. Both
constants also go into the `cast_consts!` import block (`program.rs:15-27`).

- `EXPR_SELECT = 35`: `[EXPR_SELECT, dst, cond, a | (b << 16)]`.
  Semantics (SQL CASE): rows where `cond` is non-NULL and truthy take `a`'s value and
  null bit; all other rows (false **or NULL** cond) take `b`'s. The result carries a
  *value*, never a boolean classification.
- `EXPR_LOAD_NULL = 36`: `[EXPR_LOAD_NULL, dst, 0, 0]` — value 0, null bit set for all
  rows. Needed for `CASE` without `ELSE` (→ `ELSE NULL`) and `NULLIF`.

`ExprBuilder` gains `select(cond, a, b) -> u32` and `load_null() -> u32` (fresh
`alloc_reg` dst each, preserving single-assignment). Blob format is untouched — the
container (`EXPR_BLOB_VERSION = 1`) is opcode-agnostic and encode/decode ship in the
same build (`program.rs:557` panics on unknown opcodes; no version bump).

### 2. Engine decode / validate / classify (`expr/program.rs`)

- `LogicalInstr::Select { dst, cond, a, b }` and `LoadNull { dst }`; `from_wire` arms
  (`cond = q[2] as u16`, `a = (q[3] & 0xFFFF) as u16`, `b = (q[3] >> 16) as u16`).
- `LogicalProgram::new`: the Select arm asserts `dst != cond && dst != a && dst != b`
  plus bounds — extending the binary-op anti-aliasing rule (`program.rs:406-431`) that
  makes the raw split borrows sound. (This is the expr-VM SSA rule; it is unrelated to
  the operator-DAG "destructive register ordering" invariant in
  `query/compiler/emit.rs:909-961`, which concerns batch registers — note this in the
  arm comment to keep the two register concepts separate.)
- `resolve`: U64 signedness propagates through Select exactly as through `IntAdd` —
  `reg_tc[dst] = propagate_u64(&reg_tc, a, b)` (U64 if **either** branch is U64) — so a
  downstream ordered compare / div / `int_to_float` on a U64-carrying CASE result picks
  the unsigned variant and values ≥ 2^63 order correctly. Float unification is the
  lowering's job (§4) and is orthogonal to this.
- `each_reg_read` (`program.rs:807-839`): Select reads `cond, a, b`; LoadNull reads none.
- `classify_registers` (`program.rs:930-1002`): `cond` → `bool_input` (its producer must
  pack `bool_bits`), `a`/`b` → `non_bool_read`, `dst` → neither set (a value register;
  keeping it out of `bool_produced` keeps it out of `bit_only`). `LoadNull.dst` →
  neither.
- `is_strictly_non_nullable` (`program.rs:1029-1076`): `LoadNull` → `return false` (it
  manufactures NULL); `Select` → the exhaustive provably-non-null remainder arm (it only
  copies branch values whose own nullability is already accounted for by the branches'
  producers). The wildcard-free match forces both arms at compile time.

### 3. Engine eval (`expr/batch.rs`)

New helpers `reg4` / `null_words4` mirroring `reg3`/`null_words3` (`batch.rs:77-105`)
with three shared sources + one mutable dst (soundness from the §2 anti-alias assert).
Two arms:

- **`no_nulls` fast arm** (never contains LoadNull — `is_strictly_non_nullable` forces
  the nullable path whenever LoadNull appears; cond nullability likewise): per row
  `dst[i] = if cond[i] != 0 { a[i] } else { b[i] }`.
- **Nullable arm**, word-level for the masks, row-level blend for values:
  `take_a = bool_bits[cond] & !null_bits[cond]` per word;
  `null_bits[dst] = (take_a & null_bits[a]) | (!take_a & null_bits[b])`;
  values per row from the corresponding source. Tail: `maybe_pack_bool_bits(dst)`
  (`batch.rs:268-276`) so a Select feeding a downstream boolean consumer repacks
  correctly.
- `LoadNull`: zero the value lane, set `null_bits[dst]` for the live rows.

EMIT already propagates a computed register's null bit into the output null bitmap
(`expr/plan.rs:511-532`), so nullable CASE results in computed projections work without
further changes; `run_filter` already drops NULL results (`plan.rs:370-443`), giving
correct 3VL for CASE in WHERE.

### 4. SQL surface: `BoundExpr::Case` (`ir.rs`, `bind/structural.rs`, `lower.rs`)

- `BoundExpr::Case { branches: Vec<(BoundExpr, BoundExpr)>, else_: Option<Box<BoundExpr>> }`
  and `BoundExpr::LitNull` (unit variant; `infer_type` → `I64`, the neutral element of
  the `unify_numeric` rule below, so a NULL branch never drags a U64/float sibling back
  down). `Case::infer_type` folds every result branch and the else through
  `unify_numeric`, seeded from the else (or `I64` when the else is implicit).
- **Type inference must preserve U64** (folded-in correctness fix — the Case arm forces
  the question and the existing `BinOp` arithmetic arm shares the same bug). Today
  `infer_type`'s arithmetic `BinOp` arm returns `F64` for any float operand else **`I64`
  unconditionally** (`ir.rs:40-46`), so `SELECT u64_a + u64_b AS v` materializes a view
  column typed `I64` (`codec/project_schema.rs`: a computed column's type is
  `bound.infer_type(source_schema)`). A downstream `WHERE v > 100` then seeds `reg_tc`
  from that `I64` column type (`expr/program.rs` `LoadColInt`) and picks a **signed**
  compare (`Cmp` is signed unless a register is U64), so a stored value ≥ 2^63 orders as
  negative — silently wrong. This is a live pre-alpha bug independent of CASE; the new
  Case arm would inherit it. Fix both through one helper mirroring the engine's own
  runtime register rule (`propagate_u64` — *U64 dominates for integer arithmetic*):

  ```rust
  /// Common numeric type for arithmetic and conditional blends, matching the
  /// engine's runtime register rule (`propagate_u64`): any float operand → F64;
  /// else any U64 operand → U64 (so the materialized column re-seeds a downstream
  /// unsigned compare); else I64. All three are 8-byte slots — a pure type-label
  /// decision (the integer arithmetic itself is bit-identical either way).
  fn unify_numeric(a: TypeCode, b: TypeCode) -> TypeCode {
      if matches!(a, TypeCode::F32 | TypeCode::F64) || matches!(b, TypeCode::F32 | TypeCode::F64) {
          TypeCode::F64
      } else if matches!(a, TypeCode::U64) || matches!(b, TypeCode::U64) {
          TypeCode::U64
      } else {
          TypeCode::I64
      }
  }
  ```

  The `BinOp` arithmetic arm becomes `_ => unify_numeric(lt, rt)`; `Case::infer_type`
  folds its branch/else types through the same helper (seed `I64`, so an all-`LitNull`
  CASE stays `I64`). Only U64 is affected — narrow unsigned (U8/U16/U32) values stay
  < 2^63 and order correctly under a signed compare, so their result type is unchanged.
  Scope is single-table computed projections: join and set-op projections reject computed
  expressions (`plan/view/join.rs`), so `a.x + b.y` cannot even be written there.
- **`SUM(u64)` has the same demotion in a sibling path**, folded in for consistency.
  `agg_result_type` (`plan/view/group_by.rs:94-97`) widens every non-float SUM to `I64`.
  The engine's SUM accumulator is an i64 `wrapping_add` of `value × weight`
  (`ops/reduce/agg.rs`, `merge_accumulated`/`fold` Sum arm), i.e. the true sum mod 2^64 —
  so for a U64 source its bit pattern already *is* the correct unsigned value, and the
  output column must be typed **U64**, exactly as MIN/MAX preserve their source type
  (`group_by.rs:100`, with the same 8-byte-width guarantee). Change the SUM arm to return
  `U64` for a U64 source; a narrow unsigned source still widens to `I64` (its sum stays
  < 2^63, so signed order is correct) and the comment's "widen to I64" rationale is
  updated (i64/u64 are the same width — widening never prevented the wrap; the label is
  the only choice, and U64 is the correct one for a U64 source).
- `bind_structural` arms (one site, lands on all three `LeafBinder` contexts):
  - `Expr::Case { operand, conditions, else_result }`: searched form binds each
    `CaseWhen { condition, result }` pair; simple-operand form desugars each WHEN `v` to
    `operand = v` (operand bound once per branch, as the BETWEEN desugar re-binds).
    Missing ELSE → `LitNull`. The generic AST walker `expr_operands` (`ast_util.rs`) —
    the documented "≡ what `bind_structural` recurses through" mirror, driving
    `collect_column_refs` / `expr_has_aggregate` and §5's `rewrite_subqueries` — has **no
    `Case` arm today** (CASE is unsupported), so it gains one here in lockstep: a subquery
    or column reference inside a `CASE` branch is otherwise invisible to those passes.
  - `Expr::Function` named `coalesce`/`nullif` intercepted in the `Expr::Function` arm
    **before** `leaf.bind_function` (structural desugar, so the three leaf impls stay
    untouched). The interceptor lowercases `f.name`, extracts the plain positional arg
    exprs into a `&[&Expr]` (rejecting `*`, named, and DISTINCT/qualifier forms with a
    clean error), and dispatches; every other name falls through to `leaf.bind_function`.
    `COALESCE` desugars right-to-left with **explicit base cases**, and handles literal
    operands at the AST level — `bind_null_test` resolves a *column*
    (`single_relation_col_name` returns `None` for a literal, so the leaf errors "expected
    a column reference"), so a literal must never reach it:

    ```rust
    // COALESCE(args…) — total over arity; literal operands never reach bind_null_test.
    fn bind_coalesce<L: LeafBinder>(args: &[&Expr], leaf: &L) -> Result<BoundExpr, GnitzSqlError> {
        match args {
            [] => Ok(BoundExpr::LitNull),                   // COALESCE() ≡ NULL (parser forbids; stay total)
            [a] => bind_coalesce_last(a, leaf),             // last operand: its value is the result
            [a, rest @ ..] => {
                if let Expr::Value(v) = a {
                    // A NULL literal contributes nothing; any other literal is non-null → the result.
                    return match &v.value {
                        Value::Null => bind_coalesce(rest, leaf),
                        _ => bind_literal(&v.value),
                    };
                }
                let cond = leaf.bind_null_test(a, /* want_null = */ false)?; // IsNotNull(col) | LitInt(1)
                if matches!(cond, BoundExpr::LitInt(1)) {
                    return bind_structural(a, leaf); // provably non-null (NOT NULL column) → always taken
                }
                Ok(BoundExpr::Case {
                    branches: vec![(cond, bind_structural(a, leaf)?)],
                    else_: Some(Box::new(bind_coalesce(rest, leaf)?)),
                })
            }
        }
    }
    // Trailing operand: a bare NULL literal is LitNull; any other literal/column binds normally.
    fn bind_coalesce_last<L: LeafBinder>(a: &Expr, leaf: &L) -> Result<BoundExpr, GnitzSqlError> {
        if let Expr::Value(v) = a {
            return match &v.value {
                Value::Null => Ok(BoundExpr::LitNull),
                _ => bind_literal(&v.value),
            };
        }
        bind_structural(a, leaf)
    }
    ```

    So `COALESCE(col, 0)` binds (nullable `col` → `Case{[(IsNotNull(col), col)], else: 0}`;
    a NOT NULL `col` → the operand directly via the `LitInt(1)` truncation),
    `COALESCE(NULL, col)` folds to `col`, `COALESCE(a)` to `a`, and `COALESCE(a + 1, 0)`
    still rejects with the leaf's clean "expected a column reference" error (pinned by a
    test) rather than mis-binding. The single-arg and empty base cases make the fold
    total — the plain `else: coalesce(rest)` form alone bottoms out at `coalesce()` and
    breaks even `COALESCE(a, b)`.
    `NULLIF(a, b)` (exactly two args, else a clean error) → `Case { [(a = b, LitNull)],
    else: a }` with both `a` and `b` bound through `bind_structural` (not
    `bind_null_test`), so `NULLIF` has no literal-operand pitfall. `NULL` is
    `BoundExpr::LitNull` (the new unit variant, lowering to `load_null`).
- `lower.rs`: `BoundExprBackend` gains `case(...)`. **Float unification is a single
  global decision, not a per-pair fold**: `EXPR_SELECT` blends raw i64 bit patterns and
  converts nothing, so the `OpcodeBackend` impl first computes the case's unified type
  (`Case::infer_type` via `unify_numeric`; F64 iff *any* result branch or the else is
  float — U64-vs-I64 needs no lift, only the sign label of §4's type rule), lowers
  **every** result and the else, `int_to_float`-lifts each non-float one up front, and only then
  folds right-to-left (`acc = else`; per pair `acc = select(cond_reg, result_reg,
  acc)`). A per-pair unification in the style of `binop` (`lower.rs:212-220`) would
  blend int and float bit patterns in one register — spelled out here because that is
  the natural wrong implementation. String-typed results rejected. The client-side DML
  row evaluator (`exec/eval.rs`) gains the matching `BoundExpr::Case`/`LitNull` arms
  (three-valued: cond NULL → else branch), which also makes `UPDATE … SET x = CASE …`
  work via `eval_set_expr` → `eval_expr` — in scope and tested.
- HAVING lowering (`group_by.rs`) and residual binding flow through the shared walk.
  Compile-error coverage is only partial: `lower_bound_expr` (`lower.rs:33-48`) and
  `infer_type` are wildcard-free and break until the new arms exist, but
  `set_op.rs:110,126`, `group_by.rs:271`, and `mutate.rs::eval_set_expr` carry `_` arms
  that silently accept new variants — each gets an explicit test (the first two pinning
  their rejection messages, the last pinning the now-working SET = CASE).

### 5. Subqueries in arbitrary boolean positions (`plan/view/dispatch.rs` + `plan/view/exists.rs`)

`plan/view/exists.rs` already ships the semi/anti construction — `emit_equi_exists` /
`emit_range_exists`, `semi_or_anti`, and the `ν = positive_diff(a_all, π_A(inner))`
null-fill (the outer-join analogue is `plan/view/join.rs`). Today it serves exactly one
shape: a single top-level `[NOT] EXISTS`/`[NOT] IN` WHERE conjunct, classified
`ViewShape::Subquery` in `dispatch.rs` and emitted as a direct semi/anti **filter** view.
This section adds a subquery in *any* boolean position of a Simple-shaped view — under
`OR`/`NOT`, inside `CASE` (§4), or projected as a column — by computing its truth value
as a `0/1` **mark** column and letting ordinary expression evaluation consume it. The
correlation core (`ExistsCtx`: `build_alias_map` / `count_side_refs` /
`extract_join_predicates`, and the reindex/semi/anti circuit) is already
`(tid, schema)`-relative — nothing in it assumes the outer is a base table; only
`emit_exists_pieces`'s top-of-function `extract_table_name_and_alias(&select.from[0])`
(`exists.rs:88`) and its projection tail are FROM-coupled.

**Scope: exactly one subquery per view.** Two or more subqueries in one view are rejected
with "at most one EXISTS/IN subquery per view; compose via stacked views" — *no
regression* (multi-subquery is rejected today), and the headline cases (`WHERE x=1 OR
EXISTS(…)`, `WHERE NOT(EXISTS(…) AND y>2)`, `SELECT a.*, EXISTS(…) AS f`,
`CASE WHEN EXISTS(…) …`) are all single-subquery. One subquery needs one mark join,
which is one circuit — no `ViewChain`, no hidden segments, no cross-level keying.

**Dispatch (`ViewShape::MarkSubquery { select, subq }`).** After the single-conjunct
`ViewShape::Subquery` check fails to peel a clean top-level subquery conjunct (its
`flatten_conjuncts` / `as_subquery_conjunct` pair only sees top-level AND conjuncts, so
it misses subqueries under OR/NOT and in the SELECT list), scan the WHERE **and** every
projection item for `Expr::Exists` / `Expr::InSubquery`: exactly one → `MarkSubquery`;
two or more → the error above; zero → fall through unchanged. The single-top-level-conjunct
fast path stays first-checked and unchanged — it needs no mark column or filter step and
is the dominant shape (a specialized path for the common case, like the equi/range split;
not legacy). `MarkSubquery` and `Subquery` share the `ExistsCtx` correlation/circuit core
through two thin entry points.

**Rewrite.** `rewrite_subqueries(&mut Expr, …)` — a hand-written *mutable* mirror of
`expr_operands`' arms (`ast_util.rs`), **including the `Expr::Case` arm §4 adds** (so a
subquery inside a `CASE WHEN` is reachable). No generic mutable walker exists; every
`&Expr`-returning walker deliberately treats subqueries as opaque leaves. It replaces the
one subquery node in place with `Expr::Identifier(_mark)`, passing the node (with its own
`negated` flag) to `emit_mark_pieces`. The node's own `NOT EXISTS`/`NOT IN` negation is
baked into `_mark` (below); any *external* `NOT`/`OR`/`CASE` around the node stays in the
AST and evaluates normally over `_mark` — so a single uniform substitution serves every
position. The engine already reads a nonzero
i64 register as boolean-true (`pack_to_bool_bits`'s `!= 0`; `run_filter`'s slow path scans
`regs[i] != 0`), so a bare `_mark` in a WHERE/OR/NOT/CASE-condition position needs no new
int-to-bool machinery. `Expr::Subquery`/`AnyOp`/`AllOp` are left untouched → still rejected
by `bind_structural`; nested subqueries inside a subquery's own body are already rejected
(`count_side_refs` + `bind_single_table`), so the walk never descends into one.

**Circuit (`emit_mark_pieces`, one circuit).** Generalize `emit_exists_pieces` to take the
outer as a passed `(outer_tid, outer_schema, outer_alias)` triple (built from
`select.from[0]` by the dispatcher), reusing the shared `ExistsCtx` core verbatim. Then:

- Emit both semi/anti branches (both already exist inside `semi_or_anti`):
  `matched = union(negate(ν), a_all)` (weight `w_A·[S>0]`) and
  `unmatched = ν = positive_diff(a_all, π_A(inner))` (weight `w_A·[S=0]`), keyed by the
  correlation PK (equi `_join_pk`, band/range `_src_pk`).
- Tag and union with the **polarity baked in**:
  `out = union(with_mark(matched, hit), with_mark(unmatched, miss))` where
  `(hit, miss) = if negated { (0, 1) } else { (1, 0) }`, so `_mark` is `1` exactly when the
  (possibly-negated) subquery predicate is true. `with_mark(rows, c)` is a `map_expr` that
  `copy_col`s every payload column and appends a constant `_mark` i64 column — the
  computed-projection pattern (`copy_col` + `load_const` + `emit_col`, as in `simple.rs`;
  the output width comes from the node's declared out-schema, so appending past the input
  width is standard). Each outer row lands in exactly one branch at weight `w_A`, so `out`
  is the outer relation with a deterministic `_mark` per row — weight-exact, `_mark` part of
  the payload identity so the branches never merge.
- Filter: bind the rewritten WHERE against the post-union schema
  (`[synthetic_pk…, outer cols…, _mark]`, so the compiled column indices are physical) and
  `cb.filter` over the post-union node — skipped when the subquery was projection-only with
  no other WHERE conjunct. `WHERE y>5 AND EXISTS(…)` becomes `y>5 AND _mark`; a NULL-key
  row's `_mark` follows the semi/anti rules unchanged.
- Project: reuse `build_join_view_projection` over the outer alias map — so `*` and `t.col`
  resolve to the **outer** columns exactly as the exists path does (not the synthetic PK or
  `_mark`) — extended to resolve the reserved `_mark` identifier to the appended mark column,
  so a projected `EXISTS(…) AS f` (rewritten to `_mark AS f`) emits the `0/1` value. Synthetic
  PK = the correlation PK region; sink.

`with_mark` adds one i64, so the widest-intermediate `reject_column_overflow` guard already
covers it (+1). Column names are not reserved from a leading `_` and
`reject_duplicate_column_names` is skipped on a wildcard projection, so reject up front if
the outer schema already carries a `_mark` column.

**Nullable-IN rejection (mark position).** EXISTS is always two-valued, so a NULL
correlation key just makes the outer row unmatched (`ν = w_A` → mark 0), correct in
*every* boolean position including under NOT. For `IN`, a NULL outer operand or NULL
subquery value makes SQL's membership `UNKNOWN`, but the mark encodes it as `0` (a NULL
key is filtered out of the match on both sides, so the row lands in `ν` at full
multiplicity). In a top-level `AND` conjunct `UNKNOWN` and `FALSE` both drop the row, so
the conjunct path (`emit_exists_pieces`) allows a *positive* nullable IN and gates its
rejection on `negated` (`exists.rs:255`). A mark, however, can be read under `NOT` or
projected as a value, where `mark = 0` (≈ `FALSE`) diverges from `UNKNOWN` — e.g.
`NOT (x IN …)` with a NULL `x` gives `NOT(_mark)` → true, but SQL yields `UNKNOWN` → the
row is excluded. So `emit_mark_pieces` rejects a nullable IN **unconditionally**,
independent of `negated` — strictly stronger than the conjunct check, which stays as-is
(dropping its `negated &&` gate would wrongly reject the still-valid positive-conjunct
form):

```rust
// Mark position: an IN mark can be negated or projected, so a NULL key's mark=0
// (≈ FALSE) would leak where SQL means UNKNOWN. Reject any nullable operand,
// regardless of polarity — the conjunct path's `negated &&` gate is unsound here.
if outer_schema.columns[outer_col].is_nullable || inner_schema.columns[inner_col].is_nullable {
    return Err(GnitzSqlError::Unsupported(
        "IN (SELECT …) in a mark position (under OR/NOT, in CASE, or projected) requires \
         the outer operand and the subquery column to be NOT NULL — SQL's 3VL diverges \
         from the two-valued mark; use a top-level AND `x IN (SELECT …)` conjunct, or \
         NOT EXISTS with an explicit equality"
            .into(),
    ));
}
```

This over-rejects the rare positive nullable IN that sits in a pure-`AND` mark position
(sound there, but flagged) — a deliberate conservative choice matching the codebase's
reject-rather-than-drift stance. EXISTS marks are never nullable and are unaffected.

## Sequencing

1. **Opcodes end-to-end** (`gnitz-wire`, `gnitz-core::ExprBuilder`, engine
   decode/validate/classify/eval incl. `reg4`): unit tests per arm — no_nulls and
   nullable eval, null-bit blending, `bit_only` classification with a Select feeding
   BOOL_AND, LoadNull forcing the nullable path, anti-alias assertions.
2. **SQL CASE/COALESCE/NULLIF + U64 type inference** (`ir.rs`, `bind/structural.rs`,
   `lower.rs`, `exec/eval.rs`, `plan/view/group_by.rs`): `unify_numeric` (U64-preserving
   `BinOp` + `Case`) and the `SUM(u64)` arm land here; binder/lowering units, e2e in
   projections, WHERE, HAVING, DML residuals; float/int unification; string rejection;
   U64 downstream-compare e2e.
3. **Mark subquery** (`rewrite_subqueries` in `ast_util.rs`, `ViewShape::MarkSubquery` in
   `dispatch.rs`, `emit_mark_pieces` in `exists.rs`): circuit-shape units + e2e for EXISTS
   under OR/NOT, projected EXISTS, CASE-over-EXISTS, all three correlation paths;
   nullable-IN mark-position rejection (regardless of polarity) with the positive
   top-level conjunct still accepted; two-subqueries-in-one-view rejection.

Each step compiles and passes `make verify` + `make e2e` independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-wire/src/expr.rs` | `EXPR_SELECT = 35`, `EXPR_LOAD_NULL = 36` |
| `crates/gnitz-core/src/expr.rs` | `ExprBuilder::select`, `::load_null` |
| `crates/gnitz-engine/src/expr/program.rs` | Logical/resolved variants, decode, validation, resolve, `each_reg_read`, `classify_registers`, `is_strictly_non_nullable` |
| `crates/gnitz-engine/src/expr/batch.rs` | `reg4`/`null_words4`, Select + LoadNull eval arms |
| `crates/gnitz-sql/src/ir.rs` | `BoundExpr::Case`, `BoundExpr::LitNull`, `Case::infer_type`; `unify_numeric` helper + U64-preserving `BinOp` arithmetic arm |
| `crates/gnitz-sql/src/bind/structural.rs` | `Expr::Case` arm; `bind_coalesce`/`bind_coalesce_last` desugar (base cases + literal handling); NULLIF desugar |
| `crates/gnitz-sql/src/plan/view/group_by.rs` | `agg_result_type`: preserve U64 for `SUM` over a U64 source (mirrors MIN/MAX) |
| `crates/gnitz-sql/src/lower.rs` | backend `case` method + opcode lowering |
| `crates/gnitz-sql/src/exec/eval.rs` | client-side Case/LitNull evaluation |
| `crates/gnitz-sql/src/ast_util.rs` | `rewrite_subqueries` (mutable walker); `expr_operands` gains the `Case` arm |
| `crates/gnitz-sql/src/plan/view/dispatch.rs` | `ViewShape::MarkSubquery` classify + single-subquery scan; drives the rewrite |
| `crates/gnitz-sql/src/plan/view/exists.rs` | outer relation factored to a passed `(tid, schema, alias)`; `emit_mark_pieces` (both branches tagged, rewritten-WHERE filter, `_mark`-aware projection) over the shared `ExistsCtx` core |
| `crates/gnitz-py/tests/test_case_expr.py` | new e2e suite (CASE/COALESCE/NULLIF) |
| `crates/gnitz-py/tests/test_exists.py` | mark-join e2e cases |

## Testing

- **VM units**: select truth table incl. NULL cond → else; null-bit blend across word
  boundaries (row counts 1, 63, 64, 65, 256); LoadNull; select-of-select nesting;
  select feeding BOOL_AND / IS_NULL; filter over a NULL-producing CASE; register-cap
  and aliasing assertions.
- **Binder/lowering units**: searched + simple CASE, missing ELSE → NULL; COALESCE base
  cases (`COALESCE(a)` → `a`, `COALESCE(NULL, col)` → `col`, `COALESCE(col, 0)` shape,
  nested 3-arg), COALESCE computed-operand rejection (`COALESCE(a + 1, 0)`); NULLIF;
  float unification; string rejection; `unify_numeric` (`u64 + u64` and
  `CASE`-between-U64 infer `U64`, `u64 + f64` infers `F64`, `u32 + u32` stays `I64`);
  `SUM(u64)` infers `U64` while `SUM(u32)` stays `I64`.
- **E2E** (W=4, weight-asserted): CASE in computed projections incl. NULL branch
  round-trip to the client; CASE/COALESCE in WHERE and HAVING; `EXISTS(…) OR x = 1`
  flipping membership incrementally through both disjuncts; projected
  `EXISTS(…) AS flag` transitioning 0↔1 under inserts/retractions (assert the
  old-mark-row retraction and new-mark-row emission weights); `NOT IN` under OR with
  non-nullable columns; **nullable-IN in a mark position rejected regardless of polarity**
  (`WHERE NOT (x IN (SELECT …))`, `SELECT x IN (SELECT …) AS f`, `… OR x IN (SELECT …)`
  with a nullable `x` or nullable subquery column — while the positive top-level
  `AND x IN (SELECT …)` conjunct keeps working); a mark over each of the equi, band, and
  pure-range correlation paths; two subqueries in one view rejected with the compose-via-
  views message.
- **U64 downstream (folded fix)**: a view `SELECT u64_a + u64_b AS v` (and
  `CASE … THEN u64 … END`) with a value ≥ 2^63, read by a downstream view
  `WHERE v > 100`, returns the row (fails today: signed compare drops it); the same for
  `SELECT SUM(u64_col) AS s … GROUP BY g` with a group sum ≥ 2^63 filtered by `WHERE
  s > 100`. Regression-guard existing narrow-unsigned projection/compare tests to confirm
  U8/U16/U32 are unchanged.
- **Perf**: engine micro-bench (`*_bench`, `--release`) of a select-heavy filter vs the
  equivalent AND/OR chain to confirm the nullable arm's word-level path; `make bench`
  unchanged for circuits without the new opcodes.

## Invariants preserved

- Expression VM SSA + 64-register cap + wildcard-free classifier matches (new variants
  classified, not wildcarded).
- Blob container format and version unchanged; opcode space extended in the 35-39 gap.
- Mark circuits reuse the semi/anti partition-locality and weight-exactness arguments
  unchanged; the appended mark column is non-nullable so `is_strictly_non_nullable`
  fast paths are unaffected for mark-free programs.
- All existing programs decode and evaluate byte-identically (new opcodes only).
