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

Under the standing restrictions (NOT IN requires non-nullable columns on both sides;
IN's NULL cases are absorbed by NULL-key join filtering), every supported subquery
predicate is **two-valued** — EXISTS is always two-valued in SQL. So the mark column is
a non-nullable `0/1` i64 column and no three-valued mark machinery is needed: the "IN
mark can be NULL" case is exactly the nullable-NOT-IN surface this codebase deliberately
rejects.

## Prerequisite state

- Semi/anti circuits for equi/band/pure-range correlation exist
  (`plan/view/exists.rs`) with `π_A(inner)` / `ν = positive_diff(a_all, π_A(inner))`
  construction, and the planner lowers views through the bound IR + splitter
  (`plan/lp.rs`: `Rel` nodes, one-anchor segments, hidden-view chains via
  `create_view_chain`). Verify against the live tree before starting.

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
2. **Subquery predicates in arbitrary boolean positions** of a view WHERE and as
   projected boolean columns: `WHERE x = 1 OR EXISTS(…)`,
   `WHERE NOT (EXISTS(…) AND y > 2)`, `SELECT a.*, EXISTS(…) AS has_child`,
   `CASE WHEN EXISTS(…) THEN … END`. Per-subquery restrictions are unchanged from the
   conjunct form (correlation vocabulary, non-nullable NOT IN, no same-source, single
   inner FROM).

Still rejected: scalar subqueries, ANY/ALL, subqueries in HAVING/DML/direct SELECT,
string CASE branches.

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

- `BoundExpr::Case { branches: Vec<(BoundExpr, BoundExpr)>, else_: Option<Box<BoundExpr>> }`.
  `infer_type`: `F64` if any result branch (or the else) infers `F32|F64`, else `I64`
  (mirroring the `BinOp` rule, `ir.rs:33-47`).
- `bind_structural` arms (one site, lands on all three `LeafBinder` contexts):
  - `Expr::Case { operand, conditions, else_result }`: searched form binds each
    `CaseWhen { condition, result }` pair; simple-operand form desugars each WHEN `v` to
    `operand = v` (operand bound once per branch, as the BETWEEN desugar re-binds).
    Missing ELSE → `LitNull`.
  - `Expr::Function` named `coalesce`/`nullif` intercepted **before** `leaf.bind_function`
    (structural desugar, so the three leaf impls stay untouched):
    `COALESCE(a, rest…)` → `Case { [(null_test(a, false), bind(a))], else: coalesce(rest) }`
    where the null test goes through `leaf.bind_null_test(a, false)` — so operands are
    whatever the context's null test accepts (columns everywhere; aggregate results in
    HAVING via its aggregate-aware null-test leaf; a literal operand is non-null and
    truncates the fold). Operands the leaf's null test rejects (general computed
    expressions) get that leaf's clean error, pinned by a test asserting
    `COALESCE(a + 1, 0)` rejects rather than mis-binds.
    `NULLIF(a, b)` → `Case { [(a = b, NULL)], else: a }` with `NULL` represented as
    `BoundExpr::LitNull` (new unit variant, lowering to `load_null`).
- `lower.rs`: `BoundExprBackend` gains `case(...)`. **Float unification is a single
  global decision, not a per-pair fold**: `EXPR_SELECT` blends raw i64 bit patterns and
  converts nothing, so the `OpcodeBackend` impl first computes the case's unified type
  (`infer_type`: F64 if *any* result branch or the else is float), lowers **every**
  result and the else, `int_to_float`-lifts each non-float one up front, and only then
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

### 5. Mark anchors (`plan/lp.rs` + `plan/view/exists.rs`)

This section builds on the prerequisite-state deliverables (`plan/lp.rs`,
`plan/view/exists.rs` and its ν construction) — those symbols do not exist in today's
tree; the landed analogue of every circuit fragment referenced below is the outer-join
null-fill in `plan/view/join.rs` (`positive_diff` uses at `join.rs:362,1039,1060`).
Re-ground the file/symbol references against the live tree at implementation time.

IR: `Rel::Mark { outer, inner, kind: Exists|In, negated: bool, eq, range,
mark_name: String }` — output schema = outer's columns + one appended non-nullable
`i64` column.

**Lowering.** During WHERE/projection lowering, every `Expr::Exists`/`Expr::InSubquery`
found *anywhere* in an expression (not just top-level conjuncts) is replaced by a
`ColRef` to a fresh mark column, and a `Mark` anchor is stacked onto the FROM tree
(one per distinct subquery expression). Top-level single-conjunct subqueries keep
lowering to `SemiAnti` (cheaper: no mark column, no second branch). `NOT
EXISTS(…)`/`NOT IN` in expression position lower as `mark = 0` — negation lives in the
expression, so one Mark anchor kind serves both polarities. The rewritten WHERE then
binds as an ordinary `Filter` above the Mark anchors; a projected
`EXISTS(…) AS name` becomes a `Project` pass-through of the mark column under `name`.
A final projection drops unprojected mark columns.

**Circuit (`emit_mark`, one per correlation path).** Reuses the semi/anti construction;
both branches are emitted and tagged:

- equi: `matched = union(negate(ν), a_all)` (the semi node) and `unmatched = ν`, both
  keyed `[_join_pk × k, A]`. Append the mark via `map_expr` with a program that
  `copy_col`s every payload column and emits `load_const(1)` / `load_const(0)` into the
  appended slot (the computed-projection pattern, `simple.rs:86-106`):
  `out = union(with_mark(matched, 1), with_mark(unmatched, 0))`, keyed
  `[_join_pk × k, A, mark]`. Identities of the two branches are disjoint per row
  (a row is matched xor unmatched at net weight), so consolidation cannot merge across
  branches; total multiset = A with a mark column, weight-exact by the x1 arguments.
- band / pure-range: same two branches over the source-PK-keyed forms
  (`[a.pk…, A, mark]`), riding the range circuit's output exchange.

Mark anchors are anchors for the splitter (one per segment). `k`-column-count guard
includes the mark column.

**NULL-key note.** A NULL-correlation-key outer row is unmatched by construction
(`ν = w_A`), so its mark is 0 — matching EXISTS/IN semantics in *every* boolean
position, including under NOT (`NOT EXISTS` → `mark = 0` → true; `x IN` with NULL x
→ mark 0 → `NOT(mark=1)`… evaluates true — which is why nullable-x `NOT IN`/negated-IN
stays **rejected**: the lowering checks the IN operand's nullability regardless of
polarity or position, as x1 does for the conjunct form).

## Sequencing

1. **Opcodes end-to-end** (`gnitz-wire`, `gnitz-core::ExprBuilder`, engine
   decode/validate/classify/eval incl. `reg4`): unit tests per arm — no_nulls and
   nullable eval, null-bit blending, `bit_only` classification with a Select feeding
   BOOL_AND, LoadNull forcing the nullable path, anti-alias assertions.
2. **SQL CASE/COALESCE/NULLIF** (`ir.rs`, `bind/structural.rs`, `lower.rs`,
   `exec/eval.rs`): binder/lowering units, e2e in projections, WHERE, HAVING, DML
   residuals; float/int unification; string rejection.
3. **Mark anchors** (`plan/lp.rs`, `emit_mark` in `exists.rs`): circuit-shape units +
   e2e for EXISTS under OR/NOT, projected EXISTS, CASE-over-EXISTS, two marks in one
   WHERE, all three correlation paths.

Each step compiles and passes `make verify` + `make e2e` independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-wire/src/expr.rs` | `EXPR_SELECT = 35`, `EXPR_LOAD_NULL = 36` |
| `crates/gnitz-core/src/expr.rs` | `ExprBuilder::select`, `::load_null` |
| `crates/gnitz-engine/src/expr/program.rs` | Logical/resolved variants, decode, validation, resolve, `each_reg_read`, `classify_registers`, `is_strictly_non_nullable` |
| `crates/gnitz-engine/src/expr/batch.rs` | `reg4`/`null_words4`, Select + LoadNull eval arms |
| `crates/gnitz-sql/src/ir.rs` | `BoundExpr::Case`, `BoundExpr::LitNull`, `infer_type` arms |
| `crates/gnitz-sql/src/bind/structural.rs` | `Expr::Case` arm; COALESCE/NULLIF structural desugar |
| `crates/gnitz-sql/src/lower.rs` | backend `case` method + opcode lowering |
| `crates/gnitz-sql/src/exec/eval.rs` | client-side Case/LitNull evaluation |
| `crates/gnitz-sql/src/plan/lp.rs` | `Rel::Mark`, expression-position subquery rewrite |
| `crates/gnitz-sql/src/plan/view/exists.rs` | `emit_mark` (three correlation paths) |
| `crates/gnitz-py/tests/test_case_expr.py` | new e2e suite (CASE/COALESCE/NULLIF) |
| `crates/gnitz-py/tests/test_exists.py` | mark-join e2e cases |

## Testing

- **VM units**: select truth table incl. NULL cond → else; null-bit blend across word
  boundaries (row counts 1, 63, 64, 65, 256); LoadNull; select-of-select nesting;
  select feeding BOOL_AND / IS_NULL; filter over a NULL-producing CASE; register-cap
  and aliasing assertions.
- **Binder/lowering units**: searched + simple CASE, missing ELSE → NULL, COALESCE fold
  and its computed-operand rejection, NULLIF, float unification, string rejection,
  `infer_type`.
- **E2E** (W=4, weight-asserted): CASE in computed projections incl. NULL branch
  round-trip to the client; CASE/COALESCE in WHERE and HAVING; `EXISTS(…) OR x = 1`
  flipping membership incrementally through both disjuncts; projected
  `EXISTS(…) AS flag` transitioning 0↔1 under inserts/retractions (assert the
  old-mark-row retraction and new-mark-row emission weights); `NOT IN` under OR with
  non-nullable columns; nullable-operand rejection regardless of position; marks over
  band and pure-range correlation; two subqueries in one WHERE.
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
