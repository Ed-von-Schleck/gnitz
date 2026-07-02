# x4: correlated scalar aggregate subqueries + ANY/ALL quantifiers

## Problem

With semi/anti/mark lowering in place, the remaining high-value subquery forms are
value-producing: `WHERE x > (SELECT MAX(y) FROM b WHERE b.k = a.k)`,
`SELECT a.*, (SELECT COUNT(*) FROM b WHERE b.k = a.k)`, and quantified comparisons
(`x < ANY(SELECT …)`, `x >= ALL(SELECT …)`). All are currently rejected by the targeted
`bind_structural` arms.

The standard decorrelation is: aggregate the inner side grouped by its correlation
columns into a derived relation `G`, LEFT JOIN the outer side against `G`, and rewrite
the subquery expression to a column reference — plus the "COUNT bug" repair: a grouped
reduce emits nothing for an empty group (`should_emit` gates on the group's net count,
`gnitz-engine/src/ops/reduce/op_reduce.rs:616-636`), so `COUNT` must read `0` through
the join's null-fill, not lose the row. GnitzDB has every ingredient: grouped and
global reduces (the global ground row synthesizes `COUNT=0`/others-NULL over an empty
source, `emit_global_ground`, `ops/reduce/emit.rs:116-182`, planner flag
`group_by.rs:407`), LEFT JOIN with weight-exact null-fill, the conditional-select
opcode for `COALESCE`, and the IR/splitter/hidden-chain substrate. This plan is pure
lowering — no engine, wire, or opcode changes.

**Cardinality note (the IVM-specific decision).** SQL requires a runtime error when a
scalar subquery yields more than one row. A materialized view cannot raise mid-tick, so
GnitzDB supports scalar subqueries only where single-row-ness is *provable*: a single
aggregate over the correlation group (grouped `G` emits exactly one row per key;
`global_ground` emits exactly one row total). Non-aggregate scalar subqueries stay
rejected with an error saying exactly that.

## Prerequisite state

The planner lowers CREATE VIEW through the bound IR (`plan/lp.rs`: `Rel` nodes incl.
`SemiAnti`/`Mark`, splitter, `create_view_chain` hidden-view bundles), and
`BoundExpr::Case`/`LitNull` with the `EXPR_SELECT`/`EXPR_LOAD_NULL` opcodes exist.
Verify against the live tree before starting.

## Scope (committed)

Supported after this plan (in CREATE VIEW bodies):

1. **Correlated scalar aggregate subqueries** — `(SELECT AGG(e) FROM <subplan> WHERE
   corr [AND inner-local…])` with `AGG ∈ {COUNT(*), COUNT(x), SUM, MIN, MAX, AVG}` —
   in WHERE comparisons, in projections, and inside CASE branches. Inside COALESCE:
   SUM/MIN/MAX/AVG work (they substitute as a bare `ColRef`, which COALESCE's null-test
   desugar accepts); a COUNT subquery inside COALESCE is rejected with a clean error —
   its substitution is a computed `Case` (below), and wrapping the already-never-NULL
   repaired COUNT in COALESCE is pointless anyway.
   Correlation: one or more `inner_col = outer_col` equality conjuncts in the
   subquery's top-level WHERE (equality only; range correlation with aggregation is
   rejected). The inner body may be anything the IR lowers (joins, filters, derived
   tables — it becomes a hidden chain).
2. **Uncorrelated scalar aggregate subqueries in comparisons** —
   `outer_col OP (SELECT AGG(e) FROM …)` with `OP ∈ {=, <, <=, >, >=}` and an
   **integer-typed aggregate** — lowered to a join against the one-row global-aggregate
   view (`=` → equi join, range op → pure-range join). Float-typed aggregates (`AVG`
   always, `group_by.rs:89`; `SUM`/`MIN`/`MAX` over a float source) are rejected at
   interception with a scalar-subquery-specific message: the lowering keys a join on
   the aggregate value and float join keys are categorically rejected
   (`reject_float_key` via `validate_join_key_pair`, `predicates.rs:114-116`) —
   without the up-front check the user would see a bewildering join-key error. The
   **correlated** form (§1) has no such limit: there the aggregate is a post-join
   `ColRef`, never a key, so float `AVG`/`SUM`/`MIN`/`MAX` work. `<>` and
   projection/CASE positions of *uncorrelated* scalars are rejected (they would need a
   keyless cross join; the error names the restriction).
3. **Quantified comparisons over subqueries**:
   - `x = ANY(sub)` ≡ `x IN (sub)` and `x <> ALL(sub)` ≡ `x NOT IN (sub)` — routed to
     the existing SemiAnti/Mark lowering with its restrictions.
   - `x OP ANY(sub)` / `x OP ALL(sub)` for range `OP`, correlated or uncorrelated-ANY,
     via MIN/MAX rewrites (below). The subquery output column must be **non-nullable**
     (with NULLs in the set, SQL's ANY/ALL go three-valued in ways the MIN/MAX rewrite
     cannot reproduce under negation; the restriction makes them coincide exactly).
   - Rejected: `= ALL`, `<> ANY`, uncorrelated `ALL` (empty-set-TRUE needs a keyless
     preserved join).
4. **Nesting**: subquery bodies lower recursively; a nested subquery may correlate to
   *its* immediately enclosing query only. Correlation across more than one level is
   rejected by scope construction (the two-scope alias map simply does not contain
   grander-outer relations, so the column fails to resolve, with an error naming the
   depth-1 rule).

Still rejected: non-aggregate scalar subqueries, scalar subqueries in HAVING/DML/direct
SELECT, uncorrelated EXISTS, subqueries in aggregate arguments, LATERAL.

## Design

### 1. IR lowering of a correlated scalar aggregate

For each scalar subquery expression found while lowering a WHERE/projection expression
(`Expr::Subquery(q)` whose body is a single-aggregate select):

- Split the inner WHERE conjuncts by side (the SemiAnti classifier's side analysis):
  inner-only → stay in the inner plan; `inner_col = outer_col` equalities → the
  correlation key list `K = [(inner_k_i, outer_k_i)]` (at least one required, else the
  uncorrelated path §2); anything else referencing the outer → reject.
- Build the hidden aggregate segment
  `G = Reduce { input: inner_plan, group_cols: [inner_k…], aggs: [agg, …implied] }` —
  the existing grouped-reduce emitter (`group_by.rs`), which already adds the required
  NULL-blind COUNT companion and AVG machinery. `G`'s output schema in full: the
  leading synthetic `_group_pk: U128` PK column for a general grouping
  (`group_by.rs:340-346`), then **explicitly synthesized group-column projections for
  every correlation column** (the `GroupCol` select-item path, `group_by.rs:480-492` —
  the subquery's own SELECT list is just the aggregate, so without synthesizing these
  the join below would have no key columns; equi-joining `a.k` against the hash PK is
  not an option), then the aggregate column(s). One row per live group (weight-exact
  under retraction; a group whose net count hits 0 disappears,
  `op_reduce.rs:624-636` — exactly what the LEFT JOIN's null-fill repairs).
- Stack `Join { kind: Left, left: outer_plan, right: G, eq: (outer_k…, g_k…,
  target_tcs) }` onto the FROM tree. A float-typed correlation column is rejected at
  interception with a scalar-subquery-specific message (it would otherwise surface as
  the grouped emitter's `reject_float_key` "GROUP BY" error, `group_by.rs:193`).
  Replace the subquery expression by:
  - `COUNT(*)` / `COUNT(x)`: `Case { [(IsNotNull(c), c)], else: LitInt(0) }` — the
    COUNT-bug repair; `c` is `G`'s agg column, nullable after the LEFT JOIN.
  - `SUM` / `MIN` / `MAX` / `AVG`: the bare `ColRef` — SQL defines these as NULL over an
    empty group, which is precisely what the null-fill delivers.
- Multiple scalar subqueries stack multiple `(G, LeftJoin)` pairs; no structural
  dedup of identical subqueries (each gets its own hidden segment — accepted cost,
  revisit only with evidence).

Weight-exactness: `G` emits weight-1 rows (one per group), so the LEFT JOIN preserves
each outer row at weight `w_A` — matched via the bilinear product `w_A·1`, unmatched via
`ν = w_A`. The rewritten predicate/projection is a linear filter/map over the join
output. Incremental transitions are inherited: when a group's aggregate changes, the
reduce retracts the old row and inserts the new one; the join propagates the paired
retraction/insertion; when the last inner row of a group is retracted, the reduce
retracts the `G` row and the null-fill re-emits the outer row with NULLs — flipping
`COALESCE(c,0)` back to 0 with balanced weights.

### 2. Uncorrelated scalar aggregates in comparisons

`G_global` = the ungrouped scalar aggregate view (`Reduce` with empty `group_cols`,
`global_ground = true` — the emitter already routes this, `group_by.rs:407-456`), which
holds **exactly one row at all times**: real aggregates when the source is non-empty,
the ground row (`COUNT = 0`, `SUM/MIN/MAX/AVG = NULL`) when empty.

`outer_col OP (SELECT AGG …)` lowers to a **join between the outer plan and
`G_global`** with the comparison as the join predicate — this is ordinary,
already-supported join SQL once `G_global` exists as a (hidden) relation:

- `=` → equi `Join` on `(outer_col, agg_col)`. A NULL aggregate (empty source,
  SUM/MIN/MAX ground) is dropped by the join's NULL-key gate — matching SQL
  (`x = NULL` is UNKNOWN → row filtered). `COUNT`'s ground row is a real `0`, so
  `x = 0` matches — also SQL-correct.
- `<, <=, >, >=` → pure-range **INNER** `Join` (`ON outer_col OP agg_col`, n_eq = 0).
  INNER skips the pure-range LEFT null-fill/threshold block entirely
  (`join.rs:836-838`), so neither the inline threshold reduce nor its ≤8-byte cap is in
  play — only `validate_range_join_key_pair` applies (integer aggregate per the scope
  restriction).

Because the join is INNER and `G_global` has exactly one weight-1 row, every surviving
outer row keeps weight `w_A`. Positions other than a WHERE comparison (projection,
CASE) are rejected for the uncorrelated form.

### 3. Quantifier rewrites (`Expr::AnyOp` / `Expr::AllOp`, sqlparser 0.56)

`right` must be `Expr::Subquery(q)`; `left` a column of the outer scope; `compare_op`
maps by table (`is_some` is a synonym for ANY). Let `S_K` be the subquery's value set
per correlation key `K` (or globally when uncorrelated), with the **non-nullable inner
column restriction** enforced at bind:

| Form | Lowering |
|------|----------|
| `x = ANY(S)` | `x IN (S)` → SemiAnti/Mark path |
| `x <> ALL(S)` | `x NOT IN (S)` → SemiAnti/Mark path (its non-nullable rules) |
| `x < ANY(S_K)` (corr.) | hidden `G(K, m = MAX(y))`; LEFT JOIN on K; predicate `x < m` |
| `x < ALL(S_K)` (corr.) | hidden `G(K, m = MIN(y))`; LEFT JOIN on K; predicate `m IS NULL OR x < m` |
| `x < ANY(S)` (uncorr.) | §2 pure-range join against `G_global(m = MAX(y))` |
| other range ops | same with MIN/MAX swapped per the op's direction |
| `= ALL`, `<> ANY`, uncorr. `ALL` | rejected |

Correctness of the three-valued edges under the non-nullable-inner restriction:

- **ANY over an empty group** must be FALSE: the LEFT JOIN null-fills `m = NULL`, the
  comparison evaluates UNKNOWN, and — because the rewritten predicate participates in
  ordinary 3VL — it behaves as SQL's UNKNOWN in *every* position (plain WHERE drops it;
  under NOT it stays UNKNOWN and drops). SQL's `ANY(∅)` is FALSE, not UNKNOWN, so under
  `NOT` the rewrite diverges (SQL: `NOT FALSE = TRUE`); therefore the ANY lowering emits
  `m IS NOT NULL AND x OP m` — the explicit existence conjunct makes empty-group ANY a
  definite FALSE, exact in all positions.
- **ALL over an empty group** must be TRUE: covered by the explicit `m IS NULL OR …`
  disjunct.
- **NULL outer `x`**: comparison is UNKNOWN in both SQL and the rewrite (the `m IS NOT
  NULL AND …` / `m IS NULL OR …` forms leave a NULL comparison NULL) — exact.

Both `IS [NOT] NULL` tests bind against the LEFT JOIN output where `m` is nullable, so
`fold_null_test`'s constant-folding does not fire.

### 4. Binder / IR touch points

- The targeted rejection arms for `Expr::Subquery` and `Expr::AnyOp/AllOp` in
  `bind_structural` remain the backstop for unsupported contexts (HAVING, DML, direct
  SELECT); view-WHERE/projection lowering intercepts these variants during expression
  lowering in `plan/lp.rs` *before* binding, replacing them with rewritten predicate
  fragments + stacked IR anchors, exactly as the mark rewrite does.
- Scalar-subquery detection: `Expr::Subquery(q)` where the body's projection is a
  single `FunctionArg` aggregate call (reusing the aggregate-binding vocabulary of the
  grouped builder, incl. `reject_unsupported_agg_qualifiers`); everything else →
  "scalar subqueries must be a single aggregate" error.
- Nesting falls out of recursion: lowering the inner plan runs the same interception
  against the inner scope pair. The alias map handed to a subquery's lowering contains
  only (enclosing relations, its own FROM) — a two-level-out column reference fails
  resolution with the depth-1 error.

## Sequencing

1. **Correlated scalar aggregates in WHERE** (`plan/lp.rs` interception, `G` segment
   synthesis, COUNT repair via `Case`): planner units + e2e incl. the COUNT bug.
2. **Scalar aggregates in projections/CASE** (same machinery, projection rewrite), e2e
   transition tests.
3. **Uncorrelated comparisons** (`G_global` + equi/pure-range join lowering), e2e incl.
   empty-source ground behavior for `=` vs range vs COUNT.
4. **ANY/ALL rewrites** (mapping table, existence conjuncts, restrictions), e2e truth
   tables.

Each step compiles and passes `make verify` + `make e2e` independently.

## Files touched

| File | Change |
|------|--------|
| `crates/gnitz-sql/src/plan/lp.rs` | scalar/quantifier interception during expression lowering; `G`/`G_global` segment synthesis; predicate rewrites |
| `crates/gnitz-sql/src/bind/structural.rs` | rejection-arm messages updated (context-specific: supported in views, not here) |
| `crates/gnitz-py/tests/test_scalar_subquery.py` | new e2e suite |
| `crates/gnitz-py/tests/test_exists.py` | ANY/ALL cases (`= ANY` / `<> ALL` parity with IN/NOT IN) |

## Testing

- **Planner units**: correlation-key extraction (multi-column, cross-width promotion);
  inner-local conjunct placement; every rejection (range correlation with aggregates,
  non-aggregate scalar, `<>` uncorrelated, `= ALL`, uncorrelated ALL, nullable inner
  column for range quantifiers, two-level correlation, scalar in HAVING/DML,
  **float-typed aggregate in the uncorrelated form** — `x = (SELECT AVG(y)…)` and
  `x < (SELECT MAX(f))` over a float `f` must produce the scalar-specific message, not
  a join-key error — float correlation column, COUNT inside COALESCE); rewrite shapes
  (COUNT → Case, SUM → bare ColRef, `G` synthesizing group-column projections, ANY
  existence conjunct, ALL disjunct).
- **E2E behavior** (W=4, weight-asserted, backfill + incremental for every scenario):
  - **COUNT bug**: outer rows with zero inner matches show `0` (not dropped, not NULL);
    insert first inner row → transitions to `1` with a balanced retract/insert pair;
    delete it → back to `0`.
  - SUM/MIN/MAX/AVG: NULL over empty group; MIN/MAX retraction of the current extremum
    with a **non-nullable** correlation key (the AggValueIndex fast path is gated on
    non-nullable group keys — a nullable key falls back to trace replay, correct but
    slower, so the AVI case must be exercised deliberately); AVG float path
    (correlated form only).
  - Scalar in WHERE comparisons crossing the threshold incrementally (rows entering and
    leaving the view as the group aggregate moves).
  - Uncorrelated: `x = (SELECT MAX…)` incl. empty source (no matches) and `COUNT`
    ground (`x = 0` matches an empty source); range form against a moving global MAX.
  - ANY/ALL: full truth-table sweep per op — empty set, all-smaller, all-larger, mixed,
    NULL outer x — for correlated and (ANY-only) uncorrelated forms; `= ANY`/`<> ALL`
    parity with IN/NOT IN.
  - Nesting: scalar aggregate whose inner FROM is a derived table with its own join;
    EXISTS inside a scalar subquery's WHERE (depth-1 each).
- **Perf**: `make bench` unchanged (no engine code touched); one e2e sanity timing on a
  scalar-subquery view chain kept generous.

## Invariants preserved

- No engine/wire/opcode changes; every circuit shape emitted already exists (grouped
  reduce, global-ground reduce, equi/range joins, LEFT null-fill, linear filter/map).
- Provable scalar cardinality: every value-producing subquery materializes through a
  reduce keyed by its correlation columns (or global ground) — one row per key by
  construction; no runtime-error path is introduced.
- Grouped reduces keep `global_ground = false` (only the genuine global scalar sets it,
  preserving the range-join threshold reduce's empty-trace requirement).
- Weight-exactness of the LEFT JOIN null-fill and reduce retract/insert pairs is relied
  on, not modified.
