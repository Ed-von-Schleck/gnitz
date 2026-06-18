# Residual JOIN-ON predicates via a post-join filter (INNER)

Extend the JOIN planner to accept ON clauses that carry conjuncts the physical
join cannot consume directly — a **second (or third…) range conjunct**, an
inequality (`<>`), a column-vs-literal test, a same-table comparison, an
arithmetic comparison — by collecting them as **residual** predicates and
evaluating them in a single linear `Filter` node spliced over the join's
normalized output. The Filter operator and its 3VL semantics already exist; this
change is entirely in the SQL front end (`gnitz-sql/src/planner.rs`) plus tests.
No engine change.

Scope is **INNER joins** (equi, band, and pure-range). LEFT/OUTER joins that
carry a residual are rejected with a clear error (rationale in §3).

---

## 1. Motivation — query surface this unlocks

Today a JOIN ON clause must be an `AND` of cross-table column equijoins plus *at
most one* cross-table column range conjunct. Anything else is a hard
`Unsupported` error (`collect_join_predicates`). These currently-rejected (but
semantically incremental-friendly) INNER shapes start working:

```sql
-- Two range conjuncts (band on the first, residual filter on the second):
SELECT * FROM a JOIN b ON a.x < b.y AND a.w > b.z;

-- Equijoin + inequality residual:
SELECT * FROM a JOIN b ON a.k = b.k AND a.tag <> b.tag;

-- Equijoin/band + column-vs-literal residual:
SELECT * FROM a JOIN b ON a.k = b.k AND a.ts > b.ts AND a.region = 5;

-- Residual comparing an expression:
SELECT * FROM a JOIN b ON a.k = b.k AND a.lo + 10 < b.hi;
```

Each is the join the engine already builds (equi prefix + ≤1 physical range
conjunct), with the leftover predicates applied as a row-wise selection on the
join output. Because that selection is linear, it is incrementally free
(`Q^Δ = Q` for LTI Q, CLAUDE.md §3) and needs no consolidation or extra state.

---

## 2. Committed design in one line

`collect_join_predicates` stops rejecting non-equi/-range conjuncts; it
**collects** them into a `Vec<Expr>` residual list. For an INNER join, the two
circuit builders bind that list against the merged join-output schema and splice
one `cb.filter(merged, prog)` over `merged` (the normalized
`[_join_pk × k, A cols, B cols]` union of the bilinear terms) before projection /
re-key / exchange.

---

## 3. Scope boundaries (committed, not deferrals)

These are part of the design, enforced by explicit planner errors:

- **At least one equijoin or range anchor is still required.** A residual-only ON
  (`ON a.r <> b.s`) would be an incremental cross-join, which the engine cannot
  build; the existing `left_cols.is_empty() && range.is_none()` guard rejects it.
  Residuals are only ever evaluated *alongside* a physical equi/range join.

- **A cross-table column `=` is always an equijoin key pair, validated as
  today.** A mixed string/native or wide-unsigned-cross-sign `=`
  (`a.s = b.n`, `a.u128 = b.i64`) still errors via `validate_join_key_pair`; it is
  **not** silently demoted to a residual equality filter. This keeps every
  existing equijoin-rejection test green and preserves the by-design key-type
  boundaries (CLAUDE.md / `wide-pk-incremental-views.md` §2).

- **LEFT/OUTER + residual is rejected.** For an outer join the ON predicate is
  part of the *match* condition: a preserved-side row whose only physical matches
  all fail the residual must still null-fill. The three null-fill constructions
  differ in whether they read the inner output:
  - Band (`n_eq ≥ 1`): `A − distinct(π_A(inner))` — reads `merged`, so a residual
    filter on `merged` *would* compose.
  - Equi: antijoin/correction keyed on `distinct(B)` *key existence* — independent
    of the inner output, so a residual filter would not retro-null-fill.
  - Pure-range (`n_eq == 0`): threshold `A − (A ⋈ {MAX/MIN(b.range)})` — keyed on
    the range predicate alone, likewise independent of the residual.

  Making LEFT correct means reworking the equi and pure-range null-fills to be
  inner-output-based — a separate, larger change. A consistent boundary
  (reject LEFT+residual everywhere) beats an inconsistent partial one
  (residuals silently work only on LEFT band joins). Spin LEFT residuals out as a
  separate plan if needed.

- **Single-table residuals are filtered post-join, not pushed into the pre-join
  input filter.** `a.region = 5` is evaluated on the join output, not on the
  `a`-side delta. Pushdown is a pure optimization and out of scope; correctness
  is unaffected.

- **Residual expression surface = what `compile_bound_expr` supports**: column
  refs (either table), int/float/string literals, arithmetic, all six
  comparisons, `AND`/`OR`, unary `-`/`NOT`, `IS [NOT] NULL`. `BETWEEN`, `IN`,
  `LIKE`, subqueries, and aggregates in ON are rejected with `Unsupported` (they
  have no `BoundExpr` form). This is a clean, safe surface; it can widen later
  without touching this plan's structure.

---

## 4. Current state (what the code does today)

All references are `crates/gnitz-sql/src/planner.rs` unless noted; line numbers
are orientation hints as of writing — anchor on the named items.

- **`execute_create_join_view`** (~1069) parses the join, builds `alias_map`
  (left alias → offset 0, right alias → offset `left_schema.columns.len()`),
  rejects self-joins, calls `extract_join_predicates`, then routes: a present
  range conjunct → `build_range_join_view`; otherwise the inline equi path.

- **`extract_join_predicates` / `collect_join_predicates`** (~2057 / ~2092)
  classify the ON `AND`-tree into `(left_cols, right_cols, target_tcs, range)`.
  The `Eq` arm consumes cross-table column equalities (validated by
  `validate_join_key_pair`, which returns the pair's common reindex type `T`).
  The range arm consumes one cross-table column `<,<=,>,>=` (canonicalized to
  `left OP right`, validated by `validate_range_join_key_pair`). A second range
  conjunct, and the catch-all `_`, both raise `Unsupported`.

- **Equi builder** (inline, ~1152–1377). After integrate/join/normalize it forms
  `merged` — for INNER, `inner_merged = union(proj_ab, proj_ba)` with layout
  `[_join_pk × k, A cols, B cols]` (`k = left_join_cols.len()`). It builds
  `out_cols` (that same `[_join_pk × k, A, B]` schema, `view_pk = 0..k`), applies
  the user projection `Map`, sinks. INNER output is co-partitioned by `_join_pk`
  (no output exchange).

- **`build_range_join_view`** (~1406). Reindexes both sides to
  `[eq slots…, range slot]` (`k = n_eq + 1`), integrates, runs the two
  `DeltaTraceRange` terms, normalizes each to `[A, B]`, and forms
  `merged = union(proj_ab, proj_ba)` (layout `[_join_pk × k, A, B]`) with schema
  `union_schema` (`pk_cols = 0..k`). It then re-keys `merged` onto the source-PK
  pair `[a.pk…, b.pk…]`, projects, and `cb.shard`s on the pair-PK before the sink
  (this join *does* have an output exchange). The LEFT null-fill branches read
  `merged` (band) / a threshold reduce (pure range).

- **Filter / binder plumbing already present**:
  - `cb.filter(node, Some(prog))` adds an `OpNode::Filter(blob)` (`circuit.rs`).
    Already used for the NULL-key gates (`multi_null_filter_prog`).
  - `compile_bound_expr(&BoundExpr, &Schema, &mut ExprBuilder)` →
    `eb.build(r)` → `ExprProgram` is the WHERE→filter lowering path
    (`gnitz-sql/src/expr.rs`, used at `execute_create_view` and
    `multi_null_filter_prog`).
  - `resolve_join_col_ref(expr, alias_map)` → **global** column index
    (`0..left_n` = left, `left_n..left_n+right_n` = right).

- **Engine — mid-circuit predicate Filter is already schema-correct.**
  `crates/gnitz-engine/src/query/compiler/emit.rs`, `OpNode::Filter` arm:
  ```rust
  let in_reg    = in_regs.get(&PORT_IN).copied().unwrap_or(0);
  let in_schema = reg_schemas[in_reg as usize];   // input register's schema
  reg_schemas[reg_id as usize] = in_schema;        // filter preserves schema
  // create_expr_predicate(... &in_schema ...) → Plan::from_predicate
  //   → prog.resolve_column_indices(&in_schema)
  ```
  Per-register schemas are threaded for every node; a `Union` sets its output to
  its `in_a` schema, and a `Map(Projection)` to `build_map_output_schema(...)`
  (PK region preserved, payload = projected columns). So a Filter whose input is
  `merged` resolves its predicate against exactly `[_join_pk × k, A, B]` — the
  same schema the planner constructs as `out_cols` / `union_schema`. A logical
  column index `k + g` (g = global) is a payload column and resolves to payload
  slot `g`. **No engine change is required**; the only existing predicate filters
  happen to sit on base inputs, but the handler is fully generic.

---

## 5. Why it is correct

- **Linearity / incrementality.** A row-wise selection on the join output is a
  linear operator: `filter(Δ(A⋈B)) = Δ(filter(A⋈B))`. Splicing it over `merged`
  (the union of the two bilinear delta terms) filters each term independently and
  sums — exactly the delta of the filtered join. It adds no state and needs no
  consolidation (CLAUDE.md §3, "Linear operators … Q^Δ = Q").

- **3VL matches INNER ON semantics.** For an INNER join, `A JOIN B ON p` keeps a
  pair iff `p` is *true*; `p` evaluating to NULL/UNKNOWN excludes the pair. The
  engine Filter already drops rows whose predicate is NULL or 0
  (`eval_pred_row`: `UNKNOWN → false`). So no extra "3VL bookkeeping" is needed
  for INNER — that bookkeeping is precisely the LEFT null-fill problem we exclude
  in §3. Every output row of an INNER join carries real A and B values (no
  null-fill), so residual columns read genuine values.

- **Both A and B columns are in scope.** `merged`'s payload is `[A cols, B cols]`
  (source PKs ride as ordinary payload via `build_reindex_program`), so a residual
  referencing either table — including a join key column, which is *also* copied
  into payload — resolves to a real column. Index map: global `g` → `merged`
  logical column `k + g` → engine payload slot `g`.

- **Placement vs. exchange.** Equi INNER has no output exchange (already
  `_join_pk`-co-partitioned) — the filter is partition-local. Range INNER filters
  `merged` *before* the source-PK re-key + `cb.shard`; filtering before a linear
  exchange is order-immaterial and correct.

---

## 6. Implementation

All edits in `crates/gnitz-sql/src/planner.rs` unless stated.

### 6.1 Thread a residual list through extraction

Extend the result tuple and the recursive collector to gather residual conjuncts
instead of rejecting them.

```rust
// type alias (~2045): add the residual list.
type JoinPredicates =
    (Vec<usize>, Vec<usize>, Vec<u8>, Option<RangeConjunct>, Vec<Expr>);

// extract_join_predicates (~2057): allocate + thread `residual`, return it.
fn extract_join_predicates(
    on_expr: &Expr, left_schema: &Schema, right_schema: &Schema,
    alias_map: &JoinAliasMap,
) -> Result<JoinPredicates, GnitzSqlError> {
    let mut left_cols = Vec::new();
    let mut right_cols = Vec::new();
    let mut target_tcs = Vec::new();
    let mut range: Option<RangeConjunct> = None;
    let mut residual: Vec<Expr> = Vec::new();
    collect_join_predicates(on_expr, left_schema, right_schema, alias_map,
        &mut left_cols, &mut right_cols, &mut target_tcs, &mut range, &mut residual)?;
    if left_cols.is_empty() && range.is_none() {
        // unchanged: residuals cannot stand alone (no incremental cross-join).
        return Err(GnitzSqlError::Bind(
            "JOIN ON must have at least one equijoin or range predicate".into()));
    }
    // arity cap (unchanged) …
    Ok((left_cols, right_cols, target_tcs, range, residual))
}
```

Rewrite `collect_join_predicates`'s binary-op arm so a conjunct is consumed as an
equi pair / range conjunct **only** when both operands resolve to columns of
different tables; everything else falls through to `residual.push(expr.clone())`.
The `_ =>` catch-all (`IS NULL`, `OR`-group, function, …) also pushes to
`residual` instead of erroring.

```rust
Expr::BinaryOp { left, op, right } => match op {
    BinaryOperator::And => { /* recurse both, unchanged */ }
    _ => {
        // Resolve both operands as columns (Err ⇒ not a plain cross-table
        // column conjunct ⇒ treat as residual; the residual binder re-resolves
        // and surfaces any genuine "column not found").
        let cross = match (resolve_join_col_ref(left,  alias_map),
                           resolve_join_col_ref(right, alias_map)) {
            (Ok(l), Ok(r)) => cross_table_pair(l, r, left_schema.columns.len()),
            _ => None,   // a literal/expression operand, or unresolved
        };
        match (op, cross) {
            // Cross-table column equality → equijoin key pair (validate; the dup
            // check and validate_join_key_pair are the EXISTING Eq-arm body).
            (BinaryOperator::Eq, Some((li, ri, _swapped))) => {
                /* existing dup-skip + validate_join_key_pair + push li/ri/T */
            }
            // Cross-table column range, slot free → the one physical range conjunct
            // (canonicalize with converse_rel on swap; existing range-arm body).
            (_, Some((li, ri, swapped)))
                if sql_binop_to_range_rel(op).is_some() && range.is_none() => {
                /* existing canonicalize + validate_range_join_key_pair + set range */
            }
            // Anything else (2nd range, <>, col=literal, same-table, expr) → residual.
            _ => residual.push(expr.clone()),
        }
    }
},
```

Add the small helper next to `converse_rel`:

```rust
/// `(left_idx, right_idx_rel, swapped)` when `l` and `r` (global indices)
/// reference different tables; `None` for same-table or same-side. `swapped`
/// is true when the *right* table's column was the left operand (drives
/// `converse_rel` in the range arm).
fn cross_table_pair(l: usize, r: usize, left_n: usize) -> Option<(usize, usize, bool)> {
    if l < left_n && r >= left_n { Some((l, r - left_n, false)) }
    else if r < left_n && l >= left_n { Some((r, l - left_n, true)) }
    else { None }
}
```

Notes:
- A cross-table column `=` whose types are incompatible still reaches
  `validate_join_key_pair` and errors (boundary preserved).
- `Expr::Nested` keeps its existing unwrap-and-recurse arm so parenthesization
  doesn't change classification.
- The residual order is the ON left-to-right order (deterministic; the filter
  ANDs them, so order is immaterial to results).

### 6.2 Join-aware residual binder

Add a recursive lowering from a residual `Expr` to a `BoundExpr` whose `ColRef`
indices are **merged-schema** indices (`payload_base + global`). It mirrors
`Binder::bind_expr`'s operator mapping but resolves columns through the
`alias_map` (so `a.r` and `b.s` disambiguate). `payload_base = k`.

```rust
/// Lower one residual ON conjunct against the join's merged output layout
/// `[_join_pk × k, A cols, B cols]`. `payload_base = k`; `resolve_join_col_ref`
/// yields the global index `g` (0..left_n+right_n), so a column maps to merged
/// logical index `payload_base + g`.
fn bind_join_residual(
    expr: &Expr, alias_map: &JoinAliasMap, payload_base: usize,
) -> Result<BoundExpr, GnitzSqlError> {
    let col = |e: &Expr| -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(payload_base + resolve_join_col_ref(e, alias_map)?))
    };
    match expr {
        Expr::Nested(inner) => bind_join_residual(inner, alias_map, payload_base),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => col(expr),
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse::<i64>().map(BoundExpr::LitInt)
                .or_else(|_| n.parse::<f64>().map(BoundExpr::LitFloat))
                .map_err(|_| GnitzSqlError::Bind(format!("invalid number literal: {n}"))),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) =>
                Ok(BoundExpr::LitStr(s.clone())),
            v => Err(GnitzSqlError::Unsupported(
                format!("JOIN ON residual: value type not supported: {v:?}"))),
        },
        Expr::BinaryOp { left, op, right } => {
            let l = bind_join_residual(left,  alias_map, payload_base)?;
            let r = bind_join_residual(right, alias_map, payload_base)?;
            Ok(BoundExpr::BinOp(Box::new(l), map_residual_binop(op)?, Box::new(r)))
        }
        Expr::UnaryOp { op, expr } => {
            let inner = bind_join_residual(expr, alias_map, payload_base)?;
            let uop = match op {
                UnaryOperator::Minus => UnaryOp::Neg,
                UnaryOperator::Not   => UnaryOp::Not,
                o => return Err(GnitzSqlError::Unsupported(
                    format!("JOIN ON residual: unary operator {o:?} not supported"))),
            };
            Ok(BoundExpr::UnaryOp(uop, Box::new(inner)))
        }
        Expr::IsNull(inner)    => Ok(BoundExpr::IsNull(
            payload_base + resolve_join_col_ref(inner, alias_map)?)),
        Expr::IsNotNull(inner) => Ok(BoundExpr::IsNotNull(
            payload_base + resolve_join_col_ref(inner, alias_map)?)),
        _ => Err(GnitzSqlError::Unsupported(
            "JOIN ON residual: unsupported expression (only column/literal \
             comparisons, arithmetic, AND/OR, unary -/NOT, IS [NOT] NULL)".into())),
    }
}

/// sqlparser binary op → BoundExpr BinOp for residuals (mirrors Binder::bind_expr).
fn map_residual_binop(op: &BinaryOperator) -> Result<BinOp, GnitzSqlError> {
    Ok(match op {
        BinaryOperator::Plus => BinOp::Add,   BinaryOperator::Minus => BinOp::Sub,
        BinaryOperator::Multiply => BinOp::Mul, BinaryOperator::Divide => BinOp::Div,
        BinaryOperator::Modulo => BinOp::Mod,
        BinaryOperator::Eq => BinOp::Eq,      BinaryOperator::NotEq => BinOp::Ne,
        BinaryOperator::Gt => BinOp::Gt,      BinaryOperator::GtEq => BinOp::Ge,
        BinaryOperator::Lt => BinOp::Lt,      BinaryOperator::LtEq => BinOp::Le,
        BinaryOperator::And => BinOp::And,    BinaryOperator::Or => BinOp::Or,
        o => return Err(GnitzSqlError::Unsupported(
            format!("JOIN ON residual: binary operator {o:?} not supported"))),
    })
}
```

Compile the AND of all residuals into one `ExprProgram` against the merged schema:

```rust
/// Build the post-join residual Filter program: AND every residual conjunct,
/// bound against `merged_schema` (`[_join_pk × k, A, B]`, payload_base = k).
/// `residual` is non-empty (callers guard).
fn build_residual_filter_prog(
    residual: &[Expr], alias_map: &JoinAliasMap,
    merged_schema: &Schema, payload_base: usize,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    let mut acc: Option<BoundExpr> = None;
    for e in residual {
        let b = bind_join_residual(e, alias_map, payload_base)?;
        acc = Some(match acc {
            None => b,
            Some(a) => BoundExpr::BinOp(Box::new(a), BinOp::And, Box::new(b)),
        });
    }
    let mut eb = ExprBuilder::new();
    let (r, _) = compile_bound_expr(&acc.expect("non-empty residual"), merged_schema, &mut eb)?;
    Ok(eb.build(r))
}
```

Imports to add to the `sqlparser::ast` use-list: `UnaryOperator`. To the
`crate::logical_plan` use-list: `UnaryOp`. (`Expr`, `Value`, `BinaryOperator`,
`BoundExpr`, `BinOp`, `ExprBuilder`, `compile_bound_expr` are already imported.)

### 6.3 Reject LEFT/OUTER + residual

In `execute_create_join_view`, immediately after `extract_join_predicates`,
before the equi/range split:

```rust
let (left_join_cols, right_join_cols, target_tcs, range_conjunct, residual) =
    extract_join_predicates(on_expr, &left_schema, &right_schema, &alias_map)?;

if is_left_join && !residual.is_empty() {
    return Err(GnitzSqlError::Unsupported(
        "LEFT/OUTER JOIN with a residual ON predicate (a non-equi/non-range \
         conjunct, or a second range conjunct) is not supported; the residual \
         would have to participate in the outer null-fill. Use INNER JOIN, or \
         move the predicate to a WHERE over a wrapping view.".into()));
}
```

Both builders therefore only ever see a non-empty `residual` for INNER joins.

### 6.4 Splice the filter — equi path

The equi path already constructs `out_cols` (`[_join_pk × k, A, B]`, `view_pk =
0..k`) just before the user projection. Insert the residual filter there,
shadowing `merged` so the projection consumes the filtered node:

```rust
// after out_cols is fully built (right before build_join_view_projection):
let merged = if residual.is_empty() {
    merged
} else {
    // residual ⇒ INNER (LEFT rejected in 6.3) ⇒ merged == inner_merged.
    let merged_schema = Schema { columns: out_cols.clone(), pk_cols: (0..k).collect() };
    let prog = build_residual_filter_prog(&residual, &alias_map, &merged_schema, k)?;
    cb.filter(merged, Some(prog))
};
```

`k = left_join_cols.len()`. Downstream (`is_identity`/projection `Map`, sink) is
unchanged — it already reads `merged`.

### 6.5 Splice the filter — range path

Add `residual: &[Expr]` to `build_range_join_view`'s signature and pass
`&residual` from the call site in `execute_create_join_view`. `union_schema`
(`[_join_pk × k, A, B]`, `pk_cols = 0..k`, `k = n_eq + 1`) is built right after
`merged`. Insert the filter there, shadowing `merged` before the source-PK re-key:

```rust
// immediately after `let union_schema = Schema { … };` and before the rekey:
let merged = if residual.is_empty() {
    merged
} else {
    // residual ⇒ INNER; the LEFT band null-fill (which also reads `merged`)
    // is unreachable here, so shadowing is safe.
    let prog = build_residual_filter_prog(residual, alias_map, &union_schema, k)?;
    cb.filter(merged, Some(prog))
};
```

`k = n_eq + 1`, `payload_base = k`. The re-key (`cb.map_reindex(merged, …)`),
projection, and `cb.shard` all consume the (now filtered) `merged`.

---

## 7. Tests

### 7.1 Planner unit (`crates/gnitz-sql/tests/planner_join.rs`)

- **Replace** `test_range_join_reject_two_range_conjuncts` (currently asserts
  `Unsupported` for `a.x < b.y AND a.w > b.z`) with
  `test_range_join_two_range_conjuncts_registers`: assert the view is **created**
  and `resolve_table_or_view_id` succeeds (first range → band physical, second →
  residual filter).
- **Add** positive registration tests (assert created + correct output schema /
  `view_pk`):
  - equijoin + inequality residual: `ON a.k = b.k AND a.t <> b.t`;
  - equijoin + column-vs-literal residual: `ON a.k = b.k AND a.r = 5`;
  - band + second-range residual: `ON a.k = b.k AND a.lo < b.hi AND a.x > b.y`;
  - pure-range + residual: `ON a.x < b.y AND a.r <> b.s`;
  - residual referencing an arithmetic expr: `ON a.k = b.k AND a.lo + 1 < b.hi`.
- **Add** negative tests (assert `Unsupported`, no view registered):
  - LEFT + residual: `… a LEFT JOIN b ON a.k = b.k AND a.t <> b.t` (the 6.3 guard);
  - residual-only ON: `… a JOIN b ON a.t <> b.t` (no equi/range anchor);
  - unsupported residual construct: `… ON a.k = b.k AND a.t LIKE b.t`.
- **Confirm unchanged** (these classify identically and must stay green):
  `test_join_reject_cross_sign` (`a.fk DECIMAL(38,0) = b.fk BIGINT`),
  `test_join_reject_mixed_string_native`, `test_join_reject_arity_cap`,
  `test_pure_range_left_join_rejects_wide_int_range_column`, the string-range
  rejections.

### 7.2 E2E correctness (`crates/gnitz-py/tests/`)

Run with `GNITZ_WORKERS=4` (the range path exchanges on the pair-PK; residual
correctness must hold across the fanout). Add `test_join_residual.py`:

- **Equijoin + residual:** seed `a`,`b`; create
  `SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k=b.k AND a.v <> b.v`;
  assert rows == the equi-join rows minus those with `a.v == b.v`. Then push
  INSERTs/DELETEs that flip the residual outcome (rows entering/leaving the
  result) and assert incremental deltas match a from-scratch recompute.
- **Band + second range residual:** `ON a.k=b.k AND a.lo<b.hi AND a.x>b.y`;
  compare against a brute-force INNER evaluation of all three conjuncts after a
  mixed insert/delete workload.
- **Pure range + residual:** `ON a.x<b.y AND a.r<>b.s`, similarly verified.
- **NULL 3VL:** a nullable residual column that is NULL must exclude the pair
  (INNER), and a later UPDATE making it non-NULL must emit the row.

A focused way to force the range *merge-walk* path (large delta vs. trace) is in
`memory/range-join-strategy-selection-distribution.md`; reuse it so the residual
is exercised on both `range_per_row_seek` and `range_merge_walk` outputs.

### 7.3 Gate

`make verify` (fmt + clippy-as-errors + unit) and
`make e2e K='residual' WORKERS=4`.

---

## 8. Edge cases & risks

- **Residual references a join-key column** (`ON a.k=b.k AND a.k>100`): the key
  also rides in payload, so `resolve_join_col_ref(a.k)` → its left global index →
  payload column. Reads the real value. ✓
- **Mixed-type / cross-sign residual comparisons** (`a.i32 > b.i64`,
  unsigned vs signed): handled by `compile_bound_expr`'s existing per-column-type
  opcode selection — identical machinery to WHERE-clause comparisons, not new
  here. Tests should include a same-width signed pair and an int-vs-literal case;
  any exotic gap is a pre-existing `compile_bound_expr` limitation, surfaced as an
  `Unsupported`/wrong-type error at CREATE, never silent corruption.
- **String residuals** (`a.s <> b.s`): `compile_bound_expr` has the
  `str_col_*_col` col-vs-col path for `String == String`; `<>` lowers to
  `NOT (eq)`. Include a string-residual test.
- **`out_cols.clone()` in the equi path** is a one-time `Vec<ColumnDef>` clone at
  CREATE (cold path), not per-row — negligible.
- **No new circuit node types, wire opcodes, or catalog schema**: `OpNode::Filter`
  already round-trips (`circuit.rs`), so existing serialized views are unaffected
  and there are no migration concerns (gnitz is pre-alpha regardless).
- **Determinism / weights:** the filter is weight-preserving and per-row; it
  cannot reorder or merge rows, so it cannot perturb the `(PK, payload)` sort
  invariants downstream (it only drops whole rows).

---

## 9. File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-sql/src/planner.rs` | `JoinPredicates` +residual; `extract_join_predicates`/`collect_join_predicates` collect instead of reject; `cross_table_pair`, `bind_join_residual`, `map_residual_binop`, `build_residual_filter_prog` helpers; LEFT+residual guard; filter splice in the equi path and in `build_range_join_view` (+`residual` param); `UnaryOperator`/`UnaryOp` imports |
| `crates/gnitz-sql/tests/planner_join.rs` | Flip `test_range_join_reject_two_range_conjuncts`; add positive/negative residual tests |
| `crates/gnitz-py/tests/test_join_residual.py` | New: INNER residual correctness + incremental + 3VL, `WORKERS=4` |

Engine, wire, core, capi: **no changes.**
