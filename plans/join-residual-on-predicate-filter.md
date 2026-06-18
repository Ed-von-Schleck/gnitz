# Residual JOIN-ON predicates via a post-join filter (INNER)

Extend the JOIN planner to accept ON clauses that carry conjuncts the physical
join cannot consume directly — a **second (or third…) range conjunct**, an
inequality (`<>`), a column-vs-literal test, a same-table comparison, an
arithmetic comparison — by collecting them as **residual** predicates and
evaluating them in a single linear `Filter` node spliced over the join's
normalized output. The Filter operator and its 3VL semantics already exist; the
change lives in the SQL front end — `gnitz-sql/src/binder.rs` (a unified
`bind_structural` expression core that the residual, WHERE, and HAVING binders
all share — §6.2), `gnitz-sql/src/planner.rs` (collection, splicing, the HAVING
leaf), and `gnitz-sql/src/expr.rs` (hardening the shared expression compiler so a
string/blob column can never be silently miscompiled into the integer path) —
plus tests. No engine change.

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

The binding is delivered by collapsing GnitzDB's three structural `Expr →
BoundExpr` walks (`Binder::bind_expr`, `bind_having_expr`, and the would-be join
binder) onto one `bind_structural` core parametrized by a three-method
`LeafBinder` (column / function / null-test). The residual is one small leaf; the
unification also retires HAVING's drift (`bind_having_expr` is missing `UnaryOp`,
`BETWEEN`, and `Mul`/`Div`/`Mod` today). SQL front-end only; no engine change.

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

- **Residual expression surface = the shared `bind_structural` surface** (§6.2):
  column refs (either table), int/float/string literals, arithmetic, all six
  comparisons, `AND`/`OR`, unary `-`/`NOT`, `IS [NOT] NULL`, and `BETWEEN`. `IN`,
  `LIKE`, subqueries, and aggregates in ON are rejected with `Unsupported` (the
  `JoinResidual` leaf rejects aggregates outright). Because this is the *same*
  core the WHERE binder uses, the residual surface widens automatically whenever
  `bind_structural` does — there is no separate surface to keep in sync.

- **String/blob residuals compare by content; mixed string↔non-string is
  rejected.** A residual comparing two German-string columns (`STRING` or `BLOB`,
  either combination) — or a German-string column against a string literal —
  lowers to the engine's content-comparison opcodes (`str_col_*`). Any other use
  of a string/blob column in a residual (arithmetic, or a comparison whose other
  operand is a non-string column) is rejected at CREATE by the hardened
  `compile_bound_expr` (§6.6) with an `Unsupported` error — never silently
  miscompiled. This boundary is enforced in the shared expression compiler, so it
  also closes the same latent hole for WHERE / computed-projection expressions.

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
  - Three structural `Expr → BoundExpr` binders exist today, sharing nothing:
    `Binder::bind_expr` (binder.rs — WHERE/projection/set-op/DML, single-table),
    and `bind_having_expr` + `bind_having_null_test` (planner.rs — grouped
    relation). `Binder::bind_expr` is used *only* for recursion
    (`#[allow(clippy::only_used_in_recursion)]`), so its body is a free function
    in disguise. `bind_having_expr` has drifted (no `UnaryOp`/`BETWEEN`/`Mul`/
    `Div`/`Mod`); HAVING materialises aggregates in a `collect_having_aggs`
    pre-pass that walks the same tree (§6.2). `bind_do_update_rhs` (DML ON
    CONFLICT) already delegates to `bind_expr` after its EXCLUDED check — the
    pattern done right.

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

### 6.2 Unify expression binders on a `LeafBinder` core

The residual needs an `Expr → BoundExpr` lowering that resolves `a.r`/`b.s`
through the join `alias_map`. A bespoke `bind_join_residual` would be the **third**
hand-copy of one structural recursion. The first two already exist and have
**already drifted**:

| Binder | Column leaf | Function leaf | Null-test leaf |
|--------|-------------|---------------|----------------|
| `Binder::bind_expr` (binder.rs) | `find_unique_column` | COUNT/SUM/… → `AggCall` | column nullability fold |
| `bind_having_expr` (planner.rs) | group-col → reduce pos | agg → reduce `ColRef` | companion `cnt==0` |

`bind_having_expr` is the proof of the hazard: it dropped `UnaryOp`, `BETWEEN`,
and `Mul`/`Div`/`Mod` from its operator map. The audit's Issues 3 & 4 (missing IS
NULL fold, missing `BETWEEN`) are the *same* drift the residual copy would
re-introduce. So instead of writing copy #3, **collapse all three onto one
structural recursion** parametrized by a three-method leaf. Each binder then *is*
the canonical structural surface plus a tiny leaf; drift becomes impossible.

The only things that genuinely differ across the three are the three leaves —
column reference, function call, and `IS [NOT] NULL` — measured against source.
Everything else (literals, the full operator map, `UnaryOp`, the `BETWEEN`
desugar, `Nested` unwrap) is shared and lives once in `bind_structural`.

```rust
// binder.rs — the shared core + the leaf abstraction.
use sqlparser::ast::Function;

pub(crate) trait LeafBinder {
    /// An `Identifier` / `CompoundIdentifier` column reference.
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError>;
    /// A function call (aggregates, or a context-specific rejection).
    fn bind_function(&self, f: &Function) -> Result<BoundExpr, GnitzSqlError>;
    /// `inner IS [NOT] NULL` (`want_null` picks IS NULL vs IS NOT NULL).
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError>;
}

/// The one structural recursion. Needs no schema — every schema-aware decision
/// is a leaf method. Literals, operators, BETWEEN, and Nested are defined here
/// exactly once, so a new operator or fold lands on all three binders at once.
pub(crate) fn bind_structural<L: LeafBinder>(
    expr: &Expr, leaf: &L,
) -> Result<BoundExpr, GnitzSqlError> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => leaf.bind_column(expr),
        Expr::Function(f)  => leaf.bind_function(f),
        Expr::IsNull(i)    => leaf.bind_null_test(i, true),
        Expr::IsNotNull(i) => leaf.bind_null_test(i, false),
        Expr::Nested(i)    => bind_structural(i, leaf),
        Expr::Value(vws)   => bind_literal(&vws.value),
        Expr::BinaryOp { left, op, right } => {
            let l = bind_structural(left, leaf)?;
            let r = bind_structural(right, leaf)?;
            Ok(BoundExpr::BinOp(Box::new(l), map_binop(op)?, Box::new(r)))
        }
        Expr::UnaryOp { op, expr } => {
            let inner = bind_structural(expr, leaf)?;
            let uop = match op {
                UnaryOperator::Minus => UnaryOp::Neg,
                UnaryOperator::Not   => UnaryOp::Not,
                o => return Err(GnitzSqlError::Unsupported(
                    format!("unary operator {o:?} not supported"))),
            };
            Ok(BoundExpr::UnaryOp(uop, Box::new(inner)))
        }
        // `e BETWEEN lo AND hi` ≡ `e >= lo AND e <= hi`; NOT BETWEEN negates it.
        Expr::Between { expr: e, negated, low, high } => {
            let ge = BoundExpr::BinOp(Box::new(bind_structural(e, leaf)?), BinOp::Ge,
                                      Box::new(bind_structural(low, leaf)?));
            let le = BoundExpr::BinOp(Box::new(bind_structural(e, leaf)?), BinOp::Le,
                                      Box::new(bind_structural(high, leaf)?));
            let between = BoundExpr::BinOp(Box::new(ge), BinOp::And, Box::new(le));
            Ok(if *negated { BoundExpr::UnaryOp(UnaryOp::Not, Box::new(between)) } else { between })
        }
        _ => Err(GnitzSqlError::Unsupported(format!("expression type not supported: {expr:?}"))),
    }
}

/// sqlparser binary op → `BinOp` (the single, complete map).
fn map_binop(op: &BinaryOperator) -> Result<BinOp, GnitzSqlError> {
    Ok(match op {
        BinaryOperator::Plus => BinOp::Add, BinaryOperator::Minus => BinOp::Sub,
        BinaryOperator::Multiply => BinOp::Mul, BinaryOperator::Divide => BinOp::Div,
        BinaryOperator::Modulo => BinOp::Mod,
        BinaryOperator::Eq => BinOp::Eq, BinaryOperator::NotEq => BinOp::Ne,
        BinaryOperator::Gt => BinOp::Gt, BinaryOperator::GtEq => BinOp::Ge,
        BinaryOperator::Lt => BinOp::Lt, BinaryOperator::LtEq => BinOp::Le,
        BinaryOperator::And => BinOp::And, BinaryOperator::Or => BinOp::Or,
        o => return Err(GnitzSqlError::Unsupported(format!("binary operator {o:?} not supported"))),
    })
}

/// Number/string literal → `BoundExpr` (the existing bind_expr `Value` body).
fn bind_literal(v: &Value) -> Result<BoundExpr, GnitzSqlError> {
    match v {
        Value::Number(n, _) => n.parse::<i64>().map(BoundExpr::LitInt)
            .or_else(|_| n.parse::<f64>().map(BoundExpr::LitFloat))
            .map_err(|_| GnitzSqlError::Bind(format!("invalid number literal: {n}"))),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(BoundExpr::LitStr(s.clone())),
        _ => Err(GnitzSqlError::Unsupported(format!("value type not supported in expressions: {v:?}"))),
    }
}

/// Shared NOT-NULL fold: a non-nullable column makes `IS [NOT] NULL` a constant,
/// keeping the null-tracking opcode (which forces the batch evaluator's slow path
/// — `is_strictly_non_nullable` returns false on any is_null) out of the program.
fn fold_null_test(schema: &Schema, idx: usize, want_null: bool) -> BoundExpr {
    if !schema.columns[idx].is_nullable { BoundExpr::LitInt(i64::from(!want_null)) }
    else if want_null { BoundExpr::IsNull(idx) } else { BoundExpr::IsNotNull(idx) }
}
```

**Leaf 1 — `SingleTable` (WHERE, projections, set-ops, DML).** `Binder::bind_expr`
becomes a one-line wrapper; the moved aggregate body and column lookup are
verbatim, so every existing WHERE/projection caller is byte-for-byte unchanged.

```rust
struct SingleTable<'a> { schema: &'a Schema }
impl SingleTable<'_> {
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        let name = match e {
            Expr::Identifier(id) => &id.value,
            Expr::CompoundIdentifier(p) if p.len() == 2 => &p[1].value,   // single-table: qualifier is informational
            _ => return Err(GnitzSqlError::Unsupported("expected a column reference".into())),
        };
        find_unique_column(&self.schema.columns, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found")))
    }
}
impl LeafBinder for SingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> { Ok(BoundExpr::ColRef(self.idx(e)?)) }
    fn bind_function(&self, f: &Function) -> Result<BoundExpr, GnitzSqlError> {
        // … the existing bind_expr aggregate body (COUNT/SUM/MIN/MAX/AVG → AggCall,
        // MIN/MAX wide-type reject), with arguments bound via bind_structural(arg, self) …
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        Ok(fold_null_test(self.schema, self.idx(inner)?, want_null))
    }
}
impl Binder<'_> {
    pub fn bind_expr(&self, expr: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
        bind_structural(expr, &SingleTable { schema })
    }
}
```

**Leaf 2 — `JoinResidual` (the feature).** Resolves columns through the
`alias_map`, shifting the global index `g` into the merged layout
`[_join_pk × k, A, B]` by `+ base` (`base = k`); folds nulls against `merged`
(which carries each base column's real nullability for INNER — the equi
`out_cols` / range `union_cols` clone the source `ColumnDef`s); aggregates in ON
are rejected.

```rust
struct JoinResidual<'a> { alias_map: &'a JoinAliasMap, merged: &'a Schema, base: usize }
impl JoinResidual<'_> {
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        Ok(self.base + resolve_join_col_ref(e, self.alias_map)?)   // resolve_join_col_ref unwraps Nested
    }
}
impl LeafBinder for JoinResidual<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> { Ok(BoundExpr::ColRef(self.idx(e)?)) }
    fn bind_function(&self, _f: &Function) -> Result<BoundExpr, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported("JOIN ON: aggregate functions are not allowed".into()))
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        Ok(fold_null_test(self.merged, self.idx(inner)?, want_null))
    }
}
```

**Leaf 3 — `Having` (fixes its own drift).** `bind_having_expr` /
`bind_having_null_test` collapse into this leaf; the `UnaryOp`, `BETWEEN`, and
`Mul`/`Div`/`Mod` it lacked are now inherited from the core. The three leaf bodies
are the *current* HAVING arms, moved verbatim.

```rust
struct Having<'a> { ctx: &'a HavingCtx<'a> }
impl LeafBinder for Having<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        // current bind_having_expr Identifier arm: source col → group_col_reduce_pos,
        // rejecting a non-grouped column (CompoundIdentifier resolves the bare name the same way).
    }
    fn bind_function(&self, f: &Function) -> Result<BoundExpr, GnitzSqlError> {
        // current bind_having_expr Function arm: resolve_having_mapping → Avg / NullfillSum / Direct.
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        // current bind_having_null_test body (Nested unwrap, companion cnt==0, else reject).
    }
}
fn bind_having_expr(expr: &Expr, ctx: &HavingCtx) -> Result<BoundExpr, GnitzSqlError> {
    bind_structural(expr, &Having { ctx })
}
```

**Lockstep with `collect_having_aggs`.** HAVING materialises its aggregates in a
*pre-pass* (`collect_having_aggs`, step 3b) that must run before the reduce schema
is built — it is not a `bind_structural` consumer, so it must descend the same
node set the binder now reaches. It already recurses `BinaryOp`/`UnaryOp`/`IsNull`/
`Nested`/`Function`; the binder newly reaches `BETWEEN`, so add the matching arm
(otherwise `HAVING agg BETWEEN lo AND hi` binds an aggregate that was never
materialised and `resolve_having_mapping` fails):

```rust
Expr::Between { expr, low, high, .. } => {
    collect_having_aggs(expr, source_schema, agg_specs, agg_mappings)?;
    collect_having_aggs(low,  source_schema, agg_specs, agg_mappings)?;
    collect_having_aggs(high, source_schema, agg_specs, agg_mappings)
}
```

This is the *only* coupling: the agg-collection node set must be a superset of the
binder's. The agg ordering (SELECT aggs in 3a, HAVING-only in 3b) is unchanged, so
the SELECT projection's column positions are untouched.

**The residual filter program** then binds through the `JoinResidual` leaf:

```rust
/// AND every residual conjunct (bound against the merged join-output schema via
/// the JoinResidual leaf) into one ExprProgram. `residual` is non-empty (callers guard).
fn build_residual_filter_prog(
    residual: &[Expr], alias_map: &JoinAliasMap,
    merged_schema: &Schema, payload_base: usize,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    let leaf = JoinResidual { alias_map, merged: merged_schema, base: payload_base };
    let mut acc: Option<BoundExpr> = None;
    for e in residual {
        let b = bind_structural(e, &leaf)?;
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

**Deleted / not written:** `map_residual_binop` and the standalone
`bind_join_residual` (never created); the duplicated operator map, `Value` body,
and recursion inside `bind_having_expr`. **Inherited for free:** the IS NULL fold
and `BETWEEN` in the residual (Issues 3 & 4 — structurally impossible to drift now)
and `UnaryOp`/`BETWEEN`/`Mul`/`Div`/`Mod` in HAVING; `IS [NOT] NULL` on a
compound identifier (`a.x IS NULL` in a residual, `t.x IS NULL` in WHERE) now
works everywhere — the old `bind_expr` accepted only a bare `Identifier`.

**Placement / visibility.** The trait, `bind_structural`, and the shared helpers
(`map_binop`, `bind_literal`, `fold_null_test`) live in binder.rs as `pub(crate)`
so planner.rs can implement leaves against them. `SingleTable` stays in binder.rs
(it needs only `Schema` + `find_unique_column`); `JoinResidual` and `Having` live
in planner.rs beside their resolution context (`resolve_join_col_ref`/
`JoinAliasMap`, `HavingCtx`/`group_col_reduce_pos`/`resolve_having_mapping`). A
leaf's `bind_function` recurses via `bind_structural(arg, self)`, so the core
must be generic over `L: LeafBinder` (static dispatch), not `&dyn LeafBinder`.

Imports: add `UnaryOperator` and `Function` to the `sqlparser::ast` use-list in
binder.rs; `UnaryOp` is already imported there. (`Expr`, `Value`,
`BinaryOperator`, `BoundExpr`, `BinOp`, `find_unique_column` are already in scope.)

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

### 6.6 Harden the expression compiler (`crates/gnitz-sql/src/expr.rs`)

The residual binder feeds `compile_bound_expr`, the same lowering WHERE clauses
and computed projections use. Two pre-existing gaps in that compiler turn a
string/blob column into a silently wrong integer program, and a residual is a new
way to reach them. Both are closed here, in the shared compiler, so WHERE and
projection expressions benefit too.

**Root cause.** A `STRING`/`BLOB` value is a 16-byte German-string descriptor.
`compile_bound_expr`'s `ColRef` arm lowers any non-float, non-128-bit column with
`eb.load_col_int`, and the engine's `EXPR_LOAD_PAYLOAD_INT` handler has arms only
for 1/2/4/8-byte columns (`match col_size { … _ => {} }`, `expr/batch.rs`): a
16-byte column hits the no-op arm and leaves the destination register holding
**stale scratch bytes**, which the following compare reads — silent,
nondeterministic corruption, no error, no panic. (The row-at-a-time interpreter
in `dml.rs` already rejects string/blob/U128 residual columns; only the
view-circuit compiler is exposed.)

**6.6a — route blob comparisons through the German-string opcodes.**
`try_compile_string_cmp` recognizes a comparison only when the column is
`TypeCode::String`, so a `BLOB` falls through to the integer path. Blobs share
the 16-byte layout and the engine's `str_col_*` opcodes already content-compare
both (they dispatch through `compare_german_strings`). Widen the two type guards
from `== TypeCode::String` to `.is_german_string()` (true for `STRING` and
`BLOB`):

```rust
// col-vs-const guard:
if schema.columns[idx].type_code.is_german_string() {
// col-vs-col guard:
if schema.columns[*a].type_code.is_german_string()
   && schema.columns[*b].type_code.is_german_string() {
```

This makes `a.blob <> b.blob`, `a.s < 'lit'`, etc. compile to the correct
content comparison. The existing `_ => Err("… not supported for strings")` arms
and the original-`op`-in-message behavior are unchanged (broaden the wording to
"strings/blobs").

**6.6b — reject a string/blob column reaching the integer path.** Every *valid*
string/blob use is the six comparisons, which `try_compile_string_cmp` intercepts
before any recursion into `compile_bound_expr`. So a `STRING`/`BLOB` reaching the
`ColRef` arm means it is being used in arithmetic or a mixed-type comparison
(`a.s > b.int`): reject it instead of emitting a garbage int load. Add the arm
alongside the existing `U128`/`UUID` rejection:

```rust
BoundExpr::ColRef(idx) => {
    let tc = schema.columns[*idx].type_code;
    match tc {
        TypeCode::U128 | TypeCode::UUID => Err(/* unchanged */),
        TypeCode::String | TypeCode::Blob => Err(GnitzSqlError::Unsupported(format!(
            "column {:?} is {tc:?}; string/blob columns support only =, <>, <, <=, \
             >, >= against another string/blob column or a string literal — not \
             arithmetic or comparison with a non-string column",
            schema.columns[*idx].name))),
        TypeCode::F32 | TypeCode::F64 => Ok((eb.load_col_float(*idx), true)),
        _ => Ok((eb.load_col_int(*idx), false)),
    }
}
```

This guard is the safety net for *all* contexts (arithmetic, unary, comparison),
strictly subsuming a per-operator check: it fires exactly at the corruption site.
It cannot reject a valid program — `infer_type` returns `I64` for every
comparison/boolean result, so a nested predicate like `(a.s = 'x') AND (a.n > 5)`
never presents a string-typed operand to an enclosing op, and the genuine string
comparison was already consumed by `try_compile_string_cmp`. With 6.6a + 6.6b,
the residual surface's string/blob behavior is: content comparison when both
sides are string/blob (or string-literal), `Unsupported` at CREATE otherwise.

Because this tightens the *shared* compiler, an existing WHERE / computed-projection
test that registered a string/blob column in a numeric context relied on the
silent miscompile and now errors at CREATE; such a test was asserting a bug and
should be re-pointed at the new `Unsupported` (none is expected — plain
string/blob column *projection* is `PassThrough`, never compiled through this
arm).

---

## 7. Tests

### 7.1 Extraction unit tests (`crates/gnitz-sql/src/planner.rs`, inline `mod tests`)

`JoinPredicates` gains a fifth element, so the extraction test helpers must absorb
it (mechanical, no behavior change):
- `extract` and `extract_with_tcs`: extend the `.map(|(l, r, …)| …)` closures with
  one trailing `_` (`|(l, r, _, _, _)|`, `|(l, r, t, range, _)|`).
- `extract_full` returns the full `JoinPredicates`; its four `(_, _, _, range)`
  call sites (`extract_equi_only_has_no_range`, the band/eq-prefix tests,
  `range_pair_cross_sign_promotes`) each gain a trailing `_`.
- **`range_extraction_rejections`:** drop the first assertion — `a.x < b.x AND
  a.y < b.y` is no longer `Unsupported`; the first range is physical, the second a
  residual. Keep the single-conjunct string/blob/float-range and same-table
  rejections (unchanged). Add a sibling assertion that a second range conjunct
  lands in `residual` (extract via a helper returning the full tuple, assert
  `residual.len() == 1`).
- **Add** positive extraction tests: an equijoin + inequality (`a.k=b.k AND
  a.t<>b.t`) classifies as one equi pair, no range, `residual.len() == 1`; a
  band + second-range residual yields `range.is_some()` and `residual.len() == 1`.

### 7.2 Expression-compiler unit tests (`crates/gnitz-sql/src/expr.rs`, inline `mod tests`)

The `str_schema` helper (U64 pk, two `String` cols) already exists; add a `Blob`
column and a mixed-type case:
- **Blob comparison compiles to content opcodes:** `try_compile_string_cmp` on
  `blob <> blob` and `blob </<=/>/>= 'lit'` returns `Some` and emits the
  `str_col_*` program byte-identical to the `String` form (6.6a).
- **Mixed string↔int rejects:** `compile_bound_expr` on `BinOp(ColRef(string),
  Gt, ColRef(i64))` returns `Err(Unsupported)` (6.6b) — the corruption case,
  now a clean error.
- **String arithmetic rejects:** `compile_bound_expr` on `ColRef(string) + 1`
  returns `Err(Unsupported)`.
- **Confirm green:** the existing `string_cmp_*` symmetry tests (still `String`).

### 7.3 Binder unification (`binder.rs` inline tests + HAVING E2E)

The unification must leave every existing binder caller behaviorally identical
except for the intended widenings. Guard both:
- **Regression — WHERE/projection unchanged:** the existing `binder.rs` tests
  (`test_bind_between_desugars_to_comparison_tree`, the MIN/MAX accept/reject
  pairs) stay green unmodified — `bind_expr` is now a `SingleTable`-leaf wrapper
  over `bind_structural`, same output.
- **Widening — compound-ident null test:** add a `binder.rs` test that
  `t.x IS NULL` / `t.x IS NOT NULL` (a `CompoundIdentifier` inner) now binds to
  the folded constant / `IsNull` instead of erroring "IS NULL on non-column".
- **HAVING gains (drift retired), E2E in `crates/gnitz-py/tests/`** — these error
  today and must now compute correctly against a brute-force recompute:
  - `… GROUP BY g HAVING SUM(v) * 2 > 10` (the `Mul`/`Div`/`Mod` gap);
  - `… HAVING NOT (COUNT(*) = 1)` (the `UnaryOp` gap);
  - `… HAVING SUM(v) BETWEEN 1 AND 10` (the `BETWEEN` gap **and** the
    `collect_having_aggs` lockstep — the aggregate inside `BETWEEN` must be
    materialised, else binding fails to resolve it).
- **HAVING regression:** existing HAVING E2E (plain `HAVING agg <cmp> lit`,
  `HAVING agg IS NULL` on nullable SUM/AVG) stay green — the leaf bodies are moved
  verbatim.

### 7.4 Planner integration (`crates/gnitz-sql/tests/planner_join.rs`)

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
  - residual referencing an arithmetic expr: `ON a.k = b.k AND a.lo + 1 < b.hi`;
  - string residual: `ON a.k = b.k AND a.s1 <> b.s2` (both `VARCHAR`);
  - blob residual: `ON a.k = b.k AND a.b1 <> b.b2` (both `BLOB`);
  - `BETWEEN` residual: `ON a.k = b.k AND a.v BETWEEN 1 AND 10`.
- **Add** negative tests (assert `Unsupported`, no view registered):
  - LEFT + residual: `… a LEFT JOIN b ON a.k = b.k AND a.t <> b.t` (the 6.3 guard);
  - residual-only ON: `… a JOIN b ON a.t <> b.t` (no equi/range anchor);
  - unsupported residual construct: `… ON a.k = b.k AND a.t LIKE b.t`;
  - mixed string↔int residual: `… ON a.k = b.k AND a.s <> b.n` (`a.s VARCHAR`,
    `b.n BIGINT`) — rejected by 6.6b, no silent corruption.
- **Confirm unchanged** (these classify identically and must stay green):
  `test_join_reject_cross_sign` (`a.fk DECIMAL(38,0) = b.fk BIGINT`),
  `test_join_reject_mixed_string_native`, `test_join_reject_arity_cap`,
  `test_pure_range_left_join_rejects_wide_int_range_column`,
  `test_range_join_reject_string_range_pair`.

### 7.5 E2E correctness (`crates/gnitz-py/tests/`)

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
- **String/blob residual:** `ON a.k=b.k AND a.s <> b.s` (`VARCHAR`) and a `BLOB`
  variant; verify content comparison (not descriptor bytes) by seeding rows whose
  string/blob values share a 4-byte prefix but differ in the heap tail, so a
  descriptor-byte compare would wrongly equate them.
- **NULL 3VL:** a nullable residual column that is NULL must exclude the pair
  (INNER), and a later UPDATE making it non-NULL must emit the row. Separately,
  `… AND a.x IS NULL` on a **NOT NULL** `a.x` must register and produce the empty
  result (the 6.2 fold to `LitInt(0)`).
- **Mixed-type rejection:** `CREATE VIEW … ON a.k=b.k AND a.s <> b.n`
  (`a.s VARCHAR`, `b.n BIGINT`) must fail at CREATE with `Unsupported` and
  register no view (6.6b) — the end-to-end guard against silent corruption.

A focused way to force the range *merge-walk* path (large delta vs. trace) is in
`memory/range-join-strategy-selection-distribution.md`; reuse it so the residual
is exercised on both `range_per_row_seek` and `range_merge_walk` outputs.

### 7.6 Gate

`make verify` (fmt + clippy-as-errors + unit) and
`make e2e K='residual or having' WORKERS=4`.

---

## 8. Edge cases & risks

- **Residual references a join-key column** (`ON a.k=b.k AND a.k>100`): the key
  also rides in payload, so `resolve_join_col_ref(a.k)` → its left global index →
  payload column. Reads the real value. ✓
- **Numeric mixed-type / cross-sign residual comparisons** (`a.i32 > b.i64`,
  unsigned vs signed): handled by `compile_bound_expr`'s existing per-column-type
  opcode selection — identical machinery to WHERE-clause comparisons, not new
  here. Tests should include a same-width signed pair and an int-vs-literal case.
- **String/blob residuals** (`a.s <> b.s`, `a.b1 < b.b2`): `try_compile_string_cmp`
  handles the col-vs-col and col-vs-literal paths for both `STRING` and `BLOB`
  (§6.6a); `<>` lowers to `NOT (eq)`. Comparison is by content, so a test must
  cover values sharing a 4-byte prefix but differing in the heap tail.
- **String/blob × non-string residual** (`a.s > b.n`, `a.s + 1`): rejected at
  CREATE by the §6.6b `ColRef` guard with `Unsupported` — **not** silently
  miscompiled into an integer load. This is the corruption case the audit
  surfaced; §6.6 closes it in the shared compiler (so WHERE / projections gain
  the same guard).
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
| `crates/gnitz-sql/src/binder.rs` | §6.2: new `LeafBinder` trait + `bind_structural` core + `map_binop`/`bind_literal`/`fold_null_test` helpers (the existing `bind_expr` body, hoisted); `SingleTable` leaf; `Binder::bind_expr` → one-line wrapper; `UnaryOperator`/`Function` imports; add compound-ident null-test test |
| `crates/gnitz-sql/src/planner.rs` | `JoinPredicates` +residual; `extract_join_predicates`/`collect_join_predicates` collect instead of reject; `cross_table_pair`, `JoinResidual` leaf, `build_residual_filter_prog`; LEFT+residual guard; filter splice in the equi path and in `build_range_join_view` (+`residual` param); `bind_having_expr`/`bind_having_null_test` → `Having` leaf; `collect_having_aggs` gains the `BETWEEN` arm (lockstep); extraction-test helper/`range_extraction_rejections` updates |
| `crates/gnitz-sql/src/expr.rs` | §6.6: `try_compile_string_cmp` widens the two type guards to `.is_german_string()` (STRING+BLOB content comparison); `compile_bound_expr` `ColRef` arm rejects `STRING`/`BLOB` reaching the integer-load path; add blob-comparison + mixed-type-rejection unit tests |
| `crates/gnitz-sql/tests/planner_join.rs` | Flip `test_range_join_reject_two_range_conjuncts`; add positive (string/blob/BETWEEN/arith) + negative (LEFT, residual-only, LIKE, mixed string↔int) residual tests |
| `crates/gnitz-py/tests/test_join_residual.py` | New: INNER residual correctness + incremental + 3VL + string/blob content + IS NULL fold + mixed-type CREATE rejection, `WORKERS=4` |
| `crates/gnitz-py/tests/` (HAVING) | New cases: `HAVING` with `*`/`BETWEEN`/`NOT`/arithmetic now compute (drift retired); existing HAVING cases stay green |

Engine, wire, core, capi: **no changes.** The work is confined to the `gnitz-sql`
front end: one expression-binding core shared by WHERE / residual / HAVING
(§6.2), plus the `compile_bound_expr` string/blob hardening (§6.6) that also
fixes WHERE / computed-projection expressions.
