# EXPLAIN for direct SELECT: split planning from dispatch, format the access decisions

## 1. Problem

The ad-hoc read path decides an access strategy per query — a PK seek, a PK range, a
`pk IN` set gather, a selectivity-gated secondary-index range, or a full scan; a server-side
predicate or none; a projection; a server top-k or a client sort; a per-worker aggregate fold —
and today none of it is visible. A user cannot tell whether `WHERE id = 5` became a point seek
or a full scan, whether an index was a candidate, or whether ORDER BY ran server-side. `EXPLAIN`
surfaces those decisions.

Two obstacles:

1. **The planner and the data-path request are fused.** `plan_read_spec`
   (`crates/gnitz-sql/src/dml/select.rs:156`) builds the `ReadSpec` in steps 1–6 (lines 165–223)
   and then **dispatches** it via `client.scan_spec` (line 224) in the same function;
   `execute_aggregate_select` (`select.rs:329`) does the same (dispatch at line 466). There is no
   seam to "plan without executing." EXPLAIN needs one.
2. **The access decision is not reified.** The `ReadSpec`'s compiled predicate and projection are
   opaque `Vec<u8>` blobs; the human-readable facts EXPLAIN prints (how many predicate conjuncts,
   how many projected columns, which bound, the ORDER BY/LIMIT strategy) are known only
   transiently inside the planner. The planner must return them.

This plan factors both read planners into a **plan-only builder** returning a reified
`ReadAccessPlan`, drives the existing execution from that struct unchanged, and adds an `EXPLAIN`
statement arm that runs the builder and formats its `ReadAccessPlan` into rows — never issuing a
data-path request.

## 2. The plan/dispatch split

Both read planners factor into `build …_plan` (pure planning, no wire call) + a thin
execute-and-finish tail. The builders return a reified plan the executor and EXPLAIN both consume.

### 2a. The read (rows) path

Introduce:

```rust
/// The reified access decision for a rows read — everything the worker needs
/// (`spec`, `reply_schema`, `tid`) plus the two descriptive facts EXPLAIN cannot
/// recover from `spec`/`reply_schema` alone.
struct ReadAccessPlan {
    spec: ReadSpec,
    reply_schema: Schema,
    tid: u64,
    offset: usize,
    limit: Option<usize>,
    /// Conjuncts re-imposed as the server-side predicate. Not recoverable — the
    /// `spec.predicate` blob is an opaque compiled program.
    n_pred_conjuncts: usize,
    /// Non-projected source columns appended (hidden) to order the result.
    n_hidden_order_cols: usize,
}
```

The projected-column count and the index-gating flag are **not** reified — EXPLAIN derives them at
format time: the visible column count from `reply_schema.visible_columns()`, and index-gating from
`spec.bound` being `ReadBound::IndexRange` with a ≤8-byte-int range column (the same `!is_wide_int`
test `extract_bound` uses, `select.rs:288-294`). Only `n_pred_conjuncts` (opaque blob) and
`n_hidden_order_cols` genuinely need threading.

`build_read_plan(client, select, query, schema, tid, limit, offset) -> Result<Option<ReadAccessPlan>, GnitzSqlError>`
is `plan_read_spec`'s current steps 1–6 (`select.rs:165–223`) with the `client.scan_spec` call
and the client-side finish removed. It returns `Ok(None)` for a shape the read spec cannot
express, exactly as `plan_read_spec` does today. `n_pred_conjuncts` comes from `pred_exprs.len()`,
`n_hidden_order_cols` from the count `resolve_read_spec_order` appended.

`plan_read_spec` becomes: `build_read_plan(...)? ` then, on `Some(plan)`, `client.scan_spec(plan.tid,
&plan.spec.encode(), &encode_schema_block(&plan.reply_schema, plan.tid as u32))` and
`read_spec_finish`, returning `SqlResult::Rows`. Behaviour is identical to today; the only change
is that the spec-building half is now callable alone.

### 2b. The aggregate/DISTINCT (fold) path

Introduce the parallel `AggAccessPlan`:

```rust
struct AggAccessPlan {
    spec: ReadSpec,          // sink = ReadSink::Fold
    partial_schema: Schema,  // the per-worker reduce-output reply schema
    out_schema: Schema,      // the final client-facing schema
    tid: u64,
    // enough to run the client finish: layout, bound_having, offset, limit
    layout: GroupByLayout,   // carries group cols + agg specs — the group-by/distinct line reads them
    having: Option<BoundExpr>,
    offset: usize,
    limit: Option<usize>,
    is_distinct: bool,       // group-by-with-no-aggs vs DISTINCT is not inferable from an empty agg list
    n_pred_conjuncts: usize, // opaque predicate blob — the one count not recoverable from the above
}
```

`build_agg_plan(client, select, query, binder) -> Result<Option<AggAccessPlan>, GnitzSqlError>` is
`execute_aggregate_select`'s current body (`select.rs:335–461`) minus the dispatch (line 466) and
the client finish (473–487). `execute_aggregate_select` becomes: `build_agg_plan(...)?` then, on
`Some`, dispatch + `agg_finish` + `order_limit_passthrough`, returning `SqlResult::Rows`.

The `LIMIT 0` short-circuits (rows path `select.rs:203`, agg path `select.rs:439`) stay in the
executing wrappers (they return an empty `SqlResult` without a plan); EXPLAIN never sees them
because `EXPLAIN … LIMIT 0` still describes the access, so keep them out of the builders — the
builders always produce a real plan.

## 3. The EXPLAIN statement arm

New arm in `execute_statement`'s match (`crates/gnitz-sql/src/dispatch.rs:53–135`) for
`sqlparser::ast::Statement::Explain`, placed before the catch-all (`:132`).

sqlparser 0.62.0 shape (`ast/mod.rs:4602–4623`):

```rust
Statement::Explain {
    describe_alias: DescribeAlias,      // enum { Describe, Explain, Desc } (ast/mod.rs:8662)
    analyze: bool,
    verbose: bool,
    query_plan: bool,
    estimate: bool,
    statement: Box<Statement>,
    format: Option<AnalyzeFormatKind>,
    options: Option<Vec<UtilityOption>>,
}
```

- **Accepted:** `analyze == false && verbose == false && query_plan == false && estimate == false
  && format.is_none() && options.is_none()`, `describe_alias` any of the three (all of
  `EXPLAIN` / `DESCRIBE` / `DESC` `<select>` parse to `Statement::Explain` in this version —
  verified: `parse_explain` first `maybe_parse`s a full statement; a valid `SELECT` succeeds
  regardless of the introducer keyword), and `*statement` is `Statement::Query`. Anything else →
  `GnitzSqlError::Unsupported` naming what was rejected (`EXPLAIN ANALYZE`, `EXPLAIN VERBOSE`,
  `EXPLAIN (FORMAT …)`, `EXPLAIN <non-SELECT>`).
- `Statement::ExplainTable` (bare `DESC t` / `DESCRIBE t` — a table name that is not a valid
  standalone statement) stays **unmatched** → the catch-all `Unsupported`. (Table introspection is
  a separate feature.)
- The arm calls `execute_explain(client, schema_name, &query, binder)`.

### `execute_explain`

Reproduces `execute_select`'s routing — the same envelope guards, body-kind check, CTE inlining,
FROM-shape check, subquery walk, and aggregate/rows split — but calls the **plan-only** builders
instead of the executing wrappers, and formats the result rather than dispatching. To avoid two
copies of the routing drifting, factor `execute_select`'s routing into a shared
`route_select(...) -> SelectRoute` enum (`Derivation(err)` | `Rows(build_read_plan args)` |
`Agg(build_agg_plan args)` | `PlainScan(tid)`) that both `execute_select` and `execute_explain`
consume — `execute_select` runs the executing tail, `execute_explain` runs the formatting tail:

- A derivation shape (JOIN / set-op / subquery / non-pass-through CTE / derived table) returns the
  **same error `execute_select` would return for it** — the plan of a query that has no plan is its
  rejection. (EXPLAIN performs the identical routing; it just stops at the plan.)
- A rows shape → `build_read_plan(...)`; `Some(plan)` → format (§4); `None` → the same
  inexpressible-shape error `execute_select` would raise for it.
- An aggregate/DISTINCT shape → `build_agg_plan(...)`; same `Some`/`None` handling.
- A bare `SELECT *` with no WHERE/ORDER BY/LIMIT/OFFSET (the plain-scan fast path,
  `select.rs:118`) has no `ReadSpec`; EXPLAIN describes it directly as a full scan with an identity
  projection.

**Metadata lookups only, never a data-path request.** The builders call `binder.resolve`,
`client.table_indexes` (index candidacy), and `best_index_bound` — all planning-time metadata
already fetched on the normal path — but never `client.scan` / `client.scan_spec`. EXPLAIN of a
view still notes it drains pending ticks *at execution time* (a fact about the real path), but
EXPLAIN itself issues no drain.

## 4. Output

`SqlResult::Rows` (`crates/gnitz-sql/src/lib.rs:47`) with a two-column schema:

- a **hidden U64 line-number PK** — a schema must have a PK and STRING is not PK-eligible
  (`Schema::validate_parts`, `crates/gnitz-core/src/protocol/types.rs:248–262`; `is_pk_eligible`
  is an integer allow-list, `crates/gnitz-wire/src/types.rs:120`). The hidden flag is
  `ColumnDef::hidden()` (`types.rs:66`, wire `META_FLAG_HIDDEN`, `flags.rs:336`); hidden columns
  are stripped at presentation by `Schema::visible_columns()` (`types.rs:191`, applied in
  gnitz-py at `lib.rs:966`). The PK makes line order deterministic.
- `plan` (TEXT, non-null).

Weight-1 rows, one per line, in order, surfacing through gnitz-py's existing `SqlResult::Rows`
arm (`crates/gnitz-py/src/lib.rs:1728`) with **no binding changes** (EXPLAIN reuses the `Rows`
variant — no new `SqlResult` variant).

Line vocabulary (order fixed):

1. `read <table|view> <name>` — a view appends ` (drains pending ticks)`.
2. bound: `seek pk = <v>` | `pk range [<lo>, <hi>]` | `pk set (<n> keys)` |
   `index range on (<cols>) — used at runtime iff ≤ 1/16 of the local slice matches` (only when
   `index_gated`; a wide-int index range is un-gated and prints without the caveat) | `full scan`.
3. `predicate: server-side (<n> conjuncts)` when `n_pred_conjuncts > 0`, else `predicate: none`.
4. `projection: <n> columns` (+ `, <k> hidden order keys` when `n_hidden_order_cols > 0`).
5. aggregate line (fold path only): `group by (<cols>): <ops>` | `distinct (<cols>)`; append
   `; per-worker group cap <cap>` from the runtime cap constant.
6. `order/limit:` one of `server top-k <k> + client window` (ORDER BY + LIMIT) |
   `server early-stop <k>` (LIMIT, no ORDER BY) | `server sort + client merge` (ORDER BY, no
   LIMIT; append ` + client window` under OFFSET) | `client sort` (aggregate results, sorted
   client-side by `order_limit_passthrough`) | `streamed` (none of the above).

The selectivity caveat in line 2 is the honest report of a **runtime**-measured index choice:
EXPLAIN names the candidate and the gate rule, not a verdict it cannot know at plan time.

## 5. Semantics pinned

- EXPLAIN performs planning-time metadata lookups only; it never issues a data-path request. Its
  output schema (hidden U64 PK + `plan` TEXT) and its line vocabulary are the tested surface.
- The plan/dispatch split is behaviour-preserving for the executing path: `plan_read_spec` /
  `execute_aggregate_select` produce byte-identical `ReadSpec`s and identical results before and
  after the refactor.
- `EXPLAIN` accepts only the default option set; every non-default flag/format and every
  non-SELECT inner statement is `Unsupported`. `ExplainTable` (bare `DESC t`) is out of scope.
- EXPLAIN of a derivation shape returns that shape's rejection error, identical to `execute_select`.

## 6. Testing

- Refactor parity (Rust, `crates/gnitz-sql/tests/`): a handful of representative rows/aggregate
  queries assert `build_read_plan` / `build_agg_plan` produce the expected `ReadSpec` (bound
  variant, predicate empty/non-empty, sink), independent of dispatch.
- EXPLAIN e2e (`crates/gnitz-py/tests/`): seek / pk-range / pk-set / gated-index-range /
  wide-index-range / full-scan / global-aggregate / grouped-aggregate / DISTINCT / top-k / early-
  stop / server-sort lines, each asserting the exact line text. EXPLAIN over a view asserts the
  `(drains pending ticks)` note.
- EXPLAIN of a JOIN / UNION / EXISTS returns the same rejection `execute_select` returns for it
  (assert identical error text).
- `EXPLAIN INSERT …`, `EXPLAIN ANALYZE <select>`, `EXPLAIN (FORMAT JSON) <select>`, and bare
  `DESCRIBE t` are each `Unsupported`.
- `make verify` and `make e2e WORKERS=4` green.

## 7. Sequencing

- [ ] Factor `build_read_plan` + `ReadAccessPlan` out of `plan_read_spec`; rewire `plan_read_spec`
      as builder + dispatch + finish. Refactor-parity tests.
- [ ] Factor `build_agg_plan` + `AggAccessPlan` out of `execute_aggregate_select`; rewire.
- [ ] Add the `Statement::Explain` arm + `execute_explain` + line formatting + the two-column
      hidden-PK schema; EXPLAIN e2e + rejection + unsupported-option tests.
- [ ] `make verify` + `make e2e WORKERS=4`.
