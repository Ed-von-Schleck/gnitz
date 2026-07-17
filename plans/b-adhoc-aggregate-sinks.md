# Ad-hoc aggregation sinks: GROUP BY / global aggregates / DISTINCT without circuits

## 1. Problem

A single-table aggregate SELECT (`SELECT status, COUNT(*) FROM t GROUP BY status`,
`SELECT MAX(ts) FROM t`, `SELECT DISTINCT city FROM users`) runs today as a **transient DBSP
circuit**: the engine compiles the same circuit a CREATE VIEW would, drives it once over the full
committed base, streams the result, and discards all state
(`execute_select_via_executor`, `crates/gnitz-sql/src/dml/select.rs:250-278`). Per query this
builds and throws away the reduce's operator state (up to three tables per reduce —
`_reduce_*`, `_reduce_in_*`, `_avidx_*`), pays a full `ExchangeShard` of every filtered row by
group key, and for a **global** `MIN`/`MAX` disables the two-phase fast path
(`two_phase` gate, `crates/gnitz-sql/src/plan/view/group_by.rs:438-478`) so every row of the table
ships over IPC to one worker for a single-threaded fold. `SELECT DISTINCT` additionally never
receives an index bound (`crates/gnitz-sql/src/plan/view/dispatch.rs:111-114`). Nothing is cached
between two identical queries.

One-shot aggregation needs none of that. It is a stateless, single-pass, per-worker
**hash-fold** over the scanned rows — no history, no retraction index, no exchange — with the
≤ N-groups partials merged client-side. This plan adds that fold as a new sink on the
parameterized scan (`ReadSpec`, `crates/gnitz-wire/src/read_spec.rs`): WHERE bounds and
server-side predicates compose with it unchanged, and single-relation GROUP BY / global
aggregates / HAVING / DISTINCT leave the transient executor. After this plan the executor serves
only join-shaped queries (joins, set-ops, EXISTS/IN, mark/scalar subqueries).

Out of scope (unchanged): aggregates over a JOIN (multi-segment, rejected ad-hoc today —
unchanged), set operations, `COUNT(DISTINCT …)`/`FILTER`/`OVER` (rejected at bind today —
unchanged, `crates/gnitz-sql/src/bind/structural.rs:361-363,604-614`), aggregate arguments that
are not bare columns and GROUP BY expressions (rejected at plan today — unchanged,
`group_by.rs:667-675,210-221`). CREATE VIEW's aggregation planning is untouched.

## 2. Design overview

```
SQL layer                          wire                     one worker
─────────                          ────                     ──────────
plan_read_spec(select):            ReadSpec v2:             merged cursor from bound
  bound + predicate (as before)      + aggregate section    loop drain_chunk:
  aggregate {group_cols, aggs} —     (order empty,            op_filter(predicate)
  the aggs list is the view          limit_k = 0,             hash-fold (in ops, one
  planner's own reduce layout        projection empty)          pub(crate) entry):
  DISTINCT ⇒ aggs = []                                          group_key_hash →
                                                                [Accumulator; n_aggs]
client: merge W partial runs                                 cap: > GROUP_CAP distinct
by (hash, group values), combine                               groups → STATUS_ERROR frame
accumulators, ground row,                                    emit partials: [hash | group
AVG divide, HAVING filter,                                     cols | acc cols], one row
SELECT-order projection,                                       per group
ORDER BY/LIMIT (bounded)
```

The worker fold reuses the engine's existing per-row aggregation kernel unchanged
(`Accumulator::step_from_batch(&mut self, mb: &MemBatch, row: usize, weight: i64)`,
`crates/gnitz-engine/src/ops/reduce/agg.rs:181-256`) — the same code that gives views their
weight semantics (`COUNT = Σw`, `SUM = Σ w·x` wrapping, `MIN`/`MAX` presence-based, NULL-skipping
for `COUNT(col)`/`SUM`/`MIN`/`MAX`). The kernel and `extract_group_key` are `pub(super)` inside
`ops::reduce` / `ops::util`, so the fold itself lives in **ops** (new module `ops/agg_fold.rs`)
exposing one `pub(crate)` entry the catalog-layer `scan_spec_family` calls — no visibility bumps
scattered across reduce internals. Semantics parity with a CREATE VIEW of the same query is
structural, not reimplemented.

## 3. Wire: `ReadSpec` version 2

The `ReadSpec` format version bumps to 2, adding one optional trailing section (client and engine
change in the same tree; version-1 decoding is removed — pre-alpha, no compatibility concerns):

```
[aggregate] u8 present (0/1); if 1:
            u16 n_group_cols; n_group_cols × u16 src_col        (source-schema indices)
            u8  n_aggs;       n_aggs × { u8 op, u16 src_col }   (op = wire AggFunc code;
                                                                 src_col = 0xFFFF for COUNT(*))
```

```rust
pub struct AggSpec {
    pub group_cols: Vec<u16>,
    pub aggs: Vec<AggItem>,
}
pub struct AggItem { pub op: u8, pub src_col: u16 }   // op ∈ wire AggFunc codes 1..=6
pub const MAX_AGG_GROUP_COLS: usize = 8;
pub const MAX_AGG_ITEMS: usize = 16;
```

The `aggs` list is **not** the SELECT list — it is the same physical reduce layout the view
planner builds via `push_agg_specs` (`group_by.rs:708-798`), produced by the same (extracted)
code: AVG contributes `[Sum, CountNonNull]` (`AggShape::Avg`), a nullable SUM carries its
`CountNonNull` companion (`NullfillSum`), HAVING-only aggregates append items
(`collect_having_aggs`, `group_by.rs:845-872`), and the client keeps the resulting `AggMapping`
to finish. This is what makes the view's HAVING binder reusable verbatim (§5b.4). The wire→engine
op mapping exists (`impl From<gnitz_wire::AggFunc> for AggOp`, `agg.rs:29-40`).

Decode-time constraints (rejected otherwise): when the aggregate section is present, `order` must
be empty, `limit_k` must be 0, and `projection` must be empty — the worker builds partial rows
from the aggregate spec alone; SQL-level ORDER BY/LIMIT/HAVING/AVG/projection-ordering are
client-side over the bounded merged groups. `bound` and `predicate` compose freely. `DISTINCT` is
exactly `aggs = []` — it *is* GROUP BY over the projected columns. Decode validates structure,
caps, and the `0xFFFF` sentinel only; column-index schema membership is worker-validated (the
same posture as the spec's expr programs). A SELECT needing more than `MAX_AGG_ITEMS` physical
items or `MAX_AGG_GROUP_COLS` group columns is a clean SQL-layer `Unsupported` — a
(works → error) change from the executor path, accepted and stated.

The group cap is engine-side: `ADHOC_GROUP_CAP = 65_536` distinct groups **per worker**,
overridable via `GNITZ_ADHOC_GROUP_CAP` (the `GNITZ_DDL_SCAN_CHUNK_ROWS` bootstrap pattern,
`catalog/bootstrap.rs:62-66`). Exceeding it mid-fold aborts the request with the error
`"GROUP BY exceeds {cap} distinct groups for ad-hoc execution; CREATE VIEW to maintain this
aggregation incrementally"`. Because the cap is per-worker, whether a borderline query errors
depends on worker count and data distribution — a resource-exhaustion posture, stated, not hidden.

## 4. Worker execution: the hash-fold sink (`ops/agg_fold.rs`)

Inside `scan_spec_family`'s chunk loop (bound → cursor → `drain_chunk` → `op_filter`), an
aggregate spec routes each surviving chunk into the fold entry
(`ops::agg_fold::fold_chunk(state, &batch.as_mem_batch(), …)`; `Batch::as_mem_batch` at
`storage/repr/batch.rs:895`):

1. **Group key**: `extract_group_key(src, row, schema, group_cols)`
   (`ops/util.rs:347`, u128) — the same content-hash fold the incremental reduce uses for
   `SyntheticFold` grouping, hashing the group columns' values *and* null bits, so a NULL key
   forms its own distinct group exactly as in views (pinned by
   `test_extract_group_key_null_distinct_from_zero`, `ops/reduce/tests.rs:1786`). The global
   case (`group_cols = []`) uses `global_group_key()` (`ops/util.rs:227`) directly —
   `extract_group_key` with an empty column list is documented panicking on empty batches.
2. **Fold state**: `HashMap<u128, GroupEntry>`; `GroupEntry` holds the group columns' values
   (copied from the first row of the group) and one `Accumulator` per `AggItem`
   (`Accumulator::new(&AggDescriptor { col_idx, agg_op, col_type_code }, loc)`, `agg.rs:93`;
   locator via `SchemaDescriptor::locate`, `schema.rs:633`). **On a hash hit, the entry's stored
   group values are compared to the row's** — a mismatch (u128 collision) allocates a chained
   entry rather than merging two groups, matching the view path, which segments groups by actual
   column values (`compare_by_group_cols`, `op_reduce.rs:85-89`) and never trusts the hash
   alone. Each surviving row steps every accumulator via `step_from_batch(mb, row, weight)`.
   Weight ≥ 1 per live row (`debug_assert`), so `COUNT(*) = Σw` counts logical rows — on a
   UNION ALL view source a weight-2 row counts twice, exactly as a view-over-view aggregation
   would.
3. **Cap**: inserting a new group beyond `ADHOC_GROUP_CAP` aborts the request; the worker emits
   the §3 error as a **scan-train `STATUS_ERROR` frame on the scan request id** — the existing,
   tested error path: `parse_train_header` is status-gated (`master/mod.rs:324-362`),
   `drain_scan_train` checks before forwarding, the master surfaces `send_error` to the client,
   and the client's per-frame `check_response` discards any partials already received (pinned by
   the worker-error scan test at `unique_filter.rs:802-813`). Zero new protocol machinery.
4. **Partial emission**: one reply batch in the **SyntheticFold reduce layout** —
   `[_agg_pk: U128 hidden (group hash, PK region)] + group columns (source types) + one partial
   column per AggItem` — partial types per the shared rule `agg_output_type(func, src_tc)`
   (`crates/gnitz-wire/src/circuit.rs:152-176`): counts → I64; SUM → F64/U64/I64 by source;
   MIN/MAX → source type (floats widen to F64; the accumulator stores `f64::to_bits` even for
   F32 sources, `agg.rs:340-355`). A partial with `has_value == false` (all-NULL group input for
   SUM/MIN/MAX) is NULL; count partials render 0 (`AggOp::empty_renders_zero`, `agg.rs:47-53`).
   The reply schema block is client-built and echoed, as established for spec replies; rows are
   unordered.

The fold is exact for the same reason `op_reduce` degenerates to a pure fold on an empty trace:
with no history there is no retraction arithmetic — `Accumulator` over consolidated,
positive-weight scan rows *is* the aggregate, and the incremental path applies no per-group
post-processing beyond emission gating that a ≥1-positive-row group always passes
(`should_emit`, `op_reduce.rs:666-678`). A differential test pins this (§7).

## 5. SQL layer

### 5a. Routing and validation

`execute_select`'s shape routing sends to the aggregate read path every query that is
single-relation (no JOIN in FROM) and: has GROUP BY, or a bare aggregate projection (global), or
HAVING, or `SELECT DISTINCT`. Aggregates/DISTINCT over a JOIN keep routing to the executor and
keep its multi-segment rejection. The clause gate is `reject_unhonored_select_clauses` with
`HonoredClauses { where_filter: true, grouping, distinct }` set per query (grouping for
GROUP BY/HAVING/aggregates, distinct for DISTINCT) — PREWHERE/TOP/QUALIFY/DISTINCT ON rejection
is unconditional in the gate, so parity with both current paths
(`group_by.rs:175-183`, `set_op.rs:69-77`) is preserved.

Validation and layout are **shared, not duplicated** — extracted from the view planner into
helpers consumed by both `emit_group_by_pieces` and `plan_read_spec` (refactor, no behavior
change; all the donor code is free functions or inline blocks in a free function, no planner
state):

- `agg_arg_col` (`group_by.rs:667-675`): aggregate argument must be a bare `ColRef`.
- The type gates carved out of `push_agg_specs` (`group_by.rs:727-748`): SUM/AVG on non-wide
  integer/float; MIN/MAX on `is_min_max_orderable`.
- `push_agg_specs` + `append_agg_mapping` themselves (`group_by.rs:708-837`): produce the wire
  `AggItem` layout and the `AggMapping` the client finisher consumes — AVG split, NullfillSum
  companions, HAVING-only items — one construction site for views and ad-hoc alike.
- The projection mixing rule and GROUP-BY bare-column check, carved out of the inline loop
  bodies (`group_by.rs:210-221,249-291`).
- **Output naming** via the same rule the planner uses (`group_by.rs:263-274`): SELECT alias,
  else `_count{idx}`/`_sum{idx}`/`_min{idx}`/`_max{idx}`/`_avg{idx}` by position — the ad-hoc
  result's column names equal the equivalent view's.
- **DISTINCT gates** shared with `resolve_set_projection` (`set_op.rs:139-194`): bare column
  items only, float projection keys rejected (`reject_float_keys`), wildcard expands to visible
  columns only — `SELECT DISTINCT price` (F64) errors ad-hoc exactly as the view path does.
- HAVING binding via `bind_having_expr` + `collect_having_aggs` + `resolve_having_mapping`
  (`group_by.rs:845-1047`), see §5b.4.
- `agg_result_type` (`group_by.rs:82-96`) and `direct_agg_nullable` (`group_by.rs:114-121`,
  gets a `pub(crate)` bump) for the finished schema.

`AVG` is client-lowered exactly as the view planner lowers it: the wire spec carries
`[Sum, CountNonNull]`; the client computes the F64 divide, NULL when the count is 0
(`group_by.rs:562-565`).

### 5b. Client finishing (`crates/gnitz-sql/src/exec/agg_finish.rs`, new)

Input: the per-worker partial runs from `recv_scan_spec` (unordered rows in the SyntheticFold
layout, ≤ cap groups per worker) plus the `AggMapping`. Steps, in order:

1. **Combine** across runs by `_agg_pk` hash **with group-column value verification** (same
   collision posture as the worker fold; mismatched values stay separate groups). Typed,
   engine-equivalent combination: counts and integer sums wrapping-add on the 8-byte images
   (signedness-agnostic), float sums add; MIN/MAX take the extreme under the engine's
   **total order** — `encode_ordered` semantics (`ops/util.rs:155-172`): floats via `total_cmp`
   (positive NaN is the greatest F64, pinned by `max_uses_total_order_for_nan`,
   `agg.rs:520-529`), U64 unsigned; NULL partials skip.
2. **Global ground row**: `group_cols = []` and zero merged groups → synthesize the SQL scalar
   row — count-family columns 0, SUM/MIN/MAX/AVG NULL — mirroring `emit_global_ground`
   (`crates/gnitz-engine/src/ops/reduce/emit.rs:81-125`). This covers both the empty table and
   the everything-filtered case (the view's `n == 0` seed, `op_reduce.rs:162-191`). Grouped
   queries synthesize nothing. Workers with zero groups send no partial rows, so no collision
   with the synthesized row is possible.
3. **AVG / NullfillSum finishing** per the `AggMapping`: AVG = SUM partial ÷ COUNT_NON_NULL
   partial (F64; NULL on count 0); a nullable SUM renders NULL when its companion count is 0 —
   for a one-shot fold, companion `== 0` and `has_value == false` coincide, so either view is
   consistent; the companion column is what HAVING binds against.
4. **HAVING**: bind with the existing `Having` leaf binder over a `HavingCtx` describing the
   merged-partials layout — `out_key = SyntheticFold`, `agg_col_offset = 1 + n_group_cols`, the
   shared `AggMapping` — which is precisely the reduce-output layout `bind_having_expr` already
   targets (`group_by.rs:917-1047`); its output contains **no aggregate nodes** (AVG is lowered
   to `(sum * 1.0) / cnt`, `IS NULL` to companion tests at bind). Evaluate the bound expression
   per group with the client evaluator, **extended in this plan from its integer-only domain to
   a typed Int/Float/NULL domain with engine semantics** (float comparisons via `total_cmp`
   ordering, int→float promotion, float divide, integer div-by-zero → NULL): today
   `eval_expr` rejects F64 columns and float literals outright
   (`crates/gnitz-sql/src/exec/eval.rs:56-67,92-96`), which would make every float-aggregate
   HAVING — including all AVG HAVING — unexecutable. Group count is ≤ W × cap, bounded.
5. **Projection to SELECT order**: group columns and finished aggregate results arranged per the
   SELECT list via the `AggMapping`; output names per §5a; output types per `agg_result_type`;
   global SUM/MIN/MAX nullable, grouped not (`direct_agg_nullable`) — identical schema to the
   equivalent view.
6. **ORDER BY / OFFSET / LIMIT**: the retained client-side sort + window
   (`order_limit_passthrough`, `exec/order.rs:350`) over the bounded group rows. Its resolver
   accepts output aliases and 1-based positions only (`order_target`, `order.rs:168-184`) — so
   `SELECT COUNT(*) AS c … ORDER BY c DESC LIMIT 10` works, while the bare function form
   `ORDER BY COUNT(*)` stays rejected, exactly as on today's executor path (parity, pinned).
7. **DISTINCT** (`aggs = []`): steps 2-5 degenerate to dedup; output rows are the distinct
   visible combinations, weight 1, presentation identical to scanning a DISTINCT view (hidden
   `_distinct_pk` convention, `set_op.rs:426-435`).

### 5c. What changes, what does not

- The transient executor no longer receives single-relation aggregate/DISTINCT queries; its
  code (`emit_group_by_pieces`, `emit_distinct_pieces`, `emit_bounded_group_by`) is untouched —
  CREATE VIEW is its remaining consumer for these shapes, plus ad-hoc join-shaped queries until
  they are addressed separately.
- No new engine operator state, tables, exchanges, or locks: the fold is a request-scoped
  in-RAM map inside the read handler's call tree.
- `SELECT MAX(ts) FROM t` ad-hoc becomes an embarrassingly parallel per-worker fold + W-value
  client merge — the global MIN/MAX single-worker funnel is a view-path (incremental) artifact
  that one-shot execution simply does not have.

## 6. Semantics pinned (decisions, not options)

- **Exact parity targets** with a CREATE VIEW of the same statement: output column names, types,
  nullability (global vs grouped), NULL groups (distinct group), empty-input global row
  (COUNT 0 / others NULL), weight accounting (Σw / Σ w·x / presence), integer SUM wrapping,
  U64-source SUM staying U64, MIN/MAX preserving source type (floats widen to F64), MIN/MAX
  NaN-greatest total order, `COUNT(col)` NULL-skipping, HAVING filtering the grouped relation
  (filter/map commute with the post-projection — the view's own note, `group_by.rs:618-623`),
  DISTINCT float-key rejection.
- **One accepted divergence**: cross-worker **float SUM/AVG addition order** differs from the
  view's fold order, so results can differ in the last ulp. SQL defines no float addition
  order; the view path itself excludes float SUM from its own two-phase optimization for
  related reasons (`group_by.rs:441-444`). The parity e2e uses integer data for exact equality
  and a float case with approximate comparison.
- **Loud errors, never silent degradation**: the per-worker group cap (deployment-dependent
  firing, stated in the message), and the `MAX_AGG_ITEMS`/`MAX_AGG_GROUP_COLS` spec caps
  (works → error vs the executor for >16 physical agg items / >8 group columns; clean
  `Unsupported` naming the limit).
- **`ORDER BY <aggregate-fn>` stays rejected** (alias/position forms work) — parity with today.
- **Snapshot semantics**: identical to plan-a spec scans (per-worker cursors behind the same SAL
  prefix; view sources drain pending ticks first).
- **No result caching**: repeated identical aggregate queries re-fold; the repeated-query answer
  remains a view.

## 7. Testing

Rust (engine):
- **Differential fold parity**: for randomized batches (weights ≥ 1, NULLs, every agg op, NULL
  group keys, empty input), the hash-fold's merged output equals driving the same rows through
  the view-style reduce (`op_reduce` against empty traces) — the load-bearing parity proof.
- Collision chaining: two synthetic entries forced onto one hash → separate groups end-to-end
  (fold and combine).
- Cap: cap+1 distinct groups → `STATUS_ERROR` train frame, master surfaces the error, worker
  continues serving subsequent requests.
- Partial semantics: all-NULL group SUM/MIN/MAX partial NULL; count partials 0; U64 SUM partial
  type; wrapping SUM; F32-source MIN partial is F64.
- Combine: MIN/MAX across partials with NaN (NaN wins MAX), U64 partials ≥ 2^63 (unsigned),
  NULL-partial skip.
- Spec decode: aggregate-section structural constraints (order/limit/projection absent), caps,
  COUNT(*) sentinel; worker-side col-index validation errors cleanly.
- Extended evaluator: float compare/promotion/divide semantics agree with the engine expr VM on
  a differential case set (incl. div-by-zero → NULL, `total_cmp` ordering).
E2E (`GNITZ_WORKERS=4`, new `test_adhoc_aggregates.py`):
- Parity: for each of `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG` — global and grouped,
  with and without WHERE (bound and non-bound), HAVING including `HAVING AVG(x) > 5` and an
  aggregate only in HAVING, GROUP BY a nullable column with NULL rows present,
  `SELECT DISTINCT` — the ad-hoc result equals `CREATE VIEW v AS <same query>` + scan of `v`
  (exact for integer data; approx for the float case), including column names.
- Empty table: global aggregates return the single ground row; grouped return zero rows; a WHERE
  filtering out every row behaves identically.
- `COUNT(*)` over a UNION ALL view whose rows carry weight 2 counts logical rows.
- Group cap: exceeding `GNITZ_ADHOC_GROUP_CAP` (set low via env) yields the CREATE VIEW error.
- `SELECT status, COUNT(*) AS c FROM t GROUP BY status ORDER BY c DESC LIMIT 3`; bare
  `ORDER BY COUNT(*)` errors.
- `SELECT DISTINCT float_col` errors (parity with the view path).
- `make verify` and `make e2e WORKERS=4` green.

## 8. Sequencing

- [ ] gnitz-wire: `ReadSpec` v2 aggregate section + decode constraints + caps + unit tests.
- [ ] gnitz-sql (refactor, no behavior change): extract the shared helpers from `group_by.rs` /
      `set_op.rs` — `agg_arg_col`, type gates, mixing + GROUP-BY-column checks,
      `push_agg_specs`/`append_agg_mapping`, output naming, DISTINCT projection gates, HAVING
      binding entries — with `emit_group_by_pieces`/`emit_distinct_pieces` as the first
      consumers.
- [ ] gnitz-sql: extend the client evaluator (`eval_expr`) to the typed Int/Float/NULL domain
      with engine semantics + differential tests (consumed immediately by existing DML SET
      paths; required by §5b.4).
- [ ] engine: `ops/agg_fold.rs` — the `pub(crate)` fold entry (extract_group_key +
      `Accumulator` + `AggDescriptor` construction, collision chaining, cap +
      `STATUS_ERROR`-frame abort, SyntheticFold-layout partial emission), wired into
      `scan_spec_family`; differential parity test + unit tests.
- [ ] gnitz-sql: aggregate arm of `plan_read_spec` (routing, wire spec via the shared layout
      builder, HAVING items), `exec/agg_finish.rs` (typed combine, ground row, AVG/NullfillSum,
      `HavingCtx` binding + evaluation, projection/naming, order/window), reroute
      single-relation aggregates/DISTINCT off the executor.
- [ ] e2e: `test_adhoc_aggregates.py`; full suite; `make verify`.
