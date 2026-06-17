# Nullable SUM does not transition to NULL on last-value retraction

## Defect

`SUM` over a nullable column emits a concrete `0` instead of `NULL` when a
retraction removes the **last non-null contributor** from a group that still
survives (via `COUNT(*)` or another aggregate). SQL `SUM` of a group with no
non-null values is `NULL`; the engine yields `0`.

Root cause: the accumulator (`ops/reduce/agg.rs`) tracks presence as a saturating
boolean `has_value`, not a non-null count. The emit path writes a `SUM` column
NULL iff `is_zero()`, and `is_zero()` is exactly `!has_value` (`agg.rs:109-111`,
consumed at `emit.rs:66` and `emit.rs:240`). Every non-null `step_from_batch` and
every `merge_accumulated` fold sets `has_value = true`; only `reset()` clears it.
On the linear fast path the accumulator is never reset, so across a retraction the
*value* nets correctly (SUM is linear) but `has_value` cannot fall back to
`false`: a group whose non-null count returns to 0 keeps `has_value = true` and
emits `SUM = 0`.

A boolean cannot encode "non-null count crossed back to zero"; only a real count
can. `SUM = 0` is itself ambiguous — `SUM({5, −5}) = 0` (non-null) and
`SUM({NULL}) = NULL` are indistinguishable under `has_value`.

No new ghost rows in the target scenario: the old `SUM` was non-null and the
retraction row re-emits it non-null (the `use_old_val` branch never null-marks —
`emit.rs:60-71`), cancelling the prior output byte-for-byte; only the new insert
row carries the wrong value (`0` where `NULL` is correct).

## Reachability

The bug lives on the **linear fold** path, which folds the previous *output* value
forward without replaying inputs. It is absent wherever the reduce resets and
re-steps net-positive rows.

- **MIN/MAX — unaffected.** Non-linear; recomputed from history+delta each tick
  (replay / AVI). The replay consolidates `history + delta`, drops net-zero rows,
  `reset()`s every accumulator, then re-steps only `w > 0` rows
  (`op_reduce.rs:560-574`), so `has_value` rebuilds and correctly falls to `false`.
- **SUM alongside MIN/MAX — unaffected.** One non-linear agg makes the whole
  reduce non-linear (`all_linear == false`, `op_reduce.rs:163`), so SUM rides the
  same reset+replay. A retracted `(pk,5,+1)/(pk,5,−1)` pair nets to zero in the
  consolidation and never re-sets `has_value`.
- **All-linear SUM (SUM/COUNT only), single worker — affected.** The fast fold
  folds the old output value with `merge_accumulated(old_vals[k], 1)` and never
  resets (`op_reduce.rs:483-486`); no `trace_in` is allocated (only
  `!all_linear && !will_use_avi` gets one — `compiler.rs:1779,1803`).
- **Multi-worker GROUP BY — affected, via the same per-worker fold.**
  `reduce_multi` inserts an `ExchangeShard` on the group columns
  (`circuit.rs:465-472`, used at `planner.rs:2539`), so each group is owned wholly
  by one worker whose `op_reduce` computes the *complete* group on the same buggy
  linear fold. The downstream `ExchangeGather` only unions disjoint groups; it does
  not re-aggregate. A 4-worker run reproduces the bug on whichever worker owns the
  group — an integration check, not a distinct aggregation path.
- **Global aggregate (no GROUP BY) — affected, same fold.** Built with
  `reduce_multi_local` over a broadcast input (`circuit.rs:482-489`; e.g.
  `planner.rs:1648`): every worker computes the same complete aggregate on the
  linear fold.

There is **no partial-then-combine reduce on the SQL path.** `op_gather_reduce`
(its linear old-global fold is `op_gather.rs:101-104`) is constructed only by
engine test scaffolding (`compiler.rs:4490`, inside `#[test]`); the circuit builder
exposes no method that emits an `OpNode::GatherReduce`, and no planner path builds
one. It is unreached dead code, out of scope here.

Trigger shape: `SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k` with `v` nullable;
insert `(k, v)` and `(k, NULL)`, then retract `(k, v)`. Expected surviving row
`(k, NULL, 1)`; actual `(k, 0, 1)`.

## Reproduction

The fix lives in the SQL planner (it injects a companion aggregate and a finalize
gate), so a unit test that calls `op_reduce` directly with a bare `[Count, Sum]`
descriptor list cannot be made to pass — `op_reduce` has no extra column in which
to carry the non-null count, and that absence is the defect. Both tests below
exercise the companion + finalize together.

- **Engine level** (`ops/reduce/tests.rs`): drive `op_reduce` with
  `aggs = [Count, Sum, CountNonNull(same col)]`, a finalize `ExprProgram` that
  emits `SUM` null-gated by the companion (see Fix), and a finalize output schema
  projecting `[pk, grp, count, sum]` (companion stripped). After tick 1
  `{(pk1,5),(pk2,NULL)}` and tick 2 retract `(pk1,5)`, assert the finalized insert
  row has the `SUM` null bit set and `COUNT(*) = 1`. Pins the mechanism without the
  planner.
- **E2E** (`gnitz-py/tests/`), `GNITZ_WORKERS=1` and `=4`, on
  `SELECT k, SUM(v) AS sm, COUNT(*) AS c FROM t GROUP BY k` with `v` nullable:
  insert `(k, 5)` and `(k, NULL)`, retract `(k, 5)`; assert the surviving row has
  `sm IS NULL` and `c = 1`. The 4-worker run confirms the fix holds when the
  group's reduce is sharded onto one worker.

## Fix

The mechanism already exists — `AVG` is built from exactly these parts. Reuse them.

`AggOp::CountNonNull` is a first-class linear aggregate (`agg.rs:24,56`), maintained
by the same `step_from_batch` / `merge_accumulated` / `combine` paths as `COUNT`
(`agg.rs:135-153,234,265`) and typed `I64` by both `agg_output_type`
(`compiler.rs:967`) and `agg_result_type` (`planner.rs:2225`). `AVG(v)` already
plans as a `SUM(v) + COUNT_NON_NULL(v)` spec pair (`push_agg_specs`,
`planner.rs:2770-2775`) and derives its NULL in the fused finalize MAP:
`float_div(sum, cnt)` returns NULL when `cnt == 0` (`planner.rs:2609-2636`),
because division by zero yields NULL for both int and float
(`expr/tests/batch_tests.rs:258,298`).

For each `SUM(v)` whose source column `v` is nullable, the planner emits a hidden
companion `COUNT_NON_NULL(v)` and builds the finalize column as a *type-preserving*
null-gate in place of a plain `copy_col`. Dispatch on the source type, as AVG does
at `planner.rs:2616-2624`:

- integer source:
  `emit_col(int_div(load_col_int(sum), cmp_ne(load_col_int(cnt), load_const(0))), out_idx)`
- float source:
  `emit_col(float_div(load_col_float(sum), int_to_float(cmp_ne(load_col_int(cnt), load_const(0)))), out_idx)`

`cmp_ne` yields i64 `1`/`0` (`expr/batch.rs:427`); dividing by `1` is exact
(identity for every `i64`, and `x / 1.0` is exact) and dividing by `0` produces
NULL. So the gate keeps the SUM value when the non-null count is positive and marks
it NULL when the count is zero — distinguishing `SUM({5, −5}) = 0` from
`SUM({NULL}) = NULL`. The companion is consumed by the finalize and never
projected, exactly as AVG's count is.

This covers every affected path with **no engine change**:

- The companion is just another linear spec, so the existing fold (`op_reduce`
  linear path and the non-linear replay alike) maintains it, and the compiler's
  reduce-output schema picks it up through the shared `agg_output_type`.
- The finalize MAP is always attached after the reduce (`planner.rs:2687`) and
  fused into the reduce's inline finalize program (`compiler.rs:101,1321`). It runs
  **per group on complete aggregates** in every reachable layout — single-worker,
  group-sharded multi-worker, and broadcast global — so a count of zero is always
  the true non-null count.
- The finalize runs on **both** the retraction row (old `SUM`/old `cnt`) and the
  insert row (new `SUM`/new `cnt`) (`op_reduce.rs:471-479,588-596`). Because
  null-ness is derived purely from the exact companion on each, a re-emitted
  retraction is gated identically to the original emission and cancels — the fix
  introduces no ghost and needs no separate retraction-null handling. The raw `SUM`
  null bit (`has_value`) becomes irrelevant for nullable SUM, in both the
  stays-NULL and becomes-NULL directions.

A non-nullable-source `SUM` over a surviving (non-empty) group always has non-null
count ≥ 1, so it can never be NULL — it keeps its plain `copy_col` and gets no
companion. The companion is emitted strictly for nullable-source SUM.

Threading (sites):
- `push_agg_specs` (`planner.rs:2750`): for a nullable-source `SUM`, push
  `(AGG_SUM, c)` then `(AGG_COUNT_NON_NULL, c)` — the AVG two-spec shape. Each
  nullable SUM materializes its own companion; do not dedup against an `AVG(v)` or
  `COUNT(v)` companion (a redundant linear accumulator is negligible).
- `AggMapping`: add an `is_nullfill_sum: bool` alongside `is_avg`; set it for a
  nullable-source SUM so the projection knows to gate.
- SELECT projection (`planner.rs:2637-2653`): when `is_nullfill_sum`, build the
  gate above (honoring the `is_float` source split) and push one nullable output
  column; do not project the companion.
- HAVING runs on the raw reduce output before the finalize MAP
  (`planner.rs:2667-2685`), so it sees the raw `SUM` column. Numeric predicates
  (`SUM(v) > k`) are unaffected (`0 > k` and `NULL > k` both drop the row). Bind
  `SUM(v) IS NULL` / `IS NOT NULL` to `companion == 0` / `companion != 0` so HAVING
  agrees with the projected null-ness.

## Scope

- Correctness only; no change to SUM's value arithmetic or to which groups emit a
  row.
- Self-contained for nullable SUM: null-ness is derived entirely from the companion
  `COUNT_NON_NULL` in the finalize, covering both the all-NULL group gaining another
  NULL (stays NULL) and the last non-null being retracted (becomes NULL), without
  relying on `has_value`-based emit logic.
- Out of scope: pruning a group that loses its **last** row (a `has_value`-vs-
  `acc == 0` emit question, independent of SUM null-ness); nullable MIN/MAX null
  transitions (already handled by the non-linear replay); `op_gather_reduce`
  (unreached dead code).
