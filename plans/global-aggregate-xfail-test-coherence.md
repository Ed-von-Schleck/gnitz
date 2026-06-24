# Fix the misleading global-aggregate gather-reduce xfail tests

Two `xfail(strict=True)` tests in `crates/gnitz-py/tests/test_multiworker_ops.py` claim to
pin a distributed gather-reduce retraction bug, but they actually trip on an unrelated
planner rejection and never reach the behavior they describe. They pass for the wrong reason
and give false coverage signal. This plan makes them honest: they will instead assert the
engine's real current contract (ungrouped aggregate views are rejected at plan time), and
the genuine — but unreachable — gather-reduce defect is left documented only where its dead
code lives.

This is a test/comment-coherence fix. It changes **no engine behavior** and does **not**
implement global-aggregate support (a separate, larger feature decision, explicitly out of
scope here).

## 1. The defect

`test_global_min_multiworker_retract_current_min` and
`test_global_max_multiworker_retract_current_max`
(`crates/gnitz-py/tests/test_multiworker_ops.py`, the block under the
`KNOWN BUG (expected failures)` comment) are marked
`@pytest.mark.xfail(strict=True, reason="distributed gather-reduce global MIN/MAX retraction
re-emits deleted extremum")`. Each test:

1. `CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)`
2. `CREATE VIEW v AS SELECT MIN(val) AS m FROM t`  ← **ungrouped aggregate**
3. inserts 30 rows, asserts the global MIN/MAX, deletes the current extremum, and asserts the
   view recomputes to the next-best value.

The intended failure is at step 3 (the retraction assertion). The **actual** failure is at
step 2: an ungrouped aggregate view does not compile. `ViewShape::classify`
(`crates/gnitz-sql/src/plan/view/dispatch.rs:92-136`) routes a `SELECT` with an empty
`GROUP BY` to `ViewShape::Simple`; the `Simple` view's projection is lowered as ordinary
expressions, and an aggregate has no expression-context lowering —
`OpcodeBackend::agg_call` (`crates/gnitz-sql/src/lower.rs:274-278`) unconditionally returns
`Unsupported("aggregate function not allowed in expression context")`. So `CREATE VIEW v`
raises, the `xfail(strict=True)` is satisfied by *that* exception, and the test is green —
but it never executes a single insert, delete, or gather-reduce. Verified empirically: under
`--runxfail`, both tests fail at the `CREATE VIEW` line with the expression-context message,
not at the retraction assertion.

Consequences:

- **False coverage.** The suite appears to track the gather-reduce retraction behavior; it
  tracks nothing of the sort. The `_scan_global_agg` helper (asserting one global row →
  `m`) is never reached.
- **Inaccurate documentation.** The `KNOWN BUG` comment states "A GLOBAL aggregate (no GROUP
  BY) ... is the broken case ... `op_gather_reduce` ... re-emits the deleted value instead of
  the next-best." That is a true statement about `op_gather_reduce` as *code*, but it is
  presented as a reachable engine bug. It is not reachable: `op_gather_reduce` is **unwired in
  SQL planning** — the SQL/core layers never emit a `GatherReduce` node — so no SQL query
  reaches it. The accurate status is already recorded at
  `crates/gnitz-engine/src/ops/reduce/tests.rs:6651-6652` ("Latent today (op_gather_reduce is
  unwired in SQL planning) but a real value bug for the future GatherReduce milestone").
- **Latent maintenance trap.** When a future change touches either the planner rejection or
  the GatherReduce wiring, these `strict=True` tests will flip in a confusing way (they may
  "unexpectedly pass" because the *rejection* path changed, with the gather-reduce bug still
  unfixed), pointing a maintainer at the wrong subsystem.

## 2. The genuine underlying defect (left as-is, documented only)

`op_gather_reduce` (`crates/gnitz-engine/src/ops/reduce/op_gather.rs`) combines per-worker
partials for a single logical group. For a negative-weight partial it does:

```rust
if w > 0 {
    accs[k].combine(bits);
} else if w < 0 && accs[k].is_linear() {
    accs[k].merge_accumulated(bits, -1);
}
```

(`op_gather.rs`, the per-group accumulation loop). For a **non-linear** aggregate (MIN/MAX,
`is_linear() == false`) a negative-weight partial takes neither branch and is dropped: the
operator has only the previous global result in `trace_out` and no per-worker input history,
so it cannot recover the next-best extremum and re-emits the retracted value. This is a real
defect **in dead code** — correct to leave unfixed today, mandatory to fix before any future
milestone wires `GatherReduce` for non-linear aggregates over retractable sources. The
existing comments at `op_gather.rs` (the NULL-partial / negative-weight rationale) and
`tests.rs:6651-6652` already capture this accurately and are the canonical record; this plan
adds nothing to the engine and removes nothing from those comments.

## 3. Decided fix

Convert the two tests from "expected-fail retraction tests" into **planner-rejection
contract tests**, and delete the inaccurate `KNOWN BUG` framing. Converting (rather than
deleting) preserves a regression guard for the real current contract — *ungrouped aggregate
views are unsupported* — which is otherwise untested.

In `crates/gnitz-py/tests/test_multiworker_ops.py`:

1. **Replace** the two `@_NEEDS_MULTI @pytest.mark.xfail(strict=True, ...)` tests with two
   plain tests (no `xfail`, no `_NEEDS_MULTI` — the rejection is worker-count-independent and
   happens at plan time) that assert `CREATE VIEW` over an ungrouped aggregate is rejected.
   Cover both MIN and MAX, and add SUM and `COUNT(*)` to document that the rejection is
   total across aggregate kinds, not MIN/MAX-specific:

   ```python
   import pytest
   from _native import GnitzError  # the error type raised by execute_sql on rejection

   @pytest.mark.parametrize("agg", ["MIN(val)", "MAX(val)", "SUM(val)", "COUNT(*)"])
   def test_ungrouped_aggregate_view_rejected(client, agg):
       """An aggregate view with no GROUP BY does not compile: the projection is
       lowered as an ordinary expression and aggregates have no expression-context
       lowering (gnitz-sql lower.rs OpcodeBackend::agg_call). This pins the current
       contract; if ungrouped aggregate views are ever supported, this test changes
       together with the planner and op_gather_reduce (op_gather.rs negative-weight
       drop) in one feature change."""
       sn = "uagg_" + _uid()
       client.create_schema(sn)
       try:
           client.execute_sql(
               "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
               schema_name=sn,
           )
           with pytest.raises(GnitzError, match="aggregate function not allowed in expression context"):
               client.execute_sql(
                   f"CREATE VIEW v AS SELECT {agg} AS m FROM t",
                   schema_name=sn,
               )
       finally:
           _drop_all(client, sn, views=[], tables=["t"])
   ```

   (Confirm the imported exception type and `match` string against a live run before
   committing — use the type `client.execute_sql` actually raises on a planner `Unsupported`
   error, and the exact message from `crates/gnitz-sql/src/lower.rs:276`. The `_drop_all`
   call passes `views=[]` because the view was never created.)

2. **Delete** the `KNOWN BUG (expected failures)` comment block and the now-unused
   `_scan_global_agg` helper (it has no other callers — verify with a grep before removing).

3. **Leave** `op_gather.rs` and `tests.rs:6651-6652` untouched: they correctly document the
   latent defect where the dead code lives. Optionally add a one-line cross-reference in
   `op_gather.rs`'s negative-weight comment noting that the SQL surface forbids ungrouped
   aggregates today (so the drop is unreachable), inlined — no reference to this plan file.

## 4. Out of scope (stated to bound the change)

- **Implementing ungrouped aggregate views.** If desired, that is a separate feature: it
  requires the planner to recognize a top-level aggregate projection without `GROUP BY` (a
  single-group reduce), route it through `reduce`/gather, and — for distributed non-linear
  aggregates — fix the `op_gather.rs` negative-weight drop (e.g. funnel the single logical
  group to one worker via the empty-key `ExchangeShard`, so the existing single-node
  `op_reduce` + `AggValueIndex` handles retraction). That feature plan would replace the
  rejection test above with the retraction tests these two tests *meant* to be. Not done here.
- **Any engine change.** This plan edits only `test_multiworker_ops.py` (and at most one
  inline comment in `op_gather.rs`).

## 5. Acceptance criteria

1. `cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_multiworker_ops.py -k
   "ungrouped_aggregate" -v` passes; the four parametrizations (MIN/MAX/SUM/COUNT) each assert
   the `CREATE VIEW` rejection with the expression-context message.
2. `grep -n "xfail" tests/test_multiworker_ops.py` shows no `xfail` referencing global
   aggregate / gather-reduce retraction; the `KNOWN BUG` comment and `_scan_global_agg` are
   gone.
3. `make rust-engine-test` (or the engine test target) still green — `op_gather.rs` /
   `tests.rs` were not behaviorally changed.
4. The full `test_multiworker_ops.py` suite passes at W=1 and W=4 with `xfail_strict=true`
   still in effect (no stray strict-xfail now passing for the wrong reason).
