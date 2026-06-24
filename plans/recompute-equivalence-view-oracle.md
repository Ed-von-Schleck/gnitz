# Recompute-equivalence view oracle (debug-gated, float-aggregate carve-out)

A debug-gated facility that, at a quiescent point, asserts every materialized view's
stored state equals a from-scratch recomputation of that view from current base-table
state, compared as a weighted `(PK, payload) → weight` multiset. It turns silent
divergence between the incremental-maintenance path and a clean rebuild — the failure
mode behind both the boot under-fill and the live-create double-count — into a loud,
weight-exact abort with a per-row diff. Float `SUM`/`AVG` output columns are excluded
from the **value** comparison by design (see *Float-aggregate carve-out*); everything
else is compared exactly.

## What it asserts, and what it does not

For each user view `v`, at quiescence:

```
consolidate(live_store(v))  ==  consolidate(recompute(v, current_base_state))
```

as a multiset over `(PK_opk_bytes, payload, weight)`, with float `SUM`/`AVG`-derived
columns compared structurally (presence + cardinality) rather than by value.

This is a **consistency** check, not an independent **correctness** check. The recompute
drives the same compiled circuit and the same operators as the live path, so it catches
any *divergence between incremental maintenance and a clean rebuild* (under-count,
over-count, misplacement, drop), in either direction — but it cannot catch a bug that
both paths commit identically inside a shared operator (e.g. a wrong join kernel). An
independent, non-DBSP reference evaluator would be required for that and is **not** part
of this facility. Stating the bound precisely is load-bearing: a green run means "the
maintained state matches a clean rebuild," not "the view is semantically correct."

The two motivating bugs are both divergences this facility catches:
- boot under-fill: live (correct) vs a rebuild path that dropped rows → mismatch.
- live-create double-count: a live view at inflated weights vs a clean rebuild at weight
  1 → mismatch.

## Float-aggregate carve-out (the committed decision)

`SUM`/`AVG` over a float (`F32`/`F64`) source is a **non-associative IEEE-754 fold**:
the live path accumulates `new = old + delta` per tick in arrival order
(`reduce/agg.rs` linear fold; `group_by.rs:490` float SUM accumulator), while a
recompute folds one PK-sorted batch. Measured, with `SUM(fval)` over `{1.0, 1e17,
-1e17}` arriving across three ticks: live `= 1.0`, restart-rebuilt `= 0.0` — both correct
sums of the same multiset, differing only in association. The same base state therefore
maps to different view values, so weight-exact recompute-equivalence is **false** for
these columns by mathematics, not by bug.

Committed decision: **do not** canonicalize the fold to force determinism. Instead, the
oracle excludes float `SUM`/`AVG`-derived columns from its value comparison. Concretely:

- **Excluded from value comparison:** any float-typed output column whose circuit lineage
  includes a `SUM` or `AVG` aggregate over a float source. `AVG` is `SUM/COUNT` in a post
  map (`group_by.rs:493-501`), so it inherits the non-determinism. A projection or
  expression downstream of such an aggregate inherits it too — exclusion must follow the
  taint.
  - Implementation rule: walk the view's circuit; mark the output column of every
    `AggFunc::Sum`/`AggFunc::Avg` node whose argument column is `F32`/`F64`, then
    propagate the taint forward through `Map`/expression nodes to every output column
    derived from it. Conservative fallback (safe, less coverage): exclude **all**
    float-typed output columns of any view whose circuit contains a float `SUM`/`AVG`.
- **NOT excluded:** `MIN`/`MAX` over floats (an order-independent selection — the maximum
  of a set is a function of the set, and incremental maintenance always yields the true
  current extremum), `COUNT`/integer `SUM` (associative — exact), and per-row float
  projections/expressions (deterministic per row, no cross-row fold). These stay in the
  exact value comparison. The oracle's own tests (below) guard this classification: if a
  float `MIN`/`MAX` view false-positives, the taint set is wrong and widens.
- **What is still checked for an excluded column's view:** the group-key set, row
  cardinality, weights, and all non-excluded columns must still match exactly. Only the
  tainted float value bytes are skipped.

## Design

### Comparison core (the shared primitive)

1. **Consolidate both sides** through the engine's own consolidation
   (`merge_runs_to_consolidated` / `ArenaZSetBatch::to_consolidated`) before comparing.
   Views can hold unconsolidated deltas; comparing unconsolidated-vs-consolidated would
   false-mismatch. Post-consolidation, `(PK, payload)` is unique → a canonical normal
   form.
2. **Compare with the engine's own identity.** PK by `compare_pk_bytes` over the OPK
   region (a bijection — byte-equality is key-equality); payload by `compare_rows`
   (German-string comparison, `total_cmp` for floats, null-before-non-null). Reuse these
   so the oracle's notion of "equal" is exactly the engine's; a naïve byte compare
   false-mismatches on representation differences (`-0.0`/`+0.0`, NaN bit patterns,
   inlined-vs-relocated strings).
3. **Per-worker, then gathered.** A view is partitioned (join/group key); routing is a
   pure function of the OPK bytes (the hash invariant), so the recompute lands each row
   on the same worker as the live path. Compare **per worker** (localizes a mismatch to
   a partition) and report the gathered multiset as the top-level verdict.
4. **Excluded columns** (per the carve-out): drop the tainted float bytes from the
   payload tuple before hashing/comparing; still compare PK, weight, group keys, and
   untainted columns.
5. **Cheap path:** reduce each side to an XXH3 fingerprint (`foundation/xxh`) over the
   sorted `(PK, payload-minus-excluded, weight)` stream — O(1) comparison after an O(n)
   hash, fingerprints storable as golden values. Compute the full per-row diff only when
   fingerprints differ.

### Recompute (the reference state)

Recompute `v` by driving each of `v`'s transitive base tables' **current committed
state** through `v`'s compiled circuit **from empty**, in the boot-correct order (each
source driven once; a join's sides driven so the first sees an empty opposite trace and
the second sees a full one), writing output into a **scratch ephemeral store** without
touching the live view. This is the same drive `fan_out_backfill` performs at boot — a
clean rebuild — **not** the live `CREATE VIEW` inline backfill path (which has its own
redundancy with pending ticks). Reusing the boot-correct drive is deliberate: it makes
the reference a clean rebuild and the check a consistency check against it.

The recompute reads each base's committed store via the existing source-cursor path and
feeds chunks through the view's circuit into the scratch store; the live view store is
read-only during the check.

### Quiescence

The check is only meaningful at a globally settled point: every `pending_deltas` entry
drained (all ticks applied), every exchange round complete, no in-flight epoch — view
state is legitimately inconsistent mid-tick and mid-exchange. The oracle must therefore
**quiesce before comparing**: drain all pending source deltas through their views and
await exchange/relay completion across all workers. If no single primitive guarantees
this today, building one is a prerequisite of this facility (it is also independently
useful for clean shutdown and checkpointing). The check runs after the system reports
settled; it never samples mid-flight.

### Gating and invocation

- **Debug-build + env-gated** (`GNITZ_VERIFY_VIEWS=1`), in the style of the existing
  `GNITZ_INJECT_*` / `GNITZ_CHECKPOINT_BYTES` toggles. Full recompute is O(view size);
  this is a test/diagnostic facility, never a production hot path.
- **Engine entry point:** a catalog method `verify_views_against_recompute()` that
  quiesces, recomputes each user view into scratch, runs the comparison core, and on
  mismatch emits the diff and (debug) aborts via the `gnitz_fatal_abort!` convention or
  (configurable) logs and continues.
- **Test adoption:** an E2E harness postcondition that calls the verifier at end of test
  (or after each DDL). This retrofits the oracle onto every existing E2E test with no
  per-test rewrite. Restart-equivalence (kill + boot rebuild + diff) is the same
  invariant realized through the existing recovery path; the boot tests are already a
  special case of it, and the harness may use either the in-process verifier or a
  restart-and-diff — both feed the same comparison core.
- **Dirty-tracking:** only re-verify views whose transitive base tables changed since the
  last check, so repeated invocation in a long test is cheap.

### Failure reporting

On mismatch, emit a structured, per-worker diff: rows only in the live store, rows only
in the recompute, and rows present in both with differing weight (`live w=4 vs recompute
w=1`), keyed by `(PK, payload)`. That converts "view `v` diverged" into "view `v`, row
`(aid=0, …)`, live weight 4 vs expected 1, worker 2" — the localization that was 90% of
the debugging value in the bugs this catches.

## Tests for the oracle itself

The oracle is correctness-critical infrastructure; it gets its own tests:

- **Detects real divergence.** Under a fault-injection toggle that deliberately drops or
  doubles a view-store row (or replay the conditions of the two known bugs), the verifier
  must fire with the correct diff. Without injection, every view must pass.
- **No false positive on the carve-out.** A `SUM`/`AVG`-over-`DOUBLE` view fed across
  multiple ticks (so live ≠ a clean fold) must **pass** — proving the float column is
  excluded — while its group-key set, cardinality, and any non-float columns are still
  checked exactly. Construct the multi-tick non-associative case (`{1.0, 1e17, -1e17}`
  across ticks) and confirm the verifier passes despite live `1.0` ≠ rebuild `0.0`.
- **Keeps float `MIN`/`MAX` and projections in scope.** A float `MAX` view and a
  per-row float-expression view must be compared by value and pass — guarding against
  over-tainting.
- **All shapes, both worker counts.** Projection, filter, GROUP BY (each aggregate),
  DISTINCT, set-ops, inner/outer/anti/semi/range joins, nested views — at W=1 and W=4 —
  must pass under normal operation.
- **No false positive mid-flight.** Confirm the verifier only runs post-quiescence (or
  that running it before quiescence is rejected, not silently mismatched).

## File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-engine/src/catalog/` (new module, e.g. `verify.rs`) | `verify_views_against_recompute()`: quiesce → recompute each view into a scratch store via the boot-correct drive → comparison core → diff/abort. |
| `crates/gnitz-engine/src/query/dag/` | Float-`SUM`/`AVG` taint analysis over a view's circuit (identify excluded output columns + forward propagation); a recompute-into-scratch entry that reuses the clean drive without touching the live store. |
| `crates/gnitz-engine/src/storage/repr/` (comparison core) | Weighted-multiset compare over two consolidated view stores using `compare_pk_bytes` + `compare_rows`, excluded-column projection, and an XXH3 fingerprint; per-worker and gathered diffs. |
| `crates/gnitz-engine/src/runtime/` | A quiescence barrier (drain pending ticks + await exchange completion) if one does not already exist; `GNITZ_VERIFY_VIEWS` wiring. |
| `crates/gnitz-py/tests/` | Oracle self-tests: fault-injection detection, float `SUM`/`AVG` carve-out non-false-positive, `MIN`/`MAX`/projection still-checked, all-shapes × W∈{1,4}, quiescence-gating. |

## Cost and scale

A recompute is O(view size) in time and memory; the fingerprint makes comparison O(1)
after the hash, and dirty-tracking skips unchanged views. This bounds the facility to
debug/test use — its value is catching divergence in CI and local runs, not in
production. The carve-out keeps it free of false positives on the one class of view
(float aggregates) whose value is legitimately non-deterministic under incremental
maintenance, which is the precondition for it to stay enabled rather than muted.
