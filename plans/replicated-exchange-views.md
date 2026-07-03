# Replicated-source views: run exchange-shaped circuits locally

A view whose sources are **all** replicated base tables is read from **worker 0
only**, but three exchange-shaped circuits scatter their output across all
workers — so worker 0 holds only the rows whose hash lands on its partition and
the rest are silently dropped, while broadcast writes make every worker scatter
the same rows and inflate the surviving weights ×W (worker count). This is
silent wrong results at `GNITZ_WORKERS ≥ 2` (correct at W=1, which masks it).

## Bug

The scan side classifies a view as replicated iff **all** its sources are
replicated base tables: `relation_output_is_replicated`
(`catalog/partition_lsn.rs:117-124`) → `view_all_sources_replicated`
(`query/dag/mod.rs:659-665`); when true the scan calls
`fan_out_scan_single_worker_async(..., 0, ...)`
(`runtime/orchestration/executor.rs:1233,1266-1277`) — worker 0 only. The output
store is single-partition (`Routing::Replicated`) whenever any source is
replicated (`catalog/hooks.rs:444-462`).

The compile side scatters output for three shapes even when their sources are
replicated, so the single-partition worker-0 store never sees the whole result:

- **binary set-ops** — UNION, UNION ALL, INTERSECT, EXCEPT (DISTINCT and ALL),
- **`SELECT DISTINCT`** over a replicated source,
- **pure-range and band joins** (and the pure-range-threshold behind EXISTS/IN).

Empirical repro (two `WITH (replicated=true)` tables, W=4 vs the W=1 oracle):

| view | expected | W=4 actual |
|---|---|---|
| `SELECT id,x FROM a UNION SELECT id,x FROM b` (8 distinct) | 8 rows | 3 rows |
| `… UNION ALL …` | 8 rows, Σw=8 | 4 rows, Σw=16 |
| `SELECT a.id,b.id FROM a JOIN b ON a.lo < b.hi` (16 pairs) | 16 pairs, Σw=16 | 5 pairs, Σw=80 |

Equi-joins (co-partition skip, `query/compiler/optimize.rs:36-45`) and GROUP BY
(shard-free `reduce_multi_local`, `plan/view/group_by.rs:469-472`) over
replicated sources already compute locally and are correct. The intended
invariant — "every worker computes the full result locally" — is documented at
`query/dag/mod.rs:646-654` and `compiler/emit.rs:838-852` but was realized only
for those two shapes.

## Current mechanics (verified against source)

A replicated base table's push is broadcast to every worker (the master's
broadcast write path; the pure-range input relay broadcasts the full delta,
`master/dispatch.rs:650-652`), so on **every** worker each replicated source's
trace is the full source and each tick delta is the full delta.

`execute_multi_worker_step` (`query/dag/mod.rs:1243-1296`) dispatches by shape
after `ensure_compiled`:

- **Arm 1 — range/band join** (`view_range_join_n_eq(view_id).is_some()`,
  1259-1268): `do_exchange` the input, then
  `run_exchange_pre_post(..., skip=false, ...)` — output exchange unconditional.
- **Arm 2 — binary set-op** (`has_exchange && view_has_side_b`, 1269-1274):
  `run_two_sided` scatters both sides via `do_exchange`.
- **Arm 3 — plain exchange incl. `SELECT DISTINCT`** (1275-1279): the synthetic
  content-hash PK never satisfies `view_skips_exchange`, so `skip=false`.
- **Arms 4/5** — equi-join (co-partition skip) and no-exchange — already local.

The single-worker path `execute_epoch_for_dag` (`query/dag/mod.rs:1452-1479`)
already runs every one of these shapes with **no exchange IPC**: set-op →
`run_two_sided` with an identity relay (1459-1463); exchange plan (DISTINCT,
range join) → `execute_pre_phase` → `consolidate_exchanged` → `execute_post_phase`
(1465-1478); else `execute_pre_phase`. The output `ExchangeShard` opcode emits no
VM instruction (`compiler/emit.rs:552`), so a circuit that keeps it runs
correctly on the local path.

The pure-range join alone bakes worker ownership into the **circuit**: it inserts
a `PartitionFilter` between the reindex and the trace integrate
(`plan/view/join.rs:1020-1024`), compiled with the build-time `worker_rank` /
`num_workers` to drop rows this worker does not own before `integrate_trace`
(`compiler/emit.rs:554-561`). The band path (`n_eq ≥ 1`) integrates the scattered
reindex directly and inserts no filter (1022-1023). The pure-range LEFT null-fill
adds a second `PartitionFilter` on the NULL-range-key branch (`join.rs:1236`).
The pure-range-threshold pattern in EXISTS/IN subquery views carries the
identical two `PartitionFilter`s (`plan/view/exists.rs:593,632`).

`client.table_replicated(tid)` (`gnitz-core/src/client.rs:1185-1197`) is the
planner's replication oracle, already used by GROUP BY (`group_by.rs:189`).

## Fix

The engine change fixes set-ops, `SELECT DISTINCT`, and band joins outright; the
planner change lets the pure-range (and EXISTS/IN) circuits also run correct-local.

### Part A — engine: run an all-replicated view's circuit locally

In `execute_multi_worker_step`, right after `ensure_compiled`, short-circuit an
all-replicated-source view to the existing single-owner path — no exchange IPC:

```rust
self.ensure_compiled(view_id);
// A view whose sources are all replicated holds the full copy of every source
// and receives the full (broadcast) delta on every worker, so it computes its
// entire result locally: the output is itself replicated and the worker-0 scan
// reads it whole. Run it exactly as single-worker mode does — no exchange IPC.
// (Equi-join and GROUP BY already reach a local path via the co-partition skip
// and reduce_multi_local; this intercepts the compiled-exchange shapes: binary
// set-ops, SELECT DISTINCT, range/band joins.) Every worker evaluates
// view_all_sources_replicated identically, so they skip the same exchange rounds
// and the collective barrier stays balanced.
if self.view_all_sources_replicated(view_id) {
    return self.execute_epoch_for_dag(view_id, input, src_id);
}
let has_exchange = self.view_needs_exchange(view_id);
// … arms 1-5 unchanged …
```

`execute_epoch_for_dag` ignores the `exchange` callback and runs the circuit
in-process; keeping each circuit's (no-op) output `ExchangeShard` is harmless.
No circuit change — set-ops, `SELECT DISTINCT`, and band joins over replicated
sources are correct after the engine rebuild.

### Part B — planner: drop the pure-range `PartitionFilter` when both sources are replicated

The `PartitionFilter` trims a broadcast pure-range input to the owning worker's
slice. When both join sources are replicated the view runs local (Part A) over
full traces, so the filter would wrongly discard rows and re-introduce data loss.
Thread a `both_replicated` flag and skip it:

- `lower_join` (`join.rs:124`, has `client`):
  `let both_replicated = client.table_replicated(left_tid)? && client.table_replicated(right_tid)?;`
  stored on `LoweredJoin` and passed through `emit_join` into `emit_range_join`.
  The equi chain builder `plan_multi_join_chain` sets it `false` (unused — chains
  are equi).
- `join.rs:1020-1024` — gate the two filters:

  ```rust
  let (int_a, int_b) = if n_eq == 0 && !both_replicated {
      (cb.partition_filter(reindex_a), cb.partition_filter(reindex_b))
  } else {
      (reindex_a, reindex_b)
  };
  ```

- `join.rs:1236` — the pure-range LEFT NULL-range-key branch: gate
  `cb.partition_filter(anull_keyed)` behind `!both_replicated` (feed
  `anull_keyed` straight into the `union` when replicated).

`build_pure_range_threshold` needs no change: its MIN/MAX threshold is an inline
reduce over the broadcast input, so it is already replicated on every worker.

EXISTS/IN subquery views carry the identical pure-range-threshold shape; apply
the same gate at `plan/view/exists.rs:593,632`, sourced from the outer/inner
relations' replication. Their engine execution is covered uniformly by Part A.

## Correctness

Premise (from code): a replicated table broadcasts each push to every worker, so
on every worker each replicated source trace = the full source and each tick
delta = the full delta.

- **Set-op / DISTINCT / band join (Part A).** `execute_epoch_for_dag` runs the
  identical circuit single-worker mode runs, over identical full inputs; since
  single-worker is correct, each worker independently computes the complete
  result. The outputs are identical on every worker (genuinely replicated), so
  the worker-0 scan returns the whole result. No gather/scatter occurs, so no
  ×W summation — weights are exact. UNION/UNION ALL are Z-set addition;
  INTERSECT/EXCEPT and DISTINCT are weight-clamp algebra over content-hashed
  leaves whose `distinct`/`positive_part` own their integrals and see full net
  weights. The incremental path is correct exchange-free: a broadcast delta is
  applied and ticked locally per worker; single-source-per-epoch is unchanged.
- **Pure-range join (Part A + Part B).** With `PartitionFilter` stripped,
  `trace_a`/`trace_b` integrate the full broadcast reindex → full traces on every
  worker; `join_ab`/`join_ba` (`join.rs:1027-1028`) produce the complete inner
  result, and the LEFT null-fill's `A − matched` over the full `trace_a` plus the
  replicated threshold yields the complete preserved side. Output = full result
  per worker → replicated → worker-0 scan correct; no double-broadcast → no
  inflation.
- **Barrier balance.** `view_all_sources_replicated` is a deterministic per-view
  property (cached dependency map + schema flags), identical on every worker, so
  all workers take the same local/exchange decision and issue the same sequence
  of collective rounds. Backfill drains in lockstep because a replicated source
  has identical row counts on every worker.
- **No regression for the already-correct shapes.** GROUP BY replicated has no
  `ExchangeShard`/side-B → `execute_epoch_for_dag` falls to `execute_pre_phase`,
  identical to today's Arm 5. Equi-join replicated is co-partitioned → today's
  Arm 4 also calls `execute_pre_phase`. Both are byte-identical under Part A.

## Scope boundary

This plan covers views whose sources are **all** replicated. A set-op or
range/band join with **mixed** sources (one replicated, one partitioned) is a
distinct defect: `view_all_sources_replicated` is false, so the scan correctly
fans out, but the replicated side's broadcast delta is gathered ×W by the
exchange and inflates. Its fix (contribute the replicated side from a single
worker, analogous to the equi-join co-partition skip) is unrelated to the
local-run change here and is not addressed. Nested views over a replicated-source
view are treated as partitioned (`query/dag/mod.rs:651-654`) and are likewise
out of scope.

## Tests

E2E (`crates/gnitz-py/tests/`, always `GNITZ_WORKERS=4` — the bug is masked at
W=1). Assert per-row **weights**, not just row presence: the bug inflates
weights, so a presence-only assertion would miss the ALL variants. Model on
`test_replicated.py::test_replicated_join_replicated_single_source`.

- Set-ops over two replicated tables: UNION ALL (8 rows, every weight 1, Σw=8),
  UNION DISTINCT, INTERSECT, EXCEPT (DISTINCT and ALL) — exact row set and
  weights (use two distinct tables to avoid the same-relation reject).
- `SELECT DISTINCT` over a single replicated source.
- Pure-range and band joins over two replicated tables, INNER and LEFT: every
  expected pair once (weight 1), total pair count equal to the W=1 oracle;
  include a pure-range LEFT with a NULL range key (exercises the second filter).
- EXISTS/IN subquery view over replicated sources.
- Self `a UNION ALL a` over a replicated table (weights ×2 per identical row =
  correct, not ×2W) — pins that the local path is not itself inflating.

Run: `make e2e K='replicated or set_op or join' WORKERS=4`.

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`) after
each.

- [ ] 1. **Engine local-run branch (Part A).** Add the
  `view_all_sources_replicated` short-circuit in `execute_multi_worker_step`.
  Fixes set-ops, `SELECT DISTINCT`, and band joins immediately (no circuit
  change). Add the set-op / DISTINCT / band-join e2e tests.
- [ ] 2. **Pure-range PartitionFilter gate (Part B).** Thread `both_replicated`
  through `lower_join` → `emit_join` → `emit_range_join`; gate the two
  `PartitionFilter`s in `join.rs` (1020-1024, 1236) and the two in `exists.rs`
  (593, 632). Pure-range and EXISTS/IN views over replicated sources now run
  correct-local. Add the pure-range/band LEFT and EXISTS/IN e2e tests. Any
  pure-range replicated view created before this commit keeps its old persisted
  circuit until re-`CREATE`d; the tests create fresh views.
