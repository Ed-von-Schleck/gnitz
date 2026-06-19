# Non-exchange views lose committed-but-unflushed data on crash recovery

A **non-exchange view** (a view whose compiled circuit has no `ExchangeShard` —
pure filter/map/projection over one source) silently drops every base-table row
that was committed but not yet checkpointed to a shard when the server crashed.
After restart the view reflects only data that had been flushed before the crash;
all rows written since the last checkpoint are gone, even though the base table
recovers them correctly. This violates the durability contract: an ACKed write is
fdatasync'd to the SAL, so it must reappear everywhere it logically belongs —
including in derived views — after a crash.

Reproduced at `GNITZ_WORKERS=4`
(`crates/gnitz-py/tests/test_persistence.py::test_nonexchange_view_retains_unflushed_data_after_restart`,
currently `xfail(strict=True)`): create `t` + `CREATE VIEW v AS SELECT pk, val + 1
FROM t`, `INSERT INTO t VALUES (7, 70)` (ACKed → durable in the SAL, never
checkpointed), SIGKILL, restart → `scan(v)` is **empty**. The base table `t`
recovers row 7; only the view loses it. An exchange view (JOIN / GROUP BY) in the
same scenario recovers correctly — the bug is specific to non-exchange views.

## Root cause: boot rebuilds non-exchange views *before* the SAL replay

The boot sequence populates the three derived-state kinds at three different
points relative to the SAL replay (`recover_from_sal`), and only non-exchange
views are rebuilt on the wrong side of it.

1. **Catalog open (pre-fork).** `CatalogEngine::open` (`catalog/bootstrap.rs:81`)
   runs `replay_catalog` (`catalog/bootstrap.rs:96`) → `hook_view_register`
   (`catalog/hooks.rs:361`), whose inline `backfill_view(vid)`
   (`catalog/hooks.rs:464`) fires for every **non-exchange** view (guard:
   `!in_rollback && active_part_start != active_part_end && ensure_compiled &&
   !view_needs_exchange`, `hooks.rs:459-465`). At this point the only base data on
   disk is the **durable shards**; the committed-but-unflushed rows are still in
   the SAL and have not been replayed. `open` then calls `go_live`
   (`catalog/bootstrap.rs:99`).

2. **Fork + SAL replay (post-fork, per worker).** `server_main` forks workers
   (`runtime/bootstrap.rs:432`); each sets its partition range, then
   `recover_from_sal` (`runtime/bootstrap.rs:498`) replays unflushed `FLAG_PUSH`
   batches via `replay_ingest` (`catalog/store_io.rs:509`). `replay_ingest` routes
   through `ingest_returning_effective`, which writes the **base store and projects
   to secondary-index shards** (`store_io.rs:506-508`) — but it is a raw store
   ingest that **bypasses DAG/view derivation** (`recover_from_sal`,
   `runtime/bootstrap.rs:172-196`). So views are never updated by the replay.

3. **Exchange-view backfill (post-recovery, master).** After every worker acks
   recovery, the master runs `backfill_exchange_views`
   (`runtime/bootstrap.rs:582`), which `fan_out_backfill`s **only** views where
   `view_needs_exchange(vid)` is true (`runtime/bootstrap.rs:203-216`). These run
   against the workers' now-complete base stores, so exchange views see the
   replayed rows.

The asymmetry is the whole bug:

| Derived state | Populated by | Relative to SAL replay | Sees unflushed rows? |
|---|---|---|---|
| Secondary indexes | `replay_ingest` projection (`store_io.rs:506`) | during | **yes** |
| Exchange views | `backfill_exchange_views` (`bootstrap.rs:582`) | after | **yes** |
| **Non-exchange views** | inline `backfill_view` at `open` (`hooks.rs:464`) | **before** | **no** |

A non-exchange view is built from durable shards at `open`, and the replay that
restores the unflushed base rows neither re-derives it (step 2 bypasses the DAG)
nor is it re-backfilled afterward (step 3 skips it). The rows are therefore
present in the base table and absent from the view — permanently, until the view
is recompiled by some later full rebuild that happens to run after a checkpoint.

Views are ephemeral (`RelationKind::View ⇒ Persistence::Ephemeral`,
`query/dag/mod.rs:80-83`; their store is erased at open), so there is no durable
view shard to fall back on — the boot backfill is the *only* thing that fills a
view at startup, and for non-exchange views it runs too early.

## Committed fix: backfill non-exchange views after recovery, per worker

Move the non-exchange-view boot backfill to the same side of the SAL replay that
exchange views already use — after `recover_from_sal` — and run it per worker
(non-exchange views are embarrassingly partitioned, so each worker rebuilds its
own partition locally with no exchange).

### Change 1 — stop the inline boot backfill for views

In `hook_view_register` (`catalog/hooks.rs:459-465`), gate the inline
`backfill_view` on the live phase so it fires only for a live `CREATE VIEW`, never
during boot replay:

```rust
// During DROP VIEW rollback the partition files are intact; re-pushing
// source rows through the circuit would double every aggregation.
// Boot replay backfills non-exchange views AFTER recover_from_sal (see
// runtime bootstrap), so an unflushed base row replayed from the SAL is not
// lost; only live CREATE backfills inline here.
if !self.ctx.in_rollback()
    && self.ctx.is_live()
    && self.active_part_start != self.active_part_end
    && self.dag.ensure_compiled(vid)
    && !self.dag.view_needs_exchange(vid)
{
    self.backfill_view(vid);
}
```

`is_live()` (`catalog/apply_context.rs:68`) is `false` throughout
`CatalogEngine::open` (the `go_live` flip is the last step, `catalog/bootstrap.rs:99`)
and `true` when a worker applies a live `FLAG_DDL_SYNC` — so a live non-exchange
`CREATE VIEW` is unaffected (its worker hook still backfills inline), while boot
replay no longer pre-fills the view from durable-shards-only.

Update the `ApplyContext` doc comment (`catalog/apply_context.rs:33`) that today
says rebuild-at-boot (`backfill_index` / `backfill_view`) "must run during replay
and is gated on rollback, not on go_live": that remains true for `backfill_index`
(indexes are correctly completed by the replay projection), but **`backfill_view`
for non-exchange views now runs post-recovery**, not during replay.

### Change 2 — backfill non-exchange views post-recovery on each worker

In `server_main`'s worker branch (`runtime/bootstrap.rs`), after
`recover_from_sal` (line 498), the base-table flush loop (lines 508-513), and
`invalidate_all_plans` (line 523), and before `worker.run` (line 540), rebuild
this worker's non-exchange views from the now-complete base stores:

```rust
catalog.invalidate_all_plans();

// Rebuild non-exchange views from this worker's recovered base stores.
// recover_from_sal restored committed-but-unflushed base rows but bypassed
// view derivation; the inline open-time backfill (now live-only) did not run.
// Exchange views are handled post-recovery by the master's
// backfill_exchange_views. Depth order (sources before dependents) so a view
// built over another non-exchange view reads a populated source.
let mut nx_views: Vec<i64> = catalog
    .iter_user_table_ids()
    .filter(|&vid| {
        catalog.dag.tables.get(&vid).is_some_and(|e| e.kind == RelationKind::View)
            && !catalog.dag.view_needs_exchange(vid)
    })
    .collect();
nx_views.sort_by_key(|&vid| catalog.dag.tables.get(&vid).map(|e| e.depth).unwrap_or(0));
for vid in nx_views {
    catalog.backfill_view(vid);
}
```

`backfill_view` (`catalog/ddl.rs:381`) is **view-scoped** (it drains the view's
own `get_source_ids` and runs `execute_epoch(vid, …)` — it does *not* evaluate a
source's whole closure the way the source-scoped `handle_backfill` /
`fan_out_backfill` path does, so two views over the same source do not
cross-contaminate), local (no exchange — correct for a non-exchange view), and
flushes the view internally. It asserts the target is ephemeral, which holds.

Exchange views stay exactly as they are: `backfill_exchange_views`
(`runtime/bootstrap.rs:582`) on the master, untouched.

## Once-only correctness (no double-count)

With Change 1 the inline open-time backfill no longer runs at boot, so each
worker inherits an **empty** ephemeral view store across `fork` (and
`trim_worker_partitions` keeps it empty). Change 2 then fills it exactly once from
the base store, which after `recover_from_sal` holds durable shards **plus** the
replayed unflushed rows — each base row contributes to the view exactly once.
`backfill_view`'s ephemeral assertion (`catalog/ddl.rs:385`) guards against a
durable view shard being double-counted; views have none.

A live `CREATE VIEW` is unchanged: `is_live()` is true, the worker's inline
`backfill_view` runs once when it applies `FLAG_DDL_SYNC`, and no boot path
re-touches the view.

## Edge cases

- **Already-flushed data** — if everything was checkpointed before the crash, the
  base store after recovery is just the durable shards (the replay finds nothing
  new); `backfill_view` rebuilds the view from them. Identical result to today,
  produced post-recovery instead of at open.
- **Nested non-exchange views** (a projection over another non-exchange view) —
  the depth sort backfills the inner view first, so the outer view reads a
  populated source. (A non-exchange view ID is always higher than its source
  view's, since the source must exist at `CREATE` time, so plain ID order is also
  depth-correct; the explicit sort makes the dependency intent unmistakable.)
- **Empty source** — `backfill_view` drains zero chunks; the view stays empty,
  correctly.
- **Non-exchange view over an *exchange* view** — out of scope. Such a view reads
  the exchange view's store, which the master fills only at
  `backfill_exchange_views` (`runtime/bootstrap.rs:582`), *after* every worker has
  left boot — so the worker-local backfill here (like the inline open-time
  backfill it replaces) sees the source still empty. This is a pre-existing gap
  for that rarer shape, not a regression; fixing it needs a master-coordinated
  pass that backfills non-exchange dependents after their exchange sources, and
  belongs in its own plan.

## Tests

- `crates/gnitz-py/tests/test_persistence.py::test_nonexchange_view_retains_unflushed_data_after_restart`
  — remove the `xfail(strict=True)` marker; it must pass at `GNITZ_WORKERS=4`.
- Add a flushed-data control: same scenario but with an explicit
  checkpoint/flush before the SIGKILL, asserting the view still has the row
  (guards against Change 1 regressing the already-flushed path).
- Add a nested non-exchange case: `v2 AS SELECT … FROM v1` where `v1` is a
  non-exchange view over a base table, unflushed data, restart, assert both views
  carry the row (guards the depth ordering).
- Keep an exchange-view restart case (JOIN or GROUP BY over unflushed data) as a
  control: it already recovers via `backfill_exchange_views` and must continue to.

## File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-engine/src/catalog/hooks.rs` | Add `&& self.ctx.is_live()` to the inline `backfill_view` guard (`hooks.rs:459-465`) so boot replay no longer pre-fills views from durable-shards-only; live CREATE is unaffected. |
| `crates/gnitz-engine/src/catalog/apply_context.rs` | Update the rebuild-at-boot doc comment (`apply_context.rs:33`): non-exchange `backfill_view` now runs post-recovery, not during replay; `backfill_index` is unchanged. |
| `crates/gnitz-engine/src/runtime/bootstrap.rs` | In the worker branch, after `recover_from_sal` + base flush + `invalidate_all_plans`, backfill each registered non-exchange view (`kind == View && !view_needs_exchange`) in depth order via `catalog.backfill_view(vid)`. |
| `crates/gnitz-py/tests/test_persistence.py` | Un-`xfail` `test_nonexchange_view_retains_unflushed_data_after_restart`; add flushed-data control, nested non-exchange case, exchange-view control. |

Secondary indexes and exchange views already recover correctly (replay projection
and post-recovery `backfill_exchange_views` respectively); this plan only moves the
non-exchange-view rebuild to the correct side of the SAL replay.
