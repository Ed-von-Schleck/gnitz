# Backfill live-created exchange views from existing base data

A view created **after** its base tables already hold committed data must reflect
that data. Today this holds only for **non-exchange** views. A view that compiles
to a circuit containing an `ExchangeShard` node — every JOIN, GROUP BY, set-op
(UNION/INTERSECT/EXCEPT), and SELECT DISTINCT — created live (after server start)
over already-committed tables comes up **empty** and only ever fills from deltas
ingested *after* the view was created. Pre-existing rows are silently omitted.

Reproduced at `GNITZ_WORKERS=4` (`crates/gnitz-py/tests/test_view_backfill_existing_data.py`):
load + commit data, then `CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id`
(or `… SUM(n) … GROUP BY g`), then scan `v` → **0 rows**. The view-first control
(create then insert) passes, because post-create inserts flow through the view as
normal ticks. Forcing the inserts to tick/commit *before* the view exists is what
exposes the gap (otherwise still-pending deltas tick through the freshly-registered
view and mask it).

## Root cause

A non-exchange view is backfilled inline when its `VIEW_TAB` row is applied:
`hook_view_register` (`catalog/hooks.rs:459-465`) calls `self.backfill_view(vid)` —
but only `&& !self.dag.view_needs_exchange(vid)`. `view_needs_exchange`
(`query/dag/mod.rs:674-685`) is true for any circuit with an `ExchangeShard` node,
so the inline backfill is **skipped for every exchange view**. The single-process
`backfill_view` (`catalog/ddl.rs:381`) carries the same `!view_needs_exchange`
guard.

The distributed backfill that *does* handle exchange views, `fan_out_backfill`
(`runtime/orchestration/master/dispatch.rs:737`), has exactly one non-test caller:
`backfill_exchange_views` (`runtime/bootstrap.rs:203-217`), invoked once from
`server_main` at boot, before the listen socket opens. **It is never reached from
the live DDL path.**

The live DDL handler `handle_system_dml` (`runtime/orchestration/executor.rs:1261`)
registers the view (mutate phase → `ingest_to_family` → `fire_hooks` →
`hook_view_register`), broadcasts the catalog rows to workers (`FLAG_DDL_SYNC`),
fsyncs, seeds unique-index filters, and responds. Its `evaluate_dag(target_id, …)`
call (executor.rs:1409) passes `target_id = VIEW_TAB_ID`, whose dependents are
empty (`drive_dag` returns at `query/dag/mod.rs:1021-1025`), so it does not push
base data through the new view. **Nothing in the live path backfills the new
exchange view.** Workers' `ddl_sync` (`catalog/store_io.rs:475`) only fires the same
guarded hook and stores the sys rows; they register the empty view and wait for
ticks. No planner/catalog guard rejects creating a view over populated tables, so
it fails silently rather than erroring.

Net: **(b) silent omission of pre-existing data** — a correctness bug.

## Committed fix

After a live `CREATE VIEW` of an exchange view is durable and synced to all workers,
the master runs the **same distributed backfill the boot path uses**
(`fan_out_backfill` per `(view, source)`), then responds. The boot path is the
template; the only new work is invoking it live with correct lock/quiescence/
once-only semantics.

### Where it hooks in

In `handle_system_dml`, the `target_id == VIEW_TAB_ID` DDL, **after** the existing
broadcast + `commit_zone` + fsync (executor.rs:1463-1470, so every worker has the
view registered and compiled) and **before** `send_ok_response` (so the client sees
a populated view on return):

1. Scan the applied batch for newly-created views (`weight > 0`) that
   `dag.view_needs_exchange(vid)`. (Non-exchange views are already backfilled in the
   hook; skip them.)
2. For each such `vid`, for each `source_id` in `dag.get_source_ids(vid)` that is a
   registered table, run the distributed backfill.

This mirrors `backfill_exchange_views` (`bootstrap.rs:203-217`) exactly, restricted
to the views created by this DDL.

### Async distributed backfill (don't block the reactor or hold the write lock long)

The boot `fan_out_backfill` is synchronous (`maybe_checkpoint` + `send_broadcast` +
`collect_acks_and_relay`), valid only because boot is single-threaded before the
reactor runs. The live DDL handler is an async task on the single reactor and holds
`catalog_rwlock.write()` for the whole function. Add an async variant
`fan_out_backfill_async(disp_ptr, reactor, sal_excl, view_id, source_id)` modeled on
`validate_unique_index_create_async` / `fan_out_seek_async`
(`master/dispatch.rs:752-767`): it takes `*mut MasterDispatcher` + `&Reactor` +
`&sal_writer_excl` so the exclusive dispatcher borrow ends before each `.await`,
letting the reactor keep draining W2M and routing relays while the backfill's
lockstep rounds run. Drive the existing per-chunk `FLAG_BACKFILL` round protocol
(pad bit / `BACKFILL_DECISION_*`, `collect_acks_and_relay`) unchanged.

**Lock discipline.** Do not hold `catalog_rwlock.write()` across the chunked
backfill (it would block every concurrent SCAN/SEEK for the backfill's duration).
Downgrade to a read lock for the backfill phase: the backfill only *reads* the
catalog (`get_schema_and_names`, `get_source_ids`) and drives workers, which read
their own committed source partitions. Acquire the read lock after the write lock is
dropped (restructure the tail of `handle_system_dml` so the write-locked critical
section ends at fsync, and the backfill runs under a freshly-acquired read lock).
The view is already registered and durable at this point, so a read lock suffices.

### Once-only correctness (no double-count, no gap)

The boot backfill is race-free because no deltas flow concurrently. Live, the new
view is a dependent of its sources the moment it is registered, so steady-state
ticks could otherwise interleave. The fix makes each base row reach the view
**exactly once** via an LSN watermark, reusing the engine's existing per-family
ingest-LSN tracking (the same watermarks `recover_from_sal` and the boot backfill
rely on, `bootstrap.rs:172-196`):

- **Snapshot bound.** Before the first source chunk, record `w = ` the source's
  committed ingest LSN. The backfill replays committed base storage as of `w` (its
  read cursors are consolidated snapshots taken at backfill start, so later commits
  do not enter the snapshot mid-drain).
- **Pre-backfill flush.** The DDL handler already drains a committer barrier at entry
  (executor.rs:1304-1306); ensure all *currently pending* source deltas are ticked
  into committed base storage before the snapshot is taken, so the snapshot is
  complete. Because the view is not yet a dependent when those pre-existing deltas
  were (or are) ticked, they enter the view only via the backfill, never via a tick.
- **Incremental start = `w`.** The view's steady-state evaluation begins after `w`:
  a base delta with LSN `≤ w` is delivered by the backfill; a delta with LSN `> w`
  is delivered by the normal incremental tick. A delta is committed to base storage
  exactly once, so it falls on exactly one side of `w` — no row is doubled and none
  is dropped.

Per-worker, this is naturally serialized: a worker runs the whole multi-round
`handle_backfill` loop (`worker/mod.rs:1203-1221`) to completion before returning to
its main SAL loop, so it never processes a concurrent `FLAG_PUSH`/`FLAG_TICK` mid-
backfill; queued pushes (LSN `> w`) are applied through the populated view afterward.

### Client semantics

`CREATE VIEW` returns only after the backfill completes (the backfill is awaited
before `send_ok_response`), so a query issued right after `CREATE VIEW` sees the full
result — matching the non-exchange-view contract and SQL expectations.

## Edge cases

- **Non-exchange views** — unchanged: still backfilled inline by `hook_view_register`
  (`backfill_view`); the new path's `view_needs_exchange` filter skips them.
- **Empty sources** — the backfill drains zero chunks and pads immediately; the view
  stays empty, correctly.
- **Multi-source views (joins)** — iterate every `get_source_ids(vid)` entry, exactly
  as `backfill_exchange_views` does; each source is backfilled through the view's
  circuit (the join's other-side trace is populated by that side's backfill round).
- **Views over views (nested)** — a source that is itself a view must already be
  populated. `get_source_ids` returns the immediate sources; if a source view was
  created in the **same** DDL batch, order the backfills by `dag` depth
  (`TableEntry.depth`) so a dependency is backfilled before its dependent. Single-
  statement `CREATE VIEW` creates one view, so this only matters if a future batched
  DDL creates several; ordering by depth is correct regardless.
- **Replicated-source views** (single-partition, `has_replicated_source`) — handled
  by the same `fan_out_backfill` the boot path already uses for them; no special case.
- **Crash mid-backfill** — the view is ephemeral (`RelationKind::View ⇒
  Persistence::Ephemeral`); a crash before the backfill completes is recovered by the
  boot `backfill_exchange_views`, which rebuilds it from scratch. The CREATE itself is
  durable (committed before the backfill), so the view exists post-recovery and is
  repopulated at boot. No half-applied state: the backfill only *adds* to an empty
  ephemeral store.
- **CREATE VIEW latency** — a live backfill of a large base table is O(base size),
  paid once at CREATE under a catalog *read* lock. Acceptable: CREATE VIEW is rare and
  the alternative is silent data loss.

## Tests

- `crates/gnitz-py/tests/test_view_backfill_existing_data.py` — the two `xfail`
  tests (`test_vbf_join_over_populated_tables_sees_existing_data`,
  `test_vbf_group_by_over_populated_table_sees_existing_data`) become passing;
  remove the `xfail` markers when the fix lands. Extend with:
  - set-op (`UNION`/`EXCEPT`/`INTERSECT`) and `SELECT DISTINCT` views over committed
    data (all exchange views).
  - a concurrent-insert variant: load + commit data, `CREATE VIEW`, and immediately
    interleave new inserts; assert the view equals brute-force over the full base
    (backfilled snapshot + post-create deltas, each counted once) — guards the
    watermark.
  - a non-exchange view over committed data (control: already works, must keep
    working).
  - nested: a view over an exchange view, both created after data; assert correctness.
- Engine integration test at `W=4`: assert per-source `fan_out_backfill` is invoked
  exactly once per new exchange view and that the view's row count equals the boot-
  backfilled reference (create-then-restart vs create-live produce identical views).

## File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs` | Add `fan_out_backfill_async(disp_ptr, reactor, sal_excl, view_id, source_id)` (async sibling of `fan_out_backfill`, modeled on `validate_unique_index_create_async`/`fan_out_seek_async`); record/return the source watermark `w` for the view's incremental start |
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | In `handle_system_dml`, after the DDL broadcast+commit+fsync: end the write-locked section, take a catalog read lock, and for each newly-created `view_needs_exchange` view backfill each source via `fan_out_backfill_async`; await before `send_ok_response` |
| `crates/gnitz-engine/src/query/dag/mod.rs` (or catalog) | Set the new view's incremental-evaluation start LSN to the backfill watermark `w`, so pre-`w` base deltas are not re-applied via ticks |
| `crates/gnitz-engine/src/catalog/hooks.rs` | (No behavior change required, but document at the `!view_needs_exchange` guard that exchange views are backfilled by the live DDL handler / boot path, not inline) |
| `crates/gnitz-py/tests/test_view_backfill_existing_data.py` | Un-`xfail` the two tests; add set-op/distinct/nested/concurrent-insert cases |
| `crates/gnitz-engine` integration tests | `fan_out_backfill_async` invoked once per source; create-live == create-then-restart |

The boot path (`backfill_exchange_views`) and the inline non-exchange `backfill_view`
are unchanged; this only adds the missing live trigger for exchange views.
