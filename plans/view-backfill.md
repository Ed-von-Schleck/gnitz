# View Backfill on Creation and Restart ✅ COMPLETE

Materialized views are no longer empty after server restart or when data
exists before `CREATE VIEW`. All five changes are implemented and tested.

---

## Background

### The actual scope of the bug

`EphemeralTable.__init__` creates an `FLSMIndex` with a blank slate — no
manifest read, no shard discovery. Every process start leaves all
materialized views empty until new DML flows through.

`IndexEffectHook.on_delta` accounts for this by calling `_backfill_index`
unconditionally on every hook fire. `ViewEffectHook.on_delta` had no
equivalent.

### Why indexes survive but views don't

`replay_catalog` replays Schema → Table → **View** → Index. Index replay
fires `_backfill_index` (scan source → populate EphemeralTable). View replay
created an empty EphemeralTable and stopped there.

### The topological order invariant

Views get IDs from `allocate_table_id()` — the same sequence as tables.
`vid > source_tid` always; `vid_B > vid_A` for views-on-views. Replay feeds
hook rows in ascending PK order, so V1 is registered and backfilled before
V2's row is processed. **No explicit topological sort is needed.**

### Why `plan.execute_epoch`, not `evaluate_dag` for backfill

`evaluate_dag` reads the fully-persisted DepTab, which during restart already
contains `V1 → [V2]` entries for downstream views not yet registered. V1's
backfill would cascade to V2, `registry.get_by_id(V2_id)` would raise.
`plan.execute_epoch` is self-contained: scan source → compute → write to
target, no cascade. Each view's backfill is independent.

### The correctness boundary for multi-worker

With `exchange_handler=None`, join and reduce views produce silently wrong
results (rows co-located by `hash(pk)`, not `hash(join_key)`). The guard:

```python
def _view_needs_exchange(plan):
    return (plan.exchange_post_plan is not None   # reduce/aggregate
            or plan.join_shard_map is not None)   # join
```

Non-exchange views (filter, map, project) backfill locally in the hook.
Exchange views backfill via the master's `fan_out_backfill` after workers spawn.

---

## Changes (all complete)

### Change 1 — EphemeralTable stale-file cleanup ✅

`EPH_SHARD_PREFIX = "eph_shard_"` defined as module constant. `EphemeralTable._erase_stale_shards()` deletes stale `eph_shard_*` files from the directory on init. `PersistentTable._erase_stale_shards()` is a no-op override (shards tracked by MANIFEST must survive restarts).

**Files:** `gnitz/storage/ephemeral_table.py`, `gnitz/storage/table.py`

---

### Change 2 — `wire_catalog_hooks(engine)` refactor ✅

Collapsed 4-argument `wire_catalog_hooks(registry, sys, base_dir, program_cache)` to `wire_catalog_hooks(engine)`. All hooks store `self.engine` in `_immutable_fields_ = ["engine"]`. `Engine(...)` is now constructed before the hooks call.

**Files:** `gnitz/catalog/engine.py`, `gnitz/catalog/hooks.py`

---

### Change 3 — `get_source_ids`; fix view depth ✅

`get_dep_map` builds the inverse `{view_id → [source_ids]}` in the same scan at zero extra cost, stored as `_source_map`. `get_source_ids(view_id)` exposes it. Deleted broken `_compute_view_depth` (read dead `COL_DEP_VIEW_ID` column, always 0). `ViewEffectHook.on_delta` now computes depth inline via `get_source_ids`.

**Files:** `gnitz/catalog/program_cache.py`, `gnitz/catalog/hooks.py`

---

### Change 4 — `_backfill_view` for non-exchange views ✅

`_backfill_view(engine, vid)` scans each source family, calls `plan.execute_epoch(scan_batch, source_id=source_id)`, and ingests results. Called from `ViewEffectHook.on_delta` after `register(family)`, guarded by `not _view_needs_exchange(plan)`. Works identically on workers (partition-scoped engine) and master (single-worker mode).

Schema mismatches now raise `LayoutError` at `create_view` time (fail-fast, not lazily at `get_program` time). `test_schema_mismatch_raises_layout_error` updated accordingly.

Also fixed: unknown opcodes in `_emit_node` now propagate their input register as a pass-through instead of leaving the output slot null (pre-existing crash exposed by the fail-fast schema check).

**Files:** `gnitz/catalog/hooks.py`, `gnitz/catalog/program_cache.py`

---

### Change 5 — Post-spawn backfill for exchange-requiring views ✅

`FLAG_BACKFILL = 2048` added to `ipc.py`. Master calls `fan_out_backfill(vid, source_tid, schema)` for each exchange-requiring view after workers spawn but before `run_socket_server`. Worker `_handle_backfill(source_tid, view_id)` scans local partition and calls `evaluate_dag` with `exchange_handler` set — workers participate in the exchange barrier normally.

**Files:** `gnitz/server/ipc.py`, `gnitz/server/main.py`, `gnitz/server/master.py`, `gnitz/server/worker.py`

---

## Tests (all passing)

| Test | Where | What |
|------|-------|-------|
| `test_view_backfill_simple` | `catalog_additional_test.py` | Backfill fires at create-view time; live update path also correct |
| `test_view_backfill_on_restart` | `catalog_additional_test.py` | Replay triggers backfill; view populated after re-open |
| `test_view_on_view_backfill_on_restart` | `catalog_additional_test.py` | V1→V2 chain; correct depths (1, 2); restart repopulates both |
| `test_workers_view_ddl_then_push` | `test_workers.py` | Updated assertion `[1, 2, 3]` (pre-creation row now visible) |
| `test_workers_view_on_view_backfill` | `test_workers.py` | V1→V2 chain in multi-worker; both views backfilled correctly |
| `test_reactive_view_via_push` | `server_test.py` | Updated to expect 3 rows (backfill adds pre-existing pk=3) |
