# Single-source the unique-index preflight for replicated owners

`CREATE UNIQUE INDEX` on a **populated replicated table** always fails with a
false duplicate error under multi-worker operation. Fix: fan the preflight to
exactly one worker when the owner's store is replicated — every worker holds
an identical full copy, so one stream is the complete span set.

## The bug (verified against source)

- `validate_unique_index_create_async`
  (`runtime/orchestration/master/preflight.rs:1012-1108`) fans
  `FLAG_UNIQUE_PREFLIGHT` to **all** workers unconditionally via
  `dispatch_scan_fanout` (`preflight.rs:1084-1101`); each worker sorts and
  streams its base slice's OPK value spans back.
- The master's k-way merge flags a duplicate on **adjacent equal value
  spans**: `PreflightAccumulator::offer` sets `duplicate` when
  `self.prev == Some(key)` (`preflight.rs:139-150`), and the heap orders by
  `(span, worker)` so equal spans from different workers pop adjacently by
  design (`preflight.rs:165-170` — that is how genuine cross-partition
  duplicates are caught on hashed tables).
- Pushes to a replicated table are broadcast whole to every worker, which
  "enforces uniqueness against its identical full copy"
  (`master/dispatch.rs:1698-1705`); at boot the replicated store is exempt
  from partition trim and re-homed per worker with its full flushed copy
  (`catalog/partition_lsn.rs:50-108`). So with W ≥ 2 workers every distinct
  value in a populated replicated owner arrives W times and pops adjacently
  → `merged.duplicate` → `unique_create_dup_err` (`preflight.rs:1105-1107`)
  — the CREATE fails even though every value is unique.
- The same W-fold streams also make the filter seed (`into_seed`,
  `preflight.rs:152-157`) W× more likely to hit its cap for no reason.

Precedent for the fix already exists: replicated relations are single-sourced
on the scan path (`fan_out_scan_single_worker_async`, chosen at
`master/dispatch.rs:1251-1258`).

## Design

In `validate_unique_index_create_async`, before the fan-out, read the owner's
store shape off the master's handle — `PartitionedTable::is_replicated()`
(the store-shape predicate; routing is intact on the master even with
partitions closed). When replicated, issue the `FLAG_UNIQUE_PREFLIGHT` group
to worker 0 only — the single-worker analogue of the existing
`dispatch_scan_fanout` closure, mirroring how
`fan_out_scan_single_worker_async` targets one worker's req-id slot — and run
the same `merge_index_scan` over the single stream (the merge machinery is
count-agnostic; a one-stream merge still catches genuine within-copy
duplicates via the same adjacent-equal check). Hashed owners keep the full
fan-out unchanged.

W = 1 is unchanged by construction. The seeded unique filter
(`unique_filter_seed`) now receives each distinct span once, so the cap
reflects real cardinality.

## Tests

E2E (`make e2e`, `GNITZ_WORKERS=4`):

- `test_create_unique_index_on_populated_replicated_table`: replicated table
  (the small-dimension shape that gets `Routing::Replicated`), insert N rows
  with distinct values, `CREATE UNIQUE INDEX` → must succeed (fails with a
  false duplicate today); then insert a genuinely colliding value → rejected.
- Duplicate-detection still works: same setup with two rows sharing the
  indexed value inserted **before** the CREATE → `CREATE UNIQUE INDEX` fails.
- Hashed-owner regression guard: existing unique-index e2e tests cover the
  all-workers fan-out; no changes expected.

## Sequencing

- [ ] 1. Single-source the replicated preflight + both e2e tests
  (`make verify` + `make e2e` green).
