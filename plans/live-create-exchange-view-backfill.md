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
`hook_view_register` (`catalog/hooks.rs:361`) calls `self.backfill_view(vid)` at
`hooks.rs:459-465` — but only when
`!self.ctx.in_rollback() && self.active_part_start != self.active_part_end &&
self.dag.ensure_compiled(vid) && !self.dag.view_needs_exchange(vid)`.
`view_needs_exchange` (`query/dag/mod.rs:674`) is true for any circuit with an
`ExchangeShard` node, so the inline backfill is **skipped for every exchange view**.
The single-process `backfill_view` (`catalog/ddl.rs:381`) is the body that is
skipped.

The distributed backfill that *does* handle exchange views, `fan_out_backfill`
(`runtime/orchestration/master/dispatch.rs:737`), has exactly one non-test caller:
`backfill_exchange_views` (`runtime/bootstrap.rs:202-218`), invoked once from
`server_main` at boot, before the listen socket opens. **It is never reached from
the live DDL path.**

The live DDL handler `handle_system_dml` (`runtime/orchestration/executor.rs:1261`)
registers the view (mutate phase: `ingest_to_family` → `fire_hooks` →
`hook_view_register`, driven by `evaluate_dag(target_id, batch)` at
`executor.rs:1409`), broadcasts the catalog rows to workers (`FLAG_DDL_SYNC`,
`executor.rs:1447-1449`), closes the zone and fsyncs (`executor.rs:1458-1466`),
publishes the zone LSN (`executor.rs:1475`), and responds. The `evaluate_dag` call
passes `target_id = VIEW_TAB_ID`, whose user-view dependents are empty
(`drive_dag` returns at `query/dag/mod.rs:1023-1025` because `dep_map[VIEW_TAB_ID]`
is empty), so it does not push base data through the new view. **Nothing in the
live path backfills the new exchange view.** Workers' `ddl_sync`
(`catalog/store_io.rs:475`) only fires the same guarded hook and stores the sys
rows; they register the empty view and wait for ticks. No planner/catalog guard
rejects creating a view over populated tables, so it fails silently rather than
erroring.

Net: **silent omission of pre-existing data** — a correctness bug.

## Why the live path cannot simply re-invoke the boot backfill

Boot is single-threaded before the reactor runs: no concurrent commits, no
concurrent ticks, no relay loop, no committer, and a fresh recovery has **no
running `pending_deltas`** (the per-table un-ticked-delta buffers). The live path
violates every one of those preconditions, and three concrete mechanisms make a
naive "call `fan_out_backfill` from the DDL handler" wrong:

1. **The tick delta is a buffer, not an LSN replay.** A worker tick sources its
   delta from an in-memory buffer: `handle_push` (`worker/mod.rs:1127`) ingests to
   base storage **and** appends to `self.pending_deltas[tid]`; `handle_tick`
   (`worker/mod.rs:1155`) *removes* (drains) that buffer and flows it through
   `evaluate_dag` (`worker/mod.rs:1532`) over the full dependent closure. There is
   **no per-view LSN filter** — `evaluate_dag` pushes the delta to every current
   dependent unconditionally. So a base row that is still in `pending_deltas` when
   the new view becomes a dependent will tick into the view, *and* the backfill —
   which reads committed base storage (`open_store_cursor`) — replays the same row.
   Double count. The cut between "delivered by backfill" and "delivered by tick"
   cannot be an LSN watermark on the tick path; it must be **positional**
   (SAL-order) plus an explicit **drain** of the sources' `pending_deltas` before
   the view is a dependent.

   Auto-tick (`committer.rs:520`) is threshold-gated (`TICK_COALESCE_ROWS`) and
   only *enqueues* `TickTrigger::Auto`; the DDL entry committer-barrier
   (`executor.rs:1304-1306`) flushes commits and is signalled *after*
   `commit_pushes` (`committer.rs:171-173`), never waiting for the downstream tick.
   So `pending_deltas` is routinely non-empty at `CREATE VIEW` time, and **the
   entry barrier does not drain it.**

2. **Holding the catalog write lock across a distributed backfill deadlocks.** The
   async exchange relay path (`relay_loop`, `executor.rs:553`) must take
   `catalog_rwlock.read()` to `prepare_relay` for every exchange round. The
   backfill's rounds *are* exchanges; the backfill cannot complete until its relays
   are serviced. If the DDL handler holds `catalog_rwlock.write()` across the
   backfill, `relay_loop`'s read acquisition blocks for the whole backfill →
   deadlock. (This is why the boot path's synchronous `collect_acks_and_relay`
   relays **inline** instead of via `relay_loop`.)

3. **A live backfill may not checkpoint the SAL the boot way.** `fan_out_backfill`
   calls `maybe_checkpoint` (`dispatch.rs:738`), and its `collect_acks_and_relay`
   (`dispatch.rs:363`) calls `self.sal.checkpoint_reset` (`dispatch.rs:400`) to
   reclaim SAL space mid-backfill for sources larger than the SAL. async-invariants
   §III.3a (`async-invariants.md:79-83`) forbids `maybe_checkpoint` from
   `fan_out_*_async` / `broadcast_ddl` / `tick_loop_async` paths: a concurrent
   `FLAG_FLUSH` from the committer races `checkpoint_reset` and orphans SAL writes
   straddling the reset, and a checkpoint between `FLAG_TICK` and
   `FLAG_EXCHANGE_RELAY` silently drops the relay (`async-invariants.md:91-99`).
   The committer is "the sole checkpoint driver"; only it and the boot backfill may
   reset.

The boot path is correct *because* it is stop-the-world. The live fix must
reconstruct that stop-the-world property for the duration of the backfill.

## Committed fix

Make `CREATE VIEW` of an exchange view a **quiescent, stop-the-world** operation on
the master: under the catalog write lock, drain the sources, register the view,
then drive the boot `fan_out_backfill` **synchronously** (inline relays, reactor
parked), and respond only after it completes. Workers are unchanged — they already
run `handle_backfill` (`worker/mod.rs:1189`) to completion before their next SAL
message, so no `FLAG_PUSH`/`FLAG_TICK` interleaves mid-backfill on a worker.

The exact sequence inside `handle_system_dml`, for a `target_id == VIEW_TAB_ID` DDL
that creates one or more `view_needs_exchange` views:

1. **Entry barrier** (existing, `executor.rs:1304-1306`) — flush the bulk of
   pending commits before taking the write lock, so the lock-held barrier in step 3
   is short.
2. **Acquire `catalog_rwlock.write()`** (existing, `executor.rs:1308`). The
   `AsyncRwLock` is writer-preference (`reactor/sync.rs:380`): once a writer is
   waiting, new readers park, and `write()` resolves only after in-flight readers
   drain. So holding it guarantees **no push can validate** (push validation takes
   `catalog_rwlock.read()`, `executor.rs:809`) and **no tick can emit**
   (`run_tick` takes the read lock, `executor.rs:498`). New `CommitRequest::Push`
   cannot be produced for the duration.
3. **Lock-held barrier** — issue a second committer barrier and await it. With the
   write lock held, the committer queue cannot grow (no new validated pushes), so
   this flushes every straggler that slipped in between steps 1 and 2. After it,
   the committer is idle and **stays** idle, so no `FLAG_PUSH` is written for the
   rest of the handler — including across the fsync `.await` in step 6.
4. **Drain `pending_deltas`** — for every dirty source table (the set the committer
   tracks in `tick_rows`; superset-safe to drain all dirty user tables), drive a
   tick **synchronously with inline relays** and await worker ACKs, so each
   worker's `handle_tick` empties `pending_deltas[source]` through the views that
   already exist. The new views are not yet registered, so this cannot feed them. A
   plain tick is a single exchange round; reuse the backfill's inline-relay
   collection generalized to one round (see *Implementation notes*). After this
   step, every source the new views read has an empty `pending_deltas`.
5. **Register the new views** — the existing mutate phase (`ingest_to_family` +
   `evaluate_dag(VIEW_TAB_ID, …)`, `executor.rs:1404-1411`), broadcast
   `FLAG_DDL_SYNC`, `commit_zone`, fsync, publish `ingest_lsn`
   (`executor.rs:1434-1475`). The views are now registered and durable on master
   and every worker, and are tick dependents of their sources — but no tick can
   reach them yet (write lock still held; `pending_deltas` drained in step 4).
6. **Synchronous backfill** — for each newly-created view with
   `dag.view_needs_exchange(vid)`, for each `source_id` in
   `dag.get_source_ids(vid)` that is a registered table, call the existing
   `fan_out_backfill(vid, source_id)` directly on the dispatcher
   (`(*disp_ptr).fan_out_backfill(...)`, exactly as `backfill_exchange_views`
   does). `fan_out_backfill` is synchronous and parks the reactor for its duration;
   that is the stop-the-world window. Skip non-exchange views (already backfilled
   inline by `hook_view_register` in step 5).
7. **Release the write lock and `send_ok_response`.** Queued and subsequent pushes
   now validate, commit, and tick through the populated views exactly once.

`CREATE VIEW` returns only after the backfill completes, matching the
non-exchange-view contract and SQL expectations: a query issued right after
`CREATE VIEW` sees the full result.

### Once-only correctness (no double-count, no gap)

Every base row reaches each new view exactly once. The cut is **positional**, not
an LSN watermark:

- A row committed **before** step 6's `FLAG_BACKFILL` is in the source's committed
  base storage, so the backfill's snapshot cursor (`open_store_cursor`, taken when
  the worker processes `FLAG_BACKFILL`) replays it into the view. It is **not** in
  any `pending_deltas` (drained in step 4, and the write lock blocked all writes
  since), so no tick re-delivers it.
- A row committed **after** the write lock is released (step 7) lands in
  `pending_deltas` *after* the worker has finished `handle_backfill` (the worker
  runs it to completion before the next SAL message, and `FLAG_BACKFILL` was
  written before any such `FLAG_PUSH`). It is not in the snapshot, and the normal
  tick delivers it once.

There is no in-between: between step 3 and step 7 the committer is idle and the
write lock blocks all SAL data writes, so no row can be both snapshotted and
buffered for a later tick. No per-view LSN gate is introduced or needed (none
exists on the tick path).

The drain in step 4 is the load-bearing addition the boot path does not need.
**It also closes a latent double-count for inline non-exchange views**: today
`backfill_view` runs in step 5 against base storage that may still hold rows whose
`pending_deltas` would later tick through the just-registered non-exchange view.
Draining before registration fixes both kinds in one place; the concurrent-insert
test below covers the non-exchange case as well.

### SAL reclamation under stop-the-world

`fan_out_backfill` → `collect_acks_and_relay` performs the boot reclamation
protocol (per-round pad bit, `BACKFILL_DECISION_CHECKPOINT`, `checkpoint_reset`)
for sources larger than the SAL. This is safe here for the same reason it is safe
at boot: the synchronous `collect_acks_and_relay` never `.await`s, so the reactor
is parked for the whole backfill — the committer task cannot run, hence cannot
issue a concurrent `FLAG_FLUSH`, and no other SAL writer exists. The §III.3a
prohibition targets *concurrent* async checkpointers; a stop-the-world backfill has
none. The entry + lock-held barriers (steps 1, 3) prove the committer queue is
empty before the park, so nothing is stranded by the park. This safety argument
must be stated at the call site and re-validated if the backfill is ever made to
`.await`.

### Implementation notes

- **No new async sibling.** The plan reuses the synchronous `fan_out_backfill`
  unchanged; it does **not** add `fan_out_backfill_async`. Stop-the-world removes
  the deadlock (no `relay_loop` dependency — `collect_acks_and_relay` relays
  inline) and the checkpoint hazard, which were the only reasons to go async.
- **Synchronous single-round tick for the drain.** `collect_acks_and_relay`
  (`dispatch.rs:363`) is currently backfill-specific (it reads per-round pad bits
  and stamps `BACKFILL_DECISION_*`). The drain needs the same inline-relay
  collection for a single steady-state tick round. Factor the inline
  collect-and-relay loop so it serves both: a normal tick is the degenerate
  "one round, no pad bit, `BACKFILL_DECISION_CONTINUE`" case. Driving the drain via
  the async `run_tick`/`relay_loop` path is **not** an option — it takes the read
  lock and would deadlock against the held write lock (step 2).
- **Dirty set.** The committer already tracks per-tid pending row counts
  (`shared.tick_rows`, `committer.rs:515`); reuse it (or the executor's
  `drain_tick_rows_into`, `executor.rs:147`) to choose which tables to drain in
  step 4.

## Cost

A live backfill of a large base table is O(base size) and **parks the reactor** for
that duration — all clients (scans, seeks, pushes) wait, not just writers. This is
the price of correctness for a rare DDL on a pre-alpha engine with no compatibility
constraints. If `CREATE VIEW` over very large tables later needs to stay
responsive, the evolution is a committer-coordinated pause (commits queued, ticks
paused, backfill under a read lock with `relay_loop` live) — a separate plan, since
it requires new quiescence primitives and a fresh §III.3a analysis.

## Edge cases

- **Non-exchange views** — still backfilled inline by `hook_view_register` in step
  5; the step-6 `view_needs_exchange` filter skips them. The step-4 drain newly
  guarantees their inline backfill is also once-only under concurrency.
- **Empty sources** — `fan_out_backfill` drains zero chunks and pads immediately;
  the view stays empty, correctly.
- **Multi-source views (joins)** — iterate every `get_source_ids(vid)` entry, as
  `backfill_exchange_views` does; each source is backfilled through the view's
  circuit, and the join's other-side trace is populated by that side's backfill
  round.
- **Replicated-source views** (single-partition, `has_replicated_source`,
  `catalog/hooks.rs:429`) — handled by the same `fan_out_backfill` the boot path
  already uses for them; no special case. Stop-the-world is what makes their
  broadcast/relay routing safe (no concurrent tick competes for relays).
- **Crash mid-backfill** — the view is ephemeral (`RelationKind::View ⇒
  Persistence::Ephemeral`; `backfill_view`/`handle_backfill` both assert it). The
  `CREATE` is durable (committed at step 5, before the backfill), so the view
  exists post-recovery and is rebuilt from scratch by the boot
  `backfill_exchange_views`. The backfill only *adds* to an empty ephemeral store —
  no half-applied state.
- **One view per statement** — single-statement `CREATE VIEW` registers exactly one
  view, so step 6 iterates one `vid`. Views-over-views are created in separate
  statements, and an exchange source-view is already populated (it was backfilled at
  its own `CREATE`), so `get_source_ids` returning the immediate source suffices; no
  cross-view ordering is required.

## Tests

- `crates/gnitz-py/tests/test_view_backfill_existing_data.py` — the two `xfail`
  tests (`test_vbf_join_over_populated_tables_sees_existing_data`,
  `test_vbf_group_by_over_populated_table_sees_existing_data`) become passing;
  remove the `xfail` markers when the fix lands. Extend with:
  - set-op (`UNION`/`EXCEPT`/`INTERSECT`) and `SELECT DISTINCT` views over committed
    data (all exchange views).
  - **concurrent-insert variant (guards the drain + positional cut):** load + commit
    data, then issue `CREATE VIEW` while interleaving fresh inserts to its sources;
    assert the view equals brute-force over the full base (backfilled snapshot +
    post-create deltas, each counted once). Run a non-exchange-view shape of this
    same scenario too, to guard the latent inline double-count the drain also fixes.
  - nested: a view over an already-created exchange view, both created after data;
    assert correctness.
  - a non-exchange view over committed data (control: must keep working).
- Engine integration test at `W=4`: assert per-source `fan_out_backfill` is invoked
  exactly once per new exchange view, and that a create-live view's row count equals
  the create-then-restart (boot-backfilled) reference.

## File-change summary

| File | Change |
|------|--------|
| `crates/gnitz-engine/src/runtime/orchestration/executor.rs` | In `handle_system_dml`, for `target_id == VIEW_TAB_ID`: add the lock-held barrier (step 3) and the synchronous `pending_deltas` drain (step 4) before the mutate phase; after fsync + `ingest_lsn` publish, for each newly-created `view_needs_exchange` view call `fan_out_backfill` per source (step 6); release the write lock and respond (step 7). All under the existing `catalog_rwlock.write()`. |
| `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs` | Factor the inline collect-and-relay loop out of `collect_acks_and_relay` so it serves both a backfill round and a single steady-state tick round (the drain); no new async function. |
| `crates/gnitz-engine/src/catalog/hooks.rs` | Document at the `!view_needs_exchange` guard (`hooks.rs:462`) that exchange views are backfilled by the live DDL handler (stop-the-world) and the boot path, not inline. |
| `crates/gnitz-py/tests/test_view_backfill_existing_data.py` | Un-`xfail` the two tests; add set-op/distinct/nested cases and the concurrent-insert variant (exchange and non-exchange). |
| `crates/gnitz-engine` integration tests | `fan_out_backfill` invoked once per source; create-live == create-then-restart. |

The boot path (`backfill_exchange_views`) and the synchronous `fan_out_backfill` /
`collect_acks_and_relay` are reused as-is; this adds the missing live trigger plus
the drain that the live (running-`pending_deltas`) world requires and that boot does
not.
