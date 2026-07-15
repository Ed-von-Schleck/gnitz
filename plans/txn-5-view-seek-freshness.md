# View seek freshness: seeks drain pending ticks like scans do

## Problem

A SCAN of a view is fresh; a SEEK of the same view can be stale. Before
reading, `handle_scan` drains the pending-tick set — looping
`TickTrigger::Drain` until `tick_tids` is observed empty across a drain — so
views derived from recent pushes are caught up before the read
(`runtime/orchestration/executor.rs`, `handle_scan`, drain loop at 1642-1657).
`handle_seek` performs no such drain: it validates existence via
`reject_unknown_table` and fans straight out (`executor.rs`, `handle_seek`,
1163-1197). Since the committer bumps `tick_rows`/`tick_tids` **before** it
ACKs a push (`committer.rs`, `commit_pushes`: the tick bump at 794-806 is in
Phase C, strictly before `unit.resolve` at 871-873, which sends the `done`
that unblocks the client ACK in `handle_push` at `executor.rs:1134`), the scan
drain covers every commit the client has an ACK for — read-your-writes
causality holds for scans, cross-client included. For seeks it silently does
not:

```
client A: push to t            -> ACK (tick for t now pending)
client A: seek view v over t   -> may miss the push (tick not yet run)
client A: scan view v          -> always sees it (drain)
```

This is a general server-side read-your-writes gap, not a SQL-transaction
concern: it is independent of the client-buffered, base-table-only
read-your-own-writes of SQL `BEGIN/COMMIT` (a plain `SELECT` bypasses that
buffer by design). The stale path is reachable from both API layers:

- **Raw client:** `GnitzClient::seek` (`gnitz-core/src/client.rs:438-440`)
  takes an arbitrary numeric `table_id` with no relation-kind gating and sends
  `FLAG_SEEK` (`connection.rs:287-292`).
- **SQL:** `SELECT` resolves views (`gnitz-sql/src/bind/resolve.rs`,
  `Binder::resolve` → `client.resolve_table_or_view_id`, which falls through
  `TABLE_TAB` to `VIEW_TAB`), and the seek fast path in `execute_select`
  (`gnitz-sql/src/dml/select.rs:111-113`) calls `client.seek(tid, &pk)`
  whenever `try_extract_pk_seek_residual` (`dml/plan.rs`) finds the WHERE
  conjuncts fully bind the relation's PK columns to literals — **with no
  base-table-vs-view discrimination**. So `SELECT ... FROM v WHERE <pk> = k`
  over a view is dispatched as a real point-lookup `FLAG_SEEK`, not a scan.
  Point-lookup-by-key against a view is a primary materialized-view use case
  (precompute a join/aggregate, then look up one key), so this is a
  first-class surface, not an edge.

Only DML and CREATE INDEX are base-table-only (`resolve_base_table`,
`gnitz-sql/src/bind/resolve.rs`), which rejects a view with a precise error.

Base-table seeks have no staleness problem: base state is updated at push
apply time, not at tick time, so a drain would buy them nothing and cost
latency on the hot RMW read path — the OCC read-modify-write loop
(`gnitz-sql/src/dml/rmw.rs`, `RMW_MAX_ATTEMPTS = 4`) whose retries matter
precisely under write contention on the target table, exactly when that
table's own commits keep `tick_tids` non-empty and a drain would force a
full FLAG_TICK round-trip to all workers on every attempt. Secondary-index
seeks cannot target views at all: CREATE INDEX on a view is rejected at DDL
precheck (`catalog/write_path.rs`, `validate_index_registration`:
`if !entry.kind.is_base_table() { return Err("… is not a base table") }`;
exercised by `test_idx_tab_view_owner_rejected` in
`catalog/tests/atomicity_tests.rs`). So the plain PK seek is the only stale
surface.

### Why the current-code structure blocks the naive fix

The FLAG_SEEK dispatch acquires the catalog read lock and holds it through the
**entire** `handle_seek` call (`executor.rs`, dispatch, lines 971-983:
`let _g = shared.catalog_rwlock.read().await;` then `handle_seek(...).await`).
Running the scan drain loop inside `handle_seek` under that held lock would
**deadlock**: `handle_scan` acquires its read lock *after* the drain and
documents why (`executor.rs:1637-1641`) — the drain parks at `rx.await`;
`AsyncRwLock` is writer-preferring (`reactor/sync.rs:371-373`: a read acquire
returns `Poll::Ready` only when there is no active or waiting writer); a read
lock held across the parked drain blocks DDL writers **and** prevents
`tick_loop_async` from taking its own read lock in `run_tick`
(`executor.rs:733`) — a three-way deadlock (the "BF-1" invariant).

The fix therefore has to release the dispatch's lock before draining. The
classification (base vs. view) that decides *whether* to drain must read the
catalog, so it stays under the read lock: `Shared::cat()` is
`unsafe { &mut *self.catalog }` (`executor.rs:139`), and every reader
(`handle_scan`, `reject_unknown_table`) touches the catalog only under the
read lock — the disciplined, DDL-excluding choice, even though today's DDL
write path happens not to hold `&mut` across its awaits.

## Change

Two edits, both in `gnitz-engine/src/runtime/orchestration/executor.rs`. No
new catalog accessor, no wire-protocol, worker, tick-loop, or SQL-layer
change.

### 1. Extract the scan drain loop into a shared helper, with a watermark early-out

The drain loop is currently duplicated verbatim in `handle_scan` (1642-1657)
and `handle_scan_multi` (1786-1798); the seek fix would add a third copy.
Extract it once — and add a sound fast-path skip that also cheapens the two
existing scan callers. Place the helper next to `handle_scan`:

```rust
/// Drain every pending view tick, returning with NO catalog lock held.
///
/// Fast path: if `last_tick_lsn >= published()`, every published — hence every
/// ACKed — commit is already reflected in all views, so return without a drain.
/// Sound: `run_tick` snapshots `published()` into `last_tick_lsn` BEFORE any
/// await, atomically with the tid set it drains (executor.rs:723-726, "Snapshot
/// before any .await"), and the committer bumps `tick_tids` before it publishes
/// a zone LSN before it ACKs (committer.rs:794-806 precede 832 precede
/// 871-873). So `last_tick_lsn >= L` ⇒ L's tid was in some completed tick's
/// drained set ⇒ L is reflected. The reverse can under-report (one extra
/// drain), never over-report (a stale read). Cross-client causality is
/// preserved: any un-ticked published commit forces `last_tick_lsn <
/// published()`, so the full drain runs — including serializing behind an
/// in-flight auto-tick (which has not yet advanced `last_tick_lsn`).
///
/// Slow path: loop sending `TickTrigger::Drain` until `tick_tids` is observed
/// empty across a full drain, so a push landing mid-drain is also ticked (the
/// tick loop drains the live `tick_tids` via `drain_tick_rows_into` and unions
/// each Drain trigger's snapshot on top — executor.rs:211-216 and 676-689).
///
/// MUST be called with the catalog read lock RELEASED (BF-1, above). Under an
/// unbounded concurrent write storm the loop can iterate for the storm's
/// duration (each pass sees `tick_tids` non-empty); the caller's own ACKed
/// writes are covered after pass 1. View seeks confine this to views —
/// base-table seeks never call it. Same property `handle_scan` has today.
async fn drain_pending_ticks(shared: &Rc<Shared>) {
    if shared.last_tick_lsn.get() >= shared.lsn_alloc.published() {
        return;
    }
    loop {
        let snapshot: Vec<i64> = shared.tick_tids.borrow().clone();
        let was_empty = snapshot.is_empty();
        let (tx, rx) = oneshot::channel::<()>();
        shared.tick_tx.send(TickTrigger::Drain { tids: snapshot, done: tx });
        let _ = rx.await;
        if was_empty && shared.tick_tids.borrow().is_empty() {
            break;
        }
    }
}
```

Replace the inline loop in `handle_scan` (1642-1657) with
`drain_pending_ticks(shared).await;` and the inline loop in
`handle_scan_multi` (1786-1798) likewise, keeping each call site's one-line
rationale comment.

Do **not** fold in the committer's single-shot drain (`committer.rs`,
"Step 2 — DRAIN", ~381-384): it deliberately sends one Drain (not the loop)
because it holds pushes so `tick_tids` cannot grow, and its contract is to
flush view state for persistence — a different invariant. Leave it un-gated
by the watermark too.

### 2. Restructure `handle_seek` and drop the dispatch lock

Remove the read-lock acquisition from the FLAG_SEEK dispatch arm (lines
971-983) so `handle_seek` owns its locking — the other seek arms
(`FLAG_SEEK_BY_INDEX` at 985, `FLAG_SEEK_BY_INDEX_RANGE` at 1000) keep their
dispatch-level lock unchanged:

```rust
if flags & FLAG_SEEK != 0 {
    handle_seek(shared, peer, client_id, target_id,
                decoded.control.seek_pk, &decoded.control.seek_pk_extra,
                client_version).await;
    return;
}
```

Replace `handle_seek` (1163-1197) with a three-phase shape: classify under a
short read lock, drain iff view (lock released), then one serve path for base,
view, and system alike under a re-acquired lock. An uncontended read acquire
returns synchronously (`reactor/sync.rs:371-373`), so the second lock costs a
base/system seek nothing measurable and there is no reason to special-case it
or duplicate the fan-out:

```rust
async fn handle_seek(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    // Phase 1 — classify under a short read lock (cat() aliases the catalog;
    // the lock excludes a concurrent DDL writer). A user relation is a base
    // table or a view; `!is_base_table()` ⟺ view here because system tids take
    // the `< FIRST_USER_TABLE_ID` path in Phase 3. `is_some_and` ends the
    // `dag.tables` borrow before Phase 2's await (the idiom already used in
    // push_target_error, executor.rs:2373, and hooks.rs:403).
    let is_view = {
        let _g = shared.catalog_rwlock.read().await;
        if reject_unknown_table(shared, peer, client_id, target_id).await {
            return;
        }
        target_id >= FIRST_USER_TABLE_ID
            && shared.cat().dag.tables.get(&target_id).is_some_and(|e| !e.kind.is_base_table())
    };

    // Phase 2 — a view drains with NO lock held (BF-1); base/system skip it,
    // so the RMW hot path is untouched.
    if is_view {
        drain_pending_ticks(shared).await;
    }

    // Phase 3 — one serve path. Re-validate existence: a DDL may have dropped
    // the view during the drain (the same post-drain re-check handle_scan does
    // at 1658-1661). SEEK unicasts to one worker by PK hash
    // (partition_lsn.rs:155-156), so no replicated fork is needed.
    let _g = shared.catalog_rwlock.read().await;
    if reject_unknown_table(shared, peer, client_id, target_id).await {
        return;
    }
    if target_id < FIRST_USER_TABLE_ID {
        match unsafe { (*shared.catalog).seek_family(target_id, pk, seek_pk_extra) } {
            Ok((batch, _)) => {
                send_ok_response(shared, peer, target_id, batch.as_ref(), client_id, pk, client_version).await
            }
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
    } else {
        match MasterDispatcher::fan_out_seek_async(
            shared.dispatcher, &shared.reactor, &shared.sal_writer_excl,
            target_id, pk, seek_pk_extra,
        ).await {
            Ok(slot) => peer.send_slot_or_close(slot).await,
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
    }
}
```

Cost: base-table and system seeks pay one extra `FxHashMap` get
(`dag.tables.get` for the classify) and one extra uncontended (synchronous)
read-lock acquire — no drain, no await round-trip. View seeks pay the drain
(itself skipped by the watermark early-out when views are already fresh) plus
the second read-lock acquisition. A view seek drains the **global** pending
set, not just the target's source closure — identical to `handle_scan`.

## Semantics (the contract)

**Any commit whose ACK the client has observed is reflected in every
subsequent scan or seek of any view**, because the committer's tick bump
precedes the ACK (Problem, above) and the drain (or the equivalent watermark
early-out) covers all pending tids. Seek freshness now equals scan freshness;
the remaining (identical) window — a push committing after the drain returns —
is the one scans already have. Cross-client causality holds: `tick_tids` and
`last_tick_lsn` are shared master state, so B's drain covers A's ACKed tid
whether or not A's tick has already run. A single drain refreshes views at any
derivation depth: `run_tick` emits one FLAG_TICK per pending base tid and the
worker's `evaluate_dag_multi_worker` cascades each view's output onto its own
dependents until the queue drains, so multi-layer view chains are covered.

## What explicitly does not change

- Base-table and system-table seek behavior (no drain; identical fan-out /
  `seek_family`) and their effective latency; the secondary-index seek paths
  (`fan_out_seek_by_index_async` and the range variant — base-table-only
  surfaces, dispatch-level lock kept); the scan path's semantics (only its
  drain gains the sound early-out); the tick loop; workers; the wire protocol.
- The SQL layer: it calls the same `client.seek`; freshness improves
  transparently wherever its access ladder seeks a view.

## Tests

**E2E (pytest, `GNITZ_WORKERS=4`):**
- Read-your-writes via seek: push to `t`, then immediately `seek(v, k)` for a
  view `v` over `t` — the pushed row's effect on `v` is visible, with no
  interposed scan, repeated in a tight loop (fails before the change whenever
  the auto-tick loses the race).
- Cross-client causality: A pushes and ACKs, signals B out-of-band, B seeks
  `v` — sees the push. Exercises the watermark early-out's causality: B's read
  must not skip the drain while A's write is un-ticked.
- SQL surface: `INSERT` into `t`, then `SELECT ... FROM v WHERE pk = k` (which
  classifies as a PK seek) reflects the insert.
- Multi-layer view: a view over a view over `t` — seek the top view after a
  push, sees the effect (confirms the transitive tick cascade).
- Base-table seek unchanged: seek on `t` after a push returns the row exactly
  as today; the suite's existing RMW/OCC-heavy paths (which seek base tables
  in loops) show no regression.
- Seek on a view during a tick storm (concurrent pusher): every seek reflects
  all ACKed pushes at its drain point; weights conserved. Also exercises the
  slow-path drain loop under sustained writes.
- Empty / never-ticked view, and a replicated view (all sources replicated):
  seek returns the same result a scan of the same view would (empty-OK vs.
  row), no error, no divergence.
- No deadlock under concurrent DDL: run view seeks in a loop while a second
  client issues CREATE VIEW / DROP VIEW on unrelated relations — exercises the
  lock-release-drain-reacquire path against a live DDL writer (would hang if
  the drain ever ran under the read lock).

The freshness tests double as the correctness proof for the watermark
early-out: any staleness it introduced would surface as a missed push in the
read-your-writes and cross-client cases.
