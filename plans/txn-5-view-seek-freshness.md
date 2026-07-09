# View seek freshness: seeks drain pending ticks like scans do

## Problem

A SCAN of a view is fresh; a SEEK of the same view can be stale. Before
reading, `handle_scan` unconditionally drains the pending-tick set —
looping `TickTrigger::Drain` until `tick_tids` is observed empty across a
drain — so views derived from recent pushes are caught up before the read
(`runtime/orchestration/executor.rs:1279-1310`). `handle_seek` performs no
such drain: it checks `has_id` and fans straight out
(`executor.rs:1016-1050`). Since the committer bumps `tick_rows`/`tick_tids`
*before* it ACKs a push (`committer.rs:682-694` precedes the `done` sends
at `:757-763`), the scan drain covers every commit the client has an ACK
for — read-your-writes causality holds for scans, cross-client included.
For seeks it silently does not:

```
client A: push to t            -> ACK (tick for t now pending)
client A: seek view v over t   -> may miss the push (tick not yet run)
client A: scan view v          -> always sees it (drain)
```

The stale path is reachable from both API layers: the raw client
(`GnitzClient::seek` with a view tid), and SQL — `SELECT` resolves views
(`bind/resolve.rs:56-57` "readable-relation (table or view)";
`resolve.rs:170` `resolve_table_or_view_id`), so a
`SELECT ... FROM v WHERE <pk> = k` that classifies as a PK seek reads the
view through `handle_seek`. Only DML and CREATE INDEX are base-table-only
(`resolve_base_table`, `resolve.rs:178-183`).

Base-table seeks have no such problem: base state is updated at push
apply time, not at tick time, so a drain would buy them nothing and cost
latency on the hot RMW read path. Secondary-index seeks cannot target
views at all (CREATE INDEX on a view is rejected at DDL precheck — the
view-index-owner rejection exercised by
`catalog/tests/atomicity_tests.rs`), so the plain PK seek is the only
stale surface.

## Change (committed)

`handle_seek` drains pending ticks **iff the target is a view**, using
verbatim the scan path's loop, with the scan path's lock ordering:

1. Classify **bare**, exactly as today's `has_id` check runs
   (`shared.cat().has_id(target_id)` takes no lock, `executor.rs:1025`;
   the classification is fully synchronous with no `await` between read
   and use, so on the single-threaded master it cannot tear against a
   DDL — no lock is added to the seek path): `has_id` (existing) and the
   new `relation_is_view(target_id)` — a one-line catalog accessor
   beside `table_has_unique_pk` (`catalog/metadata.rs:121-123`), reading
   the registered relation kind (`RelationKind::View`,
   `query/dag/mod.rs:112`, enum at `:103-113`; index-circuit-internal
   tables never appear in `dag.tables`, so they fail `has_id` before
   classification).
2. If a view: run the `handle_scan` drain loop verbatim
   (`executor.rs:1295-1310`) — snapshot `tick_tids`, send
   `TickTrigger::Drain`, await, repeat until observed-empty-and-stayed-
   empty. The loop runs with no lock held (the same BF-1 rule the scan
   path documents at `executor.rs:1290-1294`). Coverage is stronger
   than the snapshot suggests: the tick loop's `Drain` handling drains
   the **live** `tick_tids` and unions the snapshot on top
   (`drain_tick_rows_into`, `executor.rs:182-188, 543-556`), so every
   tid pending at drain time is ticked regardless of snapshot timing.
3. Fan out exactly as today (`fan_out_seek_async` for user relations).
   A DDL dropping the view between step 1's lock release and the fanout
   surfaces as the existing worker/table-not-found error — the same
   window every scan has between its drain and its fanout.

Base-table seeks take step 1's bare classification read and skip
straight to the fanout — no drain, no added await, no lock, one map
lookup of added cost.

System-table seeks (`target_id < FIRST_USER_TABLE_ID`,
`executor.rs:1044-1048`) are unchanged: the catalog is master-resident
and tick-independent.

Semantics after the change, stated as the contract: **any commit whose
ACK the client has observed is reflected in every subsequent scan or
seek of any view**, because the committer's tick bump precedes the ACK
and the drain covers all pending tids. Seek freshness equals scan
freshness; the remaining (identical) window — a push committing after
the drain loop exits — is the one scans already have.

## What explicitly does not change

- Base-table seek latency and behavior; secondary-index seek paths
  (`fan_out_seek_by_index_async` and the range variant — base-table-only
  surfaces); system-table seeks; the scan path; the tick loop; workers;
  the wire protocol (no new frames, no new flags).
- The SQL layer: it calls the same `client.seek`; freshness improves
  transparently wherever its access ladder seeks a view.

## Tests

**Rust engine:** `relation_is_view` classification (base table, view,
index-circuit-internal tids).

**E2E (pytest, `GNITZ_WORKERS=4`):**
- Read-your-writes via seek: push to `t`, then immediately `seek(v, k)`
  for a view `v` over `t` — the pushed row's effect on `v` is visible,
  with no interposed scan, repeated in a tight loop (this fails before
  the change whenever the auto-tick loses the race).
- Cross-client causality: A pushes and ACKs, signals B out-of-band, B
  seeks `v` — sees the push.
- SQL surface: `INSERT` into `t`, then `SELECT ... FROM v WHERE pk = k`
  (the PK-seek classification) reflects the insert.
- Base-table seek behavior unchanged: seek on `t` after a push returns
  the row exactly as today (and the test suite's existing RMW-heavy
  paths, which seek base tables in loops, show no regression).
- Seek on a view during a tick storm (concurrent pusher): every seek
  reflects all ACKed pushes at its drain point; weights conserved.
- Seek on a view that has never been pushed to (empty pending set): the
  drain loop's observed-empty fast path — single Drain roundtrip, then
  the normal reply.
