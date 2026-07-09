# Consistent multi-relation scan: one SAL cut across N relations

## Problem

Reading two relations consistently is impossible today. Each SCAN is an
independent request: `handle_scan` drains pending ticks, fans one relation
out to the workers, and streams the reply
(`runtime/orchestration/executor.rs:1279-1394`). Scanning `t1` then `t2`
is two point-in-time reads at two different instants — a write committed
between them (or concurrently) is visible in the later read and absent
from the earlier one. A client cannot take a snapshot of `orders` and
`inventory` that reflects one logical moment, and an atomic multi-table
commit (one SAL zone) can be observed *torn across two scans* even though
it was never torn in the store.

The engine's architecture makes the fix almost free:

- **Every scan snapshot is taken at one serial point.** The worker is
  single-threaded; dispatching a scan group calls `scan_family`, which
  materializes the partition's full committed state into an immutable
  `Rc<Batch>` at that dispatch instant; chunks then stream from the
  frozen batch across `drain_sal` passes while later SAL messages mutate
  only the live store (`worker/mod.rs:451-562, 1103-1110`). Dispatch of
  subsequent groups is not delayed by pending chunk emission
  (`worker/mod.rs:451-459` — one chunk of the front train per pass,
  other dispatch proceeds), so two consecutive groups snapshot at
  effectively adjacent serial points with nothing between them.
- **Every SAL writer serializes on one lock.** Scan fanouts, the
  committer's commit zones (groups + sentinel written under one hold),
  tick emission, relays, and DDL broadcasts all write under
  `sal_writer_excl` (`dispatch_scan_fanout`, `master/mod.rs:400-452`;
  committer Phase B, `committer.rs:564-629`). Consequently, N scan
  groups written **under a single lock hold** occupy consecutive SAL
  positions: no push group, no tick group, and — critically — no *part*
  of a commit zone can land between them. Each worker therefore
  snapshots all N relations at the same SAL cut, and a multi-table
  atomic commit is either entirely before the cut (visible in every
  snapshot) or entirely after (visible in none).

This plan adds the client-addressable version: **SCAN_MULTI**, one frame
naming N relations, answered by N standard scan reply trains taken at one
cut.

## Semantics (committed)

- **Instant consistency (one SAL cut).** All N relation snapshots are
  taken at the same SAL cut (the scan groups' consecutive SAL position,
  which every worker observes identically), and the state at the cut is
  the same deterministic function of the SAL prefix on every worker.
  Base tables reflect the exact prefix (Push dispatches inline in every
  worker context, `worker/mod.rs:650, 814`). A view may additionally lag
  the prefix by *deferred* ticks (a tick that arrives inside an exchange
  wait is deferred and replayed after the wait,
  `worker/mod.rs:651, 706-713`) — deterministically and identically on
  every worker, since the defer decision follows the shared log
  position. Atomic commits are never torn across the result set.
- **Derivation freshness is the same as today's scans, minus one
  documented window.** `handle_scan`'s tick-drain loop runs once, before
  the fanout, exactly as for a single scan (`executor.rs:1295-1310`). A
  push that commits between the drain's completion and the scan groups'
  SAL write is included in base-table snapshots but its view effects are
  not (its tick has not run) — so a SCAN_MULTI covering a base table and
  a view over it can show the base ahead of the view by exactly the
  writes of that window (and, per the previous bullet, by any deferred
  ticks). This is not a regression: a single view scan today has the
  same freshness (the view simply reflects the drain point); the
  multi-scan makes the base-vs-view lag *observable in one result set*.
  Instant consistency is unaffected — both snapshots sit at the same
  cut; the lag is in the DBSP derivation, not the read. The drain cannot
  be moved under the writer lock (`run_tick` needs `sal_writer_excl` per
  tick — holding it across the drain would deadlock), so this window is
  inherent and documented.
- **Per-relation results are byte-identical in shape to N single
  scans**: same chunk trains, same schema-negotiation behavior, same
  terminal frames carrying `last_tick_lsn`. A SCAN_MULTI of one relation
  is exactly a scan.

## Wire format

New request flag. In the live tree the top allocated bit is
`FLAG_DDL_TXN = 1 << 57`; bits 58 and 59 are claimed by the
transaction-frame and watermark work that precedes this plan in the
series, so SCAN_MULTI takes the next one (the compile-time disjointness
guard `flags.rs:83-118` gains the constant):

```rust
pub const FLAG_SCAN_MULTI: u64 = 1 << 60;
```

Request frame: control block (`target_id = 0`, `flags` carrying
`FLAG_SCAN_MULTI` and the schema-version bits of relation 0 — per-relation
versions ride the count section):

```
control block
u32 relation_count       LE, 1 ..= 64
per relation:
  u64 tid                LE
  u16 schema_version     LE (the client's cached version, 0 = none)
```

Shape rules, checked before any group is written: `1 <= count <= 64`
(`SCAN_MULTI: too many relations` above), every tid a user relation
(`tid >= FIRST_USER_TABLE_ID`, `has_id`; base tables **and views** are
legal — both are scannable relations; system tables are master-resident
and stay on the plain scan path), no duplicate tids
(`SCAN_MULTI: duplicate relation {tid}`).

Reply: **interleaved trains, demuxed by `target_id`.** Every scan reply
frame already carries its relation's tid in the header, so the master
forwards frames in *arrival* order and the client demuxes:

- first, one preliminary schema-only frame per cache-missing relation
  (all emitted up front — negotiation bytes and versions captured once,
  under the catalog read lock, before the fanout; the per-relation
  section's `schema_version` is the authoritative version field, the
  control block's version bits are unused and zero);
- then worker chunk frames (`FLAG_CONTINUATION`) of any relation in any
  interleaving — chunk order within a relation is irrelevant (batches
  concatenate, exactly as a single scan concatenates its N workers'
  trains today, `connection.rs:174-186`);
- one terminal frame per relation (`make_terminal_scan_frame`, that
  relation's tid + the shared `last_tick_lsn`,
  `executor.rs:1391-1394`), emitted when the master has drained all of
  that relation's worker trains. The request completes at the Nth
  terminal.

Arrival-order forwarding plus a **hybrid queue policy** is what keeps
the reply path inside a hard engine invariant: a queued/parked
`W2mSlot` pins its ring space and an in-flight entry until dropped
(`reactor/mod.rs:993-1020`), and the W2M in-flight accounting is
hard-capped at `W2M_MAX_IN_FLIGHT = 64` per ring — no capacity check on
`take()`, a release-build `assert!` on `release()`
(`runtime/protocol/w2m.rs:102-168`). Neither request-ordered parking
nor naive arrival-order queuing respects that cap: while the master
awaits a slow client's socket, the reactor keeps draining every worker
ring (`runloop.rs:81`), a worker emits its N trains gated only by ring
*bytes* (`w2m_ring.rs:447`), and a batch under the reply budget is one
frame of exactly its own size (`reply.rs:270-276`) — so frame *count*
per ring is unbounded by bytes alone, and 64 relations put the count at
the cap: silent `InFlightState` clobber, then abort. The committed
policy in `route_scan_slot` for multi-scan group ids:

- a frame `≤ SCAN_MULTI_COPY_THRESHOLD` is **copied** into the group
  queue as owned bytes and its slot released immediately;
- a larger frame's slot is **held** in the queue.

The threshold is derived from the ring, not from the reply budget:
`SCAN_MULTI_COPY_THRESHOLD = W2M_REGION_SIZE / 32` (32 MiB at the 1 GiB
ring, `w2m_ring.rs:72`). That makes the held-count bound an arithmetic
identity independent of every tunable: held slots pin their ring bytes,
each held frame exceeds `ring/32`, so **fewer than 32 can be
ring-resident simultaneously** — including under a shrunken debug
`GNITZ_REPLY_FRAME_BUDGET`, where frames simply fall below the
threshold and take the copy branch (a budget below the threshold holds
nothing at all). The copy branch's cost is one memcpy of ≤ 32 MiB per
mid-sized frame, acceptable on a deliberate bulk-snapshot op and
bounded in aggregate by the scan's result size (the same order the
client buffers anyway).

Two guards make the bound global rather than per-request:

- **one multi-scan at a time**: `handle_scan_multi` serializes on a
  master-side `AsyncMutex` (a concurrent second multi-scan would sum
  its held slots onto the same rings; serializing a bulk snapshot op
  is the honest cost of the hard cap), keeping total multi-scan-held
  slots per ring `< 32` with ≥ 32 slots of headroom for control
  traffic and single scans;
- a **per-ring held counter** on the reactor (incremented when the
  group branch of `route_scan_slot` enqueues a held slot for that
  worker's ring, decremented on release), with a
  `debug_assert!(held < 48)` at the enqueue site in
  `reactor/mod.rs`'s `route_scan_slot` — the group-branch analogue of
  the per-id assert at `reactor/mod.rs:1014`, which cannot see
  cross-relation aggregation. Debug builds may override the threshold
  (`GNITZ_SCAN_MULTI_COPY_THRESHOLD`, via the existing
  `debug_env_usize` mechanism) so tests can drive the held branch
  cheaply.

## Master handler

`handle_scan_multi` in `runtime/orchestration/executor.rs`:

1. Decode; enforce shape rules (catalog read lock for `has_id`).
2. Run the tick-drain loop once (verbatim the `handle_scan` loop,
   `executor.rs:1295-1310`; same lock-ordering caveat — the catalog read
   lock is taken only after the drain, BF-1).
3. Under the catalog read lock, resolve per relation: replicated shape
   (`replicated_unicast`, `master/mod.rs:371-378` — worker-0 unicast for
   replicated relations, broadcast otherwise) and schema version /
   preliminary-frame decision.
4. **One fanout, one lock hold**: a new
   `dispatch_scan_multi_fanout(disp_ptr, reactor, sal_excl, shapes)`
   generalizing `dispatch_scan_fanout` (`master/mod.rs:405-460`):
   allocate per-relation request-id sets (a broadcast relation gets `nw`
   ids, a unicast relation one mirrored id, exactly as today); register
   every id into **one multiplexed scan group** (below) before any
   await; then, under **one** `sal_excl.lock().await` hold, write all N
   scan groups back-to-back (each group encoded exactly as the
   single-scan submit closure writes it today — no SAL format change; a
   unicast group carries only worker 0's slot; all workers already skip
   groups with no slot for them, `worker/mod.rs:698-701`) and signal
   once (`signal_all`). The lock releases before any await.
5. **Multiplexed forwarding, arrival order.** The reactor gains a
   grouped registration: a `ScanGroupLease` owning an arbitrary id set
   (Vec-backed — the existing `ScanLease` stores at most `MAX_WORKERS`
   ids inline in a fixed array, `reactor/futures.rs:166-183`, and
   cannot hold N×nw ids) plus an arrival queue: `route_scan_slot`
   delivers any member id's frame into the group's queue under the
   hybrid copy/hold policy (wire-format section) instead of the per-id
   parking. The handler loop pops the next entry (owned bytes or held
   slot), parses its train header (`parse_train_header`,
   `master/mod.rs:462-489`), forwards the frame to the client, and
   releases any held slot; per-relation bookkeeping decides when to
   emit that relation's terminal frame. The bookkeeping lives on the
   `ScanGroupLease`: an `id → relation index` map (keyed by the slot's
   request id) and a per-relation `trains_remaining` counter
   initialized to the relation's id-set size (nw for broadcast, 1 for
   unicast), decremented on each `FLAG_SCAN_LAST` (a single-frame
   no-continuation reply counts as a length-1 train,
   `master/mod.rs:485-496`; every worker train ends in exactly one
   `FLAG_SCAN_LAST`, zero-row replies included, `reply.rs:240,275`).
   Lease drop
   deregisters all ids and discards queued and future slots — a
   mid-stream client death cancels the whole multi-scan. A worker fault
   frame or decode error on any relation terminates the whole request
   with a single `send_error` after the lease drop — the client
   discards partial results.

Worker: **zero changes.** Each of the N groups is an ordinary scan group
dispatched through the existing `SalMessageKind::Scan` arm; snapshots at
dispatch, FIFO chunk trains, per-request-id reply routing all behave
identically.

## Client API

```rust
impl Connection {
    // gnitz-core/src/connection.rs, beside scan()
    pub fn scan_multi(&mut self, tids: &[u64], cache: &mut LruCache<...>)
        -> Result<Vec<(Option<Arc<Schema>>, Option<ZSetBatch>, u64)>, ClientError>
}
impl GnitzClient {
    pub fn scan_many(&mut self, tids: &[u64]) -> Result<Vec<ScanTriple>, ClientError>
}
```

`scan_multi` sends the frame, then runs one receive loop that demuxes by
each frame's `target_id`: schema frames and continuation chunks
accumulate into the matching relation's slot (batch concatenation via
`extend_from_owned`, as the single-scan loop does,
`connection.rs:174-186`); a frame without `FLAG_CONTINUATION` is that
relation's terminal (LSN in `seek_pk`); the loop ends at the Nth
terminal. Results are returned in request order regardless of arrival
interleaving. The returned LSNs update `last_seen_lsn` as any scan reply
does.

Python: `client.scan_many([tid, ...])` returning a list of result sets,
same row decoding as `scan`. The SQL layer does not consume SCAN_MULTI
(a SQL `SELECT` targets one relation; joins are views), so this is a
client-API feature; SQL integration is deliberately absent.

## Error cases

| Case | Outcome |
|------|---------|
| empty list / count > 64 | `SCAN_MULTI: empty relation list` / `SCAN_MULTI: too many relations` |
| duplicate tid | `SCAN_MULTI: duplicate relation {tid}` |
| system tid / unknown tid | `SCAN_MULTI: {tid} is not a user relation` / `table {tid} not found` |
| worker fault / decode error mid-stream | single `send_error` frame; lease drop discards undrained trains; client discards partial results |

All shape errors are checked before any group is written (nothing to
clean up); mid-stream errors follow the existing single-scan fault
contract (`parse_train_header`, `master/mod.rs:462-489`).

## What explicitly does not change

- Single-relation scan, seek, and their handlers; the worker; the SAL
  group encodings; tick semantics; the committer.
- The plain scan's freshness contract — SCAN_MULTI inherits it verbatim,
  window and all.

## Tests

**Rust unit (gnitz-core):** frame codec (1, N, 64 relations; version
bytes); shape-rule rejections; the demux receive loop against a mocked
interleaved frame sequence.

**Rust engine (reactor):** the group branch of `route_scan_slot` —
copy/hold split at the threshold, held-counter increment/decrement,
`debug_assert` firing point; `ScanGroupLease` drop discards queued
owned frames and held slots and deregisters all ids.

**E2E (pytest, `GNITZ_WORKERS=4`):**
- **Torn-commit impossibility (the headline test):** one client commits
  atomic two-table transactions `{A: k→v_i, B: k→v_i}` in a tight loop
  (each pair in one commit); a second client hammers
  `scan_many([A, B])`; every result set must satisfy
  `A.v == B.v` — never a torn pair. Run long enough to cross checkpoint
  boundaries.
- Sequential-scan skew is real (control test): the same workload
  observed via two *single* scans does exhibit torn pairs (asserting the
  feature is actually load-bearing, not vacuous).
- Mixed shapes: one replicated relation + one hash-partitioned relation
  in a single `scan_many`; snapshot consistency holds (replicated read
  from worker 0's cut equals the partitioned cut).
- View + base: `scan_many([t, v])` where `v` derives from `t`, quiescent
  writer → base and view agree; under concurrent writes the documented
  window is tolerated (test asserts only instant-consistency invariants,
  e.g. the view never shows a row whose base row is absent *in the same
  result set* for an insert-only workload).
- Single-relation `scan_many([t])` equals `scan(t)` byte-for-byte
  (schema, rows, lsn).
- Error paths: empty list, 65 relations, duplicate, system tid, unknown
  tid; server healthy after each.
- Schema negotiation: cold cache (preliminary frames per relation), warm
  cache, and a stale version on one of N relations (only that relation
  re-negotiates); the schema bytes are the step-3 capture even when a
  concurrent DDL commits mid-stream (versions in the prelim and worker
  frames agree).
- **Maximum accepted width under load**: a 64-relation `scan_many` of
  small tables at `GNITZ_WORKERS=4` with a concurrent pusher; asserts
  completion and per-relation correctness.
- **Slow-reader stress (the in-flight-cap test)**: a 64-relation
  `scan_many` read by a deliberately stalling client while a pusher
  runs, with the debug threshold override set *below* the debug reply
  budget so frames land in the **held** branch (e.g. threshold 16 KiB,
  budget 64 KiB) — the exact shape that overflows `W2M_MAX_IN_FLIGHT`
  without the ring-derived bound; asserts completion, correctness,
  per-ring held count staying under the assert ceiling, and server
  health. A second variant with the threshold *above* the budget pins
  the all-copy degenerate case (zero held slots).
- Concurrent `scan_many` from two clients serialize (both complete
  correctly; the second's start is delayed, not rejected).
- Cancellation: client disconnects mid-stream; server stays healthy
  (group lease discards queued and future trains); subsequent scans
  work.
