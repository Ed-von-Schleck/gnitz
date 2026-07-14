# Consistent multi-relation scan: one SAL cut across N relations

## Problem

Reading two relations consistently is impossible today. Each SCAN is an
independent request: `handle_scan` (`runtime/orchestration/executor.rs:1614`)
drains pending ticks, fans one relation out to the workers, and streams the
reply. Scanning `t1` then `t2` is two point-in-time reads at two different
instants — a write committed between them (or concurrently) is visible in the
later read and absent from the earlier one. A client cannot take a snapshot of
`orders` and `inventory` that reflects one logical moment, and an atomic
multi-table commit (one SAL zone, `FLAG_PUSH_TXN`) can be observed *torn across
two scans* even though it was never torn in the store. This is the read-side
completion of the atomic multi-table *write* story shipped in the txn series.

The engine's architecture makes the *snapshot* nearly free; the reply path costs
a small worker tweak (below):

- **Every scan snapshot is taken at one serial point.** The worker is
  single-threaded; dispatching a `Scan` group runs its `dispatch_inner` arm
  (`worker/mod.rs:853-863`), which calls `Catalog::scan_family`
  (`catalog/store_io.rs:30-37`) → `StoreHandle::full_scan`
  (`query/dag/store_handle.rs:101-106`), materializing the partition's full
  committed state into an immutable `Rc<Batch>` **at that dispatch instant**.
  Chunks then stream from that frozen `Rc` across later `drain_sal` passes
  (`worker/reply.rs` `emit_pending_scan_chunk`, 350-426) while subsequent SAL
  messages mutate only the live store. `drain_sal` (`worker/mod.rs:396-406`)
  dispatches *every* currently-available SAL message in one pass — a second
  `Scan` group's `scan_family` snapshot is taken back-to-back with the first,
  gated only by the first group's cheap dispatch, never on its chunk stream
  draining.
- **Every SAL writer serializes on one lock.** `sal_writer_excl`
  (`Rc<AsyncMutex<()>>`, created `executor.rs:262`, field `executor.rs:98`) is
  acquired by the committer, tick emission, relays, DDL broadcasts, and every
  fan-out op. Each critical section holds it with **no `.await` inside**
  (`dispatch_scan_fanout` `master/mod.rs:210-221`; committer Phase B
  `committer.rs:633-737`), and the reactor is single-threaded and cooperative —
  so a section that writes several SAL groups under one hold cannot be
  preempted, and the groups land at consecutive SAL positions (`write_cursor`
  advances per group, `sal.rs:812/852`, protected solely by this lock). N scan
  groups written under one hold occupy consecutive SAL positions: no push group,
  no tick group, and no *part* of a commit zone can land between them. Each
  worker snapshots all N relations at the same SAL cut, and a multi-table atomic
  commit is either entirely before the cut (visible in every snapshot) or
  entirely after (visible in none).

The existing multi-group precedent is `commit_pushes` (`committer.rs:522`, lock
block 633-737), which writes N family groups + a sentinel under one hold, then
collects their reply futures after releasing the lock. This plan adds the
read-side sibling: **SCAN_MULTI**, one frame naming N relations, answered by N
standard scan reply trains taken at one cut.

## Design rationale: what is and isn't new

- **No new serialization lock.** `sal_writer_excl` already serializes every SAL
  writer; holding it across N scan-group writes both forces the one cut and, as
  a side effect of mutual exclusion, prevents two concurrent multi-scans from
  interleaving submits.
- **No MVCC / read-at-LSN alternative** exists and none can: destructive
  consolidation (§2) keeps no history, so the only point-in-time coherence the
  engine offers is "now, at this worker's SAL position" — exactly what N
  consecutive SAL positions capture. An **OCC alternative** (read each relation,
  re-read `commit_lsn_of` / `published()`, retry on advance — `executor.rs:190`)
  is rejected: it gives only detection+retry, not a snapshot, and **livelocks**
  under the headline workload (a tight atomic-commit loop). Structural
  one-cut is the only mechanism that delivers torn-commit *impossibility*.
- **One small worker change is required** (not "zero worker changes"): a
  multi-scan's relations must reach each worker's reply ring in **request
  order**, which the immediate-emit fast path for small relations violates (see
  *Reply path*). A per-group flag routes them through the existing FIFO
  `pending_streams` instead. Everything else on the master and client reuses the
  single-scan reply path verbatim.

## Semantics (committed)

- **Instant consistency (one SAL cut).** All N snapshots are taken at the same
  SAL cut (the scan groups' consecutive SAL position, observed identically by
  every worker), and the state at the cut is the same deterministic function of
  the SAL prefix on every worker. Base tables reflect the exact prefix (Push
  dispatches inline in every worker context, `worker/mod.rs:609-621`, applying
  via `handle_push` → `ingest_returning_effective`, `worker/mod.rs:898-919`).
  Atomic commits are never torn across the result set.
- **View freshness = today's single-view scans, with a per-worker deferred-tick
  skew.** The tick-drain loop runs once before the fanout
  (`handle_scan`, `executor.rs:1630-1645`). A view may lag the cut by (a) the
  window between the drain and the scan-group write (a push in that window is in
  base snapshots but its tick has not run — same as a single view scan today,
  which reflects the drain point), and (b) *deferred* ticks: a `Tick` arriving
  while a worker is inside `do_exchange_wait` is deferred and replayed at top
  level (`worker/mod.rs:526-534`; `worker/exchange.rs:109-124`), whereas `Scan`
  dispatches inline in both contexts (`worker/mod.rs:479,621`). So during a
  worker's exchange-wait window one worker can snapshot a view at its
  pre-deferred-tick state while another snapshots post-tick — a per-worker skew
  of at most one tick, **pre-existing for single-relation view scans** and not a
  torn *base-table* commit. Instant consistency of base tables is unaffected;
  the multi-scan merely makes any base-vs-view derivation lag observable in one
  result set. This window is inherent (the drain cannot move under the writer
  lock — `run_tick` needs it per tick — so holding it across the drain would
  deadlock) and documented.
- **Per-relation results are byte-identical in shape to N single scans**: same
  chunk trains, same schema-negotiation, same terminal frame carrying
  `last_tick_lsn`. A SCAN_MULTI of one relation is exactly a scan.

## Wire format

New request flag. The disjointness guard (the `high_flags` array literal,
`gnitz-wire/src/flags.rs:116-131`, inside the `const _: () = {…}` block 111-139)
must gain the constant. Bits 57–61 are already allocated (`FLAG_DDL_TXN`=57,
`FLAG_ALLOCATE_TABLE_ID`=58, `FLAG_ALLOCATE_SCHEMA_ID`=59,
`FLAG_ALLOCATE_INDEX_ID`=60, `FLAG_PUSH_TXN`=61); bit 63 is the sign bit, left
clear; so SCAN_MULTI takes **bit 62**:

```rust
/// SCAN_MULTI request flag. Client→master frame naming N relations to snapshot
/// at one SAL cut. Wire-level routing hint consumed at `handle_message`; never
/// written to the SAL. Bit 62 — the next free high client-only bit above
/// `FLAG_PUSH_TXN` (61); 63 is the sign bit, left clear.
pub const FLAG_SCAN_MULTI: u64 = 1 << 62;
```

Add `FLAG_SCAN_MULTI` to the `high_flags` array so the compile-time guard covers
it.

Request frame: control block (`target_id = 0`, `flags` carrying `FLAG_SCAN_MULTI`;
the control block's schema-version bits 24–39 are unused and written zero —
per-relation versions ride the count section), followed by a trailing body:

```
u32 relation_count       LE, 1 ..= 16
per relation:
  u64 tid                LE
  u16 schema_version     LE (the client's cached version, 0 = none)
```

Encode/decode follow the existing transaction-frame idiom in
`runtime/protocol/wire.rs:892-991` (`txn_frame_prologue` / `decode_push_txn` —
control-block prologue, then `codec::read_u32_le` count, then a bounds-checked
per-record loop). `codec` has `read_u32_le`/`read_u64_le` but **no
`read_u16_le`** — read the `schema_version` with `u16::from_le_bytes`. Decode
lives in a new `decode_scan_multi(data) -> Result<Vec<(u64,u16)>, _>` there;
routing is a new arm in `handle_message` (`executor.rs:838`), sibling to the
`FLAG_PUSH_TXN` (922-925) and `FLAG_DDL_TXN` (913-916) arms.

Shape rules, checked before any group is written: `1 <= count <= 16`; every tid a
user relation (`tid >= FIRST_USER_TABLE_ID` and `shared.cat().has_id(tid)` under
the catalog read lock — base tables **and** views are legal; system tables stay
on the plain path); no duplicate tids. `16` is a product / master-state limit
(N `ScanLease`s + N trains of bookkeeping), not a per-frame ring-safety bound —
see *Reply-path safety*.

Reply: **N sequential trains, one relation fully before the next**, each shaped
exactly as a single scan's reply and drained by the master in request order:

- an optional preliminary schema-only frame (master→client, `FLAG_CONTINUATION`
  + the relation's `server_version`, only when
  `wire_should_include_schema(client_version, server_version)` — the same gate
  and captured bytes as `handle_scan`, `executor.rs:1658-1686`);
- then that relation's worker chunk frames (`FLAG_CONTINUATION`), forwarded
  verbatim, drained from the workers in ascending worker order;
- then one terminal frame (`make_terminal_scan_frame`, `executor.rs:1715-1718` —
  the relation's tid + the shared `last_tick_lsn`, flags = 0), master→client.

Terminal and preliminary schema frames are sent master→client directly
(`peer.send_buffer`), not through the W2M ring, so they consume no ring slots.
The client reads the N trains positionally (train `i` = relation `i` in request
order), reusing the single-scan receive loop N times.

## Worker change: FIFO reply ordering for multi-scan groups

`send_scan_response` (`worker/reply.rs`) has **three** emit paths, two of which
emit *immediately* during the dispatch pass and one of which queues:

- **non-wire-safe** (any German-string/TEXT column, `is_wire_safe` false via
  `schema_wire_safe`, `protocol/sal.rs:363-368`): builds one frame with the
  blob-capable `encode_wire_into` and `send_encoded`s it **immediately**, then
  returns (`reply.rs:243-275`);
- **wire-safe single-frame** (fits the budget): emits immediately (`reply.rs:284-291`);
- **wire-safe multi-chunk**: queues in `pending_streams` (FIFO, one chunk per
  later `drain_sal` pass via `emit_pending_scan_chunk`, the columnar
  `encode_wire_into_range`).

For a multi-scan these paths **reorder** relations on the ring:
`[rel0(large-wire-safe), rel1(tiny)]` emits rel1 first (immediate) and rel0's
chunks later, so ring order ≠ request order. Draining in request order then pins
the front on frames drained last → the worker blocks in `send_encoded` on a full
ring while the master waits for rel0's later chunks → **deadlock** (the exact
hazard `worker/mod.rs:173-183` documents). TEXT relations make this the common
case, not a corner: a non-wire-safe relation *always* emits before any queued
wire-safe relation, so it wedges whenever a wire-safe relation the master must
drain after it is large enough to fill a ring.

Fix: route **every** multi-scan relation through `pending_streams` so ring order
= request order, and extend the queue to carry the non-wire-safe case:

- Make `PendingScan` an enum (or add a discriminant) with a **non-wire-safe
  single-frame** variant holding the frozen `Rc<Batch>` + `request_id`,
  `client_id`, `target_id`, `prebuilt_schema`, `server_version` (exactly what the
  immediate branch consumes — the `SchemaDescriptor` is not load-bearing once a
  prebuilt block is present, so it need not be stored, matching how the wire-safe
  pending path already drops it). `emit_pending_scan_chunk` dispatches on the
  variant: non-wire-safe → `encode_wire_into` (blob-capable), single frame,
  always pop; wire-safe → the existing chunk logic. Flags are byte-identical to
  the immediate branch: `wire_flags_set_schema_version(0, server_version) |
  FLAG_CONTINUATION | FLAG_SCAN_LAST`, schema block emitted iff `prebuilt_schema`
  is `Some`. Keep the `wire_sz > MAX_W2M_MSG` reject (`reply.rs:247-253`) in
  `send_scan_response` **before** enqueue — `emit_pending_scan_chunk` returns no
  `Result`, so an oversized (>256 MiB) TEXT partition must error at enqueue time,
  not emit time. Only the encode moves to emit time.
- The wire-safe single-frame case reuses the existing chunked path
  (`enqueue_stream`): a one-row result is a one-chunk train — its single chunk is
  already a ≤budget frame with those exact flags, so nothing new is exercised
  (`reply.rs:390-398`). This is the battle-tested path every large single scan
  already uses; that is why the change is low-risk, not because the reply path is
  fragile.

Gate all three on a `force_fifo: bool`. Its signal is a scan-group flag
`FLAG_SCAN_FIFO_REPLY`. The wire-flag space is full (bits 48–62 all allocated,
63 is the sign bit), so `FLAG_SCAN_FIFO_REPLY` **reuses bit 62** —
`FLAG_SCAN_MULTI`'s value — which is safe by frame direction: bit 62 on a
client→master frame is the SCAN_MULTI *request* (consumed at `handle_message`,
never propagated into a SAL group), and on a master→worker scan group it is the
FIFO-reply *directive*. It is **not** added to the `high_flags` disjointness
guard (that would trip the collision assert). `dispatch_scan_multi_fanout` sets
it on every group's `wire_flags`; the full u64 survives to the worker
(`encode_ctrl_block` stores all 8 bytes, `control.rs:254` → read back as
`ctrl_wire_flags`, `worker/mod.rs:660`), so the `Scan` dispatch arm
(`worker/mod.rs:853-863`) threads `force_fifo = ctrl_wire_flags & FLAG_SCAN_FIFO_REPLY
!= 0` into `send_scan_response`. Single scans (flag clear) keep all three paths,
including the immediate-emit jump-ahead latency benefit. The dispatch matrix is
untouched — only the reply emission path changes.

Schema-skip via `effective_client_version` is preserved on the FIFO path: the
first chunk emits the schema block iff `wire_should_include_schema(client_version,
server_version)` (`reply.rs:230`), which is false when the master already
captured the schema (`effective = server_version`), so no duplicate schema.

With this, each worker streams a multi-scan's relations in strict request order
(rel0's whole train, then rel1's, …), exactly as it already streams one relation
across `drain_sal` passes.

## Master handler: `handle_scan_multi`

New async handler in `executor.rs`, routed from the `FLAG_SCAN_MULTI` arm of
`handle_message`.

**Phase 0 — decode + drain.** Decode via `decode_scan_multi`; enforce the
count/dup shape rules (tid legality is checked in Phase 1 under the catalog
lock). Run the tick-drain loop once, verbatim from `handle_scan`
(`executor.rs:1630-1645`); the catalog read lock is acquired only *after* the
drain (BF-1, `executor.rs:1625-1629`). Capture `let lsn =
shared.last_tick_lsn.get();` after the drain — the shared LSN stamped into every
terminal.

**Phase 1 — capture shapes + schemas + write N groups.** Take the catalog read
lock. For each relation resolve, from the catalog snapshot (so all N are
consistent with the cut even if a DDL commits during the later drain):

- tid legality (`has_id`, `>= FIRST_USER_TABLE_ID`) → `SCAN_MULTI: {tid} is not a
  user relation` / `table {tid} not found`;
- fan-out shape via `replicated_unicast(disp_ptr, tid)` (`master/mod.rs:140-147`):
  `0` (worker-0 unicast) for a replicated relation, `-1` (broadcast) otherwise;
- schema negotiation: `server_version = cat().get_schema_version(tid)`,
  `include_schema = wire_should_include_schema(client_version_i, server_version)`;
  if including, capture the schema wire block (`get_schema_wire_block`,
  `executor.rs:155`) for the Phase-2 preliminary frame and set that relation's
  `effective_client_version = server_version` (so its workers omit their schema
  blocks); else `effective_client_version = client_version_i`.

Then, nested inside the catalog read lock (the catalog-read ⊃ `sal_writer_excl`
order `handle_scan` establishes; every SAL writer takes catalog-then-SAL or SAL
alone, so no inversion — `executor.rs:2277` documents it), call a new
`dispatch_scan_multi_fanout`. This is **new code (~50 lines)**, not a
generalization of `dispatch_scan_fanout` (which locks per call, `master/mod.rs:211`,
so calling it N times reopens the lock and destroys the one cut). It follows
`commit_pushes`'s "N groups under one hold" shape: for each relation allocate its
request-id set (`nw` ids for broadcast, one for unicast — the private
`alloc_scan_request_id`/`SCAN_REQ_ID_FLAG` machinery, `reactor/mod.rs:537/50`,
is reached the same way `dispatch_scan_fanout` reaches it) and register it into
its own `ScanLease` (`reactor.scan_lease(&ids)`, `reactor/mod.rs:580`) *before*
any await; then under **one** `sal_excl.lock().await` hold write all N scan
groups back-to-back via `write_group_with_req_ids` (`master/dispatch.rs:110-149`)
— each with `wire_flags = wire_flags_set_schema_version(0, effective_client_version_i)
| FLAG_SCAN_FIFO_REPLY`, the captured schema block, its `unicast`, and its
req_ids — then `signal_all()` once; the lock releases at block end before any
await. Return a `Vec` of per-relation `(first_slots, req_ids, ScanLease)`
(N leases — `ScanLease`'s inline `[u32; MAX_WORKERS]` holds one fan-out's ids,
`reactor/futures.rs:159-163`). Release the catalog read lock after this block:
Phase 2 touches no catalog state, the snapshot is worker-frozen, and the schemas
are captured, so releasing early avoids blocking DDL for the whole bulk read.

**Phase 2 — sequential per-relation drain (no locks).** For each relation `i` in
request order:

1. If `include_schema_i`, send its preliminary schema frame (`peer.send_buffer`,
   `FLAG_CONTINUATION` + `server_version_i` + captured block); on a negative rc,
   drop the leases and return.
2. For each worker slot of relation `i` in ascending worker order, call
   `drain_scan_train(reactor, peer, slot, req_id, worker)`
   (`master/dispatch.rs:1498-1522`), exactly as `fan_out_scan_async` does
   (1166-1173). **This fn is currently a private free fn in `mod dispatch`;
   make it `pub(crate)` so `handle_scan_multi` can call it.** A `false` return
   (client disconnect) or `Err` (worker fault / malformed train) drops all
   leases (discarding undrained frames at the ring boundary so no worker wedges)
   and terminates the request — on a fault, one `send_error` after the lease
   drop; the client discards partial results.
3. Send relation `i`'s terminal (`make_terminal_scan_frame(tid_i, client_id, lsn)`,
   `peer.send_buffer_or_close`).

All leases are held for the whole of Phase 2 (a `Vec<ScanLease>`); dropping them
on return deregisters every id and discards queued/future slots, cancelling the
multi-scan on client death.

Because the worker streams multi-scan relations in request order (the FIFO flag),
draining relation `i` before `i+1` matches ring order: relation `i`'s frames are
at the front of each ring and released in order as the master forwards them, so
`consume_cursor` advances and no train wedges. This is the FIFO invariant's
supported usage — the master drains one request's train at a time.

## Reply-path safety: the shared per-ring in-flight budget

With request-order draining the design is deadlock-free (above). The remaining
concern is the per-ring W2M in-flight cap. Each parked (drained-but-unforwarded)
scan frame pins one entry in the ring's single `InFlightState` (`w2m.rs:109-119`,
one per worker, shared by **all** req_ids on that ring), hard-capped at
`W2M_MAX_IN_FLIGHT = 64` (`w2m.rs:107`): `take()` (`w2m.rs:134`) has **no
capacity check** (a 65th slot wraps the queue index, clobbering slot 0), and
`release()` advances only the consecutive completed prefix (`w2m.rs:154`,
`trailing_ones`) under a release-build `assert!(bit < 64)` (`w2m.rs:146`) that
**aborts the process** on overflow. Non-scan replies (exchange/ACK) release
immediately (`decode_slot_owned`), so the front is pinned by the oldest
un-forwarded *scan* frame and every slot opened behind it counts.

The real per-ring invariant is therefore **Σ over that ring's relations of
per-worker frame count `< 64`** — not "one frame per relation." A `count <= 16`
cap does **not** bound that sum directly: a single large relation chunks into
many frames. What bounds it in production is **byte-backpressure**: at the 256
MiB reply budget (`MAX_W2M_MSG`, `w2m_ring.rs:77`) the 1 GiB ring
(`w2m_ring.rs:72`) holds ~4 frames, and FIFO emission means only the *one*
relation currently mid-chunk is large — so per ring the sum is ≤ (≤4 for that
relation) + (one frame each for ≤15 already-emitted tiny relations) ≈ 19 < 64.
Tiny single-frame relations never trigger byte-backpressure (`try_reserve` is
byte-only, `w2m_ring.rs:442-449`), so the `count` cap is what bounds *their*
contribution.

`N <= 16` is thus a **product / master-state limit** (the master holds N
`ScanLease`s and N trains of bookkeeping; 16 covers realistic consistent
snapshots — a handful of related tables), and *incidentally* keeps a single
multi-scan's parked-frame contribution equivalent to ≤16 concurrent single scans,
so a multi-scan cannot overflow the ring on its own (16 < 64). It is **not** a
correctness bound on the frame sum; that safety is byte-backpressure of large
chunks. A test that shrinks `GNITZ_REPLY_FRAME_BUDGET` (to force chunking without
a huge table) **defeats byte-backpressure** and must therefore keep the total
frame sum per ring under 64 (a `big` relation of ~60 per-worker small frames +
tiny siblings would overflow) — see Tests.

A **reactor drain gate is explicitly rejected.** `drain_w2m_for_worker`
(`reactor/mod.rs:946-964`) is the single FIFO drain for *all* reply kinds, and
`read_cursor` is strictly monotonic. Gating it on in-flight depth would leave an
exchange-output frame (`FLAG_EXCHANGE` → `exchange_acc`, `reactor/mod.rs:1036`)
parked *behind* scan frames unreachable — the exchange round never completes, a
worker spins in `do_exchange_wait`, and view maintenance freezes cluster-wide
until client eviction. Today the reactor reads *past* parked scan frames
(advancing `read_cursor` independently of `consume_cursor`) and routes exchange
immediately; a gate would convert overflow into a routine cluster stall. Copying
frames out is likewise rejected: a copied frame released behind a still-parked
one cannot advance `consume_cursor`, so it does not bound in-flight, and copy-all
regresses single-scan throughput. Because parked multi-scan frames are tiny
(bytes), they do not byte-backpressure the ring, so concurrent exchange/ACK
traffic is read past them normally — no new priority inversion beyond the
byte-backpressure a single scan already produces.

> **Pre-existing latent overflow (independent of this feature).** The
> `InFlightState` abort is reachable **today** via single scans — e.g. 64
> concurrent stalled clients scanning replicated tiny tables (all single-sourced
> from worker 0), or one scan with a shrunk reply budget parking >64 small
> frames. Its correct fix cannot gate the shared drain (exchange starvation,
> above); the sound fix is to let `InFlightState` **grow past 64** (its 64-slot
> `u64` bitmap is the only reason it aborts; in-flight is ultimately byte-bounded
> by the ring). That is a small, localized `w2m.rs` change worth landing **first**
> on its own merits, and it removes the ring-frame-sum constraint on `N`
> entirely, leaving `N`'s cap a pure product limit. This plan is production-safe
> standalone (byte-backpressure + the `count` cap); the growth fix makes it robust
> under any reply budget and retires the CREATE-UNIQUE-INDEX preflight frame-size
> workaround (`worker/mod.rs:1460-1472`) that exists only to keep a full ring
> under 64 frames.

Worker: no changes beyond the `FLAG_SCAN_FIFO_REPLY` reply-path branches above.
Each group is an ordinary `Scan` group; snapshot-at-dispatch, per-request-id
routing, and unicast-slot skipping (`worker/mod.rs:519-522`) are unchanged.

## Client API

`gnitz-core` has no `Connection` type — the session object is `Session`
(`connection.rs:76`, re-exported as `Session`), and `GnitzClient` (`client.rs`)
delegates to `self.session`.

```rust
// gnitz-core/src/connection.rs, beside Session::scan (228-232)
pub type MultiScanResult =
    Result<Vec<(Option<Arc<Schema>>, Option<ZSetBatch>, u64)>, ClientError>;

impl Session   { pub fn scan_multi(&mut self, tids: &[u64]) -> MultiScanResult }
impl GnitzClient { pub fn scan_many(&mut self, tids: &[u64]) -> MultiScanResult }
```

`scan_multi` encodes the SCAN_MULTI frame (one `schema_version` per tid via
`self.schema_cache.peek(&tid)`, the `Session` field at `connection.rs:84`,
mirroring `versioned_flags`, `connection.rs:388-391`), sends it, then runs the
single-scan receive loop (`recv_scan`, `connection.rs:356-378`) **N times in
request order**: each call accumulates that relation's schema (first frame
carrying one) and batches (`extend_from_owned`) until a frame without
`FLAG_CONTINUATION`, whose `seek_pk` is the relation's LSN, and updates
`schema_cache` exactly as `scan` does. Results are returned in request order. No
`target_id` demux is needed — trains arrive sequentially. Do **not** touch
`last_seen_lsn`: `scan` never updates it (only push/txn paths do, via `track_lsn`,
`client.rs:283`); `scan_multi` matches `scan`. The tuple matches the existing
`ScanResult` inner shape (`connection.rs:27`); there is no `ScanTriple` type and
no external `cache` parameter.

Python: `client.scan_many([tid, ...])` returning a list of result sets, same row
decoding as `scan` (sync binding beside `gnitz-py/src/lib.rs:1655-1661`; async
beside `2252-2255`). The SQL layer does not consume SCAN_MULTI (a `SELECT`
targets one relation; joins are views), so SQL integration is deliberately
absent.

## Error cases

| Case | Outcome |
|------|---------|
| empty list / count > 16 | `SCAN_MULTI: empty relation list` / `SCAN_MULTI: too many relations` |
| duplicate tid | `SCAN_MULTI: duplicate relation {tid}` |
| system tid | `SCAN_MULTI: {tid} is not a user relation` |
| unknown tid | `table {tid} not found` |
| worker fault / decode error mid-stream | single `send_error`; all leases dropped (discards undrained trains); client discards partial results |

Shape/tid errors are checked before any group is written. Mid-stream faults
follow the single-scan contract (`parse_train_header`, `master/mod.rs:254-266`;
`drain_scan_train` returns `Err`/`false`).

## What explicitly does not change

- Single-relation scan/seek and their handlers; the SAL group encodings; tick
  semantics; the committer; the worker dispatch matrix.
- `drain_scan_train` (beyond `pub(crate)`), `recv_scan`, `make_terminal_scan_frame`,
  `ScanLease`, `route_scan_slot`, the reactor drain — all reused verbatim.
- The single-scan immediate-emit fast path (multi-scan opts out via the group
  flag; single scans keep it).
- The plain scan's freshness contract — SCAN_MULTI inherits it, window and all.

## Tests

**Rust unit (gnitz-core):** SCAN_MULTI frame codec (1, N, 16 relations; version
bytes; control-block version bits zero); shape-rule rejections; the N-times
positional receive loop against a mocked sequence of N single-scan reply trains,
asserting each train's schema/batch/LSN lands in the matching request-order slot.

**Rust engine:** `dispatch_scan_multi_fanout` writes N groups under one
`sal_writer_excl` hold at consecutive `write_cursor` positions (a concurrent
committer submit lands wholly before or after); `FLAG_SCAN_FIFO_REPLY` forces
`send_scan_response` through `pending_streams` for **both** a one-row wire-safe
batch **and** a one-row non-wire-safe (TEXT) batch (assert each is queued, not
immediately emitted, and the non-wire-safe one emits via the blob-capable
encoder with `FLAG_CONTINUATION | FLAG_SCAN_LAST`).

**E2E (pytest, `GNITZ_WORKERS=4`):**
- **Torn-commit impossibility (headline):** one client commits atomic two-table
  transactions `{A: k→v_i, B: k→v_i}` in a tight `FLAG_PUSH_TXN` loop; a second
  hammers `scan_many([A, B])`; every result set satisfies `A.v == B.v`. Run
  across checkpoint boundaries.
- **Sequential-scan skew is real (control):** the same workload via two *single*
  scans does exhibit torn pairs (proves the feature is load-bearing).
- **FIFO-ordering regression (the deadlock guard):** `scan_many([big, s1..s8])`
  where `big` is a wire-safe table forced to chunk and `s1..s8` are tiny — the
  shape that deadlocks without the `FLAG_SCAN_FIFO_REPLY` change; assert
  completion and correctness, and the reordered `scan_many([s1..s8, big])`. Add a
  **non-wire-safe variant** `scan_many([big, dim_text])` where `dim_text` has a
  TEXT column (the mainline case Finding 1 identifies — the non-wire-safe reply
  must FIFO too). If a shrunk `GNITZ_REPLY_FRAME_BUDGET` is used to force chunking
  without a huge table, keep `big`'s per-worker frame count small enough that the
  total parked frame sum per ring stays < 64 (else the pre-existing `InFlightState`
  overflow, not the ordering, aborts the run).
- **Mixed shapes:** one replicated + one hash-partitioned relation in one
  `scan_many`; snapshot consistency holds (worker-0 replicated read == the
  partitioned cut).
- **View + base:** `scan_many([t, v])`, `v` derived from `t`; quiescent writer →
  agree; under concurrent inserts assert only the instant-consistency invariant
  (the view never shows a row whose base row is absent in the same result set,
  insert-only).
- **`scan_many([t]) == scan(t)`** byte-for-byte (schema, rows, lsn).
- **Error paths:** empty, 17 relations, duplicate, system tid, unknown tid;
  server healthy after each.
- **Schema negotiation** (one parametrized test over cold / warm / stale-on-one-of-N):
  a prelim frame is sent iff a relation's cached version misses; only the stale
  relation re-negotiates; schema bytes are the Phase-1 capture even when a DDL
  commits mid-drain.
- **Slow-reader:** `scan_many` of 16 tiny tables read by a stalling client while a
  pusher runs; assert completion after the reader resumes, correctness, and
  server health; the never-resuming variant asserts eviction plus a healthy
  server and unaffected other clients. (The >64-parked-frames in-flight-under-stall
  stress belongs to the pre-existing overflow's own coverage, not here.)
- **Concurrent `scan_many` from two clients:** both complete correctly (serialized
  only at the one-cut submit, not the drain).
- **Cancellation:** client disconnects mid-stream; server healthy; later scans work.
