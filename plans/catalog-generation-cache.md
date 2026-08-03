# Catalog epoch: cross-statement client catalog cache, one validity counter, GET_INDICES retired

## Goal

Cut steady-state ad-hoc statement latency from ~5 server round trips to 1 by
making the client's catalog knowledge a **cross-statement cache** validated by a
single global **catalog epoch**, and delete the entire GET_INDICES / per-table
index-version mechanism, whose job the cache absorbs. (The counter is named
*epoch*, not *generation*, to avoid conflation with the durable checkpoint
generation `SEQ_ID_CHECKPOINT_GEN` used for view resume.)

## Current state (verified)

Every SQL statement brackets a **statement-scoped** catalog snapshot
(`GnitzClient::begin_catalog_snapshot` / `end_catalog_snapshot`,
`crates/gnitz-core/src/client.rs:299-308`) that is dropped at statement end:

```rust
fn scan_catalog(&mut self, tab: u64) -> Result<Option<Arc<ZSetBatch>>, ClientError> {
    if let Some(snap) = &self.catalog_snapshot {
        if let Some(cached) = snap.get(&tab) { return Ok(cached.clone()); }
    }
    let (_, batch, _) = self.session.scan(tab)?;
    ...
```

So a `SELECT … WHERE x = 5` pays, per statement: a SCHEMA_TAB scan, a TABLE_TAB
scan, a COL_TAB scan (each shipping the **full** system table over the wire),
one GET_INDICES round trip (`refresh_indices`, `client.rs:518` — the u8 epoch
only skips the reply payload, never the round trip; `Session::fetch_indices`,
`connection.rs:312`), and finally the SCAN_SPEC read. Five round trips, with
catalog payloads that grow with the number of tables. Raw (non-SQL) clients —
gnitz-py `resolve_table` + binary `push`, the gnitz-capi DDL helpers — call the
same catalog readers with **no** snapshot active, so today they always scan
fresh; that guarantee must be preserved, not silently deleted.

Server-side facts this plan builds on:

- **Bits 40–47 of the flags word** are the index-metadata version field
  (`crates/gnitz-wire/src/flags.rs:151-163`), used today only by the
  GET_INDICES request/reply pair. Every other frame carries 0 there. Verified:
  bits 48–62 are allocated boolean flags; only bits 55 and 63 are free, and
  they are not contiguous with 40–47 — so widening this field would cost a
  flag relocation. u8 it stays; the ABA bound below is the accepted trade.
- **System-catalog scans are master-served** (`handle_system_scan`,
  `executor.rs:1911` → `send_ok_response`); **SCAN_SPEC / user-table
  scans / seeks are worker-served** (frames built in
  `runtime/orchestration/worker/reply.rs`). Both sides must therefore agree on
  the epoch at every SAL position — which dictates where the bump lives (§1).
- **IDX_TAB is an ordinary client-scannable system table** (the client already
  scans it in `drop_index_by_name`, `client.rs:619`) whose rows carry
  everything GET_INDICES serves: `owner_id`, `source_col_idx`
  (= `pack_pk_cols(col_list)`), `is_unique`
  (`crates/gnitz-wire/src/catalog.rs:112-123`). The engine's own uniqueness
  maintenance is already "OR over live IDX_TAB rows per column list"
  (`hook_index_register` promotion, `hooks.rs:663-668`; DROP demotion
  recompute, `hooks.rs:709-718`), so a client-side parse with the same dedup
  rule is exact — including the FK-auto + UNIQUE promotion case, where two
  IDX_TAB rows (distinct `index_id`) share one circuit.
- The per-table u8 `index_version` counter (`catalog/cache.rs:52-57`) and the
  GET_INDICES handler (`executor.rs:1568`) exist **only** to let the client
  cache the index list — dead once the list parses from a cached IDX_TAB scan.
- `SCAN_SPEC` requests carry no version information (`connection.rs:429-445`),
  so the engine cannot detect a plan built against a stale catalog. The
  staleness fallbacks that do exist: narrow index bounds degrade to a full
  cursor (`store_io.rs:352-368`), `SEEK_BY_INDEX` replies `STATUS_NO_INDEX`
  (`executor.rs:1490-1503`), but a **wide-int** (U128/UUID/I128) bound on a
  dropped index is a hard, non-retryable statement error
  (`scan_spec.rs:144-150` → `store_io.rs:151`).

Load-bearing invariants this design relies on (all verified; state them at the
epoch's definition site):

- **tids/vids are monotonic and never reused** (`alloc_table_id`) — a stale
  tid resolves to the same relation or to a clean "unknown table" error, never
  to a different relation.
- **The REPLICATED table flag is immutable** for a table's lifetime.
- **DROP COLUMN preserves physical slot indices** (hidden-slot retirement), so
  a stale schema's column indices stay physically valid; visible output is
  filtered by the *server's* reply schema's hidden flags at presentation.
- **The catalog cache lives and dies with its connection** — `GnitzClient`
  builds a fresh cache per connect and no transparent in-place reconnect
  exists. A server restart resets the epoch to a small value; a reconnecting
  client starts empty, so no cross-boot ABA arises. If a transparent
  reconnect is ever added, it must rebuild the cache.

## Design

One counter, six rules.

### 1. The counter: a global u8 catalog epoch, bumped at the broadcast boundary

`CatalogCacheSet` gains `catalog_epoch: u8` (starts 1, wraps 255 → 1, never
emits 0; 0 is the wire sentinel "no information"). The bump site is **not**
`apply_local` — that is shared by the master-only compensation path
(`submit_local`, `write_path.rs:51-59`; `compensate_stage_a`,
`write_path.rs:1123`), so bumping there would let a *failed* DDL (e.g. a
duplicate CREATE TABLE: COL_TAB applied, TABLE_TAB precheck fails, COL_TAB
negated) advance the master's count while workers — which receive no broadcast
on the error path (`executor.rs:2213-2253`) — advance nothing, permanently
skewing the two and silently degrading every client to rescan-per-statement.

The bump lives at the **committed-broadcast boundary**, the true 1:1 point:

- **Master:** once per family batch actually emitted to the SAL in the DDL
  success path (the `drain_pending_broadcasts()` → `emit_zone_to_sal` loop,
  `executor.rs:2250-2253`), gated `family != SysFamily::Sequence && count > 0`.
- **Worker:** once per received DDL_SYNC family batch in `ddl_sync`
  (`store_io.rs:284`), same gate.

This makes the two sides agree exactly: rolled-back families (never emitted)
bump neither; the FK-auto-index cascade (self-produced on both sides via
`submit_local`) bumps neither; boot shard-replay (`fire_hooks`, bypasses this
boundary) bumps neither; the pre-fork SAL-tail recovery bumps the master and
workers inherit the value at `fork()`. Post-fork, master emissions ↔ worker
receipts are 1:1.

The per-table `index_version` counter, `get_index_version`, its
`apply_index_by_id` bump, its purge, and the `metadata.rs` pass-through are
**deleted**.

### 2. The wire field: bits 40–47 become the catalog epoch

Rename `wire_flags_set_index_version` / `wire_flags_get_index_version` to
`wire_flags_set_catalog_epoch` / `wire_flags_get_catalog_epoch` (same bits,
same u8, same 0-sentinel), updating the import sites in `protocol/header.rs`,
`protocol/mod.rs`, and `connection.rs`.

**Replies:** every reply-frame builder stamps the current epoch:

- master: `send_ok_response`, `send_control_only`, `send_error`, and the
  master-built scan continuation/terminal frames (`executor.rs`);
- worker: **every** scan/seek frame emitter in
  `runtime/orchestration/worker/reply.rs` — audit all builders, including the
  `FLAG_CONTINUATION | FLAG_SCAN_LAST` one at ~line 618 that today stamps no
  schema version; the selection criterion is "emits a client-bound frame",
  not "already stamps a schema version".

Error frames stamp it too — a rejected statement must still re-anchor the
client (this is what heals the raw-client push-mismatch loop in §4).

**Requests:** the three read dispatches whose *plan* derives from the catalog —
`FLAG_SCAN_SPEC`, `FLAG_SEEK`, `FLAG_SEEK_BY_INDEX` — stamp the client's
**plan basis** (§4): the epoch its catalog cache held when the statement's
first catalog read ran (0 = no basis, skip validation). Validation is by the
epoch **alone**; no schema-version check is added anywhere. (The per-table u16
schema version field survives untouched for its own job — warm-push
schema-block elision, which is per-table and orthogonal; a global epoch would
over-invalidate it. It is not a validation input here: any DDL that changes a
schema also bumps the epoch, so the epoch subsumes it, and for pure-SELECT
clients `scan_spec` never populates the schema cache anyway.)

Pushes are unchanged (the existing `STATUS_SCHEMA_MISMATCH` machinery fully
guards them). DDL requests are unchanged (rule 6 makes DDL planning fresh).

### 3. The validation: `STATUS_STALE_CATALOG`

New control status `STATUS_STALE_CATALOG: u32 = 5` (`flags.rs`, after
`STATUS_TXN_CONFLICT = 4`). In the master's `handle_message`, before
dispatching a `FLAG_SCAN_SPEC` / `FLAG_SEEK` / `FLAG_SEEK_BY_INDEX` request,
under the catalog read lock it already holds:

```rust
let basis = wire_flags_get_catalog_epoch(flags);
if basis != 0 && basis != shared.cat().catalog_epoch() {
    send_control_only(peer, target_id, client_id, STATUS_STALE_CATALOG).await;
    return;
}
```

The short-circuit runs **before** any worker fan-out, so the single
control-only frame is a legal train terminator (`drain_reply_train` is
continuation-bit-driven) with no desync.

This converts *every* stale-plan execution — including the wide-int
dropped-index hard error, the one staleness outcome that is neither
silent-correct nor self-healing today — into one clean retryable rejection
before any work runs. The residual race (a DDL landing between master
validation and worker execution) is exactly today's microsecond-class
plan-vs-execute window.

**`check_response` (`connection.rs:46-72`) must map the new status to a new
`ClientError::StaleCatalog`** — without that arm, the control frame decodes as
a successful empty result and the entire retry design is inert. The rejection
is control-only: nothing executed, so retrying is always safe.

### 4. The cache: persistent, entry-point-gated, torn-statements-abort

In `GnitzClient`, `catalog_snapshot: Option<HashMap<…>>` is replaced by:

```rust
struct CatalogCache {
    /// Epoch all cached batches were scanned at. 0 = empty.
    epoch: u8,
    tabs: HashMap<u64, Option<Arc<ZSetBatch>>>,
}
```

and `Session` gains `last_seen_epoch: u8`, absorbed via one
`note_catalog_epoch(flags)` helper called from the **actual** receive
chokepoints (ignoring 0 per the sentinel convention):

- the frame loop inside `drain_reply_train` (covers plain scans, SCAN_SPEC —
  which deliberately bypasses `recv_cached` — and SCAN_MULTI);
- `recv_cached` (push ACKs, seeks);
- `send_txn_frame` (`connection.rs:194-198`) — the DDL-ACK and txn-commit
  receive path; without this, read-your-own-DDL breaks;
- `roundtrip` (the alloc RPCs).

**The freshness gate runs at every public catalog-reading entry point, not
only in `SqlPlanner`.** A shared `GnitzClient` helper —

```rust
fn ensure_catalog_basis(&mut self) {
    let seen = self.session.last_seen_epoch();
    if self.catalog_cache.epoch != 0 && self.catalog_cache.epoch != seen {
        self.catalog_cache.wipe();
    }
}
```

— is called on entry by `resolve_table_id`, `resolve_table_or_view_id`,
`resolve_relation_kind`, `table_replicated`, `table_indexes`,
`index_for_column`, and every raw DDL helper (`create_table`, `create_view*`,
`drop_table`, `create_schema`, `drop_schema`, `create_index`,
`drop_index_by_name`). This preserves today's guarantee for raw gnitz-py /
gnitz-capi callers that never run a statement bracket: any traffic they do
(including a failing push's error ACK, now epoch-stamped) re-anchors
`last_seen_epoch`, and their next catalog read wipes and rescans. Without
this, a raw py client that `resolve_table`d before a foreign ADD COLUMN would
push into `STATUS_SCHEMA_MISMATCH` forever. `SqlPlanner` keeps its statement
bracket only to add the DDL-fresh wipe (rule 6) and to fix the statement
basis; `end_catalog_snapshot` is deleted.

**The statement basis is fixed at the statement's first catalog read, and any
mid-statement change aborts and replans.** `scan_catalog` records the reply
epoch of the first scan it absorbs for the current statement (a cache hit
inherits `cache.epoch`); if a later scan in the same statement absorbs a
*different* epoch — or a resolution-layer refresh (rule 5) wipes the cache
mid-statement — planning aborts with an internal `StaleCatalog` and the
statement-layer retry replans from scratch. This is strictly stronger than
today's snapshot (which tolerates torn views within a statement) and it closes
the hole where a mid-statement cache clear would advance `cache.epoch` to the
new value, letting a torn plan stamp a *current* basis and sail through
validation. Mechanically: the cache carries a monotonically increasing local
`wipe_count`; the planner captures `(epoch, wipe_count)` after the begin
check; the request-stamp helper and the bind/plan steps compare before use.
Cached batches are always same-epoch by construction (a scan absorbing a new
epoch wipes first, then triggers the abort).

The client's own DDL needs no special-case invalidation: its ACK (via
`send_txn_frame`) carries the post-DDL epoch, `last_seen_epoch` advances, and
the next entry-point gate wipes. Read-your-own-DDL is exact.

### 5. The retry rules

- **Resolution-layer retry (kept — it is the only healer raw clients have):**
  `lookup_schema_id` / `lookup_table_record` / `lookup_relation` — on a miss
  while the cache was warm (not freshly scanned by this call chain), wipe,
  rescan, retry the lookup once before reporting "not found". A verified miss
  is then always fresh-verified, for SQL and raw callers alike. Inside a
  `SqlPlanner` statement this refresh also trips the torn-statement abort
  (§4), so a statement never continues on a half-swapped catalog — in
  particular an RMW build never binds columns against a different catalog
  than its seek used.
- **Statement-layer retry:** at the `SqlPlanner` statement entry, re-run the
  statement (bounded: 3 attempts total, then surface "catalog changed
  concurrently") when it fails with `GnitzSqlError::Bind`, `::Plan`, or
  `::Unsupported` while planned warm, or with
  `Exec(ClientError::StaleCatalog)` (from dispatch-time rejection or the
  internal torn-statement abort). The predicate is **by variant**, never "does
  it wrap a ClientError" — a resolution miss is constructed client-side as
  `ClientError::ServerError` and must not be classified by wrapper type (it
  is owned by the resolution-layer retry and never reaches this layer as a
  staleness symptom). All other `Exec(..)` errors and `Conflict` are never
  auto-retried. Retry safety: the retry granularity is **one `Statement`**
  (never a multi-statement submission — re-running an earlier statement of a
  batch would double-buffer its writes); the only wire operations a statement
  performs before a `Bind`/`Plan` error or a basis abort are idempotent
  *reads* (the RMW builders dispatch seeks before evaluating SET expressions
  — `rmw.rs::commit_rmw_or_buffer` — but the txn-buffer mutation is strictly
  last, so an aborted build leaves the buffer untouched and a retry
  re-buffers exactly once). The statement retry composes with the RMW
  driver's own bounded OCC loop (`RMW_MAX_ATTEMPTS = 4`) multiplicatively in
  the worst case; both are small and both terminate.
- Auto-retry is kept rather than "wipe and surface the error": today every
  statement rescans, so foreign DDL essentially never spuriously fails
  another client's statement. The persistent cache breaks that contract;
  auto-retry restores it. Even when the retry also fails, the wipe guarantees
  the next statement plans fresh.

### 6. DDL statements always plan fresh

The planner passes `fresh = true` — an unconditional wipe before planning —
for **exactly** these statement arms (the set is total; DDL is rejected inside
transactions, so no buffered forms exist): `CreateTable`, `CreateView` (both
variants), `CreateIndex`, `Drop` (table/view/schema/index), `CreateSchema`,
`AlterTable`, `AlterView`. Honest justification: the server re-validates the
FK gate authoritatively (`validate_fk_column`), so DDL-fresh is **not** a
correctness requirement for stale-*accepts*; it exists to (a) prevent spurious
client-side *rejects* (FK gate, DROP COLUMN guard, name-taken probes) that no
retry can distinguish from genuine ones, and (b) ensure CREATE VIEW bakes its
scan bounds (`scan_bound_for_input`) and its replicated-source routing
(`table_replicated` — immutable flag, but the row must exist) against the
current catalog, since circuit bounds are persisted and never re-validated by
the epoch. Cost: full catalog scans on the rare DDL path — today's cost.

### The index list: parsed from the cached IDX_TAB

`GnitzClient::table_indexes(tid)` / `index_for_column(tid, col)` are
reimplemented as pure parses over `scan_catalog(IDX_TAB)`:

- iterate **`live_rows()`** (net-consolidated positive-weight rows — a
  dropped index's retraction nets out; iterating raw rows would leak it);
- filter `owner_id == tid` (sufficient alone: ids are allocator-unique across
  tables and views, and indexes are table-only by write-path enforcement);
- decode `source_col_idx` via the existing `unpack_pk_cols` well-formedness
  guard, `is_unique` from its column;
- **dedup by column list** with `is_unique = any(rows)` — exactly the
  engine's own promotion/demotion rule (§ Current state);
- return `Arc<Vec<IndexMeta>>` (fresh `Arc::new` per parse, keeping the
  signature) in IDX_TAB PK (`index_id`) order — deterministic; the
  seek-candidate collector and `best_index_bound` rank by their own criteria.
- Rewrite the `table_indexes` / `index_for_column` doc comments
  (`client.rs:481-539`) — they currently describe the epoch-validated
  GET_INDICES cache and would become false.

Deleted with this: `refresh_indices`, `Session::fetch_indices` (and its doc),
`index_cache` + the `SCHEMA_CACHE_CAP` constant at `client.rs:13` (its only
user; the unrelated same-named constant in `connection.rs:22` stays),
`FLAG_GET_INDICES` (frees flag bit 54), `handle_get_indices`, the
`index_version` machinery (§1), and `IndexListMemo`
(`gnitz-sql/src/dml/plan.rs:327-349`) — its motivating comment
("`table_indexes` ALWAYS hits the wire") becomes false; the range→equality
collector fall-through then parses the cached batch twice per statement,
which is accepted (client CPU on a tiny in-RAM list).

**Per-call batch walks are retained.** Parsing system-table batches once per
epoch into typed maps (name→tid, tid→schema) is a client-CPU optimization
that would add a second invalidation surface; the win this plan targets is
round trips, so the typed-map layer is deliberately rejected.

## Steady-state round-trip accounting

| Statement | Today | After |
|---|---|---|
| `SELECT … WHERE x = 5` | 3 catalog scans + GET_INDICES + read = 5 | read = **1** |
| `INSERT` (warm push) | 3 catalog scans + push = 4 | push = **1** |
| `UPDATE … WHERE` | 3 scans + GET_INDICES + seek + push | seek + push = **2** |
| any statement, first after a foreign DDL | n/a | + one refresh cycle (STALE_CATALOG or entry gate) |
| DDL | scans + push | unchanged |

## ABA bound

The epoch is u8 with period 255. A client mis-validates only if exactly 255·k
committed catalog family batches land between two of its observations. The
consequence class is bounded by the invariants above (monotonic tids,
cols-matched index resolution, schema-version-guarded pushes, presentation
filtering by the server's reply schema) to today's plan-vs-execute race
outcomes: correct-but-slower reads, or a clean error healed by the next
statement. Accepted uniformly — no per-path second counter — and documented
at the epoch definition.

## Tests

Engine unit (`catalog/tests/`):
- epoch bumps once per **broadcast** non-Sequence family batch: a CREATE
  TABLE bundle bumps ≥ 2 (COL_TAB + TABLE_TAB); a **failed** CREATE (duplicate
  name) bumps **zero** on the master (compensation is bump-free); SEQ_TAB
  serial refill and user-table pushes bump nothing; wraps 255 → 1 skipping 0.
- master validation: a scan_spec with basis ≠ current epoch gets
  `STATUS_STALE_CATALOG`; basis 0 passes; matching basis passes.
- master/worker agreement: after a mix of successful and failed DDL, a
  worker-served reply and a master-served reply stamp the same epoch.

Client/planner unit (`gnitz-sql`):
- `plan/index_bound.rs` closure-counter tests rewritten (the memo and its
  `calls == 1` assertions are deleted with `IndexListMemo`).
- IDX_TAB parse: retracted rows excluded via `live_rows`; FK+UNIQUE same-cols
  rows dedup to one `is_unique` entry; multi-column packed lists round-trip.
- statement retry predicate: `Bind`/`Plan`/`Unsupported`/`StaleCatalog`
  retried once-then-surface; `Exec(ServerError)` never retried.

E2E (`gnitz-py`, `GNITZ_WORKERS=4`, new `tests/test_catalog_cache.py`):
- cross-client heal via SQL: client A `CREATE INDEX` / `DROP INDEX` /
  `CREATE TABLE` / `DROP TABLE`; idle client B's next statement returns
  correct results (covers STALE_CATALOG replan, resolution-layer retry, and
  the wide-int dropped-index case: `DROP INDEX` on a U128-bound column
  between B's statements must succeed via replan, not error).
- cross-client heal via the **raw** surface: client B does
  `resolve_table` + binary `push`; client A runs a DDL touching B's table;
  B's next pushes/resolves recover without reconnecting (the wedge-regression
  test for the entry-point gate).
- own-DDL: `CREATE INDEX` then `SELECT` on the same connection plans against
  the new index; `CREATE TABLE` then immediate `INSERT`/`SELECT` works.
- a failed DDL (duplicate CREATE TABLE) followed by SELECTs on both
  connections: results correct and (assertable via repeated statements) no
  permanent per-statement rescan regression.
- repeated SELECTs with no DDL in between return identical results.

## Sequencing

- [ ] **Engine + wire mechanism (additive):** `catalog_epoch` counter with
  bumps at the master broadcast-emit loop and worker `ddl_sync`; rename bits
  40–47 accessors to catalog-epoch (+ the three import sites); stamp all
  master reply builders and **all** worker frame emitters;
  `STATUS_STALE_CATALOG` + master basis validation of
  `SCAN_SPEC`/`SEEK`/`SEEK_BY_INDEX` (requests still send 0, so validation is
  dormant); engine unit tests for bump agreement + validation.
- [ ] **Client switch:** `CatalogCache` + `last_seen_epoch` absorption at
  `drain_reply_train` / `recv_cached` / `send_txn_frame` / `roundtrip`;
  `ensure_catalog_basis` entry gate on every public catalog-reading and raw
  DDL method; basis-stamped read requests with the fixed-at-first-read basis
  + torn-statement abort; `ClientError::StaleCatalog` mapping in
  `check_response`; resolution-layer retry; `SqlPlanner` statement-layer
  retry + DDL-fresh rule; IDX_TAB-parsed `table_indexes` /
  `index_for_column` with rewritten docs; delete `IndexListMemo`; gnitz-sql
  test updates.
- [ ] **Delete the dead mechanism:** `FLAG_GET_INDICES`, `fetch_indices`,
  `refresh_indices`, `index_cache`, `SCHEMA_CACHE_CAP` (client.rs),
  `handle_get_indices`, the `index_version` counter + bump + purge + the
  `metadata.rs` pass-through + its unit tests.
- [ ] **E2E staleness suite** per the test list above; full `make verify` +
  `make e2e`.
