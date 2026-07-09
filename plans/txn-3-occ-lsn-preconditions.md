# Optimistic concurrency: per-table LSN preconditions on the TXN frame

## Problem

Every SQL mutation that reads before it writes is a lost-update race.
UPDATE and DELETE resolve their target rows client-side
(`resolve_where_rows`, `gnitz-sql/src/dml/mutate.rs:182-236`), compute new
rows client-side (`write_set_rows`, `mutate.rs:134-155`), and then push;
INSERT's ON CONFLICT paths seek existing rows client-side
(`client_side_filter_do_nothing` / `client_side_merge_do_update`,
`gnitz-sql/src/dml/insert.rs:325-412`). Nothing prevents another writer
from committing between the read and the write: two concurrent
`UPDATE t SET x = x + 1 WHERE pk = k` connections both read `x`, both
write `x+1`, and one increment is silently lost. SQL `BEGIN`/`COMMIT`
transactions have the same window stretched across every statement.

This plan adds **per-table LSN preconditions** to the user-table TXN
frame: the client says "commit only if table T has not been written since
LSN L"; the master checks it under the same table locks that serialize the
commit, and rejects with a structured, retryable **conflict** error
otherwise. The SQL layer uses it to make autocommit read-modify-write
statements lose-update-free (bounded internal retry) and to give
`BEGIN`/`COMMIT` transactions a first-committer-wins conflict check.

**Engine-side surface consumed as fact (delivered by the user-table
transaction-frame work that precedes this plan):** the `FLAG_PUSH_TXN`
frame (control block, `u32 family_count`, per-family
`[u8 mode][schema block][wal block]`), `handle_push_txn` in
`runtime/orchestration/executor.rs` (catalog read lock → sorted
`fk_lock_set` union held through the committer ACK → bundle validation →
`CommitRequest::Txn` → ACK with the zone LSN), and the client-side
`TxnBuffer` / `GnitzClient::commit_txn`. This plan extends that frame and
that handler, and restructures the committer's *error payload* (the
`CommitError` type in Master changes); it changes no validation rule and
no committer commit-path logic.

## Ground facts (verified against the live tree)

- **There is no master-side per-user-table commit LSN today.** The
  committer publishes only the global durability watermark
  (`ZoneLsnAllocator`, two `Cell<u64>`s — `reserved`/`published`,
  `runtime/orchestration/lsn.rs:15-61`; `publish(zone_lsn)` after fsync,
  `committer.rs:716-721`). Per-table `current_lsn` exists only on
  worker-side `Table`s and is a **drifted hybrid counter**, not zone-LSN
  space: `Table::ingest` does `self.current_lsn += 1`
  (`storage/lsm/table/mod.rs:392`) while the DDL path pins real zone LSNs
  (`catalog/write_path.rs:116`) — which is exactly why
  `ZoneLsnAllocator::reserve` takes a floor "past any drifted family
  counter" (`lsn.rs:31-41`). Worker `current_lsn` is therefore unusable as
  a precondition authority; this plan adds the authoritative map on the
  master.
- **Zone LSNs are the one comparable currency.** Push and TXN ACKs return
  the commit's zone LSN (`seek_pk`, `committer.rs:757-763`). A scan's
  terminal frame reports `last_tick_lsn` — a snapshot of
  `lsn_alloc.published()` taken before the tick's ACKs (`executor.rs:591-624,
  1323`) — the same counter. Zone LSNs are monotone in commit
  order (reserved under `sal_writer_excl`, `lsn.rs:42-46`).
- **Seek replies carry no LSN.** `Connection::seek` returns
  `msg.seek_pk as u64` as its third tuple element
  (`gnitz-core/src/connection.rs:195-217`) — for seeks that field is the
  *echoed request PK* (`executor.rs:1046` for the system path; the worker
  seek reply echoes likewise), not an LSN. The SQL layer's `_lsn`
  bindings on seek results (`insert.rs:340,383`, `mutate.rs` resolve
  paths) are misnomers today. A transaction basis therefore cannot be
  harvested from seek replies; it needs an explicit watermark read.
- **A user-table commit's data is fully applied before its LSN is
  client-visible.** The committer awaits worker ACKs (Phase C,
  `committer.rs:649-660`, futures covering all `nw` workers) strictly
  before fsync (`:710`), publish (`:720`), and the client ACK (`:763`);
  a worker's push ACK means it applied the group (`worker/mod.rs:981-991`).
  The other two publishers cannot break the ordering for user tables:
  SERIAL publishes without worker awaits (`executor.rs:1966-1972`) and
  DDL publishes after its own synchronous application, but both require
  the catalog **write** lock (`executor.rs:1928, 1549`) while every push
  holds the catalog **read** lock across its commit ACK
  (`executor.rs:938-1005`), so neither can interleave a publish inside a
  user-table commit window. Independently, all client reads of user
  tables are SAL-ordered commands consumed sequentially by workers. So if
  a client observes watermark `W = published()`, any subsequent read
  reflects every user-table commit `≤ W`. (This dependency — publish
  strictly after apply, guarded by the catalog lock split — is
  load-bearing for the soundness argument below; an allocator that
  published outside the catalog write lock would invalidate it.)
- **The push arm holds its table locks through the committer ACK**
  (`_tlocks` scope, `executor.rs:944-1005`), and every `unique_pk` table
  self-locks (`needs_lock`, `catalog/cache.rs:450-463`). The TXN handler
  does the same with the sorted lock union. These lock holds are the
  race-free window in which a precondition check and the commit are one
  atomic step.
- **Wire real estate:** `1 << 58` is allocated to `FLAG_PUSH_TXN` by the
  transaction-frame work this plan extends (in the live tree the top bit
  is `FLAG_DDL_TXN = 1 << 57`); `1 << 59` is free (`gnitz-wire/src/flags.rs:11-62`,
  disjointness guard `flags.rs:83-118`). Status codes 0-3 are used
  (`STATUS_OK/ERROR/SCHEMA_MISMATCH/NO_INDEX`, `flags.rs:191-200`); 4 is
  free. Client errors flatten into one `GnitzError` exception in Python
  today (`gnitz-py/src/lib.rs:24-33`); `ClientError` is the only
  structured layer (`gnitz-core/src/error.rs:5-9`).

## Semantics (committed)

A precondition `(tid, basis_lsn)` asserts: **no commit has written table
`tid` with a zone LSN greater than `basis_lsn`.** Checked on the master
inside `handle_push_txn`, after lock acquisition and before validation;
on failure the whole transaction is rejected with the structured conflict
status — nothing validated, nothing written, cleanly retryable.

Soundness argument, in the two directions that matter:

- *No false pass:* the check runs under the union of the transaction's
  table locks, and every writer to a precondition-relevant table (all are
  `unique_pk` — the TXN shape rule requires it, and SQL tables always
  are) holds that same table lock through its own commit ACK. So between
  a passing check and this transaction's durability, no conflicting
  commit can interleave; and any commit that completed *before* the check
  is visible in the master's per-table map (updated before its lock
  released, below).
- *No false conflict from a stale basis:* a basis `B` obtained as
  `published()` at time `t` undercounts nothing: every commit `≤ B` was
  already applied when the client's subsequent reads ran (previous
  section), so a conflict is flagged only for commits the reads could not
  have seen. Underestimating the basis is conservative (spurious retry);
  overestimating is impossible because the client only ever uses
  server-issued watermarks and commit-ACK LSNs.

Preconditions are per-table (coarse). Two writers touching *different*
rows of one hot table will conflict spuriously; the retry loop absorbs
it. This is the committed granularity: it needs no storage changes, no
per-key history (which consolidation deliberately destroys), and no
read-set shipping; its map is one `u64` per table.

After a server restart the map is empty and must seed with a value that
**dominates the pre-crash published watermark** — otherwise a basis
issued before the crash could falsely pass against post-restart commits.
The existing boot seed does NOT dominate it: `max_table_current_lsn()`
is a max over *drifted* per-table counters that by design lag real zone
LSNs (`Table::ingest` does `current_lsn += 1`,
`storage/lsm/table/mod.rs:391-392`; the documented invariant is "every
allocated zone LSN is strictly greater than each table's current
counter", `catalog/partition_lsn.rs:334-349`). Committed fix, two parts:

- a new system sequence `SEQ_ID_LSN_HIGH_WATER` (id 6 — inside the
  4..16 reserved gap `observe_user_sequence` ignores), maintained at two
  points, both via the existing sequence machinery (`advance_sequence` +
  `flush_all_system_tables`, no SAL write needed,
  `catalog/registry.rs:279-294`; recovery gains one `recover_sequences`
  match arm):
  1. **steady-state checkpoints**: the ephemeral round gains a pre-reset
     step — under `sal_writer_excl`, immediately before
     `checkpoint_reset_only`'s reset (`dispatch.rs:1756-1764`), snapshot
     the allocator's `reserved` into the sequence and flush the system
     tables. The base round alone would be unsound: `FLAG_ALLOCATE_SERIAL_RANGE`
     takes no committer barrier (`executor.rs:1918-1923`), so a SERIAL
     commit can reserve, publish, and ACK a zone *during the drain
     window* between the rounds; the ephemeral reset would then discard
     it from the SAL while it is above the base-round snapshot — lost
     from both seed sources. (That window is also a pre-existing
     durability bug in its own right — the ACKed SERIAL range itself is
     discarded by the second reset, since `sys_sequences` is unflushed
     since the base round; the pre-reset flush added here closes it as a
     side effect.)
  2. **recovery start**: the pre-fork recovery step
     (`recovery_start_generation_bump`, `catalog/registry.rs:318-326` —
     it already advances a sequence and flushes system tables durably
     before the fork) additionally computes and persists the boot seed
     `max(persisted SEQ_ID_LSN_HIGH_WATER, max committed zone LSN in the
     recovered SAL tail, max_table_current_lsn())` back into the
     sequence. The tail maximum is free — `collect_committed_lsns`
     (`bootstrap.rs:62-81`) already enumerates exactly the committed
     zone LSNs pre-fork — and persisting the *computed* seed (not just
     reading it) is what makes a **second** crash safe: the boot
     checkpoint resets the SAL before the executor's allocator exists
     (`bootstrap.rs:740-772`, allocator constructed at
     `executor.rs:220-221`), so without re-persisting, a double crash
     would boot from the stale pre-first-crash value with an empty tail
     (SAL reset at `bootstrap.rs:734-738`, boot checkpoint at `:772`).
- at boot, the `ZoneLsnAllocator` seed and the map default are simply
  the persisted `SEQ_ID_LSN_HIGH_WATER` (one authority; the executor
  reads it where it reads the other sequences). Every pre-crash
  published zone is ≤ that value by the two maintenance points above,
  so the seed dominates `published_pre` and any pre-restart basis
  conflicts, as required.

## Wire changes

- `pub const FLAG_GET_LSN: u64 = 1 << 59;` — a master-only watermark
  read, added to the compile-time disjointness guard
  (`flags.rs:83-118`). Request: header-only frame (`target_id = 0`),
  mirroring `alloc_table_id`'s shape (`connection.rs:87-89`). Reply:
  standard ACK with `seek_pk = lsn_alloc.published()`. No locks, no
  fanout, no catalog access; handled in the executor's `target_id == 0`
  control block (`executor.rs:831-850`). This is the cold-start
  basis-establishment op — necessary when the connection has observed no
  server LSN yet, and at `BEGIN` for the tightest transaction basis
  (seek replies carry no LSN; scan replies and commit ACKs do, and the
  client tracks their maximum — SQL layer section).
- The TXN frame gains a precondition section between the control block
  and the family count:

  ```
  control block            (unchanged)
  u32 precondition_count   LE, may be 0
  per precondition:
    u64 tid                LE
    u64 basis_lsn          LE
  u32 family_count         (unchanged)
  per family: ...          (unchanged)
  ```

  `decode_push_txn` grows the section; a frame with
  `precondition_count = 0` is byte-for-byte today's frame apart from the
  four zero bytes. Precondition tids must be **`unique_pk` base tables**
  (the same rule families obey; checked with `table_has_unique_pk`,
  `catalog/metadata.rs:121-123`) — read-only preconditions on tables the
  transaction does not write are legal and useful, but a view tid (its
  content changes via ticks, which never touch the map — the precondition
  would be permanently vacuous) or a non-`unique_pk` table (its plain
  pushes take **no table lock**, `fk_lock_set` empty per `needs_lock`,
  `catalog/cache.rs:449-462`, so a concurrent push could commit between
  check and durability — the no-false-pass argument's premise fails) is
  rejected: `TXN: precondition on {tid}: not a unique_pk base table`.
  Unknown tids reject the frame.
- `pub const STATUS_TXN_CONFLICT: u32 = 4;` — the reply status for a
  failed precondition. The error payload names the first conflicting
  table: `txn conflict: table {schema}.{name} changed past LSN {basis}`.
  The control block carries the message independently of status (engine
  keys on `!error_msg.is_empty()`, `wire.rs:258,276`; client on
  `NULL_BIT_ERROR_MSG`, `message.rs:147`), but the client-side routing
  must be added in **`parse_response`** (`message.rs:447-454`) — today
  only `STATUS_ERROR` populates `error_text`, and the
  `SCHEMA_MISMATCH`/`NO_INDEX` arms are message-less control frames, so
  a status-4 arm that extracts the message is required *before*
  `check_response` can surface it.

## Master changes

- **The per-table commit-LSN map.** `Shared` (executor) gains
  `table_commit_lsn: RefCell<FxHashMap<i64, u64>>` plus the boot seed
  value. Update sites — each while the writer still holds the table's
  lock, immediately after a successful commit ACK:
  - the push arm (`executor.rs:995-1000`): on `Ok(Ok(lsn))`,
    `map[tid] = lsn`, **before** `send_ok_response` (the `_tlocks` and
    `_cat` guards live to the arm's `return` at `executor.rs:1005`, so
    even the reply await stays inside the lock scope);
  - commit **errors** become structured so the map stays exact: the
    committer's `done` payload changes from `Result<u64, String>` to
    `Result<u64, CommitError>` with
    `CommitError { msg: String, durable_zone: Option<u64> }` —
    `durable_zone = Some(zone_lsn)` iff the group's bytes reached the
    SAL under a written sentinel (a Phase-C worker-ACK error;
    `committer.rs:572-587, 617-621, 651-660`), `None` for pre-SAL
    failures (Phase-A panics, fit-check rejections). On
    `Ok(Err(e))` with `Some(zone)`, `map[tid] = max(map[tid], zone)`:
    the table *did* durably change (recovery replays the sentineled
    zone) while the client saw an error. Pre-SAL failures bump nothing,
    so the common error paths cause no spurious conflicts. One honest
    residual: when *every* group in such a batch errs, `publish` is
    skipped (`committer.rs:719`) while the zone is durable, so the
    bumped table conflicts with all published-derived bases until the
    next successful commit raises `published` past it — a stall confined
    to the near-unreachable worker-apply-error path (the same path the
    TXN frame fail-stops on), accepted and documented;
  - the TXN arm: the same two rules for every family tid.
  Reads default to the boot seed for missing entries. Single-threaded
  reactor: `RefCell`, no synchronization. (Writers to tables outside
  `needs_lock` cannot be precondition targets — the wire rule above
  rejects those tids — so their unlocked pushes need no map coverage.)
- **The precondition check** in `handle_push_txn`, after the lock union
  is acquired (step 3 of the handler) and before validation (step 4):

  ```rust
  for &(tid, basis) in &preconditions {
      if table_commit_lsn(tid) > basis {
          return conflict_reply(tid, basis);   // STATUS_TXN_CONFLICT
      }
  }
  ```

  For precondition tids that are not family tids (read-only
  preconditions), the lock union must include them: the handler extends
  its lock set to `⋃ fk_lock_set(tid) ∪ {precondition tids}`, still
  sorted ascending, still deadlock-free (single global order, superset
  acquisition at one site). Without the lock, a conflicting writer could
  commit between check and this transaction's durability.
- `FLAG_GET_LSN` handling: reply `lsn_alloc.published()`. One branch in
  the `target_id == 0` control dispatch.

## Client changes (gnitz-core)

- `TxnBuffer` gains `preconditions: Vec<(u64, u64)>` and
  `pub fn require_unchanged(&mut self, tid: u64, basis_lsn: u64)`
  (last-write-wins per tid). `Connection::push_txn` encodes the section.
- `GnitzClient::get_lsn(&mut self) -> Result<u64, ClientError>` — the
  `FLAG_GET_LSN` roundtrip.
- `GnitzClient` gains `last_seen_lsn: u64` — the running maximum over
  every server-issued LSN the connection observes: push/`commit_txn`
  ACKs (`push_with_mode` returns it, `client.rs:278-287`), scan replies
  (`Connection::scan`'s third tuple element, `connection.rs:157-192`),
  and `get_lsn` results. Every one is `≤ published()` at receipt, so it
  is always a sound (conservative) basis. Seek replies never update it
  (their third element is the PK echo, not an LSN).
- `ClientError` gains a structured variant:

  ```rust
  TxnConflict(String),   // STATUS_TXN_CONFLICT reply; message names the table
  ```

  routed in `parse_response` (the new status-4 arm above), then decoded
  in `check_response` beside `SchemaMismatch`/`NoIndex`
  (`connection.rs:36-55`).

## SQL layer

Two consumers, one mechanism.

**Autocommit read-modify-write statements** (UPDATE, DELETE with a WHERE
that resolves rows, INSERT ... ON CONFLICT — the statements whose writes
are functions of reads; plain INSERT reads nothing and keeps the plain
push):

1. Establish the basis *before* any read the statement performs: the
   client's `last_seen_lsn` (every observed LSN is a published watermark
   at receipt time, so using it as a basis only ever undercounts, which
   is conservative), falling back to one `client.get_lsn()?` roundtrip
   when the connection has observed none. Warm RMW loops therefore pay
   no extra roundtrip (the previous iteration's commit ACK is the
   basis).
2. Execute the statement's reads and build the write batch exactly as
   today.
3. Ship as a **single-family TXN frame** (the same `TxnBuffer` machinery,
   one family) with `require_unchanged(tid, basis)`.
4. On `ClientError::TxnConflict`: discard, re-run from step 1. Bounded at
   `3` attempts; the third failure surfaces the conflict error to the
   caller. A retried `INSERT ... ON CONFLICT` re-runs its build and so
   draws fresh SERIAL ids (`client.next_serial_id`, `insert.rs:195`) —
   each retry burns an id range slot, standard SQL sequence-gap
   behavior, documented here and tested.

This closes the lost-update window: between `basis` and the commit, any
interleaved commit to `tid` bumps the map past `basis` and forces the
retry to re-read. Two concurrent `x = x + 1` clients now always sum to
+2 (one retries). The switch from plain push to one-family TXN frame
changes durability shape not at all (one zone, one sentinel — the frame
path is the same committer), and validation semantics for a single
Update-mode family are identical to the push path's.

**`BEGIN`/`COMMIT` transactions** (the SQL transaction state on
`GnitzClient`):

- `BEGIN` fetches `basis = get_lsn()` once and stores it on the
  transaction state.
- Statement execution records every table its *reads* touch (the
  resolve/seek/scan targets of UPDATE/DELETE/ON CONFLICT — the tables
  whose staleness feeds buffered writes) into a read-set of tids.
  SELECT-only tables are **not** recorded: a pure query feeds no buffered
  write, and including it would only manufacture aborts (this is the
  committed choice of conflict scope: write-skew involving SELECT-only
  tables is accepted). Write-only tables are likewise not recorded: a
  buffered plain INSERT carries no precondition — a blind write cannot
  lose an update, and its duplicate-PK race is closed by the commit-time
  Error-mode bundle validation running under the table locks.
- **`basis[tid]` is fixed at the transaction's first read of that
  table** and never raised afterwards: it is
  `max(BEGIN watermark, scan LSN)` when that first read was a scan
  (`last_tick_lsn` is a pre-read published snapshot, so it is sound —
  but it can lag the BEGIN watermark, since it is stamped by the tick
  loop, not the scan; the max keeps the basis as tight as what the
  transaction provably saw), else the transaction's `BEGIN` watermark. Raising it on a *later* scan would be unsound: an earlier
  seek-read of the table may have fed buffered writes, a concurrent
  commit may have landed after that read, and the later scan's LSN
  covers the commit — the precondition would certify reads it does not
  cover, readmitting the lost update. First-read-only, never-raise is
  the rule.
- `COMMIT` attaches `require_unchanged(tid, basis[tid])` for every
  read-set tid, then ships as today. Seek replies carry no LSN and never
  set a basis.
- On `TxnConflict` at COMMIT: **no auto-retry** — the buffered statements
  cannot be replayed client-side (their reads are stale by definition).
  The transaction is closed (the buffer was consumed) and the structured
  conflict error surfaces; the application re-runs the transaction. This
  is ordinary OCC.

## Python / C surface

- Python: new exception `GnitzConflictError(GnitzError)` (registered
  exactly as `GnitzError` is, `create_exception!` + module add,
  `gnitz-py/src/lib.rs:24, 2612`). The generic `to_py_err`
  (`lib.rs:31-33`) flattens every error by `Display` and structurally
  cannot branch, so the mapping happens at the SQL boundary: the
  `execute_sql` binding (`lib.rs:1744`) matches
  `Err(GnitzSqlError::Exec(ClientError::TxnConflict(m)))` explicitly
  before falling back to `to_py_err` for everything else. Retry loops in
  application code become `except GnitzConflictError: retry`.
- `get_lsn` exposed on the Python client (useful for tests and manual
  OCC). C API: `set_error` stores only the message string
  (`gnitz-capi/src/lib.rs:91-95`) and `gnitz_execute_sql` returns `-1`
  on any error today (`lib.rs:1373-1375`); the `Err` arm branches on
  `TxnConflict` and returns the new `GNITZ_ERR_TXN_CONFLICT = -2`
  (generic errors keep `-1`).

## What explicitly does not change

- No per-key or per-range conflict detection: the precondition is the
  whole table. No storage-format, worker, or validation-rule changes;
  the committer's commit path is untouched (only its error payload is
  restructured, and recovery gains only the seed computation at the
  recovery-start step).
- No read-at-LSN or wait-for-LSN read path: scans already drain pending
  ticks before reading (`handle_scan`, `executor.rs:1279-1310`), and the
  committer bumps `tick_tids` before ACKing (`committer.rs:682-694,
  757-763`), so read-your-writes causality across clients already holds
  for scans; preconditions address write-write races, not read freshness.
- The plain push path keeps its wire format and semantics; only the SQL
  layer's RMW statements migrate onto one-family TXN frames.

## Tests

**Rust unit:** frame codec with 0/1/N preconditions (and the
truncated-section decode errors); `require_unchanged` last-write-wins;
map default-to-seed behavior; conflict check ordering (locks → check →
validation) via an engine test that interleaves a push between basis and
commit; the `CommitError` branches — `durable_zone = Some(zone)` bumps
the map via `max`, `None` (fit-check rejection / Phase-A panic) bumps
nothing.

**E2E (pytest, `GNITZ_WORKERS=4`):**
- Lost-update closed: two connections race `UPDATE t SET x = x + 1
  WHERE pk = k` in loops (N rounds each); final `x == 2N` exactly.
- The same race on *different rows* of one table: both succeed, some via
  retry (spurious table-level conflicts absorbed; total row states
  correct).
- Autocommit retry exhaustion: a writer hammering the table makes a
  3-attempt UPDATE surface `GnitzConflictError`; the table state shows
  the UPDATE either fully applied or not at all.
- `BEGIN`/`COMMIT` conflict: txn A reads t (UPDATE resolving rows),
  concurrent connection commits to t, A's COMMIT raises
  `GnitzConflictError`, A's buffered writes are absent, re-running A
  succeeds.
- Read-only precondition: a transaction that reads t (via UPDATE's
  resolve on t feeding writes to u) but writes only u conflicts when t
  changes concurrently.
- SELECT-only tables do not conflict (documented scope): a transaction
  that only SELECTs from t and writes u commits despite concurrent
  writes to t.
- First-read scan basis: a transaction whose **first** read of t was a
  scan performed after a concurrent commit to t does not conflict on
  that commit (the scan saw it); the unsound raise is pinned — a
  transaction that seek-read t, then had a concurrent commit land, then
  scanned t, **does** conflict at COMMIT (the later scan must not raise
  the basis over the earlier read).
- Restart: a basis obtained before a server restart conflicts after it —
  including the drift shape (several pushes after the last checkpoint,
  crash, restart: the recovered seed must dominate the pre-crash
  watermark via the SAL-tail maximum), the double-crash shape (crash,
  boot, immediate second crash: the re-persisted boot seed still
  dominates), and the drain-window shape (a SERIAL allocation between a
  checkpoint's base and ephemeral rounds, then crash: the
  ephemeral-round high-water snapshot covers it, and the ACKed SERIAL
  range itself survives the restart).
- Retried `INSERT ... ON CONFLICT` leaves SERIAL id gaps (asserted, not
  just tolerated).
- `get_lsn` monotonicity across pushes; `STATUS_TXN_CONFLICT` maps to
  `GnitzConflictError` and to `GNITZ_ERR_TXN_CONFLICT = -2` in the C
  API; the conflict message names the table (non-empty through the
  `parse_response` routing).
