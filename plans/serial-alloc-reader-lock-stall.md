# Release the catalog reader lock before the SERIAL allocation fsync

`commit_serial_range_durable` (`runtime/orchestration/executor.rs`) holds
`catalog_rwlock.write()` across the SAL `fdatasync` and the `ingest_lsn.set()`
that follows it. While it is parked on that fsync, **every** catalog reader —
`FLAG_SEEK` / `FLAG_SEEK_BY_INDEX` / `FLAG_SEEK_BY_INDEX_RANGE` /
`FLAG_GET_INDICES` handlers and `run_tick` tick emission, all of which take
`catalog_rwlock.read()` — is blocked for the full fsync (~ms). A client reserves
a SERIAL range every `SERIAL_RANGE_SIZE = 64` inserts, so under multi-client
SERIAL-PK insert load these fsyncs recur constantly and couple their latency onto
all seek/scan/tick latency.

The write lock is **load-bearing for correctness**, not incidental (its doc
comment spells this out): without it, two concurrent range requests read the same
stale `ingest_lsn`, compute the same `zone_lsn`, and double-open / double-commit
/ double-broadcast one LSN, violating strict-monotonic zone LSNs and letting a
checkpoint watermark dedup-drop a committed-but-unflushed advance on recovery
(re-issuing a committed id). So the naive fix (downgrade to a read lock) is
wrong. This plan releases the reader-blocking lock **before** the fsync while
preserving every invariant, via a reserved-vs-published LSN split.

Pre-alpha: no compatibility concern.

---

## Scope

The lock-release change applies to `commit_serial_range_durable` **only**. The
committer (`committer.rs`) and `handle_ddl_txn` (`executor.rs`) receive a small,
mandatory bookkeeping change — a reserved-vs-published LSN split — that is
required to keep SERIAL's early release safe; their lock structure is otherwise
untouched.

DDL's own lock-across-fsync is deliberately left in place: CREATE/DROP/CREATE
INDEX is rare schema traffic on no latency-sensitive path, its post-fsync tail
mutates catalog/dag state (`defer_pending_dir_deletions`, `unique_filter_*`,
`fan_out_backfill`) under the write lock, and it is already heavyweight (committer
barrier, `TickGate` quiesce). Making it lock-free would mean auditing all of that
for reader-safety for zero latency benefit. SERIAL is the hot path; DDL is not.

## Facts established from source

- **The master executor is single-threaded cooperative** (`runtime/reactor`):
  tasks interleave only at `.await` points. `ingest_lsn` is `Rc<Cell<u64>>` (not
  atomic). `AsyncMutex::lock()` on a free lock returns `Ready` without yielding.
  `AsyncRwLock` is writer-preferring. Global lock order is
  `catalog_rwlock` → `sal_writer_excl` (INSERT and SEEK both take `catalog`
  first; the committer takes only `sal_writer_excl`).
- **The SERIAL critical section is entirely synchronous.** In
  `commit_serial_range_durable` the only `.await`s are the two lock acquisitions
  and the fsync. `reserve_user_sequence`, `max_table_current_lsn`,
  `open_ddl_zone`, `ingest_to_family`, `drain_pending_broadcasts`,
  `broadcast_ddl`, `commit_zone` are synchronous.
- **The zone pin bumps `max_table_current_lsn()` synchronously.**
  `ingest_to_family(SEQ_TAB_ID, …)` → `apply_local` sets
  `table.current_lsn = zone_lsn` (`write_path.rs`) using `ctx.ddl_zone_lsn()`.
  `ctx.ddl_zone_lsn` is a **single shared slot** consumed only during that
  ingest; nothing after `ingest_to_family` reads it (`broadcast_ddl`/`commit_zone`
  take `zone_lsn` as an explicit argument). DDL's late `close_ddl_zone` is safe
  today only because DDL holds `catalog_rwlock.write()` across it.
- **The committer is a second, lock-free zone allocator.** It computes
  `zone_lsn = ingest_lsn.get() + 1` and publishes `ingest_lsn.set(zone_lsn)`
  after its fsync holding **no** `catalog_rwlock`. Today it is quiesced during
  SERIAL/DDL because the INSERT handler holds `catalog_rwlock.read()` across the
  whole committer round-trip, so `catalog_rwlock.write()` cannot be granted while
  a push is in flight — i.e. **SERIAL's write-lock-across-fsync is the sole thing
  keeping the committer from allocating a colliding zone during a SERIAL fsync.**
  Releasing it early is unsafe unless the committer is made concurrency-safe.
- **`ingest_lsn` is the durability watermark** consumed by `run_tick` (snapshot
  into `last_tick_lsn`, reported to SEEK/SCAN clients). Publishing a non-durable
  LSN is exactly the bug the "publish only after fsync" rule prevents — so the
  reservation counter must be separated from the published watermark.

## Design

**(A) Reserved-vs-published LSN split.** Add `reserved_lsn: Rc<Cell<u64>>`
alongside `ingest_lsn`, seeded identically at boot. `reserved_lsn` is the
monotonic *allocation high-water* (bumped synchronously at reservation, may lead
durability); `ingest_lsn` stays the *durability watermark* (published after fsync,
by monotonic max). Every zone allocator — committer, SERIAL, DDL — reserves from
`reserved_lsn` and publishes to `ingest_lsn` with `max`.

**(B) SERIAL reserves + mutates + emits under both locks, then releases both
before the fsync.** Acquire `catalog_rwlock.write()` then `sal_writer_excl`
(matching global lock order); reserve `zone_lsn = reserved_lsn.get().max(
max_table_current_lsn()) + 1` under `sal_writer_excl`; run the synchronous catalog
mutation with a fully self-contained `open_ddl_zone…close_ddl_zone`; emit the SAL
group; submit the fsync SQE; **drop both guards**; `await` the fsync with no locks
held; publish `ingest_lsn` via monotonic max.

The invariant that makes the split safe is **SAL physical write order == zone-LSN
order across all allocators**: every allocator reserves its `zone_lsn` and writes
its SAL group while holding `sal_writer_excl` with no intervening `.await`
(committer, SERIAL), or with the committer quiesced and no other reserver able to
interleave (DDL). Given that, any completed fsync (which flushes all bytes written
before its submission) implies every lower-numbered zone is already durable, so a
monotonic-max publish never exposes a non-durable LSN and never regresses. As a
bonus, SERIAL fsyncs now overlap with each other and with the committer's
(io_uring holds multiple in-flight fsync SQEs).

### `executor.rs` — `Shared` struct (after `ingest_lsn`)

```rust
    ingest_lsn: Rc<Cell<u64>>,
    /// Allocation high-water for durable zone LSNs, distinct from the durability
    /// watermark `ingest_lsn`. Bumped synchronously when the committer / DDL /
    /// SERIAL path reserves a zone; may lead `ingest_lsn` while a zone's fdatasync
    /// is in flight. Every allocator reserves `reserved_lsn + 1` and publishes
    /// `ingest_lsn` (monotone max) only after its fsync, so readers never see an
    /// LSN whose data is not yet on disk.
    reserved_lsn: Rc<Cell<u64>>,
```

### `executor.rs` — `ServerExecutor::run` construction

```rust
        let ingest_lsn = Rc::new(Cell::new(initial_ingest_lsn));
        let reserved_lsn = Rc::new(Cell::new(initial_ingest_lsn));
```

Add `reserved_lsn: Rc::clone(&reserved_lsn),` to both the `committer::Shared { … }`
and the `Shared { … }` literals (after each `ingest_lsn` field).

### `committer.rs` — `Shared` struct (after `ingest_lsn`)

```rust
    pub ingest_lsn: Rc<Cell<u64>>,
    /// Allocation high-water shared with the executor. Reserved under
    /// `sal_writer_excl` so reservation order == SAL write order across the
    /// committer, DDL, and SERIAL allocators.
    pub reserved_lsn: Rc<Cell<u64>>,
```

### `committer.rs` — Phase B reservation

Move the reservation inside the `sal_writer_excl` block and source it from
`reserved_lsn` (keep `zone_lsn` in function scope for the Phase D publish via
deferred init):

```rust
    // (delete the pre-lock `let zone_lsn = shared.ingest_lsn.get() + 1;`)
    let zone_lsn;
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;

        // Reserve under sal_writer_excl so reservation order == SAL write order
        // across every durable committer (this task, DDL, SERIAL).
        zone_lsn = shared.reserved_lsn.get() + 1;
        shared.reserved_lsn.set(zone_lsn);

        for g in groups.iter_mut() {
            // … unchanged …
```

### `committer.rs` — Phase D publish

```rust
    if groups.iter().any(|g| g.write_err.is_none()) {
        shared.ingest_lsn.set(shared.ingest_lsn.get().max(zone_lsn));
    }
```

### `executor.rs` — `handle_ddl_txn` reservation + publish

Lock structure unchanged; only the counter usage changes so DDL participates in
the shared reservation counter (else a later committer/SERIAL reserving
`reserved_lsn + 1` could collide with DDL's zone):

```rust
    let zone_lsn = shared
        .reserved_lsn
        .get()
        .max(unsafe { (*cat_ptr_raw).max_table_current_lsn() })
        + 1;
    shared.reserved_lsn.set(zone_lsn);
    let zone_lsn_nz = NonZeroU64::new(zone_lsn).expect("zone LSN allocator starts above 0");
    // … at publish (after fsync):
    shared.ingest_lsn.set(shared.ingest_lsn.get().max(zone_lsn));
```

### `executor.rs` — `commit_serial_range_durable` (full rewrite)

Rewrite the doc comment to describe the new invariant inline (do not reference
this plan), then:

```rust
async fn commit_serial_range_durable(shared: &Rc<Shared>, seq_id: i64, count: i64) -> i64 {
    // Reserve + mutate + emit under BOTH the catalog write lock and the
    // SAL-writer lock, releasing both BEFORE the fdatasync. Everything in this
    // block is synchronous — the only awaits are the two lock acquisitions — so
    // catalog readers (SEEK / SEEK_BY_INDEX* / GET_INDICES / tick emission) block
    // for this brief span only, never across the fdatasync.
    let (base, zone_lsn, fsync_fut) = {
        // Lock order catalog -> SAL, matching INSERT/SEEK, so acquiring SAL under
        // catalog.write cannot deadlock.
        let _write = shared.catalog_rwlock.write().await;
        let _sal_excl = shared.sal_writer_excl.lock().await;

        let cat_ptr = shared.catalog;
        let (base, delta) = unsafe { (*cat_ptr).reserve_user_sequence(seq_id, count) };

        // Reserve from the shared high-water under sal_writer_excl, so reservation
        // order == SAL write order across every durable committer. Dominate every
        // family current_lsn so a checkpoint watermark cannot dedup-drop this
        // advance on recovery.
        let zone_lsn = shared
            .reserved_lsn
            .get()
            .max(unsafe { (*cat_ptr).max_table_current_lsn() })
            + 1;
        shared.reserved_lsn.set(zone_lsn);
        let zone_lsn_nz = NonZeroU64::new(zone_lsn).expect("zone LSN allocator starts above 0");

        // Full open..pin..close lifecycle inside this one await-free write-lock
        // critical section, so the single ctx.ddl_zone_lsn slot is never observed
        // by a concurrent DDL/SERIAL once the write lock is released below.
        let ingest_res = unsafe {
            (*cat_ptr).ctx.open_ddl_zone(zone_lsn_nz);
            (*cat_ptr).ingest_to_family(SEQ_TAB_ID, &delta)
        };
        if let Err(e) = ingest_res {
            gnitz_fatal_abort!("sys_sequences ingest (serial range) failed: {}", e);
        }
        unsafe {
            (*cat_ptr).ctx.close_ddl_zone();
        }

        // SAL emission under sal_writer_excl. A failure after the in-memory
        // mutation permanently diverges master/worker state — abort.
        let drained = unsafe { (*cat_ptr).drain_pending_broadcasts() };
        let disp = shared.dispatcher;
        if let Err(e) = guard_panic("serial-range-broadcast", || unsafe {
            for (tid, bat) in &drained {
                (*disp).broadcast_ddl(*tid, bat, zone_lsn)?;
            }
            (*disp).commit_zone(zone_lsn)?;
            Ok::<(), String>(())
        }) {
            gnitz_fatal_abort!("serial-range broadcast failed after in-memory catalog mutation: {}", e);
        }

        // Submit the fdatasync SQE (synchronous — returns a future). Both guards
        // drop as this block ends, before the await below.
        (base, zone_lsn, shared.reactor.fsync(shared.sal_fd))
    };

    if fsync_fut.await < 0 {
        gnitz_fatal_abort!("SAL fdatasync (serial range) failed");
    }

    // Publish only after fsync, monotonically. SAL write order == zone order, so a
    // completed fsync implies every lower zone is durable; max() never regresses.
    shared.ingest_lsn.set(shared.ingest_lsn.get().max(zone_lsn));
    base
}
```

## Correctness argument

The invariants the original doc comment requires: (i) no two range requests share
a `zone_lsn`; (ii) strict-monotonic zone LSNs; (iii) `ingest_lsn` published only
after fsync so a checkpoint watermark can't dedup-drop a committed-but-unflushed
advance and re-issue an id.

- **(i) Distinct zone LSNs.** Every allocator reserves `reserved_lsn + 1` and
  immediately `set`s `reserved_lsn`. Committer and SERIAL do this under
  `sal_writer_excl` (mutually exclusive); SERIAL additionally under
  `catalog_rwlock.write()`; DDL reserves under `catalog_rwlock.write()` with the
  committer quiesced. No two reservations read the same `reserved_lsn`, so no two
  zones share an LSN. SERIAL's `max(reserved_lsn, max_table_current_lsn())` still
  dominates any drifted family counter — strictly stronger than before, since
  `reserved_lsn ≥ ingest_lsn` always.
- **(ii) Strict monotonicity.** `reserved_lsn` only ever `set`s to a strictly
  greater value. The `ctx.ddl_zone_lsn` clobber hazard is eliminated: SERIAL's
  `open_ddl_zone…ingest…close_ddl_zone` is fully contained in one await-free
  `catalog_rwlock.write()` critical section, so the single slot is never non-`None`
  outside a write-lock holder (DDL's late close remains safe because DDL still
  holds the write lock across it).
- **(iii) Publish-after-fsync.** The pin (`sys_sequences.current_lsn = zone_lsn`)
  happens synchronously under the locks, before release — identical to today, so
  recovery's `msg.lsn <= flushed` dedup still matches the SAL group LSN.
  `ingest_lsn` is published only after `fsync_fut.await` succeeds, via `max`.
  **SAL-write-order == zone-order** (every allocator reserves and emits its SAL
  group under `sal_writer_excl` with no `.await` between reserve and emit; DDL with
  the committer quiesced) guarantees that when any zone's fsync completes, all
  lower zones' earlier-written SAL bytes are also durable, so every published
  `ingest_lsn` is backed by durable data.

The raw-pointer catalog mutation stays exclusive — it runs entirely within the
synchronous `catalog_rwlock.write()` section with no `.await`, so no
`&mut CatalogEngine` aliases across an await and no reader interleaves. The
catalog is not touched after the write lock drops (`broadcast_ddl` / `commit_zone`
/ fsync / `ingest_lsn.set` are dispatcher/reactor/cell operations; the last
catalog mutation, `drain_pending_broadcasts`, runs before the drop).

**Committer concurrency (the new interleaving).** After SERIAL releases
`catalog_rwlock.write()` early, an INSERT may take `catalog_rwlock.read()` and
drive a committer commit concurrently with SERIAL's in-flight fsync. Safe: SERIAL
emitted its (lower) SAL group before releasing the locks; the committer reserves a
strictly higher zone under `sal_writer_excl` and emits after — SAL order == zone
order holds, and both publish by monotonic max after their own fsyncs. The same
holds for a DDL acquiring the write lock while a SERIAL fsync is outstanding (DDL
barriers the committer, reserves higher via `max_table_current_lsn()`/`reserved_lsn`,
emits after the already-written lower SERIAL zone).

## Measurement (lead with this; the design does not depend on the number)

- **Primary — reader tail latency under SERIAL load.** Concurrent clients doing
  SERIAL-PK inserts (forcing `alloc_serial_range` every 64 rows) via the binary
  `push()` API (not `INSERT VALUES`, which is parse-bound), while a separate client
  streams `SEEK` / `SEEK_BY_INDEX`; record p50/p99/p999 SEEK latency. Expect the
  p99/p999 tail (currently a full `fdatasync` per SERIAL allocation) to collapse
  toward the no-SERIAL baseline.
- **Secondary — SERIAL allocation throughput** with N concurrent SERIAL clients
  (overlapped fsyncs).
- **Guardrail — no INSERT/commit regression.** `make bench-full` before/after,
  interleaved A/B/A/B given its ±3–4 % run-to-run variance; the pure-push path must
  be within noise.

## Tests

- **Distinct-monotonic zones under overlap (Rust unit).** Seed `reserved_lsn = L`;
  simulate two overlapping reservations (reserve A → reserve B before A publishes)
  and assert `zone_A = L+1`, `zone_B = L+2`, `reserved_lsn = L+2`, and that
  `reserve_user_sequence` returns disjoint increasing bases (`[1,65)` then
  `[65,129)`).
- **Monotonic-max publish (Rust unit).** With `ingest_lsn = X` and two out-of-order
  fsync completions publishing zones `X+1` and `X+2`, assert `ingest_lsn` ends at
  `X+2` and never regresses regardless of completion order.
- **End-to-end concurrency (Python e2e, `GNITZ_WORKERS=4`).** Many concurrent
  clients inserting into a SERIAL-PK table interleaved with SEEKs; assert (1) all
  generated ids are globally unique and strictly increasing per client range, (2)
  reported `last_tick_lsn` is non-decreasing, (3) after a mid-run restart no SERIAL
  id is re-issued. Multi-worker is mandatory (the reader-blocking path is
  exchange/fanout-adjacent).
- **Reader-not-blocked (targeted async test).** Mirror the existing
  `sal_writer_excl_not_held_across_commit_await` test: assert `catalog_rwlock` is
  not held across the SERIAL fsync await, and that a concurrent SEEK completes
  before the SERIAL response.

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`) after
each.

- [ ] 1. **Reserved-vs-published LSN split.** Add `reserved_lsn` to both `Shared`
  structs, seed at boot; committer reserves under `sal_writer_excl` from
  `reserved_lsn` and publishes `ingest_lsn` by monotonic max; `handle_ddl_txn`
  reserves from `reserved_lsn` and publishes by max. No behavior change yet
  (SERIAL still holds the write lock across its fsync). Unit tests for the LSN
  algebra.
- [ ] 2. **Release SERIAL's locks before the fsync.** Rewrite
  `commit_serial_range_durable` per §design; reserve/mutate/emit under both locks,
  drop before the fsync, publish by max. E2E concurrency + reader-not-blocked
  tests; the measurement.
