# Fail-stop on storage errors while applying committed state

The engine's documented philosophy is that storage failures are fatal
(`storage/error.rs:8-11`), and the runtime consistently enforces it with
`gnitz_fatal_abort!` (`foundation/log.rs:157-162`: log FATAL, then
`libc::_exit(134)`) at durability/consistency faults — committer
(`committer.rs:159,448,541`), executor (`executor.rs:1596,1616`), DDL
compensation (`catalog/write_path.rs:821-827`), reactor/IPC layers. Nine
sites on the apply path took the other branch and silently swallow storage
errors, leaving process state diverged from the durable SAL while the client
holds an ACK — most importantly the **base-table PUSH apply itself**. This
plan routes every one of them into the documented philosophy: **a storage
error while applying committed or allocated state is fatal at the point of
detection; recovery is restart + SAL replay. Where a pre-durability failure
path with sound rollback already exists (master DDL Stage A, boot-time
`CatalogEngine::open`), propagate a `Result` into that existing path instead
— never invent a new handling path.**

Why crash-and-recover is sound (verified): a push is durable once its SAL
zone is fsynced and committed; recovery replays every committed `FLAG_PUSH`
above the per-family flushed-shard watermark
(`runtime/bootstrap.rs:86-137,175-199`), and a batch whose ingest failed was
never folded into a durable shard, so its zone stays above the watermark and
**will** be replayed. The reachable failure is essentially
`StorageError::Io` from the > `INMEM_CEILING` spill
(`storage/lsm/table/flush.rs:366-427`; `Capacity` is unreachable by the
pre-flush check, `table/mod.rs`), and the spill is transactional — a failure
means "this batch was not applied", prior state intact. Silent swallowing is
the one unsound response: the entry is neither applied nor replayed (no
crash → no recovery), and the next successful checkpoint advances the
watermark past it, orphaning it permanently.

---

## The swallow sites and their fates

| # | Site | Today | Fix |
|---|---|---|---|
| S1 | `query/dag/mod.rs:1059` — base-table write in `ingest_store_and_indices` (PUSH apply + SAL replay) | `let _ =`; additionally `ingest_returning_effective` returns `rc = 0` regardless (`:1010-1047`), so `handle_push` cannot observe it | **abort** |
| S2 | `query/dag/mod.rs:1062` — secondary-index write, same function | `let _ =` | **abort** |
| S3 | `catalog/store_io.rs:449` — sys-table ingest in `ddl_sync` | `let _ =`, so a storage failure bypasses the worker's DdlSync-fatal path | **propagate** into the existing DdlSync-fatal shutdown (`worker/mod.rs:802-816`) |
| S4 | `catalog/store_io.rs:460` — `raw_store_ingest` | `let _ =` | **propagate** (consumer hardened by S-R) |
| S-R | `runtime/bootstrap.rs:182-191` — worker replay closure | `replay_ingest(..).is_ok()` only feeds a counter — a propagated `Err` would be re-swallowed here | **abort** on `Err` |
| S5 | `catalog/utils.rs:109-111` — `ingest_batch_into` helper | swallows internally | return `Result`; per-caller handling below |
| S6 | `catalog/ddl.rs:515` — index-backfill chunk in `stream_index_projection` | `let _ =` | **propagate** with `?` (fn already returns `Result<_, String>`) |
| S7 | `ops/index.rs:161` — AVI write in `op_integrate_with_indexes` | `let _ =` while `:92` propagates the base ingest — incoherent contract | **propagate** with `?` (ops stays a `Result` library layer) |
| S8 | `query/vm/exec.rs:288` — WeightClamp history write | `let _ =` | **abort** (shared helper) |
| S9 | `query/vm/exec.rs:381` — whole `op_integrate_with_indexes` Result (includes the `:92` base error and, after S7, the AVI error) | `let _ =` | **abort** (shared helper — the single abort point for the Integrate instruction) |

### S1/S2 — abort in `ingest_store_and_indices`

Add `table_id: i64` to `ingest_store_and_indices` (both callers, `:1002` and
`:1045`, have it in scope; `TableEntry` carries no id):

```rust
if let Err(e) = entry.handle.ingest_borrowed_batch(source) {
    gnitz_fatal_abort!(
        "dag: base-table ingest failed (table_id={}): {} — committed data \
         not applied, state diverged from durable SAL; aborting for \
         restart+replay",
        table_id, e,
    );
}
// index loop (IndexCircuitEntry.index_id at :58):
if let Err(e) = ic.table_mut().ingest_owned_batch(idx_batch) {
    gnitz_fatal_abort!(
        "dag: secondary-index ingest failed (table_id={}, index_id={}): {} \
         — index diverged from base table; aborting for restart+replay",
        table_id, ic.index_id, e,
    );
}
```

`ingest_returning_effective`'s rc plumbing is deliberately untouched: after
this change a storage failure never returns, so `rc < 0` means exactly
"table not registered" (`:1014-1016`) — a request-level error `handle_push`
already routes as STATUS_ERROR. Add one doc-comment line at `:1010` stating
that narrowing so the rc is never mistaken for a storage channel again.

### S3/S4/S-R — catalog apply and replay

```rust
// store_io.rs ddl_sync
table.ingest_owned_batch(batch)
    .map_err(|e| format!("ddl_sync: sys-table ingest failed (table_id={table_id}): {e}"))?;
// store_io.rs raw_store_ingest
entry.handle.ingest_owned_batch(batch)
    .map_err(|e| format!("raw_store_ingest: ingest failed (table_id={table_id}): {e}"))?;
// runtime/bootstrap.rs replay closure
match cat.replay_ingest(msg.target_id as i64, batch) {
    Ok(()) => true,
    Err(e) => crate::gnitz_fatal_abort!(
        "SAL replay apply failed (table_id={}, lsn={}): {} — aborting \
         before the SAL sentinel is reset",
        msg.target_id, msg.lsn, e,
    ),
}
```

S3's consumer chain already exists end-to-end: worker dispatch treats a
`DdlSync` error as fatal (STATUS_ERROR + `shutdown()` + `_exit`), and the
master's `worker_watcher` (`executor.rs:45,358-373`, 100 ms poll) turns the
dead worker into a cluster abort. S-R fires pre-readiness-ACK; the master's
`wait_all_workers` probes `fail_if_worker_dead` (`dispatch.rs:371-373`) and
fails boot **before** the SAL sentinel zeroing (`bootstrap.rs:602-608`), so
the replayed data's only durable copy survives — the same guarantee the
graceful `boot_error` ACK path provides. (S4 itself is production-dead — the
worker replay filter excludes system tids — but the honest contract feeds
the same consumers; its sole unit-test caller already `.unwrap()`s.)

### S5 — `ingest_batch_into` returns `Result`; callers decide

```rust
pub(crate) fn ingest_batch_into(table: &mut Table, batch: &Batch) -> Result<(), StorageError> {
    table.ingest_borrowed_batch(batch)
}
```

- `catalog/bootstrap.rs:124,143,278,295` (`bootstrap_system_tables`,
  returns `Result<_, String>`): `.map_err(...)?` → fails
  `CatalogEngine::open` → server boot returns 1.
- `catalog/write_path.rs:109` (`apply_local`): `.map_err(...)?` → live DDL
  fails **pre-broadcast** with Stage-A compensation — the one place a
  storage error has a sound non-fatal exit (nothing durable, nothing
  broadcast, client not ACKed). Residual accepted: if the failed ingest
  never entered the memtable, compensation replays the batch negated —
  reachability is effectively nil (sys-table batches hit the spill only past
  a multi-MiB ceiling), a deterministic fault re-fails inside compensation
  and lands on the existing `gnitz_fatal_abort!` backstop
  (`write_path.rs:821-827`), and a ghost −1 is invisible to cursors and nets
  to zero against a re-CREATE.
- `catalog/registry.rs:207` (`advance_sequence`, returns `()`): **abort in
  place.** Its callers have already handed the allocated id to memory;
  un-allocating is impossible and replying an error while keeping the bump
  re-issues the id after restart. This is byte-for-byte the existing fatal
  site `executor.rs:1848-1855` (serial-range sequence ingest); match its
  wording: `"sys_sequences ingest (object id advance, seq_id={}) failed: {}
  — allocated id would be reissued after restart"`.
- Test callers (`engine_tests.rs:96,192,225,242`, `index_tests.rs:99`):
  append `.unwrap()`.

### S6 — backfill chunk propagates

```rust
unsafe { &mut *table }.ingest_owned_batch(projected)
    .map_err(|e| format!("index backfill: ingest failed (owner {owner_id}): {e}"))?;
```

Consumers all exist: live master CREATE INDEX → hook error → DDL fails
pre-broadcast (clean client error); worker via `ddl_sync` hooks → S3's
DdlSync-fatal path; boot replay hooks → `CatalogEngine::open` Err → boot
abort. The partially-ingested ephemeral index is already covered by the
duplicate-key error path in the same function (pre-staged dir deletion).

### S7/S8/S9 — tick paths

S7 becomes `?` (matching the base-table `?` at `ops/index.rs:92`), keeping
`ops` a `Result`-returning library layer whose test/bench consumers
`.unwrap()` — an `_exit` inside `ops` would kill the test runner instead of
failing one test. The runtime consumer aborts, mirroring the existing split
where `vm.rs` returns codes and `dag/mod.rs:1644-1655` (`vm_epoch_result`)
aborts. One shared helper in `exec.rs`:

```rust
/// Storage failure while integrating a tick's delta into an owned table:
/// the view/history state has diverged from its durable inputs and there is
/// no sound continue (dropping the delta permanently desyncs the integral).
/// Mirrors dag::vm_epoch_result's fail-stop; recovery is restart+SAL replay.
fn fatal_on_tick_ingest_err(op: &str, table_idx: i32, r: Result<(), StorageError>) {
    if let Err(e) = r {
        crate::gnitz_fatal_abort!(
            "vm: {} ingest failed (table_idx={}): {} — tick state diverged \
             from durable inputs; aborting for restart+replay",
            op, table_idx, e,
        );
    }
}
```

S8: `fatal_on_tick_ingest_err("weight-clamp history", *hist_table_idx, ...)`;
S9: `fatal_on_tick_ingest_err("integrate", *table_idx,
ops::op_integrate_with_indexes(...))`. Funneling these through a new
`vm_epoch_result` error code was rejected: that abort's message is reserved
for malformed-circuit codes and would mislead debugging.

### Explicitly out of scope (legitimate best-effort, verified)

- `worker/mod.rs:1595` `let _ = self.handle_flush_all()` in `shutdown()` —
  exiting process; SAL replay recovers a lost final flush.
- `catalog/bootstrap.rs:299-302` fresh-DB sys flushes — data stays live in
  the memtable and the pre-fork `flush_all_system_tables()`
  (`runtime/bootstrap.rs:337-343`) re-flushes durably and already aborts
  boot on `Err`.
- `catalog/bootstrap.rs:447,453` — `#[cfg(test)]` graceful close.
- The master pre-fork sys-replay closure (`bootstrap.rs:160`,
  `ingest_to_family(..).is_ok()`) — its `Err` mixes replay-idempotency
  precheck rejections with storage errors; making it fatal risks bricking
  boots that recover correctly today. Its storage half is defused twice
  over: a failed `apply_local` never pins the family LSN
  (`write_path.rs:109-112`) so the zone re-replays next boot, and the
  following `flush_all_system_tables` aborts on any real disk fault.

## Correctness positions (decided)

- **Crash loop on a deterministic fault (ENOSPC, dead disk) is accepted and
  intended.** The class already exists (`committer.rs:541` aborts on SAL
  fdatasync failure and re-fails on restart); the loop is observable and
  operator-actionable with zero data loss (the abort preserves the un-reset
  SAL), whereas silence orphans ACKed data permanently once the watermark
  advances; and exit 134 is documented as the convention external
  supervisors (systemd/monit) expect — backoff is the supervisor's job.
  Replay-safety of abort-at-detection is verified: `current_lsn` bumps feed
  only shard naming and the zone-LSN floor, never the recovery watermark,
  which moves only on durable barrier flush.
- **Abort mid-tick / mid-exchange is safe.** Peers die with the cluster
  (worker `PR_SET_PDEATHSIG` + the master's 100 ms `worker_watcher` →
  `shutdown_workers` + reactor shutdown; `worker/exchange.rs:77` documents
  the peer-death case). Clients see a dead socket; the push ACK promised
  durability, which replay honors. The next boot trusts only registered
  shards + the append-only SAL with commit sentinels; W2M rings are per-boot
  `memfd` regions; the spill path is transactional — nothing survives an
  abort in a boot-breaking state.
- **Uniform abort during worker boot replay is acceptable.** Threading a
  `Result` out of shared dag code purely so boot can die politely would
  re-introduce the plumbing whose absence caused this bug class; the master
  already converts a dead child into a clean boot failure before the SAL
  sentinel reset.
- **`gnitz_fatal_abort!` cannot be intercepted**: it is `libc::_exit`, not a
  panic; `guard_panic` (`posix_io.rs:437-445`) is `catch_unwind` and none of
  the nine sites run inside it anyway.

## Fault-injection seam

One debug-only env knob in the house style (`#[cfg(debug_assertions)]` +
env var; precedent `GNITZ_INJECT_DDL_PANIC`):
**`GNITZ_INJECT_INGEST_APPLY_ERROR`** = `store` | `index`, read in
`DagEngine::ingest_store_and_indices`; when armed, substitutes
`Err(StorageError::Io)` for the matching ingest result. No one-shot latch —
the process aborts on first fire. Placing it at the dag layer keeps
`storage` seam-free and fires exactly on the push-apply/replay path
(`raw_store_ingest` and VM integration bypass it), so a seam-armed server
boots cleanly on a pushless data dir and fails deterministically on the
first INSERT apply. No seams for the tick/catalog sites: their tests use the
real-fault recipe (`set_inmem_ceiling_for_test(1)` + removing the table
directory so `open_dirfd` fails with a genuine `Io` in
`spill_in_memory_to_disk`) or the direct-helper subprocess pattern.

## Tests

Rust (`make test`; abort tests use the subprocess pattern of
`test_vm_epoch_result_abort_exit_status`, `dag/mod.rs:2510-2540` — spawn
`current_exe` gated by an env var, assert exit code 134):

1. `query/dag`: `ingest_store_and_indices` aborts under the seam — one
   variant per `store`/`index` (the latter with one index circuit).
2. `query/vm`: `fatal_on_tick_ingest_err` subprocess test asserting 134.
3. `ops/index`: S7 propagation — AVI table with
   `set_inmem_ceiling_for_test(1)` + removed dir →
   `op_integrate_with_indexes` **returns** `Err(StorageError::Io)` (proving
   ops does not abort).
4. `catalog`: S3 propagation — same real-fault recipe on a sys table,
   `ddl_sync` returns `Err` containing `"ddl_sync: sys-table ingest
   failed"`.

E2E (`make e2e`, `GNITZ_WORKERS=4`, extend
`crates/gnitz-py/tests/test_crash_recovery.py`; skip on release builds):

5. `test_ingest_apply_error_aborts_and_replays` (parametrized
   `store`/`index`): phase 1 clean server, CREATE TABLE (+ INDEX for the
   index variant); phase 2 restart with the seam armed, INSERT inside
   try/except (the ACK races the abort), assert the whole cluster exits
   (worker_watcher) and `worker_*.log` contains `FATAL` + `ingest failed`;
   phase 3 restart clean, scan, assert every inserted PK present — the
   load-bearing assertion: ACKed data the old code silently lost is
   recovered by SAL replay.
6. `test_boot_replay_apply_error_aborts_boot`: phase 1 clean server, CREATE
   TABLE, INSERT, SIGKILL (rows live only in the SAL); phase 2 restart with
   the seam armed → process exits nonzero without creating the socket
   (abort inside `recover_from_sal` → `fail_if_worker_dead`), master output
   mentions the worker exiting during recovery sync; phase 3 restart clean →
   rows present (the SAL sentinel was never zeroed).

## Sequencing

Each checkbox is one commit; the tree is green (`make verify` + `make e2e`)
after each.

- [ ] 1. **Push-apply path**: S1 + S2 (`table_id` param, two aborts), the
  seam, the rc doc line; unit test 1; e2e tests 5 + 6.
- [ ] 2. **Tick paths**: S7 (`?`), S8 + S9 (`fatal_on_tick_ingest_err` +
  both call sites); unit tests 2 + 3.
- [ ] 3. **Catalog paths**: S3, S4, S5 (signature + all callers incl. the
  `advance_sequence` abort and five test `.unwrap()`s), S6, S-R; unit
  test 4.
