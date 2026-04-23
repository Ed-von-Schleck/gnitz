# Runtime Invariants

Non-obvious invariants for the reactor, SAL writes, and cross-process
coordination. Update before changing any invariant described here.

---

## Kernel coordination

**M2W eventfd re-arm** — only the master→worker eventfds remain after
the W2M migration. `read()` the eventfd before re-arming `POLL_ADD`;
a counter > 0 fires the next arm immediately.

**SQE buffer lifetime** — memory referenced by an in-flight SQE must
outlive the CQE. Cancelled futures must transfer ownership to the reactor
(`SendFuture::Drop` → `send_buffers_in_flight`). The reactor's
`FUTEX_WAITV` SQE holds a pointer into the boxed `[FutexWaitV]` array
owned by `futex_waitv_storage`; `request_shutdown` must `AsyncCancel`
the SQE and await the cancellation CQE before dropping the storage.

**CQE ordering** — none guaranteed between independent SQEs. Routing uses
wire request IDs, not CQE order.

**Socket flow control** — one outstanding RECV per fd deadlocks pipelined
clients (AF_UNIX 208 KB default buffer). Multiple completed messages must
be buffered in user space; next RECV armed eagerly on `MessageDone`.

---

## Shared state

**SAL write cursor** — group writes are atomic: reserve → fill →
release-store header. Workers observe a group only after the release
(Acquire load).

**W2M ring protocol** — each W2M is a 1 GiB MAP_SHARED memfd mapping
with a 128-byte header (cursors + u32 seq counters + waiter_flags)
followed by an SPSC data region. `write_cursor` and `read_cursor` are
**virtual monotonic offsets** — they only ever increase; physical
position is `phys(virt) = HEADER + (virt - HEADER) % (cap - HEADER)`.
Unread bytes is `vwc - vrc` (always `<= DCAP`); empty is `vwc == vrc`.
Publish: stamp size prefix → `write_cursor.store(Release)` →
`reader_seq.fetch_add(Release)` → if `MASTER_PARKED` set,
`FUTEX_WAKE`. Consume: Acquire `write_cursor`, handle `u64::MAX` SKIP
marker by advancing `vrc` by `pad = cap - phys(vrc)`, decode,
advance `read_cursor`, bump `writer_seq`, wake if `WRITER_PARKED`.

**W2M slot lifetime** — the slot is the producer's the moment
`read_cursor` advances. Any decoded view that holds interior pointers
into the slot (today: `DecodedWire.data_batch`) must own a heap copy
of the slot bytes pinned to `DecodedWire.batch_backing` before the
cursor advances. Control-only replies hold no slot-interior pointers
and decode in place without copying.

**W2M master wait** — one `IORING_OP_FUTEX_WAITV` SQE spans all 64
`reader_seq` words with `FUTEX2_SIZE_U32`, **shared** (no
`FUTEX2_PRIVATE`; W2M is MAP_SHARED). On CQE, drain every ring,
rebuild expected-values array, re-arm. Wake index is not
authoritative.

**W2M capacity invariant** — `capacity >= 2 * MAX_W2M_MSG +
W2M_HEADER_SIZE + 16`. Workers are strictly sequential (one publish
in flight per ring), so this bound is sufficient for SKIP-wrap tail
waste plus 8-byte alignment slack.

**W2M backpressure** — writer on full: `WRITER_PARKED` set → recheck
ring room → `FUTEX_WAIT` on `writer_seq`. Writer clears its own flag
on wake; master never CAS-clears. Master on all-rings-idle:
`MASTER_PARKED` set → `FUTEX_WAITV` → cleared on wake.

**Worker death** — a dead worker parked on `writer_seq` is not a
liveness hazard: `executor::worker_watcher` polls
`MasterDispatcher::check_workers` (a `waitpid`-based crash probe) and
triggers `shutdown_workers` + `request_shutdown`. Symmetric master
death: the worker side polls `getppid` in `worker_main` and exits if
the master's PID changes. FUTEX contracts inherit this abort semantic
— no per-contract liveness claim.

**Checkpoint exclusivity** — committer is the sole checkpoint driver:
`FLAG_FLUSH` → ACKs → `flush_all_system_tables` → `checkpoint_reset`. No
concurrent SAL writer permitted in that window. `maybe_checkpoint` must not
be called from `fan_out_*_async`, `tick_loop_async`,
`execute_pipeline_async`, `broadcast_ddl`, or `single_worker_async`.

`sal_writer_excl` must be held for the **entire** checkpoint sequence (write
+ ACK wait + `checkpoint_post_ack`). Releasing it before the await would let
concurrent writers write SAL groups with the old epoch; workers skip
mismatched epochs, and `checkpoint_post_ack` then resets `write_cursor` to 0,
permanently orphaning those groups and hanging the writers.

**SAL-writer mutex** — `sal_writer_excl: Rc<AsyncMutex<()>>` serialises
tick, commit, DDL, relay, and all read-only fan-out SAL emissions (seek,
scan, pipeline checks, unique-filter warmup). Non-reentrant: holders must not
`.await` anything that could re-acquire it. For fan-out operations the lock
covers only the synchronous write + signal phase and is dropped before
awaiting replies; for checkpoint it spans the full sequence (see above).
Without serialisation, a `FLAG_FLUSH` between `FLAG_TICK` and
`FLAG_EXCHANGE_RELAY` bumps the worker epoch and silently drops the relay;
likewise a fan-out writing during a checkpoint window would be orphaned.

**Per-fd lifecycle** — `register_conn` must clear all sentinels from the
previous incarnation (fd numbers reuse). `recv_closed[fd]` persists until
after `reap_closing_conns` so in-flight `recv().await` still sees EOF.

---

## Behavioral contracts

**Tick before scan** — scan of view V must tick V's source table first;
otherwise the scan sees stale DAG state.

**Unique-filter ordering** — `unique_filter_ingest_batch` must run only
after commit fsync succeeds. On any commit error call
`unique_filter_invalidate_table`.

**DDL durability** — `broadcast_ddl` must `fdatasync` before ACKing the
client; workers have no ACK path for DDL.

**LSN in wire replies** — INSERT: `seek_pk_lo` = commit LSN. SCAN:
`seek_pk_lo` = `last_tick_lsn`. SEEK/SEEK_BY_INDEX: 0.

**Tick batch signalling** — write all FLAG_TICK groups, then call
`signal_all` ONCE. Multiple signals serialise a concurrent broadcast.

**Commit visibility vs. durability** — `signal_all` is pipelined before
fsync; concurrent SCANs can observe in-flight rows. `done` fires only after
`join2(fsync_fut, reply_futs)` — the inserting client sees `Ok(lsn)` only
after fsync. Never send `done` before fsync.

---

## Async scheduling

**Fsync failure** — any push or DDL fsync failure calls
`gnitz_fatal_abort!`. Workers may have observed un-persisted data;
split-brain recovery is not implementable.

**Panic isolation** — `reactor.poll_task` does NOT wrap in `catch_unwind`;
a panic in any task propagates up through `tick` and crashes the process.
Sync calls inside async handlers that can panic on malformed input must use
`guard_panic` so the handler catches the panic, returns `STATUS_ERROR`, and
the task itself remains live.

Wrapped: `ingest_to_family`, `dag.evaluate_dag`, `validate_unique_indices`,
`broadcast_ddl`, `build_merged + write_commit_group`,
`write_tick_group + signal_all`, `relay_exchange`.

Not yet wrapped: `seek_family`, `seek_by_index`, `scan_family`,
`checkpoint_post_ack`, `unique_filter_ingest_batch`.

**Table-lock scope** — constrained tables (FK parent/child, unique index,
Error-mode PK) hold per-table `AsyncMutex` across validate + commit + ack.
Unconstrained tables skip it.

**Task liveness** — dead committer → `committer_tx` backs up → every
`done.rx.await` hangs → catalog read guards accumulate → DDL stalls (full
deadlock). Same for `tick_loop_async` and `relay_loop`. Every sync call in
these tasks that can panic must use `guard_panic`.

---

## Known latent issues

- **L1** ~~Cancelled lock acquires leave stale wakers~~ Fixed: `release()`,
  `release_read()`, `release_write()`, and `WriteFuture::Drop` now wake all
  waiters rather than one, so a stale waker cannot absorb the baton and block
  live waiters.
- **L2** `table_locks` grows unboundedly — no removal on DROP TABLE.
- **L3** DDL first-fsync 500 ms+ (SAL mmap extent extension).
- **L4** Scan paths not wrapped in `catch_unwind` — see panic isolation.
- **L5** Reactor SQ capacity = 256 (auto-flushes; hides backpressure).
- **L6** No graceful shutdown — `request_shutdown` drops in-flight SENDs.
- **L7** Committer single-threaded; all commits serialise.
