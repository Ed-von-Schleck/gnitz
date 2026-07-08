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
liveness hazard: `executor::watchdog` polls
`MasterDispatcher::check_workers` (a `waitpid`-based crash probe) and
triggers `shutdown_workers` + `request_shutdown`. Symmetric master
death: the worker side polls `getppid` in `worker_main` and exits if
the master's PID changes. FUTEX contracts inherit this abort semantic
— no per-contract liveness claim.

**Checkpoint exclusivity** — committer is the sole steady-state checkpoint
driver, now a three-step sequence (`run_checkpoint_sequence`): step 0 bumps the
checkpoint generation (lock-free); step 1 is the **base round** (`FLAG_FLUSH` →
ACKs → `checkpoint_post_ack`); step 2 **drains** the dirty views (a
`TickTrigger::Drain` then `TickTrigger::Quiesce`, lock released so the tick task
can acquire it per tick); step 3 is the **ephemeral round** (`FLAG_FLUSH_EPH` →
ACKs → bare `checkpoint_reset`) that persists view trace/output state stamped
with the generation. No concurrent SAL writer is permitted inside any round.
`maybe_checkpoint`/`do_checkpoint` must not be called from `fan_out_*_async`,
`tick_loop_async`, `execute_pipeline_async`, `broadcast_ddl`, or
`single_worker_async` — the committer sequence (or boot-end `boot_checkpoint`,
pre-reactor) owns SAL checkpoint exclusivity.

`sal_writer_excl` is held for the **entire** window of **each round** (write +
ACK wait + reset), but **released across the step-2 drain** (which acquires it
per tick). Releasing it before a round's await would let concurrent writers
write SAL groups with the old epoch; workers skip mismatched epochs, and the
round's reset then orphans those groups and hangs the writers.

**SAL prefix-epoch gate** — a group's size and epoch are published together in
the one atomically-stored u64 prefix (`(epoch << 32) | payload_size`,
Release-stored by `SalGroup::commit`, Acquire-loaded by
`sal_read_group_header`). A live reader dereferences a group's header/payload
bytes **only after** the prefix's epoch equals its `expected_epoch`; on any
mismatch it parks (`NO_MESSAGE`) without a single plain read. This is what
makes the post-checkpoint in-place rewrite of offset 0 safe: a slot's prefix
transitions `0 → (epoch | size)` at most once per epoch, and slot reuse always
changes the published word (the reset bumps the epoch), so a matching prefix
proves the group is fully written, immutable, and ordered-visible. The
ungated (`expected_epoch = None`) read exists only for the recovery walkers,
which run against a quiescent SAL with no concurrent writer.

**Drain-window servicing.** While the committer awaits a Drain `done` /
Quiesce `acked` (`await_servicing`), it keeps the SAL live: a **Reclaim**
Barrier (relay-space) is serviced by a reclaim-only base round (no gen
re-bump) so `relay_loop` can refill space; the triggering reclaim barriers are
signaled **right after step 1** (before the drain) to avoid a relay deadlock;
**Ddl**/**Shutdown** Barriers are deferred to sequence end (servicing a DDL
mid-sequence would interleave a CREATE VIEW's reactor-parked backfill); pushes
stay queued and fold into the post-sequence commit. The **quiesce** parks the
tick loop across the ephemeral round so no tick runs during the view flush.
Graceful shutdown sends one **Shutdown** Barrier, which both forces the full
sequence for its batch and resolves only at sequence end.

**Recovery ordering** (`server_main`, pre-reactor). Boot recovers in a fixed
order so a crash at any point rebuilds rather than silently resumes stale views:
(1) master opens the catalog + replays system-table DDL; (2) master computes the
per-view resume verdict (`compute_invalid_views`) and does the **recovery-start
generation bump** (durable `G → G+1`, without publishing to `worker_ctx`), both
pre-fork; (3) each forked worker rebuilds indexes, replays the SAL push tail
(buffering each view-feeding base's effective delta into `pending_deltas`), and
**boot-flushes** the replayed base rows durable; (4) master collects readiness
ACKs and **resets the SAL**; (5) master **tick sweep** — one `drain_tick_blocking`
per reachable base drains `pending_deltas` into every view (resumed views extended
state-exactly, invalid views polluted); (6) master **rebuilds** only the invalid
views — each worker resets its own output partitions + operator scratch on the
first backfill command, then `fan_out_backfill(vid, src)` refills; (7)
`boot_checkpoint` bumps `G+1 → G+2` and durably checkpoints the resumed+rebuilt
state. The monotonic generation is the crash-window guard: the recovery-start bump
covers the reset→boot_checkpoint gap (durable gen ≥ `G+1` while un-checkpointed
views are stamped `G` ⇒ forced rebuild), and the committer's step-0 bump covers a
steady-state checkpoint crash.

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
Unconstrained tables skip it. For FK constraints, `fk_lock_set` returns the
full neighborhood (target + all FK parents + all FK children); all are
acquired in ascending tid order before validation begins. Sorted acquisition
prevents deadlock between concurrent child INSERT and parent DELETE, which
otherwise pass their respective FK/RESTRICT validations against a stale view.

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
