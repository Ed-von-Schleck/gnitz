# Runtime Invariants

Non-obvious invariants the master-side async code depends on. Each is
stated positively, with the failure mode spelled out. This is a
reference — the code is the source of truth; read this before changing
anything that touches the reactor, SAL writes, or cross-process
coordination.

---

## I. Architecture

- **Master.** Single-threaded. One `Reactor` drives io_uring, a run
  queue, and a timer heap. Owns catalog, dispatcher, committer, tick,
  accept loop, and per-fd connection loops.
- **Workers.** N single-threaded processes. Read SAL, write W2M.
- **SAL.** 1 GB mmap, master writes, workers read. Group-framed; each
  group has per-worker slots.
- **W2M.** N per-worker rings. Worker writes, master reads.
- **Eventfds.** `m2w_efd[w]` for SAL signalling, `w2m_efd[w]` for reply
  signalling. Pure notification — ordering is via atomic cursors and
  (once fully async) request IDs.

---

## II. Kernel coordination

### II.1 eventfd consumption

`POLL_ADD` does NOT drain the counter. Before re-arming POLL_ADD, the
handler must `read()` the eventfd — otherwise the counter stays > 0
and the next POLL_ADD fires immediately in a tight loop.

### II.2 io_uring SQE buffer lifetime

Memory referenced by an in-flight SQE must outlive the corresponding
CQE. If a future owning such a buffer is cancelled before the CQE
arrives, ownership must transfer to a structure the reactor's CQE
handler controls. `SendFuture::Drop` parks the buffer in
`send_buffers_in_flight`; the `KIND_SEND` handler removes it.

### II.3 CQE ordering

The kernel makes no ordering guarantee between independent SQEs, even
on the same fd. Do not assume SEND-before-RECV or similar. Reply
routing uses wire-level request IDs, not CQE order.

### II.4 Socket-buffer flow control

AF_UNIX default rmem=wmem=208 KB. Under client-side pipelining, a
server with only one outstanding RECV SQE per fd deadlocks: the kernel
buffer fills, the client's `send()` blocks, and if the client is
single-threaded it cannot read responses either.

> The reactor must allow multiple completed messages to queue in user
> space. `pending_recv: HashMap<fd, VecDeque<(ptr, len)>>` serves this;
> `handle_recv_cqe` arms the next RECV eagerly on MessageDone.

### II.5 OP_SEND may return partial

`IORING_OP_SEND` on a stream socket can return rc < len when the
socket's send buffer fills. The remainder is NOT queued — the caller
must resubmit. `Reactor::send_buffer` loops; buffer ownership is via
`Rc<Vec<u8>>` so a cancelled outer future still keeps the allocation
alive until the final CQE.

### II.6 Fdatasync latency is unbounded

btrfs first-fsync on a fresh mmap can take 500+ ms (extent extend +
metadata). Subsequent fsyncs are <1 ms. Handlers awaiting fsync CQEs
must remain responsive to other events — one slow fsync must not
starve the reactor. Current: fsync SQE is submitted eagerly and the
reactor polls other tasks during the wait.

---

## III. Shared state

### III.1 reactor.in_flight[w]

Tracks requests the reactor is routing via `await_reply`, NOT
arbitrary SAL writes.

- Incremented in the same sync window as the SQE submit (BEFORE
  `signal_all`, so the worker cannot reply until the counter is live).
- Decremented by `route_reply` when a waker is present.
- Unrouted replies (req_id == 0 from broadcasts) do NOT decrement.
- FLAG_EXCHANGE wires also do NOT decrement: their tick req_id's
  waker stays parked until the final post-DAG ACK arrives.

The reactor is the sole W2M reader. `drain_w2m_for_worker`
takes care of ring resets in-place (see III.5).

### III.2 SAL write cursor

Monotonic until checkpoint. A group write is atomic:

1. `sal_begin_group` reserves space (advances cursor tentatively).
2. `encode_wire_into` fills in place.
3. `group.commit` publishes via atomic release on the header.

Workers observe a group's header only after commit (Acquire load).

### III.3 Checkpoint exclusivity

**The committer task is the sole driver of SAL checkpoints.**  A
checkpoint (write FLAG_FLUSH → await worker ACKs →
flush_all_system_tables → `sal.checkpoint_reset`) must run with no
concurrent SAL writer. An intermediate write between FLAG_FLUSH and
`checkpoint_reset` is either orphaned (stale epoch) or duplicated
(re-read at a cursor position the master already reused).

No async path (`fan_out_*_async`, `execute_pipeline_async`,
`broadcast_ddl`, `tick_loop_async`, `single_worker_async`)
may call `maybe_checkpoint`. See the doc comment on `maybe_checkpoint`.

### III.3b SAL-writer exclusivity

Tick, commit, DDL, and relay each acquire a single reactor-wide
`sal_writer_excl: Rc<AsyncMutex<()>>` for the duration of any
contiguous SAL group-emission sequence. The mutex is non-reentrant:
no holder may `.await` on anything that could recursively re-acquire
it.

Replaces the deleted `TickIdleBarrier`. Without it, the committer's
`FLAG_FLUSH` could be written between a tick's `FLAG_TICK` group and
the relay's `FLAG_EXCHANGE_RELAY` group; workers bump
`expected_epoch` on FLUSH and silently drop the relay, leaving
`do_exchange_impl` blocked on a 30 s wait forever.

### III.4 Per-fd state lifecycle

Per-fd state lives in `reactor.conns`, `pending_recv`, `recv_closed`,
`recv_waiters`. Kernel fd numbers are reused, so lifecycle must clear
sentinels from the previous incarnation:

- `register_conn(fd)`: fresh `Conn`, clear `recv_closed[fd]`, clear
  `pending_recv[fd]`. Arm first RECV.
- `handle_recv_cqe(fd, res ≤ 0)`: `conn.closing = true`,
  `recv_closed[fd] = true`, wake waiter.
- `reap_closing_conns`: remove conn + recv_waiter + pending_recv,
  close(fd). Keep `recv_closed[fd] = true` so any in-flight
  `recv().await` still sees EOF.
- Outstanding SEND SQEs must complete before fd close — use
  `conn.send_inflight`.

### III.5 W2M per-worker ring reset

`Reactor::drain_w2m_for_worker` is the sole W2M reader; it owns the
per-worker ring reset. After draining replies, if `in_flight[w] == 0`
AND the cursor has crossed `RESET_THRESHOLD` (~half the region), the
reactor calls `W2mReceiver::reset_one_unsafe(w)` and zeros its read
cursor.

Hard safety: warn at 3/4 region, fatal abort at 7/8. The abort is a
backstop for sustained mixed load where `in_flight[w]` rarely hits
zero and the cursor would otherwise overflow.

---

## IV. Behavioral contracts

### IV.1 User-space message buffering is load-bearing

The per-fd VecDeque in `pending_recv` IS the flow-control fix in II.4.
A Stage N rewrite that reduces buffering to a single slot will
deadlock pipelined clients.

### IV.2 Tick before scan of a view

Views derive from source-table pushes through the DAG. A scan of view
V whose source is table T must fire T's tick first, otherwise the
scan sees the DAG's previous state. `handle_scan` loops until no
source table has pending rows. (Bounded perf concern under sustained
concurrent writes; structurally fixed in Stage 6 when tick becomes an
awaited task.)

### IV.3 Unique-filter ordering

`unique_filter_ingest_batch(tid, batch)` must be called AFTER the
commit fsync succeeds. Called earlier and the filter accumulates
phantom keys if fsync fails. `unique_filter_invalidate_table(tid)` on
any commit error.

### IV.4 DDL durability

`broadcast_ddl` writes FLAG_DDL_SYNC and signals workers. Workers
mutate their catalog replica WITHOUT sending an ACK. The master must
`fdatasync` the SAL before ACKing the client — otherwise a crash
between broadcast and client-visible success leaves the workers ahead
of durable master state.

### IV.5 LSN assignment in wire replies

- INSERT response: LSN of this commit in `control.seek_pk_lo`.
- SCAN response: `last_tick_lsn` in `seek_pk_lo`.
- SEEK / SEEK_BY_INDEX: 0.

Clients rely on INSERT LSN for read-after-write ordering.

### IV.6 Tick batch signalling

`tick_loop_async` writes N FLAG_TICK groups to SAL (one per pending
tid, with per-worker req_ids) and signals ONCE via `signal_all`.
Multiple signals would wake workers N times and serialise what
should be a concurrent broadcast.

---

## V. Async scheduling

### V.1 Serial connection_loop per fd

At most one `handle_message` future per fd at a time — per-session
FIFO by construction. One push at a time per fd reaches `committer_tx`;
the catalog read guard is held by at most one handler per fd.

### V.2 Writer preference on catalog_rwlock

Writer-preference rwlock: once a DDL writer parks, new INSERT readers
block. Without this a continuous INSERT stream starves DDL.

### V.3 Fsync handling

- Success (rc ≥ 0): proceed.
- Failure (rc < 0) on push or DDL: `gnitz_fatal_abort!`. Workers may
  have observed data the master never persisted; split-brain recovery
  is not implementable.

### V.4 Panic isolation

`reactor.poll_task` wraps `future.poll` in `catch_unwind`: a task
panic kills the task, not the reactor. Sync calls from async handlers
that can panic on client-visible malformed input must be wrapped in
`catch_unwind` inside the handler — the handler sends STATUS_ERROR
and returns.

Currently wrapped: `cat.ingest_to_family`, `dag.evaluate_dag`,
`cat.validate_unique_indices`, `disp.broadcast_ddl`, the committer's
`build_merged + write_commit_group`, the tick task's
`write_tick_group + signal_all`, the relay task's `relay_exchange`.

Not wrapped (latent): `cat.seek_family`, `cat.seek_by_index`,
`cat.scan_family`, `checkpoint_post_ack`, `unique_filter_ingest_batch`.

### V.5 oneshot / mpsc drop semantics

- Drop `oneshot::Sender` before send → Receiver gets `Err(Cancelled)`.
- Drop `oneshot::Receiver` → Sender's `send` returns `Err(v)`; callers
  typically `let _ =`.
- Drop all `mpsc::Sender`s → parked Receiver gets `None`; the loop
  must treat `None` as termination.

### V.6 Table-lock scope (I8)

For constrained tables (FK parent, FK child, has_any_unique_index, or
Error-mode on unique PK), the INSERT handler holds the per-table
`AsyncMutex` across validate + commit + ack. This serialises
same-table INSERTs so validation sees a consistent committed state.
Unconstrained tables skip the lock.

### V.7 Committer / tick / relay liveness

The committer task must never die without being replaced. A dead
committer means `committer_tx` backs up, every push's `done.rx.await`
hangs, and because handlers still hold the catalog read guard, DDL
stalls too — full-server deadlock.

The same applies to `tick_loop_async` (handlers waiting on a
`TickTrigger` would hang) and `relay_loop` (workers blocked in
`do_exchange_impl` would time out at 30 s and the next tick would
fail).

Every sync call in these tasks that could panic on
malformed-but-decoded input must be wrapped in `guard_panic`. On
panic, the affected request returns an error; the task keeps
running. See V.4 for the wrapping status.

---

## VI. Known latent issues

- **VI.1 AsyncMutex / AsyncRwLock cancellation.** Cancelled acquire
  futures leave stale wakers in the queue. When released, the wake is
  a no-op on a dead task — wasted wakeup, no deadlock. Fix: Drop
  impls that remove the waker entry.
- **VI.2 `table_locks` grows unboundedly.** No removal on DROP TABLE.
  Low impact (tables are long-lived).
- **VI.3 DDL first-fsync latency.** Every DDL fdatasyncs the SAL;
  first fsync after SAL allocation is 500+ ms on btrfs. Schema+table
  creation doubles it. Options: batch DDL fsyncs with the next
  commit, pre-fsync at startup, or use non-btrfs storage.
- **VI.4 Unwrapped panics in catalog scan paths.** See V.4 "Not
  wrapped" list.
- **VI.5 Reactor SQ capacity = 256.** Auto-flushes, so not a
  correctness issue, but hides backpressure.
- **VI.6 No graceful shutdown.** `request_shutdown` drops in-flight
  SENDs and spawned tasks. Fine for process exit, not for
  hot-reload / clean SIGTERM.
- **VI.7 Committer single-threaded.** All commits serialise through
  one task. Multi-table workloads with independent commits could
  parallelise; not in scope today.

---

## VII. Using this document

When changing async or kernel-interface code:

1. Identify which invariants the change might affect.
2. Verify the new code preserves each — walk the code, not just the
   diff.
3. If an invariant is modified, update this document first, then the
   code.
