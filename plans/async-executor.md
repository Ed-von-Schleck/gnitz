# Async Executor: From Sync-with-Overlays to a Reactor

---

## Unifying Principle

The master process is already asynchronous — it just encodes async patterns as
hand-rolled state machines scattered across eight separate mechanisms. Each
mechanism is locally reasonable; cumulatively they form most of the executor's
incidental complexity. A single-threaded reactor on top of io_uring collapses
all eight into one primitive (futures suspended on CQEs), making the
concurrency structure explicit and uniform instead of accidental and varied.

This is not a performance plan. It is a simplification plan. Performance is a
neutral-to-positive consequence; the reason to do it is that the end-state has
fewer concepts, fewer invariants to remember, and fewer ways to get subtle
ordering wrong.

---

## Implementation Status

| Stage | Status | Landed in |
|---|---|---|
| 0 — W2M request IDs | ✅ done | `8b5a186` |
| 1 — Reactor primitive | ✅ done | `716075e` |
| 2 — Async SELECT (pk_seek + cached index_seek + single-worker scan) | ✅ done | `6abb21c`, `308df12`, `2cca343` |
| 3 — Async fdatasync via reactor | ⏳ pending | — |
| 4 — Async group commit (committer task) | ⏳ pending | — |
| 5 — Async pipelined checks + validation | ⏳ pending | — |
| 6 — Async tick pipeline | ⏳ pending | — |
| 7 — Delete main-loop timeout math | ⏳ pending | — |

See `### Lessons from Stages 0-2` below for constraints discovered on the
way that Stage 3+ authors should internalize before planning their work.

---

## The Nine Manual-Async Mechanisms (current state)

Each of these exists because some part of the master's work needs to overlap
with some other part, or because the sync model's ordering invariants leak
into other components. Each was implemented ad-hoc. Together they span ~900
lines of bookkeeping.

### 1. `AsyncFsync` — a one-off io_uring mini-runtime

`ipc::AsyncFsync` opens its own io_uring instance solely to submit `fdatasync`
non-blockingly. It has its own `submit()` / `wait_complete()` state and is
threaded through `executor.rs` as a dedicated field. ~44 LoC of infrastructure
to say "don't block on fsync."

### 2. `pre_write_pushes` / `complete_pre_written` — manual Phase A/B overlap

During tick evaluation (workers are CPU-busy), the master wants to write the
next push group to SAL and submit its fdatasync *concurrently*. The current
solution:

- `pre_write_pushes`: encode group to SAL, submit async fsync, stash result in
  `pre_written: Option<PreWritten>`
- `complete_pre_written`: on tick completion, wait fsync, collect ACKs, reply
  to clients

~180 LoC implementing what is in essence `join!(tick_eval, pre_commit)`.

### 3. `TickState::{Idle, Active}` machine

Exists specifically to gate which overlap patterns are legal: pre-writes are
only allowed during `Active`, deferred requests are processed when returning to
`Idle`, etc. State transitions are threaded across `flush_pending_pushes`,
`process_deferred_requests`, `start_all_async_ticks`, and the main loop
(~100 LoC).

### 4. `deferred_requests: Vec<DeferredRequest>` queue ✅ eliminated (Stage 2)

SELECT / seek / scan requests that arrive while DML is pending must wait until
DML commits (to see committed state). Rather than blocking the dispatch of
new client messages, the current code queues them and processes after pending
drains. This is a manual "await on prior commits" point (~80 LoC).

*Replaced by reactor tasks awaiting `tick_idle_barrier`; `deferred_requests` /
`process_deferred_requests` deleted in `6abb21c`. The matching "truly async"
fan-outs for cached paths (pk_seek, cached index_seek, single-worker scan)
land in `2cca343`. Cache-miss seek_by_index and multi-worker scan still
fall back to the sync path — see Stage 5 for the broadcast routing that
removes the fallback.*

### 5. Main-loop timeout math

```rust
let timeout_ms = if tick_active { 1 }
                 else { min(500, tick_deadline_remaining, flush_deadline_remaining) };
```

Exists because worker eventfds, tick deadlines, and flush deadlines are *not*
registered with io_uring, so the main loop must periodically wake up to check
them. ~30 LoC, plus the conceptual burden of "don't sleep longer than the
nearest non-io_uring event."

### 6. `PendingBatch` vs `deferred_requests` bifurcation

Two parallel queuing systems with different semantics:

- `PendingBatch` (INSERTs): grouped by target_id, drain-all semantics, feeds
  group commit
- `deferred_requests` (reads + some DDL): flat vec, processed after pending
  drains

Two drain paths, two `is_empty` checks, two lifecycle rules. ~200 LoC combined.

### 7. The `collect_*` family

`collect_one`, `collect_acks`, `collect_acks_at`, `wait_all_workers`, plus
`execute_pipeline`'s inner drain loop — each hand-rolls "wait for N responses,
handle errors, reset cursors." ~200 LoC total. All are variants of
`try_join_all(worker_replies)` with different error-handling shapes.

### 8. Cursor-position correlation + cursor management 🔄 partially eliminated

The W2M wire protocol carries no request ID. Correlation is by *position in
the W2M ring*: the k-th message at worker W corresponds to SAL message K in
submission order. This is a clever sync-era optimization, but it spreads
correlation concerns across every `collect_*` call site: cursor state must be
threaded, reset points are rule-governed, and any reordering breaks
correctness silently.

*Request IDs in the wire format (Stage 0) + reactor-owned drain (Stage 2)
give req_id-based routing for async paths. Sync paths still use
cursor-position correlation through their own `async_w2m_rcs` cursor vec;
`collect_*` is still present. Full elimination happens after Stage 5.*

### 9. Worker-side response stashing and replay (`worker.rs`)

Because cursor correlation (§8) forces a strict ordering on the W2M ring —
*all* tick ACKs must precede *all* push ACKs in a batched tick — the worker
cannot simply ACK a `FLAG_PUSH` it encounters mid-exchange. It must *stash*
the push and replay it after the current tick's ACK. Same for `FLAG_TICK`
messages encountered during exchange in a multi-table batched tick. Present
in `worker.rs` as:

- `DeferredPush` struct and `deferred_pushes: Vec<DeferredPush>` field
- `deferred_ticks: Vec<i64>` field
- Stash logic inside `do_exchange_impl` (~18 LoC)
- `replay_deferred_pushes()` / `replay_deferred_ticks()` functions

~80 LoC total, existing *purely* because the master needs a specific arrival
order on the W2M ring. With request IDs (D3), the master routes replies by
ID — W2M arrival order becomes irrelevant, workers ACK immediately in the
order they receive messages, stashing vanishes.

---

## End-State Architecture

### Core model

One reactor thread running a work-stealing-free, single-threaded async
executor on top of io_uring. Every concurrent operation is a `Future` whose
state machine is generated by the Rust compiler. The reactor's only job is to
wait for io_uring CQEs and wake the futures that were parked on them.

```rust
// Skeleton of the rewritten loop:
fn run(self) {
    loop {
        // One unified wait. No timeout math — timer CQEs wake us.
        let cqe = self.reactor.wait_one_cqe();
        match cqe.decode() {
            Event::Accept(fd)              => self.spawn(connection_loop(fd)),
            Event::ClientRecv(fd, buf)     => self.reactor.deliver_to_conn(fd, buf),
            Event::WorkerReply(w, req_id)  => self.reactor.wake_reply(req_id, ...),
            Event::FsyncComplete(group_id) => self.reactor.wake_fsync(group_id),
            Event::TimerExpired(timer_id)  => self.reactor.wake_timer(timer_id),
        }
        self.reactor.poll_ready_tasks();
    }
}

// One task per connection, NOT one task per message.
// This is load-bearing for Invariant I2 (read-after-write within a session):
// if we spawned per message, an INSERT task parked on commit could be
// overtaken by a SELECT task spawned from the next ClientRecv on the same fd.
// Per-connection sequencing prevents that by construction.
async fn connection_loop(fd: i32) {
    loop {
        let msg = match reactor().recv(fd).await {
            Some(m) => m,
            None    => return,        // client closed
        };
        if let Err(e) = handle_message(fd, msg).await {
            send_error(fd, e).await;
        }
        // Next recv does not fire until handle_message returns.
    }
}
```

### Per-operation flow (illustrative)

Today's `validate_all_distributed` is ~180 lines with two hand-rolled pipeline
bursts and manual cursor bookkeeping. The async version:

```rust
async fn validate_all_distributed(&self, target: TableId, batch: &Batch,
                                   mode: WireConflictMode) -> Result<()> {
    let plan = self.plan_phase1(target, batch, mode)?;
    if plan.is_empty() { return Ok(()); }

    let phase1 = self.execute_pipeline(plan.checks).await?;
    let phase2_plan = self.plan_phase2(target, batch, mode, &phase1)?;
    if phase2_plan.is_empty() { return Ok(()); }

    let phase2 = self.execute_pipeline(phase2_plan.checks).await?;
    self.interpret_phase2(&phase2, mode)
}

async fn execute_pipeline(&self, checks: Vec<PipelinedCheck>) -> Result<Vec<HashSet<u128>>> {
    let req_ids: Vec<_> = checks.iter()
        .map(|c| self.sal.submit_check(c))
        .collect();
    self.sal.signal_all();
    // Responses routed by req_id; order of resolution doesn't matter.
    try_join_all(req_ids.into_iter().map(|id| self.reactor.await_reply(id))).await
}
```

No cursor bookkeeping, no interleaved error handling, Phase 1 → Phase 2
dependency is literally `await`.

### Group commit as a task

Today's group commit policy lives implicitly across five callsites. End state
is one dedicated task:

```rust
async fn committer(mut rx: Receiver<CommitRequest>, sal: SalWriter) {
    loop {
        let first = rx.recv().await;
        let deadline = Instant::now() + FLUSH_DEADLINE;
        let mut batch = vec![first];
        loop {
            tokio::select! {
                msg = rx.recv()           => batch.push(msg),
                _ = sleep_until(deadline) => break,
                _ if batch_full(&batch)   => break,
            }
        }
        let groups = write_groups(&sal, &batch);
        sal.signal_all();
        let fsync = sal.fdatasync();  // returns Future
        let acks = join_all(groups.iter().map(|g| collect_acks(g)));
        let (_, _) = join!(fsync, acks);
        for req in batch { req.notify_complete(); }
    }
}
```

The policy is in one place. The debounce + size-cap logic is two `tokio::select!`
arms. The fsync + ACK overlap is `join!` (no more `pre_written` dance).

### Tick evaluation as a task

```rust
async fn tick(&self, tids: Vec<TableId>) {
    let next_group = self.pending_committer.try_pre_write();  // returns Future
    let ticks      = self.tick_batch(&tids);
    join!(ticks, next_group);  // overlap
}

/// Write all FLAG_TICK messages then signal once, then await all ACKs.
///
/// INVARIANT: do NOT split this into per-tid futures that each call signal_all.
/// The current synchronous code achieves its performance by writing N tick
/// messages to the SAL and issuing a single eventfd kick for all N — one
/// wakeup per worker per tick batch. If each tick_one were to signal
/// individually you would issue N signals instead of 1, waking workers N
/// times and serialising what should be a concurrent broadcast.
async fn tick_batch(&self, tids: &[TableId]) {
    // 1. Write all ticks to SAL without signalling in between.
    for &tid in tids { self.sal.write_tick(tid); }
    // 2. One signal for the whole batch.
    self.sal.signal_all();
    // 3. Await all ACKs concurrently, routed by req_id.
    try_join_all(tids.iter().map(|&tid| self.reactor.await_broadcast_reply(tid))).await;
}
```

Whatever `TickState::Active` tried to express, this *is*.

---

## Design Decisions

### D1. Single-threaded reactor

**Decision:** The master runs on one OS thread, using single-threaded async
(no `Send` bounds, no multi-threaded work-stealing, no locks).

**Rationale:** The current code relies heavily on "single-threaded master
naturally serializes" — `schema_names_cache`, `unique_filters`, `router`, and
the catalog pointer are all `!Sync` and accessed via raw-pointer tricks.
Single-threaded async preserves this thread-safety model. Moving to
multi-threaded async is a separate refactor with its own tradeoffs (locking
everything, Send bounds on every future) and is **out of scope** for this
plan.

**Important caveat (not glossed over):** single-threaded async does *not*
free us from Rust's borrow-checker XOR-mutability rules. Multiple tasks
concurrently alive cannot simultaneously hold `&mut self.dispatcher` across
`.await` points — the compiler will reject this regardless of threading.

The current code already sidesteps this using raw pointers:
`catalog: *mut CatalogEngine`, `dispatcher: *mut MasterDispatcher`, accessed
via `unsafe { &mut *ptr }` at call sites. This pattern is a deliberate choice
to aliased-mutable access under a runtime guarantee of "the master is
single-threaded, so no one else is borrowing." It is the *same* pattern
async code needs.

Under this plan, the reactor owns the `MasterDispatcher` and the
`CatalogEngine` (either directly or via `Rc<T>`), and async fns reach them
via the same raw-pointer / `&self`-methods-with-interior-mutability pattern
already used today. We **do not** introduce `Rc<RefCell<T>>` by default —
runtime borrow checking in the hot path is unnecessary given the existing
single-threaded invariant, and it would mask bugs the compiler currently
catches.

Concrete rule: shared-long-lived state (dispatcher, catalog, SAL writer) is
reached through raw pointers or `Rc<T> + &self` methods. Per-task owned
state (Phase 1 aggregates, commit requests) lives in the task's future and
is borrowed normally across awaits.

**Named invariant — no `&mut` across `.await` points (D1-NM):**
Materializing `&mut T` from a raw pointer is correct only if no other live
code can materialize a second `&mut T` to the same location before the first
is dropped. In single-threaded async that breaks at `.await`: Task A suspends,
Task B runs, Task B also calls `unsafe { &mut *ptr }`, and the Rust abstract
machine now sees two simultaneously live `&mut T` references — UB regardless
of threading, because the compiler is allowed to assume exclusive aliasing.

*Banned pattern:*
```rust
// WRONG — `disp` is part of the future's state, lives across the yield inside
// validate_distributed, allowing Task B to create a second &mut in between.
let disp = unsafe { &mut *self.dispatcher };
disp.validate_all_distributed(...).await;
```

*Required pattern:*
```rust
// CORRECT — async fns that touch shared state take &self; any field they
// must mutate (e.g. unique_filters) is wrapped in Cell/RefCell scoped to
// that field only.  No raw &mut T is held across a yield point.
async fn validate_all_distributed(&self, ...) -> Result<()> { ... }
```

The fix is not "use an expression-scoped `&mut`" — that borrow still lives
for the full duration of the async call, including internal yield points.
The fix is `&self` for async methods, with interior mutability (`Cell`,
`RefCell`) for the specific fields that need mutation within the method.

Developers doing Stage 4+ should expect to see the existing raw-pointer
idiom preserved in async fns. A rewrite that replaces it with
`Rc<RefCell<T>>` "for safety" is a footgun — it adds overhead without adding
real correctness (the invariant already holds) and moves borrow errors from
compile time to runtime. Interior mutability is only warranted for specific
mutated fields inside `&self` async methods, not as a wholesale replacement.

### D2. Hand-rolled reactor, not tokio

**Decision:** Build a focused ~500-LoC reactor on top of `gnitz-transport`'s
io_uring wrapper. Do not import tokio or a general-purpose async runtime.

**Rationale:**

- Tokio is multi-threaded-first. Its single-threaded flavors exist but come
  with runtime machinery we don't need.
- GnitzDB already owns `gnitz-transport/uring.rs`. A reactor is the natural
  next layer; fits the in-house, io_uring-centric architecture.
- We can make reactor-specific choices (no waker `Arc` allocations; pool task
  slots; avoid virtual calls) that a general runtime can't.
- Reactor semantics we need are narrow: CQEs, timers, request IDs. Tokio
  provides 10× that, all of which adds conceptual overhead.

Minimum reactor surface (to be defined in `gnitz-engine/src/reactor.rs`):

- `Reactor::spawn(Future)` — push a task
- `Reactor::wait_one_cqe() -> Cqe` — block on io_uring
- `Reactor::register_reply_waker(req_id, Waker)` — park a future on a W2M reply
- `Reactor::register_fsync_waker(id, Waker)` — park on fsync
- `Reactor::timer(deadline) -> impl Future`
- `Reactor::poll_ready_tasks()` — drain the run queue after dispatch

**W2M cursor management is the reactor's responsibility (not tasks').** This is
a load-bearing consequence of moving to concurrent requests. Currently
`reset_w2m_cursors()` (which calls `W2mReceiver::reset_all()`, atomically
storing `W2M_HEADER_SIZE` into each worker's write-cursor pointer) is safe
because the master is sequential: send all, drain all, reset, repeat. In the
async world, multiple tasks have replies in flight simultaneously. If any task
called `reset_w2m_cursors()` after reading its own reply it would wipe the
region, potentially erasing a reply that another task has not yet consumed.

The reactor must own cursor resets and apply them only when the region is
empty:

```rust
// Per-worker in-flight counter maintained inside the reactor:
in_flight: [usize; MAX_WORKERS]

// On submitting any SAL message targeting worker W:
in_flight[W] += 1;

// When the reactor reads a reply from W2M[W] and wakes the parked future:
in_flight[W] -= 1;
if in_flight[W] == 0 {
    w2m.reset_one(W);   // safe: region is provably empty
}
```

For broadcasts (tick, push group) each worker produces one reply, so the
reactor increments `in_flight[W]` once per worker per broadcast message.
No task ever calls `reset_w2m_cursors` or `reset_one` directly.

### D3. Explicit request IDs in W2M

**Decision:** W2M control block grows by 8 bytes (u64 request ID). Master
assigns IDs from a monotonic counter; workers echo the ID in their reply.

**Rationale:** Cursor-position correlation is the sync model's central
simplification but it spreads correlation concerns across every callsite.
Explicit IDs make correlation a *wire-level* concern with zero callsite
involvement — `reactor.await_reply(id)` is the whole API.

**Wire-format migration:** This is a breaking change but the SAL format is
purely internal (not client-facing). Changes land in one commit alongside
worker and master updates. No compat shim needed — gnitz-server-release is a
monoprocess binary; rolling restart is atomic.

Cost: +8 bytes per W2M message. For a pk_seek, that's ~0.02% wire overhead.
For a bulk-insert ACK, <0.01%. Well below noise.

### D4. Use explicit request IDs everywhere — no FIFO special case

**Decision:** Every W2M message carries a req_id (D3). No operation relies
on implicit ordering for correlation. Reads, writes, pipelined checks all
route through the same ID-based waker dispatch.

**Rationale:** An earlier draft carved out a per-worker-FIFO fast path for
SELECTs to save one slab lookup per reply. After review, this was rejected:

- The ID lookup is O(1) slab indexing; saving it is ~10 ns on a hot path
  that's already bounded by ~0.6 ms of RTT. Not worth it.
- Maintaining two correlation mechanisms (slab-by-ID + VecDeque-FIFO)
  doubles the reactor's correlation surface for negligible gain.
- The 8-byte wire overhead is ~0.02% of an average W2M message — not
  worth a code-path split.
- Uniformity is a core goal of this refactor. Having one mechanism for
  everything IS the simplification.

One correlation mechanism. One wire format. One reactor code path.

### D5. Task state heap-allocated, pooled

**Decision:** Each in-flight client request corresponds to one task.
Task futures are boxed (`Pin<Box<dyn Future>>`) and stored in a slab. Request
IDs key into the slab.

**Cost:** One allocation per in-flight task. Under sustained load, this is
amortized by the slab's free-list recycling — each completing task frees its
slot to be reused.

**Alternative considered:** Intrusive data structures or stack pinning. Not
pursued because the allocation is ~100 bytes per task and complete-reuse is
easy; diminishing returns relative to the simplicity gain.

### D6. Group commit policy is one task

**Decision:** Deletes `pending: PendingBatch` and `deferred_requests` in
favor of a dedicated `committer` task and a dedicated `reader` task (or: reads
just dispatch directly without queuing, since they can now suspend).

### Lessons from Stages 0-2

Constraints discovered during Stages 0-2. Each lesson is actionable for
Stage 3+ authors; the reactor does NOT currently enforce any of these in
code, so overlooking them will silently break later stages.

**L1. Raw-pointer-through-public-API is the signature of choice for async
dispatcher methods.** The three `fan_out_*_async` fns take `*mut MasterDispatcher`,
not `&mut self`: the exclusive borrow must end at the `.await` point so
other reactor tasks can re-enter the dispatcher to deliver their own
replies. This is D1-NM applied to a concrete API. Stage 3+ async fns on
the dispatcher must follow the same pattern. See `master.rs:single_worker_async`.

**L2. W2M cursor desync: reactor and sync paths share the ring.** Sync
paths (push validation, tick collection, checkpoint) still drain W2M
through the dispatcher's `async_w2m_rcs` + `reset_one` / `reset_all`. The
reactor owns its own `w2m_cursors`. They are supposed to be used in
disjoint time windows — but Stage 2's sync fallback (runs INSIDE a
reactor task via `resolve_async`) violates that. Symptoms: reactor's
cursor sits past the new `write_cursor` after a sync `reset_one`, future
drains miss every message. **Mitigation:** defensive snap in
`drain_w2m_for_worker` — if `cursor > write_cursor`, reset cursor to
`W2M_HEADER_SIZE`. Stage 5+ must either (a) keep the defensive snap, or
(b) make the reactor the sole W2M owner (requires full async migration of
validate / tick / checkpoint). Do not remove the snap until option (b)
lands.

**L3. `in_flight` must only count tracked replies.** `route_reply`
decrements `in_flight[w]` ONLY when a waker was found. Unrouted replies
are logged (`unrouted W2M reply req_id=…`) and dropped without touching
the counter. If you ever decrement on unrouted, a stray reply from the
sync path triggers a premature `reset_one` while a legitimate async reply
is still in flight — the ring gets wiped, the awaiter waits forever. The
three `route_reply_*` tests in `reactor.rs` pin this invariant.

**L4. Bookkeeping must be unit-testable separately from the I/O shell.**
Stage 2's initial implementation had the `in_flight` bug because the only
tests available were happy-path `inject_parked_reply`, which skips the
bookkeeping. Splitting `drain_w2m_for_worker` → pure `route_reply(w, decoded)
-> should_reset` + I/O shell makes bookkeeping testable without a real
`W2mReceiver`. Stage 3+ should apply the same split to any new
drain/route machinery.

**L5. Sentinel strings in `Err(String)` are a footgun.** An early Stage 2
draft used `"__async_scan_fallback__"` smuggled through `Err(String)` to
signal "fall back to sync." A worker error containing those characters
would silently trigger a fallback. Replace with a real enum. Stage 2's
current shape: `enum FanOutError { Worker(String), NeedsSyncFallback }`.
For future async ops that need any "first-class non-error success
branch" (e.g. "need a tick before retrying", "batch full"), model it as
an enum variant, not an error string.

**L6. Broadcast reply routing is the missing primitive blocking full
async migration.** Stage 2 ships truly-async only for single-worker
messages. Multi-worker broadcasts (cache-miss index_seek, multi-worker
scan, ticks, group-commit ACKs) still use sync `wait_all_workers`.
The plan's wire protocol reserves `u64::MAX` for broadcasts and
mentions `pending_broadcasts: Slab<BroadcastState>`, but that machinery
is not built yet.

Two paths to build it, in order of complexity:
- **Per-worker req_ids at write time.** Extend `SalWriter::write_group_direct`
  to accept `&[u64]` of per-worker req_ids instead of one. Each worker
  echoes its own slot's req_id; master allocates N distinct req_ids for
  one broadcast and parks N `ReplyFuture`s. Cleanest for Stage 5's
  `execute_pipeline` which already issues distinct checks.
- **Broadcast-set slab.** One req_id, multiple awaited replies. The
  reactor tracks a "broadcast set" keyed by req_id; `drain_w2m_for_worker`
  increments a counter; the future resolves when all workers answer.
  Simpler wire format (1 req_id), more reactor state.

Stage 4 (committer) and Stage 6 (async tick) are broadcast-heavy and
will force the decision. Land the choice in whichever of those arrives
first.

**L7. Don't interleave sync and async W2M paths within the same tick
window.** The `resolve_async` → sync-fallback pattern in Stage 2's
`spawn_deferred_select` only works because the defensive cursor snap
(L2) + the "unrouted no-decrement" rule (L3) defuse the resulting race.
Stage 3+ should strongly prefer a design where a given tick window is
*either* reactor-driven *or* sync-driven for all its W2M traffic, not a
mix. If broadcast routing (L6) is in place, the mix disappears
naturally.

**Rationale:** PendingBatch and deferred_requests are manual queues for
"things to do later." A task that's `.await`-ing on an event *is* a deferred
computation — no queue needed, the scheduler holds the continuation.

---

## Wire Protocol Change (W2M control block)

Current control block (16 bytes header + variable):
```
[ 0..16 ] reply header (target_id, flags, status, lengths)
```

New:
```
[ 0..16 ] reply header (target_id, flags, status, lengths)
[16..24 ] u64 request_id
```

Workers echo whatever request_id they received. ID allocation is:

- `0` reserved for "unsolicited" messages (shutdown, errors not tied to a request)
- `u64::MAX` reserved for "broadcast" (group commit ACK, tick ACK — one reply
  per worker per broadcast)
- Other IDs assigned from a per-master monotonic counter (never reused within
  a process lifetime)

Master side maintains:
- `pending_replies: Slab<Waker>` keyed by req_id, for single-reply requests
- `pending_broadcasts: Slab<BroadcastState>` for N-reply requests (tracks
  which workers have answered)

---

## Files Affected

Inventoried against current tree. LoC counts are `wc -l` at HEAD; "mentions"
is grep for the key concepts (`AsyncFsync|TickState|PendingBatch|deferred_requests|pre_writ|collect_one|collect_acks|wait_all_workers|fan_out_*|execute_pipeline|validate_all_distributed|wait_one|eventfd_wait`).

### Primary — rewritten substantially

**`crates/gnitz-engine/src/executor.rs`** — 1609 LoC, 67 concept mentions
- Structs deleted: `PendingBatch`, `PreWritten`, `TickState`, `DeferredRequest`, `PendingDdlResponse`, `GroupInfo` (moves inside committer task).
- `ServerExecutor` fields deleted: `deferred_requests`, `pending_ddl_responses`, `tick_state`, `tick_queue_tids`, `t_first_pending`, `pre_written`, `async_fsync`.
- Functions deleted: `flush_pending_pushes`, `pre_write_pushes`, `complete_pre_written`, `process_deferred_requests`, `should_fire_ticks` (the main loop parts of it — policy moves into tick task), all the tick-state transitions.
- `run_loop` rewritten as a reactor-driven main loop (~60 LoC).
- `dispatch_message` becomes a task spawner.
- **Delta: ~-300 LoC. Post-refactor ~1300 LoC.**

**`crates/gnitz-engine/src/master.rs`** — 1916 LoC, 41 concept mentions
- `MasterDispatcher` gains a reactor handle; `schema_names_cache` stays.
- All `fan_out_*` functions become `async fn` (6 functions).
- `execute_pipeline` and `validate_all_distributed` become `async fn`.
- `collect_one`, `collect_acks`, `collect_acks_at`, `wait_all_workers` deleted entirely.
- `signal_one`, `signal_all`, `write_group`, `write_broadcast`, `send_to_workers` retained; internal only.
- Phase 1 / Phase 2 plan-and-interpret helpers stay synchronous (they're pure logic on already-fetched data).
- **Delta: ~-200 LoC. Post-refactor ~1700 LoC.**

**`crates/gnitz-engine/src/ipc.rs`** — 2552 LoC, 18 concept mentions
- W2M control block gains `request_id: u64` field (encode + decode).
- `W2mReceiver`: `try_read`, `write_cursor`, region access primitives retained. `wait_one`, `poll` deleted — reactor owns the waiting.
- `AsyncFsync` struct and its ~44 LoC of standalone io_uring plumbing deleted.
- `SalWriter` unchanged except passing req_id through control-block encoding.
- `decode_wire` / `decode_wire_with_schema` / `encode_wire_into`: extended to read/write the req_id; no structural change.
- **Delta: ~-30 LoC. Post-refactor ~2520 LoC.**

**`crates/gnitz-engine/src/ipc_sys.rs`** — 260 LoC, 10 concept mentions
- `eventfd_wait` and `eventfd_wait_any` deleted; reactor uses io_uring `POLL_ADD` on eventfds instead.
- `eventfd_create`, `eventfd_signal` retained (workers still need to signal via syscall; master-side wait moves to io_uring).
- **Delta: ~-60 LoC. Post-refactor ~200 LoC.**

### Primary — new files

**`crates/gnitz-engine/src/reactor.rs`** — new, ~500 LoC
- Single-threaded reactor + task slab + waker registry + req-ID table.
- Timer futures, CQE dispatch loop.
- Public API: `spawn`, `block_on`, `await_reply(req_id)`, `await_fsync(id)`, `timer(deadline)`.
- Unit-tested in isolation before any caller depends on it.

### Secondary — worker.rs gains a real simplification

**`crates/gnitz-engine/src/worker.rs`** — 1070 LoC
- `send_response` / `send_ack`: echo the inbound request_id on the reply.
- `dispatch`: decode req_id from inbound SAL message and thread it.
- **Bonus structural simplification from D3:** the `DeferredPush` struct,
  the `deferred_pushes` / `deferred_ticks` fields on `WorkerExchangeHandler`,
  the push/tick stashing branches inside `do_exchange_impl` (~18 LoC), and
  the `replay_deferred_pushes` / `replay_deferred_ticks` functions can all
  be deleted. They exist purely because cursor-position correlation
  requires a specific W2M arrival ordering. With request IDs the master
  routes replies by ID, so workers ACK messages in arrival order without
  stashing. (See §9 of the mechanism catalog.)
- **Delta: ~-60 LoC.** Promoted from "minor" to real simplification.

**`crates/gnitz-engine/src/bootstrap.rs`** — 485 LoC, 1 mention
- Line 456 `dispatcher.collect_acks()` becomes part of an async fn at startup, called via `reactor.block_on`. **Delta: ~+5 LoC.**

**`crates/gnitz-engine/src/main.rs`** — 81 LoC
- Wrap the server entrypoint in `Reactor::new().block_on(run_server())`. **Delta: ~+10 LoC.**

**`crates/gnitz-engine/src/lib.rs`** — 21 LoC
- `pub mod reactor;`. **Delta: +1 LoC.**

### Tests

- In-module tests in `executor.rs` / `master.rs` that exercise `PendingBatch` or `collect_*` internals become obsolete — deletes alongside the code they test.
- Integration tests in `crates/gnitz-core/tests/` go through the gnitz-core client and **should pass unchanged**. They are the primary regression surface.
- Bench suite (`benchmarks/`) unchanged; serves as the performance-parity gate.

### Total footprint

| | before | after | Δ |
|---|---|---|---|
| executor.rs | 1609 | ~1300 | -300 |
| master.rs | 1916 | ~1700 | -200 |
| ipc.rs | 2552 | ~2520 | -30 |
| ipc_sys.rs | 260 | ~200 | -60 |
| worker.rs | 1070 | ~1010 | -60 |
| reactor.rs (new) | 0 | ~500 | +500 |
| bootstrap / main / lib | 587 | ~605 | +18 |
| **gnitz-engine total** | ~7994 | ~7835 | **~-160** |

Roughly 8000 LoC reviewed/modified out of ~56000 LoC in the repo (~14%). The LoC delta is small; the conceptual-complexity delta (nine mechanisms → one) is the point.

---

## Relevant Files NOT Directly Affected

These must be understood to do the refactor safely, but no code in them changes.

### Transport layer — `crates/gnitz-transport/`

The reactor is built *on top of* this crate. It needs:
- `uring.rs` (111 LoC): `IoUringRing` provides `prep_send`, `prep_recv`, `submit_and_wait_timeout`, `drain_cqes`. The reactor will use these plus **possibly add `prep_poll_add(fd, events, user_data)` and `prep_timeout(ts, user_data)`** if they aren't already exposed. Likely a small extension of the `Ring` trait in `ring.rs` (88 LoC).
- `transport.rs` (799 LoC): client-side socket handling. Its `poll()` function is currently the master-side event source; the reactor will *absorb* this role by owning the io_uring instance directly, or transport can remain as a layered abstraction. Decision point in Stage 1.
- `conn.rs`, `recv.rs`, `send.rs`, `mock.rs`: unchanged.

### Catalog — `crates/gnitz-engine/src/catalog/`

~4900 LoC (mod.rs 2916, tests 1990). Read and mutated from inside master tasks. **Not modified structurally**, but two new invariants apply:

1. Catalog methods called from tasks must not yield (they're sync fns); async tasks treat them as instantaneous. This is already true — catalog ops are in-memory HashMap/Vec work, no I/O.
2. DDL (catalog mutation) must run with the committer drained and no other tasks in flight. Enforced by the DDL barrier in Stage 4, not by catalog changes.

### Worker-side code — untouched

These run in forked worker processes, which stay **fully synchronous** internally. The async refactor is master-side only.

- `crates/gnitz-engine/src/storage/` — ~8000 LoC: batch, memtable, merge, compact, shard handling. No async.
- `crates/gnitz-engine/src/ops/` — ~3700 LoC: DAG operators (exchange, reduce, join, linear). No async.
- `crates/gnitz-engine/src/{vm,dag,compiler,expr,scalar_func}.rs` — ~9000 LoC combined: query planning, DAG construction, expression evaluation. No async.

### Shared primitives — untouched

- `crates/gnitz-engine/src/{schema,util,sys,log,layout,xxh}.rs`: pure helpers, read from everywhere. No change.

### Client-facing crates — untouched

The W2M wire format is **internal to gnitz-engine**. Verified by grep: `decode_wire | encode_wire_into | W2mReceiver | SalWriter` hits only in `executor.rs`, `master.rs`, `worker.rs`, `ipc.rs`, `bootstrap.rs` — all gnitz-engine.

- `crates/gnitz-protocol/` (header.rs, message.rs, types.rs, wal_block.rs): client SQL wire protocol. Unchanged.
- `crates/gnitz-wire/`: wire-format primitives shared with client. Unchanged.
- `crates/gnitz-core/` (client.rs 869 LoC, tests 1613 LoC): client library + integration tests. **Tests are the regression surface**; library unchanged.
- `crates/gnitz-sql/` (~3700 LoC): SQL parser, planner, DML lowering. Unchanged.
- `crates/gnitz-capi/` (1108 LoC), `crates/gnitz-py/` (1727 LoC): FFI bindings. Unchanged.

---

## Migration Plan (Staged)

Each stage is a distinct commit (or PR), can be tested and benchmarked in
isolation, and leaves the tree in a working state. Later stages can be
reverted without undoing earlier ones.

### Stage 0: W2M request IDs ✅ done (`8b5a186`)

Wire + plumbing added. Workers echo the req_id; master threads a default
of `0` through every `write_group` / `write_broadcast` / `send_to_workers`.
No behavior change at the time of landing.

### Stage 1: Reactor primitive ✅ done (`716075e`)

`crates/gnitz-engine/src/reactor.rs` with `spawn` / `block_on` / `tick` /
`await_reply` / `timer` / `TickIdleBarrier`. Single-threaded, owns its
own `IoUringRing`. Unit-tested in isolation.

### Stage 2: Async SELECT (pk_seek + cached index_seek + single-worker scan) ✅ done

Landed across `6abb21c` (executor skeleton + `deferred_requests` removal),
`308df12` (cleanups), and `2cca343` (truly async fan-outs + W2M reactor
ownership + bookkeeping fix).

**What works:** Deferred SELECTs are spawned as reactor tasks that await
`tick_idle_barrier`. After `notify_all()`, the executor calls
`reactor.block_until_idle()`; the reactor owns W2M drain via `POLL_ADD`
on per-worker eventfds, parks replies by `request_id`, wakes `ReplyFuture`.
Truly async: pk_seek; cached index_seek; single-worker scan.

**What's still sync (scope-cut, see Stage 5):** cache-miss index_seek and
multi-worker scan — the async fan-outs return `FanOutError::NeedsSyncFallback`,
the executor's `resolve_async` helper falls back to the sync `fan_out_*`.
Need per-worker req_id slots in `write_group_direct` + a "broadcast reply
set" slab in the reactor to remove the fallback.

**Non-obvious choices** — read `### Lessons from Stages 0-2` below before
touching Stage 3+.

### Stage 3: Async fdatasync via reactor

- Delete `ipc::AsyncFsync` entirely
- fsync-submit becomes a regular io_uring SQE with a reactor req_id
- `committer` task `.await`s the fsync CQE

**Invariant preserved:** Client ack still follows successful fdatasync. The
await point encodes this.

**LoC:** -80 (AsyncFsync), +10 (reactor call). Net -70.

### Stage 4: Async group commit (committer task)

- New long-running task: `committer(rx: Receiver<CommitReq>) -> !`
- Debounce: `select!(rx.recv(), sleep(FLUSH_DEADLINE))` — rediscovers the
  same policy expressed explicitly
- Each client INSERT flow sends a CommitReq and awaits completion
- `PendingBatch` struct deleted
- `pre_write_pushes` / `complete_pre_written` / `pre_written` field deleted
  (overlap is expressed as `join!` in the tick task)

**Invariants preserved:**
- Group commit amortizes fsync over concurrent inserts (committer drains
  up to N or until deadline).
- **I8 (constraint checks see committed state):** per-table async mutex
  held by the INSERT task across `validate_all_distributed.await` and the
  oneshot reply from the committer. Only one validate→commit at a time per
  constrained table — matches today's `flush_pending_for_tid` pre-flush.
  Unconstrained tables skip the lock.

**Subtle case:** DDL that bumps catalog state must drain the committer first
(a "barrier"). Express as:
```rust
enum CommitRequest {
    Push { /* ... */, done: oneshot::Sender<Result<()>> },
    Barrier(oneshot::Sender<()>),       // signals when all prior pushes have committed
}

async fn ddl_barrier(&self) {
    let (tx, rx) = oneshot::channel();
    self.committer_tx.send(CommitRequest::Barrier(tx)).await;
    rx.await.unwrap();   // fires after the in-progress group's fsync completes
}
```
The committer processes `Barrier` by finishing any in-flight group's fsync
and then signalling the oneshot. No subsequent push is started until the
barrier's sender drops, so the DDL can then safely mutate catalog state.

**LoC:** -400 (PendingBatch + pre_write complex), +200 (committer + Barrier).
Net -200.

### Stage 5: Async pipelined checks + validation

- `execute_pipeline` becomes async: submits all check SAL writes, signals
  once, awaits `try_join_all(req_ids)` for the replies
- `validate_all_distributed` becomes async: Phase 1 `.await`, Phase 2 `.await`
- `collect_one`, `collect_acks`, `collect_acks_at`, `wait_all_workers` deleted

**Invariant preserved:** Phase 1 → Phase 2 ordering is `await`. Per-check
result aggregation is `try_join_all`.

**LoC:** -350 (collect family + execute_pipeline internals), +150 (async
versions). Net -200.

### Stage 6: Async tick pipeline

- Tick evaluation becomes a task; tick start / tick complete are await points
- `TickState` enum deleted
- Tick overlap with next group's SAL write/fsync expressed as `join!`

**LoC:** -150 (TickState + transitions), +80 (async tick). Net -70.

### Stage 7: Delete main-loop timeout math

Now that every event source is an io_uring CQE (via POLL_ADD for worker
eventfds and timer SQEs for deadlines), the main loop just
`wait_one_cqe()` — no timeout computation, no periodic wakeups.

**LoC:** -30.

### Running total

| Stage | Net LoC delta |
|---|---|
| 0 (req IDs) | +100 |
| 1 (reactor) | +500 |
| 2 (async SELECT) | -40 |
| 3 (reactor fsync) | -70 |
| 4 (committer) | -200 |
| 5 (async checks) | -200 |
| 6 (async tick) | -70 |
| 7 (timeout math) | -30 |
| **Total** | **~-10** |

LoC is a weak metric; the story is that ~850 lines of manual-async plumbing
disappear, replaced by ~500 lines of focused reactor + ~350 lines of async
task bodies. The *conceptual* reduction is much larger — five mechanisms
collapse into one.

---

## Correctness Invariants to Preserve

These are load-bearing and must survive each stage's rewrite. Each is
currently "accidentally true" because of sync execution; the async version
must encode them explicitly.

**I1. Write serialization.** Only one writer to the SAL at any moment. In the
async world: SAL writes happen from within tasks, but SAL append is
non-blocking (mmap + cursor advance). The *order* of appends is the order
tasks reach the write call. Since the reactor runs one task at a time, this
is preserved trivially.

**I2. Read-after-write within a client session.** If client C sends INSERT
then SELECT, the SELECT sees the INSERT. Preserved because: (a) per-client
messages arrive in FIFO order over the socket; (b) the handler is a task; (c)
the task's `insert.await` completes after commit. The subsequent `.await` on
the SELECT sees committed state.

**I3. Phase 1 → Phase 2 dependency in validation.** Encoded as `.await`.

**I4. fdatasync before ack.** Encoded as `join!(fsync, acks).await`; the client
reply is sent *after* this resolves.

**I5. Tick barrier for exchange.** All workers must evaluate the tick at the
same logical timestamp. Tick firing is a broadcast; the tick task awaits all
worker ACKs. Preserved.

**I6. Cross-client isolation.** SQL doesn't define concurrent INSERT of the
same PK; today's implicit behavior is "first task to commit wins." This
remains true because the committer serializes.

**I7. DDL exclusion.** DDL barriers drain the committer and block new task
dispatch until DDL commits. Explicit in Stage 4.

**I8. Distributed validation sees committed state** *(load-bearing, almost
missed)*. For constrained tables (FK, unique secondary, or Error-mode
unique-PK), `validate_all_distributed` checks worker storage — which only
reflects *committed* state. Two concurrent validates of INSERTs with the
same PK would both see "PK not present," both pass, both commit, corrupting
the table.

Today's code prevents this with the pre-flush at `executor.rs:1205-1222`:
before calling `validate_all_distributed` on a constrained table, the
executor synchronously flushes any pending entries for that target_id —
forcing prior clients' INSERTs on this table to reach worker storage before
the current one validates. The comment on line 1201 ("*The pre-flush exists
solely to give distributed validation fresh worker state*") names this
invariant explicitly.

**The async plan must encode this or it silently breaks data integrity.**

Implementation: a per-table async mutex held across the validate→commit
window for constrained tables. Only one task per (constrained) table can be
between `validate_all_distributed.await` and commit completion at any time.
Unconstrained tables skip the lock (matches today's `needs_flush == false`
fast path — no pre-flush, multiple validates in flight is harmless because
validate is a no-op).

```rust
struct MasterDispatcher {
    // ... existing fields ...
    // Per-table validate→commit serialization for constrained tables.
    // Created lazily on first INSERT to a constrained table. Entry is an
    // async mutex; held across validate+commit to enforce I8.
    table_locks: HashMap<TableId, Rc<AsyncMutex<()>>>,
}

async fn handle_insert(&self, tid: TableId, batch: Batch, mode: WireConflictMode)
    -> Result<()>
{
    let needs_lock = self.cat().get_fk_count(tid) > 0
        || self.cat().get_fk_children_count(tid) > 0
        || self.cat().has_any_unique_index(tid)
        || (matches!(mode, WireConflictMode::Error) && self.cat().table_has_unique_pk(tid));

    let _guard = if needs_lock {
        Some(self.table_lock(tid).lock().await)
    } else {
        None
    };

    self.disp().validate_all_distributed(tid, &batch, mode).await?;

    let (tx, rx) = oneshot::channel();
    self.committer_tx.send(CommitRequest::Push { tid, batch, mode, done: tx }).await;
    rx.await??;
    // _guard drops here; next validate→commit for this tid can proceed.
}
```

Performance characteristics exactly match today's:
- Different tables: fully parallel (per-tid lock doesn't cross tables).
- Same constrained table: one validate→commit at a time (same as today's
  per-tid pre-flush serialization).
- Same unconstrained table: no lock, full group-commit batching.

**Note on committer batching:** the per-tid lock serializes *validate→commit*
but does **not** prevent the committer from batching different tables'
commits in one fsync. Client A's INSERT on T1 and client B's INSERT on T2
run their validates concurrently, both send CommitRequest to the committer,
committer batches both into one SAL group + one fsync, both reply on their
oneshots, both clients ACK'd. Group commit benefit preserved.

**Subtle case: cross-table FK races.** Today's code has a pre-existing race:
INSERT into child table T2 and DELETE on parent table T1 use *different* tid
locks and can interleave, potentially allowing an orphan child row. The
async plan preserves this exact behavior (no better, no worse). Fixing it
would require either a coarser lock or FK-aware dependency tracking — an
orthogonal correctness concern, explicitly **out of scope** for this plan.

---

## What's Out of Scope

**Multi-threaded master.** This plan keeps the master single-threaded. Going
multi-threaded is a separate initiative that would require: Send bounds on
every future, locks or lock-free structures for catalog / router / cache
access, and careful reasoning about isolation across threads. Not motivated
by current profiles (memmove + syscall dominate; CPU serialization isn't the
bottleneck).

**Importing a general-purpose async runtime.** Tokio, async-std, and smol are
all designed for broader use cases. The reactor we need is narrow; hand-rolled
fits better and avoids vendoring abstractions we don't want (e.g., tokio's
multi-threaded assumptions leaking into type bounds).

**Changing the worker execution model.** Workers are process-level forks
doing DAG evaluation. They stay synchronous internally — they receive a SAL
message, process it, reply. Async is a master-side concept. (Future: workers
could also go async, but that's a separate question about whether their
IPC volume or CPU patterns benefit; probably not, since they're largely
CPU-bound in DAG ops.)

**Client-facing protocol changes.** SQL wire remains unchanged. The async
refactor is entirely internal to the server.

---

## Risks and Mitigations

### R1. Debug legibility

Async stack traces are harder to read. A crash inside `fan_out_seek.await`
shows reactor state, not the call site.

**Mitigation:** Every async fn that can error gets an explicit `tracing::span`
or equivalent at entry. Breadcrumbs via `gnitz_debug!` at await points. Crash
dumps include the task's last span. This is a one-time investment that pays
off as the codebase grows.

### R2. Allocation regression

Per-task boxing costs allocations. Today many per-request state structs live
on the stack.

**Mitigation:** Slab-based task pool — one `Vec<Option<Task>>` recycled. A
completing task frees its slot for the next. Steady-state allocation is zero
after warm-up.

### R3. Group commit latency regression

Sync group commit has a natural "batch until pending drains" boundary. Async
version has to express this explicitly via debounce, which might over- or
under-batch.

**Mitigation:** Port existing policy verbatim (FLUSH_DEADLINE_MS, MAX_PENDING_ROWS)
into the committer task's `select!`. Benchmark against sync baseline at each
stage; Stage 4 has a specific benchmark gate.

### R4. Subtle ordering changes

"Events arriving in different order to what sync produced" could break
behavior that sync accidentally encoded.

**Mitigation:** I1-I7 above, enforced by tests. Stage 4 adds stress tests for
concurrent client submissions.

### R5. Reactor bugs

A hand-rolled reactor could have waker / slab / CQE-dispatch bugs that are
subtle.

**Mitigation:** Reactor is introduced in Stage 1 with no users — it's tested
in isolation (unit tests exercising spawn, timer, CQE dispatch, waker
semantics) before anything depends on it. If a reactor bug is found later, it
can be fixed in the narrow reactor module without touching the task bodies.

---

## Success Criteria

The refactor succeeds if, after all stages land:

1. The eight manual-async mechanisms (§Current State) are deleted.
2. All existing tests pass (441+ engine, all E2E).
3. Bench suite pk_seek / index_seek / full_scan / limit / insert_bulk within
   ±3% of pre-refactor baseline; OLAP workloads same or better.
4. The executor's core control flow can be explained in one diagram: reactor →
   tasks → io_uring. No TickState, no pre_written, no deferred_requests, no
   PendingBatch bifurcation.
5. New operations (future SQL features) can be added as async fns without
   touching the reactor or inventing a new overlap mechanism.

Criterion 5 is the real one — this plan is justified by the *next* ten features
being easier to write, not by the refactor itself.
