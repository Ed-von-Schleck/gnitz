# fd-path inbound memory cap (OOM guard, not backpressure)

## Problem

The reactor's Unix-socket receive path buffers inbound frames with no upper
bound. `handle_recv_cqe` (`runtime/reactor/conn.rs`) allocates a full payload
buffer at `HeaderDone` (`libc::malloc(plen)`, up to 64 MB —
`MAX_FRAME_PAYLOAD_SERVER`), receives into it, then at `MessageDone` queues it
into `ReactorShared.pending_recv` (`FxHashMap<i32, VecDeque<RecvBuf>>`,
`reactor/mod.rs`) and unconditionally re-arms the next recv. The consumer,
`connection_loop` (`orchestration/executor.rs`), awaits `handle_message` between
frames, and a push handler blocks there on the single per-master committer / SAL
append / worker-ring space (`rx.await`, table locks) while a client pipelines
whole request batches ahead of it. The gnitz Python async client is *built* to do
this: it drains up to `IO_BATCH_MAX = 1024` queued pushes and sends them in one
batch before reading any response. So one connection's held memory grows by the
full backlog — one in-flight buffer plus unboundedly many queued 64 MB frames.

The kernel does not throttle this: no `SO_RCVBUF` is set on accepted client
sockets (they run at the ~208 KiB default), and `pending_recv` is *intentionally*
unbounded (see its doc comment) so the reactor can keep draining that small
kernel buffer without pausing. The backlog therefore lives in user-space heap,
decoupled from any OS limit.

Nothing bounds the *number* of connections either: accept is multishot
(`AcceptMulti`, `reactor/uring.rs`), `accept_loop` registers every accepted fd
with no admission control, and startup raises the fd limit to 65536
(`raise_fd_limit(65536)`, `runtime/bootstrap.rs`). Total held inbound memory =
(connections) × (per-connection backlog), both factors unbounded. As an
order-of-magnitude illustration (exact figures unmeasured), a modest fleet of
slow-consuming or deeply-pipelining clients reaches many GiB — well past a normal
box — and the many-connection *in-flight* variant (open N connections, each
sending a 64 MB-header-claimed payload it never completes) allocates N × 64 MB of
uncounted buffers, ~4 TiB at the fd limit. Master RSS is a *global* resource, so
the guard must be global and must cover *all* inbound buffers, in-flight and
queued.

## Why a hard cap, not backpressure

`pending_recv` is intentionally unbounded: with a single slot, pipelined clients
deadlock once the kernel socket buffer fills. Pause-based backpressure (stop
re-arming recv at a watermark) recreates that deadlock as a four-party cycle: the
client blocks mid-send on a full kernel buffer ← the server stopped reading ←
`connection_loop` is blocked writing a large response (a scan train,
`handle_scan` → `send_slot_or_close`) to that same client ← the write blocks
because the client won't read until its own send completes. Nothing moves.
Bounding via backpressure would require the blocking fd client to drain responses
while it sends, which it structurally cannot do.

A hard cap sidesteps the cycle: nobody ever waits. A connection that would push
held inbound memory past the ceiling is closed; a well-behaved client whose
backlog the server keeps up with never approaches it.

## Design

One **global** ceiling on the sum of `frame_weight(len)` over every inbound
payload buffer currently held — in-flight *and* queued — across all connections.
The check happens at the point of allocation (`HeaderDone`, where the payload
length is known and the buffer is about to be `malloc`'d, beside the existing
`max_payload_len` check). Checking *before* allocating makes the ceiling a true
bound with **no overshoot**: a buffer that would breach it is never allocated,
and — because the reactor is single-threaded and sequential — the check is the
true gate on all inbound buffer memory, closing both the queued-backlog and the
uncounted-in-flight vectors in one stroke.

There is deliberately **no** per-connection sub-cap. It would not make the guard
sounder — a hard global ceiling must shed *some* connection when memory is
exhausted, and bounding each connection's share does not make the connection that
happens to trip the global cap the largest contributor (an offender that hit its
own sub-cap is already closed and no longer arming, so the tripper is whoever
sends next — often a low-rate client). Its only real effect is capping a single
connection's footprint, which is speculative isolation for the current
trusted-client reality and, at any fixed value, risks spuriously disconnecting a
legitimate deep-pipelining client (the Python client's 1024-deep batching can
legitimately hold multiple GiB in flight). Global-only is simpler and meets the
OOM goal exactly.

**Sizing.** `global_cap` is resolved once at startup into a
`ReactorShared` cell:

```rust
fn resolve_inbound_cap() -> usize {
    if let Ok(v) = std::env::var("GNITZ_INBOUND_MEM_BYTES")
        .ok().and_then(|s| s.parse::<usize>().ok())
    {
        return v.max(INBOUND_CAP_FLOOR); // operator override wins
    }
    // Default: a quarter of the memory budget, clamped. `available_memory_bytes`
    // reads cgroup v2 `memory.max` (the real container limit) and falls back to
    // total physical RAM only when it is absent or "max".
    (crate::foundation::posix_io::available_memory_bytes() / 4)
        .clamp(INBOUND_CAP_FLOOR, INBOUND_CAP_CEIL)
}
```

A flat default is not defensible: gnitz already pins a 1 GiB always-resident SAL
plus per-worker 1 GiB W2M rings and memtables, so a fixed 4 GiB inbound ceiling
composes to OOM on a mid-size box yet never trips inside a 2 GiB container (the
cgroup OOM-killer fires first) — false confidence at both ends. A fraction of the
*actual* memory budget scales with the deployment; the cgroup read is ~20 lines
of `std::fs` (the technique the JVM and Go use), so the sysconf-reports-host-RAM
pitfall is avoided rather than used to justify a broken flat default. The `/4`
fraction and the 4 GiB ceiling are heuristics — owned as such — but they scale
correctly, unlike any constant. The env override is the escape hatch for
deployments with legitimately deep pipelines or unusual budgets.

Constants / helper in `runtime/reactor/io.rs`:

```rust
pub(crate) const INBOUND_CAP_FLOOR: usize = 64 << 20;    // 64 MiB
pub(crate) const INBOUND_CAP_CEIL: usize = 4usize << 30; // 4 GiB

#[inline]
pub(crate) fn frame_weight(len: usize) -> usize {
    // Floor: a 1-byte payload really costs ~48 B (malloc's 32 B min chunk +
    // 16 B RecvBuf in the VecDeque); without the floor a tiny-frame flood
    // bypasses the byte cap ~48x. Keeps accounted bytes ≥ real RSS.
    len.max(64)
}
```

`available_memory_bytes()` in `foundation/posix_io.rs`: read
`/sys/fs/cgroup/memory.max`; if present and not `"max"`, parse it; else
`sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE)`. Cached in a `OnceLock` (read
once — the client reactor lives only in the master, so there is no fork-time
divergence to worry about).

`ReactorShared` gains `total_inbound_bytes: Cell<usize>` (init 0) and
`global_cap: Cell<usize>` (init `resolve_inbound_cap()`), plus a `#[cfg(test)]`
setter `set_inbound_cap` to lower it in tests. `Conn` gains
`inbound_bytes: usize` (init 0) — *not* a cap, but the per-connection running
total the reap path subtracts (below). The hot-path read `global_cap.get()` is an
unconditional plain load; only the mutating setter is `#[cfg(test)]`.

**Producer — `handle_recv_cqe`, `HeaderDone` arm.** The payload length is known
here and the `max_payload_len` / malloc-null checks already live here. Add the
cap check between them, count on successful allocation:

```rust
io::RecvAdvance::HeaderDone => {
    let plen = conn.recv_state.payload_len();
    if plen > conn.max_payload_len {
        self.begin_recv_close(conn, fd);      // via helper (below)
        return;
    }
    let w = io::frame_weight(plen);
    if self.inner.total_inbound_bytes.get() + w > self.inner.global_cap.get() {
        crate::gnitz_warn!(
            "reactor: inbound cap would be exceeded, closing fd={} (held={} B + {} B, cap={} B)",
            fd, self.inner.total_inbound_bytes.get(), w, self.inner.global_cap.get(),
        );
        self.begin_recv_close(conn, fd);
        return;                               // refuse before malloc — no overshoot
    }
    let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
    if pbuf.is_null() {
        self.begin_recv_close(conn, fd);
        return;
    }
    conn.inbound_bytes += w;
    self.inner.total_inbound_bytes.set(self.inner.total_inbound_bytes.get() + w);
    conn.recv_state.start_payload(pbuf, plen);
    self.inner.ring.borrow_mut()
        .prep_recv(fd, pbuf, plen as u32, udata(KIND_RECV, fd as u32 as u64));
    conn.recv_armed = true;
}
```

Refusing before the `malloc` means `total_inbound_bytes` never exceeds
`global_cap` — the ceiling is exact, no per-frame or per-connection overshoot.
`MessageDone` is **unchanged**: it only moves the already-counted buffer from
`recv_state` into `pending_recv` and re-arms; the bytes stay counted across the
move.

**Consumer — `RecvFuture::poll`.** On a successful pop (the frame is consumed and
its `RecvBuf` freed on drop), subtract `frame_weight(buf.len)` from both counters.
Drop the `pending_recv` borrow first, then borrow `conns`; the global counter is a
`Cell`, no borrow.

```rust
if let Some(buf) = taken {
    let w = io::frame_weight(buf.len);
    self.inner.total_inbound_bytes
        .set(self.inner.total_inbound_bytes.get().saturating_sub(w));
    if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&self.fd) {
        conn.inbound_bytes = conn.inbound_bytes.saturating_sub(w);
    }
    return Poll::Ready(Some(buf));
}
```

`saturating_sub` in both sites (and reap) is defensive only — the invariant below
guarantees the terms never go negative — but keeps the three decrement points
uniform.

**Reap — `reap_closing_conns` reconciles the global counter.** `Conn.inbound_bytes`
dies with its `Conn`, but `total_inbound_bytes` is a persistent cell. When a
connection closes with buffers still held — queued frames in `pending_recv` *and*
possibly one in-flight buffer in `recv_state` — reap drops them directly (the
`VecDeque<RecvBuf>` and, via `Conn::drop → free_payload`, the in-flight buffer).
None flow through `RecvFuture::poll`, so their weights, added at
`HeaderDone`, would never be subtracted; left uncorrected the global counter leaks
monotonically until it exceeds `global_cap` permanently and spuriously closes
every new connection. Reap subtracts the reaped connection's whole share before
dropping it:

```rust
// in reap_closing_conns, replacing `self.inner.conns.borrow_mut().remove(&fd);`
if let Some(conn) = self.inner.conns.borrow_mut().remove(&fd) {
    self.inner.total_inbound_bytes.set(
        self.inner.total_inbound_bytes.get().saturating_sub(conn.inbound_bytes),
    );
}
```

`conn.inbound_bytes` at reap equals exactly the weight of the buffers still held
(consumed frames already decremented it), so this removes the undrained share with
no double-count. Keeping the running total on `Conn` is why it is preferred over
re-summing the `VecDeque` at reap: the sum would miss the in-flight `recv_state`
buffer, which is counted but not queued. Invariant:
`total_inbound_bytes == Σ_live_conns conn.inbound_bytes == Σ frame_weight of all
held inbound buffers`.

**Close-path helper (simplification).** `handle_recv_cqe` today repeats the same
five-line close sequence — `conn.closing = true; closing_fds.insert(fd);
recv_closed.insert(fd, true); wake recv waiter; return` — in four arms (res≤0,
`plen > max_payload_len`, malloc-null, `Disconnect`). Extract it:

```rust
fn begin_recv_close(&self, conn: &mut io::Conn, fd: i32) {
    conn.closing = true;
    self.inner.closing_fds.borrow_mut().insert(fd);
    self.inner.recv_closed.borrow_mut().insert(fd, true);
    if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
        w.wake();
    }
}
```

`self` borrows only the sibling `RefCell`s, never `conns`, so it composes with the
caller's live `conns` borrow and the `&mut Conn` it holds. The helper omits the
trailing `return`, so each caller keeps its own `return;` (the cap-trip arm too —
no re-arm, so `recv_armed` stays false and reap fires as soon as `send_inflight`
is 0). One helper + five one-line calls replace ~20 lines of duplication and the
sixth copy the cap check would otherwise add.

## What the cap guarantees, and what it does not

- **Hard, overshoot-free memory bound.** Total held inbound memory (in-flight +
  queued, across all connections) never exceeds `global_cap`, regardless of
  connection count — the check refuses the allocation that would breach it. This
  is the OOM guard, and it holds against both the queued-backlog and the
  many-connection uncounted-in-flight vectors.
- **Shedding is inherent and not surgical.** On a genuine breach the connection
  whose next frame would cross the ceiling is closed — an *active sender* (it is
  trying to allocate), but not necessarily the largest hoarder. Freeing its
  buffers drops the total back under the ceiling; sustained pressure sheds further
  senders as they arrive. A hard global ceiling has no way to close "the worst"
  connection without an O(connections) scan, which is not worth it for a rare
  memory-pressure event.
- **Closing drops the connection's whole backlog.** `begin_recv_close` leaves
  `recv_armed` false, so `reap_closing_conns` runs in the same tick (before task
  polling) and drops the *entire* `pending_recv[fd]` queue, not just the tripping
  frame — every already-received-but-unprocessed request from that burst is lost,
  and the client's outstanding futures fail with a transport error. This is the
  correct backstop: it trades an uncontrolled OOM for a controlled disconnect. It
  also means a client that pipelines beyond the server's memory budget *will* be
  disconnected; bounding in-flight bytes client-side is the complementary
  mitigation and lives in the client, outside this change.
- **Memory is bounded, not always instantly reclaimed.** If a closed connection
  has an in-flight response send to a peer that stopped reading,
  `reap_closing_conns` waits for `send_inflight` to reach 0, so its held buffers
  stay (bounded, not growing) until the send CQE completes — when the peer reads
  or disconnects. The reactor itself never blocks; only that one fd's reclamation
  defers, and the connection is already closed (no service).

## Accounting correctness

Count on allocation (`HeaderDone`, the sole `malloc` site), decrement on free.
Two free paths: consumer pop (`RecvFuture::poll`, the sole `pending_recv` pop
site) decrements both counters; reap (`reap_closing_conns`) subtracts the whole
`conn.inbound_bytes` from the global counter as the `Conn` and its `VecDeque` are
dropped. Because the reactor is single-threaded with no `.await` between a pop and
its paired decrement or inside reap, and `conns`/`pending_recv` for an fd are
inserted and removed together in one synchronous call, a buffer's weight is added
exactly once and removed exactly once (pop *or* reap, never both — a popped frame
is no longer in the conn's share at reap). No double-count, underflow, or leak.
`register_conn`'s stale-`pending_recv` removal `gnitz_fatal_abort!`s on a
non-empty queue and runs before any `Conn`/counter for the fd exists, so it cannot
desync the counter. (Reviewed against every path that drops a `Conn` or
`pending_recv` entry.)

## Out of scope

- **Admission control / max-connections limit.** Accounting in-flight buffers at
  `HeaderDone` makes connection count irrelevant to the *memory* goal (total
  inbound is bounded no matter how many connections exist), and fd exhaustion is
  already self-throttled by the EMFILE/ENFILE accept backoff. A connection-count
  limit would bound per-connection struct/task overhead, a distinct and far
  smaller resource, not required here.
- **Client-side in-flight byte bound.** The Python async client pipelines up to
  1024 requests deep with no byte bound, so against a slow server it can build a
  backlog large enough to trip this cap and be disconnected. Bounding the client's
  in-flight bytes is the right fix for that path, but it is a client-side change in
  a different crate and does not affect the server's OOM guarantee.
- **`register_conn` stale-comment fix.** The comment above the stale-`pending_recv`
  check claims "in release, free the buffers so we don't leak," but
  `gnitz_fatal_abort!` unconditionally `_exit`s in every profile. Pre-existing,
  behavior is safe (abort), purely a misleading comment; correct it opportunistically
  if touching that function.

## File changes

1. `foundation/posix_io.rs`: `available_memory_bytes()` (cgroup v2 `memory.max`
   → sysconf fallback, `OnceLock`-cached) (~25 lines).
2. `runtime/reactor/io.rs`: `INBOUND_CAP_FLOOR`, `INBOUND_CAP_CEIL`,
   `frame_weight`, one `Conn` field `inbound_bytes` (~12 lines).
3. `runtime/reactor/mod.rs`: `total_inbound_bytes` + `global_cap` fields,
   `resolve_inbound_cap()` + their init in `Reactor::new`, `#[cfg(test)]`
   `set_inbound_cap` setter (~18 lines).
4. `runtime/reactor/conn.rs`: `begin_recv_close` helper; refactor the four
   existing close arms to call it; the cap check + counting in the `HeaderDone`
   arm; the global-counter reconciliation in `reap_closing_conns` (~18 lines net,
   including duplication removed).
5. `runtime/reactor/futures.rs`: both-counter decrement in `RecvFuture::poll`
   (~7 lines).

## Tests

Reactor unit tests. No existing test drives the recv/`register_conn` path with
real io_uring completions (the one socketpair test,
`send_buffer_loops_until_full_payload_sent_over_socketpair`, is send-side only),
so these are greenfield, but every primitive exists: a real `socketpair`,
`register_conn(fd)` on a live fd, `libc::write` of framed bytes, and
`poll_nonblocking()` / `tick()`. Each frame needs two real completions (header CQE
→ `HeaderDone`, payload CQE → `MessageDone`) and a fresh SQE prepped mid-tick is
not observably complete until a later tick, so drive with a **bounded,
condition-checked** poll loop: `for _ in 0..N { r.poll_nonblocking(); if <stop
condition> { break; } }`. Assert `recv()` → `None` with a single manual `.poll()`
(the `Box::pin(fut)` + `make_waker` pattern already used for the dropped-future
tests). Tests lower the cap via `set_inbound_cap` (no GiB allocation).

Gotcha: a new `Conn` starts at `max_payload_len = HELLO_PRE_HANDSHAKE_LEN`
(8 bytes). Any test frame with payload > 8 bytes must call
`set_max_payload_len(fd, N)` first, or it trips the *existing* payload-size limit
instead of the new cap. The tiny-frame test (1-byte payloads) is unaffected. The
cap trips at `HeaderDone` — when the *header* of the frame that would breach
arrives — so that frame is refused (never queued); the queue holds the frames
received before it.

1. **Cap trips.** `set_inbound_cap` low; write complete frames whose cumulative
   weight passes the cap on one connection without consuming; poll-loop; assert
   the connection closes (`recv()` → `None`), and after reap
   `total_inbound_bytes == 0` (proves the reap reconciliation) and the queued
   `RecvBuf`s are freed.
2. **In-flight counts.** Send one header claiming a large payload and dribble only
   part of it (no `MessageDone`); poll-loop; assert `total_inbound_bytes` reflects
   the allocated buffer (not 0), proving in-flight buffers are accounted — the
   many-connection OOM vector. Then send a second connection's header that would
   breach; assert it is refused/closed.
3. **Accounting balances.** Write frames, consume all via `Reactor::recv`, write
   again — the counter decrements on pops, so total traffic far above the cap never
   trips as long as consumption keeps pace; assert `total_inbound_bytes` returns to
   0 when the queue drains.
4. **Tiny-frame floor.** `set_inbound_cap(4096)`; flood with 1-byte-payload frames
   (4-byte header + 1-byte payload, 5 wire bytes each, no `set_max_payload_len`
   needed since payload ≤ 8). `frame_weight = 64` each, so the cap trips when the
   65th frame's header arrives (64×64 = 4096 held; the 65th would make 4160 >
   4096) — ~`CAP/64` frames, not `CAP`, ~325 wire bytes total, one `libc::write`.
5. Full `make test` + `make e2e` unchanged (the cap never trips under normal
   consumption).

## Sequencing

- [ ] 1. `available_memory_bytes()` + constants + `frame_weight` +
      `Conn.inbound_bytes` + `total_inbound_bytes`/`global_cap` fields with
      `Reactor::new` init (`resolve_inbound_cap`) + `begin_recv_close` helper
      (refactor the four existing close arms) + `HeaderDone` cap check/count +
      consumer decrement + `reap_closing_conns` reconciliation + `#[cfg(test)]`
      `set_inbound_cap`. Unit tests 1–4. `make test` green.
- [ ] 2. `make e2e` green (multi-worker, W=4).
