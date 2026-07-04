# A slow client freezes the whole cluster via the W2M ring

A single slow — or maliciously zero-window — TCP client can wedge the entire
GnitzDB cluster. Scan continuation frames are forwarded **zero-copy** straight
out of a worker's W2M shared-memory ring: the master holds each `W2mSlot` alive
until the kernel has accepted every byte of the client `OP_SEND`. A client that
stops reading freezes `consume_cursor`, the ring fills, and the worker — single-
threaded — **blocks synchronously in `send_encoded`'s `futex_wait`** (no timeout)
at the top of its SAL-drain loop. It then processes no `FLAG_PUSH` / `FLAG_TICK`
/ `FLAG_FLUSH` for **any** client. When a checkpoint next fires it holds
`sal_writer_excl` across the all-worker ACK wait, so the blocked worker's missing
ACK stalls every SAL writer cluster-wide.

The worker's own field docs already name this hazard
(`worker/mod.rs:227-241`: "the ring fills, the worker blocks in `send_encoded`,
and the cluster deadlocks" / "full because the queued train's master-side
consumer paces a slow client TCP connection"). Client **disconnect** is handled
(the `ScanLease` drop makes `route_scan_slot` discard undrained frames); a
slow-but-connected client is not — there is no timeout, watchdog, or copy-and-
drop anywhere on the egress path.

Fix: put a **wall-clock deadline on the master's per-frame client egress**. When
a single scan-frame send exceeds it, the client is treated as dead: the socket
is `shutdown()` to force the in-flight `OP_SEND` to error out promptly, the send
future is awaited to completion so the held `W2mSlot` drops safely (no io_uring
use-after-free), the peer is closed, and the existing `ScanLease` teardown
discards the remaining frames — freeing the ring so the worker's `send_encoded`
returns. Bounded worst case: a wedged client stalls the cluster for at most one
deadline, then is evicted.

Pre-alpha: no compatibility concern.

---

## Current mechanics (verified against source)

- **Zero-copy egress holds the ring.** `drain_scan_train`
  (`master/dispatch.rs:1792-1810`) forwards each frame with
  `peer.send_slot(slot).await` (`:1801`) then `await_scan_slot(req_id)` for the
  next. `send_slot` → `Reactor::send_slot` (`reactor/conn.rs:121-125`) keeps the
  slot alive via `SendAlive::Slot(Rc::new(slot))`; `consume_cursor` only advances
  when that `Rc` drops (`W2mSlot::drop` → `InFlightState::release`,
  `reactor/protocol` W2M `w2m.rs:218-222,144-181`). `try_read_slot` advances
  `read_cursor` immediately but `consume_cursor` only on drop — so the reactor
  keeps *reading* the ring while a slow client stalls, but stops *freeing* it.
- **`send_slot` never times out.** `send_buf_inner`
  (`reactor/conn.rs:137-181`) loops `while sent < len`, re-submitting for the
  remainder whenever the socket buffer fills, and clones `_alive` into every
  in-flight `SendFuture`. `SendFuture::poll` (`reactor/futures.rs:332-343`)
  returns `Pending` until the `OP_SEND` CQE fires — there is no deadline arm.
- **The worker blocks synchronously, before SAL drain.** `W2mWriter::send_encoded`
  (`protocol/w2m.rs:37-95`) on `TryReserve::Full` (`:50`) sets `FLAG_WRITER_PARKED`
  and `futex_wait_u32` (`:55`) until the master advances `consume_cursor`. It is
  the only public send API (no `try_send_encoded`). The worker calls it from
  `emit_pending_scan_chunk` (`worker/mod.rs:437-439`, `emit` → `send_encoded` at
  `:519`) at the **top** of `drain_sal`, *before* the
  `while let Some(..) = self.next_sal_message()` loop (`:440`). The worker is a
  synchronous forked process (`worker/mod.rs:394-428`) with no reactor to yield
  to. Production frames are `MAX_W2M_MSG` (256 MiB) against a 1 GiB ring
  (`W2M_REGION_SIZE`), so ~4 stalled frames fill it and the worker futex-blocks.
- **Cluster-wide escalation.** A checkpoint holds `sal_writer_excl` across the
  all-worker FLAG_FLUSH ACK wait (`committer.rs:246,257`); a blocked worker never
  ACKs, so the checkpoint (and behind it every push/tick/DDL/fan-out) stalls.
- **Existing mitigation covers disconnect only.** The `ScanLease`
  (`reactor/futures.rs:154`) is held across the whole train; on drop
  `route_scan_slot` (`reactor/mod.rs:987-998`) discards frames whose req_id is no
  longer registered, advancing `consume_cursor`. A *disconnect* frees the ring;
  a slow-but-connected client does not trigger it. There is no `shutdown`
  primitive in `foundation/posix_io.rs` today (only `close_fd`,
  `reactor/conn.rs:186`).

## Why `shutdown`, not `close`, and why await-after-timeout

The slot is live shared-ring memory referenced by an in-flight `OP_SEND` SQE. On
deadline we must NOT simply drop the `SendFuture`: dropping it drops the `_alive`
`Rc<W2mSlot>` while the kernel may still read that buffer — a use-after-free.

`close(fd)` does not force the pending `OP_SEND` to complete: with data queued the
kernel keeps the socket in the background (absent `SO_LINGER=0`), so the CQE can
linger indefinitely for a zero-window peer. `shutdown(fd, SHUT_RDWR)` aborts the
connection, so the pending `OP_SEND` errors out (`ECONNRESET`/`EPIPE`) promptly
and its CQE fires. Therefore the deadline path keeps the send future alive,
`shutdown`s the fd, then **awaits the same future to completion** (it now
resolves with `rc < 0`), and only then lets the slot drop. This is the only
ordering that both unblocks the ring and stays memory-safe.

## Design

Add `posix_io::shutdown(fd: i32)` — a thin `libc::shutdown(fd, SHUT_RDWR)` wrapper
(retry on `EINTR`, ignore `ENOTCONN`).

Add a deadline knob `GNITZ_CLIENT_SEND_TIMEOUT_MS` (default `30_000`). Rationale:
long enough that ordinary transient congestion never evicts a client; the
deadline is **per frame send**, so a client making steady progress across a large
train is never penalised — only one that makes *zero* progress for the full
window is evicted. Bounded worst case: cluster stall ≤ one deadline.

Replace the bare `peer.send_slot(slot).await` in `drain_scan_train`
(`master/dispatch.rs:1801`) with a deadline-guarded send. The same helper is used
by both `fan_out_scan_async` and `fan_out_scan_single_worker_async` (they share
`drain_scan_train`), so one edit covers every ring-slot-holding egress:

```rust
// reactor + peer + fd in scope; TIMEOUT read once at startup.
let mut send_fut = Box::pin(peer.send_slot(slot));
let deadline = /* now + client_send_timeout */;
let rc = match select2(send_fut.as_mut(), reactor.timer(deadline)).await {
    Either::A(rc) => rc,
    Either::B(()) => {
        // Deadline: the client is not draining. shutdown() forces the
        // in-flight OP_SEND to error promptly; await the SAME future
        // (never drop it early) so the held W2mSlot is released only after
        // its CQE — then the ScanLease drop discards the rest of the train
        // and consume_cursor advances, unblocking the worker.
        crate::foundation::posix_io::shutdown(fd);
        let _ = send_fut.await;
        peer.close();
        return Ok(false);
    }
};
if rc < 0 {
    return Ok(false);
}
```

Passing `send_fut.as_mut()` (a `Pin<&mut SendFuture>`, itself a `Future`) to
`select2` means the timer winning does NOT drop the send future — it stays owned
by this scope for the mandatory post-`shutdown` await. `Instant::now()` is
available on the master (reactor timers already use it, `reactor/mod.rs:552`).

Only the scan train needs this: point replies (SEEK and other small responses)
go through `send_buffer` with `SendAlive::Pooled` (a heap copy,
`reactor/conn.rs:110-113`) — they do **not** pin a ring slot, so a slow client
stalls only that one send, never the worker's ring. This plan deliberately leaves
that path unchanged.

## Correctness

- **No use-after-free.** The slot's `Rc` lives inside `send_fut`; the deadline
  arm `shutdown`s then awaits `send_fut`, so the slot drops only after the
  `OP_SEND` CQE. The happy path (`Either::A`) drops `send_fut` normally after the
  CQE. Neither path frees the slot while an SQE references it.
- **Ring is freed.** After the aborted send returns, `drain_scan_train` returns
  `Ok(false)`; its caller drops the `ScanLease`, and `route_scan_slot` discards
  every later frame of this req_id at the ring boundary
  (`reactor/mod.rs:987-998`), advancing `consume_cursor`. The worker's parked
  `send_encoded` is woken by that advance (`w2m.rs` writer wakeup).
- **No false eviction of healthy clients.** The deadline is per-frame and
  generous; a client that accepts bytes resets it every frame. Only zero-progress
  clients hit it.
- **Bounded blast radius.** Worst case a wedged client freezes cluster SAL
  progress for ≤ one deadline (until the in-flight frame times out and the ring
  frees), versus today's unbounded freeze.

## Tests

- **Slow-client eviction frees the cluster (E2E, `make e2e`, `GNITZ_WORKERS=4`).**
  A short `GNITZ_CLIENT_SEND_TIMEOUT_MS` (e.g. 1500 ms). Client A issues a SCAN
  spanning many frames, then stops reading (raw socket with a tiny `SO_RCVBUF`,
  or `kill -STOP` on a helper); Client B pushes and scans. Assert B's ops
  complete within a few deadlines (they hang forever pre-fix) and that A's
  connection is closed by the server.
- **Healthy large scan is never evicted (E2E).** A large multi-frame SCAN read at
  normal pace under the same short timeout completes and returns every row —
  proving the per-frame deadline does not penalise steady progress.
- **`shutdown` unit test.** `posix_io::shutdown` on a live socketpair fd makes a
  pending blocking `send` on the peer error out (EINTR retry, ENOTCONN tolerated).

## Sequencing

One commit; the tree is green (`make verify` + `make e2e`) after it.

- [ ] 1. **Deadline-guard the scan egress.** Add `posix_io::shutdown`; add
  `GNITZ_CLIENT_SEND_TIMEOUT_MS`; wrap the `send_slot` in `drain_scan_train` with
  the `select2(send, timer)` deadline + `shutdown` + await-to-completion + peer
  close + `Ok(false)` return. Add the slow-client-eviction and healthy-large-scan
  E2E tests and the `shutdown` unit test.
