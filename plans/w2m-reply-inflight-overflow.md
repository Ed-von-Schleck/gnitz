# W2M reply-ring in-flight overflow: grow `InFlightState` past 64

## Problem

The per-worker W2M reply ring tracks outstanding (drained-but-not-yet-released)
slots in `InFlightState` (`crates/gnitz-engine/src/runtime/protocol/w2m.rs:109-119`),
a fixed structure sized by `W2M_MAX_IN_FLIGHT = 64` (`w2m.rs:107`): a
`queue: [u64; 64]` of per-slot vrcs and a `completed: u64` bitmap. The count is
**not** bounded at the producer or the drain:

- `take()` (`w2m.rs:134-139`) registers a new in-flight slot with **no capacity
  check** — `self.queue[(front_idx + len) % 64] = vrc; len += 1`. The 65th
  concurrent slot writes `queue[idx % 64]`, aliasing and **clobbering** slot 0's
  vrc; `len: u8` (`w2m.rs:116`) keeps counting.
- `release()` (`w2m.rs:144-181`) computes `bit = push_idx - front_idx` and
  `assert!(bit < 64, …)` — a **release-build `assert!`** (not `debug_assert!`),
  so once ≥64 slots are simultaneously in flight a subsequent release **aborts
  the process**.
- The drain reads unbounded: `drain_w2m_for_worker`
  (`crates/gnitz-engine/src/runtime/reactor/mod.rs:946-964`) loops
  `while let Some(slot) = w2m.try_read_slot(w)`, and `try_reserve`
  (`crates/gnitz-engine/src/runtime/protocol/w2m_ring.rs:442-449`) gates emission
  on **ring bytes only** — there is no frame-count term.

So in-flight count is bounded only by how many frames fit in the ring by *bytes*.
For large reply frames (256 MiB `MAX_W2M_MSG`, `w2m_ring.rs:77`) the 1 GiB ring
(`w2m_ring.rs:72`) holds ~4 — far under 64. But **small frames carry no
byte-backpressure**: a single-frame scan reply of a tiny table (a few KB) parks
one in-flight slot indefinitely while its client is slow, and thousands fit in
the ring by bytes. The count crosses 64 whenever enough small reply frames are
simultaneously parked (drained but not yet forwarded) on one worker's ring.

The only runtime guard is a per-request `debug_assert!` in `route_scan_slot`
(`reactor/mod.rs:987-992`) that (a) compiles out in release builds and (b) is
keyed per `req_id`, so it cannot see the cross-request aggregate that actually
overflows the shared per-ring `InFlightState`.

### Reachable triggers (client-controlled)

- **Concurrent stalled small scans.** `active_scans` (`reactor/mod.rs`) is
  unbounded. 64+ clients each scanning a tiny or replicated table, all slow to
  drain, park 64+ small frames on worker 0's ring (replicated relations
  single-source from worker 0). `take()` clobbers, `release()` aborts.
- **Shrunk reply budget.** With `GNITZ_REPLY_FRAME_BUDGET` set small (a debug
  knob, `worker/mod.rs:332-334`), even one large scan chunks into >64 small
  frames; a slow client parks them past 64.

The abort is a clean process crash of a worker/master, reachable **today**
without special privilege and independent of any single feature — so this fix
stands on its own merits and is worth landing before any feature that makes the
overflow easier to reach.

## Why the obvious fixes are wrong

- **Gating the drain** (`while in_flight_len(w) < K { try_read_slot… }`) is a
  **regression**, not a fix. `drain_w2m_for_worker` is the single strictly-FIFO
  drain for *every* reply kind — scan frames, push ACKs, and exchange output
  (`FLAG_EXCHANGE` → `exchange_acc`, routed via `route_reply`,
  `reactor/mod.rs`). `read_cursor` is monotonic and cannot skip. Stopping the
  drain at `K` parked scan frames leaves an exchange-output frame parked *behind*
  them unreachable → the exchange round never completes → a worker spins in
  `do_exchange_wait` → view maintenance and checkpoints freeze cluster-wide until
  client eviction. Today the reactor reads *past* parked scan frames (advancing
  `read_cursor` independently of `consume_cursor`) and routes exchange
  immediately; a gate destroys exactly that.
- **Copying frames out of the ring** (release the slot, buffer owned bytes) does
  **not** bound in-flight: `release()` advances only the *consecutive completed
  prefix* (`w2m.rs:154`, `trailing_ones`), so a copied frame released behind a
  still-parked earlier frame never advances `consume_cursor` — it frees neither
  the ring bytes nor the in-flight slot. Copy-all additionally regresses
  single-scan throughput (an extra memcpy of every 256 MiB frame that is already
  being written to the client socket).
- **Admission control at dispatch** (bound concurrent scan trains per worker
  ring) works but adds a per-worker semaphore, dispatch-time waiting, and a
  catalog-lock ordering wart (the target-worker set is known only after shape
  resolution under the catalog read lock).

## Fix: make `InFlightState` grow

The abort exists solely because the completion bitmap is a single `u64` and the
vrc queue is a `[u64; 64]`. Nothing about the ring semantics requires ≤64 — the
in-flight *data* lives in the ring (byte-bounded by `W2M_REGION_SIZE`), and the
in-flight *bookkeeping* is a few bytes per slot. Replace the fixed structure with
a single growable queue that carries the completion flag inline:

- `queue: [u64; 64]` + `completed: u64` + `len: u8` → one
  **`queue: VecDeque<(vrc: u64, completed: bool)>`** (front = oldest in-flight).

`take(vrc)` pushes `(vrc, false)` — unbounded; the ring's byte capacity remains
the real backpressure. `release(push_idx)` marks `queue[push_idx - front_idx].completed
= true`, then `while queue.front().is_some_and(|e| e.completed) { let (vrc,_) =
queue.pop_front(); front_idx += 1; last = vrc; }`, and if any were popped,
`advance_consume_cursor(last)` + wake the writer. This is **net simpler than the
current code**: it deletes the `completed` bitmap, the `trailing_ones`
bit-twiddling, *and* the `n == 64` shift-by-width special case (`w2m.rs:161-165`)
outright, while preserving the exact retirement order (front-consecutive
completed prefix).

`W2M_MAX_IN_FLIGHT` becomes advisory: keep it as the `VecDeque::with_capacity`
hint and a `gnitz_warn!`-once observability threshold, but it no longer gates
correctness. Remove the release-build `assert!(bit < 64)` — a dynamic queue
cannot overflow, and a stale/double `push_idx` still panics on `VecDeque`
out-of-bounds, so no real check is lost. Delete or downgrade the per-req_id
`debug_assert!` in `route_scan_slot` (`reactor/mod.rs:987-992`); it is per-req_id
and never saw the per-ring aggregate anyway (`scan_parked` is
`FxHashMap<u32, VecDeque<W2mSlot>>`).

**Pointer safety is sound (no dangling on `VecDeque` growth).** A `W2mSlot`
holds `state: *mut InFlightState` (the struct's stable address) plus
`&'static` slices into the ring mmap — it never borrows the queue's heap buffer.
`in_flight: Vec<UnsafeCell<InFlightState>>` is built **once** in
`W2mReceiver::new` and never re-pushed (`attach_w2m` panics on a second attach),
so struct addresses are stable; growing the `VecDeque` *inside* a struct never
moves the struct. All access is single-threaded via sequential `Drop`→`release`.

Peak bookkeeping memory is `~len × 16` bytes (the `(u64, bool)` queue). `len` is
ultimately bounded by ring-byte capacity — a full 1 GiB ring of ~64-byte minimum
frames is ~16M in flight → ~128 MB of queue in the absurd worst case (still
dwarfed by the 1 GiB ring), and a few KB in practice. The real balloon bound is
**client eviction** (`GNITZ_CLIENT_SEND_TIMEOUT_MS`, 30 s): a stalled reader's
lease is dropped and its parked frames released, retiring the front. The common
path (≤ a few in flight) stays a small `VecDeque` with O(1) push/prefix-pop — no
throughput regression for single scans or pushes.

## Vestigial consumers of the hard-64 to clean up

Growing `InFlightState` leaves two now-unnecessary constraints that encoded the
64 limit. Neither *breaks* (a dynamic structure holds more, so each stays
conservatively true), but both go vestigial and their rationale comments must be
updated or the constraints relaxed:

- **`worker/mod.rs:1460-1472`** — a compile-time `assert!(W2M_REGION_SIZE /
  min_preflight_frame_bytes < W2M_MAX_IN_FLIGHT)` that deliberately **sizes
  CREATE UNIQUE INDEX preflight frames large** so a full ring holds < 64 frames.
  This is a *workaround* for exactly this overflow on the preflight reply path;
  with the fix the sizing constraint can relax (smaller preflight frames become
  legal). Relax or delete the assert and its comment.
- **`worker/mod.rs:201-203`** — the `reply_frame_budget` doc instructing debug
  overrides to keep a train under 64 frames. Update to reflect that the 64 is no
  longer a hard abort.

## Consequence for multi-relation scan

The consistent multi-scan feature caps its relation count at 16 as a product /
master-state limit that also bounds the *byte-immune tiny-relation* contribution
to the per-ring in-flight count (large relations are already bounded by
byte-backpressure, so the cap is not the whole safety story — see that plan). It
is production-safe standalone on that basis. With this fix the fixed-64 ceiling
is gone entirely, so the cap reverts to a pure product / master-memory bound and
could be raised. No cross-dependency: multi-scan is safe standalone; this fix
removes the ring-frame constraint and the frame-sizing subtlety, and improves the
general single-scan case.

## Tests

**Rust engine (w2m):** drive `InFlightState` past 64 concurrent `take()`s with
out-of-order `release()`s and assert correct `consume_cursor` advancement and no
panic (the current code aborts here); a randomized property test of
interleaved take/release against a reference model of the retirement order;
a large-count (e.g. 10k) take/release sweep asserting bounded memory and correct
final cursor.

**Rust engine (reactor):** `drain_w2m_for_worker` drains >64 parked small scan
frames on one ring without abort while interleaved exchange/ACK frames on the
same ring are still routed (assert `exchange_acc` sees the frame that sits behind
the parked scan frames — the property the rejected drain-gate would break).

**E2E (pytest, `GNITZ_WORKERS=4`):** many concurrent slow-reader scans of a
replicated tiny table (all single-sourced from worker 0) exceeding 64 parked
frames; assert every scan completes correctly and the server stays healthy,
including a concurrent view-maintenance tick completing (exchange not starved).
