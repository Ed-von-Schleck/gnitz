# BPF for io_uring in the gnitz Reactor: Feasibility Study

Research date: 2026-06-14

Context: Linux 7.1 was released today (2026-06-14) carrying Pavel Begunkov's
BPF-driven io_uring event-loop hook. The feature is **not yet available on the
development machine** but will be in the near future. This document records what
the feature actually is, maps the gnitz master/worker runtime in the detail
needed to judge applicability, and assesses whether — and where — gnitz could
use it.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [The Feature: BPF for io_uring](#2-the-feature-bpf-for-io_uring)
   - 2.1 [Three distinct things under one banner](#21-three-distinct-things)
   - 2.2 [Begunkov's loop hook (the relevant one)](#22-begunkovs-loop-hook)
   - 2.3 [Ming Lei's IORING_OP_BPF](#23-ming-leis-ioring_op_bpf)
   - 2.4 [SQE filtering (7.0)](#24-sqe-filtering)
   - 2.5 [Capability and security model](#25-capability-and-security-model)
   - 2.6 [Status and timeline](#26-status-and-timeline)
3. [gnitz Runtime Architecture (current state)](#3-gnitz-runtime-architecture)
   - 3.1 [Process and memory model](#31-process-and-memory-model)
   - 3.2 [The single multiplexing reactor](#32-the-single-multiplexing-reactor)
   - 3.3 [Master to Worker: SAL + eventfd](#33-master-to-worker)
   - 3.4 [Worker to Master: W2M ring + FUTEX_WAKE](#34-worker-to-master)
   - 3.5 [The master's FUTEX_WAITV park cycle](#35-the-masters-futex_waitv-park-cycle)
   - 3.6 [Per-tick coordination and round-trip accounting](#36-per-tick-coordination)
   - 3.7 [The submit-then-flush pattern](#37-the-submit-then-flush-pattern)
   - 3.8 [fdatasync batching](#38-fdatasync-batching)
4. [Fit Analysis: where BPF could apply](#4-fit-analysis)
   - 4.1 [Candidate 1 — collapse the per-tick wake storm](#41-candidate-1)
   - 4.2 [Candidate 2 — scan-stream relay splicing](#42-candidate-2)
   - 4.3 [Candidate 3 — fdatasync (rejected)](#43-candidate-3)
   - 4.4 [What BPF cannot do on this hot path](#44-what-bpf-cannot-do)
5. [Costs and Risks](#5-costs-and-risks)
6. [Adoption Path (if pursued)](#6-adoption-path)
7. [Recommendation](#7-recommendation)
8. [Open Questions](#8-open-questions)
9. [References](#9-references)

---

## 1. Executive Summary

Linux 7.1 lets a process attach a BPF program to an io_uring instance so the
program runs **inside the kernel's event loop** when userspace calls
`io_uring_enter()`. The program can drain completions and submit new submissions
without returning to userspace, and decides via a return code whether to loop
again or surface to the application. The motivation is eliminating
userspace↔kernel context switches in I/O-driven loops.

gnitz is structurally a **good fit** for exactly one reason: the master process
already funnels *everything* — client network I/O, timers, `fdatasync`, and
worker-reply wakeups — onto a **single** io_uring instance
(`crates/gnitz-engine/src/runtime/reactor/mod.rs`). The mechanism that wakes the
master when workers publish results (a persistent `FUTEX_WAITV` SQE spanning
every worker's `reader_seq` word, plus a userspace drain/refresh/re-arm cycle on
each wake) is a textbook candidate for moving into a BPF loop callback.

The honest verdict: **applicable, but not now.** The realistic payoff is
bounded — it removes spurious/partial userspace wakeups and the per-wake
re-arm overhead under skewed or high-frequency tick loads; it does **not** lower
the fundamental wakeup-latency floor, and balanced fan-out already coalesces into
roughly one wake per tick. Against that bounded upside sit real costs:
`CAP_BPF`/`CAP_SYS_ADMIN` at load time (a concern for an unprivileged database), a
brand-new and still-moving kfunc API, no mature Rust binding story, and the
insertion of a Rust/BPF split into the single most race-sensitive code in the
project. The feature is additive and can be **runtime-probed with the existing
FUTEX_WAITV path as fallback**, exactly like gnitz already probes io_uring opcode
support today. Track it; revisit when the API has stabilized across a couple of
kernel releases and profiling actually shows per-wake overhead as a bottleneck.

---

## 2. The Feature: BPF for io_uring

### 2.1 Three distinct things under one banner {#21-three-distinct-things}

"BPF + io_uring" conflates three separate efforts with very different purposes.
Only the second is relevant to gnitz performance.

| Effort | Kernel | Purpose | Relevant? |
|---|---|---|---|
| SQE filtering (Axboe) | 7.0 | Sandbox: allow/deny SQE opcodes | No (security, not perf) |
| **Loop hook (Begunkov)** | **7.1** | **Run BPF in the event loop; submit/complete without leaving the kernel** | **Yes** |
| `IORING_OP_BPF` (Ming Lei) | pending | BPF program as an SQ entry; logic stays in userspace | Marginal |

### 2.2 Begunkov's loop hook (the relevant one) {#22-begunkovs-loop-hook}

A user associates a BPF `struct_ops` program with an io_uring instance. When
userspace calls `io_uring_enter()`, the registered program runs **within the
kernel's event loop** in place of the standard wait/return path. The callback
shape reported by LWN is:

```c
int (*loop)(struct io_ring_ctx *ctx, struct iou_loop_state *ls)
```

Key kfuncs:

- `bpf_io_uring_submit_sqes()` — submit submission-queue entries from inside the
  program.
- `bpf_io_uring_get_region()` — access the SQ/CQ ring memory.

Control flow: the program returns either

- `IOU_LOOP_CONTINUE` — re-run the program (optionally after a delay or a
  completion-count threshold), staying in the kernel; or
- `IOU_LOOP_STOP` — return control to userspace.

Safety contract: the program must tolerate **spurious wakeups** and
**kernel-initiated cancellation**. If the owning task is killed, the kernel
terminates BPF execution and cleanly releases io_uring resources. Begunkov notes
that userspace↔BPF communication "can be handled efficiently using a BPF arena."

The stated use cases: avoid context switches across complex I/O sequences;
migrate existing io_uring extensions (e.g. `IOSQE_IO_DRAIN`) into BPF to simplify
core kernel code; and experiment with polling/scheduling strategies before
committing them to the kernel.

### 2.3 Ming Lei's IORING_OP_BPF {#23-ming-leis-ioring_op_bpf}

A competing, narrower design: a new `IORING_OP_BPF` operation places a BPF
program directly in the submission queue. Users register a `uring_bpf_ops`
struct with an operation ID (up to 256 BPF-defined ops) and four function
pointers: `prep_fn`, `issue_fn`, `fail_fn`, `cleanup_fn`. Kfuncs read request
data from SQEs, store results in CQEs, and bulk-copy between buffers. The design
keeps "the bulk of the application logic, including the creation of SQEs, in user
space," trading flexibility for a more intuitive, targeted model. As of 7.1 this
half still needs rebasing; acceptance looks likely but it is not merged.

This model does **not** help gnitz: gnitz's problem is the *wait/wake* loop, not
per-SQE custom operations.

### 2.4 SQE filtering (7.0) {#24-sqe-filtering}

Axboe's earlier, already-shipped feature: load a **classic** BPF program to
allow/deny SQE operations per opcode, with stackable filters and container-aware
use. This is a sandboxing primitive (restrict what an io_uring can do), not a
performance lever, and is unrelated to the loop hook beyond sharing the
"BPF + io_uring" name.

### 2.5 Capability and security model {#25-capability-and-security-model}

The central operational concern raised in review: **BPF requires elevated
privilege (CAP_BPF, and CAP_SYS_ADMIN for `struct_ops` program loading), whereas
io_uring itself does not.** Moving functionality into BPF therefore restricts it
to privileged processes. For a database intended to run unprivileged this is the
single biggest adoption friction. The standard mitigation is to **load the BPF
program at startup while privileged, then drop capabilities** before serving — but
that adds privileged-startup surface and operational complexity.

Additional review concerns: programs can blur the kernel/userspace boundary by
absorbing application logic; BPF is harder to write than userspace code; and
debugging/observability suffers because a task running a long BPF loop "appears
suspended indefinitely in a syscall" to tracing tools.

### 2.6 Status and timeline {#26-status-and-timeline}

- Begunkov's loop-hook patch set: merged by Jens Axboe on **2026-03-17**, shipped
  in **Linux 7.1** (released **2026-06-14**, today).
- Ming Lei's `IORING_OP_BPF`: not merged as of 7.1; needs rebasing.
- The kfunc surface (`bpf_io_uring_submit_sqes`, `bpf_io_uring_get_region`,
  return codes) is brand-new and should be treated as **unstable** until it has
  survived a few releases.

---

## 3. gnitz Runtime Architecture (current state)

All line references verified against the working tree at the research date
(commit `afb2222`).

### 3.1 Process and memory model {#31-process-and-memory-model}

gnitz is a **multi-process** engine: one master and N worker processes, forked at
boot, communicating exclusively through `MAP_SHARED` memory regions plus kernel
wakeup primitives (`crates/gnitz-engine/src/runtime/bootstrap.rs`). There are two
shared rings per worker:

- **SAL** (Shared Append-only Log) — master→worker broadcast; a single large mmap
  written sequentially by the master, read by all workers
  (`crates/gnitz-engine/src/runtime/sal.rs`).
- **W2M** (Worker-to-Master) — per-worker SPSC reply ring, a `MAP_SHARED` memfd
  with a 128-byte header (cursors + `u32` seq counters + `waiter_flags`) followed
  by a tail-chasing data region
  (`crates/gnitz-engine/src/runtime/w2m_ring.rs`, `w2m.rs`).

Workers do **not** run io_uring for their main wait loop (one local io_uring
exists only for batched `fdatasync`, §3.8). Only the master runs a reactor.

### 3.2 The single multiplexing reactor {#32-the-single-multiplexing-reactor}

The master owns **one** io_uring (`reactor/mod.rs`) that multiplexes every event
source onto one wait. CQE `user_data` packs an 8-bit kind tag in the high byte;
the kinds enumerate the entire I/O surface (`reactor/mod.rs:76-83`):

```
KIND_REPLY  KIND_TIMEOUT  KIND_FSYNC  KIND_FUTEX_WAITV
KIND_ACCEPT KIND_RECV     KIND_SEND   KIND_FUTEX_CANCEL
```

The event loop is `tick()` (`reactor/mod.rs:932-989`):

0. **Proactive W2M drain** — lost-wake safety net: if the FUTEX_WAITV SQE is
   armed, `try_consume` each worker ring (two Acquire loads each)
   (`reactor/mod.rs:947-954`).
1. **Drain CQEs** from the memory-mapped CQ (no syscall) and dispatch
   (`reactor/mod.rs:1014-1023`, `dispatch_cqe` at `1025`).
2. **Poll the run queue** — each ready task polled at most once per tick.
3. **Submit + optionally block**: if blocking and nothing is runnable,
   `submit_and_wait_timeout(1, -1)` sleeps until the next CQE
   (`reactor/mod.rs:980-988`). This one syscall is where the master parks on the
   union of {worker replies, client I/O, timers, fsync}.

This unification is the architectural keystone: the master sleeps in exactly one
place on every kind of event at once.

### 3.3 Master to Worker: SAL + eventfd {#33-master-to-worker}

The master writes work groups into SAL (reserve → fill → release-store header;
the `SAL write cursor` invariant in `async-invariants.md`), then signals workers
via **eventfd** — the master→worker (M2W) wakeup is a plain eventfd counter, the
only M2W primitive left after the W2M migration (`M2W eventfd re-arm` invariant,
`async-invariants.md`). Tick emission writes all groups then calls `signal_all`
**once** (`Tick batch signalling` invariant).

A worker's main loop parks on that eventfd (`worker.rs:456-493`):

```rust
loop {
    if self.pending_streams.is_empty() {
        let ready = self.sal_reader.wait(1000);   // eventfd_wait on m2w_efd
        if ready == 0 { /* timeout: check getppid for master death */ continue; }
        if ready < 0 { continue; }
    }
    if self.drain_sal() { return 0; }              // shutdown
}
```

`SalReader::wait` is `ipc_sys::eventfd_wait(self.m2w_efd, timeout_ms)`
(`sal.rs:1082-1085`). SAL reads are zero-copy: `try_read` returns a `&[u8]` slice
straight into the SAL mmap (`sal.rs:1050-1081`).

### 3.4 Worker to Master: W2M ring + FUTEX_WAKE {#34-worker-to-master}

When a worker has a reply it calls `W2mWriter::send_encoded` (`w2m.rs:71-94`):

```rust
encode_fn(slice);                                  // encode directly into ring slot
w2m_ring::commit(hdr, reservation);                // Release-store write_cursor
hdr.reader_seq().fetch_add(1, Ordering::Release);
if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_MASTER_PARKED != 0 {
    ipc_sys::futex_wake_u32(hdr.reader_seq() as *const AtomicU32, 1);  // raw v1 FUTEX_WAKE
}
```

The `FUTEX_WAKE` is a raw v1 syscall (no `FUTEX_PRIVATE_FLAG` — the ring is
`MAP_SHARED`), and is **gated on the `FLAG_MASTER_PARKED` flag** so the worker
skips the syscall whenever the master is already awake (`w2m_ring.rs:36-66` module
doc). Backpressure is symmetric: a writer that fills the ring sets
`FLAG_WRITER_PARKED` and `FUTEX_WAIT`s on `writer_seq`.

### 3.5 The master's FUTEX_WAITV park cycle {#35-the-masters-futex_waitv-park-cycle}

This is the load-bearing mechanism and the prime BPF candidate. The master arms
**one** persistent `IORING_OP_FUTEX_WAITV` SQE spanning **every** worker's
`reader_seq` word with `FUTEX2_SIZE_U32`, shared (`W2M master wait` invariant;
`attach_w2m` at `reactor/mod.rs:554-578`; `arm_futex_waitv` at `584-604`). A
single SQE therefore watches all N workers simultaneously.

On each wake (CQE under `KIND_FUTEX_WAITV`, `reactor/mod.rs:1039-1063`):

```rust
self.inner.futex_waitv_armed.set(false);
if !self.inner.shutdown.get() {
    loop {
        for w in 0..nw { self.drain_w2m_for_worker(w); }   // drain every ring
        if !self.refresh_futex_waitv_vals() { break; }     // re-snapshot N words
    }
    self.arm_futex_waitv();                                 // re-submit the SQE
}
```

`refresh_futex_waitv_vals` (`reactor/mod.rs:620-665`) is the per-wake cost: for
each of N workers it (re-)publishes `FLAG_MASTER_PARKED` (AcqRel RMW, skipped when
already set), snapshots `reader_seq` (Acquire), writes the `FutexWaitV` entry, and
checks `write_cursor != read_cursor` for the lost-wake race. The store ordering
(flag before reader_seq load) is what closes the wake window. The "wake index is
not authoritative" property forces draining **all** rings on every wake, not just
the one the kernel flagged.

**The cost shape that matters for BPF:** every time *any* worker advances its
`reader_seq`, the master wakes and pays a full N-word refresh + SQE re-arm — even
if only one worker replied and the in-flight tick is waiting on the other N−1.

### 3.6 Per-tick coordination and round-trip accounting {#36-per-tick-coordination}

A tick (`executor.rs` `tick_loop_async`; `master.rs` `run_tick`;
`async-invariants.md`):

1. **Master**: acquire `sal_writer_excl`, write all `FLAG_TICK` groups to SAL,
   `signal_all` once (N eventfd writes), drop the lock, then `join_all` the
   per-(tid, worker) `req_id` ACK futures via reactor reply routing.
2. **Each worker**: eventfd wake → `drain_sal` → process → `send_encoded` reply →
   conditional `FUTEX_WAKE`.
3. **Master**: parked on the FUTEX_WAITV SQE inside `submit_and_wait_timeout(1,
   -1)`; wakes, drains all rings, routes each decoded reply to its task waker by
   `req_id`; `join_all` resolves when the last ACK lands.

Round-trip accounting per tick:

- **M→W**: N eventfd writes (one blocking syscall batch).
- **W→M**: up to N `FUTEX_WAKE` syscalls (gated; one per replying worker that
  finds the master parked).
- **Master wakeups**: **1 to N**. If all workers reply within one scheduler
  quantum, the first `FUTEX_WAKE` wakes the master and a single drain collects all
  N replies. If replies are staggered (skewed worker load, large N, or high tick
  frequency), the master wakes multiple times, each paying a full
  refresh + re-arm. **This 1..N spread is the addressable inefficiency.**

### 3.7 The submit-then-flush pattern {#37-the-submit-then-flush-pattern}

Every `prep_*` call in the reactor is immediately followed by a flush-only
`submit_and_wait_timeout(0, 0)` to push the SQE to the kernel: send
(`reactor/mod.rs:858-868`), recv (`777`), accept (`722`), fsync (`678`),
FUTEX_WAITV re-arm (`595`). The SQ capacity is 256 and auto-flushes when full
(`ensure_sq_room`, `reactor/uring.rs`; latent issue `L5` in `async-invariants.md`).
Consequence: each network op costs a submit syscall now and a reap (in the
blocking tick) later. This is a deliberate latency-minimizing choice, not an
accident, and is **not** primarily what BPF would change.

### 3.8 fdatasync batching {#38-fdatasync-batching}

Durability `fdatasync` is already efficient. The reactor submits an fsync SQE
early so disk I/O overlaps CPU DAG evaluation (`submit_fsync`,
`reactor/mod.rs:667-687`; the `pre_write_pushes` Phase-A overlap). Worker-side
`uring_batch_fdatasync` (`worker.rs`) pushes up to SQ-capacity fsync SQEs then
`submit_and_wait`s for all CQEs, looping if the fd count exceeds SQ entries. This
is a fan-out-then-barrier, not a dependent chain — there is nothing for a BPF loop
to collapse.

---

## 4. Fit Analysis

### 4.1 Candidate 1 — collapse the per-tick wake storm {#41-candidate-1}

**Best fit.** Replace the userspace FUTEX_WAITV wait/drain/refresh/re-arm cycle
(§3.5) with a BPF loop callback. The program, running inside `io_uring_enter`,
would:

- Watch the N `reader_seq` words.
- On an advance, do the trivial in-kernel bookkeeping (the cursor/seq checks that
  `refresh_futex_waitv_vals` does today) and decide whether the in-flight tick is
  complete — e.g., compare a per-tick completion count/bitmask kept in a **BPF
  arena** that userspace stamps when it issues the tick.
- Return `IOU_LOOP_CONTINUE` to keep waiting in-kernel while replies are still
  outstanding (or while a client CQE/timer has not fired), and `IOU_LOOP_STOP`
  only when the batch is worth surfacing.

Effect: collapse the **1..N** master wakeups per tick (§3.6) into a single
userspace surfacing, and move the per-wake N-word refresh + SQE re-arm into the
kernel. The wire decode and waker routing still happen in userspace, but once per
tick instead of once per partial wake.

**Honest magnitude.** Bounded and workload-dependent. Balanced fan-out already
coalesces to ~one wake (the existing drain-all-rings-per-wake design), so the win
concentrates on **skewed worker load, large N, and high tick frequency**. It does
**not** reduce the wakeup-latency floor: the futex word still has to change and
the io_uring task still has to be scheduled. The gain is CPU/wakeup overhead, not
latency.

### 4.2 Candidate 2 — scan-stream relay splicing {#42-candidate-2}

The scan path is the one genuine multi-hop dependent-I/O chain that crosses the
network: workers publish chunked scan frames into W2M; the master forwards each as
a `W2mSlot` to the client socket **without intermediate decode/copy**
(`master.rs:2763-2774`; reactor `send_slot`/`send_slot_or_close`,
`reactor/mod.rs:811-840`). This is the closest gnitz comes to the network-proxy
use case BPF+io_uring targets: in principle a BPF program could submit the client
`SEND` directly off a W2M completion in-kernel.

**Verdict: avoid (for now).** The framing (length-prefix), the `active_scans`
liveness check that drops frames from abandoned scans
(`reactor/mod.rs:230-234`), and socket flow control (the `pending_recv`/one-RECV
invariants) are all userspace logic. Doing this in BPF means reimplementing the
framer and routing inside the verifier's constraints. High complexity, uncertain
payoff.

### 4.3 Candidate 3 — fdatasync (rejected) {#43-candidate-3}

Already a clean fan-out-then-barrier (§3.8). No dependent chain to collapse. BPF
adds nothing.

### 4.4 What BPF cannot do on this hot path {#44-what-bpf-cannot-do}

BPF in the loop hook is good at: reading ring memory, simple counter/bitmask
arithmetic over shared words, submitting pre-shaped SQEs, deciding loop/stop. It
is **not** a place to run:

- W2M **wire decoding** (`DecodedWire`, batch backing, exchange-accumulator logic
  in `reactor/exchange.rs`).
- Rust **task-waker dispatch** and the `req_id → waker` routing
  (`reply_wakers`, `route_reply`).
- The **exchange/relay** state machine (`PendingRelay`, SAL write-back).

So even Candidate 1 keeps all genuinely complex logic in userspace; BPF only gates
*when* userspace is woken and absorbs the trivial re-arm bookkeeping. That ceiling
on what BPF can absorb is exactly why the projected win is bounded.

---

## 5. Costs and Risks

1. **Privilege (the big one).** Loading a `struct_ops` BPF program needs
   `CAP_BPF`/`CAP_SYS_ADMIN`; io_uring itself does not (§2.5). gnitz presumably
   wants to run unprivileged. Mitigation — load at startup while privileged, then
   drop caps — is workable but adds privileged-startup surface.
2. **Bleeding edge.** Shipped today (7.1). The kfunc API is new and Ming Lei's
   complementary half is not even merged. Expect churn; treat the API as unstable.
3. **Complexity in the riskiest code.** The FUTEX_WAITV park cycle is already the
   subtlest unsafe Rust in the tree — lost-wake ordering, the proactive-drain
   safety net, the AsyncCancel-before-free dance for SQE storage lifetime
   (`reactor/mod.rs:404-441`, the `SQE buffer lifetime` invariant). Splitting that
   across a Rust/BPF boundary multiplies the surface for lost-wake and
   use-after-free bugs.
4. **Debuggability.** A task in a long BPF loop "appears suspended indefinitely in
   a syscall" to tracing tools — directly at odds with debugging a reactor whose
   failure modes are park-forever hangs.
5. **No Rust binding story.** No mature `libbpf-rs` path for these specific kfuncs
   / the loop `struct_ops` yet. Early adoption means hand-rolling the BPF object
   build, skeleton, and arena plumbing.
6. **Kernel floor.** gnitz already requires Linux 6.7+ for `IORING_OP_FUTEX_WAITV`
   (probed at `reactor/mod.rs:264-311`). A 7.1 dependency for an *optional*
   optimization is acceptable **only** if it stays optional (see §6).

---

## 6. Adoption Path (if pursued)

The feature is **additive and probe-gated**, which de-risks deferral entirely:

1. **Feature-probe at `Reactor::new`**, mirroring `probe_futex_waitv_support_inner`
   (`reactor/mod.rs:264-311`): attempt to attach the loop `struct_ops`; on
   `ENOSYS`/`EPERM`/verifier rejection, fall back to today's FUTEX_WAITV path. No
   hard 7.1 floor; no behavior change on older kernels or unprivileged
   deployments.
2. **Scope to Candidate 1 only.** The target is the wait/drain/refresh/re-arm loop
   in §3.5 — one well-bounded mechanism, not a reactor rewrite.
3. **BPF arena as the userspace↔BPF channel.** Userspace stamps the per-tick
   expected-completion set (count or bitmask over the N workers); BPF reads it,
   compares against observed `reader_seq` advances, and decides loop/stop.
4. **Keep wire decode + waker routing in userspace** (§4.4). BPF only gates
   surfacing and does the N-word re-arm bookkeeping.
5. **Gate the whole effort on a profile.** Only build this once a flamegraph shows
   per-wake `refresh_futex_waitv_vals` cost or a measurable 1..N wake storm under
   real tick load. Until 7.1 is on the box this cannot even be measured.

---

## 7. Recommendation

**Do not adopt now. Track it.** gnitz's single-ring multiplexing reactor makes it
an unusually clean structural fit for Begunkov's loop hook, but the payoff is a
bounded CPU/wakeup-overhead win (not latency, not correctness), and it lands
against real privilege, stability, complexity, and tooling costs. Because the
feature is probe-gated and additive, there is **zero cost to waiting**: adopt the
current FUTEX_WAITV design as the permanent fallback and revisit when (a) the
kfunc API has stabilized across ~2 kernel releases, (b) profiling on a 7.1 box
shows per-wake overhead or a wake storm as a real bottleneck, and (c) the
`CAP_BPF`-at-startup model is acceptable for the target deployment. The one
concrete prototype worth doing first, when those line up, is Candidate 1.

---

## 8. Open Questions

- **Exact capability requirement** for attaching an io_uring loop `struct_ops`
  program in 7.1 — `CAP_BPF` alone, or `CAP_SYS_ADMIN`? Determines whether the
  load-then-drop-caps pattern is viable.
- **Verifier constraints** on the loop callback: can it do the multi-word
  `reader_seq`/`write_cursor`/`read_cursor` comparison and arena bitmask logic
  Candidate 1 needs within instruction/complexity limits?
- **Interaction with the existing FUTEX_WAITV SQE.** Does the loop hook subsume
  futex-wait semantics, or would the program still arm/observe a FUTEX_WAITV SQE
  itself? (Affects how much of §3.5 actually moves into BPF.)
- **Spurious-wakeup contract** vs. gnitz's lost-wake invariants: does the "must
  tolerate spurious wakeups" requirement compose cleanly with the
  flag-before-load ordering in `refresh_futex_waitv_vals`?
- **Measured wake-storm frequency.** What is the real 1..N master-wakeup
  distribution per tick under skewed load? Unmeasurable until 7.1 is available;
  it is the gating number for the whole effort.

---

## 9. References

### Kernel feature

- BPF integration with io_uring — LWN.net, Article 1062286.
  <https://lwn.net/Articles/1062286/>
- BPF and io_uring, two different ways — LWN.net, Article 1046950.
  <https://lwn.net/Articles/1046950/>
- BPF meets io_uring (background) — LWN.net, Article 847951.
  <https://lwn.net/Articles/847951/>
- Linux 7.0 adds BPF filtering for io_uring — Phoronix.
  <https://www.phoronix.com/news/Linux-7.0-IO-uring-BPF-Filter>

### gnitz source (commit afb2222)

- `crates/gnitz-engine/src/runtime/reactor/mod.rs` — the reactor: `tick`
  (932-989), `dispatch_cqe` (1025), FUTEX_WAITV cycle (554-665, 1039-1063),
  opcode probe (264-311), submit-then-flush sites (595, 678, 722, 777, 858-868),
  send_slot (811-840).
- `crates/gnitz-engine/src/runtime/worker.rs` — worker main loop (456-493),
  `uring_batch_fdatasync`.
- `crates/gnitz-engine/src/runtime/sal.rs` — `SalReader::try_read`/`wait`
  (1050-1086).
- `crates/gnitz-engine/src/runtime/w2m.rs` — `send_encoded` + gated FUTEX_WAKE
  (71-94).
- `crates/gnitz-engine/src/runtime/w2m_ring.rs` — ring protocol + ordering doc
  (36-66).
- `crates/gnitz-engine/src/runtime/master.rs` — scan fan-out / slot forwarding
  (2763-2774), bootstrap `wait_all_workers` (548-564).
- `crates/gnitz-engine/src/runtime/bootstrap.rs` — fork + shared-region setup.
- `async-invariants.md` — `M2W eventfd re-arm`, `W2M master wait`,
  `Tick batch signalling`, `SQE buffer lifetime`, latent issue `L5`.
