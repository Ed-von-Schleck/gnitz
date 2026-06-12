# Bound distributed-validation waits on worker death

## What

The master's pipelined distributed checks (FK existence, unique-index, upsert
PK) await every worker's reply with no deadline and no liveness awareness:

```rust
// crates/gnitz-engine/src/runtime/master.rs:855 (execute_pipeline_async)
let decoded_vec: Vec<DecodedWire> = crate::runtime::reactor::join_all_unpin(
    all_req_ids.iter().map(|&id| reactor.await_reply(id))
).await;
```

(The same pattern recurs at `master.rs:1722`.) `await_reply`
(`runtime/reactor/mod.rs:492`) returns a `ReplyFuture` that resolves only when
the target worker writes its reply into the W2M ring and bumps `reader_seq`,
waking the master's `FUTEX_WAITV`. If a worker dies, deadlocks, or otherwise
never writes its reply, the `ReplyFuture` stays `Pending` forever. Because the
master executor is single-threaded and cooperative, the whole master halts — all
database operations hang indefinitely.

## Why `tokio::time::timeout` does not apply

This runtime is **not** tokio. The reactor is a custom single-threaded
io_uring/FUTEX2 executor (`runtime/reactor/mod.rs`). It already provides the
primitives needed:

- `Reactor::timer(deadline: Instant)` (`reactor/mod.rs:485`) — an io_uring-backed
  timeout future.
- `Either` / `join_into` select combinators (imported at `reactor/mod.rs:65`).
- `KIND_FUTEX_CANCEL` machinery for cancelling outstanding SQEs.

Workers are forked OS processes; the master is their parent, so it holds their
pids and can probe liveness directly (`kill(pid, 0)` / `waitpid(.., WNOHANG)`).
Workers already detect *master* death via `getppid()`; this plan adds the
reverse on the validation await path.

## Change

Race the reply-join against a deadline (and/or a worker-death signal) using the
reactor's own select, rather than awaiting the join unconditionally:

```rust
// sketch — exact combinator per reactor API
let join = reactor::join_all_unpin(all_req_ids.iter().map(|&id| reactor.await_reply(id)));
let deadline = reactor.timer(Instant::now() + VALIDATION_DEADLINE);
match reactor::select(join, deadline).await {
    Either::Left(decoded_vec) => { /* existing path */ }
    Either::Right(()) => {
        // Deadline hit: probe worker liveness; a dead worker is a hard error,
        // not a transient slowdown. Surface it (or trigger the existing
        // controlled-shutdown path) instead of hanging.
        return Err(distributed_validation_timed_out(/* which workers未replied */));
    }
}
```

Design decisions to settle during implementation:

1. **Fixed deadline vs liveness-reset.** A fixed wall-clock cap risks false
   timeouts when a validation legitimately takes long under load. Prefer a
   *liveness-based* deadline: reset the timer whenever any watched `reader_seq`
   advances (the reactor already observes these on every `FUTEX_WAITV` CQE), so
   the timeout only fires when **no** worker has made progress for the window.
2. **Death detection vs timeout.** The strongest signal is the worker process
   actually being gone. On deadline, `waitpid(WNOHANG)` / `kill(pid, 0)` each
   worker; treat a dead worker as a fatal, non-retryable error and route it
   through whatever controlled-abort path the master already uses for fatal
   worker conditions. A live-but-slow worker that breaches the cap should log
   loudly but may warrant a longer or liveness-reset window rather than a hard
   failure.
3. **Cancellation hygiene.** On the timeout branch, the still-pending
   `ReplyFuture`s must be dropped/cleaned so their request-id waker slots in the
   reactor are not leaked. Confirm `ReplyFuture::drop` (or an explicit cancel)
   removes the entry from the reply-waker map.

## Scope / non-goals

- Only the distributed-validation await paths (`master.rs:855` and `:1722`).
  General W2M draining and the exchange barrier are out of scope.
- Not a worker-supervision/restart system — this bounds the *master's wait* and
  converts an indefinite hang into a surfaced, diagnosable failure.

## Testing

- Unit/integration test with a fault-injected worker that never replies to a
  validation request: the master must return an error within the deadline rather
  than hanging. Use a generous test timeout and assert the error path fires.
- Verify reply-waker slots are reclaimed after a timeout (no growth in the
  reactor's pending-reply map across repeated injected timeouts).
- `make e2e` must stay green — the happy path (all workers reply promptly) must
  be unaffected and must not pay a measurable latency cost from the added timer.
