# Name the scan-pre-tick barrier contract

`handle_scan` in `runtime/executor.rs` drains pending view ticks before
reading. The loop was correct when `tick_tids` non-empty implied "work
to do" and empty implied "all done." Auto-tick broke that: when the
committer fires `TickTrigger::Auto` after a large push, the tick loop
drains `tick_tids` *before* the tick body completes. A scan that races
in during the gap observes `tick_tids.is_empty()`, breaks out of the
drain loop, and reads a not-yet-materialised view — silently returning
zero rows.

The race had been latent for who knows how long, manifesting as
`test_bulk` flakes that everyone treated as ambient noise. The
compound-PK work made the race deterministic on a single machine; the
fix turned out to be 5 lines, but only after the cause was identified.

Make the contract explicit so future contributors can't unknowingly
break it.

## Scope

In:

- Add `Shared::barrier_drain(&self) -> impl Future<Output = ()>`. Its
  documented contract: "When this future resolves, every push that
  ACK'd to a client before the call has had its view-propagation
  tick complete." Internally it sends one `TickTrigger::Drain` and
  awaits the ack, regardless of `tick_tids` state.
- `handle_scan` calls `barrier_drain` exactly once at entry, replacing
  the current loop. The "always wait" guarantee is what closes the
  race; the loop with the empty-check was the bug.
- Rust-level regression test that forces the race window: a test-only
  knob `GNITZ_TEST_DELAY_TICK_BODY_MS` adds a sleep inside `run_tick`
  before it actually computes the view. Without the barrier, a scan
  immediately after a push would observe an empty view. With the
  barrier, it must observe the full view.
- Doc comment on `TickTrigger::Drain` and `Shared::tick_tids` that
  names the contract and forbids the "if empty, skip" optimisation
  on the consumer side.

Out:

- Removing the auto-tick path or changing its threshold. The race was
  on the consumer side, not the producer; auto-tick is a real
  optimisation worth keeping.
- Adding a barrier to non-scan paths (subscribe, seek). Those either
  already imply tick freshness (subscribe waits for the next tick by
  construction) or are explicitly snapshot-style (`seek` reads
  whatever's there).
- Tracking in-flight tick counts via atomics. The serialisation that
  comes from the tick loop processing triggers FIFO is enough; an
  extra counter would re-implement the same invariant out-of-band.

## Touchpoints

`crates/gnitz-engine/src/runtime/executor.rs`:

```rust
impl Shared {
    /// Wait for every push that has already ACK'd to a client to
    /// have its view-propagation tick complete. This is the read-
    /// path counterpart of the auto-tick / drain machinery: it
    /// guarantees serialisation behind any pending or in-flight
    /// tick, regardless of `tick_tids` state.
    ///
    /// Sends one `TickTrigger::Drain` and awaits its ack. The tick
    /// loop processes triggers FIFO, so when the ack fires every
    /// earlier trigger (Auto, Drain, threshold-fired) has run to
    /// completion.
    ///
    /// Callers that need stronger guarantees (e.g. "tick after my
    /// own push") should send their push first, then call this.
    pub async fn barrier_drain(&self) {
        let snapshot: Vec<i64> = self.tick_tids.borrow().clone();
        let (tx, rx) = oneshot::channel::<()>();
        self.tick_tx.send(TickTrigger::Drain {
            tids: snapshot,
            done: tx,
        });
        let _ = rx.await;
        // Re-check: if a new push landed *during* our drain, it
        // queued behind the barrier and the next scan will pick
        // it up via its own barrier_drain. The scan-handler caller
        // doesn't loop because it would only observe pushes that
        // happen AFTER its entry, which it has no obligation to
        // wait for (those pushes ACK to other clients, with their
        // own subsequent scans).
    }
}

async fn handle_scan(...) {
    // Replace the existing drain loop with:
    shared.barrier_drain().await;
    let _g = shared.catalog_rwlock.read().await;
    // ... rest unchanged ...
}
```

`crates/gnitz-engine/src/runtime/executor.rs::run_tick` (around the
top of the body): test-only sleep gate.

```rust
#[cfg(test)]
async fn maybe_delay_tick_body() {
    if let Ok(ms) = std::env::var("GNITZ_TEST_DELAY_TICK_BODY_MS") {
        if let Ok(n) = ms.parse::<u64>() {
            tokio::time::sleep(Duration::from_millis(n)).await;
        }
    }
}

async fn run_tick(...) -> Result<(), String> {
    #[cfg(test)] maybe_delay_tick_body().await;
    // ... existing body ...
}
```

## Implementation sketch — regression test

```rust
// crates/gnitz-engine/src/tests/scan_barrier.rs (new)

#[tokio::test]
async fn scan_after_push_sees_view_under_forced_tick_delay() {
    // 1. Set GNITZ_TEST_DELAY_TICK_BODY_MS=200 in the spawned
    //    server's env.
    // 2. CREATE TABLE / CREATE VIEW (GROUP BY).
    // 3. Push 100k rows.
    // 4. Immediately scan(view) — without barrier_drain this
    //    races and returns 0 rows.
    // 5. Assert all 100 groups present.
}
```

The test fails on the *pre-fix* implementation deterministically
(200 ms sleep > network RTT between push ACK and scan request) and
passes on the post-fix implementation. Without this test, the
regression that landed the bug originally would have landed again.

## Testing

- `make e2e tests/test_bulk.py` (the suite that exposed the bug in
  the first place) passes deterministically across 10 consecutive
  runs. Currently passes by accident; the barrier makes it
  deterministic.
- The new `scan_barrier.rs` regression test passes via `make test`.
  It's a unit test, fast enough for the inner loop.
- Doc comment audit: every existing call site that reads
  `tick_tids` (search the workspace) either calls `barrier_drain`
  or has a comment naming why it can short-circuit. No silent
  empty-check optimisations.
