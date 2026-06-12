# Spike: Worker epoch-mismatch stall on independent restart

## Claim

`WorkerProcess::new` hardcodes `expected_epoch: 1`. If a worker process
restarts while the master stays alive at epoch N > 1, every SAL message
the worker reads will have `msg.epoch = N`, the epoch gate in
`next_sal_message` will return `None` indefinitely, and the worker will
stall — never processing a Flush, never advancing `expected_epoch`.

## Why it is uncertain

Workers detect master death via `getppid()` in `wait()` and exit. If the
master also restarts (the common recovery path), both sides reset to epoch
1 and there is no mismatch. The bug only fires if a worker can restart
**while the master continues**. This requires answering:

1. Does the master ever fork a replacement worker without restarting
   itself? (OOM killer hitting only a worker, or an explicit worker
   watchdog.)
2. If so, what SAL cursor does the new worker's `SalReader` start at —
   0 (replay all) or the current write head (skip past)?
3. Does `recover_from_sal` in `bootstrap.rs` reset the SAL write cursor
   or leave it where it was? If it resets to 0, the new worker would
   consume epoch-1 messages from a prior incarnation; if it does not,
   the new worker reads epoch-N messages with `expected_epoch = 1`.

## Questions to answer

- **Q1.** Find every `fork()` / `spawn_worker` call site in `master.rs`
  and `bootstrap.rs`. Is there a watchdog loop that re-forks a dead worker
  without restarting the master process?
- **Q2.** On the path where a replacement worker is forked, does the
  master write a synthetic epoch-1 sequence to the SAL before the new
  worker starts reading, or does it just resume at the current epoch?
- **Q3.** What is `SalReader::new`'s initial read cursor? Is it always 0
  or does it snap to the current write head?
- **Q4.** Does `next_sal_message` ever skip non-matching epochs (advancing
  past them) or does it stop at the first mismatch?

## Investigation steps

1. `grep -n "fork\|spawn_worker\|WorkerProcess::new" crates/gnitz-engine/src/runtime/master.rs`
   to find all worker-fork call sites and see if any are inside a restart
   loop rather than the initial bootstrap.

2. Read the section of `master.rs` that handles `SIGCHLD` or otherwise
   detects worker death. If there is a re-fork, trace what SAL state the
   master and the new worker share.

3. Read `SalReader::new` to check whether `read_cursor` starts at 0 or at
   the SAL's current committed write offset.

4. Read `recover_from_sal` in `bootstrap.rs` fully. Check whether it
   consumes epoch-1 messages and leaves the SAL write position at epoch N,
   or resets it.

5. If independent worker restart is confirmed possible, determine whether
   the fix belongs in:
   - `WorkerProcess::new`: scan forward from `read_cursor = 0` to find
     the first readable message and adopt its epoch.
   - The master re-fork path: write a synthetic Flush at epoch 1 before
     the new worker starts, or pass the current epoch to the new worker
     as a startup argument.
   - `next_sal_message`: treat epoch-mismatch as a skip (advance cursor
     and retry) rather than a hard stop — but only if the epoch is
     *higher* than expected (lower would mean a replay from a prior
     incarnation, which is a different problem).

## Expected outcome

Either:

- **Not a bug:** the master always restarts with the worker; independent
  worker restart is not implemented. Close the spike, no fix needed.
- **Bug confirmed:** document the exact fault path, the minimum epoch
  delta that triggers the stall, and a proposed fix location with a test
  case (e.g., a unit test that creates a `WorkerProcess` with
  `expected_epoch = 1`, feeds it a SAL message with `epoch = 3`, and
  asserts the worker does not stall).
