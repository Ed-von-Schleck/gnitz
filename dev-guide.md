# GnitzDB Developer Guide

## Build targets

| What | Command | Rebuilds |
|------|---------|----------|
| Unit tests | `make test` | gnitz-engine |
| Server binary | `make server` | gnitz-engine |
| Python extension | `make pyext` | gnitz-sql, gnitz-core, gnitz-py |
| E2E tests | `make e2e` | server + pyext, then runs pytest |
| Release binary | `make release-server` | gnitz-engine (release) |

`make e2e` rebuilds both the server binary and the Python extension
(which contains the SQL planner). Maturin is a no-op when nothing
changed (~0.5s), so there's no cost.

## Running E2E tests

**Always run E2E with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss distributed bugs.

```bash
make e2e                    # rebuilds server, GNITZ_WORKERS=4
```

## Multi-worker invariant checklist

When adding a new constraint, validation rule, or invariant:

- [ ] Is it enforced in the catalog for the master DDL path?
- [ ] Single-process vs multi-worker: FK validation runs in-process;
      partition-global constraints (unique indices) run distributed via master.
- [ ] Does it need to fire on every worker even for empty batches
      (e.g. exchange barrier participation)?
- [ ] Run `make e2e` (rebuilds both server and Python extension).
- [ ] Is there a multi-worker E2E test in `test_workers.py`?
- [ ] Is there a Rust unit test?
- [ ] Does the feature use `hash_row_by_columns` for exchange routing across
      tables where the same logical value may have different column types
      (e.g., PK = U64 vs join column = I64)? The hash must produce
      identical partitions regardless of type.

## Debug logging

```bash
GNITZ_WORKERS=4 GNITZ_LOG_LEVEL=debug uv run pytest tests/test_joins.py -x
```

Master log: `~/git/gnitz/tmp/server_debug.log`
Worker logs: `~/git/gnitz/tmp/last_worker_N.log`

Log format: `<epoch>.<ms> <tag> <level> <message>` — tags: `M` (master), `W0`–`WN`.

For temporary logging: `gnitz_debug!` / `gnitz_info!` macros. Remove before committing.

Live tail: `tail -f ~/git/gnitz/tmp/gnitz_py_*/data/worker_*.log`

## IPC flag constants

Flags are defined once in `ipc.rs` as `pub const FLAG_*: u32`.
Both `worker.rs` and `master.rs` import from there. When adding a new flag:

- [ ] Add to `ipc.rs`
- [ ] Handle in `worker.rs` dispatch loop
- [ ] Handle in `master.rs` if it requires master-side logic

## IPC optimization checklist

When modifying IPC message flow (new flags, pre-routing, stashing):

- [ ] State preconditions explicitly. For every column value the optimization
      reads: is it correctly filled for DELETE/retraction rows too?
- [ ] Is there an E2E test that pushes rows *and then retracts them*?
      Retraction tests are mandatory for any new IPC fast-path.
- [ ] Test each sub-phase in isolation before combining.
- [ ] Comment any guard/fallback code with the invariant it protects.

## Consolidation and merge correctness

Any code that merges or consolidates Z-Set batches must respect the
invariants in `foundations.md`. The critical rule:

> **All merge/consolidation paths must produce output sorted by
> (PK, payload), not just PK.**

When adding or modifying a merge path:

- [ ] Comparison function includes payload columns after PK.
- [ ] Handles within-cursor duplicates (MemTable runs are sorted but
      NOT consolidated).
- [ ] `compare_rows` respects column order, null handling, sign-extension.
- [ ] Test with same-PK, different-payload entries across ≥3 inputs.
- [ ] Test covers the flush-between-ticks scenario (compaction after
      L0_COMPACT_THRESHOLD ticks).

## Exchange schema contract

The exchange serializes batches using the **pre-plan's output schema**,
not the view's final output schema. This schema determines column sizes
during batch packing/unpacking.

> **The pre-plan's `out_schema` MUST match the physical layout of the
> batch at the exchange output register.**

If set to the view's final schema instead of the exchange intermediate
schema, STRING columns (16 bytes) may be serialized as I64 (8 bytes),
causing silent data corruption.

### SAL interleaving during exchanges

The SAL is a single shared channel. During async ticks, the master may
interleave DDL_SYNC messages with exchange relay messages. Exchange relays
use `FLAG_EXCHANGE_RELAY`. The worker exchange wait loop filters by this
flag: DDL_SYNC messages are decoded eagerly and deferred for post-DAG
dispatch; other non-relay messages are discarded. **Never assume the next
SAL message in an exchange wait is the relay.**

Every exchange code path must set `batch.schema` before `do_exchange`.
Missing it causes `encode_wire` to panic on `None` schema.

## Aggregate output type contract

REDUCE output schema column types:

- COUNT, COUNT_NON_NULL → **I64**
- SUM/MIN/MAX on F32/F64 → **F64**
- SUM/MIN/MAX on anything else → **I64**

The accumulator stores values as `u64` bit patterns (8 bytes). The output
column is always 8 bytes. MIN/MAX on STRING stores the German String
compare key (first 8 bytes), not the full 16-byte string struct.

## Benchmarking

```bash
make bench           # quick mode, 1 worker, ~60s
make bench-full      # full mode (150k rows), 4 workers, ~5 min
make bench-sweep     # sweep workers=1,2,4 × clients=1,2,4
make bench-perf      # full + perf record + perf stat
```

Results: `benchmarks/results/` (gitignored). The runner rotates old results (keeps 3).

Workflow: `make bench` → change → commit → `make bench` → compare `summary.json`.

## Debugging failures

1. **Log first, never guess.** Each rebuild costs ~50s. One well-instrumented
   run reveals more than ten speculative attempts.
   - `gnitz_debug!` / `gnitz_info!` for structured logging
   - `GNITZ_LOG_LEVEL=debug` to enable debug-level messages
   - `RUST_BACKTRACE=1` for panic backtraces
2. **Use the debug binary for crashes.** Release builds silently clamp
   corrupt values. `GNITZ_SERVER_BIN=../../gnitz-server`.
3. **Isolate with 1 worker first.** Pass with W=1 but fail with W=4 →
   bug is in exchange/fanout, not computation.
4. **Bisect by sub-path.** Disable the new fast-path to confirm the bug
   is in the new code.
5. **Verify your fix is in the binary.** `make e2e` rebuilds both the
   server and the Python extension before running tests.
