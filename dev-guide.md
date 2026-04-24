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
changed, so there's no cost.

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
- [ ] **Narrow-PK hash invariance**: if the code touches exchange routing or
      XOR8 filter construction for U64-PK tables, confirm it zero-extends the
      stored value to u128 before hashing (`hash_u128(pk as u128)`). The
      physical storage uses 8 bytes/row for U64 PKs, but partition assignment
      must be identical to a U128 table holding the same value.
- [ ] **Schema-dependent PK stride**: IPC fast paths (WAL encode/decode, shard
      read/write, wire batch serialization) derive `pk_stride` from the schema
      (`pk_stride = 8` for U64, `pk_stride = 16` for U128). Hard-coding 16 is
      wrong for U64-PK tables. See `foundations.md` §6 for the region layout.

## Debug logging

```bash
GNITZ_WORKERS=4 GNITZ_LOG_LEVEL=debug uv run pytest tests/test_joins.py -x
```

Log format: `<epoch>.<ms> <tag> <level> <message>` — tags: `M` (master), `W0`–`WN`.

For temporary logging: `gnitz_debug!` / `gnitz_info!` macros. Remove before committing.

### Where test logs go

Server stderr (master process): always written to `~/git/gnitz/tmp/server_debug.log`
(see `tests/conftest.py`). Pytest's `-s` flag does NOT capture this — it lives
on disk regardless of pytest's stdout/stderr capture mode.

Worker logs: workers write to `<data_dir>/worker_<N>.log` inside the per-test
tmpdir. The conftest copies them to `~/git/gnitz/tmp/last_worker_N.log` on
session teardown — those are the canonical post-mortem files.

Live tail (during a long-running test): `tail -f ~/git/gnitz/tmp/gnitz_py_*/data/worker_*.log`

Pre-existing logs from the previous session are overwritten on the next test
run, so save copies before re-running if you need them.

### Using tests for debugging

1. **Reproduce the failure on a single test** with `pytest tests/foo.py::test_bar -v`.
   Always pass `GNITZ_WORKERS=4` — multi-worker bugs hide at W=1.
2. **Re-run a few times** to check determinism. Flaky failures often point to
   a race that the deterministic single-test run will mask.
3. **Read both logs side-by-side** — the master log shows what was
   dispatched; the worker log shows what was processed. Discrepancies in
   ordering between them are usually the smoking gun.
4. **Add temporary `gnitz_info!` lines** at the suspected boundary
   (handler entry, SAL emit, ACK reply). `make server pyext` then re-run.
   Strip them before committing.
5. **Use the debug binary** (the default `make server` output). Release
   builds clamp corrupt values silently and hide the real failure mode.
6. **Test logs survive the session**, code state does NOT — if you want
   to attach a log to a bug report, copy it out of `~/git/gnitz/tmp/`
   before the next test run overwrites it.

## SAL durability contract

**Rule: an ACK to a client implies fdatasync iff the operation upserted data.**

The SAL (Shared Append-Only Log) carries both data writes and ephemeral
commands on the same mmap'd fd. Workers see all SAL entries immediately
via Acquire/Release atomics on the size prefix — fdatasync is irrelevant
for cross-process visibility. It exists solely for crash recovery.

**Requires fdatasync before ACK** (upserts table data):
- `flush_pending_pushes` — batched user-table DML (already batched: one
  fdatasync per batch).
- `broadcast_ddl` — system-table writes (CREATE TABLE/VIEW/INDEX, etc.).
- `fan_out_push` / `fan_out_ingest` — non-batched data writes.

**Does NOT require fdatasync** (no upserted data):
- `fan_out_tick` — triggers view evaluation (derived, re-derivable).
- `fan_out_scan` — read-only table scan.
- `fan_out_seek` / `fan_out_seek_by_index` — read-only point lookups.
- `fan_out_backfill` — view materialization (derived).
- `check_pk_exists_broadcast` / `check_index_keys` — validation queries.

These command-only operations use `signal` (eventfd wake) without
fdatasync. If the server crashes, commands are lost but nothing needs
recovery — base table data is intact, views are re-derived, and
in-flight reads have no durability semantics.

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
make bench                          # quick mode, 1 worker
make bench-full                     # full mode, 4 workers
make bench-sweep                    # sweep workers=1,2,4 × clients=1,2,4
make bench-perf                     # full + perf record + perf stat

# Knobs: WORKERS, CLIENTS, FULL=1, PERF=1
make bench WORKERS=4 PERF=1        # quick, 4 workers, perf
```

Results: `benchmarks/results/` (gitignored). The runner rotates old results (keeps 3).

Workflow: `make bench` → change → commit → `make bench` → compare `summary.json`.

## Debugging failures

1. **Log first, never guess.** Rebuilds are expensive. One well-instrumented
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

## Claude Code memory policy

Never save code-specific details (function names, file paths, invariants,
fix recipes) to Claude Code memory. Code changes; memories become stale
and cause more problems than they solve. If something matters, put it in
the code (comments, doc strings, assertions) or in this guide.
