# GnitzDB Developer Guide

## Multi-worker invariant checklist

When adding a new server-side constraint, validation rule, or invariant:

- [ ] Is it enforced in `engine.py` / `registry.py` for the master DDL path?
- [ ] Is it in `validate_fk_inline` (single-process) or `validate_fk_distributed`
      (multi-worker)? FK and partition-global constraints belong at the request layer,
      NOT inside `ingest_to_family`.
- [ ] Does it require distributed coordination (like FK validation)?
      If so, extend `validate_fk_distributed` / `check_fk_batch`.
- [ ] Does it need to fire on every worker even for empty batches
      (e.g. exchange barrier participation)?
- [ ] Any change touching `worker.py`, `registry.py` ingest path, or `ipc.py`:
      run `make e2e`.
- [ ] Is there a multi-worker E2E test in `rust_client/gnitz-py/tests/test_workers.py`
      covering it?
- [ ] Is there a unit test in `multicore_test.py` covering the IPC path?
- [ ] Does the feature use `hash_row_by_columns` for exchange routing across
      tables where the same logical value may have different column types
      (e.g., PK column = U64 vs regular column = I64)? The hash must produce
      identical partitions regardless of type for co-partitioning correctness.

## Running E2E tests

**Always run the E2E suite with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss bugs in distributed execution.

```bash
cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v
```

The Makefile `e2e` target already sets `GNITZ_WORKERS=4`. Never run
`uv run pytest tests/` without the `GNITZ_WORKERS` variable set.

## Debug logging during development

To enable verbose logging during E2E tests:

```bash
GNITZ_WORKERS=4 GNITZ_LOG_LEVEL=debug uv run pytest tests/test_joins.py -x
```

Master log: `~/git/gnitz/tmp/server_debug.log`
Worker logs (always preserved from last run): `~/git/gnitz/tmp/last_worker_N.log`

Log format: `<epoch>.<ms> <process> <level> <message>`
- Process tags: `M` (master), `W0`-`WN` (workers)

To add temporary debug logging in RPython code, use `log.debug()` —
never raw `os.write(2, ...)`. This ensures timestamps, process identity,
and level-gating. Remove debug calls before committing.

To watch all worker logs live:

```bash
tail -f ~/git/gnitz/tmp/gnitz_py_*/data/worker_*.log
```

## IPC optimization checklist

When adding any optimization that modifies the IPC message flow (new flags,
pre-routing, stashing, skipping round-trips):

- [ ] State the optimization's preconditions explicitly in the plan doc. For every
      column value the optimization reads (for routing, key lookup, etc.): does the
      client guarantee that value is correctly filled for *all* message types, including
      DELETE/retraction rows?
- [ ] Is there an E2E test that pushes rows *and then retracts them* through this path?
      Retraction tests (delete + re-query) are mandatory for any new IPC fast-path.
- [ ] Did you test each IPC sub-phase in isolation before combining? E.g. for a 4-step
      optimization: run tests after steps 1-2 before writing steps 3-4.
- [ ] Add a comment to any guard/fallback code naming the invariant it protects.

## Rust static lib rebuild rule

Individual test targets (`make run-<name>-c`) do **not** rebuild the Rust
static libraries. Only the top-level `make test` has `rust-engine-debug` as
a prerequisite. After changing any Rust code in `rust_client/gnitz-engine/`
or `rust_client/gnitz-transport/`, always rebuild before running RPython
tests:

```bash
make rust-engine-debug          # after gnitz-engine changes
make rust-transport-debug       # after gnitz-transport changes
```

Forgetting this causes linker errors (`undefined reference`) for new symbols,
or silent use of stale code for changed symbols — the RPython binary links
the old `.a` and passes or fails for the wrong reasons.

## Benchmarking

### Binaries

Three server binaries, built via Makefile targets:

| Binary | Make target | Flags | Use |
|--------|-------------|-------|-----|
| `gnitz-server-c` | `make server` | `--opt=1 --gc=incminimark --lldebug` | Dev/debug (has debug alloc) |
| `gnitz-server-release-c` | `make release-server` | `--opt=jit --gc=incminimark --lto` | JIT release profiling |
| `gnitz-server-nojit-c` | `make release-server-nojit` | `--opt=2 --gc=incminimark --lto` | Static-opt baseline |

Rebuild release binaries before benchmarking if source has changed:

```bash
make release-server            # ~3 min
make release-server-nojit      # ~2 min
```

### Benchmark scripts

All scripts run from the repo root. They start a server, run a workload via
`uv run python -c ...` (so the gnitz client is importable), then shut down.

```bash
# CPU profile with perf (requires perf, paranoid ≤ 1)
python scripts/perf_profile.py --ticks 5 --rows 50000

# Realistic multi-client latency profile
python scripts/perf_profile.py --realistic --ticks 5 --rows 5000 --clients 4

# JIT vs non-JIT comparison
python scripts/compare_jit.py --ticks 5 --rows 50000

# JIT vs non-JIT with realistic workload
python scripts/compare_jit.py --realistic --ticks 5 --rows 10000 --clients 4
```

### Benchmark history

Every benchmark run automatically appends a record to `bench_history/history.jsonl`
(gitignored), keyed by commit hash and timestamp.

```bash
python scripts/bench_history.py                          # list all runs
python scripts/bench_history.py --last 10                # last 10
python scripts/bench_history.py --commit abc1234         # filter by commit
python scripts/bench_history.py --compare abc1234 def5678  # side-by-side
python scripts/bench_history.py --trend                  # throughput over time
```

### Typical workflow

1. Rebuild binaries: `make release-server && make release-server-nojit`
2. Run baselines (throughput + realistic) on the current commit
3. Make changes, commit
4. Rebuild and re-run
5. `python scripts/bench_history.py --compare <old> <new>` to see the delta

## Resource ownership: the close-chain rule

RPython has no RAII, no release-on-scope-exit, and no reliable finalizers.
The only defense against fd/mmap/buffer leaks is an explicit close chain.

**Invariant:** Every class that holds a closeable sub-resource (cursor, fd,
mmap, Buffer) MUST have a `close()` method, and its parent's `close()` MUST
call it — all the way up to `Engine.close()`.

When adding a field that stores a cursor, fd, or any resource with `close()`:

- [ ] Does the containing class have `close()`?
- [ ] Does `close()` close the new sub-resource?
- [ ] Does the parent class's `close()` call this class's `close()`?
- [ ] Trace the chain up to `Engine.close()` — is every link present?

Cache eviction counts as a close site too: if a cache (`ProgramCache`,
`_cache dict`) drops an entry, it must call `close()` on the evicted value
before deleting it.

Process exit is not a substitute for `close()`. Tests that run multiple
engine lifecycles in one process will expose missing links.

## Debugging multi-worker failures

When a multi-worker test fails and the root cause is not immediately obvious:

1. **Add logging before re-running.** If you don't know exactly what value is wrong
   and where, you don't have enough information to re-run yet. Add targeted `log.debug()`
   calls at the send side and receive side of the suspected path — at minimum log the
   key column values and view_id. One instrumented run is worth more than ten blind
   re-runs.
2. **Isolate with 1 worker.** If the test passes with `GNITZ_WORKERS=1` and fails with
   `GNITZ_WORKERS=4`, the bug is in the exchange/fanout/stash path, not in computation.
3. **Bisect by sub-path.** Add a flag or env var to disable the new fast-path and force
   the old path. If the test passes with the old path, the bug is in the new path only.
