# GnitzDB Developer Guide

## Multi-worker invariant checklist

When adding a new server-side constraint, validation rule, or invariant:

- [ ] Is it enforced in `catalog.rs` for the master DDL path?
- [ ] Is it in `catalog_validate_fk_inline` (single-process) or
      `validate_unique_distributed` in `master.rs` (multi-worker)?
      FK and partition-global constraints belong at the request layer.
- [ ] Does it need to fire on every worker even for empty batches
      (e.g. exchange barrier participation)?
- [ ] Any change touching `worker.rs`, `master.rs`, `ipc.rs`, or `executor.rs`:
      run `make e2e`.
- [ ] Is there a multi-worker E2E test in `crates/gnitz-py/tests/test_workers.py`
      covering it?
- [ ] Is there a Rust unit test in the relevant module?
- [ ] Does the feature use `hash_row_by_columns` for exchange routing across
      tables where the same logical value may have different column types
      (e.g., PK column = U64 vs regular column = I64)? The hash must produce
      identical partitions regardless of type for co-partitioning correctness.

## Running E2E tests

**Always run the E2E suite with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss bugs in distributed execution.

```bash
cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v
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

For temporary debug logging:
- Rust: `gnitz_debug!` / `gnitz_info!` macros (in `log.rs`)
These ensure timestamps, process identity, and level-gating.
Remove debug calls before committing.

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

## IPC flag constants

IPC group-header flags are defined once in `ipc.rs` as `pub const FLAG_*: u32`.
Both `worker.rs` and `master.rs` import from there. When adding a new flag:

- [ ] Add to `ipc.rs`
- [ ] Handle in `worker.rs` dispatch loop
- [ ] Handle in `master.rs` if it requires master-side logic

## Consolidation and merge correctness

Any code that merges or consolidates Z-Set batches must respect the
invariants in `foundations.md`. The critical rule:

> **All merge/consolidation paths must produce output sorted by
> (PK, payload), not just PK.**

When adding or modifying a merge path (tournament tree, sort, compaction):

- [ ] Does the comparison function include payload columns after PK?
      Both `sift_down` / `sift_up` in the heap AND any sequential
      pending-group accumulation depend on (PK, payload) adjacency.
- [ ] Does the path handle within-cursor duplicates? MemTable runs are
      sorted but NOT consolidated — the same (PK, payload) can appear
      multiple times in one run (e.g., +1 then -1 for the same key).
- [ ] Does `compare_rows` respect column order, null handling (null < non-null),
      and sign-extension?
- [ ] Test with entries that share the same PK but differ in payload
      columns. This pattern arises in: reduce output (retraction of old
      aggregate + insertion of new), non-linear aggregates (MIN/MAX)
      with secondary index tables, and any Z-Set where multiple logical
      values share a key. PK-only ordering silently breaks consolidation
      for these cases.
- [ ] Is there a Rust unit test with **same-PK, different-payload**
      entries across multiple input sources? This is the pattern that
      PK-only ordering silently breaks. Test with ≥3 inputs so entries
      span multiple heap rounds (existing: `test_compact_same_pk_*`,
      `test_compact_10_tick_*` in `compact.rs`).
- [ ] For view stores: does the test cover the flush-between-ticks
      scenario? `evaluate_dag` calls `view_family.store.flush()` after
      every tick, creating one shard per non-empty partition per tick.
      After L0_COMPACT_THRESHOLD (4) ticks, compaction merges these
      shards — the exact path where the heap ordering matters.
- [ ] Are memtable runs actually sorted? `merge_batches` assumes each
      run is sorted by (PK, payload). If a run is unsorted, entries
      appear out of order and the pending-group algorithm silently
      produces wrong weights. `upsert_and_maybe_flush` defensively
      re-sorts as a safety net, but the caller should also ensure the
      batch is sorted before ingestion.

## Exchange schema contract

The exchange (OPCODE_EXCHANGE_SHARD) serializes batches using the
**pre-plan's `out_schema`** attribute on `ExecutablePlan`. This schema
determines column sizes during batch packing/unpacking.

> **The pre-plan's `out_schema` MUST match the physical layout of the
> batch at the exchange output register.** It is NOT the view's final
> output schema — it is the intermediate schema at the exchange point.

For a view `INPUT → FILTER → EXCHANGE → REDUCE → INTEGRATE`:
- Pre-plan out_schema = FILTER output schema (= input table schema)
- Post-plan out_schema = view output schema (= REDUCE output schema)

If the pre-plan's `out_schema` is set to the view's final schema instead
of the exchange intermediate schema, STRING columns (16 bytes) may be
serialized as I64 (8 bytes), causing silent data corruption at workers.

## Aggregate output type contract

The REDUCE output schema column type rule is simple:

- COUNT, COUNT_NON_NULL → **I64** (always)
- SUM/MIN/MAX on F32/F64 → **F64**
- SUM/MIN/MAX on anything else → **I64** (including STRING, I32, U32, etc.)

The accumulator stores values as `u64` bit patterns (8 bytes). The output
column is ALWAYS 8 bytes. MIN/MAX on STRING stores the German String
compare key (first 8 bytes), not the full 16-byte string struct. Any Rust
code that builds REDUCE output schemas must follow this rule exactly.

## Benchmarking

### Binaries

| Binary | Make target | Use |
|--------|-------------|-----|
| `gnitz-server` | `make server` | Dev/debug build |
| `gnitz-server-release` | `make release-server` | Release profiling |

Rebuild before benchmarking if source has changed:

```bash
make release-server
```

### Benchmark scripts

All scripts run from the repo root. They start a server, run a workload via
`uv run python -c ...` (so the gnitz client is importable), then shut down.

```bash
# Benchmark history
python scripts/bench_history.py --last 10
python scripts/bench_history.py --compare <old_commit> <new_commit>
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

1. Rebuild binary: `make release-server`
2. Run baselines (throughput + realistic) on the current commit
3. Make changes, commit
4. Rebuild and re-run
5. `python scripts/bench_history.py --compare <old> <new>` to see the delta

## Debugging failures

When any test fails (single-worker or multi-worker) and the root cause is
not immediately obvious:

1. **Add COMPREHENSIVE logging before re-running — never guess.** If you
   don't know exactly what value is wrong and where, you don't have enough
   information to re-run yet. Each rebuild cycle costs ~50s. A guess that's
   wrong wastes that entire cycle. One well-instrumented run reveals more
   than ten speculative fix attempts.
   - Log EVERY key variable at EVERY decision point in the suspected path.
   - Log full data structures (HashMaps, Vecs, schemas), not just single values.
   - For batch/schema mismatches: log `col_data.len()` per column, `count`,
     `blob.len()`, AND the schema's column types/sizes. Compare them.
   - Rust: `gnitz_debug!` / `gnitz_info!` (structured, goes to worker/master log)
   - Rust: `eprintln!` (stderr, goes to worker/master log depending on process)
   - `GNITZ_LOG_LEVEL=debug` required for debug-level messages
   - For panics: use `RUST_BACKTRACE=1` to get the full call chain.
2. **Isolate with 1 worker first.** If the test passes with `GNITZ_WORKERS=1`
   and fails with `GNITZ_WORKERS=4`, the bug is in the exchange/fanout/stash
   path, not in computation. Common exchange bugs:
   - Schema mismatch: pre-plan `out_schema` != actual batch layout
     (see "Exchange schema contract" above).
   - ext_trace_regs assigned to the wrong plan (pre vs post).
3. **Bisect by sub-path.** Add a flag or env var to disable the new fast-path
   and force the old path. If the test passes with the old path, the bug is
   in the new path only.
4. **Verify your fix is in the binary.** Run `make server` before testing.
   Cargo tracks source changes automatically, but always confirm the binary
   was rebuilt after your changes.
