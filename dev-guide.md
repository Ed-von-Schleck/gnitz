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
- [ ] Is there a Rust unit test in `ipc.rs` / `exchange.rs` covering the IPC path?
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
- [ ] Is `compare_rows` identical between RPython and Rust? Column order,
      null handling (null < non-null), and sign-extension must match.
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
      run is sorted by (PK, payload). If a run is unsorted (e.g., due
      to a stale `sorted` flag on the Python/Rust boundary), entries
      appear out of order and the pending-group algorithm silently
      produces wrong weights. The Rust `upsert_and_maybe_flush`
      defensively re-sorts as a safety net, but the caller should also
      ensure the batch is sorted before ingestion.

## FFI handle type safety

RPython wraps two distinct Rust handle types for table storage:

- `EphemeralTable._handle` → `*mut Table` (Rust `Table` struct)
- `PartitionedTable._handle` → `*mut PartitionedTable` (Rust `PartitionedTable` struct)

Both are stored as `rffi.VOIDP` in Python. **They are NOT interchangeable.**
Casting a `PartitionedTable*` to `Table*` and dereferencing it reads garbage
(different struct layout). Any Rust FFI function that expects a `Table*` will
corrupt memory if given a `PartitionedTable*`.

When writing FFI code that operates on "the view's store":

- [ ] Does the code distinguish `EphemeralTable` from `PartitionedTable`?
      Use `isinstance()` on the Python side, or pass a type tag.
- [ ] If you need a `Table*` from a `PartitionedTable`, use
      `gnitz_ptable_create_child` or `gnitz_ptable_get_child_dir` — never
      cast the PartitionedTable handle to `Table*`.
- [ ] For child table creation in multi-worker mode, each worker's
      `PartitionedTable` has different partition directories. Use
      `PartitionedTable.get_child_base_dir()` (→ partition 0's directory)
      to get a worker-unique base path. Using `TableFamily.directory`
      gives the SAME path for all workers, causing data corruption.

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

`UniversalAccumulator.output_column_type()` (RPython) determines the
column type in the REDUCE output schema. The rule is simple:

- COUNT, COUNT_NON_NULL → **I64** (always)
- SUM/MIN/MAX on F32/F64 → **F64**
- SUM/MIN/MAX on anything else → **I64** (including STRING, I32, U32, etc.)

The accumulator stores values as `u64` bit patterns (8 bytes). The output
column is ALWAYS 8 bytes. MIN/MAX on STRING stores the German String
compare key (first 8 bytes), not the full 16-byte string struct. Any Rust
code that builds REDUCE output schemas must follow this rule exactly.

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

## RPython server rebuild for E2E tests

`make server` is a `.PHONY` target that depends only on the Rust lib targets.
**It does NOT track changes to Python source files.** RPython caches its
intermediate C files in `/tmp/usession-main-*`. If the generated C is
up-to-date (by RPython's internal hashing), `make server` only re-links
(~55s) without re-translating (~25s extra for annotate + rtype).

After changing any `.py` file under `gnitz/`, force a full re-translation:

```bash
rm -rf /tmp/usession-main-* && rm -f gnitz-server-c && make server
```

**How to verify the rebuild included your changes:** look for `annotate`
and `rtype_lltype` in the build output. If you only see `database_c`,
`source_c`, `compile_c` — translation was skipped and the old Python
code is still baked in.

## RPython–Rust flag synchronization

`ArenaZSetBatch` (Python) wraps a Rust `OwnedBatch`. Both have `sorted`
and `consolidated` flags. These can desync:

- `ArenaZSetBatch._invalidate_cache()` sets Python `_sorted = False` but
  does NOT call `_batch_set_sorted` on the Rust side.
- `OwnedBatch::append_batch` / `append_batch_negated` clear Rust
  `sorted` and `consolidated`, but the Python wrapper's `_invalidate_cache`
  is what the caller relies on.
- `BatchWriter.mark_sorted(value)` delegates to `ArenaZSetBatch.mark_sorted`
  which syncs both sides. Always use `mark_sorted` / `mark_consolidated`
  for explicit flag changes — never assign `_sorted` directly.

The Rust `Table::upsert_and_maybe_flush` defensively re-sorts batches
whose `sorted` flag is false (batches from `OwnedBatch::from_regions`
always start with `sorted = false`). This catches any desync from the
Python side.

When adding new Rust-side batch mutation functions (append, copy, filter):

- [ ] Does the function clear `self.sorted` and `self.consolidated`?
- [ ] Is there a corresponding Python-side `_invalidate_cache()` call?

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

## German String blob passthrough contract

Any `RowAccessor` subclass that reads from a Rust-backed blob arena
(OwnedBatch, ReadCursor, MappedShard) **must** override `get_blob_source()`
to return `(blob_ptr, blob_len)`. Without this, `append_from_accessor` passes
NULL as the blob source to `gnitz_batch_append_row`, and long strings
(> 12 bytes) are silently zeroed — no error, no assertion, just corrupt data
on the wire.

The failure mode is insidious: short strings (≤ 12 bytes) inline their data
and work fine. Only strings exceeding `SHORT_STRING_THRESHOLD` (12) need the
blob arena. System table column names like `"sql_definition"` (14 bytes) or
`"cache_directory"` (15 bytes) are the typical first casualties.

When adding a new `RowAccessor` subclass backed by Rust storage:

- [ ] Does `get_str_struct()` return a non-NULL `heap_ptr` (4th element)?
      If yes, `get_blob_source()` **must** be overridden.
- [ ] Does the override return the correct `(ptr, len)` for the backing
      blob arena? The ptr must remain valid until `append_from_accessor`
      returns (no reallocation between `get_blob_source()` and the FFI call).
- [ ] Test with a table that has a STRING column containing a value > 12
      bytes, then SCAN the table back. If the blob passthrough is missing,
      the scan result will have empty blobs and the client decode will fail
      with "German String blob arena out of bounds".

Current implementations: `RustCursorAccessor` (cursor.py),
`ColumnarBatchAccessor` (batch.py — uses dedicated fast-path, not
`get_blob_source`). Known gap: `PTableFoundAccessor` and
`EphemeralTableFoundAccessor` do not yet override `get_blob_source()`;
these are used only in `retract_pk`, which currently only retracts by PK
lookup — but will break if a retracted row contains a long string.

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
   - For cross-boundary bugs (RPython ↔ Rust FFI): add `eprintln!` on the
     Rust side AND `log.warn()` on the Python side. Know where each goes:
     - `eprintln!` in the server process → worker log files
       (`~/git/gnitz/tmp/gnitz_py_*/data/worker_N.log`)
     - `eprintln!` in the master process → `~/git/gnitz/tmp/server_debug.log`
       (only if the test harness redirects stderr there)
     - `log.debug()`/`log.warn()` in RPython → worker or master log file
       (requires `GNITZ_LOG_LEVEL=debug` for debug-level messages)
   - For panics: use `RUST_BACKTRACE=1` to get the full call chain.
2. **Isolate with 1 worker first.** If the test passes with `GNITZ_WORKERS=1`
   and fails with `GNITZ_WORKERS=4`, the bug is in the exchange/fanout/stash
   path, not in computation. Common exchange bugs:
   - Schema mismatch: pre-plan `out_schema` != actual batch layout
     (see "Exchange schema contract" above).
   - Child table directory collision: all workers creating tables at the
     same path (see "FFI handle type safety" above).
   - ext_trace_regs assigned to the wrong plan (pre vs post).
3. **Bisect by sub-path.** Add a flag or env var to disable the new fast-path
   and force the old path. If the test passes with the old path, the bug is
   in the new path only.
4. **Verify your fix is in the binary.** See "RPython server rebuild" below.
   If the build output does not show `annotate` and `rtype_lltype` timer
   lines, the RPython translation was skipped and your Python changes are
   NOT in the binary. This is the single most common reason a "fix" appears
   to not work.
