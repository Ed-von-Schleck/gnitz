# Microbenchmarks for the RAM-tier flush/compaction hot path

## Problem

The `test_view_maintenance` e2e profile (W=4, full scale, 125k rows/s) spends
the majority of worker cycles inside `Table::flush` machinery. Worker
userspace breakdown:

| % cycles | symbol |
|---|---|
| 14.9% | `storage::repr::merge::run_merge_body::{{closure}}` |
| 11.0% | `storage::lsm::memtable::runs::bloom_add_batch` |
| 10.6% | `storage::repr::batch::write_to_batch` (self) |
| 6.1%  | `storage::repr::scatter::scatter_unified_sources_with_weights` |
| 5.5%  | `__memmove_avx512_unaligned_erms` |
| 5.4%  | `__memset_avx512_unaligned_erms` |
| 2.6%  | `_int_malloc` |
| 2.1%  | `storage::lsm::shard_file::write_shard_streaming` |

All of the top symbols funnel through one call chain:
`DagEngine::evaluate_dag_multi_worker` → `flush_view_or_abort`
(`query/dag/mod.rs:1145`) → `Table::flush` (`table/flush.rs:34`) →
`flush_prepare` (`table/flush.rs:163`) → `flush_to_ram` (`table/flush.rs:105`)
→ `fold_memtable_into_l0` + `enforce_inmem_bound` → `compact_in_memory`
(`table/flush.rs:357`). The perf caller edges `run_merge_body::{{closure}}` ←
`write_to_batch` and `scatter_unified_sources_with_weights` ← `write_to_batch`
are inlining artifacts: `consolidate_batches` (`memtable/runs.rs:14`) calls
`write_to_batch(schema, total, blob, |writer| merge_batches(batches, schema,
writer))`, and `merge_batches` (phase 1 `run_merge` → survivor list, phase 2
`scatter_unified_sources_with_weights`) is inlined into the `write_fn` closure.

## Why these symbols dominate

Three stacked mechanisms, verified in source:

1. **Unconditional per-tick view-store flush.** After every epoch,
   `drive_dag` flushes each dirty view once; `flush_prepare` for every
   non-`SalReplay` table (view output stores, index circuits) calls
   `flush_to_ram()` with no size gate (`table/flush.rs:163-170`), across all
   partitions of the view's `PartitionedTable` (`partitioned_table.rs:332`;
   untouched partitions early-out via `memtable.is_empty()`).

2. **Flatten-to-one-run RAM-tier compaction.** `enforce_inmem_bound`
   (`table/flush.rs:345`) fires `compact_in_memory` whenever
   `in_memory_l0.len() > INMEM_COMPACT_THRESHOLD` (= 4, `table/mod.rs:28`).
   `compact_in_memory` merges the **entire** run set into one run — there is
   no tiering. Each per-tick fold pushes one small run; every ~5 ticks the
   whole accumulated window (bounded only by `INMEM_CEILING` = 4 MiB per
   partition, `table/mod.rs:36`) is re-merged (`run_merge`), re-materialized
   (`write_to_batch` + scatter), and re-bloomed (`InMemRun::from_batch` →
   `BloomFilter::new` + `bloom_add_batch` over **all** surviving rows,
   `table/mod.rs:129-133`). For per-tick delta `d` and window of `W` rows, the
   bytes moved to grow the window from 0 to `W` are Θ(W²/d) — quadratic within
   each spill window, repeated per window. This is a **call-volume ×
   O(window)** problem: the kernels are hot because they are fed the same
   accumulated rows over and over, not (only) because they are slow.

3. **Per-call kernel costs multiplied by that volume:**
   - `write_to_batch` zero-fills the full arena on every call
     (`batch.rs:1675-1684`: pooled buffers are recycled at `len == 0` with
     dirty contents, so `buf.resize(arena_size, 0)` memsets `max_rows ×
     row_stride` bytes; the fallback `alloc_large_zeroed` likewise). Live rows
     are then immediately overwritten by the writer — double-write — and the
     dead tail (`[actual_rows, max_rows)`) is never read. Primary memset
     source.
   - `bloom_add_batch` (`memtable/runs.rs:55`) is 7 scattered bit-writes per
     key (`bloom.rs:27-37`) plus a fresh `vec![0u8; …]` filter allocation per
     rebuild; the per-run bloom is rebuilt from scratch over the whole merged
     run at every fold.
   - `run_merge_body`/`drive_merge` (`heap.rs:263-300`) pays an O(log K)
     loser-tree sift per row plus tied-PK payload compares.
   - `scatter_unified_sources_with_weights` (`scatter.rs:354`) gathers each
     row from K independent arenas; `gather_unified_col` (`scatter.rs:503`)
     has no prefetch (unlike single-source `gather_col`, `scatter.rs:174`),
     and non-{1,2,4,8,16} strides (compound PKs) fall back to runtime-width
     `copy_nonoverlapping` = a real per-row memcpy call.

The workload shape maximizes the amplification: per benchmark iteration, one
4000-row INSERT plus 15 single-row UPDATE/DELETE statements, each its own
epoch, propagating through 5 views × 256 hash partitions — per-partition
per-tick deltas of ~1–16 rows repeatedly pay O(window) merges. That shape is
representative of real incremental view maintenance, not a benchmark artifact.

## Why microbenchmarks, and the coverage gap

`make bench-full` view_maintenance has multi-percent run-to-run variance and
cannot resolve kernel-level or policy-level wins; the compaction-policy and
kernel changes this profile motivates need deterministic, isolated
instruments with amplification counters, plus codegen-verifiable inner loops.

Existing `#[ignore]`d benches (all `std::time::Instant` + `black_box`, run via
`cargo test -p gnitz-engine --release <name> -- --ignored --nocapture
--test-threads=1`):

- `run_merge_dup_pk_bench` (`merge.rs:846`) — loser-tree compare loop only,
  balanced K=4 equal-size sources, cheap emit. No materialization, no skew.
- `materialize_row_vs_column_bench` (`merge.rs:925`) — scatter +
  `write_to_batch` over a precomputed survivor stream, balanced K=4. No
  merge, no arena-size axis, no sparse-survivor axis.
- `secondary_index_bench_*` (`ops/bench_secondary_index.rs`) — memtable
  upsert path, not the RAM-tier fold.

Nothing covers: the `compact_in_memory` composition at its real skewed run
shape, the tick-cadence amplification itself, `bloom_add_batch`, or
`write_to_batch`'s arena provisioning as a function of `max_rows`. These four
benches close exactly those gaps. Existing benches are not modified.

## Deliverables

Four new `#[ignore = "benchmark; run with --release --ignored --nocapture
--test-threads=1"]` tests. Shared schema for all four (the view-output-store
shape of `v_rev`: one hidden U64 group key + two non-null I64 aggregates,
40 B/row, `FixedIntNonnull` payload comparator — the dominant shape in the
profiled workload):

```rust
let schema = SchemaDescriptor::new(
    &[
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 0),
    ],
    &[0],
);
```

### B1 — `flush_cadence_amplification_bench`

**Location:** new file `crates/gnitz-engine/src/storage/lsm/table/bench_flush.rs`,
registered in `table/mod.rs` as `#[cfg(test)] mod bench_flush;` (precedent:
`ops/mod.rs:12-13` for `bench_secondary_index`). As a child module of `table`
it can read `Table` internals directly.

**What it isolates:** the end-to-end per-tick flush policy cost — the
composition of `fold_memtable_into_l0` (memtable consolidation + per-run
bloom), `compact_in_memory` (window re-merge + re-materialize + re-bloom) and
`spill_in_memory_to_disk`, at production trigger thresholds. This is the
bench a compaction-policy change must move, and the one whose profile must
reproduce the e2e symbol shape (see Validation).

**Setup:** per config, a fresh `Table` in a fresh `tempfile::tempdir()`
subdirectory: `Table::new(dir, "bench", schema, id, 256 << 10,
RecoverySource::Rederive)` — 256 KiB arena matching the `Routing::Hashed`
per-partition arena (`partitioned_table.rs:66-70`); `Rederive` takes the same
`flush_prepare` branch as view output stores.

**Tick loop (timed):** all tick batches are pre-generated before the
`Instant` starts; the timed region is only

```rust
for batch in ticks {
    table.ingest_owned_batch(batch).unwrap();
    table.flush().unwrap(); // Rederive → flush_prepare → flush_to_ram
}
```

**Generators (pre-generation, untimed):** each tick batch is built with
`Batch::with_schema` + `extend_pk`/`extend_weight`/`extend_null_bmp`/`extend_col`
(the `merge.rs:798-809` pattern); `ingest_owned_batch` consolidates internally.

- `distinct(d)` — tick `t` emits `d` rows with fresh keys `t*d .. t*d+d`,
  weight +1: append-only window growth (the INSERT-stream shape).
- `churn(H, d)` — `d/2` updates per tick over a hot key set of size `H`: the
  bench keeps `last_payload: Vec<i64>` per key; for each update key `k`
  (round-robin `(t*d/2 + j) % H`) it emits `(k, last_payload[k], -1)` and
  `(k, new_payload, +1)`, then records the new payload. Steady-state net
  window ≈ `H` rows — the UPDATE/re-aggregation shape where the same rows are
  re-merged forever.

**Config matrix (fixed, one pass each; one small untimed warmup config first
to warm the thread-local batch pool):**

| label | generator | ticks | notes |
|---|---|---|---|
| `distinct_d1`    | distinct(1)        | 2000 | 1-row ticks, window → 2k rows |
| `distinct_d16`   | distinct(16)       | 2000 | window → 32k rows |
| `distinct_d4096` | distinct(4096)     | 200  | crosses the 4 MiB ceiling → exercises `spill_in_memory_to_disk` + `write_shard_streaming` (unsynced buffered IO) |
| `churn_h8192_d2` | churn(8192, 2)     | 2000 | 1 update/tick, steady window 8k rows |
| `churn_h8192_d32`| churn(8192, 32)    | 2000 | 16 updates/tick |
| `churn_h65536_d512` | churn(65536, 512) | 1000 | 256 updates/tick, steady window 64k rows (~2.6 MiB, no spill) |

**Counters:** after each `flush()`, if a compaction fired this tick (detected
as `runs_before_estimate > INMEM_COMPACT_THRESHOLD`, where
`runs_before_estimate` = `table.in_memory_runs().count()` sampled before the
flush + 1 for the incoming fold), accumulate `merged_out +=
table.in_memory_runs().map(|b| b.count).sum::<usize>()` — the merge-output
row count (merge input = output + cancelled rows; output is the stable,
directly observable proxy). `in_memory_runs()` is the existing pub(crate)
accessor (`table/mod.rs:446`).

**Report + sanity:**

```text
flush_cadence/{label}: {ingested} rows / {ticks} ticks in {secs:.3}s = {rps:.0} ingest-rows/s  merged-out {merged} rows  amp {merged/ingested:.1}x
```

Assert `merged_out > 0` for every config (compaction actually fired) and
`table.in_memory_runs().count() <= 4` at the end (bound invariant held).
`black_box` the counters.

### B2 — `merge_batches_skewed_bench`

**Location:** `merge.rs` `#[cfg(test)] mod tests`, beside
`run_merge_dup_pk_bench`.

**What it isolates:** one `compact_in_memory` unit of work at its real run
shape — the full `consolidate_batches` composition (`write_to_batch` arena +
`run_merge` + `scatter_unified_sources_with_weights`) over **1 big run + 4
small runs**, which no existing bench covers (both existing benches use
balanced K=4). The per-delta-row cost this reports *is* the amplification
factor's unit price.

**Data:** big run = `N` rows, keys `2i` (even), weight +1; small runs = `d`
rows each, keys odd and uniformly interleaved across the big range
(`(i * (N/d)) * 2 + 1` per small run, offset per run index) — uniform
hash-partition interleave, no (PK,payload) ties, so consolidation drops
nothing and row counts are exact. Built with the `bench_sorted_batch`-style
helper; sources wrapped exactly as at `merge.rs:860-862`
(`as_mem_batch` → `SortedMemBatch::new_unchecked`).

**Timed body** (mirrors `memtable/runs.rs:25-27`):

```rust
let b = write_to_batch(&schema, total_rows, 0, |w| {
    merge_batches(&sorted, &schema, w);
});
black_box(&b);
```

**Configs:** `(N, d, ITERS)` ∈ {(16_384, 16, 400), (65_536, 16, 100),
(262_144, 16, 25), (262_144, 4096, 25)}. Warmup iteration untimed.

**Report:**

```text
merge_skew/N{N}_d{d}: {total} rows  {rps:.0} merge-rows/s  {ns_per_delta:.0} ns/delta-row
```

where `ns_per_delta = secs * 1e9 / (ITERS * 4 * d)` — the cost charged to
each newly-arrived row for re-merging the window.

### B3 — `write_to_batch_arena_zeroing_bench`

**Location:** `merge.rs` `#[cfg(test)] mod tests` (reuses that module's
existing imports of `write_to_batch`, `scatter_unified_sources_with_weights`,
`mem_batch_to_unified`, `UnifiedSource`).

**What it isolates:** the arena provisioning cost of `write_to_batch`
(`batch.rs:1675-1684`) — the pooled-buffer `resize(arena_size, 0)` memset —
as a function of `max_rows`, separated from the scatter work, and the proof
that the cost scales with `max_rows` (pre-consolidation upper bound), not
with rows actually written.

**Data:** one source batch of `max_rows` rows (40 B/row schema above), one
`UnifiedSource` vec via `mem_batch_to_unified`, and two precomputed survivor
lists: `full` = all `max_rows` rows, `sparse` = every 64th row (survivor-poor
consolidation, the churn-cancellation shape).

**Arms, per arena size** `max_rows` ∈ {6_553 (~256 KiB), 26_214 (~1 MiB),
104_857 (~4 MiB)}, `ITERS` ∈ {2000, 500, 120}, each arm its own loop with one
untimed warmup call (steady-state pool):

- (a) `write_to_batch(&schema, max_rows, 0, |_w| {})` — pure
  acquire+zero+wrap.
- (b) same, `write_fn` = scatter of `full` survivors.
- (c) same `max_rows`, `write_fn` = scatter of `sparse` survivors — near-(a)
  timing is the direct evidence that provisioning is O(max_rows), not
  O(rows written).

**Report:**

```text
arena/{kib}KiB: provision {gbps:.1} GB/s ({us_a:.1} µs/call)  full-scatter {us_b:.1} µs/call  sparse {us_c:.1} µs/call  provision-share {100*us_a/us_b:.0}%
```

### B4 — `bloom_rebuild_bench`

**Location:** `memtable/mod.rs` `#[cfg(test)] mod tests`, beside
`memtable_find_positive_payload_row_bench`. `bloom_add_batch` is re-exported
at `memtable/mod.rs:18`; `BloomFilter` is imported there at line 11.

**What it isolates:** the per-key cost of the from-scratch per-run bloom
rebuild that `InMemRun::from_batch` (`table/mod.rs:129`) performs on every
fold — filter allocation (`vec![0u8; next_power_of_two]`) plus 7 scattered
bit-writes per key (`bloom.rs:27-37`) — across filter sizes that cross the
cache hierarchy (10 bits/key, power-of-two rounded: 1k keys ≈ 2 KiB … 512k
keys ≈ 1 MiB).

**Data:** per size `n` ∈ {1_000, 16_000, 128_000, 512_000}, one consolidated
batch of `n` distinct U64 PKs (same 40 B/row schema; only the PK region is
read).

**Arms, per size,** `ITERS` ∈ {4000, 400, 60, 15}:

- (a) rebuild composition — timed: `let mut bf = BloomFilter::new(n as u32);
  bloom_add_batch(&mut bf, &batch);` (exactly `InMemRun::from_batch`).
- (b) add-only — one filter created before the loop; per iteration
  `bf.reset()` **outside** the timed span, `bloom_add_batch` inside;
  isolates hashing+probe stores from the allocation/zeroing.

`black_box(&bf)` after each iteration.

**Report:**

```text
bloom/{n}: rebuild {ns_a:.1} ns/key  add-only {ns_b:.1} ns/key ({mkeys:.1} Mkeys/s)
```

## Validation

1. `make verify` (benches are `#[ignore]`d tests; they must compile under
   clippy-as-errors and not run in `make test`).
2. Run each bench:
   ```bash
   cd crates && cargo test -p gnitz-engine --release flush_cadence_amplification_bench -- --ignored --nocapture --test-threads=1
   cd crates && cargo test -p gnitz-engine --release merge_batches_skewed_bench       -- --ignored --nocapture --test-threads=1
   cd crates && cargo test -p gnitz-engine --release write_to_batch_arena_zeroing_bench -- --ignored --nocapture --test-threads=1
   cd crates && cargo test -p gnitz-engine --release bloom_rebuild_bench              -- --ignored --nocapture --test-threads=1
   ```
3. **Representativeness check for B1** — the point of the suite is that the
   cadence bench reproduces the e2e hot-symbol shape, so kernel/policy wins
   measured here transfer:
   ```bash
   cd crates && cargo test -p gnitz-engine --release --no-run
   perf record -g -o /tmp/bench_flush.perf.data -- \
     <the compiled test binary printed by --no-run> flush_cadence_amplification_bench --ignored --nocapture --test-threads=1
   perf report -i /tmp/bench_flush.perf.data --stdio --no-children --percent-limit 2
   ```
   Expected: `run_merge_body::{{closure}}`, `bloom_add_batch`,
   `write_to_batch`, `scatter_unified_sources_with_weights`, memset/memmove
   as the dominant symbols, mirroring the e2e worker profile. If the shape
   diverges materially (e.g. batch-generation or `into_consolidated`
   dominating), fix the bench (pre-generation, warmup) until it matches —
   the bench is wrong, not the profile.
4. Expected qualitative results recorded in the bench output (not asserted):
   B1 amp grows with window/delta ratio (`distinct_d1` ≫ `distinct_d4096`;
   `churn_h65536_d512` amp ≈ steady-window/delta ≈ 128×÷5); B3 arm (a) ≈ arm
   (c) and both O(max_rows); B4 rebuild > add-only by the filter-zeroing
   delta, ns/key rising once the filter leaves L2.

## Sequencing

- [ ] B2 `merge_batches_skewed_bench` + B3 `write_to_batch_arena_zeroing_bench` in `merge.rs` tests
- [ ] B4 `bloom_rebuild_bench` in `memtable/mod.rs` tests
- [ ] B1 `flush_cadence_amplification_bench` in new `table/bench_flush.rs` + module registration
- [ ] `make verify`; run all four benches in `--release`, record baseline numbers in the commit message
- [ ] perf-record B1 and confirm the hot-symbol shape matches the e2e worker profile (Validation step 3)
