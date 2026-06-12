# Reduce secondary-index maintenance: where the cost really is

## Status

Investigation complete. Conclusion: the "flush cost" is **not** an optimization
lever — the LSM flush path is beneficial in the real workload. The one concrete,
validated lever found is **shrinking the GroupIndex arena** (1 MiB → 256 KiB).
This doc records the measurements so the flush angle is not re-chased.

## How this started

A single-batch population microbench (`ops/bench_secondary_index.rs`,
`*_decomposition`) attributed ~37% of secondary-index population to the memtable
"upsert" layer, which includes the flush. That looked like an adjacent lever
("index tables flush too eagerly into tiny 256 KiB–1 MiB arenas"). It is not —
the single-batch shape is unrepresentative: it ingests 500k rows in one call
(one big, efficient flush) and never reads back. Production does the opposite:
many small per-epoch deltas, and a read-back every epoch.

## What production actually does

- GI / AVI / trace tables are created **once per circuit** and live for the
  view's lifetime, accumulating the integrated trace across all epochs
  (`compiler.rs`: GI via `Table::new(.., 1024*1024, false)` at ~1374; AVI/trace
  via `create_child_table` = `256*1024`, `compiler.rs:840`). All `durable=false`.
- Each epoch: INTEGRATE writes that epoch's small delta into the index
  (`ingest_owned_batch_memonly`), flushing an ephemeral shard when the memtable
  passes 75% of the arena (`memtable.rs:130`).
- Each epoch: `op_reduce` reads the index back. The GI/AVI cursors are built
  **fresh every epoch** via `create_cursor_compacting()` (`vm.rs:986`/`997`) —
  they are *not* cached by `refresh_owned_cursors` (that path only handles the
  trace-input cursor). The read seeks each touched group's prefix
  (`op_reduce.rs:336-369`: `seek_first_positive_with_prefix` + `advance` +
  `walk_to_positive_with_prefix`). A read past `L0_COMPACT_THRESHOLD = 4` shards
  triggers a compaction (`shard_index.rs:45`).

## Measurements

Incremental bench `secondary_index_bench_gi_incremental` (one long-lived GI
table, 500k rows total, read-back every epoch; `GNITZ_BENCH_ARENA_KB` /
`GNITZ_BENCH_DELTA` configurable). Release, numbers stable to ~3% across reps on
a loaded machine.

Per-row cost vs arena size (delta = 100 rows/epoch):

| arena | write (integrate) | read: cursor open | read: prefix seeks | total |
|---|---|---|---|---|
| 256 KiB | 210 (11%) | 460 (24%) | 1200 (64%) | ~1810 |
| 1 MiB (shipped GI) | 390 (16%) | 248 (10%) | 1900 (74%) | ~2400 |
| 64 MiB | 6600 (87%) | 5 (0%) | 1000 (13%) | ~7600 |

Arena × delta sweep (total ns/row):

| arena \ delta | 100 | 1000 |
|---|---|---|
| 256 KiB | 1814 | 1673 |
| 1 MiB | 2394 | 2204 |
| 4 MiB | 2863 | 2080 |

## Findings

1. **Flush is not a lever.** Making flushes rarer by enlarging the arena makes
   things *dramatically worse*, not better. The LSM flush-to-sorted-shards path
   keeps reads as binary searches over compacted shards; a small memtable is
   good. Do not pursue "reduce flush frequency" / "keep index in memory".

2. **Incremental index maintenance is read-back-dominated** (~88% at production
   arenas), and the read is dominated by **per-epoch prefix seeks** (64–74%).
   Those seeks are largely inherent IVM recompute (every changed group must be
   re-read to recompute its aggregate) — not obviously reducible without an
   algorithmic change to op_reduce. Cursor construction is a secondary 10–24%
   and scales with shard count.

3. **Enlarging the arena is actively harmful.** At 64 MiB the write cost
   explodes ~30× (6600 ns/row) because the memtable never flushes and
   `maybe_inline_consolidate` repeatedly re-sorts an ever-larger memtable. The
   naive "avoid flushes" intuition is exactly backwards.

4. **Concrete lever — GI arena is too large.** GI ships with a 1 MiB arena
   while AVI/trace use 256 KiB. Across both delta sizes, 256 KiB is ~25–30%
   faster end-to-end for GI maintenance (fewer/smaller memtable runs to
   merge-search on every per-epoch seek; cheaper inline consolidation), with
   *lower* memory use. Larger arenas are monotonically worse here.

## Proposed change

In `compiler.rs` (~1374), change the GI table arena from `1024 * 1024` to
`256 * 1024` (matching AVI/trace via `create_child_table`).

- Risk: low. Arena size is only a flush threshold; correctness is unaffected
  (a smaller arena flushes more often → more ephemeral shards → more frequent
  L0→L1 compaction, all already-exercised paths). Memory drops.
- Open question: why GI got 1 MiB originally (GI is denser than AVI — many
  (group, source_pk) rows per group vs one (group, agg) per group — so it may
  have been sized to hold more before flushing). The measurements say that
  rationale is counterproductive for the read-heavy reduce loop, but confirm
  there is no separate write-heavy path that benefits from the larger arena.

## Validation plan

1. Apply the one-line arena change.
2. Re-run `secondary_index_bench_gi_incremental` at the default (now 256 KiB
   equivalent) and confirm the ~25% improvement; sweep `GNITZ_BENCH_DELTA`
   (100, 1000, 10000) and a high-cardinality vs low-cardinality `N_GROUPS` to
   confirm the win is not workload-specific (the 100/1000 sweep already agrees).
3. Full `cargo test -p gnitz-engine` + the reduce GI e2e suites green.
4. An end-to-end view-maintenance benchmark (not just the microbench) to
   confirm the win survives the full op_reduce path, since the microbench
   approximates the read pattern rather than running the real recompute.

## Out of scope / non-levers (recorded so they are not re-investigated)

- Reducing flush frequency, larger index arenas, keeping the index in memory —
  all measured to be harmful (findings 1, 3).
- The per-epoch prefix-seek cost (finding 2) is the largest single component but
  is inherent IVM recompute; attacking it means changing op_reduce's
  recompute-per-changed-group strategy, a much larger effort than arena tuning.
