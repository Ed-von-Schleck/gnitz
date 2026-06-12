# Performance plan: `INLINE_CONSOLIDATE_THRESHOLD`

## Verdict up front

Lowering `INLINE_CONSOLIDATE_THRESHOLD` from 16 to 8 is **not worth implementing**.
A from-scratch cost model of the actual consolidation strategy shows it is an
ingest-side *pessimization* (roughly 2× the rows re-merged, ~1.6× the comparator
calls), its only addressable hot function (`merge_batches`) is ~1.4% of worker
cycles in the current profile, the read-side payoff it promises is weak and
conditional, and none of the benchmarks named for validation actually drive a
memtable to a run count where the threshold even fires. The real cost is in the
read/lookup path, which this knob does not touch. This document records the
analysis so the experiment is not re-run, and points at the levers that would
actually move the number.

## Background (current code)

`MemTable` (`crates/gnitz-engine/src/storage/memtable.rs`) holds incoming
sorted batches as separate "runs" (`runs: Vec<Rc<Batch>>`). Each
`upsert_sorted_batch` appends exactly one run. PK lookups search every run
individually; a scan merges all runs lazily in a `ReadCursor`.

The constant:

```rust
// crates/gnitz-engine/src/storage/memtable.rs:18
const INLINE_CONSOLIDATE_THRESHOLD: usize = 16;
```

is used in exactly three places, all in `memtable.rs`:

- the definition (line 18),
- `runs: Vec::with_capacity(INLINE_CONSOLIDATE_THRESHOLD)` in `MemTable::new`
  (line 82) — the runs-vector preallocation is coupled to the constant, so
  changing it also changes the initial allocation (benign; the vec grows on
  demand), and
- the trigger in `maybe_inline_consolidate` (line 316):

```rust
// crates/gnitz-engine/src/storage/memtable.rs:315
fn maybe_inline_consolidate(&mut self) {
    if self.runs.len() >= INLINE_CONSOLIDATE_THRESHOLD {
        self.force_consolidate();
    }
}
```

**The consolidation strategy is a single-level major compaction, not a tiered
LSM.** `force_consolidate` (memtable.rs:153) merges *all* runs — including the
large already-consolidated survivor from the previous consolidation — into one,
via `runs_as_sorted()` over the whole `runs` vector and a full
`consolidate_batches` → `merge::merge_batches`. There are no levels; the prior
survivor is re-read and re-written on every subsequent trigger.

**The merge is a loser tree, not a binary min-heap.** `merge_batches`
(merge.rs:423) builds a `LoserTree` (heap.rs) and runs `drive_merge`. A loser
tree pays exactly **one** comparator call per tree level via `walk_up`
(heap.rs:127-145), explicitly contrasted in the module docs with the two
comparisons a binary heap's sift-down pays. The functions `sift_down_static`
and `collect_min_indices` do **not exist** anywhere in the tree (they were the
old min-heap implementation; `grep` returns zero hits). Tree height is
`⌈log₂(next_pow2(fan_in))⌉`; for the power-of-two fan-ins 8 and 16 that is 3 and
4 respectively.

**There is no `MemTable::get_snapshot` and no cached consolidation in the
memtable.** The read API is `snapshot_runs() -> &[Rc<Batch>]` (memtable.rs:147),
which hands the *raw* runs to a `ReadCursor`; cross-run merging is deferred to
the consumer and happens lazily during iteration (`ReadCursor::drain_sorted_into`,
read_cursor.rs). The only scan cache is `Table::cached_full_scan`
(table.rs:514), which lives on `Table`, not `MemTable`, and is invalidated by
*writes*, not by consolidation staleness.

**The byte budget often pre-empts the run-count trigger.** The production
ingest path `Table::upsert_owned_and_maybe_flush` (table.rs:206) calls
`should_flush()` (true when `runs_bytes > max_bytes * 3 / 4`, memtable.rs:126)
both before and after each upsert and flushes (consolidate + write shard +
`reset()`) when it trips. Arenas are 1 MiB for single-partition tables and
256 KiB per partition for multi-partition (256-way) user tables
(`partition_arena_size`, partitioned_table.rs:447). For a 256 KiB arena the
byte flush fires at ~192 KiB. Whether 8 or 16 runs accumulate *before* a flush
depends entirely on rows-per-ingest-per-partition, which is workload-dependent
and unverified.

## The real cost model

Treat each batch as `b` rows, no cancellation, `M` batches arriving in one
memtable fill cycle (between flushes), threshold `T`. A consolidation fires
every `T-1` arrivals after the first, so there are `K ≈ M/(T-1)` of them. Each
fires over the growing survivor plus the new small runs, and re-merges
everything:

- Rows present at the k-th consolidation: `S_k = T·b + (k-1)·(T-1)·b`.
- Total rows **moved** across the fill:
  `R = K·T·b + (T-1)·b·K(K-1)/2 ≈ b·M² / (2·(T-1))`.

So total ingest-side rows-moved is **quadratic in `M` and proportional to
`1/(T-1)`** — the threshold scales the dominant term; it is not a
constant-factor-only knob. The average rows-moved *per consolidation* is `≈ b·M/2`,
independent of `T`; the `1/(T-1)` term comes purely from doing `K` more
consolidations, not from any per-merge growth.

Comparator calls add a `log₂(T)` factor per moved row (loser-tree height at
fan-in `T`), giving total compares `∝ log₂(T)/(T-1)`.

Evaluating 16 → 8:

| Quantity | T=16 | T=8 | ratio (8 vs 16) |
|---|---|---|---|
| consolidations `K` (∝ 1/(T-1)) | 1/15 | 1/7 | **2.14×** |
| rows moved `R` (∝ 1/(T-1)) | 1/15 | 1/7 | **2.14×** |
| comparator calls (∝ log₂T/(T-1)) | 4/15 | 3/7 | **1.61×** |
| loser-tree height per moved row | 4 | 3 | 0.75× |

Direct simulation (b=1, M=10000) confirms the rows-moved closed form: T=16 moves
3,332,331 rows; T=8 moves 7,143,570 rows (≈ 2.14×).

The 0.75× shallower tree does not offset the 2.14× increase in rows re-merged.
**Lowering `T` increases total ingest merge work.** If anything, the model says
*raising* `T` reduces ingest merge work (at the cost of larger read-time fan-in).

## Where the time actually goes

The latest in-tree profile (`benchmarks/results/20260508_220249/report.txt`,
Workers Userspace, release) attributes worker cycles roughly as:

| % | function | path |
|---|---|---|
| 15.37% | `ReadCursor::drain_sorted_into` | read |
| 12.85% | `MemTable::find_positive_payload_row` | read / lookup |
| 8.96% | `Batch::find_lower_bound` | read / lookup |
| 3.35% | `compare_rows_int_nonnull` | shared comparator |
| **1.44%** | **`merge::merge_batches`** | **consolidation + flush** |
| 0.63% | `compare_rows` | shared comparator |

`merge_batches` — the *only* function the threshold directly moves — is ~1.4%,
and even that is shared between inline consolidation, flush-time consolidation,
and is **not** the read-cursor's merge (the cursor has its own `LoserTree` inside
`drain_sorted_into`). The inline-consolidate slice the threshold can move is a
fraction of 1.4%.

The "~18.5% to consolidation" figure used to motivate the change is stale and
unsupported: there is no `compare_rows_perf.md` in the repo, and the figure
bundled `merge_batches` with `sift_down_static` + `collect_min_indices`, which no
longer exist. In an older profile the bulk of that 15% was the binary-heap
sift-down serving the **read cursor's** n-way merge, which today appears as
`drain_sorted_into` (15.37%) — a read-path cost the threshold does not touch.

## The read-side effect is real but weak and conditional

Lowering `T` does reduce the *average* number of memtable runs, and the read
cursor's loser tree merges a unified source set whose fan-in is
`memtable_runs + shard_arcs` (read_cursor.rs). So fewer memtable runs slightly
shrink that tree — but:

- The tree size is usually dominated by shards once any flush has occurred.
- Any non-empty memtable already disqualifies the `PkUnique` shard fast path
  (`advance_pk_unique`, read_cursor.rs), so the run-count reduction does not
  unlock a faster path, it only trims one or two tree levels at most.
- The benefit only exists when many memtable runs are live at scan time, which
  requires interleaving ingests with scans on the same table/partition.

## Better levers (in rough priority)

1. **Attack the read/lookup path, not consolidation.** `drain_sorted_into`
   (15%), `find_positive_payload_row` (13%), and `find_lower_bound` (9%) are the
   real hot spots. `find_positive_payload_row_bytes` (memtable.rs:285) iterates
   *all* runs and, for each candidate, calls `find_weight_for_row_bytes`
   (memtable.rs:251) which itself re-scans *all* runs — an O(runs²) shape per PK.
   Reducing run count would help this too, but the direct fix is the algorithm,
   not the threshold. This lookup fix is now specified separately in
   `memtable-pk-lookup-single-pass.md`, which makes each lookup linear in run
   count independent of any threshold value.
2. **Don't fold the survivor on every trigger.** `force_consolidate` re-reads and
   re-writes the entire accumulated base each time. A minimal memtable-local
   scheme — accumulate the small new runs and only merge them into the base when
   the *small-run* count hits a sub-threshold (a 2-tier "tier-0 small runs /
   tier-1 base") — caps base re-touch frequency and cuts the `b·M²/(2(T-1))`
   volume directly, with no fan-in tradeoff. This is local to
   `maybe_inline_consolidate`/`force_consolidate` and far smaller than the on-disk
   tiered compaction in `shard_index.rs`. (Note: a plain "2-way merge with the
   base" does **not** help — it still reads every base byte each trigger; the win
   only comes from *deferring* the base merge.)

## Should the threshold be *raised* instead?

No. The threshold is a balance point between two costs that move in opposite
directions with `T`:

| | ingest-merge (`force_consolidate`→`merge_batches`) | read/lookup (scales with live run count, avg ≈ `T/2`) |
|---|---|---|
| lower `T` | worse (∝ `1/(T-1)`, ~2.14× at T=8) | fewer runs |
| raise `T` | better | **more** runs |

The profile decides the trade, and it is overwhelmingly read/lookup-dominated:
`merge_batches` (the only thing raising `T` improves) is **1.4%**, while the
run-count-sensitive read/lookup functions total **~37%** (`drain_sorted_into`
15.4%, `find_positive_payload_row` 12.9%, `find_lower_bound` 9.0%). Worse,
`find_positive_payload_row_bytes` is **O(runs²)** in *memtable* run count
specifically (it iterates all runs and re-scans all runs per candidate, see
"Better levers" #1), and it is the #2 hot function. Raising `T` lets more runs
accumulate, directly inflating that quadratic cost to save a sliver of an
already-tiny 1.4% — spending the expensive resource to save the cheap one.

So neither direction is justified: lowering `T` roughly doubles ingest merge work
for an unproven read benefit; raising `T` worsens the dominant read/lookup path.

**This does not make 16 optimal — it makes the value nearly irrelevant at current
scale.** Because the byte-flush usually pre-empts the run-count trigger (256 KiB
arena, ~192 KiB flush ceiling, one run per ingest), the threshold rarely fires in
production: the cost curve is flat across a wide band (roughly 8–32 behave
identically), so a value chosen by rule of thumb sits in that flat region, which
is exactly why 16 has never surfaced as a problem. There is no measured optimum;
there is no gradient to act on.

Where the threshold *does* bind (small batches, large arena, point-lookup- or
update-heavy on a single partition), the gradient is not flat and it points
**down**, not up — read/lookup cost grows with average run count (≈ `T/2`), so a
lower `T` would help reads there. That win is not actionable today because it is
(a) gated behind first fixing the quadratic lookup in "Better levers" #1, (b)
bought at ~2× the ingest-merge cost, and (c) unmeasurable in the current bench
suite.

**What the lookup fix changes.** Once `memtable-pk-lookup-single-pass.md` lands,
`find_positive_payload_row` (12.9%) stops being quadratic in run count and becomes
linear, removing the largest run-count-sensitive read cost. That flattens the
down-gradient above: with the lookup linear, lowering `T` buys far less on the
read side while still costing ~2× on ingest, so the lookup fix does not make
lowering attractive — it makes the threshold genuinely neutral. The only
remaining run-count-sensitive read cost is then `drain_sorted_into`; the threshold
becomes worth setting deliberately only if that proves to dominate on a workload
that actually binds.

Net: **leave `INLINE_CONSOLIDATE_THRESHOLD` at 16 — not because 16 is the optimum,
but because the constant is the wrong knob to tune.** The read/lookup algorithm
(`memtable-pk-lookup-single-pass.md`) and the consolidation scheme below are the
leverage; do them first. Only after the lookup is linear and a binding workload
exists does the threshold become a real, measurable knob — and at that point set
it by measurement, not by guess.

## Bug to fix (independent of any threshold change)

`test_inline_consolidate_should_flush` has stale inline comments that contradict
its own (correct) top-of-test arithmetic. The test uses `max_bytes = 560`
(threshold `560*3/4 = 420`); 14 one-row inserts = 448 bytes (`should_flush`
true); after consolidation 12 net rows = 384 bytes (`should_flush` false). But
the inline comments read:

```rust
// memtable.rs:1095
// 6 more insertions → 14 runs, runs_bytes ≈ 560 > 525   <-- wrong: 448 > 420
// memtable.rs:1109
// After consolidation: net 12 rows ≈ 480 < 525          <-- wrong: 384 < 420
```

The `525`/`560`/`480` numbers are left over from an earlier `max_bytes` and
should be corrected to `420`/`448`/`384`.

## If you still insist on running the experiment

The procedure below is corrected for current paths and APIs. Expect it to show
either a small ingest regression or noise, per the analysis above.

### Instrument first — confirm the threshold even fires

Add a temporary counter to `force_consolidate` (or log on entry) and run the
target benchmarks. If it fires **< 1×** per flush cycle, the change is a no-op
on those benchmarks and nothing below will move. Per the regime analysis, the
named micro-benchmarks almost certainly fall here:

- `test_full_scan`: a single bulk `push` → one run per partition, then scans
  over an unmodified (and result-cached) table → consolidation never fires.
- `test_update_pk_seek` / `test_delete_pk`: bulk load + ~50 point ops scattered
  across 256 partitions → a single partition almost never reaches 8 runs, let
  alone 16.

To exercise consolidation at all you need a workload that drives **one**
partition (single-partition table, or a system-table-shaped workload) to a high
run count with non-trivial run sizes — e.g. many sequential same-key-range
batches before any flush. None of the six benchmarks the original plan named do
this, so as configured the suite cannot validate the change.

### Benchmark commands

```bash
make bench-full        # full mode, 4 workers; results under benchmarks/results/<timestamp>/
uv run benchmarks/report.py   # writes report.txt (print the path); reports the LATEST run only
```

Note: `report.py` does **not** diff baseline vs candidate — it summarizes one
run. To compare, read the two `summary.json` files yourself. They live at
`benchmarks/results/<timestamp>/w4_c1/summary.json` (the per-config subdir), not
at the timestamp root.

### Production code change

```rust
// crates/gnitz-engine/src/storage/memtable.rs:18
const INLINE_CONSOLIDATE_THRESHOLD: usize = 8;
```

That is the only production line. The `Vec::with_capacity` at line 82 picks up
the new value automatically.

### Test updates (only if the constant changes)

Several tests hardcode 16/15 and break at any other value. The corrected
rewrites below use the **real** API: `consolidate_for_flush() -> Rc<Batch>` (there
is no `get_snapshot`) and `lookup_pk_bytes(&key.to_be_bytes())` (there is no
`lookup_pk`). The arithmetic for T=8 is correct.

`test_inline_consolidate_basic` — push 7 (no consolidation), then an 8th:

```rust
for i in 0..7u64 {
    let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
    mt.upsert_sorted_batch(b).unwrap();
}
assert_eq!(mt.runs.len(), 7);
let b8 = make_batch(&schema, &[(8, 1, 800)]);
mt.upsert_sorted_batch(b8).unwrap();
assert_eq!(mt.runs.len(), 1);
let snap = mt.consolidate_for_flush();
assert_eq!(snap.count, 8);
assert_eq!(snap.get_pk(0), 1);
assert_eq!(snap.get_pk(7), 8);
```

`test_inline_consolidate_below_threshold` — push 7, no consolidation:

```rust
for i in 0..7u64 { /* push pk = i+1 */ }
assert_eq!(mt.runs.len(), 7);
let snap = mt.consolidate_for_flush();
assert_eq!(snap.count, 7);
```

`test_inline_consolidate_repeated` — 8 → 1, +6 → 7, +1 → 8 → 1, final count 15:

```rust
for i in 0..8u64 { /* push pk = i+1 */ }
assert_eq!(mt.runs.len(), 1);
for i in 8..14u64 { /* push pk = i+1 */ }
assert_eq!(mt.runs.len(), 7);
let b15 = make_batch(&schema, &[(15, 1, 1500)]);
mt.upsert_sorted_batch(b15).unwrap();
assert_eq!(mt.runs.len(), 1);
let snap = mt.consolidate_for_flush();
assert_eq!(snap.count, 15);
```

`test_inline_consolidate_all_cancelled` — interleave so the 8th push (the 4th
retract) triggers consolidation and everything cancels:

```rust
for i in 0..4u64 {
    let ins = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
    mt.upsert_sorted_batch(ins).unwrap();
}
for i in 0..4u64 {
    let ret = make_batch(&schema, &[(i + 1, -1, (i + 1) as i64 * 100)]);
    mt.upsert_sorted_batch(ret).unwrap();
}
assert_eq!(mt.runs.len(), 0);
assert!(mt.is_empty());
assert_eq!(mt.total_row_count(), 0);
let snap = mt.consolidate_for_flush();
assert_eq!(snap.count, 0);
```

`test_inline_consolidate_should_flush` — each row is 32 bytes (8 PK + 8 weight +
8 null + 8 payload, no blob), confirmed via `Batch::total_bytes`. `should_flush`
uses strict `>`. Pick `max_bytes = 280` (75% = 210): 7 inserts = 224 > 210
(true), then 1 retract → 8 runs → consolidation → 6 net rows = 192 < 210 (false):

```rust
let mut mt = MemTable::new(schema, 280);
for i in 0..7u64 {
    let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
    mt.upsert_sorted_batch(b).unwrap();
}
assert!(mt.should_flush(), "gross bytes should exceed threshold");
let r1 = make_batch(&schema, &[(1, -1, 100)]);
mt.upsert_sorted_batch(r1).unwrap();
assert!(!mt.should_flush(), "net state should be under threshold");
```

`test_inline_consolidate_has_found_cleared` — `force_consolidate` clears
`has_found`, and a consolidation-triggering push is the only ingest path that
reaches it (`reset()` and the lookup functions also write `has_found`, but not
during an upsert). Keep run count under 8 before the lookup, then push the 8th:

```rust
for i in 0..7u64 {
    let b = make_batch(&schema, &[(i + 1, 1, (i + 1) as i64 * 100)]);
    mt.upsert_sorted_batch(b).unwrap();
}
let (w, found, _) = mt.lookup_pk_bytes(&5u64.to_be_bytes());
assert_eq!(w, 1);
assert!(found);
assert!(mt.has_found);
let b8 = make_batch(&schema, &[(8, 1, 800)]);
mt.upsert_sorted_batch(b8).unwrap();
assert!(!mt.has_found);
```

### Verification checklist (only if committing a threshold change)

- [ ] `make test` — unit tests pass with updated counts.
- [ ] `make e2e` — consolidation runs in real worker processes.
- [ ] `force_consolidate` fire-count instrumentation shows the threshold
      actually fires on the benchmarked workload (else the change is a no-op).
- [ ] Both `summary.json` files compared by hand; record both numbers.

## What this plan does NOT cover

- **Replacing the loser tree.** It is correct and asymptotically optimal;
  fan-in is a constant-factor tweak only.
- **On-disk tiered/leveled compaction** (`shard_index.rs`, `L0_COMPACT_THRESHOLD`).
  The memtable-local "don't fold the survivor" scheme above is a much smaller,
  separate change.
- **Per-table or adaptive thresholds.** Moving the constant into
  `MemTable::new` parameters and threading it through every caller is a separate
  design.
