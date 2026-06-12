# Per-Range Compaction Trigger

Replace the single global `L0_COMPACT_THRESHOLD = 4` shard-count trigger
with a **per-range overlap-count trigger** that compacts only the L0
shards covering a hot PK range, leaving cold ranges alone.

The goal is to bound *per-range read amplification*: today a workload
that concentrates writes in one PK range can sit at exactly 4 L0 shards
indefinitely (each new shard overlaps the hot range, the merge picks
all 4 up at once), and a workload that spreads writes uniformly across
disjoint ranges pays the full L0→L1 cost on every shard.

No prerequisites. Self-contained inside `storage/shard_index.rs` and
its callers.

---

## Background

### Today's trigger

```rust
// crates/gnitz-engine/src/storage/shard_index.rs:204-209
fn update_flags(&mut self) {
    self.needs_compaction = self.l0.len() > L0_COMPACT_THRESHOLD;
}
pub fn should_compact(&self) -> bool {
    self.needs_compaction
}
```

`L0_COMPACT_THRESHOLD = 4`. When the global L0 shard count exceeds 4,
`run_compact()` (line 441) merges **every** L0 shard via
`compact::merge_and_route`, routes the merged output into L1 guards by
PK, and clears L0.

This is correct but coarse:

- **Cold-range write amplification.** If four flushes write into four
  disjoint PK ranges (e.g. four guards), the trigger fires and every
  shard is rewritten into L1 even though no range has any overlap.
- **Hot-range read amplification is unbounded below the threshold.**
  Four L0 shards stacked on the same PK range deliver four cursor
  hits per read until the threshold trips — and stay at four for
  arbitrarily long if some writes also land in unrelated ranges.
- **Per-tick latency is bursty.** When the trigger fires, `run_compact`
  rewrites all of L0 in one shot. A per-range trigger spreads the work
  across more, smaller compactions.

### Why L0 has overlap at all

`ShardIndex.l0` is a `Vec<ShardEntry>` sorted by `pk_min` (line 200).
Each entry has `(pk_min, pk_max)`. Two L0 shards overlap iff their
ranges intersect; L1+ levels are guard-partitioned and never overlap
within a guard. The L1 guard key list (`l1_guard_keys`, line 498) is
the canonical "ranges of interest". The number of L0 shards that
intersect a given guard range is the read-amp signal we want to bound.

---

## Design

### The trigger

For every L0 shard insertion (`add_shard`), recompute a per-range
overlap count and set `needs_compaction` if any range exceeds
`PER_RANGE_OVERLAP_THRESHOLD`.

```rust
const PER_RANGE_OVERLAP_THRESHOLD: usize = 3;

fn update_flags(&mut self) {
    self.needs_compaction =
        self.l0.len() > L0_COMPACT_THRESHOLD                   // safety net
        || self.hottest_range_overlap() > PER_RANGE_OVERLAP_THRESHOLD;
}
```

The global threshold stays as a backstop: it caps L0 size when writes
are spread across many ranges that each individually stay below the
per-range bound. `PER_RANGE_OVERLAP_THRESHOLD = 3` is intentionally
tighter than the global 4 so a single hot range fires before global
backlog does.

### Computing the hottest range overlap

The candidate ranges are the L1 guard intervals (or, when L1 is empty,
the L0 shards' own intervals). For each candidate range `[gk_i, gk_{i+1})`,
count L0 entries whose `[pk_min, pk_max]` intersects it.

```rust
fn hottest_range_overlap(&self) -> usize {
    let keys = self.l1_guard_keys();           // already exists, line 498
    let mut max_overlap = 0;
    for i in 0..keys.len() {
        let lo = keys[i];
        let hi = keys.get(i + 1).copied().unwrap_or(u128::MAX);
        let n = self.l0.iter()
            .filter(|e| e.pk_max >= lo && e.pk_min < hi)
            .count();
        max_overlap = max_overlap.max(n);
    }
    max_overlap
}
```

`l0` is sorted by `pk_min`, so this is `O(L0 × G)` with both small
constants in practice (L0 ≤ 4 typically, G = number of L1 guards).
Cheap enough to run on every `add_shard`.

### Partial compaction: `run_compact_range`

`run_compact()` today merges *all* of L0 into L1. The new partial
path takes a range, identifies the L0 shards overlapping it, and
merges only those into the L1 guard(s) for that range.

```rust
pub fn run_compact_range(&mut self, lo: u128, hi: u128)
    -> Result<(), StorageError>
{
    // 1. Collect overlapping L0 entries
    let overlap_idx: Vec<usize> = self.l0.iter().enumerate()
        .filter(|(_, e)| e.pk_max >= lo && e.pk_min < hi)
        .map(|(i, _)| i)
        .collect();
    if overlap_idx.len() < 2 { return Ok(()); }  // nothing useful

    // 2. Run the existing merge_and_route on this subset only.
    //    Restrict guard_keys to keys in [lo, hi).
    let filenames: Vec<String> = overlap_idx.iter()
        .map(|&i| self.l0[i].filename.clone()).collect();
    let max_lsn = overlap_idx.iter()
        .map(|&i| self.l0[i].max_lsn).max().unwrap_or(0);
    let guard_keys: Vec<u128> = self.l1_guard_keys().into_iter()
        .filter(|k| *k >= lo && *k < hi)
        .collect();

    // ... call compact::merge_and_route with those keys, same shape
    //     as run_compact lines 458–475.

    // 3. commit_l0_to_l1_partial: remove only the merged entries,
    //    install new L1 entries into their guards.
}
```

The subroutine `compact::merge_and_route` already accepts an
arbitrary guard key list and an arbitrary input file list; only the
caller changes.

`run_compact_range` does **not** clear all of L0 — it removes the
specific indices it merged and leaves the rest. The remaining L0
shards still answer reads for their cold ranges.

### Dispatch

`run_compact()` becomes a dispatcher that decides between global and
per-range modes:

```rust
pub fn run_compact(&mut self) -> Result<(), StorageError> {
    if !self.needs_compaction { return Ok(()); }
    self.compact_seq += 1;

    // Prefer per-range path when a hot range exists and L0 isn't full.
    if self.l0.len() <= L0_COMPACT_THRESHOLD {
        let keys = self.l1_guard_keys();
        for i in 0..keys.len() {
            let lo = keys[i];
            let hi = keys.get(i + 1).copied().unwrap_or(u128::MAX);
            let count = self.l0.iter()
                .filter(|e| e.pk_max >= lo && e.pk_min < hi).count();
            if count > PER_RANGE_OVERLAP_THRESHOLD {
                self.run_compact_range(lo, hi)?;
            }
        }
        // L1 housekeeping carries over from the old run_compact tail.
        self.compact_guards_if_needed()?;
        if !self.levels.is_empty() && self.levels[0].total_file_count() > L1_TARGET_FILES {
            self.compact_guard_vertical(1)?;
        }
        self.update_flags();
        return Ok(());
    }

    // Global path: existing run_compact body below this point — unchanged.
    self.run_compact_all()
}
```

Rename the existing `run_compact` body to `run_compact_all` so the
two paths share nothing but the trigger check.

### What about cascading?

`compact_guards_if_needed` (line 548) and `compact_guard_vertical`
(level vertical merge, called from line 491) already handle L1+ →
deeper-level cascade. Partial compaction lands new entries into the
same L1 guards the global path would land them in, so cascading is
unaffected.

---

## File Changes

### 1. `crates/gnitz-engine/src/storage/shard_index.rs`

- Add `PER_RANGE_OVERLAP_THRESHOLD: usize = 3` near the existing
  constants (line 26-30).
- Replace `update_flags` (line 204) with the variant above.
- Add `hottest_range_overlap` (private helper).
- Rename `run_compact` to `run_compact_all`; add the new
  `run_compact` dispatcher above.
- Add `run_compact_range(lo, hi)` (~50 lines: builds the subset
  filename list, runs `compact::merge_and_route`, calls a new
  `commit_l0_to_l1_partial`).
- Add `commit_l0_to_l1_partial(merged_indices, guard_outputs,
  max_lsn)`. Mirrors `commit_l0_to_l1` (line 519) but removes only
  `merged_indices` from `self.l0` (in reverse order to keep indices
  valid), instead of `self.l0.clear()`.

### 2. No callers change

`compact_if_needed` (`table.rs:611`), `vm.rs:564`, `dag.rs:1197`,
`partitioned_table.rs:128` all call through `should_compact` and
`run_compact`. Both keep their existing signatures.

---

## Edge cases

**Empty L1.** When L1 has no guards, `l1_guard_keys` returns L0's own
`pk_min` values (line 506-516). The per-range overlap check then
counts overlaps against intervals defined by L0 itself — meaningful
only when ≥ 2 L0 shards exist, otherwise overlap is trivially 1.
Already correct: with 0 or 1 L0 shards there is nothing to compact.

**Single-shard hot range below threshold.** A range with one L0 shard
has overlap 1, below 3, no trigger. The cost — at most one extra L0
probe per read for that range — is acceptable and is the entire point
of bounding *per-range* fan-out rather than eliminating it.

**Range bigger than the whole keyspace.** When L1 has one guard
covering all keys, the per-range count equals the global L0 count.
The two thresholds (3 per-range vs 4 global) collapse into "3" in
that case, which is intentional: a single hot range with 4 shards is
exactly the case we want to trigger sooner.

**Concurrent partial compactions.** Not a concern: `run_compact` runs
synchronously inside one worker call. Multiple per-range compactions
in the dispatcher loop run sequentially.

**Compaction failure mid-pass.** Each `run_compact_range` is atomic
in the same sense as the existing path: the new L1 entries are
opened before `self.l0` is mutated (mirrors `commit_l0_to_l1`'s
"all opens succeed before touching state" pattern at line 528-543).
If a later range in the dispatcher loop fails, earlier successful
ranges have already moved to L1 — strictly better than the all-or-
nothing global path.

---

## Tests

All new tests go in `crates/gnitz-engine/src/storage/shard_index.rs`
as `#[cfg(test)] mod tests` next to the existing compaction tests.

**`test_per_range_trigger_fires_on_hot_range`**

Create an index with 2 disjoint L1 guards. Flush 4 L0 shards that all
fall into guard 0's range, 0 shards into guard 1. Assert
`should_compact()` returns true *and* `hottest_range_overlap()` ≥ 4.
Call `run_compact()`. Assert: guard 0 received the merged output,
guard 1 is empty, L0 is empty. `compact_seq` advanced by 1.

**`test_per_range_trigger_skips_cold_range`**

Create 4 L0 shards across 4 disjoint guards (one shard per guard,
each overlap = 1). Assert `should_compact()` is false (global
threshold not exceeded; per-range overlap = 1 < 3). Add a fifth
shard into any guard: now global = 5 > 4, global trigger fires,
falls through to `run_compact_all`. Verify all L0 cleared.

**`test_per_range_partial_compaction_leaves_other_shards`**

Create 4 L0 shards: 3 in guard 0, 1 in guard 1. Per-range overlap =
3 in guard 0, which is *not* > 3 — no trigger. Add a 4th shard to
guard 0: overlap = 4 > 3, trigger fires. Call `run_compact()`.
Assert: guard 0 got the merge of those 4 shards, guard 1's single
L0 shard is still in L0, L0.len() == 1.

**`test_per_range_then_global`**

Set up state where one range triggers per-range first
(`PER_RANGE_OVERLAP_THRESHOLD + 1` shards in range A) and a second
range crosses the threshold after. Verify both ranges' shards land
in their respective L1 guards; remaining L0 is whatever didn't
overlap either range.

**`test_run_compact_failure_partial_progress`**

Mirror of the existing `test_run_compact_fails_on_long_path_l0_intact`
(line 1077) but with the per-range path: cause `merge_and_route` to
fail on the second of two hot ranges. Assert: first range's shards
are in L1, second range's shards remain in L0, no data is lost.

---

## Implementation order

```
[1] add hottest_range_overlap + new update_flags (no behavior change
    until run_compact uses it). Test: assert overlap counts on synthetic
    L0 states.
[2] add commit_l0_to_l1_partial. Test: round-trip insert/remove
    specific indices.
[3] add run_compact_range. Test: per-range compaction in isolation.
[4] wire run_compact dispatcher. Run full storage test suite.
[5] tune PER_RANGE_OVERLAP_THRESHOLD by benchmarking
    `make bench-sweep` against the current trigger.
```

Each step is a separate commit; the engine still passes `make test`
after each.

---

## What this enables

- **Bounded per-range read fan-out.** Cursor reads on any single key
  now traverse at most `PER_RANGE_OVERLAP_THRESHOLD = 3` L0 shards
  before the merge into L1 collapses them.
- **Smaller, more frequent compactions.** Each `run_compact_range`
  rewrites a subset of L0, not all of it. Tick-time compaction cost
  becomes proportional to the hot range's size, not to total L0.
- **Cold-range write amplification eliminated.** Disjoint flushes
  into different guards no longer trigger a full L0 rewrite.

The change is purely additive to the trigger logic; the merge,
manifest, and cursor paths are untouched.
