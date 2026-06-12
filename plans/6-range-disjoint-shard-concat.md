# Performance plan: detect range-disjoint sources and concatenate

## Background

The k-way merge in `ReadCursor` (`storage/read_cursor.rs`) compares
keys at every step regardless of whether the inputs actually interleave.
For workloads that flush memtables in a way that produces shards with
non-overlapping PK ranges — append-mostly tables, time-series, anything
keyed by a monotonic identifier — most of those compares produce
trivial answers: shard 0 is fully drained before shard 1's first key
ever has to be considered.

A shard that holds PKs in `[lo_i, hi_i]` is **range-disjoint** from
shard *j* if `hi_i < lo_j` (or the symmetric reverse). A merge over
range-disjoint sources is by definition a *concatenation*: emit source
0 in full, then source 1, etc. Zero comparisons across sources.

`MappedShard` already exposes `count` and `get_pk(row)`
(`storage/shard_reader.rs:317`), so `(get_pk(0), get_pk(count - 1))` is
the (lo, hi) range. In-memory `Batch` runs expose the same.

The opportunity is bounded but cheap: a constant-time check per cursor
build that, when it succeeds, replaces the entire heap path with a
linear walk.

## The change

### Step 1 — capture per-source range at cursor construction

In `read_cursor::create_read_cursor` (`storage/read_cursor.rs:888`):

```rust
struct SourceRange { lo: u128, hi: u128 }

let ranges: Vec<SourceRange> = entries.iter().map(|e| {
    let lo = e.source.get_pk(0);
    let hi = e.source.get_pk(e.count - 1);
    SourceRange { lo, hi }
}).collect();
```

Skip empty entries (already handled — `entries` excludes `count == 0`).

### Step 2 — detect total ordering

Sort source indices by `lo`. The set is range-disjoint iff
`ranges[order[i]].hi < ranges[order[i+1]].lo` for every adjacent pair.
(Strict `<` because two sources with the same boundary PK could share
that PK — needs the merge for ghost detection.)

```rust
let mut order: Vec<usize> = (0..ranges.len()).collect();
order.sort_unstable_by_key(|&i| ranges[i].lo);
let disjoint = order.windows(2).all(|w| ranges[w[0]].hi < ranges[w[1]].lo);
```

### Step 3 — add a TreeKind variant

Extend `TreeKind` (`read_cursor.rs:331`):

```rust
enum TreeKind {
    None,
    Two,
    Heap(MergeHeap),
    Concat { order: Vec<u32>, current: usize },
}
```

Selection in `ReadCursor::new`:

```rust
let tree = match (n, disjoint) {
    (0 | 1, _) => TreeKind::None,
    (_, true)  => TreeKind::Concat { order, current: 0 },
    (2, _)     => TreeKind::Two,
    _          => TreeKind::Heap(...),
};
```

### Step 4 — implement Concat path

`advance` and `find_next_non_ghost` for the `Concat` branch:

```rust
TreeKind::Concat { order, current } => {
    let idx = order[*current] as usize;
    self.entries[idx].advance();
    if !self.entries[idx].is_valid() {
        *current += 1;
    }
    // No cross-source compare. Ghost handling is per-entry only,
    // since same-PK-different-source can't happen in disjoint mode.
}
```

`find_next_non_ghost` for `Concat`: walk the active entry, skipping
ghosts via `skip_ghosts` (already on the entry). When the active entry
exhausts, advance `current`. When all are exhausted, `valid = false`.

### Step 5 — `seek`

`ReadCursor::seek` (`read_cursor.rs:421`) needs to position into the
right source for the disjoint case: binary-search `order` for the first
range whose `hi >= key`, set `current` to that index, then `seek` the
underlying entry. Other entries don't need to seek — they will be
visited in order later.

## Why this is safe

Range-disjointness is a strictly stronger property than what the heap
path requires; the heap path is the fallback if it doesn't hold. The
output row sequence (PK, weight, payload) is identical between the two
paths because:

- Within a source, both paths deliver rows in the source's stored order
  (the cursor's `entry.advance()` is the only mutation).
- Across sources, range-disjointness guarantees that source `order[i]`
  is fully ordered before source `order[i+1]`'s first row. The heap
  path would also emit them in that order.

Ghost handling: in disjoint mode, no two sources share a PK, so the
weight-summation across sources can never apply. The only ghosts
possible are within a single source (shard with retraction entry that
hasn't been compacted out), which is handled by `entry.skip_ghosts()`.

The `<` rather than `≤` boundary check is load-bearing. If two adjacent
sources both contain `pk = K`, those entries must enter the merge
because their weights might cancel (Z-set ghost) or stack (DBSP weight
sum). Falling back to the heap path in that case is correct.

## Validation procedure

1. Add unit tests in `read_cursor.rs::tests`:
   - Disjoint two-source: ranges `[1..10]` and `[20..30]` → output order
     matches heap path on the same input.
   - Adjacent-but-touching: `[1..10]` and `[10..20]` (shared boundary
     PK) → must take the heap path; verify by an internal accessor on
     `TreeKind`.
   - Disjoint n-source for n ∈ {3, 5, 16}.
   - Empty-source-mixed-in: should still detect disjoint.
   - Seek into the second range: `seek(25)` lands correctly.

2. Property test: random sources, `compare_rows`-equivalent output
   between heap path and concat path (when disjoint).

3. `make test` + `make e2e`.

4. Bench:
   - The hot benchmarks today don't have a clean disjoint-shard
     workload, so add one to `benchmarks/micro/`:

     ```python
     # benchmarks/micro/test_select_append_only.py
     def test_full_scan_append_only(client, schema_name, bench_timer, scale):
         """Bulk-load in monotonic PK order, then full scan.
         Each push becomes a memtable run; flushes produce range-disjoint
         shards.  Exercises the disjoint-concat fast path."""
         ...
     ```

   - Bench baseline + candidate, compare via
     `uv run benchmarks/report.py`.

### Expected to win

- The new append-only benchmark.
- `test_insert_bulk_throughput` post-flush reads (if any).
- Any time-series-shaped test added later.

### Expected to be neutral

- All existing benchmarks (random PK distribution, overlapping ranges
  by construction).

### Decision criteria

- The detection check is O(n log n) on cursor construction with n
  typically ≤ 32. Worst-case overhead per cursor build: a sort and a
  windowed scan. If the bench shows this overhead is measurable on
  cursors that don't take the fast path, gate the check behind a
  heuristic (e.g. only attempt detection when `n ≥ 4`, since n=2 and
  n=3 already have specialised paths and disjoint-detection saves
  little).
- Win on the disjoint benchmark with no regression elsewhere: commit.
- Otherwise: park the plan; the workload it targets is rare enough that
  the overhead-on-the-cold-path doesn't pay back.

## Composes with

- **Loser tree** and **skip-snapshot-pre-merge**: this plan is a fast
  path that bypasses both. The plans don't conflict; the disjoint
  detection runs before the loser tree is even constructed.
- **Single-row-per-PK**: orthogonal. Disjoint-concat skips comparison
  entirely; same-PK-tie-break never fires.

## What this plan does NOT cover

- **Partial disjointness**: shards 0..i are disjoint among themselves,
  but shard i+1 overlaps. Could in principle concat 0..i and merge from
  i+1 onward. Out of scope; it complicates the cursor state machine for
  modest additional benefit.
- **Detecting disjointness across memtable runs**: memtable runs almost
  always overlap (they represent successive writes to the same key
  space). The detection works there too, just rarely succeeds.
- **Reordering shards in storage to enforce disjointness**: a
  compaction-policy change. Out of scope; this plan is a read-side
  fast path that exploits the property when it happens to hold.
- **Filtered scans**: a `WHERE` predicate that narrows the PK range
  could prune sources whose ranges fall outside; that is a separate
  predicate-pushdown plan and unrelated to disjointness detection.
