# Performance plan: bound the merge fan-in with a leveled LSM layout

## Background

The merge cost on the read path scales with the number of sources the
cursor sees. Today that is:

- **Memtable runs**: 0..16, governed by `INLINE_CONSOLIDATE_THRESHOLD`
  (`storage/memtable.rs:19`).
- **Shards**: unbounded — every memtable flush produces a new shard.
  Compaction (`storage/compact.rs`) merges shards but the policy that
  decides *when* to compact and *which* shards to merge is not enforcing
  a global cap.

For a long-running table the shard count grows. The cursor's heap walks
all of them on every read. Even with the loser tree
(`3-loser-tree-merge.md`), which makes the per-row cost `O(log k)`
instead of `O(2 log k)`, growing `k` is bad: cache footprint of the
heap, comparator dispatch overhead, and the fixed per-shard cursor
construction cost all scale with `k`.

The textbook answer is **leveled compaction** (LevelDB / RocksDB style):
constrain the system so that at most a small constant number of sources
exist at any moment, regardless of total data size. With L levels of
size ratio T, the steady-state shard count is `≤ L`, and `T^L = N` so
`L = log_T(N)`. For `T = 10`, `L ≤ 7` even at petabyte scale.

This plan trades **write amplification** (each row is rewritten
`L × T` times across its lifetime instead of `1`) for **predictable
read latency**. It is the right answer for read-heavy workloads where
the merge dominates, and the wrong answer for write-only workloads
where every additional rewrite is pure cost.

## Background within this codebase

The repo already has the moving parts:

- A memtable that merges runs at threshold (`memtable.rs:19`).
- A flush path that produces shards (`shard_file.rs`,
  `storage/table.rs::flush_*`).
- A compactor (`storage/compact.rs`) that runs k-way merges over
  shards.

What it does not have:

- A **policy** that decides which shards live in which level.
- A **trigger** that keeps each level's size within a target.
- A guarantee that the read path's `k` is bounded by `L`.

The current compactor decides what to merge based on local heuristics
(see the file). This plan replaces that policy.

## The change

### Phase 1 — represent levels in shard metadata

Each shard gets a `level: u8` field, persisted in its file header. New
flushes from the memtable land at level 0. Compactions move shards
upward.

`storage/shard_index.rs` (the in-memory list of shard `Rc`s) groups
them by level: `Vec<Vec<Rc<MappedShard>>>` instead of the flat
`Vec<Rc<MappedShard>>` it likely is today.

### Phase 2 — leveled compaction policy

Two sub-policies, picked by table at creation time:

- **Tiered** (default for write-heavy): when level `L` has `T` shards,
  merge them into one shard at level `L+1`. Lower write amplification,
  larger read fanout (up to `T × L`).
- **Leveled** (default for read-heavy): each level holds at most one
  shard (or one per non-overlapping PK range). When level `L` exceeds
  its size budget, merge it with the overlapping range in level `L+1`.
  Higher write amplification, smaller read fanout (`L`).

Configurable per table; default to leveled because reads are the hot
path per the profile.

### Phase 3 — bound the memtable too

`INLINE_CONSOLIDATE_THRESHOLD = 16` allows up to 16 runs visible to a
cursor before consolidation collapses them. For a strict bound, lower
to 1 with a tiny "write buffer" of incoming batches that gets merged
into the consolidated run on every batch:

```
Memtable layout under this plan:

  one consolidated_run: Rc<Batch>   ← merged-in result of every push
  pending: Option<Rc<Batch>>        ← single staging slot

  upsert_sorted_batch(b):
    if let Some(p) = pending.take():
        consolidated_run = merge(consolidated_run, p)   // amortise
    pending = Some(b)
```

The cursor sees `consolidated_run + pending + L shards` =
`2 + L` sources, total. With `L ≤ 7` that's `≤ 9` sources always.

Trade-off: this bumps memtable insert work from
`amortised O(batch_size)` to `O(consolidated_run_size + batch_size)` per
push, which can matter for small-batch high-frequency inserts. The
existing `INLINE_CONSOLIDATE_THRESHOLD` plan
(`5-inline-consolidate-threshold.md`) is the lighter-touch version of
this; that plan is a knob, this plan is a redesign.

### Phase 4 — the cursor's heap path is now bounded

With Phases 1–3, the cursor's `k` is bounded by a small constant
(call it `K_MAX`, configurable; default 10). The loser tree's per-row
comparator count is `⌈log₂ K_MAX⌉ = 4`. The whole concept of "the merge
gets slow as the table grows" is structurally eliminated.

## Code touch points

1. **Shard format** (`storage/shard_file.rs`): add `level: u8` to the
   header. Bump the format version. Provide a one-time migration path
   that opens v1 shards as level 0 and rewrites them on first
   compaction.

2. **`storage/shard_index.rs`**: group shards by level; expose
   `shards_at_level(l: u8) -> &[Rc<MappedShard>]`.

3. **`storage/compact.rs`**: replace the current trigger with a
   level-target policy. Drive it from `Table::compact_if_needed`
   (`table.rs:413` calls it on cursor creation), maybe also from a
   periodic background trigger.

4. **`storage/memtable.rs`**: implement the
   `consolidated_run + pending` layout described in Phase 3. The
   existing `runs: Vec<Batch>` field becomes `consolidated:
   Option<Rc<Batch>>` and `pending: Option<Rc<Batch>>` (or similar).
   `lookup_pk`, `find_positive_payload_row`, `get_snapshot`,
   `upsert_sorted_batch` all change.

5. **`storage/read_cursor.rs`**: no API change. The cursor doesn't
   know which level a shard is from; it sees a flat list of sources.
   The bounded fanout is a property of the *callers*
   (`Table::create_cursor`, `PartitionedTable::create_cursor`), not of
   the cursor.

6. **WAL replay**
   (`storage/wal.rs`,
   `crates/gnitz-engine/src/storage/recovery`):
   recovery replays writes; at the end of replay, the level-0 set
   contains whatever flushed during the run, plus a memtable rebuilt
   from the WAL tail. Level-0 may exceed its budget temporarily;
   compaction restores the invariant on the first `compact_if_needed`
   call. Document this as the recovery contract; do not block startup
   on compaction.

## Why this is a redesign, not a tuning

This plan changes the on-disk format (shard headers), the in-memory
memtable structure, and the compaction policy simultaneously. None of
those alone is enough — bounding the memtable without bounding shards
just moves the merge cost; bounding shards without bounding the
memtable leaves the per-cursor fanout uncapped.

Compatibility:

- Shard format change: forward-only. Old shards become level-0 on first
  open; they propagate upward via normal compaction.
- Memtable structure: in-memory only, no compatibility surface.
- Compaction policy: the new policy can be enabled per-table with a
  feature flag for safe rollout.

## Validation procedure

1. **Correctness**:
   - All existing unit tests pass (`make test`).
   - All e2e tests pass (`make e2e`) — they exercise long-running
     compaction and recovery.
   - Add a property test that does N random writes, then asserts
     `cursor.entries.len() ≤ K_MAX` for any `K_MAX = 10` configuration.
   - Recovery test: write, kill mid-flush, restart, verify post-recovery
     state is identical to a clean shutdown.

2. **Steady-state bench**:
   - Baseline (current code) → `BASELINE_DIR`.
   - Candidate (leveled mode) → `CANDIDATE_DIR`.
   - `uv run benchmarks/report.py`.

3. **Long-running bench** (the workload this plan targets):
   - Add a benchmark that runs for ~5 minutes of writes interleaved
     with reads, instead of the short bursts the existing benchmarks
     use. Without this benchmark, the plan can't be evaluated — the
     short benchmarks don't accumulate enough shards to expose the
     problem.

### Expected to win

- The long-running benchmark above.
- Read benchmarks after long ingest histories.

### Expected to lose (acceptably)

- `test_insert_bulk_throughput` and `test_insert_values[10]` —
  more rewrites per row. Magnitude depends on tier ratio `T` and
  level count.
- `test_update_pk_seek` — same reason.

### Decision criteria

- Read benchmarks after long ingest improve substantially (≥30%
  on the long-running bench).
- Write benchmarks regress by no more than 2× (typical for
  leveled compaction trade-offs).
- The recovery and durability tests are unbroken.

If the write regression exceeds 2× or any durability test breaks,
revert and revisit.

## Composes with

- **Loser tree** (`3-loser-tree-merge.md`): bounded fanout makes the
  loser tree's `O(1)`-per-row property exactly the win it advertises.
- **Skip the snapshot pre-merge** (`2-skip-snapshot-pre-merge.md`):
  Phase 3's memtable layout is even simpler than what that plan
  assumes (one consolidated run + one pending, vs. up-to-16 raw runs).
  Land Phase 3 *after* the snapshot-skip plan; it makes the snapshot
  question moot rather than answering it.
- **Single-row-per-PK** (`4-single-row-per-pk.md`): orthogonal. Land
  independently.

## What this plan does NOT cover

- **Per-table tuning** of `K_MAX`, `T`, or memtable buffer size. This
  plan picks one default; per-workload tuning is a follow-up.
- **Compaction concurrency.** Today compaction is synchronous on
  cursor creation (`Table::create_cursor` at `table.rs:413` calls
  `compact_if_needed` first). For workloads where compaction work
  exceeds inter-cursor idle time, that pattern stalls reads. A
  background compaction worker is a separate, larger plan
  (`background-rematerialisation.md`-shaped territory).
- **Compression.** Each level's shards could carry different
  compression settings (LZ4 at L0, zstd at higher levels). Out of
  scope.
- **Bloom filters per shard for point lookups.** Already addressed by
  the existing `BloomFilter` in `storage/bloom.rs` for the memtable;
  extending to shards is independent of this plan.
- **Universal compaction** (the third LSM compaction style, where
  the level abstraction is replaced with size buckets). Plausible
  alternative; not investigated here.
