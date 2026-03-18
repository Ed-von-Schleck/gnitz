# FLSM Storage Implementation Plan

## Context

gnitz/storage currently implements a single-level flat-shard LSM: flushed MemTable segments land
in a flat `ShardIndex`, shards overlap arbitrarily, and the planned compaction (compaction.md)
merges all shards into one. This achieves bounded read amplification but provides no write
amplification control and no level-based key isolation.

This plan upgrades storage to a real Fragmented Log-Structured Merge-tree (FLSM) in the style of
PebbleDB (SOSP 2017), adapted to gnitz's columnar Z-set semantics and incorporating Lazy Leveling
(Dostoevsky, SIGMOD 2018) at the last level. The plan assumes compaction.md has already been
implemented.

---

## Terminology

- **L0**: The flush-receiving level. Shards overlap freely, exactly as today.
- **Level (L1, L2, ...)**: A higher level in the hierarchy. Divided into guards.
- **Guard**: A key-range partition within one level. Identified by a `guard_key` (the first key
  assigned to it). Within a level, guards cover disjoint key ranges. Files *within* a guard may
  overlap — this is the "fragmentation" that gives FLSM its write-amplification advantage.
- **Vertical compaction**: Moving files from Ln to L(n+1), routing rows to the correct guard.
- **Horizontal compaction**: Merging overlapping files *within a single guard* without promoting
  to the next level. Bounds read amplification within guards.
- **Ghost**: A (pk, payload) tuple whose net Z-set weight is zero. In gnitz, ghosts are eliminated
  during any compaction at any level (Z-set advantage; PebbleDB must propagate tombstones to Lmax).

---

## Key Design Choices

- **MAX_LEVELS = 3** (L0 + L1 + L2): DBSP operator state is bounded; fewer levels means fewer I/O
  hops per read. Tunable constant.
- **Lazy Leveling at L2** (Dostoevsky 2018): L0 and L1 use tiering (up to K=4 files per guard);
  L2 (Lmax) uses leveling with Z=1 (exactly one file per guard, merged immediately on addition).
  For L=2, T=4: write amp O(L+T)=6 vs O(L·T)=8 for full leveling, same read/space bounds.
- **Guard keys sampled at compaction time**: on the first L0→L1 compaction, guard boundaries are
  taken from the sorted pk_min values of L0 shards. No in-memory guard bookkeeping between
  compactions.
- **Horizontal compaction at all levels**: DBSP trace tables are read every tick; bounding file
  count per guard throughout the hierarchy reduces point-lookup cost for the dominant workload.
- **Ghost elimination at any level**: Z-set merge sums weights; zero-weight rows are dropped
  immediately. Guards never accumulate cancelled-out rows.
- **LSNs propagated through compaction outputs**: gnitz uses LSNs for WAL recovery, not MVCC.
  Every output shard inherits `max_lsn = max(input shards' lsn)` to preserve the recovery
  boundary invariant.

---

## Constants

All live in `gnitz/storage/flsm.py`:

```python
MAX_LEVELS = 3               # Total levels: L0 (flat pool), L1 (guarded), L2 (leveled)
L0_COMPACT_THRESHOLD = 4     # L0 file count before triggering L0→L1 vertical compaction
GUARD_FILE_THRESHOLD = 4     # Files per guard before horizontal compaction (L0, L1)
LMAX_FILE_THRESHOLD = 1      # Files per guard at L2 (Lazy Leveling: one file per guard)
LEVEL_SIZE_RATIO = 4         # T: size ratio between levels
L1_TARGET_FILES = 16         # L1 total file count before triggering L1→L2 vertical compaction
                             # (= L0_COMPACT_THRESHOLD * LEVEL_SIZE_RATIO)

# Fluid LSM framing (Dostoevsky):
#   K = GUARD_FILE_THRESHOLD  — max runs at non-last levels
#   Z = LMAX_FILE_THRESHOLD   — max runs at Lmax (L2); Z=1 = Lazy Leveling
#   T = LEVEL_SIZE_RATIO      — size ratio between levels
#   L = MAX_LEVELS - 1        — levels above L0 (= 2: L1 and L2)
#
# Write amplification: O(L + T) = 6  vs full leveling O(L·T) = 8

# Sentinel for "no upper bound" in key-range comparisons. Using r_uint128(-1) is avoided
# per project convention (unsigned wrap semantics differ from signed -1 on 64-bit).
MAX_UINT128 = (r_uint128(0xFFFFFFFFFFFFFFFF) << 64) | r_uint128(0xFFFFFFFFFFFFFFFF)
```

These are module-level constants (RPython immutable after translation), not `_immutable_fields_`.

---

## Phase 1: Data Structures + Read Path ✓ DONE

**Commit**: db39246 — `LevelGuard`, `FLSMLevel`, `FLSMIndex`, `index_from_manifest` added to new
`flsm.py`; `metadata.py` gained optional `level`/`guard_key_lo`/`guard_key_hi` fields; `EphemeralTable`
switched from `ShardIndex` to `FLSMIndex`; `_build_cursor` uses `all_handles_for_cursor()`.
Tests: `test_flsm_data_structures`, `test_flsm_guard_read_path`.

---

## Phase 2: L0→L1 Vertical Compaction ✓ DONE

**Commit**: 1ad51ec — `manifest.py` bumped to VERSION=3 (ENTRY_SIZE=208, backward-compat V2 stride);
`_read_manifest_entry(fd, version)` version-aware. `flsm.py` added `populate_from_reader`,
`commit_l0_to_l1`, `run_compact`. `compactor.py` added `_find_guard_for_key`, `_merge_and_route`,
`compact_l0_to_l1`; deleted `execute_compaction`. `ephemeral_table._compact()` delegates to
`run_compact()`; `table.compact_if_needed()` calls `run_compact()` + `publish_new_version`;
`PersistentTable.__init__` recovery uses `populate_from_reader`.
Tests: `test_flsm_l0_to_l1_compaction` (guard structure + ghost elimination),
`test_flsm_manifest_persistence` (V3 round-trip); six existing compaction tests updated.

---

## Phase 3: Within-Guard Horizontal Compaction

**Goal**: When any guard in any level has more files than its level threshold, merge those files
within the guard into one output file (same level, same guard). L1 uses `GUARD_FILE_THRESHOLD`
(4); L2 (Lmax) uses `LMAX_FILE_THRESHOLD` (1, Lazy Leveling: one file per guard). This bounds
read amplification within a guard and, via Z-set weight summing, shrinks guards over time as
inserts and retractions cancel.

### New function: `compactor.compact_guard_horizontal`

```python
def compact_guard_horizontal(guard, output_path, schema, table_id, validate_checksums=False):
    """Merges all files within a single guard into one output file. Zero-weight rows dropped."""
    input_files = newlist_hint(len(guard.handles))
    for h in guard.handles:
        input_files.append(h.filename)
    compact_shards(input_files, output_path, schema, table_id, validate_checksums)
```

`compact_shards` already does weight-summing and zero-weight elimination — no changes needed.

### New methods on `FLSMIndex`

Both methods use `self.output_dir` and `self.validate_checksums` stored at construction;
no external state injection needed.

```python
def compact_guards_if_needed(self):
    """
    Scans all level guards. Runs horizontal compaction on any guard exceeding its threshold.
    L2 (Lmax) uses LMAX_FILE_THRESHOLD=1; all other levels use GUARD_FILE_THRESHOLD=4.
    """
    for level in self.levels:
        threshold = LMAX_FILE_THRESHOLD if level.level_num == MAX_LEVELS else GUARD_FILE_THRESHOLD
        g_idx = 0
        while g_idx < len(level.guards):
            guard = level.guards[g_idx]
            if guard.needs_horizontal_compact(threshold):
                self._compact_one_guard(level, g_idx)
            g_idx += 1

def _compact_one_guard(self, level, guard_idx):
    guard = level.guards[guard_idx]
    input_files = newlist_hint(len(guard.handles))
    guard_max_lsn = r_uint64(0)
    for h in guard.handles:
        input_files.append(h.filename)
        if h.lsn > guard_max_lsn:
            guard_max_lsn = h.lsn

    out_path = self.output_dir + "/hcomp_%d_L%d_%d_%d.db" % (
        self.table_id, level.level_num, intmask(guard.guard_key_lo), intmask(guard_max_lsn))

    try:
        compactor.compact_guard_horizontal(guard, out_path, self.schema,
                                            self.table_id, self.validate_checksums)
        new_handle = ShardHandle(out_path, self.schema, r_uint64(0), guard_max_lsn,
                                  self.validate_checksums)
        new_handles = newlist_hint(1)
        new_handles.append(new_handle)
        for h in guard.handles:
            h.close()
            self.ref_counter.release(h.filename)
            self.ref_counter.mark_for_deletion(h.filename)
        self.ref_counter.try_cleanup()
        self.ref_counter.acquire(new_handle.filename)
        guard.handles = new_handles
    except Exception as e:
        if os.path.exists(out_path):
            try: os.unlink(out_path)
            except OSError: pass
        raise e
```

### Integration

Extend `FLSMIndex.run_compact()` with the horizontal compaction step (replaces the Phase 2
stub comment):

```python
    # Phase 3: horizontal compaction — enforces GUARD_FILE_THRESHOLD at L1,
    # LMAX_FILE_THRESHOLD at L2 (Lazy Leveling invariant). Guards accumulate files only
    # through L0→L1 promotions, so this runs only when needs_compaction was True.
    self.compact_guards_if_needed()

    # Phase 4 adds: L1→L2 vertical if L1 is overfull
```

**No changes to `EphemeralTable` or `PersistentTable`**: both already call `run_compact()` which
now includes horizontal compaction.

**`PartitionedTable`**: no changes needed. Each partition is an `EphemeralTable` or
`PersistentTable` whose `compact_if_needed()` / `create_cursor()` delegates to `run_compact()`
automatically. The Phase 3 plan entry for `partitioned_table.py` is a no-op.

### Phase 3 Tests

Add `test_flsm_horizontal_compaction` in `storage_comprehensive_test.py`:

1. Create `EphemeralTable`. Force L1 to have a single guard with 6 manually added handles
   (10 distinct rows each). Call `index.compact_guards_if_needed(...)`. Assert guard has 1 handle.
2. Scan a cursor. Assert all 60 rows returned with correct weights.
3. Assert zero-weight rows are absent: insert key X (+1), retract key X (-1), place both in the
   guard, run horizontal compaction, assert X is absent.
4. Assert original input files are queued for deletion via the ref counter.

Add `test_flsm_lmax_horizontal_threshold`:

1. Manually add 2 handles to an L2 guard (`level_num == MAX_LEVELS`). Call
   `compact_guards_if_needed`. Assert L2 guard now has 1 handle (LMAX_FILE_THRESHOLD=1 fired).
2. Manually add 4 handles to an L1 guard. Assert it is NOT compacted (GUARD_FILE_THRESHOLD=4,
   trigger is >4).

Add `test_flsm_horizontal_does_not_promote`: after horizontal compaction, assert no new level was
created and guard count is unchanged.

---

## Phase 4: L1→L2 Vertical Compaction

**Goal**: When L1's total file count exceeds `L1_TARGET_FILES`, promote the overfull guard to L2.
Generalizes Phase 2's L0→L1 logic to Ln→L(n+1). Enables the full 3-level hierarchy.

### Algorithm

Pick the guard in L1 with the most files. For that guard G:
1. Find all guards in L2 whose key range overlaps G's range (using `find_guards_for_range`).
2. Merge G's files with each overlapping L2 guard's files via tournament tree.
3. Route output rows into L2 guard boundaries; create new L2 guards for any missing boundaries.
4. Remove G from L1 (fully promoted). Replace overlapping L2 guards' handles with outputs.
5. Immediately enforce Lazy Leveling: any L2 guard that now has >1 file is horizontally
   compacted to 1 (Z=1 invariant).

One guard is promoted per compaction event. Write amplification is bounded to O(guard_size × 2)
per event, not O(level_size).

### New function: `compactor.compact_guard_vertical`

```python
def compact_guard_vertical(src_guard, dest_guards, output_dir, schema, table_id,
                            dest_level_num, lsn_tag, validate_checksums=False):
    """
    Merges src_guard's files with each overlapping dest_guard's files.
    Returns list of (dest_guard_key_lo, dest_guard_key_hi, output_filename).

    dest_guards: list of LevelGuard that overlap with src_guard's key range.
    dest_level_num: level number of the destination (used in output filenames).
    """
    all_input_files = newlist_hint(len(src_guard.handles))
    for h in src_guard.handles:
        all_input_files.append(h.filename)
    for dg in dest_guards:
        for h in dg.handles:
            all_input_files.append(h.filename)

    if dest_guards:
        guard_keys = newlist_hint(len(dest_guards))
        for dg in dest_guards:
            guard_keys.append((dg.guard_key_lo, dg.guard_key_hi))
    else:
        guard_keys = newlist_hint(1)
        guard_keys.append((src_guard.guard_key_lo, src_guard.guard_key_hi))

    return _merge_and_route(all_input_files, guard_keys, output_dir, table_id,
                             dest_level_num, lsn_tag, schema, validate_checksums)
```

### New method: `FLSMIndex.compact_guard_vertical_if_needed`

Uses `self.output_dir` and `self.validate_checksums` throughout; no external state injection.

```python
def compact_guard_vertical_if_needed(self, src_level_num):
    """Picks the overfull guard in src_level and promotes it to src_level+1."""
    src_level = self.levels[src_level_num - 1]
    if src_level_num >= MAX_LEVELS:
        # Already at Lmax; fall back to horizontal compaction
        self.compact_guards_if_needed()
        return

    # Pick the guard with the most files
    worst_idx = -1
    worst_count = 0
    for i in range(len(src_level.guards)):
        n = len(src_level.guards[i].handles)
        if n > worst_count:
            worst_count = n
            worst_idx = i
    if worst_idx == -1 or worst_count <= 1:
        return

    src_guard = src_level.guards[worst_idx]
    dest_level = self.get_or_create_level(src_level_num + 1)

    # Collect aggregate max_lsn from src + dest handles BEFORE modifying any files.
    vert_max_lsn = r_uint64(0)
    for h in src_guard.handles:
        if h.lsn > vert_max_lsn:
            vert_max_lsn = h.lsn

    src_min = src_guard.guard_key()
    if worst_idx + 1 < len(src_level.guards):
        src_max_bound = src_level.guards[worst_idx + 1].guard_key() - r_uint128(1)
    else:
        src_max_bound = MAX_UINT128

    dest_guard_indices = dest_level.find_guards_for_range(src_min, src_max_bound)
    dest_guards = newlist_hint(len(dest_guard_indices))
    for di in dest_guard_indices:
        dg = dest_level.guards[di]
        dest_guards.append(dg)
        for h in dg.handles:
            if h.lsn > vert_max_lsn:
                vert_max_lsn = h.lsn

    lsn_tag = intmask(vert_max_lsn)
    results = compactor.compact_guard_vertical(
        src_guard, dest_guards, output_dir, self.schema, self.table_id,
        src_level_num + 1, lsn_tag, validate_checksums)

    # Close and schedule deletion of all input handles
    for h in src_guard.handles:
        h.close()
        self.ref_counter.release(h.filename)
        self.ref_counter.mark_for_deletion(h.filename)
    for dg in dest_guards:
        for h in dg.handles:
            h.close()
            self.ref_counter.release(h.filename)
            self.ref_counter.mark_for_deletion(h.filename)

    # Remove src_guard from Ln (fully promoted)
    new_src_guards = newlist_hint(len(src_level.guards) - 1)
    for i in range(len(src_level.guards)):
        if i != worst_idx:
            new_src_guards.append(src_level.guards[i])
    src_level.guards = new_src_guards

    # Install outputs in dest_level
    for (dest_gk_lo, dest_gk_hi, out_fn) in results:
        if not os.path.exists(out_fn):
            continue
        new_handle = ShardHandle(out_fn, self.schema, r_uint64(0), vert_max_lsn,
                                  self.validate_checksums)
        dest_gk = (r_uint128(dest_gk_hi) << 64) | r_uint128(dest_gk_lo)
        g_idx = dest_level.find_guard_idx(dest_gk)
        if g_idx == -1 or dest_level.guards[g_idx].guard_key() != dest_gk:
            new_guard = LevelGuard(dest_gk_lo, dest_gk_hi)
            dest_level.insert_guard_sorted(new_guard)
            g_idx = dest_level.find_guard_idx(dest_gk)
        dest_level.guards[g_idx].add_handle(self.ref_counter, new_handle)

    self.ref_counter.try_cleanup()
    self._update_flags()

    # Lazy Leveling enforcement: L2 guards must hold exactly one file (Z=1).
    # The vertical compaction may have added a second file to some L2 guards.
    # Run horizontal compaction on any violating L2 guards immediately, so
    # PersistentTable needs only one manifest publish after this call returns.
    if dest_level.level_num == MAX_LEVELS:
        for g_idx in range(len(dest_level.guards)):
            if dest_level.guards[g_idx].needs_horizontal_compact(LMAX_FILE_THRESHOLD):
                self._compact_one_guard(dest_level, g_idx)
```

### Integration: triggering L1→L2

`FLSMIndex.run_compact()` (introduced in Phase 2, extended each phase) is the single integration
point. Extend it to cover L1→L2:

```python
def run_compact(self):
    # Phase 2: L0→L1 vertical
    if self.needs_compaction:
        self.commit_l0_to_l1()
    # Phase 3: horizontal compaction within guards
    self.compact_guards_if_needed()
    # Phase 4: L1→L2 vertical if L1 is too large
    if len(self.levels) > 0:
        l1 = self.levels[0]
        if l1.total_file_count() > L1_TARGET_FILES:
            self.compact_guard_vertical_if_needed(1)
```

`EphemeralTable._compact()` and `PersistentTable.compact_if_needed()` remain unchanged from Phase
3 — they already call `self.index.run_compact()`. The Lazy Leveling enforcement within
`compact_guard_vertical_if_needed` (the per-L2-guard horizontal pass) completes before
`run_compact` returns, so `PersistentTable` needs only one `publish_new_version` call after the
whole sequence.

### Phase 4 Tests

Add `test_flsm_multilevel_compaction` in `storage_comprehensive_test.py`:

1. Create `EphemeralTable` with a very small arena (2048 bytes). Insert 200 rows in batches →
   many flushes, multiple L0→L1 cycles, eventually L1→L2.
2. Assert `len(index.levels) >= 2` (L2 created). Assert L2 has at least one guard.
3. Scan all rows via cursor. Assert correct count and weights across all 3 levels.
4. Assert `has_pk` returns True for each inserted key.
5. Assert guard keys are non-overlapping within L1 and within L2.

Add `test_flsm_lazy_leveling_lmax`: after any L1→L2 vertical compaction, assert every L2 guard
has exactly 1 handle. Repeat after a second L0→L1→L2 cycle to confirm Z=1 is enforced on
re-compaction.

Add `test_flsm_guard_isolation` (regression): after L1→L2 compaction, insert rows that only touch
one guard's key range. Trigger another compaction. Assert the untouched L1 guard's handle
filenames are unchanged (only the affected guard was recompacted).

---

## File Change Summary

| File | Phase | Change |
|------|-------|--------|
| `gnitz/storage/flsm.py` | 1, 2 | **New file** (Phase 1): `LevelGuard`, `FLSMLevel`, `FLSMIndex` + constants; `MAX_UINT128` sentinel. **Phase 2**: adds `FLSMIndex.populate_from_reader` (shared recovery logic); `FLSMIndex.run_compact`, `FLSMIndex.commit_l0_to_l1`; `index_from_manifest` becomes thin wrapper over `populate_from_reader` |
| `gnitz/storage/metadata.py` | 1 | Add `level`, `guard_key_lo`, `guard_key_hi` to `ManifestEntry` (defaulted) |
| `gnitz/storage/index.py` | 1 | Move `index_from_manifest` to `flsm.py`; `index.py` keeps no `flsm` import; delete `execute_compaction` in Phase 2 |
| `gnitz/storage/ephemeral_table.py` | 1, 2 | Construct `FLSMIndex(…, directory, validate_checksums)`; update `_build_cursor` → `all_handles_for_cursor()`; `_compact()` → 2-line delegate to `self.index.run_compact()` |
| `gnitz/storage/table.py` | 2 | `compact_if_needed` replaced with 5-line wrapper calling `run_compact` + `publish_new_version`; `__init__` manifest recovery loop replaced with `self.index.populate_from_reader(self.table_id, reader)` |
| `gnitz/storage/compactor.py` | 2, 3, 4 | Add `r_ulonglonglong as r_uint128` to arithmetic imports; add `_merge_and_route`, `compact_l0_to_l1`, `compact_guard_horizontal`, `compact_guard_vertical`; delete `execute_compaction` in Phase 2 to break circular import |
| `gnitz/storage/manifest.py` | 2 | Bump `VERSION=3`, `ENTRY_SIZE=208`; add `ENTRY_SIZE_V2=184`, `VERSION_LEGACY=2`; `_read_manifest_entry(fd, version)` gains version param and reads correct stride per version; `iterate_entries` passes `self.version` |
| `gnitz/storage/partitioned_table.py` | — | **No changes**: each partition already delegates to `run_compact()` via `EphemeralTable._compact()` |
| `rpython_tests/storage_comprehensive_test.py` | all | New test functions per phase |

---

## What Each Phase Delivers

| Phase | What you have after | Key test |
|-------|---------------------|----------|
| 1 | `FLSMIndex` replaces `ShardIndex`; guard/level data structures exist but are empty; read path traverses L1+ when guards are present | guard binary search unit tests; manually inject L1 guards and verify reads; all existing tests pass |
| 2 | L0→L1 vertical compaction; first guard-partitioned structure on disk; manifest V3; ghost elimination at L1 | auto-compact, verify guard structure, verify manifest round-trip, verify ghost elimination |
| 3 | Files within guards bounded; Lazy Leveling Z=1 enforced at L2 | guard horizontal compact, verify file count + data integrity, verify level-aware thresholds |
| 4 | Full 3-level hierarchy; guard-targeted L1→L2 promotion; write amplification bounded | multilevel data round-trip, guard isolation regression, Lazy Leveling invariant after multiple cycles |
