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
```

These are module-level constants (RPython immutable after translation), not `_immutable_fields_`.

---

## Phase 1: Data Structures + Read Path

**Goal**: Introduce `LevelGuard`, `FLSMLevel`, and `FLSMIndex`. Replace `ShardIndex` everywhere.
Extend the read path (`find_all_shards_and_indices`, `all_handles_for_cursor`, `_build_cursor`)
to traverse L1+ guards when present. Since levels start empty, all existing tests pass unchanged.
New tests manually inject L1 guards to verify the guard-aware read path works before any
compaction code is written.

### New file: `gnitz/storage/flsm.py`

#### `LevelGuard`

```python
class LevelGuard(object):
    """A guard: a key-range partition within one level.

    guard_key: the minimum key assigned to this guard (stored as lo/hi u64 pair).
    Covers [guard_key, next_guard.guard_key) or [guard_key, ∞) if last.
    handles: list of ShardHandle. Files within a guard may overlap (FLSM fragmentation).
    """
    def __init__(self, guard_key_lo, guard_key_hi):
        self.guard_key_lo = guard_key_lo   # r_uint64
        self.guard_key_hi = guard_key_hi   # r_uint64
        self.handles = newlist_hint(4)

    def guard_key(self):
        return (r_uint128(self.guard_key_hi) << 64) | r_uint128(self.guard_key_lo)

    def needs_horizontal_compact(self, threshold):
        return len(self.handles) > threshold

    def add_handle(self, ref_counter, handle):
        ref_counter.acquire(handle.filename)
        self.handles.append(handle)

    def close_all(self, ref_counter):
        for h in self.handles:
            h.close()
            ref_counter.release(h.filename)
        self.handles = newlist_hint(0)
```

#### `FLSMLevel`

```python
class FLSMLevel(object):
    """One leveled tier (L1, L2, ...).

    guards is sorted ascending by guard_key. Guards in a level are non-overlapping.
    """
    def __init__(self, level_num):
        self.level_num = level_num
        self.guards = newlist_hint(8)

    def find_guard_idx(self, key):
        """Binary search: index of guard whose range covers key. -1 if level is empty."""
        n = len(self.guards)
        if n == 0:
            return -1
        lo, hi = 0, n - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self.guards[mid].guard_key() <= key:
                lo = mid
            else:
                hi = mid - 1
        if self.guards[lo].guard_key() <= key:
            return lo
        return -1

    def find_guards_for_range(self, range_min, range_max):
        """Returns list of guard indices whose key range overlaps [range_min, range_max]."""
        n = len(self.guards)
        result = newlist_hint(4)
        for i in range(n):
            gk = self.guards[i].guard_key()
            if gk > range_max:
                break
            if i + 1 < n:
                next_gk = self.guards[i + 1].guard_key()
            else:
                next_gk = r_uint128(-1)
            if next_gk > range_min:
                result.append(i)
        return result

    def insert_guard_sorted(self, guard):
        """Insert a new guard into the sorted guards list (rebuild — RPython has no list.insert)."""
        gk = guard.guard_key()
        new_guards = newlist_hint(len(self.guards) + 1)
        inserted = False
        for g in self.guards:
            if not inserted and g.guard_key() > gk:
                new_guards.append(guard)
                inserted = True
            new_guards.append(g)
        if not inserted:
            new_guards.append(guard)
        self.guards = new_guards

    def total_file_count(self):
        n = 0
        for g in self.guards:
            n += len(g.handles)
        return n

    def close_all(self, ref_counter):
        for g in self.guards:
            g.close_all(ref_counter)
        self.guards = newlist_hint(0)
```

#### `FLSMIndex`

```python
class FLSMIndex(object):
    """FLSM index: L0 flat pool + L1..L(MAX_LEVELS) guard-partitioned levels.

    Backward-compatible API shim properties:
      .handles          → self.l0_handles  (for compaction.md code paths)
      .needs_compaction → self.needs_l0_compact
    """
    def __init__(self, table_id, schema, ref_counter):
        self.table_id = table_id
        self.schema = schema
        self.ref_counter = ref_counter
        self.l0_handles = newlist_hint(8)
        self.levels = newlist_hint(MAX_LEVELS - 1)  # L1..Lmax
        self.needs_l0_compact = False

    # Backward-compat shims (used by compaction.md code and existing tests)
    def _get_handles(self):       return self.l0_handles
    handles = property(_get_handles)
    def _get_needs_compact(self): return self.needs_l0_compact
    needs_compaction = property(_get_needs_compact)

    # --- L0 management -------------------------------------------------------

    def add_handle(self, handle):
        """Backward-compat: adds to L0."""
        self.add_l0_handle(handle)

    def add_l0_handle(self, handle):
        self.ref_counter.acquire(handle.filename)
        self.l0_handles.append(handle)
        self._sort_l0()
        self._update_flags()

    def _sort_l0(self):
        for i in range(1, len(self.l0_handles)):
            h = self.l0_handles[i]
            k = h.get_min_key()
            j = i - 1
            while j >= 0 and self.l0_handles[j].get_min_key() > k:
                self.l0_handles[j + 1] = self.l0_handles[j]
                j -= 1
            self.l0_handles[j + 1] = h

    def _update_flags(self):
        self.needs_l0_compact = len(self.l0_handles) > L0_COMPACT_THRESHOLD

    def replace_handles(self, old_filenames, new_handle):
        """Backward-compat: compaction.md's _compact() calls this on L0."""
        new_list = newlist_hint(len(self.l0_handles))
        for h in self.l0_handles:
            superseded = False
            for old_fn in old_filenames:
                if h.filename == old_fn:
                    superseded = True
                    break
            if superseded:
                h.close()
                self.ref_counter.release(h.filename)
            else:
                new_list.append(h)
        self.ref_counter.acquire(new_handle.filename)
        new_list.append(new_handle)
        self.l0_handles = new_list
        self._sort_l0()
        self._update_flags()

    # --- Level management ----------------------------------------------------

    def get_or_create_level(self, level_num):
        """Return FLSMLevel for level_num (1-based), creating missing levels."""
        idx = level_num - 1
        while len(self.levels) <= idx:
            self.levels.append(FLSMLevel(len(self.levels) + 1))
        return self.levels[idx]

    # --- Read API (L0 + all levels) ------------------------------------------

    def find_all_shards_and_indices(self, key):
        results = newlist_hint(8)

        # L0: flat pool, all shards are candidates
        for h in self.l0_handles:
            if h.get_min_key() <= key <= h.get_max_key():
                if h.xor8_filter is not None and not h.xor8_filter.may_contain(key):
                    continue
                row_idx = h.view.find_row_index(key)
                if row_idx != -1:
                    results.append((h, row_idx))

        # L1+: binary search to the covering guard, XOR8-filter within it
        for level in self.levels:
            g_idx = level.find_guard_idx(key)
            if g_idx == -1:
                continue
            guard = level.guards[g_idx]
            for h in guard.handles:
                if h.get_min_key() <= key <= h.get_max_key():
                    if h.xor8_filter is not None and not h.xor8_filter.may_contain(key):
                        continue
                    row_idx = h.view.find_row_index(key)
                    if row_idx != -1:
                        results.append((h, row_idx))

        return results

    def all_handles_for_cursor(self):
        n = len(self.l0_handles)
        for level in self.levels:
            n += level.total_file_count()
        result = newlist_hint(n)
        for h in self.l0_handles:
            result.append(h)
        for level in self.levels:
            for guard in level.guards:
                for h in guard.handles:
                    result.append(h)
        return result

    # --- Metadata ------------------------------------------------------------

    def get_metadata_list(self):
        """Produces ManifestEntry list for all levels."""
        result = newlist_hint(64)
        for h in self.l0_handles:
            result.append(ManifestEntry(self.table_id, h.filename,
                h.get_min_key(), h.get_max_key(), h.min_lsn, h.lsn,
                level=0, guard_key_lo=r_uint64(0), guard_key_hi=r_uint64(0)))
        for level in self.levels:
            for guard in level.guards:
                for h in guard.handles:
                    result.append(ManifestEntry(self.table_id, h.filename,
                        h.get_min_key(), h.get_max_key(), h.min_lsn, h.lsn,
                        level=level.level_num,
                        guard_key_lo=guard.guard_key_lo,
                        guard_key_hi=guard.guard_key_hi))
        return result

    def max_lsn(self):
        """Returns the highest max_lsn across all shards in all levels."""
        result = r_uint64(0)
        for h in self.l0_handles:
            if h.lsn > result:
                result = h.lsn
        for level in self.levels:
            for guard in level.guards:
                for h in guard.handles:
                    if h.lsn > result:
                        result = h.lsn
        return result

    # --- Cleanup -------------------------------------------------------------

    def close_all(self):
        for h in self.l0_handles:
            h.close()
            self.ref_counter.release(h.filename)
        self.l0_handles = newlist_hint(0)
        for level in self.levels:
            level.close_all(self.ref_counter)
        self.levels = newlist_hint(0)
```

### Changes to `gnitz/storage/metadata.py`

Add three new fields to `ManifestEntry`. All existing call sites pass the new parameters with
default values `level=0, guard_key_lo=r_uint64(0), guard_key_hi=r_uint64(0)`:

```python
def __init__(self, table_id, shard_filename, min_key, max_key, min_lsn, max_lsn,
             level=0, guard_key_lo=r_uint64(0), guard_key_hi=r_uint64(0)):
    # ... existing assignments ...
    self.level = level
    self.guard_key_lo = r_uint64(guard_key_lo)
    self.guard_key_hi = r_uint64(guard_key_hi)
```

The manifest serialization format is NOT extended yet (that happens in Phase 2). The new fields
exist on the object but are ignored during read/write in Phase 1.

### Changes to `gnitz/storage/index.py`

- Import `FLSMIndex` from `flsm.py`.
- Update `index_from_manifest` to construct and return an `FLSMIndex` instead of `ShardIndex`,
  placing all loaded handles into L0 (since all existing manifests are `level=0`). Phase 2
  updates this function to route entries to the correct level/guard from V3 manifest data.

### Changes to `gnitz/storage/ephemeral_table.py`

- Replace `self.index = index.ShardIndex(...)` with `self.index = flsm.FLSMIndex(...)`.
- Update `_build_cursor()` to use `all_handles_for_cursor()`:

```python
def _build_cursor(self):
    all_handles = self.index.all_handles_for_cursor()
    cs = newlist_hint(1 + len(all_handles))
    cs.append(cursor.MemTableCursor(self.memtable))
    for h in all_handles:
        cs.append(cursor.ShardCursor(h.view))
    return cursor.UnifiedCursor(self.schema, cs)
```

- `_compact()` (from compaction.md) uses `self.index.handles` and `self.index.needs_compaction`,
  both still work via the compatibility shims. No other changes needed.

### Changes to `gnitz/storage/table.py`

- Same import and `FLSMIndex` replacement for `PersistentTable`.
- Same `_build_cursor()` update (delegates to `all_handles_for_cursor()`).
- Replace the inline manifest-reconstruction loop in `PersistentTable.__init__` with a call to
  `index_from_manifest`. The inline loop calls `self.index.add_handle(h)` which adds everything
  to L0 via the shim — correct for Phase 1, but Phase 2 upgrades `index_from_manifest` to route
  V3 entries to the correct level/guard. Using `index_from_manifest` here avoids two diverging
  reconstruction paths:

```python
# Replace the inline loop in PersistentTable.__init__:
if self.manifest_manager.exists():
    self.index = index.index_from_manifest(
        manifest_path, self.table_id, schema,
        self.ref_counter, validate_checksums,
    )
    reader = self.manifest_manager.load_current()
    try:
        self.current_lsn = reader.global_max_lsn + r_uint64(1)
    finally:
        reader.close()
else:
    self.current_lsn = r_uint64(1)
```

Note: `self.index` is initialized by `EphemeralTable.__init__` (called first) as an empty
`FLSMIndex`. `index_from_manifest` constructs a new `FLSMIndex` and overwrites it. Safe: no
handles have been acquired on the empty index yet.

### Phase 1 Tests

Add `test_flsm_data_structures` in `rpython_tests/storage_comprehensive_test.py`:

1. Construct `LevelGuard(guard_key_lo=r_uint64(100), guard_key_hi=r_uint64(0))`. Assert
   `guard.guard_key() == r_uint128(100)`.
2. Construct `FLSMLevel(1)` with 4 guards at keys 0, 100, 200, 300 using
   `insert_guard_sorted`. Assert `find_guard_idx(50)==0`, `find_guard_idx(100)==1`,
   `find_guard_idx(250)==2`, `find_guard_idx(350)==3`, `find_guard_idx(0)==0`. Assert empty
   level returns -1.
3. Assert `find_guards_for_range(r_uint128(80), r_uint128(210))` returns `[0, 1, 2]`.
4. Construct `FLSMIndex`. Verify `index.handles` returns `index.l0_handles` (shim works).
   Verify `index.needs_compaction` returns `index.needs_l0_compact` (shim works).
5. Run all existing `storage_comprehensive_test` cases — all must pass unchanged.

Add `test_flsm_guard_read_path`:

1. Create an `EphemeralTable`. Insert 20 rows and flush 5 times → 5 L0 handles.
2. Manually call `index.get_or_create_level(1)`. Build 2 guards at keys 0 and 1000.
   Add the first 2 L0 handles to guard[0] and the last 2 to guard[1]. Leave 1 handle in L0.
3. Manually set `index.l0_handles` to contain only the 1 unaffected handle.
4. Call `create_cursor()`. Assert cursor yields all 20 rows with correct weights.
5. For each inserted PK, call `has_pk()`. Assert True for all 20.
6. For a PK not inserted, call `has_pk()`. Assert False.
7. Assert `all_handles_for_cursor()` returns exactly 5 handles total.

---

## Phase 2: L0→L1 Vertical Compaction

**Goal**: When L0 overflows (`needs_l0_compact == True`), merge and route L0 data into L1 guards.
Replaces `compact_shards()` (merge-all-to-one) with guard-partitioned output. Extends the manifest
binary format so L1 guard info survives `PersistentTable` restart.

### LSN invariant

`PersistentTable` uses LSNs for WAL recovery. The critical invariant:

> After compaction that deletes input shard files, the manifest must reflect output shard files
> with `max_lsn ≥ highest LSN of any deleted input shard.

If broken, crash recovery may skip WAL blocks and lose data. Fix: collect `l0_max_lsn` before
modifying any files; write it into every output shard handle and into the manifest header. The WAL
is already truncated to zero at flush time, so compaction never needs to touch the WAL.

### Guard selection algorithm

**L1 empty (first L0→L1 compaction):** Use the sorted pk_min of each L0 shard as guard keys.
Guard i covers `[l0_sorted[i].pk_min, l0_sorted[i+1].pk_min)`. Last guard covers to ∞.

**L1 has existing guards (subsequent compactions):** Route L0 rows into existing guards using
their guard keys as boundaries. Rows below the first guard key go into guard[0].

In both cases, routing is: for each row with pk `k`, find the largest `i` such that
`guard_keys[i] ≤ k`. Write the row to output shard `i`.

**Filename uniqueness**: embed `lsn_tag` (monotonically increasing integer) in output filenames
to prevent collisions when the same guard is compacted again:
- PersistentTable: `lsn_tag = intmask(l0_max_lsn)`
- EphemeralTable: `lsn_tag = self._flush_seq` (incremented by caller before the call)

### Helper: `_merge_and_route` in `compactor.py`

Both L0→L1 and L1→L2 compaction need a tournament-tree merge that routes rows to multiple output
shards. Extract as a private helper:

```python
def _merge_and_route(input_files, guard_keys, output_dir, table_id, level_num, lsn_tag,
                     schema, validate_checksums=False):
    """
    Tournament-tree merge over input_files. Routes each row to the output shard
    corresponding to the guard_keys partition it falls in. Drops zero-weight rows (Z-set).

    guard_keys: list of (lo, hi) tuples in sorted ascending order.
    Returns: list of (guard_key_lo, guard_key_hi, output_filename) for non-empty outputs.
    """
    num_guards = len(guard_keys)
    writers = newlist_hint(num_guards)
    out_filenames = newlist_hint(num_guards)
    for i in range(num_guards):
        writers.append(TableShardWriter(schema, table_id))
        out_filenames.append(
            output_dir + "/shard_%d_%d_L%d_G%d.db" % (table_id, lsn_tag, level_num, i)
        )

    views = [TableShardView(fn, schema, validate_checksums) for fn in input_files]
    cursors_list = [ShardCursor(v) for v in views]
    tree = TournamentTree(cursors_list, schema)

    try:
        while not tree.is_exhausted():
            min_key = tree.get_min_key()
            num_at_min = tree.get_all_indices_at_min()

            net_weight = r_int64(0)
            i = 0
            while i < num_at_min:
                net_weight += cursors_list[tree._min_indices[i]].weight()
                i += 1

            if net_weight != r_int64(0):
                guard_idx = _find_guard_for_key(guard_keys, min_key)
                exemplar = cursors_list[tree._min_indices[0]]
                acc = exemplar.get_accessor()
                writers[guard_idx].add_row_from_accessor(min_key, net_weight, acc)

            i = 0
            while i < num_at_min:
                tree.advance_cursor_by_index(tree._min_indices[i])
                i += 1

        for i in range(num_guards):
            writers[i].finalize(out_filenames[i])
    finally:
        tree.close()
        for v in views:
            v.close()

    results = newlist_hint(num_guards)
    for i in range(num_guards):
        if os.path.exists(out_filenames[i]):
            results.append((guard_keys[i][0], guard_keys[i][1], out_filenames[i]))
    return results
```

`_find_guard_for_key(guard_keys, key)` — binary search returning the index `i` such that
`guard_keys[i] ≤ key < guard_keys[i+1]` (last guard wins for `key ≥ guard_keys[-1]`).

### New function: `compactor.compact_l0_to_l1`

```python
def compact_l0_to_l1(flsm_index, output_dir, schema, table_id, lsn_tag,
                     validate_checksums=False):
    """
    Merges all L0 shards and routes rows into L1 guards.
    Returns list of (guard_key_lo, guard_key_hi, output_filename).
    """
    l0_handles = flsm_index.l0_handles
    if not l0_handles:
        return []

    l1 = flsm_index.get_or_create_level(1)
    guard_keys = newlist_hint(len(l1.guards) if l1.guards else len(l0_handles))
    if not l1.guards:
        for h in l0_handles:
            guard_keys.append((h.pk_min_lo, h.pk_min_hi))
    else:
        for g in l1.guards:
            guard_keys.append((g.guard_key_lo, g.guard_key_hi))

    input_files = newlist_hint(len(l0_handles))
    for h in l0_handles:
        input_files.append(h.filename)

    return _merge_and_route(input_files, guard_keys, output_dir, table_id, 1, lsn_tag,
                             schema, validate_checksums)
```

### New method: `FLSMIndex.commit_l0_to_l1`

```python
def commit_l0_to_l1(self, l0_input_filenames, guard_outputs, max_lsn, validate_checksums):
    """
    Atomically clears compacted L0 handles and installs output handles into L1 guards.

    guard_outputs: list of (guard_key_lo, guard_key_hi, filename)
    max_lsn: aggregate max LSN of compacted L0 handles (r_uint64(0) for EphemeralTable).
    """
    new_l0 = newlist_hint(len(self.l0_handles))
    for h in self.l0_handles:
        found = False
        for fn in l0_input_filenames:
            if h.filename == fn:
                found = True
                break
        if found:
            h.close()
            self.ref_counter.release(h.filename)
        else:
            new_l0.append(h)
    self.l0_handles = new_l0

    l1 = self.get_or_create_level(1)
    for (gk_lo, gk_hi, filename) in guard_outputs:
        new_handle = ShardHandle(filename, self.schema, r_uint64(0), max_lsn,
                                  validate_checksums)
        gk = (r_uint128(gk_hi) << 64) | r_uint128(gk_lo)
        g_idx = l1.find_guard_idx(gk)
        if g_idx == -1 or l1.guards[g_idx].guard_key() != gk:
            new_guard = LevelGuard(gk_lo, gk_hi)
            l1.insert_guard_sorted(new_guard)
            g_idx = l1.find_guard_idx(gk)
        l1.guards[g_idx].add_handle(self.ref_counter, new_handle)

    self._update_flags()
```

### Changes to `gnitz/storage/ephemeral_table.py`

Replace `_compact()` (from compaction.md):

```python
def _compact(self):
    if len(self.index.l0_handles) <= 1:
        return

    l0_input_files = newlist_hint(len(self.index.l0_handles))
    for h in self.index.l0_handles:
        l0_input_files.append(h.filename)

    self._flush_seq += 1
    lsn_tag = self._flush_seq

    try:
        guard_outputs = compactor.compact_l0_to_l1(
            self.index, self.directory, self.schema, self.table_id, lsn_tag,
            self.validate_checksums,
        )
        self.index.commit_l0_to_l1(l0_input_files, guard_outputs,
                                    r_uint64(0), self.validate_checksums)
        for fn in l0_input_files:
            self.ref_counter.mark_for_deletion(fn)
        self.ref_counter.try_cleanup()
    except Exception as e:
        for _, _, fn in guard_outputs:
            if os.path.exists(fn):
                try: os.unlink(fn)
                except OSError: pass
        raise e
```

### Changes to `gnitz/storage/table.py`

Replace `compact_if_needed()` (from compaction.md):

```python
def compact_if_needed(self):
    if not self.index.needs_l0_compact:
        return

    l0_input_files = newlist_hint(len(self.index.l0_handles))
    l0_max_lsn = r_uint64(0)
    for h in self.index.l0_handles:
        l0_input_files.append(h.filename)
        if h.lsn > l0_max_lsn:
            l0_max_lsn = h.lsn
    lsn_tag = intmask(l0_max_lsn)

    try:
        guard_outputs = compactor.compact_l0_to_l1(
            self.index, self.directory, self.schema, self.table_id, lsn_tag,
            self.validate_checksums,
        )
        self.index.commit_l0_to_l1(l0_input_files, guard_outputs,
                                    l0_max_lsn, self.validate_checksums)
        for fn in l0_input_files:
            self.ref_counter.mark_for_deletion(fn)
        self.ref_counter.try_cleanup()
        self.manifest_manager.publish_new_version(
            self.index.get_metadata_list(), l0_max_lsn,
        )
    except Exception as e:
        for _, _, fn in guard_outputs:
            if os.path.exists(fn): os.unlink(fn)
        raise e
```

### Manifest format extension (PersistentTable only)

EphemeralTable has no manifest; skip this section for ephemeral.

**`gnitz/storage/manifest.py`**:

- Bump `VERSION = 3`. Keep `VERSION_LEGACY = 2`.
- Change `ENTRY_SIZE = 208` (was 184). New trailing 24 bytes:
  - `[184–191]` level_num (u64)
  - `[192–199]` guard_key_lo (u64)
  - `[200–207]` guard_key_hi (u64)
- `_write_manifest_entry`: write the three new fields.
- `_read_manifest_entry`: if version == 2 (legacy), return with `level=0, guard_key_lo=0,
  guard_key_hi=0`. If version == 3, read all fields.

**`gnitz/storage/index.py` — `index_from_manifest`**:

Update to route entries to the correct level/guard:

```python
def index_from_manifest(manifest_path, table_id, schema, ref_counter, validate_checksums=False):
    idx = FLSMIndex(table_id, schema, ref_counter)
    reader = ManifestReader(manifest_path)
    try:
        for entry in reader.iterate_entries():
            if entry.table_id != table_id:
                continue
            handle = ShardHandle(entry.shard_filename, schema, entry.min_lsn, entry.max_lsn,
                                  validate_checksums)
            if entry.level == 0:
                idx.add_l0_handle(handle)
            else:
                level = idx.get_or_create_level(entry.level)
                gk = (r_uint128(entry.guard_key_hi) << 64) | r_uint128(entry.guard_key_lo)
                g_idx = level.find_guard_idx(gk)
                if g_idx == -1 or level.guards[g_idx].guard_key() != gk:
                    g = LevelGuard(entry.guard_key_lo, entry.guard_key_hi)
                    level.insert_guard_sorted(g)
                    g_idx = level.find_guard_idx(gk)
                level.guards[g_idx].add_handle(ref_counter, handle)
    finally:
        reader.close()
    return idx
```

### Phase 2 Tests

Add `test_flsm_l0_to_l1_compaction` in `storage_comprehensive_test.py`:

1. Create `EphemeralTable` with arena size 4096 bytes. Insert 5 rows and flush after each → 5 L0
   handles. Trigger `create_cursor()` → auto-compact via `_compact()`.
2. Assert `len(index.l0_handles) == 0`. Assert `len(index.levels) == 1` (L1 created).
3. Assert guards are sorted by guard_key. Assert guard count == number of distinct pk_min values.
4. Call `create_cursor()`. Scan all rows — assert count == 5, weights correct.
5. Test ghost elimination: insert key A (+1), insert key B (+1), flush each. Insert retraction of
   A (-1), flush. Trigger compaction. Assert A is absent from all L1 guards. Assert B is present.

Add `test_flsm_manifest_persistence` in `storage_comprehensive_test.py`:

1. Create `PersistentTable`. Write 6 rows, triggering 5 flushes. Run `compact_if_needed()`.
2. Assert manifest entries have `level=1` and non-zero guard keys.
3. Close the table. Reconstruct via `index_from_manifest`. Assert same guard structure and file
   count.
4. Verify reads return correct data after reconstruction.

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

```python
def compact_guards_if_needed(self, output_dir, validate_checksums):
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
                self._compact_one_guard(level, g_idx, output_dir, validate_checksums)
            g_idx += 1

def _compact_one_guard(self, level, guard_idx, output_dir, validate_checksums):
    guard = level.guards[guard_idx]
    input_files = newlist_hint(len(guard.handles))
    guard_max_lsn = r_uint64(0)
    for h in guard.handles:
        input_files.append(h.filename)
        if h.lsn > guard_max_lsn:
            guard_max_lsn = h.lsn

    out_path = output_dir + "/hcomp_%d_L%d_%d_%d.db" % (
        self.table_id, level.level_num, intmask(guard.guard_key_lo), intmask(guard_max_lsn))

    try:
        compactor.compact_guard_horizontal(guard, out_path, self.schema,
                                            self.table_id, validate_checksums)
        new_handle = ShardHandle(out_path, self.schema, r_uint64(0), guard_max_lsn,
                                  validate_checksums)
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

**EphemeralTable** (`create_cursor`): call `compact_guards_if_needed` after L0 auto-compaction:

```python
def create_cursor(self):
    if self.index.needs_l0_compact:
        self._compact()
    self.index.compact_guards_if_needed(self.directory, self.validate_checksums)
    return self._build_cursor()
```

**PersistentTable** (`compact_if_needed`): add horizontal compaction step and re-publish manifest
once after all compaction. The manifest must be re-published even if only horizontal compaction
ran (filenames in guards changed):

```python
def compact_if_needed(self):
    if not self.index.needs_l0_compact:
        return
    # ... (L0→L1 vertical — same as Phase 2) ...

    # Horizontal compaction: runs after vertical, enforces GUARD_FILE_THRESHOLD at L1
    # and LMAX_FILE_THRESHOLD at L2 (Lazy Leveling invariant).
    self.index.compact_guards_if_needed(self.directory, self.validate_checksums)

    # Single manifest publish covers both vertical and horizontal changes.
    self.manifest_manager.publish_new_version(
        self.index.get_metadata_list(), self.index.max_lsn(),
    )
```

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

```python
def compact_guard_vertical_if_needed(self, src_level_num, output_dir, validate_checksums):
    """Picks the overfull guard in src_level and promotes it to src_level+1."""
    src_level = self.levels[src_level_num - 1]
    if src_level_num >= MAX_LEVELS:
        # Already at Lmax; fall back to horizontal compaction
        self.compact_guards_if_needed(output_dir, validate_checksums)
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
        src_max_bound = r_uint128(-1)

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
                                  validate_checksums)
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
                self._compact_one_guard(dest_level, g_idx, output_dir, validate_checksums)
```

### Integration: triggering L1→L2

In `_compact()` (EphemeralTable) and `compact_if_needed()` (PersistentTable), add a check after
horizontal compaction:

```python
def _compact(self):
    # 1. L0→L1 vertical (Phase 2)
    if self.index.needs_l0_compact:
        ...
    # 2. Horizontal compaction within guards (Phase 3)
    self.index.compact_guards_if_needed(self.directory, self.validate_checksums)
    # 3. L1→L2 vertical if L1 is too large (Phase 4)
    if len(self.index.levels) > 0:
        l1 = self.index.levels[0]
        if l1.total_file_count() > L1_TARGET_FILES:
            self.index.compact_guard_vertical_if_needed(1, self.directory,
                                                         self.validate_checksums)
```

For `PersistentTable.compact_if_needed()`, the existing single manifest publish at the end of
Phase 3 already covers this: `compact_guard_vertical_if_needed` finishes (including L2 Lazy
Leveling enforcement) before `publish_new_version` is called.

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
| `gnitz/storage/flsm.py` | 1 | **New file**: `LevelGuard`, `FLSMLevel`, `FLSMIndex` + constants |
| `gnitz/storage/metadata.py` | 1 | Add `level`, `guard_key_lo`, `guard_key_hi` to `ManifestEntry` |
| `gnitz/storage/index.py` | 1, 2 | Import `FLSMIndex`; update `index_from_manifest` to place entries in correct level/guard |
| `gnitz/storage/ephemeral_table.py` | 1, 2, 3, 4 | Use `FLSMIndex`; update `_build_cursor`, `_compact`, `create_cursor` |
| `gnitz/storage/table.py` | 1, 2, 3, 4 | Use `FLSMIndex`; replace inline manifest loop in `__init__`; update `_build_cursor`, `compact_if_needed` |
| `gnitz/storage/compactor.py` | 2, 3, 4 | Add `_merge_and_route`, `compact_l0_to_l1`, `compact_guard_horizontal`, `compact_guard_vertical` |
| `gnitz/storage/manifest.py` | 2 | Bump `VERSION=3`, `ENTRY_SIZE=208`; handle V2 legacy reads |
| `gnitz/storage/partitioned_table.py` | 3 | `compact_if_needed` delegates horizontal compaction to each partition |
| `rpython_tests/storage_comprehensive_test.py` | all | New test functions per phase |

---

## What Each Phase Delivers

| Phase | What you have after | Key test |
|-------|---------------------|----------|
| 1 | `FLSMIndex` replaces `ShardIndex`; guard/level data structures exist but are empty; read path traverses L1+ when guards are present | guard binary search unit tests; manually inject L1 guards and verify reads; all existing tests pass |
| 2 | L0→L1 vertical compaction; first guard-partitioned structure on disk; manifest V3; ghost elimination at L1 | auto-compact, verify guard structure, verify manifest round-trip, verify ghost elimination |
| 3 | Files within guards bounded; Lazy Leveling Z=1 enforced at L2 | guard horizontal compact, verify file count + data integrity, verify level-aware thresholds |
| 4 | Full 3-level hierarchy; guard-targeted L1→L2 promotion; write amplification bounded | multilevel data round-trip, guard isolation regression, Lazy Leveling invariant after multiple cycles |
