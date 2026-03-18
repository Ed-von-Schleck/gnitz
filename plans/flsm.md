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

**Commit**: db39246

- `gnitz/storage/flsm.py` — new file: `LevelGuard`, `FLSMLevel`, `FLSMIndex`, `index_from_manifest`
  (moved from `index.py`). `FLSMIndex` stores `output_dir`/`validate_checksums`/`_compact_seq`
  for use by future `run_compact`. L1+ levels start empty.
- `gnitz/storage/metadata.py` — added `level`, `guard_key_lo`, `guard_key_hi` to `ManifestEntry`
  (default 0; ignored by manifest serialization until Phase 2).
- `gnitz/storage/index.py` — removed `index_from_manifest`.
- `gnitz/storage/ephemeral_table.py` — `ShardIndex` → `FLSMIndex`; `_build_cursor` uses
  `all_handles_for_cursor()`.
- Tests: `test_flsm_data_structures` (guard binary search, `find_guards_for_range`,
  `needs_compaction` threshold) and `test_flsm_guard_read_path` (manually inject L1 guards,
  verify `has_pk` + cursor over all 20 PKs). All existing storage tests pass.

---

## Phase 2: L0→L1 Vertical Compaction ✓ DONE

**Goal**: When L0 overflows (`needs_compaction == True`), merge and route L0 data into L1 guards.
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

**Filename uniqueness**: `lsn_tag` (monotonically increasing integer) is embedded in output
filenames to prevent collisions across compaction cycles. It is computed inside
`FLSMIndex.run_compact()` (introduced below): for `PersistentTable` where handles carry real
LSNs, `lsn_tag = intmask(l0_max_lsn)`; for `EphemeralTable` where all handle LSNs are
`r_uint64(0)`, `lsn_tag = self._compact_seq` (incremented each cycle). Both cases are handled
internally — callers of `run_compact()` supply nothing.

### Module boundaries and the circular import

`FLSMIndex.run_compact()` (added below) calls helpers in `compactor.py`. This creates an import:
`flsm.py → compactor.py`. The constraint is that `compactor.py` must never import `flsm.py`,
which is maintained because `compactor.py` receives `FLSMIndex` and `LevelGuard` only as
duck-typed parameters — no explicit import needed.

The critical step in this phase is **deleting `execute_compaction` from `compactor.py`**. That
function is the only reason `compactor.py` currently imports `index.py` (it uses
`index.ShardHandle`). Once deleted, `compactor.py` has no dependency on `index.py`. Combined
with `index.py` not importing `flsm.py` (ensured in Phase 1), the module graph is acyclic:

```
flsm.py      → compactor.py   (run_compact calls merge helpers)
             → index.py       (ShardHandle for new handles in commit_l0_to_l1)
compactor.py → shard_table, writer_table, tournament_tree, cursor   (no index, no flsm)
             + adds `r_ulonglonglong as r_uint128` to arithmetic imports (used by `_find_guard_for_key`)
index.py     → shard_table, metadata, manifest                      (no flsm)
```

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
            if writers[i].count > 0:
                writers[i].finalize(out_filenames[i])
            else:
                writers[i].close()  # free buffers without writing file
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

`TableShardWriter.finalize()` always creates the output file via an unconditional `O_CREAT` open
followed by atomic rename — even when zero rows were written. Guards that received only ghost rows
(all weights cancelled to zero) would produce empty shard files that get registered as real
handles. The `count > 0` guard ensures only non-empty writers call `finalize`; empty writers call
`close()` to free their buffers. Because files are now only created for non-empty output, the
`os.path.exists` check in the results loop is a reliable filter.

`_find_guard_for_key(guard_keys, key)` — binary search over the sorted `(lo, hi)` tuple list.
Returns index `i` such that `guard_keys[i] ≤ key < guard_keys[i+1]`; last guard wins for
`key ≥ guard_keys[-1]`. `guard_keys` is always sorted ascending (built from sorted L0 handles or
from the already-sorted `l1.guards` list):

```python
def _find_guard_for_key(guard_keys, key):
    n = len(guard_keys)
    lo, hi = 0, n - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        gk_lo, gk_hi = guard_keys[mid]
        gk = (r_uint128(gk_hi) << 64) | r_uint128(gk_lo)
        if gk <= key:
            lo = mid
        else:
            hi = mid - 1
    return lo
```

### New function: `compactor.compact_l0_to_l1`

```python
def compact_l0_to_l1(flsm_index, output_dir, schema, table_id, lsn_tag,
                     validate_checksums=False):
    """
    Merges all L0 shards and routes rows into L1 guards.
    Returns list of (guard_key_lo, guard_key_hi, output_filename).
    """
    l0_handles = flsm_index.handles
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
def commit_l0_to_l1(self, l0_input_filenames, guard_outputs, max_lsn):
    """
    Atomically clears compacted L0 handles and installs output handles into L1 guards.

    guard_outputs: list of (guard_key_lo, guard_key_hi, filename)
    max_lsn: aggregate max LSN of compacted L0 handles (r_uint64(0) for EphemeralTable).
    Uses self.validate_checksums when opening new ShardHandles.
    """
    new_l0 = newlist_hint(len(self.handles))
    for h in self.handles:
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
    self.handles = new_l0

    l1 = self.get_or_create_level(1)
    for (gk_lo, gk_hi, filename) in guard_outputs:
        new_handle = ShardHandle(filename, self.schema, r_uint64(0), max_lsn,
                                  self.validate_checksums)
        gk = (r_uint128(gk_hi) << 64) | r_uint128(gk_lo)
        g_idx = l1.find_guard_idx(gk)
        if g_idx == -1 or l1.guards[g_idx].guard_key() != gk:
            new_guard = LevelGuard(gk_lo, gk_hi)
            l1.insert_guard_sorted(new_guard)
            g_idx = l1.find_guard_idx(gk)
        l1.guards[g_idx].add_handle(self.ref_counter, new_handle)

    self._update_flags()
```

### New method: `FLSMIndex.run_compact`

This is the single owner of all compaction state transitions. Both table classes call it; neither
duplicates its logic. Horizontal compaction (Phase 3) and L1→L2 (Phase 4) are added to this
method body in their respective phases — no table-class changes required.

```python
def run_compact(self):
    """Full compaction cycle. No-op if L0 is not overflowing."""
    if not self.needs_compaction:
        return

    # Compute lsn_tag. For PersistentTable handles carry real LSNs; use the L0 max.
    # For EphemeralTable all LSNs are r_uint64(0); fall back to the monotone counter.
    self._compact_seq += 1
    l0_max_lsn = r_uint64(0)
    l0_input_files = newlist_hint(len(self.handles))
    for h in self.handles:
        l0_input_files.append(h.filename)
        if h.lsn > l0_max_lsn:
            l0_max_lsn = h.lsn
    lsn_tag = intmask(l0_max_lsn) if l0_max_lsn > r_uint64(0) else self._compact_seq

    guard_outputs = []   # must be initialised before try; except block iterates it
    try:
        guard_outputs = compactor.compact_l0_to_l1(
            self, self.output_dir, self.schema, self.table_id, lsn_tag,
            self.validate_checksums,
        )
        self.commit_l0_to_l1(l0_input_files, guard_outputs, l0_max_lsn)
        for fn in l0_input_files:
            self.ref_counter.mark_for_deletion(fn)
        self.ref_counter.try_cleanup()
    except Exception as e:
        for _, _, fn in guard_outputs:
            if os.path.exists(fn):
                try: os.unlink(fn)
                except OSError: pass
        raise e

    # Phase 3 adds: self.compact_guards_if_needed()
    # Phase 4 adds: L1→L2 vertical if L1 is overfull
```

### Changes to `gnitz/storage/ephemeral_table.py`

Replace `_compact()` with a one-line delegate. All compaction logic now lives in `run_compact`:

```python
def _compact(self):
    self.index.run_compact()
```

`create_cursor()` already calls `compact_if_needed()` which calls `_compact()`. No further change.

### Changes to `gnitz/storage/table.py`

**`compact_if_needed`**: replace with a thin wrapper. The only thing `PersistentTable` adds beyond
`EphemeralTable` is the manifest publish; `run_compact()` handles everything else:

```python
def compact_if_needed(self):
    if not self.index.needs_compaction:
        return
    self.index.run_compact()
    self.manifest_manager.publish_new_version(
        self.index.get_metadata_list(), self.index.max_lsn())
```

`self.index.max_lsn()` after `run_compact()` returns the correct boundary: L0 handles are gone,
their LSNs are now embedded in L1 `ShardHandle` objects.

**`__init__` manifest recovery**: the existing inline loop calls `self.index.add_handle(h)` for
every entry regardless of `entry.level`, routing all entries to L0. After Phase 2, a V3 manifest
contains L1 guard entries; the old loop would load them into L0 on restart, destroying the guard
structure. Replace the loop body with `populate_from_reader`:

```python
if self.manifest_manager.exists():
    reader = self.manifest_manager.load_current()
    try:
        self.index.populate_from_reader(self.table_id, reader)
        self.current_lsn = reader.global_max_lsn + r_uint64(1)
    finally:
        reader.close()
```

`self.index` already exists as an empty `FLSMIndex` (created by `EphemeralTable.__init__`);
`populate_from_reader` fills it in-place. `reader.global_max_lsn` (set during `load_current()`)
is read after `populate_from_reader` returns — single file open, no second pass needed. No new
import is required: `self.index` is typed as `FLSMIndex` by the annotator, so the method call
resolves without an explicit `import flsm` in `table.py`.

### Manifest format extension (PersistentTable only)

EphemeralTable has no manifest; skip this section for ephemeral.

**`gnitz/storage/manifest.py`**:

- Bump `VERSION = 3`. Add `VERSION_LEGACY = 2`. Add `ENTRY_SIZE_V2 = 184` (legacy stride).
  `ENTRY_SIZE = 208`. New trailing 24 bytes:
  - `[184–191]` level_num (u64)
  - `[192–199]` guard_key_lo (u64)
  - `[200–207]` guard_key_hi (u64)
- `_write_manifest_entry`: allocate 208-byte buffer; write the three new fields at offsets 184,
  192, 200. The write path always produces V3.
- `_read_manifest_entry(fd, version)` — **signature gains a `version` parameter**. Read
  `ENTRY_SIZE if version >= VERSION else ENTRY_SIZE_V2` bytes; return `None` if fewer bytes are
  available. Extract the V2 fields from [0..183] unconditionally. Read level/guard fields from
  [184..207] only when `version >= VERSION`; otherwise default to `level=0, guard_key_lo=0,
  guard_key_hi=0`. This is the critical fix: a V2 file stores 184-byte entries. Reading 208 bytes
  per entry without branching on version would advance the file cursor 24 bytes into the next
  entry after the first read, corrupting every subsequent entry. A V2 manifest with a single shard
  would return zero entries immediately (184 < 208 triggers the `None` guard), silently losing the
  shard.
- `ManifestReader.iterate_entries`: pass `self.version` to each `_read_manifest_entry` call.
  `ManifestReader.reload` already stores `self.version` from the header; this is the single
  threading point that makes V2/V3 stride selection automatic for all callers.

**`gnitz/storage/flsm.py` — new method `FLSMIndex.populate_from_reader`**:

All manifest recovery logic — both in `index_from_manifest` and in `PersistentTable.__init__` —
needs the same level-routing loop. Extract it as a method on `FLSMIndex` so there is exactly one
definition:

```python
def populate_from_reader(self, table_id, reader):
    """Populate this index from an open ManifestReader, routing entries by level."""
    for entry in reader.iterate_entries():
        if entry.table_id != table_id:
            continue
        handle = ShardHandle(entry.shard_filename, self.schema,
                             entry.min_lsn, entry.max_lsn, self.validate_checksums)
        if entry.level == 0:
            self.add_l0_handle(handle)
        else:
            level = self.get_or_create_level(entry.level)
            gk = (r_uint128(entry.guard_key_hi) << 64) | r_uint128(entry.guard_key_lo)
            g_idx = level.find_guard_idx(gk)
            if g_idx == -1 or level.guards[g_idx].guard_key() != gk:
                g = LevelGuard(entry.guard_key_lo, entry.guard_key_hi)
                level.insert_guard_sorted(g)
                g_idx = level.find_guard_idx(gk)
            level.guards[g_idx].add_handle(self.ref_counter, handle)
```

`add_l0_handle` acquires the refcount; `LevelGuard.add_handle` also acquires. Both paths are
refcount-balanced: every handle tracked by the index has exactly one acquisition, released by
`close_all()`. V2 manifest entries arrive with `entry.level == 0` (the default after the Bug 1
fix), so all V2 entries route to L0 correctly without any special-casing here.

**`gnitz/storage/flsm.py` — `index_from_manifest`** (moved from `index.py` in Phase 1):

`index_from_manifest` is only called from tests; `PersistentTable.__init__` has its own inline
recovery. Update both to delegate to `populate_from_reader`. `index_from_manifest` becomes a
thin factory:

```python
def index_from_manifest(manifest_path, table_id, schema, ref_counter,
                        output_dir="", validate_checksums=False):
    idx = FLSMIndex(table_id, schema, ref_counter, output_dir, validate_checksums)
    reader = manifest.ManifestReader(manifest_path)
    try:
        idx.populate_from_reader(table_id, reader)
    finally:
        reader.close()
    return idx
```

The signature keeps `output_dir=""` as a keyword-with-default (unchanged from Phase 1) so the
existing test call `index_from_manifest(path, tid, schema, rc)` continues to work without
modification.

Also delete `execute_compaction` from `compactor.py` in this phase (it is replaced by
`run_compact`). Remove the now-unused `import index` line from `compactor.py`.

### Phase 2 Tests

Add `test_flsm_l0_to_l1_compaction` in `storage_comprehensive_test.py`:

1. Create `EphemeralTable` with arena size 4096 bytes. Insert 5 rows and flush after each → 5 L0
   handles. Trigger `create_cursor()` → auto-compact via `index.run_compact()`.
2. Assert `len(index.handles) == 0`. Assert `len(index.levels) == 1` (L1 created).
3. Assert guards are sorted by guard_key. Assert guard count == number of distinct pk_min values.
4. Call `create_cursor()`. Scan all rows — assert count == 5, weights correct.
5. Test ghost elimination: insert key A (+1) and flush; insert key B (+1) and flush; insert
   retraction of A (−1) and flush; insert two further dummy rows and flush each — 5 L0 handles
   total. Call `create_cursor()` to auto-compact. Assert A is absent from all L1 guards; assert B
   is present. (Exactly 5 flushes are required: `L0_COMPACT_THRESHOLD = 4`, so
   `needs_compaction = (n > 4)`; fewer than 5 handles leaves the flag False and `create_cursor()`
   skips compaction silently.)

Add `test_flsm_manifest_persistence` in `storage_comprehensive_test.py`:

1. Create `PersistentTable`. Write 6 rows, triggering 5 flushes. Run `compact_if_needed()`.
2. Assert manifest entries have `level=1` and non-zero guard keys.
3. Close the table. Open a new `PersistentTable` on the same directory; its `__init__` now uses
   `populate_from_reader`, so L1 guards are restored correctly. Assert `index.levels` has 1 level
   with the same guard count and filenames as before close. Also call `index_from_manifest`
   directly on the MANIFEST path and assert identical guard structure — both paths share
   `populate_from_reader`, so this tests the shared logic from two entry points.
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
