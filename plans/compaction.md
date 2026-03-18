# Automatic Compaction for Ephemeral Tables

## Context

EphemeralTable instances (operator state: distinct history, reduce traces, group indices,
secondary indices) accumulate shard files indefinitely. `compact_shards()` exists and is
tested, `ShardIndex.needs_compaction` is set correctly (threshold >4 handles), but nothing
ever reads the flag. Unbounded shard growth causes O(K log K) read amplification per cursor
seek, degrading performance over time with no upper bound.

The fix introduces `compact_if_needed()` as the **single, universal compaction trigger** for
all store types. Every store implements it. `EphemeralTable.create_cursor()` calls it
transparently so all callers (GroupIndex, IndexCircuit, direct users) benefit without knowing
about compaction. `PersistentTable.create_cursor()` overrides to skip the call — its
compaction runs explicitly in `TraceRegister.refresh()`, the one safe point where no cursor
references the table's shards.

The entire compaction wiring adds ~25 lines of production code, zero changes to operators,
executor, interpreter, or program_cache.

---

## Design: One Compaction Method, Two Cursor Strategies

The core insight: `compact_if_needed()` is the universal compaction interface. Every store
type implements it. The override chain mirrors the inheritance hierarchy exactly:

```
ZSetStore.compact_if_needed()           → pass (base default)
  EphemeralTable.compact_if_needed()    → compact shards if threshold exceeded
    PersistentTable.compact_if_needed() → compact shards + update manifest

ZSetStore.create_cursor()               → abstract
  EphemeralTable.create_cursor()        → compact_if_needed() + _build_cursor()
    PersistentTable.create_cursor()     → _build_cursor() only (cursor safety)

PartitionedTable.compact_if_needed()    → iterate partitions
PartitionedTable.create_cursor()        → iterate partitions' create_cursor()
```

**Why PersistentTable.create_cursor() skips compact_if_needed():**
`executor.py:_scan_family` (line 400), `executor.py:_seek_family` (line 416),
`worker.py:_handle_scan` (line 214), `worker.py:_handle_seek` (line 235), and catalog
loading all create cursors on source data tables while TraceRegister cursors on the same
table remain alive from the last `prepare_for_tick()`. Compacting would close ShardHandle
views, invalidating those live cursors. `PersistentTable.compact_if_needed()` runs only in
`TraceRegister.refresh()`, between cursor close and cursor create — the one moment where
no cursor references the table's shards.

**For EphemeralTable via `refresh()`:** `compact_if_needed()` runs (compacts if needed),
then `create_cursor()` calls `compact_if_needed()` again (immediate no-op since
`needs_compaction` is now False). The double call is a single boolean field read — zero cost.

---

## Files to modify

| File | Change |
|------|--------|
| `gnitz/core/store.py` | Add `compact_if_needed()` no-op to ZSetStore |
| `gnitz/storage/ephemeral_table.py` | Add `compactor` import, `compact_if_needed()` override, extract `_build_cursor()`, new `create_cursor()`, new `_compact()` |
| `gnitz/storage/table.py` | PersistentTable: override `create_cursor()`, override `compact_if_needed()`, add `compactor` import |
| `gnitz/storage/partitioned_table.py` | PartitionedTable: override `compact_if_needed()` |
| `gnitz/vm/runtime.py` | Add `compact_if_needed()` call in `TraceRegister.refresh()` |
| `rpython_tests/storage_comprehensive_test.py` | New `test_ephemeral_compaction` |
| `rpython_tests/dbsp_comprehensive_test.py` | New `test_compaction_through_ticks` |

---

## Step 1: `gnitz/core/store.py` — ZSetStore base method

Add after `close()` (line 112-114), before `ingest_batch_memonly()` (line 116):

```python
def compact_if_needed(self):
    """Trigger compaction if shard count exceeds threshold. No-op by default."""
    pass
```

---

## Step 2: `gnitz/storage/ephemeral_table.py` — Core changes

### 2a: Add `compactor` import

At lines 20-26, add `compactor` to the existing storage import block:

```python
from gnitz.storage import (
    index,
    memtable,
    refcount,
    compactor,
    comparator as storage_comparator,
    cursor,
)
```

### 2b: Rename `create_cursor()` → `_build_cursor()`

Rename the existing method at lines 105-113. Body unchanged:

```python
def _build_cursor(self):
    num_shards = len(self.index.handles)
    cs = newlist_hint(1 + num_shards)
    cs.append(cursor.MemTableCursor(self.memtable))
    for h in self.index.handles:
        cs.append(cursor.ShardCursor(h.view))
    return cursor.UnifiedCursor(self.schema, cs)
```

### 2c: `compact_if_needed()` override

Add immediately after `_build_cursor()`:

```python
def compact_if_needed(self):
    if self.index.needs_compaction:
        self._compact()
```

### 2d: New `create_cursor()` — delegates to `compact_if_needed()`

Add immediately after `compact_if_needed()`:

```python
def create_cursor(self):
    self.compact_if_needed()
    return self._build_cursor()
```

### 2e: New `_compact()` method

Add after `create_cursor()`, before `_scan_shards_for_pk()` (line 115):

```python
def _compact(self):
    handles = self.index.handles
    num_h = len(handles)
    if num_h <= 1:
        return

    input_files = newlist_hint(num_h)
    h_idx = 0
    while h_idx < num_h:
        input_files.append(handles[h_idx].filename)
        h_idx += 1

    self._flush_seq += 1
    out_path = self.directory + "/compact_%d_%d.db" % (
        self.table_id, self._flush_seq)

    try:
        compactor.compact_shards(
            input_files, out_path, self.schema,
            self.table_id, self.validate_checksums,
        )

        new_handle = index.ShardHandle(
            out_path, self.schema,
            r_uint64(0), r_uint64(0),
            validate_checksums=self.validate_checksums,
        )

        self.index.replace_handles(input_files, new_handle)

        f_idx = 0
        while f_idx < len(input_files):
            self.ref_counter.mark_for_deletion(input_files[f_idx])
            f_idx += 1
        self.ref_counter.try_cleanup()

    except Exception as e:
        if os.path.exists(out_path):
            try:
                os.unlink(out_path)
            except OSError:
                pass
        raise e
```

Key design points:
- `_flush_seq` provides monotonic uniqueness. `"compact_"` prefix vs `"eph_shard_"` prevents
  collision with flush-created shards. Shared counter ensures no duplicate sequence numbers.
- LSN is `r_uint64(0)` for both min and max — ephemeral tables don't track LSN.
- `replace_handles` closes old ShardHandles (frees mmap + xor8 memory), releases ref locks.
- `compact_shards` → `TableShardWriter.finalize()` automatically creates `.xor8` sidecar.
- Exception handler unlinks orphaned output file, matching `execute_compaction` pattern.
- No recursion risk: `compact_shards` opens its own `TableShardView` instances internally,
  does not call `self.create_cursor()`.
- Compaction failure is intentionally fatal. If compaction fails, something is seriously wrong
  (disk full, data corruption). Per Appendix B: "fail loudly." The original shards are intact
  (replace_handles hasn't been called), so no data is lost — the process just stops.

---

## Step 3: `gnitz/storage/table.py` — PersistentTable overrides

### 3a: Add `compactor` import

At lines 11-16:

```python
from gnitz.storage import (
    wal,
    index,
    manifest,
    memtable,
    compactor,
)
```

### 3b: Override `create_cursor()` — skip compact_if_needed()

Add after `recover_from_wal` (line 86), before the `# -- Mutations` comment (line 88):

```python
def create_cursor(self):
    return self._build_cursor()
```

PersistentTable overrides to prevent the inherited `compact_if_needed()` call. Compaction
for PersistentTable runs only via the explicit `compact_if_needed()` in
`TraceRegister.refresh()`, where the old cursor has already been closed.

### 3c: Override `compact_if_needed()` — manifest-aware compaction

Add immediately after `create_cursor()`:

```python
def compact_if_needed(self):
    if not self.index.needs_compaction:
        return
    compactor.execute_compaction(
        self.index, self.manifest_manager,
        output_dir=self.directory,
        validate_checksums=self.validate_checksums,
    )
```

This reuses the existing, tested `execute_compaction` (reachable from
`storage_comprehensive_test.py:663`) which handles manifest updates, LSN tracking, and
ref-counter cleanup.

---

## Step 4: `gnitz/storage/partitioned_table.py` — PartitionedTable override

Add after `flush()` (line 204-210), before `close_partitions_outside()` (line 212):

```python
def compact_if_needed(self):
    for local in range(len(self.partitions)):
        self.partitions[local].compact_if_needed()
```

Each partition's `compact_if_needed()` dispatches polymorphically:
- EphemeralTable partition → compacts shards if threshold exceeded
- PersistentTable partition → `execute_compaction` with manifest update

---

## Step 5: `gnitz/vm/runtime.py` — TraceRegister.refresh()

Add one line between cursor close and cursor create (line 65→66):

```python
def refresh(self):
    if self.table is None:
        return
    if self.cursor is not None:
        self.cursor.close()
    self.table.compact_if_needed()       # ← new
    self.cursor = self.table.create_cursor()
```

This is the lifecycle boundary: close → maintain → reopen. `compact_if_needed()` is
meaningful for ALL store types here:
- EphemeralTable: compacts if threshold exceeded (then `create_cursor()` calls it again →
  no-op since `needs_compaction` is now False)
- PersistentTable: runs `execute_compaction` with manifest update
- PartitionedTable: iterates partitions, each dispatches polymorphically

---

## Step 6: Tests

### 6a: `test_ephemeral_compaction` in `rpython_tests/storage_comprehensive_test.py`

Test plan:
1. Create EphemeralTable with default threshold (4).
2. Insert 6 distinct rows, flushing after each → 6 shards.
3. Assert `needs_compaction == True`, `len(handles) == 6`.
4. Call `create_cursor()` — triggers auto-compaction.
5. Assert `len(handles) == 1`, `needs_compaction == False`.
6. Scan cursor — assert 6 rows with correct data.
7. Insert retraction for one PK, add 4 more rows with flushes → triggers second compaction.
8. Call `create_cursor()` again.
9. Assert retracted PK is physically absent (ghost property).
10. Assert remaining row count is correct.

### 6b: `test_compaction_through_ticks` in `rpython_tests/dbsp_comprehensive_test.py`

Test plan:
1. Create EphemeralTable trace tables with small memtable arena (4096 bytes).
2. Wire a DISTINCT operator manually (same pattern as `test_distinct_op`).
3. Run 10+ ticks, each pushing enough rows to trigger a memtable flush on the
   history table.
4. Assert that after enough ticks, history table shard count stays bounded. The actual
   bound is `threshold + flushes_per_epoch` (typically threshold + 1 for DISTINCT, where
   each epoch produces one flush). Assert `len(handles) <= 2 * compaction_threshold` as a
   conservative upper bound.
5. Assert DISTINCT output correctness across all ticks (weights are correct).

---

## What this covers automatically (zero extra code)

| Table type | How it compacts |
|---|---|
| DISTINCT history (`_hist_*`) | `TraceRegister.refresh()` → `compact_if_needed()` + `create_cursor()` → auto-compact |
| REDUCE output trace (`_reduce_*`) | `TraceRegister.refresh()` → `compact_if_needed()` + `create_cursor()` → auto-compact |
| REDUCE input trace (`_reduce_in_*`) | `TraceRegister.refresh()` → `compact_if_needed()` + `create_cursor()` → auto-compact |
| ReduceGroupIndex (`_gidx`) | `group_index.py:93` → `self.table.create_cursor()` → auto-compact |
| Secondary indices (IndexCircuit) | `index_circuit.py:161` → `self.table.create_cursor()` → auto-compact |
| Source data (PersistentTable) | `TraceRegister.refresh()` → `compact_if_needed()` → `execute_compaction` |
| User tables (PartitionedTable) | `TraceRegister.refresh()` → `compact_if_needed()` → per-partition |

---

## Verification

1. Run `make test` — all existing tests must pass unchanged.
2. The two new tests (`test_ephemeral_compaction`, `test_compaction_through_ticks`)
   verify auto-compaction trigger, data integrity, ghost property, and bounded shard count.
3. Key correctness properties to verify:
   - Shard count stays bounded (never unbounded growth)
   - Cursor data integrity preserved across compaction
   - Annihilated rows (net weight 0) are physically absent after compaction
   - No filename collisions between `eph_shard_*` and `compact_*` files
   - PersistentTable `create_cursor()` does NOT trigger compaction
   - `compact_if_needed()` on any store type does the right thing

---

## Structural Pattern: Compaction and Consolidation Are the Same Operation

`compact_if_needed()` on `ZSetStore` and `to_consolidated()` on `ArenaZSetBatch` are
structurally identical. Both are lazy, idempotent invariant restorers — one at the storage
layer, one at the batch layer:

```
                    compact_if_needed()          to_consolidated()
                    ───────────────────          ─────────────────
Layer               Storage (shard files)        Batch (row order)
Degraded state      K shards → O(K log K)        Unsorted/duped → O(N log N)
                    per cursor seek              to re-sort
Clean state         1 shard                      Sorted, deduped, no zeros
Guard flag          needs_compaction             _consolidated
Short-circuit       if !needs_compaction: pass   if _consolidated: return self
Trigger             refresh() + create_cursor()  execute_epoch() seal
Idempotent          Yes (1 shard → no-op)        Yes (returns self)
```

Both converge and then propagate for free. After compaction, `needs_compaction` stays False
until enough flushes accumulate (sawtooth: 1→2→3→4→5→compact→1→...). After the seal,
`_consolidated` propagates through filter, negate, delay, reduce, distinct — every operator
either preserves it or produces consolidated output by construction. Both systems tolerate
bounded degradation between maintenance cycles.

The two compose multiplicatively in the epoch boundary:

```
prepare_for_tick()
  ├─ compact_if_needed()        L1: fewer shards → cheaper cursor ops
  ├─ cursor refresh             cursors now see compacted shards
  └─ delta.clear()

execute_epoch()
  ├─ to_consolidated()          L2: sorted+deduped → enables merge-walk
  ├─ run_vm()                   all operators see clean L1 + L2 data
  │    ├─ merge-walk join       exploits L2 (consolidated) over L1 (cheap cursors)
  │    ├─ cursor-based distinct exploits L1 (fewer shards in UnifiedCursor)
  │    ├─ group index hoist     exploits L1 (single cursor creation)
  │    └─ is_distinct skip      exploits L2 (consolidated → already distinct)
  └─ evict result               L3: zero-copy extraction
```

Every downstream optimization in the masterplan is a consumer of one or both invariants.
The masterplan's phase structure reflects this: Phase 1 (compaction) establishes L1, Phase 2
(source optimizations) establishes L2, Phases 3-7 exploit them.

This pattern — **lazy idempotent invariant restoration at epoch boundaries, with boolean
flags enabling short-circuit at consumption points** — is the single organizing principle
of the entire optimization stack.

---

## Review Notes

Validated by full codebase audit (91 `create_cursor()` call sites: 26 production, 65 test).

**Concurrent cursor safety (PersistentTable):** Confirmed that `executor.py:_scan_family`
(line 400), `executor.py:_seek_family` (line 416), `worker.py:_handle_scan` (line 214),
`worker.py:_handle_seek` (line 235), and catalog loading create cursors on source data
tables while TraceRegister cursors on the same table remain alive. The override in
`PersistentTable.create_cursor()` is essential.

**Single-cursor safety (EphemeralTable):** All 26 production call sites on EphemeralTable
follow cursor-safe patterns: try/finally with immediate close, single-ownership via
TraceRegister, or delegate methods (GroupIndex.create_cursor at group_index.py:93,
IndexCircuit.create_cursor at index_circuit.py:161). The gi_cursor in reduce.py:465 is
created and closed within the same loop iteration (line 487). No concurrent cursors exist
on any EphemeralTable instance.

**Shard count bound:** The practical bound is `threshold + flushes_per_epoch`. For typical
operator state (1 flush/epoch), peak handles = threshold + 1 = 5. The sawtooth pattern is:
1, 2, 3, 4, 5, compact→1, 2, 3, 4, 5, compact→1, ... Tests should assert a conservative
upper bound (2× threshold) rather than an exact number.

**`execute_compaction` RPython compatibility:** Already reachable from
`storage_comprehensive_test.py:663` and compiles successfully. The `os.path.join` on
compactor.py:98 works despite the codebase convention to avoid it (the annotation issue
in Appendix A §10 is specific to `rfind`-derived slice indices, not `os.path.join`'s
implementation).

**Compaction timing:** Compaction runs in the gap between cursor close and cursor create
(the epoch boundary). It cannot run during operator execution on the same table because
flush()-time compaction would invalidate live cursors. The plan's timing is the natural and
only correct placement.
