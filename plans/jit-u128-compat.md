# JIT Compatibility: r_uint128 → hi/lo Refactoring

## Problem

The RPython JIT (`--opt=jit`) hard-crashes during the `pyjitpl_lltype` phase when it
encounters a **residual call** (a call the JIT cannot inline) whose return type or any
argument type is `UnsignedLongLongLong` (128-bit).  The crash is an `AssertionError` in
`rpython/jit/metainterp/history.py:getkind()` which asserts `rffi.sizeof(TYPE) == 8`,
but `rffi.sizeof(r_uint128)` = 16.

This is a fundamental architectural limitation of the RPython JIT value model: every
JIT-managed value must fit in one 64-bit slot (int, float, or ref).  128-bit integers
have no slot type, so the JIT cannot represent them — neither as inline values nor as
the result of a residual call.

The two observed failure modes:
1. **"ignoring graph" WARNING** — graph has r_uint128 as a local variable; the JIT
   marks it as non-inlineable and makes it a residual call.
2. **CRASH in `get_call_descr`** — a graph that IS being JIT-compiled contains a call to
   an ignored function that returns r_uint128; the call descriptor for that residual call
   needs `map_type_to_argclass(r_uint128)` which calls `getkind()` → assert fires.

The `release-server` build hit exactly this sequence: 5 "ignoring" warnings (for merge
walk helpers in `join.py`, `anti_join.py`, and `memtable.py`), then a crash when the
codewriter tried to build a call descriptor for a function that returns r_uint128.

## Why Option A ("@jit.dont_look_inside on callers") Is Insufficient Alone

`@jit.dont_look_inside` prevents the JIT from tracing INTO a function, but the JIT
still builds a call descriptor FROM the caller's perspective.  That descriptor includes
argument and return types.  If those contain r_uint128, it crashes — the annotation
makes no difference.

`dont_look_inside` only avoids the crash when placed on the **caller of the
r_uint128-returning function**, making that caller itself opaque.  This chains upward:
to protect a caller G that calls `get_pk()` (returns r_uint128), G must be marked
`dont_look_inside`.  And any caller of G must also not directly use r_uint128, or
also be marked `dont_look_inside`.

The decisive blocker: **`run_vm` itself uses r_uint128 as local variables** (lines
188–189, 211 in `gnitz/vm/interpreter.py`) for the OPCODE_INTEGRATE handler, and calls
`memtable.upsert_single(ck, …)` where `ck` is r_uint128.  `run_vm` is the JIT portal —
it cannot be marked `dont_look_inside`.  So even marking every operator with
`dont_look_inside` would not fix the crash: the JIT would still see the r_uint128
locals in `run_vm`'s own graph and the r_uint128 arg in the `upsert_single` residual
call.

A pure Option A approach would therefore produce at best a JIT that only compiles the
outer dispatch skeleton of `run_vm`, with all data-touching code completely opaque.
For a database engine where operator work vastly dominates loop dispatch, this yields
negligible performance benefit from the JIT.

## Recommendation: Option B — Full hi/lo Refactoring of the Hot Path

Replace every use of `r_uint128` as a **function parameter or return type** throughout
the JIT hot path with an explicit `(lo: r_uint64, hi: r_uint64)` pair.  The cold path
(WAL serialization, manifest I/O, catalog DDL) can keep r_uint128 locally since it is
never JIT-traced.

**Why this is the right call:**

- Storage already internally stores all 128-bit PKs as two 64-bit halves
  (`pk_lo_buf`/`pk_hi_buf` in `ArenaZSetBatch`, `pk_min_lo`/`pk_min_hi` in
  `ManifestEntry`, etc.).  The r_uint128 type only appears at API call boundaries as a
  transient assembled value.  Changing signatures is therefore largely mechanical.
- The JIT's primary benefit for a query engine is tracing through operator inner loops
  (join merge walks, reduce group key assembly, accessor dispatch).  This requires the
  operators to be fully traceable, not opaque.
- The hot path represents roughly 20 functions across 10 files — manageable scope.

---

## Site-by-Site Analysis

### HOT PATH — Must Change (Option B)

Each site is reachable (directly or transitively) from `run_vm`'s `jit_merge_point`.

#### 1. `gnitz/vm/interpreter.py` — `run_vm` (lines 188–189, 211)

```python
# CURRENT (lines 188-189)
ck = ((r_uint128(gc_u64) << 64) | r_uint128(r_uint64(intmask(source_pk))))
# CURRENT (line 211)
ck = (r_uint128(gc_u64) << 64) | r_uint128(av_u64)
```

`ck` is passed to `memtable.upsert_single(ck, weight, acc)`.  Both the local variable
and the call argument must go.

**Fix:** Replace `ck: r_uint128` with `(ck_lo: r_uint64, ck_hi: r_uint64)` and change
`upsert_single` to accept lo/hi (see §3 below).  `source_pk` at line 187 is also an
r_uint128 local; eliminate it by calling `b.get_pk_lo(idx)` and `b.get_pk_hi(idx)`
directly (see §3 — storage already stores lo/hi internally in `pk_lo_buf`/`pk_hi_buf`;
`get_pk()` currently *assembles* r_uint128 from them, so splitting is mechanical).

**Note:** both halves of `source_pk` are used: `intmask(source_pk)` feeds `ck_lo` and
`intmask(source_pk >> 64)` feeds `gi_acc.spk_hi` at line 190.  The fix needs
`get_pk_lo(idx)` for the former and `get_pk_hi(idx)` for the latter.

---

#### 1b. `gnitz/vm/interpreter.py` — `run_vm` OPCODE_SEEK_TRACE (lines 70–76)

```python
key = reg_k.batch.get_pk(0)          # r_uint128 local in run_vm
ops.op_seek_trace(reg_t.cursor, key)  # r_uint128 arg passed to seek
```

This is inside `run_vm`'s main loop — also a JIT portal site.  Not covered by §1 above.

**Fix:** Once `get_pk` returns lo/hi (§3) and `cursor.seek` takes lo/hi (§5), change
to `ops.op_seek_trace(reg_t.cursor, key_lo, key_hi)`.  Also update `op_seek_trace`
itself (source.py:48): change signature to `op_seek_trace(cursor, key_lo, key_hi)` and
body to `cursor.seek(key_lo, key_hi)`.

---

#### 1c. `gnitz/dbsp/ops/source.py` — `op_scan_trace` (lines 32–44)

Completely absent from the original plan.  `op_scan_trace` is called directly from
`run_vm`'s scan opcode handler — it is on the JIT hot path.

```python
key = cursor.key()                                  # r_uint128 local
...
out_writer.append_from_accessor(key, weight, accessor)   # r_uint128 arg
```

**Fix:** After `cursor.key()` returns lo/hi (§5) and `append_from_accessor` takes lo/hi
(§3), change to:
```python
key_lo, key_hi = cursor.key_lo(), cursor.key_hi()
...
out_writer.append_from_accessor(key_lo, key_hi, weight, accessor)
```
No signature change to `op_scan_trace` itself is needed (cursor and writer are passed
in; the r_uint128 is a pure local).

---

#### 2. `gnitz/storage/memtable.py` — `MemTable.upsert_single`, `upsert_batch`

Both take `pk: r_uint128`.

**Fix:** Change signatures to `upsert_single(pk_lo, pk_hi, weight, acc)` and
`upsert_batch(batch)` (batch already carries lo/hi internally so `upsert_batch` signature
does not change).  Update all callers.

**Note:** `upsert_batch` body calls `self.bloom.add(batch.get_pk(i))` at line 49 — when
`bloom.add()` changes to accept `(key_lo, key_hi)` (§7), this call site must also be
updated to `self.bloom.add(batch.get_pk_lo(i), batch.get_pk_hi(i))`.

---

#### 3. `gnitz/core/batch.py` — `ArenaZSetBatch.get_pk`, `_read_col_u128`, `append_from_accessor`, `RowBuilder.begin`, `RowBuilder.get_u128`

- `get_pk(i) → r_uint128` — called in every operator merge walk
- `_read_col_u128(i, col_idx) → r_uint128` — called by `get_u128()`
- `append_from_accessor(pk, weight, acc)` where `pk: r_uint128`
- `RowBuilder.begin(pk, weight)` where it assembles `pk_u128 = r_uint128(pk)`
- `RowBuilder.get_u128(col_idx) → r_uint128`

**Fix:**
- `get_pk(i) → (lo: r_uint64, hi: r_uint64)` OR split into `get_pk_lo(i)` /
  `get_pk_hi(i)`.  Since RPython cannot return tuples cleanly in JIT-traced code, the
  cleanest approach is two separate methods `get_pk_lo`/`get_pk_hi`.  **Note:** only
  private `_read_pk_lo`/`_read_pk_hi` exist today — the public pair must be added.
- `_read_col_u128` → keep internal; expose as `get_col_u128_lo`/`get_col_u128_hi` or
  fold the lo/hi decomposition at the few call sites (comparator, serialize).
- `append_from_accessor(pk_lo, pk_hi, weight, acc)` — all callers pass assembled
  r_uint128 today; change to pass lo/hi directly.
- `RowBuilder.begin(pk_lo, pk_hi, weight)` — remove the `r_uint128(pk)` assembly.
- `RowBuilder.commit()` — currently assembles `pk = (r_uint128(_pk_hi) << 64) | r_uint128(_pk_lo)`
  to call `append_from_accessor`.  After that signature changes, `commit()` passes
  `self._pk_lo, self._pk_hi` directly instead.
- `RowBuilder.get_u128(col_idx)` — either remove entirely (callers go through lo/hi)
  or keep for cold paths.

---

#### 4. `gnitz/core/comparator.py` — `compare_rows`, `NullAccessor.get_u128`

`compare_rows` is `@jit.unroll_safe` and calls `acc.get_u128(i)` returning r_uint128.

**Fix:** Change `get_u128(col_idx)` across the accessor interface to
`get_u128_lo(col_idx)` / `get_u128_hi(col_idx)`.  `compare_rows` compares the 128-bit
value — replace with lexicographic compare on (hi, lo) pair.  `NullAccessor.get_u128`
similarly returns two zeros.

---

#### 5. `gnitz/storage/cursor.py` and `gnitz/storage/shard_table.py` — all cursor types

`SortedBatchCursor`, `MemTableCursor`, `ShardCursor`, and `UnifiedCursor` all expose
`key() → r_uint128`, `peek_key() → r_uint128`, and `seek(key: r_uint128)`.  Called in
every operator merge walk loop.

`UnifiedCursor` additionally assembles r_uint128 in `key()` from its cached
`_current_key_lo`/`_current_key_hi` fields:
```python
def key(self):
    return (r_uint128(self._current_key_hi) << 64) | r_uint128(self._current_key_lo)
```
and uses `r_uint128(-1)` as an exhaustion sentinel in `_find_next_non_ghost`.

**`ShardCursor` delegates entirely to `shard_table.py` — that file must also change.**
`ShardCursor.key()` calls `self.view.get_pk_u128(self.position)` or wraps
`get_pk_u64()` in r_uint128.  `ShardCursor.seek(target_key)` calls
`self.view.find_lower_bound(target_key)`, which is a binary search that assembles
`mid_key = r_uint128(hi) << 64 | r_uint128(lo)` and compares `mid_key >= key`.
The same pattern appears in `find_row_index()`.

Fix for `shard_table.py`:
- `get_pk_lo(index)` / `get_pk_hi(index)` — read `pk_lo_buf` / `pk_hi_buf` directly;
  replace `get_pk_u128()` at the call sites in `ShardCursor.key_lo()`/`key_hi()`.
- `find_lower_bound(key_lo, key_hi)` — binary search uses `pk_lt(mid_lo, mid_hi,
  key_lo, key_hi)` for the comparison instead of assembling a mid r_uint128.
  Same change for `find_row_index()`.
- `get_pk_u128()` is then only used by cold-path callers (compaction, index init);
  no change needed there.

`MemTableCursor.seek()` also has an internal binary search with
`self._snapshot.get_pk(mid) < target_key` — once `get_pk()` returns lo/hi (§3),
this comparison must also use `pk_lt`.

**Fix:** Split into `key_lo()`/`key_hi()` across all cursor types.  All merge walk
comparisons like `cursor.key() < d_key` become lexicographic compares on hi then lo.
All `cursor.seek(key: r_uint128)` become `seek(key_lo, key_hi)`.  Write `pk_lt` /
`pk_eq` helpers.  Replace `r_uint128(-1)` sentinel comparisons with
`key_hi() == MAX_U64 and key_lo() == MAX_U64` or equivalent.

This is the highest-impact site for correctness: the merge walk comparison logic
`while trace_cursor.key() < d_key` must be faithfully reproduced with lo/hi comparison.
A helper `@jit.unroll_safe` function `pk_lt(a_lo, a_hi, b_lo, b_hi) → bool` avoids
repeated boilerplate.

**`UnifiedCursor._find_next_non_ghost()` must also be updated at both entry points:**

- *Single-source path* (cursor.py lines 290–292): `k = cursor.key()` then inline
  `intmask` decomposition to `_current_key_lo`/`_current_key_hi`.  After `cursor.key()`
  returns lo/hi, replace with `self._current_key_lo = cursor.key_lo()` etc.
- *Multi-source path* (cursor.py lines 301, 316–317): `min_key = self.tree.get_min_key()`
  then `intmask(min_key)` / `intmask(min_key >> 64)`.  See §6 for the `get_min_key()`
  change; once it returns lo/hi the intmask decomposition becomes a direct read.

---

#### 6. `gnitz/storage/tournament_tree.py` — `get_min_key`, `rebuild`, `advance_cursor_by_index`

Four distinct sites, not one:

**`_get_key(idx)` and `get_min_key()`:** `_get_key` assembles r_uint128 from struct
fields; `get_min_key()` calls `_get_key(0)` and returns the same.  `_compare_nodes`
does **not** use `_get_key` — it reads `key_high`/`key_low` directly, so no comparison
logic needs changing.

**Fix:** Change `get_min_key()` to return `(lo: r_uint64, hi: r_uint64)` by reading
struct fields directly (matching `_compare_nodes`).  Replace the `r_uint128(-1)` sentinel
with `(MAX_U64, MAX_U64)`.  `_get_key` can be removed.  Update the caller in
`UnifiedCursor._find_next_non_ghost()` (see §5).

**`rebuild()` (lines 64–65)** and **`advance_cursor_by_index()` (lines 227–228)** both
call `cursor.peek_key()` and decompose inline with `intmask`:
```python
self.heap[idx].key_low  = rffi.cast(rffi.ULONGLONG, r_uint64(intmask(nk)))
self.heap[idx].key_high = rffi.cast(rffi.ULONGLONG, r_uint64(intmask(nk >> 64)))
```
After `peek_key()` splits into `peek_key_lo()`/`peek_key_hi()` (§5), replace with:
```python
self.heap[idx].key_low  = rffi.cast(rffi.ULONGLONG, cursor.peek_key_lo())
self.heap[idx].key_high = rffi.cast(rffi.ULONGLONG, cursor.peek_key_hi())
```

---

#### 7. `gnitz/storage/bloom.py` and `gnitz/storage/xor8.py` — `_hash_key(key: r_uint128)`

Both filters are `@jit.elidable` and accept r_uint128.

**Fix:** Change to `_hash_key(key_lo, key_hi)`.  `BloomFilter._hash_key` already splits
its argument internally (`lo = r_uint64(intmask(key))`, `hi = r_uint64(intmask(key>>64))`)
before calling `xxh.hash_u128_inline(lo, hi)` — so the change is purely to the signature.
`Xor8Filter._hash_key` does the same split before calling
`xxh.hash_u128_inline(lo, hi, seed_lo, seed_hi)`.  Note: the two callers use different
arities of `hash_u128_inline` (2-arg for bloom, 4-arg for xor8); both are already split
internally.

`Xor8Filter._get_seed()` returns r_uint128 — cold path, change to internal pair or
keep cold.

---

#### 8. `gnitz/storage/comparator.py` — `SoAAccessor.get_u128()`

Returns r_uint128; internal reads are already split on lo/hi pointers.

**Fix:** Change to `get_u128_lo(col_idx)` / `get_u128_hi(col_idx)` matching the
accessor interface change from §4.

---

#### 9. `gnitz/dbsp/ops/group_index.py` — `_extract_group_key()`, `GroupIdxAccessor.get_u128()`

`_extract_group_key` returns r_uint128; called from reduce integrate and exchange.

**Fix:** Return `(key_lo, key_hi)`.  All callers that previously assembled a single r_uint128
to pass to `upsert_single` or `partition_for_key` switch to the lo/hi pair.

`GroupIdxAccessor.get_u128()` is part of the accessor interface — fix with §4.

---

#### 9b. `gnitz/dbsp/ops/linear.py` — `_promote_col_to_u128()`, `_op_union_merge()`, `op_map()`

Completely absent from the original plan.  Three hot-path sub-sites:

**`_promote_col_to_u128()` (lines 21–47):** Returns r_uint128.  Structurally identical
to `_extract_group_key()` (§9) — promotes a single column value (U128, I64, U64,
string, float) to a 128-bit key for reindexing.  Called from `op_map` when
`reindex_col >= 0`.  Must return `(key_lo, key_hi)`.

**`_op_union_merge()` (lines 126–151):** A merge walk over two sorted batches —
identical structure to the join merge walks in §11 — but missing from the plan:
```python
pk_a = batch_a.get_pk(i)    # r_uint128
pk_b = batch_b.get_pk(j)    # r_uint128
if pk_a < pk_b:             # r_uint128 comparison
elif pk_b < pk_a:
```
`op_union` fires every circuit tick (all delay/integrate paths).  Needs the same
`pk_lt(a_lo, a_hi, b_lo, b_hi)` treatment as §11.  Replace `pk_a`/`pk_b` locals with
`(pk_a_lo, pk_a_hi)` / `(pk_b_lo, pk_b_hi)` pairs; comparisons become `pk_lt` calls.

**`op_map()` line 107 (non-reindex path):**
```python
builder.commit_row(in_batch.get_pk(i), in_batch.get_weight(i))
```
Once `get_pk()` splits (§3) and `commit_row` takes lo/hi (§4), change to
`builder.commit_row(in_batch.get_pk_lo(i), in_batch.get_pk_hi(i), ...)`.

---

#### 10. `gnitz/dbsp/ops/reduce.py` — `ReduceAccessor.get_u128()`, `_compare_by_cols()`, and hot-path cursor sites

`ReduceAccessor.get_u128()` and `_compare_by_cols()` are accessor interface and
comparison — fix with §4/§5.

**Additional hot-path sites inside `op_reduce` and `_apply_agg_from_value_index`
not covered by the "rewrite group_key local" note in the implementation order:**

**`_apply_agg_from_value_index()` line 302:**
```python
avi_cursor.seek(r_uint128(gc_u64) << 64)
while avi_cursor.is_valid():
    k = avi_cursor.key()
    if r_uint64(intmask(k >> 64)) != gc_u64: break  # extracts hi bits
    encoded = r_uint64(intmask(k))                  # extracts lo bits
```
After §5: `avi_cursor.seek(r_uint64(0), gc_u64)` (zero lo, gc_u64 hi); loop body
uses `k_hi` for the prefix check and `k_lo` for the encoded value.

**`op_reduce` GI walk, lines 497–519 — composite seek key reconstruction:**
```python
target_prefix = r_uint128(gc_u64) << 64
gi_cursor.seek(target_prefix)
while gi_cursor.is_valid():
    gk = gi_cursor.key()
    if r_uint64(intmask(gk >> 64)) != gc_u64: break
    spk_lo = r_uint128(r_uint64(intmask(gk)))
    spk_hi = r_uint128(rffi.cast(rffi.ULONGLONG, gi_cursor.get_accessor().get_int_signed(1)))
    src_pk = (spk_hi << 64) | spk_lo
    trace_in_cursor.seek(src_pk)
    if trace_in_cursor.is_valid() and trace_in_cursor.key() == src_pk: ...
```
This is structurally different from "rewriting group_key" — it reconstructs a seek
key from two different sources (cursor lo bits + accessor field).  After §5:
`gi_cursor.seek(r_uint64(0), gc_u64)`; `gk_hi` for prefix check; `gk_lo` for
`spk_lo_u64`; `spk_hi_u64` from accessor; `trace_in_cursor.seek(spk_lo_u64, spk_hi_u64)`
and `trace_in_cursor.key_lo() == spk_lo_u64 and key_hi() == spk_hi_u64` for equality.

**`op_reduce` fallback trace seek, line 520:**
```python
trace_in_cursor.seek(r_uint128(0))
```
Becomes `trace_in_cursor.seek(r_uint64(0), r_uint64(0))`.

---

#### 11. `gnitz/dbsp/ops/join.py`, `anti_join.py` — merge walk functions

`_join_dt_merge_walk`, `_join_dt_outer_merge_walk`, `_anti_join_dt_merge_walk`,
`_semi_join_dt_merge_walk`, `_semi_join_dt_swapped` — every loop body has this
pattern:

```python
d_key = delta_batch.get_pk(i)          # r_uint128 local
...
while trace_cursor.key() < d_key: ...  # r_uint128 comparison
while trace_cursor.key() == d_key: ... # r_uint128 comparison
out_writer.append_from_accessor(d_key, w_out, composite_acc)  # r_uint128 arg
```

**Fix:** After steps 3 and 5 (get_pk_lo/hi and cursor key_lo/hi), every one of these
sites must be explicitly rewritten.  Replace `d_key` with a `(d_key_lo, d_key_hi)`
pair; replace comparisons with `pk_lt`/`pk_eq` helper calls; replace
`append_from_accessor(d_key, ...)` with `append_from_accessor(d_key_lo, d_key_hi, ...)`.
This is mechanical but **not automatic** — each merge walk function needs an explicit
pass.  There are approximately 6 such sites across the four functions.

---

#### 12. `gnitz/dbsp/ops/exchange.py` — `hash_row_by_columns`

Calls `_extract_group_key` (§9) which returns r_uint128.

**Fix:** Consume the lo/hi return directly.  No signature change to `hash_row_by_columns`
itself since it only uses the value internally.

---

#### 13. `gnitz/server/executor.py` and `gnitz/server/worker.py` — scan, seek, and index-lookup functions

These functions are cold-path for the JIT but must change because the cursor and
accessor APIs are system-wide.

**executor.py** has four sites:

- `_scan_family()` line 418: `result.append_from_accessor(r_uint128(pk), w, acc)` where
  `pk = cursor.key()` — straightforward once §3 and §5 are done.
- `_seek_family()` (lines 424–445): assembles `key = r_uint128(...)` from lo/hi args,
  calls `cursor.seek(key)`, checks `cursor.key() != key`, calls
  `result.append_from_accessor(key, w, acc)`.  Fix: drop the r_uint128 assembly; pass
  `(pk_lo, pk_hi)` through directly to `cursor.seek`, use lo/hi equality check, pass
  lo/hi to `append_from_accessor`.
- `_seek_by_index()` (lines 447–478): same key-assembly pattern plus `source_pk =
  idx_cursor.get_accessor().get_u128(1)` (U128 branch) or `r_uint128(r_uint64(get_int(1)))`
  (U64 branch), followed by `intmask` decomposition to `src_lo`/`src_hi`.  Fix: after
  accessor splits (§1 / §4), use `get_u128_lo(1)` / `get_u128_hi(1)` for the U128 branch;
  U64 branch becomes `get_int(1)` for lo and `r_uint64(0)` for hi.

**worker.py** has five sites:

- `_handle_scan()` line 253: `result.append_from_accessor(r_uint128(pk), w, acc)` —
  same as executor scan fix.
- `_handle_seek()` (lines 264–285): same as `_seek_family` — assembles r_uint128 from
  lo/hi args, seeks, compares, appends.  Same fix.
- `_handle_seek_by_index()` (lines 287–316): same as `_seek_by_index` — assembles key,
  seeks, extracts `source_pk` via `get_u128(1)` or int cast.  Same fix.
- `_handle_backfill()` line 332: `local_batch.append_from_accessor(r_uint128(pk), w, acc)`
  — same as scan fix.

---

#### 14. `gnitz/catalog/hooks.py` — `ingest_scan_batch`

Same pattern as §13.

---

#### 14b. `gnitz/storage/memtable.py` — `_merge_runs_to_consolidated` and internal lookup functions

Not on the JIT hot path, but must change because cursor and batch APIs are system-wide.

**`_merge_runs_to_consolidated()` (lines 181–244):** Uses `SortedBatchCursor.key()` to
get r_uint128, compares `cur_pk != pending_pk`, and calls
`consolidated.append_from_accessor(pending_pk, ...)` at three places.  After
`SortedBatchCursor.key()` returns lo/hi and `append_from_accessor` takes lo/hi, rewrite
`pending_pk` as a `(pending_pk_lo, pending_pk_hi)` pair; equality check becomes
`pk_eq`; append calls pass the pair directly.

**`MemTable.lookup_pk(key)` and `find_weight_for_row(key, accessor)`:** Both take
`key: r_uint128`, drive binary searches via `_lower_bound(run, key)` (which calls
`get_pk(mid) < key`), and compare `get_pk(lo) == key`.  Change all three function
signatures to `(key_lo, key_hi)` and replace comparisons with `pk_lt` / `pk_eq`.

**`MemTable.may_contain_pk(key)`:** Passes r_uint128 to `bloom.may_contain(key)`.
After bloom changes (§6 / §7), change to `bloom.may_contain(key_lo, key_hi)`.

**`_lower_bound(run, key)` and `_binary_search_weight(run, key)`:** Free functions
taking r_uint128 key.  Change signatures to `(run, key_lo, key_hi)` and use `pk_lt` /
`pk_eq` for comparisons.

---

### COLD PATH — Leave as r_uint128 (Option A / no change)

These are never reachable from the JIT-traced region.  Changing them would add noise
without benefit.

| Site | Why cold |
|------|----------|
| `gnitz/storage/wal_layout.py` — `read_u128`/`write_u128` | WAL I/O, never in operator loop |
| `gnitz/storage/metadata.py` — `ManifestEntry.get_min_key`/`get_max_key` | Compaction bookkeeping |
| `gnitz/storage/index.py` — `ShardHandle.get_min_key`/`get_max_key` | Shard index init |
| `gnitz/storage/compactor.py` | Compaction, not in operator loop |
| `gnitz/storage/flsm.py` | Compaction / index routing |
| `gnitz/core/keys.py` — `promote_to_index_key` | FK validation during DDL |
| `gnitz/core/serialize.py` — `compute_hash` | Hash computation, DDL path |
| `gnitz/catalog/engine.py` — `_read_view_deps` | DDL |
| `gnitz/catalog/system_tables.py`, `catalog/metadata.py`, etc. | All catalog DDL |
| `gnitz/server/ipc.py` | IPC wire format, not operator loop |

---

## The Accessor Interface — Central Change

Most of the r_uint128 usage funnels through the accessor interface: every class that
implements `get_u128(col_idx) → r_uint128` needs to change to
`get_u128_lo(col_idx) → r_uint64` / `get_u128_hi(col_idx) → r_uint64`.

Affected accessor implementations:
- `ColumnarBatchAccessor` (batch.py)
- `RowBuilder` (batch.py, as virtual accessor)
- `NullAccessor` (comparator.py)
- `SoAAccessor` (storage/comparator.py)
- `ReduceAccessor` (ops/reduce.py)
- `GroupIdxAccessor` (ops/group_index.py)
- `CompositeAccessor` (ops/join.py) — **do not omit**: this is `@jit.unroll_safe`,
  directly on the join merge walk hot path, and implements `get_u128()` at lines 101–105

Callers of `get_u128`:
- `compare_rows` (core/comparator.py)
- `compute_hash` (cold — leave as-is, can keep old interface or call lo/hi separately)
- `append_from_accessor` (batch.py, cold-ish path within operator)

---

## Implementation Order

Bottom-up, each step keeps tests passing:

1. **Accessor interface** — Add `get_u128_lo`/`get_u128_hi` to all accessor classes.
   Keep old `get_u128` on cold-path-only callers for now.

2. **Add `get_pk_lo`/`get_pk_hi` to `ArenaZSetBatch`** — only private `_read_pk_lo`/
   `_read_pk_hi` exist today; add the public wrappers.  Then audit all call sites that
   assemble `get_pk()` into r_uint128 and switch them to the lo/hi pair.

3. **`cursor.key()` / `peek_key()` / `seek()` → lo/hi, plus all consumers** — Change
   all cursor types in cursor.py to split `key()` → `key_lo()`/`key_hi()`, `peek_key()`
   → `peek_key_lo()`/`peek_key_hi()`, and `seek(key)` → `seek(key_lo, key_hi)`.
   Write `pk_lt` / `pk_eq` helpers.
   **Also change `shard_table.py`**: add `get_pk_lo(idx)`/`get_pk_hi(idx)` reading the
   buffers directly; change `find_lower_bound(key_lo, key_hi)` and `find_row_index()`
   to use `pk_lt` for mid-key comparison.  Update `MemTableCursor.seek()`'s internal
   binary search to use `pk_lt`.
   **`TournamentTree`** (§6): update `rebuild()` and `advance_cursor_by_index()` to use
   `peek_key_lo()`/`peek_key_hi()`; change `get_min_key()` to return `(lo, hi)`.
   **`UnifiedCursor._find_next_non_ghost()`** (§5): update both the single-source path
   (direct `cursor.key_lo()`/`key_hi()` reads) and multi-source path (`get_min_key()`
   lo/hi pair, replace `r_uint128(-1)` sentinel check).
   **`op_scan_trace` and `op_seek_trace`** (§1c / §1b): update bodies to use lo/hi cursor
   API and lo/hi `append_from_accessor`.
   Explicitly rewrite all merge walk bodies in `join.py` and `anti_join.py` (§11):
   replace every `d_key` local and every `cursor.key() < d_key` / `== d_key` comparison.
   Rewrite `_op_union_merge()` in `linear.py` (§9b) the same way.
   Also rewrite the r_uint128 `group_key` local and its cursor/append call sites in
   `op_reduce` (lines 422–561), plus all cursor seek sites in `_apply_agg_from_value_index`
   (line 302) and the GI walk (lines 497–519) and fallback seek (line 520) — see §10
   for the specific lo/hi decompositions required at each site.

4. **`append_from_accessor` / `RowBuilder.begin`/`commit_row`** — Change to lo/hi pk
   arguments.  Update `op_map` line 107 and `_op_union_merge` row copy calls (§9b).
   Also update `_merge_runs_to_consolidated`, `_lower_bound`, `_binary_search_weight`,
   `MemTable.lookup_pk`, `find_weight_for_row`, `may_contain_pk` (§14b), and the
   server/worker lookup functions `_seek_family`, `_seek_by_index`, `_handle_seek`,
   `_handle_seek_by_index` (§13).

5. **`memtable.upsert_single`** — Change to `(pk_lo, pk_hi, weight, acc)`.

6. **`bloom` / `xor8` filter `_hash_key`** — Change to `(key_lo, key_hi)`.

7. **`_extract_group_key` and `_promote_col_to_u128`** — Change both to return
   `(key_lo, key_hi)`.  (`_promote_col_to_u128` in `linear.py` is the MAP-operator
   analogue of `_extract_group_key` in `group_index.py`; both need the same treatment.)

8. **`run_vm` OPCODE_INTEGRATE** — Replace `ck: r_uint128` with `(ck_lo, ck_hi)`.
   At this point all callee signatures accept lo/hi, so this is straightforward.

9. **Run `make test`** to confirm no regressions.

10. **Run `make release-server`** to confirm JIT compilation succeeds.

---

## Expected Outcome

After this refactoring:
- `make release-server` succeeds
- The JIT can fully trace `run_vm`, all operator merge walks, accessor dispatch, bloom
  filter lookups, and group key assembly
- Cold-path code (WAL, catalog, compaction) is unchanged
- The stub JitDriver in `rpython_tests/helpers/jit_stub.py` remains but becomes
  redundant for test binaries that exercise the VM (which will have the real portal)
