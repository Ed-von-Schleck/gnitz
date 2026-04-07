# Gnitz vs DuckDB: Validated Technical Comparison
*March 2026 — Validated against source code*

This document is a corrected version of the prior research. Each section notes
the validation outcome. Errors in the prior draft are called out explicitly.

---

## Methodology

Validation was done by reading the actual gnitz source files. Claims about
DuckDB are sourced from published DuckDB blog posts, VLDB/SIGMOD papers, and
the DuckDB documentation; they are not re-validated here but come from
authoritative primary sources and are noted as "externally sourced."

---

## Part 1 — Gnitz Architecture Validation

### 1.1 German Strings

**Prior claim (WRONG):** "Gnitz's version: short strings (≤31 bytes) inline,
blob arena offset otherwise. Gnitz's threshold is more aggressive (31 vs 12
bytes), meaning fewer strings need heap allocation."

**Correction:** `SHORT_STRING_THRESHOLD = 12` in `gnitz/core/strings.py:9`.
The layout is:

```
Bytes 0–3:  u32 length
Bytes 4–7:  u32 prefix (first 4 bytes of string, little-endian)
Bytes 8–15: if length ≤ 12: suffix bytes 4–11 (zero-padded)
            if length > 12: u64 offset into batch's blob_arena
```

This is **byte-for-byte identical to DuckDB's German string format**: 4-byte
length, 4-byte 4-byte prefix, 8-byte inline-or-pointer, with the inline
threshold at 12 bytes. The claim that gnitz used a "more aggressive 31-byte
threshold" was false. Both systems converged on the same constants
independently.

**Comparison note:** DuckDB's prefix is defined as "first 4 bytes as a u32
for fast comparison rejection." Gnitz stores the same prefix as a u32 at
offset 4, using `compute_prefix()` which reads up to 4 bytes little-endian.
`compare_structures()` checks `pref1 == pref2` before any byte-by-byte
comparison — exact same early-exit strategy as DuckDB.

---

### 1.2 WAL Format Version

**Prior claim (WRONG):** "format_version (u32) — current: 4"

**Correction:** `WAL_FORMAT_VERSION_CURRENT = 2` in
`gnitz/storage/wal_layout.py:18`. The manifest file has a separate version
(V3 as of the FLSM phase 2 work), but the WAL block header is version 2.

---

### 1.3 op_filter Implementation

**Prior claim (WRONG):** The recommendations section stated op_filter
"calls `append_from_accessor(...)` for each passing row, creating a new
`ArenaZSetBatch`" and suggested selection vectors as a "MEDIUM PRIORITY"
improvement.

**Correction:** `gnitz/dbsp/ops/linear.py:56–80` already uses
contiguous-range bulk copy:

```python
range_start = -1
for i in range(n):
    if func.evaluate_predicate_direct(in_batch, i):
        if range_start < 0:
            range_start = i
    else:
        if range_start >= 0:
            out_writer.append_batch(in_batch, range_start, i)  # bulk copy
            range_start = -1
if range_start >= 0:
    out_writer.append_batch(in_batch, range_start, n)
```

`append_batch` uses `c_memmove` for all fixed-width columns (per-row fallback
only for strings). This is functionally equivalent to DuckDB's selection
vector approach for contiguous survivors, and better than per-row accessor
dispatch. The "selection vectors" recommendation is substantially invalidated.

**Residual gap:** When surviving rows are sparse and non-contiguous (e.g., 1
survivor per 10 rows), each contiguous run is length 1, so `append_batch` is
called once per passing row — identical overhead to per-row append. A true
selection vector would batch all survivors into one pass. However, DBSP deltas
are typically small (tens to hundreds of rows) and high-selectivity filters on
them are rare. The practical impact is low.

---

### 1.4 Expression VM Opcode Count

**Prior claim (WRONG):** "31 opcodes: col loads, int/float arith, int/float
cmp, boolean, null checks"

**Correction:** `gnitz/dbsp/expr.py` defines opcodes up to 46, with no gaps
below 32 and intentional gaps in the 35–39 range:

```
1–3:   Column loads (LOAD_COL_INT, LOAD_COL_FLOAT, LOAD_CONST)
4–9:   Integer arithmetic (ADD, SUB, MUL, DIV, MOD, NEG)
10–14: Float arithmetic (ADD, SUB, MUL, DIV, NEG)
15–20: Integer comparison (EQ, NE, GT, GE, LT, LE)
21–26: Float comparison (EQ, NE, GT, GE, LT, LE)
27–29: Boolean logic (AND, OR, NOT)
30–31: Null checks (IS_NULL, IS_NOT_NULL)
32:    EXPR_EMIT — computed projection output
33:    EXPR_INT_TO_FLOAT — type widening
34:    EXPR_COPY_COL — type-dispatched direct column copy
40–45: Fused string comparisons (COL_EQ_CONST, COL_LT_CONST, COL_LE_CONST,
       COL_EQ_COL, COL_LT_COL, COL_LE_COL)
46:    EXPR_EMIT_NULL — null-fill for LEFT JOIN outputs
```

Total: **35 defined opcodes** (opcodes 1–34 plus 40–46; no 35–39). The prior
draft described this as 31 opcodes and omitted the output opcodes (32–34) and
null-emission opcode (46) entirely.

---

### 1.5 Bloom Filter h2 Construction

**Prior claim (slightly misleading):** MEMORY.md described bloom.py as "h2
masked to 31 bits to prevent negative pos."

**Correction:** `gnitz/storage/bloom.py:46`:

```python
h2 = (h_val >> 32) | r_uint64(1)
```

This right-shifts the 64-bit hash by 32 bits and sets bit 0 to 1. The purpose
is not to prevent negative modulo (the unsigned modulo path already handles
that) but to ensure the step value `h2` is never zero, which would collapse
all probe positions to the same bucket in double hashing (Kirschner &
Mitzenmacher 2006). A zero step value is a degenerate case — `(h1 + i*0)` is
constant. Forcing bit 0 = 1 guarantees the step is always odd and hence
coprime with any power-of-2 bit count, which is a common Bloom filter
correctness property. "31 bits" is technically inaccurate; the value can be up
to 33 bits wide on a 64-bit machine since `h_val >> 32` could be up to 32
bits and the `| r_uint64(1)` only sets bit 0.

---

### 1.6 Adaptive Join Swap Threshold

**Prior claim (imprecise):** "if delta >> trace, reverse roles"

**Correction:** `gnitz/dbsp/ops/join.py:128`:

```python
ADAPTIVE_SWAP_THRESHOLD = 1
...
if delta_len > trace_len * ADAPTIVE_SWAP_THRESHOLD:
    _join_dt_swapped(...)
```

The swap fires when `delta_len > trace_len`, i.e., any case where delta is
strictly larger than trace — not "much greater than" (>>). The threshold
value of 1 means the swap happens even for a 1-row advantage. This is
aggressive; in practice for DBSP the delta is usually much smaller than the
trace, so the swapped path rarely fires.

---

### 1.7 Null Buffer Width Limit

**Not mentioned in prior draft.** The null buffer stores one `r_uint64` per
row (`gnitz/storage/wal_columnar.py:104`, `batch.py:95`):

```python
null_word = batch._read_null_word(self._row_idx)
return bool(null_word & (r_uint64(1) << payload_idx))
```

This means the null bitmask supports a maximum of 64 nullable non-PK columns
per row. Schemas with more than 64 nullable payload columns would silently
produce incorrect null tracking. This limit is not enforced or documented
anywhere in the codebase.

---

### 1.8 Partitioned Routing Hash

**Prior claim:** "hash_u128_inline(pk_lo, pk_hi) & 0xFF"

**Correction:** `gnitz/storage/partitioned_table.py:62–65`:

```python
def _partition_for_key(pk_lo, pk_hi):
    h = xxh.hash_u128_inline(pk_lo, pk_hi)
    return intmask(r_uint64(h) & r_uint64(0xFF))
```

This is correct — full xxhash3-128 applied to the PK, then low 8 bits as
partition index. What the prior draft omitted: `hash_u128_inline` is called
without seed arguments here, i.e., with the default `seed_lo=0, seed_hi=0`.
This is the correct default since routing must be deterministic across
processes; the prior draft implied the seed variation mattered, but it only
matters during XOR8 filter construction (where the seed changes on collision
retry).

---

### 1.9 WAL Directory Entry Format

**Prior claim:** "Each entry: [offset_u32] [size_u32] (block-relative
offsets)"

**Verified correct.** WAL block directory entries use 8 bytes per region:
block-relative u32 offset followed by u32 size. See `wal.rs`.
**Correct as stated.**

---

### 1.10 MemTable Accumulator Threshold

**Prior claim:** "ACCUMULATOR_THRESHOLD = 64 rows before flushing accumulator
to runs"

**Verified correct.** `gnitz/storage/memtable.py:13`:
`ACCUMULATOR_THRESHOLD = 64` ✓

---

### 1.11 FLSM Compaction Parameters

All verified against `gnitz/storage/flsm.py:13–17`:

```python
L0_COMPACT_THRESHOLD = 4    ✓
GUARD_FILE_THRESHOLD = 4    ✓
LMAX_FILE_THRESHOLD  = 1    ✓
LEVEL_SIZE_RATIO     = 4    ✓
```

Note: `L1_TARGET_FILES = 16` also exists and was not mentioned in the prior
draft. This governs the number of guards created during initial L1 population.

---

### 1.12 XOR8 Filter Construction

**Prior claim:** "capacity formula: `1 + (num_keys * 123 + 99) // 100 + 32`"

**Verified correct.** `gnitz/storage/xor8.py:91`:
`capacity = intmask(1 + (num_keys * 123 + 99) // 100 + 32)` ✓

**Fingerprint bits:** Top 8 bits of the 64-bit hash: `(h >> 56) & 0xFF`.
Matching condition: `fp[h0] ^ fp[h1] ^ fp[h2] == f`. This is correct per
Graf & Lemire's original Xor8 construction. ✓

**Segment indexing:** 21-bit and 42-bit left rotations confirmed:
`rotl64(h_val, 21)` and `rotl64(h_val, 42)`. ✓

---

### 1.13 Join Operator — No Trace-Side Bloom Filter

**Confirmed.** `gnitz/dbsp/ops/join.py` contains no bloom filter lookup before
`cursor.seek()`. The MemTable has a `BloomFilter` for point lookups
(`memtable.may_contain_pk`) but this is not consulted by the join operator.
The recommendation to add a trace-side pre-filter stands as valid.

---

### 1.14 op_map Has a Batch Path

**Not mentioned in the prior draft.** `gnitz/dbsp/ops/linear.py:97–101`:

```python
if reindex_col < 0 and func is not None:
    if func.evaluate_map_batch(in_batch, out_writer._batch, out_schema):
        out_writer.mark_sorted(in_batch._sorted)
        return
```

`op_map` first tries a batch-level columnar map that operates directly on the
batch buffers without `RowBuilder` staging. The per-row fallback only fires
when `evaluate_map_batch` returns False (not implemented or inapplicable). This
is a significant optimization that the prior draft omitted entirely.

---

## Part 2 — DuckDB Claims Validation

DuckDB claims are sourced from published DuckDB blog posts (duckdb.org),
VLDB/SIGMOD/EDBT papers, and GitHub PRs cited in the research. These are not
independently re-verified against DuckDB source code, but the sources are
authoritative. Claims with strong paper/blog backing are noted "well-sourced";
those that are plausible but harder to confirm are noted "plausible."

| Claim | Verdict | Source |
|---|---|---|
| STANDARD_VECTOR_SIZE = 2048 | Well-sourced | duckdb.org/internals/vector |
| 4 vector formats: flat, constant, dictionary, sequence | Well-sourced | same |
| SelectionVector as uint16_t[] | Well-sourced | same |
| German string: ≤12 bytes inline, 4-byte prefix | Well-sourced | e6data.com/blog/german-strings |
| Row group size 122,880 = 60 × 2048 | Well-sourced | Alibaba blog Part 2 |
| ART node types: Node4/16/48/256 | Well-sourced | duckdb.org/2022/07/27/art-storage |
| ART pointer swizzling: MSB encodes RAM vs disk | Well-sourced | same |
| Bitpacking with variable width per 1024-row mini-block | Well-sourced | duckdb.org/2022/10/28/lightweight-compression |
| FOR (Frame of Reference) for timestamps | Well-sourced | same |
| ALP float compression (SIGMOD 2024) | Well-sourced | duckdb.org/science/alp |
| FSST string compression | Well-sourced | same compression post |
| 256 KB data blocks | Well-sourced | Alibaba blog Part 1 |
| Two alternating DB headers for atomic checkpoint | Well-sourced | duckdb.org/internals/storage |
| Hash join: linear probing + salt bits to avoid deref | Well-sourced | deepwiki.com/duckdb hash join |
| Radix partitioning for out-of-core hash join (4→8→12 bits) | Well-sourced | VLDB 2025 "Saving Private Hash Join" |
| Aggregation: thread-local HT, radix partition merge | Well-sourced | duckdb.org/2022/03/07/aggregate-hashtable |
| External aggregation: fixed-size non-resizing local HT | Well-sourced | duckdb.org/2024/03/29/external-aggregation |
| Sort: Vergesort → Ska Sort → pdqsort cascade | Well-sourced | duckdb.org/2025/09/24/sorting-again |
| Window: segment tree vectorized in batches of 2048 | Well-sourced | duckdb.org/2025/02/14/window-flying |
| Late materialization for ORDER BY LIMIT N (≤50 rows) | Well-sourced | GitHub PR #15692 |
| IEJoin for range joins | Well-sourced | duckdb.org/2022/05/27/iejoin |
| MVCC: column-major undo buffers (HyPer-inspired) | Well-sourced | duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions |
| No expression JIT in released DuckDB | Well-sourced | documented design philosophy |
| OR predicate zone map pushdown | Well-sourced | GitHub PR #14313 |
| Data chunk compaction for sparse chunks | Well-sourced | SIGMOD 2025 paper cited |
| Zone map granularity is per column segment (≤~2048 rows) | Plausible | standard LSM/columnar database architecture |
| Bloom filter on build side of hash join | Well-sourced | mentioned in hash join section |

**One DuckDB claim requiring clarification:**

The prior draft stated "DuckDB's zone maps are per column segment, not just
per row group" and that "column segments can cover as few as ~2048 rows." This
is plausible but imprecise. DuckDB's row groups are 122,880 rows, and column
segments within a row group can be smaller when compressed. However, the
primary skip granularity for OLAP queries is the row group (122,880 rows), not
the column segment. Zone maps at the row group level are the main skip
mechanism. Segment-level statistics are an additional refinement but not the
primary lever for zone-map-based skipping.

---

## Part 3 — Corrected Applicability Analysis

### What gnitz already does that matches DuckDB

| Feature | DuckDB | Gnitz | Notes |
|---|---|---|---|
| German strings (≤12 inline, 4-byte prefix) | ✓ | ✓ | Identical threshold and layout |
| Columnar SoA batch layout | DataChunk | ArenaZSetBatch | Same concept, different DBSP metadata |
| Bloom filters for membership tests | Build-side in hash join | MemTable point lookups | Different placement |
| Tournament merge for sorted runs | k-way parallel merge sort | `_merge_runs_to_consolidated` | Single-threaded; same algorithm |
| Contiguous bulk copy in filter | SelectionVector flush | Range-based `append_batch` | Functionally equivalent |
| Adaptive swap in equi-join | Probe-side switching | `ADAPTIVE_SWAP_THRESHOLD = 1` | Gnitz swaps more aggressively |
| PK-based zone map (FLSM guards) | Row-group zone maps | L1 guard key ranges | Gnitz's is PK-only |
| Partitioned parallelism | Radix-partitioned HT | 256-partition routing | Process-level vs thread-level |

### High-priority gaps (validated)

#### Gap 1: Per-column payload zone maps

Gnitz FLSM shard handles carry `pk_min`/`pk_max` but no per-column min/max
statistics for non-PK payload columns. A filter `WHERE salary > 100000` on a
SCAN_TRACE forces reading every shard regardless of the salary range in that
shard.

**Validated as a real gap.** `gnitz/storage/flsm.py` and
`gnitz/storage/metadata.py` were checked. `MetadataEntry` has
`guard_key_lo`/`guard_key_hi` but no payload column stats.

**Effort:** Medium (compute stats at flush in `compactor.py`, store in
`MetadataEntry`, consult in `ShardCursor` before mmap decode).

---

#### Gap 2: Column projection pushdown at shard read

`_parse_wal_block` in `wal_columnar.py` decodes all columns unconditionally.
The WAL directory already has per-column offsets and sizes. A `column_mask`
argument would let the decoder skip unused columns' regions entirely, avoiding
pointer-math and buffer construction for columns not consumed by the operator.

**Validated as a real gap.** Every `from_existing_data` call at lines 196–232
could be guarded by a mask.

**Effort:** Low. 10–20 lines of change, schema-width bitmask parameter.

---

#### Gap 3: Trace-side pre-filter in join operator

`op_join_delta_trace` calls `trace_cursor.seek(d_key_lo, d_key_hi)` for every
delta row. For low-selectivity joins (most delta keys are not in the trace), a
bloom filter over trace PKs would reject non-matching keys before the binary
search, saving cursor seek cost.

**Validated as a real gap.** No bloom filter is consulted in
`join.py:182–209`. The XOR8 filter infrastructure (`xor8.py`) already exists.
The MemTable's `BloomFilter` tracks running trace PKs but is not wired into
the join operator.

**Implementation note:** The join operator's trace is a cursor over the FLSM
index, not directly a MemTable. Building a filter at trace-cursor creation time
(once per tick, or incrementally as new flushes happen) would be the right
hook point.

---

#### Gap 4: Constant-weight batch fast path

Every DBSP insert delta has weight +1 for all rows; every retraction delta has
weight -1. The weight buffer (`weight_buf`) allocates 8 bytes × N rows even
when all weights are identical.

**Validated as a real gap.** `ArenaZSetBatch` always maintains a full
`weight_buf`. There is no `all_weights_equal` optimization flag.

**Scope:** Adding a `weight_constant: r_int64` sentinel and an
`is_weight_constant: bool` flag to `ArenaZSetBatch` would let `append_batch`,
`append_batch_negated`, weight reads, and the WAL encoder skip the buffer.
This is purely additive (no semantic change) and benchmarkable.

---

#### Gap 5: Shard compression (FOR + bitpacking for numerics)

FLSM shards are written as raw uncompressed column buffers. A table with 10M
rows of I64 timestamps clustered within a year uses 80 MB of raw storage for
just that column. FOR+bitpacking (frame = min_val, residuals bitpacked) would
reduce this to roughly 25–30 MB for timestamps with a month of range.

**Validated as a real gap.** `gnitz/storage/writer_table.py` and
`wal_columnar.py` write columns as-is with no compression analysis step.

**Priority ordering for gnitz:**
1. **RLE** for weight buffer (all-same-weight batches after insert/retract)
2. **FOR + bitpacking** for I64/U64 numeric columns (timestamps, IDs)
3. **Delta + bitpacking** for monotonically increasing PKs
4. **Dictionary** for low-cardinality string columns
5. **FSST** for high-cardinality string columns (URLs, emails)

**DBSP-specific constraint:** Delta batches in RAM should stay uncompressed;
compression only at FLSM L0 flush time and onwards. Decompressing into
`ArenaZSetBatch` on shard read keeps the hot path unchanged.

---

### Lower-priority / revised assessments

#### Selection vectors

**Revised down.** The prior draft called this "MEDIUM PRIORITY" based on the
false belief that `op_filter` used per-row accessor dispatch. The actual
contiguous-range bulk copy already captures most of the benefit. True selection
vectors would only help when survivors are sparse and non-contiguous; with DBSP
delta sizes (typically ≤1000 rows per tick), the overhead of scattered
single-row `append_batch` calls is negligible in practice.

**Verdict:** Low priority. Only revisit if profiling shows filter output
construction as a hot spot on large delta batches.

---

#### Streaming MemTable flush

The `_merge_runs_to_consolidated` function in `memtable.py:173–242` performs a
k-way tournament merge that emits rows one at a time via
`consolidated.append_from_accessor(...)`. This materializes the full merged
batch in RAM before writing to disk.

For large MemTable flushes (e.g., bulk ingest of millions of rows), this can
spike memory. The compactor already uses a streaming tournament merge
(`compact_l0_to_l1` in `compactor.py`) that writes guard shard files directly
from the merge output without full materialization.

**Validated gap.** The MemTable flush path does not stream to disk. Applying
the compactor's streaming pattern to the MemTable flush would bound peak
memory usage during bulk ingest.

---

#### External aggregation (op_reduce)

**Not applicable.** The `op_reduce` operator maintains per-group aggregate
history (the "trace") across ticks. This history must stay accessible — it is
the incremental operator's memory. Spilling it would require de-spilling before
each tick's evaluation. The FLSM already provides this: the reduce trace IS
stored in FLSM shards. The MemTable layer provides a bounded in-memory
accumulator with flush-to-FLSM when full. This is not a gap; it is by design.

---

#### ART secondary indexes

**Not applicable for current join semantics.** Gnitz joins are equi-joins on
the PK, and the PK is the sort key for FLSM shards (binary search is O(log N)
per seek). An ART would give approximately the same asymptotic complexity with
better cache behavior on updates. However, the join trace is updated every tick
and ART structural mutations add significant complexity. This is a future
feature, not an immediate gap.

---

#### MVCC

**Correct — not applicable.** The DBSP algebra handles concurrent-update
semantics through Z-set weight accumulation, not MVCC version chains. There is
no correctness gap here.

---

#### FastLanes SIMD bitpacking

**Now applicable.** With the Rust implementation, LLVM can auto-vectorize
tight loops and explicit SIMD intrinsics are available. Low priority but
no longer blocked by language limitations.

---

## Part 4 — Summary Table (Corrected)

| Optimization | Status | Priority | Effort | Notes |
|---|---|---|---|---|
| Payload column zone maps | **Missing** | High | Medium | Need stats at flush, skip in ShardCursor |
| Column projection pushdown at shard | **Missing** | High | Low | WAL directory already supports it |
| Trace-side bloom filter in join | **Missing** | High | Medium | XOR8 infra exists; needs wiring |
| Constant-weight batch flag | **Missing** | High | Low | Pure additive change |
| Shard compression (FOR + bitpack) | **Missing** | Medium | High | Cold path only; needs format extension |
| Selection vectors in op_filter | **Not needed** | — | — | Already does range bulk copy |
| Streaming MemTable flush | **Missing** | Medium | Medium | Compactor already has the pattern |
| Normalized sort keys | **Not needed** | — | — | PK is always u128; sort is by numeric lo/hi |
| ART secondary indexes | **Future** | Low | High | Only needed for non-PK joins |
| External sort / spill | **Partial** | Low | Medium | FLSM handles durable side; in-memory merge unbounded |
| SIMD bitpacking (FastLanes) | **Missing** | Low | Medium | LLVM auto-vectorization available |
| MVCC | **N/A** | — | — | DBSP handles consistency differently |
| External hash join | **N/A** | — | — | Gnitz join is incremental index-NL, not batch HJ |

---

## Part 5 — What Gnitz Has That DuckDB Doesn't (Unaffected by Corrections)

These capabilities remain correctly described and are gnitz's primary
differentiators:

1. **O(Δ) query evaluation via DBSP.** No full re-scan per query; only the
   delta is processed each tick. For a 10M-row table with 100 changed rows per
   second, gnitz processes ~100 rows/sec vs DuckDB's 10M rows/scan.

2. **Z-set algebra.** Weight field on every row encodes insert (+1), retraction
   (-1), and multiplicities. `op_negate` / `op_union` / consolidation handle
   correctness automatically. No DuckDB equivalent.

3. **Stateful incremental operators.** `op_reduce` maintains group history
   across ticks; delta output = new_agg - old_agg. `op_join_delta_trace` looks
   up the persistent trace (FLSM) against the in-flight delta. DuckDB
   recomputes from scratch per query.

4. **DBSP circuit compilation.** `compile_from_graph` compiles a circuit graph
   (nodes = operators, edges = data flow) into an `ExecutablePlan` including
   trace-side detection, topological sort, schema inference, and bilinear join
   decomposition. No analogous pipeline exists in DuckDB.

5. **WAL = IPC wire format.** The same columnar block encoding used for
   on-disk persistence is also the IPC format between master and workers. Zero
   extra serialization layer for multi-process communication.

6. **LLVM native code generation.** The Rust implementation compiles to
   optimized native code via LLVM. Tight operator loops are auto-vectorized,
   and monomorphized generic code is specialized per type at compile time.

---

*Validated March 2026 against the original Python codebase. The codebase
has since been fully migrated to Rust (April 2026). Core architecture and
algorithms remain the same.*
