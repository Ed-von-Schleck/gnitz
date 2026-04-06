# Roaring Bitmaps and Null Representation in gnitz

Research date: 2026-03-25

Companion to: `research/openzl-shard-compression.md`, `research/lightweight-columnar-encoding.md`

---

## Table of Contents

1. [Context: What This Document Investigates](#1-context)
2. [Current Null Representation](#2-current-null-representation)
3. [Exhaustive Null Access Inventory](#3-exhaustive-null-access-inventory)
4. [The Row-Major vs Column-Major Question](#4-the-row-major-vs-column-major-question)
5. [Roaring Bitmap Fundamentals](#5-roaring-bitmap-fundamentals)
6. [Where Roaring Could Apply](#6-where-roaring-could-apply)
7. [Weight Column and Ghost Rows](#7-weight-column-and-ghost-rows)
8. [What Other Systems Do](#8-what-other-systems-do)
9. [Design for the Rust Architecture](#9-design-for-the-rust-architecture)
10. [Open Questions](#10-open-questions)
11. [References](#11-references)

---

## 1. Context

This document investigates whether roaring bitmaps (or other bitmap representations) can
improve gnitz's storage or execution, with three specific questions:

1. Can roaring bitmaps improve the on-disk null representation in shards?
2. Can roaring bitmaps improve ghost row tracking or cursor iteration?
3. What null representation should the Rust-native architecture use?

**Prior status**: No prior investigation of roaring bitmaps exists in the gnitz codebase. No
commits, plans, or research documents reference roaring. The `roaring` Rust crate (v0.11.3,
31.5M downloads) was not previously evaluated.

---

## 2. Current Null Representation

### 2.1 Layout

gnitz uses a **row-major** null bitmap: one u64 word per row, with one bit per nullable
payload column.

```
null_buf[row_i] = u64 where bit j = 1 iff payload column j is null for row i
```

- Maximum 63 nullable payload columns (PK is never null)
- Bit index mapping: `payload_idx = col_idx if col_idx < pk_index else col_idx - 1`
- Stored as region 3 in the shard file (after pk_lo, pk_hi, weight)
- Size: N rows x 8 bytes = 8 bytes/row regardless of how many columns are nullable

### 2.2 Schema-Level Optimization

If a table has **no nullable columns**, `schema.has_nullable = False` and null checks are
short-circuited:

```python
def is_null(self, col_idx):
    if not schema.has_nullable:
        return False
```

However, even tables with a single nullable column pay the full 8 bytes/row for null_buf.

### 2.3 Storage Cost

For a 100K-row shard:
- null_buf region: 800 KB (always, regardless of nullable column count)
- A table with 10 columns (3 nullable): 59 of 64 bits per word are always zero
- A table with 10 columns (0 nullable): region still allocated, all zeros

---

## 3. Exhaustive Null Access Inventory

Every location in the codebase that reads, writes, or transforms null data,
classified by access pattern.

### 3.1 BULK_MEMCPY (13 sites)

These treat null_buf as raw 8-byte-per-row bytes, copied in bulk alongside pk_lo, pk_hi, and
weight. This is the dominant pattern.

| Operation | File | Hot path? |
|---|---|---|
| `append_batch()` — range copy | batch.py:757 | Very hot (filter, union, consolidation) |
| `append_batch_negated()` | batch.py:816 | Very hot (negate) |
| `_copy_rows_indexed()` x2 variants | batch.py:923, 983 | Hot (consolidation, sorting) |
| `_direct_append_row()` | batch.py:874 | Medium |
| op_filter via `append_batch` | linear.py:72 | Very hot |
| op_union merge via `append_batch` x5 | linear.py:152-183 | Hot |
| op_delay via `append_batch` | linear.py:212 | Hot |
| op_negate via `append_batch_negated` | linear.py:125 | Very hot |
| `to_sorted()` via indexed copy | batch.py:1048 | Medium |
| `to_consolidated()` via indexed copy | batch.py:1107 | Very hot |

In all cases, null_buf is treated identically to pk_lo_buf, pk_hi_buf, and weight_buf —
one `c_memmove` call per structural column.

### 3.2 PER_ROW_CONSTRUCT (7 sites)

These build null words from scratch by testing per-column nullability. This happens when the
output schema differs from the input (joins, reduces, map with computed columns).

| Operation | File | How null word is built |
|---|---|---|
| Join delta-trace (inner) | join.py:205 | `append_from_accessor(CompositeAccessor)` → bit loop |
| Join delta-trace (swapped) | join.py:174 | Same |
| Join delta-trace (outer) | join.py:258, 265 | CompositeAccessor or NullAccessor |
| Join delta-delta | join.py:324 | CompositeAccessor |
| Reduce emission | reduce.py:402 | `append_from_accessor(ReduceAccessor)` → bit loop |
| Reduce finalize | reduce.py:408 | RowBuilder via eval_expr |
| Map per-row path | linear.py:102-117 | RowBuilder.commit_row |

The construction path in `append_from_accessor()` (batch.py:623-628):
```python
null_word = r_uint64(0)
for i in range(num_cols):
    if i == pk_index: continue
    if accessor.is_null(i):
        null_word |= r_uint64(1) << payload_idx
```

### 3.3 PER_ROW_SHUFFLE (2 sites)

These rearrange null bits when projecting (column subset / reorder).

| Operation | File | Pattern |
|---|---|---|
| `UniversalProjection.evaluate_map_batch()` | functions.py:201-213 | Per-row: read input u64, extract bits, shift to output positions |
| `ExprMap.evaluate_map_batch()` | expr.py:831-854 | Per-row: read input u64, shuffle COPY_COL bits, OR with EMIT_NULL and compute masks |

The batch map null shuffle (expr.py:836-841):
```python
for k in range(len(copy_src)):
    in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
    out_null |= ((in_null >> in_pi) & r_uint64(1)) << copy_out[k]
```

This is the one location that could become a bulk per-column operation with column-major
nulls (N memcpys instead of N*rows bit extractions).

### 3.4 Expression VM Null Handling (hot path)

Two paths exist:

**Accessor path** (`eval_expr`, expr.py:142-143):
```python
null[dst] = accessor.is_null(a1)    # calls _read_null_word + bit test
regs[dst] = accessor.get_int_signed(a1)
```

**Direct batch path** (`_eval_row_direct`, expr.py:420-428):
```python
row_null_word = in_batch._read_null_word(row_idx)  # read once per row
# ... then per column load:
pi = a1 if a1 < pk_idx else a1 - 1
nw = row_null_word    # local variable, not re-read
null[dst] = bool(nw & (r_uint64(1) << pi))
```

The direct path reads the null word **once per row** and reuses it across all column loads
in the expression. With column-major nulls, each column load would need a separate bit-vector
read.

### 3.5 Rust Compaction Null Handling

`compact.rs` handles nulls in three places:

**Row comparison** (compact.rs:161-174):
```rust
let null_a = is_null(shard_a, row_a, col_idx, pk_index);
let null_b = is_null(shard_b, row_b, col_idx, pk_index);
// null sorts before non-null
```

**Row writing** (compact.rs:546-561):
```rust
let mut null_word: u64 = 0;
for ci in 0..schema.num_columns {
    if is_null(shard, row, ci, pk_index) {
        null_word |= 1u64 << null_bit_pos;
    }
    self.write_column(ci, payload_idx, is_null, ...);
}
self.null_bitmap.extend_from_slice(&null_word.to_le_bytes());
```

**Null-conditional column write** (compact.rs:577-580):
```rust
if is_null {
    // write zeros, skip blob relocation
}
```

### 3.6 Summary

| Pattern | Count | % of null access sites |
|---|---|---|
| BULK_MEMCPY (null_buf as raw bytes) | 13 | 46% |
| PER_ROW_CONSTRUCT (build null word) | 7 | 25% |
| PER_ROW_COPY (copy whole null word) | 5 | 18% |
| PER_ROW_SHUFFLE (rearrange bits) | 2 | 7% |
| Expression VM (read once, test bits) | 1 | 4% |

---

## 4. The Row-Major vs Column-Major Question

### 4.1 What Row-Major Costs on Disk

For a table with 10 columns (3 nullable), 100K rows:
- **Row-major (current)**: 100K x 8 bytes = 800 KB. 59 of 64 bits per word are always zero.
- **Column-major**: 3 x ceil(100K / 8) = 37.5 KB for nullable columns. 0 bytes for the 7
  non-nullable columns.

That's a **21x size difference** for the null metadata. While 800 KB is small relative to
payload (~8 MB for this example), it's not negligible — it's 10% of shard size spent on
mostly-zero bits.

### 4.2 What Row-Major Costs in Computation

The per-row null bit shuffle in `evaluate_map_batch` (expr.py:831-854) processes N rows x K
columns of bit extraction/insertion. With column-major nulls:

```rust
// Column-major: K bulk operations instead of N*K bit operations
for k in 0..num_copied_columns {
    // Copy entire null bit-vector for column k
    out_null_vecs[out_col[k]].copy_from(&in_null_vecs[src_col[k]], 0..n);
}
for k in 0..num_emit_null_columns {
    out_null_vecs[null_col[k]].fill_ones(0..n);
}
```

K memcpys of ~12.5 KB each vs N iterations of K bit extractions. For 100K rows and 8 copied
columns: 8 memcpys vs 800K bit operations.

### 4.3 What Row-Major Helps

The expression VM's direct path reads the null word **once per row** and reuses it for all
column loads in the expression. With column-major nulls, each `EXPR_LOAD_COL_INT` would need
a separate bit-vector read for that column's null state. For an expression referencing K
columns, that's K reads per row instead of 1.

However, these reads would be into dense bit-vectors with excellent cache locality (sequential
row access within each vector). The practical cost difference depends on whether K bit-vector
reads are cheaper than 1 u64 read + K bit extractions. For small K (1-3 columns, the common
case in filter predicates), the difference is negligible.

### 4.4 What This Means for the Rust Architecture

The row-major layout is not inherently superior — it's convenient for the current per-row
loop structure. The Rust architecture should not inherit this design reflexively. Instead:

**On disk (shard format)**: Column-major null vectors are strictly better for storage. Each
nullable column gets its own bit-vector (ceil(N/8) bytes). Non-nullable columns get nothing.
This is where roaring or other bitmap compression applies.

**In memory (batch representation)**: This is the real design question, tied to how the Rust
DBSP operators will work. Options:

1. **Column-major everywhere** (Arrow-style): Each column has an optional validity bitmap.
   Bulk copy operations become per-column memcpy (including the null bitmap). The expression
   VM accesses one bit-vector per referenced column.

2. **Column-major on disk, row-major in memory**: Serialize/deserialize between formats.
   This preserves the current per-row null word for the expression VM while getting
   column-major storage benefits. But adds conversion cost on every shard read/write.

3. **Column-major on disk, no row-major in memory** (recommended for Rust rewrite): The Rust
   expression VM should be designed for column-at-a-time evaluation (vectorized), not
   row-at-a-time. In a vectorized VM, column-major nulls are natural — you process a column's
   null vector with SIMD operations, not per-row bit extraction.

---

## 5. Roaring Bitmap Fundamentals

### 5.1 Architecture

Roaring bitmaps represent sets of u32 integers. Values are split into 16-bit high key
(bits 31..16) and 16-bit low value (bits 15..0). The high key indexes into an array of
containers, each holding up to 65,536 values.

**Container types:**

| Type | When used | Memory |
|---|---|---|
| Array | Cardinality <= 4,096 per chunk | 2 x cardinality bytes |
| Bitmap | Cardinality > 4,096 per chunk | 8,192 bytes (fixed) |
| Run | Consecutive ranges | 2 + 4 x num_runs bytes |

The 4,096 threshold is the crossover: an array of 4,096 u16 values = 8,192 bytes = one bitmap
container.

### 5.2 Size vs Flat Bit-Vector

For a column with N = 100,000 rows:

| Null density | Null count | Flat bit-vector | Roaring | Winner |
|---|---|---|---|---|
| 0% | 0 | 12,500 B | ~8 B (empty) | Roaring (1562x) |
| 0.1% | 100 | 12,500 B | ~200 B | Roaring (62x) |
| 1% | 1,000 | 12,500 B | ~2,000 B | Roaring (6x) |
| 10% | 10,000 | 12,500 B | ~8,200 B | Roaring (1.5x) |
| 50% | 50,000 | 12,500 B | ~16,400 B | Flat (roaring is 1.3x larger) |

Roaring beats flat bit-vectors when density is below ~12-15%. Above that, the fixed 8 KB
bitmap container overhead makes roaring larger.

### 5.3 Performance

- `contains(row_id)`: O(log C) binary search on container keys + O(1) bitmap test. ~15-50 ns.
- Iteration: ~1 billion values/sec for dense bitmaps (sequential within containers).
- Union/Intersection: Operates on paired containers. Bitmap|Bitmap is bitwise OR of 1024
  u64 words.
- Serialization: Cross-language format (RoaringFormatSpec). `serialized_size()` is O(1).

### 5.4 Rust Crate

`roaring` v0.11.3 (31.5M downloads):
- `RoaringBitmap`: u32 values
- `RoaringTreemap`: u64 values (BTreeMap<u32, RoaringBitmap>)
- Pure Rust, MIT/Apache-2.0 licensed
- Serialization-compatible with C/C++/Java/Go implementations

---

## 6. Where Roaring Could Apply

### 6.1 On-Disk Null Vectors (Column-Major Shard Format)

If the shard format moves to column-major null vectors (one bit-vector per nullable column),
roaring is one encoding option for those vectors.

**When roaring helps**: Sparse nulls (0-10% density). A nullable FK column in an outer join
result might have 5% nulls — roaring compresses this to ~1 KB vs 12.5 KB flat.

**When roaring doesn't help**: Dense nulls (>15%), random patterns. Flat bit-vector is smaller
and simpler. All-null or all-valid columns are better represented as flags (zero bytes).

**Simpler alternatives that cover most cases:**

| Encoding | Best for | Size |
|---|---|---|
| All-valid flag (no bitmap stored) | Non-nullable columns, nullable columns with 0% nulls | 0 bytes |
| Flat bit-vector | Dense or random null patterns | ceil(N/8) bytes |
| RLE on bit-vector | Clustered nulls (e.g., trailing nulls from append-only) | Small for clustered |
| Roaring | Sparse random nulls | ~2K per 1% density at 100K rows |

A practical approach: store a 1-byte encoding tag per column's null metadata in the shard
directory, then dispatch:

```
0x00 = ALL_VALID (no data stored)
0x01 = ALL_NULL (no data stored)
0x02 = FLAT_BITVEC (ceil(N/8) bytes)
0x03 = ROARING (roaring serialized format)
```

The `ALL_VALID` case alone eliminates the null region for most columns in most tables.

### 6.2 Filter Pushdown (Future)

The most compelling database use case for roaring bitmaps is representing **predicate result
sets**: evaluate a predicate on a column, get a roaring bitmap of matching row IDs, intersect
multiple predicate results to get the final scan set.

Systems that use roaring this way: Apache Druid, Apache Doris, ClickHouse (bitmap aggregate
functions).

For gnitz, this would require:
1. Column-level predicate evaluation producing a `RoaringBitmap`
2. Intersection of multiple predicate bitmaps
3. Skip-scan driven by the result bitmap

This is a major architectural change to the expression evaluation path (currently row-at-a-time
in the bytecode VM). Worth investigating separately when/if gnitz adds scan-path optimization.
Not related to the shard compression investigation.

### 6.3 Ghost Row Tracking

`ShardCursor._skip_ghosts()` linearly scans the weight column:

```python
while position < count:
    if get_weight(position) != 0:
        return
    position += 1
```

A precomputed "valid rows" bitmap could avoid these sequential weight reads. However:
- Ghosts are transient — compaction eliminates them. Only unconsolidated shards have ghosts.
- The writer already filters zero-weight rows (`if weight == 0: return` in
  `writer_table.py:218`). Shards produced by flush never contain ghosts.
- Ghosts only appear in shards during compaction when cross-shard cancellations haven't been
  resolved yet.
- A simple dense bit-vector (ceil(N/8) bytes) suffices — roaring adds complexity for no
  benefit at typical shard sizes.

**Verdict**: Minor optimization. Not worth a roaring dependency. If profiling shows ghost
skipping as a bottleneck, a flat bit-vector built at shard open time is sufficient.

---

## 7. Weight Column and Ghost Rows

### 7.1 Weight Is Not Replaceable

The weight column (i64 per row) serves Z-set algebraic semantics throughout gnitz. It cannot
be replaced by a validity bitmap because operators perform arithmetic on weights:

| Operator | Weight operation | Requires actual value? |
|---|---|---|
| Join (all variants) | `w_out = w_left * w_right` | Yes — multiplication |
| Distinct | `sign(w_old + w_delta) - sign(w_old)` | Yes — addition + sign |
| Reduce | `agg.step(accessor, weight)` — scales contribution | Yes — multiplication |
| Consolidation | `weight_acc += weight` | Yes — summation |
| Negate | `weight = -weight` | Yes — negation |
| Union | Weights preserved as-is | Copy only |
| Filter | Weights preserved as-is | Copy only |
| Anti/Semi-join | Check `weight > 0` for match | Sign test only |

**12 of 20 weight access sites require the actual i64 value.** Only 8 sites are pure
ghost-detection (zero check).

### 7.2 Weight Distribution

Typical weight values in gnitz:

| Source | Distribution |
|---|---|
| Base table insert | Always +1 |
| Base table delete | Always -1 |
| Join output | Product of inputs (usually +1 or -1) |
| Distinct output | -1, 0, or +1 |
| After consolidation | Non-zero only (ghosts eliminated) |

Most weights are +1 or -1. This makes the weight column an excellent compression candidate
(see `lightweight-columnar-encoding.md` Section 4.8 — Pcodec achieves 32-64x on two-value
distributions).

---

## 8. What Other Systems Do

### 8.1 Null Representation

| System | Null representation | Roaring? |
|---|---|---|
| Apache Arrow | Column-major: one flat bit-vector per column, 1 bit/row. All-valid = null pointer. | No |
| DuckDB | Column-major: `ValidityMask` per column (u64 array). All-valid = null pointer. | No |
| ClickHouse | Column-major: separate null mask file per Nullable column (1 byte/row in MergeTree). | No (uses roaring only for user-facing bitmap aggregates) |
| Apache Parquet | Definition levels (RLE-encoded integers). No bitmap. | No |
| gnitz (current) | Row-major: one u64 per row, bits = columns. | No |

**No production columnar database uses roaring for null/validity tracking.** The common
pattern is column-major flat bit-vectors with an all-valid optimization (null pointer or flag).

### 8.2 Why Column-Major Nulls Win for Storage

BtrBlocks (SIGMOD 2023) stores null bitmaps per-column as flat bit vectors and applies
lightweight encoding (constant for all-valid, RLE for clustered nulls, uncompressed for random
nulls). Their finding: per-column null bitmaps compress well because null patterns are
typically highly structured (all-valid, all-null, or clustered).

Arrow's approach: `null_count == 0` → validity buffer pointer is NULL → zero bytes. This
handles the common case (non-nullable columns, or nullable columns where all rows happen to be
non-null) at zero cost.

---

## 9. Design for the Rust Architecture

### 9.1 Recommended On-Disk Format

Move to **column-major null vectors** in the shard format. Each nullable column gets an
optional null metadata section in the shard directory.

**Shard directory entry** (per column, extending the v4 format from
`openzl-shard-compression.md` Section 8):

```
[data_offset: u64] [data_size: u64] [data_checksum: u64]
[compression: u8] [null_encoding: u8] [reserved: 6 bytes]
```

Where `null_encoding`:
```
0x00 = ALL_VALID   — no null data stored (column has zero nulls)
0x01 = ALL_NULL    — no null data stored (every row is null)
0x02 = FLAT_BITVEC — ceil(N/8) bytes, inline after column data
0x03 = ROARING     — roaring serialized format, inline after column data
0x04 = RLE_BITVEC  — RLE-encoded bit vector
```

**The ALL_VALID case is the critical optimization.** It eliminates null metadata for:
- All non-nullable columns (by schema)
- Nullable columns where this particular shard has no nulls (common after filtering)

**Storage savings** (100K-row shard, 10 columns, 3 nullable, 1% null density):

| Format | Null metadata size |
|---|---|
| Current (row-major u64/row) | 800 KB |
| Column-major flat bit-vector (3 columns) | 37.5 KB |
| Column-major with ALL_VALID for 2 of 3 + flat for 1 | 12.5 KB |
| Column-major with ALL_VALID for 2 of 3 + roaring for 1 | ~2 KB |

### 9.2 Recommended In-Memory Format

For the Rust-native batch representation, use **column-major validity bitmaps** (Arrow-style):

```rust
struct ColumnArray {
    data: Buffer,           // raw column bytes
    validity: Option<Bitmap>,  // None = all valid
    len: usize,
}

struct Bitmap {
    bits: Vec<u64>,   // ceil(len/64) words
    null_count: usize,
}
```

The `Option<Bitmap>` encodes the all-valid case at zero cost (no allocation, no branch in
the non-nullable path).

### 9.3 How Operators Would Use Column-Major Nulls

**Bulk copy (filter, union, delay, negate)**: Copy each column's validity bitmap alongside
its data buffer. One memcpy per column for data + one memcpy per column for validity. Same
total memcpy count as today.

**Projection (column subset/reorder)**: Copy the validity bitmaps for selected columns. No
per-row bit shuffling — each column's bitmap is an independent unit. This is faster than the
current per-row bit extraction loop.

**Join (composite rows)**: Build output validity per column from left/right input columns.
Each output column's validity is either the left column's validity, the right column's
validity, or all-null (for outer join fill). No per-row bit-word construction needed.

**Expression VM (predicate evaluation)**: For vectorized evaluation, process one column at a
time. The column's validity bitmap is available as a dense bit-vector for SIMD
null-propagation:

```rust
// Vectorized null propagation for binary op (e.g., a + b):
// result_valid = a_valid AND b_valid
let result_validity: Vec<u64> = a.validity.bits.iter()
    .zip(b.validity.bits.iter())
    .map(|(&va, &vb)| va & vb)
    .collect();
```

This is vastly more efficient than the current per-row null register tracking.

**For row-at-a-time evaluation** (if needed as a fallback): Checking one column's null status
for a specific row is O(1) in a flat bit-vector: `(bits[row / 64] >> (row % 64)) & 1`. This
is comparable to the current `(_read_null_word(row) >> payload_idx) & 1` — one memory access
+ one bit test.

### 9.4 Conversion During Shard Read

When reading a shard with column-major null vectors into column-major in-memory bitmaps,
the conversion is trivial:

- `ALL_VALID`: Set `validity = None`
- `FLAT_BITVEC`: Directly use the bit-vector bytes (zero-copy if mmap'd, or memcpy)
- `ROARING`: Deserialize to flat bit-vector for in-memory use
- `RLE_BITVEC`: Decode to flat bit-vector

The in-memory representation is always a flat bit-vector (or None). Roaring and RLE are
on-disk encodings only — they reduce I/O but are decoded at read time.

---

## 10. Open Questions

### 10.1 Is Roaring Worth the Dependency?

The `ALL_VALID` flag + flat bit-vector covers the vast majority of real-world cases:
- Non-nullable columns: ALL_VALID (zero bytes). This is most columns.
- Nullable columns with data: FLAT_BITVEC (12.5 KB per 100K rows). Tiny.

Roaring adds value only for nullable columns with **sparse random null patterns** (0.1-10%
density), where it saves 5-60x vs flat bit-vector. The question is whether this saving
(a few KB per column per shard) justifies the complexity.

**Arguments for roaring**: The `roaring` crate is mature (31.5M downloads), well-maintained,
and the serialization format is cross-language standard. The integration cost is low
(`cargo add roaring`). Even if the savings are small per-shard, they compound across many
shards and many nullable columns.

**Arguments against**: Flat bit-vectors are trivial to implement (no dependency), fast to
decode (memcpy), and the size difference is small in absolute terms. RLE on bit-vectors
handles clustered null patterns (common for append-only workloads) without roaring's
container overhead.

**Recommendation**: Include roaring as a supported encoding in the shard format (it's one
enum variant and a few lines of serialize/deserialize code), but default to `ALL_VALID` or
`FLAT_BITVEC`. Use roaring when the flush-time null density check indicates it would save
space. The cost of supporting it is near-zero; the benefit is occasional.

### 10.2 Weight Column Compression vs Validity Bitmap

The weight column (i64 per row, mostly +1/-1) compresses extremely well with Pcodec (32-64x)
or FastLanes (32x with zigzag + 2-bit bitpack). A separate validity bitmap for "non-ghost
rows" would be redundant — the compressed weight column already encodes this information
more compactly than an additional bitmap would.

### 10.3 Prior Roaring Investigation

This document found no evidence of a prior roaring bitmap investigation in the gnitz codebase.
The claim in an earlier draft of `lightweight-columnar-encoding.md` that roaring was
"investigated previously and ruled out" was incorrect and has been corrected.

---

## 11. References

### Crates
- roaring (Rust): https://github.com/RoaringBitmap/roaring-rs — v0.11.3
- Apache Arrow validity bitmaps: https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps

### Papers & Docs
- Roaring Bitmaps: https://roaringbitmap.org/
- BtrBlocks (SIGMOD 2023) — per-column null encoding: https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf
- DuckDB ValidityMask: https://duckdb.org/docs/internals/storage

### gnitz Internal
- Current null representation: `gnitz/core/batch.py` (null_buf, _read_null_word, is_null)
- Shard null region: `gnitz/storage/shard_table.py`, `gnitz/storage/writer_table.py`
- Rust compaction null handling: `crates/gnitz-engine/src/compact.rs`
- Expression VM null tracking: `gnitz/dbsp/expr.py` (_eval_row_direct, evaluate_map_batch)
- Null formal proofs: `proofs/checks/null_word.py`
- Weight column: `gnitz/storage/cursor.py` (_skip_ghosts), `gnitz/dbsp/ops/distinct.py`,
  `gnitz/dbsp/ops/join.py`
