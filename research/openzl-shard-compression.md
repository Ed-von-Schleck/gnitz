# OpenZL Shard Compression: Feasibility Study

Research date: 2026-03-25

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [OpenZL Overview](#openzl-overview)
3. [gnitz Storage Architecture (Current State)](#gnitz-storage-architecture)
4. [The Cursor Problem](#the-cursor-problem)
5. [PK Distribution Analysis](#pk-distribution-analysis)
6. [Integration Path](#integration-path)
7. [Phased Implementation Plan](#phased-implementation-plan)
8. [Shard Format v4 Specification](#shard-format-v4-specification)
9. [Open Questions and Risks](#open-questions-and-risks)
10. [References](#references)

---

## 1. Executive Summary

OpenZL is a format-aware compression framework from Meta that builds specialized compressors
from composable codec DAGs (delta, bitpack, transpose, entropy, etc.). Unlike generic
compressors (zstd, lz4), it takes typed descriptions of data structure and generates optimized
compression pipelines, achieving 2x+ ratios at zstd-level speeds on structured columnar data.

gnitz currently stores shard data **uncompressed** in columnar (SoA) layout. Each shard region
is a contiguous typed array — exactly the input OpenZL is designed for.

**Key findings:**

- OpenZL has **no random access within a compressed frame**. Decompression is whole-frame only.
  The delta codec (prefix sum) and chained codec DAGs make intra-frame seeking impossible.
- The **multi-frame block compression** approach (one independent frame per block of rows, with a
  block index) is the only viable path for compressing PK columns while preserving binary search.
- **Payload columns** (user data) are already lazily accessed and fit perfectly into a
  "decompress on first access" pattern — zero impact on seek performance.
- **PK compressibility varies dramatically by distribution**: monotonic u64 compresses 5-8x,
  semi-random u64 only 1.2-1.4x (not worth the seek overhead), UUIDv7 splits into a
  compressible half (pk_hi, 2-5x) and an incompressible half (pk_lo, ~1.1x).
- The existing **hi/lo PK split on disk** is a compression asset — it naturally separates
  structured bits (timestamp) from random bits (entropy) for UUIDv7. Keep it even when the
  in-memory representation moves to native u128.
- The recommended approach is **phased**: prove the architecture with zstd on payload columns
  first (low risk, big savings), then upgrade to OpenZL for better ratios, then optionally
  add block-compressed PKs for distributions that warrant it.

---

## 2. OpenZL Overview

### 2.1 What It Is

OpenZL (formerly "ZStrong" internally at Meta) is a format-aware data compression framework
open-sourced in October 2025. Repository: https://github.com/facebook/openzl. Website:
https://openzl.org/.

Core idea: instead of treating data as an opaque byte stream, you describe the data's structure
and OpenZL builds a specialized compressor from a DAG of modular codecs. All compressed output
can be decompressed by a single universal decompressor — no need to track which compressor was
used per-file.

Written in C (9.3 MB) and C++ (7.2 MB) with Python bindings. BSD licensed. Notable
contributors include Yann Collet (creator of zstd and lz4).

### 2.2 Compression Pipeline

The workflow is:

1. **Describe** data format via SDDL (Simple Data Description Language), pre-built profiles,
   or custom parsers
2. **Train** (optional, offline) on sample data — the trainer explores codec combinations and
   produces Pareto-optimal compression plans
3. **Compress** using the plan — the plan is embedded in the frame header
4. **Decompress** with the universal decoder — reads the plan from the frame and executes it

### 2.3 Available Codecs

| Codec | Function |
|---|---|
| **Delta** | Stores first value + N-1 unsigned deltas. Supports 1/2/4/8-byte elements. |
| **Bitpack** | Packs elements at a global bit-width (single `nbBits` for entire block). AVX2+BMI2 fast path. |
| **Transpose** | Byte-lane shuffle (blosc-style). Separates byte[0], byte[1], ... across elements. |
| **SplitTranspose** | Same but produces W separate output streams (one per byte lane) — each lane gets its own downstream codec. |
| **Constant** | Encodes arrays where all elements are identical: one value + varint count. |
| **Entropy (FSE)** | tANS (Finite State Entropy) — same entropy coder as zstd. Sub-bit-per-symbol coding. |
| **Entropy (Huffman)** | Faster than FSE, slightly worse ratio. |
| **FieldLZ** | LZ-style matching on structured fields. Good for repetitive delta patterns. |
| **Store** | Passthrough — writes data as-is. Used when compression provides no benefit. |
| **Zigzag** | Maps signed integers to unsigned (small absolute values → small unsigned values). |
| **Zstd** | Falls back to generic zstd compression. |
| **LZ4** | Falls back to generic LZ4 compression. |
| **BitSplit** | Bit-level splitting (sign/exponent/mantissa for floats, high/low bits for ints). |
| **Interleave** | Round-robin interleaves multiple same-type inputs (string-only currently). |
| **Clustering** | Groups parsed streams into compression-optimal buckets. |
| **MLSelector** | Statistical classifier that selects codec graph at runtime based on data features. |
| **ACE** | Automated Compression Explorer — genetic algorithm for codec composition search. |

### 2.4 Performance Characteristics

From Meta's published benchmarks on the SAO dataset (star catalog, structured columnar):

| Metric | zstd -3 | xz -9 | OpenZL |
|---|---|---|---|
| Compression Ratio | 1.31x | 1.64x | **2.06x** |
| Compression Speed | 115-220 MB/s | 3.1-3.5 MB/s | **203-340 MB/s** |
| Decompression Speed | 850-890 MB/s | 30-45 MB/s | **822-1200 MB/s** |

On unstructured text (enwik, dickens), OpenZL falls back to zstd and offers no advantage.
On trained numeric arrays, ratios of 3-4x+ are achievable.

### 2.5 API Surface

**C API** (primary):
```c
// Compression
ZL_CCtx* cctx = ZL_CCtx_create();
ZL_TypedRef* input = ZL_TypedRef_createNumeric(data_ptr, element_width, element_count);
size_t compressed = ZL_CCtx_compressTypedRef(cctx, dst, dstCap, input);

// Decompression (universal, no schema needed)
size_t decompressed = ZL_decompress(dst, dstCap, src, srcSize);

// Typed decompression with auto-allocation
ZL_TypedBuffer* tbuf = NULL;
ZL_DCtx_decompressTBuffer(dctx, &tbuf, src, srcSize);

// Frame queries (no decompression)
size_t orig_size = ZL_getDecompressedSize(src, srcSize);
```

**Key limitation**: `ZL_TypedRef_createNumeric` supports element widths 1, 2, 4, or 8 bytes only.
No native u128 (16-byte) support. UUIDs must be treated as structs or byte arrays.

**SDDL** operates at byte granularity only — no sub-byte field descriptions. Cannot express
UUIDv7's 48-bit timestamp / 4-bit version / 12-bit random structure directly.

### 2.6 Training Pipeline

OpenZL's offline trainer uses ACE (Automated Compression Explorer), a genetic algorithm that
searches the combinatorial space of codec compositions:

1. Provide sample data + seed compressor (SDDL or custom parser)
2. Trainer mutates the codec DAG, locally experiments with parameters
3. Produces Pareto-optimal set of (speed, ratio) tradeoff plans
4. Serialize to `.zlc` file for use at compression time
5. Decompression needs no profile — the codec recipe is embedded in each frame

CLI:
```bash
zli train --profile le-u64 data_samples/ --output trained.zlc
zli train --profile le-u64 data_samples/ --output compressors/ --pareto-frontier
```

The clustering trainer can discover optimal groupings (which columns benefit from joint vs.
independent compression). The ML selector can learn to route data to different codec graphs
based on statistical features.

### 2.7 Project Maturity

- Version: v0.1.0 (October 2025). API explicitly subject to change.
- Backward-compatible decompression guaranteed for released versions.
- Actively maintained — multiple commits per day, multiple Meta engineers.
- Used in production at Meta (Nimble columnar database integration).

---

## 3. gnitz Storage Architecture

### 3.1 On-Disk Shard Format (v3)

Each shard is a `.db` file: `shard_<table_id>_<lsn>.db`

```
[64-byte Header]
[Directory: N × 24-byte entries]
[64-byte-aligned data regions]
[Optional: XOR8 filter footer]
```

**Header** (layout.py):
```
Offset  Size  Field
0       8     Magic: 0x31305F5A54494E47 ("GNITZ_01")
8       8     Format version: 3
16      8     Row count (u64)
24      8     Directory offset (u64)
32      8     Table ID (u64)
40      8     XOR8 filter offset (0 if absent)
48      8     XOR8 filter size
56      8     Reserved
```

**Directory entry** (24 bytes each):
```
Offset  Size  Field
0       8     Region offset (bytes from file start)
8       8     Region size (bytes)
16      8     XXH128 checksum
```

**Region order**:
1. `pk_lo` — N × 8 bytes (lower 64 bits of PK)
2. `pk_hi` — N × 8 bytes (upper 64 bits of PK)
3. `weight` — N × 8 bytes (i64, algebraic Z-set weight)
4. `null_buf` — N × 8 bytes (u64, one bit per payload column)
5. `col_bufs[0..num_non_pk-1]` — N × stride bytes each
6. `blob_heap` — variable-length string data

All regions are 64-byte aligned. Data is stored in native little-endian byte order with no
compression or encoding beyond the raw columnar layout.

### 3.2 PK Representation

The on-disk format stores PKs in two regions of little-endian u64 bytes:

- **Region 0 (pk_lo)**: N × 8 bytes — lower 64 bits of the PK
- **Region 1 (pk_hi)**: N × 8 bytes — upper 64 bits of the PK

For `TYPE_U64` tables, pk_hi is all zeros. For `TYPE_U128` tables (including UUIDv7), pk_hi
carries the upper half.

**The on-disk format is just bytes.** The in-memory representation is irrelevant to what gets
written. Whether the writer holds a native `u128` and splits it at serialization time, or
holds two `u64` fields, the bytes on disk are identical:

```rust
// These produce the same bytes on disk:
pk_lo_region.write(&(key as u64).to_le_bytes());         // from native u128
pk_lo_region.write(&key_lo.to_le_bytes());               // from separate u64
```

The RPython side currently uses separate u64 fields (due to `r_uint128` alignment issues in
resizable lists — documented in appendix.md). The Rust compaction side already reads the bytes
back as native u128:

```rust
fn get_pk(&self, row: usize) -> u128 {
    let lo = read_u64_le(d, self.pk_lo_off + row * 8);
    let hi = read_u64_le(d, self.pk_hi_off + row * 8);
    ((hi as u128) << 64) | (lo as u128)
}
```

Moving to native u128 in-memory in Rust changes nothing about the on-disk format or the
compression story.

### 3.3 Cursor Architecture

**Cursor hierarchy:**

| Cursor | Source | Binary search | Notes |
|---|---|---|---|
| `SortedBatchCursor` | In-memory `ArenaZSetBatch` | `pk_lt()` on batch buffers | Used for freshly ingested data |
| `MemTableCursor` | Consolidated MemTable snapshot | `pk_lt()` on snapshot | Merges sorted runs before cursor creation |
| `ShardCursor` | mmap'd `.db` file (`TableShardView`) | `find_lower_bound()` via `pk_lt()` | Delegates to view's binary search |
| `UnifiedCursor` | N sources (MemTable + shards) | Seeks all sub-cursors, tournament tree merge | Ghost-skipping, weight consolidation |

**The `pk_lt` comparison function** (batch.py:16-22):
```python
def pk_lt(a_lo, a_hi, b_lo, b_hi):
    if a_hi < b_hi:
        return True
    if a_hi > b_hi:
        return False
    return a_lo < b_lo
```

Always compares both hi and lo, even for u64 PKs where hi is known to be 0. No short-circuit
optimization exists for u64-only tables.

### 3.4 Binary Search on Shards

`TableShardView.find_lower_bound()` (shard_table.py:313-329):

```python
def find_lower_bound(self, key_lo, key_hi):
    low = 0
    high = self.count - 1
    res = self.count
    while low <= high:
        mid = (low + high) // 2
        if pk_lt(self.get_pk_lo(mid), self.get_pk_hi(mid), key_lo, key_hi):
            low = mid + 1
        else:
            res = mid
            high = mid - 1
    return res
```

Each `get_pk_lo(mid)` / `get_pk_hi(mid)` reads a single u64 at offset `mid * 8` from the
mmap'd region buffer. This is **random access** — O(log n) probes into the PK arrays,
potentially touching different memory pages on each probe.

### 3.5 Memory Access Pattern Summary

| Operation | Access type | What's touched | Granularity |
|---|---|---|---|
| Binary search (seek) | **Random** | pk_lo, pk_hi only | 8 bytes per probe |
| Ghost skipping | Sequential forward | weight only | 8 bytes per step |
| Payload column access | Lazy, sequential after seek | col_bufs[i] | stride bytes per row |
| Dedup comparison (compaction) | Sequential per-column | All columns, early exit on diff | stride bytes per column |
| Row copy (compaction merge) | Sequential per-column | All non-null columns + blob heap | Full row |

**Critical insight**: Binary search touches only PK arrays. Payload columns are never accessed
during seeking. Ghost skipping is sequential. Compaction is fully sequential.

### 3.6 Lazy Validation Pattern

`TableShardView._init_region()` eagerly validates checksums for regions 0-2 (pk_lo, pk_hi,
weight) at shard open time. All other regions are lazily validated on first access via
`_check_region()`:

```python
def _check_region(self, buf, idx):
    if self.validate_on_access and not self.region_validated[idx]:
        if buf.size > 0:
            actual = checksum.compute_checksum(buf.ptr, buf.size)
            if actual != self.dir_checksums[idx]:
                raise errors.CorruptShardError(...)
        self.region_validated[idx] = True
```

Ghost rows (weight=0) never trigger payload validation. This lazy pattern is the natural
insertion point for decompression — decompress when first accessed, replacing the mmap'd
pointer with a heap-allocated decompressed buffer.

### 3.7 Existing Compression and Encoding

**None.** The on-disk format stores raw columnar data with no compression, no dictionary
encoding, no run-length encoding. The only existing space optimizations are:

- String deduplication in the blob heap (checksum-based)
- XOR8 probabilistic filter for PK membership (embedded in shard footer)
- Bloom filter in MemTable (in-memory only)

### 3.8 Rust Compaction

`compact.rs` implements N-way merge compaction:

1. Open all input shards (mmap'd)
2. Create `ShardCursor` per shard (position 0, skip ghosts)
3. Build `TournamentTree` — min-heap keyed by u128 PK
4. Merge loop: extract minimum key group, sum weights, write non-zero rows to output
5. Finalize output shard

The merge loop accesses shards **sequentially** — cursors only advance forward. Row comparison
for deduplication iterates columns sequentially with early exit on first difference.
No random access into individual rows during merge.

---

## 4. The Cursor Problem

### 4.1 The Fundamental Constraint

OpenZL decompression is **whole-frame only**. From the decompression API
(`include/openzl/zl_decompress.h`):

- `ZL_decompress()` — decompress entire frame
- `ZL_DCtx_decompress()` — same, with context
- `ZL_DCtx_decompressTBuffer()` — decompress entire frame into typed buffer
- `ZL_DCtx_decompressMultiTBuffer()` — decompress entire frame into multiple typed buffers

There is no `ZL_decompressRange()`, no seek-to-offset, no partial decompression within a frame.

**Why this is fundamental, not a missing feature:**

The codec DAG chains transforms that destroy random-access capability:

1. **Delta codec**: `dst[n] = dst[n-1] + delta[n-1]` — a prefix sum. Must decode from element 0.
   Has SIMD (SSSE3) vectorized paths for 8/16/32-bit widths, but still sequential.

2. **Chained codecs**: A typical pipeline for sorted integers is `delta → zigzag → bitpack →
   entropy`. Even if bitpack is theoretically addressable (element N at bit `N * nbBits`), the
   delta before it makes the full pipeline sequential.

3. **Internal chunking**: OpenZL supports multiple chunks per frame (v21+, hardcoded 16 MiB
   chunk size), but chunks are parsed sequentially — no chunk index, no skip. The decompression
   loop in `decompress2.c` walks chunks in order via `ZL_DCtx_decompressChunk()`.

### 4.2 No Seekable Format

Unlike zstd (which has a contributed seekable format in `contrib/seekable_format/`), OpenZL has
no equivalent. There is no seek table, no block index within frames, no `ZL_seekable_*` API.

### 4.3 The Solution: Multi-Frame Block Compression

OpenZL frames are **self-contained and independently decompressible**. The frame header embeds
the full codec recipe (decompression map). This means:

**Compress each block of B rows as a separate OpenZL frame. Store a block index for seeking.**

Per-frame overhead analysis from the wire format (`wire_format.h`):

| Component | Size |
|---|---|
| Magic number | 4 bytes |
| Frame properties (v21+) | 1 byte |
| Output count + type tags | 1-2 bytes |
| Decompressed sizes (VarInt per output) | 1-9 bytes |
| Header checksum | 1 byte |
| Chunk header (codec DAG recipe) | ~15-30 bytes (for 2-3 codec pipeline) |
| Stored stream sizes | 1-5 bytes each |
| Codec private headers (e.g., delta first value, bitpack width) | variable |
| Content checksum (optional) | 4 bytes |
| Compressed checksum (optional) | 4 bytes |
| End-of-frame marker | 1 byte |
| **Total for simple pipeline** | **~30-55 bytes** |

Overhead as a function of block size:

| Block size (rows) | Raw bytes (u64 col) | Frame overhead | Overhead % |
|---|---|---|---|
| 256 rows | 2 KB | ~50 B | 2.4% |
| 512 rows | 4 KB | ~50 B | 1.2% |
| 1024 rows | 8 KB | ~50 B | 0.6% |
| 4096 rows | 32 KB | ~50 B | 0.15% |

For a complex pipeline (10-node DAG, multi-column format), the decoding map scales to
~80-150 bytes per frame — still under 4% at 4 KB block size.

### 4.4 Two-Level Binary Search

With block compression, binary search becomes:

1. **Search block index**: O(log(n/B)) comparisons on (min_pk, offset, size) tuples
2. **Decompress target block**: One OpenZL frame at ~1 GB/s decompression
3. **Search within block**: O(log B) comparisons on decompressed data

For 100K rows with B=1024: 98 blocks, block index = 2.3 KB (fits in L1 cache), ~7 comparisons
on the index, one ~8 KB frame decompression (~8 μs), ~10 comparisons within the block.

**Cold reads**: Potentially *faster* than uncompressed — fewer bytes to read from disk, fewer
page faults during binary search. Decompression at 1 GB/s is faster than SSD random reads.

**Hot reads**: ~8-16 μs overhead per seek from one block decompression. Acceptable for most
workloads; the XOR8 filter already eliminates most shard accesses before binary search begins.

### 4.5 What Doesn't Need Block Compression

**Payload columns**: Accessed lazily, always sequentially after seeking. Compress as a single
frame per region. Decompress the entire region on first access. This is the simplest and
highest-value change.

**Weight column**: Accessed sequentially during ghost skipping. Could be compressed as a single
frame (small region). Or left raw — weight is 8 bytes/row and often takes only a few distinct
values (+1, -1), so entropy coding alone gives good ratios.

**Null bitmap**: Small (8 bytes/row), highly compressible (mostly zeros for non-null-heavy
tables). Single-frame compression, decompress on first access.

---

## 5. PK Distribution Analysis

### 5.1 Supported PK Types

gnitz supports exactly two PK types (enforced in `types.py:110-113`):
- `TYPE_U64` (code 8): 8-byte unsigned integer
- `TYPE_U128` (code 12): 16-byte unsigned integer

Both are stored on disk as hi/lo u64 pairs in separate regions.

### 5.2 Case 1: u64, Monotonically Increasing with Gaps

Example: auto-increment IDs, timestamps, sequence numbers.

```
pk_lo: [100, 103, 107, 108, 115, 122, 500, 501, 502, ...]
pk_hi: [0, 0, 0, 0, 0, ...]
```

**pk_hi**: All zeros. OpenZL's Constant codec encodes the entire region as (value=0, count=N) —
literally bytes. Even without OpenZL, this region could be detected as constant and omitted.

**pk_lo with delta encoding**:
- Deltas: `[3, 4, 1, 7, 7, 378, 1, 1, ...]`
- Mostly small positive integers with occasional large gaps

**Codec pipeline analysis**:

| Pipeline | Behavior | Expected ratio |
|---|---|---|
| delta → constant | Only works if stride is perfectly uniform | >100x if applicable |
| delta → bitpack | Global nbBits = max delta width. If max gap < 2^10, 10 bits/element = 1.25 bytes/row → **6.4x**. One large gap inflates all elements. |
| delta → FSE (entropy) | Adapts per-symbol. Handles bimodal distribution (many small deltas + few large ones) well. | **3-5x** |
| delta → fieldLZ | Good for repetitive delta patterns (e.g., many identical gaps). | **3-8x** depending on pattern |
| delta → zstd | Generic fallback. Still benefits from small deltas. | **2-3x** |

**Bitpack's global-width problem**: If 99.9% of deltas are <256 (8 bits) but one delta is
50,000 (16 bits), the entire block packs at 16 bits. Training would likely prefer
`delta → FSE` or `delta → fieldLZ` over `delta → bitpack` for variable-gap distributions.

**Block compression (B=1024)**:
- Raw block: 8 KB
- Compressed block (delta → FSE): ~1.6-2.7 KB
- Decompression time: ~2-3 μs
- Block index: 98 entries × 24 bytes = 2.3 KB for 100K rows

**Verdict**: Excellent compression. Block-compress both pk_lo and pk_hi. Net PK compression
**5-8x** (factoring in the trivially-compressed pk_hi). Binary search overhead negligible.

### 5.3 Case 2: u64, Semi-Random (e.g., Hash-Based IDs)

Example: hash(input), random assignment, imported foreign keys.

```
pk_lo: [0x0A3F..., 0x1B72..., 0x2E01..., ...]  (sorted within shard)
pk_hi: [0, 0, 0, 0, ...]
```

The shard is sorted at flush time, so pk_lo is in ascending order. But the values are spread
across the full u64 range.

**pk_hi**: All zeros → trivial compression (same as Case 1).

**pk_lo delta analysis for sorted random u64s (N = 100K rows)**:
- Expected gap between consecutive sorted values: `2^64 / N ≈ 1.8 × 10^14`
- Gap variance is high (exponential distribution)
- Deltas require ~47 bits each (floor(log2(2^64 / N)))

| Pipeline | Behavior | Expected ratio |
|---|---|---|
| delta → bitpack(47) | 47 bits/element = 5.9 bytes/row | **1.36x** |
| delta → FSE | Deltas are high-entropy (near-uniform large values). FSE provides marginal gain. | **1.3-1.5x** |
| SplitTranspose → per-lane entropy | High bytes of sorted random u64s have slightly lower entropy. Minor effect. | **1.1-1.2x** |
| Store (passthrough) | No compression. | **1.0x** |

**Is 1.2-1.4x compression worth the seek overhead?**

For a 1024-row block (8 KB raw → ~6 KB compressed):
- Disk savings: ~2 KB per block
- Decompression cost per seek: ~6 μs
- Cold reads: reduced I/O roughly breaks even with decompression cost
- Hot reads: strictly slower (decompressing data already in page cache for no meaningful
  space saving)

**Verdict**: Do **not** compress pk_lo. Leave it raw — binary search works unchanged with zero
overhead. Only compress pk_hi (all zeros → trivial). The space savings from pk_hi alone halve
the PK storage for u64 tables (8 bytes/row saved). Net PK compression: **~2x** (pk_hi only).

### 5.4 Case 3: u128, UUIDv7

UUIDv7 structure (RFC 9562):
```
Bits 127-80 (bytes 0-5):   48-bit unix timestamp in milliseconds (monotonic)
Bits 79-76  (byte 6 high): 4-bit version = 0b0111 (constant)
Bits 75-64  (byte 6 low + byte 7): 12-bit rand_a (random)
Bit  63     (byte 8 high 2 bits):  2-bit variant = 0b10 (constant)
Bits 61-0   (bytes 8-15 remaining): 62-bit rand_b (random)
```

In the hi/lo split:
```
pk_hi (upper 64 bits) = [timestamp: 48b] [version: 4b] [rand_a: 12b]
pk_lo (lower 64 bits) = [variant: 2b] [rand_b: 62b]
```

#### 5.4.1 pk_lo (Lower 64 Bits) — Mostly Random

- Variant bits (high 2 bits) are constant (`10`) → byte 0 is always `0x80`-`0xBF`
- Remaining 62 bits are random
- After sorting by full UUID: pk_lo is random between different pk_hi values; within the same
  pk_hi value, pk_lo is sorted (but the groups are small — typically a few rows per ms)

SplitTranspose analysis:
- Byte 0: reduced entropy (~6 bits instead of 8, range 0x80-0xBF) → slight gain
- Bytes 1-7: full entropy → incompressible

**Net pk_lo compression: ~1.05-1.1x. Not worth the overhead.**

#### 5.4.2 pk_hi (Upper 64 Bits) — Structured

The delta behavior depends on data arrival rate relative to shard size.

**High-throughput ingestion (many rows per millisecond)**:

Example: 100K-row shard spanning 1 second (1000 distinct ms values, ~100 rows per ms).

Within a ms-group:
- Timestamp bits (high 48) are identical
- Deltas come only from rand_a (12 bits of randomness)
- Delta magnitude: ~12 bits

At ms boundaries:
- Timestamp increments by 1 ms = delta of `1 << 16` in the u64 representation (because
  rand_a occupies the low 16 bits including version)
- Plus rand_a jump → total delta ~17-20 bits

| Pipeline | Behavior | Expected ratio |
|---|---|---|
| delta → bitpack(20) | Global width 20 bits covers both intra-ms and boundary deltas. 2.5 bytes/row. | **3.2x** |
| delta → FSE | Bimodal distribution (small intra-ms + larger boundary). FSE handles this well. | **3-4x** |
| SplitTranspose → per-lane | Bytes 0-4 (timestamp MSBs): near-zero deltas → 10-20x. Byte 5 (timestamp LSB): small deltas → 4-8x. Byte 6 (version+rand_a high): version constant, rand_a varies → 2-3x. Byte 7 (rand_a low): random → ~1.2x. | **3-5x net** |

**Low-throughput ingestion (few rows per millisecond)**:
- Timestamps spread across more distinct ms values
- Most deltas involve a timestamp change
- Still structured — timestamp component gives monotonic structure
- **~2-3x** expected

**Block compression for pk_hi (B=1024)**:
- Raw block: 8 KB
- Compressed (SplitTranspose → per-lane): ~1.6-4 KB depending on arrival rate
- Decompression time: ~2-4 μs

#### 5.4.3 Binary Search with Mixed Compressed/Raw PK

With pk_hi block-compressed and pk_lo raw:

1. Read block index for pk_hi (24 bytes per block, min_pk_hi + offset + size)
2. Binary search block index by pk_hi — O(log(n/B)) comparisons
3. Identify candidate block range (pk_hi ties span at most a few blocks for distinct-ms PKs)
4. Decompress target pk_hi block(s)
5. Binary search within decompressed pk_hi + raw pk_lo for exact match

Since pk_hi determines sort order in the vast majority of cases (different timestamps), ties
at block boundaries are rare. Typically only one block needs decompression per seek.

#### 5.4.4 Summary for UUIDv7

| Region | Compression | Ratio | Seek impact |
|---|---|---|---|
| pk_hi | Block-compressed (SplitTranspose + delta) | 2-5x | One block decompression per seek (~2-4 μs) |
| pk_lo | **Uncompressed** | 1.0x | None |
| **Net PK** | | **1.5-2.5x** (on 16 bytes/row) | |

### 5.5 The Two-Region Layout and Compression

The on-disk format already stores PKs as two separate regions of u64 LE bytes (pk_lo, pk_hi).
This is a property of the shard format, not of the in-memory representation. The serialization
layer writes bytes; the compression layer reads bytes. They are independent.

**Why two separate u64 regions compress better than one u128 region:**

1. **For u64 tables**: pk_hi is a constant-zero region → compresses to nothing. A single u128
   region would interleave zeros with data, making the constant pattern harder to exploit.
2. **For UUIDv7**: pk_hi (timestamp-heavy, structured) and pk_lo (random-heavy, incompressible)
   have fundamentally different entropy profiles. Separate regions let the compression layer
   make independent per-region decisions (compress pk_hi, leave pk_lo raw). A single u128
   region would require SplitTranspose to recover this separation.
3. **OpenZL typed API**: `ZL_TypedRef_createNumeric(ptr, 8, count)` treats each region as a u64
   array — directly usable with OpenZL's typed codecs (delta, bitpack, etc.). A 16-byte element
   would need byte-array treatment (`Bytes(16)` in SDDL), losing typed compression benefits.
4. **SIMD paths**: OpenZL's SplitTranspose has AVX2-optimized paths for 2/4/8-byte element
   widths. The generic (no-SIMD) fallback handles 16-byte. Two u64 regions use the fast path.

**The serialization layer does not need to know about any of this.** It writes `(key & 0xFFFFFFFFFFFFFFFF).to_le_bytes()` to pk_lo and `(key >> 64).to_le_bytes()` to pk_hi,
regardless of whether `key` is a native u128, two u64 fields, or anything else. The only
place the type matters is the `element_width` parameter passed to OpenZL at compression time,
which comes from the schema (always 8 for PK regions).

### 5.6 Per-Distribution Strategy Summary

| PK type | pk_hi treatment | pk_lo treatment | Binary search change | Net PK ratio |
|---|---|---|---|---|
| u64 monotonic w/ gaps | Constant (~∞x) | Block-compressed (3-5x) | Two-level (index + block) | **5-8x** |
| u64 semi-random | Constant (~∞x) | **Raw (unchanged)** | **Unchanged** | **~2x** (hi only) |
| u128 UUIDv7 | Block-compressed (2-5x) | **Raw (unchanged)** | Two-level on pk_hi, direct on pk_lo | **1.5-2.5x** |

The decision of which strategy to use can be made **per-table at flush time** by sampling
the first ~128 pk_lo deltas:

```
max_delta = max(pk_lo[i+1] - pk_lo[i] for i in 0..127)

if max_delta < 2^32:
    block-compress pk_lo  (monotonic case)
else:
    leave pk_lo raw       (semi-random / UUIDv7 case)

if pk_hi is all zeros:
    encode as constant    (u64 case)
elif pk_hi has structure:
    block-compress pk_hi  (UUIDv7 case)
else:
    leave pk_hi raw       (shouldn't happen with supported PK types)
```

---

## 6. Integration Path

### 6.1 Dependency Chain

```
OpenZL (C/C++) ──linked by──▸ gnitz-engine (Rust) ──FFI──▸ RPython
```

This follows the existing pattern for bloom filters, XOR8 filters, WAL encoding, manifest
serialization, and compaction — all of which are Rust functions called from RPython via
`rffi.llexternal()`.

### 6.2 Layer Separation

The serialization and compression layers are fully decoupled:

```
Writer (serialization)          Compressor (compression)
─────────────────────          ──────────────────────────
Writes raw bytes to regions.    Reads raw bytes from regions.
Doesn't know about compression. Doesn't know about PK types.
Gets values from in-memory       Gets element_width from schema
representation (u128, u64,        (always 8 for PK regions).
whatever) and writes LE bytes.
```

The only configuration the compression layer needs from the schema is `element_width` — passed
as a parameter to `ZL_TypedRef_createNumeric(ptr, element_width, count)`. For PK regions this
is always 8. For payload columns it's the column's `field_type.size`. The compression layer
never needs to know whether the bytes came from a native u128, two u64 fields, or any other
in-memory representation.

Decompression needs nothing at all — OpenZL frames are self-describing (the codec recipe and
decompressed size are embedded in the frame header).

### 6.3 Rust Wrapper Functions

New FFI functions in `gnitz-engine`:

```rust
// Single-frame compression (for payload columns)
#[no_mangle]
pub extern "C" fn gnitz_compress_region(
    src: *const u8, src_len: usize,
    dst: *mut u8, dst_cap: usize,
    element_width: u32,  // 0 = untyped bytes, 1/2/4/8 = numeric
) -> i64  // compressed size, or negative error code

// Single-frame decompression
#[no_mangle]
pub extern "C" fn gnitz_decompress_region(
    src: *const u8, src_len: usize,
    dst: *mut u8, dst_cap: usize,
) -> i64  // decompressed size, or negative error code

// Query decompressed size (no decompression needed)
#[no_mangle]
pub extern "C" fn gnitz_decompressed_size(
    src: *const u8, src_len: usize,
) -> i64

// Block-compressed region: compress B-row blocks with index
#[no_mangle]
pub extern "C" fn gnitz_compress_region_blocked(
    src: *const u8, src_len: usize,
    dst: *mut u8, dst_cap: usize,
    element_width: u32,
    block_rows: u32,
    total_rows: u32,
) -> i64

// Block-compressed region: decompress single block by index
#[no_mangle]
pub extern "C" fn gnitz_decompress_block(
    region: *const u8, region_len: usize,
    block_idx: u32,
    dst: *mut u8, dst_cap: usize,
) -> i64
```

### 6.4 Build Integration

In `gnitz-engine/build.rs`:

```rust
// Phase 1: zstd (already a Rust crate, no build complexity)
// cargo dependency: zstd = "0.13"

// Phase 2: OpenZL (C library, needs cc crate)
cc::Build::new()
    .files(openzl_sources)
    .include("vendor/openzl/include")
    .flag("-std=c11")
    .compile("openzl");
```

OpenZL can be vendored as a git submodule or fetched at build time. The C library compiles
with a standard C11 compiler. C++ is only needed for the C++ wrapper API (not needed here).

### 6.5 RPython FFI Bindings

Following the existing pattern (e.g., `bloom.py`, `xor8.py`, `manifest.py`):

```python
# gnitz/storage/compression.py

_compress_region = rffi.llexternal(
    'gnitz_compress_region',
    [rffi.CCHARP, rffi.LONG, rffi.CCHARP, rffi.LONG, rffi.UINT],
    rffi.LONGLONG,
    compilation_info=eci,
)

_decompress_region = rffi.llexternal(
    'gnitz_decompress_region',
    [rffi.CCHARP, rffi.LONG, rffi.CCHARP, rffi.LONG],
    rffi.LONGLONG,
    compilation_info=eci,
)
```

---

## 7. Phased Implementation Plan

### 7.1 Phase 1: Payload Compression with zstd

**Goal**: Prove the compression architecture (format versioning, per-region flags, lazy
decompression, compaction awareness) with a zero-risk dependency.

**Changes**:

1. **Shard format v4**: Add compression byte to directory entries (see Section 8).

2. **Writer path** (`writer_table.py` + `compact.rs`):
   - After building each payload region buffer, compress with zstd level 3.
   - Write compressed bytes to disk. Directory entry records compressed size.
   - Checksum the compressed bytes (validation works on mmap'd compressed data).

3. **Reader path** (`shard_table.py`):
   - `_init_region()`: Read compression byte from directory. If compressed, store a
     `CompressedRegion` marker instead of a `MappedBuffer`.
   - `_check_region()` / first access: Allocate heap buffer sized to decompressed size
     (stored in frame header), decompress, replace the region pointer.
   - PK regions (0, 1, 2): Not compressed. Initialized as raw `MappedBuffer` (current behavior).

4. **Compaction** (`compact.rs`):
   - Input shards: Decompress each payload region at shard open time (sequential access
     pattern — full region needed anyway).
   - Output shard: Compress payload regions at finalize.
   - Merge loop unchanged.

5. **pk_hi optimization**: Detect all-zero pk_hi at flush time. Write a single-frame
   compressed region (zstd will compress 8KB of zeros to ~20 bytes). Reader decompresses on
   first access. Binary search on pk_hi still works after decompression — it's just comparing
   zeros.

**What this does NOT change**: Binary search, cursor architecture, PK access, ghost skipping,
weight access. All PK-path code remains identical.

**Expected savings**: 30-45% of total shard size (payload + pk_hi). Higher for tables with
many columns or highly compressible data types.

### 7.2 Phase 2: Upgrade to OpenZL for Payload

**Goal**: Better compression ratios on typed columnar data.

**Changes**:

1. Add OpenZL as a C dependency in `gnitz-engine`.
2. Replace zstd compress/decompress calls with OpenZL typed API:
   - Integer columns: `ZL_TypedRef_createNumeric(ptr, stride, count)`
   - String columns: `ZL_TypedRef_createString(blob_buf, blob_size, lengths, count)` or
     treat the 16-byte string structs as `createNumeric(ptr, 16, count)` (struct mode)
   - Null bitmap: `createNumeric(ptr, 8, count)` (mostly-zero u64 array)
3. Directory entry compression byte changes from `0x01` (zstd) to `0x02` (openzl-single).
4. Reader: `ZL_decompress()` instead of zstd decompress. The lazy-access pattern is identical.
5. Old zstd-compressed shards remain readable (reader dispatches on compression byte).

**Training pipeline** (optional enhancement):
- At first compaction of each table, run OpenZL trainer on the payload regions.
- Store trained `.zlc` profile in the manifest metadata (keyed by table_id + schema_hash).
- Use a default profile (sorted-int or generic) before first training.
- Re-train after schema changes or significant data distribution shifts.

**Expected improvement over Phase 1**: Payload compression 2-4x (vs. 1.5-2.5x with zstd).
Greater improvement on highly typed data (integer columns, timestamps). Marginal improvement
on string-heavy blob heaps.

### 7.3 Phase 3: Block-Compressed PKs

**Goal**: Compress PK columns for distributions where it's worthwhile (monotonic u64,
UUIDv7 pk_hi), with minimal binary search overhead.

**Changes**:

1. **Block index format** (appended to region):
   ```
   [Frame₀] [Frame₁] ... [Frameₖ₋₁]
   [k × BlockIndexEntry]
   [index_entry_count: u32]
   [index_start_offset: u32]
   ```
   Each `BlockIndexEntry` (zone-map-ready; see `lightweight-columnar-encoding.md` §11.7):
   ```
   min_value:    u64  (8 bytes) — first/min PK value in block
   max_value:    u64  (8 bytes) — last/max PK value in block
   frame_offset: u32  (4 bytes) — byte offset of frame within region
   frame_size:   u32  (4 bytes) — compressed frame size in bytes
   ────────────────────
   24 bytes per entry
   ```

2. **Writer**: At flush time, sample deltas to decide compression strategy per PK region
   (see Section 5.6 heuristic). If block-compressing, split the column into B-row blocks,
   compress each as an independent OpenZL frame, append block index.

3. **Reader**: New `BlockCompressedRegion` abstraction:
   ```python
   class BlockCompressedRegion:
       def __init__(self, mmap_ptr, region_size, block_rows):
           self.mmap_ptr = mmap_ptr
           self.region_size = region_size
           self.block_rows = block_rows
           self._parse_index()
           self._cache = [None] * self.num_blocks  # or small LRU

       def read_u64(self, byte_offset):
           row = byte_offset // 8
           block = row // self.block_rows
           if self._cache[block] is None:
               self._cache[block] = self._decompress_block(block)
           local_row = row % self.block_rows
           return self._cache[block].read_u64(local_row * 8)
   ```

4. **Binary search**: Two-level. First search the block index (O(log(n/B)) comparisons on
   `min_pk` values), then decompress one block and search within it (O(log B) comparisons).

5. **Compaction**: Iterate blocks sequentially. Decompress block[i], process rows, move to
   block[i+1]. Simple sequential access pattern — no random block access.

6. **Block size selection**: B=1024 rows is a good default.
   - 8 KB raw per block (u64 column) → 2-4 KB compressed → ~2-4 μs decompression
   - Block index: 16 bytes per block → 1.6 KB per 100K rows → fits in L1 cache
   - Amortizes frame overhead well (50 bytes / 8 KB = 0.6%)

**What to block-compress**:
- pk_lo for monotonic u64: Yes (3-5x ratio)
- pk_lo for semi-random u64: No (1.2-1.4x ratio, not worth seek cost)
- pk_lo for UUIDv7: No (~1.1x ratio)
- pk_hi for u64 tables: Single-frame constant (not blocked — entire region is zeros)
- pk_hi for UUIDv7: Yes (2-5x ratio)

---

## 8. Shard Format v4 Specification

### 8.1 Header Changes

```
Offset  Size  Field
0       8     Magic: 0x31305F5A54494E47 ("GNITZ_01") — unchanged
8       8     Format version: 4 (was 3)
16      8     Row count — unchanged
24      8     Directory offset — unchanged
32      8     Table ID — unchanged
40      8     XOR8 offset — unchanged
48      8     XOR8 size — unchanged
56      8     Reserved — unchanged
```

### 8.2 Directory Entry Changes

**v3** (24 bytes):
```
[offset: u64] [size: u64] [checksum: u64]
```

**v4** (32 bytes, padded to maintain 8-byte alignment):
```
[offset: u64] [size: u64] [checksum: u64] [compression: u8] [reserved: 7 bytes]
```

Compression byte values:
```
0x00 = RAW          — current behavior, uncompressed
0x01 = ZSTD         — zstd single-frame (Phase 1)
0x02 = OPENZL       — OpenZL single-frame, decompress entire region on first access (Phase 2)
0x03 = OPENZL_BLOCK — OpenZL block-compressed with block index (Phase 3)
0xFF = CONSTANT     — entire region is a repeated single value (stored as: value + count)
```

The `size` field stores the on-disk (compressed) size. The decompressed size is obtained from
the compression frame header (`ZL_getDecompressedSize()` for OpenZL, `ZSTD_getFrameContentSize()`
for zstd) without decompressing.

The `checksum` field covers the compressed bytes (what's on disk), not the decompressed content.
This allows validation of the mmap'd data without decompression.

### 8.3 Backward Compatibility

v4 readers must handle v3 shards (all regions are implicitly RAW). v3 readers encountering a
v4 shard should fail gracefully with a version-mismatch error. The format version in the header
enables this dispatch.

Compaction naturally upgrades shard format: input shards may be v3 or v4, output shard is
always v4.

---

## 9. Open Questions and Risks

### 9.1 OpenZL API Stability

OpenZL is v0.1.0. The C API surface may change before 1.0. Mitigations:
- Pin to a specific git commit or release tag.
- Wrap behind gnitz-engine's own Rust API — if OpenZL changes, only the Rust wrapper changes.
- The compressed frame format has backward-compatible decompression guarantees for released
  versions.

### 9.2 Build Complexity

Adding a C/C++ dependency to the Rust build chain requires:
- `cc` crate in `build.rs` (or `cmake` crate if using CMake)
- Vendoring OpenZL source or managing it as a submodule
- C11 compiler requirement (already satisfied on all target platforms)
- Potential issues with cross-compilation if ever needed

Phase 1 avoids this entirely by using zstd (pure Rust crate).

### 9.3 Memory Overhead

- **Compression**: OpenZL can use ~10x payload size during compression. For a 64 KB region,
  that's ~640 KB — manageable, but worth noting if compressing many regions concurrently.
- **Decompression**: Requires a heap buffer for the decompressed region. For payload columns,
  this replaces the mmap'd region pointer. The decompressed buffer must live until the shard
  is closed or evicted. Unlike mmap, the OS cannot page it out and re-read from disk — it's
  heap memory.
- **Block cache (Phase 3)**: The `BlockCompressedRegion` cache holds decompressed blocks. With
  B=1024 rows × 8 bytes = 8 KB per block, a cache of 4-8 blocks per PK region = 32-64 KB.
  Small enough to be negligible.

### 9.4 Decompressed Payload Lifetime

When a compressed payload region is decompressed into a heap buffer, that buffer replaces the
mmap'd pointer for the lifetime of the shard view. Unlike mmap, the heap buffer:
- Cannot be lazily paged in by the OS (fully resident once allocated)
- Cannot be reclaimed by the kernel under memory pressure
- Must be explicitly freed when the shard view is closed

For queries that scan many columns across many shards, this could increase peak memory usage.
Potential mitigations:
- Decompress columns on demand (already the plan — lazy access)
- Track decompressed region sizes in the shard index for memory budgeting
- Evict decompressed columns from cold shards under memory pressure (LRU at shard level)

### 9.5 Training Pipeline Operations

OpenZL training requires:
- Representative sample data (ideally from the first few shards of each table)
- Offline training invocation (could be done during compaction)
- Persistent storage of trained profiles (manifest metadata)
- Profile invalidation on schema changes

This adds operational complexity. The default (untrained) profiles still beat zstd on typed
data, so training is an optimization, not a requirement.

### 9.6 Compression Decision at Flush Time

The per-region compression decision (compress or raw) must be made at flush time without
knowing the full data distribution. The delta-sampling heuristic (Section 5.6) is a good
approximation but could misfire on pathological distributions. Conservative default: if in
doubt, don't compress PK columns (compression of payload columns is always safe).

### 9.7 zstd as Long-Term Fallback

Even after Phase 2 (OpenZL for payload), zstd support should be retained:
- Old shards compressed with zstd must remain readable forever
- zstd is the fallback for data where OpenZL's typed compression doesn't help (e.g., random
  byte blobs)
- Simpler build for development/testing (skip OpenZL dependency)

### 9.8 Streaming Decompression

OpenZL does not currently support streaming decompression. All input must be present before
decompression begins. This is fine for gnitz's use case (mmap'd regions are fully available),
but would matter for a network-fetch scenario.

---

## 10. References

- OpenZL GitHub: https://github.com/facebook/openzl
- OpenZL website: https://openzl.org/
- OpenZL paper: arXiv:2510.03203
- Meta engineering blog: https://engineering.fb.com/2025/10/06/developer-tools/openzl-open-source-format-aware-compression-framework/
- UUIDv7 specification: RFC 9562
- zstd seekable format: https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/
- gnitz shard format: `gnitz/storage/layout.py`
- gnitz cursor architecture: `gnitz/storage/cursor.py`, `gnitz/storage/shard_table.py`
- gnitz Rust compaction: `crates/gnitz-engine/src/compact.rs`
- gnitz PK types: `gnitz/core/types.py`, `gnitz/core/batch.py`
