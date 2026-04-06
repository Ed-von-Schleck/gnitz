# Lightweight Columnar Encoding: FastLanes and Pcodec for gnitz Shards

Research date: 2026-03-25

Companion to: `research/openzl-shard-compression.md`

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [The Pure-Rust Encoding Landscape](#2-the-pure-rust-encoding-landscape)
3. [FastLanes: Deep Dive](#3-fastlanes-deep-dive)
4. [Pcodec: Deep Dive](#4-pcodec-deep-dive)
5. [FastLanes vs Pcodec: Head-to-Head](#5-fastlanes-vs-pcodec-head-to-head)
6. [Interaction with DBSP Operators](#6-interaction-with-dbsp-operators)
7. [Interaction with Compaction](#7-interaction-with-compaction)
8. [Interaction with Cursors and Binary Search](#8-interaction-with-cursors-and-binary-search)
9. [Per-Region Encoding Strategy](#9-per-region-encoding-strategy)
10. [Comparison with the OpenZL Approach](#10-comparison-with-the-openzl-approach)
11. [Open Questions](#11-open-questions)
12. [References](#12-references)

---

## 1. Motivation

The companion document (`openzl-shard-compression.md`) evaluated Meta's OpenZL as a
format-aware compression framework for gnitz shards. A key finding was that OpenZL's primary
value proposition — discovering structure in opaque data via format parsers and field
extraction — is wasted on gnitz, which already stores data as typed columnar arrays. The
codecs that would actually help (delta encoding, bitpacking, frame-of-reference, entropy
coding) exist as mature, pure-Rust crates with no C/C++ dependency.

This document evaluates the two most relevant Rust-native alternatives:

- **FastLanes** (`fastlanes` crate): SIMD-friendly delta + bitpacking + FOR in 1024-value
  blocks. Pure Rust, `no_std`, auto-vectorizes via LLVM. Used by Vortex, which DuckDB adopted
  as of January 2026.

- **Pcodec** (`pco` crate): Mode detection + delta + entropy-coded binning via 4-way
  interleaved tANS. Pure Rust. Achieves 29-162% better compression ratios than delta+bitpack
  approaches. Used in production by CnosDB and Zarr.

Both integrate trivially with `gnitz-engine` — a `Cargo.toml` dependency, no build system
changes, no C/C++ toolchain.

### 1.1 Academic Background

The approach of cascading lightweight encodings for columnar storage is well-established:

- **BtrBlocks** (TU Munich, SIGMOD 2023): Demonstrated that cascading lightweight encodings
  (constant → RLE → dictionary → FOR+bitpack) beats Parquet+ZSTD by 38% on size AND is
  10-25x faster to decompress. The key insight: type-aware encoding removes redundancy that
  generic compressors cannot exploit, making a second-pass generic compressor largely redundant.

- **FastLanes** (CWI Amsterdam, VLDB 2023/2025): Showed that a transposed memory layout
  enables scalar code to auto-vectorize to full SIMD width, achieving >100 billion integers/sec
  decode with no explicit SIMD intrinsics.

- **VLDB 2024 Empirical Evaluation** (Zeng et al.): "For most columns, the space savings of
  block compression [zstd/lz4] is limited because they are already compressed via lightweight
  encoding." Lightweight encoding first, generic compression second (if at all).

---

## 2. The Pure-Rust Encoding Landscape

### 2.1 Crates Relevant to gnitz

| Crate | Version | Downloads | What it does | Types |
|---|---|---|---|---|
| `fastlanes` | 0.5.0 | 430K | Delta + bitpack + FOR + RLE, 1024-value blocks | u8-u64 |
| `pco` | 1.0.1 | 413K | Mode detection + delta + tANS entropy binning | u8-u64, i8-i64, f16-f64 |
| `lz4_flex` | 0.13.0 | 82M | LZ4 block/frame compression, pure Rust | byte-level |
| `zstd` | 0.13.3 | 250M | Zstandard compression (wraps C libzstd) | byte-level |
| `bitpacking` | 0.9.3 | 15.4M | SIMD bitpacking (SSE3/AVX2), delta built-in | **u32 only** |
| `fsst-rs` | 0.5.9 | 431K | String compression (1-byte symbol table, ~2x) | strings |

### 2.2 What Was Explicitly Not Evaluated

- **Roaring bitmaps** (`roaring` crate): Evaluated in companion document
  `research/roaring-bitmap-analysis.md`. Not applicable to gnitz's current row-major null
  layout, but relevant for the Rust-native column-major design as an on-disk encoding for
  sparse null vectors. The real win is column-major nulls themselves (21x storage savings for
  typical schemas), not roaring specifically.

- **Vortex framework** (`vortex-*` crates): Full columnar file format with sampling-based
  scheme selection. Uses FastLanes and Pcodec internally. Evaluated as reference architecture
  but too heavyweight to adopt as a dependency (dozens of sub-crates, its own array type system).
  Individual encoding crates (fastlanes, pco) are usable standalone.

---

## 3. FastLanes: Deep Dive

### 3.1 Core Design

FastLanes implements the CWI paper's unified transposed layout. Data is organized so that
standard scalar loops auto-vectorize to full SIMD width via LLVM, without explicit SIMD
intrinsics. The crate is `#![no_std]`, zero runtime dependencies beyond `arrayref`,
`num-traits`, `paste`, and `seq-macro`.

**Block size**: Fixed at 1024 values. This is a fundamental constant derived from the
transposed layout. The last block must be zero-padded to 1024.

**Supported types**: `u8`, `u16`, `u32`, `u64` via the `FastLanes` trait. Signed integers are
handled by transmuting to unsigned equivalents (e.g., zigzag encoding or FOR subtraction to
bring values non-negative).

### 3.2 Encoding Modules

#### BitPacking

```rust
pub trait BitPacking: FastLanes {
    // Compile-time width (monomorphized per width 0..=T::BITS)
    fn pack<const W: usize, const B: usize>(
        input: &[Self; 1024], output: &mut [Self; B]
    );
    fn unpack<const W: usize, const B: usize>(
        input: &[Self; B], output: &mut [Self; 1024]
    );
    fn unpack_single<const W: usize, const B: usize>(
        packed: &[Self; B], index: usize
    ) -> Self;

    // Runtime width (dispatches via match)
    unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self]);
    unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self]);
}
```

**Bit width is caller-provided.** The crate does not auto-detect. You compute it yourself
(scan for max value, take `ceil(log2(max + 1))`).

**Output size**: For W-bit packing of 1024 values: `128 * W` bytes. At W=10 for u64 values:
1280 bytes vs 8192 raw = **6.4x**.

**Random access**: `unpack_single(packed, index)` extracts one value in O(1) without
decompressing the full block. Slower than bulk unpack for >~10 values.

#### Frame-of-Reference (FOR)

```rust
pub trait FoR: BitPacking {
    fn for_pack<const W: usize, const B: usize>(
        input: &[Self; 1024], reference: Self, output: &mut [Self; B]
    );
    fn unfor_pack<const W: usize, const B: usize>(
        input: &[Self; B], reference: Self, output: &mut [Self; 1024]
    );
}
```

**Fused kernel**: Subtracts reference and bitpacks in a single pass. No intermediate buffer.
The reference value is a scalar (typically `min(block)`) stored as per-block metadata.

FOR preserves `unpack_single` random access (subtract reference, extract bits). This is
relevant for PK binary search — see Section 8.

#### Delta Encoding

```rust
pub trait Delta: BitPacking {
    fn delta<const LANES: usize>(
        input: &[Self; 1024], base: &[Self; LANES], output: &mut [Self; 1024]
    );
    fn undelta<const LANES: usize>(
        input: &[Self; 1024], base: &[Self; LANES], output: &mut [Self; 1024]
    );
    fn undelta_pack<const LANES: usize, const W: usize, const B: usize>(
        input: &[Self; B], base: &[Self; LANES], output: &mut [Self; 1024]
    );
}
```

**How it works**: The 1024-value block is transposed into `LANES` independent lanes (16 lanes
for u64). Delta is computed within each lane. The `base` array stores one value per lane
(LANES values). After delta, the deltas are bitpacked.

**Fused decode**: `undelta_pack` unpacks and applies cumulative sum in one pass.

**Pipeline**: `Transpose → Delta → BitPack` (encode), `undelta_pack → Untranspose` (decode)

**Per-block metadata for delta**: LANES base values. For u64: 16 × 8 = 128 bytes. Plus 1 byte
for bit width. Total: 129 bytes overhead per 1024-value block.

**No random access**: Deltas are cumulative. Must decode full block.

### 3.3 Per-Block Metadata Summary

| Encoding | Metadata | Size (u64) |
|---|---|---|
| BitPack only | bit_width: u8 | 1 byte |
| FOR + BitPack | bit_width: u8, reference: T | 9 bytes |
| Delta + BitPack | bit_width: u8, bases: [T; LANES] | 129 bytes |

Compare to OpenZL's 30-55 byte frame header for a simple pipeline.

### 3.4 Handling Outliers (Patches)

Plain bitpacking uses a single global bit width. One outlier inflates the width for all 1024
values. Two strategies:

1. **Use FOR instead of delta**: FOR(min) + bitpack(max-min). If the range is tight, this
   works well. If one outlier is far from the range, this is bad.

2. **Patched encoding** (how Vortex does it): Compute a bit-width histogram over the block.
   Use a cost model to pick the optimal width: `cost = (width * 1024) / 8 + num_exceptions *
   (type_bytes + 4)`. Outliers exceeding the chosen width are stored in a separate sparse
   patches array. This is essentially PFOR (Patched Frame-of-Reference).

   gnitz would need to implement this patching logic (~200-300 lines of Rust) or depend on a
   PFOR crate. The `fastpfor-rs` crate (v0.8.1, March 2026) provides `FastPFor256` and
   `FastPFor128` with automatic patching, but only for u32.

### 3.5 Performance

From the FastLanes paper and measured characteristics of the Rust crate:

- **BitPack decode**: ~15-30 GB/s (auto-vectorized, depends on width and type)
- **BitPack encode**: Similar range
- **Fused FOR decode**: Near-identical to plain bitpack (the add is free)
- **Fused delta decode**: Slightly slower due to cumulative sum dependency within each lane

Compile with `RUSTFLAGS='-C target-cpu=native'` for best auto-vectorization. The crate
uses `#[inline(never)]` on outer functions and lane-iteration inner loops that LLVM vectorizes.

---

## 4. Pcodec: Deep Dive

### 4.1 Core Design

Pcodec is a lossless numerical compression codec that approaches Shannon entropy through
three stages:

1. **Mode detection** (offline, on sample data): Detects structure like integer multiples
   (GCD patterns), float quantization waste, or dictionary-encodable distributions.
2. **Delta encoding**: Consecutive delta (orders 1-7) or lookback delta. Order selected by
   sampling ~200 consecutive values and picking the cheapest.
3. **Binning + tANS entropy coding**: Values are assigned to bins, each with a tANS weight,
   lower bound, and offset bit count. 4-way interleaved tANS decoding hides data-dependency
   latency.

### 4.2 Wrapped API (for storage engine embedding)

Pcodec's "wrapped" API separates compressed data into three components that you place wherever
you want in your file format:

| Component | Role | Stored where (in gnitz) |
|---|---|---|
| **Header** (2 bytes) | Format version | Once per shard file |
| **Chunk metadata** | Mode, delta config, bin tables (tANS weights, boundaries) | Per-column, in block index |
| **Data pages** | Compressed bits | Per-block, inline in region |

**Hierarchy**:
- **Chunk**: Unit of compression. One mode + delta config per chunk. Best with >10K values.
- **Page**: Unit of random access. Independently decompressible given chunk metadata.
  Configurable size, minimum ~1K values.
- **Batch**: Fixed at 256 values. Implicit boundaries within a page. Not independently
  decompressible (tANS state carries over).

### 4.3 API Usage

```rust
use pco::wrapped::{FileCompressor, FileDecompressor};
use pco::{ChunkConfig, PagingSpec};

// Encode a column of u64 values into pages of 1024
let data: &[u64] = &column_values;
let config = ChunkConfig::default()
    .with_paging_spec(PagingSpec::EqualPagesUpTo(1024));

let fc = FileCompressor::default();
let mut header_buf = Vec::new();
fc.write_header(&mut header_buf)?;

let mut cc = fc.chunk_compressor::<u64>(data, &config)?;
let mut meta_buf = Vec::new();
cc.write_meta(&mut meta_buf)?;     // chunk metadata (bin tables, etc.)

for page_idx in 0..cc.n_per_page().len() {
    let mut page_buf = Vec::new();
    cc.write_page(page_idx, &mut page_buf)?;
    // Store page_buf at known offset in shard region
}

// Decode a single page
let (fd, rest) = FileDecompressor::new(&header_bytes)?;
let (mut cd, rest) = fd.chunk_decompressor::<u64>(rest)?;
let mut pd = cd.page_decompressor(&page_bytes, page_count)?;
let mut output = vec![0u64; page_count];
pd.read(&mut output)?;
```

### 4.4 Random Access

**Page-level**: YES. Each page is independently decompressible given the chunk metadata. Store
page offsets in the block index.

**Within-page**: NO. Batches within a page depend on tANS state from the previous batch. Must
decompress from the start of the page.

**Practical**: With pages of 1024 values, random access granularity matches FastLanes' 1024-value
block size. For PK binary search with a block index, this is equivalent.

### 4.5 Compression Ratios

From published benchmarks (M3 Max, real-world columnar data):

| Dataset | Types | Pco | Blosc+Zstd | Parquet+Zstd |
|---|---|---|---|---|
| Air quality | i16, i32, i64 | **10.00x** | 5.16x | 3.83x |
| r/place | i32, i64 | **6.79x** | 4.71x | 4.64x |
| Taxi (NYC) | f64, i32, i64 | **6.89x** | 2.68x | 5.03x |

Compared to TurboPFor (a C delta+bitpack codec, comparable to FastLanes in approach):

| Dataset | Pco | TurboPFor+Zstd | Pco advantage |
|---|---|---|---|
| f64_spain_gas_price | 9.73x | 5.06x | **+92%** |
| f32_citytemp | 3.85x | 1.47x | **+162%** |
| f32_tpch_lineitem | 3.15x | 1.97x | **+60%** |
| f64_tpch_order | 2.50x | 1.83x | **+37%** |

**Why pcodec beats delta+bitpack**: Bitpacking uses a single global bit width per block. If
deltas range from 0-1000 but 99% are 0-10, bitpack still uses `ceil(log2(1000))` = 10 bits
per value. Pcodec's binning creates tight bins for common values (few tANS bits + small offset)
and wider bins for outliers, approaching Shannon entropy.

### 4.6 Decompression Speed

| Dataset | Pco decomp | TurboPFor+Zstd decomp |
|---|---|---|
| f64_spain_gas_price | 0.97 GB/s | 0.52 GB/s |
| f32_citytemp | 1.28 GB/s | 0.78 GB/s |
| f64_tpch_order | 1.22 GB/s | 0.47 GB/s |
| f64_nyc_taxi2015 | 1.35 GB/s | 0.42 GB/s |

Note: These compare Pco against TurboPFor+Zstd (a two-stage pipeline). Raw FastLanes bitunpack
without zstd would be faster (5-10+ GB/s) but with much worse ratios. The question is whether
the ratio improvement of Pco justifies the lower decode speed vs raw bitpack.

### 4.7 Specific Column Type Projections for gnitz

| Column type | Expected Pco behavior | Expected ratio |
|---|---|---|
| Sorted monotonic u64 PKs | Delta order 1 reduces to near-constant deltas. Binning compresses to ~1-2 bits/value. | **10-30x** |
| Sorted UUIDv7 pk_hi | Delta order 1. Timestamp component dominates. Bimodal deltas (intra-ms vs boundary). | **3-8x** |
| Weight (i64, mostly +1/-1) | Dict mode or Classic with 2 bins. Each value ≈ 1 tANS bit. | **32-64x** |
| Random u64 | Near-incompressible. Guaranteed not to expand beyond raw + small metadata. | **~1.0x** |
| Low-cardinality i32 (e.g., status codes) | Dict mode. Log2(cardinality) bits/value + overhead. | **5-15x** |
| Timestamps (i64, monotonic) | Delta order 1. Inter-arrival times entropy-coded. | **5-15x** |
| F64 measurements | FloatMult or FloatQuant mode if applicable. Otherwise delta + binning. | **2-5x** |

### 4.8 The Weight Column: A DBSP-Specific Win

In DBSP Z-set algebra, every row carries a weight:
- **+1**: Row inserted / alive
- **-1**: Row deleted / retracted
- **0**: Ghost (logically gone, physically present in unconsolidated shards)
- **>1 or <-1**: Rare; represents row multiplicity

A weight column of 100K rows that is 99% +1 and 1% -1 has approximately 0.08 bits of entropy
per value. With Pcodec's binning (2 bins: +1 and -1, nearly equal weights), each value costs
≈1 tANS bit. That's 100K bits = 12.5 KB vs 800 KB raw = **64x compression**.

FastLanes would need to zigzag-encode (+1 → 1, -1 → 2), then bitpack at 2 bits per value
= 25 KB = **32x**. Still very good, but Pco's entropy coding gets closer to the theoretical
minimum.

After compaction, all weights are non-zero (ghosts removed). The distribution becomes even
more skewed (almost all +1), improving compression further.

---

## 5. FastLanes vs Pcodec: Head-to-Head

| Criterion | FastLanes | Pcodec |
|---|---|---|
| **Compression ratio** | Baseline (delta+bitpack) | **29-162% better** |
| **Decompression speed** | **5-10+ GB/s** (raw bitunpack) | 1-4 GB/s |
| **Compression speed** | **1-5 GB/s** | 0.1-0.7 GB/s |
| **Block size** | 1024 values (fixed) | Configurable pages (min ~1K) |
| **Random access** | Block-level; FOR preserves `unpack_single` | Page-level only |
| **Per-block metadata** | 1-129 bytes (encoding-dependent) | Variable (chunk meta shared) |
| **Outlier handling** | None built-in (need PFOR wrapper) | Built-in via binning |
| **Weight column** | 32x (zigzag + 2-bit bitpack) | **64x** (tANS entropy) |
| **Sorted PKs** | 4-8x (delta + bitpack) | **10-30x** (delta + entropy) |
| **Dependencies** | `no_std`, zero C deps | `no_std`-compatible core, zero C deps |
| **Integration effort** | `Cargo.toml` + ~200-400 lines wrapping | `Cargo.toml` + ~100-200 lines wrapping |
| **Maturity** | Used by Vortex/DuckDB | Used by CnosDB/Zarr, 1.0 release |

### 5.1 Which Matters More for gnitz?

gnitz is a **disk-based storage engine**. Shards are mmap'd files read from NVMe (3-7 GB/s
sequential, lower for random). The bottleneck for reads is I/O, not CPU. This favors higher
compression ratio (less I/O) over higher decode speed:

- Pcodec at 1-4 GB/s decode is sufficient — NVMe doesn't deliver faster than that anyway.
- FastLanes at 5-10+ GB/s exceeds NVMe bandwidth — the extra decode speed is wasted on
  disk-sourced data.
- The 29-162% ratio improvement of Pcodec translates directly to less disk read per query.

For **compaction** (write-heavy), compression speed matters more. FastLanes at 1-5 GB/s
compress vs Pcodec at 0.1-0.7 GB/s is a 5-7x difference. On a 100K-row shard with 10 columns
of 8 bytes each (8 MB payload), FastLanes compresses in ~2 ms; Pcodec in ~12 ms. Both are
negligible compared to the fsync cost (~1-10 ms).

**Conclusion**: For the on-disk shard format, Pcodec's ratio advantage outweighs FastLanes'
speed advantage. If there's ever a need for in-memory compressed representation (hot cache),
FastLanes' speed advantage becomes relevant — but that's a different concern.

### 5.2 They're Not Mutually Exclusive

Vortex uses both: FastLanes for lightweight bitpacking stages, Pcodec as an optional
high-ratio backend. gnitz could:

1. Start with one (simpler integration, fewer dependencies)
2. Add the other later if measurements justify it

The shard format's per-region compression byte (Section 9) supports multiple codecs. A region
compressed with FastLanes and one compressed with Pcodec can coexist in the same shard.

---

## 6. Interaction with DBSP Operators

### 6.1 Key Finding: Encoding Is Transparent to Operators

Every DBSP operator in gnitz accesses column data through the `ColumnarBatchAccessor`
interface, which reads individual typed values per row:

```python
# Inside op_filter, op_map, op_reduce, op_join, etc.
accessor.bind(batch, row_idx)
value = accessor.get_int_signed(col_idx)     # reads 8 bytes at col_bufs[ci] + row*stride
is_null = accessor.is_null(col_idx)          # reads null word, tests bit
```

The expression bytecode VM (`expr.py`) executes inside operator loops with per-row column
reads:

```python
# EXPR_LOAD_COL_INT: regs[dst] = accessor.get_int_signed(a1)
# EXPR_LOAD_COL_FLOAT: regs[dst] = float2longlong(accessor.get_float(a1))
```

**Operators cannot operate on encoded data.** They require full-width typed values. Encoding
must be fully decoded before data reaches any operator.

### 6.2 Where Decoding Happens

The clean boundary is the **storage layer**. Operators receive `ArenaZSetBatch` objects with
raw (decoded) column buffers. The decoding point is:

1. **Shard reader** (`shard_table.py` / `shard_reader.rs`): When a shard region is first
   accessed, decode the compressed bytes into a heap-allocated buffer. Replace the mmap'd
   pointer with the decoded buffer. This fits the existing lazy validation pattern.

2. **MemTable flush** (`writer_table.py`): MemTable data is already in raw `ArenaZSetBatch`
   format. No decoding needed — encoding happens at shard write time.

### 6.3 Operator Access Patterns (for context)

All operators access data sequentially or via cursor merge walks. None do random column access:

| Operator | Column access pattern |
|---|---|
| FILTER | Sequential: per-row predicate evaluation on predicate columns |
| MAP | Sequential: per-row transformation, reads input columns, writes output |
| UNION | Merge walk: PK comparison only, bulk range copy for payloads |
| JOIN | Merge walk: PK comparison, then per-match accessor pair reads |
| DISTINCT | Two-pass: PK + weight only (pass 1), then bulk indexed subset copy (pass 2) |
| REDUCE | Group walk: grouping column comparison, then per-group aggregation |
| NEGATE | Bulk: weight flip only, no payload access |
| INTEGRATE | Terminal: write to storage |

**Bulk copy operations** (`append_batch`, `copy_rows_indexed`) use `c_memmove` per column
region. These operate on raw bytes and would need decoded source data.

### 6.4 What This Means

**No operator changes required.** Encoding is invisible beyond the storage layer boundary.
`ArenaZSetBatch` column buffers remain raw typed arrays. The only code that touches
encoded/decoded data is the shard reader and shard writer.

---

## 7. Interaction with Compaction

### 7.1 Current Compaction Data Flow

```
Input Shards (mmap'd)
    ↓
MappedShard::get_pk() → read u64 from mmap
MappedShard::get_col_ptr() → &[u8] slice from mmap (zero-copy)
    ↓
Tournament Tree (min-heap on u128 PK)
    ↓
Merge Loop: sum weights, pick exemplar row
    ↓
ShardWriter::add_row() → extend_from_slice per column (memcpy from mmap to Vec)
    ↓
ShardWriter::finalize() → build image buffer, compute checksums, write file
```

### 7.2 With Encoding

```
Input Shards (mmap'd, encoded regions)
    ↓
Decode on first access:
  - PK regions: decode block(s) needed for current cursor position
  - Payload regions: decode full column on first access (lazy)
  - Weight: decode full column at shard open (needed for ghost skipping)
    ↓
Tournament Tree (unchanged — operates on decoded u128 PKs)
    ↓
Merge Loop (unchanged — reads decoded bytes via get_col_ptr)
    ↓
ShardWriter::add_row() (unchanged — appends raw bytes to Vecs)
    ↓
ShardWriter::finalize():
  - NEW: encode each region Vec before writing to image
  - Compute checksums on encoded bytes
  - Write encoded shard to disk
```

### 7.3 Decode in Compaction: Sequential Access Pattern

Compaction reads shards **sequentially** — cursors only advance forward, never seek backwards.
This means:

- **PK blocks**: Decoded in order. Block 0 first, then block 1, etc. A simple single-block
  lookahead prefetch (decode block[i+1] while processing block[i]) eliminates decode latency.

- **Payload columns**: The full column is needed anyway (every row gets copied). Decode the
  entire column at shard open time. Cost: one decode pass per column, amortized over all rows.

- **Weight column**: Decoded at shard open time (needed for ghost skipping from position 0).

This is ideal for both FastLanes and Pcodec — sequential block access is their best case.

### 7.4 Encode in Compaction: At finalize()

After the merge loop, `ShardWriter` holds raw byte Vecs for each column. The natural encoding
point is `finalize()`, after all rows are written but before the image buffer is assembled:

```rust
fn finalize(/* ... */) {
    // For each column Vec:
    let encoded = encode_column(&self.col_bufs[i], element_width, encoding_strategy);
    // Use encoded bytes instead of raw bytes when building image
    // Store encoding type in directory entry
}
```

This is a single pass over each column, happening once per compaction output shard.
Compression speed of 0.1-5 GB/s (depending on codec) on a few-MB column is single-digit
milliseconds.

---

## 8. Interaction with Cursors and Binary Search

### 8.1 The Constraint (unchanged from OpenZL analysis)

Binary search on PK columns requires random access:

```python
def find_lower_bound(self, key_lo, key_hi):
    while low <= high:
        mid = (low + high) // 2
        if pk_lt(self.get_pk_lo(mid), self.get_pk_hi(mid), key_lo, key_hi):
            low = mid + 1
```

Each `get_pk_lo(mid)` reads 8 bytes at offset `mid * 8`. O(log n) probes, touching different
memory pages. Delta-encoded or entropy-coded data cannot support this.

### 8.2 Two-Level Block Index (same approach as OpenZL)

Split each PK column into blocks of B values (B=1024 for both FastLanes and Pcodec).
Store a block index:

```
[EncodedBlock₀] [EncodedBlock₁] ... [EncodedBlockₖ₋₁]
[k × BlockIndexEntry]
[index_entry_count: u32] [index_start_offset: u32]
```

Each `BlockIndexEntry` (zone-map-ready, see Section 11.7):
```
min_value: u64    (8 bytes)
max_value: u64    (8 bytes)
block_offset: u32 (4 bytes)
block_size: u32   (4 bytes)
────────────────
24 bytes per entry
```

Binary search:
1. Search block index: O(log(n/1024)) comparisons on `min_value` / `max_value`
2. Decode target block (1024 values)
3. Search within decoded block: O(log 1024) = 10 comparisons

### 8.3 FastLanes-Specific Optimization: FOR + unpack_single

FastLanes' FOR encoding preserves per-element random access via `unpack_single(packed, index)`.
This extracts one value in O(1) without decoding the full block:

```rust
let value = FoR::unpack_single::<W, B>(packed_block, index) + reference;
```

With FOR (not delta), binary search within a 1024-value block could use `unpack_single`
for each of the 10 probes, avoiding full block decode.

**Tradeoff**: FOR gives wider bit widths than delta (FOR encodes `value - min`, delta encodes
`value - prev`). For a sorted PK block of 1024 consecutive values:
- FOR bit width = `bits(pk[1023] - pk[0])` — the span of the whole block
- Delta bit width = `bits(max(pk[i+1] - pk[i]))` — the largest single gap

For monotonically increasing PKs with small gaps, delta is significantly tighter. For
semi-random PKs (where we've already decided not to compress), the point is moot.

**When FOR+unpack_single helps**: When the PK range within a block is tight enough that FOR's
wider bit width is acceptable, AND you want to avoid the ~1-4 μs cost of full block decode.
In practice, the full-decode cost is negligible compared to I/O, so this optimization has
marginal benefit for disk-based reads. It could matter for hot-cache query patterns.

### 8.4 Pcodec's Page-Level Random Access

Pcodec pages are independently decompressible given the chunk metadata. With pages of 1024
values, the random access granularity is identical to FastLanes blocks. The decode cost is
slightly higher (1-4 GB/s vs 5-10+ GB/s), meaning ~2-8 μs per page decode vs ~1-2 μs per
FastLanes block decode. Still negligible for disk-based reads.

### 8.5 When NOT to Compress PKs

Unchanged from the OpenZL analysis: semi-random u64 PKs compress only 1.2-1.4x with any
codec. The per-seek decode overhead is not justified. Leave them raw. The flush-time heuristic
(sample first 128 deltas, check max delta < 2^32) determines per-table whether PK
compression is worthwhile.

---

## 9. Per-Region Encoding Strategy

### 9.1 Shard Format Changes

Same as the OpenZL document proposes — extend the directory entry with a compression byte:

```
Directory entry (32 bytes, up from 24):
  [offset: u64] [size: u64] [checksum: u64] [compression: u8] [reserved: 7 bytes]
```

Compression byte values:

```
0x00 = RAW           — no encoding (current behavior)
0x01 = ZSTD          — generic zstd (fallback)
0x02 = FASTLANES_BP  — FastLanes bitpack (+ FOR or delta, metadata inline)
0x03 = PCODEC        — Pcodec wrapped format (chunk meta + pages)
0x04 = CONSTANT      — single repeated value (value + count)
0x05 = FASTLANES_BLK — FastLanes block-compressed with block index (for PKs)
0x06 = PCODEC_BLK    — Pcodec block-compressed with block index (for PKs)
```

### 9.2 Encoding Decision Per Region

| Region | Encoding | Rationale |
|---|---|---|
| **pk_lo (u64 monotonic)** | PCODEC_BLK or FASTLANES_BLK | High compression (5-30x), block index for binary search |
| **pk_lo (u64 semi-random)** | **RAW** | Poor ratio (1.2-1.4x), seek overhead not justified |
| **pk_lo (UUIDv7 lower)** | **RAW** | Mostly random entropy, ~1.1x ratio |
| **pk_hi (u64 tables)** | CONSTANT | All zeros, trivial — ~10 bytes for entire region |
| **pk_hi (UUIDv7)** | PCODEC_BLK or FASTLANES_BLK | Timestamp-structured, 2-5x ratio |
| **weight (i64)** | CONSTANT / TWO_VALUE / EXPLICIT | Structural encoding, not compression. See `plans/weight-column-encoding.md`. Eliminates the region entirely for the common all-+1 case. |
| **null_buf (u64)** | PCODEC or RAW | Sparse (mostly zeros), moderate ratio. Small region — may not justify overhead. See `research/roaring-bitmap-analysis.md` for column-major redesign. |
| **Integer payload** | PCODEC | Best ratio for general integer distributions |
| **Float payload** | PCODEC | FloatMult/FloatQuant modes exploit float structure |
| **String column (16B structs)** | CONSTANT / DICT / DECOMPOSED | See Section 11.4. CONSTANT and dictionary encoding exploit cardinality (same pattern as weight). Struct decomposition enables typed compression per field. |
| **Blob heap** | ZSTD or RAW | Variable-length strings, already deduplicated. Generic compression may help on repetitive text. |

### 9.3 Flush-Time Decision Heuristic

At shard write time, for each region:

```
fn choose_encoding(region_data: &[u8], element_width: usize, region_type: RegionType) -> Encoding {
    match region_type {
        PkLo | PkHi => {
            if is_all_zeros(region_data) {
                return CONSTANT;
            }
            let deltas = sample_deltas(region_data, element_width, 128);
            let max_delta = deltas.iter().max();
            if max_delta < (1u64 << 32) {
                return PCODEC_BLK;  // or FASTLANES_BLK
            }
            return RAW;
        }
        Weight => {
            // Always compresses well (skewed distribution)
            return PCODEC;
        }
        Payload => {
            if element_width <= 8 {
                return PCODEC;
            }
            return RAW;  // u128 columns — handle separately
        }
        StringColumn => {
            // See Section 11.4 for string-specific encoding
            let n_distinct = count_distinct_strings(region_data, row_count);
            if n_distinct == 1 {
                return CONSTANT;
            }
            if n_distinct <= 256 {
                return DICTIONARY;  // 1-byte indices, dict of structs
            }
            // Struct decomposition: split into length/prefix/payload arrays,
            // encode each independently. See Section 11.4.
            return DECOMPOSED;
        }
        BlobHeap => {
            // Try zstd, keep if ratio > 1.2x
            let compressed = zstd::compress(region_data, 3);
            if compressed.len() < region_data.len() * 5 / 6 {
                return ZSTD;
            }
            return RAW;
        }
    }
}
```

---

## 10. Comparison with the OpenZL Approach

| Dimension | OpenZL | FastLanes | Pcodec |
|---|---|---|---|
| **Language** | C/C++ | Pure Rust | Pure Rust |
| **Build complexity** | CMake + C11 compiler | `cargo add fastlanes` | `cargo add pco` |
| **API stability** | v0.1.0, "subject to change" | 0.5.0, stable trait API | **1.0.1** |
| **Type support** | u8-u64 (no u128) | u8-u64 | u8-u64, i8-i64, f16-f64 |
| **Format awareness** | Core feature (SDDL, parsers) | None (typed arrays only) | Mode detection (GCD, float structure) |
| **Training pipeline** | Offline trainer + `.zlc` profiles | Not needed | Automatic (per-chunk) |
| **Random access** | None within frame | `unpack_single` for FOR | Page-level |
| **Ratio on sorted u64** | 2-4x (estimated) | 4-8x (delta+bitpack) | **10-30x** (delta+entropy) |
| **Ratio on weight** | 2-4x (estimated) | 32x (zigzag+2-bit) | **64x** (tANS binning) |
| **Decode speed** | 800-1200 MB/s | **5-10+ GB/s** | 1-4 GB/s |
| **Encode speed** | 200-340 MB/s | **1-5 GB/s** | 0.1-0.7 GB/s |

### 10.1 Where OpenZL Was Better

OpenZL's codec DAG can discover cross-column correlations and apply multi-stage transforms
(transpose + delta + entropy) as a single pipeline with shared training. Neither FastLanes
nor Pcodec does cross-column optimization.

However, gnitz stores each column as an independent region. Cross-column compression would
require fundamentally changing the shard format (interleaving column data). This is a major
architectural change with negative implications for lazy column access and independent column
validation. OpenZL's cross-column capability is not usable without that change.

### 10.2 Where the Rust Crates Are Better

1. **Zero build complexity.** No C/C++ toolchain, no CMake, no vendored submodules.
2. **Stable APIs.** Pcodec is 1.0. FastLanes has a stable trait interface.
3. **No training pipeline.** Pcodec's mode detection is automatic per-chunk. No offline
   training step, no profile management, no retraining on schema change.
4. **Better ratios on typed arrays.** Pcodec's entropy-coded binning consistently beats
   OpenZL's published benchmarks on homogeneous numerical data (where OpenZL's format-parsing
   layer adds nothing).
5. **Better ecosystem integration.** Both crates are used by Vortex (Linux Foundation project,
   DuckDB extension) and other Rust database projects.

---

## 11. Open Questions

### 11.1 Needs Benchmarking on gnitz Data

All compression ratios in this document are projected from published benchmarks on other
datasets. Before committing to a codec, benchmark on actual gnitz shard data:
- Sample shards from representative workloads
- Measure compression ratio, encode speed, and decode speed per column type
- Compare FastLanes, Pcodec, zstd, and raw

A simple benchmark script using both crates on extracted shard regions would take a few hours
to write and provide definitive answers.

### 11.2 Pcodec Minimum Chunk Size

Pcodec's bin optimizer works best with >10K values per chunk. For small shards (<10K rows),
compression ratio may degrade. Need to measure at what shard size the overhead of chunk
metadata dominates. For very small shards, raw or zstd may be better.

Could mitigate by sharing chunk metadata across multiple blocks (one chunk = many pages), but
this ties the encoding decision to a larger scope than individual block index entries.

### 11.3 Patched Encoding for FastLanes

FastLanes' plain bitpacking wastes bits on outliers (one large delta inflates the width for
all 1024 values). A PFOR-style patching layer would fix this. Options:
- Implement ~200-300 lines of Rust (bit-width histogram, cost model, sparse patch storage)
- Use `fastpfor-rs` crate (v0.8.1, but u32 only)
- Accept wider bit widths (simpler, slightly worse ratio)

If choosing Pcodec, this is irrelevant — binning handles outliers inherently.

### 11.4 String Column Encoding

gnitz stores strings as 16-byte German string structs: `[length: u32] [prefix: u32]
[payload: u64]`. For strings ≤ 12 bytes, payload contains inline suffix bytes. For strings
> 12 bytes, payload contains a u64 offset into the per-shard blob heap. Neither FastLanes
nor Pcodec can operate on the interleaved 16-byte struct directly — the three fields have
different types and distributions.

**Opportunity 1: CONSTANT and dictionary encoding** (same pattern as weight column)

At flush time, track distinct string values in a hash set. If n_distinct = 1, store the
string once and eliminate the N × 16 struct array (CONSTANT mode). If n_distinct ≤ 256,
replace the struct array with a dictionary table (D × 16 bytes) + index array (N × 1 byte).

For 100K rows, 50 distinct values: 1.6 MB → ~101 KB (94% savings). This is the highest-value
string optimization and requires no struct decomposition.

For short strings (≤ 12 bytes), the hash key is the 16-byte struct itself — no blob heap
access needed for the cardinality check. For long strings, the hash needs to include the
actual blob content (or its checksum — the writer already computes this for dedup).

**Opportunity 2: Struct decomposition** (enables typed compression)

Split the N × 16 interleaved struct array into three independent arrays:

```
lengths:  [len₀, len₁, ...]     N × 4 bytes — small positive integers
prefixes: [pre₀, pre₁, ...]     N × 4 bytes — first 4 bytes of each string
payloads: [pay₀, pay₁, ...]     N × 8 bytes — inline suffix OR blob offset
```

Each compresses independently with typed codecs:

- **Lengths**: Small positive integers. If all strings have the same length (country codes,
  currency codes, date strings), this is CONSTANT → zero bytes. Otherwise bitpack to actual
  bit-width (e.g., 7 bits for lengths 0-127): 400 KB → ~88 KB for 100K rows.

- **Prefixes**: First 4 bytes of each string. For columns with shared prefixes (URLs, paths,
  structured codes), dictionary or delta encoding is effective. For random strings, these
  have high entropy and compress poorly.

- **Payloads**: Bimodal distribution. For all-short columns (every string ≤ 12 bytes), these
  are suffix bytes — opaque data, compress with generic codec or leave raw. For all-long
  columns (every string > 12 bytes), these are monotonically increasing blob offsets — delta
  + bitpack compresses to nearly nothing (same observation as monotonic PK compression).

For mixed short/long columns, the length array tells the reader which interpretation to use
per row (length > 12 → offset, else → inline suffix).

**Opportunity 3: All-short detection**

If `max_length ≤ 12` for a column (detected at flush time with a single max tracker), the
blob heap contributes zero bytes for that column. The column is fully self-contained in the
struct array. Benefits:

- Reader skips blob heap initialization for this column
- Compaction skips blob relocation for this column
- Comparison never chases a heap pointer — all data is inline

**Opportunity 4: Blob heap compression**

The blob heap is a concatenation of raw string bytes, already deduplicated at flush/compaction
time. Generic compression (zstd, lz4) works here since it's text data. FSST (`fsst-rs` crate,
~2x compression, 1-3 GB/s) is worth evaluating for blob heaps with repetitive string content
(log messages, URLs, structured text).

**Recommended priority:**

1. CONSTANT detection (zero-dependency, same pattern as weight, highest savings/effort ratio)
2. Dictionary encoding for n_distinct ≤ 256 (zero-dependency, high savings)
3. All-short detection (zero-dependency, enables reader/compaction optimizations)
4. Struct decomposition (shard format change, enables typed compression per field)
5. Blob heap compression with zstd (small marginal gain after dedup)

### 11.5 Null Bitmap Encoding

The null bitmap is 8 bytes per row (u64, one bit per nullable column). For a table with 5
nullable columns, 59 of 64 bits are always zero. Options:
- Pcodec or FastLanes on the u64 array (treats it as integer data)
- Custom RLE on the u64 values (most are 0x0000000000000000)
- Leave raw (8 bytes/row is small relative to payload)

Need to measure whether the overhead of encoding/decoding is justified for a region that's
typically <10% of shard size.

### 11.6 Roaring Bitmaps and Null Representation

Evaluated in `research/roaring-bitmap-analysis.md`. Key findings: roaring is not applicable
to the current row-major null layout but is relevant for the Rust column-major
design as an on-disk encoding for sparse null vectors. The bigger win is column-major nulls
themselves (21x storage savings via the ALL_VALID flag for non-nullable columns). Roaring
optimizes for column-major sparse
sets of row IDs. Restructuring to column-major nulls would change the access pattern used by
operators.

### 11.7 Zone Maps as a Byproduct of Block Encoding

#### What zone maps are

Zone maps (also called min/max indices or data skipping indices) store per-block `(min, max)`
metadata for each column. A query with predicate `col > 100` can skip any block where
`max < 100` without decompressing it. DuckDB, Parquet, ClickHouse, and Vortex all use them.

#### Why they're free with block encoding

If columns are split into 1024-value blocks for encoding (FastLanes/Pcodec), the encoding
scan already touches every value to compute parameters (FOR needs the min, bitpacking needs
the max, Pcodec samples the distribution). Persisting `(min, max)` per block adds ~16-24
bytes per block per column — **0.2-0.3% overhead**. There is no additional scan.

#### How gnitz's query model affects the value proposition

gnitz is a DBSP incremental engine. Most operations process small delta batches (10s-1000s
of rows) through circuit operators. Zone maps provide **zero benefit for the incremental
path** — there are no blocks to skip in a small in-memory batch.

Zone maps help on the **full-scan path**, which occurs when:

| Path | When | Zone maps help? |
|---|---|---|
| `_scan_family()` | Client SELECT on materialized view | Yes — if predicate pushed to cursor |
| `SCAN_TRACE` | Circuit reads historical state (init, catch-up, non-linear aggs) | Yes — same |
| Incremental delta processing | Every tick (the common case) | No — batches too small |
| Join probe (PK seek) | Per join match | No — PK binary search, not scan |
| Compaction merge | Background | No — reads all rows anyway |

Neither Feldera nor Materialize (the closest incremental/DBSP peers) has implemented zone
maps. The incremental community does not consider them a priority.

#### But zone maps require predicate pushdown to be useful

gnitz currently has no predicate pushdown to the storage layer. The architecture is:

```
Storage → Cursor (reads everything) → op_filter (evaluates predicate row-by-row)
```

Zone maps require:

```
Storage → Cursor (skips blocks via zone maps) → op_filter (evaluates remaining rows)
```

This means:
1. A `Predicate` interface that the cursor can evaluate against zone map metadata
2. Passing predicates from op_filter / circuit compiler to `create_cursor()`
3. Block-level iteration in ShardCursor (currently row-level)
4. Per-block min/max metadata loaded into ShardHandle or read from the block index

This is a separate project from the compression work. Predicate pushdown changes the cursor
API, the circuit compiler, and the shard reader.

#### What DuckDB's numbers show

DuckDB benchmarks (PR #14313, TPC-H SF100):
- Ordered data: **67-200x speedup** from zone map pushdown
- Random data: ~5% overhead (negligible)

The caveat: DuckDB is an OLAP engine where scans are the primary operation. For gnitz,
scans are occasional. The speedup matters less when scans are 1% of operations.

#### Recommendation: store the metadata now, use it later

**Store zone maps alongside block encoding** — it's free at write time and forward-looking.
When predicate pushdown eventually arrives, the data is already in the shard files.

**The block index format should include max alongside min.** The current design from
Section 8.2 stores only `min_pk` per block (16 bytes per entry). Extending to include max
costs 8 bytes per block per column:

```
BlockIndexEntry (zone-map-ready):
  min_value: u64    (8 bytes)
  max_value: u64    (8 bytes)
  block_offset: u32 (4 bytes)
  block_size: u32   (4 bytes)
  ────────────────
  24 bytes per entry
```

For PK columns, `min_value`/`max_value` enable binary search on the block index (current
use). For payload columns, they enable future predicate pushdown. Same entry format for both.

For 100K rows with 1024-value blocks: 98 blocks × 24 bytes = 2.4 KB per column. For a table
with 10 columns: 24 KB total zone map metadata — negligible vs the column data.

**Do NOT build predicate pushdown as part of the compression work.** It's a separate
initiative requiring circuit compiler changes, cursor API changes, and block-level shard
iteration. The compression work stores the metadata; a future project consumes it.

#### Zone map effectiveness by column type

| Column pattern | Zone map skip rate | Notes |
|---|---|---|
| Sorted (PK, correlated timestamp) | 97-99%+ | Blocks have tight, non-overlapping ranges |
| Clustered (status codes in temporal order) | 50-90% | Values group by time, moderate overlap |
| Random (hash IDs, uncorrelated enums) | ~0% | Every block spans full range |
| Low-cardinality constant | 0% (but CONSTANT encoding eliminates the column) | min=max always |

Zone maps are most valuable for columns correlated with PK sort order. For truly random
columns, they add 24 bytes/block of metadata that never prunes anything — but the overhead
is so small (0.3%) that it's not worth conditionalizing. Store them for all columns
unconditionally.

#### Connection to ExaLogLog sketches

The ExaLogLog plan (`plans/exll-shard-statistics.md`, `research/sketches.md` Opportunity 1)
computes per-column `n_distinct` at flush time. Per-shard `n_distinct(col) / global_n_distinct`
reveals whether zone maps will be effective for a column at query time: a low ratio means the
shard covers a narrow slice of the column's value domain (zone maps prune well); a ratio near
1.0 means zone maps won't prune (every block spans the full range). This signal could be used
to skip zone map checks at query time for columns where they're known to be useless. This is
a query-time optimization (not a storage-time decision) and only matters once predicate
pushdown exists.

### 11.8 Relationship Between Sketch Infrastructure and Encoding Selection

The `research/sketches.md` document proposes ExaLogLog (cardinality) and KLL (quantile)
sketches computed at flush time. These sketches are designed for join ordering, filter
selectivity, and approximate aggregates — not for encoding selection. Investigation of the
"ExaLogLog → encoding selection pipeline" shows the connection is weaker than it initially
appeared.

#### Why ExaLogLog doesn't drive encoding decisions

Encoding selection for small-cardinality cases (CONSTANT, TWO_VALUE, DICTIONARY) requires
**exact** n_distinct values (is it exactly 1? exactly 2? ≤ 256?). ExaLogLog provides
approximate n_distinct with ~1.6% relative error — it cannot distinguish n_distinct=1 from
n_distinct=2 reliably.

The flush-time encoding scan already computes exact small cardinality via a hash set (the
same approach used in the weight column plan's `detect_weight_mode()` and in the string
column's dictionary detection heuristic from Section 11.4). If the hash set exceeds 256
entries, the scan stops tracking and falls through to entropy/delta encoding — the exact
count doesn't matter for that decision.

| Encoding decision | Signal needed | Source |
|---|---|---|
| CONSTANT | n_distinct = 1 (exact) | Flush scan hash set |
| TWO_VALUE | n_distinct = 2 (exact) | Flush scan hash set |
| DICTIONARY | n_distinct ≤ 256 (exact) | Flush scan hash set |
| Delta + bitpack / entropy | "high cardinality" (approximate OK) | Hash set overflow (>256) |
| FOR vs delta | min, max, delta distribution | Flush scan min/max + delta sample |
| Block-compress PK | max delta < 2^32 | Flush scan delta sample |

ExaLogLog is redundant for all of these. The flush scan provides exact signals where exactness
matters and implicit approximate signals (hash set overflow) where approximation suffices.

#### Where sketches DO connect to encoding

**KLL quantiles for PFOR patching**: If the 99th percentile of delta values fits in 10 bits
but the max fits in 40 bits, the optimal bitpack width is 10 with 1% exceptions stored as
patches. KLL from a previous shard of the same table could inform this threshold for the next
shard. However, this is marginal — the flush scan already touches every value, so computing
percentiles directly is O(N) additional work dominated by the scan itself.

**ExaLogLog for zone map query-time behavior**: See Section 11.7 above.

**Shared flush scan**: Both sketch construction and encoding parameter detection scan the
same column data at flush time. They should share the scan — one pass computes ExaLogLog
registers, KLL samples, min/max, exact small cardinality, and encoding parameters
simultaneously. This is an implementation concern, not an algorithmic one.

#### Summary

The sketch infrastructure and the encoding infrastructure are complementary but independent:
- **Sketches** answer statistical questions for the query planner (join cardinality, filter
  selectivity, quantile estimates)
- **Encoding** reduces storage size and I/O based on value distribution properties
- Both are computed at flush time on the same data
- They should share the flush scan for efficiency
- But neither drives the other's decisions

### 11.9 Cross-Shard Encoding / DBSP Delta-over-Delta (Investigated, Not Worthwhile)

The idea: DBSP computes incrementally — each tick produces a small delta batch. Consecutive
shards from the same table contain data from consecutive time periods. Could compression
parameters trained on shard N be reused for shard N+1? Could we exploit the temporal
similarity between consecutive shards?

#### The DBSP runtime makes this redundant

Investigation of the DBSP runtime (vm/runtime.py, vm/interpreter.py, dbsp/ops/reduce.py,
dbsp/ops/distinct.py) reveals that **the LSM structure IS the delta-over-delta mechanism**.

Each tick, DBSP operators produce small delta batches (typically 1-100 rows per changed
group for REDUCE, a few sign-change rows for DISTINCT). These are ingested into trace tables
via INTEGRATE. The trace table's memtable accumulates hundreds of these tiny deltas. At flush
time, consolidation merges them: intermediate retraction/insertion pairs cancel, leaving the
net state.

The resulting shard contains the consolidated state — not a sequence of diffs. There is no
"previous shard" to diff against because the memtable already performed the accumulation.
Cross-shard encoding would require maintaining a reference to a prior shard and computing
structural diffs at read time — duplicating what the LSM already does.

#### Trace table shards are nearly CONSTANT_WEIGHT

The REDUCE operator's trace update pattern produces:
- Per changed group per tick: `[(group_key, old_agg, -1), (group_key, new_agg, +1)]`
- After memtable consolidation: intermediate pairs cancel
- After flush: surviving rows have weight +1 (the current aggregate state)

The DISTINCT operator's history table follows the same pattern: after consolidation, live
entries have weight +1 (present), and cancelled entries are eliminated (weight summed to 0,
dropped by consolidation).

**This means the CONSTANT_WEIGHT optimization from `plans/weight-column-encoding.md` is
exactly the optimization that the DBSP runtime's trace update pattern calls for.** Trace
table shards are the primary beneficiary of CONSTANT mode — their weight column is almost
always all +1 after consolidation.

#### External evidence confirms the conclusion

**RocksDB tried cross-SST zstd dictionaries and rejected them.** They found that keyspace
proximity (within a file) correlated more with data similarity than temporal proximity
(across files). Per-file training was substantially better.

**gnitz's shard lifecycle reinforces this:**

- **L0 shards** (pre-compaction) are temporally correlated but short-lived — compaction
  triggers at 4 shards and merges them into L1.
- **L1+ shards** (post-compaction) are reorganized by FLSM guard key ranges. Temporal
  correlation is destroyed.
- **zstd dictionary benefit window** is <100 KB per input. gnitz's column regions are
  hundreds of KB to MBs — large enough for per-shard encoding to work well.

**No production database** (DuckDB, ClickHouse, Parquet, Pebble/CockroachDB, TiKV, RocksDB)
uses cross-file compression sharing. OceanBase reuses encoding *algorithm choices* (not
dictionaries) from prior blocks as hints — but gnitz's flush-time scan already detects
optimal encoding cheaply.

#### What to do instead

The per-shard optimizations already designed in this document capture the available
compression:

- **CONSTANT_WEIGHT** eliminates the weight region for trace table shards (the primary
  DBSP-specific win)
- **Dictionary encoding** handles low-cardinality payload columns per shard
- **Pcodec/FastLanes** adapts to each shard's value distribution independently
- **Zone maps** store per-block min/max metadata computed during the encoding scan

These are all per-shard, zero-dependency, and capture the patterns that the DBSP runtime
produces. Cross-shard encoding adds operational complexity (shard dependencies break
independent lifecycle) for no additional benefit.

---

## 12. References

### Crates

- FastLanes Rust: https://github.com/spiraldb/fastlanes — `fastlanes` v0.5.0
- Pcodec: https://github.com/pcodec/pcodec — `pco` v1.0.1
- Vortex: https://github.com/spiraldb/vortex — Reference architecture using both
- lz4_flex: https://github.com/PSeitz/lz4_flex — `lz4_flex` v0.13.0
- bitpacking: https://github.com/quickwit-oss/bitpacking — `bitpacking` v0.9.3 (u32 only)
- fsst-rs: https://github.com/spiraldb/fsst — `fsst-rs` v0.5.9

### Papers

- FastLanes (VLDB 2023/2025): "Decoding >100 Billion Integers per Second with Scalar Code"
  https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf
- BtrBlocks (SIGMOD 2023): "Efficient Columnar Compression for Data Lakes"
  https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf
- Pcodec (arXiv 2025): "Better Compression for Numerical Sequences"
  https://arxiv.org/html/2502.06112v2
- Columnar Format Evaluation (VLDB 2024): "An Empirical Evaluation of Columnar Storage Formats"
  https://www.vldb.org/pvldb/vol17/p148-zeng.pdf
- ALP (SIGMOD 2023): "ALP: Adaptive Lossless Floating-Point Compression"
  https://dl.acm.org/doi/10.1145/3626717

### Blog Posts

- DuckDB Lightweight Compression: https://duckdb.org/2022/10/28/lightweight-compression
- DuckDB Parquet Encodings: https://duckdb.org/2025/01/22/parquet-encodings
- DuckDB + Vortex: https://duckdb.org/2026/01/23/duckdb-vortex-extension
- SpiralDB BtrBlocks Analysis: https://spiraldb.com/post/cascading-compression-with-btrblocks
- CedarDB String Compression: https://cedardb.com/blog/string_compression/
- Meta OpenZL Blog: https://engineering.fb.com/2025/10/06/developer-tools/openzl-open-source-format-aware-compression-framework/

### gnitz Internal

- OpenZL feasibility study: `research/openzl-shard-compression.md`
- Shard format: `gnitz/storage/layout.py`
- Cursor architecture: `gnitz/storage/cursor.py`, `gnitz/storage/shard_table.py`
- Rust compaction: `crates/gnitz-engine/src/compact.rs`
- DBSP operators: `gnitz/dbsp/ops/`
- Expression VM: `gnitz/dbsp/expr.py`
