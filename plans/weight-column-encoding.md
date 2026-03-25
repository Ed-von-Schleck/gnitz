# Shard Format v4: Per-Region Encoding + Weight Structural Encoding

No external crate dependencies. Pure Rust implementation in `gnitz-engine`.

---

## Background

### The format change enables all future encoding work

The current shard format (v3) has a fixed 24-byte directory entry per region:
`[offset: u64] [size: u64] [checksum: u64]`. There is no mechanism to indicate that a
region uses a different encoding than raw bytes. Every encoding plan (weight modes,
Pcodec, FastLanes, dictionary encoding, CONSTANT detection) would need its own format hack
without a general encoding byte.

This plan introduces shard format v4 with a per-region encoding byte in the directory entry.
The first two encoding modes deliver immediate value:

- **CONSTANT** (any region): If all values are identical, store the value once. Zero bytes
  for the region data. Applies to weight (all +1), pk_hi for u64 tables (all zeros), null
  bitmap for non-nullable tables (all zeros), and any payload column that happens to be
  constant.

- **TWO_VALUE** (weight-specific): If the weight column has exactly two distinct values
  (+1 and -1), store a bit-vector. 12.5 KB instead of 800 KB for 100K rows.

### What the weight column costs

For a 100K-row shard: 800 KB. This is ~10% of a typical shard (10 columns × 8 bytes × 100K
= 8 MB payload + 1.6 MB PK + 800 KB weight + 800 KB null = 11.2 MB total).

### Verified weight distribution

**Verified invariant**: No shard on disk ever contains a weight==0 row. Every write path
(flush, compaction) enforces this through consolidation + explicit guards.

**Verified distribution**:

| Shard source | Typical weights | Frequency |
|---|---|---|
| Base table, post-compaction, no deletions | All +1 | Most common |
| Trace table (reduce/distinct outputs) | +1 and -1 | Common |
| Base table with accumulated inserts | +1, occasionally >1 | Uncommon |
| Join output tables | Arbitrary non-zero | Rare |

### Other columns that benefit from CONSTANT detection

| Region | When constant | Savings per 100K rows |
|---|---|---|
| pk_hi (u64 PK tables) | Always — all zeros | 800 KB |
| Null bitmap (no nullable columns) | Always — all zeros | 800 KB |
| Weight (post-compaction base/trace) | Almost always — all +1 | 800 KB |
| Any payload column | Rare (status columns, booleans) | Variable |

---

## Design

### Directory entry changes

**v3** (24 bytes):
```
[offset: u64] [size: u64] [checksum: u64]
```

**v4** (32 bytes, padded to 8-byte alignment):
```
[offset: u64] [size: u64] [checksum: u64] [encoding: u8] [reserved: 7 bytes]
```

Encoding byte values:
```
0x00 = RAW           — uncompressed region data (current behavior, default)
0x01 = CONSTANT      — all values identical; constant stored in offset field, size = 0
0x02 = TWO_VALUE     — exactly two distinct values; bit-vector + two constants in region
```

Future values (reserved, not implemented in this plan):
```
0x10 = PCODEC        — Pcodec single-frame encoding
0x11 = PCODEC_BLK    — Pcodec block-compressed with block index
0x20 = FASTLANES_BLK — FastLanes block-compressed with block index
0x30 = ZSTD          — Generic zstd compression
0x40 = DICTIONARY    — Dictionary encoding (index array + dictionary table)
```

### CONSTANT mode (any region)

When all values in a region are identical, the directory entry stores:
```
[constant_value: u64] [size: 0] [checksum: 0] [encoding: 0x01] [reserved]
```

The `offset` field is reused to store the first 8 bytes of the constant value. `size` = 0
indicates no region data on disk. For regions where the element width is ≤ 8 bytes (all
integer types, floats), the constant fits in the offset field directly.

For 16-byte types (u128, string structs): CONSTANT mode stores the first 8 bytes in the
offset field and the second 8 bytes in the checksum field. This is a natural reuse of the
two otherwise-unused fields.

The reader returns the constant for any row index — no memory access, no region data.

### TWO_VALUE mode (weight region only, for now)

When the weight region has exactly two distinct i64 values, the directory entry points to a
region containing:
```
[value_a: i64] [value_b: i64] [bit_vector: ceil(N/8) bytes]
```

Bit `i` = 0 → row `i` has `value_a`. Bit `i` = 1 → row `i` has `value_b`.

The directory entry is standard: `[offset] [size] [checksum] [encoding: 0x02]`.

For 100K rows with +1/-1 weights: 16 + 12,500 = 12,516 bytes (vs 800,000 raw).

### Version and backward compatibility

```
Offset  Size  Field
0       8     Magic (unchanged)
8       8     Version: 4 (was 3)
16      8     Row count (unchanged)
24      8     Directory offset (unchanged)
32      8     Table ID (unchanged)
40      8     XOR8 offset (unchanged)
48      8     XOR8 size (unchanged)
56      8     Reserved (unchanged — NOT used for weight_mode)
```

The header is unchanged beyond the version bump. All encoding information lives in the
per-region directory entries, not in the header.

**v4 readers accept v3 shards**: DIR_ENTRY_SIZE is known from the version. For v3, the reader
uses 24-byte entries and treats all regions as RAW. For v4, the reader uses 32-byte entries
and reads the encoding byte.

**v3 readers reject v4 shards**: Existing version check (`if version != SHARD_VERSION`)
returns an error. This is the existing behavior for unknown versions.

**Compaction naturally upgrades**: Input shards may be v3 or v4. Output shard is always v4.

### Detection at flush/compaction time

For each region, after building the data buffer but before writing:

```rust
fn detect_encoding(data: &[u8], element_width: usize) -> RegionEncoding {
    if data.is_empty() {
        return RegionEncoding::Constant([0u8; 16]);
    }

    // Check if all elements are identical
    let first = &data[..element_width];
    let mut all_same = true;
    let count = data.len() / element_width;
    for i in 1..count {
        if &data[i * element_width..(i + 1) * element_width] != first {
            all_same = false;
            break;
        }
    }
    if all_same {
        let mut constant = [0u8; 16];
        constant[..element_width].copy_from_slice(first);
        return RegionEncoding::Constant(constant);
    }

    RegionEncoding::Raw
}
```

For the weight region specifically, extend with TWO_VALUE detection:

```rust
fn detect_weight_encoding(weights: &[u8]) -> RegionEncoding {
    let count = weights.len() / 8;
    if count == 0 {
        return RegionEncoding::Constant(1i64.to_le_bytes_padded());
    }

    let first = read_i64_le(weights, 0);
    let mut second: Option<i64> = None;
    for i in 1..count {
        let w = read_i64_le(weights, i * 8);
        if w != first {
            match second {
                None => second = Some(w),
                Some(s) if w == s => {}
                Some(_) => return RegionEncoding::Raw, // 3+ distinct values
            }
        }
    }
    match second {
        None => {
            let mut c = [0u8; 16];
            c[..8].copy_from_slice(&first.to_le_bytes());
            RegionEncoding::Constant(c)
        }
        Some(s) => RegionEncoding::TwoValue(first, s),
    }
}
```

Both are single sequential passes over data already in cache from the merge/flush. Cost:
negligible.

### Shard reader changes

`MappedShard::open()` reads the encoding byte from each directory entry:

```rust
for i in 0..num_regions {
    let entry_off = dir_off + i * dir_entry_size;
    let r_off = read_u64_le(data, entry_off) as usize;
    let r_sz = read_u64_le(data, entry_off + 8) as usize;
    let r_cs = read_u64_le(data, entry_off + 16);
    let encoding = if version >= 4 {
        data[entry_off + 24]
    } else {
        0x00 // RAW for v3 shards
    };
    regions.push((r_off, r_sz, r_cs, encoding));
}
```

`get_weight()` dispatches on the weight region's encoding:

```rust
pub fn get_weight(&self, row: usize) -> i64 {
    match self.weight_encoding {
        RAW => read_i64_le(self.data(), self.weight_off + row * 8),
        CONSTANT => self.weight_constant,
        TWO_VALUE => {
            let byte = self.data()[self.weight_bitvec_off + row / 8];
            if (byte >> (row % 8)) & 1 == 0 {
                self.weight_a
            } else {
                self.weight_b
            }
        }
        _ => read_i64_le(self.data(), self.weight_off + row * 8), // unknown → RAW fallback
    }
}
```

For other CONSTANT regions (pk_hi, null_buf), the reader stores the constant and returns it
directly. For RAW regions, existing behavior.

### Shard writer changes

`write_shard()` accepts per-region encoding decisions:

```rust
pub fn write_shard(
    dirfd: i32,
    filename: &CStr,
    table_id: u32,
    row_count: usize,
    regions: &[RegionData],   // NEW: includes encoding + data
    pk_lo: &[u64],
    pk_hi: &[u64],
    durable: bool,
) -> Result<(), i32>
```

Where `RegionData` is:
```rust
enum RegionData<'a> {
    Raw(&'a [u8]),
    Constant([u8; 16]),        // constant value, no region data on disk
    TwoValue {
        value_a: i64,
        value_b: i64,
        bitvec: Vec<u8>,       // ceil(N/8) bytes
    },
}
```

For `Constant` regions: write encoding byte 0x01, store constant in offset field, size = 0.
For `TwoValue` regions: write encoding byte 0x02, build region = [value_a | value_b | bitvec].
For `Raw` regions: write encoding byte 0x00, existing behavior.

### Cursor changes

`ShardCursor::skip_ghosts()` can be eliminated for CONSTANT and TWO_VALUE weight modes:
the constant(s) are non-zero by construction (consolidation guarantees no weight==0 rows, and
CONSTANT/TWO_VALUE encode only the non-zero values). Set a `has_ghosts: bool` flag at shard
open time and short-circuit skip_ghosts() when false.

### Compaction changes

The merge loop calls `get_weight()` which dispatches on encoding mode transparently. No
merge loop changes needed.

The `fast_concatenate` path for disjoint CONSTANT+1 shards is deferred (see "What this plan
does NOT cover").

---

## Savings

### Disk (per 100K-row shard)

| Region | v3 (raw) | v4 CONSTANT | v4 TWO_VALUE |
|---|---|---|---|
| Weight (all +1) | 800 KB | **0 bytes** | N/A |
| Weight (+1/-1) | 800 KB | N/A | **12.5 KB** |
| pk_hi (u64 table) | 800 KB | **0 bytes** | N/A |
| Null bitmap (no nullable cols) | 800 KB | **0 bytes** | N/A |
| Directory overhead | 0 | +8 bytes/region | +8 bytes/region |

For a u64 PK table with no nullable columns and all +1 weights:
2.4 MB saved per 100K-row shard (pk_hi + weight + null_buf). Directory overhead: ~48 bytes
(6 regions × 8 bytes). Net savings: **~2.4 MB per shard**.

### CPU

| Operation | RAW | CONSTANT | TWO_VALUE |
|---|---|---|---|
| `get_weight(row)` | 8-byte mmap read | Return constant | 1-byte read + bit test |
| `skip_ghosts()` | Sequential weight reads | No-op | No-op |
| Flush-time detection | N/A | Single pass (free) | Single pass (free) |

---

## Implementation

### Step 1: Layout constants

Update `layout.rs`:
```rust
pub const SHARD_VERSION: u64 = 4;
pub const DIR_ENTRY_SIZE: usize = 32;  // was 24

pub const ENCODING_RAW: u8 = 0x00;
pub const ENCODING_CONSTANT: u8 = 0x01;
pub const ENCODING_TWO_VALUE: u8 = 0x02;
```

### Step 2: Shard writer

In `shard_file.rs`:

1. Add `RegionData` enum and `detect_encoding()` / `detect_weight_encoding()` functions
2. Update `write_shard()` to accept `RegionData` instead of `&[u8]` per region
3. Write 32-byte directory entries with encoding byte
4. For CONSTANT: store constant in offset field, skip region data
5. For TWO_VALUE: build [value_a | value_b | bitvec] as region data
6. For RAW: existing behavior

### Step 3: Shard reader

In `shard_reader.rs`:

1. `MappedShard::open()`: detect version, use 24-byte entries for v3, 32-byte for v4
2. Read encoding byte per region
3. For CONSTANT regions: store constant in struct, don't record region offset
4. For TWO_VALUE weight: store value_a, value_b, bitvec offset
5. `get_weight()`: dispatch on encoding
6. `get_null_word()`: dispatch on encoding (CONSTANT returns 0 for non-nullable tables)
7. `get_pk_hi()`: dispatch on encoding (CONSTANT returns 0 for u64 tables)

### Step 4: Callers (flush + compaction)

Update all `write_shard()` callers to detect encoding per region:

1. `memtable.rs` flush path: for each region buffer, call `detect_encoding()`. For weight
   buffer, call `detect_weight_encoding()`. Pass `RegionData` to `write_shard()`.
2. `compact.rs` finalize: same detection on ShardWriter's output buffers.

### Step 5: Ghost skipping

In cursor code: check weight encoding at shard open time. If CONSTANT or TWO_VALUE,
set `has_ghosts = false`. `skip_ghosts()` returns immediately when `!has_ghosts`.

### Step 6: Tests

1. **CONSTANT detection**: write shard with all-same values per region → read back → verify
   encoding byte = 0x01, get_weight/get_pk_hi/get_null_word return constant.
2. **TWO_VALUE detection**: write shard with +1/-1 weights → read back → verify per-row
   values via get_weight().
3. **RAW fallback**: write shard with 3+ distinct weights → verify encoding = 0x00, existing
   behavior.
4. **Backward compat**: create a v3 shard file (24-byte dir entries) → open with v4 reader →
   verify all regions treated as RAW.
5. **Compaction roundtrip**: compact two CONSTANT+1 shards → verify output is CONSTANT+1.
6. **Compaction mixed**: compact CONSTANT+1 with TWO_VALUE shard → verify correct merge,
   output weight values correct.
7. **Ghost skipping no-op**: open CONSTANT weight shard, create cursor, verify skip_ghosts
   doesn't read weight column.
8. **E2E**: full circuit with inserts/deletes → verify shard encoding modes are detected
   correctly across flush + compaction cycles.
9. **pk_hi CONSTANT for u64 table**: write shard with u64 PKs → verify pk_hi region is
   CONSTANT with value 0.
10. **Null bitmap CONSTANT for non-nullable table**: write shard with all non-null rows →
    verify null_buf region is CONSTANT with value 0.

### Step 7: Measure

- Shard file sizes before/after for a representative workload
- Flush throughput (detection overhead should be negligible)
- Cursor iteration speed on CONSTANT weight shards (no weight reads)

---

## What this plan does NOT cover

- **Fast concatenation compaction**: When all input shards are CONSTANT weight +1 with
  disjoint PK ranges, compaction can skip the merge loop entirely. Deferred — the merge loop
  works correctly with all encoding modes.

- **Pcodec/FastLanes/zstd encoding**: Future encoding modes that use the same per-region
  encoding byte. Each is a separate plan building on the v4 format established here.

- **Dictionary encoding**: The encoding byte 0x40 is reserved. Implementation requires
  flush-time cardinality tracking (hash set). Separate plan.

- **Zone maps in block index**: The v4 directory entry has 7 reserved bytes. Per-block
  min/max metadata would be stored in the block index (within the region data), not in the
  directory entry. Separate concern.

- **String struct decomposition**: Splitting the 16-byte German string struct into
  length/prefix/payload arrays for typed encoding. Separate format change.

- **Column-major null bitmaps**: The null_buf CONSTANT detection in this plan saves space
  for non-nullable tables. A full column-major redesign is a separate, larger project
  (see `research/roaring-bitmap-analysis.md`).
