# FastLanes integer packing: FoR + bit-packed payload columns in compacted shards

## Problem

Integer payload columns are stored raw in shard files — 8 bytes per row for
an I64 that is usually a small value, a re-keyed ex-PK, or a
narrow-cardinality attribute. This is the dominant disk cost of the engine's
DBSP state: every stateful operator keeps a full integral (`_int_` join
traces of both inputs, `_hist_` distinct history, `_reduce_in_` aggregation
history), each payload-widened by `map_reindex` (the original PK columns move
into payload as integers), each duplicated per view. After every checkpoint
the RAM tier is dropped (`persist_l0_run` clears `in_memory_l0`,
`storage/lsm/table/flush.rs:277-278`), so shards are the steady-state read
substrate for every tick — but the per-tick trace access is monotone
galloping merges and sequential drains, which tolerate materialized-on-open
columns; only the byte-compared OPK PK region needs in-place random access,
and it is not touched here.

This plan encodes eligible integer payload regions with frame-of-reference +
FastLanes bit-packing in 1024-row blocks, on **compaction outputs only**.
Spill/checkpoint L0 shards stay plain — they sit on the mid-epoch latency
path and are merged away once a table accrues `L0_COMPACT_THRESHOLD = 4` L0
runs (`storage/lsm/shard_index/mod.rs:35`; `compact_if_needed` runs after
every spill, `table/flush.rs:400-407`, and on every per-epoch
`create_cursor_compacting`, `table/mod.rs:420-424`). The savings are
therefore **disk footprint and read/page-fault volume of the compacted bulk
(L1/L2 plus compacted L0), amortized post-compaction**; checkpoint-write
bandwidth itself is unchanged, and up to 4 uncompacted L0 runs per partition
remain raw at any time. Reads materialize an encoded region once per shard
open via a safe `OnceCell`-backed decode; all consumers are unchanged.

## Load-bearing facts

### Precondition — the shard-codec-layer restructure (not yet in the tree)

This plan is **blocked on** a preceding behavior-neutral restructure of the
shard writer and must not start before it lands. That restructure delivers,
and this plan assumes as its starting state: a schema-aware writer —
`write_shard_streaming(dirfd, basename, table_id, row_count, regions,
schema, durable, flags)` (with `Batch::write_as_shard_with_flags` forwarding
a `schema: &SchemaDescriptor` param from its compaction call sites and
`persist_l0_run` passing `&self.schema`) — classifying each region by a
`RegionRole` (Pk/Weight/NullBmp/Payload{col}/Blob) with one selection
function owning the role → encoding mapping; and baseline microbenches
`shard_merge_scan_bench` / `shard_point_probe_bench` in
`storage/lsm/read_cursor`. On-disk format and reader are untouched by that
restructure: `SHARD_VERSION == 7`, encodings 0x00/0x01/0x02 =
Raw/Constant/TwoValue. Everything in this subsection describes that
delivered state; all other citations in this plan are against the current
tree.

### Current tree

- Payload regions are read through exactly four surfaces on `MappedShard`
  (verified exhaustive: `col_regions` is read only in
  `shard_reader/access.rs` and `open.rs`): `get_col_ptr` (row-at-a-time via
  `ColumnarSource`, `access.rs:115-131`), `col_ptr_by_logical`
  (`access.rs:137-172`, serving `ReadCursor::col_ptr` for join/reduce reads,
  `read_cursor/mod.rs:757-783`, and the point-get `found_row` path,
  `table/mod.rs:181`), `to_unified` (`access.rs:353-407`, serving compaction
  scatter `compact/merge.rs:102` and cursor drains
  `read_cursor/output.rs:322-330`), and `slice_to_owned_batch`
  (`access.rs:244-336`, the single-source drain fast path,
  `read_cursor/output.rs:212-217`). All four can serve from an owned decoded
  buffer with a stable address; none require the bytes to live in the mmap.
- `to_unified` and `slice_to_owned_batch` touch **every** payload column
  (`access.rs:385-397`, `:310-314`) — a drain or compaction over a packed
  shard decodes all of its payload regions, not just "hot" ones.
- Expression evaluation never reads shard memory — `eval_batch` runs only
  over owned delta batches in VM registers (`expr/batch.rs:1-16`,
  `query/vm/exec.rs:178-236`).
- `MappedShard` is `Rc`-owned, single-threaded (`read_cursor/source.rs:25`;
  no `Arc<MappedShard>`, no thread spawns in compact/flush paths);
  `OnceCell<Box<[u8]>>` decode is safe Rust and its content address is
  stable for the shard's lifetime, matching `to_unified`'s existing "caller
  keeps `self` alive" pointer contract (`access.rs:346-348`).
- Compaction writes every long-lived shard: `compact_routed`
  (`storage/lsm/compact/merge.rs:62-144`) via
  `Batch::write_as_shard_with_flags` (`merge.rs:137`, `compact/mod.rs:1158`,
  `:1324`). Compaction opens up to `L1_TARGET_FILES = 16` inputs at once
  (`shard_index/mod.rs:34-38`) and `to_unified`s all of them
  (`merge.rs:102`). Spill/checkpoint L0 shards come only from
  `Table::persist_l0_run` (`table/flush.rs:241-280`).
- Point probes (`Table::has_pk_bytes`, `table/mod.rs:496-508`) read PK bytes
  and weights only (`scan_shards_for_pk_bytes` with `need_candidates =
  false`) — no payload region, no decode. The point-**get**/retract path
  (`found_row`, `table/mod.rs:665-669`, then `get_col_ptr` at
  `table/mod.rs:181`) does read single payload cells from a shard row.
- Directory entries carry per-region `{offset, size, xxh3, encoding u8}`;
  checksums cover the **encoded** bytes (`shard_file.rs:339-364`). Region
  starts are 64-byte aligned (`ALIGNMENT = 64`, `layout.rs:7`; `align64`,
  `shard_file.rs:21-23`).
- Eligible type codes (`gnitz-wire/src/types.rs:5-18`): `is_fixed_int`
  (`types.rs:259`) admits exactly U8=1, I8=2, U16=3, I16=4, U32=5, I32=6,
  U64=8, I64=9 (strides 1/2/4/8) — floats (F32=7, F64=10) and all 16-byte
  codes (STRING=11, U128=12, UUID=13, BLOB=14, I128=15) are outside it, so
  `is_fixed_int(col.type_code)` alone is the eligibility predicate.
- NULL cells occupy normal payload bytes, zeroed by writers
  (`fill_col_zero`, e.g. `ops/join/rowwrite.rs:29`); comparisons consult the
  null word first (`storage/repr/columnar.rs:47-61`). Packing is bit-exact
  on whatever bytes are stored, so correctness needs no special casing — but
  a NULL's zero cell does join the block's `[min, max]`, so a
  large-magnitude column with occasional NULLs packs at the width its range
  spans down to zero (a ratio cost, not a correctness one).
- The engine's on-disk regions are little-endian throughout (§6 region
  convention); this plan's packed payloads add one more LE-only surface
  (below). `fsync`/checksum plumbing is unaffected.
- The `fastlanes` crate (spiraldb), v0.5.2, Apache-2.0, `no_std`,
  deps: `const_for`, `core_detect`, `num-traits`, `paste`, `seq-macro`.
  Exercised in production by Vortex (`vortex-fastlanes`). API facts
  (docs.rs/fastlanes):
  - `BitPacking` implemented for `u8/u16/u32/u64` **only** (no u128, no
    signed types). Blocks are exactly **1024 elements**; a packed block is
    `1024 * W` bits = **`128 * W` bytes** for every element type.
  - Runtime-width entry points (the intended dispatch — no hand-written
    match over widths):
    `unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self])`,
    `unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self])`.
    Preconditions (debug-checked only): exact slice lengths
    (1024 unpacked, `1024*W/T::BITS` packed elements), `width <= T::BITS`.
  - `FoR` trait: `unsafe fn unchecked_unfor_pack(width, input, reference: Self,
    output)` — fused unpack + **wrapping add** of `reference`.
  - No partial-block API: the caller pads the tail block to 1024 and tracks
    the true count (the Vortex convention).
  - MSRV 1.91 (repo policy: stable Rust, nothing version-pinned — satisfied).

## Committed design

### Encoding: `ENCODING_PACKED = 0x03`, payload regions only

Eligibility: `RegionRole::Payload { col }` with
`is_fixed_int(col.type_code)`, `row_count > 0`, and the writer's
`pack_ints` policy bit set (below). Selection order per payload region:
Constant first (unchanged), then Packed, else Raw. Packed is kept only if it
is strictly smaller than raw (`encoded_size < count * stride`); otherwise
Raw. Deterministic — no sampling, no tuning knob.

**Region layout** (all offsets relative to the region start, which is
64-byte aligned):

```
refs:    nb × u64 LE      — per-block FoR reference (native value's bit
                            pattern zero-extended to 64 bits)
widths:  nb × u8          — per-block packed bit width W ∈ [0, 8*stride]
pad:     to 8-byte alignment
blocks:  concatenated packed payloads, block i occupying 128 * W_i bytes
```

`nb = count.div_ceil(1024)`. Block payload offsets are the running prefix
sum of `128 * W_i` — recomputed during the (single, sequential) decode, never
stored. `W_i == 0` (block constant at `ref_i`) contributes zero payload
bytes. Every block start is 8-byte aligned (region start 64-aligned; refs
`8·nb` + widths `nb` padded to 8; each block length `128·W` is a multiple of
8), so the packed bytes can be viewed as `&[u64]`/`&[u32]`/… without
unaligned access.

**Encode** (writer phase 1, per eligible region; monomorphized over
`T ∈ {u8, u16, u32, u64}` selected by `col.size()`):

```rust
// Per block of ≤1024 values:
//  1. min/max in the column's order — signed compare for signed type codes,
//     unsigned otherwise (order only affects the offset range, never
//     correctness: offsets are wrapping, decode wraps back).
//  2. offsets[j] = v[j].wrapping_sub(min)  (tail padded with `min` → offset 0)
//  3. W = T::BITS - max_offset.leading_zeros()
//  4. if W > 0 { unsafe { T::unchecked_pack(W, &offsets, &mut packed[..1024*W/T::BITS]) } }
```

The encoder computes offsets itself (a trivially vectorizable
`wrapping_sub` loop) rather than relying on `for_pack`'s subtract semantics;
decode uses the fused `unchecked_unfor_pack` whose wrapping-add exactly
inverts it. Signed types (`I8/I16/I32/I64`) take `min` in signed order and
carry its bit pattern as the reference: `v.wrapping_sub(min)` as unsigned
then spans exactly `[0, smax − smin]`, so mixed-sign blocks pack to the true
signed range instead of degenerating to full width. (Extremes are covered:
`min = MIN, max = MAX` gives `max_offset = u64::MAX` → `W = 64` → the
encoded region is not smaller than raw → Raw by the size rule.)

The encoded region bytes are assembled into an owned buffer (like today's
TwoValue path, `shard_file.rs:332,356-362`), checksummed, and pwritten;
`actual_sizes[i]` is the encoded size.

**Policy** — `write_shard_streaming` gains a `pack_ints: bool` parameter
(threaded like `flags`): `true` from `Batch::write_as_shard_with_flags`'
compaction callers (`compact/merge.rs:137`, `compact/mod.rs:1158`, `:1324`),
`false` from `persist_l0_run` and from `write_as_shard` (test-only). L0
spill/checkpoint shards therefore stay plain; every compaction output packs.
Rationale: spills run mid-epoch on the latency path and their data is merged
away at the L0 threshold; compaction is the amortized rewrite that produces
the shards the per-tick merges live on. Consequence stated plainly:
checkpoint write bandwidth does not shrink, and a table's ≤ 4 pending L0
runs are always raw — the packing ratio applies to the compacted bulk.

### Decode: `ScalarRegion::Packed`, materialized once per shard open

```rust
#[derive(Clone)]
pub(crate) enum ScalarRegion {
    Raw { offset: usize, size: usize },
    Constant { value: [u8; 16], offset: usize },
    /// FoR + FastLanes-packed integer payload region. `decoded` lazily holds
    /// the full `count × elem_width` little-endian image; every accessor
    /// serves from it. Populated at most once per shard open; the content
    /// address is stable for the shard's lifetime.
    Packed {
        offset: usize,
        size: usize,
        elem_width: usize,
        decoded: OnceCell<Box<[u8]>>,
    },
}
```

`MappedShard` gains one private method, the single decode site:

```rust
fn packed_bytes(&self, region: &ScalarRegion) -> &[u8] {
    let ScalarRegion::Packed { offset, size, elem_width, decoded } = region else { unreachable!() };
    decoded.get_or_init(|| {
        // Sequential walk: refs[], widths[], prefix-sum offsets; for each
        // block: W == 0 → splat ref; else unchecked_unfor_pack(W, block,
        // ref as T, out_block). Tail block decodes into a stack [T; 1024]
        // scratch, then copies count % 1024 elements.
        decode_packed_region(&self.data()[*offset..*offset + *size], self.count, *elem_width)
    })
}
```

Accessor arms:

- `get_col_ptr` → `&self.packed_bytes(region)[row * cs..][..cs]`.
- `col_ptr_by_logical` → `self.packed_bytes(region).as_ptr().add(row * col_size)`
  (bounds-checked like the Raw arm).
- `to_unified` → `ColPtr { base: self.packed_bytes(region).as_ptr(), stride: cs }`.
- `slice_to_owned_batch`'s `expand_scalar` → copy from
  `packed_bytes(region)` at `start * stride` (one memcpy, same as Raw).

PK stays `Raw`/`Constant` only (Packed on region 0 rejected at open — the
memcmp binary search, gallop, and XOR8 build read OPK bytes in place and are
untouched). Weight and null regions are untouched.

**Unsafe inventory.** Three `fastlanes` calls (`unchecked_pack`,
`unchecked_unpack` for the tail scratch, `unchecked_unfor_pack`) plus the
`&[u8]` ↔ `&[T]` reinterpretations feeding them (on encode: over owned,
naturally-aligned buffers; on decode: over the mmap'd packed blocks, whose
8-byte alignment the region layout guarantees, asserted in debug). The
reinterpret is little-endian-only — packed words are stored as native-`T`
LE, consistent with every other region in the format. There is no existing
typed-slice-over-mmap precedent in the tree, so each cast site carries a
`// SAFETY:` comment stating the alignment/length invariant it relies on;
no `bytemuck` dependency (two call sites do not justify one).

**Open-time validation** (`build_scalar_region`'s Packed arm, payload
entries only; Packed on pk/null/weight/blob entries → `InvalidShard`):
region size ≥ `9 * nb` padded; per-block `W_i <= 8 * elem_width`;
`descriptor_end + Σ 128·W_i == size`. After validation, decode cannot fail —
`decode_packed_region` is infallible (pure arithmetic on validated bounds),
keeping `to_unified` infallible (`read_cursor/source.rs:102-114`).

**Memory accounting (accepted trade, stated honestly).** A decoded region
lives as long as its `MappedShard` is open, and any drain, scan, or
compaction over the shard decodes **all** of its packed payload columns
(`to_unified` builds every column's `ColPtr`). Peak RSS for a scanned packed
shard is therefore the decoded raw-size heap **plus** whatever packed file
pages the decode faulted in — more than today's raw-only page cache for the
same shard, with the difference that the packed pages are reclaimable page
cache while the decoded heap is pinned until shard close. Compaction
transiently decodes all payload columns of up to 16 input shards at once
(`merge.rs:102`), on top of the output batch it already materializes today.
The point-get path (`found_row` → `get_col_ptr`) pays a whole-region decode
for its first single-cell read on a shard — a first-touch cliff bounded by
one decode per column per shard open. What shrinks: disk footprint,
compacted-read page-fault volume (boot resume, backfills, cold scans), and
the bytes a *probe-only* shard occupies (probes decode nothing). What does
not shrink: steady-state RSS of actively scanned tables, checkpoint write
bandwidth.

### Format: version 8

`SHARD_VERSION` 7 → 8 (`layout.rs`); `ENCODING_PACKED = 0x03` added to
`layout.rs` and the open-time known-encoding check (`open.rs:71-73`). The
bump is a hard break with no migration: on an un-wiped data dir the reader
fails `MappedShard::open` with `InvalidVersion` at boot/resume — base-table
shards have no self-healing gate, so the failure mode is a boot error, not
a rebuild; data dirs are wiped across format bumps (pre-alpha convention).
`STATE_FORMAT` (`storage/lsm/manifest.rs:45`, the resume-topology knob)
deliberately stays 1 — it
gates resume-vs-rebuild of *views*, and bumping it cannot save an un-wiped
dir whose base shards are unreadable anyway. Stale version strings updated
in the same commit: the `"must write V7"` assert message
(`shard_file.rs:586`), the "Old v7 shards" comment (`open.rs:219-221`), the
"V7 format decision" comment (`storage/lsm/manifest.rs:78`).

### Dependency

`fastlanes = "0.5"` in `crates/gnitz-engine/Cargo.toml`.

### Explicitly out of scope

- Floats (F32/F64) and 16-byte types (U128/UUID/I128, German-string structs)
  — stay Raw/Constant.
- The PK region, weight, null, and blob regions.
- Packing spill/checkpoint L0 shards (`pack_ints = false` there by design,
  not deferral).
- Delta/RLE/dictionary encodings, per-block min-key indexes, and any
  block-granular (partial-region) decode: the committed decode unit is the
  whole region, once per shard open.
- The WAL/SAL block format (client wire surface).

## Sequencing

- [ ] **Codec module.** `storage/lsm/pack.rs`: `encode_packed_region` /
      `decode_packed_region` / `packed_encoded_size`, monomorphized over
      u8/u16/u32/u64 with signed-min handling by type code. Exhaustive unit
      tests (below) including `i64::MIN/MAX` mixes and W=0 blocks. Add the
      `fastlanes` dependency. `make verify`.
- [ ] **Writer + reader + v8, one commit** (the version bump makes a split
      unbuildable: after the bump every shard the old reader arm opens would
      fail, so writer and reader land together). `pack_ints` parameter
      threaded through `write_shard_streaming{,_inner}` /
      `write_as_shard_with_flags`; Packed selection for eligible payload
      regions; compaction callers pass `true`, `persist_l0_run` `false`;
      `ScalarRegion::Packed`, `packed_bytes`, the four accessor arms,
      open-time validation and role restrictions; `SHARD_VERSION = 8`,
      `ENCODING_PACKED = 0x03`, stale-V7 string updates. Roundtrip +
      rejection tests in the same commit. `make verify` + `make e2e` (W=4).
- [ ] **Benches.** Extend `shard_merge_scan_bench` /
      `shard_point_probe_bench` with packed-shard variants; add
      `packed_decode_bench` (region decode throughput, values/s) and a
      packed-input compaction variant (compact 4 packed vs 4 raw inputs —
      pins the decode-inputs + re-encode-output cost). Run `make bench`
      before/after and compare `summary.json`; judge merge-scan deltas by
      the microbench (e2e has ±3-4% noise); record the compacted-shard size
      reduction on the bench dataset in the commit message. `make verify` +
      `make e2e`.

## Testing

- Codec unit tests: roundtrip over seeded-random blocks for every eligible
  type code × widths {sparse small values, dense full-range, all-equal
  (W=0), mixed-sign for signed types, `MIN`/`MAX` extremes, exact-1024 and
  ragged tails (1, 1023, 1025 rows)}; encoded-size formula matches
  `packed_encoded_size`; byte-exact reproduction of input (bit-pattern
  equality, including whatever bytes NULL cells carried); NULL-zero
  widening case (large values + zeros packs at full-span width, still
  roundtrips).
- Reader tests: packed shard roundtrip through all four surfaces —
  `get_col_ptr` per row, `col_ptr_by_logical` (including a
  `found_row`-shaped single-cell read), a multi-source cursor drain
  (`to_unified` scatter) against a Raw-shard control producing
  byte-identical batches, and `slice_to_owned_batch` slices;
  `packed_bytes` pointer stable across calls; forged Packed encoding on
  pk/weight/null/blob dir entries → `InvalidShard`; truncated descriptor /
  oversized `W` / size-sum mismatch → `InvalidShard`; checksum over encoded
  bytes still caught by `validate_checksums` on a corrupted packed region.
- Compaction integration: ingest mixed-type rows (incl. nullable ints and
  strings) into a `Table`, spill, compact; assert compaction outputs carry
  `ENCODING_PACKED` on eligible columns (read the dir entry), L0 spill
  shards do not; cursor reads and point probes over the packed table match
  a RAM-only control with weight-verified equality; re-compaction of packed
  inputs (decode → merge → re-encode) preserves content.
- Full `make verify` + `make e2e` (W=4) after every checkbox — the existing
  ~1100-test e2e suite over compacted views is the behavioral regression
  net, since packing is transparent above `MappedShard`.
