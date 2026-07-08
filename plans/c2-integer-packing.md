# Frame-of-reference integer packing: byte-width-truncated payload columns in compacted shards

## Problem

Integer payload columns are stored raw in shard files — 8 bytes per row for an
I64 that is usually a small value, a re-keyed ex-PK, or a narrow-cardinality
attribute. This is the dominant disk cost of the engine's DBSP state: every
stateful operator keeps a full integral (`_int_` join traces of both inputs,
`_hist_` distinct history, `_reduce_in_` aggregation history), each
payload-widened by `map_reindex` (the original PK columns move into payload as
integers), each duplicated per view. Payload regions today are encoded only as
Constant (all rows equal) or Raw (`detect_encoding`, `shard_file.rs:117`;
TwoValue is weight-only) — so a payload column of a handful of small distinct
values pays the full 8 bytes/row.

After every checkpoint the RAM tier is dropped (`persist_l0_run` clears
`in_memory_l0`), so shards are the steady-state read substrate for every tick.
Per-tick trace access is monotone galloping merges and sequential drains, which
tolerate materialized-on-open columns; only the byte-compared OPK PK region
needs in-place random access, and it is not touched here.

This plan adds a **frame-of-reference (FoR) + byte-width truncation** encoding
for eligible integer payload regions, on **compaction outputs only**.
Spill/checkpoint L0 shards stay plain — they sit on the mid-epoch latency path
and are merged away once a table accrues `L0_COMPACT_THRESHOLD = 4` L0 runs. The
savings are therefore **disk footprint and read/page-fault volume of the
compacted bulk (L1/L2 plus compacted L0), amortized post-compaction**;
checkpoint-write bandwidth is unchanged, and up to 4 uncompacted L0 runs per
partition remain raw. Reads materialize an encoded region once per shard open
via a safe `OnceCell`-backed decode; all consumers are unchanged.

### Why byte-width truncation, not bit-packing

The committed encoding stores, per region, one 8-byte reference (the region's
minimum) and each row's `value − min` truncated to the fewest whole bytes that
hold the region's offset range. Decode is a widening loop.

This deliberately trades sub-byte compression ratio for radical simplicity over
a bit-packing library (FastLanes / FoR-bitpacking): **no new dependency, no SIMD
`unsafe`, no 1024-row block bookkeeping, no per-block descriptor, byte-aligned
throughout, portable little-endian.** It captures the win the Problem section
names — wide integer columns holding small or narrow-range values, 8 → 1–2
bytes — because that win is at *byte* granularity already. Bit-packing's extra
leverage (sub-byte widths; per-1024-block adaptivity) is declined on purpose:
the target columns are arbitrary-order re-keyed ex-PKs and attributes with no
guaranteed local clustering for per-block widths to exploit, and Constant
already claims the all-equal case. A byte-truncated row is also independently
decodable, so a single corrupt byte damages one row's value — unlike
bit-lane-interleaved packing, where it can smear across a whole block on the
checksum-skipped read path (most production opens pass `validate_checksums =
false`).

## Load-bearing facts (current tree)

The schema-aware shard writer is already in the tree (delivered by the shipped
codec-layer refactor, commit `447206e8`):

- `Batch::write_as_shard(path, table_id, schema, flags)` (`batch_wire.rs:28`)
  forwards `&SchemaDescriptor` into `write_shard_streaming(dirfd, basename,
  table_id, row_count, regions, schema, durable, flags)` (`shard_file.rs:221`),
  which sizes regions from the schema via `strides_from_schema`. Region roles
  are **index-based**, not a `RegionRole` enum (none exists): `REG_PK = 0`,
  `REG_WEIGHT = 1`, `REG_NULL_BMP = 2`, `REG_PAYLOAD_START = 3`
  (`storage/repr/batch.rs:44-47`); the trailing index `nr` is the blob region.
  Encoding selection is inline in `write_shard_streaming_inner`'s Phase-1 loop
  (`shard_file.rs:301-305`): `detect_weight_encoding` for `REG_WEIGHT`, else
  `detect_encoding`.
- `SHARD_VERSION == 7` (`layout.rs:4`); encodings `0x00`/`0x01`/`0x02` =
  Raw/Constant/TwoValue (`layout.rs:17-19`). Directory entries carry per-region
  `{offset, size, xxh3, encoding u8}`; checksums cover the **encoded** bytes and
  are validated in `open()` before any region view is built. Region starts are
  64-byte aligned.
- Payload regions are read through exactly four `MappedShard` surfaces (verified
  exhaustive — `col_regions` / `ScalarRegion` are referenced only in
  `shard_reader/{open,access}.rs`): `get_col_ptr` (row-at-a-time),
  `col_ptr_by_logical` (join/reduce reads + point-get `found_row`), `to_unified`
  (compaction scatter + cursor drains), `slice_to_owned_batch` (single-source
  drain fast path). All four can serve from an owned decoded buffer with a
  stable address; none require the bytes to live in the mmap. `to_unified` and
  `slice_to_owned_batch` touch **every** payload column, so a drain or
  compaction over a packed shard decodes all of its packed payload regions.
- Point probes (`Table::has_pk_bytes`) read PK bytes and weights only — no
  payload region, no decode. The point-**get** path (`found_row` →
  `get_col_ptr`) does read single payload cells from a shard row.
- Eligible type codes: `is_fixed_int` (`gnitz-wire/src/types.rs`) admits
  U8/I8/U16/I16/U32/I32/U64/I64 (strides 1/2/4/8); floats (F32/F64) and all
  16-byte codes are excluded, so `is_fixed_int(col.type_code)` alone is the
  eligibility predicate. `col.size()` is pinned to the type code by a
  compile-time round-trip assertion, so stride and eligibility can never
  disagree.
- NULL cells occupy normal payload bytes, zeroed by writers; `compare_rows`
  consults the null bitmap and short-circuits *before* reading a NULL cell's
  bytes. FoR is an exact wrapping-sub/add pair, so a zeroed NULL cell round-trips
  to zero exactly — packing needs no NULL special-casing (a NULL's zero does join
  the region's `[min, max]`, a ratio cost only, not a correctness one).
- `MappedShard` is `Rc`-owned and single-threaded; an `OnceCell`-backed decode
  is safe Rust with a lifetime-stable content address, matching `to_unified`'s
  "caller keeps `self` alive" pointer contract. Compaction writes every
  long-lived shard (`compact_routed` via `write_as_shard`, `compact/merge.rs:137`,
  `compact/mod.rs:1158`, `:1324`), opening up to `L1_TARGET_FILES = 16` inputs at
  once; L0 spill shards come only from `persist_l0_run`.

## Committed design

### Encoding: `ENCODING_FOR = 0x03`, integer payload regions only

Eligibility: a payload region (`REG_PAYLOAD_START ≤ i < nr`) whose column is
`is_fixed_int`, with `row_count > 0` and the writer's `pack_ints` policy bit set
(below). Selection order per payload region: Constant first (unchanged), then
FoR, else Raw.

**Region layout** (region start is 64-byte aligned):

```
ref:    8 bytes LE          — frame reference = region min's bit pattern, zero-extended to u64
values: count × bw bytes LE — each row's (value − min) truncated to its low bw bytes
```

`bw` is any width in `[1, stride)`; the encoded size is `8 + count·bw`, byte-
aligned with no interior padding. (U8/I8 columns, stride 1, can never pack.)

**Encode** (one pass over `widen(v)`, the row cell widened to u64):

```
// widen(v): sign-extend to i64→u64 for signed type codes, zero-extend for
// unsigned — the existing read_signed / read_unsigned convention (schema.rs).
lo = min widen(v) ; hi = max widen(v)                  // column's signed/unsigned order
max_offset = hi.wrapping_sub(lo)                       // the true value span
bw = (64 - max_offset.leading_zeros()).div_ceil(8)     // whole bytes, 1..=8
keep FoR iff bw < stride AND 8 + count·bw < count·stride
ref = lo                                               // = widen(min), stored as 8 LE bytes
per row: offset = widen(v).wrapping_sub(ref)           // store low bw bytes LE
```

The size test is closed-form from the one min/max scan — **no** value buffer is
allocated to measure a region that ends up Raw. The widening **must** match the
column's signedness: sign-extend for signed type codes, zero-extend for unsigned
(`read_signed` / `read_unsigned` in `schema.rs`). Zero-extending a *signed*
negative leaves its set sign bit in the high bytes, so any range spanning zero
computes `max_offset ≈ 2^64` and is silently forced to Raw — the common
signed-attribute / delta / negative-with-a-NULL-zero case would never pack. With
the correct extension a signed column with negatives frames on its signed min, so
`[smin, smax]` packs to `smax − smin`. Extremes fall back to Raw by the size test
(`min = MIN, max = MAX` ⇒ `bw ≥ stride`). Decode needs no signedness flag: it
reads the 8-byte `ref` (already sign/zero-extended at encode) and adds.

### Decode: `ScalarRegion::Packed`, materialized once per shard open

```rust
#[derive(Clone)]
pub(crate) enum ScalarRegion {
    Raw { offset: usize, size: usize },
    Constant { value: [u8; 16], offset: usize },
    /// FoR + byte-width-truncated integer payload region. `decoded` lazily holds
    /// the full `count × elem_width` little-endian image. Backed by `Box<[u64]>`
    /// (8-aligned) so `col_ptr_by_logical` hands out naturally-aligned pointers,
    /// matching the `Constant` variant's mmap-offset contract and the reader
    /// tests' typed reads. Populated at most once per shard open; the content
    /// address is stable for the shard's lifetime.
    Packed { offset: usize, size: usize, elem_width: usize, decoded: OnceCell<Box<[u64]>> },
}
```

Single decode site on `MappedShard`:

```rust
fn packed_bytes(&self, region: &ScalarRegion) -> &[u8] {
    let ScalarRegion::Packed { offset, size, elem_width, decoded } = region else { unreachable!() };
    let words = decoded.get_or_init(|| decode_for_region(
        &self.data()[*offset..*offset + *size], self.count, *elem_width));
    let byte_len = self.count * *elem_width;
    // SAFETY: `words` is 8-aligned and holds ≥ byte_len bytes.
    unsafe { std::slice::from_raw_parts(words.as_ptr() as *const u8, byte_len) }
}
```

`decode_for_region` reads `ref` (first 8 bytes), derives `bw = (size − 8) /
count`, and for each row: read the row's `bw` bytes LE → zero-extend to u64 →
`wrapping_add(ref)` → write the low `elem_width` bytes LE into the output. Pure
byte copies — no `unsafe` reinterpret, no SIMD, no block or tail special case.
The uniform u64 add-then-truncate reconstructs every width's bit pattern exactly
(borrow bits above `elem_width` are discarded on the final truncation), so it is
byte-exact for signed narrow types with negative minimums.

Accessor arms all serve from `packed_bytes` at the same `row * elem_width`
offset the Raw arm uses: `get_col_ptr` (slice), `col_ptr_by_logical` (pointer),
`to_unified`'s per-column `ColPtr { base, stride }`, and `slice_to_owned_batch`'s
`expand_scalar` memcpy. PK, weight, and null regions are never Packed.

**Open-time validation and role restriction.** `ENCODING_FOR` is legal **only on
payload column directory entries**. `build_scalar_region` gains an `allow_for`
flag — `true` only in the payload-column loop; the PK and null-bitmap calls pass
`false`, and a `false` site seeing `ENCODING_FOR` returns `InvalidShard`.
`build_weight_region` and the blob-entry read likewise reject any encoding
outside their legal set (weight: Raw/Constant/TwoValue; blob: Raw). On an
accepted payload entry, validate `size == 8 + count·bw` with `1 ≤ bw < stride`;
after that `decode_for_region` is infallible (pure arithmetic on validated
bounds), keeping `to_unified` infallible. Adding the variant forces a new match
arm at **every** `ScalarRegion` site in `access.rs` (Rust exhaustiveness catches
each — nothing is silently skipped): the payload-serving sites (`get_col_ptr`,
`col_ptr_by_logical`'s payload branch, `to_unified`'s payload loop, and
`slice_to_owned_batch`'s `expand_scalar` when it serves a payload column) decode
via `packed_bytes`; the PK-only and null-bmp-only sites (`get_pk`, `get_pk_bytes`,
`get_null_word`, the PK/null-bmp branches of `col_ptr_by_logical` / `to_unified`,
and `expand_scalar` when it serves PK/null-bmp) are `unreachable!()`, sound
because open rejects Packed on every non-payload entry.

### Policy

`write_shard_streaming` gains a `pack_ints: bool` parameter (threaded like
`flags`): `true` from `write_as_shard`'s compaction callers (`compact/merge.rs:137`,
`compact/mod.rs:1158`, `:1324`), `false` from `persist_l0_run` and from the
test-only direct `write_as_shard` calls. L0 spill/checkpoint shards stay plain;
every compaction output packs. Consequence, stated plainly: checkpoint write
bandwidth does not shrink, and a table's ≤ 4 pending L0 runs are always raw — the
ratio applies to the compacted bulk.

### Memory accounting (accepted trade, stated honestly)

A decoded region lives as long as its `MappedShard` is open, and any drain,
scan, or compaction over the shard decodes **all** of its packed payload
columns. For a hot, actively-scanned trace shard the `OnceCell` laziness
essentially never helps: the first tick after the shard opens decodes and
**pins** the full raw-size image, and `compact_if_needed` runs on the per-tick
VM path, so a freshly-compacted packed shard takes that decode synchronously on
its next read, not only at cold boot. Net RSS for such a shard therefore **can
rise**, not merely stay flat: what was reclaimable raw page cache becomes a
pinned decoded heap allocation (freed only at shard close) plus the smaller,
reclaimable packed page cache. The point-get path pays a whole-region decode for
its first single-cell read on a shard.

What shrinks: disk footprint, compacted-read page-fault volume (boot resume,
backfills, cold scans), and the bytes a *probe-only* shard occupies (probes
decode nothing). What does not shrink and may regress under memory pressure:
steady-state RSS of actively scanned tables; checkpoint write bandwidth. **The
value case rests on disk and cold-read savings, not RAM** — a memory-pressure
test (many long-lived L1/L2 shards, constrained RSS ceiling) gates acceptance
(Testing).

### Format: version 8

`SHARD_VERSION` 7 → 8 (`layout.rs`); `ENCODING_FOR = 0x03` added and admitted by
the open-time known-encoding check (`open.rs:73`) only via the role-aware paths
above. The bump is a hard break with no migration: on an un-wiped data dir the
reader fails `MappedShard::open` with `InvalidVersion` at boot — data dirs are
wiped across format bumps (pre-alpha convention). `STATE_FORMAT` stays 1 (it
gates view resume, and cannot save a dir whose base shards are unreadable
anyway). Stale V7 strings updated in the same commit (the `shard_file.rs`
assert message, the `open.rs` / `manifest.rs` comments).

### Explicitly out of scope

- Floats (F32/F64) and all 16-byte types (U128/UUID/I128, German-string structs)
  — stay Raw/Constant.
- The PK, weight, null, and blob regions.
- Packing spill/checkpoint L0 shards (`pack_ints = false` there by design).
- Bit-packing, per-block widths, delta/RLE/dictionary encodings, and any
  partial-region (block-granular) decode: whole-region byte-width is the
  committed granularity and the whole region decodes once per shard open.
- String/blob payload compression (a separate concern).
- The WAL/SAL block format (client wire surface).

## Sequencing

- [ ] **Codec + measurement gate.** `encode_for_region` / `decode_for_region` /
      `for_encoded_size`, alongside `detect_encoding` in `shard_file.rs` (no new
      module — the codec is ~30 lines). Exhaustive unit tests (below). **Before
      the format bump:** a throwaway pass computing `for_encoded_size` over the
      integer payload columns of a real compacted `_int_` / `_hist_` /
      `_reduce_in_` shard, plus a decode-throughput microbench — record the
      compacted-shard size reduction and decode values/s in the commit message,
      and proceed only if the ratio justifies the irreversible format bump.
      `make verify`.
- [ ] **Writer + reader + v8, one commit** (the version bump makes a split
      unbuildable: after the bump the old reader would fail on every new shard, so
      writer and reader land together). `pack_ints` threaded through
      `write_shard_streaming{,_inner}` / `write_as_shard`; FoR selection for
      eligible payload regions (compaction callers `true`, `persist_l0_run`
      `false`); `ScalarRegion::Packed`, `packed_bytes`, the four accessor arms,
      `allow_for` role-aware rejection (payload-only; PK/weight/null/blob →
      `InvalidShard`) and size validation; `SHARD_VERSION = 8`, `ENCODING_FOR =
      0x03`, stale-V7 string updates. Roundtrip + rejection tests in the same
      commit. `make verify` + `make e2e` (W=4).
- [ ] **Benches + memory-pressure test.** Extend `shard_merge_scan_bench` /
      `shard_point_probe_bench` with packed-shard variants; add a packed-input
      compaction variant (decode 4 packed inputs → merge → re-encode); run a
      constrained-RSS test over many long-lived L1/L2 packed shards to confirm no
      unacceptable resident-memory regression. Run `make bench` before/after and
      compare `summary.json`; judge merge-scan deltas by the microbench (e2e has
      ±3-4% noise). `make verify` + `make e2e`.

## Testing

- Codec unit tests: roundtrip over seeded-random regions for every eligible type
  code — {small unsigned values (bw 1/2), high-floor unsigned ranges (FoR drops a
  byte), signed with negative min (frames on the signed min), all-equal (→
  Constant wins first), `MIN`/`MAX` extremes (→ Raw by the size test), NULL-zero
  cells among large values (bit-exact zero round-trip), row counts 1 / 1023 /
  1024 / 10000}; `for_encoded_size` matches the emitted size; byte-exact
  reproduction of input (bit-pattern equality, including whatever bytes NULL
  cells carried).
- Reader tests: packed shard roundtrip through all four surfaces against a
  Raw-shard control producing byte-identical batches (including a `found_row`-
  shaped single-cell read and a multi-source `to_unified` drain); `packed_bytes`
  pointer stable across calls and 8-byte aligned; forged `ENCODING_FOR` on
  pk/weight/null/blob dir entries → `InvalidShard`; `size ≠ 8 + count·bw` or
  `bw ∉ [1, stride)` → `InvalidShard`; checksum over encoded bytes still caught
  by `validate_checksums` on a corrupted packed region.
- Compaction integration: ingest mixed-type rows (incl. nullable ints and
  strings) into a `Table`, spill, compact; assert compaction outputs carry
  `ENCODING_FOR` on eligible columns (read the dir entry) and L0 spill shards do
  not; cursor reads and point probes over the packed table match a RAM-only
  control with weight-verified equality; re-compaction of packed inputs (decode →
  merge → re-encode) preserves content.
- Full `make verify` + `make e2e` (W=4) after every checkbox — the existing
  ~1100-test e2e suite over compacted views is the behavioral regression net,
  since packing is transparent above `MappedShard`.
