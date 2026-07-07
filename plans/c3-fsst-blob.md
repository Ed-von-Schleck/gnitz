# FSST blob-heap compression: per-shard symbol table over the string heap

## Problem

The blob region — the variable-length heap behind long (> 12-byte) STRING
and BLOB values — is stored raw in shard files. For text-heavy tables and
the DBSP traces derived from them (join `_int_` integrals, distinct
`_hist_`, reduce `_reduce_in_` — each holding a full payload-widened copy of
its input relation, per view), the heap is the bulk of the shard. FSST
("Fast Static Symbol Table") halves typical database text and decodes at
1–3 GB/s with a ≤ 2 KiB table.

This plan compresses the blob region of **compaction outputs** as a single
FSST stream with a per-shard symbol table. German-string structs are
untouched: their u64 offsets keep addressing the *decoded* heap image, which
the reader materializes **eagerly at shard open** — the decode is
content-dependent and therefore fallible, and open is the one place with an
error channel (`StorageError::InvalidShard`), preserving the codebase's
degrade-don't-abort norm for at-rest corruption. Every consumer of blob
content — merge-path string comparison, join right-side relocation, cursor
drains, hashing — reads through `blob_slice()` and is unchanged.

## Load-bearing facts

### Precondition — codec-layer restructure and packed-int encoding (not yet in the tree)

This plan is **blocked on** two preceding shard-format steps and must not
start before both land. Their delivered state, which this plan assumes: a
schema-aware writer — `write_shard_streaming(dirfd, basename, table_id,
row_count, regions, schema, pack_ints, durable, flags)` classifying regions
by `RegionRole` (Pk/Weight/NullBmp/Payload{col}/Blob), with the `pack_ints`
policy bit passed `true` by the compaction call sites and `false` by
`persist_l0_run`; integer payload regions may carry `ENCODING_PACKED =
0x03` (lazily decoded on the reader — lazy is sound there because the
packed layout is fully validated structurally at open, making the decode
infallible; the FSST decode below is content-dependent, which is why it is
eager instead); `SHARD_VERSION == 8`; encoding bytes 0x00–0x03 taken;
microbenches `shard_merge_scan_bench` / `shard_point_probe_bench` in
`storage/lsm/read_cursor`. Everything in this subsection describes that
delivered state; all other citations are against the current tree.

### Current tree

- German strings (`gnitz-wire/src/german_string.rs:9-40`): 16-byte struct —
  `[0..4)` length u32 LE, `[4..8)` 4-byte prefix, and for `len > 12` a
  u64 LE **byte offset into the blob region** at `[8..16)`. Strings ≤ 12
  bytes are fully inline and never touch the heap. Decode is
  `try_decode_german_string(st, blob)` (`german_string.rs:44-70`), which
  bounds-checks `offset + len` against the blob slice and returns `None`
  on overrun — the degrade-don't-abort contract this plan preserves
  (`read_cursor/mod.rs:829-848` documents it for the cursor readers).
- Every blob-content read goes through two accessors on `MappedShard`:
  `blob_slice(&self) -> &[u8]` and `blob_ptr(&self) -> *const u8`
  (`storage/lsm/shard_reader/access.rs:174-182`), surfaced through
  `CursorSource::blob_slice/blob_ptr` (`read_cursor/source.rs:54-73`) and
  `ReadCursor::blob_slice/blob_ptr/blob_len` (`read_cursor/mod.rs:801-823`).
  The single exception reading the blob via its raw directory offsets is
  `slice_to_owned_batch` (`access.rs:317-328`), converted below. Consumers
  of content: payload comparison — `compare_rows` (`storage/repr/
  columnar.rs:40`) → `cmp_col_window` (`columnar.rs:113`) →
  `compare_german_strings` (defined at `schema.rs:1320`; returns on a
  distinct 4-byte prefix or `min_len ≤ 4` before touching the heap,
  `schema.rs:1327-1333`); join right-side relocation and drain copies via
  `relocate_german_string_vec` (defined at `schema.rs:1217`, BlobCache dedup
  `schema.rs:1194-1254`; call sites `storage/repr/merge.rs:492`,
  `ops/join/rowwrite.rs:81`); reduce retraction copies
  (`read_cursor/output.rs:101-115`); compaction scatter and cursor drains
  via `UnifiedSource.blob_ptr/blob_len` from `to_unified`
  (`access.rs:399-406`, null-guard `storage/repr/scatter.rs:394-401`); and
  `read_german_bytes` (`read_cursor/mod.rs:834-841`).
- Blob **length** (not content) sizing paths: `ReadCursor::total_blob_len`
  (`read_cursor/output.rs:247-249`), `ReadCursor::blob_len`
  (`read_cursor/mod.rs:810-815`), and compaction's arena estimate
  (`compact/merge.rs:79`). Under eager decode these read the decoded
  buffer's length — free.
- `materialize`'s single-shard fast path returns
  `rc.to_owned_batch()` when `!has_ghosts` (`read_cursor/output.rs:154-161`)
  — the SELECT/scan egress fast path; it copies the whole blob
  (`access.rs:317-328`, "string offsets stay valid"), which under this plan
  copies the decoded image. Offsets stay valid by construction: no path
  slices a blob sub-range (even a row sub-slice copies the whole heap), and
  compaction/scatter rebuild offsets via `relocate_german_string_vec` into
  fresh raw heaps — there is no verbatim-old-offset-into-new-blob path.
- The heap image is produced by relocation at batch build time
  (`encode_german_string` appends, `german_string.rs:35-37`; the pooled
  `BlobCache` dedupes shared extents, `schema.rs:1194-1254`), so a shard's
  heap may contain extents shared by multiple structs. Whole-stream
  compression is immune to sharing: the decoded image is byte-identical to
  the raw heap, so every stored offset remains valid.
- Compaction is threshold-gated, not per-epoch: `create_cursor_compacting`
  (`table/mod.rs:420-424`, driven from `query/vm/mod.rs:213`) runs
  `compact_if_needed`, which fires at `L0_COMPACT_THRESHOLD = 4` L0 runs
  (`shard_index/mod.rs:35`). Compaction opens all inputs with checksum
  validation (`MappedShard::open(path, schema, true)` via `open_shards`;
  the query path passes `false`, `shard_index/mod.rs:83`) and reads their
  heaps through `to_unified` — under this plan that means **decode every
  FSST input at open, re-train and re-compress the merged heap on output**,
  a real per-level-up CPU cost that the benches below measure.
- Checksums cover the **encoded** region bytes (`shard_file.rs:339-364`) —
  a corrupted stream is caught before decode wherever checksums are on.
- The `fsst-rs` crate (spiraldb, package `fsst-rs`, lib name `fsst`),
  v0.5.11, Apache-2.0, dep: `rustc-hash`. Exercised in production by Vortex
  (`vortex-fsst`). API facts (docs.rs/fsst-rs + published tarball
  `src/lib.rs`):
  - `Compressor::train(values: &Vec<&[u8]>) -> Compressor` — trains a ≤ 255
    symbol table (code 256 reserved for escape).
  - `Compressor::compress(&self, plaintext: &[u8]) -> Vec<u8>`; worst case
    2× (escape byte + raw byte per untable'd input byte); `unsafe
    compress_into` needs a caller-sized `2 × len` buffer.
  - `Decompressor<'a>::new(symbols: &'a [Symbol], lengths: &'a [u8])`
    decodes with only the table (borrows both slices);
    `decompress_into(&self, compressed, decoded: &mut [MaybeUninit<u8>]) -> usize`
    with the documented buffer bound `max_decompression_capacity =
    8 × (compressed.len() + 1)` (codes expand via unaligned 8-byte stores).
  - `Symbol` is `pub struct Symbol(u64)` — **no repr guarantee, no
    transmuting**. The public round trip: out via
    `symbol.to_u64().to_le_bytes()` (8 bytes LE), in via
    `Symbol::from_slice(&[u8; 8])` (which is `u64::from_le_bytes`).
    `symbol_lengths() -> &[u8]` must be persisted alongside
    `symbol_table() -> &[Symbol]`: `Symbol::len()` cannot reconstruct the
    length of a symbol with trailing 0x00 bytes.
  - **Little-endian only** (explicit in the README). The engine is LE-native
    throughout (every region is defined LE, §6 region convention), so this
    adds no new constraint.
  - README carries a standing "not production ready" caveat. Accepted for a
    pre-alpha engine; mitigated by checksums over the compressed bytes, the
    eager open-time decode validation, and the roundtrip/corpus tests below.

## Committed design

### Encoding: `ENCODING_FSST = 0x04`, blob region only

Eligibility: `RegionRole::Blob`, `blob_len >= 4096`, and the writer's
`compress_blob` policy bit set (below). Kept only if
`encoded_size < blob_len` (strictly smaller, table included); otherwise Raw.
Deterministic — no tuning knob. The 4 KiB floor skips heaps where the ≤ 2 KiB
symbol table + training cost cannot pay.

**Region layout** (region start is 64-byte aligned):

```
n_symbols:    u32 LE           (1 ..= 255)
reserved:     u32 LE           (0; validated)
decoded_len:  u64 LE           (raw heap image size)
symbols:      n_symbols × 8B   (Symbol::to_u64().to_le_bytes() each)
lengths:      n_symbols × 1B   (symbol_lengths(), 1..=8 each)
pad:          to 8-byte alignment
stream:       FSST-compressed image of the raw heap, one stream
```

**Write path** (writer phase 1, Blob role):

1. **Gather training samples.** The writer has the schema and all regions:
   walk every STRING/BLOB payload column's 16-byte structs
   (`region[3 + pi]`, stride 16), collect `(offset, len)` for `len > 12`
   entries, bounds-check against the heap, and take every k-th long string
   so at most 4096 samples are used (deterministic stride, no RNG). A
   Constant-encoded string column contributes its single struct once —
   sample gathering runs on the raw input regions before any encoding is
   applied, so struct bytes are always available. If the heap is non-empty
   but no valid long string references it (corrupt or foreign input), fall
   through to Raw.
2. `Compressor::train(&samples)`, then compress the **whole heap image as
   one stream** into a `2 × blob_len` scratch via `compress_into`. FSST has
   no window; symbol matches may span string boundaries, which is irrelevant
   because decoding always reproduces the exact input image and no reader
   ever random-accesses the *compressed* stream.
3. Assemble the region buffer (header + table via
   `to_u64().to_le_bytes()` + lengths + stream), compare against
   `blob_len`, keep or fall back to Raw. Checksum over the encoded bytes as
   for every region.

**Policy** — `write_shard_streaming` gains `compress_blob: bool`, threaded
exactly like `pack_ints`: `true` from the compaction callers
(`compact/merge.rs:137`, `compact/mod.rs:1158`, `:1324`), `false` from
`persist_l0_run` (spill/checkpoint L0 stays raw — short-lived, on the
latency path) and the test-only `write_as_shard`.

### Decode: `BlobRegion`, eagerly materialized at open

`MappedShard`'s `blob_off: usize, blob_len: usize` fields
(`shard_reader/mod.rs:137-138`) are replaced by one enum:

```rust
pub(crate) enum BlobRegion {
    /// Raw heap served from the mmap, as today.
    Raw { offset: usize, len: usize },
    /// FSST heap, decoded at open. Holds the exact raw heap image;
    /// German-string offsets in the payload structs address it directly.
    Decoded(Box<[u8]>),
}
```

Decoding happens in `MappedShard::open` (the documented cold path — "once
per shard open, never per row", `shard_reader/open.rs:1-3`), which is the
one place with an error channel. For an `ENCODING_FSST` blob entry:

1. Validate structurally: `1 <= n_symbols <= 255`, `reserved == 0`, every
   length ∈ 1..=8, header + table + stream exactly fill the region size —
   else `InvalidShard`.
2. Rebuild the table (`Symbol::from_slice` per 8-byte chunk), build
   `Decompressor::new(&symbols, &lengths)`, decode the stream into a
   scratch of the documented `max_decompression_capacity`
   (`8 × (stream_len + 1)`), and require the returned length to equal
   `decoded_len` — else `InvalidShard` (graceful error, never abort: a
   corrupt re-derivable trace shard then takes the normal
   rebuild/replace paths, matching the codebase's degrade-don't-abort norm
   for at-rest corruption — all 11 open-time failure sites return
   `StorageError` today).
3. Copy the exact `decoded_len` bytes into a `Box<[u8]>`; the scratch is
   dropped. The extra copy is confined to the cold open path.

Eager (not lazy) is deliberate: an FSST decode can fail on content a
structural check cannot vet, and the infallible `blob_slice(&self) ->
&[u8]` accessor has no error channel — lazy decode would force an abort or
a dangling-offset degrade, both worse. The cost: a string-heavy shard's
heap is decoded and held for the shard's whole open lifetime even under
probe-only workloads, and boot/resume decodes every FSST shard's heap while
loading manifests (sequential, 1–3 GB/s — bounded by the compacted string
volume).

Accessors:

- `blob_slice()` — Raw: mmap window (unchanged); Decoded: `&buf[..]`.
- `blob_ptr()` — pointer to the same bytes; stable for the shard's
  lifetime (matching `to_unified`'s pointer contract, `access.rs:346-348`).
- `slice_to_owned_batch`'s raw `blob_off/blob_len` read
  (`access.rs:317-328`) switches to `blob_slice()`; the whole-heap copy and
  "offsets stay valid" invariant are unchanged.
- `total_blob_len` / `ReadCursor::blob_len` / compaction's `s.blob_len`
  sizing read the decoded length (a field read; compaction's
  `total_blob` sum at `compact/merge.rs:79` switches to a
  `blob_decoded_len()` accessor on `MappedShard`).

**Cost accounting (accepted trades, stated plainly).** (a) RSS: every open
FSST shard pins its decoded heap on the Rust heap for its open lifetime —
about what the page cache holds for a raw heap today, plus the (reclaimable)
compressed pages the decode faulted. (b) Compaction: every level-up of a
string-heavy shard decodes all FSST inputs at open, re-trains, and
re-compresses the merged heap — CPU the compaction bench below measures
against raw-blob inputs. (c) Egress fast paths (`materialize`'s
`to_owned_batch`, `slice_to_owned_batch`) copy the decoded image — same
memcpy as today, preceded by the one open-time decode. What shrinks: disk
footprint of compacted string-heavy shards (traces most of all), checkpoint
resume/backfill page-fault volume for those shards, cold-scan read volume.

### Format: version 9

`SHARD_VERSION` 8 → 9 (`layout.rs`); `ENCODING_FSST = 0x04` added to
`layout.rs` and the open-time known-encoding check; the byte on any
non-blob region → `InvalidShard`. Hard break, no migration: an un-wiped
data dir fails `MappedShard::open` with `InvalidVersion` at boot (base
shards have no self-healing gate); data dirs are wiped across format bumps
(pre-alpha convention), and the `STATE_FORMAT` manifest knob deliberately
stays 1 for the same reason as the previous bump.

### Dependency

`fsst-rs = "0.5"` in `crates/gnitz-engine/Cargo.toml` (lib name `fsst`).
The `unsafe` inventory: `compress_into` on the writer with an exact
`2 × blob_len` scratch (the documented worst case), and the
`MaybeUninit` buffer handling around `decompress_into` at open.

### Explicitly out of scope

- Per-string compressed extents / random access into the compressed stream:
  the committed unit is the whole heap image, decoded once at open.
- Compressing spill/checkpoint L0 blob heaps (`compress_blob = false` there
  by design, not deferral).
- The 16-byte German-string structs in payload regions (their offset
  semantics are unchanged; as 16-byte columns they are outside the packed
  integer encoding too).
- Dictionary encoding of repeated strings, cross-shard shared tables, and
  any change to `encode_german_string` / the in-memory `Batch` blob arena —
  the in-engine representation stays raw everywhere; compression exists only
  at rest in shard files.
- The WAL/SAL block format (client wire surface).

## Sequencing

- [ ] **Codec module.** `storage/lsm/fsst_blob.rs`: sample gathering
      (`collect_long_string_samples(regions, schema)`), region
      encode/decode, symbol-table (de)serialization
      (`to_u64().to_le_bytes()` / `Symbol::from_slice`), size accounting.
      Unit tests (below) including the 2×-expansion fallback, the
      decoded-length check, and trailing-zero-byte symbols. Add the
      `fsst-rs` dependency. `make verify`.
- [ ] **Writer + reader + v9, one commit** (the version bump makes a split
      unbuildable). `compress_blob` parameter threaded through
      `write_shard_streaming{,_inner}` / `write_as_shard_with_flags`;
      Blob-role FSST selection; compaction passes `true`, `persist_l0_run`
      `false`; `BlobRegion` replaces `blob_off/blob_len` with eager decode
      + validation in `open`; `blob_slice/blob_ptr/blob_decoded_len`;
      `slice_to_owned_batch` and the sizing paths switched;
      `SHARD_VERSION = 9`, `ENCODING_FSST = 0x04`. Roundtrip + rejection
      tests in the same commit. `make verify` + `make e2e` (W=4).
- [ ] **Benches.** String-heavy variant of `shard_merge_scan_bench` (4-byte
      shared prefixes forcing content compares, plus a distinct-prefix
      control), `fsst_blob_decode_bench` (open-time heap decode GB/s), and
      a string-heavy compaction bench (compact 4 FSST-blob vs 4 raw-blob
      inputs — pins decode-inputs + retrain + recompress). Run `make bench`
      before/after; judge merge deltas by the microbench (e2e noise ±3-4%);
      record the compacted-shard size reduction on a string-heavy dataset
      in the commit message. `make verify` + `make e2e`.

## Testing

- Codec unit tests: heap-image roundtrip (byte-identical, offsets
  untranslated) over: repetitive text, natural-language corpus lines,
  seeded-random binary (must fall back to Raw via the size rule), heaps just
  under/over the 4 KiB floor, a heap with BlobCache-style shared extents
  (two structs, one extent), and an empty heap; symbol tables containing
  symbols with trailing 0x00 bytes round-trip through the region header
  (the persisted lengths, not `Symbol::len()`, are authoritative); sample
  gathering skips short-string columns, handles Constant string regions,
  and bounds-rejects corrupt offsets.
- Reader tests: FSST shard roundtrip through every content surface —
  `read_german_bytes` per row, a two-shard cursor merge whose payload
  tiebreak hits long-string content (`compare_german_strings` through
  `cmp_col_window`), a join-shape relocation and drain against a Raw-shard
  control producing byte-identical batches, `materialize`'s
  `to_owned_batch` fast path, `slice_to_owned_batch` with valid offsets;
  malformed header (n_symbols 0 or > 255, nonzero reserved, bad symbol
  length, size mismatch) → `InvalidShard` at open; a stream whose decode
  length ≠ `decoded_len` → `InvalidShard` at open (graceful, no panic);
  forged `ENCODING_FSST` on pk/weight/null/payload entries →
  `InvalidShard`; corrupted stream caught by `validate_checksums` on the
  compaction path.
- Compaction integration: string-heavy `Table` (long + short + NULL strings,
  compound payloads) → spill → compact; compaction outputs carry
  `ENCODING_FSST` on the blob entry, spill shards stay Raw; SELECT-style
  drains, join-side relocation, and re-compaction (decode → merge → retrain
  → re-encode) all weight-verified against a RAM-only control.
- Full `make verify` + `make e2e` (W=4) after every checkbox — the existing
  string e2e suites exercise every consumer transparently.
