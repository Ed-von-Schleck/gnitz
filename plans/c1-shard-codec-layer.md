# Shard codec layer: schema-aware region encoding

## Problem

The shard writer is structurally unable to host real column encodings, and
there is no performance baseline to regression-check one against:

- **The writer is schema-blind.** `write_shard_streaming` takes only
  `regions: &[(*const u8, usize)]` (`storage/lsm/shard_file.rs:220-228`),
  infers element width as `orig_sz / n` (`shard_file.rs:279`), and identifies
  regions positionally — `i == 1` is the weight region
  (`shard_file.rs:281-285`), `i == last_region` is the blob
  (`shard_file.rs:275`). It cannot know a payload column's type code, so no
  type-dependent encoding (integer packing, string-heap compression) can
  ever be selected here.
- **Encoding selection is hardwired into the phase bodies.** Detection
  (`shard_file.rs:273-294`), checksum assembly (`:337-376`), and the pwrite
  loop (`:414-435`) each re-match the encoding; adding one encoding means
  touching three positional `match`es that only agree by convention.
- **There is no performance baseline.** No microbench pins the shard-backed
  merge-scan or point-probe throughput, so a later encoding change cannot be
  regression-checked (e2e `make bench-full` has ±3-4% run-to-run noise and
  cannot resolve small deltas; the repo's answer is isolated `*_bench`
  release tests).

This plan makes the writer schema-aware and role-dispatched, and adds the
baseline microbenches. It is behavior-neutral: output shard bytes are
identical, `SHARD_VERSION` stays 7, no reader change.

## Load-bearing facts

- Sole shard writer: `write_shard_streaming`
  (`storage/lsm/shard_file.rs:220-250`), inner body
  `write_shard_streaming_inner` (`shard_file.rs:257-443`) with four phases:
  encoding detection, XOR8 build, offset computation, header+pwrite. The
  caller set is exhaustive (`SHARD_MAGIC` is written nowhere else):
  - `Batch::write_as_shard_with_flags` (`storage/repr/batch_wire.rs:33-36`)
    — production callers are the compaction sites
    (`storage/lsm/compact/merge.rs:137`, `compact/mod.rs:1158`, `:1324`),
    each with `schema: &SchemaDescriptor` in scope. The `#[cfg(test)]`
    sibling `write_as_shard` (`batch_wire.rs:25-29`).
  - `Table::persist_l0_run` (`storage/lsm/table/flush.rs:241-280`) — the
    spill/checkpoint L0 writer; `Table` owns `schema: SchemaDescriptor`
    (`table/mod.rs:199`).
  - Test helpers in `shard_reader/mod.rs`, `shard_index/mod.rs`,
    `read_cursor/mod.rs` build raw region vectors directly — each already
    constructs the matching `SchemaDescriptor` for the subsequent
    `MappedShard::open`.
- `Batch` carries `pub schema: Option<SchemaDescriptor>`
  (`storage/repr/batch.rs:240`), `None` for some constructors — so the
  schema is threaded as an explicit parameter from the call sites (which all
  have it), never read out of the batch.
- `SchemaDescriptor` exposes `num_payload_cols()` (`schema.rs:490`),
  `payload_columns()` (iterator of `(payload_idx, col_idx, SchemaColumn)`),
  `pk_stride()`; `SchemaColumn` exposes `size()` and `type_code`.
- Directory entry: `{offset u64, size u64, xxh3 u64, encoding u8, 7 reserved
  zero bytes}` (`shard_file.rs:366-375`); checksums cover the **encoded**
  bytes (`shard_file.rs:339-364`). Encoding constants
  `ENCODING_RAW/CONSTANT/TWO_VALUE = 0x00/0x01/0x02`
  (`storage/lsm/layout.rs:17-19`); `SHARD_VERSION = 7` (`layout.rs:4`).
- Current selection behavior to preserve byte-for-byte: PK and payload
  regions → Constant iff all elements identical and element width ≤ 16,
  else Raw (`detect_encoding`, `shard_file.rs:116-132`); weight (index 1) →
  Constant / TwoValue / Raw (`detect_weight_encoding`,
  `shard_file.rs:135-174`); null bitmap → same as payload (Constant/Raw);
  blob (last region), empty regions, and null-pointer regions → Raw
  (`shard_file.rs:275-277`).
- Bench conventions: `#[ignore]`d `*_bench` release tests with
  `std::time::Instant` + `std::hint::black_box` exist
  (e.g. `runtime/protocol/relay.rs:1586`, `ops/reduce/sort.rs`), run via
  `cargo test -p gnitz-engine --release <name>_bench -- --ignored
  --nocapture --test-threads=1`; seeded RNG via `crate::test_rng`.
  Cursor APIs used by the benches: `advance_to_exact_live`
  (`read_cursor/mod.rs:390`), `seek_bytes` (`mod.rs:256`), `drain_to_batch`
  (`read_cursor/output.rs:122`).

## Committed design

### 1. Schema parameter

`write_shard_streaming` (and `_inner`) gain a `schema: &SchemaDescriptor`
parameter:

```rust
pub fn write_shard_streaming(
    dirfd: c_int,
    basename: &CStr,
    table_id: u32,
    row_count: u32,
    regions: &[(*const u8, usize)],
    schema: &SchemaDescriptor,
    durable: bool,
    flags: u8,
) -> Result<(), StorageError>
```

`Batch::write_as_shard_with_flags` (and the test-only `write_as_shard`)
gain the same parameter and forward it; the compaction call sites pass their
in-scope `schema`, `persist_l0_run` passes `&self.schema`. The schema is an
explicit parameter — not read from `Batch.schema`, which is `Option` and
absent for some constructors. Test helpers pass the `SchemaDescriptor` they
already build.

### 2. Role-dispatched encoding selection

Phase 1 stops inferring widths and roles positionally. Each region index is
classified against the schema:

```rust
enum RegionRole<'a> {
    Pk { stride: usize },            // index 0
    Weight,                          // index 1
    NullBmp,                         // index 2
    Payload { col: &'a SchemaColumn }, // index 3 .. 3 + num_payload_cols
    Blob,                            // last index
}
```

via a single `region_role(i, schema) -> RegionRole` helper, with a
`debug_assert_eq!(regions.len(), 3 + schema.num_payload_cols() + 1)` (the
all-PK wide-key test shape keeps working: payload count comes from the
schema, not `num_cols - 1`). One selection function owns the role → encoding
mapping:

- `Pk { stride }` → Constant (stride ≤ 16) or Raw.
- `Weight` → `detect_weight_encoding` (unchanged).
- `NullBmp` → Constant or Raw (element width 8).
- `Payload { col }` → Constant (width `col.size()`, ≤ 16) or Raw.
- `Blob` → Raw, always.

Element widths now come from the schema (`pk_stride()` / `col.size()` / 8)
instead of `orig_sz / n` — identical values, authoritative source. The
`RegionEncoding` result type and the checksum/pwrite arms are unchanged;
they already dispatch on the detected encoding, not on position. Output
bytes are byte-identical for every input (the existing encoding and
roundtrip tests are the proof, plus the pinning test below).

### 3. Baseline microbenches

Two `#[ignore]`d release-mode timing tests in
`storage/lsm/read_cursor/mod.rs`'s test module, following the repo bench
convention:

- `shard_merge_scan_bench` — write 4 overlapping-key shards of 256K rows
  (U64 PK; I64, nullable I64, and STRING payload columns), open them, drain
  a 4-source `ReadCursor` via `drain_to_batch`, report rows/s. Pins the
  loser-tree merge + scatter path over mmap'd shards.
- `shard_point_probe_bench` — one 1M-row shard; 100K
  `advance_to_exact_live` probes in ascending key order and 100K
  `seek_bytes` probes in shuffled order (seeded `test_rng`), report
  probes/s. Pins the gallop/binary-search path.

These are the regression instruments for subsequent codec work; they land
here so the pre-encoding baseline is measurable on demand (numbers live in
bench output and commit messages, not in source).

### Explicitly out of scope

- Any new encoding byte, any reader change, any format change:
  `SHARD_VERSION` stays 7 and the on-disk bytes are unchanged.
- The WAL/SAL block format (`storage/lsm/wal.rs`, the wire half of
  `batch_wire.rs`): shared with the client wire protocol ("ZSets over the
  wire"); not touched.
- Weight/null region representation on the reader side (`WeightRegion`,
  `ScalarRegion` stay exactly as they are).

## Sequencing

- [ ] **Schema param.** Thread `schema: &SchemaDescriptor` through
      `write_shard_streaming{,_inner}`, `Batch::write_as_shard{,_with_flags}`,
      `persist_l0_run`, and every test-helper call site. Pure plumbing.
      `make verify` + `make e2e` (W=4).
- [ ] **RegionRole restructure.** `region_role` + the single selection
      function replace positional phase-1 detection; widths from schema. Add
      the encoding-pinning test (below). `make verify` + `make e2e` (W=4).
- [ ] **Benches.** `shard_merge_scan_bench` + `shard_point_probe_bench`;
      record baseline numbers in the commit message. `make verify`.

## Testing

- Encoding-pinning test: one synthetic batch exercising every selection
  branch at once (constant PK column shard, constant/two-value/raw weights,
  all-zero nulls, constant and varying payload columns, non-empty blob) —
  assert each directory entry's encoding byte and size. This pins the
  restructure to today's behavior; the existing suite
  (`test_write_shard_streaming_encodings`, `u64_pk_constant_shard`,
  `constant_weight_roundtrip`, `two_value_weight_roundtrip`,
  `build_image_roundtrip`) stays green throughout.
- The wide all-PK schema shape (`find_lower_bound_bytes_wide_pk_distinct`,
  `shard_reader/mod.rs:844-929`) keeps passing — region count derived from
  `num_payload_cols`, not `num_cols - 1`.
- Full `make verify` + `make e2e` (W=4) after every checkbox.
