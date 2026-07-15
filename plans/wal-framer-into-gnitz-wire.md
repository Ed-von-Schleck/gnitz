# Consolidate the low-level WAL codec into `gnitz-wire`

## Problem

`gnitz-wire` is the crate whose sole purpose is *"the one definition client and
engine agree on."* It already owns the neighbouring pieces of the wire format:
the control-block codec (`control.rs`), the German-string transcoder
(`german_string.rs`), the OPK PK-column transcoder (`pk.rs`), `align8`, and the
WAL header **constants** (`wal.rs`). But three low-level pieces of the *same
format* still live outside it, duplicated or misplaced:

1. **The WAL-block framer** — header + region directory + 8-byte alignment + XXH3
   body checksum — is implemented **twice**. The canonical version is
   `gnitz-engine/src/storage/repr/wal.rs` (`encode`, `validate_and_parse`,
   `block_size`, `write_header_and_directory`, `stamp_checksum`, `WalBlockHeader`);
   it is already representation-agnostic (raw region byte-lists, no `Batch`/schema
   knowledge). The client hand-rolls an equivalent in
   `gnitz-core/src/protocol/wal_block.rs` (`encode_wal_block` lines 288-342,
   `decode_wal_block_impl` lines 369-430, plus `append_region`/`append_64bit_region`).
2. **The LE read/write helpers** (`read_u32_le`/`write_u32_le`/`read_u64_le`/
   `write_u64_le`) live in `gnitz-engine/src/foundation/codec.rs` — even though
   that same file already does `pub use gnitz_wire::align8;` (`codec.rs:30`). The
   established pattern is *gnitz-wire owns the wire primitive, the engine
   re-exports it*; the LE helpers are the exception, and the framer (which needs
   them) is why.
3. **The control-block checksum stamp** is hand-written at each caller of the
   *otherwise shared* control-block codec: `encode_control_block`
   (`gnitz-core/src/protocol/message.rs:56-57`) and `encode_ctrl_block_direct`
   (`gnitz-engine/src/runtime/protocol/wire.rs:289-290`) each compute XXH3 over
   `[HEADER..total]` and write `WAL_OFF_CHECKSUM` — the exact body of the
   `stamp_checksum` function this plan lifts.

The cost of this split is **maintenance drift, not a latent runtime bug**: the
whole e2e suite pushes real data through both codecs, so a framer mismatch fails
loudly and immediately (it is not a silent-desync hazard). But every generic
format change must be hand-mirrored across the two implementations. That is not
hypothetical: the `WAL_FORMAT_VERSION 5→6` bump (header 48B→32B, 2026-07-09, six
days before this plan) required a coordinated edit across all three files. This
recurs in a fast-moving pre-alpha codebase.

The fix: move the framer, the LE helpers, and the checksum out of `gnitz-engine`
into `gnitz-wire`, delete the client's hand-rolled framer duplicate, and route
the control-block checksum through the shared `stamp_checksum`. `gnitz-wire`
becomes the single owner of the **whole** low-level wire codec — constants,
primitives, framer, and checksum — not just the constants.

## Non-goals (decided boundaries, rejected on the merits)

- **Do NOT unify the two batch types** (`gnitz-core::ZSetBatch`,
  `gnitz-engine::Batch`). The load-bearing reasons — verified against source, and
  *not* the naive "decode cost regresses" argument (which the code disproves:
  `ColData::Fixed`/`U128s` are already verbatim region-byte copies, and the
  repeat-read consumers `eval_expr`/RMW/C-ABI already re-decode from bytes with no
  cache):
  1. **Crate boundary.** `gnitz-core` must never link `gnitz-engine` (the engine
     depends on `gnitz-core` only as a *dev*-dependency, for the roundtrip test).
     The engine's `Batch` is welded to engine-only internals — arena pool
     (`batch_pool`), hugepage `madvise`, `Layout` fast-path tracking, `blob_id`
     sharing, `SchemaDescriptor` — none of which can live in the leaf crate. "One
     batch type" is structurally impossible; the most you can share is a
     *representation*, giving you two physical batch types, not one.
  2. **The transcode is already shared.** Both sides call the *same* `gnitz-wire`
     primitives (`encode_german_string`, `encode_pk_column`); the client's
     `wal_block.rs` is glue driving those over logical `Vec`s. A collapse
     relocates the glue (encode-time → appender/accessor-time); it deletes nothing.
  3. **Net-simpler bar.** The typed `ColData` API is load-bearing ergonomics
     across `gnitz-sql`, `gnitz-capi`, `gnitz-py`; replacing it with region
     accessors is more surface at more sites.
  4. **The framer lift is a *prerequisite* of every "grand" alternative, not an
     alternative to it.** A physical client batch would still encode via
     region-gather-then-frame and decode via frame-parse-then-region-slice — the
     representation-agnostic framer is called by both regardless. This plan
     targets exactly that maximal shareable unit and puts it where every future
     design, bounded or radical, needs it.
- **Do NOT unify `Schema` / `SchemaDescriptor`.** Cleanly orthogonal: a physical
  client batch would derive strides from the client `Schema` and never need the
  engine `SchemaDescriptor`, whose extra fields (`dist_stride`, `replicated`,
  `payload_cmp`) are pure engine-routing concerns.
- **Do NOT touch the engine's zero-copy scatter fast path** (`scatter.rs`
  `DirectWriter`) — a row-scatter, not a framer. It already calls
  `write_header_and_directory` for the header/directory part (which this plan
  moves; the call keeps working through the migrated symbol).
- **The per-column transcode stays per-crate** (client OPK/German/U128 glue vs
  engine region gather) — genuinely representation-specific.

## Committed decisions

**D1 — `gnitz-wire` owns the checksum; it gains `xxhash-rust` as its first
dependency.** The XXH3 body checksum is part of the WAL format; the crate that
defines the format owns the code that computes it. *Cost is genuinely nil in the
build graph:* `gnitz-core` and `gnitz-engine` already pin `xxhash-rust = 0.8`
(with `features = ["xxh3"]`), it has zero transitive deps (`Cargo.lock`), and
Cargo unifies it — nothing new enters the tree; only `gnitz-wire`'s
`[dependencies]` stops being empty. `gnitz-wire` exposes `pub fn checksum(bytes:
&[u8]) -> u64` wrapping `xxh3_64`. *Rejected:* a caller-injected `hash` fn (keeps
the leaf dep-free but reopens the exact drift this closes and adds a footgun).

**D2 — `encode` takes `regions: &[&[u8]]`, not `(&[*const u8], &[u32])`.** Sizes
are derived via `.len()`. This is a correctness improvement, not cosmetics: the
current two-array signature carries a release-mode-only
`debug_assert_eq!(ptrs.len() == sizes.len())` (`repr/wal.rs:115`) whose violation
is instant UB, and a null-pointer sentinel branch (`repr/wal.rs:138`) that is dead
code at both real call sites (the only null passed is paired with `size == 0`,
consumed by the `sz == 0` branch first). A single slice-of-slices makes the
length invariant structural and lets the null branch be deleted. It also makes the
client's rewritten encoder **borrow-checked**: the region buffers
(`build_pk_region()`'s `Vec`, the incrementally-grown `blob`) are borrowed by the
`&[u8]` slices, so the compiler enforces "allocate/grow to completion, then
collect slices, then call `encode` once" — the ordering hazard a raw-pointer
array would leave silent. `block_size`/`write_header_and_directory` keep their
`&[u32]` size-only signatures (the SAL scatter path uses them without pointers).

**D3 — one shared `WalError` with a canonical `Display` in `gnitz-wire`.** A
5-variant `#[derive(Clone, Copy, PartialEq, Eq, Debug)]` enum — `Truncated`,
`InvalidVersion`, `ChecksumMismatch`, `BufferTooSmall`, `InvalidShard` — whose
`Display` wording (`"truncated"`, `"invalid version"`, `"checksum mismatch"`,
`"buffer too small"`, `"invalid shard layout"`, matching the engine's existing
`StorageError::Display`) is authored **once**. The client conversion delegates:
`impl From<WalError> for ProtocolError { … ProtocolError::DecodeError(format!("WAL {e}")) }`.
*Rejected:* a typed `ProtocolError::Wal(WalError)` variant — it fights
`gnitz-core`'s pervasive `DecodeError(String)` + substring-assert idiom (40+ sites,
including the client's own schema-conformance checks that stay string-based).
*Also rejected:* the previous draft's hand-authored 5 English strings in the
`From` impl — that reintroduced description duplication one layer up.

**D4 — full migration; no re-export shim, no dead `From` impl.** Delete
`gnitz-engine/src/storage/repr/wal.rs` entirely and repoint its callers to
`gnitz_wire::wal`. Verified diff surface: `wal::encode`/`validate_and_parse`/
`block_size` have exactly one direct consumer (`batch_wire.rs`, via `use super::wal`);
`write_header_and_directory`/`stamp_checksum`/`block_size` reach the scatter path
through `storage/mod.rs:53-55` aliases (`wal_write_header_and_directory` etc.).
Full migration = swap `use super::wal;` → `use gnitz_wire::wal;` in `batch_wire.rs`,
swap `repr::wal::` → `gnitz_wire::wal::` in the `storage/mod.rs` aliases, drop
`mod wal;` from `repr/mod.rs`. **No `From<WalError> for StorageError` is added**:
its only would-be call sites (`batch_wire.rs`) use `.expect(...)` / `.map_err(|_| &'static str)`,
never `?` against `StorageError`, so the impl would be dead code from day one.
*Rejected:* the `pub use gnitz_wire::wal::*` shim — CLAUDE.md forbids legacy
forwarding shims, and the migration is ~4 lines.

**D5 — move the LE helpers to `gnitz-wire`; `foundation::codec` re-exports them.**
`read_u32_le`/`write_u32_le`/`read_u64_le`/`write_u64_le` move next to `align8` in
`gnitz-wire`; `gnitz-engine/src/foundation/codec.rs` replaces its local
definitions with `pub use gnitz_wire::{read_u32_le, write_u32_le, read_u64_le,
write_u64_le};` — exactly the pattern it already uses for `align8` (`codec.rs:30`).
The 15 engine files that consume `codec::read_u32_le` are unchanged. This turns a
would-be *new* duplication (the framer needing LE helpers in `gnitz-wire`) into
*additional* dedup.

**D6 — `MAX_WIRE_REGIONS` moves to `gnitz-wire`** (`pub const MAX_WIRE_REGIONS:
usize = 69;`, the framer's `positions` array bound). The engine keeps
`MAX_BATCH_REGIONS = 68` (an arena concern) and ties them:
`const _: () = assert!(gnitz_wire::MAX_WIRE_REGIONS == MAX_BATCH_REGIONS + 1);`.
(`MAX_COLUMNS = 65` bounds any legitimate schema to ≤ 68 regions, so 69 only ever
caps a forged block.)

**D7 — route the control-block checksum through `stamp_checksum`.** Replace the
hand-rolled XXH3-stamp at `message.rs:56-57` and `wire.rs:289-290` with
`gnitz_wire::wal::stamp_checksum(&mut buf, total)` /
`gnitz_wire::wal::stamp_checksum(&mut out[offset..offset + n], n)`. Both callers
already import `gnitz_wire`; zero new deps; directly serves D1's principle.

**D8 — keep the run-coalescing memcpy** (the framer's one `unsafe`). It merges
source-adjacent regions into a single `copy_nonoverlapping`, a real win on the
engine's per-flush/per-IPC gather from its contiguous arena. It is inert-but-safe
for the client (disjoint per-`Vec` regions never coalesce → per-region copies).
Dropping it for a fully-safe per-region `copy_from_slice` loop is **not** done
here: it would regress the engine hot path by an unmeasured amount, and this plan
will not assert an unbenchmarked perf-neutral. The residual `unsafe` is one
localized, documented block, confined by D2's `&[&[u8]]` signature.

## Exact changes

### `gnitz-wire`

- **`Cargo.toml`:** add `[dependencies]\nxxhash-rust = { version = "0.8", features = ["xxh3"] }`.
- **`src/lib.rs`:** beside `align8`, add `pub const fn`/`pub fn` `read_u32_le`,
  `write_u32_le`, `read_u64_le`, `write_u64_le`; add `pub fn checksum(b: &[u8]) ->
  u64 { xxhash_rust::xxh3::xxh3_64(b) }`; add `pub mod error; pub use error::*;`.
- **`src/error.rs`** (new): `WalError` (5 variants) + `Display` (§D3 wording).
- **`src/wal.rs`:** add `pub const MAX_WIRE_REGIONS: usize = 69;` and the framer,
  moved from `gnitz-engine/src/storage/repr/wal.rs` with three mechanical
  substitutions — `WalError` for `StorageError`, `crate::{read_u32_le, …, align8}`
  for `foundation::codec::*`, `crate::checksum` for `foundation::xxh::checksum` —
  and `encode`'s signature changed to `regions: &[&[u8]]` per D2 (derive
  `region_sizes` internally; delete the null-pointer branch; the coalescing loop
  compares `regions[j].as_ptr()` adjacency). Public surface:
  ```rust
  pub fn block_size(region_sizes: &[u32]) -> usize;
  pub fn write_header_and_directory(block: &mut [u8], table_id: u32, entry_count: u32,
      region_sizes: &[u32], total_size: usize) -> [usize; MAX_WIRE_REGIONS];
  pub fn stamp_checksum(block: &mut [u8], total_size: usize);
  pub fn encode(out_buf: &mut [u8], out_offset: usize, table_id: u32, entry_count: u32,
      regions: &[&[u8]], checksum: bool) -> Result<usize, WalError>;
  pub fn validate_and_parse(block: &[u8], out_region_offsets: &mut [u64],
      out_region_sizes: &mut [u32], verify_checksum: bool) -> Result<WalBlockHeader, WalError>;
  pub struct WalBlockHeader { pub table_id: u32, pub entry_count: u32,
      pub num_regions: u32, pub total_size: usize }
  ```
  The framer's 12 unit tests move here (retargeting `StorageError` → `WalError`).

### `gnitz-engine`

- **`src/storage/repr/wal.rs`:** deleted. `src/storage/repr/mod.rs`: drop `mod wal;`.
- **`src/storage/mod.rs:53-55`:** aliases now `pub(crate) use gnitz_wire::wal::{block_size as wal_block_size,
  stamp_checksum as wal_stamp_checksum, write_header_and_directory as wal_write_header_and_directory};`.
- **`src/storage/repr/batch_wire.rs`:** `use super::wal;` → `use gnitz_wire::wal;`.
  The two `wal::encode(...)` call sites build `regions: [&[u8]; MAX_WIRE_REGIONS]`
  from `Batch::region_ptr`/`region_size` (`unsafe { slice::from_raw_parts(ptr, sz) }`
  — the same arena reasoning already present, producing `&[u8]` instead of a
  `*const u8`+len pair). Error handling unchanged (`.expect(...)` / `.map_err(|_|
  …)` accept `WalError` verbatim).
- **`src/storage/repr/batch.rs`:** `pub(crate) use gnitz_wire::MAX_WIRE_REGIONS;`
  + the `MAX_BATCH_REGIONS + 1` static assert (D6).
- **`src/foundation/codec.rs`:** replace the four local LE `fn`s with
  `pub use gnitz_wire::{read_u32_le, write_u32_le, read_u64_le, write_u64_le};`
  (D5).
- **`src/foundation/xxh.rs`:** unchanged (its `checksum` stays for the many
  non-WAL callers), but `wire.rs:289-290`'s control-block stamp switches to
  `gnitz_wire::wal::stamp_checksum` (D7).

### `gnitz-core`

- **`src/protocol/error.rs`:** add `impl From<gnitz_wire::WalError> for ProtocolError`
  delegating to `WalError`'s `Display` (D3).
- **`src/protocol/message.rs:56-57`:** control-block stamp → `gnitz_wire::wal::stamp_checksum` (D7).
- **`src/protocol/wal_block.rs`:** the substantive rewrite.
  - *Encode:* keep the per-column region builders — rename `append_pk_region` →
    `build_pk_region(...) -> Vec<u8>` (returns the OPK region instead of appending
    into a shared buffer), keep `encode_german_col`, the U128 bulk copy,
    `ColData::Fixed` slice borrows, and the incrementally-grown `blob`. **Grow the
    blob to completion first, then** collect `regions: [&[u8]; N]` in canonical
    order (pk, weight, null, payload cols, blob — weight/null borrow
    `batch.weights`/`batch.nulls` bytes directly), then
    `let total = gnitz_wire::wal::block_size(&sizes); let mut out = vec![0u8;
    total]; gnitz_wire::wal::encode(&mut out, 0, tid, count as u32, &regions[..nr],
    true).unwrap();`. Delete `append_region`, `append_64bit_region`, the manual
    directory/header/checksum assembly (lines 320-342). The `&[&[u8]]` signature
    makes the ordering compiler-enforced (D2).
  - *Decode:* replace lines 369-430 with
    `let mut offs = [0u64; gnitz_wire::MAX_WIRE_REGIONS]; let mut szs = [0u32; …];
    let hdr = gnitz_wire::wal::validate_and_parse(data, &mut offs, &mut szs, verify)?;`,
    then keep the client's schema-conformance checks (`hdr.num_regions ==
    3 + schema.num_payload_cols() + 1`, per-region `reg_sz == count * stride`) and
    all domain decode reading from `offs`/`szs`. Split of responsibility: shared
    framer = format validity (version, in-bounds, checksum, region-extent, region
    count ≤ cap); client = schema conformance.

### Tests

- The 12 framer unit tests move to `gnitz-wire/src/wal.rs` (retargeted to `WalError`).
- `gnitz-core`'s `wal_block.rs` tests are unchanged: the `WalError::Display`
  wording keeps `test_bad_checksum` (`contains("checksum")`) and `test_bad_version`
  (`contains("version")`) green; `test_xxh3_pins_known_vector` still passes because
  `gnitz_wire::checksum` *is* `xxh3_64`.
- **The cross-codec guarantee is `ddl_txn_roundtrip_client_to_server` in
  `gnitz-engine/src/runtime/tests/wire.rs`** (engine dev-deps `gnitz-core`) — it
  now drives client-encode → shared framer bytes → engine-decode through the single
  implementation, a strictly stronger check than before. *(The previous draft
  proposed a byte-identity test inside `gnitz-wire`; that is impossible — the leaf
  crate cannot link `gnitz-core`/`gnitz-engine` — and is dropped.)*

## Sequencing

- [ ] **1.** `gnitz-wire`: `xxhash-rust` dep; `checksum`; the four LE helpers;
  `WalError` + `Display`; `MAX_WIRE_REGIONS`; move the framer (`&[&[u8]]` `encode`,
  null-branch deleted) + its 12 tests. `cargo test -p gnitz-wire` green.
- [ ] **2.** `gnitz-engine`: delete `repr/wal.rs`; repoint `batch_wire.rs`
  (`&[&[u8]]` call sites) and the `storage/mod.rs` aliases; `foundation::codec`
  re-exports the LE helpers; `batch.rs` `MAX_WIRE_REGIONS` tie; `wire.rs` control
  stamp → `stamp_checksum`. `cargo test -p gnitz-engine` green (proves `batch_wire`
  and the scatter path are unperturbed).
- [ ] **3.** `gnitz-core`: `From<WalError>`; `message.rs` control stamp →
  `stamp_checksum`; rewrite `encode_wal_block`/`decode_wal_block_impl`; delete the
  hand-rolled framing helpers. `cargo test -p gnitz-core` green.
- [ ] **4.** `make verify` + `make e2e WORKERS=4`.

## Honest cost / benefit

- **Workspace LOC is roughly flat** (~250-line framer *moves* engine→wire; the
  genuine deletion is ~50 net lines of client framer glue, plus the LE-helper and
  control-checksum dedup). The win is **not** a line count — it is: (a) one framer,
  one LE-helper set, one checksum stamp, all in the crate that defines the format;
  (b) generic format changes become single-site instead of a 3-file hand-mirror;
  (c) `gnitz-wire` finally owns the *whole* low-level codec it already half-owns.
- **`gnitz-wire` gains one external dep** (`xxhash-rust`, already in the tree, nil
  build cost) and **one `unsafe` block** (the coalescing memcpy; confined by the
  `&[&[u8]]` signature). These are inherent to hosting a wire codec and are the
  accepted price of single-source-of-truth.
- **Accepted behavioural caveats** (both reject-either-way, neither test-visible,
  both inert on trusted production bytes): a block that is *simultaneously*
  wrong-region-count and out-of-bounds now returns `WalError::Truncated` instead of
  the client's `"num_regions mismatch"` (the shared framer validates format before
  the client validates schema); and the client's PK region is copied twice
  (`build_pk_region` `Vec`, then `encode`'s memcpy) rather than once — offset by the
  single pre-sized `vec!` replacing the old incremental `resize`/`extend`
  reallocations (a wash). The engine hot path is byte-identical (same coalescing,
  same symbols).
