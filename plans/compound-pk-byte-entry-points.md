# Byte-addressed PK entry points + wide-PK wire control

## Goal

Introduce a parallel, byte-addressed PK access path through the storage
read path (batch, read cursor, shard reader, partitioned-table found-row
plumbing) and extend the wire control block so it can carry a PK region
wider than 16 bytes.

This is pure infrastructure. **No feature becomes user-visible**: no new
SQL is accepted, no table can yet be created with a PK region wider than
16 bytes, and every existing single-PK call site keeps its current
`u128`-keyed fast path untouched. The test suite must stay green at every
commit. The payoff is that all later compound-PK work has a complete,
order-correct byte path to build on.

All file paths below are under `crates/`.

## Design: one always-present column

`seek_pk_extra` is a single nullable BLOB column appended to the shared
`CONTROL_COLS`. This is the least-complex and most-verifiable shape:

- One schema, one template, one encode path, one decode path. No flag
  bit, no second `SchemaDescriptor`, no encoder/decoder branch, no
  direction-dependent semantics.
- `CONTROL_COLS` is the single source of truth: the engine's
  `CONTROL_SCHEMA_DESC` and the client's `control_schema()` both *derive*
  from it (`schema_from_wire_cols`), so client/server layout drift is
  structurally impossible, not merely tested against.
- The existing cross-crate guard tests
  (`test_ctrl_block_client_server_byte_equality`,
  `gnitz-core/tests/ipc_failures.rs`) already exercise encode→decode on
  every run and pick up the new column's empty case for free; one added
  non-empty round-trip gives complete coverage.

Adding a nullable BLOB column grows `CTRL_BLOCK_SIZE_NO_BLOB` by
**exactly 24 bytes**: a 16-byte fixed German-string locator region
(`gnitz_wire::wire_stride(BLOB) == 16`, the per-column overhead of any
STRING/BLOB column — *not* zero; the variable blob arena is separate and
costs 0 when empty) plus an 8-byte directory entry from `NUM_REGIONS`
12→13 (`= 3 + (NUM_COLUMNS-1) + 1`, so `3 + 9 + 1`). `CTRL_BLOCK_SIZE_NO_BLOB`,
`CTRL_BLOCK_TEMPLATE`'s size, and every `OFF_*` offset are `const fn`
(`ctrl_region_offset`) outputs — they recompute automatically. **No
absolute structural-size assertion exists anywhere in the workspace**
(verified: every `CTRL_BLOCK_SIZE_NO_BLOB` use is relative — vecs sized
by the const, and the `LazyLock` `assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB)`
is self-relative). Do **not** spend time hunting for a hard-coded size
constant to bump; there is none. The +24 is a real per-message cost on
the hot path — see *Performance*.

`ctrl_region_offset`'s `sizes[r] % 8 == 0` assertion still holds: the
new region is 16 bytes (16 % 8 == 0) and the blob region stays 0.

## Performance

The control block is encoded on **every reply** through the
`encode_ctrl_block_direct` template-memcpy fast path, and a control WAL
frame rides every message. The +24 bytes is therefore a per-message
cost: +24 bytes memcpy'd per `encode_ctrl_block_direct` call and +24
bytes on every control frame on the wire. This is not free bookkeeping —
it is the price of the "one always-present column" shape.

It is, however, the **minimum-overhead** shape for the common (narrow,
empty) case, which is why it is the right call here:

- A nullable BLOB costs 16 (fixed German-string locator) + 8 (directory
  entry) and **zero** variable blob bytes when empty. Narrow PKs (every
  table today) always hit the empty case.
- A fixed-width raw column sized for the worst case would cost
  `MAX_PK_BYTES - 16 = 64` bytes *unconditionally* — far worse.
- A flag-gated second `SchemaDescriptor` could cost 0 in the narrow case
  but is exactly the encode/decode-branch complexity the design
  deliberately rejects (and would defeat the `schema_from_wire_cols`
  single-source-of-truth guarantee).

So +24 is the floor given a single shared schema. Surfaced here so the
trade is explicit; the choice between this and a flag-gated layout is a
plans-level decision, not one this infra change should silently make.

Hot-path invariants this change must preserve (regressing any of these
is a correctness-of-performance bug, not a style issue):

- **`encode_ctrl_block_direct` stays a pure template memcpy + 7 fixed
  patches.** It takes no new parameter; the template already carries the
  empty `seek_pk_extra` region and its null bit. No new branch, no
  `Batch` allocation on the no-error path.
- **`peek_control_block` stays allocation- and blob-lookup-free when
  both `error_msg` and `seek_pk_extra` are null** (the universal case
  until a wide-PK send path exists). The only added hot-path work is one
  `null_bmp & NULL_BIT_SEEK_PK_EXTRA` test and a branch — both already
  cheap, both on data already in registers. The shared
  `wal_dir_entry(REGION_BLOB)` lookup must remain gated behind
  `!error_is_null || !seek_extra_is_null`. Until the wide-PK send path
  lands, `seek_extra_is_null` is *always* true, so the non-empty
  `seek_pk_extra` decode runs only under test, never in production.
- **Single-column PKs must never be routed through the byte path.**
  `compare_pk_bytes` for a single-column PK still walks the
  `pk_columns()` iterator with a per-type-code `match` *per probe* —
  strictly slower than the one `u128` numeric compare on the existing
  path, for a bit-identical result. Keep `find_lower_bound(key: u128)` /
  `seek(key: u128)` / `current_key` as the narrow path. The test that
  asserts `find_lower_bound` and `find_lower_bound_bytes` land on the
  same row proves equivalence — it must **not** be read as license to
  unify them onto the byte path. This is a hard rule.

## Background the implementer needs

A PK is stored as a **concatenated raw-byte region**: `pk_stride` bytes
per row, `pk_stride = sum(strides of PK columns)`. PK columns are *not*
duplicated into payload — the region *is* the PK. Today every table has
exactly one PK column and `pk_stride` is 8 or 16, so the region fits in a
`u128` and ordered comparison is a single `u128` numeric compare. That
fast path stays.

Two orthogonal axes matter:

- **Region width** (`pk_stride <= 16` vs `> 16`): governs whether the
  region fits in a `u128` word. Opaque-bytes call sites (writers that
  treat the region as bytes) gate on this. `schema.pk_is_wide()`
  (`schema.rs`) is the precomputed predicate (`pk_stride >
  NARROW_PK_MAX_BYTES`).
- **Column count** (`pk_count == 1` vs `>= 2`): governs which comparison
  is correct. `pk_count == 1` allows a `u128` numeric compare. For
  `pk_count >= 2` a `u128` compare over a concatenated tuple puts the
  trailing column in the high bits and reverses priority, so ordered
  comparison must go column-by-column. Ordered call sites
  (`find_lower_bound`, cursor seek, shard overlap) gate on this.

`MAX_PK_BYTES = 80` (`schema.rs`, `= MAX_PK_COLUMNS * 16`; `manifest.rs`
statically asserts `== 80`) bounds any PK region.
`compare_pk_bytes(schema, a, b) -> Ordering`
(`storage/columnar.rs:128`) is the column-aware comparator: it iterates
`schema.pk_columns()` with per-column signed/unsigned/float/u128 type
dispatch. **It already exists, is complete, and is tested** for
single-column (u8..u64, i8..i64, u128, uuid) and compound (mixed-type,
pk-indices-order-not-schema-order) cases — this plan only adds *callers*
of it, it does not modify it.

`compare_pk_bytes` `debug_assert_eq!`s `a.len() == schema.pk_stride()`
and `b.len() == schema.pk_stride()` (`columnar.rs:137-144`). These are
**debug-only**: in a release build the length contract is *unchecked*,
and a wrong-length slice does not assert — it either silently misreads
adjacent bytes or panics deep in a `try_into().unwrap()` / slice index
with no diagnostic. The contract is therefore the *only* thing between a
release-mode silent-corruption bug and correctness. Every new `_bytes`
entry point carries it: **the key slice passed in must be exactly
`pk_stride` bytes**, identical width to the stored regions it is compared
against. State this in each new fn's doc and uphold it at every call
site — do not rely on the debug assert to catch a violation.

## Already in place (do not rebuild)

- `Batch::get_pk_bytes`, `extend_pk_bytes`, `set_pk_at_bytes` — the
  bytes accessors on `Batch` (`storage/batch.rs:533/667/696`). All three
  are `#[allow(dead_code)]` today; this plan activates production
  callers. `extend_pk_bytes` `assert_eq!`s `bytes.len() == pk_stride`.
- `compare_pk_bytes` in `storage/columnar.rs:128`, fully tested.
- `MappedShard::get_pk_bytes` (`storage/shard_reader.rs:331`).
- The wire control block already declares `seek_pk: U128`
  (`gnitz-wire/src/lib.rs`, `control` module / `CONTROL_COLS`,
  `NUM_COLUMNS == 9`).
- `PartitionedTable` found-row accessors `found_null_word`,
  `found_col_ptr`, `found_blob_ptr` (`storage/partitioned_table.rs`).
- `schema.pk_stride()`, `schema.pk_columns()`, `schema.pk_indices()`,
  `schema.pk_is_wide()` all exist; the catalog already persists the full
  PK list.

## The change, by file

### 1. `gnitz-wire/src/lib.rs` — control schema

Append one column to `CONTROL_COLS` (after `error_msg`):

- `seek_pk_extra: BLOB nullable` — carries PK region bytes `16..` for a
  wide PK; empty for `pk_stride <= 16`.

Add four explicit consts alongside the existing `error_msg` ones (these
are hand-written `col_index`/`payload_index` derivations, not
auto-generated): `COL_SEEK_PK_EXTRA`, `PAYLOAD_SEEK_PK_EXTRA`,
`REGION_SEEK_PK_EXTRA`, and `NULL_BIT_SEEK_PK_EXTRA = 1u64 <<
PAYLOAD_SEEK_PK_EXTRA` (mirrors `NULL_BIT_ERROR_MSG`). Appending after
`error_msg` keeps `PAYLOAD_ERROR_MSG`/`REGION_ERROR_MSG` unchanged;
`seek_pk_extra` becomes payload 8 / region 11, `REGION_BLOB` shifts to
region 12 via `NUM_REGIONS - 1`. Update the column-list doc comment above `pub mod control`: change the
header `Schema (9 columns, pk_index = 0)` to `10 columns` and append the
`col  9: seek_pk_extra  BLOB   (nullable)` line so the documented layout
matches the array (the plan's "documented layout matches the array"
guarantee depends on both edits, not just the new line).

Keep `seek_pk: U128` as-is. Do **not** widen `seek_pk` itself to BLOB:
a BLOB `seek_pk` would break the precomputed template-copy fast path
(`CTRL_BLOCK_TEMPLATE` in `runtime/wire.rs`) that dominates the
error-response cost. The split — fixed `seek_pk` for the common narrow
case, optional `seek_pk_extra` blob for the rare wide case — preserves
the fast path.

### 2. `gnitz-engine/src/runtime/wire.rs` — control encode/decode

- `encode_ctrl_block_ipc`: add a `seek_pk_extra: &[u8]` parameter and a
  corresponding `b.extend_col(ctrl::PAYLOAD_SEEK_PK_EXTRA, &st)` write
  (German-string struct: `encode_german_string` when non-empty, else
  `[0u8; 16]` exactly as the `error_msg` arm does). The `Batch` built
  from `CONTROL_SCHEMA_DESC` now has 9 payload columns; every payload
  region must be written or the block is malformed. This function does
  **not** call `pk_index_single()` — it writes via `extend_pk(0u128)` +
  `PAYLOAD_*` consts; no PK-index derivation exists here to change.
- Null bitmap: `seek_pk_extra` and `error_msg` are both nullable. The
  encoded null word must OR each column's null bit independently when
  that value is empty:
  `null_word = (if has_error {0} else {NULL_BIT_ERROR_MSG}) | (if
  has_seek_extra {0} else {NULL_BIT_SEEK_PK_EXTRA})`.
- `encode_ctrl_block_direct`: takes **no** new parameter. Its no-error
  fast path still memcpys the (now 24-byte-larger) template and patches
  the same 7 fixed fields — the template already carries the correct
  empty `seek_pk_extra` region and null bit. Its non-empty-`error_msg`
  fallback call into `encode_ctrl_block_ipc` must pass the new argument
  as `&[]`. Wide PK (none yet) is not reachable through the direct
  encoder; that is acceptable until the wide-PK send path exists.
- **Every** `encode_ctrl_block_ipc` caller must thread the new arg, or
  it will not compile. These are: the `CTRL_BLOCK_TEMPLATE` builder
  (`wire.rs:287`), the direct-encoder fallback (`wire.rs:315`), the test
  `test_ctrl_block_client_server_byte_equality` (`wire.rs:1111`), and
  the unit-test callers in `runtime/tests/wire.rs` (the direct⟷ipc
  equivalence test ~`:557` and the fallback-equivalence test ~`:698`).
  All pass `b""`. `encode_ctrl_block_direct` callers (`wire.rs:553/609`,
  `runtime/sal.rs:775`, and its test callers) are unchanged since its
  signature does not change.
- `DecodedControl`: add `pub seek_pk_extra: Vec<u8>`. `peek_control_block`
  must decode it. `seek_pk_extra` (BLOB) and `error_msg` (STRING) each
  own a 16-byte German-string struct in their own fixed region
  (`REGION_ERROR_MSG`, `REGION_SEEK_PK_EXTRA`) and spill overflow into
  the **shared** `REGION_BLOB`. Preserve the existing fast path: today
  `peek` does **no** `wal_dir_entry(REGION_BLOB)` lookup when
  `error_msg` is null (the common case). New shape: compute
  `error_is_null` and `seek_extra_is_null` from the null word; resolve
  the shared `(blob_off, blob_sz)` directory entry **once, only if**
  `!error_is_null || !seek_extra_is_null`; feed that single slice to
  both `decode_german_string` calls (each reads its own 16-byte struct
  region). When both are null, skip the blob lookup entirely as today.
- `CONTROL_SCHEMA_DESC` / `ctrl_region_offset` / `CTRL_BLOCK_SIZE_NO_BLOB`:
  the new column's region offset is computed automatically.
  `CTRL_BLOCK_SIZE_NO_BLOB` grows by exactly 24 bytes (16-byte fixed
  German-string region + 8-byte directory entry; the constant and
  `CTRL_BLOCK_TEMPLATE` size are derived, so they update themselves —
  only absolute-size test assertions need editing).
  `ctrl_region_offset`'s 8-alignment assertion still holds (a 16-byte
  German-string region is 8-aligned). `ctrl_region_offset` keeps its
  existing `pk_index_single()` call on the single-PK control schema —
  the control schema is itself single-PK and stays so; this is not a
  site this plan migrates.

`wire_size`, `wire_size_range`, `encode_wire`, and `encode_wire_into*`
need **no signature or body change**. They size the ctrl blob region
from `error_msg` overflow only; this stays correct because
`seek_pk_extra` is always empty here, its fixed 16-byte German-string
region is already counted in `CTRL_BLOCK_SIZE_NO_BLOB` /
`schema_wal_block_size`, and an empty value spills zero blob bytes.
Threading a wide `seek_pk_extra` through the sizing API belongs to the
later wide-PK send work, not here.

### 3. `gnitz-engine/src/storage/batch.rs`

- `append_row_from_ptable_found(ptable, pk: u128, weight)` → take
  `pk_bytes: &[u8]` and write it via `extend_pk_bytes` instead of
  `extend_pk(pk)`. `pk` is used **only** at the `self.extend_pk(pk)`
  line today (verified: `batch.rs:1029-1078`); the rest of the function
  reads `ptable.found_*`, so the swap is mechanically clean. For a
  narrow PK, the stored region bytes are exactly the LE bytes
  `extend_pk` would have written, so single-PK callers are
  byte-for-byte unaffected (see §4 for how the caller supplies the
  bytes). `extend_pk_bytes` asserts `bytes.len() == pk_stride`.
- Keep `find_lower_bound(key: u128)` as the narrow-PK helper. **Add**
  `find_lower_bound_bytes(key: &[u8]) -> usize` that runs the same
  binary search but compares via `compare_pk_bytes(&self.schema,
  self.get_pk_bytes(mid), key)` against the batch's own `self.schema`.
  Doc the `key.len() == pk_stride` contract. Do not route the existing
  `find_lower_bound` through the byte path — single-PK keeps its `u128`
  compare (the byte path is strictly slower: per-probe column-walk vs a
  single `u128` compare).

### 4. `gnitz-engine/src/dag.rs` — found-row callsite

No accessor is added to `PartitionedTable`/`Table`. There are **exactly
two** `append_row_from_ptable_found` callsites, both in
`enforce_unique_pk_partitioned` (`dag.rs:1388` in the `w > 0` branch and
`dag.rs:1406` in the `w < 0` branch); they are the only callers in the
workspace. Both follow the same shape and change identically. `pk =
batch.get_pk(row)` at the top of the loop, and `effective` shares
`*schema` with `batch`, so `batch.get_pk_bytes(row)` is exactly the
stored region for that row at the matching `pk_stride`. The borrow is
clean: `batch` (immutable), `effective` (mutable), `ptable` (`&mut`) are
distinct objects.

```rust
let (_existing_w, found) = ptable.retract_pk(pk);
if found && !store_retracted.contains(&pk) {
    effective.append_row_from_ptable_found(ptable, batch.get_pk_bytes(row), -1);
    store_retracted.insert(pk);
}
```

Apply the analogous one-line edit at the `w < 0` site.

### 5. `gnitz-engine/src/storage/read_cursor.rs`

`ReadCursor` does not touch `Batch`/`MappedShard` directly — it
dispatches through the `CursorSource` enum (2 variants: `Batch(Rc<Batch>)`,
`Shard(Rc<MappedShard>)`) and the per-source `CursorState`. The byte
path must mirror that layering, not bypass it:

- `CursorSource`: **add** `find_lower_bound_bytes(&self, key: &[u8],
  schema: &SchemaDescriptor) -> usize` and `get_pk_bytes(&self, row:
  usize) -> &[u8]`, each a two-arm match delegating to §3 (`Batch`) and
  §6 (`MappedShard`). The `Batch` arm compares against the batch's own
  schema (§3) and ignores the passed `schema`; the `Shard` arm forwards
  the passed `schema` (§6 — `MappedShard` has no `SchemaDescriptor`,
  only a `pk_stride` field).
- `CursorState`: **add** `seek_bytes(&mut self, src: &CursorSource, key:
  &[u8], schema: &SchemaDescriptor)` mirroring `seek` — calls
  `src.find_lower_bound_bytes` then `skip_ghosts`.
- Keep `seek(key: u128)` as the narrow-PK entry point. **Add**
  `seek_bytes(key: &[u8])` that drives the same per-source positioning
  and (for `Multi` mode) tree rebuild, passing `&self.schema` down to
  `CursorState::seek_bytes`. `ReadCursor` already owns
  `schema: SchemaDescriptor` (`read_cursor.rs:309`).
- Keep `current_key: u128`. **Add** `current_pk_bytes(&self) -> &[u8]`
  reading the active source's PK region without copying via
  `CursorSource::get_pk_bytes`. Existing single-PK consumers keep
  calling `current_key` / `seek`.

`seek_bytes` must not drive over a wide PK yet: `drive`/`drive_single`
and `build_tree`'s heap-key closure unconditionally call `get_pk` →
`widen_pk_le`, which panics for `stride ∉ {8,16}`. For narrow PK
`seek_bytes` is safe; wide-PK exercise of the byte path happens only at
the source level (§3, §6), never through the cursor.

### 6. `gnitz-engine/src/storage/shard_reader.rs`

Mirror §3: keep `find_lower_bound(key: u128)`; add
`find_lower_bound_bytes(key: &[u8], schema: &SchemaDescriptor) -> usize`
going through `compare_pk_bytes(schema, self.get_pk_bytes(mid), key)`.
`MappedShard` stores only `pk_stride`, not a `SchemaDescriptor`, so the
schema must be passed explicitly by the caller
(`ReadCursor::seek_bytes` passes `&self.schema`). Doc the `key.len() ==
pk_stride` contract.

Caveat for the wide-PK source-level test (§Testing): `get_pk_bytes`'s
`RegionView::Constant` arm returns `&value[..stride]` where `value:
[u8;16]`; a wide-PK shard whose PK region collapsed to a `Constant`
(all PKs identical) would panic the `..stride` slice. Build the wide-PK
shard fixture with distinct PKs so its PK region stays `Raw`.

### 7. `gnitz-core/src/protocol/message.rs` — client control codec (mirror)

`CONTROL_COLS` is shared and `control_schema()` derives the wire
*layout* from it via `schema_from_wire_cols`, so it stays in sync
automatically. But `encode_control_block` builds a `[Option<ColData>;
ctrl::NUM_COLUMNS]` and `.unwrap()`s every slot — an unset 10th column
panics **every** client encode, including the whole
`gnitz-core/tests/ipc_failures.rs` round-trip suite and the cross-crate
`test_ctrl_block_client_server_byte_equality`. Set the new slot:

```rust
columns[ctrl::COL_SEEK_PK_EXTRA] = Some(ColData::Bytes(vec![None]));
```

and OR `ctrl::NULL_BIT_SEEK_PK_EXTRA` into `nulls_val` (alongside the
existing `NULL_BIT_ERROR_MSG`) when the value is empty. `ColData::Bytes`
encodes byte-for-byte like `ColData::Strings` / the engine's
German-string path (its own doc states "Same on-wire encoding as
Strings"), so a `vec![None]` empty entry + null bit produces the same 16
zero bytes the engine emits — client/server bytes stay identical.
`decode_control_block` needs **no** change: it reads columns by index
and never touches `seek_pk_extra` (no wide PK exists yet);
`decode_wal_block` decodes the BLOB column generically and the result is
discarded.

## Constraints

- `pk_index_single()` call sites are **not** touched in this change.
  Because no compound or wide-PK table can be created yet, the
  `pk_count == 1` assertion inside `pk_index_single()` never fires
  (this includes `ctrl_region_offset`'s use of it on the single-PK
  control schema). Migrating those callers belongs to later work and is
  out of scope here.
- Every new `_bytes` / `seek_pk_extra` path is *additive*. If any
  existing single-PK test changes behaviour, the change is wrong. The
  one expected non-behavioural change is the +24-byte
  `CTRL_BLOCK_SIZE_NO_BLOB`.

## Out of scope

SQL planner accepting `PRIMARY KEY (a, b, ...)`; dropping the
I*/U*→U64 PK coercion; DML PK-tuple extraction; operator schema
builders; secondary-index prefix-scan; FK rules; Python/C API surface;
wide-PK *send* path (sizing API threading, `encode_ctrl_block_direct`
wide path). None of these are needed to land this change green.

## Testing

Rust unit tests (`gnitz-engine`):

- `storage/batch.rs`: build a batch via `with_schema` whose schema has a
  wide (`pk_stride > 16`) and/or multi-column PK region; round-trip
  `append_row_from_ptable_found` and verify `find_lower_bound_bytes`
  ordering matches `compare_pk_bytes` on the same inputs (key slice
  exactly `pk_stride` bytes). Include a narrow single-PK case asserting
  `find_lower_bound` and `find_lower_bound_bytes` land on the same row
  (`compare_pk_bytes` is correct for single-column PKs — already tested
  in `columnar.rs`).
- `read_cursor.rs` / `shard_reader.rs`: `seek_bytes` /
  `find_lower_bound_bytes` land on the same row as the `u128` variants
  for narrow PK. Wide-PK fixtures are exercised **only** at the source
  level — `Batch::find_lower_bound_bytes` and
  `MappedShard::find_lower_bound_bytes` (explicit schema, distinct PKs
  so the shard PK region stays `Raw`) — never through
  `ReadCursor::seek_bytes`, which would panic in `widen_pk_le` while
  driving. `seek_bytes` is tested with narrow PK only.
- `runtime/wire.rs`: encode/decode a control block with `seek_pk_extra`
  non-empty (sharing the blob region with a non-empty `error_msg`) and
  confirm both round-trip independently. Assert
  `encode_ctrl_block_direct` and `encode_ctrl_block_ipc` still produce
  byte-identical output for empty `seek_pk_extra` (fast-path
  equivalence). No test asserts an absolute `CTRL_BLOCK_SIZE_NO_BLOB` or
  template size (verified workspace-wide) — the +24 propagates through
  the derived const with no test edit; do not add a hard-coded size
  assertion. `test_ctrl_block_client_server_byte_equality` must stay
  green — after the §7 client change both encoders still emit an
  identical empty `seek_pk_extra` region (16 zero bytes via
  `[0u8; 16]` on both sides) and an identical null bitmap (both OR
  `NULL_BIT_SEEK_PK_EXTRA`); its `encode_ctrl_block_ipc` caller passes
  `b""` for the new arg.

Cross-crate (`gnitz-core`):

- `gnitz-core/tests/ipc_failures.rs` (round-trips through
  `encode_control_block`) must stay green — it is the regression guard
  for the §7 client change; no new test needed beyond confirming it
  passes.

Use a timeout on every test; diagnose any hang rather than re-running.
Run `make test`; the full suite must be green before and after.
