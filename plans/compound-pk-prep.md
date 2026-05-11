# Structural plan: factor the PK out of "one column" shape

## Status

The reader-side migration described in `pk-list-accessors.md` has
landed. Every engine reader of `pk_index` now goes through one of the
four accessors (`pk_indices`, `pk_index_single`, `is_pk_col`,
`payload_idx`) — plus the `payload_columns()` iterator — on both the
engine `SchemaDescriptor` and the core `Schema` mirror. The
single-PK boundary is grep-able as `pk_index_single()`.

What remains is the **constructor-side sweep**: replace the underlying
`pk_index: u32` field with a list-shaped representation, install O(1)
mapping arrays, and migrate every direct `SchemaDescriptor { ... }`
literal to call a new validating constructor. After that lands the
hot accessors collapse to single-load lookups and the only way to
build a `SchemaDescriptor` is through a constructor that always
recomputes the mappings in lock-step with the columns and PK list.

## Background

`foundations.md` §1: "**Single-column PKs only.** Each table has exactly
one PK column (enforced by schema validation). The 128-bit API surface
holds one column value." Every part of the engine — storage formats,
sort/merge comparators, exchange routing, XOR8 filters, the SQL planner,
and the system catalog — is built on top of this assumption.

Lifting it is not a single feature change; it touches at least four
layers (schema, on-disk formats, operator/IPC surfaces, SQL planner).
This plan tackles the one remaining structural blocker that makes
compound PKs a redesign instead of a feature.

## What's actually blocking compound PKs today

A thorough sweep of the codebase finds five categories of single-PK
assumption. The reader sweep (done) covered category 1 below; the rest
remain explicit boundaries marked by `pk_index_single()` calls.

### 1. The schema field — the chokepoint

```rust
// crates/gnitz-engine/src/schema.rs
#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    pub num_columns: u32,
    pub pk_index: u32,                              // ← single column
    pub columns: [SchemaColumn; MAX_COLUMNS],
}
```

`payload_columns()` and `payload_idx()` are scalar (single-branch)
helpers sourced from `pk_index_single()`. The mapping-array rewrite
arrives here in this plan — once the underlying field is a real list
we have somewhere to anchor the precomputed mappings and removing
optimizer dependence on `pk_count == 1` actually matters.

### 2. On-disk and in-memory formats

```rust
// foundations.md §6 region layout:
// region[0] = pk         (count × pk_stride bytes; pk_stride ∈ {8, 16})
// region[1] = weight     (count × 8 bytes)
// region[2] = null       (count × 8 bytes)
// region[3..]            payload columns

// crates/gnitz-engine/src/storage/shard_reader.rs
pub(crate) pk_stride: u8,                          // ← one stride

// crates/gnitz-engine/src/layout.rs
SHARD_VERSION: u64 = 6                             // ← bump on layout change

// crates/gnitz-wire/src/lib.rs
WAL_FORMAT_VERSION: u32 = 4                        // ← bump on layout change

// crates/gnitz-core/src/protocol/wal_block.rs
let pk_stride = schema.columns[schema.pk_index_single()].type_code.wire_stride();

// crates/gnitz-engine/src/storage/batch.rs
pub fn get_pk(&self, row: usize) -> u128 { ... }   // ← one u128 slot
```

The shard file, WAL block, wire batch, and `ArenaZSetBatch` all encode
"region[0] holds one PK column at one stride". Lifting this requires a
format-version bump and a migration path. **This plan does not touch
the formats.** The formats remain valid for single-column PKs (which
is all that exists today); a future plan will bump them.

### 3. The catalog row

```rust
// crates/gnitz-core/src/types.rs
TABLETAB_COL_PK_COL_IDX = pk_col_idx: U64          // ← one u64 per table

// crates/gnitz-core/src/protocol/codec.rs
// Decodes META_FLAG_IS_PK and asserts at most one column has it set
```

`TABLE_TAB.pk_col_idx` is the durable serialization. Lifting requires
either widening the column to a blob/array or adding a side table.
This plan does not touch the catalog.

### 4. SQL parser silently degrades compound PK

`PRIMARY KEY (a, b)` is rejected at planner.rs ~140 with
`GnitzSqlError::Unsupported("PRIMARY KEY must specify exactly one
column")`. Lifting this is a parser change. This plan does not touch
the parser.

### 5. The 128-bit PK abstraction

```rust
// crates/gnitz-engine/src/storage/batch.rs                 get_pk(row) -> u128
// crates/gnitz-engine/src/xxh.rs                           hash_u128(pk: u128) -> u64
// crates/gnitz-engine/src/storage/xor8.rs                  build(&[u128]) / may_contain(_, u128)
// crates/gnitz-engine/src/storage/partitioned_table.rs     partition_for_key(pk: u128)
// crates/gnitz-engine/src/storage/read_cursor.rs           seek(key: u128)
// crates/gnitz-capi/gnitz.h                                append_row(pk_lo: u64, pk_hi: u64, ...)
```

A compound PK over (i64, i64) fits in u128. A compound PK over more
than two 8-byte columns does not. The 128-bit slot is documented in
foundations §1 as "a sort/routing key … not unique for intermediate
batches" — i.e. it is *already* allowed to be a hash rather than the
literal PK. Two physical-layout directions for compound PKs are
viable:

- **Direction A (wide PK region):** `region[0]` becomes
  `region[0..N]`, one column per PK component. Comparators compare PK
  columns lexicographically. Range scans on PK work natively.
  Format change: large.
- **Direction B (hash + payload-stored PK columns):** `region[0]` stays
  a single u128 — a hash of the compound PK. The actual PK columns
  live in the payload region. Comparator-on-hash-collision falls
  through to the payload comparison, which already handles per-column
  ordering. Range scans on PK do not work natively (hash destroys
  order).
  Format change: minimal.

This plan does not pick a direction. The constructor-side rewrite
below is a precondition for both.

## What this plan changes

One commit. A constructor-side refactor.

Flip the underlying representation and install O(1) mappings for both
the payload-vs-PK question and the dense payload iteration:

```rust
pub const MAX_PK_COLUMNS: usize = 4;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    num_columns: u32,
    pk_count: u32,                                 // 1 today
    pk_indices: [u32; MAX_PK_COLUMNS],             // first pk_count entries valid
    /// payload_mapping[ci] = dense payload index, or PAYLOAD_MAPPING_PK_SENTINEL.
    payload_mapping: [u8; MAX_COLUMNS],
    /// payload_to_ci[pi] = logical column index for dense payload slot `pi`.
    /// Inverse of payload_mapping over the non-PK columns. Lets
    /// `payload_columns()` walk a contiguous range with one indirection
    /// per element — no per-row predicate.
    payload_to_ci: [u8; MAX_COLUMNS],
    pub columns: [SchemaColumn; MAX_COLUMNS],
}
```

**Field privacy.** `pk_count`, `pk_indices`, `payload_mapping`, and
`payload_to_ci` are private. `num_columns` is also private (it must
match `pk_count + popcount(non-PK cols)`, which `new` guarantees). The
struct is constructed exclusively through:

```rust
impl SchemaDescriptor {
    pub const fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self;
    pub const fn minimal_u64() -> Self;          // existing helper
}

impl Default for SchemaDescriptor {
    fn default() -> Self { Self::new(&[], &[]) }
}
```

`new` validates `pk_indices.len() <= MAX_PK_COLUMNS` and computes
`payload_mapping` / `payload_to_ci` in a single pass using a `while`
loop (legal in `const fn`). `Default` delegates to `new(&[], &[])` so
`payload_mapping` and `payload_to_ci` are filled with the sentinel —
never an all-zero array that would silently make every column look like
a payload column at index 0.

Two read-side getters replace the privatized field:

```rust
#[inline] pub fn num_columns(&self)      -> usize { self.num_columns as usize }
#[inline] pub fn num_payload_cols(&self) -> usize {
    self.num_columns as usize - self.pk_count as usize
}
```

`num_payload_cols()` replaces the `schema.num_columns as usize - 1`
pattern (linear.rs, join.rs, etc.) which encodes "the single PK gets
subtracted". Generalises naturally to compound PKs.

The reason for privacy is mechanical: a public `payload_mapping` field
allows any caller (test or otherwise) to construct a `SchemaDescriptor`
with `..Default::default()` or struct-literal syntax that bypasses the
recompute, producing a schema where `payload_idx` returns 0 for every
column. Forcing construction through `new` makes that error
unrepresentable. The migration touches every direct
`SchemaDescriptor { ... }` literal — see "Validation procedure".

Privatizing `num_columns`, `pk_count`, and `pk_indices` together is what
makes the "default + mutate" idiom impossible to compile, not a style
convention to enforce by review. A test that writes
`let mut s = SchemaDescriptor::default(); s.columns[0] = ...;
s.num_columns = 1; s.pk_index = 0;` no longer typechecks — the only way
to reach a populated descriptor is `SchemaDescriptor::new(&cols, &pks)`,
which always recomputes the mapping arrays in lock-step with the columns
and PK list.

**`empty_schema()` is removed.** The helper at `compiler.rs` returns
a stub that is then mutated field-by-field at the call site:
`out.columns[ci] = ...; out.num_columns = ci as u32; out.pk_index = 0;`.
This pattern is incompatible with the new representation — a stub-then-mutate
schema has `payload_mapping` filled with sentinels, so every appended
column appears to be a PK and `payload_idx` is undefined for all of
them. The fix is not "ban the field assignment" (the field is private
anyway) but "rewrite the builders to collect columns and call `new`",
using the existing `payload_columns()` iterator instead of manual
"skip the PK index" loops:

- `compiler.rs::merge_schemas_for_join_impl`: already rebuilds as
  `[left.pk] + left.payload_columns() + right.payload_columns()` with
  the right side's `nullable` bit OR-ed in for outer joins. After the
  rewrite this collects a `Vec<SchemaColumn>` and calls `new(&cols,
  &[0])`.
- `compiler.rs::build_map_output_schema`: same shape.
- `compiler.rs::build_reduce_output_schema`: same shape; natural-vs-
  synthetic-PK branches both end at one `new` call.
- `make_group_idx_schema` and the sibling fixed-shape helper: rewrite
  as straight `new(&[col(...), col(...)], &[0])`.
- Inline `let mut s = empty_schema(); ...` builders inside the
  per-instruction lowering loop: same — collect columns, call `new`.
- `vec![empty_schema(); num_regs]`: replace with
  `vec![SchemaDescriptor::default(); num_regs]`. The slot is a
  placeholder — every entry is overwritten by the per-register schema
  before it is read.
- `out_schema: empty_schema()` initializers and test scaffolding:
  replace with `SchemaDescriptor::default()`.
- The same "empty + mutate" appears in `compiler.rs` test scaffolding
  (`let mut left = empty_schema(); ...`) and the
  `test_build_reduce_output_schema_*` cases. Same rewrite.

After this, `empty_schema()` is deleted. There is no helper that
returns an in-progress, structurally-invalid descriptor.

Existing const helpers (`make_schema`, `from_wire_cols`,
`build_schema_from_col_defs`, the catalog-load path) call `new`
internally; they already accept a single `pk_index: u32` value and
forward it as `&[pk_index]`.

The four hot accessors collapse to single-load lookups:

```rust
#[inline]
pub fn pk_indices(&self) -> &[u32] {
    &self.pk_indices[..self.pk_count as usize]
}

#[inline]
pub fn pk_index_single(&self) -> u32 {
    debug_assert_eq!(self.pk_count, 1, "compound PK not yet supported here");
    self.pk_indices[0]
}

#[inline]
pub fn is_pk_col(&self, ci: usize) -> bool {
    self.payload_mapping[ci] == PAYLOAD_MAPPING_PK_SENTINEL
}

#[inline]
pub fn payload_idx(&self, ci: usize) -> usize {
    debug_assert!(!self.is_pk_col(ci), "payload_idx: ci is a pk column");
    self.payload_mapping[ci] as usize
}

pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &SchemaColumn)> {
    let num_payload = self.num_columns as usize - self.pk_count as usize;
    (0..num_payload).map(move |pi| {
        let ci = self.payload_to_ci[pi] as usize;
        (pi, ci, &self.columns[ci])
    })
}
```

Branchless: `payload_columns()` walks a contiguous `0..num_payload`
range with one byte load per element. No filter chain in
`compare_rows`. No optimizer dependence on `pk_count` being known at
compile time. Generalizes uniformly to `pk_count > 1`.

`MAX_PK_COLUMNS = 4` is a sizing choice, not a semantic limit.
`SchemaDescriptor` grows by 16 bytes for `pk_indices` plus 65 bytes for
`payload_mapping` plus 65 bytes for `payload_to_ci` (= 146 bytes on top
of the existing layout). Hot accesses hit `pk_indices` and
`payload_mapping` together; `payload_to_ci` is hot in iteration loops
and amortises across the row scan.

Mapping recompute lives inside `new`:

```rust
const fn compute_mappings(
    num_columns: u32, pk_count: u32, pk_indices: &[u32; MAX_PK_COLUMNS],
) -> ([u8; MAX_COLUMNS], [u8; MAX_COLUMNS]) {
    let mut payload_mapping = [PAYLOAD_MAPPING_PK_SENTINEL; MAX_COLUMNS];
    let mut payload_to_ci   = [PAYLOAD_MAPPING_PK_SENTINEL; MAX_COLUMNS];
    let mut pi: u8 = 0;
    let mut ci: usize = 0;
    while ci < num_columns as usize {
        let mut is_pk = false;
        let mut k = 0;
        while k < pk_count as usize {
            if pk_indices[k] as usize == ci { is_pk = true; }
            k += 1;
        }
        if !is_pk {
            payload_mapping[ci] = pi;
            payload_to_ci[pi as usize] = ci as u8;
            pi += 1;
        }
        ci += 1;
    }
    (payload_mapping, payload_to_ci)
}
```

There is no behaviour change for callers. Every call site already goes
through `pk_indices()` / `pk_index_single()` / `payload_idx()` /
`is_pk_col()`, all of which keep working — and `payload_idx` /
`is_pk_col` / `payload_columns()` get faster.

## What this plan deliberately does NOT cover

- **Implementing compound PKs.** That work — bumping shard/WAL/wire
  formats, widening the catalog row, parsing
  `PRIMARY KEY (a, b)`, and choosing Direction A vs B for the physical
  layout — is what this plan unblocks.
- **Format version bumps.** `SHARD_VERSION = 6`, `WAL_FORMAT_VERSION =
  4`, and the wire format are unchanged. Single-PK shards continue to
  be produced and consumed bit-for-bit identically.
- **Deciding wide-PK-region vs hash-key.** Both directions are
  consistent with the in-memory abstraction this plan installs. The
  decision is part of the actual implementation, not the prep.
- **The `get_pk -> u128` API.** It stays scalar. Compound PKs that fit
  in u128 (most two-column cases) work without touching it. Wider
  compound PKs require a follow-up that introduces a `PkKey`
  abstraction or chooses Direction B; this is not in scope here.
- **Catalog row format.** `TABLE_TAB.pk_col_idx` (single u64) is
  unchanged. The codec keeps decoding/encoding a single
  `META_FLAG_IS_PK` column. Lifting this is part of the actual
  compound-PK implementation.
- **SQL planner compound-PK acceptance.** The planner continues to
  reject `PRIMARY KEY (a, b)` with a clear `Unsupported` error.
  Lifting this is part of the actual compound-PK implementation.
- **C / Python bindings widening.** `gnitz_batch_get_pk_lo/hi` and
  `PySchema.pk_index` remain scalar. Replacing them with column-array
  access is part of the actual implementation. `BatchAppender::col_index`
  similarly stays single-PK; introducing a `logical_idx()` inverse
  accessor is deferred to the client-surface widening.
- **Index machinery (`IndexCircuit`, AggValueIndex).** Multi-column
  secondary indices are independently out of scope here — they are
  unrelated to compound primary keys and would be a separate redesign.
  GroupIndex and AggValueIndex already use composite `(gc_u64,
  source_pk)` keys; no changes here.
- **Foreign-key validation against compound PKs.**
  `catalog/validation.rs` continues to validate FKs against a single PK
  column via `pk_index_single()`. FK-vs-compound-PK is follow-up.
- **Per-table `MAX_PK_COLUMNS` tuning.** One global constant.

## Validation procedure

1. **`make test`** — engine unit tests. All pass.
2. **`make e2e`** — end-to-end suite with `GNITZ_WORKERS=4`. Exercises
   real DML, exchange routing, and recovery paths. All pass.
3. **Debug-only assertions**: `pk_index_single()` carries
   `debug_assert_eq!(pk_count, 1)` so a future compound-PK schema that
   accidentally reaches an un-generalized boundary fires loudly.
   `payload_idx()` carries `debug_assert!(!is_pk_col(ci))`.
4. **Benchmark check**: `make bench` baseline vs candidate. The
   mapping-array rewrite is expected to be neutral or marginally
   faster on `compare_rows`-heavy workloads (sort, distinct, N-way
   merge); confirm no regression > 1% on any benchmark and no
   regression > 0.5% on hot-path comparators.
5. **Constructor audit**:
   `rg "SchemaDescriptor\s*\{" crates/` should match only the struct
   definition itself and the `Default` / `new` / `minimal_u64`
   implementations. Every other construction site (test helpers in
   `ops/{distinct,exchange,join,reduce,scan}.rs`, `vm.rs`,
   `storage/shard_file.rs`, `compiler.rs`'s `merge_schemas_*` /
   `build_map_output_schema` / `build_reduce_output_schema` / inline
   per-instruction builders, etc.) must go through
   `SchemaDescriptor::new(...)` so `payload_mapping` and `payload_to_ci`
   are always populated. `rg "empty_schema" crates/` should return zero
   matches.
