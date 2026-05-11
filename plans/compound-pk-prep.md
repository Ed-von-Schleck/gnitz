# Structural plan: factor the PK out of "one column" shape

## Background

`foundations.md` §1: "**Single-column PKs only.** Each table has exactly
one PK column (enforced by schema validation). The 128-bit API surface
holds one column value." Every part of the engine — storage formats,
sort/merge comparators, exchange routing, XOR8 filters, the SQL planner,
and the system catalog — is built on top of this assumption.

Lifting it is not a single feature change; it touches at least four
layers (schema, on-disk formats, operator/IPC surfaces, SQL planner).
This plan tackles the schema-shape structural blocker that makes
compound PKs a redesign instead of a feature.

## What's actually blocking compound PKs today

A sweep of the codebase finds five categories of single-PK assumption.
This plan targets category 1; the other four remain explicit boundaries
marked by `pk_index_single()` calls and are out of scope here.

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

The reader-side accessors (`num_columns`, `pk_indices`,
`pk_index_single`, `is_pk_col`, `payload_idx`, `payload_columns`,
`num_payload_cols`) are scalar shims over the single `pk_index: u32`
field — `pk_indices()` returns a `slice::from_ref(&pk_index)`,
`is_pk_col` and `payload_idx` reduce to a single equality / branch on
that one index. The mapping-array rewrite arrives in this plan — once
the underlying field is a real list we have somewhere to anchor the
precomputed mappings and removing optimizer dependence on
`pk_count == 1` actually matters.

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
// table_tab_schema(): one `pk_col_idx: U64` ColumnDef per table row

// crates/gnitz-core/src/protocol/codec.rs
// Decodes META_FLAG_IS_PK and asserts at most one column has it set
```

`TABLE_TAB.pk_col_idx` is the durable serialization. Lifting requires
either widening the column to a blob/array or adding a side table.
This plan does not touch the catalog.

### 4. SQL parser silently degrades compound PK

`PRIMARY KEY (a, b)` is rejected at `gnitz-sql/src/planner.rs` with
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

## What's already in place

The schema-constructor refactor landed in commit `1cb1830` and
established the constructor that this plan hangs the mapping
computation on:

- `SchemaDescriptor::new(cols: &[SchemaColumn], pk_indices: &[u32])`
  exists as a `const fn`. The body validates `pk_indices.len() == 1 ||
  (cols.is_empty() && pk_indices.is_empty())` and writes `pk_index`
  from `pk_indices[0]`.
- Every stub-then-mutate site (the `empty_schema()` / direct struct
  literal / `Default::default()` + field-mutation patterns) routes
  through `new()`. The `empty_schema()` and `col()` helpers are
  deleted.
- The reader-side accessors — `num_columns()`, `pk_indices()`,
  `pk_index_single()`, `is_pk_col()`, `payload_idx()`,
  `payload_columns()`, `num_payload_cols()` — are all in place. They
  read through the existing scalar `pk_index` field today; the bodies
  flip in this plan.
- `PAYLOAD_MAPPING_PK_SENTINEL: u8 = u8::MAX` exists and is already
  consumed by `ops::reduce` for `SortDesc::pi`.

What is *not* yet routed through `new()` is the build-then-literal
pattern: ~30 sites that fill a `[SchemaColumn; MAX_COLUMNS]` array and
construct the descriptor with one struct literal that names every
field. These compile today because `num_columns`, `pk_index`, and
`columns` are still `pub`. They are the surface this plan migrates.

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

**Field privacy.** `num_columns`, `pk_count`, `pk_indices`,
`payload_mapping`, and `payload_to_ci` are private. `columns` stays
`pub` because external callers index it heavily (`schema.columns[ci]`).
The struct is constructed exclusively through:

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

The existing `num_payload_cols()` body changes from
`self.num_columns() - self.pk_indices().len()` to
`self.num_columns as usize - self.pk_count as usize` — a one-line
substitution against the new representation.

The reason for privacy is mechanical: a public `payload_mapping` field
allows any caller (test or otherwise) to construct a `SchemaDescriptor`
with `..Default::default()` or struct-literal syntax that bypasses the
recompute, producing a schema where `payload_idx` returns 0 for every
column. Forcing construction through `new` makes that error
unrepresentable.

Privatizing `num_columns`, `pk_count`, and `pk_indices` together is what
makes the "default + mutate" idiom impossible to compile, not a style
convention to enforce by review. Ten existing test scaffolds today still
write `let mut s = SchemaDescriptor::default(); s.num_columns = N;
s.pk_index = K;` (in `schema.rs` tests at L472–501 and `runtime/executor.rs`
tests at L1163, L1170) — those stop typechecking and migrate to a
single `SchemaDescriptor::new(&cols, &[K])` call as part of this plan.

### Build-then-literal site migration

The schema-constructor refactor left every site that fills a
`[SchemaColumn; MAX_COLUMNS]` array and ends with a single struct
literal in place. These compile today because the fields are `pub`;
they stop compiling once the fields privatize. Each is a mechanical
one-line substitution: `SchemaDescriptor { num_columns, pk_index,
columns }` → `SchemaDescriptor::new(&columns[..num_columns],
&[pk_index])`.

Audit shows ~30 sites under `rg "SchemaDescriptor\s*\{\s*num_columns"
crates/`:

- `vm.rs::make_schema`, `runtime/master.rs::{u64_schema, two_col_schema}`
  and inline literals.
- `expr/tests/{program_tests, plan_tests, batch_tests}.rs::make_schema*`.
- `storage/{batch, columnar, partitioned_table, table, read_cursor,
  shard_reader, merge}.rs::make_*_schema` test helpers (one each).
- `storage/memtable.rs::{make_u64_i64_schema, make_schema_cols}` plus
  one inline literal.
- `storage/columnar.rs` test helpers (4 sites).
- `ops/{scan, distinct, exchange, join, linear}.rs::make_*_schema` test
  helpers; one production literal in `ops/linear.rs::op_null_extend`.
- `ops/reduce.rs::{make_schema_u64_i64, make_schema_u64_f32,
  make_schema_with_type}` plus the `runtime/wire.rs` snapshots.

None of these touches operator logic, storage code, or the hot path —
they are all schema-shape mechanics. Existing const-fn helpers
(`catalog::sys_tables::make_schema`, `from_wire_cols`) already forward
through `new()` since the constructor refactor; nothing changes there.

The hot accessors collapse to single-load lookups:

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

There is no behaviour change for callers. The accessor signatures —
`pk_indices()`, `pk_index_single()`, `payload_idx()`, `is_pk_col()`,
`payload_columns()`, `num_payload_cols()` — are unchanged; only their
bodies and the underlying field representation flip. `payload_idx`,
`is_pk_col`, and `payload_columns` get faster.

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
   implementations. `rg "\.num_columns\s*=" crates/` and
   `rg "\.pk_index\s*=" crates/` should both return zero matches —
   field privatization makes those writes impossible to compile.
