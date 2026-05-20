# PK-list-aware catalog caches and helpers

## Goal

Replace the single-column-PK assumptions in the catalog's in-memory
cache and a couple of helpers so internal data structures faithfully
represent the full PK column list. Pure infrastructure: **no new SQL
is accepted, no table can yet be created with a compound PK**, and
every behavior is bit-identical for current single-PK tables. The test
suite must stay green at every commit. The payoff is removing four
`pk_index_single()` callers from the catalog layer and giving the
planner / DML / operator work that follows a catalog cache it can
trust for compound PKs.

All file paths below are under `crates/gnitz-engine/`.

## Background the implementer needs

The catalog persists the full PK column list:

- `TABLE_TAB.pk_cols` packs `pk_count + pk_col_idx[..]` via
  `pack_pk_cols` / `unpack_pk_cols` (`catalog/sys_tables.rs:69/82`).
  The decoded shape is `PkColList { cols: [u32; 4], len: usize }` with
  `as_slice() -> &[u32]` and `decoded_count() -> usize`
  (`sys_tables.rs:60-67`). `validate_pk_cols` (`sys_tables.rs:103`)
  rejects malformed counts (`5..=15`) and out-of-range / nullable /
  non-PK-eligible / duplicate columns. `PkColList` is the canonical
  in-memory shape of the on-disk encoding — **reuse it; do not
  introduce a parallel representation.**
- `create_table` takes `pk_cols: &[u32]`
  (`catalog/ddl.rs:150` packs it back into the wire row).
- `build_schema_from_col_defs` and the table-register hook already
  construct multi-PK `SchemaDescriptor`s via
  `SchemaDescriptor::new(cols, pk_indices)`.

What still flattens to a single PK column:

- **`catalog/cache.rs`** holds `pk_col_of: FxHashMap<i64, u32>`
  (`cache.rs:18`). `apply_pk_col_of` (`cache.rs:184`) reads the
  packed wire u64 with `unpack_pk_cols(...).as_slice().first()
  .copied().unwrap_or(0)`, **dropping every PK column after the
  first**. For tables the truncation is silent; for views it
  hard-codes `0` (views always pk_col=0 today). The cache then drives
  schema-version invalidation when the stored value changes.
- **`catalog/bootstrap.rs:293`** seeds `pk_col_of` with bare `0` for
  bootstrapped system tables before the wire hook ever runs.
- **`catalog/ddl.rs:411`** (`create_fk_indices`) reads `pk_col_of` to
  decide which columns get an FK auto-index — it skips the single PK
  column (`if col_idx == pk_idx { continue; }`). For a compound-PK
  table that skip is too narrow: every PK column should be skipped
  (the PK region itself already covers them).
- **`catalog/utils.rs:97`** (`BatchBuilder::physical_col_idx`) maps a
  dense payload slot to a schema column index via
  `schema.pk_index_single()`. `SchemaDescriptor::payload_col_idx(pi)`
  (`schema.rs:317`) does exactly this for any `pk_count` — the helper
  here is a single-PK reimplementation of that precomputed mapping.

None of these sites `assert!`s today because no current table is
multi-PK. Each `pk_index_single()` call is the canary that fires the
day a compound PK reaches the site.

## Constraints

- Behavior must be bit-identical for single-PK tables (every existing
  table). `make test` and the E2E suite stay green at every commit.
- `pk_col_of` is engine-internal; its value type can widen freely.
- The catalog already correctly persists the full PK list on disk —
  this change is purely about the live cache and two helpers, not
  about the wire/SAL/bootstrap encoding.
- Don't touch the FK *type-resolution* sites in
  `catalog/validation.rs:22` or the *index schema* construction in
  `catalog/hooks.rs:297`. Those are coupled to the FK-rule change and
  the GI/AVI index rework respectively (see *Out of scope*).
- Don't touch `pk_index_single()` callers outside the catalog.

## The change, by file

### 1. `catalog/cache.rs`

- `pk_col_of: FxHashMap<i64, u32>` → `pk_col_of: FxHashMap<i64,
  PkColList>` (import `PkColList` from `super::sys_tables`).
- `apply_pk_col_of` (`cache.rs:184-214`): for table rows, replace the
  current first-element extraction with the full list:

  ```rust
  let pk_list = if is_table {
      unpack_pk_cols(self.read_batch_u64(batch, i, 3))
  } else {
      // Views always pk_col=0; mirror today's behavior.
      PkColList::single(0)
  };
  ```

  Use the existing `unpack_pk_cols` for tables. For views, today's
  hard-coded `0` becomes a single-element `PkColList`. Add a
  `PkColList::single(idx: u32)` constructor in
  `catalog/sys_tables.rs` (one line: `PkColList { cols: [idx, 0, 0,
  0], len: 1 }`) so views and bootstrap (§2) share one factory.

  The schema-invalidation guard
  (`if old != Some(pk_col) { invalidate_col_names(tid); }`) now
  compares `Option<PkColList>` — derive `PartialEq, Eq, Clone, Debug`
  on `PkColList` (it's `Copy`-eligible: 16-byte array + usize).

- **Validation gap**: today `apply_pk_col_of` *silently* truncates a
  malformed PK list to its first element via `unwrap_or(0)`. The hook
  contract (per the existing `// apply_pk_col_of fires before
  hook_table_register` comment) is that `hook_table_register` will
  return `Err` immediately afterward for the same row, so the
  truncated cache entry is never observed. Preserve this: if
  `unpack_pk_cols` decodes a count outside `1..=4`, store
  `PkColList::single(0)` (matches today's `unwrap_or(0)` fallback
  numerically) so the read remains panic-free until the hook
  rejects. Do not call `validate_pk_cols` here — that's the hook's
  job and it needs `col_defs` we don't have at this layer.

### 2. `catalog/bootstrap.rs:293`

`self.caches.pk_col_of.insert(info.id, 0)` →
`self.caches.pk_col_of.insert(info.id, PkColList::single(0))`. System
tables are bootstrapped with their PK at column 0; the new shape
preserves that exactly.

### 3. `catalog/ddl.rs:411` (`create_fk_indices`)

Replace the single-pk_idx skip with an any-PK-column skip:

```rust
let pk_list = self.caches.pk_col_of.get(&table_id)
    .copied().unwrap_or_else(|| PkColList::single(0));
// ...
for (col_idx, cd) in col_defs.iter().enumerate() {
    if cd.fk_table_id == 0 { continue; }
    if pk_list.as_slice().contains(&(col_idx as u32)) { continue; }
    // ... existing index-creation body unchanged
}
```

For single-PK tables `pk_list.as_slice() == [pk_idx]`, so the
`contains` check yields the same outcome as today's `col_idx ==
pk_idx`. For a compound PK every PK column is skipped — the PK
region's storage layer already covers the PK columns, so an FK whose
column is itself part of the PK doesn't need a separate auto-index.

### 4. `catalog/utils.rs:95-103` (`BatchBuilder::physical_col_idx`)

Replace the single-PK reimplementation with the precomputed mapping
that `SchemaDescriptor` already exposes:

```rust
fn physical_col_idx(&self) -> usize {
    self.schema.payload_col_idx(self.curr_col)
}
```

`payload_col_idx` (`schema.rs:317`) reads `payload_to_ci[pi]` — built
once at `SchemaDescriptor::new` to skip *all* PK columns regardless
of count. For single-PK schemas the result is identical to today's
branch; for compound it walks every PK column correctly. No other
batch-builder logic needs to change — `self.curr_col` is already the
dense payload index, exactly what `payload_col_idx` takes.

## Already in place (do not rebuild)

- `pack_pk_cols` / `unpack_pk_cols` / `PkColList` / `validate_pk_cols`
  in `catalog/sys_tables.rs`.
- `SchemaDescriptor::payload_col_idx(pi)`,
  `SchemaDescriptor::pk_indices()`, `SchemaDescriptor::pk_columns()`,
  `SchemaDescriptor::payload_columns()`.
- The `apply_pk_col_of` → `hook_table_register` ordering invariant
  (the cache stores a possibly-invalid value briefly; the hook
  rejects the row before any consumer reads the cache for the same
  TID).

## Out of scope

- `catalog/validation.rs:22` (`validate_fk_column` reads target's
  `pk_index_single()` and `target_pk_type`). The FK *target* must
  still be single-PK today; the broader "single-column FK to compound
  parent requires UNIQUE on the named column" rule belongs to the FK
  step in `compound-pk.md`.
- `catalog/hooks.rs:297` (`source_pk_type` for `make_index_schema`).
  This builds the GI/AVI index table's PK from the *source* table's
  PK type as a single `u8` type code; generalising it requires
  reshaping `make_gi_schema` / `make_avi_schema` (compound-PK index
  table) and is part of the Operators / Index step in
  `compound-pk.md`.
- Any planner, DML, or operator work.
- Any change to the on-disk catalog encoding.

## Testing

Rust unit tests (`gnitz-engine`):

- `catalog/tests/ddl_tests.rs`: the existing
  `apply_pk_col_of_invalidates_only_on_change` (and the broader DDL
  tests around `pk_col_of`) must stay green — they assert the
  no-op-on-equal invalidation behavior the new
  `Option<PkColList> != Some(...)` comparison must preserve.
- Add one test in `catalog/tests/ddl_tests.rs` (or a fresh `mod
  pk_col_of` block) constructing a TABLE_TAB row whose packed
  `pk_cols` carries `2..=4` columns, applying it via
  `apply_pk_col_of`, and asserting the cache entry round-trips the
  full list. Pair it with a single-column row and assert the new
  shape equals `PkColList::single(idx)` exactly.
- Add one focused test for `BatchBuilder::physical_col_idx` (in
  `catalog/utils.rs::tests` or `catalog/tests/`) with a 4-column
  schema where `pk_indices = [1, 2]`: writing two payload values
  must land at physical columns `0` and `3`, not `0` and `1`. With
  the helper now delegating to `payload_col_idx`, this test is a
  direct regression guard for any future drift.
- `create_fk_indices` is already exercised by the FK auto-index DDL
  tests; the `pk_list.contains(...)` change must keep them green
  without new tests (single-PK behavior unchanged).

Run `make test`; the full suite must be green before and after. Use
a timeout on every test; diagnose any hang rather than re-running.
