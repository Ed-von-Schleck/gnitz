# Lift the wide-PK admission gate (CREATE TABLE)

## 1. Goal
Admit primary keys whose total physical stride exceeds 16 bytes (up to
`MAX_PK_BYTES`), and the sub-16 non-power-of-two strides currently rejected
(e.g. `(U64, U32)` = 12). This enables multi-column compound PKs such as three
`U64` columns (stride 24) while keeping the 4-column cap. A single-column PK
stays bound to ≤ 16 bytes (`is_pk_eligible` admits only fixed-width integers,
`U128`, and `UUID`), so any stride > 16 is necessarily a compound PK.

Today both gates reject every stride outside `{1, 2, 4, 8, 16}`, so a valid
`(U64, U32)` stride-12 PK is rejected even though it is narrow, and no
stride-24 PK can be created at all.

## 2. Prerequisite (gating condition)
The base-table execution layer must handle wide PKs end to end — point lookup,
retraction, UPSERT/unique-PK identification, gather, merge/compaction, exchange
routing, and secondary indexing. That layer is in place: the cursor
(`read_cursor.rs`), memtable, shard reader/index, merge/compact, reduce,
exchange, master/worker, and catalog store all carry explicit `pk_stride > 16`
byte-path branches, and `ops/index.rs` already sizes composite index keys
against `MAX_PK_BYTES` rather than the narrow cap. The narrow `u128` fast path
(`find_lower_bound`, `seek`, `find_row_index`) is only ever reached behind a
`pk_is_fast()` (single unsigned column) guard; every signed, compound, or wide
PK dispatches to the byte-keyed sibling.

**Confirm the wide-PK unit tests and `test_compound_pk.py` round-trip cases
pass before landing §3.** Lifting the gate is the last step — it is what makes
the wide-PK execution paths reachable end to end — not the first.

## 3. The change
Both admission gates compute `pk_stride` as the sum of per-column
`wire_stride()` and reject anything outside `{1, 2, 4, 8, 16}`. Replace each
check with a continuous bound. The 4-column count check (`1..=4`, already
present in both gates) caps a valid PK at 64 bytes (four `U128`); `MAX_PK_BYTES`
(80) is the absolute ceiling, enforced as defence in depth.

### 3a. Catalog — `crates/gnitz-engine/src/catalog/sys_tables.rs::validate_pk_cols`
This gate also guards the `raw_store_ingest` → `TABLE_TAB` path against a crafted
oversized PK encoding, so it must keep validating stride independently of the
planner. Replace the comment block and check (currently lines ~96–110):

```rust
    // The PK region must fit MAX_PK_BYTES. Strides ≤ 16 widen to a `u128` fast
    // key via `storage::batch::widen_pk_le`; wider compound PKs (stride > 16)
    // route through the byte-path accessors (`get_pk_bytes` / `compare_pk_bytes`)
    // — every narrow `u128` seek is guarded by `pk_is_fast()`. The 4-column cap
    // above bounds a valid PK at 64 bytes (four `U128`); MAX_PK_BYTES is the
    // ceiling, defending the catalog worker against a crafted `raw_store_ingest`
    // into `TABLE_TAB` whose decoded PK list packs an oversized region.
    // `pk_stride == 0` is unreachable once `is_pk_eligible` passed (every
    // eligible type is ≥ 1 byte) but is rejected explicitly as defence in depth.
    let pk_stride: usize = cols.iter()
        .map(|&c| gnitz_wire::wire_stride(col_defs[c as usize].type_code))
        .sum();
    if pk_stride == 0 || pk_stride > gnitz_wire::MAX_PK_BYTES {
        return Err(format!(
            "Primary Key total stride must be 1..={} bytes, got {pk_stride}",
            gnitz_wire::MAX_PK_BYTES
        ));
    }
```

### 3b. SQL planner — `crates/gnitz-sql/src/planner.rs::execute_create_table`
The planner pre-check that names the offending column for non-PK-eligible
types (the `is_pk_eligible` loop) stays unchanged; this only changes the stride
bound. Replace the comment block and check (currently lines ~282–293):

```rust
    // The PK region must fit MAX_PK_BYTES. Strides ≤ 16 widen to a `u128` fast
    // key; wider compound PKs (stride > 16, e.g. three `U64`s = 24) route
    // through the byte-path cursor/merge accessors. The 4-column cap above
    // bounds a valid PK at 64 bytes; MAX_PK_BYTES is the ceiling. The engine
    // re-validates the same bound in `validate_pk_cols`.
    let pk_stride: usize = pk_indices.iter()
        .map(|&i| cols[i as usize].type_code.wire_stride())
        .sum();
    if pk_stride == 0 || pk_stride > gnitz_core::MAX_PK_BYTES {
        return Err(GnitzSqlError::Unsupported(format!(
            "PRIMARY KEY total stride must be 1..={} bytes, got {pk_stride}",
            gnitz_core::MAX_PK_BYTES
        )));
    }
```

`gnitz_core` re-exports `MAX_PK_BYTES`; the planner already imports from it.
The new message still contains the substring `stride`, so any test asserting on
that substring for the *remaining* rejections (count > 4, STRING/BLOB/float)
keeps passing.

## 4. Verification
- **Unit — `gnitz-sql/tests/planner_create_table.rs`:** convert
  `test_compound_pk_stride_12_rejected` and `test_compound_pk_stride_24_rejected`
  to acceptance cases asserting the created schema's `pk_stride()` (12 and 24).
  Keep `test_compound_pk_five_columns_rejected` (count > 4),
  `test_compound_pk_string_column_rejected`, and `test_pk_float_column_rejected`
  green — these reject before/independent of the stride bound. Add a stride-64
  acceptance case (four `U128`) and a `> MAX_PK_BYTES` rejection driven through
  a crafted decoded PK list at the `validate_pk_cols` level (a 4-column SQL PK
  cannot exceed 64, so the ceiling is only reachable via the catalog path).
  Build warning-free.

- **E2E (`GNITZ_WORKERS=4`, `crates/gnitz-py/tests/test_compound_pk.py`):**
  convert `test_compound_pk_stride_12_rejected`,
  `test_compound_pk_stride_24_rejected`, and
  `test_compound_pk_uuid_plus_companion_rejected_by_stride` to
  acceptance + round-trip: create the table, then INSERT/SELECT/DELETE,
  including rows that share the first 16 bytes but differ past byte 16 (drives
  the wide `compare_pk_bytes` tie-break) and, for stride-12, rows that differ
  only in the trailing `U32`. The existing adversarial-consolidation,
  multi-worker-routing, and signed-first-column tests should be extended to a
  stride-24 table, since those paths were previously only exercised at
  stride ≤ 16.

- **E2E — wide-PK secondary index:** add a case that creates a secondary index
  on a table with a 16-byte (`U128`/`UUID`) or stride-24 PK and round-trips an
  indexed lookup. `ops/index.rs` already builds the composite index key
  (`indexed_col + source_pk`) against `MAX_PK_BYTES`, so this is reachable once
  the gate is lifted; the test guards that the index path stays correct for
  wide source PKs. (`test_index_on_compound_pk_narrow_source_via_sql` already
  covers the narrow-source case.)
