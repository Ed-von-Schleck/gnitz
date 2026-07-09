# Pre-flight DDL validation in the client — reject bad CREATE input before it panics or corrupts

## Problem

`crates/gnitz-core/src/client.rs` is the catalog gateway for every front end
including non-SQL callers (capi), where the SQL planner's earlier checks are
absent. Its `create_table` / `create_view_chain` entry points validate DDL input
only partially, so malformed column/PK/index specs slip past into either a
client-process panic or silent catalog corruption.

### 1. `create_table` accepts an over-wide column list (null-bitmap corruption)

`MAX_COLUMNS` is 65 (`gnitz_wire::catalog`): the region null bitmap is a single
64-bit word, one bit per payload column, so at most 64 payload + 1 PK column fit.
`create_table` never checks `columns.len()`; the only cap on the path is
`pack_col_id`, which rejects `col_idx >= 512` (`client.rs:100`). A table with
66–511 columns is therefore packed and shipped: the null-bitmap bit for payload
index ≥ 64 overflows the word, silently corrupting nullability for those columns.
`create_view_chain` already guards this (`client.rs:912`, `> MAX_COLUMNS`);
`create_table` must match.

### 2. `create_table` never bounds-checks or de-dups the PK column list

`create_table` checks only the PK *count* (`client.rs:720`,
`1..=PK_LIST_MAX_COLS`). It never verifies each `pk_cols` index is
`< columns.len()`, nor that the list has no duplicates. `pack_pk_cols`
(`catalog.rs:330`) asserts the count and `c < 128` but neither bound-checks
against the column list nor rejects duplicates — so `pk_cols = &[10]` on a
2-column table, or `&[0, 0]`, packs cleanly and writes a physically impossible
PK into `TABLE_TAB`.

### 3. `create_table` never bounds-checks unique-index column indices → panic

For each inline `UNIQUE` index, `create_table` calls `validate_pk_col_list`
(structure only — `catalog.rs:284`, caps index at `< 128`) then immediately
indexes `columns[c as usize].type_code` (`client.rs:767`). A `col_indices`
entry in `[columns.len(), 128)` (e.g. index 5 on a 3-column table) triggers an
uncatchable slice-index **panic** in the client process before any payload is
built.

### 4. `create_view_chain` never checks PK non-empty / limit / duplicates → panic

The per-view PK loop (`client.rs:919-935`) validates nullability and bounds of
each index against `output_columns`, but an **empty** `pk_cols` skips the loop
entirely, and a list longer than `PK_LIST_MAX_COLS` (4) is never rejected. Both
reach `pack_pk_cols(&pv.pk_cols)` (`client.rs:1055`), whose count assertion
**panics** on an empty or over-long list. Duplicate PK indices pass the loop and
pack silently into `VIEW_TAB`.

## Change

`Schema::validate_pk_cols(pk_cols: &[usize], ncols: usize)` (`types.rs:218`)
already enforces the exact rule set both entry points need — non-empty,
`≤ PK_LIST_MAX_COLS`, every index `< ncols`, no duplicates. Route both PK checks
through it, add the missing column-count and unique-index bounds guards, and keep
every check ahead of the first id allocation so a rejection leaves no residue.

### `create_table` (`client.rs:700`)

Replace the count-only PK check (`client.rs:720-726`) and add the column-limit
guard at the top of the body, before `alloc_table_id`:

```rust
        let schema_name = canon_name(schema_name);
        let table_name = canon_name(table_name);
        if columns.len() > gnitz_wire::MAX_COLUMNS {
            return Err(ClientError::ServerError(format!(
                "Table has {} columns, exceeding the {}-column limit",
                columns.len(),
                gnitz_wire::MAX_COLUMNS
            )));
        }
        let pk_indices: Vec<usize> = pk_cols.iter().map(|&c| c as usize).collect();
        Schema::validate_pk_cols(&pk_indices, columns.len())
            .map_err(|e| ClientError::ServerError(format!("create_table: invalid PRIMARY KEY: {e}")))?;

        let new_tid = self.conn.alloc_table_id()?;
```

Bounds-check each unique-index column against the column list (`client.rs:766`),
inside the existing loop, before the `columns[c]` type read:

```rust
        for spec in unique_indexes {
            gnitz_wire::validate_pk_col_list(spec.col_indices)
                .map_err(|e| ClientError::ServerError(format!("create_table: unique index: {e}")))?;
            for &c in spec.col_indices {
                if c as usize >= columns.len() {
                    return Err(ClientError::ServerError(format!(
                        "create_table: unique index '{}' column {c} out of range ({} columns)",
                        spec.name,
                        columns.len()
                    )));
                }
                validate_index_col_type(columns[c as usize].type_code)?;
            }
            index_ids.push(self.conn.alloc_index_id()?);
        }
```

### `create_view_chain` (`client.rs:881`)

Replace the per-view PK loop (`client.rs:919-935`) with a `validate_pk_cols`
call followed by the nullability check. `validate_pk_cols` has already
bounds-checked every index, so the nullability loop indexes `output_columns`
directly:

```rust
            let pk_indices: Vec<usize> = pv.pk_cols.iter().map(|&c| c as usize).collect();
            Schema::validate_pk_cols(&pk_indices, pv.output_columns.len())
                .map_err(|e| ClientError::ServerError(format!("View '{}' PRIMARY KEY is invalid: {e}", pv.name)))?;
            for &p in &pv.pk_cols {
                // The view PK region is the leading `k` columns and carries no null
                // bitmap, so a nullable PK slot is internally inconsistent.
                if pv.output_columns[p as usize].is_nullable {
                    return Err(ClientError::ServerError(format!(
                        "View '{}' PRIMARY KEY column at index {p} must not be nullable",
                        pv.name
                    )));
                }
            }
```

## Tests

- `create_table` with `columns.len()` = 66 returns `Err(... "exceeding the 65-column limit" ...)`
  and consumes **no** table id (assert the server's table-id sequence is
  unchanged) — proving the guard runs before `alloc_table_id`.
- `create_table` with `pk_cols = &[10]` on a 2-column table returns
  `Err(... "index out of range" ...)`; `pk_cols = &[0, 0]` returns
  `Err(... "duplicates" ...)`.
- `create_table` with an inline unique index `col_indices = &[5]` on a 3-column
  table returns `Err(... "out of range" ...)` — no panic (regression: today it
  panics indexing `columns[5]`).
- `create_view_chain` with an empty `pk_cols`, and separately with a 5-element
  `pk_cols`, each returns `Err(... "PRIMARY KEY is invalid" ...)` — no panic
  (regression: today `pack_pk_cols` asserts). Duplicate `pk_cols = &[0, 0]`
  returns the duplicates error.
