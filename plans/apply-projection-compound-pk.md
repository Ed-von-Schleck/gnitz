# apply_projection: compound-PK source

## Symptom

`SELECT a, b, payload FROM t` on a compound-PK table (`PRIMARY KEY (a, b)`)
returns a result batch whose payload columns are empty buffers. Iterating
the rows from Python panics with `range end index 8 out of range for slice
of length 0` reading `columns[ci]`.

`SELECT *` works (the wildcard branch returns the source batch unchanged).

## Root cause

`crates/gnitz-sql/src/dml.rs:1137-1138`:

```rust
let new_pk_idx = col_indices.iter().position(|&i| schema.is_pk_col(i)).unwrap_or(0);
let new_schema = Schema { columns: new_cols, pk_cols: vec![new_pk_idx] };
```

`apply_projection` rebuilds the projected schema with `pk_cols: vec![new_pk_idx]`
— a single-PK assumption. For a compound-PK source, only the first PK column
that appears in the projection survives as PK in the new schema; the rest
should become payload columns in the new schema, but the copy loop at
line 1147 skips every column that was a PK in the *source* schema:

```rust
if schema.is_pk_col(old_ci) { continue; }   // skips source-PK cols entirely
```

So source-PK columns that the projection lists are silently dropped:
the new schema declares them as either PK (just one) or payload (the rest,
left as empty `Fixed`), but the copy loop never populates them.

## Fix sketch

1. Compute the new PK column set in the projected schema by collecting *all*
   `col_indices` entries that map to PK columns in the source, preserving
   their declared order. If empty (no PK projected), fall back to the
   `unwrap_or(0)` default already there.
2. For each old-PK column that becomes a payload column in the new schema,
   copy its values out of `src_batch.pks` (decode the right slice based on
   `pk_byte_offset(old_ci)` and the column's `wire_stride()`) into the
   corresponding `Fixed`/`Strings`/`U128s` payload region.
3. For new-PK columns, project the appropriate `PkColumn` variant (assemble
   `Bytes` if the new schema has compound PK, otherwise `U64s`/`U128s`).

## Out of scope here

- ORDER BY (rejected separately in `execute_select`).
- Group/aggregate projections (handled in views, not direct SELECT).
- Any planner-side rewrites of PK columns.
