# Blob columns — DML payload helpers

`ColData` has four variants: `Fixed` (fixed-stride byte buffer),
`Strings` (`Vec<Option<String>>`), `U128s` (`Vec<u128>`), and `Bytes`
(`Vec<Option<Vec<u8>>>`, for `BLOB` columns). Three row-copying helpers
in `gnitz-sql/src/dml.rs` enumerate the first three and fall through
to `unreachable!()` on `Bytes`. Any DML touching a table that has a
`BLOB` payload column therefore aborts the client process at that
`unreachable!()`.

The three sites:

```rust
// dml.rs::copy_batch_row (line ~1071)
match (&src.columns[ci], &mut dst.columns[ci]) {
    (ColData::Fixed(s),   ColData::Fixed(d))   => d.extend_from_slice(&s[i*stride..(i+1)*stride]),
    (ColData::Strings(s), ColData::Strings(d)) => d.push(s[i].clone()),
    (ColData::U128s(s),   ColData::U128s(d))   => d.push(s[i]),
    _ => unreachable!("mismatched ColData variants for column {}", ci),
}
```

```rust
// dml.rs::write_set_columns (line ~1286)
match (&current.columns[ci], &mut dst.columns[ci]) {
    (ColData::Fixed(s),   ColData::Fixed(d))   => d.extend_from_slice(&s[row_idx*stride..(row_idx+1)*stride]),
    (ColData::Strings(s), ColData::Strings(d)) => d.push(s[row_idx].clone()),
    (ColData::U128s(s),   ColData::U128s(d))   => d.push(s[row_idx]),
    _ => unreachable!("mismatched ColData variants for column {}", ci),
}
```

```rust
// dml.rs::client_side_merge_do_update (line ~349)
match (&existing_batch.columns[ci], &mut out.columns[ci]) {
    (ColData::Fixed(s),   ColData::Fixed(d))   => d.extend_from_slice(&s[0..stride]),
    (ColData::Strings(s), ColData::Strings(d)) => d.push(s[0].clone()),
    (ColData::U128s(s),   ColData::U128s(d))   => d.push(s[0]),
    _ => unreachable!("mismatched ColData variants for column {}", ci),
}
```

Crashes when any of these helpers runs:

- `copy_batch_row` — filtered `SELECT` / `DELETE` that copies matching
  rows into a new batch (e.g. `DELETE FROM t WHERE ...`,
  `SELECT * FROM t WHERE ...` with a residual filter).
- `write_set_columns` — every `UPDATE t SET ... WHERE ...` on a table
  with a `BLOB` column, even when the BLOB column itself is not in
  the SET list (the helper iterates *all* payload columns and copies
  through the unchanged ones).
- `client_side_merge_do_update` — `INSERT ... ON CONFLICT DO UPDATE`
  on a table with a `BLOB` column for any row whose PK already exists
  in the store.

`INSERT` without `ON CONFLICT`, full-table `SELECT *` (no projection
copy), and `DROP TABLE` paths do not exercise these helpers and stay
working.

## Fix

Add the missing `Bytes` arm to each match. The variant carries
`Vec<Option<Vec<u8>>>`, so the per-row copy is a single
`d.push(s[i].clone())` — the same shape as the `Strings` arm:

```rust
// copy_batch_row
match (&src.columns[ci], &mut dst.columns[ci]) {
    (ColData::Fixed(s),   ColData::Fixed(d))   => d.extend_from_slice(&s[i*stride..(i+1)*stride]),
    (ColData::Strings(s), ColData::Strings(d)) => d.push(s[i].clone()),
    (ColData::Bytes(s),   ColData::Bytes(d))   => d.push(s[i].clone()),
    (ColData::U128s(s),   ColData::U128s(d))   => d.push(s[i]),
    _ => unreachable!("mismatched ColData variants for column {}", ci),
}
```

```rust
// write_set_columns
match (&current.columns[ci], &mut dst.columns[ci]) {
    (ColData::Fixed(s),   ColData::Fixed(d))   => d.extend_from_slice(&s[row_idx*stride..(row_idx+1)*stride]),
    (ColData::Strings(s), ColData::Strings(d)) => d.push(s[row_idx].clone()),
    (ColData::Bytes(s),   ColData::Bytes(d))   => d.push(s[row_idx].clone()),
    (ColData::U128s(s),   ColData::U128s(d))   => d.push(s[row_idx]),
    _ => unreachable!("mismatched ColData variants for column {}", ci),
}
```

```rust
// client_side_merge_do_update — existing-row branch
match (&existing_batch.columns[ci], &mut out.columns[ci]) {
    (ColData::Fixed(s),   ColData::Fixed(d))   => d.extend_from_slice(&s[0..stride]),
    (ColData::Strings(s), ColData::Strings(d)) => d.push(s[0].clone()),
    (ColData::Bytes(s),   ColData::Bytes(d))   => d.push(s[0].clone()),
    (ColData::U128s(s),   ColData::U128s(d))   => d.push(s[0]),
    _ => unreachable!("mismatched ColData variants for column {}", ci),
}
```

A fourth `apply_projection` site at `dml.rs:1139` also enumerates the
three variants but the wildcard arm is `_ => {}` rather than
`unreachable!()`, so it silently drops a `Bytes` column from a
projected SELECT. Add the same `Bytes` arm here for completeness:

```rust
// apply_projection
match (&src_columns[old_ci], &mut new_batch.columns[new_ci]) {
    (ColData::Fixed(src),   ColData::Fixed(dst))   => dst.extend_from_slice(src),
    (ColData::Strings(src), ColData::Strings(dst)) => dst.extend(src.iter().cloned()),
    (ColData::Bytes(src),   ColData::Bytes(dst))   => dst.extend(src.iter().cloned()),
    (ColData::U128s(src),   ColData::U128s(dst))   => dst.extend(src.iter().copied()),
    _ => {}
}
```

## Touchpoints

- `crates/gnitz-sql/src/dml.rs::copy_batch_row` (line ~1071)
- `crates/gnitz-sql/src/dml.rs::write_set_columns` (line ~1286)
- `crates/gnitz-sql/src/dml.rs::client_side_merge_do_update`
  (line ~349)
- `crates/gnitz-sql/src/dml.rs::apply_projection` (line ~1139)

No new helpers, no API changes — each fix is a single match arm.

## Why the `unreachable!()` doesn't catch this in tests today

`ColData::Bytes` is only constructed when the schema declares a
`BLOB` column. The SQL planner does emit `BLOB` columns (it round-trips
through `TypeCode::Blob` in `sql_type_to_typecode`), but the e2e and
integration tests rarely combine BLOB payloads with filtered
`UPDATE`/`DELETE` or `ON CONFLICT DO UPDATE`. The `unreachable!()`
arms then look reachable on inspection but get no coverage. The fix
should be paired with tests exercising each helper against a BLOB
column.

## Testing

A single small fixture exercises every site. Schema:
`CREATE TABLE t (id U64 PRIMARY KEY, payload BLOB, tag I64)`.

- `INSERT INTO t VALUES (1, X'deadbeef', 10), (2, X'cafebabe', 20)`.
- `UPDATE t SET tag = 99 WHERE id = 1` — exercises `write_set_columns`
  (BLOB column is unchanged but copied through).
- `INSERT INTO t VALUES (1, X'feedface', 11) ON CONFLICT (id) DO
  UPDATE SET tag = excluded.tag` — exercises
  `client_side_merge_do_update` (existing PK 1, BLOB column preserved
  from existing row).
- `DELETE FROM t WHERE tag > 50` — exercises `copy_batch_row`.
- `SELECT payload, tag FROM t` — exercises `apply_projection` (the
  silent-drop case): output BLOB column must contain the same bytes
  as the source, not an empty Vec.

All four queries must complete without panicking and the resulting
table state must match the SQL semantics of each statement.
