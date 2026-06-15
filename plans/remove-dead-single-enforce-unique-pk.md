# Remove the dead Single-store `enforce_unique_pk` path

## The defect

`enforce_unique_pk` (`dag.rs:1807-1848`, the Single-`Table` variant) and
`enforce_unique_pk_partitioned` (`dag.rs:1861-1939`, the `PartitionedTable`
variant) maintain the same intra-batch `seen` map (PK → batch row of the last
`+1` insertion) for dedup. The partitioned variant clears that map when a key is
deleted; the Single variant does not.

Partitioned `w < 0` branch (`dag.rs:1925-1928`):

```rust
if let Some(&prev_pos) = seen.get(pkb) {
    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
    seen.remove(pkb);                 // <-- clears the stale insertion
}
```

Single `w < 0` branch (`dag.rs:1841-1844`) — no `seen` maintenance at all:

```rust
} else if w < 0 {
    // Negative weight = deletion
    effective.append_batch(&batch, row, row + 1);
}
```

For a batch carrying, on one PK `K`, the sequence `+1, -1, +1` (insert, delete,
re-insert), the Single variant produces a net weight of **0** instead of `+1`:

| row | weight | Single `enforce_unique_pk` action | effective rows for `K` |
|-----|--------|------------------------------------|------------------------|
| 0 | `+1` | `seen[K] = 0`; append `+1` | `+1` |
| 1 | `-1` | append `-1`; **`seen` untouched** | `+1, -1` |
| 2 | `+1` | `seen.get(K) = Some(0)` (stale) → `append_batch_negated(0)` = `-1`; append `+1`; `seen[K] = 2` | `+1, -1, -1, +1` |

Net for `K` = `+1 − 1 − 1 + 1 = 0`: the key silently vanishes. `seen` is keyed on
PK bytes only, so the payloads of the three rows do not change the outcome. The
partitioned variant handles the identical input correctly (`+1, -1, +1`, net `+1`)
because `seen.remove` on the delete prevents the stale re-negation.

The divergence is an oversight, not a semantic distinction. The two variants
*intentionally* differ only in whether they emit the **stored-row** retraction
into the effective batch (partitioned emits it for downstream views; Single's doc
comment, `dag.rs:1800-1802`, says retractions "pass through"). That distinction is
about store state; it has nothing to do with maintaining the intra-batch `seen`
map, which both variants need and only one gets right.

## Why this is dead in production

The Single path fires only for a `StoreHandle::Borrowed` handle that is also
`unique_pk == true` — a combination no production relation has:

- `StoreHandle` has exactly two variants (`dag.rs:20-27`): `Partitioned` (base
  tables and views) and `Borrowed` (system tables, owned by `CatalogEngine`).
- `table_ptr()` is non-null **only** for `Borrowed`; `ptable_ptr()` is non-null
  **only** for `Partitioned` (`dag.rs:40-53`). The dispatch in
  `ingest_returning_effective` (`dag.rs:980-992`) routes to Single
  `enforce_unique_pk` only when `ptable_ptr` is null **and** `table_ptr` is
  non-null — i.e. a `Borrowed` handle.
- `unique_pk()` is true only for `RelationKind::BaseTable { unique_pk: true }`
  (`dag.rs:325-326`). Every base table is registered `Partitioned` (`hooks.rs:218`);
  every view is registered `Partitioned` (`hooks.rs:339`). The only `Borrowed`
  registration in production is for system tables (`bootstrap.rs`), which are
  `RelationKind::SystemCatalog` (`unique_pk == false`).

So `Borrowed && unique_pk` never co-occurs at runtime. The partitioned variant's
own comment confirms it is the live path: *"this is the sole path now"*
(`dag.rs:1866`). The Single variant's doc comment ("Used for Single (view)
stores", `dag.rs:1800-1802`) describes a configuration that no longer exists —
views are `Partitioned`.

The only code that reaches Single `enforce_unique_pk` is two test fixtures that
register a `Borrowed` + `unique_pk: true` table and ingest through
`ingest_to_family`:

- `catalog/tests/engine_tests.rs::test_enforce_unique_pk` (registration at
  `engine_tests.rs:20`) — covers insert / intra-batch dup / insert+delete, but
  **not** the `+1, -1, +1` case, so it does not lock in the buggy behavior.
- `catalog/tests/wide_pk_validation.rs` (registrations at `:64`, `:182`; ingests at
  `:216`, `:224`) — exercises the wide-PK (`pk_stride > 16`) `PkBuf` keying that
  avoids a `u128` round-trip.

That keying logic is **identical** in both variants (both key on
`get_pk_bytes` / `PkBuf`), so the wide-PK coverage belongs on the live
partitioned path, not the dead Single one.

## Plan: remove the Single path

Per the project rule that no legacy code remains, delete the vestigial variant
and move its still-useful coverage onto the path that actually runs. This
eliminates the bug by construction (there is no second `seen`-handling copy to
drift) and makes the wide-PK test exercise production code.

### 1. Delete `enforce_unique_pk` (Single)

Remove `dag.rs:1798-1848` (the doc comment through the function body).

### 2. Collapse the dispatch in `ingest_returning_effective`

`unique_pk` relations are always `Partitioned`. Replace `dag.rs:980-992` with:

```rust
let effective_batch = if unique_pk {
    // unique_pk ⟹ Partitioned (base tables; system tables are not unique_pk).
    debug_assert!(
        !ptbl_ptr.is_null(),
        "unique_pk relation {table_id} must be a PartitionedTable",
    );
    if !ptbl_ptr.is_null() {
        let ptable = unsafe { &mut *ptbl_ptr };
        Self::enforce_unique_pk_partitioned(ptable, &schema, batch)
    } else {
        batch
    }
} else {
    batch
};
```

`tbl_ptr` is then unused by this function; drop its binding (`dag.rs:977`) if no
other statement references it.

### 3. Migrate the two test fixtures to a single-partition `PartitionedTable`

`PartitionedTable::new(dir, name, schema, table_id, 1, persistence, 0, 1,
arena_size)` (`partitioned_table.rs:39-49`) builds a single-partition table whose
`ptable_ptr` is non-null, so it routes to `enforce_unique_pk_partitioned`. In both
`engine_tests.rs::test_enforce_unique_pk` and the `wide_pk_validation.rs` tests,
replace the `Table::new(...)` + `StoreHandle::Borrowed(table_ptr)` setup with:

```rust
let pt = PartitionedTable::new(&tdir, "upk", schema, 100, 1,
    crate::storage::Persistence::Ephemeral, 0, 1, SYS_TABLE_ARENA).unwrap();
dag.register_table(100,
    StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt))),
    schema, RelationKind::BaseTable { unique_pk: true }, 0, tdir.clone());
```

The partitioned variant emits the stored-row retraction into the effective batch,
so a cross-batch upsert now yields `(-1 old, +1 new)` instead of the Single path's
insert-only delta. Adjust the assertions accordingly (the `engine_tests.rs:64-68`
note about the Single path not emitting the stored retraction no longer applies —
remove it and assert the retraction is present).

### 4. Add the `+1, -1, +1` regression case

To `test_enforce_unique_pk` (now on the partitioned path), append a fourth case: a
single batch with rows `+1 K(v)`, `-1 K(v)`, `+1 K(v)`, and assert `K` survives at
net weight `+1`. This guards the live path against re-introducing the
`seen`-staleness defect.

## Folded-in optimization: drop the redundant `PkBuf` alloc on the surviving path

On the same `enforce_unique_pk_partitioned`, the intra-batch insertion bookkeeping
re-allocates a `PkBuf` key even when the PK already lives in `seen`
(`dag.rs:1906-1912`):

```rust
if let Some(&prev_pos) = seen.get(pkb) {
    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
}
effective.append_batch(&batch, row, row + 1);
seen.insert(PkBuf::from_bytes(pkb), row);   // re-allocates the key on a hit
```

`seen.get` then `seen.insert` is two hashes plus a fresh `PkBuf::from_bytes` (a heap
allocation) on every `+1` row, including when the key is a repeat. Use a single
`get_mut` and update the position in place on a hit, allocating the key only on the
miss:

```rust
if let Some(prev_pos) = seen.get_mut(pkb) {
    effective.append_batch_negated(&batch, *prev_pos, *prev_pos + 1);
    *prev_pos = row;
} else {
    seen.insert(PkBuf::from_bytes(pkb), row);
}
effective.append_batch(&batch, row, row + 1);
```

The `append_batch(&batch, row, row + 1)` of the new insertion stays
**unconditional** — it runs on both the hit and miss arms (the original appends it
after the `if`). The negation, when a prior insertion exists, is still appended
before it. Effective output is identical to the original for every input; only the
per-hit `PkBuf` allocation and one hash lookup are removed.

> Apply the same `get_mut` shape to the `w < 0` branch's `seen.get` +
> `append_batch_negated` + `seen.remove` (`dag.rs:1925-1928`) only if convenient —
> there the key is being removed, so no allocation is saved; leave it as-is to keep
> the diff minimal.

## Minimal alternative (if removal is deferred)

If the Single path must be retained, the one-line correctness fix is to mirror the
partitioned variant's `seen` maintenance in the `w < 0` branch of
`enforce_unique_pk` (`dag.rs:1841-1844`):

```rust
} else if w < 0 {
    // Negative weight = deletion. Clear any intra-batch insertion of this PK so
    // a later re-insert does not re-negate it (matches the partitioned variant).
    if let Some(&prev_pos) = seen.get(pkb) {
        effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
        seen.remove(pkb);
    } else {
        effective.append_batch(&batch, row, row + 1);
    }
}
```

This still requires adding the `+1, -1, +1` regression case to
`test_enforce_unique_pk`. It is strictly inferior to removal — it keeps a
production-dead duplicate alive and leaves the wide-PK test exercising code that
never runs — and is recorded only as a fallback.
