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
into the effective batch (partitioned emits it for downstream views; the Single
variant emits nothing — its `w > 0` branch calls `table.retract_pk_bytes`, which
is read-only, and discards the result, `dag.rs:1830`). That distinction is about
store state; it has nothing to do with maintaining the intra-batch `seen` map,
which both variants need and only one gets right. The Single path's documented
use — a "view store with no INSERT-over-existing row to retract" (`dag.rs:1800-1802`)
— matches no relation that exists today, so it is both buggy and obsolete, not a
deliberate variant worth keeping.

## Why this is dead in production

The Single path fires only for a `StoreHandle::Borrowed` handle that is also
`unique_pk == true` — a combination no production relation has:

- `StoreHandle` has exactly two variants (`dag.rs:20-28`): `Partitioned` (base
  tables and views) and `Borrowed` (system tables, owned by `CatalogEngine`).
- `table_ptr()` is non-null **only** for `Borrowed`; `ptable_ptr()` is non-null
  **only** for `Partitioned` (`dag.rs:40-53`). The dispatch in
  `ingest_returning_effective` (`dag.rs:980-992`) routes to Single
  `enforce_unique_pk` only when `ptable_ptr` is null **and** `table_ptr` is
  non-null — i.e. a `Borrowed` handle.
- `unique_pk()` is true only for `RelationKind::BaseTable { unique_pk: true }`
  (`dag.rs:325-326`). Every base table is registered `Partitioned` (`hooks.rs:218`);
  every view is registered `Partitioned` (`hooks.rs:339`). The only `Borrowed`
  registration in production is for system tables (`bootstrap.rs:314-316`), which
  are `RelationKind::SystemCatalog` (`unique_pk == false`).

So `Borrowed && unique_pk` never co-occurs at runtime. The partitioned variant's
own comment confirms it is the live path: *"this is the sole path now"*
(`dag.rs:1866`).

**Exactly one test reaches the Single `enforce_unique_pk`:**
`engine_tests.rs::test_enforce_unique_pk` (registration `:20`), which registers a
`Borrowed` + `unique_pk: true` `Table` and drives it through `dag.ingest_to_family`
(→ `ingest_returning_effective` → Single dispatch; `dag.rs:937-939, 984-986`). It
covers insert / intra-batch dup / insert+delete but **not** the `+1, -1, +1` case,
so it does not lock in the buggy behaviour.

Three other fixtures register the same `Borrowed` + `unique_pk: true` combination
but **never** call `ingest_to_family` / `ingest_returning_effective` on it, so none
reaches `enforce_unique_pk`; all three seed with `Table::ingest_owned_batch` and
exercise only read/validation paths:

- `wide_pk_validation.rs:64` (`setup_wide_unique`) — tests `validate_unique_indices`.
  Its own doc comment states it bypasses `ingest_to_family`.
- `wide_pk_validation.rs:182` (`wide_pk_seek_family_bytes_resolves_non_pk_col`) —
  tests `seek_family_bytes` (a read path).
- `index_tests.rs:2794` — tests `seek_by_index_range` (a read path).

(The `ingest_to_family` calls at `wide_pk_validation.rs:216,224` are in
`seek_family_bytes_matches_seek_family_narrow`, on a *normally-created, Partitioned,
narrow-PK* table from `create_table(..., true)` at `:207` — they exercise
`enforce_unique_pk_partitioned`, the live path, not the Single one.)

So removing the Single path requires changing exactly one test
(`test_enforce_unique_pk`); the three read-only fixtures keep their `Borrowed`
handles untouched. The wide-PK `PkBuf` keying those fixtures exercise lives in
`validate_unique_indices` / `seek_family_bytes` / `batch_project_index`, **not** in
`enforce_unique_pk` — the dead variant has no wide-PK coverage to preserve.

## Plan: remove the Single path

Per the project rule that no legacy code remains, delete the vestigial variant,
fold the one live test onto the path that actually runs, and add the missing
regression. This eliminates the bug by construction (there is no second
`seen`-handling copy to drift) and makes the intra-batch coverage exercise
production code.

### 1. Delete `enforce_unique_pk` (Single)

Remove `dag.rs:1798-1848` (the doc comment through the function body). **Keep**
`normalize_unique_pk_weights` (`dag.rs:1783-1796`) immediately above it — it is
shared and still called by `enforce_unique_pk_partitioned` (`dag.rs:1874`).

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

`tbl_ptr` is then unused by this function; drop its binding (`dag.rs:977`). The
`debug_assert!` turns any future stray `Borrowed` + `unique_pk` registration into
a loud failure at its first ingest, rather than a silent no-enforcement passthrough.

### 3. Migrate the one live fixture

Only `engine_tests.rs::test_enforce_unique_pk` touches the Single path, and its
current body scans the `Table` it owns directly (`scan(&mut table)`,
`engine_tests.rs:30-38`). That `table` cannot survive a move into a `Partitioned`
handle, so do **not** hand-build a `PartitionedTable` and re-register it (that
re-introduces the moved-`table` scan problem). Rewrite the test on the high-level
`CatalogEngine` API, mirroring the existing `test_ingest_unique_pk_partitioned`
(`engine_tests.rs:203-230`):

```rust
let mut engine = CatalogEngine::open(&dir).unwrap();
let cols = vec![u64_col_def("id"), u64_col_def("val")];
let tid = engine.create_table("public.t", &cols, &[0], true).unwrap(); // unique_pk ⟹ Partitioned
let schema = engine.get_schema(tid).unwrap();
// per case: engine.ingest_to_family(tid, &batch); engine.flush_family(tid);
//           assert_eq!(engine.scan_family(tid).count, expected);
```

Port the existing cases unchanged — their net live-row counts (`1, 2, 3, 3`) are
identical on the partitioned path, because none is a cross-batch upsert, so the
partitioned variant's extra stored-row retraction never fires. Drop the manual
`Table`/`Borrowed` setup, the `scan` closure, and the `:64-68` note (it described
the Single path's non-emission of the stored retraction — irrelevant here). Follow
the existing partitioned test's ingest → `flush_family` → `scan_family` rhythm so
the scan is deterministic. Leave `wide_pk_validation.rs` and `index_tests.rs`
untouched.

### 4. Add the `+1, -1, +1` regression case

Append a case to the rewritten `test_enforce_unique_pk`: a single batch with rows
`+1 K(v)`, `-1 K(v)`, `+1 K(v)` on a fresh PK, ingest + flush, and assert the scan
count rises by exactly one (K survives at net `+1`). On the deleted Single path
this nets to `0`; on the partitioned path it nets to `+1`, so the case is a genuine
guard against re-introducing the `seen`-staleness defect.

For a tighter function-level guard, optionally also add a sibling of the
`test_enforce_unique_pk_partitioned_*` unit tests in `dag.rs` (≈`:2196+`) that
calls `DagEngine::enforce_unique_pk_partitioned(&mut pt, &schema, batch)` on the
same `+1, -1, +1` batch and asserts the returned effective batch nets to `+1` for
`K`. None of the existing partitioned unit tests covers insert→delete→re-insert.

## Folded-in optimization: one map probe instead of two on the surviving path

On `enforce_unique_pk_partitioned`, the `w > 0` insertion bookkeeping does
`seen.get` then an unconditional `seen.insert` (`dag.rs:1906-1912`):

```rust
if let Some(&prev_pos) = seen.get(pkb) {
    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
}
effective.append_batch(&batch, row, row + 1);
seen.insert(PkBuf::from_bytes(pkb), row);   // rebuilds the key on every +1 row
```

`PkBuf` is a `Copy` 81-byte stack value (`manifest.rs:71` — `[u8; 80]` + `len`),
and `seen.get(pkb)` already does a zero-allocation heterogeneous lookup via
`Borrow<[u8]>` (`manifest.rs:104`), so there is **no heap allocation** on this path.
What every `+1` row pays is two hash probes plus, in the unconditional `insert`, an
80-byte stack `PkBuf` build and a bucket write. Fold the lookup and the update into
one `get_mut`, building the key only when the PK is new:

```rust
if let Some(prev_pos) = seen.get_mut(pkb) {
    effective.append_batch_negated(&batch, *prev_pos, *prev_pos + 1);
    *prev_pos = row;
} else {
    seen.insert(PkBuf::from_bytes(pkb), row);
}
effective.append_batch(&batch, row, row + 1);
```

`append_batch(&batch, row, row + 1)` stays **unconditional** (it runs on both arms,
as in the original); the negation, when a prior insertion exists, is still appended
before it. Output is identical to the original for every input (verified on the hit
and miss arms). The saving — one hash probe, one `PkBuf` build, one bucket write —
applies **only to repeated-PK rows within a batch** (intra-batch upserts / dedup);
an all-distinct batch still pays `get_mut` + `insert` and is unchanged. This is a
small, low-risk cleanup, not a hot-path win.

> Leave the `w < 0` branch's `seen.get` + `seen.remove` (`dag.rs:1925-1928`) as-is:
> it removes the key, so `get_mut` buys nothing there.

## Coverage gap: wide-PK enforcement is untested

Because the wide_pk fixtures never reach `enforce_unique_pk`/`_partitioned`, the
wide-PK (`pk_stride > 16`) `PkBuf` keying on the *enforcement* path has no test —
the `test_enforce_unique_pk_partitioned_*` unit tests (`dag.rs:2196+`) all use
narrow `I64`/`U64` PKs. This keying is the very logic the `PkBuf`-over-`u128`
comments on both variants warn about (a `u128` round-trip double-flips a signed PK's
sign bit), so it is worth one direct test: build a Partitioned table with a 24-byte
(3×`u64`) PK and run an intra-batch dedup + cross-batch upsert through
`enforce_unique_pk_partitioned`, asserting the effective batch keys and retracts on
the full 24 bytes. A `Table::ingest_owned_batch`-only fixture like `setup_wide_unique`
cannot cover this — the batch must flow through the enforcement function.

## Simplification: rename the sole survivor

With the Single variant gone, `enforce_unique_pk_partitioned` is the only
enforcement function and the `_partitioned` suffix is vestigial. Renaming it to
`enforce_unique_pk` makes the prose that already calls it that accurate again
(`validation.rs`, `master.rs`, `table.rs:798`, `store.rs:1225`,
`dml.rs:187/295/2021`). Trade-off: it also renames the
`test_enforce_unique_pk_partitioned_*` unit tests and touches those comment sites —
mechanical, but more churn than the removal itself. Optional; fold into the same
change or skip.

Also drop the stale claim in `wide_pk_validation.rs:47` that "the `enforce_unique_pk`
u128 keying panics on a wide PK" — the surviving function keys on `PkBuf` for every
width and would not panic; only the `create_table` stride gate still justifies
`setup_wide_unique`'s direct seeding.

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
`test_enforce_unique_pk`. It is strictly inferior to removal: it patches only the
intra-batch defect (the Single path still never emits the stored-row retraction, so
it stays divergent from the live semantics for cross-batch upserts), it keeps a
production-dead duplicate alive against the no-legacy rule, and it leaves the
wide-PK enforcement gap unaddressed. Recorded only as a fallback.
