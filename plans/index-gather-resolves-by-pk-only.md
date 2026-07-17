# Index gather resolves by source PK alone — wrong rows on non-`unique_pk` base tables

## 1. The bug

An index entry's key is `[promoted indexed cols… ‖ source PK cols…]` (`make_index_schema`,
`schema.rs:1468-1497`) — it identifies a row by **(indexed span, PK)**. The gather that turns
index entries back into base rows throws the span away and resolves by **PK alone**:

```rust
// catalog/store_io.rs:241-264
fn resolve_source_pks(handle: &StoreHandle, src_schema: SchemaDescriptor, pks: &[PkBuf]) -> Option<Batch> {
    ...
    let mut acc = Batch::with_schema(src_schema, pks.len());
    for pk in pks {
        if src_cursor.advance_to_exact_live(pk.pk_bytes()) {
            let w = src_cursor.current_weight;
            src_cursor.copy_current_row_into(&mut acc, w);
        }
    }
    (acc.count > 0).then_some(acc)
}
```

`advance_to_exact_live` positions at the **first** `(PK, payload)` group with that PK and emits
exactly that one group. Its own doc states the assumption the code rests on — as a fact, not a
checked invariant:

> `store_io.rs:236-240` — *"`acc` is sized to `pks.len()` — a tight upper bound, **since base PKs
> are unique and each carries one indexed value**"*

**Base PKs are not always unique.** `RelationKind::BaseTable { unique_pk: bool }`
(`query/dag/mod.rs:96`) has both flavors, and `unique_pk()` (`dag/mod.rs:~192`) is
`matches!(self.kind, RelationKind::BaseTable { unique_pk: true })`. SQL `CREATE TABLE` hardcodes
`true` (`gnitz-sql/src/plan/ddl.rs:580`), but the binary client exposes the flag — `gnitz-py`'s
`create_table(..., unique_pk=False)` is used across the suite
(`gnitz-py/tests/test_dbsp_ops.py:214,496,552,611,658`; `test_unique_pk.py:90` — *"unique_pk=False
table accumulates weights"*; `test_tpch.py` creates LINEITEM/ORDERS that way). On such a table
pushes **accumulate** instead of upserting, so two live rows may share a PK and differ in payload
— including in an indexed column.

**Nothing prevents indexing such a table.** `CREATE INDEX` validation checks the identifier,
column list, and types (`gnitz-core/src/client.rs:580-602`); `validate_index_registration` /
`hook_index_register` (`catalog/hooks.rs:581-600`) resolve the owner without consulting its kind.
The index write path is fully live for them and `validation.rs:195` says so explicitly: *"Empty on
non-unique_pk tables (no `enforce_unique_pk`)"*.

Both index read paths gather this way — `seek_by_index` (`store_io.rs:223`) and
`seek_by_index_range` (`store_io.rs:393`).

## 2. Reachable failures

Table `t(pk, val)`, `unique_pk = false`, index on `val`. Push `(pk=1, val=5)` then `(pk=1, val=7)`
— two live rows, weight 1 each. The index holds `(5‖1)` and `(7‖1)`.

- **Point seek returns the wrong row.** `WHERE val = 7` → the walk yields entry `(7‖1)` → `pks =
  [1]` → `advance_to_exact_live(1)` lands on the **first** group at PK 1, which is `(1, val=5)`
  (rows sort by `(PK, payload)`, and `5 < 7`). The client receives `(1, 5)` for a `val = 7`
  query. `WHERE val = 5` happens to be right — by sort-order luck.
- **Range seek loses a row and doubles a weight.** `WHERE val BETWEEN 5 AND 7` → entries `(5‖1)`
  and `(7‖1)` → `pks = [1, 1]` → two probes, both landing on `(1, 5)`.
  `advance_to`'s doc names this exact case: *"At `key == current_pk` the lower bound of `key` can
  be a row this cursor already consumed (**an earlier payload at the same PK**) … takes the
  absolute reposition + rebuild"* (`read_cursor/mod.rs:272-275`). Emitted: `{(1,5): w=2}`.
  A full scan + filter emits `{(1,5): w=1, (1,7): w=1}`. **`SUM(val)` → 10 instead of 12.**

Both are silent — no error, no log. Note the loss occurs even for a single collected PK: one
probe can never emit a second payload group.

Ghosts and per-`(PK,payload)` weight are **not** implicated: a base ghost's index entry is a
ghost too and both sides filter on `current_weight > 0`, and `copy_current_row_into(&mut acc, w)`
already carries the net weight. The defect is strictly **multi-payload-per-PK**.

## 3. Fix: resolve by the index entry's span, not by its PK

The gather must re-apply the predicate the index walk applied. Both walks already know it:
`seek_by_index` matches a **prefix** (`opk_prefix`, `store_io.rs:~179`); `seek_by_index_range`
matches a **half-open range** (`start`/`end` cut keys, `store_io.rs:333-360`). Both are byte
comparisons over the same projected span, so one gather serves both.

Replace `resolve_source_pks` with:

```rust
/// Resolve collected source PKs against the base table, emitting every live row of each PK
/// group whose indexed span satisfies `span_ok` — the same predicate the index walk applied.
/// Resolving by PK alone is wrong on a non-`unique_pk` table, where one PK can carry several
/// live payloads with different indexed values: the entry identifies (span, PK), not PK.
///
/// `pks` must be ascending and **deduplicated** (a PK group is walked once). `cap` is the
/// collected entry count — an exact upper bound on emitted rows, one per matching entry.
fn resolve_source_pks_matching(
    cursor: &mut ReadCursor,
    src_schema: SchemaDescriptor,
    pks: &[PkBuf],
    cap: usize,
    spec: &IndexKeySpec,
    span_ok: impl Fn(&[u8]) -> bool,
) -> Option<Batch> {
    if pks.is_empty() {
        return None;
    }
    let mut acc = Batch::with_schema(src_schema, cap);
    let mut span = PkBuf::empty(0);
    for pk in pks {
        cursor.advance_to(pk.pk_bytes());
        cursor.for_each_pk_group_row(pk.pk_bytes(), |c| {
            if c.current_weight > 0 {
                let (mb, row) = /* c.current_row_source() */;
                if spec.key_bytes(&mb, row, &mut span) && span_ok(span.pk_bytes()) {
                    c.copy_current_row_into(&mut acc, c.current_weight);
                }
            }
        });
    }
    (acc.count > 0).then_some(acc)
}
```

- `for_each_pk_group_row` (`read_cursor/mod.rs:429`) walks the equal-PK group at
  `(PK, payload)`-sub-group granularity — exactly the rows one PK can carry. `advance_to` seeds
  it and is backward-capable, so the ascending sweep is preserved.
- `IndexKeySpec::key_bytes(&mb, row, &mut buf) -> bool` re-projects a row's indexed span through
  the **same** spec that wrote the entry (`schema.rs:929` `write_span` →
  `encode_pk_column_promoted`), so the comparison is byte-exact against stored keys and needs no
  type dispatch. It is already used this way by the unique pre-flight
  (`worker/mod.rs:~1471`: `spec.key_bytes(&mb, row, &mut keybuf)`); a `false` return (a NULL
  indexed column, which the index skips — `ingest.rs:427-431`) correctly emits nothing.
- **Dedup**: `pks.dedup()` after the existing `sort_unstable()`. `seek_by_index`'s
  skip-the-sort fast path (full-arity equality, `store_io.rs:~220`) must still dedup — a
  non-`unique_pk` table can repeat a PK there too; the walk yields those entries adjacent, so
  `dedup()` alone suffices and the sort stays skipped.
- **`cap`**: pass the pre-dedup entry count. One entry per matching row makes it exact, restoring
  the "never grows row by row" property the old sizing claimed.

Call sites: `seek_by_index` passes `spec.prefix(natives.len())` and
`span_ok = |s| s == opk_prefix`; `seek_by_index_range` passes `spec.prefix(n_eq + 1)` and
`span_ok = |s| s >= &start[..prefix_len] && end.as_ref().is_none_or(|e| s < &e[..prefix_len])`.
Both already have `spec`, `prefix_len`, and the cut keys in scope.

**On a `unique_pk` table this is behaviour-identical and costs the same**: the PK group holds one
row, whose span produced the entry, so `span_ok` is always true and the walk emits exactly what
the single probe emitted. There is no fast/slow split to maintain — one path, correct for both
kinds.

## 4. Tests

- **Rust unit** (`gnitz-engine`, alongside the existing index tests in `catalog/tests/`): a
  `unique_pk = false` base table with `(pk=1, val=5)` and `(pk=1, val=7)` both live, indexed on
  `val` —
  - `seek_by_index(val = 7)` returns exactly `(1, 7)` at weight 1 (today: `(1, 5)`);
  - `seek_by_index_range(val ∈ [5, 7])` returns `{(1,5): w=1, (1,7): w=1}` (today:
    `{(1,5): w=2}`), asserted **on weights**, and equal to a full scan filtered by the same
    predicate;
  - three payloads at one PK with only the middle in range returns only that row;
  - a NULL indexed column at a shared PK is emitted by neither path;
  - a `unique_pk = true` table returns byte-identical results to the pre-change gather
    (regression guard for the merged path).
- **Rust unit**: a PK whose every payload fails `span_ok` contributes no rows and does not
  terminate the walk early (guards the dedup + `cap` sizing).
- **E2E** (`gnitz-py`, `GNITZ_WORKERS=4`): over a `unique_pk=False` table with a secondary index
  and rows sharing PKs across partitions, `SELECT * FROM t WHERE val = k` and
  `WHERE val BETWEEN a AND b` return the same rows and weights as the same queries with the index
  dropped (forcing a scan). The W=4 run also guards the master's raw-append merge, which is only
  sound because per-worker result sets are disjoint.
- `make verify` + `make e2e WORKERS=4` green.

## 5. Sequencing

1. `resolve_source_pks` → `resolve_source_pks_matching` (`catalog/store_io.rs`), both call sites
   converted (`seek_by_index` `:223`, `seek_by_index_range` `:393`), sort+dedup at both, `cap`
   from the entry count; the unit tests above.
2. E2E; `make verify` + `make e2e WORKERS=4`.
