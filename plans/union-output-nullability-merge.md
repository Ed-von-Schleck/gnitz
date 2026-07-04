# Union output-schema nullability merge (null-comparator honesty)

`op_map`/`op_union` and every consolidation downstream pick their row comparator from
`SchemaDescriptor::payload_cmp`. A schema whose payload columns are **all** non-nullable
fixed-width ints ≤8 bytes classifies as `PayloadCmpKind::FixedIntNonnull`, whose comparator
`compare_rows_fixedint_nonnull` (`storage/repr/columnar.rs`) orders by raw payload bytes and
**skips null-bitmap reads entirely** (its doc: "skipping null-bitmap reads"). Every other
shape falls back to `Generic` / `compare_rows`, which treats `null == null` regardless of the
bytes under the null bit.

The `OpNode::Union` emit arm (`query/compiler/emit.rs`) inherits its output register schema
verbatim from `PORT_IN_A`:

```rust
reg_meta[reg_id as usize] = RegisterMeta::delta(reg_meta[in_a as usize].schema);
```

Both union inputs share a physical layout (equal type codes and sizes — the merge reads
payload at fixed widths), but their per-column **nullability may differ**. When `PORT_IN_A` is
non-nullable and `PORT_IN_B` is nullable, the inherited schema understates nullability: an
all-fixed-int output is stamped `FixedIntNonnull` even though `PORT_IN_B` feeds genuine NULL
rows. The downstream consolidation — the `distinct` after `UNION [DISTINCT]`, the sink
integrate under `UNION ALL`, or the `positive_part` inside `positive_diff` — then compares
those NULL rows with the null-blind fast comparator.

## Why this corrupts results

Consolidation groups by `(PK, payload)`. Set-op leaves carry a synthetic content-hash PK
(`reindex_hash_row`) that mixes a per-column `is_null` marker and **skips the payload bytes of
a null column**, so two logically-NULL rows co-hash to one PK. Their payload bytes, however,
are compared by the row comparator:

- `Generic`: `null == null` by the null bit → the two NULL rows coalesce (correct).
- `FixedIntNonnull`: orders by raw payload bytes → if those bytes differ, the rows are treated
  as **distinct elements** and fail to coalesce, so `UNION DISTINCT` emits two NULL rows where
  set semantics demand one (and `UNION ALL` splits a weight-2 duplicate into two weight-1
  rows).

Null-vs-non-null-zero can **never** collide (the content hash mixes the `is_null` marker, so a
NULL and a genuine `0` land on different PKs) — so this is strictly a *fail-to-coalesce* bug,
not a false-merge.

Whether the differing bytes arise: the SQL and Python clients canonically zero a NULL integer
slot (`codec/colwrite.rs` `append_null`; `gnitz-py` `append_null_column`), and operators copy
payload regions verbatim, so via those clients two NULL rows carry identical (zero) bytes and
even the null-blind comparator coalesces them. The raw **C API** `gnitz_batch_append_row`
(`gnitz-capi`) copies the caller's column bytes verbatim **without consulting the null mask**,
so a client may set a null bit over non-zero data bytes. The engine is otherwise deliberately
robust to this ("the null bit governs" — the `NullGarbage` merge test constructs non-zero
bytes under a set null bit and asserts correct decoding); the `Union` arm is the one spot that
leaks that guarantee, because it can hand null-carrying data to the null-blind comparator.

## Shared chokepoint: set-ops *and* outer joins

`positive_diff(minuend, subtrahend)` (`gnitz-core` circuit builder) expands to
`positive_part(union(negate(subtrahend), minuend))` — it routes through `OpNode::Union`. It
backs both `INTERSECT`/`EXCEPT` and the `LEFT`/`RIGHT`/`FULL` outer-join null-fills
(`ν = positive_part(P − π_P(inner))`). The outer-join null-fill's weight-exact cancellation
depends on the null-aware content comparator to net a preserved NULL-keyed row against its
re-keyed inner projection; a `FixedIntNonnull` misclassification of that intermediate union
would defeat it. Fixing the single `Union` arm therefore restores null-comparator correctness
across `UNION`/`UNION ALL`, `INTERSECT`/`EXCEPT`, and the three outer joins at once.

## Fix

In the `OpNode::Union` arm, when `PORT_IN_B` is present, build the output schema by OR-ing the
two inputs' per-column nullability instead of inheriting `PORT_IN_A`'s verbatim. Types, sizes,
and PK indices are identical by construction, so only the nullability bits move — and only ever
toward *more* nullable, which is always sound (`Generic` is correct for every input;
`FixedIntNonnull` is a same-nullability-only optimization). `SchemaDescriptor::new` recomputes
`payload_cmp` from the merged columns, so the reclassification is automatic.

```rust
gnitz_wire::OpNode::Union => {
    let in_a = in_regs.get(&PORT_IN_A).copied().unwrap_or(0);
    let has_b = in_regs.contains_key(&PORT_IN_B);
    let in_b = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
    let a_schema = reg_meta[in_a as usize].schema;
    let out_schema = if has_b {
        union_nullability_merge(&a_schema, &reg_meta[in_b as usize].schema)
    } else {
        // O(1) identity pass-through (op_union returns PORT_IN_A unchanged); no B
        // schema to merge.
        a_schema
    };
    reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
    builder.add_union(in_a as u16, in_b as u16, has_b, reg_id as u16);
}
```

```rust
/// Both inputs of a `Union` share a physical layout (equal type codes, sizes, and
/// PK indices); only per-column nullability may differ. Return `a`'s schema with
/// each column's nullability OR-ed with `b`'s, so a null-carrying side forces the
/// null-aware `Generic` row comparator instead of the null-blind `FixedIntNonnull`
/// fast path (which orders by raw payload bytes and would fail to coalesce two
/// logically-NULL rows carrying non-zero bytes under the null bit).
fn union_nullability_merge(a: &SchemaDescriptor, b: &SchemaDescriptor) -> SchemaDescriptor {
    debug_assert_eq!(a.num_columns(), b.num_columns(), "union inputs must share a layout");
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    for c in 0..a.num_columns() {
        let ac = a.columns[c];
        // b.columns[c] shares ac's type_code/size; only its nullable bit is read.
        cols[c] = SchemaColumn::new(ac.type_code, ac.nullable | b.columns[c].nullable);
    }
    SchemaDescriptor::new(&cols[..a.num_columns()], a.pk_indices())
}
```

The union output is always an intermediate (never a replicated base table), so `new` — not
`new_with_dist` — is correct, matching how the other intermediate-schema arms (`NullExtend`,
`HashRow`) build their schemas. Same-nullability unions (the overwhelming majority) OR equal
flags and are byte-identical to today, including `payload_cmp`.

## Gate

`make verify` + `make e2e` (W=4):

- **Unit (`gnitz-engine`):** `union_nullability_merge` of a `[U128 pk, I64 nonnull]` schema with
  a `[U128 pk, I64 nullable]` schema yields a nullable output column whose `payload_cmp ==
  Generic`; the all-non-nullable pair stays `FixedIntNonnull`; a nullable-left / non-nullable-
  right pair is also `Generic` (OR is symmetric).
- **Behavioral (`gnitz-engine`, garbage-under-null):** feed a two-input `union` where the
  `PORT_IN_B` batch carries two rows that are both NULL in an `I64` payload column but hold
  **different** non-zero bytes under the null bit (the `NullGarbage` construction), share one
  content-hash PK, and run through the `union` + a `distinct` weight-clamp. Assert the two NULL
  rows coalesce to a single row of weight 1. Fails before the fix (they split), passes after.
- **E2E (`UNION DISTINCT`, mainstream path):** `(SELECT a FROM t_nonnull) UNION (SELECT b FROM
  t_nullable)` with `a: BIGINT NOT NULL`, `b: BIGINT` (nullable); insert NULL `b` rows and
  assert a single NULL row in the view with weight 1, and that a NULL never coincides with a
  non-null `0`. Confirms the compiled view schema is null-aware end to end.
