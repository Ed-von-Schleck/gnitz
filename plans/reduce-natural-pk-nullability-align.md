# Reduce — align natural-PK predicate nullability check

The planner and the engine each carry their own copy of the
"single-column natural PK" predicate used to decide whether a GROUP BY
view emits its output keyed by a synthetic `U128 _group_pk` column or
by the source column itself. The two copies disagree on nullability.

`crates/gnitz-core/src/protocol/types.rs` (planner side):

```rust
pub fn is_single_col_natural_pk(&self, cols: &[usize]) -> bool {
    cols.len() == 1
        && matches!(
            self.columns[cols[0]].type_code,
            TypeCode::U64 | TypeCode::U128 | TypeCode::UUID,
        )
        && !self.columns[cols[0]].is_nullable
}
```

`crates/gnitz-engine/src/ops/reduce.rs::is_single_col_natural_pk`
(engine side):

```rust
pub(crate) fn is_single_col_natural_pk(schema: &SchemaDescriptor, group_cols: &[u32]) -> bool {
    group_cols.len() == 1
        && matches!(
            TypeCode::from_validated_u8(schema.columns[group_cols[0] as usize].type_code),
            TypeCode::U64 | TypeCode::U128 | TypeCode::UUID,
        )
}
```

The engine drops the nullability check.

## What goes wrong

`CREATE VIEW v AS SELECT g, SUM(x) FROM t GROUP BY g` where `t.g` is a
nullable `U64` payload column. The planner sees a nullable group col,
returns `false`, and registers the view's output schema with the
synthetic-PK layout — column 0 is `_group_pk: U128 NOT NULL`, column 1
is `g: U64 NULL`, column 2 is `SUM(x): I64`.

The engine compiler (`compiler.rs:697`) calls its copy of the helper,
which returns `true` (skips the nullability check), so it builds the
reduce circuit with the natural-PK layout — column 0 is `g: U64`,
column 1 is the aggregate.

Output rows now have:

- A `g: U64` value where the planner expects a synthetic `u128` PK
  byte region (PK strides differ: 8 vs 16).
- The aggregate column shifted one slot relative to the planner's
  schema.

The reader (SQL query, downstream view, wire decoder) reads through
the planner's schema and mis-interprets bytes column-by-column.
There's no null bitmap for the PK region, so the engine has no place
to put a NULL `g` either — it stores raw `0`, silently colliding all
NULL groups into a single bucket that the planner believes is the
`u128 = 0` synthetic key.

## Fix

Add the missing nullability check to the engine helper so it mirrors
the planner's predicate exactly:

```rust
pub(crate) fn is_single_col_natural_pk(schema: &SchemaDescriptor, group_cols: &[u32]) -> bool {
    group_cols.len() == 1
        && matches!(
            TypeCode::from_validated_u8(schema.columns[group_cols[0] as usize].type_code),
            TypeCode::U64 | TypeCode::U128 | TypeCode::UUID,
        )
        && schema.columns[group_cols[0] as usize].nullable == 0
}
```

The planner-side comment at `planner.rs:953-967` already documents
that the two copies must agree — it just describes a guarantee the
engine wasn't actually meeting.

## Why two copies in the first place

`SchemaDescriptor` (engine) and `Schema` (gnitz-core) are different
types because gnitz-core can't depend on gnitz-engine. Both crates
need the predicate at compile time: the planner uses it to pick the
output schema *before* shipping the create-view request; the compiler
uses it to pick the reduce-op variant *after* the view is registered.
A divergence between the two predicates produces the schema mismatch
above, so keeping them in lockstep is load-bearing.

A consolidation into `gnitz-wire` (or a trait shared between the two
schema types) would prevent future drift but is not in scope of this
fix — the two implementations are tiny and easy to keep aligned by
inspection.

## Touchpoints

- `crates/gnitz-engine/src/ops/reduce.rs::is_single_col_natural_pk`
  (line 421): add the `nullable == 0` clause shown above.

## Testing

- `CREATE TABLE t (id U64 PRIMARY KEY, g U64 NULL, x I64)` then
  `CREATE VIEW v AS SELECT g, SUM(x) FROM t GROUP BY g`. Confirm the
  view's catalog schema matches the engine's output column layout
  (synthetic `_group_pk` present, `g` non-PK).
- Insert rows with mixed NULL and non-NULL `g`. Confirm NULL rows
  aggregate into one group whose `g` column reads back as NULL (not
  zero), and non-NULL rows aggregate per distinct value.
- Repeat the test with `g U128 NULL` and `g UUID NULL` — both should
  follow the synthetic-PK path now.
- Non-regression: `g U64 NOT NULL` keeps using the natural-PK path
  (no synthetic column in the output schema), aggregation results
  identical to before the fix.
