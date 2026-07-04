# View-compiler HAVING completeness: `IS [NOT] NULL` on direct aggregates and group columns

`bind_null_test` (`plan/view/group_by.rs`) accepts `IS [NOT] NULL` in a HAVING clause
only for aggregates that carry a hidden `COUNT_NON_NULL` companion — `AggShape::Avg`
and `AggShape::NullfillSum` (nullable-source SUM and AVG). Every other HAVING null test
is rejected, even though the engine can evaluate all of them. Two standard-SQL shapes
are wrongly refused:

- a **direct aggregate**: `SELECT k, MIN(v) FROM t GROUP BY k HAVING MIN(v) IS NOT NULL`
  → `HAVING: IS [NOT] NULL on Min is not supported`;
- a **group column**: `SELECT k, COUNT(*) FROM t GROUP BY k HAVING k IS NOT NULL`
  → `HAVING: IS [NOT] NULL is only supported on nullable SUM/AVG aggregates`.

Both are fixed in the one function, `Having::bind_null_test`. They are independent —
the aggregate arm (`Expr::Function`) and the group-column arm (the trailing `_ =>`).

## 1. `IS [NOT] NULL` on direct aggregates (MIN / MAX / COUNT / non-nullable SUM)

### Why the raw null bit is authoritative for `AggShape::Direct`

`AggShape::Direct` covers MIN, MAX, COUNT, COUNT_NON_NULL, and non-nullable SUM. For all of
them the value column's raw null bit is exact:

- **MIN / MAX** are non-linear and re-derive from history. When a group's last non-null
  contributor is retracted the accumulator returns to `is_untouched()`, so `reduce/emit.rs`
  writes the null bit (regression-tested: "MIN renders NULL for all-NULL group"). Unlike a
  linear-fold SUM — which saturates its `has_value` bit and emits a concrete `0` where NULL
  is correct, the sole reason SUM needs the companion gate — MIN/MAX never emit a phantom
  value.
- **COUNT / COUNT_NON_NULL / non-nullable SUM** never emit NULL on a surviving group
  (`empty_renders_zero` / non-nullable source), so the null bit is simply never set.

Decisively, the SELECT projection *already* trusts this: `bind_function`'s `AggShape::Direct`
arm returns `BoundExpr::ColRef(sum_col)` and reads the raw column, null bit included. The
HAVING binder rejecting the same read is an inconsistency, not a safeguard.

### Design

Replace the `else`-error in `bind_null_test`'s `Expr::Function` arm (the `!has_count_companion`
case is exactly `AggShape::Direct`) with a direct raw-null-bit test, mirroring the SELECT
projection. `BoundExpr::IsNull(usize)` / `IsNotNull(usize)` already exist (`ir.rs`).

```rust
Expr::Function(func) => {
    let m = resolve_having_mapping(func, self.ctx)?;
    let val_col = self.ctx.agg_col_offset + m.specs_start;
    if m.shape.has_count_companion() {
        // Nullable SUM / AVG: NULL ⇔ COUNT_NON_NULL companion (specs_start + 1) is 0.
        let cnt_col = val_col + 1;
        let bop = if want_null { BinOp::Eq } else { BinOp::Ne };
        Ok(BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(cnt_col)),
            bop,
            Box::new(BoundExpr::LitInt(0)),
        ))
    } else {
        // AggShape::Direct (MIN/MAX/COUNT/non-nullable SUM): the value column is copied
        // straight through and its raw null bit is authoritative — set for an all-NULL
        // MIN/MAX group, never for COUNT / non-nullable SUM. Read it directly, matching
        // the SELECT projection.
        if want_null {
            Ok(BoundExpr::IsNull(val_col))
        } else {
            Ok(BoundExpr::IsNotNull(val_col))
        }
    }
}
```

Update the arm's leading doc comment to state that `Direct` aggregates read the raw null
bit while companion-carrying aggregates read `companion == 0`.

## 2. `IS [NOT] NULL` on a group column

### Why a naive `IsNull(reduce_col)` is unsafe, and the layout rule that fixes it

A group column's reduce-output position depends on how the reduce lays out the group key
(`group_col_reduce_pos`, mirroring `build_reduce_output_schema`):

- **Natural-PK grouping** (`group_set_eq_pk` or `single_col_natural_pk`): the group
  columns *are* the reduce PK, stored in the PK region. PK columns are non-nullable
  (`is_single_col_natural_pk` requires `!is_nullable`; a `group_set_eq_pk` group set is a
  permutation of the non-nullable PK), and they carry **no payload null bit** — the
  reduce-output column maps to `PAYLOAD_MAPPING_PK_SENTINEL`. Emitting `EXPR_IS_NULL`
  against that slot trips the engine assertion in `eval_is_null` ("IS [NOT] NULL operand
  resolved to a PK column; the binder must const-fold …") and, in release, shifts the null
  word by a sentinel index (garbage result).
- **Synthetic-PK grouping**: the U128 `_group_pk` is the PK; the group columns follow it in
  the payload region, keeping their source nullability and a real null bit.

So the null-ness of a group column is a compile-time fact:

- non-nullable (every PK-region case, plus a `NOT NULL` synthetic-path column) → const-fold
  the test: `IS NULL` ≡ `false`, `IS NOT NULL` ≡ `true`;
- nullable (a nullable source column in the synthetic path) → the payload null bit is
  authoritative → emit `IsNull` / `IsNotNull` on the reduce-output slot.

This mirrors the base-relation binder's `fold_null_test` (`bind/structural.rs`), which
already const-folds `IS [NOT] NULL` on any non-nullable column so `EXPR_IS_NULL` never
reaches a PK sentinel on the WHERE/filter path.

### Design

Replace the trailing `_ =>` reject arm of `bind_null_test` (whose message is being changed
anyway) with group-column handling:

```rust
_ => {
    // A bare group-column reference. Its IS [NOT] NULL is decided at compile time:
    // const-fold when the reduce-output column cannot be NULL (every PK-region group
    // key — group_set_eq_pk / single_col_natural_pk — and any NOT NULL synthetic-path
    // column), else read the raw payload null bit. Const-folding the non-nullable case
    // also keeps EXPR_IS_NULL off the PK sentinel (see eval_is_null's assertion),
    // mirroring the base-relation `fold_null_test`.
    let ctx = self.ctx;
    let col_name = single_relation_col_name(inner).ok_or_else(|| {
        GnitzSqlError::Unsupported(
            "HAVING: IS [NOT] NULL is only supported on an aggregate or a group column".to_string(),
        )
    })?;
    let src = find_unique_column(&ctx.source_schema.columns, col_name)?
        .ok_or_else(|| GnitzSqlError::Bind(format!("HAVING: column '{col_name}' not found")))?;
    if !ctx.group_col_indices.contains(&src) {
        return Err(GnitzSqlError::Bind(format!(
            "HAVING: column '{col_name}' must appear in GROUP BY or an aggregate function"
        )));
    }
    // Natural-PK grouping stores the key in the non-nullable PK region; the synthetic
    // path stores group columns in payload with their source nullability.
    let in_pk_region = ctx.group_set_eq_pk || ctx.single_col_natural_pk;
    let nullable = !in_pk_region && ctx.source_schema.columns[src].is_nullable;
    if !nullable {
        Ok(BoundExpr::LitInt((!want_null) as i64))
    } else {
        let reduce_col = group_col_reduce_pos(
            src,
            ctx.group_set_eq_pk,
            ctx.single_col_natural_pk,
            ctx.source_schema,
            ctx.group_col_indices,
        );
        if want_null {
            Ok(BoundExpr::IsNull(reduce_col))
        } else {
            Ok(BoundExpr::IsNotNull(reduce_col))
        }
    }
}
```

`single_relation_col_name`, `find_unique_column`, and `group_col_reduce_pos` are already in
scope in `group_by.rs` (all three are used by `Having::bind_column`). The `Expr::Nested`
arm is unchanged. A non-column, non-aggregate `inner` (e.g. `(x + y) IS NULL`) still errors,
now via the group-column arm's `single_relation_col_name` / group-membership checks.

## Gate

`make verify` + `make e2e` (W=4), weight-verified:

- **Direct aggregates:** `HAVING MIN(v) IS NOT NULL` and `IS NULL` over a group that flips
  to all-NULL by retraction (emits `(k, NULL)` then disappears); `HAVING COUNT(*) IS NOT NULL`
  (always true).
- **Group columns:**
  - a nullable group column on the synthetic path
    (`SELECT g, COUNT(*) FROM t GROUP BY g HAVING g IS NULL` with a NULL-key group) emits the
    NULL group and only it; `IS NOT NULL` emits every other group — the compiled circuit
    carries an `EXPR_IS_NULL` / `EXPR_IS_NOT_NULL` on the group column's payload slot.
  - a natural-PK group column (`GROUP BY pk … HAVING pk IS NOT NULL` — `pk` the single
    U64/U128/UUID PK, and a multi-column `group_set_eq_pk`) compiles with **no**
    `EXPR_IS_NULL`/`EXPR_IS_NOT_NULL` for the group slot (const-folded) and runs without
    tripping the PK-sentinel assertion; `IS NULL` yields the empty view, `IS NOT NULL` the
    full grouped view.
- **Unchanged:** the existing nullable-SUM/AVG HAVING tests; a non-aggregate non-column
  null test (`(x+y) IS NULL`) still errors.
