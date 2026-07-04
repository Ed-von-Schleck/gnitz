# View-compiler SQL completeness: HAVING IS NULL on direct aggregates + set-op integer promotion

Two independent over-restrictions in the CREATE VIEW compiler reject standard SQL the
engine can already execute. Each is self-contained with its own gate.

## 1. HAVING `IS [NOT] NULL` on direct aggregates (MIN / MAX / COUNT / non-nullable SUM)

### Current behaviour

`bind_null_test` (`plan/view/group_by.rs`) binds `IS [NOT] NULL` only for aggregates that
carry a hidden `COUNT_NON_NULL` companion — `AggShape::Avg` and `AggShape::NullfillSum`
(nullable-source SUM and AVG). Every other aggregate hits

```
HAVING: IS [NOT] NULL on Min is not supported
```

so `SELECT k, MIN(v) FROM t GROUP BY k HAVING MIN(v) IS NOT NULL` fails to compile.

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
bit while companion-carrying aggregates read `companion == 0`. The final `_ =>` arm still
rejects a non-aggregate `inner` (e.g. `(x + y) IS NULL`); generalize its message from
"only supported on nullable SUM/AVG aggregates" to "only supported on an aggregate".

### Gate

`make verify` + `make e2e` (W=4), weight-verified: `HAVING MIN(v) IS NOT NULL` and
`IS NULL` over a group that flips to all-NULL by retraction (emits `(k, NULL)` then
disappears), `HAVING COUNT(*) IS NOT NULL` (always true), and the existing nullable-SUM/AVG
HAVING tests unchanged.

## 2. Set-operation integer type promotion

### Current behaviour

`emit_set_op_pieces` (`plan/view/set_op.rs`) requires exact `TypeCode` equality per column
pair, so `(SELECT a FROM t1) UNION (SELECT b FROM t2)` with `a: I32`, `b: I64` fails:

```
set operation: column 0 type mismatch (I32 vs I64)
```

The restriction is real but over-broad: it exists because `map_hash_row` content-hashes the
projected columns' **payload bytes** and the downstream union/positive_part merge reads
columns at fixed widths, so the two sides must share a physical layout. Widening the narrower
side to a common type restores that.

Floats are already excluded from set-op projections entirely (`reject_float_keys`), so this
is purely an **integer** promotion; STRING / U128 / UUID have no cross-type promotion and keep
their exact-match requirement.

### Promotion rule

For each column pair `(l, r)` the common type is `l.type_code.join_key_common_type(r.type_code)`
— the same ladder the join key uses, which returns the narrowest type that *faithfully holds
both ranges* (cross-width same-sign → wider; cross-sign with the unsigned operand ≤ U64 →
narrowest signed holding both). Accept only a concrete ≤8-byte integer result; anything else
(`None`, or the U128 collapse for U64-vs-I64 / wide / non-integer pairs) keeps the existing
type-mismatch error.

Promotion widens a column via a compute Map before the hash. Signed-correct widening is
`load_col_int` (which sign/zero-extends into the i64 register per the source column's
signedness) followed by `emit_col` into an output column declared at the common type; the
already-wide side (or a same-type column) is a straight `copy_col`. A widened source is always
strictly narrower than its ≤8-byte target, so its value always fits the i64 register losslessly.

`emit_col` writes the register value unconditionally and does not carry a source null bit,
while `copy_col` carries the null bit but cannot widen — so **a column that requires widening
must be `NOT NULL`**. If a cast-requiring column is nullable, keep the exact-match error
(nullable cross-width promotion would need a widening-copy-with-null opcode; out of scope).
A pair needing no cast (equal types) is unaffected by this rule regardless of nullability.

```rust
// Per-pair common type; reject anything without a faithful ≤8-byte integer promotion.
let mut common_tcs: Vec<TypeCode> = Vec::with_capacity(left_cols.len());
for (i, (l, r)) in left_cols.iter().zip(&right_cols).enumerate() {
    let t = if l.type_code == r.type_code {
        l.type_code
    } else {
        match l.type_code.join_key_common_type(r.type_code) {
            Some(t) if FixedInt::from_type_code(t).is_some() => t,
            _ => {
                return Err(GnitzSqlError::Plan(format!(
                    "set operation: column {i} type mismatch ({:?} vs {:?})",
                    l.type_code, r.type_code
                )))
            }
        }
    };
    // A column that must widen has to be NOT NULL (emit_col drops the null bit).
    if (l.type_code != t && l.is_nullable) || (r.type_code != t && r.is_nullable) {
        return Err(GnitzSqlError::Plan(format!(
            "set operation: column {i} needs a {t:?} widening cast but a side is nullable; \
             cast it explicitly in the branch's SELECT"
        )));
    }
    common_tcs.push(t);
}
```

### Pipeline change

`compile_set_op_side` currently fuses project + hash + shard: `map_hash_row(filtered,
&proj_indices, branch_id)` both projects and hashes. Split it so the promotion sits between:

1. Resolve each side's `(filtered_node, proj_indices, out_cols, source_tid)` — clause
   rejection, input delta, optional WHERE, projection resolution — **without** hashing.
2. In `emit_set_op_pieces`, compute `common_tcs` from both sides' `out_cols` (above).
3. For each side, if any projected column's type differs from its `common_tcs` entry, insert
   a compute Map over `filtered_node` producing the promoted columns (per column: `copy_col`
   when already at the target, else `load_col_int` + `emit_col` into an output column typed at
   the target), then `map_hash_row` with an identity `0..m` projection over the Map output and
   `shard`. When no column promotes on a side, keep today's direct
   `map_hash_row(&proj_indices)` (byte-identical to current circuits).
4. Build the output schema from `common_tcs` (not `left_cols[i].type_code`); the existing
   per-operator nullability tightening is unchanged.

Because both sides emit the promoted layout, the content hash agrees across sides (equal
logical integers → equal representations in the common type) and the union/positive_part merge
reads one consistent width.

### Gate

`make verify` + `make e2e` (W=4), all weight-verified across widths and signs:
`I32 UNION I64`, `U16 UNION U32`, `U32 UNION I32` (→ I64), for UNION / UNION ALL / INTERSECT /
EXCEPT (both DISTINCT and ALL), asserting set membership matches equal logical values across
types (e.g. `I32` 5 and `I64` 5 coalesce under UNION DISTINCT, intersect, and cancel under
EXCEPT), plus an incremental insert/retraction that flips membership. Negative tests: a
nullable cross-width pair and a String-vs-I64 pair still error.
