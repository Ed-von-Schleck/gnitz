# Set-operation integer type promotion (cross-width UNION / INTERSECT / EXCEPT)

`emit_set_op_pieces` (`plan/view/set_op.rs`) requires exact `TypeCode` equality per column
pair, so `(SELECT a FROM t1) UNION (SELECT b FROM t2)` with `a: I32`, `b: I64` fails with
`set operation: column 0 type mismatch (I32 vs I64)`. Widen the narrower side to a common
integer type so equal logical values get one physical representation, then the content-hash
set arithmetic works unchanged.

The restriction is real: `map_hash_row` content-hashes the projected columns' payload bytes
and the downstream `union` / `positive_part` merge reads each column at the output schema's
fixed width, so the two sides must share a physical layout. Floats are already excluded from
set-op projections (`reject_float_keys`); STRING / U128 / UUID have no cross-type promotion.
This plan covers **integer** promotion only.

## Promotion rule

For each column pair `(l, r)` the common type is `join_key_common_type(l.type_code,
r.type_code)` — the join-key ladder: same-sign cross-width → the wider type; cross-sign with
the unsigned operand ≤ U32 → the narrowest signed type strictly wider than the unsigned
operand (e.g. `U32` vs `I32` → `I64`). Accept the pair only when the result is a concrete
≤8-byte integer (`FixedInt::from_type_code(t).is_some()`); everything else — `None`, or the
`I128` collapse for `U64`-vs-`I64` and any 16-byte / non-integer pair — keeps the exact-match
type-mismatch error. The ≤8-byte bound is load-bearing: the widening path loads a value into
one i64 register (§ mechanism), which cannot represent a 16-byte extremum.

```rust
// Per-pair common type; None target = keep the exact-match error.
fn set_op_common_type(l: TypeCode, r: TypeCode) -> Option<TypeCode> {
    if l == r {
        return Some(l);
    }
    let t = l.join_key_common_type(r)?;
    FixedInt::from_type_code(t).is_some().then_some(t)
}
```

Nullable columns promote — no `NOT NULL` restriction. The widening copy carries the source
null bit into the output null bitmap, and `reindex_hash_row` hashes an `is_null` flag per
column and **skips the payload bytes of a null row** (`ops/reindex.rs`), so a widened-nullable
side and a native-nullable side hash identically regardless of width; only equal *non-null*
logical values must share a representation, which the value-preserving widen guarantees.

## Mechanism: widen inside the hash map

A compute `map_expr` node cannot host the widening: with empty `reindex_cols` the compiler
assigns it `loaded.out_schema` (the whole view's output schema), not a per-node schema, and
`EMIT` columns carry no type in the program — so a `map_expr` is only correct as the terminal
node before the sink, never as an intermediate promotion node feeding `map_hash_row`. The
join-key `reindex_target_tcs` mechanism does not help either: it promotes only the synthetic
**PK** slots (`reindex_output_schema` copies payload columns verbatim) and is unused on the
`reindex_hash` branch.

Fuse the promotion into the hash map. `op_map`'s `reindex_hash` branch (`ops/linear.rs`)
already runs `func.evaluate_map_batch(batch, out_schema)` then `reindex_hash_row(out_schema,
…)`; if `out_schema` declares the promoted payload widths and `func` widens each narrowed
column into them, the hash sees one consistent representation. Two engine pieces change: the
`HashRow` map carries per-column target type codes, and the projection kernel widens.

### `MapKind::HashRow` carries `target_tcs`

Add a `Vec<u8>` parallel to the projection columns (0 = no promotion), mirroring the
`Expression` variant's `reindex_target_tcs`:

```rust
// gnitz-wire circuit.rs
HashRow(Vec<u16> /* proj_cols */, Vec<u8> /* target_tcs */, u8 /* branch_id */),
```

Encode (`gnitz-core` circuit.rs) writes each proj col's row with `value2 = target_tcs[i]`
(the free slot `encode_col_list` currently leaves 0), and decode (`gnitz-wire` circuit.rs)
collects `value2` per proj col, rejecting a non-zero target that is not a fixed-width integer
(`FixedInt::from_type_code(tc).is_some()`) — the catalog trust boundary, matching how the
`Expression` decode validates `is_pk_eligible`. `CircuitBuilder::map_hash_row` gains the
`target_tcs: &[u8]` argument.

### Compiler HashRow arm: promoted schema + widening func

`query/compiler/emit.rs`'s `MapKind::HashRow` arm builds the node schema by promoting each
projected payload column's type while keeping its nullability, and drives the widening
projection off the same `target_tcs`:

```rust
let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
cols[0] = SchemaColumn::new(type_code::U128, 0);
for (j, &i) in src_indices.iter().enumerate() {
    let src = in_reg_schema.columns[i as usize];
    let tgt = target_tcs.get(j).copied().unwrap_or(0);
    let out_tc = if tgt != 0 { tgt } else { src.type_code };
    // `new` re-derives size/signedness for the promoted type; keep source nullability
    // (`SchemaColumn::size`/`is_signed` are private and derived, so a field mutation
    // would leave them at the source width).
    cols[1 + j] = SchemaColumn::new(out_tc, src.nullable);
}
let node_schema = SchemaDescriptor::new(&cols[..1 + src_indices.len()], &[0]);
```

`create_universal_projection` (→ `ScalarFunc::from_projection`) then widens per column: the
output-slot stride comes from `node_schema` (the promoted width) while the source read width
and signedness come from the input column.

### Widening `ColMove`

`ScalarFunc::from_projection` (`expr/plan.rs`) builds one `ColMove` per column with a single
`stride`; `copy_column`'s payload branch bulk-`copy_from_slice`s `n * stride` bytes verbatim,
which panics the moment the output slot is wider than the source. Split the widths and
extend:

```rust
struct ColMove {
    src_pi: u8,
    dst_payload: usize,
    type_code: u8,   // source type — drives widening signedness
    src_stride: u8,  // source read width
    stride: u8,      // destination write width (== src_stride when not widening)
    pk_byte_offset: u8,
}
```

`from_projection` sets `src_stride = in_schema.columns[src_ci].size()` and `stride =
out_schema.columns[out_ci].size()`. `copy_column`'s payload-int branch keeps the bulk copy
when `src_stride == stride`, else widens per row exactly as `LoadPayloadInt` does — matching
on `(src_stride, is_signed_int(type_code))` to sign/zero-extend the source value into the
wider little-endian slot:

```rust
if cm.src_stride == cm.stride {
    output.col_data_mut(cm.dst_payload).copy_from_slice(&in_batch.col_data(in_pi)[..n * stride]);
} else {
    // Widen: read src_stride LE bytes, sign/zero-extend per the source type, write `stride`.
    let src = in_batch.col_data(in_pi);
    let dst = output.col_data_mut(cm.dst_payload);
    let signed = crate::schema::is_signed_int(cm.type_code);
    for row in 0..n {
        let v: i64 = match (cm.src_stride, signed) {
            (4, true) => i32::from_le_bytes(src[row * 4..row * 4 + 4].try_into().unwrap()) as i64,
            (4, false) => u32::from_le_bytes(src[row * 4..row * 4 + 4].try_into().unwrap()) as i64,
            (2, true) => i16::from_le_bytes(src[row * 2..row * 2 + 2].try_into().unwrap()) as i64,
            (2, false) => u16::from_le_bytes(src[row * 2..row * 2 + 2].try_into().unwrap()) as i64,
            (1, true) => src[row] as i8 as i64,
            (1, false) => src[row] as i64,
            _ => unreachable!("widen source stride must be 1/2/4"),
        };
        dst[row * cm.stride as usize..(row + 1) * cm.stride as usize]
            .copy_from_slice(&v.to_le_bytes()[..cm.stride as usize]);
    }
}
```

The `Projection` map never triggers this (its output types equal its source types, so
`src_stride == stride`), so the change is dormant for every path except a promoting HashRow.
Null rows carry their bit through `NullPerm::from_projection` unchanged; their widened value
bytes are deterministic but never hashed.

## Planner change

`compile_set_op_side` currently fuses resolve + hash + shard, but the common type needs both
sides' output columns. Split it:

1. `resolve_set_op_side` — clause rejection, input delta, optional WHERE, projection
   resolution — returns `(filtered_node, proj_indices, out_cols, source_tid)` without hashing.
2. In `emit_set_op_pieces`, compute `common_tcs` from both sides' `out_cols` via
   `set_op_common_type` (error on `None`).
3. Per side, `hash_shard_side(cb, filtered_node, &proj_indices, &target_tcs, branch_id)` where
   `target_tcs[i] = if side_col_tc[i] == common_tcs[i] { 0 } else { common_tcs[i] }`; it calls
   `cb.map_hash_row(filtered_node, proj_indices, &target_tcs, branch_id)` then `cb.shard(_, &[0])`.
4. Build the output schema from `common_tcs`; the per-operator nullability tightening is
   unchanged (INTERSECT: `l.is_nullable && r.is_nullable`; EXCEPT: `l.is_nullable`; UNION:
   `l.is_nullable || r.is_nullable`).

`emit_distinct_pieces` reuses the same two stages with an all-zero `target_tcs` (a single
source has nothing to promote against), so `SELECT DISTINCT` compiles a byte-identical circuit
to today.

Because both sides emit the promoted layout, the content hash agrees across sides and the
`union` / `positive_part` merge reads one consistent width.

## Gate

`make verify` + `make e2e` (W=4), all weight-verified across widths and signs, for UNION /
UNION ALL / INTERSECT / EXCEPT (DISTINCT and ALL):

- `I32 UNION I64`, `U16 UNION U32`, `U32 UNION I32` (→ I64): equal logical values coalesce
  under UNION DISTINCT, intersect, and cancel under EXCEPT; distinct values stay distinct.
- A **nullable** cross-width pair (`nullable_i32` vs `nullable_i64`): a NULL on either side
  coalesces with a NULL on the other under UNION DISTINCT / INTERSECT and cancels under
  EXCEPT (no spurious null-fill), and a NULL never collides with a non-null zero.
- An incremental insert then retraction that flips membership (both a promoted and a
  non-promoted column), asserting the emitted delta weights.
- Negative tests still error cleanly: `String` vs `I64`; `U64` vs `I64` (→ I128, rejected);
  a float column (rejected earlier by `reject_float_keys`).
