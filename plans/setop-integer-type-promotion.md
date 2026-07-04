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

Encode (`gnitz-core` circuit.rs) hand-rolls the proj-col rows to carry `value2 = target tc`
(the shared `encode_col_list` helper leaves `value2 = 0`), using the same
`.get(i).copied().unwrap_or(0)` slice the `Expression` encode already uses so a short/absent
`target_tcs` degrades to "no promotion" instead of panicking:

```rust
// gnitz-core circuit.rs — encode_op_node
OpNode::Map(MapKind::HashRow(cols, target_tcs, branch_id)) => {
    // value1 = source column index, value2 = promoted target type code
    // (0 = keep the source type). Hand-rolled (not encode_col_list) to carry
    // the per-column target, exactly like the Expression reindex_target_tcs encode.
    let mut kind_rows: Vec<(u64, u16, u64, u64)> = cols
        .iter()
        .enumerate()
        .map(|(i, &col)| {
            let v2 = target_tcs.get(i).copied().unwrap_or(0) as u64;
            (NODE_COL_KIND_PROJ, i as u16, col as u64, v2)
        })
        .collect();
    kind_rows.push((NODE_COL_KIND_BRANCH_ID, 0, branch_id as u64, 0));
    ((OPCODE_MAP_HASH_ROW, None, None), kind_rows)
}
```

Decode (`gnitz-wire` circuit.rs) collects `value2` per proj col and rejects a non-zero target
that is not a **≤8-byte fixed-width integer** — the promotion domain `set_op_common_type`
produces (stricter than the `Expression` decode's `is_pk_eligible`, which also admits the
16-byte `U128`/`UUID`/`I128` that the one-i64-register widen path cannot represent). The
`cols` slice is position-ordered per kind, so filtering by `NODE_COL_KIND_PROJ` preserves
projection order:

```rust
// gnitz-wire circuit.rs — decode_op_node
OPCODE_MAP_HASH_ROW => {
    let branch_id = cols
        .iter()
        .find(|c| c.kind == NODE_COL_KIND_BRANCH_ID)
        .map(|c| c.value1 as u8)
        .unwrap_or(0);
    let mut proj_cols: Vec<u16> = Vec::new();
    let mut target_tcs: Vec<u8> = Vec::new();
    for c in cols.iter().filter(|c| c.kind == NODE_COL_KIND_PROJ) {
        let tc = c.value2 as u8;
        // Trust boundary: a bogus target would drive a wrong payload width in
        // copy_column, so reject it here rather than downstream.
        if tc != 0 && TypeCode::try_from_u8(tc).and_then(FixedInt::from_type_code).is_none() {
            return Err(format!("MAP_HASH_ROW target type code {tc} is not a fixed-width integer"));
        }
        proj_cols.push(c.value1 as u16);
        target_tcs.push(tc);
    }
    OpNode::Map(MapKind::HashRow(proj_cols, target_tcs, branch_id))
}
```

`CircuitBuilder::map_hash_row` gains a `target_tcs: &[u8]` argument (stored via
`target_tcs.to_vec()`); `MapKind::Projection` decode is untouched (it carries no targets).

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
    // `new` re-derives size/signedness for the promoted type; keep THIS SIDE's
    // nullability (`SchemaColumn::size`/`is_signed` are private and derived, so a
    // field mutation would leave them at the source width). Per-side, not the
    // operator-merged view nullability: this leaf and its downstream `distinct`
    // history must classify their row comparator (`PayloadCmpKind`) by what this
    // side can actually emit — an INTERSECT/EXCEPT leaf is `distinct`-ed on its own
    // before the tuple-tightening combine, so tightening here would mis-drop a
    // nullable leaf's null-awareness.
    cols[1 + j] = SchemaColumn::new(out_tc, src.nullable);
}
let node_schema = SchemaDescriptor::new(&cols[..1 + src_indices.len()], &[0]);
```

`create_universal_projection` (→ `ScalarFunc::from_projection`) then widens per column: the
output-slot stride comes from `node_schema` (the promoted width) while the source read width
and signedness come from the input column.

### Widening `ColMove`

`ScalarFunc::from_projection` (`expr/plan.rs`) builds one `ColMove` per column with a single
`stride`; `copy_column` copies `stride` bytes verbatim, which is wrong the moment the output
slot is wider than the source — both on the **payload** branch (a `copy_from_slice` length
mismatch panic) and on the **PK** branch (`decode_pk_column(&opk[pk_off..pk_off + stride], …)`
over-reads the OPK region past the source column into the next PK column / off the end, and
never sign-extends). A HashRow can project a source **PK** column — e.g. `(SELECT id FROM t1)
UNION (SELECT val FROM t2)` with `id: I32 PK`, `val: I64` — so the PK branch must widen too.
Split the read/write widths and add a shared widen step used by both branches:

```rust
struct ColMove {
    src_pi: u8,
    dst_payload: usize,
    type_code: u8,   // SOURCE type — drives widening signedness and src_stride
    src_stride: u8,  // source read width  (= the source column's own width)
    stride: u8,      // destination write width (== src_stride when not widening)
    pk_byte_offset: u8,
}
```

`col_move_stride` is deleted — its payload arm returned the *source* width (correct only
when source and destination types match, which promotion breaks). Both constructors now set
the widths explicitly: `src_stride` from the source column's own width (via the logical column
index `ci`, valid for a PK or a payload source — never `payload_col_idx`, which panics on the
PK sentinel), and `stride` from the output slot:

```rust
// ScalarFunc::from_projection — per column
let src_stride = in_schema.columns[ci as usize].size();      // source read width
let out_ci = out_schema.payload_col_idx(i);
let stride = out_schema.columns[out_ci].size();              // destination write width
ColMove { src_pi, dst_payload: i, type_code: tc, src_stride, stride, pk_byte_offset }
```

`from_map` sets `src_stride = crate::schema::type_size(tc)` (the source type width) and the
same output-slot `stride`; a compute map never widens a *copied* column (a CAST goes through
the EMIT/compute path, not a `ColMove`), so `src_stride == stride` there and the copy stays a
bulk `memcpy`.

The shared widen step decodes a native little-endian source value and sign/zero-extends it
into the wider slot, reusing the existing `read_signed` / `read_unsigned` helpers. The
`debug_assert` is the tripwire if a >8-byte target ever slips past `set_op_common_type`
(the one-i64 register cannot hold a 16-byte extremum):

```rust
/// Widen a native little-endian integer of `src_stride` (1/2/4) bytes into `dst`
/// (≤ 8 bytes), sign- or zero-extending per `signed`. Reached only from a promoting
/// set-op HashRow — `set_op_common_type` caps the target at a ≤8-byte integer.
#[inline]
fn widen_int_le(src_le: &[u8], src_stride: usize, dst: &mut [u8], signed: bool) {
    debug_assert!(dst.len() <= 8, "copy_column: widening target {} > 8 bytes", dst.len());
    let v: i64 = if signed {
        crate::schema::read_signed(src_le, src_stride)
    } else {
        crate::schema::read_unsigned(src_le, src_stride) as i64
    };
    dst.copy_from_slice(&v.to_le_bytes()[..dst.len()]);
}
```

`copy_column` calls it from both branches (PK: after `decode_pk_column` yields native LE at
the *source* width; payload: on the raw LE slot), keeping the fast path when `src_stride ==
stride`:

```rust
let stride = cm.stride as usize;         // destination write width
let src_stride = cm.src_stride as usize; // source read width

if cm.src_pi == PAYLOAD_MAPPING_PK_SENTINEL {
    let dst = output.col_data_mut(cm.dst_payload);
    let pk_off = cm.pk_byte_offset as usize;
    let mut le = [0u8; crate::schema::MAX_PK_BYTES];
    let signed = crate::schema::is_signed_int(cm.type_code);
    for row in 0..n {
        let opk = in_batch.get_pk_bytes(row);
        // Read the source column's OWN width from the OPK region (not the wider
        // destination stride), decode to native LE, then widen if needed.
        gnitz_wire::decode_pk_column(&opk[pk_off..pk_off + src_stride], cm.type_code, &mut le[..src_stride]);
        let out = &mut dst[row * stride..row * stride + stride];
        if src_stride == stride {
            out.copy_from_slice(&le[..stride]);
        } else {
            widen_int_le(&le[..src_stride], src_stride, out, signed);
        }
    }
} else if gnitz_wire::is_german_string(cm.type_code) {
    // ... unchanged: STRING/BLOB never promote (rejected by set_op_common_type) ...
} else {
    let in_pi = cm.src_pi as usize;
    if src_stride == stride {
        // ... unchanged bulk copy (keeps its existing bounds debug_assert) ...
        output.col_data_mut(cm.dst_payload).copy_from_slice(&in_batch.col_data(in_pi)[..n * stride]);
    } else {
        let src = in_batch.col_data(in_pi);
        let dst = output.col_data_mut(cm.dst_payload);
        let signed = crate::schema::is_signed_int(cm.type_code);
        for row in 0..n {
            widen_int_le(
                &src[row * src_stride..row * src_stride + src_stride],
                src_stride,
                &mut dst[row * stride..row * stride + stride],
                signed,
            );
        }
    }
}
```

The `Projection` map and every compute map keep `src_stride == stride`, so the change is
dormant for every path except a promoting HashRow. Null rows carry their bit through
`NullPerm` unchanged; a canonically-zeroed null source widens `0 → 0`, so widened-nullable
and native-nullable sides carry byte-identical (zero) payload for a null, and the value bytes
are never hashed regardless (`reindex_hash_row` skips a null column).

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
- A widened **PK-column source** on the narrow side — `(SELECT id FROM t1) UNION (SELECT val
  FROM t2)` with `id: I32 PRIMARY KEY`, `val: I64` — coalesces `id == val` rows (exercises
  `copy_column`'s PK branch widening + sign-extension), including a **compound PK** so
  `pk_byte_offset` is non-zero, and a negative `id` to pin sign-extension.
- A **nullable** cross-width pair (`nullable_i32` vs `nullable_i64`): a NULL on either side
  coalesces with a NULL on the other under UNION DISTINCT / INTERSECT and cancels under
  EXCEPT (no spurious null-fill), and a NULL never collides with a non-null zero.
- An incremental insert then retraction that flips membership (both a promoted and a
  non-promoted column), asserting the emitted delta weights.
- Negative tests still error cleanly: `String` vs `I64`; `U64` vs `I64` (→ I128, rejected);
  a float column (rejected earlier by `reject_float_keys`).
