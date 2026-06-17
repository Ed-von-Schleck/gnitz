# Adaptive MIN/MAX aggregate output type (preserve the source type)

## Goal

Make the MIN/MAX reduce emit a value of the **source column's type** instead of
widening every ≤8-byte integer to `I64`. `MIN(INT)` becomes `INT`, `MAX(SMALLINT
UNSIGNED)` becomes `SMALLINT UNSIGNED`, etc. This is two things at once:

1. **A correctness fix.** SQL MIN/MAX are extremal *selections* — the result is
   one of the input rows, so it is always representable in the source type (no
   overflow, unlike SUM). Widening to `I64` is a storage convenience, not a
   semantic requirement, and it gives the wrong output column type.
2. **The enabler for range/band LEFT joins on narrow range columns.** The
   threshold null-fill of a range/band LEFT join reduces the range column with
   MIN/MAX and reindexes the result back onto the **same key slot type** the
   delta side uses (`range.tc`). When the reduce widens a narrow range column to
   `I64`, that slot type no longer matches the delta's narrow slot, which is why
   the pure-range path rejects any non-`{I64, U64}` `range.tc`
   (`planner.rs:1483-1492`, the `is_left_join && n_eq == 0` guard). With adaptive
   output typing the reduce result is already `range.tc`, so that guard can relax
   to "any ≤8-byte integer" for pure range and the band LEFT threshold form (see
   `plans/band-left-bag-multiplicity-overfill.md`) works for all ≤8-byte integer
   range columns with no regression. That relaxation belongs to the band-left
   plan; this plan only removes the reason for the cap.

Scope: **MIN/MAX only.** SUM stays `I64` (it can overflow the source width),
COUNT/COUNT_NON_NULL stay `I64`, AVG stays `F64`, float MIN/MAX stay `F64`.

## 1. Why this is safe — the emit already truncates

The reduce accumulator is one 8-byte `i64` slot. The load path (`decode_signed`
in `ops/reduce/agg.rs:344`) reads a column into that slot **zero-extended** for
unsigned and **sign-extended** for signed, so the low `size_of(T)` bytes of the
slot are exactly the native little-endian value of the type-`T` extremum (U64 is
the bit pattern verbatim; floats go through `decode_float` and the slot holds the
`f64::to_bits` value). The row emitters already write the accumulator at the
**output column width**, not a fixed 8:

```rust
// ops/reduce/emit.rs — emit_reduce_row (:70), emit_gather_row (:244)
output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);   // cs = col.size()
```

So a narrower output column already serializes the correct value (e.g. `I32`
`-5` → low 4 bytes `FB FF FF FF`; `U32` `4_000_000_000` → `00 28 6B EE`). The
only thing forcing `I64` is `agg_output_type` *declaring* the column 8 bytes
wide. The U64 special case (kept today so an unsigned extremum above `i64::MAX`
is not tagged signed) folds into the general rule: the source type *is* `U64`.

## 2. The blockers — width-sensitive read-backs

Three engine sites read an aggregate value back assuming an 8-byte column. They
are the "8-byte trace read-back" the current `agg_output_type` doc comment cites:

- `ops/reduce/op_reduce.rs:455` — retraction old-value read from `trace_out`
  (`col_ptr(agg_col_idx, 8)` → `u64::from_le_bytes`).
- `ops/reduce/op_gather.rs:57` — per-worker partial combine
  (`get_col_ptr(idx, pi, 8)` → `combine(bits)`).
- `ops/reduce/op_gather.rs:84` — gather retraction old-value read from
  `trace_out` (`col_ptr(agg_col_idx, 8)` → `merge_accumulated`).

**The `col_size` argument feeds the row offset, not just the slice length.** Both
`MemBatch::get_col_ptr` (`storage/batch.rs:590`) and the cursor `col_ptr`
(`storage/read_cursor.rs:1212` → `col_ptr_by_logical`) compute
`off = region_base + row * col_size`. Passing `8` for a 4-byte column therefore
reads the **wrong bytes for every row after the first** (and runs off the region
for the last rows) — not a benign over-read. So once §3a makes the output column
narrow, these reads silently corrupt every group beyond the first unless fixed in
the **same** change. §3a/§3b/§3c must land atomically.

**Reconstruction must be width-gated, not source-type-dispatched.** The
read-back has to rebuild the 8-byte `i64` accumulator from the emitted bytes. The
correct discriminator is the **output column width**, not the agg's source type:

- An 8-byte output column holds the raw accumulator bits verbatim. This covers
  SUM, COUNT/COUNT_NON_NULL, `I64`/`U64` MIN/MAX, **and float MIN/MAX** (which
  widens to `F64` and stores `f64::to_bits`, even for an `F32` *source*).
- A narrow (<8-byte) output column is only ever a narrow-integer MIN/MAX value;
  `decode_signed` sign/zero-extends it back into the slot exactly as the load
  path produced it.

Dispatching on source type instead would be wrong two ways: a float MIN/MAX
stores `F64` bits under an `F32` source type, so `decode_float(.., F32)` would
read 4 of the 8 stored bytes and misinterpret them; and COUNT can carry a
non-integer `col_type_code` (e.g. `COUNT(uuid_col)` → `UUID`, `COUNT(name)` →
`String`), so `decode_signed(.., UUID)` hits its `unreachable!()` and panics.
Gating on `len == 8` sidesteps both — the narrow branch is reached only for
narrow integers, where `decode_signed` is exactly right.

## 3. Edits

### 3a. `agg_output_type` (engine) — `compiler.rs:965`

MIN/MAX over a non-float column returns the **source type**. The binder
(`binder.rs:308-319`) and the planner (`planner.rs:2381-2389`) both reject MIN/MAX
on `U128`/`UUID`/`I128`/`String`/`Blob`, so a MIN/MAX `col_type_code` reaching
here is always a ≤8-byte integer or a float. `col_type_code as u8` is sound:
`TypeCode`'s `#[repr(u8)]` discriminants are defined to equal the matching
`type_code::*` constants (`gnitz-wire/src/types.rs:29-45`).

```rust
const fn agg_output_type(agg_op: AggOp, col_type_code: TypeCode) -> u8 {
    match agg_op {
        AggOp::Count | AggOp::CountNonNull | AggOp::Null => type_code::I64,
        AggOp::Sum => {
            if col_type_code.is_float() { type_code::F64 } else { type_code::I64 }
        }
        // MIN/MAX select an existing row, so the extremum is representable in the
        // source type. Float → F64; every ≤8-byte integer keeps its own type (the
        // binder/planner reject MIN/MAX on 16-byte / string / blob upstream).
        AggOp::Min | AggOp::Max => {
            if col_type_code.is_float() { type_code::F64 } else { col_type_code as u8 }
        }
    }
}
```

Update the in-module unit test `test_agg_output_type` (`compiler.rs:2858-2869`):

- `agg_output_type(Min, I32)` now `type_code::I32` (was `I64`) — the assertion at
  `:2862` flips.
- `agg_output_type(Max, String)` now returns `type_code::STRING` (the `else`
  branch is type-preserving). String never reaches the function in practice
  (rejected upstream), so the direct-call assertion at `:2864` is no longer a
  meaningful contract — drop it (or assert the new pass-through) and replace it
  with the reachable narrow cases: `Min/Max` on `I8`/`I16`/`U8`/`U16`/`U32` each
  return their own `type_code`.
- The `U64`→`U64` (`:2867-2868`) and `Sum`/`Count`→`I64` cases stay green.

### 3b. Planner — both type mirrors must move in lockstep

The planner encodes the same widening rule in **two** places, and both feed the
physical reduce/post-map layout. A mismatch with §3a silently scrambles the
view's output column positions, widths, and types.

1. **`agg_result_type` — `planner.rs:2223`.** Feeds `AggMapping.output_type`
   (`:2394`, `:2790`), i.e. the user-visible view `ColumnDef` (`:2643`).

   ```rust
   fn agg_result_type(func: AggFunc, src_col: Option<usize>, schema: &Schema) -> TypeCode {
       match func {
           AggFunc::Count | AggFunc::CountNonNull => TypeCode::I64,
           AggFunc::Avg => TypeCode::F64,
           AggFunc::Sum => match src_col {
               Some(c) if schema.columns[c].type_code.is_float() => TypeCode::F64,
               _ => TypeCode::I64,
           },
           AggFunc::Min | AggFunc::Max => match src_col {
               Some(c) if schema.columns[c].type_code.is_float() => TypeCode::F64,
               Some(c) => schema.columns[c].type_code,   // preserve the source type (incl. U64)
               None => TypeCode::I64,
           },
       }
   }
   ```

2. **The inline `reduce_schema` agg-column type — `planner.rs:2476-2486`.** This
   hand-rolled copy builds `reduce_schema`, the planner's model of the engine's
   physical reduce output. It is read to (a) type the post-map `copy_col` that
   moves each agg value into the finalized view row (`:2640`, byte width =
   `reduce_schema.columns[agg_col].type_code.size()`), (b) the GROUP BY group-col
   post-map (`:2571`), and (c) compile the HAVING expression (`:2677`). If it
   stays `I64` while §3a emits a narrow column, the finalize program copies 8
   bytes out of a 4-byte column and HAVING binds the wrong width — the same
   offset corruption as §2, on the planner side.

   Replace the inline widening with the shared rule so there is one source of
   truth (the `agg_specs` op code maps to `AggFunc` via `AggFunc::from_wire`,
   `gnitz-wire/src/circuit.rs:116`):

   ```rust
   // planner.rs:2476-2486
   let func = AggFunc::from_wire(op).expect("agg_specs holds valid agg op codes");
   let type_code = agg_result_type(func, Some(col), &source_schema);
   ```

   AVG never reaches this loop (it is expanded to separate SUM + COUNT specs at
   `:2610-2612`), so every `op` here is Count/Sum/Min/Max/CountNonNull — all
   handled by `agg_result_type`.

### 3c. Width-aware agg read-backs — the three sites in §2

Add one helper next to the decoders in `ops/reduce/agg.rs` (exposed to the
sibling op modules; `decode_signed` itself stays private behind it):

```rust
/// Reconstruct the 8-byte accumulator bits from an emitted agg column.
///
/// `bytes.len()` is the *output* column width. An 8-byte column (SUM, COUNT,
/// I64/U64/F64 — including F32 MIN/MAX, which widens to F64) holds the raw
/// accumulator bits verbatim. A narrow (<8) column is only ever a narrow-integer
/// MIN/MAX value; `decode_signed` sign/zero-extends it into the slot exactly as
/// the load path produced it. Width-gating (not a source-type dispatch) is
/// load-bearing — see §2.
pub(super) fn readback_agg_bits(bytes: &[u8], src_tc: TypeCode) -> u64 {
    if bytes.len() == 8 {
        u64::from_le_bytes(bytes.try_into().unwrap())
    } else {
        decode_signed(bytes, src_tc) as u64
    }
}
```

Each site reads the **output column width** (so the row offset is correct) and
passes the agg's **source type** for the narrow branch:

- `op_reduce.rs:455` — `output_schema` is in scope (`:148`), `agg_descs` (`:150`):
  ```rust
  let w = output_schema.columns[agg_col_idx].size() as usize;
  let ptr = trace_out_cursor.col_ptr(agg_col_idx, w);     // w, not 8 → correct offset
  old_vals[k] = if ptr.is_null() { 0 } else {
      readback_agg_bits(unsafe { std::slice::from_raw_parts(ptr, w) },
                        agg_descs[k].col_type_code)
  };
  ```
- `op_gather.rs:57` (combine) — schema is `partial_schema`, `agg_descs` is a param.
  The combine loop already binds `let w = smb.get_weight(idx)` (the **row weight**,
  `i64`, drives the `combine` vs `merge_accumulated(-1)` branch), so the column
  width must use a different name — shadowing `w` would break the branch and trip
  a `usize < 0` lint:
  ```rust
  let cw = partial_schema.columns[agg_col_idx].size() as usize;   // column width ≠ row weight
  let bits = readback_agg_bits(smb.get_col_ptr(idx, pi, cw), agg_descs[k].col_type_code);
  ```
- `op_gather.rs:84` (gather retraction) — mirror `op_reduce.rs:455`, reading width
  from `partial_schema.columns[agg_col_idx].size()` (no row-weight `w` is in scope
  in the retraction block, so `w` is free to reuse there).

`combine` and `merge_accumulated` require the fully extended value (cross-worker
MIN/MAX comparison, and the U64 unsigned-compare path), which `readback_agg_bits`
delivers. For every existing 8-byte output column the helper is a verbatim
`u64::from_le_bytes`, so the change is behavior-preserving for SUM/COUNT/AVG and
`I64`/`U64`/float MIN/MAX.

## 4. Knock-on checks

- **AVI** (`apply_agg_from_value_index`, `agg.rs:405`) keys on the **source**
  column type (`avi_agg_col_type_code`, taken from the *input* register schema at
  `compiler.rs:1863`) and stores an 8-byte ordered encoding internally —
  orthogonal to the output column type. The AVI's own 8-byte read
  (`bench_secondary_index.rs:292`, `get_col_ptr(.., 8)`) is the encoded `av`
  column, always 8 bytes. No change.
- **GROUP BY post-map** (`planner.rs:2640`/`:2571`) and **HAVING**
  (`planner.rs:2677`) copy/bind the agg column at
  `reduce_schema.columns[..].type_code`. With §3b part 2 that is now the source
  type, matching the engine's physical reduce output (§3a) and the view
  `ColumnDef` (`m.output_type`, §3b part 1). All three widths agree.
- **MIN/MAX `is_nullable`** (NULL for an all-NULL group) is unchanged: the
  finalize path reads the raw output's null word at runtime (`emit.rs:170`), and
  the view `ColumnDef` already marks SUM/MIN/MAX nullable (`planner.rs:2649`),
  independent of the agg column's declared width.
- **Cross-process / cross-worker wire encoding** serializes region-by-region at
  the schema's `col_size`, so a narrower agg column encodes narrower on the
  partial-aggregate exchange; no fixed-8 assumption remains once §3c lands (the
  only fixed-8 agg reads in the tree are the three §2 sites — verified by sweep).

## 5. Testing

- **Narrow MIN/MAX round-trip (single worker).** `GROUP BY k, MIN(v)` /
  `MAX(v)` where `v` is `INT` (I32), `SMALLINT` (I16), `INT UNSIGNED` (U32),
  `TINYINT` (I8): assert the view column type equals the source type and the
  values are correct, including negatives for signed and values above
  `i32::MAX`/`i16::MAX` for the unsigned widths (the zero-extension path).
- **Narrow MIN/MAX across workers.** The same with enough distinct groups to
  spread across all workers and per-group rows split across workers, so the
  gather-reduce combine (`op_gather.rs:57`) and its retraction read
  (`op_gather.rs:84`) run on a narrow column — the regression guard for §3c and
  the §2 offset corruption (≥2 groups in one partial batch).
- **F32 MIN/MAX across workers stays F64.** `MAX(v)` on a `FLOAT` (F32) column
  with groups split across workers: assert the view column is `F64` and the value
  round-trips. Guards the width-gated read-back against a source-type dispatch
  that would mis-decode the stored F64 bits as F32.
- **Retraction.** Update a group's extremal row so the old MIN/MAX is retracted
  and a new one emitted; assert the retraction reads the narrow old value
  (`op_reduce.rs:455`) correctly, signed and unsigned-above-`i32::MAX`.
- **Post-map + HAVING on a narrow agg.** `SELECT k, MAX(v) AS m FROM t GROUP BY k
  HAVING MAX(v) > <threshold>` with `v` narrow (e.g. I16 with values straddling
  the threshold): assert the projected `m` is the narrow type and HAVING filters
  correctly — covers the `reduce_schema` mirror (§3b part 2).
- **8-byte unchanged.** Existing `BIGINT`/`BIGINT UNSIGNED`/float MIN/MAX and all
  SUM/COUNT/AVG tests stay green (the `len == 8` path is verbatim).
- **Unit test.** Update `compiler.rs:2858-2869` per §3a (the `Min, I32` and
  `Max, String` assertions; add narrow positives).

## 6. Scope

- **MIN/MAX output type only.** No change to SUM/COUNT/AVG, to the accumulator
  arithmetic, to the AVI, or to grouping/PK layout.
- **16-byte MIN/MAX stays rejected** by the binder/planner (no 8-byte
  accumulator). This plan does not add 16-byte MIN/MAX support.
- **Atomic landing.** §3a (engine output width), §3b (both planner mirrors), and
  §3c (engine read-backs) are one change; any subset corrupts agg values via
  width/offset mismatch.
- **Shared read-back loops.** §3c rewrites the same three loops
  (`op_reduce.rs:451-462`, `op_gather.rs:50-66`, `op_gather.rs:81-104`) that
  `plans/reduce-gather-null-agg-state-propagation.md` rewrites to honor the old
  null word. The two are orthogonal — this plan fixes the value *width*, that one
  fixes *null-ness* — but they overlap line-for-line, so merge them rather than
  applying both blindly. Both replace `op_gather`'s `pi = agg_col_idx - 1` with
  `partial_schema.try_payload_idx(agg_col_idx)` (the null-bit/value indexing the
  closed form gets wrong under a compound PK).
- **Unblocks** the relaxed ≤8-byte range-column requirement in
  `plans/band-left-bag-multiplicity-overfill.md`; that plan's threshold builder
  consumes the source-typed reduce result directly with no relabel and no narrow
  cap, and removes the `planner.rs:1483-1492` guard.
