# Reduce/gather NULL aggregate-state propagation

## Goal

Make REDUCE (and the latent gather-REDUCE) preserve the NULL-ness of an aggregate
value across **retractions**, **old-state folds**, and **cross-worker combines**. A
group whose `MIN`/`MAX`/`SUM` is NULL (every contributing value is NULL) is a
legal, reachable state — it is kept alive in the output by any non-null
co-aggregate (`COUNT(*)`, a second column's aggregate). Two reachable paths drop
that NULL-ness and substitute a concrete `0` — the **retraction emit** and the
**all-linear fold**, both in `op_reduce` — producing ghost rows and
`0`-instead-of-`NULL` results in single- and multi-worker views. A third path, the
combine in `op_gather_reduce`, has the same defect but is **not reachable today**
(§C, see "The defects"). The insert path is already correct (`emit_*_row`
null-marks a zero accumulator), so the static all-NULL case is right; only the
incremental transitions are wrong.

**Scope boundary.** This plan re-emits an aggregate that is *already* NULL
(retraction, linear fold, gather combine). It does **not** make a *non-null*
`SUM` over a nullable column transition *to* NULL when a retraction removes its
last non-null contributor while the group survives — the all-linear fold lacks
the non-null count needed to detect that crossing. That converse defect is tracked
in `plans/sum-nullable-retraction-becomes-null.md`; §B's fold-gate is its
prerequisite.

None of the three paths has any existing coverage: every MIN/MAX retraction and
multi-worker reduce test uses a `NOT NULL` aggregate column, and the one all-NULL
mixed-aggregate test
(`crates/gnitz-sql/tests/planner_group_by.rs::test_aggregate_all_null_group_emits_null`)
stops after the inserts — it never retracts or mutates the group. The three unit
tests under Testing reproduce each defect and fail on current code.

## The defects

The aggregate accumulator (`ops/reduce/agg.rs`) tracks a value as
`(acc: i64, has_value: bool)`; `is_zero()` returns `!has_value`, and the row
emitters null-mark a column iff `is_zero()`. NULL-ness of an *already emitted*
value therefore lives only in the output null word — never in `acc`. The output
null word is **read back nowhere**, so every path that reconstructs old state
treats a NULL aggregate as `0`.

1. **Retraction re-emits NULL as `0`** (`ops/reduce/emit.rs`,
   `emit_reduce_row:57-71`, `emit_gather_row:233-245`). The null-marking branch
   is gated `!use_old_val && accs[..].is_zero()`, so a retraction
   (`use_old_val = true`) *never* sets the null bit — it writes `old_vals[k]`
   (which is `0` for a previously-NULL column) as NOT NULL. A retraction must be
   byte- and null-identical to the row it cancels; `compare_rows` ranks
   `null < non-null` (`foundations.md` §5), so a NOT-NULL-`0` retraction does not
   consolidate against the NULL row it should cancel. Both survive → the view
   keeps a ghost `(…, agg = NULL)` row plus a spurious `(…, agg = 0)@-1`. Affects
   every aggregate whose raw null bit can be set for an all-NULL group:
   `MIN`/`MAX`, `SUM`, **and `COUNT_NON_NULL`** (its raw bit is set too — it only
   *decodes* as `0` because its output column is schema-non-nullable; see
   `test_aggregate_all_null_group_emits_null:483-486`).

2. **Folding an old NULL SUM corrupts the new row** (`ops/reduce/op_reduce.rs:483-486`).
   In the all-linear fast path the new aggregate is `old + delta`, applied via
   `merge_accumulated(old_vals[k], 1)`. `merge_accumulated` for `Sum`/`Count`/
   `CountNonNull` **unconditionally** sets `has_value = true` (`agg.rs:237,249`).
   Folding a previously-NULL `SUM` (old bits `0`) flips `has_value`, so the new
   emit writes `SUM = 0` NOT NULL instead of NULL. A previously-NULL
   `COUNT_NON_NULL` is corrupted the same way by the same fold.

3. **Gather reads NULL partials as `0`** — *latent: `op_gather_reduce` is not
   wired into SQL planning.* The combine loop (`ops/reduce/op_gather.rs:50-66`)
   reads each partial's agg bytes and calls `combine(bits)` with no null check; a
   NULL partial (a worker that saw only NULLs for the group) would inject
   `combine(0)`, corrupting `MIN`/`MAX` (e.g. global `MIN(5, 0) = 0`) and `SUM`.
   The retraction old-global fold (`:101-104`) likewise folds the old global
   unconditionally — a NULL old `MIN` reaches `combine(0)`. The defect is real in
   the code but **unreachable today**: distributed SQL reduce scatters rows by
   group key (`ExchangeShard`, via `reduce_multi`) and runs **one local
   `op_reduce` per worker**, so a group is never split into per-worker partials
   that need combining. `op_gather_reduce` / `OpNode::GatherReduce` is constructed
   only by a compiler unit test (`compiler.rs:4355`); the wire format reserves the
   opcode for a future GatherReduce planning milestone
   (`gnitz-wire/src/circuit.rs:224`). §C keeps the function correct for when that
   milestone wires it up; it can equally be deferred to that milestone.

## Invariant

> A retraction row must reproduce the exact `(payload bytes, null bits)` of the
> output row it cancels. NULL aggregate state is carried only in the output null
> word, so every reconstruction of old aggregate state (retraction emit, linear
> fold, gather combine, gather fold) must read and honor that null word, indexed
> by the **output payload index** (the bit position `emit_*_row` set).

## Edits

### A. `emit_reduce_row` / `emit_gather_row` honor the old null word — `ops/reduce/emit.rs`

Both emitters take a new `old_null_word: u64` (the trace_out null word at the
group's row) and, on the retraction path, null-mark the column iff its old null
bit is set. `out_pi` is the output payload index — exactly the bit
`emit_*_row` ORs into `null_word` — so it indexes both the source (`old_null_word`)
and the destination (`null_word`) identically, compound PK included.

Add the parameter immediately after `old_vals: &[u64]` in both signatures:

```rust
    old_vals: &[u64],
    old_null_word: u64,
    use_old_val: bool,
```

Replace the aggregate-column branch in **`emit_reduce_row`** (`:57-71`):

```rust
        if ci >= agg_base {
            // Aggregate column. A retraction re-emits the previous output row, so
            // it must reproduce that row's null bit (read back from trace_out's
            // null word); an insertion is NULL iff the accumulator saw no value.
            let agg_idx = ci - agg_base;
            let (bits, is_null) = if use_old_val {
                (old_vals[agg_idx], (old_null_word >> out_pi) & 1 != 0)
            } else {
                (accs[agg_idx].get_value_bits(), accs[agg_idx].is_zero())
            };
            if is_null {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else {
                output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);
            }
        } else if use_natural_pk {
```

Apply the identical transform to **`emit_gather_row`**'s aggregate branch
(`:233-245`) — same structure, same `out_pi`:

```rust
        if ci >= agg_base {
            let agg_idx = ci - agg_base;
            let (bits, is_null) = if use_old_val {
                (old_vals[agg_idx], (old_null_word >> out_pi) & 1 != 0)
            } else {
                (accs[agg_idx].get_value_bits(), accs[agg_idx].is_zero())
            };
            if is_null {
                null_word |= 1u64 << out_pi;
                output.fill_col_zero(out_pi, cs);
            } else {
                output.extend_col(out_pi, &bits.to_le_bytes()[..cs]);
            }
        } else {
```

After this change both emitters' aggregate branches are byte-identical;
optionally factor them into one `#[inline]` helper
`emit_agg_col(output, out_pi, cs, agg_idx, accs, old_vals, old_null_word, use_old_val, &mut null_word)`
so the two cannot drift (`plans/adaptive-minmax-agg-output-type.md` also edits
both). This fixes the retraction ghost for `MIN`/`MAX`/`SUM`/`COUNT_NON_NULL`
alike, since the branch keys purely on the old null bit.

`emit_finalized_row` needs **no** edit: it reads the *raw* output row's null
word (`emit.rs:137`, propagated at `:170`) and re-evaluates the finalize program,
so once the raw retraction row is null-correct (above) the finalized retraction
byte-matches the finalized original.

### B. `op_reduce` — capture the old null word, gate the linear fold — `ops/reduce/op_reduce.rs`

Capture the trace_out null word right after `has_old` (`:448-449`), at the
group-loop level so both emit calls see it:

```rust
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_pk_eq(trace_out_key);
        // NULL-ness of the old aggregates lives in trace_out's null word, read at
        // the group's row (valid only when has_old). Carried into the retraction
        // emit (A) and the linear fold below.
        let old_null_word = if has_old { trace_out_cursor.current_null_word } else { 0 };
```

Pass `old_null_word` to **both** `emit_reduce_row` calls — the retraction
(`:464`) and the new-value (`:581`):

```rust
            // retraction (:464)
                &old_vals, old_null_word, true,
            // new value (:581) — old_null_word unused for use_old_val=false
                &old_vals, old_null_word, false,
```

Gate the all-linear fold (`:483-486`) so a NULL old aggregate is not folded back
in. The gate keys on the **raw** old null bit (the bit `emit_reduce_row` set from
`is_zero()`), read at the agg's output payload index via `try_payload_idx`:

```rust
        if all_linear && has_old {
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let pi = output_schema.try_payload_idx(agg_col_idx)
                    .expect("agg column is a payload column");
                if (old_null_word >> pi) & 1 == 0 {
                    accs[k].merge_accumulated(old_vals[k], 1);
                }
            }
        }
```

Why this is correct for every linear aggregate. `COUNT(*)` (`Count`) never has
the bit set — `step_from_batch` sets `has_value` unconditionally — so it always
folds. `SUM` and `COUNT_NON_NULL` *can* have it set for an all-NULL group: `SUM`
skips every null row and `COUNT_NON_NULL` returns before `has_value` when the
value is null, so both leave `has_value = false` → `is_zero()` → the emit sets the
raw null bit. (The bit is set even for `COUNT_NON_NULL`, whose output column is
schema-non-nullable and therefore *decodes* to `0`; see
`planner_group_by.rs::test_aggregate_all_null_group_emits_null`.) For such a
column `old_vals[k]` is the zero-filled byte (`fill_col_zero`), so folding it via
`merge_accumulated(0, 1)` would add `0` while *spuriously* setting
`has_value = true`; skipping the fold leaves the new row's null-ness driven solely
by the delta step — exactly what keeps an already-NULL aggregate NULL. Combined
with §A's retraction null-marking, this also removes the `COUNT_NON_NULL` ghost,
not only `SUM`'s.

The MIN/MAX (non-linear) new value is recomputed from history+delta via the
replay/AVI/GI path (`:487-575`), which resets and re-steps the accumulators — it
never folds `old_vals` and naturally yields NULL for an all-NULL group, so its new
value is already correct. Only the retraction emit (A) is needed for the
non-linear path; the fold gate above is needed only for the linear fast path.

### C. `op_gather` — null-gate the combine and the old-global fold — `ops/reduce/op_gather.rs`

*Latent path.* `op_gather_reduce` is not wired into SQL planning (see defect 3);
these edits keep the function correct for the future GatherReduce milestone, and
the unit test below pins the intended behavior. They can be deferred to that
milestone, which (per `gnitz-wire/src/circuit.rs:224`) reworks the opcode and is
the natural owner of the combine loop. `plans/adaptive-minmax-agg-output-type.md`
§3c rewrites these same loops for value *width* and adopts the identical
`try_payload_idx` change — land the two together.

Capture the old null word after `has_old` (`:77-79`), mirroring §B:

```rust
        let has_old = trace_out_cursor.valid
            && trace_out_cursor.current_pk_eq(group_pk_bytes);
        let old_null_word = if has_old { trace_out_cursor.current_null_word } else { 0 };
```

Rewrite the combine loop (`:50-66`). Read the per-row null word, skip NULL
partials, and index by the true payload slot (`try_payload_idx`, not
`agg_col_idx - 1`). The `agg_col_idx - 1` closed form is wrong for a compound
output PK — `get_col_ptr` wants a payload index, and the closed form is off by
`num_pk_cols - 1`, so it would read the wrong agg column. `try_payload_idx` is
correct at any PK arity and is also the slot the null bit must be read at (the
position the emitter wrote). The row weight stays `w`; do **not** shadow it with
the column width:

```rust
        while idx < n && smb.get_pk_bytes(idx) == group_pk_bytes
        {
            let w = smb.get_weight(idx);
            let in_null_word = smb.get_null_word(idx);
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let pi = partial_schema.try_payload_idx(agg_col_idx)
                    .expect("agg column is a payload column");
                // A NULL partial (a worker that saw only NULLs for this group)
                // contributes nothing: reading its zero-filled bytes as a value
                // would corrupt MIN/MAX (combine(0)) and SUM.
                if (in_null_word >> pi) & 1 != 0 {
                    continue;
                }
                let ptr = smb.get_col_ptr(idx, pi, 8);
                let bits = u64::from_le_bytes(ptr.try_into().unwrap());
                if w > 0 {
                    accs[k].combine(bits);
                } else if w < 0 && accs[k].is_linear() {
                    accs[k].merge_accumulated(bits, -1);
                }
            }
            idx += 1;
        }
```

In the retraction block (`:81-106`) pass `old_null_word` to `emit_gather_row`
and gate the old-global fold the same way (the old-global read at `:82-91`
already keys on the full column index via `col_ptr(agg_col_idx, 8)`, correct at
any PK arity, and zero-fills a NULL column — leave it):

```rust
        if has_old {
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let ptr = trace_out_cursor.col_ptr(agg_col_idx, 8);
                if !ptr.is_null() {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, 8) };
                    old_vals[k] = u64::from_le_bytes(bytes.try_into().unwrap());
                } else {
                    old_vals[k] = 0;
                }
            }

            emit_gather_row(
                &mut output, &smb, exemplar_row,
                group_pk, -1,
                &old_vals, old_null_word, true,
                &accs, partial_schema, num_aggs,
            );

            // Fold the old global back in — but a NULL old aggregate contributes
            // nothing (folding 0 into MIN/MAX via combine, or flipping SUM's
            // has_value, corrupts it).
            for k in 0..num_aggs {
                let agg_col_idx = num_out_cols - num_aggs + k;
                let pi = partial_schema.try_payload_idx(agg_col_idx)
                    .expect("agg column is a payload column");
                if (old_null_word >> pi) & 1 == 0 {
                    accs[k].merge_accumulated(old_vals[k], 1);
                }
            }
        }
```

Pass `old_null_word` to the new-global `emit_gather_row` (`:111`, unused for
`use_old_val=false`):

```rust
                &old_vals, old_null_word, false,
```

### D. Call sites for the new `old_null_word` parameter

- `op_reduce.rs:464`, `op_reduce.rs:581` — `&old_vals, old_null_word, <bool>`.
- `op_gather.rs:94`, `op_gather.rs:111` — `&old_vals, old_null_word, <bool>`.
- `tests.rs:1885` (`test_emit_reduce_row_compound_pk_bytes`, an insertion) —
  `&[0u64], 0, false`.

These are the only direct callers of `emit_reduce_row`/`emit_gather_row`
(`op_reduce`/`op_gather`/`op_gather_reduce` signatures are unchanged, so every
test that drives them — and every gather unit test calling `op_gather_reduce` —
compiles untouched).

## Testing

Three `#[test]`s in `crates/gnitz-engine/src/ops/reduce/tests.rs`, each verified
to **fail on current code** and to assert the fixed behavior. They reuse the
file's `op_reduce`/`op_gather_reduce` call pattern (cf. `test_reduce_sum_retraction`).
`null_min_retraction_re_emits_null` pins §A, `null_sum_fold_stays_null` pins §B,
and `gather_combine_skips_null_partial` pins §C (a unit test of the latent
`op_gather_reduce`).

```rust
#[test]
fn null_min_retraction_re_emits_null() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    // in: pk(U64), grp(I64), val(I64 nullable); out (payload GROUP BY grp):
    // pk(U128), grp(I64), count(I64), min(I64 nullable). min is payload index 2.
    let in_schema = SchemaDescriptor::new(&[
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
    ], &[0]);
    let out_schema = SchemaDescriptor::new(&[
        SchemaColumn::new(type_code::U128, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::I64, 1),
    ], &[0]);
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2] },
        AggDescriptor { col_idx: 2, agg_op: AggOp::Min,   col_type_code: TypeCode::I64, _pad: [0; 2] },
    ];
    let min_null_bit = 1u64 << 2;

    // Tick 1: (pk=1, grp=10, val=NULL) → COUNT keeps the group alive, MIN=NULL.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1; b.sorted = true; b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out1, _) = op_reduce(&delta1, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &aggs,
        None, false, TypeCode::U64, None, 0, None, None);
    assert_eq!(out1.count, 1);
    assert!(out1.as_mem_batch().get_null_word(0) & min_null_bit != 0,
        "tick1 MIN must be NULL");

    // Tick 2: (pk=2, grp=10, val=7) → MIN=7, retracts the NULL row.
    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &7i64.to_le_bytes());
        b.count += 1; b.sorted = true; b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out2, _) = op_reduce(&delta2, None, to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &aggs,
        None, false, TypeCode::U64, None, 0, None, None);
    let mb2 = out2.as_mem_batch();
    let retr = (0..out2.count).find(|&i| out2.get_weight(i) < 0).expect("retraction row");
    assert!(mb2.get_null_word(retr) & min_null_bit != 0,
        "retraction of an all-NULL MIN group must re-emit MIN as NULL to cancel \
         the tick-1 row (null_word={:#x})", mb2.get_null_word(retr));
}

#[test]
fn null_sum_fold_stays_null() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = SchemaDescriptor::new(&[
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
    ], &[0]);
    let out_schema = SchemaDescriptor::new(&[
        SchemaColumn::new(type_code::U128, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::I64, 1),
        SchemaColumn::new(type_code::I64, 1),
    ], &[0]);
    let aggs = [
        AggDescriptor { col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2] },
        AggDescriptor { col_idx: 2, agg_op: AggOp::Sum,   col_type_code: TypeCode::I64, _pad: [0; 2] },
    ];
    let sum_null_bit = 1u64 << 2;
    let null_row = |pk: u128| {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1; b.sorted = true; b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let (out1, _) = op_reduce(&null_row(1), None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &aggs,
        None, false, TypeCode::U64, None, 0, None, None);
    assert!(out1.as_mem_batch().get_null_word(0) & sum_null_bit != 0, "tick1 SUM NULL");

    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let (out2, _) = op_reduce(&null_row(2), None, to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &aggs,
        None, false, TypeCode::U64, None, 0, None, None);
    let mb2 = out2.as_mem_batch();
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("insert row");
    assert!(mb2.get_null_word(new_row) & sum_null_bit != 0,
        "SUM of a still-all-NULL group must stay NULL (null_word={:#x})",
        mb2.get_null_word(new_row));
}

#[test]
fn gather_combine_skips_null_partial() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    // partial/output: pk(U128), min(I64 nullable). min is payload index 0.
    let schema = SchemaDescriptor::new(&[
        SchemaColumn::new(type_code::U128, 0),
        SchemaColumn::new(type_code::I64, 1),
    ], &[0]);
    let agg_min = AggDescriptor { col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::I64, _pad: [0; 2] };
    let min_null_bit = 1u64 << 0;

    // Group pk=1: an all-NULL partial and a MIN=5 partial → global MIN = 5.
    let mut partial = Batch::with_schema(schema, 2);
    partial.extend_pk(1u128);
    partial.extend_weight(&1i64.to_le_bytes());
    partial.extend_null_bmp(&min_null_bit.to_le_bytes());
    partial.extend_col(0, &0i64.to_le_bytes());
    partial.count += 1;
    partial.extend_pk(1u128);
    partial.extend_weight(&1i64.to_le_bytes());
    partial.extend_null_bmp(&0u64.to_le_bytes());
    partial.extend_col(0, &5i64.to_le_bytes());
    partial.count += 1;

    let empty_out = Rc::new(Batch::empty_with_schema(&schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);
    let out = op_gather_reduce(&partial, to_ch.cursor_mut(), &schema, &[agg_min]);
    assert_eq!(out.count, 1);
    assert_eq!(crate::util::read_i64_le(out.col_data(0), 0), 5,
        "gather MIN must ignore the all-NULL partial, not combine it as 0");
}
```

E2E (`crates/gnitz-py/tests/`). Every scenario exercises the reachable `op_reduce`
paths; under `GNITZ_WORKERS=4` the reduce is sharded (`ExchangeShard` → one local
`op_reduce` per worker), confirming the per-worker fix holds when distributed. A
group is scattered whole to a single worker, so there is **no** cross-worker
partial-combine to exercise (the gather path is unwired — see defect 3). Two
distinct views, because a view that selects any MIN/MAX is non-linear and routes
SUM through the replay path, which would not exercise the §B fold-gate:

- **§A retraction (non-linear), `SELECT k, COUNT(*) AS c, MIN(v) AS mn FROM t
  GROUP BY k`, `v` nullable.** Insert an all-NULL group, then insert a non-NULL
  row into it; assert exactly one view row with `mn` equal to the non-NULL value
  and no ghost rows. Then retract the non-NULL row; assert `mn` returns to NULL
  with `c` correct and no ghosts.
- **§B fold-gate (all-linear), `SELECT k, COUNT(*) AS c, SUM(v) AS sm FROM t
  GROUP BY k`, `v` nullable** (no MIN/MAX → the reduce takes the all-linear fast
  path). Insert two all-NULL rows for one group across two pushes; assert
  `sm IS NULL`, `c = 2`, and a single row — the second push must re-emit the NULL
  SUM (§A) to cancel the first, then keep it NULL (§B).

## Scope

- **NULL-state only.** No change to accumulator arithmetic, grouping/PK layout,
  the AVI, or which groups are emitted. The deliberate suppression of a
  from-inception all-NULL group with no surviving non-null aggregate
  (`any_nonzero == false` → no row; documented in `planner_group_by.rs`) is
  unchanged.
- **Converse SUM defect is separate.** A non-null `SUM` over a nullable column
  that should become NULL when a retraction removes its last non-null value
  (group kept alive by a co-aggregate) is **not** fixed here. The all-linear fold
  carries only a saturating `has_value` bool, not the non-null count required to
  detect the 0-crossing. Tracked in
  `plans/sum-nullable-retraction-becomes-null.md`; §B's fold-gate (which keeps an
  *already*-NULL SUM null) is a prerequisite for it.
- **§C is latent.** `op_gather_reduce` is reachable only from a compiler unit test
  (`compiler.rs:4355`); SQL distributed reduce uses `reduce_multi`
  (`ExchangeShard` scatter → one local `op_reduce`), never a partial-aggregate
  combine. §C therefore fixes no user-facing bug today — it hardens the function
  (NULL-skip + compound-PK `try_payload_idx` indexing) for the future GatherReduce
  planning milestone, which owns wiring it up. Land §C with that milestone or now;
  either way it is behavior-preserving for the test-only callers.
- **Width-independent.** These fixes concern null-ness, not value width; they are
  correct at the current 8-byte agg width and at any narrower width.
- **Coordination.** §C edits the same `op_gather` loops, and §B the same
  `op_reduce` retraction block, that `plans/adaptive-minmax-agg-output-type.md`
  §3c rewrites for value *width*. Merge the two rather than applying blindly: that
  plan changes the literal `8` reads here to schema-derived widths via
  `readback_agg_bits`, and adopts the same `try_payload_idx(agg_col_idx)` in place
  of `agg_col_idx - 1`.
