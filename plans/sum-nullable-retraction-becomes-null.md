# Nullable SUM does not transition to NULL on last-value retraction

## Defect

`SUM` over a nullable column emits a concrete `0` instead of `NULL` when a
retraction removes the **last non-null contributor** from a group that survives
(via `COUNT(*)` or another aggregate). SQL `SUM` of a group with no non-null
values is `NULL`; the engine yields `0`.

Root cause: the accumulator (`ops/reduce/agg.rs`) tracks presence as a saturating
boolean `has_value`, not a non-null count. `SUM` is emitted NULL iff
`is_zero() == !has_value`. Both `step_from_batch` (non-null row) and
`merge_accumulated` (fold) set `has_value = true` and never clear it. Across a
retraction the *value* nets correctly (SUM is linear), but `has_value` cannot
fall back to `false`, so a group whose non-null count returns to 0 keeps
`has_value = true` and emits `SUM = 0`.

This is the converse of the "stays-NULL" fold defect fixed in
`plans/reduce-gather-null-agg-state-propagation.md` §B. That fold-gate keeps an
*already-NULL* SUM null; it cannot make a *non-null* SUM become null, because the
all-linear path has no history to detect the non-null count crossing zero.

No ghost results: the retraction row still cancels the old output row byte-for-byte
(old SUM was non-null, re-emitted non-null). The new row simply carries the wrong
value (`0` where `NULL` is correct).

## Reachability

- **MIN/MAX:** unaffected. Non-linear; recomputed from history+delta each tick
  (replay / AVI), so `has_value` is rebuilt and correctly drops to false.
- **SUM alongside MIN/MAX in one reduce:** unaffected — the reduce is non-linear,
  so SUM also replays history and rebuilds `has_value`.
- **All-linear SUM (SUM/COUNT only), single worker:** affected. The fast fold
  path (`op_reduce.rs:483-486`) has no `trace_in` (the compiler allocates it only
  for `!all_linear && !will_use_avi`, `compiler.rs:1803`).
- **Distributed (gather), any SUM:** affected. `op_gather_reduce` folds the old
  global SUM linearly (`op_gather.rs:101-104`) and has no per-worker input
  history, so even if every worker emitted a correct NULL partial the gather's
  old-global fold re-creates `has_value = true`.

Trigger shape: `SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k` with `v` nullable;
insert `(k, v)` and `(k, NULL)`, then retract `(k, v)`. Expected surviving row
`(k, NULL, 1)`; actual `(k, 0, 1)`.

## Reproduction

Unit test in `crates/gnitz-engine/src/ops/reduce/tests.rs` (fails on current code
*and* after the prerequisite plan's §A/§B — it is a distinct defect):

```rust
#[test]
fn sum_becomes_null_when_last_value_retracted() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    // in: pk(U64), grp(I64), val(I64 nullable); out: pk(U128), grp, count, sum.
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
    let sum_null_bit = 1u64 << 2; // sum is payload index 2

    // Tick 1: group 10 = {(pk1,val=5),(pk2,val=NULL)} → COUNT=2, SUM=5.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
        b.sorted = true; b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out1, _) = op_reduce(&delta1, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &aggs,
        None, false, TypeCode::U64, None, 0, None, None);
    assert!(out1.as_mem_batch().get_null_word(0) & sum_null_bit == 0, "tick1 SUM=5");

    // Tick 2: retract (pk1, val=5). Group 10 = {(pk2,val=NULL)} → COUNT=1, SUM=NULL.
    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.sorted = true; b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out2, _) = op_reduce(&delta2, None, to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &aggs,
        None, false, TypeCode::U64, None, 0, None, None);
    let mb2 = out2.as_mem_batch();
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("insert row");
    assert!(mb2.get_null_word(new_row) & sum_null_bit != 0,
        "SUM must become NULL when the last non-null value is retracted (null_word={:#x})",
        mb2.get_null_word(new_row));
}
```

E2E (`crates/gnitz-py/tests/`), both `GNITZ_WORKERS=1` and `GNITZ_WORKERS=4`, on
`SELECT k, SUM(v) AS sm, COUNT(*) AS c FROM t GROUP BY k` with `v` nullable:
insert `(k, 5)` and `(k, NULL)`, then retract `(k, 5)`; assert the surviving row
has `sm IS NULL` and `c = 2`. The 4-worker run is what exercises the gather
old-global fold path.

## Fix

SUM's *value* is linearly maintainable; its *NULL-ness* is a boundary condition
(`NULL ⇔ non-null count == 0`), which the saturating `has_value` bool cannot
track across retractions. A correct incremental answer needs the non-null count.

**Recommended — companion `COUNT_NON_NULL`.** For each nullable `SUM(v)` the
planner emits a hidden companion `COUNT_NON_NULL(v)` on the same column; `SUM` is
written NULL iff its companion count `== 0`. The count is linear and is maintained
by the *same* fold (`op_reduce`) and combine + old-global fold (`op_gather`)
already in place, so it fixes the single-worker *and* distributed cases and keeps
the linear fast path. A real count reaches 0 on the last retraction where the
bool cannot.

Threading (needs a design pass on exactly where the companion lives):
- Planner (`gnitz-sql/src/planner.rs`, agg-spec construction near `:2563`):
  inject the companion descriptor for every nullable-source `SUM`; record the
  SUM→companion pairing.
- Reduce output schema / trace: one extra non-nullable I64 column per paired SUM.
- Emit (`emit_reduce_row`/`emit_gather_row`): null-mark the SUM column from its
  companion slot instead of `is_zero()`. (Equivalently, fold the decision into
  the finalize program so the companion never reaches user output.)
- Strip the companion before user-visible output (it must not appear in the
  view's projected schema).

**Rejected — force history replay for nullable SUM.** Making nullable SUM
non-linear (exclude it from `op_reduce.rs:163` / `compiler.rs:1779` `all_linear`,
and allocate `trace_in` for it) repairs the single-worker case via replay, but
**not** gather: the gather has no input history and still folds the old global
SUM linearly, so `GNITZ_WORKERS > 1` stays wrong. It also pays a full history
replay per epoch for a previously O(delta) aggregate. Incomplete; do not pursue.

## Scope

- Correctness only; no change to SUM's value arithmetic or to which groups emit.
- Prerequisite: `plans/reduce-gather-null-agg-state-propagation.md` (its §A
  retraction null-word emit and §B fold-gate keep an *already*-NULL SUM null;
  this plan adds the *becomes*-NULL transition on top).
