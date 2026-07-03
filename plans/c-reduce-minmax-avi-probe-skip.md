# Reduce MIN/MAX: skip the AVI probe for insert-only groups

## Problem

Every epoch, `op_reduce`'s combined-AVI branch probes the aggregate value
index once per touched group per MIN/MAX aggregate, unconditionally
(`ops/reduce/op_reduce.rs` ~577-585 → `apply_agg_from_value_index`,
`ops/reduce/agg.rs` ~441-469). Each probe is `seek_first_positive_with_prefix`
(`storage/lsm/read_cursor/mod.rs` ~475-485): an absolute `seek_bytes` —
full-height `find_lower_bound_bytes` in every source, plus a loser-tree rebuild
with two Vec allocations for `SourceMode::Multi` cursors (≥3 sources;
`read_cursor/mod.rs` ~223-233, 256-261; `storage/repr/heap.rs` build) —
followed by a ghost-skipping walk.

In the recorded `view_maintenance` bench profile
(`benchmarks/results/20260702_095803/w4_c1/perf.data`) this call chain
(`op_reduce → seek_first_positive_with_prefix →
{Batch,MappedShard}::find_lower_bound_bytes`) is **8.42%** of worker cycles
(3.36% Batch + 5.06% MappedShard). The workload is insert-dominant — per
iteration one batched INSERT of 4,000 rows vs 12 single-row UPDATEs and 3
single-row DELETEs (`benchmarks/combined/test_view_maintenance.py`) — yet every
touched group pays the index probe as if its extreme might have receded. With
`OPEN_PROB=0.5` over `REGION_CARD=200,000` group buckets, an INSERT epoch
touches ~2,000 groups, **none** of which can retract an existing row; only the
15 single-row UPDATE/DELETE epochs carry a retraction. So **≈99% of
group-touch instances re-derive from the index an extreme they could compute
from the delta and the trace_out row already in hand.**

**The fix.** A MIN/MAX extreme can only *recede* when a retraction removes the
last row carrying it. If a group's consolidated delta is **all inserts** (no
weight ≤ 0 row), the extreme can only rise: `new = combine(old_extreme,
extreme of the delta's inserts)`, computed from the reduce's own accumulator and
the old value already under the trace_out cursor — no index seek. Any group with
a retraction probes, exactly as today. The AVI *population*
(`op_integrate_with_indexes`, `ops/index.rs` ~77-166) stays unconditional — the
index must always reflect `I(input)` for the probes that do run. This plan does
not change the AVI key encoding, so it touches no layout tests.

The motivating view is integer-typed and payload-grouped
(`MIN(o_amount)/MAX(o_amount) GROUP BY c_region`, `o_amount` BIGINT, `c_region`
a non-PK payload column). The design keeps to the simplest rule that captures it:
a coarse per-group "did this group recede?" bit, integer aggregates only, reusing
the machinery the reduce already has.

## Ground facts this design builds on

- **AVI key layout / population.** `group-prefix ‖ ordinal(1 byte) ‖
  av(8 bytes BE)`; all-PK schema (`make_avi_schema`, `ops/index.rs` ~45-67),
  one entry per (input row × value-indexed aggregate), entry weight = the row's
  weight, NULL aggregate values skipped (`ops/index.rs` ~105-163).
- **Encoding contract.** `encode_ordered(bytes, tc, for_max)` (`ops/util.rs`
  ~232-249): unsigned raw / signed sign-flip (`+1<<63`) / IEEE total-order for
  floats, then bitwise NOT for MAX. The MIN/MAX accumulator holds the same
  **MIN-oriented** `encode_ordered(.., for_max=false)` u64; `step_from_batch`
  updates it with a single unsigned u64 extreme compare (`agg.rs` ~243-254);
  `apply_agg_from_value_index` un-inverts (`!av`) and reseeds via
  `seed_from_raw_bits` (`agg.rs` ~441-469); `get_value_bits` decodes at emit
  (`agg.rs` ~154-162). Aggregate values admit fixed ints and floats
  (`agg_value_idx_eligible`, `compiler/optimize.rs` ~447-456).
- **The reduce already owns a per-aggregate accumulator.** `accs[k]`
  (`op_reduce.rs` ~266) is built on `input_schema`, reset per group
  (`op_reduce.rs` ~491-493). Today the group-delta walk steps **only linear**
  accumulators (`op_reduce.rs` ~507-511, gated `if acc.is_linear()`); the AVI
  owns each MIN/MAX, so its `accs[k]` is left untouched until
  `apply_agg_from_value_index` seeds it. `step_from_batch` NULL-gates
  (`agg.rs` ~185) and reads the aggregate column through
  `ColumnLocator::native_le_bytes` (`agg.rs` ~215), which OPK-decodes a
  PK-source column's at-rest bytes to native LE — so stepping `accs[k]` handles
  PK-source aggregates (`SELECT MAX(pk_col) … GROUP BY …`, E2E
  `test_pk_source_min_group_by_full_pk`) correctly, for free.
- **`fold_old_aggs` skips MIN/MAX** (`agg.rs` ~386-408): it folds only the
  linear companions off the still-positioned `trace_out` cursor.
  `merge_accumulated` is `unreachable!` for MIN/MAX.
- **The old MIN/MAX value is in the trace_out row.** `emit_agg_col` writes every
  aggregate column uniformly (`emit.rs` ~16-28, 61); the stored extreme sits in
  the trailing agg column at output width — source width for ints, F64 for float
  sources (`agg_output_type`, `compiler/optimize.rs` ~385-393) — with a null bit
  for all-NULL groups.
- **AVI Integrate precedes Reduce** (`compiler/emit.rs` ~802-813): on the probe
  path the index already reflects post-delta `I(input)`.
- **Reduce inputs are bag-positive.** A reduce reads only a *materialized*
  relation's delta (base table, or a hidden/committed view), optionally through a
  linear filter/map — the planner splits every composite body into circuits
  joined by materialized views (`gnitz-sql` `plan/view/{group_by,dispatch}.rs`,
  `plan/lp.rs`). Every relation-producing operator's *integral* is per-(PK,
  payload) ≥ 0: base tables by DML; filter/map/union-all preserve positivity;
  DISTINCT/INTERSECT/EXCEPT clamp via `distinct`/`positive_part`; joins multiply
  positive weights; outer-join null-fill uses `positive_part`; a reduce emits one
  +1 row per surviving group and its −1 retraction always cancels a +1 already in
  the downstream integral. `Negate` produces a negative integral but appears only
  *inside* `positive_part`/set-op algebra, never as a view output, so it is never
  a reduce input. This is what makes "all-inserts ⟹ extreme only rises" exact.

## Semantics

Per group, one shared bit across the group's MIN/MAX aggregates:

- `saw_negative` = the group's **consolidated** delta contains a row with
  weight ≤ 0.

Per value-indexed aggregate `k`:

- **Probe the AVI** iff `saw_negative`, **or** `k`'s source column is float,
  **or** the group's tracked delta-row count hit the cap (below).
- **Skip** otherwise (integer aggregate, all-insert group, within cap): the new
  extreme is `combine(old, pos)`, where `pos` is the extreme over the group's
  positive-weight, non-NULL delta rows (held directly in the reduce's own
  `accs[k]`, stepped during the group walk) and `old` is the stored extreme from
  the trace_out row (absent when the group is new or previously all-NULL). If
  neither is present, `accs[k]` stays untouched and renders NULL.

Correctness:

- **Skip ⟹ pure inserts.** Every delta row has weight > 0, so (bag positivity)
  `I(input)` only grows: no row is removed, and `accs[k]` after the walk holds
  the max/min over the inserted rows. Folding `old` gives `combine(old, pos) =
  extreme(I + δ)` exactly. A same-epoch retract+insert of one (PK, payload) nets
  to weight 0 and is dropped by consolidation, so it never appears as a positive
  row and never falsely suppresses `saw_negative`.
- **Coarse is a correct superset.** Any group with a retraction probes, which is
  always correct. This forgoes the finer "skip a retraction that lands strictly
  below the extreme" optimization; on realistic workloads that residual is far
  below the bench's ±3-4% noise floor (for the motivating view, ~9 extra probes
  per 4,000-row iteration), and it is not worth a per-aggregate old-vs-retraction
  comparison or the extra tracking state it needs.
- **Float always probes** — today's exact behavior. It sidesteps the one
  encoding hazard a skip would introduce: an F32 source's `old` is stored as F64
  and would need re-encoding on the F32 scale (`old_f64 as f32`), and Rust float
  casts do not guarantee NaN payload preservation. Integer sources have no such
  hazard: the output slot is native-LE at the source width, so
  `encode_ordered` round-trips the stored `old` exactly and bijectively.
- **Probe path unchanged.** `apply_agg_from_value_index` resets/overwrites
  `accs[k]` from the post-delta index (`agg.rs` ~464, 467), discarding any
  positive-row value pre-stepped into it — so pre-stepping never corrupts a
  probing group.

## Implementation in `op_reduce`

Reuse the existing accumulators; no new per-aggregate tracking structs. The only
new state is two `usize`/`bool` locals per group.

**Delta walk** (`op_reduce.rs` ~494-513). Track `saw_negative`, and pre-step each
MIN/MAX `accs[k]` on positive rows (bounded by the cap). `track_nonlinear =
avi.is_some()` is loop-invariant (the non-AVI replay path handles MIN/MAX itself
and must stay untouched):

```rust
let mut saw_negative = false;      // per group
let mut tracked = 0usize;          // per group: rows fed to the MIN/MAX accs
while idx < n {
    // ... group-membership break as today ...
    let w = mb.get_weight(curr_idx);
    if w <= 0 { saw_negative = true; }
    for acc in accs.iter_mut() {
        if acc.is_linear() {
            acc.step_from_batch(&mb, curr_idx, w);
        } else if track_nonlinear && w > 0 && tracked < SKIP_TRACK_CAP {
            // Pre-step the delta's inserts into the MIN/MAX accumulator so it
            // holds `pos` for the skip path; overwritten if the group probes.
            acc.step_from_batch(&mb, curr_idx, w);
        }
    }
    if track_nonlinear && w > 0 { tracked += 1; }
    idx += 1;
}
let capped = tracked >= SKIP_TRACK_CAP && idx - group_start_pos > SKIP_TRACK_CAP;
```

`SKIP_TRACK_CAP` (a small constant, ~128) bounds the added per-group work. The
pre-step is O(positive delta rows); the probe it saves is a fixed O(log N) seek,
so beyond ~this many delta rows in a group, computing the extreme from the delta
costs more than probing. Capped groups force-probe (their partial `accs[k]` is
overwritten by the probe, so this is correctness-neutral). This keeps the
optimization net-beneficial across the whole grouping-cardinality range — a
low-cardinality GROUP BY, a boot backfill (whole table in one epoch), or a
large single-key batch never pays an unbounded pre-step to save one cheap probe.

**Probe/skip block** (replaces the unconditional probe loop, `op_reduce.rs`
~577-585). The `gk` group-key gather stays as today (once per group, cheap,
needed by the probe arm):

```rust
for (j, (k, d)) in agg_descs.iter().enumerate()
    .filter(|(_, d)| d.agg_op.uses_value_index()).enumerate()
{
    if saw_negative || d.col_type_code.is_float() || capped {
        gk[gstride] = j as u8;
        apply_agg_from_value_index(avi_c, &gk[..gstride + 1], d.agg_op == AggOp::Max, &mut accs[k]);
    } else if has_old {
        // accs[k] already holds `pos` (or is untouched → NULL); fold in `old`.
        if let Some(enc) = read_old_minmax_encoded(trace_out_cursor, cbase + k, pbase + k,
                                                   agg_col_widths[k], d.col_type_code) {
            accs[k].merge_encoded_extreme(enc);
        }
    }
    // else: new group, all inserts — accs[k] already holds the answer (pos or NULL).
}
```

`read_old_minmax_encoded` is read off the trace_out cursor already positioned by
the unconditional `has_old` seek (`op_reduce.rs` ~519-524) and left in place by
`fold_old_aggs` (which folds the linear companions immediately above,
`op_reduce.rs` ~560-562) — a column read, not a seek. Two small helpers in
`agg.rs`:

```rust
/// The stored integer MIN/MAX value in the trace_out row's agg column as its
/// MIN-oriented encoding, or None if the null bit is set. Integer sources only
/// (float MIN/MAX always probes): the output slot is native-LE at the source
/// width, so encode_ordered is exact and bijective — no F32 rescale, no NaN.
pub(super) fn read_old_minmax_encoded(
    cursor: &ReadCursor, c_idx: usize, pi: usize, cw: usize, src_tc: TypeCode,
) -> Option<u64> {
    if (cursor.current_null_word >> pi) & 1 != 0 { return None; }
    let ptr = cursor.col_ptr(c_idx, cw);
    if ptr.is_null() { return None; }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, cw) };
    Some(super::super::util::encode_ordered(bytes, src_tc as u8, false))
}

// on Accumulator
/// Fold a pre-encoded MIN-oriented extreme into this MIN/MAX accumulator — the
/// same extreme compare as `step_from_batch`'s MIN/MAX arm.
pub(super) fn merge_encoded_extreme(&mut self, enc: u64) {
    debug_assert!(matches!(self.agg_op, AggOp::Min | AggOp::Max));
    let replaces = if self.agg_op == AggOp::Max { enc > self.acc as u64 } else { enc < self.acc as u64 };
    if !self.has_value || replaces { self.acc = enc as i64; self.has_value = true; }
}
```

The empty-delta global-ground seed path (`op_reduce.rs` ~156-202) and the
non-AVI `trace_in` replay fallback are untouched.

## Sequencing

Single self-contained change (no encoding change, no layout-test churn):

1. `read_old_minmax_encoded` and `Accumulator::merge_encoded_extreme` in `agg.rs`.
2. `saw_negative` / `tracked` / `capped` tracking + MIN/MAX pre-step in the delta
   walk, and the conditional probe/skip block in `op_reduce.rs`.
3. Behavior tests (below).

## Files touched

| File | Change |
|---|---|
| `crates/gnitz-engine/src/ops/reduce/agg.rs` | `read_old_minmax_encoded`; `Accumulator::merge_encoded_extreme` |
| `crates/gnitz-engine/src/ops/reduce/op_reduce.rs` | `saw_negative`/`tracked`/`capped` + MIN/MAX pre-step in the group walk; conditional probe/skip replacing the unconditional probe loop |
| `crates/gnitz-engine/src/ops/reduce/tests.rs` | new behavior tests |

No change to `ops/util.rs`, `ops/index.rs`, or `read_cursor`. The AVI key
encoding and population are unchanged, so every existing `avi_*` layout-pin test
stays green untouched.

## Testing

New behavior tests (`reduce/tests.rs`), all asserting against the trace-scan
reference (weights, not just row presence):

- **Randomized equivalence, mixed churn** (primary oracle): a multi-epoch stream
  of inserts, updates (retract+insert), and deletes over many groups; assert the
  AVI path equals the trace-scan reference after every epoch.
- **Skip/probe unit cases** (single group, explicit epochs): (a) insert-only
  raising MAX above old (skip; extreme rises); (b) insert-only below old (skip;
  extreme = old); (c) retraction strictly inside the range (probe by the coarse
  rule; extreme unchanged); (d) retraction of the extreme with multiplicity 2 at
  that value (probe; extreme unchanged); (e) retraction of the unique extreme
  (probe; recede to next); (f) new group all-NULL values (skip; NULL render);
  (g) group emptied to zero cardinality (cardinality gate sheds); (h) integer MIN
  and float MAX on distinct columns of one group in one insert-only epoch — the
  integer skips, the float probes, both correct.
- **Cap**: a single group with more than `SKIP_TRACK_CAP` pure-insert rows —
  force-probes (capped), result matches the reference. Confirms the cap is
  correctness-neutral.
- **Float**: F32 and F64 MIN/MAX always probe; assert results (including ±0.0 and
  NaN handling) match the reference — i.e. the today-behavior float path is
  preserved.
- **PK-source aggregate**: `MAX(pk_col)` / `MIN(pk_col) … GROUP BY …` over a
  signed and an unsigned PK column, insert-only and retract-at-extreme epochs —
  exercises `native_le_bytes` in the pre-stepped accumulator.
- **Global aggregate** (`SELECT MIN(x)/MAX(x)`) insert-only and
  retract-at-extreme epochs (V₀ group, empty prefix).

E2E: existing `make e2e` MIN/MAX suites must pass at `GNITZ_WORKERS=4`
(exchange-sharded grouped MIN/MAX, `v_ext`-shaped views).

Perf validation: interleaved A/B/A/B `make bench-full` on `test_view_maintenance`
plus `perf report` — the `op_reduce → seek_first_positive_with_prefix →
find_lower_bound_bytes` cluster should nearly vanish (it drops from every touched
group to only the ~0.75% that retract). Unlike a probe-acceleration change, this
removal is above the ±3-4% noise floor.

## Invariants preserved

- The AVI always reflects `I(input)` per (group, ordinal) — population is
  untouched; only reads are skipped when provably answer-preserving.
- Bag-positivity of reduce inputs underwrites "all-inserts ⟹ extreme only
  rises." It is an emergent planner property: a reduce is only ever wired over a
  *materialized*, positivity-clamped relation, never directly over an in-circuit
  `Negate`/set-op node. A future fusion that fed an unclamped node straight into
  a reduce would have to insert a `positive_part` before it — the same
  requirement today's probe path (which skips non-positive AVI entries) already
  imposes. The group-level `debug_assert` at `op_reduce.rs` ~649-661 is a
  weaker tripwire (net-group cardinality, only when a COUNT exists), not a proof
  of the per-element property. A per-element violation that manifests *this
  epoch* as a weight-≤0 delta row routes to the probe; the only shape that could
  mis-skip is an already-corrupted integral holding a net-negative element,
  re-inserted to net-zero this epoch (no negative row this epoch → skip). That
  state is unreachable — no circuit drives a reduce element net-negative — and it
  would equally defeat today's `seek_first_positive` probe path, so the skip is
  no weaker than the code it replaces.
- The probe path is byte-for-byte the current behavior; only all-insert integer
  groups within the cap take the skip path, whose accumulator state equals what
  the probe would have produced. Float MIN/MAX is unchanged.
- The `av` slice offset and the MIN-orientation contract of
  `seed_from_raw_bits`/`get_value_bits`/`merge_encoded_extreme` are consistent;
  MIN/MAX emit width and NULL rendering (`emit_agg_col`) are unchanged.
