# Reduce MIN/MAX: skip AVI probes for non-receding groups; canonical AVI prefix + galloping probes

## Problem

Every epoch, `op_reduce`'s combined-AVI branch probes the aggregate value index
once per touched group per MIN/MAX aggregate, unconditionally
(`ops/reduce/op_reduce.rs:529-560` → `apply_agg_from_value_index`,
`ops/reduce/agg.rs:447-475`). Each probe is
`seek_first_positive_with_prefix` (`storage/lsm/read_cursor/mod.rs:475-485`):
an absolute `seek_bytes` — full-height `find_lower_bound_bytes` in every
source plus a loser-tree rebuild with two Vec allocations
(`read_cursor/mod.rs:256-261, 223-233`) — followed by a ghost-skipping walk.

In one recorded `view_maintenance` bench profile
(`benchmarks/results/20260702_095803/w4_c1`) this is the single largest
op-level cost: ~8.4% of worker cycles
(`op_reduce → seek_first_positive_with_prefix → {Batch,MappedShard}::find_lower_bound_bytes`).
The workload is insert-dominant (4,000 INSERTs vs 15 UPDATE/DELETEs per
iteration), yet every group pays the index probe as if its extreme might have
receded. (The tree-rebuild/allocation cost applies to `SourceMode::Multi`
cursors — ≥3 sources; Single/Pair cursors pay the full-height binary
searches without the rebuild.)

Two independent inefficiencies compound:

1. **Probes run even when they cannot change the answer.** A MIN/MAX extreme
   can only *recede* when a retraction removes the last row carrying it. For a
   group whose delta contains no negative-weight row at the stored extreme,
   the new extreme is `combine(old_extreme, extreme of the delta's positive
   rows)` — no index needed.
2. **The probes that do run cannot gallop.** `GroupKeyExtractor::gather`
   (`ops/util.rs:158-165`) concatenates group columns' **raw little-endian**
   bytes, so AVI key order does not match the group-visit order
   (`compare_by_group_cols`, typed) and each probe must be an absolute seek.
   A strictly-forward `advance_to` on a live multi-source cursor instead
   routes through `seek_forward_multi` — Θ(log sources), in-place, zero
   allocation (`read_cursor/mod.rs:282-299`).

This plan fixes both: (A) a per-group, per-aggregate dynamic probe-skip, and
(B) an order-preserving (canonical) AVI group prefix so the remaining probes
gallop. The AVI *population* (`op_integrate_with_indexes`,
`ops/index.rs:76-157`) stays unconditional — the index must always reflect
`I(input)` for the probes that do run.

## Ground facts this design builds on

- AVI key layout: `group-prefix ‖ ordinal(1 byte) ‖ av(8 bytes BE)`; all-PK
  schema, one entry per (input row × value-indexed aggregate), entry weight =
  the row's weight, NULL aggregate values skipped
  (`ops/index.rs:104-154`, `make_avi_schema` at `index.rs:44-66`).
- The value part is `encode_ordered(bytes, tc, for_max)` (`ops/util.rs:231-247`):
  unsigned raw / signed sign-flip / IEEE total-order for floats, then bitwise
  NOT for MAX — *for payload-column sources*, whose `ColumnLocator::bytes`
  are native LE. (A PK-column aggregate source hands `encode_ordered` OPK
  big-endian bytes it reads as LE — a pre-existing write-side bug tracked in
  its own plan; this plan's helpers and tests use payload-column and,
  knowingly, non-AVI PK-column sources only, and Part A's
  `read_old_minmax_encoded`/tracker encodes reproduce whatever image the AVI
  holds, since encode/decode are a bijection per image.)
  `apply_agg_from_value_index` un-inverts (`!av`) and seeds the
  accumulator with the **MIN-oriented** encoding via `seed_from_raw_bits`
  (`agg.rs:447-475`).
- The MIN/MAX accumulator state is that same MIN-oriented `encode_ordered(..,
  for_max=false)` u64; `step_from_batch` compares extremes with a single
  unsigned u64 compare (`agg.rs:249-260`); `get_value_bits` decodes at emit
  (`agg.rs:154-162`).
- `fold_old_aggs` reads old *linear* aggregate values off the still-positioned
  `trace_out` cursor row and deliberately **skips MIN/MAX**
  (`agg.rs:392-414`), because today the AVI probe overwrites them anyway.
- Old MIN/MAX values ARE present in that same trace_out row: the trailing agg
  columns, at output width `agg_col_widths[k]` (source width for ints, F64
  for float sources — `agg_output_type`, `query/compiler/optimize.rs:385-393`),
  with a null bit for all-NULL groups.
- AVI group keys admit only non-nullable `is_pk_eligible` columns — fixed
  ints, U128/UUID, I128; **no floats, strings, or nullables**
  (`avi_group_key_eligible`, `query/compiler/optimize.rs:468-490`). Aggregate
  *values* additionally admit floats (`agg_value_idx_eligible`,
  `optimize.rs:447-456`).
- Group-visit order (`ops/reduce/sort.rs:26-76, 112-160`; op_reduce.rs:239-245):
  for `group_by_pk` (group cols a permutation of the PK set) it is
  **canonical PK-region order**; otherwise it is `compare_by_group_cols`
  order over the group columns **in list order**. The AVI prefix is gathered
  in `group_by_cols` list order on both the write and read side. The two
  orders coincide except for one shape: `group_by_pk` with *permuted* group
  columns — which the SQL planner never emits (it normalizes
  `group_set_eq_pk` group cols to source-PK order,
  `gnitz-sql/src/plan/view/group_by.rs:393-397`), leaving only low-level
  CircuitBuilder circuits.
- AVI tables are per-view ephemeral operator state (`create_child_table`,
  Persistence::Ephemeral), re-created on every compile and re-derived from
  base data at boot — there is no stored-format migration concern.
- Reduce inputs are bag-positive: the integral of the reduce input never
  holds a negative net weight for any (PK, payload). (The cardinality gate's
  `debug_assert` at `op_reduce.rs:624-636` checks only the weaker net-group
  form, and only when a COUNT exists — the per-element form is a stated
  invariant of reduce inputs, relied on today by the emission gate and by
  this plan's skip rule to exactly the same degree; a violation routes to
  the probe path via the `>=`/`<=` tie test.)

## Part A — dynamic probe-skip

### Semantics

Per group, per value-indexed aggregate `k` (MAX shown; MIN mirrors with the
opposite comparisons — all comparisons are on the shared MIN-oriented
`encode_ordered(.., false)` u64 image, where larger image == larger value):

Let

- `old` = the group's stored aggregate value from the trace_out row
  (`None` when `!has_old` or the agg column's null bit is set),
- `pos` = max encoded value over the group's delta rows with weight > 0 and a
  non-NULL aggregate value (`None` if no such row),
- `neg` = max encoded value over the group's delta rows with weight < 0 and a
  non-NULL aggregate value (`None` if none).

Then:

- **Probe the AVI** iff `neg` is `Some` and (`old` is `None` or
  `neg >= old`). (For MIN: `neg <= old`.)
- Otherwise **skip**: the new extreme is `max(old, pos)` over the present
  values; if both are `None`, leave the accumulator untouched (renders NULL —
  identical to today's empty-seek `acc.reset()` path).

Correctness:

- Skip with `old = Some`: every retracted value is strictly below the old
  maximum, and (bag-positivity) a retraction can only remove rows that exist
  in `I(input)`, all of whose values are ≤ old max. Removals strictly below
  the max cannot change it; insertions can only raise it. Hence
  `max(I + δ) = max(old, pos)` exactly. The `>=` tie test (not `==`) is the
  conservative form: any retraction at-or-above the stored extreme routes to
  the probe, so a bag-positivity violation (which the existing debug_assert
  culture treats as a caller bug) degrades to the probe path rather than a
  silently wrong skip.
- Skip with `old = None` and `neg = None`: the group is new (or previously
  all-NULL); its post-delta extreme over positive entries is exactly `pos`,
  and `pos = None` reproduces today's NULL render.
- Probe cases are unchanged behavior: `apply_agg_from_value_index` reads the
  post-delta extreme from the index, which `op_integrate_with_indexes`
  populated *before* the reduce ran (the AVI `Integrate` is emitted before
  the `Reduce` instruction — `query/compiler/emit.rs:805-813`).
- Retraction at the extreme with remaining multiplicity: probe runs, index
  returns the same extreme. Retraction of the unique extreme: probe runs,
  index returns the next value. MIN and MAX over the same column decide
  independently (separate `pos`/`neg` trackers, separate ordinals).

**Float NaN special case.** For a float-source aggregate whose `old` decoded
value is NaN, always probe. Reason: `old` is stored as F64 bits; an F32
source's old value must be re-encoded on the F32 scale (`old_f64 as f32`) to
be comparable with delta encodings, and Rust float casts do not guarantee NaN
payload preservation — the tie test could miss a retraction of the stored
NaN-extreme. Non-NaN f32↔f64 round-trips are exact, so only NaN needs the
escape hatch. (For F64 sources the re-encode is the identity, but the NaN
rule is applied uniformly for simplicity.)

### Implementation in `op_reduce`

State, allocated once before the group loop:

```rust
// Per value-indexed aggregate (in the same uses_value_index() descriptor
// order as the AVI ordinals): MIN-oriented encoded extremes of this group's
// positive- and negative-weight delta rows.
#[derive(Clone, Copy, Default)]
struct DeltaExtremes { pos: Option<u64>, neg: Option<u64> }
// None when the reduce has no AVI; Some(vec) sized to the value-indexed
// descriptor count otherwise. vi_locs is the hoisted ColumnLocator per
// value-indexed aggregate, in the same order.
let mut dx: Option<Vec<DeltaExtremes>> =
    avi.is_some().then(|| vec![DeltaExtremes::default(); num_value_indexed]);
```

reset at the top of each group iteration:

```rust
if let Some(dx) = dx.as_mut() {
    dx.fill(DeltaExtremes::default());
}
```

Inside the existing per-group delta walk (`op_reduce.rs:467-486`, the loop
that steps linear accumulators), add the non-linear tracking when
`avi.is_some()` — mirroring `Accumulator::step_from_batch`'s NULL gate and
encoding:

```rust
let w = mb.get_weight(curr_idx);
for acc in accs.iter_mut() {
    if acc.is_linear() {
        acc.step_from_batch(&mb, curr_idx, w);
    }
}
if let Some(dx) = dx.as_mut() {
    for (j, (_k, d)) in agg_descs.iter().enumerate()
        .filter(|(_, d)| d.agg_op.uses_value_index()).enumerate()
    {
        let loc = &vi_locs[j]; // ColumnLocator per value-indexed agg, hoisted
        if loc.is_null(&mb, curr_idx) { continue; }
        let enc = encode_ordered(loc.bytes(&mb, curr_idx), d.col_type_code as u8, false);
        let slot = &mut dx[j];
        // w == 0 (a lying Consolidated flag) routes to `neg` — harmlessly
        // conservative: it can only force a probe, never a wrong skip.
        let tgt = if w > 0 { &mut slot.pos } else { &mut slot.neg };
        let better = match (d.agg_op, *tgt) {
            (_, None) => true,
            (AggOp::Max, Some(cur)) => enc > cur,
            (AggOp::Min, Some(cur)) => enc < cur,
            _ => unreachable!(),
        };
        if better { *tgt = Some(enc); }
    }
}
```

(Consolidation guarantee: the non-linear path already works on the
consolidated batch — `Batch::consolidate_if_needed` at `op_reduce.rs:147-152`
— so a same-epoch insert+retract of one row nets to a single signed row and
the `w > 0` split is well-defined.)

Replace the unconditional probe block (`op_reduce.rs:552-560`) with, per
value-indexed aggregate:

```rust
// Old extreme off the still-positioned trace_out cursor (same access pattern
// as fold_old_aggs, which deliberately skips these accumulators).
let old: Option<OldExtreme> = if has_old {
    read_old_minmax_encoded(trace_out_cursor, cbase + k, pbase + k,
                            agg_col_widths[k], d.col_type_code)
} else {
    None
};
// A NaN old extreme always probes — BEFORE any skip arm: the F32 re-encode
// cannot guarantee NaN payload round-trip, so neither the tie test nor the
// skip-path reseed is byte-trustworthy for it.
let probe = old.is_some_and(|o| o.is_nan)
    || match (dx[j].neg, old) {
        (None, _) => false,
        // Defensively conservative: unreachable for planner reduces (a
        // retraction with no stored row implies zero net cardinality under
        // bag positivity), but a probe is always correct.
        (Some(_), None) => true,
        (Some(neg), Some(o)) => {
            (d.agg_op == AggOp::Max && neg >= o.enc)
                || (d.agg_op == AggOp::Min && neg <= o.enc)
        }
    };
if probe {
    gk[gstride] = j as u8;
    apply_agg_from_value_index(avi_c, &gk[..gstride + 1], d.agg_op == AggOp::Max, &mut accs[k]);
} else {
    let winner = match (old.map(|o| o.enc), dx[j].pos) {
        (None, None) => None,
        (Some(o), None) => Some(o),
        (None, Some(p)) => Some(p),
        (Some(o), Some(p)) => Some(if d.agg_op == AggOp::Max { o.max(p) } else { o.min(p) }),
    };
    match winner {
        Some(enc) => accs[k].seed_from_raw_bits(enc), // sets has_value
        None => accs[k].reset(),                      // renders NULL
    }
}
```

`read_old_minmax_encoded` is a new helper in `agg.rs` beside
`readback_agg_bits`:

```rust
/// The stored MIN/MAX value in the trace_out row's agg column, as its
/// MIN-oriented encoding plus a NaN flag, or None when the null bit is set.
/// Float sources are stored as F64 bits at width 8; an F32 source re-encodes
/// on the F32 scale (exact for non-NaN; the caller routes NaN to the probe
/// path via `is_nan`).
#[derive(Clone, Copy)]
pub(super) struct OldExtreme { pub enc: u64, pub is_nan: bool }

pub(super) fn read_old_minmax_encoded(
    cursor: &ReadCursor, c_idx: usize, pi: usize, cw: usize, src_tc: TypeCode,
) -> Option<OldExtreme> { /* null-bit check (mirrors fold_old_aggs' pbase
    indexing, agg.rs:404), col_ptr read at cw, encode_ordered / F32 recast,
    is_nan for float sources */ }
```

The `gk` group-key gather and `avi_extractor` remain as today (they are also
needed by Part B); the only behavioral change is that the probe becomes
conditional and the skip path seeds from `old`/`pos`.

The empty-delta global-ground seed path (`op_reduce.rs:156-201`) and the
non-AVI fallback (`trace_in` replay) are untouched.

## Part B — canonical AVI group prefix + galloping probes

### Encoding change

`GroupKeyExtractor` (`ops/util.rs:126-166`) currently copies each group
column's raw bytes (LE for payload columns, OPK for PK columns). Change
`gather` to emit **canonical order-preserving bytes at each column's width**:

- PK group column: verbatim OPK window (unchanged — already canonical).
- Payload unsigned int (U8..U64, U128, UUID): big-endian bytes of the value.
- Payload signed int (I8..I64, I128): big-endian bytes with the sign bit
  flipped — the same image the OPK encoder and `PromoteKind::Narrow`
  (`ops/reindex.rs:116-119, 146-150`) produce for the same value at the same
  width.

Floats/strings/nullables cannot appear (`avi_group_key_eligible` excludes
them; the constructor's non-nullable assert stays). Implementation: extend
each precomputed `ColumnLocator` entry with a 3-way kind
(`PkWindow | UPayload | IPayload`) resolved in `new`, and encode in `gather`
(~20 lines; byte-reverse + sign-flip at width, no allocation).

Because population (`op_integrate_with_indexes`) and probing (`op_reduce`)
share this one extractor, the write and read images move together; the AVI is
private to the operator (ephemeral, rebuilt at boot/recompile), so nothing
else reads its key bytes positionally except the trailing `av` slice in
`apply_agg_from_value_index`, whose offset (`group_key.len()`) is unchanged.

### Why probes are monotone (and where they are not)

Canonical per-column images are order-preserving for every admissible type
(unsigned BE = unsigned order; sign-flip BE = signed order; PK window = OPK
order), and columns are fixed-width, so the concatenated prefix is ordered
exactly like a column-by-column typed comparison **in gather (list) order**.
That equals the group-visit order whenever the visit order is
list-order-typed — every payload/mixed GROUP BY, and `group_by_pk` with
group cols in PK order (all SQL-planned reduces). The one exception is
`group_by_pk` with a *permuted* low-level group-col list (visit order is
canonical PK order, prefixes are permuted). Define once in `op_reduce`:

```rust
// Probe keys ascend in group-visit order iff the gather order equals the
// visit order: always for the non-pk paths (argsort by the same list), and
// for group_by_pk only when the list is in PK order (the SQL planner
// normalizes it; a permuted low-level list visits in canonical PK order
// while gathering in list order).
let monotone_avi_probe = !group_by_pk
    || group_by_cols.iter().map(|&c| c as usize).eq(input_schema.pk_indices().iter().copied());
```

`apply_agg_from_value_index` gains the galloping/absolute choice as a
parameter driven by this flag: galloping
(`advance_to_first_positive_with_prefix`) when set, today's absolute
`seek_first_positive_with_prefix` otherwise. Within a group the ordinal byte
ascends (`j = 0, 1, …`), and skipping probes (Part A) only removes elements
from the ascending sequence.

### Cursor change

New method beside `seek_first_positive_with_prefix`
(`read_cursor/mod.rs:475-485`):

```rust
/// `seek_first_positive_with_prefix`, but galloping from the live position:
/// zero-pads `prefix` to the stride (0x00 is the OPK minimum for every PK
/// type), `advance_to`s the padded key, then walks to the first positive
/// entry under the prefix. Lands on the identical row the absolute form
/// would; a backward target degrades to the bounded per-source rescan
/// inside `advance_to`, so callers need monotonicity only for speed.
pub fn advance_to_first_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
    let stride = self.schema.pk_stride() as usize;
    let mut key = [0u8; crate::schema::MAX_PK_BYTES];
    let copy_len = prefix.len().min(stride);
    key[..copy_len].copy_from_slice(&prefix[..copy_len]);
    self.advance_to(&key[..stride]);
    self.walk_to_positive_with_prefix(prefix)
}
```

The AVI cursor is created fresh per Reduce instruction
(`query/vm/exec.rs:433-439`), so under the flag each epoch is one forward
pass over the index. A backward target inside `advance_to` degrades to the
bounded per-source rescan (correct, unaccelerated), so the flag guards speed
only. Note the zero-allocation `seek_forward_multi` fast path fires only on
`SourceMode::Multi` (≥3 sources); a freshly compacted AVI cursor is often
Single/Pair, where the win is the hint-seeded per-source gallop replacing
the full-height `find_lower_bound_bytes` per probe — smaller, still real.

## Sequencing

1. **Part B encoding** — `GroupKeyExtractor` canonical `gather` + updated AVI
   layout tests. (Self-contained: exact-prefix seeks are order-agnostic, so
   this lands green on its own.)
2. **Part B galloping** — `advance_to_first_positive_with_prefix`, the
   `monotone_avi_probe` flag, and the `apply_agg_from_value_index` seek
   selection. Add a `cfg(debug_assertions)`-gated tripwire in `op_reduce`,
   active only under `monotone_avi_probe`: keep the previous probe key in a
   debug-only `Vec<u8>` and assert `prev < current` before each probe
   (probes are distinct — group prefixes differ across groups and the
   ordinal byte ascends within one — so strict `<` is the correct pin; an
   equality is itself an encoding/order divergence).
3. **Part A probe-skip** — trackers, `read_old_minmax_encoded`, conditional
   probe.

## Files touched

| File | Change |
|---|---|
| `crates/gnitz-engine/src/ops/util.rs` | canonical `GroupKeyExtractor::gather` (per-column kind + sign-flip/BE encode) |
| `crates/gnitz-engine/src/ops/index.rs` | none functionally (population flows through the extractor); update module docs describing the key layout |
| `crates/gnitz-engine/src/storage/lsm/read_cursor/mod.rs` | `advance_to_first_positive_with_prefix` |
| `crates/gnitz-engine/src/ops/reduce/agg.rs` | `read_old_minmax_encoded`; `apply_agg_from_value_index` uses the galloping seek |
| `crates/gnitz-engine/src/ops/reduce/op_reduce.rs` | `DeltaExtremes` tracking in the group walk; conditional probe/skip; debug tripwires |
| `crates/gnitz-engine/src/ops/reduce/tests.rs`, `ops/index.rs` tests | updated layout pins + new behavior tests |

## Testing

**Layout pins to update** (they assert the raw LE prefix today and must
assert the canonical image instead): `avi_two_groups_distinct_byte_form_keys`
(`reduce/tests.rs:3451+`, asserts `key[0..4] = a.to_le_bytes()` → becomes the
canonical BE image), `avi_retraction_returns_next_extremum` (3569+),
`avi_non_power_of_two_stride_drives_cursor` (3693+),
`avi_wide_two_u64_groups_match_reference` (4209+),
`avi_wide_single_u128_group_distinct` (4358+),
`avi_wide_mixed_signed_unsigned_key` (4466+),
`avi_wide_prefix_collision_distinct_groups` (4586+),
`avi_wide_retraction_returns_next_extremum` (4693+),
`avi_multi_col_retraction_returns_next_extremum` (4081+, hand-built LE
prefixes `key[0..4]=a.to_le_bytes()` / `key[4..8]=bb.to_le_bytes()`),
`global_lone_min_avi_empty_prefix` (6396+; empty prefix — unchanged bytes).
`avi_full_path_min_max_across_high_byte` (4872+) builds both sides through
`GroupKeyExtractor`/`op_integrate_with_indexes` and needs no byte update;
the `ops/index.rs` `avi_encode_tests` pin the value codec only — unchanged.

**New behavior tests** (`reduce/tests.rs`):

- **Randomized equivalence, mixed churn**: extend the
  `avi_wide_two_u64_groups_match_reference` pattern to a multi-epoch stream
  of inserts, updates (retract+insert), and deletes over many groups,
  asserting the AVI path equals the trace-scan reference after every epoch.
  This is the primary skip-logic oracle (skip vs probe divergence shows up as
  a wrong extreme).
- **Skip-path unit cases** (single group, explicit epochs): (a) insert-only
  raising MAX above old; (b) insert-only below old (extreme = old); (c)
  retraction strictly inside the range (skip; extreme unchanged); (d)
  retraction of the extreme with multiplicity 2 at that value (probe; extreme
  unchanged); (e) retraction of the unique extreme (probe; recede to next);
  (f) new group all-NULL values (NULL render); (g) group emptied to zero
  cardinality (cardinality gate sheds; probe/skip irrelevant); (h) MIN and
  MAX on the same column where one probes and the other skips in the same
  epoch.
- **Float cases**: F32 and F64 MIN/MAX with (i) non-NaN old + NaN retraction,
  (ii) NaN old extreme retracted (must probe — the NaN rule), (iii)
  ±0.0 ties (total-order encoding). Assert against the trace-scan reference.
- **Signed compound group key ordering**: groups over `(I64, I32)` with
  negative values across several epochs — exercises the canonical prefix +
  galloping walk across sign-flip boundaries; assert values and (debug) the
  ascending-probe tripwire holds.
- **Global aggregate** (`SELECT MIN(x)/MAX(x)`) insert-only and
  retract-at-extreme epochs (V₀ group, empty prefix).
- **Permuted-PK low-level circuit** (`GROUP BY b, a` over PK `(a, b)` via
  the CircuitBuilder, as in `test_reduce_group_by_pk_permuted_preserves_pk_order`,
  reduce/tests.rs:2141): `monotone_avi_probe` is false, probes stay on the
  absolute seek, results match the reference, and the tripwire does not
  fire.

All aggregate-source columns in these tests are payload columns; PK-column
aggregate sources are deliberately excluded from AVI-path tests (their AVI
image is corrupted by a pre-existing write-side encode bug with its own
plan; the non-AVI PK-source tests `test_reduce_min_pk_col_*` are unaffected
and stay as-is).

E2E: existing `make e2e` MIN/MAX suites must pass at `GNITZ_WORKERS=4`
(exchange-sharded grouped MIN/MAX, `v_ext`-shaped views).

Perf validation: interleaved A/B/A/B `make bench-full` on
`test_view_maintenance`, plus `perf report` confirming the
`seek_first_positive_with_prefix`/`find_lower_bound_bytes` cluster shrinks;
the suite's ±3-4% noise swallows smaller deltas, so judge by the symbol
profile, not only end-to-end rows/s.

## Invariants preserved

- The AVI always reflects `I(input)` per (group, ordinal) — population is
  untouched; only reads are skipped when provably answer-preserving.
- Bag-positivity of reduce inputs is *relied on exactly as much as today*:
  the `>=`/`<=` tie test routes violations to the probe path, which computes
  from the index like the current code.
- One extractor defines the AVI key bytes for both writer and reader; the
  `av` slice offset and MIN-orientation contract of
  `seed_from_raw_bits`/`get_value_bits` are unchanged.
- MIN/MAX emit width and NULL rendering (`emit_agg_col`) are unchanged; the
  skip path produces the same accumulator states the probe path would.
