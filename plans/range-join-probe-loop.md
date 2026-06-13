# Range-join probe-loop performance

## Goal

The range / band join probe (`op_join_delta_trace_range`, `ops/join.rs:403`) is
**delta-driven only** and **re-seeks the trace from scratch for every delta row**.
For a consolidated delta of `m` rows over a trace of `r` rows the seek cost is
`O(m · log r)`. Consecutive rows' `[start, end)` intervals also overlap, so the
cursor *re-advances* over the shared region once per delta row — and this is
**symmetric across all four rels**, not specific to one direction: `Lt`/`Le` (and
any pure-range `n_eq == 0`) share a **low prefix** (every row's interval starts at
the group head `eq ‖ 0`), while `Gt`/`Ge` share a **high suffix** (every row's
interval ends at the next group's first key). The per-row walk therefore performs
`O(output)` cursor advances — each a merge heap-sift — where `output = Σ_i
|matches_i|` is the emitted pair count. Those advances are the cost paid twice, and
they are **not** redundant output: every walked trace row inside `[start, end)` is a
genuine match, so the emitted pair count is intrinsic and identical under any
strategy. What a merge eliminates is the *revisiting* — each trace row is touched
once (`O(r)` advances) instead of once per delta row whose interval covers it
(`O(output)` advances). The equi-join already avoids the
re-seek: it swaps driver/probe by size (`op_join_delta_trace:249`, `join_dt_swapped`)
and seeks the sorted run once. This plan brings the range op to parity by exploiting
two facts the operator currently ignores:

- The consolidated delta is **sorted by its full reindex PK `[eq…, range]`**, so
  the per-row probe `start` is **globally monotone non-decreasing**. A from-scratch
  binary search per row is wasted work — a hint-seeded forward seek bounds each
  probe to the gap actually advanced.
- Within one equality group both sides are sorted by the range slot, so the whole
  group is a **single merge** (one forward walk of the trace + one monotone delta
  pointer) rather than `m_group` independent seek-and-walks. This collapses the
  `m_group` per-row seeks (`O(m_group · log r)`) into a single forward trace scan —
  **no seek at all** — and the `O(output)` repeated trace advances into one
  `O(r_group)` walk; the emission (write) count is unchanged, so the win is the
  seek + advance overhead, largest when the delta is dense relative to the trace.
  Mirrors the equi-join's trace-driven swap.

No wire-format, planner, or distribution change: this is entirely inside
`op_join_delta_trace_range` plus a few private helpers in the same module. It is
independent of the
distribution work (`wide-pk-incremental-views.md` §1,
`chunked-distributed-backfill.md`) — it touches no SAL, relay, master, or backfill
path.

```sql
-- band join: delta sorted by [k, t]; per-row start monotone within & across k-groups
CREATE VIEW v AS SELECT a.id, b.id, b.t FROM a JOIN b ON a.k = b.k AND a.lo <= b.t;
-- pure range: one eq group (n_eq = 0); every Lt/Le row re-walks from trace start today
CREATE VIEW w AS SELECT a.id, b.id FROM a JOIN b ON a.x < b.y;
```

## Background — current probe loop (verified against code)

`op_join_delta_trace_range` (`ops/join.rs:403`) consolidates the delta
(`consolidate_if_needed`, sorted + duplicate-PK-merged) and, for each row `i`:

```rust
let pk_i = consolidated.get_pk_bytes(i);          // sorted ascending by [eq‖d]
let (eq, d) = pk_i.split_at(eq_size);
let Some((start, end)) = range_cut_points(eq, d, rel) else { continue };
cursor.seek_bytes(start.as_slice());              // from-scratch O(log r) EVERY row
while cursor.valid && end.is_none_or(|e| cursor.current_pk_cmp_bytes(e).is_lt()) {
    let w_out = w_delta.wrapping_mul(cursor.current_weight);
    if w_out != 0 { write_join_row(&mut output, &delta_mb, i, cursor, w_out, …); }
    cursor.advance();
}
```

`range_cut_points` (`storage/range_key.rs:67`) maps `(eq, d, rel)` to the half-open
`[start, end)` over the trace PK space:

| `rel` | `start`            | `end`                              |
|-------|--------------------|------------------------------------|
| `Gt`  | `succ(eq‖d)`; carry⇒∅ | `succ(eq)` zero-padded; none⇒table end |
| `Ge`  | `eq‖d`             | same as `Gt`                       |
| `Lt`  | `eq ‖ 0x00*slot`   | `Some(eq‖d)`                       |
| `Le`  | `eq ‖ 0x00*slot`   | `succ(eq‖d)`; carry⇒table end       |

Two structural facts fall out, neither exploited today:

1. **`start` is globally monotone non-decreasing across `i`.** `Gt`/`Ge` start at
   `succ(p_i)`/`p_i`; `Lt`/`Le` start at `eq_i ‖ 0`. `p_i` is the `i`-th sorted PK
   and `eq_i` is its (sorted) prefix, so all four `start` sequences are
   non-decreasing. The from-scratch `cursor.seek_bytes` (`read_cursor.rs:518` →
   per-source `find_lower_bound_bytes`, a `[0, count)` binary search) ignores the
   live position entirely.
2. **One eq group is one merge.** Within rows sharing an eq prefix `E`, both the
   delta (by `d`) and the trace eq group (by range slot) are sorted, and the match
   is a single inequality on the range slot. `Lt`/`Le` make this glaring: every row
   in the group has `start = E ‖ 0` (the group's first key) and `end = p_i` growing
   with `d_i`, so the current loop re-seeks `E‖0` and re-advances the shared low
   prefix `m_group` times. `Gt`/`Ge` are the mirror image: every row has `end =
   succ(E) ‖ 0` (the next group's head) and `start = p_i` (or its successor) rising
   with `d_i`, so the loop re-advances the shared **high suffix** `m_group` times.
   Each re-advance does emit (the shared rows are genuine matches for the covering
   delta rows), so this is repeated cursor cost, not duplicate output — symmetric
   across all four rels. For `n_eq == 0` (one group = whole trace) a `Lt` delta of
   `m` rows re-advances from trace position 0 `m` times; a `Gt` delta re-advances to
   the table end `m` times.

The trace is the merge arrangement built by `integrate_trace` over
`[eq…, range]` (`planner.rs:1432-1435`); `join_with_trace_range_node(reindex, trace,
n_eq, rel)` supplies `n_eq` and the per-term relation (`rel_ab = converse(op)`,
`rel_ba = op`; `planner.rs:1398-1399`). The delta is a random-access `MemBatch`
(`consolidated.as_mem_batch()`); the trace is the sequential `ReadCursor`. INNER
only; the output is marked `sorted = false` and re-keyed + exchanged + consolidated
downstream, so the probe is free to emit in any order.

The equi-join's adaptive swap is the template: `op_join_delta_trace`
(`ops/join.rs:226`) does `cursor.rewind()` (`read_cursor.rs:504`), reads
`cursor.estimated_length()` (`read_cursor.rs:831`, `Σ count − position`), and if
`n > trace_len` drives from the trace (`join_dt_swapped`) instead of the delta.
The range op never rewinds, never estimates, never swaps.

## Design

Two execution strategies selected by the same size heuristic the equi-join uses.

### Strategy selection (size-adaptive, mirrors the equi-join)

```rust
let cs = Batch::consolidate_if_needed(delta, left_schema);
let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
let n = consolidated.count;
if n == 0 { return Batch::empty(out_npc, left_schema.pk_stride()); }

cursor.rewind();                          // estimated_length sees the full trace;
let trace_len = cursor.estimated_length();// merge walk starts at the trace head
if n > trace_len {
    range_merge_walk(consolidated, cursor, left_schema, right_schema, n_eq, rel)
} else {
    range_per_row_seek(consolidated, cursor, left_schema, right_schema, n_eq, rel)
}
```

`rewind` is required so `estimated_length` (`Σ count − position`) sees the full
trace for the heuristic and so the merge walk starts at the trace head. The range
circuit is INNER-only with two independent terms — `join_ab` over `trace_b`,
`join_ba` over `trace_a` — and, unlike the LEFT-join equi path (`op_join_delta_trace:246`,
where `join_ba` and `correction_raw` share `trace_a`), neither range trace is reused
by a second op within a tick, so each cursor enters at position 0 already; the
rewind is the same defensive reset the equi op uses, not a correctness fix for
in-tick sharing. The delta-driven path seeks per row, so the rewind is harmless there.

### Strategy 1 — delta-driven, monotone forward seek (default, `|Δ| ≤ |trace|`)

Structurally the current loop, with the only change being the seek call:

```rust
-   cursor.seek_bytes(start.as_slice());
+   cursor.seek_bytes_forward(start.as_slice());   // hint = live position
```

`seek_bytes_forward` (the galloping, hint-seeded lower bound from
`galloping-forward-seek.md`) seeds each source's lower-bound search at its current
position and is **correct for any hint** — an overshooting hint falls back to a
bounded `[0, hint]` search. Because `start` is monotone:

- **At an eq-group boundary** the next `start` lies at or after the previous walk's
  end (`= succ(eq)` for `Gt`/`Ge`, `= next_E ‖ 0` for `Lt`/`Le`), so the hint is at
  or before it ⇒ the forward gallop probes `O(log gap)`, not `O(log r)`.
- **Within an eq group**, `Gt`/`Ge` walk to `next_group(E)` then the next (smaller)
  `start` is a backward seek; the overshoot fallback searches `[0, hint]` with
  `hint =` the post-walk position `≤ r`. That is **never worse** than today's
  `[0, count)` and usually tighter. `Lt`/`Le` within a group re-seek the constant
  `E ‖ 0` (backward to the group head) — also bounded. But galloping only bounds the
  **seek**; for every rel the next intra-group `start` is backward, so the shared
  region is still **re-advanced** once per delta row. Strategy 2 is the real fix for
  the dense within-group case (any rel), eliminating the re-advance, not just the
  seek.

This is the cheapest win and a one-call change; it is gated only by
`galloping-forward-seek.md` landing the `ReadCursor::seek_bytes_forward` primitive.
It never changes *which* rows match (galloping returns the same lower bound), only
the seek cost — so it is safe under any delta/trace shape.

### Strategy 2 — trace-driven eq-group merge walk (`|Δ| > |trace|`)

When the delta outnumbers the trace, sweep the **trace forward exactly once** — no
per-group seek — keeping a monotone pointer into the current delta eq group and
emitting the matching contiguous delta sub-range per trace row. Both sides are
globally sorted by `[eq…, range]`, so eq groups line up by a forward scan: the
cursor (rewound to the head by the selector) advances past any trace group below
the current delta group, then walks the matching group. This is structurally the
equi-join's `join_dt_swapped` (drive the smaller trace, never seek), but with a
monotone delta pointer replacing its per-trace-row binary search — `O(r + m +
output)`, not `O(r · log m)`, and **no binary search of any kind**.

```rust
fn range_merge_walk(consolidated, cursor, left_schema, right_schema, n_eq, rel) -> Batch {
    let stride  = right_schema.pk_stride() as usize;
    let eq_size = right_schema.leading_key_size(n_eq);
    let delta_mb = consolidated.as_mem_batch();          // MemBatch::get_pk_bytes returns &'a,
    let mut output = Batch::empty_joined(left_schema, right_schema);  // freely held across the walk
    let m = consolidated.count;

    // The selector rewound the cursor to the trace head. One forward sweep of the
    // (smaller) trace: each row is touched once, by either the group-skip or the
    // group-walk. No seek.
    let mut lo = 0;                                   // start of the current delta eq group
    while lo < m && cursor.valid {
        // Delta eq group [lo, hi): contiguous rows sharing the eq prefix E.
        let e = &delta_mb.get_pk_bytes(lo)[..eq_size]; // E
        let mut hi = lo + 1;
        while hi < m && &delta_mb.get_pk_bytes(hi)[..eq_size] == e { hi += 1; }

        // Forward-advance over any trace eq group below E (none when n_eq == 0).
        // Cursor moves forward only and delta groups ascend, so this never
        // re-scans — skip + walk over all groups totals O(r).
        while cursor.valid && &cursor.current_pk_bytes()[..eq_size] < e { cursor.advance(); }

        let mut ptr = lo;                             // monotone delta pointer
        while cursor.valid && &cursor.current_pk_bytes()[..eq_size] == e {
            let s = &cursor.current_pk_bytes()[eq_size..stride];   // trace range slot
            let w_t = cursor.current_weight;
            // Advance `ptr` and emit the matching contiguous delta sub-range.
            let (rs, re) = match rel {
                // prefix [lo, ub): grows as s↑
                Gt => { advance_ub(&delta_mb, eq_size, stride, &mut ptr, hi, |d| d <  s); (lo, ptr) }
                Ge => { advance_ub(&delta_mb, eq_size, stride, &mut ptr, hi, |d| d <= s); (lo, ptr) }
                // suffix [lb, hi): shrinks as s↑
                Lt => { advance_lb(&delta_mb, eq_size, stride, &mut ptr, hi, |d| d <= s); (ptr, hi) }
                Le => { advance_lb(&delta_mb, eq_size, stride, &mut ptr, hi, |d| d <  s); (ptr, hi) }
            };
            for k in rs..re {
                let w_out = delta_mb.get_weight(k).wrapping_mul(w_t);
                if w_out != 0 {
                    write_join_row(&mut output, &delta_mb, k, cursor, w_out, left_schema, right_schema);
                }
            }
            cursor.advance();
        }
        lo = hi;
    }
    output.sorted = false;
    output
}
```

`advance_ub` advances `ptr` (the prefix upper bound) forward while the predicate on
`delta[ptr]`'s range slot holds; `advance_lb` advances the suffix lower bound the
same way. Both compare the delta row's range slot
(`delta_mb.get_pk_bytes(ptr)[eq_size..stride]`) against `s` by `memcmp` (OPK order =
typed order). For `Gt`/`Ge` the prefix `[lo, ptr)` **grows** with `s`; for
`Lt`/`Le` the suffix `[ptr, hi)` **shrinks** (its lower bound rises) — both `ptr`
moves are monotone, so the whole walk costs `O(r)` trace steps (skip + group) +
`O(m)` pointer advance + `O(output)` emission, with **no seek**. The per-row path
over the same inputs costs `O(m · log r)` seeks **plus** `O(output)` cursor advances
(it revisits every matched trace row once per covering delta row — §Goal); the merge
replaces both with `O(r)` advances + `O(m)` pointer moves, emitting the identical
`O(output)` pairs. Two consequences for the `n > trace_len` size heuristic:

- **It is safe.** When the heuristic selects the merge, `m > r`, so `O(r + m +
  output)` collapses to `O(m + output) ≤` the per-row `O(m · log r + output)` — the
  merge is never asymptotically worse than the path it replaces.
- **It under-selects.** Range fan-out is **unbounded**: one delta row can match up to
  `r` trace rows, unlike the equi-join where a delta row matches only its equal-key
  group (so equi `output ≈ m` and `m > trace_len` is the right line). For `Lt`/`Le`
  over large intervals and the symmetric `Gt`/`Ge` over small `d`, `output` is
  `Θ(m · r)`, so the merge's advance saving (`O(output) → O(r)`) makes it win
  whenever `output > r` — far below `m = trace_len`. Strategy 1's galloping shrinks
  only the **seek** term (`O(m · log r) → O(m · log gap)`); it does **not** touch the
  `O(output)` re-advance, so across the whole `n ≤ trace_len` regime the per-row path
  still pays the full revisit cost and the two strategies do **not** meet smoothly at
  the boundary for high-fan-out rels. Closing that gap needs a fan-out-aware or
  per-eq-group choice (§Scope boundaries), not the seek refinement. The global
  heuristic is retained for parity with the equi-join and because it is only ever
  sub-optimal here, never wrong.

Two implementation notes verified against the APIs. (1) `delta_mb` (`MemBatch`,
`storage/merge.rs:200,206`) returns `get_pk_bytes` as `&'a [u8]` (tied to the batch
data, not a `&self` borrow) and exposes `get_weight`, so the merge reads every
delta-row value (group extents, pointer slot, emission weight) from `delta_mb`
alone — `e` is held freely across the walk and `consolidated` is not threaded in
(only `consolidated.count` = `m` is read up front). (2) Unlike `join_dt_swapped`
(which stack-copies the trace PK because it holds it across the inner delta loop
*and* the `advance`), the merge consumes `s` entirely in the pointer advance
**before** any emission, so no stack copy of `s` is needed: the `&` into the cursor
dies before the `for k in rs..re` writes and the trailing `cursor.advance()`.

**Why a single forward pointer suffices** (trace ascending ⇒ `s` non-decreasing):

| `rel` | match set per trace slot `s`        | pointer        | advance while       |
|-------|-------------------------------------|----------------|---------------------|
| `Gt`  | `{i : d_i < s}` = prefix `[lo, ub)` | `ub` (upper)   | `d_ub < s`          |
| `Ge`  | `{i : d_i ≤ s}` = prefix `[lo, ub)` | `ub` (upper)   | `d_ub ≤ s`          |
| `Lt`  | `{i : d_i > s}` = suffix `[lb, hi)` | `lb` (lower)   | `d_lb ≤ s`          |
| `Le`  | `{i : d_i ≥ s}` = suffix `[lb, hi)` | `lb` (lower)   | `d_lb < s`          |

As `s` increases the upper bound `ub` rises (prefix grows) and the lower bound `lb`
rises (suffix shrinks); neither ever moves backward, so starting each at the group
head `lo` and only advancing is exact and amortized `O(m_group)`. The match set
matches `range_cut_points` exactly (Strategy 1's `[start, end)` over the same eq
group), which the differential test below pins.

`n_eq == 0` is the degenerate single-group case: `eq_size == 0`, `e` is empty, the
group-skip `prefix < []` is always false, and the whole trace is one group walked
once — the maximal win for pure-range `Lt`/`Le`, which today re-advance from trace
position 0 for every delta row.

## The change, by file

### `crates/gnitz-engine/src/ops/join.rs` — split `op_join_delta_trace_range`

`op_join_delta_trace_range` (`:403`) becomes the consolidate + `rewind` +
`estimated_length` selector above, delegating to:

- `range_per_row_seek` — the current `:435-458` loop verbatim, with
  `cursor.seek_bytes` → `cursor.seek_bytes_forward` (Strategy 1). Keeps
  `range_cut_points`, the `[start, end)` walk, and `write_join_row`.
- `range_merge_walk` — Strategy 2, plus the two private `advance_ub` / `advance_lb`
  pointer helpers (or one helper taking a `Bound` + comparator). Reuses
  `write_join_row`, `Batch::empty_joined`, `leading_key_size`, `current_pk_bytes`,
  `current_weight`, `advance` — **no seek and no new storage primitive**; it is a
  pure forward cursor sweep.

The existing `debug_assert`s (`:426-429`: strides match, trace PK arity `= n_eq+1`)
move into the shared selector so both strategies keep them. The module already
imports `RangeRel`, `range_cut_points`, `ReadCursor`, `MemBatch`, `write_to_batch`.

The operator's doc comment (`:402`, currently *"Stays delta-driven (no trace-driven
swap variant in slice 1)"*) is rewritten to describe the size-adaptive split. That
comment is the **only** stale internal-strategy statement in the tree (verified:
`grep "delta-driven\|slice 1"` hits it alone) and the **only** change outside
`op_join_delta_trace_range`. Nothing else references the operator's internal
strategy: the `vm.rs:1002` `JoinDTRange` dispatch, the `circuit.rs:392`
`join_with_trace_range_node` builder, and the `master.rs`/`dag.rs` range-relay
routing (`view_range_join_n_eq`, the eq-prefix scatter / broadcast `PartitionFilter`
decision) all key off the **unchanged** node opcode `Join(DeltaTraceRange { n_eq,
rel })` and the unchanged operator signature, and the output already carries
`sorted = false`, so the input relay → output `ExchangeShard` → `consolidate`
pipeline that re-orders the result is indifferent to delta-major vs trace-major
emission. **Scope confirmed wide enough**: the change is contained to
`op_join_delta_trace_range` plus its private helpers, with Strategy 1 additionally
consuming `seek_bytes_forward` from `galloping-forward-seek.md`.

### `crates/gnitz-engine/src/storage/read_cursor.rs` — (consumed by Strategy 1 only)

`ReadCursor::seek_bytes_forward` is introduced by `galloping-forward-seek.md`; only
Strategy 1's per-row path calls it. Strategy 2 uses no seek at all (forward sweep),
so it is **fully independent** of that plan and carries the bulk of the win on its
own. See [Dependency](#dependency).

No planner, compiler, wire, master, or dag change: the operator's signature,
node opcode (`Join(DeltaTraceRange { n_eq, rel })`), and the two-term circuit
shape are unchanged.

## Correctness invariants to preserve

- **Match-set identity.** For every delta row and eq group the merge emits exactly
  the `(delta, trace)` pairs the `range_cut_points` `[start, end)` walk emits — same
  weights (`w_delta · w_trace`), same zero-weight suppression
  (`if w_out != 0`). The boundary table above is the converse of the cut-point
  table; the differential test pins them equal over random inputs.
- **OPK byte order is the typed order.** Both the delta-pointer comparison
  (`delta` range-slot bytes vs trace slot `s`) and the eq-group membership check
  (`current_pk_bytes()[..eq_size] == e`) are raw `memcmp` on the common-`T`
  reindex PK region, valid at every promoted width (`foundations.md`; both sides
  pack at the pair's common type, `planner.rs:1393-1394`).
- **Uncompacted-trace weights.** A trace entry may carry weight `≤ 0`; the product
  is emitted verbatim (negative output weights allowed) and consolidated downstream
  — identical to the current loop. The merge must **not** filter trace rows by
  weight before emitting (only the `w_out != 0` net-zero skip applies).
- **Duplicate PKs.** The consolidated delta may hold several rows with the same
  `[eq‖d]` but distinct payloads (source PKs ride as payload); they are adjacent,
  share one `d`, and are all included in the pointer range. The trace arrangement
  may hold several rows per `[eq‖d]` (distinct payloads); each is a separate trace
  step and emits the full delta range. Both match the current per-row behavior.
- **Output is unsorted.** Both strategies emit delta-major-with-trace-runs
  (per-row seek) or trace-major-with-delta-runs (merge); set `output.sorted =
  false`, exactly as today (`:463`). The downstream re-key + `ExchangeShard` +
  `consolidate_exchanged` restore order — do not let a merge trust this batch.
- **Empty / single-side.** `n == 0` returns the empty joined batch before any
  seek. An empty trace (`trace_len == 0`) with `n > 0` makes `n > trace_len` true,
  so the **merge** is selected: it walks zero trace rows per group and emits
  nothing — correct (the per-row path would also emit nothing, every seek landing
  on an invalid cursor). Confirm the `n > trace_len` boundary at `trace_len == 0`.
- **Both terms.** `join_ab` and `join_ba` are independent operator calls over
  distinct cursors (`trace_b`, `trace_a`) with relations `converse(op)` / `op`;
  each rewinds its own cursor. The size heuristic is evaluated per term.
- **Galloping is hint-only.** `seek_bytes_forward` returns the same lower bound as
  `seek_bytes` for every input (`galloping-forward-seek.md`), so swapping it into
  the per-row path (Strategy 1) changes cost, never results. Strategy 2 takes no
  seek, so it is unaffected either way.

## Dependency

Only Strategy 1 (`seek_bytes_forward` in the per-row path) depends on
`galloping-forward-seek.md`'s `ReadCursor::seek_bytes_forward`. That plan adds the
primitive for two other monotone sweeps (`resolve_collected_pks_into`,
`gather_family_bytes`); the range-join per-row probe is a **third consumer** (now
cross-referenced there). Strategy 2 takes no seek (a pure forward sweep), so it has
**no dependency** and carries the bulk of the win on its own. Sequencing:

1. Ship Strategy 2 + the selector now — self-contained, regardless of
   `galloping-forward-seek.md`. It removes the per-*row* re-seek entirely whenever
   the delta outnumbers the trace.
2. When `galloping-forward-seek.md` lands, swap `seek_bytes → seek_bytes_forward` in
   `range_per_row_seek` (Strategy 1) — the incremental sparse-delta refinement for
   the `n ≤ trace_len` regime.

## Migration order

1. **Selector + Strategy 2** (`ops/join.rs`): consolidate/rewind/estimate split and
   `range_merge_walk` (a seek-free forward sweep) with the pointer helpers. Add the
   differential and merge-specific unit tests. Self-contained — no external
   dependency, compiles and passes alone.
2. **Strategy 1 forward seek**: once `galloping-forward-seek.md` lands, swap
   `seek_bytes` → `seek_bytes_forward` in `range_per_row_seek`. Add the monotone-seek
   anti-regression test.
3. `make e2e` (`GNITZ_WORKERS=4`) across `TestRangeJoin` — both strategies exercised
   by widening one band test's delta past its trace (forces the merge) and keeping
   another below (per-row).

## Testing

**Unit (`ops/join.rs` tests, alongside the existing range tests at `:1604+`).**

- **Differential merge vs per-row, all four rels, `n_eq ∈ {0, 1, 2}`.** Random
  small delta + trace; assert `range_merge_walk` and `range_per_row_seek` produce
  the same consolidated multiset (sort + compare `(PK, payload, weight)`). Each
  helper consumes the trace cursor, so build (or `rewind`) a fresh cursor per call.
  The oracle is the existing per-row path, so this pins Strategy 2 against the
  shipped semantics. Include: boundary equality (`d_i == s`, separating `Gt`/`Ge`
  and `Lt`/`Le`), the maximal slot at `n_eq == 0` (`Gt` of `0xFF…` → `range_cut_points`
  returns `None`, emits nothing) **and** at `n_eq ≥ 1` (`Gt` of a maximal slot in a
  non-maximal group → a zero-width `Some((next_E‖0, next_E‖0))`, which the per-row
  walk and the merge must both treat as no match), a mid-group trace row with weight
  `≤ 0` (the merge must advance `ptr` past it yet suppress its emission, exactly like
  the per-row `w_out == 0` skip — a tombstone slot still participates in the monotone
  walk), empty trace, a trace eq group with **no** matching delta group (the merge
  must forward-advance past it and emit nothing), and a delta eq group with **no**
  matching trace group (skipped, emits nothing).
- **High-fan-out merge** (`output ≫ r`): the case the merge targets, exercised for
  **both** pointer directions — a prefix rel (`Gt`/`Ge`) over small `d` and a suffix
  rel (`Lt`/`Le`) over large `d`, each delta row matching most of its trace group.
  Verify the trace is walked once per group (the pointer is monotone) and output
  equals the oracle.
- **Multiset delta + multi-payload trace**: duplicate `[eq‖d]` on both sides;
  confirm full cross-product and weight products match the oracle.
- **`n_eq == 0` `Lt`/`Le`** (one group, whole trace): the degenerate single-walk
  case; equals the oracle and walks the trace exactly once.
- **Monotone-seek anti-regression** (Strategy 1, after the dependency): a
  *descending*/random delta still matches the per-row `seek_bytes` oracle
  (exercises `seek_bytes_forward`'s overshoot fallback inside the probe).

**Engine circuit shape**: unchanged — the two `Join(DeltaTraceRange { n_eq, rel })`
nodes and the `PartitionFilter` split are not touched; the existing
`test_circuit_range_join_n_eq_discriminator` (`compiler.rs`) and
`test_range_join_partition_filter_circuit_shape` (`gnitz-sql/tests/planner_join.rs`)
stay green with no edit.

**Multi-worker E2E (`make e2e`, `GNITZ_WORKERS=4`, `TestRangeJoin`).** The existing
suite must stay green through both strategies; add deltas that straddle the size
heuristic:

- Strengthen one band test (`test_band_join_eq_prefix_plus_range`) so that for one
  term a single epoch's delta exceeds the *whole* other side's integrated trace
  (`n > trace_len` is global, per term — not per eq group): e.g. seed a small side
  `b`, then insert a much larger side `a` in one epoch so `|ΔA| > |trace_b|` forces
  `range_merge_walk` on the `join_ab` term. Assert the result equals the Python
  cross-filter reference.
- Keep `test_pure_range_all_ops_both_orders` (delta ≤ trace) on the per-row path and
  add a pure-range epoch whose single-epoch delta exceeds the other side's trace
  (forces the single-group merge). Both equal the reference.
- `test_band_join_retraction` and `test_cross_worker_retraction` confirm `±1`
  cancellation is unaffected (output identity unchanged: same pairs, same weights,
  reordered then re-consolidated downstream).

## Scope boundaries (by design, not deferred work)

- **Per-eq-group and fan-out-aware selection are not done.** The heuristic is global
  (`n > trace_len`), mirroring the equi-join. It is **safe** (it never selects the
  merge when the per-row path would be asymptotically cheaper — §Strategy 2) but
  **conservative in a data-dependent way**: because range fan-out is unbounded, a
  high-fan-out delta (`output ≫ r`) would benefit from the merge well below
  `m = trace_len`, and a delta dense in one eq group but sparse in another uses one
  strategy throughout. A fan-out-aware global threshold (e.g. trigger on an estimate
  of `output` rather than `m`) or a per-group choice (merge dense groups, seek
  sparse ones) is a refinement with its own bookkeeping; the global heuristic
  captures the dominant `|Δ| > |trace|` split and is only ever sub-optimal, never
  wrong, elsewhere. Deliberately not split.
- **Output-emission vectorization is orthogonal.** `write_join_row` stays
  row-at-a-time here; column-first block emission for the join cross-product is
  `join-block-vectorize.md` (gated on `merge-walk-column-first.md`) and applies to
  both strategies once that lands. Not in scope.
- **No new storage primitive.** Strategy 1 consumes `seek_bytes_forward` from
  `galloping-forward-seek.md`; this plan adds none. Strategy 2 uses only existing
  cursor/MemBatch operations.
