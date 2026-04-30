# Join Block Vectorization

## Problem

`write_join_row` (`ops/join.rs:430`) is called once per matched `(left_row,
right_cursor_row)` pair. For a delta row with N_R matching trace rows it fires
N_R times, each time touching M_L + M_R column regions. This is the same
row-at-a-time anti-pattern as `copy_current_row_into`, applied to an output
that is structurally a cross-product block.

### Call structure (inner join, `join_dt`, lines 260–283)

```
for i in 0..n_delta:
    while cursor matches d_pk:
        write_join_row(output, delta_mb, i, cursor, w_out, ...)
        cursor.advance()
```

The outer loop is per-left-row; the inner loop is per-matching-right-row.
Left column data for row `i` is repeated unchanged for every inner iteration.
Right column data varies per inner iteration.

The same structure appears in `join_dt_swapped` (lines 292–327) and
`op_join_delta_trace_outer` (lines 336–400).

`write_join_row_null_right` (line 488) — unmatched left rows in outer joins —
is already one row per call with no cross-product; not a target here.

`write_join_row_from_batches` (line 766, delta-delta inner join) — both sides
are `MemBatch`; a different vectorization applies and is left for later.

---

## Fix plan

### Step A — Buffer matching right rows per left key

Before emitting output for a left row, collect all matching right cursor
positions into a scratch buffer `Vec<(u8, u32, i64)>` (entry_idx, row_idx,
net_weight), draining the inner cursor loop:

```rust
thread_local! {
    static RIGHT_BUFFER: RefCell<Vec<(u8, u32, i64)>> = const { RefCell::new(Vec::new()) };
}
```

For each left row `i`:
1. Drain the matching inner cursor loop into `RIGHT_BUFFER`.
2. Allocate (or reuse) an output block of `right_rows.len()` rows.
3. Write left side column-first (Step B).
4. Write right side column-first (Step C).

This replaces the N_R calls to `write_join_row` with two column-first passes.

### Step B — Left side: column-first broadcast

For each left payload column, write the single source value N_R times into
consecutive output positions. This is a broadcast, not a gather:

```rust
fn broadcast_col(
    output: &mut Batch,
    col_idx: usize,
    src: &[u8],   // one column element (col_size bytes)
    n: usize,
    col_size: usize,
) {
    // Extend col region by src repeated n times.
    // For fixed-size columns: a tight copy loop or memset variant.
}
```

PK and null_bmp also broadcast (same left PK and null word for all N_R rows);
weights come from `right_rows[j].2` (Step C handles them with the right pass).

### Step C — Right side: column-first scatter

Use `scatter_cursor_sources` (from the column-first plan) over the buffered
right row indices. This is identical to the `scatter_cursor_sources` call in
the `materialize` / `scan` paths — same infrastructure, same entry accessor
(`ReadCursorEntry::as_columnar()` from Step B0 of merge-walk-column-first.md).

Right column indices in the output are offset by `left_npc` (left payload
column count), so the scatter target columns start at `out_pi = left_npc + rpi`.
`scatter_cursor_sources` needs an output column offset parameter, or a wrapper
that shifts indices.

### Step D — Wire into `join_dt` / `join_dt_swapped` / `op_join_delta_trace_outer`

Replace the inner `write_join_row` loop with the buffered path. The outer
structure (seek, advance, PK matching) is unchanged.

For `op_join_delta_trace_outer`, the null-right fallback (`write_join_row_null_right`)
is unaffected — it fires only when the inner loop produced zero matches.

---

## Dependencies

- `ReadCursorEntry::as_columnar()` and `ReadCursor::all_batch_sources()` —
  Step B0 of `merge-walk-column-first.md`. Required for `scatter_cursor_sources`
  to reach right-side column data.
- `scatter_cursor_sources` itself — Step B1 of `merge-walk-column-first.md`.
- `write_string` generalized to `&impl ColumnarSource` — Step B1.

This plan should not be started until the column-first plan is complete and
`make e2e` is green.

---

## Expected impact

The `write_join_row` symbol does not appear in the 20260427 profile, so this
is not yet a measured bottleneck. Implement only if profiling after the
column-first changes reveals join output cost is significant. The structural
fix (block-per-left-row) is straightforward once the scatter infrastructure
from the column-first plan is in place.
