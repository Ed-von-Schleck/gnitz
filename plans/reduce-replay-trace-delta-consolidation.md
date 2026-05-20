# op_reduce replay path leaves trace_in + delta unconsolidated

## Problem

The non-linear replay path in `op_reduce`
(`crates/gnitz-engine/src/ops/reduce.rs`) rebuilds the per-group input
from scratch each tick by stepping over `trace_in ⊕ delta`. The replay
buffer is filled by `fill_cleared_batch`
(`reduce.rs:13-37`), which appends:

- trace_in rows pushed by `ReadCursor::push_current_row`
  (`storage/read_cursor.rs:771-784`) — already net-consolidated by the
  cursor's own merge across its sources,
- delta rows scattered via `scatter_copy`.

`fill_cleared_batch` does NOT merge trace_in rows against delta rows
within the replay buffer. The merge is supposed to happen one line
later inside the replay block (`reduce.rs:984-988`):

```rust
let merged_cs = Batch::consolidate_if_needed(replay, input_schema);
let merged: &Batch = merged_cs.as_deref().unwrap_or(&*replay);
```

But `Batch::clear()` (`storage/batch.rs:1262-1269`) sets
`consolidated = true` as part of its reset:

```rust
pub fn clear(&mut self) {
    self.count = 0;
    self.blob.clear();
    self.sorted = true;
    self.consolidated = true;
    // ...
}
```

`fill_cleared_batch` then writes new rows but leaves the flag alone.
`consolidate_if_needed` short-circuits on `batch.consolidated` and
returns `None`, so `merged = &*replay` — the unconsolidated mixture
of trace_in rows + delta rows.

The step loop only processes positive weights:

```rust
for m in 0..merged.count {
    let w = merged_mb.get_weight(m);
    if w > 0 {
        for acc in accs.iter_mut() {
            acc.step_from_batch(&merged_mb, m, w);
        }
    }
}
```

So if a tick's delta retracts a row that exists in trace_in (same PK,
same payload — the Z-set identity per `foundations.md` §1), the
replay batch carries both as separate rows: trace_in's `(R, +w)` and
delta's `(R, −w)`. The `w > 0` row gets stepped, contributing R's
column value to the new MIN/MAX. The retraction is silently dropped.

For MIN/MAX over a group whose smallest/largest member was retracted
in the current tick, the operator can return the retracted value as
the new aggregate — the retraction has no effect on the output.

## Impact

Silent wrong answers in any MIN/MAX (or future non-linear) view when:

- The current tick's delta retracts at least one row that exists in
  trace_in for the same group, AND
- The operator goes through the replay fallback (i.e., no AVI for this
  agg). AVI MIN/MAX uses `apply_agg_from_value_index`
  (`reduce.rs:650-676`) and does not go through `fill_cleared_batch`.

Both the GI-driven replay (`reduce.rs:926-962`) and the full-trace-scan
fallback (`reduce.rs:963-976`) share the same fill+consolidate flow, so
both inherit the bug. The `group_by_pk` path (`reduce.rs:918-925`)
likewise uses `push_current_row` + `fill_cleared_batch`.

Linear aggregates (COUNT, SUM, COUNT_NON_NULL) are unaffected: they
skip the replay block entirely (`reduce.rs:894-897` takes the
`all_linear && has_old` branch and applies weight arithmetic directly).

## Why no test caught it

`test_reduce_gi_same_pk_multiple_payloads` (`reduce.rs:1677-…`) does
exercise retraction-through-replay: trace_in has `("apple", +1)` and
`("zebra", +1)`, delta retracts `("apple", −1)`, query is
`MAX(string_col)`. With the bug, the replay batch contains apple
(+1), zebra (+1), apple (−1). Step processes apple and zebra; under
the (separately broken) string-as-i64 comparator the result happens to
match the expected MAX, so the test passes for the wrong reason. No
other test exercises retraction through replay.

The fix below is detectable by a small extension of
`test_reduce_min_u64_replay_via_trace_in` that has tick 2 retract a
trace_in row whose value is the current MIN.

## Fix

`fill_cleared_batch` must reset the post-`clear()` "empty and trivially
consolidated" flag before appending — the appended rows are not
self-consolidated against each other.

```rust
fn fill_cleared_batch(...) {
    let needed = trace_rows.len() + delta_indices.len();
    if needed == 0 {
        return;
    }
    batch.reserve_rows(needed);
    {
        let mut writer = batch.capacity_writer();
        if let Some(cursor) = trace_cursor {
            cursor.scatter_drained_into(trace_rows, &mut writer);
        }
        scatter_copy(delta_mb, delta_indices, &[], &mut writer);
    }
    batch.count = needed;
    // Trace + delta are independently sorted but not merged against
    // each other; downstream consolidate_if_needed must run.
    batch.sorted = false;
    batch.consolidated = false;
}
```

That alone reactivates the existing `consolidate_if_needed` call and
makes the replay batch's row identity match the Z-set algebra.

### Alternative considered

Move the `consolidated = false` to the caller in `op_reduce`. Worse:
every caller of `fill_cleared_batch` would need the same comment, and
the invariant is local to the function — it's the one putting the
mixed rows in.

### Performance

The replay batch already runs `consolidate_if_needed` when the input
was non-trivial — that's the call we're "re-enabling." For ticks where
trace_in is empty (first tick), `needed == trace_rows.len() +
delta_indices.len()` might still trigger consolidation on the delta's
own rows; the delta arrives already consolidated, but
`consolidate_if_needed` doesn't trust the source. Marginal: most ticks
already paid this cost when the operator was first written and the bug
hadn't yet hidden it.

If a profile shows the always-on consolidate is meaningful (it
shouldn't be — replay batches are per-group and small), gate it on
`trace_rows.len() > 0 && delta_indices.len() > 0` and skip the
consolidate when one side is empty.

### Sortedness

`scatter_drained_into` writes trace_in rows in cursor order (sorted by
PK across the cursor's sources). `scatter_copy` writes delta rows in
`delta_indices` order (sorted by *group columns*, not by PK — that's
the argsort output from `argsort_delta`). So even per-side, the
concatenation is not PK-sorted; setting `sorted = false` is the honest
flag.

## Testing

Regression test in `crates/gnitz-engine/src/ops/reduce.rs` tests
module — extend or add alongside
`test_reduce_min_u64_replay_via_trace_in`:

- **`test_reduce_min_retracts_trace_in_row`** — trace_in:
  `[(pk=1, grp=7, val=10), (pk=2, grp=7, val=20)]`. trace_out: old
  MIN = 10. Delta: retract `(pk=1, grp=7, val=10)`. Expected: retract
  old MIN (10) + emit new MIN (20). Pre-fix the operator keeps MIN at
  10 because the retracted row's positive copy from trace_in still
  gets stepped.

- **`test_reduce_max_retracts_trace_in_row`** — symmetric on MAX.

- **`test_reduce_count_non_null_unaffected`** — sanity check that the
  linear-aggregate fast path (which never enters the replay block) is
  unchanged. Mostly exists to lock in the "fix doesn't accidentally
  reroute linear aggs" invariant.

- Existing `test_reduce_gi_same_pk_multiple_payloads` stays green; its
  asserted MAX is independent of whether the retracted apple is
  stepped (zebra > apple either way). The test still validates the
  GI-driven replay's PK enumeration, just not the consolidation.

E2E case: `SELECT grp, MIN(val) FROM t GROUP BY grp` after
`INSERT (1, 7, 10); INSERT (2, 7, 20); DELETE WHERE pk=1`. Expected
`MIN = 20`. Pre-fix: `MIN = 10` (the deleted row's value).

## Out of scope

- **AVI semantics.** AVI MIN/MAX seeds the accumulator from the index
  cursor's smallest/largest visible key
  (`apply_agg_from_value_index`); it bypasses replay entirely. Whether
  AVI deletion bookkeeping is correct is a separate question.
- **Cursor-self consolidation.** trace_in's own multi-source merge in
  `ReadCursor` is correct (it produces net weights). The bug is purely
  in how trace_in rows are combined *with* the delta inside the replay
  buffer.
- **String/U128/UUID MIN/MAX.** The binder
  (`crates/gnitz-sql/src/binder.rs`) now rejects these for MIN/MAX
  (sibling fix in `reduce-unsigned-minmax-correctness.md`). Once the
  fix here lands, no String row can reach the replay step via MIN/MAX,
  so the wrong-comparator masking that hides the bug in the existing
  GI test is moot — the test still passes (zebra > apple by *some*
  ordering on inline German-string prefixes, even if not the right
  one), but the bug it should have caught is fixed by this plan.
- **Linear aggregate paths.** COUNT, SUM, COUNT_NON_NULL use weight
  arithmetic (`merge_accumulated(old_vals[k], 1)`) instead of replay
  and do not touch `fill_cleared_batch`.
