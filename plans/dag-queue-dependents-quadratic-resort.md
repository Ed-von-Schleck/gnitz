# `queue_dependents` resorts the pending queue once per pushed dependent

`Evaluator::queue_dependents` (`crates/gnitz-engine/src/dag.rs:1462`) fans a
producer view's output onto each dependent's pending edge. For every **newly
pushed** edge it calls `resort_pending` *inside* the loop:

```rust
} else {
    let new_idx = pending.len();
    let batch = match delta {
        Some(d) => d.clone_batch(),
        None => Batch::with_schema(src_schema, 0),
    };
    pending.push(PendingEntry { depth: dep_depth, view_id: dep_id, source_id: view_id, batch });
    pending_pos.insert((dep_id, view_id), new_idx);
    Self::resort_pending(pending, pending_pos);   // ← per push
}
```

`resort_pending` (`dag.rs:1436`) sorts the whole `pending` vector by depth and
**clears and rebuilds the entire `pending_pos` map**:

```rust
fn resort_pending(pending: &mut [PendingEntry], pending_pos: &mut FxHashMap<(i64, i64), usize>) {
    pending.sort_by_key(|n| std::cmp::Reverse(n.depth));
    pending_pos.clear();
    for (i, pe) in pending.iter().enumerate() {
        pending_pos.insert((pe.view_id, pe.source_id), i);
    }
}
```

A producer with `M` fresh dependents runs `M` full sort+rebuild passes over a
queue of size `P`, where `P` itself grows with `M`. The cost is
`O(M · P log P) ≈ O(M² log M)` for a wide fan-out (one view feeding many),
where a single resort at the end would be `O(P log P)`.

## Why one resort at the end is equivalent

The per-push resort is redundant for both correctness and ordering:

- **Indices stay valid without it.** Within this call `pending` is only ever
  *appended to* (the merge branch mutates a batch in place; the push branch
  appends). No element moves, so every index already recorded in `pending_pos`
  — including each `new_idx` inserted on push — keeps pointing at its element.
  The merge branch's `pending[existing_idx]` read and the duplicate-`dep_id`
  lookup (`pending_pos.get(&(dep_id, view_id))`) therefore remain correct
  mid-loop with no intervening resort.
- **Final order is identical.** `sort_by_key` is stable, and within a depth band
  stable order = insertion order. The current code stable-sorts after each push;
  the deferred version stable-sorts once over the same append sequence. Both
  leave each depth band in insertion order, so the resulting `pending` and
  `pending_pos` are byte-for-byte the same. The ordering matters only before the
  driver next pops, which is after this function returns either way.

A resort is needed *iff at least one edge was pushed* — pure merges (the
`if let Some(&existing_idx)` branch) change neither membership nor depth, so they
require none (the current code already skips resort on that branch).

## Fix

Track whether any edge was pushed and resort exactly once after the loop:

```rust
fn queue_dependents(
    pending: &mut Vec<PendingEntry>,
    pending_pos: &mut FxHashMap<(i64, i64), usize>,
    tables: &FxHashMap<i64, TableEntry>,
    dep_view_ids: &[i64],
    view_id: i64,
    src_schema: SchemaDescriptor,
    delta: Option<&Batch>,
) {
    let mut pushed = false;
    for &dep_id in dep_view_ids {
        let dep_depth = match tables.get(&dep_id) {
            Some(e) => e.depth,
            None => continue,
        };

        if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
            if let (true, Some(d)) = (existing_idx < pending.len(), delta) {
                let existing = std::mem::replace(
                    &mut pending[existing_idx].batch,
                    Batch::empty_with_schema(&src_schema),
                );
                let schema = existing.schema.unwrap_or(src_schema);
                let merged = ops::op_union(existing, Some(d), &schema);
                pending[existing_idx].batch = merged;
            }
        } else {
            let new_idx = pending.len();
            let batch = match delta {
                Some(d) => d.clone_batch(),
                None => Batch::with_schema(src_schema, 0),
            };
            pending.push(PendingEntry {
                depth: dep_depth,
                view_id: dep_id,
                source_id: view_id,
                batch,
            });
            pending_pos.insert((dep_id, view_id), new_idx);
            pushed = true;
        }
    }
    if pushed {
        Self::resort_pending(pending, pending_pos);
    }
}
```

## Testing

- Behaviour is unchanged, so the existing `evaluate_dag` / multi-dependent DAG
  tests cover correctness: `cargo test -p gnitz-engine` (the `dag` module tests in
  particular). A fan-out view feeding several dependents in one epoch must still
  produce identical per-dependent outputs and identical processing order.
- No new test is required for behaviour; the change is a pure complexity
  reduction. If a regression guard is wanted, assert that a producer with N
  dependents leaves `pending` sorted by descending depth and `pending_pos`
  consistent with the final indices after a single `queue_dependents` call.
