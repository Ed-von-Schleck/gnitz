# Zero-copy worker indices in `compute_worker_indices`

## Problem

`compute_worker_indices` (`ops/exchange.rs:175`) routes batch rows to per-worker
index lists using a thread-local scratch buffer (`SCATTER_INDICES`), then clones
the result to return it:

```rust
pub fn compute_worker_indices(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<u32>> {
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        // fill worker_indices[w] for each row ...
        worker_indices[..num_workers].to_vec()   // <-- deep clone: N allocs + N memcpy
    })
}
```

The `.to_vec()` clones `num_workers` `Vec<u32>` objects — each with its own heap
allocation and a memcpy of the accumulated indices. The caller immediately passes
the cloned `Vec<Vec<u32>>` as `&[Vec<u32>]` to `scatter_wire_group` or stores it
in `CheckPayload::ScatterSource`, then drops it.

In the 2026-04-30 profile, `LocalKey::with → compute_worker_indices → committer`
appears at 2.41% self-cost and contributes 6.23% of the memmove bucket. The
clone is the dominant cost once the per-row hashing is done.

There are three call sites in `master.rs`:
- `write_commit_group` (line 1481) — committer hot path, called for every push
- `validate_all_distributed_async` FK parent check (line 1097)
- `validate_all_distributed_async` unique-index check (line 1165)

All three immediately pass the result to `scatter_wire_group(&[Vec<u32>])`.

---

## Fix

`scatter_wire_group` already takes `&[Vec<u32>]`. The fix is to keep the TLS
borrow alive through the `scatter_wire_group` call, eliminating the clone
entirely. Encapsulating the borrow in a closure-taking helper keeps
`SCATTER_INDICES` private to `exchange.rs` and prevents callers from holding the
borrow longer than needed.

### Step 1 — Add `fill_worker_indices` and a closure-based public API

```rust
/// Fill `out[..num_workers]` with per-worker row indices from `batch`.
/// Grows `out` to `num_workers` if shorter; clears `out[0..num_workers]`
/// while preserving inner-Vec capacity.
fn fill_worker_indices(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
    out: &mut Vec<Vec<u32>>,
) {
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);
    if out.len() < num_workers {
        out.resize_with(num_workers, Vec::new);
    }
    for w in 0..num_workers { out[w].clear(); }
    for i in 0..batch.count {
        let partition = hash_row_for_partition(&mb, i, col_indices, schema);
        out[w_map[partition]].push(i as u32);
    }
}

/// Compute per-worker row indices into the TLS pool and call `f` with a
/// borrowed view. Avoids cloning out of the pool. The closure must not
/// re-enter `compute_worker_indices` / `with_worker_indices` on the same
/// thread (RefCell would panic).
pub fn with_worker_indices<F, R>(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
    f: F,
) -> R
where
    F: FnOnce(&[Vec<u32>]) -> R,
{
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        fill_worker_indices(batch, col_indices, schema, num_workers, &mut worker_indices);
        f(&worker_indices[..num_workers])
    })
}
```

Reduce `compute_worker_indices` to a thin wrapper that delegates to
`fill_worker_indices`, keeping the owned-result API for FK/unique paths:

```rust
pub fn compute_worker_indices(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<u32>> {
    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        fill_worker_indices(batch, col_indices, schema, num_workers, &mut worker_indices);
        worker_indices[..num_workers].to_vec()
    })
}
```

### Step 2 — Rewrite `write_commit_group` to borrow through the closure

```rust
pub(crate) fn write_commit_group(
    &mut self,
    target_id: i64, lsn: u64, batch: &Batch, mode: WireConflictMode,
    req_ids: &[u64],
) -> Result<(), String> {
    let (schema, col_names) = self.get_schema_and_names(target_id);
    let pk_col = &[schema.pk_index];
    let (name_refs, n) = col_names_as_refs(&col_names);
    let col_names_opt = if n == 0 { None } else { Some(&name_refs[..n]) };
    with_worker_indices(batch, pk_col, &schema, self.num_workers, |worker_indices| {
        self.record_index_routing(target_id, &schema, batch, worker_indices);
        self.sal.scatter_wire_group(
            batch, worker_indices, &schema, col_names_opt,
            target_id as u32, lsn, FLAGS_PUSH_CONFLICT_MODE,
            0, mode.as_u8() as u64, req_ids, -1,
        )
    })
}
```

Safety: `write_commit_group` is fully synchronous; there is no `.await` between
the borrow and the end of the closure. `record_index_routing` and
`scatter_wire_group` do not re-enter `SCATTER_INDICES` (only
`compute_worker_indices` and the test-only `op_multi_scatter` access that
pool). The single-threaded master event loop guarantees no concurrent access.

If `write_commit_group` is ever refactored to introduce an `.await`, the
closure-shaped API forces the borrow to end at the closure boundary — the
`.await` simply cannot live inside it. No runtime guard required.

### Step 3 — Leave the two `validate_all_distributed_async` call sites unchanged

Both sites build a `CheckPayload::ScatterSource { source, worker_indices }` where
`worker_indices` is stored in the `PipelinedCheck` and later passed to
`scatter_wire_group` inside `execute_pipeline_async`. The plan phase (in
`validate_all_distributed_async`) and the execute phase
(`execute_pipeline_async`, which crosses an `.await`) are decoupled, so a Vec
held inside `PipelinedCheck` cannot be a TLS borrow.

These paths are not flagged in the current profile. The unique-index check
(line 1165) does scale with the upsert batch and is not "tens of rows" in
bulk-upsert workloads, but the closure-based zero-copy refactor here does not
apply unchanged: the worker_indices field is constructed in one phase and
consumed in another, with intervening `.await`. Keep the owned-Vec API for
these sites.

Two alternatives were considered for these sites and rejected as out of scope:

- **`Box<[Box<[u32]>]>` for `ScatterSource`** — same N+1 heap allocations on
  construction (each inner Vec shrink-to-fits on `into_boxed_slice`); net wash.
- **Defer index computation to execute time** — change `ScatterSource` to hold
  `col_indices: Vec<u32>` (the spec) and call `with_worker_indices` per check
  inside the sync section of `execute_pipeline_async` (between
  `sal_excl.lock().await` and `disp.signal_all()`, where no `.await` separates
  the scatter calls). Eliminates clones at the cost of recomputing per-check
  routing. Defer until profile flags these paths.

---

## Scope

| File | Change |
|------|--------|
| `ops/exchange.rs` | Add private `fill_worker_indices` and pub `with_worker_indices`; rewrite `compute_worker_indices` as a wrapper |
| `runtime/master.rs` | Replace `compute_worker_indices` import with `with_worker_indices` in `write_commit_group`; FK/unique sites unchanged |

`record_index_routing` already takes `&[Vec<u32>]`. `scatter_wire_group`
already takes `&[Vec<u32>]`. The plain slice satisfies both, so the borrowed
view from inside the closure works without further API changes.

`SCATTER_INDICES` stays private to `exchange.rs`.

---

## Cross-pool note

`SCATTER_INDICES` is distinct from `WORKER_ROWS` (the pool used by
`op_repartition_batches_impl` for relay scatter). The two pools do not alias.
The only other reader of `SCATTER_INDICES` is `op_multi_scatter`, which is
`#[cfg(test)]`. There is no production re-entry path.

---

## Expected impact

Eliminates `num_workers + 1` heap allocations, `num_workers` memcpys, and
`num_workers + 1` deallocations per committer push (one outer `Vec` plus one
per inner). The pool's inner-Vec capacity is already preserved across calls
via `clear()`; the new win is on the consumer side — the outer `Vec<Vec<u32>>`
and its `num_workers` heap-allocated inner slots are no longer materialized
(empty inner Vecs clone cheaply but non-empty ones each allocate + memcpy).

In the 2026-04-30 profile, `LocalKey::with → compute_worker_indices →
committer` accounted for 2.41% self-cost and 6.23% of the memmove bucket. The
self-cost attribution is to the closure body — primarily the `to_vec` clone
chain — so removing `to_vec` removes that body cost; the bare `with` call
itself stays. Expected reduction: the full 6.23% memmove contribution and most
of the 2.41% closure self-cost.
