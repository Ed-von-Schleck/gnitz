# Single-row fast path for `enforce_unique_pk_partitioned`

## What

`enforce_unique_pk_partitioned` (`crates/gnitz-engine/src/dag.rs:1368`) runs on
**every** base-table ingest write. It unconditionally allocates two collections
sized to the batch:

```rust
let mut seen: HashMap<u128, usize> = HashMap::with_capacity(batch.count);
let mut store_retracted: HashSet<u128> = HashSet::with_capacity(batch.count);
```

- `seen` tracks the last +1 insertion per PK for **intra-batch** dedup.
- `store_retracted` prevents emitting a stored-row retraction twice for the same
  PK within one batch.

For a single-row batch (`batch.count == 1`) neither is needed: there is no second
row to dedup against, and at most one stored row can be retracted. The two
`with_capacity(1)` allocations are pure per-write overhead on the hottest ingest
path (single-row INSERT/DELETE).

## Change

Add a `batch.count == 1` fast path before the map allocations. It preserves the
exact semantics of the general loop: `w > 0` retracts any existing stored row
(emitting `-1` with the stored payload) then appends the insertion; `w < 0`
retracts the stored row if present, else passes the retraction through; `w == 0`
is dropped (the general loop's `if w>0 / else if w<0` also drops it).

```rust
fn enforce_unique_pk_partitioned(
    &self,
    ptable: &mut PartitionedTable,
    schema: &SchemaDescriptor,
    batch: Batch,
) -> Batch {
    if batch.count == 0 {
        return batch;
    }

    // Single-row fast path: no intra-batch dedup is possible and at most one
    // stored row can be retracted, so the seen/store_retracted maps are dead.
    if batch.count == 1 {
        let w = batch.get_weight(0);
        let pk = batch.get_pk(0);
        let mut effective = Batch::with_schema(*schema, 2);
        if w > 0 {
            let (_existing_w, found) = ptable.retract_pk(pk);
            if found {
                effective.append_row_from_ptable_found(ptable, batch.get_pk_bytes(0), -1);
            }
            effective.append_batch(&batch, 0, 1);
        } else if w < 0 {
            let (_existing_w, found) = ptable.retract_pk(pk);
            if found {
                effective.append_row_from_ptable_found(ptable, batch.get_pk_bytes(0), -1);
            } else {
                effective.append_batch(&batch, 0, 1);
            }
        }
        return effective;
    }

    // ... existing multi-row path unchanged (seen / store_retracted) ...
}
```

`retract_pk`, `append_row_from_ptable_found`, and `append_batch` are the same
methods the multi-row path already calls, with the same signatures
(`storage/partitioned_table.rs`, `storage/batch.rs`).

## Scope / non-goals

- The multi-row path is untouched.
- This is an allocation trim, not a correctness fix; the general path is already
  correct. Expect a small win concentrated on single-row-insert workloads;
  measure with the ingest benchmarks before and after rather than assuming.

## Testing

- `make test` — existing DML/unique-PK unit tests cover both paths; the fast
  path must produce byte-identical `effective` batches to the general path for
  `count == 1` inputs (insert-new, upsert-existing, delete-existing,
  delete-absent).
- Add a unit test that runs the same single-row inputs through both code paths
  (force the general path via a `count == 1` batch with the fast path disabled
  behind a test-only flag, or assert against a hand-built expected batch).
- `make e2e` must stay green.
