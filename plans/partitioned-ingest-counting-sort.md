# Counting-sort scatter in `PartitionedTable::ingest_owned_batch`

## The problem

`ingest_owned_batch` scatters an incoming batch across up to 256 partition
tables. The current implementation maintains a `Vec<Vec<u32>>` of row
indices, one inner `Vec` per partition:

```rust
let mut part_indices: Vec<Vec<u32>> = Vec::with_capacity(np);
for _ in 0..np {
    part_indices.push(Vec::new());
}
for i in 0..mb.count {
    let p = partition_for_key(mb.get_pk(i));
    part_indices[p].push(i as u32);
}
for p in 0..np {
    if part_indices[p].is_empty() { continue; }
    let sub_batch = Batch::from_indexed_rows(&mb, &part_indices[p], &self.schema);
    self.tables[local].ingest_owned_batch(sub_batch)?;
}
```

Allocation breakdown per call (256-partition table):

| Item | Count |
|---|---|
| Outer `Vec<Vec<u32>>` (`with_capacity(256)`) | 1 |
| Inner `Vec<u32>` per populated partition | ≤ min(batch\_size, 256) |
| `Batch::from_indexed_rows` data+blob buffers | pool-served, ≈ 0 |

The `Batch` buffers are served from the thread-local pool and cost nothing
in steady state. The bookkeeping Vecs are not pooled and are the actual
overhead. Critically, the outer-vec allocation is proportional to
`num_partitions`, not to batch size — a 1-row batch through a 256-partition
table pays the same outer-vec cost as a 1000-row batch.

The batch pool (`batch_pool.rs`) only recycles `Vec<u8>` Batch data/blob
buffers; the `Vec<u32>` index vecs are too small and wrong-typed to benefit.

## The fix

Replace the `Vec<Vec<u32>>` with a two-pass counting sort:

1. **Count pass** (O(N)): tally rows per partition bucket into a
   stack-allocated `[u32; 257]` counter array.
2. **Prefix-sum** (O(256)): convert counts into start offsets in-place.
   Keep a mutable copy (`cursor`) for the fill pass.
3. **Fill pass** (O(N)): scatter row indices into a single flat
   `Vec<u32>` using the cursor offsets.
4. **Iterate** over each partition's contiguous slice of the flat vec.

This reduces the heap allocation count from O(min(N, 256)) to **1**
(the flat index vec). The flat vec is also a candidate for pooling.

```rust
pub fn ingest_owned_batch(&mut self, batch: Batch) -> Result<(), StorageError> {
    if batch.count == 0 || self.tables.is_empty() {
        return Ok(());
    }
    if self.num_partitions == 1 {
        return self.tables[0].ingest_owned_batch(batch);
    }

    let mb = batch.as_mem_batch();
    let np = self.num_partitions as usize; // always 256
    let n = mb.count;

    // Pass 1: count rows per partition.
    let mut counts = [0u32; 257]; // [0..np] counts, [np] unused sentinel
    if mb.pk_stride as usize > NARROW_PK_MAX_BYTES {
        for i in 0..n {
            counts[partition_for_pk_bytes(mb.get_pk_bytes(i))] += 1;
        }
    } else {
        for i in 0..n {
            counts[partition_for_key(mb.get_pk(i))] += 1;
        }
    }

    // Pass 2: prefix sums → start offsets. cursor[p] = write head for p.
    let mut offsets = [0u32; 257];
    let mut cursor  = [0u32; 256];
    let mut acc = 0u32;
    for p in 0..np {
        offsets[p] = acc;
        cursor[p]  = acc;
        acc += counts[p];
    }
    offsets[np] = acc; // sentinel: end of last partition

    // Pass 3: scatter row indices into flat vec.
    let mut indices: Vec<u32> = Vec::with_capacity(n);
    // SAFETY: every element is written before it is read; total writes == n.
    unsafe { indices.set_len(n); }
    if mb.pk_stride as usize > NARROW_PK_MAX_BYTES {
        for i in 0..n {
            let p = partition_for_pk_bytes(mb.get_pk_bytes(i));
            indices[cursor[p] as usize] = i as u32;
            cursor[p] += 1;
        }
    } else {
        for i in 0..n {
            let p = partition_for_key(mb.get_pk(i));
            indices[cursor[p] as usize] = i as u32;
            cursor[p] += 1;
        }
    }

    // Pass 4: dispatch each partition's contiguous slice.
    let offset = self.part_offset as usize;
    let num_live = self.tables.len();
    for p in 0..np {
        let start = offsets[p] as usize;
        let end   = offsets[p + 1] as usize;
        if start == end { continue; }
        let local = p.wrapping_sub(offset);
        if local >= num_live { continue; }
        let sub_batch = Batch::from_indexed_rows(&mb, &indices[start..end], &self.schema);
        self.tables[local].ingest_owned_batch(sub_batch)?;
    }
    Ok(())
}
```

The `counts` and `cursor`/`offsets` arrays are stack-allocated (no heap).
The single `indices` Vec is the only allocation; it can be pooled using the
same `batch_pool::acquire_buf` / `recycle_buf` pattern (cast to `Vec<u8>`
via `into_raw_parts` + `from_raw_parts` with appropriate length/capacity
scaling) if profiling shows it worth the complexity.

## Correctness notes

- `set_len(n)` followed by write-then-read is safe because the prefix-sum
  guarantees every slot `indices[0..n]` is written exactly once before
  being read in pass 4. The same invariant holds for both the narrow and
  wide PK code paths.
- The `offsets[np]` sentinel (= `n`) makes the `end = offsets[p+1]`
  expression safe for `p == np - 1` without a bounds check.
- Partition indices `p` are always in `0..np` (= 0..256) because
  `partition_for_key` and `partition_for_pk_bytes` both return `(h >> 56)
  as usize`, which is always in `0..256`.

## Out of scope

- Pooling the flat `indices` Vec. The benefit would be marginal (one
  `Vec::with_capacity(n)` per call) and requires either an unsafe
  `Vec<u8>` reinterpretation or a separate u32-typed pool.
- Eliminating `Batch::from_indexed_rows` entirely by passing contiguous
  index slices directly to `Table`'s memtable. That requires a new
  `Table` ingest API accepting a non-owning view and is a larger change.

## Testing

- The existing `multi_partition_hash_routing`, `multi_partition_cursor`,
  and `retract_pk_routing` tests in `partitioned_table.rs` cover the
  functional contract; they must stay green.
- Add a round-trip test with a batch larger than the pool (`count > 256`)
  to exercise the full counting-sort path.
