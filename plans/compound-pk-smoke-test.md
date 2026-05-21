# Compound-PK smoke test inside `make test`

A single integration test covering the engine compound-PK byte paths,
runnable via `make test` (the canonical sub-second feedback loop), would
catch every engine-layer compound-PK bug simultaneously and without
spinning up a server process.

## Scope

In:

- New test module `crates/gnitz-engine/src/catalog/tests/compound_pk_smoke.rs`
  registered in the existing `catalog/tests/mod.rs`. Living here gives the
  module immediate access to all private helpers (`u64_col_def`,
  `str_col_def`, `temp_dir`) without re-exporting or duplicating them.
- Coverage matrix per test: at least one `(U64, U64)` stride-16 case
  and one `(U32, U32, U32, U32)` stride-16 case. The two share the
  same code paths but trip different stride / per-column-size arithmetic.
- A routing-symmetry deterministic test: for a fixed set of compound-PK
  values, assert that the master's `compute_worker_indices` and the
  worker's `partition_for_pk_bytes` map each row to the same worker.
- A schema round-trip test at two levels:
  - **Wire codec**: `schema_to_batch` / `batch_to_schema` (both in
    `crate::runtime::wire`; the latter is `#[cfg(test)]` only).
  - **Catalog persistence**: write a compound-PK schema to a
    `CatalogEngine`, close, reopen, and assert `pk_indices()` is
    preserved in order.

Out:

- SQL-level coverage. That lives in `crates/gnitz-sql/tests/` and
  `crates/gnitz-py/tests/`. The smoke test exists because SQL failures
  obscure the engine-layer root cause.
- Stride > 16 coverage. The reduce/trace cursors still pack the PK as
  `u128`; lifting that is its own plan. The smoke test caps at stride 16.
- Catalog DDL flows. Each is exercised by its own dedicated test file.

## Touchpoints

Add to the existing `catalog/tests/mod.rs` module list:

```rust
mod compound_pk_smoke;
```

The smoke test module declares its own column-builder shims so it does not
depend on the `*_col_def` family (which produce `ColumnDef`, not
`SchemaColumn`):

```rust
fn u64_col() -> SchemaColumn { SchemaColumn::new(type_code::U64, 0) }
fn u32_col() -> SchemaColumn { SchemaColumn::new(type_code::U32, 0) }
```

`crates/gnitz-engine/src/ops/exchange.rs`:

- Tighten `is_single_pk_col` — the current predicate returns `true` for any
  single PK column, including one column of a compound PK, breaking the
  fast-path assumption that the `u128` equals the full routing key:

  ```rust
  #[inline]
  fn is_single_pk_col(schema: &SchemaDescriptor, col_indices: &[u32]) -> bool {
      col_indices.len() == 1 && schema.pk_indices() == col_indices
  }
  ```

- Fix `extract_col_key` for compound PKs. The current code returns the full
  packed `u128` PK region whenever `is_pk_col(col_idx)` is true. For a
  compound PK, every PK column satisfies that predicate, so a single-column
  routing request (e.g. a unique-index route on column 0 of `(U64, U64)`)
  gets the concatenated region instead of the specific column value:

  ```rust
  fn extract_col_key(
      mb: &MemBatch<'_>,
      row: usize,
      col_idx: usize,
      schema: &SchemaDescriptor,
  ) -> u128 {
      if schema.is_pk_col(col_idx) {
          if schema.pk_indices().len() == 1 {
              return mb.get_pk(row);          // fast path: single-PK u128
          }
          // Compound PK: slice just this column's bytes out of the PK region.
          let offset = schema.pk_byte_offset(col_idx) as usize;
          let bytes = mb.get_pk_bytes(row);
          let col = &schema.columns[col_idx];
          return crate::schema::promote_to_index_key(
              bytes, offset, col.size() as usize, col.type_code,
          );
      }
      // ... payload columns unchanged ...
  }
  ```

- Extend the sorted/consolidated flag propagation in `op_multi_scatter` to
  cover the full-compound-PK scatter case. Currently the propagation is
  gated only on `is_single_pk_col`; `is_compound_pk` is computed in the
  routing loop but not consulted here. When scattering by the full compound
  PK, each worker's sub-batch is still a sorted, consolidated subset of the
  original:

  ```rust
  // Flag propagation: PK-spec sub-batches inherit sorted/consolidated from source.
  for si in 0..n_specs {
      let spec = col_specs[si];
      if is_single_pk_col(schema, spec) || schema.group_cols_eq_pk(spec) {
          if crate::storage::ConsolidatedBatch::from_batch_ref(batch).is_some() {
              for batch in results[si].iter_mut().take(num_workers) {
                  if batch.count > 0 {
                      batch.sorted = true;
                      batch.consolidated = true;
                  }
              }
          } else if batch.sorted {
              for batch in results[si].iter_mut().take(num_workers) {
                  if batch.count > 0 {
                      batch.sorted = true;
                  }
              }
          }
      }
  }
  ```

`crates/gnitz-engine/src/storage/batch.rs`:

- Add `assert!` bounds checks at the top of `append_mem_batch_range`.
  The checks are O(1) relative to the batch copy. In release builds,
  a caller passing `start > end` wraps the subtraction to a huge `n`,
  which `reserve_rows(n)` passes directly to the allocator (OOM):

  ```rust
  pub(crate) fn append_mem_batch_range(
      &mut self,
      src: &MemBatch<'_>,
      start: usize,
      end: usize,
      weight_override: Option<i64>,
  ) {
      assert!(
          start <= end,
          "append_mem_batch_range: start ({start}) > end ({end})",
      );
      assert!(
          end <= src.count,
          "append_mem_batch_range: end ({end}) > src.count ({})",
          src.count,
      );
      let n = end - start;
      if n == 0 { return; }
      ...
  }
  ```

- Fix batch_pool recycling in `with_schema`, `reserve_rows`, and
  `write_to_batch`. All three paths currently return a too-small pooled
  buffer to the pool before falling back to a fresh allocation. Drop it
  instead:

  ```rust
  // Before:
  super::batch_pool::recycle_buf(buf);
  vec![0u8; total_size]

  // After:
  drop(buf);   // evict the undersized buffer; pool converges to larger sizes
  vec![0u8; total_size]
  ```

  In `write_to_batch` the fallback is `alloc_large_zeroed(arena_size)`;
  apply the same eviction before it.

## Out-of-scope hardening (unambiguously better, include in this commit)

`crates/gnitz-engine/src/storage/batch.rs` — `from_regions`:

Replace `unsafe { data.set_len(total_size) }` with `data.resize(total_size, 0)`.
The safety comment's invariant (every non-null region's bytes are
overwritten by the subsequent copy loop) holds for well-formed inputs, but
a null pointer with non-zero size leaves uninitialized bytes in the buffer.
`resize` eliminates the uninitialized-memory window at the cost of zeroing
bytes that are immediately overwritten — acceptable for a cold path.

All callers pass `sizes[i] = count * element_size`. If they don't,
integer-division truncates the stride silently; if `stride > 255`, the `as
u8` cast also silently truncates, corrupting every offset computed from it.
Make both invariants explicit:

```rust
pub unsafe fn from_regions(
    ptrs: &[*const u8],
    sizes: &[u32],
    count: usize,
    num_payload_cols: usize,
) -> Self {
    ...
    let nr = REG_PAYLOAD_START + num_payload_cols;
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    for i in 0..nr {
        assert_eq!(
            sizes[i] as usize % count,
            0,
            "from_regions: region {i} size ({}) is not a multiple of row count ({count})",
            sizes[i],
        );
        let stride = sizes[i] as usize / count;
        assert!(
            stride <= u8::MAX as usize,
            "from_regions: region {i} stride ({stride}) exceeds u8::MAX",
        );
        strides[i] = stride as u8;
    }
    ...
    let mut data = super::batch_pool::acquire_buf();
    data.clear();
    data.resize(total_size, 0);  // replaces reserve + set_len
    ...
}
```

`crates/gnitz-engine/src/storage/batch.rs` — `encode_multi_to_wire`:

Add a schema-consistency guard inside the batch loop. If a planner bug
routes batches from different tables into the same slice, region-count
mismatches produce silent payload corruption — `region_size` falls back
to returning `blob.len()` for any index beyond the batch's own `blob_idx`,
silently contributing the wrong size to every non-blob region after it:

```rust
for batch in batches {
    debug_assert_eq!(
        batch.num_regions_total(),
        nr,
        "encode_multi_to_wire: batch schema mismatch in multi-encode payload"
    );
    ...
}
```

`crates/gnitz-engine/src/storage/partitioned_table.rs` — `close_partitions_outside`:

If `start < part_offset`, surviving tables are pushed back at indices
shifted right by `start` but the new offset is `start`, so every lookup
routes to the wrong physical directory. The current production call site
(`trim_worker_partitions`) only shrinks ranges so this path is not live,
but the invariants should be explicit before any caller grows. Also guard
against inverted ranges:

```rust
pub fn close_partitions_outside(&mut self, start: u32, end: u32) {
    assert!(
        start <= end,
        "close_partitions_outside: start ({start}) > end ({end})",
    );
    assert!(
        start >= self.part_offset,
        "close_partitions_outside: left-expansion (start={start} < part_offset={}) \
         not supported — surviving tables would map to wrong indices",
        self.part_offset,
    );
    ...
}
```

## Implementation: helpers

```rust
// crates/gnitz-engine/src/catalog/tests/compound_pk_smoke.rs

use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::storage::Batch;

fn u64_col() -> SchemaColumn { SchemaColumn::new(type_code::U64, 0) }
fn u32_col() -> SchemaColumn { SchemaColumn::new(type_code::U32, 0) }
fn i64_col() -> SchemaColumn { SchemaColumn::new(type_code::I64, 0) }

fn compound_pk_schema_u64_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[u64_col(), u64_col(), i64_col()],
        &[0, 1],
    )
}

fn compound_pk_schema_u32_x4() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[u32_col(), u32_col(), u32_col(), u32_col(), i64_col()],
        &[0, 1, 2, 3],
    )
}

/// Build a batch for a `(U64, U64, I64)` compound-PK schema with a single
/// I64 payload column.  Weights are +1; payload is 0.
fn make_batch_with_pks(schema: &SchemaDescriptor, keys: &[(u64, u64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, keys.len());
    for &(k0, k1) in keys {
        let mut pk = [0u8; 16];
        pk[..8].copy_from_slice(&k0.to_le_bytes());
        pk[8..16].copy_from_slice(&k1.to_le_bytes());
        b.extend_pk_bytes(&pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    b
}
```

## Implementation: routing-symmetry tests

The bug these catch: master using `extract_group_key` (hash combine of all
PK columns) while the worker uses `partition_for_pk_bytes` (raw bytes),
agreeing only by accident for single-PK U64.

`compute_worker_indices` groups rows by worker, not original order, so
the master assignments must be scatter-filled before comparison:

```rust
#[test]
fn routing_symmetry_master_worker() {
    use crate::ops::exchange::{compute_worker_indices, worker_for_partition};
    use crate::storage::partition_for_pk_bytes;

    let schema = compound_pk_schema_u64_u64();
    // 100 deterministic pairs covering varied hash buckets.
    let pk_pairs: Vec<(u64, u64)> = (0u64..100)
        .map(|i| (i.wrapping_mul(13).wrapping_add(7), i.wrapping_mul(97).wrapping_add(11)))
        .collect();
    let batch = make_batch_with_pks(&schema, &pk_pairs);
    let num_workers = 4;

    // Scatter-fill: master_workers[row] = which worker the master routed it to.
    let mut master_workers = vec![0usize; batch.count];
    for (w, row_indices) in compute_worker_indices(
        &batch, schema.pk_indices(), &schema, num_workers,
    ).into_iter().enumerate() {
        for row_idx in row_indices {
            master_workers[row_idx as usize] = w;
        }
    }

    // Worker side: partition_for_pk_bytes on the raw PK region.
    let worker_workers: Vec<usize> = (0..batch.count)
        .map(|i| {
            let p = partition_for_pk_bytes(batch.as_mem_batch().get_pk_bytes(i));
            worker_for_partition(p, num_workers)
        })
        .collect();

    assert_eq!(
        master_workers, worker_workers,
        "master and worker routing diverged for compound U64 PK",
    );
}

#[test]
fn routing_symmetry_four_u32() {
    use crate::ops::exchange::{compute_worker_indices, worker_for_partition};
    use crate::storage::partition_for_pk_bytes;

    let schema = compound_pk_schema_u32_x4();
    let mut b = Batch::with_schema(schema, 100);
    for i in 0u32..100 {
        let mut pk = [0u8; 16];
        pk[0..4].copy_from_slice(&i.to_le_bytes());
        pk[4..8].copy_from_slice(&i.wrapping_mul(13).wrapping_add(7).to_le_bytes());
        pk[8..12].copy_from_slice(&i.wrapping_mul(97).wrapping_add(11).to_le_bytes());
        pk[12..16].copy_from_slice(&i.wrapping_mul(31).wrapping_add(3).to_le_bytes());
        b.extend_pk_bytes(&pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    let num_workers = 4;

    let mut master_workers = vec![0usize; b.count];
    for (w, row_indices) in compute_worker_indices(
        &b, schema.pk_indices(), &schema, num_workers,
    ).into_iter().enumerate() {
        for row_idx in row_indices {
            master_workers[row_idx as usize] = w;
        }
    }

    let worker_workers: Vec<usize> = (0..b.count)
        .map(|i| {
            let p = partition_for_pk_bytes(b.as_mem_batch().get_pk_bytes(i));
            worker_for_partition(p, num_workers)
        })
        .collect();

    assert_eq!(
        master_workers, worker_workers,
        "master and worker routing diverged for compound 4xU32 PK",
    );
}
```

## Implementation: schema round-trip tests

**Wire codec** — `schema_to_batch` is `pub(crate)` in `runtime::wire`
and takes column names; `batch_to_schema` is `#[cfg(test)] pub(crate)`
and returns `Result<(SchemaDescriptor, Vec<Vec<u8>>), &'static str>`.
The names slice may be empty (yields empty name bytes, which is fine
for a structural round-trip).

```rust
#[test]
fn schema_roundtrip_wire_preserves_pk_order() {
    use crate::runtime::wire::{schema_to_batch, batch_to_schema};

    let cases: &[(&[SchemaColumn], &[u32])] = &[
        (&[u64_col(), u64_col()], &[0, 1]),     // (a, b)
        (&[u64_col(), u64_col()], &[1, 0]),     // PRIMARY KEY (b, a)
        (&[u32_col(), u32_col(), u32_col(), u32_col()], &[0, 1, 2, 3]),
    ];
    for &(cols, pk_indices) in cases {
        let original = SchemaDescriptor::new(cols, pk_indices);
        let batch = schema_to_batch(&original, &[]);
        let (decoded, _names) = batch_to_schema(&batch).unwrap();
        assert_eq!(
            original, decoded,
            "pk_indices {:?} did not survive wire round-trip",
            pk_indices,
        );
    }
}
```

**Catalog persistence** — tests the full write-back path:

```rust
#[test]
fn schema_roundtrip_catalog_preserves_pk_order() {
    let cols = vec![
        super::u64_col_def("payload"),
        super::u64_col_def("a"),
        super::u64_col_def("b"),
    ];
    let dir = super::temp_dir("cpk_pk_order_roundtrip");

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_table("public.cpk_order", &cols, &[2, 1], true).unwrap();
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.get_by_name("public", "cpk_order").unwrap();
        let schema = engine.get_schema(tid).unwrap();
        assert_eq!(
            schema.pk_indices(), &[2, 1],
            "PK order (b, a) was not preserved across catalog restart",
        );
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}
```

## Testing

- `make test` runs the smoke module with the same sub-second turnaround
  as the rest of `gnitz-engine`'s tests.
- Any future plan that touches PK encoding, routing, or schema layout
  must cite a successful smoke-test run before merge. Plans that claim
  "compound PK still works" must pass these tests, not hand-wave.
- If the smoke test fails as a side effect of an unrelated change, the
  failure points at the exact engine layer that broke.
