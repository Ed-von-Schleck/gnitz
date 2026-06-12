# Stack-allocate the index-PK scratch buffer in `batch_project_index`

## What

`batch_project_index` (`crates/gnitz-engine/src/dag.rs:1446`) projects a source
batch into a secondary-index batch. It allocates a heap scratch buffer for the
packed index PK:

```rust
// dag.rs:1473 — hoisted above the per-row loop
let mut idx_pk_buf = vec![0u8; idx_stride];
```

This is **one allocation per call** (per index, per ingested batch), not per
row — the buffer is reused across rows inside the loop. The savings are
therefore modest: it removes one heap allocation per index-projection call,
which matters most when many small batches each fan out to several indexes.

`idx_stride = idx_schema.pk_stride()`. The index PK is
`(indexed_col_promoted, src_pk…)`, both bounded by the 16-byte PK slot, so
`idx_stride` is small and always `<= MAX_PK_BYTES` (`gnitz-wire`,
`MAX_PK_BYTES = MAX_PK_COLUMNS * 16 = 256`).

## Change

Replace the heap buffer with a fixed-size stack array sized to the hard upper
bound, sliced to `idx_stride`:

```rust
let mut idx_pk_buf = [0u8; gnitz_wire::MAX_PK_BYTES];
let buf = &mut idx_pk_buf[..idx_stride];
```

Then use `buf` where `idx_pk_buf` is used today, and write the row out with the
exact-width slice:

```rust
out.extend_pk_bytes(&idx_pk_buf[..idx_stride]);
```

The zero-pad invariant the current code relies on (the comment at dag.rs:1469
notes the middle `[src_col_size..idx_key_size]` slot stays zero across rows)
still holds: the stack array is zero-initialized, and that middle slot is never
written.

## Scope / non-goals

- This is a micro-optimization. A 256-byte stack array is zero-initialized each
  call (a fixed `memset`), traded against removing one heap allocation. For
  small/single-row batches the trade favors the stack buffer; for large batches
  the original alloc is already well amortized. **Measure** with the ingest +
  index benchmarks; drop the change if it does not move the needle.
- No semantic change to the projected index batch.

## Testing

- `make test` — index-projection unit tests must produce byte-identical output.
- A property test asserting `batch_project_index` output is identical before and
  after, over random source batches with PK-column and payload-column indexed
  values, single- and multi-row.
