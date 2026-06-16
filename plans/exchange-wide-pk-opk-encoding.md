# Exchange wide-PK tests must build OPK keys, not raw LE bytes

The `exchange.rs` test helper `wide_pk` (`crates/gnitz-engine/src/ops/exchange.rs:1119`)
builds a 24-byte `(U64, U64, U64)` PK with little-endian column bytes, and
`make_wide_batch` (`:1127`) writes them into the PK region verbatim:

```rust
fn wide_pk(c0: u64, c1: u64, c2: u64) -> [u8; 24] {
    let mut pk = [0u8; 24];
    pk[..8].copy_from_slice(&c0.to_le_bytes());
    pk[8..16].copy_from_slice(&c1.to_le_bytes());
    pk[16..24].copy_from_slice(&c2.to_le_bytes());
    pk
}
```

The PK region's contract is the **order-preserving key (OPK, §6)**: each column
big-endian, signed columns sign-flipped, packed in pk-list order. For an
all-unsigned PK, OPK is plain big-endian — so this writes the **wrong byte
order**. The two consumers these tests exercise read OPK bytes:

- ordering is `compare_pk_bytes` — a `memcmp` over OPK bytes (§2);
- routing is `partition_for_pk_bytes` (`crates/gnitz-engine/src/storage/partitioned_table.rs:479`),
  which for wide regions (`len > 16`) is `xxh3(opk_bytes) >> 56`, and for narrow
  is `mix(widen_pk_be(opk_bytes))`.

So these tests scatter and route a 24-byte layout **no production row carries**
(production wide PKs are always OPK-encoded by the ingest path before they reach
the exchange).

## Why it passes anyway, and what it fails to cover

Both wide-PK tests assert **self-referentially**, which is why LE bytes pass:

- `test_relay_scatter_wide_pk_order_and_routing` (`:1143`) checks (a) each
  worker's output is non-decreasing under `compare_pk_bytes` on the *actual*
  output bytes, and (b) every output row landed in
  `worker_for_partition(partition_for_pk_bytes(actual_pk), num_workers)`.
- `test_relay_scatter_wide_pk_single_source_bulk_drain` (`:1387`) checks the
  same routing identity on the bulk-drain path.

Neither hard-codes a worker index or partition; both recompute the expected
worker from the same `partition_for_pk_bytes` applied to whatever bytes the
scatter emitted. So any self-consistent byte layout — LE included — satisfies
them.

The cost is faithfulness and coverage, not a live failure:

- The scatter, the comparator, and the wide-routing `xxh3` are all exercised
  over a byte pattern that cannot occur in production, so the tests do not
  validate routing/ordering of the bytes the engine actually produces.
- The helpers **never call `encode_order_preserving_pk`** (via `extend_pk_opk`),
  so a wide-compound OPK encoding bug — wrong per-column offset, or a missing
  sign-flip — is invisible here, exactly as in the storage-layer
  `table_wide_pk_has_and_retract_bytes`.
- It is the lone inconsistency in this file: the narrow sibling
  `make_narrow_compound_batch` (`:1209`) was already converted to OPK encoding,
  with the explicit comment "The compound PK at rest is OPK = col0_BE ++
  col1_BE, so encode … through extend_pk_opk rather than writing the raw u128."
  Only the wide helper was left on raw LE. (The wide-PK tests in
  `ops/reduce/tests.rs:272` and `dag.rs:2350` build big-endian by hand, correctly,
  for their all-unsigned keys.)

## Fix

Behaviour-preserving and low-risk: every existing assertion holds because they
are self-referential, the row counts are unchanged, and the input ordering
precondition stays valid (OPK byte order of unsigned columns *is* `(c0,c1,c2)`
lexicographic, and the test rows are already in that order). Mirror the narrow
sibling exactly.

### Change 1 — `make_wide_batch` OPK-encodes via `extend_pk_opk`

Take native column tuples and encode through `extend_pk_opk` (`#[cfg(test)] pub`,
`crates/gnitz-engine/src/storage/batch.rs:739`), as `make_narrow_compound_batch`
already does:

```rust
/// Build a pre-consolidated wide-PK batch from native column tuples. `rows`
/// must already be sorted ascending by (c0,c1,c2) with unique PKs — which is
/// exactly OPK byte order for these unsigned columns. PKs are OPK-encoded via
/// `extend_pk_opk` (matching `make_narrow_compound_batch`), so the bytes are the
/// ones the ingest path and `partition_for_pk_bytes` actually see.
fn make_wide_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, u64, i64, i64)]) -> ConsolidatedBatch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(c0, c1, c2, w, val) in rows {
        b.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}
```

`extend_pk_opk(&self, schema: &SchemaDescriptor, native_col_vals: &[u128])`
packs each native value's low `size()` little-endian bytes, then calls
`encode_order_preserving_pk` — so the resulting region is byte-identical to a
real ingested wide PK.

### Change 2 — delete `wide_pk`

`wide_pk` is used only to feed `make_wide_batch`; no assertion references its
literals (verified: its only call sites are the two test bodies' batch
construction). Remove it (`:1119`-`:1125`).

### Change 3 — update the two call sites to native tuples

`test_relay_scatter_wide_pk_order_and_routing` (`:1150`-`:1164`):

```rust
let b0 = make_wide_batch(&schema, &[
    (0, 0, 0, 1, 10),
    (1, 1, 0, 1, 11),
    (5, 9, 2, 1, 12),
]);
let b1 = make_wide_batch(&schema, &[
    (1, 1, 1, 1, 20),
    (2, 2, 2, 1, 21),
    (9, 9, 9, 1, 22),
]);
let b2 = make_wide_batch(&schema, &[
    (1, 1, 2, 1, 30),
    (3, 3, 3, 1, 31),
    (7, 0, 0, 1, 32),
]);
```

`test_relay_scatter_wide_pk_single_source_bulk_drain` (`:1391`-`:1396`):

```rust
let b0 = make_wide_batch(&schema, &[
    (1, 2, 3, 1, 1),
    (4, 5, 6, 1, 2),
    (7, 8, 9, 1, 3),
    (10, 11, 12, 1, 4),
]);
```

### Invariants preserved

- **`sorted = true` stays valid.** Under OPK (BE for unsigned), byte order of the
  PK region equals `(c0,c1,c2)` lexicographic; all rows above are already in that
  order, so `ConsolidatedBatch::new_unchecked` (which trusts `sorted`) is sound.
  The "sorted ascending by (c0,c1,c2)" doc is now genuinely OPK order rather than
  a coincidence of single-byte values.
- **The low-16-prefix-collision intent holds.** `(1,1,0)`, `(1,1,1)`, `(1,1,2)`
  still share their leading 16 OPK bytes (`BE(1) ++ BE(1)`) and differ only in
  the trailing column, so `compare_pk_bytes`' third-column walk (assertion a) and
  the independent routing of the three via `xxh3` of the full 24 bytes
  (assertion b) are both still exercised. The existing comment at `:1147` stays
  accurate; optionally note it now refers to OPK bytes.
- **Routing assertions hold unchanged.** Both tests recompute the expected worker
  from `partition_for_pk_bytes(actual_pk)`; the actual bytes change LE→OPK and the
  expected worker changes with them, so the equality still holds. Counts (9 and 4)
  are unchanged.

## Out of scope

- **Signed-column wide-PK coverage is intentionally not added here.** At the
  exchange layer, ordering is `compare_pk_bytes` (a sign-agnostic `memcmp`) and
  wide routing is `xxh3` over opaque bytes, so a signed scatter test would add
  little. The canonical signed wide-PK OPK coverage (the sign-flip teeth) belongs
  at the storage layer — it is the subject of the companion fix to
  `table_wide_pk_has_and_retract_bytes` — and `partition_for_pk_bytes`'s own
  determinism is covered by `partition_for_pk_bytes_wide_determinism_and_spread`
  (`partitioned_table.rs:748`).
- **Narrow helpers are already correct.** `make_batch` / `make_batch_str` use
  `extend_pk(pk as u128)`, which writes right-aligned big-endian = OPK for narrow
  unsigned PKs; `make_narrow_compound_batch` already uses `extend_pk_opk`. No
  change needed.
- **`reduce/tests.rs` and `dag.rs`** build their wide keys big-endian by hand and
  are correct; migrating them to a shared helper is optional cleanup, not part of
  this fix.

## Testing

- `cargo test -p gnitz-engine test_relay_scatter_wide_pk_order_and_routing
  test_relay_scatter_wide_pk_single_source_bulk_drain`.
- Both expected green: the changes are byte-layout-only and every assertion is
  self-referential or count-based.
- Confirm parity with `make_narrow_compound_batch` — after this change, both the
  narrow and wide exchange batch builders construct PK regions through the OPK
  encoder, and no `to_le_bytes`-into-PK construction remains in
  `exchange.rs` tests.
