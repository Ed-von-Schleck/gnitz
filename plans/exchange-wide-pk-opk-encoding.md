# Exchange wide-PK tests must build OPK keys, not raw LE bytes

The `exchange.rs` test helper `wide_pk` (`crates/gnitz-engine/src/ops/exchange.rs:1124`)
builds a 24-byte `(U64, U64, U64)` PK with little-endian column bytes, and
`make_wide_batch` (`:1134`) writes them into the PK region verbatim via
`extend_pk_bytes` (`:1137`):

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

- ordering is `compare_pk_bytes` — a `memcmp` over OPK bytes (§2); the wide
  merge comparator `select_wide` (`exchange.rs:762`) calls it directly to pick
  the next row, and assertion (a) re-checks the per-worker output with it;
- routing is `partition_for_pk_bytes` (`crates/gnitz-engine/src/storage/partitioned_table.rs:479`),
  which for wide regions (`len > 16`) is `xxh3(opk_bytes) >> 56`, and for narrow
  is `mix(widen_pk_be(opk_bytes))`.

So these tests scatter and route a 24-byte layout **no production row carries**
(production wide PKs are always OPK-encoded by the ingest path —
`encode_order_preserving_pk`, `columnar.rs:292` — before they reach the
exchange).

## What the tests actually catch — and what they don't

Both wide-PK tests assert **self-referentially**, which is why LE bytes pass:

- `test_relay_scatter_wide_pk_order_and_routing` (`:1149`) checks (a) each
  worker's output is non-decreasing under `compare_pk_bytes` on the *actual*
  output bytes, and (b) every output row landed in
  `worker_for_partition(partition_for_pk_bytes(actual_pk), num_workers)`.
- `test_relay_scatter_wide_pk_single_source_bulk_drain` (`:1392`) checks the
  same routing identity (b) on the bulk-drain path; it has no ordering check.

Neither hard-codes a worker index or partition; both recompute the expected
worker from the same `partition_for_pk_bytes` applied to whatever bytes the
scatter emitted. So any self-consistent byte layout — LE included — satisfies
them. (Note: the companion plan `wide-pk-test-opk-encoding.md` describes the
exchange tests as having "partition expectations" that switching to OPK would
force re-deriving. That is inaccurate for these tests — they assert no fixed
partition, so the OPK switch is value-neutral here.)

This makes the conversion, on its own, a **faithfulness** fix, not a coverage
fix — and with the *current* test values it is purely faithfulness:

- The scatter, `compare_pk_bytes`, and the wide-routing `xxh3` are exercised
  over a byte pattern that cannot occur in production. Converting to OPK makes
  the bytes the ones the engine actually produces (the `xxh3` routing genuinely
  sees different input: LE `(1,1,0)` and OPK `(1,1,0)` are distinct 24-byte
  strings). That is the real and sufficient reason to do this.
- It does **not**, by itself, give the tests teeth against an encoder bug. The
  assertions are self-referential, and every PK value here is `< 256`, so each
  column occupies a single byte and **LE and BE produce the identical memcmp
  order**. A dropped big-endian flip in `encode_order_preserving_pk` would still
  yield a "sorted", self-consistent batch that passes (a) and (b). The
  "wrong per-column offset / missing sign-flip is now visible" reasoning does
  not hold at these values.
- **Sign-flip is out of scope here regardless.** The wide schema is three `U64`
  columns; no sign-flip ever runs. Signed wide-PK coverage belongs at the
  storage layer (see *Out of scope*), not here.
- This is **unlike the narrow sibling.** `make_narrow_compound_batch` (`:1214`)
  was load-bearing to convert: its `mk_compound_pk` packed `(c1 << 64) | c0`, so
  a raw u128 write byte-*reverses the column order*, which its small values
  `(1,10) < (2,0)` already expose. The wide all-unsigned helper has the columns
  in the same order under LE and OPK; only the per-column endianness differs, and
  that is invisible below one byte. "Mirror the narrow sibling exactly" therefore
  understates what the wide case needs to be meaningful (see Change 3's teeth).

## Fix

For a correct encoder every existing assertion holds (self-referential), the row
counts are unchanged, and the input ordering precondition stays valid — OPK byte
order of unsigned columns *is* `(c0,c1,c2)` lexicographic. Mirror the narrow
sibling's encoder use, and add a multi-byte value so the conversion earns teeth.

### Change 1 — `make_wide_batch` OPK-encodes via `extend_pk_opk`

Take native column tuples and encode through `extend_pk_opk`
(`#[cfg(test)] pub`, `crates/gnitz-engine/src/storage/batch.rs:740`), as
`make_narrow_compound_batch` and `make_i64_batch` (`:1305`) already do:

```rust
/// Build a pre-consolidated wide-PK batch from native column tuples. `rows`
/// must already be sorted ascending by (c0,c1,c2) with unique PKs — which is
/// exactly OPK byte order for these unsigned columns, at any value. PKs are
/// OPK-encoded via `extend_pk_opk` (matching `make_narrow_compound_batch`), so
/// the bytes are the ones the ingest path and `partition_for_pk_bytes` see.
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
packs each native value's low `col.size()` little-endian bytes, then calls
`encode_order_preserving_pk` (which iterates `pk_columns()` and runs
`encode_pk_column` — BE + sign-flip — per column) — so the resulting region is
byte-identical to a real ingested wide PK.

### Change 2 — delete `wide_pk`

`wide_pk` (`:1124`-`:1130`) is used only to feed `make_wide_batch`; no assertion
references its literals (its only call sites are the two test bodies' batch
construction). Remove it.

### Change 3 — update call sites to native tuples, with multi-byte teeth

`test_relay_scatter_wide_pk_order_and_routing` (`:1155`-`:1169`). Keep the
low-16-prefix-collision intent — `(1, 1, *)` rows whose leading `BE(1) ++ BE(1)`
prefix collides and differ only in the trailing column — but raise some values
past `255` so OPK byte order is *load-bearing*, not coincidental. `c2 ∈ {1, 256}`
within `b0` and `c0 ∈ {2, 256}` within `b1` are pairs whose native order is the
*reverse* of their raw-LE byte order, so a dropped BE flip makes the source
genuinely unsorted:

```rust
// (1,1,*) prefix-twins span the sources; c2=1 vs 256 (b0) and c0=2 vs 256 (b1)
// are LE/BE order inversions, so OPK encoding is load-bearing for `sorted`.
let b0 = make_wide_batch(&schema, &[
    (0, 0, 0, 1, 10),
    (1, 1, 1, 1, 11),
    (1, 1, 256, 1, 12),   // shares BE(1)++BE(1) prefix with (1,1,1); c2 multi-byte
]);
let b1 = make_wide_batch(&schema, &[
    (1, 1, 257, 1, 20),   // third prefix-twin
    (2, 2, 2, 1, 21),
    (256, 0, 0, 1, 22),   // c0 multi-byte: LE would sort before (2,2,2)
]);
let b2 = make_wide_batch(&schema, &[
    (1, 1, 2, 1, 30),     // fourth prefix-twin
    (3, 3, 3, 1, 31),
    (7, 0, 0, 1, 32),
]);
```

Then assert the precondition the scatter *trusts but does not verify*
(`ConsolidatedBatch::new_unchecked`, `batch.rs:1698`, only debug-asserts the
`sorted` flag, not the bytes) — that each source is genuinely OPK-sorted. Add
this immediately after the three `make_wide_batch` calls, before the scatter:

```rust
// new_unchecked trusts `sorted` without inspecting bytes; with c0/c2 > 255 a
// dropped big-endian flip in the OPK encoder sorts 256 before 1 and trips this,
// so an encoder regression fails here instead of silently scattering scrambled
// bytes past the self-referential routing/ordering checks below.
for (i, src) in [&b0, &b1, &b2].iter().enumerate() {
    for r in 1..src.count {
        assert_ne!(
            compare_pk_bytes(src.get_pk_bytes(r - 1), src.get_pk_bytes(r)),
            std::cmp::Ordering::Greater,
            "source {i} row {r}: make_wide_batch produced non-OPK-sorted bytes",
        );
    }
}
```

`test_relay_scatter_wide_pk_single_source_bulk_drain` (`:1396`-`:1401`) — raise
one value past `255` so the bulk-drain path also routes a realistic wide OPK
(`xxh3` over a 24-byte key with a non-low byte set):

```rust
let b0 = make_wide_batch(&schema, &[
    (1, 2, 3, 1, 1),
    (4, 5, 6, 1, 2),
    (7, 8, 9, 1, 3),
    (256, 11, 12, 1, 4),   // c0 multi-byte
]);
```

The bulk-drain test asserts only routing (no ordering check), so its teeth are
faithfulness of the `xxh3` input; the ordering teeth live in test 1's
precondition assert and assertion (a).

### Invariants preserved

- **`sorted = true` stays valid for a correct encoder.** Under OPK (BE for
  unsigned), byte order of the PK region equals `(c0,c1,c2)` lexicographic *at
  any value*; every source above is in that order, so
  `ConsolidatedBatch::new_unchecked` (which trusts `sorted`) is sound. With the
  multi-byte values this is genuine OPK order rather than a coincidence of
  single-byte values.
- **The low-16-prefix-collision intent holds and is strengthened.**
  `(1,1,1)`, `(1,1,2)`, `(1,1,256)`, `(1,1,257)` share their leading 16 OPK bytes
  (`BE(1) ++ BE(1)`) and differ only in the trailing column. The scatter's wide
  comparator `select_wide` (`:762`) resolves them with the full `compare_pk_bytes`
  walk past the 16-byte prefix (now over a *multi-byte* trailing column), and
  assertion (a) re-checks per-worker monotonicity. (The three sources route the
  twins independently via `xxh3` of the full 24 bytes, so they need not co-locate;
  the prefix-walk is exercised in the merge comparator regardless.)
- **Routing assertions hold unchanged.** Both tests recompute the expected worker
  from `partition_for_pk_bytes(actual_pk)`; the actual bytes change LE→OPK and the
  expected worker changes with them, so the equality still holds. Counts (9 and 4)
  are unchanged.

## Out of scope

- **Signed wide-PK coverage (the sign-flip teeth) lives at the storage layer.**
  At the exchange layer ordering is a sign-agnostic `memcmp` and wide routing is
  `xxh3` over opaque bytes, so a signed scatter test adds little. The canonical
  signed wide-PK OPK coverage is `table_wide_signed_compound_pk_opk_order`, added
  by the companion plan `wide-pk-test-opk-encoding.md` (which also fixes the
  analogous raw-LE helper in `table_wide_pk_has_and_retract_bytes`,
  `table.rs:1483`). `partition_for_pk_bytes`'s own determinism/spread is covered
  by `partition_for_pk_bytes_wide_determinism_and_spread`
  (`partitioned_table.rs:748`).
- **Narrow helpers are already correct.** `make_batch` (`:1030`) /
  `make_batch_str` (`:1046`) use `extend_pk(pk as u128)`, which writes
  right-aligned big-endian = OPK for narrow unsigned PKs;
  `make_narrow_compound_batch` and `make_i64_batch` already use `extend_pk_opk`.
  No change needed.
- **`reduce/tests.rs` and `dag.rs`** build their wide keys big-endian by hand and
  are correct for their all-unsigned PKs — `reduce_trace_seek_wide_pk`
  (`crates/gnitz-engine/src/ops/reduce/tests.rs:272`) and
  `test_enforce_unique_pk_wide_pk` (`crates/gnitz-engine/src/dag.rs:2370`).
  Migrating them to a shared OPK helper is optional cleanup, not part of this fix.
- **The I32 `JoinPromote` block** (`exchange.rs:2074`) builds its PK via
  `encode_pk_column` + `extend_pk_bytes` directly — already correct OPK, just
  spelled at a lower level than `extend_pk_opk`. Collapsing it to
  `extend_pk_opk(&pk_schema, &[(pk as u32) as u128])` is optional uniformity,
  not a fix.

## Testing

- `cargo test -p gnitz-engine test_relay_scatter_wide_pk_order_and_routing
  test_relay_scatter_wide_pk_single_source_bulk_drain`.
- Both expected green: for a correct encoder the OPK-sorted precondition holds,
  routing/ordering are self-referential, and counts are unchanged.
- Regression value: after this change, deleting the big-endian conversion in
  `encode_pk_column` (so the encoder emits raw LE) makes `b0`/`b1` sort `256`
  before `1` and trips the new per-source OPK-sorted assertion — a failure where
  today there is none. (The sign-flip half of that regression is what the
  companion storage test catches.)
- Parity check: after this change every PK region in `exchange.rs` tests is built
  through an order-preserving encoder (`extend_pk` for narrow unsigned,
  `extend_pk_opk` for compound, `encode_pk_column` in the lone `JoinPromote`
  block) — no raw little-endian bytes are written into a PK region anywhere in
  the file.
