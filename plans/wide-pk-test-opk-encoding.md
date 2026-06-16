# Wide-PK storage tests must build OPK keys, not raw LE bytes

`table_wide_pk_has_and_retract_bytes` (`crates/gnitz-engine/src/storage/table.rs:1482`)
constructs its 24-byte `(U64, U64, U64)` PK with little-endian column bytes:

```rust
let pk3 = |a: u64, b: u64, c: u64| {
    let mut v = a.to_le_bytes().to_vec();
    v.extend_from_slice(&b.to_le_bytes());
    v.extend_from_slice(&c.to_le_bytes());
    v
};
```

The PK region everywhere in the engine holds the **order-preserving key (OPK,
§6)**: each PK column big-endian, signed columns sign-flipped, packed in
pk-list order. For an all-unsigned PK, OPK is plain big-endian — so this helper
writes the **wrong byte order** (LE) into a region whose contract is BE. The
real ingest/DML path always emits OPK (`encode_order_preserving_pk`); this test
feeds bytes no production row ever carries.

## Why it passes anyway, and what it fails to cover

The test passes for two reasons, both incidental:

1. **The PK region is byte-opaque below the encode boundary.** `has_pk_bytes`,
   `retract_pk_bytes`, and consolidation key on raw bytes via `compare_pk_bytes`
   (a plain `memcmp`, §2). Writing LE and looking up with the same LE bytes is
   self-consistent, so membership and retraction round-trip regardless of byte
   order.
2. **It never asserts ordering or routing.** LE and OPK diverge only where the
   *typed order* matters — merge/compaction order, range/prefix scans, the
   `pack_pk_be` bloom prefix, partition routing. This test checks none of them.

The cost is real coverage, not a live failure:

- The test exercises a byte layout **no production row has**, so it documents a
  false model of wide-PK data. Anyone extending it with an order- or
  routing-dependent assertion would get results that disagree with production
  (LE bytes do not sort like OPK once any column value exceeds one byte).
- It **never calls `encode_order_preserving_pk`**. A bug in wide-compound OPK
  encoding — a wrong per-column offset, or (the load-bearing case) a missing
  sign-flip — is invisible to it. **No** wide-PK test in the tree exercises a
  *signed* PK column: `reduce_trace_seek_wide_pk`
  (`crates/gnitz-engine/src/ops/reduce/tests.rs:272`) and
  `test_enforce_unique_pk_wide_pk` (`crates/gnitz-engine/src/dag.rs:2350`) build
  their wide keys big-endian *by hand* and comment "OPK == BE for unsigned" — so
  the sign-flip on the wide byte path has zero coverage today.

## Fix

Two changes, both confined to the `#[cfg(test)] mod tests` of
`crates/gnitz-engine/src/storage/table.rs`. The principle: build every PK
through the real OPK encoder, so tests model production bytes and a sign-flip /
offset regression in `encode_order_preserving_pk` fails a storage test.

### Shared helper

Add one `#[cfg(test)]` helper to the test module. It mirrors the internals of
`Batch::extend_pk_opk` (`crates/gnitz-engine/src/storage/batch.rs:739`) but
returns the bytes, so the **same** function produces both batch keys and lookup
keys — they are byte-identical by construction:

`columnar` is already in module scope in this file (the non-test `has_pk` calls
`columnar::opk_key`), and the test mod inherits it through `use super::*`; no new
`use` is needed. `encode_order_preserving_pk` is `pub(crate)` in
`storage::columnar`.

```rust
/// OPK-encode native PK column values (one per PK column, in pk-list order)
/// into the canonical order-preserving key — the exact bytes the ingest path
/// produces. Signed columns are passed as `v as u128` (the low `size()` bytes
/// are the two's-complement little-endian image the encoder sign-flips).
fn opk_pk(schema: &SchemaDescriptor, vals: &[u128]) -> Vec<u8> {
    let stride = schema.pk_stride() as usize;
    let mut le = vec![0u8; stride];
    let mut off = 0;
    for ((_ord, _ci, col), &v) in schema.pk_columns().zip(vals) {
        let cs = col.size() as usize;
        le[off..off + cs].copy_from_slice(&v.to_le_bytes()[..cs]);
        off += cs;
    }
    let mut opk = vec![0u8; stride];
    columnar::encode_order_preserving_pk(schema, &le, &mut opk);
    opk
}
```

API facts (verified): `SchemaDescriptor::pk_columns()` is `pub`, yielding
`(usize, usize, &SchemaColumn)` (`schema.rs:370`); `SchemaColumn::size() -> u8`;
`encode_order_preserving_pk(schema, native_le_bytes, out)` is `pub(crate)` in
`storage::columnar` (`columnar.rs:292`) and iterates `pk_columns()` calling
`encode_pk_column` (BE + sign-flip) per column; `compare_pk_bytes` is
`pub(crate)` returning `std::cmp::Ordering` (`columnar.rs:145`).

### Change 1 — fix `table_wide_pk_has_and_retract_bytes`

Replace only the `pk3` closure body; every call site
(`has_pk_bytes(&pk3(..))`, `wide_batch(&[(pk3(..), w, val)])`,
`retract_pk_bytes(&pk3(..))`) and every assertion stays byte-for-byte the same:

```rust
let pk3 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);
```

This is behaviour-preserving (all-unsigned ⇒ OPK is the big-endian image of the
same values; the test only checks membership/retraction, which are byte-opaque),
and the **prefix-twin invariant is preserved**: `(1,1,100)` and `(1,1,200)`
still share their leading 16 OPK bytes (`BE(1) ++ BE(1)`) and differ only in the
trailing column, so the memtable-bloom 16-byte-prefix collision the test was
written to stress (`table.rs:1480` comment) still fires. The win is that the
test now routes through `encode_order_preserving_pk` and carries
production-faithful bytes.

Borrow note: `Table::new(.., schema, ..)` copies `schema` (`SchemaDescriptor:
Copy`), so the `pk3`/`wide_batch` closures may keep borrowing the local `schema`
across the `Table::new` call exactly as they do today.

### Change 2 — add wide *signed* compound-PK coverage (the teeth)

Change 1 alone still wouldn't exercise the sign-flip (unsigned OPK == BE). Add a
new test with a signed leading PK column and negative values, asserting the OPK
typed order end-to-end. This is the case where hand-written BE/LE bytes are
*wrong* and only the real encoder is correct:

```rust
#[test]
fn table_wide_signed_compound_pk_opk_order() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("wide_signed_pk_test");
    // (I64, U64, U64) PK [stride 24, wide] + I64 payload used as an order marker.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    );
    assert_eq!(schema.pk_stride(), 24);
    assert!(schema.pk_is_wide());

    let key = |a: i64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);

    // Direct sign-flip property: a negative leading column sorts before a
    // positive one in OPK byte order. Plain big-endian (no flip) would place
    // 0xFF.. (negatives) after 0x00.. (positives) and fail this.
    assert_eq!(
        columnar::compare_pk_bytes(&key(-1, 0, 0), &key(1, 0, 0)),
        std::cmp::Ordering::Less,
        "OPK must order a negative signed PK column before a positive one",
    );

    let mut t = Table::new(
        tdir.to_str().unwrap(), "test", schema, 4243, 1 << 20, Persistence::Durable,
    ).unwrap();

    // Payload marker == the signed leading value, so scan order is read back
    // without decoding the PK.
    let row = |a: i64| {
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk_bytes(&key(a, 0, 0));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &a.to_le_bytes());
        b.count += 1;
        b
    };
    for a in [3i64, -5, 0, -1] {                  // scrambled insertion order
        t.ingest_owned_batch(row(a)).unwrap();
    }
    t.flush().unwrap();                           // Durable: one on-disk shard

    // Membership at signed width, byte-keyed (both must survive the flush).
    assert!(t.has_pk_bytes(&key(-5, 0, 0)));
    assert!(t.has_pk_bytes(&key(3, 0, 0)));
    assert!(!t.has_pk_bytes(&key(-2, 0, 0)), "absent signed key must not be found");

    // full_scan returns rows in OPK (= typed signed) order: -5, -1, 0, 3.
    // A missing sign-flip would scan back as 0, 3, -5, -1 and fail here.
    let scanned = t.full_scan();
    let payloads: Vec<i64> = (0..scanned.count)
        .map(|r| i64::from_le_bytes(scanned.get_col_ptr(r, 0, 8).try_into().unwrap()))
        .collect();
    assert_eq!(
        payloads, vec![-5, -1, 0, 3],
        "wide signed compound PK must scan back in typed (sign-flipped) order",
    );

    // Retraction keys on the full OPK bytes at signed width.
    let (w, found) = t.retract_pk_bytes(&key(-5, 0, 0));
    assert_eq!((w, found), (1, true));
    assert!(!t.has_pk_bytes(&key(-5, 0, 0)), "retracted signed key is gone");

    t.close();
}
```

`full_scan()` returns `Rc<Batch>`; `count` / `get_col_ptr` reach
through `Deref`. `get_col_ptr(r, 0, 8)` reads payload index 0 (the lone payload
column), 8 bytes, as the `i64` marker — no PK decode needed, so the order
assertion is non-circular: storage sorts by raw OPK bytes, and the markers
reveal whether that order equals the intended signed order.

## Out of scope

- **`exchange.rs` carries the identical smell** — `wide_pk`
  (`crates/gnitz-engine/src/ops/exchange.rs:1090`) builds 24-byte keys with
  `to_le_bytes`, and `make_wide_batch` (`:1100`) requires rows "sorted ascending
  by (c0,c1,c2)" — true for its single-byte test values under either layout, but
  not in general. Unlike the `table.rs` test, the exchange tests are
  **routing/order-sensitive**: partition assignment hashes the OPK bytes (or the
  distribution prefix), so switching `wide_pk` to OPK changes the hash inputs and
  would force re-deriving any partition expectations these tests assert. That is
  a distinct, higher-risk change and belongs in its own plan, not folded in here.
- **`reduce/tests.rs` and `dag.rs` are already correct** (big-endian by hand,
  for all-unsigned PKs). Migrating them to `opk_pk` for consistency is optional
  cleanup, not a fix; it is not required and is not part of this plan.
- **PK-column value decoding.** Change 2 reads order off a payload marker, not by
  decoding PK columns; full PK-decode coverage (`decode_pk_column`) is a separate
  concern.

## Testing

- `cargo test -p gnitz-engine table_wide_pk_has_and_retract_bytes` and
  `cargo test -p gnitz-engine table_wide_signed_compound_pk_opk_order`.
- Both are expected green: Change 1 is behaviour-preserving, and Change 2's
  assertions hold for a correct `encode_order_preserving_pk`.
- Regression value: with Change 2 in place, deleting or corrupting the
  signed-column sign-flip in `encode_pk_column` flips the
  `compare_pk_bytes(key(-1,..), key(1,..))` assertion and reorders the scanned
  markers to `[0, 3, -5, -1]` — a storage-test failure where today there is
  none.
