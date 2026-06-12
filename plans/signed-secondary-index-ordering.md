# Order-preserving signed encoding for secondary-index keys

## 1. Problem

A secondary index's PK is `(promoted indexed-key…, src_pk…)` and is documented as
order-preserving big-endian (OPK) at rest. For **signed** indexed columns this is
false: the index leading-key region is not order-preserving, so a range or ordered
scan over a signed indexed column would return numerically mis-ordered results
(negatives sort *after* non-negatives).

Two facts combine:

- `get_index_key_type` (`catalog/utils.rs:134`) promotes every signed integer
  (`I64`/`I32`/`I16`/`I8`) to **`U64`** — an *unsigned* type code.
- The leading-key encoder calls `encode_pk_column(bytes, idx_key_type, dst)`
  (`dag.rs:1874` projection, `schema.rs:818` `index_opk_prefix`). `encode_pk_column`
  (`gnitz-wire/src/pk.rs:24`) only flips the sign bit when the *type code* is
  signed (`is_signed_int`). With the promoted code `U64`, **no sign-flip happens.**

So `-5: I32` is read as the zero-extended canonical key `0x0000_0000_FFFF_FFFB`
(`payload_native_key`/`pk_native_key` zero-extend, `schema.rs:781`/`:760`), encoded
as `U64` big-endian to `00 00 00 00 FF FF FF FB`, while `+5` encodes to
`00 00 00 00 00 00 00 05`. `-5` sorts **after** `+5`.

**This is latent.** Every current consumer of a secondary index does
equality / leading-prefix-**equality** seeks only (point-lookup `SELECT`,
FK existence, single-column uniqueness). Equality is *self-consistent and correct*:
projection and seek both zero-extend and encode with the same promoted `U64`, so a
given value's stored bytes equal its seek bytes regardless of sign. Nothing
range-scans or ordered-walks a secondary index, so the mis-ordering has no current
trigger. This plan fixes the encoding so a future range/ordered scan is correct and
the OPK-at-rest invariant holds for all types. It is **orthogonal to index arity**
and independent of the composite-index plans.

## 2. Why the obvious one-line fix is wrong

Changing only `get_index_key_type` to return `I64` for signed columns (so
`encode_pk_column` sign-flips) **corrupts equality**, because the canonical native
key is *zero-extended*, not sign-extended: `-5: I32` arrives as
`0x0000_0000_FFFF_FFFB`, which at 8-byte width has a **clear** sign bit (it looks
positive). `encode_pk_column(…, I64, …)` would flip the *wrong* bit and produce a
key unequal to a correctly sign-extended `-5: I64`. Write and seek would also
disagree if only some encoders changed.

The correct transform must **sign-extend from the source width** before encoding.
`gnitz-wire` already has the primitive: `encode_pk_column_promoted(src, src_tc,
target_tc, dst)` (`pk.rs:87`) sign-extends a signed source, zero-extends an
unsigned source, then OPK-encodes at the target type. It is the same primitive the
cross-width join reindex path uses, and is extensively tested (`pk.rs` co-partition
ladders). When `src_tc == target_tc` it is exactly `encode_pk_column`.

The fix is therefore two coordinated changes, applied **atomically** across every
leading-key encoder (a partial change makes write and seek bytes disagree and
breaks equality lookups):

1. `get_index_key_type`: promote signed `≤8B` to `I64` (width 8, unchanged from
   `U64`, so no schema-size / `MAX_PK_BYTES` impact); `U128`/`UUID`→16B and unsigned
   unchanged.
2. Every leading-key encoder: replace `encode_pk_column(native[..target_w],
   target_tc, dst)` with `encode_pk_column_promoted(native[..source_w], source_tc,
   target_tc, dst)`, threading the **source** column type/width (the value is
   zero-extended in `native_key`; the promoted encoder re-derives the sign from the
   low `source_w` bytes).

The index leading-key region is **never decoded back** — seeks match it as a `memcmp`
prefix and reconstruct the source PK from the verbatim OPK *suffix* — so this is an
encode-only change.

## 3. Sites to change (all leading-key encoders, atomically)

| Site | Location | Change |
|---|---|---|
| Promotion | `catalog/utils.rs:134` `get_index_key_type` | signed `≤8B` → `I64` |
| Projection (write) | `dag.rs:1874` `batch_project_index` | `encode_pk_column_promoted(&native.to_le_bytes()[..loc.size()], loc.type_code(), target_type, dst)` per column |
| Single-col seek prefix | `schema.rs:812` `index_opk_prefix` | add `src_type` param; encode via `_promoted` |
| Composite seek prefix | `schema.rs` `index_opk_prefix_composite` (composite-secondary-indexes.md §6.5) | per-column `src_type`; encode via `_promoted` |
| In-batch unique check | `catalog/validation.rs:313` | pass `loc.type_code()` into `index_opk_prefix` |
| Distributed check batch | `master.rs:3077` `build_check_batch` | pass the source type into the prefix encoder |
| Worker preflight key | `runtime/worker.rs` `handle_unique_preflight` (and `index_key_bytes`, composite-unique-secondary-indexes.md §4) | encode via `_promoted` with the source type |

`seek_by_index` (`store.rs:924`) already slices the client's native value to the
source width and zero-extends; it routes through `index_opk_prefix*`, so it inherits
the fix once the prefix helpers thread the source type. Every site has the source
schema/locator in scope, so the source type is available without new plumbing.

The **routing** cache key (`route_key`/`index_route_key`, `master.rs:3115`) is a
separate comparison space — routing keys are only ever compared to other routing
keys to pick a worker, never to index PK bytes — so it needs no change for
correctness; it already maps equal values to equal keys consistently.

### Helper signature

```rust
pub(crate) fn index_opk_prefix(
    native: u128, src_type: u8, idx_key_type: u8, idx_key_size: usize,
) -> [u8; MAX_PK_BYTES] {
    let mut opk = [0u8; MAX_PK_BYTES];
    let src_w = gnitz_wire::wire_stride(src_type);
    gnitz_wire::encode_pk_column_promoted(
        &native.to_le_bytes()[..src_w], src_type, idx_key_type, &mut opk[..idx_key_size]);
    opk
}
```

Projection inner encode:

```rust
let src_w = loc.size();                 // source column width
gnitz_wire::encode_pk_column_promoted(
    &native.to_le_bytes()[..src_w], loc.type_code(), target_types[i],
    &mut idx_pk_buf[off..off + target_sizes[i]]);
```

## 4. Testing

- **Ordering**: project rows with a signed indexed column over `[i32::MIN, -3, -1,
  0, 1, i32::MAX]`; assert the index walks them in numeric order (negatives before
  non-negatives) — fails before, passes after.
- **Equality regression**: a `WHERE col = -5` seek still returns the right rows
  (write/seek byte-consistency preserved); round-trip an exhaustive signed/unsigned/
  U128 mix through projection + seek.
- **Mixed composite**: an index on `(signed, unsigned)` orders correctly on the
  leading signed column and breaks ties on the unsigned column.
- **Width ladder**: `I8`/`I16`/`I32`/`I64` columns each promote to `I64` and order
  correctly (the `encode_pk_column_promoted` sign-extension at every source width).

## 5. Notes

- Pre-production: the index store schema's leading-column type code changing from
  `U64` to `I64` for signed columns is a clean break with no data to migrate.
- Apply all of §3 in one change. A partial application (e.g. projection updated,
  `index_opk_prefix` not) makes stored and sought bytes disagree and breaks equality
  lookups — strictly worse than the current latent mis-ordering.
