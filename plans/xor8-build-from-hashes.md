# XOR8 build: drop the transient `Vec<u128>`

## Goal

Eliminate the per-shard transient `Vec<u128>` allocated on the
filter-build path. Today every shard write materialises one `u128` per
row (16 bytes/row) purely to hand it to `xor8::build`, which immediately
re-maps it to a `Vec<u64>` (8 bytes/row) and drops the `u128` buffer.
For a shard with millions of rows this is a 16 bytes/row transient that
exists only to be consumed one line later.

This is a PK-width-independent micro-optimisation of the filter-build
primitive. It does not change the filter algorithm, the on-disk filter
bytes, the false-positive rate, or any probe path. The produced filter
is byte-for-byte identical for every shard.

## Background that this change relies on

These already exist in the tree; this plan consumes them:

- `xor8::build(pks: &[u128]) -> Option<Xor8>`
  (`crates/gnitz-engine/src/storage/xor8.rs`) — maps each `u128` through
  `xxh::hash_u128`, sorts, dedups, and builds the filter.
- `crate::xxh::hash_u128(pk: u128) -> u64` (`crates/gnitz-engine/src/xxh.rs`)
  — the exact `u128 -> u64` reduction `xor8::build` applies internally.
- `build_xor8_from_pk_region(pk_ptr, pk_sz, n)`
  (`crates/gnitz-engine/src/storage/shard_file.rs`) is the **sole**
  caller of `xor8::build` outside `xor8`'s own tests. It is called from
  both shard-write sites (`build_shard_image` and
  `write_shard_streaming_inner`); fixing the helper covers both.

## Current state (what this plan changes)

`crates/gnitz-engine/src/storage/xor8.rs`:

```rust
pub fn build(pks: &[u128]) -> Option<Xor8> {
    if pks.is_empty() {
        return None;
    }
    let mut keys: Vec<u64> = pks.iter().map(|&k| xxh::hash_u128(k)).collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return None;
    }
    Some(Xor8::from(keys.as_slice()))
}
```

`crates/gnitz-engine/src/storage/shard_file.rs`,
`build_xor8_from_pk_region`, builds a `Vec<u128>` over the PK region and
passes `&pks` to `xor8::build`. The `Vec<u128>` is never used for
anything except that single `xor8::build` call, after which
`xor8::build` allocates a second `Vec<u64>` from it.

## The change

### 1. Split `xor8::build` into a hash-owning core + a thin wrapper

Add a public `build_from_hashes` that takes **ownership** of the
already-hashed `u64` keys, and reduce `build` to a wrapper so every
existing caller and test (which pass `&[u128]`) is unaffected:

```rust
/// Build an Xor8 filter from already-hashed u64 keys. Takes ownership,
/// sorts and dedups in place. Returns None if the (post-dedup) set is
/// empty. The caller is responsible for having applied `xxh::hash_u128`
/// (or an equivalent reduction) to produce `keys`.
pub fn build_from_hashes(mut keys: Vec<u64>) -> Option<Xor8> {
    if keys.is_empty() {
        return None;
    }
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return None;
    }
    Some(Xor8::from(keys.as_slice()))
}

pub fn build(pks: &[u128]) -> Option<Xor8> {
    build_from_hashes(pks.iter().map(|&k| xxh::hash_u128(k)).collect())
}
```

`build`'s observable behaviour is unchanged: same empty-input handling,
same `hash_u128` mapping, same sort/dedup, same filter.

### 2. Hash directly at the single call site

In `build_xor8_from_pk_region`, replace the `Vec<u128>` collection +
`xor8::build(&pks)` with a `Vec<u64>` collection that applies
`xxh::hash_u128` to each row's `u128` key as it is produced, then call
`xor8::build_from_hashes`:

```rust
let keys: Vec<u64> = /* per-row u128 key, exactly as today */
    .map(|k| crate::xxh::hash_u128(k))
    .collect();
xor8::build_from_hashes(keys)
```

The per-row `u128` is still computed, but only as a stack temporary that
is immediately reduced to `u64`; no `Vec<u128>` is materialised. This
transformation is mechanical and orthogonal to **how** each row's `u128`
key is derived — only the container and the point at which
`xxh::hash_u128` is applied change.

## Why this is safe

- `xxh::hash_u128` is exactly the reduction `xor8::build` applies
  internally. Applying it at the call site instead of inside `build`
  yields the identical `u64` multiset, hence a byte-for-byte identical
  filter for every shard (existing and future).
- `build` remains a wrapper with identical signature and semantics, so
  all other callers and all `xor8` unit tests are unaffected.
- Empty / all-duplicate inputs still return `None`: the post-dedup
  empty check moves into `build_from_hashes` and is reached on the same
  inputs (an empty `Vec<u64>` short-circuits before sort, and a
  fully-deduped-to-empty set is impossible since dedup never empties a
  non-empty vec — the explicit second check is retained defensively,
  matching today).
- No probe path, serialization format, or filter parameter changes;
  the false-positive rate is identical.

## Testing

- `xor8.rs` unit tests: add a test that `build_from_hashes` and `build`
  produce filters with identical `seed`, `block_length`, and
  `fingerprints` for the same logical key set (`build(&pks)` vs
  `build_from_hashes(pks.map(hash_u128).collect())`), pinning the
  wrapper equivalence.
- `xor8.rs`: `build_from_hashes(vec![])` returns `None`; a
  hashes-with-duplicates input dedups and still probes positive for
  every distinct key (no false negatives).
- The existing `xor8` build/probe/roundtrip tests and the
  `shard_file.rs` `xor8_may_contain` shard tests are unchanged and must
  stay green — they assert the filter behaviour that this change
  preserves bit-for-bit.

`make test` stays green: the filter is byte-for-byte identical, so every
shard-level assertion holds without modification.

## Out of scope

- The per-row PK-key derivation in `build_xor8_from_pk_region` (whatever
  stride/width logic produces each row's `u128`): unchanged; this plan
  only moves where `xxh::hash_u128` is applied and which container
  collects the result.
- The XOR8 probe path (`may_contain`), serialization, and on-disk
  format.
- Any change to the filter algorithm, seed, block length, or
  false-positive rate.
