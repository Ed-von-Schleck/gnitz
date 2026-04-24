# Narrow Primary Key Physical Storage to its Logical Size

U64 PK tables store 8 bytes/row everywhere; U128 keeps 16. Unified `u128` API surface unchanged.

## Goals

1. U64 PK: **8 bytes/row** in Batch, shard file, WAL block, wire ZSetBatch.
2. U128 PK: 16 bytes/row (no change).
3. Engine API (`get_pk`, `extend_pk`, `find_lower_bound`, `seek`) unchanged.
4. XOR8 filter keyed on zero-extended u128.

## Non-goals

- Narrower types (U8/U16/U32). Stride set is exactly {8, 16}.
- Manifest V3 bump. `pk_min/max/guard_key` stay u128 pairs.
- IPC SEEK bump. `seek_pk` stays U128; worker narrows at comparison time.

---

## Design

```
Region 0 (REG_PK):       stride = pk_stride  (8 for U64, 16 for U128)
Region 1 (REG_WEIGHT):   stride = 8
Region 2 (REG_NULL_BMP): stride = 8
Region 3..:              payload strides (unchanged)
```

`pk_stride(schema) = schema.columns[pk_index].size` — no new schema fields.

---

## Phase plan

### Phase 1 ✓ DONE (`be5726c`) — Schema-driven Batch stride
### Phase 2 ✓ DONE (`3650218`) — Shard V6
### Phase 3 ✓ DONE (`3650218`) — WAL V4
### Phase 4 ✓ DONE (`5bd9dfe`) — Wire ZSetBatch → PkColumn

`ZSetBatch.pks: Vec<u128>` replaced with `PkColumn { U64s(Vec<u64>), U128s(Vec<u128>) }`.
`BatchAppender::add_row(pk: u128)` unchanged externally; dispatches on variant.
Downstream: `wal_block.rs`, `codec.rs`, `message.rs`, `client.rs`, `dml.rs`, `gnitz-py`, `gnitz-capi`, all tests.

### Phase 5 ✓ DONE (`b23959b`) — Hash invariance + narrow-PK E2E tests

- `xxh.rs`: `hash_u128_zero_extension_invariant`
- `exchange.rs`: `test_partition_routing_invariance_narrow_pk`
- `test_joins.py`: `test_inner_join_wide_u64_pks_multiworker` (W≥2, PKs to 2^40)
- `test_workers.py`: `test_partition_balance_wide_u64_range` (96 rows, three U64 regions)
- `test_fk.py`: not yet added (low priority; FK tests already cover W=4)

### Phase 6 ✓ DONE — Documentation cleanup

- `foundations.md` §1: physical stride narrowing, zero-extension invariant.
- `foundations.md` §6: update region convention table with `pk_stride`.
- `dev-guide.md`: narrow-PK hash invariance; stride is schema-dependent for IPC fast paths.

---

## Version bumps

| Format | Before | After | Status |
|---|---|---|---|
| `SHARD_VERSION` | 5 | 6 | ✓ done |
| `WAL_FORMAT_VERSION` | 3 | 4 | ✓ done |
| Manifest | 3 | 3 | unchanged |
| Wire `ZSetBatch.pks` | `Vec<u128>` | `PkColumn` | ✓ done |

Wipe data directory between PRs (format bumps reject old files).

## Invariants

- Sort order: `get_pk -> u128` zero-extends narrow types; sort key preserved.
- XOR8: write and probe both zero-extend to u128 before hashing.
- Partition routing: `hash_u128(zero_extended(v))` identical on PK and payload-column routes.
- Retraction path: WAL retraction tests required for each phase touching WAL encoding.
