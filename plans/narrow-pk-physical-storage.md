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

### Phase 1 — Schema-driven Batch stride ✓ DONE (`be5726c`)

Engine Batch/MemBatch/DirectWriter store `pk_stride` bytes per PK.
Throwaway shims kept wire (V3) and shard (V5) formats unchanged.

**Lesson:** Hidden `* 16` PK assumptions outside `storage/`. After each phase grep all crates.
Fixed in Phase 1: `expr/batch.rs` LOAD_PK_INT handler; `expr/plan.rs` reindex stride mismatch.

### Phase 2 — Shard file V6 ✓ DONE (`3650218`)

`SHARD_VERSION` 5→6. Removed Phase 1 shims in `shard_file.rs`/`shard_reader.rs`.
XOR8 build uses `stride = pk_sz / n` with zero-extension. `MappedShard` gained `pk_stride: u8`.
`get_pk`, `col_ptr_by_logical`, `slice_to_owned_batch` now use dynamic stride.

**Cascade:** `shard_index.rs` test helper and `runtime/wire.rs` `schema_wal_block_size` both
hardcoded 16B/row PK — fixed.

### Phase 3 — WAL block V4 ✓ DONE (`3650218`)

`WAL_FORMAT_VERSION` 3→4. Removed Phase 1 shims in `batch.rs`.
`wal_block.rs`: added `append_pk_region`; encode/decode derive `pk_stride` from schema.

**Note:** `runtime/wire.rs::schema_wal_block_size` is the companion sizing function for
`encode_to_wire`. It must stay in sync with `wire_byte_size`/`encode_to_wire` — same pk_stride.

### Phase 4 — Wire ZSetBatch → PkColumn + binding narrowing

**`gnitz-core/src/protocol/types.rs`:**
```rust
pub enum PkColumn { U64s(Vec<u64>), U128s(Vec<u128>) }
pub struct ZSetBatch { pub pks: PkColumn, ... }
```
`BatchAppender::add_row(pk: u128)` stays; dispatches on variant.

**Downstream:** `wal_block.rs`, `codec.rs`, `message.rs`, `dml.rs`, `gnitz-py`, `gnitz-capi`.
Tests: BatchAppender round-trip for both PK types. Python `pks()`. C API `get_pk_lo/hi`.

### Phase 5 — Multi-worker hash invariance + narrow-PK E2E

- `xxh.rs` test: `hash_u128(42u128) == hash_u128(42u64 as u128)`.
- `exchange.rs` test: same partition via PK-column and join-column routes.
- `tests/test_joins.py`: W=4 join with U64 PKs spanning 2^32.
- `tests/test_workers.py`: 10k rows U64 PKs over `[0, 2^40]`; verify partition balance.
- `tests/test_fk.py`: U64 PK parent + child, W=4.

### Phase 6 — Documentation cleanup

- `foundations.md` §1: physical stride narrowing, zero-extension invariant.
- `foundations.md` §6: update region convention table with `pk_stride`.
- `dev-guide.md`: narrow-PK hash invariance; stride is schema-dependent for IPC fast paths.
- `plans/unify-u128-primary-keys.md` Appendix open question #4: mark resolved.

---

## Version bumps

| Format | Before | After | Status |
|---|---|---|---|
| `SHARD_VERSION` | 5 | 6 | ✓ done |
| `WAL_FORMAT_VERSION` | 3 | 4 | ✓ done |
| Manifest | 3 | 3 | unchanged |
| Wire `ZSetBatch.pks` | `Vec<u128>` | `PkColumn` | Phase 4 |

Wipe data directory between PRs (format bumps reject old files).

## Invariants

- Sort order: `get_pk -> u128` zero-extends narrow types; sort key preserved.
- XOR8: write and probe both zero-extend to u128 before hashing.
- Partition routing: `hash_u128(zero_extended(v))` identical on PK and payload-column routes.
- Retraction path: WAL retraction tests required for each phase touching WAL encoding.
