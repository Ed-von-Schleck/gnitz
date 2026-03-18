# IPC Unification: One Format to Rule Them All

## Problem

The IPC layer used a bespoke ZSet wire encoding inconsistent with everything else in gnitz.
WAL blocks and shard files transmit `ArenaZSetBatch` in its native SoA columnar form.
IPC reinvented a different string encoding (`[u32 global_blob_offset | u32 length]` per row)
requiring a two-pass conversion and causing multiple correctness bugs.

The complete fix: **IPC is WAL block streaming**. An IPC message is 1–3 WAL blocks in a
memfd: a control block, an optional schema block, and an optional data block. The control
block is a single-row batch using `CONTROL_SCHEMA` — the same RowBuilder / WAL codec path
as everything else. There is no envelope, no bespoke integer writes, no unchecked metadata.
Every byte is covered by a WAL block XXHash64 checksum.

## Goal

- IPC messages are sequences of WAL blocks. `encode_batch_to_buffer` /
  `decode_batch_from_ptr` are the universal codec. Nothing else exists.
- The entire bespoke encoding layer is deleted.
- The 96-byte envelope is deleted. `CONTROL_SCHEMA` replaces it.
- `IPCPayload.owned_bufs` is gone — no scratch buffers, no two-step free.
- All external API signatures (`send_batch`, `receive_payload`, `try_receive_payload`,
  `schema_to_batch`, `batch_to_schema`) are unchanged.
- `rust_client` will need to be updated to the new format (tracked separately).

## Non-Goals

- Shard file format unification.
- WAL directory entry u32→u64 upgrade.
- Zero-copy send path.
- `py_client/` update (being removed).

## Wire Format

An IPC message is 1–3 WAL blocks concatenated in a memfd:

```
[WAL block: control batch,  TID=IPC_CONTROL_TID, count=1]   ← always present
[WAL block: schema batch,   TID=target_id,        count=N]   ← if FLAG_HAS_SCHEMA
[WAL block: data batch,     TID=target_id,        count=M]   ← if FLAG_HAS_DATA
```

```
IPC_CONTROL_TID = 0xFFFFFFFF   # max uint32; get_table_id() returns 4294967295 (not -1)
FLAG_HAS_SCHEMA = r_uint64(1 << 48)
FLAG_HAS_DATA   = r_uint64(1 << 49)
```

`CONTROL_SCHEMA`: 9 columns — msg_idx (PK, U64), status, client_id, target_id, flags,
seek_pk_lo, seek_pk_hi, seek_col_idx (all U64), error_msg (STRING, nullable). `target_id`
travels in the control batch so allocation responses (no schema/data block) can return it.

## ~~Steps 1–4 — Python server~~ DONE

- `wal_columnar.py`: extracted `_parse_wal_block` / `decode_batch_from_ptr`.
- `ipc.py`: deleted bespoke encoding; rewrote as 3-block WAL-block format with `CONTROL_SCHEMA`, `_encode/decode_control_batch`, `serialize_to_memfd`, `_recv_and_parse`.
- All Python tests pass (`ipc_comprehensive_test`, `master_worker_test`, `server_test`).

## ~~Step 5a — `gnitz-protocol` wire format port~~ DONE (6904bae)

- `wal_block.rs` (new): `encode_wal_block`/`decode_wal_block`, German String helpers,
  XXH3-64 checksums, `recompute_block_checksum`, `get_region_offset_size`.
- `header.rs`: dropped `MAGIC_V2`/`HEADER_SIZE`/`pack`/`unpack`; kept simplified `Header`
  struct and all `FLAG_*`/`STATUS_*`/`META_FLAG_*` constants; added `FLAG_HAS_SCHEMA/DATA`.
- `codec.rs`: dropped `Layout`/`encode_zset`/`decode_zset`; kept `schema_to_batch`/`batch_to_schema`.
- `message.rs`: rewrote `send_message`/`recv_message` for 3-block format; added
  `encode_control_block`/`decode_control_block`, `CONTROL_SCHEMA` singleton.
- `lib.rs`: updated exports.
- `ipc_failures.rs`: rewritten for WAL-block format.
- All 27 `gnitz-protocol` unit tests pass; `gnitz-core` builds cleanly.

## Step 5b — Design cleanup (open)

Wire protocol is correct but `gnitz-core` still accesses fields via the legacy `Header` wrapper.
These are cosmetic but needed for full `ipc-unification` completion:

| Item | Location | Change needed |
|---|---|---|
| `TypeCode::String.wire_stride()` returns 8, should be 16 | `gnitz-protocol/src/types.rs` | Fix return value for `TypeCode::String` |
| `Message` struct nests `header: Header` | `gnitz-protocol/src/message.rs` | Flatten to top-level fields (see "New `Message` struct" below) |
| `send_message` takes `Header` param | `gnitz-protocol/src/message.rs` | Rewrite to positional args |
| `msg.header.status` / `msg.header.target_id` / `msg.header.client_id` | `gnitz-core/src/connection.rs`, `gnitz-core/src/ops.rs` | Update to `msg.status` / `msg.target_id` / `msg.client_id` after Message is flattened |

E2E tests (`GNITZ_WORKERS=4`) require the wire format to be correct (done). The cleanup
items above do not block E2E but are needed for the codebase to match this plan fully.

---

### Verification

```
cd rust_client && cargo test -p gnitz-protocol   # 27 unit tests pass
cd rust_client && cargo test                      # all crates
GNITZ_WORKERS=4 make e2e                          # E2E (requires Step 5b)
```

## Future Work

See `plans/wal-block-improvements.md`.
