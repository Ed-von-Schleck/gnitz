# Direct-encode the IPC ctrl block without allocating a Batch

## Problem

`encode_ctrl_block_ipc` (`runtime/wire.rs:224`) encodes the fixed-format control
WAL block that heads every per-worker SAL slot. It does this by allocating a
one-row `Batch`:

```rust
let cs = CONTROL_SCHEMA_DESC;
let mut b = Batch::with_schema(cs, 1);   // pool acquire + Vec::resize(~200B, 0)
b.extend_pk(0u128);
b.extend_weight(&1i64.to_le_bytes());
// ... 7 more extend_col calls ...
b.encode_to_wire(IPC_CONTROL_TID, out, offset, checksum)
```

`Batch::with_schema` acquires a buffer from the thread-local pool and calls
`Vec::resize(total_size, 0)` — zeroing ~200 bytes every time. With 4 workers per
SAL write and the committer, FK check, tick, and relay paths all going through
here, this happens hundreds of times per second.

`encode_ctrl_block_ipc` is called twice in `wire.rs` and twice in `sal.rs`: once
from `encode_wire_into_impl` (response + relay paths) and once per worker slot in
`scatter_wire_group` (committer + FK check paths). Each invocation allocates and
zeroes a fresh Batch.

The ctrl block has a **completely constant schema** (`CONTROL_SCHEMA_DESC`): 9
columns, all fixed-width (8×U64 + 1×U128 for seek_pk), always 1 row, never a
blob. The full WAL encoding is fully determined at compile time except for the
8 per-call field values. No heap allocation is needed.

In the 2026-04-30 profile this function contributes ~6% of the `memmove` bucket
from the committer scatter path alone, which represents roughly 0.8% of total
CPU. Combined across all call sites and including the pool acquire/release
overhead the impact is higher.

---

## Fix

Replace the `Batch`-based encoding with a direct byte-writing path that encodes
the ctrl WAL block in place into `out[offset..]`.

### What the WAL layout looks like for 1-row fixed-width batch

The V4 wire format for a batch with schema `S`, 1 row, no blob is:

```
WAL header    (WAL_HEADER_SIZE bytes)
directory     (num_regions × 8 bytes)  — (data_offset: u32, data_size: u32) each
[align to 8]
pk region     (pk_stride bytes)        — pk_stride = 8 (U64 PK) or 16 (U128)
[align to 8]
weight region (8 bytes)                — i64 weight
[align to 8]
null_bmp region (8 bytes)             — u64 null bitmap
[align to 8]
col_0 region  (8 bytes)               — PAYLOAD_STATUS       u64
[align to 8]
col_1 region  (8 bytes)               — PAYLOAD_CLIENT_ID    u64
... (7 payload columns total, each 8 bytes except seek_pk which is 16)
blob region   (0 bytes)
```

`CONTROL_SCHEMA_DESC` has pk_index=0 (the PK column is not a payload column),
so there are 8 payload columns total. The ctrl block PK is always 0 (the PK
field carries no semantic value for ctrl blocks).

### Proposed implementation

Add a new function `encode_ctrl_block_direct` that writes bytes without
allocating:

```rust
/// Encode the IPC control block directly into `out[offset..]`.
/// No Batch is allocated. Returns bytes written.
/// All per-call fields are passed explicitly; the schema layout is hardcoded
/// from CONTROL_SCHEMA_DESC (compile-time constant).
pub(crate) fn encode_ctrl_block_direct(
    out: &mut [u8],
    offset: usize,
    target_id: u64,
    client_id: u64,
    wire_flags: u64,
    seek_pk: u128,
    seek_col_idx: u64,
    request_id: u64,
    status: u32,
    error_msg: &[u8],
    checksum: bool,
) -> usize {
    use gnitz_wire::control as ctrl;

    // ctrl block: pk(U64=8B), weight(8B), null_bmp(8B),
    // 7×U64 payload(8B each), seek_pk(U128=16B payload)  => see CONTROL_SCHEMA_DESC
    // Compute the fixed WAL block size once (no heap).
    let written = encode_ctrl_wal_direct(
        out, offset, target_id, client_id, wire_flags,
        seek_pk, seek_col_idx, request_id, status, error_msg, checksum,
    );
    written
}
```

The inner function writes the WAL header + directory + each region with direct
`u8` slice writes and `u64::to_le_bytes()` / `u128::to_le_bytes()` copies. The
directory offsets and region sizes are computable with the same arithmetic as
`schema_wal_block_size` but without allocating.

### Call-site changes

- `encode_wire_into_impl` (`wire.rs:419`): replace `encode_ctrl_block_ipc` call
  with `encode_ctrl_block_direct`. The callers and signature are identical.
- `scatter_wire_group` (`sal.rs:714`): same replacement.

`encode_ctrl_block_ipc` can then be deleted.

---

## Scope

| File | Change |
|------|--------|
| `runtime/wire.rs` | Add `encode_ctrl_block_direct`; delete `encode_ctrl_block_ipc` |
| `runtime/sal.rs` | Update 1 call site |
| `runtime/wire.rs` | Update 1 call site in `encode_wire_into_impl` |

No behaviour change. The encoded bytes are identical — same WAL format, same
field layout — just without the intermediate `Batch` allocation.

---

## What to watch out for

`encode_ctrl_block_ipc` has a `checksum: bool` parameter. In practice it is
always `false` on the IPC path (checksums are only enabled for WAL files written
to disk, not for SAL mmap slots). The direct implementation should preserve the
parameter so callers need no changes, but the `checksum=false` branch can be a
fast-path with no hash computation.

The `error_msg` blob path (non-empty `error_msg`) requires a German-string
encoding in the blob region. This is rare (only error responses) and can be
handled as a fallback to the existing `Batch`-based path or implemented
directly. The common path has `error_msg = b""` and no blob.

---

## Expected impact

Eliminates 4 × (pool acquire + `Vec::resize(~200B, 0)`) per SAL write on the
committer path, and proportionally across all other paths. Removes ~6% of the
memmove bucket attributed to `Batch::with_schema → encode_ctrl_block_ipc →
scatter_wire_group → committer` in the 2026-04-30 profile. All paths that write
SAL groups benefit.
