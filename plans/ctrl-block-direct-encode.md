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

`encode_ctrl_block_ipc` has 2 call sites: `wire.rs:419` (inside
`encode_wire_into_impl`) and `sal.rs:714` (`scatter_wire_group`).

The ctrl block schema (`CONTROL_SCHEMA_DESC`) is compile-time constant:
**7×U64 + 1×U128 + 1×nullable STRING**, 9 columns, always 1 row. The nullable
STRING column (`error_msg`, col 8) writes a 16-byte German string inline header
and may write a blob region when `error_msg` is non-empty. For the common path
(`error_msg = b""`), the blob region is 0 bytes and the entire WAL block layout
is a compile-time constant.

In the 2026-04-30 profile this function contributes ~6% of the `memmove` bucket
from the committer scatter path alone, roughly 0.8% of total CPU.

---

## Fix

Replace the `Batch`-based encoding with a direct byte-writing path for the
**no-error fast path** (`error_msg.is_empty()`). The rare error path falls back
to the existing `Batch`-based implementation.

### Schema layout

`CONTROL_SCHEMA_DESC`: `num_columns=9, pk_index=0`

| col | name | type | bytes |
|-----|------|------|-------|
| 0 | msg_idx (PK) | U64 | 8 |
| 1 | status | U64 | 8 |
| 2 | client_id | U64 | 8 |
| 3 | target_id | U64 | 8 |
| 4 | flags | U64 | 8 |
| 5 | seek_pk | U128 | 16 |
| 6 | seek_col_idx | U64 | 8 |
| 7 | request_id | U64 | 8 |
| 8 | error_msg | STRING nullable | 16 inline + optional blob |

### WAL layout for the no-error path (blob_size=0)

`schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, 0)` = 248 bytes.
Using `wal_block_size` with `num_regions=12` (pk + weight + null_bmp + 8 payload
+ blob):

```
WAL header    (48 bytes, offset 0)
directory     (12 × 8 = 96 bytes, offset 48)
pk region     (8 bytes,  offset 144) — PK always 0
weight region (8 bytes,  offset 152) — i64 = 1
null_bmp      (8 bytes,  offset 160) — ctrl::NULL_BIT_ERROR_MSG (error_msg is null)
col 1: status        (8 bytes,  offset 168)
col 2: client_id     (8 bytes,  offset 176)
col 3: target_id     (8 bytes,  offset 184)
col 4: flags         (8 bytes,  offset 192)
col 5: seek_pk       (16 bytes, offset 200)
col 6: seek_col_idx  (8 bytes,  offset 216)
col 7: request_id    (8 bytes,  offset 224)
col 8: error_msg     (16 bytes, offset 232) — all-zero null German string header
blob region          (0 bytes,  offset 248)
```

`CTRL_BLOCK_SIZE_NO_BLOB = 248` is a compile-time constant derived by walking
the layout above (all region sizes are constant; `align8` arithmetic is
deterministic). `wal_block_size` and `schema_wal_block_size` are not `const fn`,
so this value is hard-coded as a named const and verified against
`schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, 0)` in a `#[test]`.

**Null bitmap:** `null_bmp = ctrl::NULL_BIT_ERROR_MSG` when `error_msg.is_empty()`
(col 8 is null). When `error_msg` is non-empty, `null_bmp = 0` (col 8 is
non-null). This matches the existing implementation exactly; getting it wrong
causes `peek_control_block` to misread the error_msg field.

### Implementation approach: static template

All directory offsets, region offsets, and every WAL header field that does not
depend on call arguments are compile-time constants. Pre-build a
`static CTRL_BLOCK_TEMPLATE: [u8; CTRL_BLOCK_SIZE_NO_BLOB]` with those bytes
pre-filled. The template encodes: WAL header (including the constant WAL size
field), directory, pk=0, weight=1, null_bmp=`NULL_BIT_ERROR_MSG`, and the
all-zero 16-byte German string header for error_msg. Each call:

1. `out[offset..][..CTRL_BLOCK_SIZE_NO_BLOB].copy_from_slice(&CTRL_BLOCK_TEMPLATE)`
2. Patch the **7 variable fields** at their known offsets (status at 168, client_id
   at 176, target_id at 184, wire_flags at 192, seek_pk at 200, seek_col_idx at 216,
   request_id at 224).
3. If `checksum=true`, compute and write the WAL block checksum over the written bytes.

The WAL size field is constant (`CTRL_BLOCK_SIZE_NO_BLOB = 248`) and is baked into
the template; it does not need to be patched per call.

This eliminates all per-call offset arithmetic. The only work per call is 7 store
operations into known byte offsets plus the optional checksum.

`CTRL_BLOCK_SIZE_NO_BLOB = 248` must be confirmed equal to
`schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, 0)` in a `#[test]`
(not a `const assert` — `schema_wal_block_size` is not `const fn`).

### Checksum handling

`encode_wire_into` (line 354) passes `checksum=true` to `encode_wire_into_impl`.
`encode_wire_into_ipc` (line 365) passes `false`. Both call `encode_ctrl_block_ipc`
through `encode_wire_into_impl`. The direct encoder must correctly implement both
branches — `checksum=false` skips the hash, `checksum=true` computes and writes it.

### Error path fallback

When `!error_msg.is_empty()`, fall back to the existing `Batch`-based path inside
the same function rather than branching to a separate function. This keeps the
call sites unchanged and avoids any observable difference in encoded bytes.
`encode_ctrl_block_ipc` is kept as the fallback and is not deleted.

### Proposed implementation

```rust
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
    if !error_msg.is_empty() {
        // Rare error path: fall back to the Batch-based encoder.
        return encode_ctrl_block_ipc(
            out, offset, target_id, client_id, wire_flags,
            seek_pk, seek_col_idx, request_id, status, error_msg, checksum,
        );
    }
    // Fast path: direct write via static template.
    // ... patch template copy, then patch variable fields at known offsets ...
    CTRL_BLOCK_SIZE_NO_BLOB
}
```

---

## Scope

| File | Change |
|------|--------|
| `runtime/wire.rs` | Add `encode_ctrl_block_direct` (with static template); keep `encode_ctrl_block_ipc` |
| `runtime/wire.rs` | Replace `encode_ctrl_block_ipc` call at line 419 with `encode_ctrl_block_direct` |
| `runtime/sal.rs` | Replace `encode_ctrl_block_ipc` call at line 714 with `encode_ctrl_block_direct` |

No behaviour change. The encoded bytes are identical for both paths.

---

## What to watch out for

- **Null bitmap**: must write `ctrl::NULL_BIT_ERROR_MSG` (not `0`) when `error_msg.is_empty()`. Zero means the error_msg column is non-null, which is wrong on the common path.
- **Checksum**: `encode_wire_into` passes `checksum=true`; implement the hash branch correctly, do not silently drop it.
- **Byte-count identity**: assert `CTRL_BLOCK_SIZE_NO_BLOB == schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, 0)` in a `#[test]`. A `const assert` cannot be used because `schema_wal_block_size` is not `const fn`.
- **Template correctness**: validate by encoding the same message with both `encode_ctrl_block_direct` and `encode_ctrl_block_ipc` and asserting byte-for-byte equality. Round-tripping through `peek_control_block` catches decode errors but not subtle field-offset swaps; the direct comparison catches those.

---

## Expected impact

Eliminates pool acquire/release and the entire `encode_to_wire` call stack for
the common (no-error) path. The `copy_from_slice` of 248 bytes is the same
byte-move volume as `Vec::resize(248, 0)`, but the pool synchronization and
encoding overhead are gone. Removes the pool + encoding overhead attributed to
`Batch::with_schema → encode_ctrl_block_ipc → scatter_wire_group → committer`
in the 2026-04-30 profile (the memmove bucket entry itself is not eliminated,
only its pool and call-chain overhead). All paths that write SAL groups benefit.
