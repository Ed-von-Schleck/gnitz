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

In the 2026-04-30 profile, pool + encoding overhead attributed to this function
accounts for ~6% of the `memmove` bucket from the committer scatter path alone,
roughly 0.8% of total CPU.

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

`CTRL_BLOCK_SIZE_NO_BLOB = 248` is computed at compile time by the same
`ctrl_region_offset` const fn that produces the OFF_* constants below — see
"Compile-time offset computation". No hardcoded literal, no runtime
size-equality test required.

**Null bitmap:** `null_bmp = ctrl::NULL_BIT_ERROR_MSG` when `error_msg.is_empty()`
(col 8 is null). When `error_msg` is non-empty, `null_bmp = 0` (col 8 is
non-null). This matches the existing implementation exactly; getting it wrong
causes `peek_control_block` to misread the error_msg field.

### Implementation approach: lazily-generated template

All directory offsets, region offsets, and every WAL header field that does not
depend on call arguments are compile-time constants. Use a
`static CTRL_BLOCK_TEMPLATE: std::sync::LazyLock<[u8; CTRL_BLOCK_SIZE_NO_BLOB]>`
initialized by calling `encode_ctrl_block_ipc` once with dummy zero values and
`checksum=false`. The template encodes: WAL header (including the constant WAL size
field), directory, pk=0, weight=1, null_bmp=`NULL_BIT_ERROR_MSG`, and the
all-zero 16-byte German string header for error_msg. Because the template is
derived directly from `encode_ctrl_block_ipc`, any future changes to the WAL
header layout or schema are automatically reflected without manual sync.

```rust
static CTRL_BLOCK_TEMPLATE: std::sync::LazyLock<[u8; CTRL_BLOCK_SIZE_NO_BLOB]> =
    std::sync::LazyLock::new(|| {
        let mut arr = [0u8; CTRL_BLOCK_SIZE_NO_BLOB];
        let n = encode_ctrl_block_ipc(&mut arr, 0, 0, 0, 0, 0, 0, 0, 0, b"", false);
        assert_eq!(n, CTRL_BLOCK_SIZE_NO_BLOB,
            "ctrl block template size mismatch");
        arr
    });
```

The buffer is sized by `CTRL_BLOCK_SIZE_NO_BLOB` directly: if `encode_ctrl_block_ipc`
were to write more bytes (because the const fn drifted from `wal::encode`'s actual
layout), the slice indexing inside `encode_ctrl_block_ipc` would panic with a clear
out-of-bounds error during init rather than silently writing a malformed template.
The byte-equality test (see below) is the primary correctness guard.

Each call:

1. Sub-slice once: `let buf = &mut out[offset..offset + CTRL_BLOCK_SIZE_NO_BLOB]`.
   This allows LLVM to prove all subsequent indexed writes are in-bounds and elide
   those bounds checks entirely.
2. `buf.copy_from_slice(&*CTRL_BLOCK_TEMPLATE)`.
3. Patch the **7 variable fields** at their known offsets into `buf`:
   - `status` at 168 (8 bytes — see casting note below)
   - `client_id` at 176
   - `target_id` at 184
   - `wire_flags` at 192
   - `seek_pk` at 200 (16 bytes)
   - `seek_col_idx` at 216
   - `request_id` at 224
4. If `checksum=true`, compute `crate::xxh::checksum(&buf[gnitz_wire::WAL_HEADER_SIZE..CTRL_BLOCK_SIZE_NO_BLOB])`
   and write the result as 8 LE bytes at `buf[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8]`.
   This mirrors `wal::encode` exactly: the hash covers the payload only (post-header),
   and the stored value occupies the 8-byte checksum slot in the header.

The WAL size field is constant and is baked into the template; it does not
need to be patched per call.

This eliminates all per-call offset arithmetic. The only work per call is 7 store
operations into known byte offsets plus the optional checksum.

### Checksum handling

`encode_wire_into` (line 354) passes `checksum=true` to `encode_wire_into_impl`.
`encode_wire_into_ipc` (line 365) passes `false`. Both call `encode_ctrl_block_ipc`
through `encode_wire_into_impl`. The direct encoder must correctly implement both
branches — `checksum=false` skips the hash, `checksum=true` computes and writes it.

The hash input is the post-header payload: `buf[WAL_HEADER_SIZE..CTRL_BLOCK_SIZE_NO_BLOB]`
= `buf[48..248]`. The result (8 bytes, LE) is written at `buf[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM+8]`
= `buf[24..32]`. Hashing the entire 248-byte slice would include the checksum field
itself in the input, causing decoder mismatch and block rejection.

### Error path fallback

When `!error_msg.is_empty()`, fall back to the existing `Batch`-based path inside
the same function rather than branching to a separate function. This keeps the
call sites unchanged and avoids any observable difference in encoded bytes.
`encode_ctrl_block_ipc` is kept as the fallback and is not deleted.

### Compile-time offset computation

A single `const fn ctrl_region_offset` derives both the OFF_* constants and
`CTRL_BLOCK_SIZE_NO_BLOB`. It walks `CONTROL_SCHEMA_DESC` once to build the
per-region size table, then sums the first `target_region` sizes. Passing
`NUM_REGIONS` returns the position after the last region — i.e., the total
block size for the no-blob path.

Mirroring `wal::encode`'s phase-1 directory walk: directory immediately follows
the WAL header; each region is `align8`-padded before its data. This helper
omits explicit `align8` calls because every region in `CONTROL_SCHEMA_DESC` has
a size that is a multiple of 8 (PK is U64, payload is U64/U128/STRING-16, blob
is 0). A const-time assertion enforces this precondition for **every** region
(including the PK), so a future schema change that introduces an unaligned
column fails at compile time rather than silently producing wrong offsets:

```rust
const fn ctrl_region_offset(target_region: usize) -> usize {
    use gnitz_wire::control::NUM_REGIONS;
    let schema = &CONTROL_SCHEMA_DESC;
    let pk_idx = schema.pk_index as usize;

    // Build the size table for all 12 regions. Asserts 8-alignment for each.
    let mut sizes = [0usize; NUM_REGIONS];
    sizes[0] = schema.columns[pk_idx].size as usize;          // pk
    sizes[1] = 8;                                             // weight
    sizes[2] = 8;                                             // null_bmp
    let mut pi = 0usize;
    let mut ci = 0usize;
    while ci < schema.num_columns as usize {
        if ci == pk_idx { ci += 1; continue; }
        sizes[3 + pi] = schema.columns[ci].size as usize;
        pi += 1;
        ci += 1;
    }
    sizes[NUM_REGIONS - 1] = 0;                               // blob (no-blob path)

    // Sum sizes[0..target_region] starting from the post-directory position.
    // The 8-alignment assertion ensures the implicit `align8` between regions
    // is a no-op (so we can omit it from the running sum).
    let mut pos = gnitz_wire::WAL_HEADER_SIZE + NUM_REGIONS * 8;
    let limit = if target_region < NUM_REGIONS { target_region } else { NUM_REGIONS };
    let mut r = 0;
    while r < limit {
        assert!(sizes[r] % 8 == 0,
            "ctrl_region_offset assumes every region size is 8-aligned");
        pos += sizes[r];
        r += 1;
    }
    pos
}

const CTRL_BLOCK_SIZE_NO_BLOB: usize = ctrl_region_offset(gnitz_wire::control::NUM_REGIONS);

const OFF_STATUS:       usize = ctrl_region_offset(gnitz_wire::control::REGION_STATUS);
const OFF_CLIENT_ID:    usize = ctrl_region_offset(gnitz_wire::control::REGION_CLIENT_ID);
const OFF_TARGET_ID:    usize = ctrl_region_offset(gnitz_wire::control::REGION_TARGET_ID);
const OFF_FLAGS:        usize = ctrl_region_offset(gnitz_wire::control::REGION_FLAGS);
const OFF_SEEK_PK:      usize = ctrl_region_offset(gnitz_wire::control::REGION_SEEK_PK);
const OFF_SEEK_COL_IDX: usize = ctrl_region_offset(gnitz_wire::control::REGION_SEEK_COL_IDX);
const OFF_REQUEST_ID:   usize = ctrl_region_offset(gnitz_wire::control::REGION_REQUEST_ID);
```

The byte-equality test against `encode_ctrl_block_ipc` (see "What to watch
out for") is the primary correctness guard against this helper drifting from
`wal::encode`'s layout — the const-fn alignment assertion only catches the
narrower case of a non-8-aligned region.

### Proposed implementation

```rust
#[inline]
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
        return encode_ctrl_block_ipc(
            out, offset, target_id, client_id, wire_flags,
            seek_pk, seek_col_idx, request_id, status, error_msg, checksum,
        );
    }
    let buf = &mut out[offset..offset + CTRL_BLOCK_SIZE_NO_BLOB];
    buf.copy_from_slice(&*CTRL_BLOCK_TEMPLATE);
    buf[OFF_STATUS..OFF_STATUS + 8].copy_from_slice(&(status as u64).to_le_bytes());
    buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 8].copy_from_slice(&client_id.to_le_bytes());
    buf[OFF_TARGET_ID..OFF_TARGET_ID + 8].copy_from_slice(&target_id.to_le_bytes());
    buf[OFF_FLAGS..OFF_FLAGS + 8].copy_from_slice(&wire_flags.to_le_bytes());
    buf[OFF_SEEK_PK..OFF_SEEK_PK + 16].copy_from_slice(&seek_pk.to_le_bytes());
    buf[OFF_SEEK_COL_IDX..OFF_SEEK_COL_IDX + 8].copy_from_slice(&seek_col_idx.to_le_bytes());
    buf[OFF_REQUEST_ID..OFF_REQUEST_ID + 8].copy_from_slice(&request_id.to_le_bytes());
    if checksum {
        let cs = crate::xxh::checksum(&buf[gnitz_wire::WAL_HEADER_SIZE..CTRL_BLOCK_SIZE_NO_BLOB]);
        buf[WAL_OFF_CHECKSUM..WAL_OFF_CHECKSUM + 8].copy_from_slice(&cs.to_le_bytes());
    }
    CTRL_BLOCK_SIZE_NO_BLOB
}
```

---

## Scope

| File | Change |
|------|--------|
| `runtime/wire.rs` | Add `ctrl_region_offset` const fn, derived `CTRL_BLOCK_SIZE_NO_BLOB` and `OFF_*` consts, `CTRL_BLOCK_TEMPLATE` LazyLock, `encode_ctrl_block_direct`; keep `encode_ctrl_block_ipc` |
| `runtime/wire.rs` | Replace `encode_ctrl_block_ipc` call at line 419 with `encode_ctrl_block_direct` |
| `runtime/wire.rs` | In `wire_size`: replace `schema_wal_block_size(&CONTROL_SCHEMA_DESC, 1, ctrl_blob)` with `if ctrl_blob == 0 { CTRL_BLOCK_SIZE_NO_BLOB } else { schema_wal_block_size(...) }` |
| `runtime/sal.rs` | Replace `encode_ctrl_block_ipc` call at line 714 with `encode_ctrl_block_direct`; replace the `wire_size(STATUS_OK, b"", None, None, None, None)` call for `ctrl_size` with `CTRL_BLOCK_SIZE_NO_BLOB` directly |

No behaviour change. The encoded bytes are identical for both paths.

---

## What to watch out for

- **Null bitmap**: must write `ctrl::NULL_BIT_ERROR_MSG` (not `0`) when `error_msg.is_empty()`. Zero means the error_msg column is non-null, which is wrong on the common path. The `LazyLock` template is initialized with `error_msg=b""` so the null bit is already baked in — do not overwrite it.
- **Checksum**: hash covers `buf[48..248]` only; result written at `buf[24..32]`. See checksum handling section.
- **`status` u32→u64 cast**: `status` is `u32` but col 1 is `U64` (8 bytes). Write `(status as u64).to_le_bytes()` at `OFF_STATUS`. Writing only 4 bytes would corrupt `client_id`.
- **Parameter order at call sites**: `encode_ctrl_block_ipc` and `encode_ctrl_block_direct` share the same signature. `target_id`, `client_id`, and `wire_flags` are all `u64` — a positional swap compiles silently. Verify each substitution against the parameter list mechanically, not visually. At `sal.rs:714`, `client_id=0` is a literal; confirm it stays in slot 4 (the `client_id` position) after renaming.
- **Direct encoder correctness**: validate by encoding the same message with both `encode_ctrl_block_direct` and `encode_ctrl_block_ipc` and asserting byte-for-byte equality. Use **distinct non-zero values** for every variable field (`status`, `client_id`, `target_id`, `wire_flags`, `seek_pk`, `seek_col_idx`, `request_id`) — identical or shared values can pass even when two field offsets are swapped. Use **`offset = 64`** (not 0) — a test with `offset = 0` passes even if writes are accidentally `out[offset + OFF_X..]` rather than `buf[OFF_X..]`. Test both `checksum=false` and `checksum=true`. This test is the primary guard against `ctrl_region_offset` drifting from `wal::encode`'s actual layout.
- **error_msg/blob directory entries need no patching**: for the fast path, `error_msg` is null (`null_bmp = NULL_BIT_ERROR_MSG`), the German string inline header is all-zeros, and the blob region has size 0. All three are constant across calls and are baked into the template. The directory entries for these regions (pointing to offsets 232 and 248, respectively) are therefore also constant. Do not add patching for them.

---

## Expected impact

Eliminates pool acquire/release and the entire `encode_to_wire` call stack for
the common (no-error) path. The `copy_from_slice` of 248 bytes is the same
byte-move volume as `Vec::resize(248, 0)`, but the pool synchronization and
encoding overhead are gone. Removes the pool + encoding overhead attributed to
`Batch::with_schema → encode_ctrl_block_ipc → scatter_wire_group → committer`
in the 2026-04-30 profile (the memmove bucket entry itself is not eliminated,
only its pool and call-chain overhead). All paths that write SAL groups benefit.
