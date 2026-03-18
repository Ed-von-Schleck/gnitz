# IPC Unification: One Format to Rule Them All

## Problem

The IPC layer uses a bespoke ZSet wire encoding that is architecturally inconsistent with
everything else in gnitz. WAL blocks and shard files transmit `ArenaZSetBatch` in its native
SoA columnar form, including German Strings (16-byte inline-or-blob structs) with a shared
blob arena. IPC reinvents a completely different string encoding: `[u32 global_blob_offset |
u32 length]` per row pointing into a flat blob area. This requires a two-pass string
conversion in both directions and has been the source of multiple correctness bugs
(shared_blob_buf issue, global blob offset miscalculation, multiple `owned_bufs` frees).

Beyond the data encoding, the original plan would still have left a bespoke 80-byte envelope
carrying control metadata (flags, seek params, status, error) as raw integers with no
checksum protection. That envelope is structurally identical to the problem it was solving:
a parallel format sitting outside the WAL codec.

The complete fix: **IPC is WAL block streaming**. An IPC message is 1–3 WAL blocks in a
memfd: a control block, an optional schema block, and an optional data block. The control
block is a single-row batch using `CONTROL_SCHEMA` — the same RowBuilder / WAL codec path
as everything else. There is no envelope, no bespoke integer writes, and no unchecked
metadata. Every byte in every message is covered by a WAL block checksum.

## Goal

- IPC messages are sequences of WAL blocks. `encode_batch_to_buffer` /
  `decode_batch_from_ptr` are the universal codec. Nothing else exists.
- The entire IPC string conversion layer (`_write_ipc_strings`, `_read_ipc_strings`,
  `_parse_zset_section`, `_write_zset_section`, etc.) is deleted.
- The 80-byte bespoke envelope is deleted. `CONTROL_SCHEMA` replaces it.
- `IPCPayload.owned_bufs` is gone — no scratch buffers, no two-step free.
- All external API signatures (`send_batch`, `receive_payload`, `try_receive_payload`,
  `schema_to_batch`, `batch_to_schema`) are unchanged.
- `rust_client` will need to be updated to the new format (tracked separately).

## Non-Goals

- Shard file format unification (different integrity model: per-region checksums, 64-byte
  SIMD alignment, XOR8 filter; intentionally distinct from WAL blocks).
- WAL directory entry u32→u64 upgrade.
- Zero-copy send path (still uses memfd + memmove; Rust-IPC concern).
- Rust client implementation (separate project).
- `py_client/` update — it is being removed and replaced by `rust_client`.

## Wire Format

An IPC message is 1–3 WAL blocks concatenated in a memfd. Each block is self-delimiting:
`total_size` is at `WAL_OFF_SIZE` (offset 16, u32) in every WAL block header. Blocks carry
their own XXHash64 checksum over the body.

```
[WAL block: control batch,  TID=IPC_CONTROL_TID, count=1]   ← always present
[WAL block: schema batch,   TID=target_id,        count=N]   ← if FLAG_HAS_SCHEMA
[WAL block: data batch,     TID=target_id,        count=M]   ← if FLAG_HAS_DATA
```

The `FLAG_HAS_SCHEMA` and `FLAG_HAS_DATA` bits live in the control batch's `flags` column,
which is also where all the existing protocol flags (`FLAG_PUSH`, `FLAG_SEEK`, etc.) live.
Block offsets are computed by chaining `total_size` reads from WAL headers — no explicit
block-size fields in the control metadata.

```
Constants:
  IPC_CONTROL_TID = -1   (sentinel TID; get_table_id() returns intmask(u32_all_ones) = -1)
```

### CONTROL_SCHEMA

A compile-time constant, analogous to `META_SCHEMA`.

```python
CONTROL_SCHEMA = types.TableSchema(
    [
        types.ColumnDefinition(types.TYPE_U64, name="msg_idx"),     # col 0: PK (always 0)
        types.ColumnDefinition(types.TYPE_U64, name="status"),      # col 1
        types.ColumnDefinition(types.TYPE_U64, name="client_id"),   # col 2
        types.ColumnDefinition(types.TYPE_U64, name="target_id"),   # col 3
        types.ColumnDefinition(types.TYPE_U64, name="flags"),       # col 4
        types.ColumnDefinition(types.TYPE_U64, name="seek_pk_lo"),  # col 5
        types.ColumnDefinition(types.TYPE_U64, name="seek_pk_hi"),  # col 6
        types.ColumnDefinition(types.TYPE_U64, name="seek_col_idx"),# col 7
        types.ColumnDefinition(
            types.TYPE_STRING, is_nullable=True, name="error_msg"  # col 8
        ),
    ],
    pk_index=0,
)

CTRL_COL_STATUS      = 1
CTRL_COL_CLIENT_ID   = 2
CTRL_COL_TARGET_ID   = 3
CTRL_COL_FLAGS       = 4
CTRL_COL_SEEK_PK_LO  = 5
CTRL_COL_SEEK_PK_HI  = 6
CTRL_COL_SEEK_COL    = 7
CTRL_COL_ERROR_MSG   = 8
```

All control fields are U64; `error_msg` is nullable STRING. On OK messages the string is
NULL — blob arena is empty. `target_id` travels in the control batch (col 3) — this is
the explicit fix for allocation responses, which carry no schema or data block and therefore
cannot infer `target_id` from any other block's TID field.

## ~~Step 1 — Refactor `wal_columnar.py`~~ DONE (a9ed26c)

- `_parse_wal_block(ptr, total_size, schema) -> (batch, lsn, tid)` extracted
- `decode_batch_from_buffer` delegates to it (no behavior change)
- `decode_batch_from_ptr` added as non-owning entry point for IPC

## Step 2 — Rewrite `ipc.py`

### 2a. Delete

Remove entirely:

| Symbol | Reason |
|---|---|
| `MAGIC_V2` | replaced by WAL format_version check + `IPC_CONTROL_TID` |
| `HEADER_SIZE = 96` | no envelope |
| `ALIGNMENT = 64` | WAL uses 8-byte alignment internally |
| All 13 `OFF_*` constants | no envelope |
| `IPC_STRING_STRIDE` | bespoke encoding gone |
| `IPC_NULL_STRING_OFFSET` | bespoke encoding gone |
| `_compute_ipc_blob_size` | bespoke encoding gone |
| `_write_ipc_strings` | bespoke encoding gone |
| `_read_ipc_strings` | bespoke encoding gone |
| `_compute_zset_wire_size` | bespoke encoding gone |
| `_write_zset_section` | bespoke encoding gone |
| `_parse_zset_section` | bespoke encoding gone |
| `_copy_buf` | only used by above |
| `_copy_raw` | only used by above |
| `IPCPayload.owned_bufs` field | no scratch buffers needed |

### 2b. Add imports and `CONTROL_SCHEMA` and constants

Add to the import block:
```python
from gnitz.storage import wal_columnar, wal_layout
from gnitz.storage import buffer as buffer_ops   # rename existing 'buffer' import
```
(`buffer_ops` is the alias used throughout the plan code for `buffer.Buffer` / `buffer.c_memmove`.)

### 2c. Add `CONTROL_SCHEMA` and constants

```python
IPC_CONTROL_TID = -1   # intmask(u32_all_ones); set_table_id(0xFFFFFFFF) → get_table_id() = -1

CONTROL_SCHEMA = types.TableSchema(
    [
        types.ColumnDefinition(types.TYPE_U64, name="msg_idx"),
        types.ColumnDefinition(types.TYPE_U64, name="status"),
        types.ColumnDefinition(types.TYPE_U64, name="client_id"),
        types.ColumnDefinition(types.TYPE_U64, name="target_id"),
        types.ColumnDefinition(types.TYPE_U64, name="flags"),
        types.ColumnDefinition(types.TYPE_U64, name="seek_pk_lo"),
        types.ColumnDefinition(types.TYPE_U64, name="seek_pk_hi"),
        types.ColumnDefinition(types.TYPE_U64, name="seek_col_idx"),
        types.ColumnDefinition(types.TYPE_STRING, is_nullable=True, name="error_msg"),
    ],
    pk_index=0,
)

CTRL_COL_STATUS      = 1
CTRL_COL_CLIENT_ID   = 2
CTRL_COL_TARGET_ID   = 3
CTRL_COL_FLAGS       = 4
CTRL_COL_SEEK_PK_LO  = 5
CTRL_COL_SEEK_PK_HI  = 6
CTRL_COL_SEEK_COL    = 7
CTRL_COL_ERROR_MSG   = 8

FLAG_HAS_SCHEMA = r_uint64(1 << 48)   # set when schema block follows control block
FLAG_HAS_DATA   = r_uint64(1 << 49)   # set when data block follows schema block
```

`FLAG_HAS_SCHEMA` and `FLAG_HAS_DATA` are new high bits in the existing `flags` field;
they do not conflict with the existing low-bit protocol flags.

### 2d. `IPCPayload` simplification

No `_immutable_fields_` (post-construction assignment pattern is incompatible); the JIT
optimization is recovered by `@jit.dont_look_inside` on callers.

```python
class IPCPayload(object):
    def __init__(self):
        self.fd           = -1
        self.ptr          = lltype.nullptr(rffi.CCHARP.TO)
        self.total_size   = 0
        self.batch        = None
        self.schema       = None
        self.target_id    = 0
        self.client_id    = 0
        self.flags        = r_uint64(0)
        self.seek_col_idx = 0
        self.seek_pk_lo   = r_int64(0)
        self.seek_pk_hi   = r_int64(0)
        self.status       = 0
        self.error_msg    = ""

    def close(self):
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, self.total_size)
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        if self.fd >= 0:
            os.close(intmask(self.fd))
            self.fd = -1
```

`close()` uses existing `mmap_posix.munmap_file` and `os.close` — no new FFI helpers
needed. No `for buf in owned_bufs: buf.free()` loop.

### 2e. Control batch helpers

```python
def _encode_control_batch(target_id, client_id, flags, seek_pk_lo, seek_pk_hi,
                          seek_col_idx, status, error_msg):
    # type: (...) -> ArenaZSetBatch
    ctrl = batch.ArenaZSetBatch(CONTROL_SCHEMA, initial_capacity=1)
    rb = RowBuilder(CONTROL_SCHEMA, ctrl)
    rb.begin(r_uint128(r_uint64(0)), r_int64(1))   # msg_idx PK = 0
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(status)))
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(client_id)))
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(target_id)))
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(flags)))
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(seek_pk_lo)))
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(seek_pk_hi)))
    rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(seek_col_idx)))
    if len(error_msg) > 0:
        rb.put_string(error_msg)
    else:
        rb.put_null()
    rb.commit()
    return ctrl


def _decode_control_batch(ctrl_batch, payload):
    # type: (ArenaZSetBatch, IPCPayload) -> None
    """Extract control fields from a decoded CONTROL_SCHEMA batch into payload."""
    acc = batch.ColumnarBatchAccessor(CONTROL_SCHEMA)
    acc.bind(ctrl_batch, 0)
    payload.status       = intmask(r_uint64(acc.get_int(CTRL_COL_STATUS)))
    payload.client_id    = intmask(r_uint64(acc.get_int(CTRL_COL_CLIENT_ID)))
    payload.target_id    = intmask(r_uint64(acc.get_int(CTRL_COL_TARGET_ID)))
    payload.flags        = r_uint64(acc.get_int(CTRL_COL_FLAGS))
    payload.seek_pk_lo   = r_int64(intmask(r_uint64(acc.get_int(CTRL_COL_SEEK_PK_LO))))
    payload.seek_pk_hi   = r_int64(intmask(r_uint64(acc.get_int(CTRL_COL_SEEK_PK_HI))))
    payload.seek_col_idx = intmask(r_uint64(acc.get_int(CTRL_COL_SEEK_COL)))
    length, prefix, sptr, hptr, py_s = acc.get_str_struct(CTRL_COL_ERROR_MSG)
    if not acc.is_null(CTRL_COL_ERROR_MSG):
        payload.error_msg = string_logic.resolve_string(sptr, hptr, py_s)
```

### 2f. `serialize_to_memfd` rewrite

`Buffer(0)` is growable by default. `encode_batch_to_buffer` calls `block_buf.reset()` then
`block_buf.alloc(...)` which triggers `ensure_capacity` on the first call. All three encode
buffers are allocated before any encoding begins so a single `try/finally` covers all of
them — if any `encode_batch_to_buffer` call raises, no buffer leaks.

```python
@jit.dont_look_inside
def serialize_to_memfd(target_id, client_id, zbatch, schema, flags,
                       seek_pk_lo, seek_pk_hi, seek_col_idx,
                       status, error_msg):
    has_data   = zbatch is not None and zbatch.length() > 0
    has_schema = has_data or (schema is not None and status == STATUS_OK)

    ctrl_flags = r_uint64(flags)
    if has_schema:
        ctrl_flags = ctrl_flags | FLAG_HAS_SCHEMA
    if has_data:
        ctrl_flags = ctrl_flags | FLAG_HAS_DATA

    ctrl_buf   = buffer_ops.Buffer(0)
    schema_buf = buffer_ops.Buffer(0)
    data_buf   = buffer_ops.Buffer(0)
    try:
        ctrl_batch = _encode_control_batch(
            target_id, client_id, ctrl_flags,
            seek_pk_lo, seek_pk_hi, seek_col_idx,
            status, error_msg,
        )
        wal_columnar.encode_batch_to_buffer(
            ctrl_buf, CONTROL_SCHEMA, r_uint64(0), IPC_CONTROL_TID, ctrl_batch
        )
        ctrl_batch.free()
        ctrl_size = ctrl_buf.offset

        schema_size = 0
        if has_schema:
            schema_batch = schema_to_batch(schema if schema is not None else zbatch._schema)
            wal_columnar.encode_batch_to_buffer(
                schema_buf, META_SCHEMA, r_uint64(0), target_id, schema_batch
            )
            schema_batch.free()
            schema_size = schema_buf.offset

        data_size = 0
        if has_data:
            wal_columnar.encode_batch_to_buffer(
                data_buf, zbatch._schema, r_uint64(0), target_id, zbatch
            )
            data_size = data_buf.offset

        total_size = ctrl_size + schema_size + data_size
        if total_size == 0:
            total_size = wal_layout.WAL_BLOCK_HEADER_SIZE  # defensive floor

        fd = mmap_posix.memfd_create_c("gnitz_ipc")
        mmap_posix.ftruncate_c(fd, total_size)
        ptr = mmap_posix.mmap_file(
            fd, total_size,
            prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
            flags=mmap_posix.MAP_SHARED,
        )
        if ctrl_size > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, ptr),
                rffi.cast(rffi.VOIDP, ctrl_buf.base_ptr),
                rffi.cast(rffi.SIZE_T, ctrl_size),
            )
        if schema_size > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(ptr, ctrl_size)),
                rffi.cast(rffi.VOIDP, schema_buf.base_ptr),
                rffi.cast(rffi.SIZE_T, schema_size),
            )
        if data_size > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(ptr, ctrl_size + schema_size)),
                rffi.cast(rffi.VOIDP, data_buf.base_ptr),
                rffi.cast(rffi.SIZE_T, data_size),
            )
        mmap_posix.munmap_file(ptr, total_size)
    finally:
        ctrl_buf.free()
        schema_buf.free()
        data_buf.free()

    return fd, total_size
```

`send_batch` unpacks the tuple: `memfd, _size = serialize_to_memfd(...)`.

### 2g. `_recv_and_parse` rewrite

Blocks are self-delimiting via `get_total_size()`. Bounds-check `off` before reading each
block header. Explicit guard for `FLAG_HAS_DATA` without `FLAG_HAS_SCHEMA` (protocol
error, not a silent skip). Exception handler restores pre-`IPCPayload` cleanup behaviour:
on any error after mmap, `payload.close()` is called to unmap and close the fd.

```python
@jit.dont_look_inside
def _recv_and_parse(fd):
    payload = IPCPayload()
    payload.fd = fd

    total_size = mmap_posix.fget_size(fd)
    if total_size < wal_layout.WAL_BLOCK_HEADER_SIZE:
        raise errors.StorageError("IPC payload too small")
    payload.total_size = total_size

    ptr = mmap_posix.mmap_file(
        fd, total_size,
        prot=mmap_posix.PROT_READ,
        flags=mmap_posix.MAP_SHARED,
    )
    payload.ptr = ptr

    try:
        # Block 0: control — always present
        ctrl_header = wal_layout.WALBlockHeaderView(ptr)
        if ctrl_header.get_table_id() != IPC_CONTROL_TID:
            raise errors.StorageError("IPC: bad control block TID")
        ctrl_size  = ctrl_header.get_total_size()
        ctrl_batch = wal_columnar.decode_batch_from_ptr(ptr, ctrl_size, CONTROL_SCHEMA)
        _decode_control_batch(ctrl_batch, payload)
        ctrl_batch.free()

        off = ctrl_size

        # Validate FLAG_HAS_DATA/SCHEMA consistency
        if payload.flags & FLAG_HAS_DATA and not (payload.flags & FLAG_HAS_SCHEMA):
            raise errors.StorageError("IPC: FLAG_HAS_DATA requires FLAG_HAS_SCHEMA")

        # Block 1: schema — if FLAG_HAS_SCHEMA
        if payload.flags & FLAG_HAS_SCHEMA:
            if off + wal_layout.WAL_BLOCK_HEADER_SIZE > total_size:
                raise errors.StorageError("IPC: truncated schema block")
            schema_size = wal_layout.WALBlockHeaderView(rffi.ptradd(ptr, off)).get_total_size()
            schema_batch = wal_columnar.decode_batch_from_ptr(
                rffi.ptradd(ptr, off), schema_size, META_SCHEMA
            )
            payload.schema = batch_to_schema(schema_batch)
            schema_batch.free()
            off += schema_size

        # Block 2: data — if FLAG_HAS_DATA
        if payload.flags & FLAG_HAS_DATA:
            if off + wal_layout.WAL_BLOCK_HEADER_SIZE > total_size:
                raise errors.StorageError("IPC: truncated data block")
            data_size = wal_layout.WALBlockHeaderView(rffi.ptradd(ptr, off)).get_total_size()
            payload.batch = wal_columnar.decode_batch_from_ptr(
                rffi.ptradd(ptr, off), data_size, payload.schema
            )
            # batch holds non-owning Buffer views into ptr (the mmap).
            # Must not be used after payload.close().

    except Exception as e:
        payload.close()
        raise e

    return payload
```

## Step 3 — `send_batch` update

`serialize_to_memfd` now returns `(fd, total_size)`. Update `send_batch`:

```python
@jit.dont_look_inside
def send_batch(sock_fd, target_id, zbatch, status=0, error_msg="",
               client_id=0, schema=None, flags=0,
               seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
    memfd, _total = serialize_to_memfd(
        target_id, client_id, zbatch,
        schema if schema is not None else (zbatch._schema if zbatch else None),
        flags, seek_pk_lo, seek_pk_hi, seek_col_idx,
        status, error_msg,
    )
    try:
        if ipc_ffi.send_fd(sock_fd, memfd) < 0:
            raise errors.StorageError("Failed to transmit IPC segment (Disconnected)")
    finally:
        os.close(memfd)
```

## Step 4 — Update `ipc_comprehensive_test.py`

Rename existing `v2_` tests (actual names in file → new names):

| Current name | New name |
|---|---|
| `test_v2_roundtrip` | `test_ipc_roundtrip` |
| `test_v2_int_only_roundtrip` | `test_int_only_roundtrip` |
| `test_v2_error_path` | `test_error_path` |
| `test_v2_scan_request` | `test_scan_request` |
| `test_v2_ipc_string_encoding` | `test_string_roundtrip` |
| `test_v2_null_strings` | `test_null_strings` |
| `test_v2_multi_string_column_roundtrip` | `test_multi_string_column_roundtrip` |
| `test_v2_schema_mismatch` | `test_schema_mismatch` |

Tests unchanged (no v2 internals): `test_unowned_buffer_lifecycle`,
`test_ipc_fd_hardening`, `test_meta_schema_roundtrip`.

`test_string_roundtrip`: remove any comment about IPC-internal string layout; verify only
push/receive correctness. `test_multi_string_column_roundtrip`: remove the `shared_blob_buf`
comment; it no longer applies.

Add one new test: `test_control_schema_roundtrip` — verifies that a control batch
encode/decode roundtrip preserves all fields including a non-null error string. No other
new tests; existing coverage is sufficient.

Update the top banner comment: `# IPC comprehensive test (WAL-block envelope)`.

## Implementation Order

1. `wal_columnar.py` — extract `_parse_wal_block`, add `decode_batch_from_ptr`.
   Run `make test` (storage_comprehensive_test, ipc_comprehensive_test).

2. `ipc.py` — add `CONTROL_SCHEMA` + constants, `_encode_control_batch`,
   `_decode_control_batch`, rewrite `serialize_to_memfd`, rewrite `_recv_and_parse`,
   update `send_batch`, delete all bespoke encoding. Run `make test`
   (ipc_comprehensive_test, master_worker_test, server_test).

3. `ipc_comprehensive_test.py` — rename tests, add `test_control_schema_roundtrip`.
   Run `make test`.

4. E2E: `GNITZ_WORKERS=4` — will fail until rust_client is updated; tracked separately.

Each step is a single commit.

## Breaking Changes

- **Wire protocol**: magic semantics change. Old clients using `GNITZ2PC` envelope parsing
  fail immediately (WAL format_version=2 at byte 20 is not a recognisable v2 envelope).
- `rust_client/gnitz-py/` E2E tests fail after step 2 until rust_client is updated.
- `py_client/` breaks but is being removed entirely.
- RPython unit tests (`make test`) are fully functional throughout all steps.

## Future Work

See `plans/wal-block-improvements.md`: exchange relay streaming scatter, parallel
`fan_out_scan`, DDL WAL fan-out, zero-copy WAL recovery, shared slot arena, exchange fault
tolerance.

## Validated Assumptions

| Claim | Verdict | Evidence |
|---|---|---|
| `encode_batch_to_buffer` handles count=0 | ✓ | writes header with count=0; `count * 8` region sizes → 0; skips no code |
| `decode_batch_from_buffer` owns `raw_buf` | ✓ | `WALColumnarBlock.free()` calls `lltype.free(self._raw_buf, flavor="raw")` |
| `Buffer.from_external_ptr.free()` is a no-op on memory | ✓ | `if self.is_owned: lltype.free(...)` — external ptrs have `is_owned=False`; pointer is nulled but not freed |
| `batch_to_schema` survives munmap | ✓ | uses `rffi.charpsize2str` → Python str; `TableSchema` holds only Python objects |
| WAL block checksum protects data integrity | ✓ | XXHash64 over everything after header, verified on decode |
| `Buffer(0)` works with `encode_batch_to_buffer` | ✓ | `growable=True` is default; `ensure_capacity` triggers on first alloc; `malloc(0)` + grow on first use is safe |
| `mmap_posix.munmap_file` / `os.close` are the correct cleanup calls | ✓ | confirmed; `ipc_ffi` has no `munmap`, `close_fd`, `create_memfd`, `mmap_fd` |
| `WALBlockHeaderView.get_total_size()` gives block size | ✓ | `WAL_OFF_SIZE = 16`, u32 getter |
| `_immutable_fields_` removed from `IPCPayload` | ✓ | post-construction assignment is incompatible with immutable field JIT hints |
| `ipc_ffi` exports only 7 functions | ✓ | confirmed by reading `ipc_ffi.py` |
| `IPC_CONTROL_TID` must be `-1`, not `0xFFFFFFFF` | ✓ | `set_table_id(-1)` → `rffi.cast(rffi.UINT, -1)` stores 0xFFFFFFFF; `get_table_id()` → `intmask(0xFFFFFFFF) = -1` |
| `target_id` cannot be inferred from schema/data block TID for allocation responses | ✓ | FLAG_ALLOCATE_* responses carry no schema or data block |
| `get_str_struct` on null column is safe | ✓ | reads 16 zero bytes from column buffer; `unpack_string` exits immediately on `length == 0`; no blob dereference |
| `FLAG_HAS_SCHEMA/DATA` bits 48/49 don't conflict with existing flags | ✓ | highest existing flag is `FLAG_ALLOCATE_INDEX_ID = 512 = 2⁹` |
| `rffi.cast(rffi.CCHARP, 0)` in `IPCPayload.__init__` | ✗ FIXED | inconsistent with `lltype.nullptr(rffi.CCHARP.TO)` used in `close()`; RPython annotator prefers consistent null-pointer form — use `lltype.nullptr` throughout |
| `serialize_to_memfd` try/finally covers only memmove | ✗ FIXED | buffers allocated before try → leak if encode raises; moved all allocs inside single try/finally |
| `_recv_and_parse` reads block header at `off` without bounds check | ✗ FIXED | truncated payload → SIGSEGV; added `off + WAL_BLOCK_HEADER_SIZE > total_size` guard before each header read |
| `FLAG_HAS_DATA` without `FLAG_HAS_SCHEMA` silently skipped | ✗ FIXED | protocol error should raise explicitly, not silently produce a no-data payload |
| `_recv_and_parse` has no exception handler | ✗ FIXED | on parse error after mmap, fd and ptr leaked; added `except` that calls `payload.close()` |
