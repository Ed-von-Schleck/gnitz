# IPC Push Overhead Reduction

Two independent improvements to the push-message hot path. Neither changes
the wire format or worker dispatch logic.

---

## Background

Every `fan_out_push` call serialises one IPC message per worker via
`serialize_to_memfd`. For N=4 workers that is 4 outbound push messages plus
4 ACK messages, each going through the full memfd lifecycle. Two redundant
costs stand out:

1. **Three heap buffers, three memmoves.** `serialize_to_memfd` encodes the
   control, schema, and data WAL blocks into three separate `Buffer` objects,
   then copies all three into the memfd with three `c_memmove` calls.

2. **Schema block re-encoded on every push.** The schema block
   (`schema_to_batch` → `encode_batch_to_buffer`) is rebuilt from scratch for
   every message. Workers already have the schema in `engine.registry`; the
   block is transmitted to keep the protocol self-describing, but its
   *encoding* does not need to repeat.

These two items are addressed below. They compose cleanly: item B threads its
cached buffer directly into the single-buffer path from item A.

---

## Item A: Single wire buffer in `serialize_to_memfd`

### What changes

**`gnitz/storage/wal_columnar.py`** — add `encode_batch_append`:

```python
def encode_batch_append(block_buf, schema, lsn, table_id, batch):
    """Append one WAL block to block_buf without resetting it.

    Identical to encode_batch_to_buffer except:
    - No block_buf.reset().
    - block_start = block_buf.offset at entry; all directory offsets and
      header pointers are relative to block_start, not to base_ptr.
    - total_size stored in the header is the block's own byte count,
      not block_buf.offset.

    Block sizes are always multiples of 8 (all column types have sizes that
    are powers of 2; header=48 and directory=num_regions×8 are multiples of
    8). Consecutive encode_batch_append calls therefore produce adjacent
    blocks with no inter-block gap, matching the existing layout in the memfd
    and the decoder's sequential-scan assumption.
    """
    block_start = block_buf.offset

    count = batch.length()
    num_cols = len(schema.columns)
    num_non_pk = 0
    for i in range(num_cols):
        if i != schema.pk_index:
            num_non_pk += 1
    num_data_regions = 4 + num_non_pk + 1

    block_buf.alloc(wal_layout.WAL_BLOCK_HEADER_SIZE)

    dir_start = block_buf.offset
    dir_size = num_data_regions * 8
    block_buf.alloc(dir_size, alignment=1)

    region_offsets = newlist_hint(num_data_regions)
    region_sizes   = newlist_hint(num_data_regions)
    for _ in range(num_data_regions):
        region_offsets.append(0)
        region_sizes.append(0)

    region_idx = 0
    region_idx = _copy_and_record(block_buf, batch.pk_lo_buf.base_ptr,
                                  count * 8, region_offsets, region_sizes, region_idx)
    region_idx = _copy_and_record(block_buf, batch.pk_hi_buf.base_ptr,
                                  count * 8, region_offsets, region_sizes, region_idx)
    region_idx = _copy_and_record(block_buf, batch.weight_buf.base_ptr,
                                  count * 8, region_offsets, region_sizes, region_idx)
    region_idx = _copy_and_record(block_buf, batch.null_buf.base_ptr,
                                  count * 8, region_offsets, region_sizes, region_idx)
    for ci in range(num_cols):
        if ci == schema.pk_index:
            continue
        col_sz = count * batch.col_strides[ci]
        region_idx = _copy_and_record(block_buf, batch.col_bufs[ci].base_ptr,
                                      col_sz, region_offsets, region_sizes, region_idx)
    blob_size = batch.blob_arena.offset
    region_idx = _copy_and_record(block_buf, batch.blob_arena.base_ptr,
                                  blob_size, region_offsets, region_sizes, region_idx)

    # Directory entries store block-relative offsets.
    for ri in range(num_data_regions):
        dir_entry_ptr = rffi.ptradd(block_buf.base_ptr, dir_start + ri * 8)
        u32p = rffi.cast(rffi.UINTP, dir_entry_ptr)
        u32p[0] = rffi.cast(rffi.UINT, region_offsets[ri] - block_start)
        u32p[1] = rffi.cast(rffi.UINT, region_sizes[ri])

    total_size = block_buf.offset - block_start
    header = wal_layout.WALBlockHeaderView(
        rffi.ptradd(block_buf.base_ptr, block_start)
    )
    header.set_lsn(r_uint64(lsn))
    header.set_table_id(table_id)
    header.set_entry_count(count)
    header.set_total_size(total_size)
    header.set_format_version(wal_layout.WAL_FORMAT_VERSION_CURRENT)
    header.set_num_regions(num_data_regions)
    header.set_blob_size(r_uint64(blob_size))

    body_ptr  = rffi.ptradd(block_buf.base_ptr,
                            block_start + wal_layout.WAL_BLOCK_HEADER_SIZE)
    body_size = total_size - wal_layout.WAL_BLOCK_HEADER_SIZE
    if body_size > 0:
        header.set_checksum(xxh.compute_checksum(body_ptr, body_size))
```

`encode_batch_to_buffer` becomes a two-line wrapper:

```python
def encode_batch_to_buffer(block_buf, schema, lsn, table_id, batch):
    block_buf.reset()
    encode_batch_append(block_buf, schema, lsn, table_id, batch)
```

All existing callers (`wal.py`, tests) use `encode_batch_to_buffer` and are
unaffected.

**`gnitz/server/ipc.py`** — rewrite `serialize_to_memfd` body:

Replace the three `Buffer` objects (`ctrl_buf`, `schema_buf`, `data_buf`) with
a single `wire_buf`. Replace the three `c_memmove` calls with one.

```python
wire_buf     = buffer_ops.Buffer(0)
ctrl_batch   = None
schema_batch = None
try:
    ctrl_batch = _encode_control_batch(
        target_id, client_id, ctrl_flags,
        seek_pk_lo, seek_pk_hi, seek_col_idx,
        status, error_msg,
    )
    wal_columnar.encode_batch_append(
        wire_buf, CONTROL_SCHEMA, r_uint64(0), IPC_CONTROL_TID, ctrl_batch
    )
    ctrl_batch.free()
    ctrl_batch = None

    if has_schema:
        eff_schema = schema if schema is not None else zbatch._schema
        schema_batch = schema_to_batch(eff_schema)
        wal_columnar.encode_batch_append(
            wire_buf, META_SCHEMA, r_uint64(0), target_id, schema_batch
        )
        schema_batch.free()
        schema_batch = None

    if has_data:
        wal_columnar.encode_batch_append(
            wire_buf, zbatch._schema, r_uint64(0), target_id, zbatch
        )

    total_size = wire_buf.offset
    if total_size == 0:
        total_size = wal_layout.WAL_BLOCK_HEADER_SIZE

    fd = mmap_posix.memfd_create_c("gnitz_ipc")
    mmap_posix.ftruncate_c(fd, total_size)
    ptr = mmap_posix.mmap_file(
        fd, total_size,
        prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        flags=mmap_posix.MAP_SHARED,
    )
    buffer_ops.c_memmove(
        rffi.cast(rffi.VOIDP, ptr),
        rffi.cast(rffi.VOIDP, wire_buf.base_ptr),
        rffi.cast(rffi.SIZE_T, total_size),
    )
    mmap_posix.munmap_file(ptr, total_size)
finally:
    if ctrl_batch is not None:
        ctrl_batch.free()
    if schema_batch is not None:
        schema_batch.free()
    wire_buf.free()

return fd, total_size
```

### What this saves per message
- 2 heap Buffer allocations eliminated (`schema_buf`, `data_buf`)
- 2 internal `ensure_capacity` reallocs eliminated
- 3 `c_memmove` calls → 1
- 2 `Buffer.free()` calls eliminated
- Syscall count unchanged (memfd lifecycle is unchanged)

### Test
`make ipc_comprehensive_test` — the existing round-trip tests cover
encode+decode for all message types.

---

## Item B: Schema encoding cache in `MasterDispatcher`

### What changes

**`gnitz/server/ipc.py`** — expose a schema pre-encoder and add an optional
bypass in `serialize_to_memfd`:

```python
def encode_schema_block(table_id, schema):
    """Pre-encode a schema WAL block for caching. Caller owns the returned Buffer."""
    buf = buffer_ops.Buffer(0)
    schema_batch = schema_to_batch(schema)
    encode_batch_append(buf, META_SCHEMA, r_uint64(0), table_id, schema_batch)
    schema_batch.free()
    return buf
```

Add `preencoded_schema=None` to `send_batch` and `serialize_to_memfd`.
In `serialize_to_memfd`, when `has_schema` is true and `preencoded_schema` is
not None, copy the pre-encoded bytes directly instead of calling
`schema_to_batch + encode_batch_append`:

```python
if has_schema:
    if preencoded_schema is not None:
        wire_buf.put_bytes(preencoded_schema.base_ptr, preencoded_schema.offset)
    else:
        eff_schema = schema if schema is not None else zbatch._schema
        schema_batch = schema_to_batch(eff_schema)
        wal_columnar.encode_batch_append(
            wire_buf, META_SCHEMA, r_uint64(0), target_id, schema_batch
        )
        schema_batch.free()
        schema_batch = None
```

`send_batch` signature gains `preencoded_schema=None` and passes it through
to `serialize_to_memfd`.

**`gnitz/server/master.py`** — schema cache in `MasterDispatcher`:

```python
def __init__(self, num_workers, worker_fds, worker_pids, assignment, program_cache):
    ...
    self._schema_cache = {}   # int -> Buffer (pre-encoded META_SCHEMA WAL block)

def _get_schema_buf(self, table_id, schema):
    if table_id in self._schema_cache:
        return self._schema_cache[table_id]
    buf = ipc.encode_schema_block(table_id, schema)
    self._schema_cache[table_id] = buf
    return buf
```

In `fan_out_push`, compute once and pass to every `send_batch` call:

```python
schema_buf = self._get_schema_buf(target_id, schema)
# ... inside the worker loop:
ipc.send_batch(
    self.worker_fds[w], target_id, sb, schema=schema,
    flags=ipc.FLAG_PUSH, preencoded_schema=schema_buf,
)
```

Apply the same `preencoded_schema=schema_buf` argument to the
`FLAG_PRELOADED_EXCHANGE` send calls.

`_relay_exchange` sends to workers with known schemas; add the cache lookup
there too. `broadcast_batch` (DDL sync) is infrequent and keeps the existing
path (`preencoded_schema=None`).

The cache is never invalidated because table schemas are immutable after
creation. The cache key is `table_id` (int); the value is a `Buffer`
containing a valid `META_SCHEMA` WAL block.

### What this saves per push message (all N workers)
- 1 `ArenaZSetBatch` allocation for the schema batch (not N — the cache is
  computed once per table, not per message)
- 1 `encode_batch_append` call for the schema block
- `num_cols` rows of `RowBuilder.commit()` work
- Same number of bytes transmitted (wire format unchanged)

### Test
`make master_worker_test` — exercises `fan_out_push` with N=4 workers,
including exchange relay paths.

---

## Implementation order

Item A first. Item B builds on it (uses `encode_batch_append` from ipc.py's
`encode_schema_block`; the `put_bytes` path in `serialize_to_memfd` slots
cleanly into the single-buffer layout).

```
Item A  encode_batch_append + serialize_to_memfd consolidation
        Files: wal_columnar.py, ipc.py
        Test:  make ipc_comprehensive_test

Item B  Schema encoding cache in MasterDispatcher
        Files: ipc.py (+encode_schema_block, +preencoded_schema param),
               master.py (+_schema_cache, +_get_schema_buf, fan_out_push)
        Test:  make master_worker_test
```

One commit per item. Run `GNITZ_WORKERS=4 pytest rust_client/gnitz-py/tests/`
after both items are in.

---

## What this does NOT do

- Does not eliminate the memfd lifecycle (that is Phase 5 / Option 3 territory)
- Does not eliminate schema bytes from the wire (workers still receive and
  decode the schema block; the cache only avoids re-encoding it)
- Does not touch the worker receive path, exchange relay, or WAL storage
