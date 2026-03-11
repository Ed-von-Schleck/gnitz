# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint32, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from gnitz.storage import mmap_posix, buffer
from gnitz.server import ipc_ffi
from gnitz.core import errors, batch, types, strings as string_logic
from gnitz.core.batch import RowBuilder
from gnitz.catalog import system_tables

# "GNITZ2PC" in little-endian hex
MAGIC_V2 = r_uint64(0x474E49545A325043)

# --- v2 Header: 96 bytes ---
# [00-07]  Magic            u64
# [08-11]  Status           u32    0=OK, 1=ERROR
# [12-15]  Error Str Len    u32
# [16-23]  Target ID        u64
# [24-31]  Client ID        u64
# [32-39]  Schema Row Count u64
# [40-47]  Schema Blob Size u64
# [48-55]  Data Row Count   u64
# [56-63]  Data Blob Size   u64
# [64-71]  Data PK Index    u64
# [72-79]  Flags            u64
# [80-95]  Reserved         16B
HEADER_SIZE = 96
ALIGNMENT = 64
MAX_ERR_LEN = 65536

OFF_MAGIC = 0
OFF_STATUS = 8
OFF_ERR_LEN = 12
OFF_TARGET_ID = 16
OFF_CLIENT_ID = 24
OFF_SCHEMA_COUNT = 32
OFF_SCHEMA_BLOB_SZ = 40
OFF_DATA_COUNT = 48
OFF_DATA_BLOB_SZ = 56
OFF_DATA_PK_INDEX = 64
OFF_FLAGS = 72

# --- Status Codes ---
STATUS_OK = 0
STATUS_ERROR = 1

# --- Protocol Flags ---
# ID allocation protocol:
#   Client sends: target_id=0, flags=FLAG_ALLOCATE_*_ID, no batch
#   Server responds: target_id=allocated_id, STATUS_OK, no schema/data
#   Server atomically: allocates in-memory + advances SeqTab
FLAG_ALLOCATE_TABLE_ID = 1   # Allocate a table/view ID atomically
FLAG_ALLOCATE_SCHEMA_ID = 2  # Allocate a schema ID atomically
FLAG_ALLOCATE_ID = FLAG_ALLOCATE_TABLE_ID  # backward compat

# --- IPC String Encoding ---
IPC_STRING_STRIDE = 8
IPC_NULL_STRING_OFFSET = 0xFFFFFFFF

# --- Meta-Schema Constants ---
# The Schema ZSet has this fixed 4-column layout.
META_FLAG_NULLABLE = 1
META_FLAG_IS_PK = 2

META_SCHEMA = types.TableSchema(
    [
        types.ColumnDefinition(types.TYPE_U64, name="col_idx"),
        types.ColumnDefinition(types.TYPE_U64, name="type_code"),
        types.ColumnDefinition(types.TYPE_U64, name="flags"),
        types.ColumnDefinition(types.TYPE_STRING, name="name"),
    ],
    pk_index=0,
)


def align_up(val, align):
    u_val = r_uint64(val)
    u_align = r_uint64(align)
    return (u_val + u_align - 1) & ~(u_align - 1)


# ---------------------------------------------------------------------------
# Schema <-> Batch conversion
# ---------------------------------------------------------------------------


def schema_to_batch(schema):
    """Converts a TableSchema into a META_SCHEMA batch (one row per column)."""
    num_cols = len(schema.columns)
    result = batch.ArenaZSetBatch(META_SCHEMA, initial_capacity=num_cols)
    rb = RowBuilder(META_SCHEMA, result)
    for ci in range(num_cols):
        col = schema.columns[ci]
        flags = 0
        if col.is_nullable:
            flags |= META_FLAG_NULLABLE
        if ci == schema.pk_index:
            flags |= META_FLAG_IS_PK
        rb.begin(r_uint128(r_uint64(ci)), r_int64(1))
        rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(col.field_type.code)))
        rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(flags)))
        rb.put_string(col.name)
        rb.commit()
    return result


def batch_to_schema(schema_batch):
    """Reconstructs a TableSchema from a META_SCHEMA batch."""
    count = schema_batch.length()
    if count == 0:
        raise errors.StorageError("Empty schema batch")

    acc = batch.ColumnarBatchAccessor(META_SCHEMA)
    pk_index = -1
    columns = newlist_hint(count)

    for i in range(count):
        acc.bind(schema_batch, i)
        col_idx = intmask(r_uint64(acc.get_int(0)))
        type_code = intmask(r_uint64(acc.get_int(1)))
        flags = intmask(r_uint64(acc.get_int(2)))

        length, prefix, struct_ptr, heap_ptr, py_string = acc.get_str_struct(3)
        name = string_logic.resolve_string(struct_ptr, heap_ptr, py_string)

        if col_idx != i:
            raise errors.StorageError(
                "Schema batch col_idx out of order: expected %d, got %d"
                % (i, col_idx)
            )

        field_type = system_tables.type_code_to_field_type(type_code)
        is_nullable = bool(flags & META_FLAG_NULLABLE)
        is_pk = bool(flags & META_FLAG_IS_PK)

        columns.append(types.ColumnDefinition(field_type, is_nullable=is_nullable, name=name))

        if is_pk:
            if pk_index >= 0:
                raise errors.StorageError("Multiple PK columns in schema batch")
            pk_index = i

    if pk_index < 0:
        raise errors.StorageError("No PK column found in schema batch")

    return types.TableSchema(columns, pk_index=pk_index)


# ---------------------------------------------------------------------------
# IPC String Encoding Helpers
# ---------------------------------------------------------------------------


def _compute_ipc_blob_size(src_batch, col_idx):
    """Compute total blob bytes needed for IPC string encoding of a column."""
    total = 0
    count = src_batch.length()
    for row in range(count):
        length, prefix, struct_ptr, heap_ptr, py_string = (
            src_batch._read_col_str_struct(row, col_idx)
        )
        s = string_logic.resolve_string(struct_ptr, heap_ptr, py_string)
        total += len(s)
    return total


def _write_ipc_strings(src_batch, col_idx, dest_ptr, dest_blob_ptr, count):
    """
    Writes IPC string encoding for a column.
    dest_ptr: points to the column buffer (count * 8 bytes).
    dest_blob_ptr: points to the blob arena start.
    Returns bytes written to blob arena.
    """
    blob_offset = 0
    for row in range(count):
        null_word = src_batch._read_null_word(row)
        payload_idx = col_idx if col_idx < src_batch._schema.pk_index else col_idx - 1
        is_null = bool(null_word & (r_uint64(1) << payload_idx))

        entry_ptr = rffi.ptradd(dest_ptr, row * IPC_STRING_STRIDE)
        u32_entry = rffi.cast(rffi.UINTP, entry_ptr)

        if is_null:
            u32_entry[0] = rffi.cast(rffi.UINT, IPC_NULL_STRING_OFFSET)
            u32_entry[1] = rffi.cast(rffi.UINT, 0)
            continue

        length, prefix, struct_ptr, heap_ptr, py_string = (
            src_batch._read_col_str_struct(row, col_idx)
        )
        s = string_logic.resolve_string(struct_ptr, heap_ptr, py_string)
        s_len = len(s)

        u32_entry[0] = rffi.cast(rffi.UINT, blob_offset)
        u32_entry[1] = rffi.cast(rffi.UINT, s_len)

        for bi in range(s_len):
            dest_blob_ptr[blob_offset + bi] = s[bi]
        blob_offset += s_len

    return blob_offset


def _read_ipc_strings(
    ipc_col_ptr, ipc_blob_ptr, ipc_blob_sz, count,
    dest_batch, dest_col_idx, owned_bufs
):
    """
    Reads IPC string encoding and writes German Strings into dest_batch's column.
    Creates owned buffers for the German String column and blob data.
    Appends created buffers to owned_bufs for lifecycle management.
    """
    stride = types.TYPE_STRING.size  # 16
    col_buf = buffer.Buffer(count * stride)
    blob_buf = buffer.Buffer(intmask(ipc_blob_sz) + 64)
    allocator = batch.BatchBlobAllocator(blob_buf)

    for row in range(count):
        entry_ptr = rffi.ptradd(ipc_col_ptr, row * IPC_STRING_STRIDE)
        u32_entry = rffi.cast(rffi.UINTP, entry_ptr)
        offset_val = intmask(u32_entry[0])
        length_val = intmask(u32_entry[1])

        dest = col_buf.alloc(stride, alignment=types.TYPE_STRING.alignment)

        # Check for null
        if r_uint64(offset_val) == r_uint64(IPC_NULL_STRING_OFFSET):
            # Write a zero-length German String (null marker handled via null_buf)
            for b in range(stride):
                dest[b] = "\x00"
            continue

        # Validate bounds
        if offset_val + length_val > intmask(ipc_blob_sz):
            raise errors.StorageError(
                "IPC string offset+length exceeds blob arena"
            )

        # Extract the string bytes
        src_data = rffi.ptradd(ipc_blob_ptr, offset_val)
        s = rffi.charpsize2str(src_data, length_val)

        string_logic.pack_and_write_blob(dest, s, allocator)

    owned_bufs.append(col_buf)
    owned_bufs.append(blob_buf)
    return col_buf, blob_buf


# ---------------------------------------------------------------------------
# ZSet Section Layout Computation
# ---------------------------------------------------------------------------


def _compute_zset_wire_size(schema, count, blob_sizes_for_strings):
    """
    Computes the total wire bytes for a ZSet section.
    blob_sizes_for_strings: list of blob sizes per string column (in schema order).
    Returns (total_bytes, col_offsets, col_wire_strides, blob_offset, total_blob_sz).
    col_offsets and col_wire_strides indexed by schema column index.
    """
    cur_off = r_uint64(0)
    num_cols = len(schema.columns)

    # Structural buffers: pk_lo, pk_hi, weight, null
    struct_sz = r_uint64(count * 8)
    pk_lo_off = cur_off
    cur_off = align_up(cur_off + struct_sz, ALIGNMENT)
    pk_hi_off = cur_off
    cur_off = align_up(cur_off + struct_sz, ALIGNMENT)
    weight_off = cur_off
    cur_off = align_up(cur_off + struct_sz, ALIGNMENT)
    null_off = cur_off
    cur_off = align_up(cur_off + struct_sz, ALIGNMENT)

    col_offsets = newlist_hint(num_cols)
    col_wire_strides = newlist_hint(num_cols)
    str_col_idx = 0
    total_blob_sz = r_uint64(0)

    for ci in range(num_cols):
        if ci == schema.pk_index:
            col_offsets.append(r_uint64(0))
            col_wire_strides.append(0)
        elif schema.columns[ci].field_type.code == types.TYPE_STRING.code:
            col_offsets.append(cur_off)
            col_wire_strides.append(IPC_STRING_STRIDE)
            cur_off = align_up(cur_off + r_uint64(count * IPC_STRING_STRIDE), ALIGNMENT)
            if str_col_idx < len(blob_sizes_for_strings):
                total_blob_sz = total_blob_sz + r_uint64(blob_sizes_for_strings[str_col_idx])
            str_col_idx += 1
        else:
            wire_stride = schema.columns[ci].field_type.size
            col_offsets.append(cur_off)
            col_wire_strides.append(wire_stride)
            cur_off = align_up(cur_off + r_uint64(wire_stride * count), ALIGNMENT)

    blob_offset = cur_off
    total_size = cur_off + total_blob_sz

    return (
        total_size, pk_lo_off, pk_hi_off, weight_off, null_off,
        col_offsets, col_wire_strides, blob_offset, total_blob_sz,
    )


# ---------------------------------------------------------------------------
# IPCPayload
# ---------------------------------------------------------------------------


class IPCPayload(object):
    """Handle for a received IPC v2 segment."""

    _immutable_fields_ = [
        "total_size",
        "status",
        "error_msg",
        "batch",
        "schema",
        "target_id",
        "client_id",
        "flags",
    ]

    def __init__(
        self, fd, ptr, total_size, status, error_msg,
        schema, batch_obj, target_id, client_id, flags, owned_bufs
    ):
        self.fd = fd
        self.ptr = ptr
        self.total_size = r_uint64(total_size)
        self.status = status
        self.error_msg = error_msg
        self.schema = schema
        self.batch = batch_obj
        self.target_id = target_id
        self.client_id = client_id
        self.flags = flags
        self.owned_bufs = owned_bufs

    def close(self):
        """Physical cleanup of the mapped segment and owned buffers."""
        for buf in self.owned_bufs:
            buf.free()
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, intmask(self.total_size))
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


# ---------------------------------------------------------------------------
# Serialize
# ---------------------------------------------------------------------------


def _copy_buf(dest_base, dest_off, src_buf, nbytes):
    """Copies nbytes from src_buf.base_ptr to dest_base + dest_off."""
    if nbytes > 0 and src_buf.base_ptr:
        buffer.c_memmove(
            rffi.cast(rffi.VOIDP, rffi.ptradd(dest_base, intmask(dest_off))),
            rffi.cast(rffi.VOIDP, src_buf.base_ptr),
            rffi.cast(rffi.SIZE_T, nbytes),
        )


def _copy_raw(dest_base, dest_off, src_ptr, nbytes):
    """Copies nbytes from src_ptr to dest_base + dest_off."""
    if nbytes > 0:
        buffer.c_memmove(
            rffi.cast(rffi.VOIDP, rffi.ptradd(dest_base, intmask(dest_off))),
            rffi.cast(rffi.VOIDP, src_ptr),
            rffi.cast(rffi.SIZE_T, nbytes),
        )


def _write_zset_section(
    ptr, section_base, zbatch, schema, count,
    pk_lo_off, pk_hi_off, weight_off, null_off,
    col_offsets, col_wire_strides, blob_offset
):
    """
    Writes a ZSet's column data into the mmap at section_base.
    Returns bytes written to blob arena.
    """
    base = intmask(section_base)

    # Structural buffers
    _copy_buf(ptr, section_base + pk_lo_off, zbatch.pk_lo_buf, count * 8)
    _copy_buf(ptr, section_base + pk_hi_off, zbatch.pk_hi_buf, count * 8)
    _copy_buf(ptr, section_base + weight_off, zbatch.weight_buf, count * 8)
    _copy_buf(ptr, section_base + null_off, zbatch.null_buf, count * 8)

    # Payload columns
    num_cols = len(schema.columns)
    blob_written = 0

    for ci in range(num_cols):
        if ci == schema.pk_index:
            continue

        col_type = schema.columns[ci].field_type
        dest_off = section_base + col_offsets[ci]

        if col_type.code == types.TYPE_STRING.code:
            # IPC string encoding
            dest_col_ptr = rffi.ptradd(ptr, intmask(dest_off))
            dest_blob_ptr = rffi.ptradd(ptr, intmask(section_base + blob_offset) + blob_written)
            written = _write_ipc_strings(
                zbatch, ci, dest_col_ptr, dest_blob_ptr, count
            )
            blob_written += written
        else:
            # Fixed-width: direct memcpy
            _copy_buf(ptr, dest_off, zbatch.col_bufs[ci], col_type.size * count)

    return blob_written


@jit.dont_look_inside
def serialize_to_memfd(
    target_id, schema=None, zbatch=None,
    status=0, error_msg="", client_id=0, flags=0
):
    """
    Serializes a v2 IPC message into a shared-memory file descriptor.
    Schema is always included when zbatch has rows (or when explicitly provided).
    """
    err_len = r_uint64(len(error_msg))
    data_count = intmask(zbatch.length()) if zbatch else 0

    if schema is None and zbatch is not None:
        schema = zbatch._schema

    # Build schema batch if we have a schema to send
    schema_batch = None
    schema_count = 0
    if schema is not None and (data_count > 0 or status == STATUS_OK):
        schema_batch = schema_to_batch(schema)
        schema_count = schema_batch.length()

    # Compute string blob sizes for schema batch
    schema_str_blob_sizes = newlist_hint(1)
    if schema_batch is not None and schema_count > 0:
        # META_SCHEMA has 1 string column: "name" at index 3
        schema_str_blob_sizes.append(_compute_ipc_blob_size(schema_batch, 3))

    # Compute string blob sizes for data batch
    data_str_blob_sizes = newlist_hint(4)
    if zbatch is not None and data_count > 0 and schema is not None:
        num_cols = len(schema.columns)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                continue
            if schema.columns[ci].field_type.code == types.TYPE_STRING.code:
                data_str_blob_sizes.append(_compute_ipc_blob_size(zbatch, ci))

    # Compute section sizes
    # Error section
    body_start = align_up(r_uint64(HEADER_SIZE) + err_len, ALIGNMENT)

    # Schema ZSet section
    schema_section_base = body_start
    schema_section_size = r_uint64(0)
    s_pk_lo_off = r_uint64(0)
    s_pk_hi_off = r_uint64(0)
    s_weight_off = r_uint64(0)
    s_null_off = r_uint64(0)
    s_col_offsets = newlist_hint(0)
    s_col_wire_strides = newlist_hint(0)
    s_blob_offset = r_uint64(0)
    s_total_blob_sz = r_uint64(0)

    if schema_batch is not None and schema_count > 0:
        (
            schema_section_size, s_pk_lo_off, s_pk_hi_off, s_weight_off, s_null_off,
            s_col_offsets, s_col_wire_strides, s_blob_offset, s_total_blob_sz,
        ) = _compute_zset_wire_size(META_SCHEMA, schema_count, schema_str_blob_sizes)

    # Data ZSet section
    data_section_base = align_up(schema_section_base + schema_section_size, ALIGNMENT)
    data_section_size = r_uint64(0)
    d_pk_lo_off = r_uint64(0)
    d_pk_hi_off = r_uint64(0)
    d_weight_off = r_uint64(0)
    d_null_off = r_uint64(0)
    d_col_offsets = newlist_hint(0)
    d_col_wire_strides = newlist_hint(0)
    d_blob_offset = r_uint64(0)
    d_total_blob_sz = r_uint64(0)

    if zbatch is not None and data_count > 0 and schema is not None:
        (
            data_section_size, d_pk_lo_off, d_pk_hi_off, d_weight_off, d_null_off,
            d_col_offsets, d_col_wire_strides, d_blob_offset, d_total_blob_sz,
        ) = _compute_zset_wire_size(schema, data_count, data_str_blob_sizes)

    total_size = align_up(data_section_base + data_section_size, ALIGNMENT)
    if total_size < r_uint64(HEADER_SIZE):
        total_size = r_uint64(HEADER_SIZE)

    data_pk_index = r_uint64(schema.pk_index) if schema is not None else r_uint64(0)

    fd = -1
    ptr = lltype.nullptr(rffi.CCHARP.TO)
    try:
        fd = mmap_posix.memfd_create_c("gnitz_ipc_v2")
        mmap_posix.ftruncate_c(fd, intmask(total_size))

        ptr = mmap_posix.mmap_file(
            fd,
            intmask(total_size),
            prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
            flags=mmap_posix.MAP_SHARED,
        )

        # Write Header (96 bytes)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_MAGIC))[0] = MAGIC_V2
        rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0] = r_uint32(status)
        rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0] = r_uint32(err_len)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_TARGET_ID))[0] = r_uint64(target_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_CLIENT_ID))[0] = r_uint64(client_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_SCHEMA_COUNT))[0] = r_uint64(schema_count)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_SCHEMA_BLOB_SZ))[0] = s_total_blob_sz
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_DATA_COUNT))[0] = r_uint64(data_count)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_DATA_BLOB_SZ))[0] = d_total_blob_sz
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_DATA_PK_INDEX))[0] = data_pk_index
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_FLAGS))[0] = r_uint64(flags)
        # Reserved (bytes 80-95): zero by default from ftruncate

        # Write Error String
        if err_len > 0:
            for i in range(intmask(err_len)):
                ptr[HEADER_SIZE + i] = error_msg[i]

        # Write Schema ZSet
        if schema_batch is not None and schema_count > 0:
            _write_zset_section(
                ptr, schema_section_base, schema_batch, META_SCHEMA, schema_count,
                s_pk_lo_off, s_pk_hi_off, s_weight_off, s_null_off,
                s_col_offsets, s_col_wire_strides, s_blob_offset,
            )

        # Write Data ZSet
        if zbatch is not None and data_count > 0 and schema is not None:
            _write_zset_section(
                ptr, data_section_base, zbatch, schema, data_count,
                d_pk_lo_off, d_pk_hi_off, d_weight_off, d_null_off,
                d_col_offsets, d_col_wire_strides, d_blob_offset,
            )

        mmap_posix.munmap_file(ptr, intmask(total_size))
        ptr = lltype.nullptr(rffi.CCHARP.TO)

        if schema_batch is not None:
            schema_batch.free()

        return fd

    except Exception as e:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)
        if schema_batch is not None:
            schema_batch.free()
        raise e


@jit.dont_look_inside
def send_batch(
    sock_fd, target_id, zbatch, status=0, error_msg="",
    client_id=0, schema=None, flags=0
):
    """Synchronous send: Serializes and transmits a single v2 batch."""
    memfd = serialize_to_memfd(
        target_id, schema=schema, zbatch=zbatch,
        status=status, error_msg=error_msg, client_id=client_id, flags=flags,
    )
    try:
        if ipc_ffi.send_fd(sock_fd, memfd) < 0:
            raise errors.StorageError("Failed to transmit IPC segment (Disconnected)")
    finally:
        os.close(memfd)


def send_error(sock_fd, error_msg, target_id=0, client_id=0):
    """Convenience helper to send an error response."""
    send_batch(
        sock_fd, target_id, None,
        status=STATUS_ERROR, error_msg=error_msg, client_id=client_id,
    )


# ---------------------------------------------------------------------------
# Receive
# ---------------------------------------------------------------------------


def _parse_zset_section(
    ptr, section_base, total_file_size, schema, count, blob_sz, owned_bufs
):
    """
    Parses a ZSet from mapped memory at section_base using the given schema.
    Returns an ArenaZSetBatch. String columns get owned buffers appended to owned_bufs.
    Non-string columns use zero-copy external pointers.
    """
    num_cols = len(schema.columns)
    struct_sz = count * 8

    # Walk the layout to find offsets (mirrors _compute_zset_wire_size)
    cur_off = r_uint64(0)

    pk_lo_off = section_base + cur_off
    cur_off = align_up(cur_off + r_uint64(struct_sz), ALIGNMENT)
    pk_hi_off = section_base + cur_off
    cur_off = align_up(cur_off + r_uint64(struct_sz), ALIGNMENT)
    weight_off = section_base + cur_off
    cur_off = align_up(cur_off + r_uint64(struct_sz), ALIGNMENT)
    null_off = section_base + cur_off
    cur_off = align_up(cur_off + r_uint64(struct_sz), ALIGNMENT)

    # Bounds check structural buffers
    if null_off + r_uint64(struct_sz) > total_file_size:
        raise errors.StorageError("Truncated IPC (structural buffer boundary)")

    pk_lo_buf = buffer.Buffer.from_external_ptr(
        rffi.ptradd(ptr, intmask(pk_lo_off)), struct_sz
    )
    pk_lo_buf.offset = struct_sz
    pk_hi_buf = buffer.Buffer.from_external_ptr(
        rffi.ptradd(ptr, intmask(pk_hi_off)), struct_sz
    )
    pk_hi_buf.offset = struct_sz
    weight_buf = buffer.Buffer.from_external_ptr(
        rffi.ptradd(ptr, intmask(weight_off)), struct_sz
    )
    weight_buf.offset = struct_sz
    null_buf = buffer.Buffer.from_external_ptr(
        rffi.ptradd(ptr, intmask(null_off)), struct_sz
    )
    null_buf.offset = struct_sz

    # Payload columns
    col_bufs = newlist_hint(num_cols)
    col_strides = newlist_hint(num_cols)
    blob_arena_offset = r_uint64(0)  # tracks where blob region starts

    for ci in range(num_cols):
        if ci == schema.pk_index:
            col_bufs.append(buffer.Buffer(0))
            col_strides.append(0)
        elif schema.columns[ci].field_type.code == types.TYPE_STRING.code:
            # IPC string column: 8 bytes per row, need to convert to German Strings
            col_wire_sz = count * IPC_STRING_STRIDE
            col_ptr_off = section_base + cur_off
            cur_off = align_up(cur_off + r_uint64(col_wire_sz), ALIGNMENT)
            # Defer string parsing until we know the blob offset
            col_bufs.append(None)  # placeholder
            col_strides.append(types.TYPE_STRING.size)
        else:
            wire_stride = schema.columns[ci].field_type.size
            col_wire_sz = wire_stride * count
            col_ptr_off = section_base + cur_off

            if col_ptr_off + r_uint64(col_wire_sz) > total_file_size:
                raise errors.StorageError("Truncated IPC (column buffer boundary)")

            cb = buffer.Buffer.from_external_ptr(
                rffi.ptradd(ptr, intmask(col_ptr_off)), col_wire_sz
            )
            cb.offset = col_wire_sz
            col_bufs.append(cb)
            col_strides.append(wire_stride)
            cur_off = align_up(cur_off + r_uint64(col_wire_sz), ALIGNMENT)

    # The blob region starts after all columns
    blob_region_off = section_base + cur_off

    # Now parse string columns (need blob region offset)
    # Re-walk to find each string column's wire offset
    str_cur_off = r_uint64(0)
    # Skip structural buffers
    str_cur_off = align_up(r_uint64(struct_sz), ALIGNMENT)
    str_cur_off = align_up(str_cur_off + r_uint64(struct_sz), ALIGNMENT)
    str_cur_off = align_up(str_cur_off + r_uint64(struct_sz), ALIGNMENT)
    str_cur_off = align_up(str_cur_off + r_uint64(struct_sz), ALIGNMENT)

    for ci in range(num_cols):
        if ci == schema.pk_index:
            continue

        col_type = schema.columns[ci].field_type
        if col_type.code == types.TYPE_STRING.code:
            col_wire_sz = count * IPC_STRING_STRIDE
            col_ptr_off = section_base + str_cur_off
            str_cur_off = align_up(str_cur_off + r_uint64(col_wire_sz), ALIGNMENT)

            ipc_col_ptr = rffi.ptradd(ptr, intmask(col_ptr_off))
            ipc_blob_ptr = rffi.ptradd(ptr, intmask(blob_region_off))

            col_buf, str_blob_buf = _read_ipc_strings(
                ipc_col_ptr, ipc_blob_ptr, blob_sz, count,
                None, ci, owned_bufs,
            )
            col_bufs[ci] = col_buf
        else:
            wire_stride = col_type.size
            str_cur_off = align_up(str_cur_off + r_uint64(wire_stride * count), ALIGNMENT)

    # Build a combined blob arena for the batch
    # For string columns we use the owned blobs from _read_ipc_strings.
    # For non-string batches, just use a dummy empty blob.
    # We need to pick the blob_buf from the last string column parsed,
    # or create an empty one.
    final_blob_buf = buffer.Buffer(0)
    if schema.has_string:
        # Find the last string column's blob buffer from owned_bufs
        # The _read_ipc_strings appends [col_buf, blob_buf] pairs
        # The last blob_buf is the one we want as the batch blob_arena
        if len(owned_bufs) >= 2:
            final_blob_buf = owned_bufs[len(owned_bufs) - 1]

    zbatch_obj = batch.ArenaZSetBatch.from_buffers(
        schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
        col_bufs, col_strides, final_blob_buf, count, is_sorted=True,
    )
    return zbatch_obj


@jit.dont_look_inside
def receive_payload(sock_fd):
    """
    Receives a v2 IPC segment. Self-describing — no registry needed.
    """
    fd = ipc_ffi.recv_fd(sock_fd)
    if fd < 0:
        raise errors.StorageError("Failed to receive IPC descriptor (Socket closed)")

    ptr = lltype.nullptr(rffi.CCHARP.TO)
    total_size = r_uint64(0)
    owned_bufs = newlist_hint(8)
    try:
        total_size = r_uint64(mmap_posix.fget_size(fd))
        if total_size < r_uint64(HEADER_SIZE):
            raise errors.StorageError("IPC payload too small for header")

        ptr = mmap_posix.mmap_file(
            fd,
            intmask(total_size),
            prot=mmap_posix.PROT_READ,
            flags=mmap_posix.MAP_SHARED,
        )

        # Verify Magic
        magic = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_MAGIC))[0]
        if magic != MAGIC_V2:
            raise errors.StorageError("Invalid IPC Magic (expected v2)")

        # Parse Header
        status = intmask(rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0])
        raw_err_len = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0]
        err_len = r_uint64(raw_err_len)
        target_id = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_TARGET_ID))[0])
        client_id = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_CLIENT_ID))[0])
        schema_count = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_SCHEMA_COUNT))[0])
        schema_blob_sz = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_SCHEMA_BLOB_SZ))[0])
        data_count = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_DATA_COUNT))[0])
        data_blob_sz = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_DATA_BLOB_SZ))[0])
        data_pk_index = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_DATA_PK_INDEX))[0])
        flags = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_FLAGS))[0])

        if err_len > r_uint64(MAX_ERR_LEN):
            raise errors.StorageError("Error message length exceeds safety limit")
        if r_uint64(HEADER_SIZE) + err_len > total_size:
            raise errors.StorageError("Truncated IPC (Error string boundary)")

        error_msg = ""
        if err_len > 0:
            error_msg = rffi.charpsize2str(
                rffi.ptradd(ptr, HEADER_SIZE), intmask(err_len)
            )

        # Validate protocol invariant
        if data_count > 0 and schema_count == 0:
            raise errors.StorageError("Protocol error: data_rows > 0 but schema_rows == 0")

        body_start = align_up(r_uint64(HEADER_SIZE) + err_len, ALIGNMENT)

        # Parse Schema ZSet
        wire_schema = None
        schema_section_base = body_start
        schema_section_end = schema_section_base

        if schema_count > 0:
            schema_owned = newlist_hint(4)
            schema_batch = _parse_zset_section(
                ptr, schema_section_base, total_size,
                META_SCHEMA, schema_count, schema_blob_sz, schema_owned,
            )
            wire_schema = batch_to_schema(schema_batch)
            # Free the intermediate schema batch buffers
            schema_batch.free()
            for buf in schema_owned:
                buf.free()

            # Compute schema section size to find data section start
            schema_str_blobs = newlist_hint(1)
            schema_str_blobs.append(intmask(schema_blob_sz))
            schema_wire_result = _compute_zset_wire_size(
                META_SCHEMA, schema_count, schema_str_blobs
            )
            schema_section_end = align_up(
                schema_section_base + schema_wire_result[0], ALIGNMENT
            )

        # Parse Data ZSet
        zbatch = None
        if data_count > 0 and wire_schema is not None:
            data_section_base = schema_section_end
            zbatch = _parse_zset_section(
                ptr, data_section_base, total_size,
                wire_schema, data_count, data_blob_sz, owned_bufs,
            )

        return IPCPayload(
            fd, ptr, total_size, status, error_msg,
            wire_schema, zbatch, target_id, client_id, flags, owned_bufs,
        )

    except Exception as e:
        for buf in owned_bufs:
            buf.free()
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)
        raise e
