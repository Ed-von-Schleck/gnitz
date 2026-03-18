# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from gnitz.storage import mmap_posix
from gnitz.storage import buffer as buffer_ops
from gnitz.storage import wal_columnar, wal_layout
from gnitz.server import ipc_ffi
from gnitz.core import errors, batch, types, strings as string_logic
from gnitz.core.batch import RowBuilder
from gnitz.catalog import system_tables

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
FLAG_SHUTDOWN = 4
FLAG_DDL_SYNC = 8
FLAG_EXCHANGE = 16
FLAG_PUSH = 32
FLAG_HAS_PK = 64
FLAG_SEEK = 128
FLAG_SEEK_BY_INDEX     = 256   # p4=col_idx, p5/p6=index_key; target_id=table_id
FLAG_ALLOCATE_INDEX_ID = 512   # target_id=0; response carries new index_id in target_id
FLAG_SCAN              = 0     # explicit name for "no flags" (no wire change)

# --- New WAL-block wire format flags ---
FLAG_HAS_SCHEMA         = r_uint64(1 << 48)  # schema block follows control block
FLAG_HAS_DATA           = r_uint64(1 << 49)  # data block follows schema block
FLAG_BATCH_SORTED       = r_uint64(1 << 50)  # batch is sorted by PK
FLAG_BATCH_CONSOLIDATED = r_uint64(1 << 51)  # batch is consolidated (unique PKs, positive weights)

# --- Control block TID ---
IPC_CONTROL_TID = 0xFFFFFFFF   # max uint32; never a real table ID; get_table_id() returns 4294967295

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

# --- Control Schema ---
CONTROL_SCHEMA = types.TableSchema(
    [
        types.ColumnDefinition(types.TYPE_U64, name="msg_idx"),      # col 0: PK
        types.ColumnDefinition(types.TYPE_U64, name="status"),       # col 1
        types.ColumnDefinition(types.TYPE_U64, name="client_id"),    # col 2
        types.ColumnDefinition(types.TYPE_U64, name="target_id"),    # col 3
        types.ColumnDefinition(types.TYPE_U64, name="flags"),        # col 4
        types.ColumnDefinition(types.TYPE_U64, name="seek_pk_lo"),   # col 5
        types.ColumnDefinition(types.TYPE_U64, name="seek_pk_hi"),   # col 6
        types.ColumnDefinition(types.TYPE_U64, name="seek_col_idx"), # col 7
        types.ColumnDefinition(types.TYPE_STRING, is_nullable=True, name="error_msg"),  # col 8
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
# Control Batch Encode/Decode
# ---------------------------------------------------------------------------


def _encode_control_batch(target_id, client_id, flags, seek_pk_lo, seek_pk_hi,
                          seek_col_idx, status, error_msg):
    ctrl = batch.ArenaZSetBatch(CONTROL_SCHEMA, initial_capacity=1)
    rb = RowBuilder(CONTROL_SCHEMA, ctrl)
    rb.begin(r_uint128(r_uint64(0)), r_int64(1))          # msg_idx PK = 0
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
    acc = batch.ColumnarBatchAccessor(CONTROL_SCHEMA)
    acc.bind(ctrl_batch, 0)
    payload.status       = intmask(r_uint64(acc.get_int(CTRL_COL_STATUS)))
    payload.client_id    = intmask(r_uint64(acc.get_int(CTRL_COL_CLIENT_ID)))
    payload.target_id    = intmask(r_uint64(acc.get_int(CTRL_COL_TARGET_ID)))
    payload.flags        = r_uint64(acc.get_int(CTRL_COL_FLAGS))
    payload.seek_pk_lo   = intmask(r_uint64(acc.get_int(CTRL_COL_SEEK_PK_LO)))
    payload.seek_pk_hi   = intmask(r_uint64(acc.get_int(CTRL_COL_SEEK_PK_HI)))
    payload.seek_col_idx = intmask(r_uint64(acc.get_int(CTRL_COL_SEEK_COL)))
    if not acc.is_null(CTRL_COL_ERROR_MSG):
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(CTRL_COL_ERROR_MSG)
        payload.error_msg = string_logic.resolve_string(sptr, hptr, py_s)


# ---------------------------------------------------------------------------
# IPCPayload
# ---------------------------------------------------------------------------


class IPCPayload(object):
    def __init__(self):
        self.fd           = -1
        self.ptr          = lltype.nullptr(rffi.CCHARP.TO)
        self.total_size   = r_uint64(0)
        self.batch        = None
        self.schema       = None
        self.target_id    = 0
        self.client_id    = 0
        self.flags        = r_uint64(0)
        self.seek_col_idx = 0
        self.seek_pk_lo   = 0
        self.seek_pk_hi   = 0
        self.status       = 0
        self.error_msg    = ""

    def close(self):
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, intmask(self.total_size))
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        if self.fd >= 0:
            os.close(intmask(self.fd))
            self.fd = -1


# ---------------------------------------------------------------------------
# Serialize
# ---------------------------------------------------------------------------


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
        if zbatch._sorted:
            ctrl_flags = ctrl_flags | FLAG_BATCH_SORTED
        if zbatch._consolidated:
            ctrl_flags = ctrl_flags | FLAG_BATCH_CONSOLIDATED

    ctrl_buf   = buffer_ops.Buffer(0)
    schema_buf = buffer_ops.Buffer(0)
    data_buf   = buffer_ops.Buffer(0)
    ctrl_batch   = None
    schema_batch = None
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
        ctrl_batch = None
        ctrl_size = ctrl_buf.offset

        schema_size = 0
        if has_schema:
            eff_schema = schema if schema is not None else zbatch._schema
            schema_batch = schema_to_batch(eff_schema)
            wal_columnar.encode_batch_to_buffer(
                schema_buf, META_SCHEMA, r_uint64(0), target_id, schema_batch
            )
            schema_batch.free()
            schema_batch = None
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
        if ctrl_batch is not None:
            ctrl_batch.free()
        if schema_batch is not None:
            schema_batch.free()
        ctrl_buf.free()
        schema_buf.free()
        data_buf.free()

    return fd, total_size


@jit.dont_look_inside
def send_batch(sock_fd, target_id, zbatch, status=0, error_msg="",
               client_id=0, schema=None, flags=0,
               seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
    """Synchronous send: Serializes and transmits a single IPC batch."""
    eff_schema = schema
    if eff_schema is None and zbatch is not None:
        eff_schema = zbatch._schema
    memfd, _total = serialize_to_memfd(
        target_id, client_id, zbatch,
        eff_schema, flags, seek_pk_lo, seek_pk_hi, seek_col_idx,
        status, error_msg,
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


def broadcast_batch(sock_fds, target_id, zbatch, schema=None, flags=0,
                    seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
    """Encode once, send fd to N sockets via SCM_RIGHTS. No ACK expected."""
    eff_schema = schema if schema is not None else (zbatch._schema if zbatch is not None else None)
    fd, _ = serialize_to_memfd(target_id, 0, zbatch, eff_schema, flags,
                               seek_pk_lo, seek_pk_hi, seek_col_idx, STATUS_OK, "")
    try:
        for i in range(len(sock_fds)):
            if ipc_ffi.send_fd(sock_fds[i], fd) < 0:
                raise errors.StorageError("broadcast_batch: send_fd failed")
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Receive
# ---------------------------------------------------------------------------


@jit.dont_look_inside
def _recv_and_parse(fd):
    payload = IPCPayload()
    payload.fd = fd

    total_size = intmask(r_uint64(mmap_posix.fget_size(fd)))
    if total_size < wal_layout.WAL_BLOCK_HEADER_SIZE:
        raise errors.StorageError("IPC payload too small")
    payload.total_size = r_uint64(total_size)

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

        # Protocol invariant: FLAG_HAS_DATA requires FLAG_HAS_SCHEMA
        if payload.flags & FLAG_HAS_DATA and not (payload.flags & FLAG_HAS_SCHEMA):
            raise errors.StorageError("IPC: FLAG_HAS_DATA requires FLAG_HAS_SCHEMA")

        # Block 1: schema — if FLAG_HAS_SCHEMA
        if payload.flags & FLAG_HAS_SCHEMA:
            if off + wal_layout.WAL_BLOCK_HEADER_SIZE > total_size:
                raise errors.StorageError("IPC: truncated schema block")
            schema_header = wal_layout.WALBlockHeaderView(rffi.ptradd(ptr, off))
            schema_size = schema_header.get_total_size()
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
            data_header = wal_layout.WALBlockHeaderView(rffi.ptradd(ptr, off))
            data_size = data_header.get_total_size()
            payload.batch = wal_columnar.decode_batch_from_ptr(
                rffi.ptradd(ptr, off), data_size, payload.schema
            )
            if payload.flags & FLAG_BATCH_SORTED:
                payload.batch.mark_sorted(True)
            if payload.flags & FLAG_BATCH_CONSOLIDATED:
                payload.batch.mark_consolidated(True)
            # batch holds non-owning Buffer views into ptr (the mmap).
            # Must not be used after payload.close().

    except Exception as e:
        payload.close()
        raise e

    return payload


@jit.dont_look_inside
def receive_payload(sock_fd):
    """Receives an IPC segment. Self-describing — no registry needed."""
    fd = ipc_ffi.recv_fd(sock_fd)
    if fd < 0:
        raise errors.StorageError("Failed to receive IPC descriptor (Socket closed)")
    return _recv_and_parse(fd)


@jit.dont_look_inside
def try_receive_payload(sock_fd):
    """Non-blocking receive. Returns None if no message ready (EAGAIN)."""
    fd = ipc_ffi.recv_fd_nb(sock_fd)
    if fd == -2:
        return None
    if fd == -1:
        raise errors.ClientDisconnectedError("Client disconnected")
    if fd < 0:
        raise errors.StorageError("Message received without file descriptor")
    return _recv_and_parse(fd)
