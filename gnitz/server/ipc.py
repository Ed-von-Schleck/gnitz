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
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from gnitz.core import errors, batch, types, strings as string_logic
from gnitz.core.batch import RowBuilder
from gnitz.catalog import system_tables

# Volatile u64 read for cross-process shared memory.
# Prevents GCC from caching the value in a register across loop iterations.
_volatile_eci = ExternalCompilationInfo(
    pre_include_bits=[
        "unsigned long long gnitz_volatile_read_u64(const char *p);",
        "void gnitz_volatile_write_u64(char *p, unsigned long long val);",
    ],
    separate_module_sources=["""
unsigned long long gnitz_volatile_read_u64(const char *p) {
    return *(volatile unsigned long long *)p;
}
void gnitz_volatile_write_u64(char *p, unsigned long long val) {
    *(volatile unsigned long long *)p = val;
}
"""],
)

_volatile_read_u64 = rffi.llexternal(
    "gnitz_volatile_read_u64",
    [rffi.CCHARP],
    rffi.ULONGLONG,
    compilation_info=_volatile_eci,
    _nowrapper=True,
)

_volatile_write_u64 = rffi.llexternal(
    "gnitz_volatile_write_u64",
    [rffi.CCHARP, rffi.ULONGLONG],
    lltype.Void,
    compilation_info=_volatile_eci,
    _nowrapper=True,
)

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
FLAG_PRELOADED_EXCHANGE = 1024 # master pre-sends repartitioned batch before FLAG_PUSH
FLAG_BACKFILL          = 2048  # master asks worker to scan source and backfill a view
FLAG_TICK              = 4096  # master asks workers to run evaluate_dag on accumulated pending_deltas
FLAG_SCAN              = 0     # explicit name for "no flags" (no wire change)
FLAG_CHECKPOINT        = 8192  # 1 << 13 — worker signals all partitions flushed

# --- SAL constants ---
MAX_WORKERS = 64
# Group header: 32 fixed + MAX_WORKERS*4 offsets + MAX_WORKERS*4 sizes + 32 pad
GROUP_HEADER_SIZE = 576    # cache-line aligned
SAL_MMAP_SIZE   = 1 << 30  # 1 GB
W2M_REGION_SIZE = 1 << 30  # 1 GB per worker
W2M_HEADER_SIZE = 128      # 2 cache lines: write_cursor @ 0, padding to 64

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
        rb.begin(r_uint64(ci), r_uint64(0), r_int64(1))
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
    rb.begin(r_uint64(0), r_uint64(0), r_int64(1))          # msg_idx PK = 0
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
# SAL Data Structures
# ---------------------------------------------------------------------------


class SharedAppendLog(object):
    """Master-side handle for the file-backed shared append-only log."""
    _immutable_fields_ = ["ptr", "fd", "mmap_size"]

    def __init__(self, ptr, fd, mmap_size):
        self.ptr = ptr            # rffi.CCHARP — mmap base
        self.fd = fd              # file fd for fdatasync
        self.mmap_size = mmap_size
        self.write_cursor = 0     # master writes here
        self.lsn_counter = 0      # monotonic LSN


class W2MRegion(object):
    """Per-worker memfd-backed response channel (worker→master).

    Cursors are stored at fixed offsets in the shared mmap so both
    processes can see them. Layout:
      offset 0:  u64 write_cursor (worker updates, master reads)
      offset 64: padding (separate cache line)
      offset 128+: message data
    """
    _immutable_fields_ = ["ptr", "fd", "size"]

    def __init__(self, ptr, fd, size):
        self.ptr = ptr
        self.fd = fd
        self.size = size
        # Initialize write_cursor to W2M_HEADER_SIZE (no messages)
        _volatile_write_u64(ptr, rffi.cast(rffi.ULONGLONG, W2M_HEADER_SIZE))

    def get_write_cursor(self):
        return intmask(_volatile_read_u64(self.ptr))

    def set_write_cursor(self, val):
        _volatile_write_u64(self.ptr, rffi.cast(rffi.ULONGLONG, val))


class SALMessage(object):
    """Return type for read_worker_message."""

    def __init__(self):
        self.payload = None       # IPCPayload or None
        self.flags = 0            # group header flags (int)
        self.target_id = 0        # int
        self.lsn = r_uint64(0)
        self.advance = 0          # bytes to advance read_cursor


# ---------------------------------------------------------------------------
# Raw u64/u32 read/write helpers for mmap regions
# ---------------------------------------------------------------------------


def _write_u64_raw(ptr, byte_offset, val):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, byte_offset))
    p[0] = rffi.cast(rffi.ULONGLONG, val)


def _read_u64_raw(ptr, byte_offset):
    """Read u64 from shared memory via volatile load.

    Uses a C helper to ensure GCC does not cache the value in a register
    across iterations — critical for cross-process shared memory.
    """
    return rffi.cast(rffi.ULONGLONG,
                     _volatile_read_u64(rffi.ptradd(ptr, byte_offset)))


def _write_u32_raw(ptr, byte_offset, val):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, byte_offset))
    p[0] = rffi.cast(rffi.UINT, val)


def _read_u32_raw(ptr, byte_offset):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, byte_offset))
    return rffi.cast(rffi.UINT, p[0])


def _align8(n):
    return (n + 7) & ~7


# ---------------------------------------------------------------------------
# Serialize
# ---------------------------------------------------------------------------


def _encode_wire(target_id, client_id, zbatch, schema, flags,
                 seek_pk_lo, seek_pk_hi, seek_col_idx,
                 status, error_msg):
    """Encode an IPC message into a Buffer (control + optional schema + data).

    Returns a Buffer that the caller must free. Does NOT create a memfd.
    Used by both the memfd transport (external clients) and the SAL
    transport (internal master/worker).
    """
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
    except:
        if ctrl_batch is not None:
            ctrl_batch.free()
        if schema_batch is not None:
            schema_batch.free()
        wire_buf.free()
        raise

    if ctrl_batch is not None:
        ctrl_batch.free()
    if schema_batch is not None:
        schema_batch.free()

    return wire_buf


@jit.dont_look_inside
def serialize_to_memfd(target_id, client_id, zbatch, schema, flags,
                       seek_pk_lo, seek_pk_hi, seek_col_idx,
                       status, error_msg):
    wire_buf = _encode_wire(target_id, client_id, zbatch, schema, flags,
                            seek_pk_lo, seek_pk_hi, seek_col_idx,
                            status, error_msg)
    try:
        total_size = wire_buf.offset
        if total_size == 0:
            total_size = wal_layout.WAL_BLOCK_HEADER_SIZE  # defensive floor

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
        wire_buf.free()

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


def _parse_from_ptr(ptr, total_size):
    """Parse an IPC message from a raw pointer + length.

    Returns an IPCPayload with fd=-1 and ptr=nullptr (non-owning).
    The batch (if any) holds non-owning Buffer views into the source
    memory — caller must ensure the memory remains valid while the
    payload is in use.
    """
    payload = IPCPayload()

    if total_size < wal_layout.WAL_BLOCK_HEADER_SIZE:
        raise errors.StorageError("IPC payload too small")

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

    return payload


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
        inner = _parse_from_ptr(ptr, total_size)
        # Transfer parsed fields to the owning payload (which has fd + ptr)
        payload.batch        = inner.batch
        payload.schema       = inner.schema
        payload.target_id    = inner.target_id
        payload.client_id    = inner.client_id
        payload.flags        = inner.flags
        payload.seek_col_idx = inner.seek_col_idx
        payload.seek_pk_lo   = inner.seek_pk_lo
        payload.seek_pk_hi   = inner.seek_pk_hi
        payload.status       = inner.status
        payload.error_msg    = inner.error_msg
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


# ---------------------------------------------------------------------------
# SAL Transport (master→workers via file-backed mmap)
# ---------------------------------------------------------------------------


@jit.dont_look_inside
def write_message_group(sal, target_id, lsn, flags, worker_bufs, num_workers):
    """Append a message group for all N workers into the SAL.

    worker_bufs is a list of Buffer-or-None (one per worker). Each buffer
    contains an _encode_wire result. The caller must free worker_bufs after
    this returns. Does NOT fdatasync or signal — caller does that.
    """
    # Compute total size: 8-byte prefix + GROUP_HEADER_SIZE + per-worker data
    payload_size = GROUP_HEADER_SIZE
    for w in range(num_workers):
        wb = worker_bufs[w]
        if wb is not None and wb.offset > 0:
            payload_size += _align8(wb.offset)

    total = 8 + _align8(payload_size)
    if sal.write_cursor + total > sal.mmap_size:
        raise errors.StorageError("SAL full — checkpoint required")

    base = sal.write_cursor

    # 8-byte size prefix
    _write_u64_raw(sal.ptr, base, r_uint64(payload_size))

    # Group header at base + 8
    hdr_off = base + 8
    _write_u64_raw(sal.ptr, hdr_off + 0, r_uint64(payload_size))
    _write_u64_raw(sal.ptr, hdr_off + 8, r_uint64(lsn))
    _write_u32_raw(sal.ptr, hdr_off + 16, num_workers)
    _write_u32_raw(sal.ptr, hdr_off + 20, flags)
    _write_u32_raw(sal.ptr, hdr_off + 24, target_id)
    _write_u32_raw(sal.ptr, hdr_off + 28, 0)  # reserved

    # Worker offsets and sizes
    data_offset = GROUP_HEADER_SIZE  # relative to group start (hdr_off)
    for w in range(MAX_WORKERS):
        if w < num_workers:
            wb = worker_bufs[w]
            if wb is not None and wb.offset > 0:
                wsz = wb.offset
                _write_u32_raw(sal.ptr, hdr_off + 32 + w * 4, data_offset)
                _write_u32_raw(sal.ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, wsz)
                # Copy data
                buffer_ops.c_memmove(
                    rffi.cast(rffi.VOIDP, rffi.ptradd(sal.ptr, hdr_off + data_offset)),
                    rffi.cast(rffi.VOIDP, wb.base_ptr),
                    rffi.cast(rffi.SIZE_T, wsz),
                )
                data_offset += _align8(wsz)
            else:
                _write_u32_raw(sal.ptr, hdr_off + 32 + w * 4, 0)
                _write_u32_raw(sal.ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, 0)
        else:
            _write_u32_raw(sal.ptr, hdr_off + 32 + w * 4, 0)
            _write_u32_raw(sal.ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, 0)

    sal.write_cursor += total


@jit.dont_look_inside
def read_worker_message(sal_ptr, read_cursor, worker_id):
    """Read this worker's data from the next message group in the SAL.

    Returns a SALMessage. If the worker has no data in this group,
    msg.payload is None but msg.advance is still set (caller must
    advance read_cursor).
    """
    msg = SALMessage()

    # 8-byte size prefix
    payload_size = intmask(_read_u64_raw(sal_ptr, read_cursor))
    if payload_size == 0:
        return msg  # no message (advance=0)

    hdr_off = read_cursor + 8
    msg.lsn = _read_u64_raw(sal_ptr, hdr_off + 8)
    msg.flags = intmask(_read_u32_raw(sal_ptr, hdr_off + 20))
    msg.target_id = intmask(_read_u32_raw(sal_ptr, hdr_off + 24))
    msg.advance = 8 + _align8(payload_size)

    # Extract this worker's offset and size
    my_offset = intmask(_read_u32_raw(sal_ptr, hdr_off + 32 + worker_id * 4))
    my_size = intmask(_read_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + worker_id * 4))

    if my_size > 0 and my_offset > 0:
        data_ptr = rffi.ptradd(sal_ptr, hdr_off + my_offset)
        msg.payload = _parse_from_ptr(data_ptr, my_size)

    return msg


# ---------------------------------------------------------------------------
# W2M Transport (worker→master via memfd-backed shared region)
# ---------------------------------------------------------------------------


@jit.dont_look_inside
def write_to_w2m(region, target_id, zbatch, schema, flags,
                 seek_pk_lo, seek_pk_hi, seek_col_idx, status, error_msg):
    """Worker writes a response into its W2M region."""
    wire_buf = _encode_wire(target_id, 0, zbatch, schema, flags,
                            seek_pk_lo, seek_pk_hi, seek_col_idx,
                            status, error_msg)
    try:
        size = wire_buf.offset
        wc = region.get_write_cursor()

        total = 8 + _align8(size)
        if wc + total > region.size:
            raise errors.StorageError("W2M region full")

        # 8-byte size prefix
        _write_u64_raw(region.ptr, wc, r_uint64(size))

        # Copy wire data
        if size > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(region.ptr, wc + 8)),
                rffi.cast(rffi.VOIDP, wire_buf.base_ptr),
                rffi.cast(rffi.SIZE_T, size),
            )

        region.set_write_cursor(wc + total)
    finally:
        wire_buf.free()


@jit.dont_look_inside
def read_from_w2m(region, read_cursor):
    """Master reads the next response from a worker's W2M region.

    Returns (IPCPayload, new_read_cursor). The payload is non-owning
    (fd=-1, ptr=nullptr).
    """
    size = intmask(_read_u64_raw(region.ptr, read_cursor))
    if size == 0:
        raise errors.StorageError("W2M: unexpected zero-size message")

    data_ptr = rffi.ptradd(region.ptr, read_cursor + 8)
    payload = _parse_from_ptr(data_ptr, size)
    new_rc = read_cursor + 8 + _align8(size)
    return payload, new_rc
