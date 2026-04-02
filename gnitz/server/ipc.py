# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import mmap_posix
from gnitz.storage import buffer as buffer_ops
from gnitz.storage import engine_ffi
from gnitz.server import ipc_ffi
from gnitz.core import errors, batch, types
from gnitz.catalog import system_tables

# Atomics from Rust (ipc.rs via engine_ffi)
atomic_load_u64 = engine_ffi._atomic_load_u64
atomic_store_u64 = engine_ffi._atomic_store_u64

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
FLAG_FLUSH             = 16384 # 1 << 14 — master asks workers to flush all families

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
# IPCPayload
# ---------------------------------------------------------------------------


class IPCPayload(object):
    def __init__(self):
        self.fd           = -1
        self.ptr          = lltype.nullptr(rffi.CCHARP.TO)
        self.total_size   = r_uint64(0)
        self.raw_buf      = lltype.nullptr(rffi.CCHARP.TO)
        self.raw_buf_size = 0
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
        if self.raw_buf:
            lltype.free(self.raw_buf, flavor='raw')
            self.raw_buf = lltype.nullptr(rffi.CCHARP.TO)
            self.raw_buf_size = 0
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
        self.epoch = 1            # bumped on each checkpoint reset


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
        atomic_store_u64(ptr, rffi.cast(rffi.ULONGLONG, W2M_HEADER_SIZE))

    def get_write_cursor(self):
        return intmask(atomic_load_u64(self.ptr))

    def set_write_cursor(self, val):
        atomic_store_u64(self.ptr, rffi.cast(rffi.ULONGLONG, val))


class SALMessage(object):
    """Return type for read_worker_message."""

    def __init__(self):
        self.payload = None       # IPCPayload or None
        self.flags = 0            # group header flags (int)
        self.target_id = 0        # int
        self.lsn = r_uint64(0)
        self.advance = 0          # bytes to advance read_cursor
        self.epoch = 0            # group header epoch (for checkpoint fencing)


# ---------------------------------------------------------------------------
# Raw u64/u32 read/write helpers for mmap regions
# ---------------------------------------------------------------------------


def _write_u64_raw(ptr, byte_offset, val):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, byte_offset))
    p[0] = rffi.cast(rffi.ULONGLONG, val)


def read_u64_raw(ptr, byte_offset):
    """Read u64 from shared memory via atomic acquire load."""
    return rffi.cast(rffi.ULONGLONG,
                     atomic_load_u64(rffi.ptradd(ptr, byte_offset)))


def _write_u32_raw(ptr, byte_offset, val):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, byte_offset))
    p[0] = rffi.cast(rffi.UINT, val)


def _read_u32_raw(ptr, byte_offset):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, byte_offset))
    return rffi.cast(rffi.UINT, p[0])


def _decode_u32_le(buf):
    """Decode a little-endian u32 from a 4-byte char buffer."""
    return (
        (ord(buf[0]) & 0xFF)
        | ((ord(buf[1]) & 0xFF) << 8)
        | ((ord(buf[2]) & 0xFF) << 16)
        | ((ord(buf[3]) & 0xFF) << 24)
    )


def _make_payload_from_raw(raw, raw_len):
    """Parse raw wire bytes into an IPCPayload that owns the buffer."""
    try:
        inner = _parse_from_ptr(raw, raw_len)
    except Exception as e:
        lltype.free(raw, flavor='raw')
        raise e
    payload = IPCPayload()
    payload.raw_buf      = raw
    payload.raw_buf_size = raw_len
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
    return payload


def _align8(n):
    return (n + 7) & ~7


# ---------------------------------------------------------------------------
# Serialize
# ---------------------------------------------------------------------------


def _encode_wire(target_id, client_id, zbatch, schema, flags,
                 seek_pk_lo, seek_pk_hi, seek_col_idx,
                 status, error_msg):
    """Encode an IPC message via Rust (gnitz_ipc_encode_wire).

    Returns a Buffer that the caller must free. Does NOT create a memfd.
    Used by both the memfd transport (external clients) and the SAL
    transport (internal master/worker).
    """
    # Prepare schema descriptor if needed
    eff_schema = schema
    if eff_schema is None and zbatch is not None:
        eff_schema = zbatch._schema

    schema_buf = lltype.nullptr(rffi.CCHARP.TO)
    c_name_ptrs = lltype.nullptr(rffi.CCHARPP.TO)
    c_name_lens = lltype.nullptr(rffi.UINTP.TO)
    num_names = 0

    try:
        if eff_schema is not None:
            schema_buf = engine_ffi.pack_schema(eff_schema)
            # Pack column names
            num_names = len(eff_schema.columns)
            c_name_ptrs = lltype.malloc(rffi.CCHARPP.TO, num_names, flavor='raw')
            c_name_lens = lltype.malloc(rffi.UINTP.TO, num_names, flavor='raw')
            for ci in range(num_names):
                name = eff_schema.columns[ci].name
                nlen = len(name)
                if nlen > 0:
                    name_raw = lltype.malloc(rffi.CCHARP.TO, nlen, flavor='raw')
                    for j in range(nlen):
                        name_raw[j] = name[j]
                    c_name_ptrs[ci] = name_raw
                else:
                    c_name_ptrs[ci] = lltype.nullptr(rffi.CCHARP.TO)
                c_name_lens[ci] = rffi.cast(rffi.UINT, nlen)

        # Prepare error message
        error_buf = lltype.nullptr(rffi.CCHARP.TO)
        error_len = len(error_msg)
        if error_len > 0:
            error_buf = lltype.malloc(rffi.CCHARP.TO, error_len, flavor='raw')
            for j in range(error_len):
                error_buf[j] = error_msg[j]

        # Batch handle
        batch_handle = rffi.cast(rffi.VOIDP, 0)
        if zbatch is not None and zbatch.length() > 0:
            batch_handle = zbatch._handle

        # Output pointers
        out_ptr = lltype.malloc(rffi.CCHARPP.TO, 1, flavor='raw')
        out_len = lltype.malloc(rffi.UINTP.TO, 1, flavor='raw')

        try:
            rc = engine_ffi._ipc_encode_wire(
                rffi.cast(rffi.ULONGLONG, r_uint64(target_id)),
                rffi.cast(rffi.ULONGLONG, r_uint64(client_id)),
                rffi.cast(rffi.ULONGLONG, r_uint64(flags)),
                rffi.cast(rffi.ULONGLONG, r_uint64(seek_pk_lo)),
                rffi.cast(rffi.ULONGLONG, r_uint64(seek_pk_hi)),
                rffi.cast(rffi.ULONGLONG, r_uint64(seek_col_idx)),
                rffi.cast(rffi.UINT, status),
                error_buf, rffi.cast(rffi.UINT, error_len),
                rffi.cast(rffi.VOIDP, schema_buf),
                c_name_ptrs, c_name_lens,
                rffi.cast(rffi.UINT, num_names),
                batch_handle,
                out_ptr, out_len)

            if rffi.cast(lltype.Signed, rc) < 0:
                raise errors.StorageError("IPC encode_wire failed")

            wire_ptr = out_ptr[0]
            wire_size = intmask(rffi.cast(lltype.Signed, out_len[0]))
        finally:
            lltype.free(out_ptr, flavor='raw')
            lltype.free(out_len, flavor='raw')

        if error_len > 0:
            lltype.free(error_buf, flavor='raw')

        # Wrap in a Buffer for callers. Copy the Rust-allocated bytes into a
        # Buffer and free the Rust allocation.
        result = buffer_ops.Buffer(wire_size)
        if wire_size > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, result.base_ptr),
                rffi.cast(rffi.VOIDP, wire_ptr),
                rffi.cast(rffi.SIZE_T, wire_size),
            )
            result.offset = wire_size
        engine_ffi._ipc_wire_free(wire_ptr, rffi.cast(rffi.UINT, wire_size))
        return result
    finally:
        if schema_buf:
            # Free name buffers
            for ci in range(num_names):
                if c_name_ptrs[ci]:
                    lltype.free(c_name_ptrs[ci], flavor='raw')
            if c_name_ptrs:
                lltype.free(c_name_ptrs, flavor='raw')
            if c_name_lens:
                lltype.free(c_name_lens, flavor='raw')
            lltype.free(schema_buf, flavor='raw')


@jit.dont_look_inside
def send_framed(sock_fd, target_id, zbatch, status=0, error_msg="",
                client_id=0, schema=None, flags=0,
                seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
    """Send a length-prefixed IPC message over SOCK_STREAM."""
    eff_schema = schema
    if eff_schema is None and zbatch is not None:
        eff_schema = zbatch._schema
    wire_buf = _encode_wire(target_id, client_id, zbatch,
                            eff_schema, flags, seek_pk_lo, seek_pk_hi,
                            seek_col_idx, status, error_msg)
    try:
        if ipc_ffi.send_framed(sock_fd, wire_buf.base_ptr, wire_buf.offset) < 0:
            raise errors.StorageError("Failed to send framed IPC message (Disconnected)")
    finally:
        wire_buf.free()


def send_framed_error(sock_fd, error_msg, target_id=0, client_id=0):
    """Convenience helper to send an error response over SOCK_STREAM."""
    send_framed(
        sock_fd, target_id, None,
        status=STATUS_ERROR, error_msg=error_msg, client_id=client_id,
    )


@jit.dont_look_inside
def recv_framed(sock_fd):
    """Blocking receive of a length-prefixed IPC message.
    Returns an IPCPayload owning the raw buffer."""
    hdr_buf = lltype.malloc(rffi.CCHARP.TO, 4, flavor='raw')
    try:
        if ipc_ffi.recv_exact(sock_fd, hdr_buf, 4) < 0:
            raise errors.ClientDisconnectedError("recv_framed: header read failed")
        payload_len = _decode_u32_le(hdr_buf)
    finally:
        lltype.free(hdr_buf, flavor='raw')

    if payload_len == 0:
        raise errors.ClientDisconnectedError("recv_framed: zero-length (close sentinel)")

    raw = lltype.malloc(rffi.CCHARP.TO, payload_len, flavor='raw')
    if ipc_ffi.recv_exact(sock_fd, raw, payload_len) < 0:
        lltype.free(raw, flavor='raw')
        raise errors.ClientDisconnectedError("recv_framed: payload read failed")
    return _make_payload_from_raw(raw, payload_len)


# ---------------------------------------------------------------------------
# Receive
# ---------------------------------------------------------------------------


def _parse_from_ptr(ptr, total_size):
    """Parse an IPC message from a raw pointer + length via Rust.

    Returns an IPCPayload with fd=-1 and ptr=nullptr (non-owning).
    The batch (if any) is a Rust-owned OwnedBatch.
    """
    payload = IPCPayload()

    if total_size < 48:  # WAL block header size
        raise errors.StorageError("IPC payload too small")

    # Allocate output buffers
    out_status     = lltype.malloc(rffi.UINTP.TO, 1, flavor='raw')
    out_client_id  = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor='raw')
    out_target_id  = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor='raw')
    out_flags      = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor='raw')
    out_seek_pk_lo = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor='raw')
    out_seek_pk_hi = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor='raw')
    out_seek_col   = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor='raw')
    out_err_ptr    = lltype.malloc(rffi.CCHARPP.TO, 1, flavor='raw')
    out_err_len    = lltype.malloc(rffi.UINTP.TO, 1, flavor='raw')
    out_schema_buf = lltype.malloc(rffi.CCHARP.TO, engine_ffi.SCHEMA_DESC_SIZE, flavor='raw')
    out_has_schema = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')
    out_batch      = lltype.malloc(rffi.VOIDPP.TO, 1, flavor='raw')
    out_num_names  = lltype.malloc(rffi.UINTP.TO, 1, flavor='raw')

    try:
        handle = engine_ffi._ipc_decode_wire(
            ptr, rffi.cast(rffi.UINT, total_size),
            out_status, out_client_id,
            out_target_id, out_flags,
            out_seek_pk_lo, out_seek_pk_hi,
            out_seek_col,
            out_err_ptr, out_err_len,
            rffi.cast(rffi.VOIDP, out_schema_buf), out_has_schema,
            out_batch, out_num_names)

        if not handle:
            raise errors.StorageError("IPC decode_wire failed")

        payload.status       = intmask(rffi.cast(lltype.Signed, out_status[0]))
        payload.client_id    = intmask(out_client_id[0])
        payload.target_id    = intmask(out_target_id[0])
        payload.flags        = r_uint64(out_flags[0])
        payload.seek_pk_lo   = intmask(out_seek_pk_lo[0])
        payload.seek_pk_hi   = intmask(out_seek_pk_hi[0])
        payload.seek_col_idx = intmask(out_seek_col[0])

        err_len = intmask(rffi.cast(lltype.Signed, out_err_len[0]))
        if err_len > 0 and out_err_ptr[0]:
            payload.error_msg = rffi.charpsize2str(out_err_ptr[0], err_len)

        # Schema
        has_schema = intmask(rffi.cast(lltype.Signed, out_has_schema[0]))
        if has_schema:
            # Reconstruct TableSchema from SchemaDescriptor + column names
            sd_ptr = out_schema_buf
            num_cols = intmask(rffi.cast(lltype.Signed,
                rffi.cast(rffi.UINTP, sd_ptr)[0]))
            pk_idx = intmask(rffi.cast(lltype.Signed,
                rffi.cast(rffi.UINTP, rffi.ptradd(sd_ptr, 4))[0]))
            num_names = intmask(rffi.cast(lltype.Signed, out_num_names[0]))

            columns = []
            for ci in range(num_cols):
                base = 8 + ci * 4
                tc = ord(sd_ptr[base])
                sz = ord(sd_ptr[base + 1])
                nl = ord(sd_ptr[base + 2])

                # Get column name from decode result
                name = ""
                if ci < num_names:
                    cn_ptr = lltype.malloc(rffi.CCHARPP.TO, 1, flavor='raw')
                    cn_len = lltype.malloc(rffi.UINTP.TO, 1, flavor='raw')
                    engine_ffi._ipc_decode_result_col_name(
                        handle, rffi.cast(rffi.UINT, ci),
                        cn_ptr, cn_len)
                    nlen = intmask(rffi.cast(lltype.Signed, cn_len[0]))
                    if nlen > 0 and cn_ptr[0]:
                        name = rffi.charpsize2str(cn_ptr[0], nlen)
                    lltype.free(cn_ptr, flavor='raw')
                    lltype.free(cn_len, flavor='raw')

                field_type = system_tables.type_code_to_field_type(tc)
                columns.append(types.ColumnDefinition(
                    field_type, is_nullable=bool(nl), name=name))

            payload.schema = types.TableSchema(columns, pk_index=pk_idx)

        # Data batch
        batch_handle = out_batch[0]
        if batch_handle:
            # Take ownership of the batch from the decode result
            taken = engine_ffi._ipc_decode_result_take_batch(handle)
            if taken:
                payload.batch = batch.ArenaZSetBatch._wrap_handle(
                    payload.schema, taken,
                    bool(payload.flags & FLAG_BATCH_SORTED),
                    bool(payload.flags & FLAG_BATCH_CONSOLIDATED))

        engine_ffi._ipc_decode_result_free(handle)
    finally:
        lltype.free(out_status, flavor='raw')
        lltype.free(out_client_id, flavor='raw')
        lltype.free(out_target_id, flavor='raw')
        lltype.free(out_flags, flavor='raw')
        lltype.free(out_seek_pk_lo, flavor='raw')
        lltype.free(out_seek_pk_hi, flavor='raw')
        lltype.free(out_seek_col, flavor='raw')
        lltype.free(out_err_ptr, flavor='raw')
        lltype.free(out_err_len, flavor='raw')
        lltype.free(out_schema_buf, flavor='raw')
        lltype.free(out_has_schema, flavor='raw')
        lltype.free(out_batch, flavor='raw')
        lltype.free(out_num_names, flavor='raw')

    return payload


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
    payload_size = GROUP_HEADER_SIZE
    for w in range(num_workers):
        wb = worker_bufs[w]
        if wb is not None and wb.offset > 0:
            payload_size += _align8(wb.offset)

    total = 8 + _align8(payload_size)
    if sal.write_cursor + total > sal.mmap_size:
        raise errors.StorageError("SAL full — checkpoint required")

    base = sal.write_cursor
    hdr_off = base + 8
    _write_u64_raw(sal.ptr, hdr_off + 0, r_uint64(payload_size))
    _write_u64_raw(sal.ptr, hdr_off + 8, r_uint64(lsn))
    _write_u32_raw(sal.ptr, hdr_off + 16, num_workers)
    _write_u32_raw(sal.ptr, hdr_off + 20, flags)
    _write_u32_raw(sal.ptr, hdr_off + 24, target_id)
    _write_u32_raw(sal.ptr, hdr_off + 28, sal.epoch)

    data_offset = GROUP_HEADER_SIZE
    for w in range(num_workers):
        wb = worker_bufs[w]
        if wb is not None and wb.offset > 0:
            wsz = wb.offset
            _write_u32_raw(sal.ptr, hdr_off + 32 + w * 4, data_offset)
            _write_u32_raw(sal.ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, wsz)
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(sal.ptr, hdr_off + data_offset)),
                rffi.cast(rffi.VOIDP, wb.base_ptr),
                rffi.cast(rffi.SIZE_T, wsz),
            )
            data_offset += _align8(wsz)
        else:
            _write_u32_raw(sal.ptr, hdr_off + 32 + w * 4, 0)
            _write_u32_raw(sal.ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, 0)

    # Size prefix written LAST via atomic release store — workers use this
    # as the "data ready" sentinel.
    atomic_store_u64(
        rffi.ptradd(sal.ptr, base),
        rffi.cast(rffi.ULONGLONG, payload_size))

    sal.write_cursor += total


@jit.dont_look_inside
def read_worker_message(sal_ptr, read_cursor, worker_id):
    """Read this worker's data from the next message group in the SAL.

    Returns a SALMessage. If the worker has no data in this group,
    msg.payload is None but msg.advance is still set (caller must
    advance read_cursor).
    """
    msg = SALMessage()

    # 8-byte size prefix (atomic acquire load)
    payload_size = intmask(read_u64_raw(sal_ptr, read_cursor))
    if payload_size == 0:
        return msg  # no message (advance=0)

    hdr_off = read_cursor + 8
    msg.lsn = read_u64_raw(sal_ptr, hdr_off + 8)
    msg.flags = intmask(_read_u32_raw(sal_ptr, hdr_off + 20))
    msg.target_id = intmask(_read_u32_raw(sal_ptr, hdr_off + 24))
    msg.epoch = intmask(_read_u32_raw(sal_ptr, hdr_off + 28))
    msg.advance = 8 + _align8(payload_size)

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
    """Worker writes a response into its W2M region.

    Delegates mmap write to Rust (gnitz_w2m_write).
    """
    wire_buf = _encode_wire(target_id, 0, zbatch, schema, flags,
                            seek_pk_lo, seek_pk_hi, seek_col_idx,
                            status, error_msg)
    try:
        size = wire_buf.offset
        new_wc = engine_ffi._w2m_write(
            region.ptr, wire_buf.base_ptr,
            rffi.cast(rffi.UINT, size),
            rffi.cast(rffi.ULONGLONG, region.size))
        if rffi.cast(lltype.Signed, new_wc) < 0:
            raise errors.StorageError("W2M region full")
    finally:
        wire_buf.free()


@jit.dont_look_inside
def read_from_w2m(region, read_cursor):
    """Master reads the next response from a worker's W2M region.

    Returns (IPCPayload, new_read_cursor). The payload is non-owning.
    Delegates mmap read to Rust (gnitz_w2m_read).
    """
    out_ptr = lltype.malloc(rffi.CCHARPP.TO, 1, flavor='raw')
    out_size = lltype.malloc(rffi.UINTP.TO, 1, flavor='raw')
    try:
        new_rc = engine_ffi._w2m_read(
            region.ptr,
            rffi.cast(rffi.ULONGLONG, read_cursor),
            out_ptr, out_size)
        data_size = intmask(rffi.cast(lltype.Signed, out_size[0]))
        if data_size == 0:
            raise errors.StorageError("W2M: unexpected zero-size message")
        data_ptr = out_ptr[0]
    finally:
        lltype.free(out_ptr, flavor='raw')
        lltype.free(out_size, flavor='raw')
    payload = _parse_from_ptr(data_ptr, data_size)
    return payload, intmask(new_rc)
