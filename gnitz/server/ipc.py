# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import mmap_posix
from gnitz.storage import engine_ffi
from gnitz.core import errors, types
from gnitz.storage import owned_batch
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
FLAG_SHUTDOWN = 4
FLAG_DDL_SYNC = 8
FLAG_EXCHANGE = 16
FLAG_PUSH = 32
FLAG_HAS_PK = 64
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


def read_u64_raw(ptr, byte_offset):
    """Read u64 from shared memory via atomic acquire load."""
    return rffi.cast(rffi.ULONGLONG,
                     atomic_load_u64(rffi.ptradd(ptr, byte_offset)))


def _read_u32_raw(ptr, byte_offset):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, byte_offset))
    return rffi.cast(rffi.UINT, p[0])


def _align8(n):
    return (n + 7) & ~7


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
                payload.batch = owned_batch.ArenaZSetBatch._wrap_handle(
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


