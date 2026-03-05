# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint32, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import mmap_posix, buffer
from gnitz.server import ipc_ffi
from gnitz.core import errors, batch

# "GNITZIPC" in little-endian hex: 0x474E49545A495043
MAGIC_IPC = r_uint64(0x474E49545A495043)

# Header: 72 bytes
# [00-07] Magic
# [08-11] Status (u32)
# [12-15] Error String Length (u32)
# [16-23] Row Count (u64)
# [24-31] Target Entity ID (u64)
# [32-39] Client ID (u64)
# [40-47] Blob Arena Size (u64)
# [48-55] Num Columns (u64) — total schema columns including PK
# [56-63] PK Index (u64)
# [64-71] Reserved (u64)
HEADER_SIZE = 72
ALIGNMENT = 64
MAX_ERR_LEN = 65536

OFF_MAGIC = 0
OFF_STATUS = 8
OFF_ERR_LEN = 12
OFF_COUNT = 16
OFF_TARGET_ID = 24
OFF_CLIENT_ID = 32
OFF_BLOB_SZ = 40
OFF_NUM_COLS = 48
OFF_PK_INDEX = 56

# --- Status Codes ---
STATUS_OK = 0
STATUS_ERROR = 1



def align_up(val, align):
    u_val = r_uint64(val)
    u_align = r_uint64(align)
    return (u_val + u_align - 1) & ~(u_align - 1)


class IPCPayload(object):
    """
    Handle for a received IPC segment.
    The Server Loop uses this to reconstruct the batch and manage physical cleanup.
    """

    _immutable_fields_ = [
        "total_size",
        "status",
        "error_msg",
        "batch",
        "target_id",
        "client_id",
    ]

    def __init__(self, fd, ptr, total_size, status, error_msg, batch_obj, target_id, client_id):
        self.fd = fd
        self.ptr = ptr
        self.total_size = r_uint64(total_size)
        self.status = status
        self.error_msg = error_msg
        self.batch = batch_obj
        self.target_id = target_id
        self.client_id = client_id

    def close(self):
        """Physical cleanup of the mapped segment."""
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, intmask(self.total_size))
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


def _compute_col_sizes(schema, count):
    """Returns list of byte sizes for each column buffer (0 for PK column)."""
    num_cols = len(schema.columns)
    sizes = newlist_hint(num_cols)
    for i in range(num_cols):
        if i == schema.pk_index:
            sizes.append(0)
        else:
            sizes.append(schema.columns[i].field_type.size * count)
    return sizes


@jit.dont_look_inside
def serialize_to_memfd(target_id, zbatch=None, status=0, error_msg="", client_id=0):
    """
    Serializes a columnar batch into a shared-memory file descriptor.

    Wire format (columnar):
      [Header 72B]
      [Error string (aligned to 64)]
      [PK Lo buffer — count * 8]
      [PK Hi buffer — count * 8]
      [Weight buffer — count * 8]
      [Null buffer — count * 8]
      [Col 0 buffer — count * stride] (skipping PK col)
      [Col 1 buffer — count * stride]
      ...
      [Blob arena]
    Each section is aligned to ALIGNMENT (64).
    """
    err_len = r_uint64(len(error_msg))
    count = intmask(zbatch.length()) if zbatch else 0
    blob_sz = r_uint64(zbatch.blob_arena.offset) if zbatch else r_uint64(0)

    schema = zbatch._schema if zbatch else None
    num_cols = intmask(len(schema.columns)) if schema else 0

    # Calculate total size
    cur_off = align_up(r_uint64(HEADER_SIZE) + err_len, ALIGNMENT)

    if zbatch and count > 0:
        pk_lo_sz = r_uint64(count * 8)
        pk_hi_sz = r_uint64(count * 8)
        weight_sz = r_uint64(count * 8)
        null_sz = r_uint64(count * 8)

        pk_lo_off = cur_off
        cur_off = align_up(cur_off + pk_lo_sz, ALIGNMENT)
        pk_hi_off = cur_off
        cur_off = align_up(cur_off + pk_hi_sz, ALIGNMENT)
        weight_off = cur_off
        cur_off = align_up(cur_off + weight_sz, ALIGNMENT)
        null_off = cur_off
        cur_off = align_up(cur_off + null_sz, ALIGNMENT)

        col_offsets = newlist_hint(num_cols)
        col_sizes = newlist_hint(num_cols)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                col_offsets.append(r_uint64(0))
                col_sizes.append(r_uint64(0))
            else:
                sz = r_uint64(schema.columns[ci].field_type.size * count)
                col_offsets.append(cur_off)
                col_sizes.append(sz)
                cur_off = align_up(cur_off + sz, ALIGNMENT)

        blob_off = cur_off
        total_size = cur_off + blob_sz
    else:
        pk_lo_off = cur_off
        pk_hi_off = cur_off
        weight_off = cur_off
        null_off = cur_off
        col_offsets = newlist_hint(0)
        col_sizes = newlist_hint(0)
        blob_off = cur_off
        total_size = cur_off

    fd = -1
    ptr = lltype.nullptr(rffi.CCHARP.TO)
    try:
        fd = mmap_posix.memfd_create_c("gnitz_ipc")
        mmap_posix.ftruncate_c(fd, intmask(total_size))

        ptr = mmap_posix.mmap_file(
            fd,
            intmask(total_size),
            prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
            flags=mmap_posix.MAP_SHARED,
        )

        # Write Header
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_MAGIC))[0] = MAGIC_IPC
        rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0] = r_uint32(status)
        rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0] = r_uint32(err_len)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_COUNT))[0] = r_uint64(count)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_TARGET_ID))[0] = r_uint64(target_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_CLIENT_ID))[0] = r_uint64(client_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_BLOB_SZ))[0] = blob_sz
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_NUM_COLS))[0] = r_uint64(num_cols)
        pk_idx_val = r_uint64(schema.pk_index) if schema else r_uint64(0)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_PK_INDEX))[0] = pk_idx_val

        # Write Error String
        if err_len > 0:
            for i in range(intmask(err_len)):
                ptr[HEADER_SIZE + i] = error_msg[i]

        # Write column buffers
        if zbatch and count > 0:
            _copy_buf(ptr, pk_lo_off, zbatch.pk_lo_buf, count * 8)
            _copy_buf(ptr, pk_hi_off, zbatch.pk_hi_buf, count * 8)
            _copy_buf(ptr, weight_off, zbatch.weight_buf, count * 8)
            _copy_buf(ptr, null_off, zbatch.null_buf, count * 8)

            for ci in range(num_cols):
                if ci == schema.pk_index:
                    continue
                sz = intmask(col_sizes[ci])
                if sz > 0:
                    _copy_buf(ptr, col_offsets[ci], zbatch.col_bufs[ci], sz)

            if blob_sz > 0:
                _copy_buf(ptr, blob_off, zbatch.blob_arena, intmask(blob_sz))

        mmap_posix.munmap_file(ptr, intmask(total_size))
        return fd

    except Exception as e:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)
        raise e


def _copy_buf(dest_base, dest_off, src_buf, nbytes):
    """Copies nbytes from src_buf.base_ptr to dest_base + dest_off."""
    if nbytes > 0 and src_buf.base_ptr:
        buffer.c_memmove(
            rffi.cast(rffi.VOIDP, rffi.ptradd(dest_base, intmask(dest_off))),
            rffi.cast(rffi.VOIDP, src_buf.base_ptr),
            rffi.cast(rffi.SIZE_T, nbytes),
        )


@jit.dont_look_inside
def send_batch(sock_fd, target_id, zbatch, status=0, error_msg="", client_id=0):
    """
    Synchronous send: Serializes and transmits a single batch.
    """
    memfd = serialize_to_memfd(target_id, zbatch, status, error_msg, client_id)
    try:
        if ipc_ffi.send_fd(sock_fd, memfd) < 0:
            raise errors.StorageError("Failed to transmit IPC segment (Disconnected)")
    finally:
        os.close(memfd)


def send_error(sock_fd, error_msg, target_id=0, client_id=0):
    """Convenience helper to send an error response."""
    send_batch(sock_fd, target_id, None, status=STATUS_ERROR, error_msg=error_msg, client_id=client_id)


@jit.dont_look_inside
def receive_payload(sock_fd, registry):
    """
    Receives an IPC segment and reconstructs metadata and batch view.
    """
    fd = ipc_ffi.recv_fd(sock_fd)
    if fd < 0:
        raise errors.StorageError("Failed to receive IPC descriptor (Socket closed)")

    ptr = lltype.nullptr(rffi.CCHARP.TO)
    total_size = r_uint64(0)
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
        if magic != MAGIC_IPC:
            raise errors.StorageError("Invalid IPC Magic")

        # Parse Header
        status = intmask(rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0])
        raw_err_len = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0]
        err_len = r_uint64(raw_err_len)
        count = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_COUNT))[0])
        target_id = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_TARGET_ID))[0]
        client_id = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_CLIENT_ID))[0]
        blob_sz = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_BLOB_SZ))[0])

        if err_len > r_uint64(MAX_ERR_LEN):
            raise errors.StorageError("Error message length exceeds safety limit")

        if r_uint64(HEADER_SIZE) + err_len > total_size:
            raise errors.StorageError("Truncated IPC (Error string boundary)")

        error_msg = ""
        if err_len > 0:
            error_msg = rffi.charpsize2str(
                rffi.ptradd(ptr, HEADER_SIZE), intmask(err_len)
            )

        zbatch = None
        tid = intmask(target_id)

        if count > 0:
            if not registry.has_id(tid):
                raise errors.StorageError(
                    "IPC routing failed: Target ID %d not found in registry" % tid
                )

            family = registry.get_by_id(tid)
            schema = family.get_schema()
            num_cols = len(schema.columns)

            cur_off = align_up(r_uint64(HEADER_SIZE) + err_len, ALIGNMENT)

            pk_lo_sz = count * 8
            pk_lo_off = cur_off
            cur_off = align_up(cur_off + r_uint64(pk_lo_sz), ALIGNMENT)

            pk_hi_off = cur_off
            cur_off = align_up(cur_off + r_uint64(count * 8), ALIGNMENT)

            weight_off = cur_off
            cur_off = align_up(cur_off + r_uint64(count * 8), ALIGNMENT)

            null_off = cur_off
            cur_off = align_up(cur_off + r_uint64(count * 8), ALIGNMENT)

            if cur_off > total_size:
                raise errors.StorageError("Truncated IPC (column buffer boundary)")

            pk_lo_buf = buffer.Buffer.from_external_ptr(
                rffi.ptradd(ptr, intmask(pk_lo_off)), pk_lo_sz
            )
            pk_lo_buf.offset = pk_lo_sz
            pk_hi_buf = buffer.Buffer.from_external_ptr(
                rffi.ptradd(ptr, intmask(pk_hi_off)), count * 8
            )
            pk_hi_buf.offset = count * 8
            weight_buf = buffer.Buffer.from_external_ptr(
                rffi.ptradd(ptr, intmask(weight_off)), count * 8
            )
            weight_buf.offset = count * 8
            null_buf = buffer.Buffer.from_external_ptr(
                rffi.ptradd(ptr, intmask(null_off)), count * 8
            )
            null_buf.offset = count * 8

            col_bufs = newlist_hint(num_cols)
            col_strides = newlist_hint(num_cols)
            for ci in range(num_cols):
                if ci == schema.pk_index:
                    col_bufs.append(buffer.Buffer(0))
                    col_strides.append(0)
                else:
                    col_sz = schema.columns[ci].field_type.size * count
                    cb = buffer.Buffer.from_external_ptr(
                        rffi.ptradd(ptr, intmask(cur_off)), col_sz
                    )
                    cb.offset = col_sz
                    col_bufs.append(cb)
                    col_strides.append(schema.columns[ci].field_type.size)
                    cur_off = align_up(cur_off + r_uint64(col_sz), ALIGNMENT)

            blob_buf = buffer.Buffer.from_external_ptr(
                rffi.ptradd(ptr, intmask(cur_off)), intmask(blob_sz)
            )
            blob_buf.offset = intmask(blob_sz)

            zbatch = batch.ArenaZSetBatch.from_buffers(
                schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
                col_bufs, col_strides, blob_buf, count, is_sorted=True
            )

        return IPCPayload(fd, ptr, total_size, status, error_msg, zbatch, tid, intmask(client_id))

    except Exception as e:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)
        raise e
