# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint32, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import mmap_posix, buffer
from gnitz.server import ipc_ffi
from gnitz.core import errors, batch

# "GNITZIPC" in little-endian hex
MAGIC_IPC = r_uint64(0x474E49545A495043)

HEADER_SIZE = 40
ALIGNMENT = 64
MAX_ERR_LEN = 65536  # Safety limit for error messages

OFF_MAGIC = 0
OFF_STATUS = 8
OFF_ERR_LEN = 12
OFF_PRIMARY_SZ = 16
OFF_BLOB_SZ = 24
OFF_COUNT = 32


def align_up(val, align):
    u_val = r_uint64(val)
    u_align = r_uint64(align)
    return (u_val + u_align - 1) & ~(u_align - 1)


class IPCPayload(object):
    """
    Handle for a received IPC segment.
    The Server Loop uses this to reconstruct the batch and manage cleanup.
    """
    _immutable_fields_ = ["fd", "ptr", "total_size", "status", "error_msg", "batch"]

    def __init__(self, fd, ptr, total_size, status, error_msg, batch_obj):
        self.fd = fd
        self.ptr = ptr
        self.total_size = r_uint64(total_size)
        self.status = status
        self.error_msg = error_msg
        self.batch = batch_obj

    def close(self):
        """Physical cleanup of the mapped segment."""
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, intmask(self.total_size))
        if self.fd >= 0:
            os.close(self.fd)


@jit.dont_look_inside
def send_batch(sock_fd, zbatch, status=0, error_msg=""):
    """
    Serializes a batch into a memfd and sends it over the socket.
    zbatch can be None for metadata-only responses.
    """
    err_len = r_uint64(len(error_msg))
    primary_sz = r_uint64(zbatch.primary_arena.offset) if zbatch else r_uint64(0)
    blob_sz = r_uint64(zbatch.blob_arena.offset) if zbatch else r_uint64(0)
    count = r_uint64(zbatch.length()) if zbatch else r_uint64(0)

    # 1. Calculate strictly aligned offsets
    err_str_off = r_uint64(HEADER_SIZE)
    primary_off = align_up(err_str_off + err_len, ALIGNMENT)
    blob_off = align_up(primary_off + primary_sz, ALIGNMENT)
    total_size = blob_off + blob_sz

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

        # 2. Write Header
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_MAGIC))[0] = MAGIC_IPC
        rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0] = r_uint32(status)
        rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0] = r_uint32(err_len)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_PRIMARY_SZ))[0] = primary_sz
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_BLOB_SZ))[0] = blob_sz
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_COUNT))[0] = count

        # 3. Write Error String
        if err_len > 0:
            for i in range(intmask(err_len)):
                ptr[intmask(err_str_off) + i] = error_msg[i]

        # 4. Copy Arenas
        if zbatch:
            if primary_sz > 0:
                buffer.c_memmove(
                    rffi.cast(rffi.VOIDP, rffi.ptradd(ptr, intmask(primary_off))),
                    rffi.cast(rffi.VOIDP, zbatch.primary_arena.base_ptr),
                    rffi.cast(rffi.SIZE_T, primary_sz),
                )
            if blob_sz > 0:
                buffer.c_memmove(
                    rffi.cast(rffi.VOIDP, rffi.ptradd(ptr, intmask(blob_off))),
                    rffi.cast(rffi.VOIDP, zbatch.blob_arena.base_ptr),
                    rffi.cast(rffi.SIZE_T, blob_sz),
                )

        mmap_posix.munmap_file(ptr, intmask(total_size))
        ptr = lltype.nullptr(rffi.CCHARP.TO)

        if ipc_ffi.send_fd(sock_fd, fd) < 0:
            raise errors.StorageError("Failed to send memfd")

    finally:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)


def send_error(sock_fd, error_msg):
    """Lightweight helper to send an error without instantiating a batch object."""
    send_batch(sock_fd, None, status=1, error_msg=error_msg)


@jit.dont_look_inside
def receive_payload(sock_fd, schema):
    """
    Receives an IPC segment and reconstructs the metadata and batch view.
    """
    fd = ipc_ffi.recv_fd(sock_fd)
    if fd < 0:
        raise errors.StorageError("Failed to receive memfd")

    ptr = lltype.nullptr(rffi.CCHARP.TO)
    total_size = r_uint64(0)  # <--- Fix: Initialize for the Flow Object Space
    try:
        total_size = r_uint64(mmap_posix.fget_size(fd))
        if total_size < r_uint64(HEADER_SIZE):
            raise errors.StorageError("IPC payload too small for header")

        ptr = mmap_posix.mmap_file(
            fd, intmask(total_size), prot=mmap_posix.PROT_READ, flags=mmap_posix.MAP_SHARED
        )

        # 1. Verify Magic
        magic = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_MAGIC))[0]
        if magic != MAGIC_IPC:
            raise errors.StorageError("Invalid IPC Magic")

        # 2. Parse and Validate Header (using r_uint64 to prevent overflows)
        status = intmask(rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0])
        err_len = rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0])
        primary_sz = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_PRIMARY_SZ))[0]
        blob_sz = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_BLOB_SZ))[0]
        count = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_COUNT))[0]

        # Validation: Bounds and Arithmetic Overflows
        if err_len > r_uint64(MAX_ERR_LEN):
            raise errors.StorageError("Error message too long")

        # Ensure err_str fits
        if r_uint64(HEADER_SIZE) + err_len > total_size:
            raise errors.StorageError("Truncated IPC (error string)")

        error_msg = ""
        if err_len > 0:
            error_msg = rffi.charpsize2str(rffi.ptradd(ptr, HEADER_SIZE), intmask(err_len))

        # Check Primary Arena bounds
        primary_off = align_up(r_uint64(HEADER_SIZE) + err_len, ALIGNMENT)
        if primary_off > total_size or primary_sz > (total_size - primary_off):
            raise errors.StorageError("Truncated IPC (primary arena)")

        # Check Blob Arena bounds
        blob_off = align_up(primary_off + primary_sz, ALIGNMENT)
        if blob_off > total_size or blob_sz > (total_size - blob_off):
            raise errors.StorageError("Truncated IPC (blob arena)")

        # 3. Slice and Reconstruct Batch
        primary_buf = buffer.Buffer.from_external_ptr(
            rffi.ptradd(ptr, intmask(primary_off)), intmask(primary_sz)
        )
        blob_buf = buffer.Buffer.from_external_ptr(
            rffi.ptradd(ptr, intmask(blob_off)), intmask(blob_sz)
        )
        blob_buf.offset = intmask(blob_sz)

        zbatch = batch.ArenaZSetBatch.from_buffers(
            schema, primary_buf, blob_buf, intmask(count), is_sorted=True
        )

        return IPCPayload(fd, ptr, total_size, status, error_msg, zbatch)

    except Exception as e:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        os.close(fd)
        raise e
