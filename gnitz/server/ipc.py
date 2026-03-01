# gnitz/server/ipc.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint32, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import mmap_posix, buffer
from gnitz.server import ipc_ffi
from gnitz.core import errors, batch

# "GNITZIPC" in little-endian hex: 0x474E49545A495043
MAGIC_IPC = r_uint64(0x474E49545A495043)

# Increased from 48 to 56 to accommodate Client ID
HEADER_SIZE = 56
ALIGNMENT = 64
MAX_ERR_LEN = 65536  # Safety limit for error messages

# Header Layout:
# [00-07] Magic
# [08-11] Status (u32)
# [12-15] Error String Length (u32)
# [16-23] Primary Arena Size (u64)
# [24-31] Blob Arena Size (u64)
# [32-39] Row Count (u64)
# [40-47] Target Entity ID (u64)
# [48-55] Client ID (u64)

OFF_MAGIC = 0
OFF_STATUS = 8
OFF_ERR_LEN = 12
OFF_PRIMARY_SZ = 16
OFF_BLOB_SZ = 24
OFF_COUNT = 32
OFF_TARGET_ID = 40
OFF_CLIENT_ID = 48

# --- Status Codes ---
STATUS_OK = 0
STATUS_ERROR = 1

# --- Yield Reason Codes ---
# Mirroring runtime.py for protocol interpretation
YIELD_REASON_NONE        = 0
YIELD_REASON_BUFFER_FULL = 1
YIELD_REASON_ROW_LIMIT   = 2
YIELD_REASON_USER        = 3


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
        "fd",
        "ptr",
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


@jit.dont_look_inside
def serialize_to_memfd(target_id, zbatch=None, status=0, error_msg="", client_id=0):
    """
    Decoupled Serialization: Writes a batch into a shared-memory file descriptor.
    In Phase 4, the resulting FD is broadcast to multiple subscribers.
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
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_TARGET_ID))[0] = r_uint64(target_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_CLIENT_ID))[0] = r_uint64(client_id)

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
        return fd

    except Exception as e:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)
        raise e


@jit.dont_look_inside
def send_batch(sock_fd, target_id, zbatch, status=0, error_msg="", client_id=0):
    """
    Synchronous send: Serializes and transmits a single batch.
    Used for request-response pairs and ingestion acknowledgments.
    """
    memfd = serialize_to_memfd(target_id, zbatch, status, error_msg, client_id)
    try:
        if ipc_ffi.send_fd(sock_fd, memfd) < 0:
            # Most likely the client disconnected (EPIPE)
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
    Utilizes target_id for O(1) schema resolution.
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

        # 1. Verify Magic
        magic = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_MAGIC))[0]
        if magic != MAGIC_IPC:
            raise errors.StorageError("Invalid IPC Magic")

        # 2. Parse Header
        # Fixed: Use primitive constructors (intmask, r_uint64) for RPython type conversion,
        # not rffi.cast.
        status = intmask(rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_STATUS))[0])
        
        raw_err_len = rffi.cast(rffi.UINTP, rffi.ptradd(ptr, OFF_ERR_LEN))[0]
        err_len = r_uint64(raw_err_len)

        primary_sz = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_PRIMARY_SZ))[0])
        blob_sz = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_BLOB_SZ))[0])
        count = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_COUNT))[0])
        target_id = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_TARGET_ID))[0]
        client_id = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, OFF_CLIENT_ID))[0]

        # 3. Validation: Bounds and Arithmetic Overflows
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

        # 4. Resolve Schema and Reconstruct Batch if data is present
        if count > 0:
            if not registry.has_id(tid):
                raise errors.StorageError(
                    "IPC routing failed: Target ID %d not found in registry" % tid
                )

            family = registry.get_by_id(tid)
            schema = family.get_schema()

            # Check Primary Arena bounds
            primary_off = align_up(r_uint64(HEADER_SIZE) + err_len, ALIGNMENT)
            if primary_off > total_size or primary_sz > (total_size - primary_off):
                raise errors.StorageError("Truncated IPC (Primary arena boundary)")

            # Check Blob Arena bounds
            blob_off = align_up(primary_off + primary_sz, ALIGNMENT)
            if blob_off > total_size or blob_sz > (total_size - blob_off):
                raise errors.StorageError("Truncated IPC (Blob arena boundary)")

            # Construct zero-copy buffer view (is_owned=False)
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

        return IPCPayload(fd, ptr, total_size, status, error_msg, zbatch, tid, intmask(client_id))

    except Exception as e:
        if ptr:
            mmap_posix.munmap_file(ptr, intmask(total_size))
        if fd >= 0:
            os.close(fd)
        raise e
