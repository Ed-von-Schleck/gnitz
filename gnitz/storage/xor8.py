# gnitz/storage/xor8.py
#
# XOR8 filter backed by Rust (libgnitz_engine).

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask
from rpython.rlib import rposix, rposix_stat

from gnitz.storage import engine_ffi, mmap_posix
from gnitz import log


class Xor8Filter(object):
    """
    8-bit fingerprint XOR filter. Wraps an opaque Rust handle.
    """

    _immutable_fields_ = ["_handle"]

    def __init__(self, handle):
        self._handle = handle

    def may_contain(self, key_lo, key_hi):
        res = engine_ffi._xor8_may_contain(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )
        return intmask(res) != 0

    def serialized_size(self):
        return intmask(engine_ffi._xor8_serialize(
            self._handle, lltype.nullptr(rffi.CCHARP.TO), rffi.cast(rffi.LONGLONG, 0)
        ))

    def serialize_into(self, dest_ptr, capacity):
        """Serialize directly into a caller-provided buffer. Returns bytes written."""
        return intmask(engine_ffi._xor8_serialize(
            self._handle, dest_ptr, rffi.cast(rffi.LONGLONG, capacity)
        ))

    def free(self):
        if self._handle:
            engine_ffi._xor8_free(self._handle)
            self._handle = lltype.nullptr(rffi.VOIDP.TO)


def parse_xor8_from_ptr(ptr, block_size):
    """Parse an XOR8 filter from a memory-mapped region.
    Returns Xor8Filter or None. Returns None for old-format data."""
    handle = engine_ffi._xor8_deserialize(ptr, rffi.cast(rffi.LONGLONG, block_size))
    if not handle:
        return None
    return Xor8Filter(handle)


def build_xor8(pk_lo_ptr, pk_hi_ptr, num_keys):
    """Build an XOR8 filter from raw PK pointer arrays.
    Returns Xor8Filter or None."""
    if num_keys <= 0:
        return None
    handle = engine_ffi._xor8_build(
        rffi.cast(rffi.ULONGLONGP, pk_lo_ptr),
        rffi.cast(rffi.ULONGLONGP, pk_hi_ptr),
        rffi.cast(rffi.UINT, num_keys),
    )
    if not handle:
        return None
    return Xor8Filter(handle)


def save_xor8(xor_filter, dirfd, basename):
    """Write XOR8 filter to a sidecar file. Best-effort -- errors silently ignored."""
    size = xor_filter.serialized_size()
    if size <= 0:
        return
    buf = lltype.malloc(rffi.CCHARP.TO, size, flavor="raw")
    try:
        written = xor_filter.serialize_into(buf, size)
        if written <= 0:
            return
        try:
            fd = mmap_posix.openat_c(
                dirfd, basename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644
            )
        except mmap_posix.MMapError:
            log.warn("xor8: cannot create " + basename)
            return
        try:
            mmap_posix.write_all(fd, buf, rffi.cast(rffi.SIZE_T, written))
        except mmap_posix.MMapError:
            log.warn("xor8: write failed for " + basename)
        rposix.close(fd)
    finally:
        lltype.free(buf, flavor="raw")


def load_xor8(filepath):
    """Load XOR8 filter from a sidecar file. Returns None if file doesn't exist
    or if the format is unrecognized (e.g. old pre-Rust format)."""
    try:
        fd = rposix.open(filepath, os.O_RDONLY, 0)
    except OSError:
        return None
    try:
        try:
            st = rposix_stat.fstat(fd)
        except OSError:
            return None
        file_size = intmask(st.st_size)
        if file_size < 20:  # minimum: 4 magic + 8 seed + 4 block_length + 4 fp_count
            return None

        buf = lltype.malloc(rffi.CCHARP.TO, file_size, flavor="raw")
        try:
            bytes_read = mmap_posix.read_into_ptr(fd, buf, file_size)
            if bytes_read < file_size:
                return None
            handle = engine_ffi._xor8_deserialize(
                buf, rffi.cast(rffi.LONGLONG, file_size)
            )
            if not handle:
                return None
            return Xor8Filter(handle)
        finally:
            lltype.free(buf, flavor="raw")
    finally:
        rposix.close(fd)
